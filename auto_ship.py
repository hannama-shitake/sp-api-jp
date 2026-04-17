"""
Amazon AU の未出荷(Unshipped)注文を Ship&co で DHL 出荷 → 追跡番号を Amazon に登録するスクリプト。

フロー:
  1. Amazon AU の Unshipped 注文を取得
  2. 注文ごとに Ship&co で DHL ラベル作成
  3. confirm_shipment で追跡番号を Amazon に登録
  4. メール通知（ラベル URL・追跡番号）

使い方:
  python auto_ship.py              # 全 Unshipped 注文を処理
  python auto_ship.py --dry-run    # テスト（ラベル作成・Amazon 登録なし）
  python auto_ship.py --test-label # Ship&co テストラベル（課金なし）で動作確認
"""
import argparse
import sys
import time
from datetime import datetime, timezone

from sp_api.api import Orders
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.shipco_api import create_shipment
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}


# ─────────────────────────────────────────────
# Amazon 注文取得
# ─────────────────────────────────────────────

def get_unshipped_orders() -> list:
    """Unshipped かつ MFN(自己発送) な注文を全件取得する"""
    api = Orders(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    orders = []
    next_token = None

    while True:
        kwargs = {
            "MarketplaceIds":      [config.MARKETPLACE_AU],
            "OrderStatuses":       ["Unshipped"],
            "FulfillmentChannels": ["MFN"],
        }
        if next_token:
            kwargs["NextToken"] = next_token

        try:
            resp = api.get_orders(**kwargs)
            payload = resp.payload
            orders.extend(payload.get("Orders", []))
            next_token = payload.get("NextToken")
            if not next_token:
                break
            time.sleep(1)
        except SellingApiException as e:
            logger.error("[auto_ship] 注文取得失敗: %s", e)
            break

    logger.info("[auto_ship] Unshipped 注文: %d件", len(orders))
    return orders


def get_order_items(order_id: str) -> list:
    """注文アイテムを取得する"""
    api = Orders(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    try:
        resp = api.get_order_items(order_id)
        return resp.payload.get("OrderItems", [])
    except SellingApiException as e:
        logger.warning("[auto_ship] アイテム取得失敗 %s: %s", order_id, e)
        return []


# ─────────────────────────────────────────────
# Amazon 出荷確認登録
# ─────────────────────────────────────────────

def confirm_shipment_amazon(order_id: str, tracking_number: str) -> bool:
    """Amazon に DHL 追跡番号を登録して出荷確認する"""
    api = Orders(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    body = {
        "packageDetail": {
            "packageReferenceId": "1",
            "carrierCode":        "DHL",
            "carrierName":        "DHL",
            "shippingMethod":     "Express Worldwide",
            "trackingNumber":     tracking_number,
            "shipDate":           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "marketplaceId": config.MARKETPLACE_AU,
    }
    try:
        api.confirm_shipment(orderId=order_id, body=body)
        logger.info("[auto_ship] 出荷確認完了: %s | 追跡=%s", order_id, tracking_number)
        return True
    except Exception as e:
        logger.error("[auto_ship] 出荷確認失敗 %s: %s", order_id, e)
        return False


# ─────────────────────────────────────────────
# データ変換
# ─────────────────────────────────────────────

def build_shipco_address(order: dict) -> dict:
    """Amazon 注文の配送先住所を Ship&co 形式に変換する"""
    addr  = order.get("ShippingAddress", {})
    buyer = order.get("BuyerInfo", {})
    return {
        "full_name": addr.get("Name", ""),
        "phone":     buyer.get("BuyerPhone", "") or addr.get("Phone", ""),
        "email":     buyer.get("BuyerEmail", ""),
        "country":   addr.get("CountryCode", "AU"),
        "zip":       addr.get("PostalCode", ""),
        "province":  addr.get("StateOrRegion", ""),
        "city":      addr.get("City", ""),
        "address1":  addr.get("AddressLine1", ""),
        "address2":  addr.get("AddressLine2", "") or "",
    }


def build_shipco_products(items: list) -> list:
    """注文アイテムを Ship&co customs 申告形式に変換する"""
    products = []
    for item in items:
        price = int(float(
            item.get("ItemPrice", {}).get("Amount", 0)
            or item.get("ItemTax", {}).get("Amount", 0)
            or 3000
        ))
        products.append({
            "name":           item.get("Title", "Japanese goods")[:50],
            "price":          price,
            "quantity":       int(item.get("QuantityOrdered", 1)),
            "origin_country": "JP",
        })
    return products or [{"name": "Japanese goods", "price": 3000, "quantity": 1, "origin_country": "JP"}]


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Amazon AU 未出荷注文を Ship&co DHL で自動出荷")
    parser.add_argument("--dry-run",    action="store_true", help="ラベル作成・Amazon 登録を行わない")
    parser.add_argument("--test-label", action="store_true", help="Ship&co テストラベル（課金なし）で動作確認")
    args = parser.parse_args()

    if not config.SHIPCO_API_TOKEN and not args.dry_run:
        logger.error("[auto_ship] SHIPCO_API_TOKEN が未設定。--dry-run で確認してください")
        sys.exit(1)

    if args.dry_run:
        logger.info("[auto_ship] *** DRY-RUN モード ***")
    if args.test_label:
        logger.info("[auto_ship] *** テストラベルモード（Ship&co 課金なし）***")

    orders = get_unshipped_orders()
    if not orders:
        logger.info("[auto_ship] 未出荷注文なし。終了")
        return

    shipped = failed = skipped = 0
    details = []

    for order in orders:
        order_id   = order.get("AmazonOrderId", "")
        buyer_name = order.get("ShippingAddress", {}).get("Name", "")
        total      = order.get("OrderTotal", {})
        amount_str = f"AU${total.get('Amount', '?')}"

        # 注文アイテム取得
        items = get_order_items(order_id)
        if not items:
            logger.warning("[auto_ship] %s: アイテム取得失敗 → スキップ", order_id)
            skipped += 1
            continue

        to_address = build_shipco_address(order)
        products   = build_shipco_products(items)

        log_base = f"{order_id} | {buyer_name} | {amount_str}"

        # DRY-RUN
        if args.dry_run:
            logger.info("[auto_ship][DRY] 出荷予定: %s | 宛先=%s %s",
                        log_base, to_address.get("city"), to_address.get("country"))
            details.append({"order_id": order_id, "buyer": buyer_name, "total": amount_str})
            shipped += 1
            continue

        # Ship&co ラベル作成
        result = create_shipment(
            order_id=order_id,
            to_address=to_address,
            products=products,
            test=args.test_label,
        )
        if not result:
            logger.error("[auto_ship] Ship&co 失敗: %s → スキップ", order_id)
            failed += 1
            continue

        tracking  = result["tracking_number"]
        label_url = result["label_url"]
        fee_jpy   = result["fee_jpy"]

        # テストラベルは Amazon 登録しない
        amazon_ok = True
        if not args.test_label:
            amazon_ok = confirm_shipment_amazon(order_id, tracking)

        if amazon_ok or args.test_label:
            label_status = "（テスト）" if args.test_label else ""
            logger.info("[auto_ship] 完了%s: %s | 追跡=%s | 送料¥%d",
                        label_status, log_base, tracking, fee_jpy)
            details.append({
                "order_id":    order_id,
                "buyer":       buyer_name,
                "total":       amount_str,
                "tracking":    tracking,
                "label_url":   label_url,
                "fee_jpy":     fee_jpy,
                "test":        args.test_label,
            })
            shipped += 1
        else:
            failed += 1

        time.sleep(1)

    # サマリーログ
    logger.info("[auto_ship] 完了: 出荷%d件 / 失敗%d件 / スキップ%d件",
                shipped, failed, skipped)

    # メール通知
    dry_label  = "[DRY-RUN] " if args.dry_run else ""
    test_label = "[TEST] "    if args.test_label else ""
    prefix = dry_label or test_label

    subject = f"[Ship&co] {prefix}自動出荷: {shipped}件完了 / {failed}件失敗"
    lines = [
        f"=== {prefix}Amazon AU 自動出荷結果 ===",
        "",
        f"未出荷注文:  {len(orders)}件",
        f"出荷{'予定' if args.dry_run else '完了'}:    {shipped}件",
        f"失敗:       {failed}件",
        f"スキップ:   {skipped}件",
        "",
    ]
    if details:
        lines.append(f"--- 出荷{'予定' if args.dry_run else '完了'}リスト ---")
        for d in details:
            if args.dry_run:
                lines.append(f"  {d['order_id']} | {d['buyer']} | {d['total']}")
            else:
                test_mark = "（TEST）" if d.get("test") else ""
                lines.append(
                    f"  {d['order_id']} | {d['buyer']} | {d['total']}"
                    f" | 追跡: {d['tracking']}{test_mark} | 送料¥{d['fee_jpy']:,}"
                )
                lines.append(f"    ラベル: {d['label_url']}")
        lines.append("")

    body = "\n".join(lines)
    send_email(subject=subject, body=body)
    print(body)


if __name__ == "__main__":
    main()

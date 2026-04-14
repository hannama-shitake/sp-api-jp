"""
指定ASINのAU出品を再アクティブ化するスクリプト。
qty=0で停止中の出品をJP価格から最適価格を計算してqty=1に戻す。

使い方:
  python reactivate_listing.py B072863VKR
  python reactivate_listing.py B072863VKR B0D69JFQKH  # 複数可
  REACTIVATE_ASINS=B072863VKR,B0D69JFQKH python reactivate_listing.py  # 環境変数
"""
import csv
import gzip
import io
import os
import sys
import time

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP_CREDS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}


def get_all_au_listings() -> dict:
    """Reports APIで全出品（停止中含む）のASIN→SKUマップを返す"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]
    logger.info("[reactivate] レポートID: %s", report_id)

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[reactivate] ステータス: %s (%d/120)", status, attempt + 1)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")

    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")

    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    asin_to_sku = {}
    asin_to_info = {}
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        status = row.get("status", "").strip().lower()
        title = row.get("item-name", "").strip()
        price = row.get("price", "").strip()
        if asin and len(asin) == 10 and sku and status != "deleted":
            asin_to_sku[asin] = sku
            asin_to_info[asin] = {"title": title, "price": price, "status": status}

    logger.info("[reactivate] 全出品取得: %d件", len(asin_to_sku))
    return asin_to_sku, asin_to_info


def get_jp_price(asin: str) -> tuple:
    """JP価格と在庫状況を取得 → (price_jpy, in_stock)"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    try:
        resp = api.get_competitive_pricing_for_asins([asin])
        items = resp.payload if isinstance(resp.payload, list) else []
        for item in items:
            comp_prices = (
                item.get("Product", {})
                .get("CompetitivePricing", {})
                .get("CompetitivePrices", [])
            )
            for cp in comp_prices:
                if cp.get("condition") == "New":
                    amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                    if amount:
                        return int(float(amount)), True
    except SellingApiException as e:
        logger.warning("[reactivate] JP価格取得エラー %s: %s", asin, e)
    return None, False


def reactivate(asin: str, sku: str, seller_id: str, exchange_rate: float, info: dict):
    """ASIN を再アクティブ化: JP価格から最適価格を計算してqty=1に設定"""
    title = info.get("title", "")[:50]
    current_price = info.get("price", "")
    current_status = info.get("status", "")

    logger.info("[reactivate] %s (%s) 処理開始 現在状態: %s 価格: %s",
                asin, title, current_status, current_price)

    jp_price, in_stock = get_jp_price(asin)
    if not in_stock or not jp_price:
        logger.warning("[reactivate] %s: JP在庫なし → スキップ", asin)
        return False, "JP在庫なし"

    min_price = calc_optimal_au_price(jp_price)
    result = calc_profit(
        asin=asin, title=title,
        jp_price_jpy=jp_price,
        au_price_aud=min_price,
        exchange_rate=exchange_rate,
    )

    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    body = {
        "productType": "PRODUCT",
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/fulfillment_availability",
                "value": [{
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": 1,
                    "marketplace_id": config.MARKETPLACE_AU,
                }],
            },
            {
                "op": "replace",
                "path": "/attributes/purchasable_offer",
                "value": [{
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": min_price}]}],
                    "marketplace_id": config.MARKETPLACE_AU,
                }],
            },
        ],
    }
    api.patch_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[config.MARKETPLACE_AU], body=body,
    )

    logger.info("[reactivate] %s: 再出品完了 JP¥%d → AU$%.2f (粗利率 %.1f%%)",
                asin, jp_price, min_price, result.profit_rate)
    return True, f"JP¥{jp_price} → AU${min_price:.2f} (粗利率{result.profit_rate:.1f}%)"


def main():
    # 対象ASINを引数または環境変数から取得
    target_asins = sys.argv[1:] if len(sys.argv) > 1 else []
    if not target_asins:
        env_asins = os.getenv("REACTIVATE_ASINS", "")
        target_asins = [a.strip() for a in env_asins.split(",") if a.strip()]
    if not target_asins:
        logger.error("対象ASINを引数またはREACTIVATE_ASINS環境変数で指定してください")
        sys.exit(1)

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        logger.error("AMAZON_AU_SELLER_ID未設定")
        sys.exit(1)

    exchange_rate = get_jpy_to_aud()
    logger.info("[reactivate] 為替: 1 JPY = %.6f AUD", exchange_rate)

    logger.info("[reactivate] 全出品データ取得中...")
    asin_to_sku, asin_to_info = get_all_au_listings()

    results = []
    for asin in target_asins:
        sku = asin_to_sku.get(asin)
        if not sku:
            logger.warning("[reactivate] %s: 出品データなし（SKU不明）", asin)
            results.append((asin, False, "出品データなし"))
            continue

        try:
            ok, msg = reactivate(asin, sku, seller_id, exchange_rate, asin_to_info.get(asin, {}))
            results.append((asin, ok, msg))
        except Exception as e:
            logger.error("[reactivate] %s: エラー - %s", asin, e)
            results.append((asin, False, str(e)))
        time.sleep(1)

    # 結果をメール通知
    body = "=== 再出品結果 ===\n\n"
    for asin, ok, msg in results:
        status = "✅ 成功" if ok else "❌ 失敗/スキップ"
        body += f"{status} {asin}: {msg}\n"
    send_email(subject=f"[SP-API] 再出品完了: {len([r for r in results if r[1]])}件成功", body=body)
    print(body)


if __name__ == "__main__":
    main()

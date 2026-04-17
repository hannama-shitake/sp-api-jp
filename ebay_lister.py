"""
Amazon AU 出品中の商品を eBay に一括クロスリストするスクリプト。

フロー:
  1. Amazon AU 全出品取得（Reports API）→ active な ASIN/タイトル/AUD価格
  2. eBay 既存出品取得 → 既に出品中のASINを除外（custom_label=ASIN で管理）
  3. JP価格確認 → JP在庫ありのみ対象
  4. USD価格計算（JP価格 + DHL + 利益率 + eBay手数料）
  5. Amazon Catalog API → 商品画像URL取得
  6. eBay 新規出品（Trading API AddItem）
  7. JP在庫なし → eBay出品終了（EndItem）
  8. メール通知

使い方:
  python ebay_lister.py                  # 新規出品（上限200件）
  python ebay_lister.py --dry-run        # テスト（出品しない）
  python ebay_lister.py --max-new 50     # 上限50件
  python ebay_lister.py --sync-only      # 価格同期・終了のみ（新規出品なし）
"""
import argparse
import csv
import gzip
import io
import os
import sys
import time

import requests as _requests
from sp_api.api import Reports, Products, CatalogItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis import ebay_api
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price
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

JP_INTERVAL = 2.1
EBAY_INTERVAL = 0.5


# ─────────────────────────────────────────────
# 為替レート（JPY→USD）
# ─────────────────────────────────────────────

def get_jpy_to_usd() -> float:
    """JPY→USD レートを取得する（exchangerate-api 経由）"""
    try:
        r = _requests.get(
            "https://api.exchangerate-api.com/v4/latest/JPY", timeout=10
        )
        r.raise_for_status()
        rate = r.json().get("rates", {}).get("USD", config.JPY_TO_USD_FALLBACK)
        logger.info("[ebay_lister] JPY→USD: %.6f", rate)
        return float(rate)
    except Exception as e:
        logger.warning("[ebay_lister] 為替取得失敗、フォールバック使用: %s", e)
        return config.JPY_TO_USD_FALLBACK


# ─────────────────────────────────────────────
# Amazon AU 出品一覧取得
# ─────────────────────────────────────────────

def get_au_active_listings() -> list:
    """
    Reports API で AU アクティブ出品を取得する。
    [{asin, sku, title, price_aud}]
    """
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    logger.info("[ebay_lister] AU出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[ebay_lister] レポートステータス: %s (%d/120)", status, attempt + 1)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")
    else:
        raise RuntimeError("タイムアウト")

    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")
    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    listings = []
    seen = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        item_status = row.get("status", "").strip().lower()
        title = row.get("item-name", "").strip()
        price_str = row.get("price", "").strip()
        if (asin and len(asin) == 10 and sku
                and asin not in seen and item_status == "active"):
            seen.add(asin)
            try:
                price_aud = float(price_str) if price_str else None
            except ValueError:
                price_aud = None
            listings.append({
                "asin": asin, "sku": sku,
                "title": title, "price_aud": price_aud,
            })

    logger.info("[ebay_lister] AU active出品: %d件", len(listings))
    return listings


# ─────────────────────────────────────────────
# JP 価格一括取得
# ─────────────────────────────────────────────

def get_jp_prices_bulk(asins: list) -> dict:
    """20件バッチで JP 価格 {asin: (price_jpy, in_stock)} を返す"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    result = {}
    for i in range(0, len(asins), 20):
        batch = asins[i: i + 20]
        if i % 200 == 0:
            logger.info("[ebay_lister] JP価格取得中: %d/%d", i, len(asins))
        try:
            resp = api.get_competitive_pricing_for_asins(batch)
            items = resp.payload if isinstance(resp.payload, list) else []
            for item in items:
                asin = item.get("ASIN", "")
                comp_prices = (
                    item.get("Product", {})
                    .get("CompetitivePricing", {})
                    .get("CompetitivePrices", [])
                )
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            result[asin] = (int(float(amount)), True)
                        break
                if asin not in result:
                    result[asin] = (None, False)
        except SellingApiException as e:
            logger.warning("[ebay_lister] JP価格バッチエラー: %s", e)
        time.sleep(JP_INTERVAL)
    return result


# ─────────────────────────────────────────────
# 商品画像取得（Amazon Catalog Items API）
# ─────────────────────────────────────────────

def get_product_image(asin: str) -> str:
    """Catalog Items API から商品メイン画像URLを取得する"""
    try:
        api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
        resp = api.get_catalog_item(
            asin,
            marketplaceIds=[config.MARKETPLACE_JP],
            includedData=["images"],
        )
        images = (resp.payload or {}).get("images", [])
        for img_set in images:
            for img in img_set.get("images", []):
                if img.get("variant") == "MAIN":
                    url = img.get("link", "")
                    if url:
                        # eBay要件: https で直接アクセスできるURL
                        return url.replace("http://", "https://")
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────
# USD 価格計算
# ─────────────────────────────────────────────

def calc_ebay_usd_price(jp_price_jpy: int, jpy_to_usd: float) -> float:
    """
    JP仕入値から eBay USD 出品価格を計算する。
    利益 = MIN_PROFIT_RATE、送料 = DHL_SHIPPING_JPY、手数料 = EBAY_FEE_RATE
    """
    required_revenue_jpy = (
        jp_price_jpy * (1 + config.MIN_PROFIT_RATE / 100)
        + config.DHL_SHIPPING_JPY
    )
    net_usd = required_revenue_jpy * jpy_to_usd
    price_usd = net_usd / (1 - config.EBAY_FEE_RATE)
    return round(price_usd, 2)


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Amazon AU→eBay 一括クロスリスト")
    parser.add_argument("--dry-run", action="store_true", help="出品せず確認のみ")
    parser.add_argument("--max-new", type=int, default=200, help="新規出品上限（デフォルト200）")
    parser.add_argument("--sync-only", action="store_true",
                        help="新規出品なし、価格同期・終了のみ")
    args = parser.parse_args()

    if not config.EBAY_USER_TOKEN and not args.dry_run:
        logger.error("[ebay_lister] EBAY_USER_TOKEN が未設定。--dry-run で確認してください")
        sys.exit(1)

    if args.dry_run:
        logger.info("[ebay_lister] *** DRY-RUN モード ***")

    jpy_to_usd = get_jpy_to_usd()
    jpy_to_aud = get_jpy_to_aud()
    logger.info("[ebay_lister] 為替: 1 JPY = %.6f USD / %.6f AUD",
                jpy_to_usd, jpy_to_aud)

    # 1. Amazon AU アクティブ出品
    au_listings = get_au_active_listings()
    if not au_listings:
        logger.info("[ebay_lister] AU出品なし。終了")
        return

    asins = [l["asin"] for l in au_listings]
    au_by_asin = {l["asin"]: l for l in au_listings}

    # 2. eBay 既存出品（ASIN→ItemID マップ）
    ebay_listings = ebay_api.get_active_listings() if not args.dry_run else {}
    # custom_label (SKU) = ASIN で管理
    ebay_asin_to_item = {
        v["custom_label"]: k
        for k, v in ebay_listings.items()
        if v.get("custom_label")
    }
    logger.info("[ebay_lister] eBay既存出品: %d件", len(ebay_asin_to_item))

    # 3. JP価格取得
    logger.info("[ebay_lister] JP価格確認: %d件", len(asins))
    jp_prices = get_jp_prices_bulk(asins)

    # 4. 処理
    listed = updated = ended = failed = skipped = 0
    listed_details = []

    for listing in au_listings:
        asin = listing["asin"]
        title = listing["title"] or asin
        jp_price, in_stock = jp_prices.get(asin, (None, False))

        # JP在庫なし → eBay出品があれば終了
        if not in_stock or not jp_price:
            if asin in ebay_asin_to_item and not args.dry_run:
                ebay_api.end_item(ebay_asin_to_item[asin])
                ended += 1
            continue

        # USD価格計算
        price_usd = calc_ebay_usd_price(jp_price, jpy_to_usd)

        # eBay既存 → 価格更新
        if asin in ebay_asin_to_item:
            item_id = ebay_asin_to_item[asin]
            current_usd = ebay_listings[item_id]["price_usd"]
            # 3%以上乖離したら更新
            if abs(price_usd - current_usd) / max(current_usd, 0.01) > 0.03:
                if not args.dry_run:
                    if ebay_api.revise_price(item_id, price_usd):
                        updated += 1
                    else:
                        failed += 1
                else:
                    logger.info("[ebay_lister][DRY] 価格更新: %s $%.2f→$%.2f",
                                asin, current_usd, price_usd)
                    updated += 1
            else:
                skipped += 1
            time.sleep(EBAY_INTERVAL)
            continue

        # 新規出品スキップ条件
        if args.sync_only:
            continue
        if listed >= args.max_new:
            continue

        # 画像取得
        image_url = get_product_image(asin)
        time.sleep(0.3)

        log_msg = f"{asin} | {title[:40]} | ${ price_usd:.2f} (JP¥{jp_price:,})"

        if args.dry_run:
            logger.info("[ebay_lister][DRY] 出品予定: %s", log_msg)
            listed_details.append({
                "asin": asin, "title": title,
                "jp_price": jp_price, "price_usd": price_usd,
            })
            listed += 1
            continue

        item_id = ebay_api.add_item(
            title=title,
            price_usd=price_usd,
            image_url=image_url,
        )
        if item_id:
            logger.info("[ebay_lister] 出品完了: %s | ItemID=%s", log_msg, item_id)
            listed_details.append({
                "asin": asin, "title": title,
                "jp_price": jp_price, "price_usd": price_usd,
                "item_id": item_id,
            })
            listed += 1
        else:
            failed += 1

        time.sleep(EBAY_INTERVAL)

    # 5. 結果サマリー
    logger.info(
        "[ebay_lister] 完了: 新規%d件 / 価格更新%d件 / 終了%d件 / スキップ%d件 / 失敗%d件",
        listed, updated, ended, skipped, failed,
    )

    dry_label = "[DRY-RUN] " if args.dry_run else ""
    subject = (
        f"[eBay] {dry_label}クロスリスト: 新規{listed}件 / 更新{updated}件 / 終了{ended}件"
    )
    lines = [
        f"=== {dry_label}eBay クロスリスト結果 ===",
        "",
        f"AU active出品ベース:  {len(au_listings)}件",
        f"新規出品{'予定' if args.dry_run else '完了'}:        {listed}件",
        f"価格更新:            {updated}件",
        f"出品終了(JP在庫切れ): {ended}件",
        f"スキップ(価格変動小): {skipped}件",
        f"失敗:               {failed}件",
        "",
    ]
    if listed_details:
        lines.append(f"--- 新規出品{'予定' if args.dry_run else '完了'} ({listed}件) ---")
        for item in listed_details[:50]:
            lines.append(
                f"  {item['asin']}  JP¥{item['jp_price']:,} → US${item['price_usd']:.2f}"
                f"  {item['title'][:40]}"
            )
        if listed > 50:
            lines.append(f"  ... 他 {listed - 50}件")

    body = "\n".join(lines)
    send_email(subject=subject, body=body)
    print(body)


if __name__ == "__main__":
    main()

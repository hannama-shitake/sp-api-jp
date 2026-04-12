"""
出品中の全商品について JP 価格を確認し、AU 出品価格を自動更新するスクリプト。
DBに依存せず、Reports APIでリアルタイムに自分の出品一覧を取得する。

ロジック:
  - JP在庫あり + 利益率OK → AU価格を最適価格に更新
  - JP在庫あり + 利益率NG → 在庫0にして出品停止
  - JP在庫なし             → 在庫0にして出品停止
"""
import csv
import gzip
import io
import sys
import time

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.logger import get_logger

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

_JP_INTERVAL = 2.1   # Products API: 0.5 req/s
_AU_INTERVAL = 0.3   # ListingsItems: 5 req/s


# ─────────────────────────────────────────────
# 1. AU 出品一覧取得
# ─────────────────────────────────────────────

def get_my_au_listings() -> list:
    """Reports API で自分の有効な AU 出品一覧 {asin, sku} を取得する"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[price_update] 出品レポートをリクエスト中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[price_update] レポートステータス: %s (%d/120)", status, attempt + 1)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")
    else:
        raise RuntimeError("レポートタイムアウト（20分）")

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
        status = row.get("status", "").strip().lower()
        # 削除済み・停止中以外を対象とする（"active" or "" どちらも含む）
        if asin and len(asin) == 10 and sku and asin not in seen and status != "deleted":
            seen.add(asin)
            listings.append({"asin": asin, "sku": sku})

    logger.info("[price_update] 出品取得完了: %d件", len(listings))
    return listings


# ─────────────────────────────────────────────
# 2. JP 価格一括取得
# ─────────────────────────────────────────────

def get_jp_prices_bulk(asins: list) -> dict:
    """20件バッチで JP 価格 {asin: (price_jpy, in_stock)} を返す"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    result = {}
    batch_size = 20
    total = len(asins)

    for i in range(0, total, batch_size):
        batch = asins[i : i + batch_size]
        if i % 200 == 0:
            logger.info("[price_update] JP価格取得中: %d/%d", i, total)
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
                price_jpy = None
                in_stock = False
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            price_jpy = int(float(amount))
                            in_stock = True
                        break
                result[asin] = (price_jpy, in_stock)
        except SellingApiException as e:
            logger.warning("[price_update] バッチエラー (%s...): %s", batch[0], e)
        time.sleep(_JP_INTERVAL)

    logger.info("[price_update] JP価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 3. AU 価格更新 / 停止
# ─────────────────────────────────────────────

def update_au_prices(listings: list, jp_prices: dict, exchange_rate: float, seller_id: str):
    """JP価格をもとに AU 出品価格を更新 or 停止する"""
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    updated = paused = skipped = failed = 0

    for listing in listings:
        asin = listing["asin"]
        sku = listing["sku"]
        jp_price, in_stock = jp_prices.get(asin, (None, False))

        # JP在庫なし → 停止
        if not in_stock or not jp_price:
            try:
                _set_quantity(api, seller_id, sku, 0)
                paused += 1
                logger.debug("[price_update] %s: JP在庫なし → 停止", asin)
            except Exception as e:
                logger.warning("[price_update] %s: 停止失敗 - %s", asin, e)
                failed += 1
            time.sleep(_AU_INTERVAL)
            continue

        # 利益計算
        # AU販売価格はJP仕入から最適価格を算出（最低利益確保ライン）
        optimal_price = calc_optimal_au_price(jp_price)
        result = calc_profit(
            asin=asin, title="", jp_price_jpy=jp_price,
            au_price_aud=optimal_price, exchange_rate=exchange_rate,
        )

        if not result.is_profitable:
            # 利益率不足 → 停止
            try:
                _set_quantity(api, seller_id, sku, 0)
                paused += 1
                logger.info("[price_update] %s: 利益率%.1f%% → 停止", asin, result.profit_rate)
            except Exception as e:
                logger.warning("[price_update] %s: 停止失敗 - %s", asin, e)
                failed += 1
        else:
            # 価格更新 + 在庫1に設定
            try:
                _put_price_and_quantity(api, seller_id, sku, optimal_price)
                updated += 1
                logger.debug("[price_update] %s: JP¥%d → AU$%.2f (%.1f%%)",
                             asin, jp_price, optimal_price, result.profit_rate)
            except Exception as e:
                logger.warning("[price_update] %s: 価格更新失敗 - %s", asin, e)
                failed += 1

        time.sleep(_AU_INTERVAL)

    logger.info(
        "[price_update] 完了: 更新 %d件 / 停止 %d件 / 失敗 %d件 / スキップ %d件",
        updated, paused, failed, skipped,
    )
    return updated, paused, failed


def _set_quantity(api, seller_id, sku, quantity):
    body = {
        "productType": "PRODUCT",
        "patches": [{
            "op": "replace",
            "path": "/attributes/fulfillment_availability",
            "value": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": quantity,
                "marketplace_id": config.MARKETPLACE_AU,
            }],
        }],
    }
    api.patch_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[config.MARKETPLACE_AU], body=body,
    )


def _put_price_and_quantity(api, seller_id, sku, price_aud):
    body = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "condition_type": [
                {"value": "new_new", "marketplace_id": config.MARKETPLACE_AU}
            ],
            "fulfillment_availability": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": 1,
                "marketplace_id": config.MARKETPLACE_AU,
            }],
            "purchasable_offer": [{
                "currency": "AUD",
                "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                "marketplace_id": config.MARKETPLACE_AU,
            }],
        },
    }
    api.put_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[config.MARKETPLACE_AU], body=body,
    )


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main():
    import os
    seller_id = (os.getenv("AMAZON_AU_SELLER_ID") or "").strip()
    if not seller_id:
        logger.error("AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    exchange_rate = get_jpy_to_aud()
    logger.info("[price_update] 為替レート: 1 JPY = %.6f AUD", exchange_rate)

    # 1. 自分の出品一覧取得
    listings = get_my_au_listings()
    if not listings:
        logger.info("[price_update] 出品なし。終了")
        return

    # 2. JP価格一括取得
    asins = [l["asin"] for l in listings]
    eta = len(asins) / 20 * _JP_INTERVAL / 60
    logger.info("[price_update] JP価格確認: %d件（約%.0f分）", len(asins), eta)
    jp_prices = get_jp_prices_bulk(asins)

    # 3. AU価格更新
    update_au_prices(listings, jp_prices, exchange_rate, seller_id)


if __name__ == "__main__":
    main()

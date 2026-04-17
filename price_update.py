"""
出品中・停止中の全商品について JP 価格・AU 競合価格を確認し、AU 出品価格を自動更新するスクリプト。
DBに依存せず、Reports APIでリアルタイムに自分の出品一覧を取得する。

価格決定ロジック:
  1. JP在庫なし                    → 在庫0で停止
  2. JP在庫あり + AU競合価格あり
       競合価格 >= 最低利益ライン   → 競合価格で出品（利益最大化）
       競合価格 <  最低利益ライン   → 在庫0で停止（赤字回避）
  3. JP在庫あり + AU競合価格なし   → 最低利益ラインで出品

  ★ 停止中(inactive)の出品も対象。他セラーが販売開始 + JP在庫あり → 自動再出品。
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
from utils.notify import notify_price_update_summary

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

_JP_INTERVAL = 2.1   # Products API JP: 0.5 req/s
_AU_PRICE_INTERVAL = 2.1  # Products API AU: 0.5 req/s
_AU_INTERVAL = 0.3   # ListingsItems: 5 req/s


# ─────────────────────────────────────────────
# 1. AU 出品一覧取得
# ─────────────────────────────────────────────

def get_my_au_listings() -> list:
    """Reports API で自分の AU 出品一覧（停止中含む）{asin, sku, status} を取得する"""
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
    active_count = inactive_count = 0
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        item_status = row.get("status", "").strip().lower()
        price_str = row.get("price", "").strip()
        try:
            current_price_aud = float(price_str) if price_str else None
        except ValueError:
            current_price_aud = None
        # 削除済みは除外、停止中(inactive)も対象に含める
        if asin and len(asin) == 10 and sku and asin not in seen and item_status != "deleted":
            seen.add(asin)
            listings.append({
                "asin": asin,
                "sku": sku,
                "status": item_status,
                "current_price_aud": current_price_aud,
            })
            if item_status == "active":
                active_count += 1
            else:
                inactive_count += 1

    logger.info("[price_update] 出品取得完了: %d件（active=%d, inactive=%d）",
                len(listings), active_count, inactive_count)
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
                # 競合価格（複数出品者いる場合のみ返る）
                price_jpy = None
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            price_jpy = int(float(amount))
                        break

                # NumberOfOfferListings で実際の在庫有無を判定
                # Count > 0 → JP に出品あり（競合価格なしでも在庫あり）
                # Count = 0 → JP 在庫切れ
                offer_listings = (
                    item.get("Product", {})
                    .get("CompetitivePricing", {})
                    .get("NumberOfOfferListings", [])
                )
                new_count = sum(
                    ol.get("Count", 0) for ol in offer_listings
                    if (ol.get("condition") or "").lower() in ("new", "new_new")
                )
                in_stock = new_count > 0

                result[asin] = (price_jpy, in_stock)
        except SellingApiException as e:
            logger.warning("[price_update] バッチエラー (%s...): %s", batch[0], e)
        time.sleep(_JP_INTERVAL)

    logger.info("[price_update] JP価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 3. AU 競合価格一括取得
# ─────────────────────────────────────────────

def get_au_competitor_prices_bulk(asins: list) -> dict:
    """20件バッチで AU 競合価格 {asin: price_aud} を返す（自分以外の最安値）"""
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    batch_size = 20
    total = len(asins)

    for i in range(0, total, batch_size):
        batch = asins[i : i + batch_size]
        if i % 200 == 0:
            logger.info("[price_update] AU競合価格取得中: %d/%d", i, total)
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
                        # belongsToRequester=True は自分の出品なのでスキップ
                        if cp.get("belongsToRequester"):
                            continue
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            result[asin] = float(amount)
                        break
        except SellingApiException as e:
            logger.warning("[price_update] AU価格バッチエラー (%s...): %s", batch[0], e)
        time.sleep(_AU_PRICE_INTERVAL)

    logger.info("[price_update] AU競合価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 4. AU 価格更新 / 停止 / 再出品
# ─────────────────────────────────────────────

def update_au_prices(listings: list, jp_prices: dict, au_comp_prices: dict, exchange_rate: float, seller_id: str):
    """
    JP価格をもとに AU 出品価格を更新 or 停止 or 再出品する。

    ★ 新ロジック:
    - 他セラーが販売中 + JP在庫あり + 利益>=MIN_PROFIT_RATE → 再出品（inactive→active）
    - JP在庫なし → 停止
    - 他セラー価格が最低ラインを下回る → 停止（赤字回避）
    """
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    updated = paused = reactivated = failed = 0

    for listing in listings:
        asin = listing["asin"]
        sku = listing["sku"]
        was_inactive = listing.get("status", "active") != "active"
        jp_data = jp_prices.get(asin)

        # JP価格データ自体がない（APIバッチ未取得）→ スキップ（誤停止防止）
        if jp_data is None:
            logger.debug("[price_update] %s: JP価格未取得 → スキップ", asin)
            continue

        jp_price, in_stock = jp_data

        # JP在庫切れ（NumberOfOfferListings Count=0）→ 停止
        if not in_stock:
            try:
                _set_quantity(api, seller_id, sku, 0)
                paused += 1
                logger.info("[price_update] %s: JP在庫切れ(Count=0) → 停止", asin)
            except Exception as e:
                logger.warning("[price_update] %s: 停止失敗 - %s", asin, e)
                failed += 1
            time.sleep(_AU_INTERVAL)
            continue

        # JP在庫あり・競合価格なし（独占出品）→ 価格更新せずアクティブ維持
        if not jp_price:
            logger.debug("[price_update] %s: JP独占出品（競合価格なし）→ 現在価格維持", asin)
            continue

        # 最低利益ライン（赤字にならない最安値）
        min_price = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate)

        # AU競合価格があればそれを使う、なければ現在価格を維持（下げない）
        comp_price = au_comp_prices.get(asin)
        if comp_price:
            if comp_price < min_price:
                # 競合が安すぎて利益出ない → 停止
                try:
                    _set_quantity(api, seller_id, sku, 0)
                    paused += 1
                    logger.info("[price_update] %s: 競合AU$%.2f < 最低ライン$%.2f → 停止",
                                asin, comp_price, min_price)
                except Exception as e:
                    logger.warning("[price_update] %s: 停止失敗 - %s", asin, e)
                    failed += 1
                time.sleep(_AU_INTERVAL)
                continue
            final_price = comp_price  # 競合と同額で出品
        else:
            # ★ 競合なし: 現在価格が最低ラインより高ければ維持（独占状態の利益を守る）
            current_price = float(listing.get("current_price_aud") or 0)
            if current_price >= min_price:
                final_price = current_price
                logger.debug("[price_update] %s: 競合なし → 現在価格維持 AU$%.2f",
                             asin, final_price)
            else:
                final_price = min_price   # 未設定 or 安すぎ → 最低ラインで出品
                logger.debug("[price_update] %s: 競合なし → 最低ライン AU$%.2f",
                             asin, final_price)

        # Amazon Fair Pricing Policy: JP基準価格の MAX_FAIR_PRICE_RATIO 倍を超える価格は設定しない
        jp_ref_aud = jp_price * exchange_rate
        if final_price > jp_ref_aud * config.MAX_FAIR_PRICE_RATIO:
            try:
                _set_quantity(api, seller_id, sku, 0)
                paused += 1
                logger.info(
                    "[price_update] %s: フェアプライシング上限超過 AU$%.2f > AU$%.2f×%.1f → 停止",
                    asin, final_price, jp_ref_aud, config.MAX_FAIR_PRICE_RATIO,
                )
            except Exception as e:
                logger.warning("[price_update] %s: 停止失敗 - %s", asin, e)
                failed += 1
            time.sleep(_AU_INTERVAL)
            continue

        # 利益確認ログ
        result = calc_profit(
            asin=asin, title="", jp_price_jpy=jp_price,
            au_price_aud=final_price, exchange_rate=exchange_rate,
        )
        try:
            _patch_price_and_quantity(api, seller_id, sku, final_price)
            if was_inactive:
                # ★ 停止中 → 再出品（他セラーが販売開始 or JP在庫復活）
                reactivated += 1
                logger.info("[price_update] %s: 再出品 JP¥%d → AU$%.2f (粗利率%.1f%%, 競合$%s)",
                            asin, jp_price, final_price, result.profit_rate,
                            f"{comp_price:.2f}" if comp_price else "なし")
            else:
                updated += 1
                logger.debug("[price_update] %s: 価格更新 JP¥%d → AU$%.2f (粗利率%.1f%%, 競合$%s)",
                             asin, jp_price, final_price, result.profit_rate,
                             f"{comp_price:.2f}" if comp_price else "なし")
        except Exception as e:
            logger.warning("[price_update] %s: 価格更新失敗 - %s", asin, e)
            failed += 1

        time.sleep(_AU_INTERVAL)

    logger.info(
        "[price_update] 完了: 更新 %d件 / 再出品 %d件 / 停止 %d件 / 失敗 %d件",
        updated, reactivated, paused, failed,
    )
    return updated, paused, failed, reactivated


def _set_quantity(api, seller_id, sku, quantity):
    body = {
        "productType": "PRODUCT",
        "patches": [{
            "op": "replace",
            "path": "/attributes/fulfillment_availability",
            "value": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": quantity,
                "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                "marketplace_id": config.MARKETPLACE_AU,
            }],
        }],
    }
    api.patch_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[config.MARKETPLACE_AU], body=body,
    )


def _patch_price_and_quantity(api, seller_id, sku, price_aud):
    """
    価格と数量を PATCH で更新する。
    PUT（put_listings_item）と違い、inactive な出品にも適用できる。
    reactivate_listing.py と同じアプローチ。
    """
    body = {
        "productType": "PRODUCT",
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/fulfillment_availability",
                "value": [{
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": 1,
                    "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                    "marketplace_id": config.MARKETPLACE_AU,
                }],
            },
            {
                "op": "replace",
                "path": "/attributes/purchasable_offer",
                "value": [{
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                    "marketplace_id": config.MARKETPLACE_AU,
                }],
            },
        ],
    }
    api.patch_listings_item(
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

    # 1. 自分の出品一覧取得（active + inactive 含む）
    listings = get_my_au_listings()
    if not listings:
        logger.info("[price_update] 出品なし。終了")
        return

    # 2. JP価格・AU競合価格を取得
    asins = [l["asin"] for l in listings]
    logger.info("[price_update] JP価格確認: %d件（約%.0f分）", len(asins), len(asins) / 20 * _JP_INTERVAL / 60)
    jp_prices = get_jp_prices_bulk(asins)

    logger.info("[price_update] AU競合価格確認: %d件（約%.0f分）", len(asins), len(asins) / 20 * _AU_PRICE_INTERVAL / 60)
    au_comp_prices = get_au_competitor_prices_bulk(asins)

    # 3. AU価格更新 / 停止 / 再出品
    updated, paused, failed, reactivated = update_au_prices(
        listings, jp_prices, au_comp_prices, exchange_rate, seller_id
    )
    notify_price_update_summary(updated, paused, failed, reactivated=reactivated)


if __name__ == "__main__":
    main()

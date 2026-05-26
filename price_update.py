"""
出品中の全商品について JP 価格・AU 競合価格を確認し、AU 出品価格を自動更新するスクリプト。

価格決定ロジック:
  1. JP在庫なし                    → 出品削除 + DB候補戻し（再発掘待ち）
  2. JP在庫あり + AU競合価格あり
       競合価格 >= 最低利益ライン   → 競合価格で出品（利益最大化）
       競合価格 <  最低利益ライン   → 出品削除 + DB候補戻し（赤字回避）
  3. JP在庫あり + AU競合価格なし   → 最低利益ラインで出品

  ★ inactive は作らない。条件悪化 → 即削除 → DB候補戻し → 次回発掘で再出品。
"""
import csv
import gzip
import io
import sqlite3
import sys
import time
from datetime import date as _date

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from db.database import update_listing_price, mark_listing_deleted
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
    """FBMセラーのみの最安値 {asin: price_aud} を返す（FBA除外・自分除外）"""
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    my_seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
    result = {}
    total = len(asins)

    for i, asin in enumerate(asins):
        if i % 50 == 0:
            logger.info("[price_update] AU FBM価格取得中: %d/%d", i, total)
        try:
            resp = api.get_item_offers(asin, ItemCondition="New")
            offers = (resp.payload or {}).get("Offers", [])
            fbm_prices = []
            for offer in offers:
                if offer.get("IsFulfilledByAmazon"):
                    continue  # FBA除外
                if offer.get("SellerId") == my_seller_id:
                    continue  # 自分除外
                amount = offer.get("ListingPrice", {}).get("Amount")
                if amount:
                    fbm_prices.append(float(amount))
            if fbm_prices:
                result[asin] = min(fbm_prices)
        except SellingApiException as e:
            logger.warning("[price_update] AU FBM価格エラー %s: %s", asin, e)
        time.sleep(_AU_PRICE_INTERVAL)

    logger.info("[price_update] AU FBM価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 4. DB候補戻し / 削除ヘルパー
# ─────────────────────────────────────────────

def _demote_to_candidate(asin: str, reason: str):
    """削除した出品を arbitrage.db の candidate に戻す（再発掘待ち）"""
    try:
        with sqlite3.connect(config.DB_PATH) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO asin_candidates (asin, first_seen, status) VALUES (?, ?, 'candidate')",
                (asin, str(_date.today()))
            )
            conn.execute(
                "UPDATE asin_candidates SET status='candidate', skip_reason=?, last_checked=NULL WHERE asin=?",
                (reason, asin)
            )
    except Exception as e:
        logger.warning("[price_update] %s: DB候補戻し失敗 - %s", asin, e)


def _delete_and_demote(api, seller_id: str, sku: str, asin: str, reason: str):
    """出品を削除 + DB を candidate に戻す + listings テーブルを 'deleted' に更新"""
    api.delete_listings_item(
        sellerId=seller_id,
        sku=sku,
        marketplaceIds=[config.MARKETPLACE_AU],
    )
    _demote_to_candidate(asin, reason)
    try:
        mark_listing_deleted(sku)
    except Exception as _e:
        logger.debug("[price_update] %s: DB削除マーク失敗（継続） - %s", sku, _e)


# ─────────────────────────────────────────────
# 5. AU 価格更新 / 削除
# ─────────────────────────────────────────────

def _load_weight_map() -> dict:
    """asin_candidates テーブルから {asin: weight_kg} を取得する"""
    try:
        with sqlite3.connect(config.DB_PATH) as conn:
            rows = conn.execute(
                "SELECT asin, weight_kg FROM asin_candidates WHERE weight_kg IS NOT NULL"
            ).fetchall()
        wmap = {r[0]: float(r[1]) for r in rows if r[1] is not None}
        logger.info("[price_update] 重量DB読込: %d件", len(wmap))
        return wmap
    except Exception as e:
        logger.warning("[price_update] 重量DBロード失敗（継続）: %s", e)
        return {}


def update_au_prices(listings: list, jp_prices: dict, au_comp_prices: dict, exchange_rate: float, seller_id: str):
    """
    JP価格をもとに AU 出品価格を更新 or 削除する。
    条件悪化 → deleteListingsItem + DB候補戻し → 次回 recheck で再出品。
    inactive は作らない。

    min_price 計算は asin_candidates に保存済みの実重量を優先する。
    重量未取得の場合は DEFAULT_WEIGHT_KG（config.py）にフォールバック。
    """
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    # asin_candidates から実重量を一括取得（DB未登録は None → DEFAULT_WEIGHT_KG）
    weight_map = _load_weight_map()

    updated = failed = 0
    deleted_no_stock = 0    # JP在庫なし → 削除
    deleted_too_cheap = 0   # 競合価格が利益ラインを下回る → 削除
    deleted_fair = 0        # フェアプライシング上限超過 → 削除
    sole_seller = 0         # AU独占（競合なし） → 自動でFeatured Offer獲得
    buybox_win = 0          # 競合あり・アンダーカット出品 → Featured Offer狙い

    for listing in listings:
        asin = listing["asin"]
        sku = listing["sku"]
        jp_data = jp_prices.get(asin)

        # JP価格データ自体がない（APIバッチ未取得）→ スキップ（誤削除防止）
        if jp_data is None:
            logger.debug("[price_update] %s: JP価格未取得 → スキップ", asin)
            continue

        jp_price, in_stock = jp_data

        # JP在庫切れ → 出品維持（削除しない。競合在庫切れ待ち戦略と同じ）
        if not in_stock:
            logger.info("[price_update] %s: JP在庫切れ → 出品維持（削除しない）", asin)
            deleted_no_stock += 1  # カウントのみ

        # JP価格なし → 最低出品価格フロアを下回っていれば引き上げ・それ以外は維持
        if not jp_price:
            current_price = float(listing.get("current_price_aud") or 0)
            if current_price < config.MIN_AU_LISTING_PRICE:
                final_price = config.MIN_AU_LISTING_PRICE
                logger.info("[price_update] %s: JP価格なし・現在AU$%.2f < フロアAU$%.2f → 引き上げ",
                            asin, current_price, config.MIN_AU_LISTING_PRICE)
                try:
                    _patch_price_and_quantity(api, seller_id, sku, final_price)
                    updated += 1
                except Exception as e:
                    logger.warning("[price_update] %s: フロア価格更新失敗 - %s", asin, e)
                    failed += 1
                time.sleep(_AU_INTERVAL)
            else:
                logger.debug("[price_update] %s: JP価格なし・現在AU$%.2f → 維持", asin, current_price)
            continue

        # 最低利益ライン（赤字にならない最安値）
        # asin_candidates に実重量があればそれを使う。なければ DEFAULT_WEIGHT_KG にフォールバック。
        weight_kg = weight_map.get(asin)  # None → get_shipping_jpy が DEFAULT_WEIGHT_KG を使用

        # 重量超過チェック: 実重量が判明していて MAX_LISTING_WEIGHT_KG 以上 → 即削除
        if weight_kg is not None and weight_kg >= config.MAX_LISTING_WEIGHT_KG:
            try:
                _delete_and_demote(api, seller_id, sku, asin, f"重量超過_{weight_kg:.2f}kg")
                deleted_no_stock += 1
                logger.info("[price_update] %s: 重量%.2fkg >= 上限%.1fkg → 削除",
                            asin, weight_kg, config.MAX_LISTING_WEIGHT_KG)
            except Exception as e:
                logger.warning("[price_update] %s: 重量超過削除失敗 - %s", asin, e)
                failed += 1
            time.sleep(_AU_INTERVAL)
            continue

        min_price = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate, weight_kg=weight_kg)

        comp_price = au_comp_prices.get(asin)
        if comp_price:
            if comp_price < min_price:
                # 競合が安すぎて利益出ない → min_price で維持（削除しない）
                # 理由: JPの在庫は頻繁に切れる。競合が消えた瞬間に売れるよう出品を維持する。
                # 削除→再出品サイクルは件数を減らすだけ。
                final_price = min_price
                deleted_too_cheap += 1  # カウントのみ（削除はしない）
                logger.info("[price_update] %s: 競合AU$%.2f < 最低ライン$%.2f → min_price維持（削除しない）",
                            asin, comp_price, min_price)
            elif comp_price < min_price * config.BUYBOX_MIN_GAP_RATIO:
                # 競合が利益ラインのすぐ上（ダンパー） → min_price 維持
                final_price = min_price
                logger.debug("[price_update] %s: 競合AU$%.2f < min×%.2f → ダンパー無視・min_price AU$%.2f 維持",
                             asin, comp_price, config.BUYBOX_MIN_GAP_RATIO, min_price)
                sole_seller += 1
            else:
                undercut = round(comp_price * (1 - config.BUYBOX_UNDERCUT_RATE), 2)
                final_price = max(undercut, min_price)
                buybox_win += 1
        else:
            current_price = float(listing.get("current_price_aud") or 0)
            sole_seller += 1
            final_price = current_price if current_price >= min_price else min_price

        # Amazon Fair Pricing Policy: JP基準価格の MAX_FAIR_PRICE_RATIO 倍を超える価格は削除
        jp_ref_aud = jp_price * exchange_rate
        if final_price > jp_ref_aud * config.MAX_FAIR_PRICE_RATIO:
            try:
                _delete_and_demote(api, seller_id, sku, asin, "フェアプライシング上限")
                deleted_fair += 1
                logger.info(
                    "[price_update] %s: フェアプライシング上限超過 AU$%.2f > AU$%.2f×%.1f → 削除・候補DB戻し",
                    asin, final_price, jp_ref_aud, config.MAX_FAIR_PRICE_RATIO,
                )
            except Exception as e:
                logger.warning("[price_update] %s: 削除失敗 - %s", asin, e)
                failed += 1
            time.sleep(_AU_INTERVAL)
            continue

        result = calc_profit(
            asin=asin, title="", jp_price_jpy=jp_price,
            au_price_aud=final_price, exchange_rate=exchange_rate,
        )
        try:
            _patch_price_and_quantity(api, seller_id, sku, final_price)
            updated += 1
            # DB の listings テーブルを即時更新（listings_sync.py の代替として蓄積）
            try:
                update_listing_price(sku, final_price, status="active")
            except Exception as _e:
                logger.debug("[price_update] %s: DB価格更新失敗（継続） - %s", sku, _e)
            if comp_price:
                logger.debug("[price_update] %s: 価格更新 JP¥%d → AU$%.2f [FO狙い -%.1f%% 競合$%.2f]",
                             asin, jp_price, final_price,
                             (comp_price - final_price) / comp_price * 100, comp_price)
            else:
                logger.debug("[price_update] %s: 価格更新 JP¥%d → AU$%.2f [独占 粗利率%.1f%%]",
                             asin, jp_price, final_price, result.profit_rate)
        except Exception as e:
            logger.warning("[price_update] %s: 価格更新失敗 - %s", asin, e)
            failed += 1

        time.sleep(_AU_INTERVAL)

    deleted = deleted_no_stock + deleted_too_cheap + deleted_fair
    featured_offer_est = sole_seller + buybox_win
    logger.info(
        "[price_update] 完了: 更新 %d件 / 削除 %d件 (在庫なし%d / 赤字%d / FP%d) / 失敗 %d件",
        updated, deleted, deleted_no_stock, deleted_too_cheap, deleted_fair, failed,
    )
    logger.info(
        "[price_update] Featured Offer推定: 独占 %d件 + 競合アンダーカット %d件 = 計 %d件",
        sole_seller, buybox_win, featured_offer_est,
    )
    return updated, deleted, failed, 0, sole_seller, buybox_win, deleted_no_stock, deleted_too_cheap, deleted_fair


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
    updated, paused, failed, reactivated, sole_seller, buybox_win, paused_no_stock, paused_too_cheap, paused_fair = update_au_prices(
        listings, jp_prices, au_comp_prices, exchange_rate, seller_id
    )
    notify_price_update_summary(
        updated, paused, failed,
        reactivated=reactivated,
        sole_seller=sole_seller,
        buybox_win=buybox_win,
        paused_no_stock=paused_no_stock,
        paused_too_cheap=paused_too_cheap,
        paused_fair=paused_fair,
    )


if __name__ == "__main__":
    main()

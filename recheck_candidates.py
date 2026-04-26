"""
候補DB（asin_candidates）に蓄積されたASINを再チェックして条件が整ったものを出品する。

catalog_discover.py が「今日は利益が出なかった」ASINをDBに保存し、
このスクリプトが毎日再チェックして条件を満たしたら自動出品する。

フロー:
  1. DBから未出品候補を最大N件取得（今日未チェック分優先）
  2. JP価格・在庫確認（バッチ20件）
  3. AU競合価格確認（バッチ20件）
  4. 利益ライン確認
  5. NGワード・認証チェック
  6. 出品 → DBステータス更新

使い方:
  python recheck_candidates.py            # 最大500件
  python recheck_candidates.py --max 1000
  python recheck_candidates.py --dry-run
  python recheck_candidates.py --stats    # DB統計表示のみ
"""
import argparse
import os
import sys
import time

from sp_api.api import Products, ListingsItems, CatalogItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price
from utils.candidates_db import (
    init_db, get_candidates, update_candidate, get_stats,
    mark_listed_as_candidate,
    STATUS_CANDIDATE, STATUS_LISTED, STATUS_NG, STATUS_RESTRICTED,
)
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP_CREDS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

MARKETPLACE_AU = config.MARKETPLACE_AU
MARKETPLACE_JP = config.MARKETPLACE_JP
BATCH_SIZE     = 20
JP_INTERVAL    = 2.1   # 0.5 req/s
AU_INTERVAL    = 2.1
PATCH_INTERVAL = 0.3   # 5 req/s
RESTRICTION_INTERVAL = 1.1  # 1 req/s


# ─────────────────────────────────────────────
# 価格取得（catalog_discover と同じロジック）
# ─────────────────────────────────────────────

def _get_jp_prices(asins: list) -> dict:
    """JP価格一括取得 {asin: (price_jpy or None, in_stock)}"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    result = {}
    for i in range(0, len(asins), BATCH_SIZE):
        batch = asins[i:i + BATCH_SIZE]
        if i % 200 == 0:
            logger.info("[recheck] JP価格取得中: %d/%d", i, len(asins))
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
                price = None
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            price = int(float(amount))
                        break
                offer_listings = (
                    item.get("Product", {})
                    .get("CompetitivePricing", {})
                    .get("NumberOfOfferListings", [])
                )
                in_stock = sum(
                    ol.get("Count", 0) for ol in offer_listings
                    if (ol.get("condition") or "").lower() in ("new", "new_new")
                ) > 0
                result[asin] = (price, in_stock)
        except SellingApiException as e:
            logger.warning("[recheck] JP価格バッチエラー: %s", e)
        time.sleep(JP_INTERVAL)
    return result


def _get_au_prices(asins: list) -> dict:
    """AU競合価格一括取得 {asin: min_price_aud}"""
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    for i in range(0, len(asins), BATCH_SIZE):
        batch = asins[i:i + BATCH_SIZE]
        if i % 200 == 0:
            logger.info("[recheck] AU価格取得中: %d/%d", i, len(asins))
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
                            result[asin] = float(amount)
                        break
        except SellingApiException as e:
            logger.warning("[recheck] AU価格バッチエラー: %s", e)
        time.sleep(AU_INTERVAL)
    return result


def _check_ng(title: str) -> tuple:
    """NGワードチェック (is_ng, matched_word)"""
    import json
    _path = os.path.join(os.path.dirname(__file__), "ng_words.json")
    try:
        with open(_path, encoding="utf-8") as f:
            data = json.load(f)
        ng_list = []
        for words in data.values():
            if isinstance(words, list):
                ng_list.extend([w.lower() for w in words])
        title_lower = title.lower()
        for word in ng_list:
            if word in title_lower:
                return True, word
    except Exception:
        pass
    return False, ""


def _check_restriction(asin: str, seller_id: str) -> tuple:
    """ListingsRestrictions API で出品可否確認 (is_restricted, reason_code)"""
    try:
        from sp_api.api import ListingsRestrictions
        api = ListingsRestrictions(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        resp = api.get_listings_restrictions(
            asin=asin,
            sellerId=seller_id,
            marketplaceIds=[MARKETPLACE_AU],
            conditionType="new_new",
        )
        restrictions = (resp.payload or {}).get("restrictions", [])
        if not restrictions:
            return False, ""
        for r in restrictions:
            for reason in (r.get("reasons") or []):
                return True, reason.get("reasonCode", "RESTRICTED")
        return True, "RESTRICTED"
    except Exception as e:
        logger.debug("[recheck] ListingsRestrictions エラー %s: %s", asin, e)
        return False, ""


def _list_item(api, seller_id: str, asin: str, price_aud: float) -> tuple:
    """新規出品 PUT + PATCH (ok, sku_or_error)"""
    sku = f"{config.SKU_PREFIX}{asin}"
    put_body = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [{"value": asin, "marketplace_id": MARKETPLACE_AU}],
            "condition_type": [{"value": "new_new", "marketplace_id": MARKETPLACE_AU}],
            "fulfillment_availability": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": 1,
                "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                "marketplace_id": MARKETPLACE_AU,
            }],
            "purchasable_offer": [{
                "currency": "AUD",
                "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                "marketplace_id": MARKETPLACE_AU,
            }],
        },
    }
    resp = api.put_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[MARKETPLACE_AU], body=put_body,
    )
    if resp.payload.get("status") not in ("ACCEPTED", "VALID"):
        issues = resp.payload.get("issues", [])
        return False, "; ".join(i.get("message", "") for i in issues)
    time.sleep(10)
    patch_body = {
        "productType": "PRODUCT",
        "patches": [
            {"op": "replace", "path": "/attributes/fulfillment_availability",
             "value": [{"fulfillment_channel_code": "DEFAULT", "quantity": 1,
                        "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                        "marketplace_id": MARKETPLACE_AU}]},
            {"op": "replace", "path": "/attributes/purchasable_offer",
             "value": [{"currency": "AUD",
                        "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                        "marketplace_id": MARKETPLACE_AU}]},
        ],
    }
    try:
        api.patch_listings_item(
            sellerId=seller_id, sku=sku,
            marketplaceIds=[MARKETPLACE_AU], body=patch_body,
        )
    except Exception as e:
        logger.warning("[recheck] PATCH失敗（PUTは成功） %s: %s", sku, e)
    return True, sku


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="候補ASIN再チェック＆自動出品")
    parser.add_argument("--max",     type=int, default=500, help="再チェック上限（デフォルト500）")
    parser.add_argument("--dry-run", action="store_true",   help="出品しない確認モード")
    parser.add_argument("--stats",   action="store_true",   help="DB統計を表示して終了")
    args = parser.parse_args()

    init_db()

    if args.stats:
        stats = get_stats()
        print("\n=== 候補DB統計 ===")
        total = sum(stats.values())
        for s, c in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"  {s:12s}: {c:6,}件")
        print(f"  {'合計':12s}: {total:6,}件")
        return

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[recheck] AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    # DB統計
    stats = get_stats()
    total_candidates = stats.get(STATUS_CANDIDATE, 0)
    logger.info("[recheck] 候補DB統計: %s", stats)
    logger.info("[recheck] candidate %d件中 最大%d件を再チェック", total_candidates, args.max)

    if total_candidates == 0:
        logger.info("[recheck] 候補なし。終了")
        return

    exchange_rate = get_jpy_to_aud()
    logger.info("[recheck] 為替レート: 1 JPY = %.6f AUD", exchange_rate)

    # 候補取得（今日未チェック分）
    candidates = get_candidates(status=STATUS_CANDIDATE, limit=args.max, skip_checked_today=True)
    if not candidates:
        logger.info("[recheck] 今日チェック済みか候補なし。終了")
        return

    asins = [c["asin"] for c in candidates]
    cand_map = {c["asin"]: c for c in candidates}
    logger.info("[recheck] 対象: %d件", len(asins))

    # ── JP価格確認 ──────────────────────────────────────────────────
    jp_prices = _get_jp_prices(asins)
    in_stock_asins = [a for a in asins if jp_prices.get(a, (None, False))[1]]

    # JP在庫なし → 記録してスキップ
    for asin in asins:
        if asin not in in_stock_asins:
            update_candidate(asin, jp_price=jp_prices.get(asin, (None, None))[0],
                             skip_reason="jp_no_stock")

    logger.info("[recheck] JP在庫あり: %d件 / なし: %d件",
                len(in_stock_asins), len(asins) - len(in_stock_asins))

    if not in_stock_asins:
        return

    # ── AU価格確認 ──────────────────────────────────────────────────
    au_prices = _get_au_prices(in_stock_asins)

    # ── 利益チェック → 出品 ─────────────────────────────────────────
    listings_api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    listed = skipped_profit = skipped_ng = skipped_restricted = failed = 0

    for asin in in_stock_asins:
        jp_price = jp_prices.get(asin, (None, False))[0]
        if not jp_price:
            continue

        weight_kg  = cand_map[asin].get("weight_kg")
        comp_price = au_prices.get(asin)
        min_line   = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate,
                                           weight_kg=weight_kg)

        if comp_price is None or comp_price < min_line:
            update_candidate(asin, jp_price=jp_price, au_price=comp_price,
                             skip_reason="unprofitable")
            skipped_profit += 1
            continue

        # タイトル・重量（DB未取得なら Catalog APIで取得）
        title     = cand_map[asin].get("title") or ""
        if not title:
            try:
                cat_api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
                cat_resp = cat_api.get_catalog_item(
                    asin, marketplaceIds=[MARKETPLACE_JP],
                    includedData=["summaries", "dimensions"],
                )
                payload   = cat_resp.payload or {}
                title     = (payload.get("summaries") or [{}])[0].get("itemName", "") or ""
                weight_kg = None
                try:
                    dims = payload.get("dimensions") or []
                    for dim in dims:
                        w = dim.get("weight") or {}
                        v = w.get("value")
                        u = (w.get("unit") or "").lower()
                        if v is None:
                            continue
                        v = float(v)
                        if "kilogram" in u or u == "kg":
                            weight_kg = v
                        elif "gram" in u:
                            weight_kg = v / 1000.0
                        elif "pound" in u:
                            weight_kg = v * 0.453592
                        break
                except Exception:
                    pass
            except Exception:
                pass

        # NGワードチェック
        if title:
            is_ng, ng_word = _check_ng(title)
            if is_ng:
                logger.info("[recheck] %s: NGワード「%s」→ 永久スキップ", asin, ng_word)
                update_candidate(asin, title=title, status=STATUS_NG,
                                 skip_reason=f"ng:{ng_word}")
                skipped_ng += 1
                continue

        # 認証チェック
        is_restricted, restriction_code = _check_restriction(asin, seller_id)
        time.sleep(RESTRICTION_INTERVAL)
        if is_restricted:
            logger.info("[recheck] %s: 出品不可 [%s] → スキップ", asin, restriction_code)
            update_candidate(asin, status=STATUS_RESTRICTED,
                             skip_reason=f"restricted:{restriction_code}")
            skipped_restricted += 1
            continue

        # 出品
        final_price = comp_price
        log_msg = (f"{asin}: JP¥{jp_price:,} → AU${final_price:.2f} "
                   f"(最低ライン${min_line:.2f})")

        if args.dry_run:
            logger.info("[recheck][DRY-RUN] 出品予定 %s", log_msg)
            update_candidate(asin, jp_price=jp_price, au_price=comp_price,
                             title=title, weight_kg=weight_kg, skip_reason="dry_run")
            listed += 1
            continue

        try:
            ok, msg = _list_item(listings_api, seller_id, asin, final_price)
            if ok:
                logger.info("[recheck] 出品完了 %s (SKU: %s)", log_msg, msg)
                update_candidate(asin, status=STATUS_LISTED, listed_sku=msg,
                                 jp_price=jp_price, au_price=final_price,
                                 title=title, weight_kg=weight_kg)
                listed += 1
            else:
                logger.warning("[recheck] 出品失敗 %s: %s", asin, msg)
                update_candidate(asin, skip_reason=f"list_failed:{msg[:60]}")
                failed += 1
        except Exception as e:
            logger.warning("[recheck] 出品例外 %s: %s", asin, e)
            failed += 1

        time.sleep(PATCH_INTERVAL)

    # ── メール通知 ──────────────────────────────────────────────────
    dry_label = "[DRY-RUN] " if args.dry_run else ""
    final_stats = get_stats()
    subject = (
        f"[SP-API] {dry_label}候補再チェック: "
        f"出品{listed}件 / 候補DB {final_stats.get(STATUS_CANDIDATE, 0):,}件残"
    )
    body = "\n".join([
        f"=== {dry_label}候補ASIN 再チェック 完了 ===",
        "",
        f"候補DB総数（candidate）: {total_candidates:,}件",
        f"今回チェック:            {len(asins):,}件",
        f"出品{'予定' if args.dry_run else '完了'}:                {listed:,}件",
        f"スキップ（利益不足）:    {skipped_profit:,}件",
        f"スキップ（NGワード）:    {skipped_ng:,}件",
        f"スキップ（認証不可）:    {skipped_restricted:,}件",
        f"失敗:                    {failed:,}件",
        "",
        "--- 候補DB 全体統計 ---",
    ] + [f"  {s:12s}: {c:,}件" for s, c in sorted(final_stats.items(), key=lambda x: -x[1])])
    send_email(subject=subject, body=body)
    logger.info("[recheck] 完了: 出品%d件 / 候補残%d件",
                listed, final_stats.get(STATUS_CANDIDATE, 0))


if __name__ == "__main__":
    main()

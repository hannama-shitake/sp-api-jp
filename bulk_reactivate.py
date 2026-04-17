"""
停止中（inactive）の全出品を一括チェックし、条件を満たすものを再出品するスクリプト。

再出品ロジック:
  1. JP在庫なし                              → スキップ（停止のまま）
  2. AU競合セラー数 < min_sellers (デフォルト3) → スキップ（需要未実証）
  3. JP在庫あり + 競合セラー数 >= min_sellers
       競合最安値 >= 最低利益ライン           → 競合最安値で再出品
       競合最安値 <  最低利益ライン           → スキップ（赤字回避）

★ セラー3人以上＝需要が実証されている商品のみ再出品

使い方:
  python bulk_reactivate.py                      # 実際に再出品（min-sellers=3）
  python bulk_reactivate.py --dry-run            # テスト実行（PATCHしない）
  python bulk_reactivate.py --min-sellers 2      # セラー2人以上に緩和
"""
import csv
import gzip
import io
import os
import sys
import time
import argparse

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

MARKETPLACE_AU = config.MARKETPLACE_AU

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

JP_INTERVAL = 2.1         # Products API JP: 0.5 req/s
AU_PRICE_INTERVAL = 2.1   # Products API AU: 0.5 req/s
AU_PATCH_INTERVAL = 0.3   # ListingsItems: 5 req/s


# ─────────────────────────────────────────────
# 1. AU 出品一覧取得（active + inactive、deleted除外）
# ─────────────────────────────────────────────

def get_my_au_listings() -> list:
    """
    Reports API で自分の AU 出品一覧（停止中含む、削除済み除外）を取得する。
    各要素: {asin, sku, status, price, title}
    """
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[bulk_reactivate] 出品レポートをリクエスト中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]
    logger.info("[bulk_reactivate] レポートID: %s", report_id)

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[bulk_reactivate] レポートステータス: %s (%d/120)", status, attempt + 1)
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
        title = row.get("item-name", "").strip()
        price = row.get("price", "").strip()
        # 削除済みは除外
        if asin and len(asin) == 10 and sku and asin not in seen and item_status != "deleted":
            seen.add(asin)
            listings.append({
                "asin": asin,
                "sku": sku,
                "status": item_status,
                "price": price,
                "title": title,
            })
            if item_status == "active":
                active_count += 1
            else:
                inactive_count += 1

    logger.info(
        "[bulk_reactivate] 出品取得完了: %d件（active=%d, inactive=%d）",
        len(listings), active_count, inactive_count,
    )
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
        batch = asins[i: i + batch_size]
        if i % 200 == 0:
            logger.info("[bulk_reactivate] JP価格取得中: %d/%d", i, total)
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
            logger.warning("[bulk_reactivate] JP価格バッチエラー (%s...): %s", batch[0], e)
        time.sleep(JP_INTERVAL)

    logger.info("[bulk_reactivate] JP価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 3a. AU 競合価格バッチ取得（事前フィルタ用）
# ─────────────────────────────────────────────

def get_au_competitor_prices_bulk(asins: list) -> dict:
    """
    20件バッチで AU 競合価格 {asin: price_aud} を返す（事前フィルタ用）。
    ここで価格が取れたASINのみ次のステップでセラー数チェックを行う。
    """
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    batch_size = 20
    total = len(asins)

    for i in range(0, total, batch_size):
        batch = asins[i: i + batch_size]
        if i % 200 == 0:
            logger.info("[bulk_reactivate] AU競合価格取得中: %d/%d", i, total)
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
                        if cp.get("belongsToRequester"):
                            continue
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            result[asin] = float(amount)
                        break
        except SellingApiException as e:
            logger.warning("[bulk_reactivate] AU競合価格バッチエラー (%s...): %s", batch[0], e)
        time.sleep(AU_PRICE_INTERVAL)

    logger.info("[bulk_reactivate] AU競合価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 3b. AU セラー数カウント（get_item_offers）
# ─────────────────────────────────────────────

def get_au_seller_counts(asins: list) -> dict:
    """
    get_item_offers で各 ASIN の AU 競合セラー数と最安値を返す。
    {asin: {"seller_count": int, "min_price": float or None}}

    事前フィルタ通過済みの候補のみ呼び出すこと（1 ASIN = 1 API コール）。
    Rate limit: 0.5 req/s → 2.1s 間隔
    """
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
    total = len(asins)

    for i, asin in enumerate(asins):
        if i % 50 == 0:
            logger.info("[bulk_reactivate] セラー数確認中: %d/%d", i, total)
        try:
            resp = api.get_item_offers(asin, item_condition="New")
            payload = resp.payload if hasattr(resp, "payload") else {}
            offers = payload.get("Offers", [])

            competitor_offers = [
                o for o in offers
                if o.get("SellerId", "") != my_id
                and o.get("ListingPrice", {}).get("Amount")
            ]
            seller_count = len(competitor_offers)
            if competitor_offers:
                prices = [float(o["ListingPrice"]["Amount"]) for o in competitor_offers]
                min_price = min(prices)
            else:
                min_price = None

            result[asin] = {"seller_count": seller_count, "min_price": min_price}
        except SellingApiException as e:
            logger.warning("[bulk_reactivate] get_item_offers エラー %s: %s", asin, e)
            result[asin] = {"seller_count": 0, "min_price": None}
        time.sleep(AU_PRICE_INTERVAL)

    logger.info("[bulk_reactivate] セラー数確認完了: %d件 (3人以上: %d件)",
                total, sum(1 for v in result.values() if v["seller_count"] >= 3))
    return result


# ─────────────────────────────────────────────
# 4. PATCH で再出品
# ─────────────────────────────────────────────

def _patch_reactivate(api, seller_id: str, sku: str, price_aud: float):
    """
    patch_listings_item で qty=1 + 価格を設定して再出品する。
    PUT（put_listings_item）ではなく PATCH を使うことで inactive な出品にも適用可能。
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
                    "marketplace_id": MARKETPLACE_AU,
                }],
            },
            {
                "op": "replace",
                "path": "/attributes/purchasable_offer",
                "value": [{
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                    "marketplace_id": MARKETPLACE_AU,
                }],
            },
        ],
    }
    api.patch_listings_item(
        sellerId=seller_id,
        sku=sku,
        marketplaceIds=[MARKETPLACE_AU],
        body=body,
    )


# ─────────────────────────────────────────────
# 5. 一括再出品メイン処理
# ─────────────────────────────────────────────

def bulk_reactivate(inactive_listings: list, jp_prices: dict, seller_counts: dict,
                    exchange_rate: float, seller_id: str,
                    min_sellers: int = 3, dry_run: bool = False):
    """
    停止中（inactive）の出品を一括チェックし、条件を満たすものを再出品する。

    ★ セラー数 >= min_sellers の商品のみ再出品（需要実証フィルタ）

    Returns:
        (reactivated_details, skipped_no_stock, skipped_few_sellers, skipped_unprofitable, failed_count)
    """
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    reactivated_details = []
    skipped_no_stock = 0
    skipped_few_sellers = 0
    skipped_unprofitable = 0
    skipped_fair_price = 0
    failed_count = 0

    for listing in inactive_listings:
        asin = listing["asin"]
        sku = listing["sku"]
        title = listing.get("title", "")[:50]

        jp_price, in_stock = jp_prices.get(asin, (None, False))

        # JP在庫なし → スキップ
        if not in_stock or not jp_price:
            skipped_no_stock += 1
            continue

        # 最低利益ライン
        min_line = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate)

        # セラー数チェック（seller_counts に含まれない = 競合なし = スキップ）
        offer_info = seller_counts.get(asin)
        if not offer_info or offer_info["seller_count"] < min_sellers:
            count = offer_info["seller_count"] if offer_info else 0
            logger.debug("[bulk_reactivate] %s: 競合セラー%d人 < %d人 → スキップ",
                         asin, count, min_sellers)
            skipped_few_sellers += 1
            continue

        # 競合最安値が最低利益ラインを下回る → スキップ
        comp_price = offer_info["min_price"]
        if comp_price is None or comp_price < min_line:
            logger.info(
                "[bulk_reactivate] %s: 競合AU$%s < 最低ライン$%.2f → スキップ（赤字）",
                asin, f"{comp_price:.2f}" if comp_price else "?", min_line,
            )
            skipped_unprofitable += 1
            continue

        final_price = comp_price

        # Amazon Fair Pricing Policy: JP基準価格の MAX_FAIR_PRICE_RATIO 倍を超える価格は出品しない
        jp_ref_aud = jp_price * exchange_rate
        if final_price > jp_ref_aud * config.MAX_FAIR_PRICE_RATIO:
            logger.info(
                "[bulk_reactivate] %s: フェアプライシング上限超過 AU$%.2f > AU$%.2f×%.1f → スキップ",
                asin, final_price, jp_ref_aud, config.MAX_FAIR_PRICE_RATIO,
            )
            skipped_fair_price += 1
            continue

        # 利益計算ログ
        result = calc_profit(
            asin=asin, title=title,
            jp_price_jpy=jp_price, au_price_aud=final_price,
            exchange_rate=exchange_rate,
        )

        seller_count = offer_info["seller_count"]
        log_msg = (f"{asin}: JP¥{jp_price:,} → AU${final_price:.2f} "
                   f"(粗利率{result.profit_rate:.1f}%, 競合{seller_count}人)")

        if dry_run:
            logger.info("[bulk_reactivate][DRY-RUN] 再出品予定 %s", log_msg)
            reactivated_details.append({
                "asin": asin, "title": title, "jp_price": jp_price,
                "au_price": final_price, "profit_rate": result.profit_rate,
                "seller_count": seller_count,
            })
            continue

        try:
            _patch_reactivate(api, seller_id, sku, final_price)
            logger.info("[bulk_reactivate] 再出品完了 %s", log_msg)
            reactivated_details.append({
                "asin": asin, "title": title, "jp_price": jp_price,
                "au_price": final_price, "profit_rate": result.profit_rate,
                "seller_count": seller_count,
            })
        except Exception as e:
            logger.warning("[bulk_reactivate] %s: 再出品失敗 - %s", asin, e)
            failed_count += 1

        time.sleep(AU_PATCH_INTERVAL)

    logger.info(
        "[bulk_reactivate] 完了: 再出品 %d件 / JP在庫なし %d件 "
        "/ セラー不足 %d件 / 赤字 %d件 / 価格上限 %d件 / 失敗 %d件",
        len(reactivated_details), skipped_no_stock,
        skipped_few_sellers, skipped_unprofitable, skipped_fair_price, failed_count,
    )
    return reactivated_details, skipped_no_stock, skipped_few_sellers, skipped_unprofitable, skipped_fair_price, failed_count


# ─────────────────────────────────────────────
# 6. メール通知
# ─────────────────────────────────────────────

def build_email(inactive_total: int, reactivated_details: list,
                skipped_no_stock: int, skipped_few_sellers: int,
                skipped_unprofitable: int, skipped_fair_price: int,
                failed_count: int,
                min_sellers: int = 3, dry_run: bool = False) -> tuple:
    """メール件名と本文を生成する"""
    success_count = len(reactivated_details)
    dry_run_label = "[DRY-RUN] " if dry_run else ""

    subject = (
        f"[SP-API] {dry_run_label}一括再出品: "
        f"{success_count}件成功 / {failed_count}件失敗"
    )

    lines = [
        f"=== {dry_run_label}一括再出品 結果サマリー (競合{min_sellers}人以上) ===",
        "",
        f"停止中チェック対象:              {inactive_total}件",
        f"再出品{'予定' if dry_run else '完了'}:                      {success_count}件",
        f"スキップ（JP在庫なし）:          {skipped_no_stock}件",
        f"スキップ（競合{min_sellers}人未満）:         {skipped_few_sellers}件",
        f"スキップ（利益不足）:            {skipped_unprofitable}件",
        f"スキップ（価格上限超過）:        {skipped_fair_price}件",
        f"失敗:                          {failed_count}件",
        "",
    ]

    if reactivated_details:
        lines.append(f"--- 再出品{'予定' if dry_run else '完了'}リスト ({success_count}件) ---")
        for item in reactivated_details:
            lines.append(
                f"  {item['asin']}  JP¥{item['jp_price']:,} → AU${item['au_price']:.2f}"
                f"  粗利率{item['profit_rate']:.1f}%"
                f"  競合{item.get('seller_count', '?')}人"
                f"  {item['title'][:35]}"
            )
        lines.append("")

    body = "\n".join(lines)
    return subject, body


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="停止中の AU 出品を一括チェックし、競合セラーN人以上の商品を再出品する"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="実際にPATCHせず、再出品対象の確認のみ行う",
    )
    parser.add_argument(
        "--min-sellers", type=int, default=3,
        help="再出品に必要な最小競合セラー数（デフォルト: 3）",
    )
    args = parser.parse_args()

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[bulk_reactivate] AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    if args.dry_run:
        logger.info("[bulk_reactivate] *** DRY-RUN モード ***")
    logger.info("[bulk_reactivate] 競合セラー最小数: %d人", args.min_sellers)

    exchange_rate = get_jpy_to_aud()
    logger.info("[bulk_reactivate] 為替レート: 1 JPY = %.6f AUD", exchange_rate)

    # 1. 全出品取得（active + inactive、deleted除外）
    all_listings = get_my_au_listings()
    if not all_listings:
        logger.info("[bulk_reactivate] 出品なし。終了")
        return

    # 2. active / inactive に分離
    inactive_listings = [l for l in all_listings if l["status"] != "active"]
    logger.info("[bulk_reactivate] active: %d件 / inactive: %d件",
                len(all_listings) - len(inactive_listings), len(inactive_listings))

    if not inactive_listings:
        logger.info("[bulk_reactivate] 停止中の出品なし。終了")
        return

    inactive_asins = [l["asin"] for l in inactive_listings]

    # 3. JP価格一括取得（バッチ）
    logger.info("[bulk_reactivate] JP価格確認: %d件（約%.0f分）",
                len(inactive_asins), len(inactive_asins) / 20 * JP_INTERVAL / 60)
    jp_prices = get_jp_prices_bulk(inactive_asins)

    # 4. JP在庫ありのASINだけ次のステップへ（API節約）
    asins_with_stock = [
        a for a in inactive_asins
        if jp_prices.get(a, (None, False))[1]
    ]
    logger.info("[bulk_reactivate] JP在庫あり: %d件 / なし: %d件",
                len(asins_with_stock), len(inactive_asins) - len(asins_with_stock))

    if not asins_with_stock:
        logger.info("[bulk_reactivate] 在庫あり商品なし。終了")
        return

    # 5. AU競合価格バッチ取得（事前フィルタ：競合ありのASINを特定）
    logger.info("[bulk_reactivate] AU競合価格確認: %d件（約%.0f分）",
                len(asins_with_stock), len(asins_with_stock) / 20 * AU_PRICE_INTERVAL / 60)
    au_comp_prices_bulk = get_au_competitor_prices_bulk(asins_with_stock)

    # 競合価格が最低ライン以上のASINのみ item_offers でセラー数を確認
    candidates = []
    for asin in asins_with_stock:
        jp_price, _ = jp_prices.get(asin, (None, False))
        if not jp_price:
            continue
        min_line = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate)
        comp_price = au_comp_prices_bulk.get(asin)
        if comp_price and comp_price >= min_line:
            candidates.append(asin)

    logger.info("[bulk_reactivate] セラー数確認候補: %d件（約%.0f分）",
                len(candidates), len(candidates) * AU_PRICE_INTERVAL / 60)

    # 6. get_item_offers でセラー数と実際の最安値を確認
    seller_counts = get_au_seller_counts(candidates)

    # 7. 一括再出品
    reactivated_details, skipped_no_stock, skipped_few_sellers, skipped_unprofitable, skipped_fair_price, failed_count = bulk_reactivate(
        inactive_listings=inactive_listings,
        jp_prices=jp_prices,
        seller_counts=seller_counts,
        exchange_rate=exchange_rate,
        seller_id=seller_id,
        min_sellers=args.min_sellers,
        dry_run=args.dry_run,
    )

    # 8. メール送信
    subject, body = build_email(
        inactive_total=len(inactive_listings),
        reactivated_details=reactivated_details,
        skipped_no_stock=skipped_no_stock,
        skipped_few_sellers=skipped_few_sellers,
        skipped_unprofitable=skipped_unprofitable,
        skipped_fair_price=skipped_fair_price,
        failed_count=failed_count,
        min_sellers=args.min_sellers,
        dry_run=args.dry_run,
    )
    send_email(subject=subject, body=body)
    print(body)


if __name__ == "__main__":
    main()

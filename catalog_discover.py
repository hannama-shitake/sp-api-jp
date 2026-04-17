"""
JP カタログから新規ASIN を発掘して AU に出品するスクリプト。
既存カタログ(2,778件)を超えて3,000件に向けた新規開拓用。

フロー:
  1. AU 自分の出品一覧取得（Reports API）→ 既知ASIN セット
  2. JP カタログ検索（Catalog Items API, キーワード×ページ）→ 未知ASIN リスト
  3. JP 価格・在庫確認（Products API, バッチ20件）
  4. AU 競合価格確認（Products API, バッチ20件）→ 事前フィルタ
  5. AU セラー数確認（Products API, get_item_offers）→ min_sellers 人以上
  6. 利益確認 → 競合価格 >= 最低利益ライン
  7. AU 新規出品（ListingsItems API, PUT）

使い方:
  python catalog_discover.py                      # 実際に出品（上限300件）
  python catalog_discover.py --dry-run            # テスト実行（出品しない）
  python catalog_discover.py --max-new 100        # 新規出品上限100件
  python catalog_discover.py --min-sellers 3      # 競合セラー3人以上
  python catalog_discover.py --pages-per-kw 3     # キーワードあたりページ数
"""
import csv
import gzip
import io
import os
import sys
import time
import argparse

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems, CatalogItems
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

MARKETPLACE_AU = config.MARKETPLACE_AU
MARKETPLACE_JP = config.MARKETPLACE_JP

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

# JP→AU アービトラージで実績のあるカテゴリのキーワード
# 売れ筋 TOP_ASINS (find_au_sellers.py) から導出した傾向に基づく
SEARCH_KEYWORDS = [
    # フィギュア・コレクタブル
    "S.H.Figuarts",
    "Nendoroid",
    "MAFEX figure",
    "figma",
    "Revoltech",
    "BANDAI figure",
    # ガンプラ・模型
    "MG Gundam",
    "RG Gundam",
    "HG Gundam",
    "BANDAI SPIRITS",
    # トレーディングカード
    "One Piece Card Game",
    "Pokemon card Japanese",
    "Digimon card",
    "Dragon Ball card",
    # カメラ・レンズ
    "Sony mirrorless",
    "Fujifilm lens",
    "Sigma lens Japan",
    # ゲーム・ホビー
    "TAMIYA mini 4WD",
    "Daiwa fishing",
    "Orient Star watch",
]

JP_INTERVAL = 2.1        # Products API JP: 0.5 req/s
AU_PRICE_INTERVAL = 2.1  # Products API AU: 0.5 req/s
CATALOG_INTERVAL = 0.6   # Catalog Items API: 2 req/s (burst 2)
PATCH_INTERVAL = 0.3     # ListingsItems: 5 req/s


# ─────────────────────────────────────────────
# 1. 既存AU出品一覧取得
# ─────────────────────────────────────────────

def get_existing_asins() -> set:
    """
    Reports API で自分の現在のAU出品一覧を取得し、
    既知ASINセット（active + inactive、deleted除外）を返す。
    """
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[catalog_discover] 既存出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[catalog_discover] レポートステータス: %s (%d/120)", status, attempt + 1)
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

    known = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        item_status = row.get("status", "").strip().lower()
        if asin and len(asin) == 10 and item_status != "deleted":
            known.add(asin)

    logger.info("[catalog_discover] 既存ASIN: %d件", len(known))
    return known


# ─────────────────────────────────────────────
# 2. JP カタログ検索 → 未知ASIN 収集
# ─────────────────────────────────────────────

def search_jp_catalog(keywords: list, pages_per_kw: int = 5, existing_asins: set = None) -> list:
    """
    Catalog Items API (2022-04-01) で JP カタログをキーワード検索し、
    既存カタログにない新規 ASIN リストを返す。

    Args:
        keywords: 検索キーワードリスト
        pages_per_kw: キーワードあたりページ数（max 20items/page）
        existing_asins: 除外する既知 ASIN セット

    Returns:
        新規 ASIN リスト
    """
    if existing_asins is None:
        existing_asins = set()

    # JP 認証で Catalog を検索（JP カタログにある商品 = JPで仕入れ可能な候補）
    api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)

    new_asins = []
    seen = set(existing_asins)

    for kw in keywords:
        logger.info("[catalog_discover] カタログ検索: '%s'", kw)
        page_token = None
        for page in range(pages_per_kw):
            try:
                kwargs = {
                    "keywords": kw,
                    "marketplaceIds": [MARKETPLACE_JP],
                    "pageSize": 20,
                }
                if page_token:
                    kwargs["pageToken"] = page_token

                resp = api.search_catalog_items(**kwargs)
                payload = resp.payload if hasattr(resp, "payload") else {}
                items = payload.get("items", [])

                added = 0
                for item in items:
                    asin = item.get("asin", "")
                    if asin and len(asin) == 10 and asin not in seen:
                        seen.add(asin)
                        new_asins.append(asin)
                        added += 1

                pagination = payload.get("pagination", {})
                page_token = pagination.get("nextToken")

                logger.debug("[catalog_discover] '%s' page%d: %d件追加 (累計%d件)",
                             kw, page + 1, added, len(new_asins))

                if not page_token:
                    break  # ページなし

            except SellingApiException as e:
                logger.warning("[catalog_discover] カタログ検索エラー '%s': %s", kw, e)
                break

            time.sleep(CATALOG_INTERVAL)

        time.sleep(CATALOG_INTERVAL)

    logger.info("[catalog_discover] 新規ASIN候補: %d件", len(new_asins))
    return new_asins


# ─────────────────────────────────────────────
# 3. JP 価格一括取得
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
            logger.info("[catalog_discover] JP価格取得中: %d/%d", i, total)
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
            logger.warning("[catalog_discover] JP価格バッチエラー (%s...): %s", batch[0], e)
        time.sleep(JP_INTERVAL)

    in_stock_count = sum(1 for v in result.values() if v[1])
    logger.info("[catalog_discover] JP価格取得完了: %d件中%d件在庫あり", total, in_stock_count)
    return result


# ─────────────────────────────────────────────
# 4. AU 競合価格バッチ取得（事前フィルタ）
# ─────────────────────────────────────────────

def get_au_competitor_prices_bulk(asins: list) -> dict:
    """20件バッチで AU 競合価格 {asin: price_aud} を返す（事前フィルタ用）"""
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    batch_size = 20
    total = len(asins)

    for i in range(0, total, batch_size):
        batch = asins[i: i + batch_size]
        if i % 200 == 0:
            logger.info("[catalog_discover] AU競合価格取得中: %d/%d", i, total)
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
            logger.warning("[catalog_discover] AU競合価格バッチエラー (%s...): %s", batch[0], e)
        time.sleep(AU_PRICE_INTERVAL)

    logger.info("[catalog_discover] AU競合価格取得完了: %d件中%d件取得", total, len(result))
    return result


# ─────────────────────────────────────────────
# 5. AU セラー数確認（get_item_offers）
# ─────────────────────────────────────────────

def get_au_seller_counts(asins: list) -> dict:
    """
    {asin: {"seller_count": int, "min_price": float or None}}
    自分以外の競合セラー数と最安値を返す。
    """
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
    total = len(asins)

    for i, asin in enumerate(asins):
        if i % 50 == 0:
            logger.info("[catalog_discover] セラー数確認中: %d/%d", i, total)
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
            min_price = (
                min(float(o["ListingPrice"]["Amount"]) for o in competitor_offers)
                if competitor_offers else None
            )
            result[asin] = {"seller_count": seller_count, "min_price": min_price}
        except SellingApiException as e:
            logger.warning("[catalog_discover] get_item_offers エラー %s: %s", asin, e)
            result[asin] = {"seller_count": 0, "min_price": None}
        time.sleep(AU_PRICE_INTERVAL)

    three_plus = sum(1 for v in result.values() if v["seller_count"] >= 3)
    logger.info("[catalog_discover] セラー数確認完了: %d件 (3人以上: %d件)", total, three_plus)
    return result


# ─────────────────────────────────────────────
# 6. 新規出品
# ─────────────────────────────────────────────

def list_new_item(api, seller_id: str, asin: str, price_aud: float):
    """PUT で新規 FBM 相乗り出品する（既存SKUがない場合に使用）"""
    sku = f"{config.SKU_PREFIX}{asin}"
    body = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "condition_type": [
                {"value": "new_new", "marketplace_id": MARKETPLACE_AU}
            ],
            "fulfillment_availability": [
                {
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": 1,
                    "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                    "marketplace_id": MARKETPLACE_AU,
                }
            ],
            "purchasable_offer": [
                {
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                    "marketplace_id": MARKETPLACE_AU,
                }
            ],
        },
    }
    resp = api.put_listings_item(
        sellerId=seller_id,
        sku=sku,
        marketplaceIds=[MARKETPLACE_AU],
        body=body,
    )
    status = resp.payload.get("status", "")
    if status in ("ACCEPTED", "VALID"):
        return True, sku
    issues = resp.payload.get("issues", [])
    msg = "; ".join(i.get("message", "") for i in issues)
    return False, msg


# ─────────────────────────────────────────────
# 7. メイン処理
# ─────────────────────────────────────────────

def discover_and_list(
    min_sellers: int = 3,
    max_new: int = 300,
    pages_per_kw: int = 5,
    dry_run: bool = False,
):
    """
    JPカタログ検索 → AU需要確認 → 利益確認 → 新規出品

    Returns:
        (listed_details, skipped_no_stock, skipped_no_au,
         skipped_few_sellers, skipped_unprofitable, failed_count)
    """
    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[catalog_discover] AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    exchange_rate = get_jpy_to_aud()
    logger.info("[catalog_discover] 為替レート: 1 JPY = %.6f AUD", exchange_rate)

    # ── Step 1: 既存ASIN取得 ──────────────────────────────────────
    existing_asins = get_existing_asins()

    # ── Step 2: JPカタログ検索 → 新規ASIN ──────────────────────────
    new_asins = search_jp_catalog(
        keywords=SEARCH_KEYWORDS,
        pages_per_kw=pages_per_kw,
        existing_asins=existing_asins,
    )
    if not new_asins:
        logger.info("[catalog_discover] 新規ASIN候補なし。終了")
        return [], 0, 0, 0, 0, 0

    logger.info("[catalog_discover] 新規ASIN候補: %d件", len(new_asins))

    # ── Step 3: JP価格確認 ──────────────────────────────────────────
    logger.info("[catalog_discover] JP価格確認: %d件 (約%.0f分)",
                len(new_asins), len(new_asins) / 20 * JP_INTERVAL / 60)
    jp_prices = get_jp_prices_bulk(new_asins)

    asins_with_stock = [
        a for a in new_asins
        if jp_prices.get(a, (None, False))[1]
    ]
    skipped_no_stock = len(new_asins) - len(asins_with_stock)
    logger.info("[catalog_discover] JP在庫あり: %d件 / なし: %d件",
                len(asins_with_stock), skipped_no_stock)

    if not asins_with_stock:
        return [], skipped_no_stock, 0, 0, 0, 0

    # ── Step 4: AU競合価格確認（事前フィルタ）──────────────────────
    logger.info("[catalog_discover] AU競合価格確認: %d件 (約%.0f分)",
                len(asins_with_stock), len(asins_with_stock) / 20 * AU_PRICE_INTERVAL / 60)
    au_bulk_prices = get_au_competitor_prices_bulk(asins_with_stock)

    # AUに競合が存在するASINだけをセラー数チェックへ
    au_candidates = []
    for asin in asins_with_stock:
        jp_price, _ = jp_prices.get(asin, (None, False))
        if not jp_price:
            continue
        min_line = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate)
        comp_price = au_bulk_prices.get(asin)
        if comp_price and comp_price >= min_line:
            au_candidates.append(asin)

    skipped_no_au = len(asins_with_stock) - len(au_candidates)
    logger.info("[catalog_discover] セラー数確認候補: %d件 (AU価格なし/利益不足: %d件)",
                len(au_candidates), skipped_no_au)

    if not au_candidates:
        return [], skipped_no_stock, skipped_no_au, 0, 0, 0

    # ── Step 5: AU セラー数確認 ────────────────────────────────────
    logger.info("[catalog_discover] セラー数確認: %d件 (約%.0f分)",
                len(au_candidates), len(au_candidates) * AU_PRICE_INTERVAL / 60)
    seller_counts = get_au_seller_counts(au_candidates)

    # ── Step 6 & 7: 利益確認 → 出品 ──────────────────────────────
    listings_api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    listed_details = []
    skipped_few_sellers = 0
    skipped_unprofitable = 0
    failed_count = 0

    for asin in au_candidates:
        if len(listed_details) + failed_count >= max_new:
            logger.info("[catalog_discover] 上限 %d件 に達しました", max_new)
            break

        jp_price, _ = jp_prices.get(asin, (None, False))
        if not jp_price:
            continue

        min_line = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate)
        offer_info = seller_counts.get(asin, {"seller_count": 0, "min_price": None})

        if offer_info["seller_count"] < min_sellers:
            skipped_few_sellers += 1
            continue

        comp_price = offer_info["min_price"]
        if comp_price is None or comp_price < min_line:
            skipped_unprofitable += 1
            continue

        final_price = comp_price
        result = calc_profit(
            asin=asin, title="",
            jp_price_jpy=jp_price, au_price_aud=final_price,
            exchange_rate=exchange_rate,
        )

        log_msg = (f"{asin}: JP¥{jp_price:,} → AU${final_price:.2f} "
                   f"(粗利率{result.profit_rate:.1f}%, 競合{offer_info['seller_count']}人)")

        if dry_run:
            logger.info("[catalog_discover][DRY-RUN] 出品予定 %s", log_msg)
            listed_details.append({
                "asin": asin, "jp_price": jp_price,
                "au_price": final_price, "profit_rate": result.profit_rate,
                "seller_count": offer_info["seller_count"],
            })
            continue

        try:
            ok, msg = list_new_item(listings_api, seller_id, asin, final_price)
            if ok:
                logger.info("[catalog_discover] 出品完了 %s (SKU: %s)", log_msg, msg)
                listed_details.append({
                    "asin": asin, "jp_price": jp_price, "sku": msg,
                    "au_price": final_price, "profit_rate": result.profit_rate,
                    "seller_count": offer_info["seller_count"],
                })
            else:
                logger.warning("[catalog_discover] 出品失敗 %s: %s", asin, msg)
                failed_count += 1
        except Exception as e:
            logger.warning("[catalog_discover] 出品例外 %s: %s", asin, e)
            failed_count += 1

        time.sleep(PATCH_INTERVAL)

    logger.info(
        "[catalog_discover] 完了: 出品 %d件 / JP在庫なし %d件 "
        "/ AU競合なし・利益不足 %d件 / セラー不足 %d件 / 赤字 %d件 / 失敗 %d件",
        len(listed_details), skipped_no_stock, skipped_no_au,
        skipped_few_sellers, skipped_unprofitable, failed_count,
    )
    return listed_details, skipped_no_stock, skipped_no_au, skipped_few_sellers, skipped_unprofitable, failed_count


def build_email(
    listed_details, skipped_no_stock, skipped_no_au,
    skipped_few_sellers, skipped_unprofitable, failed_count,
    min_sellers: int, dry_run: bool,
) -> tuple:
    success_count = len(listed_details)
    dry_label = "[DRY-RUN] " if dry_run else ""
    subject = (
        f"[SP-API] {dry_label}カタログ発掘: "
        f"新規出品 {success_count}件 / 失敗 {failed_count}件"
    )
    total_checked = (success_count + skipped_no_stock + skipped_no_au +
                     skipped_few_sellers + skipped_unprofitable + failed_count)
    lines = [
        f"=== {dry_label}カタログ発掘 結果サマリー (競合{min_sellers}人以上) ===",
        "",
        f"JPカタログ候補ASIN:              {total_checked}件",
        f"新規出品{'予定' if dry_run else '完了'}:                  {success_count}件",
        f"スキップ（JP在庫なし）:          {skipped_no_stock}件",
        f"スキップ（AU競合なし/利益不足）: {skipped_no_au}件",
        f"スキップ（競合{min_sellers}人未満）:         {skipped_few_sellers}件",
        f"スキップ（競合価格が赤字ライン）:{skipped_unprofitable}件",
        f"失敗:                          {failed_count}件",
        "",
    ]
    if listed_details:
        lines.append(f"--- 新規出品{'予定' if dry_run else '完了'}リスト ({success_count}件) ---")
        for item in listed_details:
            lines.append(
                f"  {item['asin']}  JP¥{item['jp_price']:,} → AU${item['au_price']:.2f}"
                f"  粗利率{item['profit_rate']:.1f}%"
                f"  競合{item.get('seller_count', '?')}人"
            )
    body = "\n".join(lines)
    return subject, body


def main():
    parser = argparse.ArgumentParser(
        description="JPカタログから新規ASINを発掘してAUに出品する"
    )
    parser.add_argument("--dry-run", action="store_true", help="出品せず確認のみ")
    parser.add_argument("--max-new", type=int, default=300, help="新規出品上限（デフォルト300）")
    parser.add_argument("--min-sellers", type=int, default=3, help="競合セラー最小数（デフォルト3）")
    parser.add_argument("--pages-per-kw", type=int, default=5, help="キーワードあたりページ数（デフォルト5）")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("[catalog_discover] *** DRY-RUN モード ***")
    logger.info("[catalog_discover] 設定: max_new=%d, min_sellers=%d, pages_per_kw=%d",
                args.max_new, args.min_sellers, args.pages_per_kw)

    listed_details, skipped_no_stock, skipped_no_au, skipped_few_sellers, skipped_unprofitable, failed_count = discover_and_list(
        min_sellers=args.min_sellers,
        max_new=args.max_new,
        pages_per_kw=args.pages_per_kw,
        dry_run=args.dry_run,
    )

    subject, body = build_email(
        listed_details, skipped_no_stock, skipped_no_au,
        skipped_few_sellers, skipped_unprofitable, failed_count,
        min_sellers=args.min_sellers,
        dry_run=args.dry_run,
    )
    send_email(subject=subject, body=body)
    print(body)


if __name__ == "__main__":
    main()

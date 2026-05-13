"""
jp_to_au_lister.py — Amazon JP → Amazon AU 直送出品（第6の柱）

Amazon JP で安く仕入れられる商品を Amazon AU に MFN 出品。
注文が来たら Amazon JP で注文して AU バイヤーの住所に直送。

既存スクリプトとの違い:
  ebay_jp_discover.py → JP → eBay
  jp_to_au_lister.py  → JP → Amazon AU ★

利点:
  - Amazon AU は eBay より購買力が高い（日本製品）
  - 同一 ASIN のページを使うのでレビュー・ランキングを引き継げる
  - BuyBox 獲得で検索上位に表示される
  - 仕入れ + 国際送料が消費税還付の対象

フロー:
  1. JP カタログ検索（ebay_jp_discover と同一の検索リスト）
  2. JP 価格・在庫確認（20件バッチ）
  3. AU 競合価格確認（＝ AU 存在確認を兼ねる、20件バッチ）
  4. 利益計算（BuyBox アンダーカット価格 vs 最低利益価格）
  5. Amazon AU に MFN 出品（ListingsItems PUT / LISTING_OFFER_ONLY）
  6. メール通知

使い方:
  python jp_to_au_lister.py               # 実行（上限200件）
  python jp_to_au_lister.py --dry-run     # テスト（出品しない）
  python jp_to_au_lister.py --max-new 50  # 上限50件
  python jp_to_au_lister.py --rank-limit 20000  # JP ランク上位2万以内

削除系:
  python jp_to_au_lister.py --delete-sku JP-B012XIYYBW
      # 特定 SKU を Amazon AU から削除
  python jp_to_au_lister.py --prune-overpriced --max-price 1000
      # JP- SKU で A$1,000 超の出品を一括削除（豪州税関対策）
  python jp_to_au_lister.py --prune-overpriced --dry-run
      # 削除対象の確認のみ（実際には削除しない）
"""
import argparse
import sys
import time
from typing import Optional

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import sqlite3
from datetime import datetime, timezone

from sp_api.api import CatalogItems, ListingsItems, Products
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from ebay_jp_discover import (
    TARGET_JP_BRANDS,
    TARGET_JP_SEARCHES,
    check_authenticity,
    get_jp_prices_bulk,
    search_jp_catalog,
)
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.logger import get_logger
from utils.notify import send_email

DB_PATH = "arbitrage.db"


# ─────────────────────────────────────────────
# DB ユーティリティ（JP→AU 出品管理）
# ─────────────────────────────────────────────

def _get_listed_jp_asins() -> set:
    """
    arbitrage.db の listings テーブルから JP-AU 出品済み ASIN セットを返す。
    sku = 'JP-{ASIN}' で登録されているもの。
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT asin FROM listings WHERE platform='amazon_au' AND sku LIKE 'JP-%'"
        )
        result = {row[0] for row in c.fetchall()}
        conn.close()
        return result
    except Exception as e:
        logger.warning("[jp2au] DB読み込みエラー: %s", e)
        return set()


def _mark_listed(asin: str, price_aud: float) -> None:
    """出品完了した ASIN を arbitrage.db に記録する（重複出品防止）。"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        c.execute(
            """INSERT OR REPLACE INTO listings
               (asin, sku, platform, status, listed_at, updated_at)
               VALUES (?, ?, 'amazon_au', 'active', ?, ?)""",
            (asin, f"JP-{asin}", now, now),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("[jp2au] DB書き込みエラー: asin=%s | %s", asin, e)

logger = get_logger(__name__)

MARKETPLACE_AU = config.MARKETPLACE_AU
AU_SELLER_ID   = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")

_AU_CREDS = {
    "refresh_token":     config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":        config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}

AU_BATCH_INTERVAL = 2.1   # get_competitive_pricing: 0.5 req/s
LIST_INTERVAL     = 0.5   # ListingsItems PUT
TYPE_INTERVAL     = 0.5   # CatalogItems productType 取得


# ─────────────────────────────────────────────
# AU 競合価格取得（＝ AU 存在確認兼用）
# ─────────────────────────────────────────────

def get_au_prices_bulk(asins: list) -> dict:
    """
    Amazon AU の競合価格を 20件バッチで取得する。
    AU に存在しない・売り手がいない ASIN は result に含まれない
    （= AU 存在確認を兼ねる）。

    Returns:
        {asin: price_aud (float)}
    """
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    total  = len(asins)

    for i in range(0, total, 20):
        batch = asins[i:i + 20]
        if i % 200 == 0:
            logger.info("[jp2au] AU価格確認: %d/%d", i, total)
        try:
            resp  = api.get_competitive_pricing_for_asins(batch)
            items = resp.payload if isinstance(resp.payload, list) else []
            for item in items:
                asin        = item.get("ASIN", "")
                comp_prices = (
                    item.get("Product", {})
                        .get("CompetitivePricing", {})
                        .get("CompetitivePrices", [])
                )
                for cp in comp_prices:
                    if (cp.get("condition") or "").lower() in ("new", "new_new"):
                        amt = (
                            cp.get("Price", {})
                              .get("ListingPrice", {})
                              .get("Amount")
                        )
                        if amt:
                            result[asin] = float(amt)
                        break
        except SellingApiException as e:
            logger.warning("[jp2au] AU価格バッチエラー: %s", e)

        time.sleep(AU_BATCH_INTERVAL)

    logger.info("[jp2au] AU価格確認完了: %d件中 %d件がAU存在・価格あり",
                total, len(result))
    return result


# ─────────────────────────────────────────────
# 出品価格決定
# ─────────────────────────────────────────────

def decide_listing_price(
    asin: str,
    title: str,
    jp_price_jpy: int,
    au_market_aud: float,
    jpy_to_aud: float,
    weight_kg: Optional[float],
) -> tuple:
    """
    BuyBox 狙いの出品価格を決定する。

    戦略:
      - 市場最安値から BUYBOX_UNDERCUT_RATE 分引く（デフォルト 1% 引き）
      - 利益が出ない場合は最低利益価格（calc_optimal_au_price）まで引き上げ
      - それでも市場より高くなる場合は出品しない

    Returns:
        (is_viable: bool, our_price_aud: float, profit_rate: float)
    """
    min_price = calc_optimal_au_price(
        jp_price_jpy, exchange_rate=jpy_to_aud, weight_kg=weight_kg
    )

    # BuyBox 狙い: 現在の最安値からわずかに引く
    undercut = round(au_market_aud * (1 - config.BUYBOX_UNDERCUT_RATE), 2)

    # 利益が出る範囲で最安値に設定
    our_price = max(undercut, min_price)

    # 市場価格の 2% 超え → 競合不可 → スキップ
    if our_price > au_market_aud * 1.02:
        return False, our_price, 0.0

    result = calc_profit(asin, title, jp_price_jpy, our_price, jpy_to_aud, weight_kg)
    return result.is_profitable, our_price, result.profit_rate


# ─────────────────────────────────────────────
# Amazon AU 出品
# ─────────────────────────────────────────────

def _get_listing_metadata(asin: str) -> tuple:
    """
    ASIN の Amazon AU productType と外部識別子（EAN/JAN/UPC）を取得する。

    Returns:
        (product_type: str, id_type: str, id_value: str)
        id_type/id_value は取得できなければ空文字
    """
    try:
        api = CatalogItems(
            credentials=_AU_CREDS,
            marketplace=Marketplaces.AU,
            version="2022-04-01",
        )
        resp = api.get_catalog_item(
            asin,
            marketplaceIds=[MARKETPLACE_AU],
            includedData=["productTypes", "identifiers"],
        )
        payload = resp.payload or {}

        # productType
        product_type = "PRODUCT"
        pts = payload.get("productTypes", [])
        for pt in pts:
            if pt.get("marketplaceId") == MARKETPLACE_AU:
                product_type = pt.get("productType", "PRODUCT")
                break
        if product_type == "PRODUCT" and pts:
            product_type = pts[0].get("productType", "PRODUCT")

        # 外部識別子（EAN/JAN/UPC 優先）
        id_type, id_value = "", ""
        for id_group in (payload.get("identifiers") or []):
            for ident in (id_group.get("identifiers") or []):
                t = (ident.get("identifierType") or "").upper()
                v = ident.get("identifier", "")
                if t in ("EAN", "JAN") and v:
                    id_type, id_value = "ean", v
                    break
                if t == "UPC" and v and not id_value:
                    id_type, id_value = "upc", v
            if id_value:
                break

        return product_type, id_type, id_value

    except Exception:
        pass
    return "PRODUCT", "", ""


def delete_au_listing(sku: str) -> bool:
    """
    Amazon AU から指定 SKU の出品を削除する。
    豪州税関 A$1,000 超えなど問題のある出品を取り下げる際に使用。

    Returns:
        True if deleted successfully, False otherwise
    """
    if not AU_SELLER_ID:
        logger.error("[jp2au] AMAZON_AU_SELLER_ID が未設定")
        return False

    try:
        api  = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        resp = api.delete_listings_item(
            sellerId=AU_SELLER_ID,
            sku=sku,
            marketplaceIds=[MARKETPLACE_AU],
        )
        payload = resp.payload or {}
        status  = str(payload.get("status", "")).upper()
        if status in ("ACCEPTED", "DELETED"):
            logger.info("[jp2au] 出品削除完了: SKU=%s", sku)
            return True
        # status が空でも 200 系ならば成功扱い
        if hasattr(resp, "status_code") and resp.status_code in (200, 204):
            logger.info("[jp2au] 出品削除完了(HTTP%d): SKU=%s", resp.status_code, sku)
            return True
        logger.warning("[jp2au] 出品削除応答(%s): SKU=%s | payload=%s",
                       status or "?", sku, payload)
        return False
    except SellingApiException as e:
        # 404 = すでに存在しない → 削除済みとして OK
        if "404" in str(e) or "not found" in str(e).lower():
            logger.info("[jp2au] SKU は既に存在しない（削除済み）: %s", sku)
            return True
        logger.error("[jp2au] 出品削除失敗: SKU=%s | %s", sku, e)
        return False
    except Exception as e:
        logger.error("[jp2au] 出品削除失敗: SKU=%s | %s", sku, e)
        return False


def get_au_jp_listings(price_threshold_aud: float = 0.0) -> dict:
    """
    Amazon AU に出品中の JP- SKU（jp_to_au_lister 作成分）を取得する。
    price_threshold_aud > 0 の場合はその価格を超えるものだけ返す。

    Returns:
        {sku: {"asin": str, "price_aud": float}}
    """
    try:
        api    = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        result = {}
        page_token = None

        while True:
            kwargs = {
                "sellerId":       AU_SELLER_ID,
                "marketplaceIds": [MARKETPLACE_AU],
                "pageSize":       50,
            }
            if page_token:
                kwargs["pageToken"] = page_token

            resp    = api.get_listings_items(**kwargs)
            payload = resp.payload or {}
            items   = payload.get("items") or []

            for item in items:
                sku = item.get("sku", "")
                if not sku.startswith("JP-"):
                    continue
                # 価格取得
                price_aud = 0.0
                for offer in (item.get("offers") or []):
                    schedule = (
                        offer.get("buyingPrice", {})
                             .get("listingPrice", {})
                    )
                    if schedule.get("amount"):
                        price_aud = float(schedule["amount"])
                        break
                # 価格フィルタ
                if price_threshold_aud and price_aud <= price_threshold_aud:
                    continue
                asin = sku[3:]  # "JP-" の後ろが ASIN
                result[sku] = {"asin": asin, "price_aud": price_aud}

            page_token = payload.get("nextPageToken")
            if not page_token:
                break
            time.sleep(0.5)

        return result

    except SellingApiException as e:
        logger.warning("[jp2au] JP-出品一覧取得失敗（GetListingsItems未対応の可能性）: %s", e)
        return {}
    except Exception as e:
        logger.error("[jp2au] JP-出品一覧取得エラー: %s", e)
        return {}


def create_au_listing(asin: str, price_aud: float) -> bool:
    """
    Amazon AU に MFN（自己発送）オファーを作成する。
    SKU = "JP-{ASIN}" で管理（既存 AU 出品との混在を防ぐ）。
    数量 99 で登録（JP 在庫状況は別途監視が必要）。
    """
    if not AU_SELLER_ID:
        logger.error("[jp2au] AMAZON_AU_SELLER_ID が未設定")
        return False

    sku                          = f"JP-{asin}"
    product_type, id_type, id_value = _get_listing_metadata(asin)
    time.sleep(TYPE_INTERVAL)

    attributes = {
        # どの ASIN に紐付けるかを明示（LISTING_OFFER_ONLY の必須項目）
        "merchant_suggested_asin": [{
            "value":          asin,
            "marketplace_id": MARKETPLACE_AU,
        }],
        "condition_type": [{
            "value":          "new_new",
            "marketplace_id": MARKETPLACE_AU,
        }],
        "fulfillment_availability": [{
            "fulfillment_channel_code": "DEFAULT",   # MFN（自己発送）
            "quantity":                 99,
            "marketplace_id":           MARKETPLACE_AU,
        }],
        "purchasable_offer": [{
            "currency":       "AUD",
            "our_price":      [{"schedule": [{"value_with_tax": price_aud}]}],
            "marketplace_id": MARKETPLACE_AU,
        }],
    }

    # EAN/UPC が取得できた場合のみ追加（必須エラー回避）
    if id_type and id_value:
        attributes["externally_assigned_product_identifier"] = [{
            "type":           id_type,
            "value":          id_value,
            "marketplace_id": MARKETPLACE_AU,
        }]

    body = {
        "productType": product_type,
        "requirements": "LISTING_OFFER_ONLY",
        "attributes":   attributes,
    }

    try:
        api  = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        resp = api.put_listings_item(
            sellerId=AU_SELLER_ID,
            sku=sku,
            marketplaceIds=[MARKETPLACE_AU],
            body=body,
        )
        payload = resp.payload or {}
        status  = str(payload.get("status", "")).upper()
        if status in ("ACCEPTED", "VALID"):
            logger.info("[jp2au] AU出品完了: %s | SKU=%s | A$%.2f", asin, sku, price_aud)
            return True
        issues = payload.get("issues", [])
        logger.warning("[jp2au] AU出品応答(%s): %s | issues=%s",
                       status, asin, issues[:3])
        return False
    except Exception as e:
        logger.error("[jp2au] AU出品失敗: %s | %s", asin, e)
        return False


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Amazon JP → AU 直送出品")
    parser.add_argument("--dry-run",    action="store_true",
                        help="出品しない（確認のみ）/ 削除系は削除しない")
    parser.add_argument("--max-new",    type=int, default=500,
                        help="新規出品上限（デフォルト500 / 実質はフィルタが上限）")
    parser.add_argument("--rank-limit", type=int, default=30000,
                        help="JP ランク上限（デフォルト30000）")
    parser.add_argument("--max-pages",  type=int, default=3,
                        help="検索あたり最大ページ数（デフォルト3）")
    parser.add_argument("--max-price",  type=float, default=1000.0,
                        help="AU 出品価格の上限 AUD（デフォルト1000）"
                             " ※豪州税関: A$1,000超は個別通関・追加関税リスク")
    # ── 削除系 ──────────────────────────────────────
    parser.add_argument("--delete-sku", type=str, default="",
                        help="指定 SKU を Amazon AU から削除して終了")
    parser.add_argument("--prune-overpriced", action="store_true",
                        help="JP- SKU で --max-price 超の出品を一括削除して終了")
    args = parser.parse_args()

    # ── --delete-sku: 単一 SKU 削除 ────────────────────────────────
    if args.delete_sku:
        sku = args.delete_sku.strip()
        logger.info("[jp2au] 出品削除リクエスト: SKU=%s", sku)
        if args.dry_run:
            logger.info("[jp2au][DRY] 削除をスキップ: SKU=%s", sku)
        else:
            ok = delete_au_listing(sku)
            if ok:
                print(f"✅ 削除完了: {sku}")
            else:
                print(f"❌ 削除失敗: {sku}  （Seller Central で手動確認してください）")
                sys.exit(1)
        return

    # ── --prune-overpriced: 価格上限超え JP- 出品の一括削除 ──────────
    if args.prune_overpriced:
        threshold = args.max_price
        logger.info("[jp2au] A$%.0f 超の JP- 出品を取得中…", threshold)
        overpriced = get_au_jp_listings(price_threshold_aud=threshold)
        if not overpriced:
            # API が GetListingsItems に対応していない場合もあるので手動 SKU を案内
            print(f"⚠️  A$%.0f 超の JP- 出品が取得できませんでした。" % threshold)
            print("  SP-API GetListingsItems が未サポートの場合は")
            print("  --delete-sku JP-XXXXXXXXXXXX で個別削除してください。")
            return
        print(f"\n【削除対象】 A${threshold:.0f} 超の JP- 出品: {len(overpriced)} 件")
        for sku, info in overpriced.items():
            print(f"  {sku}  A${info['price_aud']:.2f}")
        print()
        if args.dry_run:
            print("[DRY-RUN] 実際には削除しません。")
            return
        deleted = failed = 0
        for sku in overpriced:
            if delete_au_listing(sku):
                deleted += 1
            else:
                failed += 1
            time.sleep(0.5)
        print(f"\n削除完了: {deleted}件  失敗: {failed}件")
        return

    if args.dry_run:
        logger.info("[jp2au] *** DRY-RUN モード ***")

    jpy_to_aud = get_jpy_to_aud()
    logger.info("[jp2au] JPY→AUD: %.6f", jpy_to_aud)

    # 出品済み ASIN を DB から取得（重複出品防止）
    already_listed = _get_listed_jp_asins()
    logger.info("[jp2au] 出品済みスキップ対象: %d件", len(already_listed))

    # ── STEP 1: JP カタログ検索 ─────────────────────────────────
    candidates: dict = {}

    for label, keywords, node_id in TARGET_JP_SEARCHES:
        logger.info("[jp2au] JP検索: %s", label)
        items = search_jp_catalog(
            keywords=keywords,
            browse_node_id=node_id or None,
            max_pages=args.max_pages,
            rank_limit=args.rank_limit,
        )
        for item in items:
            if item["asin"] not in candidates:
                candidates[item["asin"]] = item
        time.sleep(0.8)

    for brand in TARGET_JP_BRANDS:
        logger.info("[jp2au] ブランド検索: %s", brand)
        items = search_jp_catalog(
            keywords=brand,
            brand=brand,
            max_pages=args.max_pages,
            rank_limit=args.rank_limit,
        )
        for item in items:
            if item["asin"] not in candidates:
                candidates[item["asin"]] = item
        time.sleep(0.8)

    logger.info("[jp2au] JP候補: %d件", len(candidates))
    if not candidates:
        logger.info("[jp2au] 候補なし。終了")
        return

    all_asins = list(candidates.keys())

    # ── STEP 2: JP 価格・在庫確認 ───────────────────────────────
    logger.info("[jp2au] JP価格確認: %d件", len(all_asins))
    jp_prices   = get_jp_prices_bulk(all_asins)
    jp_ok_asins = [
        a for a in all_asins
        if jp_prices.get(a) and jp_prices[a][1] and jp_prices[a][0]
    ]
    logger.info("[jp2au] JP在庫・価格あり: %d件", len(jp_ok_asins))

    # ── STEP 3: AU 競合価格確認（＝ AU 存在確認）────────────────
    logger.info("[jp2au] AU価格確認（存在確認兼）: %d件", len(jp_ok_asins))
    au_prices = get_au_prices_bulk(jp_ok_asins)
    logger.info("[jp2au] AU存在・価格あり: %d件", len(au_prices))

    # ── STEP 4: 利益フィルタ ─────────────────────────────────────
    profitable    = []
    skipped_auth  = skipped_profit = skipped_dup = 0

    for asin, au_market in au_prices.items():
        # 出品済みスキップ（重複防止）
        if asin in already_listed:
            skipped_dup += 1
            continue

        item  = candidates[asin]
        title = item.get("title", "") or asin
        brand = item.get("brand", "")
        wt    = item.get("weight_kg")

        # 真贋チェック
        auth_ok, _ = check_authenticity(title, brand)
        if not auth_ok:
            skipped_auth += 1
            continue

        # 出品価格・利益確認
        viable, our_price, margin = decide_listing_price(
            asin, title, jp_prices[asin][0], au_market, jpy_to_aud, wt
        )
        if not viable:
            skipped_profit += 1
            continue

        # 価格上限チェック（豪州税関: A$1,000超は個別通関リスク）
        if our_price > args.max_price:
            logger.debug("[jp2au] 価格上限スキップ: %s A$%.2f > A$%.0f",
                         asin, our_price, args.max_price)
            skipped_profit += 1
            continue

        profitable.append({
            "asin":      asin,
            "title":     title,
            "brand":     brand,
            "rank":      item.get("rank"),
            "weight_kg": wt,
            "jp_price":  jp_prices[asin][0],
            "au_market": au_market,
            "our_price": our_price,
            "margin":    margin,
        })

    # 利益率の高い順にソート
    profitable.sort(key=lambda x: x["margin"], reverse=True)
    logger.info("[jp2au] 出品対象: %d件 (重複スキップ%d / 真贋NG%d / 利益不足%d)",
                len(profitable), skipped_dup, skipped_auth, skipped_profit)

    # ── STEP 5: Amazon AU 出品 ───────────────────────────────────
    listed = failed = 0
    listed_details = []

    for i, p in enumerate(profitable):
        if listed >= args.max_new:
            logger.info("[jp2au] 上限%d件到達。残り%d件スキップ",
                        args.max_new, len(profitable) - i)
            break

        asin     = p["asin"]
        rank_str = f"rank{p['rank']:,}" if p["rank"] else "rank不明"
        log_msg  = (
            f"[{i+1}/{len(profitable)}] {asin} | "
            f"JP¥{p['jp_price']:,} → AU市場A${p['au_market']:.2f} → "
            f"出品A${p['our_price']:.2f} | 粗利{p['margin']:.1f}% | "
            f"{rank_str} | {p['title'][:40]}"
        )

        if args.dry_run:
            logger.info("[jp2au][DRY] %s", log_msg)
            listed_details.append(p)
            listed += 1
            continue

        if create_au_listing(asin, p["our_price"]):
            logger.info("[jp2au] 出品完了: %s", log_msg)
            listed_details.append(p)
            listed += 1
            _mark_listed(asin, p["our_price"])   # DB に記録（重複防止）
        else:
            logger.warning("[jp2au] 出品失敗: %s", asin)
            failed += 1

        time.sleep(LIST_INTERVAL)

    # ── サマリー & メール ────────────────────────────────────────
    dry     = "[DRY-RUN] " if args.dry_run else ""
    subject = (
        f"[JP→AU直送] {dry}新規出品{listed}件 / "
        f"利益不足{skipped_profit}件 / 真贋NG{skipped_auth}件"
    )
    lines = [
        f"=== {dry}Amazon JP → AU 直送出品結果 ===",
        "",
        f"JP候補:           {len(candidates)}件",
        f"JP在庫・価格あり:  {len(jp_ok_asins)}件",
        f"AU存在・価格あり:  {len(au_prices)}件",
        f"出品{'予定' if args.dry_run else '完了'}:        {listed}件",
        f"失敗:             {failed}件",
        f"重複スキップ:      {skipped_dup}件",
        f"利益不足スキップ:  {skipped_profit}件",
        f"真贋NGスキップ:   {skipped_auth}件",
        "",
        f"JPY→AUD: {jpy_to_aud:.6f}",
        f"MIN_PROFIT_RATE: {config.MIN_PROFIT_RATE}%",
        f"AU手数料: {config.AU_FEE_RATE*100:.0f}%  |  BuyBox: -{config.BUYBOX_UNDERCUT_RATE*100:.1f}%",
        "",
    ]
    if listed_details:
        top_n = min(listed, 50)
        lines.append(f"--- 出品{'予定' if args.dry_run else '完了'} Top {top_n}件（利益率順）---")
        for d in listed_details[:50]:
            rl = f"rank{d['rank']:,}" if d["rank"] else "rank不明"
            lines.append(
                f"  {d['asin']}  [{d['brand']}]  "
                f"JP¥{d['jp_price']:,}  "
                f"A${d['au_market']:.2f}→A${d['our_price']:.2f}  "
                f"粗利{d['margin']:.1f}%  {rl}  {d['title'][:35]}"
            )
        if listed > 50:
            lines.append(f"  ... 他 {listed - 50}件")

    body = "\n".join(lines)
    send_email(subject=subject, body=body)
    try:
        print(body)
    except UnicodeEncodeError:
        print(body.encode("utf-8", errors="replace").decode("ascii", errors="replace"))


if __name__ == "__main__":
    main()

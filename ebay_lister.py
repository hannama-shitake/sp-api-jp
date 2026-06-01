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

価格監視（JP連動）:
  毎日 05:05 JST に GitHub Actions で自動実行。
  JP価格が前回から 3% 以上変動した場合に revise_price で自動更新。
  JP在庫切れになった場合は EndItem で出品を自動終了。

使い方:
  python ebay_lister.py                  # 新規出品（上限200件）
  python ebay_lister.py --dry-run        # テスト（出品しない）
  python ebay_lister.py --max-new 50     # 上限50件
  python ebay_lister.py --sync-only      # 価格同期・終了のみ（新規出品なし）
  python ebay_lister.py --fix-shipping   # 既存出品の送料を $25/$30 に一括更新（価格も再計算）
"""
import argparse
import csv
import gzip
import io
import os
import sys
import time

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

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

def get_product_info(asin: str) -> tuple[str, list[str]]:
    """
    Catalog Items API から商品タイトルと全商品画像URLリストを取得する。
    JP → AU の順で試み、最大 MAX_EBAY_IMAGES 枚まで収集する。

    eBay は最大12枚まで掲載可能。複数画像があると「実物感・信頼感」が増し
    購買率が上がる（MAIN1枚より PT01〜PT07 のマルチアングルが効果的）。

    Returns:
        (catalog_title: str, image_urls: list[str])
        image_urls: MAIN を先頭に最大12枚。必ず1枚以上（フォールバックあり）
    """
    MAX_EBAY_IMAGES = 12
    # eBay が推奨する掲載順: MAIN を先頭、角度違い→詳細→ライフスタイルの順
    VARIANT_ORDER = ["MAIN", "PT01", "PT02", "PT03", "PT04", "PT05",
                     "PT06", "PT07", "TOPP", "FRNT", "SIDE", "BACK",
                     "BOTT", "LTHT", "BKGR"]

    catalog_title = ""
    images_by_variant: dict[str, str] = {}

    for creds, marketplace_id, marketplace in [
        (_JP_CREDS, config.MARKETPLACE_JP, Marketplaces.JP),
        (_AU_CREDS, config.MARKETPLACE_AU, Marketplaces.AU),
    ]:
        try:
            api = CatalogItems(credentials=creds, marketplace=marketplace,
                               version="2022-04-01")
            resp = api.get_catalog_item(
                asin,
                marketplaceIds=[marketplace_id],
                includedData=["images", "summaries"],
            )
            payload = resp.payload or {}

            # カタログタイトル取得
            if not catalog_title:
                for s in payload.get("summaries", []):
                    t = s.get("itemName", "").strip()
                    if t:
                        catalog_title = t
                        break

            # 全バリアント画像を収集（既に取得済みのバリアントは上書きしない）
            for img_set in payload.get("images", []):
                for img in img_set.get("images", []):
                    variant = img.get("variant", "")
                    url     = (img.get("link") or "").strip()
                    if variant and url and variant not in images_by_variant:
                        images_by_variant[variant] = url.replace("http://", "https://")

        except Exception:
            pass

        # タイトルと MAIN 画像が取れたら JP だけで十分
        if catalog_title and "MAIN" in images_by_variant:
            break

    # VARIANT_ORDER 順に並べ、最大 MAX_EBAY_IMAGES 枚に絞る
    ordered_urls = []
    for v in VARIANT_ORDER:
        if v in images_by_variant:
            ordered_urls.append(images_by_variant[v])
    # VARIANT_ORDER に含まれないバリアントも末尾に追加
    for v, url in images_by_variant.items():
        if v not in VARIANT_ORDER and url not in ordered_urls:
            ordered_urls.append(url)
    ordered_urls = ordered_urls[:MAX_EBAY_IMAGES]

    # フォールバック: Catalog API で1枚も取れなかった場合
    if not ordered_urls:
        ordered_urls = [
            f"https://m.media-amazon.com/images/P/{asin}.01._SCLZZZZZZZ_.jpg"
        ]

    logger.info("[ebay_lister] 画像取得: %s → %d枚 (%s)",
                asin, len(ordered_urls),
                ", ".join(images_by_variant.keys())[:60])

    return catalog_title, ordered_urls


def get_product_image(asin: str) -> str:
    """後方互換ラッパー（catalog_title は不要な呼び出し元用）"""
    _, image_urls = get_product_info(asin)
    return image_urls[0] if image_urls else ""


# ─────────────────────────────────────────────
# USD 価格計算
# ─────────────────────────────────────────────

def calc_ebay_usd_price(jp_price_jpy: int, jpy_to_usd: float) -> float:
    """
    JP仕入値から eBay USD 出品価格（商品代金のみ）を計算する。

    送料は buyer 負担（US $25 / 国際 $30）に変更済み。
    eBay FVF は (item_price + shipping) の合計にかかるため shipping 分を逆算して除く。

    計算式:
      required_net = jp仕入USD × (1 + 利益率) + DHL運送費USD
        ↑ eBay 手数料前の必要受取額（jp 仕入れに対して利益率確保、DHL は実費）
      required_total = required_net ÷ (1 − eBay手数料率)
        ↑ buyer が払う (item_price + shipping) の最低合計
      item_price = required_total − US送料($25)
        ↑ US 送料基準で計算（保守的）。shipping 分は buyer から別途回収。

    効果: 旧来（送料込み）vs 新（送料別）
      jp¥5,000 の例（JPY→USD=0.0067）:
        旧: item $97.4（送料 $0）  ← 検索では高く見える
        新: item $72.4（送料 +$25）← $25 安く見える、利益率は同等
    """
    jp_cost_usd  = jp_price_jpy * jpy_to_usd
    dhl_cost_usd = config.DHL_SHIPPING_JPY * jpy_to_usd

    # ★ JP価格バッファ: APIの価格は実際の仕入れ値より安く出ることがある
    # EBAY_JP_PRICE_BUFFER(デフォルト1.3)をかけて赤字を防ぐ
    jp_cost_usd = jp_cost_usd * config.EBAY_JP_PRICE_BUFFER

    # 必要受取合計（buyer が払う item + shipping の最低ライン）
    required_net   = jp_cost_usd * (1 + config.MIN_PROFIT_RATE / 100) + dhl_cost_usd
    required_total = required_net / (1 - config.EBAY_FEE_RATE)

    # 送料 $25 は buyer 負担なので item_price から差し引く
    item_price = required_total - config.EBAY_SHIPPING_US_USD
    return round(max(item_price, 1.0), 2)


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Amazon AU→eBay 一括クロスリスト")
    parser.add_argument("--dry-run", action="store_true", help="出品せず確認のみ")
    parser.add_argument("--max-new", type=int, default=200, help="新規出品上限（デフォルト200）")
    parser.add_argument("--sync-only", action="store_true",
                        help="新規出品なし、価格同期・終了のみ")
    parser.add_argument("--fix-shipping", action="store_true",
                        help="既存 eBay 出品の送料を US$25/国際$30 に一括更新（価格も新計算式で再設定）")
    parser.add_argument("--fix-images", action="store_true",
                        help="既存 eBay 出品に複数画像（最大12枚）を一括追加")
    args = parser.parse_args()

    if not config.EBAY_USER_TOKEN and not args.dry_run:
        logger.error("[ebay_lister] EBAY_USER_TOKEN が未設定。--dry-run で確認してください")
        sys.exit(1)

    if args.dry_run:
        logger.info("[ebay_lister] *** DRY-RUN モード ***")

    # ── --fix-shipping: 既存出品の送料 + 価格を一括更新 ─────────────────
    if args.fix_shipping:
        jpy_to_usd = get_jpy_to_usd()
        logger.info("[ebay_lister] 送料一括修正モード (US$%.0f / Intl$%.0f)",
                    config.EBAY_SHIPPING_US_USD, config.EBAY_SHIPPING_INTL_USD)

        ebay_listings = ebay_api.get_active_listings()
        if not ebay_listings:
            print("eBay アクティブ出品なし")
            return

        # custom_label (ASIN) → ItemID マップ（タイトルも保持）
        asin_to_item = {v["custom_label"]: k for k, v in ebay_listings.items()
                        if v.get("custom_label")}
        asin_to_title = {v["custom_label"]: v.get("title", "") for v in ebay_listings.values()
                         if v.get("custom_label")}
        asins = list(asin_to_item.keys())
        logger.info("[ebay_lister] 対象: %d件", len(asins))

        # JP 価格を取得して新しい item 価格を計算
        jp_prices = get_jp_prices_bulk(asins)

        updated = failed = skipped = 0
        for asin, item_id in asin_to_item.items():
            jp_price, in_stock = jp_prices.get(asin, (None, False))
            if not jp_price:
                logger.info("[ebay_lister][fix] JP価格不明: %s", asin)
                skipped += 1
                continue

            new_price = calc_ebay_usd_price(jp_price, jpy_to_usd)
            old_price = ebay_listings[item_id]["price_usd"]
            title     = asin_to_title.get(asin, "")

            if args.dry_run:
                print(f"[DRY] {asin}  ItemID={item_id}  ${old_price:.2f}→${new_price:.2f}  "
                      f"US$%.0f/Intl$%.0f  desc={'yes' if title else 'no'}"
                      % (config.EBAY_SHIPPING_US_USD, config.EBAY_SHIPPING_INTL_USD))
                updated += 1
            else:
                if ebay_api.revise_shipping_and_price(item_id, new_price, title=title):
                    print(f"OK  {asin}  ${old_price:.2f}→${new_price:.2f}  "
                          f"US${config.EBAY_SHIPPING_US_USD:.0f}/Intl${config.EBAY_SHIPPING_INTL_USD:.0f}")
                    updated += 1
                else:
                    print(f"NG  {asin}  ItemID={item_id}")
                    failed += 1
            time.sleep(EBAY_INTERVAL)

        print(f"\n送料修正完了: {updated}件  失敗: {failed}件  スキップ: {skipped}件")
        return

    # ── --fix-images: 既存出品に複数画像を一括追加 ──────────────────────
    if args.fix_images:
        logger.info("[ebay_lister] 複数画像追加モード（最大12枚）")
        ebay_listings = ebay_api.get_active_listings()
        if not ebay_listings:
            print("eBay アクティブ出品なし")
            return
        asin_to_item = {v["custom_label"]: k for k, v in ebay_listings.items()
                        if v.get("custom_label")}
        updated = failed = skipped = 0
        for asin, item_id in asin_to_item.items():
            _, image_urls = get_product_info(asin)
            time.sleep(0.3)
            if len(image_urls) <= 1:
                logger.info("[ebay_lister][img] 1枚のみ、スキップ: %s", asin)
                skipped += 1
                continue
            if args.dry_run:
                print(f"[DRY] {asin}  ItemID={item_id}  {len(image_urls)}枚")
                updated += 1
            else:
                if ebay_api.revise_images(item_id, image_urls):
                    print(f"OK  {asin}  {len(image_urls)}枚")
                    updated += 1
                else:
                    print(f"NG  {asin}  ItemID={item_id}")
                    failed += 1
            time.sleep(EBAY_INTERVAL)
        print(f"\n画像追加完了: {updated}件  失敗: {failed}件  1枚のみスキップ: {skipped}件")
        return

    jpy_to_usd = get_jpy_to_usd()
    jpy_to_aud = get_jpy_to_aud()
    logger.info("[ebay_lister] 為替: 1 JPY = %.6f USD / %.6f AUD",
                jpy_to_usd, jpy_to_aud)

    # 1. Amazon AU アクティブ出品
    au_listings = get_au_active_listings()
    if not au_listings:
        logger.info("[ebay_lister] AU出品なし。終了")
        return

    # NG・重量超過ASINをDBから取得してフィルタ
    import sqlite3 as _sqlite3
    _ng_asins: set = set()
    _weight_map: dict = {}
    try:
        with _sqlite3.connect(config.DB_PATH) as _conn:
            for _row in _conn.execute(
                "SELECT asin FROM asin_candidates WHERE status='ng'"
            ).fetchall():
                _ng_asins.add(_row[0])
            for _row in _conn.execute(
                "SELECT asin, weight_kg FROM asin_candidates WHERE weight_kg IS NOT NULL"
            ).fetchall():
                _weight_map[_row[0]] = float(_row[1])
        logger.info("[ebay_lister] NGリスト読込: %d件 / 重量DB: %d件",
                    len(_ng_asins), len(_weight_map))
    except Exception as _e:
        logger.warning("[ebay_lister] NGリスト読込失敗（継続）: %s", _e)

    _before = len(au_listings)
    au_listings = [
        l for l in au_listings
        if l["asin"] not in _ng_asins
        and _weight_map.get(l["asin"], 0) < config.MAX_LISTING_WEIGHT_KG
    ]
    logger.info("[ebay_lister] NG・重量フィルタ後: %d件 → %d件",
                _before, len(au_listings))

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

        # カタログタイトルと全画像を取得
        # カタログタイトルを優先: AU Reports の seller title より正確
        # 複数画像: MAIN + PT01〜PT07 等、最大12枚まで取得してeBayに掲載
        catalog_title, image_urls = get_product_info(asin)
        time.sleep(0.3)

        # タイトル決定: JP/AU カタログ > AU Reports seller title > ASIN
        ebay_title = (catalog_title or title or asin)[:80]

        if catalog_title and catalog_title.lower() != title.lower():
            logger.info("[ebay_lister] タイトル上書き: '%s' → '%s'",
                        title[:40], catalog_title[:40])

        log_msg = (f"{asin} | {ebay_title[:40]} | ${price_usd:.2f} "
                   f"(JP¥{jp_price:,}) | 画像{len(image_urls)}枚")

        if args.dry_run:
            logger.info("[ebay_lister][DRY] 出品予定: %s", log_msg)
            listed_details.append({
                "asin": asin, "title": ebay_title,
                "jp_price": jp_price, "price_usd": price_usd,
                "image_count": len(image_urls),
            })
            listed += 1
            continue

        item_id = ebay_api.add_item(
            title=ebay_title,
            price_usd=price_usd,
            image_urls=image_urls,
            custom_label=asin,
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
    try:
        print(body)
    except UnicodeEncodeError:
        print(body.encode('utf-8', errors='replace').decode('ascii', errors='replace'))


if __name__ == "__main__":
    main()

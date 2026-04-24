"""
競合セラーページから新規ASIN を発掘して AU に出品するスクリプト。

キーワード検索は「他のセラーが売っていない商品（真贋リスクが高い）」を引っ張る恐れがある。
代わりに SELLER_URLS（GitHub Secret）に登録した競合セラーのAU出品ページを
スクレイピングし、AU市場で需要が証明済みの商品だけを取り込む。

フロー:
  1. AU 自分の出品一覧取得（Reports API）→ 既知ASIN セット
  2. 競合セラーページスクレイピング（data-asin 抽出）→ 未知ASIN リスト
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
  python catalog_discover.py --max-pages 10       # セラーあたり最大ページ数
"""
import csv
import gzip
import io
import os
import re
import random
import sys
import time
import argparse
from typing import Optional

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

JP_INTERVAL = 2.1        # Products API JP: 0.5 req/s
AU_PRICE_INTERVAL = 2.1  # Products API AU: 0.5 req/s
PATCH_INTERVAL = 0.3     # ListingsItems: 5 req/s

# Playwright スクレイピング用 User-Agent リスト（ランダム選択）
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


def _human_delay(min_sec: float = 1.5, max_sec: float = 4.0) -> None:
    """人間らしいランダム待機"""
    time.sleep(random.uniform(min_sec, max_sec))


def _random_scroll(page) -> None:
    """
    ページをランダムにスクロールする（人間らしく）。
    一度に大きくスクロールせず、少しずつ読んでいるように見せる。
    """
    num_scrolls = random.randint(4, 9)
    for _ in range(num_scrolls):
        scroll_y = random.randint(150, 600)
        page.evaluate(f"window.scrollBy(0, {scroll_y})")
        time.sleep(random.uniform(0.2, 0.9))


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
# 2. 競合セラーページスクレイピング → 未知ASIN 収集
# ─────────────────────────────────────────────

def scrape_seller_asins(
    seller_urls: list,
    max_pages: int = 10,
    existing_asins: set = None,
) -> list:
    """
    Playwright（ヘッドレスChromium）で競合セラーの AU 出品ページを巡回し、
    新規 ASIN リストを返す。

    設計方針（BAN回避）:
      - JS レンダリング済みの実ブラウザ → Amazon の bot 検知を回避
      - ランダムスクロール・ホバー・非等間隔待機で「人間らしさ」を演出
      - セラーごとにページを開き直し（ブラウザコンテキストは使い回す）
      - CAPTCHA 検出時はそのセラーをスキップして次へ

    Args:
        seller_urls: 競合セラーURL リスト（config.SELLER_URLS）
        max_pages:   1セラーあたり最大ページ数
        existing_asins: 除外する既知 ASIN セット

    Returns:
        新規 ASIN リスト（重複なし）
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    if existing_asins is None:
        existing_asins = set()

    if not seller_urls:
        logger.error(
            "[catalog_discover] SELLER_URLS が未設定。"
            "GitHub Secrets に SELLER_URLS を登録してください。"
        )
        return []

    all_asins: list = []
    seen = set(existing_asins)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            viewport={
                "width":  random.choice([1280, 1366, 1440, 1920]),
                "height": random.choice([768, 800, 900, 1080]),
            },
            user_agent=random.choice(_USER_AGENTS),
            locale="en-AU",
            timezone_id="Australia/Sydney",
            # 自動化フラグを隠す
            extra_http_headers={
                "Accept-Language": "en-AU,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        # navigator.webdriver を隠す
        context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )

        page = context.new_page()

        for raw_url in seller_urls:
            m = re.search(r"me=([A-Z0-9]+)", raw_url)
            if not m:
                logger.warning("[catalog_discover] seller_id 抽出失敗: %s", raw_url)
                continue
            seller_id = m.group(1)
            logger.info("[catalog_discover] Playwright: seller=%s 開始（最大%dページ）",
                        seller_id, max_pages)

            seller_new = 0

            for page_num in range(1, max_pages + 1):
                page_url = (
                    f"https://www.amazon.com.au/s"
                    f"?me={seller_id}&marketplaceID={MARKETPLACE_AU}&page={page_num}"
                )
                try:
                    page.goto(page_url, wait_until="domcontentloaded", timeout=30_000)
                    _human_delay(2.0, 4.5)

                    # ── CAPTCHA / ブロック検出 ──
                    url_now = page.url.lower()
                    if "captcha" in url_now or "robot" in url_now or "sorry" in url_now:
                        logger.warning("[catalog_discover] CAPTCHA/Block検出 seller=%s page=%d → スキップ",
                                       seller_id, page_num)
                        break
                    if page.query_selector("#captchacharacters") or page.query_selector("form[action*='captcha']"):
                        logger.warning("[catalog_discover] CAPTCHA入力フォーム検出 seller=%s → スキップ",
                                       seller_id)
                        break

                    # ── 人間らしいスクロール ──
                    _random_scroll(page)

                    # ── たまに商品にホバー（無駄な動き = 人間らしさ）──
                    if random.random() < 0.45:
                        items = page.query_selector_all("[data-asin]")
                        if items:
                            target = random.choice(items[:min(6, len(items))])
                            try:
                                target.hover()
                                time.sleep(random.uniform(0.4, 1.2))
                            except Exception:
                                pass

                    # ── さらにスクロールして下部まで読む ──
                    _random_scroll(page)
                    _human_delay(0.8, 2.0)

                    # ── ASIN 抽出 ──
                    content = page.content()
                    raw_asins = re.findall(r'data-asin="([A-Z0-9]{10})"', content)
                    page_new = 0
                    for asin in dict.fromkeys(raw_asins):
                        if asin and asin not in seen:
                            seen.add(asin)
                            all_asins.append(asin)
                            page_new += 1
                            seller_new += 1

                    logger.info("[catalog_discover] seller=%s page=%d: %d件新規（累計%d件）",
                                seller_id, page_num, page_new, len(all_asins))

                    # ── 次ページ判定 ──
                    next_btn = page.query_selector(".s-pagination-next:not([aria-disabled])")
                    if not next_btn or page_new == 0:
                        logger.info("[catalog_discover] seller=%s page=%d で終端", seller_id, page_num)
                        break

                    # ── 次ページへ（長めに待つ = 人間が読んでいる） ──
                    _human_delay(3.0, 6.5)

                except PWTimeout:
                    logger.warning("[catalog_discover] タイムアウト seller=%s page=%d", seller_id, page_num)
                    break
                except Exception as e:
                    logger.warning("[catalog_discover] エラー seller=%s page=%d: %s", seller_id, page_num, e)
                    break

            logger.info("[catalog_discover] seller=%s 完了: %d件収集", seller_id, seller_new)
            # セラー間は長めに待つ（急ぎすぎない）
            _human_delay(5.0, 10.0)

        context.close()
        browser.close()

    logger.info("[catalog_discover] 全セラー 新規ASIN候補: %d件", len(all_asins))
    return all_asins


# ─────────────────────────────────────────────
# 3. JP 価格一括取得
# ─────────────────────────────────────────────

def get_jp_prices_bulk(asins: list) -> dict:
    """
    20件バッチで JP 価格 {asin: (price_jpy, in_stock)} を返す。
    get_competitive_pricing_for_asins は複数セラーがいる場合のみ価格を返すため、
    独占出品（1セラー）の場合は get_item_offers でフォールバック取得する。
    """
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
                # 競合価格（複数出品者がいる場合のみ返る）
                price_jpy = None
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            price_jpy = int(float(amount))
                        break

                # NumberOfOfferListings で実在庫を判定
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
            logger.warning("[catalog_discover] JP価格バッチエラー (%s...): %s", batch[0], e)
        time.sleep(JP_INTERVAL)

    # ── フォールバック: JP独占出品（in_stock=True だが price=None）の価格を取得 ──
    # get_competitive_pricing はセラーが1人だと価格を返さないため get_item_offers で補完
    sole_asins = [a for a, (p, s) in result.items() if s and not p]
    if sole_asins:
        logger.info("[catalog_discover] JP独占出品フォールバック価格取得: %d件", len(sole_asins))
        for asin in sole_asins:
            try:
                resp = api.get_item_offers(asin, item_condition="New")
                offers = (resp.payload or {}).get("Offers", [])
                prices = [
                    int(float(o["ListingPrice"]["Amount"]))
                    for o in offers
                    if o.get("ListingPrice", {}).get("Amount")
                ]
                if prices:
                    result[asin] = (min(prices), True)
                    logger.debug("[catalog_discover] JP独占価格取得: %s ¥%d", asin, min(prices))
            except SellingApiException as e:
                logger.debug("[catalog_discover] JP独占価格取得失敗 %s: %s", asin, e)
            time.sleep(JP_INTERVAL)

    in_stock_count = sum(1 for v in result.values() if v[1])
    price_count = sum(1 for v in result.values() if v[0])
    logger.info("[catalog_discover] JP価格取得完了: %d件中 在庫あり%d件 価格あり%d件",
                total, in_stock_count, price_count)
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
# 5b. NGワードチェック
# ─────────────────────────────────────────────
import json as _json
import re as _re

_NG_WORDS: list = []


def _load_ng_words():
    global _NG_WORDS
    if _NG_WORDS:
        return
    try:
        path = os.path.join(os.path.dirname(__file__), "ng_words.json")
        with open(path, "r", encoding="utf-8") as f:
            data = _json.load(f)
        for category_words in data.values():
            if isinstance(category_words, list):
                _NG_WORDS.extend([w.lower() for w in category_words])
        logger.info("[catalog_discover] NGワード辞書ロード: %d件", len(_NG_WORDS))
    except Exception as e:
        logger.warning("[catalog_discover] NGワード辞書ロード失敗: %s", e)


def _check_ng_words(title: str, asin: str) -> tuple:
    """タイトルにNGワードが含まれているか確認。(is_ng: bool, matched: str)"""
    _load_ng_words()
    title_lower = title.lower()
    for word in _NG_WORDS:
        if word in title_lower:
            return True, word
    return False, ""


# ─────────────────────────────────────────────
# 6. 新規出品
# ─────────────────────────────────────────────

def list_new_item(api, seller_id: str, asin: str, price_aud: float):
    """
    新規 FBM 相乗り出品: PUT（ASIN紐付け）→ PATCH（価格・数量設定）の2ステップ。
    PUTだけでは Missing Offer になるため、必ずPATCHで価格・数量を確定する。
    """
    sku = f"{config.SKU_PREFIX}{asin}"

    # ── Step 1: PUT でASINに相乗り登録 ──
    put_body = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [
                {"value": asin, "marketplace_id": MARKETPLACE_AU}
            ],
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
        body=put_body,
    )
    status = resp.payload.get("status", "")
    if status not in ("ACCEPTED", "VALID"):
        issues = resp.payload.get("issues", [])
        msg = "; ".join(i.get("message", "") for i in issues)
        return False, msg

    # ── Step 2: PATCH で価格・数量を確実に設定 ──
    # PUTだけでは Missing Offer になるケースがある（bulk_reactivate.pyと同パターン）
    # Amazon が PUT を非同期処理するため 10 秒待ってから PATCH する
    time.sleep(10)
    patch_body = {
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
    try:
        api.patch_listings_item(
            sellerId=seller_id,
            sku=sku,
            marketplaceIds=[MARKETPLACE_AU],
            body=patch_body,
        )
    except Exception as e:
        logger.warning("[catalog_discover] PATCH失敗（PUTは成功） %s: %s", sku, e)

    return True, sku


# ─────────────────────────────────────────────
# 7. メイン処理
# ─────────────────────────────────────────────

def discover_and_list(
    min_sellers: int = 3,
    max_new: int = 300,
    max_pages: int = 10,
    dry_run: bool = False,
):
    """
    競合セラーページスクレイピング → AU需要確認 → 利益確認 → 新規出品

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

    # ── Step 2: 競合セラーページスクレイピング → 新規ASIN ─────────
    seller_urls = config.SELLER_URLS
    if not seller_urls:
        logger.error("[catalog_discover] SELLER_URLS 未設定。終了")
        return [], 0, 0, 0, 0, 0
    logger.info("[catalog_discover] 競合セラー: %d件 / 最大%dページ/セラー",
                len(seller_urls), max_pages)
    new_asins = scrape_seller_asins(
        seller_urls=seller_urls,
        max_pages=max_pages,
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

    # AUセラー数チェックへ渡す候補を選定
    # get_competitive_pricing はAU競合が1人だと価格を返さないため、
    # 「AU価格なし」≠「AU競合なし」。AU価格なしの商品もセラー数チェックへ回す。
    # （セラー数チェックで get_item_offers を使うため実際の価格・競合数を確認できる）
    au_candidates = []
    skipped_no_au = 0
    for asin in asins_with_stock:
        jp_price, _ = jp_prices.get(asin, (None, False))
        if not jp_price:
            skipped_no_au += 1
            continue
        min_line = calc_optimal_au_price(jp_price, exchange_rate=exchange_rate)
        comp_price = au_bulk_prices.get(asin)
        # AU価格があって明らかに利益不足な場合のみスキップ（AU価格なしは通す）
        if comp_price is not None and comp_price < min_line:
            skipped_no_au += 1
            continue
        au_candidates.append(asin)

    logger.info("[catalog_discover] セラー数確認候補: %d件 (JP価格なし/AU明確赤字: %d件)",
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

        # NGワードチェック（Catalog APIでタイトル取得）
        try:
            cat_api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
            cat_resp = cat_api.get_catalog_item(asin, marketplaceIds=[MARKETPLACE_JP], includedData=["summaries"])
            title = ((cat_resp.payload or {}).get("summaries") or [{}])[0].get("itemName", "") or ""
        except Exception:
            title = ""
        if title:
            is_ng, ng_word = _check_ng_words(title, asin)
            if is_ng:
                logger.info("[catalog_discover] %s: NGワード「%s」検出 → スキップ", asin, ng_word)
                skipped_unprofitable += 1
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
    parser.add_argument("--max-pages", type=int, default=10, dest="max_pages",
                        help="セラーあたり最大スクレイピングページ数（デフォルト10）")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("[catalog_discover] *** DRY-RUN モード ***")
    logger.info("[catalog_discover] 設定: max_new=%d, min_sellers=%d, max_pages=%d",
                args.max_new, args.min_sellers, args.max_pages)

    listed_details, skipped_no_stock, skipped_no_au, skipped_few_sellers, skipped_unprofitable, failed_count = discover_and_list(
        min_sellers=args.min_sellers,
        max_new=args.max_new,
        max_pages=args.max_pages,
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

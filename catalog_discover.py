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
import sqlite3
from bs4 import BeautifulSoup
import io
import os
import re
import random
import sys
import time
import argparse

# Windows CP932 ターミナルでも日本語・記号が表示できるように
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
from typing import Optional

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems, CatalogItems, ListingsRestrictions
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.candidates_db import (
    init_db as _init_candidates_db,
    upsert_candidates,
    update_candidate,
    get_checked_today_asins,
    STATUS_LISTED, STATUS_NG, STATUS_RESTRICTED,
)
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
PATCH_INTERVAL = 0.3        # ListingsItems: 5 req/s
RESTRICTION_INTERVAL = 1.1  # ListingsRestrictions: 1 req/s

# Playwright スクレイピング用 User-Agent リスト（ランダム選択・最新Chrome）
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
]

# ソート順リスト: 毎回ランダムで変えることで同じセラーから異なる商品を発掘
_SORT_ORDERS = [
    "",                      # Featured（デフォルト）
    "&s=price-asc-rank",     # 価格の安い順
    "&s=price-desc-rank",    # 価格の高い順
    "&s=date-desc-rank",     # 新着順
    "&s=review-rank",        # カスタマーレビュー評価順
    "&s=relevanceblender",   # 関連性順（代替フィーチャード）
]

# 連続空ページ最大許容数: この数だけ既知ASINしかないページが続いたら終端とみなす
# ページ1が全部既知でも2,3ページ目に新規があるので 1 で打ち切らない
MAX_CONSECUTIVE_EMPTY = 3

# AU配送先ポストコード（Sydney）
AU_DELIVERY_POSTCODE = "2002"


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
        # active のみ除外（inactive/suppressed は候補DBで再チェック対象にする）
        if asin and len(asin) == 10 and item_status == "active":
            known.add(asin)

    logger.info("[catalog_discover] 既存active ASIN: %d件（inactive は候補DB経由で再チェック）",
                len(known))
    return known


# ─────────────────────────────────────────────
# 2a. Price Analyzer CSV から ASIN+価格を読み込む
# ─────────────────────────────────────────────

def load_price_analyzer_csvs() -> tuple:
    """
    Amazon Price Analyzer（ローカルGUIツール）の出力CSVを読み込む。
    CSVはローカルで Price Analyzer を実行することで生成される。

    CSV列: asin, price, title
    フォルダ: config.PRICE_ANALYZER_CSV_DIR（デフォルト: csv_output/AU/base）
    有効期限: config.PRICE_ANALYZER_CSV_MAX_AGE_HOURS 時間以内のファイルのみ使用

    Returns:
        (asins: list, csv_prices: dict {asin: float}, csv_titles: dict {asin: str})
        CSVがない・古い場合は ([], {}, {}) を返す
    """
    import glob
    from datetime import datetime, timedelta

    csv_dir = config.PRICE_ANALYZER_CSV_DIR
    if not os.path.isabs(csv_dir):
        csv_dir = os.path.join(os.path.dirname(__file__), csv_dir)

    pattern = os.path.join(csv_dir, "amazon_prices_*.csv")
    all_files = sorted(glob.glob(pattern), reverse=True)  # 最新順

    if not all_files:
        logger.info("[catalog_discover] Price Analyzer CSV なし (%s) → Playwrightスクレイピングにフォールバック", csv_dir)
        return [], {}, {}

    # max_age_hours 以内のファイルのみ使用（古い価格で出品しないため）
    max_age = config.PRICE_ANALYZER_CSV_MAX_AGE_HOURS
    cutoff = datetime.now() - timedelta(hours=max_age)
    recent_files = [f for f in all_files if datetime.fromtimestamp(os.path.getmtime(f)) >= cutoff]

    if not recent_files:
        logger.info(
            "[catalog_discover] Price Analyzer CSV: 全%d件が%d時間超 → スキップ",
            len(all_files), max_age,
        )
        return [], {}, {}

    logger.info(
        "[catalog_discover] Price Analyzer CSV: %d件使用（最新: %s）",
        len(recent_files), os.path.basename(recent_files[0]),
    )

    asins: list = []
    csv_prices: dict = {}
    csv_titles: dict = {}
    seen: set = set()

    for f in recent_files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    asin = str(row.get("asin", "") or "").strip()
                    if not asin or len(asin) != 10 or asin in seen:
                        continue
                    try:
                        price = float(row.get("price", 0) or 0)
                    except (ValueError, TypeError):
                        price = 0.0
                    title = str(row.get("title", "") or "").strip()

                    seen.add(asin)
                    asins.append(asin)
                    if price > 0:
                        csv_prices[asin] = price
                    if title:
                        csv_titles[asin] = title
        except Exception as e:
            logger.warning("[catalog_discover] CSV読み込みエラー %s: %s", f, e)

    logger.info(
        "[catalog_discover] Price Analyzer CSV 読み込み完了: ASIN %d件 / 価格あり %d件 / タイトルあり %d件",
        len(asins), len(csv_prices), len(csv_titles),
    )
    return asins, csv_prices, csv_titles


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
    scrape_prices: dict = {}  # {asin: au_price_aud} スクレイピングで取得したAU価格

    # プロキシリスト（セラーごとにローテーション）
    _proxy_list = config.PROXY_LIST if config.PROXY_USER else []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                # 本物ブラウザに近づける追加フラグ
                "--disable-infobars",
                "--window-size=1920,1080",
                "--start-maximized",
                "--disable-extensions",
                "--disable-gpu",
                "--ignore-certificate-errors",
                "--disable-web-security",
                "--allow-running-insecure-content",
            ],
        )

        def _make_context(browser, proxy_cfg):
            """ブラウザコンテキストを生成（人間っぽい設定）"""
            ua = random.choice(_USER_AGENTS)
            ctx = browser.new_context(
                viewport={
                    "width":  random.choice([1280, 1366, 1440, 1920]),
                    "height": random.choice([768, 800, 900, 1080]),
                },
                user_agent=ua,
                locale="en-AU",
                timezone_id="Australia/Sydney",
                proxy=proxy_cfg,
                extra_http_headers={
                    "Accept-Language": "en-AU,en;q=0.9,en-US;q=0.8",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                },
            )
            # webdriver検知を複数箇所で無効化
            ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-AU', 'en']});
                window.chrome = {runtime: {}};
                Object.defineProperty(navigator, 'permissions', {
                    get: () => ({query: () => Promise.resolve({state: 'granted'})})
                });
            """)
            return ctx

        def _set_au_delivery_location(page) -> bool:
            """
            Amazon AUの配送先をSydney（郵便番号2002）に設定する。
            Playwright ネイティブ API を優先使用（JavaScript より確実）。
            失敗してもスクレイピングは続行する（価格取得精度が落ちるだけ）。
            """
            try:
                page.goto("https://www.amazon.com.au", wait_until="domcontentloaded", timeout=20_000)
                _human_delay(1.5, 2.5)

                # CAPTCHA/ブロック確認
                url_now = page.url.lower()
                if "captcha" in url_now or "robot" in url_now or "sorry" in url_now:
                    logger.warning("[catalog_discover] 配送先設定: ホームページでCAPTCHA検出 → スキップ")
                    return False

                # 既に2002設定済みか確認
                try:
                    loc = page.inner_text("#glow-ingress-line2")
                    if AU_DELIVERY_POSTCODE in loc or "Sydney" in loc:
                        logger.debug("[catalog_discover] 配送先: 既にSydney(2002)設定済み")
                        return True
                except Exception:
                    pass

                # ── Step 1: 配送先変更ダイアログを開く ──
                # Playwright click を優先（より確実）、失敗したらJS evaluate にフォールバック
                opened = False
                for selector in ("#nav-global-location-popover-link", "#glow-ingress-block"):
                    try:
                        el = page.query_selector(selector)
                        if el:
                            el.click()
                            opened = True
                            break
                    except Exception:
                        pass
                if not opened:
                    page.evaluate(
                        "document.querySelector('#nav-global-location-popover-link,"
                        "#glow-ingress-block')?.click()"
                    )
                time.sleep(1.5)

                # ── Step 2: 郵便番号を入力 ──
                # wait_for_selector でダイアログのinputが出るまで最大3秒待つ
                INPUT_SELECTORS = (
                    'input[placeholder*="postcode" i]',
                    'input[placeholder*="postal" i]',
                    'input[placeholder*="zip" i]',
                    'input[placeholder*="code" i]',
                    'input[data-testid*="postal" i]',
                )
                filled = False
                for sel in INPUT_SELECTORS:
                    try:
                        page.wait_for_selector(sel, timeout=2000)
                        inp = page.query_selector(sel)
                        if inp:
                            inp.triple_click()   # 既存値を全選択してから上書き
                            inp.fill(AU_DELIVERY_POSTCODE)
                            filled = True
                            break
                    except Exception:
                        pass
                if not filled:
                    # フォールバック: JavaScript で直接設定
                    page.evaluate(f"""
                        const inputs = document.querySelectorAll('input[type="text"]');
                        for (const inp of inputs) {{
                            const ph = (inp.placeholder || '').toLowerCase();
                            if (ph.includes('post') || ph.includes('zip') || ph.includes('code')) {{
                                inp.value = '{AU_DELIVERY_POSTCODE}';
                                inp.dispatchEvent(new Event('input', {{bubbles: true}}));
                                inp.dispatchEvent(new Event('change', {{bubbles: true}}));
                                break;
                            }}
                        }}
                    """)
                time.sleep(0.6)

                # ── Step 3: Apply ボタンをクリック ──
                APPLY_SELECTORS = (
                    'button:has-text("Apply")',
                    'button:has-text("Done")',
                    'input[type="submit"]',
                )
                applied = False
                for sel in APPLY_SELECTORS:
                    try:
                        btn = page.query_selector(sel)
                        if btn:
                            btn.click()
                            applied = True
                            break
                    except Exception:
                        pass
                if not applied:
                    page.evaluate("""
                        const buttons = document.querySelectorAll('button');
                        for (const btn of buttons) {
                            const t = (btn.textContent || '').toLowerCase().trim();
                            if (t.includes('apply') || t === 'done') { btn.click(); break; }
                        }
                    """)
                time.sleep(1.5)

                # ── Step 4: 確認 ──
                try:
                    loc = page.inner_text("#glow-ingress-line2")
                    logger.info("[catalog_discover] 配送先設定後: [%s]", loc.strip())
                    if AU_DELIVERY_POSTCODE in loc or "Sydney" in loc:
                        logger.info("[catalog_discover] ✅ 配送先: Sydney(2002) 設定成功")
                        return True
                except Exception:
                    pass

                logger.warning("[catalog_discover] ⚠️ 配送先が2002になったか確認不可（スクレイピングは続行）")
                return False

            except Exception as e:
                logger.warning("[catalog_discover] 配送先設定失敗: %s", e)
                return False

        for raw_url in seller_urls:
            m = re.search(r"me=([A-Z0-9]+)", raw_url)
            if not m:
                logger.warning("[catalog_discover] seller_id 抽出失敗: %s", raw_url)
                continue
            seller_id = m.group(1)

            # ── プロキシ設定（失敗時はノープロキシにフォールバック）──
            proxy_cfg = None
            if _proxy_list:
                host, port = random.choice(_proxy_list)
                proxy_cfg = {
                    "server":   f"http://{host}:{port}",
                    "username": config.PROXY_USER,
                    "password": config.PROXY_PASS,
                }
                logger.info("[catalog_discover] Playwright: seller=%s 開始（proxy=%s:%s）",
                            seller_id, host, port)
            else:
                logger.info("[catalog_discover] Playwright: seller=%s 開始（proxyなし）",
                            seller_id)

            # セラーごとに新しいコンテキストを作成
            context = _make_context(browser, proxy_cfg)
            page = context.new_page()

            # ── 画像・メディア・フォントのみブロック（JSとCSSは通す = 本物ブラウザっぽく）──
            # スクリプトをブロックするとbot検知されやすいため通す
            page.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in ("image", "media", "font")
                else route.continue_(),
            )

            # ── AU配送先をSydney(2002)に設定 ──
            # JPのままだと価格が正しく表示されない or 価格0件になる
            _set_au_delivery_location(page)

            seller_new = 0
            # ソート順をランダムに選択（毎セラーごと）→ 同じセラーから異なる商品面を発掘
            sort_param = random.choice(_SORT_ORDERS)
            logger.info("[catalog_discover] seller=%s ソート順: %s",
                        seller_id, sort_param or "Featured（デフォルト）")

            consecutive_empty = 0  # 連続して新規ASINがなかったページ数

            # ★ while ループ: プロキシ失敗時に同じページを再試行できるよう for→while に変換
            # for の continue はページを advance するが、while + page_num -= 1 で同ページに戻れる
            page_num = 0
            while page_num < max_pages:
                page_num += 1
                page_url = (
                    f"https://www.amazon.com.au/s"
                    f"?me={seller_id}&marketplaceID={MARKETPLACE_AU}"
                    f"&page={page_num}{sort_param}"
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

                    # ── ASIN + AU価格 抽出（BeautifulSoup）──
                    content = page.content()
                    soup = BeautifulSoup(content, "html.parser")
                    page_new = 0
                    for item_el in soup.find_all(attrs={"data-asin": True}):
                        asin = item_el.get("data-asin", "")
                        if not asin or len(asin) != 10 or asin in seen:
                            continue
                        price_aud = None
                        try:
                            price_el = item_el.find("span", class_="a-offscreen")
                            if price_el:
                                price_text = price_el.get_text().replace("AU$", "").replace(",", "").strip()
                                price_aud = float(price_text)
                        except Exception:
                            pass
                        seen.add(asin)
                        all_asins.append(asin)
                        scrape_prices[asin] = price_aud
                        page_new += 1
                        seller_new += 1

                    logger.info("[catalog_discover] seller=%s page=%d: %d件新規（累計%d件）",
                                seller_id, page_num, page_new, len(all_asins))

                    # ── 次ページ判定 ──
                    # ★ page_new==0でも即打ち切らない（既知ASINだらけのページをスキップして先に進む）
                    # MAX_CONSECUTIVE_EMPTY ページ連続で新規なしの場合だけ終端とみなす
                    if page_new == 0:
                        consecutive_empty += 1
                    else:
                        consecutive_empty = 0

                    next_btn = page.query_selector(".s-pagination-next:not([aria-disabled])")
                    if not next_btn:
                        logger.info("[catalog_discover] seller=%s page=%d で最終ページ", seller_id, page_num)
                        break
                    if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                        logger.info(
                            "[catalog_discover] seller=%s page=%d: %d連続で新規なし → 終端",
                            seller_id, page_num, consecutive_empty,
                        )
                        break

                    # ── 次ページへ（長めに待つ = 人間が読んでいる） ──
                    _human_delay(3.0, 6.5)

                except PWTimeout:
                    logger.warning("[catalog_discover] タイムアウト seller=%s page=%d", seller_id, page_num)
                    break
                except Exception as e:
                    err_str = str(e)
                    # プロキシ接続失敗 → ノープロキシでリトライ
                    if proxy_cfg and ("ERR_TUNNEL_CONNECTION_FAILED" in err_str
                                      or "ERR_PROXY_CONNECTION_FAILED" in err_str
                                      or "ERR_CONNECTION_REFUSED" in err_str):
                        logger.warning(
                            "[catalog_discover] プロキシ接続失敗 seller=%s → ノープロキシでリトライ",
                            seller_id,
                        )
                        context.close()
                        proxy_cfg = None
                        context = _make_context(browser, None)
                        page = context.new_page()
                        page.route(
                            "**/*",
                            lambda route: route.abort()
                            if route.request.resource_type in ("image", "media", "font")
                            else route.continue_(),
                        )
                        _set_au_delivery_location(page)  # ノープロキシ再試行でも配送先を設定
                        consecutive_empty = 0
                        page_num -= 1  # ★ 同じページを再試行（whileループで再実行）
                        continue
                    logger.warning("[catalog_discover] エラー seller=%s page=%d: %s", seller_id, page_num, e)
                    break

            logger.info("[catalog_discover] seller=%s 完了: %d件収集", seller_id, seller_new)
            # セラーごとのコンテキストを閉じる（Cookie・セッションをリセット）
            context.close()
            # セラー間は長めに待つ（急ぎすぎない）
            _human_delay(5.0, 10.0)

        browser.close()

    logger.info("[catalog_discover] 全セラー 新規ASIN候補: %d件（価格取得済み: %d件）",
                len(all_asins), sum(1 for p in scrape_prices.values() if p))
    return all_asins, scrape_prices


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
        except Exception as e:
            logger.warning("[catalog_discover] JP価格タイムアウト (%s...): %s → スキップ", batch[0], e)
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
# 4b. AU FBM最安値取得（get_item_offers, FBAと自分を除外）
# ─────────────────────────────────────────────

def get_au_fbm_prices(asins: list) -> dict:
    """
    FBMセラーの最安値 {asin: price_aud} を返す。
    FBAセラー・自分は除外。FBMセラーが存在しない商品はキーを含まない。

    ★ FBAフィルタ: FBA最安値 < MIN_AU_LISTING_PRICE の商品は出品しない。
    FBAが安すぎる商品（例: FBA$15.99 vs 我々$45）は注文されると赤字になるため除外。
    """
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    result = {}
    my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
    total = len(asins)
    skipped_fba_cheap = 0

    for i, asin in enumerate(asins):
        if i % 50 == 0:
            logger.info("[catalog_discover] AU FBM価格確認中: %d/%d", i, total)
        for attempt in range(3):
            try:
                resp = api.get_item_offers(asin, item_condition="New")
                payload = resp.payload if hasattr(resp, "payload") else {}
                offers = payload.get("Offers", [])

                # 注意: get_item_offersは競合上位オファーしか返さないため
                # 3セラーが出品中でもレスポンスに含まれないことがある。
                # 3セラー確認はスクレイピング（ASINの発見元）で担保済みのため
                # ここでの追加チェックは行わない。
                fbm_prices = []
                fba_prices = []
                for offer in offers:
                    amount = offer.get("ListingPrice", {}).get("Amount")
                    if not amount:
                        continue
                    # ★ 商品価格＋送料の合計を競合価格とする（送料別建ての競合に対応）
                    shipping = float(offer.get("ShippingPrice", {}).get("Amount") or 0)
                    price = float(amount) + shipping
                    if offer.get("IsFulfilledByAmazon"):
                        fba_prices.append(price)  # FBA価格を記録
                        continue
                    if offer.get("SellerId", "") == my_id:
                        continue  # 自分除外
                    fbm_prices.append(price)

                # ★ FBAが安すぎる場合はスキップ（注文されたら赤字になる）
                if fba_prices and min(fba_prices) < config.MIN_AU_LISTING_PRICE:
                    logger.info(
                        "[catalog_discover] %s: FBA最安値AU$%.2f < フロアAU$%.2f → FBA安値スキップ",
                        asin, min(fba_prices), config.MIN_AU_LISTING_PRICE,
                    )
                    skipped_fba_cheap += 1
                    break

                if fbm_prices:
                    result[asin] = min(fbm_prices)
                break
            except SellingApiException as e:
                logger.warning("[catalog_discover] AU FBM価格エラー %s: %s", asin, e)
                break
            except Exception as e:
                if attempt < 2:
                    logger.warning(
                        "[catalog_discover] AU FBM価格タイムアウト %s (試行%d/3) → リトライ",
                        asin, attempt + 1,
                    )
                    time.sleep(5)
                else:
                    logger.warning("[catalog_discover] AU FBM価格失敗 %s: %s", asin, e)
        time.sleep(AU_PRICE_INTERVAL)

    logger.info(
        "[catalog_discover] AU FBM価格取得完了: %d件中 FBM価格あり %d件 / FBA安値スキップ %d件",
        total, len(result), skipped_fba_cheap,
    )
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
        for attempt in range(3):
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
                break
            except SellingApiException as e:
                logger.warning("[catalog_discover] get_item_offers エラー %s: %s → セラー数1とみなす", asin, e)
                result[asin] = {"seller_count": 1, "min_price": None}
                break
            except Exception as e:
                # ReadTimeout 等のネットワークエラー → リトライ
                if attempt < 2:
                    logger.warning("[catalog_discover] get_item_offers タイムアウト %s (試行%d/3) → リトライ", asin, attempt + 1)
                    time.sleep(5)
                else:
                    logger.warning("[catalog_discover] get_item_offers 失敗 %s: %s → セラー数1とみなす", asin, e)
                    result[asin] = {"seller_count": 1, "min_price": None}
        time.sleep(AU_PRICE_INTERVAL)

    three_plus = sum(1 for v in result.values() if v["seller_count"] >= 3)
    logger.info("[catalog_discover] セラー数確認完了: %d件 (3人以上: %d件)", total, three_plus)
    return result


# ─────────────────────────────────────────────
# 5b. NGワードチェック / 重量チェック
# ─────────────────────────────────────────────
import json as _json
import re as _re
from typing import Optional as _Optional_float  # 型ヒント用（重量返り値）

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


def _dim_to_cm(dim_obj: dict) -> _Optional_float:
    """寸法オブジェクト {"value": X, "unit": "..."} をcm単位に変換する"""
    value = dim_obj.get("value")
    unit = (dim_obj.get("unit") or "").lower()
    if value is None:
        return None
    value = float(value)
    if "centimeter" in unit or unit == "cm":
        return value
    elif "inch" in unit:
        return value * 2.54
    elif "millimeter" in unit or unit == "mm":
        return value / 10.0
    elif "meter" in unit:   # meter（centimeterを含まない）
        return value * 100.0
    return None


def _extract_effective_weight_kg(payload: dict) -> _Optional_float:
    """
    CatalogItems API レスポンスから有効重量(kg)を返す。

    DHL は「実重量」と「容積重量(L×W×H/5000)」の大きい方で課金する。
    例: マウスパッド 45×40×2cm → 容積重量 0.72kg（実重量 0.35kg より大）
        大型フィギュア箱 35×30×25cm → 容積重量 5.25kg（実重量 1.2kg より大）

    取得できない場合は None を返す（送料はデフォルト値を使用）。
    """
    try:
        dims = (payload or {}).get("dimensions") or []
        actual_kg = None
        vol_kg = None

        for dim in dims:
            # ── 実重量 ──
            w = dim.get("weight") or {}
            val = w.get("value")
            unit = (w.get("unit") or "").lower()
            if val is not None:
                val = float(val)
                if "kilogram" in unit or unit == "kg":
                    actual_kg = val
                elif "gram" in unit:
                    actual_kg = val / 1000.0
                elif "pound" in unit or unit == "lb":
                    actual_kg = val * 0.453592
                elif "ounce" in unit or unit == "oz":
                    actual_kg = val * 0.0283495

            # ── 容積重量 = L × W × H(cm) ÷ 5000 ──
            l_cm = _dim_to_cm(dim.get("length") or {})
            w_cm = _dim_to_cm(dim.get("width") or {})
            h_cm = _dim_to_cm(dim.get("height") or {})
            if l_cm and w_cm and h_cm:
                vol_kg = round((l_cm * w_cm * h_cm) / 5000.0, 3)

        candidates = [k for k in [actual_kg, vol_kg] if k is not None]
        if not candidates:
            return None
        effective = max(candidates)
        if vol_kg and actual_kg and vol_kg > actual_kg * 1.2:
            logger.debug(
                "[catalog_discover] 容積重量が実重量より大: 実%.2fkg → 容積%.2fkg（有効）",
                actual_kg, vol_kg,
            )
        return effective
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────
# 6. 出品制限チェック（ListingsRestrictions API）
# ─────────────────────────────────────────────

def check_listing_restriction(asin: str, seller_id: str) -> tuple:
    """
    ListingsRestrictions API でこの ASIN が自アカウントで出品可能か確認する。

    Amazon では一部のカテゴリ・ブランドを出品するには事前認証（Approval）が必要。
    認証なしで PUT しても INVALID/REJECTED になるためここで事前スクリーニングする。

    Returns:
        (is_restricted: bool, reason_code: str)
        is_restricted=False → 出品可能（または確認できなかった）
        is_restricted=True  → APPROVAL_REQUIRED / BRAND_NOT_AUTHORIZED / 等
    """
    try:
        api = ListingsRestrictions(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        resp = api.get_listings_restrictions(
            asin=asin,
            sellerId=seller_id,
            marketplaceIds=[MARKETPLACE_AU],
            conditionType="new_new",
        )
        restrictions = (resp.payload or {}).get("restrictions", [])
        if not restrictions:
            return False, ""       # 制限なし → 出品可能
        # 制限あり → 最初の reason_code を返す
        for r in restrictions:
            for reason in (r.get("reasons") or []):
                code = reason.get("reasonCode", "RESTRICTED")
                return True, code
        return True, "RESTRICTED"  # reasons が空でも restrictions が存在 → 制限あり

    except SellingApiException as e:
        # API エラー → 楽観的に通す（list_new_item が失敗すれば failed_count に記録される）
        logger.debug("[catalog_discover] ListingsRestrictions API エラー %s: %s", asin, e)
        return False, ""
    except Exception as e:
        logger.debug("[catalog_discover] ListingsRestrictions 予期しないエラー %s: %s", asin, e)
        return False, ""


# ─────────────────────────────────────────────
# 7. 新規出品
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

    # ── Step 2: ASIN収集（CSV優先 → Playwrightフォールバック）────────
    # Price Analyzer（ローカルGUIツール）のCSVが新しければそちらを優先使用。
    # CSVは実ブラウザ + AU配送先設定済みの正確な価格データを持つ。
    # CSVがない場合（GitHub Actions等）はPlaywrightスクレイピングにフォールバック。
    csv_asins, csv_prices, csv_titles = load_price_analyzer_csvs()

    if csv_asins:
        # ── CSV利用パス ──
        logger.info("[catalog_discover] Price Analyzer CSV: %d件 → Playwrightスクレイピングをスキップ", len(csv_asins))
        new_asins = [a for a in csv_asins if a not in existing_asins]
        scrape_prices = csv_prices  # CSV価格をscrape_pricesとして引き継ぐ
        logger.info("[catalog_discover] CSV由来の新規ASIN: %d件（既存%d件除外済み）",
                    len(new_asins), len(csv_asins) - len(new_asins))
    else:
        # ── Playwright スクレイピングパス（フォールバック）──
        seller_urls = config.SELLER_URLS
        if not seller_urls:
            logger.error("[catalog_discover] SELLER_URLS 未設定。終了")
            return [], 0, 0, 0, 0, 0
        logger.info("[catalog_discover] 競合セラー: %d件 / 最大%dページ/セラー",
                    len(seller_urls), max_pages)
        new_asins, scrape_prices = scrape_seller_asins(
            seller_urls=seller_urls,
            max_pages=max_pages,
            existing_asins=existing_asins,
        )
        csv_titles = {}

    if not new_asins:
        logger.info("[catalog_discover] 新規ASIN候補なし。終了")
        return [], 0, 0, 0, 0, 0, 0

    # ── 候補DBに全件保存（利益チェック前に保存 = 捨てない）──────────
    _init_candidates_db()
    db_added = upsert_candidates(new_asins)
    logger.info("[catalog_discover] 候補DB: +%d件新規追加（累計は recheck --stats で確認）",
                db_added)

    # recheck_candidates.py が今日すでにチェック済みのASINはスキップ
    checked_today = get_checked_today_asins()
    before = len(new_asins)
    new_asins = [a for a in new_asins if a not in checked_today]
    if before != len(new_asins):
        logger.info("[catalog_discover] recheck済みASIN除外: %d件 → %d件",
                    before, len(new_asins))

    # ── 既存DBのcandidate（未処理残留分）も追加処理 ──────────────────
    # 価格不足でスキップされ candidate のまま残っているASINをバックフィル
    try:
        con_bf = sqlite3.connect(config.DB_PATH)
        cur_bf = con_bf.cursor()
        cur_bf.execute(
            "SELECT asin FROM asin_candidates WHERE status='candidate' ORDER BY last_checked DESC LIMIT 500"
        )
        backfill = [r[0] for r in cur_bf.fetchall()]
        con_bf.close()
        new_set = set(new_asins)
        extra = [a for a in backfill if a not in new_set and a not in existing_asins]
        if extra:
            logger.info("[catalog_discover] 既存candidate backfill: %d件追加", len(extra))
            new_asins = new_asins + extra
    except Exception as e:
        logger.warning("[catalog_discover] backfill取得失敗: %s", e)

    logger.info("[catalog_discover] 新規ASIN候補: %d件", len(new_asins))

    # ── Step 3: JP価格確認 ──────────────────────────────────────────
    logger.info("[catalog_discover] JP価格確認: %d件 (約%.0f分)",
                len(new_asins), len(new_asins) / 20 * JP_INTERVAL / 60)
    jp_prices = get_jp_prices_bulk(new_asins)

    # JP在庫チェックをスキップ（ターゲットセラーが出品中＝仕入れ可能とみなす）
    asins_with_stock = new_asins
    skipped_no_stock = 0
    logger.info("[catalog_discover] JP在庫チェックスキップ: %d件全件対象", len(asins_with_stock))

    if not asins_with_stock:
        return [], skipped_no_stock, 0, 0, 0, 0, 0

    # ── Step 4 & 5: AU FBM価格を取得して出品候補を絞る ──
    # CSV由来のASINはすでに価格を持っているのでAPIコール不要。
    # Playwright由来 or CSV価格なしのASINのみAPIで取得。
    asins_need_api_price = [a for a in asins_with_stock if a not in scrape_prices]
    asins_have_csv_price = [a for a in asins_with_stock if a in scrape_prices]

    fbm_prices: dict = {}

    if asins_need_api_price:
        logger.info(
            "[catalog_discover] AU FBM価格確認（API）: %d件 (約%.0f分)"
            " / CSV価格あり: %d件（APIスキップ）",
            len(asins_need_api_price), len(asins_need_api_price) * AU_PRICE_INTERVAL / 60,
            len(asins_have_csv_price),
        )
        fbm_prices = get_au_fbm_prices(asins_need_api_price)
    else:
        logger.info("[catalog_discover] 全%d件にCSV価格あり → AU FBM API呼び出しスキップ",
                    len(asins_with_stock))

    # CSV価格をマージ（MIN_AU_LISTING_PRICE 以上のもののみ）
    csv_merged = 0
    for asin in asins_have_csv_price:
        csv_price = scrape_prices[asin]
        if csv_price and csv_price >= config.MIN_AU_LISTING_PRICE:
            fbm_prices[asin] = csv_price
            csv_merged += 1
        elif csv_price and csv_price < config.MIN_AU_LISTING_PRICE:
            # CSV価格がフロア未満でも MIN_AU_LISTING_PRICE で出品
            fbm_prices[asin] = config.MIN_AU_LISTING_PRICE
            logger.info("[catalog_discover] %s: CSV価格AU$%.2f < フロアAU$%.2f → フロア価格で出品",
                        asin, csv_price, config.MIN_AU_LISTING_PRICE)
            csv_merged += 1

    if csv_merged:
        logger.info("[catalog_discover] CSV価格マージ: %d件", csv_merged)

    au_candidates = [a for a in asins_with_stock if fbm_prices.get(a)]
    skipped_no_au = len(asins_with_stock) - len(au_candidates)
    logger.info(
        "[catalog_discover] 出品候補: %d件（FBM価格なし %d件スキップ）",
        len(au_candidates), skipped_no_au,
    )

    if not au_candidates:
        return [], skipped_no_stock, skipped_no_au, 0, 0, 0, 0

    seller_counts = {a: {"seller_count": 1, "min_price": fbm_prices[a]} for a in au_candidates}

    # ── Step 6 & 7: 利益確認 → 出品 ──────────────────────────────
    listings_api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    listed_details = []
    skipped_few_sellers = 0
    skipped_unprofitable = 0
    skipped_restricted = 0
    failed_count = 0

    for asin in au_candidates:
        if len(listed_details) + failed_count >= max_new:
            logger.info("[catalog_discover] 上限 %d件 に達しました", max_new)
            break

        offer_info = seller_counts.get(asin, {"seller_count": 0, "min_price": None})

        # 価格決定: スクレイプ/API価格 → 最低フロア(MIN_AU_LISTING_PRICE)以上を保証
        raw_price = offer_info["min_price"]
        if not raw_price:
            skipped_unprofitable += 1
            continue
        final_price = max(raw_price, config.MIN_AU_LISTING_PRICE)
        if final_price != raw_price:
            logger.info("[catalog_discover] %s: AU$%.2f < フロアAU$%.2f → フロア価格で出品",
                        asin, raw_price, config.MIN_AU_LISTING_PRICE)

        jp_price, _ = jp_prices.get(asin, (None, False))
        log_msg = (f"{asin}: AU${final_price:.2f}"
                   + (f" (JP¥{jp_price:,})" if jp_price else ""))

        if dry_run:
            logger.info("[catalog_discover][DRY-RUN] 出品予定 %s", log_msg)
            listed_details.append({
                "asin": asin, "jp_price": jp_price or 0,
                "au_price": final_price, "profit_rate": 0,
                "seller_count": offer_info["seller_count"],
            })
            continue

        # NGワード＋重量チェック（Catalog APIでタイトル・重量を同時取得）
        # CSV由来のタイトルがあればAPIコールをスキップ（重量はAPIが必要）
        # まずJPカタログで取得。JPにないASIN（AU専用）はAUカタログでフォールバック
        weight_kg = None

        # CSV由来タイトルがあればAPIを使わずタイトルを設定、重量取得のみAPI実行
        if asin in csv_titles:
            title = csv_titles[asin]
            # 重量はCatalog APIで取得（タイトルより重要）
            try:
                cat_api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
                cat_resp = cat_api.get_catalog_item(
                    asin,
                    marketplaceIds=[MARKETPLACE_JP],
                    includedData=["dimensions"],  # タイトル不要 → dimensionsのみ
                )
                payload = cat_resp.payload or {}
                weight_kg = _extract_effective_weight_kg(payload)
            except Exception:
                pass
        else:
            try:
                cat_api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
                cat_resp = cat_api.get_catalog_item(
                    asin,
                    marketplaceIds=[MARKETPLACE_JP],
                    includedData=["summaries", "dimensions"],
                )
                payload = cat_resp.payload or {}
                title = (payload.get("summaries") or [{}])[0].get("itemName", "") or ""
                weight_kg = _extract_effective_weight_kg(payload)  # 実重量 vs 容積重量の大きい方
            except Exception:
                title = ""
                weight_kg = None

        # JPでタイトルが取れなかった場合 → AUカタログでフォールバック
        # AU専用ASINはJPに存在しないためタイトルが空になる → NGチェックが効かないバグを防ぐ
        if not title:
            try:
                cat_au = CatalogItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
                cat_au_resp = cat_au.get_catalog_item(
                    asin,
                    marketplaceIds=[MARKETPLACE_AU],
                    includedData=["summaries"],
                )
                au_payload = cat_au_resp.payload or {}
                title = (au_payload.get("summaries") or [{}])[0].get("itemName", "") or ""
                if title:
                    logger.debug("[catalog_discover] %s: AUカタログからタイトル取得: %s", asin, title[:60])
            except Exception:
                pass

        # NGワードチェック（タイトルが空でもスキップしない）
        if title:
            is_ng, ng_word = _check_ng_words(title, asin)
            if is_ng:
                logger.info("[catalog_discover] %s: NGワード「%s」検出 → スキップ", asin, ng_word)
                update_candidate(asin, title=title, status=STATUS_NG,
                                 skip_reason=f"ng:{ng_word}")
                skipped_unprofitable += 1
                continue
        else:
            # タイトルがJP・AU両方で取れなかった → 安全のためスキップ
            logger.info("[catalog_discover] %s: タイトル取得不可 → 安全のためスキップ", asin)
            skipped_unprofitable += 1
            continue

        # 重量チェック（MAX_LISTING_WEIGHT_KG 超 → 無条件スキップ）
        # 1kg超は国際送料が高すぎるため出品しない（利益が出ていても送料リスクが大きい）
        if weight_kg is not None and weight_kg >= config.MAX_LISTING_WEIGHT_KG:
            logger.info(
                "[catalog_discover] %s: 重量%.2fkg >= 上限%.1fkg → 重量超過スキップ",
                asin, weight_kg, config.MAX_LISTING_WEIGHT_KG,
            )
            skipped_unprofitable += 1
            continue

        # ── 認証チェック（ListingsRestrictions API）────────────────────
        # ゲートカテゴリ・要認証ブランドを事前スクリーニング（PUT失敗を未然に防ぐ）
        is_restricted, restriction_code = check_listing_restriction(asin, seller_id)
        time.sleep(RESTRICTION_INTERVAL)
        if is_restricted:
            logger.info(
                "[catalog_discover] %s: 出品不可 [%s] → スキップ", asin, restriction_code
            )
            update_candidate(asin, status=STATUS_RESTRICTED,
                             skip_reason=f"restricted:{restriction_code}")
            skipped_restricted += 1
            continue

        try:
            ok, msg = list_new_item(listings_api, seller_id, asin, final_price)
            if ok:
                logger.info("[catalog_discover] 出品完了 %s (SKU: %s)", log_msg, msg)
                update_candidate(asin, status=STATUS_LISTED, listed_sku=msg,
                                 jp_price=jp_price, au_price=final_price,
                                 title=title, weight_kg=weight_kg)
                listed_details.append({
                    "asin": asin, "jp_price": jp_price or 0, "sku": msg,
                    "au_price": final_price, "profit_rate": 0,
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
        "/ AU競合なし・利益不足 %d件 / セラー不足 %d件 / 赤字 %d件 "
        "/ 認証不可 %d件 / 失敗 %d件",
        len(listed_details), skipped_no_stock, skipped_no_au,
        skipped_few_sellers, skipped_unprofitable, skipped_restricted, failed_count,
    )
    return (listed_details, skipped_no_stock, skipped_no_au,
            skipped_few_sellers, skipped_unprofitable, skipped_restricted, failed_count)


def build_email(
    listed_details, skipped_no_stock, skipped_no_au,
    skipped_few_sellers, skipped_unprofitable, skipped_restricted, failed_count,
    min_sellers: int, dry_run: bool,
) -> tuple:
    success_count = len(listed_details)
    dry_label = "[DRY-RUN] " if dry_run else ""
    subject = (
        f"[SP-API] {dry_label}カタログ発掘: "
        f"新規出品 {success_count}件 / 失敗 {failed_count}件"
    )
    total_checked = (success_count + skipped_no_stock + skipped_no_au +
                     skipped_few_sellers + skipped_unprofitable + skipped_restricted + failed_count)
    lines = [
        f"=== {dry_label}カタログ発掘 結果サマリー (競合{min_sellers}人以上) ===",
        "",
        f"JPカタログ候補ASIN:                   {total_checked}件",
        f"新規出品{'予定' if dry_run else '完了'}:                         {success_count}件",
        f"スキップ（JP在庫なし）:               {skipped_no_stock}件",
        f"スキップ（AU競合なし/利益不足）:      {skipped_no_au}件",
        f"スキップ（競合{min_sellers}人未満）:              {skipped_few_sellers}件",
        f"スキップ（競合価格が赤字ライン）:     {skipped_unprofitable}件",
        f"スキップ（認証不可・要申請）:         {skipped_restricted}件",
        f"失敗:                                 {failed_count}件",
        "",
    ]
    if skipped_restricted > 0:
        lines.append(
            "⚠️ 認証不可の商品が多い場合、出品したいカテゴリの申請を"
            "Seller Central > カタログ > 制限された商品 から行ってください。"
        )
        lines.append("")
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

    (listed_details, skipped_no_stock, skipped_no_au,
     skipped_few_sellers, skipped_unprofitable, skipped_restricted, failed_count) = discover_and_list(
        min_sellers=args.min_sellers,
        max_new=args.max_new,
        max_pages=args.max_pages,
        dry_run=args.dry_run,
    )

    subject, body = build_email(
        listed_details, skipped_no_stock, skipped_no_au,
        skipped_few_sellers, skipped_unprofitable, skipped_restricted, failed_count,
        min_sellers=args.min_sellers,
        dry_run=args.dry_run,
    )
    send_email(subject=subject, body=body)
    try:
        print(body)
    except UnicodeEncodeError:
        print(body.encode('utf-8', errors='replace').decode('ascii', errors='replace'))


if __name__ == "__main__":
    main()

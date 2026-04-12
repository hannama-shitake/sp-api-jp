import re
import time
import random
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import os
import config
from utils.logger import get_logger

logger = get_logger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

AU_BASE_URL = "https://www.amazon.com.au"

# Webshare プロキシリスト（環境変数から取得、なければ直接接続）
_PROXY_USER = os.getenv("PROXY_USER", "")
_PROXY_PASS = os.getenv("PROXY_PASS", "")
_PROXY_LIST = [
    ("31.59.20.176", "6754"),
    ("198.23.239.134", "6540"),
    ("45.38.107.97", "6014"),
    ("107.172.163.27", "6543"),
    ("198.105.121.200", "6462"),
    ("216.10.27.159", "6837"),
    ("142.111.67.146", "5611"),
    ("191.96.254.138", "6185"),
    ("31.58.9.4", "6077"),
    ("104.164.49.38", "7693"),
]


def _get_proxy() -> Optional[dict]:
    if not _PROXY_USER or not _PROXY_PASS:
        return None
    host, port = random.choice(_PROXY_LIST)
    proxy_url = f"http://{_PROXY_USER}:{_PROXY_PASS}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-AU,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _get_page(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    proxies = _get_proxy()
    headers = _get_headers()
    try:
        resp = session.get(url, headers=headers, proxies=proxies, timeout=20)
        if resp.status_code == 503:
            logger.warning("Amazon は一時的にブロックしています (503). プロキシを変えてリトライします...")
            time.sleep(5)
            proxies = _get_proxy()
            resp = session.get(url, headers=_get_headers(), proxies=proxies, timeout=20)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")
    except requests.RequestException as e:
        logger.error("ページ取得エラー: %s - %s", url, e)
        return None


def _parse_products_from_page(soup: BeautifulSoup) -> List[Dict]:
    products = []

    # Amazon AU の商品カードは data-asin 属性を持つ div
    items = soup.select("div[data-asin]")
    for item in items:
        asin = item.get("data-asin", "").strip()
        if not asin or len(asin) != 10:
            continue

        # タイトル
        title_el = item.select_one("h2 a span, h2 span")
        title = title_el.get_text(strip=True) if title_el else ""

        # 価格（AUD）
        # 方法1: .a-price .a-offscreen（例: "$29.95"）
        price_aud = None
        offscreen = item.select_one("span.a-price span.a-offscreen")
        if offscreen:
            try:
                price_text = re.sub(r"[^\d.]", "", offscreen.get_text(strip=True))
                if price_text:
                    price_aud = float(price_text)
            except ValueError:
                pass

        # 方法2: .a-price-whole + .a-price-fraction（フォールバック）
        if price_aud is None:
            price_whole = item.select_one(".a-price-whole")
            price_frac = item.select_one(".a-price-fraction")
            if price_whole:
                try:
                    whole = re.sub(r"[^\d]", "", price_whole.get_text())
                    frac = re.sub(r"[^\d]", "", price_frac.get_text()) if price_frac else "0"
                    if whole:
                        price_aud = float(f"{whole}.{frac}")
                except ValueError:
                    pass

        # 商品URL
        link_el = item.select_one("h2 a")
        product_url = ""
        if link_el and link_el.get("href"):
            product_url = AU_BASE_URL + link_el["href"]

        products.append({
            "asin": asin,
            "title": title,
            "au_price_aud": price_aud,
            "product_url": product_url,
        })
        logger.debug("ASIN取得: %s | %s | AUD %.2f", asin, title[:40], price_aud or 0)

    return products


def _get_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    next_el = soup.select_one("a.s-pagination-next, li.a-last a")
    if next_el and next_el.get("href"):
        href = next_el["href"]
        if href.startswith("http"):
            return href
        return AU_BASE_URL + href
    return None


def _set_au_delivery_location(session: requests.Session, postcode: str = "2000") -> bool:
    """
    セッションの配送先をオーストラリア（デフォルト: Sydney 2000）に設定する。
    海外IPからアクセスすると「Currently unavailable」になるため、AU郵便番号を設定して回避。
    """
    proxies = _get_proxy()
    try:
        resp = session.get(AU_BASE_URL, headers=_get_headers(), proxies=proxies, timeout=20)
        soup = BeautifulSoup(resp.text, "lxml")
        csrf = ""
        for inp in soup.select('input[name="anti-csrftoken-a2z"]'):
            csrf = inp.get("value", "")
            break

        change_headers = {
            **_get_headers(),
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/x-www-form-urlencoded",
        }
        data = {
            "locationType": "LOCATION_INPUT",
            "zipCode": postcode,
            "storeContext": "generic",
            "deviceType": "web",
            "pageType": "Search",
            "actionSource": "glow",
            "anti-csrftoken-a2z": csrf,
        }
        r = session.post(
            f"{AU_BASE_URL}/gp/delivery/ajax/address-change.html",
            headers=change_headers,
            data=data,
            proxies=proxies,
            timeout=15,
        )
        success = r.status_code == 200 and '"successful":1' in r.text
        if success:
            logger.info("[scraper] 配送先をAU(%s)に設定しました", postcode)
        else:
            logger.warning("[scraper] 配送先設定に失敗 (status=%d)", r.status_code)
        return success
    except Exception as e:
        logger.warning("[scraper] 配送先設定エラー: %s", e)
        return False


def scrape_seller_products(seller_url: str, max_pages: int = None) -> List[Dict]:
    """
    Amazon AU のセラーストアページから商品一覧 (ASIN, タイトル, 価格) を取得する。

    Args:
        seller_url: セラーストアの URL
            例: https://www.amazon.com.au/s?me=AXXX&marketplaceID=A39IBJ37TRP1C6
        max_pages: 最大取得ページ数（None で config.SCRAPER_MAX_PAGES を使用）

    Returns:
        商品情報のリスト [{"asin": ..., "title": ..., "au_price_aud": ..., "product_url": ...}]
    """
    if max_pages is None:
        max_pages = config.SCRAPER_MAX_PAGES

    session = requests.Session()
    _set_au_delivery_location(session)  # 配送先をAUに設定（価格表示のため）
    all_products: List[Dict] = []
    seen_asins = set()
    current_url = seller_url
    page = 1

    while current_url and page <= max_pages:
        logger.info("[scraper] ページ %d を取得中: %s", page, current_url)
        soup = _get_page(current_url, session)
        if soup is None:
            break

        products = _parse_products_from_page(soup)
        for p in products:
            if p["asin"] not in seen_asins:
                seen_asins.add(p["asin"])
                all_products.append(p)

        logger.info("[scraper] ページ %d: %d件 取得 (累計 %d件)", page, len(products), len(all_products))

        current_url = _get_next_page_url(soup)
        page += 1

        if current_url:
            time.sleep(config.SCRAPER_REQUEST_DELAY)

    logger.info("[scraper] 完了: 合計 %d 件の商品を取得", len(all_products))
    return all_products

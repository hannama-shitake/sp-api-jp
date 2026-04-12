import re
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import config
from utils.logger import get_logger

logger = get_logger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

AU_BASE_URL = "https://www.amazon.com.au"


def _get_page(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 503:
            logger.warning("Amazon は一時的にブロックしています (503). 少し待ってリトライします...")
            time.sleep(10)
            resp = session.get(url, headers=HEADERS, timeout=15)
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
        price_aud = None
        price_whole = item.select_one(".a-price-whole")
        price_frac = item.select_one(".a-price-fraction")
        if price_whole:
            try:
                whole = re.sub(r"[^\d]", "", price_whole.get_text())
                frac = re.sub(r"[^\d]", "", price_frac.get_text()) if price_frac else "0"
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

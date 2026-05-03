"""
スクレイパーの価格取得デバッグ用スクリプト
実際のHTMLを保存して、価格セレクターを確認する
"""
import re
import os
import random
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

_PROXY_USER = os.getenv("PROXY_USER", "")
_PROXY_PASS = os.getenv("PROXY_PASS", "")
_PROXY_LIST = [
    ("31.59.20.176", "6754"),
    ("198.23.239.134", "6540"),
    ("45.38.107.97", "6014"),
]

SELLER_URL = os.getenv("SELLER_URL", "")


def get_proxy():
    if not _PROXY_USER or not _PROXY_PASS:
        return None
    host, port = random.choice(_PROXY_LIST)
    proxy_url = f"http://{_PROXY_USER}:{_PROXY_PASS}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}


def fetch_page(url):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-AU,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    proxies = get_proxy()
    print(f"プロキシ: {proxies is not None}")
    resp = requests.get(url, headers=headers, proxies=proxies, timeout=20)
    print(f"ステータス: {resp.status_code}")
    return resp.text


def analyze_html(html):
    soup = BeautifulSoup(html, "lxml")

    # HTMLを保存
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("HTMLを debug_page.html に保存しました")

    # data-asin を持つ div を確認
    items = soup.select("div[data-asin]")
    print(f"\ndata-asin div 数: {len(items)}")

    if not items:
        print("ERROR: data-asin div が見つかりません。ページ構造が変わった可能性があります。")
        # ページのタイトルを確認
        title = soup.find("title")
        print(f"ページタイトル: {title.get_text() if title else 'なし'}")
        return

    # 最初の3件を詳しく確認
    for i, item in enumerate(items[:3]):
        asin = item.get("data-asin", "").strip()
        if not asin or len(asin) != 10:
            continue
        print(f"\n--- 商品 {i+1}: ASIN={asin} ---")

        # タイトル
        title_el = item.select_one("h2 a span, h2 span")
        print(f"タイトル: {title_el.get_text(strip=True)[:60] if title_el else 'なし'}")

        # 価格要素を全て探す
        price_elements = item.select("[class*='price']")
        print(f"price系クラス要素数: {len(price_elements)}")
        for pe in price_elements[:5]:
            print(f"  クラス={pe.get('class')}, テキスト='{pe.get_text(strip=True)[:30]}'")

        # .a-price-whole を確認
        whole = item.select_one(".a-price-whole")
        frac = item.select_one(".a-price-fraction")
        print(f".a-price-whole: {whole.get_text(strip=True) if whole else 'なし'}")
        print(f".a-price-fraction: {frac.get_text(strip=True) if frac else 'なし'}")

        # span.a-price を確認（新しい構造）
        price_span = item.select_one("span.a-price")
        if price_span:
            print(f"span.a-price: {price_span.get_text(strip=True)[:30]}")
            offscreen = price_span.select_one(".a-offscreen")
            if offscreen:
                print(f"  .a-offscreen: {offscreen.get_text(strip=True)}")

        # aria-label で価格を探す
        price_aria = item.select_one("[aria-label*='$'], [aria-label*='AUD']")
        if price_aria:
            print(f"aria-label 価格: {price_aria.get('aria-label')}")


if __name__ == "__main__":
    if not SELLER_URL:
        print("ERROR: SELLER_URL が .env に設定されていません")
        exit(1)

    print(f"URL: {SELLER_URL[:80]}...")
    print("ページを取得中...")
    html = fetch_page(SELLER_URL)
    analyze_html(html)

"""
Amazon AU Price Analyzer
========================
seller_urls.txt に登録した3セラーのAU出品ページを
Selenium実ブラウザでスクレイピングし、ASIN・価格・タイトルをCSVに出力する。

使い方:
  python price_analyzer.py              # 全セラー・全ページ
  python price_analyzer.py --max-pages 5  # セラーあたり最大5ページ

出力先:
  csv_output/AU/base/amazon_prices_YYYYMMDD_HHMMSS.csv
  列: asin, price, title

前提:
  pip install selenium webdriver-manager
"""

import os
import re
import sys
import csv
import time
import random
import argparse
import datetime
import traceback

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ── 設定 ─────────────────────────────────────────────────────────────
_BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
CSV_OUTPUT_DIR = os.path.join(_BASE_DIR, "csv_output", "AU", "base")
AU_POSTCODE    = "2002"   # Sydney

# ソート順リスト: 毎回ランダムに選択 → 同じセラーから毎回異なる商品を発掘
_SORT_ORDERS = [
    "",                       # Featured（デフォルト）
    "&s=price-asc-rank",      # 価格の安い順
    "&s=price-desc-rank",     # 価格の高い順
    "&s=date-desc-rank",      # 新着順
    "&s=review-rank",         # カスタマーレビュー評価順
    "&s=relevanceblender",    # 関連性順
]


# ── セラーURL読み込み ─────────────────────────────────────────────────
def load_seller_urls() -> list:
    txt = os.path.join(_BASE_DIR, "seller_urls.txt")
    if not os.path.exists(txt):
        print(f"[ERROR] seller_urls.txt が見つかりません: {txt}")
        sys.exit(1)
    urls = [
        l.strip() for l in open(txt, encoding="utf-8")
        if l.strip() and not l.strip().startswith("#")
    ]
    print(f"[INFO] セラー {len(urls)} 件読み込み")
    return urls


# ── ブラウザ起動 ──────────────────────────────────────────────────────
def create_driver() -> webdriver.Chrome:
    options = Options()
    # ★ headless なし（実ブラウザ表示）→ Amazon のbot検知を回避
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")          # ★ ウィンドウを最大化して前面表示
    options.add_argument("--window-position=100,100")  # ★ 画面左上に配置
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    # webdriver フラグを消す
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"}
    )
    return driver


# ── AU配送先をSydney(2002)に設定 ─────────────────────────────────────
def set_delivery_location(driver) -> bool:
    try:
        print("[INFO] 配送先をSydney(2002)に設定中...")
        driver.get("https://www.amazon.com.au")
        time.sleep(2)

        # 既に設定済みか確認
        try:
            loc = driver.find_element(By.ID, "glow-ingress-line2").text
            if AU_POSTCODE in loc or "Sydney" in loc:
                print(f"[INFO] ✅ 配送先設定済み: {loc}")
                return True
        except Exception:
            pass

        # ダイアログを開く
        driver.execute_script(
            "document.querySelector('#nav-global-location-popover-link,"
            "#glow-ingress-block')?.click();"
        )
        time.sleep(1.5)

        # 郵便番号を入力（Seleniumのfindで試みる → 失敗したらJS）
        filled = False
        for placeholder in ["postcode", "postal", "zip", "code"]:
            try:
                inp = driver.find_element(
                    By.CSS_SELECTOR, f'input[placeholder*="{placeholder}" i]'
                )
                inp.clear()
                inp.send_keys(AU_POSTCODE)
                filled = True
                break
            except Exception:
                pass
        if not filled:
            driver.execute_script(f"""
                const inputs = document.querySelectorAll('input[type="text"]');
                for (const inp of inputs) {{
                    const ph = (inp.placeholder || '').toLowerCase();
                    if (ph.includes('post') || ph.includes('zip') || ph.includes('code')) {{
                        inp.value = '{AU_POSTCODE}';
                        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
                        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
                        break;
                    }}
                }}
            """)
        time.sleep(0.6)

        # Applyボタン
        applied = False
        for text in ["Apply", "Done", "Continue"]:
            try:
                btns = driver.find_elements(By.XPATH, f"//button[contains(.,'{text}')]")
                if btns:
                    btns[0].click()
                    applied = True
                    break
            except Exception:
                pass
        if not applied:
            driver.execute_script("""
                const buttons = document.querySelectorAll('button');
                for (const btn of buttons) {
                    const t = (btn.textContent||'').toLowerCase().trim();
                    if (t.includes('apply')||t==='done') { btn.click(); break; }
                }
            """)
        time.sleep(1.5)

        # 確認
        try:
            loc = driver.find_element(By.ID, "glow-ingress-line2").text
            if AU_POSTCODE in loc or "Sydney" in loc:
                print(f"[INFO] ✅ 配送先設定成功: {loc}")
                return True
            else:
                print(f"[WARN] 配送先が変わっていない可能性: {loc}")
        except Exception:
            print("[WARN] 配送先の確認ができません（続行）")
        return False

    except Exception as e:
        print(f"[WARN] 配送先設定中にエラー: {e}")
        return False


# ── 価格・タイトル抽出 ────────────────────────────────────────────────
def _get_price(item) -> float:
    selectors = [
        ".a-price .a-offscreen",
        ".a-price-whole",
        ".a-price",
        ".a-color-price",
    ]
    for sel in selectors:
        try:
            el = item.find_element(By.CSS_SELECTOR, sel)
            text = el.get_attribute("innerHTML") or el.text
            # 数字と小数点のみ抽出
            nums = re.sub(r"[^\d.]", "", text.replace(",", ""))
            if nums:
                val = float(nums)
                if val > 0:
                    return round(val, 2)
        except Exception:
            pass
    return 0.0


def _get_title(item) -> str:
    selectors = [
        "h2 .a-link-normal",
        ".a-text-normal",
        ".a-link-normal h2",
        "h2",
    ]
    for sel in selectors:
        try:
            el = item.find_element(By.CSS_SELECTOR, sel)
            t = el.text.strip()
            if t:
                return t
        except Exception:
            pass
    return ""


# ── 1セラーを全ページスクレイピング ──────────────────────────────────
def scrape_seller(driver, url: str, max_pages: int, seen: set) -> list:
    m = re.search(r"me=([A-Z0-9]+)", url)
    seller_id = m.group(1) if m else url

    # ★ ソート順をランダムに選択（毎回異なる商品を発掘）
    sort_param = random.choice(_SORT_ORDERS)
    sort_label = sort_param.replace("&s=", "") or "Featured（デフォルト）"
    print(f"\n[INFO] ── セラー {seller_id} 開始（ソート: {sort_label}）──")

    products = []
    page_num  = 0

    # ページURLを構築（pageパラメータ付き）
    base_url = url if "?" in url else url + "?"

    while page_num < max_pages:
        page_num += 1
        page_url = base_url + sort_param + f"&page={page_num}"

        try:
            driver.get(page_url)
            time.sleep(2.5)

            # CAPTCHA/ブロック確認
            current_url = driver.current_url.lower()
            if "captcha" in current_url or "robot" in current_url:
                print(f"[WARN] CAPTCHA検出 page={page_num} → セラー {seller_id} スキップ")
                break
            if driver.find_elements(By.ID, "captchacharacters"):
                print(f"[WARN] CAPTCHAフォーム検出 → セラー {seller_id} スキップ")
                break

            # 商品要素を取得
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'div[data-asin]:not([data-asin=""])')
                    )
                )
            except Exception:
                print(f"[WARN] page={page_num} 商品要素タイムアウト → 終了")
                break

            items = driver.find_elements(
                By.CSS_SELECTOR, 'div[data-asin]:not([data-asin=""])'
            )
            page_new = 0
            for item in items:
                try:
                    asin = item.get_attribute("data-asin") or ""
                    if not asin or len(asin) != 10 or asin in seen:
                        continue
                    price = _get_price(item)   # 0でもスキップしない（ASINだけでも価値あり）
                    title = _get_title(item)
                    seen.add(asin)
                    products.append({"asin": asin, "price": price, "title": title})
                    page_new += 1
                except Exception:
                    pass

            print(f"[INFO] seller={seller_id} page={page_num}: {page_new}件新規 / 累計{len(products)}件")

            # 次ページボタン確認
            next_btns = driver.find_elements(
                By.CSS_SELECTOR, ".s-pagination-next:not([aria-disabled])"
            )
            if not next_btns:
                print(f"[INFO] seller={seller_id} 最終ページ（page={page_num}）")
                break

            time.sleep(1.5)

        except Exception as e:
            print(f"[WARN] seller={seller_id} page={page_num} エラー: {e}")
            break

    print(f"[INFO] seller={seller_id} 完了: {len(products)}件収集")
    return products


# ── CSV保存 ──────────────────────────────────────────────────────────
def save_csv(products: list) -> str:
    os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
    ts  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(CSV_OUTPUT_DIR, f"amazon_prices_{ts}.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["asin", "price", "title"])
        writer.writeheader()
        writer.writerows(products)
    print(f"\n[INFO] CSV保存完了: {out}")
    print(f"[INFO] 合計 {len(products)} 件")
    return out


# ── メイン ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Amazon AU Price Analyzer")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="セラーあたり最大ページ数（デフォルト: 100 = 全件）")
    args = parser.parse_args()

    seller_urls = load_seller_urls()
    driver      = create_driver()
    all_products = []
    seen_asins   = set()

    try:
        # 1. AU配送先をSydney(2002)に設定（自動試行 → 失敗なら手動）
        driver.get("https://www.amazon.com.au")
        time.sleep(3)
        set_delivery_location(driver)
        time.sleep(1)

        # 配送先確認 → 未設定なら手動で設定してもらう
        try:
            loc = driver.find_element(By.ID, "glow-ingress-line2").text
        except Exception:
            loc = ""
        if "2002" not in loc and "Sydney" not in loc.lower():
            print("\n" + "="*60)
            print("⚠️  配送先の自動設定に失敗しました。")
            print("   Chromeウィンドウを開いて手動で設定してください：")
            print("   1. 左上の配送先（📍マーク）をクリック")
            print("   2. 郵便番号に「2002」を入力")
            print("   3. 「Apply」ボタンをクリック")
            print("="*60)
            input("   設定が完了したらここでEnterを押してください → ")
            print("[INFO] 手動設定完了。スクレイピングを開始します...")
        else:
            print(f"[INFO] ✅ 配送先確認: {loc}")

        # 2. 3セラーを順番にスクレイピング
        for url in seller_urls:
            products = scrape_seller(driver, url, args.max_pages, seen_asins)
            all_products.extend(products)
            time.sleep(3)   # セラー間のインターバル

    except KeyboardInterrupt:
        print("\n[INFO] ユーザーによる中断")
    finally:
        driver.quit()

    # 3. CSV保存
    if all_products:
        save_csv(all_products)
        print("\n✅ 完了。次のコマンドで出品してください:")
        print("   cd C:\\Users\\user\\Desktop\\sp-api")
        print("   python catalog_discover.py --max-new 300")
    else:
        print("\n[WARN] 商品が1件も取得できませんでした")


if __name__ == "__main__":
    main()

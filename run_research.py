import sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

import logging
# ログをファイルに出力
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler("research_log.txt", encoding="utf-8", mode="w"),
        logging.StreamHandler(sys.stdout),
    ]
)

from db.database import init_db
from apis import amazon_au
from apis.exchange_rate import get_jpy_to_aud
from scraper.au_seller import scrape_seller_products
from modules.product_matcher import match_and_research
import config

URL = "https://www.amazon.com.au/s?me=A3I4NYJKP3GMMP&marketplaceID=A39IBJ37TRP1C6"

# Seller ID 設定
seller_id = os.getenv("AMAZON_AU_SELLER_ID", "A42QCBK09XFXL")
amazon_au.set_seller_id(seller_id)
init_db()

logging.info("スクレイプ開始: %s", URL)
products = scrape_seller_products(URL)
logging.info("スクレイプ完了: %d件", len(products))

profitable = match_and_research(products, dry_run=True)
logging.info("利益商品: %d件 (MIN_PROFIT_RATE=%.0f%%)", len(profitable), config.MIN_PROFIT_RATE)

for r in profitable[:10]:
    logging.info("  ASIN=%s title=%s AU=%.2f 粗利率=%.1f%%",
                 r.asin, (r.title or "")[:40], r.au_price_aud, r.profit_rate)

with open("research_done.txt", "w", encoding="utf-8") as f:
    f.write(f"件数: {len(profitable)}\n")
    for r in profitable[:10]:
        f.write(f"{r.asin} {r.profit_rate:.1f}% AU${r.au_price_aud:.2f} {r.title[:40]}\n")

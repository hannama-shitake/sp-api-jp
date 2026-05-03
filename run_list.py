import sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("list_log.txt", encoding="utf-8", mode="w"),
    ]
)

from db.database import init_db
from apis import amazon_au
from apis.exchange_rate import get_jpy_to_aud
from scraper.au_seller import scrape_seller_products
from modules.product_matcher import match_and_research
from modules.listing_manager import list_profitable_products
import config

URL = "https://www.amazon.com.au/s?me=A3I4NYJKP3GMMP&marketplaceID=A39IBJ37TRP1C6"

seller_id = os.getenv("AMAZON_AU_SELLER_ID", "A42QCBK09XFXL")
amazon_au.set_seller_id(seller_id)
init_db()

logging.info("=== スクレイプ開始 ===")
products = scrape_seller_products(URL)
logging.info("スクレイプ完了: %d件", len(products))

profitable = match_and_research(products, dry_run=False)
logging.info("利益商品: %d件", len(profitable))

if not profitable:
    logging.info("利益商品なし。終了")
else:
    result = list_profitable_products(profitable, dry_run=False)
    logging.info("=== 出品結果 ===")
    logging.info("成功: %d件", result["success"])
    logging.info("スキップ: %d件", result["skipped"])
    logging.info("失敗: %d件", result["failed"])

with open("list_done.txt", "w", encoding="utf-8") as f:
    f.write("完了\n")

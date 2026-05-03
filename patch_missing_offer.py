"""
Missing Offer になっている DB 上の listings を全件 PATCH し直す。
PUT 後に Amazon が非同期処理するため、0.5秒では間に合わない問題の対処。
"""
import sqlite3, time, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
import config
from utils.logger import get_logger

logger = get_logger(__name__)

MARKETPLACE_AU = config.MARKETPLACE_AU
_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
SELLER_ID = config.AMAZON_AU_CREDENTIALS.get("seller_id") or __import__('os').getenv("AMAZON_AU_SELLER_ID","")

# DB から active な listings + 価格を取得
conn = sqlite3.connect("arbitrage.db")
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT l.asin, l.sku, p.au_price_aud
    FROM listings l
    LEFT JOIN products p ON l.asin = p.asin
    WHERE l.status = 'active'
    ORDER BY l.listed_at DESC
""").fetchall()
conn.close()

print(f"対象: {len(rows)}件")
api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

success = failed = 0
for row in rows:
    asin = row["asin"]
    sku  = row["sku"]
    price = float(row["au_price_aud"]) if row["au_price_aud"] else None

    if not price:
        print(f"  SKIP {asin} - 価格なし")
        continue

    body = {
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
                    "our_price": [{"schedule": [{"value_with_tax": price}]}],
                    "marketplace_id": MARKETPLACE_AU,
                }],
            },
        ],
    }
    try:
        resp = api.patch_listings_item(
            sellerId=SELLER_ID, sku=sku,
            marketplaceIds=[MARKETPLACE_AU], body=body,
        )
        status = resp.payload.get("status", "?")
        issues = resp.payload.get("issues", [])
        issue_msg = "; ".join(i.get("message","") for i in issues)
        print(f"  {'OK' if status in ('ACCEPTED','VALID') else 'NG'} {asin} AUD${price:.2f} → {status} {issue_msg}")
        if status in ("ACCEPTED", "VALID"):
            success += 1
        else:
            failed += 1
    except Exception as e:
        print(f"  ERR {asin}: {e}")
        failed += 1

    time.sleep(0.5)

print(f"\n完了: 成功 {success}件 / 失敗 {failed}件")

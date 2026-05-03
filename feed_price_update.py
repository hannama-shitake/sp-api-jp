"""
Feeds API (POST_PRODUCT_PRICING_DATA + POST_INVENTORY_AVAILABILITY_DATA) で
価格・在庫を一括セット。AU marketplace は POST_FLAT_FILE_LISTINGS_FEED 非対応。
Missing Offer になっている listings の価格・数量を XML フィードで確実に設定する。
"""
import sqlite3, time, sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from sp_api.api.feeds.feeds_2021_06_30 import FeedsV20210630
from sp_api.base import Marketplaces
import config
from utils.logger import get_logger

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
SELLER_ID = config.AMAZON_AU_CREDENTIALS.get("seller_id") or os.getenv("AMAZON_AU_SELLER_ID", "")

# DB から active な listings + 価格を取得
conn = sqlite3.connect("arbitrage.db")
conn.row_factory = sqlite3.Row
rows = conn.execute("""
    SELECT l.asin, l.sku, p.au_price_aud
    FROM listings l
    LEFT JOIN products p ON l.asin = p.asin
    WHERE l.status = 'active' AND p.au_price_aud IS NOT NULL
    ORDER BY l.listed_at DESC
""").fetchall()
conn.close()

print(f"対象: {len(rows)}件  SELLER_ID: {SELLER_ID[:6]}...")

# ── XML フィード作成 ────────────────────────────────────────────
# POST_PRODUCT_PRICING_DATA : 価格更新
price_messages = ""
inv_messages   = ""
for i, row in enumerate(rows, 1):
    sku   = row["sku"]
    price = float(row["au_price_aud"])

    price_messages += f"""  <Message>
    <MessageID>{i}</MessageID>
    <Price>
      <SKU>{sku}</SKU>
      <StandardPrice currency="AUD">{price:.2f}</StandardPrice>
    </Price>
  </Message>
"""
    inv_messages += f"""  <Message>
    <MessageID>{i}</MessageID>
    <Inventory>
      <SKU>{sku}</SKU>
      <Available>true</Available>
      <Quantity>1</Quantity>
      <FulfillmentLatency>{config.HANDLING_TIME_DAYS}</FulfillmentLatency>
    </Inventory>
  </Message>
"""

price_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:noNamespaceSchemaLocation="amzn-envelope.xsd">
  <Header>
    <DocumentVersion>1.01</DocumentVersion>
    <MerchantIdentifier>{SELLER_ID}</MerchantIdentifier>
  </Header>
  <MessageType>Price</MessageType>
{price_messages}</AmazonEnvelope>"""

inv_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:noNamespaceSchemaLocation="amzn-envelope.xsd">
  <Header>
    <DocumentVersion>1.01</DocumentVersion>
    <MerchantIdentifier>{SELLER_ID}</MerchantIdentifier>
  </Header>
  <MessageType>Inventory</MessageType>
{inv_messages}</AmazonEnvelope>"""

feeds_api = FeedsV20210630(credentials=_AU_CREDS, marketplace=Marketplaces.AU)


def submit_and_wait(feed_type: str, xml_content: str, label: str) -> bool:
    """フィードを送信して完了を待つ。成功したら True を返す。"""
    print(f"\n=== {label} フィード送信中 ===")
    file_obj = io.StringIO(xml_content)
    doc_resp, feed_resp = feeds_api.submit_feed(
        feed_type,
        file_obj,
        content_type="text/xml; charset=UTF-8",
        marketplaceIds=[config.MARKETPLACE_AU],
    )
    feed_id = feed_resp.payload["feedId"]
    print(f"フィードID: {feed_id}")

    for i in range(60):
        time.sleep(10)
        status_resp = feeds_api.get_feed(feed_id)
        status = status_resp.payload.get("processingStatus", "")
        if i % 3 == 0:
            print(f"  ステータス: {status} ({(i+1)*10}秒)")
        if status == "DONE":
            print(f"✅ {label} 完了!")
            # 結果確認
            result_doc_id = status_resp.payload.get("resultFeedDocumentId")
            if result_doc_id:
                result_text = feeds_api.get_feed_document(result_doc_id)
                lines = result_text.splitlines()
                print(f"  結果 (先頭10行):")
                for line in lines[:10]:
                    print(f"    {line}")
            return True
        if status in ("FATAL", "CANCELLED"):
            print(f"❌ {label} 失敗: {status}")
            return False

    print(f"⚠️ {label} タイムアウト（10分）")
    return False


# ── 1. 価格フィード ─────────────────────────────────────────────
ok1 = submit_and_wait("POST_PRODUCT_PRICING_DATA", price_xml, "価格")
time.sleep(5)

# ── 2. 在庫フィード ─────────────────────────────────────────────
ok2 = submit_and_wait("POST_INVENTORY_AVAILABILITY_DATA", inv_xml, "在庫")

with open("feed_result.txt", "w", encoding="utf-8") as f:
    f.write(f"件数: {len(rows)}\n価格フィード: {'OK' if ok1 else 'NG'}\n在庫フィード: {'OK' if ok2 else 'NG'}\n")

print(f"\n完了: 価格={'OK' if ok1 else 'NG'} / 在庫={'OK' if ok2 else 'NG'}")

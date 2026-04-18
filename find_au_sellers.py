"""
現在の全AU出品ASINから競合セラーIDを抽出し、SELLER_URLSに追加すべき候補を出力する。

旧版: 20件ハードコード → 新版: Reports APIで全出品ASIN取得（active優先、最大N件）

使い方:
  python find_au_sellers.py              # 全出品から最大300件チェック
  python find_au_sellers.py --max 500    # 最大500件チェック（時間がかかる）
"""
import argparse
import csv
import gzip
import io
import time
from collections import Counter

import requests as _requests
from sp_api.api import Reports, Products
from sp_api.base import Marketplaces, SellingApiException

import config
from utils.logger import get_logger

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}

# 手動で追加した補完用ASIN（売れ筋実績あり）
EXTRA_ASINS = [
    "B072863VKR", "B0D69JFQKH", "B0157NB4SG", "B0030AO94U", "B0DQ7MNJ4P",
    "B0CGZL5SQR", "B0BJGGL1SQ", "B0CVZPB5Q5", "B09W5NBTCV", "B0BSTN8L7Z",
    "B0CSFJN835", "B0D46M9DHD", "B0D1BJFGWP", "B0DMZP5W5G", "B09QFZF8XW",
    "B07QWWLJ3Y", "B07SZSDKR4", "B0C2P5FWPH", "B07GZQR62Z", "B0BNQ6M5KK",
]

AU_INTERVAL = 2.1  # get_item_offers: 0.5 req/s


def get_all_au_asins() -> list:
    """
    Reports API で全AU出品ASIN を取得する。
    active を先頭に並べて返す（active = 実需証明済みで優先度高）。
    """
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[find_au_sellers] 出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[find_au_sellers] レポートステータス: %s (%d/120)", status, attempt + 1)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")
    else:
        raise RuntimeError("レポートタイムアウト")

    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")

    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    active_asins, inactive_asins = [], []
    seen = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        item_status = row.get("status", "").strip().lower()
        if asin and len(asin) == 10 and asin not in seen and item_status != "deleted":
            seen.add(asin)
            if item_status == "active":
                active_asins.append(asin)
            else:
                inactive_asins.append(asin)

    logger.info("[find_au_sellers] 出品取得: active=%d, inactive=%d",
                len(active_asins), len(inactive_asins))
    return active_asins + inactive_asins


def find_sellers_for_asins(asins: list) -> tuple:
    """ASINリストからAU出品セラーIDをget_item_offersで収集"""
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    seller_counter = Counter()
    seller_asins = {}
    my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
    total = len(asins)

    for i, asin in enumerate(asins):
        if i % 50 == 0:
            logger.info("[find_au_sellers] セラー収集中: %d/%d", i, total)
        try:
            resp = api.get_item_offers(asin, item_condition="New")
            payload = resp.payload if hasattr(resp, "payload") else {}
            offers = payload.get("Offers", [])
            for offer in offers:
                seller_id = offer.get("SellerId", "")
                if seller_id and seller_id != my_id:
                    seller_counter[seller_id] += 1
                    seller_asins.setdefault(seller_id, [])
                    if asin not in seller_asins[seller_id]:
                        seller_asins[seller_id].append(asin)
        except SellingApiException as e:
            logger.warning("[find_au_sellers] エラー %s: %s", asin, e)
        time.sleep(AU_INTERVAL)

    return seller_counter, seller_asins


def main():
    parser = argparse.ArgumentParser(description="AU競合セラー発見ツール")
    parser.add_argument("--max", type=int, default=500,
                        help="チェックするASIN上限（デフォルト500）")
    args = parser.parse_args()

    # 1. 全出品ASINを取得（active優先）
    all_asins = get_all_au_asins()

    # EXTRA_ASINSを先頭に追加（重複除去）
    seen = set(all_asins)
    extra = [a for a in EXTRA_ASINS if a not in seen]
    asins_to_check = extra + all_asins
    asins_to_check = asins_to_check[:args.max]

    logger.info("[find_au_sellers] セラー検索開始: %d ASIN (約%.0f分)",
                len(asins_to_check), len(asins_to_check) * AU_INTERVAL / 60)

    # 2. セラー収集
    seller_counter, seller_asins = find_sellers_for_asins(asins_to_check)

    # 3. 結果表示
    print("\n" + "=" * 60)
    print("Amazon AU 競合セラー候補（被りが多い順）")
    print("=" * 60)

    # 上位30件表示（Gemini推奨: 利益重視セラーを選別するため多めに出す）
    top_sellers = seller_counter.most_common(30)
    for seller_id, count in top_sellers:
        url = f"https://www.amazon.com.au/s?me={seller_id}&marketplaceID=A39IBJ37TRP1C6"
        examples = ", ".join(seller_asins[seller_id][:3])
        print(f"\n{count}件被り | {seller_id}")
        print(f"  URL  : {url}")
        print(f"  例ASIN: {examples}")

    print("\n" + "=" * 60)
    print("SELLER_URLSに追加するURL（カンマ区切り・上位20件）")
    print("※ 安売り大手を避け、利益重視の中堅セラーを手動で選んでください")
    print("=" * 60)
    top20_urls = [
        f"https://www.amazon.com.au/s?me={sid}&marketplaceID=A39IBJ37TRP1C6"
        for sid, _ in seller_counter.most_common(20)
    ]
    print(",".join(top20_urls))
    print()


if __name__ == "__main__":
    main()

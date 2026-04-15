"""
過去の売れ筋ASINからAmazon AUで出品中のセラーIDを抽出するスクリプト。
見つかったセラーIDをSELLER_URLSに追加する候補として出力する。
"""
import time
import sys
from collections import Counter

from sp_api.api import Products
from sp_api.base import Marketplaces, SellingApiException

import config
from utils.logger import get_logger

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}

# 過去の売れ筋ASIN（複数回売れたもの優先）
TOP_ASINS = [
    "B072863VKR",  # DCA 8回
    "B0D69JFQKH",  # S.H.Figuarts ACE 6回
    "B0157NB4SG",  # Apagado Royal 6回
    "B0030AO94U",  # Mr.Hobby 6回
    "B0DQ7MNJ4P",  # デジモンカード 6回
    "B0CGZL5SQR",  # S.H.Figuarts Kaido 4回
    "B0BJGGL1SQ",  # Solidigm SSD 4回
    "B0CVZPB5Q5",  # Sunsmile Rail Cube 4回
    "B09W5NBTCV",  # Nendoroid Komi 3回
    "B0BSTN8L7Z",  # Daiwa S500JP 3回
    "B0CSFJN835",  # Intel Arc A310 3回
    "B0D46M9DHD",  # One Piece OP-09 3回
    "B0D1BJFGWP",  # One Piece PRB-01 3回
    "B0DMZP5W5G",  # One Piece OP-11 3回
    "B09QFZF8XW",  # Gundam Lancelot 3回
    "B07QWWLJ3Y",  # HX Outdoors Axes 3回
    "B07SZSDKR4",  # Batman Hush MAFEX 3回
    "B0C2P5FWPH",  # TAMIYA BBX 3回
    "B07GZQR62Z",  # Orient Star Watch 2回
    "B0BNQ6M5KK",  # Horimiya Nendoroid 2回
]


def find_sellers_for_asins(asins: list) -> Counter:
    """ASINリストからAU出品セラーIDをget_item_offersで収集"""
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    seller_counter = Counter()
    seller_asins = {}  # seller_id → [asins]

    for asin in asins:
        try:
            resp = api.get_item_offers(asin, ItemCondition="New")
            payload = resp.payload if hasattr(resp, "payload") else {}
            offers = payload.get("Offers", [])
            for offer in offers:
                seller_id = offer.get("SellerId", "")
                is_mine = offer.get("IsBuyBoxWinner", False)
                # 自分のセラーIDは除外
                my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
                if seller_id and seller_id != my_id:
                    seller_counter[seller_id] += 1
                    if seller_id not in seller_asins:
                        seller_asins[seller_id] = []
                    if asin not in seller_asins[seller_id]:
                        seller_asins[seller_id].append(asin)
        except SellingApiException as e:
            logger.warning("get_item_offers エラー %s: %s", asin, e)
        time.sleep(0.5)

    return seller_counter, seller_asins


def main():
    logger.info("AUセラー検索開始: %d ASIN", len(TOP_ASINS))
    seller_counter, seller_asins = find_sellers_for_asins(TOP_ASINS)

    print("\n=== Amazon AU 競合セラー候補 ===")
    print("（自分のASINと被りが多い順）\n")

    for seller_id, count in seller_counter.most_common(20):
        url = f"https://www.amazon.com.au/s?me={seller_id}&marketplaceID=A39IBJ37TRP1C6"
        asins = seller_asins[seller_id][:3]
        print(f"{count}件被り | {seller_id}")
        print(f"  URL: {url}")
        print(f"  例: {', '.join(asins)}")
        print()

    print("\n=== SELLER_URLSに追加するURL一覧 ===")
    urls = [
        f"https://www.amazon.com.au/s?me={sid}&marketplaceID=A39IBJ37TRP1C6"
        for sid, _ in seller_counter.most_common(10)
    ]
    print(",".join(urls))


if __name__ == "__main__":
    main()

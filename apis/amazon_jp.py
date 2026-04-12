from typing import Optional, Dict, List
import time
from sp_api.api import CatalogItems, Products
from sp_api.base import Marketplaces, SellingApiException
import config
from utils.logger import get_logger

logger = get_logger(__name__)

# SP-API の Credentials（AWSキーなし = LWAのみ）
_CREDENTIALS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

# CatalogItems v2022-04-01 のレート制限: 2 req/s（バースト5）
_REQUEST_INTERVAL = 0.6  # 秒


def get_jp_product(asin: str) -> Optional[Dict]:
    """
    Amazon JP で ASIN の価格・在庫を取得する。
    タイトルは AU スクレイパーから取得済みのため、ここでは価格・在庫のみ。

    Returns:
        {"asin": str, "title": str, "price_jpy": int|None, "in_stock": bool, "weight_kg": None}
        または None（JP に存在しない場合）
    """
    time.sleep(_REQUEST_INTERVAL)
    price_jpy, in_stock = _get_jp_price(asin)

    if price_jpy is None and not in_stock:
        # JP に存在しないか価格なし
        return None

    return {
        "asin": asin,
        "title": "",  # AU スクレイパー側のタイトルを使用
        "price_jpy": price_jpy,
        "in_stock": in_stock,
        "weight_kg": None,
    }


def _extract_weight_kg(item: dict) -> Optional[float]:
    """
    CatalogItems レスポンスから重量を kg で取得する。
    """
    try:
        dimensions_list = item.get("dimensions", [])
        for dim in dimensions_list:
            weight = dim.get("package", {}).get("weight") or dim.get("item", {}).get("weight")
            if weight:
                value = float(weight.get("value", 0))
                unit = weight.get("unit", "").lower()
                if unit in ("kilograms", "kg"):
                    return round(value, 3)
                elif unit in ("grams", "g", "gram"):
                    return round(value / 1000, 3)
                elif unit in ("pounds", "lb", "lbs"):
                    return round(value * 0.453592, 3)
                elif unit in ("ounces", "oz"):
                    return round(value * 0.0283495, 3)
    except Exception:
        pass
    return None


def _get_jp_price(asin: str):
    """
    JP の最安値（出品価格）と在庫状況を取得する。

    Returns:
        (price_jpy: int | None, in_stock: bool)
    """
    try:
        products_api = Products(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.JP,
        )
        resp = products_api.get_competitive_pricing_for_asins([asin])
        if not resp.payload:
            return None, False

        for item in resp.payload:
            product = item.get("Product", {})
            comp_pricing = product.get("CompetitivePricing", {})
            comp_prices = comp_pricing.get("CompetitivePrices", [])

            in_stock = False
            price_jpy = None

            for cp in comp_prices:
                condition = cp.get("condition", "")
                if condition == "New":
                    price = cp.get("Price", {})
                    listing_price = price.get("ListingPrice", {})
                    amount = listing_price.get("Amount")
                    if amount:
                        price_jpy = int(float(amount))
                        in_stock = True
                    break

            return price_jpy, in_stock

    except SellingApiException as e:
        logger.warning("[amazon_jp] 価格取得エラー (ASIN %s): %s", asin, e)
        return None, False


def get_jp_products_bulk(asins: List[str]) -> List[Dict]:
    """
    複数 ASIN の JP 商品情報を一括取得する（20件ずつ分割）。
    """
    results = []
    chunk_size = 20
    for i in range(0, len(asins), chunk_size):
        chunk = asins[i:i + chunk_size]
        for asin in chunk:
            product = get_jp_product(asin)
            if product:
                results.append(product)
    return results


def check_connection() -> bool:
    """SP-API (JP) への接続を確認する"""
    try:
        catalog_api = CatalogItems(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.JP,
        )
        # テスト用 ASIN（任意）
        catalog_api.get_catalog_item(asin="B00005N5PF", includedData=["summaries"])
        logger.info("[amazon_jp] 接続OK")
        return True
    except Exception as e:
        logger.error("[amazon_jp] 接続失敗: %s", e)
        return False

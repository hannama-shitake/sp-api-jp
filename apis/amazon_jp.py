from typing import Optional, Dict, List
from sp_api.api import CatalogItems, Products
from sp_api.base import Marketplaces, SellingApiException
import config
from utils.logger import get_logger

logger = get_logger(__name__)

# SP-API の Credentials オブジェクト形式
_CREDENTIALS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
    "aws_access_key": config.AMAZON_JP_CREDENTIALS["aws_access_key"],
    "aws_secret_key": config.AMAZON_JP_CREDENTIALS["aws_secret_key"],
}


def get_jp_product(asin: str) -> Optional[Dict]:
    """
    Amazon JP で ASIN の商品情報（タイトル・価格・在庫）を取得する。

    Returns:
        {
            "asin": str,
            "title": str,
            "price_jpy": int | None,
            "in_stock": bool,
        }
        または None（商品が見つからない場合）
    """
    try:
        catalog_api = CatalogItems(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.JP,
        )
        resp = catalog_api.get_catalog_item(
            asin=asin,
            includedData=["summaries", "salesRanks", "dimensions"],
        )
        item = resp.payload

        title = ""
        summaries = item.get("summaries", [])
        if summaries:
            title = summaries[0].get("itemName", "")

        # 重量取得（kg換算）
        weight_kg = _extract_weight_kg(item)

        price_jpy, in_stock = _get_jp_price(asin)

        return {
            "asin": asin,
            "title": title,
            "price_jpy": price_jpy,
            "in_stock": in_stock,
            "weight_kg": weight_kg,
        }

    except SellingApiException as e:
        if "404" in str(e) or "NOT_FOUND" in str(e):
            logger.debug("[amazon_jp] ASIN %s は JP に存在しません", asin)
        else:
            logger.error("[amazon_jp] CatalogItems エラー (ASIN %s): %s", asin, e)
        return None
    except Exception as e:
        logger.error("[amazon_jp] 予期せぬエラー (ASIN %s): %s", asin, e)
        return None


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

from typing import Optional, Dict, Tuple
from sp_api.api import ListingsItems, Products
from sp_api.base import Marketplaces, SellingApiException
import config
from utils.logger import get_logger

logger = get_logger(__name__)

_CREDENTIALS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
    "aws_access_key": config.AMAZON_AU_CREDENTIALS["aws_access_key"],
    "aws_secret_key": config.AMAZON_AU_CREDENTIALS["aws_secret_key"],
}

_SELLER_ID = None  # main.py で set_seller_id() を呼び出して設定


def set_seller_id(seller_id: str):
    global _SELLER_ID
    _SELLER_ID = seller_id


def list_item_fbm(asin: str, price_aud: float, quantity: int = 1) -> Tuple[bool, str]:
    """
    Amazon AU に FBM 相乗り出品する。

    Args:
        asin: 商品 ASIN
        price_aud: 出品価格（AUD）
        quantity: 在庫数（ドロップシッピングなので通常 1）

    Returns:
        (success: bool, message: str)
    """
    if not _SELLER_ID:
        return False, "SELLER_ID が設定されていません。config に AMAZON_AU_SELLER_ID を追加してください"

    sku = f"{config.SKU_PREFIX}{asin}"

    try:
        api = ListingsItems(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.AU,
        )

        body = {
            "productType": "PRODUCT",
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": {
                "condition_type": [
                    {"value": "new_new", "marketplace_id": config.MARKETPLACE_AU}
                ],
                "fulfillment_availability": [
                    {
                        "fulfillment_channel_code": "DEFAULT",
                        "quantity": quantity,
                        "marketplace_id": config.MARKETPLACE_AU,
                    }
                ],
                "purchasable_offer": [
                    {
                        "currency": "AUD",
                        "our_price": [
                            {
                                "schedule": [
                                    {"value_with_tax": price_aud}
                                ]
                            }
                        ],
                        "marketplace_id": config.MARKETPLACE_AU,
                    }
                ],
            },
        }

        resp = api.put_listings_item(
            sellerId=_SELLER_ID,
            sku=sku,
            marketplaceIds=[config.MARKETPLACE_AU],
            body=body,
        )

        status = resp.payload.get("status", "")
        if status in ("ACCEPTED", "VALID"):
            logger.info("[amazon_au] 出品成功: %s (SKU: %s, ¥%.2f AUD)", asin, sku, price_aud)
            return True, sku
        else:
            issues = resp.payload.get("issues", [])
            msg = "; ".join(i.get("message", "") for i in issues)
            logger.warning("[amazon_au] 出品警告: %s - %s", asin, msg)
            return False, msg

    except SellingApiException as e:
        logger.error("[amazon_au] 出品エラー (ASIN %s): %s", asin, e)
        return False, str(e)


def update_price(sku: str, price_aud: float) -> Tuple[bool, str]:
    """
    既存の出品の価格を更新する。

    Args:
        sku: 出品の SKU
        price_aud: 新しい価格（AUD）

    Returns:
        (success: bool, message: str)
    """
    if not _SELLER_ID:
        return False, "SELLER_ID が未設定"

    try:
        api = ListingsItems(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.AU,
        )

        body = {
            "productType": "PRODUCT",
            "patches": [
                {
                    "op": "replace",
                    "path": "/attributes/purchasable_offer",
                    "value": [
                        {
                            "currency": "AUD",
                            "our_price": [
                                {"schedule": [{"value_with_tax": price_aud}]}
                            ],
                            "marketplace_id": config.MARKETPLACE_AU,
                        }
                    ],
                }
            ],
        }

        resp = api.patch_listings_item(
            sellerId=_SELLER_ID,
            sku=sku,
            marketplaceIds=[config.MARKETPLACE_AU],
            body=body,
        )

        status = resp.payload.get("status", "")
        if status in ("ACCEPTED", "VALID"):
            logger.info("[amazon_au] 価格更新: SKU %s → AUD %.2f", sku, price_aud)
            return True, "updated"
        else:
            issues = resp.payload.get("issues", [])
            msg = "; ".join(i.get("message", "") for i in issues)
            return False, msg

    except SellingApiException as e:
        logger.error("[amazon_au] 価格更新エラー (SKU %s): %s", sku, e)
        return False, str(e)


def update_quantity(sku: str, quantity: int) -> Tuple[bool, str]:
    """
    在庫数を更新する（0 にすると出品停止）。
    """
    if not _SELLER_ID:
        return False, "SELLER_ID が未設定"

    try:
        api = ListingsItems(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.AU,
        )

        body = {
            "productType": "PRODUCT",
            "patches": [
                {
                    "op": "replace",
                    "path": "/attributes/fulfillment_availability",
                    "value": [
                        {
                            "fulfillment_channel_code": "DEFAULT",
                            "quantity": quantity,
                            "marketplace_id": config.MARKETPLACE_AU,
                        }
                    ],
                }
            ],
        }

        resp = api.patch_listings_item(
            sellerId=_SELLER_ID,
            sku=sku,
            marketplaceIds=[config.MARKETPLACE_AU],
            body=body,
        )

        status = resp.payload.get("status", "")
        if status in ("ACCEPTED", "VALID"):
            logger.info("[amazon_au] 在庫更新: SKU %s → %d", sku, quantity)
            return True, "updated"
        else:
            issues = resp.payload.get("issues", [])
            msg = "; ".join(i.get("message", "") for i in issues)
            return False, msg

    except SellingApiException as e:
        logger.error("[amazon_au] 在庫更新エラー (SKU %s): %s", sku, e)
        return False, str(e)


def check_connection() -> bool:
    """SP-API (AU) への接続を確認する"""
    try:
        api = Products(
            credentials=_CREDENTIALS,
            marketplace=Marketplaces.AU,
        )
        api.get_competitive_pricing_for_asins(["B00005N5PF"])
        logger.info("[amazon_au] 接続OK")
        return True
    except Exception as e:
        logger.error("[amazon_au] 接続失敗: %s", e)
        return False

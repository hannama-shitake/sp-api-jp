"""
eBay Trading API ラッパー。
AddItem / ReviseItem / EndItem / GetMyeBaySelling を提供する。
"""
import time
from typing import Optional

from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError as EbayConnectionError

import config
from utils.logger import get_logger

logger = get_logger(__name__)

# eBay カテゴリマッピング（タイトルキーワード → CategoryID）
# https://pages.ebay.com/sell/categoryoverview.html
CATEGORY_MAP = [
    # アニメ・フィギュア
    (["figuarts", "figma", "nendoroid", "mafex", "revoltech",
      "pop up parade", "figure", "フィギュア", "anime"],       14990),
    # ガンプラ・プラモデル
    (["gundam", "gunpla", "tamiya", "bandai", "model kit",
      "plastic model", "プラモ"],                              180273),
    # トレーディングカード
    (["card game", "trading card", "tcg", "one piece card",
      "pokemon card", "yugioh", "トレカ"],                     183454),
    # 鉄道模型
    (["n scale", "ho scale", "kato", "tomix", "locomotive",
      "train", "diorama"],                                    479),
    # 時計
    (["watch", "seiko", "orient", "casio", "citizen", "時計"],  31387),
    # カメラ・レンズ
    (["camera", "lens", "sony", "fujifilm", "sigma", "canon",
      "nikon", "カメラ", "レンズ"],                             625),
    # アウトドア・釣り
    (["fishing", "daiwa", "shimano", "rod", "reel", "釣り"],   23804),
    # ナイフ・刃物
    (["knife", "knives", "blade", "包丁"],                     3513),
    # ゲーム・ホビー（デフォルト）
]
DEFAULT_CATEGORY = 1249  # Collectibles > Decorative Collectibles


def _get_category(title: str) -> int:
    title_lower = title.lower()
    for keywords, cat_id in CATEGORY_MAP:
        if any(kw in title_lower for kw in keywords):
            return cat_id
    return DEFAULT_CATEGORY


def _make_connection() -> Trading:
    return Trading(
        appid=config.EBAY_APP_ID,
        devid=config.EBAY_DEV_ID,
        certid=config.EBAY_CERT_ID,
        token=config.EBAY_USER_TOKEN,
        siteid=config.EBAY_SITE_ID,
        config_file=None,
    )


def add_item(
    title: str,
    price_usd: float,
    description: str = "",
    image_url: str = "",
    category_id: int = None,
    handling_days: int = 3,
) -> Optional[str]:
    """
    eBay に FixedPriceItem (Buy It Now) を新規出品する。

    Returns:
        eBay ItemID (str) or None on failure
    """
    if not config.EBAY_USER_TOKEN:
        logger.error("[ebay] EBAY_USER_TOKEN が未設定")
        return None

    cat_id = category_id or _get_category(title)

    item = {
        "Title": title[:80],
        "PrimaryCategory": {"CategoryID": str(cat_id)},
        "StartPrice": f"{price_usd:.2f}",
        "Currency": "USD",
        "ListingType": "FixedPriceItem",
        "ListingDuration": "GTC",
        "Quantity": 1,
        "ConditionID": "1000",  # New
        "Country": "JP",
        "Location": "Japan",
        "PostalCode": "100-0001",
        "DispatchTimeMax": handling_days,
        "Description": description or f"<![CDATA[{title}<br>Ships from Japan via DHL/EMS. Usually arrives within 7-14 business days.]]>",
        "ShippingDetails": {
            "ShippingType": "Flat",
            "ShippingServiceOptions": {
                "ShippingServicePriority": 1,
                "ShippingService": "InternationalPriorityShipping",
                "ShippingServiceCost": "0.00",
                "ShippingServiceAdditionalCost": "0.00",
                "ShipsTo": "WorldWide",
            },
        },
        "ReturnPolicy": {
            "ReturnsAcceptedOption": "ReturnsAccepted",
            "RefundOption": "MoneyBack",
            "ReturnsWithinOption": "Days_30",
            "ShippingCostPaidByOption": "Buyer",
        },
    }

    if image_url:
        item["PictureDetails"] = {"PictureURL": image_url}

    try:
        api = _make_connection()
        resp = api.execute("AddItem", {"Item": item})
        item_id = resp.dict().get("ItemID", "")
        if item_id:
            logger.info("[ebay] 出品完了: %s | $%.2f | ItemID=%s", title[:40], price_usd, item_id)
            return str(item_id)
        else:
            errors = resp.dict().get("Errors", {})
            logger.warning("[ebay] 出品応答エラー: %s", errors)
            return None
    except EbayConnectionError as e:
        logger.error("[ebay] AddItem失敗: %s | %s", title[:40], e)
        return None


def end_item(item_id: str, reason: str = "NotAvailable") -> bool:
    """eBay 出品を終了する（JP在庫切れ時）"""
    if not config.EBAY_USER_TOKEN:
        return False
    try:
        api = _make_connection()
        api.execute("EndItem", {
            "ItemID": item_id,
            "EndingReason": reason,
        })
        logger.info("[ebay] 出品終了: ItemID=%s", item_id)
        return True
    except EbayConnectionError as e:
        logger.error("[ebay] EndItem失敗: ItemID=%s | %s", item_id, e)
        return False


def revise_price(item_id: str, new_price_usd: float) -> bool:
    """eBay 出品価格を更新する"""
    if not config.EBAY_USER_TOKEN:
        return False
    try:
        api = _make_connection()
        api.execute("ReviseItem", {
            "Item": {
                "ItemID": item_id,
                "StartPrice": f"{new_price_usd:.2f}",
            }
        })
        logger.info("[ebay] 価格更新: ItemID=%s → $%.2f", item_id, new_price_usd)
        return True
    except EbayConnectionError as e:
        logger.error("[ebay] ReviseItem失敗: ItemID=%s | %s", item_id, e)
        return False


def get_active_listings() -> dict:
    """
    eBay アクティブ出品を取得する。
    Returns: {item_id: {"title": str, "price_usd": float, "custom_label": str}}
    custom_label に ASIN を保存しているため ASIN と紐付け可能。
    """
    if not config.EBAY_USER_TOKEN:
        return {}
    try:
        api = _make_connection()
        result = {}
        page = 1
        while True:
            resp = api.execute("GetMyeBaySelling", {
                "ActiveList": {
                    "Include": True,
                    "Pagination": {
                        "EntriesPerPage": 200,
                        "PageNumber": page,
                    },
                }
            })
            data = resp.dict()
            active = data.get("ActiveList", {})
            items = active.get("ItemArray", {}).get("Item", [])
            if isinstance(items, dict):
                items = [items]
            for item in items:
                item_id = str(item.get("ItemID", ""))
                result[item_id] = {
                    "title": item.get("Title", ""),
                    "price_usd": float(item.get("SellingStatus", {})
                                       .get("CurrentPrice", {})
                                       .get("value", 0) or 0),
                    "custom_label": item.get("SKU", ""),
                }
            total_pages = int(
                active.get("PaginationResult", {}).get("TotalNumberOfPages", 1)
            )
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)
        logger.info("[ebay] アクティブ出品取得: %d件", len(result))
        return result
    except EbayConnectionError as e:
        logger.error("[ebay] GetMyeBaySelling失敗: %s", e)
        return {}

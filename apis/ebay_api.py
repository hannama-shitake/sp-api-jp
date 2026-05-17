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

# ─────────────────────────────────────────────
# 商品説明テンプレート
# ─────────────────────────────────────────────

def build_description(title: str) -> str:
    """
    eBay 商品説明 HTML を生成する。
    - 日本発送・配送日数
    - 関税・輸入税は buyer 負担である旨の注記（重要）
    """
    return (
        "<![CDATA["
        "<p><strong>{title}</strong></p>"
        "<p>Ships from Japan via DHL/EMS. "
        "Usually arrives within <strong>7–14 business days</strong>.</p>"
        "<hr/>"
        "<p><strong>⚠️ Important – Customs &amp; Import Duties:</strong><br/>"
        "This item ships internationally from Japan. "
        "Import duties, taxes, and customs fees <strong>may be charged upon delivery</strong> "
        "by your country's customs authority. "
        "These charges are the <strong>buyer's sole responsibility</strong> and are "
        "<strong>not included</strong> in the item price or shipping cost. "
        "Please check your local customs regulations before purchasing.</p>"
        "<hr/>"
        "<p>We ship carefully packaged to ensure safe delivery. "
        "If you have any questions, please feel free to message us.</p>"
        "]]>"
    ).format(title=title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


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
    # ギターエフェクター・パワーサプライ（Amazon JP直送モデル向け）
    # eBay: Musical Instruments > Guitar > Effects Pedals (48446)
    (["guitar", "bass guitar", "electric guitar", "effects pedal",
      "power supply", "fender", "marshall", "vox", "boss pedal",
      "mxr", "tc electronic", "strymon", "zoom", "engine room",
      "audio interface", "focusrite", "scarlett", "mooer",
      "ibanez", "gretsch", "epiphone", "prs guitar"],          48446),
    # 電子楽器・MIDI
    (["midi", "synthesizer", "synth", "keyboard controller",
      "akai", "arturia", "novation", "elektron",
      "digital piano", "electric piano"],                      47056),
    # レコーディング機器
    (["microphone", "condenser", "audio recorder", "tascam",
      "zoom recorder", "field recorder", "preamp", "DI box",
      "phantom power", "xlr"],                                 47069),
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


def _get_item_type(cat_id: int) -> str:
    """カテゴリIDからeBay item specific の Type を返す"""
    type_map = {
        14990:  "Action Figure",
        180273: "Model Kit",
        183454: "Trading Card",
        625:    "Camera",
        23804:  "Fishing Equipment",
        3513:   "Kitchen Knife",
        479:    "Model Train",
        48446:  "Effects Pedal",
        47056:  "Electronic Keyboard",
        47069:  "Recording Equipment",
        1249:   "Collectible",
    }
    return type_map.get(cat_id, "See Description")


def _build_item_specifics(cat_id: int, brand: str = "") -> dict:
    """
    eBay が必須とする item specifics を返す。
    Error 21919303 (missing item specifics) を回避するため
    Brand / Type / Model / Connectivity をすべて設定する。
    """
    specifics = [
        {"Name": "Brand",        "Value": brand or "Unbranded"},
        {"Name": "Type",         "Value": _get_item_type(cat_id)},
        {"Name": "Model",        "Value": "See Description"},
        # 一部カテゴリで必須。電子機器以外は "Not Applicable" で通過する
        {"Name": "Connectivity", "Value": "Not Applicable"},
        {"Name": "Country/Region of Manufacture", "Value": "Japan"},
    ]
    return {"NameValueList": specifics}


def add_item(
    title: str,
    price_usd: float,
    description: str = "",
    image_url: str = "",        # 後方互換: 単一URL（image_urls 未指定時に使用）
    image_urls: list = None,    # 複数画像URL（最大12枚、MAIN→PT01...の順）
    category_id: int = None,
    handling_days: int = 3,
    custom_label: str = "",
    brand: str = "",
) -> Optional[str]:
    """
    eBay に FixedPriceItem (Buy It Now) を新規出品する。

    image_urls: 複数画像リスト（最大12枚）。指定時は image_url を無視。
    image_url:  後方互換用。image_urls が空/None の場合のフォールバック。
    複数画像があると「実物感・信頼感」が増し購買率が上がる。
    brand は ItemSpecifics に設定される（空なら "Unbranded"）。

    Returns:
        eBay ItemID (str) or None on failure
    """
    if not config.EBAY_USER_TOKEN:
        logger.error("[ebay] EBAY_USER_TOKEN が未設定")
        return None

    cat_id = category_id or _get_category(title)

    # 画像リストを確定（image_urls 優先、なければ image_url、さらになければ ASIN から生成）
    if image_urls:
        final_image_urls = [u for u in image_urls if u][:12]
    elif image_url:
        final_image_urls = [image_url]
    elif custom_label and len(custom_label) == 10:
        final_image_urls = [
            f"https://m.media-amazon.com/images/P/{custom_label}.01._SCLZZZZZZZ_.jpg"
        ]
    else:
        final_image_urls = []

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
        "Description": description or build_description(title),
        # ─── Business Policies（SellerProfiles） ───────────────────────────
        # このアカウントは Business Policies を有効化済み。
        # legacy ShippingDetails/ReturnPolicy は eBay に無視されるため
        # SellerProfiles で明示指定することで送料 $25/$30 が確実に適用される。
        # Policy ID は config.EBAY_SHIPPING_POLICY_ID 等で管理。
        "SellerProfiles": {
            "SellerShippingProfile": {
                "ShippingProfileID": config.EBAY_SHIPPING_POLICY_ID,
            },
            "SellerReturnProfile": {
                "ReturnProfileID": config.EBAY_RETURN_POLICY_ID,
            },
            "SellerPaymentProfile": {
                "PaymentProfileID": config.EBAY_PAYMENT_POLICY_ID,
            },
        },
        # ItemSpecifics: Error 21919303 (Brand/Type/Model/Connectivity) 回避
        "ItemSpecifics": _build_item_specifics(cat_id, brand),
    }

    if final_image_urls:
        # eBay PictureDetails: リストで渡すと複数画像として掲載される（最大12枚）
        item["PictureDetails"] = {
            "PictureURL": final_image_urls if len(final_image_urls) > 1 else final_image_urls[0]
        }

    if custom_label:
        item["SKU"] = custom_label[:50]

    try:
        api = _make_connection()
        resp = api.execute("AddItem", {"Item": item})
        item_id = resp.dict().get("ItemID", "")
        if item_id:
            logger.info("[ebay] 出品完了: %s | $%.2f | %d枚 | ItemID=%s",
                        title[:40], price_usd, len(final_image_urls), item_id)
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


def revise_images(item_id: str, image_urls: list) -> bool:
    """既存出品の画像を複数画像に更新する（最大12枚）。接続エラー時は最大2回リトライ。"""
    if not config.EBAY_USER_TOKEN or not image_urls:
        return False
    urls = [u for u in image_urls if u][:12]
    for attempt in range(3):
        try:
            api = _make_connection()
            api.execute("ReviseItem", {
                "Item": {
                    "ItemID": item_id,
                    "PictureDetails": {
                        "PictureURL": urls if len(urls) > 1 else urls[0]
                    },
                }
            })
            logger.info("[ebay] 画像更新: ItemID=%s → %d枚", item_id, len(urls))
            return True
        except EbayConnectionError as e:
            err = str(e)
            if "ConnectionError" in err or "ConnectionReset" in err or "aborted" in err.lower():
                wait = (attempt + 1) * 10
                logger.warning("[ebay] 画像更新 接続エラー → %ds後リトライ(%d/3): %s", wait, attempt+1, item_id)
                time.sleep(wait)
                continue
            logger.error("[ebay] ReviseItem(画像)失敗: ItemID=%s | %s", item_id, e)
            return False
    logger.error("[ebay] ReviseItem(画像)3回失敗: ItemID=%s", item_id)
    return False


def revise_shipping_and_price(item_id: str, new_price_usd: float,
                              title: str = "") -> bool:
    """
    eBay 出品の送料・価格・商品説明を同時更新する。
    送料: US $EBAY_SHIPPING_US_USD / 国際 $EBAY_SHIPPING_INTL_USD（config 参照）
    説明: 関税注記を含む標準テンプレートで上書き（title 指定時）
    """
    if not config.EBAY_USER_TOKEN:
        return False
    try:
        api = _make_connection()
        item = {
            "ItemID": item_id,
            "StartPrice": f"{new_price_usd:.2f}",
            "ShippingDetails": {
                "ShippingType": "Flat",
                "ShippingServiceOptions": {
                    "ShippingServicePriority": 1,
                    "ShippingService": "ExpeditedShippingFromOutsideUS",
                    "ShippingServiceCost": f"{config.EBAY_SHIPPING_US_USD:.2f}",
                    "ShippingServiceAdditionalCost": "0.00",
                },
                "InternationalShippingServiceOption": {
                    "ShippingServicePriority": 1,
                    "ShippingService": "ExpeditedInternational",
                    "ShippingServiceCost": f"{config.EBAY_SHIPPING_INTL_USD:.2f}",
                    "ShippingServiceAdditionalCost": "0.00",
                    "ShipToLocation": "WorldWide",
                },
            },
        }
        if title:
            item["Description"] = build_description(title)

        api.execute("ReviseItem", {"Item": item})
        logger.info("[ebay] 送料+価格+説明更新: ItemID=%s → $%.2f | US$%.0f/Intl$%.0f",
                    item_id, new_price_usd,
                    config.EBAY_SHIPPING_US_USD, config.EBAY_SHIPPING_INTL_USD)
        return True
    except EbayConnectionError as e:
        logger.error("[ebay] ReviseItem(送料+価格+説明)失敗: ItemID=%s | %s", item_id, e)
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
                },
                "OutputSelector": [
                    "ActiveList.ItemArray.Item.ItemID",
                    "ActiveList.ItemArray.Item.Title",
                    "ActiveList.ItemArray.Item.SellingStatus.CurrentPrice",
                    "ActiveList.ItemArray.Item.SKU",
                    "ActiveList.PaginationResult",
                ],
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

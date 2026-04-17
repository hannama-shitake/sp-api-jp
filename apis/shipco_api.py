"""
Ship&co API ラッパー。
DHL出荷ラベル作成・送料照会を提供する。

認証: x-access-token ヘッダー（Ship&co Settings → API からトークン取得）
API仕様: https://developer.shipandco.com/en/
"""
from datetime import date
from typing import Optional

import requests

import config
from utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.shipandco.com/v1"
_CARRIER_CACHE: Optional[str] = None  # DHL carrier_id をキャッシュ


def _headers() -> dict:
    return {
        "x-access-token": config.SHIPCO_API_TOKEN,
        "Content-Type": "application/json",
    }


def _from_address() -> dict:
    return {
        "full_name": config.SHIPCO_FROM_NAME,
        "company":   config.SHIPCO_FROM_COMPANY,
        "email":     config.SHIPCO_FROM_EMAIL,
        "phone":     config.SHIPCO_FROM_PHONE,
        "country":   "JP",
        "zip":       config.SHIPCO_FROM_ZIP,
        "province":  config.SHIPCO_FROM_PROVINCE,
        "city":      config.SHIPCO_FROM_CITY,
        "address1":  config.SHIPCO_FROM_ADDRESS1,
    }


# ─────────────────────────────────────────────
# キャリア取得
# ─────────────────────────────────────────────

def get_carriers() -> list:
    """登録済みキャリア一覧を取得（carrier_id 確認用）"""
    r = requests.get(f"{BASE_URL}/carriers", headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def _get_dhl_carrier_id() -> Optional[str]:
    """Ship&co に登録されている DHL の carrier_id を自動検索する"""
    global _CARRIER_CACHE
    if _CARRIER_CACHE:
        return _CARRIER_CACHE
    try:
        carriers = get_carriers()
        logger.info("[shipco] 登録キャリア一覧: %s", carriers)
        # carrier / name / service / type など複数フィールドを探索
        for c in carriers:
            # フィールド値を全部結合して dhl を含むか確認
            all_values = " ".join(str(v) for v in c.values() if v).lower()
            if "dhl" in all_values:
                _CARRIER_CACHE = c.get("id") or c.get("_id") or c.get("carrier_id")
                logger.info("[shipco] DHL carrier_id: %s (raw: %s)", _CARRIER_CACHE, c)
                return _CARRIER_CACHE
        logger.warning("[shipco] DHL キャリアが見つかりません。全キャリア: %s", carriers)
    except Exception as e:
        logger.warning("[shipco] キャリア取得失敗: %s", e)
    return None


# ─────────────────────────────────────────────
# 送料照会
# ─────────────────────────────────────────────

def get_rates(
    to_address: dict,
    products: list,
    weight_g: int = 980,
) -> list:
    """
    全キャリアの送料見積もりを取得する。
    service を省略すると全キャリアのレートが返る。

    Returns: [{"carrier": str, "service": str, "price": int, ...}, ...]
    """
    carrier_id = _get_dhl_carrier_id()
    payload = {
        "setup": {
            "shipment_date": date.today().isoformat(),
            **({"carrier_id": carrier_id} if carrier_id else {}),
        },
        "from_address": _from_address(),
        "to_address": to_address,
        "products": products,
        "parcels": [{"weight": weight_g, "width": 30, "height": 20, "depth": 10}],
        "customs": {"content_type": "MERCHANDISE", "duty_paid": False},
    }
    try:
        r = requests.post(f"{BASE_URL}/rates", json=payload, headers=_headers(), timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        logger.error("[shipco] 送料照会失敗: %s | %s",
                     e, e.response.text if e.response else "")
        return []


# ─────────────────────────────────────────────
# 出荷作成
# ─────────────────────────────────────────────

def create_shipment(
    order_id: str,
    to_address: dict,
    products: list,
    weight_g: int = 980,
    service: str = "dhl_express_worldwide",
    test: bool = False,
) -> Optional[dict]:
    """
    Ship&co で DHL 出荷ラベルを作成する。

    Args:
        order_id:    Amazon 注文ID（ref_number に使用）
        to_address:  配送先住所（Ship&co 形式）
        products:    商品リスト（customs 申告用）
        weight_g:    重量(g)、デフォルト980g
        service:     DHL サービス種別
        test:        True にするとテストラベル（課金なし）

    Returns:
        {
            "shipment_id":    str,
            "tracking_number": str,
            "label_url":      str,   # DHL ラベル PDF URL
            "fee_jpy":        int,   # 実際の送料(円)
        }
        or None on failure
    """
    if not config.SHIPCO_API_TOKEN:
        logger.error("[shipco] SHIPCO_API_TOKEN が未設定")
        return None

    carrier_id = _get_dhl_carrier_id()
    if not carrier_id:
        logger.error("[shipco] DHL carrier_id が取得できませんでした")
        return None

    payload = {
        "setup": {
            "carrier_id":    carrier_id,
            "service":       service,
            "ref_number":    order_id,
            "shipment_date": date.today().isoformat(),
            "test":          test,
        },
        "from_address": _from_address(),
        "to_address":   to_address,
        "products":     products,
        "parcels":      [{"weight": weight_g, "width": 30, "height": 20, "depth": 10}],
        "customs": {
            "content_type": "MERCHANDISE",
            "duty_paid":    False,
        },
    }

    try:
        r = requests.post(
            f"{BASE_URL}/shipments", json=payload, headers=_headers(), timeout=60
        )
        r.raise_for_status()
        resp = r.json()

        delivery = resp.get("delivery", {})
        tracking_numbers = delivery.get("tracking_numbers", [])

        if not tracking_numbers:
            logger.warning("[shipco] 追跡番号なし order=%s resp=%s", order_id, resp)
            return None

        result = {
            "shipment_id":     resp.get("id", ""),
            "tracking_number": tracking_numbers[0],
            "label_url":       delivery.get("label", ""),
            "fee_jpy":         int(resp.get("setup", {}).get("shipping_fee", 0)),
        }
        logger.info("[shipco] 出荷作成完了: order=%s 追跡=%s ラベル=%s 送料¥%d",
                    order_id, result["tracking_number"],
                    result["label_url"], result["fee_jpy"])
        return result

    except requests.HTTPError as e:
        body = e.response.text if e.response else ""
        logger.error("[shipco] 出荷作成失敗 order=%s: %s | %s", order_id, e, body)
        return None

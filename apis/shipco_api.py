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


def get_best_carrier_id(prefer: str = "dhl") -> Optional[str]:
    """
    登録済みキャリアから優先順で carrier_id を返す。
    prefer="dhl" → dhl > ups > その他の順で選択。
    """
    global _CARRIER_CACHE
    if _CARRIER_CACHE:
        return _CARRIER_CACHE
    try:
        carriers = get_carriers()
        active = [c for c in carriers if c.get("state") == "active"]
        logger.info("[shipco] 登録キャリア: %s",
                    [(c.get("id"), c.get("type")) for c in active])

        # 優先順: dhl → ups → 先頭のアクティブキャリア
        priority = [prefer, "ups", "dhl"]
        for name in priority:
            for c in active:
                if name in (c.get("type") or "").lower():
                    _CARRIER_CACHE = c["id"]
                    logger.info("[shipco] 使用キャリア: %s (id=%s)",
                                c.get("type"), _CARRIER_CACHE)
                    return _CARRIER_CACHE

        # どれも一致しなければ先頭を使う
        if active:
            _CARRIER_CACHE = active[0]["id"]
            logger.warning("[shipco] 優先キャリアなし → %s (id=%s) を使用",
                           active[0].get("type"), _CARRIER_CACHE)
            return _CARRIER_CACHE

    except Exception as e:
        logger.warning("[shipco] キャリア取得失敗: %s", e)
    return None


# 後方互換用エイリアス
_get_dhl_carrier_id = get_best_carrier_id


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
    carrier_id = get_best_carrier_id()
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
    service: str = "",  # 空文字 = Ship&co が自動選択（キャリアのデフォルトサービス）
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

    carrier_id = get_best_carrier_id()
    if not carrier_id:
        logger.error("[shipco] DHL carrier_id が取得できませんでした")
        return None

    setup: dict = {
        "carrier_id":    carrier_id,
        "ref_number":    order_id,
        "shipment_date": date.today().isoformat(),
        "test":          test,
    }
    if service:
        setup["service"] = service

    payload = {
        "setup":        setup,
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

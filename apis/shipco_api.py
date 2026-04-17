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


def get_dhl_carrier_id() -> Optional[str]:
    """
    /v1/carriers から DHL carrier_id を検索する（ユーザー登録キャリア用）。
    Ship&co 共通DHL（carrier_id 不要）の場合は None を返す。
    """
    global _CARRIER_CACHE
    if _CARRIER_CACHE:
        return _CARRIER_CACHE
    try:
        carriers = get_carriers()
        active = [c for c in carriers if c.get("state") == "active"]
        logger.info("[shipco] 登録キャリア: %s",
                    [(c.get("id"), c.get("type")) for c in active])
        for c in active:
            if "dhl" in (c.get("type") or "").lower():
                _CARRIER_CACHE = c["id"]
                logger.info("[shipco] DHL carrier_id（登録済み）: %s", _CARRIER_CACHE)
                return _CARRIER_CACHE
    except Exception as e:
        logger.warning("[shipco] キャリア取得失敗: %s", e)
    # Ship&co 共通DHLは carrier_id 不要
    return None


def select_carrier_from_rates(
    to_address: dict,
    products: list,
    weight_g: int,
    max_dhl_jpy: int = None,
) -> Optional[dict]:
    """
    POST /v1/rates で全キャリアのレートを取得し、最適キャリアを選択する。

    選択ロジック:
      1. DHLレート ≤ max_dhl_jpy → DHL を使用
      2. DHLレート > max_dhl_jpy  → eパケットライト（Japan Post）に切り替え
      3. どちらも取れない          → 最安値キャリアを使用

    Returns: {"carrier_id": str, "service": str, "price": int, "carrier_name": str}
    """
    if max_dhl_jpy is None:
        max_dhl_jpy = config.SHIPCO_MAX_DHL_JPY

    payload = {
        "setup":        {"shipment_date": date.today().isoformat()},
        "from_address": _from_address(),
        "to_address":   to_address,
        "products":     products,
        "parcels":      [{"weight": weight_g, "width": 30, "height": 20, "depth": 10}],
        "customs":      {"content_type": "MERCHANDISE", "duty_paid": False},
    }
    try:
        r = requests.post(f"{BASE_URL}/rates", json=payload, headers=_headers(), timeout=30)
        r.raise_for_status()
        rates = r.json()

        # エラーなしのレートのみ対象
        valid = [rt for rt in rates if not rt.get("errors") and rt.get("price")]
        logger.info("[shipco] 有効レート: %s",
                    [(rt.get("carrier"), rt.get("service"), rt.get("price")) for rt in valid])

        def _pick(keyword: str) -> Optional[dict]:
            matched = [rt for rt in valid
                       if keyword in (rt.get("carrier") or "").lower()
                       or keyword in (rt.get("service") or "").lower()]
            return min(matched, key=lambda x: x["price"]) if matched else None

        dhl = _pick("dhl")

        # DHLが¥5,000以下 → DHL確定
        if dhl and dhl["price"] <= max_dhl_jpy:
            logger.info("[shipco] DHL選択: ¥%d (上限¥%d)", dhl["price"], max_dhl_jpy)
            return {
                "carrier_id":   dhl.get("carrier_id", ""),
                "service":      dhl.get("service", ""),
                "price":        dhl["price"],
                "carrier_name": "DHL",
            }

        # DHL高すぎ → eパケットライトに切替
        epacket = _pick("epacket") or _pick("japanpost") or _pick("japan_post")
        if epacket:
            reason = f"DHL ¥{dhl['price']:,} > 上限¥{max_dhl_jpy:,}" if dhl else "DHLなし"
            logger.info("[shipco] eパケットライト選択（%s）: ¥%d", reason, epacket["price"])
            return {
                "carrier_id":   epacket.get("carrier_id", ""),
                "service":      epacket.get("service", ""),
                "price":        epacket["price"],
                "carrier_name": "eパケット",
            }

        # フォールバック: 最安値
        if valid:
            cheapest = min(valid, key=lambda x: x["price"])
            logger.warning("[shipco] DHL/eパケット不可 → 最安値: %s ¥%d",
                           cheapest.get("carrier"), cheapest["price"])
            return {
                "carrier_id":   cheapest.get("carrier_id", ""),
                "service":      cheapest.get("service", ""),
                "price":        cheapest["price"],
                "carrier_name": cheapest.get("carrier", "unknown"),
            }

        logger.error("[shipco] 利用可能なキャリアなし")
    except Exception as e:
        logger.warning("[shipco] レート取得失敗: %s", e)
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
    carrier_id = get_dhl_carrier_id()
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

    # rates から最適キャリア（DHL or eパケット）を自動選択
    rate = select_carrier_from_rates(to_address, products, weight_g)
    if not rate:
        logger.error("[shipco] 利用可能なキャリアなし: order=%s", order_id)
        return None

    setup: dict = {
        "ref_number":    order_id,
        "shipment_date": date.today().isoformat(),
        "test":          test,
    }
    if rate.get("carrier_id"):
        setup["carrier_id"] = rate["carrier_id"]
    if rate.get("service") or service:
        setup["service"] = rate.get("service") or service

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
            "carrier_name":    rate.get("carrier_name", ""),
        }
        logger.info("[shipco] 出荷作成完了: order=%s 追跡=%s ラベル=%s 送料¥%d",
                    order_id, result["tracking_number"],
                    result["label_url"], result["fee_jpy"])
        return result

    except requests.HTTPError as e:
        body = e.response.text if e.response else ""
        logger.error("[shipco] 出荷作成失敗 order=%s: %s | %s", order_id, e, body)
        return None

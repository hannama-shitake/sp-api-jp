import requests
from utils.logger import get_logger

logger = get_logger(__name__)

# フリーの為替APIを使用 (Open Exchange Rates の無料版)
# バックアップとして複数のエンドポイントを試みる
_RATE_APIS = [
    "https://api.exchangerate-api.com/v4/latest/JPY",
    "https://open.er-api.com/v6/latest/JPY",
]

_cached_rate: float = 0.0
_cached_at: float = 0.0
_CACHE_TTL_SEC = 1800  # 30分キャッシュ


def get_jpy_to_aud() -> float:
    """
    JPY → AUD の為替レートを取得する。
    30分間キャッシュして API コールを節約する。

    Returns:
        1 JPY に対する AUD の値（例: 0.0095）
    """
    import time
    global _cached_rate, _cached_at

    now = time.time()
    if _cached_rate and (now - _cached_at) < _CACHE_TTL_SEC:
        return _cached_rate

    for api_url in _RATE_APIS:
        try:
            resp = requests.get(api_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            # api.exchangerate-api.com と open.er-api.com は同じ形式
            rate = data.get("rates", {}).get("AUD")
            if rate:
                _cached_rate = float(rate)
                _cached_at = now
                logger.info("[exchange_rate] JPY→AUD: %.6f (from %s)", _cached_rate, api_url)
                return _cached_rate
        except Exception as e:
            logger.warning("[exchange_rate] %s 失敗: %s", api_url, e)

    # フォールバック: キャッシュがあればそれを返す
    if _cached_rate:
        logger.warning("[exchange_rate] API取得失敗。キャッシュ値を使用: %.6f", _cached_rate)
        return _cached_rate

    # 最終フォールバック: 固定レート
    fallback = 0.0095
    logger.error("[exchange_rate] 為替レート取得失敗。固定値を使用: %.4f", fallback)
    return fallback


def jpy_to_aud(amount_jpy: float) -> float:
    """JPY 金額を AUD に変換する"""
    return round(amount_jpy * get_jpy_to_aud(), 2)


def aud_to_jpy(amount_aud: float) -> float:
    """AUD 金額を JPY に変換する"""
    rate = get_jpy_to_aud()
    if rate == 0:
        return 0.0
    return round(amount_aud / rate, 0)

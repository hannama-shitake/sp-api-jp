from dataclasses import dataclass
from typing import Optional
import config
from apis.exchange_rate import get_jpy_to_aud
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ProfitResult:
    asin: str
    title: str
    jp_price_jpy: int
    au_price_aud: float
    exchange_rate: float
    intl_shipping_jpy: int
    au_fee_aud: float
    au_fee_jpy: float
    revenue_jpy: float
    profit_jpy: float
    profit_rate: float
    is_profitable: bool
    # 推奨 AU 出品価格（粗利率を達成するための最低価格）
    recommended_au_price_aud: Optional[float] = None


def get_shipping_jpy(weight_kg: Optional[float] = None) -> int:
    """
    重量に基づいて国際送料(円)を返す。

    重量不明              → DHL基本料（¥3,800）
    0 〜 1kg未満          → DHL基本料（¥3,800）
    1kg 〜 DHL上限(2kg)   → DHL + 重量サーチャージ（+¥5,000）
    2kg超                 → EMS/eパケット + 重量サーチャージ
    """
    if weight_kg is None:
        return config.DHL_SHIPPING_JPY
    if weight_kg < config.HEAVY_ITEM_THRESHOLD_KG:
        return config.DHL_SHIPPING_JPY
    # 1kg以上 → 追加送料
    if weight_kg <= config.DHL_MAX_WEIGHT_KG:
        return config.DHL_SHIPPING_JPY + config.HEAVY_SHIPPING_SURCHARGE_JPY
    return config.EMS_SHIPPING_JPY + config.HEAVY_SHIPPING_SURCHARGE_JPY


def calc_profit(
    asin: str,
    title: str,
    jp_price_jpy: int,
    au_price_aud: float,
    exchange_rate: Optional[float] = None,
    weight_kg: Optional[float] = None,
) -> ProfitResult:
    """
    粗利計算を行う。

    粗利 = AU販売収益(JPY) - JP仕入値(JPY) - 国際送料(JPY)
    粗利率 = 粗利 / JP仕入値 × 100

    Args:
        asin: 商品 ASIN
        title: 商品タイトル
        jp_price_jpy: JP 仕入値（円）
        au_price_aud: AU 販売価格（AUD）
        exchange_rate: JPY→AUD レート（None なら自動取得）

    Returns:
        ProfitResult
    """
    if exchange_rate is None:
        exchange_rate = get_jpy_to_aud()

    intl_shipping_jpy = get_shipping_jpy(weight_kg)
    au_fee_aud = round(au_price_aud * config.AU_FEE_RATE, 2)
    net_revenue_aud = au_price_aud - au_fee_aud

    # AUD → JPY に変換
    if exchange_rate > 0:
        au_fee_jpy = round(au_fee_aud / exchange_rate)
        revenue_jpy = round(net_revenue_aud / exchange_rate)
    else:
        au_fee_jpy = 0.0
        revenue_jpy = 0.0

    profit_jpy = revenue_jpy - jp_price_jpy - intl_shipping_jpy

    if jp_price_jpy > 0:
        profit_rate = round(profit_jpy / jp_price_jpy * 100, 1)
    else:
        profit_rate = 0.0

    is_profitable = profit_rate >= config.MIN_PROFIT_RATE

    # 目標粗利率を達成するための最低 AU 価格を計算
    # profit_rate = (revenue_jpy - jp_price_jpy - shipping) / jp_price_jpy * 100
    # → revenue_jpy = jp_price_jpy * (1 + rate/100) + shipping
    # → net_revenue_aud = revenue_jpy * exchange_rate
    # → au_price = net_revenue_aud / (1 - AU_FEE_RATE)
    required_revenue_jpy = jp_price_jpy * (1 + config.MIN_PROFIT_RATE / 100) + intl_shipping_jpy
    if exchange_rate > 0:
        required_net_aud = required_revenue_jpy * exchange_rate
        recommended_price = round(required_net_aud / (1 - config.AU_FEE_RATE), 2)
    else:
        recommended_price = None

    return ProfitResult(
        asin=asin,
        title=title,
        jp_price_jpy=jp_price_jpy,
        au_price_aud=au_price_aud,
        exchange_rate=exchange_rate,
        intl_shipping_jpy=intl_shipping_jpy,
        au_fee_aud=au_fee_aud,
        au_fee_jpy=au_fee_jpy,
        revenue_jpy=revenue_jpy,
        profit_jpy=profit_jpy,
        profit_rate=profit_rate,
        is_profitable=is_profitable,
        recommended_au_price_aud=recommended_price,
    )


def calc_optimal_au_price(
    jp_price_jpy: int,
    target_profit_rate: float = None,
    exchange_rate: float = None,
    weight_kg: Optional[float] = None,
) -> float:
    """
    JP仕入値から、目標粗利率を達成するための最適 AU 出品価格を計算する。

    weight_kg を渡すと重量別送料（1kg以上は+¥5,000）を考慮した価格を返す。
    exchange_rate を渡すと get_jpy_to_aud() の呼び出しを省略できる（ループ内で使う場合に推奨）。
    """
    if target_profit_rate is None:
        target_profit_rate = config.MIN_PROFIT_RATE

    rate = exchange_rate if exchange_rate else get_jpy_to_aud()
    intl_shipping_jpy = get_shipping_jpy(weight_kg)   # 重量考慮した送料
    required_revenue_jpy = jp_price_jpy * (1 + target_profit_rate / 100) + intl_shipping_jpy
    required_net_aud = required_revenue_jpy * rate
    au_price = required_net_aud / (1 - config.AU_FEE_RATE)
    return round(au_price * config.PRICE_MARKUP_MULTIPLIER, 2)

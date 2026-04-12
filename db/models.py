from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Product:
    asin: str
    title: str = ""
    au_price_aud: float = 0.0
    jp_price_jpy: int = 0
    profit_jpy: float = 0.0
    profit_rate: float = 0.0
    jp_in_stock: bool = True
    exchange_rate: float = 0.0
    last_checked: str = ""


@dataclass
class Listing:
    asin: str
    sku: str
    platform: str = "amazon_au"
    status: str = "active"
    listed_at: str = ""
    updated_at: str = ""
    id: Optional[int] = None


@dataclass
class PriceHistory:
    asin: str
    platform: str
    price_aud: Optional[float]
    price_jpy: Optional[int]
    exchange_rate: float
    recorded_at: str

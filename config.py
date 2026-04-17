import os
from dotenv import load_dotenv

load_dotenv()

# ── Amazon マーケットプレイス ID ──────────────────────────────
MARKETPLACE_JP = "A1VC38T7YXB528"
MARKETPLACE_AU = "A39IBJ37TRP1C6"

# ── Amazon JP SP-API 認証 ─────────────────────────────────────
AMAZON_JP_CREDENTIALS = {
    "refresh_token": (os.getenv("AMAZON_JP_REFRESH_TOKEN") or "").strip(),
    "lwa_app_id": (os.getenv("AMAZON_JP_LWA_CLIENT_ID") or "").strip(),
    "lwa_client_secret": (os.getenv("AMAZON_JP_LWA_CLIENT_SECRET") or "").strip(),
}

# ── Amazon AU SP-API 認証 ─────────────────────────────────────
AMAZON_AU_CREDENTIALS = {
    "refresh_token": (os.getenv("AMAZON_AU_REFRESH_TOKEN") or "").strip(),
    "lwa_app_id": (os.getenv("AMAZON_AU_LWA_CLIENT_ID") or "").strip(),
    "lwa_client_secret": (os.getenv("AMAZON_AU_LWA_CLIENT_SECRET") or "").strip(),
    "seller_id": (os.getenv("AMAZON_AU_SELLER_ID") or "").strip(),
}

# ── 利益計算パラメータ ────────────────────────────────────────
MIN_PROFIT_RATE = float(os.getenv("MIN_PROFIT_RATE", "30"))  # 最低粗利率(%)
INTL_SHIPPING_JPY = int(os.getenv("INTL_SHIPPING_JPY", "4500"))  # 国際送料デフォルト(円) DHL想定
DHL_SHIPPING_JPY = int(os.getenv("DHL_SHIPPING_JPY", "4500"))   # DHL送料(2kg以下)
EMS_SHIPPING_JPY = int(os.getenv("EMS_SHIPPING_JPY", "2500"))   # EMS/eパケット送料(2kg超)
DHL_MAX_WEIGHT_KG = float(os.getenv("DHL_MAX_WEIGHT_KG", "2.0"))  # DHLのkg上限
AU_FEE_RATE = float(os.getenv("AU_FEE_RATE", "0.15"))  # Amazon AU 手数料率
PRICE_UPDATE_THRESHOLD = float(os.getenv("PRICE_UPDATE_THRESHOLD", "3"))  # 価格変動閾値(%)

# AU 出品価格の上乗せ係数（粗利確保のためのバッファ）
PRICE_MARKUP_MULTIPLIER = float(os.getenv("PRICE_MARKUP_MULTIPLIER", "1.0"))

# ── FBM 発送リードタイム ─────────────────────────────────────
# 注文から発送完了までの最大日数（ハンドリングタイム）
# 競合他社は通常 2〜3日設定。未設定だとアカウントデフォルト(5〜7日)になりBuyBox不利
HANDLING_TIME_DAYS = int(os.getenv("HANDLING_TIME_DAYS", "2"))

# ── スケジューラー設定 ────────────────────────────────────────
SCHEDULER_EXCHANGE_RATE_MINUTES = int(os.getenv("SCHEDULER_EXCHANGE_RATE_MINUTES", "30"))
SCHEDULER_JP_PRICE_HOURS = int(os.getenv("SCHEDULER_JP_PRICE_HOURS", "1"))
SCHEDULER_JP_STOCK_HOURS = int(os.getenv("SCHEDULER_JP_STOCK_HOURS", "2"))

# ── スクレイピング設定 ────────────────────────────────────────
SCRAPER_REQUEST_DELAY = float(os.getenv("SCRAPER_REQUEST_DELAY", "2.0"))  # リクエスト間隔(秒)
SCRAPER_MAX_PAGES = int(os.getenv("SCRAPER_MAX_PAGES", "10"))  # 最大ページ数

# ── データベース ──────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "arbitrage.db")

# ── SKU プレフィックス ────────────────────────────────────────
SKU_PREFIX = ""

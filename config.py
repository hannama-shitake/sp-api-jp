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
INTL_SHIPPING_JPY = int(os.getenv("INTL_SHIPPING_JPY", "3800"))  # 国際送料デフォルト(円) DHL想定
DHL_SHIPPING_JPY = int(os.getenv("DHL_SHIPPING_JPY", "3800"))   # DHL送料(2kg以下)
EMS_SHIPPING_JPY = int(os.getenv("EMS_SHIPPING_JPY", "2500"))   # EMS/eパケット送料(2kg超)
DHL_MAX_WEIGHT_KG = float(os.getenv("DHL_MAX_WEIGHT_KG", "2.0"))  # DHLのkg上限
AU_FEE_RATE = float(os.getenv("AU_FEE_RATE", "0.15"))  # Amazon AU 手数料率
PRICE_UPDATE_THRESHOLD = float(os.getenv("PRICE_UPDATE_THRESHOLD", "3"))  # 価格変動閾値(%)
MAX_FAIR_PRICE_RATIO = float(os.getenv("MAX_FAIR_PRICE_RATIO", "3.0"))  # Amazon Fair Pricing Policy: JP基準価格(JPY×rate)の最大倍率

# AU 出品価格の上乗せ係数（粗利確保のためのバッファ）
PRICE_MARKUP_MULTIPLIER = float(os.getenv("PRICE_MARKUP_MULTIPLIER", "1.0"))

# ── eBay 認証 ────────────────────────────────────────────────
EBAY_APP_ID = os.getenv("EBAY_APP_ID", "")        # Client ID
EBAY_DEV_ID = os.getenv("EBAY_DEV_ID", "")        # Dev ID
EBAY_CERT_ID = os.getenv("EBAY_CERT_ID", "")      # Client Secret
EBAY_USER_TOKEN = os.getenv("EBAY_USER_TOKEN", "")  # OAuth User Token
EBAY_SITE_ID = int(os.getenv("EBAY_SITE_ID", "0"))  # 0=US, 15=AU
EBAY_FEE_RATE = float(os.getenv("EBAY_FEE_RATE", "0.1325"))  # 13.25%
JPY_TO_USD_FALLBACK = float(os.getenv("JPY_TO_USD_FALLBACK", "0.0067"))  # 1 JPY ≈ 0.0067 USD

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

# ── Ship&co 認証・発送元情報 ──────────────────────────────────
SHIPCO_API_TOKEN    = os.getenv("SHIPCO_API_TOKEN", "")
SHIPCO_FROM_NAME    = os.getenv("SHIPCO_FROM_NAME", "Yamamoto Takeshi")
SHIPCO_FROM_COMPANY = os.getenv("SHIPCO_FROM_COMPANY", "IQQOW")
SHIPCO_FROM_EMAIL   = os.getenv("SHIPCO_FROM_EMAIL", "")
SHIPCO_FROM_PHONE   = os.getenv("SHIPCO_FROM_PHONE", "")
SHIPCO_FROM_ZIP     = os.getenv("SHIPCO_FROM_ZIP", "")
SHIPCO_FROM_PROVINCE= os.getenv("SHIPCO_FROM_PROVINCE", "IBARAKI")
SHIPCO_FROM_CITY    = os.getenv("SHIPCO_FROM_CITY", "TSUKUBA")
SHIPCO_FROM_ADDRESS1= os.getenv("SHIPCO_FROM_ADDRESS1", "")
SHIPCO_MAX_DHL_JPY  = int(os.getenv("SHIPCO_MAX_DHL_JPY", "5000"))  # DHL上限(円)、超えたらeパケットに切替

# ── プロキシ設定（スクレイピング用）─────────────────────────────────
PROXY_USER = os.getenv("PROXY_USER", "")
PROXY_PASS = os.getenv("PROXY_PASS", "")
# IPアドレス:ポート のリスト（負荷分散用）
PROXY_LIST = [
    ("31.59.20.176",    "6754"),
    ("198.23.239.134",  "6540"),
    ("45.38.107.97",    "6014"),
]

# ── 競合セラー URL リスト（catalog_discover で使用）────────────────
# 例: "https://www.amazon.com.au/s?me=A1XXXXX&marketplaceID=A39IBJ37TRP1C6"
# 複数の場合はカンマ区切り
SELLER_URLS: list = [
    u.strip() for u in os.getenv("SELLER_URLS", "").split(",") if u.strip()
]

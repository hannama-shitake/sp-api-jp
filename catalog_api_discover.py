"""
Amazon JP カタログ API（searchCatalogItems）から ASIN を発掘して AU に出品するスクリプト。
スクレイピング不要。SP-API を直接叩くため IP ブロックなし・安定動作。

核心ルール（全条件 AND）:
  ① JP ランキング上位（カテゴリ内 --rank-limit 以内、デフォルト 10,000）
  ② AU に既存セラーが --min-au-sellers 人以上
     → 版権品・地域制限・輸入禁止商品は自然にゼロになり除外される
  ③ 真贋ブラックリスト非該当（luxury ブランド・規制カテゴリ・NGワード）
  ④ ListingsRestrictions でアカウント出品制限なし
  ⑤ JP 在庫あり・価格取得可能
  ⑥ 粗利率 ≥ MIN_PROFIT_RATE

使い方:
  python catalog_api_discover.py               # 実行（上限 200 件）
  python catalog_api_discover.py --dry-run     # テスト（出品しない）
  python catalog_api_discover.py --max-new 50  # 上限 50 件
  python catalog_api_discover.py --rank-limit 5000    # ランキング上位 5000 以内
  python catalog_api_discover.py --min-au-sellers 2   # AU セラー 2 人以上必須
  python catalog_api_discover.py --nodes-only         # ブラウズノード検索のみ
  python catalog_api_discover.py --brands-only        # ブランド検索のみ
"""
import argparse
import json
import os
import sys
import time
from typing import Optional

# Windows CP932 ターミナルでも日本語・記号が表示できるように
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from sp_api.api import CatalogItems, Products, ListingsItems, ListingsRestrictions
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit
from utils.candidates_db import (
    init_db as _init_candidates_db,
    upsert_candidates,
    update_candidate,
    get_checked_today_asins,
    STATUS_LISTED, STATUS_NG, STATUS_RESTRICTED,
)
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

MARKETPLACE_JP = config.MARKETPLACE_JP
MARKETPLACE_AU = config.MARKETPLACE_AU

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP_CREDS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

# API インターバル（レート制限に合わせて調整済み）
CATALOG_INTERVAL     = 0.6   # searchCatalogItems: 2 req/s burst=5
JP_INTERVAL          = 2.1   # Products JP: 0.5 req/s
AU_INTERVAL          = 2.1   # Products AU: 0.5 req/s
PATCH_INTERVAL       = 0.3   # ListingsItems: 5 req/s
RESTRICTION_INTERVAL = 1.1   # ListingsRestrictions: 1 req/s


# ─────────────────────────────────────────────
# ターゲット設定
# ─────────────────────────────────────────────

# JP ブラウズノード（カテゴリ）
# Amazon.co.jp のカテゴリURL: amazon.co.jp/s?rh=n:XXXXX で node ID を確認できる
TARGET_BROWSE_NODES = [
    # ラベル,                   browse_node_id
    ("フィギュア全般",          "13793682051"),
    ("アクションフィギュア",    "2277719051"),
    ("プラモデル",              "13793683051"),
    ("ダイキャスト・ミニカー",  "13793686051"),
    ("トレーディングカード",    "2277721051"),
    ("デジタルカメラ",          "2127213051"),
    ("カメラレンズ",            "2127222051"),
    ("双眼鏡・望遠鏡",         "2127225051"),
    ("釣り竿",                  "2028961051"),
    ("リール",                  "2028971051"),
    ("ルアー・フライ",          "2028981051"),
    ("ヘッドフォン",            "2016929051"),
    ("スピーカー",              "2016930051"),
    ("登山・アウトドア",        "14696781"),
    ("ギター・ベース",          "562038"),
]

# ターゲットブランド（ブラウズノードに加えてブランド別にも検索）
TARGET_BRANDS = [
    # ホビー・フィギュア
    "BANDAI", "Kotobukiya", "Good Smile Company", "Max Factory",
    "Medicom Toy", "ALTER", "FREEing", "STRONGER",
    # プラモデル
    "Tamiya", "Fujimi", "Hasegawa", "Aoshima",
    # 釣り
    "SHIMANO", "DAIWA", "Abu Garcia", "Rapala", "MAJOR CRAFT",
    # カメラ
    "Sony", "Nikon", "Canon", "Olympus", "FUJIFILM",
    # オーディオ
    "Sennheiser", "Audio-Technica", "Beyerdynamic", "Shure",
    # アウトドア
    "mont-bell", "Snow Peak", "LOGOS",
]


# ─────────────────────────────────────────────
# 真贋ブラックリスト
# ─────────────────────────────────────────────

# 高級ブランド・コピー品多発ブランド（小文字で比較）
_BRAND_BLACKLIST = {
    # ラグジュアリーファッション
    "louis vuitton", "lv", "chanel", "gucci", "hermes", "hermès",
    "prada", "bottega veneta", "burberry", "dior", "christian dior",
    "fendi", "versace", "givenchy", "balenciaga", "saint laurent", "ysl",
    "celine", "céline", "valentino", "moncler", "off-white", "supreme",
    # 高級時計
    "rolex", "omega", "cartier", "patek philippe", "audemars piguet",
    "breguet", "iwc", "jaeger-lecoultre", "tag heuer", "breitling",
    "hublot", "vacheron constantin", "blancpain",
    # 高級バッグ・小物
    "coach", "kate spade", "michael kors", "tory burch",
    # 高級ジュエリー
    "tiffany", "bvlgari", "bulgari", "swarovski",
}

# タイトルキーワードブラックリスト
_TITLE_NG_KEYWORDS = [
    "replica", "fake", "knock off", "knockoff", "imitation", "counterfeit",
    "輸出禁止", "海外発送不可", "国内専用", "日本国内限定",
    "18禁", "r-18", "adult", "アダルト",
]

# NGブラウズノード（カテゴリ全体をスキップ）
# 食品・医薬品・化粧品・アルコール → AU規制が厳しく真贋も難しい
_NG_BROWSE_NODE_PREFIXES = {
    # 食品・飲料（jp.amazon.co.jp/b?node=以下）
    "2127210", "344520", "3419372",
    # 医薬品・医療機器
    "3256779", "3414009", "160778011",
    # 化粧品・スキンケア
    "50313",   "371850",  "2015683",
    # アルコール
    "13529143",
    # ベビー食品
    "2188184", "11248696",
}


def _is_ng_browse_node(node_id: str) -> bool:
    return any(node_id.startswith(prefix) for prefix in _NG_BROWSE_NODE_PREFIXES)


# ng_words.json のキャッシュ
_NG_WORDS_CACHE: list = []


def _load_ng_words() -> list:
    global _NG_WORDS_CACHE
    if _NG_WORDS_CACHE:
        return _NG_WORDS_CACHE
    try:
        path = os.path.join(os.path.dirname(__file__), "ng_words.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        words = []
        for category_words in data.values():
            if isinstance(category_words, list):
                words.extend([w.lower() for w in category_words])
        _NG_WORDS_CACHE = words
        logger.info("[catalog_api] NGワード辞書: %d件", len(words))
    except Exception as e:
        logger.warning("[catalog_api] NGワード辞書ロード失敗: %s", e)
    return _NG_WORDS_CACHE


def check_authenticity(title: str, brand: str, browse_node_id: str = "") -> tuple:
    """
    真贋フィルターを適用する。

    Returns:
        (is_ok: bool, reject_reason: str)
    """
    # 1. ブランドブラックリスト
    brand_lower = (brand or "").lower()
    for bl_brand in _BRAND_BLACKLIST:
        if bl_brand in brand_lower:
            return False, f"ブランドBL: {brand}"

    # 2. タイトルキーワードブラックリスト
    title_lower = (title or "").lower()
    for kw in _TITLE_NG_KEYWORDS:
        if kw in title_lower:
            return False, f"タイトルBL: {kw}"

    # 3. NGブラウズノード（カテゴリ）
    if browse_node_id and _is_ng_browse_node(browse_node_id):
        return False, f"NGカテゴリ: {browse_node_id}"

    # 4. ng_words.json
    ng_words = _load_ng_words()
    for word in ng_words:
        if word in title_lower:
            return False, f"NGワード: {word}"

    return True, ""


# ─────────────────────────────────────────────
# 1. JP カタログ検索
# ─────────────────────────────────────────────

def search_jp_catalog(
    browse_node_id: str = None,
    brand: str = None,
    max_pages: int = 5,
    rank_limit: int = 10000,
) -> list:
    """
    JP カタログを searchCatalogItems で検索する。

    Args:
        browse_node_id: ブラウズノード ID（カテゴリ指定）
        brand:          ブランド名指定
        max_pages:      最大取得ページ数（1ページ=20件）
        rank_limit:     このランキング以内のみ対象（None=制限なし）

    Returns:
        [{"asin", "rank", "title", "brand", "browse_node_id", "weight_kg"}]
    """
    api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    results = []
    page_token = None
    label = browse_node_id or brand or "?"

    kwargs = {
        "marketplaceIds":  [MARKETPLACE_JP],
        "includedData":    ["salesRanks", "summaries", "dimensions"],
        "pageSize":        20,
    }
    if browse_node_id:
        kwargs["classificationIds"] = [browse_node_id]
    if brand:
        kwargs["brandNames"] = [brand]

    for page_num in range(1, max_pages + 1):
        if page_token:
            kwargs["pageToken"] = page_token

        try:
            resp = api.search_catalog_items(**kwargs)
            payload = resp.payload or {}
            items = payload.get("items", [])

            if not items:
                break

            for item in items:
                asin = item.get("asin", "")
                if not asin or len(asin) != 10:
                    continue

                # タイトル・ブランド（JP マーケットプレイス優先）
                summaries = item.get("summaries") or []
                title, item_brand = "", ""
                for s in summaries:
                    if s.get("marketplaceId") == MARKETPLACE_JP:
                        title      = s.get("itemName", "")
                        item_brand = s.get("brand", "")
                        break
                if not title and summaries:
                    title      = summaries[0].get("itemName", "")
                    item_brand = summaries[0].get("brand", "")

                # ランキング（カテゴリ内最高位を採用）
                rank = None
                found_node = ""
                for rank_data in (item.get("salesRanks") or []):
                    if rank_data.get("marketplaceId") != MARKETPLACE_JP:
                        continue
                    for cr in (rank_data.get("classificationRanks") or []):
                        r = cr.get("rank")
                        if r is not None and (rank is None or r < rank):
                            rank = r
                            found_node = cr.get("classificationId", browse_node_id or "")

                # ランク上限フィルター（ランク情報なしは通す）
                if rank is not None and rank_limit and rank > rank_limit:
                    continue

                # 重量（容積重量考慮）
                weight_kg = _extract_weight_kg(item.get("dimensions"))

                results.append({
                    "asin":          asin,
                    "rank":          rank,
                    "title":         title,
                    "brand":         item_brand,
                    "browse_node_id": found_node or browse_node_id or "",
                    "weight_kg":     weight_kg,
                })

            logger.info("[catalog_api] 検索 %s page%d: %d件取得 累計%d件",
                        label, page_num, len(items), len(results))

            page_token = payload.get("nextToken")
            if not page_token:
                break

        except SellingApiException as e:
            logger.warning("[catalog_api] 検索エラー %s page%d: %s", label, page_num, e)
            break

        time.sleep(CATALOG_INTERVAL)

    return results


def _extract_weight_kg(dimensions_list) -> Optional[float]:
    """CatalogItems API の dimensions から有効重量(kg)を返す"""
    if not dimensions_list:
        return None
    try:
        for dim in (dimensions_list or []):
            w = dim.get("weight") or {}
            val = w.get("value")
            unit = (w.get("unit") or "").lower()
            if val is None:
                continue
            val = float(val)
            if "kilogram" in unit or unit == "kg":
                actual_kg = val
            elif "gram" in unit:
                actual_kg = val / 1000.0
            elif "pound" in unit or unit == "lb":
                actual_kg = val * 0.453592
            else:
                actual_kg = None

            # 容積重量
            def to_cm(d):
                v2, u2 = d.get("value"), (d.get("unit") or "").lower()
                if v2 is None:
                    return None
                v2 = float(v2)
                if "centimeter" in u2 or u2 == "cm":
                    return v2
                if "inch" in u2:
                    return v2 * 2.54
                if "millimeter" in u2 or u2 == "mm":
                    return v2 / 10.0
                return None

            l_cm = to_cm(dim.get("length") or {})
            w_cm = to_cm(dim.get("width")  or {})
            h_cm = to_cm(dim.get("height") or {})
            vol_kg = round(l_cm * w_cm * h_cm / 5000.0, 3) if (l_cm and w_cm and h_cm) else None

            candidates = [k for k in [actual_kg, vol_kg] if k is not None]
            if candidates:
                return max(candidates)
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────
# 2. AU セラー確認（版権・地域制限チェックの核心）
# ─────────────────────────────────────────────

def get_au_sellers(asin: str) -> tuple:
    """
    AU マーケットプレイスの既存セラー数と最安値を返す。

    AU セラー = 0 の場合:
      - 版権品でAU輸入が禁止されている可能性
      - アニメ等の地域ライセンス商品
      - 品質/安全規制でAU販売不可の商品
      → いずれも出品リスクが高いためスキップ

    Returns:
        (seller_count: int, min_price_aud: float or None)
    """
    api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")

    try:
        resp = api.get_item_offers(asin, item_condition="New")
        payload = resp.payload if hasattr(resp, "payload") else {}
        offers = payload.get("Offers", [])

        competitor_offers = [
            o for o in offers
            if o.get("SellerId", "") != my_id
            and o.get("ListingPrice", {}).get("Amount")
        ]
        count = len(competitor_offers)
        min_price = (
            min(float(o["ListingPrice"]["Amount"]) for o in competitor_offers)
            if competitor_offers else None
        )
        return count, min_price

    except SellingApiException as e:
        err_str = str(e)
        # AUに存在しないASINは0人として扱う（スキップ対象）
        if any(kw in err_str for kw in ("InvalidParameterValue", "ItemNotApplicable", "NoOfferListings")):
            return 0, None
        logger.debug("[catalog_api] AU seller check error %s: %s", asin, e)
        return 0, None


# ─────────────────────────────────────────────
# 3. JP 価格取得
# ─────────────────────────────────────────────

def get_jp_price(asin: str) -> tuple:
    """
    JP 競合価格と在庫状況を返す。
    独占出品（1セラー）の場合は get_item_offers でフォールバック。

    Returns:
        (price_jpy: int or None, in_stock: bool)
    """
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    try:
        resp = api.get_competitive_pricing_for_asins([asin])
        items = resp.payload if isinstance(resp.payload, list) else []
        for item in items:
            if item.get("ASIN") != asin:
                continue
            comp_prices = (
                item.get("Product", {})
                .get("CompetitivePricing", {})
                .get("CompetitivePrices", [])
            )
            price_jpy = None
            for cp in comp_prices:
                if cp.get("condition") == "New":
                    amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                    if amount:
                        price_jpy = int(float(amount))
                    break

            offer_listings = (
                item.get("Product", {})
                .get("CompetitivePricing", {})
                .get("NumberOfOfferListings", [])
            )
            new_count = sum(
                ol.get("Count", 0) for ol in offer_listings
                if (ol.get("condition") or "").lower() in ("new", "new_new")
            )
            in_stock = new_count > 0

            # 独占出品フォールバック
            if in_stock and not price_jpy:
                try:
                    r2 = api.get_item_offers(asin, item_condition="New")
                    offers = (r2.payload or {}).get("Offers", [])
                    prices = [
                        int(float(o["ListingPrice"]["Amount"]))
                        for o in offers
                        if o.get("ListingPrice", {}).get("Amount")
                    ]
                    if prices:
                        price_jpy = min(prices)
                except Exception:
                    pass
                time.sleep(JP_INTERVAL)

            return price_jpy, in_stock

    except SellingApiException as e:
        logger.debug("[catalog_api] JP price error %s: %s", asin, e)

    return None, False


# ─────────────────────────────────────────────
# 4. 出品制限チェック
# ─────────────────────────────────────────────

def check_listing_restriction(asin: str, seller_id: str) -> tuple:
    """
    ListingsRestrictions API で出品制限を確認する。

    Returns:
        (is_restricted: bool, reason_code: str)
    """
    try:
        api = ListingsRestrictions(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        resp = api.get_listings_restrictions(
            asin=asin,
            sellerId=seller_id,
            marketplaceIds=[MARKETPLACE_AU],
            conditionType="new_new",
        )
        restrictions = (resp.payload or {}).get("restrictions", [])
        if not restrictions:
            return False, ""
        for r in restrictions:
            for reason in (r.get("reasons") or []):
                code = reason.get("reasonCode", "RESTRICTED")
                return True, code
        return True, "RESTRICTED"
    except SellingApiException as e:
        logger.debug("[catalog_api] ListingsRestrictions error %s: %s", asin, e)
        return False, ""
    except Exception:
        return False, ""


# ─────────────────────────────────────────────
# 5. 新規出品（catalog_discover.py と同一ロジック）
# ─────────────────────────────────────────────

def list_new_item(api, seller_id: str, asin: str, price_aud: float) -> tuple:
    """
    新規 FBM 相乗り出品: PUT → PATCH の 2 ステップ。

    Returns:
        (success: bool, sku_or_error_msg: str)
    """
    sku = f"{config.SKU_PREFIX}{asin}"

    put_body = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [
                {"value": asin, "marketplace_id": MARKETPLACE_AU}
            ],
            "condition_type": [
                {"value": "new_new", "marketplace_id": MARKETPLACE_AU}
            ],
            "fulfillment_availability": [
                {
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": 1,
                    "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                    "marketplace_id": MARKETPLACE_AU,
                }
            ],
            "purchasable_offer": [
                {
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                    "marketplace_id": MARKETPLACE_AU,
                }
            ],
        },
    }
    resp = api.put_listings_item(
        sellerId=seller_id,
        sku=sku,
        marketplaceIds=[MARKETPLACE_AU],
        body=put_body,
    )
    status = resp.payload.get("status", "")
    if status not in ("ACCEPTED", "VALID"):
        issues = resp.payload.get("issues", [])
        msg = "; ".join(i.get("message", "") for i in issues)
        return False, msg

    time.sleep(10)  # Amazon の非同期処理を待つ

    patch_body = {
        "productType": "PRODUCT",
        "patches": [
            {
                "op":    "replace",
                "path":  "/attributes/fulfillment_availability",
                "value": [{
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": 1,
                    "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                    "marketplace_id": MARKETPLACE_AU,
                }],
            },
            {
                "op":    "replace",
                "path":  "/attributes/purchasable_offer",
                "value": [{
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                    "marketplace_id": MARKETPLACE_AU,
                }],
            },
        ],
    }
    try:
        api.patch_listings_item(
            sellerId=seller_id,
            sku=sku,
            marketplaceIds=[MARKETPLACE_AU],
            body=patch_body,
        )
    except Exception as e:
        logger.warning("[catalog_api] PATCH失敗（PUTは成功） %s: %s", sku, e)

    return True, sku


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="JP カタログ API → AU 出品スクリプト")
    parser.add_argument("--dry-run",         action="store_true", help="出品しない（確認のみ）")
    parser.add_argument("--max-new",         type=int,   default=200,   help="新規出品上限（デフォルト200）")
    parser.add_argument("--rank-limit",      type=int,   default=10000, help="JP ランキング上限（デフォルト10000）")
    parser.add_argument("--min-au-sellers",  type=int,   default=1,     help="AU 既存セラー最低人数（デフォルト1）")
    parser.add_argument("--max-pages",       type=int,   default=5,     help="カテゴリ/ブランドあたり最大ページ数（デフォルト5）")
    parser.add_argument("--nodes-only",      action="store_true", help="ブラウズノード検索のみ")
    parser.add_argument("--brands-only",     action="store_true", help="ブランド検索のみ")
    args = parser.parse_args()

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[catalog_api] AMAZON_AU_SELLER_ID が未設定")
        sys.exit(1)

    if args.dry_run:
        logger.info("[catalog_api] *** DRY-RUN モード ***")

    exchange_rate = get_jpy_to_aud()
    logger.info("[catalog_api] 為替: 1 JPY = %.6f AUD", exchange_rate)

    # ── STEP 1: JP カタログ検索 ─────────────────────────────────────
    _init_candidates_db()
    checked_today = get_checked_today_asins()

    candidates: dict = {}   # asin → item dict（重複排除）

    # ブラウズノード検索
    if not args.brands_only:
        for label, node_id in TARGET_BROWSE_NODES:
            if _is_ng_browse_node(node_id):
                logger.info("[catalog_api] NGカテゴリスキップ: %s (%s)", label, node_id)
                continue
            logger.info("[catalog_api] ブラウズノード検索: %s (%s)", label, node_id)
            items = search_jp_catalog(
                browse_node_id=node_id,
                max_pages=args.max_pages,
                rank_limit=args.rank_limit,
            )
            for item in items:
                asin = item["asin"]
                if asin not in candidates:
                    candidates[asin] = item
            logger.info("[catalog_api] %s: %d件収集 → 累計候補 %d件", label, len(items), len(candidates))
            time.sleep(1.0)

    # ブランド検索
    if not args.nodes_only:
        for brand in TARGET_BRANDS:
            logger.info("[catalog_api] ブランド検索: %s", brand)
            items = search_jp_catalog(
                brand=brand,
                max_pages=args.max_pages,
                rank_limit=args.rank_limit,
            )
            for item in items:
                asin = item["asin"]
                if asin not in candidates:
                    candidates[asin] = item
            logger.info("[catalog_api] %s: %d件収集 → 累計候補 %d件", brand, len(items), len(candidates))
            time.sleep(1.0)

    logger.info("[catalog_api] JP カタログ検索完了: %d件候補", len(candidates))

    if not candidates:
        logger.info("[catalog_api] 候補なし。終了")
        return

    # 候補DBに全件保存
    all_asins = list(candidates.keys())
    db_added = upsert_candidates(all_asins)
    logger.info("[catalog_api] 候補DB: +%d件新規追加", db_added)

    # ── STEP 2: 個別チェック & 出品 ────────────────────────────────
    listings_api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    listed = failed = skipped_auth = skipped_au = skipped_profit = skipped_restrict = 0
    listed_details = []

    for i, asin in enumerate(all_asins):
        if listed >= args.max_new:
            logger.info("[catalog_api] 上限 %d件に達した。残り %d件スキップ",
                        args.max_new, len(all_asins) - i)
            break

        if asin in checked_today:
            continue

        item   = candidates[asin]
        title  = item.get("title", "") or asin
        brand  = item.get("brand", "")
        node   = item.get("browse_node_id", "")
        rank   = item.get("rank")
        weight = item.get("weight_kg")

        rank_str = f"rank{rank:,}" if rank else "rank不明"
        log_prefix = f"[{i+1}/{len(all_asins)}] {asin} {rank_str}"

        # ── フィルター① 真贋チェック ──
        auth_ok, auth_reason = check_authenticity(title, brand, node)
        if not auth_ok:
            logger.info("[catalog_api] 真贋NG %s: %s | %s", log_prefix, auth_reason, title[:50])
            update_candidate(asin, title=title, status="ng", skip_reason=auth_reason)
            skipped_auth += 1
            continue

        # ── フィルター② AU セラー確認（版権・地域制限の核心チェック）──
        time.sleep(AU_INTERVAL)
        au_seller_count, au_min_price = get_au_sellers(asin)
        update_candidate(asin, seller_count=au_seller_count, au_price=au_min_price)

        if au_seller_count < args.min_au_sellers:
            logger.info("[catalog_api] AU セラー不足 %s: %d人 | %s",
                        log_prefix, au_seller_count, title[:50])
            skipped_au += 1
            continue

        logger.info("[catalog_api] AU %d人 最安AU$%.2f | %s",
                    au_seller_count, au_min_price or 0, title[:40])

        # ── フィルター③ JP 価格・在庫確認 ──
        time.sleep(JP_INTERVAL)
        jp_price, in_stock = get_jp_price(asin)
        update_candidate(asin, title=title, weight_kg=weight, jp_price=jp_price)

        if not in_stock or not jp_price:
            logger.debug("[catalog_api] JP在庫なし/価格なし %s", asin)
            continue

        # ── フィルター④ 利益確認 ──
        profit = calc_profit(
            asin=asin, title=title,
            jp_price_jpy=jp_price,
            au_price_aud=au_min_price,
            exchange_rate=exchange_rate,
            weight_kg=weight,
        )
        recommended_price = calc_optimal_au_price(
            jp_price_jpy=jp_price,
            exchange_rate=exchange_rate,
            weight_kg=weight,
        )

        if not profit.is_profitable:
            logger.info("[catalog_api] 利益不足 %s: AU$%.2f (推奨AU$%.2f) 粗利%.1f%% | %s",
                        log_prefix, au_min_price or 0, recommended_price,
                        profit.profit_rate, title[:40])
            skipped_profit += 1
            continue

        # ── フィルター⑤ 出品制限チェック ──
        time.sleep(RESTRICTION_INTERVAL)
        is_restricted, restrict_code = check_listing_restriction(asin, seller_id)
        if is_restricted:
            logger.info("[catalog_api] 出品制限 %s: %s | %s",
                        log_prefix, restrict_code, title[:50])
            update_candidate(asin, status=STATUS_RESTRICTED, skip_reason=restrict_code)
            skipped_restrict += 1
            continue

        # ── 出品価格決定（競合最安値を BuyBox アンダーカット、または推奨価格） ──
        if au_min_price:
            listing_price = round(au_min_price * (1 - config.BUYBOX_UNDERCUT_RATE), 2)
            # ただし推奨価格（利益確保）を下回らない
            listing_price = max(listing_price, recommended_price)
        else:
            listing_price = recommended_price

        log_msg = (
            f"{asin} | {title[:45]} | "
            f"JP¥{jp_price:,} → AU${listing_price:.2f} "
            f"(粗利{profit.profit_rate:.1f}% AU競合{au_seller_count}人)"
        )

        if args.dry_run:
            logger.info("[catalog_api][DRY] 出品予定: %s", log_msg)
            listed_details.append({
                "asin":    asin, "title": title,
                "jp_price": jp_price, "price_aud": listing_price,
                "au_sellers": au_seller_count, "profit_rate": profit.profit_rate,
            })
            listed += 1
            continue

        # ── 実際に出品 ──
        try:
            success, sku_or_err = list_new_item(listings_api, seller_id, asin, listing_price)
            time.sleep(PATCH_INTERVAL)

            if success:
                logger.info("[catalog_api] 出品完了: %s | SKU=%s", log_msg, sku_or_err)
                update_candidate(asin, status=STATUS_LISTED, listed_sku=sku_or_err,
                                 jp_price=jp_price, au_price=listing_price)
                listed_details.append({
                    "asin":    asin, "title": title,
                    "jp_price": jp_price, "price_aud": listing_price,
                    "au_sellers": au_seller_count, "profit_rate": profit.profit_rate,
                })
                listed += 1
            else:
                logger.warning("[catalog_api] 出品失敗: %s | %s", asin, sku_or_err)
                failed += 1

        except Exception as e:
            logger.error("[catalog_api] 出品例外 %s: %s", asin, e)
            failed += 1

    # ── サマリー ─────────────────────────────────────────────────────
    logger.info(
        "[catalog_api] 完了: 出品%d / AU不足%d / 真贋NG%d / 利益不足%d / 制限%d / 失敗%d",
        listed, skipped_au, skipped_auth, skipped_profit, skipped_restrict, failed,
    )

    dry_label = "[DRY-RUN] " if args.dry_run else ""
    subject = (
        f"[JP-API] {dry_label}カタログ発掘: "
        f"新規{listed}件 / AU不足{skipped_au}件 / 真贋NG{skipped_auth}件"
    )
    lines = [
        f"=== {dry_label}JP カタログ API 発掘結果 ===",
        "",
        f"JP検索候補:          {len(all_asins)}件",
        f"新規出品{'予定' if args.dry_run else '完了'}:          {listed}件",
        f"AU セラー不足(スキップ): {skipped_au}件  ← 版権品・地域制限品",
        f"真贋NG(スキップ):      {skipped_auth}件",
        f"利益不足(スキップ):    {skipped_profit}件",
        f"出品制限(スキップ):    {skipped_restrict}件",
        f"失敗:                {failed}件",
        "",
        f"設定: JPランク上限={args.rank_limit:,} / AU最低セラー={args.min_au_sellers}人"
        f" / 利益率={config.MIN_PROFIT_RATE}%",
        "",
    ]
    if listed_details:
        lines.append(f"--- 出品{'予定' if args.dry_run else '完了'} ({listed}件) ---")
        for d in listed_details[:50]:
            lines.append(
                f"  {d['asin']}  JP¥{d['jp_price']:,} → AU${d['price_aud']:.2f}"
                f"  粗利{d['profit_rate']:.1f}%  競合{d['au_sellers']}人  {d['title'][:40]}"
            )
        if listed > 50:
            lines.append(f"  ... 他 {listed - 50}件")

    body = "\n".join(lines)
    send_email(subject=subject, body=body)
    try:
        print(body)
    except UnicodeEncodeError:
        print(body.encode("utf-8", errors="replace").decode("ascii", errors="replace"))


if __name__ == "__main__":
    main()

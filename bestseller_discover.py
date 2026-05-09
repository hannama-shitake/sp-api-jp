"""
bestseller_discover.py — AU ベストセラーランキング発掘 → JP仕入れ確認 → AU + eBay 同時出品

第4の柱: 「AU で今売れているものを見つけ、JP 仕入れ可能なら即出品」
  ① AU CatalogItems API でカテゴリ/ブランド別ベストセラーを取得（AU 在庫あり確定）
  ② 既存出品・チェック済みASIN を除外
  ③ 真贋・NGワードフィルタ
  ④ AU セラー確認（競合価格の取得 & 最低2人以上）
  ⑤ JP 在庫・価格確認（同一ASIN で JP にも存在するか）
  ⑥ 利益率チェック（MIN_PROFIT_RATE 以上）
  ⑦ 出品制限チェック（ListingsRestrictions）
  ⑧ Amazon AU 出品（FBM 相乗り PUT + PATCH）
  ⑨ eBay 同時出品（ebay_api.add_item）
  ⑩ メール通知

catalog_api_discover.py との違い:
  catalog_api_discover → JP カタログを検索 → AU に既存セラーがいるか確認
  bestseller_discover  → AU カタログを検索 → JP 仕入れ値が取れるか確認 → AU + eBay

実績カテゴリ（売上履歴より）:
  S∙H∙Figuarts >> ねんどろいど >> フィギュア系全般 >> Shimano 釣り具 >> ゲームパッド

使い方:
  python bestseller_discover.py               # 実行（上限200件）
  python bestseller_discover.py --dry-run     # テスト（出品しない）
  python bestseller_discover.py --max-new 50  # 上限50件
  python bestseller_discover.py --rank-limit 5000   # AU ランキング上位5000以内
  python bestseller_discover.py --min-au-sellers 2  # AU セラー2人以上必須
  python bestseller_discover.py --max-pages 3       # 検索あたり最大ページ数
  python bestseller_discover.py --skip-ebay         # eBay 出品をスキップ
"""
import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
from typing import Optional

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

import requests as _requests
from sp_api.api import CatalogItems, Products, ListingsItems, ListingsRestrictions, Reports
from sp_api.base import Marketplaces, SellingApiException

import config
from apis import ebay_api
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
    "refresh_token":    config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":       config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP_CREDS = {
    "refresh_token":    config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id":       config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

# API インターバル
CATALOG_INTERVAL     = 0.6   # searchCatalogItems: 2 req/s burst=5
JP_INTERVAL          = 2.1   # Products JP: 0.5 req/s
AU_INTERVAL          = 2.1   # Products AU: 0.5 req/s
PATCH_INTERVAL       = 0.3   # ListingsItems: 5 req/s
RESTRICTION_INTERVAL = 1.1   # ListingsRestrictions: 1 req/s
EBAY_INTERVAL        = 0.5   # eBay AddItem


# ─────────────────────────────────────────────
# AU カタログ検索ターゲット
# 売上実績（2024/11-2025/12）をもとに選定
# ─────────────────────────────────────────────

# ラベル, keywords（AU英語+日本語混在OK）, 任意コメント
TARGET_AU_SEARCHES = [
    # ── アニメ・フィギュア（最高売上カテゴリ）──
    ("SH Figuarts",        "SH Figuarts bandai"),
    ("ねんどろいど",        "nendoroid good smile"),
    ("figma",              "figma max factory"),
    ("MAFEX",              "MAFEX medicom figure"),
    ("POP UP PARADE",      "pop up parade figure"),
    ("アクションフィギュア", "action figure anime collectible"),
    # ── プラモデル・ガンプラ ──
    ("ガンプラ",            "gundam gunpla bandai model kit"),
    ("タミヤ模型",          "tamiya plastic model kit"),
    ("ミニカー",            "diecast minicar tomica"),
    # ── ゲームアクセサリー（ARTISAN等）──
    ("ゲーミングマウスパッド", "gaming mouse pad artisan"),
    ("ゲーミングデバイス",    "gaming peripheral keyboard mouse"),
    # ── 釣り具（Shimano/Daiwa実績あり）──
    ("Shimano リール",      "shimano fishing reel"),
    ("Daiwa リール",        "daiwa fishing reel"),
    ("フィッシングロッド",    "fishing rod japan"),
    # ── 音響・カメラ ──
    ("オーディオテクニカ",   "audio technica headphone"),
    ("ソニーカメラ",         "sony camera lens japan"),
    ("双眼鏡",              "binoculars birdwatching japan"),
    # ── アウトドア ──
    ("モンベル",             "mont-bell outdoor japan"),
    ("アウトドアツール",      "outdoor camping tool japan"),
    # ── 工具・DIY ──
    ("日本製工具",           "japanese tool knipex vessel"),
]

# ターゲットブランド（キーワード検索に加えてブランド別でも検索）
TARGET_AU_BRANDS = [
    # フィギュア
    "Bandai",
    "Good Smile Company",
    "Max Factory",
    "Medicom Toy",
    "Kotobukiya",
    # 釣り具
    "Shimano",
    "Daiwa",
    # オーディオ
    "Audio-Technica",
    "Sennheiser",
    # カメラ
    "Sony",
    "Fujifilm",
    # アウトドア
    "mont-bell",
    "Snow Peak",
    # ゲーミング
    "ARTISAN",
]


# ─────────────────────────────────────────────
# 真贋ブラックリスト（catalog_api_discover.py と共通）
# ─────────────────────────────────────────────

_BRAND_BLACKLIST = {
    "louis vuitton", "lv", "chanel", "gucci", "hermes", "hermès",
    "prada", "bottega veneta", "burberry", "dior", "christian dior",
    "fendi", "versace", "givenchy", "balenciaga", "saint laurent", "ysl",
    "celine", "céline", "valentino", "moncler", "off-white", "supreme",
    "rolex", "omega", "cartier", "patek philippe", "audemars piguet",
    "breguet", "iwc", "jaeger-lecoultre", "tag heuer", "breitling",
    "hublot", "vacheron constantin", "blancpain",
    "coach", "kate spade", "michael kors", "tory burch",
    "tiffany", "bvlgari", "bulgari", "swarovski",
}

_TITLE_NG_KEYWORDS = [
    "replica", "fake", "knock off", "knockoff", "imitation", "counterfeit",
    "輸出禁止", "海外発送不可", "国内専用", "日本国内限定",
    "18禁", "r-18", "adult", "アダルト",
]

_NG_BROWSE_NODE_PREFIXES = {
    "2127210", "344520", "3419372",   # 食品・飲料
    "3256779", "3414009", "160778011", # 医薬品・医療機器
    "50313",   "371850",  "2015683",  # 化粧品
    "13529143",                        # アルコール
    "2188184", "11248696",             # ベビー食品
}

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
        for v in data.values():
            if isinstance(v, list):
                words.extend([w.lower() for w in v])
        _NG_WORDS_CACHE = words
        logger.info("[bestseller] NGワード辞書: %d件", len(words))
    except Exception as e:
        logger.warning("[bestseller] NGワード辞書ロード失敗: %s", e)
    return _NG_WORDS_CACHE


def _is_ng_browse_node(node_id: str) -> bool:
    return any(node_id.startswith(p) for p in _NG_BROWSE_NODE_PREFIXES)


def check_authenticity(title: str, brand: str, node_id: str = "") -> tuple:
    brand_lower = (brand or "").lower()
    for bl in _BRAND_BLACKLIST:
        if bl in brand_lower:
            return False, f"ブランドBL: {brand}"
    title_lower = (title or "").lower()
    for kw in _TITLE_NG_KEYWORDS:
        if kw in title_lower:
            return False, f"タイトルBL: {kw}"
    if node_id and _is_ng_browse_node(node_id):
        return False, f"NGカテゴリ: {node_id}"
    for word in _load_ng_words():
        if word in title_lower:
            return False, f"NGワード: {word}"
    return True, ""


# ─────────────────────────────────────────────
# 1. 既存AU出品ASIN取得
# ─────────────────────────────────────────────

def get_active_asins() -> set:
    """Reports API で現在のアクティブ出品 ASIN セットを返す"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    logger.info("[bestseller] AU出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[bestseller] レポートステータス: %s (%d/120)", status, attempt + 1)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")
    else:
        raise RuntimeError("レポートタイムアウト")

    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")
    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    reader = csv.DictReader(
        io.StringIO(content.decode("utf-8", errors="replace")), delimiter="\t"
    )

    active = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        if asin and len(asin) == 10 and row.get("status", "").strip().lower() == "active":
            active.add(asin)

    logger.info("[bestseller] アクティブ出品: %d件", len(active))
    return active


# ─────────────────────────────────────────────
# 2. AU カタログ検索（ベストセラー発掘）
# ─────────────────────────────────────────────

def _extract_weight_kg(dimensions_list) -> Optional[float]:
    """CatalogItems API の dimensions から有効重量(kg)を返す"""
    if not dimensions_list:
        return None
    try:
        for dim in dimensions_list:
            w = dim.get("weight") or {}
            val, unit = w.get("value"), (w.get("unit") or "").lower()
            if val is None:
                continue
            val = float(val)
            if "kilogram" in unit or unit == "kg":
                actual_kg = val
            elif "gram" in unit:
                actual_kg = val / 1000.0
            elif "pound" in unit or "lb" in unit:
                actual_kg = val * 0.453592
            else:
                actual_kg = None

            def to_cm(d):
                v2, u2 = d.get("value"), (d.get("unit") or "").lower()
                if v2 is None:
                    return None
                v2 = float(v2)
                if "centimeter" in u2 or u2 == "cm":  return v2
                if "inch" in u2:                       return v2 * 2.54
                if "millimeter" in u2 or u2 == "mm":  return v2 / 10.0
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


def search_au_bestsellers(
    keywords: str,
    brand: str = None,
    max_pages: int = 5,
    rank_limit: int = 5000,
) -> list:
    """
    AU マーケットプレイスのカタログを searchCatalogItems で検索し、
    ベストセラー（AU ランキング上位）を返す。

    JP catalog_api_discover との違い:
      - AU 認証で AU マーケットプレイスを検索
      - salesRanks は AU ランキングを参照
      - 同じ ASIN が JP にも存在することを後続ステップで確認する

    Args:
        keywords:    検索キーワード（必須）
        brand:       ブランド名フィルタ（任意）
        max_pages:   最大取得ページ数（1ページ=20件）
        rank_limit:  AU ランキングこの値以内のみ対象（None なら全件）

    Returns:
        [{"asin", "rank", "title", "brand", "browse_node_id", "weight_kg"}]
    """
    api = CatalogItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU,
                       version="2022-04-01")
    results = []
    page_token = None
    label = brand or keywords

    kwargs = {
        "marketplaceIds": [MARKETPLACE_AU],
        "keywords":       keywords,
        "includedData":   ["salesRanks", "summaries", "dimensions"],
        "pageSize":       20,
    }
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

                # タイトル・ブランド（AU マーケットプレイス優先）
                summaries = item.get("summaries") or []
                title, item_brand = "", ""
                for s in summaries:
                    if s.get("marketplaceId") == MARKETPLACE_AU:
                        title      = s.get("itemName", "")
                        item_brand = s.get("brand", "")
                        break
                if not title and summaries:
                    title      = summaries[0].get("itemName", "")
                    item_brand = summaries[0].get("brand", "")

                # AU ランキング（カテゴリ内最高位を採用）
                rank = None
                found_node = ""
                for rank_data in (item.get("salesRanks") or []):
                    if rank_data.get("marketplaceId") != MARKETPLACE_AU:
                        continue
                    for cr in (rank_data.get("classificationRanks") or []):
                        r = cr.get("rank")
                        if r is not None and (rank is None or r < rank):
                            rank = r
                            found_node = cr.get("classificationId", "")

                # ランク上限フィルタ（ランク不明は通す）
                if rank is not None and rank_limit and rank > rank_limit:
                    continue

                results.append({
                    "asin":          asin,
                    "rank":          rank,
                    "title":         title,
                    "brand":         item_brand or brand or "",
                    "browse_node_id": found_node,
                    "weight_kg":     _extract_weight_kg(item.get("dimensions")),
                })

            logger.info("[bestseller] AU検索 '%s' page%d: %d件取得 累計%d件",
                        label, page_num, len(items), len(results))

            page_token = payload.get("nextToken")
            if not page_token:
                break

        except SellingApiException as e:
            logger.warning("[bestseller] AU検索エラー '%s' page%d: %s", label, page_num, e)
            break

        time.sleep(CATALOG_INTERVAL)

    return results


# ─────────────────────────────────────────────
# 3. AU セラー確認（競合価格取得）
# ─────────────────────────────────────────────

def get_au_sellers(asin: str) -> tuple:
    """
    AU マーケットプレイスの既存セラー数と最安値を返す。

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
        if any(kw in err_str for kw in ("InvalidParameterValue", "ItemNotApplicable", "NoOfferListings")):
            return 0, None
        logger.debug("[bestseller] AU seller check error %s: %s", asin, e)
        return 0, None


# ─────────────────────────────────────────────
# 4. JP 価格取得（同一ASIN が JP に存在するか）
# ─────────────────────────────────────────────

def get_jp_price(asin: str) -> tuple:
    """
    JP マーケットプレイスで同一 ASIN の価格と在庫を返す。

    AU ベストセラー ASIN の多くは国際版商品で JP にも存在する。
    JP に存在しない場合は (None, False) を返しスキップされる。

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
        logger.debug("[bestseller] JP price error %s: %s", asin, e)

    return None, False


# ─────────────────────────────────────────────
# 5. 出品制限チェック
# ─────────────────────────────────────────────

def check_listing_restriction(asin: str, seller_id: str) -> tuple:
    """
    Returns:
        (is_restricted: bool, reason_code: str)
    """
    try:
        api = ListingsRestrictions(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
        resp = api.get_listings_restrictions(
            asin=asin, sellerId=seller_id,
            marketplaceIds=[MARKETPLACE_AU], conditionType="new_new",
        )
        restrictions = (resp.payload or {}).get("restrictions", [])
        if not restrictions:
            return False, ""
        for r in restrictions:
            for reason in (r.get("reasons") or []):
                return True, reason.get("reasonCode", "RESTRICTED")
        return True, "RESTRICTED"
    except Exception:
        return False, ""


# ─────────────────────────────────────────────
# 6. Amazon AU 出品
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
            "merchant_suggested_asin": [{"value": asin, "marketplace_id": MARKETPLACE_AU}],
            "condition_type":          [{"value": "new_new", "marketplace_id": MARKETPLACE_AU}],
            "fulfillment_availability": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": 1,
                "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                "marketplace_id": MARKETPLACE_AU,
            }],
            "purchasable_offer": [{
                "currency": "AUD",
                "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                "marketplace_id": MARKETPLACE_AU,
            }],
        },
    }
    resp = api.put_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[MARKETPLACE_AU], body=put_body,
    )
    status = resp.payload.get("status", "")
    if status not in ("ACCEPTED", "VALID"):
        issues = resp.payload.get("issues", [])
        return False, "; ".join(i.get("message", "") for i in issues)

    time.sleep(10)

    patch_body = {
        "productType": "PRODUCT",
        "patches": [
            {"op": "replace", "path": "/attributes/fulfillment_availability",
             "value": [{"fulfillment_channel_code": "DEFAULT", "quantity": 1,
                        "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                        "marketplace_id": MARKETPLACE_AU}]},
            {"op": "replace", "path": "/attributes/purchasable_offer",
             "value": [{"currency": "AUD",
                        "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                        "marketplace_id": MARKETPLACE_AU}]},
        ],
    }
    try:
        api.patch_listings_item(
            sellerId=seller_id, sku=sku,
            marketplaceIds=[MARKETPLACE_AU], body=patch_body,
        )
    except Exception as e:
        logger.warning("[bestseller] PATCH失敗（PUTは成功） %s: %s", sku, e)

    return True, sku


# ─────────────────────────────────────────────
# 7. eBay USD 価格計算
# ─────────────────────────────────────────────

def calc_ebay_usd_price(jp_price_jpy: int, jpy_to_usd: float,
                         weight_kg: Optional[float] = None) -> float:
    """
    JP仕入値から eBay USD 出品価格を計算する。
    送料は重量考慮（1kg以上は DHLサーチャージ追加）。
    """
    if weight_kg is None or weight_kg < config.HEAVY_ITEM_THRESHOLD_KG:
        shipping_jpy = config.DHL_SHIPPING_JPY
    elif weight_kg <= config.DHL_MAX_WEIGHT_KG:
        shipping_jpy = config.DHL_SHIPPING_JPY + config.HEAVY_SHIPPING_SURCHARGE_JPY
    else:
        shipping_jpy = config.EMS_SHIPPING_JPY + config.HEAVY_SHIPPING_SURCHARGE_JPY

    required_revenue_jpy = jp_price_jpy * (1 + config.MIN_PROFIT_RATE / 100) + shipping_jpy
    net_usd = required_revenue_jpy * jpy_to_usd
    return round(net_usd / (1 - config.EBAY_FEE_RATE), 2)


# ─────────────────────────────────────────────
# 8. 商品画像取得（eBay 出品用）
# ─────────────────────────────────────────────

def get_product_image(asin: str) -> str:
    """JP Catalog Items API から商品メイン画像 URL を取得する"""
    try:
        api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP,
                           version="2022-04-01")
        resp = api.get_catalog_item(
            asin,
            marketplaceIds=[MARKETPLACE_JP],
            includedData=["images"],
        )
        images = (resp.payload or {}).get("images", [])
        for img_set in images:
            for img in img_set.get("images", []):
                if img.get("variant") == "MAIN":
                    url = img.get("link", "")
                    if url:
                        return url.replace("http://", "https://")
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────
# JPY→USD 為替レート取得
# ─────────────────────────────────────────────

def get_jpy_to_usd() -> float:
    try:
        r = _requests.get(
            "https://api.exchangerate-api.com/v4/latest/JPY", timeout=10
        )
        r.raise_for_status()
        rate = r.json().get("rates", {}).get("USD", config.JPY_TO_USD_FALLBACK)
        logger.info("[bestseller] JPY→USD: %.6f", rate)
        return float(rate)
    except Exception as e:
        logger.warning("[bestseller] USD為替取得失敗、フォールバック使用: %s", e)
        return config.JPY_TO_USD_FALLBACK


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="AU ベストセラー発掘 → JP仕入れ確認 → AU + eBay 同時出品"
    )
    parser.add_argument("--dry-run",         action="store_true", help="出品しない（確認のみ）")
    parser.add_argument("--max-new",         type=int, default=200,  help="新規出品上限（デフォルト200）")
    parser.add_argument("--rank-limit",      type=int, default=10000, help="AU ランキング上限（デフォルト10000）")
    parser.add_argument("--min-au-sellers",  type=int, default=2,    help="AU 既存セラー最低人数（デフォルト2）")
    parser.add_argument("--max-pages",       type=int, default=3,    help="検索あたり最大ページ数（デフォルト3）")
    parser.add_argument("--skip-ebay",       action="store_true",    help="eBay 出品をスキップ")
    parser.add_argument("--brands-only",     action="store_true",    help="ブランド検索のみ")
    parser.add_argument("--keywords-only",   action="store_true",    help="キーワード検索のみ")
    args = parser.parse_args()

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[bestseller] AMAZON_AU_SELLER_ID が未設定")
        sys.exit(1)

    if args.dry_run:
        logger.info("[bestseller] *** DRY-RUN モード ***")
    if args.skip_ebay:
        logger.info("[bestseller] eBay 出品スキップモード")

    # 為替レート取得（ループ外で1回だけ）
    exchange_rate = get_jpy_to_aud()
    jpy_to_usd    = get_jpy_to_usd()
    logger.info("[bestseller] 為替: 1 JPY = %.6f AUD / %.6f USD",
                exchange_rate, jpy_to_usd)

    # ── STEP 1: 既存出品ASIN取得 ──────────────────────────────────
    active_asins = get_active_asins()

    # ── STEP 2: AU カタログ検索 ──────────────────────────────────
    _init_candidates_db()
    checked_today = get_checked_today_asins()

    candidates: dict = {}   # asin → item dict（重複排除）

    # キーワード検索
    if not args.brands_only:
        for label, keywords in TARGET_AU_SEARCHES:
            logger.info("[bestseller] AU検索: %s [%s]", label, keywords)
            items = search_au_bestsellers(
                keywords=keywords,
                max_pages=args.max_pages,
                rank_limit=args.rank_limit,
            )
            new_count = 0
            for item in items:
                asin = item["asin"]
                if asin not in active_asins and asin not in candidates:
                    candidates[asin] = item
                    new_count += 1
            logger.info("[bestseller] %s: %d件取得 → %d件新規候補 (累計%d件)",
                        label, len(items), new_count, len(candidates))
            time.sleep(1.0)

    # ブランド検索
    if not args.keywords_only:
        for brand in TARGET_AU_BRANDS:
            logger.info("[bestseller] ブランド検索: %s", brand)
            items = search_au_bestsellers(
                keywords=brand,
                brand=brand,
                max_pages=args.max_pages,
                rank_limit=args.rank_limit,
            )
            new_count = 0
            for item in items:
                asin = item["asin"]
                if asin not in active_asins and asin not in candidates:
                    candidates[asin] = item
                    new_count += 1
            logger.info("[bestseller] ブランド %s: %d件取得 → %d件新規候補 (累計%d件)",
                        brand, len(items), new_count, len(candidates))
            time.sleep(1.0)

    logger.info("[bestseller] AU カタログ検索完了: 候補 %d件", len(candidates))

    if not candidates:
        logger.info("[bestseller] 候補なし。終了")
        return

    all_asins = list(candidates.keys())
    db_added = upsert_candidates(all_asins)
    logger.info("[bestseller] 候補DB: +%d件新規追加", db_added)

    # ── STEP 3: フィルタリング & 出品 ────────────────────────────
    listings_api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    listed = failed = skipped_auth = skipped_au = skipped_jp = \
        skipped_profit = skipped_restrict = 0
    ebay_listed = ebay_failed = 0
    listed_details = []

    for i, asin in enumerate(all_asins):
        if listed >= args.max_new:
            logger.info("[bestseller] 上限%d件到達。残り%d件スキップ",
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

        rank_str   = f"rank{rank:,}" if rank else "rank不明"
        log_prefix = f"[{i+1}/{len(all_asins)}] {asin} {rank_str}"

        # ── フィルター① 真贋チェック ──
        auth_ok, auth_reason = check_authenticity(title, brand, node)
        if not auth_ok:
            logger.info("[bestseller] 真贋NG %s: %s | %s", log_prefix, auth_reason, title[:50])
            if not args.dry_run:
                update_candidate(asin, title=title, status="ng", skip_reason=auth_reason)
            skipped_auth += 1
            continue

        # ── フィルター② AU セラー確認（競合価格取得）──
        time.sleep(AU_INTERVAL)
        au_sellers, au_min_price = get_au_sellers(asin)
        if not args.dry_run:
            update_candidate(asin, seller_count=au_sellers, au_price=au_min_price)

        if au_sellers < args.min_au_sellers:
            logger.info("[bestseller] AU セラー不足 %s: %d人 | %s",
                        log_prefix, au_sellers, title[:50])
            skipped_au += 1
            continue

        # ── フィルター③ JP 価格・在庫確認（同一ASIN が JP に存在するか）──
        time.sleep(JP_INTERVAL)
        jp_price, in_stock = get_jp_price(asin)
        if not args.dry_run:
            update_candidate(asin, title=title, weight_kg=weight, jp_price=jp_price)

        if not in_stock or not jp_price:
            logger.debug("[bestseller] JP在庫なし/価格なし %s | %s", asin, title[:40])
            skipped_jp += 1
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
            logger.info(
                "[bestseller] 利益不足 %s: AU$%.2f (推奨AU$%.2f) 粗利%.1f%% | %s",
                log_prefix, au_min_price or 0, recommended_price,
                profit.profit_rate, title[:40]
            )
            skipped_profit += 1
            continue

        # 出品価格（競合アンダーカット or 推奨価格の高い方）
        if au_min_price:
            listing_price_aud = max(
                round(au_min_price * (1 - config.BUYBOX_UNDERCUT_RATE), 2),
                recommended_price,
            )
        else:
            listing_price_aud = recommended_price

        # ── フィルター⑤ 出品制限チェック ──
        time.sleep(RESTRICTION_INTERVAL)
        is_restricted, restrict_code = check_listing_restriction(asin, seller_id)
        if is_restricted:
            if not args.dry_run:
                update_candidate(asin, status=STATUS_RESTRICTED, skip_reason=restrict_code)
            skipped_restrict += 1
            continue

        # ── ログメッセージ作成 ──
        log_msg = (
            f"{asin} | {title[:45]} | "
            f"JP¥{jp_price:,} → AU${listing_price_aud:.2f} "
            f"(粗利{profit.profit_rate:.1f}% AU競合{au_sellers}人) [{brand}] {rank_str}"
        )

        if args.dry_run:
            logger.info("[bestseller][DRY] 出品予定: %s", log_msg)
            listed_details.append({
                "asin": asin, "title": title, "brand": brand,
                "jp_price": jp_price, "price_aud": listing_price_aud,
                "au_sellers": au_sellers, "profit_rate": profit.profit_rate,
                "rank": rank, "ebay_id": None,
            })
            listed += 1
            continue

        # ── Amazon AU 出品 ──
        try:
            success, sku_or_err = list_new_item(
                listings_api, seller_id, asin, listing_price_aud
            )
            time.sleep(PATCH_INTERVAL)
        except Exception as e:
            logger.error("[bestseller] 出品例外 %s: %s", asin, e)
            failed += 1
            continue

        if not success:
            logger.warning("[bestseller] 出品失敗: %s | %s", asin, sku_or_err)
            failed += 1
            continue

        logger.info("[bestseller] AU出品完了: %s | SKU=%s", log_msg, sku_or_err)
        update_candidate(asin, status=STATUS_LISTED, listed_sku=sku_or_err,
                         jp_price=jp_price, au_price=listing_price_aud)
        listed += 1

        # ── eBay 同時出品 ──
        ebay_item_id = None
        if not args.skip_ebay and config.EBAY_USER_TOKEN:
            try:
                price_usd = calc_ebay_usd_price(jp_price, jpy_to_usd, weight)
                image_url = get_product_image(asin)
                time.sleep(CATALOG_INTERVAL)

                ebay_item_id = ebay_api.add_item(
                    title=title[:80],
                    price_usd=price_usd,
                    image_url=image_url,
                    handling_days=config.HANDLING_TIME_DAYS,
                )
                if ebay_item_id:
                    logger.info("[bestseller] eBay出品完了: %s | $%.2f | ItemID=%s",
                                asin, price_usd, ebay_item_id)
                    ebay_listed += 1
                else:
                    ebay_failed += 1
                time.sleep(EBAY_INTERVAL)
            except Exception as e:
                logger.warning("[bestseller] eBay出品失敗 %s: %s", asin, e)
                ebay_failed += 1

        listed_details.append({
            "asin": asin, "title": title, "brand": brand,
            "jp_price": jp_price, "price_aud": listing_price_aud,
            "au_sellers": au_sellers, "profit_rate": profit.profit_rate,
            "rank": rank, "ebay_id": ebay_item_id,
        })

    # ── サマリー & メール通知 ─────────────────────────────────────
    logger.info(
        "[bestseller] 完了: AU出品%d / eBay出品%d / JP在庫なし%d / "
        "AU不足%d / 真贋NG%d / 利益不足%d / 制限%d / 失敗%d",
        listed, ebay_listed, skipped_jp,
        skipped_au, skipped_auth, skipped_profit, skipped_restrict, failed,
    )

    dry_label = "[DRY-RUN] " if args.dry_run else ""
    subject = (
        f"[ベストセラー発掘] {dry_label}AU{listed}件 / eBay{ebay_listed}件 / "
        f"JP在庫なし{skipped_jp}件 / AU不足{skipped_au}件"
    )
    lines = [
        f"=== {dry_label}AU ベストセラー発掘結果 ===",
        "",
        f"AU カタログ候補:             {len(all_asins)}件",
        f"AU 新規出品{'予定' if args.dry_run else '完了'}:           {listed}件",
        f"eBay 同時出品{'予定' if args.dry_run else '完了'}:         {ebay_listed}件",
        f"JP在庫なし/価格なし(スキップ): {skipped_jp}件  ← JP対応なしASIN",
        f"AUセラー不足(スキップ):       {skipped_au}件  ← 版権品・地域制限品",
        f"真贋NG(スキップ):            {skipped_auth}件",
        f"利益不足(スキップ):          {skipped_profit}件",
        f"出品制限(スキップ):          {skipped_restrict}件",
        f"失敗:                       {failed}件",
        "",
        f"ランキング上限: AU rank {args.rank_limit:,}以内",
        f"MIN_PROFIT_RATE: {config.MIN_PROFIT_RATE}%",
        "",
    ]
    if listed_details:
        lines.append(f"--- 出品{'予定' if args.dry_run else '完了'} ({listed}件) ---")
        for d in listed_details[:60]:
            rank_label = f"rank{d['rank']:,}" if d.get("rank") else "rank不明"
            ebay_label = f" eBay:{d['ebay_id']}" if d.get("ebay_id") else ""
            lines.append(
                f"  {d['asin']}  [{d['brand']}]  "
                f"JP¥{d['jp_price']:,} → AU${d['price_aud']:.2f}  "
                f"粗利{d['profit_rate']:.1f}%  競合{d['au_sellers']}人  "
                f"{rank_label}{ebay_label}  {d['title'][:35]}"
            )
        if listed > 60:
            lines.append(f"  ... 他 {listed - 60}件")

    body = "\n".join(lines)
    send_email(subject=subject, body=body)
    try:
        print(body)
    except UnicodeEncodeError:
        print(body.encode("utf-8", errors="replace").decode("ascii", errors="replace"))


if __name__ == "__main__":
    main()

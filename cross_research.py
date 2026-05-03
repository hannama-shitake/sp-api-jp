"""
出品済み商品の横リサーチスクリプト。

既存AU出品（~500件）を起点に、同一ブランド・同一カテゴリの未出品商品を発掘する。

なぜ精度が高いか:
  - 出品済み = 「AU需要あり・利益あり・制限なし」が確認済みの実績データ
  - 同一ブランドの隣人 → 同じ仕入れ先・同じ需要層 → ハズレが少ない
  - 出品が増えるほど検索範囲が広がる（自己強化型）

フロー:
  1. AU 出品中の全ASIN を取得（Reports API）
  2. 各ASINのブランド・カテゴリを Catalog API で取得
  3. ブランド別・カテゴリ別の出現頻度をランキング
  4. 上位ブランド/カテゴリで searchCatalogItems → 未出品ASIN を収集
  5. catalog_api_discover.py と同じフィルター適用
     ① AU セラー確認（0人→スキップ。版権品・地域制限品の自動除外）
     ② 真贋ブラックリスト
     ③ JP 在庫・価格確認
     ④ 利益率チェック
     ⑤ ListingsRestrictions 出品制限チェック
  6. AU 出品 → arbitrage.db 記録 → メール通知

使い方:
  python cross_research.py                    # 実行（上限200件）
  python cross_research.py --dry-run          # テスト
  python cross_research.py --max-new 50       # 上限50件
  python cross_research.py --top-brands 30    # 上位30ブランドを検索
  python cross_research.py --top-nodes 20     # 上位20カテゴリを検索
  python cross_research.py --min-au-sellers 1 # AU セラー最低1人
"""
import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
from collections import Counter
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

CATALOG_INTERVAL     = 0.6
JP_INTERVAL          = 2.1
AU_INTERVAL          = 2.1
PATCH_INTERVAL       = 0.3
RESTRICTION_INTERVAL = 1.1


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
    "2127210", "344520", "3419372",
    "3256779", "3414009", "160778011",
    "50313",   "371850",  "2015683",
    "13529143", "2188184", "11248696",
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
    except Exception as e:
        logger.warning("[cross] NGワード辞書ロード失敗: %s", e)
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
    logger.info("[cross] AU出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[cross] レポートステータス: %s (%d/120)", status, attempt + 1)
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
    reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")), delimiter="\t")

    active = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        if asin and len(asin) == 10 and row.get("status", "").strip().lower() == "active":
            active.add(asin)

    logger.info("[cross] アクティブ出品: %d件", len(active))
    return active


# ─────────────────────────────────────────────
# 2. 各ASINのブランド・カテゴリを取得
# ─────────────────────────────────────────────

def get_asin_metadata(asins: list) -> list:
    """
    Catalog API で各ASINのブランド・カテゴリ（ブラウズノード）を取得する。

    Returns:
        [{"asin", "brand", "node_id", "node_name"}]
    """
    api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP,
                       version="2022-04-01")
    results = []
    total = len(asins)

    for i, asin in enumerate(asins):
        if i % 50 == 0:
            logger.info("[cross] メタデータ取得: %d/%d", i, total)
        try:
            resp = api.get_catalog_item(
                asin,
                marketplaceIds=[MARKETPLACE_JP],
                includedData=["summaries", "classifications"],
            )
            payload = resp.payload or {}

            # ブランド取得
            brand = ""
            for s in (payload.get("summaries") or []):
                if s.get("marketplaceId") == MARKETPLACE_JP:
                    brand = s.get("brand", "")
                    break
            if not brand:
                summaries = payload.get("summaries") or []
                if summaries:
                    brand = summaries[0].get("brand", "")

            # カテゴリ（ブラウズノード）取得
            node_id, node_name = "", ""
            for cl in (payload.get("classifications") or []):
                if cl.get("marketplaceId") == MARKETPLACE_JP:
                    for node in (cl.get("classifications") or []):
                        node_id   = node.get("classificationId", "")
                        node_name = node.get("displayName", "")
                        break
                    break

            if brand or node_id:
                results.append({
                    "asin":      asin,
                    "brand":     brand,
                    "node_id":   node_id,
                    "node_name": node_name,
                })

        except SellingApiException as e:
            logger.debug("[cross] メタデータ取得失敗 %s: %s", asin, e)
        time.sleep(CATALOG_INTERVAL)

    logger.info("[cross] メタデータ取得完了: %d/%d件", len(results), total)
    return results


# ─────────────────────────────────────────────
# 3. ブランド/カテゴリで隣人を検索
# ─────────────────────────────────────────────

def search_neighbors(
    keywords: str,
    brand: str = None,
    node_id: str = None,
    max_pages: int = 5,
) -> list:
    """
    既存出品と同一ブランド/カテゴリの商品を searchCatalogItems で取得する。

    Returns:
        [{"asin", "title", "brand", "node_id", "weight_kg"}]
    """
    api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP,
                       version="2022-04-01")
    results = []
    page_token = None

    kwargs = {
        "marketplaceIds": [MARKETPLACE_JP],
        "keywords":       keywords,
        "includedData":   ["summaries", "dimensions", "salesRanks"],
        "pageSize":       20,
    }
    if brand:
        kwargs["brandNames"] = [brand]
    if node_id:
        kwargs["classificationIds"] = [node_id]

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

                results.append({
                    "asin":    asin,
                    "title":   title,
                    "brand":   item_brand or brand or "",
                    "node_id": node_id or "",
                    "weight_kg": _extract_weight_kg(item.get("dimensions")),
                })

            page_token = payload.get("nextToken")
            if not page_token:
                break

        except SellingApiException as e:
            logger.debug("[cross] 検索エラー kw=%s: %s", keywords, e)
            break
        time.sleep(CATALOG_INTERVAL)

    return results


def _extract_weight_kg(dimensions_list) -> Optional[float]:
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


# ─────────────────────────────────────────────
# 4. 個別チェック（AU/JP/利益/制限）
# ─────────────────────────────────────────────

def get_au_sellers(asin: str) -> tuple:
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
        if any(kw in str(e) for kw in ("InvalidParameterValue", "ItemNotApplicable", "NoOfferListings")):
            return 0, None
        return 0, None


def get_jp_price(asin: str) -> tuple:
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    try:
        resp = api.get_competitive_pricing_for_asins([asin])
        items = resp.payload if isinstance(resp.payload, list) else []
        for item in items:
            if item.get("ASIN") != asin:
                continue
            comp_prices = (
                item.get("Product", {}).get("CompetitivePricing", {}).get("CompetitivePrices", [])
            )
            price_jpy = None
            for cp in comp_prices:
                if cp.get("condition") == "New":
                    amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                    if amount:
                        price_jpy = int(float(amount))
                    break
            offer_listings = (
                item.get("Product", {}).get("CompetitivePricing", {}).get("NumberOfOfferListings", [])
            )
            new_count = sum(
                ol.get("Count", 0) for ol in offer_listings
                if (ol.get("condition") or "").lower() in ("new", "new_new")
            )
            in_stock = new_count > 0
            if in_stock and not price_jpy:
                try:
                    r2 = api.get_item_offers(asin, item_condition="New")
                    offers = (r2.payload or {}).get("Offers", [])
                    prices = [int(float(o["ListingPrice"]["Amount"])) for o in offers
                              if o.get("ListingPrice", {}).get("Amount")]
                    if prices:
                        price_jpy = min(prices)
                except Exception:
                    pass
                time.sleep(JP_INTERVAL)
            return price_jpy, in_stock
    except SellingApiException as e:
        logger.debug("[cross] JP price error %s: %s", asin, e)
    return None, False


def check_listing_restriction(asin: str, seller_id: str) -> tuple:
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


def list_new_item(api, seller_id: str, asin: str, price_aud: float) -> tuple:
    sku = f"{config.SKU_PREFIX}{asin}"
    put_body = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [{"value": asin, "marketplace_id": MARKETPLACE_AU}],
            "condition_type": [{"value": "new_new", "marketplace_id": MARKETPLACE_AU}],
            "fulfillment_availability": [{
                "fulfillment_channel_code": "DEFAULT", "quantity": 1,
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
        sellerId=seller_id, sku=sku, marketplaceIds=[MARKETPLACE_AU], body=put_body,
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
        api.patch_listings_item(sellerId=seller_id, sku=sku,
                                marketplaceIds=[MARKETPLACE_AU], body=patch_body)
    except Exception as e:
        logger.warning("[cross] PATCH失敗（PUTは成功） %s: %s", sku, e)
    return True, sku


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="出品済み商品の横リサーチ → AU 新規出品")
    parser.add_argument("--dry-run",        action="store_true", help="出品しない（確認のみ）")
    parser.add_argument("--max-new",        type=int, default=200,  help="新規出品上限（デフォルト200）")
    parser.add_argument("--top-brands",     type=int, default=20,   help="上位ブランド数（デフォルト20）")
    parser.add_argument("--top-nodes",      type=int, default=10,   help="上位カテゴリ数（デフォルト10）")
    parser.add_argument("--max-pages",      type=int, default=5,    help="検索あたり最大ページ数（デフォルト5）")
    parser.add_argument("--min-au-sellers", type=int, default=1,    help="AU 既存セラー最低人数（デフォルト1）")
    parser.add_argument("--skip-metadata",  action="store_true",    help="メタデータ取得をスキップ（DB再利用）")
    args = parser.parse_args()

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[cross] AMAZON_AU_SELLER_ID が未設定")
        sys.exit(1)

    if args.dry_run:
        logger.info("[cross] *** DRY-RUN モード ***")

    exchange_rate = get_jpy_to_aud()
    logger.info("[cross] 為替: 1 JPY = %.6f AUD", exchange_rate)

    # ── STEP 1: 既存出品ASIN取得 ──────────────────────────────────
    active_asins = get_active_asins()
    if not active_asins:
        logger.info("[cross] 出品なし。終了")
        return

    # ── STEP 2: ブランド・カテゴリ取得 ────────────────────────────
    # キャッシュファイルで節約（--skip-metadata で再取得スキップ）
    meta_cache = os.path.join(os.path.dirname(__file__), "cross_research_meta.json")

    if args.skip_metadata and os.path.exists(meta_cache):
        with open(meta_cache, "r", encoding="utf-8") as f:
            metadata = json.load(f)
        logger.info("[cross] メタデータキャッシュ使用: %d件", len(metadata))
    else:
        metadata = get_asin_metadata(list(active_asins))
        with open(meta_cache, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        logger.info("[cross] メタデータをキャッシュ保存: %s", meta_cache)

    # ── STEP 3: 出現頻度ランキング ────────────────────────────────
    brand_counter = Counter()
    node_counter  = Counter()
    node_names    = {}

    for m in metadata:
        if m.get("brand"):
            brand_counter[m["brand"]] += 1
        if m.get("node_id"):
            node_counter[m["node_id"]] += 1
            if m.get("node_name"):
                node_names[m["node_id"]] = m["node_name"]

    top_brands = [b for b, _ in brand_counter.most_common(args.top_brands)]
    top_nodes  = [n for n, _ in node_counter.most_common(args.top_nodes)]

    logger.info("[cross] 上位ブランド (%d): %s", len(top_brands),
                ", ".join(f"{b}({brand_counter[b]}件)" for b in top_brands[:5]))
    logger.info("[cross] 上位カテゴリ (%d): %s", len(top_nodes),
                ", ".join(f"{node_names.get(n,n)}({node_counter[n]}件)" for n in top_nodes[:5]))

    # ── STEP 4: 隣人検索 ─────────────────────────────────────────
    _init_candidates_db()
    checked_today = get_checked_today_asins()

    candidates: dict = {}   # asin → item dict

    # ブランド別検索
    for brand in top_brands:
        auth_ok, _ = check_authenticity("", brand)
        if not auth_ok:
            logger.info("[cross] ブランドBLスキップ: %s", brand)
            continue
        logger.info("[cross] ブランド検索: %s (%d件出品中)", brand, brand_counter[brand])
        items = search_neighbors(keywords=brand, brand=brand, max_pages=args.max_pages)
        new_count = 0
        for item in items:
            asin = item["asin"]
            if asin not in active_asins and asin not in candidates:
                candidates[asin] = item
                new_count += 1
        logger.info("[cross] %s: %d件取得 → %d件新規候補", brand, len(items), new_count)
        time.sleep(1.0)

    # カテゴリ別検索
    for node_id in top_nodes:
        if _is_ng_browse_node(node_id):
            continue
        label = node_names.get(node_id, node_id)
        logger.info("[cross] カテゴリ検索: %s (%d件出品中)", label, node_counter[node_id])
        items = search_neighbors(
            keywords=label or node_id,
            node_id=node_id,
            max_pages=args.max_pages,
        )
        new_count = 0
        for item in items:
            asin = item["asin"]
            if asin not in active_asins and asin not in candidates:
                candidates[asin] = item
                new_count += 1
        logger.info("[cross] %s: %d件取得 → %d件新規候補", label, len(items), new_count)
        time.sleep(1.0)

    logger.info("[cross] 候補合計: %d件", len(candidates))

    if not candidates:
        logger.info("[cross] 候補なし。終了")
        return

    all_asins = list(candidates.keys())
    db_added = upsert_candidates(all_asins)
    logger.info("[cross] 候補DB: +%d件新規追加", db_added)

    # ── STEP 5: フィルタリング & 出品 ────────────────────────────
    listings_api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    listed = failed = skipped_auth = skipped_au = skipped_profit = skipped_restrict = 0
    listed_details = []

    for i, asin in enumerate(all_asins):
        if listed >= args.max_new:
            logger.info("[cross] 上限%d件到達。残り%d件スキップ", args.max_new, len(all_asins) - i)
            break
        if asin in checked_today:
            continue

        item   = candidates[asin]
        title  = item.get("title", "") or asin
        brand  = item.get("brand", "")
        node   = item.get("node_id", "")
        weight = item.get("weight_kg")

        log_prefix = f"[{i+1}/{len(all_asins)}] {asin}"

        # ① 真贋チェック
        auth_ok, auth_reason = check_authenticity(title, brand, node)
        if not auth_ok:
            update_candidate(asin, title=title, status="ng", skip_reason=auth_reason)
            skipped_auth += 1
            continue

        # ② AU セラー確認（版権・地域制限の核心）
        time.sleep(AU_INTERVAL)
        au_sellers, au_min_price = get_au_sellers(asin)
        update_candidate(asin, seller_count=au_sellers, au_price=au_min_price)

        if au_sellers < args.min_au_sellers:
            logger.info("[cross] AU セラー不足 %s: %d人 | %s",
                        log_prefix, au_sellers, title[:50])
            skipped_au += 1
            continue

        # ③ JP 価格・在庫
        time.sleep(JP_INTERVAL)
        jp_price, in_stock = get_jp_price(asin)
        update_candidate(asin, title=title, weight_kg=weight, jp_price=jp_price)
        if not in_stock or not jp_price:
            continue

        # ④ 利益確認
        profit = calc_profit(
            asin=asin, title=title, jp_price_jpy=jp_price,
            au_price_aud=au_min_price, exchange_rate=exchange_rate, weight_kg=weight,
        )
        recommended_price = calc_optimal_au_price(
            jp_price_jpy=jp_price, exchange_rate=exchange_rate, weight_kg=weight,
        )
        if not profit.is_profitable:
            logger.info("[cross] 利益不足 %s: 粗利%.1f%% (推奨AU$%.2f) | %s",
                        log_prefix, profit.profit_rate, recommended_price, title[:40])
            skipped_profit += 1
            continue

        # ⑤ 出品制限チェック
        time.sleep(RESTRICTION_INTERVAL)
        is_restricted, restrict_code = check_listing_restriction(asin, seller_id)
        if is_restricted:
            update_candidate(asin, status=STATUS_RESTRICTED, skip_reason=restrict_code)
            skipped_restrict += 1
            continue

        # 出品価格（競合アンダーカット or 推奨価格の高い方）
        if au_min_price:
            listing_price = max(
                round(au_min_price * (1 - config.BUYBOX_UNDERCUT_RATE), 2),
                recommended_price,
            )
        else:
            listing_price = recommended_price

        log_msg = (
            f"{asin} | {title[:45]} | "
            f"JP¥{jp_price:,} → AU${listing_price:.2f} "
            f"(粗利{profit.profit_rate:.1f}% AU競合{au_sellers}人) [{brand}]"
        )

        if args.dry_run:
            logger.info("[cross][DRY] 出品予定: %s", log_msg)
            listed_details.append({
                "asin": asin, "title": title, "brand": brand,
                "jp_price": jp_price, "price_aud": listing_price,
                "au_sellers": au_sellers, "profit_rate": profit.profit_rate,
            })
            listed += 1
            continue

        try:
            success, sku_or_err = list_new_item(listings_api, seller_id, asin, listing_price)
            time.sleep(PATCH_INTERVAL)
            if success:
                logger.info("[cross] 出品完了: %s | SKU=%s", log_msg, sku_or_err)
                update_candidate(asin, status=STATUS_LISTED, listed_sku=sku_or_err,
                                 jp_price=jp_price, au_price=listing_price)
                listed_details.append({
                    "asin": asin, "title": title, "brand": brand,
                    "jp_price": jp_price, "price_aud": listing_price,
                    "au_sellers": au_sellers, "profit_rate": profit.profit_rate,
                })
                listed += 1
            else:
                logger.warning("[cross] 出品失敗: %s | %s", asin, sku_or_err)
                failed += 1
        except Exception as e:
            logger.error("[cross] 出品例外 %s: %s", asin, e)
            failed += 1

    # ── サマリー ─────────────────────────────────────────────────
    logger.info(
        "[cross] 完了: 出品%d / AU不足%d / 真贋NG%d / 利益不足%d / 制限%d / 失敗%d",
        listed, skipped_au, skipped_auth, skipped_profit, skipped_restrict, failed,
    )

    dry_label = "[DRY-RUN] " if args.dry_run else ""
    subject = (
        f"[横リサーチ] {dry_label}新規{listed}件 / "
        f"AU不足{skipped_au}件 / 真贋NG{skipped_auth}件"
    )
    lines = [
        f"=== {dry_label}出品済み横リサーチ結果 ===",
        "",
        f"既存出品ベース:       {len(active_asins)}件",
        f"検索ブランド数:       {len(top_brands)}件",
        f"検索カテゴリ数:       {len(top_nodes)}件",
        f"候補総数:             {len(all_asins)}件",
        f"新規出品{'予定' if args.dry_run else '完了'}:         {listed}件",
        f"AUセラー不足(スキップ): {skipped_au}件  ← 版権品・地域制限品",
        f"真贋NG(スキップ):      {skipped_auth}件",
        f"利益不足(スキップ):    {skipped_profit}件",
        f"出品制限(スキップ):    {skipped_restrict}件",
        f"失敗:                {failed}件",
        "",
        f"上位ブランド: {', '.join(f'{b}({brand_counter[b]})' for b in top_brands[:8])}",
        "",
    ]
    if listed_details:
        lines.append(f"--- 出品{'予定' if args.dry_run else '完了'} ({listed}件) ---")
        for d in listed_details[:50]:
            lines.append(
                f"  {d['asin']}  [{d['brand']}]  "
                f"JP¥{d['jp_price']:,} → AU${d['price_aud']:.2f}  "
                f"粗利{d['profit_rate']:.1f}%  競合{d['au_sellers']}人  {d['title'][:40]}"
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

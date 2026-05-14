"""
ebay_jp_discover.py — JP カタログ直結 eBay 大量出品（第5の柱）

Amazon AU の制約（AU既存ページ必須・出品制限）を完全バイパス。
JP に在庫があり利益が出る商品をそのまま eBay に直接出品する。

既存スクリプトとの違い:
  ebay_lister.py      → Amazon AU 出品済みリストを eBay へ
  ebay_jp_discover.py → JP カタログを直接 eBay へ（AU 不問）

なぜ件数が多いか:
  - AU セラー確認不要 = 天井がない
  - JP は 1 億品以上 → eBay に出せる候補が桁違い
  - バルク価格確認（20件/回）で高速処理

フロー:
  1. JP カタログ検索（50+ カテゴリ × 3〜5 ページ）
  2. 候補を DB に記録（重複排除）
  3. JP 価格一括確認（20件バッチ）
  4. 真贋フィルタ
  5. 利益計算（JPY→USD、eBay 手数料込み）
  6. eBay 出品（custom_label=ASIN で重複管理）
  7. メール通知

使い方:
  python ebay_jp_discover.py               # 実行（上限500件）
  python ebay_jp_discover.py --dry-run     # テスト
  python ebay_jp_discover.py --max-new 100 # 上限100件
  python ebay_jp_discover.py --rank-limit 30000  # ランク上位3万以内
  python ebay_jp_discover.py --max-pages 5       # 検索あたり最大5ページ
"""
import argparse
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
from sp_api.api import CatalogItems, Products
from sp_api.base import Marketplaces, SellingApiException

import config
from apis import ebay_api
from utils.candidates_db import (
    init_db as _init_candidates_db,
    upsert_candidates,
    update_candidate,
    get_checked_today_asins,
)
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

MARKETPLACE_JP = config.MARKETPLACE_JP

_JP_CREDS = {
    "refresh_token":     config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id":        config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

CATALOG_INTERVAL = 0.6   # searchCatalogItems: 2 req/s
JP_BULK_INTERVAL = 2.1   # get_competitive_pricing_for_asins: 0.5 req/s
IMG_INTERVAL     = 0.4   # get_catalog_item images
EBAY_INTERVAL    = 0.5   # AddItem


# ─────────────────────────────────────────────
# JP カタログ検索ターゲット（50+ 検索）
# "ラベル", "キーワード", "ブラウズノードID（任意）"
# ─────────────────────────────────────────────
TARGET_JP_SEARCHES = [
    # ── フィギュア（最高売上カテゴリ）──────────────────────────────
    ("SH Figuarts",       "SH Figuarts bandai",           "13793682051"),
    ("ねんどろいど",       "nendoroid good smile company",  "13793682051"),
    ("figma",             "figma max factory",             "13793682051"),
    ("MAFEX",             "MAFEX medicom toy figure",      "13793682051"),
    ("POP UP PARADE",     "pop up parade figure",          "13793682051"),
    ("ALTER フィギュア",  "ALTER figure anime",            "13793682051"),
    ("フィギュア全般",    "anime figure collectible",      "13793682051"),
    ("アクションフィギュア", "action figure robot anime",  "2277719051"),
    # ── プラモデル・ラジコン ─────────────────────────────────────
    ("ガンプラ",          "ガンプラ gunpla bandai",         "13793683051"),
    ("タミヤ模型",        "tamiya scale model kit",        "13793683051"),
    ("ハセガワ",          "hasegawa aircraft model kit",   "13793683051"),
    ("ミニカー",          "diecast minicar tomica",        "13793686051"),
    ("ラジコン",          "rc car radio control tamiya",   "13793684051"),
    # ── トレーディングカード ────────────────────────────────────
    ("ポケモンカード",    "pokemon card japanese booster",  "2277721051"),
    ("遊戯王カード",      "yugioh card japanese",           "2277721051"),
    ("ワンピースカード",  "one piece card game bandai",     "2277721051"),
    ("デュエルマスターズ","duel masters card game",         "2277721051"),
    # ── 釣り具（Shimano/Daiwa 実績あり）─────────────────────────
    ("シマノリール",      "shimano fishing reel spinning",  "2028971051"),
    ("ダイワリール",      "daiwa fishing reel",             "2028971051"),
    ("シマノロッド",      "shimano fishing rod",            "2028961051"),
    ("ダイワロッド",      "daiwa fishing rod",              "2028961051"),
    ("ルアー",            "fishing lure japan jig",         "2028981051"),
    ("釣り用品",          "fishing tackle accessories",     "2028951051"),
    ("ABU リール",        "abu garcia reel",                "2028971051"),
    # ── カメラ・光学機器 ────────────────────────────────────────
    ("デジタルカメラ",    "digital camera compact japan",   "2127213051"),
    ("カメラレンズ",      "camera lens japan",              "2127222051"),
    ("双眼鏡",            "binoculars optics japan",        "2127225051"),
    ("フィルムカメラ",    "film camera 35mm vintage",       "2127213051"),
    ("カメラアクセサリ",  "camera accessory filter strap",  "2127222051"),
    # ── 音響機器 ────────────────────────────────────────────────
    ("ヘッドフォン",      "headphone audiophile japan",     "2016929051"),
    ("インイヤー",        "in ear monitor iem earphone",    "2016929051"),
    ("スピーカー",        "speaker hifi portable japan",    "2016930051"),
    ("ターンテーブル",    "turntable record player audio",  "2016929051"),
    ("ポータブルアンプ",  "portable amplifier dac japan",   "2016929051"),
    # ── ゲーム・周辺機器 ────────────────────────────────────────
    ("ゲーミングパッド",  "gaming mouse pad artisan",       ""),
    ("ゲームコントローラ","game controller gamepad switch",  ""),
    ("ゲームカード",      "nintendo switch game japanese",  ""),
    ("メモリカード",      "memory card sd flash storage",   ""),
    # ── サイクリング ────────────────────────────────────────────
    ("シマノコンポ",      "shimano groupset derailleur",    "2028641051"),
    ("自転車パーツ",      "bicycle component brake pedal",  "2028641051"),
    ("サイクリングウェア","cycling jersey clothing japan",  "2028641051"),
    # ── アウトドア・キャンプ ─────────────────────────────────────
    ("モンベル",          "mont-bell outdoor japan",        "14696781"),
    ("スノーピーク",      "snow peak camping gear",         "14696781"),
    ("アウトドアクッカー","camping cookware japan titanium", "14696781"),
    # ── 楽器・エフェクター（Amazon JP直送向け：日本価格優位が大きい）─────
    ("エレキギター",      "electric guitar japan",                   "562038"),
    ("ベースギター",      "bass guitar japan",                       "562038"),
    ("電子ピアノ",        "digital piano keyboard japan",            "562038"),
    ("ハーモニカ",        "harmonica suzuki hohner",                 "562038"),
    ("管楽器",            "wind instrument brass japan",             "562038"),
    # エフェクター・パワーサプライ（Fenderモデル: JP20%引、国際送料¥3,000台）
    ("エフェクターペダル",  "guitar effects pedal boss mxr",         "562038"),
    ("パワーサプライ",      "guitar pedal power supply fender korg", "562038"),
    ("マルチエフェクター",  "multi effects processor boss zoom",      "562038"),
    ("ギターアンプ",        "guitar amplifier fender marshall vox",  "562038"),
    ("チューナーペダル",    "guitar tuner pedal boss tc electronic", "562038"),
    ("ギターケーブル",      "mogami canare guitar cable instrument",  "562038"),
    ("オーディオIF",        "audio interface usb focusrite scarlett","562038"),
    ("MIDIキーボード",      "midi keyboard controller akai arturia", "562038"),
    ("レコーダー",          "portable recorder zoom tascam field",   "562038"),
    ("ワイヤレスシステム",  "wireless guitar system boss line6",     "562038"),
    # ── 文房具・画材 ────────────────────────────────────────────
    ("万年筆",            "fountain pen pilot namiki japan",""),
    ("ボールペン",        "ballpoint pen japanese luxury",  ""),
    ("絵の具",            "watercolor paint japan holbein", ""),
    # ── 工具 ────────────────────────────────────────────────────
    ("ニッパー・ペンチ",  "nipper pliers knipex vessel",    ""),
    ("精密ドライバー",    "precision screwdriver set japan",""),
    ("ノギス・計測器",    "digital calipers measuring tool",""),
    # ── スポーツ ────────────────────────────────────────────────
    ("卓球用品",          "table tennis paddle butterfly",  "2028991051"),
    ("弓道・アーチェリー","archery japanese bow arrow",     "2028991051"),
    # ── 腕時計（JP価格優位・AU/欧米需要高）──────────────────────
    ("セイコープレサージ",   "seiko presage japan watch",           ""),
    ("セイコープロスペックス","seiko prospex turtle srpc turtle",   ""),
    ("シチズンプロマスター", "citizen promaster eco drive japan",   ""),
    ("G-SHOCK",             "casio g-shock gw japan",              ""),
    ("セイコー5スポーツ",   "seiko 5 sports automatic japan",      ""),
    # ── 美容・健康グッズ（雑貨扱い・規制リスク低）───────────────
    ("コンニャクスポンジ",  "konjac sponge facial japan",          ""),
    ("美顔ローラー",        "face roller jade gua sha japan",      ""),
    ("YA-MAN美顔器",        "ya-man rf facial device japan",       ""),
    ("爪切り匠の技",        "japanese nail clipper takumi no waza",""),
    ("耳かき",              "japanese ear pick ear cleaner",       ""),
    ("パナソニック美容家電", "panasonic beauty hair care japan",   ""),
    # ── 包丁・キッチン（JP生産・高品質）─────────────────────────
    ("Macナイフ",           "mac knife chef professional japan",   ""),
    ("グローバル包丁",      "global knife japanese chef stainless",""),
    ("ツヴィリング",        "zwilling j.a. henckels knife japan",  ""),
    ("貝印関孫六",          "kai seki magoroku knife japan",       ""),
    # ── バイク用品（JP限定モデル・AU高需要）─────────────────────
    ("アライヘルメット",    "arai helmet motorcycle japan rx7",    ""),
    ("SHOEIヘルメット",     "shoei helmet motorcycle japan nxr",   ""),
    ("コミネ バイクウェア", "komine motorcycle jacket japan",      ""),
]

# ブランド検索（上記カテゴリに加えてブランド別にも検索）
TARGET_JP_BRANDS = [
    # フィギュア
    "Bandai Spirits", "Good Smile Company", "Max Factory",
    "Medicom Toy", "Kotobukiya", "ALTER", "FREEing",
    # プラモデル
    "Tamiya", "Hasegawa", "Fujimi", "Aoshima",
    # 釣り
    "Shimano", "Daiwa", "Abu Garcia", "Major Craft",
    # カメラ
    "Nikon", "Fujifilm", "Olympus", "Ricoh",
    # オーディオ
    "Audio-Technica", "Sennheiser", "Beyerdynamic", "Fostex",
    # サイクリング
    "Shimano",  # (重複OK、別途検索)
    # アウトドア
    "Snow Peak", "mont-bell",
    # 楽器・エフェクター（Amazon JP直送モデル向け）
    "Roland", "Yamaha", "Korg", "Boss", "Fender", "Ibanez",
    "Line 6", "TC Electronic", "Electro-Harmonix", "MXR",
    "Strymon", "Zoom", "Tascam", "Focusrite", "Mooer",
    # 文房具
    "Pilot", "Uni Mitsubishi",
    # 腕時計（新規追加）
    "Seiko", "Citizen", "Casio",
    # 美容家電（新規追加）
    "YA-MAN", "Panasonic",
    # 包丁（新規追加）
    "Mac Knife", "Global", "Zwilling",
    # バイク（新規追加）
    "Arai", "Shoei",
]


# ─────────────────────────────────────────────
# 真贋ブラックリスト
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
        logger.info("[ebay_jp] NGワード辞書: %d件", len(words))
    except Exception as e:
        logger.warning("[ebay_jp] NGワード辞書ロード失敗: %s", e)
    return _NG_WORDS_CACHE


def check_authenticity(title: str, brand: str) -> tuple:
    brand_lower = (brand or "").lower()
    for bl in _BRAND_BLACKLIST:
        if bl in brand_lower:
            return False, f"ブランドBL: {brand}"
    title_lower = (title or "").lower()
    for kw in _TITLE_NG_KEYWORDS:
        if kw in title_lower:
            return False, f"タイトルBL: {kw}"
    for word in _load_ng_words():
        if word in title_lower:
            return False, f"NGワード: {word}"
    return True, ""


# ─────────────────────────────────────────────
# 1. JP カタログ検索
# ─────────────────────────────────────────────

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


def search_jp_catalog(
    keywords: str,
    browse_node_id: str = None,
    brand: str = None,
    max_pages: int = 3,
    rank_limit: int = 30000,
) -> list:
    """
    JP カタログを searchCatalogItems で検索。

    Returns:
        [{"asin", "rank", "title", "brand", "weight_kg"}]
    """
    api = CatalogItems(credentials=_JP_CREDS, marketplace=Marketplaces.JP,
                       version="2022-04-01")
    results = []
    page_token = None
    label = brand or browse_node_id or keywords

    kwargs = {
        "marketplaceIds": [MARKETPLACE_JP],
        "keywords":       keywords,
        "includedData":   ["salesRanks", "summaries", "dimensions"],
        "pageSize":       20,
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

                rank = None
                for rank_data in (item.get("salesRanks") or []):
                    if rank_data.get("marketplaceId") != MARKETPLACE_JP:
                        continue
                    for cr in (rank_data.get("classificationRanks") or []):
                        r = cr.get("rank")
                        if r is not None and (rank is None or r < rank):
                            rank = r

                if rank is not None and rank_limit and rank > rank_limit:
                    continue

                results.append({
                    "asin":      asin,
                    "rank":      rank,
                    "title":     title,
                    "brand":     item_brand or brand or "",
                    "weight_kg": _extract_weight_kg(item.get("dimensions")),
                })

            logger.info("[ebay_jp] JP検索 '%s' page%d: %d件 累計%d件",
                        label, page_num, len(items), len(results))
            page_token = payload.get("nextToken")
            if not page_token:
                break

        except SellingApiException as e:
            logger.warning("[ebay_jp] JP検索エラー '%s': %s", label, e)
            break

        time.sleep(CATALOG_INTERVAL)

    return results


# ─────────────────────────────────────────────
# 2. JP 価格一括確認（20件バッチ）
# ─────────────────────────────────────────────

def get_jp_prices_bulk(asins: list) -> dict:
    """
    JP 価格を 20件バッチで一括取得する（個別確認より約20倍速い）。

    Returns:
        {asin: (price_jpy: int, in_stock: bool)}
    """
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    result = {}
    total = len(asins)

    for i in range(0, total, 20):
        batch = asins[i: i + 20]
        if i % 200 == 0:
            logger.info("[ebay_jp] JP価格一括確認: %d/%d", i, total)
        try:
            resp = api.get_competitive_pricing_for_asins(batch)
            items = resp.payload if isinstance(resp.payload, list) else []
            for item in items:
                asin = item.get("ASIN", "")
                comp_prices = (
                    item.get("Product", {})
                        .get("CompetitivePricing", {})
                        .get("CompetitivePrices", [])
                )
                price_jpy = None
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amt = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amt:
                            price_jpy = int(float(amt))
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
                if price_jpy:
                    result[asin] = (price_jpy, in_stock)
                elif in_stock:
                    result[asin] = (None, True)   # 在庫あり・価格未取得
                # else: JP に存在しない → result に入れない

        except SellingApiException as e:
            logger.warning("[ebay_jp] JP価格バッチエラー: %s", e)

        time.sleep(JP_BULK_INTERVAL)

    in_stock_count = sum(1 for v in result.values() if v[1])
    with_price_count = sum(1 for v in result.values() if v[0])
    logger.info("[ebay_jp] JP価格確認完了: %d件中 在庫あり%d件 / 価格取得%d件",
                total, in_stock_count, with_price_count)
    return result


# ─────────────────────────────────────────────
# 3. eBay USD 価格計算
# ─────────────────────────────────────────────

def calc_ebay_usd_price(jp_price_jpy: int, jpy_to_usd: float,
                         weight_kg: Optional[float] = None) -> float:
    """
    JP仕入値から eBay USD 出品価格を計算する（重量考慮）。
    利益率 = MIN_PROFIT_RATE、手数料 = EBAY_FEE_RATE
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
# 4. 商品画像取得
# ─────────────────────────────────────────────

def get_product_image(asin: str) -> str:
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
# JPY→USD 為替レート
# ─────────────────────────────────────────────

def get_jpy_to_usd() -> float:
    try:
        r = _requests.get(
            "https://api.exchangerate-api.com/v4/latest/JPY", timeout=10
        )
        r.raise_for_status()
        rate = r.json().get("rates", {}).get("USD", config.JPY_TO_USD_FALLBACK)
        logger.info("[ebay_jp] JPY→USD: %.6f", rate)
        return float(rate)
    except Exception as e:
        logger.warning("[ebay_jp] USD為替取得失敗: %s", e)
        return config.JPY_TO_USD_FALLBACK


# ─────────────────────────────────────────────
# メイン処理
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="JP カタログ直結 eBay 大量出品"
    )
    parser.add_argument("--dry-run",       action="store_true", help="出品しない（確認のみ）")
    parser.add_argument("--max-new",       type=int, default=500,   help="新規出品上限（デフォルト500）")
    parser.add_argument("--rank-limit",    type=int, default=30000, help="JP ランキング上限（デフォルト30000）")
    parser.add_argument("--max-pages",     type=int, default=3,     help="検索あたり最大ページ数（デフォルト3）")
    parser.add_argument("--brands-only",   action="store_true",     help="ブランド検索のみ")
    parser.add_argument("--keywords-only", action="store_true",     help="キーワード検索のみ")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("[ebay_jp] *** DRY-RUN モード ***")

    if not config.EBAY_USER_TOKEN and not args.dry_run:
        logger.error("[ebay_jp] EBAY_USER_TOKEN が未設定")
        sys.exit(1)

    jpy_to_usd = get_jpy_to_usd()

    # ── STEP 1: eBay 既存出品（ASIN重複チェック用）──────────────────
    logger.info("[ebay_jp] eBay 既存出品確認中...")
    if not args.dry_run:
        ebay_listings = ebay_api.get_active_listings()
        ebay_asin_set = {
            v["custom_label"]
            for v in ebay_listings.values()
            if v.get("custom_label")
        }
    else:
        ebay_asin_set = set()
    logger.info("[ebay_jp] eBay 既存出品: %d件", len(ebay_asin_set))

    # ── STEP 2: JP カタログ検索 ─────────────────────────────────────
    _init_candidates_db()
    checked_today = get_checked_today_asins()

    candidates: dict = {}   # asin → item dict

    if not args.brands_only:
        for label, keywords, node_id in TARGET_JP_SEARCHES:
            logger.info("[ebay_jp] JP検索: %s", label)
            items = search_jp_catalog(
                keywords=keywords,
                browse_node_id=node_id or None,
                max_pages=args.max_pages,
                rank_limit=args.rank_limit,
            )
            new_count = 0
            for item in items:
                asin = item["asin"]
                if asin not in ebay_asin_set and asin not in candidates:
                    candidates[asin] = item
                    new_count += 1
            logger.info("[ebay_jp] %s: %d件取得 → %d件新規候補 (累計%d件)",
                        label, len(items), new_count, len(candidates))
            time.sleep(0.8)

    if not args.keywords_only:
        for brand in TARGET_JP_BRANDS:
            logger.info("[ebay_jp] ブランド検索: %s", brand)
            items = search_jp_catalog(
                keywords=brand,
                brand=brand,
                max_pages=args.max_pages,
                rank_limit=args.rank_limit,
            )
            new_count = 0
            for item in items:
                asin = item["asin"]
                if asin not in ebay_asin_set and asin not in candidates:
                    candidates[asin] = item
                    new_count += 1
            logger.info("[ebay_jp] ブランド %s: %d件取得 → %d件新規候補 (累計%d件)",
                        brand, len(items), new_count, len(candidates))
            time.sleep(0.8)

    logger.info("[ebay_jp] JP カタログ検索完了: 候補 %d件", len(candidates))

    if not candidates:
        logger.info("[ebay_jp] 候補なし。終了")
        return

    # 候補 DB に記録
    all_asins = [
        a for a in candidates.keys()
        if a not in checked_today
    ]
    db_added = upsert_candidates(all_asins)
    logger.info("[ebay_jp] 候補DB: +%d件新規追加（検索済みスキップ後 %d件対象）",
                db_added, len(all_asins))

    # ── STEP 3: JP 価格一括確認（高速バッチ）──────────────────────
    logger.info("[ebay_jp] JP価格一括確認: %d件", len(all_asins))
    jp_prices = get_jp_prices_bulk(all_asins)

    # 在庫あり・価格あり・利益計算通過 の候補に絞る
    profitable: list = []
    skipped_no_jp = skipped_profit = skipped_auth = 0

    for asin in all_asins:
        item = candidates[asin]
        title    = item.get("title", "") or asin
        brand    = item.get("brand", "")
        weight   = item.get("weight_kg")

        jp_data = jp_prices.get(asin)
        if not jp_data or not jp_data[1] or not jp_data[0]:
            skipped_no_jp += 1
            continue

        jp_price = jp_data[0]
        price_usd = calc_ebay_usd_price(jp_price, jpy_to_usd, weight)

        # 最低価格チェック（$5以下は出品しない）
        if price_usd < 5.0:
            skipped_profit += 1
            continue

        # 真贋フィルタ
        auth_ok, auth_reason = check_authenticity(title, brand)
        if not auth_ok:
            if not args.dry_run:
                update_candidate(asin, title=title, status="ng", skip_reason=auth_reason)
            skipped_auth += 1
            continue

        profitable.append({
            "asin":      asin,
            "title":     title,
            "brand":     brand,
            "rank":      item.get("rank"),
            "weight_kg": weight,
            "jp_price":  jp_price,
            "price_usd": price_usd,
        })

    logger.info(
        "[ebay_jp] フィルタ後: %d件 → 出品対象 (JP無し%d / 利益不足%d / 真贋NG%d)",
        len(profitable), skipped_no_jp, skipped_profit, skipped_auth,
    )

    # ── STEP 4: eBay 出品 ──────────────────────────────────────────
    listed = failed = 0
    listed_details = []

    for i, p in enumerate(profitable):
        if listed >= args.max_new:
            logger.info("[ebay_jp] 上限%d件到達。残り%d件スキップ",
                        args.max_new, len(profitable) - i)
            break

        asin      = p["asin"]
        title     = p["title"]
        brand     = p["brand"]
        jp_price  = p["jp_price"]
        price_usd = p["price_usd"]
        rank      = p["rank"]
        weight    = p["weight_kg"]
        rank_str  = f"rank{rank:,}" if rank else "rank不明"

        log_msg = (
            f"[{i+1}/{len(profitable)}] {asin} | "
            f"JP¥{jp_price:,} → ${price_usd:.2f} | "
            f"{rank_str} | {title[:40]}"
        )

        if args.dry_run:
            logger.info("[ebay_jp][DRY] 出品予定: %s", log_msg)
            listed_details.append(p)
            listed += 1
            continue

        # 画像取得（失敗してもスキップしない）
        image_url = get_product_image(asin)
        if image_url:
            time.sleep(IMG_INTERVAL)

        # eBay 出品
        item_id = ebay_api.add_item(
            title=title[:80],
            price_usd=price_usd,
            image_url=image_url,
            handling_days=config.HANDLING_TIME_DAYS,
            custom_label=asin,          # ASIN をカスタムラベルに設定
        )

        if item_id:
            logger.info("[ebay_jp] 出品完了: %s | ItemID=%s", log_msg, item_id)
            update_candidate(asin, title=title, weight_kg=weight, jp_price=jp_price)
            p["item_id"] = item_id
            listed_details.append(p)
            listed += 1
        else:
            logger.warning("[ebay_jp] 出品失敗: %s", asin)
            failed += 1

        time.sleep(EBAY_INTERVAL)

    # ── サマリー & メール ────────────────────────────────────────
    logger.info(
        "[ebay_jp] 完了: 出品%d / 失敗%d / JP無し%d / 利益不足%d / 真贋NG%d",
        listed, failed, skipped_no_jp, skipped_profit, skipped_auth,
    )

    dry_label = "[DRY-RUN] " if args.dry_run else ""
    subject = (
        f"[eBay JP直結] {dry_label}新規{listed}件 / "
        f"JP無し{skipped_no_jp}件 / 真贋NG{skipped_auth}件"
    )
    lines = [
        f"=== {dry_label}eBay JP 直結出品結果 ===",
        "",
        f"JP カタログ候補:         {len(candidates)}件",
        f"本日対象（DB重複除外後）: {len(all_asins)}件",
        f"JP在庫あり・価格あり:    {len(profitable) + skipped_profit + skipped_auth}件",
        f"出品{'予定' if args.dry_run else '完了'}:             {listed}件",
        f"失敗:                   {failed}件",
        f"JP在庫なし(スキップ):   {skipped_no_jp}件",
        f"利益不足(スキップ):     {skipped_profit}件",
        f"真贋NG(スキップ):       {skipped_auth}件",
        "",
        f"JPY→USD: {jpy_to_usd:.6f}",
        f"MIN_PROFIT_RATE: {config.MIN_PROFIT_RATE}%",
        f"ランク上限: JP rank {args.rank_limit:,}以内",
        "",
    ]
    if listed_details:
        lines.append(f"--- 出品{'予定' if args.dry_run else '完了'} ({listed}件) ---")
        for d in listed_details[:80]:
            rank_label = f"rank{d['rank']:,}" if d.get("rank") else "rank不明"
            item_label = f" ItemID:{d.get('item_id','')}" if d.get("item_id") else ""
            lines.append(
                f"  {d['asin']}  [{d['brand']}]  "
                f"JP¥{d['jp_price']:,} → ${d['price_usd']:.2f}  "
                f"{rank_label}{item_label}  {d['title'][:40]}"
            )
        if listed > 80:
            lines.append(f"  ... 他 {listed - 80}件")

    body = "\n".join(lines)
    send_email(subject=subject, body=body)
    try:
        print(body)
    except UnicodeEncodeError:
        print(body.encode("utf-8", errors="replace").decode("ascii", errors="replace"))


if __name__ == "__main__":
    main()

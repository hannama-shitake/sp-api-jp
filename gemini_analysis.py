"""
出品中の全商品データをGeminiに投げて分析レポートを生成するスクリプト。
週1回GitHub Actionsで実行し、結果をメールで送信する。
"""
import csv
import gzip
import io
import sys
import time
import json

import requests as _requests
import google.generativeai as genai
from sp_api.api import Reports, Products
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_profit, calc_optimal_au_price
from utils.notify import send_email
from utils.logger import get_logger

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP_CREDS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

_JP_INTERVAL = 2.1


def get_my_au_listings() -> list:
    """Reports APIで自分の出品一覧を取得"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]
    logger.info("[gemini] レポートID: %s", report_id)

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[gemini] レポートステータス: %s", status)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")

    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")

    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    listings = []
    seen = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        price_str = row.get("price", "").strip()
        title = row.get("item-name", "").strip()
        status = row.get("status", "").strip().lower()
        if asin and len(asin) == 10 and sku and asin not in seen and status != "deleted":
            seen.add(asin)
            try:
                au_price = float(price_str) if price_str else None
            except ValueError:
                au_price = None
            listings.append({"asin": asin, "sku": sku, "au_price": au_price, "title": title})

    logger.info("[gemini] 出品取得: %d件", len(listings))
    return listings


def get_jp_prices_bulk(asins: list) -> dict:
    """20件バッチでJP価格取得"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    result = {}
    for i in range(0, len(asins), 20):
        batch = asins[i:i + 20]
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
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amount:
                            result[asin] = int(float(amount))
                        break
        except SellingApiException as e:
            logger.warning("[gemini] JP価格バッチエラー: %s", e)
        time.sleep(_JP_INTERVAL)
    return result


def build_analysis_data(listings: list, jp_prices: dict, exchange_rate: float) -> list:
    """分析用データを構築"""
    data = []
    for l in listings:
        asin = l["asin"]
        au_price = l["au_price"]
        jp_price = jp_prices.get(asin)
        title = l["title"]

        if not au_price or not jp_price:
            data.append({
                "asin": asin,
                "title": title[:40],
                "au_price_aud": au_price,
                "jp_price_jpy": jp_price,
                "profit_rate": None,
                "status": "JP価格なし" if not jp_price else "AU価格なし",
            })
            continue

        result = calc_profit(
            asin=asin, title=title, jp_price_jpy=jp_price,
            au_price_aud=au_price, exchange_rate=exchange_rate,
        )
        data.append({
            "asin": asin,
            "title": title[:40],
            "au_price_aud": au_price,
            "jp_price_jpy": jp_price,
            "profit_rate": result.profit_rate,
            "status": "利益あり" if result.is_profitable else "利益なし",
        })
    return data


def ask_gemini(data: list, exchange_rate: float) -> str:
    """Gemini APIにデータを投げて分析レポートを取得"""
    import os
    api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return "GEMINI_API_KEY未設定"

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    # サマリー統計
    total = len(data)
    profitable = [d for d in data if d["status"] == "利益あり"]
    no_profit = [d for d in data if d["status"] == "利益なし"]
    no_jp = [d for d in data if d["status"] == "JP価格なし"]

    # 価格帯別分類
    price_ranges = {
        "〜$50": [d for d in profitable if d["au_price_aud"] and d["au_price_aud"] <= 50],
        "$51〜$100": [d for d in profitable if d["au_price_aud"] and 50 < d["au_price_aud"] <= 100],
        "$101〜$200": [d for d in profitable if d["au_price_aud"] and 100 < d["au_price_aud"] <= 200],
        "$201〜$500": [d for d in profitable if d["au_price_aud"] and 200 < d["au_price_aud"] <= 500],
        "$501以上": [d for d in profitable if d["au_price_aud"] and d["au_price_aud"] > 500],
    }

    # 高額商品リスト（$200以上）
    high_value = sorted(
        [d for d in profitable if d["au_price_aud"] and d["au_price_aud"] > 200],
        key=lambda x: x["au_price_aud"], reverse=True
    )[:20]

    prompt = f"""
あなたはAmazon JP→AUクロスボーダーせどりの専門アナリストです。
以下のデータを分析して、日本語でレポートを作成してください。

## 現状データ
- 総出品数: {total}件
- 為替レート: 1 JPY = {exchange_rate:.4f} AUD
- 利益あり（30%以上）: {len(profitable)}件
- 利益なし: {len(no_profit)}件
- JP価格なし: {len(no_jp)}件

## 価格帯別内訳（利益あり商品）
{json.dumps({k: len(v) for k, v in price_ranges.items()}, ensure_ascii=False)}

## 高額商品TOP20（$200以上）
{json.dumps(high_value, ensure_ascii=False, indent=2)}

## 分析してほしいこと
1. 現在のポートフォリオの健全性評価
2. 高額商品（$200以上）のリスク分析（詐欺・返品・在庫リスク）
3. 価格帯別の収益性評価
4. 利益率改善のための具体的な推奨アクション
5. 注意すべき商品・価格帯

簡潔に、実用的なアドバイスをお願いします。
"""

    response = model.generate_content(prompt)
    return response.text


def main():
    import os
    seller_id = (os.getenv("AMAZON_AU_SELLER_ID") or "").strip()
    if not seller_id:
        logger.error("AMAZON_AU_SELLER_ID未設定")
        sys.exit(1)

    exchange_rate = get_jpy_to_aud()
    logger.info("[gemini] 為替: 1 JPY = %.6f AUD", exchange_rate)

    # 1. 出品一覧取得
    listings = get_my_au_listings()
    if not listings:
        logger.info("[gemini] 出品なし")
        return

    # 2. JP価格取得
    asins = [l["asin"] for l in listings]
    logger.info("[gemini] JP価格取得: %d件", len(asins))
    jp_prices = get_jp_prices_bulk(asins)

    # 3. 分析データ構築
    data = build_analysis_data(listings, jp_prices, exchange_rate)

    # 4. Gemini分析
    logger.info("[gemini] Gemini分析中...")
    report = ask_gemini(data, exchange_rate)
    logger.info("[gemini] 分析完了")

    # 5. メール送信
    send_email(
        subject=f"[SP-API] Gemini週次分析レポート（出品{len(listings)}件）",
        body=report,
    )
    print(report)


if __name__ == "__main__":
    main()

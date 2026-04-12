"""
AU出品中の全ASINについてJP在庫を一括確認するスクリプト
GitHub Actions の check_stock ワークフローから実行される。

出力: jp_stock_check.csv (asin, jp_in_stock, jp_price_jpy)
"""
import csv
import gzip
import io
import sys
import time

import requests as _requests
from sp_api.api import Reports, Products
from sp_api.base import Marketplaces, SellingApiException

import config
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

# JP Products API: 0.5 req/s → 2.1s 間隔
_JP_INTERVAL = 2.1


def get_au_listing_asins() -> list:
    """AU Seller Central の全出品 ASIN を Reports API で取得する"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    # レポート作成
    logger.info("AU出品レポートをリクエスト中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]
    logger.info("レポートID: %s", report_id)

    # 完成まで待機（最大20分・10秒おき）
    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        logger.info("[%d/120] レポートステータス: %s", attempt + 1, status)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")
    else:
        raise RuntimeError("レポートタイムアウト（20分）")

    # ドキュメントURL取得
    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")

    # ダウンロード
    logger.info("レポートダウンロード中...")
    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content

    # TSV パース → ASIN 抽出（重複除去）
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    asins = []
    seen = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        if asin and len(asin) == 10 and asin not in seen:
            seen.add(asin)
            asins.append(asin)

    logger.info("AU出品ASIN取得完了: %d件", len(asins))
    return asins


def check_jp_stock_bulk(asins: list) -> list:
    """20件ずつバッチで JP 競合価格 API を叩き在庫を確認する"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    results = []
    batch_size = 20
    total = len(asins)

    for i in range(0, total, batch_size):
        batch = asins[i : i + batch_size]
        logger.info("[%d/%d] JP在庫確認中...", min(i + batch_size, total), total)

        try:
            resp = api.get_competitive_pricing_for_asins(batch)
            items = resp.payload if isinstance(resp.payload, list) else []

            found = {}
            for item in items:
                asin = item.get("ASIN", "")
                comp_prices = (
                    item.get("Product", {})
                    .get("CompetitivePricing", {})
                    .get("CompetitivePrices", [])
                )
                in_stock = False
                price_jpy = None
                for cp in comp_prices:
                    if cp.get("condition") == "New":
                        amount = (
                            cp.get("Price", {})
                            .get("ListingPrice", {})
                            .get("Amount")
                        )
                        if amount:
                            price_jpy = int(float(amount))
                            in_stock = True
                        break
                found[asin] = (in_stock, price_jpy)

            for asin in batch:
                in_stock, price_jpy = found.get(asin, (False, None))
                results.append(
                    {"asin": asin, "jp_in_stock": in_stock, "jp_price_jpy": price_jpy}
                )

        except SellingApiException as e:
            logger.warning("バッチエラー (%s...): %s", batch[0], e)
            for asin in batch:
                results.append(
                    {"asin": asin, "jp_in_stock": False, "jp_price_jpy": None}
                )

        time.sleep(_JP_INTERVAL)

    return results


def main():
    # 1. AU 出品 ASIN 取得
    asins = get_au_listing_asins()
    if not asins:
        logger.error("ASINが取得できませんでした")
        sys.exit(1)

    # 2. JP 在庫確認
    logger.info("JP在庫確認開始: %d件（約%.0f分かかります）", len(asins), len(asins) / 20 * _JP_INTERVAL / 60)
    results = check_jp_stock_bulk(asins)

    # 3. CSV 保存
    output = "jp_stock_check.csv"
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["asin", "jp_in_stock", "jp_price_jpy"])
        writer.writeheader()
        writer.writerows(results)

    # 4. サマリー表示
    in_stock = sum(1 for r in results if r["jp_in_stock"])
    print(f"\n{'='*40}")
    print(f"総件数:       {len(results):>6}")
    print(f"JP在庫あり:   {in_stock:>6} 件")
    print(f"JP在庫なし:   {len(results) - in_stock:>6} 件")
    print(f"結果ファイル: {output}")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()

"""
AU出品中の全ASINについてJP在庫を確認し、在庫なしの出品を自動削除するスクリプト。
GitHub Actions の check_stock ワークフローから実行される。

出力: jp_stock_check.csv (asin, sku, jp_in_stock, jp_price_jpy, action)
"""
import csv
import gzip
import io
import sys
import time

import requests as _requests
from sp_api.api import Reports, Products, ListingsItems
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
# ListingsItems 削除: 5 req/s
_DELETE_INTERVAL = 0.3


def get_au_listings() -> list:
    """AU Seller Central の全出品 {asin, sku} を Reports API で取得する"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

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

    logger.info("レポートダウンロード中...")
    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content

    # TSV パース → {asin, sku} 抽出（ASIN重複除去）
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    listings = []
    seen_asins = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        if asin and len(asin) == 10 and asin not in seen_asins and sku:
            seen_asins.add(asin)
            listings.append({"asin": asin, "sku": sku})

    logger.info("AU出品取得完了: %d件", len(listings))
    return listings


def check_jp_stock_bulk(listings: list) -> list:
    """20件ずつバッチで JP 在庫を確認し結果を返す"""
    api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    asins = [l["asin"] for l in listings]
    sku_map = {l["asin"]: l["sku"] for l in listings}

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
                results.append({
                    "asin": asin,
                    "sku": sku_map[asin],
                    "jp_in_stock": in_stock,
                    "jp_price_jpy": price_jpy,
                    "action": "",
                })

        except SellingApiException as e:
            logger.warning("バッチエラー (%s...): %s", batch[0], e)
            for asin in batch:
                results.append({
                    "asin": asin,
                    "sku": sku_map[asin],
                    "jp_in_stock": False,
                    "jp_price_jpy": None,
                    "action": "error",
                })

        time.sleep(_JP_INTERVAL)

    return results


def delete_no_stock_listings(results: list, seller_id: str):
    """JP在庫なしの出品を AU SP-API で削除する"""
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    to_delete = [r for r in results if not r["jp_in_stock"] and r["action"] != "error"]
    total = len(to_delete)
    logger.info("削除対象: %d件", total)

    deleted = 0
    failed = 0
    for i, r in enumerate(to_delete, 1):
        try:
            api.delete_listings_item(
                sellerId=seller_id,
                sku=r["sku"],
                marketplaceIds=[config.MARKETPLACE_AU],
            )
            r["action"] = "deleted"
            deleted += 1
            if i % 50 == 0:
                logger.info("[%d/%d] 削除済み %d件", i, total, deleted)
        except SellingApiException as e:
            logger.warning("削除失敗 (SKU %s): %s", r["sku"], e)
            r["action"] = "delete_failed"
            failed += 1
        time.sleep(_DELETE_INTERVAL)

    logger.info("削除完了: 成功 %d件 / 失敗 %d件", deleted, failed)
    return deleted, failed


def main():
    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id") or ""
    if not seller_id:
        import os
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    # 1. AU 出品一覧取得
    listings = get_au_listings()
    if not listings:
        logger.error("出品が取得できませんでした")
        sys.exit(1)

    # 2. JP 在庫確認
    eta = len(listings) / 20 * _JP_INTERVAL / 60
    logger.info("JP在庫確認開始: %d件（約%.0f分）", len(listings), eta)
    results = check_jp_stock_bulk(listings)

    # 3. JP在庫なしを自動削除
    in_stock_count = sum(1 for r in results if r["jp_in_stock"])
    no_stock_count = len(results) - in_stock_count
    logger.info("JP在庫あり: %d件 / なし: %d件 → 削除します", in_stock_count, no_stock_count)
    deleted, failed = delete_no_stock_listings(results, seller_id)

    # 4. CSV 保存
    output = "jp_stock_check.csv"
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["asin", "sku", "jp_in_stock", "jp_price_jpy", "action"]
        )
        writer.writeheader()
        writer.writerows(results)

    # 5. サマリー
    print(f"\n{'='*40}")
    print(f"総件数:       {len(results):>6}")
    print(f"JP在庫あり:   {in_stock_count:>6} 件（残存）")
    print(f"削除成功:     {deleted:>6} 件")
    print(f"削除失敗:     {failed:>6} 件")
    print(f"結果ファイル: {output}")
    print(f"{'='*40}")


if __name__ == "__main__":
    main()

"""
inactive（停止中）の出品を一括削除するスクリプト。

真贋調査（authenticity investigation）リスク回避のため、
active 以外の出品をセラセンからすべて削除する。

Amazon は inactive 状態の出品にも真贋調査を送ることがある。
inactive は「売れていないが存在する」状態で、リスクだけ残る。
削除後に再度出品したい場合は catalog_discover.py が自動的に再発掘・再出品する。

使い方:
  python bulk_delete_inactive.py           # 件数確認 → 入力で実行
  python bulk_delete_inactive.py --yes     # 確認スキップ（GitHub Actions 用）
  python bulk_delete_inactive.py --dry-run # 削除せず件数確認のみ
"""
import csv
import gzip
import io
import os
import sys
import time
import argparse

import requests as _requests
from sp_api.api import Reports, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}

DELETE_INTERVAL = 0.3   # ListingsItems: 5 req/s
RETRY_DELAY = 2.0       # 失敗時リトライ間隔(秒)
MAX_RETRIES = 2         # 最大リトライ回数


# ─────────────────────────────────────────────
# 1. inactive 出品一覧取得
# ─────────────────────────────────────────────

def get_inactive_listings() -> list:
    """Reports API で inactive の出品リストを取得する"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[bulk_delete_inactive] 出品レポートをリクエスト中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[bulk_delete_inactive] レポートステータス: %s (%d/120)", status, attempt + 1)
        if status == "DONE":
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {status}")
    else:
        raise RuntimeError("レポートタイムアウト（20分）")

    doc_id = status_resp.payload["reportDocumentId"]
    doc_resp = api.get_report_document(doc_id)
    url = doc_resp.payload["url"]
    compression = doc_resp.payload.get("compressionAlgorithm", "")

    r = _requests.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    active_count = inactive_count = deleted_count = 0
    inactive_listings = []

    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        item_status = row.get("status", "").strip().lower()

        if not asin or not sku:
            continue

        if item_status == "deleted":
            deleted_count += 1
            continue
        elif item_status == "active":
            active_count += 1
        else:
            # inactive / suppressed / incomplete など
            inactive_listings.append({"asin": asin, "sku": sku, "status": item_status})
            inactive_count += 1

    logger.info(
        "[bulk_delete_inactive] 出品取得完了: active=%d / inactive=%d / 削除済=%d",
        active_count, inactive_count, deleted_count,
    )
    return inactive_listings


# ─────────────────────────────────────────────
# 2. 一括削除
# ─────────────────────────────────────────────

def delete_listings(inactive_listings: list, seller_id: str, dry_run: bool = False) -> tuple:
    """inactive 出品を一括削除する。(deleted, failed, failed_details) を返す"""
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    deleted = 0
    failed = 0
    failed_details = []   # {"sku", "asin", "status", "error"} のリスト
    total = len(inactive_listings)

    for i, listing in enumerate(inactive_listings):
        sku = listing["sku"]
        asin = listing["asin"]
        status = listing["status"]

        if i % 50 == 0:
            logger.info("[bulk_delete_inactive] 削除中: %d/%d (失敗累計: %d)", i, total, failed)

        if dry_run:
            logger.debug("[bulk_delete_inactive][DRY-RUN] 削除予定: %s (%s) [%s]", sku, asin, status)
            deleted += 1
            continue

        success = False
        last_error = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                api.delete_listings_item(
                    sellerId=seller_id,
                    sku=sku,
                    marketplaceIds=[config.MARKETPLACE_AU],
                )
                deleted += 1
                success = True
                break
            except SellingApiException as e:
                last_error = str(e)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.warning(
                        "[bulk_delete_inactive] 削除失敗(%d回試行): %s (%s) [%s] - %s",
                        MAX_RETRIES + 1, sku, asin, status, last_error,
                    )
            except Exception as e:
                last_error = str(e)
                logger.warning("[bulk_delete_inactive] 予期しないエラー: %s (%s) - %s", sku, asin, e)
                break

        if not success:
            failed += 1
            failed_details.append({"sku": sku, "asin": asin, "status": status, "error": last_error or "unknown"})

        time.sleep(DELETE_INTERVAL)

    action = "DRY-RUN削除予定" if dry_run else "削除完了"
    logger.info("[bulk_delete_inactive] %s: %d件 / 失敗: %d件", action, deleted, failed)

    # 失敗したSKUをCSVに保存（デバッグ用）
    if failed_details and not dry_run:
        import csv as _csv
        fail_path = "bulk_delete_failed.csv"
        with open(fail_path, "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=["sku", "asin", "status", "error"])
            writer.writeheader()
            writer.writerows(failed_details)
        logger.info("[bulk_delete_inactive] 失敗リストを保存: %s", fail_path)

        # エラー種別集計
        error_summary: dict = {}
        for d in failed_details:
            err = d["error"][:80]  # 先頭80文字
            error_summary[err] = error_summary.get(err, 0) + 1
        logger.info("[bulk_delete_inactive] 失敗エラー内訳:")
        for err, cnt in sorted(error_summary.items(), key=lambda x: -x[1])[:10]:
            logger.info("  %d件: %s", cnt, err)

    return deleted, failed, failed_details


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="inactive（停止中）の出品を一括削除して真贋調査リスクを下げる"
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="確認プロンプトをスキップして即削除（GitHub Actions 用）",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="削除せず件数確認のみ（実際の削除は行わない）",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="bulk_delete_failed.csv の失敗SKUのみ再試行",
    )
    args = parser.parse_args()

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[bulk_delete_inactive] AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    # --retry-failed: 前回の失敗CSVから再試行
    if args.retry_failed:
        import csv as _csv
        fail_path = "bulk_delete_failed.csv"
        if not os.path.exists(fail_path):
            logger.error("[bulk_delete_inactive] %s が見つかりません", fail_path)
            sys.exit(1)
        with open(fail_path, newline="", encoding="utf-8") as f:
            inactive_listings = list(_csv.DictReader(f))
        logger.info("[bulk_delete_inactive] 再試行モード: %d件", len(inactive_listings))
    else:
        # 1. inactive 出品一覧取得
        inactive_listings = get_inactive_listings()

    if not inactive_listings:
        logger.info("[bulk_delete_inactive] inactive 出品なし。終了")
        return

    total = len(inactive_listings)
    logger.info("[bulk_delete_inactive] 削除対象: %d件", total)

    # 件数ごとの内訳表示
    status_counts: dict = {}
    for l in inactive_listings:
        s = l["status"]
        status_counts[s] = status_counts.get(s, 0) + 1
    for s, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        logger.info("  %s: %d件", s, cnt)

    if args.dry_run:
        logger.info("[bulk_delete_inactive] *** DRY-RUN モード: 実際には削除しません ***")
        deleted, failed, _ = delete_listings(inactive_listings, seller_id, dry_run=True)
        logger.info("[bulk_delete_inactive] DRY-RUN 完了: 削除予定 %d件", deleted)
        return

    # 2. 確認（--yes なら自動承認）
    if not args.yes:
        print(f"\n⚠️  inactive 出品 {total}件 を完全に削除します。")
        print("   削除後はセラセンから消え、catalog_discover が再発掘するまで出品されません。")
        print(f"   内訳: {status_counts}")
        answer = input("\n実行しますか？ (yes/no): ").strip().lower()
        if answer != "yes":
            logger.info("[bulk_delete_inactive] キャンセルしました")
            return

    # 3. 一括削除
    logger.info("[bulk_delete_inactive] 削除開始: %d件 (約%.0f分)",
                total, total * DELETE_INTERVAL / 60)
    deleted, failed, failed_details = delete_listings(inactive_listings, seller_id, dry_run=False)

    # 4. エラー内訳集計
    error_summary: dict = {}
    for d in failed_details:
        err = d["error"][:100]
        error_summary[err] = error_summary.get(err, 0) + 1

    # 5. メール通知
    subject = (
        f"[SP-API] inactive出品 一括削除完了: {deleted}件削除 / {failed}件失敗"
        if failed == 0
        else f"[SP-API] inactive出品 一括削除: {deleted}件削除 / ⚠️{failed}件失敗"
    )
    body = (
        f"=== inactive出品 一括削除 完了 ===\n\n"
        f"対象件数:  {total}件\n"
        f"削除完了:  {deleted}件\n"
        f"失敗:      {failed}件\n\n"
        f"ステータス内訳:\n"
        + "\n".join(f"  {s}: {c}件" for s, c in status_counts.items())
        + ("\n\n--- 失敗エラー内訳 ---\n"
           + "\n".join(f"  {c}件: {e}" for e, c in sorted(error_summary.items(), key=lambda x: -x[1])[:10])
           if failed > 0 else "")
        + "\n\n"
        f"再出品: catalog_discover.py が次回実行時に利益商品を再発掘します\n"
        f"真贋調査リスク: inactive削除によりリスクを低減しました\n"
        + (f"\n⚠️ 失敗したSKUは bulk_delete_failed.csv に保存済み（次回再試行可）\n"
           if failed > 0 else "")
    )
    send_email(subject=subject, body=body)
    logger.info("[bulk_delete_inactive] 完了")


if __name__ == "__main__":
    main()

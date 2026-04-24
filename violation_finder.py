"""
ListingsItems API を使ってポリシー違反のある出品を検出するスクリプト。

ロジック:
  1. Reports API で全出品一覧（active + inactive）を取得
  2. 各SKUに get_listings_item を呼び出し、issues を取得
  3. severity==ERROR またはコードに食品安全・ポリシー関連ワードが含まれるものを収集
  4. violation_asins.json に書き出し

使い方:
  python violation_finder.py               # 全件チェック → violation_asins.json 書き出し
  python violation_finder.py --dry-run     # 書き出しなし（検出内容をログ出力のみ）
  python violation_finder.py --max 200     # 最大200件チェック
"""
import argparse
import csv
import gzip
import io
import json
import os
import time

import requests as _requests
from sp_api.api import Reports, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from utils.logger import get_logger

logger = get_logger(__name__)

MARKETPLACE_AU = config.MARKETPLACE_AU

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_SELLER_ID = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")

# violation 関連キーワード（issue code / message に含まれていれば対象）
_VIOLATION_KEYWORDS = [
    "food", "safety", "policy", "compliance", "restricted",
    "hazmat", "pesticide", "drug", "supplement",
]

LISTINGS_INTERVAL = 0.2  # 5 req/s


# ─────────────────────────────────────────────
# 1. AU 出品一覧取得（active + inactive）
# ─────────────────────────────────────────────

def get_my_au_listings() -> list:
    """Reports API で自分の AU 出品一覧を取得する（削除済み除外）。"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[violation_finder] 出品レポートをリクエスト中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]
    logger.info("[violation_finder] レポートID: %s", report_id)

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[violation_finder] レポートステータス: %s (%d/120)", status, attempt + 1)
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

    listings = []
    seen = set()
    active_count = inactive_count = 0
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        item_status = row.get("status", "").strip().lower()
        title = row.get("item-name", "").strip()
        if asin and len(asin) == 10 and sku and asin not in seen and item_status != "deleted":
            seen.add(asin)
            listings.append({"asin": asin, "sku": sku, "status": item_status, "title": title})
            if item_status == "active":
                active_count += 1
            else:
                inactive_count += 1

    logger.info(
        "[violation_finder] 出品取得完了: %d件（active=%d, inactive=%d）",
        len(listings), active_count, inactive_count,
    )
    return listings


# ─────────────────────────────────────────────
# 2. violations チェック
# ─────────────────────────────────────────────

def _is_violation_issue(issue: dict) -> bool:
    """1件の issue が違反に該当するか判定する。"""
    severity = issue.get("severity", "").upper()
    if severity == "ERROR":
        return True
    # コード・メッセージにキーワードが含まれるか確認
    code = issue.get("code", "").lower()
    message = issue.get("message", "").lower()
    for kw in _VIOLATION_KEYWORDS:
        if kw in code or kw in message:
            return True
    return False


def check_violations(listings: list, max_count: int = 0) -> list:
    """
    各SKUの issues を取得し、違反のある出品リストを返す。
    Returns: [{"asin": ..., "sku": ..., "issues": [...]}, ...]
    """
    if not _SELLER_ID:
        raise RuntimeError("AMAZON_AU_SELLER_ID が未設定です。.env に追加してください。")

    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    targets = listings if not max_count else listings[:max_count]
    total = len(targets)
    violations = []

    logger.info("[violation_finder] %d件をチェックします（rate: 5 req/s）", total)

    for idx, item in enumerate(targets, 1):
        sku = item["sku"]
        asin = item["asin"]

        if idx % 50 == 0 or idx == total:
            logger.info("[violation_finder] 進捗: %d/%d", idx, total)

        try:
            resp = api.get_listings_item(
                sellerId=_SELLER_ID,
                sku=sku,
                marketplaceIds=[MARKETPLACE_AU],
                includedData=["issues", "summaries"],
            )
            issues_raw = resp.payload.get("issues", [])
            matched_issues = [i for i in issues_raw if _is_violation_issue(i)]
            if matched_issues:
                logger.info(
                    "[violation_finder] 違反検出: %s (SKU: %s) — %d件",
                    asin, sku, len(matched_issues),
                )
                violations.append({"asin": asin, "sku": sku, "issues": matched_issues})

        except SellingApiException as e:
            logger.warning("[violation_finder] APIエラー (SKU %s): %s", sku, e)
        except Exception as e:
            logger.warning("[violation_finder] 例外 (SKU %s): %s", sku, e)

        time.sleep(LISTINGS_INTERVAL)

    logger.info("[violation_finder] チェック完了: 違反=%d件 / 全体=%d件", len(violations), total)
    return violations


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="ポリシー違反出品を検出する")
    parser.add_argument("--dry-run", action="store_true", help="検出のみ、JSON書き出しなし")
    parser.add_argument("--max", type=int, default=0, help="チェックする最大件数（0=全件）")
    args = parser.parse_args()

    listings = get_my_au_listings()
    violations = check_violations(listings, max_count=args.max)

    if not violations:
        logger.info("[violation_finder] 違反出品は見つかりませんでした")
        return

    logger.info("[violation_finder] 違反出品 %d件:", len(violations))
    for v in violations:
        codes = [i.get("code", "?") for i in v["issues"]]
        logger.info("  ASIN=%s  SKU=%s  issues=%s", v["asin"], v["sku"], codes)

    if args.dry_run:
        logger.info("[violation_finder] --dry-run のため violation_asins.json は書き出しません")
        return

    out_path = os.path.join(os.path.dirname(__file__), "violation_asins.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(violations, f, ensure_ascii=False, indent=2)
    logger.info("[violation_finder] violation_asins.json に書き出しました (%d件)", len(violations))


if __name__ == "__main__":
    main()

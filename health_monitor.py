"""
Amazon Account Health 監視スクリプト。

SP-API には AHR スコア直接取得のエンドポイントがないため、
ListingsItems API で出品の issues を確認し、
ERROR 件数の増加を検知してメール通知する。

動作:
  1. Reports API で全出品一覧取得
  2. 最大 SAMPLE_SIZE 件の SKU について issues を確認
  3. 前回の ERROR 件数（health_state.json）と比較
  4. 増加していればメール通知
  5. health_state.json を更新

使い方:
  python health_monitor.py
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from sp_api.api import Reports, ListingsItems
from sp_api.base import Marketplaces, SellingApiException
import requests as _requests
import csv
import gzip
import io

import config
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

SAMPLE_SIZE = 200        # チェックするSKU上限（全件は時間がかかりすぎるため）
INTERVAL = 0.21          # ListingsItems: 5 req/s
STATE_FILE = os.path.join(os.path.dirname(__file__), "health_state.json")

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}

# ─────────────────────────────────────────────
# 違反に関連するキーワード（issue code / message）
# ─────────────────────────────────────────────
VIOLATION_KEYWORDS = [
    "food", "safety", "pesticide", "drug", "supplement", "vitamin",
    "hazmat", "restricted", "compliance", "policy", "recall",
    "prohibited", "regulation", "banned", "illegal",
]


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"last_error_count": 0, "last_check": None, "last_violation_skus": []}


def _save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _get_all_listings() -> list:
    """Reports API で全出品一覧（active+inactive）を取得"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    logger.info("[health_monitor] 出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[health_monitor] レポートステータス: %s (%d/120)", status, attempt + 1)
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
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    listings = []
    seen = set()
    for row in reader:
        asin = row.get("asin1", "").strip()
        sku = row.get("seller-sku", "").strip()
        status = row.get("status", "").strip().lower()
        if asin and sku and asin not in seen and status != "deleted":
            seen.add(asin)
            listings.append({"asin": asin, "sku": sku, "status": status})

    logger.info("[health_monitor] 出品取得完了: %d件", len(listings))
    return listings


def _check_issues(listings: list, seller_id: str) -> list:
    """
    ListingsItems API で issues を確認し、
    ERROR/WARNING の違反関連 issue を持つ SKU リストを返す。
    """
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    violation_items = []
    sample = listings[:SAMPLE_SIZE]
    total = len(sample)

    for i, listing in enumerate(sample):
        sku = listing["sku"]
        asin = listing["asin"]
        if i % 20 == 0:
            logger.info("[health_monitor] issues確認中: %d/%d", i, total)
        try:
            resp = api.get_listings_item(
                sellerId=seller_id,
                sku=sku,
                marketplaceIds=[config.MARKETPLACE_AU],
                includedData=["issues", "summaries"],
            )
            payload = resp.payload or {}
            issues = payload.get("issues", [])
            for issue in issues:
                severity = issue.get("severity", "").upper()
                code = issue.get("code", "").lower()
                message = issue.get("message", "").lower()
                combined = f"{code} {message}"
                is_violation = any(kw in combined for kw in VIOLATION_KEYWORDS)
                if severity in ("ERROR",) or is_violation:
                    violation_items.append({
                        "asin": asin,
                        "sku": sku,
                        "severity": severity,
                        "code": issue.get("code", ""),
                        "message": issue.get("message", ""),
                    })
                    break  # 1 SKU で 1件カウントで十分
        except SellingApiException as e:
            logger.debug("[health_monitor] %s: issues取得エラー - %s", sku, e)
        except Exception as e:
            logger.debug("[health_monitor] %s: 予期しないエラー - %s", sku, e)
        time.sleep(INTERVAL)

    logger.info("[health_monitor] 違反/ERROR issue検出: %d件 / %d件チェック",
                len(violation_items), total)
    return violation_items


def main():
    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[health_monitor] AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    state = _load_state()
    prev_error_count = state.get("last_error_count", 0)
    prev_violation_skus = set(state.get("last_violation_skus", []))
    logger.info("[health_monitor] 前回ERROR件数: %d件", prev_error_count)

    # 1. 全出品取得
    listings = _get_all_listings()
    if not listings:
        logger.info("[health_monitor] 出品なし。終了")
        return

    # 2. issues確認（アクティブ優先）
    active = [l for l in listings if l["status"] == "active"]
    inactive = [l for l in listings if l["status"] != "active"]
    check_targets = (active + inactive)[:SAMPLE_SIZE]

    violation_items = _check_issues(check_targets, seller_id)
    current_error_count = len(violation_items)
    current_violation_skus = {v["sku"] for v in violation_items}

    # 3. 新規違反の検出
    new_violations = [v for v in violation_items if v["sku"] not in prev_violation_skus]

    now = datetime.now(timezone.utc).isoformat()

    # 4. 状態更新
    _save_state({
        "last_error_count": current_error_count,
        "last_check": now,
        "last_violation_skus": list(current_violation_skus),
    })

    # 5. 結果ログ
    logger.info(
        "[health_monitor] 完了: 現在ERROR %d件 / 前回 %d件 / 新規違反 %d件",
        current_error_count, prev_error_count, len(new_violations)
    )

    # 6. 悪化していればメール通知
    if current_error_count > prev_error_count or new_violations:
        subject = (
            f"[緊急] Amazon AHR悪化検知 - 新規違反{len(new_violations)}件 "
            f"(合計ERROR {current_error_count}件)"
        )
        lines = [
            "=== Amazon Account Health 悪化検知 ===",
            "",
            f"チェック日時:   {now}",
            f"前回ERROR件数:  {prev_error_count}件",
            f"今回ERROR件数:  {current_error_count}件",
            f"新規違反:       {len(new_violations)}件",
            "",
        ]
        if new_violations:
            lines.append("--- 新規違反 SKU ---")
            for v in new_violations[:30]:
                lines.append(f"  {v['sku']} ({v['asin']})  [{v['severity']}] {v['code']}: {v['message'][:80]}")
        if current_error_count > 30:
            lines.append("")
            lines.append("⚠️ 違反件数が多いため violation_finder.py での詳細確認を推奨します。")

        body = "\n".join(lines)
        send_email(subject=subject, body=body)
        logger.info("[health_monitor] メール通知送信: %s", subject)
    else:
        logger.info("[health_monitor] 異常なし（前回比変化なし）")


if __name__ == "__main__":
    main()

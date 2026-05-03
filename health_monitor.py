"""
Amazon Account Health 監視スクリプト。

出品の issues を確認し、真贋・IP侵害クレームを即時自動削除。
その他の違反（ポリシー違反等）は通知のみ。

動作:
  1. Reports API で全出品一覧取得
  2. 全 SKU の issues を Listings API で確認
  3. 「危険フラグ」（真贋/IP侵害）→ 即時 deleteListingsItem + 緊急メール
  4. 「注意フラグ」（その他違反）→ 通知のみ（前回比で悪化時）
  5. health_state.json を更新

使い方:
  python health_monitor.py               # 実行（危険フラグは自動削除）
  python health_monitor.py --dry-run     # テスト（削除しない・メール送信のみ）
  python health_monitor.py --max 500     # チェック上限件数（デフォルト: 全件）
"""
import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

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
from sp_api.api import Reports, ListingsItems
from sp_api.base import Marketplaces, SellingApiException

import config
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

INTERVAL    = 0.21   # ListingsItems GET: 5 req/s
DELETE_INTERVAL = 0.3  # ListingsItems DELETE: 5 req/s
MAX_RETRIES = 2
STATE_FILE  = os.path.join(os.path.dirname(__file__), "health_state.json")

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}

# ─────────────────────────────────────────────
# 危険フラグ（真贋・IP侵害）→ 自動削除
# ─────────────────────────────────────────────
DANGER_CODES = {
    "AUTHENTICITY_COMPLAINT",
    "INTELLECTUAL_PROPERTY_COMPLAINT",
    "PRODUCT_AUTHENTICITY_CUSTOMER_COMPLAINT",
    "SUSPECTED_COUNTERFEIT",
    "IP_COMPLAINT",
    "ASIN_AUTHENTICITY_UNVERIFIABLE",
    "BRAND_AUTHENTICITY_CONCERN",
    "COUNTERFEIT_COMPLAINT",
    "INFRINGEMENT",
}

DANGER_KEYWORDS = [
    "authenticity", "counterfeit", "intellectual property",
    "inauthentic", "brand registry", "trademark", "copyright",
    "suspected counterfeit", "brand owner", "ip complaint",
    "infringement", "genuine", "not genuine",
    "真贋", "偽物", "著作権", "商標", "知的財産",
]

# ─────────────────────────────────────────────
# 注意フラグ（その他ポリシー違反）→ 通知のみ
# ─────────────────────────────────────────────
WARNING_KEYWORDS = [
    "food", "safety", "pesticide", "drug", "supplement", "vitamin",
    "hazmat", "restricted", "compliance", "policy", "recall",
    "prohibited", "regulation", "banned", "illegal",
    "condition", "listing quality", "stranded",
]


def _load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "last_error_count": 0,
        "last_check": None,
        "last_violation_skus": [],
        "total_auto_deleted": 0,
    }


def _save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _get_all_listings() -> list:
    """Reports API で全出品一覧（active+inactive）を取得"""
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    logger.info("[health] 出品レポート取得中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[health] レポートステータス: %s (%d/120)", status, attempt + 1)
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
        status_val = row.get("status", "").strip().lower()
        title = row.get("item-name", "").strip()
        if asin and sku and asin not in seen and status_val != "deleted":
            seen.add(asin)
            listings.append({
                "asin": asin,
                "sku": sku,
                "status": status_val,
                "title": title[:60],
            })

    logger.info("[health] 出品取得完了: %d件", len(listings))
    return listings


def _classify_issue(code: str, message: str) -> str:
    """
    issue の分類を返す:
      "DANGER"   → 真贋/IP侵害 → 自動削除対象
      "WARNING"  → その他ポリシー違反 → 通知のみ
      ""         → 無害（スキップ）
    """
    code_upper = code.upper()
    combined = f"{code.lower()} {message.lower()}"

    # 危険フラグ（真贋/IP）
    if code_upper in DANGER_CODES:
        return "DANGER"
    if any(kw in combined for kw in DANGER_KEYWORDS):
        return "DANGER"

    # 注意フラグ（その他違反）
    if any(kw in combined for kw in WARNING_KEYWORDS):
        return "WARNING"

    return ""


def _check_all_issues(listings: list, seller_id: str, max_items: int) -> tuple:
    """
    全出品の issues を確認する。

    Returns:
        danger_items: [{asin, sku, title, code, message}]  # 自動削除対象
        warning_items: [{asin, sku, title, code, message}]  # 通知のみ
    """
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    danger_items = []
    warning_items = []

    # アクティブ優先でチェック
    active = [l for l in listings if l["status"] == "active"]
    inactive = [l for l in listings if l["status"] != "active"]
    targets = (active + inactive)[:max_items]
    total = len(targets)
    logger.info("[health] issues確認: %d件 (アクティブ%d件 + 非アクティブ%d件)",
                total, len(active), len(inactive))

    for i, listing in enumerate(targets):
        sku = listing["sku"]
        asin = listing["asin"]
        title = listing.get("title", "")
        if i % 50 == 0:
            logger.info("[health] issues確認中: %d/%d (危険:%d 注意:%d)",
                        i, total, len(danger_items), len(warning_items))
        try:
            resp = api.get_listings_item(
                sellerId=seller_id,
                sku=sku,
                marketplaceIds=[config.MARKETPLACE_AU],
                includedData=["issues"],
            )
            payload = resp.payload or {}
            issues = payload.get("issues") or []
            for issue in issues:
                severity = issue.get("severity", "").upper()
                code = issue.get("code", "")
                message = issue.get("message", "")
                kind = _classify_issue(code, message)

                if kind == "DANGER":
                    danger_items.append({
                        "asin": asin, "sku": sku, "title": title,
                        "severity": severity, "code": code, "message": message,
                    })
                    break  # 1 SKUで1件カウント（最初の危険issueで打ち切り）
                elif kind == "WARNING" and severity == "ERROR":
                    warning_items.append({
                        "asin": asin, "sku": sku, "title": title,
                        "severity": severity, "code": code, "message": message,
                    })
                    break

        except SellingApiException as e:
            logger.debug("[health] %s: issues取得エラー - %s", sku, e)
        except Exception as e:
            logger.debug("[health] %s: 予期しないエラー - %s", sku, e)
        time.sleep(INTERVAL)

    logger.info("[health] issues確認完了: 危険=%d件 / 注意=%d件 / チェック=%d件",
                len(danger_items), len(warning_items), total)
    return danger_items, warning_items


def _delete_items(danger_items: list, seller_id: str, dry_run: bool) -> tuple:
    """
    危険フラグ付き出品を削除する。

    Returns:
        (deleted_count, failed_items)
    """
    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    deleted = 0
    failed = []

    for item in danger_items:
        sku = item["sku"]
        asin = item["asin"]

        if dry_run:
            logger.info("[health][DRY-RUN] 削除予定: %s (%s) [%s] %s",
                        sku, asin, item["code"], item["title"][:40])
            deleted += 1
            continue

        success = False
        last_err = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                api.delete_listings_item(
                    sellerId=seller_id,
                    sku=sku,
                    marketplaceIds=[config.MARKETPLACE_AU],
                )
                logger.info("[health] 削除完了: %s (%s) [%s] %s",
                            sku, asin, item["code"], item["title"][:40])
                deleted += 1
                success = True
                break
            except SellingApiException as e:
                last_err = str(e)
                if attempt < MAX_RETRIES:
                    time.sleep(2)
        if not success:
            logger.warning("[health] 削除失敗: %s (%s) - %s", sku, asin, last_err)
            failed.append({**item, "error": last_err})
        time.sleep(DELETE_INTERVAL)

    return deleted, failed


def _send_danger_alert(
    danger_items: list,
    deleted: int,
    failed_items: list,
    dry_run: bool,
    now: str,
):
    """真贋/IP危険フラグのメール（削除実績付き）"""
    prefix = "[DRY-RUN] " if dry_run else ""
    subject = (
        f"{prefix}[緊急🚨] 真贋・IP違反検知 → {deleted}件削除"
        f"{'（失敗' + str(len(failed_items)) + '件）' if failed_items else ''}"
        f" / 合計{len(danger_items)}件"
    )
    lines = [
        "=== 真贋・IP侵害クレーム 自動削除レポート ===",
        "",
        f"検知日時: {now}",
        f"危険フラグ検出: {len(danger_items)}件",
        f"削除{'予定' if dry_run else '完了'}: {deleted}件",
        f"削除失敗:       {len(failed_items)}件",
        "",
        "--- 削除した出品 ---",
    ]
    for item in danger_items:
        status = "削除済" if (not dry_run and item not in failed_items) else ("DRY-RUN" if dry_run else "失敗")
        lines.append(
            f"  [{status}] {item['sku']} ({item['asin']})"
            f"\n    コード: {item['code']}"
            f"\n    内容: {item['message'][:100]}"
            f"\n    商品: {item['title']}"
        )
    if failed_items:
        lines += ["", "--- 削除失敗 ---"]
        for item in failed_items:
            lines.append(f"  {item['sku']} ({item['asin']}) - {item.get('error', '')[:80]}")
    lines += [
        "",
        "⚠️ 対象商品は Amazon Seller Central でも確認してください。",
        "   アカウントヘルスへの影響が懸念される場合は Appeal を提出してください。",
    ]
    send_email(subject=subject, body="\n".join(lines))
    logger.info("[health] 緊急メール送信: %s", subject)


def _send_warning_alert(warning_items: list, new_warnings: list, now: str):
    """注意フラグ（前回比増加時のみ）メール通知"""
    if not new_warnings:
        return
    subject = f"[注意⚠️] Amazon 出品ポリシー違反増加 - 新規{len(new_warnings)}件 (合計{len(warning_items)}件)"
    lines = [
        "=== Amazon 出品ポリシー違反 増加通知 ===",
        "",
        f"検知日時: {now}",
        f"新規違反: {len(new_warnings)}件",
        f"合計違反: {len(warning_items)}件",
        "",
        "--- 新規違反 SKU ---",
    ]
    for item in new_warnings[:30]:
        lines.append(
            f"  {item['sku']} ({item['asin']})  [{item['severity']}]"
            f"\n    コード: {item['code']}"
            f"\n    内容: {item['message'][:100]}"
        )
    lines += [
        "",
        "violation_finder.py で詳細確認・violation_deleter.py で手動削除できます。",
    ]
    send_email(subject=subject, body="\n".join(lines))
    logger.info("[health] 注意メール送信: %s", subject)


def main():
    parser = argparse.ArgumentParser(description="Amazon Account Health 監視・自動削除")
    parser.add_argument("--dry-run", action="store_true",
                        help="DRY RUN（削除しない・メール送信のみ）")
    parser.add_argument("--max", type=int, default=0,
                        help="チェック上限件数 (0=全件)")
    args = parser.parse_args()

    dry_run = args.dry_run
    max_items = args.max if args.max > 0 else 999999

    if dry_run:
        logger.info("[health] === DRY-RUN モード ===")

    seller_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "").strip()
    if not seller_id:
        seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("[health] AMAZON_AU_SELLER_ID が設定されていません")
        sys.exit(1)

    state = _load_state()
    prev_warning_skus = set(state.get("last_violation_skus", []))
    prev_deleted_total = state.get("total_auto_deleted", 0)
    logger.info("[health] 前回状態: 注意SKU=%d件 / 累計自動削除=%d件",
                len(prev_warning_skus), prev_deleted_total)

    # 1. 全出品取得
    listings = _get_all_listings()
    if not listings:
        logger.info("[health] 出品なし。終了")
        return

    # 2. issues 全件確認
    danger_items, warning_items = _check_all_issues(listings, seller_id, max_items)
    now = datetime.now(timezone.utc).isoformat()

    # 3. 危険フラグ → 即時削除
    deleted, failed_items = 0, []
    if danger_items:
        logger.warning("[health] 危険フラグ %d件検出！自動削除開始...", len(danger_items))
        deleted, failed_items = _delete_items(danger_items, seller_id, dry_run)
        _send_danger_alert(danger_items, deleted, failed_items, dry_run, now)
    else:
        logger.info("[health] 危険フラグ（真贋・IP）: なし ✓")

    # 4. 注意フラグ → 前回比増加時のみ通知
    current_warning_skus = {v["sku"] for v in warning_items}
    new_warnings = [v for v in warning_items if v["sku"] not in prev_warning_skus]
    if warning_items:
        logger.info("[health] 注意フラグ: %d件 (前回比 新規%d件)", len(warning_items), len(new_warnings))
        _send_warning_alert(warning_items, new_warnings, now)
    else:
        logger.info("[health] 注意フラグ: なし ✓")

    # 5. 状態保存
    new_deleted_total = prev_deleted_total + deleted
    _save_state({
        "last_check": now,
        "last_error_count": len(warning_items),
        "last_violation_skus": list(current_warning_skus),
        "last_danger_count": len(danger_items),
        "total_auto_deleted": new_deleted_total,
    })

    # 6. サマリーログ
    logger.info(
        "[health] === 完了 ===\n"
        "  チェック件数:     %d件\n"
        "  危険フラグ:       %d件 → %s%d件削除\n"
        "  注意フラグ:       %d件 (新規%d件)\n"
        "  累計自動削除:     %d件",
        min(len(listings), max_items),
        len(danger_items), "DRY-RUN " if dry_run else "", deleted,
        len(warning_items), len(new_warnings),
        new_deleted_total,
    )


if __name__ == "__main__":
    main()

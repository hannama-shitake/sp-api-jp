"""
violation_asins.json を読み込み、違反出品を一括削除するスクリプト。

ロジック:
  1. violation_asins.json をロード
  2. 削除対象SKUを一覧表示（確認ステップ）
  3. "yes" 入力で削除実行（ListingsItems DELETE）
  4. 結果（成功/失敗）をログ出力

使い方:
  python violation_deleter.py               # 確認後に削除実行
  python violation_deleter.py --dry-run     # 一覧表示のみ（削除しない）
"""
import argparse
import json
import os
import sys
import time

from sp_api.api import ListingsItems
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

LISTINGS_INTERVAL = 0.2  # 5 req/s


# ─────────────────────────────────────────────
# 1. violation_asins.json ロード
# ─────────────────────────────────────────────

def load_violations(path: str) -> list:
    """violation_asins.json を読み込んで返す。"""
    if not os.path.exists(path):
        logger.error("[violation_deleter] %s が見つかりません。先に violation_finder.py を実行してください。", path)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        logger.error("[violation_deleter] violation_asins.json の形式が正しくありません。")
        sys.exit(1)
    return data


# ─────────────────────────────────────────────
# 2. 一括削除
# ─────────────────────────────────────────────

def delete_violations(violations: list, dry_run: bool = False) -> None:
    """violations リストの各SKUを削除する。"""
    if not _SELLER_ID:
        logger.error("[violation_deleter] AMAZON_AU_SELLER_ID が未設定です。.env に追加してください。")
        sys.exit(1)

    api = ListingsItems(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    total = len(violations)
    success_count = 0
    fail_count = 0

    for idx, item in enumerate(violations, 1):
        sku = item.get("sku", "")
        asin = item.get("asin", "")
        issues = item.get("issues", [])
        codes = [i.get("code", "?") for i in issues]

        if not sku:
            logger.warning("[violation_deleter] SKUが空のエントリをスキップ (ASIN: %s)", asin)
            continue

        logger.info(
            "[violation_deleter] [%d/%d] 削除: SKU=%s / ASIN=%s / issues=%s",
            idx, total, sku, asin, codes,
        )

        if dry_run:
            logger.info("[violation_deleter] --dry-run のためスキップ")
            continue

        try:
            resp = api.delete_listings_item(
                sellerId=_SELLER_ID,
                sku=sku,
                marketplaceIds=[MARKETPLACE_AU],
            )
            status = resp.payload.get("status", "")
            if status == "ACCEPTED":
                logger.info("[violation_deleter] 削除成功: SKU=%s", sku)
                success_count += 1
            else:
                resp_issues = resp.payload.get("issues", [])
                msg = "; ".join(i.get("message", "") for i in resp_issues)
                logger.warning("[violation_deleter] 削除警告 (SKU=%s): status=%s %s", sku, status, msg)
                # ACCEPTED 以外でも処理は継続（サーバー側の非同期）
                success_count += 1

        except SellingApiException as e:
            logger.error("[violation_deleter] 削除APIエラー (SKU=%s): %s", sku, e)
            fail_count += 1
        except Exception as e:
            logger.error("[violation_deleter] 削除例外 (SKU=%s): %s", sku, e)
            fail_count += 1

        time.sleep(LISTINGS_INTERVAL)

    if not dry_run:
        logger.info(
            "[violation_deleter] 完了: 成功=%d件 / 失敗=%d件 / 全体=%d件",
            success_count, fail_count, total,
        )


# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="違反出品を一括削除する")
    parser.add_argument("--dry-run", action="store_true", help="一覧表示のみ、削除しない")
    args = parser.parse_args()

    violation_path = os.path.join(os.path.dirname(__file__), "violation_asins.json")
    violations = load_violations(violation_path)

    if not violations:
        logger.info("[violation_deleter] 削除対象がありません")
        return

    # ── 削除対象一覧を表示 ──
    print("\n" + "=" * 60)
    print(f"削除対象: {len(violations)}件")
    print("=" * 60)
    for item in violations:
        sku = item.get("sku", "N/A")
        asin = item.get("asin", "N/A")
        issues = item.get("issues", [])
        codes = [i.get("code", "?") for i in issues]
        print(f"  SKU={sku}  ASIN={asin}  issues={codes}")
    print("=" * 60 + "\n")

    if args.dry_run:
        logger.info("[violation_deleter] --dry-run モード: 削除は実行しません")
        delete_violations(violations, dry_run=True)
        return

    # ── 確認プロンプト ──
    try:
        answer = input("続行しますか？ (yes/no): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\nキャンセルされました")
        sys.exit(0)

    if answer != "yes":
        logger.info("[violation_deleter] キャンセルされました")
        sys.exit(0)

    delete_violations(violations, dry_run=False)


if __name__ == "__main__":
    main()

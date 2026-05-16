"""
listings_sync.py
Amazon AU出品一覧をDBの listings テーブルに同期するスクリプト。

【目的】
  - Reports API で全AU出品 (~382件) を取得して listings テーブルにUPSERT
  - price_update / no_jp_optimizer が Reports API を呼ばずに DB から参照できるようにする
  - DB未登録の出品（手動出品・古い出品）も一括登録

【使い方】
  python listings_sync.py              # 同期実行
  python listings_sync.py --dry-run   # DB変更なし（確認のみ）
  python listings_sync.py --stats     # DB統計表示のみ（API不使用）

【GitHub Actions での使用】
  毎日 or price_update の前に実行することで DB を常に最新化できる。
  Reports API は生成に 2-10 分かかるため、単独ジョブとして事前実行推奨。
"""
import csv
import gzip
import io
import os
import sqlite3
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
os.chdir(os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

import requests as _req
from sp_api.api import Reports
from sp_api.base import Marketplaces

import config
from db.database import init_db, upsert_listing, get_active_listings
from utils.logger import get_logger

logger = get_logger(__name__)

_DRY_RUN = "--dry-run" in sys.argv
_STATS_ONLY = "--stats" in sys.argv

_AU = {
    "refresh_token":    config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":       config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}


# ─────────────────────────────────────────────────────────────
# 1. Reports API から全AU出品を取得
# ─────────────────────────────────────────────────────────────
def fetch_all_au_listings() -> list:
    """
    Reports API (GET_MERCHANT_LISTINGS_ALL_DATA) で全AU出品を取得する。
    active / inactive 両方を返す。削除済みは除外。
    戻り値: [{"asin", "sku", "status", "current_price_aud", "title"}, ...]
    """
    api = Reports(credentials=_AU, marketplace=Marketplaces.AU)

    logger.info("[sync] 出品レポートをリクエスト中...")
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    report_id = resp.payload["reportId"]
    logger.info("[sync] レポートID: %s", report_id)

    for attempt in range(120):
        time.sleep(10)
        status_resp = api.get_report(report_id)
        status = status_resp.payload.get("processingStatus", "")
        if attempt % 6 == 0:
            logger.info("[sync] レポートステータス: %s (%d/120)", status, attempt + 1)
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

    r = _req.get(url, timeout=60)
    r.raise_for_status()
    content = gzip.decompress(r.content) if compression == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    listings = []
    seen = set()
    for row in reader:
        asin  = row.get("asin1", "").strip()
        sku   = row.get("seller-sku", "").strip()
        st    = row.get("status", "").strip().lower()
        title = row.get("item-name", "").strip()
        price_str = row.get("price", "").strip()
        try:
            price = float(price_str) if price_str else None
        except ValueError:
            price = None

        # 削除済みは除外、ASIN10桁チェック
        if asin and len(asin) == 10 and sku and asin not in seen and st != "deleted":
            seen.add(asin)
            listings.append({
                "asin": asin,
                "sku": sku,
                "status": st,  # "active" or "inactive"
                "current_price_aud": price,
                "title": title,
            })

    active   = sum(1 for l in listings if l["status"] == "active")
    inactive = sum(1 for l in listings if l["status"] != "active")
    logger.info("[sync] 出品取得完了: %d件（active=%d, inactive=%d）", len(listings), active, inactive)
    return listings


# ─────────────────────────────────────────────────────────────
# 2. asin_candidates から title を補完
# ─────────────────────────────────────────────────────────────
def enrich_titles(listings: list) -> list:
    """
    title が空の出品に対して asin_candidates テーブルから title を補完する。
    """
    no_title = [l for l in listings if not l.get("title")]
    if not no_title:
        return listings

    with sqlite3.connect(config.DB_PATH) as conn:
        for l in no_title:
            row = conn.execute(
                "SELECT title FROM asin_candidates WHERE asin=?", (l["asin"],)
            ).fetchone()
            if row and row[0]:
                l["title"] = row[0]

    enriched = sum(1 for l in no_title if l.get("title"))
    logger.info("[sync] タイトル補完: %d/%d件（asin_candidates から）", enriched, len(no_title))
    return listings


# ─────────────────────────────────────────────────────────────
# 3. DB に同期
# ─────────────────────────────────────────────────────────────
def sync_to_db(listings: list, dry_run: bool = False) -> dict:
    """
    取得した出品一覧を listings テーブルに UPSERT する。
    戻り値: {"inserted": N, "updated": N, "unchanged": N, "total": N}
    """
    # 既存DBの状態確認
    with sqlite3.connect(config.DB_PATH) as conn:
        existing = {
            row[0]: {"status": row[1], "price": row[2]}
            for row in conn.execute(
                "SELECT sku, status, current_price_aud FROM listings"
            ).fetchall()
        }

    inserted = updated = unchanged = 0

    for l in listings:
        sku  = l["sku"]
        prev = existing.get(sku)

        if prev is None:
            # 新規
            if not dry_run:
                upsert_listing(
                    asin=l["asin"],
                    sku=sku,
                    status=l["status"],
                    current_price_aud=l["current_price_aud"],
                    title=l["title"] or None,
                )
            inserted += 1
        else:
            # 既存 → 変更があれば更新
            price_changed = (
                l["current_price_aud"] is not None
                and abs((l["current_price_aud"] or 0) - (prev["price"] or 0)) > 0.001
            )
            status_changed = prev["status"] != l["status"]

            if price_changed or status_changed:
                if not dry_run:
                    upsert_listing(
                        asin=l["asin"],
                        sku=sku,
                        status=l["status"],
                        current_price_aud=l["current_price_aud"],
                        title=l["title"] or None,
                    )
                updated += 1
            else:
                unchanged += 1

    # Reports API に存在しない sku を 'deleted' に変更
    api_skus = {l["sku"] for l in listings}
    marked_deleted = 0
    with sqlite3.connect(config.DB_PATH) as conn:
        active_in_db = conn.execute(
            "SELECT sku FROM listings WHERE status='active'"
        ).fetchall()
        for row in active_in_db:
            if row[0] not in api_skus:
                if not dry_run:
                    conn.execute(
                        "UPDATE listings SET status='deleted', updated_at=? WHERE sku=?",
                        (datetime.now().isoformat(), row[0]),
                    )
                marked_deleted += 1

    return {
        "total":   len(listings),
        "inserted": inserted,
        "updated":  updated,
        "unchanged": unchanged,
        "marked_deleted": marked_deleted,
    }


# ─────────────────────────────────────────────────────────────
# 4. DB統計表示
# ─────────────────────────────────────────────────────────────
def show_stats():
    with sqlite3.connect(config.DB_PATH) as conn:
        total    = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        active   = conn.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        inactive = conn.execute("SELECT COUNT(*) FROM listings WHERE status='inactive'").fetchone()[0]
        deleted  = conn.execute("SELECT COUNT(*) FROM listings WHERE status='deleted'").fetchone()[0]
        no_price = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE status='active' AND current_price_aud IS NULL"
        ).fetchone()[0]
        no_title = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE status='active' AND (title IS NULL OR title='')"
        ).fetchone()[0]
        products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        candidates = conn.execute("SELECT COUNT(*) FROM asin_candidates").fetchone()[0]

        # listings に products エントリがない件数
        orphan = conn.execute("""
            SELECT COUNT(*) FROM listings l
            WHERE l.status='active' AND NOT EXISTS (
                SELECT 1 FROM products p WHERE p.asin = l.asin
            )
        """).fetchone()[0]

    print("\n" + "="*60)
    print("【DB統計】")
    print(f"  listings (合計)  : {total}件")
    print(f"    active         : {active}件")
    print(f"    inactive       : {inactive}件")
    print(f"    deleted        : {deleted}件")
    print(f"  active で価格なし: {no_price}件")
    print(f"  active でタイトルなし: {no_title}件")
    print(f"  products テーブル: {products}件")
    print(f"  asin_candidates  : {candidates}件")
    print(f"  listings(active)のうちproductsに未登録: {orphan}件")
    print("="*60)


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────
def main():
    # DB初期化（スキーマ拡張も実行される）
    init_db()

    if _STATS_ONLY:
        show_stats()
        return

    mode = "DRY RUN（DB変更なし）" if _DRY_RUN else "SYNC（DB更新あり）"
    print(f"\n[listings_sync] モード: {mode}")
    logger.info("[sync] 開始 mode=%s", mode)

    # 1. Reports API から全AU出品を取得
    listings = fetch_all_au_listings()

    # 2. asin_candidates からタイトル補完
    listings = enrich_titles(listings)

    # 3. DB に同期
    result = sync_to_db(listings, dry_run=_DRY_RUN)

    print(f"\n[listings_sync] 同期完了")
    print(f"  API取得件数      : {result['total']}件")
    print(f"  新規登録         : {result['inserted']}件")
    print(f"  更新（価格/状態）: {result['updated']}件")
    print(f"  変更なし         : {result['unchanged']}件")
    print(f"  削除マーク       : {result['marked_deleted']}件（DBにあるがAPIに存在しない）")

    if _DRY_RUN:
        print("\n※ ドライランのため DB は変更されていません。--dry-run を外して再実行してください。")

    # 4. 統計表示
    show_stats()

    logger.info("[sync] 完了: 取得%d / 新規%d / 更新%d / 変更なし%d / 削除マーク%d",
                result["total"], result["inserted"], result["updated"],
                result["unchanged"], result["marked_deleted"])


if __name__ == "__main__":
    main()

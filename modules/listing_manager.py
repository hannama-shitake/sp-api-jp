import csv
import gzip
import io
import time
from typing import List, Optional, Set
from datetime import datetime, timezone

import requests as _requests
from sp_api.api import Reports
from sp_api.base import Marketplaces

from modules.profit_calc import ProfitResult, calc_optimal_au_price
from apis import amazon_au
from db.database import get_connection
import config
from utils.logger import get_logger

logger = get_logger(__name__)


def get_existing_asins_from_amazon() -> Set[str]:
    """
    Reports API で Amazon AU の実際の出品状況を取得し、
    既知 ASIN セット（active + inactive、deleted 除外）を返す。
    GitHub Actions 環境でも動作（ローカル DB に依存しない）。
    """
    _AU_CREDS = {
        "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
        "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
        "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
    }
    api = Reports(credentials=_AU_CREDS, marketplace=Marketplaces.AU)

    logger.info("[listing] Amazon AU 既存出品レポート取得中...")
    try:
        resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
        report_id = resp.payload["reportId"]

        for attempt in range(120):
            time.sleep(10)
            status_resp = api.get_report(report_id)
            status = status_resp.payload.get("processingStatus", "")
            if attempt % 6 == 0:
                logger.info("[listing] レポートステータス: %s (%d/120)", status, attempt + 1)
            if status == "DONE":
                break
            if status in ("FATAL", "CANCELLED"):
                logger.error("[listing] レポート失敗: %s", status)
                return set()
        else:
            logger.error("[listing] レポートタイムアウト（20分）")
            return set()

        doc_id = status_resp.payload["reportDocumentId"]
        doc_resp = api.get_report_document(doc_id)
        url = doc_resp.payload["url"]
        compression = doc_resp.payload.get("compressionAlgorithm", "")

        r = _requests.get(url, timeout=60)
        r.raise_for_status()
        content = gzip.decompress(r.content) if compression == "GZIP" else r.content

        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")

        known: Set[str] = set()
        for row in reader:
            asin = row.get("asin1", "").strip()
            item_status = row.get("status", "").strip().lower()
            if asin and len(asin) == 10 and item_status != "deleted":
                known.add(asin)

        logger.info("[listing] Amazon AU 既存ASIN: %d件", len(known))
        return known

    except Exception as e:
        logger.error("[listing] 既存ASIN取得エラー: %s", e)
        return set()


def list_profitable_products(
    profitable_products: List[ProfitResult],
    dry_run: bool = False,
    existing_asins: Optional[Set[str]] = None,
) -> dict:
    """
    利益率基準を満たした商品を Amazon AU に FBM 相乗り出品する。

    Args:
        profitable_products: product_matcher から取得した利益商品リスト
        dry_run: True の場合は出品せずプレビューのみ表示
        existing_asins: 呼び出し元で取得済みの既存 ASIN セット（省略時は自動取得）

    Returns:
        {"success": int, "skipped": int, "failed": int}
    """
    # 既存出品を Amazon Reports API から取得（ローカル DB は GitHub Actions で消えるため）
    if existing_asins is None:
        existing_asins = get_existing_asins_from_amazon()

    success = skipped = failed = 0

    for result in profitable_products:
        asin = result.asin

        # Amazon 実態ベースで重複チェック
        if asin in existing_asins:
            logger.info("[listing] %s: すでに出品中。スキップ", asin)
            skipped += 1
            continue

        price_to_list = result.au_price_aud
        if result.recommended_au_price_aud and price_to_list < result.recommended_au_price_aud:
            price_to_list = result.recommended_au_price_aud
            logger.info(
                "[listing] %s: AU価格が低いため推奨価格 AUD %.2f に調整",
                asin, price_to_list,
            )

        logger.info(
            "[listing] %s %s | AU $%.2f | 粗利率 %.1f%%",
            "[DRY-RUN]" if dry_run else "出品:",
            asin, price_to_list, result.profit_rate,
        )

        if dry_run:
            success += 1
            continue

        ok, msg = amazon_au.list_item_fbm(asin=asin, price_aud=price_to_list)
        if ok:
            _save_listing(asin=asin, sku=msg)
            existing_asins.add(asin)  # 今回出品分も即時追加して重複を防ぐ
            success += 1
        else:
            logger.error("[listing] %s: 出品失敗 - %s", asin, msg)
            failed += 1

    logger.info(
        "[listing] 完了: 成功 %d / スキップ %d / 失敗 %d",
        success, skipped, failed,
    )
    return {"success": success, "skipped": skipped, "failed": failed}


def _is_already_listed(asin: str) -> bool:
    """ローカル DB チェック（後方互換用。新規コードは existing_asins を使うこと）"""
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM listings WHERE asin = ? AND status = 'active'", (asin,)
    ).fetchone()
    conn.close()
    return row is not None


def _save_listing(asin: str, sku: str):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute("""
            INSERT INTO listings (asin, sku, platform, status, listed_at)
            VALUES (?, ?, 'amazon_au', 'active', ?)
            ON CONFLICT(sku) DO UPDATE SET
                status     = 'active',
                updated_at = ?
        """, (asin, sku, now, now))
    conn.close()


def pause_listing(asin: str):
    """在庫切れ時に出品を一時停止（在庫=0）"""
    sku = _get_sku(asin)
    if not sku:
        return

    ok, msg = amazon_au.update_quantity(sku=sku, quantity=0)
    if ok:
        _update_listing_status(asin, "paused")
        logger.info("[listing] %s: 出品一時停止（在庫0）", asin)
    else:
        logger.error("[listing] %s: 在庫更新失敗 - %s", asin, msg)


def resume_listing(asin: str, price_aud: float):
    """在庫復活時に出品を再開"""
    sku = _get_sku(asin)
    if not sku:
        return

    ok, _ = amazon_au.update_quantity(sku=sku, quantity=1)
    if ok:
        amazon_au.update_price(sku=sku, price_aud=price_aud)
        _update_listing_status(asin, "active")
        logger.info("[listing] %s: 出品再開 AUD %.2f", asin, price_aud)


def update_listing_price(asin: str, new_price_aud: float):
    """価格変動に応じて AU 出品価格を更新"""
    sku = _get_sku(asin)
    if not sku:
        return

    ok, msg = amazon_au.update_price(sku=sku, price_aud=new_price_aud)
    if ok:
        _update_listing_updated_at(asin)
        logger.info("[listing] %s: 価格更新 AUD %.2f", asin, new_price_aud)
    else:
        logger.error("[listing] %s: 価格更新失敗 - %s", asin, msg)


def _get_sku(asin: str):
    conn = get_connection()
    row = conn.execute(
        "SELECT sku FROM listings WHERE asin = ? ORDER BY id DESC LIMIT 1", (asin,)
    ).fetchone()
    conn.close()
    return row["sku"] if row else None


def _update_listing_status(asin: str, status: str):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            "UPDATE listings SET status = ?, updated_at = ? WHERE asin = ?",
            (status, now, asin),
        )
    conn.close()


def _update_listing_updated_at(asin: str):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            "UPDATE listings SET updated_at = ? WHERE asin = ?", (now, asin)
        )
    conn.close()

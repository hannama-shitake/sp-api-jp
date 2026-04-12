from typing import List
from datetime import datetime, timezone
from modules.profit_calc import ProfitResult, calc_optimal_au_price
from apis import amazon_au
from db.database import get_connection
from utils.logger import get_logger

logger = get_logger(__name__)


def list_profitable_products(
    profitable_products: List[ProfitResult],
    dry_run: bool = False,
) -> dict:
    """
    利益率基準を満たした商品を Amazon AU に FBM 相乗り出品する。

    Args:
        profitable_products: product_matcher から取得した利益商品リスト
        dry_run: True の場合は出品せずプレビューのみ表示

    Returns:
        {"success": int, "skipped": int, "failed": int}
    """
    success = skipped = failed = 0

    for result in profitable_products:
        asin = result.asin

        # すでに active 出品中かチェック
        if _is_already_listed(asin):
            logger.info("[listing] %s: すでに出品中。スキップ", asin)
            skipped += 1
            continue

        # AU 出品価格 = JP仕入値から最適価格を計算
        # すでに result.au_price_aud は AU の競合価格だが、
        # 最低でも推奨価格以上で出品する
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

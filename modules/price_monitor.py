from datetime import datetime, timezone
from apis import amazon_jp
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_profit, calc_optimal_au_price
from modules import listing_manager
from db.database import get_connection
import config
from utils.logger import get_logger

logger = get_logger(__name__)


def run_price_check():
    """
    出品中の全商品の JP 価格を再チェックし、必要に応じて AU 価格を更新する。
    """
    active_listings = _get_active_listings()
    if not active_listings:
        logger.info("[price_monitor] 出品中の商品なし")
        return

    exchange_rate = get_jpy_to_aud()
    logger.info("[price_monitor] %d件の価格チェック開始 (1 JPY = %.6f AUD)", len(active_listings), exchange_rate)

    updated = skipped = 0

    for row in active_listings:
        asin = row["asin"]
        current_jp_price = row["jp_price_jpy"]
        current_au_price = row["au_price_aud"]

        jp_product = amazon_jp.get_jp_product(asin)
        if not jp_product:
            logger.warning("[price_monitor] %s: JP情報取得失敗。スキップ", asin)
            continue

        new_jp_price = jp_product.get("price_jpy")
        if not new_jp_price:
            continue

        if new_jp_price == current_jp_price:
            logger.debug("[price_monitor] %s: JP価格変化なし (¥%d)", asin, new_jp_price)
            skipped += 1
            continue

        change_pct = abs(new_jp_price - current_jp_price) / current_jp_price * 100
        logger.info(
            "[price_monitor] %s: JP価格変動 ¥%d → ¥%d (%.1f%%)",
            asin, current_jp_price, new_jp_price, change_pct,
        )

        if change_pct < config.PRICE_UPDATE_THRESHOLD:
            logger.debug("[price_monitor] %s: 変動が閾値未満 (%.1f%% < %.1f%%)。スキップ",
                         asin, change_pct, config.PRICE_UPDATE_THRESHOLD)
            skipped += 1
            continue

        new_au_price = calc_optimal_au_price(new_jp_price)
        listing_manager.update_listing_price(asin=asin, new_price_aud=new_au_price)
        _update_product_price(asin, new_jp_price, new_au_price, exchange_rate)
        _record_price_history(asin, new_au_price, new_jp_price, exchange_rate)
        updated += 1

    logger.info("[price_monitor] 価格チェック完了: 更新 %d件 / スキップ %d件", updated, skipped)


def run_stock_check():
    """
    出品中の全商品の JP 在庫を確認し、在庫切れなら AU 出品を一時停止する。
    在庫復活なら AU 出品を再開する。
    """
    active_listings = _get_active_listings()
    paused_listings = _get_paused_listings()
    all_listings = list(active_listings) + list(paused_listings)

    if not all_listings:
        logger.info("[stock_monitor] 出品商品なし")
        return

    logger.info("[stock_monitor] %d件の在庫チェック開始", len(all_listings))
    paused = resumed = 0

    for row in all_listings:
        asin = row["asin"]
        is_currently_active = row["status"] == "active"

        jp_product = amazon_jp.get_jp_product(asin)
        if not jp_product:
            continue

        jp_in_stock = jp_product.get("in_stock", False)
        jp_price = jp_product.get("price_jpy")

        if is_currently_active and not jp_in_stock:
            logger.info("[stock_monitor] %s: JP在庫切れ → AU出品を一時停止", asin)
            listing_manager.pause_listing(asin)
            _update_product_stock(asin, False)
            paused += 1

        elif not is_currently_active and jp_in_stock and jp_price:
            new_au_price = calc_optimal_au_price(jp_price)
            logger.info("[stock_monitor] %s: JP在庫復活 → AU出品を再開 AUD %.2f", asin, new_au_price)
            listing_manager.resume_listing(asin=asin, price_aud=new_au_price)
            _update_product_stock(asin, True)
            resumed += 1

    logger.info("[stock_monitor] 在庫チェック完了: 停止 %d件 / 再開 %d件", paused, resumed)


def _get_active_listings():
    conn = get_connection()
    rows = conn.execute("""
        SELECT l.asin, l.status, p.jp_price_jpy, p.au_price_aud, p.profit_rate
        FROM listings l
        LEFT JOIN products p ON l.asin = p.asin
        WHERE l.status = 'active' AND l.platform = 'amazon_au'
    """).fetchall()
    conn.close()
    return rows


def _get_paused_listings():
    conn = get_connection()
    rows = conn.execute("""
        SELECT l.asin, l.status, p.jp_price_jpy, p.au_price_aud, p.profit_rate
        FROM listings l
        LEFT JOIN products p ON l.asin = p.asin
        WHERE l.status = 'paused' AND l.platform = 'amazon_au'
    """).fetchall()
    conn.close()
    return rows


def _update_product_price(asin: str, jp_price: int, au_price: float, exchange_rate: float):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute("""
            UPDATE products SET
                jp_price_jpy  = ?,
                au_price_aud  = ?,
                exchange_rate = ?,
                last_checked  = ?
            WHERE asin = ?
        """, (jp_price, au_price, exchange_rate, now, asin))
    conn.close()


def _update_product_stock(asin: str, in_stock: bool):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute(
            "UPDATE products SET jp_in_stock = ?, last_checked = ? WHERE asin = ?",
            (1 if in_stock else 0, now, asin),
        )
    conn.close()


def _record_price_history(asin: str, price_aud: float, price_jpy: int, exchange_rate: float):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute("""
            INSERT INTO price_history (asin, platform, price_aud, price_jpy, exchange_rate, recorded_at)
            VALUES (?, 'amazon_au', ?, ?, ?, ?)
        """, (asin, price_aud, price_jpy, exchange_rate, now))
    conn.close()

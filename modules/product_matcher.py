from typing import List, Dict, Optional
from datetime import datetime, timezone
import config
from apis import amazon_jp
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_profit, ProfitResult
from db.database import get_connection
from utils.logger import get_logger

logger = get_logger(__name__)


def match_and_research(au_products: List[Dict], dry_run: bool = False) -> List[ProfitResult]:
    """
    AU 商品リストから各 ASIN を JP で検索し、利益計算を行う。
    利益率が MIN_PROFIT_RATE 以上の商品のみ返す。

    Args:
        au_products: scraper から取得した商品リスト
        dry_run: True の場合 DB への保存をスキップ

    Returns:
        利益率基準を満たした ProfitResult のリスト
    """
    exchange_rate = get_jpy_to_aud()
    logger.info("[matcher] 為替レート: 1 JPY = %.6f AUD", exchange_rate)
    logger.info("[matcher] %d 件の商品を JP で照合します...", len(au_products))

    profitable: List[ProfitResult] = []
    not_found = 0
    below_threshold = 0

    for i, au_product in enumerate(au_products, 1):
        asin = au_product["asin"]
        au_price_aud = au_product.get("au_price_aud")

        if not au_price_aud or au_price_aud <= 0:
            logger.debug("[matcher] %s: AU価格なし。スキップ", asin)
            continue

        logger.debug("[matcher] [%d/%d] ASIN %s を JP で検索中...", i, len(au_products), asin)
        jp_product = amazon_jp.get_jp_product(asin)

        if not jp_product:
            not_found += 1
            logger.debug("[matcher] %s: JP に存在しません", asin)
            continue

        jp_price = jp_product.get("price_jpy")
        in_stock = jp_product.get("in_stock", False)

        if not jp_price or jp_price <= 0:
            logger.debug("[matcher] %s: JP価格なし。スキップ", asin)
            continue

        if not in_stock:
            logger.debug("[matcher] %s: JP在庫なし。スキップ", asin)
            continue

        weight_kg = jp_product.get("weight_kg")
        if weight_kg and weight_kg > 1.0:
            logger.debug("[matcher] %s: 重量 %.3fkg > 1kg。スキップ", asin, weight_kg)
            continue

        title = jp_product.get("title") or au_product.get("title", "")
        result = calc_profit(
            asin=asin,
            title=title,
            jp_price_jpy=jp_price,
            au_price_aud=au_price_aud,
            exchange_rate=exchange_rate,
            weight_kg=weight_kg,
        )

        logger.info(
            "[matcher] %s | JP ¥%d → AU $%.2f | 粗利率 %.1f%% (%s)",
            asin, jp_price, au_price_aud, result.profit_rate,
            "○" if result.is_profitable else "×",
        )

        if not dry_run:
            _save_product(result, in_stock)

        if result.is_profitable:
            profitable.append(result)
        else:
            below_threshold += 1

    logger.info(
        "[matcher] 完了: 利益あり %d件 / JP未存在 %d件 / 基準未達 %d件",
        len(profitable), not_found, below_threshold,
    )
    return profitable


def _save_product(result: ProfitResult, in_stock: bool):
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    with conn:
        conn.execute("""
            INSERT INTO products
                (asin, title, au_price_aud, jp_price_jpy, profit_jpy, profit_rate,
                 jp_in_stock, exchange_rate, last_checked)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asin) DO UPDATE SET
                title        = excluded.title,
                au_price_aud = excluded.au_price_aud,
                jp_price_jpy = excluded.jp_price_jpy,
                profit_jpy   = excluded.profit_jpy,
                profit_rate  = excluded.profit_rate,
                jp_in_stock  = excluded.jp_in_stock,
                exchange_rate = excluded.exchange_rate,
                last_checked = excluded.last_checked
        """, (
            result.asin, result.title, result.au_price_aud,
            result.jp_price_jpy, result.profit_jpy, result.profit_rate,
            1 if in_stock else 0, result.exchange_rate, now,
        ))
    conn.close()

"""
APScheduler を使ったバックグラウンド監視スケジューラー。
`python scheduler.py` で起動する。
"""

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
import config
from apis import exchange_rate as ex_rate_module
from modules import price_monitor
from utils.logger import get_logger

logger = get_logger(__name__)

scheduler = BlockingScheduler(timezone="Asia/Tokyo")


def job_refresh_exchange_rate():
    """為替レートキャッシュをリフレッシュ"""
    logger.info("[scheduler] 為替レート更新")
    ex_rate_module._cached_at = 0  # キャッシュ強制クリア
    rate = ex_rate_module.get_jpy_to_aud()
    logger.info("[scheduler] JPY→AUD: %.6f", rate)


def job_price_check():
    """出品中商品の JP 価格チェック → AU 価格自動更新"""
    logger.info("[scheduler] JP価格チェック開始")
    price_monitor.run_price_check()


def job_stock_check():
    """出品中商品の JP 在庫チェック → AU 在庫自動更新"""
    logger.info("[scheduler] JP在庫チェック開始")
    price_monitor.run_stock_check()


def start():
    scheduler.add_job(
        job_refresh_exchange_rate,
        trigger=IntervalTrigger(minutes=config.SCHEDULER_EXCHANGE_RATE_MINUTES),
        id="exchange_rate",
        name="為替レート更新",
        replace_existing=True,
    )
    scheduler.add_job(
        job_price_check,
        trigger=IntervalTrigger(hours=config.SCHEDULER_JP_PRICE_HOURS),
        id="price_check",
        name="JP価格チェック",
        replace_existing=True,
    )
    scheduler.add_job(
        job_stock_check,
        trigger=IntervalTrigger(hours=config.SCHEDULER_JP_STOCK_HOURS),
        id="stock_check",
        name="JP在庫チェック",
        replace_existing=True,
    )

    logger.info("スケジューラー起動")
    logger.info("  - 為替レート更新: %d分毎", config.SCHEDULER_EXCHANGE_RATE_MINUTES)
    logger.info("  - JP価格チェック: %d時間毎", config.SCHEDULER_JP_PRICE_HOURS)
    logger.info("  - JP在庫チェック: %d時間毎", config.SCHEDULER_JP_STOCK_HOURS)
    logger.info("Ctrl+C で停止")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("スケジューラー停止")


if __name__ == "__main__":
    start()

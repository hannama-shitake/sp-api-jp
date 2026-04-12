import sqlite3
import config
from utils.logger import get_logger

logger = get_logger(__name__)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_connection()
    with conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS products (
                asin            TEXT PRIMARY KEY,
                title           TEXT,
                au_price_aud    REAL,
                jp_price_jpy    INTEGER,
                profit_jpy      REAL,
                profit_rate     REAL,
                jp_in_stock     INTEGER DEFAULT 1,
                exchange_rate   REAL,
                last_checked    TEXT
            );

            CREATE TABLE IF NOT EXISTS listings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                asin        TEXT NOT NULL,
                sku         TEXT UNIQUE NOT NULL,
                platform    TEXT NOT NULL DEFAULT 'amazon_au',
                status      TEXT NOT NULL DEFAULT 'active',
                listed_at   TEXT NOT NULL,
                updated_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS price_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                asin            TEXT NOT NULL,
                platform        TEXT NOT NULL,
                price_aud       REAL,
                price_jpy       INTEGER,
                exchange_rate   REAL,
                recorded_at     TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_listings_asin ON listings(asin);
            CREATE INDEX IF NOT EXISTS idx_price_history_asin ON price_history(asin);
        """)
    conn.close()
    logger.info("Database initialized: %s", config.DB_PATH)

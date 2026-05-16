import sqlite3
from datetime import datetime
from typing import Optional
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
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                asin                TEXT NOT NULL,
                sku                 TEXT UNIQUE NOT NULL,
                platform            TEXT NOT NULL DEFAULT 'amazon_au',
                status              TEXT NOT NULL DEFAULT 'active',
                current_price_aud   REAL,
                title               TEXT,
                listed_at           TEXT NOT NULL,
                updated_at          TEXT
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
            CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
            CREATE INDEX IF NOT EXISTS idx_price_history_asin ON price_history(asin);
        """)
        # 既存DBへのカラム追加（ALTER TABLE は IF NOT EXISTS 非対応のため個別 try）
        _add_column_if_missing(conn, "listings", "current_price_aud", "REAL")
        _add_column_if_missing(conn, "listings", "title", "TEXT")
    conn.close()
    logger.info("Database initialized: %s", config.DB_PATH)


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, col_type: str):
    """テーブルにカラムが存在しない場合のみ ALTER TABLE ADD COLUMN を実行する"""
    existing = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        logger.info("DB migration: %s.%s (%s) を追加", table, column, col_type)


# ─────────────────────────────────────────────────────────────
# listings テーブルヘルパー
# ─────────────────────────────────────────────────────────────

def upsert_listing(asin: str, sku: str, status: str,
                   current_price_aud: Optional[float] = None,
                   title: Optional[str] = None,
                   platform: str = "amazon_au"):
    """
    listings テーブルに出品情報を UPSERT する。
    sku が存在する場合は status / current_price_aud / updated_at を更新。
    存在しない場合は新規挿入。
    """
    now = datetime.now().isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO listings (asin, sku, platform, status, current_price_aud, title, listed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sku) DO UPDATE SET
                status            = excluded.status,
                current_price_aud = COALESCE(excluded.current_price_aud, listings.current_price_aud),
                title             = COALESCE(excluded.title, listings.title),
                updated_at        = excluded.updated_at
            """,
            (asin, sku, platform, status, current_price_aud, title, now, now),
        )


def update_listing_price(sku: str, price_aud: float, status: str = "active"):
    """price_update / no_jp_optimizer から価格変更後に呼ぶ"""
    now = datetime.now().isoformat()
    with get_connection() as conn:
        conn.execute(
            "UPDATE listings SET current_price_aud=?, status=?, updated_at=? WHERE sku=?",
            (price_aud, status, now, sku),
        )


def mark_listing_deleted(sku: str):
    """出品削除時にステータスを 'deleted' へ"""
    now = datetime.now().isoformat()
    with get_connection() as conn:
        conn.execute(
            "UPDATE listings SET status='deleted', updated_at=? WHERE sku=?",
            (now, sku),
        )


def get_active_listings(platform: str = "amazon_au") -> list:
    """
    DB から active な出品一覧を返す。
    price_update / no_jp_optimizer が Reports API の代わりに使用できる。
    戻り値: [{"asin", "sku", "current_price_aud", "title"}, ...]
    """
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT asin, sku, current_price_aud, title FROM listings WHERE status='active' AND platform=?",
            (platform,),
        ).fetchall()
    return [dict(r) for r in rows]

"""
ASIN候補データベース管理。

catalog_discover.py で発掘した全ASINを arbitrage.db に蓄積し、
条件変化（価格下落・競合増加）を毎日再チェックして自動出品する。

テーブル: asin_candidates
  status の種類:
    candidate   - 未出品・条件待ち（毎日再チェック対象）
    listed      - 出品中
    ng          - NGワード/禁止カテゴリ（永久スキップ）
    restricted  - 認証必要（定期的に再試行）
"""
import sqlite3
from datetime import date, timedelta
from typing import Optional

import config

STATUS_CANDIDATE  = "candidate"
STATUS_LISTED     = "listed"
STATUS_NG         = "ng"
STATUS_RESTRICTED = "restricted"

_DDL = """
CREATE TABLE IF NOT EXISTS asin_candidates (
    asin              TEXT PRIMARY KEY,
    title             TEXT,
    weight_kg         REAL,
    first_seen        TEXT NOT NULL,
    last_checked      TEXT,
    last_jp_price     INTEGER,
    last_au_price     REAL,
    last_seller_count INTEGER,
    status            TEXT DEFAULT 'candidate',
    skip_reason       TEXT,
    check_count       INTEGER DEFAULT 0,
    listed_sku        TEXT
)
"""


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(config.DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    """テーブルを作成する（存在すれば何もしない）"""
    with _conn() as conn:
        conn.execute(_DDL)


def upsert_candidates(asins: list, skip_reason: str = "") -> int:
    """
    ASINリストを候補として一括登録する（既存は変更しない）。
    Returns: 新規追加件数
    """
    today = str(date.today())
    added = 0
    with _conn() as conn:
        for asin in asins:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO asin_candidates
                       (asin, first_seen, status, skip_reason, check_count)
                       VALUES (?, ?, ?, ?, 0)""",
                    (asin, today, STATUS_CANDIDATE, skip_reason),
                )
                if conn.execute(
                    "SELECT changes()"
                ).fetchone()[0]:
                    added += 1
            except Exception:
                pass
    return added


def update_candidate(
    asin: str,
    *,
    title: Optional[str] = None,
    weight_kg: Optional[float] = None,
    jp_price: Optional[int] = None,
    au_price: Optional[float] = None,
    seller_count: Optional[int] = None,
    status: Optional[str] = None,
    skip_reason: Optional[str] = None,
    listed_sku: Optional[str] = None,
):
    """候補レコードを部分更新する"""
    today = str(date.today())
    fields = ["last_checked = ?", "check_count = check_count + 1"]
    vals: list = [today]

    if title is not None:
        fields.append("title = ?"); vals.append(title)
    if weight_kg is not None:
        fields.append("weight_kg = ?"); vals.append(weight_kg)
    if jp_price is not None:
        fields.append("last_jp_price = ?"); vals.append(jp_price)
    if au_price is not None:
        fields.append("last_au_price = ?"); vals.append(au_price)
    if seller_count is not None:
        fields.append("last_seller_count = ?"); vals.append(seller_count)
    if status is not None:
        fields.append("status = ?"); vals.append(status)
    if skip_reason is not None:
        fields.append("skip_reason = ?"); vals.append(skip_reason)
    if listed_sku is not None:
        fields.append("listed_sku = ?"); vals.append(listed_sku)

    vals.append(asin)
    with _conn() as conn:
        conn.execute(
            f"UPDATE asin_candidates SET {', '.join(fields)} WHERE asin = ?",
            vals,
        )


def get_candidates(
    status: str = STATUS_CANDIDATE,
    limit: int = 1000,
    skip_checked_today: bool = True,
) -> list:
    """
    再チェック対象候補を返す。
    skip_checked_today=True のとき、今日すでにチェック済みのASINを除外する。
    """
    today = str(date.today())
    with _conn() as conn:
        if skip_checked_today:
            rows = conn.execute(
                """SELECT * FROM asin_candidates
                   WHERE status = ?
                     AND (last_checked IS NULL OR last_checked < ?)
                   ORDER BY last_checked ASC
                   LIMIT ?""",
                (status, today, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM asin_candidates WHERE status = ? LIMIT ?",
                (status, limit),
            ).fetchall()
    return [dict(r) for r in rows]


def get_checked_today_asins() -> set:
    """今日すでにチェックしたASINセットを返す（catalog_discoverの重複チェック回避用）"""
    today = str(date.today())
    with _conn() as conn:
        rows = conn.execute(
            "SELECT asin FROM asin_candidates WHERE last_checked = ?",
            (today,),
        ).fetchall()
    return {r["asin"] for r in rows}


def mark_listed_as_candidate(active_asins: set):
    """
    DBで listed だが実際のactive一覧にない → 出品が切れた → candidate に戻す。
    price_update が pause した場合などに対応。
    """
    today = str(date.today())
    with _conn() as conn:
        listed = conn.execute(
            "SELECT asin FROM asin_candidates WHERE status = ?",
            (STATUS_LISTED,),
        ).fetchall()
        for row in listed:
            if row["asin"] not in active_asins:
                conn.execute(
                    """UPDATE asin_candidates
                       SET status = ?, skip_reason = 'relisted_to_candidate',
                           last_checked = ?
                       WHERE asin = ?""",
                    (STATUS_CANDIDATE, today, row["asin"]),
                )


def get_stats() -> dict:
    """ステータス別件数を返す"""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM asin_candidates GROUP BY status"
        ).fetchall()
    return {r["status"]: r["cnt"] for r in rows}

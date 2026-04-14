"""
GmailのAmazon AU注文メール・仕入れ確認メールを解析して
Googleスプレッドシートに自動記録するスクリプト。

対応メール:
  - Amazon AU "Sold, ship now" → 注文行を追加
  - Amazon JP 注文確認        → 仕入れ価格を更新
  - 楽天市場 注文確認          → 仕入れ価格を更新
  - Yahoo!ショッピング 注文確認 → 仕入れ価格を更新

必要なGitHub Secrets:
  GMAIL_USER            : Gmailアドレス
  GMAIL_APP_PASSWORD    : Gmailアプリパスワード
  GOOGLE_SERVICE_ACCOUNT_JSON : サービスアカウントJSONの内容
  SPREADSHEET_ID        : スプレッドシートのID
"""

import imaplib
import email
import email.header
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

import gspread
from google.oauth2.service_account import Credentials
from apis.exchange_rate import get_jpy_to_aud
from utils.logger import get_logger

logger = get_logger(__name__)

# ── 設定 ──────────────────────────────────────────────────────
GMAIL_USER = (os.getenv("GMAIL_USER") or "").strip()
GMAIL_PASS = (os.getenv("GMAIL_APP_PASSWORD") or "").strip()
SA_JSON    = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEET_ID   = (os.getenv("SPREADSHEET_ID") or "").strip()

# 何日前までのメールを対象にするか
DAYS_BACK = int(os.getenv("GMAIL_DAYS_BACK", "3"))

# スプレッドシートのシート名（未指定なら当月タブ: 例 "4月"）
_now_jst = datetime.now(timezone(timedelta(hours=9)))
SHEET_NAME = os.getenv("SHEET_NAME") or f"{_now_jst.month}月"

# 列定義（1始まり）
COL = {
    "date":       1,   # 日付
    "order_id":   2,   # 注文番号
    "asin":       3,   # ASIN
    "url":        4,   # URL
    "status":     5,   # ステータス
    "source":     6,   # 仕入れ先
    "title":      7,   # 商品名
    "aud":        8,   # AUD
    "revenue_jpy":9,   # 入金(JPY)
    "cost_jpy":   10,  # 仕入(JPY)
    "ship_jpy":   11,  # 送料(JPY)
    "profit_jpy": 12,  # 粗利(JPY)
}

AU_FEE_RATE   = float(os.getenv("AU_FEE_RATE", "0.15"))
SHIP_JPY      = int(os.getenv("DHL_SHIPPING_JPY", "4500"))


# ── Gmail接続 ─────────────────────────────────────────────────

def connect_gmail() -> imaplib.IMAP4_SSL:
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(GMAIL_USER, GMAIL_PASS)
    return mail


def decode_header(raw: str) -> str:
    parts = email.header.decode_header(raw)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


def get_body(msg) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
            if ct == "text/html":
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""


def fetch_recent_emails(mail: imaplib.IMAP4_SSL, folder: str = "INBOX") -> list:
    mail.select(folder)
    since = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%d-%b-%Y")
    _, data = mail.search(None, f'SINCE "{since}"')
    ids = data[0].split()
    messages = []
    for uid in ids:
        _, raw = mail.fetch(uid, "(RFC822)")
        msg = email.message_from_bytes(raw[0][1])
        messages.append(msg)
    logger.info("[sheets] %s から %d件取得", folder, len(messages))
    return messages


# ── Amazon AU 注文メール解析 ───────────────────────────────────

def parse_amazon_au_order(msg) -> dict | None:
    """
    件名: "Sold, ship now: 商品名" または "Amazon.com.au - 出荷してください"
    """
    subject = decode_header(msg.get("Subject", ""))
    from_addr = msg.get("From", "")

    if "amazon" not in from_addr.lower():
        return None
    if "sold" not in subject.lower() and "ship now" not in subject.lower() and "出荷" not in subject:
        return None

    body = get_body(msg)

    # 注文番号
    order_id = ""
    m = re.search(r"Order\s*(?:ID|#|Number)[:\s#]*([0-9\-]{15,25})", body, re.IGNORECASE)
    if not m:
        m = re.search(r"([0-9]{3}-[0-9]{7}-[0-9]{7})", body)
    if m:
        order_id = m.group(1).strip()

    # ASIN
    asin = ""
    m = re.search(r"ASIN[:\s]*([A-Z0-9]{10})", body, re.IGNORECASE)
    if m:
        asin = m.group(1).strip()

    # 価格（AUD）
    aud_price = None
    m = re.search(r"(?:Item price|Price|Total)[:\s]*(?:AUD|A\$|AU\$)?\s*([\d,]+\.?\d*)", body, re.IGNORECASE)
    if m:
        try:
            aud_price = float(m.group(1).replace(",", ""))
        except ValueError:
            pass

    # 商品名（件名から）
    title = re.sub(r"^(Sold,?\s*ship\s*now[:\s]*|出荷してください[：:\s]*)", "", subject, flags=re.IGNORECASE).strip()

    # 日付
    date_str = ""
    try:
        dt = parsedate_to_datetime(msg.get("Date", ""))
        jst = dt.astimezone(timezone(timedelta(hours=9)))
        date_str = jst.strftime("%Y/%m/%d")
    except Exception:
        date_str = datetime.now().strftime("%Y/%m/%d")

    if not order_id:
        return None

    return {
        "type": "au_order",
        "date": date_str,
        "order_id": order_id,
        "asin": asin,
        "title": title,
        "aud_price": aud_price,
    }


# ── 仕入れ確認メール解析 ───────────────────────────────────────

def parse_purchase_email(msg) -> dict | None:
    """
    Amazon JP / 楽天 / Yahoo の購入確認メールから仕入れ価格を取得
    """
    subject = decode_header(msg.get("Subject", ""))
    from_addr = msg.get("From", "").lower()
    body = get_body(msg)

    source = None
    cost_jpy = None
    product_name = ""
    order_ref = ""

    # Amazon JP
    if "amazon.co.jp" in from_addr or ("amazon" in from_addr and "au" not in from_addr):
        if "注文" not in subject and "order" not in subject.lower():
            return None
        source = "amazon_jp"
        m = re.search(r"注文番号[:\s：]*([0-9\-]{15,25})", body)
        if m:
            order_ref = m.group(1).strip()
        m = re.search(r"(?:合計|小計|お支払い金額)[:\s：]*[¥￥]?\s*([\d,]+)\s*円?", body)
        if m:
            try:
                cost_jpy = int(m.group(1).replace(",", ""))
            except ValueError:
                pass
        m = re.search(r"商品名[:\s：]*(.*?)(?:\n|送料|数量)", body)
        if m:
            product_name = m.group(1).strip()

    # 楽天
    elif "rakuten" in from_addr or "楽天" in subject:
        if "注文" not in subject and "ご購入" not in subject:
            return None
        source = "rakuten"
        m = re.search(r"注文番号[:\s：]*([0-9\-]+)", body)
        if m:
            order_ref = m.group(1).strip()
        m = re.search(r"(?:合計金額|お支払い合計)[:\s：]*[¥￥]?\s*([\d,]+)\s*円?", body)
        if m:
            try:
                cost_jpy = int(m.group(1).replace(",", ""))
            except ValueError:
                pass
        m = re.search(r"商品名[:\s：]*(.*?)(?:\n|個数|数量)", body)
        if m:
            product_name = m.group(1).strip()

    # Yahoo!ショッピング
    elif "yahoo" in from_addr:
        if "注文" not in subject and "購入" not in subject:
            return None
        source = "yahoo"
        m = re.search(r"注文番号[:\s：]*([0-9\-]+)", body)
        if m:
            order_ref = m.group(1).strip()
        m = re.search(r"(?:合計|お支払い)[:\s：]*[¥￥]?\s*([\d,]+)\s*円?", body)
        if m:
            try:
                cost_jpy = int(m.group(1).replace(",", ""))
            except ValueError:
                pass

    if not source or not cost_jpy:
        return None

    return {
        "type": "purchase",
        "source": source,
        "cost_jpy": cost_jpy,
        "product_name": product_name,
        "order_ref": order_ref,
    }


# ── Google Sheets操作 ─────────────────────────────────────────

def connect_sheets():
    if not SA_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON未設定")
    sa_info = json.loads(SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.sheet1
    return ws


def get_existing_order_ids(ws) -> dict:
    """既存の注文番号→行番号マップを返す"""
    col_data = ws.col_values(COL["order_id"])
    return {v.strip(): i + 1 for i, v in enumerate(col_data) if v.strip()}


def calc_revenue_jpy(aud_price: float, exchange_rate: float) -> int:
    net_aud = aud_price * (1 - AU_FEE_RATE)
    return int(net_aud / exchange_rate)


def calc_profit(revenue_jpy: int, cost_jpy: int | None, ship_jpy: int) -> int | None:
    if cost_jpy is None:
        return None
    return revenue_jpy - cost_jpy - ship_jpy


def add_order_row(ws, order: dict, exchange_rate: float):
    """AU注文を新しい行として追加"""
    aud = order.get("aud_price")
    revenue_jpy = calc_revenue_jpy(aud, exchange_rate) if aud else ""

    asin = order.get("asin", "")
    au_url = f"https://www.amazon.com.au/dp/{asin}" if asin else ""

    row = [""] * 12
    row[COL["date"] - 1]       = order.get("date", "")
    row[COL["order_id"] - 1]   = order.get("order_id", "")
    row[COL["asin"] - 1]       = asin
    row[COL["url"] - 1]        = au_url
    row[COL["status"] - 1]     = "未発送"
    row[COL["source"] - 1]     = ""
    row[COL["title"] - 1]      = order.get("title", "")[:50]
    row[COL["aud"] - 1]        = aud or ""
    row[COL["revenue_jpy"] - 1] = revenue_jpy
    row[COL["cost_jpy"] - 1]   = ""
    row[COL["ship_jpy"] - 1]   = SHIP_JPY
    row[COL["profit_jpy"] - 1] = ""

    ws.append_row(row, value_input_option="USER_ENTERED")
    logger.info("[sheets] 注文追加: %s (%s)", order["order_id"], order.get("title", "")[:30])


def update_purchase_info(ws, purchase: dict, order_ids: dict, exchange_rate: float):
    """
    仕入れメールの情報で既存行を更新。
    商品名の部分一致でマッチングを試みる。
    """
    cost_jpy = purchase["cost_jpy"]
    product_name = purchase.get("product_name", "")
    source = purchase["source"]

    # 未入力の仕入れ行を探す
    all_values = ws.get_all_values()
    for i, row in enumerate(all_values):
        if len(row) < 12:
            continue
        existing_cost = row[COL["cost_jpy"] - 1].strip()
        existing_source = row[COL["source"] - 1].strip()
        title = row[COL["title"] - 1].strip()
        status = row[COL["status"] - 1].strip()

        # 仕入れ未入力 + 未発送 + タイトル部分一致（or 空でもOK）
        if existing_cost == "" and status in ("未発送", ""):
            if not product_name or any(w in title for w in product_name.split()[:3] if len(w) > 2):
                row_num = i + 1
                revenue_jpy_str = row[COL["revenue_jpy"] - 1].strip()
                revenue_jpy = int(revenue_jpy_str.replace(",", "")) if revenue_jpy_str else 0
                ship_jpy_str = row[COL["ship_jpy"] - 1].strip()
                ship_jpy = int(ship_jpy_str.replace(",", "")) if ship_jpy_str else SHIP_JPY
                profit = calc_profit(revenue_jpy, cost_jpy, ship_jpy)

                ws.update_cell(row_num, COL["source"], source)
                ws.update_cell(row_num, COL["cost_jpy"], cost_jpy)
                if profit is not None:
                    ws.update_cell(row_num, COL["profit_jpy"], profit)
                logger.info("[sheets] 仕入れ更新: 行%d %s ¥%d", row_num, title[:30], cost_jpy)
                return True

    logger.warning("[sheets] 仕入れ対象行が見つかりません: %s ¥%d", product_name[:30], cost_jpy)
    return False


# ── main ──────────────────────────────────────────────────────

def main():
    if not GMAIL_USER or not GMAIL_PASS:
        logger.error("GMAIL_USER / GMAIL_APP_PASSWORD未設定")
        sys.exit(1)
    if not SA_JSON or not SHEET_ID:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON / SPREADSHEET_ID未設定")
        sys.exit(1)

    exchange_rate = get_jpy_to_aud()
    logger.info("[sheets] 為替: 1 JPY = %.6f AUD", exchange_rate)

    # Gmail接続
    mail = connect_gmail()
    messages = fetch_recent_emails(mail)
    mail.logout()

    # Sheets接続
    ws = connect_sheets()
    order_ids = get_existing_order_ids(ws)

    au_orders = []
    purchases = []

    for msg in messages:
        # AU注文メール
        order = parse_amazon_au_order(msg)
        if order:
            au_orders.append(order)
            continue
        # 仕入れ確認メール
        purchase = parse_purchase_email(msg)
        if purchase:
            purchases.append(purchase)

    logger.info("[sheets] AU注文: %d件 / 仕入れ: %d件", len(au_orders), len(purchases))

    # AU注文を追加（重複チェック）
    added = 0
    for order in au_orders:
        oid = order["order_id"]
        if oid in order_ids:
            logger.debug("[sheets] 既存注文スキップ: %s", oid)
            continue
        add_order_row(ws, order, exchange_rate)
        order_ids[oid] = -1  # 追加済みマーク
        added += 1
        time.sleep(1)  # Sheets APIレート制限対策

    # 仕入れ情報を更新
    updated = 0
    for purchase in purchases:
        if update_purchase_info(ws, purchase, order_ids, exchange_rate):
            updated += 1
        time.sleep(1)

    logger.info("[sheets] 完了: 注文追加 %d件 / 仕入れ更新 %d件", added, updated)
    print(f"\n注文追加: {added}件 / 仕入れ更新: {updated}件")


if __name__ == "__main__":
    main()

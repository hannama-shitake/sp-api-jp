import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import gspread
from google.oauth2.service_account import Credentials

SA_FILE  = "emerald-ivy-446109-i5-41d73a3f2cee.json"
SHEET_ID = "17nSmtUnM3BlBJQkk2AaYTWKeY8g9ZNLP3oTeJYmnu2s"

scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet('4月')

# ── 計算値 ────────────────────────────────────
AUD       = 106.00
RATE      = 0.008780        # 1 JPY = 0.008780 AUD
AU_FEE    = 0.15
COST_JPY  = 5511
SHIP_JPY  = 4500

revenue_jpy = int(AUD * (1 - AU_FEE) / RATE)   # 10262
profit_jpy  = revenue_jpy - COST_JPY - SHIP_JPY # 251

TITLE = "Senkichi Bronze Award Chisels 3pcs (9/15/24mm)"
ASIN  = "B0029DP64G"
URL   = f"https://www.amazon.com.au/dp/{ASIN}"

print(f"入金(JPY) = {revenue_jpy}")
print(f"粗利(JPY) = {profit_jpy}")

# ── Row 10 を全消し（A〜N列まで） ─────────────
ws.update(range_name="A10:N10", values=[[""] * 14])
print("Row 10 クリア完了")

# ── 正しい列に書き込み（A〜L）────────────────
row_data = [
    "2026/04/13",          # A: 日付
    "250-3444139-7463004", # B: 注文番号
    ASIN,                  # C: ASIN
    URL,                   # D: URL
    "未発送",               # E: ステータス
    "楽天",                 # F: 仕入れ先
    TITLE,                 # G: 商品名
    AUD,                   # H: AUD
    revenue_jpy,           # I: 入金(JPY)
    COST_JPY,              # J: 仕入(JPY)
    SHIP_JPY,              # K: 送料(JPY)
    profit_jpy,            # L: 粗利(JPY)
]
ws.update(range_name="A10:L10", values=[row_data], value_input_option="USER_ENTERED")
print("Row 10 書き込み完了")
print(f"  日付: 2026/04/13")
print(f"  注文番号: 250-3444139-7463004")
print(f"  ASIN: {ASIN}")
print(f"  ステータス: 未発送")
print(f"  仕入れ先: 楽天")
print(f"  AUD: {AUD}")
print(f"  入金(JPY): {revenue_jpy:,}")
print(f"  仕入(JPY): {COST_JPY:,}")
print(f"  送料(JPY): {SHIP_JPY:,}")
print(f"  粗利(JPY): {profit_jpy:,}")

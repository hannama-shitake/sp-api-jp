import gspread
from google.oauth2.service_account import Credentials

SA_FILE  = "emerald-ivy-446109-i5-41d73a3f2cee.json"
SHEET_ID = "17nSmtUnM3BlBJQkk2AaYTWKeY8g9ZNLP3oTeJYmnu2s"

scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet("4\u6708")  # 4月

REVENUE_JPY = 10261
COST_JPY    = 5511
SHIP_JPY    = 3673   # 実費
profit      = REVENUE_JPY - COST_JPY - SHIP_JPY  # 1077

# K列（送料）= 11列、L列（粗利）= 12列、Row 10
ws.update_cell(10, 11, SHIP_JPY)
ws.update_cell(10, 12, profit)

with open("fix_ship_result.txt", "w", encoding="utf-8") as f:
    f.write(f"送料(K10): {SHIP_JPY}\n")
    f.write(f"粗利(L10): {profit}\n")
print("Done")

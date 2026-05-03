import gspread
from google.oauth2.service_account import Credentials

SA_FILE  = "emerald-ivy-446109-i5-41d73a3f2cee.json"
SHEET_ID = "17nSmtUnM3BlBJQkk2AaYTWKeY8g9ZNLP3oTeJYmnu2s"

scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet("4\u6708")  # 4月

row10 = ws.row_values(10)
with open("sheet_result.txt", "w", encoding="utf-8") as f:
    f.write("Row 10:\n")
    for i, v in enumerate(row10[:15]):
        col = chr(ord('A') + i)
        f.write(f"  {col}: {repr(v)}\n")
print("Done. See sheet_result.txt")

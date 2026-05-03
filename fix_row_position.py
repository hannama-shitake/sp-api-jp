import gspread
from google.oauth2.service_account import Credentials

SA_FILE  = "emerald-ivy-446109-i5-41d73a3f2cee.json"
SHEET_ID = "17nSmtUnM3BlBJQkk2AaYTWKeY8g9ZNLP3oTeJYmnu2s"

scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet("4\u6708")

# Row 10 のデータを取得
row10 = ws.row_values(10)
print("Row 10:", row10[:12])

# Row 2 に書き込む
ws.update(range_name="A2:L2", values=[row10[:12]], value_input_option="USER_ENTERED")

# Row 3〜10 をクリア（空行 + 旧データ行）
ws.batch_clear(["A3:L10"])

with open("fix_pos_result.txt", "w", encoding="utf-8") as f:
    f.write("Row 2 に移動完了\n")
    for i, v in enumerate(row10[:12]):
        col = chr(ord('A') + i)
        f.write(f"  {col}: {v}\n")
print("Done")

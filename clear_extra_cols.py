import gspread
from google.oauth2.service_account import Credentials

SA_FILE  = "emerald-ivy-446109-i5-41d73a3f2cee.json"
SHEET_ID = "17nSmtUnM3BlBJQkk2AaYTWKeY8g9ZNLP3oTeJYmnu2s"

scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet("4\u6708")  # 4月

# シートの実際の列数を確認
all_vals = ws.get_all_values()
max_col = max(len(row) for row in all_vals) if all_vals else 0
print(f"現在の最大列数: {max_col}")

# M列(13)以降を全クリア（行1〜28）
if max_col > 12:
    # M列〜最終列をクリア
    num_rows = len(all_vals)
    # 列インデックス13〜max_col をクリア
    # gspreadのclear_rangeを使う
    end_col_letter = gspread.utils.rowcol_to_a1(1, max_col)[:-1]  # 列文字だけ
    clear_range = f"M1:{end_col_letter}{num_rows}"
    ws.batch_clear([clear_range])
    print(f"クリア完了: {clear_range}")
else:
    print("M列以降に余分なデータなし")

with open("clear_result.txt", "w", encoding="utf-8") as f:
    f.write(f"最大列数: {max_col}\nクリア範囲: M1:{end_col_letter}{num_rows if max_col > 12 else '-'}\n")
print("Done")

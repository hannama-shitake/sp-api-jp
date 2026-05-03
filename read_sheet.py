import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import gspread
from google.oauth2.service_account import Credentials

scopes = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('emerald-ivy-446109-i5-41d73a3f2cee.json', scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key('17nSmtUnM3BlBJQkk2AaYTWKeY8g9ZNLP3oTeJYmnu2s')
ws = sh.worksheet('4月')
vals = ws.get_all_values()
print('行数:', len(vals))
for i, r in enumerate(vals):
    if any(c.strip() for c in r):
        print(f'Row {i+1}: {r[:15]}')

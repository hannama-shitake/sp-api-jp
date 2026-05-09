@echo off
cd /d C:\Users\user\Desktop\sp-api

:wait_net
ping -n 1 -w 3000 8.8.8.8 >nul 2>&1
if errorlevel 1 (
    timeout /t 10 /nobreak >nul
    goto wait_net
)

python bestseller_discover.py --max-new 200 --rank-limit 5000 --min-au-sellers 2 >> C:\Users\user\Desktop\sp-api\bestseller_discover.log 2>&1

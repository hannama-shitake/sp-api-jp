@echo off
cd /d C:\Users\user\Desktop\sp-api

:wait_net
ping -n 1 -w 3000 8.8.8.8 >nul 2>&1
if errorlevel 1 (
    timeout /t 10 /nobreak >nul
    goto wait_net
)

python ebay_jp_discover.py --max-new 500 --rank-limit 30000 --max-pages 3 >> C:\Users\user\Desktop\sp-api\ebay_jp_discover.log 2>&1

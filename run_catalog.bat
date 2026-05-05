@echo off
cd /d C:\Users\user\Desktop\sp-api

:wait_net
ping -n 1 -w 3000 8.8.8.8 >nul 2>&1
if errorlevel 1 (
    timeout /t 10 /nobreak >nul
    goto wait_net
)

python catalog_discover.py --max-pages 5 --max-new 100 --min-sellers 2 >> C:\Users\user\Desktop\sp-api\catalog_discover.log 2>&1

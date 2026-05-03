@echo off
cd /d C:\Users\user\Desktop\sp-api
python catalog_api_discover.py --max-new 100 --rank-limit 10000 --min-au-sellers 1 >> C:\Users\user\Desktop\sp-api\catalog_api_discover.log 2>&1

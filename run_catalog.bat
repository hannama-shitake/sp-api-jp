@echo off
cd /d C:\Users\user\Desktop\sp-api
python catalog_discover.py --max-pages 5 --max-new 100 --min-sellers 2 >> C:\Users\user\Desktop\sp-api\catalog_discover.log 2>&1

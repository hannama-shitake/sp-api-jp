@echo off
cd /d C:\Users\user\Desktop\sp-api
python cross_research.py --max-new 200 --top-brands 20 --top-nodes 10 --min-au-sellers 1 >> C:\Users\user\Desktop\sp-api\cross_research.log 2>&1

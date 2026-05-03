import requests, random, re
from bs4 import BeautifulSoup
import os, sys
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv()

PROXY_USER = os.getenv('PROXY_USER', '')
PROXY_PASS = os.getenv('PROXY_PASS', '')

def get_proxy():
    host, port = random.choice([('198.23.239.134', '6540'), ('107.172.163.27', '6543'), ('216.10.27.159', '6837')])
    return {'http': f'http://{PROXY_USER}:{PROXY_PASS}@{host}:{port}', 'https': f'http://{PROXY_USER}:{PROXY_PASS}@{host}:{port}'}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'en-AU,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

SELLER_ID = 'A3I4NYJKP3GMMP'
asin = 'B0DK6RS19P'
url = f'https://www.amazon.com.au/dp/{asin}?m={SELLER_ID}'

resp = requests.get(url, headers=headers, proxies=get_proxy(), timeout=15)
print('Status:', resp.status_code, 'Size:', len(resp.text))

soup = BeautifulSoup(resp.text, 'lxml')
title = soup.find('title')
print('Title:', title.get_text()[:100] if title else 'none')

# dollar amounts in raw HTML
dollars = re.findall(r'\$[\d,]+\.?\d*', resp.text)
print('Dollar amounts:', dollars[:15])

# HTMLを保存
with open('debug_dp.html', 'w', encoding='utf-8') as f:
    f.write(resp.text)
print('Saved to debug_dp.html')

# 特定のdivを確認
for sel in ['#availability', '#price', '#buybox', '#apex_desktop', '#centerCol']:
    el = soup.select_one(sel)
    if el:
        print(f'\n{sel}: {el.get_text(strip=True)[:100]}')

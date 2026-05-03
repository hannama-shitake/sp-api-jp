"""
Amazon AUの配送先をオーストラリアに設定してから価格取得するテスト
"""
import requests, random, re
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
load_dotenv()

PROXY_USER = os.getenv('PROXY_USER', '')
PROXY_PASS = os.getenv('PROXY_PASS', '')

PROXY_LIST = [
    ("198.23.239.134", "6540"),
    ("107.172.163.27", "6543"),
    ("216.10.27.159", "6837"),
    ("142.111.67.146", "5611"),
]

def get_proxy():
    host, port = random.choice(PROXY_LIST)
    return {'http': f'http://{PROXY_USER}:{PROXY_PASS}@{host}:{port}',
            'https': f'http://{PROXY_USER}:{PROXY_PASS}@{host}:{port}'}

BASE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'en-AU,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def set_au_location(session, postcode='2000'):
    """セッションにAU配送先（郵便番号）を設定する"""
    # まずトップページを取得してcookieを初期化
    proxies = get_proxy()
    resp = session.get('https://www.amazon.com.au', headers=BASE_HEADERS, proxies=proxies, timeout=15)

    # CSRF token取得
    soup = BeautifulSoup(resp.text, 'lxml')
    csrf = ''
    for inp in soup.select('input[name="anti-csrftoken-a2z"]'):
        csrf = inp.get('value', '')
        break

    # 配送先変更エンドポイント
    change_headers = {**BASE_HEADERS, 'x-requested-with': 'XMLHttpRequest',
                      'content-type': 'application/x-www-form-urlencoded'}
    data = {
        'locationType': 'LOCATION_INPUT',
        'zipCode': postcode,
        'storeContext': 'generic',
        'deviceType': 'web',
        'pageType': 'Search',
        'actionSource': 'glow',
        'anti-csrftoken-a2z': csrf,
    }
    r = session.post(
        'https://www.amazon.com.au/gp/delivery/ajax/address-change.html',
        headers=change_headers,
        data=data,
        proxies=proxies,
        timeout=15,
    )
    print(f'Location set status: {r.status_code}, response: {r.text[:100]}')
    return r.status_code == 200


SELLER_ID = 'A3I4NYJKP3GMMP'
session = requests.Session()
proxies = get_proxy()

print('配送先をAU(2000)に設定中...')
set_au_location(session, '2000')

# セラーストアページを取得
print('\nセラーストア取得中...')
url = f'https://www.amazon.com.au/s?me={SELLER_ID}&marketplaceID=A39IBJ37TRP1C6'
resp = session.get(url, headers=BASE_HEADERS, proxies=proxies, timeout=15)
soup = BeautifulSoup(resp.text, 'lxml')

items = soup.select('div[data-asin]')
print(f'商品数: {len(items)}')

for item in items[:10]:
    asin = item.get('data-asin', '').strip()
    if not asin or len(asin) != 10:
        continue

    offscreen = item.select_one('span.a-price span.a-offscreen')
    price_whole = item.select_one('.a-price-whole')
    unavail = 'unavailable' in str(item).lower()

    price = offscreen.get_text(strip=True) if offscreen else None
    if not price and price_whole:
        price = price_whole.get_text(strip=True)

    print(f'{asin}: price={price!r}, unavail={unavail}')

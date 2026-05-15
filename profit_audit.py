"""収益性フル監査 — 生き残り商品・利益構造・改善ポイントを全出力"""
import sys, os, time, io, csv, sqlite3
sys.path.insert(0, os.path.dirname(__file__))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
os.chdir(os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

from sp_api.api import Products, Reports
from sp_api.base import Marketplaces, SellingApiException
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_profit, calc_optimal_au_price, get_shipping_jpy
import config, requests as _req, gzip

_AU = {"refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
       "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
       "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"]}
_JP = {"refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
       "lwa_app_id":    config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
       "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"]}

rate = get_jpy_to_aud()
print(f"為替: 1 JPY = {rate:.5f} AUD\n")

# ── 1. AU出品一覧（Reports API）─────────────────────────────
print("=== AU出品一覧を取得中 ===")
rapi = Reports(credentials=_AU, marketplace=Marketplaces.AU)
resp = rapi.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
rid = resp.payload["reportId"]
for i in range(120):
    time.sleep(5)
    s = rapi.get_report(rid)
    st = s.payload.get("processingStatus","")
    if st == "DONE": break
    if st in ("FATAL","CANCELLED"): sys.exit(1)
doc = rapi.get_report_document(s.payload["reportDocumentId"])
url = doc.payload["url"]; comp = doc.payload.get("compressionAlgorithm","")
r = _req.get(url, timeout=60); r.raise_for_status()
content = gzip.decompress(r.content) if comp=="GZIP" else r.content
text = content.decode("utf-8","replace")
reader = csv.DictReader(io.StringIO(text), delimiter="\t")
listings = []
for row in reader:
    asin = row.get("asin1","").strip()
    sku  = row.get("seller-sku","").strip()
    st   = row.get("status","").strip().lower()
    title= row.get("item-name","").strip()
    try: price = float(row.get("price","0") or 0)
    except: price = 0.0
    if asin and len(asin)==10 and st != "deleted":
        listings.append({"asin":asin,"sku":sku,"status":st,"price":price,"title":title})

print(f"AU出品: {len(listings)}件 (active={sum(1 for l in listings if l['status']=='active')})\n")

# ── 2. JP価格一括取得 ────────────────────────────────────────
print("=== JP価格確認中 ===")
papi_jp = Products(credentials=_JP, marketplace=Marketplaces.JP)
papi_au = Products(credentials=_AU, marketplace=Marketplaces.AU)
asins = [l["asin"] for l in listings]
jp_prices = {}
for i in range(0, len(asins), 20):
    batch = asins[i:i+20]
    try:
        resp = papi_jp.get_competitive_pricing_for_asins(batch)
        for item in (resp.payload if isinstance(resp.payload,list) else []):
            asin = item.get("ASIN","")
            cps  = item.get("Product",{}).get("CompetitivePricing",{}).get("CompetitivePrices",[])
            for cp in cps:
                if cp.get("condition")=="New":
                    amt = cp.get("Price",{}).get("ListingPrice",{}).get("Amount")
                    if amt: jp_prices[asin] = int(float(amt))
                    break
    except: pass
    time.sleep(2.1)

# ── 3. AU競合価格 ────────────────────────────────────────────
au_comp = {}
for i in range(0, len(asins), 20):
    batch = asins[i:i+20]
    try:
        resp = papi_au.get_competitive_pricing_for_asins(batch)
        for item in (resp.payload if isinstance(resp.payload,list) else []):
            asin = item.get("ASIN","")
            cps  = item.get("Product",{}).get("CompetitivePricing",{}).get("CompetitivePrices",[])
            for cp in cps:
                if cp.get("condition")=="New" and not cp.get("belongsToRequester"):
                    amt = cp.get("Price",{}).get("ListingPrice",{}).get("Amount")
                    if amt: au_comp[asin] = float(amt)
                    break
    except: pass
    time.sleep(2.1)

# ── 4. 利益計算・分析 ────────────────────────────────────────
print("\n" + "="*70)
print("【生き残り商品 - 利益率ランキング】")
print("="*70)
results = []
for l in listings:
    if l["status"] != "active": continue
    asin = l["asin"]; jp = jp_prices.get(asin); our = l["price"]
    if not jp or not our: continue
    res = calc_profit(asin, l["title"], jp, our, rate)
    comp = au_comp.get(asin)
    results.append({"asin":asin,"title":l["title"],"sku":l["sku"],
                    "jp_jpy":jp,"our_aud":our,"comp_aud":comp,
                    "profit_rate":res.profit_rate,"profit_jpy":res.profit_jpy,
                    "shipping":get_shipping_jpy()})

results.sort(key=lambda x: x["profit_rate"], reverse=True)

total_profit_jpy = 0
for r in results:
    comp_str = f"競合A${r['comp_aud']:.2f}" if r['comp_aud'] else "独占"
    print(f"  {r['profit_rate']:+.1f}%  JP¥{r['jp_jpy']:,} → AU${r['our_aud']:.2f}  {comp_str}")
    print(f"    ASIN:{r['asin']} SKU:{r['sku']}")
    print(f"    {r['title'][:60]}")
    total_profit_jpy += r['profit_jpy']

print(f"\n  アクティブ出品: {len(results)}件")
print(f"  平均粗利率: {sum(r['profit_rate'] for r in results)/len(results):.1f}%") if results else None
print(f"  全件売れた場合の総粗利(推定): ¥{total_profit_jpy:,.0f}")

# ── 5. 構造問題の数値化 ────────────────────────────────────
print("\n" + "="*70)
print("【構造分析】")
print("="*70)
viable_20 = viable_18 = profitable_30 = 0
for l in listings:
    if l["status"] != "active": continue
    jp = jp_prices.get(l["asin"])
    if not jp: continue
    min20 = calc_optimal_au_price(jp, target_profit_rate=20, exchange_rate=rate)
    min18 = calc_optimal_au_price(jp, target_profit_rate=18, exchange_rate=rate)
    min30 = calc_optimal_au_price(jp, target_profit_rate=30, exchange_rate=rate)
    comp  = au_comp.get(l["asin"])
    if comp and comp >= min20: viable_20 += 1
    if comp and comp >= min18: viable_18 += 1
    if comp and comp >= min30: profitable_30 += 1

print(f"  MIN_PROFIT_RATE=20% で競合に勝てる商品: {viable_20}件")
print(f"  MIN_PROFIT_RATE=18% で競合に勝てる商品: {viable_18}件")
print(f"  MIN_PROFIT_RATE=30% で競合に勝てた商品: {profitable_30}件 ← 旧設定")

# ── 6. eBay概況 ────────────────────────────────────────────
print("\n" + "="*70)
print("【eBay出品 概況（arbitrage.dbより）】")
print("="*70)
try:
    from apis.ebay_api import get_active_listings
    ebay = get_active_listings()
    total_usd = sum(v["price_usd"] for v in ebay.values())
    print(f"  アクティブ: {len(ebay)}件  総掲載額: ${total_usd:,.2f}")
    prices = sorted([v["price_usd"] for v in ebay.values()])
    if prices:
        print(f"  価格帯: ${prices[0]:.2f}〜${prices[-1]:.2f}  中央値: ${prices[len(prices)//2]:.2f}")
except Exception as e:
    print(f"  eBay取得エラー: {e}")

print("\n完了")

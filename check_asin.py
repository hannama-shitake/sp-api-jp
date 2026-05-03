"""
ASIN check script: JP stock/price + AU competitor price + profit estimate
Usage: python check_asin.py B094F6VQCB
"""
import sys
import time

from sp_api.api import Products
from sp_api.base import Marketplaces, SellingApiException

import config
from apis.exchange_rate import get_jpy_to_aud
from modules.profit_calc import calc_optimal_au_price, calc_profit

_AU_CREDS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP_CREDS = {
    "refresh_token": config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id": config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}


def p(s):
    """Print with safe encoding."""
    print(str(s).encode('utf-8', errors='replace').decode('utf-8', errors='replace'), flush=True)


def check_asin(asin: str):
    p("=" * 60)
    p(f"ASIN: {asin}")
    p("=" * 60)

    rate = get_jpy_to_aud()
    p(f"\nExchange rate: 1 JPY = {rate:.6f} AUD  (1 AUD = {1/rate:.1f} JPY)")

    # --- JP price / stock ---
    p("\n[JP] Amazon Japan")
    jp_api = Products(credentials=_JP_CREDS, marketplace=Marketplaces.JP)
    jp_price = None
    jp_in_stock = False
    try:
        resp = jp_api.get_competitive_pricing_for_asins([asin])
        items = resp.payload if isinstance(resp.payload, list) else []
        for item in items:
            comp_prices = (
                item.get("Product", {})
                    .get("CompetitivePricing", {})
                    .get("CompetitivePrices", [])
            )
            for cp in comp_prices:
                if cp.get("condition") == "New":
                    amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                    if amount:
                        jp_price = int(float(amount))
                        jp_in_stock = True
        if jp_in_stock:
            p(f"  Price: JPY {jp_price:,}")
            p(f"  Stock: YES [OK]")
        else:
            p(f"  Stock: NONE [NG]  (no competitive price data)")
    except SellingApiException as e:
        p(f"  ERROR: {e}")

    time.sleep(2)

    # --- AU competitor price (competitive pricing) ---
    p("\n[AU] Amazon Australia - competitive pricing (batch)")
    au_api = Products(credentials=_AU_CREDS, marketplace=Marketplaces.AU)
    au_comp_price = None
    try:
        resp = au_api.get_competitive_pricing_for_asins([asin])
        items = resp.payload if isinstance(resp.payload, list) else []
        for item in items:
            comp_prices = (
                item.get("Product", {})
                    .get("CompetitivePricing", {})
                    .get("CompetitivePrices", [])
            )
            p(f"  Entries: {len(comp_prices)}")
            for cp in comp_prices:
                belongs = cp.get("belongsToRequester", False)
                cond = cp.get("condition", "?")
                amount = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                mine = " <-- MINE" if belongs else ""
                p(f"    condition={cond}, mine={belongs}, price=AUD {amount}{mine}")
                if cond == "New" and not belongs and amount:
                    au_comp_price = float(amount)
    except SellingApiException as e:
        p(f"  ERROR: {e}")

    time.sleep(2)

    # --- AU competitor price (item offers - more accurate) ---
    p("\n[AU] Amazon Australia - item offers (real-time)")
    au_offer_price = None
    try:
        resp = au_api.get_item_offers(asin, item_condition="New")
        payload = resp.payload if hasattr(resp, "payload") else {}
        offers = payload.get("Offers", [])
        summary = payload.get("Summary", {})
        lowest = summary.get("LowestPrices", [])
        p(f"  Offer count: {len(offers)}")
        if lowest:
            p(f"  Lowest prices summary: {lowest}")
        my_id = config.AMAZON_AU_CREDENTIALS.get("seller_id", "")
        for i, offer in enumerate(offers[:5]):
            sid = offer.get("SellerId", "?")
            price = offer.get("ListingPrice", {}).get("Amount", "?")
            is_mine = "(MY LISTING)" if (sid == my_id) else ""
            bb = "[BuyBox]" if offer.get("IsBuyBoxWinner", False) else ""
            p(f"  [{i+1}] {sid} {is_mine} AUD {price} {bb}")
        competitor_prices = [
            float(o.get("ListingPrice", {}).get("Amount", 0))
            for o in offers
            if o.get("SellerId", "") != my_id and o.get("ListingPrice", {}).get("Amount")
        ]
        if competitor_prices:
            au_offer_price = min(competitor_prices)
            p(f"  --> Competitor lowest: AUD {au_offer_price:.2f}")
        else:
            p(f"  --> No competitor offers found")
    except SellingApiException as e:
        p(f"  ERROR: {e}")

    # --- Profit estimate ---
    p("\n[PROFIT ESTIMATE]")
    if not jp_in_stock or not jp_price:
        p("  JP out of stock -> cannot list")
        return

    min_price = calc_optimal_au_price(jp_price, exchange_rate=rate)
    p(f"  JP cost:        JPY {jp_price:,}")
    p(f"  Min price line: AUD {min_price:.2f}  (profit >= {config.MIN_PROFIT_RATE}%)")
    p(f"  Shipping:       JPY {config.DHL_SHIPPING_JPY:,}  (DHL)")
    p(f"  AU fee rate:    {config.AU_FEE_RATE*100:.0f}%")

    for label, price in [
        ("competitive_pricing", au_comp_price),
        ("item_offers", au_offer_price),
    ]:
        if price:
            result = calc_profit(asin=asin, title="", jp_price_jpy=jp_price,
                                 au_price_aud=price, exchange_rate=rate)
            status = "[OK PROFITABLE]" if result.is_profitable else "[NG LOSS/INSUFFICIENT]"
            can_list = "-> LIST" if price >= min_price else "-> PAUSE"
            p(f"\n  Source: {label}")
            p(f"    AU price:    AUD {price:.2f}")
            p(f"    Profit rate: {result.profit_rate:.1f}%  {status}")
            p(f"    Profit JPY:  {result.profit_jpy:,.0f}")
            p(f"    vs min line: {'OK' if price >= min_price else 'BELOW'}  {can_list}")

    if not au_comp_price and not au_offer_price:
        p("\n  No AU competitor -> would list at min price AUD {:.2f}".format(min_price))
        result = calc_profit(asin=asin, title="", jp_price_jpy=jp_price,
                             au_price_aud=min_price, exchange_rate=rate)
        p(f"  Profit rate at min price: {result.profit_rate:.1f}%")


if __name__ == "__main__":
    asin = sys.argv[1] if len(sys.argv) > 1 else "B094F6VQCB"
    check_asin(asin)

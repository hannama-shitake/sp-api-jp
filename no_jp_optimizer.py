"""
JP競合価格が取れない商品（独占・廃番等）の AU価格を AU競合データのみで最適化する。
price_update.py が毎回スキップしている商品に対して収益を最大化する。

実行方法:
  python no_jp_optimizer.py            # ドライラン（変更なし・確認のみ）
  python no_jp_optimizer.py --apply    # 実際に価格更新

戦略:
  独占（AU競合なし）      : 現在価格 × (1 + SOLO_UPLIFT_RATE) → 最大 × SOLO_MAX_MULT
  競合あり・自分が安い    : 競合価格 × (1 - BUYBOX_UNDERCUT_RATE) まで値上げ
  競合あり・自分が高い/同等: 変更なし
"""
import csv
import gzip
import io
import sys
import time
import os

sys.path.insert(0, os.path.dirname(__file__))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
os.chdir(os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

from sp_api.api import Reports, Products, ListingsItems
from sp_api.base import Marketplaces, SellingApiException
import requests as _req, gzip as _gz

import config
from apis.exchange_rate import get_jpy_to_aud
from utils.logger import get_logger
from utils.notify import send_email

logger = get_logger(__name__)

# ── パラメータ ────────────────────────────────────────────────
# 独占商品の1回あたり値上げ率（10%）
SOLO_UPLIFT_RATE   = float(os.getenv("SOLO_UPLIFT_RATE",   "0.10"))
# 独占商品の1回あたり値上げ上限倍率（現在価格の最大1.5倍まで）
SOLO_MAX_MULT      = float(os.getenv("SOLO_MAX_MULT",      "1.50"))
# 競合比較の最小差（競合より何%以上安い時だけ値上げ）
COMP_MIN_GAP_RATE  = float(os.getenv("COMP_MIN_GAP_RATE",  "0.02"))  # 2%以上安い場合のみ
# 最低価格（AU$）
MIN_PRICE_AUD      = float(os.getenv("MIN_PRICE_AUD",      "20.0"))

_APPLY = "--apply" in sys.argv

_AU = {
    "refresh_token":   config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":      config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}
_JP = {
    "refresh_token":   config.AMAZON_JP_CREDENTIALS["refresh_token"],
    "lwa_app_id":      config.AMAZON_JP_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_JP_CREDENTIALS["lwa_client_secret"],
}

# ─────────────────────────────────────────────────────────────
# 1. AU出品一覧取得
# ─────────────────────────────────────────────────────────────
def get_au_listings() -> list:
    api = Reports(credentials=_AU, marketplace=Marketplaces.AU)
    resp = api.create_report(reportType="GET_MERCHANT_LISTINGS_ALL_DATA")
    rid = resp.payload["reportId"]
    logger.info("[no_jp_opt] レポートID: %s", rid)
    for i in range(120):
        time.sleep(10)
        s = api.get_report(rid)
        st = s.payload.get("processingStatus", "")
        if i % 6 == 0:
            logger.info("[no_jp_opt] レポートステータス: %s", st)
        if st == "DONE":
            break
        if st in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"レポート失敗: {st}")
    doc = api.get_report_document(s.payload["reportDocumentId"])
    url = doc.payload["url"]
    comp = doc.payload.get("compressionAlgorithm", "")
    r = _req.get(url, timeout=60)
    r.raise_for_status()
    content = _gz.decompress(r.content) if comp == "GZIP" else r.content
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    listings = []
    seen = set()
    for row in reader:
        asin  = row.get("asin1", "").strip()
        sku   = row.get("seller-sku", "").strip()
        st    = row.get("status", "").strip().lower()
        title = row.get("item-name", "").strip()
        try: price = float(row.get("price", "0") or 0)
        except: price = 0.0
        if asin and len(asin) == 10 and sku and asin not in seen and st == "active":
            seen.add(asin)
            listings.append({"asin": asin, "sku": sku, "price": price, "title": title})
    logger.info("[no_jp_opt] アクティブ出品: %d件", len(listings))
    return listings


# ─────────────────────────────────────────────────────────────
# 2. JP価格一括取得（JP価格なし商品の特定に使用）
#    クォータ超過時はリトライ（最大3回、15秒待機）
# ─────────────────────────────────────────────────────────────
def get_jp_prices(asins: list) -> dict:
    api = Products(credentials=_JP, marketplace=Marketplaces.JP)
    result = {}
    for i in range(0, len(asins), 20):
        batch = asins[i:i+20]
        for attempt in range(3):
            try:
                resp = api.get_competitive_pricing_for_asins(batch)
                for item in (resp.payload if isinstance(resp.payload, list) else []):
                    a = item.get("ASIN", "")
                    cps = (item.get("Product", {})
                               .get("CompetitivePricing", {})
                               .get("CompetitivePrices", []))
                    price_jpy = None
                    for cp in cps:
                        if cp.get("condition") == "New":
                            amt = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                            if amt:
                                price_jpy = int(float(amt))
                            break
                    offer_listings = (item.get("Product", {})
                                          .get("CompetitivePricing", {})
                                          .get("NumberOfOfferListings", []))
                    new_count = sum(
                        ol.get("Count", 0) for ol in offer_listings
                        if (ol.get("condition") or "").lower() in ("new", "new_new")
                    )
                    result[a] = (price_jpy, new_count > 0)
                break
            except SellingApiException as e:
                msg = str(e)
                if "QuotaExceeded" in msg and attempt < 2:
                    logger.warning("[no_jp_opt] QuotaExceeded バッチ%d → 15秒待機リトライ", i//20)
                    time.sleep(15)
                else:
                    logger.warning("[no_jp_opt] バッチエラー: %s", e)
                    break
        time.sleep(5)  # 通常のインターバルは長めに
    logger.info("[no_jp_opt] JP価格: %d/%d件取得", len(result), len(asins))
    return result


# ─────────────────────────────────────────────────────────────
# 3. AU競合価格一括取得
# ─────────────────────────────────────────────────────────────
def get_au_comp_prices(asins: list) -> dict:
    api = Products(credentials=_AU, marketplace=Marketplaces.AU)
    result = {}
    for i in range(0, len(asins), 20):
        batch = asins[i:i+20]
        try:
            resp = api.get_competitive_pricing_for_asins(batch)
            for item in (resp.payload if isinstance(resp.payload, list) else []):
                a = item.get("ASIN", "")
                cps = (item.get("Product", {})
                           .get("CompetitivePricing", {})
                           .get("CompetitivePrices", []))
                for cp in cps:
                    if cp.get("condition") == "New" and not cp.get("belongsToRequester"):
                        amt = cp.get("Price", {}).get("ListingPrice", {}).get("Amount")
                        if amt:
                            result[a] = float(amt)
                        break
        except SellingApiException as e:
            logger.warning("[no_jp_opt] AU価格バッチエラー: %s", e)
        time.sleep(2.1)
    logger.info("[no_jp_opt] AU競合価格: %d/%d件取得", len(result), len(asins))
    return result


# ─────────────────────────────────────────────────────────────
# 4. 価格パッチ（price_update.py と同じロジック）
# ─────────────────────────────────────────────────────────────
def patch_price(api, seller_id: str, sku: str, price_aud: float):
    body = {
        "productType": "PRODUCT",
        "patches": [
            {
                "op": "replace",
                "path": "/attributes/fulfillment_availability",
                "value": [{
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": 1,
                    "lead_time_to_ship_max_days": config.HANDLING_TIME_DAYS,
                    "marketplace_id": config.MARKETPLACE_AU,
                }],
            },
            {
                "op": "replace",
                "path": "/attributes/purchasable_offer",
                "value": [{
                    "currency": "AUD",
                    "our_price": [{"schedule": [{"value_with_tax": price_aud}]}],
                    "marketplace_id": config.MARKETPLACE_AU,
                }],
            },
        ],
    }
    api.patch_listings_item(
        sellerId=seller_id, sku=sku,
        marketplaceIds=[config.MARKETPLACE_AU], body=body,
    )


# ─────────────────────────────────────────────────────────────
# 5. メイン最適化ロジック
# ─────────────────────────────────────────────────────────────
def optimize(listings, jp_prices, au_comp, seller_id, apply=False):
    api = ListingsItems(credentials=_AU, marketplace=Marketplaces.AU)

    # price_update.py が価格更新をスキップする商品を対象にする:
    #   jp_prices にないもの (APIが何も返さなかった)
    #   jp_prices にあるが jp_price=None かつ in_stock=True (JP独占=競合価格なし)
    #   jp_prices にあるが in_stock=False は price_update.py が削除するので除外
    def is_no_jp_price(asin):
        data = jp_prices.get(asin)
        if data is None:
            return True   # APIが一切データを返さなかった
        jp_price, in_stock = data
        if not in_stock:
            return False  # JP在庫切れ → price_update.py が削除するので触らない
        return jp_price is None  # 在庫あり・競合価格なし → 現在価格維持のままスキップされている

    no_jp_items   = [l for l in listings if is_no_jp_price(l["asin"])]
    solo_up       = []   # 独占 → 値上げ
    comp_up       = []   # 競合あり → 値上げ
    no_change     = []   # 変更なし
    skipped       = []   # 価格¥0 など

    for l in no_jp_items:
        asin  = l["asin"]
        cur   = l["price"]
        title = l["title"][:45]

        if cur <= 0:
            skipped.append(l)
            continue

        comp = au_comp.get(asin)

        if not comp:
            # 独占: +SOLO_UPLIFT_RATE% ただし SOLO_MAX_MULT 倍を超えない
            new_price = round(cur * (1 + SOLO_UPLIFT_RATE), 2)
            cap_price = round(cur * SOLO_MAX_MULT, 2)
            new_price = min(new_price, cap_price)
            new_price = max(new_price, MIN_PRICE_AUD)
            if new_price > cur + 0.01:
                solo_up.append({**l, "new_price": new_price, "comp": None,
                                 "reason": f"独占+{SOLO_UPLIFT_RATE*100:.0f}%"})
            else:
                no_change.append({**l, "reason": "独占・変更なし"})
        else:
            gap = (comp - cur) / comp  # 競合より何%安いか
            if gap > COMP_MIN_GAP_RATE:
                # 競合より2%以上安い → 値上げ余地あり
                new_price = round(comp * (1 - config.BUYBOX_UNDERCUT_RATE), 2)
                new_price = max(new_price, MIN_PRICE_AUD)
                if new_price > cur + 0.01:
                    comp_up.append({**l, "new_price": new_price, "comp": comp,
                                     "reason": f"競合A${comp:.2f}より{gap*100:.1f}%安い"})
                else:
                    no_change.append({**l, "reason": "競合あり・変更なし"})
            else:
                no_change.append({**l, "reason": f"競合A${comp:.2f}と同等以上"})

    # ── レポート出力 ─────────────────────────────────────────
    mode = "【実行モード】" if apply else "【ドライランモード - 変更なし】"
    print(f"\n{'='*70}")
    print(f"JP価格なし商品 AU価格最適化レポート  {mode}")
    print(f"{'='*70}")
    print(f"JP価格なし合計: {len(no_jp_items)}件")
    print(f"  独占 → 値上げ対象    : {len(solo_up)}件")
    print(f"  競合あり → 値上げ対象: {len(comp_up)}件")
    print(f"  変更なし             : {len(no_change)}件")
    print(f"  スキップ（価格0等）  : {len(skipped)}件")

    total_gain_aud = 0.0
    updated = failed = 0

    if solo_up:
        print(f"\n--- 独占 値上げ ({len(solo_up)}件) ---")
        for item in sorted(solo_up, key=lambda x: x["new_price"] - x["price"], reverse=True):
            gain = item["new_price"] - item["price"]
            total_gain_aud += gain
            print(f"  {item['asin']}  AU${item['price']:.2f} → AU${item['new_price']:.2f}  (+A${gain:.2f})  {item['title']}")
            if apply:
                try:
                    patch_price(api, seller_id, item["sku"], item["new_price"])
                    updated += 1
                    logger.info("[no_jp_opt] %s: $%.2f → $%.2f (独占値上げ)", item["asin"], item["price"], item["new_price"])
                except Exception as e:
                    logger.warning("[no_jp_opt] %s: 更新失敗 - %s", item["asin"], e)
                    failed += 1
                time.sleep(0.3)

    if comp_up:
        print(f"\n--- 競合あり 値上げ ({len(comp_up)}件) ---")
        for item in sorted(comp_up, key=lambda x: x["new_price"] - x["price"], reverse=True):
            gain = item["new_price"] - item["price"]
            total_gain_aud += gain
            print(f"  {item['asin']}  AU${item['price']:.2f} → AU${item['new_price']:.2f}  (+A${gain:.2f})  競合A${item['comp']:.2f}  {item['title']}")
            if apply:
                try:
                    patch_price(api, seller_id, item["sku"], item["new_price"])
                    updated += 1
                    logger.info("[no_jp_opt] %s: $%.2f → $%.2f (競合値上げ)", item["asin"], item["price"], item["new_price"])
                except Exception as e:
                    logger.warning("[no_jp_opt] %s: 更新失敗 - %s", item["asin"], e)
                    failed += 1
                time.sleep(0.3)

    print(f"\n{'='*70}")
    print(f"値上げ対象合計: {len(solo_up)+len(comp_up)}件")
    print(f"値上げ額合計(AU$): +A${total_gain_aud:.2f}  (全件完売時の追加収益)")
    if apply:
        print(f"実際に更新: {updated}件  失敗: {failed}件")
    print(f"{'='*70}")

    if not apply:
        print("\n※ ドライランです。実際に変更するには --apply を付けて実行してください")

    return solo_up, comp_up, updated, failed, total_gain_aud


# ─────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────
def main():
    seller_id = os.getenv("AMAZON_AU_SELLER_ID", "").strip()
    if not seller_id:
        logger.error("AMAZON_AU_SELLER_ID 未設定")
        sys.exit(1)

    rate = get_jpy_to_aud()
    logger.info("[no_jp_opt] 為替: 1 JPY = %.5f AUD", rate)
    logger.info("[no_jp_opt] モード: %s", "APPLY" if _APPLY else "DRY RUN")

    # 1. AU出品一覧
    listings = get_au_listings()
    asins = [l["asin"] for l in listings]

    # 2. JP価格（どれがJP価格なしかの判定）
    logger.info("[no_jp_opt] JP価格取得中（クォータ節約のため5秒間隔）...")
    jp_prices = get_jp_prices(asins)

    # 3. AU競合価格
    logger.info("[no_jp_opt] AU競合価格取得中...")
    au_comp = get_au_comp_prices(asins)

    # 4. 最適化実行
    solo_up, comp_up, updated, failed, gain_aud = optimize(
        listings, jp_prices, au_comp, seller_id, apply=_APPLY
    )

    # 5. メール通知（--applyの場合のみ）
    if _APPLY:
        total_items = len(solo_up) + len(comp_up)
        subject = f"[SP-API] JP価格なし商品 価格最適化 +A${gain_aud:.2f} ({total_items}件更新)"
        lines = [
            f"JP価格なし商品の価格最適化を実行しました。",
            f"",
            f"独占値上げ: {len(solo_up)}件",
            f"競合値上げ: {len(comp_up)}件",
            f"更新成功: {updated}件 / 失敗: {failed}件",
            f"値上げ額合計（全完売時追加収益）: +A${gain_aud:.2f}",
            f"",
            "--- 独占 値上げ ---",
        ]
        for item in sorted(solo_up, key=lambda x: x["new_price"] - x["price"], reverse=True):
            lines.append(f"  {item['asin']}  A${item['price']:.2f}→A${item['new_price']:.2f}  {item['title']}")
        lines.append("\n--- 競合 値上げ ---")
        for item in sorted(comp_up, key=lambda x: x["new_price"] - x["price"], reverse=True):
            lines.append(f"  {item['asin']}  A${item['price']:.2f}→A${item['new_price']:.2f}  競合A${item['comp']:.2f}  {item['title']}")
        send_email(subject=subject, body="\n".join(lines))
        logger.info("[no_jp_opt] メール送信完了")


if __name__ == "__main__":
    main()

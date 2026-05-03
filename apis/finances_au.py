# -*- coding: utf-8 -*-
"""
Amazon AU Finances API — 売上データ取得
注文ごとの売上・手数料・返金をASIN単位で取得し、
achoo_accounting の input/amazon_au/ にCSVとして出力する。
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from pathlib import Path
import csv, time

from sp_api.api import Finances
from sp_api.base import Marketplaces, SellingApiException
import config
from utils.logger import get_logger

logger = get_logger(__name__)

_CREDENTIALS = {
    "refresh_token": config.AMAZON_AU_CREDENTIALS["refresh_token"],
    "lwa_app_id":    config.AMAZON_AU_CREDENTIALS["lwa_app_id"],
    "lwa_client_secret": config.AMAZON_AU_CREDENTIALS["lwa_client_secret"],
}


def _get_finances_client():
    return Finances(credentials=_CREDENTIALS, marketplace=Marketplaces.AU)


def fetch_order_events(date_from: datetime, date_to: datetime) -> list[dict]:
    """
    指定期間の注文金融イベント一覧を取得する。
    返り値: [{"order_id", "posted_date", "asin", "sku",
              "sales_aud", "commission_aud", "net_aud"}, ...]
    """
    client = _get_finances_client()
    rows = []
    next_token = None

    while True:
        try:
            if next_token:
                resp = client.list_financial_events(NextToken=next_token)
            else:
                resp = client.list_financial_events(
                    PostedAfter=date_from.isoformat(),
                    PostedBefore=date_to.isoformat(),
                    MaxResultsPerPage=100,
                )
        except SellingApiException as e:
            logger.error(f"Finances API エラー: {e}")
            break

        payload = resp.payload or {}
        events = payload.get("FinancialEvents", {})
        order_events = events.get("ShipmentEventList", [])

        for event in order_events:
            order_id    = event.get("AmazonOrderId", "")
            posted_date = event.get("PostedDate", "")
            items       = event.get("ShipmentItemList", [])

            for item in items:
                asin = item.get("ASIN", "")
                sku  = item.get("SellerSKU", "")

                # 売上
                sales = 0.0
                for charge in item.get("ItemChargeList", []):
                    if charge.get("ChargeType") == "Principal":
                        sales += float(charge.get("ChargeAmount", {}).get("CurrencyAmount", 0))

                # 手数料（Amazon commission）
                commission = 0.0
                for fee in item.get("ItemFeeList", []):
                    commission += float(fee.get("FeeAmount", {}).get("CurrencyAmount", 0))

                net = sales + commission  # commissionはマイナス値

                if sales == 0:
                    continue

                rows.append({
                    "order_id":    order_id,
                    "posted_date": posted_date[:10] if posted_date else "",
                    "asin":        asin,
                    "sku":         sku,
                    "sales_aud":   round(sales, 2),
                    "commission_aud": round(commission, 2),
                    "net_aud":     round(net, 2),
                })

        next_token = payload.get("NextToken")
        if not next_token:
            break
        time.sleep(1)

    logger.info(f"Finances API: {len(rows)}件取得 ({date_from.date()} ～ {date_to.date()})")
    return rows


def export_to_accounting(rows: list[dict], output_path: Path):
    """取得データをaccounting用CSVに出力"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["posted_date", "asin", "sku", "order_id",
              "sales_aud", "commission_aud", "net_aud"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"CSV出力: {output_path} ({len(rows)}件)")


if __name__ == "__main__":
    # Q1 2026 売上データを取得してaccountingに出力
    date_from = datetime(2026, 1, 1, tzinfo=timezone.utc)
    date_to   = datetime(2026, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

    print("Finances API から Q1 2026 データ取得中...")
    rows = fetch_order_events(date_from, date_to)

    if rows:
        out = Path(r"C:\Users\user\Desktop\achoo_accounting\input\amazon_au\amazon_au_2026Q1.csv")
        export_to_accounting(rows, out)
        total_sales = sum(r["sales_aud"] for r in rows)
        total_net   = sum(r["net_aud"] for r in rows)
        print(f"\n--- Q1 2026 Amazon AU 実売上 ---")
        print(f"件数:    {len(rows)}件")
        print(f"売上合計: {total_sales:.2f} AUD")
        print(f"手数料後: {total_net:.2f} AUD")
        print(f"出力先:  {out}")
    else:
        print("データなし（期間内の売上がないか、APIエラー）")

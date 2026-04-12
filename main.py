"""
Amazon JP → AU アービトラージシステム CLI

使い方:
    python main.py test-connection            # API接続確認
    python main.py scrape --url URL           # AU セラーをスクレイピング
    python main.py research --url URL         # スクレイピング + 利益計算（出品なし）
    python main.py list --url URL             # スクレイピング + 利益計算 + 出品
    python main.py monitor price              # JP価格チェック & AU価格更新（1回）
    python main.py monitor stock              # JP在庫チェック & AU在庫更新（1回）
    python main.py status                     # 出品状況一覧
"""

import argparse
import os
import sys
from rich.console import Console
from rich.table import Table

from db.database import init_db
from scraper.au_seller import scrape_seller_products
from modules.product_matcher import match_and_research
from modules.listing_manager import list_profitable_products
from modules.price_monitor import run_price_check, run_stock_check
from apis import amazon_jp, amazon_au
from apis.exchange_rate import get_jpy_to_aud
from db.database import get_connection
import config
from utils.logger import get_logger

logger = get_logger(__name__)
console = Console()


def cmd_test_connection(args):
    console.print("\n[bold]API 接続確認[/bold]")

    # 為替レート
    rate = get_jpy_to_aud()
    console.print(f"  為替レート (JPY→AUD): [green]{rate:.6f}[/green]")

    # Amazon JP
    jp_ok = amazon_jp.check_connection()
    console.print(f"  Amazon JP SP-API: {'[green]OK[/green]' if jp_ok else '[red]失敗[/red]'}")

    # Amazon AU
    au_ok = amazon_au.check_connection()
    console.print(f"  Amazon AU SP-API: {'[green]OK[/green]' if au_ok else '[red]失敗[/red]'}")

    if jp_ok and au_ok:
        console.print("\n[bold green]すべての接続が正常です[/bold green]")
    else:
        console.print("\n[bold red]接続に失敗した API があります。setup_guide.md を確認してください[/bold red]")
        sys.exit(1)


def cmd_scrape(args):
    if not args.url:
        console.print("[red]--url を指定してください[/red]")
        sys.exit(1)

    console.print(f"\n[bold]スクレイピング開始:[/bold] {args.url}")
    products = scrape_seller_products(args.url)

    table = Table(title=f"取得商品 ({len(products)}件)")
    table.add_column("ASIN", style="cyan")
    table.add_column("タイトル")
    table.add_column("AU価格 (AUD)", justify="right")
    for p in products[:50]:
        table.add_row(
            p["asin"],
            (p["title"] or "")[:60],
            f"${p['au_price_aud']:.2f}" if p["au_price_aud"] else "-",
        )
    console.print(table)
    if len(products) > 50:
        console.print(f"  ...他 {len(products) - 50} 件")


def cmd_research(args):
    if not args.url:
        console.print("[red]--url を指定してください[/red]")
        sys.exit(1)

    console.print(f"\n[bold]リサーチ開始:[/bold] {args.url}")
    products = scrape_seller_products(args.url)
    console.print(f"  {len(products)}件 スクレイピング完了")

    profitable = match_and_research(products, dry_run=True)

    if not profitable:
        console.print(f"\n[yellow]粗利 {config.MIN_PROFIT_RATE}% 以上の商品は見つかりませんでした[/yellow]")
        return

    table = Table(title=f"利益商品 ({len(profitable)}件) ※ 粗利率 {config.MIN_PROFIT_RATE}% 以上")
    table.add_column("ASIN", style="cyan")
    table.add_column("タイトル")
    table.add_column("JP仕入 (JPY)", justify="right")
    table.add_column("AU販売 (AUD)", justify="right")
    table.add_column("粗利率", justify="right", style="green")
    table.add_column("推奨価格 (AUD)", justify="right")

    for r in profitable:
        table.add_row(
            r.asin,
            (r.title or "")[:40],
            f"¥{r.jp_price_jpy:,}",
            f"${r.au_price_aud:.2f}",
            f"{r.profit_rate:.1f}%",
            f"${r.recommended_au_price_aud:.2f}" if r.recommended_au_price_aud else "-",
        )
    console.print(table)


def cmd_list(args):
    if not args.url:
        console.print("[red]--url を指定してください[/red]")
        sys.exit(1)

    dry_run = args.dry_run
    console.print(f"\n[bold]{'[DRY-RUN] ' if dry_run else ''}出品開始:[/bold] {args.url}")

    products = scrape_seller_products(args.url)
    console.print(f"  {len(products)}件 スクレイピング完了")

    profitable = match_and_research(products, dry_run=dry_run)
    if not profitable:
        console.print(f"[yellow]利益商品なし[/yellow]")
        return

    result = list_profitable_products(profitable, dry_run=dry_run)
    console.print(
        f"\n出品結果: [green]成功 {result['success']}件[/green] / "
        f"スキップ {result['skipped']}件 / [red]失敗 {result['failed']}件[/red]"
    )


def cmd_copy_seller(args):
    """競合セラーの全商品を同じ価格でAUに出品する"""
    if not args.url:
        console.print("[red]--url を指定してください[/red]")
        sys.exit(1)

    dry_run = args.dry_run
    console.print(f"\n[bold]{'[DRY-RUN] ' if dry_run else ''}セラーコピー開始:[/bold] {args.url}")

    products = scrape_seller_products(args.url)
    console.print(f"  {len(products)}件 スクレイピング完了")

    success, skipped, failed = 0, 0, 0
    for p in products:
        asin = p["asin"]
        price = p["au_price_aud"]

        if not price:
            console.print(f"  [yellow]SKIP[/yellow] {asin} - 価格取得できず")
            skipped += 1
            continue

        if dry_run:
            console.print(f"  [cyan]DRY[/cyan] {asin} - AUD {price:.2f}")
            success += 1
            continue

        ok, msg = amazon_au.list_item_fbm(asin, price)
        if ok:
            console.print(f"  [green]OK[/green] {asin} - AUD {price:.2f} (SKU: {msg})")
            success += 1
        else:
            console.print(f"  [red]NG[/red] {asin} - {msg}")
            failed += 1

    console.print(
        f"\n結果: [green]成功 {success}件[/green] / "
        f"スキップ {skipped}件 / [red]失敗 {failed}件[/red]"
    )


def cmd_monitor(args):
    if args.target == "price":
        console.print("\n[bold]JP価格チェック実行[/bold]")
        run_price_check()
    elif args.target == "stock":
        console.print("\n[bold]JP在庫チェック実行[/bold]")
        run_stock_check()
    else:
        console.print("[red]monitor の引数は price または stock です[/red]")
        sys.exit(1)


def cmd_status(args):
    conn = get_connection()
    rows = conn.execute("""
        SELECT l.asin, l.sku, l.status, l.listed_at,
               p.jp_price_jpy, p.au_price_aud, p.profit_rate, p.jp_in_stock
        FROM listings l
        LEFT JOIN products p ON l.asin = p.asin
        WHERE l.platform = 'amazon_au'
        ORDER BY l.listed_at DESC
    """).fetchall()
    conn.close()

    if not rows:
        console.print("[yellow]出品商品なし[/yellow]")
        return

    table = Table(title=f"出品状況 ({len(rows)}件)")
    table.add_column("ASIN", style="cyan")
    table.add_column("SKU")
    table.add_column("状態")
    table.add_column("JP在庫")
    table.add_column("JP仕入 (JPY)", justify="right")
    table.add_column("AU価格 (AUD)", justify="right")
    table.add_column("粗利率", justify="right")
    table.add_column("出品日時")

    for r in rows:
        status_str = "[green]active[/green]" if r["status"] == "active" else "[yellow]paused[/yellow]"
        stock_str = "[green]あり[/green]" if r["jp_in_stock"] else "[red]なし[/red]"
        table.add_row(
            r["asin"],
            r["sku"] or "",
            status_str,
            stock_str,
            f"¥{r['jp_price_jpy']:,}" if r["jp_price_jpy"] else "-",
            f"${r['au_price_aud']:.2f}" if r["au_price_aud"] else "-",
            f"{r['profit_rate']:.1f}%" if r["profit_rate"] else "-",
            (r["listed_at"] or "")[:16],
        )
    console.print(table)


def main():
    # セラーID を設定（AU セラーセントラルで確認）
    seller_id = os.getenv("AMAZON_AU_SELLER_ID", "")
    if seller_id:
        amazon_au.set_seller_id(seller_id)

    # DB 初期化
    init_db()

    parser = argparse.ArgumentParser(
        description="Amazon JP → AU アービトラージシステム",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command")

    # test-connection
    subparsers.add_parser("test-connection", help="API接続確認")

    # scrape
    p_scrape = subparsers.add_parser("scrape", help="AU セラーページをスクレイピング")
    p_scrape.add_argument("--url", required=True, help="AU セラー URL")

    # copy-seller
    p_copy = subparsers.add_parser("copy-seller", help="競合セラーの商品を同価格でAUに出品")
    p_copy.add_argument("--url", required=True, help="競合セラーの URL")
    p_copy.add_argument("--dry-run", action="store_true", help="プレビューのみ（出品しない）")

    # research
    p_research = subparsers.add_parser("research", help="スクレイピング + 利益計算（出品なし）")
    p_research.add_argument("--url", required=True, help="AU セラー URL")

    # list
    p_list = subparsers.add_parser("list", help="スクレイピング + 利益計算 + 出品")
    p_list.add_argument("--url", required=True, help="AU セラー URL")
    p_list.add_argument("--dry-run", action="store_true", help="プレビューのみ（出品しない）")

    # monitor
    p_monitor = subparsers.add_parser("monitor", help="JP価格/在庫チェック & AU自動更新")
    p_monitor.add_argument("target", choices=["price", "stock"], help="price または stock")

    # status
    subparsers.add_parser("status", help="出品状況一覧")

    args = parser.parse_args()

    if args.command == "copy-seller":
        cmd_copy_seller(args)
    elif args.command == "test-connection":
        cmd_test_connection(args)
    elif args.command == "scrape":
        cmd_scrape(args)
    elif args.command == "research":
        cmd_research(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "monitor":
        cmd_monitor(args)
    elif args.command == "status":
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

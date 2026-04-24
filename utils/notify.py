"""
Gmail通知ユーティリティ
GitHub ActionsからSMTPでメール送信する。

必要なGitHub Secrets:
  GMAIL_USER: 送信元Gmailアドレス (例: degital.sales.ymcorp@gmail.com)
  GMAIL_APP_PASSWORD: Gmailアプリパスワード (16桁)
  NOTIFY_EMAIL: 通知先メールアドレス (未設定ならGMAIL_USERと同じ)
"""
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.logger import get_logger

logger = get_logger(__name__)

_GMAIL_USER = (os.getenv("GMAIL_USER") or "").strip()
_GMAIL_PASS = (os.getenv("GMAIL_APP_PASSWORD") or "").strip()
_NOTIFY_TO = (os.getenv("NOTIFY_EMAIL") or _GMAIL_USER).strip()


def send_email(subject: str, body: str) -> bool:
    """Gmail SMTPでメール送信する"""
    if not _GMAIL_USER or not _GMAIL_PASS:
        logger.debug("[notify] GMAIL_USER/GMAIL_APP_PASSWORD未設定。通知スキップ")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = _GMAIL_USER
        msg["To"] = _NOTIFY_TO
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(_GMAIL_USER, _GMAIL_PASS)
            smtp.sendmail(_GMAIL_USER, _NOTIFY_TO, msg.as_string())

        logger.info("[notify] メール送信完了: %s", subject)
        return True
    except Exception as e:
        logger.warning("[notify] メール送信失敗: %s", e)
        return False


def notify_profitable(profitable: list, exchange_rate: float):
    """利益商品が見つかった時の通知"""
    if not profitable:
        return

    lines = [
        f"【SP-API】利益商品 {len(profitable)}件 見つかりました",
        f"為替レート: 1 JPY = {exchange_rate:.6f} AUD",
        "",
    ]
    for r in profitable:
        lines.append(
            f"ASIN: {r.asin}\n"
            f"  JP: ¥{r.jp_price_jpy:,}  →  AU: ${r.au_price_aud:.2f}\n"
            f"  粗利率: {r.profit_rate:.1f}%"
        )
        lines.append("")

    send_email(
        subject=f"[SP-API] 利益商品 {len(profitable)}件 出品しました",
        body="\n".join(lines),
    )


def notify_monitor_summary(scraped: int, profitable: int, listed: int, errors: int = 0):
    """Arbitrage Monitor の実行サマリー通知（利益0件でも送信）"""
    subject = (
        f"[SP-API Monitor] 利益{profitable}件 / スクレイプ{scraped}件"
        if profitable > 0
        else f"[SP-API Monitor] 利益なし（スクレイプ{scraped}件）"
    )
    body = (
        f"Arbitrage Monitor 実行完了\n\n"
        f"スクレイプ件数: {scraped}\n"
        f"利益商品:       {profitable}\n"
        f"出品成功:       {listed}\n"
        f"エラー:         {errors}\n"
    )
    if errors > 0:
        subject = "[SP-API Monitor] エラーあり - " + subject

    send_email(subject=subject, body=body)


def notify_price_update_summary(
    updated: int,
    paused: int,
    failed: int,
    reactivated: int = 0,
    sole_seller: int = 0,
    buybox_win: int = 0,
    paused_no_stock: int = 0,
    paused_too_cheap: int = 0,
    paused_fair: int = 0,
):
    """Price Update の実行サマリー通知（変化があった時のみ）"""
    if updated == 0 and paused == 0 and failed == 0 and reactivated == 0:
        return  # 何も変化なければ通知しない

    featured_offer_est = sole_seller + buybox_win
    subject = f"[SP-API Price] 価格更新{updated}件 / 再出品{reactivated}件 / 停止{paused}件"
    if failed > 0:
        subject += f" / エラー{failed}件"

    # 停止内訳（"ごそっと消え"の診断用）
    pause_detail = ""
    if paused > 0:
        pause_detail = (
            f"\n--- 停止内訳（消えた原因） ---\n"
            f"  JP在庫なし:               {paused_no_stock}件\n"
            f"  競合価格が利益ライン以下:  {paused_too_cheap}件  ← 利益率高すぎ？\n"
            f"  フェアプライシング上限:    {paused_fair}件\n"
        )
        if paused_too_cheap > paused_no_stock:
            pause_detail += "  ⚠️ 赤字停止が多い場合は MIN_PROFIT_RATE の引き下げ（30→25%）を検討\n"

    fo_lines = ""
    if featured_offer_est > 0:
        fo_lines = (
            f"\n--- Featured Offer 推定獲得数 ---\n"
            f"独占出品（自動FO）:           {sole_seller}件\n"
            f"競合あり・1%アンダーカット:   {buybox_win}件\n"
            f"合計 Featured Offer 推定:      {featured_offer_est}件\n"
        )

    body = (
        f"Price Update 実行完了\n\n"
        f"価格更新: {updated}件\n"
        f"再出品:   {reactivated}件（停止中→出品中）\n"
        f"出品停止: {paused}件\n"
        f"失敗:     {failed}件\n"
        f"{pause_detail}"
        f"{fo_lines}"
    )
    send_email(subject=subject, body=body)


def notify_error(source: str, error: str):
    """致命的エラーの通知"""
    send_email(
        subject=f"[SP-API ERROR] {source} でエラーが発生しました",
        body=f"エラー内容:\n\n{error}",
    )

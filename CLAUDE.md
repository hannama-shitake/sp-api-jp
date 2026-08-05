# CLAUDE.md

このファイルは、このリポジトリで作業する Claude Code（および将来のセッション）向けのガイドです。
まず `README.md` を読むこと。以下はコードベース解析から得た補足情報。

## ⚠️ 最重要

- **Publicリポジトリ**。認証情報・APIキー・`.env`・セラーID等の機密情報は**絶対にコミットしない**。
- 秘密情報はすべて **GitHub Secrets / `.env`**（`.gitignore` 済み）で管理。`config.py` は `os.getenv` 経由でのみ参照。
- 空の Secret で落ちないよう、認証値は `(os.getenv(...) or "").strip()` パターンを使う（`config.py` 参照）。

## 🔄 現況・方針転換（2026-08〜）

**このリポジトリの Amazon JP→AU アービトラージ自動化は縮小・停止方向。** 事業を別モデルへピボット中。
（機微なアカウント情報はPublicリポジトリのため本ファイルに記載しない。判断根拠はオフラインで管理）

- **Amazon(AU)**: 新規出品・価格更新を停止し、good standing のまま畳む方針（Seller Central 休暇設定＋GitHub Actions 無効化）。未発送発送・バイヤー返信・残高出金など後始末を優先。
- **今後の方向性**: **eBay × オリジナル商品**（自社ブランド/正規在庫を手元から発送）。リテール転売ドロップシップからは撤退。→ 真贋・ドロップシップ規約・Amazon停止の巻き添え、いずれのリスクも回避できるモデル。
- **既存資産の扱い**: `catalog_discover.py`（ASIN発掘）・`price_update.py`（競合追随）等の“転売機械”は**新モデルでは再利用しない**。eBay出品コード（`ebay_lister.py` 等）は過去に削除済みで、残るのは `apis/ebay_api.py` と `config.py` の `EBAY_*`（再構築の土台）。
- **進め方の原則**: **商品が先、自動化は後**。低volumeのうちは手動出品で回し、スケールしてから在庫同期・注文→発送の自動化を作る。
- **次アクション**: (1) AU の GitHub Actions 停止（catalog_discover / recheck_candidates / bulk_reactivate / price_update）、(2) 販売するオリジナル商品の確定、(3) 確定後に eBay 自動化を設計。

## プロジェクト概要

Amazon **JP → AU** FBM ドロップシッピング型アービトラージの自動化システム。
JP で仕入れ、AU で出品・販売し、競合価格に追随して Featured Offer（Buy Box）を狙う。

- KPI: 出品数 10,000件 / Featured Offer 獲得率 10%
- マーケットプレイスID: JP `A1VC38T7YXB528` / AU `A39IBJ37TRP1C6`
- eBay は**完全廃止済み**（`ebay_api.py`・`config.py` の EBAY_* は残骸。新規で eBay 機能を足さない）。

## アーキテクチャ

2系統のコードが同居している点に注意：

1. **本番の自動化スクリプト（リポジトリ直下の各 `*.py`）** — GitHub Actions から実行される単独スクリプト群。実際の運用はこちら。
2. **`main.py` CLI + `modules/`・`apis/`・`db/`・`scraper/` パッケージ** — 初期の設計。SQLite（`arbitrage.db`）ベース。共通ロジック（`modules/profit_calc.py` など）は本番スクリプトからも import される。

### 主要な本番スクリプト（GitHub Actions 実行）

| スクリプト | 役割 | スケジュール(UTC) |
|-----------|------|------------------|
| `catalog_discover.py` | ASIN発掘→利益計算→自動出品（メイン出品エンジン） | `0 16 * * *`（01:00 JST） |
| `recheck_candidates.py` | 候補DBの再チェック→自動出品 | `0 17 * * *`（02:00 JST） |
| `price_update.py` | 全出品を競合価格に追随して更新 | `0 */6 * * *`（0/6/12/18時） |
| `listings_sync.py` | Amazon Reports API と DB を同期 | `0 0 * * *` |
| `health_monitor.py` | 稼働監視・異常検知 | `0 0,12 * * *` |
| `no_jp_optimizer.py` | JP在庫なし出品の価格最適化 | `0 7 * * 1`（月曜） |
| `find_au_sellers.py` | 競合セラー発掘→`seller_urls.txt` 更新 | 手動 |
| `bulk_reactivate.py` | inactive出品の一括回復 | `0 18 * * 0`（日曜） |
| `gmail_sheets_sync.py` | Gmail注文通知→スプレッドシート同期 | `0 */3 * * *` |
| `gemini_analysis.py` | Gemini週次分析レポート | `0 9 * * 1`（月曜） |
| `violation_finder.py` / `violation_deleter.py` | ポリシー違反出品の検出・削除 | 手動 |
| `bulk_delete_inactive.py` | inactive出品の一括削除 | 手動 |

`price_analyzer.py` はローカル手動実行（Playwright で Chrome 起動、配送先を Sydney に設定）。CSV を `csv_output/AU/base/` に出力し、`catalog_discover.py` が 48時間以内のCSVをASIN+価格ソースとして優先使用（GitHub Actions では Playwright スクレイピングにフォールバック）。

### パッケージ構成

- `config.py` — 全パラメータ（環境変数で上書き可能）。数値の閾値・バッファはここに集約。
- `apis/` — `amazon_jp.py` / `amazon_au.py`（SP-API）、`exchange_rate.py`、`finances_au.py`。
- `modules/profit_calc.py` — **利益計算の中核**。`calc_profit()` と `calc_optimal_au_price()`。
- `utils/candidates_db.py` — ASIN候補DB管理 / `utils/notify.py` — メール通知 / `utils/logger.py`。
- `db/` — SQLite スキーマ（`models.py`）と接続（`database.py`）。
- `scraper/au_seller.py` — AUセラーページのスクレイピング。

## 利益計算ロジック（`modules/profit_calc.py`）

```
粗利(JPY) = AU販売収益(手数料控除後をJPY換算) − JP仕入値 − 国際送料
粗利率(%) = 粗利 / JP仕入値 × 100 ≥ MIN_PROFIT_RATE(18%) で出品対象
```

- 国際送料は**重量別**（`get_shipping_jpy`）: ~1kg=DHL¥3,800 / 1〜2kg=¥6,300 / 2kg超=EMS実費近似。重量不明は `DEFAULT_WEIGHT_KG=1.0kg` 扱いで保守的に計算。
- `MAX_LISTING_WEIGHT_KG=1.0` 超は国際送料が高すぎるため出品しない。
- `calc_optimal_au_price()` は `AU_JP_PRICE_BUFFER(1.15)` で JP価格上昇を吸収し、`PRICE_MARKUP_MULTIPLIER(1.3)` を掛けて出品価格を返す。
- Featured Offer 戦略: `BUYBOX_UNDERCUT_RATE(1%)` で競合最安をアンダーカット。ただし競合が `min_price × BUYBOX_MIN_GAP_RATIO(1.15)` 未満の安値セラーには追随せず min_price を維持。

## ⚠️ 過去の重大バグ（診断時必読）

- **競合価格は `ListingPrice + ShippingPrice` の合計を使う**。ListingPrice のみは赤字の原因。
- **`python-amazon-sp-api` の引数は snake_case**（`item_condition="New"` ✅ / `ItemCondition="New"` ❌）。
- 出品数が増えない場合は**削除ループ**を疑う。診断チェックリスト:
  1. GitHub Secret の `MIN_PROFIT_RATE` 実値を確認
  2. `price_update` の直近メールレポートを確認
  3. 出品数推移を確認

## 開発メモ

- 依存: `pip install -r requirements.txt`（`python-amazon-sp-api==2.1.8`, `playwright`, `gspread`, `google-generativeai` 等）。ローカルは venv + `cp .env.example .env`。
- テストフレームワークは無し。破壊的スクリプトは基本 `--dry-run` を持つ（`bulk_delete_inactive.py`, `listings_sync.py`, `main.py list` など）。**まず `--dry-run` で確認する**。
- 為替レート・SP-API を叩くコードは、ループ内で `exchange_rate` を引数で渡して API 呼び出しを減らす（`calc_optimal_au_price` の設計意図）。
- 候補DB（`arbitrage-db` artifact）は Actions 間で GitHub Actions Artifact 経由で受け渡される（`catalog_discover.yml` / `recheck_candidates.yml` 参照）。
- コミットメッセージは Conventional Commits + 日本語説明（例: `fix(price_update): JP価格急落検知で赤字出品を防ぐ`）。既存の履歴に合わせる。
</content>
</invoke>

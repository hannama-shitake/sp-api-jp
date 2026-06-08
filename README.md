# Amazon AU 自動出品・価格管理システム

> ⚠️ このリポジトリは**Public（公開）**。認証情報・APIキー・.envファイルは絶対にコミットしない。

---

## システム概要

Amazon JP→AU FBM ドロップシッピング型アービトラージの自動化システム。  
競合セラーの出品を追跡し、価格を自動更新してFeatured Offer（Buy Box）を狙う。

### KPI目標
- 出品数: **10,000件**
- Featured Offer獲得率: **10%**

---

## システム構成

| スクリプト | 役割 | 実行方式 |
|-----------|------|---------|
| `price_analyzer.py` | 競合価格をCSV取得 | ローカル手動 |
| `catalog_discover.py` | ASIN発掘→利益計算→自動出品 | GitHub Actions 毎日01:00 JST |
| `price_update.py` | 全出品の価格を競合に追随して更新 | GitHub Actions 0/6/12/18 UTC |
| `listings_sync.py` | AmazonレポートとDBを同期 | GitHub Actions 毎日自動 |

### マーケットプレイスID
- JP: `A1VC38T7YXB528` / AU: `A39IBJ37TRP1C6`

---

## 日常ワークフロー

### 出品数を増やしたいとき

```bash
# 1. Price Analyzerを実行（Chromeが起動、配送先をSydneyに手動設定してEnter）
python price_analyzer.py

# 2. CSV生成後に出品実行
python catalog_discover.py --max-new 300
```

CSV出力先: `csv_output/AU/base/amazon_prices_*.csv`（48時間以内のCSVを自動使用）

### GitHub Actions確認
- Actions実行ログ: https://github.com/hannama-shitake/sp-api-jp/actions
- price_updateのメールレポートで正常稼働確認

---

## 設定値（config.py / GitHub Secrets）

| 変数 | 値 | 説明 |
|------|----|------|
| MIN_PROFIT_RATE | 18% | **Secretsの実値を必ず確認** |
| PRICE_MARKUP_MULTIPLIER | 1.3 | JP仕入れ価格への掛け率 |
| MAX_FAIR_PRICE_RATIO | 4.0 | 競合価格の上限倍率 |
| MAX_LISTING_WEIGHT_KG | 1.0 | 出品重量上限（kg） |
| AU_FEE_RATE | 0.15 | Amazon AU手数料率 |
| MIN_AU_LISTING_PRICE | 45.0 | AU最低出品価格（AUD） |
| LISTING_PRICE_BUFFER_AUD | 35.0 | 出品時バッファ（price_updateが6h以内に修正） |

---

## ⚠️ 過去の重大バグ（診断時必読）

### 診断時チェックリスト
1. GitHub SecretのMIN_PROFIT_RATE実値確認
2. price_updateの直近メールレポート確認
3. 出品数推移（増えてないなら削除ループ疑う）

### 技術的注意点
- **競合価格はListingPrice + ShippingPriceの合計を使う**（ListingPriceのみは赤字の原因）
- **sp-api-pythonライブラリの引数はsnake_case**（`item_condition="New"` ✅ / `ItemCondition="New"` ❌）

---

## ファイル構成

```
sp-api/
├── catalog_discover.py   # メイン出品エンジン
├── price_update.py       # 価格自動更新
├── price_analyzer.py     # 価格スクレイパー
├── listings_sync.py      # DB同期
├── config.py             # 設定値（環境変数対応）
├── ng_words.json         # NGワード辞書
├── modules/
│   └── profit_calc.py    # 利益計算ロジック
├── utils/
│   ├── candidates_db.py  # ASIN候補DB管理
│   └── notify.py         # メール通知
├── csv_output/AU/base/   # 価格スクレイパーのCSV出力先
└── .github/workflows/    # GitHub Actions定義
```

---

## 環境セットアップ（初回のみ）

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# .env に認証情報を設定
```

### .env 設定項目

```env
# Amazon JP SP-API
AMAZON_JP_REFRESH_TOKEN=Atza|...
AMAZON_JP_LWA_CLIENT_ID=amzn1.application-oa2-client.xxx
AMAZON_JP_LWA_CLIENT_SECRET=xxx

# Amazon AU SP-API
AMAZON_AU_REFRESH_TOKEN=Atza|...
AMAZON_AU_LWA_CLIENT_ID=amzn1.application-oa2-client.xxx
AMAZON_AU_LWA_CLIENT_SECRET=xxx

# AWS IAM（JP/AU 共通）
AWS_ACCESS_KEY=AKIA...
AWS_SECRET_KEY=xxx
```

### SP-API認証情報の取得
1. セラーセントラル →「アプリとサービス」→「アプリの開発」→ LWA Client ID / Secret を取得
2. セラーセントラル → 認証ボタン → Refresh Token を取得
3. AWS IAMコンソール → ユーザー作成 → アクセスキーを取得
4. JP / AU それぞれ別のセラーセントラルで実施

参考: https://developer-docs.amazon.com/sp-api/

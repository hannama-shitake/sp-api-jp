# Amazon AU 自動出品・価格管理システム

achoo合同会社 / 担当: 山本 Takeshi  
Gmail: no.more.awamori@gmail.com（SP-API通知・Amazon AU・eBay全部ここ）  
GitHub: https://github.com/hannama-shitake/sp-api-jp  
ローカル: `C:\Users\user\Desktop\sp-api\`

> ⚠️ このリポジトリは**Public（公開）**。認証情報・APIキー・.envファイルは絶対にコミットしない。
> ⚠️ YM商会Gmail（degital.sales.ymcorp@gmail.com）と混在禁止

---

## ビジネス概要

**特定セラーがAmazon AUで売っている商品に相乗り出品し、競合1%アンダーカットで売る。**

- JP→AU FBM ドロップシッピング（注文が来たらJPで購入してAUに発送）
- 対象セラーURLは `seller_urls.txt` で管理

### KPI目標
- 出品数: **10,000件**（2026-06現在 ~1,200件 active）
- Featured Offer獲得率: **10%**（1,000件でBuy Box = 毎日数十件の受注ペース）

---

## システム構成

| スクリプト | 役割 | 実行方式 |
|-----------|------|---------|
| `price_analyzer.py` | Selenium実ブラウザで競合価格をCSV取得 | ローカル手動 |
| `catalog_discover.py` | ASIN発掘→利益計算→自動出品 | GitHub Actions 毎日01:00 JST |
| `price_update.py` | 全出品の価格を競合に追随して更新 | GitHub Actions 0/6/12/18 UTC |
| `listings_sync.py` | AmazonレポートとDBを同期 | GitHub Actions 毎日自動 |

### マーケットプレイスID
- JP: `A1VC38T7YXB528` / AU: `A39IBJ37TRP1C6`

---

## 日常ワークフロー

### 出品数を増やしたいとき（Price Analyzerワークフロー）

```bash
# 1. Price Analyzerを実行（Chromeが起動、配送先を2002 Sydneyに手動設定してEnter）
python price_analyzer.py

# 2. CSV生成後に出品実行
python catalog_discover.py --max-new 300
```

CSV出力先: `csv_output/AU/base/amazon_prices_*.csv`（48時間以内のCSVを自動使用）

### GitHub Actions確認
- Actions実行ログ: https://github.com/hannama-shitake/sp-api-jp/actions
- price_updateのGmailレポートで正常稼働確認（no.more.awamori@gmail.com）

---

## 設定値（config.py / GitHub Secrets）

| 変数 | 値 | 説明 |
|------|----|------|
| MIN_PROFIT_RATE | 18% | **Secretsの実値を必ず確認**（過去に30%になっていた事故あり） |
| PRICE_MARKUP_MULTIPLIER | 1.3 | JP仕入れ価格への掛け率 |
| MAX_FAIR_PRICE_RATIO | 4.0 | 競合価格の上限倍率 |
| MAX_LISTING_WEIGHT_KG | 1.0 | 出品重量上限 |
| AU_FEE_RATE | 0.15 | Amazon AU手数料率 |
| MIN_AU_LISTING_PRICE | 45.0 | AU最低出品価格（AUD） |
| LISTING_PRICE_BUFFER_AUD | 35.0 | 出品時バッファ（price_updateが6h以内に修正） |

---

## ⚠️ 絶対に忘れるな（過去の重大バグ）

### 診断時チェックリスト
1. GitHub SecretのMIN_PROFIT_RATE実値確認（デフォルト18%と違う可能性）
2. price_updateの直近Gmailレポート確認
3. 出品数推移（増えてないなら削除ループ疑う）

### やってはいけないこと
- **「3セラー不在→削除」ロジックは絶対に使わない**  
  `get_item_offers`は競合上位のみ返す→3セラーが出品中でも含まれないことがある（2026-05-29: 大量誤削除）
- **競合価格はListingPrice + ShippingPriceの合計を使う**  
  ListingPriceのみだと送料別建て競合に対して大幅安値出品になる（2026-06-04: 修正済み）
- **sp-api-pythonライブラリの引数はsnake_case**  
  `item_condition="New"` ✅ / `ItemCondition="New"` ❌（2026-05-28: 全滅）

---

## ファイル構成

```
sp-api/
├── catalog_discover.py   # メイン出品エンジン
├── price_update.py       # 価格自動更新
├── price_analyzer.py     # Selenium価格スクレイパー
├── listings_sync.py      # DB同期
├── config.py             # 設定値（環境変数対応）
├── seller_urls.txt       # 3セラーURL固定
├── ng_words.json         # NGワード辞書
├── modules/
│   └── profit_calc.py    # 利益計算ロジック
├── utils/
│   ├── candidates_db.py  # ASIN候補DB管理
│   └── notify.py         # Gmail通知
├── csv_output/AU/base/   # Price AnalyzerのCSV出力先
└── .github/workflows/    # GitHub Actions定義
```

---

## 環境セットアップ（初回のみ）

```bash
cd C:/Users/user/Desktop/sp-api
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# .env に認証情報を設定（下記参照）
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

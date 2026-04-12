# Amazon SP-API セットアップガイド

## 1. Amazon SP-API 認証情報の取得

### 手順

#### Step 1: Amazon セラーセントラルにログイン
- JP: https://sellercentral.amazon.co.jp
- AU: https://sellercentral.amazon.com.au

#### Step 2: 開発者として登録
1. セラーセントラル右上の「設定」→「ユーザー権限」→「開発者向け情報」
2. 「デベロッパー登録」ボタンをクリック
3. アプリケーション名・説明を入力して登録完了

#### Step 3: IAM ユーザー作成（AWS）
1. https://aws.amazon.com/jp/ でAWSアカウント作成（無料）
2. IAMコンソール → 「ユーザー」→「ユーザーを作成」
3. ユーザー名を入力（例: `sp-api-user`）
4. 「次のステップ: アクセス許可」→「既存のポリシーを直接アタッチ」
5. `AmazonEC2FullAccess` を選択（SP-API用）
6. 作成後、**アクセスキーID** と **シークレットアクセスキー** を保存

#### Step 4: SP-API アプリを登録
1. セラーセントラル →「アプリとサービス」→「アプリの開発」
2. 「新しいアプリバージョンを追加」
3. 以下を入力:
   - アプリ名: `arbitrage-system`
   - IAM ARN: Step 3 で作成したIAMユーザーのARN（例: `arn:aws:iam::123456789:user/sp-api-user`）
4. 「ドラフトを保存」→「OAuth認証情報を表示」
   - **LWA Client ID** をコピー
   - **LWA Client Secret** をコピー

#### Step 5: Refresh Token の取得
1. 同じページで「認証」ボタンをクリック
2. セラーアカウントでログインして権限を承認
3. 表示された **Refresh Token** をコピー

#### Step 6: AU マーケットプレイスにも適用
- AU セラーセントラルでも同様に Step 2〜5 を実施
- または、JP と AU が同一セラーアカウントの場合は JP の Refresh Token をそのまま使用可能

---

## 2. eBay API 認証情報の取得（Phase 2 用）

#### Step 1: eBay Developer アカウント作成
- https://developer.ebay.com/signin でアカウント作成

#### Step 2: アプリケーションを作成
1. 「My Account」→「Application Keys」
2. 「Create a Keyset」→ Production を選択
3. アプリ名を入力して作成
4. 以下を取得:
   - **App ID (Client ID)**
   - **Dev ID**
   - **Cert ID (Client Secret)**

#### Step 3: User Access Token 取得
1. OAuth Credentials ページでコールバックURLを設定
2. 「Get a Token from eBay via Your Application」でトークン取得
3. **Refresh Token** をコピー

---

## 3. .env ファイルの設定

`sp-api/.env` ファイルを作成して以下を設定:

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

# eBay API（Phase 2）
EBAY_APP_ID=xxx
EBAY_DEV_ID=xxx
EBAY_CERT_ID=xxx
EBAY_REFRESH_TOKEN=xxx
```

---

## 4. Python 環境セットアップ

```bash
# Python 3.11+ が必要
python --version

# 仮想環境作成
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Mac/Linux

# 依存関係インストール
pip install -r requirements.txt
```

---

## 5. 動作確認

```bash
# API接続テスト
python main.py test-connection

# スクレイピングテスト（--dry-run で出品なし）
python main.py scrape --url "https://www.amazon.com.au/s?me=YOUR_SELLER_ID" --dry-run

# 利益計算テスト
python main.py research --dry-run

# 本番出品（--dry-run なし）
python main.py list

# 監視スケジューラー起動
python scheduler.py
```

---

## 参考リンク
- SP-API ドキュメント: https://developer-docs.amazon.com/sp-api/
- SP-API Python ライブラリ: https://github.com/saleweaver/python-amazon-sp-api
- eBay API ドキュメント: https://developer.ebay.com/docs

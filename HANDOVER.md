# 引き継ぎ書 — achoo合同会社 SP-APIアービトラージシステム

作成日: 2026-03-29

---

## 担当者情報

| 項目 | 内容 |
|------|------|
| 担当者 | 山本（TAKESHI） |
| 個人会社 | achoo合同会社 |
| Gmail | no.more.awamori@gmail.com |
| 勤め先 | YM商会（別事業・混在禁止） |

---

## プロジェクト概要

**Amazon JP で仕入れ → Amazon AU に FBM 相乗り出品するドロップシッピング型アービトラージ自動化システム**

- Phase 1: Amazon JP → Amazon AU（今回構築済み）
- Phase 2: Amazon JP → eBay（未着手）

---

## 構築済みシステム（C:/Users/user/Desktop/sp-api/）

```
sp-api/
├── setup_guide.md          ← SP-API認証情報取得手順（まず読む）
├── .env.example            ← 環境変数テンプレート
├── requirements.txt        ← pip install -r requirements.txt
├── config.py               ← 設定値（粗利率・送料・手数料）
├── main.py                 ← CLIエントリーポイント
├── scheduler.py            ← 価格・在庫監視バッチ
├── db/
│   ├── database.py         ← SQLite初期化
│   └── models.py           ← データモデル
├── scraper/
│   └── au_seller.py        ← AUセラーページスクレイピング
├── apis/
│   ├── amazon_jp.py        ← JP SP-API（商品・価格取得）
│   ├── amazon_au.py        ← AU SP-API（FBM相乗り出品・更新）
│   └── exchange_rate.py    ← JPY→AUD為替レート
└── modules/
    ├── profit_calc.py      ← 粗利計算エンジン
    ├── product_matcher.py  ← AU ASIN → JP照合
    ├── listing_manager.py  ← 出品・停止・価格更新
    └── price_monitor.py    ← JP価格/在庫監視→AU自動更新
```

### ビジネスフロー
```
AU競合セラーURL → スクレイピング → JP照合 → 粗利計算
→ MIN_PROFIT_RATE(30%)以上 → AU FBM相乗り出品
→ JP価格/在庫監視 → AU価格・在庫自動更新
```

### CLIコマンド
```bash
python main.py test-connection
python main.py research --url "https://www.amazon.com.au/s?me=XXX"
python main.py list --url "..." --dry-run
python main.py list --url "..."
python main.py monitor price
python main.py monitor stock
python main.py status
python scheduler.py
```

---

## 未完了タスク（次のClaudeへ）

### 優先度 高
1. **SP-API 認証情報の取得・設定**
   - setup_guide.md を参照して .env を作成
   - Amazon JP / AU 両方の Refresh Token / LWA Client ID / Secret が必要
   - `python main.py test-connection` で確認

2. **Gmail 注文管理モジュール（modules/order_manager.py）**
   - no.more.awamori@gmail.com の Gmail API で注文メールを監視
   - Amazon AU / eBay の注文通知を解析
   - JP仕入れタスクを自動生成
   - Google Cloud Console で `achoo-arbitrage` プロジェクトを作成して Gmail API を有効化

3. **Gemini 連携（apis/gemini.py）**
   - JP商品タイトル（日本語）→ AU向け英語タイトル/説明文を自動生成
   - CoWork指示書のGeminiプロンプトパターンを転用可能
   - google-generativeai ライブラリを使用
   - Gemini API キーは no.more.awamori@gmail.com の Google AI Studio から取得

### 優先度 中
4. **eBay 出品機能（Phase 2）**
   - apis/ebay.py を実装
   - ebaysdk ライブラリ使用

---

## 重要な注意事項

### アカウント分離（絶対厳守）
| 会社 | Gmail | 用途 |
|------|-------|------|
| achoo合同会社（本プロジェクト） | no.more.awamori@gmail.com | Amazon AU / eBay |
| YM商会（勤め先・別事業） | degital.sales.ymcorp@gmail.com | ヤフオク・メルカリ |

**2つのアカウントを混在させない。**

### Gemini 活用のヒント
山本さんはYM商会のCoWorkプロジェクトでGeminiを使った実績あり。
動画 → Gemini → JSON抽出のパターンが確立済み（Google Driveの「CoWork専用指示書_YM商会」参照）。
このパターンをAU向け商品説明文生成に転用できる。

### SP-API について
- 今回が完全初挑戦（Gmail・Driveに過去の記録なし）
- JP・AU それぞれ別のセラーセントラルアカウントで認証が必要
- マーケットプレイスID: JP = A1VC38T7YXB528 / AU = A39IBJ37TRP1C6

---

## 環境セットアップ手順

```bash
cd C:/Users/user/Desktop/sp-api
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# .env を編集して認証情報を設定
python main.py test-connection
```

---

## Gmail MCP 接続状況

このドキュメント作成時点では Claude の Gmail コネクタが
`degital.sales.ymcorp@gmail.com`（YM商会）に接続されていた。

山本さんが `no.more.awamori@gmail.com`（achoo合同会社）に
切り替え作業中のため、次のセッション開始時に接続確認を行うこと。

確認コマンド（Claude内）: Gmail プロフィールを取得して
`no.more.awamori@gmail.com` になっているか確認する。

---

以上。次のClaudeへ — よろしく頼みます。

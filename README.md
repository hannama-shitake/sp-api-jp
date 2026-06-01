# Amazon AU 自動出品・価格管理システム

> JP→AU FBM ドロップシッピング型アービトラージの完全自動化。  
> **非エンジニアが生成AI（Claude）をディレクションして単独で構築・運用中。**

---

## システム概要

Amazon JP で仕入れ → Amazon AU に FBM 相乗り出品するドロップシッピング型の  
輸出アービトラージ業務を、**ゼロから設計・実装・本番運用**しているシステム。

```
【データフロー】

競合セラーの出品ページ
        ↓  Selenium/Playwright スクレイピング
    ASIN候補DB（SQLite）
        ↓  Amazon SP-API で価格・在庫・制限チェック
    出品判定（利益計算 / NGワード / 重量制限）
        ↓  ListingsItems API で自動出品
  Amazon AU マーケットプレイス（現在 1,200件超 出品中）
        ↓  6時間ごと price_update が価格を自動更新
    競合1%アンダーカット → Featured Offer（Buy Box）狙い
```

---

## 主な機能

| スクリプト | 役割 | 実行方式 |
|-----------|------|---------|
| `price_analyzer.py` | Selenium実ブラウザで競合価格をCSV取得 | ローカル手動 |
| `catalog_discover.py` | ASIN発掘→利益計算→自動出品 | GitHub Actions 毎日自動 |
| `price_update.py` | 全出品の価格を競合に追随して更新 | GitHub Actions 6時間ごと |
| `listings_sync.py` | AmazonレポートとDBを同期 | GitHub Actions 毎日自動 |

---

## 技術スタック

- **言語**: Python 3.x
- **API連携**: Amazon SP-API（商品・価格・出品・レポート）
- **スクレイピング**: Selenium / Playwright（ヘッドレスChromium）
- **DB**: SQLite（出品候補 13,000件超を管理）
- **自動化**: GitHub Actions（CI/CD・定期実行）
- **通知**: Gmail SMTP（実行レポートをメール送信）
- **その他**: 為替レートAPI連携、Webshareプロキシ対応

---

## 設計のこだわり

### 1. 現場が止まらない堅牢設計
- API失敗時の自動リトライ・フォールバック
- プロキシ障害時のノープロキシ自動切替
- 大量削除バグの即時検知・緊急revert体制

### 2. ビジネスリスクを仕組みで排除
- **商標侵害リスク**: NGワード辞書で自動除外
- **赤字リスク**: FBA安値品スキップ・最低価格フロア設定
- **過剰在庫リスク**: JP在庫連動の出品管理
- **真贋リスク**: 独占出品・メーカー直販ブランドの除外

### 3. ゼロエクストラコストの自動化
- GitHub Actionsの無料枠で**全自動運用**
- 月額固定費: プロキシ ¥580/月 のみ

---

## 運用実績

- 出品数: **1,200件超**（自動発掘・出品）
- 価格更新: **459件/6時間**（price_update安定稼働中）
- 候補ASIN管理: **13,000件超**をSQLiteで管理
- 自動化率: **95%以上**（人手介入はスクレイピング起動のみ）

---

## 構築の経緯

プログラミング経験ゼロの状態から、  
生成AI（Claude Code）に要件を伝えながら**全工程を自力で設計・実装**。

- Amazon SP-API の仕様理解から認証設定まで独力で突破
- バグが出るたびにログを読み解き、根本原因を特定して修正
- 「動けばいい」ではなく、**ビジネスリスクを排除した堅牢な設計**を意識

> *「コードを書く」より「現場が止まらない仕組みを作る」ことに注力した。*

---

## ファイル構成

```
sp-api/
├── catalog_discover.py   # メイン出品エンジン
├── price_update.py       # 価格自動更新
├── price_analyzer.py     # Selenium価格スクレイパー
├── listings_sync.py      # DB同期
├── config.py             # 設定値（環境変数対応）
├── modules/
│   └── profit_calc.py    # 利益計算ロジック
├── utils/
│   ├── candidates_db.py  # ASIN候補DB管理
│   └── notify.py         # Gmail通知
└── .github/workflows/    # GitHub Actions定義
```

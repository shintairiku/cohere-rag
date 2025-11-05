# Image Search System with Cohere Embeddings

企業ごとにGoogle Driveの画像をベクトル化し、類似画像検索を行うシステムです。Google Apps ScriptによるスプレッドシートUIとCloud Run上のAPIを組み合わせて、直感的な画像検索体験を提供します。

## 🎯 機能概要

### 主な機能
- **自動ベクトル化**: Google Drive内の画像をCohere APIでベクトル化
- **類似画像検索**: テキストクエリから類似度の高い画像を検索
- **ランダム画像検索**: ランダムに画像を選択表示
- **スプレッドシートUI**: Google Sheetsを操作インターフェースとして活用
- **企業別管理**: UUIDによる企業別のデータ分離

### システム構成
```
Google Sheets (UI) ←→ Cloud Run API ←→ Cohere API
                              ↓
                       Google Cloud Storage
                              ↓
                       Vector Data (JSON)
```

## 📋 必要な環境・アカウント

### Google Cloud Platform
- プロジェクト作成済み
- Cloud Run API有効化
- Cloud Storage API有効化
- Cloud Build API有効化
- サービスアカウント設定済み

### 外部サービス
- [Cohere](https://cohere.com/) APIキー
- Google Driveアクセス権限

### 開発環境
- Python 3.9+
- Docker
- gcloud CLI

## 🚀 セットアップ手順

### 1. 環境変数の設定

`.env`ファイルを作成し、以下の変数を設定：

```env
# Google Cloud
GCP_PROJECT_ID=your-project-id
GCS_BUCKET_NAME=your-bucket-name
GCP_REGION=asia-northeast1

# Cohere API
COHERE_API_KEY=your-cohere-api-key

# Cloud Run Job
# 本番環境
VECTORIZE_JOB_NAME=cohere-rag-vectorize-job
# 開発環境
# VECTORIZE_JOB_NAME=cohere-rag-vectorize-job-dev
ENVIRONMENT=production
```

### 2. Google Cloud Storageバケットの作成

```bash
gsutil mb gs://your-bucket-name
```

### 3. Cloud Buildトリガーの設定

1. Google Cloud Consoleでトリガーを作成
2. リポジトリを連携
3. `cloudbuild.yaml`を使用する設定に変更
4. 以下の置換変数を設定：

```yaml
_AR_HOSTNAME: asia-northeast1-docker.pkg.dev
_AR_PROJECT_ID: your-project-id
_AR_REPOSITORY: cohere-rag
_SERVICE_NAME: cohere-rag
_JOB_NAME_VECTORIZE: cohere-rag-vectorize-job        # 本番トリガー
# _JOB_NAME_VECTORIZE: cohere-rag-vectorize-job-dev  # 開発トリガー
_DEPLOY_REGION: asia-northeast1
_COHERE_API_KEY: your-cohere-api-key
_GCS_BUCKET_NAME: your-bucket-name
```

### 4. Google Apps Scriptの設定

1. `api_caller.gs`内の`API_BASE_URL`を実際のCloud Run URLに更新
2. Google Sheetsでスクリプトエディタを開き、コードを貼り付け
3. 必要な権限を承認

## 📊 使用方法

### スプレッドシートの準備

1. **会社一覧シート**を作成：
   - A列: UUID（自動生成）
   - B列: 会社名
   - C列: Google Drive URL

2. **各企業用シート**を作成（例：`platform-sample`）：
   - A列: 検索クエリ
   - C列: 実行状況（プルダウン）
   - D列以降: 検索結果

### ベクトル化の実行

1. 会社一覧シートで対象企業の行を選択
2. メニュー「✨画像検索メニュー」→「選択行のベクトル化を実行」
3. Cloud Run Jobが起動し、Google Drive内の画像をベクトル化

### 画像検索の実行

1. 企業シートのA列に検索したいテキストを入力
2. C列で「類似画像検索」または「ランダム画像検索」を選択
3. 自動的にAPIが呼び出され、結果が表示

## 🏗️ システムアーキテクチャ

### コンポーネント構成

#### 1. Cloud Run Service (`main.py`)
- **FastAPI**ベースのRESTful API
- エンドポイント：
  - `POST /vectorize`: ベクトル化ジョブのトリガー
  - `GET /search`: 画像検索の実行
  - `GET /`: ヘルスチェック

#### 2. Cloud Run Job (`img_meta_processor_gdrive.py`)
- Google Drive画像の取得・処理
- Cohere APIでのマルチモーダルベクトル化
- 結果のCloud Storageへの保存

#### 3. Search Engine (`search.py`)
- ベクトルデータの読み込み
- コサイン類似度による検索
- ランダム検索機能

#### 4. Google Apps Script (`api_caller.gs`)
- スプレッドシートUI
- API呼び出し
- 結果の表示・整形

## 🔧 主要ファイル

### バックエンド（Python）
- `main.py` - FastAPI メインアプリケーション
- `search.py` - 画像検索エンジン
- `img_meta_processor_gdrive.py` - ベクトル化ジョブ
- `drive_scanner.py` - Google Drive スキャナー

### インフラ
- `Dockerfile` - メインサービス用コンテナ
- `Dockerfile.job` - ジョブ用コンテナ
- `cloudbuild.yaml` - Cloud Build設定
- `pyproject.toml` - Python依存関係とビルドメタデータ

### フロントエンド
- `api_caller.gs` - Google Apps Script

## 🛠️ トラブルシューティング

### よくある問題

#### 1. ベクトル化が失敗する
- Google Driveの共有設定を確認
- サービスアカウントに適切な権限があるか確認
- 画像ファイル形式がサポート対象か確認

#### 2. 検索結果が表示されない
- ベクトルデータが正常に作成されているか確認
- API URLが正しく設定されているか確認

#### 3. Google Apps Scriptでエラーが発生
- スクリプトの実行権限を確認
- APIエンドポイントの認証設定を確認

### ログの確認方法

```bash
# Cloud Run Service のログ
gcloud logs read --filter="resource.type=cloud_run_revision"

# Cloud Run Job のログ  
gcloud logs read --filter="resource.type=cloud_run_job"
```

## 📝 技術仕様

- **プログラミング言語**: Python 3.9+, JavaScript (Google Apps Script)
- **フレームワーク**: FastAPI, Google Apps Script
- **ML API**: Cohere embed-multilingual-v3.0
- **インフラ**: Google Cloud Run, Cloud Storage, Cloud Build
- **UI**: Google Sheets

---

**バージョン**: 1.0.0  
**最終更新**: 2025年9月

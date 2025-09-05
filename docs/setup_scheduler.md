# Incremental Update Scheduler Setup Guide

このガイドでは、Google Drive内の画像変更を自動検出して差分更新する機能のセットアップ方法を説明します。

## 📋 概要

実装された機能：

1. **差分検出**: ファイルの変更時刻とサイズで変更を検出
2. **効率的な更新**: 追加・更新・削除されたファイルのみ処理
3. **バッチ処理**: 複数の企業を並行処理
4. **自動スケジュール**: Cloud Schedulerと連携
5. **通知機能**: メール・Slack通知
6. **エラー処理**: 詳細なログと復旧機能

## 🚀 デプロイ手順

### 1. 環境変数の設定

```bash
# 必須設定
export GCS_BUCKET_NAME="your-bucket-name"
export COHERE_API_KEY="your-cohere-api-key"
export COMPANY_SPREADSHEET_ID="your-spreadsheet-id"

# オプション設定
export MAX_WORKERS="3"
export NOTIFICATION_EMAIL="admin@yourcompany.com"
export SMTP_HOST="smtp.gmail.com"
export SMTP_PORT="587"
export SMTP_USER="your-smtp-user"
export SMTP_PASSWORD="your-smtp-password"
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
```

### 2. Secretの作成

```bash
# Cohere APIキー
gcloud secrets create cohere-api-key --data-file=<(echo -n "your-cohere-api-key")

# メール設定
gcloud secrets create email-config \
  --data-file=<(echo '{"user":"your-smtp-user","password":"your-smtp-password"}')

# Slack設定
gcloud secrets create slack-config \
  --data-file=<(echo '{"webhook-url":"your-webhook-url"}')
```

### 3. Cloud Run Jobのデプロイ

```bash
# 設定ファイルを更新
sed -i 's/PROJECT_ID/your-project-id/g' scheduler-job.yaml
sed -i 's/your-bucket-name/actual-bucket-name/g' scheduler-job.yaml
sed -i 's/your-spreadsheet-id/actual-spreadsheet-id/g' scheduler-job.yaml
sed -i 's/your-notification@email.com/actual-email@domain.com/g' scheduler-job.yaml

# Cloud Buildでデプロイ
gcloud builds submit --config=cloudbuild-scheduler.yaml
```

### 4. Cloud Schedulerの設定

```bash
# 毎日午前2時に実行
gcloud scheduler jobs create http incremental-update-daily \
  --schedule="0 2 * * *" \
  --uri="https://asia-northeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/your-project-id/jobs/incremental-updater-scheduler:run" \
  --http-method=POST \
  --oidc-service-account-email="your-service-account@your-project-id.iam.gserviceaccount.com" \
  --location=asia-northeast1 \
  --time-zone="Asia/Tokyo"

# 毎週月曜日午前1時に実行（より頻繁な更新が必要な場合）
gcloud scheduler jobs create http incremental-update-weekly \
  --schedule="0 1 * * 1" \
  --uri="https://asia-northeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/your-project-id/jobs/incremental-updater-scheduler:run" \
  --http-method=POST \
  --oidc-service-account-email="your-service-account@your-project-id.iam.gserviceaccount.com" \
  --location=asia-northeast1 \
  --time-zone="Asia/Tokyo"
```

## 🔧 使用方法

### コマンドライン実行

```bash
# 単一企業の差分更新
python incremental_updater.py --uuid "company-uuid" --drive-url "https://drive.google.com/drive/folders/..."

# バッチ差分更新（Google Sheetsから）
python batch_incremental_updater.py --spreadsheet-id "your-spreadsheet-id"

# バッチ差分更新（JSONファイルから）
python batch_incremental_updater.py --companies-file companies.json

# スケジュール実行
python scheduler.py --mode update

# ヘルスチェック
python scheduler.py --mode health
```

### API経由の実行

```bash
# 単一企業の差分更新
curl -X POST "https://your-api-url/incremental-update" \
  -H "Content-Type: application/json" \
  -d '{
    "uuid": "company-uuid",
    "drive_url": "https://drive.google.com/drive/folders/..."
  }'

# バッチ差分更新
curl -X POST "https://your-api-url/batch-incremental-update" \
  -H "Content-Type: application/json" \
  -d '{
    "spreadsheet_id": "your-spreadsheet-id",
    "max_workers": 3
  }'

# ヘルスチェック
curl "https://your-api-url/health"
```

## 📊 監視とログ

### Cloud Loggingでのログ確認

```bash
# スケジュール実行のログ
gcloud logs read "resource.type=cloud_run_job AND resource.labels.job_name=incremental-updater-scheduler" \
  --limit=50 --format="table(timestamp,textPayload)"

# エラーログの確認
gcloud logs read "resource.type=cloud_run_job AND severity>=ERROR" \
  --limit=20 --format="table(timestamp,severity,textPayload)"
```

### Cloud Monitoringでのアラート設定

```bash
# エラー率のアラート
gcloud alpha monitoring policies create --policy-from-file=monitoring-policy.yaml
```

## 🔄 差分更新の仕組み

### 変更検出ロジック

1. **ファイル一覧の取得**: Google Drive APIで現在のファイル状態を取得
2. **既存embedding.jsonの読み込み**: 現在保存されているembedding情報を取得
3. **差分計算**:
   - **追加ファイル**: Driveにあるが embeddings にないファイル
   - **更新ファイル**: 変更時刻またはサイズが異なるファイル  
   - **削除ファイル**: embeddingsにあるがDriveにないファイル
4. **処理実行**: 変更があったファイルのみembedding処理
5. **結果保存**: 更新されたembedding.jsonを保存（バックアップ付き）

### パフォーマンス最適化

- **並行処理**: 複数企業・ファイルの並行処理
- **バッチAPI呼び出し**: Cohere APIの効率的な利用
- **メタデータキャッシュ**: ファイル情報の高速比較
- **段階的処理**: メモリ効率を考慮した処理

## 🚨 トラブルシューティング

### よくある問題

1. **権限エラー**:
   ```bash
   # サービスアカウントの権限確認
   gcloud projects get-iam-policy your-project-id
   ```

2. **APIクォータ制限**:
   - Cohere API: 1000 requests/minute
   - Google Drive API: 10,000 requests/100 seconds/user

3. **メモリ不足**:
   - Cloud Run Jobのメモリを増量: `memory: 8Gi`
   - max_workersの調整: `MAX_WORKERS=2`

### ログでの診断

```bash
# 詳細ログの有効化
export LOG_LEVEL=DEBUG

# 特定企業のログ確認
gcloud logs read "textPayload:\"company-uuid\"" --limit=100
```

## 📈 運用のベストプラクティス

1. **定期実行頻度**: 画像更新頻度に応じて調整（推奨: 日次）
2. **リソース監視**: CPU・メモリ使用量の定期確認
3. **コスト最適化**: 不要なバックアップファイルの定期削除
4. **通知設定**: 失敗時の即座な通知設定
5. **テスト環境**: 本番前のテスト実行

## 🔐 セキュリティ

- APIキーはGoogle Secret Managerで管理
- サービスアカウントは最小権限の原則
- VPCファイアウォールでのアクセス制限
- 定期的な権限監査

## 📝 設定ファイル例

### companies.json（ファイルベースの設定）
```json
{
  "companies": [
    {
      "uuid": "company-1-uuid",
      "name": "株式会社サンプル1",
      "drive_url": "https://drive.google.com/drive/folders/1abc..."
    },
    {
      "uuid": "company-2-uuid", 
      "name": "株式会社サンプル2",
      "drive_url": "https://drive.google.com/drive/folders/2def..."
    }
  ]
}
```

この仕組みにより、効率的で自動化された差分更新システムが構築できます。
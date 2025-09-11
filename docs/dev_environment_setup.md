# 開発環境セットアップ手順

## 1. GCS バケットの作成

### embedding_storage_dev バケットを作成
```bash
gsutil mb -p marketing-automation-461305 -l asia-northeast1 gs://embedding_storage_dev/
```

## 2. Dockerイメージのビルドとプッシュ（重要：最初に実行）

### 方法1: スクリプトを使用（推奨）
```bash
# Artifact Registry への認証設定
gcloud auth configure-docker asia-northeast1-docker.pkg.dev

# スクリプトの実行
cd /home/yuki/development/cohere-rag
./deployment/build-dev-images.sh
```

### 方法2: Cloud Build トリガーを使用
```bash
# Cloud Build トリガーを実行してイメージをビルド
gcloud builds triggers run cohere-rag-dev-deploy --branch=search_with_meta
```

**注意**: Cloud Run Jobsを作成する前に、必ずイメージをビルドしてプッシュしてください。

## 3. Cloud Run Jobs の作成

### 3.1 cohere-rag-vectorize-job-dev の作成
```bash
gcloud run jobs create cohere-rag-vectorize-job-dev \
  --image=asia-northeast1-docker.pkg.dev/marketing-automation-461305/cloud-run-source-deploy/cohere-rag-vectorize-job-dev:latest \
  --region=asia-northeast1 \
  --service-account=marketing-automation@marketing-automation-461305.iam.gserviceaccount.com \
  --set-env-vars="GCS_BUCKET_NAME=embedding_storage_dev,COHERE_API_KEY=6T3N7PvvKj0J3xAj6WbXvSKQAms4VlEQ3oAFXFIL,ENVIRONMENT=development" \
  --task-timeout=36000 \
  --parallelism=1 \
  --max-retries=1
```

### 3.2 scheduler-job-dev の作成
```bash
gcloud run jobs create scheduler-job-dev \
  --image=asia-northeast1-docker.pkg.dev/marketing-automation-461305/cloud-run-source-deploy/scheduler-job-dev:latest \
  --region=asia-northeast1 \
  --service-account=marketing-automation@marketing-automation-461305.iam.gserviceaccount.com \
  --set-env-vars="GCS_BUCKET_NAME=embedding_storage_dev,COHERE_API_KEY=6T3N7PvvKj0J3xAj6WbXvSKQAms4VlEQ3oAFXFIL,ENVIRONMENT=development,VECTORIZE_JOB_NAME=cohere-rag-vectorize-job-dev,MAX_WORKERS=3" \
  --task-timeout=3600 \
  --parallelism=1 \
  --max-retries=3
```

## 4. Cloud Run Service (cohere-rag-dev) の環境変数設定

Google Cloud Console で以下の環境変数を設定:

| 環境変数名 | 値 |
|-----------|-----|
| GCP_PROJECT_ID | marketing-automation-461305 |
| GCS_BUCKET_NAME | embedding_storage_dev |
| COHERE_API_KEY | [実際のAPIキー] |
| VECTORIZE_JOB_NAME | cohere-rag-vectorize-job-dev |
| SCHEDULER_JOB_NAME | scheduler-job-dev |
| GCP_REGION | asia-northeast1 |
| ENVIRONMENT | development |

## 5. Cloud Build トリガーの更新

### 5.1 置換変数の設定
Cloud Build トリガー `cohere-rag-dev-deploy` で以下の置換変数を更新:
- `_COHERE_API_KEY`: 実際のCohere APIキーを設定

## 6. IAM 権限の確認

サービスアカウント `marketing-automation@marketing-automation-461305.iam.gserviceaccount.com` に以下の権限があることを確認:

- **Cloud Run 管理者**
- **Cloud Storage 管理者** (embedding_storage_dev バケットへのアクセス)
- **Cloud Build サービスアカウント**
- **Artifact Registry 書き込み**

## 7. 後続のデプロイ

### 自動デプロイ
search_with_metaブランチにプッシュすると自動的にビルドとデプロイが実行されます。

### 手動デプロイ
```bash
gcloud builds triggers run cohere-rag-dev-deploy --branch=search_with_meta
```

## 8. 動作確認

### 8.1 サービスの確認
```bash
gcloud run services describe cohere-rag-dev --region=asia-northeast1
```

### 8.2 ジョブの確認
```bash
gcloud run jobs describe cohere-rag-vectorize-job-dev --region=asia-northeast1
gcloud run jobs describe scheduler-job-dev --region=asia-northeast1
```

### 8.3 ログの確認
```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=cohere-rag-vectorize-job-dev" --limit=50
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=scheduler-job-dev" --limit=50
```

## 注意事項

1. **バケット名の一意性**: `embedding_storage_dev` バケット名がグローバルで一意である必要があります。既に使用されている場合は別の名前に変更してください。

2. **APIキーの管理**: COHERE_API_KEY は Secret Manager を使用して管理することを推奨します。

3. **初回デプロイ**: ジョブの作成時に使用するイメージがまだ存在しない場合は、先にCloud Buildトリガーを実行してイメージをビルドする必要があります。

4. **環境の分離**: 開発環境と本番環境で異なるGCSバケット、ジョブ名、サービス名を使用することで完全に分離されています。
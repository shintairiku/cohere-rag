# デバッグ用コマンド集

## 1. メモリ不足エラーのシミュレーション

10枚目の画像でメモリ不足エラーを発生させる：

```bash
export DEBUG_MODE=true
export SIMULATE_MEMORY_ERROR_AT=10
export UUID=test-uuid
export DRIVE_URL=your-drive-url
export GCS_BUCKET_NAME=your-bucket

python img_meta_processor_gdrive.py
```

## 2. 処理エラーのシミュレーション

15枚目の画像で処理エラーを発生させる：

```bash
export DEBUG_MODE=true
export SIMULATE_PROCESSING_ERROR_AT=15
export UUID=test-uuid
export DRIVE_URL=your-drive-url
export GCS_BUCKET_NAME=your-bucket

python img_meta_processor_gdrive.py
```

## 3. APIコスト削減テスト

Cohere APIを使わずダミーベクトルでテスト：

```bash
export DEBUG_MODE=true
export UUID=test-uuid
export DRIVE_URL=your-drive-url
export GCS_BUCKET_NAME=your-bucket

python img_meta_processor_gdrive.py
```

## 4. チェックポイント間隔のテスト

3枚ごとにチェックポイントを保存：

```bash
export DEBUG_MODE=true
export CHECKPOINT_INTERVAL=3
export UUID=test-uuid
export DRIVE_URL=your-drive-url
export GCS_BUCKET_NAME=your-bucket

python img_meta_processor_gdrive.py
```

## 5. 再開機能のテスト

1. 最初に途中で止める：
```bash
export DEBUG_MODE=true
export SIMULATE_MEMORY_ERROR_AT=7
export UUID=test-uuid
export DRIVE_URL=your-drive-url
export GCS_BUCKET_NAME=your-bucket

python img_meta_processor_gdrive.py
```

2. 再実行して再開確認：
```bash
export DEBUG_MODE=true
# エラーシミュレーションを無効化
export SIMULATE_MEMORY_ERROR_AT=0
export UUID=test-uuid
export DRIVE_URL=your-drive-url
export GCS_BUCKET_NAME=your-bucket

python img_meta_processor_gdrive.py
```

## 6. Cloud Run jobでのテスト

```bash
gcloud run jobs update cohere-rag-vectorize-job \
  --set-env-vars="DEBUG_MODE=true,SIMULATE_MEMORY_ERROR_AT=25" \
  --region=asia-northeast1
```

## デバッグ機能の利点

- **APIコスト削減**: Cohere APIを使わずダミーベクトルで動作確認
- **意図的なエラー**: 指定した位置でエラーを発生させてエラーハンドリングをテスト
- **再現可能**: 常に同じ位置でエラーを発生させることができる
- **段階的テスト**: チェックポイント保存→エラー発生→再開の流れを確認
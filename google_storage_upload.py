import os
from google.cloud import storage

def upload_json_files(bucket_name: str, source_directory: str):
    """
    指定されたディレクトリ内の .json ファイルのみを Google Cloud Storage にアップロードします。

    Args:
        bucket_name (str): アップロード先の GCS バケット名。
        source_directory (str): アップロードするファイルが含まれるローカルディレクトリのパス。
    """
    # Google Cloud Storage クライアントを初期化
    try:
        storage_client = storage.Client()
    except Exception as e:
        print(f"エラー: Google Cloud クライアントの初期化に失敗しました。認証情報を確認してください。")
        print(f"詳細: {e}")
        return

    print(f"ディレクトリ '{source_directory}' 内の .json ファイルを検索中...")

    # 指定されたディレクトリのファイルを走査
    for root, _, files in os.walk(source_directory):
        for file_name in files:
            # ファイル名が .json で終わるかチェック
            if file_name.endswith('.json') and 'embedding' in file_name:
                local_file_path = os.path.join(root, file_name)
                # アップロード先のパスを設定 (ディレクトリ構造を維持)
                destination_blob_name = os.path.relpath(local_file_path, start=source_directory).replace(os.sep, '/')
                
                print(f"'{local_file_path}' をアップロード中...")
                
                try:
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(destination_blob_name)
                    
                    # ファイルをアップロード
                    blob.upload_from_filename(local_file_path)
                    
                    print(f"✅ '{local_file_path}' を '{bucket_name}/{destination_blob_name}' に正常にアップロードしました。")
                except Exception as e:
                    print(f"❌ '{local_file_path}' のアップロードに失敗しました。")
                    print(f"詳細: {e}")

if __name__ == "__main__":
    # 使用例：
    # ここにアップロードしたいバケット名とローカルのディレクトリパスを設定してください。
    
    # 例： 'my-json-bucket' というバケットに './data' ディレクトリのファイルをアップロード
    bucket = "embedding_storage"
    directory_to_upload = "./"

    upload_json_files(bucket, directory_to_upload)
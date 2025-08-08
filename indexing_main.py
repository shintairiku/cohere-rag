# =================================================================
# indexing_main.py
#
# Cloud Runで実行するデータ投入（Indexing）サービス。
# Google Driveから画像を読み込み、GCSに保存後、
# ベクトル化してAlloyDBに登録する。
# =================================================================
import os
import io
import hashlib
from typing import List, Dict

import cohere
import pg8000
from PIL import Image
from google.api_core.client_options import ClientOptions
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage
from google.cloud.alloydb.connector import Connector

# --- 定数定義 ---
# Cohere APIのモデルと次元数
COHERE_MODEL = "embed-v4.0"
VECTOR_DIMENSIONS = 4096

# --- 環境変数から設定を読み込み ---
# GCPプロジェクト関連
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")

# AlloyDB関連
ALLOYDB_REGION = os.environ.get("ALLOYDB_REGION")
ALLOYDB_CLUSTER = os.environ.get("ALLOYDB_CLUSTER")
ALLOYDB_INSTANCE = os.environ.get("ALLOYDB_INSTANCE")
ALLOYDB_DATABASE = os.environ.get("ALLOYDB_DATABASE")
ALLOYDB_USER = os.environ.get("ALLOYDB_USER") # DBユーザー名

# Google Drive/Sheets API関連
# サービスアカウントキーのJSON文字列を環境変数から取得
SERVICE_ACCOUNT_JSON_STRING = os.environ.get("SERVICE_ACCOUNT_JSON")
# 会社一覧が記載されたスプレッドシートのID
COMPANY_SHEET_ID = os.environ.get("COMPANY_SHEET_ID")
COMPANY_SHEET_RANGE = "A2:C" # A列:会社名, B列:Drive URL, C列:スキーマID

# APIキー（Secret Managerから取得することを推奨）
COHERE_API_KEY = os.environ.get("COHERE_API_KEY")


# --- クライアントの初期化 ---
co_client = cohere.Client(COHERE_API_KEY)
storage_client = storage.Client()
connector = Connector()

# --- Google API関連のヘルパー関数 ---

def get_google_api_services():
    """Google DriveとSheetsのAPIサービスクライアントを生成する"""
    creds = Credentials.from_service_account_info(
        info=eval(SERVICE_ACCOUNT_JSON_STRING), # JSON文字列を辞書に変換
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets.readonly"
        ]
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service

def get_drive_folder_id_from_url(url: str) -> str:
    """Google DriveのURLからフォルダIDを抽出する"""
    # 'https://drive.google.com/drive/folders/...' or 'open?id=...'
    parts = url.split('/')
    if 'folders' in parts:
        return parts[parts.index('folders') + 1].split('?')[0]
    elif 'id=' in url:
        return url.split('id=')[1].split('&')[0]
    raise ValueError(f"Invalid Google Drive URL format: {url}")

def list_files_in_drive(drive_service, folder_id: str) -> List[Dict]:
    """指定されたGoogle Driveフォルダ内のファイルを再帰的にリストアップする"""
    files = []
    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
    page_token = None
    while True:
        response = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, parents, size, sha256Checksum)',
            pageToken=page_token
        ).execute()
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

    # サブフォルダも再帰的に探索
    subfolders_query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    subfolders_response = drive_service.files().list(q=subfolders_query, fields='files(id)').execute()
    for subfolder in subfolders_response.get('files', []):
        files.extend(list_files_in_drive(drive_service, subfolder.get('id')))

    return files


# --- AlloyDB関連のヘルパー関数 ---

def get_db_connection() -> pg8000.dbapi.Connection:
    """AlloyDBへのコネクションを確立する"""
    instance_connection_name = f"projects/{GCP_PROJECT_ID}/locations/{ALLOYDB_REGION}/clusters/{ALLOYDB_CLUSTER}/instances/{ALLOYDB_INSTANCE}"
    conn = connector.connect(
        instance_connection_name,
        "pg8000",
        user=ALLOYDB_USER,
        db=ALLOYDB_DATABASE,
        enable_iam_auth=True, # IAM認証を利用
    )
    return conn

def setup_schema_and_table(conn: pg8000.dbapi.Connection, schema_name: str):
    """DBスキーマとテーブルが存在しない場合に作成する"""
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.images (
                id SERIAL PRIMARY KEY,
                gcs_uri VARCHAR(1024) UNIQUE NOT NULL,
                source_drive_id VARCHAR(255),
                file_hash VARCHAR(64) UNIQUE,
                file_size_bytes BIGINT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                embedding VECTOR({VECTOR_DIMENSIONS})
            );
        """)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS hnsw_idx ON {schema_name}.images USING HNSW (embedding vector_cosine_ops);")
    conn.commit()
    print(f"Schema '{schema_name}' and table 'images' are ready.")

def get_existing_hashes(conn: pg8000.dbapi.Connection, schema_name: str) -> set:
    """指定スキーマ内の既存のファイルハッシュをすべて取得する"""
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT file_hash FROM {schema_name}.images WHERE file_hash IS NOT NULL;")
        return {row[0] for row in cursor.fetchall()}


# --- 画像処理・ベクトル化のロジック (img_meta_processor.pyから移植・修正) ---

def resize_image_if_needed(image_bytes: bytes, max_size_mb: int = 15) -> bytes:
    """画像がサイズ上限を超える場合にリサイズする"""
    max_size_bytes = max_size_mb * 1024 * 1024
    if len(image_bytes) <= max_size_bytes:
        return image_bytes

    print(f"  Resizing image ({len(image_bytes) / (1024*1024):.1f}MB)...")
    try:
        img = Image.open(io.BytesIO(image_bytes))
        # ... (リサイズロジックは元のスクリプトと同様。ここでは簡略化)
        output = io.BytesIO()
        img.thumbnail((2048, 2048)) # 2048pxに縮小
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        img.save(output, format='JPEG', quality=85)
        resized_bytes = output.getvalue()
        print(f"  Resized to {len(resized_bytes) / (1024*1024):.1f}MB")
        return resized_bytes
    except Exception as e:
        print(f"  Resize failed: {e}. Using original image.")
        return image_bytes

def get_embedding(image_bytes: bytes) -> List[float]:
    """画像のバイトデータからベクトル表現を生成する"""
    base64_string = base64.b64encode(image_bytes).decode('utf-8')
    data_url = f"data:image/jpeg;base64,{base64_string}"
    
    response = co_client.embed(
        model=COHERE_MODEL,
        inputs=[{"content": [{"type": "image_url", "image_url": {"url": data_url}}]}],
        input_type="search_document",
        embedding_types=["float"]
    )
    return response.embeddings.float_[0]


# --- メイン処理 ---

def main():
    """メインの実行関数"""
    print("Starting indexing process...")
    drive_service, sheets_service = get_google_api_services()
    db_conn = get_db_connection()

    # 1. 会社一覧シートから処理対象リストを取得
    sheet = sheets_service.spreadsheets()
    result = sheet.values().get(spreadsheetId=COMPANY_SHEET_ID, range=COMPANY_SHEET_RANGE).execute()
    companies = result.get('values', [])

    if not companies:
        print("No companies found in the sheet.")
        return

    # 2. 各会社ごとに処理を実行
    for company_info in companies:
        company_name, drive_url, schema_id = company_info[0], company_info[1], company_info[2]
        print(f"\n--- Processing company: {company_name} (Schema: {schema_id}) ---")

        try:
            # 3. DBスキーマとテーブルを準備
            setup_schema_and_table(db_conn, schema_id)
            existing_hashes = get_existing_hashes(db_conn, schema_id)
            print(f"Found {len(existing_hashes)} existing images in DB.")

            # 4. Google Driveからファイルリストを取得
            folder_id = get_drive_folder_id_from_url(drive_url)
            drive_files = list_files_in_drive(drive_service, folder_id)
            print(f"Found {len(drive_files)} files in Google Drive.")

            # 5. 新規ファイルのみを処理
            new_files_count = 0
            for file_info in drive_files:
                file_hash = file_info.get('sha256Checksum')
                if not file_hash or file_hash in existing_hashes:
                    continue
                
                new_files_count += 1
                print(f"  Processing new file: {file_info['name']} (ID: {file_info['id']})")

                try:
                    # 6. Driveから画像をダウンロード
                    request = drive_service.files().get_media(fileId=file_info['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                    image_bytes = fh.getvalue()

                    # 7. 画像をリサイズ（必要に応じて）
                    resized_bytes = resize_image_if_needed(image_bytes)

                    # 8. GCSにアップロード
                    gcs_path = f"{schema_id}/{file_info['name']}"
                    bucket = storage_client.bucket(GCS_BUCKET_NAME)
                    blob = bucket.blob(gcs_path)
                    blob.upload_from_string(resized_bytes, content_type=file_info['mimeType'])
                    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
                    print(f"    Uploaded to {gcs_uri}")

                    # 9. ベクトル化
                    embedding = get_embedding(resized_bytes)
                    print("    Vectorized successfully.")

                    # 10. AlloyDBに保存
                    with db_conn.cursor() as cursor:
                        sql = f"""
                            INSERT INTO {schema_id}.images 
                            (gcs_uri, source_drive_id, file_hash, file_size_bytes, embedding)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        cursor.execute(sql, (
                            gcs_uri,
                            file_info['id'],
                            file_hash,
                            int(file_info.get('size', 0)),
                            embedding
                        ))
                    db_conn.commit()
                    print("    Saved to AlloyDB.")

                except Exception as e:
                    print(f"    !! FAILED to process file {file_info['name']}: {e}")
                    db_conn.rollback()

            print(f"Finished processing for {company_name}. Added {new_files_count} new images.")

        except Exception as e:
            print(f"!! An error occurred while processing {company_name}: {e}")
            continue

    connector.close()
    print("\nIndexing process finished.")


if __name__ == "__main__":
    main()


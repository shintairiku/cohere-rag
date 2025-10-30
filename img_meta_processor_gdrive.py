import os
import io
import json
import traceback
import signal
import sys
from datetime import datetime

import numpy as np
from dotenv import load_dotenv
from google.cloud import storage
from PIL import Image as PILImage

from embedding_providers import get_embedding_provider

# Decompression bomb対策: 最大画像ピクセル数を設定（約500MP）
PILImage.MAX_IMAGE_PIXELS = 500_000_000

import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from drive_scanner import list_files_in_drive_folder

load_dotenv()

# --- 1. 環境変数の読み込みと検証 ---
BATCH_MODE = os.getenv("BATCH_MODE", "false").lower() == "true"

if BATCH_MODE:
    BATCH_TASKS_JSON = os.getenv("BATCH_TASKS", "[]")
    try:
        BATCH_TASKS = json.loads(BATCH_TASKS_JSON)
    except json.JSONDecodeError:
        raise RuntimeError("FATAL: Invalid BATCH_TASKS JSON format")
else:
    UUID = os.getenv("UUID")
    DRIVE_URL = os.getenv("DRIVE_URL")
    USE_EMBED_V4 = os.getenv("USE_EMBED_V4", "false").lower() == "true"

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION", "asia-northeast1")
VERTEX_MULTIMODAL_MODEL = os.getenv("VERTEX_MULTIMODAL_MODEL", "multimodalembedding@001")
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "vertex_ai").lower()
MAX_IMAGE_SIZE_MB = 5
CHECKPOINT_INTERVAL = 100

# デバッグ用設定
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
SIMULATE_MEMORY_ERROR_AT = int(os.getenv("SIMULATE_MEMORY_ERROR_AT", "0"))
SIMULATE_PROCESSING_ERROR_AT = int(os.getenv("SIMULATE_PROCESSING_ERROR_AT", "0"))

if BATCH_MODE:
    required_vars = ['GCS_BUCKET_NAME', 'GCP_PROJECT_ID']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(f"FATAL: Required environment variables are missing: {', '.join(missing)}")
    if not BATCH_TASKS:
        raise RuntimeError("FATAL: No tasks provided in batch mode")
else:
    required_vars = ['GCS_BUCKET_NAME', 'GCP_PROJECT_ID', 'UUID', 'DRIVE_URL']
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise RuntimeError(f"FATAL: Required environment variables are missing: {', '.join(missing)}")

if EMBEDDING_PROVIDER == "cohere" and not COHERE_API_KEY:
    raise RuntimeError("FATAL: COHERE_API_KEY must be set when EMBEDDING_PROVIDER=cohere")

if not DEBUG_MODE:
    storage_client = storage.Client()
else:
    storage_client = None
    print("🧪 [DEBUG] Skipping Google Cloud Storage client initialization")

MAX_FILE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024

def resize_image_if_needed(image_content: bytes, filename: str) -> bytes:
    """
    画像の解像度が埋め込みAPIの制限を超える場合、ピクセル数ベースでリサイズする。
    """
    try:
        try:
            img = PILImage.open(io.BytesIO(image_content))
            img.verify()
            img = PILImage.open(io.BytesIO(image_content))
        except PILImage.DecompressionBombError as e:
            print(f"    ⚠️  Decompression bomb warning for '{filename}': {e}")
            print(f"       File might be too large or corrupted. Skipping...")
            return None
        except OSError as e:
            print(f"    ⚠️  Cannot identify image file '{filename}': {e}")
            print(f"       File might not be a valid image or is corrupted. Skipping...")
            return None
        except Exception as e:
            print(f"    ⚠️  Unexpected error opening image '{filename}': {e}")
            return None
            
        original_width, original_height = img.size
        original_pixels = original_width * original_height
        original_size_mb = len(image_content) / (1024 * 1024)
        
        if original_pixels > 100_000_000:
            print(f"    ⚠️  Extremely large image: {original_width}x{original_height} ({original_pixels:,} pixels)")
            print(f"       This image is too large to process safely. Skipping...")
            return None
        
        MAX_PIXELS = 2_300_000
        
        if original_pixels <= MAX_PIXELS:
            return image_content
        
        print(f"    📏 High resolution image detected: {original_width}x{original_height} ({original_pixels:,} pixels > {MAX_PIXELS:,} limit)")
        print(f"       File size: {original_size_mb:.1f}MB")
        
        if img.mode in ('RGBA', 'LA', 'P'):
            background = PILImage.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
            img = background

        scale_factor = (MAX_PIXELS / original_pixels) ** 0.5
        scale_factor = max(0.3, scale_factor)
        
        new_width = int(original_width * scale_factor)
        new_height = int(original_height * scale_factor)
        new_pixels = new_width * new_height
        
        print(f"    🔢 Calculated scale factor: {scale_factor:.3f}")
        print(f"       New resolution: {new_width}x{new_height} ({new_pixels:,} pixels)")
        
        resized_img = img.resize((new_width, new_height), PILImage.Resampling.LANCZOS)
        
        output = io.BytesIO()
        resized_img.save(output, format='JPEG', quality=90, optimize=True)
        resized_data = output.getvalue()
        resized_size_mb = len(resized_data) / (1024 * 1024)
        
        quality = 90
        while len(resized_data) > MAX_FILE_SIZE_BYTES and quality >= 60:
            quality -= 10
            output = io.BytesIO()
            resized_img.save(output, format='JPEG', quality=quality, optimize=True)
            resized_data = output.getvalue()
            resized_size_mb = len(resized_data) / (1024 * 1024)
        
        print(f"    ✅ Successfully resized: {original_size_mb:.1f}MB -> {resized_size_mb:.1f}MB")
        print(f"       Resolution: {original_width}x{original_height} -> {new_width}x{new_height}")
        print(f"       Quality: {quality}")
        
        return resized_data
        
    except Exception as e:
        print(f"    ❌ Resize Error: {e}")
        traceback.print_exc()
        return None

def get_multimodal_embedding(image_bytes: bytes, filename: str, file_index: int = 0, use_embed_v4: bool = False) -> np.ndarray:
    """画像データとファイル名から重み付けされたベクトルを生成する"""
    try:
        if DEBUG_MODE and SIMULATE_MEMORY_ERROR_AT > 0 and file_index == SIMULATE_MEMORY_ERROR_AT:
            print(f"🧪 [DEBUG] Simulating memory error at file #{file_index}")
            raise MemoryError("Simulated out-of-memory event for debugging")
        
        if DEBUG_MODE and SIMULATE_PROCESSING_ERROR_AT > 0 and file_index == SIMULATE_PROCESSING_ERROR_AT:
            print(f"🧪 [DEBUG] Simulating processing error at file #{file_index}")
            raise Exception("Simulated processing error for debugging")
        
        provider = get_embedding_provider()
        embedding = provider.embed_multimodal(
            text=filename,
            image_bytes=image_bytes,
            use_embed_v4=use_embed_v4,
        )
        return embedding
    
    except Exception as e:
        print(f"    ⚠️  Warning: Could not generate multimodal embedding for '{filename}'. Skipping. Reason: {e}")
        traceback.print_exc()
        return None

def load_existing_embeddings(bucket_name: str, uuid: str) -> tuple:
    """既存のembeddingsと処理済みファイルリストを読み込む"""
    if DEBUG_MODE:
        print("🧪 [DEBUG] Skipping existing embeddings check")
        return [], set()
        
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{uuid}.json")
        
        if blob.exists():
            existing_data = json.loads(blob.download_as_text())
            processed_files = {item['filename'] for item in existing_data}
            print(f"📂 Found existing data with {len(existing_data)} embeddings")
            return existing_data, processed_files
        else:
            print("📂 No existing data found, starting fresh")
            return [], set()
    except Exception as e:
        print(f"⚠️  Could not load existing data: {e}")
        return [], set()

def save_checkpoint(bucket_name: str, uuid: str, embeddings: list, is_final: bool = False):
    """チェックポイントとしてembeddingsを{uuid}.jsonに保存"""
    if DEBUG_MODE:
        print(f"🧪 [DEBUG] Skipping save checkpoint ({len(embeddings)} embeddings)")
        return
        
    try:
        from datetime import datetime
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{uuid}.json")
        blob.upload_from_string(
            json.dumps(embeddings, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        
        if is_final:
            print(f"✅ [{current_time}] Final save completed: {len(embeddings)} embeddings saved to gs://{bucket_name}/{uuid}.json")
        else:
            print(f"💾 [{current_time}] Checkpoint saved: {len(embeddings)} embeddings saved to gs://{bucket_name}/{uuid}.json")
            
    except Exception as e:
        print(f"❌ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Failed to save checkpoint to gs://{bucket_name}/{uuid}.json: {e}")
        traceback.print_exc()

def calculate_diff(drive_files: list, existing_embeddings: list) -> tuple:
    """
    Google DriveとベクトルファイルのDriveとベクトルファイルの差分を計算する
    
    Args:
        drive_files: Google Driveから取得したファイルリスト
        existing_embeddings: 既存のベクトルデータ
        
    Returns:
        (files_to_add, files_to_delete): 追加対象と削除対象のタプル
    """
    # Google Driveの現在のファイルセット（フルパスで管理）
    drive_file_keys = {f"{f.get('folder_path', '')}/{f['name']}" for f in drive_files}
    
    # ベクトルファイルの既存ファイルセット（フルパスで管理）
    vector_file_keys = {f"{item.get('folder_path', '')}/{item.get('filename', '')}" for item in existing_embeddings}
    
    # 追加対象: Driveにあるがベクトルにない
    keys_to_add = drive_file_keys - vector_file_keys
    
    # 削除対象: ベクトルにあるがDriveにない
    keys_to_delete = vector_file_keys - drive_file_keys
    
    # 追加対象のファイル情報を抽出
    files_to_add = [f for f in drive_files if f"{f.get('folder_path', '')}/{f['name']}" in keys_to_add]
    
    print(f"\n📊 Diff Analysis Results:")
    print(f"   Drive files: {len(drive_file_keys)}")
    print(f"   Vector files: {len(vector_file_keys)}")
    print(f"   Files to ADD: {len(files_to_add)}")
    print(f"   Files to DELETE: {len(keys_to_delete)}")
    
    if keys_to_delete:
        print(f"\n🗑️  Files to be deleted from vector storage:")
        for key in list(keys_to_delete)[:10]:  # 最初の10件のみ表示
            print(f"     - {key}")
        if len(keys_to_delete) > 10:
            print(f"     ... and {len(keys_to_delete) - 10} more")
    
    return files_to_add, keys_to_delete

def remove_deleted_files(existing_embeddings: list, keys_to_delete: set) -> list:
    """
    ベクトルファイルから削除対象のファイルを除外する
    
    Args:
        existing_embeddings: 既存のベクトルデータ
        keys_to_delete: 削除対象のファイルキーセット
        
    Returns:
        削除処理後のベクトルデータ
    """
    if not keys_to_delete:
        return existing_embeddings
    
    original_count = len(existing_embeddings)
    
    # 削除対象以外を残す
    filtered_embeddings = [
        item for item in existing_embeddings
        if f"{item.get('folder_path', '')}/{item.get('filename', '')}" not in keys_to_delete
    ]
    
    deleted_count = original_count - len(filtered_embeddings)
    
    print(f"\n🗑️  Deletion completed:")
    print(f"   Original: {original_count} embeddings")
    print(f"   Deleted: {deleted_count} embeddings")
    print(f"   Remaining: {len(filtered_embeddings)} embeddings")
    
    return filtered_embeddings

def process_single_uuid(uuid: str, drive_url: str, use_embed_v4: bool = False, all_embeddings: list = None) -> list:
    """単一UUIDの処理（差分検出・削除機能付き）"""
    if all_embeddings is None:
        all_embeddings = []
    
    print(f"📋 Processing UUID: {uuid}")
    print(f"   Drive URL: {drive_url}")
    print(f"   Using Embed Model: {'embed-v4.0' if use_embed_v4 else 'embed-multilingual-v3.0'}")
    print(f"🔍 Debug - Looking for file: gs://{GCS_BUCKET_NAME}/{uuid}.json")
    
    try:
        # 既存のembeddingsを読み込む
        existing_embeddings, _ = load_existing_embeddings(GCS_BUCKET_NAME, uuid)
        
        if DEBUG_MODE:
            # デバッグモードではダミーのファイルリストを使用
            drive_files = [
                {'name': f'debug_image_{uuid}_1.jpg', 'id': 'debug_id_1', 'webViewLink': f'https://debug.example.com/{uuid}_1', 'folder_path': '/debug'},
                {'name': f'debug_image_{uuid}_2.png', 'id': 'debug_id_2', 'webViewLink': f'https://debug.example.com/{uuid}_2', 'folder_path': '/debug'}
            ]
            print(f"🧪 [DEBUG] Using {len(drive_files)} dummy files for UUID {uuid}")
        else:
            # Google Driveから現在のファイルリストを取得
            drive_files = list_files_in_drive_folder(drive_url)
            if not drive_files:
                print(f"⚠️  No files found in Google Drive for UUID {uuid}")
                # Driveにファイルがない場合、既存のベクトルファイルを全削除
                if existing_embeddings:
                    print(f"🗑️  Removing all {len(existing_embeddings)} embeddings (Drive is empty)")
                    save_checkpoint(GCS_BUCKET_NAME, uuid, [], is_final=True)
                return []
        
        # 差分を計算
        files_to_add, keys_to_delete = calculate_diff(drive_files, existing_embeddings)
        
        # 削除処理を実行
        task_embeddings = remove_deleted_files(existing_embeddings, keys_to_delete)
        
        # 削除が発生した場合は即座に保存
        if keys_to_delete:
            save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=False)
            print(f"💾 Saved after deletion: {len(task_embeddings)} embeddings")
        
        # 追加対象がない場合は終了
        if not files_to_add:
            print(f"✅ No new files to process for UUID {uuid}")
            if keys_to_delete:
                # 削除のみ発生した場合は最終保存
                save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=True)
            return task_embeddings
        
        print(f"\n📝 Processing {len(files_to_add)} new files...")
        
        if not DEBUG_MODE:
            print("Initializing Google Drive service...")
            drive_creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
            drive_service = build('drive', 'v3', credentials=drive_creds)
        else:
            drive_service = None
            print("🧪 [DEBUG] Skipping Google Drive service initialization")
        
        start_time = datetime.now()
        
        for i, file_info in enumerate(files_to_add, 1):
            print(f"    ({i}/{len(files_to_add)}) Processing: {file_info['name'][:50]}...")
            
            try:
                if DEBUG_MODE:
                    print("      🧪 [DEBUG] Using dummy image data (skipping actual download)")
                    dummy_img = PILImage.new('RGB', (100, 100), color='red')
                    output = io.BytesIO()
                    dummy_img.save(output, format='JPEG')
                    image_content = output.getvalue()
                else:
                    request = drive_service.files().get_media(fileId=file_info['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                    image_content = fh.getvalue()
                
                resized_content = resize_image_if_needed(image_content, file_info['name'])
                if resized_content is None:
                    print(f"      ⭕️  Skipping due to resize failure")
                    continue

                embedding = get_multimodal_embedding(resized_content, file_info['name'], i, use_embed_v4)
                if embedding is not None:
                    result_data = {
                        "filename": file_info['name'],
                        "filepath": file_info['webViewLink'],
                        "folder_path": file_info['folder_path'],
                        "embedding": embedding.tolist()
                    }
                    task_embeddings.append(result_data)
                    
                    if i % CHECKPOINT_INTERVAL == 0:
                        print(f"📌 Checkpoint reached: processed {i}/{len(files_to_add)} files")
                        save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=False)
                        print(f"💾 Checkpoint saved: {len(task_embeddings)} embeddings")

            except Exception as e:
                print(f"      ❌ Error processing {file_info['name']}: {e}")
                continue
        
        # タスク完了後にファイルを保存
        if task_embeddings != existing_embeddings or keys_to_delete:
            elapsed_total = (datetime.now() - start_time).total_seconds()
            print(f"   ⏱️  Processing time for UUID {uuid}: {elapsed_total:.1f} seconds")
            save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=True)
            print(f"   ✅ Saved {len(task_embeddings)} embeddings for UUID {uuid}")
            print(f"   📊 Changes: +{len(files_to_add)} files, -{len(keys_to_delete)} files")
        
        return task_embeddings
        
    except Exception as e:
        print(f"   ❌ Error processing UUID {uuid}: {e}")
        traceback.print_exc()
        if task_embeddings:
            try:
                save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=False)
                print(f"   💾 Emergency save for UUID {uuid}: {len(task_embeddings)} embeddings")
            except Exception as save_error:
                print(f"   ❌ Emergency save failed for UUID {uuid}: {save_error}")
        raise e


def main():
    """Cloud Runジョブとして実行されるメイン関数"""
    
    print("🔧 Environment Variables:")
    env_vars = [
        "GCS_BUCKET_NAME", "GCP_PROJECT_ID", "GCP_REGION", "VERTEX_MULTIMODAL_MODEL",
        "EMBEDDING_PROVIDER", "COHERE_API_KEY",
        "UUID", "DRIVE_URL", "USE_EMBED_V4", "BATCH_MODE", "BATCH_TASKS", "DEBUG_MODE"
    ]
    for var in env_vars:
        value = os.getenv(var, "NOT_SET")
        if var == "COHERE_API_KEY" and value != "NOT_SET":
            value = f"{value[:10]}..." if len(value) > 10 else value
        elif var == "BATCH_TASKS" and value != "NOT_SET":
            value = f"[{len(value)} characters]" if value else "EMPTY"
        print(f"  {var}: {value}")
    print()
    
    if BATCH_MODE:
        print("===================================================")
        print(f"  Starting BATCH Vectorization Job (with Diff Detection)")
        print(f"  Number of tasks: {len(BATCH_TASKS)}")
        print(f"  Features: Auto-add new files + Auto-delete removed files")
        print("===================================================")
        
        total_processed = 0
        total_errors = 0
        
        for i, task in enumerate(BATCH_TASKS, 1):
            uuid = task.get('uuid')
            drive_url = task.get('drive_url')
            company_name = task.get('company_name', '')
            use_embed_v4 = task.get('use_embed_v4', False)
            
            print(f"\n📋 Task {i}/{len(BATCH_TASKS)}: {company_name} (UUID: {uuid})")
            
            try:
                process_single_uuid(uuid, drive_url, use_embed_v4)
                total_processed += 1
                print(f"✅ Task {i} completed successfully")
                    
            except Exception as e:
                print(f"❌ Task {i} failed: {e}")
                total_errors += 1
                continue
        
        print(f"\n🎉 Batch job completed: {total_processed} successful, {total_errors} failed")
    else:
        print("===================================================")
        print(f"  Starting SINGLE Vectorization Job (with Diff Detection)")
        print(f"  UUID: {UUID}")
        print(f"  Drive URL: {DRIVE_URL}")
        print(f"  Use Embed V4: {USE_EMBED_V4}")
        print(f"  Features: Auto-add new files + Auto-delete removed files")
        print("===================================================")
        
        all_embeddings = []
        
        def signal_handler(signum, frame):
            print(f"\n⚠️  Signal {signum} received. Attempting to save current progress...")
            if all_embeddings:
                try:
                    save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=False)
                    print(f"✅ Emergency save successful: {len(all_embeddings)} embeddings saved")
                except Exception as e:
                    print(f"❌ Emergency save failed: {e}")
            sys.exit(1)
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        all_embeddings = process_single_uuid(UUID, DRIVE_URL, USE_EMBED_V4)
        print("🎉 Single job finished successfully.")

if __name__ == "__main__":
    main()

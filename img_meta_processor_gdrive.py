import os
import io
import json
import traceback
import base64
import hashlib
import gc  # ガベージコレクション用
import signal  # シグナルハンドリング用
import sys
from datetime import datetime

import cohere
import numpy as np
from dotenv import load_dotenv
from google.cloud import storage
from PIL import Image

# Decompression bomb対策: 最大画像ピクセル数を設定（約500MP）
Image.MAX_IMAGE_PIXELS = 500_000_000

import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from drive_scanner import list_files_in_drive_folder # drive_scanner.pyを再利用

load_dotenv()

# --- 1. 環境変数の読み込みと検証 ---
UUID = os.getenv("UUID")
DRIVE_URL = os.getenv("DRIVE_URL")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
 # Cloud RunジョブでGoogleスプレッドシートから受け取ったembed-v4.0使用フラグ
USE_EMBED_V4 = os.getenv("USE_EMBED_V4", "false").lower() == "true"
MAX_IMAGE_SIZE_MB = 5  # Cohere API制限: 最大5MB
# CHECKPOINT_INTERVAL は削除（エラー時のみ保存するため不要）

# デバッグ用設定
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
SIMULATE_MEMORY_ERROR_AT = int(os.getenv("SIMULATE_MEMORY_ERROR_AT", "0"))  # 指定した画像番号でメモリエラーをシミュレート
SIMULATE_PROCESSING_ERROR_AT = int(os.getenv("SIMULATE_PROCESSING_ERROR_AT", "0"))  # 指定した画像番号で処理エラーをシミュレート

if not all([GCS_BUCKET_NAME, COHERE_API_KEY, UUID, DRIVE_URL]):
    missing = [
        var for var in ['GCS_BUCKET_NAME', 'COHERE_API_KEY', 'UUID', 'DRIVE_URL']
        if not os.getenv(var)
    ]
    raise RuntimeError(f"FATAL: Required environment variables are missing: {', '.join(missing)}")

# --- 2. グローバルクライアントの初期化 ---
co_client = cohere.Client(COHERE_API_KEY)

# デバッグモードでは Google Cloud Storage クライアントを初期化しない
if not DEBUG_MODE:
    storage_client = storage.Client()
else:
    storage_client = None
    print("🧪 [DEBUG] Skipping Google Cloud Storage client initialization")

MAX_FILE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024

def resize_image_if_needed(image_content: bytes, filename: str) -> bytes:
    """
    画像の解像度がCohere API制限を超える場合、ピクセル数ベースでリサイズする。
    Cohere APIは解像度ベースで制限を行うため、ファイルサイズではなくピクセル数で判定。
    """
    try:
        # まず画像として読み込めるか検証
        try:
            img = Image.open(io.BytesIO(image_content))
            # 画像を読み込んで基本情報を確認（実際にピクセルデータを読み込む）
            img.verify()
            # verifyは画像を閉じるので、再度開く
            img = Image.open(io.BytesIO(image_content))
        except Image.DecompressionBombError as e:
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
        
        # 極端に大きい画像の場合は警告を出してスキップ
        if original_pixels > 100_000_000:  # 100MP以上
            print(f"    ⚠️  Extremely large image: {original_width}x{original_height} ({original_pixels:,} pixels)")
            print(f"       This image is too large to process safely. Skipping...")
            return None
        
        # Cohere API embed-v4.0の解像度制限: 約240万ピクセル
        # 安全マージンを考慮して2.3MP (2,300,000ピクセル) を上限とする
        MAX_PIXELS = 2_300_000
        
        # 解像度チェック
        if original_pixels <= MAX_PIXELS:
            return image_content
        
        print(f"    📏 High resolution image detected: {original_width}x{original_height} ({original_pixels:,} pixels > {MAX_PIXELS:,} limit)")
        print(f"       File size: {original_size_mb:.1f}MB")
        
        # RGBAやPモードの画像をRGBに変換
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
            img = background

        # 必要なスケールファクターを計算（ピクセル数ベース）
        scale_factor = (MAX_PIXELS / original_pixels) ** 0.5  # 面積比の平方根
        
        # 最小でも0.3倍までしかスケールダウンしない（品質保持のため）
        scale_factor = max(0.3, scale_factor)
        
        new_width = int(original_width * scale_factor)
        new_height = int(original_height * scale_factor)
        new_pixels = new_width * new_height
        
        print(f"    🔢 Calculated scale factor: {scale_factor:.3f}")
        print(f"       New resolution: {new_width}x{new_height} ({new_pixels:,} pixels)")
        
        # リサイズ実行
        resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # 品質90で保存
        output = io.BytesIO()
        resized_img.save(output, format='JPEG', quality=90, optimize=True)
        resized_data = output.getvalue()
        resized_size_mb = len(resized_data) / (1024 * 1024)
        
        # ファイルサイズも5MBを超えた場合は品質を下げる
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

def get_multimodal_embedding(image_bytes: bytes, filename: str, file_index: int = 0) -> np.ndarray:
    """画像データとファイル名から重み付けされたベクトルを生成する"""
    try:
        # 使用するモデルを決定
        embed_model = "embed-v4.0" if USE_EMBED_V4 else "embed-multilingual-v3.0"
        print(f"    🔧 Using embedding model: {embed_model}")
        # デバッグ: メモリエラーシミュレーション
        if DEBUG_MODE and SIMULATE_MEMORY_ERROR_AT > 0 and file_index == SIMULATE_MEMORY_ERROR_AT:
            print(f"🧪 [DEBUG] Simulating memory error at file #{file_index}")
            raise MemoryError("Simulated out-of-memory event for debugging")
        
        # デバッグ: 処理エラーシミュレーション
        if DEBUG_MODE and SIMULATE_PROCESSING_ERROR_AT > 0 and file_index == SIMULATE_PROCESSING_ERROR_AT:
            print(f"🧪 [DEBUG] Simulating processing error at file #{file_index}")
            raise Exception("Simulated processing error for debugging")
        
        # デバッグ: APIコストを削減するため、ダミーベクトルを返す
        if DEBUG_MODE:
            print(f"🧪 [DEBUG] Returning dummy embedding for '{filename}' (saves API cost)")
            # モデルに応じた次元数のダミーベクトル
            dimensions = 1024 if embed_model == "embed-multilingual-v3.0" else 1024  # embed-v4.0も1024次元
            dummy_vec = np.random.normal(0, 1, dimensions)
            dummy_vec = dummy_vec / np.linalg.norm(dummy_vec)  # 正規化
            return dummy_vec
        # 1. ファイル名をtextとしてベクトル化
        text_response = co_client.embed(
            texts=[filename],
            model=embed_model,
            input_type="search_document"
        )
        text_vec = np.array(text_response.embeddings[0])
        
        # 2. 画像をimageとしてベクトル化（data URI形式で送信）
        file_extension = filename.lower().split('.')[-1]
        if file_extension in ['jpg', 'jpeg']:
            mime_type = 'jpeg'
        elif file_extension in ['png']:
            mime_type = 'png'
        elif file_extension in ['gif']:
            mime_type = 'gif'
        elif file_extension in ['webp']:
            mime_type = 'webp'
        else:
            mime_type = 'jpeg'
        
        base64_string = base64.b64encode(image_bytes).decode("utf-8")
        data_uri = f"data:image/{mime_type};base64,{base64_string}"
        
        image_response = co_client.embed(
            images=[data_uri],
            model=embed_model,
            input_type="image"
        )
        image_vec = np.array(image_response.embeddings[0])
        
        # 3. コサイン類似度wを計算
        dot_product = np.dot(text_vec, image_vec)
        norm_text = np.linalg.norm(text_vec)
        norm_image = np.linalg.norm(image_vec)
        w = dot_product / (norm_text * norm_image)
        
        # wを0-1の範囲にクリップ（負の値を避ける）
        w = max(0, min(1, w))
        
        # 4. 重み付け統合ベクトルを計算
        final_vec = w * text_vec + (1 - w) * image_vec
        
        print(f"    📊 Text-Image similarity: {w:.3f} for '{filename}'")
        return final_vec
        
    except Exception as e:
        print(f"    ⚠️  Warning: Could not generate multimodal embedding for '{filename}'. Skipping. Reason: {e}")
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
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{uuid}.json")
        blob.upload_from_string(
            json.dumps(embeddings, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        
        if is_final:
            print(f"✅ Final save completed: {len(embeddings)} embeddings")
        else:
            print(f"💾 Checkpoint saved: {len(embeddings)} embeddings to {uuid}.json")
            
    except Exception as e:
        print(f"❌ Failed to save checkpoint: {e}")
        traceback.print_exc()

def main():
    """Cloud Runジョブとして実行されるメイン関数"""
    all_embeddings = []  # グローバルに参照できるように最初に初期化
    
    # シグナルハンドラーの設定
    def signal_handler(signum, frame):
        """シグナル受信時の処理"""
        print(f"\n⚠️  Signal {signum} received. Attempting to save current progress...")
        if all_embeddings:
            try:
                save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=False)
                print(f"✅ Emergency save successful: {len(all_embeddings)} embeddings saved")
            except Exception as e:
                print(f"❌ Emergency save failed: {e}")
        sys.exit(1)
    
    # SIGTERM（Cloud Runからの終了シグナル）とSIGINT（Ctrl+C）を捕捉
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    print("===================================================")
    print(f"  Starting Vectorization Job for UUID: {UUID}")
    print(f"  Target Drive URL: {DRIVE_URL}")
    print(f"  Using Embed Model: {'embed-v4.0' if USE_EMBED_V4 else 'embed-multilingual-v3.0'}")
    print(f"  Checkpoint Mode: Save on error only")
    print("===================================================")

    try:
        # 既存のembeddingsを読み込む
        existing_embeddings, processed_files = load_existing_embeddings(GCS_BUCKET_NAME, UUID)
        all_embeddings = existing_embeddings  # 読み込んだデータで初期化
        
        if DEBUG_MODE:
            # デバッグモードではダミーのファイルリストを使用
            files_to_process = [
                {'name': 'debug_image_1.jpg', 'id': 'debug_id_1', 'webViewLink': 'https://debug.example.com/1', 'folder_path': '/debug'},
                {'name': 'debug_image_2.png', 'id': 'debug_id_2', 'webViewLink': 'https://debug.example.com/2', 'folder_path': '/debug'}
            ]
            print(f"🧪 [DEBUG] Using {len(files_to_process)} dummy files for testing")
        else:
            files_to_process = list_files_in_drive_folder(DRIVE_URL)
            if not files_to_process:
                print("✅ No processable images found. Job finished successfully.")
                return
        
        # 既に処理済みのファイルをスキップ
        original_count = len(files_to_process)
        files_to_process = [f for f in files_to_process if f['name'] not in processed_files]
        skipped_count = original_count - len(files_to_process)
        
        if not files_to_process:
            print(f"✅ All {skipped_count} images already processed (found {original_count} total, {len(processed_files)} in existing data). Job finished successfully.")
            return

        print(f"Found {len(files_to_process)} new images to process (skipping {skipped_count} already processed)")
        
        if not DEBUG_MODE:
            print("Initializing Google Drive service...")
            drive_creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
            drive_service = build('drive', 'v3', credentials=drive_creds)
        else:
            drive_service = None
            print("🧪 [DEBUG] Skipping Google Drive service initialization")
        
        # 処理開始時刻を記録
        start_time = datetime.now()
        
        # 進捗表示用の固定値を計算
        total_files = len(files_to_process) + len(processed_files)
        initial_processed_count = len(processed_files)

        for i, file_info in enumerate(files_to_process, 1):
            current_index = initial_processed_count + i
            print(f"  ({current_index}/{total_files}) Processing: {file_info['name'][:50]}...")
            
            try:
                if DEBUG_MODE:
                    # デバッグモードではPILでダミー画像を生成
                    print("    🧪 [DEBUG] Using dummy image data (skipping actual download)")
                    dummy_img = Image.new('RGB', (100, 100), color='red')
                    output = io.BytesIO()
                    dummy_img.save(output, format='JPEG')
                    image_content = output.getvalue()
                else:
                    # 1. Download image from Google Drive
                    request = drive_service.files().get_media(fileId=file_info['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()
                    image_content = fh.getvalue()
                
                # 2. Resize if necessary
                resized_content = resize_image_if_needed(image_content, file_info['name'])
                if resized_content is None:
                    print(f"    ⏭️  Skipping due to resize failure")
                    continue

                # 3. Get multimodal embedding
                embedding = get_multimodal_embedding(resized_content, file_info['name'], current_index)
                if embedding is not None:
                    result_data = {
                        "filename": file_info['name'],
                        "filepath": file_info['webViewLink'],
                        "folder_path": file_info['folder_path'],
                        "embedding": embedding.tolist()
                    }
                    all_embeddings.append(result_data)
                    processed_files.add(file_info['name'])
                    
                    # 定期的なチェックポイント保存（100件ごと）
                    if len(all_embeddings) % 100 == 0 and len(all_embeddings) > 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        print(f"\n⏱️  Elapsed time: {elapsed:.1f} seconds")
                        print(f"📊 Progress: {len(all_embeddings)} embeddings generated")
                        
                        # チェックポイント保存（OOM対策）
                        save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=False)
                        
                        # メモリ解放
                        gc.collect()
                        print(f"🧹 Memory cleanup performed\n")

            except Exception as e:
                print(f"    ❌ Error processing {file_info['name']}: {e}")
                # エラーが発生したらチェックポイントを保存
                if len(all_embeddings) > 0:
                    print(f"    💾 Saving checkpoint after error (total: {len(all_embeddings)} embeddings)...")
                    save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=False)
        
        if not all_embeddings:
            print("⚠️  No embeddings were generated. Check for previous warnings.")
            return
            
        # 最終保存
        elapsed_total = (datetime.now() - start_time).total_seconds()
        print(f"\n⏱️  Total processing time: {elapsed_total:.1f} seconds")
        print(f"📊 Final count: {len(all_embeddings)} embeddings")
        
        save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=True)
        
        print(f"✅ Successfully saved vector data to gs://{GCS_BUCKET_NAME}/{UUID}.json")
        print("🎉 Job finished successfully.")

    except Exception as e:
        error_type = type(e).__name__
        print(f"❌ An unexpected error occurred during the job execution ({error_type}):")
        if DEBUG_MODE:
            print(f"🧪 [DEBUG] Error type: {error_type}")
            if "memory" in str(e).lower() or isinstance(e, MemoryError):
                print(f"🧪 [DEBUG] Memory error detected - this triggers the checkpoint save functionality")
        traceback.print_exc()
        
        # エラー時も最後に保存を試みる
        if 'all_embeddings' in locals() and all_embeddings:
            print(f"\n💾 Attempting to save {len(all_embeddings)} embeddings before exit...")
            try:
                save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=False)
                print(f"✅ Emergency save successful")
            except Exception as save_error:
                print(f"❌ Emergency save failed: {save_error}")
        
        raise e

if __name__ == "__main__":
    main()


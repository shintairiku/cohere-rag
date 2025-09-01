import os
import io
import json
import traceback
import base64
import hashlib

import cohere
import numpy as np
from dotenv import load_dotenv
from google.cloud import storage
from PIL import Image

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
MAX_IMAGE_SIZE_MB = 5  # Cohere API制限: 最大5MB

if not all([GCS_BUCKET_NAME, COHERE_API_KEY, UUID, DRIVE_URL]):
    missing = [
        var for var in ['GCS_BUCKET_NAME', 'COHERE_API_KEY', 'UUID', 'DRIVE_URL']
        if not os.getenv(var)
    ]
    raise RuntimeError(f"FATAL: Required environment variables are missing: {', '.join(missing)}")

# --- 2. グローバルクライアントの初期化 ---
co_client = cohere.Client(COHERE_API_KEY)
storage_client = storage.Client()
MAX_FILE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024

def resize_image_if_needed(image_content: bytes, filename: str) -> bytes:
    """
    画像のファイルサイズが上限を超える場合、サイズから必要なダウンサンプリング量を計算してリサイズする。
    """
    if len(image_content) <= MAX_FILE_SIZE_BYTES:
        return image_content

    original_size_mb = len(image_content) / (1024 * 1024)
    print(f"    📏 Large file detected ({original_size_mb:.1f}MB > 5MB limit): {filename}. Calculating optimal resize...")
    
    try:
        img = Image.open(io.BytesIO(image_content))
        original_width, original_height = img.size
        
        # RGBAやPモードの画像をRGBに変換
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
            img = background

        # 目標サイズを4.8MBに設定（余裕を持たせる）
        target_size_bytes = int(MAX_FILE_SIZE_BYTES * 0.96)  # 4.8MB
        
        # ファイルサイズの圧縮率を計算（線形近似）
        # 画像のピクセル数はファイルサイズにほぼ比例すると仮定
        size_ratio = target_size_bytes / len(image_content)
        
        # 必要な解像度スケールを計算（面積比の平方根）
        scale_factor = min(1.0, (size_ratio ** 0.5))
        
        # 最小でも0.3倍までしかスケールダウンしない（品質保持のため）
        scale_factor = max(0.3, scale_factor)
        
        new_width = int(original_width * scale_factor)
        new_height = int(original_height * scale_factor)
        
        print(f"    🔢 Calculated scale factor: {scale_factor:.3f} ({original_width}x{original_height} -> {new_width}x{new_height})")
        
        # 計算されたサイズでリサイズ
        resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # まず品質90で試す
        output = io.BytesIO()
        resized_img.save(output, format='JPEG', quality=90, optimize=True)
        resized_data = output.getvalue()
        
        # まだ大きい場合は品質を段階的に下げる
        quality = 90
        while len(resized_data) > MAX_FILE_SIZE_BYTES and quality >= 50:
            quality -= 10
            output = io.BytesIO()
            resized_img.save(output, format='JPEG', quality=quality, optimize=True)
            resized_data = output.getvalue()
            
        final_size_mb = len(resized_data) / (1024 * 1024)
        
        if len(resized_data) <= MAX_FILE_SIZE_BYTES:
            print(f"    ✅ Successfully resized: {original_size_mb:.1f}MB -> {final_size_mb:.1f}MB")
            print(f"       Scale: {scale_factor:.1%}, Quality: {quality}, Size: {new_width}x{new_height}")
            return resized_data
        else:
            # それでも大きい場合は、さらに解像度を下げる
            print(f"    🔄 Still too large ({final_size_mb:.1f}MB), applying additional scaling...")
            
            # より強いスケールダウンを適用
            additional_scale = 0.8
            final_width = int(new_width * additional_scale)
            final_height = int(new_height * additional_scale)
            
            final_img = resized_img.resize((final_width, final_height), Image.Resampling.LANCZOS)
            output = io.BytesIO()
            final_img.save(output, format='JPEG', quality=60, optimize=True)
            final_data = output.getvalue()
            
            final_size_mb = len(final_data) / (1024 * 1024)
            
            if len(final_data) <= MAX_FILE_SIZE_BYTES:
                print(f"    ✅ Final resize successful: {original_size_mb:.1f}MB -> {final_size_mb:.1f}MB")
                print(f"       Final size: {final_width}x{final_height}, Quality: 60")
                return final_data
            else:
                print(f"    ⚠️ Warning: Could not resize {filename} below 5MB limit. Final size: {final_size_mb:.1f}MB. Skipping.")
                return None
        
    except Exception as e:
        print(f"    ❌ Resize Error for {filename}: {e}")
        traceback.print_exc()
        return None

def get_multimodal_embedding(image_bytes: bytes, filename: str) -> np.ndarray:
    """画像データとファイル名から重み付けされたベクトルを生成する"""
    try:
        # 1. ファイル名をtextとしてベクトル化
        text_response = co_client.embed(
            texts=[filename],
            model="embed-multilingual-v3.0",
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
            model="embed-multilingual-v3.0",
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

def main():
    """Cloud Runジョブとして実行されるメイン関数"""
    print("===================================================")
    print(f"  Starting Vectorization Job for UUID: {UUID}")
    print(f"  Target Drive URL: {DRIVE_URL}")
    print("===================================================")

    try:
        files_to_process = list_files_in_drive_folder(DRIVE_URL)
        if not files_to_process:
            print("✅ No processable images found. Job finished successfully.")
            return

        print(f"Found {len(files_to_process)} images. Initializing Google Drive service...")
        
        all_embeddings = []
        drive_creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
        drive_service = build('drive', 'v3', credentials=drive_creds)

        for i, file_info in enumerate(files_to_process, 1):
            print(f"  ({i}/{len(files_to_process)}) Processing: {file_info['folder_path']}/{file_info['name']}...")
            try:
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
                    continue

                # 3. Get multimodal embedding
                embedding = get_multimodal_embedding(resized_content, file_info['name'])
                if embedding is not None:
                    result_data = {
                        "filename": file_info['name'],
                        "filepath": file_info['webViewLink'],
                        "folder_path": file_info['folder_path'],
                        "embedding": embedding.tolist()
                    }
                    all_embeddings.append(result_data)

            except Exception as e:
                print(f"    -> Error processing file {file_info['name']}: {e}")
        
        if not all_embeddings:
            print("⚠️  No embeddings were generated. Check for previous warnings.")
            return
            
        print(f"\nGenerated {len(all_embeddings)} embeddings. Uploading to Cloud Storage...")
        
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(f"{UUID}.json")
        blob.upload_from_string(
            json.dumps(all_embeddings, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        
        print(f"✅ Successfully saved vector data to gs://{GCS_BUCKET_NAME}/{UUID}.json")
        print("🎉 Job finished successfully.")

    except Exception as e:
        print(f"❌ An unexpected error occurred during the job execution:")
        traceback.print_exc()
        raise e

if __name__ == "__main__":
    main()


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
    画像のファイルサイズが上限を超える場合、上限未満になるまで品質と解像度を下げてリサイズする。
    """
    if len(image_content) <= MAX_FILE_SIZE_BYTES:
        return image_content

    print(f"    📏 Large file detected ({len(image_content) / (1024*1024):.1f}MB > 5MB limit): {filename}. Resizing...")
    
    try:
        img = Image.open(io.BytesIO(image_content))
        
        # RGBAやPモードの画像をRGBに変換
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
            img = background

        # 品質とスケールを段階的に試行
        for scale in [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]:
            new_size = (int(img.width * scale), int(img.height * scale))
            resized_img = img.resize(new_size, Image.Resampling.LANCZOS)
            
            for quality in [90, 80, 70, 60, 50]:
                output = io.BytesIO()
                resized_img.save(output, format='JPEG', quality=quality, optimize=True)
                resized_data = output.getvalue()
                
                if len(resized_data) <= MAX_FILE_SIZE_BYTES:
                    print(f"       ✅ Resized: {len(resized_data)/(1024*1024):.1f}MB (Scale: {scale*100}%, Quality: {quality})")
                    return resized_data
        
        print(f"    ⚠️ Warning: Could not resize the image below the limit. Skipping.")
        return None
    except Exception as e:
        print(f"    ❌ Resize Error: {e}")
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


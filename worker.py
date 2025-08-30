import os
import io
import re
import base64
import json
import hashlib
import traceback
from typing import Dict

import cohere
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from PIL import Image
from google.cloud import storage

import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

load_dotenv()

# --- 設定 ---
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB

if not all([GCS_BUCKET_NAME, COHERE_API_KEY]):
    raise RuntimeError("GCS_BUCKET_NAME and COHERE_API_KEY must be set.")

# --- グローバルクライアント ---
app = FastAPI()
co_client = cohere.Client(COHERE_API_KEY)
storage_client = storage.Client()

# --- 認証ヘルパー ---
def _get_google_credentials():
    creds, _ = google.auth.default(scopes=SCOPES)
    return creds

# --- 画像処理ロジック (img_meta_processor_gdrive.pyから移植・改変) ---

def get_weighted_embedding(image_data: bytes, filename: str) -> np.ndarray:
    def get_embedding(text: str, input_type: str) -> np.ndarray:
        response = co_client.embed(texts=[text], model="embed-multilingual-v3.0", input_type=input_type)
        return np.array(response.embeddings[0])

    img_vec = get_embedding(filename, "search_document")
    meta_vec = get_embedding(filename, "search_query")
    
    w = np.dot(img_vec, meta_vec) / (np.linalg.norm(img_vec) * np.linalg.norm(meta_vec))
    vec = w * meta_vec + (1.0 - w) * img_vec
    return vec

def resize_image_if_needed(image_content: bytes, filename: str) -> bytes:
    if len(image_content) <= MAX_FILE_SIZE:
        return image_content
    # ... (リサイズ処理は以前のコードと同じ) ...
    print(f"📏 Resizing large file: {filename}")
    try:
        img = Image.open(io.BytesIO(image_content))
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1])
            img = background
        
        for scale in [0.9, 0.8, 0.7, 0.6, 0.5]:
            for quality in [90, 80, 70, 60, 50]:
                new_size = (int(img.width * scale), int(img.height * scale))
                resized_img = img.resize(new_size, Image.Resampling.LANCZOS)
                output = io.BytesIO()
                resized_img.save(output, format='JPEG', quality=quality)
                resized_data = output.getvalue()
                if len(resized_data) <= MAX_FILE_SIZE:
                    return resized_data
    except Exception as e:
        print(f"❌ Resize error for {filename}: {e}")
    return None # Return None if resizing fails or is still too large

# --- Pub/Subトリガーのエンドポイント ---

@app.post("/process")
async def process_image_task(request: Request):
    """
    Pub/Subからのプッシュサブスクリプションによってトリガーされるエンドポイント。
    画像1枚を処理し、結果を一時ファイルとしてGCSに保存する。
    """
    envelope = await request.json()
    if not envelope or 'message' not in envelope:
        raise HTTPException(status_code=400, detail="Invalid Pub/Sub message format")

    message = envelope['message']
    try:
        # Pub/Subメッセージをデコード
        payload_str = base64.b64decode(message['data']).decode('utf-8')
        payload = json.loads(payload_str)
        
        uuid = payload['uuid']
        file_id = payload['file_id']
        file_name = payload['file_name']
        web_view_link = payload['web_view_link']
        folder_path = payload['folder_path']
        
        print(f"👷‍♀️ Worker received task: Process {file_name} for UUID {uuid}")

        # Google Driveから画像をダウンロード
        creds = _get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        
        drive_request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, drive_request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        image_content = fh.getvalue()
        
        # 画像をリサイズ
        resized_content = resize_image_if_needed(image_content, file_name)
        if not resized_content:
            print(f"⚠️ Skipping {file_name} due to resize failure.")
            return {"status": "skipped", "reason": "resize_failed"}

        # ベクトル化
        embedding = get_weighted_embedding(resized_content, file_name)
        
        # 結果をJSONオブジェクトとして構築
        result_data = {
            "filename": file_name,
            "filepath": web_view_link,
            "folder_path": folder_path,
            "full_path": f"{folder_path}/{file_name}" if folder_path else file_name,
            "file_id": file_id,
            "file_hash": hashlib.sha256(resized_content).hexdigest(),
            "file_size": len(resized_content),
            "embedding": embedding.tolist()
        }

        # GCSの一時ディレクトリに保存
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        # ファイルIDをファイル名に使い、重複を防ぐ
        blob_path = f"temp/{uuid}/{file_id}.json"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(json.dumps(result_data, ensure_ascii=False), content_type="application/json")

        print(f"✅ Successfully processed and saved to {blob_path}")
        return {"status": "success"}

    except Exception as e:
        print(f"❌ Worker error: {e}")
        traceback.print_exc()
        # Pub/Subに再試行させるためにエラーを返す
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def health_check():
    return {"status": "ok", "service": "worker"}

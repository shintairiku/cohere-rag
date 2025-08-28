import os
import io
import re
import base64
import json
import hashlib
import traceback
from typing import List, Dict, Set

import cohere
import numpy as np
from dotenv import load_dotenv
from PIL import Image
from google.cloud import storage

# --- 認証ライブラリ ---
import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

load_dotenv()

# --- スコープから 'spreadsheets' を削除 ---
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def _get_google_credentials():
    """
    環境に応じてGoogle Drive用の認証情報を取得するヘルパー関数。
    """
    environment = os.getenv("ENVIRONMENT", "local")
    key_file = "marketing-automation-461305-2acf4965e0b0.json"

    if environment == "production":
        print("🌐 プロダクション環境: ADC を使用します。")
        creds, _ = google.auth.default(scopes=SCOPES)
        return creds
    else:
        print(f"🏠 ローカル環境: '{key_file}' を探しています...")
        if os.path.exists(key_file):
            creds = service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES)
            return creds
        else:
            print(f"   ⚠️ キーファイルが見つかりません。ADC にフォールバックします。")
            creds, _ = google.auth.default(scopes=SCOPES)
            return creds

class ImageProcessor:
    # ... (変更点は __init__ のみ) ...
    def __init__(self, drive_folder_id_or_url: str, embeddings_file: str, credentials):
        self.drive_folder_id = self._extract_folder_id(drive_folder_id_or_url)
        if not self.drive_folder_id:
            raise ValueError(f"無効なGoogle DriveフォルダIDまたはURLです: {drive_folder_id_or_url}")
        
        self.embeddings_file = embeddings_file
        self.max_file_size = 20 * 1024 * 1024

        self.api_key = os.getenv("COHERE_API_KEY")
        if not self.api_key:
            raise ValueError("COHERE_API_KEYが環境変数に設定されていません。")
        
        self.client = cohere.Client(self.api_key)
        
        self.creds = credentials
        self.drive_service = build('drive', 'v3', credentials=self.creds)
        
        self.processed_images: Set[str] = set()
        self.embeddings_data: List[Dict] = []
        
        self.load_existing_data()

    def _extract_folder_id(self, id_or_url: str) -> str:
        if id_or_url.startswith('http') or id_or_url.startswith('www'):
            match = re.search(r'/folders/([a-zA-Z0-9_-]+)', id_or_url)
            if match:
                return match.group(1)
            match = re.search(r'id=([a-zA-Z0-9_-]+)', id_or_url)
            if match:
                return match.group(1)
        return id_or_url
    
    def load_existing_data(self):
        if os.path.exists(self.embeddings_file):
            with open(self.embeddings_file, 'r', encoding='utf-8') as f:
                self.embeddings_data = json.load(f)
            self.processed_images = {item.get('file_hash') for item in self.embeddings_data if 'file_hash' in item}
            print(f"📁 既存データ {len(self.embeddings_data)}件を読み込みました。")

    def get_file_hash(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def resize_image_if_needed(self, image_content: bytes, filename: str) -> bytes:
        if len(image_content) <= self.max_file_size:
            return image_content
        print(f"📏 大きなファイルを検出 ({len(image_content) / (1024*1024):.1f}MB): {filename}。リサイズを実行します...")
        try:
            img = Image.open(io.BytesIO(image_content))
            original_size = img.size
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
                img = background
            quality_levels = [90, 80, 70, 60, 50]
            scale_factors = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5]
            for scale in scale_factors:
                new_width = int(original_size[0] * scale)
                new_height = int(original_size[1] * scale)
                resized_img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                for quality in quality_levels:
                    output = io.BytesIO()
                    resized_img.save(output, format='JPEG', quality=quality, optimize=True)
                    resized_data = output.getvalue()
                    if len(resized_data) <= self.max_file_size:
                        print(f"   ✅ リサイズ完了: {len(resized_data)/(1024*1024):.1f}MB (スケール: {scale*100}%, 品質: {quality}%)")
                        return resized_data
            print(f"   ⚠️ 警告: 最大限圧縮しても20MBを超えました。処理をスキップします。")
            return None
        except Exception as e:
            print(f"   ❌ リサイズエラー: {e}")
            return None

    def get_image_embedding(self, image_data: bytes, filename: str) -> np.ndarray:
        try:
            response = self.client.embed(texts=[filename], model='embed-multilingual-v3.0', input_type="search_document")
            return np.array(response.embeddings[0])
        except Exception as e:
            print(f"❌ 画像埋め込み生成エラー ({filename}): {e}")
            return None

    def get_meta_embedding(self, filename: str) -> np.ndarray:
        try:
            response = self.client.embed(texts=[filename], model="embed-multilingual-v3.0", input_type="search_query")
            return np.array(response.embeddings[0])
        except Exception as e:
            print(f"❌ メタデータ埋め込み生成エラー ({filename}): {e}")
            return None

    def get_weighted_image_and_meta_embedding(self, image_data: bytes, filename: str) -> np.ndarray:
        img_vec = self.get_image_embedding(image_data, filename)
        if img_vec is None: return None
        meta_vec = self.get_meta_embedding(filename)
        if meta_vec is None: return None
        w = np.dot(img_vec, meta_vec) / (np.linalg.norm(img_vec) * np.linalg.norm(meta_vec))
        vec = w * meta_vec + (1.0 - w) * img_vec
        return vec

    def get_all_subfolders(self, folder_id: str) -> List[Dict[str, str]]:
        all_folders = [{'id': folder_id, 'name': 'ROOT', 'path': ''}]
        folders_to_check = [{'id': folder_id, 'name': 'ROOT', 'path': ''}]
        while folders_to_check:
            current_folder = folders_to_check.pop(0)
            query = f"'{current_folder['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            for subfolder in results.get('files', []):
                folder_path = f"{current_folder['path']}/{subfolder['name']}" if current_folder['path'] else subfolder['name']
                folder_info = {'id': subfolder['id'], 'name': subfolder['name'], 'path': folder_path}
                all_folders.append(folder_info)
                folders_to_check.append(folder_info)
        return all_folders

    def get_images_from_folder(self, folder_id: str, folder_path: str = '') -> List[Dict]:
        query = f"'{folder_id}' in parents and (mimeType='image/jpeg' or mimeType='image/png') and trashed=false"
        results = self.drive_service.files().list(q=query, fields="files(id, name, size, webViewLink)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        images = results.get('files', [])
        for image in images:
            image['folder_path'] = folder_path
        return images

    def process_drive_images(self):
        print(f"🔍 Google DriveフォルダID '{self.drive_folder_id}' を再帰的に検索中...")
        all_folders = self.get_all_subfolders(self.drive_folder_id)
        all_images = [img for folder in all_folders for img in self.get_images_from_folder(folder['id'], folder['path'])]
        if not all_images:
            print("✅ フォルダ内に処理対象の画像が見つかりませんでした。")
            return
        print(f"📊 合計 {len(all_images)}件の画像を検出。処理を開始します...")
        processed_count = 0
        for i, item in enumerate(all_images, 1):
            display_name = f"{item['folder_path']}/{item['name']}" if item['folder_path'] else item['name']
            print(f"🔄 処理中 {i}/{len(all_images)}: {display_name}")
            request = self.drive_service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            image_content = fh.getvalue()
            file_hash = self.get_file_hash(image_content)
            if file_hash in self.processed_images:
                print("  ⏭️  スキップ: 処理済みです。")
                continue
            resized_content = self.resize_image_if_needed(image_content, item['name'])
            if resized_content is None: continue
            embedding = self.get_weighted_image_and_meta_embedding(resized_content, item['name'])
            if embedding is not None:
                new_data = {
                    "filename": item['name'], "filepath": item['webViewLink'], "folder_path": item['folder_path'],
                    "full_path": display_name, "file_id": item['id'], "file_hash": file_hash,
                    "file_size": len(resized_content), "embedding": embedding.tolist()
                }
                self.embeddings_data.append(new_data)
                self.processed_images.add(file_hash)
                processed_count += 1
                print(f"  ✅ 完了: {display_name}")
        if processed_count > 0:
            self.save_embeddings()

    def save_embeddings(self):
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        destination_blob_name = os.path.basename(self.embeddings_file)
        json_data = json.dumps(self.embeddings_data, ensure_ascii=False, indent=2)
        if bucket_name:
            try:
                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_string(json_data, content_type='application/json')
                print(f"☁️  データベースをGCSバケット '{bucket_name}' の '{destination_blob_name}' に保存しました。")
            except Exception as e:
                print(f"❌ GCSへの保存エラー: {e}")
                traceback.print_exc()
        else:
            try:
                output_dir = os.path.dirname(self.embeddings_file)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                with open(self.embeddings_file, 'w', encoding='utf-8') as f:
                    f.write(json_data)
                print(f"💾 データベースをローカルの '{self.embeddings_file}' に保存しました。")
            except Exception as e:
                print(f"❌ ローカルへの保存エラー: {e}")

# --- スプレッドシート関連の関数を削除し、新しい関数を定義 ---
def process_drive_folder(uuid: str, drive_url: str, output_dir: str):
    """
    APIから渡された情報をもとに、単一のGoogle Driveフォルダをベクトル化する。
    """
    print(f"🚀 ベクトル化処理開始: UUID = {uuid}")
    print(f"   - Drive URL: {drive_url}")
    
    try:
        credentials = _get_google_credentials()
        output_json_path = os.path.join(output_dir, f"{uuid}.json")
        
        processor = ImageProcessor(
            drive_folder_id_or_url=drive_url,
            embeddings_file=output_json_path,
            credentials=credentials
        )
        processor.process_drive_images()
        print(f"✅ 処理完了: UUID = {uuid}")

    except Exception as e:
        print(f"❌ 予期せぬエラーが発生しました (UUID: {uuid}): {e}")
        traceback.print_exc()

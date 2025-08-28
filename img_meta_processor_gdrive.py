"""
画像処理専用スクリプト - RAGデータベース構築 (Google Drive対応版 - 再帰的フォルダ検索)
元のスクリプトのベクトル化ロジックを完全に維持し、フォルダの再帰検索機能を追加。
"""

import os
import io
import re
import base64
import traceback
import json
import hashlib
from typing import List, Dict, Set
from dotenv import load_dotenv
import cohere
import numpy as np
from PIL import Image

# Google Drive API関連のライブラリ
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage

load_dotenv()

class ImageProcessor:
    def __init__(self, drive_folder_id_or_url: str, embeddings_file: str = "embeddings.json", service_account_file: str = "service_account.json"):
        self.drive_folder_id = self._extract_folder_id(drive_folder_id_or_url)
        if not self.drive_folder_id:
            raise ValueError(f"無効なGoogle DriveフォルダIDまたはURLです: {drive_folder_id_or_url}")
        
        self.embeddings_file = embeddings_file
        self.service_account_file = service_account_file
        self.max_file_size = 20 * 1024 * 1024  # 20MB

        self.api_key = os.getenv("COHERE_API_KEY")
        if not self.api_key:
            raise ValueError("COHERE_API_KEYが環境変数に設定されていません。")
        
        self.client = cohere.ClientV2(api_key=self.api_key)
        self.drive_service = self._get_drive_service()
        
        self.processed_images: Set[str] = set()
        self.embeddings_data: List[Dict] = []
        
        self.load_existing_data()

    def _extract_folder_id(self, id_or_url: str) -> str:
        """Google DriveのURLからフォルダIDを抽出する。IDが直接渡された場合はそのまま返す。"""
        if id_or_url.startswith('http') or id_or_url.startswith('www'):
            match = re.search(r'/folders/([a-zA-Z0-9_-]+)', id_or_url)
            if match:
                return match.group(1)
        return id_or_url

    def _get_drive_service(self):
        """サービスアカウントキーを使用してGoogle Drive APIサービスを初期化する"""
        try:
            creds = service_account.Credentials.from_service_account_file(
                self.service_account_file, scopes=['https://www.googleapis.com/auth/drive.readonly'])
            return build('drive', 'v3', credentials=creds)
        except FileNotFoundError:
            raise FileNotFoundError(f"サービスアカウントキー '{self.service_account_file}' が見つかりません。")
        except Exception as e:
            raise RuntimeError(f"Google Driveサービスへの接続に失敗しました: {e}")

    def load_existing_data(self):
        """既存の埋め込みデータを読み込む"""
        if os.path.exists(self.embeddings_file):
            with open(self.embeddings_file, 'r', encoding='utf-8') as f:
                self.embeddings_data = json.load(f)
            self.processed_images = {item.get('file_hash') for item in self.embeddings_data if 'file_hash' in item}
            print(f"📁 既存データ {len(self.embeddings_data)}件を読み込みました。")

    def get_file_hash(self, content: bytes) -> str:
        return hashlib.sha256(content).hexdigest()

    def resize_image_if_needed(self, image_content: bytes, filename: str) -> bytes:
        """
        画像のファイルサイズが20MBを超える場合、20MB未満になるまで品質と解像度を下げてリサイズする。
        """
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

    def image_to_base64_data_url(self, image_data: bytes, filename: str) -> str:
        base64_string = base64.b64encode(image_data).decode('utf-8')
        ext = os.path.splitext(filename)[1].lower()
        mime_type = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png'}.get(ext, 'image/jpeg')
        return f"data:{mime_type};base64,{base64_string}"

    def get_image_embedding(self, image_data: bytes, filename: str) -> np.ndarray:
        """画像の埋め込みベクトルを生成（元のスクリプトと同じロジック）"""
        try:
            base64_url = self.image_to_base64_data_url(image_data, filename)
            
            image_input = {
                "content": [
                    {"type": "image_url", "image_url": {"url": base64_url}}
                ]
            }
            
            response = self.client.embed(
                model="embed-v4.0",
                inputs=[image_input],
                input_type="search_document",
                embedding_types=["float"]
            )
            
            embedding = response.embeddings.float_[0]
            return np.array(embedding)
            
        except Exception as e:
            print(f"❌ 画像埋め込み生成エラー ({filename}): {e}")
            return None

    def get_meta_embedding(self, filename: str) -> np.ndarray:
        """メタデータの埋め込みベクトルを生成（元のスクリプトと同じロジック）"""
        try:
            response = self.client.embed(
                model="embed-v4.0",
                texts=[filename],
                input_type="search_query",
                embedding_types=["float"]
            )
            return np.array(response.embeddings.float_[0])
        except Exception as e:
            print(f"❌ メタデータ埋め込み生成エラー ({filename}): {e}")
            return None

    def get_weighted_image_and_meta_embedding(self, image_data: bytes, filename: str) -> np.ndarray:
        """
        画像のみのベクトル(img_vec)とファイル名を検索クエリ化したテキストベクトル(meta_vec)を生成し、
        重みwで合成した最終ベクトルを返す（元のスクリプトと同じロジック）
        """
        img_vec = self.get_image_embedding(image_data, filename)
        if img_vec is None: 
            return None
        
        meta_vec = self.get_meta_embedding(filename)
        if meta_vec is None: 
            return None
        
        # 元のスクリプトと同じ重み計算ロジック
        w = np.dot(img_vec, meta_vec) / (np.linalg.norm(img_vec) * np.linalg.norm(meta_vec))
        vec = w * meta_vec + (1.0 - w) * img_vec
        return vec

    def get_all_subfolders(self, folder_id: str) -> List[Dict[str, str]]:
        """
        指定されたフォルダID以下のすべてのサブフォルダを再帰的に取得する
        戻り値: [{'id': folder_id, 'name': folder_name, 'path': folder_path}, ...]
        """
        all_folders = [{'id': folder_id, 'name': 'ROOT', 'path': ''}]
        folders_to_check = [{'id': folder_id, 'name': 'ROOT', 'path': ''}]
        
        while folders_to_check:
            current_folder = folders_to_check.pop(0)
            current_id = current_folder['id']
            current_path = current_folder['path']
            
            # 現在のフォルダ内のサブフォルダを取得
            query = f"'{current_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            subfolders = results.get('files', [])
            for subfolder in subfolders:
                folder_path = f"{current_path}/{subfolder['name']}" if current_path else subfolder['name']
                folder_info = {
                    'id': subfolder['id'], 
                    'name': subfolder['name'], 
                    'path': folder_path
                }
                all_folders.append(folder_info)
                folders_to_check.append(folder_info)
        
        return all_folders

    def get_images_from_folder(self, folder_id: str, folder_path: str = '') -> List[Dict]:
        """指定されたフォルダから画像ファイルを取得する"""
        query = f"'{folder_id}' in parents and (mimeType='image/jpeg' or mimeType='image/png') and trashed=false"
        
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name, size, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        images = results.get('files', [])
        
        # 各画像にフォルダパス情報を追加
        for image in images:
            image['folder_path'] = folder_path
            
        return images

    def process_drive_images(self):
        """Google Driveフォルダ内の新しい画像を再帰的に処理する"""
        print(f"🔍 Google DriveフォルダID '{self.drive_folder_id}' を再帰的に検索中...")
        
        # すべてのサブフォルダを取得
        all_folders = self.get_all_subfolders(self.drive_folder_id)
        print(f"📂 {len(all_folders)}個のフォルダを発見しました（ルートフォルダ含む）")
        
        # 各フォルダから画像を収集
        all_images = []
        for folder_info in all_folders:
            folder_id = folder_info['id']
            folder_path = folder_info['path']
            folder_name = folder_info['name']
            
            images = self.get_images_from_folder(folder_id, folder_path)
            if images:
                print(f"   📁 {folder_name} ({folder_path}): {len(images)}枚の画像")
                all_images.extend(images)
        
        if not all_images:
            print("✅ フォルダ内に処理対象の画像が見つかりませんでした。")
            return

        print(f"📊 合計 {len(all_images)}件の画像を検出。処理を開始します...")
        processed_count = 0
        
        for i, item in enumerate(all_images, 1):
            file_id = item['id']
            filename = item['name'] 
            file_size = int(item.get('size', 0))
            web_link = item['webViewLink']
            folder_path = item['folder_path']
            
            # フォルダパスを含めた表示用の名前を作成
            display_name = f"{folder_path}/{filename}" if folder_path else filename
            
            print(f"🔄 処理中 {i}/{len(all_images)}: {display_name} ({file_size/(1024*1024):.2f}MB)")

            # ファイルをダウンロード
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            image_content = fh.getvalue()

            file_hash = self.get_file_hash(image_content)
            if file_hash in self.processed_images:
                print("  ⏭️  スキップ: 処理済みです。")
                continue

            resized_content = self.resize_image_if_needed(image_content, filename)
            
            if resized_content is None:
                print(f"  ❌ 失敗: 画像のリサイズに失敗したため、処理をスキップします。")
                continue

            embedding = self.get_weighted_image_and_meta_embedding(resized_content, filename)
            if embedding is not None:
                new_data = {
                    "filename": filename, 
                    "filepath": web_link, 
                    "folder_path": folder_path,  # フォルダパス情報を追加
                    "full_path": display_name,   # フルパス情報を追加
                    "file_id": file_id,
                    "file_hash": file_hash, 
                    "file_size": len(resized_content),
                    "embedding": embedding.tolist()
                }
                self.embeddings_data.append(new_data)
                self.processed_images.add(file_hash)
                processed_count += 1
                print(f"  ✅ 完了: {display_name}")
            else:
                print(f"  ❌ 失敗: {display_name}")
        
        if processed_count > 0:
            self.save_embeddings()
            print(f"\n🎉 処理完了! 新たに{processed_count}件の画像を追加しました。")
            print(f"📊 総画像数: {len(self.embeddings_data)}件")

    def save_embeddings(self):
        """
        埋め込みデータをJSONファイルに保存する。
        環境変数 GCS_BUCKET_NAME が設定されていればGCSに、なければローカルに保存する。
        """
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        # self.embeddings_file には 'vector_data/[uuid].json' のようなパスが入っている
        # GCSに保存する際はファイル名部分だけを使う
        destination_blob_name = os.path.basename(self.embeddings_file)
        
        # JSONデータを文字列として準備
        json_data = json.dumps(self.embeddings_data, ensure_ascii=False, indent=2)

        # GCSへのアップロード処理
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
        
        # ローカルへの保存処理 (フォールバックまたは開発用)
        else:
            try:
                # self.embeddings_file のディレクトリ部分が存在するか確認
                output_dir = os.path.dirname(self.embeddings_file)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                    
                with open(self.embeddings_file, 'w', encoding='utf-8') as f:
                    f.write(json_data)
                print(f"💾 データベースをローカルの '{self.embeddings_file}' に保存しました。")
            except Exception as e:
                print(f"❌ ローカルへの保存エラー: {e}")

def main():
    DRIVE_FOLDER_ID_OR_URL = "19pF7i9-KrRdyPHAU5ki6f39zG7qLhk1b"  # サンプル
    DRIVE_FOLDER_ID_OR_URL = "1unmGILSEk0zj0w5izDKoF9-lAmhQjXbC"  # 尚建工務店

    if DRIVE_FOLDER_ID_OR_URL == "YOUR_GOOGLE_DRIVE_FOLDER_ID_OR_URL":
        print("❌ エラー: `main`関数内の`DRIVE_FOLDER_ID_OR_URL`を実際のIDまたはURLに書き換えてください。")
        return

    print("🖼️  画像処理システム (Google Drive版 - 再帰検索対応)")
    print("=" * 60)
    
    processor = ImageProcessor(
        drive_folder_id_or_url=DRIVE_FOLDER_ID_OR_URL,
        embeddings_file="embedding_gdrive_shoken.json"
    )
    
    processor.process_drive_images()
    
    print(f"\n✅ 全ての処理が完了しました。")

if __name__ == "__main__":
    main()
import os
import re
from typing import List, Dict

import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def _get_google_credentials():
    # ... (img_meta_processor_gdrive.pyから移動した認証ヘルパー) ...
    environment = os.getenv("ENVIRONMENT", "local")
    if environment == "production":
        creds, _ = google.auth.default(scopes=SCOPES)
        return creds
    else:
        key_file = "marketing-automation-461305-2acf4965e0b0.json"
        if os.path.exists(key_file):
            return service_account.Credentials.from_service_account_file(key_file, scopes=SCOPES)
        else:
            creds, _ = google.auth.default(scopes=SCOPES)
            return creds

def _extract_folder_id(id_or_url: str) -> str:
    # ... (img_meta_processor_gdrive.pyから移動) ...
    if id_or_url.startswith('http'):
        # /folders/xxx形式
        match = re.search(r'/folders/([a-zA-Z0-9_-]+)', id_or_url)
        if match: return match.group(1)
        # open?id=xxx形式（共有リンク）
        match = re.search(r'[?&]id=([a-zA-Z0-9_-]+)', id_or_url)
        if match: return match.group(1)
        # /d/xxx/形式
        match = re.search(r'/d/([a-zA-Z0-9_-]+)/', id_or_url)
        if match: return match.group(1)
    return id_or_url

def list_files_in_drive_folder(drive_url: str) -> List[Dict]:
    """
    指定されたGoogle Driveフォルダ内の全画像ファイル情報を再帰的にリストアップする。
    """
    creds = _get_google_credentials()
    drive_service = build('drive', 'v3', credentials=creds)
    folder_id = _extract_folder_id(drive_url)
    
    print(f"  -> Extracted folder ID: {folder_id} from URL: {drive_url}")

    # 全サブフォルダを取得
    folders_to_check = [{'id': folder_id, 'path': ''}]
    all_folders = list(folders_to_check)
    while folders_to_check:
        current_folder = folders_to_check.pop(0)
        query = f"'{current_folder['id']}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        for subfolder in results.get('files', []):
            folder_path = f"{current_folder['path']}/{subfolder['name']}" if current_folder['path'] else subfolder['name']
            folder_info = {'id': subfolder['id'], 'path': folder_path}
            all_folders.append(folder_info)
            folders_to_check.append(folder_info)

    # 全画像を取得
    all_images = []
    # サポートする画像形式を拡張
    image_mime_types = [
        'image/jpeg',
        'image/jpg',
        'image/png',
        'image/gif',
        'image/webp',
        'image/bmp',
        'image/svg+xml'
    ]
    mime_query = ' or '.join([f"mimeType='{mime}'" for mime in image_mime_types])
    
    for folder in all_folders:
        query = f"'{folder['id']}' in parents and ({mime_query}) and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        for image in results.get('files', []):
            image['folder_path'] = folder['path']
            all_images.append(image)
    
    print(f"  -> Found {len(all_images)} images in {len(all_folders)} folders")
    if all_images:
        print(f"  -> Sample images: {[img['name'] for img in all_images[:3]]}")
            
    return all_images

import os
import io
import json
import traceback
import signal
import sys
from datetime import datetime
from typing import Optional, Tuple

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
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "cohere").lower()
MAX_IMAGE_SIZE_MB = 5
CHECKPOINT_INTERVAL = 100

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

storage_client = storage.Client()

MAX_FILE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024

def resize_image_if_needed(image_content: bytes, filename: str) -> Tuple[Optional[bytes], Optional[str]]:
    """
    画像の解像度が埋め込みAPIの制限を超える場合、ピクセル数ベースでリサイズする。
    """
    try:
        try:
            img = PILImage.open(io.BytesIO(image_content))
            img.verify()
            img = PILImage.open(io.BytesIO(image_content))
        except PILImage.DecompressionBombError as e:
            print(f"    ⚠️  '{filename}' でDecompression bomb警告が発生: {e}")
            print("       画像が極端に大きいか破損している可能性があるためスキップします。")
            return None, "decompression_bomb"
        except OSError as e:
            print(f"    ⚠️  画像ファイル '{filename}' を判別できません: {e}")
            print("       画像でないか破損している可能性があるためスキップします。")
            return None, "cannot_identify"
        except Exception as e:
            print(f"    ⚠️  画像 '{filename}' の読み込み中に想定外のエラー: {e}")
            return None, "open_error"
            
        original_width, original_height = img.size
        original_pixels = original_width * original_height
        original_size_mb = len(image_content) / (1024 * 1024)
        
        if original_pixels > 100_000_000:
            print(f"    ⚠️  超高解像度画像を検出: {original_width}x{original_height} ({original_pixels:,} pixels)")
            print("       安全に処理できないためスキップします。")
            return None, "too_large"
        
        MAX_PIXELS = 2_300_000
        
        if original_pixels <= MAX_PIXELS:
            return image_content, None
        
        print(f"    📏 高解像度画像を検出: {original_width}x{original_height} ({original_pixels:,} pixels > {MAX_PIXELS:,})")
        print(f"       ファイルサイズ: {original_size_mb:.1f}MB")
        
        if img.mode in ('RGBA', 'LA', 'P'):
            background = PILImage.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[-1] if 'A' in img.mode else None)
            img = background

        scale_factor = (MAX_PIXELS / original_pixels) ** 0.5
        scale_factor = max(0.3, scale_factor)
        
        new_width = int(original_width * scale_factor)
        new_height = int(original_height * scale_factor)
        new_pixels = new_width * new_height
        
        print(f"    🔢 縮小スケール: {scale_factor:.3f}")
        print(f"       変換後の解像度: {new_width}x{new_height} ({new_pixels:,} pixels)")
        
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
        
        print(f"    ✅ リサイズ完了: {original_size_mb:.1f}MB -> {resized_size_mb:.1f}MB")
        print(f"       解像度: {original_width}x{original_height} -> {new_width}x{new_height}")
        print(f"       出力品質: {quality}")
        
        return resized_data, None
        
    except Exception as e:
        print(f"    ❌ リサイズ中にエラーが発生: {e}")
        traceback.print_exc()
        return None, "resize_failure"

def get_multimodal_embedding(image_bytes: bytes, filename: str, file_index: int = 0, use_embed_v4: bool = False) -> np.ndarray:
    """画像データとファイル名から重み付けされたベクトルを生成する"""
    try:
        provider = get_embedding_provider()
        embedding = provider.embed_multimodal(
            text=filename,
            image_bytes=image_bytes,
            use_embed_v4=use_embed_v4,
        )
        return embedding
    
    except Exception as e:
        print(f"    ⚠️  '{filename}' のマルチモーダル埋め込み生成に失敗したためスキップします: {e}")
        traceback.print_exc()
        return None

def load_existing_embeddings(bucket_name: str, uuid: str) -> tuple:
    """既存のembeddingsと処理済みファイルリストを読み込む"""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{uuid}.json")
        
        if blob.exists():
            existing_data = json.loads(blob.download_as_text())
            processed_files = {item['filename'] for item in existing_data}
            print(f"📂 既存データを {len(existing_data)} 件読み込みました")
            return existing_data, processed_files
        else:
            print("📂 既存データが見つからなかったため新規作成します")
            return [], set()
    except Exception as e:
        print(f"⚠️  既存データの読み込みに失敗しました: {e}")
        return [], set()

def save_checkpoint(bucket_name: str, uuid: str, embeddings: list, is_final: bool = False):
    """チェックポイントとしてembeddingsを{uuid}.jsonに保存"""
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
            print(f"✅ [{current_time}] 最終保存完了: {len(embeddings)} 件を gs://{bucket_name}/{uuid}.json に保存しました")
        else:
            print(f"💾 [{current_time}] チェックポイント保存: {len(embeddings)} 件を gs://{bucket_name}/{uuid}.json に退避しました")
            
    except Exception as e:
        print(f"❌ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] gs://{bucket_name}/{uuid}.json への保存に失敗しました: {e}")
        traceback.print_exc()

def calculate_diff(drive_files: list, existing_embeddings: list) -> tuple:
    """
    Google Drive上のファイル一覧と既存ベクトルデータとの差分を算出する。
    
    引数:
        drive_files: Google Driveから取得したファイル情報のリスト
        existing_embeddings: 既存のベクトルデータ
        
    戻り値:
        追加対象ファイルのリストと、削除対象を示すキー集合のタプル
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
    
    print("\n📊 差分解析結果:")
    print(f"   Drive側ファイル数: {len(drive_file_keys)}")
    print(f"   ベクトル側ファイル数: {len(vector_file_keys)}")
    print(f"   追加対象: {len(files_to_add)}")
    print(f"   削除対象: {len(keys_to_delete)}")
    
    if keys_to_delete:
        print("\n🗑️  ベクトルデータから削除されるファイル一覧:")
        for key in list(keys_to_delete)[:10]:
            print(f"     - {key}")
        if len(keys_to_delete) > 10:
            print(f"     ... 残り {len(keys_to_delete) - 10} 件")
    
    return files_to_add, keys_to_delete

def remove_deleted_files(existing_embeddings: list, keys_to_delete: set) -> list:
    """
    差分計算で判定した削除対象を既存ベクトルデータから除外する。
    
    引数:
        existing_embeddings: 既存のベクトルデータ
        keys_to_delete: 削除対象を示すファイルキー集合
        
    戻り値:
        削除済みベクトルデータのリスト
    """
    if not keys_to_delete:
        return existing_embeddings.copy()
    
    original_count = len(existing_embeddings)
    
    # 削除対象以外を残す
    filtered_embeddings = [
        item for item in existing_embeddings
        if f"{item.get('folder_path', '')}/{item.get('filename', '')}" not in keys_to_delete
    ]
    
    deleted_count = original_count - len(filtered_embeddings)
    
    print("\n🗑️  削除処理が完了しました:")
    print(f"   元データ数: {original_count}")
    print(f"   削除数: {deleted_count}")
    print(f"   残存数: {len(filtered_embeddings)}")
    
    return filtered_embeddings

def process_single_uuid(uuid: str, drive_url: str, use_embed_v4: bool = False, all_embeddings: list = None) -> list:
    """単一UUIDの処理（差分検出・削除機能付き）"""
    if all_embeddings is None:
        all_embeddings = []
    
    print(f"📋 UUID {uuid} の処理を開始します")
    print(f"   Drive URL: {drive_url}")
    print(f"   利用モデル: {'embed-v4.0' if use_embed_v4 else 'embed-multilingual-v3.0'}")
    
    try:
        # 既存のembeddingsを読み込む
        existing_embeddings, _ = load_existing_embeddings(GCS_BUCKET_NAME, uuid)
        drive_files = list_files_in_drive_folder(drive_url)
        if not drive_files:
            print(f"⚠️  Google Driveにファイルが見つかりません: UUID {uuid}")
            if existing_embeddings:
                print(f"🗑️  Driveが空のため {len(existing_embeddings)} 件のベクトルを削除します")
                save_checkpoint(GCS_BUCKET_NAME, uuid, [], is_final=True)
            return []
        
        # 差分を計算
        files_to_add, keys_to_delete = calculate_diff(drive_files, existing_embeddings)
        
        # 削除処理を実行
        task_embeddings = remove_deleted_files(existing_embeddings, keys_to_delete)
        
        # 削除が発生した場合は即座に保存
        if keys_to_delete:
            save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=False)
            print(f"💾 削除後の中間保存を実施: {len(task_embeddings)} 件")
        
        # 追加対象がない場合は終了
        if not files_to_add:
            print(f"✅ 新規処理対象はありません (UUID {uuid})")
            if keys_to_delete:
                # 削除のみ発生した場合は最終保存
                save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=True)
            return task_embeddings
        
        print(f"\n📝 新規ファイル {len(files_to_add)} 件の処理を開始します...")
        
        print("Google Driveサービスを初期化しています...")
        drive_creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/drive.readonly'])
        drive_service = build('drive', 'v3', credentials=drive_creds)
        
        start_time = datetime.now()
        
        for i, file_info in enumerate(files_to_add, 1):
            print(f"    ({i}/{len(files_to_add)}) 処理中: {file_info['name'][:50]}...")
            
            try:
                request = drive_service.files().get_media(fileId=file_info['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                image_content = fh.getvalue()
                
                resized_content, resize_error = resize_image_if_needed(image_content, file_info['name'])
                if resized_content is None:
                    reason_text = resize_error or "unknown_error"
                    print(f"      ⭕️  リサイズできないためスキップします ({reason_text})")
                    error_entry = {
                        "filename": file_info['name'],
                        "filepath": file_info.get('webViewLink'),
                        "folder_path": file_info.get('folder_path'),
                        "embedding": None,
                        "is_corrupt": True,
                        "corrupt_reason": reason_text,
                    }
                    task_embeddings.append(error_entry)
                    continue

                embedding = get_multimodal_embedding(resized_content, file_info['name'], i, use_embed_v4)
                if embedding is not None:
                    result_data = {
                        "filename": file_info['name'],
                        "filepath": file_info['webViewLink'],
                        "folder_path": file_info['folder_path'],
                        "embedding": embedding.tolist(),
                        "is_corrupt": False,
                    }
                    task_embeddings.append(result_data)
                    
                    if i % CHECKPOINT_INTERVAL == 0:
                        print(f"📌 チェックポイント: {len(files_to_add)} 件中 {i} 件処理済み")
                        save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=False)
                        print(f"💾 現在の埋め込み数: {len(task_embeddings)} 件")

            except Exception as e:
                print(f"      ❌ {file_info['name']} の処理中にエラー: {e}")
                continue
        
        # タスク完了後にファイルを保存
        if task_embeddings != existing_embeddings or keys_to_delete:
            elapsed_total = (datetime.now() - start_time).total_seconds()
            print(f"   ⏱️  UUID {uuid} の処理時間: {elapsed_total:.1f} 秒")
            save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=True)
            print(f"   ✅ UUID {uuid} 用に {len(task_embeddings)} 件保存しました")
            print(f"   📊 変化量: 追加 {len(files_to_add)} 件 / 削除 {len(keys_to_delete)} 件")
        
        return task_embeddings
        
    except Exception as e:
        print(f"   ❌ UUID {uuid} の処理でエラーが発生: {e}")
        traceback.print_exc()
        if task_embeddings:
            try:
                save_checkpoint(GCS_BUCKET_NAME, uuid, task_embeddings, is_final=False)
                print(f"   💾 UUID {uuid} の途中結果を緊急保存 ({len(task_embeddings)} 件)")
            except Exception as save_error:
                print(f"   ❌ UUID {uuid} の緊急保存に失敗: {save_error}")
        raise e


def main():
    """Cloud Runジョブとして実行されるメイン関数"""
    
    print("🔧 使用環境変数一覧:")
    env_vars = [
        "GCS_BUCKET_NAME", "GCP_PROJECT_ID", "GCP_REGION", "VERTEX_MULTIMODAL_MODEL",
        "EMBEDDING_PROVIDER", "COHERE_API_KEY",
        "UUID", "DRIVE_URL", "USE_EMBED_V4", "BATCH_MODE", "BATCH_TASKS"
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
        print("  バッチベクトル化ジョブ（差分検出あり）を開始します")
        print(f"  タスク数: {len(BATCH_TASKS)}")
        print("  機能: 新規ファイルの自動追加 + 削除ファイルの自動除去")
        print("===================================================")
        
        total_processed = 0
        total_errors = 0
        
        for i, task in enumerate(BATCH_TASKS, 1):
            uuid = task.get('uuid')
            drive_url = task.get('drive_url')
            company_name = task.get('company_name', '')
            use_embed_v4 = task.get('use_embed_v4', False)
            
            print(f"\n📋 タスク {i}/{len(BATCH_TASKS)}: {company_name} (UUID: {uuid})")
            
            try:
                process_single_uuid(uuid, drive_url, use_embed_v4)
                total_processed += 1
                print(f"✅ タスク {i}が正常に完了しました")
                    
            except Exception as e:
                print(f"❌ タスク {i}でエラーが発生しました: {e}")
                total_errors += 1
                continue
        
        print(f"\n🎉 バッチ処理完了: 成功 {total_processed} 件 / 失敗 {total_errors} 件")
    else:
        print("===================================================")
        print("  単体ベクトル化ジョブ（差分検出あり）を開始します")
        print(f"  UUID: {UUID}")
        print(f"  Drive URL: {DRIVE_URL}")
        print(f"  Embed V4利用: {USE_EMBED_V4}")
        print("  機能: 新規ファイルの自動追加 + 削除ファイルの自動除去")
        print("===================================================")
        
        all_embeddings = []
        
        def signal_handler(signum, frame):
            print(f"\n⚠️  シグナル {signum} を受信したため、中間結果を保存します...")
            if all_embeddings:
                try:
                    save_checkpoint(GCS_BUCKET_NAME, UUID, all_embeddings, is_final=False)
                    print(f"✅ 緊急保存が完了しました: {len(all_embeddings)} 件")
                except Exception as e:
                    print(f"❌ 緊急保存に失敗しました: {e}")
            sys.exit(1)
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        all_embeddings = process_single_uuid(UUID, DRIVE_URL, USE_EMBED_V4)
        print("🎉 単体ジョブが正常に終了しました。")

if __name__ == "__main__":
    main()

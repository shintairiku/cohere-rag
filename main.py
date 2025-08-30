import os
import time
import json
import traceback
from typing import List, Optional, Dict

import cohere
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Body
from pydantic import BaseModel
from google.cloud import pubsub_v1, storage

# 検索ロジックとDriveスキャンロジックをインポート
from search import ImageSearcher
from drive_scanner import list_files_in_drive_folder

load_dotenv()

# --- 設定 ---
app = FastAPI(
    title="画像検索・ベクトル化API (Pub/Sub対応版)",
    version="3.0.0"
)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
PUBSUB_TOPIC_ID = "vectorize-tasks" 

# --- グローバルクライアント ---
co = cohere.Client(os.getenv("COHERE_API_KEY"))
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)

# --- キャッシュとヘルパー関数 ---
searcher_cache: Dict[str, tuple[float, ImageSearcher]] = {}
CACHE_TTL_SECONDS = 300

def get_searcher_for_uuid(uuid: str) -> ImageSearcher:
    # ... (この関数は変更なし) ...
    current_time = time.time()
    if uuid in searcher_cache and (current_time - searcher_cache[uuid][0]) < CACHE_TTL_SECONDS:
        print(f"📦 キャッシュから '{uuid}' の検索インスタンスを返します。")
        return searcher_cache[uuid][1]
    print(f"✨ '{uuid}' の検索インスタンスを新規作成します。")
    try:
        searcher = ImageSearcher(
            uuid=uuid,
            embeddings_dir='vector_data',
            bucket_name=GCS_BUCKET_NAME
        )
        searcher_cache[uuid] = (current_time, searcher)
        return searcher
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"UUID '{uuid}' に対応するベクトルデータが見つかりません。")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"検索エンジンの初期化に失敗しました: {e}")

# --- APIエンドポイント ---

class VectorizeRequest(BaseModel):
    uuid: str
    drive_url: str

@app.post("/vectorize", status_code=202)
async def vectorize_commander(request: VectorizeRequest):
    """司令塔: Driveをスキャンし、ファイル毎の処理タスクをPub/Subに発行"""
    print(f"司令塔: UUID '{request.uuid}' のベクトル化タスクを開始します。")
    try:
        files_to_process = list_files_in_drive_folder(request.drive_url)
        if not files_to_process:
            return {"message": "対象フォルダに画像が見つかりませんでした。"}

        print(f"  -> {len(files_to_process)} 件の画像を検出。Pub/Subにタスクを発行します...")
        
        published_count = 0
        for file_info in files_to_process:
            message_payload = {
                "uuid": request.uuid,
                "file_id": file_info['id'],
                "file_name": file_info['name'],
                "web_view_link": file_info['webViewLink'],
                "folder_path": file_info['folder_path']
            }
            message_data = json.dumps(message_payload).encode("utf-8")
            future = publisher.publish(topic_path, message_data)
            future.result() # 送信完了を待つ
            published_count += 1

        # 既存のキャッシュをクリア
        if request.uuid in searcher_cache:
            del searcher_cache[request.uuid]
            print(f"🧹 キャッシュをクリアしました: {request.uuid}")
            
        return {"message": f"{published_count} 件のベクトル化タスクをPub/Subに発行しました。"}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"タスク発行中にエラー: {e}")

class AggregateRequest(BaseModel):
    uuid: str

@app.post("/aggregate", status_code=200)
async def aggregate_results(request: AggregateRequest):
    """統合役: GCS上の一時ファイルをマージして最終的なJSONを作成"""
    uuid = request.uuid
    print(f"統合役: UUID '{uuid}' の結果統合処理を開始します。")
    
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    temp_prefix = f"temp/{uuid}/"
    
    blobs = list(bucket.list_blobs(prefix=temp_prefix))
    if not blobs:
        raise HTTPException(status_code=404, detail=f"UUID '{uuid}' の処理結果が見つかりません。")
    
    print(f"  -> {len(blobs)} 件の一時ファイルを検出。統合します...")
    
    all_embeddings = []
    for blob in blobs:
        try:
            data = json.loads(blob.download_as_string())
            all_embeddings.append(data)
        except Exception as e:
            print(f"  ⚠️ 一時ファイル {blob.name} の読み込みに失敗: {e}")

    # 最終的なJSONファイルを保存
    final_blob = bucket.blob(f"{uuid}.json")
    final_blob.upload_from_string(
        json.dumps(all_embeddings, ensure_ascii=False, indent=2),
        content_type="application/json"
    )
    print(f"  ✅ 最終ファイル '{final_blob.name}' を保存しました。")
    
    # 一時ファイルを削除
    for blob in blobs:
        blob.delete()
    print(f"  🗑️  一時ファイルをすべて削除しました。")
    
    # キャッシュをクリア
    if uuid in searcher_cache:
        del searcher_cache[uuid]
        print(f"🧹 キャッシュをクリアしました: {uuid}")

    return {"message": f"{len(all_embeddings)}件の結果を '{uuid}.json' に統合しました。"}


# 検索エンドポイントは変更なし
@app.get("/search", response_model=Dict)
def search_images_api(
    uuid: str = Query(..., description="検索対象企業のUUID"),
    q: Optional[str] = Query(None, description="検索クエリ"),
    top_k: int = Query(5, ge=1, le=50),
    trigger: str = Query("類似画像検索"),
):
    print(f"🔍 検索リクエスト受信 - uuid: '{uuid}', trigger: '{trigger}', q: '{q}'")
    searcher = get_searcher_for_uuid(uuid)
    try:
        if trigger == "類似画像検索":
            if not q:
                raise HTTPException(status_code=400, detail="類似画像検索には検索クエリ 'q' が必須です。")
            response = co.embed(texts=[q], model="embed-multilingual-v3.0", input_type="search_query")
            query_embedding = response.embeddings[0]
            results = searcher.search_images(query_embedding=query_embedding, top_k=top_k)
            return {"query": q, "results": results}
        elif trigger == "ランダム画像検索":
            results = searcher.random_image_search(count=top_k)
            return {"query": "ランダム検索", "results": results}
        else:
            raise HTTPException(status_code=400, detail=f"無効なトリガー: {trigger}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"検索中に予期せぬエラーが発生しました: {str(e)}")



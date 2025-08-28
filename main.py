import os
import time
import traceback
from typing import List, Optional, Dict

import cohere
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

# 修正された関数をインポート
from img_meta_processor_gdrive import process_drive_folder
from search import ImageSearcher

load_dotenv()

app = FastAPI(
    title="画像検索・ベクトル化API",
    description="企業別の画像検索とGoogle Drive画像のベクトル化を実行するAPIです。",
    version="2.2.0"
)

# --- 設定項目 ---
VECTOR_DATA_DIR = 'vector_data'
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

COHERE_API_KEY = os.getenv("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise RuntimeError("COHERE_API_KEYが環境変数に設定されていません。")
co = cohere.Client(COHERE_API_KEY)

# --- 検索インスタンスのキャッシュ ---
searcher_cache: Dict[str, tuple[float, ImageSearcher]] = {}
CACHE_TTL_SECONDS = 300

def get_searcher_for_uuid(uuid: str) -> ImageSearcher:
    current_time = time.time()
    if uuid in searcher_cache and (current_time - searcher_cache[uuid][0]) < CACHE_TTL_SECONDS:
        print(f"📦 キャッシュから '{uuid}' の検索インスタンスを返します。")
        return searcher_cache[uuid][1]
    print(f"✨ '{uuid}' の検索インスタンスを新規作成します。")
    try:
        searcher = ImageSearcher(
            uuid=uuid,
            embeddings_dir=VECTOR_DATA_DIR,
            bucket_name=GCS_BUCKET_NAME
        )
        searcher_cache[uuid] = (current_time, searcher)
        return searcher
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"UUID '{uuid}' に対応するベクトルデータが見つかりません。")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"検索エンジンの初期化に失敗しました: {e}")

# --- APIエンドポイント ---

@app.get("/")
def read_root():
    return {"message": "画像検索API v2.2へようこそ！"}

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

# --- ここから修正 ---
class VectorizeRequest(BaseModel):
    uuid: str
    drive_url: str

@app.post("/vectorize", status_code=202)
async def vectorize_company_images(
    request: VectorizeRequest,
    background_tasks: BackgroundTasks
):
    """
    指定されたUUIDとDrive URLの画像ベクトル化をバックグラウンドで実行します。
    """
    if not all([request.uuid, request.drive_url]):
        raise HTTPException(status_code=400, detail="uuidとdrive_urlは必須です。")

    print(f"📬 ベクトル化リクエスト受信: UUID = {request.uuid}")
    background_tasks.add_task(
        process_drive_folder,
        uuid=request.uuid,
        drive_url=request.drive_url,
        output_dir=VECTOR_DATA_DIR
    )
    
    if request.uuid in searcher_cache:
        del searcher_cache[request.uuid]
        print(f"🧹 キャッシュをクリアしました: {request.uuid}")

    return {"message": f"UUID '{request.uuid}' のベクトル化処理を開始しました。"}
# --- ここまで修正 ---

@app.get("/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

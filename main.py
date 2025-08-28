import os
import time
import traceback
from typing import List, Optional, Dict

import cohere
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

# 責務を分離したスクリプトからクラスと関数をインポート
from img_meta_processor_gdrive import process_company_by_uuid
from search import ImageSearcher

load_dotenv()

# --- FastAPIアプリケーションのインスタンスを作成 ---
app = FastAPI(
    title="画像検索・ベクトル化API",
    description="企業別の画像検索とGoogle Drive画像のベクトル化を実行するAPIです。",
    version="2.0.0"
)

# --- 設定項目 ---
SERVICE_ACCOUNT_FILE = 'service_account.json'
SPREADSHEET_NAME = '類似画像検索（統合版）'
COMPANY_LIST_SHEET_NAME = '会社一覧'
VECTOR_DATA_DIR = 'vector_data'  # ローカル保存用ディレクトリ
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") # GCSバケット名

# Cohereクライアントを初期化
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
if not COHERE_API_KEY:
    raise RuntimeError("COHERE_API_KEYが環境変数に設定されていません。")
co = cohere.Client(COHERE_API_KEY)

# --- 検索インスタンスのキャッシュ ---
# パフォーマンス向上のため、一度ロードしたImageSearcherをキャッシュします。
# { "uuid": (timestamp, searcher_instance) }
searcher_cache: Dict[str, tuple[float, ImageSearcher]] = {}
CACHE_TTL_SECONDS = 300  # キャッシュの有効期間（秒）、ここでは5分

def get_searcher_for_uuid(uuid: str) -> ImageSearcher:
    """
    指定されたUUIDのImageSearcherをキャッシュから取得または新規作成する。
    """
    current_time = time.time()
    
    # キャッシュが存在し、かつ有効期間内の場合
    if uuid in searcher_cache and (current_time - searcher_cache[uuid][0]) < CACHE_TTL_SECONDS:
        print(f"📦 キャッシュから '{uuid}' の検索インスタンスを返します。")
        return searcher_cache[uuid][1]

    # キャッシュがないか、有効期限切れの場合
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
        raise HTTPException(status_code=404, detail=f"UUID '{uuid}' に対応するベクトルデータが見つかりません。先にベクトル化を実行してください。")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"検索エンジンの初期化に失敗しました: {e}")

# --- APIエンドポイント ---

@app.get("/")
def read_root():
    return {"message": "画像検索API v2.0へようこそ！"}

@app.get("/search", response_model=Dict)
def search_images_api(
    uuid: str = Query(..., description="検索対象企業のUUID"),
    q: Optional[str] = Query(None, description="検索したい画像の自然言語クエリ (例: モダンなリビング)"),
    top_k: int = Query(5, ge=1, le=50, description="取得する検索結果の数"),
    trigger: str = Query("類似画像検索", description="トリガー名 ('類似画像検索' または 'ランダム画像検索')"),
):
    """
    指定された企業のベクトルデータを使用して、自然言語クエリで画像を検索します。
    """
    print(f"🔍 検索リクエスト受信 - uuid: '{uuid}', trigger: '{trigger}', q: '{q}'")
    searcher = get_searcher_for_uuid(uuid)

    try:
        if trigger == "類似画像検索":
            if not q:
                raise HTTPException(status_code=400, detail="類似画像検索には検索クエリ 'q' が必須です。")
            
            # クエリをベクトル化
            response = co.embed(texts=[q], model="embed-v4.0", input_type="search_query")
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

class VectorizeRequest(BaseModel):
    uuid: str

@app.post("/vectorize", status_code=202)
async def vectorize_company_images(
    request: VectorizeRequest,
    background_tasks: BackgroundTasks
):
    """
    指定されたUUIDの企業の画像ベクトル化をバックグラウンドで実行します。
    """
    target_uuid = request.uuid
    if not target_uuid:
        raise HTTPException(status_code=400, detail="UUIDは必須です。")

    print(f"📬 ベクトル化リクエスト受信: UUID = {target_uuid}")
    # バックグラウンドタスクとしてベクトル化処理を呼び出し
    background_tasks.add_task(
        process_company_by_uuid,
        uuid_to_process=target_uuid,
        service_account_file=SERVICE_ACCOUNT_FILE,
        spreadsheet_name=SPREADSHEET_NAME,
        sheet_name=COMPANY_LIST_SHEET_NAME,
        output_dir=VECTOR_DATA_DIR
    )
    
    # キャッシュをクリアして、次回の検索時に新しいデータを読み込むようにする
    if target_uuid in searcher_cache:
        del searcher_cache[target_uuid]
        print(f"🧹 キャッシュをクリアしました: {target_uuid}")

    return {"message": f"UUID '{target_uuid}' のベクトル化処理をバックグラウンドで開始しました。"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

# uvicornで実行するための設定
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

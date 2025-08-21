import os
from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel

# search.pyからImageSearcherクラスをインポート
from search import ImageSearcher

# FastAPIアプリケーションのインスタンスを作成
app = FastAPI(
    title="画像検索API",
    description="自然言語クエリを使って画像を検索するAPIです。",
    version="1.0.0"
)

# グローバル変数としてsearcherと起動エラーを保持
searcher = None
startup_error = None

@app.on_event("startup")
def load_searcher():
    """
    アプリケーション起動時に一度だけImageSearcherを初期化します。
    これにより、リクエストごとのファイル読み込みを回避し、パフォーマンスを向上させます。
    """
    global searcher, startup_error
    try:
        searcher = ImageSearcher(embeddings_file="embedding_gdrive_shoken.json")
    except Exception as e:
        startup_error = str(e)
        print(f"❌ サーバー起動エラー: {startup_error}")

# APIのレスポンスモデルを定義
class SearchResult(BaseModel):
    filename: Optional[str]
    filepath: Optional[str]
    similarity: float

class SearchResponse(BaseModel):
    query: Optional[str]
    results: List[SearchResult]

@app.get("/")
def read_root():
    """
    ルートURLへのアクセス時に簡単な説明を返します。
    """
    return {"message": "画像検索APIへようこそ！ '/docs' にアクセスしてAPIドキュメントを確認してください。"}

@app.get("/search", response_model=SearchResponse)
def search_images_api(
    q: Optional[str] = Query(None, description="検索したい画像の自然言語クエリ (例: モダンなリビング)"),
    top_k: int = Query(5, ge=1, le=50, description="取得する検索結果の数"),
    trigger: str = Query(..., description="トリガー名 (例: 類似画像検索)"),
):
    """
    自然言語クエリで画像を検索します。
    - **q**: 検索クエリ (任意)
    - **top_k**: 上位何件の結果を返すか (デフォルト: 5, 最小: 1, 最大: 50)
    """
    if startup_error:
        raise HTTPException(status_code=500, detail=f"サーバー起動エラー: {startup_error}")
    if not searcher:
        raise HTTPException(status_code=500, detail="検索エンジンの初期化に失敗しました。")
    
    """
    if not q:
        raise HTTPException(status_code=400, detail="クエリパラメータ 'q' は必須です。")
    """

    if trigger == "類似画像検索" and not q:
        raise HTTPException(status_code=400, detail="類似画像検索にはクエリパラメータ 'q' が必須です。")

    if trigger == "類似画像検索":
        try:
            results = searcher.search_images(query=q, top_k=top_k)
            return {"query": q, "results": results}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"検索中に予期せぬエラーが発生しました: {str(e)}")    
    elif trigger == "ランダム画像検索":
        try:
            results = searcher.random_image_search(count=top_k)
            return {"query": q, "results": results}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"検索中に予期せぬエラーが発生しました: {str(e)}")    

# uvicornで実行するための設定（ローカルテスト用）
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

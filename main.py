import os
from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
import traceback
from dotenv import load_dotenv
load_dotenv()

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
        print("🚀 ImageSearcherを初期化中...")
        # GCSバケット名を環境変数から取得（デフォルト値も設定）
        bucket_name = os.getenv("GCS_BUCKET_NAME", "embedding_storage")
        embeddings_file = "embedding_gdrive_shoken.json"
        
        print(f"📦 GCSバケット: {bucket_name}")
        print(f"📄 埋め込みファイル: {embeddings_file}")
        
        searcher = ImageSearcher(bucket_name=bucket_name, embeddings_file=embeddings_file)
        print("✅ ImageSearcherの初期化が完了しました")
    except Exception as e:
        startup_error = str(e)
        print(f"❌ サーバー起動エラー: {startup_error}")
        traceback.print_exc()

# APIのレスポンスモデルを定義
class SearchResult(BaseModel):
    filename: Optional[str]
    filepath: Optional[str]
    similarity: Optional[float]

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
    print(f"🔍 API呼び出し - trigger: '{trigger}', q: '{q}', top_k: {top_k}")
    
    if startup_error:
        print(f"❌ 起動エラーのため処理を停止: {startup_error}")
        raise HTTPException(status_code=500, detail=f"サーバー起動エラー: {startup_error}")
    if not searcher:
        print("❌ searcherが初期化されていません")
        raise HTTPException(status_code=500, detail="検索エンジンの初期化に失敗しました。")
    
    try:
        if trigger == "類似画像検索":
            print(f"📊 類似画像検索を実行中...")
            results = searcher.search_images(query=q, top_k=top_k)
            print(f"✅ 類似画像検索完了: {len(results)}件の結果")
            return {"query": q, "results": results}
            
        elif trigger == "ランダム画像検索":
            print(f"🎲 ランダム画像検索を実行中...")
            results = searcher.random_image_search(count=top_k)
            print(f"✅ ランダム画像検索完了: {len(results)}件の結果")
            
            # 結果の構造をログ出力してデバッグ
            if results:
                print(f"🔍 最初の結果のキー: {list(results[0].keys())}")
                print(f"🔍 最初の結果: {results[0]}")
            
            return {"query": "ランダム検索", "results": results}
        else:
            print(f"❌ 無効なトリガー: {trigger}")
            raise HTTPException(status_code=400, detail=f"無効なトリガー: {trigger}")
            
    except Exception as e:
        print(f"❌ 検索エラー発生:")
        print(f"   - trigger: {trigger}")
        print(f"   - query: {q}")
        print(f"   - error: {str(e)}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"検索中に予期せぬエラーが発生しました: {str(e)}")

@app.get("/health")
def health_check():
    """サービスの状態を確認するためのヘルスチェックエンドポイント"""
    if startup_error:
        return {"status": "error", "error": startup_error}
    if not searcher:
        return {"status": "error", "error": "検索エンジンが初期化されていません"}
    return {
        "status": "ok", 
        "embeddings_count": len(searcher.embeddings_data) if searcher.embeddings_data else 0
    }

# uvicornで実行するための設定（ローカルテスト用）
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
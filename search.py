import os
import json
import numpy as np
import random
from typing import List, Dict
import cohere
from google.cloud import storage # GCSライブラリをインポート
from dotenv import load_dotenv
load_dotenv()

class ImageSearcher:
    # GCSバケット名をコンストラクタで受け取るように変更
    def __init__(self, bucket_name: str, embeddings_file: str = "embedding_gdrive_shoken.json"):
        """
        コンストラクタ
        - GCSバケット名とファイル名を設定します。
        - 環境変数からCohere APIキーを読み込みます。
        - GCSとCohereのクライアントを初期化します。
        - GCSから画像埋め込みデータをロードします。
        """
        self.bucket_name = bucket_name
        self.embeddings_file = embeddings_file # GCS上のファイルパス
        self.api_key = os.getenv("COHERE_API_KEY")
        
        if not self.api_key:
            raise ValueError("環境変数 'COHERE_API_KEY' が設定されていません。")
        
        # GCSとCohereのクライアントを初期化
        self.storage_client = storage.Client()
        self.client = cohere.Client(api_key=self.api_key)
        self.embeddings_data = []
        
        self.load_embeddings_from_gcs() # GCSから読み込むメソッドを呼び出し
    
    def load_embeddings_from_gcs(self):
        """埋め込みデータをGCS上のJSONファイルから読み込みます。"""
        try:
            # GCSバケットとファイル（blob）を取得
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(self.embeddings_file)
            
            if not blob.exists():
                raise FileNotFoundError(f"GCSバケット '{self.bucket_name}' 内に '{self.embeddings_file}' が見つかりません。")
            
            print(f"GCSから '{self.embeddings_file}' をダウンロード中...")
            
            # ファイルの内容を文字列としてダウンロード
            json_data = blob.download_as_string()
            
            # 文字列をJSONとして解析
            self.embeddings_data = json.loads(json_data)
            
            print(f"✅ GCSから {len(self.embeddings_data)}件の画像埋め込みデータを正常に読み込みました。")
            
        except Exception as e:
            raise RuntimeError(f"GCSからの埋め込みデータ読み込み中にエラーが発生しました: {e}")
    
    def get_text_embedding(self, text: str) -> np.ndarray:
        """テキストクエリの埋め込みベクトルをCohere APIで生成します。"""
        try:
            response = self.client.embed(
                model="embed-v4.0",
                texts=[text],
                input_type="search_query"
            )
            embedding = response.embeddings[0]
            return np.array(embedding)
        except Exception as e:
            print(f"❌ テキスト埋め込みの生成中にエラーが発生しました: {e}")
            return None
    
    def cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """2つのベクトル間のコサイン類似度を計算します。"""
        if a.shape != b.shape:
            raise ValueError(f"ベクトルの次元が一致しません: a.shape={a.shape}, b.shape={b.shape}")
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
    
    def search_images(self, query: str = "", top_k: int = 5) -> List[Dict]:
        """自然言語クエリで画像を検索し、類似度が高い上位K件の結果を返します。"""
        if not self.embeddings_data:
            return []
        
        query_embedding = self.get_text_embedding(query)
        if query_embedding is None:
            return []
        
        similarities = []
        for item in self.embeddings_data:
            image_embedding = np.array(item["embedding"])
            try:
                similarity = self.cosine_similarity(query_embedding, image_embedding)
                similarities.append({
                    "filename": item.get("filename"),
                    "filepath": item.get("filepath"),
                    "similarity": similarity
                })
            except ValueError as e:
                print(f"⚠️ 類似度計算エラー: {item.get('filename')} - {e}")
                continue
        
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        return similarities[:top_k]
    
    def random_image_search(self, count: int = 5) -> List[Dict]:
        """ランダムに画像を選択して返します。"""
        if not self.embeddings_data:
            print("⚠️ 埋め込みデータが読み込まれていません。")
            return []
        
        try:
            shuffled_data = self.embeddings_data.copy()
            random.shuffle(shuffled_data)
            random_results = shuffled_data[:min(count, len(shuffled_data))]
            formatted_results = []
            for item in random_results:
                formatted_results.append({
                    "filename": item.get("filename"),
                    "filepath": item.get("filepath"),
                    "similarity": 0.0
                })
            print(f"✅ {len(formatted_results)}件のランダム画像を取得しました。")
            return formatted_results
        except Exception as e:
            print(f"❌ ランダム画像検索中にエラーが発生しました: {e}")
            return []

# --- 使い方 ---
if __name__ == '__main__':
    # GCSのバケット名とJSONファイル名を設定
    GCS_BUCKET_NAME = "embedding_storage"  # 例: "my-image-embeddings-bucket"
    EMBEDDINGS_JSON_FILE = "embedding_gdrive_shoken.json" # バケット内のファイルパス

    try:
        # ImageSearcherのインスタンスを作成
        searcher = ImageSearcher(bucket_name=GCS_BUCKET_NAME, embeddings_file=EMBEDDINGS_JSON_FILE)
        
        # 画像検索の実行
        search_query = "ライト"
        results = searcher.search_images(query=search_query, top_k=3)
        
        if results:
            print(f"\n--- 検索結果: '{search_query}' ---")
            for result in results:
                print(f"ファイル: {result['filename']}, 類似度: {result['similarity']:.4f}")
        else:
            print("検索結果が見つかりませんでした。")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
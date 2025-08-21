import os
import json
import numpy as np
import random
from typing import List, Dict
import cohere

class ImageSearcher:
    def __init__(self, embeddings_file: str = "embedding_gdrive_shoken.json"):
        """
        コンストラクタ
        - 環境変数からCohere APIキーを読み込みます。
        - 指定されたJSONファイルから画像埋め込みデータをロードします。
        """
        self.embeddings_file = embeddings_file
        self.api_key = os.getenv("COHERE_API_KEY")
        
        if not self.api_key:
            raise ValueError("環境変数 'COHERE_API_KEY' が設定されていません。")
        
        # 元のスクリプトと同じClientV2を使用
        self.client = cohere.Client(api_key=self.api_key) 
        self.embeddings_data = []
        
        self.load_embeddings()
    
    def load_embeddings(self):
        """埋め込みデータをJSONファイルから読み込みます。"""
        if not os.path.exists(self.embeddings_file):
            raise FileNotFoundError(f"埋め込みファイル '{self.embeddings_file}' が見つかりません。")
        
        try:
            with open(self.embeddings_file, 'r', encoding='utf-8') as f:
                self.embeddings_data = json.load(f)
            print(f"✅ {len(self.embeddings_data)}件の画像埋め込みデータを正常に読み込みました。")
        except Exception as e:
            raise RuntimeError(f"埋め込みデータの読み込み中にエラーが発生しました: {e}")
    
    def get_text_embedding(self, text: str) -> np.ndarray:
        """テキストクエリの埋め込みベクトルをCohere APIで生成します。"""
        try:
            # 元のスクリプトと同じモデルとパラメータを使用
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
        # ベクトルの次元数が一致しているか確認
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
                # 次元が異なるなどのエラーをここで捕捉
                print(f"⚠️ 類似度計算エラー: {item.get('filename')} - {e}")
                continue
        
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        
        return similarities[:top_k]
    
    def random_image_search(self, count: int = 5) -> List[Dict]:
        """
        ランダムに画像を選択して返します。
        search_images関数と同じ形式で返すことが重要です。
        """
        if not self.embeddings_data:
            print("⚠️ 埋め込みデータが読み込まれていません。")
            return []
        
        try:
            # データをシャッフルして指定件数を取得
            shuffled_data = self.embeddings_data.copy()
            random.shuffle(shuffled_data)
            
            # 指定件数分取得（データ数がcountより少ない場合は全データを返す）
            random_results = shuffled_data[:min(count, len(shuffled_data))]
            
            # search_images関数と完全に同じ形式で結果を整形
            formatted_results = []
            for item in random_results:
                formatted_results.append({
                    "filename": item.get("filename"),
                    "filepath": item.get("filepath"),
                    "similarity": 0.0  # ランダム検索では類似度は0.0に固定
                })
            
            print(f"✅ {len(formatted_results)}件のランダム画像を取得しました。")
            return formatted_results
            
        except Exception as e:
            print(f"❌ ランダム画像検索中にエラーが発生しました: {e}")
            return []
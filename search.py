import os
import json
import traceback
from typing import List, Dict, Optional

import numpy as np
from google.cloud import storage

def _get_storage_client():
    """
    環境に応じてGCSクライアントを初期化するヘルパー関数。
    """
    environment = os.getenv("ENVIRONMENT", "local")
    key_file = "marketing-automation-461305-2acf4965e0b0.json" # ローカル開発用のキーファイル

    if environment == "production":
        print("🌐 プロダクション環境: デフォルト認証でGCSクライアントを初期化します。")
        return storage.Client()
    else:
        print(f"🏠 ローカル環境: '{key_file}' を探しています...")
        if os.path.exists(key_file):
            print(f"   ✅ キーファイル '{key_file}' を使用します。")
            return storage.Client.from_service_account_json(key_file)
        else:
            print(f"   ⚠️ キーファイルが見つかりません。デフォルト認証にフォールバックします。")
            return storage.Client()


class ImageSearcher:
    """
    指定された企業のベクトルデータ（JSON）を読み込み、画像検索を実行するクラス。
    インスタンスは企業（UUID）ごとに生成されることを想定しています。
    """
    def __init__(self, uuid: str, embeddings_dir: str = 'vector_data', bucket_name: Optional[str] = None):
        self.uuid = uuid
        self.embeddings_dir = embeddings_dir
        self.bucket_name = bucket_name
        self.embeddings_data: List[Dict] = []
        self.embeddings_matrix: Optional[np.ndarray] = None
        
        # ヘルパー関数経由でクライアントを初期化
        self.storage_client = _get_storage_client()

        print(f"🔍 ImageSearcher initialized for UUID: {uuid}")
        self.load_data()

    def load_data(self):
        """
        UUIDに対応するベクトルデータをGCSまたはローカルから読み込む。
        """
        filename = f"{self.uuid}.json"
        print(f"🔄 検索データ '{filename}' を読み込んでいます...")

        try:
            content = None
            if self.bucket_name:
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob(filename)
                if blob.exists():
                    content = blob.download_as_text()
                    print(f"☁️ GCSから '{filename}' をダウンロードしました。")
                else:
                    raise FileNotFoundError(f"GCSバケット '{self.bucket_name}' に '{filename}' が見つかりません。")
            else:
                local_path = os.path.join(self.embeddings_dir, filename)
                if os.path.exists(local_path):
                    with open(local_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    print(f"📄 ローカルから '{local_path}' を読み込みました。")
                else:
                    raise FileNotFoundError(f"ローカルディレクトリ '{self.embeddings_dir}' に '{filename}' が見つかりません。")
            
            self.embeddings_data = json.loads(content)
            
            embeddings = [item['embedding'] for item in self.embeddings_data]
            self.embeddings_matrix = np.array(embeddings, dtype=np.float32)
            print(f"✅ データ読み込み完了。{len(self.embeddings_data)}件のベクトルをロードしました。")

        except FileNotFoundError as e:
            print(f"❌ {e}")
            raise e
        except Exception as e:
            print(f"❌ データの読み込みまたは解析中にエラーが発生しました: {e}")
            traceback.print_exc()
            raise RuntimeError(f"Failed to load or parse data for UUID {self.uuid}") from e
            
    def search_images(self, query_embedding: np.ndarray, top_k: int) -> List[Dict]:
        """類似画像検索を実行"""
        print(f"🔍 Performing similarity search for top_k={top_k}")
        
        if self.embeddings_matrix is None or len(self.embeddings_matrix) == 0:
            print("⚠️ No embeddings data available for search")
            return []

        # コサイン類似度計算
        similarities = np.dot(self.embeddings_matrix, query_embedding) / \
                       (np.linalg.norm(self.embeddings_matrix, axis=1) * np.linalg.norm(query_embedding))
        
        # 上位k件を取得
        top_k_indices = np.argsort(similarities)[::-1][:top_k]
        
        results = []
        for i in top_k_indices:
            result = {
                "filename": self.embeddings_data[i].get("filename"),
                "filepath": self.embeddings_data[i].get("filepath"),
                "similarity": float(similarities[i])
            }
            results.append(result)
        
        print(f"✅ Found {len(results)} similar images")
        if results:
            print(f"   Top similarity: {results[0]['similarity']:.4f}")
            
        return results

    def random_image_search(self, count: int) -> List[Dict]:
        """ランダム画像検索を実行"""
        print(f"🎲 Performing random search for count={count}")
        
        if not self.embeddings_data:
            print("⚠️ No embeddings data available for random search")
            return []
        
        num_to_sample = min(count, len(self.embeddings_data))
        random_indices = np.random.choice(len(self.embeddings_data), num_to_sample, replace=False)
        
        results = []
        for i in random_indices:
            result = {
                "filename": self.embeddings_data[i].get("filename"),
                "filepath": self.embeddings_data[i].get("filepath"),
                "similarity": None  # ランダム検索では類似度なし
            }
            results.append(result)
        
        print(f"✅ Selected {len(results)} random images")
        return results

import os
import json
import traceback
from typing import List, Dict, Optional

import numpy as np
from google.cloud import storage

class ImageSearcher:
    """
    指定された企業のベクトルデータ（JSON）を読み込み、画像検索を実行するクラス。
    インスタンスは企業（UUID）ごとに生成されることを想定しています。
    """
    def __init__(self, uuid: str, embeddings_dir: str = 'vector_data', bucket_name: Optional[str] = None):
        """
        Args:
            uuid (str): 検索対象の企業のUUID。
            embeddings_dir (str): ローカル環境でのベクトルファイルの保存ディレクトリ。
            bucket_name (Optional[str]): GCSを使用する場合のバケット名。
        """
        self.uuid = uuid
        self.embeddings_dir = embeddings_dir
        self.bucket_name = bucket_name
        self.embeddings_data: List[Dict] = []
        self.embeddings_matrix: Optional[np.ndarray] = None

        # 初期化時にデータをロード
        self.load_data()

    def load_data(self):
        """
        UUIDに対応するベクトルデータをGCSまたはローカルから読み込む。
        """
        filename = f"{self.uuid}.json"
        print(f"🔄 検索データ '{filename}' を読み込んでいます...")

        try:
            content = None
            # GCSバケット名が指定されていればGCSから読み込む
            if self.bucket_name:
                client = storage.Client()
                bucket = client.bucket(self.bucket_name)
                blob = bucket.blob(filename)
                if blob.exists():
                    content = blob.download_as_text()
                    print(f"☁️ GCSから '{filename}' をダウンロードしました。")
                else:
                    raise FileNotFoundError(f"GCSバケット '{self.bucket_name}' に '{filename}' が見つかりません。")
            # ローカルから読み込む
            else:
                local_path = os.path.join(self.embeddings_dir, filename)
                if os.path.exists(local_path):
                    with open(local_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    print(f"📄 ローカルから '{local_path}' を読み込みました。")
                else:
                    raise FileNotFoundError(f"ローカルディレクトリ '{self.embeddings_dir}' に '{filename}' が見つかりません。")
            
            self.embeddings_data = json.loads(content)
            
            # 検索用にベクトルデータをNumpy配列に変換
            embeddings = [item['embedding'] for item in self.embeddings_data]
            self.embeddings_matrix = np.array(embeddings, dtype=np.float32)
            print(f"✅ データ読み込み完了。{len(self.embeddings_data)}件のベクトルをロードしました。")

        except FileNotFoundError as e:
            print(f"❌ {e}")
            # エラーを再送出してAPI側でハンドリングできるようにする
            raise e
        except Exception as e:
            print(f"❌ データの読み込みまたは解析中にエラーが発生しました: {e}")
            traceback.print_exc()
            raise RuntimeError(f"Failed to load or parse data for UUID {self.uuid}") from e
            
    def search_images(self, query_embedding: np.ndarray, top_k: int) -> List[Dict]:
        """
        クエリベクトルと最も類似度の高い画像を検索する。
        """
        if self.embeddings_matrix is None or len(self.embeddings_matrix) == 0:
            return []

        # コサイン類似度を計算
        similarities = np.dot(self.embeddings_matrix, query_embedding) / \
                       (np.linalg.norm(self.embeddings_matrix, axis=1) * np.linalg.norm(query_embedding))
        
        # 類似度が高い順にインデックスを取得
        top_k_indices = np.argsort(similarities)[::-1][:top_k]
        
        results = []
        for i in top_k_indices:
            result = {
                "filename": self.embeddings_data[i].get("filename"),
                "filepath": self.embeddings_data[i].get("filepath"),
                "similarity": float(similarities[i])
            }
            results.append(result)
            
        return results

    def random_image_search(self, count: int) -> List[Dict]:
        """
        ランダムに画像を抽出する。
        """
        if not self.embeddings_data:
            return []
        
        # 取得件数がデータ数より多い場合はデータ数に丸める
        num_to_sample = min(count, len(self.embeddings_data))
        
        # ランダムにインデックスをサンプリング
        random_indices = np.random.choice(len(self.embeddings_data), num_to_sample, replace=False)
        
        results = []
        for i in random_indices:
            result = {
                "filename": self.embeddings_data[i].get("filename"),
                "filepath": self.embeddings_data[i].get("filepath"),
                "similarity": None  # ランダム検索なので類似度はない
            }
            results.append(result)
            
        return results

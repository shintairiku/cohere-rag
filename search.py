"""
Google Cloud Storageからベクトルデータを読み込み、類似検索やランダム検索を行うモジュール。
"""

import os
import json
import traceback
from typing import List, Dict, Optional

import numpy as np
from google.cloud import storage


class StorageClient:
    """環境に応じてGoogle Cloud Storageクライアントを初期化するラッパー。"""
    
    def __init__(self):
        self._client = self._get_storage_client()
    
    def _get_storage_client(self) -> storage.Client:
        """
        実行環境に応じた認証方法でGCSクライアントを生成する。
        
        戻り値:
            storage.Client: 初期化済みクライアント
        """
        environment = os.getenv("ENVIRONMENT", "local")
        # ローカル開発時のみこの鍵ファイルを使用し、本番ではCloud Runのサービスアカウントを利用する。
        key_file = "marketing-automation-461305-2acf4965e0b0.json"

        if environment == "production":
            print("🌐 Production environment: Initializing GCS client with default credentials.")
            return storage.Client()
        else:
            print(f"🏠 Local environment: Looking for '{key_file}'...")
            if os.path.exists(key_file):
                print(f"   ✅ Using key file '{key_file}'.")
                return storage.Client.from_service_account_json(key_file)
            else:
                print(f"   ⚠️ Key file not found. Falling back to default credentials.")
                return storage.Client()
    
    @property
    def client(self) -> storage.Client:
        """生成済みのStorageクライアントを返す。"""
        return self._client


class ImageSearcher:
    """
    企業ごとのベクトルデータを読み込み、検索処理を提供するクラス。
    """
    
    def __init__(self, uuid: str, bucket_name: Optional[str] = None, model_name: Optional[str] = None):
        """
        指定したUUID向けに検索を行うインスタンスを初期化する。
        
        引数:
            uuid: 企業のUUID
            bucket_name: ベクトルファイルを格納しているGCSバケット
            model_name: 参照するモデル識別子（オプション）
            
        例外:
            ValueError: bucket_nameが指定されていない場合
            FileNotFoundError: 対応するベクトルファイルが存在しない場合
        """
        if not bucket_name:
            raise ValueError("GCS bucket name is not provided.")
            
        self.uuid = uuid
        self.bucket_name = bucket_name
        self.model_name = (model_name or "").strip().lower() or None
        self.embeddings_data: List[Dict] = []
        self.embeddings_matrix: Optional[np.ndarray] = None
        self.storage_client = StorageClient()
        self._loaded_blob_path: Optional[str] = None
        self.total_entries_count: int = 0
        self.corrupt_entries_count: int = 0
        self.invalid_entries_count: int = 0
        
        self._load_data()

    def _candidate_blob_paths(self) -> List[str]:
        """
        現状の運用ではUUIDごとに単一ファイル（{uuid}.json）のみを期待する。
        将来的にモデル別パスに拡張する場合はここで分岐を追加する。
        """
        return [f"{self.uuid}.json"]

    def _load_data(self) -> None:
        """
        GCS上のJSONベクトルファイルを読み込んでメモリに保持する。
        
        例外:
            FileNotFoundError: ベクトルファイルが存在しない場合
            Exception: 読み込みまたはパースに失敗した場合
        """
        bucket = self.storage_client.client.bucket(self.bucket_name)
        blob = None
        file_path = None

        candidates = self._candidate_blob_paths()
        if self.model_name:
            print(f"   🔎 Requested model hint: {self.model_name}")

        for candidate in candidates:
            candidate_blob = bucket.blob(candidate)
            if candidate_blob.exists():
                blob = candidate_blob
                file_path = candidate
                break

        if blob is None or file_path is None:
            attempted = ", ".join(candidates)
            print(f"❌ ERROR: Vector file not found for UUID '{self.uuid}'. Tried: {attempted}")
            raise FileNotFoundError(f"Vector data for UUID '{self.uuid}' not found.")

        self._loaded_blob_path = file_path
        print(f"🔍 Loading vector data for UUID '{self.uuid}' from gs://{self.bucket_name}/{file_path}")
        print(f"   📁 Vector source: {file_path}")

        try:
            json_data = blob.download_as_string()
            raw_data = json.loads(json_data)

            if not isinstance(raw_data, list):
                raise ValueError("Vector file format is invalid. Expected a list of entries.")

            self.total_entries_count = len(raw_data)
            self.corrupt_entries_count = 0
            self.invalid_entries_count = 0

            filtered_items: List[Dict] = []
            embeddings_list: List[List[float]] = []

            for item in raw_data:
                if item.get("is_corrupt"):
                    self.corrupt_entries_count += 1
                    continue
                embedding = item.get("embedding")
                if not embedding:
                    self.invalid_entries_count += 1
                    continue
                filtered_items.append(item)
                embeddings_list.append(embedding)

            self.embeddings_data = filtered_items

            if embeddings_list:
                # Create a NumPy matrix from the embeddings for efficient calculation
                self.embeddings_matrix = np.array(embeddings_list, dtype=np.float32)
                print(f"✅ Successfully loaded and processed {len(self.embeddings_data)} vectors.")
            else:
                self.embeddings_matrix = np.array([], dtype=np.float32)
                print("⚠️  Warning: No valid embeddings available after filtering.")

            if self.corrupt_entries_count:
                print(f"   ⚠️ Skipped {self.corrupt_entries_count} entries marked as corrupt.")
            if self.invalid_entries_count:
                print(f"   ⚠️ Skipped {self.invalid_entries_count} entries without embeddings.")
            if self.total_entries_count and not self.corrupt_entries_count and not self.invalid_entries_count:
                print(f"   ℹ️  Total entries loaded: {self.total_entries_count}")

        except FileNotFoundError:
            raise
        except Exception as e:
            print(f"❌ Failed to load or parse data for UUID {self.uuid}: {e}")
            traceback.print_exc()
            raise Exception(f"Failed to load vector data for UUID {self.uuid}") from e
            
    def search_images(self, query_embedding: np.ndarray, top_k: int, exclude_files: Optional[List[str]] = None, top_n_pool: int = 25) -> List[Dict]:
        """
        コサイン類似度で上位候補を取得し、その中からランダム抽出でtop_k件を返す。
        
        引数:
            query_embedding: 検索クエリのベクトル
            top_k: 返却件数
            exclude_files: 候補から除外するファイル名リスト
            top_n_pool: ランダム抽出の母数となる上位候補数
            
        戻り値:
            類似度スコア付きの結果辞書リスト
        """
        top_n_pool = top_k
        if self.embeddings_matrix is None or len(self.embeddings_matrix) == 0:
            print("⚠️ No embeddings data available for search")
            return []

        print(f"🔍 Performing similarity search with random selection (pool={top_n_pool}, select={top_k})")
        
        # Convert exclude_files to a set for faster lookup
        exclude_set = set(exclude_files) if exclude_files else set()
        
        try:
            # Filter embeddings data to exclude specified files BEFORE similarity calculation
            valid_indices = []
            excluded_count = 0
            
            for i, item in enumerate(self.embeddings_data):
                filename = item.get("filename")
                if filename in exclude_set:
                    excluded_count += 1
                    print(f"   Excluding from search candidates: {filename}")
                else:
                    valid_indices.append(i)
            
            if not valid_indices:
                print("⚠️ No search candidates available after applying exclusion list")
                return []
            
            print(f"   Search candidates: {len(valid_indices)} (excluded {excluded_count} files)")
            
            # Create filtered embeddings matrix from valid candidates only
            filtered_embeddings = self.embeddings_matrix[valid_indices]
            
            # Calculate cosine similarity only for valid candidates
            similarities = np.dot(filtered_embeddings, query_embedding) / (
                np.linalg.norm(filtered_embeddings, axis=1) * np.linalg.norm(query_embedding)
            )
            
            # Get top-n indices sorted by similarity (descending) for the pool
            pool_size = min(top_n_pool, len(similarities))
            top_pool_indices = np.argsort(similarities)[::-1][:pool_size]
            
            # Randomly select top_k items from the pool
            num_results = min(top_k, len(top_pool_indices))
            selected_pool_indices = np.random.choice(len(top_pool_indices), num_results, replace=False)
            selected_indices = top_pool_indices[selected_pool_indices]
            
            results = []
            for idx in selected_indices:
                # Map back to original embeddings_data index
                original_idx = valid_indices[idx]
                result = {
                    "filename": self.embeddings_data[original_idx].get("filename"),
                    "filepath": self.embeddings_data[original_idx].get("filepath"),
                    "similarity": float(similarities[idx])
                }
                results.append(result)
            
            # Sort results by similarity for better output readability
            results.sort(key=lambda x: x['similarity'], reverse=True)
            
            print(f"✅ Randomly selected {len(results)} images from top {pool_size} similar candidates")
            if results:
                print(f"   Similarity range: {results[0]['similarity']:.4f} ~ {results[-1]['similarity']:.4f}")
                
            return results
            
        except Exception as e:
            print(f"❌ Error during similarity search: {e}")
            traceback.print_exc()
            return []

    def random_image_search(self, count: int, exclude_files: Optional[List[str]] = None) -> List[Dict]:
        """
        ベクトルデータからランダムに画像を選択する。
        
        引数:
            count: 返却する件数
            exclude_files: 除外するファイル名リスト
            
        戻り値:
            ランダムに抽出した結果辞書リスト
        """
        if not self.embeddings_data:
            print("⚠️ No embeddings data available for random search")
            return []
        
        print(f"🎲 Performing random search for count={count}")
        
        # Convert exclude_files to a set for faster lookup
        exclude_set = set(exclude_files) if exclude_files else set()
        if exclude_set:
            print(f"   Excluding {len(exclude_set)} files from random selection")
        
        try:
            # Filter out excluded files first
            valid_indices = []
            for i, item in enumerate(self.embeddings_data):
                filename = item.get("filename")
                if filename not in exclude_set:
                    valid_indices.append(i)
                else:
                    print(f"   Excluding from pool: {filename}")
            
            if not valid_indices:
                print("⚠️ No images available after applying exclusion list")
                return []
            
            # Sample from valid indices only
            num_to_sample = min(count, len(valid_indices))
            selected_indices = np.random.choice(valid_indices, num_to_sample, replace=False)
            
            results = []
            for i in selected_indices:
                result = {
                    "filename": self.embeddings_data[i].get("filename"),
                    "filepath": self.embeddings_data[i].get("filepath"),
                    "similarity": None  # No similarity score for random search
                }
                results.append(result)
            
            excluded_count = len(self.embeddings_data) - len(valid_indices)
            print(f"✅ Selected {len(results)} random images from {len(valid_indices)} available (excluded {excluded_count})")
            return results
            
        except Exception as e:
            print(f"❌ Error during random search: {e}")
            traceback.print_exc()
            return []

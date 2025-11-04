"""
Image Search Module

This module provides functionality for loading vector data from Google Cloud Storage
and performing similarity search and random search on image embeddings.
"""

import os
import json
import traceback
from typing import List, Dict, Optional

import numpy as np
from google.cloud import storage


class StorageClient:
    """Wrapper for Google Cloud Storage client with environment-based initialization."""
    
    def __init__(self):
        self._client = self._get_storage_client()
    
    def _get_storage_client(self) -> storage.Client:
        """
        Initializes a GCS client based on the environment.
        
        Returns:
            storage.Client: Configured GCS client
        """
        environment = os.getenv("ENVIRONMENT", "local")
        # NOTE: This key file is only used for local development.
        # In production on Cloud Run, it uses the attached service account.
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
        """Get the underlying storage client."""
        return self._client


class ImageSearcher:
    """
    A class to load vector data for a specific company (by UUID) and perform searches.
    Instances are expected to be created for each company.
    """
    
    def __init__(self, uuid: str, bucket_name: Optional[str] = None, model_name: Optional[str] = None):
        """
        Initializes the searcher for a given UUID.
        
        Args:
            uuid: The UUID of the company
            bucket_name: The GCS bucket name where vector data is stored
            model_name: Optional model identifier used to narrow down vector files
            
        Raises:
            ValueError: If bucket_name is not provided
            FileNotFoundError: If vector data file is not found
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
        
        self._load_data()

    def _candidate_blob_paths(self) -> List[str]:
        """
        現状の運用ではUUIDごとに単一ファイル（{uuid}.json）のみを期待する。
        将来的にモデル別パスに拡張する場合はここで分岐を追加する。
        """
        return [f"{self.uuid}.json"]

    def _load_data(self) -> None:
        """
        Loads the vector data from a JSON file in GCS.
        
        Raises:
            FileNotFoundError: If the vector data file is not found
            Exception: If there's an error loading or parsing the data
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
            self.embeddings_data = json.loads(json_data)
            
            if self.embeddings_data:
                # Create a NumPy matrix from the embeddings for efficient calculation
                embeddings_list = [item['embedding'] for item in self.embeddings_data]
                self.embeddings_matrix = np.array(embeddings_list, dtype=np.float32)
                print(f"✅ Successfully loaded and processed {len(self.embeddings_data)} vectors.")
            else:
                print("⚠️  Warning: The vector file is empty.")

        except FileNotFoundError:
            raise
        except Exception as e:
            print(f"❌ Failed to load or parse data for UUID {self.uuid}: {e}")
            traceback.print_exc()
            raise Exception(f"Failed to load vector data for UUID {self.uuid}") from e
            
    def search_images(self, query_embedding: np.ndarray, top_k: int, exclude_files: Optional[List[str]] = None, top_n_pool: int = 25) -> List[Dict]:
        top_n_pool = top_k
        """
        Performs a similarity search using cosine similarity, then randomly selects from top N results.
        
        Args:
            query_embedding: The vector of the search query
            top_k: The number of results to return (randomly selected from top_n_pool)
            exclude_files: Optional list of filenames to exclude from search candidates
            top_n_pool: Number of top similar images to select from randomly (default: 50)
            
        Returns:
            List of dictionaries containing search results with similarity scores
        """
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
        Performs a random search.
        
        Args:
            count: The number of random items to return
            exclude_files: Optional list of filenames to exclude from search candidates
            
        Returns:
            List of dictionaries containing randomly selected results
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

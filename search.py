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
    
    def __init__(self, uuid: str, bucket_name: Optional[str] = None):
        """
        Initializes the searcher for a given UUID.
        
        Args:
            uuid: The UUID of the company
            bucket_name: The GCS bucket name where vector data is stored
            
        Raises:
            ValueError: If bucket_name is not provided
            FileNotFoundError: If vector data file is not found
        """
        if not bucket_name:
            raise ValueError("GCS bucket name is not provided.")
            
        self.uuid = uuid
        self.bucket_name = bucket_name
        self.embeddings_data: List[Dict] = []
        self.embeddings_matrix: Optional[np.ndarray] = None
        self.storage_client = StorageClient()
        
        self._load_data()

    def _load_data(self) -> None:
        """
        Loads the vector data from a JSON file in GCS.
        
        Raises:
            FileNotFoundError: If the vector data file is not found
            Exception: If there's an error loading or parsing the data
        """
        file_path = f"{self.uuid}.json"
        print(f"🔍 Loading vector data for UUID '{self.uuid}' from gs://{self.bucket_name}/{file_path}")

        try:
            bucket = self.storage_client.client.bucket(self.bucket_name)
            blob = bucket.blob(file_path)

            if not blob.exists():
                print(f"❌ ERROR: Vector file not found at gs://{self.bucket_name}/{file_path}")
                raise FileNotFoundError(f"Vector data for UUID '{self.uuid}' not found.")

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
            
    def search_images(self, query_embedding: np.ndarray, top_k: int) -> List[Dict]:
        """
        Performs a similarity search using cosine similarity.
        
        Args:
            query_embedding: The vector of the search query
            top_k: The number of top results to return
            
        Returns:
            List of dictionaries containing search results with similarity scores
        """
        if self.embeddings_matrix is None or len(self.embeddings_matrix) == 0:
            print("⚠️ No embeddings data available for search")
            return []

        print(f"🔍 Performing similarity search for top_k={top_k}")
        
        try:
            # Calculate cosine similarity
            similarities = np.dot(self.embeddings_matrix, query_embedding) / (
                np.linalg.norm(self.embeddings_matrix, axis=1) * np.linalg.norm(query_embedding)
            )
            
            # Get the indices of the top-k most similar items
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
            
        except Exception as e:
            print(f"❌ Error during similarity search: {e}")
            traceback.print_exc()
            return []

    def random_image_search(self, count: int) -> List[Dict]:
        """
        Performs a random search.
        
        Args:
            count: The number of random items to return
            
        Returns:
            List of dictionaries containing randomly selected results
        """
        if not self.embeddings_data:
            print("⚠️ No embeddings data available for random search")
            return []
        
        print(f"🎲 Performing random search for count={count}")
        
        try:
            num_to_sample = min(count, len(self.embeddings_data))
            random_indices = np.random.choice(len(self.embeddings_data), num_to_sample, replace=False)
            
            results = []
            for i in random_indices:
                result = {
                    "filename": self.embeddings_data[i].get("filename"),
                    "filepath": self.embeddings_data[i].get("filepath"),
                    "similarity": None  # No similarity score for random search
                }
                results.append(result)
            
            print(f"✅ Selected {len(results)} random images")
            return results
            
        except Exception as e:
            print(f"❌ Error during random search: {e}")
            traceback.print_exc()
            return []
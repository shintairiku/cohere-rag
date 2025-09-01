import os
import json
import traceback
from typing import List, Dict, Optional

import numpy as np
from google.cloud import storage


def _get_storage_client():
    """
    Initializes a GCS client based on the environment.
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


class ImageSearcher:
    """
    A class to load vector data for a specific company (by UUID) and perform searches.
    Instances are expected to be created for each company.
    """
    def __init__(self, uuid: str, bucket_name: Optional[str] = None):
        """
        Initializes the searcher for a given UUID.
        
        Args:
            uuid: The UUID of the company.
            bucket_name: The GCS bucket name where vector data is stored.
        """
        self.uuid = uuid
        self.bucket_name = bucket_name
        self.embeddings_data: List[Dict] = []
        self.embeddings_matrix: Optional[np.ndarray] = None
        
        self._load_data()

    def _load_data(self):
        """
        Loads the vector data from a JSON file in GCS.
        """
        if not self.bucket_name:
            raise ValueError("GCS bucket name is not provided.")
        
        file_path = f"{self.uuid}.json"
        print(f"🔍 Loading vector data for UUID '{self.uuid}' from gs://{self.bucket_name}/{file_path}")

        try:
            storage_client = _get_storage_client()
            bucket = storage_client.bucket(self.bucket_name)
            blob = bucket.blob(file_path)

            if not blob.exists():
                print(f"❌ ERROR: Vector file not found at gs://{self.bucket_name}/{file_path}")
                raise FileNotFoundError(f"Vector data for UUID '{self.uuid}' not found.")

            json_data = blob.download_as_string()
            self.embeddings_data = json.loads(json_data)
            
            if self.embeddings_data:
                # Create a NumPy matrix from the embeddings for efficient calculation
                self.embeddings_matrix = np.array([item['embedding'] for item in self.embeddings_data])
                print(f"✅ Successfully loaded and processed {len(self.embeddings_data)} vectors.")
            else:
                print("⚠️  Warning: The vector file is empty.")

        except Exception as e:
            print(f"❌ Failed to load or parse data for UUID {self.uuid}")
            traceback.print_exc()
            # Re-raise the exception to be handled by the API layer
            raise e
            
    def search_images(self, query_embedding: np.ndarray, top_k: int) -> List[Dict]:
        """
        Performs a similarity search.
        
        Args:
            query_embedding: The vector of the search query.
            top_k: The number of top results to return.

        Returns:
            A list of dictionaries, each containing result info.
        """
        if self.embeddings_matrix is None or len(self.embeddings_matrix) == 0:
            return []

        # Calculate cosine similarity
        similarities = np.dot(self.embeddings_matrix, query_embedding) / \
                       (np.linalg.norm(self.embeddings_matrix, axis=1) * np.linalg.norm(query_embedding))
        
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
            
        return results

    def random_image_search(self, count: int) -> List[Dict]:
        """
        Performs a random search.
        
        Args:
            count: The number of random items to return.
            
        Returns:
            A list of dictionaries, each containing result info.
        """
        if not self.embeddings_data:
            return []
        
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
        
        return results
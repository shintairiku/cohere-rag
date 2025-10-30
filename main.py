"""
Image Search and Vectorization API

This FastAPI application provides endpoints for:
1. Triggering vectorization jobs for Google Drive images
2. Searching similar images using configured embedding providers
"""

import os
import traceback
from typing import Dict, Optional, List

import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from google.cloud import run_v2

from embedding_providers import get_embedding_provider
from search import ImageSearcher

load_dotenv()


class Config:
    """Configuration management for the application."""
    
    def __init__(self):
        self.gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
        self.gcp_project_id = os.getenv("GCP_PROJECT_ID")
        self.vectorize_job_name = os.getenv("VECTORIZE_JOB_NAME", "cohere-rag-vectorize-job")
        self.gcp_region = os.getenv("GCP_REGION", "asia-northeast1")
        self.vertex_multimodal_model = os.getenv("VERTEX_MULTIMODAL_MODEL", "multimodalembedding@001")
        self.embedding_provider = os.getenv("EMBEDDING_PROVIDER", "vertex_ai")
        self.cohere_api_key = os.getenv("COHERE_API_KEY", "")
        # Google Sheets ID は環境変数で上書き可能。未指定時は ENVIRONMENT に応じて既定値を選ぶ
        dev_sheets_id = "1xPY1w4q9wm607hNK9Eb0D5v5ub7JFRihx9d-VOpHYOo"
        prod_sheets_id = "1pxSyLLZ-G3U3wwTYNgX_Qzijv7Mzn_6xSRIxGrM9l-4"
        default_sheets_id = prod_sheets_id if os.getenv("ENVIRONMENT") == "production" else dev_sheets_id
        self.google_sheets_id = os.getenv("GOOGLE_SHEETS_ID", default_sheets_id)
        self.company_sheet_name = "会社一覧"
        
        self._validate_required_vars()
    
    def _validate_required_vars(self):
        """Validate that all required environment variables are set."""
        required_vars = [
            ("GCS_BUCKET_NAME", self.gcs_bucket_name),
            ("GCP_PROJECT_ID", self.gcp_project_id)
        ]
        
        missing_vars = [name for name, value in required_vars if not value]
        if missing_vars:
            raise RuntimeError(f"FATAL: Required environment variables are missing: {', '.join(missing_vars)}")


# Initialize configuration and clients
config = Config()
app = FastAPI(
    title="Image Search and Vectorization API",
    version="1.0.0",
    description="API for vectorizing Google Drive images and performing similarity search"
)
run_client = run_v2.JobsClient()

class VectorizeRequest(BaseModel):
    """Request model for vectorization endpoint."""
    uuid: str
    drive_url: str
    use_embed_v4: bool = False


class VectorizeTask(BaseModel):
    """Single vectorization task model."""
    uuid: str
    drive_url: str
    company_name: str = ""
    use_embed_v4: bool = False


class BatchVectorizeRequest(BaseModel):
    """Request model for batch vectorization endpoint."""
    tasks: List[VectorizeTask]


class SearchRequest(BaseModel):
    """Request model for search endpoint."""
    uuid: str
    q: Optional[str] = None
    top_k: int = 5
    trigger: str = "スタンダード"
    exclude_files: List[str] = []
    use_embed_v4: bool = False
    top_n: Optional[int] = None


class JobService:
    """Service for managing Cloud Run Jobs."""
    
    def __init__(self, config: Config, run_client: run_v2.JobsClient):
        self.config = config
        self.run_client = run_client
    
    def _build_job_env(self, additional: List[Dict[str, str]]) -> List[Dict[str, str]]:
        env_vars = list(additional)
        env_vars.extend([
            {"name": "GCS_BUCKET_NAME", "value": self.config.gcs_bucket_name},
            {"name": "GCP_PROJECT_ID", "value": self.config.gcp_project_id},
            {"name": "GCP_REGION", "value": self.config.gcp_region},
            {"name": "VERTEX_MULTIMODAL_MODEL", "value": self.config.vertex_multimodal_model},
            {"name": "EMBEDDING_PROVIDER", "value": self.config.embedding_provider},
        ])
        if self.config.cohere_api_key:
            env_vars.append({"name": "COHERE_API_KEY", "value": self.config.cohere_api_key})
        return env_vars
    
    def trigger_vectorization_job(self, uuid: str, drive_url: str, use_embed_v4: bool = False) -> Dict:
        """
        Trigger a Cloud Run Job for single UUID vectorization.
        
        Args:
            uuid: Company UUID
            drive_url: Google Drive folder URL
            use_embed_v4: Whether to use embed-v4.0 model
            
        Returns:
            Dict with job execution information
            
        Raises:
            Exception: If job execution fails
        """
        print(f"API: Received request to start vectorization job for UUID: {uuid}")
        
        job_parent = f"projects/{self.config.gcp_project_id}/locations/{self.config.gcp_region}"
        job_name = f"{job_parent}/jobs/{self.config.vectorize_job_name}"
        
        try:
            print(f"  -> Attempting to run job: {job_name}")
            
            request_object = run_v2.RunJobRequest(
                name=job_name,
                overrides=run_v2.RunJobRequest.Overrides(
                    container_overrides=[
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            env=self._build_job_env(
                                additional=[
                                    {"name": "UUID", "value": uuid},
                                    {"name": "DRIVE_URL", "value": drive_url},
                                    {"name": "USE_EMBED_V4", "value": str(use_embed_v4)},
                                ]
                            )
                        )
                    ]
                )
            )
            
            response = self.run_client.run_job(request=request_object)
            
            # Extract execution info from response
            if hasattr(response, 'name'):
                execution_info = response.name
            elif hasattr(response, 'metadata'):
                execution_info = str(response.metadata)
            else:
                execution_info = f"Job triggered for {uuid}"
            
            print(f"  -> Job execution started. Info: {execution_info}")
            return {
                "message": f"Vectorization job started successfully for UUID: {uuid}",
                "execution_info": execution_info,
                "job_name": self.config.vectorize_job_name
            }
            
        except Exception as e:
            error_msg = f"Failed to start Cloud Run Job: {str(e)}"
            print(f"  -> ERROR: {error_msg}")
            traceback.print_exc()
            raise Exception(error_msg)

    def trigger_batch_vectorization_job(self, tasks: List[VectorizeTask]) -> Dict:
        """
        Trigger a Cloud Run Job for batch vectorization of multiple UUIDs.
        
        Args:
            tasks: List of vectorization tasks
            
        Returns:
            Dict with job execution information
            
        Raises:
            Exception: If job execution fails
        """
        print(f"API: Received request to start batch vectorization job for {len(tasks)} tasks")
        
        job_parent = f"projects/{self.config.gcp_project_id}/locations/{self.config.gcp_region}"
        job_name = f"{job_parent}/jobs/{self.config.vectorize_job_name}"
        
        try:
            print(f"  -> Attempting to run batch job: {job_name}")
            
            # Serialize tasks to JSON for passing as environment variable
            import json
            tasks_json = json.dumps([task.dict() for task in tasks])
            
            request_object = run_v2.RunJobRequest(
                name=job_name,
                overrides=run_v2.RunJobRequest.Overrides(
                    container_overrides=[
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            env=self._build_job_env(
                                additional=[
                                    {"name": "BATCH_MODE", "value": "true"},
                                    {"name": "BATCH_TASKS", "value": tasks_json},
                                ]
                            )
                        )
                    ]
                )
            )
            
            response = self.run_client.run_job(request=request_object)
            
            # Extract execution info from response
            if hasattr(response, 'name'):
                execution_info = response.name
            elif hasattr(response, 'metadata'):
                execution_info = str(response.metadata)
            else:
                execution_info = f"Batch job triggered for {len(tasks)} tasks"
            
            print(f"  -> Batch job execution started. Info: {execution_info}")
            return {
                "message": f"Batch vectorization job started successfully for {len(tasks)} tasks",
                "execution_info": execution_info,
                "job_name": self.config.vectorize_job_name,
                "task_count": len(tasks)
            }
            
        except Exception as e:
            error_msg = f"Failed to start batch Cloud Run Job: {str(e)}"
            print(f"  -> ERROR: {error_msg}")
            traceback.print_exc()
            raise Exception(error_msg)


# Initialize services
job_service = JobService(config, run_client)


@app.post("/vectorize", status_code=202)
async def trigger_vectorization_job(request: VectorizeRequest):
    """Triggers a Cloud Run Job to perform vectorization."""
    try:
        result = job_service.trigger_vectorization_job(request.uuid, request.drive_url, request.use_embed_v4)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/vectorize-batch", status_code=202)
async def trigger_batch_vectorization_job(request: BatchVectorizeRequest):
    """Triggers a Cloud Run Job to perform batch vectorization."""
    try:
        result = job_service.trigger_batch_vectorization_job(request.tasks)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SearchService:
    """Service for managing image search operations."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def _embed_query(self, query: str, use_embed_v4: bool):
        provider = get_embedding_provider()
        return provider.embed_text(text=query, use_embed_v4=use_embed_v4)
    
    def search_ranked(self, uuid: str, query: str, top_k: int, exclude_files: List[str] = None, use_embed_v4: bool = False) -> Dict:
        """Return the top_k results ordered by similarity score."""
        print(f"🧠 [STANDARD] Generating embedding for query: '{query}'")
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from ranked search")
        
        try:
            searcher = ImageSearcher(uuid=uuid, bucket_name=self.config.gcs_bucket_name)
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        query_embedding = self._embed_query(query, use_embed_v4)
        results = searcher.search_images(query_embedding=query_embedding, top_k=top_k, exclude_files=exclude_files)
        print(f"✅ Standard search completed. Returning {len(results)} results")
        
        return {"query": query, "results": results}
    
    def search_shuffle(
        self,
        uuid: str,
        query: str,
        top_k: int,
        top_n: Optional[int] = None,
        exclude_files: List[str] = None,
        use_embed_v4: bool = False
    ) -> Dict:
        """Return top_k results sampled from the ranked top_n pool."""
        print(f"🧠 [SHUFFLE] Generating embedding for query: '{query}'")
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from shuffle search")
        
        try:
            searcher = ImageSearcher(uuid=uuid, bucket_name=self.config.gcs_bucket_name)
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        query_embedding = self._embed_query(query, use_embed_v4)
        pool_size = max(top_k * 3, 20) if top_n is None else max(top_n, top_k)
        pool = searcher.search_images(query_embedding=query_embedding, top_k=pool_size, exclude_files=exclude_files)
        
        if len(pool) <= top_k:
            chosen = pool
        else:
            import random
            indices = random.sample(range(len(pool)), k=top_k)
            indices.sort()
            chosen = [pool[i] for i in indices]
        
        print(f"✅ Shuffle search completed. Returning {len(chosen)} results from pool size {len(pool)}")
        return {"query": query, "results": chosen}
    
    def search_random_images(self, uuid: str, count: int, exclude_files: List[str] = None) -> Dict:
        """
        Search for random images.
        
        Args:
            uuid: Company UUID
            count: Number of random images to return
            exclude_files: List of filenames to exclude from search results
            
        Returns:
            Dict with search results
        """
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from random search")
            
        try:
            searcher = ImageSearcher(uuid=uuid, bucket_name=self.config.gcs_bucket_name)
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        results = searcher.random_image_search(count=count, exclude_files=exclude_files)
        print(f"✅ Random search completed. Returning {len(results)} results")
        
        return {"query": "ランダム検索", "results": results}


# Initialize services
search_service = SearchService(config)


class SheetsService:
    """Service for managing Google Sheets operations."""
    
    def __init__(self, config: Config):
        self.config = config
        self._gc = self._get_sheets_client()
    
    def _get_sheets_client(self) -> gspread.Client:
        """Initialize Google Sheets client with appropriate credentials."""
        environment = os.getenv("ENVIRONMENT", "local")
        
        if environment == "production":
            import google.auth
            credentials, _ = google.auth.default(scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly'
            ])
            return gspread.authorize(credentials)
        else:
            key_file = "config/marketing-automation-461305-2acf4965e0b0.json"
            if os.path.exists(key_file):
                credentials = service_account.Credentials.from_service_account_file(
                    key_file,
                    scopes=[
                        'https://www.googleapis.com/auth/spreadsheets.readonly',
                        'https://www.googleapis.com/auth/drive.readonly'
                    ]
                )
                return gspread.authorize(credentials)
            else:
                import google.auth
                credentials, _ = google.auth.default(scopes=[
                    'https://www.googleapis.com/auth/spreadsheets.readonly',
                    'https://www.googleapis.com/auth/drive.readonly'
                ])
                return gspread.authorize(credentials)
    
    def get_companies_for_auto_update(self) -> List[Dict]:
        """
        Fetch companies that have both URL and checkbox=TRUE from Google Sheets.
        
        Returns:
            List of dictionaries with company information
        """
        try:
            spreadsheet = self._gc.open_by_key(self.config.google_sheets_id)
            sheet = spreadsheet.worksheet(self.config.company_sheet_name)
            
            # Get all values from the sheet
            all_values = sheet.get_all_values()
            
            if len(all_values) < 2:  # No data rows
                print("No data found in the company sheet")
                return []
            
            data_rows = all_values[1:]
            
            companies_to_update = []
            
            for row_index, row in enumerate(data_rows, start=2):  # Start from row 2 (skip header)
                try:
                    # Assuming columns: A=UUID, B=Company Name, C=Drive URL, F=Checkbox
                    if len(row) < 6:
                        continue
                    
                    uuid = row[0].strip() if len(row) > 0 else ""
                    company_name = row[1].strip() if len(row) > 1 else ""
                    drive_url = row[2].strip() if len(row) > 2 else ""
                    checkbox_status = row[5].strip().upper() if len(row) > 5 else ""
                    
                    # Check if URL exists and checkbox is TRUE
                    if drive_url and checkbox_status == "TRUE":
                        companies_to_update.append({
                            "uuid": uuid,
                            "company_name": company_name,
                            "drive_url": drive_url,
                            "row_number": row_index,
                            "use_embed_v4": "embed-v4.0" in company_name
                        })
                        print(f"Found company for auto-update: {company_name} (UUID: {uuid})")
                
                except Exception as e:
                    print(f"Error processing row {row_index}: {e}")
                    continue
            
            print(f"Total companies found for auto-update: {len(companies_to_update)}")
            return companies_to_update
            
        except Exception as e:
            print(f"Error fetching companies from Google Sheets: {e}")
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Failed to fetch companies from Google Sheets: {str(e)}")


# Initialize sheets service
sheets_service = SheetsService(config)


@app.post("/auto-update")
async def auto_update_vectors():
    """
    Endpoint for automatic vector file updates.
    Fetches companies with checkbox=TRUE from Google Sheets and triggers vectorization.
    """
    try:
        print("🔄 Starting automatic vector update process...")
        
        # Get companies that need to be updated
        companies = sheets_service.get_companies_for_auto_update()
        
        if not companies:
            return {
                "message": "No companies found with enabled auto-update",
                "processed_count": 0,
                "results": []
            }
        
        results = []
        success_count = 0
        failure_count = 0
        
        # バッチジョブとして実行
        try:
            print(f"🎯 Triggering batch vectorization for {len(companies)} companies")
            
            # タスクリストを作成
            tasks = []
            for company in companies:
                task = VectorizeTask(
                    uuid=company['uuid'],
                    drive_url=company['drive_url'],
                    company_name=company['company_name'],
                    use_embed_v4=company['use_embed_v4']
                )
                tasks.append(task)
            
            # バッチジョブを実行
            batch_result = job_service.trigger_batch_vectorization_job(tasks)
            
            results.append({
                "status": "success",
                "message": batch_result['message'],
                "task_count": batch_result.get('task_count', len(companies)),
                "execution_info": batch_result.get('execution_info', '')
            })
            success_count = len(companies)
            
        except Exception as e:
            error_msg = f"Failed to trigger batch vectorization: {str(e)}"
            print(f"❌ {error_msg}")
            
            results.append({
                "status": "error",
                "message": error_msg
            })
            failure_count = len(companies)
        
        print(f"✅ Auto-update process completed. Success: {success_count}, Failures: {failure_count}")
        
        return {
            "message": f"Auto-update process completed. {success_count} successful, {failure_count} failed.",
            "processed_count": len(companies),
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results
        }
        
    except Exception as e:
        print(f"❌ Error in auto-update process: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Auto-update process failed: {str(e)}")


@app.get("/search", response_model=Dict)
def search_images_api(
    uuid: str = Query(..., description="UUID of the company to search for"),
    q: Optional[str] = Query(None, description="Search query text"),
    top_k: int = Query(5, ge=1, le=50, description="Number of results to return"),
    trigger: str = Query("スタンダード", description="Search type: 'スタンダード' | 'シャッフル' | 'ランダム' (互換: '類似画像検索'→シャッフル)"),
    top_n: Optional[int] = Query(None, ge=1, le=200, description="Candidate pool size for shuffle mode"),
):
    """Performs image search using the specified vector data."""
    print(f"🔍 Search API called: UUID={uuid}, trigger={trigger}, top_k={top_k}")
    if q:
        print(f"   Query: '{q}'")
    
    normalized_trigger = "シャッフル" if trigger == "類似画像検索" else trigger
    
    try:
        if normalized_trigger == "スタンダード":
            if not q:
                print("❌ Missing query parameter for standard search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for standard search.")
            
            return search_service.search_ranked(uuid, q, top_k)
            
        elif normalized_trigger == "シャッフル":
            if not q:
                print("❌ Missing query parameter for shuffle search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for shuffle search.")
            
            return search_service.search_shuffle(uuid, q, top_k, top_n=top_n)
            
        elif normalized_trigger == "ランダム":
            return search_service.search_random_images(uuid, top_k)
            
        else:
            print(f"❌ Invalid trigger: {normalized_trigger}")
            raise HTTPException(status_code=400, detail=f"Invalid trigger: {normalized_trigger}")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unexpected error during search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during search: {str(e)}")


@app.post("/search", response_model=List[Dict])
def search_images_post(request: SearchRequest):
    """
    Performs image search using the specified vector data (POST version).
    Returns results as a list (compatible with api_caller.gs).
    """
    print(f"🔍 Search API (POST) called: UUID={request.uuid}, trigger={request.trigger}, top_k={request.top_k}")
    if request.q:
        print(f"   Query: '{request.q}'")
    if request.exclude_files:
        print(f"   Excluding {len(request.exclude_files)} files")
    
    normalized = "シャッフル" if request.trigger == "類似画像検索" else request.trigger
    
    try:
        if normalized == "スタンダード":
            if not request.q:
                print("❌ Missing query parameter for standard search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for standard search.")
            
            result = search_service.search_ranked(
                request.uuid,
                request.q,
                request.top_k,
                request.exclude_files,
                request.use_embed_v4
            )
            return result.get("results", [])
            
        elif normalized == "シャッフル":
            if not request.q:
                print("❌ Missing query parameter for shuffle search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for shuffle search.")
            
            result = search_service.search_shuffle(
                request.uuid,
                request.q,
                request.top_k,
                request.top_n,
                request.exclude_files,
                request.use_embed_v4
            )
            return result.get("results", [])
            
        elif normalized == "ランダム":
            result = search_service.search_random_images(
                request.uuid, 
                request.top_k,
                request.exclude_files
            )
            return result.get("results", [])
            
        else:
            print(f"❌ Invalid trigger: {normalized}")
            raise HTTPException(status_code=400, detail=f"Invalid trigger: {normalized}")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unexpected error during search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during search: {str(e)}")


@app.get("/")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "image-search-api", "version": "1.0.0"}

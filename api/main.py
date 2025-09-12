"""
Image Search and Vectorization API

This FastAPI application provides endpoints for:
1. Triggering vectorization jobs for Google Drive images
2. Searching similar images using Cohere embeddings
"""

import os
import traceback
from typing import Dict, Optional, List

import cohere
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from google.cloud import run_v2

from .search import ImageSearcher

load_dotenv()


class Config:
    """Configuration management for the application."""
    
    def __init__(self):
        self.gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
        self.gcp_project_id = os.getenv("GCP_PROJECT_ID")
        self.cohere_api_key = os.getenv("COHERE_API_KEY")
        self.vectorize_job_name = os.getenv("VECTORIZE_JOB_NAME", "cohere-rag-vectorize-job")
        self.gcp_region = os.getenv("GCP_REGION", "asia-northeast1")
        
        self._validate_required_vars()
    
    def _validate_required_vars(self):
        """Validate that all required environment variables are set."""
        required_vars = [
            ("GCS_BUCKET_NAME", self.gcs_bucket_name),
            ("GCP_PROJECT_ID", self.gcp_project_id),
            ("COHERE_API_KEY", self.cohere_api_key)
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
cohere_client = cohere.Client(config.cohere_api_key)
run_client = run_v2.JobsClient()

class VectorizeRequest(BaseModel):
    """Request model for vectorization endpoint."""
    uuid: str
    drive_url: str


class SearchRequest(BaseModel):
    """Request model for search endpoint."""
    uuid: str
    q: Optional[str] = None
    top_k: int = 5
    trigger: str = "類似画像検索"
    exclude_files: List[str] = []


class IncrementalUpdateRequest(BaseModel):
    """Request model for incremental update endpoint."""
    uuid: str
    drive_url: str


class BatchIncrementalUpdateRequest(BaseModel):
    """Request model for batch incremental update endpoint."""
    spreadsheet_id: Optional[str] = None
    max_workers: int = 3


class SchedulerRequest(BaseModel):
    """Request model for manual scheduler execution."""
    mode: str = "update"  # "update" or "health"
    dry_run: bool = False


class JobService:
    """Service for managing Cloud Run Jobs."""
    
    def __init__(self, config: Config, run_client: run_v2.JobsClient):
        self.config = config
        self.run_client = run_client
    
    def trigger_vectorization_job(self, uuid: str, drive_url: str) -> Dict:
        """
        Trigger a Cloud Run Job for vectorization.
        
        Args:
            uuid: Company UUID
            drive_url: Google Drive folder URL
            
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
                            env=[
                                {"name": "UUID", "value": uuid},
                                {"name": "DRIVE_URL", "value": drive_url}
                            ]
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


# Initialize services
job_service = JobService(config, run_client)


@app.post("/vectorize", status_code=202)
async def trigger_vectorization_job(request: VectorizeRequest):
    """Triggers a Cloud Run Job to perform vectorization."""
    try:
        result = job_service.trigger_vectorization_job(request.uuid, request.drive_url)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class SearchService:
    """Service for managing image search operations."""
    
    def __init__(self, config: Config, cohere_client: cohere.Client):
        self.config = config
        self.cohere_client = cohere_client
    
    def search_similar_images(self, uuid: str, query: str, top_k: int, exclude_files: List[str] = None) -> Dict:
        """
        Search for similar images using text query.
        
        Args:
            uuid: Company UUID
            query: Search query text
            top_k: Number of results to return
            exclude_files: List of filenames to exclude from search results
            
        Returns:
            Dict with search results
        """
        print(f"🧠 Generating embedding for query: '{query}'")
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from search")
        
        try:
            searcher = ImageSearcher(uuid=uuid, bucket_name=self.config.gcs_bucket_name)
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        response = self.cohere_client.embed(
            texts=[query], 
            model="embed-multilingual-v3.0", 
            input_type="search_query"
        )
        query_embedding = response.embeddings[0]
        
        results = searcher.search_images(query_embedding=query_embedding, top_k=top_k, exclude_files=exclude_files)
        print(f"✅ Similarity search completed. Returning {len(results)} results")
        
        return {"query": query, "results": results}
    
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
search_service = SearchService(config, cohere_client)


@app.get("/search", response_model=Dict)
def search_images_api(
    uuid: str = Query(..., description="UUID of the company to search for"),
    q: Optional[str] = Query(None, description="Search query text"),
    top_k: int = Query(5, ge=1, le=50, description="Number of results to return"),
    trigger: str = Query("類似画像検索", description="Search type: '類似画像検索' or 'ランダム画像検索'"),
):
    """Performs image search using the specified vector data."""
    print(f"🔍 Search API called: UUID={uuid}, trigger={trigger}, top_k={top_k}")
    if q:
        print(f"   Query: '{q}'")
    
    try:
        if trigger == "類似画像検索":
            if not q:
                print("❌ Missing query parameter for similarity search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for similar image search.")
            
            return search_service.search_similar_images(uuid, q, top_k)
            
        elif trigger == "ランダム画像検索":
            return search_service.search_random_images(uuid, top_k)
            
        else:
            print(f"❌ Invalid trigger: {trigger}")
            raise HTTPException(status_code=400, detail=f"Invalid trigger: {trigger}")
            
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
    
    try:
        if request.trigger == "類似画像検索":
            if not request.q:
                print("❌ Missing query parameter for similarity search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for similar image search.")
            
            result = search_service.search_similar_images(
                request.uuid, 
                request.q, 
                request.top_k,
                request.exclude_files
            )
            return result.get("results", [])
            
        elif request.trigger == "ランダム画像検索":
            result = search_service.search_random_images(
                request.uuid, 
                request.top_k,
                request.exclude_files
            )
            return result.get("results", [])
            
        else:
            print(f"❌ Invalid trigger: {request.trigger}")
            raise HTTPException(status_code=400, detail=f"Invalid trigger: {request.trigger}")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unexpected error during search: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during search: {str(e)}")


@app.post("/incremental-update", status_code=202)
async def trigger_incremental_update(request: IncrementalUpdateRequest):
    """Triggers an incremental update for a single company."""
    try:
        from vectorization.incremental_updater import IncrementalEmbeddingUpdater
        
        print(f"🔄 Starting incremental update for UUID: {request.uuid}")
        
        updater = IncrementalEmbeddingUpdater(config.gcs_bucket_name)
        stats = updater.update_company_embeddings(request.uuid, request.drive_url)
        
        return {
            "message": f"Incremental update completed for UUID: {request.uuid}",
            "stats": {
                "added": stats.added,
                "updated": stats.updated,
                "removed": stats.removed,
                "errors": stats.errors,
                "duration_seconds": stats.duration_seconds()
            }
        }
    except Exception as e:
        print(f"❌ Incremental update failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/batch-incremental-update", status_code=202)
async def trigger_batch_incremental_update(request: BatchIncrementalUpdateRequest):
    """Triggers a batch incremental update for all companies."""
    try:
        from scheduler.batch_incremental_updater import BatchIncrementalUpdater
        
        print(f"🚀 Starting batch incremental update")
        
        updater = BatchIncrementalUpdater(config.gcs_bucket_name, max_workers=request.max_workers)
        
        # Get companies list
        companies = []
        if request.spreadsheet_id:
            companies = updater.get_companies_from_sheets(request.spreadsheet_id)
        else:
            # Use default spreadsheet ID from environment
            default_spreadsheet_id = os.getenv("COMPANY_SPREADSHEET_ID")
            if default_spreadsheet_id:
                companies = updater.get_companies_from_sheets(default_spreadsheet_id)
            else:
                raise HTTPException(status_code=400, detail="No spreadsheet ID provided or configured")
        
        if not companies:
            raise HTTPException(status_code=404, detail="No companies found to process")
        
        # Run batch update (this may take a while)
        results = updater.run_batch_update(companies)
        
        # Save results to storage
        updater.save_results_to_storage(results)
        
        return {
            "message": f"Batch incremental update completed for {results.total_companies} companies",
            "results": {
                "total_companies": results.total_companies,
                "successful_updates": results.successful_updates,
                "failed_updates": results.failed_updates,
                "total_files_added": results.total_files_added,
                "total_files_updated": results.total_files_updated,
                "total_files_removed": results.total_files_removed,
                "total_errors": results.total_errors,
                "duration_seconds": results.duration_seconds
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Batch incremental update failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/trigger-scheduler", status_code=202)
async def trigger_scheduler(request: SchedulerRequest):
    """Manually trigger scheduler execution for debugging."""
    try:
        from scheduler.scheduler import ScheduledUpdater
        
        print(f"🎯 Manual scheduler trigger - Mode: {request.mode}, Dry run: {request.dry_run}")
        
        scheduler = ScheduledUpdater()
        
        if request.mode == "health":
            health = scheduler.health_check()
            return {
                "message": "Scheduler health check completed",
                "mode": request.mode,
                "health_status": health
            }
        elif request.mode == "update":
            if request.dry_run:
                print("🧪 DRY RUN MODE - no actual updates will be performed")
                health = scheduler.health_check()
                if health["status"] == "healthy":
                    return {
                        "message": "✅ Dry run passed - system is ready for updates",
                        "mode": request.mode,
                        "dry_run": True,
                        "health_status": health
                    }
                else:
                    return {
                        "message": "❌ Dry run failed - system health check failed", 
                        "mode": request.mode,
                        "dry_run": True,
                        "health_status": health
                    }
            else:
                results = scheduler.run_scheduled_update()
                return {
                    "message": f"Scheduler update completed: {results.successful_updates}/{results.total_companies} companies",
                    "mode": request.mode,
                    "dry_run": False,
                    "results": {
                        "total_companies": results.total_companies,
                        "successful_updates": results.successful_updates,
                        "failed_updates": results.failed_updates,
                        "duration_seconds": results.duration_seconds,
                        "files_added": results.total_files_added,
                        "files_updated": results.total_files_updated,
                        "files_removed": results.total_files_removed
                    }
                }
        else:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {request.mode}. Use 'update' or 'health'")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Manual scheduler trigger failed: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    """Health check endpoint with system status."""
    try:
        from scheduler.scheduler import ScheduledUpdater
        scheduler = ScheduledUpdater()
        health = scheduler.health_check()
        return health
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/")
def root():
    """Root endpoint."""
    return {"status": "ok", "service": "image-search-api", "version": "1.1.0"}

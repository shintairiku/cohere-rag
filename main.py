"""
Image Search and Vectorization API

This FastAPI application provides endpoints for:
1. Triggering vectorization jobs for Google Drive images
2. Searching similar images using Cohere embeddings
"""

import os
import traceback
import re
from typing import Dict, Optional, List

import cohere
import gspread
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from google.cloud import run_v2
import google.auth

from search import ImageSearcher

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


class SheetsService:
    """Service for Google Sheets operations."""
    
    def __init__(self):
        # Google認証情報を取得
        self.credentials, _ = google.auth.default()
        self.gc = gspread.authorize(self.credentials)
    
    def extract_sheet_id_and_gid(self, url: str) -> tuple:
        """スプレッドシートURLからIDとGIDを抽出"""
        # Extract sheet ID
        sheet_id_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', url)
        if not sheet_id_match:
            raise ValueError("Invalid Google Sheets URL")
        sheet_id = sheet_id_match.group(1)
        
        # Extract GID (worksheet ID)
        gid_match = re.search(r'gid=([0-9]+)', url)
        gid = gid_match.group(1) if gid_match else '0'
        
        return sheet_id, gid
    
    def get_priority_companies(self, sheet_url: str) -> List[CompanyInfo]:
        """
        スプレッドシートから優先企業のリストを取得
        
        Args:
            sheet_url: Google SheetsのURL
            
        Returns:
            List[CompanyInfo]: 優先企業のリスト
        """
        try:
            sheet_id, gid = self.extract_sheet_id_and_gid(sheet_url)
            
            # スプレッドシートを開く
            spreadsheet = self.gc.open_by_key(sheet_id)
            
            # GIDから特定のワークシートを取得
            for worksheet in spreadsheet.worksheets():
                if str(worksheet.id) == gid:
                    target_worksheet = worksheet
                    break
            else:
                # GIDが見つからない場合は最初のワークシートを使用
                target_worksheet = spreadsheet.sheet1
                print(f"⚠️ Worksheet with GID {gid} not found, using first worksheet")
            
            print(f"📊 Reading data from worksheet: {target_worksheet.title}")
            
            # 全データを取得（ヘッダー行から）
            all_values = target_worksheet.get_all_values()
            
            if len(all_values) < 2:  # ヘッダー + 最低1行のデータが必要
                print("⚠️ No data rows found in spreadsheet")
                return []
            
            companies = []
            
            # データ行を処理（1行目はヘッダーなので2行目から）
            for i, row in enumerate(all_values[1:], start=2):
                try:
                    # 行の長さをチェック
                    if len(row) < 6:  # A,B,C,D,E,F列まで必要
                        continue
                    
                    # A列: 企業名, C列: Drive URL, F列: 優先フラグ
                    company_name = row[0].strip() if len(row) > 0 else ""
                    drive_url = row[2].strip() if len(row) > 2 else ""  # C列
                    priority_flag = row[5].strip().upper() if len(row) > 5 else ""  # F列
                    
                    # 優先フラグをチェック（TRUE, YES, 1, ✓などを許可）
                    is_priority = priority_flag in ['TRUE', 'YES', '1', '✓', 'ON']
                    
                    # 優先企業かつ必要なデータがある場合のみ追加
                    if is_priority and company_name and drive_url:
                        # UUIDを生成（企業名から）
                        uuid = self._generate_uuid_from_name(company_name)
                        
                        companies.append(CompanyInfo(
                            uuid=uuid,
                            drive_url=drive_url,
                            name=company_name
                        ))
                        
                        print(f"   ✅ Added priority company: {company_name} (UUID: {uuid[:8]}...)")
                    
                except Exception as e:
                    print(f"   ⚠️ Error processing row {i}: {e}")
                    continue
            
            print(f"📋 Found {len(companies)} priority companies")
            return companies
            
        except Exception as e:
            print(f"❌ Error reading from Google Sheets: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to read from Google Sheets: {str(e)}")
    
    def _generate_uuid_from_name(self, company_name: str) -> str:
        """企業名からUUIDを生成（一意性を保つため）"""
        import hashlib
        # 企業名をハッシュ化してUUID風の文字列を生成
        hash_object = hashlib.md5(company_name.encode('utf-8'))
        hex_dig = hash_object.hexdigest()
        # UUID形式に変換 (8-4-4-4-12)
        return f"{hex_dig[:8]}-{hex_dig[8:12]}-{hex_dig[12:16]}-{hex_dig[16:20]}-{hex_dig[20:32]}"


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
            # model="embed-v4.0",
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
sheets_service = SheetsService()


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


@app.get("/")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "image-search-api", "version": "1.0.0"}


class CompanyInfo(BaseModel):
    """Model for company information."""
    uuid: str
    drive_url: str
    name: Optional[str] = None


class BatchVectorizeRequest(BaseModel):
    """Request model for batch vectorization endpoint."""
    companies: Optional[List[CompanyInfo]] = None


class BatchVectorizeResponse(BaseModel):
    """Response model for batch vectorization endpoint."""
    status: str
    timestamp: str
    total: int
    succeeded: int
    failed: int
    results: List[Dict]
    errors: List[Dict]


@app.post("/batch-vectorize", response_model=BatchVectorizeResponse)
async def batch_vectorize(request: BatchVectorizeRequest):
    """
    Batch vectorization endpoint that processes multiple companies sequentially.
    Designed to be called by Cloud Scheduler.
    
    If no companies list is provided in request, automatically fetches from Google Sheets.
    
    Args:
        request: Optional list of companies to vectorize
        
    Returns:
        BatchVectorizeResponse with processing results
    """
    from datetime import datetime
    import asyncio
    
    print(f"📅 Batch vectorization triggered at {datetime.now().isoformat()}")
    
    # Determine companies list
    if request.companies:
        companies = request.companies
        print(f"📊 Using provided companies list: {len(companies)} companies")
    else:
        # Fetch from Google Sheets
        sheet_url = "https://docs.google.com/spreadsheets/d/1DEGQefuNWfivae9VfyNLjhrhVaSy9JwWWdI7Gx3M26s/edit?gid=1884184352#gid=1884184352"
        print(f"📊 Fetching priority companies from Google Sheets...")
        try:
            companies = sheets_service.get_priority_companies(sheet_url)
        except Exception as e:
            print(f"❌ Failed to fetch companies from Google Sheets: {e}")
            return BatchVectorizeResponse(
                status="error",
                timestamp=datetime.now().isoformat(),
                total=0,
                succeeded=0,
                failed=0,
                results=[],
                errors=[{"error": f"Failed to fetch companies: {str(e)}"}]
            )
    
    if not companies:
        print("⚠️ No companies to process")
        return BatchVectorizeResponse(
            status="success",
            timestamp=datetime.now().isoformat(),
            total=0,
            succeeded=0,
            failed=0,
            results=[],
            errors=[]
        )
    
    print(f"🏗️ Processing {len(companies)} companies")
    
    job_service = JobService(config, run_client)
    results = []
    errors = []
    
    for i, company in enumerate(companies, 1):
        company_name = company.name or f"Company-{company.uuid[:8]}"
        print(f"🏢 [{i}/{len(companies)}] Processing {company_name} (UUID: {company.uuid[:8]}...)")
        
        try:
            # Trigger vectorization job
            result = job_service.trigger_vectorization_job(company.uuid, company.drive_url)
            results.append({
                "company": company_name,
                "uuid": company.uuid,
                "status": "triggered",
                "execution": result.get("execution_name")
            })
            print(f"   ✅ Successfully triggered job for {company_name}")
            
            # Wait 3 seconds between jobs to avoid overloading (except for last company)
            if i < len(companies):
                await asyncio.sleep(3)
                
        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Failed to trigger job for {company_name}: {error_msg}")
            errors.append({
                "company": company_name,
                "uuid": company.uuid,
                "error": error_msg
            })
    
    succeeded_count = len(results)
    failed_count = len(errors)
    
    print(f"✨ Batch processing completed: {succeeded_count} succeeded, {failed_count} failed")
    
    return BatchVectorizeResponse(
        status="completed",
        timestamp=datetime.now().isoformat(),
        total=len(companies),
        succeeded=succeeded_count,
        failed=failed_count,
        results=results,
        errors=errors
    )

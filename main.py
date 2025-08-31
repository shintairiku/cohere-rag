import os
import traceback
from typing import Dict, Optional

import cohere
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from google.cloud import run_v2
from google.api_core.exceptions import GoogleAPICallError

from search import ImageSearcher

load_dotenv()

# --- Environment Variable Validation ---
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
VECTORIZE_JOB_NAME = os.getenv("VECTORIZE_JOB_NAME", "cohere-rag-vectorize-job")
GCP_REGION = os.getenv("GCP_REGION", "asia-northeast1")

if not all([GCS_BUCKET_NAME, GCP_PROJECT_ID, COHERE_API_KEY]):
    raise RuntimeError("FATAL: Required environment variables are missing.")

# --- Global Clients ---
app = FastAPI(
    title="Image Search and Vectorization API (Job-based)",
    version="4.0.0"
)
co = cohere.Client(COHERE_API_KEY)
run_client = run_v2.JobsClient()

# --- API Endpoints ---
class VectorizeRequest(BaseModel):
    uuid: str
    drive_url: str

@app.post("/vectorize", status_code=202)
async def trigger_vectorization_job(request: VectorizeRequest):
    """Triggers a Cloud Run Job to perform vectorization."""
    print(f"API: Received request to start vectorization job for UUID: {request.uuid}")

    job_parent = f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}"
    job_name = f"{job_parent}/jobs/{VECTORIZE_JOB_NAME}"

    try:
        operation = run_client.run_job(
            name=job_name,
            overrides=run_v2.RunJobRequest.Overrides(
                container_overrides=[
                    run_v2.RunJobRequest.Overrides.ContainerOverride(
                        env=[
                            {"name": "UUID", "value": request.uuid},
                            {"name": "DRIVE_URL", "value": request.drive_url},
                        ]
                    )
                ]
            ),
        )
        print(f"  -> Successfully started job execution for '{VECTORIZE_JOB_NAME}'. Waiting for response...")
        response = operation.result()
        print(f"  -> Job execution created: {response.name}")
        return {"message": "Vectorization job started successfully.", "execution_name": response.name}

    except GoogleAPICallError as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to start Cloud Run Job: {e}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")


@app.get("/search", response_model=Dict)
def search_images_api(
    uuid: str = Query(..., description="UUID of the company to search for"),
    q: Optional[str] = Query(None, description="Search query text"),
    top_k: int = Query(5, ge=1, le=50),
    trigger: str = Query("類似画像検索"),
):
    """Performs image search using the specified vector data."""
    try:
        searcher = ImageSearcher(uuid=uuid, bucket_name=GCS_BUCKET_NAME)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
    
    # ... (Search logic remains the same)
    try:
        if trigger == "類似画像検索":
            if not q:
                raise HTTPException(status_code=400, detail="Query 'q' is required for similar image search.")
            response = co.embed(texts=[q], model="embed-multilingual-v3.0", input_type="search_query")
            query_embedding = response.embeddings[0]
            results = searcher.search_images(query_embedding=query_embedding, top_k=top_k)
            return {"query": q, "results": results}
        elif trigger == "ランダム画像検索":
            results = searcher.random_image_search(count=top_k)
            return {"query": "ランダム検索", "results": results}
        else:
            raise HTTPException(status_code=400, detail=f"Invalid trigger: {trigger}")
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during search: {str(e)}")


@app.get("/")
def health_check():
    return {"status": "ok", "service": "main"}

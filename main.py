"""
画像検索とベクトル化を提供するFastAPIアプリケーション。

主な機能:
1. Google Drive上の画像ベクトル化ジョブの実行
2. 埋め込みプロバイダを利用した類似画像検索
"""

import html
import os
import traceback
from typing import Dict, Optional, List, Any

import gspread
from google.oauth2 import service_account
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel
from google.cloud import run_v2

from embedding_providers import get_embedding_provider
from search import ImageSearcher
from drive_watch import DriveWatchManager, DriveNotificationProcessor

try:
    from google.cloud import translate_v2 as translate
except ImportError:  # pragma: no cover
    translate = None

load_dotenv()


class Config:
    """アプリケーション設定を読み込んで管理するクラス。"""
    
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
        self.drive_watch_callback_url = os.getenv("DRIVE_WEBHOOK_URL")
        ttl_value = os.getenv("DRIVE_WATCH_TTL_SECONDS", "").strip()
        self.drive_watch_ttl_seconds = int(ttl_value or "86400")
        cooldown_value = os.getenv("DRIVE_WATCH_COOLDOWN_SECONDS", "").strip()
        cooldown_seconds = int(cooldown_value or "60")
        self.drive_watch_cooldown_seconds = cooldown_seconds if cooldown_seconds >= 0 else 0
        verbose_flag = os.getenv("DRIVE_WATCH_VERBOSE_LOGS", "true").strip().lower()
        self.drive_watch_verbose_logs = verbose_flag not in {"false", "0", "no"}
        
        self._validate_required_vars()
    
    def _validate_required_vars(self):
        """必須の環境変数が揃っているか検証する。"""
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
    """ベクトル化エンドポイントで利用するリクエストモデル。"""
    uuid: str
    drive_url: str
    use_embed_v4: bool = False


class VectorizeTask(BaseModel):
    """バッチ処理用の単一ベクトル化タスク定義。"""
    uuid: str
    drive_url: str
    company_name: str = ""
    use_embed_v4: bool = False


class BatchVectorizeRequest(BaseModel):
    """バッチベクトル化エンドポイントのリクエストモデル。"""
    tasks: List[VectorizeTask]


class SearchRequest(BaseModel):
    """検索エンドポイントのリクエストモデル。"""
    uuid: str
    q: Optional[str] = None
    top_k: int = 5
    trigger: str = "スタンダード"
    exclude_files: List[str] = []
    use_embed_v4: bool = False
    top_n: Optional[int] = None
    search_model: Optional[str] = None


class DriveWatchRequest(BaseModel):
    """Google Driveの変更監視チャネル作成用リクエストモデル。"""
    uuid: str
    drive_url: str
    company_name: str = ""
    callback_url: Optional[str] = None
    use_embed_v4: bool = False


class CompanyState(BaseModel):
    """スプレッドシートから送信される企業設定。"""
    uuid: str
    drive_url: str
    company_name: str = ""
    use_embed_v4: bool = False


class CompanyStateBatchRequest(BaseModel):
    """企業設定をまとめて保存するリクエスト。"""
    companies: List[CompanyState]


class DeleteCompanyStateResponse(BaseModel):
    """企業設定削除のレスポンス。"""
    uuid: str
    removed_watch: bool


class ReRegisterRequest(BaseModel):
    """チャネルの再登録リクエスト。"""
    uuids: Optional[List[str]] = None


class JobService:
    """Cloud Runジョブの実行を管理するサービスクラス。"""
    
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
        単一UUID向けのCloud Runジョブを起動してベクトル化を実行する。
        
        引数:
            uuid: 企業のUUID
            drive_url: 画像を格納したGoogle DriveフォルダのURL
            use_embed_v4: embed-v4.0モデルを強制するかどうか
            
        戻り値:
            ジョブ実行情報を含む辞書
            
        例外:
            Exception: ジョブ起動に失敗した場合
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
        複数UUIDをまとめて処理するCloud Runジョブを起動する。
        
        引数:
            tasks: ベクトル化タスクのリスト
            
        戻り値:
            ジョブ実行情報を含む辞書
            
        例外:
            Exception: ジョブ起動に失敗した場合
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


def get_drive_watch_manager() -> DriveWatchManager:
    """アプリ全体で共有するDrive監視マネージャを返す。"""
    manager = getattr(app.state, "drive_watch_manager", None)
    if manager is None:
        manager = DriveWatchManager(
            bucket_name=config.gcs_bucket_name,
            default_callback_url=config.drive_watch_callback_url,
            ttl_seconds=config.drive_watch_ttl_seconds
        )
        app.state.drive_watch_manager = manager
    return manager


def get_drive_notification_processor() -> DriveNotificationProcessor:
    """Drive通知の処理器を初期化して返す。"""
    processor = getattr(app.state, "drive_notification_processor", None)
    if processor is None:
        processor = DriveNotificationProcessor(
            bucket_name=config.gcs_bucket_name,
            job_service=job_service,
            cooldown_seconds=config.drive_watch_cooldown_seconds,
            verbose_logging=config.drive_watch_verbose_logs,
        )
        app.state.drive_notification_processor = processor
    return processor


@app.post("/vectorize", status_code=202)
async def trigger_vectorization_job(request: VectorizeRequest):
    """指定されたUUIDのベクトル化ジョブをCloud Runで開始する。"""
    try:
        result = job_service.trigger_vectorization_job(request.uuid, request.drive_url, request.use_embed_v4)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/vectorize-batch", status_code=202)
async def trigger_batch_vectorization_job(request: BatchVectorizeRequest):
    """複数UUID向けのベクトル化バッチジョブをCloud Runで開始する。"""
    try:
        result = job_service.trigger_batch_vectorization_job(request.tasks)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/drive/watch")
async def register_drive_watch(request: DriveWatchRequest):
    """Google Driveの変更通知チャネルを登録する。"""
    manager = get_drive_watch_manager()
    try:
        state = manager.create_watch(
            uuid=request.uuid,
            drive_url=request.drive_url,
            company_name=request.company_name,
            callback_url=request.callback_url,
            use_embed_v4=request.use_embed_v4
        )
        return {
            "message": f"Drive watch registered for UUID {request.uuid}",
            "channel_id": state.get("channel_id"),
            "resource_id": state.get("resource_id"),
            "expiration": state.get("expiration"),
            "drive_id": state.get("drive_id"),
            "is_new_channel": state.get("is_new_channel", False),
            "drive_channel_created": state.get("drive_channel_created", False),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to register Drive watch: {exc}")


@app.delete("/drive/watch/{uuid}")
async def delete_drive_watch(uuid: str):
    """登録済みのDrive通知チャネルを停止する。"""
    manager = get_drive_watch_manager()
    state = manager.stop_watch(uuid)
    if not state:
        raise HTTPException(status_code=404, detail=f"No Drive watch found for UUID {uuid}")
    return {
        "message": f"Drive watch removed for UUID {uuid}",
        "channel_id": state.get("channel_id"),
        "resource_id": state.get("resource_id")
    }


@app.post("/drive/company-states")
async def save_company_states(request: CompanyStateBatchRequest):
    """スプレッドシートから送信された企業設定を保存する。"""
    manager = get_drive_watch_manager()
    saved: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for company in request.companies:
        try:
            state = manager.save_company_state_only(
                uuid=company.uuid,
                drive_url=company.drive_url,
                company_name=company.company_name,
                use_embed_v4=company.use_embed_v4,
            )
            saved.append({
                "uuid": company.uuid,
                "drive_id": state.get("drive_id"),
                "folder_id": state.get("folder_id"),
            })
        except Exception as exc:
            errors.append({"uuid": company.uuid, "error": str(exc)})
    if not saved and errors:
        raise HTTPException(status_code=400, detail={"errors": errors})
    return {
        "saved_count": len(saved),
        "saved": saved,
        "error_count": len(errors),
        "errors": errors,
    }


@app.delete("/drive/company-states/{uuid}", response_model=DeleteCompanyStateResponse)
async def delete_company_state(uuid: str):
    """企業設定と関連する紐づけを削除する。"""
    manager = get_drive_watch_manager()
    state = manager.stop_watch(uuid)
    if not state:
        raise HTTPException(status_code=404, detail=f"No company state found for UUID {uuid}")
    return DeleteCompanyStateResponse(uuid=uuid, removed_watch=state.get("drive_channel_stopped", False))


@app.post("/drive/watch/re-register")
async def re_register_drive_channels(request: Optional[ReRegisterRequest] = None):
    """既存企業のチャネルを共有ドライブ単位で再登録する。"""
    manager = get_drive_watch_manager()
    payload = request or ReRegisterRequest()
    try:
        result = manager.re_register_companies(payload.uuids)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to re-register Drive channels: {exc}")


@app.post("/drive/notifications", status_code=204)
async def drive_notifications(request: Request):
    """Google Drive APIからのpush通知を受信し、必要に応じてジョブを再実行する。"""
    channel_id = request.headers.get("x-goog-channel-id")
    resource_state = request.headers.get("x-goog-resource-state", "")
    resource_id = request.headers.get("x-goog-resource-id", "")
    changed_types = request.headers.get("x-goog-changed", "")
    if not channel_id:
        raise HTTPException(status_code=400, detail="Missing X-Goog-Channel-Id header.")

    processor = get_drive_notification_processor()
    try:
        processor.handle_notification(channel_id, resource_state, resource_id, changed_types)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to handle Drive notification: {exc}")
    return Response(status_code=204)


class SearchService:
    """画像検索処理をまとめたサービスクラス。"""
    
    def __init__(self, config: Config):
        self.config = config
        self._translate_client = self._init_translate_client()

    def _init_translate_client(self):
        if translate is None:
            print("⚠️ google-cloud-translate がインストールされていないため、クエリ翻訳をスキップします。")
            return None
        try:
            return translate.Client()
        except Exception as exc:
            print(f"⚠️ 翻訳クライアントの初期化に失敗しました: {exc}")
            return None

    def _translate_query(self, query: str) -> str:
        if not query:
            return query
        if not self._translate_client:
            return query

        try:
            result = self._translate_client.translate(query, target_language="en")
            translated_text = result.get("translatedText") or ""
            translated_text = html.unescape(translated_text)
            source_lang = result.get("detectedSourceLanguage", "").lower()

            if translated_text:
                if source_lang and source_lang != "en":
                    print(f"🌐 クエリを {source_lang} から英語に翻訳しました: '{translated_text}'")
                else:
                    print("🌐 クエリは英語と判断されたため、そのまま使用します。")
                return translated_text
        except Exception as exc:
            print(f"⚠️ クエリ翻訳に失敗したため原文を使用します: {exc}")

        return query
    
    def _resolve_search_options(
        self,
        search_model: Optional[str],
        use_embed_v4: bool,
    ) -> tuple[str, bool, Optional[str]]:
        """
        要求されたモデル名から埋め込みプロバイダとモデル識別子を決定する。
        (provider_name, use_embed_v4_flag, model_identifier_for_storage) を返す。
        """
        if not search_model:
            default_provider = self.config.embedding_provider
            model_identifier = None
            return default_provider, use_embed_v4, model_identifier

        normalized = search_model.strip().lower()

        if normalized in {"vertex-ai", "vertex_ai", "vertex"}:
            return "vertex_ai", False, "vertex-ai"

        if normalized in {
            "cohere-embed-v4.0",
            "cohere_embed-v4.0",
            "embed-v4.0",
            "embed_v4.0",
        }:
            return "cohere", True, "cohere-embed-v4.0"

        if normalized in {
            "cohere-multilingual-v3.0",
            "cohere_multilingual-v3.0",
            "multilingual-v3.0",
            "multilingual_v3.0",
        }:
            return "cohere", False, "cohere-multilingual-v3.0"

        # 想定外の値はデフォルト設定にフォールバック
        print(f"⚠️ Unknown search_model '{search_model}', falling back to default provider.")
        default_provider = self.config.embedding_provider
        return default_provider, use_embed_v4, None

    def _embed_query(self, query: str, provider_name: str, use_embed_v4: bool):
        provider = get_embedding_provider(provider_name=provider_name)
        return provider.embed_text(text=query, use_embed_v4=use_embed_v4)
    
    def search_ranked(
        self,
        uuid: str,
        query: str,
        top_k: int,
        exclude_files: List[str] = None,
        use_embed_v4: bool = False,
        search_model: Optional[str] = None,
    ) -> Dict:
        """類似度でソートした上位top_k件の結果を返す。"""
        print(f"🧠 [STANDARD] Generating embedding for query: '{query}'")
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from ranked search")

        provider_name, effective_use_embed_v4, model_identifier = self._resolve_search_options(
            search_model,
            use_embed_v4,
        )
        
        try:
            searcher = ImageSearcher(
                uuid=uuid,
                bucket_name=self.config.gcs_bucket_name,
                model_name=model_identifier,
            )
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        english_query = self._translate_query(query)
        query_embedding = self._embed_query(english_query, provider_name, effective_use_embed_v4)
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
        use_embed_v4: bool = False,
        search_model: Optional[str] = None,
    ) -> Dict:
        """上位候補からランダム抽出したtop_k件の結果を返す。"""
        print(f"🧠 [SHUFFLE] Generating embedding for query: '{query}'")
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from shuffle search")

        provider_name, effective_use_embed_v4, model_identifier = self._resolve_search_options(
            search_model,
            use_embed_v4,
        )
        
        try:
            searcher = ImageSearcher(
                uuid=uuid,
                bucket_name=self.config.gcs_bucket_name,
                model_name=model_identifier,
            )
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        english_query = self._translate_query(query)
        query_embedding = self._embed_query(english_query, provider_name, effective_use_embed_v4)
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
    
    def search_random_images(
        self,
        uuid: str,
        count: int,
        exclude_files: List[str] = None,
        search_model: Optional[str] = None,
    ) -> Dict:
        """
        登録済み画像からランダムに結果を返す。
        
        引数:
            uuid: 企業のUUID
            count: 返却したい件数
            exclude_files: 除外するファイル名リスト
            
        戻り値:
            検索結果を含む辞書
        """
        if exclude_files:
            print(f"📋 Excluding {len(exclude_files)} files from random search")

        _, _, model_identifier = self._resolve_search_options(search_model, False)

        try:
            searcher = ImageSearcher(
                uuid=uuid,
                bucket_name=self.config.gcs_bucket_name,
                model_name=model_identifier,
            )
        except FileNotFoundError as e:
            print(f"❌ Vector data not found: {e}")
            raise HTTPException(status_code=404, detail=f"Vector data for UUID '{uuid}' not found.")
        
        results = searcher.random_image_search(count=count, exclude_files=exclude_files)
        print(f"✅ Random search completed. Returning {len(results)} results")
        
        return {"query": "ランダム検索", "results": results}


# Initialize services
search_service = SearchService(config)


class SheetsService:
    """Google Sheets連携を扱うサービスクラス。"""
    
    def __init__(self, config: Config):
        self.config = config
        self._gc = self._get_sheets_client()
    
    def _get_sheets_client(self) -> gspread.Client:
        """環境に応じた認証情報でGoogle Sheetsクライアントを初期化する。"""
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
        Google SheetsからDrive URLありかつチェックボックスONの企業を抽出する。
        
        戻り値:
            企業情報を格納した辞書のリスト
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
    チェックボックスONの企業を自動取得し、バッチベクトル化を実行するエンドポイント。
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
    search_model: Optional[str] = Query(None, description="Search embedding model identifier"),
):
    """指定したUUIDのベクトルデータを使って画像検索を実行する。"""
    print(f"🔍 Search API called: UUID={uuid}, trigger={trigger}, top_k={top_k}")
    if q:
        print(f"   Query: '{q}'")
    
    normalized_trigger = "シャッフル" if trigger == "類似画像検索" else trigger
    
    try:
        if normalized_trigger == "スタンダード":
            if not q:
                print("❌ Missing query parameter for standard search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for standard search.")
            
            return search_service.search_ranked(uuid, q, top_k, search_model=search_model)
            
        elif normalized_trigger == "シャッフル":
            if not q:
                print("❌ Missing query parameter for shuffle search")
                raise HTTPException(status_code=400, detail="Query 'q' is required for shuffle search.")
            
            return search_service.search_shuffle(uuid, q, top_k, top_n=top_n, search_model=search_model)
            
        elif normalized_trigger == "ランダム":
            return search_service.search_random_images(uuid, top_k, search_model=search_model)
            
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
    POSTボディで指定されたパラメータを用いて画像検索を実行し、結果を配列で返す。
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
                request.use_embed_v4,
                request.search_model,
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
                request.use_embed_v4,
                request.search_model,
            )
            return result.get("results", [])
            
        elif normalized == "ランダム":
            result = search_service.search_random_images(
                request.uuid, 
                request.top_k,
                request.exclude_files,
                request.search_model,
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
    """疎通確認用エンドポイント。"""
    return {"status": "ok", "service": "image-search-api", "version": "1.0.0"}

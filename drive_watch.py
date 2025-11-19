"""
Google Driveの変更通知チャネルを管理し、通知に応じてベクトル化ジョブを再実行するための補助モジュール。
"""

import json
import os
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage

from drive_scanner import extract_folder_id

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DEFAULT_KEY_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "marketing-automation-461305-2acf4965e0b0.json")
WATCH_STATE_PREFIX = os.getenv("DRIVE_WATCH_STATE_PREFIX", "drive-watch-states")
DEFAULT_TTL_SECONDS = int(os.getenv("DRIVE_WATCH_TTL_SECONDS", "86400") or "0")


def _build_drive_credentials():
    environment = os.getenv("ENVIRONMENT", "local")
    if environment == "production":
        creds, _ = google.auth.default(scopes=DRIVE_SCOPES)
        return creds
    if DEFAULT_KEY_FILE and os.path.exists(DEFAULT_KEY_FILE):
        return service_account.Credentials.from_service_account_file(DEFAULT_KEY_FILE, scopes=DRIVE_SCOPES)
    creds, _ = google.auth.default(scopes=DRIVE_SCOPES)
    return creds


def _build_storage_client():
    environment = os.getenv("ENVIRONMENT", "local")
    if environment == "production":
        return storage.Client()
    if DEFAULT_KEY_FILE and os.path.exists(DEFAULT_KEY_FILE):
        return storage.Client.from_service_account_json(DEFAULT_KEY_FILE)
    return storage.Client()


class DriveWatchStateStore:
    """GCS上にDrive変更監視チャネルの状態を保存・管理する。"""

    def __init__(self, bucket_name: str, prefix: str = WATCH_STATE_PREFIX):
        if not bucket_name:
            raise ValueError("bucket_name is required to persist watch states.")
        self.bucket_name = bucket_name
        self.client = _build_storage_client()
        self.bucket = self.client.bucket(bucket_name)
        prefix = (prefix or "").strip("/")
        self.prefix = f"{prefix}/" if prefix else ""

    def _blob_path(self, uuid: str) -> str:
        return f"{self.prefix}{uuid}.json"

    def save(self, state: Dict[str, Any]) -> None:
        blob = self.bucket.blob(self._blob_path(state["uuid"]))
        blob.upload_from_string(
            json.dumps(state, ensure_ascii=False, indent=2),
            content_type="application/json"
        )

    def load(self, uuid: str) -> Optional[Dict[str, Any]]:
        blob = self.bucket.blob(self._blob_path(uuid))
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def delete(self, uuid: str) -> None:
        blob = self.bucket.blob(self._blob_path(uuid))
        if blob.exists():
            blob.delete()

    def list_states(self) -> List[Dict[str, Any]]:
        states: List[Dict[str, Any]] = []
        blobs = self.client.list_blobs(self.bucket_name, prefix=self.prefix)
        for blob in blobs:
            try:
                states.append(json.loads(blob.download_as_text()))
            except Exception:
                continue
        return states

    def find_by_channel_id(self, channel_id: str) -> Optional[Dict[str, Any]]:
        for state in self.list_states():
            if state.get("channel_id") == channel_id:
                return state
        return None

    def upsert(self, state: Dict[str, Any]) -> Dict[str, Any]:
        self.save(state)
        return state


class DriveWatchManager:
    """Drive APIの変更通知（Changes.watch）をセットアップ・解除する。"""

    def __init__(self, bucket_name: str, default_callback_url: Optional[str] = None, ttl_seconds: Optional[int] = None):
        if not bucket_name:
            raise ValueError("bucket_name is required to manage Drive watch channels.")
        self.bucket_name = bucket_name
        self.default_callback_url = default_callback_url
        # ttl_seconds: Drive APIの変更通知チャネルの有効期限を設定する。デフォルトは86400秒（24時間）
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else DEFAULT_TTL_SECONDS
        # store: DriveWatchStateStoreのインスタンスを作成する。
        self.store = DriveWatchStateStore(bucket_name)
        # drive_service: Drive APIのクライアントを作成する。
        self.drive_service = build("drive", "v3", credentials=_build_drive_credentials(), cache_discovery=False)

    def _get_start_page_token(self, drive_id: Optional[str]) -> str:
        params: Dict[str, Any] = {"supportsAllDrives": True}
        if drive_id:
            params["driveId"] = drive_id
        response = self.drive_service.changes().getStartPageToken(**params).execute()
        return response["startPageToken"]

    def create_watch(
        self,
        uuid: str,
        drive_url: str,
        company_name: str = "",
        callback_url: Optional[str] = None,
        use_embed_v4: bool = False
    ) -> Dict[str, Any]:
        target_callback = callback_url or self.default_callback_url
        if not target_callback:
            raise ValueError("callback_url is required to register a Drive watch channel.")

        existing = self.store.load(uuid)
        if existing:
            print(f"ℹ️  Existing watch for UUID {uuid} is being replaced.")
            self.stop_watch(uuid)

        folder_id = extract_folder_id(drive_url)
        folder_metadata = self.drive_service.files().get(
            fileId=folder_id,
            fields="id, name, driveId",
            supportsAllDrives=True
        ).execute()
        drive_id = folder_metadata.get("driveId")
        start_page_token = self._get_start_page_token(drive_id)

        channel_id = str(uuid4())
        watch_body: Dict[str, Any] = {
            "id": channel_id,
            "type": "web_hook",
            "address": target_callback,
        }
        params: Dict[str, Any] = {}
        if self.ttl_seconds:
            params["ttl"] = str(self.ttl_seconds)
        if params:
            watch_body["params"] = params

        watch_params: Dict[str, Any] = {
            "pageToken": start_page_token,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "body": watch_body,
        }
        if drive_id:
            watch_params["driveId"] = drive_id

        response = self.drive_service.changes().watch(**watch_params).execute()
        state = {
            "uuid": uuid,
            "drive_url": drive_url,
            "company_name": company_name,
            "folder_id": folder_id,
            "drive_id": drive_id,
            "channel_id": channel_id,
            "resource_id": response.get("resourceId"),
            "expiration": response.get("expiration"),
            "page_token": start_page_token,
            "use_embed_v4": use_embed_v4,
        }
        self.store.save(state)
        print(f"✅ Drive watch created for UUID {uuid} (channel: {channel_id})")
        return state

    def stop_watch(self, uuid: str) -> Optional[Dict[str, Any]]:
        state = self.store.load(uuid)
        if not state:
            return None
        body = {"id": state.get("channel_id"), "resourceId": state.get("resource_id")}
        try:
            self.drive_service.channels().stop(body=body).execute()
            print(f"🛑 Drive watch channel stopped for UUID {uuid}")
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in (404, 410):
                raise
            print(f"⚠️  Channel stop returned {status} for UUID {uuid}, continuing cleanup.")
        self.store.delete(uuid)
        return state


class DriveNotificationProcessor:
    """Drive通知を処理し、必要に応じてベクトル化ジョブを再実行する。"""

    def __init__(self, bucket_name: str, job_service):
        if not bucket_name:
            raise ValueError("bucket_name is required.")
        self.bucket_name = bucket_name
        self.store = DriveWatchStateStore(bucket_name)
        self.job_service = job_service
        self.drive_service = build("drive", "v3", credentials=_build_drive_credentials(), cache_discovery=False)
        self._parent_cache: Dict[str, List[str]] = {}

    def handle_notification(self, channel_id: str, resource_state: str = "", resource_id: str = "") -> Dict[str, Any]:
        state = self.store.find_by_channel_id(channel_id)
        if not state:
            print(f"⚠️  No stored state found for channel {channel_id}. Ignoring notification.")
            return {"handled": False, "reason": "unknown_channel"}
        print(f"📨 Drive notification received for UUID {state['uuid']} (state={resource_state}, resource={resource_id})")

        if resource_state == "sync":
            # 初回の同期リクエストは通知チャネル作成時に必ず送られる
            return {"handled": True, "changes_found": 0, "job_triggered": False, "status": "sync"}

        changes_found = self._consume_change_feed(state)
        job_triggered = False
        if changes_found > 0:
            print(f"🔁 Detected {changes_found} change(s) for UUID {state['uuid']}. Triggering vectorization job.")
            self.job_service.trigger_vectorization_job(
                uuid=state["uuid"],
                drive_url=state["drive_url"],
                use_embed_v4=state.get("use_embed_v4", False)
            )
            job_triggered = True
        else:
            print(f"ℹ️  No relevant changes found for UUID {state['uuid']}.")

        return {"handled": True, "changes_found": changes_found, "job_triggered": job_triggered}

    def _consume_change_feed(self, state: Dict[str, Any]) -> int:
        token = state.get("page_token")
        if not token:
            token = self._get_start_page_token(state.get("drive_id"))

        relevant_changes = 0
        current_token = token
        latest_new_start: Optional[str] = None

        while current_token:
            try:
                response = self._list_changes(current_token, state.get("drive_id"))
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status == 410:
                    print("⚠️  Stored page token expired. Resetting to latest start token.")
                    new_token = self._get_start_page_token(state.get("drive_id"))
                    state["page_token"] = new_token
                    self.store.save(state)
                    return 0
                raise

            filtered = self._filter_relevant_changes(response.get("changes", []), state["folder_id"])
            relevant_changes += len(filtered)
            current_token = response.get("nextPageToken")
            if response.get("newStartPageToken"):
                latest_new_start = response["newStartPageToken"]

            if not current_token:
                break

        if latest_new_start:
            state["page_token"] = latest_new_start
        elif current_token:
            state["page_token"] = current_token
        elif not state.get("page_token"):
            state["page_token"] = token

        self.store.save(state)
        return relevant_changes

    def _list_changes(self, page_token: str, drive_id: Optional[str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "pageToken": page_token,
            "spaces": "drive",
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "fields": "nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,parents,mimeType,trashed))",
        }
        if drive_id:
            params["driveId"] = drive_id
        return self.drive_service.changes().list(**params).execute()

    def _get_start_page_token(self, drive_id: Optional[str]) -> str:
        params: Dict[str, Any] = {"supportsAllDrives": True}
        if drive_id:
            params["driveId"] = drive_id
        response = self.drive_service.changes().getStartPageToken(**params).execute()
        return response["startPageToken"]

    def _filter_relevant_changes(self, changes: List[Dict[str, Any]], folder_id: str) -> List[Dict[str, Any]]:
        relevant: List[Dict[str, Any]] = []
        for change in changes:
            if change.get("removed"):
                relevant.append(change)
                continue
            file_info = change.get("file") or {}
            parents = file_info.get("parents") or []
            for parent_id in parents:
                if parent_id == folder_id or self._is_descendant(parent_id, folder_id):
                    relevant.append(change)
                    break
        return relevant

    def _is_descendant(self, candidate_id: str, ancestor_id: str, visited: Optional[Set[str]] = None) -> bool:
        if candidate_id == ancestor_id:
            return True
        visited = visited or set()
        if candidate_id in visited:
            return False
        visited.add(candidate_id)
        for parent in self._get_parent_ids(candidate_id):
            if parent == ancestor_id:
                return True
            if self._is_descendant(parent, ancestor_id, visited):
                return True
        return False

    def _get_parent_ids(self, file_id: str) -> List[str]:
        if file_id in self._parent_cache:
            return self._parent_cache[file_id]
        try:
            metadata = self.drive_service.files().get(
                fileId=file_id,
                fields="id, parents",
                supportsAllDrives=True
            ).execute()
            parents = metadata.get("parents", [])
        except HttpError:
            parents = []
        self._parent_cache[file_id] = parents
        return parents

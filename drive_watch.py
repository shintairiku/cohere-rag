"""
Google Driveの変更通知チャネルを管理し、通知に応じてベクトル化ジョブを再実行するための補助モジュール。
"""

import json
import os
import time
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
    """GCS上にDrive変更監視チャネルと企業設定の状態を保存・管理する。"""

    def __init__(self, bucket_name: str, prefix: str = WATCH_STATE_PREFIX):
        if not bucket_name:
            raise ValueError("bucket_name is required to persist watch states.")
        self.bucket_name = bucket_name
        self.client = _build_storage_client()
        self.bucket = self.client.bucket(bucket_name)
        prefix = (prefix or "").strip("/")
        self.prefix = f"{prefix}/" if prefix else ""

    def _blob_path(self, key: str) -> str:
        return f"{self.prefix}{key}.json"

    def _write_state(self, key: str, state: Dict[str, Any]) -> None:
        blob = self.bucket.blob(self._blob_path(key))
        blob.upload_from_string(
            json.dumps(state, ensure_ascii=False, indent=2),
            content_type="application/json"
        )

    def save(self, state: Dict[str, Any]) -> None:
        if "uuid" not in state:
            raise ValueError("state must include 'uuid'")
        self._write_state(state["uuid"], state)

    def load(self, key: str) -> Optional[Dict[str, Any]]:
        blob = self.bucket.blob(self._blob_path(key))
        if not blob.exists():
            return None
        return json.loads(blob.download_as_text())

    def delete(self, key: str) -> None:
        blob = self.bucket.blob(self._blob_path(key))
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

    def save_company_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        state = dict(state)
        state.pop("is_drive_channel", None)
        self.save(state)
        return state

    def load_company_state(self, uuid: str) -> Optional[Dict[str, Any]]:
        data = self.load(uuid)
        if data and data.get("is_drive_channel"):
            return None
        return data

    def delete_company_state(self, uuid: str) -> None:
        self.delete(uuid)

    def list_company_states(self, drive_id: Optional[str]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for state in self.list_states():
            if state.get("is_drive_channel"):
                continue
            current_drive = state.get("drive_id")
            if drive_id is None:
                if current_drive is not None:
                    continue
            else:
                if current_drive != drive_id:
                    continue
            results.append(state)
        return results

    def list_all_company_states(self) -> List[Dict[str, Any]]:
        return [state for state in self.list_states() if not state.get("is_drive_channel")]

    def _drive_state_key(self, drive_id: Optional[str]) -> str:
        drive_key = drive_id or "root"
        return f"drive-channel-{drive_key}"

    def save_drive_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        if "drive_id" not in state:
            raise ValueError("drive_state must include 'drive_id'")
        drive_state = dict(state)
        drive_state["uuid"] = self._drive_state_key(drive_state.get("drive_id"))
        drive_state["is_drive_channel"] = True
        self.save(drive_state)
        return drive_state

    def load_drive_state(self, drive_id: Optional[str]) -> Optional[Dict[str, Any]]:
        data = self.load(self._drive_state_key(drive_id))
        if data and not data.get("is_drive_channel"):
            return None
        return data

    def delete_drive_state(self, drive_id: Optional[str]) -> None:
        self.delete(self._drive_state_key(drive_id))

    def list_drive_states(self) -> List[Dict[str, Any]]:
        return [state for state in self.list_states() if state.get("is_drive_channel")]

    def find_drive_state_by_channel_id(self, channel_id: str) -> Optional[Dict[str, Any]]:
        for state in self.list_drive_states():
            if state.get("channel_id") == channel_id:
                return state
        return None

    def find_by_channel_id(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Backward-compatible wrapper."""
        return self.find_drive_state_by_channel_id(channel_id)



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

        folder_id = extract_folder_id(drive_url)
        folder_metadata = self.drive_service.files().get(
            fileId=folder_id,
            fields="id, name, driveId",
            supportsAllDrives=True
        ).execute()
        drive_id = folder_metadata.get("driveId")

        existing_company = self.store.load_company_state(uuid)
        is_new_company = existing_company is None
        preserved_trigger_ts = existing_company.get("last_job_trigger_ts") if existing_company else None
        company_state = {
            "uuid": uuid,
            "drive_url": drive_url,
            "company_name": company_name,
            "folder_id": folder_id,
            "drive_id": drive_id,
            "use_embed_v4": use_embed_v4,
            "last_job_trigger_ts": preserved_trigger_ts,
        }
        self.store.save_company_state(company_state)

        drive_state = self._ensure_drive_channel(drive_id, target_callback)
        response_state = {
            "uuid": uuid,
            "drive_id": drive_id,
            "folder_id": folder_id,
            "channel_id": drive_state.get("channel_id"),
            "resource_id": drive_state.get("resource_id"),
            "expiration": drive_state.get("expiration"),
            "drive_channel_created": drive_state.get("is_new_channel", False),
            "is_new_channel": is_new_company,
        }
        print(f"✅ Registered company {uuid} for Drive watch (drive_id={drive_id})")
        return response_state

    def save_company_state_only(
        self,
        uuid: str,
        drive_url: str,
        company_name: str = "",
        use_embed_v4: bool = False,
    ) -> Dict[str, Any]:
        folder_id = extract_folder_id(drive_url)
        folder_metadata = self.drive_service.files().get(
            fileId=folder_id,
            fields="id, name, driveId",
            supportsAllDrives=True
        ).execute()
        drive_id = folder_metadata.get("driveId")
        existing_company = self.store.load_company_state(uuid)
        preserved_trigger_ts = existing_company.get("last_job_trigger_ts") if existing_company else None
        company_state = {
            "uuid": uuid,
            "drive_url": drive_url,
            "company_name": company_name,
            "folder_id": folder_id,
            "drive_id": drive_id,
            "use_embed_v4": use_embed_v4,
            "last_job_trigger_ts": preserved_trigger_ts,
        }
        self.store.save_company_state(company_state)
        print(f"📝 Saved company state for UUID {uuid} (drive_id={drive_id})")
        return company_state

    def _ensure_drive_channel(self, drive_id: Optional[str], callback_url: str, force: bool = False) -> Dict[str, Any]:
        existing = self.store.load_drive_state(drive_id)
        if existing and force:
            self._stop_drive_channel(existing)
            self.store.delete_drive_state(drive_id)
            existing = None
        if existing:
            existing["is_new_channel"] = False
            return existing

        start_page_token = self._get_start_page_token(drive_id)
        channel_id = str(uuid4())
        watch_body: Dict[str, Any] = {
            "id": channel_id,
            "type": "web_hook",
            "address": callback_url,
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
        drive_state = {
            "drive_id": drive_id,
            "channel_id": channel_id,
            "resource_id": response.get("resourceId"),
            "expiration": response.get("expiration"),
            "page_token": start_page_token,
        }
        self.store.save_drive_state(drive_state)
        drive_state["is_new_channel"] = True
        print(f"✅ Drive-level watch created (drive_id={drive_id}, channel={channel_id})")
        return drive_state

    def stop_watch(self, uuid: str) -> Optional[Dict[str, Any]]:
        state = self.store.load_company_state(uuid)
        if not state:
            return None

        self.store.delete_company_state(uuid)
        drive_id = state.get("drive_id")
        remaining = self.store.list_company_states(drive_id)
        drive_state: Optional[Dict[str, Any]] = None
        if not remaining:
            drive_state = self.store.load_drive_state(drive_id)
            if drive_state:
                self._stop_drive_channel(drive_state)
                self.store.delete_drive_state(drive_id)
        response = dict(state)
        if drive_state:
            response["channel_id"] = drive_state.get("channel_id")
            response["resource_id"] = drive_state.get("resource_id")
            response["drive_channel_stopped"] = True
        else:
            response["drive_channel_stopped"] = False
        return response

    def _stop_drive_channel(self, drive_state: Dict[str, Any]) -> None:
        body = {
            "id": drive_state.get("channel_id"),
            "resourceId": drive_state.get("resource_id"),
        }
        try:
            self.drive_service.channels().stop(body=body).execute()
            print(f"🛑 Drive watch channel stopped (drive_id={drive_state.get('drive_id')})")
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status not in (404, 410):
                raise
            print(f"⚠️  Channel stop returned {status} for drive {drive_state.get('drive_id')} ({status}), continuing cleanup.")

    def re_register_companies(self, uuids: Optional[List[str]] = None) -> Dict[str, Any]:
        target_callback = self.default_callback_url
        if not target_callback:
            raise ValueError("callback_url is required to re-register Drive watch channels.")
        all_states = self.store.list_all_company_states()
        if uuids:
            uuids_set = set(uuids)
            company_states = [state for state in all_states if state.get("uuid") in uuids_set]
        else:
            company_states = all_states

        if not company_states:
            return {"processed_drive_count": 0, "details": []}

        grouped: Dict[Optional[str], List[str]] = {}
        for state in company_states:
            drive_id = state.get("drive_id")
            grouped.setdefault(drive_id, []).append(state["uuid"])

        details = []
        for drive_id, uuid_list in grouped.items():
            drive_state = self._ensure_drive_channel(drive_id, target_callback, force=True)
            details.append({
                "drive_id": drive_id,
                "channel_id": drive_state.get("channel_id"),
                "uuid_count": len(uuid_list),
                "uuids": uuid_list
            })
        return {"processed_drive_count": len(details), "details": details}

    def delete_embedding_data(self, uuid: str) -> bool:
        client = _build_storage_client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(f"{uuid}.json")
        if blob.exists():
            blob.delete()
            return True
        return False


class DriveNotificationProcessor:
    """Drive通知を処理し、必要に応じてベクトル化ジョブを再実行する。"""

    def __init__(
        self,
        bucket_name: str,
        job_service,
        cooldown_seconds: Optional[int] = None,
        verbose_logging: Optional[bool] = None,
    ):
        if not bucket_name:
            raise ValueError("bucket_name is required.")
        self.bucket_name = bucket_name
        self.store = DriveWatchStateStore(bucket_name)
        self.job_service = job_service
        self.drive_service = build("drive", "v3", credentials=_build_drive_credentials(), cache_discovery=False)
        self._parent_cache: Dict[str, List[str]] = {}
        default_cooldown = os.getenv("DRIVE_WATCH_COOLDOWN_SECONDS", "").strip()
        derived_cooldown = int(default_cooldown or "60")
        self.cooldown_seconds = cooldown_seconds if cooldown_seconds is not None else derived_cooldown
        if self.cooldown_seconds < 0:
            self.cooldown_seconds = 0
        env_verbose = os.getenv("DRIVE_WATCH_VERBOSE_LOGS", "true").strip().lower() not in {"false", "0", "no"}
        self.verbose_logging = env_verbose if verbose_logging is None else verbose_logging

    def handle_notification(
        self,
        channel_id: str,
        resource_state: str = "",
        resource_id: str = "",
        changed_types: str = "",
    ) -> Dict[str, Any]:
        drive_state = self.store.find_drive_state_by_channel_id(channel_id)
        if not drive_state:
            print(f"⚠️  No stored drive state found for channel {channel_id}. Ignoring notification.")
            return {"handled": False, "reason": "unknown_channel"}
        drive_id = drive_state.get("drive_id")
        self._log(
            f"📨 Drive notification received for drive {drive_id} "
            f"(state={resource_state}, resource={resource_id}, changed={changed_types})"
        )

        if resource_state == "sync":
            # 初回の同期リクエストは通知チャネル作成時に必ず送られる
            return {"handled": True, "changes_found": 0, "job_triggered": False, "status": "sync"}

        if changed_types:
            normalized_changed = {item.strip().lower() for item in changed_types.split(",") if item.strip()}
            if normalized_changed and "content" not in normalized_changed:
                self._log(
                    f"🔇 Ignoring notification for drive {drive_id} "
                    f"because changed types do not include file content: {normalized_changed}"
                )
                return {"handled": True, "changes_found": 0, "job_triggered": False, "status": "filtered_changed_type"}

        company_states = self.store.list_company_states(drive_id)
        if not company_states:
            self._log(f"ℹ️  No registered companies for drive {drive_id}.")
            return {"handled": True, "changes_found": 0, "job_triggered": False, "status": "no_companies"}

        changes = self._consume_drive_change_feed(drive_state)
        matches = self._match_changes_to_companies(changes, company_states)
        if not matches:
            self._log(f"ℹ️  No relevant changes found for drive {drive_id}.")
            return {"handled": True, "changes_found": 0, "job_triggered": False}

        triggered = 0
        now = time.time()
        for company_state in company_states:
            uuid = company_state["uuid"]
            if uuid not in matches:
                continue
            last_trigger = company_state.get("last_job_trigger_ts")
            try:
                last_trigger_ts = float(last_trigger) if last_trigger is not None else None
            except (TypeError, ValueError):
                last_trigger_ts = None

            if self.cooldown_seconds and last_trigger_ts:
                elapsed = now - last_trigger_ts
                if elapsed < self.cooldown_seconds:
                    print(
                        f"⏳ Drive watch cooldown active for UUID {uuid}: "
                        f"{elapsed:.1f}s elapsed (cooldown {self.cooldown_seconds}s). Skipping job trigger."
                    )
                    continue

            print(f"🔁 Detected {matches[uuid]} change(s) for UUID {uuid}. Triggering vectorization job.")
            self.job_service.trigger_vectorization_job(
                uuid=uuid,
                drive_url=company_state["drive_url"],
                use_embed_v4=company_state.get("use_embed_v4", False)
            )
            company_state["last_job_trigger_ts"] = now
            self.store.save_company_state(company_state)
            triggered += 1

        return {
            "handled": True,
            "changes_found": sum(matches.values()),
            "job_triggered": triggered > 0,
            "triggered_count": triggered,
        }

    def _log(self, message: str) -> None:
        if self.verbose_logging:
            print(message)

    def _consume_drive_change_feed(self, drive_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        token = drive_state.get("page_token")
        if not token:
            token = self._get_start_page_token(drive_state.get("drive_id"))

        aggregated: List[Dict[str, Any]] = []
        current_token = token
        latest_new_start: Optional[str] = None

        while current_token:
            try:
                response = self._list_changes(current_token, drive_state.get("drive_id"))
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                if status == 410:
                    print("⚠️  Stored page token expired for drive channel. Resetting to latest start token.")
                    new_token = self._get_start_page_token(drive_state.get("drive_id"))
                    drive_state["page_token"] = new_token
                    self.store.save_drive_state(drive_state)
                    return []
                raise

            aggregated.extend(response.get("changes", []))
            current_token = response.get("nextPageToken")
            if response.get("newStartPageToken"):
                latest_new_start = response["newStartPageToken"]

            if not current_token:
                break

        if latest_new_start:
            drive_state["page_token"] = latest_new_start
        elif current_token:
            drive_state["page_token"] = current_token
        elif not drive_state.get("page_token"):
            drive_state["page_token"] = token

        self.store.save_drive_state(drive_state)
        return aggregated

    def _match_changes_to_companies(
        self,
        changes: List[Dict[str, Any]],
        company_states: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        matches: Dict[str, int] = {}
        for company_state in company_states:
            folder_id = company_state.get("folder_id")
            if not folder_id:
                continue
            relevant = self._filter_relevant_changes(changes, folder_id)
            if relevant:
                matches[company_state["uuid"]] = len(relevant)
        return matches

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

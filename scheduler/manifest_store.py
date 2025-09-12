#!/usr/bin/env python3
"""
Manifest Store for GCS-based Drive File Change Detection

This module provides functionality to store and compare Drive file metadata
in Google Cloud Storage to enable efficient differential updates.

Features:
- Store file metadata (modifiedTime, size, md5Checksum) as manifests in GCS
- Compare current Drive state with stored manifests
- Detect added, updated, and removed files
- Enable scheduler to skip unnecessary vectorization jobs

Author: Claude Code Assistant (based on reviewer feedback)
Version: 1.0.0
"""

import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from google.cloud import storage

logger = logging.getLogger(__name__)


def _get_manifest_bucket_name(bucket_name: str) -> str:
    """
    Generate manifest bucket name based on main bucket name.
    
    Args:
        bucket_name: Main bucket name (e.g., "embedding_storage_dev")
        
    Returns:
        Manifest bucket name (e.g., "emb_manifest_dev")
    """
    if bucket_name.endswith("_dev"):
        return "emb_manifest_dev"
    elif bucket_name.endswith("_staging"):
        return "emb_manifest_staging"
    else:
        return "emb_manifest"  # Production


def _manifest_blob_name(uuid: str) -> str:
    """Generate GCS blob name for company manifest (just UUID.json)."""
    return f"{uuid}.json"


def load_manifest(bucket_name: str, uuid: str) -> Dict:
    """
    Load manifest from GCS for given company UUID.
    
    Args:
        bucket_name: Main GCS bucket name (e.g., "embedding_storage_dev")
        uuid: Company UUID
        
    Returns:
        Dict containing manifest data, or empty dict if not found
    """
    try:
        client = storage.Client()
        manifest_bucket_name = _get_manifest_bucket_name(bucket_name)
        bucket = client.bucket(manifest_bucket_name)
        blob = bucket.blob(_manifest_blob_name(uuid))
        
        if not blob.exists():
            logger.info(f"No manifest found for {uuid} in {manifest_bucket_name}, treating as first run")
            return {}
            
        manifest_data = json.loads(blob.download_as_string())
        logger.info(f"Loaded manifest for {uuid} from {manifest_bucket_name}: {len(manifest_data.get('files', {}))} files")
        return manifest_data
        
    except Exception as e:
        logger.error(f"Error loading manifest for {uuid} from {_get_manifest_bucket_name(bucket_name)}: {e}")
        return {}


def save_manifest(bucket_name: str, uuid: str, manifest: Dict):
    """
    Save manifest to GCS.
    
    Args:
        bucket_name: Main GCS bucket name (e.g., "embedding_storage_dev")
        uuid: Company UUID
        manifest: Manifest data to save
    """
    try:
        client = storage.Client()
        manifest_bucket_name = _get_manifest_bucket_name(bucket_name)
        bucket = client.bucket(manifest_bucket_name)
        blob = bucket.blob(_manifest_blob_name(uuid))
        
        blob.upload_from_string(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        
        logger.info(f"Saved manifest for {uuid} to {manifest_bucket_name}: {len(manifest.get('files', {}))} files")
        
    except Exception as e:
        logger.error(f"Error saving manifest for {uuid} to {_get_manifest_bucket_name(bucket_name)}: {e}")
        raise


def needs_update_from_manifest(bucket_name: str, uuid: str, drive_files: List[Dict]) -> bool:
    """
    Compare current Drive files with stored manifest to determine if update is needed.
    
    Args:
        bucket_name: GCS bucket name
        uuid: Company UUID
        drive_files: Current Drive files list (from drive_scanner.list_files_in_drive_folder)
                    Must include fields: id, modifiedTime, size, md5Checksum
                    
    Returns:
        bool: True if update is needed (files added/updated/removed), False otherwise
    """
    try:
        manifest = load_manifest(bucket_name, uuid)
        prev_files_index = manifest.get("files", {})  # mapping file_id -> metadata dict
        
        # Build current files index
        current_files_index = {}
        for f in drive_files:
            file_id = f.get("id") or f.get("file_id") or f.get("fileId")
            if not file_id:
                logger.warning(f"Drive file missing ID: {f}")
                continue
                
            current_files_index[file_id] = {
                "modifiedTime": f.get("modifiedTime"),
                "size": f.get("size"),
                "md5Checksum": f.get("md5Checksum"),
                "name": f.get("name"),  # For debugging
                "folder_path": f.get("folder_path", "")
            }
        
        # Detect changes
        prev_ids = set(prev_files_index.keys())
        current_ids = set(current_files_index.keys())
        
        # Files removed from Drive
        removed_files = prev_ids - current_ids
        if removed_files:
            logger.info(f"Company {uuid}: {len(removed_files)} files removed")
            for file_id in list(removed_files)[:3]:  # Log first 3
                file_info = prev_files_index.get(file_id, {})
                logger.info(f"  Removed: {file_info.get('name', file_id)}")
            return True
        
        # Files added to Drive
        added_files = current_ids - prev_ids
        if added_files:
            logger.info(f"Company {uuid}: {len(added_files)} files added")
            for file_id in list(added_files)[:3]:  # Log first 3
                file_info = current_files_index.get(file_id, {})
                logger.info(f"  Added: {file_info.get('name', file_id)}")
            return True
        
        # Files modified
        modified_files = []
        for file_id in current_ids & prev_ids:
            prev_meta = prev_files_index.get(file_id, {})
            current_meta = current_files_index.get(file_id, {})
            
            # Compare metadata
            if (prev_meta.get("modifiedTime") != current_meta.get("modifiedTime") or
                prev_meta.get("size") != current_meta.get("size") or
                prev_meta.get("md5Checksum") != current_meta.get("md5Checksum")):
                modified_files.append(file_id)
        
        if modified_files:
            logger.info(f"Company {uuid}: {len(modified_files)} files modified")
            for file_id in modified_files[:3]:  # Log first 3
                file_info = current_files_index.get(file_id, {})
                logger.info(f"  Modified: {file_info.get('name', file_id)}")
            return True
        
        # No changes detected
        logger.info(f"Company {uuid}: No changes detected ({len(current_files_index)} files)")
        return False
        
    except Exception as e:
        logger.error(f"Error checking manifest for {uuid}: {e}")
        # On error, assume update is needed to be safe
        return True


def update_manifest_after_run(bucket_name: str, uuid: str, drive_files: List[Dict]):
    """
    Update manifest after successful vectorization run.
    
    Args:
        bucket_name: GCS bucket name
        uuid: Company UUID  
        drive_files: Current Drive files list used for the update
    """
    try:
        # Build files index
        files_index = {}
        for f in drive_files:
            file_id = f.get("id") or f.get("file_id") or f.get("fileId")
            if not file_id:
                continue
                
            files_index[file_id] = {
                "modifiedTime": f.get("modifiedTime"),
                "size": f.get("size"), 
                "md5Checksum": f.get("md5Checksum"),
                "name": f.get("name"),
                "folder_path": f.get("folder_path", "")
            }
        
        # Create manifest
        now_utc = datetime.now(timezone.utc).isoformat()
        manifest = {
            "last_checked": now_utc,
            "last_updated": now_utc,
            "files_count": len(files_index),
            "files": files_index
        }
        
        # Save to GCS
        save_manifest(bucket_name, uuid, manifest)
        
        logger.info(f"Updated manifest for {uuid} with {len(files_index)} files")
        
    except Exception as e:
        logger.error(f"Error updating manifest for {uuid}: {e}")
        raise


def get_manifest_info(bucket_name: str, uuid: str) -> Optional[Dict]:
    """
    Get basic manifest information for debugging/monitoring.
    
    Args:
        bucket_name: GCS bucket name
        uuid: Company UUID
        
    Returns:
        Dict with manifest summary info, or None if not found
    """
    try:
        manifest = load_manifest(bucket_name, uuid)
        if not manifest:
            return None
            
        return {
            "uuid": uuid,
            "last_checked": manifest.get("last_checked"),
            "last_updated": manifest.get("last_updated"), 
            "files_count": manifest.get("files_count", len(manifest.get("files", {}))),
            "has_files": len(manifest.get("files", {})) > 0
        }
        
    except Exception as e:
        logger.error(f"Error getting manifest info for {uuid}: {e}")
        return None


def list_all_manifests(bucket_name: str) -> List[str]:
    """
    List all company UUIDs that have manifests stored.
    
    Args:
        bucket_name: Main GCS bucket name (e.g., "embedding_storage_dev")
        
    Returns:
        List of company UUIDs
    """
    try:
        client = storage.Client()
        manifest_bucket_name = _get_manifest_bucket_name(bucket_name)
        bucket = client.bucket(manifest_bucket_name)
        
        uuids = []
        for blob in bucket.list_blobs():
            if blob.name.endswith('.json'):
                # Extract UUID from filename: {uuid}.json
                uuid = blob.name[:-5]  # Remove .json extension
                uuids.append(uuid)
        
        logger.info(f"Found {len(uuids)} manifests in {manifest_bucket_name}")
        return sorted(uuids)
        
    except Exception as e:
        logger.error(f"Error listing manifests from {_get_manifest_bucket_name(bucket_name)}: {e}")
        return []
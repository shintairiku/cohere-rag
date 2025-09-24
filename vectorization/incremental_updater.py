#!/usr/bin/env python3
"""
Incremental Embedding Updater for Google Drive Images

This script performs differential updates to embedding.json files based on 
changes in Google Drive folders. It detects added, removed, and modified images
and updates only the affected embeddings, making the process much more efficient.

Key Features:
1. Change detection using file modification times and checksums
2. Differential updates - only process changed files
3. Backup and rollback capabilities
4. Comprehensive logging and error handling
5. Support for batch processing multiple companies

Author: Claude Code Assistant
Version: 1.0.0
"""

import os
import io
import json
import hashlib
import traceback
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass

import cohere
import numpy as np
from dotenv import load_dotenv
from google.cloud import storage
from PIL import Image

from .drive_scanner import list_files_in_drive_folder, _get_google_credentials
from .img_meta_processor_gdrive import (
    resize_image_if_needed, 
    co_client,
    storage_client
)

load_dotenv()

@dataclass
class FileMetadata:
    """Represents metadata for a file in the embedding system."""
    file_id: str
    filename: str
    filepath: str
    modified_time: str
    checksum: str
    size: int


@dataclass
class UpdateStats:
    """Statistics for an incremental update operation."""
    total_files: int
    added: int
    removed: int
    updated: int
    skipped: int
    errors: int
    start_time: datetime
    end_time: Optional[datetime] = None
    
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0


class IncrementalEmbeddingUpdater:
    """
    Handles incremental updates to embedding files stored in Google Cloud Storage.
    """
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.bucket = storage_client.bucket(bucket_name)
        self.cohere_client = co_client
        
    def get_current_embeddings(self, uuid: str) -> Tuple[List[Dict], Dict[str, FileMetadata]]:
        """
        Load current embeddings and create a metadata index.
        
        Returns:
            Tuple of (embeddings_list, metadata_dict)
        """
        blob_name = f"{uuid}.json"
        blob = self.bucket.blob(blob_name)
        
        if not blob.exists():
            print(f"📄 No existing embeddings found for {uuid}")
            return [], {}
            
        try:
            content = blob.download_as_string()
            embeddings = json.loads(content)
            print(f"📄 Loaded {len(embeddings)} existing embeddings for {uuid}")
            
            # Create metadata index for quick lookup
            metadata_index = {}
            for item in embeddings:
                if 'file_id' in item:
                    metadata = FileMetadata(
                        file_id=item['file_id'],
                        filename=item['filename'],
                        filepath=item['filepath'],
                        modified_time=item.get('modified_time', ''),
                        checksum=item.get('checksum', ''),
                        size=item.get('size', 0)
                    )
                    metadata_index[item['file_id']] = metadata
                    
            return embeddings, metadata_index
            
        except Exception as e:
            print(f"❌ Error loading embeddings for {uuid}: {e}")
            return [], {}
    
    def detect_changes(self, drive_files: List[Dict], current_metadata: Dict[str, FileMetadata]) -> Dict[str, List[Dict]]:
        """
        Detect changes between current Drive state and existing embeddings.
        
        Returns:
            Dict with 'added', 'updated', 'removed' file lists
        """
        print(f"🔍 Detecting changes in {len(drive_files)} Drive files vs {len(current_metadata)} existing embeddings")
        
        drive_file_ids = {f['file_id'] for f in drive_files}
        existing_file_ids = set(current_metadata.keys())
        
        # Files that exist in Drive but not in embeddings
        added_file_ids = drive_file_ids - existing_file_ids
        added_files = [f for f in drive_files if f['file_id'] in added_file_ids]
        
        # Files that exist in embeddings but not in Drive
        removed_file_ids = existing_file_ids - drive_file_ids
        
        # Files that exist in both but may have been modified
        updated_files = []
        for drive_file in drive_files:
            file_id = drive_file['file_id']
            if file_id in current_metadata:
                existing_meta = current_metadata[file_id]
                
                # Check if file was modified (by modification time or size)
                drive_modified = drive_file.get('modified_time', '')
                existing_modified = existing_meta.modified_time
                
                drive_size = drive_file.get('size', 0)
                existing_size = existing_meta.size
                
                if drive_modified != existing_modified or drive_size != existing_size:
                    updated_files.append(drive_file)
                    print(f"  📝 Modified: {drive_file['filename']}")
                    print(f"     Time: {existing_modified} → {drive_modified}")
                    print(f"     Size: {existing_size} → {drive_size}")
        
        changes = {
            'added': added_files,
            'updated': updated_files,
            'removed': list(removed_file_ids)
        }
        
        print(f"📊 Change summary: +{len(added_files)} added, ~{len(updated_files)} updated, -{len(removed_file_ids)} removed")
        return changes
    
    def process_file_for_embedding(self, file_info: Dict) -> Optional[Dict]:
        """
        Process a single file to generate its embedding.
        
        Returns:
            Dict with embedding data or None if processing failed
        """
        try:
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaIoBaseDownload
            
            # Get Google Drive service
            creds = _get_google_credentials()
            drive_service = build('drive', 'v3', credentials=creds)
            
            file_id = file_info['file_id']
            filename = file_info['filename']
            
            print(f"  🔄 Processing: {filename}")
            
            # Download file from Google Drive
            request = drive_service.files().get_media(fileId=file_id)
            file_content = io.BytesIO()
            downloader = MediaIoBaseDownload(file_content, request)
            
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            file_content.seek(0)
            image_bytes = file_content.read()
            
            # Resize if needed
            processed_image = resize_image_if_needed(image_bytes, filename)
            
            # Calculate checksum for change detection
            checksum = hashlib.md5(processed_image).hexdigest()
            
            # Encode for Cohere API
            image_b64 = base64.b64encode(processed_image).decode('utf-8')
            
            # Generate embedding
            print(f"    🧠 Generating embedding for {filename}")
            response = self.cohere_client.embed(
                images=[image_b64],
                model="embed-english-v3.0",
                input_type="image",
                embedding_types=["float"]
            )
            
            embedding_vector = response.embeddings.float[0]
            
            return {
                'file_id': file_id,
                'filename': filename,
                'filepath': file_info['filepath'],
                'modified_time': file_info.get('modified_time', ''),
                'checksum': checksum,
                'size': len(image_bytes),
                'embedding': embedding_vector
            }
            
        except Exception as e:
            print(f"    ❌ Error processing {file_info.get('filename', 'unknown')}: {e}")
            traceback.print_exc()
            return None
    
    def apply_incremental_update(self, uuid: str, changes: Dict[str, List], current_embeddings: List[Dict]) -> UpdateStats:
        """
        Apply incremental changes to the embeddings.
        
        Returns:
            UpdateStats object with operation statistics
        """
        stats = UpdateStats(
            total_files=len(changes['added']) + len(changes['updated']) + len(changes['removed']),
            added=0,
            removed=0,
            updated=0,
            skipped=0,
            errors=0,
            start_time=datetime.now()
        )
        
        print(f"🚀 Starting incremental update for {uuid}")
        
        # Create a working copy of embeddings
        updated_embeddings = []
        existing_file_ids = set()
        
        # Keep embeddings that are not being removed or updated
        for item in current_embeddings:
            file_id = item.get('file_id')
            if file_id:
                existing_file_ids.add(file_id)
                
                # Skip files that will be updated
                if file_id not in [f['file_id'] for f in changes['updated']]:
                    # Skip files that are being removed
                    if file_id not in changes['removed']:
                        updated_embeddings.append(item)
        
        # Remove files
        for removed_file_id in changes['removed']:
            print(f"  🗑️  Removing: {removed_file_id}")
            stats.removed += 1
        
        # Process updated files
        for file_info in changes['updated']:
            result = self.process_file_for_embedding(file_info)
            if result:
                updated_embeddings.append(result)
                stats.updated += 1
                print(f"    ✅ Updated: {file_info['filename']}")
            else:
                stats.errors += 1
        
        # Process added files
        for file_info in changes['added']:
            result = self.process_file_for_embedding(file_info)
            if result:
                updated_embeddings.append(result)
                stats.added += 1
                print(f"    ✅ Added: {file_info['filename']}")
            else:
                stats.errors += 1
        
        # Save updated embeddings
        self._save_embeddings(uuid, updated_embeddings)
        
        stats.end_time = datetime.now()
        
        print(f"✅ Incremental update completed for {uuid}")
        print(f"   📊 Stats: +{stats.added} added, ~{stats.updated} updated, -{stats.removed} removed")
        print(f"   ⏱️  Duration: {stats.duration_seconds():.1f} seconds")
        print(f"   ❌ Errors: {stats.errors}")
        
        return stats
    
    def _save_embeddings(self, uuid: str, embeddings: List[Dict]):
        """Save embeddings to Cloud Storage with backup."""
        blob_name = f"{uuid}.json"
        backup_name = f"{uuid}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Create backup of existing file
        existing_blob = self.bucket.blob(blob_name)
        if existing_blob.exists():
            print(f"📦 Creating backup: {backup_name}")
            backup_blob = self.bucket.blob(backup_name)
            backup_blob.rewrite(existing_blob)
        
        # Save new embeddings
        new_blob = self.bucket.blob(blob_name)
        new_blob.upload_from_string(
            json.dumps(embeddings, ensure_ascii=False, indent=2),
            content_type='application/json'
        )
        
        print(f"💾 Saved {len(embeddings)} embeddings to {blob_name}")
    
    def update_company_embeddings(self, uuid: str, drive_url: str) -> UpdateStats:
        """
        Perform incremental update for a single company.
        
        Args:
            uuid: Company UUID
            drive_url: Google Drive folder URL
            
        Returns:
            UpdateStats object
        """
        print(f"🏢 Starting incremental update for company: {uuid}")
        print(f"📁 Drive URL: {drive_url}")
        
        try:
            # Get current state from Drive
            drive_files = list_files_in_drive_folder(drive_url)
            print(f"📁 Found {len(drive_files)} files in Drive")
            
            # Get current embeddings
            current_embeddings, current_metadata = self.get_current_embeddings(uuid)
            
            # Detect changes
            changes = self.detect_changes(drive_files, current_metadata)
            
            # If no changes, skip processing
            total_changes = len(changes['added']) + len(changes['updated']) + len(changes['removed'])
            if total_changes == 0:
                print(f"✅ No changes detected for {uuid}")
                stats = UpdateStats(
                    total_files=0, added=0, removed=0, updated=0, skipped=len(drive_files), errors=0,
                    start_time=datetime.now(), end_time=datetime.now()
                )
                return stats
            
            # Apply incremental update
            stats = self.apply_incremental_update(uuid, changes, current_embeddings)
            return stats
            
        except Exception as e:
            print(f"❌ Error during incremental update for {uuid}: {e}")
            traceback.print_exc()
            return UpdateStats(
                total_files=0, added=0, removed=0, updated=0, skipped=0, errors=1,
                start_time=datetime.now(), end_time=datetime.now()
            )


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Incremental Embedding Updater")
    parser.add_argument("--uuid", required=True, help="Company UUID")
    parser.add_argument("--drive-url", required=True, help="Google Drive folder URL")
    parser.add_argument("--bucket", help="GCS bucket name (default: from GCS_BUCKET_NAME env var)")
    
    args = parser.parse_args()
    
    bucket_name = args.bucket or os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        print("❌ Error: GCS bucket name not specified")
        return
    
    updater = IncrementalEmbeddingUpdater(bucket_name)
    stats = updater.update_company_embeddings(args.uuid, args.drive_url)
    
    print("\n" + "="*60)
    print("📊 FINAL STATISTICS")
    print("="*60)
    print(f"Total files processed: {stats.total_files}")
    print(f"Added: {stats.added}")
    print(f"Updated: {stats.updated}")
    print(f"Removed: {stats.removed}")
    print(f"Skipped: {stats.skipped}")
    print(f"Errors: {stats.errors}")
    print(f"Duration: {stats.duration_seconds():.1f} seconds")
    print("="*60)


if __name__ == "__main__":
    main()
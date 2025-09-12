#!/usr/bin/env python3
"""
Batch Incremental Updater for Multiple Companies

This script performs incremental updates for all companies listed in a 
Google Sheets "会社一覧" or from a configuration file. It can be run as:
1. A standalone script for batch processing
2. A Cloud Run job for scheduled execution
3. Via API endpoint for on-demand batch updates

Features:
- Parallel processing of multiple companies
- Comprehensive reporting and notifications
- Error handling and retry mechanisms
- Integration with Google Sheets for company data
- Configurable scheduling and throttling

Author: Claude Code Assistant
Version: 1.0.0
"""

import os
import json
import time
import asyncio
import traceback
from datetime import datetime
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field

from dotenv import load_dotenv
from google.cloud import storage
import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build

from vectorization.incremental_updater import IncrementalEmbeddingUpdater, UpdateStats

load_dotenv()

@dataclass
class CompanyInfo:
    """Company information for batch processing."""
    uuid: str
    name: str
    drive_url: str
    sheet_url: str = field(default="")       # Column D: Sheet URL
    sheet_name: str = field(default="")      # Column E: Sheet Name  
    auto_update: bool = field(default=False)  # Column F: AutoUpdate (yes/no)
    
@dataclass
class BatchUpdateResults:
    """Results from a batch update operation."""
    total_companies: int
    successful_updates: int
    failed_updates: int
    total_files_added: int
    total_files_updated: int
    total_files_removed: int
    total_errors: int
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    company_results: List[Dict]
    
    def to_dict(self):
        return asdict(self)


class BatchIncrementalUpdater:
    """
    Handles batch incremental updates for multiple companies.
    """
    
    def __init__(self, bucket_name: str, max_workers: int = 3):
        self.bucket_name = bucket_name
        self.max_workers = max_workers
        self.updater = IncrementalEmbeddingUpdater(bucket_name)
        
    def get_companies_from_sheets(self, spreadsheet_id: str, auto_only: bool = False) -> List[CompanyInfo]:
        """
        Get company information from Google Sheets.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            auto_only: If True, return only companies with AutoUpdate enabled (column F)
            
        Returns:
            List of CompanyInfo objects
        """
        try:
            # Get credentials
            environment = os.getenv("ENVIRONMENT", "local")
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            
            if environment == "production":
                creds, _ = google.auth.default(scopes=scopes)
            else:
                key_file = "marketing-automation-461305-2acf4965e0b0.json"
                if os.path.exists(key_file):
                    creds = service_account.Credentials.from_service_account_file(key_file, scopes=scopes)
                else:
                    creds, _ = google.auth.default(scopes=scopes)
            
            # Build Sheets service
            sheets_service = build('sheets', 'v4', credentials=creds)
            
            # Read company data from "会社一覧" sheet  
            range_name = "会社一覧!A:F"  # UUID, Name, Drive URL, Sheet URL, Sheet Name, AutoUpdate
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name
            ).execute()
            
            values = result.get('values', [])
            companies = []
            
            # Skip header row and process data
            for i, row in enumerate(values[1:], 2):  # Start from row 2
                # Safe indexing for variable row lengths
                uuid = row[0].strip() if len(row) >= 1 and row[0] else ""
                name = row[1].strip() if len(row) >= 2 and row[1] else ""
                drive_url = row[2].strip() if len(row) >= 3 and row[2] else ""
                sheet_url = row[3].strip() if len(row) >= 4 and row[3] else ""
                sheet_name = row[4].strip() if len(row) >= 5 and row[4] else ""
                auto_flag_raw = row[5].strip() if len(row) >= 6 and row[5] else ""
                
                # Parse AutoUpdate flag (column F): yes/no format
                auto_update = str(auto_flag_raw).lower() in ("yes", "y", "1", "true", "on", "enable", "enabled")
                
                if uuid and name and drive_url:  # All required fields present
                    company = CompanyInfo(
                        uuid=uuid,
                        name=name,
                        drive_url=drive_url,
                        sheet_url=sheet_url,
                        sheet_name=sheet_name,
                        auto_update=auto_update
                    )
                    
                    # Filter by auto_only flag if requested
                    if auto_only:
                        if company.auto_update:
                            companies.append(company)
                            print(f"  📋 Found auto-update company: {company.name} ({company.uuid}) - Sheet: {company.sheet_name}")
                    else:
                        companies.append(company)
                        auto_status = "🔄 AUTO" if company.auto_update else "📝 MANUAL"
                        sheet_info = f"Sheet: {company.sheet_name}" if company.sheet_name else "No sheet"
                        print(f"  📋 Found company: {company.name} ({company.uuid}) - {auto_status} - {sheet_info}")
                elif len(row) > 0 and any(row):  # Non-empty row but missing data
                    print(f"  ⚠️  Row {i}: Incomplete data - {row}")
            
            print(f"📊 Loaded {len(companies)} companies from Google Sheets")
            return companies
            
        except Exception as e:
            print(f"❌ Error reading companies from Google Sheets: {e}")
            traceback.print_exc()
            return []
    
    def get_companies_from_file(self, file_path: str) -> List[CompanyInfo]:
        """
        Get company information from JSON file.
        
        Args:
            file_path: Path to JSON file with company data
            
        Returns:
            List of CompanyInfo objects
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            companies = []
            for item in data.get('companies', []):
                company = CompanyInfo(
                    uuid=item['uuid'],
                    name=item['name'], 
                    drive_url=item['drive_url']
                )
                companies.append(company)
            
            print(f"📊 Loaded {len(companies)} companies from file: {file_path}")
            return companies
            
        except Exception as e:
            print(f"❌ Error reading companies from file {file_path}: {e}")
            return []
    
    def update_single_company(self, company: CompanyInfo) -> Dict:
        """
        Update embeddings for a single company.
        
        Args:
            company: CompanyInfo object
            
        Returns:
            Dict with update results
        """
        print(f"\n🏢 Processing company: {company.name} ({company.uuid})")
        start_time = datetime.now()
        
        try:
            stats = self.updater.update_company_embeddings(company.uuid, company.drive_url)
            
            result = {
                'company': company.name,
                'uuid': company.uuid,
                'success': True,
                'stats': asdict(stats),
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'error': None
            }
            
            print(f"✅ Completed: {company.name} - Added:{stats.added}, Updated:{stats.updated}, Removed:{stats.removed}")
            return result
            
        except Exception as e:
            error_msg = f"Error updating {company.name}: {str(e)}"
            print(f"❌ {error_msg}")
            traceback.print_exc()
            
            return {
                'company': company.name,
                'uuid': company.uuid,
                'success': False,
                'stats': None,
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'error': error_msg
            }
    
    def run_batch_update(self, companies: List[CompanyInfo]) -> BatchUpdateResults:
        """
        Run batch incremental update for multiple companies.
        
        Args:
            companies: List of CompanyInfo objects
            
        Returns:
            BatchUpdateResults object
        """
        start_time = datetime.now()
        print(f"🚀 Starting batch incremental update for {len(companies)} companies")
        print(f"⚙️  Max workers: {self.max_workers}")
        
        company_results = []
        successful_updates = 0
        failed_updates = 0
        total_files_added = 0
        total_files_updated = 0
        total_files_removed = 0
        total_errors = 0
        
        # Process companies in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_company = {
                executor.submit(self.update_single_company, company): company
                for company in companies
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_company):
                result = future.result()
                company_results.append(result)
                
                if result['success']:
                    successful_updates += 1
                    stats = result['stats']
                    total_files_added += stats['added']
                    total_files_updated += stats['updated'] 
                    total_files_removed += stats['removed']
                    total_errors += stats['errors']
                else:
                    failed_updates += 1
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Create results summary
        results = BatchUpdateResults(
            total_companies=len(companies),
            successful_updates=successful_updates,
            failed_updates=failed_updates,
            total_files_added=total_files_added,
            total_files_updated=total_files_updated,
            total_files_removed=total_files_removed,
            total_errors=total_errors,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            company_results=company_results
        )
        
        # Print summary
        self._print_batch_summary(results)
        
        return results
    
    def _print_batch_summary(self, results: BatchUpdateResults):
        """Print batch update summary."""
        print("\n" + "="*80)
        print("📊 BATCH UPDATE SUMMARY")
        print("="*80)
        print(f"🏢 Total companies: {results.total_companies}")
        print(f"✅ Successful updates: {results.successful_updates}")
        print(f"❌ Failed updates: {results.failed_updates}")
        print(f"📁 Total files added: {results.total_files_added}")
        print(f"📝 Total files updated: {results.total_files_updated}")
        print(f"🗑️  Total files removed: {results.total_files_removed}")
        print(f"⚠️  Total errors: {results.total_errors}")
        print(f"⏱️  Total duration: {results.duration_seconds:.1f} seconds")
        print(f"📅 Completed at: {results.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if results.failed_updates > 0:
            print(f"\n❌ Failed companies:")
            for result in results.company_results:
                if not result['success']:
                    print(f"  - {result['company']} ({result['uuid']}): {result['error']}")
        
        print("="*80)
    
    def save_results_to_storage(self, results: BatchUpdateResults, filename: str = None):
        """
        Save batch update results to Cloud Storage.
        
        Args:
            results: BatchUpdateResults object
            filename: Optional filename (default: auto-generated)
        """
        if not filename:
            timestamp = results.start_time.strftime('%Y%m%d_%H%M%S')
            filename = f"batch_update_results_{timestamp}.json"
        
        try:
            bucket = storage.Client().bucket(self.bucket_name)
            blob = bucket.blob(f"logs/{filename}")
            
            blob.upload_from_string(
                json.dumps(results.to_dict(), ensure_ascii=False, indent=2, default=str),
                content_type='application/json'
            )
            
            print(f"💾 Saved batch results to gs://{self.bucket_name}/logs/{filename}")
            
        except Exception as e:
            print(f"❌ Error saving results to storage: {e}")


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch Incremental Updater")
    parser.add_argument("--bucket", help="GCS bucket name (default: from GCS_BUCKET_NAME env var)")
    parser.add_argument("--spreadsheet-id", help="Google Sheets spreadsheet ID")
    parser.add_argument("--companies-file", help="JSON file with companies data")
    parser.add_argument("--max-workers", type=int, default=3, help="Maximum parallel workers")
    parser.add_argument("--save-results", action="store_true", help="Save results to Cloud Storage")
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.spreadsheet_id and not args.companies_file:
        print("❌ Error: Either --spreadsheet-id or --companies-file must be specified")
        return
    
    bucket_name = args.bucket or os.getenv("GCS_BUCKET_NAME")
    if not bucket_name:
        print("❌ Error: GCS bucket name not specified")
        return
    
    # Initialize updater
    updater = BatchIncrementalUpdater(bucket_name, max_workers=args.max_workers)
    
    # Get companies list
    companies = []
    if args.spreadsheet_id:
        companies = updater.get_companies_from_sheets(args.spreadsheet_id)
    elif args.companies_file:
        companies = updater.get_companies_from_file(args.companies_file)
    
    if not companies:
        print("❌ No companies found to process")
        return
    
    # Run batch update
    results = updater.run_batch_update(companies)
    
    # Save results if requested
    if args.save_results:
        updater.save_results_to_storage(results)


if __name__ == "__main__":
    main()
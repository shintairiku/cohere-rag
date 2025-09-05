#!/usr/bin/env python3
"""
Scheduler for Automatic Incremental Updates

This module provides scheduling functionality for automatic incremental updates.
It can be deployed as:
1. Cloud Run job with Cloud Scheduler
2. Local cron job
3. Kubernetes CronJob

Features:
- Configurable scheduling intervals
- Notification integrations (email, Slack, etc.)
- Health checks and monitoring
- Error reporting and alerting

Author: Claude Code Assistant  
Version: 1.0.0
"""

import os
import json
import logging
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, List

import requests
from dotenv import load_dotenv
from google.cloud import secretmanager

from .batch_incremental_updater import BatchIncrementalUpdater, BatchUpdateResults

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NotificationManager:
    """Manages notifications for scheduled updates."""
    
    def __init__(self):
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.notification_email = os.getenv("NOTIFICATION_EMAIL")
        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        
    def send_email_notification(self, subject: str, body: str, is_html: bool = False):
        """Send email notification."""
        if not all([self.smtp_host, self.smtp_user, self.smtp_password, self.notification_email]):
            logger.warning("Email configuration incomplete, skipping email notification")
            return
            
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = self.notification_email
            msg['Subject'] = subject
            
            content_type = 'html' if is_html else 'plain'
            msg.attach(MIMEText(body, content_type))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
                
            logger.info(f"Email notification sent: {subject}")
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def send_slack_notification(self, message: str, channel: str = None):
        """Send Slack notification."""
        if not self.slack_webhook_url:
            logger.warning("Slack webhook URL not configured, skipping Slack notification")
            return
            
        try:
            payload = {
                "text": message,
                "username": "Incremental Updater",
                "icon_emoji": ":robot_face:"
            }
            
            if channel:
                payload["channel"] = channel
                
            response = requests.post(self.slack_webhook_url, json=payload)
            response.raise_for_status()
            
            logger.info("Slack notification sent successfully")
            
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
    
    def notify_batch_results(self, results: BatchUpdateResults):
        """Send notifications for batch update results."""
        # Determine notification type based on results
        if results.failed_updates == 0 and results.total_errors == 0:
            status = "✅ SUCCESS"
            color = "good"
        elif results.failed_updates > 0:
            status = "❌ PARTIAL FAILURE"
            color = "warning"
        else:
            status = "⚠️ WITH ERRORS"
            color = "warning"
        
        # Email notification
        subject = f"Incremental Update Report - {status}"
        
        email_body = f"""
Incremental Update Completed - {results.end_time.strftime('%Y-%m-%d %H:%M:%S')}

📊 SUMMARY:
• Total Companies: {results.total_companies}
• Successful Updates: {results.successful_updates}
• Failed Updates: {results.failed_updates}
• Duration: {results.duration_seconds:.1f} seconds

📁 FILE CHANGES:
• Added: {results.total_files_added}
• Updated: {results.total_files_updated}  
• Removed: {results.total_files_removed}
• Errors: {results.total_errors}

"""
        
        if results.failed_updates > 0:
            email_body += "\n❌ FAILED COMPANIES:\n"
            for result in results.company_results:
                if not result['success']:
                    email_body += f"• {result['company']} ({result['uuid']}): {result['error']}\n"
        
        self.send_email_notification(subject, email_body)
        
        # Slack notification
        slack_message = f"""
{status} Incremental Update Report

📊 **Summary**: {results.successful_updates}/{results.total_companies} companies updated successfully
📁 **Changes**: +{results.total_files_added} -{results.total_files_removed} ~{results.total_files_updated}
⏱️ **Duration**: {results.duration_seconds:.1f}s
"""
        
        if results.failed_updates > 0:
            failed_companies = [r['company'] for r in results.company_results if not r['success']]
            slack_message += f"\n❌ **Failed**: {', '.join(failed_companies[:3])}"
            if len(failed_companies) > 3:
                slack_message += f" +{len(failed_companies) - 3} more"
        
        self.send_slack_notification(slack_message)


class ScheduledUpdater:
    """Main scheduler for incremental updates."""
    
    def __init__(self):
        self.bucket_name = os.getenv("GCS_BUCKET_NAME")
        self.spreadsheet_id = os.getenv("COMPANY_SPREADSHEET_ID")
        self.max_workers = int(os.getenv("MAX_WORKERS", "3"))
        self.notification_manager = NotificationManager()
        
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")
    
    def run_scheduled_update(self) -> BatchUpdateResults:
        """Run a scheduled incremental update."""
        logger.info("Starting scheduled incremental update")
        
        try:
            # Initialize updater
            updater = BatchIncrementalUpdater(self.bucket_name, max_workers=self.max_workers)
            
            # Get companies list
            companies = []
            if self.spreadsheet_id:
                logger.info(f"Loading companies from spreadsheet: {self.spreadsheet_id}")
                companies = updater.get_companies_from_sheets(self.spreadsheet_id)
            else:
                logger.warning("No spreadsheet ID configured, looking for companies.json")
                companies_file = "companies.json"
                if os.path.exists(companies_file):
                    companies = updater.get_companies_from_file(companies_file)
            
            if not companies:
                raise ValueError("No companies found to process")
            
            # Run batch update
            results = updater.run_batch_update(companies)
            
            # Save results
            updater.save_results_to_storage(results)
            
            # Send notifications
            self.notification_manager.notify_batch_results(results)
            
            logger.info("Scheduled update completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Scheduled update failed: {e}")
            
            # Send error notification
            error_subject = "❌ Incremental Update Failed"
            error_body = f"""
Scheduled incremental update failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Error: {str(e)}

Please check the logs for more details.
"""
            self.notification_manager.send_email_notification(error_subject, error_body)
            self.notification_manager.send_slack_notification(f"❌ Scheduled update failed: {str(e)}")
            
            raise
    
    def health_check(self) -> Dict:
        """Perform health check for the scheduler."""
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        # Check GCS bucket access
        try:
            from google.cloud import storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(self.bucket_name)
            bucket.exists()
            health_status["checks"]["gcs_bucket"] = "ok"
        except Exception as e:
            health_status["checks"]["gcs_bucket"] = f"error: {str(e)}"
            health_status["status"] = "unhealthy"
        
        # Check Sheets API access
        if self.spreadsheet_id:
            try:
                updater = BatchIncrementalUpdater(self.bucket_name)
                companies = updater.get_companies_from_sheets(self.spreadsheet_id)
                health_status["checks"]["sheets_api"] = f"ok ({len(companies)} companies)"
            except Exception as e:
                health_status["checks"]["sheets_api"] = f"error: {str(e)}"
                health_status["status"] = "unhealthy"
        
        # Check Cohere API
        try:
            import cohere
            cohere_api_key = os.getenv("COHERE_API_KEY")
            if cohere_api_key:
                co = cohere.Client(cohere_api_key)
                # Simple API test
                co.embed(texts=["test"], model="embed-english-v3.0")
                health_status["checks"]["cohere_api"] = "ok"
            else:
                health_status["checks"]["cohere_api"] = "error: API key not configured"
                health_status["status"] = "unhealthy"
        except Exception as e:
            health_status["checks"]["cohere_api"] = f"error: {str(e)}"
            health_status["status"] = "unhealthy"
        
        return health_status


def main():
    """Main entry point for scheduled execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Scheduled Incremental Updater")
    parser.add_argument("--mode", choices=["update", "health"], default="update", 
                       help="Mode: 'update' to run update, 'health' for health check")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Dry run mode (for testing)")
    
    args = parser.parse_args()
    
    try:
        scheduler = ScheduledUpdater()
        
        if args.mode == "health":
            health = scheduler.health_check()
            print(json.dumps(health, indent=2))
            
            if health["status"] != "healthy":
                exit(1)
                
        elif args.mode == "update":
            if args.dry_run:
                logger.info("DRY RUN MODE - no actual updates will be performed")
                # In dry run, just validate configuration and connectivity
                health = scheduler.health_check()
                if health["status"] == "healthy":
                    logger.info("✅ Dry run passed - system is ready for updates")
                else:
                    logger.error("❌ Dry run failed - system health check failed")
                    exit(1)
            else:
                results = scheduler.run_scheduled_update()
                logger.info(f"Update completed: {results.successful_updates}/{results.total_companies} companies")
                
    except Exception as e:
        logger.error(f"Scheduler failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
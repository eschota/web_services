"""
Task worker for CGTrader Submit Server.
Single-threaded queue processor with checkpoint-based recovery.
"""
import os
import time
import shutil
import zipfile
import traceback
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import requests

from config import TMP_DIR, DOWNLOAD_TIMEOUT_SECONDS, MAX_TASK_ATTEMPTS
import database as db
from database import (
    STATUS_CREATED, STATUS_DOWNLOADING, STATUS_EXTRACTING,
    STATUS_PREPARING, STATUS_ANALYZING, STATUS_UPLOADING, STATUS_FILLING_FORM,
    STATUS_PUBLISHING, STATUS_DONE, STATUS_ERROR
)
import telegram_notifier as tg
from metadata_extractor import generate_full_metadata
from file_preparer import prepare_files_for_batch_upload
from cgtrader_http import CGTraderHTTPClient


class TaskWorker:
    """Single-threaded task worker with checkpoint recovery."""
    
    def __init__(self):
        self.running = False
        self.current_task: Optional[Dict[str, Any]] = None
        self.automation: Optional[CGTraderHTTPClient] = None
    
    def start(self):
        """Start the worker loop."""
        self.running = True
        print("[Worker] Starting...")
        
        # Notify service start
        try:
            tg.notify_service_start()
        except Exception as e:
            print(f"[Worker] Failed to send start notification: {e}")
        
        # Recover interrupted tasks
        self._recover_interrupted_tasks()
        
        # Main loop
        while self.running:
            try:
                self._process_next_task()
            except Exception as e:
                print(f"[Worker] Loop error: {e}")
                traceback.print_exc()
            
            # Sleep between iterations
            time.sleep(5)
    
    def stop(self):
        """Stop the worker."""
        self.running = False
        # HTTP client doesn't need explicit stop
        self.automation = None
        print("[Worker] Stopped")
    
    def _recover_interrupted_tasks(self):
        """Find and recover interrupted tasks."""
        interrupted = db.get_interrupted_tasks()
        
        if interrupted:
            print(f"[Worker] Found {len(interrupted)} interrupted task(s)")
            for task in interrupted:
                print(f"[Worker] Will recover: {task['id']} (status: {task['status']}, step: {task.get('step')})")
    
    def _process_next_task(self):
        """Get and process the next pending task."""
        task = db.get_next_pending_task()
        
        if not task:
            return  # No pending tasks
        
        self.current_task = task
        task_id = task["id"]
        
        print(f"[Worker] Processing task: {task_id}")
        
        try:
            # Increment attempt counter
            attempts = db.increment_attempts(task_id)
            
            if attempts > task.get("max_attempts", MAX_TASK_ATTEMPTS):
                print(f"[Worker] Max attempts exceeded for {task_id}")
                db.update_task_status(
                    task_id, STATUS_ERROR,
                    error_message=f"Max attempts ({attempts}) exceeded"
                )
                return
            
            # Process based on current status (checkpoint recovery)
            status = task["status"]
            
            if status == STATUS_CREATED:
                self._step_download(task)
            elif status == STATUS_DOWNLOADING:
                # Resume or restart download
                if task.get("download_path") and os.path.exists(task["download_path"]):
                    self._step_extract(task)
                else:
                    self._step_download(task)
            elif status == STATUS_EXTRACTING:
                # Resume or restart extract
                if task.get("extract_path") and os.path.exists(task["extract_path"]):
                    self._step_prepare(task)
                else:
                    self._step_extract(task)
            elif status == STATUS_PREPARING:
                # Resume or restart prepare
                if task.get("prepared_path") and os.path.exists(task["prepared_path"]):
                    self._step_analyze(task)
                else:
                    self._step_prepare(task)
            elif status == STATUS_ANALYZING:
                # Resume analyze
                if task.get("metadata_json"):
                    self._step_upload(task)
                else:
                    self._step_analyze(task)
            elif status == STATUS_UPLOADING:
                # Restart upload (can't really resume)
                self._step_upload(task)
            elif status == STATUS_FILLING_FORM:
                self._step_upload(task)  # Restart from upload
            elif status == STATUS_PUBLISHING:
                self._step_upload(task)  # Restart from upload
                
        except Exception as e:
            self._handle_error(task_id, task.get("status", "unknown"), e, task)
        finally:
            self.current_task = None
    
    def _step_download(self, task: Dict[str, Any]):
        """Download the ZIP file."""
        task_id = task["id"]
        input_url = task["input_url"]
        
        print(f"[Worker] Step: DOWNLOAD - {input_url}")
        db.update_task_status(task_id, STATUS_DOWNLOADING, step="downloading")
        
        # Create task directory
        task_dir = os.path.join(TMP_DIR, task_id)
        os.makedirs(task_dir, exist_ok=True)
        
        # Extract filename from URL
        parsed = urlparse(input_url)
        filename = os.path.basename(parsed.path) or "download.zip"
        download_path = os.path.join(task_dir, filename)
        
        # Download file
        print(f"[Worker] Downloading to {download_path}...")
        
        response = requests.get(
            input_url,
            stream=True,
            timeout=DOWNLOAD_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        
        with open(download_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                
                # Progress (every 10%)
                if total_size > 0 and downloaded % (total_size // 10 + 1) < 8192:
                    pct = int(downloaded / total_size * 100)
                    print(f"[Worker] Download progress: {pct}%")
        
        print(f"[Worker] Downloaded {downloaded} bytes")
        
        # Update checkpoint
        db.update_task_status(
            task_id, STATUS_DOWNLOADING,
            step="downloaded",
            download_path=download_path
        )
        
        # Continue to extract
        task["download_path"] = download_path
        self._step_extract(task)
    
    def _step_extract(self, task: Dict[str, Any]):
        """Extract the ZIP file."""
        task_id = task["id"]
        download_path = task.get("download_path")
        
        if not download_path or not os.path.exists(download_path):
            raise FileNotFoundError(f"Download file not found: {download_path}")
        
        print(f"[Worker] Step: EXTRACT - {download_path}")
        db.update_task_status(task_id, STATUS_EXTRACTING, step="extracting")
        
        # Extract to directory
        task_dir = os.path.dirname(download_path)
        extract_path = os.path.join(task_dir, "extracted")
        
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        os.makedirs(extract_path)
        
        # Extract ZIP
        print(f"[Worker] Extracting to {extract_path}...")
        
        with zipfile.ZipFile(download_path, "r") as zf:
            zf.extractall(extract_path)
        
        # Count extracted files
        file_count = sum(1 for _ in Path(extract_path).rglob("*") if _.is_file())
        print(f"[Worker] Extracted {file_count} files")
        
        # Update checkpoint
        db.update_task_status(
            task_id, STATUS_EXTRACTING,
            step="extracted",
            extract_path=extract_path
        )
        
        # Continue to prepare
        task["extract_path"] = extract_path
        self._step_prepare(task)
    
    def _step_prepare(self, task: Dict[str, Any]):
        """Prepare files for batch upload: extract views, archive subfolders."""
        task_id = task["id"]
        extract_path = task.get("extract_path")
        
        if not extract_path or not os.path.exists(extract_path):
            raise FileNotFoundError(f"Extract path not found: {extract_path}")
        
        print(f"[Worker] Step: PREPARE - {extract_path}")
        db.update_task_status(task_id, STATUS_PREPARING, step="preparing")
        
        # Prepare output directory
        task_dir = os.path.dirname(extract_path)
        prepared_path = os.path.join(task_dir, "prepared")
        
        # Prepare files for batch upload
        print("[Worker] Preparing files for batch upload...")
        prepared_folder = prepare_files_for_batch_upload(extract_path, prepared_path)
        
        print(f"[Worker] Files prepared in: {prepared_folder}")
        
        # Update checkpoint
        db.update_task_status(
            task_id, STATUS_PREPARING,
            step="prepared",
            prepared_path=prepared_folder
        )
        
        # Continue to analyze (use original extract_path for metadata extraction)
        task["prepared_path"] = prepared_folder
        self._step_analyze(task)
    
    def _step_analyze(self, task: Dict[str, Any]):
        """Analyze the model and generate metadata."""
        task_id = task["id"]
        extract_path = task.get("extract_path")
        
        if not extract_path or not os.path.exists(extract_path):
            raise FileNotFoundError(f"Extract path not found: {extract_path}")
        
        print(f"[Worker] Step: ANALYZE - {extract_path}")
        db.update_task_status(task_id, STATUS_ANALYZING, step="analyzing")
        
        # Generate metadata using OpenAI Vision
        print("[Worker] Generating metadata with OpenAI Vision...")
        metadata = generate_full_metadata(extract_path)
        
        print(f"[Worker] Generated metadata: {metadata.get('title')}")
        
        # Update checkpoint
        db.update_task_status(
            task_id, STATUS_ANALYZING,
            step="analyzed",
            metadata_json=metadata
        )
        
        # Continue to upload
        task["metadata_json"] = metadata
        self._step_upload(task)
    
    def _step_upload(self, task: Dict[str, Any]):
        """Upload to CGTrader and fill form."""
        task_id = task["id"]
        prepared_path = task.get("prepared_path")
        metadata = task.get("metadata_json")
        
        # Fallback to extract_path if prepared_path not set (for recovery)
        if not prepared_path:
            extract_path = task.get("extract_path")
            if extract_path and os.path.exists(extract_path):
                # Re-prepare
                task_dir = os.path.dirname(extract_path)
                prepared_path = os.path.join(task_dir, "prepared")
                prepared_path = prepare_files_for_batch_upload(extract_path, prepared_path)
                task["prepared_path"] = prepared_path
        
        if not prepared_path or not os.path.exists(prepared_path):
            raise FileNotFoundError(f"Prepared path not found: {prepared_path}")
        
        if not metadata:
            # Try to load from DB
            task_data = db.get_task(task_id)
            if task_data and task_data.get("metadata_json"):
                import json
                metadata = json.loads(task_data["metadata_json"])
            else:
                raise ValueError("No metadata available")
        
        print(f"[Worker] Step: UPLOAD - {prepared_path}")
        db.update_task_status(task_id, STATUS_UPLOADING, step="uploading")
        
        # Create HTTP client instance
        if self.automation is None:
            self.automation = CGTraderHTTPClient()
        
        try:
            # Full upload flow
            print("[Worker] Starting CGTrader upload flow...")
            
            # Login
            print("[Worker] Logging in to CGTrader...")
            if not self.automation.login():
                raise Exception("Failed to login to CGTrader")
            
            db.update_task_status(task_id, STATUS_UPLOADING, step="logged_in")
            
            # Upload files (use prepared folder)
            print("[Worker] Uploading prepared folder...")
            draft_id = self.automation.upload_files(prepared_path)
            if not draft_id:
                raise Exception("Failed to upload files to CGTrader")
            
            db.update_task_status(task_id, STATUS_FILLING_FORM, step="files_uploaded", cgtrader_draft_id=draft_id)
            
            # Submit metadata
            print("[Worker] Submitting metadata...")
            if not self.automation.submit_metadata(draft_id, metadata):
                raise Exception("Failed to submit metadata to CGTrader")
            
            db.update_task_status(task_id, STATUS_PUBLISHING, step="metadata_submitted")
            
            # Publish
            print("[Worker] Publishing...")
            product_url = self.automation.publish(draft_id)
            
            if not product_url:
                raise Exception("Failed to publish model on CGTrader")
            
            # Success!
            db.update_task_status(
                task_id, STATUS_DONE,
                step="published",
                cgtrader_product_url=product_url
            )
            
            print(f"[Worker] Task completed: {task_id}")
            print(f"[Worker] Product URL: {product_url}")
            
            # Notify success
            tg.notify_task_done(task_id, product_url)
            
            # Cleanup temp files
            self._cleanup_task_files(task_id)
            
        finally:
            # Cleanup (HTTP client doesn't need explicit stop, but reset reference)
            self.automation = None
    
    def _handle_error(self, task_id: str, step: str, error: Exception, task: Dict[str, Any]):
        """Handle task error."""
        error_msg = str(error)
        print(f"[Worker] Error in task {task_id} at step {step}: {error_msg}")
        traceback.print_exc()
        
        # Get current attempt count
        task_data = db.get_task(task_id)
        attempts = task_data.get("attempts", 1) if task_data else 1
        max_attempts = task_data.get("max_attempts", MAX_TASK_ATTEMPTS) if task_data else MAX_TASK_ATTEMPTS
        
        # Update status
        db.update_task_status(
            task_id, STATUS_ERROR,
            step=step,
            error_message=error_msg
        )
        
        # Send error notification
        tg.notify_task_error(
            task_id=task_id,
            step=step,
            error=error,
            input_url=task.get("input_url", ""),
            attempts=attempts,
            max_attempts=max_attempts
        )
        
        # Reset automation reference (HTTP client doesn't need explicit stop)
        self.automation = None
    
    def _cleanup_task_files(self, task_id: str):
        """Clean up temporary files for a completed task."""
        task_dir = os.path.join(TMP_DIR, task_id)
        
        if os.path.exists(task_dir):
            try:
                shutil.rmtree(task_dir)
                print(f"[Worker] Cleaned up: {task_dir}")
            except Exception as e:
                print(f"[Worker] Cleanup error: {e}")


# Singleton worker
_worker: Optional[TaskWorker] = None


def get_worker() -> TaskWorker:
    """Get or create worker instance."""
    global _worker
    if _worker is None:
        _worker = TaskWorker()
    return _worker


def run_worker():
    """Run the worker (blocking)."""
    worker = get_worker()
    try:
        worker.start()
    except KeyboardInterrupt:
        print("[Worker] Interrupted")
        worker.stop()


def run_worker_thread():
    """Run worker in a background thread."""
    import threading
    
    def _run():
        try:
            run_worker()
        except Exception as e:
            print(f"[Worker] Thread error: {e}")
            traceback.print_exc()
    
    thread = threading.Thread(target=_run, daemon=True, name="cgtrader-worker")
    thread.start()
    return thread


if __name__ == "__main__":
    run_worker()

"""
Flask API server for CGTrader Submit Server.
"""
import re
from flask import Flask, request, jsonify

from config import APP_PORT, DEBUG
import database as db
from database import STATUS_ERROR
import telegram_notifier as tg
from worker import run_worker_thread

app = Flask(__name__)


def validate_zip_url(url: str) -> bool:
    """Validate that URL points to a ZIP file."""
    if not url:
        return False
    
    # Check URL format
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    if not url_pattern.match(url):
        return False
    
    # Check .zip extension
    if not url.lower().endswith('.zip'):
        return False
    
    return True


@app.route('/api-submit-cgtrader', methods=['POST'])
def submit_task():
    """
    Create a new CGTrader submission task.
    
    Request JSON:
        {
            "input_url": "https://example.com/model.zip"
        }
    
    Response:
        {
            "task_id": "uuid",
            "status": "created"
        }
    """
    data = request.get_json()
    
    if not data:
        return jsonify({
            "error": "Request body required",
            "message": "Please provide JSON with 'input_url' field"
        }), 400
    
    input_url = data.get("input_url", "").strip()
    
    if not input_url:
        return jsonify({
            "error": "Missing input_url",
            "message": "The 'input_url' field is required"
        }), 400
    
    if not validate_zip_url(input_url):
        return jsonify({
            "error": "Invalid input_url",
            "message": "URL must be a valid HTTP/HTTPS URL ending with .zip"
        }), 400
    
    # Create task
    task_id = db.create_task(input_url)
    
    # Notify via Telegram
    try:
        tg.notify_new_task(task_id, input_url)
    except Exception as e:
        print(f"[API] Failed to send Telegram notification: {e}")
    
    return jsonify({
        "task_id": task_id,
        "status": "created",
        "message": "Task created successfully"
    }), 201


@app.route('/api-submit-cgtrader/status/<task_id>', methods=['GET'])
def get_task_status(task_id: str):
    """
    Get status of a specific task.
    
    Response:
        {
            "task_id": "uuid",
            "status": "created|downloading|extracting|analyzing|uploading|filling_form|publishing|done|error",
            "step": "current step",
            "error": "error message if any",
            "attempts": 1,
            "created_at": "timestamp",
            "updated_at": "timestamp"
        }
    """
    task = db.get_task(task_id)
    
    if not task:
        return jsonify({
            "error": "Task not found",
            "task_id": task_id
        }), 404
    
    response = {
        "task_id": task["id"],
        "status": task["status"],
        "step": task.get("step"),
        "attempts": task["attempts"],
        "max_attempts": task["max_attempts"],
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
    }
    
    if task.get("error_message"):
        response["error"] = task["error_message"]
    
    if task.get("cgtrader_product_url"):
        response["product_url"] = task["cgtrader_product_url"]
    
    if task.get("started_at"):
        response["started_at"] = task["started_at"]
    
    if task.get("completed_at"):
        response["completed_at"] = task["completed_at"]
    
    return jsonify(response)


@app.route('/api-submit-cgtrader/queue', methods=['GET'])
def get_queue():
    """
    Get current queue status.
    
    Response:
        {
            "queue_length": 5,
            "total": 100,
            "counts": {"created": 5, "done": 90, "error": 5},
            "processing": {...},
            "pending": [...]
        }
    """
    queue = db.get_queue_status()
    return jsonify(queue)


@app.route('/api-submit-cgtrader/retry/<task_id>', methods=['POST'])
def retry_task(task_id: str):
    """
    Retry a failed task.
    
    Response:
        {
            "task_id": "uuid",
            "status": "created",
            "message": "Task queued for retry"
        }
    """
    task = db.get_task(task_id)
    
    if not task:
        return jsonify({
            "error": "Task not found",
            "task_id": task_id
        }), 404
    
    if task["status"] != STATUS_ERROR:
        return jsonify({
            "error": "Cannot retry",
            "message": f"Task status is '{task['status']}', only 'error' tasks can be retried"
        }), 400
    
    if db.retry_task(task_id):
        return jsonify({
            "task_id": task_id,
            "status": "created",
            "message": "Task queued for retry"
        })
    else:
        return jsonify({
            "error": "Retry failed",
            "task_id": task_id
        }), 500


@app.route('/api-submit-cgtrader/recent', methods=['GET'])
def get_recent_tasks():
    """
    Get recent tasks.
    
    Query params:
        limit: number of tasks (default 20, max 100)
    
    Response:
        {
            "tasks": [...]
        }
    """
    limit = request.args.get("limit", 20, type=int)
    limit = min(max(1, limit), 100)
    
    tasks = db.get_recent_tasks(limit)
    
    # Simplify response
    result = []
    for task in tasks:
        result.append({
            "task_id": task["id"],
            "status": task["status"],
            "step": task.get("step"),
            "input_url": task["input_url"],
            "created_at": task["created_at"],
            "error": task.get("error_message"),
            "product_url": task.get("cgtrader_product_url"),
        })
    
    return jsonify({"tasks": result})


@app.route('/api-submit-cgtrader/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    queue = db.get_queue_status()
    
    return jsonify({
        "status": "ok",
        "service": "cgtrader-submit",
        "queue_length": queue["queue_length"],
        "total_tasks": queue["total"]
    })


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with API info."""
    return jsonify({
        "service": "CGTrader Submit Server",
        "version": "1.0.0",
        "endpoints": {
            "POST /api-submit-cgtrader": "Create new task",
            "GET /api-submit-cgtrader/status/<id>": "Get task status",
            "GET /api-submit-cgtrader/queue": "Get queue status",
            "POST /api-submit-cgtrader/retry/<id>": "Retry failed task",
            "GET /api-submit-cgtrader/recent": "Get recent tasks",
            "GET /api-submit-cgtrader/health": "Health check"
        }
    })


def main():
    """Start the Flask server and worker."""
    print(f"[App] Starting CGTrader Submit Server on port {APP_PORT}...")
    
    # Start worker thread
    worker_thread = run_worker_thread()
    print("[App] Worker thread started")
    
    # Start Telegram bot (optional, for commands)
    try:
        tg.run_bot_async()
        print("[App] Telegram bot started")
    except Exception as e:
        print(f"[App] Telegram bot failed to start: {e}")
    
    # Run Flask
    app.run(
        host="127.0.0.1",
        port=APP_PORT,
        debug=DEBUG,
        threaded=True,
        use_reloader=False  # Don't use reloader with background threads
    )


if __name__ == "__main__":
    main()

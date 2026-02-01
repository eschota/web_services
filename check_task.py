import sqlite3
import json

db_path = "/opt/autorig-online/autorig-online/backend/autorig.db"
task_id = "393d50ed-3bb9-4713-98b9-d025ae3f26ab"

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT id, status, guid, worker_api, ready_urls FROM tasks WHERE id = ?", (task_id,))
    row = cursor.fetchone()
    if row:
        print(json.dumps({
            "id": row[0],
            "status": row[1],
            "guid": row[2],
            "worker_api": row[3],
            "ready_urls": row[4]
        }, indent=2))
    else:
        print(f"Task {task_id} not found")
    conn.close()
except Exception as e:
    print(f"Error: {e}")

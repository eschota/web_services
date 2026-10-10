"""Read-only sanitized V3 migration inventory. Never submits a task or restarts.

Run on the VPS with production Python. JSON contains counts/build hashes only;
no source URLs, user identities, secret values or individual customer records.
"""
import hashlib
import json
import pathlib
import sqlite3
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timezone


def inventory():
    root = pathlib.Path("/srv/autorig/current").resolve()
    result = {"observed_at": datetime.now(timezone.utc).isoformat(),
              "release": str(root), "services": {}, "backend_files": {}}
    for unit in ("autorig-storage.service", "autorig-storage-renderfin.service", "autorig-mt.service"):
        pid = int(subprocess.check_output(["systemctl", "show", unit, "-p", "MainPID", "--value"], text=True).strip())
        item = {"pid": pid}
        if pid:
            env = pathlib.Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
            flag = next((entry.split(b"=", 1)[1].decode("ascii", "replace") for entry in env
                         if entry.startswith(b"AUTORIG_WIPE_QUEUE_ON_START=")), None)
            # Absence does not mean disabled: source defaults must also be checked.
            item["wipe_queue_env"] = flag
        result["services"][unit] = item
    for name in ("tasks.py", "v3_pipeline_adapter.py", "v3_task_store.py", "v3_callback_receiver.py"):
        path = root / "autorig-online/backend" / name
        result["backend_files"][name] = hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
    request = urllib.request.Request("http://127.0.0.1:8251/api/mt/v3", headers={"User-Agent": "AutoRigAudit/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            result["v3_dispatch_http"] = response.status
            document = json.load(response)
            result["v3_schema"] = document.get("schema")
            result["v3_intents"] = document.get("intents")
    except urllib.error.HTTPError as exc:
        result["v3_dispatch_http"] = exc.code
    except (OSError, ValueError) as exc:
        result["v3_probe_error"] = type(exc).__name__
    connection = sqlite3.connect("file:/srv/autorig/data/db/autorig.db?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        result["task_counts"] = [{"status": status, "pipeline_kind": kind, "count": count}
                                 for status, kind, count in connection.execute(
                                     "SELECT status,pipeline_kind,count(*) FROM tasks GROUP BY status,pipeline_kind")]
        result["v3_tables"] = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'v3_%'")]
    finally:
        connection.close()
    renderfin = sqlite3.connect("file:/srv/autorig/data/var/renderfin/db/renderfin.db?mode=ro", uri=True)
    try:
        renderfin.execute("PRAGMA query_only=ON")
        result["renderfin_tables"] = [row[0] for row in renderfin.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")]
        result["renderfin_status_counts"] = {}
        for table in ("tasks", "render_tasks"):
            if table not in result["renderfin_tables"]:
                continue
            columns = {row[1] for row in renderfin.execute(f'PRAGMA table_info("{table}")')}
            if "status" in columns:
                result["renderfin_status_counts"][table] = dict(renderfin.execute(
                    f'SELECT status,count(*) FROM "{table}" GROUP BY status'))
    finally:
        renderfin.close()
    return result


if __name__ == "__main__":
    print(json.dumps(inventory(), indent=2, sort_keys=True))

"""Read-only, identifier-free diagnostic summary from the dedicated SandFlow DB."""
import json
import sqlite3

with sqlite3.connect("file:/srv/sandflow/data/telemetry.db?mode=ro", uri=True) as db:
    rows = db.execute("SELECT received,json FROM device_reports ORDER BY received DESC LIMIT 2").fetchall()
print(json.dumps([
    {"receivedUnix": received, **{key: value for key, value in json.loads(payload).items() if key != "sessionId"}}
    for received, payload in rows
], indent=2))

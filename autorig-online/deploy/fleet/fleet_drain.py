"""Drain or restore one converter box for a maintenance deploy (run on the VPS with sudo).

    sudo python3 fleet_drain.py drain f13 "V3 converter deploy"
    sudo python3 fleet_drain.py status f13
    sudo python3 fleet_drain.py restore f13

drain:   stops new work reaching the box without touching what it is doing:
         worker_endpoints.enabled=0 (AutoRig dispatch, read live by the backend),
         registry enabled=false (Hunyuan) and ai_vision_enabled=false (LLM routing),
         both read on every admission pass. The previous values are saved first.
status:  what the box still holds (converter processing/pending, Hunyuan, DB rows).
restore: puts back exactly the saved values.

Secrets never leave the registry file: the token is used only for the status probe.
"""

from __future__ import annotations

import datetime as _dt
import grp
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import urllib.request

REGISTRY = "/srv/autorig/secrets/renderfin-hunyuan.json"
DB = "/srv/autorig/data/db/autorig.db"
STATE = "/srv/autorig/data/var/fleet"
NOTES = os.path.join(STATE, "operator_notes.json")
ENDPOINT_MARK = "converter-{box}.freestock.online"


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json_atomic(path: str, data, owner: str = "autorig", mode: int = 0o644) -> None:
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=1)
    shutil.chown(tmp, owner, "autorig")
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def _registry_entry(data, box: str):
    for entry in data.get("workers") or []:
        if str(entry.get("name")) == box:
            return entry
    raise SystemExit(f"no registry entry named {box}")


def _db(sql: str, params=(), write: bool = False):
    if write:
        # Write as the database owner so WAL/shm files keep their owner.
        script = ("import sqlite3,sys,json;c=sqlite3.connect(sys.argv[1],timeout=15);"
                  "c.execute('PRAGMA busy_timeout=15000');cur=c.execute(sys.argv[2],json.loads(sys.argv[3]));"
                  "c.commit();print(cur.rowcount)")
        out = subprocess.run(["sudo", "-n", "-u", "autorig", "python3", "-c", script, DB, sql, json.dumps(list(params))],
                             capture_output=True, text=True, timeout=30, check=True)
        return int(out.stdout.strip() or 0)
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=15)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _endpoint_rows(box: str):
    mark = "%" + ENDPOINT_MARK.format(box=box) + "%"
    return _db("SELECT id, url, enabled FROM worker_endpoints WHERE url LIKE ?", (mark,))


def _note(box: str, text: str | None) -> None:
    try:
        notes = json.load(open(NOTES, encoding="utf-8"))
    except Exception:
        notes = {}
    if text is None:
        if notes.get(box, {}).get("by") == "fleet_drain":
            notes.pop(box, None)
    else:
        notes[box] = {"text": text, "blocking": False, "since": _now(), "by": "fleet_drain"}
    _write_json_atomic(NOTES, notes)


def status(box: str) -> dict:
    data = json.load(open(REGISTRY, encoding="utf-8"))
    entry = _registry_entry(data, box)
    out = {"box": box, "registry_enabled": entry.get("enabled"),
           "ai_vision_enabled": entry.get("ai_vision_enabled", True),
           "worker_endpoints": [list(r) for r in _endpoint_rows(box)]}
    mark = "%" + ENDPOINT_MARK.format(box=box) + "%"
    out["db_processing"] = [r[0][:8] for r in _db(
        "SELECT id FROM tasks WHERE status='processing' AND worker_api LIKE ?", (mark,))]
    try:
        request = urllib.request.Request(entry["url"].rstrip("/") + "/api-converter-glb/server-status",
                                         headers={"Authorization": "Bearer " + entry["token"]})
        st = json.loads(urllib.request.urlopen(request, timeout=20).read())
        summary = st.get("tasks_summary") or {}
        out["converter"] = {
            "processing": int(summary.get("processing") or 0),
            "pending": int(summary.get("pending") or 0),
            "queue_size": int(summary.get("queue_size") or 0),
            "hunyuan_active": bool((st.get("hunyuan") or {}).get("active_task")),
            "processing_modes": [t.get("workload_class") or t.get("mode") or t.get("type")
                                 for t in st.get("processing_tasks") or []],
            "pending_modes": [t.get("workload_class") or t.get("mode") for t in st.get("pending_tasks") or []],
            "deploy_commit": st.get("deploy_commit"), "boot_build_id": st.get("boot_build_id"),
        }
        conv = out["converter"]
        out["idle"] = (conv["processing"] == 0 and conv["pending"] == 0 and conv["queue_size"] == 0
                       and not conv["hunyuan_active"] and not out["db_processing"])
    except Exception as exc:
        out["converter_error"] = f"{type(exc).__name__}: {exc}"[:200]
        out["idle"] = False
    return out


def drain(box: str, reason: str) -> dict:
    saved_path = os.path.join(STATE, f"drain-{box}.json")
    if os.path.exists(saved_path):
        raise SystemExit(f"{saved_path} exists: box already drained; restore first")
    data = json.load(open(REGISTRY, encoding="utf-8"))
    entry = _registry_entry(data, box)
    saved = {"box": box, "reason": reason, "drained_at": _now(),
             "registry": {"enabled": entry.get("enabled"),
                          "ai_vision_enabled_present": "ai_vision_enabled" in entry,
                          "ai_vision_enabled": entry.get("ai_vision_enabled")},
             "worker_endpoints": [list(r) for r in _endpoint_rows(box)]}
    _write_json_atomic(saved_path, saved)
    stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shutil.copy2(REGISTRY, f"/srv/autorig/secrets/backups/renderfin-hunyuan.json.bak-drain-{box}-{stamp}")
    entry["enabled"] = False
    entry["ai_vision_enabled"] = False
    tmp = REGISTRY + f".tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    os.chown(tmp, 0, grp.getgrnam("autorig").gr_gid)
    os.chmod(tmp, 0o640)
    os.replace(tmp, REGISTRY)
    for row in saved["worker_endpoints"]:
        if int(row[2] or 0):
            _db("UPDATE worker_endpoints SET enabled=0, updated_at=datetime('now') WHERE id=?", (row[0],), write=True)
    _note(box, f"drained for {reason}: no new AutoRig, Hunyuan or LLM work; running tasks finish first")
    return status(box)


def restore(box: str) -> dict:
    saved_path = os.path.join(STATE, f"drain-{box}.json")
    saved = json.load(open(saved_path, encoding="utf-8"))
    data = json.load(open(REGISTRY, encoding="utf-8"))
    entry = _registry_entry(data, box)
    entry["enabled"] = saved["registry"]["enabled"]
    if saved["registry"]["ai_vision_enabled_present"]:
        entry["ai_vision_enabled"] = saved["registry"]["ai_vision_enabled"]
    else:
        entry.pop("ai_vision_enabled", None)
    tmp = REGISTRY + f".tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    os.chown(tmp, 0, grp.getgrnam("autorig").gr_gid)
    os.chmod(tmp, 0o640)
    os.replace(tmp, REGISTRY)
    for row in saved["worker_endpoints"]:
        _db("UPDATE worker_endpoints SET enabled=?, updated_at=datetime('now') WHERE id=?",
            (int(row[2] or 0), row[0]), write=True)
    os.replace(saved_path, saved_path + f".restored-{_dt.datetime.now(_dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    _note(box, None)
    return status(box)


def main() -> None:
    action, box = sys.argv[1], sys.argv[2]
    if action == "drain":
        print(json.dumps(drain(box, sys.argv[3] if len(sys.argv) > 3 else "maintenance"), ensure_ascii=False))
    elif action == "restore":
        print(json.dumps(restore(box), ensure_ascii=False))
    elif action == "status":
        print(json.dumps(status(box), ensure_ascii=False))
    else:
        raise SystemExit("usage: fleet_drain.py drain|status|restore <box> [reason]")


if __name__ == "__main__":
    main()

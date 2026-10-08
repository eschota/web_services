#!/usr/bin/env python3
"""Manifest-gated pruning of old terminal AutoRig tasks and owned artifacts."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import stat
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

TERMINAL = {"done", "error", "cancelled", "canceled", "failed"}
RENDERFIN_TERMINAL = {"submitted", "discarded", "failed", "error", "cancelled", "canceled"}
OPS = ("artifact_cache_jobs", "task_animation_corrections", "task_likes", "task_completion_emails")
DEFAULT_ROOTS = {
    "tasks": "/srv/autorig/data/static/tasks", "glb_cache": "/srv/autorig/data/static/glb_cache",
    "videos": "/srv/autorig/data/var/videos", "preflight": "/srv/autorig/data/var/preflight-renders",
    "artifact_cache": "/srv/autorig/data/artifact-cache",
    "deliverables": "/srv/autorig/data/var/deliverables", "uploads": "/srv/autorig/data/var/uploads",
}
DEFAULT_REFERENCE_ROOTS = ("/srv/autorig/data/var/ai-graphs", "/srv/autorig/data/var/ai-avatars", "/srv/autorig/data/var/ai-avatar-build")
SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
UUID_PATTERN = re.compile(r"(?i)(?<![0-9a-f])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-f])")
IMMUTABLE_TABLE_MARKERS = ("purchase", "checkout", "gumroad", "crypto", "ledger", "rig_completion", "youtube")


def cutoff(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("cutoff must be timezone-aware ISO")
    return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None).isoformat(sep=" ")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def ro_connect(path):
    target = Path(path)
    if not target.is_file():
        raise FileNotFoundError(target)
    conn = sqlite3.connect(target.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def tables(conn):
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def columns(conn, table):
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]


def rows(conn, sql, args=()):
    return [dict(row) for row in conn.execute(sql, args)]


def text_blob(row):
    return "\n".join(unquote(value) for value in row.values() if isinstance(value, str))


def safe_component(value, label):
    value = str(value or "")
    if not SAFE_COMPONENT.fullmatch(value) or value in {".", ".."}:
        raise ValueError(f"unsafe {label}")
    return value


def identifier_refs(blob, candidate_rows):
    decoded = unquote(blob)
    lookup = {}
    fallback = []
    for row in candidate_rows:
        values = (str(row["id"]), str(row.get("guid") or ""))
        if all(not value or UUID_PATTERN.fullmatch(value) for value in values):
            for value in values:
                if value:
                    lookup.setdefault(value.lower(), set()).add(str(row["id"]))
        else:
            fallback.append(row)
    found = set()
    for value in UUID_PATTERN.findall(decoded):
        found.update(lookup.get(value.lower(), ()))
    # Production task identifiers are UUIDs. This bounded fallback only supports
    # legacy/synthetic short identifiers without restoring an N x payload scan.
    if len(fallback) <= 256:
        for row in fallback:
            if str(row["id"]) in decoded or (row.get("guid") and str(row["guid"]) in decoded):
                found.add(str(row["id"]))
    elif fallback:
        raise ValueError("too many non-UUID task identifiers for safe reference scan")
    return found


def json_source_ids(value, candidates):
    found = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "source_task_id" and str(child) in candidates:
                found.add(str(child))
            found.update(json_source_ids(child, candidates))
    elif isinstance(value, list):
        for child in value:
            found.update(json_source_ids(child, candidates))
    return found


def assert_no_symlink_path(path, root):
    path, root = Path(path).absolute(), Path(root).absolute()
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"path outside root: {path}") from exc
    root_stat = os.lstat(root)
    if stat.S_ISLNK(root_stat.st_mode):
        raise ValueError(f"symlink root rejected: {root}")
    current = root
    for component in relative.parts:
        current /= component
        info = os.lstat(current)
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f"symlink rejected: {current}")
        if info.st_dev != root_stat.st_dev:
            raise ValueError(f"mount/device boundary rejected: {current}")


def checked_root(path):
    root = Path(path).absolute()
    if not root.is_dir():
        raise ValueError(f"unsafe root: {path}")
    assert_no_symlink_path(root, root)
    return root


def protected_references(conn, candidate_rows, renderfin_db, reference_roots=()):
    candidates = {str(row["id"]) for row in candidate_rows}
    reasons = {task_id: set() for task_id in candidates}
    existing = tables(conn)
    for table in existing - set(OPS) - {"tasks"}:
        for fk in conn.execute(f'PRAGMA foreign_key_list("{table}")'):
            if str(fk[2]).lower() != "tasks":
                continue
            source_column = str(fk[3])
            for row in conn.execute(f'SELECT "{source_column}" FROM "{table}" WHERE "{source_column}" IS NOT NULL'):
                if str(row[0]) in candidates:
                    reasons[str(row[0])].add(f"foreign_key:{table}.{source_column}")
    scan = [table for table in existing if table == "scenes" or any(key in table.lower() for key in ("ai_graph", "avatar", "scene_model"))]
    for table in scan:
        for row in rows(conn, f'SELECT * FROM "{table}"'):
            for task_id in identifier_refs(text_blob(row), candidate_rows):
                reasons[task_id].add(f"payload:{table}")
    if renderfin_db:
        renderfin = ro_connect(renderfin_db)
        try:
            if "chargen_jobs" in tables(renderfin):
                available = columns(renderfin, "chargen_jobs")
                if "payload" in available:
                    select = "payload, stage" if "stage" in available else "payload, NULL AS stage"
                    for record in renderfin.execute(f"SELECT {select} FROM chargen_jobs WHERE payload IS NOT NULL"):
                        try:
                            payload = json.loads(record[0])
                        except (TypeError, json.JSONDecodeError):
                            continue
                        payload_stage = str(payload.get("stage") or "").lower() if isinstance(payload, dict) else ""
                        column_stage = str(record[1] or "").lower()
                        # Disagreement is conservatively active; both must be terminal to ignore it.
                        if payload_stage in RENDERFIN_TERMINAL and column_stage in RENDERFIN_TERMINAL:
                            continue
                        for task_id in json_source_ids(payload, candidates):
                            reasons[task_id].add("renderfin:chargen_jobs")
        finally:
            renderfin.close()
    for raw_root in reference_roots:
        if not Path(raw_root).exists():
            continue
        root = checked_root(raw_root)
        for path in root.rglob("*.json"):
            assert_no_symlink_path(path, root)
            if not path.is_file():
                raise ValueError(f"unsafe AI reference file: {path}")
            for task_id in identifier_refs(path.read_text(encoding="utf-8", errors="replace"), candidate_rows):
                reasons[task_id].add(f"reference_root:{root}")
    return reasons


def upload_token(url):
    parts = unquote(urlsplit(str(url or "")).path).split("/")
    token = parts[2] if len(parts) > 3 and parts[1] == "u" else ""
    return safe_component(token, "upload token") if token else ""


def identity(path, root, owners):
    assert_no_symlink_path(path, root)
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"not regular file: {path}")
    return {"path": str(Path(path).absolute()), "root": str(root), "device": info.st_dev, "inode": info.st_ino,
            "size": info.st_size, "allocated": getattr(info, "st_blocks", 0) * 512, "mtime_ns": info.st_mtime_ns,
            "nlink": info.st_nlink, "owners": sorted(owners)}


def strict_prefix(name, key):
    return name == key or (name.startswith(key) and len(name) > len(key) and name[len(key)] in "_.-")


def file_manifest(selected, retained, roots):
    selected_ids = {safe_component(row["id"], "task id") for row in selected}
    retained_blob = "\n".join(text_blob(row) for row in retained)
    retained_guids = {safe_component(row["guid"], "guid") for row in retained if row.get("guid")}
    guid_owners = {}
    for row in selected:
        if row.get("guid"):
            guid_owners.setdefault(safe_component(row["guid"], "guid"), set()).add(row["id"])
    entries, seen = [], set()

    def add(path, root, owners):
        key = str(Path(path).absolute())
        if key in seen or key in retained_blob:
            return
        entry = identity(path, root, owners)
        entries.append(entry)
        seen.add(key)

    scanned_roots = set()
    for kind, value in roots.items():
        root = checked_root(value)
        root_identity = (os.lstat(root).st_dev, os.lstat(root).st_ino)
        if root_identity in scanned_roots:
            continue
        scanned_roots.add(root_identity)
        if kind in ("tasks", "artifact_cache"):
            for task_id in selected_ids:
                directory = root / task_id
                if directory.exists():
                    assert_no_symlink_path(directory, root)
                    if not directory.is_dir():
                        raise ValueError(f"expected task directory: {directory}")
                    for path in directory.rglob("*"):
                        assert_no_symlink_path(path, root)
                        if path.is_file():
                            add(path, root, {task_id})
        elif kind == "glb_cache":
            for path in root.iterdir():
                prefix = path.name.split("_", 1)[0]
                owners = {prefix} if prefix in selected_ids and path.name.startswith(prefix + "_") else set()
                if owners:
                    add(path, root, owners)
        elif kind == "videos":
            for task_id in selected_ids:
                path = root / f"{task_id}.mp4"
                if path.exists():
                    add(path, root, {task_id})
        elif kind in ("preflight", "preflight_legacy"):
            for path in root.iterdir():
                prefix = path.name.split(".", 1)[0]
                owners = {prefix} if prefix in selected_ids and path.name.startswith(prefix + ".") else set()
                if owners:
                    add(path, root, owners)
        elif kind == "deliverables":
            keys = selected_ids | {guid for guid in guid_owners if guid not in retained_guids}
            for path in root.iterdir():
                matched = {key for key in keys if strict_prefix(path.name, key)}
                owners = {owner for key in matched for owner in (guid_owners.get(key) or {key})}
                if owners:
                    add(path, root, owners)
        elif kind == "uploads":
            token_owners = {}
            for row in selected:
                token = upload_token(row.get("input_url"))
                if token:
                    token_owners.setdefault(token, set()).add(row["id"])
            retained_tokens = {upload_token(row.get("input_url")) for row in retained}
            for token in set(token_owners) - retained_tokens - {""}:
                directory = root / token
                if directory.exists():
                    assert_no_symlink_path(directory, root)
                    if not directory.is_dir():
                        raise ValueError(f"expected upload directory: {directory}")
                    for path in directory.rglob("*"):
                        assert_no_symlink_path(path, root)
                        if path.is_file():
                            add(path, root, token_owners[token])
    return sorted(entries, key=lambda entry: entry["path"])


def row_hash(row):
    return sha_bytes(canonical(dict(row)))


def stable_value(value):
    if isinstance(value, bytes):
        return {"blob_sha256": sha_bytes(value), "length": len(value)}
    return value


def immutable_snapshot(conn):
    snapshot = {}
    for table in sorted(tables(conn)):
        if not any(marker in table.lower() for marker in IMMUTABLE_TABLE_MARKERS):
            continue
        records, numeric_sums = [], {}
        for row in conn.execute(f'SELECT * FROM "{table}"'):
            records.append({key: stable_value(row[key]) for key in row.keys()})
            for key in row.keys():
                if isinstance(row[key], (int, float)) and not isinstance(row[key], bool):
                    numeric_sums[key] = numeric_sums.get(key, 0) + row[key]
        encoded = sorted(canonical(record) for record in records)
        snapshot[table] = {"count": len(records), "numeric_sums": numeric_sums,
                           "sha256": sha_bytes(b"\n".join(encoded))}
    return snapshot


def workload_block_reason(row):
    preemption = str(row.get("preemption_state") or "none").lower()
    if preemption in {"requested", "stopping"}:
        return f"preemption:{preemption}"
    state = str(row.get("workload_lease_state") or "").lower()
    lease_id = str(row.get("workload_lease_id") or "")
    if state in {"active", "waiting", "acquiring", "preempting", "submission_unknown", "unknown"}:
        return f"workload_lease:{state}"
    if not lease_id:
        return ""
    if state not in {"released", "expired"}:
        return "workload_lease:unresolved"
    for key in ("workload_lease_expires_at", "workload_lease_expiry", "lease_expires_at"):
        if row.get(key):
            try:
                expires = dt.datetime.fromisoformat(str(row[key]).replace("Z", "+00:00"))
                if expires.tzinfo is None:
                    expires = expires.replace(tzinfo=dt.timezone.utc)
                if expires.astimezone(dt.timezone.utc) > dt.datetime.now(dt.timezone.utc):
                    return "workload_lease:future_expiry"
            except ValueError:
                return "workload_lease:invalid_expiry"
    return ""


def manifest_from_connection(conn, db, renderfin_db, cutoff_utc, roots, reference_roots=()):
    old = rows(conn, "SELECT * FROM tasks WHERE created_at < ?", (cutoff_utc,))
    candidates = [row for row in old if str(row.get("status") or "").lower() in TERMINAL]
    reasons = protected_references(conn, candidates, renderfin_db, reference_roots)
    for row in candidates:
        reason = workload_block_reason(row)
        if reason:
            reasons[str(row["id"])].add(reason)
    protected = {task_id for task_id, why in reasons.items() if why}
    all_tasks = rows(conn, "SELECT * FROM tasks")
    while True:
        provisional = [row for row in candidates if row["id"] not in protected]
        provisional_ids = {row["id"] for row in provisional}
        retained = [row for row in all_tasks if row["id"] not in provisional_ids]
        newly_protected = identifier_refs("\n".join(text_blob(row) for row in retained), provisional) - protected
        if not newly_protected:
            break
        for task_id in newly_protected:
            reasons[task_id].add("retained_task_payload")
        protected.update(newly_protected)
    selected = [row for row in candidates if row["id"] not in protected]
    selected_ids = {row["id"] for row in selected}
    retained = [row for row in all_tasks if row["id"] not in selected_ids]
    files = file_manifest(selected, retained, roots)
    per_root, unique = {}, set()
    manifested_links = {}
    for entry in files:
        inode = (entry["device"], entry["inode"])
        manifested_links[inode] = manifested_links.get(inode, 0) + 1
    for entry in files:
        bucket = per_root.setdefault(entry["root"], {"files": 0, "logical_bytes": 0, "unique_inode_bytes": 0,
                                                     "unique_inode_allocated": 0, "forecast_reclaim_allocated": 0})
        bucket["files"] += 1
        bucket["logical_bytes"] += entry["size"]
        inode = (entry["device"], entry["inode"])
        if inode not in unique:
            bucket["unique_inode_bytes"] += entry["size"]
            bucket["unique_inode_allocated"] += entry["allocated"]
            if manifested_links[inode] >= entry["nlink"]:
                bucket["forecast_reclaim_allocated"] += entry["allocated"]
            unique.add(inode)
    snapshots = [{"id": row["id"], "row_sha256": row_hash(row)} for row in selected]
    root_identities = {}
    for key, value in roots.items():
        root = checked_root(value); info = os.lstat(root)
        root_identities[key] = {"path": str(root), "device": info.st_dev, "inode": info.st_ino}
    protected_manifest = {task_id: sorted(why) for task_id, why in reasons.items() if why}
    reason_counts = {}
    for protections in protected_manifest.values():
        for reason in protections:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    return {"schema": 2, "db": str(Path(db).resolve()), "renderfin_db": str(Path(renderfin_db).resolve()) if renderfin_db else "",
            "cutoff_utc": cutoff_utc, "tasks": snapshots, "files": files, "candidate_count": len(candidates),
            "protected_count": len(protected_manifest), "protected": protected_manifest,
            "protected_reason_counts": dict(sorted(reason_counts.items())),
            "counts": {"tasks": len(selected), "files": len(files),
                       "bytes": sum(item["logical_bytes"] for item in per_root.values()),
                       "logical_bytes": sum(item["logical_bytes"] for item in per_root.values()),
                       "forecast_reclaim_allocated": sum(item["forecast_reclaim_allocated"] for item in per_root.values())},
            "per_root": per_root, "roots": {key: str(Path(value).absolute()) for key, value in roots.items()},
            "root_identities": root_identities, "immutable_snapshot": immutable_snapshot(conn),
            "reference_roots": [str(Path(value).absolute()) for value in reference_roots],
            "rules": {"terminal_only": True, "financial_support_audit_preserved": True, "operational_children": list(OPS)}}


def build_manifest(db, renderfin_db, cutoff_utc, roots, reference_roots=()):
    conn = ro_connect(db)
    try:
        return manifest_from_connection(conn, db, renderfin_db, cutoff_utc, roots, reference_roots)
    finally:
        conn.close()


def backup_db(db, target):
    source = ro_connect(db)
    destination = sqlite3.connect(target)
    try:
        source.backup(destination)
        if destination.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("backup integrity check failed")
    finally:
        destination.close()
        source.close()
    os.chmod(target, 0o600)


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup_proves_manifest(backup, manifest):
    conn = ro_connect(backup)
    try:
        for task in manifest["tasks"]:
            row = conn.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()
            if row is None or row_hash(row) != task["row_sha256"]:
                return False
        return True
    finally:
        conn.close()


def operational_rows_absent(conn, task_ids):
    existing = tables(conn)
    for table in OPS:
        if table not in existing or "task_id" not in columns(conn, table):
            continue
        for task_id in task_ids:
            if conn.execute(f'SELECT 1 FROM "{table}" WHERE task_id=? LIMIT 1', (task_id,)).fetchone():
                return False
    return True


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def append_records(path, records):
    with path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def read_deleted(path):
    if not path.exists():
        return set()
    return {record["path"] for record in (json.loads(line) for line in path.read_text().splitlines())
            if record.get("outcome") in {"deleted", "already_absent"}}


def current_identity(entry):
    path, root = Path(entry["path"]), Path(entry["root"])
    assert_no_symlink_path(path, root)
    info = os.lstat(path)
    # Another manifested hardlink can already have been removed by this run.
    # nlink is only a reclaim forecast, not an invariant of the remaining name.
    return stat.S_ISREG(info.st_mode) and (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) == (
        entry["device"], entry["inode"], entry["size"], entry["mtime_ns"])


def verify_root_identities(manifest):
    for key, expected in manifest["root_identities"].items():
        root = checked_root(manifest["roots"][key])
        info = os.lstat(root)
        if str(root) != expected["path"] or (info.st_dev, info.st_ino) != (expected["device"], expected["inode"]):
            raise RuntimeError(f"deletion root identity changed: {key}")


def remove_empty_owned_dirs(manifest):
    candidates = set()
    for entry in manifest["files"]:
        parent, root = Path(entry["path"]).parent, Path(entry["root"])
        while parent != root:
            candidates.add((len(parent.parts), str(parent), str(root)))
            parent = parent.parent
    for _, directory_text, root_text in sorted(candidates, reverse=True):
        directory, root = Path(directory_text), Path(root_text)
        if not directory.exists():
            continue
        assert_no_symlink_path(directory, root)
        try:
            directory.rmdir()
        except OSError:
            pass


def apply_manifest(manifest, audit_dir):
    if manifest.get("schema") != 2:
        raise RuntimeError("unsupported manifest schema")
    digest = sha_bytes(canonical(manifest))
    state_path = audit_dir / f"prune-journal-{digest}.json"
    log_path = audit_dir / f"prune-journal-{digest}.jsonl"
    backup = audit_dir / f"tasks-before-prune-{digest}.sqlite3"
    state = json.loads(state_path.read_text()) if state_path.exists() else None
    task_ids = [task["id"] for task in manifest["tasks"]]
    verify_root_identities(manifest)
    if state is None:
        if backup.exists():
            raise RuntimeError("unclaimed deterministic backup already exists")
        backup_db(manifest["db"], backup)
        state = {"db_committed": False, "backup": str(backup), "backup_sha256": file_sha256(backup),
                 "task_count": len(task_ids), "task_ids_sha256": sha_bytes(canonical(task_ids))}
        atomic_json(state_path, state)
    elif Path(state.get("backup") or "") != backup or not backup.is_file() or file_sha256(backup) != state.get("backup_sha256"):
        raise RuntimeError("backup identity differs from atomic intent")
    if not backup_proves_manifest(backup, manifest):
        raise RuntimeError("backup does not contain exact manifested task rows")
    if not state["db_committed"]:
        conn = sqlite3.connect(manifest["db"], isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            conn.execute("BEGIN IMMEDIATE")
            remaining = conn.execute(
                "SELECT count(*) FROM tasks WHERE id IN (%s)" % ",".join("?" for _ in task_ids), task_ids
            ).fetchone()[0] if task_ids else 0
            if task_ids and remaining == 0:
                # The DB commit can precede the atomic state write. All exact manifested
                # rows being absent is the only safe reconciliation for that crash window.
                if not operational_rows_absent(conn, task_ids):
                    raise RuntimeError("selected tasks absent but operational rows remain")
                conn.commit()
                state.update({"db_committed": True, "reconciled_after_commit": True})
                atomic_json(state_path, state)
                conn.close()
                conn = None
            if conn is None:
                pass
            else:
                fresh = manifest_from_connection(conn, manifest["db"], manifest.get("renderfin_db") or None,
                                                 manifest["cutoff_utc"], manifest["roots"], manifest.get("reference_roots") or ())
            if conn is not None and (fresh["tasks"] != manifest["tasks"] or fresh["files"] != manifest["files"] or
                                     fresh["immutable_snapshot"] != manifest["immutable_snapshot"]):
                raise RuntimeError("fresh selection differs: rows or protected references changed")
            for task in manifest["tasks"] if conn is not None else ():
                current = conn.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()
                if current is None or row_hash(current) != task["row_sha256"]:
                    raise RuntimeError("task CAS snapshot changed")
                if str(current["status"] or "").lower() not in TERMINAL:
                    raise RuntimeError("nonterminal task blocks prune")
                if "preemption_state" in current.keys() and str(current["preemption_state"] or "none").lower() in {"requested", "stopping"}:
                    raise RuntimeError("active preemption blocks prune")
            baseline_fk = {tuple(row) for row in conn.execute("PRAGMA foreign_key_check")} if conn is not None else set()
            existing = tables(conn) if conn is not None else set()
            for table in OPS:
                if table in existing and "task_id" in columns(conn, table):
                    conn.executemany(f'DELETE FROM "{table}" WHERE task_id=?', [(task_id,) for task_id in task_ids])
            before = conn.total_changes if conn is not None else 0
            if conn is not None:
                conn.executemany("DELETE FROM tasks WHERE id=?", [(task_id,) for task_id in task_ids])
            if conn is not None and conn.total_changes - before != len(task_ids):
                raise RuntimeError("task delete count mismatch")
            if conn is not None and not {tuple(row) for row in conn.execute("PRAGMA foreign_key_check")}.issubset(baseline_fk):
                raise RuntimeError("prune introduced foreign key violations")
            if conn is not None and immutable_snapshot(conn) != manifest["immutable_snapshot"]:
                raise RuntimeError("immutable financial/audit tables changed")
            if conn is not None:
                conn.commit()
                state["db_committed"] = True
                atomic_json(state_path, state)
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()
    else:
        conn = ro_connect(manifest["db"])
        try:
            remaining = conn.execute("SELECT count(*) FROM tasks WHERE id IN (%s)" % ",".join("?" for _ in task_ids), task_ids).fetchone()[0] if task_ids else 0
            if remaining:
                raise RuntimeError("journal says committed but selected tasks remain")
        finally:
            conn.close()
    done, pending = read_deleted(log_path), []
    for entry in manifest["files"]:
        if entry["path"] in done:
            continue
        path = Path(entry["path"])
        if path.exists() or path.is_symlink():
            if not current_identity(entry):
                raise RuntimeError("file identity changed")
            path.unlink()
            outcome = "deleted"
        else:
            outcome = "already_absent"
        pending.append({"path": entry["path"], "outcome": outcome})
        if len(pending) >= 128:
            append_records(log_path, pending)
            pending.clear()
    if pending:
        append_records(log_path, pending)
    remove_empty_owned_dirs(manifest)
    return state_path


@contextmanager
def audit_lock(audit_dir):
    path = audit_dir / "prune.lock"
    handle = path.open("a+b")
    os.chmod(path, 0o600)
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0); handle.write(b"0"); handle.flush(); handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        if os.name == "nt":
            handle.seek(0); msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def production_db(path):
    return str(Path(path).resolve()).startswith("/srv/")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True); parser.add_argument("--renderfin-db"); parser.add_argument("--cutoff", required=True)
    parser.add_argument("--audit-dir", required=True); parser.add_argument("--roots-json"); parser.add_argument("--reference-roots-json")
    parser.add_argument("--apply", action="store_true"); parser.add_argument("--manifest-sha256")
    args = parser.parse_args(argv)
    audit = Path(args.audit_dir).absolute(); allowed = Path("/srv/autorig/audits")
    if (str(audit).startswith("/srv/") or production_db(args.db)) and not audit.is_relative_to(allowed):
        raise SystemExit("audit dir outside /srv/autorig/audits")
    audit.mkdir(parents=True, exist_ok=True); os.chmod(audit, 0o700)
    supplied = json.loads(args.roots_json) if args.roots_json else None
    if production_db(args.db) and supplied and supplied != DEFAULT_ROOTS:
        raise SystemExit("custom roots forbidden for production DB")
    roots = supplied or DEFAULT_ROOTS
    reference_roots = json.loads(args.reference_roots_json) if args.reference_roots_json else DEFAULT_REFERENCE_ROOTS
    cutoff_utc = cutoff(args.cutoff)
    with audit_lock(audit):
        if not args.apply:
            manifest = build_manifest(args.db, args.renderfin_db, cutoff_utc, roots, reference_roots)
            data = canonical(manifest); digest = sha_bytes(data); path = audit / f"prune-manifest-{digest}.json"
            path.write_bytes(data); os.chmod(path, 0o600)
            print(json.dumps({"status": "dry_run", "manifest_sha256": digest, **manifest["counts"]}))
            return 0
        if not args.manifest_sha256 or not re.fullmatch(r"[0-9a-fA-F]{64}", args.manifest_sha256):
            raise SystemExit("--apply requires --manifest-sha256")
        matches = [path for path in audit.glob("prune-manifest-*.json") if sha_bytes(path.read_bytes()) == args.manifest_sha256.lower()]
        if len(matches) != 1:
            raise SystemExit("manifest hash not found uniquely")
        manifest = json.loads(matches[0].read_text(encoding="utf-8"))
        expected_renderfin = str(Path(args.renderfin_db).resolve()) if args.renderfin_db else ""
        expected_roots = {key: str(Path(value).absolute()) for key, value in roots.items()}
        if (manifest.get("db") != str(Path(args.db).resolve()) or manifest.get("renderfin_db", "") != expected_renderfin or
                manifest.get("cutoff_utc") != cutoff_utc or manifest.get("roots") != expected_roots):
            raise SystemExit("apply arguments do not exactly match manifest")
        journal = apply_manifest(manifest, audit)
        print(json.dumps({"status": "applied", "journal": journal.name, "tasks": len(manifest["tasks"]), "files": len(manifest["files"])}))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

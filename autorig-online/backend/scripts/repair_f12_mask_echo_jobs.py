#!/usr/bin/env python3
"""Audited one-time rewind for the 2026-08-22..26 F12 pose-mask echo.

The immutable incident manifest supplies the exact logical CharacterGen job
ids.  The script first backs up SQLite, then proves every current Hunyuan
binding terminal and releases its matching central lease before rewinding the
same rows to Flux.  Existing images, GLBs, videos, RenderTasks, AutoRig task ids
and Telegram message receipts are preserved as evidence; no artifact is
deleted.

Run only while ``autorig-storage-renderfin.service`` is stopped.  Without
``--apply`` this is a read-only validation report.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

# Keep direct production invocation independent of the caller's PYTHONPATH,
# matching the other one-shot backend scripts in this directory.
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from renderfin import hunyuan_client, workload_lease


REASON = (
    "f12_comfy_node_validation_failed_and_preview_echoed_control_mask_"
    "2026-08-26"
)
SERVICE_UNIT = "autorig-storage-renderfin.service"
RESET_FIELDS = {
    "flux_task_id": "",
    "flux_task_id_b": "",
    "image_url": "",
    "isolated_url": "",
    "image_url_b": "",
    "isolated_url_b": "",
    "chosen_variant": "",
    "hunyuan_task_id": "",
    "hunyuan_seed": 0,
    "hunyuan_worker": "",
    "hunyuan_worker_url": "",
    "hunyuan_workload_request_id": "",
    "hunyuan_workload_lease_id": "",
    "hunyuan_workload_physical_resource_id": "",
    "hunyuan_workload_node_id": "",
    "hunyuan_workload_lease_state": "",
    "hunyuan_workload_heartbeat_at": 0,
    "hunyuan_worker_cooldowns": {},
    "hunyuan_waiting_for_capacity": False,
    "glb_url": "",
    "glb_quality_report": {},
    "video_url": "",
    "error": "",
    "warning": "",
    "last_error": "",
    "retry_at": 0,
    "attempts": {},
    "attempts_refunded": False,
    "stage_started_at": 0,
    "timed_stage": "",
    "dispatch_not_before": 0,
}

_ACTIVE_BROKER_LEASE_STATES = {"active", "preemption_requested"}
_ACTIVE_BROKER_WAITER_STATES = {"waiting"}


def _read_manifest(path: Path, expected_sha256: str, expected_count: int) -> Dict[str, Any]:
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256.lower():
        raise RuntimeError(f"manifest SHA-256 mismatch: {digest}")
    manifest = json.loads(raw)
    jobs = manifest.get("jobs")
    if not isinstance(jobs, list) or len(jobs) != expected_count:
        raise RuntimeError(
            f"manifest expected {expected_count} jobs, got "
            f"{len(jobs) if isinstance(jobs, list) else 'invalid'}"
        )
    ids = [str(item.get("job_id") or "") for item in jobs]
    if len(set(ids)) != expected_count or not all(ids):
        raise RuntimeError("manifest job ids are missing or duplicated")
    return manifest


def _load_live_jobs(db_path: Path, job_ids: List[str]) -> Tuple[sqlite3.Connection, Dict[str, Dict[str, Any]]]:
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    marks = ",".join("?" for _ in job_ids)
    rows = connection.execute(
        f"SELECT id, payload, stage FROM chargen_jobs WHERE id IN ({marks})",
        job_ids,
    ).fetchall()
    if len(rows) != len(job_ids):
        found = {row["id"] for row in rows}
        raise RuntimeError(f"live Renderfin DB is missing jobs: {sorted(set(job_ids) - found)}")
    jobs = {}
    for row in rows:
        payload = json.loads(row["payload"])
        if str(payload.get("id") or "") != row["id"]:
            raise RuntimeError(f"payload identity mismatch for {row['id']}")
        if str(payload.get("stage") or "") != str(row["stage"] or ""):
            raise RuntimeError(f"payload/stage column mismatch for {row['id']}")
        jobs[row["id"]] = payload
    return connection, jobs


def _worker_idle_for_exact_job(status: Dict[str, Any], job: Dict[str, Any]) -> bool:
    """Fail closed unless the worker proves its whole single-consumer slot idle.

    This is used only when the terminal task record has already been pruned.
    Absence of the old task id alone is insufficient: malformed or partial
    telemetry must not authorize clearing its durable central binding.
    """
    if not isinstance(status, dict):
        return False
    exact_task = str(job.get("hunyuan_task_id") or "").rstrip("/").rsplit("/", 1)[-1]
    backend_id = str(job.get("id") or "")
    combined: List[Dict[str, Any]] = []
    for key in ("processing_tasks", "pending_tasks"):
        if key not in status:
            return False
        items = status.get(key)
        if isinstance(items, dict):
            items = list(items.values())
        if not isinstance(items, list):
            return False
        for item in items:
            if not isinstance(item, dict):
                return False
            worker_task = str(item.get("id") or item.get("task_id") or "")
            owner = str(item.get("backend_task_id") or "")
            if worker_task == exact_task or owner == backend_id:
                return False
            combined.append(item)
    if combined:
        return False
    summary = status.get("tasks_summary")
    if not isinstance(summary, dict):
        return False
    counters: List[int] = []
    for key in ("processing", "pending", "queue_size"):
        value = summary.get(key)
        if isinstance(value, bool):
            return False
        if isinstance(value, float) and not value.is_integer():
            return False
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return False
        if parsed < 0:
            return False
        counters.append(parsed)
    return counters == [0, 0, 0]


async def _retire_live_bindings(
    jobs: Dict[str, Dict[str, Any]],
    *,
    broker_evidence: Optional[Dict[str, Dict[str, Any]]] = None,
    receipts: Optional[List[Dict[str, Any]]] = None,
    on_receipt: Optional[Callable[[], None]] = None,
) -> List[Dict[str, Any]]:
    receipts = receipts if receipts is not None else []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for job_id, job in sorted(jobs.items()):
            task_url = str(job.get("hunyuan_task_id") or "").strip()
            lease_id = str(job.get("hunyuan_workload_lease_id") or "").strip()
            request_id = str(job.get("hunyuan_workload_request_id") or "").strip()
            identity_evidence = dict((broker_evidence or {}).get(job_id) or {})
            broker_active = bool(identity_evidence.get("active"))
            stage = str(job.get("stage") or "").strip().lower()
            # These stages are persisted only after the Hunyuan model was
            # downloaded successfully.  Their task URL is provenance, not a
            # live binding, and may point at a worker which has since been
            # disabled or removed from the production registry.  In contrast,
            # a URL while still in ``hunyuan`` must be retired against the
            # exact currently configured worker before its identity is reset.
            historical_terminal = bool(task_url) and stage in {
                "turntable",
                "ready",
                "submitted",
            }
            outcome = ""
            worker_name = str(job.get("hunyuan_worker") or "")
            record: Dict[str, Any] = {
                "job_id": job_id,
                "stage": str(job.get("stage") or ""),
                "state": "retiring",
                "worker": worker_name or None,
                "worker_task": task_url or None,
                "worker_outcome": "pending" if task_url else "no_binding",
                "lease_id": lease_id or None,
                "lease_outcome": (
                    "pending" if lease_id or request_id else "no_lease"
                ),
            }
            receipts.append(record)
            if on_receipt is not None:
                on_receipt()

            try:
                if historical_terminal:
                    outcome = "historical_terminal"
                    record["worker_outcome"] = outcome
                elif task_url:
                    if not task_url.startswith(("http://", "https://")):
                        raise RuntimeError(
                            f"{job_id} has unsupported non-converter Hunyuan binding {task_url}"
                        )
                    worker = hunyuan_client.worker_for_url(task_url)
                    if worker is None:
                        raise RuntimeError(f"{job_id} worker for {task_url} is not configured")
                    worker_name = str(worker.get("name") or worker_name)
                    record["worker"] = worker_name or None
                    try:
                        outcome = await hunyuan_client.preempt_bound_task(
                            client,
                            worker,
                            task_url,
                            backend_task_id=job_id,
                            requester_workload_class="ai_vision",
                        )
                    except Exception:
                        # A worker may have pruned the terminal status record.
                        # The unauthenticated status endpoint is useful only as
                        # exact operational proof that the single-consumer slot
                        # and all its reported counters are empty.
                        status = await hunyuan_client.server_status(client, worker)
                        if not _worker_idle_for_exact_job(status, job):
                            raise
                        outcome = "released"
                    record["worker_outcome"] = outcome

                lease_outcome = ""
                if lease_id:
                    if not request_id:
                        raise RuntimeError(f"{job_id} has a lease without request identity")
                    if identity_evidence and not broker_active:
                        lease_outcome = str(
                            identity_evidence.get("disposition")
                            or "broker_identity_terminal_or_absent"
                        )
                    else:
                        lease_outcome = (
                            "completed"
                            if outcome in {"completed", "historical_terminal"}
                            else "preempted"
                        )
                        await workload_lease.release(
                            client,
                            lease_id=lease_id,
                            owner_task_id=job_id,
                            request_id=request_id,
                            outcome=lease_outcome,
                        )
                elif request_id:
                    # A request can be waiting for capacity without having
                    # acquired a lease. Clearing its identity without cancelling
                    # it leaves a broker waiter that may later consume a GPU for
                    # no job.
                    if identity_evidence and not broker_active:
                        lease_outcome = str(
                            identity_evidence.get("disposition")
                            or "broker_identity_terminal_or_absent"
                        )
                    else:
                        await workload_lease.cancel_waiter(
                            client,
                            request_id=request_id,
                            owner_task_id=job_id,
                        )
                        lease_outcome = "waiter_cancelled"
                if identity_evidence:
                    record["broker_evidence"] = identity_evidence
                record["lease_outcome"] = lease_outcome or "no_lease"
                record["state"] = "terminal"
            except Exception as exc:
                record["state"] = "failed"
                record["failure"] = f"{type(exc).__name__}: {exc}"
                if on_receipt is not None:
                    on_receipt()
                raise
            if on_receipt is not None:
                on_receipt()
    return receipts


def _inspect_broker_identities(
    autorig_db_path: Path,
    jobs: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Prove whether persisted Renderfin broker identities are still live.

    The workload broker can be intentionally disabled during rollout.  A
    historical request id which is absent from both authoritative tables must
    not force operators to enable GPU admission merely to clear stale
    provenance.  Conversely, any waiting request or active lease fails closed
    unless the broker API is enabled so it can be retired idempotently.
    """

    connection = sqlite3.connect(
        f"file:{autorig_db_path.resolve().as_posix()}?mode=ro", uri=True, timeout=30
    )
    connection.row_factory = sqlite3.Row
    evidence: Dict[str, Dict[str, Any]] = {}
    try:
        required_tables = {"workload_waiters", "workload_leases"}
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if not required_tables.issubset(present):
            raise RuntimeError(
                "AutoRig DB does not contain the authoritative workload broker tables"
            )
        for job_id, job in sorted(jobs.items()):
            request_id = str(job.get("hunyuan_workload_request_id") or "").strip()
            lease_id = str(job.get("hunyuan_workload_lease_id") or "").strip()
            if lease_id and not request_id:
                raise RuntimeError(f"{job_id} has a lease without request identity")
            if not request_id and not lease_id:
                continue
            waiter = connection.execute(
                "SELECT request_id, owner_service, owner_task_id, state, lease_id "
                "FROM workload_waiters WHERE request_id=?",
                (request_id,),
            ).fetchone()
            lease_rows = connection.execute(
                "SELECT lease_id, request_id, owner_service, owner_task_id, state "
                "FROM workload_leases WHERE request_id=? OR lease_id=?",
                (request_id, lease_id or "__none__"),
            ).fetchall()
            if len(lease_rows) > 1:
                raise RuntimeError(f"{job_id} broker identity matched multiple leases")
            lease = lease_rows[0] if lease_rows else None
            for kind, row in (("waiter", waiter), ("lease", lease)):
                if row is None:
                    continue
                if (
                    str(row["owner_service"] or "") != "renderfin"
                    or str(row["owner_task_id"] or "") != job_id
                ):
                    raise RuntimeError(f"{job_id} {kind} broker owner mismatch")
            waiter_state = str(waiter["state"] or "").lower() if waiter else "absent"
            lease_state = str(lease["state"] or "").lower() if lease else "absent"
            active = (
                waiter_state in _ACTIVE_BROKER_WAITER_STATES
                or lease_state in _ACTIVE_BROKER_LEASE_STATES
            )
            if waiter is None and lease is None:
                disposition = "broker_identity_absent"
            elif active:
                disposition = "broker_identity_active"
            else:
                disposition = "broker_identity_terminal"
            evidence[job_id] = {
                "request_id": request_id or None,
                "lease_id": lease_id or None,
                "waiter_state": waiter_state,
                "lease_state": lease_state,
                "active": bool(active),
                "disposition": disposition,
            }
    finally:
        connection.close()
    return evidence


def _assert_apply_preconditions(
    *,
    db_path: Path,
    autorig_db_path: Optional[Path] = None,
    manifest_path: Path,
    backup_path: Path,
    receipt_path: Path,
    jobs: Dict[str, Dict[str, Any]],
    broker_evidence: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    resolved = {
        "db": db_path.resolve(),
        "autorig_db": (autorig_db_path or Path("autorig.db")).resolve(),
        "manifest": manifest_path.resolve(),
        "backup": backup_path.resolve(),
        "receipt": receipt_path.resolve(),
    }
    if len(set(resolved.values())) != len(resolved):
        raise RuntimeError(f"db, manifest, backup and receipt paths must be distinct: {resolved}")
    for label, path in (("backup", backup_path), ("receipt", receipt_path)):
        if path.exists():
            raise RuntimeError(f"{label} already exists: {path}")

    repaired = [
        job_id
        for job_id, job in jobs.items()
        if str(job.get("quality_repair_reason") or "") == REASON
    ]
    if repaired:
        state = "all" if len(repaired) == len(jobs) else "partial"
        raise RuntimeError(
            f"repair marker already present on {len(repaired)}/{len(jobs)} jobs ({state}); "
            "refusing a second rewind"
        )

    active_central_identities = [
        job_id
        for job_id, item in (broker_evidence or {}).items()
        if item.get("active")
    ]
    if active_central_identities and not workload_lease.enabled():
        raise RuntimeError(
            "active workload broker identities are present but "
            "RENDERFIN_WORKLOAD_BROKER_ENABLED is not active: "
            + ",".join(active_central_identities)
        )

    try:
        result = subprocess.run(
            [
                "systemctl",
                "show",
                SERVICE_UNIT,
                "--property=LoadState",
                "--property=ActiveState",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"cannot prove {SERVICE_UNIT} is stopped: {exc}") from exc
    state_by_key = {}
    for line in str(result.stdout or "").splitlines():
        key, separator, value = line.partition("=")
        if separator:
            state_by_key[key.strip()] = value.strip().lower()
    load_state = state_by_key.get("LoadState", "")
    active_state = state_by_key.get("ActiveState", "")
    if result.returncode != 0 or load_state != "loaded" or active_state not in {
        "inactive",
        "failed",
    }:
        raise RuntimeError(
            f"{SERVICE_UNIT} must be loaded and stopped before --apply; "
            f"observed load={load_state or 'unknown'} active={active_state or 'unknown'}"
        )


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    data = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _backup_database(connection: sqlite3.Connection, backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    if backup_path.exists():
        raise RuntimeError(f"backup already exists: {backup_path}")
    backup = sqlite3.connect(backup_path)
    try:
        connection.backup(backup)
        check = backup.execute("PRAGMA quick_check").fetchone()
        if not check or str(check[0]).lower() != "ok":
            raise RuntimeError(f"backup quick_check failed: {check}")
    finally:
        backup.close()


def _apply_rewind(
    connection: sqlite3.Connection,
    jobs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    now = time.time()
    before_rows = []
    connection.execute("BEGIN IMMEDIATE")
    try:
        for job_id, payload in sorted(jobs.items()):
            before_rows.append(
                {
                    "job_id": job_id,
                    "stage": payload.get("stage"),
                    "artifact_revision": int(payload.get("artifact_revision") or 0),
                    "submitted_task_id": payload.get("submitted_task_id") or None,
                    "attempts": dict(payload.get("attempts") or {}),
                    "attempts_refunded": bool(payload.get("attempts_refunded")),
                    "preemption_count": int(payload.get("preemption_count") or 0),
                    "preempted_at": float(payload.get("preempted_at") or 0),
                    "dispatch_not_before": float(
                        payload.get("dispatch_not_before") or 0
                    ),
                    "flux_task_id": payload.get("flux_task_id") or None,
                    "hunyuan_task_id": payload.get("hunyuan_task_id") or None,
                    "glb_url": payload.get("glb_url") or None,
                    "video_url": payload.get("video_url") or None,
                }
            )
            repaired = dict(payload)
            repaired.update(RESET_FIELDS)
            repaired["stage"] = "flux_render"
            repaired["artifact_revision"] = int(
                payload.get("artifact_revision") or 0
            ) + 1
            repaired["quality_repair_reason"] = REASON
            repaired["updated_at"] = now
            # Keep submitted_task_id: the bot reuses a still-created/unbound
            # row, or records it in superseded_task_ids before making a new one.
            encoded = json.dumps(
                repaired,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            changed = connection.execute(
                "UPDATE chargen_jobs SET payload=?, stage=? WHERE id=?",
                (encoded, "flux_render", job_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError(f"failed to update exact job {job_id}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    marks = ",".join("?" for _ in jobs)
    verified = connection.execute(
        f"SELECT COUNT(*) FROM chargen_jobs WHERE id IN ({marks}) AND stage='flux_render'",
        list(jobs),
    ).fetchone()[0]
    if verified != len(jobs):
        raise RuntimeError(f"post-rewind verification failed: {verified}/{len(jobs)}")
    return {"rewound": verified, "before": before_rows}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--autorig-db", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--expected-count", required=True, type=int)
    parser.add_argument("--backup", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    manifest = _read_manifest(
        args.manifest, args.manifest_sha256, args.expected_count
    )
    job_ids = [str(item["job_id"]) for item in manifest["jobs"]]
    connection, jobs = _load_live_jobs(args.db, job_ids)
    try:
        broker_evidence = _inspect_broker_identities(args.autorig_db, jobs)
        summary = {
            "manifest_sha256": args.manifest_sha256.lower(),
            "jobs": len(jobs),
            "stages": {},
            "hunyuan_task_urls": sum(
                1 for job in jobs.values() if job.get("hunyuan_task_id")
            ),
            "live_hunyuan_bindings": sum(
                1
                for job in jobs.values()
                if str(job.get("stage") or "").strip().lower() == "hunyuan"
                and job.get("hunyuan_task_id")
            ),
            "submitted_task_ids": sorted(
                {
                    str(job.get("submitted_task_id") or "")
                    for job in jobs.values()
                    if job.get("submitted_task_id")
                }
            ),
            "broker_identities": broker_evidence,
        }
        for job in jobs.values():
            stage = str(job.get("stage") or "")
            summary["stages"][stage] = summary["stages"].get(stage, 0) + 1
        if not args.apply:
            print(json.dumps({"dry_run": True, **summary}, sort_keys=True))
            return

        _assert_apply_preconditions(
            db_path=args.db,
            autorig_db_path=args.autorig_db,
            manifest_path=args.manifest,
            backup_path=args.backup,
            receipt_path=args.receipt,
            jobs=jobs,
            broker_evidence=broker_evidence,
        )
        # Take a consistent immutable copy before the first external side
        # effect.  A collision or backup failure must never happen after a
        # worker has already been preempted.
        _backup_database(connection, args.backup)
        receipts: List[Dict[str, Any]] = []
        receipt_payload = {
            "schema": "renderfin.f12_pose_mask_echo.repair_receipt.v1",
            "created_at_epoch": time.time(),
            "state": "retiring_bindings",
            "summary": summary,
            "backup": str(args.backup),
            "terminal_receipts": receipts,
        }
        _atomic_write_json(args.receipt, receipt_payload)

        def persist_receipts() -> None:
            receipt_payload["updated_at_epoch"] = time.time()
            _atomic_write_json(args.receipt, receipt_payload)

        try:
            await _retire_live_bindings(
                jobs,
                broker_evidence=broker_evidence,
                receipts=receipts,
                on_receipt=persist_receipts,
            )
        except Exception as exc:
            receipt_payload["state"] = "binding_retirement_failed"
            receipt_payload["failure"] = f"{type(exc).__name__}: {exc}"
            persist_receipts()
            raise

        receipt_payload["state"] = "bindings_retired"
        persist_receipts()
        result = _apply_rewind(connection, jobs)
        receipt_payload["rewind"] = result
        receipt_payload["state"] = "complete"
        persist_receipts()
        print(
            json.dumps(
                {
                    "dry_run": False,
                    **summary,
                    "rewound": result["rewound"],
                    "backup": str(args.backup),
                    "receipt": str(args.receipt),
                },
                sort_keys=True,
            )
        )
    finally:
        connection.close()


if __name__ == "__main__":
    asyncio.run(main())

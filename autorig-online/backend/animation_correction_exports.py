"""Revisioned export jobs for published realtime bone corrections.

The web backend owns the durable job state and immutable request contract. A
Blender-capable worker (F1 in production) may be connected through the optional
webhook. Public preview does not depend on that worker.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from sqlalchemy import select

from animation_corrections import load_json_object, payload_sha256, utc_now_iso
from config import APP_URL
from database import AsyncSessionLocal, Task, TaskAnimationCorrection


EXPORT_SCHEMA = "autorig.animation-bone-correction-export.v1"
EXPORT_ROOT = Path(
    os.getenv("ANIMATION_CORRECTION_EXPORT_ROOT", "/var/autorig/animation-corrections")
)
EXPORT_WEBHOOK = os.getenv("ANIMATION_CORRECTION_EXPORT_WEBHOOK", "").strip()
EXPORT_TOKEN = os.getenv("ANIMATION_CORRECTION_EXPORT_TOKEN", "").strip()
SOURCE_SHA256_KEYS = ("preparedGlb", "animationsGlb", "animationManifest")


def normalize_source_sha256(value: Any, *, required: bool = False) -> Optional[Dict[str, str]]:
    if value is None and not required:
        return None
    if not isinstance(value, dict):
        raise ValueError("sourceSha256 must be an object")
    normalized: Dict[str, str] = {}
    for key in SOURCE_SHA256_KEYS:
        digest = str(value.get(key) or "").strip().lower()
        if not digest:
            if required:
                raise ValueError(f"sourceSha256.{key} is required")
            continue
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(f"sourceSha256.{key} must be SHA-256 hex")
        normalized[key] = digest
    return normalized or None


def build_export_contract(
    *,
    task: Task,
    revision: int,
    corrections: Dict[str, Any],
) -> Dict[str, Any]:
    task_id = str(task.id)
    base_url = str(APP_URL).rstrip("/")
    corrections_digest = payload_sha256(corrections)
    return {
        "schema": EXPORT_SCHEMA,
        "taskId": task_id,
        "taskGuid": str(task.guid or task.id),
        "revision": int(revision),
        "createdAt": utc_now_iso(),
        "idempotencyKey": f"{task_id}:{int(revision)}:{corrections_digest}",
        "correctionsSha256": corrections_digest,
        "source": {
            "preparedGlbUrl": f"{base_url}/api/task/{task_id}/prepared.glb",
            "animationsGlbUrl": f"{base_url}/api/task/{task_id}/animations.glb",
            "animationManifestUrl": f"{base_url}/api/task/{task_id}/animation-manifest",
            "integrity": {
                "policy": "worker_compute_and_pin",
                "expectedSha256": {
                    "preparedGlb": None,
                    "animationsGlb": None,
                    "animationManifest": None,
                },
                "requiredComputedSha256": [
                    "preparedGlb",
                    "animationsGlb",
                    "animationManifest",
                ],
            },
        },
        "outputs": {
            "multiClipGlb": True,
            "perClipFbxZip": True,
            "preserveOriginals": True,
        },
        "corrections": corrections,
        "callback": {
            "url": f"{base_url}/api/internal/task/{task_id}/animation-corrections/export/{int(revision)}",
            "auth": {"header": "Authorization", "scheme": "Bearer"},
        },
    }


def write_export_contract(contract: Dict[str, Any]) -> Path:
    task_id = str(contract["taskId"])
    revision = int(contract["revision"])
    target_dir = EXPORT_ROOT / task_id / f"revision-{revision}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "export-contract.json"
    temp = target.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps(contract, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    temp.replace(target)
    return target


async def dispatch_animation_correction_export(task_id: str, revision: int) -> None:
    """Persist an immutable job and submit it to the configured Blender worker."""
    async with AsyncSessionLocal() as db:
        task = (
            await db.execute(select(Task).where(Task.id == task_id))
        ).scalar_one_or_none()
        state = (
            await db.execute(
                select(TaskAnimationCorrection).where(TaskAnimationCorrection.task_id == task_id)
            )
        ).scalar_one_or_none()
        if not task or not state or int(state.published_revision or 0) != int(revision):
            return
        corrections = load_json_object(state.published_json)
        if not corrections:
            state.export_status = "failed"
            state.export_error = "Published corrections are missing or invalid"
            await db.commit()
            return

        contract = build_export_contract(task=task, revision=revision, corrections=corrections)
        try:
            contract_path = write_export_contract(contract)
        except Exception as exc:
            state.export_status = "failed"
            state.export_error = f"Could not persist export contract: {exc}"
            await db.commit()
            return

        if not EXPORT_WEBHOOK:
            state.export_status = "awaiting_worker"
            state.export_error = None
            await db.commit()
            return

        state.export_status = "submitting"
        state.export_error = None
        await db.commit()

        headers = {"Accept": "application/json"}
        if EXPORT_TOKEN:
            headers["Authorization"] = f"Bearer {EXPORT_TOKEN}"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
                response = await client.post(
                    EXPORT_WEBHOOK,
                    json={**contract, "contractPath": str(contract_path)},
                    headers=headers,
                )
                response.raise_for_status()
                result = response.json() if response.content else {}
        except Exception as exc:
            state.export_status = "failed"
            state.export_error = f"Export worker submission failed: {exc}"
            await db.commit()
            return

        if int(state.published_revision or 0) != int(revision):
            return
        status = str(result.get("status") or "processing").strip().lower()
        state.export_status = status if status in {
            "queued", "processing", "ready", "failed"
        } else "processing"
        state.corrected_glb_url = str(result.get("correctedGlbUrl") or "").strip() or None
        state.corrected_fbx_zip_url = str(result.get("correctedFbxZipUrl") or "").strip() or None
        try:
            source_sha256 = normalize_source_sha256(
                result.get("sourceSha256"),
                required=state.export_status == "ready",
            )
        except ValueError as exc:
            state.export_status = "failed"
            state.export_error = f"Export worker returned invalid source integrity: {exc}"
            await db.commit()
            return
        state.source_sha256_json = (
            json.dumps(source_sha256, sort_keys=True, separators=(",", ":"))
            if isinstance(source_sha256, dict)
            else None
        )
        state.export_error = str(result.get("error") or "").strip() or None
        await db.commit()


async def apply_export_result(
    *,
    task_id: str,
    revision: int,
    status: str,
    corrected_glb_url: Optional[str],
    corrected_fbx_zip_url: Optional[str],
    source_sha256: Optional[Dict[str, str]],
    error: Optional[str],
) -> bool:
    async with AsyncSessionLocal() as db:
        state = (
            await db.execute(
                select(TaskAnimationCorrection).where(TaskAnimationCorrection.task_id == task_id)
            )
        ).scalar_one_or_none()
        if not state or int(state.published_revision or 0) != int(revision):
            return False
        normalized_status = str(status or "").strip().lower()
        if normalized_status not in {"queued", "processing", "ready", "failed"}:
            raise ValueError("invalid export status")
        state.export_status = normalized_status
        state.corrected_glb_url = str(corrected_glb_url or "").strip() or None
        state.corrected_fbx_zip_url = str(corrected_fbx_zip_url or "").strip() or None
        state.source_sha256_json = (
            json.dumps(source_sha256, sort_keys=True, separators=(",", ":"))
            if source_sha256
            else None
        )
        state.export_error = str(error or "").strip() or None
        await db.commit()
        return True

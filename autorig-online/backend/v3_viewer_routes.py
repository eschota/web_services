"""Read-only, fail-closed API for the experimental V3 pipeline viewer.

The converter publishes immutable, hash-bound manifests into a per-task
directory.  This module never guesses artifact paths and never turns legacy
outputs into completed V3 stages.  It is intentionally independent from the
converter so the viewer can ship before the V3 pipeline becomes a default.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from sqlalchemy import select, text


_TASK_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ARTIFACT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_COUNT_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ALLOWED_ARTIFACT_TYPES = {
    "voxel_points",
    "skeleton_graph",
    "fitted_bones",
    "skin_weights",
    "skinned_model",
    "deformation_clip",
}
_ARTIFACT_STAGE_BINDINGS = {
    "voxel_points": {"c1_surface", "c2_solid", "c3_thin"},
    "skeleton_graph": {"c4_graph"},
    "skin_weights": {"s2_weights"},
    "fitted_bones": {"r1_bones"},
    "skinned_model": {"s3_skin"},
    "deformation_clip": {"final_deformation"},
}
_ALLOWED_MEDIA_TYPES = {
    ".json": "application/json",
    ".bin": "application/octet-stream",
    ".glb": "model/gltf-binary",
}
_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
_MAX_TOTAL_ARTIFACT_BYTES = 128 * 1024 * 1024
_MAX_TOTAL_POINTS = 500_000
_MAX_TOTAL_VERTICES = 5_000_000
_MAX_EXAMPLES = 12
_STAGE_ORDER = ("source", "c1_surface", "c2_solid", "c3_thin", "c4_graph", "r1_bones", "s1_owner", "s2_weights", "s3_skin", "final_deformation")


class V3ManifestError(ValueError):
    pass


def _artifact_root() -> Path:
    return Path(os.getenv("AUTORIG_V3_ARTIFACT_ROOT", "/srv/autorig/data/v3")).resolve()


def _task_dir(task_id: str) -> Path:
    if not _TASK_ID_RE.fullmatch(str(task_id or "").lower()):
        raise HTTPException(status_code=404, detail="Task not found")
    root = _artifact_root()
    candidate = root / task_id.lower()
    if candidate.is_symlink():
        raise HTTPException(status_code=409, detail="V3 task directory may not be a symlink")
    target = candidate.resolve()
    if target.parent != root:
        raise HTTPException(status_code=404, detail="Task not found")
    return target


def _read_regular_nofollow(path: Path, maximum: int) -> bytes:
    if path.is_symlink():
        raise V3ManifestError(f"symlinks are not allowed: {path.name}")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(str(path), flags)
    except OSError as exc:
        raise V3ManifestError(f"cannot open regular file: {path.name}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size < 0 or info.st_size > maximum:
            raise V3ManifestError(f"file size/type is invalid: {path.name}")
        chunks = []
        remaining = info.st_size
        while remaining:
            block = os.read(fd, min(1024 * 1024, remaining))
            if not block:
                raise V3ManifestError(f"file changed while reading: {path.name}")
            chunks.append(block)
            remaining -= len(block)
        if os.read(fd, 1):
            raise V3ManifestError(f"file grew while reading: {path.name}")
        return b"".join(chunks)
    finally:
        os.close(fd)


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _as_nonnegative_int(value: Any, field: str, maximum: int = 100_000_000) -> int:
    if type(value) is not int:
        raise V3ManifestError(f"{field} must be an integer")
    result = value
    if result < 0 or result > maximum:
        raise V3ManifestError(f"{field} is out of range")
    return result


def validate_v3_manifest(raw: Any, *, task_id: str, task_dir: Path, verify_files: bool = True) -> Dict[str, Any]:
    if not isinstance(raw, dict) or raw.get("schema") != "autorig.v3.viewer-manifest/1":
        raise V3ManifestError("unsupported manifest schema")
    if str(raw.get("task_id") or "").lower() != task_id.lower():
        raise V3ManifestError("task identity mismatch")
    build = raw.get("build")
    if not isinstance(build, dict) or not _SHA256_RE.fullmatch(str(build.get("source_sha256") or "")):
        raise V3ManifestError("build.source_sha256 is required")
    if not _SHA256_RE.fullmatch(str(build.get("manifest_input_sha256") or "")):
        raise V3ManifestError("build.manifest_input_sha256 is required")

    stages = raw.get("stages")
    if not isinstance(stages, list) or len(stages) > len(_STAGE_ORDER):
        raise V3ManifestError("stages must be a bounded array")
    seen = set()
    clean_stages = []
    for stage in stages:
        if not isinstance(stage, dict):
            raise V3ManifestError("stage must be an object")
        name = str(stage.get("name") or "")
        status = str(stage.get("status") or "")
        if name not in _STAGE_ORDER or name in seen or status not in {"complete", "failed", "pending", "unavailable"}:
            raise V3ManifestError("invalid stage identity or status")
        seen.add(name)
        duration_ms = stage.get("duration_ms")
        if duration_ms is not None:
            duration_ms = _as_nonnegative_int(duration_ms, f"{name}.duration_ms", 86_400_000)
        receipt_sha = stage.get("receipt_sha256")
        if status == "complete" and not _SHA256_RE.fullmatch(str(receipt_sha or "")):
            raise V3ManifestError(f"{name}.receipt_sha256 is required for complete stages")
        counts = stage.get("counts") or {}
        if not isinstance(counts, dict) or len(counts) > 16:
            raise V3ManifestError(f"{name}.counts is invalid")
        clean_counts = {}
        for key, value in counts.items():
            if not isinstance(key, str) or not _COUNT_KEY_RE.fullmatch(key):
                raise V3ManifestError(f"{name}.counts key is invalid")
            clean_counts[key] = _as_nonnegative_int(value, f"{name}.counts.{key}")
        clean_stages.append({
            "name": name,
            "status": status,
            "duration_ms": duration_ms,
            "receipt_sha256": receipt_sha if receipt_sha else None,
            "counts": clean_counts,
            "message": str(stage.get("message") or "")[:500],
        })
    if [stage["name"] for stage in clean_stages] != list(_STAGE_ORDER):
        raise V3ManifestError("manifest must declare every stage in canonical order")
    prefix_open = True
    for stage in clean_stages:
        if stage["status"] == "complete" and not prefix_open:
            raise V3ManifestError(f"{stage['name']} cannot complete before its prerequisite stage")
        if stage["status"] != "complete":
            prefix_open = False
    stage_map = {stage["name"]: stage for stage in clean_stages}

    artifacts = raw.get("artifacts") or []
    if not isinstance(artifacts, list) or len(artifacts) > 16:
        raise V3ManifestError("artifacts must be a bounded array")
    clean_artifacts = []
    seen_names = set()
    total_artifact_bytes = 0
    total_points = 0
    total_vertices = 0
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise V3ManifestError("artifact must be an object")
        name = str(artifact.get("name") or "")
        kind = str(artifact.get("type") or "")
        stage_name = str(artifact.get("stage") or "")
        filename = str(artifact.get("file") or "")
        digest = str(artifact.get("sha256") or "")
        if not _ARTIFACT_NAME_RE.fullmatch(name) or name in seen_names:
            raise V3ManifestError("invalid artifact name")
        if kind not in _ALLOWED_ARTIFACT_TYPES or not _ARTIFACT_NAME_RE.fullmatch(filename):
            raise V3ManifestError("invalid artifact type or file")
        if stage_name not in _ARTIFACT_STAGE_BINDINGS[kind]:
            raise V3ManifestError("artifact type is not valid for its stage")
        stage = stage_map.get(stage_name)
        if not stage or stage["status"] != "complete":
            raise V3ManifestError("artifact stage must be complete")
        if not _SHA256_RE.fullmatch(digest):
            raise V3ManifestError("artifact sha256 is required")
        path = task_dir / filename
        if path.parent != task_dir or path.suffix.lower() not in _ALLOWED_MEDIA_TYPES:
            raise V3ManifestError("artifact path is not allowed")
        suffix = path.suffix.lower()
        if kind in {"voxel_points", "skeleton_graph", "fitted_bones"} and suffix != ".json":
            raise V3ManifestError("overlay artifacts must be JSON")
        if kind in {"skinned_model", "deformation_clip"} and suffix != ".glb":
            raise V3ManifestError("model/deformation artifacts must be GLB")
        if kind == "skin_weights" and suffix not in {".json", ".bin"}:
            raise V3ManifestError("skin weight artifact must be JSON or binary")
        declared_bytes = _as_nonnegative_int(artifact.get("bytes"), f"artifact.{name}.bytes", _MAX_ARTIFACT_BYTES)
        total_artifact_bytes += declared_bytes
        if total_artifact_bytes > _MAX_TOTAL_ARTIFACT_BYTES:
            raise V3ManifestError("aggregate artifact bytes exceed the publication limit")
        if verify_files:
            data = _read_regular_nofollow(path, _MAX_ARTIFACT_BYTES)
            if len(data) != declared_bytes or _sha256_bytes(data) != digest:
                raise V3ManifestError(f"artifact {name} failed integrity verification")
        seen_names.add(name)
        points = _as_nonnegative_int(artifact.get("points", 0), f"artifact.{name}.points", 250_000)
        vertices = _as_nonnegative_int(artifact.get("vertices", 0), f"artifact.{name}.vertices", 5_000_000)
        total_points += points
        total_vertices += vertices
        if total_points > _MAX_TOTAL_POINTS or total_vertices > _MAX_TOTAL_VERTICES:
            raise V3ManifestError("aggregate artifact element count exceeds the publication limit")
        clean_artifacts.append({
            "name": name,
            "type": kind,
            "stage": stage_name,
            "stage_receipt_sha256": stage["receipt_sha256"],
            "file": filename,
            "sha256": digest,
            "bytes": declared_bytes,
            "url": f"/api/v3/task/{task_id}/artifact/{name}",
            "points": points,
            "vertices": vertices,
        })

    raw_model = raw.get("model") if isinstance(raw.get("model"), dict) else {}
    model = {
        "input_type": str(raw_model.get("input_type") or "")[:50],
        "coordinate_space": str(raw_model.get("coordinate_space") or "model_local_gltf")[:50],
        "vertices": _as_nonnegative_int(raw_model.get("vertices", 0), "model.vertices", 5_000_000),
        "triangles": _as_nonnegative_int(raw_model.get("triangles", 0), "model.triangles", 10_000_000),
    }
    skinned_artifact = next((item for item in clean_artifacts if item["type"] == "skinned_model"), None)
    deformation_artifact = next((item for item in clean_artifacts if item["type"] == "deformation_clip"), None)
    model.update({
        "source_url": f"/api/task/{task_id}/prepared.glb",
        "skinned_url": skinned_artifact["url"] if skinned_artifact else None,
        "deformation_url": deformation_artifact["url"] if deformation_artifact else None,
    })

    required_visual = {
        "c1_surface": "voxel_points",
        "c2_solid": "voxel_points",
        "c3_thin": "voxel_points",
        "c4_graph": "skeleton_graph",
        "s2_weights": "skin_weights",
        "s3_skin": "skinned_model",
        "final_deformation": "deformation_clip",
    }
    for stage_name, artifact_type in required_visual.items():
        stage = stage_map.get(stage_name)
        if stage and stage["status"] == "complete" and not any(
            item["stage"] == stage_name and item["type"] == artifact_type for item in clean_artifacts
        ):
            raise V3ManifestError(f"{stage_name} requires a {artifact_type} artifact")

    return {
        "schema": raw["schema"],
        "task_id": task_id,
        "build": {
            "version": str(build.get("version") or "")[:80],
            "source_sha256": build["source_sha256"],
            "manifest_input_sha256": build["manifest_input_sha256"],
            "created_at": str(build.get("created_at") or "")[:64],
        },
        "model": model,
        "stages": clean_stages,
        "artifacts": clean_artifacts,
    }


def _load_manifest(task_id: str) -> Optional[Dict[str, Any]]:
    task_dir = _task_dir(task_id)
    path = task_dir / "manifest.json"
    if not path.exists():
        return None
    try:
        data = _read_regular_nofollow(path, 512 * 1024)
        return validate_v3_manifest(json.loads(data.decode("utf-8")), task_id=task_id, task_dir=task_dir)
    except (OSError, json.JSONDecodeError, V3ManifestError) as exc:
        raise HTTPException(status_code=409, detail=f"V3 manifest is unavailable: {exc}") from exc


def _stage_placeholders() -> list[dict]:
    return [
        {
            "name": name,
            "status": "complete" if name == "source" else "unavailable",
            "duration_ms": None,
            "receipt_sha256": None,
            "counts": {},
            "message": "Existing public model" if name == "source" else "No hash-bound V3 artifact has been published for this stage",
        }
        for name in _STAGE_ORDER
    ]


def _fallback_manifest(task: Any) -> Dict[str, Any]:
    return {
        "schema": "autorig.v3.viewer-manifest/1",
        "task_id": task.id,
        "build": {
            "version": "legacy-source-only",
            "source_sha256": None,
            "manifest_input_sha256": None,
            "created_at": None,
            "hash_bound": False,
        },
        "model": {
            "input_type": str(getattr(task, "input_type", "") or ""),
            "source_url": f"/api/task/{task.id}/prepared.glb",
            "skinned_url": None,
            "deformation_url": None,
        },
        "stages": _stage_placeholders(),
        "artifacts": [],
        "notice": "Only the existing source model is available. V3 stages remain unavailable until verified artifacts are published.",
    }


def _can_access_task(task: Any, *, is_public: bool, user: Any, request: Any, is_admin_email: Callable[[Optional[str]], bool]) -> bool:
    if is_public:
        return True
    if user and is_admin_email(getattr(user, "email", None)):
        return True
    if user and task.owner_type == "user" and task.owner_id == getattr(user, "email", None):
        return True
    anon_id = getattr(request.state, "api_key_anon_id", None) or request.cookies.get("anon_id")
    return bool(task.owner_type == "anon" and task.owner_id and task.owner_id == anon_id)


def build_v3_viewer_router(
    *,
    get_db: Callable[..., Any],
    get_current_user: Callable[..., Any],
    task_model: Any,
    is_admin_email: Callable[[Optional[str]], bool],
    static_dir: Path,
) -> APIRouter:
    router = APIRouter()

    async def _task_and_access(task_id: str, request: Request, user: Any, db: Any) -> Any:
        if not _TASK_ID_RE.fullmatch(str(task_id or "").lower()):
            raise HTTPException(status_code=404, detail="Task not found")
        task = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        row = (await db.execute(text("SELECT is_public FROM tasks WHERE id = :task_id"), {"task_id": task_id})).first()
        is_public = bool(row and row[0])
        if not _can_access_task(task, is_public=is_public, user=user, request=request, is_admin_email=is_admin_email):
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    @router.get("/viewer")
    @router.get("/viewer/{task_id}")
    @router.get("/task/{task_id}/viewer")
    @router.get("/{task_id}/viewer")
    async def v3_viewer_page(task_id: Optional[str] = None):
        if task_id is not None and not _TASK_ID_RE.fullmatch(str(task_id).lower()):
            raise HTTPException(status_code=404, detail="Task not found")
        return FileResponse(static_dir / "viewer-v3.html", media_type="text/html", headers={"Cache-Control": "no-store"})

    @router.get("/api/v3/task/{task_id}/manifest")
    async def v3_manifest(
        task_id: str,
        request: Request,
        user: Any = Depends(get_current_user),
        db: Any = Depends(get_db),
    ):
        task = await _task_and_access(task_id, request, user, db)
        manifest = _load_manifest(task_id) or _fallback_manifest(task)
        return JSONResponse(manifest, headers={"Cache-Control": "no-store", "X-Robots-Tag": "noindex"})

    @router.get("/api/v3/task/{task_id}/artifact/{artifact_name}")
    async def v3_artifact(
        task_id: str,
        artifact_name: str,
        request: Request,
        user: Any = Depends(get_current_user),
        db: Any = Depends(get_db),
    ):
        await _task_and_access(task_id, request, user, db)
        manifest = _load_manifest(task_id)
        if not manifest:
            raise HTTPException(status_code=404, detail="V3 artifact not available")
        artifact = next((item for item in manifest["artifacts"] if item["name"] == artifact_name), None)
        if not artifact:
            raise HTTPException(status_code=404, detail="V3 artifact not available")
        task_dir = _task_dir(task_id)
        path = task_dir / artifact["file"]
        if path.parent != task_dir:
            raise HTTPException(status_code=404, detail="V3 artifact not available")
        try:
            content = _read_regular_nofollow(path, _MAX_ARTIFACT_BYTES)
        except V3ManifestError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if len(content) != artifact["bytes"] or _sha256_bytes(content) != artifact["sha256"]:
            raise HTTPException(status_code=409, detail="V3 artifact integrity changed")
        return Response(
            content=content,
            media_type=_ALLOWED_MEDIA_TYPES[path.suffix.lower()],
            headers={
                # A private task owner may use the same endpoint.  Never allow
                # a shared proxy cache to turn that response into public data.
                "Cache-Control": "private, no-store, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
                "ETag": f'"{artifact["sha256"]}"',
                "X-Content-Type-Options": "nosniff",
            },
        )

    @router.get("/api/v3/viewer/examples")
    async def v3_examples(db: Any = Depends(get_db)):
        root = _artifact_root()
        published_ids = []
        if root.is_dir():
            for entry in root.iterdir():
                if len(published_ids) >= 256:
                    break
                if entry.is_symlink() or not entry.is_dir() or not _TASK_ID_RE.fullmatch(entry.name.lower()):
                    continue
                if (entry / "manifest.json").exists():
                    published_ids.append(entry.name.lower())
        if not published_ids:
            return {"schema": "autorig.v3.viewer-examples/1", "examples": []}
        params = {f"task_{index}": task_id for index, task_id in enumerate(published_ids)}
        placeholders = ",".join(f":task_{index}" for index in range(len(published_ids)))
        rows = (await db.execute(text(
            "SELECT id, input_type, created_at FROM tasks "
            f"WHERE is_public = 1 AND status = 'done' AND id IN ({placeholders}) "
            "ORDER BY created_at DESC"
        ), params)).all()
        examples = []
        for task_id, input_type, created_at in rows:
            try:
                manifest = _load_manifest(task_id)
            except HTTPException:
                # A corrupt/incomplete publication is not an example and must
                # not take down the whole selector. Its direct manifest route
                # still exposes the explicit 409 diagnostic to its owner.
                continue
            if not manifest:
                continue
            examples.append({
                "task_id": task_id,
                "input_type": input_type,
                "created_at": str(created_at or ""),
                "url": f"/viewer/{task_id}",
                "complete_stages": [stage["name"] for stage in manifest["stages"] if stage["status"] == "complete"],
                "build": manifest["build"],
            })
            if len(examples) >= _MAX_EXAMPLES:
                break
        return {"schema": "autorig.v3.viewer-examples/1", "examples": examples}

    return router


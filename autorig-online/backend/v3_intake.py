"""V3 intake: any input -> one exact, hash-bound GLB source -> Task + V3 binding.

Owner rule 2026-10-10: every input format enters the same conveyor.

* A GLB upload is admitted at once: the source bytes are stored immutably by
  their SHA-256 and the Task row and its V3 binding are written in one commit.
* FBX and OBJ are normalized to GLB first.  The Task row exists from the start
  with ``pipeline_kind="v3"`` and an intake record, so the legacy scheduler never
  sees it; :func:`pump_v3_intake` binds the exact GLB once it exists.
* Images, video and text become a mesh through the existing generation
  (Renderfin: image/T-pose render -> Hunyuan3D); the generated GLB re-enters
  here with a hash-bound generation receipt (:func:`bind_generated_task`,
  :func:`admit_generated_glb`).

Nothing here falls back to the legacy rig: a source that cannot be normalized
ends the Task with an explicit error.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import struct
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

INTAKE_ROOT = Path(os.getenv("AUTORIG_V3_INTAKE_ROOT", "/srv/autorig/data/v3-intake"))
SOURCE_ROOT = INTAKE_ROOT / "sources"
SOURCE_SCHEMA = "autorig.v3.source/1"
GENERATION_RECEIPT_SCHEMA = "autorig.v3.generation-receipt/1"
INTENT_PLAN_SCHEMA = "autorig.v3.intent-plan/1"
MAX_SOURCE_BYTES = 512 * 1024 * 1024
ROUTES = ("website", "telegram", "api", "generation", "retry", "convert")
INTAKE_STATES_OPEN = ("normalizing",)


class V3IntakeError(ValueError):
    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail = code, detail


# ---------------------------------------------------------------- routing policy
def _route_set(value: str) -> set[str]:
    return {item.strip().lower() for item in str(value or "").split(",") if item.strip()}


ROUTES_FILE = Path(os.getenv("AUTORIG_V3_ROUTES_FILE", "/srv/autorig/live/config/v3-routes.json"))
_ROUTES_CACHE: Dict[str, Any] = {"key": None, "value": None}


def _live_routes() -> Optional[Dict[str, str]]:
    """The live switch file (no restart): {"routes": "website,api" | "all", "admin_routes": "all"}.
    Absent or unreadable -> None, and the environment decides."""
    try:
        st = ROUTES_FILE.stat()
    except OSError:
        return None
    key = (st.st_ino, st.st_size, st.st_mtime_ns)
    if _ROUTES_CACHE["key"] == key:
        return _ROUTES_CACHE["value"]
    try:
        data = json.loads(ROUTES_FILE.read_text(encoding="utf-8"))
        value = {"routes": ",".join(data["routes"]) if isinstance(data.get("routes"), list) else str(data.get("routes") or ""),
                 "admin_routes": ",".join(data["admin_routes"]) if isinstance(data.get("admin_routes"), list)
                 else str(data.get("admin_routes") if data.get("admin_routes") is not None else "all"),
                 "classic_new_tasks": str(data.get("classic_new_tasks") or "on").strip().lower()}
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        value = None
    _ROUTES_CACHE.update(key=key, value=value)
    return value


def classic_new_tasks_enabled() -> bool:
    """Downloads · V3 (owner 2026-10-10: «отмени нахрен старый пайплайн конвертации, оставь только новый»).

    ``"classic_new_tasks": "off"`` in the live routes file sends every new task of every route to V3 and makes
    ``tasks.create_conversion_task`` refuse new classic (rig / convert) rows. Tasks that already exist, queued or
    running, finish on the classic converter as before. Rollback: ``"classic_new_tasks": "on"`` (no restart).
    ``AUTORIG_CLASSIC_NEW_TASKS=off`` does the same when the file is absent."""
    live = _live_routes()
    value = live.get("classic_new_tasks") if live else os.getenv("AUTORIG_CLASSIC_NEW_TASKS", "on")
    return str(value or "on").strip().lower() not in ("off", "0", "false", "no")


def route_enabled(route: str, *, is_admin: bool = False, explicit: bool = False) -> bool:
    """Whether a new task on this creation route goes to V3.

    The live file ``/srv/autorig/live/config/v3-routes.json`` (read on change, no
    restart) or else ``AUTORIG_V3_ROUTES`` (comma list or ``all``) switches routes
    for everyone; ``admin_routes`` / ``AUTORIG_V3_ADMIN_ROUTES`` (default ``all``)
    for administrator accounts.  An explicit ``pipeline=v3`` request always gets
    V3.  A V3 task never falls back to the legacy rig, whatever these switches say later.
    """
    if explicit or not classic_new_tasks_enabled():
        return True
    route = str(route or "").strip().lower()
    live = _live_routes()
    public = _route_set(live["routes"] if live else os.getenv("AUTORIG_V3_ROUTES", ""))
    if "all" in public or route in public:
        return True
    admin = _route_set(live["admin_routes"] if live else os.getenv("AUTORIG_V3_ADMIN_ROUTES", "all"))
    return bool(is_admin and ("all" in admin or route in admin))


# ---------------------------------------------------------------- source bytes
def local_upload_path(url: str) -> Optional[Path]:
    """The stored file behind one of our own ``/u/<token>/<name>`` upload URLs, else None."""
    from urllib.parse import unquote, urlparse

    from config import APP_URL, UPLOAD_DIR

    parsed, app = urlparse(str(url or "")), urlparse(str(APP_URL or ""))
    if parsed.netloc and parsed.netloc != app.netloc:
        return None
    parts = parsed.path.split("/")
    if len(parts) != 4 or parts[1] != "u":
        return None
    token, name = unquote(parts[2]), unquote(parts[3])
    if not re.fullmatch(r"[0-9a-f-]{8,64}", token) or name in ("", ".", "..") or "/" in name or "\\" in name:
        return None
    root = Path(UPLOAD_DIR).resolve()
    path = root / token / name
    if path.is_symlink() or not path.is_file() or path.resolve().parent.parent != root:
        return None
    return path


def sniff_format(path: Path) -> Optional[str]:
    with Path(path).open("rb") as stream:
        head = stream.read(65536)
    if head[:4] == b"glTF":
        return "glb"
    if head[:8] == bytes.fromhex("89504e470d0a1a0a") or head[:3] == bytes.fromhex("ffd8ff") or \
            (head[:4] == b"RIFF" and head[8:12] == b"WEBP"):
        return "image"
    if head[4:8] == b"ftyp" or head[:4] == bytes.fromhex("1a45dfa3"):
        return "video"
    if head.startswith(b"Kaydara FBX Binary") or head.lstrip().startswith(b"; FBX"):
        return "fbx"
    if b"\0" not in head[:4096] and re.search(rb"(?m)^\s*v\s+[-+0-9.eE]+\s+[-+0-9.eE]+", head):
        return "obj"
    return None


def glb_facts(data: bytes) -> Dict[str, Any]:
    if len(data) < 20 or data[:4] != b"glTF" or struct.unpack_from("<I", data, 4)[0] != 2:
        raise V3IntakeError("invalid_glb", "the file is not a GLB 2.0 container")
    if struct.unpack_from("<I", data, 8)[0] != len(data):
        raise V3IntakeError("invalid_glb", "the GLB length header does not match the file")
    json_len, kind = struct.unpack_from("<II", data, 12)
    if kind != 0x4E4F534A or 20 + json_len > len(data):
        raise V3IntakeError("invalid_glb", "the GLB JSON chunk is missing")
    try:
        doc = json.loads(data[20:20 + json_len].decode("utf-8").rstrip(" \t\r\n\x00"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise V3IntakeError("invalid_glb", "the GLB JSON chunk cannot be decoded") from exc
    meshes = doc.get("meshes") or []
    primitives = [p for mesh in meshes for p in (mesh.get("primitives") or [])]
    if not primitives:
        raise V3IntakeError("invalid_glb", "the GLB has no mesh to rig")
    return {"meshes": len(meshes), "primitives": len(primitives),
            "materials": len(doc.get("materials") or []), "skins": len(doc.get("skins") or []),
            "animations": len(doc.get("animations") or []),
            "extensions_required": sorted(doc.get("extensionsRequired") or [])}


def store_source(data: bytes) -> tuple[Path, str]:
    """Write the exact bytes once, addressed by their SHA-256 (atomic, immutable)."""
    if not data or len(data) > MAX_SOURCE_BYTES:
        raise V3IntakeError("source_size", "the source is empty or larger than 512 MB")
    digest = hashlib.sha256(data).hexdigest()
    SOURCE_ROOT.mkdir(parents=True, exist_ok=True)
    target = SOURCE_ROOT / f"{digest}.glb"
    if target.is_file() and not target.is_symlink():
        if hashlib.sha256(target.read_bytes()).hexdigest() == digest:
            return target, digest
    tmp = SOURCE_ROOT / f".{digest}.{uuid.uuid4().hex}.tmp"
    with tmp.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(tmp, 0o644)
    os.replace(tmp, target)
    return target, digest


def _digest_json(body: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(dict(body), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def generation_receipt(task_id: str, source_sha256: str, **facts: Any) -> Dict[str, Any]:
    body = {"schema": GENERATION_RECEIPT_SCHEMA, "status": "accepted", "task_id": task_id,
            "source_sha256": source_sha256, **{k: v for k, v in facts.items() if v not in (None, "")}}
    return {**body, "receipt_sha256": _digest_json(body)}


def convert_plan(task_id: str, source_sha256: str, **facts: Any) -> Dict[str, Any]:
    body = {"schema": INTENT_PLAN_SCHEMA, "status": "accepted", "task_id": task_id,
            "source_sha256": source_sha256, "requested_intent": "convert", "dispatch_intent": "rig",
            "why": "V3 convert = the V3 rig conveyor plus its exports; the intent is kept separately",
            **{k: v for k, v in facts.items() if v not in (None, "")}}
    return {**body, "plan_sha256": _digest_json(body)}


def build_binding(task_id: str, source_path: Path, source_sha256: str, *, requested_intent: str = "rig",
                  origin: str, filename: str = "", original: Optional[Mapping[str, Any]] = None,
                  normalization: Optional[Mapping[str, Any]] = None, facts: Optional[Mapping[str, Any]] = None,
                  receipt: Optional[Mapping[str, Any]] = None, attempt: int = 1):
    from v3_task_runtime import normalize_task_binding

    manifest: Dict[str, Any] = {
        "schema": SOURCE_SCHEMA, "intent": "rig", "path": str(source_path), "sha256": source_sha256,
        "bytes": Path(source_path).stat().st_size, "filename": str(filename or "")[:200], "origin": origin,
        "facts": dict(facts or {}), "original": dict(original or {}), "normalization": dict(normalization or {}),
    }
    upstream = plan = None
    if requested_intent == "generate":
        upstream = dict(receipt or {})
        manifest["upstream_receipt_sha256"] = upstream.get("receipt_sha256")
    elif requested_intent == "convert":
        plan = convert_plan(task_id, source_sha256, origin=origin)
    return normalize_task_binding(task_id=task_id, source_sha256=source_sha256, source_manifest=manifest,
                                  requested_intent=requested_intent, attempt=attempt,
                                  upstream_receipt=upstream, intent_plan=plan)


def _v3_settings(state: str, **intake: Any) -> Dict[str, Any]:
    return {"v3": {"state": state, "stage": "intake", "progress": 0.0,
                   "intake": {k: v for k, v in intake.items() if v is not None},
                   "updated_at": datetime.utcnow().isoformat() + "Z"}}


# ---------------------------------------------------------------- admission
async def admit_glb(db, *, data: bytes, original_url: str, filename: str, owner_type: str, owner_id: str,
                    origin: str, created_via_api: bool = False, requested_intent: str = "rig",
                    input_type: str = "t_pose", input_bytes: Optional[int] = None,
                    original: Optional[Mapping[str, Any]] = None, receipt_facts: Optional[Mapping[str, Any]] = None,
                    collection_metadata: Optional[Dict[str, Any]] = None, queue_class: str = "interactive",
                    normalization: Optional[Mapping[str, Any]] = None):
    """A ready GLB: one Task + binding commit; the runtime picks it up at once."""
    from tasks import create_conversion_task

    facts = glb_facts(data)
    path, digest = store_source(data)
    task_id = str(uuid.uuid4())
    receipt = generation_receipt(task_id, digest, **dict(receipt_facts or {})) \
        if requested_intent == "generate" else None
    binding = build_binding(task_id, path, digest, requested_intent=requested_intent, origin=origin,
                            filename=filename, original=original, facts=facts, receipt=receipt,
                            normalization=normalization)
    task, error = await create_conversion_task(
        db, original_url, input_type, owner_type, owner_id, created_via_api=created_via_api,
        pipeline_kind="v3", input_bytes=input_bytes if input_bytes is not None else len(data),
        collection_metadata=collection_metadata, queue_class=queue_class, v3_binding=binding,
        viewer_settings=_v3_settings("pending", origin=origin, format="glb", filename=filename[:200],
                                     source_sha256=digest, requested_intent=requested_intent))
    if task is None:
        raise V3IntakeError("task_create_failed", error or "the task could not be created")
    print(f"[V3 intake] {task.id} admitted {origin} GLB sha={digest[:12]} ({len(data)} bytes)")
    _notify_new(task.id)
    return task


async def admit_upload(db, *, path: Path, original_url: str, filename: str, owner_type: str, owner_id: str,
                       origin: str, created_via_api: bool = False, requested_intent: str = "rig",
                       input_type: str = "t_pose", input_bytes: Optional[int] = None):
    """Any uploaded mesh: GLB now, FBX/OBJ through format normalization."""
    fmt = sniff_format(path)
    original = {"url": original_url, "filename": str(filename or "")[:200], "format": fmt,
                "bytes": Path(path).stat().st_size}
    if fmt == "glb":
        data = Path(path).read_bytes()
        original["sha256"] = hashlib.sha256(data).hexdigest()
        return await admit_glb(db, data=data, original_url=original_url, filename=filename,
                               owner_type=owner_type, owner_id=owner_id, origin=origin,
                               created_via_api=created_via_api, requested_intent=requested_intent,
                               input_type=input_type, input_bytes=input_bytes, original=original)
    if fmt == "fbx":
        # FBX (ASCII repaired first) -> GLB with assimp right here; only if that
        # fails does the task wait for a converter in the intake pump.
        import fbx_ascii

        glb = Path(path).with_name(Path(path).stem + ".v3.glb")
        try:
            receipt = await asyncio.to_thread(fbx_ascii.fbx_to_glb, Path(path), glb)
        except Exception as exc:
            print(f"[V3 intake] local FBX -> GLB failed ({exc}); waiting for a converter")
        else:
            original["sha256"] = receipt["source_sha256"]
            return await admit_glb(db, data=glb.read_bytes(), original_url=original_url, filename=filename,
                                   owner_type=owner_type, owner_id=owner_id, origin=origin,
                                   created_via_api=created_via_api, requested_intent=requested_intent,
                                   input_type=input_type, input_bytes=input_bytes, original=original,
                                   normalization=receipt)
    if fmt in ("fbx", "obj"):
        return await _create_normalizing_task(db, original=original, owner_type=owner_type, owner_id=owner_id,
                                              origin=origin, created_via_api=created_via_api,
                                              requested_intent=requested_intent, input_type=input_type,
                                              input_bytes=input_bytes)
    if fmt in ("image", "video"):
        return await _create_generation_task(db, path=Path(path), fmt=fmt, original_url=original_url,
                                             owner_type=owner_type, owner_id=owner_id,
                                             created_via_api=created_via_api, input_bytes=input_bytes)
    raise V3IntakeError("unsupported_format", "V3 accepts GLB, FBX and OBJ meshes, images and video")


async def _create_generation_task(db, *, path: Path, fmt: str, original_url: str, owner_type: str,
                                  owner_id: str, created_via_api: bool, input_bytes: Optional[int]):
    """A picture (or a video's frame) is generated into a mesh first, then bound to V3.

    Generation spends farm GPU, so through this route it is open to
    administrator accounts only; everyone else uses /api/generate/from-image,
    which charges credits.  The row stays ``pipeline_kind="generate"`` until the
    mesh exists (generation_tasks), then becomes the V3 task of that mesh."""
    from config import is_admin_email
    from generation_tasks import set_generation_meta
    from tasks import create_conversion_task

    if not (owner_type == "user" and is_admin_email(owner_id)):
        raise V3IntakeError("generation_requires_account",
                            "image and video generation: use /api/generate/from-image (credits)")
    image_url = original_url
    if fmt == "video":
        frame = path.with_name(path.stem + "_frame.png")
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-v", "error", "-ss", "1", "-i", str(path), "-frames:v", "1", str(frame),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, err = await proc.communicate()
        if proc.returncode or not frame.is_file():
            raise V3IntakeError("video_frame", (err or b"").decode(errors="replace")[-300:] or "no frame")
        base = original_url.rsplit("/", 1)[0]
        image_url = f"{base}/{frame.name}"
    task, error = await create_conversion_task(db, input_url=image_url, task_type="t_pose", owner_type=owner_type,
                                               owner_id=owner_id, created_via_api=created_via_api,
                                               pipeline_kind="generate", input_bytes=input_bytes)
    if task is None:
        raise V3IntakeError("task_create_failed", error or "the task could not be created")
    set_generation_meta(task, stage="detect", charged=0, v3=True, source_kind=fmt,
                        source_url=original_url if fmt == "video" else None)
    await db.commit()
    print(f"[V3 intake] {task.id} {fmt} -> generation first, then the V3 conveyor")
    _notify_new(task.id)
    return task


async def _create_normalizing_task(db, *, original, owner_type, owner_id, origin, created_via_api,
                                   requested_intent, input_type, input_bytes):
    from database import Task
    from main import ensure_disk_headroom_for_new_task

    await ensure_disk_headroom_for_new_task(db)
    task = Task(id=str(uuid.uuid4()), owner_type=owner_type, owner_id=owner_id, input_url=original["url"],
                input_type=input_type, status="created", created_via_api=created_via_api, pipeline_kind="v3",
                input_bytes=input_bytes, queue_class="interactive", preemption_state="none",
                viewer_settings=json.dumps(_v3_settings("normalizing", origin=origin, format=original["format"],
                                                        filename=original["filename"],
                                                        requested_intent=requested_intent,
                                                        original_url=original["url"]), ensure_ascii=False))
    db.add(task)
    await db.commit()
    await db.refresh(task)
    print(f"[V3 intake] {task.id} waiting for {original['format'].upper()} -> GLB normalization ({origin})")
    _notify_new(task.id)
    return task


def _notify_new(task_id: str) -> None:
    """The owner's «New task started» for every V3 creation path (once per task, v3_notify)."""
    try:
        from v3_notify import schedule_new

        schedule_new(task_id)
    except Exception as exc:                             # noqa: BLE001 - a notification never blocks intake
        print(f"[V3 intake] new-task notification for {task_id} not scheduled: {exc}")


def _settings(task) -> Dict[str, Any]:
    try:
        value = json.loads(task.viewer_settings or "{}")
    except (TypeError, ValueError):
        value = {}
    return value if isinstance(value, dict) else {}


async def bind_existing_task(db, task, *, data: bytes, origin: str, requested_intent: str,
                             filename: str = "", original: Optional[Mapping[str, Any]] = None,
                             normalization: Optional[Mapping[str, Any]] = None,
                             receipt_facts: Optional[Mapping[str, Any]] = None) -> None:
    """Turn an existing row (generation or normalization intake) into a bound V3 task, one commit."""
    from v3_task_runtime import SameTaskDbBindings

    facts = glb_facts(data)
    path, digest = store_source(data)
    receipt = generation_receipt(task.id, digest, **dict(receipt_facts or {})) \
        if requested_intent == "generate" else None
    binding = build_binding(task.id, path, digest, requested_intent=requested_intent, origin=origin,
                            filename=filename, original=original, normalization=normalization,
                            facts=facts, receipt=receipt)
    settings = _settings(task)
    v3 = dict(settings.get("v3") or {})
    intake = dict(v3.get("intake") or {})
    intake.update(source_sha256=digest, bound_at=datetime.utcnow().isoformat() + "Z", origin=origin,
                  requested_intent=requested_intent)
    v3.update(state="pending", stage="dispatch", intake=intake, error=None)
    settings["v3"] = v3
    task.pipeline_kind = "v3"
    task.status = "created"
    task.error_message = None
    task.worker_api = None
    task.worker_task_id = None
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    task.updated_at = datetime.utcnow()
    try:
        await SameTaskDbBindings().bind_in_task_transaction(db, binding)
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    print(f"[V3 intake] {task.id} bound to {origin} source sha={digest[:12]}")
    _notify_new(task.id)          # a generation row (/api/generate) is announced once, when it becomes V3


async def fetch_bytes(url: str, *, timeout: float = 300.0) -> bytes:
    """Download a generated/normalized GLB (local services first, never a stranger's host)."""
    from worker_transport import worker_http_client, worker_transport_url

    async with worker_http_client(follow_redirects=True) as client:
        response = await client.get(worker_transport_url(url), timeout=timeout)
        response.raise_for_status()
        data = response.content
    if len(data) > MAX_SOURCE_BYTES:
        raise V3IntakeError("source_size", "the generated model is larger than 512 MB")
    return data


async def admit_generated_glb(db, *, glb_url: str, owner_type: str, owner_id: str, origin: str,
                              receipt_facts: Mapping[str, Any], input_url: Optional[str] = None,
                              collection_metadata: Optional[Dict[str, Any]] = None,
                              queue_class: str = "interactive", created_via_api: bool = True):
    """A finished generation (Telegram/Renderfin) re-enters the conveyor as a new V3 task."""
    data = await fetch_bytes(glb_url)
    return await admit_glb(db, data=data, original_url=input_url or glb_url,
                           filename=os.path.basename(glb_url.split("?", 1)[0])[:200] or "generated.glb",
                           owner_type=owner_type, owner_id=owner_id, origin=origin,
                           created_via_api=created_via_api, requested_intent="generate",
                           receipt_facts={**dict(receipt_facts), "glb_url": glb_url},
                           collection_metadata=collection_metadata, queue_class=queue_class)


async def bind_generated_task(db, task, *, glb_url: str, receipt_facts: Mapping[str, Any]) -> None:
    """The website generation row becomes the V3 task of the mesh it produced."""
    data = await fetch_bytes(glb_url)
    await bind_existing_task(db, task, data=data, origin="generation", requested_intent="generate",
                             filename=os.path.basename(glb_url.split("?", 1)[0])[:200],
                             original={"url": task.input_url, "kind": "image"},
                             receipt_facts={**dict(receipt_facts), "glb_url": glb_url,
                                            "image_url": task.input_url})


# ---------------------------------------------------------------- normalization pump
NORMALIZE_RETRY_SECONDS = (60, 120, 300, 600)
NORMALIZE_DEADLINE_SECONDS = float(os.getenv("AUTORIG_V3_NORMALIZE_DEADLINE", "21600"))   # 6 hours


def obj_to_glb(data: bytes) -> bytes:
    """A self-contained Wavefront OBJ -> GLB 2.0 (positions, UVs, normals, one primitive per usemtl).

    Material libraries and textures are not part of a single-file upload, so
    every material is a plain named PBR material; geometry is exact (fan
    triangulation of polygons, negative indices resolved)."""
    import numpy as np

    positions, uvs, normals = [], [], []
    groups: Dict[str, list] = {}
    order: list = []
    current = "default"
    for raw in data.decode("utf-8", "replace").splitlines():
        parts = raw.split()
        if not parts:
            continue
        head = parts[0]
        if head == "v" and len(parts) >= 4:
            positions.append((float(parts[1]), float(parts[2]), float(parts[3])))
        elif head == "vt" and len(parts) >= 3:
            uvs.append((float(parts[1]), 1.0 - float(parts[2])))
        elif head == "vn" and len(parts) >= 4:
            normals.append((float(parts[1]), float(parts[2]), float(parts[3])))
        elif head == "usemtl":
            current = " ".join(parts[1:])[:120] or "default"
        elif head == "f" and len(parts) >= 4:
            corners = []
            for token in parts[1:]:
                fields = (token.split("/") + ["", ""])[:3]
                idx = []
                for value, count in zip(fields, (len(positions), len(uvs), len(normals))):
                    if not value:
                        idx.append(-1)
                        continue
                    k = int(value)
                    idx.append(k - 1 if k > 0 else count + k)
                corners.append(tuple(idx))
            if current not in groups:
                groups[current] = []
                order.append(current)
            for k in range(1, len(corners) - 1):                       # fan triangulation
                groups[current].append((corners[0], corners[k], corners[k + 1]))
    if not positions or not any(groups.values()):
        raise V3IntakeError("invalid_obj", "the OBJ has no faces")
    if len(positions) > 5_000_000:
        raise V3IntakeError("source_size", "the OBJ has more than 5M vertices")
    P = np.asarray(positions, np.float32)
    T = np.asarray(uvs, np.float32) if uvs else None
    N = np.asarray(normals, np.float32) if normals else None
    with_uv = T is not None and all(c[1] >= 0 for tri in (t for g in groups.values() for t in g) for c in tri)
    with_n = N is not None and all(c[2] >= 0 for tri in (t for g in groups.values() for t in g) for c in tri)
    keys: Dict[tuple, int] = {}
    vp, vt, vn, prims = [], [], [], []
    for name in order:
        indices = []
        for tri in groups[name]:
            for corner in tri:
                key = (corner[0], corner[1] if with_uv else -1, corner[2] if with_n else -1)
                slot = keys.get(key)
                if slot is None:
                    if not 0 <= corner[0] < len(P):
                        raise V3IntakeError("invalid_obj", "a face references a missing vertex")
                    slot = keys[key] = len(vp)
                    vp.append(corner[0])
                    vt.append(key[1])
                    vn.append(key[2])
                indices.append(slot)
        prims.append((name, np.asarray(indices, np.uint32)))
    pos = P[np.asarray(vp)]
    blob, views, accessors = bytearray(), [], []

    def add(array, kind, target=None, minmax=False):
        array = np.ascontiguousarray(array)
        blob.extend(bytes(-len(blob) % 4))
        view = {"buffer": 0, "byteOffset": len(blob), "byteLength": int(array.nbytes)}
        if target:
            view["target"] = target
        blob.extend(array.tobytes())
        views.append(view)
        comp = 5125 if array.dtype == np.uint32 else 5126
        acc = {"bufferView": len(views) - 1, "componentType": comp, "count": int(array.shape[0]), "type": kind}
        if minmax:
            acc.update(min=[float(x) for x in array.min(0)], max=[float(x) for x in array.max(0)])
        accessors.append(acc)
        return len(accessors) - 1

    attributes = {"POSITION": add(pos, "VEC3", 34962, True)}
    if with_uv:
        attributes["TEXCOORD_0"] = add(T[np.asarray(vt)], "VEC2", 34962)
    if with_n:
        normal = N[np.asarray(vn)]
        length = np.linalg.norm(normal, axis=1, keepdims=True)
        attributes["NORMAL"] = add(np.where(length > 0, normal / np.maximum(length, 1e-12), [0, 1, 0]).astype(np.float32),
                                   "VEC3", 34962)
    materials, primitives = [], []
    for name, idx in prims:
        materials.append({"name": name, "pbrMetallicRoughness": {"baseColorFactor": [.8, .8, .8, 1],
                                                                 "metallicFactor": 0, "roughnessFactor": .8}})
        primitives.append({"attributes": attributes, "indices": add(idx, "SCALAR", 34963),
                           "material": len(materials) - 1, "mode": 4})
    blob.extend(bytes(-len(blob) % 4))
    doc = {"asset": {"version": "2.0", "generator": "AutoRig V3 intake obj_to_glb"},
           "scene": 0, "scenes": [{"nodes": [0]}], "nodes": [{"mesh": 0, "name": "model"}],
           "meshes": [{"name": "model", "primitives": primitives}], "materials": materials,
           "accessors": accessors, "bufferViews": views, "buffers": [{"byteLength": len(blob)}]}
    js = json.dumps(doc, separators=(",", ":")).encode()
    js += b" " * (-len(js) % 4)
    body = struct.pack("<II", len(js), 0x4E4F534A) + js + struct.pack("<II", len(blob), 0x004E4942) + bytes(blob)
    return b"glTF" + struct.pack("<II", 2, 12 + len(body)) + body


async def _normalize_with_converter(db, original_url: str) -> tuple[bytes, Dict[str, Any]]:
    from workers import get_configured_workers, send_fbx_to_glb

    errors = []
    for worker in await get_configured_workers(db):
        result = await send_fbx_to_glb(worker, original_url)
        if not result.success or not result.output_url:
            errors.append(f"{worker.split('//')[-1][:40]}: {str(result.error)[:120]}")
            continue
        data = await fetch_bytes(result.output_url)
        glb_facts(data)
        return data, {"method": "converter-worker:api-converter-glb-to-fbx", "worker": worker,
                      "output_url": result.output_url, "model_name": result.model_name}
    raise V3IntakeError("normalization_failed", "; ".join(errors)[:600] or "no converter worker is configured")


async def pump_v3_intake(session_factory) -> int:
    """Bind V3 rows whose FBX/OBJ source has not been normalized yet (restart-safe, idempotent)."""
    from sqlalchemy import select, text

    from database import Task

    done = 0
    async with session_factory() as db:
        rows = (await db.execute(select(Task).where(Task.pipeline_kind == "v3", Task.status == "created"))).scalars().all()
        try:
            bound = {str(r[0]) for r in (await db.execute(text(
                "SELECT DISTINCT task_id FROM v3_dispatch_task_bindings"))).all()}
        except Exception:
            bound = set()
        candidates = [t.id for t in rows if t.id not in bound and
                      (_settings(t).get("v3") or {}).get("state") in INTAKE_STATES_OPEN]
    for task_id in candidates[:4]:
        async with session_factory() as db:
            task = await db.get(Task, task_id)
            if task is None or task.pipeline_kind != "v3" or task.status != "created":
                continue
            v3_now = (_settings(task).get("v3") or {})
            intake = v3_now.get("intake") or {}
            if float(v3_now.get("retry_at") or 0) > datetime.utcnow().timestamp():
                continue
            try:
                source_url = intake.get("original_url") or task.input_url
                local = local_upload_path(source_url)
                if intake.get("format") == "obj" and local is not None:
                    raw = local.read_bytes()
                    data = await asyncio.to_thread(obj_to_glb, raw)
                    normalization = {"method": "v3-intake:obj_to_glb", "obj_sha256": hashlib.sha256(raw).hexdigest()}
                else:
                    data, normalization = await _normalize_with_converter(db, source_url)
                await bind_existing_task(db, task, data=data, origin=str(intake.get("origin") or "website"),
                                         requested_intent=str(intake.get("requested_intent") or "rig"),
                                         filename=str(intake.get("filename") or ""),
                                         original={"url": task.input_url, "format": intake.get("format")},
                                         normalization=normalization)
                done += 1
            except Exception as exc:
                await db.rollback()
                task = await db.get(Task, task_id)
                settings = _settings(task)
                v3 = dict(settings.get("v3") or {})
                attempts = int(v3.get("normalize_attempts") or 0) + 1
                started = float(v3.get("normalize_started_at") or 0) or datetime.utcnow().timestamp()
                waited = datetime.utcnow().timestamp() - started
                # An unreachable converter is a wait, not a verdict; a broken
                # source or a hard deadline ends the task explicitly.
                final = isinstance(exc, V3IntakeError) and exc.code in ("invalid_glb", "invalid_obj", "source_size") \
                    or waited >= NORMALIZE_DEADLINE_SECONDS
                delay = NORMALIZE_RETRY_SECONDS[min(attempts - 1, len(NORMALIZE_RETRY_SECONDS) - 1)]
                v3.update(normalize_attempts=attempts, normalize_started_at=started, error=str(exc)[:600],
                          retry_at=None if final else datetime.utcnow().timestamp() + delay,
                          stage="normalization", state="failed" if final else "normalizing")
                settings["v3"] = v3
                task.viewer_settings = json.dumps(settings, ensure_ascii=False)
                task.updated_at = datetime.utcnow()
                if final:
                    task.status = "error"
                    task.error_message = f"V3 could not normalize the source: {str(exc)[:300]}"
                await db.commit()
                print(f"[V3 intake] {task_id} normalization {'failed' if final else 'waiting'} "
                      f"(attempt {attempts}): {str(exc)[:200]}")
    return done


async def intake_loop(session_factory, stop: asyncio.Event, interval: float = 5.0) -> None:
    while not stop.is_set():
        try:
            await pump_v3_intake(session_factory)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[V3 intake] pump failed: {type(exc).__name__}: {exc}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

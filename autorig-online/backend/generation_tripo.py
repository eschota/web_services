"""Tripo for the owner's own picture tasks (owner 2026-10-19: «если задачу делаю я — отправляй сразу в Tripo на
качественную генерацию»).

The full tier of a picture task normally runs in renderfin (Hunyuan 2.1 on the farm). On 2026-10-19 the owner's two Santa
tasks (9e7c06e8, 3efed899) waited 30+ min at «hunyuan» because every Hunyuan box failed the 7 GB VRAM gate (the LLM holds
the 1080 Ti converters, f12 was unreachable) while the fast preview (Hunyuan3D-2mini) had planes between the limbs. So a
task whose owner is an administrator (``config.is_admin_email``) sends its full generation to Tripo (apiv3,
``POST /v3/generation/image-to-model``: textured, PBR, detailed texture) instead of renderfin; the fast preview still
runs beside it. The job id is ``tripo:<task id>`` and ``render_prompting.poll_character_gen`` answers it in renderfin's
job shape (``stage``, ``glb_url``, ``error``…), so the generation pump, the fast-tier upgrade and the V3 binding need no
other change. The finished GLB is downloaded once into the picture's upload folder (``/u/<token>/tripo.glb``, public) and
``glb_url`` points there.

Switches (environment of autorig-storage):
    AUTORIG_PICTURE_TRIPO     admin (default) | all | off     whose picture tasks go to Tripo
    AUTORIG_TRIPO_ENV         /srv/autorig/secrets/tripo.env  the file with TRIPO_API_KEY=… (never in Git or logs)
    AUTORIG_TRIPO_URL         https://openapi.tripo3d.ai      the apiv3 host
    AUTORIG_TRIPO_MODEL       v2.5-20250123                   ``model`` of the request (Tripo's model version)
    AUTORIG_TRIPO_TEXTURE     detailed                        ``texture_quality``: standard | detailed
    AUTORIG_TRIPO_VAR         /srv/autorig/data/var/tripo     job records and the ledger (credits per task)
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import time
from typing import Any, Dict, Optional

import httpx

JOB_PREFIX = "tripo:"
TRIPO_ENV = pathlib.Path(os.getenv("AUTORIG_TRIPO_ENV", "/srv/autorig/secrets/tripo.env"))
VAR = pathlib.Path(os.getenv("AUTORIG_TRIPO_VAR", "/srv/autorig/data/var/tripo"))
MAX_GLB_BYTES = 512 * 1024 * 1024
TERMINAL_BAD = ("failed", "cancelled", "banned", "expired", "unknown")
_TOKEN_URL = re.compile(r"/u/([0-9a-fA-F-]{36})/[^/?#]+\.(?:png|jpe?g|webp)$")


def base_url() -> str:
    return os.getenv("AUTORIG_TRIPO_URL", "https://openapi.tripo3d.ai").rstrip("/")


def policy() -> str:
    return (os.getenv("AUTORIG_PICTURE_TRIPO", "admin") or "admin").strip().lower()


def key() -> str:
    try:
        for line in TRIPO_ENV.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("TRIPO_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        pass
    return ""


def is_job(job_id: Any) -> bool:
    return str(job_id or "").startswith(JOB_PREFIX)


def enabled_for(task: Any) -> bool:
    """True when this task's full generation goes to Tripo: the policy says so for its owner and the key exists."""
    pol = policy()
    if pol in ("off", "0", "false", "no", "none"):
        return False
    if not key():
        return False
    if pol == "all":
        return True
    try:
        from config import is_admin_email
    except Exception:                                           # noqa: BLE001 - outside the backend tree
        return False
    return str(getattr(task, "owner_type", "") or "") == "user" and is_admin_email(getattr(task, "owner_id", None))


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {key()}"}


def _unwrap(resp: httpx.Response) -> Any:
    """Tripo's envelope: {code: 0, data: {...}}; a non-zero code is an error."""
    if resp.status_code >= 400:
        raise RuntimeError(f"tripo HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json()
    if isinstance(body, dict):
        code = body.get("code")
        if (code is not None and code != 0) or body.get("status") == "error":
            raise RuntimeError(f"tripo error {code}: {str(body.get('message') or body.get('suggestion') or '')[:200]}")
        return body.get("data") if "data" in body else body
    return body


def _record_path(tid: str) -> pathlib.Path:
    return VAR / "jobs" / f"{tid}.json"


def _write_json(path: pathlib.Path, doc: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def _read_json(path: pathlib.Path) -> Dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else {}
    except (OSError, ValueError):
        return {}


def _ledger(row: Dict[str, Any]) -> None:
    try:
        VAR.mkdir(parents=True, exist_ok=True)
        with open(VAR / "ledger.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(dict(row, at=round(time.time(), 1)), ensure_ascii=False) + "\n")
    except OSError:
        pass


def token_of(url: str) -> Optional[str]:
    m = _TOKEN_URL.search(str(url or ""))
    return m.group(1) if m else None


def glb_path(record: Dict[str, Any]) -> pathlib.Path:
    """Where the finished GLB lives: the picture's own upload folder (public under /u/), else the var dir."""
    token = record.get("token")
    if token:
        try:
            import picture_isolate

            return picture_isolate.UPLOAD_ROOT / token / "tripo.glb"
        except Exception:                                       # noqa: BLE001
            pass
    return VAR / "models" / f"{record.get('tripo_task')}.glb"


def glb_url(record: Dict[str, Any]) -> str:
    token = record.get("token")
    if token:
        try:
            from config import APP_URL
        except Exception:                                       # noqa: BLE001
            APP_URL = "https://autorig.online"
        return f"{APP_URL}/u/{token}/tripo.glb"
    return str(glb_path(record))


def _client(transport: Optional[httpx.AsyncBaseTransport] = None, timeout: float = 60.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=True)


async def start(image_url: str, task_id: str, *, transport: Optional[httpx.AsyncBaseTransport] = None) -> str:
    """Create the Tripo image-to-model task for this picture; returns the job id ``tripo:<tripo task id>``."""
    if not key():
        raise RuntimeError(f"no Tripo key: TRIPO_API_KEY missing in {TRIPO_ENV}")
    payload = {"input": str(image_url), "model": os.getenv("AUTORIG_TRIPO_MODEL", "v2.5-20250123"),
               "texture": True, "pbr": True,
               "texture_quality": os.getenv("AUTORIG_TRIPO_TEXTURE", "detailed") or "detailed"}
    async with _client(transport) as client:
        data = _unwrap(await client.post(f"{base_url()}/v3/generation/image-to-model", json=payload, headers=_headers()))
    tid = str((data or {}).get("task_id") or "") if isinstance(data, dict) else ""
    if not tid:
        raise RuntimeError(f"tripo: no task_id in {str(data)[:200]}")
    record = {"schema": "autorig.generation-tripo/1", "tripo_task": tid, "task_id": str(task_id), "image_url": str(image_url),
              "token": token_of(image_url), "model": payload["model"], "texture_quality": payload["texture_quality"],
              "created_at": round(time.time(), 1), "status": "queued"}
    _write_json(_record_path(tid), record)
    _ledger({"event": "start", "task_id": str(task_id), "tripo_task": tid, "model": payload["model"]})
    return JOB_PREFIX + tid


async def _download(client: httpx.AsyncClient, url: str, dest: pathlib.Path) -> int:
    """The signed result URL, downloaded once (atomic; a stale half file is never served)."""
    if dest.is_file() and dest.stat().st_size > 0:
        return dest.stat().st_size
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    n = 0
    async with client.stream("GET", url, timeout=600.0) as resp:
        resp.raise_for_status()
        with open(tmp, "wb") as fh:
            async for chunk in resp.aiter_bytes():
                n += len(chunk)
                if n > MAX_GLB_BYTES:
                    raise RuntimeError("tripo model larger than 512 MB")
                fh.write(chunk)
    if n == 0:
        raise RuntimeError("tripo model download was empty")
    os.replace(tmp, dest)
    return n


async def poll(job_id: str, *, transport: Optional[httpx.AsyncBaseTransport] = None) -> Dict[str, Any]:
    """The job in renderfin's character-gen shape: stage hunyuan (running, worker «tripo»), ready (+ glb_url) or failed."""
    tid = str(job_id)[len(JOB_PREFIX):] if is_job(job_id) else str(job_id)
    record = _read_json(_record_path(tid))
    base = {"job_id": str(job_id), "provider": "tripo", "hunyuan_worker": "tripo", "hunyuan_task_id": tid,
            "stage_started_at": record.get("created_at") or 0, "image_url": record.get("image_url")}
    if record.get("status") == "success" and glb_path(record).is_file():
        return dict(base, stage="ready", glb_url=glb_url(record), progress=100,
                    credits_consumed=record.get("credits_consumed"))
    async with _client(transport) as client:
        data = _unwrap(await client.get(f"{base_url()}/v3/tasks/{tid}", headers=_headers()))
        data = data if isinstance(data, dict) else {}
        status = str(data.get("status") or "").lower()
        progress = data.get("progress")
        if status == "success":
            out = data.get("output") or {}
            url = out.get("model_url") or out.get("pbr_model_url") or out.get("pbr_model") or out.get("model") or ""
            if not url:
                return dict(base, stage="failed", error="tripo: success without a model url")
            size = await _download(client, url, glb_path(record))
            record.update(status="success", finished_at=round(time.time(), 1), model_bytes=size,
                          credits_consumed=data.get("credits_consumed"), result_url=url)
            _write_json(_record_path(tid), record)
            _ledger({"event": "done", "task_id": record.get("task_id"), "tripo_task": tid, "bytes": size,
                     "seconds": round(time.time() - float(record.get("created_at") or time.time()), 1),
                     "credits_consumed": data.get("credits_consumed")})
            return dict(base, stage="ready", glb_url=glb_url(record), progress=100,
                        credits_consumed=data.get("credits_consumed"))
        if status in TERMINAL_BAD:
            err = f"tripo {status}: {str(data.get('error_msg') or data.get('error_message') or data.get('error_code') or '')[:200]}"
            if record.get("status") != status:
                record.update(status=status, error=err, finished_at=round(time.time(), 1))
                _write_json(_record_path(tid), record)
                _ledger({"event": status, "task_id": record.get("task_id"), "tripo_task": tid, "error": err})
            return dict(base, stage="failed", error=err)
    return dict(base, stage="hunyuan", progress=int(progress) if isinstance(progress, (int, float)) else 0,
                queuing_num=data.get("queuing_num"), running_left_time=data.get("running_left_time"))

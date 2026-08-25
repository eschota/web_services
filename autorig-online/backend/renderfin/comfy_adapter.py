"""Lightweight ComfyUI HTTP adapter (port of C# Adapter_Comfy.cs).

Deliberately NOT reusing animation_fitting/comfy.py: that client rejects plain
HTTP to non-loopback hosts and is welded to fingerprint-pinned workflows, while
renderfin talks plain HTTP to the farm workers (5.129.157.224) and submits
per-request templated workflows.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from . import config
from .models import RenderServer

CLIENT_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"  # C# parity (Adapter_Comfy.cs:332)

VIDEO_EXTENSIONS = (".mp4", ".webm", ".mov", ".avi", ".mkv")
MODEL_EXTENSIONS = (".glb", ".gltf", ".obj", ".fbx")


class ComfyAdapterError(RuntimeError):
    pass


class ComfyCapacityWait(RuntimeError):
    """The shared GPU is temporarily leased to Hunyuan.

    This is normal capacity pressure, not a render attempt and not evidence
    that the workflow or worker is broken.
    """


def _capacity_wait(resp: httpx.Response) -> bool:
    if resp.status_code not in (409, 423, 429, 503):
        return False
    try:
        payload = resp.json()
    except ValueError:
        payload = {}
    return bool(
        payload.get("retryable") is True
        or str(payload.get("error") or "").lower() in {
            "gpu_leased",
            "gpu_busy_comfy",
            "comfy_backend_unavailable",
        }
    )


def _validate_server_url(url: str) -> str:
    url = (url or "").rstrip("/")
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return url
    if parsed.scheme == "http" and (parsed.hostname or "") in config.ALLOWED_HTTP_HOSTS:
        return url
    raise ComfyAdapterError(f"server url not allowed: {url!r}")


def _auth_for(server: RenderServer) -> Optional[httpx.BasicAuth]:
    if server.basic_auth and config.WORKER_BASIC_AUTH and ":" in config.WORKER_BASIC_AUTH:
        user, _, password = config.WORKER_BASIC_AUTH.partition(":")
        return httpx.BasicAuth(user, password)
    return None


async def download_input_image(client: httpx.AsyncClient, image_url: str) -> Tuple[str, bytes]:
    """Fetch the caller's image_url. Local mask URLs short-circuit to disk."""
    image_url = (image_url or "").strip()
    if not image_url:
        raise ComfyAdapterError("empty image_url")
    marker = "/render/masks/"
    if marker in image_url:
        name = image_url.rsplit("/", 1)[-1]
        for base in (config.RENDER_DIR / "masks", config.MASKS_DIR):
            path = base / name
            if path.is_file() and path.resolve().parent == base.resolve():
                return name, path.read_bytes()
    # our own artifacts: serve from disk instead of looping through nginx
    own_prefix = f"{config.PUBLIC_BASE_URL}/render/"
    if image_url.startswith(own_prefix):
        rel = image_url[len(own_prefix):]
        path = (config.RENDER_DIR / rel).resolve()
        if path.is_file() and str(path).startswith(str(config.RENDER_DIR.resolve())):
            return path.name, path.read_bytes()
    resp = await client.get(image_url, timeout=60.0, follow_redirects=True)
    if resp.status_code != 200:
        raise ComfyAdapterError(f"failed to download image {image_url}: HTTP {resp.status_code}")
    name = urlparse(image_url).path.rsplit("/", 1)[-1] or "input.png"
    return name, resp.content


async def upload_image(
    client: httpx.AsyncClient, server: RenderServer, filename: str, data: bytes
) -> str:
    """POST /upload/image (multipart). Returns the server-side filename."""
    base = _validate_server_url(server.render_server_url)
    unique = f"{uuid.uuid4().hex[:8]}_{filename}"
    files = {"image": (unique, data, "application/octet-stream")}
    resp = await client.post(
        f"{base}/upload/image", files=files, data={"overwrite": "true"},
        timeout=60.0, auth=_auth_for(server),
    )
    if _capacity_wait(resp):
        raise ComfyCapacityWait(f"Comfy GPU temporarily leased: HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise ComfyAdapterError(f"upload/image failed: HTTP {resp.status_code} {resp.text[:200]}")
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    name = str(payload.get("name") or unique)
    subfolder = str(payload.get("subfolder") or "")
    return f"{subfolder}/{name}" if subfolder else name


def managed_submission_payload(
    workflow: Dict[str, Any],
    *,
    managed_identity: Optional[Dict[str, str]] = None,
    prompt_id: str = "",
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Build the exact JSON body and identity headers for `/prompt`."""

    body: Dict[str, Any] = {"prompt": workflow, "client_id": CLIENT_ID}
    if prompt_id:
        body["prompt_id"] = str(prompt_id)
    headers: Dict[str, str] = {}
    if isinstance(managed_identity, dict) and managed_identity.get("logical_task_id"):
        identity = {
            "logical_task_id": str(managed_identity.get("logical_task_id") or ""),
            "lease_id": str(managed_identity.get("lease_id") or ""),
            "request_id": str(managed_identity.get("request_id") or ""),
            "preemption_mode": "central_requeue",
        }
        body["extra_data"] = {"autorig_workload": identity}
        headers = {
            "X-AutoRig-Managed-Task-Id": identity["logical_task_id"],
            "X-AutoRig-Workload-Lease-Id": identity["lease_id"],
            "X-AutoRig-Workload-Request-Id": identity["request_id"],
            "X-AutoRig-Preemption-Mode": "central_requeue",
        }
    return body, headers


def managed_submission_sha256(
    workflow: Dict[str, Any],
    *,
    managed_identity: Dict[str, str],
    prompt_id: str,
) -> str:
    """Bind central registration to the canonical full `/prompt` body."""

    body, _headers = managed_submission_payload(
        workflow,
        managed_identity=managed_identity,
        prompt_id=prompt_id,
    )
    canonical = json.dumps(
        body,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


async def submit(
    client: httpx.AsyncClient,
    server: RenderServer,
    workflow: Dict[str, Any],
    *,
    managed_identity: Optional[Dict[str, str]] = None,
    prompt_id: str = "",
) -> str:
    """POST /prompt. Returns prompt_id."""
    base = _validate_server_url(server.render_server_url)
    body, headers = managed_submission_payload(
        workflow,
        managed_identity=managed_identity,
        prompt_id=prompt_id,
    )
    resp = await client.post(
        f"{base}/prompt",
        json=body,
        headers=headers,
        timeout=60.0,
        auth=_auth_for(server),
    )
    if _capacity_wait(resp):
        raise ComfyCapacityWait(f"Comfy GPU temporarily leased: HTTP {resp.status_code}")
    if resp.status_code != 200:
        raise ComfyAdapterError(f"prompt submit failed: HTTP {resp.status_code} {resp.text[:500]}")
    payload = resp.json()
    returned_prompt_id = str(payload.get("prompt_id") or "")
    if not returned_prompt_id:
        raise ComfyAdapterError(f"prompt submit returned no prompt_id: {json.dumps(payload)[:300]}")
    if prompt_id and returned_prompt_id != str(prompt_id):
        raise ComfyAdapterError(
            f"prompt submit identity mismatch: expected {prompt_id}, got {returned_prompt_id}"
        )
    return returned_prompt_id


def _queue_prompt_ids(payload: Dict[str, Any], bucket: str) -> List[str]:
    result: List[str] = []
    for entry in payload.get(bucket) or []:
        if isinstance(entry, (list, tuple)):
            for item in entry:
                if isinstance(item, str):
                    result.append(item)
                    break
        elif isinstance(entry, dict):
            prompt_id = str(entry.get("prompt_id") or "")
            if prompt_id:
                result.append(prompt_id)
    return result


async def preempt_owned_prompt(
    client: httpx.AsyncClient,
    server: RenderServer,
    prompt_id: str,
    *,
    logical_task_id: str = "",
) -> bool:
    """Remove/interrupt only the exact centrally-owned Comfy prompt.

    The caller resets the same RenderTask to Pending and clears prompt_id after
    this returns.  The host arbiter must not repost prompts carrying
    ``preemption_mode=central_requeue``.
    """
    base = _validate_server_url(server.render_server_url)
    response = await client.get(f"{base}/queue", timeout=15.0, auth=_auth_for(server))
    if response.status_code != 200:
        return False
    payload = response.json()
    running = _queue_prompt_ids(payload, "queue_running")
    pending = _queue_prompt_ids(payload, "queue_pending")
    headers = {"X-AutoRig-Managed-Task-Id": logical_task_id or prompt_id}
    if prompt_id in pending:
        deleted = await client.post(
            f"{base}/queue",
            json={"delete": [prompt_id]},
            headers=headers,
            timeout=15.0,
            auth=_auth_for(server),
        )
        if deleted.status_code not in {200, 204}:
            return False
    elif prompt_id in running:
        # Comfy interrupt is process-wide, therefore exact ownership is proved
        # by requiring this to be the sole running prompt on the node.
        if running != [prompt_id]:
            return False
        interrupted = await client.post(
            f"{base}/interrupt",
            json={},
            headers=headers,
            timeout=15.0,
            auth=_auth_for(server),
        )
        if interrupted.status_code not in {200, 204}:
            return False
    else:
        return True
    for _ in range(20):
        if not await queue_contains(client, server, prompt_id):
            return True
        import asyncio

        await asyncio.sleep(0.25)
    return False


async def queue_contains(
    client: httpx.AsyncClient, server: RenderServer, prompt_id: str
) -> bool:
    """Is this prompt still running or queued on the worker?

    ComfyUI answers /history for an unknown prompt with HTTP 200 and an empty
    object - identical to "queued but not started". Only /queue can tell the
    two apart, and the difference matters: a prompt the worker forgot (restart,
    crash) would otherwise hold the slot until the timeout expires.
    """
    base = _validate_server_url(server.render_server_url)
    resp = await client.get(f"{base}/queue", timeout=15.0, auth=_auth_for(server))
    if resp.status_code != 200:
        return True  # cannot tell: assume it is still there
    try:
        payload = resp.json()
    except Exception:
        return True
    for bucket in ("queue_running", "queue_pending"):
        for entry in payload.get(bucket) or []:
            if isinstance(entry, (list, tuple)):
                if any(item == prompt_id for item in entry if isinstance(item, str)):
                    return True
            elif isinstance(entry, dict) and entry.get("prompt_id") == prompt_id:
                return True
    return False


async def poll_history(
    client: httpx.AsyncClient, server: RenderServer, prompt_id: str
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """GET /history/{prompt_id}. Returns (state, entry) where state is one of
    'pending', 'unknown', 'success', 'error'. 'unknown' means the worker has no
    record of the prompt, which the caller must disambiguate against /queue."""
    base = _validate_server_url(server.render_server_url)
    resp = await client.get(f"{base}/history/{prompt_id}", timeout=30.0, auth=_auth_for(server))
    if resp.status_code != 200:
        return "pending", None
    try:
        payload = resp.json()
    except Exception:
        return "pending", None
    entry = payload.get(prompt_id)
    if not isinstance(entry, dict):
        return "unknown", None
    status = entry.get("status") or {}
    status_str = str(status.get("status_str") or "").lower()
    if status_str == "success":
        return "success", entry
    if status_str == "error" or status.get("completed") is False and status_str:
        return "error", entry
    if status.get("completed") is True:
        return "success", entry
    return "pending", entry


def _walk_filenames(obj: Any, found: List[Dict[str, str]]) -> None:
    if isinstance(obj, dict):
        if "filename" in obj and isinstance(obj.get("filename"), str):
            found.append(
                {
                    "filename": obj.get("filename", ""),
                    "subfolder": str(obj.get("subfolder") or ""),
                    "type": str(obj.get("type") or "output"),
                }
            )
        for v in obj.values():
            _walk_filenames(v, found)
    elif isinstance(obj, list):
        for v in obj:
            _walk_filenames(v, found)


def resolve_artifacts(
    history_entry: Dict[str, Any],
    *,
    output_ext: str,
    preferred_fragment: str = "",
) -> List[Dict[str, str]]:
    """Port of RenderfinComfyArtifactResolver: collect files from the history
    outputs, order by preference (fragment match, then extension match)."""
    found: List[Dict[str, str]] = []
    _walk_filenames(history_entry.get("outputs") or {}, found)
    # keep only real outputs (skip temp previews)
    outputs = [f for f in found if f.get("type") != "temp"] or found

    def rank(f: Dict[str, str]) -> Tuple[int, int]:
        name = f.get("filename", "").lower()
        frag = 0 if preferred_fragment and preferred_fragment.lower() in name else 1
        if output_ext == ".mp4":
            ext_ok = 0 if name.endswith(VIDEO_EXTENSIONS) else 1
        elif output_ext == ".glb":
            ext_ok = 0 if name.endswith(MODEL_EXTENSIONS) else 1
        else:
            ext_ok = 0 if name.endswith((".png", ".jpg", ".jpeg", ".webp")) else 1
        return (frag, ext_ok)

    return sorted(outputs, key=rank)


async def download_artifact(
    client: httpx.AsyncClient, server: RenderServer, artifact: Dict[str, str]
) -> bytes:
    base = _validate_server_url(server.render_server_url)
    params = {
        "filename": artifact.get("filename", ""),
        "subfolder": artifact.get("subfolder", ""),
        "type": artifact.get("type", "output"),
    }
    resp = await client.get(
        f"{base}/view", params=params, timeout=300.0, auth=_auth_for(server)
    )
    if _capacity_wait(resp):
        raise ComfyCapacityWait(
            f"Comfy artifact temporarily gated by GPU lease: HTTP {resp.status_code}"
        )
    if resp.status_code != 200:
        raise ComfyAdapterError(
            f"artifact download failed: HTTP {resp.status_code} {params['filename']}"
        )
    return resp.content


async def interrupt(client: httpx.AsyncClient, server: RenderServer) -> None:
    """Ask ComfyUI to stop the current prompt (best effort)."""
    base = _validate_server_url(server.render_server_url)
    await client.post(f"{base}/interrupt", timeout=15.0, auth=_auth_for(server))


async def queue_depth(client: httpx.AsyncClient, server: RenderServer) -> Optional[int]:
    """How much work the box already has, including other clients' prompts.

    We are not the only caller of these ComfyUI boxes, so our own dispatch
    records say nothing about how long a prompt will actually wait. Returns
    None when the box cannot be asked, which the caller must not read as
    "empty".
    """
    try:
        base = _validate_server_url(server.render_server_url)
        resp = await client.get(f"{base}/queue", timeout=5.0, auth=_auth_for(server))
        if resp.status_code != 200:
            return None
        data = resp.json()
        return sum(
            len(data.get(bucket) or [])
            for bucket in ("queue_running", "queue_pending")
        )
    except Exception:
        return None


async def check_server_online(client: httpx.AsyncClient, server: RenderServer) -> bool:
    try:
        base = _validate_server_url(server.render_server_url)
        resp = await client.get(f"{base}/queue", timeout=5.0, auth=_auth_for(server))
        return resp.status_code == 200
    except Exception:
        return False

"""The farm's LoRA manager: paste a Civitai link, every render box gets the file.

How a LoRA travels
------------------
1. An administrator pastes a Civitai model/version URL (or a Hugging Face file
   URL) at /lora. The VPS resolves it through the Civitai API with the site's
   token: base model, file, size, SHA-256, trigger words, preview.
2. The VPS downloads the file once into its own mirror
   (`<model dir>/loras/blobs/<sha256>`) and checks the hash. The Civitai token
   never leaves the VPS - not even as a presigned URL.
3. Every render box runs `deploy/onlyrender/lora-sync.ps1` on a schedule. It
   asks `GET /api/ai/loras/sync/manifest` what it should hold, fetches what it
   is missing - from a LAN peer first when one is configured (f5's internet is
   slow; f15 serves its models folder on the LAN), then from the VPS mirror -
   checks SHA-256, moves the file into ComfyUI's loras folder and reports its
   whole inventory back. The pull is idempotent, so a box that was offline
   catches up the next time it runs. Adding a LoRA also nudges the boxes the
   VPS can reach over SSH so nobody waits for the schedule.
4. ComfyUI lists a new file in `/object_info` without a restart, and renderfin
   dispatches a LoRA job only to a box whose `/object_info` has every LoRA of
   the stack (renderfin/model_eligibility.py). The box reports feed the
   catalogue, so a LoRA becomes selectable once at least one box holds it.

The registry is its own file rather than rows in model_catalogue.json: the
catalogue is hand-curated and edited by several people, and a tool that
rewrote it would sooner or later overwrite somebody's edit.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import io
import json
import logging
import os
import pathlib
import re
import time
import urllib.parse
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()



def _lora_dir() -> pathlib.Path:
    """Beside the model catalogue, resolved per call so a test that points the
    catalogue at a temporary directory gets an empty registry with it."""
    override = os.getenv("AUTORIG_LORA_DIR", "").strip()
    if override:
        return pathlib.Path(override)
    import ai_model_catalogue
    return pathlib.Path(ai_model_catalogue.CATALOGUE_DIR) / "loras"


def _path(name: str) -> pathlib.Path:
    return _lora_dir() / name


def _preview_dir() -> pathlib.Path:
    import ai_model_catalogue
    return pathlib.Path(ai_model_catalogue.PREVIEW_DIR)


SYNC_KEYS_FILE = pathlib.Path(os.getenv("AUTORIG_LORA_SYNC_KEYS",
                                        "/srv/autorig/secrets/lora-sync-keys.json"))
FARM_SSH_KEY = pathlib.Path(os.getenv("AUTORIG_FARM_SSH_KEY",
                                      "/srv/autorig/secrets/ssh/renderfin_farm_tunnel"))
FARM_GATEWAY = os.getenv("AUTORIG_FARM_GATEWAY", "5.129.157.224")
PUBLIC_BASE = os.getenv("AUTORIG_PUBLIC_BASE", "https://autorig.online").rstrip("/")
WORKFLOWS_DIR = pathlib.Path(__file__).resolve().parent / "renderfin" / "assets" / "workflows"
SYNC_TASK_NAME = "AutoRig LoRA Sync"
USER_AGENT = "autorig-lora-manager/1.0 (+https://autorig.online/lora)"
MAX_LORA_BYTES = 8 * 1024 ** 3
BOX_STALE_SECONDS = 45 * 60
SYNC_INTERVAL_SECONDS = 300

# Render boxes and how the VPS reaches them. Names are renderfin's server
# names. `peers` are LAN URLs (folder listings of another box's loras dir)
# tried before the VPS mirror; the file is always verified by SHA-256, so a
# stale or wrong peer only costs a retry.
# f15 serves its models folder on the LAN (python -m http.server 18998), and
# f5, Raptor and f12 share that LAN (192.168.0.x).
F15_LAN_PEER = "http://192.168.0.115:18998/loras/"
DEFAULT_BOXES: Dict[str, Dict[str, Any]] = {
    "f5": {"ssh_port": 48488, "ssh_user": "user", "peers": [F15_LAN_PEER]},
    "f15": {"ssh_port": 48588, "ssh_user": "user"},
    "Raptor": {"ssh_port": 48288, "ssh_user": "\u0410\u0434\u043c\u0438\u043d\u0438\u0441\u0442\u0440\u0430\u0442\u043e\u0440",
               "peers": [F15_LAN_PEER]},
    "f12": {"peers": [F15_LAN_PEER]},
    "worker-4090": {},
}

IMAGE_FAMILIES = {"pony", "sdxl", "illustrious", "noobai", "flux", "flux2",
                  "flux2_9b", "flux2_dev", "zimage", "krea2", "sd15"}
VIDEO_FAMILIES = {"ltx", "ltx2", "ltx23", "ltx25", "ltx098", "minimax_h3", "wan22_i2v_a14b",
                  "wan22_t2v_a14b", "wan22_5b", "wan21_14b", "wan21_1b"}

_lock = asyncio.Lock()
_background: set = set()


# ------------------------------------------------------------------ storage

def _now() -> int:
    return int(time.time())


def _read_json(path: pathlib.Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except Exception:
        logger.exception("Could not read %s", path)
        return default


def _write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(value, indent=1, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def load_registry() -> Dict[str, Any]:
    data = _read_json(_path("registry.json"), {})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("version", 1)
    data.setdefault("loras", [])
    data.setdefault("cleanup", {})
    return data


def save_registry(data: Dict[str, Any]) -> None:
    _write_json(_path("registry.json"), data)
    _catalogue_cache["at"] = 0.0


def boxes(data: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    merged = {name: dict(cfg) for name, cfg in DEFAULT_BOXES.items()}
    for name, cfg in ((data or load_registry()).get("boxes") or {}).items():
        if isinstance(cfg, dict):
            merged.setdefault(name, {}).update(cfg)
    return merged


def box_report(box: str) -> Dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", box or ""):
        return {}
    report = _read_json(_path("boxes") / f"{box}.json", {})
    return report if isinstance(report, dict) else {}


# ------------------------------------------------------------- families

def family_for_base(base: str) -> str:
    """Civitai `baseModel` (or a typed base) -> our catalogue family."""
    b = re.sub(r"\s+", " ", str(base or "").strip().lower())
    if not b:
        return ""
    if b.startswith("pony"):
        return "pony"
    if "illustrious" in b:
        return "illustrious"
    if "noobai" in b:
        return "noobai"
    if b.startswith("sdxl") or "stable diffusion xl" in b:
        return "sdxl"
    if "krea 2" in b or "krea2" in b:
        return "krea2"
    if "zimage" in b or "z-image" in b or "z image" in b:
        return "zimage"
    if "klein 9b" in b:
        return "flux2_9b"
    if "klein" in b and "flux" in b:
        return "flux2"
    if b.startswith("flux.2") or b.startswith("flux 2"):
        return "flux2_dev"
    if b.startswith("flux.1") or b.startswith("flux 1"):
        return "flux"
    if "ltx" in b:
        if "2.5" in b:
            return "ltx25"
        if "2.3" in b:
            return "ltx23"
        if "ltxv2" in b.replace(" ", "") or "19b" in b:
            return "ltx2"
        if "13b" in b or "0.9.8" in b:
            return "ltx098"
        return "ltx"
    if "minimax" in b or b in ("h3", "hailuo 3"):
        return "minimax_h3"
    if "qwen" in b:
        return "qwen_image"
    if "wan" in b:
        return wan_family(b)
    if b.startswith("sd 1") or b.startswith("sd1"):
        return "sd15"
    return ""


def wan_family(base: str) -> str:
    import ai_model_defaults
    return ai_model_defaults.wan_family(base)


def services_for_family(family: str) -> List[str]:
    if family in IMAGE_FAMILIES:
        return ["image"]
    if family in VIDEO_FAMILIES:
        return ["video"]
    if family == "qwen_image":
        return ["qwen_image"]
    return []


def compatible_checkpoints(family: str, base: str = "") -> List[str]:
    """Usable catalogue checkpoints a LoRA of this family can load onto."""
    import ai_model_catalogue
    import ai_model_defaults

    probe = {"kind": "lora", "family": family, "base": base}
    services = set(services_for_family(family))
    out = []
    for entry in ai_model_catalogue.entries():
        if entry.get("kind") != "checkpoint" or not entry.get("usable"):
            continue
        if services and not services.intersection(entry.get("services") or []):
            continue
        left = ai_model_defaults.model_family(entry)
        if left and ai_model_defaults.compatible(entry, probe):
            out.append(str(entry.get("file")))
    return out


# ------------------------------------------------------------ resolving

class ResolveError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _civitai_headers() -> Dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    token = os.getenv("CIVITAI_API_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _hf_headers() -> Dict[str, str]:
    headers = {"User-Agent": USER_AGENT}
    token = (os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_HUB_TOKEN") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def parse_source_url(url: str) -> Dict[str, Any]:
    """Which model/version/file a pasted link means. No network."""
    raw = str(url or "").strip()
    if re.fullmatch(r"\d{2,12}", raw):
        return {"kind": "civitai", "version_id": int(raw)}
    try:
        parsed = urllib.parse.urlparse(raw if "://" in raw else "https://" + raw)
    except ValueError:
        raise ResolveError("bad_url", "That is not a link") from None
    host = (parsed.hostname or "").lower()
    query = urllib.parse.parse_qs(parsed.query)
    path = parsed.path
    if host.endswith("civitai.com") or host.endswith("civitai.green") or host.endswith("civitai.red"):
        version = query.get("modelVersionId") or query.get("modelversionid")
        m = re.search(r"/api/download/models/(\d+)", path)
        if m:
            return {"kind": "civitai", "version_id": int(m.group(1))}
        m = re.search(r"/model-versions/(\d+)", path)
        if m:
            return {"kind": "civitai", "version_id": int(m.group(1))}
        m = re.search(r"/models/(\d+)", path)
        if m:
            out: Dict[str, Any] = {"kind": "civitai", "model_id": int(m.group(1))}
            if version and str(version[0]).isdigit():
                out["version_id"] = int(version[0])
            return out
        m = re.search(r"/images/(\d+)", path)
        if m:
            raise ResolveError("image_url", "That is an image link; use it in Reproduce, "
                               "or paste the LoRA's model page")
        raise ResolveError("bad_url", "A Civitai link must point at a model or a model version")
    if host in ("huggingface.co", "www.huggingface.co", "hf.co"):
        m = re.fullmatch(r"/([^/]+)/([^/]+)/(?:blob|resolve)/([^/]+)/(.+)", path)
        if not m:
            raise ResolveError("bad_url", "A Hugging Face link must point at one file "
                               "(…/blob/<revision>/<file>.safetensors)")
        return {"kind": "hf", "repo": f"{m.group(1)}/{m.group(2)}",
                "revision": m.group(3), "path": urllib.parse.unquote(m.group(4))}
    raise ResolveError("bad_url", "Paste a civitai.com or huggingface.co link")


_SAFE_FILE = re.compile(r"[^A-Za-z0-9._()+ -]+")


def safe_file_name(name: str) -> str:
    base = str(name or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    stem, dot, ext = base.rpartition(".")
    if not dot:
        stem, ext = base, "safetensors"
    stem = _SAFE_FILE.sub("_", stem).strip(" .") or "lora"
    return f"{stem[:150]}.{ext.lower()}"


def _recommended_strength(description: str) -> Optional[float]:
    text = html.unescape(re.sub(r"<[^>]+>", " ", str(description or ""))).lower()
    m = re.search(r"(?:weight|strength)s?\s*(?:of|:|=|at|around|between|is)?\s*"
                  r"([0-2](?:[.,]\d{1,2})?)", text)
    if not m:
        return None
    try:
        value = float(m.group(1).replace(",", "."))
    except ValueError:
        return None
    return value if 0.05 <= value <= 2.0 else None


async def _civitai_json(client: httpx.AsyncClient, path: str) -> Any:
    response = await client.get("https://civitai.com/api/v1" + path,
                                headers=_civitai_headers(), timeout=40.0)
    if response.status_code == 404:
        raise ResolveError("not_found", "Civitai has no such model or version", 404)
    if response.status_code in (401, 403):
        raise ResolveError("civitai_denied", "Civitai refused the site token for this model", 502)
    if response.status_code >= 400:
        raise ResolveError("civitai_error", f"Civitai answered HTTP {response.status_code}", 502)
    return response.json()


def _pick_file(files: List[Dict[str, Any]]) -> Dict[str, Any]:
    models = [f for f in files if str(f.get("type") or "Model") == "Model"]
    safe = [f for f in models
            if str((f.get("metadata") or {}).get("format") or "").lower() == "safetensor"
            or str(f.get("name") or "").lower().endswith(".safetensors")]
    if not safe:
        raise ResolveError("no_safetensors", "This version has no .safetensors file; "
                           "pickled checkpoints are not accepted")
    primary = [f for f in safe if f.get("primary")]
    return (primary or safe)[0]


async def resolve_civitai(client: httpx.AsyncClient, source: Dict[str, Any]) -> Dict[str, Any]:
    version_id = source.get("version_id")
    model: Dict[str, Any] = {}
    if not version_id:
        model = await _civitai_json(client, f"/models/{source['model_id']}")
        versions = model.get("modelVersions") or []
        if not versions:
            raise ResolveError("not_found", "That model has no published version")
        version_id = versions[0]["id"]
    version = await _civitai_json(client, f"/model-versions/{version_id}")
    model_info = version.get("model") or {}
    kind = str(model_info.get("type") or model.get("type") or "").upper()
    if kind and kind not in ("LORA", "LOCON", "LYCORIS", "DORA"):
        raise ResolveError("not_a_lora", f"That is a {kind.title()}, not a LoRA")
    chosen = _pick_file(version.get("files") or [])
    sha = str((chosen.get("hashes") or {}).get("SHA256") or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ResolveError("no_hash", "Civitai publishes no SHA-256 for this file; "
                           "it cannot be verified on the boxes")
    base = str(version.get("baseModel") or "")
    family = family_for_base(base)
    previews = [img for img in version.get("images") or []
                if str(img.get("type") or "image") == "image"
                and int(img.get("nsfwLevel") or 0) <= 1 and img.get("url")]
    preview_url = ""
    if previews:
        preview_url = re.sub(r"/original=true/", "/width=450/", str(previews[0]["url"]))
    trained = [str(word).strip() for word in version.get("trainedWords") or [] if str(word).strip()]
    model_name = str(model_info.get("name") or model.get("name") or "")
    return {
        "id": f"civitai-{version['id']}",
        "file": safe_file_name(chosen.get("name")),
        "source_file_name": str(chosen.get("name") or ""),
        "sha256": sha,
        "size_bytes": int(float(chosen.get("sizeKB") or 0) * 1024),
        "family": family,
        "base": base,
        "services": services_for_family(family),
        "title": model_name,
        "version": str(version.get("name") or ""),
        "page": f"https://civitai.com/models/{version.get('modelId')}?modelVersionId={version['id']}",
        "nsfw": bool(model_info.get("nsfw")) or int(version.get("nsfwLevel") or 0) > 2,
        "trained_words": trained,
        "recommended_strength": _recommended_strength(version.get("description") or ""),
        "preview_source": preview_url,
        "source": {"kind": "civitai", "model_id": version.get("modelId"),
                   "version_id": version["id"], "file_id": chosen.get("id"),
                   "download_url": str(chosen.get("downloadUrl") or version.get("downloadUrl") or "")},
        "aliases": sorted({alias for alias in (model_name, str(version.get("name") or ""),
                                               str(chosen.get("name") or "").rsplit(".", 1)[0])
                           if alias}),
    }


async def resolve_hf(client: httpx.AsyncClient, source: Dict[str, Any],
                     base_hint: str) -> Dict[str, Any]:
    if not source["path"].lower().endswith(".safetensors"):
        raise ResolveError("no_safetensors", "Only .safetensors files are accepted")
    response = await client.post(
        f"https://huggingface.co/api/models/{source['repo']}/paths-info/{source['revision']}",
        json={"paths": [source["path"]], "expand": True}, headers=_hf_headers(), timeout=40.0)
    if response.status_code >= 400:
        raise ResolveError("hf_error", f"Hugging Face answered HTTP {response.status_code}", 502)
    items = response.json() or []
    info = items[0] if items else {}
    lfs = info.get("lfs") or {}
    sha = str(lfs.get("oid") or lfs.get("sha256") or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ResolveError("no_hash", "That file is not stored with a SHA-256 on Hugging Face")
    family = family_for_base(base_hint)
    if not family:
        raise ResolveError("base_required", "Say which base model this LoRA is for "
                           "(e.g. SDXL 1.0, Pony, Flux.2 Klein 4B, ZImageTurbo, LTXV 2.5)")
    file_name = safe_file_name(source["path"])
    return {
        "id": f"hf-{sha[:12]}",
        "file": file_name,
        "source_file_name": source["path"].rsplit("/", 1)[-1],
        "sha256": sha,
        "size_bytes": int(lfs.get("size") or info.get("size") or 0),
        "family": family, "base": base_hint, "services": services_for_family(family),
        "title": source["repo"].split("/", 1)[-1], "version": source["revision"][:12],
        "page": f"https://huggingface.co/{source['repo']}/blob/{source['revision']}/{source['path']}",
        "nsfw": False, "trained_words": [], "recommended_strength": None, "preview_source": "",
        "source": {"kind": "hf", "repo": source["repo"], "revision": source["revision"],
                   "path": source["path"],
                   "download_url": f"https://huggingface.co/{source['repo']}/resolve/"
                                   f"{source['revision']}/{urllib.parse.quote(source['path'])}"},
        "aliases": [file_name.rsplit(".", 1)[0]],
    }


async def resolve_url(url: str, base_hint: str = "") -> Dict[str, Any]:
    source = parse_source_url(url)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        if source["kind"] == "civitai":
            record = await resolve_civitai(client, source)
        else:
            record = await resolve_hf(client, source, base_hint)
    if base_hint and source["kind"] == "civitai" and not record["family"]:
        record["family"] = family_for_base(base_hint)
        record["services"] = services_for_family(record["family"])
    record["compatible_checkpoints"] = compatible_checkpoints(record["family"], record["base"])
    if record["size_bytes"] > MAX_LORA_BYTES:
        raise ResolveError("too_large", "That file is larger than a LoRA should be")
    return record


def _unique_file_name(data: Dict[str, Any], record: Dict[str, Any]) -> str:
    """Keep one file name per content across the registry and the templates."""
    taken = {str(item.get("file")): str(item.get("sha256"))
             for item in data.get("loras", []) if item.get("state") != "removed"}
    name = record["file"]
    if taken.get(name) in (None, record["sha256"]) and name not in protected_files():
        return name
    stem, _, ext = name.rpartition(".")
    return f"{stem}_{record['sha256'][:8]}.{ext}"


# --------------------------------------------------------- mirror download

async def _download_to_mirror(entry_id: str) -> None:
    data = load_registry()
    entry = next((item for item in data["loras"] if item.get("id") == entry_id), None)
    if not entry:
        return
    sha = entry["sha256"]
    target = _path("blobs") / sha
    if target.is_file() and target.stat().st_size > 0:
        await _set_mirror(entry_id, "ready", target.stat().st_size, "")
        return
    _path("blobs").mkdir(parents=True, exist_ok=True)
    part = _path("blobs") / f"{sha}.part"
    source = entry.get("source") or {}
    url = str(source.get("download_url") or "")
    if source.get("kind") == "civitai":
        headers = _civitai_headers()
        if not url:
            url = f"https://civitai.com/api/download/models/{source.get('version_id')}"
    else:
        headers = _hf_headers()
    headers.pop("Accept", None)
    await _set_mirror(entry_id, "downloading", 0, "")
    digest = hashlib.sha256()
    written = 0
    last_note = time.monotonic()
    try:
        # httpx drops the Authorization header on the cross-origin redirect to
        # the storage host, so the token only ever goes to civitai.com itself.
        async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(60.0, read=120.0)) as client:
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code in (401, 403):
                    raise ResolveError("download_denied",
                                       "Civitai refused the download (early access or login-only file)")
                if response.status_code >= 400:
                    raise ResolveError("download_failed", f"download answered HTTP {response.status_code}")
                with open(part, "wb") as handle:
                    async for chunk in response.aiter_bytes(1024 * 1024):
                        handle.write(chunk)
                        digest.update(chunk)
                        written += len(chunk)
                        if written > MAX_LORA_BYTES:
                            raise ResolveError("too_large", "download exceeded the LoRA size limit")
                        if time.monotonic() - last_note > 3:
                            last_note = time.monotonic()
                            await _set_mirror(entry_id, "downloading", written, "")
        if digest.hexdigest() != sha:
            raise ResolveError("hash_mismatch",
                               f"downloaded file hashes to {digest.hexdigest()[:12]}…, "
                               f"Civitai says {sha[:12]}…")
        os.replace(part, target)
        await _set_mirror(entry_id, "ready", written, "")
        _spawn(kick_boxes())
    except ResolveError as exc:
        part.unlink(missing_ok=True)
        await _set_mirror(entry_id, "failed", written, exc.message)
    except Exception as exc:  # network trouble: recorded, retried on demand
        logger.exception("LoRA mirror download failed for %s", entry_id)
        part.unlink(missing_ok=True)
        await _set_mirror(entry_id, "failed", written, f"{type(exc).__name__}: {exc}"[:300])


async def _set_mirror(entry_id: str, state: str, written: int, error: str) -> None:
    async with _lock:
        data = load_registry()
        for item in data["loras"]:
            if item.get("id") == entry_id:
                item["mirror"] = {"state": state, "bytes": written, "error": error,
                                  "updated_at": _now()}
        save_registry(data)


async def _save_preview(entry_id: str, url: str) -> str:
    if not url:
        return ""
    name = f"lora-{re.sub(r'[^A-Za-z0-9._-]', '_', entry_id)}.jpg"
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=40.0)
            response.raise_for_status()
        from PIL import Image
        image = Image.open(io.BytesIO(response.content))
        image.thumbnail((450, 600))
        _preview_dir().mkdir(parents=True, exist_ok=True)
        image.convert("RGB").save(_preview_dir() / name, "JPEG", quality=85)
        return f"/api/ai/model-preview/{name}"
    except Exception:
        logger.warning("Could not store the preview for %s", entry_id, exc_info=True)
        return ""


def _spawn(coro) -> None:
    task = asyncio.get_event_loop().create_task(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)


# ---------------------------------------------------------------- boxes

def protected_files() -> set:
    """LoRA files the templates or the curated catalogue depend on."""
    names = set()
    for path in WORKFLOWS_DIR.glob("*.json"):
        try:
            names.update(re.findall(r'"lora_name"\s*:\s*"([^"$]+)"', path.read_text(encoding="utf-8")))
        except OSError:
            continue
    try:
        import ai_model_catalogue
        names.update(str(entry.get("file")) for entry in ai_model_catalogue.raw_entries()
                     if entry.get("kind") == "lora")
    except Exception:
        pass
    return names


def _box_files(report: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for item in report.get("files") or []:
        if isinstance(item, dict) and item.get("name"):
            out[str(item["name"])] = item
    return out


def lora_box_state(entry: Dict[str, Any], box: str, cfg: Dict[str, Any],
                   report: Dict[str, Any]) -> Dict[str, Any]:
    allowed = entry.get("boxes")
    if allowed and box not in allowed:
        return {"state": "excluded"}
    files = _box_files(report)
    have = files.get(entry["file"])
    if have and str(have.get("sha256") or "").lower() == entry["sha256"]:
        return {"state": "ready"}
    if not report:
        return {"state": "no_agent"}
    reported = (report.get("items") or {}).get(entry["id"]) or {}
    stale = _now() - int(report.get("reported_at") or 0) > BOX_STALE_SECONDS
    if have and have.get("sha256") and str(have["sha256"]).lower() != entry["sha256"]:
        return {"state": "hash_mismatch",
                "error": f"a different file called {entry['file']} is already on the box"}
    state = str(reported.get("state") or "")
    if state in ("downloading", "failed", "hash_mismatch", "no_space"):
        out = {"state": state, "error": str(reported.get("error") or "")}
        if reported.get("bytes"):
            out["bytes"] = int(reported["bytes"])
        if stale:
            out["stale"] = True
        return out
    return {"state": "offline" if stale else "queued"}


def ready_boxes(entry: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> List[str]:
    out = []
    for box, cfg in boxes(data).items():
        if lora_box_state(entry, box, cfg, box_report(box)).get("state") == "ready":
            out.append(box)
    return out


# ---------------------------------------------------- catalogue integration

_catalogue_cache: Dict[str, Any] = {"at": 0.0, "entries": []}


def catalogue_entries() -> List[Dict[str, Any]]:
    """Managed LoRAs as model-catalogue entries (kept 20 s)."""
    where = str(_lora_dir())
    if (time.monotonic() - _catalogue_cache["at"] < 20.0
            and _catalogue_cache.get("dir") == where):
        return _catalogue_cache["entries"]
    data = load_registry()
    out = []
    for entry in data.get("loras", []):
        if entry.get("state") == "removed":
            continue
        ready = ready_boxes(entry, data)
        mirror = entry.get("mirror") or {}
        reason = ""
        if not ready:
            reason = ("the VPS could not fetch it: " + str(mirror.get("error"))
                      if mirror.get("state") == "failed"
                      else "waiting for the render computers to download it")
        recommended = {}
        if entry.get("recommended_strength"):
            recommended["strength"] = float(entry["recommended_strength"])
        out.append({
            "kind": "lora", "family": entry.get("family") or "", "file": entry["file"],
            "title": entry.get("title") or entry["file"], "version": entry.get("version") or "",
            "base": entry.get("base") or "", "nsfw": bool(entry.get("nsfw")),
            # Trigger words are shown, not injected: Civitai users write them
            # into the prompt themselves, and a silently prefixed word would
            # make a reproduced prompt differ from the one it came from.
            "triggers": [], "trained_words": entry.get("trained_words") or [],
            "page": entry.get("page") or "", "preview": entry.get("preview") or "",
            "services": entry.get("services") or [], "usable": bool(ready),
            "unusable_reason": reason, "validated_workers": ready,
            "sha256": entry["sha256"], "size_mb": round((entry.get("size_bytes") or 0) / 1048576, 1),
            "source_version_id": (entry.get("source") or {}).get("version_id"),
            "aliases": entry.get("aliases") or [], "managed": True, "id": entry["file"],
            **({"recommended": recommended,
                "recommended_from": "Civitai model description"} if recommended else {}),
        })
    _catalogue_cache.update(at=time.monotonic(), entries=out, dir=where)
    return out


# ----------------------------------------------------------------- kicks

async def _run_ssh(box: str, cfg: Dict[str, Any], command: str, timeout: float = 25.0) -> Tuple[int, str]:
    if not cfg.get("ssh_port") or not FARM_SSH_KEY.is_file():
        return -1, "no ssh route"
    known = _path("known_hosts")
    args = ["ssh", "-i", str(FARM_SSH_KEY), "-p", str(int(cfg["ssh_port"])),
            "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={known}",
            f"{cfg.get('ssh_user') or 'user'}@{FARM_GATEWAY}", command]
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode or 0, out.decode("utf-8", "replace")[-400:]
    except Exception as exc:
        return -1, f"{type(exc).__name__}: {exc}"


async def kick_boxes() -> Dict[str, str]:
    """Ask every reachable box to run its sync task now."""
    results: Dict[str, str] = {}
    configured = boxes()
    jobs = {box: _run_ssh(box, cfg, f'schtasks /run /tn "{SYNC_TASK_NAME}"')
            for box, cfg in configured.items() if cfg.get("ssh_port")}
    outcomes = await asyncio.gather(*jobs.values(), return_exceptions=True)
    for box, outcome in zip(jobs, outcomes):
        if isinstance(outcome, Exception):
            results[box] = f"error: {outcome}"
        else:
            code, text = outcome
            results[box] = "ok" if code == 0 else f"exit {code}: {text.strip()[-160:]}"
    for box in configured:
        results.setdefault(box, "pulls on its own schedule")
    return results


# ------------------------------------------------------------ box auth

def _sync_keys() -> Dict[str, str]:
    data = _read_json(SYNC_KEYS_FILE, {})
    return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}


def _authenticate_box(request: Request) -> str:
    box = str(request.headers.get("x-autorig-box") or "").strip()
    auth = str(request.headers.get("authorization") or "")
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    expected = _sync_keys().get(box, "")
    if not box or not token or not expected or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail={"error_string": "box_unauthorised",
                                                     "message_string": "unknown box or key"})
    return box


# ------------------------------------------------------------- box API

@router.get("/api/ai/loras/sync/manifest")
async def api_sync_manifest(request: Request):
    box = _authenticate_box(request)
    data = load_registry()
    cfg = boxes(data).get(box, {})
    items = []
    for entry in data["loras"]:
        if entry.get("state") == "removed":
            continue
        if entry.get("boxes") and box not in entry["boxes"]:
            continue
        if (entry.get("mirror") or {}).get("state") != "ready":
            continue
        # The farm reaches Civitai's CDN about four times faster than it
        # reaches this VPS, so a short-lived presigned CDN link goes after the
        # LAN peers. It authorises that one file only; the API token stays here.
        cdn = await _presigned_url(entry)
        items.append({
            "id": entry["id"], "file": entry["file"], "sha256": entry["sha256"],
            "size_bytes": entry.get("size_bytes") or 0,
            "url": f"{PUBLIC_BASE}/api/ai/loras/sync/blob/{entry['sha256']}",
            "peers": [peer.rstrip("/") + "/" + urllib.parse.quote(entry["file"])
                      for peer in cfg.get("peers") or []] + ([cdn] if cdn else []),
        })
    remove = [{"file": e["file"], "sha256": e["sha256"]} for e in data["loras"]
              if e.get("state") == "removed"
              and not any(o.get("file") == e["file"] and o.get("state") != "removed"
                          for o in data["loras"])]
    cleanup = [c for c in (data.get("cleanup") or {}).get(box, []) if isinstance(c, dict)]
    return {"box_string": box, "items_array": items, "remove_array": remove,
            "cleanup_array": cleanup, "protected_array": sorted(protected_files()),
            "interval_seconds_int": SYNC_INTERVAL_SECONDS, "server_time_unix_int": _now()}


_presigned: Dict[str, Tuple[float, str]] = {}
PRESIGNED_TTL_SECONDS = 15 * 60


async def _presigned_url(entry: Dict[str, Any]) -> str:
    """Civitai's redirect target for this file, cached for a quarter hour."""
    source = entry.get("source") or {}
    if source.get("kind") != "civitai" or not os.getenv("CIVITAI_API_TOKEN"):
        return ""
    cached = _presigned.get(entry["id"])
    if cached and time.monotonic() - cached[0] < PRESIGNED_TTL_SECONDS:
        return cached[1]
    url = str(source.get("download_url") or
              f"https://civitai.com/api/download/models/{source.get('version_id')}")
    try:
        async with httpx.AsyncClient(follow_redirects=False) as client:
            response = await client.get(url, headers=_civitai_headers(), timeout=20.0)
        location = response.headers.get("location", "") if response.is_redirect else ""
    except httpx.HTTPError:
        location = ""
    if not location.startswith("https://") or "civitai.com/api/" in location:
        return ""
    _presigned[entry["id"]] = (time.monotonic(), location)
    return location


AGENT_SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "deploy" / "onlyrender" / "lora-sync.ps1"


@router.get("/api/ai/loras/sync/agent.ps1")
async def api_sync_agent():
    """The box-side agent itself; it holds no secret, the key stays on the box."""
    # Read through the live release link, like _static_html_response: a
    # script-only release must reach the boxes without restarting the API.
    live = pathlib.Path("/srv/autorig/current/autorig-online/deploy/onlyrender/lora-sync.ps1")
    script = live if live.is_file() else AGENT_SCRIPT
    if not script.is_file():
        raise HTTPException(status_code=404, detail="agent script not deployed")
    return FileResponse(script, media_type="text/plain; charset=utf-8",
                        headers={"Cache-Control": "no-store"})


@router.get("/api/ai/loras/sync/blob/{sha}")
async def api_sync_blob(sha: str, request: Request):
    _authenticate_box(request)
    if not re.fullmatch(r"[0-9a-f]{64}", sha or ""):
        raise HTTPException(status_code=400, detail="bad hash")
    data = load_registry()
    if not any(e.get("sha256") == sha and e.get("state") != "removed" for e in data["loras"]):
        raise HTTPException(status_code=404, detail="not a registered LoRA")
    path = _path("blobs") / sha
    if not path.is_file():
        raise HTTPException(status_code=404, detail="not mirrored yet")
    return FileResponse(path, media_type="application/octet-stream")


class BoxReport(BaseModel):
    loras_dirs: List[str] = Field(default_factory=list)
    free_bytes: int = 0
    files: List[Dict[str, Any]] = Field(default_factory=list)
    items: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    agent_version: str = ""
    comfy_root: str = ""


@router.post("/api/ai/loras/sync/report")
async def api_sync_report(body: BoxReport, request: Request):
    box = _authenticate_box(request)
    files = []
    for item in body.files[:2000]:
        name = str(item.get("name") or "")[:300]
        if not name:
            continue
        files.append({"name": name, "dir": str(item.get("dir") or "")[:300],
                      "size": int(item.get("size") or 0),
                      "sha256": str(item.get("sha256") or "").lower()[:64],
                      "mtime": int(item.get("mtime") or 0)})
    report = {"box": box, "reported_at": _now(), "loras_dirs": body.loras_dirs[:8],
              "free_bytes": int(body.free_bytes or 0), "files": files,
              "items": {str(k)[:80]: v for k, v in list(body.items.items())[:200]},
              "agent_version": body.agent_version[:40], "comfy_root": body.comfy_root[:200]}
    _path("boxes").mkdir(parents=True, exist_ok=True)
    _write_json(_path("boxes") / f"{box}.json", report)
    names = {f["name"] for f in files}
    async with _lock:
        data = load_registry()
        pending = (data.get("cleanup") or {}).get(box) or []
        still = [c for c in pending if c.get("file") in names]
        if len(still) != len(pending):
            data.setdefault("cleanup", {})[box] = still
            save_registry(data)
    _catalogue_cache["at"] = 0.0
    _spawn(_lookup_unknown_hashes([f["sha256"] for f in files if f["sha256"]]))
    return {"success_bool": True, "server_time_unix_int": _now()}


async def _lookup_unknown_hashes(hashes: List[str]) -> None:
    """Name the files already on the boxes by asking Civitai who they are."""
    cache = _read_json(_path("hash_lookup.json"), {})
    if not isinstance(cache, dict):
        cache = {}
    known = {e["sha256"] for e in load_registry()["loras"]}
    todo = [h for h in dict.fromkeys(hashes) if h not in cache and h not in known][:40]
    if not todo:
        return
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for sha in todo:
            try:
                response = await client.get(
                    f"https://civitai.com/api/v1/model-versions/by-hash/{sha}",
                    headers=_civitai_headers(), timeout=30.0)
                if response.status_code == 404:
                    cache[sha] = {"found": False, "checked_at": _now()}
                    continue
                response.raise_for_status()
                v = response.json()
                cache[sha] = {"found": True, "checked_at": _now(), "version_id": v.get("id"),
                              "model_id": v.get("modelId"), "base": v.get("baseModel"),
                              "title": (v.get("model") or {}).get("name"),
                              "version": v.get("name"),
                              "page": f"https://civitai.com/models/{v.get('modelId')}?modelVersionId={v.get('id')}"}
            except Exception:
                logger.warning("Civitai hash lookup failed for %s", sha[:12], exc_info=True)
            await asyncio.sleep(0.4)
    _write_json(_path("hash_lookup.json"), cache)


# ---------------------------------------------------------- public views

def _entry_view(entry: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
    states = {}
    for box, cfg in boxes(data).items():
        states[box] = lora_box_state(entry, box, cfg, box_report(box))
    view = {k: entry.get(k) for k in (
        "id", "file", "sha256", "size_bytes", "family", "base", "services", "title", "version",
        "page", "nsfw", "trained_words", "recommended_strength", "preview", "added_at",
        "state", "mirror", "boxes", "aliases")}
    view["box_states_object"] = states
    view["tag_string"] = f"<lora:{entry['file'].rsplit('.', 1)[0]}:{entry.get('recommended_strength') or 1:g}>"
    return view


def inventory_view(data: Dict[str, Any]) -> Dict[str, Any]:
    managed = {e["sha256"]: e for e in data["loras"] if e.get("state") != "removed"}
    lookups = _read_json(_path("hash_lookup.json"), {})
    protected = protected_files()
    out = {}
    for box, cfg in boxes(data).items():
        report = box_report(box)
        files = []
        for item in report.get("files") or []:
            sha = str(item.get("sha256") or "")
            owner = managed.get(sha)
            files.append({**item, "managed_id": owner["id"] if owner else "",
                          "protected": item.get("name") in protected,
                          "civitai": lookups.get(sha) if isinstance(lookups, dict) else None,
                          "cleanup_pending": any(c.get("file") == item.get("name")
                                                 for c in (data.get("cleanup") or {}).get(box, []))})
        out[box] = {"reported_at": report.get("reported_at") or 0,
                    "stale": bool(report) and _now() - int(report.get("reported_at") or 0) > BOX_STALE_SECONDS,
                    "agent": bool(report), "free_bytes": report.get("free_bytes") or 0,
                    "loras_dirs": report.get("loras_dirs") or [], "files": files,
                    "ssh": bool(cfg.get("ssh_port"))}
    return out


@router.get("/api/ai/loras")
async def api_loras(request: Request):
    data = load_registry()
    import ai_queue_admin
    admin = await ai_queue_admin.viewer_is_admin(request)
    loras = [_entry_view(e, data) for e in data["loras"] if e.get("state") != "removed"]
    return {"success_bool": True, "admin_bool": admin, "loras_array": loras,
            "boxes_object": inventory_view(data) if admin else {
                box: {"agent": bool(box_report(box))} for box in boxes(data)},
            "server_time_unix_int": _now()}


# --------------------------------------------------- Civitai image import

_A1111_SCHEDULERS = (("karras", "karras"), ("exponential", "exponential"),
                     ("sgm uniform", "sgm_uniform"), ("simple", "simple"), ("beta", "beta"))


def _split_sampler(name: str) -> Tuple[str, str]:
    import ai_model_defaults
    text = str(name or "").strip().lower()
    scheduler = ""
    for suffix, value in _A1111_SCHEDULERS:
        if text.endswith(" " + suffix):
            text, scheduler = text[: -len(suffix) - 1].strip(), value
    sampler = ai_model_defaults.SAMPLER_ALIASES.get(text, text.replace(" ", "_").replace("++", "pp"))
    return sampler, scheduler


def _graph_from_meta(meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    comfy = meta.get("comfy")
    try:
        graph = json.loads(comfy) if isinstance(comfy, str) else comfy
    except ValueError:
        return None
    if not isinstance(graph, dict):
        return None
    prompt = graph.get("prompt") if isinstance(graph.get("prompt"), dict) else graph
    return prompt if isinstance(prompt, dict) else None


def settings_from_comfy(graph: Dict[str, Any]) -> Dict[str, Any]:
    """Read one-sampler text-to-image graphs; anything else is reported, not guessed."""
    samplers = [(nid, n) for nid, n in graph.items() if isinstance(n, dict)
                and n.get("class_type") in ("KSampler", "KSamplerAdvanced")]
    notes: List[str] = []
    if len(samplers) != 1:
        notes.append(f"graph has {len(samplers)} samplers; only single-pass graphs reproduce exactly")
    out: Dict[str, Any] = {"notes": notes}
    if not samplers:
        return out
    _sid, sampler = samplers[0]
    inputs = sampler.get("inputs") or {}
    out.update(seed=inputs.get("seed", inputs.get("noise_seed")), steps=inputs.get("steps"),
               cfg=inputs.get("cfg"), sampler=inputs.get("sampler_name"),
               scheduler=inputs.get("scheduler"))
    if float(inputs.get("denoise", 1.0) or 1.0) != 1.0:
        notes.append("sampler denoise is below 1")

    def node(ref):
        return graph.get(str(ref[0])) if isinstance(ref, list) and ref else None

    def text_of(ref, depth=0):
        n = node(ref)
        if not n or depth > 8:
            return None
        if n.get("class_type") == "CLIPTextEncode":
            value = (n.get("inputs") or {}).get("text")
            return value if isinstance(value, str) else text_of(value, depth + 1)
        if isinstance(n.get("inputs"), dict):
            for key in ("conditioning", "conditioning_to", "positive"):
                if key in n["inputs"]:
                    return text_of(n["inputs"][key], depth + 1)
        if "value" in (n.get("inputs") or {}):
            return (n.get("inputs") or {}).get("value")
        return None

    out["prompt"] = text_of(inputs.get("positive"))
    out["negative_prompt"] = text_of(inputs.get("negative"))
    latent = node(inputs.get("latent_image"))
    if latent and latent.get("class_type") in ("EmptyLatentImage", "EmptySD3LatentImage"):
        out["width"] = latent["inputs"].get("width")
        out["height"] = latent["inputs"].get("height")
    else:
        notes.append("the latent is not an empty latent (img2img or upscale)")
    # Walk the model input back to the loader, collecting the LoRA chain.
    loras: List[Dict[str, Any]] = []
    ref = inputs.get("model")
    seen = 0
    checkpoint = ""
    while ref and seen < 30:
        seen += 1
        n = node(ref)
        if not n:
            break
        kind = n.get("class_type")
        n_in = n.get("inputs") or {}
        if kind in ("LoraLoader", "LoraLoaderModelOnly"):
            if float(n_in.get("strength_model", 0) or 0) != 0 or float(n_in.get("strength_clip", 0) or 0) != 0:
                loras.append({"name": n_in.get("lora_name"),
                              "strength_model": n_in.get("strength_model"),
                              "strength_clip": n_in.get("strength_clip", n_in.get("strength_model"))})
            ref = n_in.get("model")
            continue
        if kind in ("CheckpointLoaderSimple", "CheckpointLoader", "UNETLoader", "UnetLoaderGGUF"):
            checkpoint = str(n_in.get("ckpt_name") or n_in.get("unet_name") or "")
            break
        ref = n_in.get("model")
        if kind not in ("ModelSamplingDiscrete", "ModelSamplingFlux", "ModelSamplingAuraFlow"):
            notes.append(f"model passes through {kind}")
    loras.reverse()  # nearest the checkpoint first
    out["loras"] = loras
    out["checkpoint_name"] = checkpoint
    clip_skip = [n for n in graph.values() if isinstance(n, dict) and n.get("class_type") == "CLIPSetLastLayer"]
    if clip_skip:
        out["clip_skip"] = abs(int(clip_skip[0]["inputs"].get("stop_at_clip_layer", -1)))
    return out


def settings_from_a1111(meta: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"notes": []}
    sampler, scheduler = _split_sampler(str(meta.get("sampler") or ""))
    out.update(prompt=meta.get("prompt"), negative_prompt=meta.get("negativePrompt"),
               seed=meta.get("seed"), steps=meta.get("steps"), cfg=meta.get("cfgScale"),
               sampler=sampler or None,
               scheduler=scheduler or str(meta.get("Schedule type") or meta.get("scheduler") or "").lower() or None)
    size = str(meta.get("Size") or "")
    m = re.fullmatch(r"(\d+)x(\d+)", size)
    out["width"] = int(m.group(1)) if m else meta.get("width")
    out["height"] = int(m.group(2)) if m else meta.get("height")
    if meta.get("Clip skip") or meta.get("clipSkip"):
        out["clip_skip"] = int(meta.get("Clip skip") or meta.get("clipSkip"))
    out["checkpoint_name"] = str(meta.get("Model") or "")
    loras = []
    for res in meta.get("civitaiResources") or []:
        if str(res.get("type") or "").lower() in ("lora", "locon", "lycoris", "dora"):
            loras.append({"version_id": res.get("modelVersionId"),
                          "strength_model": res.get("weight", 1), "strength_clip": res.get("weight", 1)})
    for res in meta.get("resources") or []:
        if str(res.get("type") or "").lower() in ("lora", "locon", "lycoris"):
            loras.append({"name": res.get("name"), "strength_model": res.get("weight", 1),
                          "strength_clip": res.get("weight", 1), "hash": res.get("hash")})
    out["loras"] = loras
    if meta.get("engine") != "ComfyUI":
        out["notes"].append("not made with ComfyUI: A1111/Forge noise, prompt weighting and "
                            "Clip skip handling differ, so expect a close match, not a pixel match")
    return out


async def civitai_image_prefill(url: str) -> Dict[str, Any]:
    m = re.search(r"civitai\.[a-z]+/images/(\d+)", str(url or "")) or re.fullmatch(r"\s*(\d{5,12})\s*", str(url or ""))
    if not m:
        raise ResolveError("bad_url", "Paste a civitai.com/images/<id> link")
    image_id = int(m.group(1))
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(
            "https://civitai.com/api/v1/images",
            params={"imageId": image_id, "withMeta": "true", "nsfw": "X"},
            headers=_civitai_headers(), timeout=40.0)
        if response.status_code >= 400:
            raise ResolveError("civitai_error", f"Civitai answered HTTP {response.status_code}", 502)
        items = (response.json() or {}).get("items") or []
    if not items:
        raise ResolveError("not_found", "Civitai has no such image (or it is hidden)", 404)
    image = items[0]
    meta = image.get("meta") or {}
    if isinstance(meta, dict) and isinstance(meta.get("meta"), dict):
        meta = meta["meta"]
    if not meta:
        raise ResolveError("no_metadata", "That image carries no generation data")
    graph = _graph_from_meta(meta)
    settings = settings_from_comfy(graph) if graph else settings_from_a1111(meta)
    settings["source_string"] = "comfy" if graph else "a1111"
    import ai_model_catalogue
    import ai_lora_prompt
    import ai_model_defaults
    # Checkpoint: by file name, then by stem, then by the Civitai alias table.
    ckpt_name = str(settings.get("checkpoint_name") or "")
    stem = ckpt_name.rsplit(".", 1)[0].lower()
    checkpoint = ""
    for entry in ai_model_catalogue.entries():
        if entry.get("kind") != "checkpoint":
            continue
        names = {str(entry.get("file") or "")}
        names |= {alias for alias, canonical in ai_model_defaults.MODEL_FILE_ALIASES.items()
                  if canonical == entry.get("file")}
        if ckpt_name in names or any(n.rsplit(".", 1)[0].lower() == stem or
                                     n.rsplit(".", 1)[0].lower().startswith(stem + "_")
                                     for n in names if n):
            checkpoint = str(entry.get("file"))
            break
    data = load_registry()
    loras_out = []
    catalogue_loras = [e for e in ai_model_catalogue.entries() if e.get("kind") == "lora"]
    for item in settings.get("loras") or []:
        entry = None
        if item.get("version_id"):
            entry = next((e for e in data["loras"] if (e.get("source") or {}).get("version_id")
                          == item["version_id"] and e.get("state") != "removed"), None)
            name = entry["file"] if entry else ""
        else:
            name = str(item.get("name") or "")
        catalogue_entry = None
        if name:
            try:
                catalogue_entry = ai_lora_prompt.resolve_name(
                    name.rsplit(".", 1)[0] if name.endswith(".safetensors") else name, catalogue_loras)
            except ValueError:
                catalogue_entry = None
        loras_out.append({
            "name": str((catalogue_entry or {}).get("file") or name),
            "strength_model": float(item.get("strength_model") or 0),
            "strength_clip": float(item.get("strength_clip") if item.get("strength_clip") is not None
                                   else item.get("strength_model") or 0),
            "installed_bool": bool(catalogue_entry and catalogue_entry.get("usable")),
            "registered_bool": bool(catalogue_entry),
            "civitai_version_id": item.get("version_id"),
        })
    return {
        "image_id_int": image_id, "image_url_string": image.get("url"),
        "width_int": image.get("width"), "height_int": image.get("height"),
        "nsfw_level_string": image.get("nsfwLevel"), "source_string": settings["source_string"],
        "prompt_string": settings.get("prompt") or "",
        "negative_prompt_string": settings.get("negative_prompt") or "",
        "seed": settings.get("seed"), "steps": settings.get("steps"), "cfg": settings.get("cfg"),
        "sampler": settings.get("sampler"), "scheduler": settings.get("scheduler"),
        "width": settings.get("width"), "height": settings.get("height"),
        "clip_skip": settings.get("clip_skip"),
        "checkpoint_name_string": ckpt_name, "checkpoint_string": checkpoint,
        "loras_array": loras_out,
        "lora_tags_string": ai_lora_prompt.to_tags(
            [{"name": l["name"], "strength_model": l["strength_model"],
              "strength_clip": l["strength_clip"]} for l in loras_out if l["name"]]),
        "notes_array": settings.get("notes") or [],
    }


@router.get("/api/ai/loras/civitai-image/{image_id}/original")
async def api_civitai_image_original(image_id: int):
    """The original file of a Civitai image, served from our origin.

    The page compares pixels on a canvas, which a cross-origin image would
    taint. Only the URL Civitai's own API names for this image id is fetched,
    so this is not an open proxy; the file is kept for the next comparison.
    """
    from fastapi.responses import Response

    refs = _path("refs")
    for cached in refs.glob(f"{int(image_id)}.*") if refs.is_dir() else []:
        media = "image/png" if cached.suffix == ".png" else "image/jpeg"
        return FileResponse(cached, media_type=media, headers={"Cache-Control": "public, max-age=86400"})
    async with httpx.AsyncClient(follow_redirects=True) as client:
        listing = await client.get("https://civitai.com/api/v1/images",
                                   params={"imageId": int(image_id), "nsfw": "X"},
                                   headers=_civitai_headers(), timeout=40.0)
        items = (listing.json() or {}).get("items") or [] if listing.status_code < 400 else []
        if not items:
            raise HTTPException(status_code=404, detail="no such Civitai image")
        url = str(items[0].get("url") or "")
        if not url.startswith("https://image.civitai.com/"):
            raise HTTPException(status_code=502, detail="unexpected image host")
        response = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=60.0)
    if response.status_code >= 400 or len(response.content) > 40 * 1024 * 1024:
        raise HTTPException(status_code=502, detail="Civitai did not return the image")
    data = response.content
    ext = ".png" if data[:8] == b"\x89PNG\r\n\x1a\n" else ".jpg"
    refs.mkdir(parents=True, exist_ok=True)
    (refs / f"{int(image_id)}{ext}").write_bytes(data)
    return Response(content=data, media_type="image/png" if ext == ".png" else "image/jpeg",
                    headers={"Cache-Control": "public, max-age=86400"})


class ImportBody(BaseModel):
    url: str = Field(..., max_length=500)


@router.post("/api/ai/loras/civitai-image")
async def api_civitai_image(body: ImportBody):
    """Generation data of a Civitai image, mapped onto our request fields."""
    try:
        return {"success_bool": True, **(await civitai_image_prefill(body.url))}
    except ResolveError as exc:
        raise HTTPException(status_code=exc.status, detail={
            "error_string": exc.code, "message_string": exc.message}) from None


# ------------------------------------------------------------- admin API

async def add_lora(url: str, *, base: str = "", force: bool = False,
                   only_boxes: Optional[List[str]] = None, added_by: str = "",
                   wait_for_mirror: bool = False) -> Dict[str, Any]:
    """Resolve a link, register it, mirror the file and nudge the boxes.

    One code path for the /lora page and for `python3 -m ai_lora_manager add`
    run on the VPS by an operator.
    """
    record = await resolve_url(url, base)
    if not record["family"] or not record["services"]:
        raise ResolveError("unknown_base", f"Base model '{record['base']}' is not one the farm knows")
    if not record["compatible_checkpoints"] and not force:
        raise ResolveError("no_checkpoint",
                           f"No checkpoint on the farm runs {record['base']} LoRAs yet; "
                           "install anyway to have it ready when one arrives", 409)
    async with _lock:
        data = load_registry()
        existing = next((e for e in data["loras"] if e.get("sha256") == record["sha256"]), None)
        if existing and existing.get("state") != "removed":
            return {"success_bool": True, "lora_object": _entry_view(existing, data),
                    "note_string": "already installed"}
        record["file"] = _unique_file_name(data, record)
        record.pop("compatible_checkpoints", None)
        record.update(state="active", added_at=_now(), added_by=added_by,
                      mirror={"state": "queued", "bytes": 0, "error": "", "updated_at": _now()})
        if only_boxes:
            record["boxes"] = [b for b in only_boxes if b in boxes(data)]
        if existing:
            data["loras"] = [e for e in data["loras"] if e is not existing]
        data["loras"].append(record)
        save_registry(data)
    record["preview"] = await _save_preview(record["id"], record.get("preview_source", ""))
    if record["preview"]:
        async with _lock:
            data = load_registry()
            for item in data["loras"]:
                if item.get("id") == record["id"]:
                    item["preview"] = record["preview"]
            save_registry(data)
    if wait_for_mirror:
        await _download_to_mirror(record["id"])
    else:
        _spawn(_download_to_mirror(record["id"]))
    data = load_registry()
    entry = next(e for e in data["loras"] if e.get("id") == record["id"])
    return {"success_bool": True, "lora_object": _entry_view(entry, data)}


class AddBody(BaseModel):
    url: str = Field(..., max_length=600)
    base: str = Field("", max_length=80, description="Base model, required for Hugging Face links")
    force: bool = False
    boxes: Optional[List[str]] = None


class PatchBody(BaseModel):
    recommended_strength: Optional[float] = Field(None, ge=-4, le=4)
    boxes: Optional[List[str]] = None


class CleanupBody(BaseModel):
    box: str
    file: str


def build_lora_admin_router(require_admin: Callable[..., Any]) -> APIRouter:
    admin = APIRouter()

    def _raise(exc: ResolveError):
        raise HTTPException(status_code=exc.status, detail={
            "error_string": exc.code, "message_string": exc.message}) from None

    @admin.post("/api/ai/loras/resolve")
    async def api_resolve(body: AddBody, _admin=Depends(require_admin)):
        try:
            record = await resolve_url(body.url, body.base)
        except ResolveError as exc:
            _raise(exc)
        data = load_registry()
        existing = next((e for e in data["loras"] if e.get("sha256") == record["sha256"]
                         and e.get("state") != "removed"), None)
        record["already_installed_id"] = existing["id"] if existing else ""
        return {"success_bool": True, "lora_object": record}

    @admin.post("/api/ai/loras")
    async def api_add(body: AddBody, user=Depends(require_admin)):
        try:
            return await add_lora(body.url, base=body.base, force=body.force, only_boxes=body.boxes,
                                  added_by=str(getattr(user, "email", "") or ""))
        except ResolveError as exc:
            _raise(exc)

    @admin.patch("/api/ai/loras/{entry_id}")
    async def api_patch(entry_id: str, body: PatchBody, _admin=Depends(require_admin)):
        async with _lock:
            data = load_registry()
            entry = next((e for e in data["loras"] if e.get("id") == entry_id), None)
            if not entry:
                raise HTTPException(status_code=404, detail="no such LoRA")
            if body.recommended_strength is not None:
                entry["recommended_strength"] = body.recommended_strength
            if body.boxes is not None:
                entry["boxes"] = [b for b in body.boxes if b in boxes(data)]
            save_registry(data)
        return {"success_bool": True, "lora_object": _entry_view(entry, data)}

    @admin.post("/api/ai/loras/{entry_id}/retry")
    async def api_retry(entry_id: str, _admin=Depends(require_admin)):
        data = load_registry()
        if not any(e.get("id") == entry_id for e in data["loras"]):
            raise HTTPException(status_code=404, detail="no such LoRA")
        _spawn(_download_to_mirror(entry_id))
        return {"success_bool": True}

    @admin.delete("/api/ai/loras/{entry_id}")
    async def api_remove(entry_id: str, _admin=Depends(require_admin)):
        async with _lock:
            data = load_registry()
            entry = next((e for e in data["loras"] if e.get("id") == entry_id), None)
            if not entry:
                raise HTTPException(status_code=404, detail="no such LoRA")
            entry["state"] = "removed"
            entry["removed_at"] = _now()
            save_registry(data)
        (_path("blobs") / entry["sha256"]).unlink(missing_ok=True)
        _spawn(kick_boxes())
        return {"success_bool": True, "note_string": "boxes move the file to their trash on the next sync"}

    @admin.post("/api/ai/loras/cleanup")
    async def api_cleanup(body: CleanupBody, _admin=Depends(require_admin)):
        report = box_report(body.box)
        have = _box_files(report).get(body.file)
        if not have:
            raise HTTPException(status_code=404, detail={
                "error_string": "not_on_box", "message_string": "that box did not report this file"})
        if body.file in protected_files():
            raise HTTPException(status_code=409, detail={
                "error_string": "protected",
                "message_string": "a workflow template or the curated catalogue uses this file"})
        data = load_registry()
        if any(e.get("file") == body.file and e.get("state") != "removed" for e in data["loras"]):
            raise HTTPException(status_code=409, detail={
                "error_string": "managed", "message_string": "remove it from the LoRA list instead"})
        async with _lock:
            data = load_registry()
            queue = data.setdefault("cleanup", {}).setdefault(body.box, [])
            if not any(c.get("file") == body.file for c in queue):
                queue.append({"file": body.file, "size": have.get("size"),
                              "sha256": have.get("sha256") or "", "requested_at": _now()})
            save_registry(data)
        _spawn(kick_boxes())
        return {"success_bool": True, "note_string": "moved to the box's lora_trash folder on its next sync"}

    @admin.post("/api/ai/loras/kick")
    async def api_kick(_admin=Depends(require_admin)):
        return {"success_bool": True, "boxes_object": await kick_boxes()}

    @admin.get("/api/ai/loras/boxes/{box}/install")
    async def api_install(box: str, _admin=Depends(require_admin)):
        key = _sync_keys().get(box)
        if not key:
            raise HTTPException(status_code=404, detail="no sync key for that box")
        command = (
            "powershell -NoProfile -ExecutionPolicy Bypass -Command \"$d='C:\\ProgramData\\AutoRig\\lora-sync';"
            "New-Item -ItemType Directory -Force $d | Out-Null;"
            f"Set-Content -Path \\\"$d\\sync.key\\\" -Value '{key}' -Encoding ascii;"
            f"Set-Content -Path \\\"$d\\box.txt\\\" -Value '{box}' -Encoding ascii;"
            "Invoke-WebRequest -UseBasicParsing https://autorig.online/api/ai/loras/sync/agent.ps1 -OutFile \\\"$d\\lora-sync.ps1\\\";"
            "& \\\"$d\\lora-sync.ps1\\\" -Install\"")
        return {"success_bool": True, "command_string": command}

    return admin


def _cli(argv: List[str]) -> int:
    """Operator entry point on the VPS (runs as the autorig user, env from the service)."""
    import argparse
    parser = argparse.ArgumentParser(prog="python3 -m ai_lora_manager")
    sub = parser.add_subparsers(dest="cmd", required=True)
    add = sub.add_parser("add", help="install a LoRA from a Civitai / Hugging Face link")
    add.add_argument("url")
    add.add_argument("--base", default="")
    add.add_argument("--force", action="store_true")
    sub.add_parser("list", help="show every LoRA and its per-box state")
    sub.add_parser("kick", help="ask every reachable box to sync now")
    args = parser.parse_args(argv)

    async def run() -> Any:
        if args.cmd == "add":
            result = await add_lora(args.url, base=args.base, force=args.force,
                                    added_by="cli", wait_for_mirror=True)
            print(json.dumps(await kick_boxes(), ensure_ascii=False))
            return result
        if args.cmd == "kick":
            return await kick_boxes()
        data = load_registry()
        return [_entry_view(e, data) for e in data["loras"] if e.get("state") != "removed"]

    try:
        result = asyncio.run(run())
    except ResolveError as exc:
        print(json.dumps({"error": exc.code, "message": exc.message}))
        return 1
    print(json.dumps(result, indent=1, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(_cli(sys.argv[1:]))

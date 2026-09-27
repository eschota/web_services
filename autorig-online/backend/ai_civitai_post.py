"""Post a /nodes output to the owner's Civitai account (owner, 2026-09-27).

Civitai has no public write API. The website itself uploads a picture through
``/api/v1/image-upload`` (a presigned object-storage URL) and builds the post
with tRPC mutations. The server's CIVITAI_API_TOKEN (account NoDeadLine, full
scope) is accepted by those tRPC routes, so the backend can post directly.

What was measured on drafts (2026-09-27):

* Resources: ``meta.civitaiResources`` ([{modelVersionId, type, weight}]) in
  ``post.addImage`` links the versions at once (``image.getResources``). The
  website's own "add resource" button is ``post.addResourceToImage``
  ({id: [imageId], modelVersionId}); it is called for every version the
  read-back does not show. Post 31261839 had no resources because the dialog
  sent none: Qwen-Image 2.1 had no Civitai version on our side. Versions are
  now also resolved here, from the render ledger, the model catalogue and the
  LoRA registry. Limits: 10 manual resources per image, 3 checkpoints.
* Draft: ``post.update`` without ``publishedAt`` keeps the post unpublished;
  ``publishedAt: null`` is refused ("expected date") and left an orphan draft.
  Publishing sends a superjson Date.
* Tags: the website's editor allows 5 per post (POST_TAG_LIMIT); tags are free
  text, an existing tag name is reused. Image tags are Civitai's own
  auto-tagging and cannot be written.
* Techniques (txt2img, img2img, controlnet, img2vid, ...) and tools (ComfyUI)
  are ``image.addTechniques`` / ``image.addTools``.

Every failure answers with a manual path instead: the file to download, the
text to paste and the page to open. ``dry_run`` sends nothing.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import shutil
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

CIVITAI_HOST = os.getenv("CIVITAI_POST_HOST", "https://civitai.red").rstrip("/")
CREATE_PAGE = CIVITAI_HOST + "/posts/create"
NSFW_LEVELS = ("None", "Soft", "Mature", "X")
IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")
VIDEO_SUFFIXES = (".mp4", ".webm", ".mov", ".m4v")
AUDIO_SUFFIXES = (".mp3", ".wav", ".flac", ".ogg", ".m4a")
SCRATCH_DIR = Path(os.getenv("AUTORIG_SCRATCH_DIR", "/srv/autorig/data/var/civitai-post"))
MODEL_CATALOGUE = Path(os.getenv("AUTORIG_MODEL_CATALOGUE",
                                 "/srv/autorig/data/var/ai-models/model_catalogue.json"))

POST_TAG_LIMIT = 5            # Civitai's post editor (src/server/common/constants.ts)
MAX_RESOURCES = 10            # MAX_MANUAL_RESOURCES_PER_IMAGE
MAX_CHECKPOINTS = 3           # MAX_MANUAL_CHECKPOINTS_PER_IMAGE
TITLE_MAX = 120
COMFYUI_TOOL = "ComfyUI"
UPSCALER_NAME = "RealESRGAN_x2"

# Models whose Civitai version the catalogue does not carry.
KNOWN_VERSIONS: Dict[str, Tuple[int, str, str]] = {
    "qwen_image_2.1_int8_convrot.safetensors": (3352534, "Qwen Image 2.1", "checkpoint"),
    "z_image_turbo_fp8_e4m3fn.safetensors": (2442439, "Z-Image Turbo", "checkpoint"),
    "krea2_turbo_fp8_scaled.safetensors": (3091481, "Krea 2 Turbo", "checkpoint"),
    "qwen-image-2512-Q3_K_S.gguf": (2552908, "Qwen-Image-2512", "checkpoint"),
    "minimax_h3_fl2va_pruned_int8_convrot.safetensors": (3216500, "MiniMax H3", "checkpoint"),
}
# Parts of a model that have no Civitai page: named in the description.
NO_PAGE_NOTES: Dict[str, str] = {
    "qwen_image_2.1_int8_convrot.safetensors":
        'Generated with Qwen-Image 2.1 + Viggle turbo v0.2.1 (6-step LoRA): '
        '<a href="https://huggingface.co/Viggle/Qwen-Image-2.1-viggle-turbo">'
        'huggingface.co/Viggle/Qwen-Image-2.1-viggle-turbo</a>',
}


class CivitaiResource(BaseModel):
    model_version_id: int = Field(..., ge=1)
    name: str = ""
    weight: Optional[float] = None
    type: str = ""  # checkpoint | lora


class CivitaiPostRequest(BaseModel):
    media_url: str = Field(..., max_length=4096)
    title: str = Field("", max_length=300)
    description: str = Field("", max_length=12000)
    prompt: str = Field("", max_length=12000)
    tags: List[str] = Field(default_factory=list, max_length=40)
    nsfw_level: str = Field(..., description="None, Soft, Mature or X; the owner confirms it")
    resources: List[CivitaiResource] = Field(default_factory=list, max_length=20)
    publish: bool = False
    dry_run: bool = False
    poster_url: Optional[str] = Field(None, max_length=4096, description="Still for audio")
    # Removed 2026-09-27 (owner): the pre-post upscale cropped a portrait
    # clip. Posts send the output file as it is; enlarge with the Upscale 2x
    # node in the graph first. The field is accepted and ignored.
    upscale: bool = Field(False, description="Ignored: posts use the file as it is")
    generation: Dict[str, Any] = Field(default_factory=dict, description="seed, steps, sampler, cfg, model...")
    source_urls: List[str] = Field(default_factory=list, max_length=60,
                                   description="Render outputs upstream of this node (for tools that only "
                                               "join, mux or cut renders: their prompt, models and settings)")
    background: bool = Field(False, description="Answer at once with a job id; poll /api/ai/civitai/jobs/<id>")
    auto_meta: bool = Field(True, description="Write title/description/tags with the LLM when the title is "
                                              "empty or generic (Video, Image, ...)")


class CivitaiMetaRequest(BaseModel):
    media_url: str = Field(..., max_length=4096)
    prompt: str = Field("", max_length=12000)
    resources: List[str] = Field(default_factory=list, max_length=20)
    service: str = ""
    caption_only: bool = Field(False, description="Answer with the Vision caption only (first, fast step)")
    caption: str = Field("", max_length=4000, description="A caption from the first step; skips Vision")


def _token() -> str:
    return str(os.getenv("CIVITAI_API_TOKEN") or "").strip()


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json",
            "User-Agent": "AutoRigNodes/1.0 (+https://autorig.online/nodes)", "x-client": "web"}


def _kind(url: str) -> str:
    path = str(url or "").split("?", 1)[0].split("#", 1)[0].lower()
    if path.endswith(VIDEO_SUFFIXES):
        return "video"
    if path.endswith(AUDIO_SUFFIXES):
        return "audio"
    return "image"


# ---------------------------------------------------------------- jobs
# One post = one job the page can watch: writing title/tags ->
# upload to Civitai (%), post, done. Kept in memory and
# mirrored to disk so a status read survives a page reload; a job cut by a
# server restart is marked interrupted when the module loads.

JOBS: Dict[str, Dict[str, Any]] = {}
JOB_DIR = SCRATCH_DIR / "jobs"
GENERIC_TITLES = {"", "video", "image", "picture", "clip", "audio", "music", "output", "result"}
FINAL_STAGES = ("done", "failed", "manual", "interrupted")


def _job_save(job: Dict[str, Any]) -> None:
    try:
        JOB_DIR.mkdir(parents=True, exist_ok=True)
        (JOB_DIR / f"{job['id']}.json").write_text(json.dumps(job), encoding="utf-8")
    except Exception:
        pass


def _job_update(job: Optional[Dict[str, Any]], **fields: Any) -> None:
    if job is None:
        return
    job.update(fields, updated_at=time.time())
    _job_save(job)


def _load_jobs() -> None:
    try:
        paths = sorted(JOB_DIR.glob("*.json"), key=lambda path: path.stat().st_mtime)[-200:]
        for path in paths:
            job = json.loads(path.read_text(encoding="utf-8"))
            if job.get("stage") not in FINAL_STAGES:
                job.update(stage="interrupted", stage_label="Interrupted by a server restart",
                           finished_at=job.get("updated_at") or time.time())
                _job_save(job)
            JOBS[job["id"]] = job
    except Exception:
        pass


_load_jobs()


def _public_job(job: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    end = job.get("finished_at") or now
    return dict(job, elapsed_seconds_float=round(end - job.get("created_at", now), 1),
                active_bool=job.get("stage") not in FINAL_STAGES)


def _manual(body: CivitaiPostRequest, reason: str, download_url: str) -> Dict[str, Any]:
    tags = ", ".join(tag.strip() for tag in body.tags if tag.strip())
    details = "\n".join(part for part in [
        body.title.strip(), "", body.description.strip(),
        ("Prompt: " + body.prompt.strip()) if body.prompt.strip() else "",
        ("Tags: " + tags) if tags else "",
        "Rating: " + body.nsfw_level,
        "Resources: " + ", ".join(f"{CIVITAI_HOST}/model-versions/{item.model_version_id}"
                                  for item in body.resources) if body.resources else "",
    ] if part is not None)
    return {"success_bool": False, "manual_bool": True, "reason_string": reason,
            "open_url_string": CREATE_PAGE, "download_url_string": download_url,
            "details_string": details.strip()}


async def _trpc(client: httpx.AsyncClient, procedure: str, payload: Dict[str, Any],
                superjson_meta: Optional[Dict[str, Any]] = None) -> Any:
    envelope: Dict[str, Any] = {"json": payload}
    if superjson_meta:
        envelope["meta"] = {"values": superjson_meta, "v": 1}
    response = await client.post(f"{CIVITAI_HOST}/api/trpc/{procedure}", headers=_headers(),
                                 json=envelope, timeout=60.0)
    data = response.json() if response.content else {}
    if response.status_code >= 400 or "error" in data:
        message = ((data.get("error") or {}).get("json") or {}).get("message") or f"HTTP {response.status_code}"
        raise RuntimeError(f"{procedure}: {str(message)[:300]}")
    return ((data.get("result") or {}).get("data") or {}).get("json")


async def _query(client: httpx.AsyncClient, procedure: str, payload: Dict[str, Any]) -> Any:
    response = await client.get(f"{CIVITAI_HOST}/api/trpc/{procedure}", headers=_headers(),
                                params={"input": json.dumps({"json": payload})}, timeout=30.0)
    if response.status_code >= 400:
        raise RuntimeError(f"{procedure}: HTTP {response.status_code}")
    return ((response.json().get("result") or {}).get("data") or {}).get("json")


# ---------------------------------------------------------------- what made it

def render_task(url: str) -> Dict[str, Any]:
    """The render ledger's request behind an /renderfin/render/<task>.<ext> URL."""
    match = re.search(r"/renderfin/render/[^/]+/([0-9a-f-]{36})\.", str(url or ""))
    if not match:
        return {}
    try:
        from ai_pipelines_api import renderfin_db_path
        database = renderfin_db_path()
    except Exception:
        database = None
    if not database:
        return {}
    try:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=5.0)
        try:
            row = connection.execute("SELECT payload FROM render_tasks WHERE id = ?",
                                     (match.group(1),)).fetchone()
        finally:
            connection.close()
        return json.loads(row[0]) if row else {}
    except Exception as error:
        logger.info("civitai: render ledger lookup failed: %s", error)
        return {}


def _catalogue() -> Dict[str, Dict[str, Any]]:
    entries: Dict[str, Dict[str, Any]] = {}
    try:
        data = json.loads(MODEL_CATALOGUE.read_text(encoding="utf-8"))
        for entry in data if isinstance(data, list) else (data.get("models") or []):
            if isinstance(entry, dict) and entry.get("file"):
                entries[str(entry["file"])] = dict(entry, kind=entry.get("kind") or "checkpoint")
    except Exception:
        pass
    try:
        import ai_lora_manager
        for entry in ai_lora_manager.catalogue_entries():
            entries.setdefault(str(entry.get("file")), dict(entry, kind="lora"))
    except Exception as error:
        logger.info("civitai: LoRA registry unavailable: %s", error)
    return entries


def _task_loras(request: Dict[str, Any]) -> List[Tuple[str, Optional[float]]]:
    found: List[Tuple[str, Optional[float]]] = []
    if request.get("lora"):
        found.append((str(request["lora"]), float(request.get("lora_strength") or 1.0)))
    for item in request.get("loras") or []:
        if isinstance(item, dict):
            name = item.get("file") or item.get("name") or item.get("lora")
            weight = item.get("strength", item.get("weight"))
            if name:
                found.append((str(name), float(weight) if weight is not None else None))
        elif isinstance(item, str):
            match = re.match(r"<lora:([^:>]+):?([-0-9.]*)>", item.strip())
            if match:
                found.append((match.group(1), float(match.group(2)) if match.group(2) else None))
            elif item.strip():
                found.append((item.strip(), None))
    for name, weight in re.findall(r"<lora:([^:>]+):([-0-9.]+)>", str(request.get("prompt") or "")):
        found.append((name, float(weight)))
    return found


def _resolve_file(name: str, catalogue: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    stem = re.sub(r"\.(safetensors|gguf|ckpt|pt)$", "", name)
    if name in KNOWN_VERSIONS or stem + ".safetensors" in KNOWN_VERSIONS:
        version, title, kind = KNOWN_VERSIONS.get(name) or KNOWN_VERSIONS[stem + ".safetensors"]
        return {"model_version_id": version, "name": title, "type": kind}
    entry = catalogue.get(name) or catalogue.get(stem + ".safetensors") or next(
        (item for key, item in catalogue.items() if re.sub(r"\.\w+$", "", key) == stem), None)
    if entry and entry.get("source_version_id"):
        return {"model_version_id": int(entry["source_version_id"]), "name": entry.get("title") or name,
                "type": "lora" if entry.get("kind") == "lora" else "checkpoint"}
    return None


def collect_resources(body_resources: List[CivitaiResource], request: Dict[str, Any],
                      generation: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Civitai versions (with weights) and the description lines for parts without a page."""
    catalogue = _catalogue()
    resources: List[Dict[str, Any]] = []
    notes: List[str] = []

    def add(item: Dict[str, Any]) -> None:
        if any(r["model_version_id"] == item["model_version_id"] for r in resources):
            return
        if len(resources) >= MAX_RESOURCES:
            return
        if item.get("type") == "checkpoint" and sum(r.get("type") == "checkpoint" for r in resources) >= MAX_CHECKPOINTS:
            return
        resources.append(item)

    for item in body_resources:
        add({"model_version_id": item.model_version_id, "name": item.name,
             "type": item.type or "checkpoint", "weight": item.weight})
    checkpoint = str(request.get("checkpoint") or generation.get("model") or "")
    if checkpoint:
        found = _resolve_file(checkpoint, catalogue)
        if found:
            add(dict(found, weight=None if found["type"] == "checkpoint" else 1.0))
        note = NO_PAGE_NOTES.get(checkpoint)
        if note:
            notes.append(note)
        elif not found and catalogue.get(checkpoint, {}).get("page"):
            entry = catalogue[checkpoint]
            notes.append('Generated with {0}: <a href="{1}">{1}</a>'.format(
                html.escape(entry.get("title") or checkpoint), html.escape(entry["page"])))
    for name, weight in _task_loras(request):
        found = _resolve_file(name, catalogue)
        if found:
            add(dict(found, type="lora", weight=weight if weight is not None else 1.0))
        elif catalogue.get(name, {}).get("page"):
            notes.append('LoRA {0} ({1}): <a href="{2}">{2}</a>'.format(
                html.escape(catalogue[name].get("title") or name), weight if weight is not None else 1.0,
                html.escape(catalogue[name]["page"])))
    return resources, notes


def build_meta(prompt: str, request: Dict[str, Any], generation: Dict[str, Any],
               resources: List[Dict[str, Any]], upscaled: bool, size: Tuple[int, int]) -> Dict[str, Any]:
    """The A1111-style generation block Civitai indexes and shows."""
    catalogue = _catalogue()
    checkpoint = str(request.get("checkpoint") or generation.get("model") or "")
    recommended = (catalogue.get(checkpoint) or {}).get("recommended") or {}
    meta: Dict[str, Any] = {}
    text = str(request.get("prompt") or prompt or "").strip()
    if text:
        meta["prompt"] = text
    if str(request.get("negative_prompt") or "").strip():
        meta["negativePrompt"] = str(request["negative_prompt"]).strip()

    def first(*values: Any) -> Any:
        return next((value for value in values if value not in (None, "", 0, 0.0)), None)

    seed = first(request.get("noise_seed"), generation.get("seed"))
    steps = first(request.get("steps"), generation.get("steps"), recommended.get("steps"))
    cfg = first(request.get("cfg"), generation.get("cfg"), recommended.get("cfg"))
    sampler = first(request.get("sampler"), generation.get("sampler"), recommended.get("sampler"))
    scheduler = first(request.get("scheduler"), recommended.get("scheduler"))
    if seed is not None:
        meta["seed"] = int(seed)
    if steps is not None:
        meta["steps"] = int(steps)
    if cfg is not None:
        meta["cfgScale"] = float(cfg)
    if sampler:
        meta["sampler"] = str(sampler)
    if scheduler:
        meta["Schedule type"] = str(scheduler)[:120]
    width = first(request.get("main_size_width"), generation.get("width"))
    height = first(request.get("main_size_height"), generation.get("height"))
    if width and height:
        meta["Size"] = f"{int(width)}x{int(height)}"
    elif size[0] and size[1]:
        meta["Size"] = f"{size[0]}x{size[1]}"
    model = (catalogue.get(checkpoint) or {}).get("title") or checkpoint
    if model:
        meta["Model"] = str(model)
    if request.get("creativity"):
        meta["Denoising strength"] = float(request["creativity"])
    loras = [r for r in resources if r.get("type") == "lora"]
    if loras:
        meta["Lora"] = ", ".join(f"{r['name']}:{r.get('weight') or 1.0}" for r in loras)
    if upscaled:
        meta["Hires upscale"] = 2
        meta["Hires upscaler"] = UPSCALER_NAME
    workflow = str(request.get("workflow_file") or request.get("workflow") or "")
    if workflow:
        meta["workflow"] = workflow.replace(".json", "")
    meta["software"] = "AutoRig nodes (ComfyUI) - autorig.online/nodes"
    if resources:
        meta["civitaiResources"] = [
            dict({"modelVersionId": int(r["model_version_id"]), "type": r.get("type") or "checkpoint"},
                 **({"weight": float(r["weight"])} if r.get("weight") is not None else {}))
            for r in resources]
    return meta


def source_requests(url: str, sources: List[str], kind: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """The render behind the posted file, or - for a file a tool made from renders
    (joined shots, muxed audio, a cut) - the newest upstream render of the same
    kind, plus every other upstream render for its models."""
    own = render_task(url)
    if own.get("prompt"):
        return own["prompt"], []
    found: List[Dict[str, Any]] = []
    seen = set()
    for source in sources[:60]:
        task = render_task(source)
        if task.get("prompt") and task.get("id") not in seen:
            seen.add(task.get("id"))
            found.append(task)
    if not found:
        return {}, []
    video_ext = (".mp4", ".webm", ".mov")
    same_kind = [t for t in found if str(t.get("output_url") or "").lower().endswith(video_ext) == (kind == "video")]
    primary = max(same_kind or found, key=lambda t: float(t.get("created_at") or 0))
    return primary["prompt"], [t["prompt"] for t in found if t is not primary]


async def ensure_meta(client: httpx.AsyncClient, image_id: int, meta: Dict[str, Any]) -> None:
    """Civitai keeps the meta sent with post.addImage, but a clip's scan can
    replace it with the file's own (empty) metadata. Read it back; write it
    again with post.updateImage when the prompt or the resources are gone."""
    if not meta.get("prompt") and not meta.get("civitaiResources"):
        return
    for attempt in range(3):
        try:
            data = await _query(client, "image.getGenerationData", {"id": image_id}) or {}
        except Exception:
            data = {}
        stored = data.get("meta") or {}
        if (not meta.get("prompt") or stored.get("prompt")) and \
                (not meta.get("civitaiResources") or stored.get("civitaiResources")):
            return
        try:
            await _trpc(client, "post.updateImage", {"id": image_id, "meta": meta})
        except Exception as error:
            logger.info("civitai updateImage %s: %s", image_id, error)
        await asyncio.sleep(3 * (attempt + 1))
    logger.warning("civitai image %s: meta still missing after retries", image_id)


def techniques_for(request: Dict[str, Any], kind: str) -> List[str]:
    kind_of = str(request.get("type") or "")
    workflow = str(request.get("workflow") or "") + " " + str(request.get("workflow_file") or "")
    has_input = bool(request.get("image_url") or request.get("reference_image_urls"))
    found: List[str] = []
    if kind == "video":
        found.append("vid2vid" if request.get("control_video_url") else
                     "img2vid" if has_input else "txt2vid")
    else:
        found.append("img2img" if has_input or "edit" in kind_of else "txt2img")
    if "control" in workflow or "depth" in kind_of or "pose" in kind_of or "canny" in kind_of \
            or "depth map" in str(request.get("prompt") or "").lower():
        found.append("controlnet")
    found.append("workflow")
    return found


# ---------------------------------------------------------------- tags

async def pick_tags(client: httpx.AsyncClient, candidates: List[str], limit: int = POST_TAG_LIMIT) -> List[str]:
    """Up to ``limit`` tags, names that already exist on Civitai first."""
    clean: List[str] = []
    for tag in candidates:
        value = re.sub(r"\s+", " ", str(tag).strip().lower().lstrip("#"))[:40].strip(" ,.")
        if value and value not in clean:
            clean.append(value)
    existing: List[str] = []
    fresh: List[str] = []
    for value in clean[:20]:
        try:
            items = (await _query(client, "tag.getAll", {"query": value, "limit": 5}) or {}).get("items") or []
            (existing if any(str(item.get("name")) == value for item in items) else fresh).append(value)
        except Exception:
            fresh.append(value)
    return (existing + fresh)[:limit]


async def _lookup_ids(client: httpx.AsyncClient, procedure: str, names: List[str]) -> List[int]:
    try:
        data = await _query(client, procedure, {})
        items = data.get("items") if isinstance(data, dict) else data
        wanted = [name.lower() for name in names]
        return [int(item["id"]) for item in items or [] if str(item.get("name", "")).lower() in wanted]
    except Exception as error:
        logger.info("civitai %s: %s", procedure, error)
        return []


async def attach_extras(client: httpx.AsyncClient, image_id: int, resources: List[Dict[str, Any]],
                        techniques: List[str]) -> Dict[str, Any]:
    """Resources the read-back lacks, then techniques and the ComfyUI tool."""
    report: Dict[str, Any] = {"resources_added": [], "errors": []}
    try:
        linked = {int(item.get("modelVersionId") or 0)
                  for item in (await _query(client, "image.getResources", {"id": image_id}) or [])}
    except Exception:
        linked = set()
    for item in resources:
        version = int(item["model_version_id"])
        if version in linked:
            continue
        try:
            await _trpc(client, "post.addResourceToImage", {"id": [image_id], "modelVersionId": version})
            report["resources_added"].append(version)
        except Exception as error:
            report["errors"].append(str(error)[:200])
    for procedure, lookup, key, names in (
            ("image.addTechniques", "technique.getAll", "techniqueId", techniques),
            ("image.addTools", "tool.getAll", "toolId", [COMFYUI_TOOL])):
        ids = await _lookup_ids(client, lookup, names)
        if ids:
            try:
                await _trpc(client, procedure, {"data": [{"imageId": image_id, key: i} for i in ids]})
            except Exception as error:
                report["errors"].append(str(error)[:200])
    try:
        report["resources"] = [int(item.get("modelVersionId") or 0) for item in
                               (await _query(client, "image.getResources", {"id": image_id}) or [])]
    except Exception:
        report["resources"] = []
    return report


def description_html(text: str, notes: List[str], upscaled: bool) -> str:
    parts = [f"<p>{html.escape(line)}</p>" for line in str(text or "").strip().split("\n") if line.strip()]
    parts += [f"<p>{note}</p>" for note in notes]
    if upscaled:
        parts.append(f"<p>Upscaled 2x with {UPSCALER_NAME}.</p>")
    parts.append('<p>Made with AutoRig nodes: <a href="https://autorig.online/nodes">autorig.online/nodes</a></p>')
    return "".join(parts)


# ---------------------------------------------------------------- posting

async def _mux_audio(client: httpx.AsyncClient, audio_url: str, poster_url: Optional[str]) -> Path:
    """Audio on a still (the poster, or black) -> mp4 Civitai accepts."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="civ-", dir=SCRATCH_DIR))
    audio = work / ("audio" + Path(audio_url.split("?", 1)[0]).suffix)
    audio.write_bytes((await client.get(audio_url, timeout=120.0, follow_redirects=True)).content)
    out = work / "music.mp4"
    if poster_url:
        still = work / "poster.png"
        still.write_bytes((await client.get(poster_url, timeout=60.0, follow_redirects=True)).content)
        video_in = ["-loop", "1", "-i", str(still)]
    else:
        video_in = ["-f", "lavfi", "-i", "color=c=black:s=1280x720:r=24"]
    cmd = ["ffmpeg", "-loglevel", "error", "-y", *video_in, "-i", str(audio), "-shortest",
           "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2", "-c:v", "libx264", "-pix_fmt", "yuv420p",
           "-c:a", "aac", "-b:a", "192k", str(out)]
    process = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.PIPE)
    _out, err = await process.communicate()
    if process.returncode:
        shutil.rmtree(work, ignore_errors=True)
        raise RuntimeError("ffmpeg: " + err.decode("utf-8", "replace")[-300:])
    return out


def _public_scratch(path: Path) -> str:
    """A muxed file the owner can download for the manual path."""
    public = Path("/srv/autorig/current/autorig-online/static/tmp-civitai")
    try:
        public.mkdir(parents=True, exist_ok=True)
        name = uuid.uuid4().hex + path.suffix
        shutil.copy(path, public / name)
        return "https://autorig.online/static/tmp-civitai/" + name
    except Exception:
        return ""


async def _probe_video(content: bytes) -> Dict[str, Any]:
    """width, height, duration (s) and audio of a clip, via ffprobe."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    handle, path = tempfile.mkstemp(suffix=".mp4", dir=SCRATCH_DIR)
    try:
        with os.fdopen(handle, "wb") as out:
            out.write(content)
        process = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "stream=codec_type,width,height:format=duration",
            "-of", "json", path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out_text, _err = await process.communicate()
        data = json.loads(out_text or b"{}")
    except Exception as error:
        logger.info("civitai: ffprobe failed: %s", error)
        return {}
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    streams = data.get("streams") or []
    video = next((item for item in streams if item.get("codec_type") == "video"), {})
    result: Dict[str, Any] = {"audio": any(item.get("codec_type") == "audio" for item in streams)}
    if video.get("width") and video.get("height"):
        result.update(width=int(video["width"]), height=int(video["height"]))
    try:
        result["duration"] = round(float((data.get("format") or {}).get("duration") or 0), 3)
    except ValueError:
        pass
    return result


async def _post_image(client: httpx.AsyncClient, body: CivitaiPostRequest,
                      local: Optional[Path] = None, source_url: str = "",
                      upscaled: bool = False, job: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if local is not None:
        content = local.read_bytes()
        name = local.name
        mime = "video/mp4"
    else:
        picture = await client.get(body.media_url, timeout=300.0, follow_redirects=True)
        picture.raise_for_status()
        content = picture.content
        name = Path(body.media_url.split("?", 1)[0]).name or "image.png"
        mime = picture.headers.get("content-type", "image/png").split(";")[0]
    is_video = mime.startswith("video/") or name.lower().endswith(VIDEO_SUFFIXES)
    width = height = 0
    try:
        from io import BytesIO
        from PIL import Image
        with Image.open(BytesIO(content)) as image:
            width, height = image.size
    except Exception:
        pass
    media_metadata: Dict[str, Any] = {"size": len(content)}
    if is_video:
        # Civitai's web client sends the clip's size and duration; without
        # them the video lands as 0x0 (measured 2026-09-27).
        probe = await _probe_video(content)
        width, height = probe.get("width", 0), probe.get("height", 0)
        media_metadata.update(probe)
    elif width and height:
        media_metadata.update(width=width, height=height)
    request, extra_requests = source_requests(source_url or body.media_url, body.source_urls,
                                              "video" if is_video else "image")
    resources, notes = collect_resources(body.resources, request, dict(body.generation or {}))
    for other in extra_requests:
        more, more_notes = collect_resources([], other, {})
        for item in more:
            if len(resources) < MAX_RESOURCES and all(r["model_version_id"] != item["model_version_id"] for r in resources):
                resources.append(item)
        notes += [note for note in more_notes if note not in notes]
    meta = build_meta(body.prompt, request, dict(body.generation or {}), resources, upscaled, (width, height))
    techniques = techniques_for(request, "video" if is_video else "image")

    upload = await client.post(f"{CIVITAI_HOST}/api/v1/image-upload", headers=_headers(),
                               json={"filename": name, "metadata": {}}, timeout=60.0)
    if upload.status_code >= 400:
        raise RuntimeError(f"image-upload: HTTP {upload.status_code}")
    ticket = upload.json()
    target = ticket.get("uploadURL") or ticket.get("uploadUrl")
    image_key = ticket.get("id")
    if not target or not image_key:
        raise RuntimeError("image-upload: no upload URL")
    # The ticket is a presigned object-storage URL: it takes a PUT of the raw
    # bytes (a multipart POST answered 501). A Cloudflare Images direct-upload
    # URL takes the multipart POST, so that is the second try.
    total = len(content)

    async def chunks():
        step = 1 << 20
        for offset in range(0, total, step):
            yield content[offset:offset + step]
            done = min(100, int((offset + step) * 100 / max(total, 1)))
            _job_update(job, progress_percent=done,
                        stage_label=f"Uploading to Civitai {done}% of {total / 1048576:.1f} MB")

    _job_update(job, stage="uploading", stage_label="Uploading to Civitai 0%", progress_percent=0)
    sent = await client.put(target, content=chunks(), timeout=600.0,
                            headers={"Content-Type": mime, "Content-Length": str(total)})
    if sent.status_code >= 400:
        retry = await client.post(target, files={"file": (name, content, mime)}, timeout=180.0)
        if retry.status_code >= 400:
            raise RuntimeError(f"upload: PUT HTTP {sent.status_code}, POST HTTP {retry.status_code}")
    _job_update(job, stage="posting", stage_label="Creating the post", progress_percent=None)
    post = await _trpc(client, "post.create", {})
    post_id = int(post["id"])
    _job_update(job, post_id_int=post_id)
    try:
        image = await _trpc(client, "post.addImage", {
            "postId": post_id, "url": image_key, "name": name, "width": width, "height": height,
            "hash": None, "meta": meta, "index": 0, "mimeType": mime, "metadata": media_metadata,
            "type": "video" if is_video else "image"})
        image_id = int((image or {}).get("id") or 0)
        extras = await attach_extras(client, image_id, resources, techniques) if image_id else {}
        if image_id:
            await ensure_meta(client, image_id, meta)
        tags = await pick_tags(client, list(body.tags) + ["autorig"])
        for tag in tags:
            try:
                await _trpc(client, "post.addTag", {"id": post_id, "name": tag})
            except Exception as error:  # a tag is not worth failing the post
                logger.info("civitai tag %s: %s", tag, error)
        update: Dict[str, Any] = {"id": post_id, "title": body.title.strip()[:TITLE_MAX] or None,
                                  "detail": description_html(body.description, notes, upscaled)}
        # A draft is a post.update WITHOUT publishedAt: null is refused and
        # the post stays behind as an orphan draft (2026-09-27).
        if body.publish:
            update["publishedAt"] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
            await _trpc(client, "post.update", update, superjson_meta={"publishedAt": ["Date"]})
        else:
            await _trpc(client, "post.update", update)
    except Exception:
        if not body.publish:  # never leave a half-built draft behind
            try:
                await _trpc(client, "post.delete", {"id": post_id})
            except Exception as error:
                logger.warning("civitai: could not remove failed draft %s: %s", post_id, error)
        raise
    try:
        read = await _query(client, "post.get", {"id": post_id}) or {}
        published = read.get("publishedAt")
    except Exception:
        published = "unknown"
    result = {"success_bool": True, "post_id_int": post_id, "image_id_int": image_id,
              "draft_bool": not body.publish and not published,
              "resources_array": (extras or {}).get("resources", []),
              "tags_array": tags, "techniques_array": techniques, "meta_keys_array": sorted(meta),
              "post_url_string": f"{CIVITAI_HOST}/posts/{post_id}" + ("" if body.publish else "/edit")}
    if not body.publish and published:
        logger.error("civitai post %s is public although a draft was asked for", post_id)
        result["warning_string"] = "Civitai published this post although a draft was asked for — check it now"
        result["post_url_string"] = f"{CIVITAI_HOST}/posts/{post_id}"
    if (extras or {}).get("errors"):
        result["extras_errors_array"] = extras["errors"]
    return result


LOCAL = os.getenv("AUTORIG_LOCAL_BASE", "http://127.0.0.1:8200").rstrip("/")
META_SYSTEM = (
    "You write Civitai post metadata for search. Answer with one JSON object only: "
    '{"title": "...", "description": "...", "tags": ["..."]}. '
    "title: a catchy, cinematic or game-like title (3-8 words) naming what happens in the picture, "
    "never a model name. description: 3-4 sentences describing subject, action, setting, lighting, "
    "camera and style, rich in searchable words. tags: 12 to 15 short lowercase Civitai tags "
    "(one to three words each, most important first): subject, character type, action, clothing, "
    "setting, lighting, style, medium and mood. English only.")


async def _ask(client: httpx.AsyncClient, path: str, body: Dict[str, Any], timeout: float = 420.0) -> str:
    """One Vision/Text request on this backend, answer text (polls the task)."""
    response = await client.post(LOCAL + path, json=dict(body, wait_seconds=60), timeout=90.0)
    data = response.json() if response.content else {}
    deadline = time.monotonic() + timeout
    while not data.get("answer_string") and time.monotonic() < deadline:
        task = data.get("task_id_string")
        if not task or data.get("error_string"):
            break
        await asyncio.sleep(3)
        data = (await client.get(f"{LOCAL}/api/ai/status/{task}", timeout=30.0)).json()
    return str(data.get("answer_string") or "")


async def generate_meta(body: "CivitaiMetaRequest") -> Dict[str, Any]:
    kind = _kind(body.media_url)
    caption = ""
    async with httpx.AsyncClient() as client:
        caption = body.caption.strip()
        if kind in ("image", "video") and not caption:
            try:
                caption = await _ask(client, "/api/vision", dict({
                    "prompt": "Describe this " + ("clip" if kind == "video" else "picture") +
                              " in 3-4 sentences: who, what happens, setting, lighting, camera and style.",
                    "image_url": body.media_url, "structured": True},
                    **({"video_mode": "storyboard"} if kind == "video" else {})))
            except Exception as error:
                logger.info("civitai meta caption failed: %s", error)
        if body.caption_only:
            return {"success_bool": bool(caption), "caption_string": caption, "title_string": "",
                    "description_string": caption, "tags_array": [], "tag_limit_int": POST_TAG_LIMIT}
        text = await _ask(client, "/api/text2text", {
            "system_prompt": META_SYSTEM, "structured": True,
            "prompt": ("Generation prompt: " + (body.prompt or "(none)") + "\n"
                       "What the output shows: " + (caption or "(no caption)") + "\n"
                       "Made with: " + (", ".join(body.resources) or body.service or "AutoRig nodes"))})
    match = re.search(r"\{.*\}", text, re.S)
    meta: Dict[str, Any] = {}
    if match:
        try:
            meta = json.loads(match.group(0))
        except Exception:
            meta = {}
    title = str(meta.get("title") or "").strip().strip('"')[:TITLE_MAX]
    description = str(meta.get("description") or caption or body.prompt).strip()
    tags: List[str] = []
    for tag in meta.get("tags") or []:
        value = str(tag).strip().lower().lstrip("#")[:40]
        if value and value not in tags:
            tags.append(value)
    return {"success_bool": bool(title), "title_string": title, "description_string": description,
            "tags_array": tags[:15], "tag_limit_int": POST_TAG_LIMIT, "caption_string": caption}


_BACKGROUND: set = set()


async def _auto_meta(body: CivitaiPostRequest, kind: str) -> CivitaiPostRequest:
    """Title/description/tags from the LLM when the dialog sent a placeholder title."""
    if not body.auto_meta or body.title.strip().lower() not in GENERIC_TITLES:
        return body
    try:
        meta = await generate_meta(CivitaiMetaRequest(
            media_url=body.media_url, prompt=body.prompt, service=kind,
            resources=[item.name for item in body.resources if item.name]))
    except Exception as error:
        logger.info("civitai auto meta failed: %s", error)
        return body
    if not meta.get("title_string"):
        return body
    generic_tags = {"autorig", "video", "image", kind}
    tags = [tag for tag in body.tags if tag.strip().lower() not in generic_tags]
    return body.model_copy(update={
        "title": meta["title_string"],
        "description": body.description if body.description.strip() else meta.get("description_string", ""),
        "tags": tags + list(meta.get("tags_array") or [])})


async def run_post(body: CivitaiPostRequest, job: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    kind = _kind(body.media_url)
    async with httpx.AsyncClient() as client:
        download = body.media_url
        source = body.media_url
        local = None
        # A node's address exists before its file does: posting a render that
        # is still queued or running answered a bare 404 (2026-09-27).
        try:
            probe = await client.head(body.media_url, timeout=20.0, follow_redirects=True)
            missing = probe.status_code == 404
        except Exception:
            missing = False
        if missing:
            state = ""
            match = re.search(r"/renderfin/render/[^/]+/([0-9a-f-]{36})\.", body.media_url)
            if match:
                try:
                    import ai_enhance_api
                    farm = (await client.get(f"{ai_enhance_api.RENDERFIN_BASE}/api-render/tasks/{match.group(1)}",
                                              timeout=10.0)).json()
                    status = str(farm.get("status_string") or farm.get("status") or "")
                    box = str(farm.get("render_server_name") or "")
                    state = status + (f" on {box}" if box else "")
                except Exception:
                    state = ""
            reason = ("This output is not ready yet" + (f" ({state})" if state else "") +
                      ": post it when the node has finished rendering")
            return {"success_bool": False, "not_ready_bool": True, "reason_string": reason}
        # The LLM writes (when the title is a placeholder) before the upload.
        meta_task = asyncio.ensure_future(_auto_meta(body, kind))
        if kind == "audio":
            try:
                local = await _mux_audio(client, body.media_url, body.poster_url)
                download = _public_scratch(local) or body.media_url
            except Exception as error:
                logger.warning("civitai audio mux failed: %s", error)
                meta_task.cancel()
                return _manual(body, "The music could not be put on a still: " + str(error)[:120], download)
        if not _token():
            meta_task.cancel()
            return _manual(body, "No Civitai token on the server", download)
        upscaled = ""
        if not meta_task.done():
            _job_update(job, stage="writing", stage_label="Writing title and tags", progress_percent=None)
        body = await meta_task
        _job_update(job, title_string=body.title)
        try:
            answer = await _post_image(client, body, local, source_url=source, upscaled=bool(upscaled), job=job)
            answer["upscaled_url_string"] = upscaled
            logger.info("civitai post %s done: %s", answer.get("post_id_int"), answer.get("post_url_string"))
            return answer
        except Exception as error:
            logger.warning("civitai post failed: %s", error)
            return _manual(body, "Civitai did not accept the automatic post (" + str(error)[:200] +
                           "); finish it by hand", download)


def build_civitai_post_router(require_admin) -> APIRouter:
    router = APIRouter()

    @router.post("/api/ai/civitai/post")
    async def api_civitai_post(body: CivitaiPostRequest, _admin=Depends(require_admin)):
        if body.nsfw_level not in NSFW_LEVELS:
            raise HTTPException(status_code=400, detail={
                "error_string": "rating_required",
                "message_string": "Confirm the rating: None, Soft, Mature or X"})
        if not body.media_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail={
                "error_string": "media_required", "message_string": "Nothing to post"})
        kind = _kind(body.media_url)
        if body.dry_run:
            request = render_task(body.media_url).get("prompt") or {}
            resources, notes = collect_resources(body.resources, request, dict(body.generation or {}))
            return {"success_bool": True, "dry_run_bool": True, "kind_string": kind, "host_string": CIVITAI_HOST,
                    "token_present_bool": bool(_token()), "resources_array": resources, "notes_array": notes,
                    "meta": build_meta(body.prompt, request, dict(body.generation or {}), resources,
                                       False, (0, 0)),
                    "techniques_array": techniques_for(request, kind),
                    "steps_array": (["POST /api/v1/image-upload", "PUT <uploadURL> (file)", "trpc post.create",
                                     "trpc post.addImage (meta + civitaiResources)",
                                     "trpc post.addResourceToImage (missing)", "trpc image.addTechniques/addTools",
                                     f"trpc post.addTag x<={POST_TAG_LIMIT}",
                                     "trpc post.update" + (" publishedAt" if body.publish else " (draft)")]
                                    if kind != "audio" else ["mux audio onto a still -> mp4", "then as a clip"])}
        if body.background:
            job = {"id": uuid.uuid4().hex[:12], "created_at": time.time(), "stage": "starting",
                   "stage_label": "Starting", "kind_string": kind, "media_url_string": body.media_url,
                   "title_string": body.title, "publish_bool": body.publish,
                   "box_string": "", "progress_percent": None}
            JOBS[job["id"]] = job
            _job_save(job)

            async def runner() -> None:
                try:
                    answer = await run_post(body, job)
                except Exception as error:  # run_post answers failures itself; this is the last guard
                    logger.exception("civitai job %s crashed", job["id"])
                    answer = {"success_bool": False, "reason_string": str(error)[:300]}
                done = bool(answer.get("success_bool"))
                _job_update(job, result=answer, finished_at=time.time(), progress_percent=None,
                            stage="done" if done else ("manual" if answer.get("manual_bool") else "failed"),
                            stage_label=("Done" if done else
                                         "Failed: " + str(answer.get("reason_string") or "")[:200]))

            task = asyncio.get_running_loop().create_task(runner())
            _BACKGROUND.add(task)
            task.add_done_callback(_BACKGROUND.discard)
            return {"success_bool": True, "job_id_string": job["id"], "job": _public_job(job)}
        return await run_post(body, None)

    @router.get("/api/ai/civitai/jobs")
    async def api_civitai_jobs(_admin=Depends(require_admin)):
        cutoff = time.time() - 24 * 3600
        jobs = [_public_job(job) for job in JOBS.values() if job.get("created_at", 0) >= cutoff]
        jobs.sort(key=lambda job: job.get("created_at", 0), reverse=True)
        return {"success_bool": True, "jobs_array": jobs[:30],
                "active_int": sum(1 for job in jobs if job["active_bool"])}

    @router.get("/api/ai/civitai/jobs/{job_id}")
    async def api_civitai_job(job_id: str, _admin=Depends(require_admin)):
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail={"error_string": "job_not_found"})
        return {"success_bool": True, "job": _public_job(job)}

    @router.post("/api/ai/civitai/meta")
    async def api_civitai_meta(body: CivitaiMetaRequest, _admin=Depends(require_admin)):
        return await generate_meta(body)

    @router.post("/api/ai/civitai/post/{post_id}/delete")
    async def api_civitai_delete(post_id: int, _admin=Depends(require_admin)):
        async with httpx.AsyncClient() as client:
            await _trpc(client, "post.delete", {"id": int(post_id)})
        return {"success_bool": True, "post_id_int": int(post_id)}

    return router

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
# Civitai's own ratings and their nsfwLevel bit (src/server/common/enums.ts).
# The old dialog names are still accepted: None=PG, Soft=PG-13, Mature=R.
RATING_LEVELS = {"PG": 1, "PG-13": 2, "R": 4, "X": 8, "XXX": 16}
RATING_ALIASES = {"None": "PG", "Soft": "PG-13", "Mature": "R", "PG13": "PG-13"}
NSFW_LEVELS = tuple(RATING_LEVELS) + tuple(RATING_ALIASES)
LEVEL_NAMES = {value: key for key, value in RATING_LEVELS.items()}


def rating_level(name: str) -> int:
    return RATING_LEVELS.get(RATING_ALIASES.get(name, name), 0)
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
MAX_POST_FILES = 20           # files in one post (Civitai's post editor)
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


class CivitaiPostItem(BaseModel):
    media_url: str = Field(..., max_length=4096)
    source_urls: List[str] = Field(default_factory=list, max_length=60)


class CivitaiPostRequest(BaseModel):
    media_url: str = Field(..., max_length=4096)
    title: str = Field("", max_length=300)
    description: str = Field("", max_length=12000)
    prompt: str = Field("", max_length=12000)
    tags: List[str] = Field(default_factory=list, max_length=40)
    nsfw_level: str = Field(..., description="PG, PG-13, R, X or XXX; the owner confirms it")
    resources: List[CivitaiResource] = Field(default_factory=list, max_length=20)
    publish: bool = False
    dry_run: bool = False
    poster_url: Optional[str] = Field(None, max_length=4096, description="Still for audio")
    # Removed 2026-09-27 (owner): the pre-post upscale cropped a portrait
    # clip. Posts send the output file as it is; enlarge with the Upscale 2x
    # node in the graph first. The field is accepted and ignored.
    upscale: bool = Field(False, description="Ignored: posts use the file as it is")
    generation: Dict[str, Any] = Field(default_factory=dict, description="seed, steps, sampler, cfg, model...")
    extra_items: List[CivitaiPostItem] = Field(default_factory=list, max_length=MAX_POST_FILES - 1,
                                               description="More files for the same post (one post, up to 20 files)")
    title_is_placeholder: bool = Field(False, description="The dialog still shows the node's label")
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


# ---------------------------------------------------------------- daily limit
# Civitai counts every post.create against a daily quota (20/40 base, 60/120
# at score >= 1000, 150/300 at >= 5000; the second figure for paid members) -
# drafts, deleted posts and failed attempts included. It exposes no counter,
# so the server keeps its own log of creates and, once Civitai answers "daily
# limit", refuses to call post.create again until the window has passed.

LIMIT_FILE = SCRATCH_DIR / "limits.json"
RESUME_FILE = SCRATCH_DIR / "resume.json"
DAILY_TIERS = "20 a day (40 for members); 60/120 at a score of 1000; 150/300 at 5000"


def _read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, value: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(value), encoding="utf-8")
        tmp.replace(path)
    except Exception as error:
        logger.info("civitai: could not write %s: %s", path, error)


def _limits() -> Dict[str, Any]:
    data = _read_json(LIMIT_FILE, {})
    if "creates" not in data:
        # First run: seed the log from the post jobs this server already made.
        creates = [float(job.get("created_at") or 0) for job in JOBS.values() if job.get("post_id_int")]
        data = {"creates": sorted(t for t in creates if t), "limited_until": 0}
    return data


def limit_state() -> Dict[str, Any]:
    data = _limits()
    now = time.time()
    recent = [t for t in data.get("creates", []) if now - t < 86400]
    until = float(data.get("limited_until") or 0)
    limited = until > now
    return {"limited_bool": limited, "retry_at_unix_float": until if limited else 0,
            "retry_in_hours_float": round((until - now) / 3600, 1) if limited else 0,
            "created_last_24h_int": len(recent), "tiers_string": DAILY_TIERS,
            "message_string": (f"Civitai daily post limit reached - try again in ~{max(1, round((until - now) / 3600))} h"
                               if limited else "")}


def _note_create() -> None:
    data = _limits()
    now = time.time()
    data["creates"] = [t for t in data.get("creates", []) if now - t < 86400] + [now]
    _write_json(LIMIT_FILE, data)


def _note_limit_hit() -> None:
    data = _limits()
    now = time.time()
    recent = sorted(t for t in data.get("creates", []) if now - t < 86400)
    # The window is a day from the calls it counted; the oldest one we know of
    # frees first. Without any, wait a full day.
    until = (recent[0] + 86400) if recent else now + 86400
    data["limited_until"] = max(until, now + 3600)
    _write_json(LIMIT_FILE, data)


class DailyLimitError(RuntimeError):
    pass


def _resume_key(urls: List[str]) -> str:
    import hashlib
    return hashlib.sha1("\n".join(sorted(urls)).encode("utf-8")).hexdigest()[:20]


# ---------------------------------------------------------------- one file

async def prepare_item(client: httpx.AsyncClient, url: str, body: CivitaiPostRequest, sources: List[str],
                       upscaled: bool, local: Optional[Path] = None) -> Dict[str, Any]:
    """Download one file and build its meta, resources and techniques (no Civitai call)."""
    if local is not None:
        content, name, mime = local.read_bytes(), local.name, "video/mp4"
    else:
        picture = await client.get(url, timeout=300.0, follow_redirects=True)
        picture.raise_for_status()
        content = picture.content
        name = Path(url.split("?", 1)[0]).name or "image.png"
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
    request, extra_requests = source_requests(url, sources, "video" if is_video else "image")
    resources, notes = collect_resources(body.resources, request, dict(body.generation or {}))
    for other in extra_requests:
        more, more_notes = collect_resources([], other, {})
        for item in more:
            if len(resources) < MAX_RESOURCES and all(r["model_version_id"] != item["model_version_id"] for r in resources):
                resources.append(item)
        notes += [note for note in more_notes if note not in notes]
    meta = build_meta(body.prompt, request, dict(body.generation or {}), resources, upscaled, (width, height))
    return {"url": url, "content": content, "name": name, "mime": mime, "is_video": is_video,
            "width": width, "height": height, "media_metadata": media_metadata, "meta": meta,
            "resources": resources, "notes": notes,
            "techniques": techniques_for(request, "video" if is_video else "image")}


async def upload_item(client: httpx.AsyncClient, item: Dict[str, Any], job: Optional[Dict[str, Any]],
                      label: str) -> str:
    """Put one file in Civitai's storage; answers the key post.addImage takes."""
    upload = await client.post(f"{CIVITAI_HOST}/api/v1/image-upload", headers=_headers(),
                               json={"filename": item["name"], "metadata": {}}, timeout=60.0)
    if upload.status_code >= 400:
        raise RuntimeError(f"image-upload: HTTP {upload.status_code}")
    ticket = upload.json()
    target = ticket.get("uploadURL") or ticket.get("uploadUrl")
    image_key = ticket.get("id")
    if not target or not image_key:
        raise RuntimeError("image-upload: no upload URL")
    content, mime = item["content"], item["mime"]
    total = len(content)

    async def chunks():
        step = 1 << 20
        for offset in range(0, total, step):
            yield content[offset:offset + step]
            done = min(100, int((offset + step) * 100 / max(total, 1)))
            _job_update(job, progress_percent=done,
                        stage_label=f"Uploading{label} to Civitai {done}% of {total / 1048576:.1f} MB")

    _job_update(job, stage="uploading", stage_label=f"Uploading{label} to Civitai 0%", progress_percent=0)
    # A presigned object-storage URL takes a PUT of the raw bytes; a Cloudflare
    # Images direct-upload URL takes the multipart POST, so that is the second try.
    sent = await client.put(target, content=chunks(), timeout=600.0,
                            headers={"Content-Type": mime, "Content-Length": str(total)})
    if sent.status_code >= 400:
        retry = await client.post(target, files={"file": (item["name"], content, mime)}, timeout=180.0)
        if retry.status_code >= 400:
            raise RuntimeError(f"upload: PUT HTTP {sent.status_code}, POST HTTP {retry.status_code}")
    return str(image_key)


async def _post_image(client: httpx.AsyncClient, body: CivitaiPostRequest,
                      local: Optional[Path] = None, source_url: str = "",
                      upscaled: bool = False, job: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Upload every file first; create the post only when all uploads went through.

    One post takes up to 20 files (the main output plus body.extra_items).
    Nothing is created when anything fails before post.create. A failure after
    it keeps the post and its id; the next attempt with the same files reuses
    it instead of creating another (every create counts against Civitai's
    daily quota, deleted drafts included)."""
    entries = [{"media_url": source_url or body.media_url, "fetch_url": body.media_url,
                "source_urls": body.source_urls}]
    for extra in body.extra_items[:MAX_POST_FILES - 1]:
        entries.append({"media_url": extra.media_url, "fetch_url": extra.media_url,
                        "source_urls": extra.source_urls or body.source_urls})
    items: List[Dict[str, Any]] = []
    for index, entry in enumerate(entries):
        _job_update(job, stage="preparing", stage_label=f"Reading file {index + 1} of {len(entries)}")
        item = await prepare_item(client, entry["fetch_url"], body, entry["source_urls"], upscaled,
                                  local if index == 0 else None)
        item["source_url"] = entry["media_url"]
        items.append(item)
    for index, item in enumerate(items):
        label = f" {index + 1}/{len(items)}" if len(items) > 1 else ""
        item["key"] = await upload_item(client, item, job, label)

    resume_key = _resume_key([item["source_url"] for item in items])
    resume = _read_json(RESUME_FILE, {})
    record = resume.get(resume_key) or {}
    post_id = 0
    if record.get("post_id"):
        try:
            existing = await _query(client, "post.get", {"id": int(record["post_id"])}) or {}
            post_id = int(existing.get("id") or 0)
        except Exception:
            post_id = 0
        if post_id:
            _job_update(job, stage="posting", stage_label=f"Continuing post {post_id}")
    if not post_id:
        state = limit_state()
        if state["limited_bool"]:
            raise DailyLimitError(state["message_string"])
        _job_update(job, stage="posting", stage_label="Creating the post", progress_percent=None)
        try:
            post = await _trpc(client, "post.create", {})
        except RuntimeError as error:
            if "daily limit" in str(error).lower() or "posting limit" in str(error).lower():
                _note_limit_hit()
                raise DailyLimitError(limit_state()["message_string"] or str(error)) from None
            raise
        _note_create()
        post_id = int(post["id"])
        record = {"post_id": post_id, "added": {}, "created_at": time.time()}
        resume[resume_key] = record
        _write_json(RESUME_FILE, resume)
    _job_update(job, post_id_int=post_id)

    added: Dict[str, int] = dict(record.get("added") or {})
    image_ids: List[int] = []
    extras_errors: List[str] = []
    for index, item in enumerate(items):
        image_id = int(added.get(item["source_url"]) or 0)
        if not image_id:
            _job_update(job, stage="posting", stage_label=f"Adding file {index + 1} of {len(items)}")
            image = await _trpc(client, "post.addImage", {
                "postId": post_id, "url": item["key"], "name": item["name"], "width": item["width"],
                "height": item["height"], "hash": None, "meta": item["meta"], "index": index,
                "mimeType": item["mime"], "metadata": item["media_metadata"],
                "type": "video" if item["is_video"] else "image"})
            image_id = int((image or {}).get("id") or 0)
            added[item["source_url"]] = image_id
            record["added"] = added
            resume[resume_key] = record
            _write_json(RESUME_FILE, resume)
        image_ids.append(image_id)
        if image_id:
            extras = await attach_extras(client, image_id, item["resources"], item["techniques"])
            extras_errors += extras.get("errors") or []
            item["linked"] = extras.get("resources", [])
            await ensure_meta(client, image_id, item["meta"])

    tags = await pick_tags(client, list(body.tags) + ["autorig"])
    for tag in tags:
        try:
            await _trpc(client, "post.addTag", {"id": post_id, "name": tag})
        except Exception as error:  # a tag is not worth failing the post
            logger.info("civitai tag %s: %s", tag, error)
    notes: List[str] = []
    for item in items:
        notes += [note for note in item["notes"] if note not in notes]
    update: Dict[str, Any] = {"id": post_id, "title": body.title.strip()[:TITLE_MAX] or None,
                              "detail": description_html(body.description, notes, upscaled)}
    # A draft is a post.update WITHOUT publishedAt: null is refused and the
    # post stays behind as an orphan draft (2026-09-27).
    if body.publish:
        update["publishedAt"] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        await _trpc(client, "post.update", update, superjson_meta={"publishedAt": ["Date"]})
    else:
        await _trpc(client, "post.update", update)
    resume.pop(resume_key, None)
    _write_json(RESUME_FILE, resume)

    try:
        read = await _query(client, "post.get", {"id": post_id}) or {}
        published = read.get("publishedAt")
    except Exception:
        published = "unknown"
    first = items[0]
    result = {"success_bool": True, "post_id_int": post_id, "image_id_int": image_ids[0] if image_ids else 0,
              "image_ids_array": image_ids, "files_int": len(items),
              "draft_bool": not body.publish and not published,
              "resources_array": first.get("linked", []),
              "tags_array": tags, "techniques_array": first["techniques"], "meta_keys_array": sorted(first["meta"]),
              "post_url_string": f"{CIVITAI_HOST}/posts/{post_id}" + ("" if body.publish else "/edit")}
    if not body.publish and published:
        logger.error("civitai post %s is public although a draft was asked for", post_id)
        result["warning_string"] = "Civitai published this post although a draft was asked for — check it now"
        result["post_url_string"] = f"{CIVITAI_HOST}/posts/{post_id}"
    if extras_errors:
        result["extras_errors_array"] = extras_errors
    ratings = [await apply_rating(client, image_id, body.nsfw_level) for image_id in image_ids if image_id]
    if ratings:
        result.update(ratings[0])
        infos = sorted({r["rating_info_string"] for r in ratings if r.get("rating_info_string")})
        if infos:
            result["rating_info_string"] = "; ".join(infos)
    return result


async def _image_level(client: httpx.AsyncClient, image_id: int) -> Tuple[int, bool]:
    response = await client.get(f"{CIVITAI_HOST}/api/trpc/image.get", headers=_headers(),
                                params={"input": json.dumps({"json": {"id": image_id}})}, timeout=30.0)
    data = ((response.json().get("result") or {}).get("data") or {}).get("json") or {}
    return int(data.get("nsfwLevel") or 0), bool(data.get("nsfwLevelLocked"))


async def apply_rating(client: httpx.AsyncClient, image_id: int, name: str) -> Dict[str, Any]:
    """Settle the rating with Civitai's scanner, never against it.

    The scanner's level comes first. Higher than the owner's choice: it is
    adopted as it is (an owner request below the scanner only sits in review)
    and reported as a plain note. Lower: the owner's rating is sent
    (image.updateImageNsfwLevel), which can only raise it. Not scanned within
    ~18 s: nothing is sent."""
    wanted = rating_level(name)
    answer: Dict[str, Any] = {"rating_requested_string": LEVEL_NAMES.get(wanted, name)}
    if not wanted:
        return answer
    level = 0
    for _attempt in range(6):
        try:
            level, _locked = await _image_level(client, image_id)
        except Exception:
            level = 0
        if level:
            break
        await asyncio.sleep(3)
    if not level:
        # Not scanned yet (or not readable): nothing to compare, nothing sent.
        return answer
    if level >= wanted:
        answer["rating_on_civitai_string"] = LEVEL_NAMES.get(level, str(level))
        if level > wanted:
            answer["rating_info_string"] = f"Rated {LEVEL_NAMES.get(level, level)} by Civitai's scanner"
        return answer
    try:
        await _trpc(client, "image.updateImageNsfwLevel", {"id": image_id, "nsfwLevel": wanted})
    except Exception as error:
        logger.info("civitai rating %s on %s: %s", wanted, image_id, error)
    answer["rating_on_civitai_string"] = LEVEL_NAMES.get(wanted)
    return answer


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


# The farm's uncensored text/vision model: reasoning off, so an explicit clip
# never ends as "the whole budget went to reasoning" with no answer - which is
# what left post 31272978 with its node label as the title (2026-09-27: the
# default 27B spent 2048 tokens thinking; Vision had refused the .mp4 sent as
# image_url).
META_MODEL = os.getenv("CIVITAI_META_MODEL", "qwen35-9b-uncensored")
_STOP = {"a", "an", "the", "this", "that", "image", "picture", "clip", "video", "shows", "show", "showing",
         "is", "are", "of", "in", "on", "with", "and", "at", "while", "her", "his", "their", "its", "it"}


def fallback_title(*texts: str) -> str:
    """A short title made locally from the caption or prompt: never a node label."""
    for text in texts:
        words = re.findall(r"[A-Za-z][A-Za-z'-]+", str(text or ""))
        keep = [w for w in words if w.lower() not in _STOP][:6]
        if len(keep) >= 2:
            return " ".join(w.capitalize() for w in keep)[:TITLE_MAX]
    return ""


def fallback_tags(*texts: str) -> List[str]:
    words: List[str] = []
    for text in texts:
        for w in re.findall(r"[A-Za-z][A-Za-z-]{3,}", str(text or "").lower()):
            if w not in _STOP and w not in words:
                words.append(w)
    return words[:15]


def _parse_meta(text: str) -> Dict[str, Any]:
    match = re.search(r"\{.*\}", text or "", re.S)
    if not match:
        return {}
    try:
        data = json.loads(match.group(0))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


async def generate_meta(body: "CivitaiMetaRequest") -> Dict[str, Any]:
    kind = _kind(body.media_url)
    prompt = body.prompt.strip() or str((render_task(body.media_url).get("prompt") or {}).get("prompt") or "")
    async with httpx.AsyncClient() as client:
        caption = body.caption.strip()
        if kind in ("image", "video") and not caption:
            ask = {"prompt": "Describe this " + ("clip" if kind == "video" else "picture") +
                             " in 3-4 sentences: who, what happens, setting, lighting, camera and style.",
                   "structured": True, "model": META_MODEL}
            # A clip goes in as video_url: sent as image_url, Vision refused it.
            ask.update({"video_url": body.media_url, "video_mode": "storyboard"} if kind == "video"
                       else {"image_url": body.media_url})
            for attempt in range(2):
                try:
                    caption = await _ask(client, "/api/vision", ask)
                except Exception as error:
                    logger.info("civitai meta caption failed: %s", error)
                if caption:
                    break
                ask.pop("model", None)  # second try: the farm's default model
        if body.caption_only:
            return {"success_bool": bool(caption), "caption_string": caption, "title_string": "",
                    "description_string": caption, "tags_array": [], "tag_limit_int": POST_TAG_LIMIT}
        source = ("Generation prompt: " + (prompt[:2500] or "(none)") + "\n"
                  "What the output shows: " + (caption or "(no caption)") + "\n"
                  "Made with: " + (", ".join(body.resources) or body.service or "AutoRig nodes"))
        meta: Dict[str, Any] = {}
        tries = [
            {"system_prompt": META_SYSTEM, "structured": True, "prompt": source, "model": META_MODEL},
            {"system_prompt": META_SYSTEM, "structured": True, "prompt": source, "model": META_MODEL},
            {"prompt": 'Answer with JSON only: {"title": "3-8 word cinematic title", "description": '
                       '"2 sentences", "tags": ["12 short lowercase tags"]}.\n' + source,
             "structured": True, "max_output_tokens": -1},
        ]
        for attempt, ask in enumerate(tries):
            try:
                meta = _parse_meta(await _ask(client, "/api/text2text", ask))
            except Exception as error:
                logger.info("civitai meta text try %d failed: %s", attempt + 1, error)
                meta = {}
            if str(meta.get("title") or "").strip():
                break
            logger.info("civitai meta: no title on try %d for %s", attempt + 1, body.media_url)
    title = str(meta.get("title") or "").strip().strip('"')[:TITLE_MAX]
    generated = bool(title)
    if not title:
        title = fallback_title(caption, prompt)
    description = str(meta.get("description") or caption or prompt).strip()
    tags: List[str] = []
    for tag in (meta.get("tags") or []) or fallback_tags(caption, prompt):
        value = str(tag).strip().lower().lstrip("#")[:40]
        if value and value not in tags:
            tags.append(value)
    return {"success_bool": bool(title), "generated_bool": generated, "title_string": title,
            "description_string": description, "tags_array": tags[:15], "tag_limit_int": POST_TAG_LIMIT,
            "caption_string": caption}


_BACKGROUND: set = set()


async def _auto_meta(body: CivitaiPostRequest, kind: str) -> CivitaiPostRequest:
    """Title/description/tags from the LLM when the dialog sent a placeholder title."""
    placeholder = body.title.strip().lower() in GENERIC_TITLES or body.title_is_placeholder
    if not body.auto_meta or not placeholder:
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
        except DailyLimitError as error:
            return {"success_bool": False, "limited_bool": True, "reason_string": str(error),
                    "limit": limit_state()}
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
                "message_string": "Confirm the rating: PG, PG-13, R, X or XXX"})
        if not body.media_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail={
                "error_string": "media_required", "message_string": "Nothing to post"})
        kind = _kind(body.media_url)
        state = limit_state()
        if state["limited_bool"] and not body.dry_run:
            return {"success_bool": False, "limited_bool": True, "reason_string": state["message_string"],
                    "limit": state}
        if body.dry_run:
            # Builds every request and sends none to Civitai: files are read
            # and described here, nothing is uploaded or created.
            async with httpx.AsyncClient() as client:
                planned = []
                urls = [body.media_url] + [extra.media_url for extra in body.extra_items[:MAX_POST_FILES - 1]]
                sources = [body.source_urls] + [extra.source_urls or body.source_urls
                                                for extra in body.extra_items[:MAX_POST_FILES - 1]]
                for index, (url, src) in enumerate(zip(urls, sources)):
                    try:
                        item = await prepare_item(client, url, body, src, False)
                        planned.append({"index_int": index, "media_url_string": url, "kind_string":
                                        "video" if item["is_video"] else "image",
                                        "size_string": f"{item['width']}x{item['height']}",
                                        "meta": item["meta"], "resources_array": item["resources"],
                                        "notes_array": item["notes"], "techniques_array": item["techniques"],
                                        "metadata": item["media_metadata"]})
                    except Exception as error:
                        planned.append({"index_int": index, "media_url_string": url, "error_string": str(error)[:200]})
            return {"success_bool": True, "dry_run_bool": True, "host_string": CIVITAI_HOST,
                    "token_present_bool": bool(_token()), "files_array": planned, "limit": state,
                    "rating_level_int": rating_level(body.nsfw_level),
                    "steps_array": [f"image-upload + PUT x{len(planned)} (all files first)",
                                    "post.create x1 (only after every upload went through)",
                                    f"post.addImage x{len(planned)} (meta + civitaiResources)",
                                    "post.addResourceToImage (missing), image.addTechniques/addTools",
                                    f"post.addTag x<={POST_TAG_LIMIT}",
                                    "post.update" + (" publishedAt" if body.publish else " (draft)"),
                                    "rating: scanner level read back; owner rating only when higher"]}
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

    @router.get("/api/ai/civitai/limit")
    async def api_civitai_limit(_admin=Depends(require_admin)):
        return dict(limit_state(), success_bool=True)

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

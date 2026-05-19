from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from fastapi import Depends, HTTPException, Query, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from idle_ltx_vision import (
    IDLE_LTX_DEFAULT_NEGATIVE_PROMPT,
    IDLE_LTX_FRAME_COUNT_DEFAULT,
    IDLE_LTX_USER_PROMPT_DEFAULT,
    IDLE_LTX_VARIANT_COUNT,
    VisionPromptAnalyzer,
)


RENDERFIN_ANIMATION_UPLOAD_URL = "https://free3d.online/renderfin/api/animation/upload_image"
RENDERFIN_GENERATE_VIDEO_URL = "https://free3d.online/renderfin/api/generate_video"
RENDERFIN_ANIMATION_TASK_STATUS_URL = "https://free3d.online/renderfin/api/animation/task_status"
RENDERFIN_API_RENDER_GET_TASK_BY_URL = "https://free3d.online/api-render-get-task-by-url"
IDLE_LTX_STATIC_LORA_WORKFLOW = "gen_animation_by_url_ltx_19b_static_lora.json"
IDLE_LTX_REFERENCE_STORE_ROOT = Path(__file__).resolve().parent.parent / "static" / "tasks"
IDLE_LTX_REFERENCE_FILE_NAME = "idle_ltx_references.json"

IDLE_LTX_VARIANT_KEYS = ("idle", "walk", "run", "die")
IDLE_LTX_STATIC_CAMERA_SENTENCE = (
    "Single locked-off tripod shot. Static frame. Fixed viewpoint. The camera is bolted down and never moves. "
    "No push-in, no pull-back, no dolly in, no dolly out, no zoom, no pan, no tilt, no orbit, no tracking, "
    "no handheld shake, no reframing. The distance between camera and subject never changes. "
    "The entire frame, including the selected theme backdrop, stays pixel-locked with zero parallax."
)
IDLE_LTX_FIRST_FRAME_SENTENCE = (
    "Use the provided first frame as the visual source and preserve its subject, environment, props, lighting, "
    "materials, body pose, silhouette, framing, and background layout."
)
IDLE_LTX_NO_TEXT_SENTENCE = (
    "Do not add random letters, alphabet walls, unreadable text, fake signage, posters, watermarks, logos, "
    "or any typography that is not already visible in the provided first frame."
)
IDLE_LTX_CAMERA_LOCK_NEGATIVE = (
    "camera movement, moving camera, orbit camera, rotating camera, camera pan, camera tilt, camera zoom, "
    "dolly shot, tracking shot, handheld camera, camera shake, viewpoint change, reframing, dynamic camera, "
    "cinematic camera move, push in, pull out, push-in, pull-back, dolly in, dolly out, moving closer, "
    "moving away, camera forward movement, camera backward movement, changing camera distance, parallax shift, "
    "background drift, lens zoom, rack focus, random letters, alphabet wall, unreadable text, fake signage, posters"
)


def _idle_ltx_status_word_to_int(word: Optional[str]) -> int:
    x = str(word or "").strip().lower()
    if x in ("completed", "complete", "done", "success", "succeeded"):
        return 3
    if x in ("failed", "error", "cancelled", "canceled"):
        return 4
    if x in ("pending", "queued", "accepting", "waiting"):
        return 1
    if x in ("inprogress", "in_progress", "processing", "running", "working"):
        return 2
    return 2


def _idle_ltx_normalize_url_task_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    st = entry.get("status_int")
    if st is None:
        st = entry.get("status")
    try:
        status_int = int(st)
    except Exception:
        status_int = _idle_ltx_status_word_to_int(
            str(entry.get("status_string") or entry.get("status") or entry.get("phase_string") or "")
        )
    out = str(entry.get("output_url_string") or entry.get("output_url") or "").strip()
    playback = str(entry.get("playback_url_string") or entry.get("playback_url") or "").strip()
    err = str(entry.get("error_string") or entry.get("error") or entry.get("message") or "").strip()
    rsn = str(entry.get("render_server_name_string") or entry.get("render_server_name") or "").strip()
    pid = str(entry.get("prompt_id_string") or entry.get("prompt_id") or "").strip()
    phase = "completed" if status_int == 3 else ("failed" if status_int == 4 else "pending_or_processing")
    play_url = playback or out
    return {
        "status_int": status_int,
        "phase_string": phase,
        "output_url_string": out or None,
        "playback_url_string": playback or None,
        "video_url_string": play_url or None,
        "error_string": err or None,
        "render_server_name_string": rsn or None,
        "prompt_id_string": pid or None,
        "raw_object": entry,
    }


async def _idle_ltx_fetch_task_by_output_url(client: httpx.AsyncClient, output_url: str) -> Optional[List[Any]]:
    if not output_url.startswith("https://"):
        return None
    url = f"{RENDERFIN_API_RENDER_GET_TASK_BY_URL}?url={quote(output_url, safe='')}"
    resp = await client.get(url, timeout=45.0, follow_redirects=True)
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return None


async def _idle_ltx_clip_status_resolve(
    client: httpx.AsyncClient,
    *,
    output_url_string: Optional[str],
    renderfin_task_id: Optional[str],
) -> Dict[str, Any]:
    parsed_url: Optional[Dict[str, Any]] = None
    ou = (output_url_string or "").strip()
    if ou.startswith("https://"):
        rows = await _idle_ltx_fetch_task_by_output_url(client, ou)
        if rows and isinstance(rows[0], dict):
            parsed_url = _idle_ltx_normalize_url_task_entry(rows[0])
        elif rows == []:
            parsed_url = {
                "status_int": 2,
                "phase_string": "pending_or_processing",
                "output_url_string": ou,
                "playback_url_string": None,
                "video_url_string": None,
                "error_string": None,
                "render_server_name_string": None,
                "prompt_id_string": None,
                "raw_object": [],
            }

    if parsed_url and int(parsed_url.get("status_int") or 0) in (3, 4):
        return {"success_bool": True, "source_string": "output_url", **parsed_url}

    tid = (renderfin_task_id or "").strip()
    if tid and re.match(r"^[0-9a-fA-F-]{32,36}$", tid):
        status_url = f"{RENDERFIN_ANIMATION_TASK_STATUS_URL}?task_ids_string={quote(tid)}"
        resp = await client.get(status_url, timeout=45.0, follow_redirects=True)
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception:
                data = {}
            items = data.get("items_array") or data.get("items") or []
            entry: Dict[str, Any] = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else {}
            status_int = entry.get("status_int")
            if status_int is None:
                status_int = entry.get("status")
            try:
                status_int = int(status_int)
            except Exception:
                status_int = -1
            out = str(entry.get("output_url_string") or entry.get("output_url") or "").strip()
            playback = str(entry.get("playback_url_string") or entry.get("playback_url") or "").strip()
            err = str(entry.get("error_string") or entry.get("error") or "").strip()
            phase = "pending_or_processing" if status_int in (0, 1, 2) else ("completed" if status_int == 3 else ("failed" if status_int == 4 else "unknown"))
            return {
                "success_bool": True,
                "source_string": "task_id",
                "status_int": status_int,
                "phase_string": phase,
                "output_url_string": out or None,
                "playback_url_string": playback or None,
                "video_url_string": playback or out or None,
                "error_string": err or None,
                "render_server_name_string": None,
                "prompt_id_string": None,
                "renderfin_raw_object": data,
            }

    if parsed_url:
        return {"success_bool": True, "source_string": "output_url", **parsed_url}

    if ou or tid:
        return {
            "success_bool": True,
            "source_string": "pending_fallback",
            "status_int": 2,
            "phase_string": "pending_or_processing",
            "output_url_string": ou or None,
            "playback_url_string": None,
            "video_url_string": ou or None,
            "error_string": "Renderfin status is temporarily unavailable; retrying.",
            "render_server_name_string": None,
            "prompt_id_string": None,
            "raw_object": None,
        }

    raise HTTPException(status_code=502, detail="Unable to resolve clip status")


def _idle_ltx_normalize_jpeg_base64(value: str) -> bytes:
    raw = (value or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="frame_jpeg_base64_string is required")
    if raw.startswith("data:image/"):
        _, _, b64 = raw.partition(",")
        if not b64:
            raise HTTPException(status_code=400, detail="Invalid image data URL")
    else:
        b64 = raw
    try:
        decoded = base64.b64decode(b64, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="frame_jpeg_base64_string must be valid base64")
    if not decoded:
        raise HTTPException(status_code=400, detail="frame_jpeg_base64_string is empty")
    if len(decoded) > 3_500_000:
        raise HTTPException(status_code=413, detail="Idle LTX frame image is too large")
    if not decoded.startswith(b"\xff\xd8"):
        raise HTTPException(status_code=400, detail="Idle LTX frame must be JPEG")
    return decoded


async def _idle_ltx_renderfin_upload_jpeg(*, user_name: str, image_bytes: bytes) -> Dict[str, Any]:
    safe_user = re.sub(r"[^a-zA-Z0-9_\-]", "_", (user_name or "autorig_user").strip())[:64] or "autorig_user"
    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.post(
                RENDERFIN_ANIMATION_UPLOAD_URL,
                data={"user_name_string": safe_user},
                files={"file": ("idle_frame.jpg", image_bytes, "image/jpeg")},
            )
        body_text = resp.text
        try:
            data = resp.json()
        except Exception:
            data = {"raw_string": body_text[:2000]}
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Renderfin upload_image failed: HTTP {resp.status_code} {body_text[:500]}",
            )
        if not isinstance(data, dict):
            raise HTTPException(status_code=502, detail="Renderfin upload_image returned non-JSON object")
        return {"ok_bool": True, "data_object": data, "http_status_int": resp.status_code}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Renderfin upload_image error: {e}") from e


async def _idle_ltx_post_one_generate_video(
    client: httpx.AsyncClient,
    generate_body: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], int]:
    body = dict(generate_body)
    resp = await client.post(
        RENDERFIN_GENERATE_VIDEO_URL,
        headers={"Content-Type": "application/json"},
        json=body,
    )
    try:
        data = resp.json()
    except Exception:
        data = {"raw_string": resp.text[:2500]}
    return body, data, resp.status_code


def _idle_ltx_user_slug_for_task(task_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", f"autorig_{task_id}")[:56] or "autorig"


def _idle_ltx_reference_file_path(task_id: str) -> Path:
    if not re.match(r"^[0-9a-fA-F-]{8,80}$", str(task_id or "")):
        raise HTTPException(status_code=400, detail="Invalid task_id")
    root = IDLE_LTX_REFERENCE_STORE_ROOT.resolve()
    path = (root / task_id / IDLE_LTX_REFERENCE_FILE_NAME).resolve()
    if root not in path.parents:
        raise HTTPException(status_code=400, detail="Invalid task_id path")
    return path


def _idle_ltx_load_reference_store(task_id: str) -> Dict[str, Any]:
    path = _idle_ltx_reference_file_path(task_id)
    if not path.is_file():
        return {"task_id_string": task_id, "clips_array": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"task_id_string": task_id, "clips_array": []}
    if not isinstance(data, dict):
        return {"task_id_string": task_id, "clips_array": []}
    clips = data.get("clips_array")
    if not isinstance(clips, list):
        data["clips_array"] = []
    data["task_id_string"] = task_id
    return data


def _idle_ltx_save_reference_store(task_id: str, data: Dict[str, Any]) -> None:
    path = _idle_ltx_reference_file_path(task_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **(data if isinstance(data, dict) else {}),
        "task_id_string": task_id,
        "updated_at_unix_float": time.time(),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _idle_ltx_delete_reference_store(task_id: str) -> None:
    path = _idle_ltx_reference_file_path(task_id)
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _idle_ltx_reference_sort_key(item: Dict[str, Any]) -> int:
    try:
        return int(item.get("index_int", 99))
    except Exception:
        return 99


def _idle_ltx_upsert_reference_clip(task_id: str, clip: Dict[str, Any]) -> None:
    if not isinstance(clip, dict):
        return
    try:
        idx = int(clip.get("index_int"))
    except Exception:
        return
    if idx < 0 or idx >= IDLE_LTX_VARIANT_COUNT:
        return
    store = _idle_ltx_load_reference_store(task_id)
    clips: List[Dict[str, Any]] = []
    for existing in store.get("clips_array", []):
        if not isinstance(existing, dict):
            continue
        try:
            existing_idx = int(existing.get("index_int", -1))
        except Exception:
            continue
        if existing_idx != idx:
            clips.append(existing)
    row = {
        "index_int": idx,
        "variant_name_string": str(clip.get("variant_name_string") or IDLE_LTX_VARIANT_KEYS[idx])[:80],
        "detected_species_string": str(clip.get("detected_species_string") or "")[:120],
        "species_confidence_float": clip.get("species_confidence_float"),
        "prompt_string": str(clip.get("prompt_string") or "")[:2400],
        "negative_prompt_string": str(clip.get("negative_prompt_string") or "")[:2400],
        "user_variant_prompt_string": str(clip.get("user_variant_prompt_string") or "")[:700],
        "renderfin_task_id_string": str(clip.get("renderfin_task_id_string") or "").strip(),
        "output_url_string": str(clip.get("output_url_string") or "").strip() or None,
        "video_url_string": str(clip.get("video_url_string") or clip.get("output_url_string") or "").strip() or None,
        "workflow_string": IDLE_LTX_STATIC_LORA_WORKFLOW,
        "width_int": 768,
        "height_int": 448,
        "frame_count_int": IDLE_LTX_FRAME_COUNT_DEFAULT,
        "saved_at_unix_float": time.time(),
    }
    clips.append(row)
    clips.sort(key=_idle_ltx_reference_sort_key)
    store["clips_array"] = clips[:IDLE_LTX_VARIANT_COUNT]
    _idle_ltx_save_reference_store(task_id, store)


def _idle_ltx_with_hard_camera_lock(prompt: str) -> str:
    body = str(prompt or "").strip()
    if not body:
        return f"{IDLE_LTX_FIRST_FRAME_SENTENCE} {IDLE_LTX_NO_TEXT_SENTENCE} {IDLE_LTX_STATIC_CAMERA_SENTENCE}"
    pieces: List[str] = []
    if IDLE_LTX_FIRST_FRAME_SENTENCE.lower() not in body.lower():
        pieces.append(IDLE_LTX_FIRST_FRAME_SENTENCE)
    if IDLE_LTX_NO_TEXT_SENTENCE.lower() not in body.lower():
        pieces.append(IDLE_LTX_NO_TEXT_SENTENCE)
    if IDLE_LTX_STATIC_CAMERA_SENTENCE.lower() not in body.lower():
        pieces.append(IDLE_LTX_STATIC_CAMERA_SENTENCE)
    pieces.append(body)
    return " ".join(pieces)


def _idle_ltx_merge_negative_prompt(value: str) -> str:
    neg = str(value or "").strip() or IDLE_LTX_DEFAULT_NEGATIVE_PROMPT
    merged = f"{neg}, {IDLE_LTX_DEFAULT_NEGATIVE_PROMPT}, {IDLE_LTX_CAMERA_LOCK_NEGATIVE}"
    parts: List[str] = []
    seen = set()
    for raw in merged.split(","):
        item = raw.strip()
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            parts.append(item)
    return ", ".join(parts)[:2400]


def _idle_ltx_theme_context_from_body(body: Dict[str, Any]) -> Dict[str, Any]:
    raw = body.get("theme_context_object")
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in ("theme_name", "theme_short_description", "theme_id", "background_src"):
        val = str(raw.get(key) or "").strip()
        if val:
            out[key] = val[:240]
    tags = raw.get("semantic_tags")
    if isinstance(tags, list):
        out["semantic_tags"] = [str(x).strip()[:60] for x in tags if str(x).strip()][:12]
    return out


def _idle_ltx_validate_task_video_url(task_id: str, video_url: str) -> str:
    url = str(video_url or "").strip()
    if not url.startswith("https://free3d.online/render/"):
        raise HTTPException(status_code=400, detail="Only AutoRig Renderfin MP4 URLs are supported")
    expected_prefix = f"https://free3d.online/render/autorig_{task_id}/"
    if not url.startswith(expected_prefix):
        raise HTTPException(status_code=400, detail="Video URL does not belong to this task")
    if not re.search(r"/[0-9a-fA-F-]{32,36}\.mp4(?:\?.*)?$", url):
        raise HTTPException(status_code=400, detail="Expected Renderfin MP4 URL")
    return url


def _idle_ltx_clean_variant_prompt(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())[:700]


def _idle_ltx_variant_prompts_from_body(body: Dict[str, Any]) -> Dict[str, str]:
    prompts: Dict[str, str] = {}
    raw_obj = body.get("variant_prompts_object")
    if isinstance(raw_obj, dict):
        for key in IDLE_LTX_VARIANT_KEYS:
            cleaned = _idle_ltx_clean_variant_prompt(raw_obj.get(key) or raw_obj.get(f"{key}_prompt_string"))
            if cleaned:
                prompts[key] = cleaned

    raw_array = body.get("variant_prompts_array")
    if isinstance(raw_array, list):
        for i, item in enumerate(raw_array):
            if not isinstance(item, dict):
                continue
            key = str(item.get("variant_name_string") or item.get("key_string") or "").strip().lower()
            if not key and i < len(IDLE_LTX_VARIANT_KEYS):
                key = IDLE_LTX_VARIANT_KEYS[i]
            if key not in IDLE_LTX_VARIANT_KEYS:
                continue
            cleaned = _idle_ltx_clean_variant_prompt(
                item.get("user_prompt_string") or item.get("prompt_string") or item.get("text_string")
            )
            if cleaned:
                prompts[key] = cleaned
    return prompts


def _idle_ltx_build_user_prompt(
    base_prompt: str,
    variant_prompts: Dict[str, str],
    theme_context: Optional[Dict[str, Any]] = None,
) -> str:
    base = (base_prompt or "").strip() or IDLE_LTX_USER_PROMPT_DEFAULT
    theme_context = theme_context or {}
    lines = [
        base,
        "",
        "The uploaded image is the first frame and the visual source for image-to-video. The final LTX prompt must describe that exact first-frame scene, then add only the requested motion.",
        "Preserve all visible subject identity, environment, props, lighting, shadows, material appearance, framing, and background layout from the first frame.",
        "",
        "Backdrop/theme context from the current 3D viewer must be preserved as a fixed static plate:",
    ]
    if theme_context.get("theme_name"):
        lines.append(f"theme: {theme_context['theme_name']}")
    if theme_context.get("theme_short_description"):
        lines.append(f"description: {theme_context['theme_short_description']}")
    if theme_context.get("semantic_tags"):
        lines.append(f"tags: {', '.join(theme_context['semantic_tags'])}")
    lines.extend([
        "The full input frame includes both the model and the selected theme background. Do not crop, push toward, pull away from, or reframe around the model.",
        "Do not invent new signage, alphabet walls, posters, random letters, logos, or unrelated background objects. If a real prop is visible in the uploaded first frame, describe it normally.",
        "",
        "User editable variant prompts. These are mandatory motion intents only, not full scene prompts:",
    ])
    for key in IDLE_LTX_VARIANT_KEYS:
        if variant_prompts.get(key):
            lines.append(f"{key}: {variant_prompts[key]}")
    lines.extend([
        "",
        IDLE_LTX_STATIC_CAMERA_SENTENCE,
        "Do not invent camera movement. Keep every video locked to the exact same viewpoint as the input frame.",
    ])
    return "\n".join(lines)[:4000]


def _idle_ltx_apply_variant_prompts_to_vision(
    vision_json: Dict[str, Any],
    variant_prompts: Dict[str, str],
) -> Dict[str, Any]:
    variants = vision_json.get("ltx_variants_array")
    if not isinstance(variants, list):
        variants = []
    out: List[Dict[str, Any]] = []
    base = str(vision_json.get("ltx_base_prompt_string") or "").strip()
    for i, key in enumerate(IDLE_LTX_VARIANT_KEYS):
        row = variants[i] if i < len(variants) and isinstance(variants[i], dict) else {}
        prompt = str(row.get("prompt_string") or base or "").strip()
        user_part = variant_prompts.get(key, "")
        if user_part and user_part.lower() not in prompt.lower():
            prompt = f"{prompt} Motion intent: {user_part}."
        out.append({
            **row,
            "variant_name_string": key,
            "prompt_string": _idle_ltx_with_hard_camera_lock(prompt)[:2400],
            "user_variant_prompt_string": user_part,
        })
    vision_json["ltx_variants_array"] = out
    vision_json["ltx_base_prompt_string"] = _idle_ltx_with_hard_camera_lock(base)[:2400]
    return vision_json


async def _idle_ltx_vision_upload_and_analyze(
    *,
    task_id: str,
    db: AsyncSession,
    body: Dict[str, Any],
    get_task_by_id: Callable[[AsyncSession, str], Any],
    app_url: str,
) -> Dict[str, Any]:
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if str(getattr(task, "input_type", "") or "").strip().lower() != "animal":
        raise HTTPException(status_code=400, detail="Idle LTX is only available for animal tasks")

    image_bytes = _idle_ltx_normalize_jpeg_base64(str(body.get("frame_jpeg_base64_string") or ""))
    variant_prompts = _idle_ltx_variant_prompts_from_body(body)
    theme_context = _idle_ltx_theme_context_from_body(body)
    user_prompt = _idle_ltx_build_user_prompt(
        str(body.get("user_prompt_string") or body.get("base_prompt_string") or "").strip(),
        variant_prompts,
        theme_context,
    )
    user_slug = _idle_ltx_user_slug_for_task(task_id)

    upload_wrap = await _idle_ltx_renderfin_upload_jpeg(user_name=user_slug, image_bytes=image_bytes)
    upload_obj = upload_wrap.get("data_object") or {}
    image_url = str(upload_obj.get("image_url_string") or upload_obj.get("image_url") or "").strip()
    if not image_url or not image_url.startswith("https://"):
        raise HTTPException(status_code=502, detail=f"Renderfin upload_image did not return image_url: {upload_obj}")

    cfg = {
        "open_ai_api_key": os.getenv("OPENAI_API_KEY", "").strip(),
        "open_AI_api_key": os.getenv("OPENAI_API_KEY", "").strip(),
        "open_ai_api_url_string": os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions").strip(),
        "open_ai_idle_ltx_vision_model_string": os.getenv("OPENAI_IDLE_LTX_VISION_MODEL", "gpt-4o-mini").strip(),
        "open_router_api_key": os.getenv("OPENROUTER_API_KEY", "").strip(),
        "open_router_api_url_string": os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions").strip(),
        "open_router_idle_ltx_vision_model_string": os.getenv("OPENROUTER_IDLE_LTX_VISION_MODEL", "openai/gpt-4o-mini").strip(),
    }
    vision_json, provider = await VisionPromptAnalyzer.analyze(
        cfg=cfg,
        app_url=app_url,
        image_url_string=image_url,
        user_prompt_string=user_prompt,
    )
    vision_json = _idle_ltx_apply_variant_prompts_to_vision(vision_json, variant_prompts)
    neg = _idle_ltx_merge_negative_prompt(str(vision_json.get("ltx_negative_prompt_string") or ""))
    fc = int(body.get("frame_count_int") or IDLE_LTX_FRAME_COUNT_DEFAULT)
    fc = max(1, min(375, fc))
    return {
        "vision_analysis_object": vision_json,
        "vision_provider_string": provider,
        "user_prompt_string": user_prompt,
        "variant_prompts_object": variant_prompts,
        "theme_context_object": theme_context,
        "image_url_string": image_url,
        "user_name_string": user_slug,
        "upload_response_object": upload_obj,
        "negative_prompt_string": neg,
        "frame_count_int": fc,
    }


async def _ensure_animal_task(db: AsyncSession, task_id: str, get_task_by_id: Callable[[AsyncSession, str], Any]) -> Any:
    task = await get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if str(getattr(task, "input_type", "") or "").strip().lower() != "animal":
        raise HTTPException(status_code=400, detail="Idle LTX is only available for animal tasks")
    return task


def register_idle_ltx_routes(
    app: Any,
    limiter: Any,
    *,
    get_db: Callable[..., Any],
    get_current_user: Callable[..., Any],
    get_task_by_id: Callable[[AsyncSession, str], Any],
    app_url: str,
) -> None:
    @app.post("/api/task/{task_id}/idle-ltx/vision-start")
    @limiter.limit("20/minute")
    async def api_task_idle_ltx_vision_start(
        request: Request,
        task_id: str,
        db: AsyncSession = Depends(get_db),
        user: Any = Depends(get_current_user),
    ):
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="JSON body must be an object")
        phase = await _idle_ltx_vision_upload_and_analyze(
            task_id=task_id,
            db=db,
            body=body,
            get_task_by_id=get_task_by_id,
            app_url=app_url,
        )
        try:
            from telegram_bot import broadcast_ltx_video_generation_started

            theme_context = phase.get("theme_context_object") or {}
            asyncio.create_task(broadcast_ltx_video_generation_started(
                task_id=task_id,
                user_email=getattr(user, "email", None),
                theme_name=str(theme_context.get("theme_name") or ""),
                background_hint=str(theme_context.get("theme_short_description") or ""),
                variant_count=IDLE_LTX_VARIANT_COUNT,
            ))
        except Exception as e:
            print(f"[Telegram] LTX generation notification enqueue failed: {e}")
        return {"success_bool": True, **phase}

    @app.post("/api/task/{task_id}/idle-ltx/render-variant")
    @limiter.limit("40/minute")
    async def api_task_idle_ltx_render_variant(
        request: Request,
        task_id: str,
        db: AsyncSession = Depends(get_db),
    ):
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="JSON body must be an object")

        await _ensure_animal_task(db, task_id, get_task_by_id)
        try:
            idx = int(body.get("index_int"))
        except Exception:
            raise HTTPException(status_code=400, detail="index_int is required (0..3)")
        if idx < 0 or idx >= IDLE_LTX_VARIANT_COUNT:
            raise HTTPException(status_code=400, detail="index_int must be 0..3")

        image_url = str(body.get("image_url_string") or "").strip()
        if not image_url.startswith("https://"):
            raise HTTPException(status_code=400, detail="image_url_string must be https")
        if task_id not in image_url:
            raise HTTPException(status_code=400, detail="image_url_string must contain this task_id")

        user_slug = str(body.get("user_name_string") or "").strip()
        if user_slug != _idle_ltx_user_slug_for_task(task_id):
            raise HTTPException(status_code=400, detail="user_name_string does not match this task")

        prompt_clip = _idle_ltx_with_hard_camera_lock(str(body.get("prompt_string") or "").strip())[:2400]
        if not prompt_clip:
            raise HTTPException(status_code=400, detail="prompt_string is required")
        neg_base = _idle_ltx_merge_negative_prompt(str(body.get("negative_prompt_string") or "").strip())
        variant_name = str(body.get("variant_name_string") or IDLE_LTX_VARIANT_KEYS[idx]).strip()
        frame_count = int(body.get("frame_count_int") or IDLE_LTX_FRAME_COUNT_DEFAULT)
        frame_count = max(1, min(375, frame_count))

        generate_body: Dict[str, Any] = {
            "prompt": prompt_clip,
            "image_url": image_url,
            "main_size_width": 768,
            "main_size_height": 448,
            "frame_count": frame_count,
            "user_name": user_slug,
            "work_flow": IDLE_LTX_STATIC_LORA_WORKFLOW,
            "negative_prompt": neg_base,
        }
        try:
            async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
                request_body, gen_obj, gen_http_status = await _idle_ltx_post_one_generate_video(client, generate_body)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Renderfin generate_video error: {e}") from e

        if gen_http_status != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Renderfin generate_video failed: HTTP {gen_http_status} {str(gen_obj)[:600]}",
            )
        renderfin_task_id = str(gen_obj.get("task_id") or gen_obj.get("task_id_string") or "").strip()
        output_url = str(gen_obj.get("output_url") or gen_obj.get("output_url_string") or "").strip()
        if not renderfin_task_id:
            raise HTTPException(status_code=502, detail="Renderfin did not return task_id")

        clip_object = {
            "index_int": idx,
            "variant_name_string": variant_name,
            "detected_species_string": body.get("detected_species_string"),
            "species_confidence_float": body.get("species_confidence_float"),
            "prompt_string": prompt_clip,
            "negative_prompt_string": neg_base,
            "user_variant_prompt_string": str(body.get("user_variant_prompt_string") or "").strip()[:700],
            "renderfin_task_id_string": renderfin_task_id,
            "output_url_string": output_url or None,
            "generate_video_request_object": request_body,
            "generate_video_response_object": gen_obj,
            "generate_video_http_status_int": gen_http_status,
        }
        try:
            _idle_ltx_upsert_reference_clip(task_id, clip_object)
        except Exception as e:
            print(f"[IdleLTX] failed to persist reference clip task={task_id} index={idx}: {e}")

        return {
            "success_bool": True,
            "index_int": idx,
            "clip_object": clip_object,
        }

    @app.get("/api/task/{task_id}/idle-ltx/references")
    @limiter.limit("80/minute")
    async def api_task_idle_ltx_references(
        request: Request,
        task_id: str,
        db: AsyncSession = Depends(get_db),
    ):
        await _ensure_animal_task(db, task_id, get_task_by_id)
        store = _idle_ltx_load_reference_store(task_id)
        clips = [row for row in store.get("clips_array", []) if isinstance(row, dict)]
        if not clips:
            return {"success_bool": True, "task_id_string": task_id, "clips_array": []}

        updated: List[Dict[str, Any]] = []
        try:
            async with httpx.AsyncClient(timeout=50.0, follow_redirects=True) as client:
                for row in clips[:IDLE_LTX_VARIANT_COUNT]:
                    merged = dict(row)
                    try:
                        status_obj = await _idle_ltx_clip_status_resolve(
                            client,
                            output_url_string=str(row.get("output_url_string") or ""),
                            renderfin_task_id=str(row.get("renderfin_task_id_string") or ""),
                        )
                        merged.update({
                            "status_int": status_obj.get("status_int"),
                            "phase_string": status_obj.get("phase_string"),
                            "output_url_string": status_obj.get("output_url_string") or row.get("output_url_string"),
                            "playback_url_string": status_obj.get("playback_url_string"),
                            "video_url_string": status_obj.get("video_url_string") or row.get("video_url_string"),
                            "error_string": status_obj.get("error_string"),
                        })
                    except Exception as e:
                        merged["error_string"] = str(e)[:500]
                    updated.append(merged)
        except Exception:
            updated = clips[:IDLE_LTX_VARIANT_COUNT]

        updated.sort(key=_idle_ltx_reference_sort_key)
        store["clips_array"] = updated
        try:
            _idle_ltx_save_reference_store(task_id, store)
        except Exception as e:
            print(f"[IdleLTX] failed to update reference store task={task_id}: {e}")
        return {"success_bool": True, "task_id_string": task_id, "clips_array": updated}

    @app.delete("/api/task/{task_id}/idle-ltx/references")
    @limiter.limit("20/minute")
    async def api_task_idle_ltx_delete_references(
        request: Request,
        task_id: str,
        db: AsyncSession = Depends(get_db),
    ):
        await _ensure_animal_task(db, task_id, get_task_by_id)
        _idle_ltx_delete_reference_store(task_id)
        return {"success_bool": True, "task_id_string": task_id}

    @app.get("/api/task/{task_id}/idle-ltx/clip-status")
    @limiter.limit("120/minute")
    async def api_task_idle_ltx_clip_status(
        request: Request,
        task_id: str,
        renderfin_task_id: Optional[str] = Query(None, description="Renderfin task UUID"),
        output_url_string: Optional[str] = Query(None, description="Output URL from generate_video"),
        db: AsyncSession = Depends(get_db),
    ):
        await _ensure_animal_task(db, task_id, get_task_by_id)
        if not (renderfin_task_id or "").strip() and not (output_url_string or "").strip():
            raise HTTPException(status_code=400, detail="Provide renderfin_task_id and/or output_url_string")
        try:
            async with httpx.AsyncClient(timeout=50.0, follow_redirects=True) as client:
                return await _idle_ltx_clip_status_resolve(
                    client,
                    output_url_string=output_url_string,
                    renderfin_task_id=renderfin_task_id,
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Idle LTX clip-status error: {e}") from e

    @app.get("/api/task/{task_id}/idle-ltx/verify-mp4")
    @limiter.limit("120/minute")
    async def api_task_idle_ltx_verify_mp4(
        request: Request,
        task_id: str,
        video_url_string: str = Query(..., description="Final mp4 or playback URL"),
        db: AsyncSession = Depends(get_db),
    ):
        await _ensure_animal_task(db, task_id, get_task_by_id)
        url = (video_url_string or "").strip()
        if not (url.startswith("https://") or url.startswith("http://")):
            raise HTTPException(status_code=400, detail="video_url_string must be http(s)")
        try:
            async with httpx.AsyncClient(timeout=35.0, follow_redirects=True) as client:
                head = await client.head(url)
                resp = head if head.status_code < 400 else await client.get(url, headers={"Range": "bytes=0-1023"})
            content_type = (resp.headers.get("content-type") or "").lower()
            ok = resp.status_code in (200, 206) and (
                "video" in content_type or "octet-stream" in content_type or url.lower().split("?", 1)[0].endswith(".mp4")
            )
            return {
                "success_bool": True,
                "ok_bool": ok,
                "http_status_int": resp.status_code,
                "content_type_string": content_type,
            }
        except Exception as e:
            return {
                "success_bool": True,
                "ok_bool": False,
                "http_status_int": 0,
                "content_type_string": "",
                "error_string": str(e)[:500],
            }

    @app.get("/api/task/{task_id}/idle-ltx/video-proxy")
    @limiter.limit("120/minute")
    async def api_task_idle_ltx_video_proxy(
        request: Request,
        task_id: str,
        video_url_string: str = Query(..., description="Renderfin MP4 URL"),
        db: AsyncSession = Depends(get_db),
    ):
        await _ensure_animal_task(db, task_id, get_task_by_id)
        url = _idle_ltx_validate_task_video_url(task_id, video_url_string)
        headers: Dict[str, str] = {}
        range_header = request.headers.get("range")
        if range_header:
            headers["Range"] = range_header
        try:
            async with httpx.AsyncClient(timeout=90.0, follow_redirects=True) as client:
                resp = await client.get(url, headers=headers)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Video proxy fetch failed: {e}") from e
        if resp.status_code not in (200, 206):
            raise HTTPException(status_code=resp.status_code, detail="Video file is not ready")
        content_type = resp.headers.get("content-type") or "video/mp4"
        out_headers = {
            "Accept-Ranges": resp.headers.get("accept-ranges", "bytes"),
            "Cache-Control": "no-store",
        }
        for key in ("content-length", "content-range"):
            value = resp.headers.get(key)
            if value:
                out_headers[key.title()] = value
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=content_type,
            headers=out_headers,
        )

    @app.post("/api/task/{task_id}/idle-ltx/fitting-started")
    @limiter.limit("60/minute")
    async def api_task_idle_ltx_fitting_started(
        request: Request,
        task_id: str,
        db: AsyncSession = Depends(get_db),
        user: Any = Depends(get_current_user),
    ):
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="JSON body must be an object")
        await _ensure_animal_task(db, task_id, get_task_by_id)
        variant_name = str(body.get("variant_name_string") or "").strip()[:80]
        video_url = str(body.get("video_url_string") or "").strip()
        if video_url and not (video_url.startswith("https://") or video_url.startswith("http://")):
            raise HTTPException(status_code=400, detail="video_url_string must be http(s)")
        try:
            from telegram_bot import broadcast_animation_fitting_started

            asyncio.create_task(broadcast_animation_fitting_started(
                task_id=task_id,
                variant_name=variant_name or "selected reference",
                video_url=video_url,
                user_email=getattr(user, "email", None),
            ))
        except Exception as e:
            print(f"[Telegram] animation fitting notification enqueue failed: {e}")
        return {
            "success_bool": True,
            "task_id_string": task_id,
            "variant_name_string": variant_name,
        }

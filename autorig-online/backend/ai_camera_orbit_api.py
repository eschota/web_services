"""Camera orbit: see the same subject from another camera position (2026-09-27).

Two endpoints, two tools chosen after a survey of the 2026 open models:

* ``POST /api/camera-orbit/image`` — one picture (a clip gives its first
  frame) re-shot from a new camera position by Qwen-Image 2.1 turbo, the farm's
  only edit model. Measured 27.09: it follows camera language natively
  (profile, back view, three-quarter, high angle) in 30-50 s on f5/f15/Raptor,
  so no extra weights are needed. The request names a preset or a
  yaw/pitch/zoom triple and this module writes the camera prompt.
  (The published multi-angle LoRAs are for Qwen-Image-Edit 2509/2511, a
  different architecture; they do not load on 2.1.)

* ``POST /api/camera-orbit/video`` — a clip re-rendered from a new camera
  position by LTX-2.5 distilled with the CrossView-Prompt IC-LoRA
  (Cseti, Apache-2.0, trained on LTX-2.3; runs on 2.5). The source clip is the
  IC reference, no start frame; the camera is a fixed vocabulary of azimuth x
  elevation x distance (63 prompts). The LoRA is weaker on distilled models, so
  it loads at 1.5. Frontal sector only (about +-60 deg): no view from behind.
  Only worker-4090 advertises the template.
"""
from __future__ import annotations

import asyncio
import io
import logging
import math
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter()

RENDERFIN_BASE = os.getenv("RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8210").rstrip("/")
SUBMIT_TIMEOUT_SECONDS = 60.0

VIDEO_WORKFLOW = "gen_video_ltx25_crossview_by_url.json"
VIDEO_CHECKPOINT = "ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors"
VIDEO_MAX_FRAMES = 193
VIDEO_MAX_SIDE = 960

# ------------------------------------------------------------------ image

# (yaw, pitch, zoom): yaw > 0 = the camera orbits to its right around the
# subject, pitch > 0 = the camera rises and looks down, zoom > 1 = closer.
IMAGE_PRESETS: Dict[str, Tuple[float, float, float]] = {
    "front": (0, 0, 1),
    "orbit_right_30": (30, 0, 1),
    "orbit_left_30": (-30, 0, 1),
    "orbit_right_45": (45, 0, 1),
    "orbit_left_45": (-45, 0, 1),
    "orbit_right_90": (90, 0, 1),
    "orbit_left_90": (-90, 0, 1),
    "orbit_right_135": (135, 0, 1),
    "orbit_left_135": (-135, 0, 1),
    "back": (180, 0, 1),
    "high_angle": (0, 45, 1),
    "low_angle": (0, -30, 1),
    "top_down": (0, 85, 1),
    "dolly_in": (0, 0, 2),
    "dolly_out": (0, 0, 0.5),
}
IMAGE_PRESET_TITLES = {
    "custom": "Custom — yaw / pitch / zoom below",
    "front": "Front (same view, re-rendered)",
    "orbit_right_30": "Orbit right 30°", "orbit_left_30": "Orbit left 30°",
    "orbit_right_45": "Orbit right 45° (three-quarter)", "orbit_left_45": "Orbit left 45° (three-quarter)",
    "orbit_right_90": "Orbit right 90° (profile)", "orbit_left_90": "Orbit left 90° (profile)",
    "orbit_right_135": "Orbit right 135° (back three-quarter)",
    "orbit_left_135": "Orbit left 135° (back three-quarter)",
    "back": "Back view 180°", "high_angle": "High angle 45° (looking down)",
    "low_angle": "Low angle 30° (looking up)", "top_down": "Top-down (bird's-eye)",
    "dolly_in": "Dolly in (close-up)", "dolly_out": "Dolly out (wide shot)",
}

KEEP = ("Keep everything else identical: the same subject with the same identity, face, "
        "clothes, colours and proportions, the same place and the same lighting. "
        "Only the camera position changes; show the parts that were hidden before "
        "consistently with the original.")


def _edge(yaw: float) -> str:
    """The picture edge the subject's face points at after the camera moves.

    Measured 27.09: with camera-side wording ("orbit left 90") Qwen-Image 2.1
    drew the same profile for left and right. Naming the subject's side and the
    edge of the picture the face points at made all eight 45/90 test renders
    correct. The camera orbiting to its right reaches the subject's left side,
    so the face then points at the LEFT edge.
    """
    return "LEFT" if yaw > 0 else "RIGHT"


def _subject_side(yaw: float) -> str:
    return "left" if yaw > 0 else "right"


def _view_text(yaw: float, strong: bool = False) -> str:
    a = abs(yaw)
    if a < 15:
        return ""
    side, edge = _subject_side(yaw), _edge(yaw)
    move = f"Change the camera angle: the camera has moved {round(a)} degrees around to the subject's {side} side. "
    if a < 60:
        text = (move + f"A three-quarter view: the person's body is turned about {round(a)} degrees, facing between "
                f"the camera and the {edge} edge of the picture; we see the {side} side of the body and one "
                f"shoulder closer.")
    elif a < 115:
        text = (move + f"Show the subject's {side.upper()} side in full profile; the face and body now point toward "
                f"the {edge} edge of the picture.")
    elif a < 160:
        text = (move + f"A rear three-quarter view: we see mostly the back and the {side} side; the face is turned "
                f"away, toward the {edge} edge of the picture.")
    else:
        return ("Change the camera angle: the camera has moved 180 degrees behind the subject. A view from directly "
                "behind: we see the back of the head and the back of the body.")
    if strong:
        other = "RIGHT" if edge == "LEFT" else "LEFT"
        text += f" IMPORTANT: the face must point to the {edge} edge of the picture, NOT to the {other} edge."
    return text


def image_prompt(yaw: float, pitch: float, zoom: float, extra: str = "", strong: bool = False) -> str:
    yaw = max(-180.0, min(180.0, float(yaw)))
    pitch = max(-60.0, min(90.0, float(pitch)))
    zoom = max(0.25, min(4.0, float(zoom)))
    parts = []
    view = _view_text(yaw, strong)
    if view:
        parts.append(view)
    if pitch >= 75:
        parts.append("Raise the camera straight overhead: a top-down bird's-eye view looking down at the subject.")
    elif pitch >= 10:
        parts.append(f"Raise the camera: a high-angle shot looking down at the subject from about {round(pitch)} degrees above.")
    elif pitch <= -10:
        parts.append(f"Lower the camera: a low-angle shot looking up at the subject from about {abs(round(pitch))} degrees below.")
    if zoom >= 1.25:
        parts.append("Move the camera closer: a tighter close-up framing of the subject.")
    elif zoom <= 0.8:
        parts.append("Pull the camera back: a wide shot that shows more of the surroundings around the subject.")
    if not parts:
        parts.append("Re-render the same shot from the same camera position.")
    text = " ".join(parts) + " " + KEEP
    extra = str(extra or "").strip()
    return (text + " " + extra).strip()


class CameraImageRequest(BaseModel):
    image_url: Optional[str] = Field(None, description="Picture (a clip URL gives its first frame)")
    image_base64: Optional[str] = Field(None)
    video_url: Optional[str] = Field(None, description="Clip: its first frame is used")
    preset: str = Field("orbit_right_45", description="A preset name or custom")
    yaw: Optional[float] = Field(None, ge=-180, le=180, description="custom: degrees, + = camera to the right")
    pitch: Optional[float] = Field(None, ge=-60, le=90, description="custom: degrees, + = camera above")
    zoom: Optional[float] = Field(None, ge=0.25, le=4, description="custom: >1 closer, <1 further")
    prompt: Optional[str] = Field(None, max_length=4000, description="Extra words appended to the camera prompt")
    width: Optional[int] = Field(None, ge=256, le=2048)
    height: Optional[int] = Field(None, ge=256, le=2048)
    seed: Optional[int] = Field(None, ge=0, le=9007199254740991)
    verify: bool = Field(True, description="30-150 deg views: Vision checks the side, one re-render if mirrored")
    render_quality: Optional[str] = None  # the editor scales sizes itself
    structured: Optional[bool] = None
    system_prompt: Optional[str] = None


def _camera(preset: str, yaw, pitch, zoom) -> Tuple[float, float, float, str]:
    key = str(preset or "").strip().lower() or "custom"
    if key != "custom" and key not in IMAGE_PRESETS:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_camera_preset",
            "message_string": "preset must be one of: custom, " + ", ".join(IMAGE_PRESETS)})
    base = IMAGE_PRESETS.get(key, (0.0, 0.0, 1.0))
    if key == "custom":
        base = (yaw if yaw is not None else 0.0, pitch if pitch is not None else 0.0,
                zoom if zoom is not None else 1.0)
    return float(base[0]), float(base[1]), float(base[2]), key


@router.get("/api/camera-orbit/image")
async def api_camera_image_docs():
    return {"status_string": "ok", "method_string": "POST", "url_string": "/api/camera-orbit/image",
            "required_fields_array": ["image_url or image_base64 or video_url"],
            "optional_fields_array": ["preset", "yaw", "pitch", "zoom", "prompt", "width", "height", "seed"],
            "presets_object": {k: {"yaw": v[0], "pitch": v[1], "zoom": v[2],
                                   "title": IMAGE_PRESET_TITLES[k]} for k, v in IMAGE_PRESETS.items()},
            "model_string": "Qwen-Image 2.1 turbo (edit)",
            "server_time_unix_int": int(time.time())}


VERIFY_QUESTION = ("Look at the main person or object. In which direction does its face or front point in this "
                   "picture: toward the LEFT edge, toward the RIGHT edge, TOWARD the viewer, or AWAY from the "
                   "viewer? Answer with exactly one word: LEFT, RIGHT, TOWARD or AWAY.")


class RenderEnded(RuntimeError):
    """The render task failed or was cancelled; there is no picture to check."""


async def _wait_task(task_id: str, seconds: float = 240.0) -> str:
    """Poll the render task; its URL on success, RenderEnded on failure/cancel."""
    import ai_vision_api
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            status = dict(await ai_vision_api.api_render_task_status(task_id))
        except Exception:
            status = {}
        state = str(status.get("status_string") or "")
        if state == "completed":
            return str(status.get("output_url_string") or "")
        if state in ("failed", "cancelled"):
            raise RenderEnded(str(status.get("error_string") or state))
        await asyncio.sleep(3)
    raise RenderEnded("the render did not finish within %d s" % seconds)


async def _facing(request: Request, url: str) -> str:
    import ai_music_api
    import ai_vision_api
    vision = ai_vision_api.VisionRequest(prompt=VERIFY_QUESTION, image_url=url, structured=True,
                                         wait_seconds=60)
    answer = await ai_music_api._answer(dict(await ai_vision_api.api_vision(request, vision)))
    for word in ("LEFT", "RIGHT", "TOWARD", "AWAY"):
        if word in answer.upper():
            return word
    return ""


@router.post("/api/camera-orbit/image")
async def api_camera_image(request: Request, body: CameraImageRequest):
    import ai_multiref
    import ai_qwen_image_api
    yaw, pitch, zoom, key = _camera(body.preset, body.yaw, body.pitch, body.zoom)
    picture = str(body.image_url or "").strip()
    if not picture and body.video_url:
        picture = str(body.video_url).strip()
    if picture:
        picture = await ai_multiref.as_picture(picture)
    if not picture and not body.image_base64:
        raise HTTPException(status_code=400, detail={
            "error_string": "image_required",
            "message_string": "Camera orbit needs a picture or a clip (image_url, image_base64 or video_url)"})
    async def render(strong: bool, seed):
        text = image_prompt(yaw, pitch, zoom, body.prompt or "", strong)
        edit = ai_qwen_image_api.QwenImageRequest(
            prompt=text, mode="edit", image_url=picture or None,
            image_base64=None if picture else body.image_base64,
            width=body.width, height=body.height, seed=seed)
        return text, dict(await ai_qwen_image_api.api_qwen_image(edit))

    text, answer = await render(False, body.seed)
    # Side views can come out mirrored. Vision says where the face points; the
    # opposite edge re-renders once with the side spelled out harder.
    check: Dict[str, Any] = {"checked_bool": False}
    if body.verify and 30 <= abs(yaw) <= 150:
        expected = _edge(yaw)
        wrong = "RIGHT" if expected == "LEFT" else "LEFT"
        try:
            url = await _wait_task(str(answer.get("task_id_string") or ""))
            seen = await _facing(request, url)
            check = {"checked_bool": True, "expected_string": expected, "seen_string": seen,
                     "retried_bool": False}
            if seen == wrong:
                text, answer = await render(True, (int(body.seed) + 1) if body.seed else None)
                url = await _wait_task(str(answer.get("task_id_string") or ""))
                check.update(retried_bool=True, seen_after_retry_string=await _facing(request, url))
        except RenderEnded as ended:
            # Nothing to check and nothing to return: say why, at once.
            raise HTTPException(status_code=502, detail={
                "error_string": "camera_orbit_render_failed",
                "message_string": f"The camera-orbit render did not finish: {ended}",
                "task_id_string": str(answer.get("task_id_string") or "")}) from None
        except Exception as exc:  # the render stands even when the check fails
            logger.warning("camera orbit side check failed: %s", exc)
            check = {"checked_bool": False, "error_string": str(exc)[:200]}
    answer.update({"prompt_string": text, "preset_string": key,
                   "camera_object": {"yaw": yaw, "pitch": pitch, "zoom": zoom},
                   "side_check_object": check})
    return answer


# ------------------------------------------------------------------ video

AZIMUTHS = ["far to the left", "to the left", "slightly to the left", "same angle",
            "slightly to the right", "to the right", "far to the right"]
ELEVATIONS = ["lower", "same height", "higher"]
DISTANCES = ["closer", "same distance", "further"]


def video_prompt(azimuth: str, elevation: str, distance: str) -> str:
    return f"crossview. new camera angle: {azimuth}, {elevation}, {distance}."


class CameraVideoRequest(BaseModel):
    video_url: Optional[str] = Field(None, description="Clip to re-shoot (autorig.online or Civitai)")
    image_url: Optional[str] = Field(None, description="The editor's Media socket: a clip URL lands here")
    azimuth: str = Field("to the right")
    elevation: str = Field("same height")
    distance: str = Field("same distance")
    strength: float = Field(1.5, ge=0.5, le=2.5, description="CrossView LoRA strength (1.2-1.8 on distilled)")
    guide_strength: float = Field(1.0, ge=0, le=1, description="How strongly the source clip is followed")
    frame_count: Optional[int] = Field(None, ge=9, le=VIDEO_MAX_FRAMES, description="0/empty = the clip's length")
    width: Optional[int] = Field(None, ge=256, le=2048)
    height: Optional[int] = Field(None, ge=256, le=2048)
    seed: Optional[int] = Field(None, ge=0, le=9007199254740991)
    render_quality: Optional[str] = None
    prompt: Optional[str] = None  # ignored: the LoRA only knows its camera vocabulary


def _snap_frames(frames: int) -> int:
    frames = max(9, min(VIDEO_MAX_FRAMES, int(frames)))
    return ((frames - 1) // 8) * 8 + 1


def _fit(width: int, height: int, limit: int = VIDEO_MAX_SIDE) -> Tuple[int, int]:
    scale = min(1.0, limit / float(max(width, height)))
    w = max(256, int(round(width * scale / 32)) * 32)
    h = max(256, int(round(height * scale / 32)) * 32)
    return w, h


async def _probe_clip(client: httpx.AsyncClient, url: str) -> Tuple[float, int, int]:
    from renderfin.video_input import VideoInputError, download_source_video
    work = Path(tempfile.mkdtemp(prefix="camorbit-probe-"))
    try:
        probe = await download_source_video(client, url, work / "source.bin")
        seconds = float((probe.get("format") or {}).get("duration") or 0)
        stream = next((s for s in probe.get("streams") or [] if s.get("codec_type") == "video"), {})
        return seconds, int(stream.get("width") or 0), int(stream.get("height") or 0)
    except VideoInputError as exc:
        raise HTTPException(status_code=400, detail={
            "error_string": "video_rejected", "message_string": str(exc)}) from None
    finally:
        shutil.rmtree(work, ignore_errors=True)


@router.get("/api/camera-orbit/video")
async def api_camera_video_docs():
    return {"status_string": "ok", "method_string": "POST", "url_string": "/api/camera-orbit/video",
            "required_fields_array": ["video_url"],
            "optional_fields_array": ["azimuth", "elevation", "distance", "strength",
                                      "guide_strength", "frame_count", "width", "height", "seed"],
            "azimuths_array": AZIMUTHS, "elevations_array": ELEVATIONS, "distances_array": DISTANCES,
            "model_string": "LTX-2.5 distilled + CrossView-Prompt IC-LoRA v0.9 (worker-4090)",
            "note_string": "Frontal sector only (about +-60 deg); chain two runs for a bigger move.",
            "server_time_unix_int": int(time.time())}


@router.post("/api/camera-orbit/video")
async def api_camera_video(body: CameraVideoRequest):
    import ai_request_cache
    payload = body.model_dump(exclude_none=True)
    payload.pop("render_quality", None)
    payload.pop("prompt", None)
    if not body.seed:
        return {**(await _uncached_camera_video(body)), "cache_hit_bool": False}
    return await ai_request_cache.run_cached(
        "camera_orbit_video", payload, lambda: _uncached_camera_video(body),
        namespace="camera-orbit-video-20260927-v1")


async def _uncached_camera_video(body: CameraVideoRequest) -> Dict[str, Any]:
    import ai_multiref
    clip = str(body.video_url or "").strip()
    if not clip and body.image_url and await ai_multiref.is_video(body.image_url):
        clip = str(body.image_url).strip()
    if not clip:
        raise HTTPException(status_code=400, detail={
            "error_string": "video_required",
            "message_string": "Re-shooting needs a clip (video_url); for a picture use /api/camera-orbit/image"})
    for value, allowed, name in ((body.azimuth, AZIMUTHS, "azimuth"),
                                 (body.elevation, ELEVATIONS, "elevation"),
                                 (body.distance, DISTANCES, "distance")):
        if value not in allowed:
            raise HTTPException(status_code=400, detail={
                "error_string": "unknown_camera_" + name,
                "message_string": f"{name} must be one of: " + ", ".join(allowed)})
    async with httpx.AsyncClient() as client:
        seconds, src_w, src_h = await _probe_clip(client, clip)
        frames = _snap_frames(body.frame_count or max(9, math.floor(seconds * 24)))
        if body.width and body.height:
            width, height = int(body.width) // 2 * 2, int(body.height) // 2 * 2
        elif src_w and src_h:
            width, height = _fit(src_w, src_h)
        else:
            width, height = 960, 544
        first = await ai_multiref.first_frame(clip)
        text = video_prompt(body.azimuth, body.elevation, body.distance)
        payload: Dict[str, Any] = {
            "image_url": first, "control_video_url": clip,
            "control_strength": float(body.guide_strength),
            "lora_strength": float(body.strength),
            "prompt": text, "negative_prompt": "blurry, distorted, low quality, flicker",
            "work_flow": VIDEO_WORKFLOW, "checkpoint": VIDEO_CHECKPOINT,
            "main_size_width": width, "main_size_height": height, "frame_count": frames,
        }
        if body.seed:
            payload["noise_seed"] = int(body.seed)
        try:
            response = await client.post(RENDERFIN_BASE + "/api-render", json=payload,
                                         timeout=SUBMIT_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("Renderfin did not accept a camera-orbit video")
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_unreachable",
                "message_string": "The render farm did not answer"}) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "video_service_rejected",
                "message_string": f"Render farm answered HTTP {response.status_code}: {response.text[:300]}"})
        accepted = response.json() or {}
        output_url = str(accepted.get("output_url") or "")
        return {"success_bool": True, "task_id_string": str(accepted.get("task_id") or ""),
                "status_string": "pending", "finished_bool": False,
                "video_url_string": output_url, "poll_url_string": output_url,
                "source_video_url_string": clip, "first_frame_url_string": first,
                "prompt_string": text,
                "effective_params_object": {k: payload[k] for k in (
                    "main_size_width", "main_size_height", "frame_count", "work_flow",
                    "checkpoint", "lora_strength", "control_strength", "prompt", "noise_seed")
                    if k in payload},
                "server_time_unix_int": int(time.time())}

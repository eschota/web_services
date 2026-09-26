"""Music generation: POST /api/music (Stable Audio 3 medium on the render farm).

Text, a picture or a clip in; a stereo 44.1 kHz MP3 out, and for a clip also
the clip with that music under it.

How a request becomes music:

1. The prompt. With a picture or a clip, the farm's Vision model (Qwen3.5 9B
   by default, the one the /nodes Vision node uses) looks at it and writes a
   Stable Audio prompt, guided by the node's system prompt and by whatever
   text came in. With text alone, the Text model does the same rewrite, so a
   one-word idea still becomes a full prompt. `reprompt: false` sends the text
   exactly as given.
2. The length. `duration_s` when set; otherwise the clip's own length when a
   clip came in; otherwise 30 s.
3. The render. A typed Renderfin job (`music_sa3`, renderfin.music) on a box
   that advertises the audio checkpoints. Draft quality halves the sampler
   steps and caps the length, like the picture services shrink their size.

The answer has the same shape as every other farm job: `task_id_string`,
`audio_url_string` (the future public URL) and GET /api/ai/render-status.
For a clip, `video_url_string` names /api/music/video/<task>.mp4, which is
muxed on first request once the audio exists (ffmpeg, video copied).
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter()

RENDERFIN_BASE = os.getenv("RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8210").rstrip("/")
SUBMIT_TIMEOUT_SECONDS = 60.0
PROMPT_WAIT_SECONDS = 150.0

DEFAULT_SECONDS = 30.0
MAX_SECONDS = 380.0
DRAFT_MAX_SECONDS = 30.0
STEPS_BY_QUALITY = {"preview": 4, "fast": 6, "normal": 8, "highquality": 8}
MODEL_TYPES = {"medium": "music_sa3", "small": "music_sa3_small"}
# Named on the job so renderfin's eligibility check (fail-closed, from the
# box's own /object_info) only sends it where the checkpoint really is.
MODEL_FILES = {"medium": "stable_audio_3_medium.safetensors",
               "small": "stable_audio_3_small_music.safetensors"}

MUSIC_SYSTEM_PROMPT_DEFAULT = (
    "You are a music supervisor writing a prompt for the Stable Audio 3 text-to-music model. "
    "From the picture or video (and any text the user gave), decide the music that fits: "
    "genre and subgenre, mood, energy, tempo in BPM, key instruments, production style and how it "
    "develops over time. Write ONE English prompt of 25-60 words as a comma-separated description, "
    "for example: 'Cinematic orchestral adventure theme, 100 BPM, soaring strings, french horns, "
    "taiko drums, heroic and uplifting, builds to a big finale, high quality stereo mix'. "
    "Follow the user's text where it states a genre, mood or instrument. "
    "Instrumental unless the user asks for vocals. Write only the prompt, nothing else."
)
INSTRUMENTAL_NEGATIVE = "vocals, singing, voice, speech, lyrics"
_PROMPT_CLEAN_RE = re.compile(r"^\s*(prompt\s*:\s*)", re.IGNORECASE)


class MusicRequest(BaseModel):
    prompt: Optional[str] = Field(None, max_length=4000,
        description="Text idea or a finished Stable Audio prompt")
    negative: Optional[str] = Field(None, max_length=1000, description="What to avoid")
    negative_prompt: Optional[str] = Field(None, max_length=1000, description="Alias of negative")
    duration_s: Optional[float] = Field(None, ge=0, le=MAX_SECONDS,
        description="Clip length; 0/empty = the video's length, else 30 s")
    seed: Optional[int] = Field(None, ge=0, le=2**53)
    instrumental: bool = Field(True, description="Add vocals to the negative prompt")
    image_url: Optional[str] = Field(None, max_length=4096)
    image_base64: Optional[str] = Field(None)
    video_url: Optional[str] = Field(None, max_length=2048)
    media_url: Optional[str] = Field(None, max_length=4096,
        description="Picture or clip; routed by its extension")
    system_prompt: Optional[str] = Field(None, max_length=4000,
        description="Instruction for the prompt writer (the node's system prompt)")
    structured: Optional[bool] = None  # sent by /nodes for system-prompt nodes; unused
    reprompt: bool = Field(True, description="Let the language model write the prompt")
    model: Optional[str] = Field(None, description="Vision/Text model id from /api/ai/models")
    audio_model: str = Field("medium", description="medium (12 GB boxes) or small (music-small)")
    render_quality: Optional[str] = Field(None, description="preview | fast | normal | highquality")
    steps: Optional[int] = Field(None, ge=0, le=100)
    with_video: bool = Field(True, description="For a clip, also publish the clip with the music")


def _looks_like_video(url: str) -> bool:
    return bool(re.search(r"\.(mp4|webm|mov|m4v)(\?|#|$)", str(url or ""), re.IGNORECASE))


def _quality(value: Optional[str]) -> str:
    value = str(value or "normal").strip().lower()
    return value if value in STEPS_BY_QUALITY else "normal"


async def _video_seconds(client: httpx.AsyncClient, url: str) -> Optional[float]:
    """The clip's length, read with the same admission checks as every video input."""
    from renderfin.video_input import VideoInputError, download_source_video
    work = Path(tempfile.mkdtemp(prefix="music-probe-"))
    try:
        probe = await download_source_video(client, url, work / "source.bin")
        duration = float((probe.get("format") or {}).get("duration") or 0)
        return duration if duration > 0 else None
    except VideoInputError as exc:
        raise HTTPException(status_code=400, detail={
            "error_string": "video_rejected", "message_string": str(exc)}) from None
    except Exception:
        logger.exception("Could not read the length of %s", url)
        return None
    finally:
        shutil.rmtree(work, ignore_errors=True)


async def _answer(result: Dict[str, Any]) -> str:
    """Wait for a Vision/Text job and return its answer text."""
    import ai_vision_api
    deadline = time.monotonic() + PROMPT_WAIT_SECONDS
    while True:
        status = str(result.get("status_string") or "")
        if status == "completed":
            return str(result.get("answer_string") or "").strip()
        if status in ("failed", "cancelled"):
            raise HTTPException(status_code=502, detail={
                "error_string": "music_prompt_failed",
                "message_string": str(result.get("error_string") or "The prompt writer failed")})
        if time.monotonic() > deadline:
            raise HTTPException(status_code=504, detail={
                "error_string": "music_prompt_timeout",
                "message_string": "The prompt writer did not answer in time"})
        await asyncio.sleep(3)
        result = await ai_vision_api.api_ai_status(str(result.get("task_id_string") or ""))


async def _write_prompt(request: Request, body: MusicRequest, image_url: str,
                        image_base64: str, video_url: str, seconds: float) -> str:
    import ai_vision_api
    user_text = str(body.prompt or "").strip()
    system = str(body.system_prompt or "").strip() or MUSIC_SYSTEM_PROMPT_DEFAULT
    length_note = f"The music will be {seconds:.0f} seconds long."
    ask = (("User's wishes: " + user_text + "\n") if user_text else "") + length_note + (
        "\nWrite the Stable Audio prompt for music that fits this.")
    if image_url or image_base64 or video_url:
        vision = ai_vision_api.VisionRequest(
            prompt=ask, image_url=image_url or None, image_base64=image_base64 or None,
            video_url=video_url or None, video_mode="storyboard", system_prompt=system,
            structured=True, model=body.model, wait_seconds=60)
        result = await ai_vision_api.api_vision(request, vision)
    else:
        text = ai_vision_api.TextRequest(
            prompt="Turn this idea into the Stable Audio prompt. " + length_note,
            input=user_text, system_prompt=system, structured=True, model=body.model,
            wait_seconds=60)
        result = await ai_vision_api.api_text2text(request, text)
    answer = _PROMPT_CLEAN_RE.sub("", await _answer(dict(result))).strip().strip('"').strip()
    if not answer:
        raise HTTPException(status_code=502, detail={
            "error_string": "music_prompt_empty",
            "message_string": "The prompt writer returned nothing"})
    return answer


@router.get("/api/music")
async def api_music_docs():
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/music",
        "model_string": "Stable Audio 3 medium (stabilityai), ComfyUI native, stereo 44.1 kHz MP3",
        "required_fields_array": ["prompt or image_url or video_url"],
        "optional_fields_array": ["negative", "duration_s", "seed", "instrumental",
                                  "system_prompt", "reprompt", "model", "audio_model",
                                  "render_quality", "steps", "with_video"],
        "status_url_string": "/api/ai/render-status/{task_id}",
        "example_request_object": {"prompt": "calm lo-fi hip hop for studying",
                                   "duration_s": 20, "seed": 7},
        "server_time_unix_int": int(time.time()),
    }


@router.post("/api/music")
async def api_music(request: Request, body: MusicRequest):
    import ai_request_cache
    payload = body.model_dump(exclude_none=True)
    run = lambda: _uncached_api_music(request, body)  # noqa: E731
    if not body.seed:
        return await run()
    return await ai_request_cache.run_cached("music", payload, run, namespace="music-sa3-20260926-v1")


async def _uncached_api_music(request: Request, body: MusicRequest):
    image_url = str(body.image_url or "").strip()
    video_url = str(body.video_url or "").strip()
    media = str(body.media_url or "").strip()
    if media and not image_url and not video_url:
        if _looks_like_video(media):
            video_url = media
        else:
            image_url = media
    image_base64 = str(body.image_base64 or "").strip()
    user_text = str(body.prompt or "").strip()
    if not (user_text or image_url or video_url or image_base64):
        raise HTTPException(status_code=400, detail={
            "error_string": "music_input_required",
            "message_string": "Provide prompt, image_url, video_url or media_url"})
    audio_type = MODEL_TYPES.get(str(body.audio_model or "medium").strip().lower())
    if not audio_type:
        raise HTTPException(status_code=400, detail={
            "error_string": "unknown_audio_model",
            "message_string": "audio_model must be medium or small"})
    quality = _quality(body.render_quality)

    async with httpx.AsyncClient() as client:
        video_seconds = await _video_seconds(client, video_url) if video_url else None
        seconds = float(body.duration_s or 0) or video_seconds or DEFAULT_SECONDS
        if quality == "preview" and not video_seconds:
            seconds = min(seconds, DRAFT_MAX_SECONDS)
        seconds = round(max(1.0, min(MAX_SECONDS, seconds)), 2)
        steps = int(body.steps or 0) or STEPS_BY_QUALITY[quality]

        started = time.monotonic()
        if body.reprompt or not user_text:
            prompt = await _write_prompt(request, body, image_url, image_base64, video_url, seconds)
        else:
            prompt = user_text
        prompt_seconds = round(time.monotonic() - started, 1)

        negative = str(body.negative or body.negative_prompt or "").strip()
        if body.instrumental and "vocal" not in negative.lower():
            negative = ", ".join(x for x in (negative, INSTRUMENTAL_NEGATIVE) if x)
        payload: Dict[str, Any] = {
            "prompt": prompt,
            "negative_prompt": negative,
            "type": audio_type,
            "checkpoint": MODEL_FILES[str(body.audio_model or "medium").strip().lower()],
            "audio_seconds": seconds,
            "steps": steps,
            "noise_seed": int(body.seed or 0),
        }
        try:
            response = await client.post(RENDERFIN_BASE + "/api-render", json=payload,
                                         timeout=SUBMIT_TIMEOUT_SECONDS)
        except Exception:
            logger.exception("Renderfin did not accept a music request")
            raise HTTPException(status_code=502, detail={
                "error_string": "music_service_unreachable",
                "message_string": "The render farm did not answer"}) from None
        if response.status_code not in (200, 202):
            raise HTTPException(status_code=502, detail={
                "error_string": "music_service_rejected",
                "message_string": f"Render farm answered HTTP {response.status_code}"})
        accepted = response.json() or {}
    task_id = str(accepted.get("task_id") or "").strip()
    audio_url = str(accepted.get("output_url") or "").strip()
    if not task_id or not audio_url:
        raise HTTPException(status_code=502, detail={
            "error_string": "music_service_no_output",
            "message_string": "The render farm accepted the request without an output URL"})
    result: Dict[str, Any] = {
        "success_bool": True,
        "task_id_string": task_id,
        "status_string": "pending",
        "audio_url_string": audio_url,
        "output_url_string": audio_url,
        "prompt_string": prompt,
        "negative_prompt_string": negative,
        "duration_seconds_float": seconds,
        "steps_int": steps,
        "render_quality_string": quality,
        "audio_model_string": body.audio_model,
        "prompt_writer_seconds_float": prompt_seconds,
        "status_url_string": f"/api/ai/render-status/{task_id}",
        "cache_hit_bool": False,
        "server_time_unix_int": int(time.time()),
    }
    if video_url and body.with_video:
        _remember_mux(task_id, video_url, audio_url)
        result["video_url_string"] = f"https://autorig.online/api/music/video/{task_id}.mp4"
    return result


# ------------------------------------------------------------ clip with music

_TASK_RE = re.compile(r"^[A-Za-z0-9-]{8,64}$")


def _mux_dir() -> Path:
    from renderfin import config as renderfin_config
    path = Path(renderfin_config.DATA_DIR) / "music-mux"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _remember_mux(task_id: str, video_url: str, audio_url: str) -> None:
    import json
    (_mux_dir() / f"{task_id}.json").write_text(
        json.dumps({"video_url": video_url, "audio_url": audio_url}), encoding="utf-8")


def _local_audio_path(audio_url: str) -> Optional[Path]:
    from renderfin import config as renderfin_config
    prefix = str(renderfin_config.PUBLIC_BASE_URL).rstrip("/") + "/render/"
    if not audio_url.startswith(prefix):
        return None
    relative = audio_url[len(prefix):]
    if ".." in relative:
        return None
    return Path(renderfin_config.RENDER_DIR) / relative


_mux_locks: Dict[str, asyncio.Lock] = {}


@router.api_route("/api/music/video/{name}", methods=["GET", "HEAD"])
async def api_music_video(name: str):
    import json
    task_id = name[:-4] if name.endswith(".mp4") else name
    if not _TASK_RE.fullmatch(task_id):
        raise HTTPException(status_code=404, detail="not found")
    target = _mux_dir() / f"{task_id}.mp4"
    if not target.is_file():
        record_path = _mux_dir() / f"{task_id}.json"
        if not record_path.is_file():
            raise HTTPException(status_code=404, detail="not found")
        record = json.loads(record_path.read_text(encoding="utf-8"))
        audio = _local_audio_path(str(record.get("audio_url") or ""))
        if audio is None or not audio.is_file():
            raise HTTPException(status_code=404, detail="the music is not ready yet")
        lock = _mux_locks.setdefault(task_id, asyncio.Lock())
        async with lock:
            if not target.is_file():
                await _mux(str(record["video_url"]), audio, target)
    return FileResponse(target, media_type="video/mp4")


async def _mux(video_url: str, audio: Path, target: Path) -> None:
    from renderfin.video_input import download_source_video
    work = Path(tempfile.mkdtemp(prefix="music-mux-"))
    try:
        source = work / "source.bin"
        async with httpx.AsyncClient() as client:
            probe = await download_source_video(client, video_url, source)
        partial = work / "out.mp4"
        # A short clip's music is cut to the clip; a 0.8 s fade makes the cut an ending.
        video_seconds = float((probe.get("format") or {}).get("duration") or 0)
        fade = f"afade=t=out:st={max(0.0, video_seconds - 0.8):.2f}:d=0.8" if video_seconds > 2 else "anull"
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
            "-i", str(source), "-i", str(audio),
            "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-af", fade,
            "-c:a", "aac", "-b:a", "192k",
            "-shortest", "-movflags", "+faststart", str(partial),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=180)
        if process.returncode != 0 or not partial.is_file():
            # Copying the stream fails for codecs MP4 cannot hold; re-encode once.
            process = await asyncio.create_subprocess_exec(
                "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
                "-i", str(source), "-i", str(audio),
                "-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-crf", "18",
                "-pix_fmt", "yuv420p", "-af", fade, "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart", str(partial),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=600)
            if process.returncode != 0 or not partial.is_file():
                logger.error("music mux failed: %s", stderr.decode(errors="replace")[-400:])
                raise HTTPException(status_code=502, detail="mux failed")
        shutil.move(str(partial), str(target))
    finally:
        shutil.rmtree(work, ignore_errors=True)

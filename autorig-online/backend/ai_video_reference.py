"""Create a durable image reference from one trusted public video.

This service performs no recognition and makes no quality claim.  It extracts
either the first frame or a five-frame chronological storyboard that can be
fed into the existing image/avatar graph services.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Literal

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, Field, field_validator

from renderfin.video_input import (
    VideoInputError,
    download_prepare_video,
    validate_video_url,
)


router = APIRouter()

ASSET_DIR = Path(
    os.getenv("AUTORIG_AI_VIDEO_REFERENCE_DIR", "/srv/autorig/data/var/ai-video-references")
)
PUBLIC_BASE_URL = os.getenv("AUTORIG_PUBLIC_URL", "https://autorig.online").rstrip("/")
FFMPEG_BIN = os.getenv("RENDERFIN_FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.getenv("RENDERFIN_FFPROBE_BIN", "ffprobe")
MAX_OPERATION_SECONDS = 60.0
MAX_PNG_BYTES = 24 * 1024 * 1024
ASSET_ID_RE = re.compile(r"^[a-f0-9]{32}$")
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class VideoReferenceError(RuntimeError):
    pass


class VideoReferenceRequest(BaseModel):
    video_url: str = Field(..., min_length=1, max_length=2048)
    view: Literal["first_frame", "storyboard"] = "first_frame"
    frame_count: int = Field(97, ge=9, le=393)

    @field_validator("video_url")
    @classmethod
    def validate_source(cls, value: str) -> str:
        try:
            return validate_video_url(value)
        except VideoInputError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("frame_count")
    @classmethod
    def validate_frame_count(cls, value: int) -> int:
        if (value - 1) % 8:
            raise ValueError("frame_count must be 8k+1")
        return value


async def _run_process(*argv: str) -> bytes:
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise VideoReferenceError(f"cannot start {argv[0]}: {exc}") from exc
    try:
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        process.kill()
        await process.communicate()
        raise
    if process.returncode:
        detail = stderr.decode("utf-8", errors="replace")[-2000:]
        raise VideoReferenceError(
            f"{argv[0]} failed with exit {process.returncode}: {detail}"
        )
    return stdout


async def _probe_video(path: Path) -> Dict[str, Any]:
    raw = await _run_process(
        FFPROBE_BIN,
        "-v", "error",
        "-show_entries", "format=duration:stream=codec_type,width,height",
        "-of", "json",
        str(path),
    )
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VideoReferenceError("ffprobe returned invalid JSON") from exc
    streams = value.get("streams") if isinstance(value, dict) else None
    stream = next(
        (row for row in streams or [] if isinstance(row, dict) and row.get("codec_type") == "video"),
        None,
    )
    if stream is None:
        raise VideoReferenceError("prepared source has no video stream")
    try:
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
        duration = float((value.get("format") or {}).get("duration") or 0)
    except (TypeError, ValueError) as exc:
        raise VideoReferenceError("prepared source metadata is invalid") from exc
    if width < 1 or height < 1 or duration <= 0:
        raise VideoReferenceError("prepared source metadata is invalid")
    return {"width": width, "height": height, "duration": duration}


def _inspect_png(path: Path) -> tuple[int, int, bytes]:
    try:
        data = path.read_bytes()
        if not data or len(data) > MAX_PNG_BYTES:
            raise VideoReferenceError("derived PNG is empty or too large")
        with Image.open(path) as image:
            image.verify()
        with Image.open(path) as image:
            if image.format != "PNG":
                raise VideoReferenceError("derived image is not PNG")
            width, height = image.size
    except (OSError, UnidentifiedImageError, Image.DecompressionBombError) as exc:
        raise VideoReferenceError("derived PNG is invalid") from exc
    if width < 1 or height < 1 or width > 2048 or height > 2048:
        raise VideoReferenceError("derived PNG dimensions are outside the supported range")
    return width, height, data


def _store_png(data: bytes, metadata: Dict[str, Any]) -> Dict[str, Any]:
    digest = hashlib.sha256(data).hexdigest()
    asset_id = secrets.token_hex(16)
    asset_dir = ASSET_DIR / "assets" / asset_id
    asset_dir.mkdir(parents=True, mode=0o700)
    image_path = asset_dir / f"{digest}.png"
    metadata_path = asset_dir / "metadata.json"
    image_temp = asset_dir / ".image.tmp"
    metadata_temp = asset_dir / ".metadata.tmp"
    try:
        image_temp.write_bytes(data)
        os.replace(image_temp, image_path)
        stored = {
            **metadata,
            "asset_id_string": asset_id,
            "sha256_string": digest,
            "image_url_string": (
                f"{PUBLIC_BASE_URL}/api/ai/video-references/{asset_id}/{digest}.png"
            ),
            "created_at_unix_float": time.time(),
        }
        metadata_temp.write_text(
            json.dumps(stored, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(metadata_temp, metadata_path)
        return stored
    except Exception:
        shutil.rmtree(asset_dir, ignore_errors=True)
        raise


def _resolve_asset(asset_id: str, digest: str) -> Path:
    if not ASSET_ID_RE.fullmatch(asset_id) or not SHA256_RE.fullmatch(digest):
        raise HTTPException(status_code=404, detail="Video reference not found")
    asset_dir = ASSET_DIR / "assets" / asset_id
    metadata_path = asset_dir / "metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise HTTPException(status_code=404, detail="Video reference not found") from None
    if metadata.get("asset_id_string") != asset_id or metadata.get("sha256_string") != digest:
        raise HTTPException(status_code=404, detail="Video reference not found")
    path = asset_dir / f"{digest}.png"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Video reference not found")
    return path


async def _derive_reference(body: VideoReferenceRequest) -> Dict[str, Any]:
    temp_root = ASSET_DIR / ".video-reference-temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="reference-", dir=temp_root))
    source = work_dir / "prepared.mp4"
    output = work_dir / "reference.png"
    try:
        async with httpx.AsyncClient(follow_redirects=False, timeout=60.0) as client:
            _name, video_bytes = await download_prepare_video(
                client, body.video_url, body.frame_count, 24
            )
        source.write_bytes(video_bytes)
        probe = await _probe_video(source)
        if body.view == "first_frame":
            frame_indices = [0]
            await _run_process(
                FFMPEG_BIN, "-v", "error", "-y", "-i", str(source),
                "-frames:v", "1", str(output),
            )
        else:
            last = body.frame_count - 1
            frame_indices = [round(last * part / 4) for part in range(5)]
            expression = "+".join(f"eq(n\\,{index})" for index in frame_indices)
            cell_height = min(
                2048,
                max(2, round(384 * probe["height"] / probe["width"])),
            )
            if cell_height % 2:
                cell_height += 1
            await _run_process(
                FFMPEG_BIN, "-v", "error", "-y", "-i", str(source),
                "-vf", (
                    f"select='{expression}',"
                    f"scale=384:{cell_height}:force_original_aspect_ratio=decrease,"
                    f"pad=384:{cell_height}:(ow-iw)/2:(oh-ih)/2,tile=5x1"
                ),
                "-frames:v", "1", str(output),
            )
        width, height, png = _inspect_png(output)
        timepoints = [round(index / 24.0, 6) for index in frame_indices]
        stored = _store_png(png, {
            "view_string": body.view,
            "source_video_url_string": body.video_url,
            "source_width_int": probe["width"],
            "source_height_int": probe["height"],
            "source_duration_seconds_float": probe["duration"],
            "frame_count_int": body.frame_count,
            "fps_int": 24,
            "frame_indices_int_array": frame_indices,
            "timepoints_seconds_float_array": timepoints,
            "width_int": width,
            "height_int": height,
        })
        return {
            "success_bool": True,
            "status_string": "completed",
            "finished_bool": True,
            "entity_type_string": "image",
            "view_string": body.view,
            "image_url_string": stored["image_url_string"],
            "asset_id_string": stored["asset_id_string"],
            "sha256_string": stored["sha256_string"],
            "width_int": width,
            "height_int": height,
            "source_width_int": probe["width"],
            "source_height_int": probe["height"],
            "duration_seconds_float": probe["duration"],
            "frame_indices_int_array": frame_indices,
            "timepoints_seconds_float_array": timepoints,
            "server_time_unix_int": int(time.time()),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def _derive_with_deadline(body: VideoReferenceRequest) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(_derive_reference(body), timeout=MAX_OPERATION_SECONDS)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail={
            "error_string": "video_reference_timeout",
            "message_string": "Video reference extraction exceeded 60 seconds",
        }) from exc
    except VideoInputError as exc:
        raise HTTPException(status_code=400, detail={
            "error_string": "invalid_video_reference",
            "message_string": str(exc),
        }) from exc
    except VideoReferenceError as exc:
        raise HTTPException(status_code=422, detail={
            "error_string": "video_reference_extraction_failed",
            "message_string": str(exc),
        }) from exc


@router.get("/api/ai/video-reference")
async def api_video_reference_docs():
    return {
        "status_string": "ok",
        "method_string": "POST",
        "url_string": "/api/ai/video-reference",
        "required_fields_array": ["video_url"],
        "optional_fields_object": {
            "view": ["first_frame", "storyboard"],
            "frame_count": "8k+1 in 9..393; default 97",
        },
        "produces_string": "durable image reference",
        "quality_decision_string": "manual; this endpoint does not identify people or score quality",
    }


@router.post("/api/ai/video-reference")
async def api_video_reference(body: VideoReferenceRequest):
    import ai_request_cache

    payload = body.model_dump()
    payload["source_fingerprint_string"] = hashlib.sha256(
        body.video_url.encode("utf-8")
    ).hexdigest()
    return await ai_request_cache.run_cached(
        "control",
        payload,
        lambda: _derive_with_deadline(body),
        namespace="video-reference-v1",
    )


@router.api_route(
    "/api/ai/video-references/{asset_id}/{sha256}.png",
    methods=["GET", "HEAD"],
)
async def get_video_reference(asset_id: str, sha256: str):
    path = _resolve_asset(asset_id, sha256)
    return FileResponse(
        path,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )

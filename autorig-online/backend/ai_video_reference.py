"""Create a durable image reference from one trusted public video.

This service performs no recognition and makes no quality claim.  It extracts
either the first frame or a five-frame chronological storyboard that can be
fed into the existing image/avatar graph services.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
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
from PIL import Image, ImageDraw, ImageFont, UnidentifiedImageError
from pydantic import BaseModel, Field, field_validator

from renderfin.video_input import (
    VideoInputError,
    download_prepare_video,
    download_source_video,
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
# A contact sheet decodes the whole source rather than a 4-second normalisation
# of it, so it is allowed longer before it is called a timeout.
CONTACT_SHEET_OPERATION_SECONDS = 150.0
MAX_PNG_BYTES = 24 * 1024 * 1024
ASSET_ID_RE = re.compile(r"^[a-f0-9]{32}$")

# What a description of a whole video needs: the beginning, the end, and the
# moments in between where the picture actually changed. Six is the floor
# because five evenly spaced frames of a cut-heavy clip miss every cut; twelve
# is the ceiling because the tiles stop being readable below that size.
CONTACT_SHEET_MIN_FRAMES = 6
CONTACT_SHEET_MAX_FRAMES = 12
# Scene detection decodes every frame. Past this length that costs more than
# it buys, and evenly spaced frames still cover start to end.
SCENE_SCAN_MAX_SECONDS = 300.0
SCENE_SCORE_FLOOR = 0.3
CONTACT_SHEET_TILE_WIDTH = 384
CONTACT_SHEET_LABEL_HEIGHT = 26
CONTACT_SHEET_GAP = 8
CONTACT_SHEET_MAX_EDGE = 2000
LABEL_FONT_CANDIDATES = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
)
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class VideoReferenceError(RuntimeError):
    pass


class VideoReferenceRequest(BaseModel):
    video_url: str = Field(..., min_length=1, max_length=2048)
    # `storyboard` is the original five-tile strip of the normalised first
    # seconds. `contact_sheet` covers the source from start to end and labels
    # every tile, which is what a reader needs to describe a whole video.
    view: Literal["first_frame", "storyboard", "contact_sheet"] = "first_frame"
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
        "-count_frames",
        "-show_entries", "format=duration:stream=codec_type,width,height,nb_read_frames",
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
        frame_count = int(stream.get("nb_read_frames") or 0)
    except (TypeError, ValueError) as exc:
        raise VideoReferenceError("prepared source metadata is invalid") from exc
    if width < 1 or height < 1 or duration <= 0 or frame_count < 1:
        raise VideoReferenceError("prepared source metadata is invalid")
    return {"width": width, "height": height, "duration": duration,
            "frame_count": frame_count}


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


async def _probe_source(path: Path) -> Dict[str, Any]:
    """Duration, size and frame rate of the untouched source.

    Deliberately not `_probe_video`: that one counts every frame, which means
    decoding the whole file before a single tile is cut. Here the container's
    own duration and rate are enough to place the tiles.
    """
    raw = await _run_process(
        FFPROBE_BIN,
        "-v", "error",
        "-show_entries",
        "format=duration:stream=codec_type,width,height,avg_frame_rate,duration",
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
        raise VideoReferenceError("source has no video stream")
    try:
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
        duration = float((value.get("format") or {}).get("duration")
                         or stream.get("duration") or 0)
    except (TypeError, ValueError) as exc:
        raise VideoReferenceError("source metadata is invalid") from exc
    numerator, _, denominator = str(stream.get("avg_frame_rate") or "0/1").partition("/")
    try:
        fps = float(numerator) / float(denominator or 1)
    except (TypeError, ValueError, ZeroDivisionError):
        fps = 0.0
    if width < 1 or height < 1 or duration <= 0:
        raise VideoReferenceError("source metadata is invalid")
    return {"width": width, "height": height, "duration": duration,
            "fps": round(fps, 3) if fps > 0 else 0.0}


async def _scene_change_times(path: Path, duration: float) -> list[tuple[float, float]]:
    """(timestamp, score) for the moments the picture changed most.

    Scored on a downscaled copy of the stream: the score is a measure of how
    much of the frame changed, and shrinking it first makes the scan several
    times cheaper without moving the peaks. A failure here is not an error —
    evenly spaced frames still cover the video, so the caller falls back.
    """
    if duration > SCENE_SCAN_MAX_SECONDS:
        return []
    try:
        raw = await _run_process(
            FFMPEG_BIN, "-v", "error", "-nostdin",
            "-i", str(path),
            "-an", "-sn",
            "-vf", (
                "scale=256:-2,"
                f"select='gt(scene,{SCENE_SCORE_FLOOR})',"
                "metadata=print:file=-"
            ),
            "-f", "null", "-",
        )
    except VideoReferenceError:
        return []
    found: list[tuple[float, float]] = []
    pending_time = None
    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        marker = "pts_time:"
        if line.startswith("frame:") and marker in line:
            try:
                pending_time = float(line.split(marker, 1)[1].split()[0])
            except (IndexError, ValueError):
                pending_time = None
            continue
        if pending_time is not None and "lavfi.scene_score=" in line:
            try:
                score = float(line.split("lavfi.scene_score=", 1)[1].strip())
            except (IndexError, ValueError):
                score = 0.0
            if 0 < pending_time < duration:
                found.append((pending_time, score))
            pending_time = None
    return found


def _contact_sheet_times(duration: float,
                         scenes: list[tuple[float, float]]) -> list[float]:
    """Start, end, and the strongest changes in between; evenly spaced to fill.

    The first and last frames are not negotiable — a description that does not
    reach the end of the video is the wrong description — so they are placed
    first and everything else competes for the slots between them.
    """
    span = max(0.0, float(duration))
    # Just inside the end rather than at it: a seek to the container's own
    # duration lands past the final frame and ffmpeg writes nothing at all.
    last = max(0.0, span - min(0.12, span / 20.0))
    chosen = [0.0, last] if last > 0.05 else [0.0]
    # A change is only worth a tile if it is not on top of one already taken,
    # and the spacing is sized by the tiles available: a clip that cuts every
    # half second would otherwise spend all twelve on its first ten seconds and
    # never show the ending it is supposed to be covering.
    minimum_gap = max(span / float(CONTACT_SHEET_MAX_FRAMES), 0.05) if span else 0.05
    for moment, _score in sorted(scenes, key=lambda item: -item[1]):
        if len(chosen) >= CONTACT_SHEET_MAX_FRAMES:
            break
        if all(abs(moment - taken) >= minimum_gap for taken in chosen):
            chosen.append(moment)
    if len(chosen) < CONTACT_SHEET_MIN_FRAMES and last > 0.05:
        # Fill from an even grid over the *whole* span rather than by walking
        # forward from zero: a video with no cuts would otherwise get six tiles
        # of its first second and nothing after it.
        wanted = CONTACT_SHEET_MIN_FRAMES
        grid = [last * index / float(wanted - 1) for index in range(wanted)]
        for gap in (minimum_gap, minimum_gap / 3.0, 0.0):
            for moment in grid:
                if len(chosen) >= wanted:
                    break
                if all(abs(moment - taken) > gap for taken in chosen):
                    chosen.append(moment)
    return sorted(round(value, 3) for value in chosen)


async def _extract_frame(source: Path, moment: float, target: Path):
    """One frame at `moment`, seeking on the input so long clips stay cheap.

    A seek that lands between the last frame and the end of the container
    succeeds and writes nothing, which is how the tile that was supposed to
    show the ending went missing. Rather than compute the exact frame interval
    from a rate that a variable-rate source does not really have, step back and
    ask again — the point of the tile is the moment, not the frame number.
    """
    for back_off in (0.0, 0.12, 0.4, 1.0):
        at = max(0.0, moment - back_off)
        target.unlink(missing_ok=True)
        try:
            await _run_process(
                FFMPEG_BIN, "-v", "error", "-nostdin", "-y",
                "-ss", f"{at:.3f}",
                "-i", str(source),
                "-an", "-sn",
                "-frames:v", "1",
                str(target),
            )
        except VideoReferenceError:
            if at <= 0.0:
                break
            continue
        if target.is_file() and target.stat().st_size > 0:
            # The time actually shown, so the tile's label is not a claim the
            # picture cannot support.
            return at
        if at <= 0.0:
            break
    return None


def _label_font(size: int):
    for candidate in LABEL_FONT_CANDIDATES:
        try:
            return ImageFont.truetype(candidate, size)
        except (OSError, ValueError):
            continue
    return ImageFont.load_default()


def _compose_contact_sheet(frames: list[tuple[float, Path]],
                           duration: float) -> bytes:
    """One labelled sheet, read left to right and top to bottom.

    Each tile carries its index and its timestamp, because the model is being
    asked what happens *over time*: without the labels it sees a collage and
    describes the pictures, not the sequence.
    """
    if not frames:
        raise VideoReferenceError("no frames could be extracted from the source")
    opened: list[Image.Image] = []
    try:
        for _moment, path in frames:
            try:
                with Image.open(path) as image:
                    opened.append(image.convert("RGB"))
            except (OSError, UnidentifiedImageError, Image.DecompressionBombError) as exc:
                raise VideoReferenceError("an extracted frame is not a readable image") from exc
        columns = 3 if len(opened) <= 6 else 4
        rows = (len(opened) + columns - 1) // columns
        aspect = opened[0].height / max(1, opened[0].width)
        tile_width = CONTACT_SHEET_TILE_WIDTH
        tile_height = max(2, round(tile_width * aspect))
        label_height = CONTACT_SHEET_LABEL_HEIGHT
        gap = CONTACT_SHEET_GAP
        sheet_width = columns * tile_width + (columns + 1) * gap
        sheet_height = rows * (tile_height + label_height) + (rows + 1) * gap
        scale = min(1.0,
                    CONTACT_SHEET_MAX_EDGE / sheet_width,
                    CONTACT_SHEET_MAX_EDGE / sheet_height)
        if scale < 1.0:
            tile_width = max(2, int(tile_width * scale))
            tile_height = max(2, int(tile_height * scale))
            label_height = max(12, int(label_height * scale))
            gap = max(2, int(gap * scale))
            sheet_width = columns * tile_width + (columns + 1) * gap
            sheet_height = rows * (tile_height + label_height) + (rows + 1) * gap
        sheet = Image.new("RGB", (sheet_width, sheet_height), (17, 17, 20))
        draw = ImageDraw.Draw(sheet)
        font = _label_font(max(10, int(label_height * 0.62)))
        for index, image in enumerate(opened):
            column = index % columns
            row = index // columns
            left = gap + column * (tile_width + gap)
            top = gap + row * (tile_height + label_height + gap)
            tile = image.copy()
            tile.thumbnail((tile_width, tile_height), Image.LANCZOS)
            sheet.paste(tile, (left + (tile_width - tile.width) // 2,
                               top + (tile_height - tile.height) // 2))
            caption = f"#{index + 1}  t={frames[index][0]:.2f}s"
            draw.text((left + 2, top + tile_height + 3), caption,
                      fill=(235, 235, 240), font=font)
        buffer = io.BytesIO()
        sheet.save(buffer, format="PNG", optimize=True)
        return buffer.getvalue()
    finally:
        for image in opened:
            image.close()


def contact_sheet_prompt_prefix(count: int, timepoints: list[float],
                                duration: float) -> str:
    """The sentence that turns a collage back into a video for the reader."""
    first = timepoints[0] if timepoints else 0.0
    last = timepoints[-1] if timepoints else duration
    return (
        f"These are {count} chronological frames (t={first:.1f}s … t={last:.1f}s) "
        f"of one video that is {duration:.1f}s long, laid out left to right and "
        "top to bottom and labelled with their timestamps. They are not "
        "separate pictures. Describe what happens from the beginning to the "
        "end of the video."
    )


async def _derive_contact_sheet(body: VideoReferenceRequest) -> Dict[str, Any]:
    temp_root = ASSET_DIR / ".video-reference-temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="sheet-", dir=temp_root))
    source = work_dir / "source.bin"
    try:
        async with httpx.AsyncClient(follow_redirects=False, timeout=120.0) as client:
            await download_source_video(client, body.video_url, source)
        probe = await _probe_source(source)
        duration = float(probe["duration"])
        scenes = await _scene_change_times(source, duration)
        moments = _contact_sheet_times(duration, scenes)
        frames: list[tuple[float, Path]] = []
        seen: set[float] = set()
        for index, moment in enumerate(moments):
            target = work_dir / f"frame-{index:02d}.png"
            shown = await _extract_frame(source, moment, target)
            # A tile backed off onto a moment already taken would show the same
            # picture twice, which says less than leaving the space.
            if shown is not None and round(shown, 3) not in seen:
                seen.add(round(shown, 3))
                frames.append((round(shown, 3), target))
        frames.sort(key=lambda item: item[0])
        if not frames:
            raise VideoReferenceError("no frame could be read from this video")
        png = _compose_contact_sheet(frames, duration)
        width, height, png = _inspect_png_bytes(png)
        timepoints = [round(moment, 3) for moment, _path in frames]
        prefix = contact_sheet_prompt_prefix(len(frames), timepoints, duration)
        stored = _store_png(png, {
            "view_string": body.view,
            "source_video_url_string": body.video_url,
            "source_width_int": probe["width"],
            "source_height_int": probe["height"],
            "source_duration_seconds_float": duration,
            "fps_float": probe["fps"],
            "scene_changes_found_int": len(scenes),
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
            "duration_seconds_float": round(duration, 3),
            "fps_float": probe["fps"],
            "frame_count_int": len(frames),
            "scene_changes_found_int": len(scenes),
            "timepoints_seconds_float_array": timepoints,
            "prompt_prefix_string": prefix,
            "server_time_unix_int": int(time.time()),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def _inspect_png_bytes(data: bytes) -> tuple[int, int, bytes]:
    """The same guard `_inspect_png` applies, for a sheet built in memory."""
    temp_root = ASSET_DIR / ".video-reference-temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    holder = Path(tempfile.mkdtemp(prefix="verify-", dir=temp_root))
    path = holder / "sheet.png"
    try:
        path.write_bytes(data)
        return _inspect_png(path)
    finally:
        shutil.rmtree(holder, ignore_errors=True)


async def _derive_reference(body: VideoReferenceRequest) -> Dict[str, Any]:
    if body.view == "contact_sheet":
        return await _derive_contact_sheet(body)
    temp_root = ASSET_DIR / ".video-reference-temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="reference-", dir=temp_root))
    source = work_dir / "prepared.mp4"
    output = work_dir / "reference.png"
    try:
        async with httpx.AsyncClient(follow_redirects=False, timeout=60.0) as client:
            _name, video_bytes = await download_prepare_video(
                client, body.video_url, body.frame_count, 24, allow_shorter=True
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
            last = probe["frame_count"] - 1
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
            "frame_count_int": probe["frame_count"],
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
    deadline = (CONTACT_SHEET_OPERATION_SECONDS if body.view == "contact_sheet"
                else MAX_OPERATION_SECONDS)
    try:
        return await asyncio.wait_for(_derive_reference(body), timeout=deadline)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail={
            "error_string": "video_reference_timeout",
            "message_string": f"Video reference extraction exceeded {deadline:g} seconds",
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
            "view": ["first_frame", "storyboard", "contact_sheet"],
            "frame_count": "8k+1 in 9..393; default 97 (first_frame/storyboard only)",
        },
        "produces_string": "durable image reference",
        "contact_sheet_string": (
            f"{CONTACT_SHEET_MIN_FRAMES}..{CONTACT_SHEET_MAX_FRAMES} labelled frames "
            "covering the whole source from start to end, placed on the strongest "
            "scene changes and evenly spaced otherwise"),
        "quality_decision_string": "manual; this endpoint does not identify people or score quality",
    }


async def reference_for_video(video_url: str, view: str) -> Dict[str, Any]:
    """The same cached derivation the endpoint performs, for in-process callers.

    /api/vision needs a picture before it can ask a question about a video, and
    it should pay for that picture once per video, not once per question — so
    it goes through the request cache exactly as an HTTP caller would.
    """
    return await api_video_reference(VideoReferenceRequest(video_url=video_url, view=view))


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
        namespace="video-reference-v2",
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

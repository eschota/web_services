"""Strict preparation of a public control video for a ComfyUI workflow.

Only explicitly trusted public asset origins are accepted.  The downloaded
source is kept unchanged while ffprobe and ffmpeg operate on a unique temporary
directory below the Renderfin data tree; that directory alone is removed when
the operation ends.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import math
import os
import re
import shutil
import socket
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import urljoin, urlsplit

import httpx

from . import config


MAX_VIDEO_BYTES = 100 * 1024 * 1024
MAX_DIMENSION = 2048
MAX_DURATION_SECONDS = 16.4
PROCESS_TIMEOUT_SECONDS = 60.0
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
FFMPEG_BIN = os.getenv("RENDERFIN_FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.getenv("RENDERFIN_FFPROBE_BIN", "ffprobe")

_AUTORIG_PATH_PREFIXES = (
    "/dev/api/scratch/",
    "/renderfin/render/",
    "/api/ai/avatar-assets/",
)
_PVS_HOST = re.compile(r"pvs[1-9]\.microstock\.plus", re.IGNORECASE)
_CIVITAI_AUTH_HOSTS = {"civitai.com", "www.civitai.com", "image.civitai.com"}
_CIVITAI_CDN_HOSTS = {"blobs-b2.civitai.com"}
_CIVITAI_HOSTS = _CIVITAI_AUTH_HOSTS | _CIVITAI_CDN_HOSTS
_MP4_FORMATS = {"mov", "mp4", "m4a", "3gp", "3g2", "mj2"}
_SOURCE_VIDEO_FORMATS = _MP4_FORMATS | {"matroska", "webm"}


class VideoInputError(RuntimeError):
    """The control video cannot be admitted or prepared safely."""


def _validated_url(value: str) -> str:
    url = str(value or "").strip()
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise VideoInputError("invalid control video URL") from exc
    hostname = (parsed.hostname or "").rstrip(".").lower()
    if parsed.scheme.lower() != "https" or not hostname:
        raise VideoInputError("control video URL must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise VideoInputError("credentials are not allowed in a control video URL")
    if port is not None:
        raise VideoInputError("custom ports are not allowed in a control video URL")
    if parsed.fragment:
        raise VideoInputError("fragments are not allowed in a control video URL")
    if hostname == "autorig.online":
        if not any(parsed.path.startswith(prefix) for prefix in _AUTORIG_PATH_PREFIXES):
            raise VideoInputError("autorig.online control video path is not allowed")
    elif not _PVS_HOST.fullmatch(hostname) and hostname not in _CIVITAI_HOSTS:
        raise VideoInputError("control video host is not allowed")
    if not parsed.path or parsed.path.endswith("/"):
        raise VideoInputError("control video URL must identify a file")
    return url


def _validated_frame_count(frame_count: int, fps: int) -> Tuple[int, int]:
    if isinstance(frame_count, bool) or not isinstance(frame_count, int):
        raise VideoInputError("frame_count must be an integer")
    if isinstance(fps, bool) or not isinstance(fps, int) or fps != 24:
        raise VideoInputError("control video fps must be 24")
    if frame_count < 9 or frame_count > 393 or (frame_count - 1) % 8:
        raise VideoInputError("frame_count must be 8k+1 in the range 9..393")
    if frame_count / fps > MAX_DURATION_SECONDS:
        raise VideoInputError("requested control video duration exceeds 16.4 seconds")
    return frame_count, fps


async def _run_process(*argv: str) -> bytes:
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError as exc:
        raise VideoInputError(f"cannot start {argv[0]}: {exc}") from exc
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=PROCESS_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError as exc:
        process.kill()
        await process.communicate()
        raise VideoInputError(f"{argv[0]} exceeded {PROCESS_TIMEOUT_SECONDS:g}s") from exc
    if process.returncode:
        detail = stderr.decode("utf-8", errors="replace")[-2000:]
        raise VideoInputError(f"{argv[0]} failed with exit {process.returncode}: {detail}")
    return stdout


async def _probe(path: Path, *, count_frames: bool = False) -> Dict[str, Any]:
    argv = [
        FFPROBE_BIN,
        "-v", "error",
        "-show_entries", "format=format_name,duration:format_tags=comment:stream=codec_type,codec_name,pix_fmt,width,height,nb_read_frames",
        "-of", "json",
    ]
    if count_frames:
        argv.insert(3, "-count_frames")
    argv.append(str(path))
    raw = await _run_process(*argv)
    try:
        result = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VideoInputError("ffprobe returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise VideoInputError("ffprobe returned a non-object result")
    return result


def _video_stream(probe: Dict[str, Any]) -> Dict[str, Any]:
    streams = probe.get("streams")
    if not isinstance(streams, list):
        raise VideoInputError("ffprobe found no streams")
    stream = next(
        (row for row in streams if isinstance(row, dict) and row.get("codec_type") == "video"),
        None,
    )
    if stream is None:
        raise VideoInputError("control input has no video stream")
    try:
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
    except (TypeError, ValueError) as exc:
        raise VideoInputError("control input has invalid dimensions") from exc
    if width < 2 or height < 2:
        raise VideoInputError("control input has invalid dimensions")
    return stream


def _validate_container_probe(
    probe: Dict[str, Any], allowed_formats: set[str], error_message: str
) -> Dict[str, Any]:
    stream = _video_stream(probe)
    format_row = probe.get("format")
    if not isinstance(format_row, dict):
        raise VideoInputError("ffprobe found no container")
    formats = {part.strip().lower() for part in str(format_row.get("format_name") or "").split(",")}
    if not formats.intersection(allowed_formats):
        raise VideoInputError(error_message)
    try:
        duration = float(format_row.get("duration") or 0)
    except (TypeError, ValueError) as exc:
        raise VideoInputError("control input has invalid duration") from exc
    if duration <= 0:
        raise VideoInputError("control input has invalid duration")
    return stream


def _validate_source_probe(probe: Dict[str, Any]) -> Dict[str, Any]:
    return _validate_container_probe(
        probe, _SOURCE_VIDEO_FORMATS, "control input is not a supported MP4 or WebM video"
    )


def _validate_mp4_probe(probe: Dict[str, Any]) -> Dict[str, Any]:
    return _validate_container_probe(
        probe, _MP4_FORMATS, "control input is not an MP4 container"
    )


async def _download(client: httpx.AsyncClient, url: str, target: Path) -> None:
    current_url = _validated_url(url)
    may_follow_redirect = (urlsplit(current_url).hostname or "").rstrip(".").lower() in _CIVITAI_HOSTS
    try:
        for redirect_count in range(4):
            await _assert_public_dns(current_url)
            async with client.stream(
                "GET", current_url, headers=_civitai_headers(current_url),
                timeout=60.0, follow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    if not may_follow_redirect:
                        raise VideoInputError("redirects are not allowed for control videos")
                    location = str(response.headers.get("location") or "").strip()
                    if not location:
                        raise VideoInputError("control video redirect has no Location")
                    current_url = _validated_url(urljoin(current_url, location))
                    # Headers are rebuilt from the new URL on the next pass.
                    # A redirect to PVS/autorig is therefore allowed but gets
                    # no Civitai bearer; any other host fails the allowlist.
                    continue
                if response.status_code != 200:
                    raise VideoInputError(
                        f"control video download returned HTTP {response.status_code}"
                    )
                declared = response.headers.get("content-length")
                if declared:
                    try:
                        declared_size = int(declared)
                    except ValueError as exc:
                        raise VideoInputError("invalid control video Content-Length") from exc
                    if declared_size < 1 or declared_size > MAX_VIDEO_BYTES:
                        raise VideoInputError("control video exceeds the 100 MB limit")
                size = 0
                with target.open("wb") as output:
                    async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_BYTES):
                        size += len(chunk)
                        if size > MAX_VIDEO_BYTES:
                            raise VideoInputError("control video exceeds the 100 MB limit")
                        output.write(chunk)
                if size == 0:
                    raise VideoInputError("control video download is empty")
                return
        raise VideoInputError("control video exceeded the redirect limit")
    except VideoInputError:
        raise
    except (httpx.HTTPError, OSError) as exc:
        # Transport errors may quote invalid headers. Never echo a bearer
        # value, a signed query string, or the original exception chain.
        raise VideoInputError(f"control video download failed: {type(exc).__name__}") from None


validate_video_url = _validated_url


async def download_source_video(
    client: httpx.AsyncClient, url: str, target: Path
) -> Dict[str, Any]:
    """Fetch a trusted public video unchanged and return its validated probe.

    `download_prepare_video` normalises a clip for a ComfyUI workflow, which
    means 24 fps and at most 16.4 seconds. A reader that has to describe a
    whole video needs the whole video, so this keeps the bytes as served and
    only applies the same admission checks: the host allow-list, the public-DNS
    check, the Civitai bearer, the 100 MB cap and the container probe.
    """
    admitted_url = _validated_url(url)
    await _download(client, admitted_url, target)
    probe = await _probe(target)
    _validate_source_probe(probe)
    return probe


def _civitai_headers(url: str) -> Dict[str, str]:
    """Bearer auth is server-only and never follows a URL outside Civitai."""
    hostname = (urlsplit(url).hostname or "").rstrip(".").lower()
    token = str(os.getenv("CIVITAI_API_TOKEN") or "").strip()
    if hostname in _CIVITAI_AUTH_HOSTS and token:
        if any(ord(char) < 33 or ord(char) > 126 for char in token):
            raise VideoInputError("Civitai token configuration is invalid")
        return {"Authorization": f"Bearer {token}"}
    return {}


async def _assert_public_dns(url: str) -> None:
    """Reject an allowlisted name if DNS points it at a non-public address."""
    hostname = (urlsplit(url).hostname or "").rstrip(".").lower()
    try:
        rows = await asyncio.get_running_loop().run_in_executor(
            None, lambda: socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)
        )
    except OSError as exc:
        raise VideoInputError("control video host could not be resolved") from exc
    addresses = {row[4][0].split("%", 1)[0] for row in rows if row and row[4]}
    if not addresses:
        raise VideoInputError("control video host could not be resolved")
    for value in addresses:
        try:
            address = ipaddress.ip_address(value)
        except ValueError as exc:
            raise VideoInputError("control video host resolved to an invalid address") from exc
        if not address.is_global:
            raise VideoInputError("control video host resolved to a non-public address")


async def download_prepare_video(
    client: httpx.AsyncClient,
    url: str,
    frame_count: int,
    fps: int = 24,
    *,
    allow_shorter: bool = False,
) -> Tuple[str, bytes]:
    """Download and normalize a trusted control video.

    The returned filename is unique and safe to upload directly to ComfyUI.
    The source file is preserved byte-for-byte throughout processing, then the
    function removes only its own unique working directory. Render controls
    hold the final source frame when needed to satisfy the inclusive 8k+1 frame
    contract. Reference extraction may set ``allow_shorter`` to retain a real
    shorter duration without inventing repeated storyboard frames.
    """
    admitted_url = _validated_url(url)
    frame_count, fps = _validated_frame_count(frame_count, fps)
    temp_root = (config.RENDER_DIR / ".video-input-temp").resolve()
    temp_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="control-", dir=temp_root)).resolve()
    try:
        work_dir.relative_to(temp_root)
    except ValueError as exc:  # defensive: never clean an unexpected path
        raise VideoInputError("video input temporary directory escaped its root") from exc
    source = work_dir / "source.mp4"
    output = work_dir / "control.mp4"
    filename = f"control-{uuid.uuid4().hex}.mp4"
    try:
        await _download(client, admitted_url, source)
        source_probe = await _probe(source)
        _validate_source_probe(source_probe)
        source_duration = float((source_probe.get("format") or {}).get("duration") or 0)
        available_frames = max(1, int(math.floor(source_duration * fps + 1e-6)))
        held_tail_frames = 0 if allow_shorter else max(0, frame_count - available_frames)
        video_filter = (
            f"fps={fps},"
            "scale=w='min(2048,iw)':h='min(2048,ih)':"
            "force_original_aspect_ratio=decrease:force_divisible_by=2"
        )
        if not allow_shorter:
            # ``-frames:v`` cannot create an inclusive final frame. Hold the
            # actual last frame before applying that cap; never loop the clip.
            video_filter += (
                f",tpad=stop_mode=clone:stop_duration={frame_count / fps:.6f}"
            )
        comment = (
            f"AutoRig normalized control; held final frame {held_tail_frames} "
            f"time(s) to reach {frame_count} frames"
            if not allow_shorter else
            "AutoRig normalized reference; source duration retained without tail padding"
        )
        await _run_process(
            FFMPEG_BIN,
            "-v", "error",
            "-y",
            "-i", str(source),
            "-map", "0:v:0",
            "-an",
            "-vf", video_filter,
            "-frames:v", str(frame_count),
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-metadata", f"comment={comment}",
            "-movflags", "+faststart+use_metadata_tags",
            str(output),
        )
        if not output.is_file():
            raise VideoInputError("ffmpeg produced no control video")
        size = output.stat().st_size
        if size < 1 or size > MAX_VIDEO_BYTES:
            raise VideoInputError("prepared control video exceeds the 100 MB limit")
        output_probe = await _probe(output, count_frames=True)
        stream = _validate_mp4_probe(output_probe)
        if str(stream.get("codec_name") or "").lower() != "h264":
            raise VideoInputError("prepared control video is not H.264")
        if str(stream.get("pix_fmt") or "").lower() != "yuv420p":
            raise VideoInputError("prepared control video is not yuv420p")
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
        if width > MAX_DIMENSION or height > MAX_DIMENSION:
            raise VideoInputError("prepared control video exceeds 2048 pixels")
        duration = float((output_probe.get("format") or {}).get("duration") or 0)
        if duration <= 0 or duration > MAX_DURATION_SECONDS:
            raise VideoInputError("prepared control video duration exceeds 16.4 seconds")
        try:
            decoded_frames = int(stream.get("nb_read_frames") or 0)
        except (TypeError, ValueError) as exc:
            raise VideoInputError("cannot verify prepared control video frame count") from exc
        if not allow_shorter and decoded_frames != frame_count:
            raise VideoInputError(
                f"prepared control video has {decoded_frames} frames; expected {frame_count}"
            )
        if allow_shorter and not (1 <= decoded_frames <= frame_count):
            raise VideoInputError(
                f"prepared reference video has invalid frame count {decoded_frames}"
            )
        return filename, output.read_bytes()
    finally:
        # ``work_dir`` came from mkdtemp under ``temp_root`` and was checked
        # above.  Never remove the shared root or any sibling job.
        shutil.rmtree(work_dir, ignore_errors=True)

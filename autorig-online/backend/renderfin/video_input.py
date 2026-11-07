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
from fractions import Fraction
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit

import httpx

from . import config
from .errors import RequestFaultError


MAX_VIDEO_BYTES = 100 * 1024 * 1024
# A source larger than the cap is fetched up to this size and transcoded down
# (owner, 2026-09-27: never reject a big clip, make it fit).
MAX_RAW_VIDEO_BYTES = 2 * 1024 * 1024 * 1024
TRANSCODE_LONG_SIDE = 960
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


class VideoInputError(RequestFaultError):
    """The control video cannot be admitted or prepared safely.

    Always a property of the URL the caller supplied - rejected origin, wrong
    fps, oversized clip, an origin that will not serve it - so the dispatcher
    must never blame the render box it happened to be aiming at.  Still a
    RuntimeError by inheritance, so the API handlers that catch it are
    unaffected.
    """


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


# A clip's own frame rate is accepted for Upscale 2x inside this range.
MIN_SOURCE_FPS = 1
MAX_SOURCE_FPS = 120


def _parse_rate(value: Any) -> Optional[Fraction]:
    text = str(value or "").strip()
    if not text or text in {"0/0", "N/A"}:
        return None
    try:
        rate = Fraction(text)
    except (ValueError, ZeroDivisionError):
        return None
    return rate if rate > 0 else None


def rate_text(rate: Fraction) -> str:
    """An ffmpeg rate argument: ``30`` or the exact ``30000/1001``."""
    rate = Fraction(rate)
    if rate.denominator == 1:
        return str(rate.numerator)
    return f"{rate.numerator}/{rate.denominator}"


def source_frame_rate(stream: Dict[str, Any]) -> Fraction:
    """The clip's frame rate as an exact rational (30000/1001 stays 30000/1001).

    ``r_frame_rate`` is the nominal rate and is exact; it wins when it agrees
    with the measured ``avg_frame_rate`` (within 0.5 %). A variable-rate phone
    clip whose nominal rate is a timebase artefact (90000/1) falls back to the
    measured average, rounded to a /1001 grid.
    """
    real = _parse_rate(stream.get("r_frame_rate"))
    average = _parse_rate(stream.get("avg_frame_rate"))
    if real and average and abs(real - average) <= average * Fraction(5, 1000):
        rate = real
    elif average:
        rate = average.limit_denominator(1001)
    elif real:
        rate = real
    else:
        raise VideoInputError("cannot read the clip's frame rate")
    if not (MIN_SOURCE_FPS <= rate <= MAX_SOURCE_FPS):
        raise VideoInputError(
            f"the clip's frame rate {float(rate):.3f} fps is outside "
            f"{MIN_SOURCE_FPS}..{MAX_SOURCE_FPS} fps")
    return rate


def source_frame_count(stream: Dict[str, Any], format_duration: float, rate: Fraction) -> int:
    """How many frames the clip has at ``rate``: its own count when the container says."""
    try:
        counted = int(stream.get("nb_frames") or 0)
    except (TypeError, ValueError):
        counted = 0
    if counted > 0:
        return counted
    try:
        duration = float(stream.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    duration = duration if duration > 0 else float(format_duration or 0)
    if duration <= 0:
        raise VideoInputError("control input has invalid duration")
    # 1 ms of slack: 10.0 s at 30 fps is 300 frames, not 301.
    return max(1, int(math.ceil(duration * float(rate) - 1e-3)))


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


async def _run_process(*argv: str, timeout: float = 0) -> bytes:
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
            process.communicate(), timeout=timeout or PROCESS_TIMEOUT_SECONDS
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
        "-show_entries", "format=format_name,duration:format_tags=comment:stream=codec_type,codec_name,pix_fmt,width,height,nb_read_frames,nb_frames,avg_frame_rate,r_frame_rate,duration",
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
        # Content is sniffed by ffprobe, not by the address: say what arrived.
        raise VideoInputError(f"{error_message} (got {sorted(formats) or 'nothing'}; a picture wired "
                              "into a video socket, or a page instead of a file?)")
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


_CIVITAI_CDN_SEGMENT = re.compile(r"^(/[A-Za-z0-9_-]+/[0-9a-fA-F-]{36}/)([^/]+)(/[^/]+)$")


def _smaller_civitai_variant(url: str) -> str:
    """The same Civitai clip transcoded to 1080 px wide, or "" if not a CDN link.

    Originals on image.civitai.* can be hundreds of MB; the CDN serves any
    width on request, and a control input or a first frame never needs more
    than half-HD (owner, 2026-09-27).
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if not host.startswith("image.civitai."):
        return ""
    match = _CIVITAI_CDN_SEGMENT.match(parts.path)
    if not match or "width=1080" in match.group(2):
        return ""
    path = match.group(1) + "transcode=true,width=1080,optimized=true" + match.group(3)
    return parts._replace(path=path).geturl()


async def _download(client, url, target):
    """Fetch a control video that fits the 100 MB cap, making it fit if it does not.

    1. As served. 2. A Civitai original: the same clip at 1080 px from the CDN.
    3. Anything else: fetched whole (up to 2 GB) and transcoded to at most
       960 px on the long side, H.264, the first 16.4 s.
    """
    try:
        return await _download_once(client, url, target)
    except VideoInputError as error:
        if "exceeds the 100 MB limit" not in str(error):
            raise
    smaller = _smaller_civitai_variant(url)
    if smaller:
        try:
            return await _download_once(client, smaller, target)
        except VideoInputError as error:
            if "exceeds the 100 MB limit" not in str(error):
                raise
    raw = target.with_name(target.stem + ".raw" + (target.suffix or ".mp4"))
    try:
        await _download_once(client, smaller or url, raw, limit=MAX_RAW_VIDEO_BYTES)
        scale = (f"scale=w='if(gte(iw,ih),min({TRANSCODE_LONG_SIDE},iw),-2)':"
                 f"h='if(gte(iw,ih),-2,min({TRANSCODE_LONG_SIDE},ih))'")
        await _run_process(
            FFMPEG_BIN, "-v", "error", "-y", "-i", str(raw), "-t", str(MAX_DURATION_SECONDS),
            "-vf", scale + ",scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p", "-fpsmax", "30",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", str(target), timeout=600)
    finally:
        raw.unlink(missing_ok=True)
    if not target.is_file() or target.stat().st_size == 0:
        raise VideoInputError("oversized video could not be transcoded")
    if target.stat().st_size > MAX_VIDEO_BYTES:
        raise VideoInputError("video is still over 100 MB after transcoding to half-HD")


async def _download_once(client: httpx.AsyncClient, url: str, target: Path,
                         limit: int = MAX_VIDEO_BYTES) -> None:
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
                    if declared_size < 1 or declared_size > limit:
                        raise VideoInputError("control video exceeds the 100 MB limit")
                size = 0
                with target.open("wb") as output:
                    async for chunk in response.aiter_bytes(DOWNLOAD_CHUNK_BYTES):
                        size += len(chunk)
                        if size > limit:
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
    keep_audio: bool = False,
    source_fps: bool = False,
    keep_source_at: Optional[Path] = None,
) -> Tuple[str, bytes]:
    """Download and normalize a trusted control video.

    The returned filename is unique and safe to upload directly to ComfyUI.
    The source file is preserved byte-for-byte throughout processing, then the
    function removes only its own unique working directory. Render controls
    hold the final source frame when needed to satisfy the inclusive 8k+1 frame
    contract. Reference extraction may set ``allow_shorter`` to retain a real
    shorter duration without inventing repeated storyboard frames.

    ``source_fps`` (Upscale 2x) keeps the clip's own frame rate instead of 24:
    ``frame_count`` is then only the ceiling, the clip's real frame count is
    used, and a clip with more frames than the ceiling is refused, never cut.
    ``keep_source_at`` receives a copy of the downloaded original, so the
    caller can mux its untouched audio back into the finished render.
    """
    admitted_url = _validated_url(url)
    frame_count, fps = _validated_frame_count(frame_count, fps)
    rate = Fraction(fps)
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
        source_stream = _validate_source_probe(source_probe)
        source_duration = float((source_probe.get("format") or {}).get("duration") or 0)
        if keep_audio and source_duration > MAX_DURATION_SECONDS + 0.05:
            # An enlargement is the whole clip or nothing: never a silent trim.
            raise VideoInputError(
                f"the clip is {source_duration:.1f} s; Upscale 2x takes clips up to "
                f"{MAX_DURATION_SECONDS} s - split it first")
        if source_fps:
            rate = source_frame_rate(source_stream)
            needed = source_frame_count(source_stream, source_duration, rate)
            if needed > frame_count:
                # Same rule as the duration cap: the whole clip or nothing.
                raise VideoInputError(
                    f"the clip has {needed} frames ({source_duration:.1f} s at "
                    f"{float(rate):.3g} fps); Upscale 2x takes up to {frame_count} frames "
                    f"({frame_count / float(rate):.1f} s at this frame rate) - split it first")
            frame_count = needed
        available_frames = max(1, int(math.floor(source_duration * float(rate) + 1e-6)))
        held_tail_frames = 0 if allow_shorter else max(0, frame_count - available_frames)
        video_filter = (
            f"fps={rate_text(rate)},"
            "scale=w='min(2048,iw)':h='min(2048,ih)':"
            "force_original_aspect_ratio=decrease:force_divisible_by=2"
        )
        if not allow_shorter:
            # ``-frames:v`` cannot create an inclusive final frame. Hold the
            # actual last frame before applying that cap; never loop the clip.
            video_filter += (
                f",tpad=stop_mode=clone:stop_duration={frame_count / float(rate):.6f}"
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
            *(["-map", "0:a:0?", "-c:a", "aac", "-b:a", "192k"] if keep_audio else ["-an"]),
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
        if keep_source_at is not None:
            keep_source_at = Path(keep_source_at)
            keep_source_at.parent.mkdir(parents=True, exist_ok=True)
            part = keep_source_at.with_name(keep_source_at.name + ".part")
            shutil.copyfile(source, part)
            os.replace(part, keep_source_at)
        return filename, output.read_bytes()
    finally:
        # ``work_dir`` came from mkdtemp under ``temp_root`` and was checked
        # above.  Never remove the shared root or any sibling job.
        shutil.rmtree(work_dir, ignore_errors=True)


# MiniMax H3 anchors a clip guide of 17k+5 frames (5, 22, 39, ...); the node
# crops a longer batch down to that grid, so the prefix is cut to it here.
PREFIX_FRAME_STEP = 17
PREFIX_FRAME_MIN = 5
PREFIX_FRAME_MAX = 124


def prefix_frame_count(frames: int) -> int:
    """The largest 17k+5 count at or below `frames`, within 5..124."""
    try:
        count = int(frames or 0)
    except (TypeError, ValueError):
        count = 0
    count = max(PREFIX_FRAME_MIN, min(PREFIX_FRAME_MAX, count))
    return count - (count - PREFIX_FRAME_MIN) % PREFIX_FRAME_STEP


async def download_prepare_prefix(
    client: httpx.AsyncClient,
    url: str,
    frames: int,
    fps: int = 24,
) -> Tuple[str, bytes]:
    """Download a trusted clip and cut its LAST `frames` frames, with their sound.

    The result is what a video continues: 24 fps, H.264 yuv420p, AAC, exactly
    `prefix_frame_count(frames)` frames. A source shorter than that holds its
    last frame (never loops). The same admission checks as a control video.
    """
    admitted_url = _validated_url(url)
    count = prefix_frame_count(frames)
    if isinstance(fps, bool) or not isinstance(fps, int) or fps != 24:
        raise VideoInputError("prefix video fps must be 24")
    temp_root = (config.RENDER_DIR / ".video-input-temp").resolve()
    temp_root.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="prefix-", dir=temp_root)).resolve()
    try:
        work_dir.relative_to(temp_root)
    except ValueError as exc:  # defensive: never clean an unexpected path
        raise VideoInputError("video input temporary directory escaped its root") from exc
    source = work_dir / "source.mp4"
    output = work_dir / "prefix.mp4"
    filename = f"prefix-{uuid.uuid4().hex}.mp4"
    try:
        await _download(client, admitted_url, source)
        source_probe = await _probe(source, count_frames=True)
        source_stream = _validate_source_probe(source_probe)
        source_duration = float((source_probe.get("format") or {}).get("duration") or 0)
        try:
            source_frames = int(source_stream.get("nb_read_frames") or 0)
        except (TypeError, ValueError):
            source_frames = 0
        if source_frames <= 0:
            source_frames = max(1, int(math.floor(source_duration * fps + 1e-6)))
        # Output-side seek (after -i): frame-accurate, both streams cut together.
        start = max(0, source_frames - count) / fps
        video_filter = (
            f"fps={fps},"
            "scale=w='min(2048,iw)':h='min(2048,ih)':"
            "force_original_aspect_ratio=decrease:force_divisible_by=2,"
            f"tpad=stop_mode=clone:stop_duration={count / fps:.6f}"
        )
        await _run_process(
            FFMPEG_BIN,
            "-v", "error",
            "-y",
            "-i", str(source),
            "-ss", f"{start:.6f}",
            "-map", "0:v:0",
            "-map", "0:a:0?", "-c:a", "aac", "-b:a", "192k",
            "-vf", video_filter,
            "-frames:v", str(count),
            "-t", f"{count / fps + 0.5 / fps:.6f}",
            "-c:v", "libx264",
            "-pix_fmt", "yuv420p",
            "-metadata", f"comment=AutoRig video prefix: the last {count} frames of the source",
            "-movflags", "+faststart+use_metadata_tags",
            str(output),
        )
        if not output.is_file():
            raise VideoInputError("ffmpeg produced no prefix video")
        size = output.stat().st_size
        if size < 1 or size > MAX_VIDEO_BYTES:
            raise VideoInputError("prepared prefix video exceeds the 100 MB limit")
        output_probe = await _probe(output, count_frames=True)
        stream = _validate_mp4_probe(output_probe)
        if str(stream.get("codec_name") or "").lower() != "h264":
            raise VideoInputError("prepared prefix video is not H.264")
        if str(stream.get("pix_fmt") or "").lower() != "yuv420p":
            raise VideoInputError("prepared prefix video is not yuv420p")
        try:
            decoded_frames = int(stream.get("nb_read_frames") or 0)
        except (TypeError, ValueError) as exc:
            raise VideoInputError("cannot verify prepared prefix video frame count") from exc
        if decoded_frames != count:
            raise VideoInputError(
                f"prepared prefix video has {decoded_frames} frames; expected {count}"
            )
        return filename, output.read_bytes()
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# ------------------------------------------------------------- audio remux
#
# Upscale 2x (2026-11-07): ComfyUI's SaveVideo re-encodes the sound it was
# handed (AAC mono, ~69 kbps). The finished render therefore gets the ORIGINAL
# clip's first audio stream back, copied packet for packet, next to the
# rendered picture (also copied).

# Audio codecs ffmpeg muxes into MP4 by stream copy and browsers play. Any
# other codec is refused before rendering (stock video keeps its sound
# bit-exact; there is no re-encode fallback since 2026-11-07).
MP4_COPYABLE_AUDIO = frozenset({"aac", "mp3", "ac3", "eac3", "alac", "opus"})
REMUX_TIMEOUT_SECONDS = 300.0


async def probe_media(path: Path) -> Dict[str, Any]:
    """ffprobe JSON with what a remux and ``output_meta`` need."""
    raw = await _run_process(
        FFPROBE_BIN, "-v", "error",
        "-show_entries",
        "format=format_name,duration,bit_rate:stream=index,codec_type,codec_name,"
        "width,height,avg_frame_rate,r_frame_rate,duration,nb_frames,channels,"
        "channel_layout,sample_rate,bit_rate",
        "-of", "json", str(path))
    try:
        result = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VideoInputError("ffprobe returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise VideoInputError("ffprobe returned a non-object result")
    return result


def _first_stream(probe: Dict[str, Any], kind: str) -> Optional[Dict[str, Any]]:
    for row in probe.get("streams") or []:
        if isinstance(row, dict) and row.get("codec_type") == kind:
            return row
    return None


def _int_or_none(value: Any) -> Optional[int]:
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def audio_meta(stream: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """``{codec, channels, sample_rate, bit_rate}`` of an audio stream, or None."""
    if not stream:
        return None
    return {
        "codec": str(stream.get("codec_name") or "") or None,
        "channels": _int_or_none(stream.get("channels")),
        "sample_rate": _int_or_none(stream.get("sample_rate")),
        "bit_rate": _int_or_none(stream.get("bit_rate")),
    }


def _video_duration(video: Dict[str, Any], probe: Dict[str, Any]) -> float:
    try:
        seconds = float(video.get("duration") or 0)
    except (TypeError, ValueError):
        seconds = 0.0
    if seconds > 0:
        return seconds
    frames = _int_or_none(video.get("nb_frames"))
    rate = _parse_rate(video.get("avg_frame_rate")) or _parse_rate(video.get("r_frame_rate"))
    if frames and rate:
        return float(frames / rate)
    try:
        return max(0.0, float((probe.get("format") or {}).get("duration") or 0))
    except (TypeError, ValueError):
        return 0.0


def media_meta(probe: Dict[str, Any]) -> Dict[str, Any]:
    """``{w, h, fps, fps_string, duration, audio}`` of a finished clip."""
    video = _first_stream(probe, "video")
    if video is None:
        raise VideoInputError("the finished clip has no video stream")
    rate = _parse_rate(video.get("avg_frame_rate")) or _parse_rate(video.get("r_frame_rate"))
    return {
        "w": _int_or_none(video.get("width")),
        "h": _int_or_none(video.get("height")),
        "fps": round(float(rate), 3) if rate else None,
        "fps_string": rate_text(rate) if rate else "",
        "duration": round(_video_duration(video, probe), 3),
        "audio": audio_meta(_first_stream(probe, "audio")),
    }


def build_remux_command(
    rendered: Path,
    source: Path,
    output: Path,
    *,
    source_audio: Optional[Dict[str, Any]],
    duration: float,
) -> Tuple[List[str], str]:
    """The ffmpeg argv and the audio mode: ``none`` or ``copy``.

    Picture: the rendered video stream, copied. Sound: the source's first
    audio stream, copied packet for packet. Cut to the rendered picture's
    length. A codec MP4 cannot carry by copy is refused, never re-encoded.
    """
    argv: List[str] = [
        FFMPEG_BIN, "-v", "error", "-y",
        "-i", str(rendered),
        "-i", str(source),
        "-map", "0:v:0",
        "-c:v", "copy",
    ]
    if not source_audio:
        mode = "none"
        argv += ["-an"]
    else:
        codec = str(source_audio.get("codec") or "").lower()
        if codec not in MP4_COPYABLE_AUDIO:
            raise VideoInputError(
                f"source audio {codec or 'unknown'} cannot be copied into MP4 unchanged")
        mode = "copy"
        argv += ["-map", "1:a:0", "-c:a", "copy"]
    if duration > 0:
        argv += ["-t", f"{duration:.6f}"]
    # ComfyUI writes its whole workflow into the container tags; the
    # delivered clip carries none of it.
    argv += ["-map_metadata", "-1", "-movflags", "+faststart", "-f", "mp4", str(output)]
    return argv, mode


_AUDIO_SOURCE = {"none": "none", "copy": "original"}


async def audio_packets(path: Path) -> bytes:
    """The first audio stream's packets, concatenated, as stored (no decode)."""
    return await _run_process(
        FFMPEG_BIN, "-v", "error", "-i", str(path), "-map", "0:a:0", "-c", "copy",
        "-f", "data", "-", timeout=REMUX_TIMEOUT_SECONDS)


async def remux_source_audio(rendered: Path, source: Path, output: Path) -> Dict[str, Any]:
    """Write ``output``: the rendered picture with the source clip's own sound.

    Returns ``output_meta`` for the finished clip: ``{w, h, fps, fps_string,
    duration, audio, audio_source}``. A source without sound gives a clip
    without sound. The copied audio is checked against the source: its
    packets must be the source's own, byte for byte (a prefix of them when the
    picture is a fraction of a frame shorter than the sound).
    """
    rendered_probe = await probe_media(rendered)
    video = _first_stream(rendered_probe, "video")
    if video is None:
        raise VideoInputError("the rendered clip has no video stream")
    duration = _video_duration(video, rendered_probe)
    source_audio = audio_meta(_first_stream(await probe_media(source), "audio"))
    argv, mode = build_remux_command(
        rendered, source, output, source_audio=source_audio, duration=duration)
    await _run_process(*argv, timeout=REMUX_TIMEOUT_SECONDS)
    meta = media_meta(await probe_media(output))
    if mode == "copy":
        if not meta.get("audio"):
            raise VideoInputError("the finished clip lost its audio stream")
        copied, original = await audio_packets(output), await audio_packets(source)
        if not copied or not original.startswith(copied):
            raise VideoInputError("the finished clip's audio is not the source's own packets")
        for key in ("codec", "channels", "sample_rate"):
            if meta["audio"].get(key) != source_audio.get(key):
                raise VideoInputError(
                    f"the finished clip's audio {key} {meta['audio'].get(key)} differs "
                    f"from the source's {source_audio.get(key)}")
    elif meta.get("audio"):
        raise VideoInputError("the finished clip has audio the source does not")
    meta["audio_source"] = _AUDIO_SOURCE[mode]
    return meta

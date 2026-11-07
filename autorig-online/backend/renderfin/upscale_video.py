"""Upscale 2x of a whole clip, in segments (2026-11-07).

ComfyUI's ``upscale_video_x2.json`` holds every enlarged frame of its batch in
RAM as float32 - two copies at the peak (``tiled_scale``'s result and the
``clamp`` of it), about 24 bytes per output pixel plus the decoded input. A
1080p clip enlarged to 3840x2160 is ~225 MB per frame, so a long clip cannot
go through in one batch. The clip is therefore:

1. admitted (``admit``): downloaded unchanged (<= 200 MB), probed, refused
   with a coded error when it is too long, too large or carries a codec the
   result could not keep;
2. split (``split``) once, off the request, into constant-rate H.264 pieces
   cut on exact frame numbers (keyframes forced there), every piece at the
   source's own frame rate; the number of frames per piece follows a pixel
   budget (``RENDERFIN_UPSCALE_SEGMENT_MEGAPIXELS``);
3. rendered one piece per ComfyUI prompt, in order, within one Renderfin task
   (``store_output`` checks each returned piece's frame count and size);
4. joined (``assemble``) with the concat demuxer and ``-c copy`` - every piece
   came out of the same SaveVideo encoder - and given the source's first audio
   stream back, packet for packet (``video_input.remux_source_audio``).

Error strings start with a code: ``video_too_long``, ``video_too_large``,
``unsupported_codec``, ``video_unavailable``, ``upscale_segment_failed``,
``video_concat_failed``, ``audio_remux_failed``.
"""
from __future__ import annotations

import json
import math
import os
import shutil
import time
import uuid
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import config, video_input
from .video_input import VideoInputError

MAX_BYTES = 200 * 1024 * 1024
MAX_SECONDS = 60.0
MAX_FRAMES = 1800
MAX_SIDE = 2048
# A piece never holds more frames than this, whatever the budget allows.
MAX_SEGMENT_FRAMES = 390
SCALE = 2
# Output megapixel-frames one ComfyUI prompt may hold. 250 = 30 frames of a
# 1080p source (3840x2160 out, ~6.8 GB peak RAM), 67 of 720p, 120 of 540p.
# The image boxes have 10-25 GB of RAM free (f12 10, f15 15, f5 25, Raptor 70).
DEFAULT_SEGMENT_MEGAPIXELS = 250.0
# Near-transparent intermediate: the pieces are what the model sees.
SEGMENT_CRF = "12"
SPLIT_TIMEOUT_SECONDS = 900.0
PROBE_TIMEOUT_SECONDS = 120.0
_CONTAINERS = {"mov", "mp4", "m4a", "3gp", "3g2", "mj2", "matroska", "webm"}


class UpscaleError(VideoInputError):
    """A clip the service will not (or could not) enlarge; ``code`` says why."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


def segment_megapixels() -> float:
    try:
        value = float(os.getenv("RENDERFIN_UPSCALE_SEGMENT_MEGAPIXELS", "") or DEFAULT_SEGMENT_MEGAPIXELS)
    except ValueError:
        value = DEFAULT_SEGMENT_MEGAPIXELS
    return value if value > 0 else DEFAULT_SEGMENT_MEGAPIXELS


# ------------------------------------------------------------------ paths


def work_dir(task_id: str) -> Path:
    name = Path(str(task_id or "")).name
    if not name or name != str(task_id) or name.startswith("."):
        raise ValueError(f"unsafe task id for a work directory: {task_id!r}")
    return config.TMP_DIR / "upscale-sources" / name


def source_path(task_id: str) -> Path:
    return work_dir(task_id) / "source"


def input_path(task_id: str, index: int) -> Path:
    return work_dir(task_id) / f"in_{index:04d}.mp4"


def output_path(task_id: str, index: int) -> Path:
    return work_dir(task_id) / f"out_{index:04d}.mp4"


def staging_root() -> Path:
    return config.TMP_DIR / "upscale-staging"


def failed_root() -> Path:
    return config.TMP_DIR / "upscale-failed"


# --------------------------------------------------------------- planning


def frames_per_segment(out_width: int, out_height: int,
                       megapixels: Optional[float] = None) -> int:
    """How many frames one prompt may hold: the pixel budget, at most 390."""
    budget = (megapixels if megapixels else segment_megapixels()) * 1_000_000
    pixels = max(1, int(out_width) * int(out_height))
    return max(1, min(MAX_SEGMENT_FRAMES, int(budget // pixels)))


def segment_bounds(frames: int, per: int) -> List[Tuple[int, int]]:
    """[start, end) frame ranges covering 0..frames, in pieces of ``per``."""
    if frames < 1 or per < 1:
        raise ValueError("frames and frames per segment must be positive")
    return [(start, min(frames, start + per)) for start in range(0, frames, per)]


def _even(value: int) -> int:
    return max(2, int(value) - int(value) % 2)


async def _probe(path: Path) -> Dict[str, Any]:
    try:
        return await video_input.probe_media(path)
    except VideoInputError as exc:
        raise UpscaleError("unsupported_codec", f"ffprobe cannot read the clip: {str(exc)[-300:]}") from None


async def inspect(path: Path) -> Dict[str, Any]:
    """Admission checks on a downloaded clip; returns the (not yet split) plan."""
    size = path.stat().st_size
    if size > MAX_BYTES:
        raise UpscaleError("video_too_large", f"the clip is {size / 2**20:.0f} MB; the limit is 200 MB")
    probe = await _probe(path)
    fmt = probe.get("format") or {}
    formats = {part.strip().lower() for part in str(fmt.get("format_name") or "").split(",")}
    if not formats & _CONTAINERS:
        raise UpscaleError("unsupported_codec",
                           f"not an MP4, MOV, MKV or WebM video (got {sorted(formats) or 'nothing'})")
    video = video_input._first_stream(probe, "video")
    if video is None or not str(video.get("codec_name") or "").strip():
        raise UpscaleError("unsupported_codec", "the clip has no video stream ffmpeg can read")
    width = video_input._int_or_none(video.get("width")) or 0
    height = video_input._int_or_none(video.get("height")) or 0
    if width < 2 or height < 2:
        raise UpscaleError("unsupported_codec", "the clip has no readable picture size")
    if max(width, height) > MAX_SIDE:
        raise UpscaleError("video_too_large",
                           f"the clip is {width}x{height}; the long side may be at most {MAX_SIDE} px")
    try:
        rate = video_input.source_frame_rate(video)
    except VideoInputError as exc:
        raise UpscaleError("unsupported_codec", str(exc)) from None
    try:
        duration = float(fmt.get("duration") or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration > MAX_SECONDS + 0.05:
        raise UpscaleError("video_too_long",
                           f"the clip is {duration:.1f} s; the limit is {MAX_SECONDS:.0f} s")
    estimate = video_input.source_frame_count(video, duration, rate) if duration > 0 else 0
    if estimate > MAX_FRAMES:
        raise UpscaleError("video_too_long",
                           f"the clip has about {estimate} frames; the limit is {MAX_FRAMES}")
    audio_stream = video_input._first_stream(probe, "audio")
    audio = video_input.audio_meta(audio_stream)
    if audio and str(audio.get("codec") or "").lower() not in video_input.MP4_COPYABLE_AUDIO:
        raise UpscaleError(
            "unsupported_codec",
            f"the clip's audio is {audio.get('codec') or 'unknown'}, which MP4 cannot carry "
            f"unchanged; supported: {', '.join(sorted(video_input.MP4_COPYABLE_AUDIO))}")
    in_w, in_h = _even(width), _even(height)
    out_w, out_h = in_w * SCALE, in_h * SCALE
    per = frames_per_segment(out_w, out_h)
    return {
        "fps_string": video_input.rate_text(rate),
        "fps_float": round(float(rate), 3),
        "width_int": in_w, "height_int": in_h,
        "out_width_int": out_w, "out_height_int": out_h,
        "source_width_int": width, "source_height_int": height,
        "duration_float": round(duration, 3),
        "video_codec_string": str(video.get("codec_name") or ""),
        "audio": audio,
        "source_bytes_int": size,
        "frames_per_segment_int": per,
        "frames_estimate_int": estimate,
        "segments_int": max(1, math.ceil(max(1, estimate) / per)),
        "segment_frames_array": [],
        "frames_int": 0,
        "ready_bool": False,
    }


async def admit(client, url: str, target: Path) -> Dict[str, Any]:
    """Download the clip unchanged to ``target`` and check it (see ``inspect``)."""
    try:
        admitted = video_input._validated_url(url)
    except VideoInputError as exc:
        raise UpscaleError("video_unavailable", str(exc)) from None
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        await video_input._download_once(client, admitted, target, limit=MAX_BYTES)
    except VideoInputError as exc:
        if "exceeds the" in str(exc):
            raise UpscaleError("video_too_large", "the clip is over the 200 MB limit") from None
        raise UpscaleError("video_unavailable", str(exc)) from None
    return await inspect(target)


# ------------------------------------------------------------------ split


def split_command(source: Path, folder: Path, plan: Dict[str, Any]) -> List[str]:
    """One decode, one encode: constant-rate pieces cut on exact frame numbers."""
    per = int(plan["frames_per_segment_int"])
    # Split points well past any real clip; the muxer ignores the unused ones.
    points = ",".join(str(per * k) for k in range(1, MAX_FRAMES // per + 2))
    return [
        video_input.FFMPEG_BIN, "-v", "error", "-y", "-i", str(source),
        "-map", "0:v:0", "-an", "-sn", "-dn",
        "-vf", (f"fps={plan['fps_string']},"
                f"crop={plan['width_int']}:{plan['height_int']}:0:0,format=yuv420p"),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", SEGMENT_CRF, "-threads", "8",
        "-force_key_frames", f"expr:eq(mod(n,{per}),0)",
        "-f", "segment", "-segment_frames", points, "-reset_timestamps", "1",
        "-segment_format", "mp4", "-segment_format_options", "movflags=+faststart",
        str(folder / "in_%04d.mp4"),
    ]


async def packet_count(path: Path) -> Tuple[int, int, int]:
    """(video packets, width, height) without decoding."""
    raw = await video_input._run_process(
        video_input.FFPROBE_BIN, "-v", "error", "-select_streams", "v:0", "-count_packets",
        "-show_entries", "stream=nb_read_packets,width,height", "-of", "json", str(path),
        timeout=PROBE_TIMEOUT_SECONDS)
    try:
        stream = (json.loads(raw.decode("utf-8")).get("streams") or [{}])[0]
        return (int(stream.get("nb_read_packets") or 0), int(stream.get("width") or 0),
                int(stream.get("height") or 0))
    except (ValueError, TypeError, IndexError, UnicodeDecodeError) as exc:
        raise VideoInputError(f"cannot count the frames of {path.name}") from exc


async def split(task_id: str, plan: Dict[str, Any]) -> Dict[str, Any]:
    """Cut the kept source into ``in_NNNN.mp4`` pieces; returns the ready plan."""
    folder = work_dir(task_id)
    source = source_path(task_id)
    if not source.is_file():
        raise UpscaleError("video_unavailable", "the downloaded clip is gone; send it again")
    for stale in folder.glob("in_*.mp4"):
        stale.unlink(missing_ok=True)
    try:
        await video_input._run_process(*split_command(source, folder, plan),
                                       timeout=SPLIT_TIMEOUT_SECONDS)
    except VideoInputError as exc:
        raise UpscaleError("unsupported_codec",
                           f"ffmpeg cannot decode the clip: {str(exc)[-300:]}") from None
    pieces = sorted(folder.glob("in_*.mp4"))
    per = int(plan["frames_per_segment_int"])
    counts: List[int] = []
    for index, piece in enumerate(pieces):
        if piece.name != input_path(task_id, index).name:
            raise UpscaleError("upscale_segment_failed", f"unexpected piece {piece.name}")
        frames, width, height = await packet_count(piece)
        if (width, height) != (plan["width_int"], plan["height_int"]):
            raise UpscaleError("upscale_segment_failed", f"piece {index} is {width}x{height}")
        counts.append(frames)
    if not counts or sum(counts) < 1:
        raise UpscaleError("unsupported_codec", "ffmpeg decoded no frames from the clip")
    if any(count != per for count in counts[:-1]) or not 1 <= counts[-1] <= per:
        raise UpscaleError("upscale_segment_failed", f"pieces were not cut on frame {per}: {counts}")
    total = sum(counts)
    if total > MAX_FRAMES:
        raise UpscaleError("video_too_long", f"the clip has {total} frames; the limit is {MAX_FRAMES}")
    return {**plan, "segment_frames_array": counts, "segments_int": len(counts),
            "frames_int": total, "ready_bool": True,
            "duration_float": round(total / float(Fraction(plan["fps_string"])), 3)}


def read_input(task_id: str, index: int) -> Tuple[str, bytes]:
    """The piece ``index`` to upload to ComfyUI, under a unique name."""
    piece = input_path(task_id, index)
    if not piece.is_file():
        raise UpscaleError("upscale_segment_failed", f"piece {index} is missing; send the clip again")
    return f"upscale-{uuid.uuid4().hex}-{index:04d}.mp4", piece.read_bytes()


async def store_output(task_id: str, plan: Dict[str, Any], index: int, data: bytes) -> Path:
    """Keep ComfyUI's result for piece ``index`` after checking frames and size."""
    target = output_path(task_id, index)
    part = target.with_name(target.name + ".part")
    part.write_bytes(data)
    try:
        frames, width, height = await packet_count(part)
    except VideoInputError as exc:
        part.unlink(missing_ok=True)
        raise UpscaleError("upscale_segment_failed", f"piece {index} came back unreadable: {exc}") from None
    expected = int(plan["segment_frames_array"][index])
    if frames != expected or (width, height) != (plan["out_width_int"], plan["out_height_int"]):
        part.unlink(missing_ok=True)
        raise UpscaleError(
            "upscale_segment_failed",
            f"piece {index} came back with {frames} frames at {width}x{height}; expected "
            f"{expected} at {plan['out_width_int']}x{plan['out_height_int']}")
    os.replace(part, target)
    return target


# --------------------------------------------------------------- assemble


def concat_list(paths: List[Path], durations: Optional[List[Fraction]] = None) -> str:
    """The concat demuxer's list; ``duration`` lines pin where each piece starts.

    ffmpeg 5.1 (prod) otherwise starts the next piece at the previous one's
    container duration, which MP4 rounds to milliseconds - a 0.7 ms gap at
    every join and a frame rate like 3840000/128017.
    """
    lines = []
    for index, path in enumerate(paths):
        lines.append("file '" + path.name.replace("'", "'\\''") + "'\n")
        if durations is not None:
            lines.append(f"duration {float(durations[index]):.9f}\n")
    return "".join(lines)


def video_only_command(piece: Path, output: Path) -> List[str]:
    """The piece's picture alone: a sound track (SaveVideo adds one when its
    input had sound) would set the piece's length and the joined clip's start."""
    return [video_input.FFMPEG_BIN, "-v", "error", "-y", "-i", str(piece),
            "-map", "0:v:0", "-c", "copy", "-an", "-sn", "-dn", "-f", "mp4", str(output)]


def concat_command(list_file: Path, output: Path) -> List[str]:
    return [video_input.FFMPEG_BIN, "-v", "error", "-y", "-f", "concat", "-safe", "0",
            "-i", str(list_file), "-map", "0:v:0", "-c", "copy",
            "-movflags", "+faststart", "-f", "mp4", str(output)]


async def assemble(task_id: str, plan: Dict[str, Any], output: Path) -> Dict[str, Any]:
    """Join the enlarged pieces and give them the source's own sound -> ``output``."""
    folder = work_dir(task_id)
    count = int(plan["segments_int"])
    pieces = [output_path(task_id, index) for index in range(count)]
    missing = [p.name for p in pieces if not p.is_file()]
    if missing:
        raise UpscaleError("upscale_segment_failed", f"missing enlarged pieces: {missing[:5]}")
    joined = folder / "joined.mp4"
    list_file = folder / "concat.txt"
    bare: List[Path] = []
    durations: List[Fraction] = []
    rate: Optional[Fraction] = None
    try:
        for index, piece in enumerate(pieces):
            target = folder / f"v_{index:04d}.mp4"
            bare.append(target)
            await video_input._run_process(*video_only_command(piece, target),
                                           timeout=video_input.REMUX_TIMEOUT_SECONDS)
            video = video_input._first_stream(await video_input.probe_media(target), "video") or {}
            rate = (video_input._parse_rate(video.get("avg_frame_rate"))
                    or video_input._parse_rate(video.get("r_frame_rate")))
            if not rate:
                raise VideoInputError(f"piece {index} has no readable frame rate")
            frames_here, _, _ = await packet_count(target)
            durations.append(Fraction(frames_here) / rate)
        list_file.write_text(concat_list(bare, durations), encoding="utf-8")
        await video_input._run_process(*concat_command(list_file, joined),
                                       timeout=video_input.REMUX_TIMEOUT_SECONDS)
        frames, width, height = await packet_count(joined)
        joined_video = video_input._first_stream(await video_input.probe_media(joined), "video") or {}
        joined_seconds = float(joined_video.get("duration") or 0)
    except VideoInputError as exc:
        raise UpscaleError("video_concat_failed", str(exc)[-400:]) from None
    finally:
        for target in bare:
            target.unlink(missing_ok=True)
    if frames != int(plan["frames_int"]):
        raise UpscaleError("video_concat_failed",
                           f"joined clip has {frames} frames; the source has {plan['frames_int']}")
    expected_seconds = float(sum(durations))
    if rate and abs(joined_seconds - expected_seconds) > 0.5 / float(rate):
        # A gap or an overlap at a join: the timestamps are not continuous.
        raise UpscaleError("video_concat_failed",
                           f"joined clip lasts {joined_seconds:.4f} s; its pieces add up to "
                           f"{expected_seconds:.4f} s")
    try:
        meta = await video_input.remux_source_audio(joined, source_path(task_id), output)
        final_frames, _, _ = await packet_count(output)
    except VideoInputError as exc:
        raise UpscaleError("audio_remux_failed", str(exc)[-400:]) from None
    if final_frames != frames:
        raise UpscaleError("audio_remux_failed",
                           f"the finished clip has {final_frames} frames; expected {frames}")
    meta.update({"segments_int": count, "frames_int": frames})
    return meta


# ------------------------------------------------------------- housekeeping


def keep_for_diagnostics(task_id: str) -> Optional[Path]:
    """Move what was rendered out of the work directory before it is removed."""
    try:
        folder = work_dir(task_id)
    except ValueError:
        return None
    rendered = [folder / "joined.mp4"] if (folder / "joined.mp4").is_file() else sorted(folder.glob("out_*.mp4"))
    if not rendered:
        return None
    target = failed_root() / folder.name
    target.mkdir(parents=True, exist_ok=True)
    for item in rendered:
        shutil.move(str(item), str(target / item.name))
    return target


def sweep_aged(max_age_seconds: float = 3 * 86400, staging_age_seconds: float = 3600) -> int:
    """Remove failure diagnostics after three days and abandoned staging after an hour."""
    removed = 0
    now = time.time()
    for root, age in ((failed_root(), max_age_seconds), (staging_root(), staging_age_seconds)):
        if not root.is_dir():
            continue
        for item in root.iterdir():
            try:
                if now - item.stat().st_mtime < age:
                    continue
            except OSError:
                continue
            shutil.rmtree(item, ignore_errors=True) if item.is_dir() else item.unlink(missing_ok=True)
            removed += 1
    return removed


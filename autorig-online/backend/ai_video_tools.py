"""Video tools for /nodes: Scene split, Concat shots, Audio from source.

Three small ffmpeg jobs that let a graph handle a long multi-shot video inside
the editor instead of an external script (2026-09-27):

* scene split  - the reference video at 24 fps, cut where the picture changes
  (ffmpeg scene score, retried at lower thresholds when nothing is found), long
  shots split into chunks of at most `max_frames`. Every shot becomes a clip and
  a first frame at a public address; the editor runs the nodes wired after it
  once per shot (list sockets).
* concat       - per-shot clips back into one video in shot order: for every
  shot the first clip that exists among the wired candidates, trimmed to the
  shot's length, one size, one frame rate. A shot with no clip keeps the source
  shot so the timing never drifts.
* audio mux    - the original audio under the new picture.

Jobs are asynchronous: POST answers at once with `task_id_string`, the editor
polls `GET /api/ai/video-tools/status/{task_id}`. A job is keyed by its request,
so the same request is answered from the finished result.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

WORK_ROOT = Path(os.getenv("AUTORIG_AI_VIDEO_TOOLS_DIR", "/srv/autorig/data/var/ai-video-tools"))
PUBLISH_URL = os.getenv("AUTORIG_SCRATCH_UPLOAD_URL", "https://autorig.online/dev/api/scratch")
FFMPEG = os.getenv("RENDERFIN_FFMPEG_BIN", "ffmpeg")
FFPROBE = os.getenv("RENDERFIN_FFPROBE_BIN", "ffprobe")
MAX_SOURCE_BYTES = 300 * 1024 * 1024
MAX_SOURCE_SECONDS = 600.0
JOB_SECONDS = 1800.0
_ALLOWED_HOSTS = re.compile(
    r"(autorig\.online|image\.civitai\.(com|red|green)|civitai\.(com|red|green)|www\.civitai\.com|"
    r"blobs-b2\.civitai\.com|pvs[1-9]\.microstock\.plus)$", re.I)
_JOBS: Dict[str, Dict[str, Any]] = {}
_SEM = asyncio.Semaphore(2)


class VideoToolError(RuntimeError):
    pass


# ------------------------------------------------------------------ helpers

async def _run(*argv: str, timeout: float = 900.0, cwd: Optional[Path] = None) -> str:
    proc = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE,
                                                cwd=str(cwd) if cwd else None)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise VideoToolError(f"{Path(argv[0]).name} timed out")
    if proc.returncode:
        raise VideoToolError(f"{Path(argv[0]).name} failed: " + err.decode("utf-8", "replace")[-600:])
    return out.decode("utf-8", "replace") + err.decode("utf-8", "replace")


async def _ff(*args: str) -> str:
    return await _run(FFMPEG, "-hide_banner", "-loglevel", "info", "-nostdin", "-y", *args)


async def _probe(path: Path) -> Dict[str, Any]:
    raw = await _run(FFPROBE, "-v", "error", "-show_entries",
                     "format=duration:stream=codec_type,width,height", "-of", "json", str(path))
    data = json.loads(raw[raw.index("{"):])
    streams = data.get("streams") or []
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    if not video:
        raise VideoToolError("no video stream")
    return {"width": int(video.get("width") or 0), "height": int(video.get("height") or 0),
            "duration": float((data.get("format") or {}).get("duration") or 0),
            "audio": any(s.get("codec_type") == "audio" for s in streams)}


async def _resolve(url: str) -> str:
    url = str(url or "").strip()
    try:
        import civitai_media
        if civitai_media.is_civitai(url):
            return str((await civitai_media.resolve(url))["url"])
    except HTTPException:
        raise
    except Exception as exc:  # resolver refused: say so
        if "civitai" in url:
            raise VideoToolError(f"Civitai link did not resolve: {exc}")
    return url


async def _download(client: httpx.AsyncClient, url: str, target: Path) -> Path:
    url = await _resolve(url)
    parts = urlsplit(url)
    host = (parts.hostname or "").lower().rstrip(".")
    if parts.scheme != "https" or not _ALLOWED_HOSTS.search(host) or parts.port or parts.username:
        raise VideoToolError(f"video host is not allowed: {host or url[:60]}")
    headers = {}
    try:
        from renderfin.video_input import _civitai_headers
        headers = _civitai_headers(url)
    except Exception:
        headers = {}
    total = 0
    async with client.stream("GET", url, headers=headers, follow_redirects=True, timeout=120) as response:
        if response.status_code != 200:
            raise VideoToolError(f"download failed: HTTP {response.status_code} for {url[:120]}")
        with target.open("wb") as handle:
            async for chunk in response.aiter_bytes(1 << 20):
                total += len(chunk)
                if total > MAX_SOURCE_BYTES:
                    raise VideoToolError("video exceeds 300 MB")
                handle.write(chunk)
    return target


async def _publish(client: httpx.AsyncClient, path: Path, mime: str) -> str:
    for attempt in range(3):
        try:
            with path.open("rb") as handle:
                response = await client.post(PUBLISH_URL, files={"file": (path.name, handle, mime)}, timeout=300)
            data = response.json()
            if data.get("url"):
                return str(data["url"])
            raise VideoToolError(f"publish refused: {str(data)[:200]}")
        except (httpx.HTTPError, ValueError) as exc:
            if attempt == 2:
                raise VideoToolError(f"publish failed: {exc}")
            await asyncio.sleep(2 + attempt * 3)
    raise VideoToolError("publish failed")


def _even(value: int) -> int:
    return max(2, int(value) // 2 * 2)


# ------------------------------------------------------------- scene split

class SceneSplitRequest(BaseModel):
    video_url: str = Field(..., min_length=8, max_length=4096)
    # Adaptive cut detection: a cut is a frame whose change score stands out
    # from the clip's own typical change (robust z over median/MAD) AND from its
    # neighbourhood (ratio to the local mean). No fixed global threshold.
    sensitivity: float = Field(12.0, ge=2.0, le=60.0, description="robust z a cut must reach (lower = more cuts)")
    local_ratio: float = Field(6.0, ge=1.5, le=50.0, description="score / mean of the +-0.5 s neighbourhood")
    max_frames: int = Field(97, ge=9, le=393)
    fps: int = Field(24, ge=8, le=60)
    min_shot_seconds: float = Field(0.5, ge=0.0, le=10.0)
    max_shots: int = Field(32, ge=1, le=64)
    max_seconds: float = Field(0.0, ge=0.0, le=MAX_SOURCE_SECONDS)


async def _frame_scores(path: Path, work: Path) -> List[float]:
    """lavfi.scene_score of every frame (frame 0 scores 0)."""
    out = work / "scores.txt"
    await _run(FFMPEG, "-hide_banner", "-nostdin", "-i", str(path.resolve()), "-vf",
               f"select='gte(scene,0)',metadata=print:key=lavfi.scene_score:file={out.name}",
               "-an", "-f", "null", "-", cwd=work)
    text = out.read_text(encoding="utf-8", errors="replace") if out.is_file() else ""
    return [float(m) for m in re.findall(r"lavfi\.scene_score=([0-9.eE+-]+)", text)]


def _median(values: List[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def detect_cuts(scores: List[float], fps: int, sensitivity: float, local_ratio: float,
                min_shot_frames: int) -> Dict[str, Any]:
    """Cut frames where the change jumps relative to the clip and to its neighbourhood."""
    body = scores[1:]
    if len(body) < 3:
        return {"cuts": [], "median": 0.0, "mad": 0.0, "candidates": []}
    med = _median(body)
    mad = max(1.4826 * _median([abs(x - med) for x in body]), 1e-3)
    window = max(2, int(round(fps * 0.5)))
    candidates = []
    for i in range(1, len(scores)):
        near = [scores[j] for j in range(max(1, i - window), min(len(scores), i + window + 1)) if j != i]
        local = sum(near) / len(near) if near else 0.0
        z = (scores[i] - med) / mad
        ratio = scores[i] / max(local, 1e-4)
        if z >= sensitivity and ratio >= local_ratio:
            candidates.append({"frame": i, "score": round(scores[i], 4), "z": round(z, 1), "ratio": round(ratio, 1)})
    accepted: List[int] = []
    total = len(scores)
    for cand in sorted(candidates, key=lambda c: -c["score"]):
        f = cand["frame"]
        if f < min_shot_frames or total - f < min_shot_frames:
            continue
        if any(abs(f - other) < min_shot_frames for other in accepted):
            continue
        accepted.append(f)
    return {"cuts": sorted(accepted), "median": round(med, 5), "mad": round(mad, 5), "candidates": candidates[:40]}


def _label_font(size: int):
    from PIL import ImageFont
    for name in ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "DejaVuSans-Bold.ttf", "arialbd.ttf"):
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _timecode(seconds: float) -> str:
    return f"{int(seconds // 60):02d}:{seconds % 60:05.2f}"


def _compose_storyboard(tiles: List[Path], labels: List[str], header: str, target: Path) -> None:
    from PIL import Image, ImageDraw
    images = [Image.open(p).convert("RGB") for p in tiles]
    tw = 384
    th = max(1, int(round(images[0].height * tw / images[0].width)))
    cols = len(images) if len(images) <= 4 else math.ceil(math.sqrt(len(images) * 1.5))
    cols = max(1, min(cols, 8))
    rows = math.ceil(len(images) / cols)
    label_h, gap, head_h = 28, 8, 40
    sheet = Image.new("RGB", (cols * tw + (cols + 1) * gap, head_h + rows * (th + label_h + gap) + gap), (18, 18, 24))
    draw = ImageDraw.Draw(sheet)
    draw.text((gap, 8), header, fill=(255, 210, 90), font=_label_font(22))
    small = _label_font(14)
    for index, (image, label) in enumerate(zip(images, labels)):
        r, c = divmod(index, cols)
        x = gap + c * (tw + gap)
        y = head_h + r * (th + label_h + gap)
        sheet.paste(image.resize((tw, th)), (x, y + label_h))
        draw.text((x + 2, y + 6), label, fill=(235, 235, 235), font=small)
    sheet.save(target, "PNG", optimize=True)


async def split_reference(ref: Path, work: Path, body: SceneSplitRequest, info: Dict[str, Any]) -> Dict[str, Any]:
    """Cuts, scenes, parts and the storyboard of a 24 fps reference (local files only)."""
    scores = await _frame_scores(ref, work)
    total_frames = len(scores) or max(1, int(round(info["duration"] * body.fps)))
    min_frames = max(1, int(round(body.min_shot_seconds * body.fps)))
    found = detect_cuts(scores, body.fps, body.sensitivity, body.local_ratio, min_frames)
    cut_frames = found["cuts"]
    edges = [0] + cut_frames + [total_frames]
    scenes = [{"scene": i, "start_frame": a, "end_frame": b} for i, (a, b) in enumerate(zip(edges, edges[1:]))]
    segments = []
    for sc in scenes:
        length = sc["end_frame"] - sc["start_frame"]
        parts = max(1, math.ceil(length / body.max_frames))
        step = length / parts
        for part in range(parts):
            a = sc["start_frame"] + int(round(part * step))
            b = sc["start_frame"] + int(round((part + 1) * step))
            segments.append({"shot": sc["scene"], "part": part, "start_frame": a, "end_frame": b})
    tiles, labels = [], []
    for sc in scenes:
        mid = (sc["start_frame"] + sc["end_frame"] - 1) // 2
        sc["middle_frame"] = mid
        tile = work / f"mid{sc['scene']:03d}.png"
        await _ff("-i", str(ref), "-vf", f"trim=start_frame={mid}:end_frame={mid + 1}", "-frames:v", "1", str(tile))
        tiles.append(tile)
        labels.append(f"Scene {sc['scene'] + 1} · {_timecode(sc['start_frame'] / body.fps)}-"
                      f"{_timecode(sc['end_frame'] / body.fps)} · f{sc['start_frame']}-{sc['end_frame'] - 1}")
    duration = total_frames / body.fps
    header = (f"{len(scenes)} scene{'s' if len(scenes) != 1 else ''} · {total_frames} frames · "
              f"{body.fps} fps · {duration:.2f} s")
    board = work / "storyboard.png"
    _compose_storyboard(tiles, labels, header, board)
    return {"scores": scores, "total_frames": total_frames, "duration": duration, "found": found,
            "cut_frames": cut_frames, "scenes": scenes, "segments": segments[:body.max_shots],
            "tiles": tiles, "header": header, "board": board}


async def _scene_split(body: SceneSplitRequest, work: Path) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        raw = await _download(client, body.video_url, work / "source.bin")
        trim = ["-t", f"{body.max_seconds:.3f}"] if body.max_seconds else []
        ref = work / "reference.mp4"
        info = await _probe(raw)
        if info["duration"] > MAX_SOURCE_SECONDS and not body.max_seconds:
            raise VideoToolError("video is longer than 10 minutes; set max_seconds")
        audio = ["-c:a", "aac", "-b:a", "192k"] if info["audio"] else ["-an"]
        await _ff("-i", str(raw), *trim, "-r", str(body.fps), "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
                  "-c:v", "libx264", "-preset", "veryfast", "-crf", "16", "-pix_fmt", "yuv420p", *audio,
                  "-movflags", "+faststart", str(ref))
        info = await _probe(ref)
        plan = await split_reference(ref, work, body, info)
        scenes, segments, cut_frames = plan["scenes"], plan["segments"], plan["cut_frames"]
        total_frames, duration, header = plan["total_frames"], plan["duration"], plan["header"]
        source_url = await _publish(client, ref, "video/mp4")
        storyboard_url = await _publish(client, plan["board"], "image/png")
        for sc, tile in zip(scenes, plan["tiles"]):
            sc["middle_frame_url"] = await _publish(client, tile, "image/png")
        items = []
        for index, seg in enumerate(segments):
            frames = seg["end_frame"] - seg["start_frame"]
            start, end = seg["start_frame"] / body.fps, seg["end_frame"] / body.fps
            clip = work / f"shot{index:03d}.mp4"
            await _ff("-i", str(ref), "-vf", f"trim=start_frame={seg['start_frame']}:end_frame={seg['end_frame']},"
                      "setpts=PTS-STARTPTS", "-an", "-c:v", "libx264", "-preset", "veryfast", "-crf", "16",
                      "-pix_fmt", "yuv420p", "-r", str(body.fps), "-movflags", "+faststart", str(clip))
            first = work / f"shot{index:03d}.png"
            await _ff("-i", str(clip), "-frames:v", "1", str(first))
            clip_url = await _publish(client, clip, "video/mp4")
            first_url = await _publish(client, first, "image/png")
            label = (f"Scene {seg['shot'] + 1} of {len(scenes)}" + (f" · part {seg['part'] + 1}" if seg["part"] else "") +
                     f" · {_timecode(start)}-{_timecode(end)} · frames {seg['start_frame']}-{seg['end_frame'] - 1}"
                     f" ({frames})")
            items.append({"index": index, "shot": seg["shot"], "part": seg["part"],
                          "start_frame": seg["start_frame"], "end_frame": seg["end_frame"],
                          "start": round(start, 3), "end": round(end, 3), "seconds": round(end - start, 3),
                          "frames": frames, "clip_url": clip_url, "first_frame_url": first_url,
                          "middle_frame_url": scenes[seg["shot"]].get("middle_frame_url", ""), "label": label})
    lines = [f"{header}. Detected cuts at: " + (", ".join(f"frame {f} ({_timecode(f / body.fps)})" for f in cut_frames)
                                                or "none (one continuous scene)") + "."]
    for sc in scenes:
        lines.append(f"Scene {sc['scene'] + 1}: frames {sc['start_frame']}-{sc['end_frame'] - 1} "
                     f"({_timecode(sc['start_frame'] / body.fps)}-{_timecode(sc['end_frame'] / body.fps)}, "
                     f"{(sc['end_frame'] - sc['start_frame']) / body.fps:.2f} s); storyboard tile {sc['scene'] + 1} "
                     f"is its middle frame.")
    found = plan["found"]
    return {"shots_array": items, "count_int": len(items), "scenes_int": len(scenes),
            "scenes_array": scenes, "cut_frames_array": cut_frames,
            "cuts_array": [round(f / body.fps, 3) for f in cut_frames],
            "detector_object": {"median": found["median"], "mad": found["mad"], "sensitivity": body.sensitivity,
                                "local_ratio": body.local_ratio, "candidates": found["candidates"]},
            "frames_int": total_frames, "duration_float": round(duration, 3), "fps_int": body.fps,
            "width_int": info["width"], "height_int": info["height"], "has_audio_bool": info["audio"],
            "source_video_url_string": source_url, "storyboard_url_string": storyboard_url,
            "scenes_text_string": "\n".join(lines),
            "shots_json_string": json.dumps({"fps": body.fps, "frames": total_frames, "duration": round(duration, 3),
                                             "shots": [{k: item[k] for k in ("index", "shot", "part", "start_frame",
                                                                             "end_frame", "start", "end", "seconds",
                                                                             "frames", "clip_url")}
                                                       for item in items]}, separators=(",", ":"))}


# ------------------------------------------------------------------ concat

def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    return [str(item or "") for item in value]


class ConcatRequest(BaseModel):
    # Candidate lists in priority order (clip, clip_2, clip_3): for shot i the
    # first non-empty clip wins. `clips` is the same as a list of lists.
    clip: Any = None
    clip_2: Any = None
    clip_3: Any = None
    clips: List[List[str]] = Field(default_factory=list, max_length=4)
    shots_json: str = Field("", max_length=200000)
    out_width: int = Field(0, ge=0, le=4096)
    out_height: int = Field(0, ge=0, le=4096)
    fps: int = Field(24, ge=8, le=60)
    trim: bool = True

    def columns(self) -> List[List[str]]:
        cols = [_as_list(self.clip), _as_list(self.clip_2), _as_list(self.clip_3)] + [list(c) for c in self.clips]
        return [c for c in cols if c]


async def _concat(body: ConcatRequest, work: Path) -> Dict[str, Any]:
    shots: List[Dict[str, Any]] = []
    fps = body.fps
    if body.shots_json.strip():
        try:
            data = json.loads(body.shots_json)
            shots = list(data.get("shots") or [])
            fps = int(data.get("fps") or fps)
        except (ValueError, AttributeError, TypeError) as exc:
            raise VideoToolError(f"shots_json is not the Scene split list: {exc}")
    columns = body.columns()
    count = max([len(shots)] + [len(column) for column in columns])
    if not count:
        raise VideoToolError("nothing to join")
    chosen: List[Dict[str, Any]] = []
    for index in range(count):
        pick, source = "", ""
        for rank, column in enumerate(columns):
            value = str((column[index] if index < len(column) else "") or "").strip()
            if value:
                pick, source = value, f"input {rank + 1}"
                break
        shot = shots[index] if index < len(shots) else {}
        if not pick and shot.get("clip_url"):
            pick, source = str(shot["clip_url"]), "source shot (no clip arrived)"
        if not pick:
            raise VideoToolError(f"shot {index + 1} has no clip")
        chosen.append({"url": pick, "source": source, "frames": int(shot.get("frames") or 0)})
    async with httpx.AsyncClient() as client:
        local = []
        for index, item in enumerate(chosen):
            path = await _download(client, item["url"], work / f"in{index:03d}.mp4")
            local.append(path)
        width, height = body.out_width, body.out_height
        if not (width and height):
            first = await _probe(local[0])
            width, height = first["width"], first["height"]
        width, height = _even(width), _even(height)
        parts = []
        report = []
        for index, (item, path) in enumerate(zip(chosen, local)):
            out = work / f"norm{index:03d}.mp4"
            frames = item["frames"] if body.trim and item["frames"] else 0
            vf = (f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
                  f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={fps}")
            if frames:
                # Hold the last frame when a clip is shorter than its shot.
                vf += f",tpad=stop_mode=clone:stop={frames}"
            args = ["-i", str(path), "-an", "-vf", vf]
            if frames:
                args += ["-frames:v", str(frames)]
            await _ff(*args, "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p",
                      "-r", str(fps), str(out))
            parts.append(out)
            report.append({"shot": index + 1, "from": item["source"], "frames": frames or None})
        listing = work / "list.txt"
        listing.write_text("".join(f"file '{p.as_posix()}'\n" for p in parts), encoding="utf-8")
        joined = work / "joined.mp4"
        await _ff("-f", "concat", "-safe", "0", "-i", str(listing), "-c", "copy", "-movflags", "+faststart",
                  str(joined))
        info = await _probe(joined)
        url = await _publish(client, joined, "video/mp4")
    return {"video_url_string": url, "width_int": width, "height_int": height, "fps_int": fps,
            "duration_float": round(info["duration"], 3), "shots_array": report}


# --------------------------------------------------------------- audio mux

class AudioMuxRequest(BaseModel):
    video_url: str = Field(..., min_length=8, max_length=4096)
    source_url: str = Field(..., min_length=8, max_length=4096)


async def _audio_mux(body: AudioMuxRequest, work: Path) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        video = await _download(client, body.video_url, work / "video.mp4")
        source = await _download(client, body.source_url, work / "source.bin")
        vinfo, sinfo = await _probe(video), await _probe(source)
        out = work / "with_audio.mp4"
        if sinfo["audio"]:
            await _ff("-i", str(video), "-i", str(source), "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy",
                      "-c:a", "aac", "-b:a", "192k", "-t", f"{vinfo['duration']:.3f}", "-movflags", "+faststart",
                      str(out))
        else:
            shutil.copyfile(video, out)
        url = await _publish(client, out, "video/mp4")
    return {"video_url_string": url, "had_audio_bool": sinfo["audio"], "duration_float": round(vinfo["duration"], 3)}


# -------------------------------------------------------------------- jobs

def _job_id(kind: str, payload: Dict[str, Any]) -> str:
    raw = json.dumps({"kind": kind, "body": payload, "v": 1}, sort_keys=True, separators=(",", ":"))
    return "vt_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _result_path(job_id: str) -> Path:
    return WORK_ROOT / "results" / f"{job_id}.json"


def _public(job: Dict[str, Any]) -> Dict[str, Any]:
    out = {"task_id_string": job["id"], "status_string": job["status"], "kind_string": job["kind"],
           "error_string": job.get("error", ""), "elapsed_float": round(job.get("elapsed", 0.0), 1)}
    if job["status"] == "completed":
        out.update(job.get("result") or {})
        out["cache_hit_bool"] = bool(job.get("cached"))
    return out


async def _execute(job: Dict[str, Any], worker, body) -> None:
    started = time.time()
    work = Path(tempfile.mkdtemp(prefix=job["kind"] + "-", dir=WORK_ROOT / "work"))
    try:
        async with _SEM:
            job["status"] = "processing"
            result = await asyncio.wait_for(worker(body, work), JOB_SECONDS)
        job.update(status="completed", result=result, elapsed=time.time() - started)
        _result_path(job["id"]).write_text(json.dumps(job, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:  # the node shows this text
        job.update(status="failed", error=str(exc)[:1200] or exc.__class__.__name__, elapsed=time.time() - started)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _submit(kind: str, body: BaseModel, worker) -> Dict[str, Any]:
    (WORK_ROOT / "work").mkdir(parents=True, exist_ok=True)
    (WORK_ROOT / "results").mkdir(parents=True, exist_ok=True)
    job_id = _job_id(kind, body.model_dump())
    job = _JOBS.get(job_id)
    if job and job["status"] in ("queued", "processing", "completed"):
        return _public(job)
    stored = _result_path(job_id)
    if stored.is_file():
        try:
            job = json.loads(stored.read_text(encoding="utf-8"))
            job["cached"] = True
            _JOBS[job_id] = job
            return _public(job)
        except ValueError:
            pass
    job = {"id": job_id, "kind": kind, "status": "queued", "created": time.time()}
    _JOBS[job_id] = job
    asyncio.get_running_loop().create_task(_execute(job, worker, body))
    if len(_JOBS) > 500:
        for key in sorted(_JOBS, key=lambda k: _JOBS[k].get("created", 0))[:100]:
            if _JOBS[key]["status"] in ("completed", "failed"):
                _JOBS.pop(key, None)
    return _public(job)


@router.post("/api/ai/video-tools/scene-split")
async def api_scene_split(body: SceneSplitRequest):
    return _submit("scene_split", body, _scene_split)


@router.post("/api/ai/video-tools/concat")
async def api_concat(body: ConcatRequest):
    return _submit("video_concat", body, _concat)


@router.post("/api/ai/video-tools/audio-mux")
async def api_audio_mux(body: AudioMuxRequest):
    return _submit("audio_mux", body, _audio_mux)


@router.get("/api/ai/video-tools/status/{task_id}")
async def api_video_tools_status(task_id: str):
    if not re.fullmatch(r"vt_[a-f0-9]{24}", task_id or ""):
        raise HTTPException(status_code=404, detail="unknown task")
    job = _JOBS.get(task_id)
    if not job:
        stored = _result_path(task_id)
        if stored.is_file():
            job = json.loads(stored.read_text(encoding="utf-8"))
        else:
            raise HTTPException(status_code=404, detail={"error_string": "unknown_task",
                                                          "message_string": "task not found (server restarted?) — Render again"})
    return _public(job)


@router.get("/api/ai/video-tools")
async def api_video_tools_docs():
    return {"status_string": "ok", "endpoints_array": [
        {"POST": "/api/ai/video-tools/scene-split",
         "fields": "video_url; sensitivity 12 (robust z over median/MAD of per-frame scene scores); local_ratio 6 "
                   "(score / mean of +-0.5 s); max_frames 97; fps 24; "
                   "min_shot_seconds 0.5; max_shots 32; max_seconds 0 = whole video",
         "result": "shots_array [{index, shot, part, start_frame, end_frame, start, end, frames, clip_url, "
                   "first_frame_url, middle_frame_url, label}], scenes_int, scenes_array, cut_frames_array, frames_int, "
                   "storyboard_url_string (middle frame of every scene), scenes_text_string, shots_json_string, "
                   "source_video_url_string (24 fps reference with its audio)"},
        {"POST": "/api/ai/video-tools/concat",
         "fields": "clip, clip_2, clip_3 (per-shot URL lists in priority order), shots_json (from scene split), out_width, out_height, "
                   "fps 24, trim true", "result": "video_url_string"},
        {"POST": "/api/ai/video-tools/audio-mux", "fields": "video_url, source_url", "result": "video_url_string"},
        {"GET": "/api/ai/video-tools/status/{task_id}"}]}

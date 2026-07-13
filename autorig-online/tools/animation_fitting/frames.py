from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from typing import Any, Optional

import numpy as np

from .errors import ContractError, DependencyUnavailableError


FRAMES_MANIFEST_SCHEMA = "autorig-extracted-frames.v1"


@dataclass(frozen=True)
class ExtractedFrames:
    directory: Path
    manifest_path: Path
    frame_count: int
    width: int
    height: int
    fps: float
    files: tuple[Path, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _resolve_executable(value: Optional[str], name: str, *, sibling_of: Optional[Path] = None) -> Path:
    candidates: list[Path] = []
    if value:
        candidates.append(Path(value).expanduser())
    elif sibling_of is not None:
        suffix = ".exe" if sibling_of.suffix.lower() == ".exe" else ""
        candidates.append(sibling_of.with_name(name + suffix))
    located = shutil.which(name)
    if located:
        candidates.append(Path(located))
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved
    flag = f"--{name}"
    raise DependencyUnavailableError(
        f"{name} was not found. Install FFmpeg and expose {name} on PATH, or pass "
        f"{flag} C:\\path\\to\\{name}.exe explicitly."
    )


def _run(command: list[str], *, label: str) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
    except OSError as exc:
        raise DependencyUnavailableError(f"Cannot execute {label}: {exc}") from exc
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout or "").strip()
        if len(details) > 4000:
            details = details[-4000:]
        raise ContractError(f"{label} failed with exit code {completed.returncode}:\n{details}")
    return completed


def _fraction(value: Any, field: str) -> float:
    if not isinstance(value, str) or "/" not in value:
        raise ContractError(f"ffprobe returned invalid {field}: {value!r}")
    numerator, denominator = value.split("/", 1)
    try:
        result = float(numerator) / float(denominator)
    except (ValueError, ZeroDivisionError) as exc:
        raise ContractError(f"ffprobe returned invalid {field}: {value!r}") from exc
    if not np.isfinite(result) or result <= 0:
        raise ContractError(f"ffprobe returned invalid {field}: {value!r}")
    return result


def _probe(ffprobe: Path, source: Path) -> dict:
    command = [
        str(ffprobe),
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height,avg_frame_rate,r_frame_rate,duration,nb_frames",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(source),
    ]
    completed = _run(command, label="ffprobe")
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ContractError(f"ffprobe returned invalid JSON: {exc}") from exc
    streams = payload.get("streams") if isinstance(payload, dict) else None
    if not isinstance(streams, list) or len(streams) != 1:
        raise ContractError("Input must contain exactly one selected video stream")
    stream = streams[0]
    try:
        width, height = int(stream["width"]), int(stream["height"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ContractError("ffprobe did not return valid video dimensions") from exc
    if width <= 0 or height <= 0:
        raise ContractError("Input video dimensions must be positive")
    rate_value = stream.get("avg_frame_rate")
    if rate_value in (None, "0/0"):
        rate_value = stream.get("r_frame_rate")
    return {
        "width": width,
        "height": height,
        "fps": _fraction(rate_value, "frame rate"),
        "raw": payload,
        "command": command,
    }


def extract_frames(
    video_path: str | Path,
    output_dir: str | Path,
    *,
    ffmpeg: Optional[str] = None,
    ffprobe: Optional[str] = None,
    fps: Optional[float] = None,
    overwrite: bool = False,
) -> ExtractedFrames:
    source = Path(video_path).resolve()
    if not source.is_file():
        raise ContractError(f"Video does not exist: {source}")
    ffmpeg_path = _resolve_executable(ffmpeg, "ffmpeg")
    ffprobe_path = _resolve_executable(ffprobe, "ffprobe", sibling_of=ffmpeg_path)
    probe = _probe(ffprobe_path, source)
    target_fps = probe["fps"] if fps is None else float(fps)
    if not np.isfinite(target_fps) or target_fps <= 0:
        raise ContractError("Requested fps must be a finite positive number")

    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    existing = sorted(destination.glob("frame_*.png"))
    if existing and not overwrite:
        raise ContractError(
            f"Output already contains {len(existing)} frame_*.png files. "
            "Choose an empty directory or pass --overwrite."
        )
    if overwrite:
        for path in existing:
            if path.is_file() and path.parent == destination:
                path.unlink()

    pattern = destination / "frame_%06d.png"
    command = [str(ffmpeg_path), "-hide_banner", "-loglevel", "error", "-i", str(source)]
    if fps is not None:
        command.extend(["-vf", f"fps={target_fps:.12g}"])
    else:
        command.extend(["-fps_mode", "passthrough"])
    command.extend(["-start_number", "0", str(pattern)])
    _run(command, label="ffmpeg frame extraction")
    frames = tuple(sorted(destination.glob("frame_*.png")))
    if not frames:
        raise ContractError("ffmpeg completed but produced no frames")
    expected_names = [f"frame_{index:06d}.png" for index in range(len(frames))]
    if [path.name for path in frames] != expected_names:
        raise ContractError("Extracted frame sequence is not contiguous from frame 0")

    try:
        from PIL import Image
    except ImportError as exc:
        raise DependencyUnavailableError(
            "Pillow is required to verify extracted frames. Install it with: python -m pip install Pillow"
        ) from exc
    for path in frames:
        with Image.open(path) as image:
            if image.size != (probe["width"], probe["height"]):
                raise ContractError(
                    f"Extracted frame {path.name} is {image.size}, expected "
                    f"{probe['width']}x{probe['height']}"
                )

    manifest = {
        "schema": FRAMES_MANIFEST_SCHEMA,
        "source_video": str(source),
        "source_sha256": _sha256(source),
        "frame_count": len(frames),
        "width": probe["width"],
        "height": probe["height"],
        "fps": target_fps,
        "ffmpeg": str(ffmpeg_path),
        "ffprobe": str(ffprobe_path),
        "extraction_command": command,
        "files": [path.name for path in frames],
    }
    manifest_path = destination / "frames_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return ExtractedFrames(
        directory=destination,
        manifest_path=manifest_path,
        frame_count=len(frames),
        width=probe["width"],
        height=probe["height"],
        fps=target_fps,
        files=frames,
    )

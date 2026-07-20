from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import subprocess
import tempfile
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from .comfy import worker_from_environment
from .orchestrator import AnimationFittingOrchestrator
from .storage import FfmpegFrameExtractor, ImmutableArtifactStore


def ffprobe_video(ffprobe_path: Path, video_path: Path) -> Dict[str, Any]:
    completed = subprocess.run(
        [
            str(ffprobe_path),
            "-v",
            "error",
            "-count_frames",
            "-show_entries",
            (
                "stream=index,codec_name,codec_type,width,height,r_frame_rate,avg_frame_rate,"
                "nb_frames,nb_read_frames,duration:"
                "format=filename,format_name,duration,size,bit_rate"
            ),
            "-of",
            "json",
            str(video_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {completed.stderr[-3000:]}")
    parsed = json.loads(completed.stdout)
    if not isinstance(parsed, dict):
        raise RuntimeError("ffprobe returned a non-object payload")
    return parsed


def validate_ffprobe_video(probe: Dict[str, Any], *, expected_frame_count: int) -> Dict[str, Any]:
    streams = probe.get("streams")
    if not isinstance(streams, list):
        raise RuntimeError("ffprobe payload has no streams array")
    video_streams = [stream for stream in streams if stream.get("codec_type") == "video"]
    if len(video_streams) != 1:
        raise RuntimeError(f"Expected exactly one video stream, found {len(video_streams)}")
    stream = video_streams[0]
    if stream.get("codec_name") != "h264":
        raise RuntimeError(f"Expected H.264 video, found {stream.get('codec_name')!r}")
    frame_rate = Fraction(str(stream.get("avg_frame_rate") or stream.get("r_frame_rate") or "0/1"))
    if frame_rate != Fraction(30, 1):
        raise RuntimeError(f"Expected 30 fps, found {frame_rate}")
    decoded_frames = int(stream.get("nb_read_frames") or stream.get("nb_frames") or 0)
    if decoded_frames != int(expected_frame_count):
        raise RuntimeError(
            f"Expected {expected_frame_count} ffprobe frames, found {decoded_frames}"
        )
    return {
        "video_codec_string": "h264",
        "video_fps_int": 30,
        "video_frame_count_int": decoded_frames,
        "video_width_int": int(stream.get("width") or 0),
        "video_height_int": int(stream.get("height") or 0),
    }


def create_contact_sheet(
    ffmpeg_path: Path,
    video_path: Path,
    *,
    artifact_root: Path,
    raw_video_sha256: str,
) -> Dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="autorig-fitting-contact-sheet-") as temp_dir:
        temp_output = Path(temp_dir) / "contact_sheet.jpg"
        completed = subprocess.run(
            [
                str(ffmpeg_path),
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(video_path),
                "-vf",
                "select=not(mod(n\\,5)),scale=320:-1:flags=lanczos,tile=5x2:padding=4:margin=4",
                "-frames:v",
                "1",
                "-q:v",
                "2",
                str(temp_output),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0 or not temp_output.is_file():
            raise RuntimeError(f"ffmpeg contact sheet failed: {completed.stderr[-3000:]}")
        payload = temp_output.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    qa_dir = Path(artifact_root).resolve() / "qa" / raw_video_sha256 / "contact_sheet"
    qa_dir.mkdir(parents=True, exist_ok=True)
    output = qa_dir / f"{digest}.jpg"
    try:
        with output.open("xb") as handle:
            handle.write(payload)
            handle.flush()
    except FileExistsError:
        if hashlib.sha256(output.read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"Immutable contact sheet collision: {output}")
    return {
        "contact_sheet_path_string": str(output),
        "contact_sheet_sha256_string": digest,
    }


async def run_smoke(args: argparse.Namespace) -> Dict[str, Any]:
    store = ImmutableArtifactStore(args.artifact_root)
    worker = worker_from_environment("loop")
    orchestrator = AnimationFittingOrchestrator(
        store,
        frame_extractor=FfmpegFrameExtractor(str(args.ffmpeg)),
    )
    result = await orchestrator.run_candidate(
        task_id=args.task_id,
        action_id=args.action,
        candidate_index=args.candidate_index,
        species="horse",
        reference_frame_path=args.reference,
        worker=worker,
        motion_notes=args.motion_notes,
    )
    probe = await asyncio.to_thread(ffprobe_video, args.ffprobe, result.raw_video.path)
    probe_summary = validate_ffprobe_video(probe, expected_frame_count=len(result.frames))
    contact_sheet = await asyncio.to_thread(
        create_contact_sheet,
        args.ffmpeg,
        result.raw_video.path,
        artifact_root=args.artifact_root,
        raw_video_sha256=result.raw_video.sha256,
    )
    poster = result.frames[len(result.frames) // 2]
    verification = {
        "status_string": "smoke_verified_not_approved",
        "prompt_id_string": result.prompt_id,
        "seed_int": result.seed,
        "worker_base_url_string": result.worker_base_url,
        "workflow_name_string": result.workflow_name,
        "workflow_fingerprint_string": result.workflow_fingerprint,
        "raw_video_sha256_string": result.raw_video.sha256,
        "raw_video_path_string": str(result.raw_video.path),
        "decoded_frame_count_int": len(result.frames),
        "ffprobe_object": probe,
        "ffprobe_summary_object": probe_summary,
        "poster_frame_index_int": len(result.frames) // 2,
        "poster_frame_path_string": str(poster.path),
        "poster_frame_sha256_string": poster.sha256,
        **contact_sheet,
        "approved_bool": False,
    }
    store.append_job_state(result.job_id, verification)
    return {
        "ok_bool": True,
        "approved_bool": False,
        "job_id_string": result.job_id,
        "action_id_string": result.action_id,
        "candidate_index_int": result.candidate_index,
        **verification,
        "frame_root_string": str(result.frames[0].path.parent if result.frames else ""),
    }


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one non-approved local horse LTX loop smoke.")
    parser.add_argument("--reference", required=True, type=Path)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--ffmpeg", required=True, type=Path)
    parser.add_argument("--ffprobe", required=True, type=Path)
    parser.add_argument("--task-id", default="local-horse-loop-smoke-v1")
    parser.add_argument("--action", default="walk_forward")
    parser.add_argument("--candidate-index", type=int, default=0)
    parser.add_argument(
        "--motion-notes",
        default="minimal technical smoke; preserve the side-view low-poly horse identity",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    payload = asyncio.run(run_smoke(args))
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

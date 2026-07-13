from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Sequence


SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,160}$")


class ImmutableArtifactError(RuntimeError):
    """Raised when an immutable fitting artifact would be replaced or corrupted."""


class WorkerBusyError(RuntimeError):
    """Raised when another process owns the one-job-per-GPU lease."""


@dataclass(frozen=True)
class StoredArtifact:
    sha256: str
    path: Path
    size_bytes: int


class ImmutableArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.raw_root = self.root / "raw"
        self.frames_root = self.root / "frames"
        self.jobs_root = self.root / "jobs"
        self.worker_locks_root = self.root / "locks" / "workers"
        self.state_locks_root = self.root / "locks" / "states"

    def ensure(self) -> None:
        for path in (
            self.raw_root,
            self.frames_root,
            self.jobs_root,
            self.worker_locks_root,
            self.state_locks_root,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def store_raw_video(self, data: bytes) -> StoredArtifact:
        payload = bytes(data)
        if not payload:
            raise ImmutableArtifactError("Raw video is empty")
        digest = hashlib.sha256(payload).hexdigest()
        path = self.raw_root / digest[:2] / f"{digest}.mp4"
        self._write_immutable(path, payload)
        return StoredArtifact(sha256=digest, path=path, size_bytes=len(payload))

    def store_frame(self, video_sha256: str, frame_index: int, data: bytes) -> StoredArtifact:
        video_digest = _require_sha256(video_sha256, "video_sha256")
        index = int(frame_index)
        if index < 0:
            raise ImmutableArtifactError("frame_index must be non-negative")
        payload = bytes(data)
        if not payload:
            raise ImmutableArtifactError("Frame image is empty")
        frame_digest = hashlib.sha256(payload).hexdigest()
        path = self.frames_root / video_digest / f"frame_{index:06d}.png"
        self._write_immutable(path, payload)
        return StoredArtifact(sha256=frame_digest, path=path, size_bytes=len(payload))

    def append_job_state(self, job_id: str, payload: Mapping[str, Any]) -> Path:
        safe_job_id = _require_safe_id(job_id, "job_id")
        if not isinstance(payload, Mapping):
            raise ImmutableArtifactError("Job state payload must be an object")
        self.ensure()
        lock_path = self.state_locks_root / f"{hashlib.sha256(safe_job_id.encode()).hexdigest()}.lock"
        with self._exclusive_lock(lock_path, owner_id=f"state-{os.getpid()}", busy_error=ImmutableArtifactError):
            job_dir = self.jobs_root / safe_job_id
            job_dir.mkdir(parents=True, exist_ok=True)
            revisions = sorted(job_dir.glob("[0-9][0-9][0-9][0-9][0-9][0-9].json"))
            sequence = int(revisions[-1].stem) + 1 if revisions else 1
            record = {
                "schema": "autorig.animation-fitting-job-state.v1",
                "sequence_int": sequence,
                "recorded_at_unix_float": time.time(),
                **dict(payload),
            }
            encoded = (json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
            path = job_dir / f"{sequence:06d}.json"
            self._write_exclusive(path, encoded)
            return path

    def latest_job_state(self, job_id: str) -> Optional[Dict[str, Any]]:
        safe_job_id = _require_safe_id(job_id, "job_id")
        job_dir = self.jobs_root / safe_job_id
        revisions = sorted(job_dir.glob("[0-9][0-9][0-9][0-9][0-9][0-9].json"))
        if not revisions:
            return None
        parsed = json.loads(revisions[-1].read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ImmutableArtifactError(f"Invalid job state: {revisions[-1]}")
        return parsed

    @contextmanager
    def worker_lease(self, worker_key: str, owner_id: str) -> Iterator[Path]:
        key = str(worker_key or "").strip()
        if not key:
            raise ImmutableArtifactError("worker_key is required")
        owner = _require_safe_id(owner_id, "owner_id")
        self.ensure()
        lock_path = self.worker_locks_root / f"{hashlib.sha256(key.encode('utf-8')).hexdigest()}.lock"
        with self._exclusive_lock(lock_path, owner_id=owner, busy_error=WorkerBusyError):
            yield lock_path

    @contextmanager
    def _exclusive_lock(self, path: Path, *, owner_id: str, busy_error: type[RuntimeError]) -> Iterator[None]:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {"owner_id_string": owner_id, "pid_int": os.getpid(), "created_at_unix_float": time.time()},
            sort_keys=True,
        ).encode("utf-8")
        try:
            self._write_exclusive(path, payload)
        except FileExistsError as exc:
            current = ""
            try:
                current = path.read_text(encoding="utf-8")[:500]
            except OSError:
                pass
            raise busy_error(f"Lock is already held: {path} {current}") from exc
        try:
            yield
        finally:
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(current, dict) and current.get("owner_id_string") == owner_id:
                    path.unlink(missing_ok=True)
            except (OSError, json.JSONDecodeError):
                pass

    @staticmethod
    def _write_exclusive(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

    def _write_immutable(self, path: Path, payload: bytes) -> None:
        try:
            self._write_exclusive(path, payload)
            return
        except FileExistsError:
            existing = path.read_bytes()
            if hashlib.sha256(existing).digest() != hashlib.sha256(payload).digest():
                raise ImmutableArtifactError(f"Immutable artifact collision: {path}")


class FfmpegFrameExtractor:
    def __init__(self, ffmpeg_path: str = "ffmpeg") -> None:
        self.ffmpeg_path = str(ffmpeg_path or "ffmpeg")

    def extract_and_store(
        self,
        raw_video: StoredArtifact,
        store: ImmutableArtifactStore,
        *,
        expected_frame_count: int,
    ) -> Sequence[StoredArtifact]:
        expected = int(expected_frame_count)
        if expected <= 0:
            raise ImmutableArtifactError("expected_frame_count must be positive")
        with tempfile.TemporaryDirectory(prefix="autorig-fitting-frames-") as temp_dir:
            pattern = Path(temp_dir) / "frame_%06d.png"
            completed = subprocess.run(
                [
                    self.ffmpeg_path,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-i",
                    str(raw_video.path),
                    "-map",
                    "0:v:0",
                    "-vsync",
                    "0",
                    "-start_number",
                    "0",
                    str(pattern),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode != 0:
                raise ImmutableArtifactError(f"ffmpeg frame extraction failed: {completed.stderr[-2000:]}")
            frame_paths = sorted(Path(temp_dir).glob("frame_*.png"))
            if len(frame_paths) != expected:
                raise ImmutableArtifactError(
                    f"Expected {expected} decoded frames, found {len(frame_paths)}"
                )
            return tuple(
                store.store_frame(raw_video.sha256, index, frame_path.read_bytes())
                for index, frame_path in enumerate(frame_paths)
            )


def _require_sha256(value: str, label: str) -> str:
    digest = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise ImmutableArtifactError(f"{label} must be a SHA-256 digest")
    return digest


def _require_safe_id(value: str, label: str) -> str:
    token = str(value or "").strip()
    if not SAFE_ID_RE.fullmatch(token):
        raise ImmutableArtifactError(f"{label} contains unsupported characters")
    return token

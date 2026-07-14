from __future__ import annotations

import asyncio
import base64
import binascii
from collections import deque
import hashlib
import inspect
import json
import math
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Sequence

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from animation_fitting.comfy import worker_from_environment
from animation_fitting.orchestrator import AnimationFittingOrchestrator
from animation_fitting.specs import (
    AnimationFittingSpecs,
    SpecValidationError,
    load_animation_fitting_specs,
)
from animation_fitting.storage import (
    ImmutableArtifactError,
    ImmutableArtifactStore,
    StoredArtifact,
)


PIPELINE_VERSION = "semantic-comfy-browser-v1"
CAPABILITIES_SCHEMA = "autorig.animation-fitting-capabilities.v1"
JOB_REQUEST_SCHEMA = "autorig.animation-fitting-job-request.v1"
JOB_RESPONSE_SCHEMA = "autorig.animation-fitting-job.v1"
SEMANTIC_CAPTURE_SCHEMA = "autorig.browser-semantic-reference.v1"
SEMANTIC_PROFILE_ID = "horse_2.semantic_limbs.v1"
SEMANTIC_RIG_TYPE = "HORSE_2"
SEMANTIC_RESOLUTION = (768, 448)
SEMANTIC_LABELS = (
    "fore_left",
    "fore_right",
    "hind_left",
    "hind_right",
)
SEMANTIC_PALETTE = {
    "fore_left": (0.0, 0.85, 1.0),
    "fore_right": (0.12, 0.22, 1.0),
    "hind_left": (1.0, 0.72, 0.02),
    "hind_right": (1.0, 0.08, 0.55),
}
MAX_REFERENCE_BYTES = 3_500_000
MAX_REQUEST_BODY_BYTES = 5_250_000
MAX_METADATA_BYTES = 131_072
MAX_POLYLINE_POINTS = 64
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
JPEG_SOF_MARKERS = {
    0xC0, 0xC1, 0xC2, 0xC3,
    0xC5, 0xC6, 0xC7,
    0xC9, 0xCA, 0xCB,
    0xCD, 0xCE, 0xCF,
}


class SemanticAnimationFittingStoreError(RuntimeError):
    """Raised when immutable semantic job state is missing or inconsistent."""


class SingleProcessAnimationFittingExecutor:
    """App-owned executor for the production one-Uvicorn-worker contract.

    Immutable requests and queued state are persisted before ``submit``.  This
    class only owns live task references; startup recovery remains the durable
    source of truth after a process restart.
    """

    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._queue: deque[str] = deque()
        self._handlers: Dict[str, Callable[[str], Awaitable[None]]] = {}
        self._worker_task: Optional[asyncio.Task] = None
        self._active_job_id: Optional[str] = None
        self._last_errors: Dict[str, str] = {}

    async def submit(
        self,
        job_id: str,
        handler: Callable[[str], Awaitable[None]],
    ) -> bool:
        async with self._guard:
            if job_id in self._handlers:
                return False
            self._last_errors.pop(job_id, None)
            self._handlers[job_id] = handler
            self._queue.append(job_id)
            if self._worker_task is None or self._worker_task.done():
                self._worker_task = asyncio.create_task(
                    self._consume_fifo(),
                    name="semantic-animation-fitting-fifo",
                )
            return True

    async def _consume_fifo(self) -> None:
        current = asyncio.current_task()
        try:
            while True:
                async with self._guard:
                    if not self._queue:
                        if self._worker_task is current:
                            self._worker_task = None
                        return
                    job_id = self._queue.popleft()
                    handler = self._handlers[job_id]
                    self._active_job_id = job_id
                try:
                    await handler(job_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._last_errors[job_id] = f"{type(exc).__name__}: {exc}"[:2000]
                finally:
                    async with self._guard:
                        self._handlers.pop(job_id, None)
                        if self._active_job_id == job_id:
                            self._active_job_id = None
        finally:
            async with self._guard:
                if self._worker_task is current:
                    self._worker_task = None

    async def active_job_ids(self) -> tuple[str, ...]:
        async with self._guard:
            return tuple(sorted(self._handlers))

    async def wait_for_idle(self) -> None:
        while True:
            async with self._guard:
                worker = self._worker_task
                if worker is None and self._handlers:
                    worker = asyncio.create_task(
                        self._consume_fifo(),
                        name="semantic-animation-fitting-fifo",
                    )
                    self._worker_task = worker
            if worker is None:
                return
            await asyncio.gather(worker, return_exceptions=True)

    @property
    def last_errors(self) -> Dict[str, str]:
        return dict(self._last_errors)


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _linear_to_srgb_byte(value: float) -> int:
    channel = min(1.0, max(0.0, float(value)))
    srgb = channel * 12.92 if channel <= 0.0031308 else 1.055 * (channel ** (1 / 2.4)) - 0.055
    return round(min(1.0, max(0.0, srgb)) * 255)


SEMANTIC_PALETTE_SRGB = {
    label: tuple(_linear_to_srgb_byte(channel) for channel in SEMANTIC_PALETTE[label])
    for label in SEMANTIC_LABELS
}
SEMANTIC_CONTRACT_OBJECT = {
    "schema": SEMANTIC_CAPTURE_SCHEMA,
    "profile_id_string": SEMANTIC_PROFILE_ID,
    "rig_type_string": SEMANTIC_RIG_TYPE,
    "reference_resolution_array": list(SEMANTIC_RESOLUTION),
    "labels_array": list(SEMANTIC_LABELS),
    "palette_linear_object": {
        label: list(SEMANTIC_PALETTE[label]) for label in SEMANTIC_LABELS
    },
    "palette_srgb_byte_object": {
        label: list(SEMANTIC_PALETTE_SRGB[label]) for label in SEMANTIC_LABELS
    },
}
SEMANTIC_CONTRACT_SHA256 = canonical_json_sha256(SEMANTIC_CONTRACT_OBJECT)


def _immutable_write(path: Path, payload: bytes) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path)
            return True
        except FileExistsError:
            if path.read_bytes() != payload:
                raise SemanticAnimationFittingStoreError(
                    f"Immutable semantic artifact collision: {path}"
                )
            return False
    finally:
        temp_path.unlink(missing_ok=True)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class SemanticAnimationFittingStore:
    """Dedicated immutable store; it never reads or writes Idle LTX references."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        lowered_parts = tuple(part.lower() for part in self.root.parts)
        if any(
            lowered_parts[index:index + 2] == ("static", "tasks")
            for index in range(max(0, len(lowered_parts) - 1))
        ):
            raise ValueError("Semantic fitting store must not use the public static/tasks directory")
        self.references_root = self.root / "references"
        self.requests_root = self.root / "requests"
        self.artifacts = ImmutableArtifactStore(self.root / "artifacts")
        self._verified_videos: Dict[str, tuple[str, int, int]] = {}

    def store_reference_jpeg(self, payload: bytes) -> StoredArtifact:
        data = bytes(payload)
        if not data:
            raise SemanticAnimationFittingStoreError("Semantic reference JPEG is empty")
        digest = hashlib.sha256(data).hexdigest()
        path = self.references_root / digest[:2] / f"{digest}.jpg"
        _immutable_write(path, data)
        return StoredArtifact(sha256=digest, path=path, size_bytes=len(data))

    def create_job_request(self, job_id: str, payload: Mapping[str, Any]) -> bool:
        encoded = canonical_json_bytes(payload) + b"\n"
        return _immutable_write(self.requests_root / f"{job_id}.json", encoded)

    def reference_jpeg(self, source_sha256: str) -> StoredArtifact:
        digest = str(source_sha256 or "").strip().lower()
        if not SHA256_RE.fullmatch(digest):
            raise SemanticAnimationFittingStoreError("Semantic reference SHA-256 is invalid")
        path = (self.references_root / digest[:2] / f"{digest}.jpg").resolve()
        references_root = self.references_root.resolve()
        if references_root not in path.parents or not path.is_file():
            raise SemanticAnimationFittingStoreError("Semantic reference JPEG is missing")
        size = path.stat().st_size
        if size <= 0 or _file_sha256(path) != digest:
            raise SemanticAnimationFittingStoreError("Semantic reference JPEG integrity check failed")
        return StoredArtifact(sha256=digest, path=path, size_bytes=size)

    def load_job_request(self, job_id: str) -> Optional[Dict[str, Any]]:
        if not SHA256_RE.fullmatch(str(job_id or "")):
            return None
        path = self.requests_root / f"{job_id}.json"
        if not path.is_file():
            return None
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SemanticAnimationFittingStoreError("Semantic job request is corrupt") from exc
        if not isinstance(parsed, dict):
            raise SemanticAnimationFittingStoreError("Semantic job request must be an object")
        return parsed

    def append_job_state(self, job_id: str, payload: Mapping[str, Any]) -> None:
        self.artifacts.append_job_state(job_id, payload)

    def latest_job_state(self, job_id: str) -> Optional[Dict[str, Any]]:
        try:
            return self.artifacts.latest_job_state(job_id)
        except (ImmutableArtifactError, OSError, json.JSONDecodeError) as exc:
            raise SemanticAnimationFittingStoreError("Semantic job state is corrupt") from exc

    def recoverable_job_ids(self) -> tuple[str, ...]:
        if not self.requests_root.is_dir():
            return ()
        result = []
        for path in sorted(self.requests_root.glob("*.json")):
            job_id = path.stem
            if not SHA256_RE.fullmatch(job_id):
                continue
            request_record = self.load_job_request(job_id)
            if not request_record:
                continue
            state = self.latest_job_state(job_id) or {}
            if state.get("status_string") not in {"ready", "failed"}:
                result.append(job_id)
        return tuple(result)

    def ready_video(self, job_id: str) -> StoredArtifact:
        state = self.latest_job_state(job_id)
        if not state or state.get("status_string") != "ready":
            raise SemanticAnimationFittingStoreError("Semantic fitting video is not ready")
        digest = str(state.get("raw_video_sha256_string") or "").strip().lower()
        if not SHA256_RE.fullmatch(digest):
            raise SemanticAnimationFittingStoreError("Ready video has no valid SHA-256")
        raw_path = Path(str(state.get("raw_video_path_string") or "")).resolve()
        raw_root = self.artifacts.raw_root.resolve()
        if raw_root not in raw_path.parents:
            raise SemanticAnimationFittingStoreError("Ready video path is outside the immutable raw store")
        if raw_path.name != f"{digest}.mp4" or not raw_path.is_file():
            raise SemanticAnimationFittingStoreError("Ready video file is missing or misnamed")
        stat = raw_path.stat()
        size = stat.st_size
        cache_key = (str(raw_path), size, stat.st_mtime_ns)
        if size <= 0:
            raise SemanticAnimationFittingStoreError("Ready video integrity check failed")
        if self._verified_videos.get(digest) != cache_key and _file_sha256(raw_path) != digest:
            raise SemanticAnimationFittingStoreError("Ready video integrity check failed")
        self._verified_videos[digest] = cache_key
        return StoredArtifact(sha256=digest, path=raw_path, size_bytes=size)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _read_json_body(request: Request) -> Any:
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise _http_error(400, "Invalid Content-Length") from exc
        if declared_length < 0:
            raise _http_error(400, "Invalid Content-Length")
        if declared_length > MAX_REQUEST_BODY_BYTES:
            raise _http_error(413, "Animation-fitting request body is too large")
    payload = bytearray()
    async for chunk in request.stream():
        payload.extend(chunk)
        if len(payload) > MAX_REQUEST_BODY_BYTES:
            raise _http_error(413, "Animation-fitting request body is too large")
    try:
        return json.loads(bytes(payload))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _http_error(400, "Invalid JSON body") from exc


def _http_error(status_code: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail=detail)


def _task_value(task: Any, name: str, default: Any = None) -> Any:
    if isinstance(task, Mapping):
        return task.get(name, default)
    return getattr(task, name, default)


def _task_species(task: Any) -> str:
    for name in ("animal_type", "species", "animal_type_string"):
        value = str(_task_value(task, name, "") or "").strip()
        if value:
            return value[:120]
    raw_settings = _task_value(task, "viewer_settings", None)
    if isinstance(raw_settings, str):
        try:
            raw_settings = json.loads(raw_settings)
        except json.JSONDecodeError:
            raw_settings = None
    if isinstance(raw_settings, Mapping):
        for name in ("animal_type", "animal_type_string", "detected_species_string"):
            value = str(raw_settings.get(name) or "").strip()
            if value:
                return value[:120]
    return "animal"


def _require_sha256(value: Any, field: str) -> str:
    digest = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(digest):
        raise _http_error(400, f"{field} must be a lowercase SHA-256 digest")
    return digest


def _jpeg_dimensions(payload: bytes) -> tuple[int, int]:
    index = 2
    limit = len(payload) - 2
    while index < limit:
        if payload[index] != 0xFF:
            raise _http_error(400, "Semantic reference JPEG marker stream is invalid")
        while index < limit and payload[index] == 0xFF:
            index += 1
        if index >= limit:
            break
        marker = payload[index]
        index += 1
        if marker in {0x01, *range(0xD0, 0xDA)}:
            continue
        if marker in (0xD8, 0xD9, 0xDA):
            break
        if index + 2 > limit:
            break
        segment_length = int.from_bytes(payload[index:index + 2], "big")
        if segment_length < 2 or index + segment_length > len(payload):
            raise _http_error(400, "Semantic reference JPEG segment is truncated")
        if marker in JPEG_SOF_MARKERS:
            if segment_length < 8:
                raise _http_error(400, "Semantic reference JPEG frame header is invalid")
            height = int.from_bytes(payload[index + 3:index + 5], "big")
            width = int.from_bytes(payload[index + 5:index + 7], "big")
            if width <= 0 or height <= 0:
                raise _http_error(400, "Semantic reference JPEG dimensions are invalid")
            return width, height
        index += segment_length
    raise _http_error(400, "Semantic reference JPEG has no frame header")


def _decode_reference_jpeg(data_url: Any) -> bytes:
    raw = str(data_url or "").strip()
    prefix = "data:image/jpeg;base64,"
    if not raw.startswith(prefix):
        raise _http_error(400, "frame_jpeg_data_url_string must be a JPEG data URL")
    encoded = raw[len(prefix):]
    max_encoded_length = 4 * ((MAX_REFERENCE_BYTES + 2) // 3)
    if len(encoded) > max_encoded_length:
        raise _http_error(413, "Semantic reference JPEG size is invalid")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise _http_error(400, "frame_jpeg_data_url_string contains invalid base64") from exc
    if not payload or len(payload) > MAX_REFERENCE_BYTES:
        raise _http_error(413 if len(payload) > MAX_REFERENCE_BYTES else 400, "Semantic reference JPEG size is invalid")
    if not payload.startswith(b"\xff\xd8") or not payload.endswith(b"\xff\xd9"):
        raise _http_error(400, "Semantic reference must contain JPEG bytes")
    if _jpeg_dimensions(payload) != SEMANTIC_RESOLUTION:
        raise _http_error(400, "Semantic reference JPEG must be 768x448")
    try:
        import cv2
        import numpy as np

        decoded = cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_COLOR)
    except Exception as exc:
        raise _http_error(400, "Semantic reference JPEG could not be decoded") from exc
    expected_width, expected_height = SEMANTIC_RESOLUTION
    if (decoded is None or decoded.ndim != 3 or decoded.shape[2] != 3
            or decoded.shape[1] != expected_width or decoded.shape[0] != expected_height):
        raise _http_error(400, "Semantic reference JPEG could not be decoded as 768x448 RGB")
    return payload


def _same_numbers(actual: Any, expected: Sequence[float]) -> bool:
    if not isinstance(actual, list) or len(actual) != len(expected):
        return False
    try:
        return all(isinstance(value, (int, float))
                   and not isinstance(value, bool)
                   and math.isfinite(float(value))
                   and abs(float(value) - float(want)) <= 1e-9
                   for value, want in zip(actual, expected))
    except (TypeError, ValueError):
        return False


def _finite_float(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _http_error(400, f"{field} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise _http_error(400, f"{field} must be a finite number") from exc
    if not math.isfinite(parsed):
        raise _http_error(400, f"{field} must be a finite number")
    return parsed


def _semantic_point(value: Any, *, label: str) -> tuple[float, float]:
    if not isinstance(value, list) or len(value) != 2:
        raise _http_error(400, f"Semantic overlay point is invalid for {label}")
    x = _finite_float(value[0], field=f"Semantic overlay x for {label}")
    y = _finite_float(value[1], field=f"Semantic overlay y for {label}")
    return x, y


def _validate_semantic_metadata(metadata: Any) -> Dict[str, Any]:
    if not isinstance(metadata, dict):
        raise _http_error(400, "semantic_capture_object.metadata_object must be an object")
    expected_metadata_fields = {
        "schema",
        "profile_id_string",
        "rig_type_string",
        "composition_string",
        "source_resolution_array",
        "reference_resolution_array",
        "viewer_contain_object",
        "semantic_legend_object",
        "overlay_object",
    }
    if set(metadata) != expected_metadata_fields:
        raise _http_error(400, "Semantic capture metadata fields are invalid")
    if metadata.get("schema") != SEMANTIC_CAPTURE_SCHEMA:
        raise _http_error(400, "Unsupported semantic capture metadata schema")
    if metadata.get("profile_id_string") != SEMANTIC_PROFILE_ID:
        raise _http_error(400, "Semantic profile does not match Horse_2 v1")
    if metadata.get("rig_type_string") != SEMANTIC_RIG_TYPE:
        raise _http_error(400, "Semantic rig type does not match Horse_2")
    if metadata.get("composition_string") != "canonical_rgb_contain_with_semantic_bone_overlay":
        raise _http_error(400, "Semantic capture composition is invalid")
    if metadata.get("reference_resolution_array") != list(SEMANTIC_RESOLUTION):
        raise _http_error(400, "Semantic reference resolution must be 768x448")
    source_resolution = metadata.get("source_resolution_array")
    if not isinstance(source_resolution, list) or len(source_resolution) != 2:
        raise _http_error(400, "Semantic source resolution is invalid")
    for value in source_resolution:
        parsed = _finite_float(value, field="Semantic source resolution")
        if parsed <= 0 or parsed > 16384 or parsed != int(parsed):
            raise _http_error(400, "Semantic source resolution is invalid")

    contain = metadata.get("viewer_contain_object")
    contain_fields = {
        "scale_float",
        "offset_x_float",
        "offset_y_float",
        "draw_width_float",
        "draw_height_float",
    }
    if not isinstance(contain, dict) or set(contain) != contain_fields:
        raise _http_error(400, "Semantic viewer contain metadata is invalid")
    scale = _finite_float(contain.get("scale_float"), field="Semantic contain scale")
    offset_x = _finite_float(contain.get("offset_x_float"), field="Semantic contain offset x")
    offset_y = _finite_float(contain.get("offset_y_float"), field="Semantic contain offset y")
    draw_width = _finite_float(contain.get("draw_width_float"), field="Semantic contain width")
    draw_height = _finite_float(contain.get("draw_height_float"), field="Semantic contain height")
    width, height = SEMANTIC_RESOLUTION
    if (scale <= 0 or offset_x < 0 or offset_y < 0 or draw_width <= 0 or draw_height <= 0
            or offset_x + draw_width > width + 1e-6
            or offset_y + draw_height > height + 1e-6):
        raise _http_error(400, "Semantic viewer contain geometry is outside 768x448")

    legend = metadata.get("semantic_legend_object")
    legend_fields = {
        "color_space_string",
        "labels_array",
        "palette_linear_object",
        "palette_srgb_byte_object",
    }
    if (not isinstance(legend, dict) or set(legend) != legend_fields
            or legend.get("color_space_string") != "linear_rgb"):
        raise _http_error(400, "Semantic legend must use linear RGB")
    if legend.get("labels_array") != list(SEMANTIC_LABELS):
        raise _http_error(400, "Semantic legend labels are invalid")
    palette = legend.get("palette_linear_object")
    palette_srgb = legend.get("palette_srgb_byte_object")
    if (not isinstance(palette, dict) or set(palette) != set(SEMANTIC_LABELS)
            or not isinstance(palette_srgb, dict) or set(palette_srgb) != set(SEMANTIC_LABELS)):
        raise _http_error(400, "Semantic palette objects are required")
    for label in SEMANTIC_LABELS:
        if not _same_numbers(palette.get(label), SEMANTIC_PALETTE[label]):
            raise _http_error(400, f"Semantic linear palette is invalid for {label}")
        if palette_srgb.get(label) != list(SEMANTIC_PALETTE_SRGB[label]):
            raise _http_error(400, f"Semantic sRGB palette is invalid for {label}")

    overlay = metadata.get("overlay_object")
    overlay_fields = {
        "underlay_srgb_byte_array",
        "underlay_width_px_float",
        "semantic_width_px_float",
        "line_cap_string",
        "line_join_string",
        "jpeg_quality_float",
        "polylines_object",
    }
    if not isinstance(overlay, dict) or set(overlay) != overlay_fields:
        raise _http_error(400, "Semantic overlay metadata fields are invalid")
    if (overlay.get("underlay_srgb_byte_array") != [178, 185, 195]
            or overlay.get("line_cap_string") != "round"
            or overlay.get("line_join_string") != "round"):
        raise _http_error(400, "Semantic overlay rendering contract is invalid")
    semantic_width = _finite_float(
        overlay.get("semantic_width_px_float"),
        field="Semantic overlay width",
    )
    underlay_width = _finite_float(
        overlay.get("underlay_width_px_float"),
        field="Semantic underlay width",
    )
    jpeg_quality = _finite_float(
        overlay.get("jpeg_quality_float"),
        field="Semantic JPEG quality",
    )
    if semantic_width <= 0 or underlay_width <= semantic_width or not 0 < jpeg_quality <= 1:
        raise _http_error(400, "Semantic overlay widths or JPEG quality are invalid")
    polylines = overlay.get("polylines_object") if isinstance(overlay, dict) else None
    if not isinstance(polylines, dict) or set(polylines) != set(SEMANTIC_LABELS):
        raise _http_error(400, "Semantic overlay polylines are required")
    for label in SEMANTIC_LABELS:
        points = polylines.get(label)
        if (not isinstance(points, list) or len(points) < 2
                or len(points) > MAX_POLYLINE_POINTS):
            raise _http_error(400, f"Semantic overlay polyline is invalid for {label}")
        for point in points:
            x, y = _semantic_point(point, label=label)
            if x < 0 or x > width or y < 0 or y > height:
                raise _http_error(400, f"Semantic overlay point is outside 768x448 for {label}")
    return metadata


@dataclass(frozen=True)
class ValidatedSemanticJobRequest:
    action: Any
    jpeg_bytes: bytes
    source_sha256: str
    metadata: Dict[str, Any]
    metadata_sha256: str
    motion_notes: str


def _validate_job_body(body: Any, specs: AnimationFittingSpecs) -> ValidatedSemanticJobRequest:
    if not isinstance(body, dict):
        raise _http_error(400, "JSON body must be an object")
    required = {"pipeline_version_string", "action_id_string", "semantic_capture_object"}
    optional = {"motion_notes_string"}
    missing = required - set(body)
    unexpected = set(body) - required - optional
    if missing:
        raise _http_error(400, f"Missing job fields: {', '.join(sorted(missing))}")
    if unexpected:
        raise _http_error(400, f"Unsupported job fields: {', '.join(sorted(unexpected))}")
    if body.get("pipeline_version_string") != PIPELINE_VERSION:
        raise _http_error(400, "Unsupported animation-fitting pipeline version")
    action_id = str(body.get("action_id_string") or "").strip()
    try:
        action = specs.action(action_id)
    except SpecValidationError as exc:
        raise _http_error(400, str(exc)) from exc

    capture = body.get("semantic_capture_object")
    expected_capture_fields = {
        "frame_jpeg_data_url_string",
        "frame_jpeg_sha256_string",
        "metadata_object",
        "metadata_sha256_string",
        "semantic_contract_sha256_string",
    }
    if not isinstance(capture, dict) or set(capture) != expected_capture_fields:
        raise _http_error(400, "semantic_capture_object fields are invalid")
    jpeg_bytes = _decode_reference_jpeg(capture.get("frame_jpeg_data_url_string"))
    source_sha256 = hashlib.sha256(jpeg_bytes).hexdigest()
    if _require_sha256(capture.get("frame_jpeg_sha256_string"), "frame_jpeg_sha256_string") != source_sha256:
        raise _http_error(400, "Semantic reference JPEG SHA-256 mismatch")
    metadata = _validate_semantic_metadata(capture.get("metadata_object"))
    try:
        metadata_bytes = canonical_json_bytes(metadata)
    except (TypeError, ValueError) as exc:
        raise _http_error(400, "Semantic metadata must be canonical finite JSON") from exc
    if len(metadata_bytes) > MAX_METADATA_BYTES:
        raise _http_error(413, "Semantic metadata is too large")
    metadata_sha256 = hashlib.sha256(metadata_bytes).hexdigest()
    if _require_sha256(capture.get("metadata_sha256_string"), "metadata_sha256_string") != metadata_sha256:
        raise _http_error(400, "Semantic metadata SHA-256 mismatch")
    if _require_sha256(
        capture.get("semantic_contract_sha256_string"),
        "semantic_contract_sha256_string",
    ) != SEMANTIC_CONTRACT_SHA256:
        raise _http_error(400, "Semantic profile/palette/resolution contract SHA-256 mismatch")
    raw_motion_notes = body.get("motion_notes_string", "")
    if not isinstance(raw_motion_notes, str):
        raise _http_error(400, "motion_notes_string must be a string")
    motion_notes = re.sub(r"\s+", " ", raw_motion_notes.strip())[:700]
    return ValidatedSemanticJobRequest(
        action=action,
        jpeg_bytes=jpeg_bytes,
        source_sha256=source_sha256,
        metadata=metadata,
        metadata_sha256=metadata_sha256,
        motion_notes=motion_notes,
    )


class AnimationFittingRouteService:
    def __init__(
        self,
        *,
        get_task: Callable[[str], Any],
        authorize_task: Callable[[Request, Any], Any],
        submit_job: Callable[[str, Callable[[str], Awaitable[None]]], Any],
        store: SemanticAnimationFittingStore,
        executor: Optional[SingleProcessAnimationFittingExecutor] = None,
        orchestrator: Optional[Any] = None,
        worker_for_mode: Callable[[str], Any] = worker_from_environment,
        specs: Optional[AnimationFittingSpecs] = None,
    ) -> None:
        self.get_task = get_task
        self.authorize_task = authorize_task
        self.submit_job = submit_job
        self.executor = executor
        self.store = store
        self.specs = specs or load_animation_fitting_specs()
        self.orchestrator = orchestrator or AnimationFittingOrchestrator(
            store.artifacts,
            specs=self.specs,
        )
        self.worker_for_mode = worker_for_mode
        self._job_locks: Dict[str, asyncio.Lock] = {}

    async def task_and_authorize(self, request: Request, task_id: str) -> Any:
        task = await _maybe_await(self.get_task(task_id))
        if task is None:
            raise _http_error(404, "Task not found")
        await _maybe_await(self.authorize_task(request, task))
        if str(_task_value(task, "input_type", "") or "").strip().lower() != "animal":
            raise _http_error(400, "Semantic animation fitting is available only for animal tasks")
        return task

    def capabilities(self, task_id: str) -> Dict[str, Any]:
        return {
            "schema": CAPABILITIES_SCHEMA,
            "pipeline_version_string": PIPELINE_VERSION,
            "task_id_string": task_id,
            "creates_job_bool": True,
            "mutates_legacy_idle_ltx_bool": False,
            "semantic_capture_schema_string": SEMANTIC_CAPTURE_SCHEMA,
            "semantic_contract_sha256_string": SEMANTIC_CONTRACT_SHA256,
            "supported_rig_types_array": [SEMANTIC_RIG_TYPE],
            "actions_array": [
                {
                    "action_id_string": action_id,
                    "generation_mode_string": self.specs.actions[action_id].generation_mode,
                    "frame_count_int": self.specs.actions[action_id].frame_count,
                    "input_fps_int": self.specs.actions[action_id].input_fps,
                    "output_fps_int": self.specs.actions[action_id].output_fps,
                    "conditioned_frames_array": [
                        dict(row)
                        for row in self.specs.workflow_for_action(action_id).conditioned_frames
                    ],
                }
                for action_id in self.specs.action_order
            ],
        }

    @staticmethod
    def _job_id(task_id: str, action_id: str, source_sha256: str) -> str:
        return canonical_json_sha256({
            "task_id_string": task_id,
            "action_id_string": action_id,
            "source_sha256_string": source_sha256,
        })

    def _request_for_task(self, task_id: str, job_id: str) -> Dict[str, Any]:
        request_record = self.store.load_job_request(job_id)
        if not request_record or request_record.get("task_id_string") != task_id:
            raise _http_error(404, "Animation-fitting job not found")
        return request_record

    def job_payload(self, task_id: str, job_id: str, *, replay: bool = False) -> Dict[str, Any]:
        request_record = self._request_for_task(task_id, job_id)
        state = self.store.latest_job_state(job_id) or {}
        status = str(state.get("status_string") or "queued")
        payload = {
            "schema": JOB_RESPONSE_SCHEMA,
            "pipeline_version_string": PIPELINE_VERSION,
            "task_id_string": task_id,
            "job_id_string": job_id,
            "action_id_string": request_record["action_id_string"],
            "generation_mode_string": request_record["generation_mode_string"],
            "frame_count_int": request_record["frame_count_int"],
            "input_fps_int": request_record["input_fps_int"],
            "output_fps_int": request_record["output_fps_int"],
            "source_sha256_string": request_record["source_sha256_string"],
            "metadata_sha256_string": request_record["metadata_sha256_string"],
            "semantic_contract_sha256_string": request_record["semantic_contract_sha256_string"],
            "status_string": status,
            "idempotent_replay_bool": bool(replay),
        }
        if status == "ready":
            payload.update({
                "video_url_string": (
                    f"/api/task/{task_id}/animation-fitting/v1/jobs/{job_id}/video"
                ),
                "raw_video_sha256_string": state.get("raw_video_sha256_string"),
                "frame_sha256_array": list(state.get("frame_sha256_array") or []),
                "orchestrator_job_id_string": state.get("orchestrator_job_id_string"),
            })
        elif status == "failed":
            payload["error_string"] = str(state.get("error_string") or "Animation fitting failed")[:2000]
        return payload

    async def create_job(self, task_id: str, task: Any, body: Any) -> tuple[Dict[str, Any], bool]:
        validated = await asyncio.to_thread(_validate_job_body, body, self.specs)
        action = validated.action
        job_id = self._job_id(task_id, action.action_id, validated.source_sha256)
        await asyncio.to_thread(self.store.store_reference_jpeg, validated.jpeg_bytes)
        request_record = {
            "schema": JOB_REQUEST_SCHEMA,
            "pipeline_version_string": PIPELINE_VERSION,
            "task_id_string": task_id,
            "job_id_string": job_id,
            "action_id_string": action.action_id,
            "generation_mode_string": action.generation_mode,
            "frame_count_int": action.frame_count,
            "input_fps_int": action.input_fps,
            "output_fps_int": action.output_fps,
            "source_sha256_string": validated.source_sha256,
            "source_bytes_int": len(validated.jpeg_bytes),
            "species_string": _task_species(task),
            "metadata_sha256_string": validated.metadata_sha256,
            "semantic_contract_sha256_string": SEMANTIC_CONTRACT_SHA256,
            "semantic_metadata_object": validated.metadata,
            "motion_notes_string": validated.motion_notes,
        }
        try:
            created = await asyncio.to_thread(
                self.store.create_job_request,
                job_id,
                request_record,
            )
        except SemanticAnimationFittingStoreError as exc:
            raise _http_error(409, str(exc)) from exc
        try:
            await self.schedule_job(job_id)
        except Exception as exc:
            self.store.append_job_state(job_id, {
                "status_string": "queued",
                "task_id_string": task_id,
                "action_id_string": action.action_id,
                "queue_error_type_string": type(exc).__name__,
                "queue_error_string": f"Job submission failed: {exc}"[:2000],
            })
            raise _http_error(503, "Animation-fitting job queue is unavailable") from exc
        return self.job_payload(task_id, job_id, replay=not created), created

    async def schedule_job(self, job_id: str) -> bool:
        request_record = self.store.load_job_request(job_id)
        if not request_record:
            raise SemanticAnimationFittingStoreError("Semantic job request is missing")
        state = self.store.latest_job_state(job_id) or {}
        if state.get("status_string") in {"ready", "failed"}:
            return False
        if not state:
            self.store.append_job_state(job_id, {
                "status_string": "queued",
                "task_id_string": request_record["task_id_string"],
                "action_id_string": request_record["action_id_string"],
                "source_sha256_string": request_record["source_sha256_string"],
            })
        # The immutable request and queued state are already durable here.
        # The default single-process executor deduplicates the live job id;
        # an injected executor must provide the same submit contract. Startup
        # recovery reconstructs any work lost with the process.
        await _maybe_await(self.submit_job(job_id, self.run_job))
        return True

    async def recover_jobs(self) -> tuple[str, ...]:
        scheduled = []
        for job_id in self.store.recoverable_job_ids():
            if await self.schedule_job(job_id):
                scheduled.append(job_id)
        return tuple(scheduled)

    async def run_job(self, job_id: str) -> None:
        lock = self._job_locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            request_record = self.store.load_job_request(job_id)
            if not request_record:
                raise SemanticAnimationFittingStoreError("Semantic job request is missing")
            state = self.store.latest_job_state(job_id) or {}
            if state.get("status_string") in {"ready", "failed"}:
                return
            task_id = str(request_record.get("task_id_string") or "")
            action_id = str(request_record.get("action_id_string") or "")
            try:
                action = self.specs.action(action_id)
                expected_contract = {
                    "pipeline_version_string": PIPELINE_VERSION,
                    "generation_mode_string": action.generation_mode,
                    "frame_count_int": action.frame_count,
                    "input_fps_int": action.input_fps,
                    "output_fps_int": action.output_fps,
                    "semantic_contract_sha256_string": SEMANTIC_CONTRACT_SHA256,
                }
                if any(request_record.get(key) != value for key, value in expected_contract.items()):
                    raise SemanticAnimationFittingStoreError(
                        "Persisted semantic job no longer matches the pinned fitting contract"
                    )
                reference = await asyncio.to_thread(
                    self.store.reference_jpeg,
                    request_record["source_sha256_string"],
                )
                self.store.append_job_state(job_id, {
                    "status_string": "processing",
                    "task_id_string": task_id,
                    "action_id_string": action_id,
                })
                worker = self.worker_for_mode(action.generation_mode)
                result = await self.orchestrator.run_candidate(
                    task_id=task_id,
                    action_id=action_id,
                    candidate_index=0,
                    species=str(request_record.get("species_string") or "animal")[:120],
                    reference_frame_path=reference.path,
                    worker=worker,
                    motion_notes=str(request_record.get("motion_notes_string") or "")[:700],
                )
                raw_video = result.raw_video
                frames = tuple(result.frames)
                if len(frames) != action.frame_count:
                    raise SemanticAnimationFittingStoreError(
                        f"Orchestrator returned {len(frames)} frames, expected {action.frame_count}"
                    )
                if not SHA256_RE.fullmatch(str(raw_video.sha256 or "")):
                    raise SemanticAnimationFittingStoreError("Orchestrator video SHA-256 is invalid")
                raw_path = Path(raw_video.path).resolve()
                raw_root = self.store.artifacts.raw_root.resolve()
                raw_integrity_ok = (
                    raw_root in raw_path.parents
                    and raw_path.name == f"{raw_video.sha256}.mp4"
                    and raw_path.is_file()
                    and raw_path.stat().st_size == raw_video.size_bytes
                )
                if raw_integrity_ok:
                    raw_integrity_ok = (
                        await asyncio.to_thread(_file_sha256, raw_path)
                    ) == raw_video.sha256
                if not raw_integrity_ok:
                    raise SemanticAnimationFittingStoreError(
                        "Orchestrator video is outside or inconsistent with the immutable raw store"
                    )
                frame_hashes = [str(frame.sha256 or "") for frame in frames]
                if any(not SHA256_RE.fullmatch(digest) for digest in frame_hashes):
                    raise SemanticAnimationFittingStoreError("Orchestrator frame SHA-256 is invalid")
                self.store.append_job_state(job_id, {
                    "status_string": "ready",
                    "task_id_string": task_id,
                    "action_id_string": action_id,
                    "orchestrator_job_id_string": result.job_id,
                    "prompt_id_string": result.prompt_id,
                    "raw_video_sha256_string": raw_video.sha256,
                    "raw_video_path_string": str(raw_path),
                    "raw_video_bytes_int": raw_video.size_bytes,
                    "frame_count_int": len(frames),
                    "frame_sha256_array": frame_hashes,
                })
            except Exception as exc:
                self.store.append_job_state(job_id, {
                    "status_string": "failed",
                    "task_id_string": task_id,
                    "action_id_string": action_id,
                    "error_type_string": type(exc).__name__,
                    "error_string": str(exc)[:2000],
                })


def _parse_range(value: str, size: int) -> tuple[int, int]:
    match = RANGE_RE.fullmatch(str(value or "").strip())
    if not match or "," in value:
        raise ValueError("Invalid byte range")
    start_raw, end_raw = match.groups()
    if not start_raw and not end_raw:
        raise ValueError("Invalid byte range")
    if start_raw:
        start = int(start_raw)
        end = int(end_raw) if end_raw else size - 1
        if start >= size or end < start:
            raise ValueError("Unsatisfiable byte range")
        end = min(end, size - 1)
        return start, end
    suffix = int(end_raw)
    if suffix <= 0:
        raise ValueError("Unsatisfiable byte range")
    return max(0, size - suffix), size - 1


def _iter_file_segment(path: Path, start: int, end: int, *, chunk_size: int = 1024 * 1024):
    remaining = end - start + 1
    with path.open("rb") as handle:
        handle.seek(start)
        while remaining > 0:
            chunk = handle.read(min(chunk_size, remaining))
            if not chunk:
                raise OSError("Immutable video ended before the advertised byte range")
            remaining -= len(chunk)
            yield chunk


def _video_response(request: Request, artifact: StoredArtifact) -> Response:
    headers = {
        "Accept-Ranges": "bytes",
        "Cache-Control": "private, no-store",
        "Content-Encoding": "identity",
        "ETag": f'"{artifact.sha256}"',
    }
    range_header = request.headers.get("range")
    if not range_header:
        headers["Content-Length"] = str(artifact.size_bytes)
        return StreamingResponse(
            _iter_file_segment(artifact.path, 0, artifact.size_bytes - 1),
            media_type="video/mp4",
            headers=headers,
        )
    try:
        start, end = _parse_range(range_header, artifact.size_bytes)
    except ValueError:
        headers["Content-Range"] = f"bytes */{artifact.size_bytes}"
        return Response(status_code=416, headers=headers)
    content_length = end - start + 1
    headers.update({
        "Content-Length": str(content_length),
        "Content-Range": f"bytes {start}-{end}/{artifact.size_bytes}",
    })
    return StreamingResponse(
        _iter_file_segment(artifact.path, start, end),
        status_code=206,
        media_type="video/mp4",
        headers=headers,
    )


def create_animation_fitting_router(
    *,
    get_task: Callable[[str], Any],
    authorize_task: Callable[[Request, Any], Any],
    submit_job: Optional[Callable[[str, Callable[[str], Awaitable[None]]], Any]] = None,
    store: Optional[SemanticAnimationFittingStore] = None,
    store_root: Optional[Path] = None,
    orchestrator: Optional[Any] = None,
    worker_for_mode: Callable[[str], Any] = worker_from_environment,
    specs: Optional[AnimationFittingSpecs] = None,
) -> APIRouter:
    if store is None:
        configured_root = store_root or Path(os.getenv(
            "AUTORIG_SEMANTIC_FITTING_STORE_ROOT",
            "/var/autorig/animation-fitting-semantic-v1",
        ))
        store = SemanticAnimationFittingStore(configured_root)
    executor = None
    if submit_job is None:
        executor = SingleProcessAnimationFittingExecutor()
        submit_job = executor.submit
    service = AnimationFittingRouteService(
        get_task=get_task,
        authorize_task=authorize_task,
        submit_job=submit_job,
        store=store,
        executor=executor,
        orchestrator=orchestrator,
        worker_for_mode=worker_for_mode,
        specs=specs,
    )
    router = APIRouter()
    # Integration can invoke `recover_jobs()` from the app startup lifecycle.
    setattr(router, "animation_fitting_service", service)
    setattr(router, "animation_fitting_executor", executor)

    @router.get("/api/task/{task_id}/animation-fitting/v1/capabilities")
    async def animation_fitting_capabilities(request: Request, task_id: str):
        await service.task_and_authorize(request, task_id)
        return service.capabilities(task_id)

    @router.post("/api/task/{task_id}/animation-fitting/v1/jobs")
    async def animation_fitting_create_job(request: Request, task_id: str):
        task = await service.task_and_authorize(request, task_id)
        body = await _read_json_body(request)
        payload, created = await service.create_job(task_id, task, body)
        return JSONResponse(payload, status_code=202 if created else 200)

    @router.get("/api/task/{task_id}/animation-fitting/v1/jobs/{job_id}")
    async def animation_fitting_get_job(request: Request, task_id: str, job_id: str):
        await service.task_and_authorize(request, task_id)
        try:
            return service.job_payload(task_id, job_id)
        except SemanticAnimationFittingStoreError as exc:
            raise _http_error(409, str(exc)) from exc

    @router.get("/api/task/{task_id}/animation-fitting/v1/jobs/{job_id}/video")
    async def animation_fitting_get_job_video(request: Request, task_id: str, job_id: str):
        await service.task_and_authorize(request, task_id)
        service._request_for_task(task_id, job_id)
        try:
            artifact = await asyncio.to_thread(service.store.ready_video, job_id)
        except SemanticAnimationFittingStoreError as exc:
            raise _http_error(409, str(exc)) from exc
        return _video_response(request, artifact)

    return router


def register_animation_fitting_routes(app: Any, **dependencies: Any) -> APIRouter:
    """Main integration hook; intentionally not called from this module."""

    router = create_animation_fitting_router(**dependencies)
    app.include_router(router)
    app.state.animation_fitting_route_service = getattr(
        router,
        "animation_fitting_service",
    )
    app.state.animation_fitting_executor = getattr(
        router,
        "animation_fitting_executor",
    )
    return router

"""Fail-closed upload capture for browser-native animation-fitting candidates.

The browser solver produces JSON/PNG/MP4 evidence, not an FBX.  This module
copies those exact bytes into an immutable server-owned bundle and structurally
binds the upload to an existing draft library, fitting job, and completed animal
source task.  Uploaded QA assertions remain untrusted until a separate
server-side validation stage recomputes them from the pinned artifacts.
It deliberately does not create an ``AnimalAnimationCandidate`` row: the
browser evidence is only an uploaded candidate awaiting server validation.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from typing import Any, BinaryIO, Dict, Mapping, Sequence, Union
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from animal_animation_library import (
    AnimationLibraryError,
    normalize_rig_type,
    normalize_sha256,
    taxonomy_clip,
)
from config import ANIMATION_FITTING_JOBS_ROOT
from database import (
    AnimalAnimationFittingJob,
    AnimalAnimationLibraryVersion,
    Task,
)


JOB_BINDING_SCHEMA = "autorig.browser-animation-candidate-job-binding.v1"
JOB_BINDING_SCHEMA_V2 = "autorig.browser-animation-candidate-job-binding.v2"
CONTROLLED_RECEIPT_SCHEMA_V2 = (
    "autorig.animation-fitting-controlled-generation-receipt-descriptor.v2"
)
CONTROLLED_GENERATION_V2_KEYS = (
    "job_id",
    "prompt_id",
    "semantic_id",
    "generation_mode",
    "task",
    "prompt_contract",
    "reference_manifest",
    "experiment_id",
    "experiment_sha256",
    "experiment_spec",
    "worker_id",
    "worker_base_url",
    "workflow_name",
    "workflow_fingerprint_sha256",
    "trusted_latest_state",
    "retry_authorization_sha256",
)
TRUSTED_LATEST_STATE_KEYS = (
    "schema",
    "status",
    "latest",
    "job_id",
    "state_schema",
    "sequence",
    "filename",
    "pin",
)
PROMPT_SCHEMA = "autorig.animation-fitting-prompts.v1"
BUNDLE_SCHEMA = "autorig.browser-animation-candidate-bundle.v1"
VISUAL_QA_ENVELOPE_SCHEMA = "autorig.browser-horse-visual-phase-evidence-envelope.v1"
VISUAL_QA_SCHEMA = "autorig.animation-visual-phase-qa.v1"
FITTED_SCHEMA = "autorig-browser-fitted-animation.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
PHASES = ("start", "middle", "three_quarter")

MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_QA_JSON_BYTES = 8 * 1024 * 1024
MAX_PREVIEW_BYTES = 128 * 1024 * 1024
MAX_PNG_BYTES = 16 * 1024 * 1024
MAX_TOTAL_INGEST_BYTES = 224 * 1024 * 1024
MAX_SOURCE_VIDEO_BYTES = 192 * 1024 * 1024
QUATERNION_NORM_TOLERANCE = 1e-3
MOTION_EPSILON = 1e-7


class BrowserCandidateIngestError(AnimationLibraryError):
    pass


@dataclass(frozen=True)
class IngestedBrowserCandidate:
    identity_sha256: str
    directory: Path
    manifest_path: Path
    manifest_sha256: str
    manifest: Dict[str, Any]
    created: bool


ArtifactSource = Union[bytes, bytearray, memoryview, BinaryIO]


@dataclass(frozen=True)
class BrowserCandidateArtifactSet:
    """Raw transport-independent artifact streams consumed exactly once."""

    fitted_animation_json: ArtifactSource
    three_clip_json: ArtifactSource
    visual_phase_qa_json: ArtifactSource
    camera_settings_json: ArtifactSource
    deformation_report_json: ArtifactSource
    fixed_camera_preview_mp4: ArtifactSource
    phase_frames: Mapping[str, ArtifactSource]


@dataclass(frozen=True)
class BrowserCandidatePlanTrust:
    """Independent resolver output; never reconstruct this from job.config_json."""

    reference_manifest: Mapping[str, Any] | None
    latest_states: Sequence[Mapping[str, Any]]
    retry_authorization: Mapping[str, Any] | None = None


def _error(message: str, status_code: int = 409) -> BrowserCandidateIngestError:
    return BrowserCandidateIngestError(message, status_code=status_code)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_artifact(source: ArtifactSource, field: str, maximum: int) -> bytes:
    if isinstance(source, (bytes, bytearray, memoryview)):
        payload = bytes(source)
    elif hasattr(source, "read"):
        payload = source.read(maximum + 1)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise _error(f"{field} stream did not return bytes", 400)
        payload = bytes(payload)
    else:
        raise _error(f"{field} must be bytes or a binary stream", 400)
    if not payload:
        raise _error(f"{field} is empty", 400)
    if len(payload) > maximum:
        raise _error(f"{field} exceeds the server size limit", 413)
    return payload


def _reject_duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def _json_object(payload: bytes, field: str) -> Dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(ValueError(token)),
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
        RecursionError,
    ) as exc:
        raise _error(f"{field} is not strict UTF-8 JSON", 400) from exc
    if not isinstance(value, dict):
        raise _error(f"{field} must contain a JSON object", 400)

    def finite(node: Any, depth: int = 0) -> None:
        if depth > 128:
            raise _error(f"{field} exceeds the maximum JSON nesting depth", 400)
        if isinstance(node, float) and not math.isfinite(node):
            raise _error(f"{field} contains a non-finite number", 400)
        if isinstance(node, dict):
            for item in node.values():
                finite(item, depth + 1)
        elif isinstance(node, list):
            for item in node:
                finite(item, depth + 1)

    finite(value)
    return value


def _exact_object(value: Any, field: str, keys: Sequence[str]) -> Dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(keys):
        raise _error(f"{field} must contain exactly: {', '.join(keys)}")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _error(f"{field} must be a positive integer")
    return value


def _candidate_seed(value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= (2**63 - 1)
    ):
        raise _error("candidate seed must fit an unsigned SQL BigInteger range")
    return value


def _normalize_worker_url(value: Any, field: str) -> str:
    raw = str(value or "").strip()
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        raise _error(f"{field} is not a valid worker base URL") from exc
    if (
        parsed.scheme.lower() not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise _error(f"{field} is not a canonical worker base URL")
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    netloc = host if port is None else f"{host}:{port}"
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme.lower(), netloc, path, "", ""))


def derive_browser_candidate_seed(
    task_id: str, semantic_id: str, candidate_index: int
) -> int:
    """Canonical server-side seed used by the multi-candidate slot plan."""
    task_id = _uuid(task_id, "task_id")
    action = str(semantic_id or "").strip().lower()
    taxonomy_clip(action)
    if (
        isinstance(candidate_index, bool)
        or not isinstance(candidate_index, int)
        or not 0 <= candidate_index < 16
    ):
        raise _error("candidate_index must be in 0..15")
    material = f"{task_id}\n{action}\n{candidate_index}\n{PROMPT_SCHEMA}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) & ((1 << 63) - 1)


def _verify_server_owned_job_plan(
    config: Mapping[str, Any],
    *,
    binding: Mapping[str, Any],
    slots: Sequence[Mapping[str, Any]],
    semantic_id: str,
    candidate_target: int | None,
    candidate_limit: int,
    workflow_name: str | None,
    workflow_fingerprint: str | None,
    prompt_id: str | None,
    worker_url: str | None,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None,
    trusted_store_root: str | Path | None,
) -> tuple[Dict[str, Any], ...]:
    """Rebuild V2 from its server-owned inputs and bind it to the DB job.

    A syntactically valid ``browser_candidate_ingest`` object is not an
    authority.  Production consumers must reproduce the canonical plan from
    the pinned task/controlled-generation descriptors and compare the complete
    config plus the immutable fitting-job fields.
    """
    plan_binding = config.get("browser_candidate_job_plan")
    if binding.get("schema") == JOB_BINDING_SCHEMA and plan_binding is None:
        # Historical V14 canary jobs predate the server-plan envelope.  They
        # remain readable, but can never use the production V2 path.
        return tuple(dict(row) for row in slots)
    if not isinstance(plan_binding, dict):
        raise _error("server-owned browser_candidate_job_plan is missing")
    if trusted_plan_inputs is None or trusted_store_root is None:
        raise _error("independent browser candidate plan resolver inputs are required")
    if candidate_target is None or workflow_name is None or workflow_fingerprint is None or prompt_id is None or worker_url is None:
        raise _error("canonical fitting-job fields are required to verify the server plan")

    _verify_trusted_plan_files(Path(trusted_store_root), trusted_plan_inputs)

    try:
        import animation_fitting_candidate_job_plan as job_plan

        configured_receipts = plan_binding.get(
            "verified_controlled_generation_receipts"
        )
        receipts = (
            [
                {
                    key: item
                    for key, item in row.items()
                    if key != "trusted_latest_state"
                }
                for row in configured_receipts
            ]
            if isinstance(configured_receipts, list)
            and all(isinstance(row, Mapping) for row in configured_receipts)
            else configured_receipts
        )

        if binding.get("schema") == JOB_BINDING_SCHEMA_V2:
            if candidate_target != 8 or candidate_limit != 16:
                raise _error("production browser candidate policy must be target=8 limit=16")
            request = {
                "schema": job_plan.PLAN_REQUEST_SCHEMA,
                "semantic_id": str(semantic_id or "").strip().lower(),
                "candidate_target": candidate_target,
                "candidate_limit": candidate_limit,
            }
            rebuilt = job_plan.build_production_browser_candidate_job_plan(
                request,
                trusted_task=plan_binding.get("trusted_task"),
                trusted_reference_manifest=trusted_plan_inputs.reference_manifest,
                trusted_latest_states=trusted_plan_inputs.latest_states,
                trusted_retry_authorization=trusted_plan_inputs.retry_authorization,
                verified_receipts=receipts,
            )
        elif binding.get("schema") == JOB_BINDING_SCHEMA:
            if not isinstance(receipts, list) or len(receipts) != 1:
                raise _error("V14 canary server plan must pin exactly one receipt")
            rebuilt = job_plan.build_v14_nonproduction_canary_job_plan(
                trusted_task=plan_binding.get("trusted_task"),
                trusted_latest_state=(
                    trusted_plan_inputs.latest_states[0]
                    if len(trusted_plan_inputs.latest_states) == 1
                    else None
                ),
                verified_receipt=receipts[0],
            )
        else:  # pragma: no cover - the structural parser rejects this first.
            raise _error("browser candidate ingest schema is invalid")
    except BrowserCandidateIngestError:
        raise
    except (TypeError, ValueError, KeyError) as exc:
        raise _error(f"server-owned browser candidate job plan is invalid: {exc}") from exc

    expected_job_fields = {
        "semantic_id": rebuilt.semantic_id,
        "workflow_name": rebuilt.workflow_name,
        "workflow_fingerprint": rebuilt.workflow_fingerprint,
        "candidate_target": rebuilt.candidate_target,
        "candidate_limit": rebuilt.candidate_limit,
        "prompt_id": rebuilt.prompt_id,
    }
    actual_job_fields = {
        "semantic_id": str(semantic_id or "").strip().lower(),
        "workflow_name": str(workflow_name or "").strip(),
        "workflow_fingerprint": str(workflow_fingerprint or "").strip().lower(),
        "candidate_target": candidate_target,
        "candidate_limit": candidate_limit,
        "prompt_id": str(prompt_id or "").strip(),
    }
    if config != rebuilt.config or actual_job_fields != expected_job_fields:
        raise _error(
            "fitting job differs from the canonical server-owned browser candidate plan"
        )

    receipts_by_index = {
        row["candidate_index"]: row
        for row in rebuilt.config["browser_candidate_job_plan"][
            "verified_controlled_generation_receipts"
        ]
    }
    result = []
    normalized_worker_url = _normalize_worker_url(worker_url, "fitting_job.worker_url")
    for slot in slots:
        receipt = receipts_by_index.get(slot["candidate_index"])
        if receipt is None:
            raise _error("planned slot has no verified controlled-generation receipt")
        worker_id = str(receipt.get("worker_id") or "").strip()
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}", worker_id)
            or _normalize_worker_url(
                receipt.get("worker_base_url"),
                "verified_receipt.worker_base_url",
            )
            != normalized_worker_url
        ):
            raise _error("verified controlled generation is pinned to another worker")
        controlled = slot.get("controlled_generation") or {}
        if (
            controlled.get("worker_id") != worker_id
            or _normalize_worker_url(
                controlled.get("worker_base_url"),
                "controlled_generation.worker_base_url",
            )
            != normalized_worker_url
        ):
            raise _error("candidate slot worker binding differs from its verified receipt")
        result.append(
            {
                **dict(slot),
                "verified_generation_receipt": (
                    receipt["trusted_latest_state"]["pin"]
                    if binding.get("schema") == JOB_BINDING_SCHEMA_V2
                    else receipt["receipt"]
                ),
                "verified_generation": receipt,
            }
        )
    return tuple(result)


def parse_browser_candidate_plan(
    config: Mapping[str, Any],
    *,
    semantic_id: str,
    candidate_limit: int,
    candidate_target: int | None = None,
    workflow_name: str | None = None,
    workflow_fingerprint: str | None = None,
    prompt_id: str | None = None,
    worker_url: str | None = None,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
    trusted_store_root: str | Path | None = None,
) -> tuple[Dict[str, Any], tuple[Dict[str, Any], ...]]:
    """Return an immutable, server-planned slot inventory from job config.

    V1 remains readable as one immutable legacy/canary slot.  V2 is the only
    multi-candidate contract and derives every seed from task/action/index.
    """
    raw = config.get("browser_candidate_ingest") if isinstance(config, Mapping) else None
    if not isinstance(raw, dict):
        raise _error("job.config.browser_candidate_ingest is missing")
    schema = raw.get("schema")
    common_keys = (
        "schema",
        "task_id",
        "task_guid",
        "source_rig_type",
        "source_model_sha256",
        "source_skeleton_sha256",
        "frame_count",
    )
    if schema == JOB_BINDING_SCHEMA:
        # Historical V1 upload-only jobs predate the server-owned timing pin.
        # A V1 plan envelope, however, is canonical and must carry input_fps.
        timing_keys = (
            (*common_keys, "input_fps", "output_fps")
            if config.get("browser_candidate_job_plan") is not None
            else (*common_keys, "output_fps")
        )
        binding = _exact_object(
            raw,
            "job.config.browser_candidate_ingest",
            (
                *timing_keys,
                "candidate_seed",
                "source_video",
                "controlled_generation",
            ),
        )
        slots = (
            {
                "candidate_index": 0,
                "seed": _candidate_seed(binding["candidate_seed"]),
                "source_video": binding["source_video"],
                "controlled_generation": binding["controlled_generation"],
            },
        )
        verified = _verify_server_owned_job_plan(
            config,
            binding=binding,
            slots=slots,
            semantic_id=semantic_id,
            candidate_target=candidate_target,
            candidate_limit=candidate_limit,
            workflow_name=workflow_name,
            workflow_fingerprint=workflow_fingerprint,
            prompt_id=prompt_id,
            worker_url=worker_url,
            trusted_plan_inputs=trusted_plan_inputs,
            trusted_store_root=trusted_store_root,
        )
        return dict(binding), verified
    if schema != JOB_BINDING_SCHEMA_V2:
        raise _error("fitting job browser ingest binding schema is invalid")
    binding = _exact_object(
        raw,
        "job.config.browser_candidate_ingest",
        (
            *common_keys,
            "input_fps",
            "output_fps",
            "semantic_id",
            "generation_mode",
            "task",
            "prompt_contract",
            "reference_manifest",
            "workflow_name",
            "workflow_fingerprint_sha256",
            "candidate_slots",
        ),
    )
    task_id = _uuid(binding["task_id"], "binding.task_id")
    slots_raw = binding.get("candidate_slots")
    if not isinstance(slots_raw, list) or not slots_raw:
        raise _error("candidate_slots must be a non-empty array")
    if len(slots_raw) > candidate_limit:
        raise _error("candidate slot plan exceeds candidate_limit")
    slots = []
    indices = set()
    seeds = set()
    for offset, raw_slot in enumerate(slots_raw):
        slot = _exact_object(
            raw_slot,
            f"candidate_slots[{offset}]",
            (
                "candidate_index",
                "seed",
                "source_video",
                "controlled_generation",
            ),
        )
        index = slot.get("candidate_index")
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or not 0 <= index < candidate_limit
        ):
            raise _error(f"candidate_slots[{offset}].candidate_index is invalid")
        seed = _candidate_seed(slot.get("seed"))
        expected_seed = derive_browser_candidate_seed(task_id, semantic_id, index)
        if seed != expected_seed:
            raise _error(f"candidate_slots[{offset}].seed is not server-derived")
        if index in indices or seed in seeds:
            raise _error("candidate slot plan has duplicate index or seed")
        indices.add(index)
        seeds.add(seed)
        slots.append(
            {
                "candidate_index": index,
                "seed": seed,
                "source_video": slot["source_video"],
                "controlled_generation": slot["controlled_generation"],
            }
        )
    slots.sort(key=lambda row: row["candidate_index"])
    verified = _verify_server_owned_job_plan(
        config,
        binding=binding,
        slots=slots,
        semantic_id=semantic_id,
        candidate_target=candidate_target,
        candidate_limit=candidate_limit,
        workflow_name=workflow_name,
        workflow_fingerprint=workflow_fingerprint,
        prompt_id=prompt_id,
        worker_url=worker_url,
        trusted_plan_inputs=trusted_plan_inputs,
        trusted_store_root=trusted_store_root,
    )
    return dict(binding), verified


def resolve_browser_candidate_slot(
    config: Mapping[str, Any],
    *,
    semantic_id: str,
    candidate_limit: int,
    candidate_target: int | None = None,
    workflow_name: str | None = None,
    workflow_fingerprint: str | None = None,
    prompt_id: str | None = None,
    worker_url: str | None = None,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
    trusted_store_root: str | Path | None = None,
    seed: int,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    binding, slots = parse_browser_candidate_plan(
        config,
        semantic_id=semantic_id,
        candidate_limit=candidate_limit,
        candidate_target=candidate_target,
        workflow_name=workflow_name,
        workflow_fingerprint=workflow_fingerprint,
        prompt_id=prompt_id,
        worker_url=worker_url,
        trusted_plan_inputs=trusted_plan_inputs,
        trusted_store_root=trusted_store_root,
    )
    seed = _candidate_seed(seed)
    matches = [slot for slot in slots if slot["seed"] == seed]
    if len(matches) != 1:
        raise _error(
            "candidate request does not match the pinned job binding or immutable planned slot"
        )
    return binding, matches[0]


def _positive_float(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _error(f"{field} must be a positive finite number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise _error(f"{field} must be a positive finite number")
    return result


def _uuid(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not UUID_RE.fullmatch(result):
        raise _error(f"{field} must be a canonical UUID")
    return result


def _track_map(
    value: Any, field: str, frame_count: int, duration: float
) -> Dict[str, dict]:
    if not isinstance(value, list) or not value:
        raise _error(f"{field} must be a non-empty array")
    result: Dict[str, dict] = {}
    for index, row in enumerate(value):
        if not isinstance(row, dict):
            raise _error(f"{field}[{index}] must be an object")
        name = str(row.get("name") or "").strip()
        track_type = row.get("type")
        if not name or track_type not in ("quaternion", "vector") or name in result:
            raise _error(f"{field}[{index}] has an invalid or duplicate binding")
        times = row.get("times")
        values = row.get("values")
        item_size = 4 if track_type == "quaternion" else 3
        if not isinstance(times, list) or len(times) != frame_count:
            raise _error(f"{name}.times does not match frame_count")
        if any(
            isinstance(item, bool) or not isinstance(item, (int, float))
            for item in times
        ):
            raise _error(f"{name}.times contains a non-numeric value")
        parsed_times = [float(item) for item in times]
        if any(not math.isfinite(item) for item in parsed_times):
            raise _error(f"{name}.times contains a non-finite value")
        if abs(parsed_times[0]) > 1e-9 or abs(parsed_times[-1] - duration) > 1e-8:
            raise _error(f"{name}.times does not span the fitted duration")
        if any(
            parsed_times[i] <= parsed_times[i - 1] for i in range(1, len(parsed_times))
        ):
            raise _error(f"{name}.times is not strictly increasing")
        if not isinstance(values, list) or len(values) != frame_count * item_size:
            raise _error(f"{name}.values does not match its track type/timeline")
        if any(
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(float(item))
            for item in values
        ):
            raise _error(f"{name}.values contains an invalid number")
        if track_type == "quaternion":
            for frame_index in range(frame_count):
                offset = frame_index * item_size
                quaternion = [float(item) for item in values[offset : offset + 4]]
                norm = math.sqrt(sum(component * component for component in quaternion))
                if abs(norm - 1.0) > QUATERNION_NORM_TOLERANCE:
                    raise _error(f"{name} quaternion {frame_index} is not normalized")
        result[name] = row
    return result


def _track_has_motion(row: Mapping[str, Any]) -> bool:
    item_size = 4 if row["type"] == "quaternion" else 3
    values = [float(item) for item in row["values"]]
    first = values[:item_size]
    for offset in range(item_size, len(values), item_size):
        current = values[offset : offset + item_size]
        if row["type"] == "vector":
            if math.dist(first, current) > MOTION_EPSILON:
                return True
        else:
            dot = abs(sum(left * right for left, right in zip(first, current)))
            if 1.0 - min(1.0, dot) > MOTION_EPSILON:
                return True
    return False


def _validate_fitted_and_clip(
    fitted: Mapping[str, Any],
    clip: Mapping[str, Any],
    *,
    expected_loop: bool,
    expected_frame_count: int,
    expected_fps: float,
) -> tuple[float, Dict[str, dict]]:
    if fitted.get("schema") != FITTED_SCHEMA or fitted.get("loop") is not expected_loop:
        raise _error("fitted-animation.json schema/loop does not match the action")
    frame_count = _positive_int(fitted.get("frameCount"), "fitted frameCount")
    fps = _positive_float(fitted.get("fps"), "fitted fps")
    duration = _positive_float(fitted.get("durationSeconds"), "fitted durationSeconds")
    if frame_count != expected_frame_count or abs(fps - expected_fps) > 1e-9:
        raise _error("fitted timing does not match the pinned fitting job")
    if abs(duration - (frame_count - 1) / fps) > 1e-8:
        raise _error("fitted duration does not match frame_count/fps")
    fitted_rows = list(fitted.get("tracks") or []) + list(
        fitted.get("positionTracks") or []
    )
    if fitted.get("rootTrack") is not None:
        fitted_rows.append(fitted["rootTrack"])
    fitted_tracks = _track_map(fitted_rows, "fitted tracks", frame_count, duration)
    frames = fitted.get("frames")
    if not isinstance(frames, list) or len(frames) != frame_count:
        raise _error("fitted debug frames do not match frame_count")
    if any(
        not isinstance(row, dict) or row.get("frame") != index
        for index, row in enumerate(frames)
    ):
        raise _error("fitted debug frame chronology is invalid")
    if not isinstance(fitted.get("qa"), dict) or not fitted["qa"]:
        raise _error("fitted QA is missing")
    for field in (
        "targetSamples",
        "initialMeanTargetErrorPx",
        "finalMeanTargetErrorPx",
        "maximumTargetErrorPx",
        "maximumBoneLengthErrorPx",
        "maximumJointLimitViolationRad",
        "maximumContactSlidePx",
        "loopEndpointError",
    ):
        value = fitted["qa"].get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise _error(f"fitted QA {field} is missing or non-finite")
    clip_duration = _positive_float(clip.get("duration"), "Three clip duration")
    if (
        abs(clip_duration - duration) > 1e-8
        or not str(clip.get("name") or "").strip()
        or not str(clip.get("uuid") or "").strip()
        or isinstance(clip.get("blendMode"), bool)
        or not isinstance(clip.get("blendMode"), (int, float))
        or not math.isfinite(float(clip["blendMode"]))
    ):
        raise _error("Three clip header does not match fitted-animation.json")
    clip_tracks = _track_map(
        clip.get("tracks"), "Three clip tracks", frame_count, duration
    )
    if set(clip_tracks) != set(fitted_tracks):
        raise _error("Three clip track inventory differs from fitted-animation.json")
    for name in fitted_tracks:
        if _canonical_json(clip_tracks[name]) != _canonical_json(fitted_tracks[name]):
            raise _error(f"Three clip track {name} differs from fitted-animation.json")
    if not any(_track_has_motion(row) for row in clip_tracks.values()):
        raise _error("Three clip contains no nonzero animation")
    return duration, fitted_tracks


def _pin(payload: bytes, filename: str) -> Dict[str, Any]:
    return {"filename": filename, "bytes": len(payload), "sha256": _sha256(payload)}


def _validate_visual_evidence(
    report: Mapping[str, Any],
    deformation: Mapping[str, Any],
    *,
    rig_type: str,
    source_rig_type: str,
    semantic_id: str,
    skeleton_sha256: str,
    source_model_sha256: str,
    clip_pin: Mapping[str, Any],
    evidence_pins: Mapping[str, Mapping[str, Any]],
    frame_count: int,
) -> None:
    """Validate cross-file structure only; none of these assertions are trusted."""
    if report.get("schema") != VISUAL_QA_ENVELOPE_SCHEMA:
        raise _error("visual phase QA envelope schema is invalid")
    gate = report.get("visual_phase_gate")
    local = report.get("local_evidence")
    if not isinstance(gate, dict) or gate.get("schema") != VISUAL_QA_SCHEMA:
        raise _error("visual phase QA gate schema is invalid")
    if gate.get("rig_type") != rig_type or gate.get("semantic_id") != semantic_id:
        raise _error("visual phase QA rig/action does not match the fitting job")
    if gate.get("fitted_clip_sha256") != clip_pin["sha256"]:
        raise _error("visual phase QA does not bind the uploaded Three clip")
    if gate.get("decision") is not None or gate.get("reviewer") != {
        "id": None,
        "reviewed_at": None,
    }:
        raise _error("ingestion accepts only unreviewed machine-QA evidence")
    gate_camera = gate.get("camera") or {}
    if (
        gate_camera.get("static") is not True
        or gate_camera.get("root_motion_locked") is not True
        or gate_camera.get("settings_sha256")
        != evidence_pins["camera-settings.json"]["sha256"]
    ):
        raise _error("visual phase QA gate does not bind the fixed camera settings")
    separation = gate.get("coincident_rest_vertex_separation") or {}
    if (
        separation.get("measured") is not True
        or separation.get("pass") is not True
        or separation.get("report_sha256")
        != evidence_pins["deformation-report.json"]["sha256"]
    ):
        raise _error("visual phase QA gate does not bind a passing deformation report")
    if not isinstance(local, dict) or local.get("source_rig_type") != source_rig_type:
        raise _error("visual phase QA source rig does not match the job binding")
    if local.get("browser_only") is not True or local.get("blender_used") is not False:
        raise _error("visual phase QA must be browser-only")
    if local.get("animation_evaluation") != "Three.AnimationMixer":
        raise _error("visual phase QA must be evaluated by Three.AnimationMixer")
    approvals = local.get("approvals") or {}
    if (
        approvals.get("machine_qa_passed") is not True
        or approvals.get("ready_for_human_review") is not True
        or approvals.get("approved_for_animation_library") is not False
        or approvals.get("release_ready") is not False
    ):
        raise _error("uploaded visual QA does not declare pending human review")
    human_review = local.get("human_review") or {}
    if any(
        human_review.get(field) is not None
        for field in ("decision", "reviewer_id", "reviewed_at")
    ):
        raise _error("visual phase QA human review must remain unset during ingestion")
    inputs = local.get("immutable_inputs") or {}
    if (
        inputs.get("three_clip", {}).get("sha256") != clip_pin["sha256"]
        or inputs.get("skeleton", {}).get("sha256") != skeleton_sha256
        or inputs.get("source_model", {}).get("sha256") != source_model_sha256
    ):
        raise _error("visual phase QA immutable inputs do not match job provenance")
    camera = local.get("camera_settings") or {}
    target = (local.get("target_mesh_deformation_qa") or {}).get("report") or {}
    video = local.get("video") or {}
    for row, expected, field in (
        (camera, evidence_pins["camera-settings.json"], "camera settings"),
        (target, evidence_pins["deformation-report.json"], "deformation report"),
        (video, evidence_pins["fixed-camera-preview.mp4"], "fixed-camera preview"),
    ):
        if (
            row.get("sha256") != expected["sha256"]
            or row.get("bytes") != expected["bytes"]
        ):
            raise _error(f"visual phase QA {field} pin does not match uploaded bytes")
    if (
        video.get("fixed_camera") is not True
        or video.get("root_motion_locked") is not True
    ):
        raise _error("visual phase QA preview must use a static root-locked camera")
    if (
        deformation.get("schema") != "autorig.browser-horse-target-deformation-qa.v1"
        or deformation.get("passed") is not True
    ):
        raise _error("uploaded deformation report does not declare a pass")
    if deformation.get("inputs", {}).get("threeClipSha256") != clip_pin["sha256"]:
        raise _error("deformation report does not bind the uploaded Three clip")
    local_phases = local.get("phase_frames")
    gate_phases = gate.get("frames")
    if (
        not isinstance(local_phases, list)
        or not isinstance(gate_phases, list)
        or len(local_phases) != 3
        or len(gate_phases) != 3
    ):
        raise _error("visual phase QA must include exactly three required phases")
    if gate.get("required_phases") != list(PHASES):
        raise _error("visual phase QA required phase order is invalid")
    expected_indices = (0, (frame_count - 1) // 2, math.floor((frame_count - 1) * 0.75))
    for index, phase in enumerate(PHASES):
        expected = evidence_pins[f"phase-{phase}.png"]
        local_row, gate_row = local_phases[index], gate_phases[index]
        if (
            local_row.get("phase") != phase
            or gate_row.get("phase") != phase
            or local_row.get("frame_index") != expected_indices[index]
            or gate_row.get("frame_index") != expected_indices[index]
            or local_row.get("sha256") != expected["sha256"]
            or local_row.get("bytes") != expected["bytes"]
            or gate_row.get("sha256") != expected["sha256"]
            or gate_row.get("evidence_url") is not None
        ):
            raise _error(
                f"visual phase QA {phase} evidence does not match uploaded bytes"
            )


def _open_regular_file_no_follow(path: Path):
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise _error(f"artifact path is not a regular file: {path.name}")
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise


def _secure_existing_file(
    root: Path, candidate: Path, field: str
) -> tuple[Path, tuple[str, ...]]:
    lexical = Path(os.path.abspath(os.fspath(candidate)))
    try:
        relative = lexical.relative_to(root)
    except ValueError as exc:
        raise _error(f"{field} is outside ANIMATION_FITTING_JOBS_ROOT") from exc
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise _error(f"{field} must not traverse a symlink")
        if not current.exists():
            raise _error(f"{field} is missing")
        resolved = current.resolve(strict=True)
        if resolved != root and root not in resolved.parents:
            raise _error(f"{field} escapes ANIMATION_FITTING_JOBS_ROOT")
    if not lexical.is_file() or not stat.S_ISREG(os.lstat(lexical).st_mode):
        raise _error(f"{field} is not a regular file")
    return lexical, relative.parts


def _secure_directory_chain(root: Path, directory: Path, *, create: bool) -> Path:
    lexical = Path(os.path.abspath(os.fspath(directory)))
    try:
        relative = lexical.relative_to(root)
    except ValueError as exc:
        raise _error(
            "immutable browser candidate directory escapes the jobs root"
        ) from exc
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise _error("immutable browser candidate directory traverses a symlink")
        if not current.exists():
            if not create:
                raise _error("immutable browser candidate directory disappeared")
            try:
                current.mkdir()
            except FileExistsError:
                pass
        if current.is_symlink() or not current.is_dir():
            raise _error("immutable browser candidate ancestor is not a real directory")
        resolved = current.resolve(strict=True)
        if resolved != root and root not in resolved.parents:
            raise _error("immutable browser candidate directory escapes the jobs root")
    return lexical


def _read_source_video(
    root: Path,
    row: Mapping[str, Any],
    generation_job_id: str,
    generation_receipt: Mapping[str, Any] | None,
    *,
    verified_generation: Mapping[str, Any] | None,
    generation: Mapping[str, Any],
    seed: int,
    binding: Mapping[str, Any],
    worker_url: str,
    workflow_name: str,
) -> tuple[Path, Dict[str, Any]]:
    path_value = str(row.get("path") or "")
    candidate = Path(path_value)
    if not candidate.is_absolute():
        candidate = root / candidate
    path, relative_parts = _secure_existing_file(root, candidate, "pinned source video")
    video_sha = normalize_sha256(row.get("sha256"), "source_video.sha256")
    expected_tail = ("raw", video_sha[:2], f"{video_sha}.mp4")
    if tuple(relative_parts[-3:]) != expected_tail:
        raise _error(
            "pinned source video must use raw/<sha-prefix>/<sha>.mp4 content addressing"
        )
    expected_bytes = row.get("bytes")
    if (
        isinstance(expected_bytes, bool)
        or not isinstance(expected_bytes, int)
        or expected_bytes <= 0
        or expected_bytes > MAX_SOURCE_VIDEO_BYTES
    ):
        raise _error("pinned source video byte count exceeds the server limit", 413)
    before = os.lstat(path)
    digest = hashlib.sha256()
    total = 0
    with _open_regular_file_no_follow(path) as handle:
        while chunk := handle.read(1024 * 1024):
            total += len(chunk)
            if total > MAX_SOURCE_VIDEO_BYTES:
                raise _error("pinned source video exceeds the server limit", 413)
            digest.update(chunk)
    after = os.lstat(path)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise _error("pinned source video changed while it was read")
    pin = {"filename": "source-video.mp4", "bytes": total, "sha256": digest.hexdigest()}
    if row.get("sha256") != pin["sha256"] or row.get("bytes") != pin["bytes"]:
        raise _error(
            "server-computed source video integrity differs from the job binding"
        )
    if generation_receipt is not None:
        if verified_generation is None:
            raise _error("verified controlled-generation descriptor is missing")
        _verify_controlled_generation_receipt(
            root,
            generation_receipt,
            generation_job_id,
            verified_generation=verified_generation,
            generation=generation,
            seed=seed,
            binding=binding,
            worker_url=worker_url,
            workflow_name=workflow_name,
            source_video_path=path,
            source_video_pin=pin,
        )
    return path, pin


def _verify_controlled_generation_receipt(
    root: Path,
    row: Mapping[str, Any],
    generation_job_id: str,
    *,
    verified_generation: Mapping[str, Any],
    generation: Mapping[str, Any],
    seed: int,
    binding: Mapping[str, Any],
    worker_url: str,
    workflow_name: str,
    source_video_path: Path,
    source_video_pin: Mapping[str, Any],
) -> Dict[str, Any]:
    pin = _exact_object(
        row,
        "verified controlled-generation receipt",
        ("path", "sha256", "bytes"),
    )
    job_id = normalize_sha256(generation_job_id, "controlled_generation.job_id")
    digest = normalize_sha256(pin.get("sha256"), "generation_receipt.sha256")
    size = pin.get("bytes")
    if (
        isinstance(size, bool)
        or not isinstance(size, int)
        or size <= 0
        or size > MAX_JSON_BYTES
    ):
        raise _error("controlled-generation receipt byte count is invalid")
    path_value = str(pin.get("path") or "")
    candidate = Path(path_value)
    if not candidate.is_absolute():
        candidate = root / candidate
    path, relative_parts = _secure_existing_file(
        root, candidate, "verified controlled-generation receipt"
    )
    is_v2 = verified_generation.get("schema") == CONTROLLED_RECEIPT_SCHEMA_V2
    expected_filename = (
        str((generation.get("trusted_latest_state") or {}).get("filename") or "")
        if is_v2
        else relative_parts[-1] if relative_parts else ""
    )
    if tuple(relative_parts) != ("jobs", job_id, expected_filename) or not re.fullmatch(
        r"\d{6}\.json", expected_filename
    ):
        raise _error(
            "controlled-generation receipt must use jobs/<job_id>/<6-digit>.json"
        )
    if is_v2:
        state_binding = _exact_object(
            generation.get("trusted_latest_state"),
            "controlled_generation.trusted_latest_state",
            TRUSTED_LATEST_STATE_KEYS,
        )
        expected_sequence = _positive_int(
            state_binding.get("sequence"),
            "controlled_generation.trusted_latest_state.sequence",
        )
        if expected_filename != f"{expected_sequence:06d}.json":
            raise _error("controlled-generation state filename differs from its sequence")
        revisions = []
        for sibling in path.parent.iterdir():
            if re.fullmatch(r"\d{6}\.json", sibling.name):
                if sibling.is_symlink():
                    raise _error("controlled-generation state revision must not be a symlink")
                revisions.append(int(sibling.stem))
        if not revisions or max(revisions) != expected_sequence:
            raise _error("controlled-generation receipt is not the latest state revision")
    payload = _read_existing_bounded(path, size)
    if _sha256(payload) != digest:
        raise _error("controlled-generation receipt integrity differs from the job plan")
    state = _json_object(payload, "controlled-generation receipt state")
    mutable_keys = {
        "sequence_int",
        "recorded_at_unix_float",
        "status_string",
        "prompt_id_string",
        "resumed_existing_prompt_bool",
        "raw_video_path_string",
        "raw_video_sha256_string",
        "raw_video_bytes_int",
        "frame_paths_array",
        "frame_sha256_array",
        "backend_output_object",
    }
    identity = {key: value for key, value in state.items() if key not in mutable_keys}
    if _sha256(_canonical_json(identity)) != job_id:
        raise _error("controlled-generation state deterministic job identity drifted")
    prompt_material = f"autorig-controlled-animation-fitting:{job_id}"
    prompt_raw = hashlib.sha256(prompt_material.encode("utf-8")).hexdigest()[:32]
    deterministic_prompt = (
        f"{prompt_raw[:8]}-{prompt_raw[8:12]}-4{prompt_raw[13:16]}-"
        f"8{prompt_raw[17:20]}-{prompt_raw[20:32]}"
    )
    expected_worker_url = _normalize_worker_url(
        worker_url, "fitting_job.worker_url"
    )
    state_video_value = str(state.get("raw_video_path_string") or "")
    state_video_candidate = Path(state_video_value)
    if not state_video_candidate.is_absolute():
        state_video_candidate = root / state_video_candidate
    state_video_path, _ = _secure_existing_file(
        root, state_video_candidate, "controlled-generation state raw video"
    )
    v2_state_differs = False
    if is_v2:
        state_binding = generation["trusted_latest_state"]
        prompt_contract = generation.get("prompt_contract") or {}
        reference_manifest = generation.get("reference_manifest") or {}
        reference_content = reference_manifest.get("content") or {}
        reference_artifact = reference_content.get("reference_artifact") or {}
        experiment_spec = generation.get("experiment_spec") or {}
        experiment_content = experiment_spec.get("content") or {}
        experiment_pin = experiment_spec.get("pin") or {}
        experiment_differs = (
            generation.get("experiment_id")
            != experiment_content.get("experiment_id_string")
            or generation.get("experiment_sha256") != experiment_pin.get("sha256")
            or state.get("experiment_id_string") != generation.get("experiment_id")
            or state.get("experiment_sha256_string")
            != generation.get("experiment_sha256")
        )
        v2_state_differs = (
            state.get("schema") != state_binding.get("state_schema")
            or state.get("sequence_int") != state_binding.get("sequence")
            or state_binding.get("filename") != relative_parts[-1]
            or generation.get("semantic_id") != binding.get("semantic_id")
            or generation.get("generation_mode") != binding.get("generation_mode")
            or generation.get("task") != binding.get("task")
            or generation.get("prompt_contract") != binding.get("prompt_contract")
            or generation.get("reference_manifest")
            != binding.get("reference_manifest")
            or generation.get("workflow_name") != binding.get("workflow_name")
            or generation.get("workflow_fingerprint_sha256")
            != binding.get("workflow_fingerprint_sha256")
            or state.get("reference_sha256_string")
            != reference_artifact.get("sha256")
            or state.get("positive_prompt_sha256_string")
            != prompt_contract.get("positive_prompt_sha256")
            or state.get("negative_prompt_sha256_string")
            != prompt_contract.get("negative_prompt_sha256")
            or state.get("approval_state_string") != "generated_not_approved"
            or state.get("send_to_skeletal_fitting_bool") is not False
            or experiment_differs
        )
    legacy_experiment_differs = False
    if not is_v2:
        legacy_experiment_differs = (
            state.get("experiment_id_string") != generation.get("experiment_id")
            or normalize_sha256(
                state.get("experiment_sha256_string"),
                "state.experiment_sha256_string",
            )
            != normalize_sha256(
                generation.get("experiment_sha256"),
                "controlled_generation.experiment_sha256",
            )
        )
    if (
        state.get("status_string") != "completed"
        or state.get("prompt_id_string") != deterministic_prompt
        or state.get("prompt_id_string") != generation.get("prompt_id")
        or state.get("worker_id_string") != generation.get("worker_id")
        or state.get("worker_id_string") != verified_generation.get("worker_id")
        or _normalize_worker_url(
            state.get("worker_base_url_string"), "state.worker_base_url_string"
        )
        != expected_worker_url
        or _normalize_worker_url(
            verified_generation.get("worker_base_url"),
            "verified_generation.worker_base_url",
        )
        != expected_worker_url
        or state.get("workflow_name_string") != workflow_name
        or state.get("workflow_name_string") != verified_generation.get("workflow_name")
        or normalize_sha256(
            state.get("workflow_fingerprint_string"),
            "state.workflow_fingerprint_string",
        )
        != normalize_sha256(
            generation.get("workflow_fingerprint_sha256"),
            "controlled_generation.workflow_fingerprint_sha256",
        )
        or state.get("seed_int") != seed
        or legacy_experiment_differs
        or state_video_path != source_video_path
        or state.get("raw_video_sha256_string") != source_video_pin["sha256"]
        or state.get("raw_video_bytes_int") != source_video_pin["bytes"]
        or state.get("frame_count_int") != binding.get("frame_count")
        or state.get("input_fps_int") != binding.get("input_fps")
        or state.get("output_fps_int") != binding.get("output_fps")
        or v2_state_differs
    ):
        raise _error("controlled-generation state differs from its canonical job plan")
    return {"filename": relative_parts[-1], "bytes": size, "sha256": digest}


def _write_file(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _copy_file(source: Path, target: Path, expected: Mapping[str, Any]) -> None:
    digest = hashlib.sha256()
    total = 0
    with (
        _open_regular_file_no_follow(source) as read_handle,
        target.open("xb") as write_handle,
    ):
        while chunk := read_handle.read(1024 * 1024):
            digest.update(chunk)
            total += len(chunk)
            write_handle.write(chunk)
        write_handle.flush()
        os.fsync(write_handle.fileno())
    if total != expected["bytes"] or digest.hexdigest() != expected["sha256"]:
        target.unlink(missing_ok=True)
        raise _error("source video changed while the immutable bundle was copied")


def _read_existing_bounded(path: Path, expected_bytes: int) -> bytes:
    if path.is_symlink():
        raise _error(f"immutable browser candidate artifact is a symlink: {path.name}")
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_size != expected_bytes:
        raise _error(f"immutable browser candidate artifact changed: {path.name}")
    with _open_regular_file_no_follow(path) as handle:
        payload = handle.read(expected_bytes + 1)
    after = os.lstat(path)
    if len(payload) != expected_bytes or (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise _error(f"immutable browser candidate artifact changed: {path.name}")
    return payload


def _read_trusted_pin(
    root: Path,
    value: Any,
    field: str,
    *,
    maximum: int = MAX_JSON_BYTES,
) -> tuple[Path, bytes]:
    pin = _exact_object(value, field, ("path", "sha256", "bytes"))
    digest = normalize_sha256(pin.get("sha256"), f"{field}.sha256")
    size = pin.get("bytes")
    if isinstance(size, bool) or not isinstance(size, int) or not 0 < size <= maximum:
        raise _error(f"{field}.bytes is outside the trusted store limit")
    candidate = Path(str(pin.get("path") or ""))
    if not candidate.is_absolute():
        candidate = root / candidate
    path, _ = _secure_existing_file(root, candidate, field)
    payload = _read_existing_bounded(path, size)
    if _sha256(payload) != digest:
        raise _error(f"{field} integrity differs from the independent resolver")
    return path, payload


def _verify_canonical_trusted_wrapper(
    root: Path, value: Any, field: str
) -> Mapping[str, Any]:
    wrapper = _exact_object(value, field, ("content", "pin"))
    content = wrapper.get("content")
    if not isinstance(content, Mapping):
        raise _error(f"{field}.content must be an object")
    _, payload = _read_trusted_pin(root, wrapper.get("pin"), f"{field}.pin")
    if payload != _canonical_json(content):
        raise _error(f"{field} file is not the exact canonical resolver content")
    return wrapper


def _verify_trusted_plan_files(
    root_input: Path, trust: BrowserCandidatePlanTrust
) -> None:
    if root_input.is_symlink() or not root_input.is_dir():
        raise _error("trusted browser candidate store root must be a real directory")
    root = root_input.resolve(strict=True)
    if trust.reference_manifest is not None:
        reference = _verify_canonical_trusted_wrapper(
            root, trust.reference_manifest, "trusted_reference_manifest"
        )
        content = reference.get("content") or {}
        _read_trusted_pin(
            root,
            content.get("reference_artifact"),
            "trusted_reference_manifest.reference_artifact",
            maximum=MAX_PREVIEW_BYTES,
        )
    if isinstance(trust.latest_states, (str, bytes, bytearray)) or not isinstance(
        trust.latest_states, Sequence
    ):
        raise _error("trusted latest-state resolver output must be a sequence")
    for index, state in enumerate(trust.latest_states):
        if not isinstance(state, Mapping):
            raise _error(f"trusted_latest_states[{index}] must be an object")
        _, payload = _read_trusted_pin(
            root,
            state.get("pin"),
            f"trusted_latest_states[{index}].pin",
        )
        parsed = _json_object(payload, f"trusted_latest_states[{index}] state")
        if parsed.get("schema") != state.get("state_schema"):
            raise _error("trusted latest-state file schema differs from its resolver")
        if parsed.get("sequence_int") != state.get("sequence"):
            raise _error("trusted latest-state file sequence differs from its resolver")
    if trust.retry_authorization is not None:
        authorization = _verify_canonical_trusted_wrapper(
            root, trust.retry_authorization, "trusted_retry_authorization"
        )
        content = authorization.get("content") or {}
        for key in (
            "first_batch_selection_closure",
            "first_batch_qa_closure",
        ):
            _, payload = _read_trusted_pin(
                root,
                content.get(key),
                f"trusted_retry_authorization.{key}",
            )
            _json_object(payload, f"trusted_retry_authorization.{key}")


def _verify_existing(
    directory: Path,
    manifest_bytes: bytes,
    files: Mapping[str, bytes],
    source_pin: Mapping[str, Any],
) -> None:
    if directory.is_symlink() or not directory.is_dir():
        raise _error("immutable browser candidate path is not a real directory")
    manifest_path = directory / "candidate-manifest.json"
    if (
        not manifest_path.is_file()
        or _read_existing_bounded(manifest_path, len(manifest_bytes)) != manifest_bytes
    ):
        raise _error("immutable browser candidate identity collision")
    expected_names = sorted([*files, source_pin["filename"], "candidate-manifest.json"])
    entries = tuple(directory.iterdir())
    if sorted(path.name for path in entries) != expected_names:
        raise _error("immutable browser candidate bundle inventory changed")
    if any(path.is_symlink() or not path.is_file() for path in entries):
        raise _error("immutable browser candidate bundle contains a non-regular file")
    for filename, payload in files.items():
        if _read_existing_bounded(directory / filename, len(payload)) != payload:
            raise _error(f"immutable browser candidate artifact changed: {filename}")
    source = directory / source_pin["filename"]
    if not source.is_file():
        raise _error("immutable browser candidate source video changed")
    digest = hashlib.sha256(
        _read_existing_bounded(source, source_pin["bytes"])
    ).hexdigest()
    if digest != source_pin["sha256"]:
        raise _error("immutable browser candidate source video changed")


async def _recheck_publish_lifecycle(
    db: AsyncSession,
    job_id: str,
    version_id: int,
    rig_type: str,
    *,
    allow_review_transition: bool,
) -> bool:
    # SQLite runs in WAL mode, so end the earlier read snapshot before the
    # publication query.  The public entry point requires a clean session.
    await db.rollback()
    row = (
        await db.execute(
            select(AnimalAnimationFittingJob, AnimalAnimationLibraryVersion)
            .join(
                AnimalAnimationLibraryVersion,
                AnimalAnimationLibraryVersion.id
                == AnimalAnimationFittingJob.library_version_id,
            )
            .where(AnimalAnimationFittingJob.id == job_id)
            .with_for_update()
        )
    ).one_or_none()
    if row is None:
        raise _error("fitting job disappeared before immutable publication")
    current_job, current_version = row
    allowed_statuses = (
        {"queued", "generating", "review"}
        if allow_review_transition
        else {"review"}
    )
    if (
        current_job.status not in allowed_statuses
        or current_job.library_version_id != version_id
        or current_version.status != "draft"
        or normalize_rig_type(current_job.rig_type) != rig_type
        or normalize_rig_type(current_version.rig_type) != rig_type
    ):
        raise _error("fitting job or draft library changed before publication")
    transitioned = current_job.status in {"queued", "generating"}
    if transitioned:
        current_job.status = "review"
        await db.flush()
    return transitioned


async def ingest_browser_candidate_artifacts(
    db: AsyncSession,
    *,
    job_id: str,
    seed: int,
    artifacts: BrowserCandidateArtifactSet,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
    trusted_plan_inputs: BrowserCandidatePlanTrust | None = None,
) -> IngestedBrowserCandidate:
    """Structurally validate and atomically capture an untrusted upload bundle."""
    if db.new or db.dirty or db.deleted:
        raise _error("browser candidate ingestion requires a clean database session")
    _uuid(job_id, "job_id")
    seed = _candidate_seed(seed)
    job = (
        await db.execute(
            select(AnimalAnimationFittingJob).where(
                AnimalAnimationFittingJob.id == job_id
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise _error("Animation fitting job not found", 404)
    version = (
        await db.execute(
            select(AnimalAnimationLibraryVersion).where(
                AnimalAnimationLibraryVersion.id == job.library_version_id
            )
        )
    ).scalar_one()
    if (
        job.status not in {"queued", "generating", "review"}
        or version.status != "draft"
        or version.rig_type != job.rig_type
    ):
        raise _error(
            "browser uploads require a review job and its matching draft library"
        )
    rig_type = normalize_rig_type(job.rig_type)
    clip_contract = taxonomy_clip(job.semantic_id)
    root_input = Path(fitting_jobs_root)
    if root_input.is_symlink():
        raise _error("ANIMATION_FITTING_JOBS_ROOT must not be a symlink")
    root_input.mkdir(parents=True, exist_ok=True)
    if root_input.is_symlink() or not root_input.is_dir():
        raise _error("ANIMATION_FITTING_JOBS_ROOT must be a real directory")
    root = root_input.resolve(strict=True)
    config = _json_object(
        (job.config_json or "{}").encode("utf-8"), "fitting job config"
    )
    binding, candidate_slot = resolve_browser_candidate_slot(
        config,
        semantic_id=job.semantic_id,
        candidate_limit=int(job.candidate_limit),
        candidate_target=int(job.candidate_target),
        workflow_name=job.workflow_name,
        workflow_fingerprint=job.workflow_fingerprint,
        prompt_id=job.prompt_id,
        worker_url=job.worker_url,
        trusted_plan_inputs=trusted_plan_inputs,
        trusted_store_root=root,
        seed=seed,
    )
    if job.status != "review" and binding.get("schema") != JOB_BINDING_SCHEMA_V2:
        raise _error("only a resolver-verified V2 job may enter review on admission")
    candidate_index = candidate_slot["candidate_index"]
    task_id = _uuid(binding["task_id"], "binding.task_id")
    task_guid = _uuid(binding["task_guid"], "binding.task_guid")
    task = (
        await db.execute(select(Task).where(Task.id == task_id))
    ).scalar_one_or_none()
    if (
        task is None
        or str(task.guid or "").lower() != task_guid
        or task.status != "done"
        or str(task.input_type or "").strip().lower() != "animal"
    ):
        raise _error(
            "pinned source task must be a completed animal task with the exact GUID"
        )
    skeleton_sha = normalize_sha256(
        binding["source_skeleton_sha256"], "source_skeleton_sha256"
    )
    source_model_sha = normalize_sha256(
        binding["source_model_sha256"], "source_model_sha256"
    )
    if skeleton_sha != version.template_skeleton_sha256:
        raise _error("source skeleton does not match the library template skeleton")
    source_rig_type = str(binding["source_rig_type"] or "").strip()
    if not source_rig_type:
        raise _error("source_rig_type is missing")
    frame_count = _positive_int(binding["frame_count"], "binding.frame_count")
    if frame_count != int(clip_contract["frame_profile"]):
        raise _error("job frame_count does not match the canonical action profile")
    input_fps = None
    if "input_fps" in binding:
        input_fps = _positive_int(binding["input_fps"], "binding.input_fps")
        if input_fps != 24:
            raise _error("job input_fps must match the canonical 24 fps source timing")
    output_fps = _positive_float(binding["output_fps"], "binding.output_fps")
    verified_generation = candidate_slot.get("verified_generation")
    is_v2 = (
        isinstance(verified_generation, Mapping)
        and verified_generation.get("schema") == CONTROLLED_RECEIPT_SCHEMA_V2
    )
    generation_keys = CONTROLLED_GENERATION_V2_KEYS if is_v2 else (
        "job_id",
        "prompt_id",
        "experiment_id",
        "experiment_sha256",
        "workflow_fingerprint_sha256",
        *(
            ("worker_id", "worker_base_url")
            if verified_generation is not None
            else ()
        ),
    )
    generation = _exact_object(
        candidate_slot["controlled_generation"],
        "binding.controlled_generation",
        generation_keys,
    )
    generation = dict(generation)
    digest_fields = ["job_id", "workflow_fingerprint_sha256"]
    if not is_v2:
        digest_fields.append("experiment_sha256")
    for field in digest_fields:
        generation[field] = normalize_sha256(
            generation[field], f"controlled_generation.{field}"
        )
    if generation["workflow_fingerprint_sha256"] != normalize_sha256(
        job.workflow_fingerprint, "fitting_job.workflow_fingerprint"
    ):
        raise _error("controlled generation workflow differs from the fitting job")
    if not str(generation["prompt_id"] or "").strip():
        raise _error("controlled generation prompt identity is missing")
    if not is_v2 and not str(generation["experiment_id"] or "").strip():
        raise _error("controlled generation experiment identity is missing")
    if is_v2:
        expected_mode = "loop" if clip_contract.get("loop") is True else "one_shot"
        state_binding = _exact_object(
            generation.get("trusted_latest_state"),
            "controlled_generation.trusted_latest_state",
            TRUSTED_LATEST_STATE_KEYS,
        )
        state_pin = _exact_object(
            state_binding.get("pin"),
            "controlled_generation.trusted_latest_state.pin",
            ("path", "sha256", "bytes"),
        )
        if (
            generation.get("semantic_id") != job.semantic_id
            or generation.get("semantic_id") != binding.get("semantic_id")
            or generation.get("generation_mode") != expected_mode
            or generation.get("generation_mode") != binding.get("generation_mode")
            or generation.get("task") != binding.get("task")
            or generation.get("prompt_contract") != binding.get("prompt_contract")
            or generation.get("reference_manifest")
            != binding.get("reference_manifest")
            or generation.get("workflow_name") != job.workflow_name
            or generation.get("workflow_name") != binding.get("workflow_name")
            or generation.get("workflow_fingerprint_sha256")
            != binding.get("workflow_fingerprint_sha256")
            or state_binding != verified_generation.get("trusted_latest_state")
            or state_pin != candidate_slot.get("verified_generation_receipt")
        ):
            raise _error("controlled generation provenance differs from its V2 receipt")
        _verify_canonical_trusted_wrapper(
            root,
            generation.get("experiment_spec"),
            "controlled_generation.experiment_spec",
        )
    if verified_generation is not None:
        if (
            not str(generation.get("worker_id") or "").strip()
            or _normalize_worker_url(
                generation.get("worker_base_url"),
                "controlled_generation.worker_base_url",
            )
            != _normalize_worker_url(job.worker_url, "fitting_job.worker_url")
        ):
            raise _error("controlled generation worker differs from the fitting job")

    source_path, source_pin = _read_source_video(
        root,
        _exact_object(
            candidate_slot["source_video"],
            "candidate_slot.source_video",
            ("path", "sha256", "bytes"),
        ),
        generation["job_id"],
        candidate_slot.get("verified_generation_receipt"),
        verified_generation=verified_generation,
        generation=generation,
        seed=seed,
        binding=binding,
        worker_url=job.worker_url,
        workflow_name=job.workflow_name,
    )
    fitted_bytes = _read_artifact(
        artifacts.fitted_animation_json, "fitted_animation_json", MAX_JSON_BYTES
    )
    clip_bytes = _read_artifact(
        artifacts.three_clip_json, "three_clip_json", MAX_JSON_BYTES
    )
    qa_bytes = _read_artifact(
        artifacts.visual_phase_qa_json, "visual_phase_qa_json", MAX_QA_JSON_BYTES
    )
    camera_bytes = _read_artifact(
        artifacts.camera_settings_json, "camera_settings_json", MAX_QA_JSON_BYTES
    )
    deformation_bytes = _read_artifact(
        artifacts.deformation_report_json, "deformation_report_json", MAX_QA_JSON_BYTES
    )
    preview_bytes = _read_artifact(
        artifacts.fixed_camera_preview_mp4,
        "fixed_camera_preview_mp4",
        MAX_PREVIEW_BYTES,
    )
    is_mp4 = len(preview_bytes) >= 12 and preview_bytes[4:8] == b"ftyp"
    is_matroska = preview_bytes.startswith(b"\x1aE\xdf\xa3")
    if not (is_mp4 or is_matroska):
        raise _error(
            "fixed_camera_preview_mp4 is not a recognizable video container", 400
        )
    if tuple(artifacts.phase_frames) != PHASES:
        raise _error(
            "phase_frames must contain start/middle/three_quarter in order", 400
        )
    files: Dict[str, bytes] = {
        "fitted-animation.json": fitted_bytes,
        "three-clip.json": clip_bytes,
        "visual-phase-qa.json": qa_bytes,
        "camera-settings.json": camera_bytes,
        "deformation-report.json": deformation_bytes,
        "fixed-camera-preview.mp4": preview_bytes,
    }
    for phase, source in artifacts.phase_frames.items():
        payload = _read_artifact(source, f"phase_frames.{phase}", MAX_PNG_BYTES)
        if not payload.startswith(b"\x89PNG\r\n\x1a\n"):
            raise _error(f"phase_frames.{phase} is not PNG", 400)
        files[f"phase-{phase}.png"] = payload
    if sum(len(value) for value in files.values()) > MAX_TOTAL_INGEST_BYTES:
        raise _error("browser candidate artifact set exceeds the total size limit", 413)
    if (
        sum(len(value) for value in files.values()) + source_pin["bytes"]
        > MAX_TOTAL_INGEST_BYTES
    ):
        raise _error("browser candidate bundle exceeds the total size limit", 413)
    fitted = _json_object(fitted_bytes, "fitted-animation.json")
    three_clip = _json_object(clip_bytes, "three-clip.json")
    visual_qa = _json_object(qa_bytes, "visual-phase-qa.json")
    _json_object(camera_bytes, "camera-settings.json")
    deformation = _json_object(deformation_bytes, "deformation-report.json")
    duration, tracks = _validate_fitted_and_clip(
        fitted,
        three_clip,
        expected_loop=bool(clip_contract["loop"]),
        expected_frame_count=frame_count,
        expected_fps=output_fps,
    )
    pins = {filename: _pin(payload, filename) for filename, payload in files.items()}
    _validate_visual_evidence(
        visual_qa,
        deformation,
        rig_type=rig_type,
        source_rig_type=source_rig_type,
        semantic_id=job.semantic_id,
        skeleton_sha256=skeleton_sha,
        source_model_sha256=source_model_sha,
        clip_pin=pins["three-clip.json"],
        evidence_pins=pins,
        frame_count=frame_count,
    )
    binding_manifest = {
        "schema": BUNDLE_SCHEMA,
        "library": {
            "version_id": version.id,
            "revision": version.revision,
            "rig_type": rig_type,
            "template_skeleton_sha256": version.template_skeleton_sha256,
        },
        "fitting_job": {
            "id": job.id,
            "semantic_id": job.semantic_id,
            "workflow_name": job.workflow_name,
            "workflow_fingerprint": job.workflow_fingerprint,
        },
        "source_task": {"id": task_id, "guid": task_guid},
        "candidate": {
            "candidate_index": candidate_index,
            "seed": seed,
            "source_rig_type": source_rig_type,
            "source_model_sha256": source_model_sha,
            "source_skeleton_sha256": skeleton_sha,
            "frame_count": frame_count,
            **({"input_fps": input_fps} if input_fps is not None else {}),
            "fps": output_fps,
            "duration_seconds": duration,
            "track_count": len(tracks),
            "review_state": "uploaded_pending_server_validation",
            "uploaded_qa_assertions_trusted": False,
            "server_validation": {
                "status": "pending",
                "required": [
                    "task_model_sha256_binding",
                    "task_skeleton_sha256_binding",
                    "media_decode_and_phase_extraction",
                    "deformation_recompute",
                    "visual_review",
                ],
            },
        },
        "controlled_generation": generation,
        "artifacts": {**pins, source_pin["filename"]: source_pin},
    }
    identity = _sha256(_canonical_json(binding_manifest))
    manifest = {**binding_manifest, "identity_sha256": identity}
    manifest_bytes = _canonical_json(manifest) + b"\n"
    manifest_sha = _sha256(manifest_bytes)
    target_parent = _secure_directory_chain(
        root,
        root / job.id / "browser-candidates" / identity[:2],
        create=True,
    )
    target = target_parent / identity
    staging_parent = _secure_directory_chain(
        root,
        root / job.id / "browser-candidate-upload-staging",
        create=True,
    )
    staging = Path(tempfile.mkdtemp(prefix=f".{identity}.", dir=str(staging_parent)))
    try:
        for filename, payload in files.items():
            _write_file(staging / filename, payload)
        _copy_file(source_path, staging / source_pin["filename"], source_pin)
        _write_file(staging / "candidate-manifest.json", manifest_bytes)
        # Lazy import avoids the review -> ingest -> selection import cycle.
        import animation_fitting_candidate_selection as candidate_selection

        async with candidate_selection.async_candidate_publication_lock(
            job_id=job_id, fitting_jobs_root=str(root)
        ):
            candidate_selection.assert_candidate_publication_open(
                job_id=job_id, fitting_jobs_root=str(root)
            )
            transitioned = await _recheck_publish_lifecycle(
                db,
                job_id,
                version.id,
                rig_type,
                allow_review_transition=binding.get("schema") == JOB_BINDING_SCHEMA_V2,
            )
            selection_snapshot = await candidate_selection._load_job(
                db,
                job_id,
                fitting_jobs_root=str(root),
                trusted_plan_inputs=trusted_plan_inputs,
            )
            _secure_directory_chain(root, target.parent, create=False)
            if target.is_symlink():
                raise _error("immutable browser candidate target must not be a symlink")
            created = False
            if target.exists():
                _verify_existing(target, manifest_bytes, files, source_pin)
            else:
                try:
                    staging.rename(target)
                    created = True
                except OSError:
                    if not target.is_dir():
                        raise
                    _verify_existing(target, manifest_bytes, files, source_pin)
            admission = None
            try:
                admission = candidate_selection._admit_browser_candidate_locked(
                    root=root,
                    snapshot=selection_snapshot,
                    candidate_index=candidate_index,
                    candidate_identity_sha256=identity,
                )
                if transitioned:
                    await db.commit()
            except Exception:
                await db.rollback()
                if admission is not None and admission.created:
                    shutil.rmtree(admission.directory, ignore_errors=True)
                # Bundle and admission are one logical publication.  If this
                # invocation performed the rename, move it back to staging so
                # FINAL can never observe an unadmitted late bundle.
                if created and target.is_dir() and not target.is_symlink():
                    target.rename(staging)
                raise
        shutil.rmtree(staging, ignore_errors=True)
        return IngestedBrowserCandidate(
            identity,
            target,
            target / "candidate-manifest.json",
            manifest_sha,
            manifest,
            created,
        )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

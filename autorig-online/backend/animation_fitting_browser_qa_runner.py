from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import signal
import struct
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence


RUN_RECEIPT_SCHEMA = "autorig.animation-fitting-browser-horse-qa-run.v1"
REQUEST_RECEIPT_SCHEMA = "autorig.animation-fitting-browser-horse-qa-request.v1"
VISUAL_EVIDENCE_SCHEMA = "autorig.browser-horse-visual-phase-evidence-envelope.v1"
VISUAL_GATE_SCHEMA = "autorig.animation-visual-phase-qa.v1"
DEFORMATION_SCHEMA = "autorig.browser-horse-target-deformation-qa.v1"
CAMERA_SCHEMA = "autorig.browser-horse-fixed-camera.v1"
QA_PROFILE_SCHEMA = "autorig.animation-fitting-qa.v1"
PHASES = ("start", "middle", "three_quarter")
FRAME_COUNT = 49
V14_HORSE_RIG_TYPE = "horse"
V14_HORSE_SOURCE_RIG_TYPE = "HORSE_2"
V14_HORSE_SEMANTIC_ID = "walk_forward"
V14_HORSE_FRAME_COUNT = 49
V14_HORSE_OUTPUT_FPS = 30.0
V14_HORSE_LOOP = True
V14_HORSE_DEFORMATION_THRESHOLDS = MappingProxyType(
    {
        "maximumEdgeStretch": 5.0,
        "p99EdgeStretch": 2.5,
        "zeroWeightVertices": 0,
        "coincidentRestSeparationM": 0.04,
    }
)
V14_HORSE_QA_SCOPE = "horse_v14_walk_forward_nonproduction_canary"
V14_HORSE_QA_CALIBRATION_STATE = "provisional-horse-v1"
MAX_CAPTURE_BYTES = 1024 * 1024
MAX_JSON_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_PNG_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_MP4_ARTIFACT_BYTES = 256 * 1024 * 1024
MAX_OUTPUT_INVENTORY_BYTES = 512 * 1024 * 1024
POST_KILL_WAIT_SECONDS = 5.0
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
SEMANTIC_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,63}$")
WINDOWS_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{index}" for index in range(1, 10)}
    | {f"LPT{index}" for index in range(1, 10)}
)
EXPECTED_MACHINE_GATE_KEYS = frozenset(
    {
        "maximumEdgeStretch",
        "p99EdgeStretch",
        "zeroWeightVertices",
        "coincidentRestSeparation",
        "rootMotionLocked",
        "cameraStatic",
    }
)


class BrowserHorseQaError(RuntimeError):
    """Base error for a fail-closed browser Horse QA run."""


class BrowserHorseQaPathError(BrowserHorseQaError):
    """A configured or produced path is unsafe or outside its canonical root."""


class BrowserHorseQaStaleOutputError(BrowserHorseQaError):
    """The immutable attempt directory already exists."""


class BrowserHorseQaTimeoutError(BrowserHorseQaError):
    """The browser QA subprocess exceeded the configured server timeout."""


class BrowserHorseQaSubprocessError(BrowserHorseQaError):
    """The pinned browser QA command failed before producing a QA result."""


class BrowserHorseQaContractError(BrowserHorseQaError):
    """The subprocess output does not satisfy the trusted evidence contract."""


@dataclass(frozen=True)
class BrowserHorseQaRunnerConfig:
    input_root: Path
    output_root: Path
    bundle_directory: Path
    runner_executable: Path
    expected_runner_executable_sha256: str
    runner_script: Path
    expected_runner_script_sha256: str
    qa_profile_path: Path
    expected_qa_profile_sha256: str
    three_module: Path
    expected_three_module_sha256: str
    chrome_executable: Path
    expected_chrome_executable_sha256: str
    ffmpeg_executable: Path
    expected_ffmpeg_executable_sha256: str
    ffprobe_executable: Path
    expected_ffprobe_executable_sha256: str
    expected_immutable_manifest_sha256: str
    expected_fitting_bundle_sha256: str
    expected_source_model_sha256: str
    timeout_seconds: float = 900.0
    expected_three_revision: str = "160"


@dataclass(frozen=True)
class BrowserHorseQaRequest:
    job_id: str
    candidate_id: str
    attempt_id: str
    semantic_id: str
    expected_three_clip_sha256: str


@dataclass(frozen=True)
class BrowserHorseQaPaths:
    candidate_input_directory: Path
    three_clip_path: Path
    attempt_directory: Path
    qa_output_directory: Path
    request_receipt_path: Path
    run_receipt_path: Path


@dataclass(frozen=True)
class BrowserHorseQaResult:
    machine_qa_passed: bool
    ready_for_human_review: bool
    human_visual_decision: None
    approved_for_animation_library: bool
    machine_gates: Mapping[str, bool]
    attempt_directory: Path
    qa_output_directory: Path
    evidence_path: Path
    video_path: Path
    run_receipt_path: Path
    run_receipt_sha256: str
    run_receipt_bytes: int


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _require_sha256(value: str, field: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise BrowserHorseQaContractError(f"{field} must be a lowercase SHA-256")
    return value


def _require_identifier(value: str, field: str) -> str:
    windows_stem = value.split(".", 1)[0].upper() if isinstance(value, str) else ""
    if (
        not isinstance(value, str)
        or not IDENTIFIER_RE.fullmatch(value)
        or value in {".", ".."}
        or value.endswith(".")
        or windows_stem in WINDOWS_RESERVED_NAMES
    ):
        raise BrowserHorseQaPathError(f"{field} is not a safe server identifier")
    return value


def _require_semantic_id(value: str) -> str:
    if not isinstance(value, str) or not SEMANTIC_ID_RE.fullmatch(value):
        raise BrowserHorseQaContractError("semantic_id is invalid")
    return value


def _is_reparse_or_symlink(path: Path) -> bool:
    try:
        stat_result = os.lstat(path)
    except FileNotFoundError:
        return False
    attributes = getattr(stat_result, "st_file_attributes", 0)
    reparse_flag = getattr(stat_result, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(attributes & reparse_flag)


def _absolute_without_resolution(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _reject_link_components(path: Path) -> None:
    absolute = _absolute_without_resolution(path)
    components: list[Path] = []
    current = absolute
    while True:
        components.append(current)
        if current.parent == current:
            break
        current = current.parent
    for component in reversed(components):
        if component.exists() and _is_reparse_or_symlink(component):
            raise BrowserHorseQaPathError(
                f"symlink/reparse paths are forbidden: {component}"
            )


def _canonical_existing_directory(path: Path, field: str) -> Path:
    _reject_link_components(path)
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise BrowserHorseQaPathError(f"{field} does not exist: {path}") from exc
    if not resolved.is_dir():
        raise BrowserHorseQaPathError(f"{field} must be a directory: {resolved}")
    return resolved


def _canonical_existing_file(path: Path, field: str) -> Path:
    _reject_link_components(path)
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as exc:
        raise BrowserHorseQaPathError(f"{field} does not exist: {path}") from exc
    if not resolved.is_file() or _is_reparse_or_symlink(resolved):
        raise BrowserHorseQaPathError(
            f"{field} must be a regular non-link file: {resolved}"
        )
    return resolved


def _is_within(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((os.fspath(path), os.fspath(root))) == os.fspath(root)
    except ValueError:
        return False


def _require_within(path: Path, root: Path, field: str) -> Path:
    if not _is_within(path, root):
        raise BrowserHorseQaPathError(f"{field} escapes its canonical server root")
    return path


def _reject_tree_links(root: Path) -> None:
    for directory, directory_names, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in (*directory_names, *filenames):
            child = directory_path / name
            if _is_reparse_or_symlink(child):
                raise BrowserHorseQaPathError(
                    f"symlink/reparse artifact is forbidden: {child}"
                )


def _read_snapshot(
    path: Path,
    field: str,
    *,
    max_bytes: int | None = None,
) -> tuple[bytes, Mapping[str, Any]]:
    canonical = _canonical_existing_file(path, field)
    before = canonical.stat()
    if max_bytes is not None and before.st_size > max_bytes:
        raise BrowserHorseQaContractError(
            f"{field} exceeds the {max_bytes}-byte trusted artifact cap"
        )
    data = canonical.read_bytes()
    after = canonical.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ino != after.st_ino
        or len(data) != after.st_size
    ):
        raise BrowserHorseQaContractError(f"{field} changed while it was read")
    return data, MappingProxyType(
        {
            "path": os.fspath(canonical),
            "bytes": len(data),
            "sha256": _sha256(data),
        }
    )


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _reject_duplicate_json_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key}")
        result[key] = value
    return result


def _parse_json(data: bytes, field: str) -> Mapping[str, Any]:
    try:
        value = json.loads(
            data.decode("utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BrowserHorseQaContractError(f"{field} is not strict JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise BrowserHorseQaContractError(f"{field} must contain a JSON object")
    return value


def canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BrowserHorseQaContractError(
            f"receipt is not canonical JSON: {exc}"
        ) from exc


def _write_new_canonical_json(
    path: Path, value: Mapping[str, Any]
) -> Mapping[str, Any]:
    data = canonical_json_bytes(value)
    try:
        with path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise BrowserHorseQaStaleOutputError(
            f"immutable receipt already exists: {path}"
        ) from exc
    return MappingProxyType(
        {"path": os.fspath(path), "bytes": len(data), "sha256": _sha256(data)}
    )


def _json_snapshot(
    path: Path,
    field: str,
    *,
    max_bytes: int | None = None,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    data, pin = _read_snapshot(path, field, max_bytes=max_bytes)
    value = _parse_json(data, field)
    enriched = dict(pin)
    enriched["canonical_json_sha256"] = _sha256(canonical_json_bytes(value))
    return value, MappingProxyType(enriched)


def _require_object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise BrowserHorseQaContractError(f"{field} must be an object")
    return value


def _require_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise BrowserHorseQaContractError(f"{field} must be an array")
    return value


def _require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise BrowserHorseQaContractError(f"{field} must be a boolean")
    return value


def _require_int(value: Any, field: str, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise BrowserHorseQaContractError(f"{field} must be an integer >= {minimum}")
    return value


def _require_finite(value: Any, field: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        raise BrowserHorseQaContractError(f"{field} must be finite")
    return float(value)


def _require_null(value: Any, field: str) -> None:
    if value is not None:
        raise BrowserHorseQaContractError(f"{field} must remain unset")


def _verify_pin(
    emitted: Any,
    actual: Mapping[str, Any],
    field: str,
    *,
    exact_path: Path | None = None,
) -> None:
    pin = _require_object(emitted, field)
    if pin.get("bytes") != actual["bytes"] or pin.get("sha256") != actual["sha256"]:
        raise BrowserHorseQaContractError(f"{field} does not match the pinned artifact")
    if exact_path is not None:
        emitted_path = pin.get("path")
        if not isinstance(emitted_path, str):
            raise BrowserHorseQaContractError(f"{field}.path must be emitted")
        try:
            canonical = _canonical_existing_file(Path(emitted_path), f"{field}.path")
        except BrowserHorseQaPathError as exc:
            raise BrowserHorseQaContractError(str(exc)) from exc
        if canonical != exact_path:
            raise BrowserHorseQaContractError(
                f"{field}.path changed from the canonical server path"
            )


def _mkdir_parent_chain(root: Path, components: Sequence[str]) -> Path:
    current = root
    for component in components:
        next_path = current / component
        try:
            next_path.mkdir()
        except FileExistsError:
            pass
        _reject_link_components(next_path)
        if not next_path.is_dir():
            raise BrowserHorseQaPathError(
                f"canonical output parent is not a directory: {next_path}"
            )
        current = next_path.resolve(strict=True)
        _require_within(current, root, "canonical output parent")
    return current


def resolve_browser_horse_qa_paths(
    config: BrowserHorseQaRunnerConfig,
    request: BrowserHorseQaRequest,
) -> BrowserHorseQaPaths:
    input_root = _canonical_existing_directory(config.input_root, "input_root")
    output_root = _canonical_existing_directory(config.output_root, "output_root")
    job_id = _require_identifier(request.job_id, "job_id")
    candidate_id = _require_identifier(request.candidate_id, "candidate_id")
    attempt_id = _require_identifier(request.attempt_id, "attempt_id")
    semantic_id = _require_semantic_id(request.semantic_id)
    if semantic_id != V14_HORSE_SEMANTIC_ID:
        raise BrowserHorseQaContractError(
            "browser QA adapter currently supports only the nonproduction "
            "Horse V14 walk_forward loop canary"
        )
    _require_sha256(request.expected_three_clip_sha256, "expected_three_clip_sha256")

    candidate_input = input_root / "jobs" / job_id / "candidates" / candidate_id
    candidate_input = _canonical_existing_directory(
        candidate_input, "candidate input directory"
    )
    _require_within(candidate_input, input_root, "candidate input directory")
    three_clip_path = _canonical_existing_file(
        candidate_input / "three-clip.json", "three clip"
    )
    _require_within(three_clip_path, input_root, "three clip")
    _, three_clip_pin = _read_snapshot(three_clip_path, "three clip")
    if three_clip_pin["sha256"] != request.expected_three_clip_sha256:
        raise BrowserHorseQaContractError(
            "Three clip does not match its externally supplied SHA-256"
        )

    candidate_output = _mkdir_parent_chain(
        output_root,
        ("jobs", job_id, "candidates", candidate_id),
    )
    attempt_directory = candidate_output / attempt_id
    if attempt_directory.exists() or _is_reparse_or_symlink(attempt_directory):
        raise BrowserHorseQaStaleOutputError(
            f"immutable browser QA attempt already exists: {attempt_directory}"
        )
    try:
        attempt_directory.mkdir()
    except FileExistsError as exc:
        raise BrowserHorseQaStaleOutputError(
            f"immutable browser QA attempt already exists: {attempt_directory}"
        ) from exc
    attempt_directory = attempt_directory.resolve(strict=True)
    _require_within(attempt_directory, output_root, "attempt directory")
    return BrowserHorseQaPaths(
        candidate_input_directory=candidate_input,
        three_clip_path=three_clip_path,
        attempt_directory=attempt_directory,
        qa_output_directory=attempt_directory / "qa-output",
        request_receipt_path=attempt_directory / "run-request.json",
        run_receipt_path=attempt_directory / "run-receipt.json",
    )


def _remove_failed_attempt(
    config: BrowserHorseQaRunnerConfig,
    paths: BrowserHorseQaPaths,
) -> None:
    """Remove only the attempt directory created by this invocation.

    The JS runner publishes through a sibling ``qa-output.staging-*`` path. A
    hard timeout can bypass its own finally block, so deleting the owned attempt
    also removes those staging artifacts and makes an explicit retry possible.
    """

    output_root = _canonical_existing_directory(config.output_root, "output_root")
    attempt = _absolute_without_resolution(paths.attempt_directory)
    _require_within(attempt, output_root, "failed attempt directory")
    if attempt == output_root or _is_reparse_or_symlink(attempt):
        raise BrowserHorseQaPathError(
            "refusing to clean an unsafe failed browser QA attempt"
        )
    if not attempt.exists():
        return
    for retry in range(3):
        try:
            shutil.rmtree(attempt)
            return
        except FileNotFoundError:
            return
        except OSError:
            if retry == 2:
                raise BrowserHorseQaSubprocessError(
                    "failed browser QA attempt could not be cleaned for retry"
                )
            time.sleep(0.1)


def _pin_config_file(
    path: Path, field: str, expected_sha256: str | None = None
) -> tuple[Path, Mapping[str, Any]]:
    canonical = _canonical_existing_file(path, field)
    _, pin = _read_snapshot(canonical, field)
    if expected_sha256 is not None and pin["sha256"] != _require_sha256(
        expected_sha256, field
    ):
        raise BrowserHorseQaContractError(
            f"{field} does not match its server-owned SHA-256"
        )
    return canonical, pin


def _require_bundle_filename(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or Path(value).name != value
        or "/" in value
        or "\\" in value
        or ":" in value
    ):
        raise BrowserHorseQaContractError(f"{field} is not a safe bundle filename")
    return value


def _validate_config(config: BrowserHorseQaRunnerConfig) -> Mapping[str, Any]:
    bundle = _canonical_existing_directory(config.bundle_directory, "bundle_directory")
    _reject_tree_links(bundle)
    executable, executable_pin = _pin_config_file(
        config.runner_executable,
        "runner_executable",
        config.expected_runner_executable_sha256,
    )
    runner, runner_pin = _pin_config_file(
        config.runner_script,
        "runner_script",
        config.expected_runner_script_sha256,
    )
    qa_profile_path, qa_profile_pin = _pin_config_file(
        config.qa_profile_path,
        "qa_profile_path",
        config.expected_qa_profile_sha256,
    )
    profile, enriched_profile_pin = _json_snapshot(qa_profile_path, "qa_profile")
    if profile.get("schema") != QA_PROFILE_SCHEMA:
        raise BrowserHorseQaContractError(
            f"qa_profile schema must be {QA_PROFILE_SCHEMA}"
        )
    calibration = profile.get("calibration_state_string")
    if calibration != V14_HORSE_QA_CALIBRATION_STATE:
        raise BrowserHorseQaContractError(
            "qa_profile must remain the pinned provisional Horse V14 profile"
        )
    ranking_weights = _require_object(
        profile.get("ranking_weights_object"), "qa_profile ranking weights"
    )
    for key, value in ranking_weights.items():
        if (
            not isinstance(key, str)
            or not key
            or _require_finite(value, f"qa_profile ranking weight {key}") < 0
        ):
            raise BrowserHorseQaContractError("qa_profile ranking weights are invalid")

    three_module, three_module_pin = _pin_config_file(
        config.three_module,
        "three_module",
        config.expected_three_module_sha256,
    )
    chrome, chrome_pin = _pin_config_file(
        config.chrome_executable,
        "chrome_executable",
        config.expected_chrome_executable_sha256,
    )
    ffmpeg, ffmpeg_pin = _pin_config_file(
        config.ffmpeg_executable,
        "ffmpeg_executable",
        config.expected_ffmpeg_executable_sha256,
    )
    ffprobe, ffprobe_pin = _pin_config_file(
        config.ffprobe_executable,
        "ffprobe_executable",
        config.expected_ffprobe_executable_sha256,
    )
    if any(
        "blender" in path.name.lower()
        for path in (executable, runner, chrome, ffmpeg, ffprobe)
    ):
        raise BrowserHorseQaContractError(
            "Blender executables/scripts are forbidden in browser QA"
        )
    if str(config.expected_three_revision) != "160":
        raise BrowserHorseQaContractError("expected_three_revision must be exactly 160")
    if not isinstance(config.timeout_seconds, (int, float)) or not math.isfinite(
        config.timeout_seconds
    ):
        raise BrowserHorseQaContractError("timeout_seconds must be finite")
    if config.timeout_seconds <= 0 or config.timeout_seconds > 7200:
        raise BrowserHorseQaContractError("timeout_seconds must be in (0, 7200]")

    immutable_path, immutable_pin = _pin_config_file(
        bundle / "immutable_manifest.json",
        "immutable manifest",
        config.expected_immutable_manifest_sha256,
    )
    immutable_value, _ = _json_snapshot(immutable_path, "immutable manifest")
    immutable_rows = _require_list(
        immutable_value.get("files"), "immutable manifest files"
    )
    if not immutable_rows:
        raise BrowserHorseQaContractError("immutable manifest files must not be empty")
    bundle_file_paths: dict[str, Path] = {}
    bundle_file_pins: dict[str, Mapping[str, Any]] = {}
    for index, raw_row in enumerate(immutable_rows):
        row = _require_object(raw_row, f"immutable manifest file {index}")
        filename = _require_bundle_filename(
            row.get("filename"), f"immutable manifest file {index}.filename"
        )
        if filename in bundle_file_paths:
            raise BrowserHorseQaContractError(
                f"immutable manifest repeats bundle file {filename}"
            )
        expected_sha = _require_sha256(
            row.get("sha256"), f"immutable manifest file {filename}.sha256"
        )
        file_path, file_pin = _pin_config_file(
            bundle / filename, f"immutable bundle file {filename}", expected_sha
        )
        if file_pin["bytes"] != _require_int(
            row.get("bytes"), f"immutable manifest file {filename}.bytes", 1
        ):
            raise BrowserHorseQaContractError(
                f"immutable bundle file {filename} byte count changed"
            )
        bundle_file_paths[filename] = file_path
        bundle_file_pins[filename] = file_pin
    expected_bundle_inventory = {"immutable_manifest.json", *bundle_file_paths}
    if {entry.name for entry in bundle.iterdir()} != expected_bundle_inventory:
        raise BrowserHorseQaContractError(
            "Horse V14 immutable bundle inventory changed"
        )
    fitting_path, fitting_pin = _pin_config_file(
        bundle / "fitting_bundle.json",
        "fitting bundle",
        config.expected_fitting_bundle_sha256,
    )
    skeleton_path, skeleton_pin = _pin_config_file(bundle / "skeleton.json", "skeleton")
    skin_weights_path, skin_weights_pin = _pin_config_file(
        bundle / "skin_weights.json.gz", "skin weights"
    )
    topology_path, topology_pin = _pin_config_file(
        bundle / "surface_topology.json.gz", "surface topology"
    )
    for required_name, required_pin in (
        ("fitting_bundle.json", fitting_pin),
        ("skeleton.json", skeleton_pin),
        ("skin_weights.json.gz", skin_weights_pin),
        ("surface_topology.json.gz", topology_pin),
    ):
        declared_pin = bundle_file_pins.get(required_name)
        if declared_pin is None or (
            declared_pin["bytes"] != required_pin["bytes"]
            or declared_pin["sha256"] != required_pin["sha256"]
        ):
            raise BrowserHorseQaContractError(
                f"immutable manifest does not pin required {required_name}"
            )
    _require_sha256(config.expected_source_model_sha256, "expected_source_model_sha256")
    return MappingProxyType(
        {
            "bundle": bundle,
            "executable": executable,
            "runner": runner,
            "qa_profile": profile,
            "qa_profile_path": qa_profile_path,
            "three_module": three_module,
            "chrome": chrome,
            "ffmpeg": ffmpeg,
            "ffprobe": ffprobe,
            "immutable_path": immutable_path,
            "fitting_path": fitting_path,
            "skeleton_path": skeleton_path,
            "skin_weights_path": skin_weights_path,
            "topology_path": topology_path,
            "bundle_inventory": frozenset(expected_bundle_inventory),
            "bundle_file_paths": MappingProxyType(bundle_file_paths),
            "pins": MappingProxyType(
                {
                    "runner_executable": executable_pin,
                    "runner_script": runner_pin,
                    "qa_profile": enriched_profile_pin,
                    "three_module": three_module_pin,
                    "chrome": chrome_pin,
                    "ffmpeg": ffmpeg_pin,
                    "ffprobe": ffprobe_pin,
                    "immutable_manifest": immutable_pin,
                    "fitting_bundle": fitting_pin,
                    "skeleton": skeleton_pin,
                    "skin_weights": skin_weights_pin,
                    "surface_topology": topology_pin,
                    "bundle_files": MappingProxyType(bundle_file_pins),
                }
            ),
        }
    )


def _assert_pin_unchanged(
    path: Path,
    expected: Mapping[str, Any],
    field: str,
) -> None:
    canonical = _canonical_existing_file(path, field)
    _, actual = _read_snapshot(canonical, field)
    if (
        actual["bytes"] != expected.get("bytes")
        or actual["sha256"] != expected.get("sha256")
        or os.fspath(canonical) != expected.get("path")
    ):
        raise BrowserHorseQaContractError(f"{field} changed during the QA attempt")


def _assert_all_inputs_unchanged(
    *,
    validated_config: Mapping[str, Any],
    paths: BrowserHorseQaPaths,
    clip_pin: Mapping[str, Any],
) -> None:
    """Re-pin every consumed byte at the execution boundary.

    This intentionally happens immediately before the browser runner is spawned
    and again after it exits. It covers the JS runner, every executable, the
    provisional QA profile, all immutable Horse_2 bundle members, Three r160,
    and the fitted clip.
    """

    checks = (
        (validated_config["executable"], "runner_executable"),
        (validated_config["runner"], "runner_script"),
        (validated_config["qa_profile_path"], "qa_profile"),
        (validated_config["three_module"], "three_module"),
        (validated_config["chrome"], "chrome"),
        (validated_config["ffmpeg"], "ffmpeg"),
        (validated_config["ffprobe"], "ffprobe"),
        (validated_config["immutable_path"], "immutable_manifest"),
    )
    for path, pin_name in checks:
        _assert_pin_unchanged(
            Path(path),
            validated_config["pins"][pin_name],
            pin_name,
        )
    for filename, path in validated_config["bundle_file_paths"].items():
        _assert_pin_unchanged(
            Path(path),
            validated_config["pins"]["bundle_files"][filename],
            f"immutable bundle file {filename}",
        )
    _assert_pin_unchanged(paths.three_clip_path, clip_pin, "three_clip")
    bundle = _canonical_existing_directory(
        validated_config["bundle"], "bundle_directory"
    )
    _reject_tree_links(bundle)
    if {entry.name for entry in bundle.iterdir()} != set(
        validated_config["bundle_inventory"]
    ):
        raise BrowserHorseQaContractError(
            "Horse V14 immutable bundle inventory changed during the QA attempt"
        )


def _terminate_process_tree(
    process: subprocess.Popen[bytes],
    *,
    platform_name: str | None = None,
) -> None:
    if process.poll() is not None:
        return
    effective_platform = platform_name or os.name
    if effective_platform == "nt":
        try:
            result = subprocess.run(
                ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=POST_KILL_WAIT_SECONDS,
            )
            if result.returncode != 0 and process.poll() is None:
                process.kill()
        except (OSError, subprocess.SubprocessError):
            process.kill()
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            if process.poll() is None:
                process.kill()
    try:
        process.wait(timeout=POST_KILL_WAIT_SECONDS)
    except subprocess.TimeoutExpired:
        if process.poll() is None:
            process.kill()
        try:
            process.wait(timeout=POST_KILL_WAIT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            raise BrowserHorseQaSubprocessError(
                "browser QA process did not terminate after bounded kill waits"
            ) from exc


def _capture_pipe_bounded(
    pipe: Any,
    capture: bytearray,
    overflow: threading.Event,
) -> None:
    try:
        while True:
            chunk = pipe.read(64 * 1024)
            if not chunk:
                return
            remaining = MAX_CAPTURE_BYTES + 1 - len(capture)
            if remaining > 0:
                capture.extend(chunk[:remaining])
            if len(capture) > MAX_CAPTURE_BYTES or len(chunk) > remaining:
                overflow.set()
    finally:
        pipe.close()


def _run_command(
    command: Sequence[str], *, cwd: Path, timeout_seconds: float
) -> tuple[int, bytes, bytes]:
    creationflags = (
        getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
    )
    process = subprocess.Popen(
        list(command),
        cwd=os.fspath(cwd),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
        creationflags=creationflags,
        start_new_session=os.name != "nt",
    )
    if process.stdout is None or process.stderr is None:
        _terminate_process_tree(process)
        raise BrowserHorseQaSubprocessError(
            "browser QA subprocess pipes were not created"
        )
    stdout_capture = bytearray()
    stderr_capture = bytearray()
    overflow = threading.Event()
    readers = (
        threading.Thread(
            target=_capture_pipe_bounded,
            args=(process.stdout, stdout_capture, overflow),
            daemon=True,
        ),
        threading.Thread(
            target=_capture_pipe_bounded,
            args=(process.stderr, stderr_capture, overflow),
            daemon=True,
        ),
    )
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    while process.poll() is None:
        if overflow.is_set():
            _terminate_process_tree(process)
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            _terminate_process_tree(process)
            break
        try:
            process.wait(timeout=min(0.1, remaining))
        except subprocess.TimeoutExpired:
            continue
    for reader in readers:
        reader.join(timeout=POST_KILL_WAIT_SECONDS)
    if any(reader.is_alive() for reader in readers):
        raise BrowserHorseQaSubprocessError(
            "browser QA output readers did not stop after bounded waits"
        )
    if timed_out:
        raise BrowserHorseQaTimeoutError(
            f"browser Horse QA exceeded {timeout_seconds:g} seconds"
        )
    stdout = bytes(stdout_capture)
    stderr = bytes(stderr_capture)
    if overflow.is_set():
        raise BrowserHorseQaSubprocessError(
            "browser QA stdout/stderr exceeded the trusted capture limit"
        )
    return process.returncode, stdout, stderr


def _parse_cli_stdout(stdout: bytes) -> Mapping[str, Any]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise BrowserHorseQaContractError(
            "browser QA must emit exactly one JSON result line"
        )
    return _parse_json(lines[0], "browser QA stdout")


def _snapshot_output_inventory(
    output_directory: Path,
) -> tuple[Mapping[str, Mapping[str, Any]], Mapping[str, Any]]:
    output = _canonical_existing_directory(output_directory, "qa output directory")
    _reject_tree_links(output)
    expected_top = {
        "camera-settings.json",
        "deformation-report.json",
        "fixed-camera-preview.mp4",
        "frames",
        "visual-phase-qa.json",
    }
    actual_top = {entry.name for entry in output.iterdir()}
    if actual_top != expected_top:
        raise BrowserHorseQaContractError(
            f"browser QA top-level artifact inventory changed: {sorted(actual_top)}"
        )
    frames_directory = _canonical_existing_directory(
        output / "frames", "QA frames directory"
    )
    expected_frames = [f"frame_{index:04d}.png" for index in range(FRAME_COUNT)]
    actual_frames = sorted(entry.name for entry in frames_directory.iterdir())
    if actual_frames != expected_frames:
        raise BrowserHorseQaContractError(
            "browser QA must emit exactly all 49 fitted phase frames"
        )

    inventory: dict[str, Mapping[str, Any]] = {}
    json_values: dict[str, Any] = {}
    for relative in (
        "camera-settings.json",
        "deformation-report.json",
        "fixed-camera-preview.mp4",
        "visual-phase-qa.json",
        *(f"frames/{name}" for name in expected_frames),
    ):
        path = output / Path(relative)
        if relative.endswith(".json"):
            value, pin = _json_snapshot(
                path, relative, max_bytes=MAX_JSON_ARTIFACT_BYTES
            )
            json_values[relative] = value
        else:
            artifact_cap = (
                MAX_PNG_ARTIFACT_BYTES
                if relative.endswith(".png")
                else MAX_MP4_ARTIFACT_BYTES
            )
            data, pin = _read_snapshot(path, relative, max_bytes=artifact_cap)
            if relative.endswith(".png"):
                if (
                    len(data) < 24
                    or data[:8] != b"\x89PNG\r\n\x1a\n"
                    or data[12:16] != b"IHDR"
                    or struct.unpack(">II", data[16:24]) != (768, 448)
                ):
                    raise BrowserHorseQaContractError(
                        f"browser QA frame is not a 768x448 PNG: {relative}"
                    )
            elif relative == "fixed-camera-preview.mp4" and (
                len(data) < 12 or data[4:8] != b"ftyp"
            ):
                raise BrowserHorseQaContractError(
                    "fixed-camera preview is not an ISO BMFF/MP4 artifact"
                )
        if pin["bytes"] <= 0:
            raise BrowserHorseQaContractError(
                f"browser QA artifact is empty: {relative}"
            )
        inventory[relative] = MappingProxyType(
            {key: value for key, value in pin.items() if key != "path"}
        )
    if (
        sum(int(pin["bytes"]) for pin in inventory.values())
        > MAX_OUTPUT_INVENTORY_BYTES
    ):
        raise BrowserHorseQaContractError(
            "browser QA output inventory exceeds the trusted byte cap"
        )
    return MappingProxyType(inventory), MappingProxyType(json_values)


def _parse_rate(value: Any, field: str) -> float:
    if not isinstance(value, str) or "/" not in value:
        raise BrowserHorseQaContractError(f"{field} must be a rational frame rate")
    numerator_text, denominator_text = value.split("/", 1)
    try:
        numerator = float(numerator_text)
        denominator = float(denominator_text)
    except ValueError as exc:
        raise BrowserHorseQaContractError(f"{field} is invalid") from exc
    if (
        not math.isfinite(numerator)
        or not math.isfinite(denominator)
        or denominator == 0
    ):
        raise BrowserHorseQaContractError(f"{field} is invalid")
    return numerator / denominator


def _validate_fixed_camera_mp4(
    *,
    validated_config: Mapping[str, Any],
    paths: BrowserHorseQaPaths,
    timeout_seconds: float,
) -> Mapping[str, Any]:
    video = _canonical_existing_file(
        paths.qa_output_directory / "fixed-camera-preview.mp4",
        "fixed-camera preview",
    )
    command = [
        os.fspath(validated_config["ffprobe"]),
        "-v",
        "error",
        "-count_frames",
        "-show_entries",
        "format=format_name,duration:stream=index,codec_type,codec_name,pix_fmt,width,height,r_frame_rate,nb_read_frames",
        "-of",
        "json",
        os.fspath(video),
    ]
    return_code, stdout, stderr = _run_command(
        command,
        cwd=paths.qa_output_directory,
        timeout_seconds=min(timeout_seconds, 60.0),
    )
    if return_code != 0:
        diagnostic = stderr.decode("utf-8", errors="replace").strip()
        raise BrowserHorseQaContractError(
            f"fixed-camera MP4 ffprobe failed: {diagnostic[:500] or return_code}"
        )
    probe = _parse_json(stdout, "fixed-camera MP4 ffprobe")
    streams = _require_list(probe.get("streams"), "ffprobe streams")
    video_streams = [
        _require_object(stream, "ffprobe stream")
        for stream in streams
        if isinstance(stream, dict) and stream.get("codec_type") == "video"
    ]
    audio_streams = [
        stream
        for stream in streams
        if isinstance(stream, dict) and stream.get("codec_type") == "audio"
    ]
    if len(video_streams) != 1 or audio_streams:
        raise BrowserHorseQaContractError(
            "fixed-camera MP4 must contain exactly one video stream and no audio"
        )
    stream = video_streams[0]
    container = _require_object(probe.get("format"), "ffprobe format")
    format_names = str(container.get("format_name", "")).split(",")
    try:
        decoded_frames = int(stream.get("nb_read_frames"))
    except (TypeError, ValueError) as exc:
        raise BrowserHorseQaContractError(
            "fixed-camera MP4 decoded frame count is missing"
        ) from exc
    measured_fps = _parse_rate(stream.get("r_frame_rate"), "MP4 r_frame_rate")
    if (
        "mp4" not in format_names
        or stream.get("codec_name") != "h264"
        or stream.get("pix_fmt") != "yuv420p"
        or stream.get("width") != 768
        or stream.get("height") != 448
        or decoded_frames != V14_HORSE_FRAME_COUNT
        or abs(measured_fps - V14_HORSE_OUTPUT_FPS) > 1e-6
    ):
        raise BrowserHorseQaContractError(
            "fixed-camera MP4 must be h264/yuv420p 768x448 at 30fps with exactly 49 frames"
        )
    try:
        duration = float(container.get("duration"))
    except (TypeError, ValueError) as exc:
        raise BrowserHorseQaContractError("MP4 duration must be finite") from exc
    if not math.isfinite(duration):
        raise BrowserHorseQaContractError("MP4 duration must be finite")
    if duration <= 0:
        raise BrowserHorseQaContractError("fixed-camera MP4 duration must be positive")
    return MappingProxyType(
        {
            "container": "mp4",
            "codec": "h264",
            "pixel_format": "yuv420p",
            "width": 768,
            "height": 448,
            "fps": measured_fps,
            "frame_count": decoded_frames,
            "audio_stream_count": 0,
            "duration_seconds": duration,
        }
    )


def _verify_evidence(
    *,
    config: BrowserHorseQaRunnerConfig,
    request: BrowserHorseQaRequest,
    paths: BrowserHorseQaPaths,
    validated_config: Mapping[str, Any],
    inventory: Mapping[str, Mapping[str, Any]],
    json_values: Mapping[str, Any],
    mp4_contract: Mapping[str, Any],
) -> tuple[bool, Mapping[str, bool], Mapping[str, Any]]:
    deformation = _require_object(
        json_values["deformation-report.json"], "deformation report"
    )
    if deformation.get("schema") != DEFORMATION_SCHEMA:
        raise BrowserHorseQaContractError(
            f"deformation schema must be {DEFORMATION_SCHEMA}"
        )
    if deformation.get("measuredEveryFrame") is not True:
        raise BrowserHorseQaContractError("deformation report must measure every frame")
    if (
        _require_int(deformation.get("frameCount"), "deformation frameCount", 1)
        != FRAME_COUNT
    ):
        raise BrowserHorseQaContractError("deformation report frame count changed")
    vertex_count = _require_int(deformation.get("vertexCount"), "vertexCount", 1)
    if vertex_count != 344:
        raise BrowserHorseQaContractError("Horse V14 vertex count must remain 344")
    edge_count = _require_int(deformation.get("edgeCount"), "edgeCount", 1)
    edge_sample_count = _require_int(
        deformation.get("edgeSampleCount"), "edgeSampleCount", 1
    )
    if edge_sample_count != edge_count * V14_HORSE_FRAME_COUNT:
        raise BrowserHorseQaContractError("deformation edge sample inventory changed")
    coincident_group_count = _require_int(
        deformation.get("coincidentRestGroupCount"),
        "coincidentRestGroupCount",
    )
    if (
        _require_int(
            deformation.get("coincidentRestSampleCount"),
            "coincidentRestSampleCount",
        )
        != V14_HORSE_FRAME_COUNT
    ):
        raise BrowserHorseQaContractError(
            "coincident-rest sample count must cover all 49 frames"
        )
    maximum_edge_stretch = _require_finite(
        deformation.get("maximumEdgeStretch"), "maximumEdgeStretch"
    )
    p99_edge_stretch = _require_finite(
        deformation.get("p99EdgeStretch"), "p99EdgeStretch"
    )
    zero_weight_vertices = _require_int(
        deformation.get("zeroWeightVertices"), "zeroWeightVertices"
    )
    maximum_coincident_separation = _require_finite(
        deformation.get("maximumCoincidentRestSeparationM"),
        "maximumCoincidentRestSeparationM",
    )
    if (
        maximum_edge_stretch < 1
        or p99_edge_stretch < 1
        or p99_edge_stretch > maximum_edge_stretch
        or zero_weight_vertices > vertex_count
        or maximum_coincident_separation < 0
    ):
        raise BrowserHorseQaContractError("deformation numeric ranges are invalid")

    thresholds = _require_object(
        deformation.get("thresholds"), "deformation thresholds"
    )
    if set(thresholds) != set(V14_HORSE_DEFORMATION_THRESHOLDS):
        raise BrowserHorseQaContractError("deformation threshold inventory changed")
    for key, expected in V14_HORSE_DEFORMATION_THRESHOLDS.items():
        emitted = thresholds.get(key)
        if isinstance(expected, int):
            if _require_int(emitted, f"deformation threshold {key}") != expected:
                raise BrowserHorseQaContractError(
                    f"deformation threshold {key} changed"
                )
        elif (
            abs(_require_finite(emitted, f"deformation threshold {key}") - expected)
            > 1e-12
        ):
            raise BrowserHorseQaContractError(f"deformation threshold {key} changed")

    root_motion_locked = _require_bool(
        deformation.get("rootMotionLocked"), "rootMotionLocked"
    )
    camera_static = _require_bool(deformation.get("cameraStatic"), "cameraStatic")
    per_frame = _require_list(deformation.get("frames"), "deformation frames")
    if len(per_frame) != V14_HORSE_FRAME_COUNT:
        raise BrowserHorseQaContractError(
            "deformation report must contain exactly all 49 frame rows"
        )
    frame_maxima: list[float] = []
    frame_separations: list[float] = []
    collapsed_total = 0
    frame_roots: list[bool] = []
    frame_cameras: list[bool] = []
    for expected_index, raw_frame in enumerate(per_frame):
        frame = _require_object(raw_frame, f"deformation frame {expected_index}")
        if _require_int(frame.get("frameIndex"), "frameIndex") != expected_index:
            raise BrowserHorseQaContractError("deformation frame chronology changed")
        time_seconds = _require_finite(frame.get("timeSeconds"), "frame timeSeconds")
        # Three serializes fitted track times through Float32 arrays; accept only
        # that sub-microsecond representation error around the exact 30fps grid.
        if abs(time_seconds - expected_index / V14_HORSE_OUTPUT_FPS) > 1e-6:
            raise BrowserHorseQaContractError(
                "deformation frame timeline is not the exact 30fps V14 interval"
            )
        frame_maximum = _require_finite(
            frame.get("maximumEdgeStretch"), "frame maximumEdgeStretch"
        )
        frame_p99 = _require_finite(frame.get("p99EdgeStretch"), "frame p99EdgeStretch")
        frame_separation = _require_finite(
            frame.get("maximumCoincidentRestSeparationM"),
            "frame maximumCoincidentRestSeparationM",
        )
        if (
            frame_maximum < 1
            or frame_p99 < 1
            or frame_p99 > frame_maximum
            or frame_separation < 0
        ):
            raise BrowserHorseQaContractError(
                "deformation frame numeric range is invalid"
            )
        frame_maxima.append(frame_maximum)
        frame_separations.append(frame_separation)
        collapsed_total += _require_int(
            frame.get("collapsedEdgeSampleCount"), "frame collapsedEdgeSampleCount"
        )
        frame_roots.append(
            _require_bool(frame.get("rootMotionLocked"), "frame rootMotionLocked")
        )
        frame_cameras.append(
            _require_bool(frame.get("cameraStatic"), "frame cameraStatic")
        )
    if (
        abs(max(frame_maxima) - maximum_edge_stretch) > 1e-9
        or abs(max(frame_separations) - maximum_coincident_separation) > 1e-9
        or collapsed_total
        != _require_int(
            deformation.get("collapsedEdgeSampleCount"),
            "collapsedEdgeSampleCount",
        )
        or root_motion_locked != all(frame_roots)
        or camera_static != all(frame_cameras)
    ):
        raise BrowserHorseQaContractError(
            "deformation global metrics disagree with the 49 frame rows"
        )
    gates_raw = _require_object(deformation.get("gates"), "deformation gates")
    if set(gates_raw) != EXPECTED_MACHINE_GATE_KEYS:
        raise BrowserHorseQaContractError("deformation machine gate inventory changed")
    machine_gates = {
        key: _require_bool(value, f"deformation gate {key}")
        for key, value in gates_raw.items()
    }
    recomputed_gates = {
        "maximumEdgeStretch": maximum_edge_stretch
        <= V14_HORSE_DEFORMATION_THRESHOLDS["maximumEdgeStretch"],
        "p99EdgeStretch": p99_edge_stretch
        <= V14_HORSE_DEFORMATION_THRESHOLDS["p99EdgeStretch"],
        "zeroWeightVertices": zero_weight_vertices
        <= V14_HORSE_DEFORMATION_THRESHOLDS["zeroWeightVertices"],
        "coincidentRestSeparation": maximum_coincident_separation
        <= V14_HORSE_DEFORMATION_THRESHOLDS["coincidentRestSeparationM"],
        "rootMotionLocked": root_motion_locked,
        "cameraStatic": camera_static,
    }
    if machine_gates != recomputed_gates:
        raise BrowserHorseQaContractError(
            "deformation gate booleans disagree with trusted Horse V14 thresholds"
        )
    machine_passed = _require_bool(deformation.get("passed"), "deformation passed")
    if machine_passed != all(machine_gates.values()):
        raise BrowserHorseQaContractError(
            "deformation overall gate disagrees with machine gates"
        )
    deformation_inputs = _require_object(
        deformation.get("inputs"), "deformation inputs"
    )
    expected_deformation_pins = {
        "fittingBundleSha256": config.expected_fitting_bundle_sha256,
        "threeClipSha256": request.expected_three_clip_sha256,
        "skinWeightsSha256": validated_config["pins"]["skin_weights"]["sha256"],
        "topologySha256": validated_config["pins"]["surface_topology"]["sha256"],
    }
    if set(deformation_inputs) != set(expected_deformation_pins):
        raise BrowserHorseQaContractError(
            "deformation input provenance inventory changed"
        )
    for field, expected in expected_deformation_pins.items():
        if deformation_inputs.get(field) != expected:
            raise BrowserHorseQaContractError(f"deformation provenance {field} changed")

    camera = _require_object(json_values["camera-settings.json"], "camera settings")
    if camera.get("schema") != CAMERA_SCHEMA or camera.get("resolution") != [768, 448]:
        raise BrowserHorseQaContractError("fixed-camera settings contract changed")
    if (
        camera.get("rootMotionPolicy")
        != "suppress_armature_root_tracks_and_lock_model_transform"
    ):
        raise BrowserHorseQaContractError("fixed-camera root motion policy changed")

    evidence = _require_object(
        json_values["visual-phase-qa.json"], "visual phase evidence"
    )
    if evidence.get("schema") != VISUAL_EVIDENCE_SCHEMA:
        raise BrowserHorseQaContractError(
            f"visual evidence schema must be {VISUAL_EVIDENCE_SCHEMA}"
        )
    visual = _require_object(evidence.get("visual_phase_gate"), "visual phase gate")
    if (
        visual.get("schema") != VISUAL_GATE_SCHEMA
        or visual.get("rig_type") != V14_HORSE_RIG_TYPE
    ):
        raise BrowserHorseQaContractError("visual phase rig/schema contract changed")
    if visual.get("semantic_id") != request.semantic_id:
        raise BrowserHorseQaContractError(
            "visual phase action semantic does not match the server request"
        )
    if visual.get("fitted_clip_sha256") != request.expected_three_clip_sha256:
        raise BrowserHorseQaContractError("visual phase fitted clip pin changed")
    _require_null(visual.get("decision"), "visual phase decision")
    visual_camera = _require_object(visual.get("camera"), "visual phase camera")
    if (
        visual_camera.get("static") is not True
        or visual_camera.get("root_motion_locked") is not True
    ):
        raise BrowserHorseQaContractError("visual phase camera/root lock changed")
    if (
        visual_camera.get("settings_sha256")
        != inventory["camera-settings.json"]["sha256"]
    ):
        raise BrowserHorseQaContractError("visual phase camera settings pin changed")
    if visual.get("required_phases") != list(PHASES):
        raise BrowserHorseQaContractError("visual phase inventory changed")
    expected_phase_indices = (0, 24, 36)
    phase_rows = _require_list(visual.get("frames"), "visual phase frames")
    if len(phase_rows) != len(PHASES):
        raise BrowserHorseQaContractError("visual phase frame inventory is incomplete")
    for index, row_value in enumerate(phase_rows):
        row = _require_object(row_value, f"visual phase frame {index}")
        frame_relative = f"frames/frame_{expected_phase_indices[index]:04d}.png"
        if (
            row.get("phase") != PHASES[index]
            or row.get("frame_index") != expected_phase_indices[index]
            or row.get("sha256") != inventory[frame_relative]["sha256"]
        ):
            raise BrowserHorseQaContractError(
                "visual phase frame pin/action ordering changed"
            )
        _require_null(
            row.get("evidence_url"), f"visual phase frame {index} evidence_url"
        )
    reviewer = _require_object(visual.get("reviewer"), "visual phase reviewer")
    _require_null(reviewer.get("id"), "visual reviewer id")
    _require_null(reviewer.get("reviewed_at"), "visual reviewed_at")
    seam = _require_object(
        visual.get("coincident_rest_vertex_separation"),
        "coincident-rest separation",
    )
    if seam.get("measured") is not True or seam.get("report_url") is not None:
        raise BrowserHorseQaContractError(
            "coincident-rest separation evidence is not fail-closed"
        )
    if seam.get("report_sha256") != inventory["deformation-report.json"]["sha256"]:
        raise BrowserHorseQaContractError(
            "coincident-rest separation report pin changed"
        )
    seam_pass = _require_bool(seam.get("pass"), "coincident-rest separation pass")
    if seam_pass != machine_gates.get("coincidentRestSeparation"):
        raise BrowserHorseQaContractError("coincident-rest separation gates disagree")
    if (
        abs(
            _require_finite(seam.get("threshold_m"), "coincident threshold_m")
            - V14_HORSE_DEFORMATION_THRESHOLDS["coincidentRestSeparationM"]
        )
        > 1e-12
        or abs(
            _require_finite(seam.get("max_separation_m"), "coincident max_separation_m")
            - maximum_coincident_separation
        )
        > 1e-9
        or _require_int(seam.get("sample_count"), "coincident sample_count")
        != V14_HORSE_FRAME_COUNT
        or _require_int(seam.get("group_count"), "coincident group_count")
        != coincident_group_count
    ):
        raise BrowserHorseQaContractError(
            "coincident-rest numeric evidence disagrees with the deformation report"
        )

    local = _require_object(evidence.get("local_evidence"), "local evidence")
    if (
        local.get("source_rig_type") != V14_HORSE_SOURCE_RIG_TYPE
        or local.get("browser_only") is not True
        or local.get("blender_used") is not False
        or local.get("animation_evaluation") != "Three.AnimationMixer"
    ):
        raise BrowserHorseQaContractError(
            "browser-only Horse_2 execution provenance changed"
        )
    immutable_inputs = _require_object(
        local.get("immutable_inputs"), "immutable inputs"
    )
    if set(immutable_inputs) != {
        "source_model",
        "immutable_manifest",
        "fitting_bundle",
        "skeleton",
        "skin_weights",
        "surface_topology",
        "three_clip",
    }:
        raise BrowserHorseQaContractError(
            "immutable input provenance inventory changed"
        )
    source_model = _require_object(immutable_inputs.get("source_model"), "source model")
    if source_model.get("sha256") != config.expected_source_model_sha256:
        raise BrowserHorseQaContractError("source model provenance changed")
    source_filename = source_model.get("filename")
    if (
        not isinstance(source_filename, str)
        or not source_filename
        or source_filename in {".", ".."}
        or "/" in source_filename
        or "\\" in source_filename
        or ":" in source_filename
    ):
        raise BrowserHorseQaContractError("source model filename provenance is unsafe")

    pin_specs = (
        (
            "immutable_manifest",
            validated_config["immutable_path"],
            validated_config["pins"]["immutable_manifest"],
        ),
        (
            "fitting_bundle",
            validated_config["fitting_path"],
            validated_config["pins"]["fitting_bundle"],
        ),
        (
            "skeleton",
            validated_config["skeleton_path"],
            validated_config["pins"]["skeleton"],
        ),
        (
            "skin_weights",
            validated_config["skin_weights_path"],
            validated_config["pins"]["skin_weights"],
        ),
        (
            "surface_topology",
            validated_config["topology_path"],
            validated_config["pins"]["surface_topology"],
        ),
        ("three_clip", paths.three_clip_path, None),
    )
    for name, expected_path_raw, known_pin in pin_specs:
        expected_path = _canonical_existing_file(
            Path(expected_path_raw), f"immutable input {name}"
        )
        if known_pin is None:
            _, actual_pin = _read_snapshot(expected_path, f"immutable input {name}")
        else:
            actual_pin = known_pin
        _verify_pin(
            immutable_inputs.get(name),
            actual_pin,
            f"immutable_inputs.{name}",
            exact_path=expected_path,
        )
    if (
        immutable_inputs["three_clip"].get("sha256")
        != request.expected_three_clip_sha256
    ):
        raise BrowserHorseQaContractError("local evidence Three clip pin changed")

    _verify_pin(
        local.get("camera_settings"),
        inventory["camera-settings.json"],
        "local_evidence.camera_settings",
        exact_path=paths.qa_output_directory / "camera-settings.json",
    )
    video = _require_object(local.get("video"), "local evidence video")
    _verify_pin(
        video,
        inventory["fixed-camera-preview.mp4"],
        "local_evidence.video",
        exact_path=paths.qa_output_directory / "fixed-camera-preview.mp4",
    )
    if (
        video.get("fixed_camera") is not True
        or video.get("root_motion_locked") is not True
        or video.get("container") != "mp4"
        or video.get("codec") != "h264"
        or video.get("pixel_format") != "yuv420p"
        or video.get("width") != 768
        or video.get("height") != 448
        or abs(_require_finite(video.get("fps"), "video fps") - 30.0) > 1e-6
        or _require_int(video.get("frame_count"), "video frame_count")
        != V14_HORSE_FRAME_COUNT
        or _require_int(video.get("audio_stream_count"), "video audio_stream_count")
        != 0
    ):
        raise BrowserHorseQaContractError("fixed-camera preview contract changed")
    for key in (
        "container",
        "codec",
        "pixel_format",
        "width",
        "height",
        "frame_count",
        "audio_stream_count",
    ):
        if video.get(key) != mp4_contract[key]:
            raise BrowserHorseQaContractError(
                "fixed-camera evidence disagrees with the pinned ffprobe result"
            )
    for key in ("fps", "duration_seconds"):
        if (
            abs(
                _require_finite(video.get(key), f"video {key}")
                - _require_finite(mp4_contract[key], f"ffprobe {key}")
            )
            > 1e-6
        ):
            raise BrowserHorseQaContractError(
                "fixed-camera evidence disagrees with the pinned ffprobe result"
            )
    target_qa = _require_object(
        local.get("target_mesh_deformation_qa"), "target mesh QA"
    )
    if (
        target_qa.get("measured_every_frame") is not True
        or target_qa.get("passed") != machine_passed
    ):
        raise BrowserHorseQaContractError(
            "target mesh QA disagrees with deformation report"
        )
    target_thresholds = _require_object(
        target_qa.get("thresholds"), "target mesh QA thresholds"
    )
    expected_target_thresholds = {
        "maximum_edge_stretch": V14_HORSE_DEFORMATION_THRESHOLDS["maximumEdgeStretch"],
        "p99_edge_stretch": V14_HORSE_DEFORMATION_THRESHOLDS["p99EdgeStretch"],
        "zero_weight_vertices": V14_HORSE_DEFORMATION_THRESHOLDS["zeroWeightVertices"],
    }
    if set(target_thresholds) != set(expected_target_thresholds):
        raise BrowserHorseQaContractError("target mesh QA threshold inventory changed")
    if (
        abs(
            _require_finite(
                target_qa.get("maximum_edge_stretch"),
                "target maximum_edge_stretch",
            )
            - maximum_edge_stretch
        )
        > 1e-9
        or abs(
            _require_finite(
                target_qa.get("p99_edge_stretch"), "target p99_edge_stretch"
            )
            - p99_edge_stretch
        )
        > 1e-9
        or _require_int(
            target_qa.get("zero_weight_vertices"), "target zero_weight_vertices"
        )
        != zero_weight_vertices
        or any(
            abs(
                _require_finite(target_thresholds.get(key), f"target threshold {key}")
                - expected
            )
            > 1e-12
            for key, expected in expected_target_thresholds.items()
        )
    ):
        raise BrowserHorseQaContractError(
            "target mesh QA numeric evidence disagrees with the deformation report"
        )
    _verify_pin(
        target_qa.get("report"),
        inventory["deformation-report.json"],
        "target_mesh_deformation_qa.report",
        exact_path=paths.qa_output_directory / "deformation-report.json",
    )
    local_phase_rows = _require_list(local.get("phase_frames"), "local phase frames")
    if len(local_phase_rows) != len(PHASES):
        raise BrowserHorseQaContractError("local phase frame inventory is incomplete")
    for index, row_value in enumerate(local_phase_rows):
        row = _require_object(row_value, f"local phase frame {index}")
        frame_path = (
            paths.qa_output_directory
            / "frames"
            / f"frame_{expected_phase_indices[index]:04d}.png"
        )
        relative = f"frames/frame_{expected_phase_indices[index]:04d}.png"
        _verify_pin(
            row,
            inventory[relative],
            f"local phase frame {index}",
            exact_path=frame_path,
        )
        if (
            row.get("phase") != PHASES[index]
            or row.get("frame_index") != expected_phase_indices[index]
        ):
            raise BrowserHorseQaContractError("local phase frame identity changed")

    renderer = _require_object(local.get("renderer"), "renderer provenance")
    if (
        renderer.get("browser") != "headless_chrome_cdp"
        or renderer.get("three_revision") != "160"
    ):
        raise BrowserHorseQaContractError("browser renderer provenance changed")
    _verify_pin(
        renderer.get("three_module"),
        validated_config["pins"]["three_module"],
        "renderer.three_module",
        exact_path=validated_config["three_module"],
    )
    human = _require_object(local.get("human_review"), "human review")
    _require_null(human.get("decision"), "human review decision")
    _require_null(human.get("reviewer_id"), "human reviewer id")
    _require_null(human.get("reviewed_at"), "human reviewed_at")
    if human.get("required") is not True:
        raise BrowserHorseQaContractError("human visual review must remain required")
    approvals = _require_object(local.get("approvals"), "approvals")
    if (
        approvals.get("machine_qa_passed") != machine_passed
        or approvals.get("ready_for_human_review") != machine_passed
        or approvals.get("approved_for_animation_library") is not False
        or approvals.get("release_ready") is not False
    ):
        raise BrowserHorseQaContractError(
            "browser evidence attempted to bypass approval"
        )

    profile = validated_config["qa_profile"]
    qa_profile_summary = MappingProxyType(
        {
            "schema": profile["schema"],
            "calibration_state_string": profile["calibration_state_string"],
            "profile_pin": dict(validated_config["pins"]["qa_profile"]),
            "ranking_metrics_emitted": False,
            "adapter_scope": V14_HORSE_QA_SCOPE,
            "production_eligible": False,
            "rig_type": V14_HORSE_RIG_TYPE,
            "source_rig_type": V14_HORSE_SOURCE_RIG_TYPE,
            "semantic_id": V14_HORSE_SEMANTIC_ID,
            "loop": V14_HORSE_LOOP,
            "frame_count": V14_HORSE_FRAME_COUNT,
            "output_fps": V14_HORSE_OUTPUT_FPS,
            "deformation_thresholds": dict(V14_HORSE_DEFORMATION_THRESHOLDS),
        }
    )
    return machine_passed, MappingProxyType(machine_gates), qa_profile_summary


def run_browser_horse_qa(
    config: BrowserHorseQaRunnerConfig,
    request: BrowserHorseQaRequest,
) -> BrowserHorseQaResult:
    """Run and pin one immutable browser-only Horse_2 QA attempt.

    The request carries identities and externally pinned clip bytes only. All
    filesystem locations and executables come from server configuration. The
    function deliberately exposes no ranking metric values because the browser
    QA command does not measure the ranking metrics in ``qa_profile.v1.json``.
    """

    validated_config = _validate_config(config)
    paths = resolve_browser_horse_qa_paths(config, request)
    try:
        return _run_owned_browser_horse_qa_attempt(
            config=config,
            request=request,
            validated_config=validated_config,
            paths=paths,
        )
    except Exception:
        _remove_failed_attempt(config, paths)
        raise


def _run_owned_browser_horse_qa_attempt(
    *,
    config: BrowserHorseQaRunnerConfig,
    request: BrowserHorseQaRequest,
    validated_config: Mapping[str, Any],
    paths: BrowserHorseQaPaths,
) -> BrowserHorseQaResult:
    _, clip_pin = _read_snapshot(paths.three_clip_path, "three clip")
    if clip_pin["sha256"] != request.expected_three_clip_sha256:
        raise BrowserHorseQaContractError(
            "Three clip does not match its externally supplied SHA-256"
        )

    request_receipt = {
        "schema": REQUEST_RECEIPT_SCHEMA,
        "job_id": request.job_id,
        "candidate_id": request.candidate_id,
        "attempt_id": request.attempt_id,
        "semantic_id": request.semantic_id,
        "execution_contract": {
            "browser_only": True,
            "blender_used": False,
            "adapter_scope": V14_HORSE_QA_SCOPE,
            "production_eligible": False,
            "rig_type": V14_HORSE_RIG_TYPE,
            "source_rig_type": V14_HORSE_SOURCE_RIG_TYPE,
            "loop": V14_HORSE_LOOP,
            "frame_count": V14_HORSE_FRAME_COUNT,
            "output_fps": V14_HORSE_OUTPUT_FPS,
            "three_revision": "160",
            "timeout_seconds": config.timeout_seconds,
        },
        "inputs": {
            "source_model_sha256": config.expected_source_model_sha256,
            "immutable_manifest": dict(validated_config["pins"]["immutable_manifest"]),
            "fitting_bundle": dict(validated_config["pins"]["fitting_bundle"]),
            "skeleton": dict(validated_config["pins"]["skeleton"]),
            "skin_weights": dict(validated_config["pins"]["skin_weights"]),
            "surface_topology": dict(validated_config["pins"]["surface_topology"]),
            "immutable_bundle_files": {
                filename: dict(pin)
                for filename, pin in sorted(
                    validated_config["pins"]["bundle_files"].items()
                )
            },
            "three_clip": dict(clip_pin),
            "three_module": dict(validated_config["pins"]["three_module"]),
            "runner_script": dict(validated_config["pins"]["runner_script"]),
            "runner_executable": dict(validated_config["pins"]["runner_executable"]),
            "chrome_executable": dict(validated_config["pins"]["chrome"]),
            "ffmpeg_executable": dict(validated_config["pins"]["ffmpeg"]),
            "ffprobe_executable": dict(validated_config["pins"]["ffprobe"]),
            "qa_profile": dict(validated_config["pins"]["qa_profile"]),
        },
    }
    request_pin = _write_new_canonical_json(paths.request_receipt_path, request_receipt)

    command = [
        os.fspath(validated_config["executable"]),
        os.fspath(validated_config["runner"]),
        "--bundle-dir",
        os.fspath(validated_config["bundle"]),
        "--immutable-manifest-sha256",
        config.expected_immutable_manifest_sha256,
        "--fitting-bundle-sha256",
        config.expected_fitting_bundle_sha256,
        "--source-model-sha256",
        config.expected_source_model_sha256,
        "--three-clip",
        os.fspath(paths.three_clip_path),
        "--three-clip-sha256",
        request.expected_three_clip_sha256,
        "--semantic-id",
        request.semantic_id,
        "--three-module",
        os.fspath(validated_config["three_module"]),
        "--three-module-sha256",
        config.expected_three_module_sha256,
        "--three-revision",
        "160",
        "--chrome",
        os.fspath(validated_config["chrome"]),
        "--ffmpeg",
        os.fspath(validated_config["ffmpeg"]),
        "--ffprobe",
        os.fspath(validated_config["ffprobe"]),
        "--output-dir",
        os.fspath(paths.qa_output_directory),
    ]
    _assert_all_inputs_unchanged(
        validated_config=validated_config,
        paths=paths,
        clip_pin=clip_pin,
    )
    return_code, stdout, stderr = _run_command(
        command,
        cwd=validated_config["runner"].parent,
        timeout_seconds=float(config.timeout_seconds),
    )
    _assert_all_inputs_unchanged(
        validated_config=validated_config,
        paths=paths,
        clip_pin=clip_pin,
    )
    if return_code not in {0, 3}:
        error_text = stderr.decode("utf-8", errors="replace").strip()
        raise BrowserHorseQaSubprocessError(
            f"browser QA exited {return_code}: {error_text[:500] or 'no diagnostic'}"
        )

    if {entry.name for entry in paths.attempt_directory.iterdir()} != {
        "run-request.json",
        "qa-output",
    }:
        raise BrowserHorseQaContractError(
            "browser QA wrote outside its exact artifact directory"
        )
    cli_result = _parse_cli_stdout(stdout)
    inventory, json_values = _snapshot_output_inventory(paths.qa_output_directory)
    mp4_contract = _validate_fixed_camera_mp4(
        validated_config=validated_config,
        paths=paths,
        timeout_seconds=float(config.timeout_seconds),
    )
    _assert_all_inputs_unchanged(
        validated_config=validated_config,
        paths=paths,
        clip_pin=clip_pin,
    )
    machine_passed, machine_gates, qa_profile_summary = _verify_evidence(
        config=config,
        request=request,
        paths=paths,
        validated_config=validated_config,
        inventory=inventory,
        json_values=json_values,
        mp4_contract=mp4_contract,
    )

    expected_status = (
        "PASS_MACHINE_QA_AWAITING_HUMAN" if machine_passed else "FAIL_MACHINE_QA"
    )
    expected_return_code = 0 if machine_passed else 3
    if (
        return_code != expected_return_code
        or cli_result.get("status") != expected_status
    ):
        raise BrowserHorseQaContractError(
            "CLI status/exit code disagrees with pinned machine gates"
        )
    if cli_result.get("approvedForAnimationLibrary") is not False:
        raise BrowserHorseQaContractError(
            "CLI attempted to approve an animation library candidate"
        )
    for field, expected in (
        ("evidencePath", paths.qa_output_directory / "visual-phase-qa.json"),
        ("videoPath", paths.qa_output_directory / "fixed-camera-preview.mp4"),
    ):
        emitted = cli_result.get(field)
        if not isinstance(emitted, str):
            raise BrowserHorseQaContractError(f"CLI {field} is missing")
        emitted_path = _canonical_existing_file(Path(emitted), f"CLI {field}")
        if emitted_path != expected:
            raise BrowserHorseQaContractError(
                f"CLI {field} escaped the canonical QA output"
            )

    stdout_pin = {"bytes": len(stdout), "sha256": _sha256(stdout)}
    stderr_pin = {"bytes": len(stderr), "sha256": _sha256(stderr)}
    phase_artifacts = {
        phase: dict(inventory[f"frames/frame_{index:04d}.png"])
        for phase, index in zip(PHASES, (0, 24, 36))
    }
    run_receipt = {
        "schema": RUN_RECEIPT_SCHEMA,
        "job_id": request.job_id,
        "candidate_id": request.candidate_id,
        "attempt_id": request.attempt_id,
        "semantic_id": request.semantic_id,
        "status": expected_status,
        "execution": {
            "browser_only": True,
            "blender_used": False,
            "subprocess_exit_code": return_code,
            "stdout": stdout_pin,
            "stderr": stderr_pin,
            "fixed_camera_mp4_probe": dict(mp4_contract),
            "request_receipt": {
                "bytes": request_pin["bytes"],
                "sha256": request_pin["sha256"],
            },
        },
        "qa_profile": dict(qa_profile_summary),
        "gates": {
            "machine_qa_passed": machine_passed,
            "machine": dict(machine_gates),
            "human_visual_decision": None,
            "ready_for_human_review": machine_passed,
            "approved_for_animation_library": False,
            "release_ready": False,
        },
        "artifacts": {key: dict(value) for key, value in sorted(inventory.items())},
        "required_phase_artifacts": phase_artifacts,
    }
    _assert_all_inputs_unchanged(
        validated_config=validated_config,
        paths=paths,
        clip_pin=clip_pin,
    )
    final_inventory, final_json_values = _snapshot_output_inventory(
        paths.qa_output_directory
    )
    if final_inventory != inventory or final_json_values != json_values:
        raise BrowserHorseQaContractError(
            "browser QA output inventory changed after evidence verification"
        )
    run_receipt["artifacts"] = {
        key: dict(value) for key, value in sorted(final_inventory.items())
    }
    run_receipt["required_phase_artifacts"] = {
        phase: dict(final_inventory[f"frames/frame_{index:04d}.png"])
        for phase, index in zip(PHASES, (0, 24, 36))
    }
    receipt_pin = _write_new_canonical_json(paths.run_receipt_path, run_receipt)
    if {entry.name for entry in paths.attempt_directory.iterdir()} != {
        "run-request.json",
        "qa-output",
        "run-receipt.json",
    }:
        raise BrowserHorseQaContractError(
            "browser QA attempt inventory changed during receipt publication"
        )

    return BrowserHorseQaResult(
        machine_qa_passed=machine_passed,
        ready_for_human_review=machine_passed,
        human_visual_decision=None,
        approved_for_animation_library=False,
        machine_gates=machine_gates,
        attempt_directory=paths.attempt_directory,
        qa_output_directory=paths.qa_output_directory,
        evidence_path=paths.qa_output_directory / "visual-phase-qa.json",
        video_path=paths.qa_output_directory / "fixed-camera-preview.mp4",
        run_receipt_path=paths.run_receipt_path,
        run_receipt_sha256=str(receipt_pin["sha256"]),
        run_receipt_bytes=int(receipt_pin["bytes"]),
    )

"""Immutable execution bridge from admitted candidates to browser Horse QA.

The browser QA adapter owns subprocess execution and validates every rendered
frame, its fixed-camera MP4, and the machine deformation gates.  Candidate
admission owns the job/candidate lifecycle.  This module joins those two trust
domains without accepting a caller-supplied path or QA result:

* only an admitted, content-addressed candidate can be executed;
* the Three clip is copied from that immutable bundle into an isolated workdir;
* the pinned browser runner executes outside the shared publication lock;
* admission, candidate, runner configuration, and all output pins are rechecked;
* the complete browser attempt is published by atomic directory rename while
  holding the same lock used by admission, selection, and human review; and
* machine PASS remains non-production and still requires human review.

No Blender executable or artifact is accepted by this layer.  Route/auth/DB
wiring deliberately lives elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
from typing import Any, Dict, Mapping

from animal_animation_library import AnimationLibraryError
from animation_fitting_browser_qa_runner import (
    FRAME_COUNT,
    MAX_JSON_ARTIFACT_BYTES,
    MAX_MP4_ARTIFACT_BYTES,
    MAX_PNG_ARTIFACT_BYTES,
    REQUEST_RECEIPT_SCHEMA,
    RUN_RECEIPT_SCHEMA,
    V14_HORSE_QA_SCOPE,
    BrowserHorseQaRequest,
    BrowserHorseQaResult,
    BrowserHorseQaRunnerConfig,
    _validate_config,
    canonical_json_bytes as browser_canonical_json_bytes,
    run_browser_horse_qa,
)
from animation_fitting_candidate_review import (
    MAX_EVIDENCE_BYTES,
    MAX_RECEIPT_BYTES,
    SERVER_EVIDENCE_NAMES,
    TrustedQAEvidence,
    TrustedQARunContext,
    _canonical_json,
    _load_bundle,
    _open_regular_no_follow,
    _pin_payload,
    _read_bounded_file,
    _root,
    _secure_directory_chain,
    _sha,
    _sha256,
    _strict_object,
    _uuid,
)
from config import ANIMATION_FITTING_JOBS_ROOT


QA_EXECUTION_SCHEMA = "autorig.browser-animation-candidate-qa-execution.v1"
QA_EXECUTION_RUNNER_NAME = "animation_fitting_browser_qa_runner"
MAX_EXECUTION_RECEIPT_BYTES = 4 * 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
WINDOWS_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{index}" for index in range(1, 10)}
    | {f"LPT{index}" for index in range(1, 10)}
)
EXPECTED_OUTPUT_ARTIFACTS = frozenset(
    {
        "camera-settings.json",
        "deformation-report.json",
        "fixed-camera-preview.mp4",
        "visual-phase-qa.json",
        *(f"frames/frame_{index:04d}.png" for index in range(FRAME_COUNT)),
    }
)
PHASE_OUTPUTS = {
    "phase-start.png": "frames/frame_0000.png",
    "phase-middle.png": "frames/frame_0024.png",
    "phase-three_quarter.png": "frames/frame_0036.png",
}


class CandidateQaExecutionError(AnimationLibraryError):
    """Fail-closed candidate/admission/browser execution contract error."""


@dataclass(frozen=True)
class ImmutableCandidateQaExecution:
    identity_sha256: str
    directory: Path
    receipt_path: Path
    receipt_sha256: str
    receipt: Dict[str, Any]
    created: bool
    machine_qa_passed: bool
    ready_for_human_review: bool


@dataclass(frozen=True)
class AdmittedCandidateBrowserQaRunner:
    """TrustedQARunner adapter consumed by ``create_server_validation_receipt``."""

    candidate_index: int
    attempt_id: str
    browser_config: BrowserHorseQaRunnerConfig
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT

    def __call__(self, context: TrustedQARunContext) -> TrustedQAEvidence:
        _validate_review_context(self, context)
        execution = execute_admitted_candidate_browser_qa(
            job_id=context.job_id,
            candidate_index=self.candidate_index,
            candidate_identity_sha256=context.candidate_identity_sha256,
            attempt_id=self.attempt_id,
            browser_config=self.browser_config,
            fitting_jobs_root=self.fitting_jobs_root,
        )
        return load_trusted_qa_evidence(
            execution, fitting_jobs_root=self.fitting_jobs_root
        )


def _error(message: str, status_code: int = 409) -> CandidateQaExecutionError:
    return CandidateQaExecutionError(message, status_code=status_code)


def _safe_identifier(value: str, field: str) -> str:
    stem = value.split(".", 1)[0].upper() if isinstance(value, str) else ""
    if (
        not isinstance(value, str)
        or not SAFE_IDENTIFIER_RE.fullmatch(value)
        or value in {".", ".."}
        or value.endswith(".")
        or stem in WINDOWS_RESERVED_NAMES
    ):
        raise _error(f"{field} is not a safe server identifier", 400)
    return value


def _candidate_index(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 16:
        raise _error("candidate_index must be in 0..15", 400)
    return value


def _pin(value: Any, field: str, *, filename: str | None = None) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {"bytes", "sha256"}:
        raise _error(f"{field} pin is invalid")
    size = value.get("bytes")
    digest = value.get("sha256")
    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise _error(f"{field}.bytes is invalid")
    if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
        raise _error(f"{field}.sha256 is invalid")
    result = {"bytes": size, "sha256": digest}
    if filename is not None:
        result["filename"] = filename
    return result


def _browser_artifact_pin(value: Any, field: str) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) not in (
        {"bytes", "sha256"},
        {"bytes", "sha256", "canonical_json_sha256"},
    ):
        raise _error(f"{field} pin is invalid")
    pin = _pin(
        {"bytes": value.get("bytes"), "sha256": value.get("sha256")},
        field,
    )
    canonical_digest = value.get("canonical_json_sha256")
    if canonical_digest is not None and (
        not isinstance(canonical_digest, str)
        or not SHA256_RE.fullmatch(canonical_digest)
    ):
        raise _error(f"{field}.canonical_json_sha256 is invalid")
    return pin


def _json_safe_config_value(value: Any) -> Any:
    if isinstance(value, Path):
        return os.fspath(value.resolve(strict=True))
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe_config_value(child)
            for key, child in sorted(value.items(), key=lambda row: str(row[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe_config_value(child) for child in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_json_safe_config_value(child) for child in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise _error(f"browser QA config contains unsupported {type(value).__name__}")


def _browser_config_fingerprint(
    config: BrowserHorseQaRunnerConfig,
) -> tuple[Dict[str, Any], Mapping[str, Any]]:
    """Validate and hash the complete stable server runner configuration."""

    validated = _validate_config(config)
    declared = {
        field.name: _json_safe_config_value(getattr(config, field.name))
        for field in fields(BrowserHorseQaRunnerConfig)
    }
    validated_pins = _json_safe_config_value(validated.get("pins", {}))
    binding = {
        "schema": "autorig.browser-animation-candidate-qa-config.v1",
        "declared": declared,
        "validated_pins": validated_pins,
    }
    return {
        "identity_sha256": _sha256(_canonical_json(binding)),
        "binding": binding,
    }, validated


def _strict_browser_json(
    path: Path, field: str, maximum: int
) -> tuple[Dict[str, Any], bytes]:
    payload = _read_bounded_file(path, field, maximum)
    try:
        value = json.loads(
            payload.decode("utf-8"),
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON constant {token}")
            ),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise _error(f"{field} is not strict JSON: {exc}") from exc
    if not isinstance(value, dict) or payload != browser_canonical_json_bytes(value):
        raise _error(f"{field} is not canonical browser-runner JSON")
    return value, payload


def _assert_no_final(root: Path, job_id: str) -> None:
    final = root / job_id / "browser-candidate-selection" / "final"
    if final.is_symlink():
        raise _error("candidate FINAL path must not be a symlink")
    if final.exists():
        raise _error("candidate selection is FINAL; QA publication is immutable")


def _admission_snapshot(
    root: Path,
    *,
    job_id: str,
    candidate_index: int,
    candidate_identity: str,
) -> tuple[Dict[str, Any], bytes, Any]:
    # Local import avoids making the existing ingest/review/selection import
    # cycle part of module initialization.
    from animation_fitting_candidate_selection import _load_admission

    admission = _load_admission(root, job_id, candidate_index)
    receipt = admission.receipt
    if receipt.get("candidate_identity_sha256") != candidate_identity:
        raise _error("candidate identity is not admitted in the requested slot")
    bundle = _load_bundle(root, job_id, candidate_identity)
    if receipt.get("candidate_manifest") != bundle.manifest_pin:
        raise _error("candidate admission no longer binds its immutable manifest")
    candidate = bundle.manifest.get("candidate") or {}
    fitting = bundle.manifest.get("fitting_job") or {}
    if (
        candidate.get("candidate_index") != candidate_index
        or fitting.get("id") != job_id
    ):
        raise _error("candidate bundle differs from its admission job/slot")
    payload = _read_bounded_file(
        admission.directory / "admission.json",
        "candidate admission",
        MAX_RECEIPT_BYTES,
    )
    if _sha256(payload) != admission.receipt_pin["sha256"]:
        raise _error("candidate admission changed while it was opened")
    return receipt, payload, bundle


def _execution_target(
    root: Path, job_id: str, candidate_identity: str, attempt_id: str
) -> Path:
    return (
        root
        / job_id
        / "browser-candidate-qa-executions"
        / candidate_identity[:2]
        / candidate_identity
        / attempt_id
    )


def _regular_tree_inventory(directory: Path, field: str) -> set[str]:
    if directory.is_symlink() or not directory.is_dir():
        raise _error(f"{field} is not a real directory")
    result: set[str] = set()
    for parent, directories, files in os.walk(directory, followlinks=False):
        parent_path = Path(parent)
        for name in directories:
            child = parent_path / name
            if child.is_symlink() or not child.is_dir():
                raise _error(f"{field} contains a symlink or invalid directory")
        for name in files:
            child = parent_path / name
            if child.is_symlink() or not child.is_file():
                raise _error(f"{field} contains a symlink or non-file")
            result.add(child.relative_to(directory).as_posix())
    return result


def _copy_pinned(
    source: Path, target: Path, expected: Mapping[str, Any], field: str
) -> None:
    pin = _pin(expected, field)
    if source.is_symlink() or not source.is_file():
        raise _error(f"{field} source is missing or symlinked")
    before = os.lstat(source)
    if not stat.S_ISREG(before.st_mode) or before.st_size != pin["bytes"]:
        raise _error(f"{field} source size changed")
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    total = 0
    try:
        with (
            _open_regular_no_follow(source) as read_handle,
            target.open("xb") as write_handle,
        ):
            while chunk := read_handle.read(1024 * 1024):
                total += len(chunk)
                digest.update(chunk)
                write_handle.write(chunk)
            write_handle.flush()
            os.fsync(write_handle.fileno())
    except Exception:
        target.unlink(missing_ok=True)
        raise
    after = os.lstat(source)
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or total != pin["bytes"]
        or digest.hexdigest() != pin["sha256"]
    ):
        target.unlink(missing_ok=True)
        raise _error(f"{field} changed during immutable copy")


def _write_new(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _verify_tree(directory: Path, expected: Mapping[str, Mapping[str, Any]]) -> None:
    if _regular_tree_inventory(directory, "candidate QA execution") != set(expected):
        raise _error("candidate QA execution inventory changed")
    for relative, raw_pin in expected.items():
        pin = _pin(raw_pin, f"candidate QA execution {relative}")
        payload = _read_bounded_file(
            directory / Path(relative),
            f"candidate QA execution {relative}",
            pin["bytes"],
        )
        if len(payload) != pin["bytes"] or _sha256(payload) != pin["sha256"]:
            raise _error(f"candidate QA execution {relative} pin changed")


def _validate_browser_attempt(
    *,
    result: BrowserHorseQaResult,
    request: BrowserHorseQaRequest,
    bundle: Any,
    config: BrowserHorseQaRunnerConfig,
) -> tuple[Dict[str, Any], bytes, Dict[str, Any], bytes, Dict[str, Dict[str, Any]]]:
    run, run_bytes = _strict_browser_json(
        result.run_receipt_path, "browser run receipt", MAX_EXECUTION_RECEIPT_BYTES
    )
    request_path = result.attempt_directory / "run-request.json"
    requested, request_bytes = _strict_browser_json(
        request_path, "browser run request", MAX_EXECUTION_RECEIPT_BYTES
    )
    request_pin = {"bytes": len(request_bytes), "sha256": _sha256(request_bytes)}
    run_pin = {"bytes": len(run_bytes), "sha256": _sha256(run_bytes)}
    if (
        run.get("schema") != RUN_RECEIPT_SCHEMA
        or requested.get("schema") != REQUEST_RECEIPT_SCHEMA
        or run.get("job_id") != request.job_id
        or requested.get("job_id") != request.job_id
        or run.get("candidate_id") != request.candidate_id
        or requested.get("candidate_id") != request.candidate_id
        or run.get("attempt_id") != request.attempt_id
        or requested.get("attempt_id") != request.attempt_id
        or run.get("semantic_id") != request.semantic_id
        or requested.get("semantic_id") != request.semantic_id
    ):
        raise _error("browser QA request/result identity changed")
    if (
        result.run_receipt_sha256 != run_pin["sha256"]
        or result.run_receipt_bytes != run_pin["bytes"]
        or (run.get("execution") or {}).get("request_receipt") != request_pin
    ):
        raise _error("browser QA result does not bind its immutable receipts")
    contract = requested.get("execution_contract") or {}
    if (
        contract.get("browser_only") is not True
        or contract.get("blender_used") is not False
        or contract.get("adapter_scope") != V14_HORSE_QA_SCOPE
        or contract.get("production_eligible") is not False
    ):
        raise _error("browser QA execution contract attempted to bypass scope")
    inputs = requested.get("inputs") or {}
    three_pin = inputs.get("three_clip") or {}
    if (
        three_pin.get("bytes") != bundle.artifacts["three-clip.json"]["bytes"]
        or three_pin.get("sha256") != request.expected_three_clip_sha256
        or inputs.get("source_model_sha256")
        != bundle.manifest["candidate"].get("source_model_sha256")
        or config.expected_source_model_sha256
        != bundle.manifest["candidate"].get("source_model_sha256")
    ):
        raise _error("browser QA inputs differ from the admitted candidate")
    gates = run.get("gates") or {}
    machine_passed = gates.get("machine_qa_passed")
    expected_status = (
        "PASS_MACHINE_QA_AWAITING_HUMAN"
        if machine_passed is True
        else "FAIL_MACHINE_QA"
    )
    if (
        not isinstance(machine_passed, bool)
        or run.get("status") != expected_status
        or result.machine_qa_passed is not machine_passed
        or result.ready_for_human_review is not machine_passed
        or result.approved_for_animation_library is not False
        or gates.get("ready_for_human_review") is not machine_passed
        or gates.get("human_visual_decision") is not None
        or gates.get("approved_for_animation_library") is not False
        or gates.get("release_ready") is not False
    ):
        raise _error("browser QA machine/human/release gates disagree")
    profile = run.get("qa_profile") or {}
    if (
        profile.get("adapter_scope") != V14_HORSE_QA_SCOPE
        or profile.get("production_eligible") is not False
        or profile.get("ranking_metrics_emitted") is not False
    ):
        raise _error("browser QA profile attempted to claim production eligibility")
    artifacts_value = run.get("artifacts")
    if (
        not isinstance(artifacts_value, Mapping)
        or set(artifacts_value) != EXPECTED_OUTPUT_ARTIFACTS
    ):
        raise _error("browser QA result artifact inventory changed")
    artifacts = {
        relative: _browser_artifact_pin(value, f"browser QA artifact {relative}")
        for relative, value in artifacts_value.items()
    }
    phase_pins = run.get("required_phase_artifacts") or {}
    expected_phase_pins = {
        "start": artifacts[PHASE_OUTPUTS["phase-start.png"]],
        "middle": artifacts[PHASE_OUTPUTS["phase-middle.png"]],
        "three_quarter": artifacts[PHASE_OUTPUTS["phase-three_quarter.png"]],
    }
    if phase_pins != expected_phase_pins:
        raise _error("browser QA required phase pins changed")
    if _regular_tree_inventory(result.qa_output_directory, "browser QA output") != set(
        artifacts
    ):
        raise _error("browser QA output tree differs from its run receipt")
    for relative, pin in artifacts.items():
        payload = _read_bounded_file(
            result.qa_output_directory / Path(relative),
            f"browser QA artifact {relative}",
            pin["bytes"],
        )
        if _sha256(payload) != pin["sha256"]:
            raise _error(f"browser QA artifact {relative} changed after verification")
    return requested, request_bytes, run, run_bytes, artifacts


def _stage_execution_tree(
    *,
    root: Path,
    target: Path,
    result: BrowserHorseQaResult,
    request_bytes: bytes,
    run_bytes: bytes,
    artifacts: Mapping[str, Mapping[str, Any]],
    receipt: Mapping[str, Any],
) -> tuple[Path, Dict[str, Dict[str, Any]]]:
    staging_parent = _secure_directory_chain(root, target.parent, create=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=str(staging_parent)))
    expected: Dict[str, Dict[str, Any]] = {}
    try:
        for relative, payload in (
            ("browser-run-request.json", request_bytes),
            ("browser-run-receipt.json", run_bytes),
        ):
            _write_new(staging / relative, payload)
            expected[relative] = {
                "bytes": len(payload),
                "sha256": _sha256(payload),
            }
        for relative, pin in artifacts.items():
            destination = staging / "qa-output" / Path(relative)
            _copy_pinned(
                result.qa_output_directory / Path(relative),
                destination,
                pin,
                f"browser QA artifact {relative}",
            )
            expected[f"qa-output/{relative}"] = dict(pin)
        receipt_bytes = _canonical_json(dict(receipt)) + b"\n"
        if len(receipt_bytes) > MAX_EXECUTION_RECEIPT_BYTES:
            raise _error("candidate QA execution receipt exceeds the size limit")
        _write_new(staging / "execution-receipt.json", receipt_bytes)
        expected["execution-receipt.json"] = {
            "bytes": len(receipt_bytes),
            "sha256": _sha256(receipt_bytes),
        }
        _verify_tree(staging, expected)
        return staging, expected
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _publish_staged_tree(
    target: Path,
    staging: Path,
    expected: Mapping[str, Mapping[str, Any]],
) -> bool:
    if target.is_symlink():
        raise _error("candidate QA execution target must not be a symlink")
    if target.exists():
        _verify_tree(target, expected)
        shutil.rmtree(staging, ignore_errors=True)
        return False
    try:
        staging.rename(target)
    except OSError:
        if not target.is_dir():
            raise
        _verify_tree(target, expected)
        shutil.rmtree(staging, ignore_errors=True)
        return False
    return True


def _load_execution(
    target: Path, *, created: bool, root: Path | None = None
) -> ImmutableCandidateQaExecution:
    if root is not None:
        try:
            resolved_target = target.resolve(strict=True)
        except (FileNotFoundError, OSError) as exc:
            raise _error("candidate QA execution directory is missing") from exc
        if resolved_target == root or root not in resolved_target.parents:
            raise _error("candidate QA execution escaped its server-owned root")
    receipt_path = target / "execution-receipt.json"
    payload = _read_bounded_file(
        receipt_path, "candidate QA execution receipt", MAX_EXECUTION_RECEIPT_BYTES
    )
    receipt = _strict_object(payload, "candidate QA execution receipt")
    if payload != _canonical_json(receipt) + b"\n":
        raise _error("candidate QA execution receipt is not canonical JSON")
    identity = _sha(receipt.get("identity_sha256"), "execution identity_sha256")
    unsigned = dict(receipt)
    unsigned.pop("identity_sha256", None)
    if (
        receipt.get("schema") != QA_EXECUTION_SCHEMA
        or _sha256(_canonical_json(unsigned)) != identity
    ):
        raise _error("candidate QA execution receipt identity is invalid")
    config_fingerprint = receipt.get("browser_config")
    if (
        not isinstance(config_fingerprint, Mapping)
        or set(config_fingerprint) != {"identity_sha256", "binding"}
        or _sha(config_fingerprint.get("identity_sha256"), "browser config identity")
        != _sha256(_canonical_json(config_fingerprint.get("binding")))
    ):
        raise _error("candidate QA execution browser config fingerprint is invalid")
    job_id = _uuid(receipt.get("job_id"), "execution job_id")
    candidate_identity = _sha(
        receipt.get("candidate_identity_sha256"),
        "execution candidate_identity_sha256",
    )
    _candidate_index(receipt.get("candidate_index"))
    attempt_id = _safe_identifier(receipt.get("attempt_id"), "execution attempt_id")
    if root is not None:
        expected_target = _execution_target(
            root, job_id, candidate_identity, attempt_id
        )
        if target.resolve(strict=True) != expected_target.resolve(strict=True):
            raise _error("candidate QA execution escaped its server-owned path")
    artifact_pins = receipt.get("published_files")
    if not isinstance(artifact_pins, Mapping):
        raise _error("candidate QA execution published_files is invalid")
    expected_published = {
        "browser-run-request.json",
        "browser-run-receipt.json",
        *(f"qa-output/{relative}" for relative in EXPECTED_OUTPUT_ARTIFACTS),
    }
    if set(artifact_pins) != expected_published:
        raise _error("candidate QA execution published inventory is invalid")
    expected = {
        relative: _pin(pin, f"published_files.{relative}")
        for relative, pin in artifact_pins.items()
    }
    expected["execution-receipt.json"] = {
        "bytes": len(payload),
        "sha256": _sha256(payload),
    }
    _verify_tree(target, expected)
    machine_passed = (receipt.get("result") or {}).get("machine_qa_passed")
    if not isinstance(machine_passed, bool):
        raise _error("candidate QA execution result is invalid")
    return ImmutableCandidateQaExecution(
        identity_sha256=identity,
        directory=target,
        receipt_path=receipt_path,
        receipt_sha256=_sha256(payload),
        receipt=receipt,
        created=created,
        machine_qa_passed=machine_passed,
        ready_for_human_review=machine_passed,
    )


def execute_admitted_candidate_browser_qa(
    *,
    job_id: str,
    candidate_index: int,
    candidate_identity_sha256: str,
    attempt_id: str,
    browser_config: BrowserHorseQaRunnerConfig,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
) -> ImmutableCandidateQaExecution:
    """Execute browser QA and atomically publish a complete immutable attempt."""

    job_id = _uuid(job_id, "job_id")
    index = _candidate_index(candidate_index)
    identity = _sha(candidate_identity_sha256, "candidate_identity_sha256")
    attempt_id = _safe_identifier(attempt_id, "attempt_id")
    if not isinstance(browser_config, BrowserHorseQaRunnerConfig):
        raise _error("browser_config is invalid", 400)
    config_fingerprint, _ = _browser_config_fingerprint(browser_config)
    root = _root(fitting_jobs_root)
    target = _execution_target(root, job_id, identity, attempt_id)

    from animation_fitting_candidate_selection import candidate_publication_lock

    with candidate_publication_lock(job_id=job_id, fitting_jobs_root=str(root)):
        _assert_no_final(root, job_id)
        admission, admission_bytes, bundle = _admission_snapshot(
            root,
            job_id=job_id,
            candidate_index=index,
            candidate_identity=identity,
        )
        if target.exists():
            existing = _load_execution(target, created=False, root=root)
            if (
                existing.receipt.get("job_id") != job_id
                or existing.receipt.get("candidate_identity_sha256") != identity
                or existing.receipt.get("candidate_index") != index
                or existing.receipt.get("attempt_id") != attempt_id
                or existing.receipt.get("admission", {}).get("sha256")
                != _sha256(admission_bytes)
                or existing.receipt.get("browser_config") != config_fingerprint
            ):
                raise _error("candidate QA attempt identity collision")
            return existing

    work_parent = _secure_directory_chain(
        root, root / job_id / "browser-candidate-qa-work", create=True
    )
    work = Path(tempfile.mkdtemp(prefix=f".{identity[:12]}.", dir=str(work_parent)))
    staged: Path | None = None
    try:
        input_root = work / "inputs"
        output_root = work / "outputs"
        candidate_input = input_root / "jobs" / job_id / "candidates" / identity
        candidate_input.mkdir(parents=True)
        output_root.mkdir()
        clip_pin = bundle.artifacts["three-clip.json"]
        _copy_pinned(
            bundle.directory / "three-clip.json",
            candidate_input / "three-clip.json",
            {"bytes": clip_pin["bytes"], "sha256": clip_pin["sha256"]},
            "admitted candidate Three clip",
        )
        run_config = replace(
            browser_config,
            input_root=input_root,
            output_root=output_root,
        )
        request = BrowserHorseQaRequest(
            job_id=job_id,
            candidate_id=identity,
            attempt_id=attempt_id,
            semantic_id=str(bundle.manifest["fitting_job"].get("semantic_id") or ""),
            expected_three_clip_sha256=clip_pin["sha256"],
        )
        result = run_browser_horse_qa(run_config, request)
        requested, request_bytes, run, run_bytes, artifacts = _validate_browser_attempt(
            result=result,
            request=request,
            bundle=bundle,
            config=run_config,
        )
        # Revalidate all configured executable/profile/bundle pins after the
        # result has been copied out of the subprocess boundary.
        _validate_config(run_config)
        current_config_fingerprint, _ = _browser_config_fingerprint(browser_config)
        if current_config_fingerprint != config_fingerprint:
            raise _error("browser QA config/runtime changed during execution")
        admission_pin = _pin_payload(admission_bytes, "admission.json")
        published_files = {
            "browser-run-request.json": {
                "bytes": len(request_bytes),
                "sha256": _sha256(request_bytes),
            },
            "browser-run-receipt.json": {
                "bytes": len(run_bytes),
                "sha256": _sha256(run_bytes),
            },
            **{
                f"qa-output/{relative}": dict(pin)
                for relative, pin in sorted(artifacts.items())
            },
        }
        machine_passed = bool((run.get("gates") or {}).get("machine_qa_passed"))
        binding = {
            "schema": QA_EXECUTION_SCHEMA,
            "job_id": job_id,
            "candidate_index": index,
            "candidate_identity_sha256": identity,
            "attempt_id": attempt_id,
            "admission": admission_pin,
            "browser_config": config_fingerprint,
            "candidate": {
                "manifest": bundle.manifest_pin,
                "three_clip": bundle.artifacts["three-clip.json"],
                "source_video": bundle.artifacts["source-video.mp4"],
            },
            "execution_contract": {
                "browser_only": True,
                "blender_used": False,
                "adapter_scope": V14_HORSE_QA_SCOPE,
                "production_eligible": False,
                "human_review_required": True,
            },
            "browser_run": {
                "request": published_files["browser-run-request.json"],
                "receipt": published_files["browser-run-receipt.json"],
                "status": run["status"],
                "inputs": requested["inputs"],
                "qa_profile": run["qa_profile"],
            },
            "result": {
                "machine_qa_passed": machine_passed,
                "ready_for_human_review": machine_passed,
                "human_visual_decision": None,
                "approved_for_animation_library": False,
                "release_ready": False,
            },
            "published_files": published_files,
        }
        execution_identity = _sha256(_canonical_json(binding))
        receipt = {**binding, "identity_sha256": execution_identity}
        staged, expected = _stage_execution_tree(
            root=root,
            target=target,
            result=result,
            request_bytes=request_bytes,
            run_bytes=run_bytes,
            artifacts=artifacts,
            receipt=receipt,
        )
        with candidate_publication_lock(job_id=job_id, fitting_jobs_root=str(root)):
            _assert_no_final(root, job_id)
            current_admission, current_admission_bytes, current_bundle = (
                _admission_snapshot(
                    root,
                    job_id=job_id,
                    candidate_index=index,
                    candidate_identity=identity,
                )
            )
            if (
                current_admission != admission
                or current_admission_bytes != admission_bytes
                or current_bundle.manifest_pin != bundle.manifest_pin
                or current_bundle.artifacts["three-clip.json"]
                != bundle.artifacts["three-clip.json"]
            ):
                raise _error(
                    "candidate admission or immutable bundle changed before QA publication"
                )
            _validate_config(run_config)
            current_config_fingerprint, _ = _browser_config_fingerprint(browser_config)
            if current_config_fingerprint != config_fingerprint:
                raise _error("browser QA config/runtime changed before publication")
            _verify_tree(staged, expected)
            created = _publish_staged_tree(target, staged, expected)
            staged = None
            published = _load_execution(target, created=created, root=root)
            if (
                published.identity_sha256 != execution_identity
                or published.receipt != receipt
                or published.receipt_sha256
                != expected["execution-receipt.json"]["sha256"]
                or published.receipt.get("published_files") != published_files
            ):
                raise _error("published candidate QA differs from its staged receipt")
        return published
    finally:
        if staged is not None:
            shutil.rmtree(staged, ignore_errors=True)
        shutil.rmtree(work, ignore_errors=True)


def load_trusted_qa_evidence(
    execution: ImmutableCandidateQaExecution,
    *,
    fitting_jobs_root: str = ANIMATION_FITTING_JOBS_ROOT,
) -> TrustedQAEvidence:
    """Rehash a published machine PASS into the existing review-runner type."""

    if not isinstance(execution, ImmutableCandidateQaExecution):
        raise _error("candidate QA execution is invalid", 400)
    root = _root(fitting_jobs_root)
    verified = _load_execution(execution.directory, created=False, root=root)
    if (
        verified.identity_sha256 != execution.identity_sha256
        or verified.receipt_sha256 != execution.receipt_sha256
        or not verified.machine_qa_passed
    ):
        raise _error("only an immutable machine PASS may enter trusted review")
    from animation_fitting_candidate_selection import candidate_publication_lock

    with candidate_publication_lock(
        job_id=verified.receipt["job_id"], fitting_jobs_root=str(root)
    ):
        _, admission_bytes, bundle = _admission_snapshot(
            root,
            job_id=verified.receipt["job_id"],
            candidate_index=verified.receipt["candidate_index"],
            candidate_identity=verified.receipt["candidate_identity_sha256"],
        )
        if (
            verified.receipt.get("admission")
            != _pin_payload(admission_bytes, "admission.json")
            or verified.receipt.get("candidate", {}).get("manifest")
            != bundle.manifest_pin
            or verified.receipt.get("candidate", {}).get("three_clip")
            != bundle.artifacts["three-clip.json"]
        ):
            raise _error("published browser QA no longer binds its admission")

    def read_published(relative: str, field: str, maximum: int) -> bytes:
        raw_pin = verified.receipt["published_files"].get(relative)
        pin = _pin(raw_pin, f"published_files.{relative}")
        if pin["bytes"] > maximum:
            raise _error(f"{field} exceeds its trusted evidence limit")
        payload = _read_bounded_file(
            verified.directory / Path(relative), field, pin["bytes"]
        )
        if len(payload) != pin["bytes"] or _sha256(payload) != pin["sha256"]:
            raise _error(f"{field} changed after execution receipt verification")
        return payload

    metrics_bytes = read_published(
        "qa-output/visual-phase-qa.json",
        "published browser QA visual metrics",
        MAX_JSON_ARTIFACT_BYTES,
    )
    metrics = _strict_object(metrics_bytes, "published browser QA visual metrics")
    artifacts: Dict[str, bytes] = {}
    source_names = {
        "camera-settings.json": "camera-settings.json",
        "deformation-report.json": "deformation-report.json",
        "fixed-camera-preview.mp4": "fixed-camera-preview.mp4",
        **PHASE_OUTPUTS,
    }
    for output_name in SERVER_EVIDENCE_NAMES:
        source_relative = source_names[output_name]
        maximum = (
            MAX_JSON_ARTIFACT_BYTES
            if output_name.endswith(".json")
            else MAX_MP4_ARTIFACT_BYTES
            if output_name.endswith(".mp4")
            else MAX_PNG_ARTIFACT_BYTES
        )
        artifacts[output_name] = read_published(
            f"qa-output/{source_relative}",
            f"published browser QA {output_name}",
            min(MAX_EVIDENCE_BYTES, maximum),
        )
    return TrustedQAEvidence(
        runner_name=QA_EXECUTION_RUNNER_NAME,
        runner_revision=f"{QA_EXECUTION_SCHEMA}:{verified.identity_sha256}",
        metrics=metrics,
        artifacts=artifacts,
    )


def _validate_review_context(
    runner: AdmittedCandidateBrowserQaRunner,
    context: TrustedQARunContext,
) -> None:
    if not isinstance(context, TrustedQARunContext):
        raise _error("trusted QA context is invalid", 400)
    root = _root(runner.fitting_jobs_root)
    identity = _sha(context.candidate_identity_sha256, "candidate_identity_sha256")
    bundle = _load_bundle(root, _uuid(context.job_id, "job_id"), identity)
    if (
        context.candidate_directory.resolve(strict=True) != bundle.directory
        or context.three_clip_path.resolve(strict=True)
        != bundle.directory / "three-clip.json"
        or context.semantic_id != bundle.manifest["fitting_job"].get("semantic_id")
        or context.rig_type != bundle.manifest["library"].get("rig_type")
    ):
        raise _error("trusted review context differs from the admitted candidate")
    runtime = context.runtime
    runtime_pins = context.runtime_pins
    expected = (
        (
            "node",
            runtime.node_path,
            runner.browser_config.runner_executable,
            runner.browser_config.expected_runner_executable_sha256,
        ),
        (
            "chrome",
            runtime.chrome_path,
            runner.browser_config.chrome_executable,
            runner.browser_config.expected_chrome_executable_sha256,
        ),
        (
            "ffmpeg",
            runtime.ffmpeg_path,
            runner.browser_config.ffmpeg_executable,
            runner.browser_config.expected_ffmpeg_executable_sha256,
        ),
        (
            "ffprobe",
            runtime.ffprobe_path,
            runner.browser_config.ffprobe_executable,
            runner.browser_config.expected_ffprobe_executable_sha256,
        ),
        (
            "three_module",
            runtime.three_module_path,
            runner.browser_config.three_module,
            runner.browser_config.expected_three_module_sha256,
        ),
    )
    if (
        runtime.three_revision != "160"
        or runtime.three_expected_sha256
        != runner.browser_config.expected_three_module_sha256
    ):
        raise _error("trusted review context Three.js revision changed")
    for name, context_path, configured_path, configured_sha in expected:
        pin = runtime_pins.get(name) or {}
        if (
            context_path.resolve(strict=True) != configured_path.resolve(strict=True)
            or pin.get("sha256") != configured_sha
        ):
            raise _error(f"trusted review {name} differs from browser QA configuration")

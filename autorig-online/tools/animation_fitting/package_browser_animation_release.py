"""Plan, validate, and atomically publish a browser-fitted 30-clip release.

Fitting stays in the browser.  Blender is used only as a headless export and
independent FBX-import validation backend.  Approval, runtime, source asset,
and validation roots are server-owned CLI configuration; none are selected by
the caller release manifest.

Trusted FBX validation producer contract
----------------------------------------
Each export plan contains a second allowlisted Blender command.  It imports the
exact FBX snapshot with this canonical script and writes a create-exclusive
``autorig.browser-animation-fbx-validation-receipt.v1`` under the configured
server-only validation root.  The receipt binds FBX bytes, candidate motion,
source asset identity, exact skeleton, semantic take, hierarchy, frame count,
30 FPS duration, Blender executable/version, and validator-script hash.  The
release publisher never accepts an FBX header/string heuristic or a receipt
from the worker output tree.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import sys
import tempfile
from typing import Any, BinaryIO, Mapping, Sequence
import zipfile

import numpy as np

if __package__:
    from .bridge_browser_animation_motion import (
        ApprovalEvidence,
        BridgeError,
        LOOP_POSITION_CLOSURE_TOLERANCE,
        LOOP_ROTATION_CLOSURE_RADIANS,
        OUTPUT_FPS,
        RESULT_SCHEMA as BRIDGE_RESULT_SCHEMA,
        Snapshot,
        _canonical,
        _json,
        _lexical_absolute,
        _resolve_final_approval,
        _rotation_distance,
        _secure_file,
        _secure_root,
        _skeleton_fingerprint,
        _snapshot,
        _validate_glb_skeleton,
    )
    from .motion_export_contract import (
        ASSET_BUNDLE_SCHEMA,
        TARGET_SCHEMA,
        load_motion,
        load_target_spec,
        validate_target_source,
    )
    from .package_browser_animation_glb import (
        OUTPUT_SCHEMA as GLB_PACKAGE_OUTPUT_SCHEMA,
        _load_inputs as load_glb_package_inputs,
        load_animal_taxonomy,
    )
    from .rig import _load_bones
else:  # pragma: no cover - direct script execution boundary
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from animation_fitting.bridge_browser_animation_motion import (
        ApprovalEvidence,
        BridgeError,
        LOOP_POSITION_CLOSURE_TOLERANCE,
        LOOP_ROTATION_CLOSURE_RADIANS,
        OUTPUT_FPS,
        RESULT_SCHEMA as BRIDGE_RESULT_SCHEMA,
        Snapshot,
        _canonical,
        _json,
        _lexical_absolute,
        _resolve_final_approval,
        _rotation_distance,
        _secure_file,
        _secure_root,
        _skeleton_fingerprint,
        _snapshot,
        _validate_glb_skeleton,
    )
    from animation_fitting.motion_export_contract import (
        ASSET_BUNDLE_SCHEMA,
        TARGET_SCHEMA,
        load_motion,
        load_target_spec,
        validate_target_source,
    )
    from animation_fitting.package_browser_animation_glb import (
        OUTPUT_SCHEMA as GLB_PACKAGE_OUTPUT_SCHEMA,
        _load_inputs as load_glb_package_inputs,
        load_animal_taxonomy,
    )
    from animation_fitting.rig import _load_bones


INPUT_SCHEMA = "autorig.browser-animation-release-input.v2"
SERVER_CONFIG_SCHEMA = "autorig.browser-animation-release-server-config.v1"
PLAN_SCHEMA = "autorig.browser-animation-release-export-plan.v2"
FBX_RECEIPT_SCHEMA = "autorig.browser-animation-fbx-validation-receipt.v1"
ZIP_INDEX_SCHEMA = "autorig.browser-animation-fbx-index.v2"
RESULT_SCHEMA = "autorig.browser-animation-release-result.v2"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
MAX_JSON_BYTES = 64 * 1024 * 1024
GLB_MAGIC = b"glTF"
GLB_JSON_CHUNK = 0x4E4F534A
GLB_BIN_CHUNK = 0x004E4942
FLOAT_COMPONENT_TYPE = 5126
CANONICAL_APPLIER = Path(__file__).with_name("apply_fitted_motion.py")
CANONICAL_VALIDATOR = Path(__file__)


class ReleaseError(RuntimeError):
    """Fail-closed release contract violation."""


@dataclass(frozen=True)
class FilePin:
    path: Path
    bytes: int
    sha256: str

    def descriptor(self) -> dict[str, Any]:
        return {"path": str(self.path), "bytes": self.bytes, "sha256": self.sha256}

    def filename_pin(self) -> dict[str, Any]:
        return {"filename": self.path.name, "bytes": self.bytes, "sha256": self.sha256}


@dataclass(frozen=True)
class ServerConfig:
    manifest: Snapshot
    config_id: str
    fitting_jobs_root: Path
    validation_root: Path
    approval_job_ids: Mapping[str, str]
    task_id: str
    task_guid: str
    task_model: Snapshot
    skeleton: Snapshot
    source_blend: Snapshot
    target_manifest: Snapshot
    browser_asset_identity_sha256: str
    export_asset_identity_sha256: str
    blender_id: str
    blender_executable: Snapshot
    blender_version: str
    applier: Snapshot
    validator: Snapshot


@dataclass(frozen=True)
class ApprovedMotion:
    semantic_id: str
    motion: Snapshot
    frame_profile: int
    loop: bool
    approval: ApprovalEvidence


@dataclass(frozen=True)
class ReleaseInputs:
    manifest: Snapshot
    taxonomy: Snapshot
    taxonomy_value: Mapping[str, Any]
    clip_ids: tuple[str, ...]
    library_revision: str
    rig_type: str
    orientation: str
    template_skeleton_sha256: str
    glb_package_result: Snapshot
    browser_motion_bridge_result: Snapshot
    multi_clip_glb: Snapshot
    source_blend: Snapshot
    target_manifest: Snapshot
    skeleton: Snapshot
    armature_name: str
    armature_world: np.ndarray
    bones: Mapping[str, Any]
    bone_order: tuple[str, ...]
    skeleton_fingerprint_sha256: str
    approvals: tuple[ApprovalEvidence, ...]
    clips: tuple[ApprovedMotion, ...]
    server: ServerConfig


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ReleaseError(f"{field} must be an object")
    return value


def _exact(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    if set(value) != expected:
        raise ReleaseError(
            f"{field} must contain exactly {', '.join(sorted(expected))}"
        )


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ReleaseError(f"{field} must be a non-empty NUL-free string")
    return value.strip()


def _sha(value: Any, field: str) -> str:
    result = _string(value, field)
    if not SHA256_RE.fullmatch(result):
        raise ReleaseError(f"{field} must be a lowercase SHA-256")
    return result


def _uuid(value: Any, field: str) -> str:
    result = _string(value, field)
    if not UUID_RE.fullmatch(result):
        raise ReleaseError(f"{field} must be a canonical lowercase UUID")
    return result


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReleaseError(f"{field} must be a positive integer")
    return value


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReleaseError(f"{field} must be finite")
    result = float(value)
    if not math.isfinite(result):
        raise ReleaseError(f"{field} must be finite")
    return result


def _snapshot_checked(
    path: str | Path,
    field: str,
    *,
    root: Path | None = None,
    maximum: int | None = None,
) -> Snapshot:
    try:
        return _snapshot(path, field, root=root, maximum=maximum)
    except BridgeError as exc:
        raise ReleaseError(str(exc)) from exc


def _json_checked(
    snapshot: Snapshot, field: str, *, canonical: bool = False
) -> Mapping[str, Any]:
    try:
        return _json(snapshot, field, canonical=canonical)
    except BridgeError as exc:
        raise ReleaseError(str(exc)) from exc


def _secure_root_checked(path: str | Path, field: str) -> Path:
    try:
        return _secure_root(path, field)
    except BridgeError as exc:
        raise ReleaseError(str(exc)) from exc


def _descriptor(
    base: Path,
    value: Any,
    field: str,
    *,
    allowed_root: Path | None = None,
    maximum: int | None = None,
) -> Snapshot:
    descriptor = _object(value, field)
    _exact(descriptor, {"path", "bytes", "sha256"}, field)
    raw = Path(_string(descriptor["path"], f"{field}.path"))
    path = raw if raw.is_absolute() else base / raw
    snapshot = _snapshot_checked(path, field, root=allowed_root, maximum=maximum)
    if snapshot.size != _positive_int(
        descriptor["bytes"], f"{field}.bytes"
    ) or snapshot.sha256 != _sha(descriptor["sha256"], f"{field}.sha256"):
        raise ReleaseError(f"{field} descriptor does not match immutable bytes")
    return snapshot


def _same_descriptor(value: Any, snapshot: Snapshot | FilePin, field: str) -> None:
    descriptor = _object(value, field)
    _exact(descriptor, {"path", "bytes", "sha256"}, field)
    expected = {
        "path": str(snapshot.path),
        "bytes": snapshot.bytes if isinstance(snapshot, FilePin) else snapshot.size,
        "sha256": snapshot.sha256,
    }
    if descriptor != expected:
        raise ReleaseError(f"{field} does not match independently resolved bytes")


def _same_filename_pin(value: Any, snapshot: Snapshot | FilePin, field: str) -> None:
    descriptor = _object(value, field)
    _exact(descriptor, {"filename", "bytes", "sha256"}, field)
    expected = {
        "filename": snapshot.path.name,
        "bytes": snapshot.bytes if isinstance(snapshot, FilePin) else snapshot.size,
        "sha256": snapshot.sha256,
    }
    if descriptor != expected:
        raise ReleaseError(f"{field} does not match independently resolved bytes")


def _repin(snapshot: Snapshot, field: str) -> None:
    current = _snapshot_checked(
        snapshot.path, field, maximum=max(snapshot.size, MAX_JSON_BYTES)
    )
    if current.size != snapshot.size or current.sha256 != snapshot.sha256:
        raise ReleaseError(f"{field} changed after validation")


def _source_identity(
    *,
    task_id: str,
    task_guid: str,
    task_model: Snapshot,
    skeleton: Snapshot,
    rig_type: str,
    orientation: str,
) -> str:
    value = {
        "source_task": {"id": task_id, "guid": task_guid},
        "task_model": task_model.filename_pin(),
        "task_skeleton": skeleton.filename_pin(),
        "template_skeleton_sha256": skeleton.sha256,
        "rig_type": rig_type,
        "orientation": orientation,
    }
    return hashlib.sha256(_canonical(value, newline=False)).hexdigest()


def _export_identity(
    browser_identity: str, source_blend: Snapshot, target: Snapshot
) -> str:
    value = {
        "browser_asset_identity_sha256": browser_identity,
        "source_blend": source_blend.descriptor(),
        "target_manifest": target.descriptor(),
    }
    return hashlib.sha256(_canonical(value, newline=False)).hexdigest()


def _load_server_config(path: str | Path, expected_sha256: str) -> ServerConfig:
    manifest = _snapshot_checked(path, "server export config", maximum=MAX_JSON_BYTES)
    if manifest.sha256 != _sha(expected_sha256, "server export config SHA-256"):
        raise ReleaseError("server export config SHA-256 mismatch")
    value = _json_checked(manifest, "server export config", canonical=True)
    _exact(
        value,
        {
            "schema",
            "config_id",
            "fitting_jobs_root",
            "fbx_validation_root",
            "approval_jobs",
            "source_asset",
            "blender",
            "canonical_applier_sha256",
            "canonical_validator_sha256",
        },
        "server export config",
    )
    if value.get("schema") != SERVER_CONFIG_SCHEMA:
        raise ReleaseError(
            f"server export config schema must be {SERVER_CONFIG_SCHEMA}"
        )
    config_id = _string(value["config_id"], "server export config_id")
    if not SAFE_ID_RE.fullmatch(config_id):
        raise ReleaseError("server export config_id is not portable")
    fitting_root = _secure_root_checked(value["fitting_jobs_root"], "fitting_jobs_root")
    validation_root = _secure_root_checked(
        value["fbx_validation_root"], "fbx_validation_root"
    )
    rows = value.get("approval_jobs")
    if not isinstance(rows, list) or len(rows) != 30:
        raise ReleaseError("server approval_jobs must contain exactly 30 rows")
    approval_jobs: dict[str, str] = {}
    seen_jobs: set[str] = set()
    for index, raw in enumerate(rows):
        row = _object(raw, f"server approval_jobs[{index}]")
        _exact(row, {"semantic_id", "job_id"}, f"server approval_jobs[{index}]")
        semantic_id = _string(
            row["semantic_id"], f"server approval_jobs[{index}].semantic_id"
        )
        job_id = _uuid(row["job_id"], f"server approval_jobs[{index}].job_id")
        if semantic_id in approval_jobs or job_id in seen_jobs:
            raise ReleaseError("server approval_jobs repeats semantic ID or job ID")
        approval_jobs[semantic_id] = job_id
        seen_jobs.add(job_id)
    source = _object(value["source_asset"], "server source_asset")
    _exact(
        source,
        {
            "task_id",
            "task_guid",
            "task_model",
            "task_skeleton",
            "source_blend",
            "target_manifest",
            "browser_asset_identity_sha256",
            "export_asset_identity_sha256",
        },
        "server source_asset",
    )
    task_id = _uuid(source["task_id"], "server source task_id")
    task_guid = _uuid(source["task_guid"], "server source task_guid")
    base = manifest.path.parent
    task_model = _descriptor(base, source["task_model"], "server task model")
    skeleton = _descriptor(
        base,
        source["task_skeleton"],
        "server task skeleton",
        maximum=MAX_JSON_BYTES,
    )
    source_blend = _descriptor(base, source["source_blend"], "server source blend")
    target = _descriptor(
        base,
        source["target_manifest"],
        "server target manifest",
        maximum=MAX_JSON_BYTES,
    )
    target_value = _json_checked(target, "server target manifest")
    rig_type = _string(target_value.get("rig_type"), "target rig_type")
    orientation = _string(target_value.get("orientation"), "target orientation")
    browser_identity = _source_identity(
        task_id=task_id,
        task_guid=task_guid,
        task_model=task_model,
        skeleton=skeleton,
        rig_type=rig_type,
        orientation=orientation,
    )
    export_identity = _export_identity(browser_identity, source_blend, target)
    if (
        source.get("browser_asset_identity_sha256") != browser_identity
        or source.get("export_asset_identity_sha256") != export_identity
    ):
        raise ReleaseError("server source asset identity hashes are invalid")

    blender = _object(value["blender"], "server blender")
    _exact(blender, {"selected_id", "allowlist"}, "server blender")
    selected_id = _string(blender["selected_id"], "server blender.selected_id")
    allowlist = blender.get("allowlist")
    if not isinstance(allowlist, list) or not allowlist:
        raise ReleaseError("server Blender allowlist must be non-empty")
    selected_rows = []
    seen_blender_ids: set[str] = set()
    seen_blender_hashes: set[str] = set()
    for index, raw in enumerate(allowlist):
        row = _object(raw, f"server blender.allowlist[{index}]")
        _exact(
            row, {"id", "executable", "version"}, f"server blender.allowlist[{index}]"
        )
        blender_id = _string(row["id"], f"server blender.allowlist[{index}].id")
        if not SAFE_ID_RE.fullmatch(blender_id) or blender_id in seen_blender_ids:
            raise ReleaseError("server Blender allowlist ID is invalid or repeated")
        executable = _descriptor(
            base,
            row["executable"],
            f"server blender.allowlist[{index}].executable",
        )
        version = _string(row["version"], f"server blender.allowlist[{index}].version")
        if executable.sha256 in seen_blender_hashes:
            raise ReleaseError("server Blender allowlist repeats executable bytes")
        seen_blender_ids.add(blender_id)
        seen_blender_hashes.add(executable.sha256)
        if blender_id == selected_id:
            selected_rows.append((executable, version))
    if len(selected_rows) != 1:
        raise ReleaseError(
            "selected Blender is absent or duplicated in server allowlist"
        )
    blender_executable, blender_version = selected_rows[0]
    applier = _snapshot_checked(CANONICAL_APPLIER, "canonical apply_fitted_motion.py")
    validator = _snapshot_checked(CANONICAL_VALIDATOR, "canonical release validator")
    if applier.path != CANONICAL_APPLIER.resolve(strict=True) or applier.sha256 != _sha(
        value["canonical_applier_sha256"], "canonical_applier_sha256"
    ):
        raise ReleaseError(
            "server config does not pin exact canonical apply_fitted_motion.py"
        )
    if validator.path != CANONICAL_VALIDATOR.resolve(
        strict=True
    ) or validator.sha256 != _sha(
        value["canonical_validator_sha256"], "canonical_validator_sha256"
    ):
        raise ReleaseError("server config does not pin this exact canonical validator")
    return ServerConfig(
        manifest=manifest,
        config_id=config_id,
        fitting_jobs_root=fitting_root,
        validation_root=validation_root,
        approval_job_ids=approval_jobs,
        task_id=task_id,
        task_guid=task_guid,
        task_model=task_model,
        skeleton=skeleton,
        source_blend=source_blend,
        target_manifest=target,
        browser_asset_identity_sha256=browser_identity,
        export_asset_identity_sha256=export_identity,
        blender_id=selected_id,
        blender_executable=blender_executable,
        blender_version=blender_version,
        applier=applier,
        validator=validator,
    )


def _approval_descriptor_matches(
    value: Any, approval: ApprovalEvidence, field: str
) -> None:
    expected = approval.result_descriptor()
    if value != expected:
        raise ReleaseError(
            f"{field} differs from current server-owned FINAL/PASS evidence"
        )


def _validate_package_result(
    result_pin: Snapshot,
    *,
    root: Path,
    taxonomy: Snapshot,
    clip_ids: tuple[str, ...],
    library_revision: str,
    rig_type: str,
    orientation: str,
    skeleton_sha256: str,
    approvals: Sequence[ApprovalEvidence],
    server: ServerConfig,
) -> tuple[Snapshot, Snapshot, Any]:
    result = _json_checked(result_pin, "multi-clip GLB package result", canonical=True)
    _exact(
        result,
        {
            "schema",
            "library_revision",
            "rig_type",
            "orientation",
            "template_skeleton_sha256",
            "taxonomy",
            "source",
            "input_manifest",
            "clips",
            "output",
            "source_bin_prefix_bytes",
            "animation_count",
        },
        "multi-clip GLB package result",
    )
    expected = {
        "schema": GLB_PACKAGE_OUTPUT_SCHEMA,
        "library_revision": library_revision,
        "rig_type": rig_type,
        "orientation": orientation,
        "template_skeleton_sha256": skeleton_sha256,
        "animation_count": 30,
    }
    for key, expected_value in expected.items():
        if result.get(key) != expected_value:
            raise ReleaseError(f"multi-clip GLB package result {key} mismatch")
    _same_descriptor(result["taxonomy"], taxonomy, "package result taxonomy")
    package_input = _descriptor(
        result_pin.path.parent,
        result["input_manifest"],
        "multi-clip package input",
        allowed_root=root,
        maximum=MAX_JSON_BYTES,
    )
    try:
        package = load_glb_package_inputs(package_input.path, package_input.sha256)
    except Exception as exc:
        raise ReleaseError(
            f"multi-clip package input no longer validates: {exc}"
        ) from exc
    for snapshot in (
        package.manifest_snapshot,
        package.source,
        *(clip.source for clip in package.clips),
    ):
        try:
            _secure_file(snapshot.path, "multi-clip package artifact", root=root)
        except BridgeError as exc:
            raise ReleaseError(str(exc)) from exc
    if (
        package.library_revision != library_revision
        or package.rig_type != rig_type
        or package.orientation != orientation
        or package.template_skeleton_sha256 != skeleton_sha256
        or package.clip_ids != clip_ids
        or package.source.size != server.task_model.size
        or package.source.sha256 != server.task_model.sha256
    ):
        raise ReleaseError(
            "multi-clip package identity/source differs from server contract"
        )
    _same_descriptor(result["source"], server.task_model, "package result source")
    rows = result.get("clips")
    if not isinstance(rows, list) or len(rows) != 30:
        raise ReleaseError("package result must contain exactly 30 clip rows")
    for index, (raw, package_clip, approval, semantic_id) in enumerate(
        zip(rows, package.clips, approvals, clip_ids, strict=True)
    ):
        row = _object(raw, f"package result clips[{index}]")
        if (
            row.get("semantic_id") != semantic_id
            or package_clip.semantic_id != semantic_id
            or row.get("candidate_id") != approval.candidate_id
            or row.get("candidate_bundle_sha256") != approval.candidate_bundle_sha256
            or row.get("human_review_sha256") != approval.human_review_sha256
            or package_clip.candidate_id != approval.candidate_id
            or package_clip.candidate_bundle_sha256 != approval.candidate_bundle_sha256
            or package_clip.human_review_sha256 != approval.human_review_sha256
            or package_clip.source.size != approval.browser_clip.size
            or package_clip.source.sha256 != approval.browser_clip.sha256
        ):
            raise ReleaseError(
                f"package result approval/clip mismatch for {semantic_id}"
            )
        clip_descriptor = {key: row[key] for key in ("path", "bytes", "sha256")}
        _same_descriptor(
            clip_descriptor, package_clip.source, f"package clip {semantic_id}"
        )
    output = _descriptor(
        result_pin.path.parent,
        result["output"],
        "multi-clip GLB output",
        allowed_root=root,
    )
    return output, package_input, package


def _load_skeleton(
    snapshot: Snapshot,
) -> tuple[str, np.ndarray, Mapping[str, Any], tuple[str, ...]]:
    with tempfile.TemporaryDirectory(prefix="autorig-release-skeleton-") as temp:
        copy = Path(temp) / "skeleton.json"
        copy.write_bytes(snapshot.data)
        try:
            return _load_bones(copy)
        except Exception as exc:
            raise ReleaseError(f"server task skeleton is invalid: {exc}") from exc


def _validate_motion_contract(
    motion_snapshot: Snapshot,
    *,
    semantic_id: str,
    frame_profile: int,
    loop: bool,
    armature_name: str,
    bones: Mapping[str, Any],
    bone_order: Sequence[str],
    approval: ApprovalEvidence,
    server: ServerConfig,
    package_input: Snapshot,
    skeleton_fingerprint: str,
) -> None:
    try:
        motion = load_motion(motion_snapshot.path)
    except Exception as exc:
        raise ReleaseError(f"browser motion {semantic_id} is invalid: {exc}") from exc
    if motion.sha256 != motion_snapshot.sha256:
        raise ReleaseError(f"browser motion {semantic_id} changed while parsed")
    provenance = _object(
        motion.raw.get("browser_export_provenance"),
        f"{semantic_id} browser_export_provenance",
    )
    timing = _object(
        provenance.get("timing_contract"), f"{semantic_id} timing_contract"
    )
    if (
        motion.raw.get("semantic_action_id") != semantic_id
        or motion.frame_count != frame_profile
        or motion.fps != OUTPUT_FPS
        or motion.loop is not loop
        or motion.armature_name != armature_name
        or set(motion.bone_names) != set(bone_order)
        or motion.parent_by_bone != {name: bones[name].parent for name in bone_order}
        or provenance.get("browser_only") is not True
        or provenance.get("blender_used") is not False
        or provenance.get("fitting_performed") is not False
        or provenance.get("candidate_id") != approval.candidate_id
        or provenance.get("candidate_bundle_sha256") != approval.candidate_bundle_sha256
        or provenance.get("human_review_sha256") != approval.human_review_sha256
        or provenance.get("source_asset_identity_sha256")
        != server.browser_asset_identity_sha256
        or provenance.get("skeleton_fingerprint_sha256") != skeleton_fingerprint
        or timing.get("output_fps") != OUTPUT_FPS
        or timing.get("frame_profile") != frame_profile
        or timing.get("loop") is not loop
    ):
        raise ReleaseError(
            f"browser motion provenance/timing mismatch for {semantic_id}"
        )
    _same_descriptor(
        provenance.get("skeleton"), server.skeleton, f"{semantic_id} skeleton"
    )
    _same_descriptor(
        provenance.get("glb_package_input"),
        package_input,
        f"{semantic_id} package input",
    )
    _approval_descriptor_matches(
        provenance.get("approval"), approval, f"{semantic_id} approval"
    )
    if loop:
        first = motion.frames[0]
        last = motion.frames[-1]
        for name in bone_order:
            left = first.bones[name].local_matrix
            right = last.bones[name].local_matrix
            position_error = float(np.linalg.norm(left[:3, 3] - right[:3, 3]))
            rotation_error = _rotation_distance(left, right)
            if position_error > LOOP_POSITION_CLOSURE_TOLERANCE:
                raise ReleaseError(
                    f"loop position closure failed for {semantic_id}.{name}"
                )
            if rotation_error > LOOP_ROTATION_CLOSURE_RADIANS:
                raise ReleaseError(
                    f"loop rotation closure failed for {semantic_id}.{name}"
                )


def _validate_bridge_result(
    bridge: Snapshot,
    *,
    taxonomy: Snapshot,
    package_input: Snapshot,
    package: Any,
    approvals: Sequence[ApprovalEvidence],
    server: ServerConfig,
    clips: Sequence[ApprovedMotion],
    skeleton_fingerprint: str,
    identity: Mapping[str, str],
) -> None:
    value = _json_checked(bridge, "browser motion bridge result", canonical=True)
    expected_identity = {
        "schema": BRIDGE_RESULT_SCHEMA,
        "library_revision": identity["library_revision"],
        "rig_type": identity["rig_type"],
        "orientation": identity["orientation"],
        "template_skeleton_sha256": server.skeleton.sha256,
        "browser_only": True,
        "blender_used": False,
        "fitting_performed": False,
        "clip_count": 30,
        "skeleton_fingerprint_sha256": skeleton_fingerprint,
    }
    for key, expected in expected_identity.items():
        if value.get(key) != expected:
            raise ReleaseError(f"browser motion bridge result {key} mismatch")
    _same_descriptor(value.get("taxonomy"), taxonomy, "bridge taxonomy")
    _same_descriptor(
        value.get("glb_package_input"), package_input, "bridge package input"
    )
    _same_descriptor(value.get("source_glb"), server.task_model, "bridge source GLB")
    _same_descriptor(value.get("skeleton"), server.skeleton, "bridge skeleton")
    source_asset = _object(value.get("source_asset"), "bridge source_asset")
    if (
        source_asset.get("identity_sha256") != server.browser_asset_identity_sha256
        or source_asset.get("source_task")
        != {"id": server.task_id, "guid": server.task_guid}
        or source_asset.get("task_model") != server.task_model.filename_pin()
        or source_asset.get("task_skeleton") != server.skeleton.filename_pin()
    ):
        raise ReleaseError("bridge source asset differs from server-owned source")
    rows = value.get("clips")
    if not isinstance(rows, list) or len(rows) != 30:
        raise ReleaseError("browser motion bridge must contain exactly 30 clips")
    for index, (raw, motion, approval, package_clip) in enumerate(
        zip(rows, clips, approvals, package.clips, strict=True)
    ):
        row = _object(raw, f"bridge clips[{index}]")
        if (
            row.get("order") != index + 1
            or row.get("semantic_id") != motion.semantic_id
            or row.get("frame_profile") != motion.frame_profile
            or row.get("output_fps") != OUTPUT_FPS
            or row.get("loop") is not motion.loop
        ):
            raise ReleaseError("browser motion bridge taxonomy/timing order mismatch")
        _same_descriptor(
            row.get("motion"), motion.motion, f"bridge {motion.semantic_id} motion"
        )
        _approval_descriptor_matches(
            row.get("approval"), approval, f"bridge {motion.semantic_id} approval"
        )
        if package_clip.source.sha256 != approval.browser_clip.sha256:
            raise ReleaseError(
                f"bridge/package candidate mismatch for {motion.semantic_id}"
            )


def _parse_multi_glb(
    snapshot: Snapshot,
    *,
    clip_ids: tuple[str, ...],
    armature_name: str,
    armature_world: np.ndarray,
    bones: Mapping[str, Any],
    bone_order: Sequence[str],
    skeleton_fingerprint: str,
) -> Mapping[str, Any]:
    try:
        document, _ = _glb_chunks(snapshot.data, "multi-clip GLB")
        fingerprint = _validate_glb_skeleton(
            document,
            armature_name=armature_name,
            armature_world=armature_world,
            bones=bones,
            bone_order=bone_order,
            field="multi-clip GLB",
        )
    except Exception as exc:
        raise ReleaseError(f"multi-clip GLB validation failed: {exc}") from exc
    animations = document.get("animations")
    if (
        not isinstance(animations, list)
        or tuple(
            row.get("name") if isinstance(row, dict) else None for row in animations
        )
        != clip_ids
    ):
        raise ReleaseError(
            "multi-clip GLB animation inventory/order differs from taxonomy"
        )
    if fingerprint != skeleton_fingerprint:
        raise ReleaseError("multi-clip GLB skeleton fingerprint mismatch")
    return document


def _load_release_inputs(
    path: str | Path,
    expected_sha256: str,
    *,
    server_config: str | Path,
    server_config_sha256: str,
) -> ReleaseInputs:
    server = _load_server_config(server_config, server_config_sha256)
    manifest = _snapshot_checked(path, "release input manifest", maximum=MAX_JSON_BYTES)
    if manifest.sha256 != _sha(expected_sha256, "release input manifest SHA-256"):
        raise ReleaseError("release input manifest SHA-256 mismatch")
    value = _json_checked(manifest, "release input manifest", canonical=True)
    _exact(
        value,
        {
            "schema",
            "taxonomy",
            "library_revision",
            "rig_type",
            "orientation",
            "template_skeleton_sha256",
            "multi_clip_glb_result",
            "browser_motion_bridge_result",
            "clips",
        },
        "release input manifest",
    )
    if value.get("schema") != INPUT_SCHEMA:
        raise ReleaseError(f"release input schema must be {INPUT_SCHEMA}")
    root = manifest.path.parent
    taxonomy = _descriptor(
        root, value["taxonomy"], "release taxonomy", maximum=MAX_JSON_BYTES
    )
    canonical_taxonomy, clip_ids, taxonomy_value = load_animal_taxonomy()
    canonical_snapshot = Snapshot(
        canonical_taxonomy.path,
        canonical_taxonomy.data,
        canonical_taxonomy.size,
        canonical_taxonomy.sha256,
    )
    if (
        taxonomy.path != canonical_snapshot.path
        or taxonomy.sha256 != canonical_snapshot.sha256
    ):
        raise ReleaseError("release taxonomy must be the checked-in canonical taxonomy")
    if taxonomy_value.get("output_fps") != OUTPUT_FPS:
        raise ReleaseError(f"canonical taxonomy output_fps must be {OUTPUT_FPS}")
    if tuple(server.approval_job_ids) != clip_ids:
        raise ReleaseError("server approval job order differs from canonical taxonomy")
    library_revision = _string(value["library_revision"], "release library_revision")
    rig_type = _string(value["rig_type"], "release rig_type")
    orientation = _string(value["orientation"], "release orientation")
    skeleton_sha = _sha(value["template_skeleton_sha256"], "template_skeleton_sha256")
    if skeleton_sha != server.skeleton.sha256:
        raise ReleaseError("release/template/server skeleton SHA mismatch")
    target_value = _json_checked(server.target_manifest, "server target manifest")
    if (
        target_value.get("schema") != TARGET_SCHEMA
        or target_value.get("rig_type") != rig_type
        or target_value.get("orientation") != orientation
        or target_value.get("source_asset")
        != {
            "identity_sha256": server.browser_asset_identity_sha256,
            "task_id": server.task_id,
            "task_guid": server.task_guid,
            "task_model_sha256": server.task_model.sha256,
            "task_skeleton_sha256": server.skeleton.sha256,
        }
    ):
        raise ReleaseError(
            "target manifest source asset identity differs from server contract"
        )
    try:
        target_spec = load_target_spec(
            manifest_path=server.target_manifest.path, armature_name=None
        )
        validate_target_source(target_spec, server.source_blend.sha256)
    except Exception as exc:
        raise ReleaseError(f"target/source blend contract is invalid: {exc}") from exc
    armature_name, armature_world, bones, bone_order = _load_skeleton(server.skeleton)
    skeleton_fingerprint = _skeleton_fingerprint(
        armature_name, armature_world, bones, bone_order
    )
    if (
        target_spec.armature_name != armature_name
        or target_spec.bone_names != bone_order
        or target_spec.bone_parents != {name: bones[name].parent for name in bone_order}
    ):
        raise ReleaseError("target manifest hierarchy differs from exact task skeleton")
    approvals = tuple(
        _resolve_final_approval(
            fitting_jobs_root=server.fitting_jobs_root,
            job_id=server.approval_job_ids[semantic_id],
            semantic_id=semantic_id,
        )
        for semantic_id in clip_ids
    )
    for approval in approvals:
        if (
            approval.library_revision != library_revision
            or approval.rig_type != rig_type
            or approval.source_task != {"id": server.task_id, "guid": server.task_guid}
            or approval.task_model_pin != server.task_model.filename_pin()
            or approval.task_skeleton_pin != server.skeleton.filename_pin()
        ):
            raise ReleaseError(
                f"server approval source/library mismatch for {approval.semantic_id}"
            )
    package_result = _descriptor(
        root,
        value["multi_clip_glb_result"],
        "release multi-clip package result",
        allowed_root=root,
        maximum=MAX_JSON_BYTES,
    )
    multi_glb, package_input, package = _validate_package_result(
        package_result,
        root=root,
        taxonomy=taxonomy,
        clip_ids=clip_ids,
        library_revision=library_revision,
        rig_type=rig_type,
        orientation=orientation,
        skeleton_sha256=skeleton_sha,
        approvals=approvals,
        server=server,
    )
    _parse_multi_glb(
        multi_glb,
        clip_ids=clip_ids,
        armature_name=armature_name,
        armature_world=armature_world,
        bones=bones,
        bone_order=bone_order,
        skeleton_fingerprint=skeleton_fingerprint,
    )
    taxonomy_by_id = {row["id"]: row for row in taxonomy_value["clips"]}
    rows = value.get("clips")
    if not isinstance(rows, list) or len(rows) != 30:
        raise ReleaseError("release clips must contain exactly 30 rows")
    approved_motions: list[ApprovedMotion] = []
    seen_paths: set[Path] = set()
    seen_hashes: set[str] = set()
    for index, (raw, semantic_id, approval) in enumerate(
        zip(rows, clip_ids, approvals, strict=True)
    ):
        row = _object(raw, f"release clips[{index}]")
        _exact(row, {"semantic_id", "fitted_motion"}, f"release clips[{index}]")
        if row.get("semantic_id") != semantic_id:
            raise ReleaseError("release clips differ from exact taxonomy order")
        motion = _descriptor(
            root,
            row["fitted_motion"],
            f"release {semantic_id} motion",
            allowed_root=root,
            maximum=MAX_JSON_BYTES,
        )
        if motion.path in seen_paths or motion.sha256 in seen_hashes:
            raise ReleaseError("release motions must have unique paths and bytes")
        seen_paths.add(motion.path)
        seen_hashes.add(motion.sha256)
        profile = _positive_int(
            taxonomy_by_id[semantic_id].get("frame_profile"),
            f"{semantic_id} frame_profile",
        )
        loop = taxonomy_by_id[semantic_id].get("loop")
        if not isinstance(loop, bool):
            raise ReleaseError(f"{semantic_id} loop contract is invalid")
        approved_motions.append(
            ApprovedMotion(semantic_id, motion, profile, loop, approval)
        )
    bridge = _descriptor(
        root,
        value["browser_motion_bridge_result"],
        "release browser motion bridge result",
        allowed_root=root,
        maximum=MAX_JSON_BYTES,
    )
    _validate_bridge_result(
        bridge,
        taxonomy=taxonomy,
        package_input=package_input,
        package=package,
        approvals=approvals,
        server=server,
        clips=approved_motions,
        skeleton_fingerprint=skeleton_fingerprint,
        identity={
            "library_revision": library_revision,
            "rig_type": rig_type,
            "orientation": orientation,
        },
    )
    for clip in approved_motions:
        _validate_motion_contract(
            clip.motion,
            semantic_id=clip.semantic_id,
            frame_profile=clip.frame_profile,
            loop=clip.loop,
            armature_name=armature_name,
            bones=bones,
            bone_order=bone_order,
            approval=clip.approval,
            server=server,
            package_input=package_input,
            skeleton_fingerprint=skeleton_fingerprint,
        )
    # Close JSON/source TOCTOU at the end of the complete trust resolution.
    # The returned objects are valid only if every byte source still equals the
    # read-once snapshot used above.
    immutable_snapshots = [
        (manifest, "release input manifest"),
        (server.manifest, "server export config"),
        (taxonomy, "canonical taxonomy"),
        (server.task_model, "server task model"),
        (server.skeleton, "server task skeleton"),
        (server.source_blend, "server source blend"),
        (server.target_manifest, "server target manifest"),
        (server.blender_executable, "allowlisted Blender"),
        (server.applier, "canonical applier"),
        (server.validator, "canonical validator"),
        (package_result, "multi-clip package result"),
        (package_input, "multi-clip package input"),
        (multi_glb, "multi-clip GLB"),
        (bridge, "browser motion bridge result"),
        *[(clip.motion, f"{clip.semantic_id} motion") for clip in approved_motions],
        *[
            (snapshot, f"{approval.semantic_id} {label}")
            for approval in approvals
            for label, snapshot in (
                ("FINAL selection", approval.selection_receipt),
                ("candidate manifest", approval.candidate_manifest),
                ("human review", approval.human_review_receipt),
                ("package descriptor", approval.package_descriptor),
                ("browser clip", approval.browser_clip),
            )
        ],
    ]
    for snapshot, label in immutable_snapshots:
        _repin(snapshot, label)
    return ReleaseInputs(
        manifest=manifest,
        taxonomy=taxonomy,
        taxonomy_value=taxonomy_value,
        clip_ids=clip_ids,
        library_revision=library_revision,
        rig_type=rig_type,
        orientation=orientation,
        template_skeleton_sha256=skeleton_sha,
        glb_package_result=package_result,
        browser_motion_bridge_result=bridge,
        multi_clip_glb=multi_glb,
        source_blend=server.source_blend,
        target_manifest=server.target_manifest,
        skeleton=server.skeleton,
        armature_name=armature_name,
        armature_world=armature_world,
        bones=bones,
        bone_order=bone_order,
        skeleton_fingerprint_sha256=skeleton_fingerprint,
        approvals=approvals,
        clips=tuple(approved_motions),
        server=server,
    )


def _job_id(inputs: ReleaseInputs, clip: ApprovedMotion) -> str:
    value = {
        "release_input_sha256": inputs.manifest.sha256,
        "server_config_sha256": inputs.server.manifest.sha256,
        "semantic_id": clip.semantic_id,
        "motion_sha256": clip.motion.sha256,
        "candidate_identity_sha256": clip.approval.candidate_identity_sha256,
        "selection_receipt_sha256": clip.approval.selection_receipt.sha256,
        "source_blend_sha256": inputs.source_blend.sha256,
        "target_manifest_sha256": inputs.target_manifest.sha256,
        "blender_sha256": inputs.server.blender_executable.sha256,
        "applier_sha256": inputs.server.applier.sha256,
        "validator_sha256": inputs.server.validator.sha256,
    }
    return hashlib.sha256(_canonical(value, newline=False)).hexdigest()


def _validation_receipt_path(inputs: ReleaseInputs, job_id: str) -> Path:
    return inputs.server.validation_root / f"{job_id}.fbx-validation-receipt.json"


def _plan_body(inputs: ReleaseInputs, work_root: Path) -> dict[str, Any]:
    jobs = []
    for order, clip in enumerate(inputs.clips, 1):
        job_id = _job_id(inputs, clip)
        output_dir = work_root / f"{order:02d}-{clip.semantic_id}-{job_id[:12]}"
        fbx_path = output_dir / f"{clip.semantic_id}.fbx"
        receipt_path = _validation_receipt_path(inputs, job_id)
        export_argv = [
            str(inputs.server.blender_executable.path),
            "--background",
            "--factory-startup",
            "--python",
            str(inputs.server.applier.path),
            "--",
            "--source",
            str(inputs.source_blend.path),
            "--motion",
            str(clip.motion.path),
            "--semantic-action-id",
            clip.semantic_id,
            "--output-dir",
            str(output_dir),
            "--fps",
            str(OUTPUT_FPS),
            "--target-manifest",
            str(inputs.target_manifest.path),
        ]
        validate_argv = [
            str(inputs.server.blender_executable.path),
            "--background",
            "--factory-startup",
            "--python",
            str(inputs.server.validator.path),
            "--",
            "validate-fbx",
            "--server-config",
            str(inputs.server.manifest.path),
            "--server-config-sha256",
            inputs.server.manifest.sha256,
            "--job-id",
            job_id,
            "--semantic-id",
            clip.semantic_id,
            "--motion",
            str(clip.motion.path),
            "--motion-sha256",
            clip.motion.sha256,
            "--fbx",
            str(fbx_path),
        ]
        jobs.append(
            {
                "order": order,
                "semantic_id": clip.semantic_id,
                "job_id": job_id,
                "fitted_motion": clip.motion.descriptor(),
                "approval": clip.approval.result_descriptor(),
                "frame_profile": clip.frame_profile,
                "output_fps": OUTPUT_FPS,
                "loop": clip.loop,
                "output_dir": str(output_dir),
                "expected_manifest": str(
                    output_dir / f"{clip.semantic_id}.animation-manifest.json"
                ),
                "expected_fbx": str(fbx_path),
                "expected_glb": str(output_dir / f"{clip.semantic_id}.glb"),
                "expected_fbx_validation_receipt": str(receipt_path),
                "export_command": {"shell": False, "argv": export_argv},
                "fbx_validation_command": {"shell": False, "argv": validate_argv},
            }
        )
    return {
        "schema": PLAN_SCHEMA,
        "input_manifest": inputs.manifest.descriptor(),
        "server_config": inputs.server.manifest.descriptor(),
        "library_revision": inputs.library_revision,
        "rig_type": inputs.rig_type,
        "orientation": inputs.orientation,
        "template_skeleton_sha256": inputs.template_skeleton_sha256,
        "taxonomy": inputs.taxonomy.descriptor(),
        "multi_clip_glb_result": inputs.glb_package_result.descriptor(),
        "browser_motion_bridge_result": inputs.browser_motion_bridge_result.descriptor(),
        "multi_clip_glb": inputs.multi_clip_glb.descriptor(),
        "source_asset": {
            "browser_identity_sha256": inputs.server.browser_asset_identity_sha256,
            "export_identity_sha256": inputs.server.export_asset_identity_sha256,
            "source_blend": inputs.source_blend.descriptor(),
            "target_manifest": inputs.target_manifest.descriptor(),
            "skeleton": inputs.skeleton.descriptor(),
            "skeleton_fingerprint_sha256": inputs.skeleton_fingerprint_sha256,
        },
        "runtime": {
            "blender_id": inputs.server.blender_id,
            "blender_executable": inputs.server.blender_executable.descriptor(),
            "blender_version": inputs.server.blender_version,
            "canonical_applier": inputs.server.applier.descriptor(),
            "canonical_validator": inputs.server.validator.descriptor(),
            "fbx_validation_root": str(inputs.server.validation_root),
        },
        "working_root": str(work_root),
        "jobs": jobs,
    }


def _write_new(path: Path, payload: bytes) -> FilePin:
    parent = path.parent
    if not parent.is_dir():
        raise ReleaseError(f"publication parent does not exist: {parent}")
    try:
        with path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise ReleaseError(f"publication collision: {path}") from exc
    snapshot = _snapshot_checked(path, "new immutable artifact")
    return FilePin(path, snapshot.size, snapshot.sha256)


def build_export_plan(
    *,
    input_manifest: str | Path,
    input_manifest_sha256: str,
    server_config: str | Path,
    server_config_sha256: str,
    working_root: str | Path,
    output_plan: str | Path,
) -> dict[str, Any]:
    inputs = _load_release_inputs(
        input_manifest,
        input_manifest_sha256,
        server_config=server_config,
        server_config_sha256=server_config_sha256,
    )
    root = _secure_root_checked(working_root, "working_root")
    body = _plan_body(inputs, root)
    for job in body["jobs"]:
        if (
            Path(job["output_dir"]).exists()
            or Path(job["expected_fbx_validation_receipt"]).exists()
        ):
            raise ReleaseError(
                f"export/validation output collision for {job['semantic_id']}"
            )
    plan_id = hashlib.sha256(_canonical(body, newline=False)).hexdigest()
    plan = {**body, "plan_id": plan_id}
    pin = _write_new(_lexical_absolute(output_plan), _canonical(plan))
    return {**plan, "plan_manifest": pin.descriptor()}


def _validate_plan(
    path: str | Path, expected_sha256: str
) -> tuple[Snapshot, Mapping[str, Any], ReleaseInputs]:
    plan_pin = _snapshot_checked(path, "export plan", maximum=MAX_JSON_BYTES)
    if plan_pin.sha256 != _sha(expected_sha256, "export plan SHA-256"):
        raise ReleaseError("export plan SHA-256 mismatch")
    plan = _json_checked(plan_pin, "export plan", canonical=True)
    if plan.get("schema") != PLAN_SCHEMA:
        raise ReleaseError(f"export plan schema must be {PLAN_SCHEMA}")
    input_descriptor = _object(plan.get("input_manifest"), "export plan input_manifest")
    config_descriptor = _object(plan.get("server_config"), "export plan server_config")
    input_path = Path(_string(input_descriptor.get("path"), "plan input path"))
    config_path = Path(_string(config_descriptor.get("path"), "plan config path"))
    inputs = _load_release_inputs(
        input_path,
        _sha(input_descriptor.get("sha256"), "plan input SHA"),
        server_config=config_path,
        server_config_sha256=_sha(config_descriptor.get("sha256"), "plan config SHA"),
    )
    _same_descriptor(input_descriptor, inputs.manifest, "plan input_manifest")
    _same_descriptor(config_descriptor, inputs.server.manifest, "plan server_config")
    work_root = _secure_root_checked(plan.get("working_root"), "plan working_root")
    expected_body = _plan_body(inputs, work_root)
    actual_body = {key: value for key, value in plan.items() if key != "plan_id"}
    if actual_body != expected_body:
        raise ReleaseError("export plan differs from deterministic server-owned plan")
    plan_id = hashlib.sha256(_canonical(expected_body, newline=False)).hexdigest()
    if plan.get("plan_id") != plan_id:
        raise ReleaseError("export plan identity is invalid")
    return plan_pin, plan, inputs


def _copy_regular_snapshot(
    source: Path,
    destination: Path,
    *,
    allowed_root: Path,
    field: str,
) -> FilePin:
    try:
        source_path = _secure_file(source, field, root=allowed_root)
    except BridgeError as exc:
        raise ReleaseError(str(exc)) from exc
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    digest = hashlib.sha256()
    total = 0
    try:
        descriptor = os.open(source_path, flags)
        with os.fdopen(descriptor, "rb") as reader, destination.open("xb") as writer:
            before = os.fstat(reader.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
                raise ReleaseError(f"{field} must be a non-empty regular file")
            while block := reader.read(1024 * 1024):
                digest.update(block)
                total += len(block)
                writer.write(block)
            writer.flush()
            os.fsync(writer.fileno())
            after = os.fstat(reader.fileno())
    except FileExistsError as exc:
        raise ReleaseError(f"snapshot collision: {destination}") from exc
    except ReleaseError:
        raise
    except OSError as exc:
        raise ReleaseError(f"{field} snapshot failed: {exc}") from exc
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or total != before.st_size:
        raise ReleaseError(f"{field} changed while snapshotted")
    return FilePin(destination, total, digest.hexdigest())


def _glb_chunks(data: bytes, field: str) -> tuple[Mapping[str, Any], bytes]:
    if len(data) < 20:
        raise ReleaseError(f"{field} has a truncated header")
    magic, version, total = struct.unpack_from("<4sII", data, 0)
    if magic != GLB_MAGIC or version != 2 or total != len(data):
        raise ReleaseError(f"{field} header/length is invalid")
    offset = 12
    document = None
    binary = None
    while offset < len(data):
        if offset + 8 > len(data):
            raise ReleaseError(f"{field} chunk header is truncated")
        length, chunk_type = struct.unpack_from("<II", data, offset)
        offset += 8
        if offset + length > len(data):
            raise ReleaseError(f"{field} chunk length is invalid")
        payload = data[offset : offset + length]
        offset += length
        if chunk_type == GLB_JSON_CHUNK:
            if document is not None or length > MAX_JSON_BYTES:
                raise ReleaseError(f"{field} JSON chunk is duplicate/oversized")
            try:
                document = _object(
                    json.loads(payload.rstrip(b" \t\r\n\x00").decode("utf-8")),
                    f"{field} JSON",
                )
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ReleaseError(f"{field} JSON is invalid: {exc}") from exc
        elif chunk_type == GLB_BIN_CHUNK:
            if binary is not None:
                raise ReleaseError(f"{field} contains duplicate BIN chunks")
            binary = payload
    if document is None or binary is None:
        raise ReleaseError(f"{field} must contain one JSON and one BIN chunk")
    return document, binary


def _accessor_float_scalars(
    document: Mapping[str, Any], binary: bytes, accessor_index: Any, field: str
) -> tuple[float, ...]:
    accessors = document.get("accessors")
    views = document.get("bufferViews")
    if not isinstance(accessors, list) or not isinstance(views, list):
        raise ReleaseError(f"{field} GLB accessors/bufferViews are invalid")
    if (
        isinstance(accessor_index, bool)
        or not isinstance(accessor_index, int)
        or not 0 <= accessor_index < len(accessors)
    ):
        raise ReleaseError(f"{field} accessor index is invalid")
    accessor = _object(accessors[accessor_index], f"{field} accessor")
    if (
        accessor.get("componentType") != FLOAT_COMPONENT_TYPE
        or accessor.get("type") != "SCALAR"
        or accessor.get("sparse") is not None
    ):
        raise ReleaseError(f"{field} time accessor must be non-sparse FLOAT SCALAR")
    count = _positive_int(accessor.get("count"), f"{field} accessor count")
    view_index = accessor.get("bufferView")
    if (
        isinstance(view_index, bool)
        or not isinstance(view_index, int)
        or not 0 <= view_index < len(views)
    ):
        raise ReleaseError(f"{field} bufferView index is invalid")
    view = _object(views[view_index], f"{field} bufferView")
    if view.get("buffer", 0) != 0:
        raise ReleaseError(f"{field} references an external/nonzero buffer")
    stride = view.get("byteStride", 4)
    if stride != 4:
        raise ReleaseError(f"{field} time accessor byteStride must be 4")
    offset = int(view.get("byteOffset", 0)) + int(accessor.get("byteOffset", 0))
    end = offset + count * 4
    if offset < 0 or end > len(binary):
        raise ReleaseError(f"{field} time accessor escapes BIN chunk")
    return struct.unpack_from(f"<{count}f", binary, offset)


def _validate_one_clip_glb(
    snapshot: Snapshot,
    *,
    clip: ApprovedMotion,
    inputs: ReleaseInputs,
) -> dict[str, Any]:
    document, binary = _glb_chunks(snapshot.data, f"{clip.semantic_id} one-clip GLB")
    try:
        fingerprint = _validate_glb_skeleton(
            document,
            armature_name=inputs.armature_name,
            armature_world=inputs.armature_world,
            bones=inputs.bones,
            bone_order=inputs.bone_order,
            field=f"{clip.semantic_id} one-clip GLB",
        )
    except Exception as exc:
        raise ReleaseError(str(exc)) from exc
    if fingerprint != inputs.skeleton_fingerprint_sha256:
        raise ReleaseError(
            f"{clip.semantic_id} one-clip GLB skeleton fingerprint mismatch"
        )
    animations = document.get("animations")
    if not isinstance(animations, list) or len(animations) != 1:
        raise ReleaseError(
            f"{clip.semantic_id} one-clip GLB must contain exactly one animation"
        )
    animation = _object(animations[0], f"{clip.semantic_id} GLB animation")
    if animation.get("name") != clip.semantic_id:
        raise ReleaseError(f"{clip.semantic_id} GLB animation name mismatch")
    samplers = animation.get("samplers")
    channels = animation.get("channels")
    if (
        not isinstance(samplers, list)
        or not samplers
        or not isinstance(channels, list)
        or not channels
    ):
        raise ReleaseError(f"{clip.semantic_id} GLB animation has no samplers/channels")
    expected_times = np.arange(clip.frame_profile, dtype=np.float64) / OUTPUT_FPS
    used_samplers: set[int] = set()
    for channel_index, raw in enumerate(channels):
        channel = _object(raw, f"{clip.semantic_id} channels[{channel_index}]")
        sampler_index = channel.get("sampler")
        if (
            isinstance(sampler_index, bool)
            or not isinstance(sampler_index, int)
            or not 0 <= sampler_index < len(samplers)
        ):
            raise ReleaseError(f"{clip.semantic_id} GLB channel sampler is invalid")
        used_samplers.add(sampler_index)
        target = _object(channel.get("target"), f"{clip.semantic_id} channel target")
        if target.get("path") not in {"translation", "rotation", "scale"}:
            raise ReleaseError(
                f"{clip.semantic_id} GLB has unsupported animation channel"
            )
    if used_samplers != set(range(len(samplers))):
        raise ReleaseError(f"{clip.semantic_id} GLB has unused animation samplers")
    for sampler_index, raw in enumerate(samplers):
        sampler = _object(raw, f"{clip.semantic_id} samplers[{sampler_index}]")
        times = _accessor_float_scalars(
            document,
            binary,
            sampler.get("input"),
            f"{clip.semantic_id} samplers[{sampler_index}]",
        )
        if len(times) != clip.frame_profile or not np.allclose(
            times, expected_times, atol=2e-6, rtol=0
        ):
            raise ReleaseError(
                f"{clip.semantic_id} GLB sampler timing/frame profile mismatch"
            )
    meshes = document.get("meshes")
    if not isinstance(meshes, list) or not meshes or not document.get("skins"):
        raise ReleaseError(f"{clip.semantic_id} one-clip GLB has no mesh/skin")
    for mesh in meshes:
        for primitive in (
            mesh.get("primitives") if isinstance(mesh, dict) else []
        ) or []:
            attributes = (
                primitive.get("attributes") if isinstance(primitive, dict) else {}
            )
            if "JOINTS_1" in attributes or "WEIGHTS_1" in attributes:
                raise ReleaseError(
                    f"{clip.semantic_id} GLB exceeds four skin influences"
                )
    return {
        "sha256": snapshot.sha256,
        "bytes": snapshot.size,
        "animation_name": clip.semantic_id,
        "frame_count": clip.frame_profile,
        "fps": OUTPUT_FPS,
        "duration_seconds": (clip.frame_profile - 1) / OUTPUT_FPS,
        "skeleton_fingerprint_sha256": fingerprint,
    }


def _receipt_identity(value: Mapping[str, Any], field: str) -> str:
    if value.get("schema") != FBX_RECEIPT_SCHEMA:
        raise ReleaseError(f"{field} schema is invalid")
    identity = _sha(value.get("identity_sha256"), f"{field}.identity_sha256")
    unsigned = dict(value)
    unsigned.pop("identity_sha256", None)
    if hashlib.sha256(_canonical(unsigned, newline=False)).hexdigest() != identity:
        raise ReleaseError(f"{field} content identity is invalid")
    return identity


def _validate_fbx_receipt(
    receipt_path: Path,
    *,
    fbx: FilePin,
    clip: ApprovedMotion,
    inputs: ReleaseInputs,
    job_id: str,
) -> Snapshot:
    expected_path = _validation_receipt_path(inputs, job_id)
    if _lexical_absolute(receipt_path) != expected_path:
        raise ReleaseError(f"{clip.semantic_id} FBX receipt path is not server-derived")
    receipt = _snapshot_checked(
        receipt_path,
        f"{clip.semantic_id} trusted FBX receipt",
        root=inputs.server.validation_root,
        maximum=MAX_JSON_BYTES,
    )
    value = _json_checked(
        receipt, f"{clip.semantic_id} trusted FBX receipt", canonical=True
    )
    identity = _receipt_identity(value, f"{clip.semantic_id} trusted FBX receipt")
    producer = _object(
        value.get("producer"), f"{clip.semantic_id} FBX receipt producer"
    )
    take = _object(value.get("take"), f"{clip.semantic_id} FBX receipt take")
    _validate_imported_fbx_identity(
        imported_armature_name=take.get("armature_name"),
        imported_action_names=[take.get("imported_action_name")],
        skeleton_armature_name=inputs.armature_name,
        semantic_id=clip.semantic_id,
    )
    if (
        value.get("job_id") != job_id
        or value.get("semantic_id") != clip.semantic_id
        or value.get("source_asset_identity_sha256")
        != inputs.server.export_asset_identity_sha256
        or value.get("skeleton_fingerprint_sha256")
        != inputs.skeleton_fingerprint_sha256
        or value.get("fbx") != fbx.filename_pin()
        or value.get("motion") != clip.motion.filename_pin()
        or value.get("skeleton") != inputs.skeleton.filename_pin()
        or producer.get("validator") != inputs.server.validator.descriptor()
        or producer.get("blender_executable")
        != inputs.server.blender_executable.descriptor()
        or producer.get("blender_version") != inputs.server.blender_version
        or producer.get("background") is not True
        or take.get("semantic_id") != clip.semantic_id
        or take.get("action_names") != [clip.semantic_id]
        or take.get("armature_name") != inputs.armature_name
        or take.get("bone_names") != list(inputs.bone_order)
        or take.get("bone_parents")
        != {name: inputs.bones[name].parent for name in inputs.bone_order}
        or take.get("frame_count") != clip.frame_profile
        or take.get("fps") != OUTPUT_FPS
        or take.get("loop") is not clip.loop
        or abs(
            _finite(
                take.get("duration_seconds"), f"{clip.semantic_id} receipt duration"
            )
            - (clip.frame_profile - 1) / OUTPUT_FPS
        )
        > 1e-6
        or value.get("validation")
        != {
            "fbx_imported": True,
            "single_semantic_action": True,
            "exact_skeleton_hierarchy": True,
            "exact_timing": True,
        }
        or value.get("identity_sha256") != identity
    ):
        raise ReleaseError(f"{clip.semantic_id} trusted FBX receipt binding mismatch")
    return receipt


def _validate_worker_bundle(
    job: Mapping[str, Any],
    clip: ApprovedMotion,
    inputs: ReleaseInputs,
    *,
    snapshot_root: Path,
    working_root: Path,
) -> tuple[Snapshot, FilePin, Snapshot, FilePin, Snapshot]:
    output_dir = _lexical_absolute(job.get("output_dir"))
    try:
        _secure_root(output_dir, f"{clip.semantic_id} worker output")
        output_dir.relative_to(working_root)
    except (BridgeError, ValueError) as exc:
        raise ReleaseError(
            f"{clip.semantic_id} worker output escapes working_root"
        ) from exc
    names = sorted(path.name for path in output_dir.iterdir())
    expected_names = sorted(
        [
            f"{clip.semantic_id}.blend",
            f"{clip.semantic_id}.fbx",
            f"{clip.semantic_id}.glb",
            f"{clip.semantic_id}.animation-manifest.json",
        ]
    )
    if names != expected_names:
        raise ReleaseError(f"{clip.semantic_id} worker bundle inventory must be exact")
    manifest = _snapshot_checked(
        output_dir / f"{clip.semantic_id}.animation-manifest.json",
        f"{clip.semantic_id} worker manifest",
        root=working_root,
        maximum=MAX_JSON_BYTES,
    )
    value = _json_checked(manifest, f"{clip.semantic_id} worker manifest")
    if (
        value.get("schema") != ASSET_BUNDLE_SCHEMA
        or value.get("semantic_action_id") != clip.semantic_id
    ):
        raise ReleaseError(f"{clip.semantic_id} worker manifest identity is invalid")
    source = _object(value.get("source"), f"{clip.semantic_id} manifest source")
    motion = _object(value.get("motion"), f"{clip.semantic_id} manifest motion")
    target = _object(value.get("target"), f"{clip.semantic_id} manifest target")
    blender = _object(value.get("blender"), f"{clip.semantic_id} manifest blender")
    timing = _object(value.get("timing"), f"{clip.semantic_id} manifest timing")
    action = _object(value.get("action"), f"{clip.semantic_id} manifest action")
    skin = _object(value.get("skin"), f"{clip.semantic_id} manifest skin")
    if (
        source.get("path") != str(inputs.source_blend.path)
        or source.get("sha256") != inputs.source_blend.sha256
        or motion.get("path") != str(clip.motion.path)
        or motion.get("sha256") != clip.motion.sha256
        or motion.get("input_fps") != OUTPUT_FPS
        or target.get("manifest") != str(inputs.target_manifest.path)
        or target.get("manifest_sha256") != inputs.target_manifest.sha256
        or target.get("armature_name") != inputs.armature_name
        or target.get("bone_count") != len(inputs.bone_order)
        or blender.get("version") != inputs.server.blender_version
        or blender.get("background") is not True
        or timing.get("frame_count") != clip.frame_profile
        or timing.get("frame_start") != 0
        or timing.get("frame_end") != clip.frame_profile - 1
        or timing.get("fps") != OUTPUT_FPS
        or timing.get("loop") is not clip.loop
        or abs(
            _finite(
                timing.get("duration_seconds"), f"{clip.semantic_id} timing duration"
            )
            - (clip.frame_profile - 1) / OUTPUT_FPS
        )
        > 1e-6
        or action.get("name") != clip.semantic_id
        or action.get("action_datablock_count") != 1
        or isinstance(skin.get("max_influences"), bool)
        or not isinstance(skin.get("max_influences"), int)
        or not 0 <= skin.get("max_influences") <= 4
        or skin.get("unweighted_vertex_count") != 0
    ):
        raise ReleaseError(f"{clip.semantic_id} worker manifest contract mismatch")
    artifacts = _object(
        value.get("artifacts"), f"{clip.semantic_id} manifest artifacts"
    )
    if set(artifacts) != {"blend", "fbx", "glb"}:
        raise ReleaseError(f"{clip.semantic_id} manifest artifact inventory is invalid")
    destination = snapshot_root / f"{job['order']:02d}-{clip.semantic_id}"
    destination.mkdir()
    fbx = _copy_regular_snapshot(
        output_dir / f"{clip.semantic_id}.fbx",
        destination / f"{clip.semantic_id}.fbx",
        allowed_root=working_root,
        field=f"{clip.semantic_id} FBX",
    )
    glb_pin = _copy_regular_snapshot(
        output_dir / f"{clip.semantic_id}.glb",
        destination / f"{clip.semantic_id}.glb",
        allowed_root=working_root,
        field=f"{clip.semantic_id} GLB",
    )
    blend = _snapshot_checked(
        output_dir / f"{clip.semantic_id}.blend",
        f"{clip.semantic_id} derived blend",
        root=working_root,
    )
    _same_filename_pin(artifacts.get("blend"), blend, f"{clip.semantic_id} blend pin")
    _same_filename_pin(artifacts.get("fbx"), fbx, f"{clip.semantic_id} FBX pin")
    _same_filename_pin(artifacts.get("glb"), glb_pin, f"{clip.semantic_id} GLB pin")
    glb_snapshot = _snapshot_checked(
        glb_pin.path, f"{clip.semantic_id} snapshotted GLB"
    )
    _validate_one_clip_glb(glb_snapshot, clip=clip, inputs=inputs)
    receipt = _validate_fbx_receipt(
        Path(job["expected_fbx_validation_receipt"]),
        fbx=fbx,
        clip=clip,
        inputs=inputs,
        job_id=str(job["job_id"]),
    )
    return manifest, fbx, glb_snapshot, blend, receipt


def _zip_info(name: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.external_attr = 0o100644 << 16
    info.create_system = 3
    return info


def _stream_copy_checked(reader: BinaryIO, writer: BinaryIO, expected: FilePin) -> None:
    digest = hashlib.sha256()
    total = 0
    while block := reader.read(1024 * 1024):
        digest.update(block)
        total += len(block)
        writer.write(block)
    if total != expected.bytes or digest.hexdigest() != expected.sha256:
        raise ReleaseError(f"ZIP source changed while streamed: {expected.path}")


def _write_and_verify_zip(
    path: Path,
    *,
    index_payload: bytes,
    members: Sequence[tuple[str, FilePin]],
) -> FilePin:
    with zipfile.ZipFile(
        path, "x", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as archive:
        archive.writestr(_zip_info("fbx-index.json"), index_payload)
        for name, pin in members:
            with (
                pin.path.open("rb") as reader,
                archive.open(_zip_info(name), "w", force_zip64=True) as writer,
            ):
                _stream_copy_checked(reader, writer, pin)
    expected = {
        "fbx-index.json": (
            len(index_payload),
            hashlib.sha256(index_payload).hexdigest(),
        ),
        **{name: (pin.bytes, pin.sha256) for name, pin in members},
    }
    with zipfile.ZipFile(path, "r") as archive:
        infos = archive.infolist()
        if [info.filename for info in infos] != list(expected) or len(
            {info.filename for info in infos}
        ) != len(infos):
            raise ReleaseError(
                "reopened ZIP member inventory/order differs from contract"
            )
        for info in infos:
            digest = hashlib.sha256()
            total = 0
            with archive.open(info, "r") as stream:
                while block := stream.read(1024 * 1024):
                    digest.update(block)
                    total += len(block)
            if (total, digest.hexdigest()) != expected[info.filename]:
                raise ReleaseError(f"reopened ZIP member bytes differ: {info.filename}")
    snapshot = _snapshot_checked(path, "verified per-clip FBX ZIP")
    return FilePin(path, snapshot.size, snapshot.sha256)


def _portable_prefix(inputs: ReleaseInputs, plan_id: str) -> str:
    revision = re.sub(r"[^a-z0-9._-]+", "-", inputs.library_revision.lower()).strip(
        "-._"
    )
    if not revision:
        raise ReleaseError("library revision cannot form a portable release filename")
    return f"{inputs.rig_type}-{inputs.orientation}-{revision}-{plan_id[:16]}"


def publish_release(
    *,
    plan_manifest: str | Path,
    plan_manifest_sha256: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    plan_pin, plan, inputs = _validate_plan(plan_manifest, plan_manifest_sha256)
    parent = _secure_root_checked(output_dir, "release output_dir")
    working_root = _secure_root_checked(plan["working_root"], "release working_root")
    prefix = _portable_prefix(inputs, str(plan["plan_id"]))
    final_dir = parent / prefix
    if final_dir.exists() or final_dir.is_symlink():
        raise ReleaseError(f"release directory collision: {final_dir}")
    workspace = Path(tempfile.mkdtemp(prefix=f".{prefix}.private-", dir=parent))
    release_stage = workspace / "release"
    snapshots = workspace / "snapshots"
    release_stage.mkdir()
    snapshots.mkdir()
    published = False
    try:
        validated = []
        for job, clip in zip(plan["jobs"], inputs.clips, strict=True):
            validated.append(
                _validate_worker_bundle(
                    job,
                    clip,
                    inputs,
                    snapshot_root=snapshots,
                    working_root=working_root,
                )
            )
        glb_name = f"{prefix}.animations.glb"
        zip_name = f"{prefix}.per-clip-fbx.zip"
        manifest_name = f"{prefix}.release-manifest.json"
        glb_pin = _copy_regular_snapshot(
            inputs.multi_clip_glb.path,
            release_stage / glb_name,
            allowed_root=inputs.manifest.path.parent,
            field="multi-clip GLB publication snapshot",
        )
        if (
            glb_pin.sha256 != inputs.multi_clip_glb.sha256
            or glb_pin.bytes != inputs.multi_clip_glb.size
        ):
            raise ReleaseError("multi-clip GLB changed before publication snapshot")
        rows = []
        members: list[tuple[str, FilePin]] = []
        for order, (clip, bundle) in enumerate(
            zip(inputs.clips, validated, strict=True), 1
        ):
            worker_manifest, fbx, glb, blend, receipt = bundle
            member = f"animations/{order:02d}-{clip.semantic_id}.fbx"
            members.append((member, fbx))
            rows.append(
                {
                    "order": order,
                    "semantic_id": clip.semantic_id,
                    "frame_profile": clip.frame_profile,
                    "output_fps": OUTPUT_FPS,
                    "loop": clip.loop,
                    "candidate_id": clip.approval.candidate_id,
                    "candidate_identity_sha256": clip.approval.candidate_identity_sha256,
                    "candidate_bundle_sha256": clip.approval.candidate_bundle_sha256,
                    "human_review_sha256": clip.approval.human_review_sha256,
                    "selection_receipt_sha256": clip.approval.selection_receipt.sha256,
                    "motion_sha256": clip.motion.sha256,
                    "worker_manifest_sha256": worker_manifest.sha256,
                    "fbx_validation_receipt_sha256": receipt.sha256,
                    "fbx_member": member,
                    "fbx_bytes": fbx.bytes,
                    "fbx_sha256": fbx.sha256,
                    "one_clip_glb_sha256": glb.sha256,
                    "derived_blend_sha256": blend.sha256,
                }
            )
        index = {
            "schema": ZIP_INDEX_SCHEMA,
            "library_revision": inputs.library_revision,
            "rig_type": inputs.rig_type,
            "orientation": inputs.orientation,
            "template_skeleton_sha256": inputs.template_skeleton_sha256,
            "source_asset_identity_sha256": inputs.server.export_asset_identity_sha256,
            "skeleton_fingerprint_sha256": inputs.skeleton_fingerprint_sha256,
            "clip_count": 30,
            "clips": rows,
        }
        index_payload = _canonical(index)
        zip_pin = _write_and_verify_zip(
            release_stage / zip_name, index_payload=index_payload, members=members
        )
        # Reopen all server-controlled roots after worker validation and ZIP
        # creation.  Release visibility is one directory rename, so no partial
        # public unlink rollback is ever required.
        refreshed_inputs = _load_release_inputs(
            inputs.manifest.path,
            inputs.manifest.sha256,
            server_config=inputs.server.manifest.path,
            server_config_sha256=inputs.server.manifest.sha256,
        )
        if (
            refreshed_inputs.server.export_asset_identity_sha256
            != inputs.server.export_asset_identity_sha256
            or refreshed_inputs.multi_clip_glb.sha256 != inputs.multi_clip_glb.sha256
            or tuple(clip.motion.sha256 for clip in refreshed_inputs.clips)
            != tuple(clip.motion.sha256 for clip in inputs.clips)
        ):
            raise ReleaseError("release inputs changed after ZIP construction")
        result = {
            "schema": RESULT_SCHEMA,
            "plan": plan_pin.descriptor(),
            "plan_id": plan["plan_id"],
            "server_config": inputs.server.manifest.descriptor(),
            "library_revision": inputs.library_revision,
            "rig_type": inputs.rig_type,
            "orientation": inputs.orientation,
            "template_skeleton_sha256": inputs.template_skeleton_sha256,
            "source_asset_identity_sha256": inputs.server.export_asset_identity_sha256,
            "skeleton_fingerprint_sha256": inputs.skeleton_fingerprint_sha256,
            "clip_count": 30,
            "clips": rows,
            "artifacts": {
                "multi_clip_glb": {
                    "filename": glb_name,
                    "bytes": glb_pin.bytes,
                    "sha256": glb_pin.sha256,
                },
                "per_clip_fbx_zip": {
                    "filename": zip_name,
                    "bytes": zip_pin.bytes,
                    "sha256": zip_pin.sha256,
                    "index_sha256": hashlib.sha256(index_payload).hexdigest(),
                },
            },
            "publication": {
                "atomic_directory": True,
                "release_directory": prefix,
                "manifest": manifest_name,
                "manifest_written_last_before_atomic_rename": True,
                "partial_publication_possible": False,
            },
        }
        manifest_payload = _canonical(result)
        manifest_pin = _write_new(release_stage / manifest_name, manifest_payload)
        # Verify the staged public inventory and manifest bytes before the one
        # visibility operation.
        if sorted(path.name for path in release_stage.iterdir()) != sorted(
            [glb_name, zip_name, manifest_name]
        ):
            raise ReleaseError("staged release inventory drifted before publication")
        if manifest_pin.sha256 != hashlib.sha256(manifest_payload).hexdigest():
            raise ReleaseError("staged release manifest hash mismatch")
        try:
            os.rename(release_stage, final_dir)
        except FileExistsError as exc:
            raise ReleaseError(f"release directory collision: {final_dir}") from exc
        published = True
        return {
            **result,
            "release_directory": str(final_dir),
            "result_manifest": FilePin(
                final_dir / manifest_name, manifest_pin.bytes, manifest_pin.sha256
            ).descriptor(),
        }
    finally:
        # workspace is dot-prefixed and never the public release path.  Even an
        # unlink failure cannot expose a partial GLB/ZIP/manifest set.
        if workspace.exists():
            shutil.rmtree(workspace, ignore_errors=True)
        if not published and final_dir.exists():
            # The only code path that can create final_dir is the successful
            # atomic rename immediately before published=True.
            raise ReleaseError(
                "unexpected final release directory after failed publication"
            )


def _blender_binary_path(bpy: Any) -> Path:
    path = str(getattr(bpy.app, "binary_path", "") or "").strip()
    if not path:
        raise ReleaseError("Blender did not expose bpy.app.binary_path")
    return Path(path)


def _validate_imported_fbx_identity(
    *,
    imported_armature_name: str,
    imported_action_names: Sequence[str],
    skeleton_armature_name: str,
    semantic_id: str,
) -> str:
    """Require Blender's exact raw or deterministic round-trip take identity."""

    if imported_armature_name != skeleton_armature_name:
        raise ReleaseError(
            "imported FBX armature name differs from the server task skeleton"
        )
    expected_names = {
        semantic_id,
        f"{skeleton_armature_name}|{skeleton_armature_name}|{semantic_id}",
    }
    names = list(imported_action_names)
    if len(names) != 1 or names[0] not in expected_names:
        raise ReleaseError(
            f"imported FBX actions must be exactly one of {sorted(expected_names)!r}, "
            f"got {names!r}"
        )
    return names[0]


def validate_fbx_with_blender(
    *,
    server_config: str | Path,
    server_config_sha256: str,
    job_id: str,
    semantic_id: str,
    motion_path: str | Path,
    motion_sha256: str,
    fbx_path: str | Path,
) -> dict[str, Any]:
    """Trusted worker entrypoint; must run inside allowlisted headless Blender."""

    try:
        import bpy
    except ImportError as exc:  # pragma: no cover - Blender-only boundary
        raise ReleaseError("validate-fbx must run inside Blender") from exc
    server = _load_server_config(server_config, server_config_sha256)
    if not bool(bpy.app.background):
        raise ReleaseError("trusted FBX validation requires background Blender")
    binary = _snapshot_checked(_blender_binary_path(bpy), "running Blender executable")
    if (
        binary.path != server.blender_executable.path
        or binary.sha256 != server.blender_executable.sha256
        or binary.size != server.blender_executable.size
        or bpy.app.version_string != server.blender_version
    ):
        raise ReleaseError(
            "running Blender is not the selected allowlisted executable/version"
        )
    semantic = _string(semantic_id, "semantic_id")
    job = _sha(job_id, "validation job_id")
    motion_snapshot = _snapshot_checked(
        motion_path, f"{semantic} validation motion", maximum=MAX_JSON_BYTES
    )
    if motion_snapshot.sha256 != _sha(motion_sha256, "motion_sha256"):
        raise ReleaseError("validation motion SHA-256 mismatch")
    motion = load_motion(motion_snapshot.path)
    if motion.raw.get("semantic_action_id") != semantic or motion.fps != OUTPUT_FPS:
        raise ReleaseError("validation motion semantic/FPS mismatch")
    skeleton_armature_name, skeleton_armature_world, skeleton_bones, skeleton_order = (
        _load_skeleton(server.skeleton)
    )
    skeleton_parents = {name: skeleton_bones[name].parent for name in skeleton_order}
    if (
        motion.armature_name != skeleton_armature_name
        or set(motion.bone_names) != set(skeleton_order)
        or motion.parent_by_bone != skeleton_parents
    ):
        raise ReleaseError("validation motion differs from the server task skeleton")
    fbx_source = _snapshot_checked(fbx_path, f"{semantic} validation FBX")
    receipt_path = server.validation_root / f"{job}.fbx-validation-receipt.json"
    if receipt_path.exists() or receipt_path.is_symlink():
        raise ReleaseError(f"trusted FBX receipt collision: {receipt_path}")
    with tempfile.TemporaryDirectory(
        prefix=f".{semantic}.fbx-import-", dir=server.validation_root
    ) as temp:
        snapshot_path = Path(temp) / fbx_source.path.name
        snapshot_path.write_bytes(fbx_source.data)
        bpy.ops.wm.read_factory_settings(use_empty=True)
        result = bpy.ops.import_scene.fbx(filepath=str(snapshot_path))
        if "FINISHED" not in result:
            raise ReleaseError(f"Blender FBX import failed: {result}")
        armatures = [obj for obj in bpy.data.objects if obj.type == "ARMATURE"]
        if len(armatures) != 1:
            raise ReleaseError("imported FBX must contain exactly one armature")
        armature = armatures[0]
        actions = list(bpy.data.actions)
        imported_action_name = _validate_imported_fbx_identity(
            imported_armature_name=armature.name,
            imported_action_names=[action.name for action in actions],
            skeleton_armature_name=skeleton_armature_name,
            semantic_id=semantic,
        )
        imported_names = [bone.name for bone in armature.data.bones]
        imported_parents = {
            bone.name: (bone.parent.name if bone.parent else None)
            for bone in armature.data.bones
        }
        if (
            set(imported_names) != set(skeleton_order)
            or imported_parents != skeleton_parents
        ):
            raise ReleaseError(
                "imported FBX skeleton hierarchy differs from motion skeleton"
            )
        action = actions[0]
        start, end = (float(value) for value in action.frame_range)
        frame_count = int(round(end - start)) + 1
        fps = int(bpy.context.scene.render.fps)
        duration = (end - start) / fps
        expected_duration = (motion.frame_count - 1) / OUTPUT_FPS
        if (
            frame_count != motion.frame_count
            or fps != OUTPUT_FPS
            or abs(duration - expected_duration) > 1e-6
        ):
            raise ReleaseError(
                f"imported FBX timing mismatch: frames={frame_count}, fps={fps}, duration={duration}"
            )
    take = {
        "semantic_id": semantic,
        "action_names": [semantic],
        "imported_action_name": imported_action_name,
        "armature_name": skeleton_armature_name,
        "bone_names": list(skeleton_order),
        "bone_parents": skeleton_parents,
        "frame_count": motion.frame_count,
        "fps": OUTPUT_FPS,
        "duration_seconds": (motion.frame_count - 1) / OUTPUT_FPS,
        "loop": motion.loop,
    }
    binding = {
        "schema": FBX_RECEIPT_SCHEMA,
        "job_id": job,
        "semantic_id": semantic,
        "source_asset_identity_sha256": server.export_asset_identity_sha256,
        "skeleton_fingerprint_sha256": _skeleton_fingerprint(
            skeleton_armature_name,
            skeleton_armature_world,
            skeleton_bones,
            skeleton_order,
        ),
        "fbx": fbx_source.filename_pin(),
        "motion": motion_snapshot.filename_pin(),
        "skeleton": server.skeleton.filename_pin(),
        "producer": {
            "validator": server.validator.descriptor(),
            "blender_executable": server.blender_executable.descriptor(),
            "blender_version": server.blender_version,
            "background": True,
        },
        "take": take,
        "validation": {
            "fbx_imported": True,
            "single_semantic_action": True,
            "exact_skeleton_hierarchy": True,
            "exact_timing": True,
        },
    }
    # Use the exact server task skeleton fingerprint, including helper/deform
    # flags and rest matrices; imported FBX independently proved the complete
    # name/parent inventory above.
    identity = hashlib.sha256(_canonical(binding, newline=False)).hexdigest()
    receipt = {**binding, "identity_sha256": identity}
    pin = _write_new(receipt_path, _canonical(receipt))
    return {**receipt, "receipt": pin.descriptor()}


def _args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--input-manifest", required=True)
    plan.add_argument("--input-manifest-sha256", required=True)
    plan.add_argument("--server-config", required=True)
    plan.add_argument("--server-config-sha256", required=True)
    plan.add_argument("--working-root", required=True)
    plan.add_argument("--output-plan", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--plan-manifest", required=True)
    publish.add_argument("--plan-manifest-sha256", required=True)
    publish.add_argument("--output-dir", required=True)
    validate = commands.add_parser("validate-fbx")
    validate.add_argument("--server-config", required=True)
    validate.add_argument("--server-config-sha256", required=True)
    validate.add_argument("--job-id", required=True)
    validate.add_argument("--semantic-id", required=True)
    validate.add_argument("--motion", required=True)
    validate.add_argument("--motion-sha256", required=True)
    validate.add_argument("--fbx", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    if argv is None:
        # Blender retains its own command-line options in sys.argv and places
        # script arguments after the conventional `--` separator.  Normal
        # Python invocations have no separator and continue to use argv[1:].
        raw_argv = (
            sys.argv[sys.argv.index("--") + 1 :] if "--" in sys.argv else sys.argv[1:]
        )
    else:
        raw_argv = list(argv)
    args = _args(raw_argv)
    try:
        if args.command == "plan":
            result = build_export_plan(
                input_manifest=args.input_manifest,
                input_manifest_sha256=args.input_manifest_sha256,
                server_config=args.server_config,
                server_config_sha256=args.server_config_sha256,
                working_root=args.working_root,
                output_plan=args.output_plan,
            )
        elif args.command == "publish":
            result = publish_release(
                plan_manifest=args.plan_manifest,
                plan_manifest_sha256=args.plan_manifest_sha256,
                output_dir=args.output_dir,
            )
        else:
            result = validate_fbx_with_blender(
                server_config=args.server_config,
                server_config_sha256=args.server_config_sha256,
                job_id=args.job_id,
                semantic_id=args.semantic_id,
                motion_path=args.motion,
                motion_sha256=args.motion_sha256,
                fbx_path=args.fbx,
            )
    except Exception as exc:
        print(
            json.dumps(
                {"error_type": type(exc).__name__, "message": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

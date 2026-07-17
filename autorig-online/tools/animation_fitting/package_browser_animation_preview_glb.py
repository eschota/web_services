"""Build a non-release GLB preview from 1..30 pinned browser-fitted clips.

This is deliberately separate from ``package_browser_animation_glb.py``.  The
production library packager remains fail-closed at exactly 30 FINAL-approved
clips.  This tool is only for reviewing an incomplete animation set in a real
viewer.  It performs no fitting or retargeting and never edits its source GLB.
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import re
import struct
import sys
from typing import Any, Mapping, Sequence

if __package__:
    from .package_browser_animation_glb import (
        ApprovedClipInput, BIN_CHUNK, GLB_MAGIC, JSON_CHUNK, PackageError,
        Snapshot, _build_output, _exact_keys, _joint_nodes, _object,
        _parse_json, _publish_exclusive, _snapshot, _string, _validate_clip,
        _validate_descriptor,
    )
else:  # pragma: no cover - direct script boundary
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from animation_fitting.package_browser_animation_glb import (
        ApprovedClipInput, BIN_CHUNK, GLB_MAGIC, JSON_CHUNK, PackageError,
        Snapshot, _build_output, _exact_keys, _joint_nodes, _object,
        _parse_json, _publish_exclusive, _snapshot, _string, _validate_clip,
        _validate_descriptor,
    )


INPUT_SCHEMA = "autorig.browser-animation-preview-glb-input.v1"
OUTPUT_SCHEMA = "autorig.browser-animation-preview-glb-result.v1"
CLIP_ID_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
SEMANTIC_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
HUMAN_DECISIONS = frozenset({"approved", "pending"})


def _descriptor(snapshot: Snapshot) -> dict[str, Any]:
    return {
        "path": str(snapshot.path),
        "bytes": snapshot.size,
        "sha256": snapshot.sha256,
    }


def _decode_source_allowing_animations(
    snapshot: Snapshot,
) -> tuple[dict[str, Any], bytes, tuple[str, ...]]:
    data = snapshot.data
    if len(data) < 20 or data[:4] != GLB_MAGIC:
        raise PackageError("preview source GLB has an invalid header")
    version, declared_length = struct.unpack_from("<II", data, 4)
    if version != 2 or declared_length != len(data):
        raise PackageError("preview source GLB must be exact glTF 2.0")
    offset = 12
    chunks: list[tuple[bytes, bytes]] = []
    while offset < len(data):
        if offset + 8 > len(data):
            raise PackageError("preview source GLB chunk header is truncated")
        length, chunk_type = struct.unpack_from("<I4s", data, offset)
        offset += 8
        end = offset + length
        if end > len(data):
            raise PackageError("preview source GLB chunk is truncated")
        chunks.append((chunk_type, data[offset:end]))
        offset = end
    if (
        offset != len(data)
        or len(chunks) != 2
        or chunks[0][0] != JSON_CHUNK
        or chunks[1][0] != BIN_CHUNK
    ):
        raise PackageError(
            "preview source GLB must contain one JSON and one embedded BIN chunk"
        )
    try:
        source_gltf = json.loads(
            chunks[0][1].rstrip(b" \t\r\n\x00").decode("utf-8")
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PackageError(f"preview source GLB JSON is invalid: {exc}") from exc
    if not isinstance(source_gltf, dict):
        raise PackageError("preview source GLB JSON root must be an object")
    old_animations = source_gltf.get("animations") or []
    if not isinstance(old_animations, list):
        raise PackageError("preview source GLB animations must be an array")
    old_names = tuple(
        str(item.get("name") or f"animation_{index}")
        if isinstance(item, dict)
        else f"animation_{index}"
        for index, item in enumerate(old_animations)
    )

    # Validate an in-memory actionless copy.  Preview sources may use the
    # standard JOINTS_1/WEIGHTS_1 layout already supported by the viewer; the
    # release packager intentionally retains its stricter four-influence rule.
    # No source file is written or modified.
    actionless = copy.deepcopy(source_gltf)
    actionless["animations"] = []
    binary_bytes = chunks[1][1]
    buffers = actionless.get("buffers")
    if (
        not isinstance(buffers, list)
        or len(buffers) != 1
        or not isinstance(buffers[0], dict)
        or "uri" in buffers[0]
    ):
        raise PackageError("preview source must have one embedded buffer")
    declared = buffers[0].get("byteLength")
    if (
        isinstance(declared, bool)
        or not isinstance(declared, int)
        or declared <= 0
        or declared > len(binary_bytes)
        or len(binary_bytes) - declared > 3
    ):
        raise PackageError("preview source embedded buffer length is invalid")
    if not isinstance(actionless.get("nodes"), list) or not actionless["nodes"]:
        raise PackageError("preview source must contain nodes")
    if not isinstance(actionless.get("skins"), list) or not actionless["skins"]:
        raise PackageError("preview source must contain a skin")
    meshes = actionless.get("meshes")
    if not isinstance(meshes, list) or not meshes:
        raise PackageError("preview source must contain a mesh")
    for mesh_index, mesh in enumerate(meshes):
        mesh = _object(mesh, f"preview source meshes[{mesh_index}]")
        primitives = mesh.get("primitives")
        if not isinstance(primitives, list) or not primitives:
            raise PackageError("preview source meshes must contain primitives")
        for primitive_index, primitive in enumerate(primitives):
            primitive = _object(
                primitive,
                f"preview source meshes[{mesh_index}].primitives[{primitive_index}]",
            )
            attributes = _object(
                primitive.get("attributes"), "preview source primitive attributes"
            )
            if "JOINTS_0" not in attributes or "WEIGHTS_0" not in attributes:
                raise PackageError("preview source mesh is not skinned")
            if ("JOINTS_1" in attributes) != ("WEIGHTS_1" in attributes):
                raise PackageError("preview source secondary skin attributes are unpaired")
    if not isinstance(actionless.get("accessors"), list):
        raise PackageError("preview source accessors must be an array")
    if not isinstance(actionless.get("bufferViews"), list):
        raise PackageError("preview source bufferViews must be an array")
    return actionless, binary_bytes, old_names


def _qa_passes(
    value: Mapping[str, Any], *, semantic_id: str, clip_sha256: str
) -> bool:
    schema = value.get("schema")
    if schema == "autorig.browser-horse-visual-phase-evidence-envelope.v1":
        gate = _object(value.get("visual_phase_gate"), "machine QA visual_phase_gate")
        evidence = _object(value.get("local_evidence"), "machine QA local_evidence")
        deformation = _object(
            evidence.get("target_mesh_deformation_qa"),
            "machine QA target_mesh_deformation_qa",
        )
        camera = _object(gate.get("camera"), "machine QA camera")
        return (
            gate.get("semantic_id") == semantic_id
            and gate.get("fitted_clip_sha256") == clip_sha256
            and camera.get("static") is True
            and evidence.get("browser_only") is True
            and evidence.get("blender_used") is False
            and evidence.get("animation_evaluation") == "Three.AnimationMixer"
            and deformation.get("measured_every_frame") is True
            and deformation.get("passed") is True
        )
    if schema == "autorig.browser-animation-semantic-visual-phase-qa.v2":
        machine = _object(value.get("machineQa"), "machine QA machineQa")
        approvals = _object(value.get("approvals"), "machine QA approvals")
        runtime = _object(value.get("runtime"), "machine QA runtime")
        immutable = _object(value.get("immutableInputs"), "machine QA immutableInputs")
        fitted = _object(
            immutable.get("fittedThreeClip"), "machine QA fittedThreeClip"
        )
        return (
            value.get("semanticId") == semantic_id
            and value.get("status") == "PASS_MACHINE_QA_AWAITING_HUMAN"
            and machine.get("passed") is True
            and approvals.get("readyForHumanReview") is True
            and runtime.get("browserOnly") is True
            and runtime.get("blenderUsed") is False
            and fitted.get("sha256") == clip_sha256
        )
    raise PackageError(f"unsupported machine QA schema: {schema!r}")


def _load_inputs(
    manifest_path: str | Path, expected_manifest_sha256: str
) -> tuple[Snapshot, Snapshot, list[dict[str, Any]]]:
    manifest_pin = _snapshot(manifest_path, "preview input manifest")
    if manifest_pin.sha256 != expected_manifest_sha256:
        raise PackageError("preview input manifest SHA-256 mismatch")
    manifest = _parse_json(manifest_pin, "preview input manifest")
    _exact_keys(manifest, {"schema", "source", "clips"}, "preview input manifest")
    if manifest.get("schema") != INPUT_SCHEMA:
        raise PackageError(f"preview input manifest schema must be {INPUT_SCHEMA}")
    base = manifest_pin.path.parent
    source = _validate_descriptor(base, manifest["source"], "preview source")
    rows = manifest.get("clips")
    if not isinstance(rows, list) or not 1 <= len(rows) <= 30:
        raise PackageError("preview input must contain between 1 and 30 clips")
    result: list[dict[str, Any]] = []
    ids: set[str] = set()
    semantics: set[str] = set()
    paths = {manifest_pin.path, source.path}
    hashes: set[str] = set()
    for index, raw in enumerate(rows):
        field = f"preview input clips[{index}]"
        row = _object(raw, field)
        _exact_keys(
            row,
            {
                "id",
                "semantic_id",
                "path",
                "bytes",
                "sha256",
                "machine_qa",
                "human_decision",
            },
            field,
        )
        clip_id = _string(row.get("id"), f"{field}.id")
        semantic_id = _string(row.get("semantic_id"), f"{field}.semantic_id")
        if not CLIP_ID_RE.fullmatch(clip_id):
            raise PackageError(f"{field}.id is not a canonical preview clip id")
        if not SEMANTIC_ID_RE.fullmatch(semantic_id):
            raise PackageError(f"{field}.semantic_id is not canonical")
        if clip_id in ids or semantic_id in semantics:
            raise PackageError("preview input repeats a clip id or semantic id")
        ids.add(clip_id)
        semantics.add(semantic_id)
        clip = _validate_descriptor(
            base,
            row,
            field,
            extra_keys={"id", "semantic_id", "machine_qa", "human_decision"},
        )
        qa = _validate_descriptor(base, row["machine_qa"], f"{field}.machine_qa")
        if clip.path in paths or qa.path in paths or clip.path == qa.path:
            raise PackageError("preview input repeats or aliases an input path")
        if clip.sha256 in hashes or qa.sha256 in hashes:
            raise PackageError("preview input repeats clip or QA content")
        decision = _string(row.get("human_decision"), f"{field}.human_decision")
        if decision not in HUMAN_DECISIONS:
            raise PackageError("preview human_decision must be approved or pending")
        qa_value = _parse_json(qa, f"{field}.machine_qa")
        if not _qa_passes(
            qa_value, semantic_id=semantic_id, clip_sha256=clip.sha256
        ):
            raise PackageError(f"{field}.machine_qa does not prove a machine PASS")
        paths.update({clip.path, qa.path})
        hashes.update({clip.sha256, qa.sha256})
        result.append(
            {
                "id": clip_id,
                "semantic_id": semantic_id,
                "clip": clip,
                "machine_qa": qa,
                "human_decision": decision,
            }
        )
    return manifest_pin, source, result


def package_browser_animation_preview_glb(
    *,
    input_manifest: str | Path,
    input_manifest_sha256: str,
    output: str | Path,
    result_manifest: str | Path,
) -> dict[str, Any]:
    manifest, source, rows = _load_inputs(
        input_manifest, input_manifest_sha256
    )
    output_path = Path(output).resolve()
    result_path = Path(result_manifest).resolve()
    inputs = {
        manifest.path,
        source.path,
        *(row["clip"].path for row in rows),
        *(row["machine_qa"].path for row in rows),
    }
    if output_path in inputs or result_path in inputs or output_path == result_path:
        raise PackageError("preview output/result collides with an immutable input")
    if output_path.exists() or result_path.exists():
        raise PackageError("preview output/result already exists")
    if not output_path.parent.is_dir() or not result_path.parent.is_dir():
        raise PackageError("preview output/result parent must already exist")
    source_gltf, source_binary, replaced_names = _decode_source_allowing_animations(
        source
    )
    joints = _joint_nodes(source_gltf)
    validated = []
    for row in rows:
        clip = _validate_clip(
            ApprovedClipInput(
                semantic_id=row["id"],
                source=row["clip"],
                candidate_id=f"preview:{row['id']}",
                candidate_bundle_sha256=row["clip"].sha256,
                human_review_sha256=row["machine_qa"].sha256,
            ),
            joints,
        )
        validated.append(clip)
    payload = _build_output(source_gltf, source_binary, validated)
    packaged = _publish_exclusive(output_path, payload)
    result = {
        "schema": OUTPUT_SCHEMA,
        "candidate_preview_only": True,
        "release_ready": False,
        "catalog_admission": False,
        "blender_fitting_used": False,
        "input_manifest": _descriptor(manifest),
        "source": _descriptor(source),
        "replaced_source_animations": list(replaced_names),
        "clips": [
            {
                "id": row["id"],
                "semantic_id": row["semantic_id"],
                "human_decision": row["human_decision"],
                "clip": _descriptor(row["clip"]),
                "machine_qa": _descriptor(row["machine_qa"]),
                "duration_seconds": clip.duration,
                "track_count": len(clip.tracks),
            }
            for row, clip in zip(rows, validated)
        ],
        "animation_count": len(validated),
        "source_bin_prefix_bytes": len(source_binary),
        "output": _descriptor(packaged),
    }
    payload_json = (
        json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    result_pin = _publish_exclusive(result_path, payload_json)
    return {**result, "result_manifest": _descriptor(result_pin)}


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a non-release GLB preview from 1..30 browser clips."
    )
    parser.add_argument("--input-manifest", required=True)
    parser.add_argument("--input-manifest-sha256", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--result-manifest", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parse_args(sys.argv[1:] if argv is None else argv)
        result = package_browser_animation_preview_glb(
            input_manifest=args.input_manifest,
            input_manifest_sha256=args.input_manifest_sha256,
            output=args.output,
            result_manifest=args.result_manifest,
        )
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except PackageError as exc:
        print(json.dumps({"status": "ERROR", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Inject 30 pinned browser Three.js clips into one canonical skinned GLB.

The tool is deliberately an asset packager, not a fitter or retargeter.  One
invocation consumes one already-oriented (front or back) source GLB and never
rotates, relabels, or otherwise edits its rig, mesh, materials, images, or UVs.
"""

from __future__ import annotations

import argparse
import copy
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import struct
import sys
import tempfile
from typing import Any, Mapping, Sequence


INPUT_SCHEMA = "autorig.browser-animation-glb-package-input.v1"
OUTPUT_SCHEMA = "autorig.browser-animation-glb-package-result.v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
REVISION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
TRACK_RE = re.compile(r"^(.+)\.(quaternion|position)$")
GLB_MAGIC = b"glTF"
JSON_CHUNK = b"JSON"
BIN_CHUNK = b"BIN\x00"
FLOAT_COMPONENT_TYPE = 5126
QUATERNION_NORM_TOLERANCE = 1e-3
MOTION_EPSILON = 1e-7
TAXONOMY_PATH = (
    Path(__file__).resolve().parents[2]
    / "backend"
    / "animal_animation_taxonomy.v1.json"
)
TAXONOMY_SCHEMA = "animal-animation-taxonomy.v1"
TAXONOMY_REVISION = "animal-base-30-v1"
SOURCE_PROVENANCE_KEYS = frozenset(
    {"sourceRigType", "sourceOrientation", "templateSkeletonSha256"}
)


class PackageError(RuntimeError):
    """Fail-closed package contract violation."""


@dataclass(frozen=True)
class Snapshot:
    path: Path
    data: bytes
    size: int
    sha256: str

    def descriptor(self) -> dict[str, Any]:
        return {"path": str(self.path), "bytes": self.size, "sha256": self.sha256}


@dataclass(frozen=True)
class ValidatedTrack:
    name: str
    bone_name: str
    target_path: str
    node_index: int
    times: tuple[float, ...]
    values: tuple[float, ...]
    item_size: int


@dataclass(frozen=True)
class ValidatedClip:
    semantic_id: str
    source: Snapshot
    duration: float
    tracks: tuple[ValidatedTrack, ...]
    candidate_id: str
    candidate_bundle_sha256: str
    human_review_sha256: str


@dataclass(frozen=True)
class ApprovedClipInput:
    semantic_id: str
    source: Snapshot
    candidate_id: str
    candidate_bundle_sha256: str
    human_review_sha256: str


@dataclass(frozen=True)
class ValidatedPackageInput:
    manifest_snapshot: Snapshot
    manifest: Mapping[str, Any]
    taxonomy: Snapshot
    clip_ids: tuple[str, ...]
    source: Snapshot
    clips: tuple[ApprovedClipInput, ...]
    library_revision: str
    rig_type: str
    orientation: str
    template_skeleton_sha256: str


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise PackageError(f"{field} must be an object")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(value)
    if actual != expected:
        raise PackageError(
            f"{field} must contain exactly {', '.join(sorted(expected))}"
        )


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise PackageError(f"{field} must be a non-empty NUL-free string")
    return value.strip()


def _sha256(value: Any, field: str) -> str:
    result = _string(value, field)
    if not SHA256_RE.fullmatch(result):
        raise PackageError(f"{field} must be a lowercase SHA-256")
    return result


def _uuid(value: Any, field: str) -> str:
    result = _string(value, field)
    if not UUID_RE.fullmatch(result):
        raise PackageError(f"{field} must be a canonical lowercase UUID")
    return result


def _integer(value: Any, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PackageError(f"{field} must be an integer >= {minimum}")
    return value


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PackageError(f"{field} must be finite")
    result = float(value)
    if not math.isfinite(result):
        raise PackageError(f"{field} must be finite")
    return result


def _snapshot(path_value: str | Path, field: str) -> Snapshot:
    path = Path(path_value).resolve()
    try:
        before = path.stat()
        data = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise PackageError(f"{field} is unavailable at {path}: {exc}") from exc
    if not path.is_file() or not data:
        raise PackageError(f"{field} must be a non-empty file")
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or before.st_size != len(data):
        raise PackageError(f"{field} changed while read")
    return Snapshot(path, data, len(data), hashlib.sha256(data).hexdigest())


def _parse_json(snapshot: Snapshot, field: str) -> Mapping[str, Any]:
    try:
        value = json.loads(snapshot.data.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PackageError(f"{field} is invalid JSON: {exc}") from exc
    return _object(value, field)


def load_animal_taxonomy(
    snapshot: Snapshot | None = None,
) -> tuple[Snapshot, tuple[str, ...], Mapping[str, Any]]:
    pin = snapshot or _snapshot(TAXONOMY_PATH, "checked-in animal animation taxonomy")
    if pin.path != TAXONOMY_PATH.resolve():
        raise PackageError(
            "input taxonomy pin must resolve to the checked-in animal taxonomy"
        )
    value = _parse_json(pin, "animal animation taxonomy")
    if (
        value.get("schema") != TAXONOMY_SCHEMA
        or value.get("revision") != TAXONOMY_REVISION
    ):
        raise PackageError("checked-in animal animation taxonomy schema/revision drift")
    rig_types = value.get("rig_types")
    if (
        not isinstance(rig_types, list)
        or not rig_types
        or any(
            not isinstance(item, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", item)
            for item in rig_types
        )
        or len(rig_types) != len(set(rig_types))
        or "t_pose" in rig_types
    ):
        raise PackageError(
            "animal animation taxonomy rig_types must be unique lowercase non-T-pose ids"
        )
    orientations = value.get("orientations")
    if orientations != ["front", "back"]:
        raise PackageError(
            "animal animation taxonomy orientations must be exactly front, back"
        )
    clips = value.get("clips")
    if not isinstance(clips, list) or len(clips) != 30:
        raise PackageError("animal animation taxonomy must contain exactly 30 clips")
    ids: list[str] = []
    for index, row_value in enumerate(clips):
        row = _object(row_value, f"animal animation taxonomy.clips[{index}]")
        if row.get("order") != index + 1:
            raise PackageError(
                "animal animation taxonomy order fields are not contiguous"
            )
        ids.append(
            _string(row.get("id"), f"animal animation taxonomy.clips[{index}].id")
        )
    if len(set(ids)) != 30 or "default_pose" in ids:
        raise PackageError(
            "animal animation taxonomy clip ids are not 30 unique non-pose ids"
        )
    return pin, tuple(ids), value


def _resolve_path(base: Path, value: Any, field: str) -> Path:
    raw = Path(_string(value, field))
    return (raw if raw.is_absolute() else base / raw).resolve()


def _validate_descriptor(
    base: Path,
    value: Any,
    field: str,
    *,
    extra_keys: set[str] | None = None,
) -> Snapshot:
    pin = _object(value, field)
    keys = {"path", "bytes", "sha256"} | (extra_keys or set())
    _exact_keys(pin, keys, field)
    snapshot = _snapshot(_resolve_path(base, pin["path"], f"{field}.path"), field)
    if snapshot.size != _integer(pin["bytes"], f"{field}.bytes", 1):
        raise PackageError(f"{field} byte count mismatch")
    if snapshot.sha256 != _sha256(pin["sha256"], f"{field}.sha256"):
        raise PackageError(f"{field} SHA-256 mismatch")
    return snapshot


def _float32(value: float, field: str) -> float:
    try:
        result = struct.unpack("<f", struct.pack("<f", value))[0]
    except (OverflowError, struct.error) as exc:
        raise PackageError(f"{field} cannot be represented as float32") from exc
    if not math.isfinite(result):
        raise PackageError(f"{field} cannot be represented as finite float32")
    return result


def _float32_array(values: Sequence[Any], field: str) -> tuple[float, ...]:
    return tuple(
        _float32(_finite(value, f"{field}[{index}]"), f"{field}[{index}]")
        for index, value in enumerate(values)
    )


def _parse_glb(snapshot: Snapshot) -> tuple[dict[str, Any], bytes]:
    data = snapshot.data
    if len(data) < 20 or data[:4] != GLB_MAGIC:
        raise PackageError("source GLB has an invalid header")
    version, declared_length = struct.unpack_from("<II", data, 4)
    if version != 2 or declared_length != len(data):
        raise PackageError(
            "source GLB must be exact glTF 2.0 with a matching declared length"
        )
    offset = 12
    chunks: list[tuple[bytes, bytes]] = []
    while offset < len(data):
        if offset + 8 > len(data):
            raise PackageError("source GLB chunk header is truncated")
        length, chunk_type = struct.unpack_from("<I4s", data, offset)
        offset += 8
        end = offset + length
        if end > len(data):
            raise PackageError("source GLB chunk is truncated")
        chunks.append((chunk_type, data[offset:end]))
        offset = end
    if (
        offset != len(data)
        or len(chunks) != 2
        or chunks[0][0] != JSON_CHUNK
        or chunks[1][0] != BIN_CHUNK
    ):
        raise PackageError(
            "source GLB must contain exactly one JSON chunk followed by one BIN chunk"
        )
    try:
        gltf = json.loads(chunks[0][1].rstrip(b" \t\r\n\x00").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PackageError(f"source GLB JSON chunk is invalid: {exc}") from exc
    if not isinstance(gltf, dict):
        raise PackageError("source GLB JSON root must be an object")
    binary = chunks[1][1]
    buffers = gltf.get("buffers")
    if (
        not isinstance(buffers, list)
        or len(buffers) != 1
        or not isinstance(buffers[0], dict)
    ):
        raise PackageError("source GLB must contain exactly one embedded buffer")
    if "uri" in buffers[0]:
        raise PackageError("source GLB buffer must be embedded")
    declared_binary = _integer(
        buffers[0].get("byteLength"), "source buffers[0].byteLength", 1
    )
    if declared_binary > len(binary) or len(binary) - declared_binary > 3:
        raise PackageError(
            "source BIN chunk does not match buffers[0].byteLength padding"
        )
    if gltf.get("animations") not in (None, []):
        raise PackageError(
            "source GLB must be actionless; existing animations are forbidden"
        )
    if not isinstance(gltf.get("meshes"), list) or not gltf["meshes"]:
        raise PackageError("source GLB must contain a mesh")
    if not isinstance(gltf.get("skins"), list) or not gltf["skins"]:
        raise PackageError("source GLB must contain a skin")
    for mesh_index, mesh_value in enumerate(gltf["meshes"]):
        mesh = _object(mesh_value, f"source meshes[{mesh_index}]")
        primitives = mesh.get("primitives")
        if not isinstance(primitives, list):
            raise PackageError(
                f"source meshes[{mesh_index}].primitives must be an array"
            )
        for primitive_index, primitive_value in enumerate(primitives):
            primitive = _object(
                primitive_value,
                f"source meshes[{mesh_index}].primitives[{primitive_index}]",
            )
            attributes = primitive.get("attributes")
            if isinstance(attributes, dict) and (
                "JOINTS_1" in attributes or "WEIGHTS_1" in attributes
            ):
                raise PackageError(
                    "source GLB exceeds the backend four-influence skin contract"
                )
    return gltf, binary


def _validate_source_provenance(
    gltf: Mapping[str, Any],
    *,
    rig_type: str,
    orientation: str,
    template_skeleton_sha256: str,
) -> None:
    asset = _object(gltf.get("asset"), "source GLB asset")
    extras = _object(asset.get("extras"), "source GLB asset.extras")
    missing = SOURCE_PROVENANCE_KEYS - set(extras)
    if missing:
        raise PackageError(
            "source GLB asset.extras is missing required provenance keys: "
            + ", ".join(sorted(missing))
        )
    expected = {
        "sourceRigType": rig_type,
        "sourceOrientation": orientation,
        "templateSkeletonSha256": template_skeleton_sha256,
    }
    for key, value in expected.items():
        if extras.get(key) != value:
            raise PackageError(
                f"source GLB asset.extras.{key} does not match package input"
            )


def _joint_nodes(gltf: Mapping[str, Any]) -> dict[str, int]:
    nodes = gltf.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise PackageError("source GLB must contain nodes")
    indices: set[int] = set()
    for skin_index, skin_value in enumerate(gltf["skins"]):
        skin = _object(skin_value, f"source skins[{skin_index}]")
        joints = skin.get("joints")
        if not isinstance(joints, list) or not joints:
            raise PackageError(f"source skins[{skin_index}].joints must be non-empty")
        for joint_index, node_index_value in enumerate(joints):
            node_index = _integer(
                node_index_value, f"source skins[{skin_index}].joints[{joint_index}]"
            )
            if node_index >= len(nodes) or not isinstance(nodes[node_index], dict):
                raise PackageError("source skin joint node is out of range")
            indices.add(node_index)
    result: dict[str, int] = {}
    for node_index in sorted(indices):
        name = _string(
            nodes[node_index].get("name"), f"source joint node {node_index}.name"
        )
        if name in result and result[name] != node_index:
            raise PackageError(f"source skin joint name is not unique: {name}")
        result[name] = node_index
    return result


def _track_has_motion(track: ValidatedTrack) -> bool:
    first = track.values[: track.item_size]
    for offset in range(track.item_size, len(track.values), track.item_size):
        current = track.values[offset : offset + track.item_size]
        if track.target_path == "translation":
            if math.dist(first, current) > MOTION_EPSILON:
                return True
        else:
            dot = abs(sum(left * right for left, right in zip(first, current)))
            if 1.0 - min(1.0, dot) > MOTION_EPSILON:
                return True
    return False


def _validate_clip(
    source: ApprovedClipInput, joints: Mapping[str, int]
) -> ValidatedClip:
    snapshot = source.source
    semantic_id = source.semantic_id
    clip = _parse_json(snapshot, f"clip {semantic_id}")
    _string(clip.get("name"), f"clip {semantic_id}.name")
    duration = _finite(clip.get("duration"), f"clip {semantic_id}.duration")
    if duration <= 0:
        raise PackageError(f"clip {semantic_id}.duration must be positive")
    raw_tracks = clip.get("tracks")
    if not isinstance(raw_tracks, list) or not raw_tracks:
        raise PackageError(f"clip {semantic_id}.tracks must be non-empty")
    names: set[str] = set()
    timeline: tuple[float, ...] | None = None
    tracks: list[ValidatedTrack] = []
    for track_index, track_value in enumerate(raw_tracks):
        field = f"clip {semantic_id}.tracks[{track_index}]"
        track = _object(track_value, field)
        allowed = {"name", "type", "times", "values", "interpolation"}
        if not set(track).issubset(allowed) or not {
            "name",
            "type",
            "times",
            "values",
        }.issubset(track):
            raise PackageError(f"{field} has unsupported or missing keys")
        name = _string(track["name"], f"{field}.name")
        if name in names:
            raise PackageError(f"clip {semantic_id} repeats track {name}")
        names.add(name)
        match = TRACK_RE.fullmatch(name)
        if not match:
            raise PackageError(
                f"clip {semantic_id} track {name} has unsupported grammar"
            )
        bone_name, property_name = match.groups()
        if bone_name not in joints:
            raise PackageError(
                f"clip {semantic_id} track {name} does not resolve to one skin joint node"
            )
        expected_type = "quaternion" if property_name == "quaternion" else "vector"
        if track["type"] != expected_type:
            raise PackageError(
                f"clip {semantic_id} track {name} type must be {expected_type}"
            )
        if "interpolation" in track and track["interpolation"] not in (2301, "LINEAR"):
            raise PackageError(
                f"clip {semantic_id} track {name} interpolation must be linear"
            )
        if not isinstance(track["times"], list) or len(track["times"]) < 2:
            raise PackageError(
                f"clip {semantic_id} track {name} needs at least two times"
            )
        times = _float32_array(track["times"], f"clip {semantic_id} track {name}.times")
        if times[0] != 0 or any(right <= left for left, right in zip(times, times[1:])):
            raise PackageError(
                f"clip {semantic_id} track {name} times must be strictly increasing from zero"
            )
        if abs(times[-1] - duration) > max(1e-6, abs(duration) * 1e-6):
            raise PackageError(
                f"clip {semantic_id} track {name} timeline does not end at duration"
            )
        if timeline is not None and times != timeline:
            raise PackageError(
                f"clip {semantic_id} tracks must share one exact float32 timeline"
            )
        timeline = times
        item_size = 4 if property_name == "quaternion" else 3
        if (
            not isinstance(track["values"], list)
            or len(track["values"]) != len(times) * item_size
        ):
            raise PackageError(
                f"clip {semantic_id} track {name} values do not match its timeline"
            )
        values = _float32_array(
            track["values"], f"clip {semantic_id} track {name}.values"
        )
        if property_name == "quaternion":
            for frame_index in range(len(times)):
                quaternion = values[frame_index * 4 : frame_index * 4 + 4]
                norm = math.sqrt(sum(component * component for component in quaternion))
                if abs(norm - 1.0) > QUATERNION_NORM_TOLERANCE:
                    raise PackageError(
                        f"clip {semantic_id} track {name} quaternion {frame_index} is not normalized"
                    )
        tracks.append(
            ValidatedTrack(
                name=name,
                bone_name=bone_name,
                target_path="rotation"
                if property_name == "quaternion"
                else "translation",
                node_index=joints[bone_name],
                times=times,
                values=values,
                item_size=item_size,
            )
        )
    ordered = tuple(sorted(tracks, key=lambda item: item.name))
    if not any(_track_has_motion(track) for track in ordered):
        raise PackageError(f"clip {semantic_id} contains no nonzero animation")
    return ValidatedClip(
        semantic_id,
        snapshot,
        duration,
        ordered,
        source.candidate_id,
        source.candidate_bundle_sha256,
        source.human_review_sha256,
    )


def _load_inputs(
    manifest_path: str | Path,
    expected_manifest_sha256: str,
) -> ValidatedPackageInput:
    manifest_snapshot = _snapshot(manifest_path, "input manifest")
    if manifest_snapshot.sha256 != _sha256(
        expected_manifest_sha256, "input manifest expected SHA-256"
    ):
        raise PackageError("input manifest SHA-256 mismatch")
    manifest = _parse_json(manifest_snapshot, "input manifest")
    _exact_keys(
        manifest,
        {
            "schema",
            "taxonomy",
            "library_revision",
            "rig_type",
            "template_skeleton_sha256",
            "source",
            "clips",
        },
        "input manifest",
    )
    if manifest.get("schema") != INPUT_SCHEMA:
        raise PackageError(f"input manifest schema must be {INPUT_SCHEMA}")
    base = manifest_snapshot.path.parent
    taxonomy = _validate_descriptor(
        base, manifest["taxonomy"], "input manifest.taxonomy"
    )
    taxonomy, clip_ids, taxonomy_value = load_animal_taxonomy(taxonomy)
    library_revision = _string(
        manifest.get("library_revision"), "input manifest.library_revision"
    )
    if not REVISION_RE.fullmatch(library_revision):
        raise PackageError(
            "input manifest.library_revision must be a canonical lowercase revision"
        )
    rig_type = _string(manifest.get("rig_type"), "input manifest.rig_type")
    if rig_type == "t_pose" or rig_type not in set(taxonomy_value["rig_types"]):
        raise PackageError(
            "input manifest.rig_type must belong to the pinned non-T-pose taxonomy"
        )
    template_skeleton_sha256 = _sha256(
        manifest.get("template_skeleton_sha256"),
        "input manifest.template_skeleton_sha256",
    )
    source_value = _object(manifest["source"], "input manifest.source")
    _exact_keys(
        source_value,
        {"path", "bytes", "sha256", "orientation"},
        "input manifest.source",
    )
    orientation = _string(
        source_value["orientation"], "input manifest.source.orientation"
    )
    if orientation not in set(taxonomy_value["orientations"]):
        raise PackageError("input manifest.source.orientation must be front or back")
    source = _validate_descriptor(
        base, source_value, "input manifest.source", extra_keys={"orientation"}
    )
    rows = manifest["clips"]
    if not isinstance(rows, list) or len(rows) != len(clip_ids):
        raise PackageError("input manifest.clips must contain exactly 30 pinned clips")
    clips: list[ApprovedClipInput] = []
    paths = {manifest_snapshot.path, taxonomy.path, source.path}
    semantic_ids: list[str] = []
    clip_hashes: set[str] = set()
    candidate_ids: set[str] = set()
    candidate_bundle_hashes: set[str] = set()
    human_review_hashes: set[str] = set()
    for index, row_value in enumerate(rows):
        field = f"input manifest.clips[{index}]"
        row = _object(row_value, field)
        _exact_keys(
            row,
            {
                "semantic_id",
                "path",
                "bytes",
                "sha256",
                "candidate_id",
                "candidate_bundle_sha256",
                "human_review_sha256",
            },
            field,
        )
        semantic_id = _string(row["semantic_id"], f"{field}.semantic_id")
        semantic_ids.append(semantic_id)
        snapshot = _validate_descriptor(
            base,
            row,
            field,
            extra_keys={
                "semantic_id",
                "candidate_id",
                "candidate_bundle_sha256",
                "human_review_sha256",
            },
        )
        if snapshot.path in paths:
            raise PackageError(f"input manifest repeats an input path: {snapshot.path}")
        if snapshot.sha256 in clip_hashes:
            raise PackageError(
                f"input manifest repeats clip SHA-256/content: {snapshot.sha256}"
            )
        candidate_id = _uuid(row["candidate_id"], f"{field}.candidate_id")
        candidate_bundle_sha256 = _sha256(
            row["candidate_bundle_sha256"], f"{field}.candidate_bundle_sha256"
        )
        human_review_sha256 = _sha256(
            row["human_review_sha256"], f"{field}.human_review_sha256"
        )
        for value, seen, label in (
            (candidate_id, candidate_ids, "candidate_id"),
            (
                candidate_bundle_sha256,
                candidate_bundle_hashes,
                "candidate_bundle_sha256",
            ),
            (human_review_sha256, human_review_hashes, "human_review_sha256"),
        ):
            if value in seen:
                raise PackageError(f"input manifest repeats {label}: {value}")
            seen.add(value)
        paths.add(snapshot.path)
        clip_hashes.add(snapshot.sha256)
        clips.append(
            ApprovedClipInput(
                semantic_id,
                snapshot,
                candidate_id,
                candidate_bundle_sha256,
                human_review_sha256,
            )
        )
    if tuple(semantic_ids) != clip_ids:
        raise PackageError(
            "input manifest clips do not match the exact 30-clip taxonomy order"
        )
    return ValidatedPackageInput(
        manifest_snapshot=manifest_snapshot,
        manifest=manifest,
        taxonomy=taxonomy,
        clip_ids=clip_ids,
        source=source,
        clips=tuple(clips),
        library_revision=library_revision,
        rig_type=rig_type,
        orientation=orientation,
        template_skeleton_sha256=template_skeleton_sha256,
    )


def _append_float_view(
    gltf: dict[str, Any],
    binary: bytearray,
    values: Sequence[float],
    accessor_type: str,
    *,
    minimum: list[float] | None = None,
    maximum: list[float] | None = None,
) -> int:
    while len(binary) % 4:
        binary.append(0)
    offset = len(binary)
    payload = struct.pack(f"<{len(values)}f", *values)
    binary.extend(payload)
    buffer_view_index = len(gltf["bufferViews"])
    gltf["bufferViews"].append(
        {"buffer": 0, "byteOffset": offset, "byteLength": len(payload)}
    )
    component_count = {"SCALAR": 1, "VEC3": 3, "VEC4": 4}[accessor_type]
    accessor: dict[str, Any] = {
        "bufferView": buffer_view_index,
        "byteOffset": 0,
        "componentType": FLOAT_COMPONENT_TYPE,
        "count": len(values) // component_count,
        "type": accessor_type,
    }
    if minimum is not None:
        accessor["min"] = minimum
    if maximum is not None:
        accessor["max"] = maximum
    accessor_index = len(gltf["accessors"])
    gltf["accessors"].append(accessor)
    return accessor_index


def _build_output(
    source_gltf: Mapping[str, Any], source_binary: bytes, clips: Sequence[ValidatedClip]
) -> bytes:
    gltf = copy.deepcopy(source_gltf)
    immutable_top = {
        key: copy.deepcopy(value)
        for key, value in source_gltf.items()
        if key not in {"accessors", "bufferViews", "buffers", "animations"}
    }
    source_accessors = copy.deepcopy(source_gltf.get("accessors", []))
    source_views = copy.deepcopy(source_gltf.get("bufferViews", []))
    if not isinstance(source_accessors, list) or not isinstance(source_views, list):
        raise PackageError("source accessors and bufferViews must be arrays")
    gltf["accessors"] = copy.deepcopy(source_accessors)
    gltf["bufferViews"] = copy.deepcopy(source_views)
    gltf["animations"] = []
    binary = bytearray(source_binary)
    for clip in clips:
        timeline = clip.tracks[0].times
        time_accessor = _append_float_view(
            gltf,
            binary,
            timeline,
            "SCALAR",
            minimum=[timeline[0]],
            maximum=[timeline[-1]],
        )
        samplers: list[dict[str, Any]] = []
        channels: list[dict[str, Any]] = []
        for track in clip.tracks:
            output_accessor = _append_float_view(
                gltf,
                binary,
                track.values,
                "VEC4" if track.item_size == 4 else "VEC3",
            )
            sampler_index = len(samplers)
            samplers.append(
                {
                    "input": time_accessor,
                    "output": output_accessor,
                    "interpolation": "LINEAR",
                }
            )
            channels.append(
                {
                    "sampler": sampler_index,
                    "target": {"node": track.node_index, "path": track.target_path},
                }
            )
        gltf["animations"].append(
            {"name": clip.semantic_id, "samplers": samplers, "channels": channels}
        )
    gltf["buffers"][0]["byteLength"] = len(binary)
    for key, value in immutable_top.items():
        if gltf.get(key) != value:
            raise PackageError(
                f"packager mutated protected source GLB JSON field {key}"
            )
    if gltf["accessors"][: len(source_accessors)] != source_accessors:
        raise PackageError("packager mutated source accessor JSON")
    if gltf["bufferViews"][: len(source_views)] != source_views:
        raise PackageError("packager mutated source bufferView JSON")
    if bytes(binary[: len(source_binary)]) != source_binary:
        raise PackageError("packager mutated the source BIN prefix")
    json_bytes = json.dumps(
        gltf,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    json_bytes += b" " * ((-len(json_bytes)) % 4)
    binary_bytes = bytes(binary)
    binary_bytes += b"\x00" * ((-len(binary_bytes)) % 4)
    total = 12 + 8 + len(json_bytes) + 8 + len(binary_bytes)
    return b"".join(
        (
            struct.pack("<4sII", GLB_MAGIC, 2, total),
            struct.pack("<I4s", len(json_bytes), JSON_CHUNK),
            json_bytes,
            struct.pack("<I4s", len(binary_bytes), BIN_CHUNK),
            binary_bytes,
        )
    )


def _publish_exclusive(output_value: str | Path, payload: bytes) -> Snapshot:
    output = Path(output_value).resolve()
    if not output.parent.is_dir():
        raise PackageError(f"output parent must exist: {output.parent}")
    if output.exists():
        raise PackageError(f"output collision: {output}")
    descriptor, staging_name = tempfile.mkstemp(
        prefix=f".{output.name}.staging-", dir=output.parent
    )
    staging = Path(staging_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(staging, output)
        except FileExistsError as exc:
            raise PackageError(f"output collision: {output}") from exc
    finally:
        staging.unlink(missing_ok=True)
    return _snapshot(output, "packaged GLB")


def package_browser_animation_glb(
    *,
    input_manifest: str | Path,
    input_manifest_sha256: str,
    output: str | Path,
    result_manifest: str | Path,
) -> dict[str, Any]:
    inputs = _load_inputs(
        input_manifest,
        input_manifest_sha256,
    )
    output_path = Path(output).resolve()
    result_path = Path(result_manifest).resolve()
    all_inputs = {
        inputs.manifest_snapshot.path,
        inputs.taxonomy.path,
        inputs.source.path,
        *(clip.source.path for clip in inputs.clips),
    }
    if output_path in all_inputs:
        raise PackageError("output path collides with an immutable input")
    if result_path in all_inputs or result_path == output_path:
        raise PackageError("result manifest path collides with an input or output")
    if output_path.exists():
        raise PackageError(f"output collision: {output_path}")
    if result_path.exists():
        raise PackageError(f"result manifest collision: {result_path}")
    if not result_path.parent.is_dir():
        raise PackageError(f"result manifest parent must exist: {result_path.parent}")
    source_gltf, source_binary = _parse_glb(inputs.source)
    _validate_source_provenance(
        source_gltf,
        rig_type=inputs.rig_type,
        orientation=inputs.orientation,
        template_skeleton_sha256=inputs.template_skeleton_sha256,
    )
    joints = _joint_nodes(source_gltf)
    clips = [_validate_clip(clip, joints) for clip in inputs.clips]
    payload = _build_output(source_gltf, source_binary, clips)
    packaged = _publish_exclusive(output_path, payload)
    result = {
        "schema": OUTPUT_SCHEMA,
        "library_revision": inputs.library_revision,
        "rig_type": inputs.rig_type,
        "orientation": inputs.orientation,
        "template_skeleton_sha256": inputs.template_skeleton_sha256,
        "taxonomy": inputs.taxonomy.descriptor(),
        "source": inputs.source.descriptor(),
        "input_manifest": inputs.manifest_snapshot.descriptor(),
        "clips": [
            {
                "semantic_id": clip.semantic_id,
                **clip.source.descriptor(),
                "duration": clip.duration,
                "track_count": len(clip.tracks),
                "candidate_id": clip.candidate_id,
                "candidate_bundle_sha256": clip.candidate_bundle_sha256,
                "human_review_sha256": clip.human_review_sha256,
            }
            for clip in clips
        ],
        "output": packaged.descriptor(),
        "source_bin_prefix_bytes": len(source_binary),
        "animation_count": len(clips),
    }
    result_payload = (
        json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    result_pin = _publish_exclusive(result_path, result_payload)
    return {**result, "result_manifest": result_pin.descriptor()}


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inject exactly 30 pinned browser clips into one already-oriented canonical skinned GLB.",
    )
    parser.add_argument("--input-manifest", required=True)
    parser.add_argument("--input-manifest-sha256", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--result-manifest", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parse_args(sys.argv[1:] if argv is None else argv)
        result = package_browser_animation_glb(
            input_manifest=args.input_manifest,
            input_manifest_sha256=args.input_manifest_sha256,
            output=args.output,
            result_manifest=args.result_manifest,
        )
        print(
            json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
        return 0
    except PackageError as exc:
        print(
            json.dumps({"status": "ERROR", "error": str(exc)}, sort_keys=True),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""AutoRig cloth manifest v1: presets, builder and a dependency-free validator.

Standard library only, so the server-side runner can check a manifest without
numpy, jsonschema or Blender. The contract is
``autorig-cloth/spec/cloth-manifest-v1.md``; the validator mirrors
``cloth-manifest.v1.schema.json`` and adds the rules the spec states in prose
(unique names, unique bones, capsules need ``to_bone``, JsonUtility shape).

The Blender script loads this file by path (never through a package), so it
must not import anything from the server.
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

FORMAT = "autorig.cloth"
VERSION = 1
GENERATOR = "autorig-regen/0.1.0"
UNITS = "meters"

KINDS = ("hair", "cloth", "tail", "accessory")
CONNECTIONS = ("none", "open", "loop")
SHAPES = ("sphere", "capsule")
BODY_TAG = "body"

# Order matters: it is the order fields are written in.
PRESET_FIELDS = (
    "gravity",
    "damping",
    "stiffness",
    "angle_limit_deg",
    "stretch",
    "connection_stiffness",
    "radius",
    "radius_tip",
    "inertia_move",
    "inertia_rotate",
    "drag",
    "wind",
)
# Fields holding a length (meters). They are scaled with the character.
PRESET_LENGTH_FIELDS = ("radius", "radius_tip")

# Built-in presets every runtime must know (spec, "presets[]"). The values are
# the canonical ones in autorig-cloth/spec/builtin-presets.v1.json, which the
# Unity runtime (BuiltInPresets.cs) matches too; a test keeps the three equal.
# They are inlined because the VPS deploy ships tools/regen/ without
# autorig-cloth/.
#
# Radii are meters for an adult of REFERENCE_HEIGHT_M (the canonical file's
# "about 1.7 m"). preset_entry() multiplies only the length fields by the
# character's height / REFERENCE_HEIGHT_M, and the producer writes every preset
# its groups use, so the file's own preset (which overrides the built-in one of
# the same name) fits the body: exactly the canonical values at 1.7 m, smaller
# radii for a child. The runtime's calibration factor then corrects only the
# import scale.
REFERENCE_HEIGHT_M = 1.7
BUILTIN_PRESETS: dict[str, dict[str, float]] = {
    "hair": {
        "gravity": 1.0, "damping": 0.12, "stiffness": 0.25, "angle_limit_deg": 70.0,
        "stretch": 0.0, "connection_stiffness": 0.0, "radius": 0.015, "radius_tip": 0.008,
        "inertia_move": 0.6, "inertia_rotate": 0.6, "drag": 0.02, "wind": 1.0,
    },
    "hair_stiff": {
        "gravity": 0.5, "damping": 0.2, "stiffness": 0.5, "angle_limit_deg": 30.0,
        "stretch": 0.0, "connection_stiffness": 0.0, "radius": 0.015, "radius_tip": 0.01,
        "inertia_move": 0.4, "inertia_rotate": 0.4, "drag": 0.02, "wind": 0.5,
    },
    "skirt": {
        "gravity": 1.0, "damping": 0.15, "stiffness": 0.3, "angle_limit_deg": 60.0,
        "stretch": 0.05, "connection_stiffness": 0.6, "radius": 0.03, "radius_tip": 0.025,
        "inertia_move": 0.6, "inertia_rotate": 0.5, "drag": 0.03, "wind": 0.7,
    },
    "cape": {
        "gravity": 1.0, "damping": 0.08, "stiffness": 0.08, "angle_limit_deg": 110.0,
        "stretch": 0.03, "connection_stiffness": 0.5, "radius": 0.035, "radius_tip": 0.035,
        "inertia_move": 0.8, "inertia_rotate": 0.75, "drag": 0.05, "wind": 1.0,
    },
    "ribbon": {
        "gravity": 0.8, "damping": 0.06, "stiffness": 0.05, "angle_limit_deg": 0.0,
        "stretch": 0.0, "connection_stiffness": 0.3, "radius": 0.01, "radius_tip": 0.01,
        "inertia_move": 0.85, "inertia_rotate": 0.85, "drag": 0.08, "wind": 1.0,
    },
    "tail": {
        "gravity": 0.4, "damping": 0.18, "stiffness": 0.45, "angle_limit_deg": 45.0,
        "stretch": 0.0, "connection_stiffness": 0.0, "radius": 0.04, "radius_tip": 0.015,
        "inertia_move": 0.5, "inertia_rotate": 0.45, "drag": 0.02, "wind": 0.2,
    },
    "accessory": {
        "gravity": 1.0, "damping": 0.25, "stiffness": 0.35, "angle_limit_deg": 50.0,
        "stretch": 0.0, "connection_stiffness": 0.0, "radius": 0.012, "radius_tip": 0.012,
        "inertia_move": 0.5, "inertia_rotate": 0.5, "drag": 0.02, "wind": 0.3,
    },
}

# Ranges from the schema: (min, max); max None = unbounded.
_PRESET_RANGES: dict[str, tuple[float, float | None]] = {
    "gravity": (0.0, 2.0),
    "damping": (0.0, 1.0),
    "stiffness": (0.0, 1.0),
    "angle_limit_deg": (0.0, 180.0),
    "stretch": (0.0, 1.0),
    "connection_stiffness": (0.0, 1.0),
    "radius": (0.0, None),
    "radius_tip": (0.0, None),
    "inertia_move": (0.0, 1.0),
    "inertia_rotate": (0.0, 1.0),
    "drag": (0.0, 1.0),
    "wind": (0.0, 1.0),
}

_STEM_BAD = re.compile(r"[^A-Za-z0-9._-]+")


# --------------------------------------------------------------------------- naming


def resolve_stem(model_path: str | Path, stem: str | None = None) -> str:
    """The artifact stem: ``--stem`` when given, else the model file's stem.

    Restricted to ``[A-Za-z0-9._-]`` so it is safe in any file system and URL.
    """
    raw = stem if stem else Path(model_path).stem
    cleaned = _STEM_BAD.sub("_", str(raw)).strip("._-")
    return cleaned or "model"


def artifact_paths(out_dir: str | Path, stem: str) -> dict[str, Path]:
    """Every file a successful run leaves in ``out_dir``.

    ``manifest`` is ``<stem>.autorig-cloth.json`` (what the Regen backend
    reads). ``manifest_alias`` is the same document named after the exported
    model (``<stem>_cloth.autorig-cloth.json``) so a runtime that follows the
    spec's discovery rule -- manifest next to the model, same stem -- finds it
    beside ``<stem>_cloth.fbx``/``.glb``.
    """
    out = Path(out_dir)
    return {
        "fbx": out / f"{stem}_cloth.fbx",
        "glb": out / f"{stem}_cloth.glb",
        "manifest": out / f"{stem}.autorig-cloth.json",
        "manifest_alias": out / f"{stem}_cloth.autorig-cloth.json",
        "report": out / "cloth_rig_report.json",
        "error": out / "error.json",
    }


# --------------------------------------------------------------------------- presets


def default_preset_name(kind: str, connection: str) -> str:
    """Built-in preset for a decomposition group that did not name one.

    This is the producer's own choice (cloth by connection). The spec's
    ``kind_fallback`` (cloth -> ``skirt``) is what a runtime uses for a group
    whose ``preset`` is empty, which this producer never writes.
    """
    if kind == "hair":
        return "hair"
    if kind == "tail":
        return "tail"
    if kind == "accessory":
        return "accessory"
    if connection == "loop":
        return "skirt"
    if connection == "open":
        return "cape"
    return "ribbon"


def length_scale_for_height(height_m: float) -> float:
    """Factor applied to built-in preset radii for a character of ``height_m``."""
    if not (isinstance(height_m, (int, float)) and math.isfinite(height_m) and height_m > 0):
        return 1.0
    return min(4.0, max(0.25, float(height_m) / REFERENCE_HEIGHT_M))


def preset_entry(name: str, *, base: str | None = None, length_scale: float = 1.0) -> dict[str, Any]:
    """A complete ``presets[]`` entry (every field present, for JsonUtility)."""
    values = BUILTIN_PRESETS.get(base or name) or BUILTIN_PRESETS["accessory"]
    entry: dict[str, Any] = {"name": name}
    for field in PRESET_FIELDS:
        value = float(values[field])
        if field in PRESET_LENGTH_FIELDS:
            value *= length_scale
        entry[field] = _round(value)
    return entry


# --------------------------------------------------------------------------- builder


def _round(value: float, digits: int = 6) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"non-finite number in manifest: {value!r}")
    rounded = round(value, digits)
    return 0.0 if rounded == 0 else rounded


def build_manifest(
    *,
    calibration: Mapping[str, Any],
    presets: Sequence[Mapping[str, Any]],
    groups: Sequence[Mapping[str, Any]],
    colliders: Sequence[Mapping[str, Any]],
    generator: str = GENERATOR,
) -> dict[str, Any]:
    """Assemble a manifest with every field present and in spec order."""
    doc: dict[str, Any] = {
        "format": FORMAT,
        "version": VERSION,
        "generator": str(generator),
        "units": UNITS,
        "calibration": {
            "bone_a": str(calibration["bone_a"]),
            "bone_b": str(calibration["bone_b"]),
            "distance": _round(calibration["distance"]),
        },
        "presets": [],
        "groups": [],
        "colliders": [],
    }
    for preset in presets:
        entry = {"name": str(preset["name"])}
        missing = [field for field in PRESET_FIELDS if field not in preset]
        if missing:  # never invent values: a reader cannot tell a missing number from 0
            raise ValueError(f"preset {entry['name']!r} lacks {missing}; every preset field is required")
        for field in PRESET_FIELDS:
            entry[field] = _round(preset[field])
        doc["presets"].append(entry)
    for group in groups:
        doc["groups"].append(
            {
                "name": str(group["name"]),
                "kind": str(group["kind"]),
                "attach_bone": str(group["attach_bone"]),
                "preset": str(group["preset"]),
                "connection": str(group["connection"]),
                "collider_tags": [str(tag) for tag in group.get("collider_tags", [BODY_TAG])],
                "chains": [{"bones": [str(b) for b in chain["bones"]]} for chain in group["chains"]],
            }
        )
    for collider in colliders:
        doc["colliders"].append(
            {
                "name": str(collider["name"]),
                "tag": str(collider.get("tag", BODY_TAG)),
                "shape": str(collider["shape"]),
                "bone": str(collider["bone"]),
                "to_bone": str(collider.get("to_bone", "") or ""),
                "t": _round(collider.get("t", 0.0)),
                "radius": _round(collider["radius"]),
                "radius_to": _round(collider.get("radius_to", 0.0)),
            }
        )
    return doc


# --------------------------------------------------------------------------- validator


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and len(value) > 0


def _json_utility_problems(value: Any, path: str, out: list[str]) -> None:
    """JsonUtility cannot read arrays of arrays or nulls."""
    if value is None:
        out.append(f"{path}: null is not allowed (JsonUtility)")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            if isinstance(item, list):
                out.append(f"{path}[{index}]: arrays of arrays are not allowed (JsonUtility)")
            _json_utility_problems(item, f"{path}[{index}]", out)
    elif isinstance(value, dict):
        for key, item in value.items():
            _json_utility_problems(item, f"{path}.{key}", out)


def validate_manifest(doc: Any) -> list[str]:
    """Return every contract violation found; an empty list means valid."""
    errors: list[str] = []
    if not isinstance(doc, dict):
        return ["manifest must be a JSON object"]
    if doc.get("format") != FORMAT:
        errors.append(f"format must be {FORMAT!r}")
    if doc.get("version") != VERSION or isinstance(doc.get("version"), bool):
        errors.append(f"version must be {VERSION}")
    if "generator" in doc and not isinstance(doc["generator"], str):
        errors.append("generator must be a string")
    if doc.get("units") != UNITS:
        errors.append(f"units must be {UNITS!r}")
    for key in ("calibration", "presets", "groups", "colliders"):
        if key not in doc:
            errors.append(f"missing required field {key!r}")

    calibration = doc.get("calibration")
    if isinstance(calibration, dict):
        for key in ("bone_a", "bone_b"):
            if not _nonempty_str(calibration.get(key)):
                errors.append(f"calibration.{key} must be a non-empty string")
        distance = calibration.get("distance")
        if not (_is_number(distance) and distance > 0):
            errors.append("calibration.distance must be a number > 0")
    elif "calibration" in doc:
        errors.append("calibration must be an object")

    preset_names: set[str] = set()
    presets = doc.get("presets")
    if isinstance(presets, list):
        for index, preset in enumerate(presets):
            where = f"presets[{index}]"
            if not isinstance(preset, dict):
                errors.append(f"{where} must be an object")
                continue
            name = preset.get("name")
            if not _nonempty_str(name):
                errors.append(f"{where}.name must be a non-empty string")
            elif name in preset_names:
                errors.append(f"{where}.name {name!r} is not unique")
            else:
                preset_names.add(name)
            for field in PRESET_FIELDS:
                if field not in preset:  # schema: every field is required (JsonUtility reads it as 0)
                    errors.append(f"{where}.{field} is missing; every preset field is required")
                    continue
                low, high = _PRESET_RANGES[field]
                value = preset[field]
                if not _is_number(value) or value < low or (high is not None and value > high):
                    bound = f"[{low}, {high}]" if high is not None else f">= {low}"
                    errors.append(f"{where}.{field} must be a number in {bound}")
    elif "presets" in doc:
        errors.append("presets must be an array")

    seen_bones: dict[str, str] = {}
    groups = doc.get("groups")
    group_names: set[str] = set()
    if isinstance(groups, list):
        for index, group in enumerate(groups):
            where = f"groups[{index}]"
            if not isinstance(group, dict):
                errors.append(f"{where} must be an object")
                continue
            name = group.get("name")
            if not _nonempty_str(name):
                errors.append(f"{where}.name must be a non-empty string")
            elif name in group_names:
                errors.append(f"{where}.name {name!r} is not unique")
            else:
                group_names.add(name)
            if group.get("kind") not in KINDS:
                errors.append(f"{where}.kind must be one of {KINDS}")
            if not _nonempty_str(group.get("attach_bone")):
                errors.append(f"{where}.attach_bone must be a non-empty string")
            if not _nonempty_str(group.get("preset")):
                errors.append(f"{where}.preset must be a non-empty string")
            if group.get("connection") not in CONNECTIONS:
                errors.append(f"{where}.connection must be one of {CONNECTIONS}")
            tags = group.get("collider_tags", [])
            if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
                errors.append(f"{where}.collider_tags must be an array of strings")
            chains = group.get("chains")
            if not isinstance(chains, list) or not chains:
                errors.append(f"{where}.chains must be a non-empty array")
                continue
            for chain_index, chain in enumerate(chains):
                cwhere = f"{where}.chains[{chain_index}]"
                bones = chain.get("bones") if isinstance(chain, dict) else None
                if not isinstance(bones, list) or len(bones) < 2:
                    errors.append(f"{cwhere}.bones must list at least 2 bones (root .. end joint)")
                    continue
                for bone in bones:
                    if not _nonempty_str(bone):
                        errors.append(f"{cwhere}.bones must hold non-empty strings")
                        continue
                    if bone in seen_bones:
                        errors.append(f"{cwhere}: bone {bone!r} already used by {seen_bones[bone]}")
                    else:
                        seen_bones[bone] = cwhere
                    if bone == group.get("attach_bone"):
                        errors.append(f"{cwhere}: attach bone {bone!r} cannot be a chain bone")
    elif "groups" in doc:
        errors.append("groups must be an array")

    colliders = doc.get("colliders")
    collider_names: set[str] = set()
    if isinstance(colliders, list):
        for index, collider in enumerate(colliders):
            where = f"colliders[{index}]"
            if not isinstance(collider, dict):
                errors.append(f"{where} must be an object")
                continue
            name = collider.get("name")
            if not _nonempty_str(name):
                errors.append(f"{where}.name must be a non-empty string")
            elif name in collider_names:
                errors.append(f"{where}.name {name!r} is not unique")
            else:
                collider_names.add(name)
            if "tag" in collider and not isinstance(collider["tag"], str):
                errors.append(f"{where}.tag must be a string")
            shape = collider.get("shape")
            if shape not in SHAPES:
                errors.append(f"{where}.shape must be one of {SHAPES}")
            if not _nonempty_str(collider.get("bone")):
                errors.append(f"{where}.bone must be a non-empty string")
            to_bone = collider.get("to_bone", "")
            if not isinstance(to_bone, str):
                errors.append(f"{where}.to_bone must be a string")
            elif shape == "capsule" and not to_bone:
                errors.append(f"{where}: a capsule needs to_bone")
            t = collider.get("t", 0.0)
            if not (_is_number(t) and 0.0 <= t <= 1.0):
                errors.append(f"{where}.t must be a number in [0, 1]")
            radius = collider.get("radius")
            if not (_is_number(radius) and radius > 0):
                errors.append(f"{where}.radius must be a number > 0")
            radius_to = collider.get("radius_to", 0.0)
            if not (_is_number(radius_to) and radius_to >= 0):
                errors.append(f"{where}.radius_to must be a number >= 0")
    elif "colliders" in doc:
        errors.append("colliders must be an array")

    _json_utility_problems(doc, "$", errors)
    return errors


def manifest_bone_names(doc: Mapping[str, Any]) -> set[str]:
    """Every bone name the manifest refers to (calibration, groups, colliders)."""
    names: set[str] = set()
    calibration = doc.get("calibration") or {}
    for key in ("bone_a", "bone_b"):
        if calibration.get(key):
            names.add(calibration[key])
    for group in doc.get("groups") or []:
        if group.get("attach_bone"):
            names.add(group["attach_bone"])
        for chain in group.get("chains") or []:
            names.update(bone for bone in chain.get("bones") or [] if bone)
    for collider in doc.get("colliders") or []:
        for key in ("bone", "to_bone"):
            if collider.get(key):
                names.add(collider[key])
    return names


def chain_bone_names(doc: Mapping[str, Any]) -> list[str]:
    """Chain bones in manifest order (root .. end joint, chain by chain)."""
    out: list[str] = []
    for group in doc.get("groups") or []:
        for chain in group.get("chains") or []:
            out.extend(chain.get("bones") or [])
    return out


def strip_namespace(name: str) -> str:
    """Runtime resolution fallback: ``mixamorig:Hips``/``Armature|Hips`` -> ``Hips``."""
    return re.split(r"[:|]", name)[-1]


def resolve_bone(name: str, available: Iterable[str]) -> str | None:
    """Resolve like a runtime does: exact name, then namespace-stripped name.

    Pass ``available`` in depth-first hierarchy order: when several bones match
    after stripping, the first one wins. An empty stripped name never matches.
    """
    pool = list(available)
    if name in pool:
        return name
    stripped = strip_namespace(name)
    if not stripped:
        return None
    for candidate in pool:
        if strip_namespace(candidate) == stripped:
            return candidate
    return None

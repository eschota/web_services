"""Strict, opt-in semantic validation for quadruped clip v2.

Importing this module does not enable v2 consumers.
"""
from dataclasses import dataclass
from types import MappingProxyType
import re
import numbers
import numpy as np

V1_CLIP_SCHEMA = "autorig-authored-quadruped-clip.v1"
V2_CLIP_SCHEMA = "autorig-authored-quadruped-clip.v2"
V1_EXPORT_REPORT_SCHEMA = "autorig-quadruped-export-candidate.v1"
FEET = ("hind_left", "fore_left", "hind_right", "fore_right")
_SAFE_ACTION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_SAFE_BONE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")
V2_SEMANTIC_FIELDS = frozenset(("playback", "motion", "reference_actor_motion",
                               "ground", "phases", "entry_contacts", "events"))

@dataclass(frozen=True)
class ValidatedClipV2:
    action: str
    sample_count: int
    times: np.ndarray
    actor_translation: np.ndarray
    pose_root_offsets: np.ndarray
    contacts: object
    targets: object
    sole_anchors: object
    phases: tuple
    events: tuple
    pose_root: str
    playback_mode: str
    seam_policy: str
    ground_height: float
    ground_tolerance: float

def require_v1_clip(clip):
    if not isinstance(clip, dict) or clip.get("schema") != V1_CLIP_SCHEMA:
        raise ValueError("clip must use the exact quadruped v1 schema")
    if V2_SEMANTIC_FIELDS.intersection(clip):
        raise ValueError("v2 semantic fields cannot be interpreted by a v1 clip consumer")
    return clip

def require_v1_export_report(report):
    if not isinstance(report, dict) or report.get("schema") != V1_EXPORT_REPORT_SCHEMA:
        raise ValueError("report must use the exact quadruped export candidate v1 schema")
    rows = report.get("clips", [])
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("legacy export clips must be a list of clip rows")
    if V2_SEMANTIC_FIELDS.intersection(report) or any(
            V2_SEMANTIC_FIELDS.intersection(row) for row in rows):
        raise ValueError("v2 semantic fields cannot be interpreted by a v1 export consumer")
    return report

def _number(value, label):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, numbers.Real):
        raise ValueError(f"{label} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be a finite number") from exc
    if not np.isfinite(result):
        raise ValueError(f"{label} must be a finite number")
    return result

def _array(value, shape, label):
    try:
        def contains_bool(item):
            if isinstance(item, (bool, np.bool_)):
                return True
            if isinstance(item, np.ndarray):
                if item.dtype.kind == 'b': return True
                if item.dtype.kind != 'O': return False
                return any(contains_bool(x) for x in item.flat)
            return isinstance(item, (list, tuple)) and any(contains_bool(x) for x in item)
        if contains_bool(value):
            raise ValueError
        raw = np.asarray(value)
        if raw.dtype.kind == "b" or raw.dtype.kind not in "iuf":
            raise ValueError
        result = np.array(raw, dtype=np.float64, copy=True)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be a finite numeric array") from exc
    if result.shape != shape or not np.all(np.isfinite(result)):
        raise ValueError(f"{label} must have shape {shape} and finite values")
    result.setflags(write=False)
    return result

def _exact_int(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise ValueError(f"{label} must be an integer")
    return int(value)

def validate_v2_clip(clip, blueprint, *, max_ground_tolerance_m=0.006):
    if not isinstance(clip, dict) or clip.get("schema") != V2_CLIP_SCHEMA:
        raise ValueError("unsupported clip schema")
    if not isinstance(blueprint, dict) or blueprint.get("schema") != "autorig-quadruped-authoring-rig.v1" or not isinstance(blueprint.get("bones"), list):
        raise ValueError("blueprint bones are required")
    action = clip.get("action")
    if not isinstance(action, str) or not _SAFE_ACTION.fullmatch(action):
        raise ValueError("unsafe action id")
    frames = clip.get("frames")
    if not isinstance(frames, list) or not 2 <= len(frames) <= 3601 or not all(isinstance(x, dict) for x in frames):
        raise ValueError("2..3601 frames are required")
    count = len(frames)
    timing = clip.get("timing")
    if not isinstance(timing, dict) or set(timing) != {"fps", "sample_count", "interval_count"}:
        raise ValueError("exact timing declaration is required")
    if _number(timing["fps"], "timing.fps") != 30.0:
        raise ValueError("timeline must be 30 FPS")
    if _exact_int(timing["sample_count"], "sample_count") != count or _exact_int(timing["interval_count"], "interval_count") != count - 1:
        raise ValueError("timing counts do not match frames")
    times = _array([frame.get("time") for frame in frames], (count,), "times")
    if abs(times[0]) > 1e-9 or not np.allclose(np.diff(times), 1 / 30, rtol=0, atol=1e-8):
        raise ValueError("frame times must start at zero and advance at 30 FPS")

    bones = blueprint["bones"]
    if not bones or not all(isinstance(b, dict) and "parent" in b and isinstance(b.get("name"), str) and _SAFE_BONE.fullmatch(b["name"]) for b in bones):
        raise ValueError("blueprint has invalid bone ids")
    bone_names = tuple(b["name"] for b in bones)
    if len(set(bone_names)) != len(bone_names):
        raise ValueError("blueprint bone ids must be unique")
    roots = [b for b in bones if b.get("parent") is None]
    motion = clip.get("motion")
    if not isinstance(motion, dict) or set(motion) != {"world_owner", "pose_root", "pose_space", "baked_actor_translation", "pose_root_offsets"}:
        raise ValueError("exact motion declaration is required")
    root_name = motion["pose_root"]
    root_rest = _array(roots[0].get("rest_local") if roots else None, (16,), "root rest_local").reshape(4, 4) if len(roots) == 1 else None
    if len(roots) != 1 or roots[0]["name"] != root_name or not np.array_equal(_array(roots[0].get("head"), (3,), "root head"), np.zeros(3)) or not np.array_equal(root_rest[:3, 3], np.zeros(3)):
        raise ValueError("pose_root must be the exact unparented zero-origin blueprint root")
    rotation = root_rest[:3, :3]
    if (not np.allclose(root_rest[3], [0, 0, 0, 1], rtol=0, atol=1e-8) or
            not np.allclose(rotation.T @ rotation, np.eye(3), rtol=0, atol=1e-6) or
            abs(np.linalg.det(rotation) - 1) > 1e-6):
        raise ValueError("pose_root rest_local must be a rigid affine transform")
    if motion["world_owner"] != "controller" or motion["pose_space"] != "actor_local" or motion["baked_actor_translation"] is not False:
        raise ValueError("unsafe motion ownership")

    playback = clip.get("playback")
    if not isinstance(playback, dict) or set(playback) != {"mode", "seam_policy"}:
        raise ValueError("exact playback declaration is required")
    mode, seam = playback["mode"], playback["seam_policy"]
    if (mode, seam) not in (("loop", "match"), ("one_shot", "end_pose"), ("hold", "end_pose")):
        raise ValueError("unknown playback semantics")
    actor = clip.get("reference_actor_motion")
    if not isinstance(actor, dict) or set(actor) != {"mode", "translations"} or actor["mode"] != "one_shot":
        raise ValueError("reference actor motion must be translation-only one_shot")
    actor_translation = _array(actor["translations"], (count, 3), "reference actor translations")
    ground = clip.get("ground")
    if not isinstance(ground, dict) or set(ground) != {"space", "height", "tolerance"} or ground["space"] != "reference_world":
        raise ValueError("exact reference_world ground declaration is required")
    ground_height = _number(ground["height"], "ground.height")
    ground_tolerance = _number(ground["tolerance"], "ground.tolerance")
    cap = _number(max_ground_tolerance_m, "max_ground_tolerance_m")
    if ground_tolerance <= 0 or cap <= 0 or ground_tolerance > cap:
        raise ValueError("ground.tolerance must be positive and within the engineering cap")

    contacts_in, targets_in, anchors_in = clip.get("contacts"), clip.get("hoof_targets"), clip.get("surface_anchors")
    if not all(isinstance(x, dict) and set(x) == set(FEET) for x in (contacts_in, targets_in, anchors_in)):
        raise ValueError("all four exact foot tracks are required")
    meshes = blueprint.get("meshes")
    if not isinstance(meshes, list) or len(meshes) != 1 or any(not isinstance(m, dict) or not isinstance(m.get("vertices"), list) for m in meshes):
        raise ValueError("exactly one blueprint mesh is required for unqualified surface anchors")
    vertex_count = sum(len(m["vertices"]) for m in meshes)
    contacts, targets, anchors = {}, {}, {}
    for foot in FEET:
        track = contacts_in[foot]
        if not isinstance(track, list) or len(track) != count or any(type(x) is not bool for x in track):
            raise ValueError(f"invalid contact track for {foot}")
        contacts[foot] = tuple(track)
        targets[foot] = _array(targets_in[foot], (count, 3), f"{foot} targets")
        anchor = anchors_in[foot]
        if not isinstance(anchor, dict) or set(anchor) != {"sole_vertices", "foot_vertices"}:
            raise ValueError(f"invalid surface anchor for {foot}")
        checked_anchor = {}
        for key in ("sole_vertices", "foot_vertices"):
            ids = anchor[key]
            if not isinstance(ids, list) or not ids or any(isinstance(i, bool) or not isinstance(i, int) or not 0 <= i < vertex_count for i in ids) or len(ids) != len(set(ids)):
                raise ValueError(f"invalid {foot} {key}")
            checked_anchor[key] = tuple(ids)
        if not set(checked_anchor["sole_vertices"]).issubset(checked_anchor["foot_vertices"]):
            raise ValueError(f"{foot} sole vertices must belong to its foot vertices")
        anchors[foot] = MappingProxyType(checked_anchor)
        world_targets = targets[foot] + actor_translation
        if np.any(world_targets[:, 2] < ground_height - ground_tolerance):
            raise ValueError(f"{foot} target penetrates reference_world ground")
        for sample in range(count):
            if track[sample] and abs(world_targets[sample, 2] - ground_height) > ground_tolerance:
                raise ValueError(f"{foot} planted target is off reference_world ground")
        for sample in range(1, count):
            if track[sample - 1] and track[sample] and np.linalg.norm(world_targets[sample] - world_targets[sample - 1]) > ground_tolerance:
                raise ValueError(f"{foot} planted reference_world target slides")

    phases_in = clip.get("phases")
    if not isinstance(phases_in, list) or not phases_in:
        raise ValueError("phases are required")
    phases, cursor = [], 0
    for phase in phases_in:
        if not isinstance(phase, dict) or set(phase) != {"kind", "start", "end"}:
            raise ValueError("exact phase rows are required")
        start, end = _exact_int(phase["start"], "phase.start"), _exact_int(phase["end"], "phase.end")
        if phase["kind"] not in ("support", "flight") or start != cursor or end <= start or end > count:
            raise ValueError("phases must be ordered half-open intervals partitioning samples")
        for sample in range(start, end):
            states = tuple(contacts[foot][sample] for foot in FEET)
            if (phase["kind"] == "flight") != (not any(states)):
                raise ValueError("phase kind is inconsistent with four-track contacts")
        phases.append(MappingProxyType(dict(phase)))
        cursor = end
    if cursor != count:
        raise ValueError("phases do not cover the timeline")

    entry = clip.get("entry_contacts")
    if not isinstance(entry, dict) or set(entry) != set(FEET) or any(type(entry[f]) is not bool for f in FEET):
        raise ValueError("entry_contacts must contain four booleans")
    derived = []
    for sample in range(count):
        for foot in FEET:
            previous = entry[foot] if sample == 0 else contacts[foot][sample - 1]
            current = contacts[foot][sample]
            if current != previous:
                derived.append({"foot": foot, "kind": "touchdown" if current else "liftoff", "sample": sample})
    events = clip.get("events")
    if not isinstance(events, list):
        raise ValueError("events must be a list of exact contact-transition rows")
    for event in events:
        if (not isinstance(event, dict) or set(event) != {"foot", "kind", "sample"} or
                event["foot"] not in FEET or event["kind"] not in ("liftoff", "touchdown") or
                not 0 <= _exact_int(event["sample"], "event.sample") < count):
            raise ValueError("invalid contact event")
    if events != derived:
        raise ValueError("events must exactly equal four-track contact transitions")
    if mode == "loop" and any(entry[f] != contacts[f][-1] for f in FEET):
        raise ValueError("loop contact state must close across its seam")
    if mode in ("loop", "hold") and action == "jump_air" and any(any(contacts[f]) for f in FEET):
        raise ValueError("loop/hold jump_air must remain all-air through its final hold or seam")

    frame_bones = []
    for frame in frames:
        transforms = frame.get("bones")
        if not isinstance(transforms, dict) or set(transforms) != set(bone_names):
            raise ValueError("frame bone coverage mismatch")
        checked = {}
        for name in bone_names:
            trs = transforms[name]
            if not isinstance(trs, dict) or set(trs) != {"translation", "rotation", "scale"}:
                raise ValueError(f"exact TRS required for {name}")
            translation = _array(trs["translation"], (3,), f"{name} translation")
            rotation = _array(trs["rotation"], (4,), f"{name} rotation")
            scale = _array(trs["scale"], (3,), f"{name} scale")
            if abs(np.linalg.norm(rotation) - 1) > 1e-6:
                raise ValueError(f"{name} quaternion is not normalized")
            if not np.allclose(scale, np.ones(3), rtol=0, atol=1e-7):
                raise ValueError(f"{name} scale must remain unit")
            checked[name] = (translation, rotation, scale)
        frame_bones.append(checked)
    pose_root_offsets = _array(motion["pose_root_offsets"], (count, 3), "pose_root_offsets")
    root_translation = np.stack([f[root_name][0] for f in frame_bones])
    rest_translation = root_rest[:3, 3]
    if not np.allclose(pose_root_offsets, root_translation - rest_translation, rtol=0, atol=1e-9):
        raise ValueError("pose_root_offsets do not match actual root translation minus rest")
    if mode == "loop" and seam == "match":
        for name in bone_names:
            first, last = frame_bones[0][name], frame_bones[-1][name]
            if not np.allclose(first[0], last[0], rtol=0, atol=1e-7) or not np.allclose(first[2], last[2], rtol=0, atol=1e-7) or abs(float(np.dot(first[1], last[1]))) < 1 - 1e-7:
                raise ValueError("loop local-pose seam mismatch")
    return ValidatedClipV2(action, count, times, actor_translation, pose_root_offsets,
        MappingProxyType(contacts), MappingProxyType(targets), MappingProxyType(anchors),
        tuple(phases), tuple(MappingProxyType(x) for x in derived), root_name, mode, seam,
        ground_height, ground_tolerance)

def apply_reference_actor_translation(points, translation, *, sample_space):
    """Convert actor-local geometry to reference-world exactly once."""
    if sample_space != "actor_local":
        raise ValueError("reference actor translation can only be applied to actor_local geometry")
    raw = np.asarray(points)
    if raw.dtype.kind == "b" or raw.dtype.kind not in "iuf" or raw.ndim < 1 or raw.shape[-1] != 3:
        raise ValueError("geometry points must be a finite numeric (..., 3) array")
    result = _array(points, raw.shape, "geometry points").copy()
    result += _array(translation, (3,), "actor translation")
    result.setflags(write=False)
    return result, "reference_world"

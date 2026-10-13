"""Reusable full-3D retargeting for approved hoof animation entries.

Input channels are local *absolute* transforms compatible with the declared
reference rest skeleton.  They are converted to rest-relative motion exactly
once, calibrated from reference bone frames into target bone frames, then
composed onto the target rest transforms.  Only root translation is scaled;
static limb translations, hierarchy, geometry, materials and skin weights are
never rewritten by this module.
"""
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "autorig.hoof-retarget.v1"
DEFAULT_OPTIONS = {
    "root_motion_scale": 1.0,
    "quaternion_norm_tolerance": 0.02,
    "singular_epsilon": 1.0e-8,
    "continuity_flip_dot": 0.0,
    "frame_parallel_dot_threshold": 0.98,
}
OPTION_RANGES = {
    "root_motion_scale": (0.1, 10.0),
    "quaternion_norm_tolerance": (1.0e-6, 0.1),
    "singular_epsilon": (1.0e-12, 1.0e-3),
    "continuity_flip_dot": (-0.1, 0.1),
    "frame_parallel_dot_threshold": (0.90, 0.999999),
}


class RetargetError(ValueError):
    """Fail-closed invalid/incomplete retarget input."""


def target_from_ap_rig(rig: Any) -> dict[str, Any]:
    """Snapshot the documented AP.Rig fields into the explicit target contract."""
    doc = getattr(rig, "doc", None)
    rests = getattr(rig, "rest", None)
    if not isinstance(doc, Mapping) or not isinstance(rests, Mapping):
        raise RetargetError("AP.Rig doc/rest metadata is unavailable")
    bones = []
    for bone in doc.get("bones") or []:
        name = bone.get("name") if isinstance(bone, Mapping) else None
        rest = rests.get(name)
        if not name or not isinstance(rest, Sequence) or len(rest) != 3:
            raise RetargetError(f"AP.Rig rest transform missing for {name}")
        bones.append({"name": name, "parent": bone.get("parent"),
                      "rest_translation": list(rest[0]), "rest_rotation": list(rest[1]),
                      "rest_scale": list(rest[2]), "head": list(bone.get("head") or []),
                      "tail": list(bone.get("tail") or [])})
    forward = getattr(rig, "forward", None)
    forward_value = forward() if callable(forward) else doc.get("forward_axis")
    if isinstance(forward_value, str):
        forward_value = {"+z": [0, 0, 1], "-z": [0, 0, -1], "+x": [1, 0, 0], "-x": [-1, 0, 0]}.get(forward_value)
    points = [coord for bone in bones for coord in (bone["head"], bone["tail"]) if len(coord) == 3]
    ground = min((float(point[1]) for point in points), default=0.0)
    return {"height": float(getattr(rig, "H", 0) or doc.get("model_height_units") or 0),
            "ground_baseline": ground, "forward_axis": list(forward_value), "up_axis":[0,1,0], "bones": bones}


def options_get() -> dict[str, Any]:
    return {"values": dict(DEFAULT_OPTIONS), "ranges": {k: list(v) for k, v in OPTION_RANGES.items()}}


def _options(value: Mapping[str, Any] | None) -> dict[str, float]:
    supplied = dict(value or {})
    unknown = sorted(set(supplied) - set(DEFAULT_OPTIONS))
    if unknown:
        raise RetargetError("unknown options: " + ", ".join(unknown))
    result = dict(DEFAULT_OPTIONS)
    result.update(supplied)
    for key, (low, high) in OPTION_RANGES.items():
        item = result[key]
        if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(float(item)):
            raise RetargetError(f"option {key} must be finite numeric")
        if not low <= float(item) <= high:
            raise RetargetError(f"option {key} outside [{low}, {high}]")
        result[key] = float(item)
    return result


def _vec(value: Any, width: int, label: str) -> tuple[float, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != width:
        raise RetargetError(f"{label} must be a {width}-vector")
    result = tuple(float(v) for v in value)
    if any(not math.isfinite(v) for v in result):
        raise RetargetError(f"{label} contains non-finite values")
    return result


def _vadd(a, b): return tuple(x + y for x, y in zip(a, b))
def _vsub(a, b): return tuple(x - y for x, y in zip(a, b))
def _vmul(a, s): return tuple(x * s for x in a)
def _dot(a, b): return sum(x * y for x, y in zip(a, b))
def _cross(a, b): return (a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2], a[0]*b[1]-a[1]*b[0])
def _norm(a): return math.sqrt(_dot(a, a))


def _unit(a, eps, label):
    n = _norm(a)
    if n <= eps:
        raise RetargetError(f"singular {label}")
    return _vmul(a, 1.0 / n)


def _qnorm(q, eps, tolerance, label):
    q = _vec(q, 4, label)
    n = _norm(q)
    if n <= eps or abs(n - 1.0) > tolerance:
        raise RetargetError(f"{label} is singular or not normalized")
    return _vmul(q, 1.0 / n)


def _qconj(q): return (-q[0], -q[1], -q[2], q[3])


def _qmul(a, b):
    ax, ay, az, aw = a; bx, by, bz, bw = b
    return (aw*bx + ax*bw + ay*bz - az*by,
            aw*by - ax*bz + ay*bw + az*bx,
            aw*bz + ax*by - ay*bx + az*bw,
            aw*bw - ax*bx - ay*by - az*bz)


def _qrot(q, v):
    return _qmul(_qmul(q, (v[0], v[1], v[2], 0.0)), _qconj(q))[:3]


def _qfrom_to(a, b, eps):
    a = _unit(a, eps, "source axis"); b = _unit(b, eps, "target axis")
    d = max(-1.0, min(1.0, _dot(a, b)))
    if d > 1.0 - eps:
        return (0.0, 0.0, 0.0, 1.0)
    if d < -1.0 + eps:
        axis = _cross(a, (1.0, 0.0, 0.0))
        if _norm(axis) <= eps:
            axis = _cross(a, (0.0, 1.0, 0.0))
        axis = _unit(axis, eps, "opposite-axis calibration")
        return (axis[0], axis[1], axis[2], 0.0)
    c = _cross(a, b)
    return _qnorm((c[0], c[1], c[2], 1.0 + d), eps, 1.0, "axis calibration")


def _frame(direction, forward, up, eps, parallel_dot_threshold):
    # Canonical +Y follows the bone; canonical +Z follows projected model forward.
    y = _unit(direction, eps, "bone direction")
    forward=_unit(forward,eps,"model forward"); up=_unit(up,eps,"anatomical up")
    fallback=abs(_dot(y,forward)) >= parallel_dot_threshold
    reference=up if fallback else forward
    z0 = _vsub(reference, _vmul(y, _dot(reference, y)))
    if _norm(z0) <= eps:
        raise RetargetError("singular bone frame: forward and anatomical up axes parallel to bone")
    z = _unit(z0, eps, "bone forward")
    x = _unit(_cross(y, z), eps, "bone side")
    z = _cross(x, y)
    # Rotation matrix columns x,y,z to quaternion (x,y,z,w).
    m00,m10,m20=x; m01,m11,m21=y; m02,m12,m22=z
    tr = m00 + m11 + m22
    if tr > 0:
        s = math.sqrt(tr + 1.0) * 2; q=((m21-m12)/s,(m02-m20)/s,(m10-m01)/s,0.25*s)
    elif m00 > m11 and m00 > m22:
        s=math.sqrt(1+m00-m11-m22)*2; q=(0.25*s,(m01+m10)/s,(m02+m20)/s,(m21-m12)/s)
    elif m11 > m22:
        s=math.sqrt(1+m11-m00-m22)*2; q=((m01+m10)/s,0.25*s,(m12+m21)/s,(m02-m20)/s)
    else:
        s=math.sqrt(1+m22-m00-m11)*2; q=((m02+m20)/s,(m12+m21)/s,0.25*s,(m10-m01)/s)
    return _qnorm(q, eps, 1.0, "bone frame"), ("anatomical_up" if fallback else "forward")


def _skeleton(value: Any, label: str, opt: Mapping[str, float]):
    if not isinstance(value, Mapping):
        raise RetargetError(f"{label} skeleton missing")
    height = float(value.get("height", 0))
    ground = float(value.get("ground_baseline", 0))
    if not math.isfinite(height) or height <= opt["singular_epsilon"] or not math.isfinite(ground):
        raise RetargetError(f"{label} skeleton has invalid height/ground baseline")
    forward = _unit(_vec(value.get("forward_axis"), 3, f"{label}.forward_axis"), opt["singular_epsilon"], f"{label} forward")
    up = _unit(_vec(value.get("up_axis"), 3, f"{label}.up_axis"), opt["singular_epsilon"], f"{label} up")
    if abs(_dot(forward,up)) > 1.0- opt["singular_epsilon"]:
        raise RetargetError(f"{label} forward/up axes are parallel")
    raw = value.get("bones")
    if not isinstance(raw, list) or not raw:
        raise RetargetError(f"{label} skeleton has no bones")
    bones = {}
    for row in raw:
        if not isinstance(row, Mapping) or not isinstance(row.get("name"), str) or not row["name"] or row["name"] in bones:
            raise RetargetError(f"{label} skeleton has invalid/duplicate bone")
        name = row["name"]
        scale = _vec(row.get("rest_scale"), 3, f"{label}.{name}.rest_scale")
        if any(abs(v) <= opt["singular_epsilon"] for v in scale):
            raise RetargetError(f"{label}.{name} has singular rest scale")
        bones[name] = {"name": name, "parent": row.get("parent"),
                       "optional_decorative": row.get("optional_decorative") is True,
                       "rest_translation": _vec(row.get("rest_translation"), 3, f"{label}.{name}.rest_translation"),
                       "rest_rotation": _qnorm(row.get("rest_rotation"), opt["singular_epsilon"], opt["quaternion_norm_tolerance"], f"{label}.{name}.rest_rotation"),
                       "rest_scale": scale, "head": _vec(row.get("head"), 3, f"{label}.{name}.head"),
                       "tail": _vec(row.get("tail"), 3, f"{label}.{name}.tail")}
    for name, bone in bones.items():
        if bone["parent"] is not None and bone["parent"] not in bones:
            raise RetargetError(f"{label}.{name} parent is missing")
        _unit(_vsub(bone["tail"], bone["head"]), opt["singular_epsilon"], f"{label}.{name} length")
    return {"height": height, "ground_baseline": ground, "forward_axis": forward, "up_axis":up, "bones": bones}


def _mapping(value: Any, source_bones, target_bones):
    if not isinstance(value, Mapping) or not value:
        raise RetargetError("complete bone_map is required")
    result = {}
    missing_required = sorted(name for name, bone in source_bones.items()
                              if not bone["optional_decorative"] and name not in value)
    if missing_required:
        raise RetargetError("bone_map missing required bones: " + ", ".join(missing_required))
    for source, spec in value.items():
        if source not in source_bones:
            raise RetargetError(f"bone_map source absent: {source}")
        if isinstance(spec, str):
            target, optional = spec, False
        elif isinstance(spec, Mapping):
            target, optional = spec.get("target"), spec.get("optional_decorative") is True
        else:
            raise RetargetError(f"invalid bone_map row: {source}")
        if target not in target_bones:
            if optional:
                result[source] = (None, True)
                continue
            raise RetargetError(f"required target bone absent: {target}")
        result[source] = (target, optional)
    return result


def _track_rows(entry: Mapping[str, Any], frame_count: int):
    tracks = entry.get("tracks")
    if not isinstance(tracks, Mapping) or not tracks:
        raise RetargetError("full 3D tracks are required; legacy curves are unsupported")
    rows = []
    for track_id, track in tracks.items():
        if not isinstance(track, Mapping): raise RetargetError(f"invalid track {track_id}")
        bone, prop = track.get("bone"), track.get("property")
        typ = track.get("type")
        if prop == "rotation" and typ != "quaternion": raise RetargetError(f"{track_id}: rotation must be quaternion")
        if prop == "translation" and typ != "translation": raise RetargetError(f"{track_id}: translation type mismatch")
        if prop not in {"rotation", "translation"}: raise RetargetError(f"{track_id}: unsupported property/morph track")
        values = track.get("values")
        if not isinstance(values, list) or len(values) != frame_count: raise RetargetError(f"{track_id}: frame count mismatch")
        rows.append((str(track_id), bone, prop, values))
    return rows


def retarget_entry(entry: Mapping[str, Any], target_rig: Mapping[str, Any], bone_map: Mapping[str, Any],
                   options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return one AP.append_clips-compatible clip without mutating inputs."""
    opt = _options(options)
    meta = entry.get("action_metadata")
    if not isinstance(meta, Mapping): raise RetargetError("action_metadata missing")
    clip_id = str(entry.get("clip") or "")
    frames = meta.get("frame_count"); fps = meta.get("fps")
    if not clip_id or isinstance(frames, bool) or not isinstance(frames, int) or frames < 2:
        raise RetargetError("invalid clip/frame count")
    if isinstance(fps, bool) or not isinstance(fps, (int, float)) or not math.isfinite(float(fps)) or fps <= 0:
        raise RetargetError("invalid fps")
    src = _skeleton(entry.get("reference_skeleton"), "reference", opt)
    tgt = _skeleton(target_rig, "target", opt)
    mapping = _mapping(bone_map, src["bones"], tgt["bones"])
    rows = _track_rows(entry, frames)
    animated = {bone for _, bone, _, _ in rows}
    missing_map = sorted(animated - set(mapping))
    if missing_map: raise RetargetError("bone_map missing animated bones: " + ", ".join(missing_map))
    global_cal = _qfrom_to(src["forward_axis"], tgt["forward_axis"], opt["singular_epsilon"])
    rotations, translations = {}, {}
    root_sources = {name for name, bone in src["bones"].items() if bone["parent"] is None}
    root_scale = tgt["height"] / src["height"] * opt["root_motion_scale"]
    source_fallback_bones=[]; target_fallback_bones=[]
    for track_id, source_name, prop, values in rows:
        target_name, optional = mapping[source_name]
        if target_name is None and optional: continue
        sb, tb = src["bones"][source_name], tgt["bones"][target_name]
        sf,sbasis = _frame(_vsub(sb["tail"], sb["head"]), src["forward_axis"],src["up_axis"],
                           opt["singular_epsilon"],opt["frame_parallel_dot_threshold"])
        tf,tbasis = _frame(_vsub(tb["tail"], tb["head"]), tgt["forward_axis"],tgt["up_axis"],
                           opt["singular_epsilon"],opt["frame_parallel_dot_threshold"])
        if sbasis=="anatomical_up" and source_name not in source_fallback_bones: source_fallback_bones.append(source_name)
        if tbasis=="anatomical_up" and target_name not in target_fallback_bones: target_fallback_bones.append(target_name)
        calibration = _qmul(_qconj(tf), _qmul(global_cal, sf))
        if prop == "rotation":
            out = []
            for index, raw in enumerate(values):
                absolute = _qnorm(raw, opt["singular_epsilon"], opt["quaternion_norm_tolerance"], f"{track_id}[{index}]")
                delta = _qmul(_qconj(sb["rest_rotation"]), absolute)
                converted = _qmul(calibration, _qmul(delta, _qconj(calibration)))
                absolute_target = _qnorm(_qmul(tb["rest_rotation"], converted), opt["singular_epsilon"], 1.0, "output quaternion")
                if out and _dot(out[-1], absolute_target) < opt["continuity_flip_dot"]:
                    absolute_target = _vmul(absolute_target, -1.0)
                out.append(list(absolute_target))
            if target_name in rotations: raise RetargetError(f"duplicate target rotation: {target_name}")
            rotations[target_name] = out
        else:
            if source_name not in root_sources:
                # Local non-root translations define bone length; preserve target rest.
                out = [list(tb["rest_translation"]) for _ in values]
            else:
                out = []
                for raw in values:
                    delta = _vsub(_vec(raw, 3, track_id), sb["rest_translation"])
                    delta = _vmul(_qrot(global_cal, delta), root_scale)
                    out.append(list(_vadd(tb["rest_translation"], delta)))
            if target_name in translations: raise RetargetError(f"duplicate target translation: {target_name}")
            translations[target_name] = out
    if not rotations: raise RetargetError("no required rotation tracks produced")
    root_targets = [mapping[name][0] for name in root_sources if name in mapping and mapping[name][0] in translations]
    hips_translation = translations[root_targets[0]] if len(root_targets) == 1 else None
    times = [i / float(fps) for i in range(frames)]
    source_root_track = next((values for _, bone, prop, values in rows if bone in root_sources and prop == "translation"), None)
    source_travel = _norm(_vsub(_vec(source_root_track[-1], 3, "source root"), _vec(source_root_track[0], 3, "source root"))) if source_root_track else 0.0
    target_travel = _norm(_vsub(hips_translation[-1], hips_translation[0])) if hips_translation else 0.0
    pose0 = max((2.0 * math.acos(min(1.0, abs(_dot(_qnorm(values[0], opt["singular_epsilon"], 1.0, "pose0"), tgt["bones"][bone]["rest_rotation"]))))
                 for bone, values in rotations.items()), default=0.0)
    lengths = {target: _norm(_vsub(tgt["bones"][target]["tail"], tgt["bones"][target]["head"]))
               for target in rotations}
    clip = {"id": clip_id, "name": clip_id, "source": "approved_hoof_library_3d",
            "duration": times[-1], "times": times, "loop": entry.get("loop") is True,
            "rotations": rotations, "translations": translations,
            "retarget_metadata": {"schema": SCHEMA, "options": opt,
                "reference_height": src["height"], "target_height": tgt["height"],
                "reference_ground_baseline": src["ground_baseline"], "target_ground_baseline": tgt["ground_baseline"],
                "reference_forward_axis":list(src["forward_axis"]),"reference_up_axis":list(src["up_axis"]),
                "target_forward_axis":list(tgt["forward_axis"]),"target_up_axis":list(tgt["up_axis"]),
                "source_anatomical_up_fallback_bones":source_fallback_bones,
                "target_anatomical_up_fallback_bones":target_fallback_bones,
                "height_ratio": tgt["height"] / src["height"], "root_motion_scale": root_scale,
                "pose0_vs_target_rest_max_radians": pose0,
                "target_bone_lengths_before": lengths, "target_bone_lengths_after": dict(lengths),
                "source_root_travel": source_travel, "target_root_travel": target_travel}}
    if hips_translation is not None: clip["hips_translation"] = hips_translation
    return clip


def retarget_library(entries: Sequence[Mapping[str, Any]], target_rig: Mapping[str, Any],
                     bone_map: Mapping[str, Any], required_ids: Iterable[str],
                     options: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """Fail atomically unless every required action retargets."""
    required = tuple(required_ids)
    by_id = {str(row.get("clip") or ""): row for row in entries}
    if len(by_id) != len(entries) or set(by_id) != set(required):
        raise RetargetError("library must exactly match the complete required action set")
    return [retarget_entry(by_id[action], target_rig, bone_map, options) for action in required]

#!/usr/bin/env python3
"""Strict post-retarget semantic QA for the V3 equine motion library.

The contract consumes sampled 3D tracks.  It never infers a hidden hoof from a
visible one and never treats a joint-angle extremum as a ground contact.  A
missing measurement is both ``pending`` and a failure, so incomplete evidence
cannot enter an accepted library release.

Input schema (all distances use the same model-space unit)::

    {
      "action": "trot", "times": [0, ...], "model_height": 1.7,
      "loop": true, "clip_type": "loop",
      "ground": {"normal": [0, 1, 0], "offset": 0},
      "axes": {"up": [0,1,0], "forward": [0,0,1], "right": [1,0,0],
               "source_supported": true},
      "root": {"positions": [[...]], "quaternions": [[x,y,z,w], ...],
               "source_track": "Root"},
      "hooves": {"LF": {"positions": [[...]], "identity_observed": true,
                           "source_track": "LeftFrontFoot",
                           "generated": false, "mirrored": false}, ...},
      "head": {"quaternions": [[...]], "source_track": "Head"},
      "tail": {"positions": [[...]], "source_track": "Tail"}
    }

``evaluate(sample, options)`` returns exactly ``pass``, ``metrics``,
``failures`` and ``pending``.  ``options_get()`` exposes documented,
range-checked options and current values.
"""
from __future__ import annotations

import math
from typing import Any, Mapping, Sequence


LEGS = ("LH", "LF", "RH", "RF")
CORE14_ACTIONS = (
    "idle", "walk", "trot", "canter", "gallop", "start", "stop",
    "turn_left", "turn_right", "backward", "jump", "rear", "look", "tail",
)
ACTION_CONTRACT = (
    ("idle_neutral", True), ("idle_alert", True), ("idle_relaxed", True),
    ("idle_look_around", True), ("idle_fidget", True),
    ("walk_forward", True), ("walk_backward", True), ("trot_jog", True),
    ("run", True), ("sprint", True), ("turn_left_90", False),
    ("turn_right_90", False), ("turn_around_180", False), ("stop_brake", False),
    ("jump_air", True), ("fall", False), ("jump_start", False),
    ("jump_land", False), ("jump_full", False), ("attack_primary", False),
    ("attack_secondary", False), ("attack_heavy", False), ("hit_front", False),
    ("hit_left", False), ("hit_right", False), ("death", False),
    ("get_up", False), ("eat_interact", True), ("sleep_rest", True),
    ("vocalize_emote", False), ("canter", True), ("start", False),
    ("rear", False), ("tail", True),
)
CANONICAL_ACTIONS = tuple(row[0] for row in ACTION_CONTRACT)
CORE14_ALIASES = {
    "idle": "idle_neutral", "walk": "walk_forward", "trot": "trot_jog",
    "canter": "canter", "gallop": "sprint", "start": "start", "stop": "stop_brake",
    "turn_left": "turn_left_90", "turn_right": "turn_right_90",
    "backward": "walk_backward", "jump": "jump_full", "rear": "rear",
    "look": "idle_look_around", "tail": "tail",
}
KNOWN_ACTIONS = tuple(dict.fromkeys(CORE14_ACTIONS + CANONICAL_ACTIONS))
CANONICAL_LOOPS = frozenset(name for name, loop in ACTION_CONTRACT if loop)
LOOPS = frozenset({name for name in KNOWN_ACTIONS if CORE14_ALIASES.get(name, name) in CANONICAL_LOOPS})

_OPTION_SPEC = {
    "contact_height_ratio": (0.025, 0.001, 0.10, "Maximum ground-relative hoof height counted as contact / model height."),
    "contact_vertical_speed_ratio": (0.12, 0.01, 1.0, "Maximum absolute normal hoof speed during contact / model height per second."),
    "max_slide_speed_ratio": (0.08, 0.005, 1.0, "Maximum tangential speed of a planted hoof / model height per second."),
    "max_penetration_ratio": (0.012, 0.0, 0.10, "Maximum permitted hoof penetration below the ground / model height."),
    "event_sync_fraction": (0.09, 0.01, 0.25, "Largest phase difference considered one simultaneous beat."),
    "min_beat_gap_fraction": (0.08, 0.01, 0.24, "Smallest phase gap between distinct beats."),
    "min_flight_fraction": (0.06, 0.01, 0.50, "Minimum all-hooves-airborne interval for jump/gallop."),
    "max_stationary_speed_ratio": (0.035, 0.001, 0.30, "Maximum hoof/root speed for a stationary action / height per second."),
    "min_root_travel_ratio": (0.08, 0.001, 2.0, "Minimum directed root travel for locomotion / height."),
    "min_turn_yaw_deg": (25.0, 1.0, 180.0, "Minimum signed root yaw for a turn."),
    "turn_angle_tolerance_deg": (12.0, 1.0, 45.0, "Allowed absolute error from canonical 90 or 180 degree root turn."),
    "max_loop_position_ratio": (0.035, 0.001, 0.30, "Maximum loop endpoint root-position mismatch / height."),
    "max_loop_rotation_deg": (8.0, 0.1, 90.0, "Maximum loop endpoint root-rotation mismatch."),
    "min_jump_lift_ratio": (0.10, 0.01, 1.0, "Minimum root lift during a jump / height."),
    "min_rear_fore_lift_ratio": (0.12, 0.01, 1.0, "Minimum fore-hoof lift during rear / height."),
    "min_head_yaw_deg": (10.0, 1.0, 120.0, "Minimum actual head yaw range for look."),
    "min_tail_motion_ratio": (0.04, 0.001, 1.0, "Minimum actual tail-tip 3D range / height."),
    "min_samples": (12, 4, 100000, "Minimum sampled frames."),
    "min_duration_s": (0.25, 0.01, 60.0, "Minimum clip duration."),
    "max_duration_s": (20.0, 0.1, 300.0, "Maximum clip duration."),
    "min_body_height_change_ratio": (0.18, 0.01, 1.0, "Minimum measured root/body height change for fall, death or get-up / height."),
    "min_body_tilt_deg": (35.0, 1.0, 180.0, "Minimum measured body angular change for fall/death/recovery."),
    "max_rest_speed_ratio": (0.025, 0.001, 0.25, "Maximum end/rest root speed / height per second."),
    "min_active_motion_ratio": (0.06, 0.001, 1.0, "Minimum measured active joint, head, mouth or hoof displacement / height."),
    "min_angular_impulse_deg_s": (45.0, 1.0, 2000.0, "Minimum torso angular-speed peak for a directional hit."),
    "min_linear_impulse_ratio": (0.12, 0.001, 10.0, "Minimum root acceleration peak / height per second squared for a hit."),
    "sleep_root_height_ratio": (0.55, 0.05, 0.95, "Maximum sleeping root height relative to standing reference / model height."),
}


def options_get(options: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """Return the effective values, ranges and documentation for every option."""
    supplied = dict(options or {})
    unknown = sorted(set(supplied) - set(_OPTION_SPEC))
    if unknown:
        raise ValueError("unknown options: " + ", ".join(unknown))
    result = {}
    for name, (default, low, high, doc) in _OPTION_SPEC.items():
        value = supplied.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValueError(f"{name} must be a finite number")
        value = int(value) if name == "min_samples" else float(value)
        if not low <= value <= high:
            raise ValueError(f"{name} must be in [{low}, {high}]")
        result[name] = {"value": value, "default": default, "min": low, "max": high, "doc": doc}
    if result["min_duration_s"]["value"] >= result["max_duration_s"]["value"]:
        raise ValueError("min_duration_s must be smaller than max_duration_s")
    return result


def _values(options: Mapping[str, Any] | None) -> dict[str, float]:
    return {name: row["value"] for name, row in options_get(options).items()}


def _finite_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(float(value))


def _vec(value: Any, size: int) -> tuple[float, ...] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != size:
        return None
    if not all(_finite_number(v) for v in value):
        return None
    return tuple(float(v) for v in value)


def _track(value: Any, count: int, size: int) -> list[tuple[float, ...]] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != count:
        return None
    rows = [_vec(row, size) for row in value]
    return None if any(row is None for row in rows) else rows  # type: ignore[return-value]


def _dot(a: Sequence[float], b: Sequence[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def _sub(a: Sequence[float], b: Sequence[float]) -> tuple[float, ...]:
    return tuple(x - y for x, y in zip(a, b))


def _norm(v: Sequence[float]) -> float:
    return math.sqrt(_dot(v, v))


def _unit(v: Sequence[float]) -> tuple[float, ...] | None:
    length = _norm(v)
    return None if length < 1e-9 else tuple(x / length for x in v)


def _quat_angle_deg(a: Sequence[float], b: Sequence[float]) -> float:
    na, nb = _unit(a), _unit(b)
    if na is None or nb is None:
        return math.inf
    return math.degrees(2.0 * math.acos(min(1.0, abs(_dot(na, nb)))))


def _quaternions_valid(track: Sequence[Sequence[float]] | None) -> bool:
    """A zero/scaled quaternion is not usable pose evidence."""
    return track is not None and all(0.98 <= _norm(q) <= 1.02 for q in track)


def _quat_rotate(q: Sequence[float], v: Sequence[float]) -> tuple[float, float, float]:
    x, y, z, w = q
    # q * v * q^-1, expanded.
    tx, ty, tz = 2 * (y * v[2] - z * v[1]), 2 * (z * v[0] - x * v[2]), 2 * (x * v[1] - y * v[0])
    return (v[0] + w * tx + y * tz - z * ty,
            v[1] + w * ty + z * tx - x * tz,
            v[2] + w * tz + x * ty - y * tx)


def _signed_yaw(q: Sequence[float], forward: Sequence[float], up: Sequence[float], right: Sequence[float]) -> float:
    facing = _quat_rotate(q, forward)
    return math.degrees(math.atan2(_dot(facing, right), _dot(facing, forward)))


def _signed_pitch(q: Sequence[float], forward: Sequence[float], up: Sequence[float]) -> float:
    facing = _quat_rotate(q, forward)
    return math.degrees(math.asin(max(-1.0, min(1.0, _dot(facing, up)))))


def _circular_delta(a: float, b: float) -> float:
    return (b - a) % 1.0


def _circular_yaw_delta(a: float, b: float) -> float:
    return (b - a + 180.0) % 360.0 - 180.0


def _unwrapped_yaw_delta(track: Sequence[Sequence[float]], forward: Sequence[float],
                         up: Sequence[float], right: Sequence[float]) -> float:
    """Accumulate signed yaw; positive is toward the declared physical right."""
    yaws = [_signed_yaw(q, forward, up, right) for q in track]
    return sum(_circular_yaw_delta(a, b) for a, b in zip(yaws, yaws[1:]))


def _add_failure(failures: list[str], code: str) -> None:
    if code not in failures:
        failures.append(code)


def _missing(failures: list[str], pending: list[str], code: str) -> None:
    _add_failure(failures, "missing_evidence:" + code)
    if code not in pending:
        pending.append(code)


def _source_track(sample: Mapping[str, Any], name: str, count: int,
                  failures: list[str], pending: list[str], *, quaternion: bool = False):
    """Read an explicitly observed source channel; generated channels are not evidence."""
    row = sample.get(name) if isinstance(sample.get(name), Mapping) else {}
    key, size = ("quaternions", 4) if quaternion else ("positions", 3)
    track = _track(row.get(key), count, size)
    valid = (track is not None and bool(row.get("source_track")) and
             row.get("generated", False) is False and row.get("mirrored", False) is False and
             row.get("source_observed", True) is True)
    if quaternion and valid:
        valid = _quaternions_valid(track)
    if not valid:
        _missing(failures, pending, f"source_{name}_{key}_track")
        return None
    return track


def _position_motion_ratio(track: Sequence[Sequence[float]], height: float) -> float:
    return max(_norm(_sub(point, track[0])) for point in track) / height


def _rotation_range_deg(track: Sequence[Sequence[float]]) -> float:
    return max(_quat_angle_deg(track[0], value) for value in track)


def _root_kinematics(track: Sequence[Sequence[float]], times: Sequence[float], height: float):
    velocities = [_norm(_sub(track[i], track[i - 1])) / (times[i] - times[i - 1]) / height
                  for i in range(1, len(track))]
    accelerations = [abs(velocities[i] - velocities[i - 1]) / (times[i + 1] - times[i])
                     for i in range(1, len(velocities))]
    return velocities, accelerations


def _strike_phases(contact: Sequence[bool], times: Sequence[float], loop: bool) -> list[float]:
    duration = times[-1] - times[0]
    phases = []
    for i, planted in enumerate(contact):
        previous = contact[i - 1] if i else (contact[-1] if loop else False)
        if planted and not previous:
            phases.append((times[i] - times[0]) / duration)
    return phases


def _nearest_phase(phases: Sequence[float], target: float) -> float:
    return min((min(abs(p - target), 1.0 - abs(p - target)) for p in phases), default=1.0)


def _gait_beats(action: str, phases: Mapping[str, list[float]], sync: float,
                gap: float, failures: list[str], metrics: dict[str, Any]) -> None:
    first = {leg: values[0] for leg, values in phases.items() if values}
    if set(first) != set(LEGS):
        _add_failure(failures, "gait_missing_hoof_strike")
        return
    metrics["first_strike_phase"] = {leg: round(first[leg], 5) for leg in LEGS}
    if action == "trot":
        diagonal = max(_nearest_phase(phases["RF"], first["LH"]), _nearest_phase(phases["LF"], first["RH"]))
        separation = min(_circular_delta(first["LH"], first["RH"]), _circular_delta(first["RH"], first["LH"]))
        metrics.update({"diagonal_sync_error": diagonal, "diagonal_pair_separation": separation})
        if diagonal > sync: _add_failure(failures, "trot_diagonal_pairs_not_synchronous")
        if not 0.5 - sync <= separation <= 0.5 + sync: _add_failure(failures, "trot_pairs_not_half_cycle_apart")
        return
    ordered = sorted((phase, leg) for leg, phase in first.items())
    distinct = []
    for phase, legs in ((p, [leg]) for p, leg in ordered):
        if distinct and min(phase - distinct[-1][0], 1.0 - phase + distinct[-1][0]) <= sync:
            distinct[-1][1].extend(legs)
        else:
            distinct.append([phase, legs])
    if action in {"walk", "gallop"}:
        if len(distinct) != 4: _add_failure(failures, f"{action}_must_have_four_distinct_beats")
        if len(distinct) == 4:
            gaps = [_circular_delta(distinct[i][0], distinct[(i + 1) % 4][0]) for i in range(4)]
            metrics["beat_gaps"] = gaps
            if min(gaps) < gap: _add_failure(failures, f"{action}_beats_too_close")
            orders = [("LH", "LF", "RH", "RF"), ("RH", "RF", "LH", "LF")]
            sequence = tuple(group[1][0] for group in distinct)
            rotations = [order[i:] + order[:i] for order in orders for i in range(4)]
            if action == "walk" and sequence not in rotations: _add_failure(failures, "walk_not_lateral_four_beat")
        return
    # Canter: trailing hind, diagonal pair, leading fore.  Lead is observed,
    # never guessed from a half-cycle mirror.
    patterns = {
        "left": (("LH",), ("RH", "LF"), ("RF",)),
        "right": (("RH",), ("LH", "RF"), ("LF",)),
    }
    matched = None
    for lead, pattern in patterns.items():
        for shift in range(3):
            groups = distinct[shift:] + distinct[:shift]
            if len(groups) == 3 and all(set(groups[i][1]) == set(pattern[i]) for i in range(3)):
                matched = lead
    metrics["observed_lead"] = matched
    if matched is None: _add_failure(failures, "canter_must_be_lead_aware_three_beat")


def evaluate(sample: Mapping[str, Any], options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Evaluate one retargeted action; incomplete evidence always fails closed."""
    opt = _values(options)
    failures: list[str] = []
    pending: list[str] = []
    metrics: dict[str, Any] = {"options": opt}
    requested_action = str(sample.get("action") or "")
    if requested_action not in KNOWN_ACTIONS:
        _add_failure(failures, "unknown_action")
        return {"pass": False, "metrics": metrics, "failures": failures, "pending": pending}
    action = CORE14_ALIASES.get(requested_action, requested_action)
    legacy_core14 = requested_action in CORE14_ALIASES
    semantic = {
        "idle_neutral": "idle", "idle_alert": "idle", "idle_relaxed": "idle",
        "idle_look_around": "look", "idle_fidget": "idle",
        "walk_forward": "walk", "walk_backward": "backward", "trot_jog": "trot",
        "run": "gallop", "sprint": "gallop", "turn_left_90": "turn_left",
        "turn_right_90": "turn_right", "stop_brake": "stop", "jump_full": "jump",
    }.get(action, action)
    metrics.update({"action": requested_action, "canonical_action": action})

    times_raw = sample.get("times")
    if not isinstance(times_raw, Sequence) or isinstance(times_raw, (str, bytes)) or not all(_finite_number(v) for v in times_raw):
        _missing(failures, pending, "finite_frame_times")
        return {"pass": False, "metrics": metrics, "failures": failures, "pending": pending}
    times = [float(v) for v in times_raw]
    n = len(times)
    if n < opt["min_samples"]: _add_failure(failures, "too_few_samples")
    if n < 2 or any(b <= a for a, b in zip(times, times[1:])):
        _add_failure(failures, "frame_times_not_strictly_increasing")
        return {"pass": False, "metrics": metrics, "failures": failures, "pending": pending}
    duration = times[-1] - times[0]
    metrics.update({"sample_count": n, "duration_s": duration})
    if not opt["min_duration_s"] <= duration <= opt["max_duration_s"]: _add_failure(failures, "duration_out_of_range")

    height = sample.get("model_height")
    if not _finite_number(height) or float(height) <= 0:
        _missing(failures, pending, "positive_model_height")
        height = 1.0
    height = float(height)
    expected_loop = action in CANONICAL_LOOPS
    if sample.get("loop") is not expected_loop: _add_failure(failures, "wrong_loop_metadata")
    expected_type = "loop" if expected_loop else "one_shot"
    if sample.get("clip_type") != expected_type: _add_failure(failures, "wrong_clip_type_metadata")

    ground = sample.get("ground") if isinstance(sample.get("ground"), Mapping) else {}
    normal = _unit(_vec(ground.get("normal"), 3) or ())
    offset = ground.get("offset")
    if normal is None or not _finite_number(offset):
        _missing(failures, pending, "ground_plane")
        normal, offset = (0.0, 1.0, 0.0), 0.0
    offset = float(offset)

    axes = sample.get("axes") if isinstance(sample.get("axes"), Mapping) else {}
    up, forward, right = (_unit(_vec(axes.get(k), 3) or ()) for k in ("up", "forward", "right"))
    axes_ok = (axes.get("source_supported") is True and up is not None and forward is not None and right is not None
               and abs(_dot(up, forward)) < 0.05 and abs(_dot(up, right)) < 0.05 and abs(_dot(forward, right)) < 0.05)
    axes_ok = bool(axes_ok and abs(_dot(up, normal)) >= 0.95)
    if not axes_ok:
        _missing(failures, pending, "source_supported_3d_axes")
        up, forward, right = normal, (0.0, 0.0, 1.0), (1.0, 0.0, 0.0)

    root = sample.get("root") if isinstance(sample.get("root"), Mapping) else {}
    root_pos, root_q = _track(root.get("positions"), n, 3), _track(root.get("quaternions"), n, 4)
    if root_pos is None or not root.get("source_track"): _missing(failures, pending, "root_position_track")
    if not _quaternions_valid(root_q) or not root.get("source_track"):
        _missing(failures, pending, "root_rotation_track")
        root_q = None

    locomotion = action in {"walk_forward", "walk_backward", "trot_jog", "run", "sprint", "canter"}
    root_motion_mode = sample.get("root_motion_mode")
    motion_pos = root_pos
    contact_trajectory = None
    if locomotion and not legacy_core14:
        if root_motion_mode not in {"world", "in_place"}:
            _missing(failures, pending, "declared_root_motion_mode")
        elif root_motion_mode == "in_place":
            trajectory = sample.get("verified_root_trajectory") if isinstance(sample.get("verified_root_trajectory"), Mapping) else {}
            contact_trajectory = _track(trajectory.get("positions"), n, 3)
            if (contact_trajectory is None or not trajectory.get("source_track") or
                    trajectory.get("source_observed") is not True):
                _missing(failures, pending, "verified_in_place_root_trajectory")
                contact_trajectory = None
            else:
                motion_pos = contact_trajectory
        metrics["root_motion_mode"] = root_motion_mode

    hooves_raw = sample.get("hooves") if isinstance(sample.get("hooves"), Mapping) else {}
    hoof_pos: dict[str, list[tuple[float, ...]]] = {}
    for leg in LEGS:
        row = hooves_raw.get(leg) if isinstance(hooves_raw.get(leg), Mapping) else {}
        pos = _track(row.get("positions"), n, 3)
        provenance_ok = (row.get("identity_observed") is True and bool(row.get("source_track"))
                         and row.get("generated") is False and row.get("mirrored") is False)
        if pos is None: _missing(failures, pending, f"hoof_track:{leg}")
        if not provenance_ok: _missing(failures, pending, f"observed_hoof_identity:{leg}")
        if pos is not None:
            # In-place clips are judged in the declared virtual world.  A foot
            # moving on the model-space floor is not accepted as planted merely
            # because its height is zero.
            if contact_trajectory is not None:
                pos = [tuple(a + b for a, b in zip(point, trajectory))
                       for point, trajectory in zip(pos, contact_trajectory)]
            hoof_pos[leg] = pos
    metrics["all_four_hooves_observed"] = len(hoof_pos) == 4 and not any("observed_hoof_identity" in p for p in pending)

    # Physical right comes from observed anatomy, not cross(up, forward).
    # Some GLTF rigs place Right* bones on -X.  The declared right axis must
    # agree with the vector from the left feet to the right feet.
    if len(hoof_pos) == 4:
        left_mid = tuple((hoof_pos["LF"][0][i] + hoof_pos["LH"][0][i]) * 0.5 for i in range(3))
        right_mid = tuple((hoof_pos["RF"][0][i] + hoof_pos["RH"][0][i]) * 0.5 for i in range(3))
        anatomical_right = _sub(right_mid, left_mid)
        anatomical_right = _sub(anatomical_right, tuple(_dot(anatomical_right, up) * value for value in up))
        anatomical_right = _unit(anatomical_right)
        if anatomical_right is None:
            _missing(failures, pending, "anatomical_right_axis")
        else:
            agreement = _dot(right, anatomical_right)
            metrics["declared_anatomical_right_dot"] = agreement
            if agreement < 0.90:
                _add_failure(failures, "declared_right_disagrees_with_observed_anatomy")
            right = anatomical_right

    contacts: dict[str, list[bool]] = {}
    phases: dict[str, list[float]] = {}
    all_speeds: dict[str, list[float]] = {}
    for leg, track in hoof_pos.items():
        heights = [_dot(normal, p) + offset for p in track]
        tangential, vertical = [0.0] * n, [0.0] * n
        for i in range(1, n):
            dt = times[i] - times[i - 1]
            velocity = tuple(v / dt for v in _sub(track[i], track[i - 1]))
            vertical[i] = abs(_dot(velocity, normal))
            tangential[i] = _norm(_sub(velocity, tuple(_dot(velocity, normal) * x for x in normal)))
        vertical[0], tangential[0] = vertical[1], tangential[1]
        planted = [h <= opt["contact_height_ratio"] * height and v <= opt["contact_vertical_speed_ratio"] * height
                   for h, v in zip(heights, vertical)]
        contacts[leg] = planted
        phases[leg] = _strike_phases(planted, times, expected_loop)
        all_speeds[leg] = tangential
        planted_slides = [speed for speed, on in zip(tangential, planted) if on]
        metrics.setdefault("hooves", {})[leg] = {
            "min_ground_height_ratio": min(heights) / height,
            "max_planted_slide_speed_ratio": max(planted_slides or [0.0]) / height,
            "contact_events": len(phases[leg]), "contact_phases": phases[leg],
            "contact_fraction": sum(planted) / n,
        }
        if min(heights) < -opt["max_penetration_ratio"] * height: _add_failure(failures, f"hoof_penetration:{leg}")
        if max(planted_slides or [0.0]) > opt["max_slide_speed_ratio"] * height: _add_failure(failures, f"hoof_slide:{leg}")

    if root_pos is not None and root_q is not None:
        endpoint_delta = _sub(root_pos[-1], root_pos[0])
        if locomotion and axes_ok:
            endpoint_delta = _sub(endpoint_delta, tuple(_dot(endpoint_delta, forward) * x for x in forward))
        endpoint_pos = _norm(endpoint_delta) / height
        endpoint_rot = _quat_angle_deg(root_q[0], root_q[-1])
        metrics.update({"loop_endpoint_position_ratio": endpoint_pos, "loop_endpoint_rotation_deg": endpoint_rot})
        if expected_loop and endpoint_pos > opt["max_loop_position_ratio"]: _add_failure(failures, "loop_root_position_mismatch")
        if expected_loop and endpoint_rot > opt["max_loop_rotation_deg"]: _add_failure(failures, "loop_root_rotation_mismatch")

    if len(contacts) == 4:
        all_air = [not any(contacts[leg][i] for leg in LEGS) for i in range(n)]
        max_air = 0.0
        start = None
        for i, airborne in enumerate(all_air + [False]):
            if airborne and start is None: start = i
            elif not airborne and start is not None:
                end = min(i, n - 1)
                max_air = max(max_air, times[end] - times[start])
                start = None
        metrics["max_all_hooves_airborne_fraction"] = max_air / duration
    else:
        all_air = []

    if semantic in {"walk", "trot", "canter", "gallop"} and len(phases) == 4:
        _gait_beats(semantic, phases, opt["event_sync_fraction"], opt["min_beat_gap_fraction"], failures, metrics)
        if semantic == "gallop" and metrics.get("max_all_hooves_airborne_fraction", 0.0) < opt["min_flight_fraction"]:
            _add_failure(failures, "gallop_missing_aerial_phase")

    if locomotion and motion_pos is not None and axes_ok:
        travel = _dot(_sub(motion_pos[-1], motion_pos[0]), forward) / height
        metrics["root_forward_travel_ratio"] = travel
        if semantic == "backward":
            if travel > -opt["min_root_travel_ratio"]: _add_failure(failures, "backward_not_negative_local_forward")
        elif not legacy_core14 and travel < opt["min_root_travel_ratio"]:
            _add_failure(failures, f"{action}_missing_forward_root_trajectory")

    if semantic in {"turn_left", "turn_right"} or action == "turn_around_180":
        if root_q is None or not axes_ok:
            _missing(failures, pending, "root_yaw_for_turn")
        else:
            yaw = _unwrapped_yaw_delta(root_q, forward, up, right)
            metrics["root_yaw_delta_deg"] = yaw
            wanted_sign = -1.0 if semantic == "turn_left" else 1.0
            if legacy_core14:
                if wanted_sign * yaw < opt["min_turn_yaw_deg"]:
                    _add_failure(failures, f"{semantic}_missing_signed_root_yaw")
            elif action == "turn_around_180":
                if abs(abs(yaw) - 180.0) > opt["turn_angle_tolerance_deg"]:
                    _add_failure(failures, "turn_around_180_wrong_root_yaw")
            else:
                target = -90.0 if semantic == "turn_left" else 90.0
                if abs(yaw - target) > opt["turn_angle_tolerance_deg"]:
                    _add_failure(failures, f"{action}_wrong_root_yaw")

    if semantic in {"idle", "look", "tail"} and len(all_speeds) == 4:
        moving = max((max(v) for v in all_speeds.values()), default=0.0) / height
        metrics["max_hoof_speed_ratio"] = moving
        if moving > opt["max_stationary_speed_ratio"]: _add_failure(failures, f"{semantic}_feet_not_stationary")

    if semantic == "jump":
        flight = metrics.get("max_all_hooves_airborne_fraction", 0.0)
        if flight < opt["min_flight_fraction"]: _add_failure(failures, "jump_missing_real_flight")
        if root_pos is None:
            _missing(failures, pending, "root_lift_for_jump")
        else:
            root_height = [_dot(p, normal) for p in root_pos]
            lift = (max(root_height) - min(root_height[0], root_height[-1])) / height
            metrics["root_lift_ratio"] = lift
            if lift < opt["min_jump_lift_ratio"]: _add_failure(failures, "jump_root_lift_too_small")
        if len(contacts) == 4 and (not any(any(v[:max(1, n // 4)]) for v in contacts.values()) or
                                   not any(any(v[-max(1, n // 4):]) for v in contacts.values())):
            _add_failure(failures, "jump_missing_takeoff_or_landing_contact")

    if action == "rear" and len(hoof_pos) == 4:
        fore_lift = min(max(_dot(p, normal) + offset for p in hoof_pos[leg]) for leg in ("LF", "RF")) / height
        hind_support = min(sum(contacts[leg]) / n for leg in ("LH", "RH")) if len(contacts) == 4 else 0.0
        metrics.update({"fore_hoof_lift_ratio": fore_lift, "hind_support_fraction": hind_support})
        if fore_lift < opt["min_rear_fore_lift_ratio"]: _add_failure(failures, "rear_forefeet_not_lifted")
        if hind_support < 0.25: _add_failure(failures, "rear_missing_hind_support")

    if semantic == "look":
        head = sample.get("head") if isinstance(sample.get("head"), Mapping) else {}
        head_q = _track(head.get("quaternions"), n, 4)
        if (not _quaternions_valid(head_q) or not head.get("source_track") or not axes_ok or
                (not legacy_core14 and head.get("source_observed") is not True)):
            _missing(failures, pending, "source_head_yaw_track")
        else:
            yaws = [_signed_yaw(q, forward, up, right) for q in head_q]
            yaw_range = max(yaws) - min(yaws)
            metrics["head_yaw_range_deg"] = yaw_range
            if yaw_range < opt["min_head_yaw_deg"]: _add_failure(failures, "look_missing_actual_head_yaw")

    if action == "tail":
        tail = sample.get("tail") if isinstance(sample.get("tail"), Mapping) else {}
        tail_pos = _track(tail.get("positions"), n, 3)
        if (tail_pos is None or not tail.get("source_track") or not axes_ok or
                (not legacy_core14 and tail.get("source_observed") is not True)):
            _missing(failures, pending, "source_tail_3d_track")
        else:
            span = max(_norm(_sub(p, tail_pos[0])) for p in tail_pos) / height
            metrics["tail_motion_ratio"] = span
            if span < opt["min_tail_motion_ratio"]: _add_failure(failures, "tail_motion_too_small")

    if semantic in {"start", "stop"} and root_pos is not None:
        speeds = [_norm(_sub(root_pos[i], root_pos[i - 1])) / (times[i] - times[i - 1]) / height for i in range(1, n)]
        q = max(1, len(speeds) // 4)
        early, late = sum(speeds[:q]) / q, sum(speeds[-q:]) / q
        metrics.update({"early_root_speed_ratio": early, "late_root_speed_ratio": late})
        if semantic == "start" and not (early <= opt["max_stationary_speed_ratio"] and late >= opt["min_root_travel_ratio"]):
            _add_failure(failures, "start_missing_stationary_to_motion_transition")
        if semantic == "stop" and not (early >= opt["min_root_travel_ratio"] and late <= opt["max_stationary_speed_ratio"]):
            _add_failure(failures, "stop_missing_motion_to_stationary_transition")
        contact_counts = [sum(1 for a, b in zip(contacts[leg], contacts[leg][1:]) if a != b) for leg in contacts]
        metrics["contact_transition_count"] = sum(contact_counts)
        if sum(contact_counts) == 0: _add_failure(failures, f"{semantic}_missing_contact_transition")

    # Split jump clips certify the actual boundary state, not a label.
    if action in {"jump_air", "jump_start", "jump_land"} and len(contacts) == 4:
        quarter = max(1, n // 4)
        start_contact = any(any(values[:quarter]) for values in contacts.values())
        end_contact = any(any(values[-quarter:]) for values in contacts.values())
        air_fraction = sum(not any(contacts[leg][i] for leg in LEGS) for i in range(n)) / n
        metrics.update({"start_has_contact": start_contact, "end_has_contact": end_contact,
                        "all_hooves_airborne_fraction": air_fraction})
        if action == "jump_air" and air_fraction < 0.80: _add_failure(failures, "jump_air_not_airborne")
        if action == "jump_start" and not (start_contact and not end_contact):
            _add_failure(failures, "jump_start_missing_contact_to_air_transition")
        if action == "jump_land" and not (not start_contact and end_contact):
            _add_failure(failures, "jump_land_missing_air_to_contact_transition")

    # Fall/death/recovery require observed body orientation plus root height.
    if action in {"fall", "death", "get_up"}:
        body_q = _source_track(sample, "body", n, failures, pending, quaternion=True)
        if root_pos is not None and body_q is not None:
            root_heights = [_dot(point, normal) for point in root_pos]
            height_delta = (root_heights[-1] - root_heights[0]) / height
            tilt_delta = _quat_angle_deg(body_q[0], body_q[-1])
            velocities, _ = _root_kinematics(root_pos, times, height)
            end_speed = sum(velocities[-max(1, len(velocities) // 5):]) / max(1, len(velocities) // 5)
            metrics.update({"body_height_delta_ratio": height_delta, "body_tilt_delta_deg": tilt_delta,
                            "end_root_speed_ratio": end_speed})
            if action in {"fall", "death"}:
                if height_delta > -opt["min_body_height_change_ratio"]: _add_failure(failures, f"{action}_missing_height_drop")
                if tilt_delta < opt["min_body_tilt_deg"]: _add_failure(failures, f"{action}_missing_body_tilt")
                if end_speed > opt["max_rest_speed_ratio"]: _add_failure(failures, f"{action}_not_at_rest_end")
            else:
                if height_delta < opt["min_body_height_change_ratio"]: _add_failure(failures, "get_up_missing_height_recovery")
                if tilt_delta < opt["min_body_tilt_deg"]: _add_failure(failures, "get_up_missing_body_reorientation")
                if end_speed > opt["max_rest_speed_ratio"]: _add_failure(failures, "get_up_not_stable_end")

    if action in {"attack_primary", "attack_secondary", "attack_heavy"}:
        active = _source_track(sample, "active_joint", n, failures, pending)
        channel = sample.get("active_joint") if isinstance(sample.get("active_joint"), Mapping) else {}
        role = channel.get("role")
        direction = channel.get("direction")
        if role not in {"head", "tail", "body", "hoof"}: _missing(failures, pending, "attack_active_joint_role")
        if direction not in {"forward", "backward", "left", "right", "up", "down"}:
            _missing(failures, pending, "attack_direction")
        if active is not None:
            motion = _position_motion_ratio(active, height)
            direction_axis = {"forward": forward, "backward": tuple(-v for v in forward),
                              "right": right, "left": tuple(-v for v in right),
                              "up": up, "down": tuple(-v for v in up)}.get(direction)
            directional_motion = (max(_dot(_sub(point, active[0]), direction_axis) for point in active) / height
                                  if direction_axis is not None else 0.0)
            metrics.update({"active_joint_motion_ratio": motion, "active_joint_role": role,
                            "attack_direction": direction, "active_joint_directional_motion_ratio": directional_motion})
            minimum = opt["min_active_motion_ratio"] * (1.5 if action == "attack_heavy" else 1.0)
            if motion < minimum: _add_failure(failures, f"{action}_active_motion_too_small")
            if directional_motion < minimum: _add_failure(failures, f"{action}_wrong_measured_direction")

    if action in {"hit_front", "hit_left", "hit_right"}:
        torso_q = _source_track(sample, "torso", n, failures, pending, quaternion=True)
        expected_direction = {"hit_front": "front", "hit_left": "left", "hit_right": "right"}[action]
        impulse_direction = sample.get("hit_impulse_direction")
        if (impulse_direction != expected_direction or not sample.get("hit_direction_source_track") or
                sample.get("hit_direction_source_observed") is not True):
            _missing(failures, pending, f"{action}_observed_impulse_direction")
        angular_peak = 0.0
        directional_angular_peak = 0.0
        if torso_q is not None:
            angular_peak = max(_quat_angle_deg(torso_q[i - 1], torso_q[i]) / (times[i] - times[i - 1])
                               for i in range(1, n))
            if action == "hit_front":
                pitch = [_signed_pitch(q, forward, up) for q in torso_q]
                directional_angular_peak = max(abs(b - a) / (times[i] - times[i - 1])
                                               for i, (a, b) in enumerate(zip(pitch, pitch[1:]), 1))
            else:
                yaw = [_signed_yaw(q, forward, up, right) for q in torso_q]
                rates = [_circular_yaw_delta(yaw[i - 1], yaw[i]) / (times[i] - times[i - 1]) for i in range(1, n)]
                directional_angular_peak = max(rates) if action == "hit_left" else max(-rate for rate in rates)
        acceleration_peak = 0.0
        directional_acceleration_peak = 0.0
        if root_pos is not None:
            _, acceleration = _root_kinematics(root_pos, times, height)
            acceleration_peak = max(acceleration or [0.0])
            velocities = [tuple(value / (times[i] - times[i - 1]) / height
                                for value in _sub(root_pos[i], root_pos[i - 1])) for i in range(1, n)]
            accelerations = [tuple((velocities[i][axis] - velocities[i - 1][axis]) /
                                   (times[i + 1] - times[i]) for axis in range(3))
                             for i in range(1, len(velocities))]
            reaction_axis = {"hit_front": tuple(-v for v in forward), "hit_left": right,
                             "hit_right": tuple(-v for v in right)}[action]
            directional_acceleration_peak = max([_dot(value, reaction_axis) for value in accelerations] or [0.0])
        metrics.update({"torso_angular_impulse_deg_s": angular_peak,
                        "root_linear_impulse_ratio": acceleration_peak,
                        "directional_torso_impulse_deg_s": directional_angular_peak,
                        "directional_root_impulse_ratio": directional_acceleration_peak,
                        "hit_impulse_direction": impulse_direction})
        if angular_peak < opt["min_angular_impulse_deg_s"] and acceleration_peak < opt["min_linear_impulse_ratio"]:
            _add_failure(failures, f"{action}_missing_measured_impulse")
        if (directional_angular_peak < opt["min_angular_impulse_deg_s"] and
                directional_acceleration_peak < opt["min_linear_impulse_ratio"]):
            _add_failure(failures, f"{action}_missing_directional_impulse")

    if action == "sleep_rest":
        standing = sample.get("standing_root_height")
        if not _finite_number(standing) or float(standing) <= 0:
            _missing(failures, pending, "standing_root_height")
        elif root_pos is not None:
            heights = [(_dot(point, normal) + offset) / height for point in root_pos]
            velocities, _ = _root_kinematics(root_pos, times, height)
            mean_height = sum(heights) / n
            relative_height = mean_height / (float(standing) / height)
            metrics.update({"sleep_mean_root_height_ratio": mean_height,
                            "sleep_height_vs_standing": relative_height,
                            "sleep_max_root_speed_ratio": max(velocities or [0.0])})
            if relative_height > opt["sleep_root_height_ratio"]: _add_failure(failures, "sleep_root_not_low")
            if max(velocities or [0.0]) > opt["max_rest_speed_ratio"]: _add_failure(failures, "sleep_pose_not_stable")

    if action == "eat_interact":
        head = sample.get("head") if isinstance(sample.get("head"), Mapping) else {}
        mouth = sample.get("mouth") if isinstance(sample.get("mouth"), Mapping) else {}
        head_q = _track(head.get("quaternions"), n, 4)
        if not (_quaternions_valid(head_q) and head.get("source_track") and head.get("source_observed") is True):
            head_q = None
        mouth_track = _track(mouth.get("positions"), n, 3)
        if not (mouth_track is not None and mouth.get("source_track") and mouth.get("source_observed") is True):
            mouth_track = None
        if head_q is None and mouth_track is None:
            _missing(failures, pending, "source_eat_head_or_mouth_track")
        pitch_range = 0.0
        if head_q is not None and axes_ok:
            pitches = [_signed_pitch(q, forward, up) for q in head_q]
            pitch_range = max(pitches) - min(pitches)
        mouth_motion = _position_motion_ratio(mouth_track, height) if mouth_track is not None else 0.0
        metrics.update({"eat_head_pitch_range_deg": pitch_range, "eat_mouth_motion_ratio": mouth_motion})
        if pitch_range < opt["min_head_yaw_deg"] and mouth_motion < opt["min_active_motion_ratio"]:
            _add_failure(failures, "eat_missing_head_pitch_or_mouth_motion")

    if action == "vocalize_emote":
        head = sample.get("head") if isinstance(sample.get("head"), Mapping) else {}
        mouth = sample.get("mouth") if isinstance(sample.get("mouth"), Mapping) else {}
        head_q = _track(head.get("quaternions"), n, 4)
        if not (_quaternions_valid(head_q) and head.get("source_track") and head.get("source_observed") is True):
            head_q = None
        mouth_track = _track(mouth.get("positions"), n, 3)
        if not (mouth_track is not None and mouth.get("source_track") and mouth.get("source_observed") is True):
            mouth_track = None
        if head_q is None and mouth_track is None:
            _missing(failures, pending, "source_vocalize_head_or_mouth_track")
        head_motion = _rotation_range_deg(head_q) if head_q is not None else 0.0
        mouth_motion = _position_motion_ratio(mouth_track, height) if mouth_track is not None else 0.0
        metrics.update({"vocalize_head_motion_deg": head_motion, "vocalize_mouth_motion_ratio": mouth_motion})
        if head_motion < opt["min_head_yaw_deg"] and mouth_motion < opt["min_active_motion_ratio"]:
            _add_failure(failures, "vocalize_missing_head_or_mouth_motion")

    # Named idle variants are separately measurable.  Their label alone is
    # never accepted as evidence of alert/relaxed/fidget behaviour.
    if action in {"idle_alert", "idle_relaxed", "idle_fidget"}:
        channel_name = "head" if action == "idle_alert" else "body" if action == "idle_relaxed" else "active_joint"
        observed = _source_track(sample, channel_name, n, failures, pending)
        if observed is not None:
            motion = _position_motion_ratio(observed, height)
            metrics[f"{action}_observed_motion_ratio"] = motion
            if action in {"idle_alert", "idle_fidget"} and motion < opt["min_active_motion_ratio"]:
                _add_failure(failures, f"{action}_motion_too_small")

    return {"pass": not failures and not pending, "metrics": metrics, "failures": failures, "pending": pending}


def gate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """Convert ``evaluate`` output to stable release-gate status values."""
    pending = list(report.get("pending") or [])
    failures = list(report.get("failures") or [])
    evidence_status = "PENDING" if pending else "PASS"
    semantics_status = "PENDING" if pending else ("FAIL" if failures else "PASS")
    passed = report.get("pass") is True and evidence_status == semantics_status == "PASS"
    return {"schema": "autorig.equine-motion-gates.v1", "pass": passed,
            "gates": {"measured_evidence": evidence_status, "action_semantics": semantics_status},
            "metrics": dict(report.get("metrics") or {})}


def evaluate_gates(sample: Mapping[str, Any], options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return gate_report(evaluate(sample, options))


def _numeric_metrics(value: Any, prefix: str = "") -> dict[str, float]:
    flat: dict[str, float] = {}
    if _finite_number(value):
        flat[prefix or "value"] = float(value)
    elif isinstance(value, Mapping):
        for key, child in value.items():
            flat.update(_numeric_metrics(child, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for index, child in enumerate(value):
            flat.update(_numeric_metrics(child, f"{prefix}.{index}" if prefix else str(index)))
    return flat


def to_registry_qa(report: Mapping[str, Any], action: str | None = None) -> dict[str, Any]:
    """Make registry evidence; a gate is true only after measured PASS."""
    gated = report if report.get("schema") == "autorig.equine-motion-gates.v1" else gate_report(report)
    canonical = CORE14_ALIASES.get(str(action or gated.get("metrics", {}).get("action") or ""),
                                   str(action or gated.get("metrics", {}).get("canonical_action") or ""))
    if canonical not in CANONICAL_ACTIONS:
        raise ValueError("unknown registry action")
    gate_values = gated.get("gates") or {}
    accepted = gated.get("pass") is True and gate_values and all(value == "PASS" for value in gate_values.values())
    return {"pass": bool(accepted), "gates": {name: value == "PASS" for name, value in gate_values.items()},
            "qa": _numeric_metrics(gated.get("metrics") or {}), "action": canonical}


__all__ = ["ACTION_CONTRACT", "CANONICAL_ACTIONS", "CORE14_ALIASES", "KNOWN_ACTIONS", "LEGS", "LOOPS",
           "evaluate", "evaluate_gates", "gate_report", "options_get", "to_registry_qa"]

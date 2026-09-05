"""30 Hz skeletal timelines, separate from LTX's 8n+1 latent frame counts.

Sample counts include the endpoint. A 33-sample walk has 32 unique loop
intervals and lasts 32/30 seconds. These are authoring defaults, not claims
about measured cadence or clip quality. Legacy published libraries keep
their pinned taxonomy unchanged.
"""
from __future__ import annotations

import argparse
import bisect
import copy
import hashlib
import json
import math
from pathlib import Path

GAME_FPS = 30
# (LTX output samples, skeletal samples, kind)
FRAME_BUDGET = {
    "walk_forward": (65, 33, "gait"), "walk_backward": (65, 33, "gait"),
    "trot_jog": (49, 25, "gait"), "run": (41, 21, "gait"),
    "sprint": (33, 17, "gait"), "jump_air": (49, 25, "static_loop"),
    **{name: (97, 97, "static_loop") for name in (
        "idle_neutral", "idle_alert", "idle_relaxed", "idle_look_around",
        "idle_fidget", "sleep_rest")},
    "eat_interact": (129, 129, "static_loop"),
    **{name: (33, 33, "one_shot") for name in (
        "turn_left_90", "turn_right_90", "stop_brake", "vocalize_emote")},
    **{name: (49, 49, "one_shot") for name in (
        "turn_around_180", "jump_full", "fall", "attack_primary", "attack_heavy")},
    "jump_start": (17, 17, "one_shot"),
    **{name: (25, 25, "one_shot") for name in (
        "jump_land", "hit_front", "hit_left", "hit_right")},
    "attack_secondary": (65, 65, "one_shot"),
    "death": (81, 81, "one_shot"), "get_up": (65, 65, "one_shot"),
}


def timing(action: str, samples: int | None = None) -> dict:
    generated, default_samples, kind = FRAME_BUDGET[action]
    samples = default_samples if samples is None else samples
    if isinstance(samples, bool) or not isinstance(samples, int) or not 2 <= samples <= 3601:
        raise ValueError("skeletal samples must be an integer in 2..3601")
    return {"action_id": action, "fps": GAME_FPS, "sample_count": samples,
            "interval_count": samples - 1, "duration_seconds": (samples - 1) / GAME_FPS,
            "loop": kind != "one_shot", "kind": kind,
            "generation_samples": generated, "endpoint_included": True}


def _numbers(values, field):
    if not isinstance(values, list) or any(
        isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
        for v in values
    ):
        raise ValueError(f"{field} must contain finite numbers")
    return [float(v) for v in values]


def _quat(q):
    length = math.sqrt(sum(v * v for v in q))
    if abs(length - 1) > 1e-3:
        raise ValueError("source quaternion is not normalized")
    return [v / length for v in q]


def _slerp(a, b, amount):
    dot = sum(x * y for x, y in zip(a, b))
    if dot < 0:
        b, dot = [-v for v in b], -dot
    dot = min(1.0, dot)
    if dot > 0.9995:
        q = [x + (y - x) * amount for x, y in zip(a, b)]
        norm = math.sqrt(sum(v * v for v in q))
        return [v / norm for v in q]
    theta = math.acos(dot)
    return [(math.sin((1 - amount) * theta) * x + math.sin(amount * theta) * y)
            / math.sin(theta) for x, y in zip(a, b)]


def retime_clip(clip: dict, action: str, samples: int | None = None) -> tuple[dict, dict]:
    """Resample Three tracks without changing poses or claiming QA approval.

    Input must already be a complete clip/cycle. Cycle extraction precedes
    this function. Invalid seams are reported, never silently forced closed.
    """
    contract = timing(action, samples)
    if clip.get("name") not in (action, 'Browser_' + action):
        raise ValueError("clip name must match the semantic action")
    duration = clip.get("duration")
    if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration <= 0:
        raise ValueError("source duration must be finite and positive")
    if not isinstance(clip.get("tracks"), list) or not clip["tracks"]:
        raise ValueError("source clip needs tracks")
    if clip.get('blendMode', 2500) != 2500:
        raise ValueError('only normal-blend skeletal clips are supported')
    # Retiming creates a new candidate; source approval/UUID metadata must not
    # survive on different clip bytes.
    result = {'name': action, 'duration': duration,
              'tracks': copy.deepcopy(clip['tracks']), 'blendMode': 2500}
    output_times = [i / GAME_FPS for i in range(contract["sample_count"])]
    source_times = [i * duration / contract["interval_count"] for i in range(contract["sample_count"])]
    names, seams = set(), []
    for src, dst in zip(clip["tracks"], result["tracks"]):
        name = src.get("name", "")
        prop = name.rsplit(".", 1)[-1]
        if '.' not in name or not name.rsplit('.', 1)[0] or prop not in ("position", "quaternion") or name in names:
            raise ValueError("tracks need unique bone.position or bone.quaternion bindings")
        names.add(name)
        if src.get("interpolation", 2301) != 2301:
            raise ValueError("only linear Three tracks are supported")
        if src.get("type") != ("quaternion" if prop == "quaternion" else "vector"):
            raise ValueError("track type disagrees with its binding")
        times, values = _numbers(src.get("times"), "times"), _numbers(src.get("values"), "values")
        if len(times) < 2 or times[0] != 0 or any(b <= a for a, b in zip(times, times[1:])) or abs(times[-1] - duration) > 1e-6:
            raise ValueError("each track must span the whole source duration in increasing time")
        size = 4 if prop == "quaternion" else 3
        if len(values) != size * len(times):
            raise ValueError("track value count mismatch")
        rows = [values[i:i + size] for i in range(0, len(values), size)]
        if prop == "quaternion":
            rows = [_quat(q) for q in rows]
        output = []
        for t in source_times:
            idx = min(len(times) - 2, max(0, bisect.bisect_right(times, t) - 1))
            amount = min(1.0, max(0.0, (t - times[idx]) / (times[idx + 1] - times[idx])))
            output.extend(_slerp(rows[idx], rows[idx + 1], amount) if size == 4 else
                          [a + (b - a) * amount for a, b in zip(rows[idx], rows[idx + 1])])
        dst["times"], dst["values"] = output_times, output
        if size == 4:
            seam = 2 * math.acos(min(1, abs(sum(a * b for a, b in zip(rows[0], rows[-1])))))
        else:
            seam = math.sqrt(sum((a - b) ** 2 for a, b in zip(rows[0], rows[-1])))
        seams.append({"track": name, "endpoint_error": seam,
                      "unit": "radians" if size == 4 else "source_units"})
    result["duration"] = contract["duration_seconds"]
    return result, {"schema": "autorig-game-timing-report.v1", **contract,
                    "source_duration_seconds": duration, "endpoint_errors": seams,
                    "quality_approved": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clip", type=Path, required=True)
    parser.add_argument("--action", choices=FRAME_BUDGET, required=True)
    parser.add_argument("--samples", type=int)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    data = args.clip.read_bytes()
    clip, report = retime_clip(json.loads(data), args.action, args.samples)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report["source_sha256"] = hashlib.sha256(data).hexdigest()
    output = (json.dumps(clip, allow_nan=False) + "\n").encode()
    report["clip_sha256"] = hashlib.sha256(output).hexdigest()
    (args.output_dir / "three-animation-clip.json").write_bytes(output)
    (args.output_dir / "timing-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()

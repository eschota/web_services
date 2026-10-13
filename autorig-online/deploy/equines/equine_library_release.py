#!/usr/bin/env python3
"""Fail-closed release registry for the V3 equid animation library.

This module deliberately does not post media or alter Motion Transfer.  It
turns already-produced library entries and canonical DEV verdicts into an
immutable release which a small runtime hook may resolve read-only.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
import pathlib
import re
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "autorig.equine-library-release.v1"
ACTION_MANIFEST_SCHEMA = "autorig.hoof-animation-actions.v1"
# The authoritative animal-base-30-v1 contract plus the four owner-required
# hoof actions.  (id, category, loop, frames, start_pose, end_pose)
ACTION_CONTRACT = (
 ("idle_neutral", "idle", True, 97, "default_pose", "default_pose"),
 ("idle_alert", "idle", True, 97, "default_pose", "default_pose"),
 ("idle_relaxed", "idle", True, 97, "default_pose", "default_pose"),
 ("idle_look_around", "idle", True, 97, "default_pose", "default_pose"),
 ("idle_fidget", "idle", True, 97, "default_pose", "default_pose"),
 ("walk_forward", "locomotion", True, 49, "locomotion_contact", "locomotion_contact"),
 ("walk_backward", "locomotion", True, 49, "locomotion_contact", "locomotion_contact"),
 ("trot_jog", "locomotion", True, 49, "locomotion_contact", "locomotion_contact"),
 ("run", "locomotion", True, 49, "locomotion_contact", "locomotion_contact"),
 ("sprint", "locomotion", True, 49, "locomotion_contact", "locomotion_contact"),
 ("turn_left_90", "locomotion", False, 33, "default_pose", "default_pose"),
 ("turn_right_90", "locomotion", False, 33, "default_pose", "default_pose"),
 ("turn_around_180", "locomotion", False, 33, "default_pose", "default_pose"),
 ("stop_brake", "locomotion", False, 33, "locomotion_contact", "default_pose"),
 ("jump_air", "air", True, 49, "airborne", "airborne"),
 ("fall", "air", False, 49, "airborne", "death_end"),
 ("jump_start", "air", False, 33, "default_pose", "airborne"),
 ("jump_land", "air", False, 33, "airborne", "default_pose"),
 ("jump_full", "air", False, 49, "default_pose", "default_pose"),
 ("attack_primary", "combat", False, 49, "default_pose", "default_pose"),
 ("attack_secondary", "combat", False, 49, "default_pose", "default_pose"),
 ("attack_heavy", "combat", False, 49, "default_pose", "default_pose"),
 ("hit_front", "combat", False, 33, "default_pose", "default_pose"),
 ("hit_left", "combat", False, 33, "default_pose", "default_pose"),
 ("hit_right", "combat", False, 33, "default_pose", "default_pose"),
 ("death", "combat", False, 65, "default_pose", "death_end"),
 ("get_up", "combat", False, 65, "death_end", "default_pose"),
 ("eat_interact", "behavior", True, 97, "default_pose", "default_pose"),
 ("sleep_rest", "behavior", True, 97, "default_pose", "default_pose"),
 ("vocalize_emote", "behavior", False, 33, "default_pose", "default_pose"),
 ("canter", "locomotion", True, 49, "locomotion_contact", "locomotion_contact"),
 ("start", "locomotion", False, 33, "default_pose", "locomotion_contact"),
 ("rear", "behavior", False, 49, "default_pose", "default_pose"),
 ("tail", "behavior", True, 97, "default_pose", "default_pose"),
)
BASE_ACTIONS = tuple(row[0] for row in ACTION_CONTRACT)
ACTION_BY_ID = {row[0]: row for row in ACTION_CONTRACT}
LOOP_ACTIONS = frozenset(row[0] for row in ACTION_CONTRACT if row[2])
ONE_SHOT_ACTIONS = frozenset(set(BASE_ACTIONS) - LOOP_ACTIONS)
LOCOMOTION_ACTIONS = frozenset({"walk_forward", "walk_backward", "trot_jog", "run", "sprint", "canter"})
CORE14_ALIASES = {
 "idle": "idle_neutral", "walk": "walk_forward", "trot": "trot_jog", "canter": "canter",
 "gallop": "sprint", "start": "start", "stop": "stop_brake", "turn_left": "turn_left_90",
 "turn_right": "turn_right_90", "backward": "walk_backward", "jump": "jump_full", "rear": "rear",
 "look": "idle_look_around", "tail": "tail",
}
ACTION_QA_GATES = {
    "idle_neutral": ("deformation", "grounding", "stability"),
    "idle_alert": ("deformation", "grounding", "stability"),
    "idle_relaxed": ("deformation", "grounding", "stability"),
    "idle_look_around": ("deformation", "grounding", "head_motion"),
    "idle_fidget": ("deformation", "grounding", "stability"),
    "walk_forward": ("deformation", "grounding", "root_motion"),
    "walk_backward": ("deformation", "grounding", "root_motion"),
    "trot_jog": ("deformation", "grounding", "root_motion"),
    "run": ("deformation", "grounding", "root_motion"),
    "sprint": ("deformation", "grounding", "root_motion"),
    "canter": ("deformation", "grounding", "root_motion"),
    "start": ("deformation", "grounding", "transition"),
    "stop_brake": ("deformation", "grounding", "transition"),
    "turn_left_90": ("deformation", "grounding", "turn_direction"),
    "turn_right_90": ("deformation", "grounding", "turn_direction"),
    "turn_around_180": ("deformation", "grounding", "turn_direction"),
    "jump_air": ("deformation", "airborne", "loop_seam"),
    "jump_start": ("deformation", "takeoff", "airborne"),
    "jump_land": ("deformation", "airborne", "landing"),
    "jump_full": ("deformation", "takeoff", "airborne", "landing"),
    "fall": ("deformation", "fall_motion", "death_pose"),
    "attack_primary": ("deformation", "grounding", "attack_motion"),
    "attack_secondary": ("deformation", "grounding", "attack_motion"),
    "attack_heavy": ("deformation", "grounding", "attack_motion"),
    "hit_front": ("deformation", "grounding", "hit_front_direction"),
    "hit_left": ("deformation", "grounding", "hit_left_direction"),
    "hit_right": ("deformation", "grounding", "hit_right_direction"),
    "death": ("deformation", "fall_motion", "death_pose"),
    "get_up": ("deformation", "recovery_motion", "default_pose"),
    "eat_interact": ("deformation", "grounding", "eat_head_motion"),
    "sleep_rest": ("deformation", "sleep_rest_pose", "loop_seam"),
    "vocalize_emote": ("deformation", "grounding", "vocalize_motion"),
    "rear": ("deformation", "grounding", "airborne"),
    "tail": ("deformation", "grounding", "tail_motion"),
}
PROFILE_WORDS = {
 "equid": frozenset({
    "horse", "pony", "zebra", "donkey", "mule", "foal",
    "лошадь", "лошади", "конь", "коня", "пони", "зебра", "осел", "осёл",
    "мул", "жеребенок", "жеребёнок"}),
 "cervid": frozenset({
    "deer", "buck", "elk", "moose", "reindeer", "antelope", "олень", "олени",
    "лось", "лоси", "антилопа"}),
 "camelid": frozenset({
    "camel", "dromedary", "llama", "alpaca", "верблюд", "дромадер", "лама", "альпака"}),
 "bovid": frozenset({
    "cow", "bull", "ox", "buffalo", "bison", "yak", "корова", "бык", "вол", "буйвол",
    "бизон", "як"}),
 "suid": frozenset({"boar", "pig", "кабан", "свинья", "поросенок", "поросёнок"}),
}
PROFILES = frozenset(PROFILE_WORDS)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)


class RegistryError(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def action_manifest() -> dict[str, Any]:
    manifest = {
        "schema": ACTION_MANIFEST_SCHEMA,
        "base_revision": "animal-base-30-v1",
        "actions": [
            {"id": row[0], "category": row[1], "loop": row[2], "frame_count": row[3],
             "start_pose": row[4], "end_pose": row[5]}
            for row in ACTION_CONTRACT
        ],
        "core14_aliases": dict(CORE14_ALIASES),
    }
    manifest["sha256"] = _digest(manifest)
    return manifest


def _sha(value: Any, label: str) -> str:
    text = str(value or "").lower()
    if not SHA256_RE.fullmatch(text):
        raise RegistryError(f"{label} must be a lowercase SHA-256")
    return text


def _safe_id(value: Any, label: str) -> str:
    text = str(value or "")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", text):
        raise RegistryError(f"unsafe {label}")
    return text


def _task_id(value: Any) -> str:
    text = str(value or "")
    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError) as exc:
        raise RegistryError("task_id must be a full UUID") from exc


def _root(path: os.PathLike[str] | str) -> pathlib.Path:
    # Canonicalize only after the directory exists.  On Windows, resolving a
    # not-yet-created descendant concurrently can alternate between drive and
    # volume/junction spellings and make an actually-contained child appear to
    # escape in another spawned process.
    root = pathlib.Path(os.path.abspath(os.fspath(path)))
    root.mkdir(parents=True, exist_ok=True)
    return pathlib.Path(os.path.realpath(root))


def _under(root: pathlib.Path, *parts: str) -> pathlib.Path:
    root = pathlib.Path(os.path.realpath(root))
    candidate = pathlib.Path(os.path.abspath(os.path.join(os.fspath(root), *(os.fspath(p) for p in parts))))
    root_key, candidate_key = os.path.normcase(os.fspath(root)), os.path.normcase(os.fspath(candidate))
    try:
        lexical_inside = os.path.commonpath((root_key, candidate_key)) == root_key
    except ValueError:
        lexical_inside = False
    if not lexical_inside:
        raise RegistryError("path escapes registry root")
    # Preserve fail-closed symlink/junction protection without resolving a
    # future leaf.  Check the nearest existing ancestor, or the leaf itself if
    # it already exists, against the now-existing canonical root.
    existing = candidate
    while not os.path.lexists(existing) and existing != root:
        existing = existing.parent
    existing_real = os.path.normcase(os.path.realpath(existing))
    try:
        real_inside = os.path.commonpath((root_key, existing_real)) == root_key
    except ValueError:
        real_inside = False
    if not real_inside:
        raise RegistryError("path escapes registry root through symlink or junction")
    return candidate


def _read_json(path: pathlib.Path, default: Any = None) -> Any:
    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except FileNotFoundError:
        if default is not None:
            return default
        raise RegistryError(f"missing file: {path.name}")
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryError(f"cannot read {path.name}: {exc}") from exc


def _atomic_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _canonical(value) + b"\n"
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)


@contextlib.contextmanager
def _lock(root: pathlib.Path, timeout: float = 15.0):
    lock = _under(root, ".registry.lock")
    deadline = time.monotonic() + timeout
    while True:
        try:
            fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
            os.close(fd)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise RegistryError("registry lock timeout")
            time.sleep(0.025)
    try:
        yield
    finally:
        with contextlib.suppress(FileNotFoundError):
            lock.unlink()


def required_actions(extra_actions: Iterable[str] = ()) -> tuple[str, ...]:
    if tuple(extra_actions):
        raise RegistryError("the reviewed 34-action manifest cannot be extended by a caller")
    return BASE_ACTIONS


def _numeric_tree(value: Any, label: str) -> int:
    """Require a non-empty, finite numeric QA tree; booleans are not metrics."""
    if isinstance(value, bool) or value is None:
        raise RegistryError(f"{label} must contain numeric metrics")
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise RegistryError(f"{label} contains a non-finite number")
        return 1
    if isinstance(value, Mapping):
        if not value:
            raise RegistryError(f"{label} is empty")
        return sum(_numeric_tree(v, label) for v in value.values())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if not value:
            raise RegistryError(f"{label} is empty")
        return sum(_numeric_tree(v, label) for v in value)
    raise RegistryError(f"{label} must contain only numeric metrics")


def _qa_block(value: Any, label: str, required_gates: Iterable[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or value.get("pass") is not True:
        raise RegistryError(f"{label}.pass must be true")
    if not isinstance(value.get("schema"), str) or not value["schema"].strip():
        raise RegistryError(f"{label}.schema is required")
    gates = value.get("gates")
    if not isinstance(gates, Mapping):
        raise RegistryError(f"{label}.gates is required")
    missing = [gate for gate in required_gates if gates.get(gate) is not True]
    if missing:
        raise RegistryError(f"{label} gates did not pass: {', '.join(missing)}")
    _numeric_tree(value.get("metrics"), f"{label}.metrics")
    return json.loads(_canonical(value).decode("utf-8"))


def _validate_tracks(entry: Mapping[str, Any], action: str) -> None:
    metadata = entry.get("action_metadata")
    if not isinstance(metadata, Mapping) or metadata.get("action") != action:
        raise RegistryError(f"{action}: action_metadata.action mismatch")
    if metadata.get("coordinate_system") not in {"right_handed_y_up", "left_handed_y_up", "right_handed_z_up"}:
        raise RegistryError(f"{action}: supported coordinate_system is required")
    if metadata.get("track_space") not in {"local", "model"}:
        raise RegistryError(f"{action}: track_space must be local or model")
    contract = ACTION_BY_ID[action]
    if metadata.get("category") != contract[1]:
        raise RegistryError(f"{action}: category contract mismatch")
    if metadata.get("start_pose") != contract[4] or metadata.get("end_pose") != contract[5]:
        raise RegistryError(f"{action}: start/end pose contract mismatch")
    frame_count = metadata.get("frame_count")
    fps = metadata.get("fps")
    duration = metadata.get("duration_s")
    if isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count < 2:
        raise RegistryError(f"{action}: invalid frame_count")
    if frame_count != contract[3]:
        raise RegistryError(f"{action}: frame_count contract mismatch")
    for value, label in ((fps, "fps"), (duration, "duration_s")):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)) or value <= 0:
            raise RegistryError(f"{action}: invalid {label}")
    tracks = entry.get("tracks")
    if not isinstance(tracks, Mapping) or not tracks:
        raise RegistryError(f"{action}: tracks must be a non-empty object")
    kinds = set()
    skeleton = entry.get("reference_skeleton")
    if not isinstance(skeleton, Mapping) or not isinstance(skeleton.get("bones"), list) or not skeleton["bones"]:
        raise RegistryError(f"{action}: explicit reference_skeleton is required")
    if isinstance(skeleton.get("height"), bool) or not isinstance(skeleton.get("height"), (int, float)) or not math.isfinite(float(skeleton.get("height", 0))) or skeleton.get("height", 0) <= 0:
        raise RegistryError(f"{action}: reference_skeleton.height is invalid")
    if isinstance(skeleton.get("ground_baseline"), bool) or not isinstance(skeleton.get("ground_baseline"), (int, float)) or not math.isfinite(float(skeleton.get("ground_baseline", 0))):
        raise RegistryError(f"{action}: reference_skeleton.ground_baseline is invalid")
    forward = skeleton.get("forward_axis")
    if not isinstance(forward, list) or len(forward) != 3 or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)) for v in forward) or math.sqrt(sum(float(v)**2 for v in forward)) < 1e-8:
        raise RegistryError(f"{action}: reference_skeleton.forward_axis is invalid")
    up = skeleton.get("up_axis")
    if not isinstance(up, list) or len(up) != 3 or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)) for v in up) or math.sqrt(sum(float(v)**2 for v in up)) < 1e-8:
        raise RegistryError(f"{action}: reference_skeleton.up_axis is invalid")
    bone_names = set()
    for bone in skeleton["bones"]:
        if not isinstance(bone, Mapping) or not isinstance(bone.get("name"), str) or bone["name"] in bone_names:
            raise RegistryError(f"{action}: invalid reference bone")
        bone_names.add(bone["name"])
        for field, width in (("rest_translation", 3), ("rest_rotation", 4), ("rest_scale", 3), ("head", 3), ("tail", 3)):
            value = bone.get(field)
            if not isinstance(value, list) or len(value) != width or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)) for v in value):
                raise RegistryError(f"{action}: invalid reference bone {bone['name']}.{field}")
        qnorm = math.sqrt(sum(float(v)**2 for v in bone["rest_rotation"]))
        if not 0.98 <= qnorm <= 1.02 or any(abs(float(v)) < 1e-8 for v in bone["rest_scale"]):
            raise RegistryError(f"{action}: singular/non-normalized reference rest transform")
        if math.sqrt(sum((float(a)-float(b))**2 for a, b in zip(bone["head"], bone["tail"]))) < 1e-8:
            raise RegistryError(f"{action}: singular reference bone length")
    for bone in skeleton["bones"]:
        if bone.get("parent") is not None and bone.get("parent") not in bone_names:
            raise RegistryError(f"{action}: missing reference parent")
    for name, track in tracks.items():
        if not isinstance(name, str) or not name or not isinstance(track, Mapping):
            raise RegistryError(f"{action}: invalid track")
        kind = track.get("type")
        bone_name, prop = track.get("bone"), track.get("property")
        if bone_name not in bone_names:
            raise RegistryError(f"{action}.{name}: track bone is absent from reference_skeleton")
        if (prop, kind) not in {("rotation", "quaternion"), ("translation", "translation")}:
            raise RegistryError(f"{action}.{name}: property/type contract mismatch")
        width = {"quaternion": 4, "translation": 3, "scale": 3}.get(kind)
        if width is None:
            raise RegistryError(f"{action}.{name}: unsupported track type")
        values = track.get("values")
        if not isinstance(values, list) or len(values) != frame_count:
            raise RegistryError(f"{action}.{name}: track length differs from frame_count")
        for frame in values:
            if not isinstance(frame, list) or len(frame) != width:
                raise RegistryError(f"{action}.{name}: invalid {kind} frame")
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)) for v in frame):
                raise RegistryError(f"{action}.{name}: non-finite/non-numeric track value")
            if kind == "quaternion":
                norm = math.sqrt(sum(float(v) ** 2 for v in frame))
                if not 0.98 <= norm <= 1.02:
                    raise RegistryError(f"{action}.{name}: quaternion is not normalized")
        kinds.add(kind)
    if "quaternion" not in kinds or "translation" not in kinds:
        raise RegistryError(f"{action}: tracks require quaternion and translation data")


def validate_entry(entry: Mapping[str, Any], action: str) -> dict[str, Any]:
    if not isinstance(entry, Mapping):
        raise RegistryError(f"{action}: entry must be an object")
    if entry.get("clip") != action:
        raise RegistryError(f"{action}: clip name mismatch")
    if entry.get("quality") != "ok":
        raise RegistryError(f"{action}: quality must be exactly 'ok'")
    expected_loop = action in LOOP_ACTIONS
    if entry.get("loop") is not expected_loop:
        raise RegistryError(f"{action}: loop must be {str(expected_loop).lower()}")
    expected_type = "loop" if expected_loop else "one_shot"
    if entry.get("type") != expected_type:
        raise RegistryError(f"{action}: type must be {expected_type}")
    gates = ACTION_QA_GATES.get(action, ("deformation", "grounding"))
    _qa_block(entry.get("qa"), f"{action}.qa", gates)
    if action in LOCOMOTION_ACTIONS:
        _qa_block(entry.get("gait_qa"), f"{action}.gait_qa", ("contacts", "timing"))
    _validate_tracks(entry, action)
    normalized = json.loads(_canonical(entry).decode("utf-8"))
    normalized["entry_sha256"] = _digest(normalized)
    return normalized


def create_candidate(root_path: os.PathLike[str] | str, entries: Sequence[Mapping[str, Any]],
                     corpus_tasks: Sequence[str], retarget_sha256: str,
                     extra_actions: Iterable[str] = (), profile: str = "equid") -> dict[str, Any]:
    root = _root(root_path)
    if profile not in PROFILES:
        raise RegistryError("unsupported hoof profile")
    actions = required_actions(extra_actions)
    by_clip: dict[str, Mapping[str, Any]] = {}
    for row in entries:
        clip = str(row.get("clip") or "") if isinstance(row, Mapping) else ""
        if clip in by_clip:
            raise RegistryError(f"duplicate clip: {clip}")
        by_clip[clip] = row
    missing = [action for action in actions if action not in by_clip]
    if missing:
        raise RegistryError("missing required clips: " + ", ".join(missing))
    if set(by_clip) != set(actions):
        raise RegistryError("candidate clips must exactly match required actions")
    checked = [validate_entry(by_clip[action], action) for action in actions]
    tasks = sorted({_task_id(task) for task in corpus_tasks})
    if not tasks:
        raise RegistryError("mandatory corpus_tasks must not be empty")
    candidate = {
        "schema": SCHEMA,
        "profile": profile,
        "status": "candidate",
        "required_actions": list(actions),
        "action_manifest": action_manifest(),
        "mandatory_corpus_tasks": tasks,
        "retarget_sha256": _sha(retarget_sha256, "retarget_sha256"),
        "entries": checked,
    }
    candidate["release_hash"] = _digest(candidate)
    candidate["created_unix"] = int(time.time())
    path = _under(root, "releases", candidate["release_hash"] + ".json")
    with _lock(root):
        if path.exists():
            existing = _read_json(path)
            existing_core = dict(existing)
            existing_core.pop("release_hash", None)
            existing_core.pop("created_unix", None)
            if _digest(existing_core) != candidate["release_hash"]:
                raise RegistryError("release hash collision or corrupt immutable manifest")
            return existing
        else:
            _atomic_json(path, candidate)
    return candidate


def _load_release(root: pathlib.Path, release_hash: str) -> dict[str, Any]:
    digest = _sha(release_hash, "release_hash")
    doc = _read_json(_under(root, "releases", digest + ".json"))
    stored = dict(doc)
    claimed = stored.pop("release_hash", None)
    stored.pop("created_unix", None)
    if claimed != digest or _digest(stored) != digest:
        raise RegistryError("release manifest hash mismatch")
    if doc.get("action_manifest") != action_manifest() or doc.get("required_actions") != list(BASE_ACTIONS):
        raise RegistryError("release does not bind the reviewed 34-action manifest")
    return doc


def add_evidence(root_path: os.PathLike[str] | str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    root = _root(root_path)
    task = _task_id(evidence.get("task_id"))
    release_hash = _sha(evidence.get("release_hash"), "release_hash")
    release = _load_release(root, release_hash)
    profile = release.get("profile")
    if profile not in PROFILES:
        raise RegistryError("release has unsupported hoof profile")
    if task not in release["mandatory_corpus_tasks"]:
        raise RegistryError("task is not in the release mandatory corpus")
    retarget = _sha(evidence.get("retarget_sha256"), "retarget_sha256")
    if retarget != release["retarget_sha256"]:
        raise RegistryError("stale retarget implementation evidence")
    rig = _sha(evidence.get("rig_glb_sha256"), "rig_glb_sha256")
    run_id = _safe_id(evidence.get("run_id"), "run_id")
    clips = evidence.get("clips")
    if not isinstance(clips, Mapping) or set(clips) != set(release["required_actions"]):
        raise RegistryError("evidence must contain QA for every required clip")
    checked_clips = {}
    for name in release["required_actions"]:
        row = clips[name]
        if not isinstance(row, Mapping):
            raise RegistryError(f"{name}: task QA is missing")
        _qa_block(row.get("qa"), f"{name}.qa", ACTION_QA_GATES.get(name, ("deformation", "grounding")))
        if name in LOCOMOTION_ACTIONS:
            _qa_block(row.get("gait_qa"), f"{name}.gait_qa", ("contacts", "timing"))
        checked_clips[name] = json.loads(_canonical(row).decode("utf-8"))
    receipts = evidence.get("media_receipts")
    if not isinstance(receipts, list) or not receipts:
        raise RegistryError("media_receipts must be a non-empty array")
    checked_receipts = []
    seen_uids = set()
    receipt_agents = set()
    for receipt in receipts:
        if not isinstance(receipt, Mapping):
            raise RegistryError("invalid media receipt")
        uid = str(receipt.get("uid") or "").strip()
        agent = _safe_id(receipt.get("agent"), "media receipt agent")
        sent = str(receipt.get("send_receipt") or "").strip()
        if not uid or not sent or uid in seen_uids:
            raise RegistryError("each media receipt needs unique uid and send_receipt")
        seen_uids.add(uid)
        receipt_agents.add(agent)
        checked_receipts.append({"uid": uid, "agent": agent, "send_receipt": sent})
    if len(receipt_agents) != 1:
        raise RegistryError("all media receipts for one evidence revision must use one agent")
    doc = {
        "schema": SCHEMA,
        "profile": profile,
        "task_id": task,
        "run_id": run_id,
        "rig_glb_sha256": rig,
        "release_hash": release_hash,
        "retarget_sha256": retarget,
        "clips": checked_clips,
        "media_receipts": checked_receipts,
        "created_unix": int(time.time()),
    }
    doc["evidence_id"] = _digest({k: v for k, v in doc.items() if k != "created_unix"})
    path = _under(root, "evidence", profile, task + ".json")
    with _lock(root):
        evidence_dir = _under(root, "evidence")
        if evidence_dir.exists():
            for other_path in evidence_dir.glob("*/*.json"):
                if other_path == path:
                    continue
                other = _read_json(other_path, {})
                other_uids = {r.get("uid") for r in other.get("media_receipts") or []}
                if seen_uids & other_uids:
                    raise RegistryError("media uid is already bound to another task evidence")
        current = _read_json(path, {})
        if current and current.get("release_hash") == release_hash and current.get("evidence_id") != doc["evidence_id"]:
            history = _under(root, "evidence", profile, "history", task, current["evidence_id"] + ".json")
            _atomic_json(history, current)
        _atomic_json(path, doc)
    return doc


def _verdict_rows(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("items", "messages", "inbox", "rows", "events"):
            if isinstance(payload.get(key), list):
                return [row for row in payload[key] if isinstance(row, Mapping)]
    raise RegistryError("canonical DEV response has no verdict rows")


def sync_verdicts(root_path: os.PathLike[str] | str, dev_api_url: str, agent: str,
                  opener=urllib.request.urlopen, trusted_owner_ids: Iterable[str] = ()) -> dict[str, Any]:
    parsed = urllib.parse.urlparse(dev_api_url)
    if parsed.scheme != "https" or parsed.hostname != "autorig.online" or parsed.path != "/dev/api/inbox":
        raise RegistryError("verdicts must be read from canonical https://autorig.online/dev/api/inbox")
    query = urllib.parse.parse_qs(parsed.query)
    query["agent"] = [_safe_id(agent, "agent")]
    url = urllib.parse.urlunparse(parsed._replace(query=urllib.parse.urlencode(query, doseq=True)))
    try:
        with opener(url, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise RegistryError(f"DEV inbox read failed: {exc}") from exc
    root = _root(root_path)
    expected_agent = _safe_id(agent, "agent")
    trusted = {str(value) for value in trusted_owner_ids if str(value)}
    applied = []
    with _lock(root):
        # Inbox order is chronological; a later verdict for the same media UID wins.
        decisions: dict[tuple[str, str], dict[str, Any]] = {}
        evidence_dir = _under(root, "evidence")
        evidences = []
        if evidence_dir.exists():
            for path in evidence_dir.glob("*/*.json"):
                doc = _read_json(path, {})
                if doc:
                    evidences.append(doc)
        receipt_index = {}
        for evidence in evidences:
            for receipt in evidence.get("media_receipts") or []:
                if receipt.get("agent") == expected_agent:
                    receipt_index[str(receipt.get("uid"))] = (evidence, receipt)
        rows = sorted(_verdict_rows(payload), key=lambda row: (
            str(row.get("iso") or ""), str(row.get("inbox_id") or ""), str(row.get("uid") or "")
        ))
        for row in rows:
            if row.get("author_kind") != "user" or row.get("kind") != "verdict":
                continue
            if trusted and str(row.get("user_id")) not in trusted:
                continue
            text = str(row.get("text") or "").strip().casefold()
            verdict_name = "accept" if ("принять" in text and "✅" in text) else \
                "improve" if ("улучшить" in text and "🔁" in text) else None
            about = row.get("about")
            if verdict_name is None or not isinstance(about, Mapping) or about.get("agent") != expected_agent:
                continue
            uid = str(about.get("uid") or "")
            indexed = receipt_index.get(uid)
            if indexed is None:
                continue
            evidence, receipt = indexed
            try:
                task = _task_id(evidence.get("task_id"))
                inbox_id = str(row.get("inbox_id") or "").strip()
                verdict_uid = str(row.get("uid") or "").strip()
                if not inbox_id or not verdict_uid:
                    continue
                decisions[(task, uid)] = {
                    "task_id": task,
                    "profile": evidence["profile"],
                    "evidence_id": evidence["evidence_id"],
                    "release_hash": evidence["release_hash"],
                    "rig_glb_sha256": evidence["rig_glb_sha256"],
                    "media_uid": uid,
                    "send_receipt": receipt["send_receipt"],
                    "verdict_receipt": {"inbox_id": inbox_id, "uid": verdict_uid},
                    "owner_user_id": str(row.get("user_id") or ""),
                    "owner_verdict": verdict_name,
                    "canonical_source": url,
                    "synced_unix": int(time.time()),
                }
            except RegistryError:
                continue
        by_task: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for (task, _uid), decision in decisions.items():
            by_task.setdefault((decision["profile"], task), []).append(decision)
        for (profile, task), task_decisions in by_task.items():
            evidence = _read_json(_under(root, "evidence", profile, task + ".json"))
            expected_uids = {r["uid"] for r in evidence["media_receipts"] if r["agent"] == expected_agent}
            decision_uids = {v["media_uid"] for v in task_decisions}
            if expected_uids and decision_uids <= expected_uids:
                doc = {"task_id": task, "profile": profile, "evidence_id": evidence["evidence_id"],
                       "release_hash": evidence["release_hash"], "agent": expected_agent,
                       "media_verdicts": sorted(task_decisions, key=lambda v: v["media_uid"])}
                _atomic_json(_under(root, "verdicts", profile, task + ".json"), doc)
                applied.append(task)
    return {"synced_tasks": sorted(set(applied)), "count": len(set(applied))}


def promotion_plan(root_path: os.PathLike[str] | str, release_hash: str) -> dict[str, Any]:
    root = _root(root_path)
    release = _load_release(root, release_hash)
    profile = release["profile"]
    tasks = []
    ready = True
    for task in release["mandatory_corpus_tasks"]:
        problems = []
        evidence = _read_json(_under(root, "evidence", profile, task + ".json"), {})
        verdict = _read_json(_under(root, "verdicts", profile, task + ".json"), {})
        if evidence.get("release_hash") != release_hash:
            problems.append("missing_or_stale_evidence")
        if evidence.get("retarget_sha256") != release["retarget_sha256"]:
            problems.append("stale_retarget")
        if verdict.get("evidence_id") != evidence.get("evidence_id"):
            problems.append("missing_or_stale_verdict")
        expected = {row["uid"] for row in evidence.get("media_receipts") or []}
        accepted = {row.get("media_uid") for row in verdict.get("media_verdicts") or []
                    if row.get("owner_verdict") == "accept" and row.get("verdict_receipt", {}).get("inbox_id")
                    and row.get("verdict_receipt", {}).get("uid")}
        if expected != accepted:
            problems.append("all_media_not_accepted")
        tasks.append({"task_id": task, "ready": not problems, "problems": problems})
        ready = ready and not problems
    return {"release_hash": release_hash, "ready": ready, "tasks": tasks}


def promote(root_path: os.PathLike[str] | str, release_hash: str) -> dict[str, Any]:
    root = _root(root_path)
    with _lock(root):
        release = _load_release(root, release_hash)
        plan = promotion_plan(root, release_hash)
        if not plan["ready"]:
            raise RegistryError("promotion blocked: mandatory task approvals/QA are incomplete")
        profile = release["profile"]
        pointer_path = _under(root, "profiles", profile, "active.json")
        old = _read_json(pointer_path, {})
        pointer = {
            "schema": SCHEMA,
            "profile": profile,
            "active_release_hash": release_hash,
            "previous_release_hash": old.get("active_release_hash"),
            "retarget_sha256": release["retarget_sha256"],
            "promoted_unix": int(time.time()),
        }
        if old:
            _atomic_json(_under(root, "profiles", profile, "pointers", f"active-{int(time.time_ns())}.json"), old)
        _atomic_json(pointer_path, pointer)
        return pointer


def classify_profile(text: str) -> str | None:
    words = {token.casefold() for token in TOKEN_RE.findall(str(text or ""))}
    matches = [profile for profile, vocabulary in PROFILE_WORDS.items() if words & vocabulary]
    return matches[0] if len(matches) == 1 else None


def resolve_runtime(root_path: os.PathLike[str] | str, classification: str,
                    retarget_sha256: str | None = None) -> list[dict[str, Any]]:
    """Return accepted immutable equid entries; return [] for every other class."""
    profile = classify_profile(classification)
    if profile is None:
        return []
    root = pathlib.Path(root_path).resolve()
    pointer = _read_json(_under(root, "profiles", profile, "active.json"), {})
    release_hash = pointer.get("active_release_hash")
    if not release_hash:
        return []
    release = _load_release(root, release_hash)
    if release.get("profile") != profile:
        return []
    if retarget_sha256 is not None and _sha(retarget_sha256, "retarget_sha256") != release["retarget_sha256"]:
        return []
    if promotion_plan(root, release_hash)["ready"] is not True:
        return []
    return json.loads(_canonical(release["entries"]).decode("utf-8"))


def status(root_path: os.PathLike[str] | str) -> dict[str, Any]:
    root = _root(root_path)
    pointers = {profile: _read_json(_under(root, "profiles", profile, "active.json"), {})
                for profile in sorted(PROFILES)}
    releases = sorted(path.stem for path in _under(root, "releases").glob("*.json")) if _under(root, "releases").exists() else []
    return {"active_by_profile": pointers, "releases": releases,
            "action_manifest": action_manifest()}


def _json_arg(path: str) -> Any:
    return _read_json(pathlib.Path(path).resolve())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, help="explicit registry state root")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    create = sub.add_parser("create")
    create.add_argument("--entries", required=True)
    create.add_argument("--corpus-tasks", required=True)
    create.add_argument("--retarget-sha256", required=True)
    create.add_argument("--profile", choices=sorted(PROFILES), default="equid")
    evidence = sub.add_parser("evidence")
    evidence.add_argument("--file", required=True)
    sync = sub.add_parser("sync-verdicts")
    sync.add_argument("--dev-api", default="https://autorig.online/dev/api/inbox")
    sync.add_argument("--agent", required=True)
    sync.add_argument("--trusted-owner-id", action="append", default=[])
    plan = sub.add_parser("plan")
    plan.add_argument("release_hash")
    promotion = sub.add_parser("promote")
    promotion.add_argument("release_hash")
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            result = status(args.root)
        elif args.command == "create":
            result = create_candidate(args.root, _json_arg(args.entries), _json_arg(args.corpus_tasks),
                                      args.retarget_sha256, profile=args.profile)
        elif args.command == "evidence":
            result = add_evidence(args.root, _json_arg(args.file))
        elif args.command == "sync-verdicts":
            result = sync_verdicts(args.root, args.dev_api, args.agent,
                                   trusted_owner_ids=args.trusted_owner_id)
        elif args.command == "plan":
            result = promotion_plan(args.root, args.release_hash)
        else:
            result = promote(args.root, args.release_hash)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except RegistryError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

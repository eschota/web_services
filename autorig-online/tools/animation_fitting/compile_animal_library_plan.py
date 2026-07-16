"""Compile an immutable, dry-run 30-action animal animation library plan.

This module deliberately has no application, database, Comfy, network, or
Blender imports.  Every source file is supplied explicitly with an expected
SHA-256, read exactly once, and parsed from those pinned bytes.  The only
mutation it can perform is publishing one new plan file atomically without
overwriting an existing path.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping, Sequence
import uuid


PLAN_SCHEMA = "autorig.animal-animation-library-plan.v1"
PLAN_VERSION = 1
CANDIDATES_PER_ACTION = 8
EXPECTED_TAXONOMY_SCHEMA = "animal-animation-taxonomy.v1"
EXPECTED_TAXONOMY_REVISION = "animal-base-30-v1"
EXPECTED_PROMPT_SCHEMA = "autorig.animation-fitting-prompts.v1"
EXPECTED_BUNDLE_SCHEMA = "autorig-actionless-fitting-bundle.v1"
EXPECTED_IMMUTABLE_MANIFEST_SCHEMA = "autorig-fitting-immutable-copy.v1"
NATIVE_IMMUTABLE_MANIFEST_SCHEMA = "autorig-fitting-immutable-bundle.v1"
SUPPORTED_IMMUTABLE_MANIFEST_SCHEMAS = {
    EXPECTED_IMMUTABLE_MANIFEST_SCHEMA,
    NATIVE_IMMUTABLE_MANIFEST_SCHEMA,
}
EXPECTED_RIG_TYPES = (
    "dog",
    "bear",
    "cat",
    "cow",
    "deer",
    "elephant",
    "giraffe",
    "horse",
    "mouse",
    "pig",
    "rabbit",
    "turtle",
)
# Independent semantic authority.  A caller may repin reviewed input bytes, but
# mutually changed taxonomy/prompt documents must not redefine this v1 release
# contract without a code review and schema/version change here.
# (id, category, loop, frame_count, start_pose_id, end_pose_id)
AUTHORITATIVE_ACTION_CONTRACT = (
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
    ("fall", "air", True, 49, "airborne", "airborne"),
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
)
EXPECTED_ACTION_IDS = tuple(row[0] for row in AUTHORITATIVE_ACTION_CONTRACT)
EXPECTED_POSE_IDS = ("default_pose", "locomotion_contact", "airborne", "death_end")
ALLOWED_FRAME_COUNTS = (33, 49, 65, 97)
WORKFLOW_NAMES = {
    "loop": "autorig_ltx2_animal_loop_v1_api.json",
    "one_shot": "autorig_ltx2_animal_one_shot_v1_api.json",
}
HORSE_PRIORITY_ACTIONS = (
    "idle_neutral",
    "walk_forward",
    "trot_jog",
    "run",
    "jump_full",
    "attack_primary",
    "death",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_REVISION_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
SAFE_SPECIES_RE = re.compile(r"^[A-Za-z][A-Za-z0-9 ._-]{0,79}$")


class PlanCompileError(ValueError):
    """Raised when a pinned input or semantic contract fails closed."""


@dataclass(frozen=True)
class PinnedBytes:
    role: str
    path: Path
    sha256: str
    size_bytes: int
    data: bytes

    def public_pin(self) -> dict[str, Any]:
        return {"sha256": self.sha256, "size_bytes": self.size_bytes}


@dataclass(frozen=True)
class CompileResult:
    output_path: Path
    output_sha256: str
    output_size_bytes: int
    plan_identity_sha256: str
    job_count: int
    candidate_count: int


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PlanCompileError(f"Plan contains a non-canonical JSON value: {exc}") from exc
    return encoded


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _require_sha256(value: str, role: str) -> str:
    digest = str(value or "").strip()
    if not SHA256_RE.fullmatch(digest):
        raise PlanCompileError(f"{role} SHA-256 must be exactly 64 lowercase hexadecimal characters")
    return digest


def _read_pinned(path_value: str | os.PathLike[str], expected_sha256: str, role: str) -> PinnedBytes:
    expected = _require_sha256(expected_sha256, role)
    try:
        path = Path(path_value).resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise PlanCompileError(f"{role} input is unavailable: {path_value}") from exc
    if not path.is_file():
        raise PlanCompileError(f"{role} input is not a regular file: {path}")
    try:
        # One open and one read by contract.  All parsing below uses this buffer.
        with open(path, "rb") as stream:
            data = stream.read()
    except OSError as exc:
        raise PlanCompileError(f"Could not read {role} input: {path}") from exc
    if not data:
        raise PlanCompileError(f"{role} input is empty: {path}")
    actual = _sha256(data)
    if actual != expected:
        raise PlanCompileError(
            f"{role} SHA-256 mismatch: expected {expected}, got {actual}"
        )
    return PinnedBytes(role=role, path=path, sha256=actual, size_bytes=len(data), data=data)


def _reject_duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise PlanCompileError(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise PlanCompileError(f"JSON contains non-finite constant {value}")


def _json_object(source: PinnedBytes) -> dict[str, Any]:
    try:
        text = source.data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise PlanCompileError(f"{source.role} must be UTF-8 JSON") from exc
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except PlanCompileError:
        raise
    except json.JSONDecodeError as exc:
        raise PlanCompileError(f"{source.role} contains invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise PlanCompileError(f"{source.role} must contain a JSON object")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise PlanCompileError(f"{label} fields drifted ({'; '.join(details)})")


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PlanCompileError(f"{label} must be a positive integer")
    return value


def _single_line(value: Any, label: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise PlanCompileError(f"{label} must be a string")
    normalized = re.sub(r"\s+", " ", value.strip())
    if not normalized or len(normalized) > max_length:
        raise PlanCompileError(f"{label} must contain 1..{max_length} normalized characters")
    return normalized


def _validate_taxonomy(value: dict[str, Any], rig_type: str) -> tuple[list[dict[str, Any]], int, int]:
    _exact_keys(
        value,
        {
            "schema",
            "revision",
            "source_fps",
            "output_fps",
            "rig_types",
            "orientations",
            "poses",
            "clips",
        },
        "taxonomy",
    )
    if value.get("schema") != EXPECTED_TAXONOMY_SCHEMA:
        raise PlanCompileError("Unsupported animal animation taxonomy schema")
    if value.get("revision") != EXPECTED_TAXONOMY_REVISION:
        raise PlanCompileError("Animal animation taxonomy revision drifted")
    if tuple(value.get("rig_types") or ()) != EXPECTED_RIG_TYPES:
        raise PlanCompileError("Animal animation rig taxonomy drifted")
    if rig_type not in EXPECTED_RIG_TYPES:
        raise PlanCompileError(f"Unsupported animal rig type: {rig_type}")
    if value.get("orientations") != ["front", "back"]:
        raise PlanCompileError("Animal animation orientations must be exactly front/back")
    poses = value.get("poses")
    if not isinstance(poses, list) or len(poses) != 4:
        raise PlanCompileError("Animal animation taxonomy must contain its four canonical poses")
    pose_ids = [row.get("id") for row in poses if isinstance(row, dict)]
    if tuple(pose_ids) != EXPECTED_POSE_IDS:
        raise PlanCompileError("Animal animation taxonomy pose IDs/order drifted")

    source_fps = _positive_int(value.get("source_fps"), "taxonomy.source_fps")
    output_fps = _positive_int(value.get("output_fps"), "taxonomy.output_fps")
    clips = value.get("clips")
    if not isinstance(clips, list) or len(clips) != len(EXPECTED_ACTION_IDS):
        raise PlanCompileError("Animal animation taxonomy must contain exactly 30 clips")
    ids: list[str] = []
    parsed: list[dict[str, Any]] = []
    expected_clip_keys = {
        "id",
        "category",
        "order",
        "loop",
        "frame_profile",
        "start_pose_id",
        "end_pose_id",
        "legacy_aliases",
    }
    for index, raw in enumerate(clips):
        if not isinstance(raw, dict):
            raise PlanCompileError(f"taxonomy.clips[{index}] must be an object")
        _exact_keys(raw, expected_clip_keys, f"taxonomy.clips[{index}]")
        action_id = _single_line(raw.get("id"), f"taxonomy.clips[{index}].id", max_length=80)
        (
            expected_id,
            expected_category,
            expected_loop,
            expected_frame_count,
            expected_start_pose,
            expected_end_pose,
        ) = AUTHORITATIVE_ACTION_CONTRACT[index]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", action_id):
            raise PlanCompileError(f"Invalid canonical action ID: {action_id}")
        if action_id != expected_id:
            raise PlanCompileError(
                f"Canonical semantic ID/order drifted at position {index + 1}: "
                f"expected {expected_id}, got {action_id}"
            )
        if raw.get("order") != index + 1:
            raise PlanCompileError(f"Canonical action order drifted at {action_id}")
        if not isinstance(raw.get("loop"), bool):
            raise PlanCompileError(f"taxonomy clip {action_id}.loop must be boolean")
        if raw["loop"] is not expected_loop:
            raise PlanCompileError(f"{action_id} loop contract drifted")
        frame_count = _positive_int(raw.get("frame_profile"), f"{action_id}.frame_profile")
        if frame_count not in ALLOWED_FRAME_COUNTS or (frame_count - 1) % 8 != 0:
            raise PlanCompileError(
                f"{action_id} frame profile must be one of {ALLOWED_FRAME_COUNTS} and satisfy 8n+1"
            )
        if frame_count != expected_frame_count:
            raise PlanCompileError(f"{action_id} frame-count contract drifted")
        start_pose = raw.get("start_pose_id")
        end_pose = raw.get("end_pose_id")
        if start_pose not in pose_ids or end_pose not in pose_ids:
            raise PlanCompileError(f"{action_id} references a non-canonical pose")
        if start_pose != expected_start_pose or end_pose != expected_end_pose:
            raise PlanCompileError(f"{action_id} start/end pose contract drifted")
        if not isinstance(raw.get("category"), str) or not raw["category"]:
            raise PlanCompileError(f"{action_id} category is invalid")
        if raw["category"] != expected_category:
            raise PlanCompileError(f"{action_id} category contract drifted")
        aliases = raw.get("legacy_aliases")
        if not isinstance(aliases, list) or not all(isinstance(item, str) and item for item in aliases):
            raise PlanCompileError(f"{action_id} legacy aliases are invalid")
        ids.append(action_id)
        parsed.append(dict(raw))
    if tuple(ids) != EXPECTED_ACTION_IDS:
        raise PlanCompileError("Canonical 30-action IDs/order drifted")
    if sum(1 for row in parsed if row["loop"]) != 14 or sum(1 for row in parsed if not row["loop"]) != 16:
        raise PlanCompileError("Canonical action modes must be exactly 14 loop and 16 one-shot")
    return parsed, source_fps, output_fps


def _validate_prompts(
    value: dict[str, Any],
    clips: Sequence[dict[str, Any]],
    source_fps: int,
    output_fps: int,
) -> tuple[dict[str, dict[str, Any]], str, str, str, str]:
    _exact_keys(
        value,
        {
            "schema",
            "taxonomy_schema",
            "input_fps_int",
            "output_fps_int",
            "frame_rule_string",
            "common_positive_prefix_string",
            "loop_instruction_string",
            "one_shot_instruction_string",
            "common_negative_prompt_string",
            "actions_array",
        },
        "prompt specification",
    )
    if value.get("schema") != EXPECTED_PROMPT_SCHEMA:
        raise PlanCompileError("Unsupported action prompt schema")
    if value.get("taxonomy_schema") != EXPECTED_TAXONOMY_SCHEMA:
        raise PlanCompileError("Prompt taxonomy schema disagrees with the canonical taxonomy")
    if value.get("input_fps_int") != source_fps or value.get("output_fps_int") != output_fps:
        raise PlanCompileError("Prompt FPS contract disagrees with the canonical taxonomy")
    if value.get("frame_rule_string") != "8n+1":
        raise PlanCompileError("Prompt frame rule must be exactly 8n+1")
    prefix = _single_line(
        value.get("common_positive_prefix_string"),
        "common_positive_prefix_string",
        max_length=4000,
    )
    loop_instruction = _single_line(
        value.get("loop_instruction_string"),
        "loop_instruction_string",
        max_length=3000,
    )
    one_shot_instruction = _single_line(
        value.get("one_shot_instruction_string"),
        "one_shot_instruction_string",
        max_length=3000,
    )
    negative = _single_line(
        value.get("common_negative_prompt_string"),
        "common_negative_prompt_string",
        max_length=4000,
    )
    actions = value.get("actions_array")
    if not isinstance(actions, list) or len(actions) != len(clips):
        raise PlanCompileError("Prompt specification must contain exactly 30 actions")
    expected_action_keys = {
        "action_id_string",
        "family_string",
        "generation_mode_string",
        "frame_count_int",
        "motion_prompt_string",
    }
    result: dict[str, dict[str, Any]] = {}
    for index, (raw, clip) in enumerate(zip(actions, clips)):
        if not isinstance(raw, dict):
            raise PlanCompileError(f"actions_array[{index}] must be an object")
        _exact_keys(raw, expected_action_keys, f"actions_array[{index}]")
        action_id = raw.get("action_id_string")
        if action_id != clip["id"]:
            raise PlanCompileError("Prompt action order must exactly match the canonical taxonomy")
        expected_mode = "loop" if clip["loop"] else "one_shot"
        if raw.get("generation_mode_string") != expected_mode:
            raise PlanCompileError(f"{action_id} prompt generation mode drifted")
        if raw.get("frame_count_int") != clip["frame_profile"]:
            raise PlanCompileError(f"{action_id} prompt frame count drifted")
        family = _single_line(raw.get("family_string"), f"{action_id}.family", max_length=80)
        motion = _single_line(
            raw.get("motion_prompt_string"), f"{action_id}.motion_prompt", max_length=3000
        )
        result[action_id] = {
            "family": family,
            "mode": expected_mode,
            "frame_count": clip["frame_profile"],
            "motion": motion,
        }
    if tuple(result) != EXPECTED_ACTION_IDS:
        raise PlanCompileError("Prompt action IDs/order drifted")
    return result, prefix, loop_instruction, one_shot_instruction, negative


def _workflow_nodes(value: dict[str, Any], role: str) -> dict[str, dict[str, Any]]:
    if "nodes" in value and isinstance(value.get("nodes"), list):
        raise PlanCompileError(f"{role} workflow is a UI workflow, not an API prompt")
    titles: dict[str, dict[str, Any]] = {}
    for node_id, node in value.items():
        if not isinstance(node_id, str) or not isinstance(node, dict):
            raise PlanCompileError(f"{role} workflow contains an invalid API node")
        meta = node.get("_meta")
        inputs = node.get("inputs")
        class_type = node.get("class_type")
        if not isinstance(meta, dict) or not isinstance(meta.get("title"), str):
            raise PlanCompileError(f"{role} workflow node {node_id} has no title")
        if not isinstance(inputs, dict) or not isinstance(class_type, str) or not class_type:
            raise PlanCompileError(f"{role} workflow node {node_id} is malformed")
        title = meta["title"]
        if title in titles:
            raise PlanCompileError(f"{role} workflow has duplicate node title {title}")
        titles[title] = node
    return titles


def _require_workflow_node(
    titles: Mapping[str, dict[str, Any]], title: str, class_type: str, mode: str
) -> dict[str, Any]:
    node = titles.get(title)
    if not isinstance(node, dict) or node.get("class_type") != class_type:
        raise PlanCompileError(f"{mode} workflow must contain {title} as {class_type}")
    return node


def _numeric_equal(value: Any, expected: int | float) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and float(value) == float(expected)


def _validate_workflow(
    value: dict[str, Any],
    mode: str,
    source_fps: int,
    output_fps: int,
) -> str:
    titles = _workflow_nodes(value, mode)
    expected_common = {
        "AUTORIG_INPUT_CONDITIONING": "LTXVConditioning",
        "AUTORIG_START_FRAME": "LoadImage",
        "AUTORIG_POSITIVE_PROMPT": "CLIPTextEncode",
        "AUTORIG_NEGATIVE_PROMPT": "CLIPTextEncode",
        "AUTORIG_VIDEO_LATENT": "EmptyLTXVLatentVideo",
        "AUTORIG_AUDIO_LATENT": "LTXVEmptyLatentAudio",
        "AUTORIG_SEED": "RandomNoise",
        "AUTORIG_OUTPUT_VIDEO": "CreateVideo",
        "AUTORIG_OUTPUT": "SaveVideo",
        "AUTORIG_START_GUIDE": "LTXVAddGuide",
        "AUTORIG_CROP_GUIDE_LATENTS": "LTXVCropGuides",
    }
    for title, class_type in expected_common.items():
        _require_workflow_node(titles, title, class_type, mode)
    conditioning = titles["AUTORIG_INPUT_CONDITIONING"]["inputs"]
    audio = titles["AUTORIG_AUDIO_LATENT"]["inputs"]
    output_video = titles["AUTORIG_OUTPUT_VIDEO"]["inputs"]
    video_latent = titles["AUTORIG_VIDEO_LATENT"]["inputs"]
    if not _numeric_equal(conditioning.get("frame_rate"), source_fps):
        raise PlanCompileError(f"{mode} workflow input FPS drifted")
    if not _numeric_equal(audio.get("frame_rate"), source_fps):
        raise PlanCompileError(f"{mode} workflow audio FPS drifted")
    if not _numeric_equal(output_video.get("fps"), output_fps):
        raise PlanCompileError(f"{mode} workflow output FPS drifted")
    latent_length = video_latent.get("length")
    if isinstance(latent_length, bool) or not isinstance(latent_length, int) or (latent_length - 1) % 8:
        raise PlanCompileError(f"{mode} workflow latent length must satisfy 8n+1")
    if audio.get("frames_number") != latent_length:
        raise PlanCompileError(f"{mode} workflow audio/video base frame lengths disagree")
    output = titles["AUTORIG_OUTPUT"]["inputs"]
    if output.get("format") != "mp4" or output.get("codec") != "h264":
        raise PlanCompileError(f"{mode} workflow output must be H.264 MP4")
    start_guide = titles["AUTORIG_START_GUIDE"]
    if not _numeric_equal(start_guide["inputs"].get("frame_idx"), 0):
        raise PlanCompileError(f"{mode} workflow start guide must target frame 0")
    guide_nodes = [node for node in value.values() if node.get("class_type") == "LTXVAddGuide"]
    crop_nodes = [node for node in value.values() if node.get("class_type") == "LTXVCropGuides"]
    if len(crop_nodes) != 1:
        raise PlanCompileError(f"{mode} workflow must contain exactly one post-sampling guide crop")
    if mode == "loop":
        if len(guide_nodes) != 2:
            raise PlanCompileError("Loop workflow must contain exactly start and end guides")
        end_guide = _require_workflow_node(
            titles, "AUTORIG_END_GUIDE_N_MINUS_1", "LTXVAddGuide", mode
        )
        if not _numeric_equal(end_guide["inputs"].get("frame_idx"), -1):
            raise PlanCompileError("Loop workflow end guide must target N-1")
        if end_guide["inputs"].get("image") != start_guide["inputs"].get("image"):
            raise PlanCompileError("Loop workflow end guide must reuse the exact start image")
    elif mode == "one_shot":
        if len(guide_nodes) != 1 or any(title.startswith("AUTORIG_END_GUIDE") for title in titles):
            raise PlanCompileError("One-shot workflow must contain only start-frame conditioning")
    else:
        raise PlanCompileError(f"Unsupported workflow mode: {mode}")
    return _sha256(_canonical_json_bytes(value))


def _validate_source_contract(
    immutable_manifest_value: dict[str, Any],
    immutable_manifest: PinnedBytes,
    value: dict[str, Any],
    bundle: PinnedBytes,
    skeleton: PinnedBytes,
    source_model_sha256: str,
    rig_type: str,
    species: str,
) -> dict[str, Any]:
    manifest_schema = immutable_manifest_value.get("schema")
    if manifest_schema not in SUPPORTED_IMMUTABLE_MANIFEST_SCHEMAS:
        raise PlanCompileError("Unsupported immutable manifest schema")
    manifest_bundle = immutable_manifest_value.get("bundle_manifest")
    manifest_files = immutable_manifest_value.get("files")
    native_manifest = manifest_schema == NATIVE_IMMUTABLE_MANIFEST_SCHEMA
    if not isinstance(manifest_bundle, dict):
        raise PlanCompileError("Immutable manifest fitting bundle pin is missing")
    if native_manifest:
        _exact_keys(
            immutable_manifest_value,
            {
                "schema",
                "revision",
                "bundle_file_count",
                "bundle_total_bytes",
                "bundle_manifest",
                "files",
            },
            "native immutable manifest",
        )
        _exact_keys(
            manifest_bundle,
            {"filename", "bytes", "sha256"},
            "native immutable manifest bundle_manifest",
        )
    else:
        manifest_model = immutable_manifest_value.get("source_model")
        if not isinstance(manifest_model, dict):
            raise PlanCompileError("Immutable manifest source model/bundle pins are missing")
        if manifest_model.get("sha256") != source_model_sha256:
            raise PlanCompileError(
                "Immutable manifest source model SHA-256 does not match the external pin"
            )
        if manifest_model.get("copied") is not False:
            raise PlanCompileError("Immutable manifest must declare source_model.copied=false")
    if manifest_bundle.get("sha256") != bundle.sha256:
        raise PlanCompileError("Immutable manifest fitting bundle SHA-256 does not match the external pin")
    if not isinstance(manifest_files, list) or not manifest_files:
        raise PlanCompileError("Immutable manifest files inventory is missing")
    file_rows: dict[str, dict[str, Any]] = {}
    casefolded_filenames: set[str] = set()
    for index, row in enumerate(manifest_files):
        if not isinstance(row, dict):
            raise PlanCompileError(f"Immutable manifest files[{index}] is invalid")
        if native_manifest:
            _exact_keys(
                row,
                {"filename", "bytes", "sha256"},
                f"native immutable manifest files[{index}]",
            )
        filename = row.get("filename")
        identity = filename.casefold() if isinstance(filename, str) else ""
        if (
            not isinstance(filename, str)
            or not filename
            or filename in file_rows
            or (native_manifest and identity in casefolded_filenames)
        ):
            raise PlanCompileError("Immutable manifest filenames must be unique non-empty strings")
        digest = row.get("sha256")
        if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
            raise PlanCompileError(f"Immutable manifest file {filename} has an invalid SHA-256")
        _positive_int(row.get("bytes"), f"immutable manifest {filename}.bytes")
        file_rows[filename] = row
        casefolded_filenames.add(identity)
    if immutable_manifest_value.get("bundle_file_count") != len(manifest_files):
        raise PlanCompileError("Immutable manifest bundle_file_count disagrees with its inventory")
    if immutable_manifest_value.get("bundle_total_bytes") != sum(row["bytes"] for row in manifest_files):
        raise PlanCompileError("Immutable manifest bundle_total_bytes disagrees with its inventory")
    manifest_bundle_filename = str(manifest_bundle.get("filename") or "")
    if native_manifest:
        if manifest_bundle_filename != "fitting_bundle.json":
            raise PlanCompileError(
                "Native immutable manifest bundle_manifest filename must be fitting_bundle.json"
            )
        if manifest_bundle.get("bytes") != bundle.size_bytes:
            raise PlanCompileError(
                "Native immutable manifest fitting bundle byte size does not match pinned bytes"
            )
    bundle_row = file_rows.get(manifest_bundle_filename)
    if not bundle_row or bundle_row.get("sha256") != bundle.sha256 or bundle_row.get("bytes") != bundle.size_bytes:
        raise PlanCompileError("Immutable manifest fitting bundle inventory row does not match pinned bytes")

    if value.get("schema") != EXPECTED_BUNDLE_SCHEMA:
        raise PlanCompileError("Unsupported immutable fitting bundle schema")
    actionless = value.get("actionless")
    if not isinstance(actionless, dict) or actionless.get("actionless") is not True:
        raise PlanCompileError("Fitting bundle must be explicitly actionless")
    source = value.get("source")
    artifacts = value.get("artifacts")
    if not isinstance(source, dict) or not isinstance(artifacts, dict):
        raise PlanCompileError("Fitting bundle source/artifacts contract is missing")
    source_filename = _single_line(source.get("filename"), "bundle.source.filename", max_length=255)
    if source.get("sha256") != source_model_sha256:
        raise PlanCompileError("Fitting bundle source model SHA-256 does not match the external pin")
    bundle_species = _single_line(source.get("species"), "bundle.source.species", max_length=80)
    if bundle_species.casefold() != species.casefold() or bundle_species.casefold() != rig_type.casefold():
        raise PlanCompileError("Fitting bundle species does not match the requested rig/species")
    if source.get("orientation") != "canonical":
        raise PlanCompileError("Fitting bundle source orientation must be canonical")
    source_rig_type = _single_line(source.get("rig_type"), "bundle.source.rig_type", max_length=80)
    if native_manifest:
        bundle_revision = _single_line(
            value.get("revision"), "fitting bundle revision", max_length=128
        )
        manifest_revision = _single_line(
            immutable_manifest_value.get("revision"),
            "native immutable manifest revision",
            max_length=128,
        )
        if manifest_revision != bundle_revision:
            raise PlanCompileError(
                "Native immutable manifest revision does not match the fitting bundle revision"
            )

        artifact_filenames: set[str] = set()
        artifact_filename_identities: set[str] = set()
        for artifact_name, artifact_pin in artifacts.items():
            if not isinstance(artifact_name, str) or not artifact_name:
                raise PlanCompileError("Fitting bundle artifact names must be non-empty strings")
            if not isinstance(artifact_pin, dict):
                raise PlanCompileError(f"Fitting bundle artifact {artifact_name} pin is invalid")
            _exact_keys(
                artifact_pin,
                {"filename", "bytes", "sha256"},
                f"fitting bundle artifact {artifact_name}",
            )
            artifact_filename = artifact_pin.get("filename")
            if not isinstance(artifact_filename, str) or not artifact_filename:
                raise PlanCompileError(
                    f"Fitting bundle artifact {artifact_name} filename is invalid"
                )
            artifact_identity = artifact_filename.casefold()
            if artifact_identity in artifact_filename_identities:
                raise PlanCompileError(
                    "More than one fitting bundle artifact references the same filename"
                )
            artifact_digest = artifact_pin.get("sha256")
            if not isinstance(artifact_digest, str) or not SHA256_RE.fullmatch(artifact_digest):
                raise PlanCompileError(
                    f"Fitting bundle artifact {artifact_name} has an invalid SHA-256"
                )
            artifact_bytes = _positive_int(
                artifact_pin.get("bytes"), f"fitting bundle artifact {artifact_name}.bytes"
            )
            artifact_row = file_rows.get(artifact_filename)
            if (
                not artifact_row
                or artifact_row.get("sha256") != artifact_digest
                or artifact_row.get("bytes") != artifact_bytes
            ):
                raise PlanCompileError(
                    f"Native immutable manifest disagrees with fitting bundle artifact {artifact_name}"
                )
            artifact_filenames.add(artifact_filename)
            artifact_filename_identities.add(artifact_identity)

        if source_filename.casefold() in casefolded_filenames:
            raise PlanCompileError(
                "Native immutable manifest must not include the external source model file"
            )
        expected_inventory = {manifest_bundle_filename, *artifact_filenames}
        if set(file_rows) != expected_inventory:
            raise PlanCompileError(
                "Native immutable manifest inventory must contain exactly fitting_bundle.json "
                "and the declared fitting bundle artifacts"
            )
    skeleton_pin = artifacts.get("skeleton")
    if not isinstance(skeleton_pin, dict):
        raise PlanCompileError("Fitting bundle has no skeleton artifact pin")
    if skeleton_pin.get("sha256") != skeleton.sha256:
        raise PlanCompileError("Fitting bundle skeleton SHA-256 does not match the external pin")
    if skeleton_pin.get("bytes") != skeleton.size_bytes:
        raise PlanCompileError("Fitting bundle skeleton byte size does not match the external input")
    skeleton_row = file_rows.get(str(skeleton_pin.get("filename") or ""))
    if (
        not skeleton_row
        or skeleton_row.get("sha256") != skeleton.sha256
        or skeleton_row.get("bytes") != skeleton.size_bytes
    ):
        raise PlanCompileError("Immutable manifest skeleton inventory row does not match pinned bytes")
    skeleton_json = _json_object(skeleton)
    armatures = skeleton_json.get("armatures")
    if not isinstance(armatures, list) or not armatures:
        raise PlanCompileError("Pinned skeleton must contain at least one armature")
    bone_count = 0
    for index, armature in enumerate(armatures):
        bones = armature.get("bones") if isinstance(armature, dict) else None
        if not isinstance(bones, list) or not bones:
            raise PlanCompileError(f"Pinned skeleton armature {index} has no bones")
        bone_count += len(bones)
    source_contract = {
        "immutable_manifest_schema": immutable_manifest_value["schema"],
        "immutable_manifest_sha256": immutable_manifest.sha256,
        "bundle_schema": value["schema"],
        "bundle_revision": str(value.get("revision") or ""),
        "bundle_sha256": bundle.sha256,
        "skeleton_sha256": skeleton.sha256,
        "source_model_sha256": source_model_sha256,
        "source_model_copied": False,
        "source_model_local_file_required": False,
        "source_rig_type": source_rig_type,
        "source_orientation": "canonical",
        "bone_count": bone_count,
    }
    if native_manifest:
        source_contract["source_model_copied"] = None
        source_contract["source_model_in_bundle"] = False
        source_contract["source_provenance_mode"] = "external_sha256_native_bundle"
    return source_contract


def _render_prompt(template: str, species: str, label: str) -> str:
    rendered = re.sub(r"\s+", " ", template.replace("{{species}}", species).strip())
    if not rendered or "{{" in rendered or "}}" in rendered:
        raise PlanCompileError(f"{label} contains an unresolved template placeholder")
    return rendered


def _candidate_rows(plan_identity: str, action_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seeds: set[int] = set()
    for candidate_index in range(CANDIDATES_PER_ACTION):
        identity = {
            "schema": "autorig.animal-animation-library-candidate.v1",
            "plan_identity_sha256": plan_identity,
            "semantic_id": action_id,
            "candidate_index": candidate_index,
        }
        digest = _sha256(_canonical_json_bytes(identity))
        seed = 1 + (int(digest[:16], 16) % ((1 << 63) - 1))
        if seed in seeds:
            raise PlanCompileError(f"Deterministic candidate seed collision for {action_id}")
        seeds.add(seed)
        rows.append(
            {
                "candidate_id": f"{action_id}-c{candidate_index:02d}-{digest[16:28]}",
                "candidate_index": candidate_index,
                "seed": seed,
            }
        )
    return rows


def _write_atomic_no_overwrite(output_path: str | os.PathLike[str], data: bytes) -> Path:
    output = Path(output_path).resolve(strict=False)
    parent = output.parent
    if not parent.exists() or not parent.is_dir():
        raise PlanCompileError(f"Output directory does not exist: {parent}")
    if output.exists():
        raise PlanCompileError(f"Refusing to overwrite existing plan: {output}")
    temp = parent / f".{output.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    fd: int | None = None
    linked = False
    try:
        fd = os.open(temp, flags, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = None
            written = stream.write(data)
            if written != len(data):
                raise PlanCompileError("Short write while publishing the compiled plan")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temp, output)
            linked = True
        except FileExistsError as exc:
            raise PlanCompileError(f"Refusing to overwrite existing plan: {output}") from exc
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                raise PlanCompileError(f"Refusing to overwrite existing plan: {output}") from exc
            raise PlanCompileError(f"Atomic no-overwrite publication failed: {exc}") from exc
    finally:
        if fd is not None:
            os.close(fd)
        try:
            temp.unlink()
        except FileNotFoundError:
            pass
        if not linked and output.exists():
            # A concurrent writer owns this path.  Never remove its output.
            pass
    return output


def compile_animal_library_plan(
    *,
    rig_type: str,
    species: str,
    library_revision: str,
    taxonomy_path: str | os.PathLike[str],
    taxonomy_sha256: str,
    prompts_path: str | os.PathLike[str],
    prompts_sha256: str,
    loop_workflow_path: str | os.PathLike[str],
    loop_workflow_sha256: str,
    one_shot_workflow_path: str | os.PathLike[str],
    one_shot_workflow_sha256: str,
    immutable_manifest_path: str | os.PathLike[str],
    immutable_manifest_sha256: str,
    fitting_bundle_path: str | os.PathLike[str],
    fitting_bundle_sha256: str,
    skeleton_path: str | os.PathLike[str],
    skeleton_sha256: str,
    source_model_sha256: str,
    output_path: str | os.PathLike[str],
) -> CompileResult:
    rig = str(rig_type or "").strip().lower()
    species_value = re.sub(r"\s+", " ", str(species or "").strip())
    revision = str(library_revision or "").strip().lower()
    if not SAFE_SPECIES_RE.fullmatch(species_value):
        raise PlanCompileError("species contains unsupported characters")
    if not SAFE_REVISION_RE.fullmatch(revision):
        raise PlanCompileError("library_revision contains unsupported characters")
    source_model_digest = _require_sha256(source_model_sha256, "source model")

    # Keep this order fixed; tests assert every source is opened exactly once.
    taxonomy_source = _read_pinned(taxonomy_path, taxonomy_sha256, "taxonomy")
    prompt_source = _read_pinned(prompts_path, prompts_sha256, "prompts")
    loop_source = _read_pinned(loop_workflow_path, loop_workflow_sha256, "loop workflow")
    one_shot_source = _read_pinned(
        one_shot_workflow_path, one_shot_workflow_sha256, "one-shot workflow"
    )
    immutable_manifest_source = _read_pinned(
        immutable_manifest_path, immutable_manifest_sha256, "immutable manifest"
    )
    bundle_source = _read_pinned(fitting_bundle_path, fitting_bundle_sha256, "fitting bundle")
    skeleton_source = _read_pinned(skeleton_path, skeleton_sha256, "skeleton")

    taxonomy = _json_object(taxonomy_source)
    prompts = _json_object(prompt_source)
    loop_workflow = _json_object(loop_source)
    one_shot_workflow = _json_object(one_shot_source)
    immutable_manifest = _json_object(immutable_manifest_source)
    bundle = _json_object(bundle_source)
    clips, source_fps, output_fps = _validate_taxonomy(taxonomy, rig)
    prompt_rows, prefix, loop_instruction, one_shot_instruction, negative_template = _validate_prompts(
        prompts, clips, source_fps, output_fps
    )
    loop_fingerprint = _validate_workflow(loop_workflow, "loop", source_fps, output_fps)
    one_shot_fingerprint = _validate_workflow(
        one_shot_workflow, "one_shot", source_fps, output_fps
    )
    if loop_source.sha256 == one_shot_source.sha256 or loop_fingerprint == one_shot_fingerprint:
        raise PlanCompileError("Loop and one-shot workflows must be distinct")
    source_contract = _validate_source_contract(
        immutable_manifest,
        immutable_manifest_source,
        bundle,
        bundle_source,
        skeleton_source,
        source_model_digest,
        rig,
        species_value,
    )

    source_model_pin = {
        "sha256": source_model_digest,
        "copied": source_contract["source_model_copied"],
        "local_file_required": False,
    }
    if source_contract["immutable_manifest_schema"] == NATIVE_IMMUTABLE_MANIFEST_SCHEMA:
        source_model_pin["in_bundle"] = False
        source_model_pin["provenance_mode"] = "external_sha256_native_bundle"

    input_pins = {
        "taxonomy": taxonomy_source.public_pin(),
        "prompts": prompt_source.public_pin(),
        "loop_workflow": loop_source.public_pin(),
        "one_shot_workflow": one_shot_source.public_pin(),
        "immutable_manifest": immutable_manifest_source.public_pin(),
        "fitting_bundle": bundle_source.public_pin(),
        "skeleton": skeleton_source.public_pin(),
        "source_model": source_model_pin,
    }
    workflow_contracts = {
        "loop": {
            "generation_mode": "loop",
            "workflow_name": WORKFLOW_NAMES["loop"],
            "workflow_file_sha256": loop_source.sha256,
            "workflow_fingerprint_sha256": loop_fingerprint,
            "conditioned_frames": ["0:start", "N-1:reuse-start"],
        },
        "one_shot": {
            "generation_mode": "one_shot",
            "workflow_name": WORKFLOW_NAMES["one_shot"],
            "workflow_file_sha256": one_shot_source.sha256,
            "workflow_fingerprint_sha256": one_shot_fingerprint,
            "conditioned_frames": ["0:start"],
        },
    }
    plan_identity_payload = {
        "schema": PLAN_SCHEMA,
        "version": PLAN_VERSION,
        "rig_type": rig,
        "species": species_value,
        "library_revision": revision,
        "taxonomy_revision": taxonomy["revision"],
        "candidate_count_per_action": CANDIDATES_PER_ACTION,
        "priority_profile": {
            "profile": "horse-technical-priority-v1",
            "wave_1": list(HORSE_PRIORITY_ACTIONS),
            "wave_2": [
                action_id for action_id in EXPECTED_ACTION_IDS if action_id not in HORSE_PRIORITY_ACTIONS
            ],
        },
        "input_pins": input_pins,
        "workflow_contracts": workflow_contracts,
    }
    plan_identity = _sha256(_canonical_json_bytes(plan_identity_payload))
    rendered_negative = _render_prompt(negative_template, species_value, "negative prompt")
    priority_wave_one = tuple(HORSE_PRIORITY_ACTIONS)
    priority_wave_two = tuple(action_id for action_id in EXPECTED_ACTION_IDS if action_id not in priority_wave_one)
    wave_one_rank = {action_id: index + 1 for index, action_id in enumerate(priority_wave_one)}
    wave_two_rank = {action_id: index + 1 for index, action_id in enumerate(priority_wave_two)}
    jobs: list[dict[str, Any]] = []
    all_candidate_ids: set[str] = set()
    all_seeds: set[int] = set()
    for clip in clips:
        action_id = clip["id"]
        prompt = prompt_rows[action_id]
        mode = prompt["mode"]
        mode_instruction = loop_instruction if mode == "loop" else one_shot_instruction
        rendered_positive = _render_prompt(
            " ".join((prefix, prompt["motion"], mode_instruction)),
            species_value,
            f"{action_id} positive prompt",
        )
        candidates = _candidate_rows(plan_identity, action_id)
        for candidate in candidates:
            if candidate["candidate_id"] in all_candidate_ids or candidate["seed"] in all_seeds:
                raise PlanCompileError("Deterministic candidate identity collision across the plan")
            all_candidate_ids.add(candidate["candidate_id"])
            all_seeds.add(candidate["seed"])
        workflow = workflow_contracts[mode]
        priority_wave = 1 if action_id in wave_one_rank else 2
        priority_rank = wave_one_rank.get(action_id) or wave_two_rank[action_id]
        jobs.append(
            {
                "semantic_id": action_id,
                "order": clip["order"],
                "category": clip["category"],
                "loop": clip["loop"],
                "generation_mode": mode,
                "frame_count": prompt["frame_count"],
                "input_fps": source_fps,
                "output_fps": output_fps,
                "start_pose_id": clip["start_pose_id"],
                "end_pose_id": clip["end_pose_id"],
                "priority_wave": priority_wave,
                "priority_rank": priority_rank,
                "positive_prompt": rendered_positive,
                "positive_prompt_sha256": _sha256(rendered_positive.encode("utf-8")),
                "negative_prompt": rendered_negative,
                "negative_prompt_sha256": _sha256(rendered_negative.encode("utf-8")),
                "workflow_name": workflow["workflow_name"],
                "workflow_file_sha256": workflow["workflow_file_sha256"],
                "workflow_fingerprint_sha256": workflow["workflow_fingerprint_sha256"],
                "candidates": candidates,
            }
        )
    if tuple(job["semantic_id"] for job in jobs) != EXPECTED_ACTION_IDS or len(jobs) != 30:
        raise PlanCompileError("Compiled jobs no longer match the canonical 30-action order")
    if len(all_candidate_ids) != 240 or len(all_seeds) != 240:
        raise PlanCompileError("Compiled plan must contain 240 unique candidate identities/seeds")

    plan = {
        "schema": PLAN_SCHEMA,
        "version": PLAN_VERSION,
        "dry_run": True,
        "side_effects_authorized": False,
        "rig_type": rig,
        "species": species_value,
        "library_revision": revision,
        "taxonomy_revision": taxonomy["revision"],
        "source_fps": source_fps,
        "output_fps": output_fps,
        "job_count": len(jobs),
        "candidate_count_per_action": CANDIDATES_PER_ACTION,
        "candidate_count_total": len(all_candidate_ids),
        "plan_identity_sha256": plan_identity,
        "priority_contract": {
            "profile": "horse-technical-priority-v1",
            "wave_1": list(priority_wave_one),
            "wave_2": list(priority_wave_two),
        },
        "input_pins": input_pins,
        "source_contract": source_contract,
        "workflow_contracts": workflow_contracts,
        "jobs": jobs,
    }
    output_bytes = _canonical_json_bytes(plan) + b"\n"
    output = _write_atomic_no_overwrite(output_path, output_bytes)
    return CompileResult(
        output_path=output,
        output_sha256=_sha256(output_bytes),
        output_size_bytes=len(output_bytes),
        plan_identity_sha256=plan_identity,
        job_count=len(jobs),
        candidate_count=len(all_candidate_ids),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compile a pinned, dry-run 30-action animal animation library plan."
    )
    parser.add_argument("--rig-type", required=True)
    parser.add_argument("--species", required=True)
    parser.add_argument("--library-revision", required=True)
    for option in (
        "taxonomy",
        "prompts",
        "loop-workflow",
        "one-shot-workflow",
        "immutable-manifest",
        "fitting-bundle",
        "skeleton",
    ):
        parser.add_argument(f"--{option}", required=True)
        parser.add_argument(f"--{option}-sha256", required=True)
    parser.add_argument("--source-model-sha256", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = compile_animal_library_plan(
            rig_type=args.rig_type,
            species=args.species,
            library_revision=args.library_revision,
            taxonomy_path=args.taxonomy,
            taxonomy_sha256=args.taxonomy_sha256,
            prompts_path=args.prompts,
            prompts_sha256=args.prompts_sha256,
            loop_workflow_path=args.loop_workflow,
            loop_workflow_sha256=args.loop_workflow_sha256,
            one_shot_workflow_path=args.one_shot_workflow,
            one_shot_workflow_sha256=args.one_shot_workflow_sha256,
            immutable_manifest_path=args.immutable_manifest,
            immutable_manifest_sha256=args.immutable_manifest_sha256,
            fitting_bundle_path=args.fitting_bundle,
            fitting_bundle_sha256=args.fitting_bundle_sha256,
            skeleton_path=args.skeleton,
            skeleton_sha256=args.skeleton_sha256,
            source_model_sha256=args.source_model_sha256,
            output_path=args.output,
        )
    except PlanCompileError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "output": str(result.output_path),
                "output_sha256": result.output_sha256,
                "output_size_bytes": result.output_size_bytes,
                "plan_identity_sha256": result.plan_identity_sha256,
                "job_count": result.job_count,
                "candidate_count": result.candidate_count,
                "dry_run": True,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

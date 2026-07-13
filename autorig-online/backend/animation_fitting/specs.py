from __future__ import annotations

import json
import hashlib
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping, Tuple


PACKAGE_ROOT = Path(__file__).resolve().parent
SPEC_ROOT = PACKAGE_ROOT / "specs"
BACKEND_ROOT = PACKAGE_ROOT.parent
CANONICAL_TAXONOMY_PATH = BACKEND_ROOT / "animal_animation_taxonomy.v1.json"


class SpecValidationError(ValueError):
    """Raised when versioned animation-fitting contracts disagree."""


@dataclass(frozen=True)
class ActionPromptProfile:
    action_id: str
    family: str
    generation_mode: str
    frame_count: int
    input_fps: int
    output_fps: int
    motion_prompt: str
    common_positive_prefix: str
    common_negative_prompt: str
    mode_instruction: str

    @property
    def is_loop(self) -> bool:
        return self.generation_mode == "loop"

    def render_positive_prompt(self, species: str, motion_notes: str = "") -> str:
        species_value = _single_line(species, max_length=120) or "animal"
        parts = [
            self.common_positive_prefix.replace("{{species}}", species_value),
            self.motion_prompt.replace("{{species}}", species_value),
            self.mode_instruction.replace("{{species}}", species_value),
        ]
        notes = _single_line(motion_notes, max_length=700)
        if notes:
            parts.append(f"Additional motion direction: {notes}")
        return " ".join(part.strip() for part in parts if part.strip())


@dataclass(frozen=True)
class WorkflowTarget:
    node_title: str
    input_name: str


@dataclass(frozen=True)
class WorkflowBinding:
    targets: Tuple[WorkflowTarget, ...]

    @property
    def node_title(self) -> str:
        return self.targets[0].node_title

    @property
    def input_name(self) -> str:
        return self.targets[0].input_name


@dataclass(frozen=True)
class WorkflowProfile:
    generation_mode: str
    workflow_name: str
    bindings: Mapping[str, WorkflowBinding]
    conditioned_frames: Tuple[Mapping[str, Any], ...]
    input_fps: int
    output_fps: int
    workflow_fingerprint: str
    post_sampling_guide_crop_required: bool


@dataclass(frozen=True)
class CandidatePolicySpec:
    initial_count: int
    top_k: int
    retry_batch_count: int
    max_count: int


@dataclass(frozen=True)
class QaProfile:
    schema: str
    calibration_state: str
    candidate_policy: CandidatePolicySpec
    hard_gate_metric_keys: Tuple[str, ...]
    loop_hard_gate_metric_keys: Tuple[str, ...]
    ranking_weights: Mapping[str, float]
    missing_metric_score: float


@dataclass(frozen=True)
class AnimationFittingSpecs:
    prompt_schema: str
    taxonomy_schema: str
    workflow_schema: str
    fingerprint_algorithm: str
    actions: Mapping[str, ActionPromptProfile]
    action_order: Tuple[str, ...]
    workflows: Mapping[str, WorkflowProfile]
    qa: QaProfile

    def action(self, action_id: str) -> ActionPromptProfile:
        try:
            return self.actions[action_id]
        except KeyError as exc:
            raise SpecValidationError(f"Unknown animation action: {action_id}") from exc

    def workflow_for_action(self, action_id: str) -> WorkflowProfile:
        action = self.action(action_id)
        try:
            return self.workflows[action.generation_mode]
        except KeyError as exc:
            raise SpecValidationError(
                f"No workflow is configured for generation mode {action.generation_mode}"
            ) from exc


def _read_json_object(path: Path) -> Dict[str, Any]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise SpecValidationError(f"Required animation-fitting spec is missing: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SpecValidationError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise SpecValidationError(f"{path} must contain a JSON object")
    return parsed


def _single_line(value: object, *, max_length: int) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())[:max_length]


def _require_positive_int(value: object, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SpecValidationError(f"{label} must be an integer") from exc
    if parsed <= 0:
        raise SpecValidationError(f"{label} must be positive")
    return parsed


def _load_prompt_profiles(prompt_doc: Mapping[str, Any]) -> tuple[Dict[str, ActionPromptProfile], Tuple[str, ...]]:
    input_fps = _require_positive_int(prompt_doc.get("input_fps_int"), "input_fps_int")
    output_fps = _require_positive_int(prompt_doc.get("output_fps_int"), "output_fps_int")
    common_prefix = _single_line(prompt_doc.get("common_positive_prefix_string"), max_length=4000)
    common_negative = _single_line(prompt_doc.get("common_negative_prompt_string"), max_length=4000)
    loop_instruction = _single_line(prompt_doc.get("loop_instruction_string"), max_length=3000)
    one_shot_instruction = _single_line(prompt_doc.get("one_shot_instruction_string"), max_length=3000)
    if not all((common_prefix, common_negative, loop_instruction, one_shot_instruction)):
        raise SpecValidationError("Prompt spec common instructions must be non-empty")

    rows = prompt_doc.get("actions_array")
    if not isinstance(rows, list):
        raise SpecValidationError("actions_array must be a list")
    actions: Dict[str, ActionPromptProfile] = {}
    order = []
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise SpecValidationError(f"actions_array[{index}] must be an object")
        action_id = _single_line(raw.get("action_id_string"), max_length=80)
        family = _single_line(raw.get("family_string"), max_length=80)
        mode = _single_line(raw.get("generation_mode_string"), max_length=20)
        motion_prompt = _single_line(raw.get("motion_prompt_string"), max_length=3000)
        frame_count = _require_positive_int(raw.get("frame_count_int"), f"{action_id}.frame_count_int")
        if not re.fullmatch(r"[a-z0-9_]+", action_id):
            raise SpecValidationError(f"Invalid action id: {action_id!r}")
        if action_id in actions:
            raise SpecValidationError(f"Duplicate action id: {action_id}")
        if mode not in ("loop", "one_shot"):
            raise SpecValidationError(f"{action_id} has invalid generation mode {mode!r}")
        if (frame_count - 1) % 8 != 0:
            raise SpecValidationError(f"{action_id} frame count must satisfy 8n+1")
        if not family or not motion_prompt:
            raise SpecValidationError(f"{action_id} requires family and motion prompt")
        actions[action_id] = ActionPromptProfile(
            action_id=action_id,
            family=family,
            generation_mode=mode,
            frame_count=frame_count,
            input_fps=input_fps,
            output_fps=output_fps,
            motion_prompt=motion_prompt,
            common_positive_prefix=common_prefix,
            common_negative_prompt=common_negative,
            mode_instruction=loop_instruction if mode == "loop" else one_shot_instruction,
        )
        order.append(action_id)
    return actions, tuple(order)


def _load_workflows(workflow_doc: Mapping[str, Any]) -> Dict[str, WorkflowProfile]:
    if workflow_doc.get("workflow_format_string") != "comfy-api-prompt":
        raise SpecValidationError("Only Comfy API-prompt workflows are supported")
    input_fps = _require_positive_int(workflow_doc.get("input_fps_int"), "workflow input_fps_int")
    output_fps = _require_positive_int(workflow_doc.get("output_fps_int"), "workflow output_fps_int")
    raw_workflows = workflow_doc.get("workflows_object")
    if not isinstance(raw_workflows, dict):
        raise SpecValidationError("workflows_object must be an object")
    result: Dict[str, WorkflowProfile] = {}
    for mode in ("loop", "one_shot"):
        raw = raw_workflows.get(mode)
        if not isinstance(raw, dict):
            raise SpecValidationError(f"Workflow {mode} is missing")
        if raw.get("generation_mode_string") != mode:
            raise SpecValidationError(f"Workflow {mode} has a mismatched generation_mode_string")
        workflow_name = _single_line(raw.get("workflow_name_string"), max_length=200)
        if not workflow_name.endswith(".json"):
            raise SpecValidationError(f"Workflow {mode} must use a .json file")
        raw_bindings = raw.get("bindings_object")
        if not isinstance(raw_bindings, dict):
            raise SpecValidationError(f"Workflow {mode} bindings_object is missing")
        bindings: Dict[str, WorkflowBinding] = {}
        required = {
            "start_image",
            "positive_prompt",
            "negative_prompt",
            "frame_count",
            "fps",
            "output_fps",
            "seed",
            "output",
        }
        if mode == "loop":
            required.add("end_image")
        for key in required:
            binding = raw_bindings.get(key)
            if not isinstance(binding, dict):
                raise SpecValidationError(f"Workflow {mode} binding {key} is missing")
            target_rows = binding.get("targets_array")
            if target_rows is None:
                target_rows = [binding]
            if not isinstance(target_rows, list) or not target_rows:
                raise SpecValidationError(f"Workflow {mode} binding {key} targets are invalid")
            targets = []
            for target in target_rows:
                if not isinstance(target, dict):
                    raise SpecValidationError(f"Workflow {mode} binding {key} target is invalid")
                title = _single_line(target.get("node_title_string"), max_length=160)
                input_name = _single_line(target.get("input_name_string"), max_length=80)
                if not title or not input_name:
                    raise SpecValidationError(f"Workflow {mode} binding {key} target is invalid")
                targets.append(WorkflowTarget(node_title=title, input_name=input_name))
            bindings[key] = WorkflowBinding(targets=tuple(targets))
        if mode == "one_shot" and "end_image" in raw_bindings:
            raise SpecValidationError("One-shot workflow must not condition an end frame")
        frames = raw.get("conditioned_frames_array")
        if not isinstance(frames, list) or not frames:
            raise SpecValidationError(f"Workflow {mode} conditioned_frames_array is missing")
        if not all(isinstance(item, dict) for item in frames):
            raise SpecValidationError(f"Workflow {mode} conditioned frames must be objects")
        expected_frames = (("start", "0"), ("end", "N-1")) if mode == "loop" else (("start", "0"),)
        actual_frames = tuple(
            (
                str(item.get("role_string") or ""),
                str(item.get("frame_index_expression_string") or ""),
            )
            for item in frames
        )
        if actual_frames != expected_frames:
            raise SpecValidationError(
                f"Workflow {mode} must condition exactly {expected_frames}, got {actual_frames}"
            )
        if mode == "loop" and frames[1].get("reuse_start_image_bool") is not True:
            raise SpecValidationError("Loop end frame must reuse the exact start image")
        if raw.get("post_sampling_guide_crop_required_bool") is not True:
            raise SpecValidationError(
                f"Workflow {mode} must crop appended guide latents after sampling"
            )
        fingerprint = str(raw.get("workflow_fingerprint_sha256_string") or "").strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
            raise SpecValidationError(f"Workflow {mode} has no pinned SHA-256 fingerprint")
        workflow_path = SPEC_ROOT / "workflows" / workflow_name
        workflow_prompt = _read_json_object(workflow_path)
        actual_fingerprint = _canonical_json_sha256(workflow_prompt)
        if actual_fingerprint != fingerprint:
            raise SpecValidationError(
                f"Workflow {mode} fingerprint mismatch: {actual_fingerprint} != {fingerprint}"
            )
        result[mode] = WorkflowProfile(
            generation_mode=mode,
            workflow_name=workflow_name,
            bindings=bindings,
            conditioned_frames=tuple(frames),
            input_fps=input_fps,
            output_fps=output_fps,
            workflow_fingerprint=fingerprint,
            post_sampling_guide_crop_required=True,
        )
    return result


def _load_qa(qa_doc: Mapping[str, Any]) -> QaProfile:
    policy_raw = qa_doc.get("candidate_policy_object")
    if not isinstance(policy_raw, dict):
        raise SpecValidationError("candidate_policy_object must be an object")
    policy = CandidatePolicySpec(
        initial_count=_require_positive_int(policy_raw.get("initial_count_int"), "initial_count_int"),
        top_k=_require_positive_int(policy_raw.get("top_k_int"), "top_k_int"),
        retry_batch_count=_require_positive_int(policy_raw.get("retry_batch_count_int"), "retry_batch_count_int"),
        max_count=_require_positive_int(policy_raw.get("max_count_int"), "max_count_int"),
    )
    if not (policy.top_k <= policy.initial_count <= policy.max_count):
        raise SpecValidationError("Candidate policy must satisfy top_k <= initial_count <= max_count")
    if policy.initial_count + policy.retry_batch_count > policy.max_count:
        raise SpecValidationError("Candidate retry batch exceeds max_count")
    hard = qa_doc.get("hard_gate_metric_keys_array")
    loop_hard = qa_doc.get("loop_hard_gate_metric_keys_array")
    weights = qa_doc.get("ranking_weights_object")
    metric_contract = qa_doc.get("metric_contract_object")
    if not isinstance(hard, list) or not isinstance(loop_hard, list) or not isinstance(weights, dict):
        raise SpecValidationError("QA gates and ranking weights must be configured")
    parsed_weights = {str(key): float(value) for key, value in weights.items()}
    if abs(sum(parsed_weights.values()) - 1.0) > 1e-6:
        raise SpecValidationError("QA ranking weights must sum to 1.0")
    if not isinstance(metric_contract, dict):
        raise SpecValidationError("metric_contract_object must be an object")
    return QaProfile(
        schema=str(qa_doc.get("schema") or ""),
        calibration_state=str(qa_doc.get("calibration_state_string") or ""),
        candidate_policy=policy,
        hard_gate_metric_keys=tuple(str(item) for item in hard),
        loop_hard_gate_metric_keys=tuple(str(item) for item in loop_hard),
        ranking_weights=parsed_weights,
        missing_metric_score=float(metric_contract.get("missing_metric_score_float", 0.0)),
    )


def _validate_against_canonical_taxonomy(
    prompt_doc: Mapping[str, Any],
    actions: Mapping[str, ActionPromptProfile],
    action_order: Tuple[str, ...],
) -> None:
    taxonomy = _read_json_object(CANONICAL_TAXONOMY_PATH)
    taxonomy_schema = str(taxonomy.get("schema") or "")
    if taxonomy_schema != str(prompt_doc.get("taxonomy_schema") or ""):
        raise SpecValidationError(
            f"Prompt taxonomy schema {prompt_doc.get('taxonomy_schema')!r} does not match {taxonomy_schema!r}"
        )
    clips = taxonomy.get("clips")
    if not isinstance(clips, list):
        raise SpecValidationError("Canonical taxonomy clips must be a list")
    canonical_rows = [row for row in clips if isinstance(row, dict)]
    canonical_order = tuple(str(row.get("id") or "") for row in canonical_rows)
    if action_order != canonical_order:
        raise SpecValidationError("Prompt action order must exactly match the canonical 30-clip taxonomy")
    if len(action_order) != 30:
        raise SpecValidationError(f"Animation fitting requires exactly 30 actions, found {len(action_order)}")
    for row in canonical_rows:
        action_id = str(row.get("id") or "")
        action = actions[action_id]
        expected_mode = "loop" if bool(row.get("loop")) else "one_shot"
        if action.generation_mode != expected_mode:
            raise SpecValidationError(f"{action_id} generation mode disagrees with canonical taxonomy")
        if action.frame_count != int(row.get("frame_profile") or 0):
            raise SpecValidationError(f"{action_id} frame count disagrees with canonical taxonomy")
    if int(taxonomy.get("source_fps") or 0) != next(iter(actions.values())).input_fps:
        raise SpecValidationError("Prompt input FPS disagrees with canonical taxonomy")
    if int(taxonomy.get("output_fps") or 0) != next(iter(actions.values())).output_fps:
        raise SpecValidationError("Prompt output FPS disagrees with canonical taxonomy")


def _canonical_json_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@lru_cache(maxsize=1)
def load_animation_fitting_specs() -> AnimationFittingSpecs:
    prompt_doc = _read_json_object(SPEC_ROOT / "action_prompts.v1.json")
    qa_doc = _read_json_object(SPEC_ROOT / "qa_profile.v1.json")
    workflow_doc = _read_json_object(SPEC_ROOT / "workflow_contracts.v1.json")
    actions, action_order = _load_prompt_profiles(prompt_doc)
    _validate_against_canonical_taxonomy(prompt_doc, actions, action_order)
    workflows = _load_workflows(workflow_doc)
    fingerprint_algorithm = str(workflow_doc.get("fingerprint_algorithm_string") or "")
    if fingerprint_algorithm != "sha256-canonical-json-v1":
        raise SpecValidationError(
            f"Unsupported workflow fingerprint algorithm: {fingerprint_algorithm!r}"
        )
    if next(iter(actions.values())).input_fps != next(iter(workflows.values())).input_fps:
        raise SpecValidationError("Prompt and workflow input FPS disagree")
    if next(iter(actions.values())).output_fps != next(iter(workflows.values())).output_fps:
        raise SpecValidationError("Prompt and workflow output FPS disagree")
    return AnimationFittingSpecs(
        prompt_schema=str(prompt_doc.get("schema") or ""),
        taxonomy_schema=str(prompt_doc.get("taxonomy_schema") or ""),
        workflow_schema=str(workflow_doc.get("schema") or ""),
        fingerprint_algorithm=fingerprint_algorithm,
        actions=actions,
        action_order=action_order,
        workflows=workflows,
        qa=_load_qa(qa_doc),
    )

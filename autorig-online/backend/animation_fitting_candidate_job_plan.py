"""Pure builder for server-owned browser animation candidate job plans.

The HTTP layer must never accept ``browser_candidate_ingest`` from a client.
It independently resolves a completed Animal task, immutable reference
manifest, latest controlled-generation states, optional second-batch
authorization, and completed controlled-generation receipts before passing
those server-owned values to this module.  The result is a canonical job
config which is compatible with the browser ingest and deterministic selection
consumers; this module performs no DB or route mutation.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
from typing import Any, Dict, Mapping, Sequence
from urllib.parse import urlsplit
import uuid


PLAN_REQUEST_SCHEMA = "autorig.browser-animation-candidate-job-plan-request.v1"
TRUSTED_TASK_PINS_SCHEMA = "autorig.completed-animal-task-pins.v1"
CONTROLLED_RECEIPT_SCHEMA_V1 = (
    "autorig.animation-fitting-controlled-generation-receipt-descriptor.v1"
)
CONTROLLED_RECEIPT_SCHEMA_V2 = (
    "autorig.animation-fitting-controlled-generation-receipt-descriptor.v2"
)
# Compatibility alias for the immutable V14 descriptor.  New production
# receipts MUST use V2 and carry the complete server-owned provenance binding.
CONTROLLED_RECEIPT_SCHEMA = CONTROLLED_RECEIPT_SCHEMA_V1
REFERENCE_MANIFEST_SCHEMA = "autorig.animation-fitting-reference-manifest.v1"
PROMPT_CONTRACT_SCHEMA = "autorig.animation-fitting-rendered-prompt-binding.v1"
CONTROLLED_STATE_SCHEMA = "autorig.animation-fitting-controlled-job-identity.v1"
TRUSTED_LATEST_STATE_SCHEMA = (
    "autorig.animation-fitting-controlled-generation-latest-state.v1"
)
RETRY_AUTHORIZATION_SCHEMA = "autorig.animation-fitting-second-batch-authorization.v1"
PLAN_BINDING_SCHEMA_V2 = "autorig.browser-animation-candidate-job-plan-binding.v2"
PLAN_BINDING_SCHEMA_V1 = "autorig.browser-animation-candidate-job-plan-binding.v1"
PLAN_IDENTITY_SCHEMA_V2 = "autorig.browser-animation-candidate-job-plan.v2"
PLAN_IDENTITY_SCHEMA_V1 = "autorig.browser-animation-candidate-job-plan.v1"
INGEST_SCHEMA_V2 = "autorig.browser-animation-candidate-job-binding.v2"
INGEST_SCHEMA_V1 = "autorig.browser-animation-candidate-job-binding.v1"
SELECTION_CONFIG_SCHEMA = "autorig.browser-animation-candidate-selection-config.v1"

V14_SEMANTIC_ID = "walk_forward"
V14_FIXED_SEED = 6550110377254033429
V14_EXPERIMENT_ID = "horse_walk_v14_browser_interval_guide_seed_6550110377254033429_v1"
V14_EXPERIMENT_SHA256 = (
    "0f172076147e94099ea7c0cf3c323a46f698ea48e55b7bce9acec789e0e77c66"
)
# These values are the duplicated fail-closed REAL_V14_CONTRACT pins consumed
# by author_v14_browser_fitting_spec.mjs and run_v14_browser_fitting_pipeline.mjs.
V14_CONTROLLED_JOB_ID = (
    "c4d04cf43ae38e92a75b4bfe3f9763c00e4c8ef1d4d2915ed4ed9ff1d41e961e"
)
V14_PROMPT_ID = "0472b8ba-385d-403d-886e-ff1f8d8bb46c"
V14_WORKFLOW_NAME = "autorig_ltx2_animal_loop_v1_api.json"
V14_WORKFLOW_FINGERPRINT_SHA256 = (
    "e0f549b58d3933027a4f4d3fde69d6e3dfb6d360f0200e8f00a9d2bff278bc56"
)
V14_SOURCE_MODEL_SHA256 = (
    "fa75772d83c2613ddd6df6f7a305a407e12abf4a75c9083bb53df4d2619f50a1"
)
V14_SOURCE_SKELETON_SHA256 = (
    "0e7fb527d4df5273c289a61a2bbb1f456d9cd10f83d2b09cbbea05daade6f8be"
)
V14_WORKER_ID = "local-4090"
V14_WORKER_BASE_URL = "http://127.0.0.1:8188"

PROMPT_SPEC_SCHEMA = "autorig.animation-fitting-prompts.v1"
PROMPT_SPEC_RELATIVE_PATH = "animation_fitting/specs/action_prompts.v1.json"
PROMPT_SPEC_SHA256 = "e9ba8e7a28a233d93d56b6ea65600dd9c9b7b2fcac014b884ec1fef528278c03"
WORKFLOW_CONTRACT_SCHEMA = "autorig.animation-fitting-workflows.v1"
WORKFLOW_CONTRACT_RELATIVE_PATH = "animation_fitting/specs/workflow_contracts.v1.json"
WORKFLOW_CONTRACT_SHA256 = (
    "d13f792728ce59cfd00387cb8a6ba0e9a57a0b21b9c0086142a39af7d9b7c83c"
)
TAXONOMY_SCHEMA = "animal-animation-taxonomy.v1"
TAXONOMY_REVISION = "animal-base-30-v1"
TAXONOMY_CANONICAL_SHA256 = (
    "f225d927ef69f946959bd74fe3968c93b40195fd1479f507160338e469f790c1"
)
CANONICAL_SOURCE_RIG_SPECIES = {
    "DOG_1": "dog",
    "BEAR_1": "bear",
    "CAT_1": "cat",
    "COW_1": "cow",
    "DEER_1": "deer",
    "ELEPHANT_1": "elephant",
    "GIRAFFE_1": "giraffe",
    "HORSE_2": "horse",
    "MOUSE_1": "mouse",
    "PIG_1": "pig",
    "RABBIT_1": "rabbit",
    "TURTLE_1": "turtle",
}
CANONICAL_WORKFLOWS = {
    "loop": (
        "autorig_ltx2_animal_loop_v1_api.json",
        "e0f549b58d3933027a4f4d3fde69d6e3dfb6d360f0200e8f00a9d2bff278bc56",
    ),
    "one_shot": (
        "autorig_ltx2_animal_one_shot_v1_api.json",
        "3ad06e73aefe81f7613b9a922812e749ed4d4f422d5da6592c43b0dbcf38200c",
    ),
}

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
MAX_CANDIDATES = 16
PRODUCTION_CANDIDATE_TARGET = 8
PRODUCTION_CANDIDATE_LIMIT = 16


class BrowserCandidateJobPlanError(ValueError):
    """Fail-closed validation failure at the server-owned planning boundary."""


@dataclass(frozen=True)
class BrowserCandidateJobPlan:
    """Canonical values which a route may copy into a fitting-job create call."""

    schema: str
    semantic_id: str
    generation_mode: str
    worker_id: str
    worker_base_url: str
    workflow_name: str
    workflow_fingerprint: str
    candidate_target: int
    candidate_limit: int
    selection_mode: str
    production_eligible: bool
    config: Dict[str, Any]
    config_json: str
    config_sha256: str
    identity_sha256: str
    idempotency_key: str
    prompt_id: str

    @property
    def worker_url(self) -> str:
        """Exact value to copy into ``AnimalAnimationFittingJob.worker_url``."""
        return self.worker_base_url


def _error(message: str) -> BrowserCandidateJobPlanError:
    return BrowserCandidateJobPlanError(message)


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise _error("job plan is not finite canonical JSON") from exc


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact_object(value: Any, field: str, keys: Sequence[str]) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(keys):
        raise _error(f"{field} must contain exactly: {', '.join(keys)}")
    return dict(value)


def _canonical_uuid(value: Any, field: str) -> str:
    raw = str(value or "").strip()
    try:
        result = str(uuid.UUID(raw))
    except (ValueError, AttributeError, TypeError) as exc:
        raise _error(f"{field} must be a canonical UUID") from exc
    if raw != result:
        raise _error(f"{field} must be a canonical lowercase UUID")
    return result


def _sha(value: Any, field: str) -> str:
    result = str(value or "").strip().lower()
    if not SHA256_RE.fullmatch(result):
        raise _error(f"{field} must be a lowercase SHA-256")
    return result


def _positive_int(value: Any, field: str, *, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _error(f"{field} must be a positive integer")
    if maximum is not None and value > maximum:
        raise _error(f"{field} must not exceed {maximum}")
    return value


def _seed(value: Any, field: str = "seed") -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= (2**63 - 1)
    ):
        raise _error(f"{field} must fit the non-negative SQL BigInteger range")
    return value


def _token(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not SAFE_TOKEN_RE.fullmatch(result):
        raise _error(f"{field} is missing or contains unsupported characters")
    return result


def _worker_url(value: Any, field: str) -> str:
    raw = str(value or "").strip()
    try:
        parsed = urlsplit(raw)
        _ = parsed.port
    except ValueError as exc:
        raise _error(f"{field} is not a valid worker base URL") from exc
    if (
        parsed.scheme.lower() not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise _error(f"{field} must be an origin-only HTTP(S) worker base URL")
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def _relative_artifact_pin(
    value: Any,
    field: str,
    *,
    suffix: str,
    content_addressed: bool = True,
) -> Dict[str, Any]:
    pin = _exact_object(value, field, ("path", "sha256", "bytes"))
    digest = _sha(pin["sha256"], f"{field}.sha256")
    size = _positive_int(pin["bytes"], f"{field}.bytes")
    raw_path = str(pin["path"] or "").strip().replace("\\", "/")
    path = PurePosixPath(raw_path)
    if (
        not raw_path
        or not path.parts
        or path.is_absolute()
        or ":" in path.parts[0]
        or any(part in ("", ".", "..") for part in path.parts)
    ):
        raise _error(f"{field}.path must be a canonical relative server path")
    if path.suffix.lower() != suffix:
        raise _error(f"{field}.path must use the {suffix} suffix")
    if content_addressed and path.name.lower() != f"{digest}{suffix}":
        raise _error(f"{field}.path must use its content-addressed {suffix} filename")
    return {"path": path.as_posix(), "sha256": digest, "bytes": size}


def _pinned_canonical_object(
    value: Any,
    field: str,
    *,
    expected_schema: str | None = None,
) -> Dict[str, Any]:
    """Verify a parsed server-owned JSON object against its canonical pin.

    Callers must construct this wrapper from a trusted server-side read.  The
    client is never allowed to supply it.  Canonical JSON makes the exact
    semantic content, rather than incidental whitespace, the immutable unit.
    """

    wrapper = _exact_object(value, field, ("content", "pin"))
    if not isinstance(wrapper["content"], Mapping):
        raise _error(f"{field}.content must be a JSON object")
    content = dict(wrapper["content"])
    if expected_schema is not None and content.get("schema") != expected_schema:
        raise _error(f"{field}.content schema is invalid")
    encoded = canonical_json_bytes(content)
    pin = _relative_artifact_pin(
        wrapper["pin"], f"{field}.pin", suffix=".json", content_addressed=False
    )
    if pin["sha256"] != _sha256(encoded) or pin["bytes"] != len(encoded):
        raise _error(f"{field}.pin does not match canonical manifest content")
    return {"content": content, "pin": pin}


def _read_exact_json_spec(
    relative_path: str,
    expected_sha256: str,
    expected_schema: str,
    field: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    path = Path(__file__).parent / PurePosixPath(relative_path)
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error(f"canonical {field} spec is unavailable") from exc
    canonical = canonical_json_bytes(value)
    if _sha256(canonical) != expected_sha256:
        raise _error(f"canonical {field} spec SHA-256 drifted")
    if not isinstance(value, Mapping) or value.get("schema") != expected_schema:
        raise _error(f"canonical {field} spec schema drifted")
    return dict(value), {
        "path": relative_path,
        "sha256": expected_sha256,
        "bytes": len(canonical),
    }


def _render_prompt(template: Any, species: str, field: str) -> str:
    if not isinstance(template, str):
        raise _error(f"{field} is invalid")
    rendered = re.sub(r"\s+", " ", template.replace("{{species}}", species).strip())
    if not rendered or "{{" in rendered or "}}" in rendered:
        raise _error(f"{field} has an unresolved template placeholder")
    return rendered


def _species_for_task(task: Mapping[str, Any]) -> str:
    rig = str(task["source_rig_type"])
    species = CANONICAL_SOURCE_RIG_SPECIES.get(rig)
    if species is None:
        raise _error("trusted task source rig is not a canonical supported rig")
    return species


def _load_taxonomy() -> Dict[str, Any]:
    taxonomy_path = Path(__file__).with_name("animal_animation_taxonomy.v1.json")
    try:
        taxonomy = json.loads(taxonomy_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _error("canonical animation taxonomy is unavailable") from exc
    if (
        not isinstance(taxonomy, Mapping)
        or taxonomy.get("schema") != TAXONOMY_SCHEMA
        or taxonomy.get("revision") != TAXONOMY_REVISION
        or taxonomy.get("source_fps") != 24
        or taxonomy.get("output_fps") != 30
        or taxonomy.get("rig_types") != list(CANONICAL_SOURCE_RIG_SPECIES.values())
        or _sha256(canonical_json_bytes(taxonomy)) != TAXONOMY_CANONICAL_SHA256
    ):
        raise _error("canonical animation taxonomy content drifted")
    return dict(taxonomy)


def _canonical_prompt_and_workflow_contract(
    *,
    semantic_id: str,
    clip: Mapping[str, Any],
    species: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    prompts, prompt_pin = _read_exact_json_spec(
        PROMPT_SPEC_RELATIVE_PATH,
        PROMPT_SPEC_SHA256,
        PROMPT_SPEC_SCHEMA,
        "prompt",
    )
    workflows, workflow_pin = _read_exact_json_spec(
        WORKFLOW_CONTRACT_RELATIVE_PATH,
        WORKFLOW_CONTRACT_SHA256,
        WORKFLOW_CONTRACT_SCHEMA,
        "workflow contract",
    )
    if (
        prompts.get("taxonomy_schema") != "animal-animation-taxonomy.v1"
        or prompts.get("input_fps_int") != 24
        or prompts.get("output_fps_int") != 30
        or workflows.get("input_fps_int") != 24
        or workflows.get("output_fps_int") != 30
    ):
        raise _error("canonical prompt/workflow timing contract drifted")
    prompt_rows = [
        row
        for row in prompts.get("actions_array", ())
        if isinstance(row, Mapping) and row.get("action_id_string") == semantic_id
    ]
    if len(prompt_rows) != 1:
        raise _error("canonical prompt action inventory drifted")
    prompt_row = prompt_rows[0]
    expected_mode = "loop" if clip.get("loop") is True else "one_shot"
    if prompt_row.get("generation_mode_string") != expected_mode or prompt_row.get(
        "frame_count_int"
    ) != clip.get("frame_profile"):
        raise _error("canonical prompt action no longer matches taxonomy")
    mode_instruction_key = (
        "loop_instruction_string"
        if expected_mode == "loop"
        else "one_shot_instruction_string"
    )
    positive = _render_prompt(
        " ".join(
            (
                str(prompts.get("common_positive_prefix_string") or ""),
                str(prompt_row.get("motion_prompt_string") or ""),
                str(prompts.get(mode_instruction_key) or ""),
            )
        ),
        species,
        "positive prompt",
    )
    negative = _render_prompt(
        prompts.get("common_negative_prompt_string"), species, "negative prompt"
    )
    workflow_rows = workflows.get("workflows_object")
    workflow = (
        workflow_rows.get(expected_mode) if isinstance(workflow_rows, Mapping) else None
    )
    expected_name, expected_fingerprint = CANONICAL_WORKFLOWS[expected_mode]
    if (
        not isinstance(workflow, Mapping)
        or workflow.get("generation_mode_string") != expected_mode
        or workflow.get("workflow_name_string") != expected_name
        or workflow.get("workflow_fingerprint_sha256_string") != expected_fingerprint
    ):
        raise _error("canonical workflow mode binding drifted")
    prompt_contract = {
        "schema": PROMPT_CONTRACT_SCHEMA,
        "spec_schema": PROMPT_SPEC_SCHEMA,
        "spec_pin": prompt_pin,
        "species": species,
        "positive_prompt_sha256": _sha256(positive.encode("utf-8")),
        "negative_prompt_sha256": _sha256(negative.encode("utf-8")),
    }
    workflow_contract = {
        "schema": WORKFLOW_CONTRACT_SCHEMA,
        "spec_pin": workflow_pin,
        "generation_mode": expected_mode,
        "workflow_name": expected_name,
        "workflow_fingerprint_sha256": expected_fingerprint,
    }
    return prompt_contract, workflow_contract


def _trusted_reference_manifest(
    value: Any,
    *,
    task: Mapping[str, Any],
    species: str,
) -> Dict[str, Any]:
    reference = _pinned_canonical_object(
        value,
        "trusted_reference_manifest",
        expected_schema=REFERENCE_MANIFEST_SCHEMA,
    )
    content = _exact_object(
        reference["content"],
        "trusted_reference_manifest.content",
        (
            "schema",
            "task_id",
            "task_guid",
            "source_rig_type",
            "species",
            "source_model_sha256",
            "source_skeleton_sha256",
            "actionless",
            "geometry_uv_normals_mutated",
            "reference_artifact",
        ),
    )
    if (
        content["task_id"] != task["task_id"]
        or content["task_guid"] != task["task_guid"]
        or content["source_rig_type"] != task["source_rig_type"]
        or content["species"] != species
        or content["source_model_sha256"] != task["source_model_sha256"]
        or content["source_skeleton_sha256"] != task["source_skeleton_sha256"]
        or content["actionless"] is not True
        or content["geometry_uv_normals_mutated"] is not False
    ):
        raise _error("trusted reference manifest does not bind the exact task pins")
    artifact = _relative_artifact_pin(
        content["reference_artifact"],
        "trusted_reference_manifest.content.reference_artifact",
        suffix=".png",
        content_addressed=False,
    )
    normalized = {**content, "reference_artifact": artifact}
    # Recheck after normalization so noncanonical URL/path spelling cannot be
    # silently bound under the caller's original content hash.
    if normalized != reference["content"]:
        raise _error("trusted reference manifest content is not canonical")
    return {"content": normalized, "pin": reference["pin"]}


def _trusted_latest_state(value: Any) -> Dict[str, Any]:
    state = _exact_object(
        value,
        "trusted_latest_state",
        (
            "schema",
            "status",
            "latest",
            "job_id",
            "state_schema",
            "sequence",
            "filename",
            "pin",
        ),
    )
    if (
        state["schema"] != TRUSTED_LATEST_STATE_SCHEMA
        or state["status"] != "completed"
        or state["latest"] is not True
        or state["state_schema"] != CONTROLLED_STATE_SCHEMA
    ):
        raise _error(
            "trusted latest-state descriptor is not an authoritative completion"
        )
    job_id = _sha(state["job_id"], "trusted_latest_state.job_id")
    sequence = _positive_int(state["sequence"], "trusted_latest_state.sequence")
    filename = str(state["filename"] or "")
    if filename != f"{sequence:06d}.json":
        raise _error("trusted latest-state filename does not match its sequence")
    pin = _relative_artifact_pin(
        state["pin"],
        "trusted_latest_state.pin",
        suffix=".json",
        content_addressed=False,
    )
    if PurePosixPath(pin["path"]).parts != ("jobs", job_id, filename):
        raise _error("trusted latest-state path does not bind its job and sequence")
    normalized = {
        "schema": TRUSTED_LATEST_STATE_SCHEMA,
        "status": "completed",
        "latest": True,
        "job_id": job_id,
        "state_schema": CONTROLLED_STATE_SCHEMA,
        "sequence": sequence,
        "filename": filename,
        "pin": pin,
    }
    if normalized != state:
        raise _error("trusted latest-state descriptor is not canonical")
    return normalized


def _trusted_latest_state_inventory(
    values: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise _error("trusted_latest_states must be an independent server sequence")
    states = [_trusted_latest_state(value) for value in values]
    by_job = {state["job_id"]: state for state in states}
    if len(by_job) != len(states):
        raise _error("trusted latest-state descriptors must bind unique jobs")
    return by_job


def _trusted_retry_authorization(
    value: Any,
    *,
    task: Mapping[str, Any],
    semantic_id: str,
    receipts: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    authorization = _pinned_canonical_object(
        value,
        "trusted_retry_authorization",
        expected_schema=RETRY_AUTHORIZATION_SCHEMA,
    )
    content = _exact_object(
        authorization["content"],
        "trusted_retry_authorization.content",
        (
            "schema",
            "status",
            "task_id",
            "task_guid",
            "semantic_id",
            "first_batch_candidate_indices",
            "first_batch_latest_state_sha256s",
            "first_batch_selection_closure",
            "first_batch_qa_closure",
            "first_batch_outcome",
            "authorized_candidate_indices",
        ),
    )
    selection_closure = _relative_artifact_pin(
        content["first_batch_selection_closure"],
        "trusted_retry_authorization.content.first_batch_selection_closure",
        suffix=".json",
        content_addressed=False,
    )
    qa_closure = _relative_artifact_pin(
        content["first_batch_qa_closure"],
        "trusted_retry_authorization.content.first_batch_qa_closure",
        suffix=".json",
        content_addressed=False,
    )
    expected_state_hashes = [
        row["trusted_latest_state"]["pin"]["sha256"] for row in receipts[:8]
    ]
    if (
        content["status"] != "authorized"
        or content["task_id"] != task["task_id"]
        or content["task_guid"] != task["task_guid"]
        or content["semantic_id"] != semantic_id
        or content["first_batch_candidate_indices"] != list(range(8))
        or content["first_batch_latest_state_sha256s"] != expected_state_hashes
        or content["first_batch_outcome"] != "no_candidate_passed"
        or content["authorized_candidate_indices"] != list(range(8, 16))
    ):
        raise _error("retry authorization does not bind the failed first batch")
    normalized_content = {
        **content,
        "first_batch_selection_closure": selection_closure,
        "first_batch_qa_closure": qa_closure,
    }
    if normalized_content != authorization["content"]:
        raise _error("trusted retry authorization content is not canonical")
    return {"content": normalized_content, "pin": authorization["pin"]}


def _experiment_spec_binding(
    value: Any,
    *,
    semantic_id: str,
    generation_mode: str,
    species: str,
    task: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    prompt_contract: Mapping[str, Any],
    workflow_name: str,
    workflow_fingerprint: str,
    frame_count: int,
    input_fps: int,
    output_fps: int,
    seed: int,
) -> Dict[str, Any]:
    if value is None:
        raise _error("production verified receipt requires a pinned experiment spec")
    binding = _pinned_canonical_object(
        value,
        "verified_receipt.experiment_spec",
        expected_schema="autorig.animation-fitting-experiment.v1",
    )
    content = _exact_object(
        binding["content"],
        "verified_receipt.experiment_spec.content",
        (
            "schema",
            "experiment_id_string",
            "base_action_id_string",
            "species_string",
            "generation_mode_string",
            "frame_count_int",
            "input_fps_int",
            "output_fps_int",
            "seed_int",
            "positive_prompt_string",
            "negative_prompt_string",
            "reference_object",
            "workflow_object",
        ),
    )
    workflow = _exact_object(
        content["workflow_object"],
        "verified_receipt.experiment_spec.content.workflow_object",
        ("workflow_name_string", "workflow_fingerprint_sha256_string"),
    )
    reference = _exact_object(
        content["reference_object"],
        "verified_receipt.experiment_spec.content.reference_object",
        ("immutable_manifest_sha256_string", "source_model_sha256_string"),
    )
    if (
        content["base_action_id_string"] != semantic_id
        or content["species_string"] != species
        or content["generation_mode_string"] != generation_mode
        or content["frame_count_int"] != frame_count
        or content["input_fps_int"] != input_fps
        or content["output_fps_int"] != output_fps
        or content["seed_int"] != seed
    ):
        raise _error("experiment spec does not match canonical action identity")
    if (
        not isinstance(workflow, Mapping)
        or workflow.get("workflow_name_string") != workflow_name
        or workflow.get("workflow_fingerprint_sha256_string") != workflow_fingerprint
    ):
        raise _error("experiment spec does not match canonical workflow")
    if (
        not isinstance(reference, Mapping)
        or reference.get("immutable_manifest_sha256_string")
        != reference_manifest["pin"]["sha256"]
        or reference.get("source_model_sha256_string") != task["source_model_sha256"]
    ):
        raise _error("experiment spec does not match trusted reference/task pins")
    if (
        _sha256(str(content["positive_prompt_string"]).encode("utf-8"))
        != prompt_contract["positive_prompt_sha256"]
        or _sha256(str(content["negative_prompt_string"]).encode("utf-8"))
        != prompt_contract["negative_prompt_sha256"]
    ):
        raise _error(
            "experiment spec prompt hashes do not match canonical prompt contract"
        )
    _token(
        content["experiment_id_string"],
        "verified_receipt.experiment_spec.content.experiment_id_string",
    )
    return {"content": content, "pin": binding["pin"]}


def _load_taxonomy_clip(semantic_id: Any) -> tuple[Dict[str, Any], int]:
    action = str(semantic_id or "").strip().lower()
    taxonomy = _load_taxonomy()
    matches = [row for row in taxonomy.get("clips", ()) if row.get("id") == action]
    if len(matches) != 1:
        raise _error("semantic_id is not a canonical animal animation clip")
    clip = dict(matches[0])
    frame_profile = clip.get("frame_profile")
    if (
        isinstance(frame_profile, bool)
        or not isinstance(frame_profile, int)
        or frame_profile <= 0
    ):
        raise _error("canonical animation taxonomy frame_profile is invalid")
    return clip, 30


def _reject_client_owned_binding(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) == "browser_candidate_ingest":
                raise _error("client-supplied browser_candidate_ingest is forbidden")
            _reject_client_owned_binding(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_client_owned_binding(item)


def _production_request(value: Any) -> tuple[str, int, int, Dict[str, Any], int]:
    _reject_client_owned_binding(value)
    request = _exact_object(
        value,
        "client_request",
        ("schema", "semantic_id", "candidate_target", "candidate_limit"),
    )
    if request["schema"] != PLAN_REQUEST_SCHEMA:
        raise _error("client_request schema is invalid")
    clip, output_fps = _load_taxonomy_clip(request["semantic_id"])
    target = _positive_int(
        request["candidate_target"], "candidate_target", maximum=MAX_CANDIDATES
    )
    limit = _positive_int(
        request["candidate_limit"], "candidate_limit", maximum=MAX_CANDIDATES
    )
    if (target, limit) != (
        PRODUCTION_CANDIDATE_TARGET,
        PRODUCTION_CANDIDATE_LIMIT,
    ):
        raise _error(
            "production candidate policy must be exactly candidate_target=8 and candidate_limit=16"
        )
    return str(clip["id"]), target, limit, clip, output_fps


def _trusted_task(value: Any) -> Dict[str, Any]:
    task = _exact_object(
        value,
        "trusted_task",
        (
            "schema",
            "task_id",
            "task_guid",
            "status",
            "input_type",
            "source_rig_type",
            "source_model_sha256",
            "source_skeleton_sha256",
        ),
    )
    if task["schema"] != TRUSTED_TASK_PINS_SCHEMA:
        raise _error("trusted_task schema is invalid")
    if task["status"] != "done" or task["input_type"] != "animal":
        raise _error("trusted_task must be the resolver-pinned completed Animal task")
    rig = str(task["source_rig_type"] or "").strip()
    if not rig or len(rig) > 64 or not re.fullmatch(r"[A-Za-z0-9_]+", rig):
        raise _error("trusted_task.source_rig_type is invalid")
    return {
        "schema": TRUSTED_TASK_PINS_SCHEMA,
        "task_id": _canonical_uuid(task["task_id"], "trusted_task.task_id"),
        "task_guid": _canonical_uuid(task["task_guid"], "trusted_task.task_guid"),
        "status": "done",
        "input_type": "animal",
        "source_rig_type": rig,
        "source_model_sha256": _sha(
            task["source_model_sha256"], "trusted_task.source_model_sha256"
        ),
        "source_skeleton_sha256": _sha(
            task["source_skeleton_sha256"],
            "trusted_task.source_skeleton_sha256",
        ),
    }


def _verified_receipt_v1(value: Any) -> Dict[str, Any]:
    receipt = _exact_object(
        value,
        "verified_receipt",
        (
            "schema",
            "status",
            "candidate_index",
            "seed",
            "job_id",
            "prompt_id",
            "experiment_id",
            "experiment_sha256",
            "worker_id",
            "worker_base_url",
            "workflow_name",
            "workflow_fingerprint_sha256",
            "frame_count",
            "input_fps",
            "output_fps",
            "receipt",
            "source_video",
        ),
    )
    if receipt["schema"] != CONTROLLED_RECEIPT_SCHEMA_V1:
        raise _error("verified_receipt schema is invalid")
    if receipt["status"] != "completed":
        raise _error("verified_receipt must describe completed controlled generation")
    index = receipt["candidate_index"]
    if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < 16:
        raise _error("verified_receipt.candidate_index must be in 0..15")
    job_id = _sha(receipt["job_id"], "verified_receipt.job_id")
    source_video = _relative_artifact_pin(
        receipt["source_video"], "verified_receipt.source_video", suffix=".mp4"
    )
    source_parts = PurePosixPath(source_video["path"]).parts
    source_sha = source_video["sha256"]
    if source_parts != ("raw", source_sha[:2], f"{source_sha}.mp4"):
        raise _error(
            "verified_receipt source video must use raw/<sha-prefix>/<sha>.mp4"
        )
    state_pin = _relative_artifact_pin(
        receipt["receipt"],
        "verified_receipt.receipt",
        suffix=".json",
        content_addressed=False,
    )
    state_parts = PurePosixPath(state_pin["path"]).parts
    if (
        len(state_parts) != 3
        or state_parts[:2] != ("jobs", job_id)
        or not re.fullmatch(r"\d{6}\.json", state_parts[2])
    ):
        raise _error("verified_receipt state must use jobs/<job_id>/<six-digit>.json")
    output_fps = receipt["output_fps"]
    if (
        isinstance(output_fps, bool)
        or not isinstance(output_fps, (int, float))
        or not math.isfinite(float(output_fps))
        or float(output_fps) <= 0
    ):
        raise _error("verified_receipt.output_fps must be positive")
    return {
        "schema": CONTROLLED_RECEIPT_SCHEMA_V1,
        "status": "completed",
        "candidate_index": index,
        "seed": _seed(receipt["seed"], "verified_receipt.seed"),
        "job_id": job_id,
        "prompt_id": _token(receipt["prompt_id"], "verified_receipt.prompt_id"),
        "experiment_id": _token(
            receipt["experiment_id"], "verified_receipt.experiment_id"
        ),
        "experiment_sha256": _sha(
            receipt["experiment_sha256"], "verified_receipt.experiment_sha256"
        ),
        "worker_id": _token(receipt["worker_id"], "verified_receipt.worker_id"),
        "worker_base_url": _worker_url(
            receipt["worker_base_url"], "verified_receipt.worker_base_url"
        ),
        "workflow_name": _token(
            receipt["workflow_name"], "verified_receipt.workflow_name"
        ),
        "workflow_fingerprint_sha256": _sha(
            receipt["workflow_fingerprint_sha256"],
            "verified_receipt.workflow_fingerprint_sha256",
        ),
        "frame_count": _positive_int(
            receipt["frame_count"], "verified_receipt.frame_count"
        ),
        "input_fps": _positive_int(receipt["input_fps"], "verified_receipt.input_fps"),
        "output_fps": int(output_fps)
        if float(output_fps).is_integer()
        else float(output_fps),
        "receipt": state_pin,
        "source_video": source_video,
    }


def _verified_receipt_v2(
    value: Any,
    *,
    trusted_latest_state: Mapping[str, Any],
    semantic_id: str,
    generation_mode: str,
    task: Mapping[str, Any],
    prompt_contract: Mapping[str, Any],
    reference_manifest: Mapping[str, Any],
    workflow_name: str,
    workflow_fingerprint: str,
    frame_count: int,
    input_fps: int,
    output_fps: int,
) -> Dict[str, Any]:
    receipt = _exact_object(
        value,
        "verified_receipt",
        (
            "schema",
            "status",
            "candidate_index",
            "seed",
            "job_id",
            "prompt_id",
            "semantic_id",
            "generation_mode",
            "task",
            "prompt_contract",
            "reference_manifest",
            "experiment_id",
            "experiment_sha256",
            "experiment_spec",
            "worker_id",
            "worker_base_url",
            "workflow_name",
            "workflow_fingerprint_sha256",
            "frame_count",
            "input_fps",
            "output_fps",
            "source_video",
        ),
    )
    if receipt["schema"] != CONTROLLED_RECEIPT_SCHEMA_V2:
        raise _error("production verified_receipt schema must be descriptor.v2")
    if receipt["status"] != "completed":
        raise _error("verified_receipt must describe completed controlled generation")
    index = receipt["candidate_index"]
    if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < 16:
        raise _error("verified_receipt.candidate_index must be in 0..15")
    if (
        receipt["semantic_id"] != semantic_id
        or receipt["generation_mode"] != generation_mode
    ):
        raise _error(
            "verified receipt action identity does not match canonical taxonomy"
        )
    if receipt["task"] != task:
        raise _error("verified receipt task pins do not match trusted task")
    if receipt["prompt_contract"] != prompt_contract:
        raise _error("verified receipt prompt contract does not match canonical prompt")
    if receipt["reference_manifest"] != reference_manifest:
        raise _error(
            "verified receipt reference manifest does not match trusted reference"
        )
    if (
        receipt["workflow_name"] != workflow_name
        or receipt["workflow_fingerprint_sha256"] != workflow_fingerprint
    ):
        raise _error("verified receipt workflow does not match canonical action mode")
    if (
        receipt["frame_count"] != frame_count
        or receipt["input_fps"] != input_fps
        or receipt["output_fps"] != output_fps
    ):
        raise _error("verified receipt does not match canonical action timing")
    seed = _seed(receipt["seed"], "verified_receipt.seed")
    job_id = _sha(receipt["job_id"], "verified_receipt.job_id")
    prompt_id = _token(receipt["prompt_id"], "verified_receipt.prompt_id")
    if prompt_id != derive_controlled_prompt_id(job_id):
        raise _error("verified receipt prompt_id is not derived from its job_id")
    if trusted_latest_state["job_id"] != job_id:
        raise _error("trusted latest-state does not bind the verified receipt job")
    source_video = _relative_artifact_pin(
        receipt["source_video"], "verified_receipt.source_video", suffix=".mp4"
    )
    source_sha = source_video["sha256"]
    if PurePosixPath(source_video["path"]).parts != (
        "raw",
        source_sha[:2],
        f"{source_sha}.mp4",
    ):
        raise _error(
            "verified_receipt source video must use raw/<sha-prefix>/<sha>.mp4"
        )
    experiment = _experiment_spec_binding(
        receipt["experiment_spec"],
        semantic_id=semantic_id,
        generation_mode=generation_mode,
        species=str(prompt_contract["species"]),
        task=task,
        reference_manifest=reference_manifest,
        prompt_contract=prompt_contract,
        workflow_name=workflow_name,
        workflow_fingerprint=workflow_fingerprint,
        frame_count=frame_count,
        input_fps=input_fps,
        output_fps=output_fps,
        seed=seed,
    )
    experiment_id = _token(receipt["experiment_id"], "verified_receipt.experiment_id")
    experiment_sha = _sha(
        receipt["experiment_sha256"], "verified_receipt.experiment_sha256"
    )
    if (
        experiment_id != experiment["content"]["experiment_id_string"]
        or experiment_sha != experiment["pin"]["sha256"]
    ):
        raise _error("verified receipt experiment identity does not match its spec pin")
    # Require the caller's object to already be normalized.  This catches URL,
    # token, pin and experiment wrapper spelling changes before identity hashing.
    normalized_input = {
        "schema": CONTROLLED_RECEIPT_SCHEMA_V2,
        "status": "completed",
        "candidate_index": index,
        "seed": seed,
        "job_id": job_id,
        "prompt_id": prompt_id,
        "semantic_id": semantic_id,
        "generation_mode": generation_mode,
        "task": dict(task),
        "prompt_contract": dict(prompt_contract),
        "reference_manifest": dict(reference_manifest),
        "experiment_id": experiment_id,
        "experiment_sha256": experiment_sha,
        "experiment_spec": experiment,
        "worker_id": _token(receipt["worker_id"], "verified_receipt.worker_id"),
        "worker_base_url": _worker_url(
            receipt["worker_base_url"], "verified_receipt.worker_base_url"
        ),
        "workflow_name": workflow_name,
        "workflow_fingerprint_sha256": workflow_fingerprint,
        "frame_count": frame_count,
        "input_fps": input_fps,
        "output_fps": output_fps,
        "source_video": source_video,
    }
    if normalized_input != receipt:
        raise _error("verified receipt descriptor.v2 is not canonical")
    return {**normalized_input, "trusted_latest_state": dict(trusted_latest_state)}


def _derive_canonical_seed(task_id: str, semantic_id: str, index: int) -> int:
    # Keep one seed authority.  The import is intentionally lazy so this pure
    # module can be syntax/unit tested without importing the database stack.
    from animation_fitting_candidate_ingest import derive_browser_candidate_seed

    return int(derive_browser_candidate_seed(task_id, semantic_id, index))


def derive_controlled_prompt_id(job_id: str) -> str:
    """Duplicate the controlled-experiment deterministic UUID-shaped ID."""

    canonical_job_id = _sha(job_id, "job_id")
    digest = hashlib.sha256(
        f"autorig-controlled-animation-fitting:{canonical_job_id}".encode("utf-8")
    ).hexdigest()[:32]
    return (
        f"{digest[:8]}-{digest[8:12]}-4{digest[13:16]}-8{digest[17:20]}-{digest[20:]}"
    )


def _receipt_inventory(
    receipts: Sequence[Mapping[str, Any]],
    *,
    trusted_latest_states: Sequence[Mapping[str, Any]],
    task_id: str,
    semantic_id: str,
    frame_count: int,
    input_fps: int,
    output_fps: int,
    target: int,
    limit: int,
    derive_seeds: bool,
    allowed_receipt_counts: tuple[int, ...],
    receipt_schema: str,
    generation_mode: str | None = None,
    task: Mapping[str, Any] | None = None,
    prompt_contract: Mapping[str, Any] | None = None,
    reference_manifest: Mapping[str, Any] | None = None,
    workflow_name: str | None = None,
    workflow_fingerprint: str | None = None,
) -> tuple[Dict[str, Any], ...]:
    if isinstance(receipts, (str, bytes, bytearray)) or not isinstance(
        receipts, Sequence
    ):
        raise _error("verified_receipts must be a sequence")
    latest_by_job = _trusted_latest_state_inventory(trusted_latest_states)

    def latest_for_receipt(row: Mapping[str, Any]) -> Dict[str, Any]:
        job_id = _sha(row.get("job_id"), "verified_receipt.job_id")
        latest = latest_by_job.get(job_id)
        if latest is None:
            raise _error("verified receipt has no independent trusted latest-state")
        return latest

    if receipt_schema == CONTROLLED_RECEIPT_SCHEMA_V1:
        parsed = [_verified_receipt_v1(row) for row in receipts]
    elif receipt_schema == CONTROLLED_RECEIPT_SCHEMA_V2:
        if not all(
            value is not None
            for value in (
                generation_mode,
                task,
                prompt_contract,
                reference_manifest,
                workflow_name,
                workflow_fingerprint,
            )
        ):
            raise _error("production receipt verification context is incomplete")
        parsed = [
            _verified_receipt_v2(
                row,
                trusted_latest_state=latest_for_receipt(row),
                semantic_id=semantic_id,
                generation_mode=str(generation_mode),
                task=task or {},
                prompt_contract=prompt_contract or {},
                reference_manifest=reference_manifest or {},
                workflow_name=str(workflow_name),
                workflow_fingerprint=str(workflow_fingerprint),
                frame_count=frame_count,
                input_fps=input_fps,
                output_fps=output_fps,
            )
            for row in receipts
        ]
    else:
        raise _error("unsupported controlled generation receipt descriptor schema")
    receipt_job_ids = {row["job_id"] for row in parsed}
    if set(latest_by_job) != receipt_job_ids:
        raise _error("trusted latest-state inventory must exactly match receipt jobs")
    if receipt_schema == CONTROLLED_RECEIPT_SCHEMA_V1:
        for row in parsed:
            latest = latest_by_job[row["job_id"]]
            if (
                latest["pin"] != row["receipt"]
                or latest["filename"] != PurePosixPath(row["receipt"]["path"]).name
            ):
                raise _error(
                    "legacy receipt does not match authoritative trusted latest-state"
                )
            row["trusted_latest_state"] = latest
    if len(parsed) not in allowed_receipt_counts:
        raise _error(
            "completed receipt count does not match an authorized candidate batch"
        )
    parsed.sort(key=lambda row: row["candidate_index"])
    if [row["candidate_index"] for row in parsed] != list(range(len(parsed))):
        raise _error("verified receipt candidate indices must be contiguous from zero")
    if len({row["job_id"] for row in parsed}) != len(parsed):
        raise _error("verified receipts must reference unique controlled jobs")
    if len({row["source_video"]["sha256"] for row in parsed}) != len(parsed):
        raise _error("verified receipts must reference unique controlled videos")
    if len({row["trusted_latest_state"]["pin"]["sha256"] for row in parsed}) != len(
        parsed
    ):
        raise _error("verified receipts must reference unique completed states")
    workflow_pairs = {
        (row["workflow_name"], row["workflow_fingerprint_sha256"]) for row in parsed
    }
    worker_pairs = {(row["worker_id"], row["worker_base_url"]) for row in parsed}
    timing_triples = {
        (row["frame_count"], row["input_fps"], row["output_fps"]) for row in parsed
    }
    if len(workflow_pairs) != 1:
        raise _error("verified receipts do not share one pinned workflow")
    if len(worker_pairs) != 1:
        raise _error("verified receipts do not share one pinned worker")
    if timing_triples != {(frame_count, input_fps, output_fps)}:
        raise _error("verified receipts do not match canonical action timing")
    if derive_seeds:
        for row in parsed:
            expected = _derive_canonical_seed(
                task_id, semantic_id, row["candidate_index"]
            )
            if row["seed"] != expected:
                raise _error(
                    "verified receipt seed is not the canonical server-derived seed"
                )
    return tuple(parsed)


def _build_plan(
    *,
    schema: str,
    binding_schema: str,
    ingest_schema: str,
    semantic_id: str,
    generation_mode: str,
    target: int,
    limit: int,
    mode: str,
    production_eligible: bool,
    task: Dict[str, Any],
    receipts: tuple[Dict[str, Any], ...],
    reference_manifest: Dict[str, Any] | None = None,
    retry_authorization: Dict[str, Any] | None = None,
) -> BrowserCandidateJobPlan:
    first = receipts[0]
    if ingest_schema == INGEST_SCHEMA_V1:
        slots = [
            {
                "candidate_index": row["candidate_index"],
                "seed": row["seed"],
                "source_video": row["source_video"],
                "controlled_generation": {
                    "job_id": row["job_id"],
                    "prompt_id": row["prompt_id"],
                    "experiment_id": row["experiment_id"],
                    "experiment_sha256": row["experiment_sha256"],
                    "worker_id": row["worker_id"],
                    "worker_base_url": row["worker_base_url"],
                    "workflow_fingerprint_sha256": row["workflow_fingerprint_sha256"],
                },
            }
            for row in receipts
        ]
    else:
        slots = [
            {
                "candidate_index": row["candidate_index"],
                "seed": row["seed"],
                "source_video": row["source_video"],
                "controlled_generation": {
                    "job_id": row["job_id"],
                    "prompt_id": row["prompt_id"],
                    "semantic_id": row["semantic_id"],
                    "generation_mode": row["generation_mode"],
                    "task": row["task"],
                    "prompt_contract": row["prompt_contract"],
                    "reference_manifest": row["reference_manifest"],
                    "experiment_id": row["experiment_id"],
                    "experiment_sha256": row["experiment_sha256"],
                    "experiment_spec": row["experiment_spec"],
                    "worker_id": row["worker_id"],
                    "worker_base_url": row["worker_base_url"],
                    "workflow_name": row["workflow_name"],
                    "workflow_fingerprint_sha256": row["workflow_fingerprint_sha256"],
                    "trusted_latest_state": row["trusted_latest_state"],
                    "retry_authorization_sha256": (
                        retry_authorization["pin"]["sha256"]
                        if retry_authorization is not None
                        and row["candidate_index"] >= 8
                        else None
                    ),
                },
            }
            for row in receipts
        ]
    common = {
        "schema": ingest_schema,
        "task_id": task["task_id"],
        "task_guid": task["task_guid"],
        "source_rig_type": task["source_rig_type"],
        "source_model_sha256": task["source_model_sha256"],
        "source_skeleton_sha256": task["source_skeleton_sha256"],
        "frame_count": first["frame_count"],
        "input_fps": first["input_fps"],
        "output_fps": first["output_fps"],
    }
    if ingest_schema == INGEST_SCHEMA_V1:
        ingest = {
            **common,
            "candidate_seed": first["seed"],
            "source_video": first["source_video"],
            "controlled_generation": slots[0]["controlled_generation"],
        }
    else:
        ingest = {
            **common,
            "semantic_id": semantic_id,
            "generation_mode": generation_mode,
            "task": first["task"],
            "prompt_contract": first["prompt_contract"],
            "reference_manifest": first["reference_manifest"],
            "workflow_name": first["workflow_name"],
            "workflow_fingerprint_sha256": first["workflow_fingerprint_sha256"],
            "candidate_slots": slots,
        }
    job_plan_binding = {
        "schema": binding_schema,
        "production_eligible": production_eligible,
        "trusted_task": task,
        "verified_controlled_generation_receipts": list(receipts),
    }
    if ingest_schema == INGEST_SCHEMA_V2:
        job_plan_binding.update(
            {
                "trusted_reference_manifest": reference_manifest,
                "trusted_latest_states": [
                    row["trusted_latest_state"] for row in receipts
                ],
                "trusted_retry_authorization": retry_authorization,
            }
        )
    config = {
        "browser_candidate_ingest": ingest,
        "browser_candidate_job_plan": job_plan_binding,
        "browser_candidate_selection": {
            "schema": SELECTION_CONFIG_SCHEMA,
            "mode": mode,
        },
    }
    config_bytes = canonical_json_bytes(config)
    config_sha = _sha256(config_bytes)
    identity = {
        "schema": schema,
        "semantic_id": semantic_id,
        "generation_mode": generation_mode,
        "worker_id": first["worker_id"],
        "worker_base_url": first["worker_base_url"],
        "workflow_name": first["workflow_name"],
        "workflow_fingerprint": first["workflow_fingerprint_sha256"],
        "candidate_target": target,
        "candidate_limit": limit,
        "selection_mode": mode,
        "production_eligible": production_eligible,
        "config_sha256": config_sha,
    }
    if ingest_schema == INGEST_SCHEMA_V2:
        identity.update(
            {
                "task_id": task["task_id"],
                "task_guid": task["task_guid"],
                "source_model_sha256": task["source_model_sha256"],
                "source_skeleton_sha256": task["source_skeleton_sha256"],
                "frame_profile": first["frame_count"],
                "input_fps": first["input_fps"],
                "output_fps": first["output_fps"],
                "prompt_spec_sha256": first["prompt_contract"]["spec_pin"]["sha256"],
                "positive_prompt_sha256": first["prompt_contract"][
                    "positive_prompt_sha256"
                ],
                "negative_prompt_sha256": first["prompt_contract"][
                    "negative_prompt_sha256"
                ],
                "reference_manifest_sha256": first["reference_manifest"]["pin"][
                    "sha256"
                ],
                "reference_sha256": first["reference_manifest"]["content"][
                    "reference_artifact"
                ]["sha256"],
                "experiment_spec_sha256_by_candidate": [
                    row["experiment_spec"]["pin"]["sha256"] for row in receipts
                ],
                "controlled_state_receipt_sha256_by_candidate": [
                    row["trusted_latest_state"]["pin"]["sha256"] for row in receipts
                ],
                "retry_authorization_sha256": (
                    retry_authorization["pin"]["sha256"]
                    if retry_authorization is not None
                    else None
                ),
            }
        )
    identity_sha = _sha256(canonical_json_bytes(identity))
    return BrowserCandidateJobPlan(
        schema=schema,
        semantic_id=semantic_id,
        generation_mode=generation_mode,
        worker_id=first["worker_id"],
        worker_base_url=first["worker_base_url"],
        workflow_name=first["workflow_name"],
        workflow_fingerprint=first["workflow_fingerprint_sha256"],
        candidate_target=target,
        candidate_limit=limit,
        selection_mode=mode,
        production_eligible=production_eligible,
        config=config,
        config_json=config_bytes.decode("utf-8"),
        config_sha256=config_sha,
        identity_sha256=identity_sha,
        idempotency_key=f"autorig-browser-animation-job-plan:{identity_sha}",
        prompt_id=identity_sha,
    )


def build_production_browser_candidate_job_plan(
    client_request: Mapping[str, Any],
    *,
    trusted_task: Mapping[str, Any],
    trusted_reference_manifest: Mapping[str, Any],
    trusted_latest_states: Sequence[Mapping[str, Any]],
    trusted_retry_authorization: Mapping[str, Any] | None,
    verified_receipts: Sequence[Mapping[str, Any]],
) -> BrowserCandidateJobPlan:
    """Build V2 solely from independent task/reference/state/retry resolvers."""
    semantic_id, target, limit, clip, output_fps = _production_request(client_request)
    task = _trusted_task(trusted_task)
    species = _species_for_task(task)
    prompt_contract, workflow_contract = _canonical_prompt_and_workflow_contract(
        semantic_id=semantic_id,
        clip=clip,
        species=species,
    )
    reference_manifest = _trusted_reference_manifest(
        trusted_reference_manifest,
        task=task,
        species=species,
    )
    generation_mode = str(workflow_contract["generation_mode"])
    receipts = _receipt_inventory(
        verified_receipts,
        trusted_latest_states=trusted_latest_states,
        task_id=task["task_id"],
        semantic_id=semantic_id,
        frame_count=int(clip["frame_profile"]),
        input_fps=24,
        output_fps=output_fps,
        target=target,
        limit=limit,
        derive_seeds=True,
        allowed_receipt_counts=(
            PRODUCTION_CANDIDATE_TARGET,
            PRODUCTION_CANDIDATE_LIMIT,
        ),
        receipt_schema=CONTROLLED_RECEIPT_SCHEMA_V2,
        generation_mode=generation_mode,
        task=task,
        prompt_contract=prompt_contract,
        reference_manifest=reference_manifest,
        workflow_name=str(workflow_contract["workflow_name"]),
        workflow_fingerprint=str(workflow_contract["workflow_fingerprint_sha256"]),
    )
    if len(receipts) == PRODUCTION_CANDIDATE_TARGET:
        if trusted_retry_authorization is not None:
            raise _error("first candidate batch must not carry retry authorization")
        retry_authorization = None
    else:
        if trusted_retry_authorization is None:
            raise _error("second candidate batch requires server retry authorization")
        retry_authorization = _trusted_retry_authorization(
            trusted_retry_authorization,
            task=task,
            semantic_id=semantic_id,
            receipts=receipts,
        )
    return _build_plan(
        schema=PLAN_IDENTITY_SCHEMA_V2,
        binding_schema=PLAN_BINDING_SCHEMA_V2,
        ingest_schema=INGEST_SCHEMA_V2,
        semantic_id=semantic_id,
        generation_mode=generation_mode,
        target=target,
        limit=limit,
        mode="production",
        production_eligible=True,
        task=task,
        receipts=receipts,
        reference_manifest=reference_manifest,
        retry_authorization=retry_authorization,
    )


def build_v14_nonproduction_canary_job_plan(
    *,
    trusted_task: Mapping[str, Any],
    trusted_latest_state: Mapping[str, Any],
    verified_receipt: Mapping[str, Any],
) -> BrowserCandidateJobPlan:
    """Bind fixed V14 V1 only against an independent authoritative latest state."""
    task = _trusted_task(trusted_task)
    if task["source_rig_type"].upper() != "HORSE_2":
        raise _error("V14 canary requires the canonical HORSE_2 source rig")
    if (
        task["source_model_sha256"] != V14_SOURCE_MODEL_SHA256
        or task["source_skeleton_sha256"] != V14_SOURCE_SKELETON_SHA256
    ):
        raise _error("trusted task is not the exact canonical V14 model and skeleton")
    receipt = _verified_receipt_v1(verified_receipt)
    if (
        receipt["candidate_index"] != 0
        or receipt["seed"] != V14_FIXED_SEED
        or receipt["job_id"] != V14_CONTROLLED_JOB_ID
        or receipt["prompt_id"] != V14_PROMPT_ID
        or receipt["experiment_id"] != V14_EXPERIMENT_ID
        or receipt["experiment_sha256"] != V14_EXPERIMENT_SHA256
        or receipt["worker_id"] != V14_WORKER_ID
        or receipt["worker_base_url"] != V14_WORKER_BASE_URL
        or receipt["workflow_name"] != V14_WORKFLOW_NAME
        or receipt["workflow_fingerprint_sha256"] != V14_WORKFLOW_FINGERPRINT_SHA256
        or receipt["frame_count"] != 49
        or receipt["input_fps"] != 24
        or receipt["output_fps"] != 30
    ):
        raise _error("verified receipt is not the exact immutable V14 runtime identity")
    receipts = _receipt_inventory(
        (receipt,),
        trusted_latest_states=(trusted_latest_state,),
        task_id=task["task_id"],
        semantic_id=V14_SEMANTIC_ID,
        frame_count=49,
        input_fps=24,
        output_fps=30,
        target=1,
        limit=1,
        derive_seeds=False,
        allowed_receipt_counts=(1,),
        receipt_schema=CONTROLLED_RECEIPT_SCHEMA_V1,
    )
    return _build_plan(
        schema=PLAN_IDENTITY_SCHEMA_V1,
        binding_schema=PLAN_BINDING_SCHEMA_V1,
        ingest_schema=INGEST_SCHEMA_V1,
        semantic_id=V14_SEMANTIC_ID,
        generation_mode="loop",
        target=1,
        limit=1,
        mode="canary_single_candidate",
        production_eligible=False,
        task=task,
        receipts=receipts,
    )

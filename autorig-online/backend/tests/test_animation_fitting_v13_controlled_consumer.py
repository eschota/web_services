from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from animation_fitting.comfy import ComfyWorker
from animation_fitting.controlled_experiment import (
    V12_ENDPOINT_GUIDE_SHA256,
    V12_EXPERIMENT_ID,
    V12_GUIDE_BUNDLE_ID,
    V12_GUIDE_FRAME_INDICES,
    V12_GUIDE_MANIFEST_SHA256,
    V12_GUIDE_PINS,
    V12_GUIDE_STRENGTHS,
    V12_RECOVERY_GUIDE_FRAME_INDICES,
    V13_BASE_VIDEO_LATENT_SLICES,
    V13_EXPERIMENT_ID,
    V13_EXPERIMENT_SPEC_SHA256,
    V13_FINAL_TEMPORAL_LATENT_SLICES,
    V13_GUIDE_STRENGTHS,
    V13_HARD_GUIDE_CONTRACT,
    ControlledExperimentError,
    _job_identity,
    _load_browser_hard_guide_sequence,
    load_controlled_plan,
    patch_browser_keyframe_guides,
    patch_native_resolution,
)


def v13_spec_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v13_browser_hard_guides_seed_6550110377254033429.v1.json"
    )


def real_fixture_paths() -> tuple[Path, Path, Path]:
    return (
        Path(r"R:\ComfyUI-data\autorig-fitting\horse-canonical-f1"),
        Path(
            r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
            r"\horse-walk-v12-browser-recovery-guides-f2"
        ),
        Path(
            r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        ),
    )


def test_v13_checked_in_spec_is_exact_hard_nine_derivation_and_unapproved() -> None:
    spec_bytes = v13_spec_path().read_bytes()
    spec = json.loads(spec_bytes)
    assert hashlib.sha256(spec_bytes).hexdigest() == V13_EXPERIMENT_SPEC_SHA256
    assert spec["experiment_id_string"] == V13_EXPERIMENT_ID
    assert spec["seed_int"] == 6550110377254033429
    assert spec["status_string"] == "prepared_for_single_controlled_generation"
    sequence = spec["guide_sequence_object"]
    assert sequence["bundle_id_string"] == V12_GUIDE_BUNDLE_ID
    assert (
        sequence["immutable_manifest_sha256_string"]
        == V12_GUIDE_MANIFEST_SHA256
    )
    assert sequence["endpoint_guide_sha256_string"] == V12_ENDPOINT_GUIDE_SHA256
    assert [row["frame_index_int"] for row in sequence["frames_array"]] == list(
        V12_GUIDE_FRAME_INDICES
    )
    assert [row["strength_float"] for row in sequence["frames_array"]] == list(
        V13_GUIDE_STRENGTHS
    )
    hard = spec["hard_guide_contract_object"]
    assert hard["hard_guide_contract_string"] == V13_HARD_GUIDE_CONTRACT
    assert hard["source_experiment_id_string"] == V12_EXPERIMENT_ID
    assert hard["source_strengths_array"] == list(V12_GUIDE_STRENGTHS)
    assert hard["generation_strengths_array"] == list(V13_GUIDE_STRENGTHS)
    assert hard["ltxv_add_guide_count_int"] == 9
    assert hard["base_video_latent_slices_int"] == V13_BASE_VIDEO_LATENT_SLICES
    assert (
        hard["final_temporal_latent_slices_int"]
        == V13_FINAL_TEMPORAL_LATENT_SLICES
    )
    assert hard["no_additional_guide_frames_bool"] is True
    assert spec["variants_array"] == [
        {
            "variant_id_string": "browser_hard_guides_seed_a",
            "start_guide_strength_float": 1.0,
            "end_guide_strength_float": 1.0,
        }
    ]
    positive = spec["positive_prompt_string"]
    negative = spec["negative_prompt_string"]
    for phrase in (
        "mandatory exact frame anchors",
        "never suppress, replace, shift, blend away or pre-empt an anchor",
        "hind-left only during frames 1-11",
        "fore-left only during frames 13-23",
        "hind-right only during frames 25-35",
        "fore-right only during frames 37-47",
        "no next swing begins before the preceding recovery anchor is reproduced",
    ):
        assert phrase in positive
    for phrase in (
        "ignored guide",
        "guide substitution",
        "suppressed swing apex",
        "recovery anchor drift",
        "early next swing",
        "late previous touchdown",
    ):
        assert phrase in negative
    assert spec["generation_authorization_object"]["authorized_bool"] is False
    assert spec["approved_bool"] is False


@pytest.mark.parametrize(
    "mutation",
    ["prompt", "seed", "strength", "hard_contract", "extra_spec_field"],
)
def test_v13_same_id_mutation_rejects_before_reference_or_bundle_reads(
    tmp_path: Path, mutation: str
) -> None:
    payload = json.loads(v13_spec_path().read_text())
    if mutation == "prompt":
        payload["positive_prompt_string"] += " mutated"
    elif mutation == "seed":
        payload["seed_int"] += 1
    elif mutation == "strength":
        payload["guide_sequence_object"]["frames_array"][4]["strength_float"] = 0.99
    elif mutation == "hard_contract":
        payload["hard_guide_contract_object"]["final_temporal_latent_slices_int"] = 17
    else:
        payload["arbitrary_mutation_bool"] = True
    assert payload["experiment_id_string"] == V13_EXPERIMENT_ID
    mutated = tmp_path / "mutated-v13.json"
    mutated.write_text(json.dumps(payload, indent=2) + "\n")

    with pytest.raises(ControlledExperimentError, match="exact code-owned checked-in pin"):
        load_controlled_plan(
            experiment_path=mutated,
            authorization=V13_EXPERIMENT_ID,
            reference_bundle=tmp_path / "must-not-read-reference",
            guide_bundle=tmp_path / "must-not-read-guides",
            artifact_root=tmp_path / "artifacts",
        )


def test_v13_derivation_contract_rejects_before_missing_bundle_access(
    tmp_path: Path,
) -> None:
    spec = json.loads(v13_spec_path().read_text())
    spec["hard_guide_contract_object"]["ltxv_add_guide_count_int"] = 10
    with pytest.raises(ControlledExperimentError, match="hard-guide derivation contract"):
        _load_browser_hard_guide_sequence(
            spec,
            guide_bundle=tmp_path / "must-not-read-guides",
            reference_sha256="9" * 64,
            frame_count=49,
            start_strength=1.0,
            end_strength=1.0,
            pins=V12_GUIDE_PINS,
        )


def test_v13_real_f2_full_validation_derives_hard_frames_without_mutation() -> None:
    reference, guides, artifacts = real_fixture_paths()
    missing = [path for path in (v13_spec_path(), reference, guides, artifacts) if not path.exists()]
    if missing:
        pytest.skip(f"external real v12 f2 fixtures are absent: {missing}")
    manifest_path = guides / "immutable_manifest.json"
    manifest_before = manifest_path.read_bytes()

    plan = load_controlled_plan(
        experiment_path=v13_spec_path(),
        authorization=V13_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    assert manifest_path.read_bytes() == manifest_before
    assert hashlib.sha256(manifest_before).hexdigest() == V12_GUIDE_MANIFEST_SHA256
    assert plan.experiment_sha256 == V13_EXPERIMENT_SPEC_SHA256
    assert plan.guide_manifest_sha256 == V12_GUIDE_MANIFEST_SHA256
    assert plan.start_guide_strength == plan.end_guide_strength == 1.0
    assert [frame.frame_index for frame in plan.guide_frames] == list(
        V12_GUIDE_FRAME_INDICES
    )
    assert [frame.strength for frame in plan.guide_frames] == list(
        V13_GUIDE_STRENGTHS
    )
    by_index = {frame.frame_index: frame for frame in plan.guide_frames}
    for frame_index in (0, *V12_RECOVERY_GUIDE_FRAME_INDICES, 48):
        assert by_index[frame_index].sha256 == V12_ENDPOINT_GUIDE_SHA256
    assert len({frame.sha256 for frame in plan.guide_frames}) == 5

    worker = ComfyWorker(
        worker_id="v13-test-worker",
        base_url="http://127.0.0.1:8188",
        workflow_name=plan.workflow_name,
        expected_workflow_fingerprint=plan.workflow_fingerprint,
    )
    identity, job_id, idempotency_key = _job_identity(plan, worker)
    identity_frames = identity["browser_guide_sequence_object"]["frames_array"]
    assert [row["frame_index_int"] for row in identity_frames] == list(
        V12_GUIDE_FRAME_INDICES
    )
    assert [row["strength_float"] for row in identity_frames] == list(
        V13_GUIDE_STRENGTHS
    )
    assert len(job_id) == 64
    assert idempotency_key.endswith(job_id)


def test_v13_graph_is_exact_hard_nine_with_sixteen_temporal_slices() -> None:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "workflows"
        / "autorig_ltx2_animal_loop_v1_api.json"
    )
    original = json.loads(workflow_path.read_text())
    native = patch_native_resolution(
        original, width=768, height=448, resize_longer=768
    )
    endpoint = "autorig_animation_fitting/v13_endpoint.png"
    uploads = {
        0: endpoint,
        6: "autorig_animation_fitting/v13_swing_006.png",
        12: endpoint,
        18: "autorig_animation_fitting/v13_swing_018.png",
        24: endpoint,
        30: "autorig_animation_fitting/v13_swing_030.png",
        36: endpoint,
        42: "autorig_animation_fitting/v13_swing_042.png",
        48: endpoint,
    }
    strengths = dict(zip(V12_GUIDE_FRAME_INDICES, V13_GUIDE_STRENGTHS))
    result = patch_browser_keyframe_guides(
        native,
        uploaded_images=uploads,
        strengths=strengths,
    )
    titled = {
        node.get("_meta", {}).get("title"): (str(node_id), node)
        for node_id, node in result.items()
        if isinstance(node, dict) and node.get("_meta", {}).get("title")
    }
    start_id, start = titled["AUTORIG_START_GUIDE"]
    end_id, end = titled["AUTORIG_END_GUIDE_N_MINUS_1"]
    ordered_ids = [
        start_id,
        *[f"94{frame_index:03d}" for frame_index in V12_GUIDE_FRAME_INDICES[1:-1]],
        end_id,
    ]
    assert [result[node_id]["inputs"]["frame_idx"] for node_id in ordered_ids] == [
        0,
        6,
        12,
        18,
        24,
        30,
        36,
        42,
        -1,
    ]
    assert [result[node_id]["inputs"]["strength"] for node_id in ordered_ids] == list(
        V13_GUIDE_STRENGTHS
    )
    for previous_id, node_id in zip(ordered_ids, ordered_ids[1:]):
        assert result[node_id]["inputs"]["positive"] == [previous_id, 0]
        assert result[node_id]["inputs"]["negative"] == [previous_id, 1]
        assert result[node_id]["inputs"]["latent"] == [previous_id, 2]
    for frame_index in V12_RECOVERY_GUIDE_FRAME_INDICES:
        recovery = result[f"94{frame_index:03d}"]
        assert recovery["inputs"]["image"] == start["inputs"]["image"]
    assert end["inputs"]["image"] == start["inputs"]["image"]
    guide_count = sum(
        1
        for node in result.values()
        if isinstance(node, dict) and node.get("class_type") == "LTXVAddGuide"
    )
    latent = next(
        node
        for node in result.values()
        if isinstance(node, dict) and node.get("class_type") == "EmptyLTXVLatentVideo"
    )
    base_slices = ((latent["inputs"]["length"] - 1) // 8) + 1
    assert guide_count == 9
    assert base_slices == V13_BASE_VIDEO_LATENT_SLICES
    assert base_slices + guide_count == V13_FINAL_TEMPORAL_LATENT_SLICES

    invalid_strengths = dict(strengths)
    invalid_strengths[30] = 0.99
    with pytest.raises(ControlledExperimentError, match="must match exactly"):
        patch_browser_keyframe_guides(
            native,
            uploaded_images=uploads,
            strengths=invalid_strengths,
        )


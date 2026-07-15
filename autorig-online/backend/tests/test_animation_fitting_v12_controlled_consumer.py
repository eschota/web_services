from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from animation_fitting.controlled_experiment import (
    V12_ENDPOINT_GUIDE_SHA256,
    V12_EXPERIMENT_ID,
    V12_EXPERIMENT_SPEC_SHA256,
    V12_GUIDE_BUNDLE_ID,
    V12_GUIDE_CLI_SHA256,
    V12_GUIDE_CONTRACT,
    V12_GUIDE_FRAME_INDICES,
    V12_GUIDE_MANIFEST_SHA256,
    V12_GUIDE_PINS,
    V12_GUIDE_STRENGTHS,
    V12_RECOVERY_GUIDE_FRAME_INDICES,
    ControlledExperimentError,
    _load_browser_recovery_guide_sequence,
    load_controlled_plan,
    patch_browser_keyframe_guides,
    patch_native_resolution,
)


def v12_spec_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v12_browser_recovery_guides_seed_6550110377254033429.v1.json"
    )


def test_v12_checked_in_spec_pins_exact_f2_recovery_bundle_and_stays_unapproved() -> None:
    spec_bytes = v12_spec_path().read_bytes()
    spec = json.loads(spec_bytes)
    sequence = spec["guide_sequence_object"]
    assert hashlib.sha256(spec_bytes).hexdigest() == V12_EXPERIMENT_SPEC_SHA256
    assert spec["experiment_id_string"] == V12_EXPERIMENT_ID
    assert spec["seed_int"] == 6550110377254033429
    assert spec["status_string"] == "prepared_for_single_controlled_generation"
    assert sequence["guide_contract_string"] == V12_GUIDE_CONTRACT
    assert sequence["bundle_id_string"] == V12_GUIDE_BUNDLE_ID
    assert sequence["immutable_manifest_sha256_string"] == V12_GUIDE_MANIFEST_SHA256
    assert sequence["endpoint_guide_sha256_string"] == V12_ENDPOINT_GUIDE_SHA256
    assert [row["frame_index_int"] for row in sequence["frames_array"]] == list(
        V12_GUIDE_FRAME_INDICES
    )
    assert [row["strength_float"] for row in sequence["frames_array"]] == list(
        V12_GUIDE_STRENGTHS
    )
    by_index = {
        row["frame_index_int"]: row["sha256_string"]
        for row in sequence["frames_array"]
    }
    for frame_index in (0, *V12_RECOVERY_GUIDE_FRAME_INDICES, 48):
        assert by_index[frame_index] == V12_ENDPOINT_GUIDE_SHA256
    recovery = spec["recovery_guide_contract_object"]
    assert recovery["author_cli_sha256_string"] == V12_GUIDE_CLI_SHA256
    assert recovery["recovery_guides_byte_identical_endpoint_bool"] is True
    assert recovery["deterministic_contact_cues_bool"] is True
    positive = spec["positive_prompt_string"]
    negative = spec["negative_prompt_string"]
    for exact_window in (
        "hind-left only during frames 1-11",
        "fore-left only during frames 13-23",
        "hind-right only during frames 25-35",
        "fore-right only during frames 37-47",
    ):
        assert exact_window in positive
    assert "Frames 0, 12, 24, 36 and 48 are hard four-hoof recovery barriers" in positive
    assert "visible hoof contact cue means that exact hoof is planted and motionless" in positive
    assert "opaque connected fixed-length anatomical chain" in positive
    for artifact in ("ghost", "detached", "wavy", "length snap"):
        assert artifact in positive
        assert artifact in negative
    assert spec["generation_authorization_object"]["authorized_bool"] is False
    assert spec["approved_bool"] is False


@pytest.mark.parametrize("mutation", ["prompt", "seed", "extra_spec_field"])
def test_v12_same_id_spec_mutation_rejects_before_reference_or_bundle_access(
    tmp_path: Path, mutation: str
) -> None:
    payload = json.loads(v12_spec_path().read_text())
    if mutation == "prompt":
        payload["positive_prompt_string"] += " mutated"
    elif mutation == "seed":
        payload["seed_int"] += 1
    else:
        payload["arbitrary_mutation_bool"] = True
    assert payload["experiment_id_string"] == V12_EXPERIMENT_ID
    mutated = tmp_path / "mutated-v12.json"
    mutated.write_text(json.dumps(payload, indent=2) + "\n")

    with pytest.raises(ControlledExperimentError, match="exact code-owned checked-in pin"):
        load_controlled_plan(
            experiment_path=mutated,
            authorization=V12_EXPERIMENT_ID,
            reference_bundle=tmp_path / "missing-reference",
            guide_bundle=tmp_path / "missing-guides",
            artifact_root=tmp_path / "artifacts",
        )


def test_v12_real_f2_bundle_loads_with_exact_code_owned_pins_when_available() -> None:
    reference = Path(r"R:\ComfyUI-data\autorig-fitting\horse-canonical-f1")
    guides = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        r"\horse-walk-v12-browser-recovery-guides-f2"
    )
    artifacts = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
    )
    missing = [
        path
        for path in (v12_spec_path(), reference, guides, artifacts)
        if not path.exists()
    ]
    if missing:
        pytest.skip(f"external real v12 f2 fixtures are absent: {missing}")

    plan = load_controlled_plan(
        experiment_path=v12_spec_path(),
        authorization=V12_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    assert plan.guide_manifest_sha256 == V12_GUIDE_MANIFEST_SHA256
    assert [frame.frame_index for frame in plan.guide_frames] == list(
        V12_GUIDE_FRAME_INDICES
    )
    assert [frame.strength for frame in plan.guide_frames] == list(
        V12_GUIDE_STRENGTHS
    )
    assert len({frame.sha256 for frame in plan.guide_frames}) == 5
    by_index = {frame.frame_index: frame for frame in plan.guide_frames}
    for frame_index in (0, *V12_RECOVERY_GUIDE_FRAME_INDICES, 48):
        assert by_index[frame_index].sha256 == V12_ENDPOINT_GUIDE_SHA256
    assert plan.reference_sha256 != V12_ENDPOINT_GUIDE_SHA256


@pytest.mark.parametrize("pin_field", ["manifest", "endpoint"])
def test_v12_rejects_arbitrary_experiment_repin_before_reading_bundle(
    tmp_path: Path, pin_field: str
) -> None:
    contract = json.loads(v12_spec_path().read_text())
    sequence = contract["guide_sequence_object"]
    if pin_field == "manifest":
        sequence["immutable_manifest_sha256_string"] = "a" * 64
        expected = "immutable guide manifest pin"
    else:
        sequence["endpoint_guide_sha256_string"] = "b" * 64
        expected = "immutable endpoint guide pin"
    guide_bundle = tmp_path / V12_GUIDE_BUNDLE_ID
    guide_bundle.mkdir()

    with pytest.raises(ControlledExperimentError, match=expected):
        _load_browser_recovery_guide_sequence(
            contract,
            guide_bundle=guide_bundle,
            reference_sha256="9" * 64,
            frame_count=49,
            start_strength=0.8,
            end_strength=0.8,
            pins=V12_GUIDE_PINS,
        )


def test_v12_http_free_graph_has_exact_nine_ordered_guides_and_shared_recovery() -> None:
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
    endpoint = "autorig_animation_fitting/autorig_endpoint.png"
    uploads = {
        0: endpoint,
        6: "autorig_animation_fitting/autorig_swing_006.png",
        12: endpoint,
        18: "autorig_animation_fitting/autorig_swing_018.png",
        24: endpoint,
        30: "autorig_animation_fitting/autorig_swing_030.png",
        36: endpoint,
        42: "autorig_animation_fitting/autorig_swing_042.png",
        48: endpoint,
    }
    strengths = dict(zip(V12_GUIDE_FRAME_INDICES, V12_GUIDE_STRENGTHS))
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
    guide_nodes = [
        node
        for node in result.values()
        if isinstance(node, dict) and node.get("class_type") == "LTXVAddGuide"
    ]
    assert len(guide_nodes) == 9
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
        V12_GUIDE_STRENGTHS
    )
    for previous_id, node_id in zip(ordered_ids, ordered_ids[1:]):
        assert result[node_id]["inputs"]["positive"] == [previous_id, 0]
        assert result[node_id]["inputs"]["negative"] == [previous_id, 1]
        assert result[node_id]["inputs"]["latent"] == [previous_id, 2]
    for frame_index in V12_RECOVERY_GUIDE_FRAME_INDICES:
        recovery = result[f"94{frame_index:03d}"]
        assert recovery["inputs"]["image"] == start["inputs"]["image"]
        assert f"91{frame_index:03d}" not in result
        assert f"92{frame_index:03d}" not in result
        assert f"93{frame_index:03d}" not in result
    assert end["inputs"]["image"] == start["inputs"]["image"]
    assert result["2004"]["inputs"]["image"] == endpoint
    assert original["2004"]["inputs"]["image"] == "ltx_i2v_reference.png"

    invalid_uploads = dict(uploads)
    invalid_uploads[24] = "autorig_animation_fitting/different_recovery.png"
    with pytest.raises(
        ControlledExperimentError,
        match="recovery guide frame 24 must use the uploaded endpoint image",
    ):
        patch_browser_keyframe_guides(
            native,
            uploaded_images=invalid_uploads,
            strengths=strengths,
        )

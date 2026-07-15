from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from animation_fitting.comfy import ComfyOutputFile, ComfySubmission, ComfyWorker
from animation_fitting.controlled_experiment import (
    EXPECTED_EXPERIMENT_ID,
    V5_EXPERIMENT_ID,
    V6_EXPERIMENT_ID,
    V7_EXPERIMENT_ID,
    V8_EXPERIMENT_ID,
    ControlledExperimentError,
    load_controlled_plan,
    patch_guide_strengths,
    patch_native_resolution,
    run_controlled_experiment,
)
from animation_fitting.specs import load_animation_fitting_specs


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_json(path: Path, value: object) -> bytes:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(data)
    return data


def build_contract(
    tmp_path: Path,
    *,
    experiment_id: str = EXPECTED_EXPERIMENT_ID,
    seed: int = 4373011867009528156,
    guide_strength: float = 0.8,
) -> tuple[Path, Path, Path]:
    bundle = tmp_path / "horse-canonical-f1-semantic-limbs-v2"
    bundle.mkdir()
    image_data = b"deterministic semantic PNG fixture bytes"
    image = bundle / "reference_ltx_semantic.png"
    image.write_bytes(image_data)
    derivation_data = write_json(bundle / "semantic_reference.json", {
        "schema": "autorig-ltx-semantic-reference-derivation.v1",
        "semantic_profile": {
            "profile_id": "horse_2.semantic_limbs.v1",
            "sha256": "1" * 64,
        },
    })
    manifest = {
        "schema": "autorig-ltx-semantic-reference-output.v1",
        "file_count": 2,
        "files": [
            {
                "filename": image.name,
                "sha256": sha(image_data),
                "bytes": len(image_data),
            },
            {
                "filename": "semantic_reference.json",
                "sha256": sha(derivation_data),
                "bytes": len(derivation_data),
            },
        ],
    }
    manifest_data = write_json(bundle / "immutable_manifest.json", manifest)
    profile = load_animation_fitting_specs().workflows["loop"]
    experiment = {
        "schema": "autorig.animation-fitting-experiment.v1",
        "experiment_id_string": experiment_id,
        "generation_mode_string": "loop",
        "frame_count_int": 49,
        "input_fps_int": profile.input_fps,
        "output_fps_int": profile.output_fps,
        "seed_int": seed,
        "positive_prompt_string": "exact semantic Horse Walk positive prompt",
        "negative_prompt_string": "exact semantic Horse Walk negative prompt",
        "reference_object": {
            "bundle_id_string": bundle.name,
            "reference_png_filename_string": image.name,
            "reference_png_sha256_string": sha(image_data),
            "derivation_manifest_filename_string": "semantic_reference.json",
            "derivation_manifest_sha256_string": sha(derivation_data),
            "immutable_manifest_filename_string": "immutable_manifest.json",
            "immutable_manifest_sha256_string": sha(manifest_data),
            "semantic_profile_id_string": "horse_2.semantic_limbs.v1",
            "semantic_profile_sha256_string": "1" * 64,
        },
        "workflow_object": {
            "workflow_name_string": profile.workflow_name,
            "workflow_fingerprint_sha256_string": profile.workflow_fingerprint,
        },
        "variants_array": [{
            "variant_id_string": "semantic_reference_guide_strength_0_80",
            "start_guide_strength_float": guide_strength,
            "end_guide_strength_float": guide_strength,
        }],
        "generation_authorization_object": {"authorized_bool": False},
        "approved_bool": False,
    }
    experiment_path = tmp_path / "experiment.json"
    write_json(experiment_path, experiment)
    return experiment_path, bundle, tmp_path / "artifacts"


def build_actionless_contract(tmp_path: Path) -> tuple[Path, Path, Path]:
    bundle = tmp_path / "horse-canonical-f1"
    bundle.mkdir()
    image_data = b"deterministic actionless RGB PNG fixture bytes"
    image = bundle / "reference_rgb.png"
    image.write_bytes(image_data)
    fitting_bundle_data = write_json(bundle / "fitting_bundle.json", {
        "schema": "autorig-actionless-fitting-bundle.v1",
        "actionless": {"actionless": True},
        "camera": {"resolution": [768, 448]},
        "artifacts": {
            "rgb": {
                "filename": image.name,
                "sha256": sha(image_data),
                "bytes": len(image_data),
            },
        },
    })
    immutable_data = write_json(bundle / "immutable_manifest.json", {
        "schema": "autorig-fitting-immutable-copy.v1",
        "bundle_file_count": 2,
        "files": [
            {"filename": image.name, "sha256": sha(image_data), "bytes": len(image_data)},
            {
                "filename": "fitting_bundle.json",
                "sha256": sha(fitting_bundle_data),
                "bytes": len(fitting_bundle_data),
            },
        ],
    })
    profile = load_animation_fitting_specs().workflows["loop"]
    experiment = {
        "schema": "autorig.animation-fitting-experiment.v1",
        "experiment_id_string": V8_EXPERIMENT_ID,
        "generation_mode_string": "loop",
        "frame_count_int": 49,
        "input_fps_int": profile.input_fps,
        "output_fps_int": profile.output_fps,
        "seed_int": 4373011867009528156,
        "positive_prompt_string": "exact RGB Horse Walk positive prompt",
        "negative_prompt_string": "exact RGB Horse Walk negative prompt",
        "reference_object": {
            "reference_contract_string": "actionless_bundle_rgb_v1",
            "bundle_id_string": bundle.name,
            "reference_png_filename_string": image.name,
            "reference_png_sha256_string": sha(image_data),
            "bundle_manifest_filename_string": "fitting_bundle.json",
            "bundle_manifest_sha256_string": sha(fitting_bundle_data),
            "immutable_manifest_filename_string": "immutable_manifest.json",
            "immutable_manifest_sha256_string": sha(immutable_data),
        },
        "workflow_object": {
            "workflow_name_string": profile.workflow_name,
            "workflow_fingerprint_sha256_string": profile.workflow_fingerprint,
        },
        "resolution_override_object": {
            "latent_width_int": 768,
            "latent_height_int": 448,
            "resize_longer_int": 768,
        },
        "variants_array": [{
            "variant_id_string": "rgb_native_768x448_guide_strength_0_80",
            "start_guide_strength_float": 0.8,
            "end_guide_strength_float": 0.8,
        }],
        "generation_authorization_object": {"authorized_bool": False},
        "approved_bool": False,
    }
    experiment_path = tmp_path / "experiment-v8.json"
    write_json(experiment_path, experiment)
    return experiment_path, bundle, tmp_path / "artifacts-v8"


def test_v5_contract_is_exactly_allowlisted_and_arbitrary_ids_fail_closed(tmp_path: Path) -> None:
    experiment, bundle, artifacts = build_contract(
        tmp_path,
        experiment_id=V5_EXPERIMENT_ID,
        seed=3794990487858656905,
        guide_strength=0.65,
    )
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V5_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    assert plan.experiment_id == V5_EXPERIMENT_ID
    assert plan.seed == 3794990487858656905
    assert plan.start_guide_strength == plan.end_guide_strength == 0.65

    payload = json.loads(experiment.read_text())
    payload["experiment_id_string"] = f"{V5_EXPERIMENT_ID}_arbitrary"
    write_json(experiment, payload)
    with pytest.raises(ControlledExperimentError, match="controlled runner does not allow"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=payload["experiment_id_string"],
            reference_bundle=bundle,
            artifact_root=artifacts,
        )


def test_v5_immutable_spec_records_predecessor_and_optional_audio_cue() -> None:
    spec_path = Path(__file__).resolve().parents[1] / "animation_fitting" / "specs" / "experiments" / (
        "horse_walk_prompt_v5_semantic_chronological_av_"
        "seed_3794990487858656905_guide_065.v1.json"
    )
    spec = json.loads(spec_path.read_text())
    assert spec["experiment_id_string"] == V5_EXPERIMENT_ID
    assert spec["seed_int"] == 3794990487858656905
    assert spec["variants_array"] == [{
        "variant_id_string": "semantic_chronological_av_guide_strength_0_65",
        "start_guide_strength_float": 0.65,
        "end_guide_strength_float": 0.65,
    }]
    assert spec["predecessor_experiment_object"]["sha256_string"] == (
        "f3d523f4fa181b261c229ed339a5e7ec6df6f66cbc5099f3b24afa347fda5458"
    )
    assert spec["predecessor_experiment_object"]["result_video_sha256_string"] == (
        "c534cdc5008b99a8149069202a3e5cd28f619e0eebab7dfb7a45ea3a1d5d27bb"
    )
    assert spec["predecessor_experiment_object"]["result_semantic_qa_sha256_string"] == (
        "60e6852017288e16dedbe2261d5d04bd54e6cb4b37e87d484c2747e79d4f3d2b"
    )
    assert spec["audio_temporal_cue_object"] == {
        "role_string": "optional_temporal_motion_cue",
        "required_for_visual_acceptance_bool": False,
        "cue_string": "four soft isolated hoof-clop sounds, one at each landing",
        "music_allowed_bool": False,
        "voice_allowed_bool": False,
    }


def test_v6_contract_is_exactly_allowlisted_at_guide_075(tmp_path: Path) -> None:
    experiment, bundle, artifacts = build_contract(
        tmp_path,
        experiment_id=V6_EXPERIMENT_ID,
        seed=3794990487858656905,
        guide_strength=0.75,
    )
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V6_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    assert plan.experiment_id == V6_EXPERIMENT_ID
    assert plan.seed == 3794990487858656905
    assert plan.start_guide_strength == plan.end_guide_strength == 0.75


def test_v6_immutable_spec_changes_only_guide_conditioning_from_v5() -> None:
    specs = Path(__file__).resolve().parents[1] / "animation_fitting" / "specs" / "experiments"
    v5 = json.loads((specs / (
        "horse_walk_prompt_v5_semantic_chronological_av_"
        "seed_3794990487858656905_guide_065.v1.json"
    )).read_text())
    v6 = json.loads((specs / (
        "horse_walk_prompt_v6_semantic_chronological_av_"
        "seed_3794990487858656905_guide_075.v1.json"
    )).read_text())

    for key in (
        "base_action_id_string",
        "species_string",
        "generation_mode_string",
        "frame_count_int",
        "input_fps_int",
        "output_fps_int",
        "seed_int",
        "seed_derivation_object",
        "reference_object",
        "workflow_object",
        "positive_prompt_string",
        "negative_prompt_string",
        "audio_temporal_cue_object",
        "manual_acceptance_gates_array",
    ):
        assert v6[key] == v5[key], key

    assert v6["experiment_id_string"] == V6_EXPERIMENT_ID
    assert v6["variants_array"] == [{
        "variant_id_string": "semantic_chronological_av_guide_strength_0_75",
        "start_guide_strength_float": 0.75,
        "end_guide_strength_float": 0.75,
    }]
    predecessor = v6["predecessor_experiment_object"]
    assert predecessor["sha256_string"] == (
        "55ab677a1c73b47bae30e7f923a46f5f9ee168e6b168f6e20fb40312485a9aca"
    )
    assert predecessor["result_video_sha256_string"] == (
        "883dd271de1af8b0968168085e25117d1e5b8c15801bf2b6a78500634bfddf04"
    )
    assert predecessor["result_semantic_qa_sha256_string"] == (
        "d37c23ebf68950a912ac6a541db030a167f0636ff42fde66331793760a0618e8"
    )
    assert predecessor["result_phase_contact_sheet_sha256_string"] == (
        "05cbb6e9fef4cc5b3e08082769837b33e356ad7142cfb728d4dd2d91da3e6510"
    )


def test_v7_contract_is_exactly_allowlisted_at_second_seed(tmp_path: Path) -> None:
    experiment, bundle, artifacts = build_contract(
        tmp_path,
        experiment_id=V7_EXPERIMENT_ID,
        seed=4891025524393280044,
        guide_strength=0.65,
    )
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V7_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    assert plan.experiment_id == V7_EXPERIMENT_ID
    assert plan.seed == 4891025524393280044
    assert plan.start_guide_strength == plan.end_guide_strength == 0.65


def test_v7_immutable_spec_changes_only_seed_from_v5_matrix_cell() -> None:
    specs = Path(__file__).resolve().parents[1] / "animation_fitting" / "specs" / "experiments"
    v5 = json.loads((specs / (
        "horse_walk_prompt_v5_semantic_chronological_av_"
        "seed_3794990487858656905_guide_065.v1.json"
    )).read_text())
    v7 = json.loads((specs / (
        "horse_walk_prompt_v7_semantic_chronological_av_"
        "seed_4891025524393280044_guide_065.v1.json"
    )).read_text())

    for key in (
        "base_action_id_string",
        "species_string",
        "generation_mode_string",
        "frame_count_int",
        "input_fps_int",
        "output_fps_int",
        "reference_object",
        "workflow_object",
        "positive_prompt_string",
        "negative_prompt_string",
        "audio_temporal_cue_object",
        "manual_acceptance_gates_array",
    ):
        assert v7[key] == v5[key], key

    assert v7["experiment_id_string"] == V7_EXPERIMENT_ID
    assert v7["seed_int"] == 4891025524393280044
    assert v7["seed_derivation_object"] == {
        "algorithm_string": "sha256-first-63-bits-big-endian-v1",
        "label_string": "autorig:horse_walk:semantic:v7-v8:seed-b",
        "label_sha256_string": "43e069ccf8d4822c101eaef4ab1a59828071a40264b39fc6b0488da5de8ab482",
    }
    assert v7["variants_array"] == [{
        "variant_id_string": "semantic_chronological_av_second_seed_guide_strength_0_65",
        "start_guide_strength_float": 0.65,
        "end_guide_strength_float": 0.65,
    }]
    predecessor = v7["predecessor_experiment_object"]
    assert predecessor["sha256_string"] == (
        "2598893c62c785e8b22aaf7ee5f86c88f3025c02da549767da33a17ca987503f"
    )
    assert predecessor["result_video_sha256_string"] == (
        "154ab78fea303a55290b94ca9fc3e083f6da1f260116046603f0f30b2cb0c941"
    )
    assert predecessor["result_semantic_qa_sha256_string"] == (
        "1dc6db69af5d857f9e2004405ed54edae0689fdc2ec445c3fe19d975799d7b72"
    )
    assert predecessor["result_phase_contact_sheet_sha256_string"] == (
        "59749c44712ba04f6e90b04ddc5a1e6d81497ffc79f02148abb3ac301e4e83f8"
    )
    assert predecessor["result_identity_visual_gate_object"] == {
        "accepted_bool": False,
        "duplicate_subject_observed_bool": True,
        "reference_subject_animated_as_requested_bool": False,
        "observed_string": (
            "second_solid_yellow_orange_horse_walked_across_"
            "mostly_static_gray_reference_horse"
        ),
    }


def test_v8_actionless_rgb_contract_is_exactly_allowlisted_at_native_resolution(
    tmp_path: Path,
) -> None:
    experiment, bundle, artifacts = build_actionless_contract(tmp_path)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V8_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    assert plan.experiment_id == V8_EXPERIMENT_ID
    assert plan.reference_image.name == "reference_rgb.png"
    assert plan.seed == 4373011867009528156
    assert (plan.latent_width, plan.latent_height, plan.resize_longer) == (768, 448, 768)


def test_v8_immutable_spec_records_resolution_only_rgb_followup() -> None:
    spec_path = Path(__file__).resolve().parents[1] / "animation_fitting" / "specs" / "experiments" / (
        "horse_walk_prompt_v8_rgb_native_768x448_"
        "seed_4373011867009528156_guide_080.v1.json"
    )
    spec = json.loads(spec_path.read_text())
    assert spec["experiment_id_string"] == V8_EXPERIMENT_ID
    assert spec["reference_object"]["reference_contract_string"] == "actionless_bundle_rgb_v1"
    assert spec["reference_object"]["reference_png_sha256_string"] == (
        "94bf47cc137c0aaee975b2a75b7cd2b28f75215e282cdb6865bdd4095630a0b1"
    )
    assert spec["resolution_override_object"] == {
        "source_reference_width_int": 768,
        "source_reference_height_int": 448,
        "latent_width_int": 768,
        "latent_height_int": 448,
        "resize_longer_int": 768,
        "base_latent_width_int": 512,
        "base_latent_height_int": 320,
        "base_resize_longer_int": 512,
    }
    assert spec["seed_int"] == 4373011867009528156
    assert spec["variants_array"] == [{
        "variant_id_string": "rgb_native_768x448_guide_strength_0_80",
        "start_guide_strength_float": 0.8,
        "end_guide_strength_float": 0.8,
    }]


def test_controlled_plan_requires_exact_runtime_authorization_and_hashes(tmp_path: Path) -> None:
    experiment, bundle, artifacts = build_contract(tmp_path)
    with pytest.raises(ControlledExperimentError, match="explicit --authorize-experiment"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization="wrong",
            reference_bundle=bundle,
            artifact_root=artifacts,
        )
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=EXPECTED_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    assert plan.reference_sha256 == sha((bundle / "reference_ltx_semantic.png").read_bytes())
    assert plan.start_guide_strength == plan.end_guide_strength == 0.8
    assert plan.frame_count == 49

    (bundle / "reference_ltx_semantic.png").write_bytes(b"mutated")
    with pytest.raises(ControlledExperimentError, match="SHA-256 mismatch"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=EXPECTED_EXPERIMENT_ID,
            reference_bundle=bundle,
            artifact_root=artifacts,
        )


def test_patch_guide_strengths_changes_only_exact_start_and_end_nodes() -> None:
    prompt = {
        "start": {"class_type": "LTXVAddGuide", "inputs": {"frame_idx": 0, "strength": 0.2}},
        "end": {"class_type": "LTXVAddGuide", "inputs": {"frame_idx": -1, "strength": 0.3}},
        "other": {"class_type": "Other", "inputs": {"strength": 0.4}},
    }
    result = patch_guide_strengths(prompt, start_strength=0.8, end_strength=0.8)
    assert result["start"]["inputs"]["strength"] == 0.8
    assert result["end"]["inputs"]["strength"] == 0.8
    assert result["other"]["inputs"]["strength"] == 0.4


def test_patch_native_resolution_changes_only_latent_and_input_resize() -> None:
    prompt = {
        "latent": {
            "class_type": "EmptyLTXVLatentVideo",
            "inputs": {"width": 512, "height": 320, "length": 49},
        },
        "resize": {
            "class_type": "ResizeImageMaskNode",
            "inputs": {
                "resize_type": "scale longer dimension",
                "resize_type.longer_size": 512,
                "scale_method": "lanczos",
            },
        },
        "other": {"class_type": "Other", "inputs": {"width": 512}},
    }
    result = patch_native_resolution(prompt, width=768, height=448, resize_longer=768)
    assert result["latent"]["inputs"] == {"width": 768, "height": 448, "length": 49}
    assert result["resize"]["inputs"]["resize_type.longer_size"] == 768
    assert result["resize"]["inputs"]["scale_method"] == "lanczos"
    assert result["other"] == prompt["other"]
    assert prompt["latent"]["inputs"]["width"] == 512


class FakeClient:
    instances: list["FakeClient"] = []

    def __init__(self, worker: ComfyWorker) -> None:
        self.worker = worker
        self.submitted = None
        self.closed = False
        self.__class__.instances.append(self)

    async def fetch_api_workflow(self):
        workflow_path = Path(__file__).resolve().parents[1] / "animation_fitting" / "specs" / "workflows" / self.worker.workflow_name
        return json.loads(workflow_path.read_text()), self.worker.expected_workflow_fingerprint

    async def queue_load(self) -> int:
        return 0

    async def upload_reference_image(self, _path: Path) -> str:
        return "autorig_animation_fitting/reference.png"

    async def submit(self, prompt, _idempotency_key: str) -> ComfySubmission:
        guides = {
            node["inputs"]["frame_idx"]: node["inputs"]["strength"]
            for node in prompt.values()
            if node.get("class_type") == "LTXVAddGuide"
        }
        assert guides == {0: 0.8, -1: 0.8}
        self.submitted = prompt
        return ComfySubmission(prompt_id="prompt-controlled", client_id="client", resumed_existing_bool=False)

    async def wait_for_output(self, _prompt_id: str):
        return {}, ComfyOutputFile("horse_walk_semantic.mp4")

    async def download_output(self, _output: ComfyOutputFile) -> bytes:
        return b"synthetic controlled MP4 payload long enough for immutable storage"

    async def close(self) -> None:
        self.closed = True


class FakeExtractor:
    def extract_and_store(self, raw_video, store, *, expected_frame_count: int):
        return tuple(
            store.store_frame(raw_video.sha256, index, f"frame-{index:03d}".encode())
            for index in range(expected_frame_count)
        )


class ActiveSamePromptClient(FakeClient):
    async def queue_load(self) -> int:
        return 1

    async def prompt_exists(self, _prompt_id: str) -> bool:
        return True


def test_controlled_run_is_fail_closed_unapproved_and_idempotently_resumable(tmp_path: Path) -> None:
    experiment, bundle, artifacts = build_contract(tmp_path)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=EXPECTED_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    FakeClient.instances.clear()
    result = asyncio.run(run_controlled_experiment(
        plan,
        worker=worker,
        client_factory=FakeClient,
        frame_extractor=FakeExtractor(),
    ))
    assert len(result.frames) == 49
    assert result.raw_video.size_bytes > 32
    assert result.resumed_existing_result is False
    assert FakeClient.instances[-1].closed is True
    assert result.to_dict()["approval_state_string"] == "generated_not_approved"
    assert result.to_dict()["send_to_skeletal_fitting_bool"] is False

    def forbidden_client(_worker):
        raise AssertionError("completed immutable result must resume without Comfy submission")

    resumed = asyncio.run(run_controlled_experiment(
        plan,
        worker=worker,
        client_factory=forbidden_client,
        frame_extractor=FakeExtractor(),
    ))
    assert resumed.job_id == result.job_id
    assert resumed.resumed_existing_result is True


def test_controlled_run_resumes_its_own_active_prompt_without_duplicate_submission(
    tmp_path: Path,
) -> None:
    experiment, bundle, artifacts = build_contract(tmp_path)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=EXPECTED_EXPERIMENT_ID,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    ActiveSamePromptClient.instances.clear()
    result = asyncio.run(run_controlled_experiment(
        plan,
        worker=worker,
        client_factory=ActiveSamePromptClient,
        frame_extractor=FakeExtractor(),
    ))
    assert result.prompt_id == "prompt-controlled"
    assert len(result.frames) == 49
    assert len(ActiveSamePromptClient.instances) == 1

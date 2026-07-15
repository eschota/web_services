from __future__ import annotations

import asyncio
import hashlib
import json
import struct
import zlib
from pathlib import Path

import pytest

import animation_fitting.controlled_experiment as controlled_experiment_module
from animation_fitting.comfy import ComfyOutputFile, ComfySubmission, ComfyWorker
from animation_fitting.controlled_experiment import (
    EXPECTED_EXPERIMENT_ID,
    V5_EXPERIMENT_ID,
    V6_EXPERIMENT_ID,
    V7_EXPERIMENT_ID,
    V8_EXPERIMENT_ID,
    V9_EXPERIMENT_IDS,
    V10_EXPERIMENT_ID,
    V10_GUIDE_FRAME_INDICES,
    V11_ENDPOINT_GUIDE_SHA256,
    V11_EXPERIMENT_ID,
    V11_GUIDE_BUNDLE_ID,
    V11_GUIDE_MANIFEST_SHA256,
    V11_STATIC_SCENE_QA_SUMMARY,
    V11_STATIC_SCENE_RENDERER_SETTINGS,
    ControlledExperimentError,
    load_controlled_plan,
    patch_browser_keyframe_guides,
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


def png_fixture(
    *, width: int = 768, height: int = 448, rgb: tuple[int, int, int] = (32, 64, 96)
) -> bytes:
    def chunk(chunk_type: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(chunk_type)
        crc = zlib.crc32(data, crc) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)

    scanline = b"\x00" + bytes(rgb) * width
    raw = scanline * height
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, level=9))
        + chunk(b"IEND", b"")
    )


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
    image_data = png_fixture(rgb=(42, 65, 89))
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


def build_v10_contract(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    experiment_path, reference_bundle, artifacts = build_actionless_contract(tmp_path)
    guide_bundle = tmp_path / "horse-walk-v10-browser-guides-f1"
    guide_bundle.mkdir()
    reference_data = (reference_bundle / "reference_rgb.png").read_bytes()
    frame_payloads = {
        0: reference_data,
        6: png_fixture(rgb=(120, 40, 40)),
        18: png_fixture(rgb=(40, 120, 40)),
        30: png_fixture(rgb=(40, 40, 120)),
        42: png_fixture(rgb=(120, 100, 40)),
        48: reference_data,
    }
    strengths = {0: 0.8, 6: 0.7, 18: 0.7, 30: 0.7, 42: 0.7, 48: 0.8}
    rows = []
    for frame_index in V10_GUIDE_FRAME_INDICES:
        filename = f"frame_{frame_index:03d}.png"
        data = frame_payloads[frame_index]
        (guide_bundle / filename).write_bytes(data)
        rows.append({
            "frame_index_int": frame_index,
            "filename_string": filename,
            "sha256_string": sha(data),
            "bytes_int": len(data),
        })
    reference_sha = sha(reference_data)
    manifest_data = write_json(guide_bundle / "immutable_manifest.json", {
        "schema": "autorig-browser-ltx-guide-bundle.v1",
        "source_reference_sha256_string": reference_sha,
        "cycle_frame_count_int": 49,
        "guide_count_int": 6,
        "renderer_object": {
            "renderer_string": "browser_threejs",
            "blender_used_bool": False,
        },
        "frames_array": rows,
    })
    experiment = json.loads(experiment_path.read_text())
    experiment["experiment_id_string"] = V10_EXPERIMENT_ID
    experiment["seed_int"] = 6550110377254033429
    experiment["guide_sequence_object"] = {
        "ready_bool": True,
        "guide_contract_string": "browser_rendered_rgb_keyframes_v1",
        "bundle_id_string": guide_bundle.name,
        "immutable_manifest_filename_string": "immutable_manifest.json",
        "immutable_manifest_sha256_string": sha(manifest_data),
        "frames_array": [
            {
                **row,
                "strength_float": strengths[row["frame_index_int"]],
            }
            for row in rows
        ],
    }
    write_json(experiment_path, experiment)
    return experiment_path, reference_bundle, guide_bundle, artifacts


def repin_v10_frames(
    experiment_path: Path,
    guide_bundle: Path,
    replacements: dict[int, bytes],
) -> None:
    manifest_path = guide_bundle / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for row in manifest["frames_array"]:
        frame_index = row["frame_index_int"]
        if frame_index not in replacements:
            continue
        data = replacements[frame_index]
        (guide_bundle / row["filename_string"]).write_bytes(data)
        row["sha256_string"] = sha(data)
        row["bytes_int"] = len(data)
    manifest_data = write_json(manifest_path, manifest)

    contract = json.loads(experiment_path.read_text())
    contract["guide_sequence_object"]["immutable_manifest_sha256_string"] = sha(
        manifest_data
    )
    for row in contract["guide_sequence_object"]["frames_array"]:
        frame_index = row["frame_index_int"]
        if frame_index not in replacements:
            continue
        data = replacements[frame_index]
        row["sha256_string"] = sha(data)
        row["bytes_int"] = len(data)
    write_json(experiment_path, contract)


def build_v11_contract(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    experiment_path, reference_bundle, artifacts = build_actionless_contract(tmp_path)
    guide_bundle = tmp_path / V11_GUIDE_BUNDLE_ID
    guide_bundle.mkdir()
    reference_data = (reference_bundle / "reference_rgb.png").read_bytes()
    frame_payloads = {
        0: png_fixture(rgb=(80, 92, 104)),
        6: png_fixture(rgb=(120, 40, 40)),
        18: png_fixture(rgb=(40, 120, 40)),
        30: png_fixture(rgb=(40, 40, 120)),
        42: png_fixture(rgb=(120, 100, 40)),
        48: png_fixture(rgb=(80, 92, 104)),
    }
    strengths = {0: 0.8, 6: 0.7, 18: 0.7, 30: 0.7, 42: 0.7, 48: 0.8}
    rows = []
    for frame_index in V10_GUIDE_FRAME_INDICES:
        filename = f"guide_{frame_index:03d}.png"
        data = frame_payloads[frame_index]
        (guide_bundle / filename).write_bytes(data)
        rows.append({
            "frame_index_int": frame_index,
            "filename_string": filename,
            "sha256_string": sha(data),
            "bytes_int": len(data),
            "strength_float": strengths[frame_index],
        })

    full_frame_luma = [
        212.7256745383853,
        212.70397149948846,
        212.71779044469199,
        212.7480343409947,
        212.70751838457016,
        212.7256745383853,
    ]
    background_luma = 213.31762790679932
    manifest = {
        "schema": "autorig-browser-ltx-static-scene-guide-bundle.v1",
        "status": "PASS",
        "approvedForAnimationLibrary": False,
        "browserOnly": True,
        "blenderUsed": False,
        "resolution": [768, 448],
        "source_reference_sha256_string": sha(reference_data),
        "source_reference_is_guide_bool": False,
        "endpoint_guide_sha256_string": sha(frame_payloads[0]),
        "cycle_frame_count_int": 49,
        "guide_count_int": 6,
        "renderer_object": {
            "renderer_string": "browser_threejs",
            "blender_used_bool": False,
            "scene_contract_string": "v11_unified_browser_static_scene_v1",
            "all_guide_frames_browser_rendered_bool": True,
            "shadows_enabled_bool": False,
        },
        "frames_array": rows,
        "postBakeQa": {
            "status": "PASS",
            "hierarchyBakeVerified": True,
            "minimumStanceHooves": 3,
            "endpointMaximumErrorPx": 0,
        },
        "staticSceneQa": {
            **V11_STATIC_SCENE_QA_SUMMARY,
            "guides_array": [
                {
                    "frame_index_int": frame_index,
                    "full_frame_mean_luma_float": full_frame_luma[index],
                    "background_mean_luma_float": background_luma,
                    "near_black_pixel_fraction_float": 0,
                }
                for index, frame_index in enumerate(V10_GUIDE_FRAME_INDICES)
            ],
        },
        "staticSceneRenderer": json.loads(
            json.dumps(V11_STATIC_SCENE_RENDERER_SETTINGS)
        ),
    }
    manifest_data = write_json(guide_bundle / "immutable_manifest.json", manifest)

    experiment = json.loads(experiment_path.read_text())
    experiment["experiment_id_string"] = V11_EXPERIMENT_ID
    experiment["seed_int"] = 6550110377254033429
    experiment["guide_sequence_object"] = {
        "ready_bool": True,
        "guide_contract_string": "browser_rendered_static_scene_rgb_keyframes_v1",
        "bundle_id_string": guide_bundle.name,
        "immutable_manifest_filename_string": "immutable_manifest.json",
        "immutable_manifest_sha256_string": sha(manifest_data),
        "endpoint_guide_sha256_string": sha(frame_payloads[0]),
        "frames_array": rows,
    }
    experiment["static_scene_contract_object"] = {
        "qa_summary_object": json.loads(json.dumps(V11_STATIC_SCENE_QA_SUMMARY)),
        "renderer_settings_object": json.loads(
            json.dumps(V11_STATIC_SCENE_RENDERER_SETTINGS)
        ),
    }
    write_json(experiment_path, experiment)
    return experiment_path, reference_bundle, guide_bundle, artifacts


def repin_v11_manifest(experiment_path: Path, guide_bundle: Path) -> None:
    manifest_path = guide_bundle / "immutable_manifest.json"
    manifest_data = write_json(manifest_path, json.loads(manifest_path.read_text()))
    contract = json.loads(experiment_path.read_text())
    contract["guide_sequence_object"]["immutable_manifest_sha256_string"] = sha(
        manifest_data
    )
    write_json(experiment_path, contract)


def repin_v11_frames(
    experiment_path: Path,
    guide_bundle: Path,
    replacements: dict[int, bytes],
) -> None:
    manifest_path = guide_bundle / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for row in manifest["frames_array"]:
        frame_index = row["frame_index_int"]
        if frame_index not in replacements:
            continue
        data = replacements[frame_index]
        (guide_bundle / row["filename_string"]).write_bytes(data)
        row["sha256_string"] = sha(data)
        row["bytes_int"] = len(data)
    if 0 in replacements:
        manifest["endpoint_guide_sha256_string"] = sha(replacements[0])
    manifest_data = write_json(manifest_path, manifest)

    contract = json.loads(experiment_path.read_text())
    contract["guide_sequence_object"]["immutable_manifest_sha256_string"] = sha(
        manifest_data
    )
    if 0 in replacements:
        contract["guide_sequence_object"]["endpoint_guide_sha256_string"] = sha(
            replacements[0]
        )
    for row in contract["guide_sequence_object"]["frames_array"]:
        frame_index = row["frame_index_int"]
        if frame_index not in replacements:
            continue
        data = replacements[frame_index]
        row["sha256_string"] = sha(data)
        row["bytes_int"] = len(data)
    write_json(experiment_path, contract)


def pin_synthetic_v11_constants(
    monkeypatch: pytest.MonkeyPatch, experiment_path: Path
) -> None:
    contract = json.loads(experiment_path.read_text())
    sequence = contract["guide_sequence_object"]
    monkeypatch.setattr(
        controlled_experiment_module,
        "V11_GUIDE_MANIFEST_SHA256",
        sequence["immutable_manifest_sha256_string"],
    )
    monkeypatch.setattr(
        controlled_experiment_module,
        "V11_ENDPOINT_GUIDE_SHA256",
        sequence["endpoint_guide_sha256_string"],
    )


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


@pytest.mark.parametrize(
    ("seed", "filename"),
    [
        (
            6550110377254033429,
            "horse_walk_prompt_v9_rgb_four_beat_seed_6550110377254033429_guide_080.v1.json",
        ),
        (
            1448959135068762145,
            "horse_walk_prompt_v9_rgb_four_beat_seed_1448959135068762145_guide_080.v1.json",
        ),
        (
            6552386848790876755,
            "horse_walk_prompt_v9_rgb_four_beat_seed_6552386848790876755_guide_080.v1.json",
        ),
    ],
)
def test_v9_seed_series_is_exactly_allowlisted_and_keeps_one_variable(
    tmp_path: Path,
    seed: int,
    filename: str,
) -> None:
    spec_path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / filename
    )
    spec = json.loads(spec_path.read_text())
    experiment_id = spec["experiment_id_string"]
    assert experiment_id in V9_EXPERIMENT_IDS
    assert spec["seed_int"] == seed
    assert spec["reference_object"]["reference_contract_string"] == "actionless_bundle_rgb_v1"
    assert spec["resolution_override_object"]["latent_width_int"] == 768
    assert spec["resolution_override_object"]["latent_height_int"] == 448
    assert "near hind hoof alone" in spec["positive_prompt_string"]
    assert "far fore hoof alone" in spec["positive_prompt_string"]
    assert spec["variants_array"][0]["start_guide_strength_float"] == 0.8
    assert spec["variants_array"][0]["end_guide_strength_float"] == 0.8

    experiment, bundle, artifacts = build_actionless_contract(tmp_path)
    payload = json.loads(experiment.read_text())
    payload["experiment_id_string"] = experiment_id
    payload["seed_int"] = seed
    write_json(experiment, payload)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=experiment_id,
        reference_bundle=bundle,
        artifact_root=artifacts,
    )
    assert plan.experiment_id == experiment_id
    assert plan.seed == seed
    assert (plan.latent_width, plan.latent_height, plan.resize_longer) == (768, 448, 768)


def test_v10_checked_in_spec_pins_browser_bundle_and_stays_unapproved() -> None:
    spec_path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v10_browser_rgb_swing_guides_seed_6550110377254033429.v1.json"
    )
    spec = json.loads(spec_path.read_text())
    assert spec["experiment_id_string"] == V10_EXPERIMENT_ID
    assert spec["status_string"] == "prepared_for_single_controlled_generation"
    guide = spec["guide_sequence_object"]
    assert guide["ready_bool"] is True
    assert guide["bundle_id_string"] == "horse-walk-v10-browser-guides-f1"
    assert guide["immutable_manifest_sha256_string"] == (
        "9b549fb634409a53ce8ae4f7ed7e8c7754b9bda3430bd22942b5aae433b2fdb2"
    )
    assert [row["frame_index_int"] for row in guide["frames_array"]] == [
        0,
        6,
        18,
        30,
        42,
        48,
    ]
    assert [row["strength_float"] for row in guide["frames_array"]] == [
        0.8,
        0.7,
        0.7,
        0.7,
        0.7,
        0.8,
    ]
    assert guide["frames_array"][0]["sha256_string"] == guide["frames_array"][-1][
        "sha256_string"
    ]
    assert spec["generation_authorization_object"]["authorized_bool"] is False
    assert spec["approved_bool"] is False
    assert "IC-LoRA" not in json.dumps(spec)


def test_v11_checked_in_spec_pins_unified_static_scene_and_stays_unapproved() -> None:
    spec_path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v11_browser_static_scene_guides_seed_6550110377254033429.v1.json"
    )
    spec = json.loads(spec_path.read_text())
    assert spec["experiment_id_string"] == V11_EXPERIMENT_ID
    assert spec["status_string"] == "prepared_for_single_controlled_generation"
    guide = spec["guide_sequence_object"]
    assert guide["ready_bool"] is True
    assert guide["guide_contract_string"] == (
        "browser_rendered_static_scene_rgb_keyframes_v1"
    )
    assert guide["bundle_id_string"] == V11_GUIDE_BUNDLE_ID
    assert guide["immutable_manifest_sha256_string"] == V11_GUIDE_MANIFEST_SHA256
    assert guide["endpoint_guide_sha256_string"] == V11_ENDPOINT_GUIDE_SHA256
    assert [row["frame_index_int"] for row in guide["frames_array"]] == list(
        V10_GUIDE_FRAME_INDICES
    )
    assert [row["strength_float"] for row in guide["frames_array"]] == [
        0.8,
        0.7,
        0.7,
        0.7,
        0.7,
        0.8,
    ]
    endpoint_sha = guide["frames_array"][0]["sha256_string"]
    assert endpoint_sha == guide["frames_array"][-1]["sha256_string"]
    assert endpoint_sha == V11_ENDPOINT_GUIDE_SHA256
    assert endpoint_sha != spec["reference_object"]["reference_png_sha256_string"]
    static_scene = spec["static_scene_contract_object"]
    assert static_scene["qa_summary_object"] == V11_STATIC_SCENE_QA_SUMMARY
    assert static_scene["renderer_settings_object"] == (
        V11_STATIC_SCENE_RENDERER_SETTINGS
    )
    prompt = spec["positive_prompt_string"]
    for recovery_frame in (12, 24, 36, 48):
        assert f"recovery frame {recovery_frame}" in prompt
    assert "next hoof must not begin lifting" in prompt
    assert "At most one hoof is in swing" in prompt
    assert "four-hoof all-stance recovery frames are allowed" in prompt
    assert "at least three hooves remain planted" in prompt
    assert spec["generation_authorization_object"]["authorized_bool"] is False
    assert spec["approved_bool"] is False
    assert "Blender" in spec["fixed_factors_array"][-1]


def test_v11_loader_is_separate_and_v10_reference_endpoint_remains_compatible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    v11_root = tmp_path / "v11"
    v11_root.mkdir()
    experiment, reference, guides, artifacts = build_v11_contract(v11_root)
    pin_synthetic_v11_constants(monkeypatch, experiment)
    with pytest.raises(ControlledExperimentError, match="explicit --authorize-experiment"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V11_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    assert plan.reference_sha256 != plan.guide_frames[0].sha256
    assert plan.guide_frames[0].sha256 == plan.guide_frames[-1].sha256
    assert [frame.frame_index for frame in plan.guide_frames] == list(
        V10_GUIDE_FRAME_INDICES
    )

    v10_root = tmp_path / "v10"
    v10_root.mkdir()
    v10_experiment, v10_reference, v10_guides, v10_artifacts = build_v10_contract(
        v10_root
    )
    v10_plan = load_controlled_plan(
        experiment_path=v10_experiment,
        authorization=V10_EXPERIMENT_ID,
        reference_bundle=v10_reference,
        guide_bundle=v10_guides,
        artifact_root=v10_artifacts,
    )
    assert v10_plan.reference_sha256 == v10_plan.guide_frames[0].sha256
    assert v10_plan.guide_frames[0].sha256 == v10_plan.guide_frames[-1].sha256


def test_v11_real_f2_bundle_loads_with_exact_production_pins_when_available() -> None:
    backend_root = Path(__file__).resolve().parents[1]
    experiment = (
        backend_root
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v11_browser_static_scene_guides_seed_6550110377254033429.v1.json"
    )
    reference = Path(r"R:\ComfyUI-data\autorig-fitting\horse-canonical-f1")
    guides = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
        r"\horse-walk-v11-browser-static-scene-guides-f2"
    )
    artifacts = Path(
        r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
    )
    missing = [path for path in (experiment, reference, guides, artifacts) if not path.exists()]
    if missing:
        pytest.skip(f"external real f2 fixtures are absent: {missing}")

    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V11_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    assert plan.guide_manifest_sha256 == V11_GUIDE_MANIFEST_SHA256
    assert plan.guide_frames[0].sha256 == V11_ENDPOINT_GUIDE_SHA256
    assert plan.guide_frames[-1].sha256 == V11_ENDPOINT_GUIDE_SHA256
    assert plan.reference_sha256 != V11_ENDPOINT_GUIDE_SHA256


def test_v11_experiment_and_manifest_hash_and_parse_the_same_read_once_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    pin_synthetic_v11_constants(monkeypatch, experiment)
    manifest_path = guides / "immutable_manifest.json"
    experiment_original = experiment.read_bytes()
    manifest_original = manifest_path.read_bytes()
    expected_experiment_sha = sha(experiment_original)
    expected_manifest_sha = sha(manifest_original)
    targets = {
        experiment.resolve(): b'{"mutated_after_first_read":true}\n',
        manifest_path.resolve(): b'{"mutated_after_first_read":true}\n',
    }
    counts = {path: 0 for path in targets}
    real_read_bytes = Path.read_bytes

    def read_once_then_mutate(path: Path) -> bytes:
        data = real_read_bytes(path)
        resolved = path.resolve()
        if resolved in targets:
            counts[resolved] += 1
            if counts[resolved] == 1:
                path.write_bytes(targets[resolved])
        return data

    monkeypatch.setattr(Path, "read_bytes", read_once_then_mutate)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V11_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    assert counts[experiment.resolve()] == 1
    assert counts[manifest_path.resolve()] == 1
    assert plan.experiment_sha256 == expected_experiment_sha
    assert plan.guide_manifest_sha256 == expected_manifest_sha


def test_v11_rejects_arbitrary_contract_manifest_repin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    pin_synthetic_v11_constants(monkeypatch, experiment)
    manifest_path = guides / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["arbitrary_repin_marker"] = True
    write_json(manifest_path, manifest)
    repin_v11_manifest(experiment, guides)

    with pytest.raises(ControlledExperimentError, match="immutable guide manifest pin"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V11_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v11_rejects_arbitrary_contract_endpoint_repin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    pin_synthetic_v11_constants(monkeypatch, experiment)
    replacement = png_fixture(rgb=(10, 20, 30))
    repin_v11_frames(experiment, guides, {0: replacement, 48: replacement})
    contract = json.loads(experiment.read_text())
    monkeypatch.setattr(
        controlled_experiment_module,
        "V11_GUIDE_MANIFEST_SHA256",
        contract["guide_sequence_object"]["immutable_manifest_sha256_string"],
    )

    with pytest.raises(ControlledExperimentError, match="immutable endpoint guide pin"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V11_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v11_rejects_endpoint_guides_equal_to_source_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    reference_data = (reference / "reference_rgb.png").read_bytes()
    repin_v11_frames(experiment, guides, {0: reference_data, 48: reference_data})
    pin_synthetic_v11_constants(monkeypatch, experiment)

    with pytest.raises(ControlledExperimentError, match="explicitly differ"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V11_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


@pytest.mark.parametrize(
    "tamper_target",
    ["qa", "renderer", "browser_only", "manifest_bytes"],
)
def test_v11_static_scene_contract_fails_closed_on_tamper(
    tmp_path: Path, tamper_target: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    manifest_path = guides / "immutable_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    contract = json.loads(experiment.read_text())
    if tamper_target == "qa":
        manifest["staticSceneQa"]["maximum_background_channel_delta_int"] = 1
        contract["static_scene_contract_object"]["qa_summary_object"][
            "maximum_background_channel_delta_int"
        ] = 1
    elif tamper_target == "renderer":
        manifest["staticSceneRenderer"]["shadowsEnabled"] = True
        contract["static_scene_contract_object"]["renderer_settings_object"][
            "shadowsEnabled"
        ] = True
    elif tamper_target == "browser_only":
        manifest["browserOnly"] = False
    else:
        pin_synthetic_v11_constants(monkeypatch, experiment)
        manifest_path.write_bytes(manifest_path.read_bytes() + b"tamper")
        with pytest.raises(ControlledExperimentError, match="SHA-256 mismatch"):
            load_controlled_plan(
                experiment_path=experiment,
                authorization=V11_EXPERIMENT_ID,
                reference_bundle=reference,
                guide_bundle=guides,
                artifact_root=artifacts,
            )
        return
    write_json(manifest_path, manifest)
    repin_v11_manifest(experiment, guides)
    contract["guide_sequence_object"]["immutable_manifest_sha256_string"] = sha(
        manifest_path.read_bytes()
    )
    write_json(experiment, contract)
    pin_synthetic_v11_constants(monkeypatch, experiment)

    with pytest.raises(ControlledExperimentError, match="v11"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V11_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_plan_requires_exact_browser_bundle_hashes_and_cycle_endpoint(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    with pytest.raises(ControlledExperimentError, match="--guide-bundle"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            artifact_root=artifacts,
        )
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V10_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    assert plan.guide_bundle == guides.resolve()
    assert [frame.frame_index for frame in plan.guide_frames] == [0, 6, 18, 30, 42, 48]
    assert plan.guide_frames[0].sha256 == plan.reference_sha256
    assert plan.guide_frames[0].sha256 == plan.guide_frames[-1].sha256
    assert [frame.strength for frame in plan.guide_frames] == [
        0.8,
        0.7,
        0.7,
        0.7,
        0.7,
        0.8,
    ]

    (guides / "frame_018.png").write_bytes(b"mutated browser frame")
    with pytest.raises(ControlledExperimentError, match="SHA-256 mismatch"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_rejects_matching_cycle_endpoints_that_are_not_exact_reference(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    replacement = png_fixture(rgb=(220, 180, 140))
    repin_v10_frames(experiment, guides, {0: replacement, 48: replacement})

    with pytest.raises(ControlledExperimentError, match="immutable reference_rgb"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_rejects_corrupt_png_even_when_hash_and_size_are_pinned(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    corrupt = b"\x89PNG\r\n\x1a\ncorrupt-but-pinned"
    repin_v10_frames(experiment, guides, {18: corrupt})

    with pytest.raises(ControlledExperimentError, match="valid decodable PNG"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_rejects_decodable_png_with_wrong_dimensions(tmp_path: Path) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    wrong_size = png_fixture(width=767, height=448, rgb=(20, 80, 160))
    repin_v10_frames(experiment, guides, {18: wrong_size})

    with pytest.raises(ControlledExperimentError, match="must be exactly 768x448"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_rejects_duplicate_intermediate_png_hashes(tmp_path: Path) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    duplicate = (guides / "frame_006.png").read_bytes()
    repin_v10_frames(experiment, guides, {18: duplicate})

    with pytest.raises(ControlledExperimentError, match="pairwise distinct"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


@pytest.mark.parametrize(
    ("frame_index", "strength", "message"),
    [
        (0, 0.79, "endpoint guide strengths must be exactly 0.8"),
        (6, 0.69, "intermediate guide strengths must be exactly 0.7"),
        (48, 0.81, "endpoint guide strengths must be exactly 0.8"),
    ],
)
def test_v10_rejects_noncanonical_guide_strengths(
    tmp_path: Path, frame_index: int, strength: float, message: str
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    contract = json.loads(experiment.read_text())
    for row in contract["guide_sequence_object"]["frames_array"]:
        if row["frame_index_int"] == frame_index:
            row["strength_float"] = strength
    write_json(experiment, contract)

    with pytest.raises(ControlledExperimentError, match=message):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_rejects_noncanonical_experiment_endpoint_strength(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    contract = json.loads(experiment.read_text())
    contract["variants_array"][0]["start_guide_strength_float"] = 0.79
    contract["variants_array"][0]["end_guide_strength_float"] = 0.79
    contract["guide_sequence_object"]["frames_array"][0]["strength_float"] = 0.79
    contract["guide_sequence_object"]["frames_array"][-1]["strength_float"] = 0.79
    write_json(experiment, contract)

    with pytest.raises(ControlledExperimentError, match="experiment endpoint.*exactly 0.8"):
        load_controlled_plan(
            experiment_path=experiment,
            authorization=V10_EXPERIMENT_ID,
            reference_bundle=reference,
            guide_bundle=guides,
            artifact_root=artifacts,
        )


def test_v10_patch_chains_four_single_hoof_swing_guides_without_iclora() -> None:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "workflows"
        / "autorig_ltx2_animal_loop_v1_api.json"
    )
    original = json.loads(workflow_path.read_text())
    native = patch_native_resolution(original, width=768, height=448, resize_longer=768)
    result = patch_browser_keyframe_guides(
        native,
        uploaded_images={
            0: "guides/frame_000.png",
            6: "guides/frame_006.png",
            18: "guides/frame_018.png",
            30: "guides/frame_030.png",
            42: "guides/frame_042.png",
            48: "guides/frame_000.png",
        },
        strengths={0: 0.8, 6: 0.7, 18: 0.7, 30: 0.7, 42: 0.7, 48: 0.8},
    )
    guide_nodes = {
        node["inputs"]["frame_idx"]: node
        for node in result.values()
        if node.get("class_type") == "LTXVAddGuide"
    }
    assert set(guide_nodes) == {0, 6, 18, 30, 42, -1}
    assert guide_nodes[6]["inputs"]["positive"] == ["900001", 0]
    assert guide_nodes[18]["inputs"]["positive"] == ["94006", 0]
    assert guide_nodes[30]["inputs"]["positive"] == ["94018", 0]
    assert guide_nodes[42]["inputs"]["positive"] == ["94030", 0]
    assert guide_nodes[-1]["inputs"]["positive"] == ["94042", 0]
    assert result["92006"]["inputs"]["resize_type.longer_size"] == 768
    assert result["2004"]["inputs"]["image"] == "guides/frame_000.png"
    assert all(node.get("class_type") != "LTXICLoRALoaderModelOnly" for node in result.values())
    assert original["2004"]["inputs"]["image"] == "ltx_i2v_reference.png"

    with pytest.raises(ControlledExperimentError, match="frame 6 strength must be exactly 0.7"):
        patch_browser_keyframe_guides(
            native,
            uploaded_images={
                0: "guides/frame_000.png",
                6: "guides/frame_006.png",
                18: "guides/frame_018.png",
                30: "guides/frame_030.png",
                42: "guides/frame_042.png",
                48: "guides/frame_000.png",
            },
            strengths={0: 0.8, 6: 0.71, 18: 0.7, 30: 0.7, 42: 0.7, 48: 0.8},
        )


def test_v11_patch_binds_browser_endpoint_to_start_and_end_without_canonical_conditioning(
) -> None:
    workflow_path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "workflows"
        / "autorig_ltx2_animal_loop_v1_api.json"
    )
    original = json.loads(workflow_path.read_text())
    native = patch_native_resolution(original, width=768, height=448, resize_longer=768)
    endpoint_upload = f"guides/{V11_ENDPOINT_GUIDE_SHA256}/guide_000.png"
    result = patch_browser_keyframe_guides(
        native,
        uploaded_images={
            0: endpoint_upload,
            6: "guides/frame_006.png",
            18: "guides/frame_018.png",
            30: "guides/frame_030.png",
            42: "guides/frame_042.png",
            48: endpoint_upload,
        },
        strengths={0: 0.8, 6: 0.7, 18: 0.7, 30: 0.7, 42: 0.7, 48: 0.8},
    )
    titled = {
        node.get("_meta", {}).get("title"): (node_id, node)
        for node_id, node in result.items()
        if isinstance(node, dict) and node.get("_meta", {}).get("title")
    }
    start_id, start_guide = titled["AUTORIG_START_GUIDE"]
    _, end_guide = titled["AUTORIG_END_GUIDE_N_MINUS_1"]
    assert result["2004"]["inputs"]["image"] == endpoint_upload
    assert V11_ENDPOINT_GUIDE_SHA256 in result["2004"]["inputs"]["image"]
    assert start_guide["inputs"]["image"] == end_guide["inputs"]["image"]
    endpoint_preprocess_id = start_guide["inputs"]["image"][0]
    endpoint_resize_id = result[endpoint_preprocess_id]["inputs"]["image"][0]
    assert result[endpoint_resize_id]["inputs"]["input"] == ["2004", 0]
    assert all(
        node.get("inputs", {}).get("image") != "ltx_i2v_reference.png"
        for node in result.values()
        if isinstance(node, dict) and node.get("class_type") == "LoadImage"
    )
    assert result["94006"]["inputs"]["positive"] == [start_id, 0]
    assert result["94018"]["inputs"]["positive"] == ["94006", 0]
    assert result["94030"]["inputs"]["positive"] == ["94018", 0]
    assert result["94042"]["inputs"]["positive"] == ["94030", 0]
    assert end_guide["inputs"]["positive"] == ["94042", 0]

    with pytest.raises(
        ControlledExperimentError,
        match="frames 0 and 48 must use the same uploaded endpoint image",
    ):
        patch_browser_keyframe_guides(
            native,
            uploaded_images={
                0: endpoint_upload,
                6: "guides/frame_006.png",
                18: "guides/frame_018.png",
                30: "guides/frame_030.png",
                42: "guides/frame_042.png",
                48: "guides/different_endpoint.png",
            },
            strengths={
                0: 0.8,
                6: 0.7,
                18: 0.7,
                30: 0.7,
                42: 0.7,
                48: 0.8,
            },
        )


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

    async def upload_reference_image(
        self,
        _path: Path,
        *,
        expected_sha256: str | None = None,
        expected_size_bytes: int | None = None,
    ) -> str:
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


class V10FakeClient(FakeClient):
    def __init__(self, worker: ComfyWorker) -> None:
        super().__init__(worker)
        self.uploaded_paths: list[Path] = []

    async def upload_reference_image(
        self,
        path: Path,
        *,
        expected_sha256: str | None = None,
        expected_size_bytes: int | None = None,
    ) -> str:
        if expected_sha256 is not None:
            assert sha(path.read_bytes()) == expected_sha256
        if expected_size_bytes is not None:
            assert path.stat().st_size == expected_size_bytes
        self.uploaded_paths.append(path)
        return f"autorig_animation_fitting/{path.name}"

    async def submit(self, prompt, _idempotency_key: str) -> ComfySubmission:
        guides = {
            node["inputs"]["frame_idx"]: node["inputs"]["strength"]
            for node in prompt.values()
            if node.get("class_type") == "LTXVAddGuide"
        }
        assert guides == {
            0: 0.8,
            6: 0.7,
            18: 0.7,
            30: 0.7,
            42: 0.7,
            -1: 0.8,
        }
        assert all(
            node.get("class_type") != "LTXICLoRALoaderModelOnly"
            for node in prompt.values()
        )
        self.submitted = prompt
        return ComfySubmission(
            prompt_id="prompt-controlled-v10",
            client_id="client",
            resumed_existing_bool=False,
        )


class V11FakeClient(V10FakeClient):
    instances: list["V11FakeClient"] = []

    async def upload_reference_image(
        self,
        path: Path,
        *,
        expected_sha256: str | None = None,
        expected_size_bytes: int | None = None,
    ) -> str:
        self.uploaded_paths.append(path)
        digest = sha(path.read_bytes())
        assert expected_sha256 == digest
        assert expected_size_bytes == path.stat().st_size
        return f"autorig_animation_fitting/autorig_{digest[:32]}.png"

    async def submit(self, prompt, _idempotency_key: str) -> ComfySubmission:
        guides = {
            node["inputs"]["frame_idx"]: node["inputs"]["strength"]
            for node in prompt.values()
            if node.get("class_type") == "LTXVAddGuide"
        }
        assert guides == {
            0: 0.8,
            6: 0.7,
            18: 0.7,
            30: 0.7,
            42: 0.7,
            -1: 0.8,
        }
        start_guide = next(
            node
            for node in prompt.values()
            if node.get("_meta", {}).get("title") == "AUTORIG_START_GUIDE"
        )
        end_guide = next(
            node
            for node in prompt.values()
            if node.get("_meta", {}).get("title") == "AUTORIG_END_GUIDE_N_MINUS_1"
        )
        endpoint_digest = sha(self.uploaded_paths[0].read_bytes())
        assert endpoint_digest[:32] in prompt["2004"]["inputs"]["image"]
        assert start_guide["inputs"]["image"] == end_guide["inputs"]["image"]
        assert end_guide["inputs"]["positive"] == ["94042", 0]
        self.submitted = prompt
        return ComfySubmission(
            prompt_id="prompt-controlled-v11",
            client_id="client",
            resumed_existing_bool=False,
        )


class V10ActiveSamePromptClient(V10FakeClient):
    async def queue_load(self) -> int:
        return 1

    async def prompt_exists(self, _prompt_id: str) -> bool:
        return True

    async def submit(self, _prompt, _idempotency_key: str) -> ComfySubmission:
        raise AssertionError("active v10 prompt must resume without duplicate submission")


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


def test_v10_controlled_run_uploads_endpoint_plus_four_swing_frames_and_patches_graph(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V10_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-v10-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    V10FakeClient.instances.clear()
    result = asyncio.run(run_controlled_experiment(
        plan,
        worker=worker,
        client_factory=V10FakeClient,
        frame_extractor=FakeExtractor(),
    ))
    client = V10FakeClient.instances[-1]
    assert [path.name for path in client.uploaded_paths] == [
        "frame_000.png",
        "frame_006.png",
        "frame_018.png",
        "frame_030.png",
        "frame_042.png",
    ]
    assert client.submitted["2004"]["inputs"]["image"].endswith("frame_000.png")
    assert result.prompt_id == "prompt-controlled-v10"
    assert len(result.frames) == 49


def test_v11_controlled_run_uses_browser_endpoint_not_canonical_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    pin_synthetic_v11_constants(monkeypatch, experiment)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V11_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-v11-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    V11FakeClient.instances.clear()
    result = asyncio.run(run_controlled_experiment(
        plan,
        worker=worker,
        client_factory=V11FakeClient,
        frame_extractor=FakeExtractor(),
    ))
    client = V11FakeClient.instances[-1]
    assert [path.name for path in client.uploaded_paths] == [
        "guide_000.png",
        "guide_006.png",
        "guide_018.png",
        "guide_030.png",
        "guide_042.png",
    ]
    assert plan.reference_image not in client.uploaded_paths
    endpoint_upload = client.submitted["2004"]["inputs"]["image"]
    assert plan.guide_frames[0].sha256[:32] in endpoint_upload
    assert plan.reference_sha256[:32] not in endpoint_upload
    assert result.prompt_id == "prompt-controlled-v11"
    assert len(result.frames) == 49


def test_v11_controlled_run_rechecks_endpoint_pin_before_first_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment, reference, guides, artifacts = build_v11_contract(tmp_path)
    pin_synthetic_v11_constants(monkeypatch, experiment)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V11_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    endpoint_path = guides / "guide_000.png"
    mutated = bytearray(endpoint_path.read_bytes())
    mutated[-16] ^= 0x01
    endpoint_path.write_bytes(mutated)

    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-v11-mutation-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    V11FakeClient.instances.clear()
    with pytest.raises(ControlledExperimentError, match="v11 guide frame 0 SHA-256 mismatch"):
        asyncio.run(run_controlled_experiment(
            plan,
            worker=worker,
            client_factory=V11FakeClient,
            frame_extractor=FakeExtractor(),
        ))
    client = V11FakeClient.instances[-1]
    assert client.uploaded_paths == []
    assert client.submitted is None
    assert client.closed is True


def test_v10_controlled_run_rechecks_pinned_guide_before_upload(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V10_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    mutated_path = guides / "frame_018.png"
    mutated = bytearray(mutated_path.read_bytes())
    mutated[-16] ^= 0x01
    mutated_path.write_bytes(mutated)

    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-v10-mutation-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    V10FakeClient.instances.clear()
    with pytest.raises(ControlledExperimentError, match="frame 18 SHA-256 mismatch"):
        asyncio.run(run_controlled_experiment(
            plan,
            worker=worker,
            client_factory=V10FakeClient,
            frame_extractor=FakeExtractor(),
        ))
    assert [path.name for path in V10FakeClient.instances[-1].uploaded_paths] == [
        "frame_000.png",
        "frame_006.png",
    ]
    assert V10FakeClient.instances[-1].closed is True


def test_v10_controlled_run_resumes_active_prompt_without_reuploading_guides(
    tmp_path: Path,
) -> None:
    experiment, reference, guides, artifacts = build_v10_contract(tmp_path)
    plan = load_controlled_plan(
        experiment_path=experiment,
        authorization=V10_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guides,
        artifact_root=artifacts,
    )
    profile = load_animation_fitting_specs().workflows["loop"]
    worker = ComfyWorker(
        worker_id="local-4090-v10-active-test",
        base_url="http://127.0.0.1:8188",
        workflow_name=profile.workflow_name,
        expected_workflow_fingerprint=profile.workflow_fingerprint,
    )
    V10ActiveSamePromptClient.instances.clear()
    result = asyncio.run(run_controlled_experiment(
        plan,
        worker=worker,
        client_factory=V10ActiveSamePromptClient,
        frame_extractor=FakeExtractor(),
    ))
    client = V10ActiveSamePromptClient.instances[-1]
    assert client.uploaded_paths == []
    assert client.submitted is None
    assert result.resumed_existing_result is False
    assert len(result.frames) == 49


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

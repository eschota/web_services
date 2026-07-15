from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from animation_fitting.comfy import (
    ComfyOutputFile,
    ComfySubmission,
    ComfyWorker,
    deterministic_prompt_id,
)
from animation_fitting.controlled_experiment import (
    V14_BASE_VIDEO_LATENT_SLICES,
    V14_EXPERIMENT_ID,
    V14_EXPERIMENT_SPEC_SHA256,
    V14_FINAL_TEMPORAL_LATENT_SLICES,
    V14_GUIDE_BUNDLE_ID,
    V14_GUIDE_MANIFEST_SHA256,
    V14_GUIDE_TEMPORAL_LATENT_SLICES,
    V14_GUIDE_VIDEO_FILENAME,
    V14_GUIDE_VIDEO_SHA256,
    V14_GUIDE_VIDEO_SIZE_BYTES,
    ControlledExperimentError,
    _job_identity,
    _load_browser_interval_guide,
    load_controlled_plan,
    patch_browser_interval_video_guide,
    patch_guide_strengths,
    patch_native_resolution,
    run_controlled_experiment,
)


def v14_spec_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v14_browser_interval_guide_seed_6550110377254033429.v1.json"
    )


def workflow_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "workflows"
        / "autorig_ltx2_animal_loop_v1_api.json"
    )


def real_fixture_paths() -> tuple[Path, Path]:
    return (
        Path(r"R:\ComfyUI-data\autorig-fitting\horse-canonical-f1"),
        Path(
            r"R:\ComfyUI-data\autorig-fitting\canonical-candidates\experiments"
            rf"\{V14_GUIDE_BUNDLE_ID}"
        ),
    )


def test_v14_checked_in_spec_is_exact_single_interval_guide_and_unapproved() -> None:
    raw = v14_spec_path().read_bytes()
    spec = json.loads(raw)
    assert hashlib.sha256(raw).hexdigest() == V14_EXPERIMENT_SPEC_SHA256
    assert spec["experiment_id_string"] == V14_EXPERIMENT_ID
    assert spec["status_string"] == "prepared_for_single_controlled_generation"
    assert spec["seed_int"] == 6550110377254033429
    contract = spec["interval_guide_contract_object"]
    assert contract["bundle_id_string"] == V14_GUIDE_BUNDLE_ID
    assert contract["immutable_manifest_sha256_string"] == V14_GUIDE_MANIFEST_SHA256
    assert contract["video_filename_string"] == V14_GUIDE_VIDEO_FILENAME
    assert contract["video_sha256_string"] == V14_GUIDE_VIDEO_SHA256
    assert contract["video_bytes_int"] == V14_GUIDE_VIDEO_SIZE_BYTES
    assert contract["frame_count_int"] == 49
    assert contract["resolution_array"] == [768, 448]
    assert contract["ltxv_add_guide_count_int"] == 1
    assert contract["ltxv_add_guide_strength_float"] == 1.0
    assert contract["base_video_latent_slices_int"] == V14_BASE_VIDEO_LATENT_SLICES
    assert (
        contract["guide_temporal_latent_slices_int"]
        == V14_GUIDE_TEMPORAL_LATENT_SLICES
    )
    assert (
        contract["final_temporal_latent_slices_int"]
        == V14_FINAL_TEMPORAL_LATENT_SLICES
    )
    workflow = spec["workflow_object"]
    assert workflow["conditioning_implementation_string"] == (
        "official_LoadVideo_GetVideoComponents_LTXVPreprocess_"
        "LTXVAddGuide_chain_only"
    )
    assert workflow["conditioned_frames_array"] == [
        {
            "role_string": "full_browser_authored_49_frame_cycle",
            "frame_index_int": 0,
            "strength_float": 1.0,
        }
    ]
    assert spec["variants_array"] == [
        {
            "variant_id_string": "browser_interval_guide_seed_a",
            "start_guide_strength_float": 1.0,
            "end_guide_strength_float": 1.0,
        }
    ]
    assert spec["generation_authorization_object"]["authorized_bool"] is False
    assert spec["approved_bool"] is False


@pytest.mark.parametrize(
    "mutation",
    ("prompt", "seed", "guide_sha", "guide_size", "graph", "extra_field"),
)
def test_v14_same_id_tamper_rejects_before_reference_or_guide_reads(
    tmp_path: Path, mutation: str
) -> None:
    payload = json.loads(v14_spec_path().read_text())
    if mutation == "prompt":
        payload["positive_prompt_string"] += " tampered"
    elif mutation == "seed":
        payload["seed_int"] += 1
    elif mutation == "guide_sha":
        payload["interval_guide_contract_object"]["video_sha256_string"] = "0" * 64
    elif mutation == "guide_size":
        payload["interval_guide_contract_object"]["video_bytes_int"] += 1
    elif mutation == "graph":
        payload["interval_guide_contract_object"]["ltxv_add_guide_count_int"] = 2
    else:
        payload["tampered_bool"] = True
    tampered = tmp_path / "tampered-v14.json"
    tampered.write_text(json.dumps(payload, indent=2) + "\n")

    with pytest.raises(ControlledExperimentError, match="exact code-owned checked-in pin"):
        load_controlled_plan(
            experiment_path=tampered,
            authorization=V14_EXPERIMENT_ID,
            reference_bundle=tmp_path / "must-not-read-reference",
            guide_bundle=tmp_path / "must-not-read-guide",
            artifact_root=tmp_path / "artifacts",
        )


def test_v14_manifest_and_video_tamper_fail_closed(tmp_path: Path) -> None:
    _, real_bundle = real_fixture_paths()
    if not real_bundle.is_dir():
        pytest.skip("external immutable v14 browser interval bundle is absent")
    spec = json.loads(v14_spec_path().read_text())

    manifest_bundle = tmp_path / "manifest-tamper" / V14_GUIDE_BUNDLE_ID
    manifest_bundle.mkdir(parents=True)
    shutil.copyfile(
        real_bundle / "immutable_manifest.json",
        manifest_bundle / "immutable_manifest.json",
    )
    shutil.copyfile(
        real_bundle / V14_GUIDE_VIDEO_FILENAME,
        manifest_bundle / V14_GUIDE_VIDEO_FILENAME,
    )
    with (manifest_bundle / "immutable_manifest.json").open("ab") as stream:
        stream.write(b"\n")
    with pytest.raises(ControlledExperimentError, match="manifest SHA-256 mismatch"):
        _load_browser_interval_guide(
            spec,
            guide_bundle=manifest_bundle,
            reference_sha256=spec["reference_object"]["reference_png_sha256_string"],
            frame_count=49,
            start_strength=1.0,
            end_strength=1.0,
        )

    video_bundle = tmp_path / "video-tamper" / V14_GUIDE_BUNDLE_ID
    video_bundle.mkdir(parents=True)
    shutil.copyfile(
        real_bundle / "immutable_manifest.json",
        video_bundle / "immutable_manifest.json",
    )
    shutil.copyfile(
        real_bundle / V14_GUIDE_VIDEO_FILENAME,
        video_bundle / V14_GUIDE_VIDEO_FILENAME,
    )
    with (video_bundle / V14_GUIDE_VIDEO_FILENAME).open("ab") as stream:
        stream.write(b"tamper")
    with pytest.raises(ControlledExperimentError, match="code-owned SHA-256/size pin"):
        _load_browser_interval_guide(
            spec,
            guide_bundle=video_bundle,
            reference_sha256=spec["reference_object"]["reference_png_sha256_string"],
            frame_count=49,
            start_strength=1.0,
            end_strength=1.0,
        )


def test_v14_real_bundle_plan_and_job_identity_are_exact_and_deterministic(
    tmp_path: Path,
) -> None:
    reference, guide_bundle = real_fixture_paths()
    if not reference.is_dir() or not guide_bundle.is_dir():
        pytest.skip("external immutable v14 fixtures are absent")
    manifest_before = (guide_bundle / "immutable_manifest.json").read_bytes()
    video_before = (guide_bundle / V14_GUIDE_VIDEO_FILENAME).read_bytes()
    plan = load_controlled_plan(
        experiment_path=v14_spec_path(),
        authorization=V14_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guide_bundle,
        artifact_root=tmp_path / "artifacts",
    )
    assert hashlib.sha256(manifest_before).hexdigest() == V14_GUIDE_MANIFEST_SHA256
    assert hashlib.sha256(video_before).hexdigest() == V14_GUIDE_VIDEO_SHA256
    assert len(video_before) == V14_GUIDE_VIDEO_SIZE_BYTES
    assert (guide_bundle / "immutable_manifest.json").read_bytes() == manifest_before
    assert (guide_bundle / V14_GUIDE_VIDEO_FILENAME).read_bytes() == video_before
    assert plan.guide_frames == ()
    assert plan.guide_manifest_sha256 == V14_GUIDE_MANIFEST_SHA256
    assert plan.guide_video is not None
    assert plan.guide_video.video == (guide_bundle / V14_GUIDE_VIDEO_FILENAME).resolve()
    assert plan.guide_video.sha256 == V14_GUIDE_VIDEO_SHA256
    assert plan.guide_video.size_bytes == V14_GUIDE_VIDEO_SIZE_BYTES
    assert plan.guide_video.frame_count == 49
    assert plan.guide_video.strength == 1.0

    worker = ComfyWorker(
        worker_id="v14-test-worker",
        base_url="http://127.0.0.1:8188",
        workflow_name=plan.workflow_name,
        expected_workflow_fingerprint=plan.workflow_fingerprint,
    )
    first = _job_identity(plan, worker)
    second = _job_identity(plan, worker)
    assert first == second
    identity, job_id, idempotency_key = first
    assert identity["browser_interval_guide_object"] == {
        "guide_manifest_sha256_string": V14_GUIDE_MANIFEST_SHA256,
        "video_sha256_string": V14_GUIDE_VIDEO_SHA256,
        "video_bytes_int": V14_GUIDE_VIDEO_SIZE_BYTES,
        "frame_count_int": 49,
        "width_int": 768,
        "height_int": 448,
        "fps_int": 30,
        "strength_float": 1.0,
        "ltxv_add_guide_count_int": 1,
    }
    assert len(job_id) == 64
    assert idempotency_key.endswith(job_id)
    assert _job_identity(replace(plan, seed=plan.seed + 1), worker)[1] != job_id


def _patched_v14_graph() -> tuple[dict, str]:
    base = json.loads(workflow_path().read_text())
    native = patch_native_resolution(
        base, width=768, height=448, resize_longer=768
    )
    endpoints = patch_guide_strengths(
        native, start_strength=1.0, end_strength=1.0
    )
    uploaded = f"autorig_animation_fitting/autorig_{V14_GUIDE_VIDEO_SHA256[:32]}.mkv"
    return patch_browser_interval_video_guide(endpoints, uploaded_video=uploaded), uploaded


def test_v14_graph_inventory_wiring_and_temporal_budget_are_exact() -> None:
    graph, uploaded = _patched_v14_graph()
    by_class: dict[str, list[tuple[str, dict]]] = {}
    for node_id, node in graph.items():
        by_class.setdefault(node["class_type"], []).append((str(node_id), node))
    for class_type in (
        "LoadVideo",
        "GetVideoComponents",
        "ResizeImageMaskNode",
        "LTXVPreprocess",
        "LTXVAddGuide",
        "LTXVCropGuides",
    ):
        assert len(by_class[class_type]) == 1
    assert "LoadImage" not in by_class
    load_id, load = by_class["LoadVideo"][0]
    components_id, components = by_class["GetVideoComponents"][0]
    resize_id, resize = by_class["ResizeImageMaskNode"][0]
    preprocess_id, preprocess = by_class["LTXVPreprocess"][0]
    guide_id, guide = by_class["LTXVAddGuide"][0]
    _, crop = by_class["LTXVCropGuides"][0]
    assert load["inputs"] == {"file": uploaded}
    assert components["inputs"] == {"video": [load_id, 0]}
    assert resize["inputs"]["input"] == [components_id, 0]
    assert resize["inputs"]["resize_type.longer_size"] == 768
    assert preprocess["inputs"] == {"image": [resize_id, 0], "img_compression": 18}
    assert guide["inputs"]["image"] == [preprocess_id, 0]
    assert guide["inputs"]["frame_idx"] == 0
    assert guide["inputs"]["strength"] == 1.0
    assert crop["inputs"]["positive"] == [guide_id, 0]
    assert crop["inputs"]["negative"] == [guide_id, 1]
    concat = by_class["LTXVConcatAVLatent"][0][1]
    cfg = by_class["CFGGuider"][0][1]
    assert concat["inputs"]["video_latent"] == [guide_id, 2]
    assert cfg["inputs"]["positive"] == [guide_id, 0]
    assert cfg["inputs"]["negative"] == [guide_id, 1]
    latent = by_class["EmptyLTXVLatentVideo"][0][1]
    base_slices = ((latent["inputs"]["length"] - 1) // 8) + 1
    guide_slices = ((49 - 1) // 8) + 1
    assert base_slices == V14_BASE_VIDEO_LATENT_SLICES
    assert guide_slices == V14_GUIDE_TEMPORAL_LATENT_SLICES
    assert base_slices + guide_slices == V14_FINAL_TEMPORAL_LATENT_SLICES

    invalid = json.loads(workflow_path().read_text())
    invalid = patch_native_resolution(
        invalid, width=768, height=448, resize_longer=768
    )
    invalid = patch_guide_strengths(
        invalid, start_strength=1.0, end_strength=1.0
    )
    invalid["extra-guide"] = json.loads(json.dumps(invalid["900001"]))
    invalid["extra-guide"]["_meta"]["title"] = "EXTRA_GUIDE"
    with pytest.raises(ControlledExperimentError, match="exact two-endpoint"):
        patch_browser_interval_video_guide(invalid, uploaded_video=uploaded)


class _FakeExtractor:
    def extract_and_store(self, raw_video, store, *, expected_frame_count: int):
        return tuple(
            store.store_frame(
                raw_video.sha256,
                index,
                f"v14-frame-{index:03d}".encode(),
            )
            for index in range(expected_frame_count)
        )


class _V14Client:
    instances: list["_V14Client"] = []

    def __init__(self, worker: ComfyWorker) -> None:
        self.worker = worker
        self.video_uploads = 0
        self.image_uploads = 0
        self.submitted = None
        self.idempotency_key = ""
        self.closed = False
        self.__class__.instances.append(self)

    async def fetch_api_workflow(self):
        return json.loads(workflow_path().read_text()), self.worker.expected_workflow_fingerprint

    async def queue_load(self) -> int:
        return 0

    async def upload_reference_image(self, *_args, **_kwargs):
        self.image_uploads += 1
        raise AssertionError("v14 must not upload the provenance-only reference PNG")

    async def upload_reference_video(
        self,
        path: Path,
        *,
        expected_sha256: str,
        expected_size_bytes: int,
    ) -> str:
        self.video_uploads += 1
        data = path.read_bytes()
        assert hashlib.sha256(data).hexdigest() == expected_sha256
        assert len(data) == expected_size_bytes
        return f"autorig_animation_fitting/autorig_{expected_sha256[:32]}.mkv"

    async def submit(self, prompt, idempotency_key: str) -> ComfySubmission:
        self.submitted = prompt
        self.idempotency_key = idempotency_key
        assert sum(
            node.get("class_type") == "LTXVAddGuide" for node in prompt.values()
        ) == 1
        assert sum(node.get("class_type") == "LoadVideo" for node in prompt.values()) == 1
        return ComfySubmission(
            prompt_id="prompt-v14-controlled",
            client_id="client-v14",
            resumed_existing_bool=False,
        )

    async def wait_for_output(self, _prompt_id: str):
        return {}, ComfyOutputFile("horse_walk_v14.mp4")

    async def download_output(self, _output: ComfyOutputFile) -> bytes:
        return b"synthetic v14 controlled MP4 payload long enough for immutable storage"

    async def close(self) -> None:
        self.closed = True


class _V14ActivePromptClient(_V14Client):
    instances: list["_V14ActivePromptClient"] = []

    async def queue_load(self) -> int:
        return 1

    async def prompt_exists(self, _prompt_id: str) -> bool:
        return True

    async def upload_reference_video(self, *_args, **_kwargs):
        raise AssertionError("active exact prompt must not re-upload the v14 guide")

    async def submit(self, *_args, **_kwargs):
        raise AssertionError("active exact prompt must not submit a duplicate")


def test_v14_run_uploads_video_once_patches_graph_and_active_resume_skips_upload(
    tmp_path: Path,
) -> None:
    reference, guide_bundle = real_fixture_paths()
    if not reference.is_dir() or not guide_bundle.is_dir():
        pytest.skip("external immutable v14 fixtures are absent")
    base_plan = load_controlled_plan(
        experiment_path=v14_spec_path(),
        authorization=V14_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guide_bundle,
        artifact_root=tmp_path / "normal",
    )
    worker = ComfyWorker(
        worker_id="v14-run-worker",
        base_url="http://127.0.0.1:8188",
        workflow_name=base_plan.workflow_name,
        expected_workflow_fingerprint=base_plan.workflow_fingerprint,
    )
    _V14Client.instances.clear()
    result = asyncio.run(
        run_controlled_experiment(
            base_plan,
            worker=worker,
            client_factory=_V14Client,
            frame_extractor=_FakeExtractor(),
        )
    )
    client = _V14Client.instances[-1]
    assert result.resumed_existing_result is False
    assert client.video_uploads == 1
    assert client.image_uploads == 0
    assert client.submitted is not None
    assert client.closed is True
    assert client.idempotency_key == _job_identity(base_plan, worker)[2]

    active_plan = replace(base_plan, artifact_root=(tmp_path / "active").resolve())
    _V14ActivePromptClient.instances.clear()
    active = asyncio.run(
        run_controlled_experiment(
            active_plan,
            worker=worker,
            client_factory=_V14ActivePromptClient,
            frame_extractor=_FakeExtractor(),
        )
    )
    active_client = _V14ActivePromptClient.instances[-1]
    assert active.resumed_existing_result is False
    assert active.prompt_id == deterministic_prompt_id(
        _job_identity(active_plan, worker)[2]
    )
    assert active_client.video_uploads == 0
    assert active_client.image_uploads == 0
    assert active_client.submitted is None
    assert active_client.closed is True

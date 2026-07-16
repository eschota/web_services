from __future__ import annotations

import asyncio
import hashlib
import json
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
    V12_ENDPOINT_GUIDE_SHA256,
    V14_BASE_VIDEO_LATENT_SLICES,
    V14_GUIDE_BUNDLE_ID,
    V14_GUIDE_MANIFEST_SHA256,
    V14_GUIDE_TEMPORAL_LATENT_SLICES,
    V14_GUIDE_VIDEO_FILENAME,
    V14_GUIDE_VIDEO_SHA256,
    V14_GUIDE_VIDEO_SIZE_BYTES,
    V15_ENDPOINT_FRAME_INDICES,
    V15_ENDPOINT_GUIDE_CONTRACT,
    V15_ENDPOINT_GUIDE_FILENAME,
    V15_ENDPOINT_GUIDE_SIZE_BYTES,
    V15_ENDPOINT_GUIDE_STRENGTHS,
    V15_EXPERIMENT_ID,
    V15_EXPERIMENT_SPEC_SHA256,
    V15_FINAL_TEMPORAL_LATENT_SLICES,
    ControlledExperimentError,
    _job_identity,
    load_controlled_plan,
    patch_browser_interval_video_with_hard_endpoints,
    patch_guide_strengths,
    patch_native_resolution,
    run_controlled_experiment,
)


def v15_spec_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "experiments"
        / "horse_walk_v15_browser_interval_hard_endpoints_"
        "seed_6550110377254033429.v1.json"
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


def test_v15_checked_in_spec_is_exact_hybrid_and_unapproved() -> None:
    raw = v15_spec_path().read_bytes()
    spec = json.loads(raw)
    assert hashlib.sha256(raw).hexdigest() == V15_EXPERIMENT_SPEC_SHA256
    assert spec["experiment_id_string"] == V15_EXPERIMENT_ID
    assert spec["status_string"] == "prepared_for_single_controlled_generation"
    assert spec["seed_int"] == 6550110377254033429
    interval = spec["interval_guide_contract_object"]
    assert interval["bundle_id_string"] == V14_GUIDE_BUNDLE_ID
    assert interval["immutable_manifest_sha256_string"] == V14_GUIDE_MANIFEST_SHA256
    assert interval["video_sha256_string"] == V14_GUIDE_VIDEO_SHA256
    assert interval["video_bytes_int"] == V14_GUIDE_VIDEO_SIZE_BYTES
    endpoints = spec["hard_endpoint_contract_object"]
    assert endpoints["guide_contract_string"] == V15_ENDPOINT_GUIDE_CONTRACT
    assert endpoints["image_filename_string"] == V15_ENDPOINT_GUIDE_FILENAME
    assert endpoints["image_sha256_string"] == V12_ENDPOINT_GUIDE_SHA256
    assert endpoints["image_bytes_int"] == V15_ENDPOINT_GUIDE_SIZE_BYTES
    assert endpoints["frame_indices_array"] == list(V15_ENDPOINT_FRAME_INDICES)
    assert endpoints["strengths_array"] == list(V15_ENDPOINT_GUIDE_STRENGTHS)
    assert endpoints["chain_order_array"] == [
        "interval_video",
        "hard_start_0",
        "hard_end_48",
    ]
    assert endpoints["final_temporal_latent_slices_int"] == 16
    assert spec["generation_authorization_object"]["authorized_bool"] is False
    assert spec["approved_bool"] is False


@pytest.mark.parametrize(
    "mutation",
    ("prompt", "seed", "video_sha", "endpoint_sha", "graph", "extra_field"),
)
def test_v15_same_id_tamper_rejects_before_fixture_reads(
    tmp_path: Path, mutation: str
) -> None:
    payload = json.loads(v15_spec_path().read_text())
    if mutation == "prompt":
        payload["positive_prompt_string"] += " tampered"
    elif mutation == "seed":
        payload["seed_int"] += 1
    elif mutation == "video_sha":
        payload["interval_guide_contract_object"]["video_sha256_string"] = "0" * 64
    elif mutation == "endpoint_sha":
        payload["hard_endpoint_contract_object"]["image_sha256_string"] = "0" * 64
    elif mutation == "graph":
        payload["hard_endpoint_contract_object"]["total_ltxv_add_guide_count_int"] = 2
    else:
        payload["tampered_bool"] = True
    tampered = tmp_path / "tampered-v15.json"
    tampered.write_text(json.dumps(payload, indent=2) + "\n")

    with pytest.raises(ControlledExperimentError, match="exact code-owned checked-in pin"):
        load_controlled_plan(
            experiment_path=tampered,
            authorization=V15_EXPERIMENT_ID,
            reference_bundle=tmp_path / "must-not-read-reference",
            guide_bundle=tmp_path / "must-not-read-guide",
            artifact_root=tmp_path / "artifacts",
        )


def test_v15_real_plan_pins_interval_and_byte_identical_endpoints(
    tmp_path: Path,
) -> None:
    reference, guide_bundle = real_fixture_paths()
    if not reference.is_dir() or not guide_bundle.is_dir():
        pytest.skip("external immutable v15 fixtures are absent")
    endpoint_before = (guide_bundle / V15_ENDPOINT_GUIDE_FILENAME).read_bytes()
    video_before = (guide_bundle / V14_GUIDE_VIDEO_FILENAME).read_bytes()
    plan = load_controlled_plan(
        experiment_path=v15_spec_path(),
        authorization=V15_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guide_bundle,
        artifact_root=tmp_path / "artifacts",
    )
    assert hashlib.sha256(endpoint_before).hexdigest() == V12_ENDPOINT_GUIDE_SHA256
    assert len(endpoint_before) == V15_ENDPOINT_GUIDE_SIZE_BYTES
    assert hashlib.sha256(video_before).hexdigest() == V14_GUIDE_VIDEO_SHA256
    assert len(video_before) == V14_GUIDE_VIDEO_SIZE_BYTES
    assert tuple(frame.frame_index for frame in plan.guide_frames) == (0, 48)
    assert tuple(frame.strength for frame in plan.guide_frames) == (1.0, 1.0)
    assert plan.guide_frames[0].image == plan.guide_frames[1].image
    assert plan.guide_frames[0].sha256 == plan.guide_frames[1].sha256
    assert plan.guide_video is not None
    assert plan.guide_video.sha256 == V14_GUIDE_VIDEO_SHA256
    assert plan.guide_manifest_sha256 == V14_GUIDE_MANIFEST_SHA256

    worker = ComfyWorker(
        worker_id="v15-test-worker",
        base_url="http://127.0.0.1:8188",
        workflow_name=plan.workflow_name,
        expected_workflow_fingerprint=plan.workflow_fingerprint,
    )
    identity, job_id, idempotency_key = _job_identity(plan, worker)
    assert identity["browser_guide_sequence_object"]["frames_array"] == [
        {
            "frame_index_int": 0,
            "sha256_string": V12_ENDPOINT_GUIDE_SHA256,
            "strength_float": 1.0,
        },
        {
            "frame_index_int": 48,
            "sha256_string": V12_ENDPOINT_GUIDE_SHA256,
            "strength_float": 1.0,
        },
    ]
    assert identity["browser_interval_guide_object"]["video_sha256_string"] == (
        V14_GUIDE_VIDEO_SHA256
    )
    assert len(job_id) == 64
    assert idempotency_key.endswith(job_id)


def _patched_v15_graph() -> tuple[dict, str]:
    base = json.loads(workflow_path().read_text())
    native = patch_native_resolution(base, width=768, height=448, resize_longer=768)
    endpoints = patch_guide_strengths(native, start_strength=1.0, end_strength=1.0)
    uploaded = f"autorig_animation_fitting/autorig_{V14_GUIDE_VIDEO_SHA256[:32]}.mkv"
    return (
        patch_browser_interval_video_with_hard_endpoints(
            endpoints,
            uploaded_video=uploaded,
        ),
        uploaded,
    )


def test_v15_graph_wiring_inventory_and_temporal_budget_are_exact() -> None:
    graph, uploaded = _patched_v15_graph()
    by_class: dict[str, list[tuple[str, dict]]] = {}
    by_title: dict[str, tuple[str, dict]] = {}
    for node_id, node in graph.items():
        by_class.setdefault(node["class_type"], []).append((str(node_id), node))
        title = node.get("_meta", {}).get("title")
        if title:
            by_title[title] = (str(node_id), node)
    assert len(by_class["LoadImage"]) == 1
    assert len(by_class["LoadVideo"]) == 1
    assert len(by_class["GetVideoComponents"]) == 1
    assert len(by_class["ResizeImageMaskNode"]) == 2
    assert len(by_class["LTXVPreprocess"]) == 2
    assert len(by_class["LTXVAddGuide"]) == 3
    assert len(by_class["LTXVCropGuides"]) == 1

    interval_load_id, interval_load = by_title["AUTORIG_INTERVAL_GUIDE_VIDEO"]
    components_id, components = by_title["AUTORIG_INTERVAL_GUIDE_COMPONENTS"]
    interval_resize_id, interval_resize = by_title["AUTORIG_INTERVAL_GUIDE_RESIZE"]
    interval_preprocess_id, interval_preprocess = by_title[
        "AUTORIG_INTERVAL_GUIDE_PREPROCESS"
    ]
    interval_id, interval = by_title["AUTORIG_INTERVAL_GUIDE_ADD"]
    start_id, start = by_title["AUTORIG_START_GUIDE"]
    end_id, end = by_title["AUTORIG_END_GUIDE_N_MINUS_1"]
    _, crop = by_title["AUTORIG_CROP_GUIDE_LATENTS"]
    assert interval_load["inputs"] == {"file": uploaded}
    assert components["inputs"] == {"video": [interval_load_id, 0]}
    assert interval_resize["inputs"]["input"] == [components_id, 0]
    assert interval_preprocess["inputs"]["image"] == [interval_resize_id, 0]
    assert interval["inputs"]["image"] == [interval_preprocess_id, 0]
    assert interval["inputs"]["frame_idx"] == 0
    assert interval["inputs"]["strength"] == 1.0
    assert start["inputs"]["positive"] == [interval_id, 0]
    assert start["inputs"]["negative"] == [interval_id, 1]
    assert start["inputs"]["latent"] == [interval_id, 2]
    assert start["inputs"]["frame_idx"] == 0
    assert start["inputs"]["strength"] == 1.0
    assert end["inputs"]["positive"] == [start_id, 0]
    assert end["inputs"]["negative"] == [start_id, 1]
    assert end["inputs"]["latent"] == [start_id, 2]
    assert end["inputs"]["frame_idx"] == -1
    assert end["inputs"]["strength"] == 1.0
    assert crop["inputs"]["positive"] == [end_id, 0]
    assert crop["inputs"]["negative"] == [end_id, 1]
    latent = by_class["EmptyLTXVLatentVideo"][0][1]
    base_slices = ((latent["inputs"]["length"] - 1) // 8) + 1
    guide_slices = ((49 - 1) // 8) + 1
    assert base_slices == V14_BASE_VIDEO_LATENT_SLICES
    assert guide_slices == V14_GUIDE_TEMPORAL_LATENT_SLICES
    assert base_slices + guide_slices + 2 == V15_FINAL_TEMPORAL_LATENT_SLICES


class _FakeExtractor:
    def extract_and_store(self, raw_video, store, *, expected_frame_count: int):
        return tuple(
            store.store_frame(
                raw_video.sha256,
                index,
                f"v15-frame-{index:03d}".encode(),
            )
            for index in range(expected_frame_count)
        )


class _V15Client:
    instances: list["_V15Client"] = []

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

    async def upload_reference_image(
        self,
        path: Path,
        *,
        expected_sha256: str,
        expected_size_bytes: int,
    ) -> str:
        self.image_uploads += 1
        data = path.read_bytes()
        assert hashlib.sha256(data).hexdigest() == expected_sha256
        assert len(data) == expected_size_bytes
        return f"autorig_animation_fitting/autorig_{expected_sha256[:32]}.png"

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
        ) == 3
        assert sum(node.get("class_type") == "LoadImage" for node in prompt.values()) == 1
        assert sum(node.get("class_type") == "LoadVideo" for node in prompt.values()) == 1
        return ComfySubmission(
            prompt_id="prompt-v15-controlled",
            client_id="client-v15",
            resumed_existing_bool=False,
        )

    async def wait_for_output(self, _prompt_id: str):
        return {}, ComfyOutputFile("horse_walk_v15.mp4")

    async def download_output(self, _output: ComfyOutputFile) -> bytes:
        return b"synthetic v15 hybrid controlled MP4 payload long enough for storage"

    async def close(self) -> None:
        self.closed = True


class _V15ActivePromptClient(_V15Client):
    instances: list["_V15ActivePromptClient"] = []

    async def queue_load(self) -> int:
        return 1

    async def prompt_exists(self, _prompt_id: str) -> bool:
        return True

    async def upload_reference_image(self, *_args, **_kwargs):
        raise AssertionError("active exact prompt must not re-upload the endpoint")

    async def upload_reference_video(self, *_args, **_kwargs):
        raise AssertionError("active exact prompt must not re-upload the interval guide")

    async def submit(self, *_args, **_kwargs):
        raise AssertionError("active exact prompt must not submit a duplicate")


def test_v15_run_uploads_one_image_and_video_and_active_resume_skips_both(
    tmp_path: Path,
) -> None:
    reference, guide_bundle = real_fixture_paths()
    if not reference.is_dir() or not guide_bundle.is_dir():
        pytest.skip("external immutable v15 fixtures are absent")
    base_plan = load_controlled_plan(
        experiment_path=v15_spec_path(),
        authorization=V15_EXPERIMENT_ID,
        reference_bundle=reference,
        guide_bundle=guide_bundle,
        artifact_root=tmp_path / "normal",
    )
    worker = ComfyWorker(
        worker_id="v15-run-worker",
        base_url="http://127.0.0.1:8188",
        workflow_name=base_plan.workflow_name,
        expected_workflow_fingerprint=base_plan.workflow_fingerprint,
    )
    _V15Client.instances.clear()
    result = asyncio.run(
        run_controlled_experiment(
            base_plan,
            worker=worker,
            client_factory=_V15Client,
            frame_extractor=_FakeExtractor(),
        )
    )
    client = _V15Client.instances[-1]
    assert result.resumed_existing_result is False
    assert client.image_uploads == 1
    assert client.video_uploads == 1
    assert client.submitted is not None
    assert client.closed is True
    assert client.idempotency_key == _job_identity(base_plan, worker)[2]

    active_plan = replace(base_plan, artifact_root=(tmp_path / "active").resolve())
    _V15ActivePromptClient.instances.clear()
    active = asyncio.run(
        run_controlled_experiment(
            active_plan,
            worker=worker,
            client_factory=_V15ActivePromptClient,
            frame_extractor=_FakeExtractor(),
        )
    )
    active_client = _V15ActivePromptClient.instances[-1]
    assert active.resumed_existing_result is False
    assert active.prompt_id == deterministic_prompt_id(
        _job_identity(active_plan, worker)[2]
    )
    assert active_client.image_uploads == 0
    assert active_client.video_uploads == 0
    assert active_client.submitted is None
    assert active_client.closed is True

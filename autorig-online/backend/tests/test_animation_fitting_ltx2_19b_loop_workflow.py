import json
from pathlib import Path
from typing import Any, Iterator


WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1]
    / "animation_fitting"
    / "specs"
    / "workflows"
    / "autorig_animal_loop_ltx2_19b_v1_api.json"
)


def _matching_input_paths(
    value: Any,
    *,
    node_id: str,
    path: tuple[str, ...] = (),
    predicate,
) -> Iterator[tuple[str, str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            if predicate(str(key)):
                yield node_id, ".".join(child_path), child
            yield from _matching_input_paths(
                child,
                node_id=node_id,
                path=child_path,
                predicate=predicate,
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _matching_input_paths(
                child,
                node_id=node_id,
                path=(*path, str(index)),
                predicate=predicate,
            )


def test_ltx2_19b_loop_workflow_is_pinned_to_dynamic_last_frame_contract() -> None:
    workflow = json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))

    titled = {
        node["_meta"]["title"]: node_id
        for node_id, node in workflow.items()
        if node.get("_meta", {}).get("title")
    }
    assert titled["AUTORIG_VIDEO_LATENT"] == "3059"
    assert titled["AUTORIG_AUDIO_LATENT"] == "3980"
    assert titled["AUTORIG_SEED"] == "4832"
    assert titled["AUTORIG_START_GUIDE"] == "900001"
    assert titled["AUTORIG_END_GUIDE_N_MINUS_1"] == "900002"

    latent = workflow["3059"]["inputs"]
    assert (latent["width"], latent["height"], latent["length"]) == (384, 224, 97)

    audio_latent = workflow["3980"]["inputs"]
    assert audio_latent["frames_number"] == 97
    assert audio_latent["frame_rate"] == 24
    assert workflow["1241"]["inputs"]["frame_rate"] == 24.0

    start_guide = workflow["900001"]["inputs"]
    end_guide = workflow["900002"]["inputs"]
    assert start_guide["frame_idx"] == 0
    assert end_guide["frame_idx"] == -1
    assert start_guide["image"] == end_guide["image"]

    guide_nodes = [
        node_id
        for node_id, node in workflow.items()
        if node.get("class_type") == "LTXVAddGuide"
    ]
    assert guide_nodes == ["900001", "900002"]

    frame_inputs = {
        (node_id, path): value
        for node_id, node in workflow.items()
        for node_id, path, value in _matching_input_paths(
            node.get("inputs", {}),
            node_id=node_id,
            predicate=lambda key: (
                "frame" in key.casefold()
                or key.casefold() == "length"
                or key.casefold() == "fps"
            ),
        )
    }
    assert frame_inputs == {
        ("1241", "frame_rate"): 24.0,
        ("3059", "length"): 97,
        ("3980", "frame_rate"): 24,
        ("3980", "frames_number"): 97,
        ("4849", "fps"): 30.0,
        ("4982", "last_frame_fix"): False,
        ("900001", "frame_idx"): 0,
        ("900002", "frame_idx"): -1,
    }

    seed_inputs = {
        (node_id, path): value
        for node_id, node in workflow.items()
        for node_id, path, value in _matching_input_paths(
            node.get("inputs", {}),
            node_id=node_id,
            predicate=lambda key: "seed" in key.casefold(),
        )
    }
    assert seed_inputs == {("4832", "noise_seed"): 43}
    assert workflow["4832"]["class_type"] == "RandomNoise"
    assert workflow["4832"]["_meta"]["title"] == "AUTORIG_SEED"

    assert workflow["3940"]["inputs"]["ckpt_name"] == "ltx-2-19b-distilled-fp8.safetensors"
    assert workflow["4010"]["inputs"]["ckpt_name"] == "ltx-2-19b-distilled-fp8.safetensors"
    assert (
        workflow["4922"]["inputs"]["lora_name"]
        == "ltx-2-19b-lora-camera-control-static.safetensors"
    )
    assert workflow["4849"]["inputs"]["fps"] == 30.0
    output = workflow["4852"]["inputs"]
    assert output["format"] == "mp4"
    assert output["codec"] == "h264"
    assert output["filename_prefix"].endswith("candidate_ltx2_19b_v1")

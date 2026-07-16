import json
from pathlib import Path
from typing import Any, Iterator


WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1]
    / "animation_fitting"
    / "specs"
    / "workflows"
    / "autorig_animal_oneshot_ltx2_19b_v1_api.json"
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


def _titles(workflow: dict[str, dict[str, Any]]) -> dict[str, tuple[str, dict[str, Any]]]:
    return {
        node["_meta"]["title"]: (node_id, node)
        for node_id, node in workflow.items()
        if node.get("_meta", {}).get("title")
    }


def test_ltx2_19b_oneshot_workflow_is_pinned_to_65_frame_start_only_contract() -> None:
    workflow = json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))
    titled = _titles(workflow)

    assert titled["AUTORIG_VIDEO_LATENT"][0] == "3059"
    assert titled["AUTORIG_AUDIO_LATENT"][0] == "3980"
    assert titled["AUTORIG_SEED"][0] == "4832"
    assert titled["AUTORIG_START_GUIDE"][0] == "900001"
    assert "AUTORIG_END_GUIDE_N_MINUS_1" not in titled

    video_latent = workflow["3059"]["inputs"]
    assert (video_latent["width"], video_latent["height"], video_latent["length"]) == (
        384,
        224,
        65,
    )

    assert workflow["3980"]["inputs"]["frames_number"] == 65
    assert workflow["4528"]["inputs"]["video_latent"] == ["900001", 2]
    assert workflow["900001"]["inputs"]["frame_idx"] == 0
    assert workflow["900001"]["inputs"]["latent"] == ["3059", 0]
    assert workflow["4849"]["inputs"]["fps"] == 30.0

    guide_nodes = [
        (node_id, node)
        for node_id, node in workflow.items()
        if node.get("class_type") == "LTXVAddGuide"
    ]
    assert [node_id for node_id, _node in guide_nodes] == ["900001"]


def test_ltx2_19b_oneshot_exhaustively_pins_every_frame_and_seed_input() -> None:
    workflow = json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))

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
        ("3059", "length"): 65,
        ("3980", "frame_rate"): 24,
        ("3980", "frames_number"): 65,
        ("4849", "fps"): 30.0,
        ("4982", "last_frame_fix"): False,
        ("900001", "frame_idx"): 0,
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


def test_ltx2_19b_oneshot_uses_static_camera_19b_and_versioned_output() -> None:
    workflow = json.loads(WORKFLOW_PATH.read_text(encoding="utf-8"))

    assert workflow["3940"]["inputs"]["ckpt_name"] == (
        "ltx-2-19b-distilled-fp8.safetensors"
    )
    assert workflow["4010"]["inputs"]["ckpt_name"] == (
        "ltx-2-19b-distilled-fp8.safetensors"
    )
    assert workflow["4922"]["inputs"]["lora_name"] == (
        "ltx-2-19b-lora-camera-control-static.safetensors"
    )

    output = workflow["4852"]["inputs"]
    assert output["format"] == "mp4"
    assert output["codec"] == "h264"
    assert output["filename_prefix"].endswith("candidate_oneshot_ltx2_19b_v1")

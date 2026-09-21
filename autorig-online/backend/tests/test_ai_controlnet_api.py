import json
from pathlib import Path

import pytest
from fastapi import HTTPException

import ai_controlnet_api
from renderfin import templating


WORKFLOWS = Path(__file__).parents[1] / "renderfin" / "assets" / "workflows"


def test_control_channels_route_to_separate_workflow_types():
    for channel, render_type in ai_controlnet_api.CHANNEL_TYPES.items():
        payload = ai_controlnet_api.renderfin_payload(channel, "https://example.test/input.png")
        assert payload["type"] == render_type
        assert payload["main_size_width"] == 960
        assert payload["main_size_height"] == 540


def test_control_channel_rejects_unknown_value():
    with pytest.raises(HTTPException) as caught:
        ai_controlnet_api.renderfin_payload("normal", "https://example.test/input.png")
    assert caught.value.status_code == 400


@pytest.mark.parametrize("channel,node_type", [
    ("pose", "OpenposePreprocessor"),
    ("depth", "DepthAnythingV2Preprocessor"),
    ("canny", "CannyEdgePreprocessor"),
])
def test_extraction_workflow_is_templatable(channel, node_type):
    path = WORKFLOWS / f"gen_control_{channel}.json"
    workflow = templating.render_workflow_text(
        path.read_text(encoding="utf-8"), width=960, height=540,
        prompt="", negative_prompt="", image_filename="input.png",
        output_prefix="control/test",
    )
    assert any(node.get("class_type") == node_type for node in workflow.values())
    assert any(node.get("class_type") == "SaveImage" for node in workflow.values())
    assert "$image" not in json.dumps(workflow)
    assert "$output_url" not in json.dumps(workflow)


@pytest.mark.parametrize("channel,union_type", [
    ("pose", "openpose"),
    ("depth", "depth"),
    ("canny", "canny/lineart/anime_lineart/mlsd"),
])
def test_flux_control_workflow_uses_extracted_map_directly(channel, union_type):
    path = WORKFLOWS / f"gen_image_control_{channel}.json"
    workflow = templating.render_workflow_text(
        path.read_text(encoding="utf-8"), prompt="two people beside a truck",
        negative_prompt="", image_filename="control.png",
        output_prefix="control/result", width=960, height=540,
    )
    union = next(node for node in workflow.values()
                 if node.get("class_type") == "SetUnionControlNetType")
    apply = next(node for node in workflow.values()
                 if node.get("class_type") == "ControlNetApplyAdvanced")
    latent = next(node for node in workflow.values()
                  if node.get("class_type") == "EmptyLatentImage")
    assert union["inputs"]["type"] == union_type
    assert apply["inputs"]["image"] == ["7", 0]
    assert latent["inputs"]["width"] == 960
    assert latent["inputs"]["height"] == 540


@pytest.mark.parametrize("channel,union_type", [
    ("pose", "openpose"),
    ("depth", "depth"),
    ("canny", "canny/lineart/anime_lineart/mlsd"),
])
def test_sdxl_control_workflow_uses_pony_and_union_map(channel, union_type):
    path = WORKFLOWS / f"gen_image_sdxl_control_{channel}.json"
    workflow = templating.render_workflow_text(
        path.read_text(encoding="utf-8"), prompt="two people beside a truck",
        negative_prompt="blurry", image_filename="control.png",
        output_prefix="control/sdxl-result", width=640, height=960,
    )
    checkpoint = next(node for node in workflow.values()
                      if node.get("class_type") == "CheckpointLoaderSimple")
    controlnet = next(node for node in workflow.values()
                      if node.get("class_type") == "ControlNetLoader")
    union = next(node for node in workflow.values()
                 if node.get("class_type") == "SetUnionControlNetType")
    assert checkpoint["inputs"]["ckpt_name"] == "CyberRealisticPony_V18.0_F16.safetensors"
    assert controlnet["inputs"]["control_net_name"] == "xinsir-controlnet-union-sdxl-1.0.safetensors"
    assert union["inputs"]["type"] == union_type


def test_flux2_klein_edit_encodes_and_injects_reference_image():
    path = WORKFLOWS / "gen_image_flux2_klein_edit.json"
    workflow = templating.render_workflow_text(
        path.read_text(encoding="utf-8"), prompt="Preserve the composition",
        negative_prompt="", image_filename="reference.png",
        output_prefix="flux2/edit", width=960, height=540, seed=123,
    )
    assert workflow["reference_image"]["inputs"]["image"] == "reference.png"
    assert workflow["reference_encode"]["inputs"]["pixels"] == ["reference_scale", 0]
    assert workflow["reference"]["inputs"] == {
        "conditioning": ["positive", 0], "latent": ["reference_encode", 0]
    }
    assert workflow["guider"]["inputs"]["conditioning"] == ["reference", 0]
    assert workflow["latent"]["inputs"]["width"] == 960
    assert workflow["latent"]["inputs"]["height"] == 540

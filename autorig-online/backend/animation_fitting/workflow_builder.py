from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence, Tuple
from urllib.parse import quote

import httpx

from .comfy import canonical_workflow_bytes, workflow_fingerprint


BASE_CHECKPOINT = "ltx-2-19b-distilled-fp8.safetensors"
STATIC_CAMERA_LORA = "ltx-2-19b-lora-camera-control-static.safetensors"
API_WORKFLOW_NAMES = {
    "loop": "autorig_animal_loop_ltx2_19b_v1_api.json",
    "one_shot": "autorig_animal_oneshot_ltx2_19b_v1_api.json",
}
PRIMITIVE_NODE_TYPES = {
    "PrimitiveBoolean",
    "PrimitiveFloat",
    "PrimitiveInt",
    "PrimitiveString",
}
WIDGET_TYPES = {"BOOLEAN", "COMBO", "FLOAT", "INT", "STRING"}
CONNECTION_WILDCARDS = {"COMFY_MATCHTYPE_V3", "*"}


class WorkflowBuildError(RuntimeError):
    """Raised when a UI graph cannot be reproduced as a pinned API prompt."""


@dataclass(frozen=True)
class BuiltWorkflow:
    generation_mode: str
    workflow_name: str
    prompt: Mapping[str, Any]
    fingerprint: str
    source_sha256: str
    omitted_node_ids: Tuple[str, ...]
    widget_remainders: Mapping[str, Tuple[Any, ...]]


def load_ui_workflow(path: Path) -> Dict[str, Any]:
    source = Path(path).resolve()
    try:
        parsed = json.loads(source.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise WorkflowBuildError(f"Base LTX UI workflow is missing: {source}") from exc
    except json.JSONDecodeError as exc:
        raise WorkflowBuildError(f"Invalid base LTX UI workflow {source}: {exc}") from exc
    if not isinstance(parsed, dict) or not isinstance(parsed.get("nodes"), list):
        raise WorkflowBuildError(f"{source} is not a Comfy UI workflow")
    if not isinstance(parsed.get("links"), list):
        raise WorkflowBuildError(f"{source} has no links array")
    return parsed


def fetch_live_object_info(
    comfy_base_url: str,
    class_types: Iterable[str],
    *,
    client: Optional[httpx.Client] = None,
) -> Dict[str, Mapping[str, Any]]:
    base_url = str(comfy_base_url or "").rstrip("/")
    owns_client = client is None
    http_client = client or httpx.Client(timeout=30.0, follow_redirects=True)
    result: Dict[str, Mapping[str, Any]] = {}
    try:
        for class_type in sorted(set(class_types)):
            if class_type in PRIMITIVE_NODE_TYPES:
                continue
            response = http_client.get(f"{base_url}/object_info/{quote(class_type, safe='')}")
            response.raise_for_status()
            payload = response.json()
            info = payload.get(class_type) if isinstance(payload, dict) else None
            if isinstance(info, dict):
                result[class_type] = info
    finally:
        if owns_client:
            http_client.close()
    return result


def build_pinned_workflows(
    ui_workflow: Mapping[str, Any],
    object_info: Mapping[str, Mapping[str, Any]],
    *,
    source_sha256: str,
) -> Dict[str, BuiltWorkflow]:
    base_prompt, omitted, remainders = convert_active_ui_graph_to_api(ui_workflow, object_info)
    built: Dict[str, BuiltWorkflow] = {}
    for mode, workflow_name in API_WORKFLOW_NAMES.items():
        prompt = patch_animation_fitting_guides(base_prompt, mode)
        validate_api_prompt(prompt, object_info)
        _validate_ltx_19b_static_contract(prompt)
        built[mode] = BuiltWorkflow(
            generation_mode=mode,
            workflow_name=workflow_name,
            prompt=prompt,
            fingerprint=workflow_fingerprint(prompt),
            source_sha256=source_sha256,
            omitted_node_ids=omitted,
            widget_remainders=remainders,
        )
    return built


def convert_active_ui_graph_to_api(
    ui_workflow: Mapping[str, Any],
    object_info: Mapping[str, Mapping[str, Any]],
) -> tuple[Dict[str, Any], Tuple[str, ...], Mapping[str, Tuple[Any, ...]]]:
    raw_nodes = ui_workflow.get("nodes")
    raw_links = ui_workflow.get("links")
    if not isinstance(raw_nodes, list) or not isinstance(raw_links, list):
        raise WorkflowBuildError("Comfy UI workflow requires nodes and links arrays")
    nodes = {
        str(node.get("id")): copy.deepcopy(node)
        for node in raw_nodes
        if isinstance(node, dict) and node.get("id") is not None
    }
    links = {}
    for raw in raw_links:
        if not isinstance(raw, list) or len(raw) < 6:
            raise WorkflowBuildError(f"Invalid Comfy UI link: {raw!r}")
        links[str(raw[0])] = tuple(raw[:6])

    roots = []
    for node_id, node in nodes.items():
        if _node_mode(node) != 0:
            continue
        info = object_info.get(str(node.get("type") or ""))
        if isinstance(info, Mapping) and info.get("output_node") is True:
            roots.append(node_id)
    if not roots:
        raise WorkflowBuildError("The base UI graph has no active output node")

    reachable: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in reachable:
            return
        node = nodes.get(node_id)
        if not node:
            raise WorkflowBuildError(f"Link references missing UI node {node_id}")
        mode = _node_mode(node)
        node_type = str(node.get("type") or "")
        if node_type in PRIMITIVE_NODE_TYPES:
            return
        if mode not in (0, 4):
            raise WorkflowBuildError(f"Reachable UI node {node_id} uses unsupported mode {mode}")
        if mode == 0:
            if node_type not in object_info:
                raise WorkflowBuildError(
                    f"Reachable class_type {node_type!r} is absent from live /object_info"
                )
            reachable.add(node_id)
        for raw_input in node.get("inputs") or []:
            if not isinstance(raw_input, dict) or raw_input.get("link") is None:
                continue
            origin_id, _ = _link_origin(links, raw_input["link"])
            visit(origin_id)

    for root in roots:
        visit(root)

    prompt: Dict[str, Any] = {}
    widget_remainders: Dict[str, Tuple[Any, ...]] = {}
    for node_id in sorted(reachable, key=_node_sort_key):
        node = nodes[node_id]
        class_type = str(node.get("type") or "")
        api_inputs, remainder = _convert_node_inputs(node, nodes, links, object_info[class_type])
        title = str(node.get("title") or class_type).strip() or class_type
        prompt[node_id] = {
            "inputs": api_inputs,
            "class_type": class_type,
            "_meta": {"title": f"{title} [{node_id}]"},
        }
        if remainder:
            widget_remainders[node_id] = remainder

    omitted = tuple(sorted(set(nodes) - reachable, key=_node_sort_key))
    validate_api_prompt(prompt, object_info)
    return prompt, omitted, widget_remainders


def patch_animation_fitting_guides(
    base_prompt: Mapping[str, Any],
    generation_mode: str,
) -> Dict[str, Any]:
    mode = str(generation_mode or "").strip().lower()
    if mode not in API_WORKFLOW_NAMES:
        raise WorkflowBuildError(f"Unsupported generation mode: {generation_mode!r}")
    prompt = copy.deepcopy(dict(base_prompt))
    condition_id = _one_node_id(prompt, "LTXVConditioning")
    image_condition_id = _one_node_id(prompt, "LTXVImgToVideoConditionOnly")
    load_image_id = _one_node_id(prompt, "LoadImage")
    video_latent_id = _one_node_id(prompt, "EmptyLTXVLatentVideo")
    audio_latent_id = _one_node_id(prompt, "LTXVEmptyLatentAudio")
    separate_av_id = _one_node_id(prompt, "LTXVSeparateAVLatent")
    video_decode_id = _one_node_id(prompt, "LTXVTiledVAEDecode")
    create_video_id = _one_node_id(prompt, "CreateVideo")
    save_video_id = _one_node_id(prompt, "SaveVideo")

    image_condition = prompt[image_condition_id]["inputs"]
    positive_source = [condition_id, 0]
    negative_source = [condition_id, 1]
    latent_source = image_condition.get("latent")
    image_source = image_condition.get("image")
    vae_source = image_condition.get("vae")
    strength = float(image_condition.get("strength", 0.7))
    for label, value in (
        ("latent", latent_source),
        ("image", image_source),
        ("vae", vae_source),
    ):
        if not _is_reference(value):
            raise WorkflowBuildError(f"Base I2V node has no linked {label} input")

    start_guide_id = "900001"
    end_guide_id = "900002"
    crop_guides_id = "900003"
    if any(node_id in prompt for node_id in (start_guide_id, end_guide_id, crop_guides_id)):
        raise WorkflowBuildError("Reserved animation-fitting node id already exists")
    prompt[start_guide_id] = {
        "inputs": {
            "positive": positive_source,
            "negative": negative_source,
            "vae": vae_source,
            "latent": latent_source,
            "image": image_source,
            "frame_idx": 0,
            "strength": strength,
        },
        "class_type": "LTXVAddGuide",
        "_meta": {"title": "AUTORIG_START_GUIDE"},
    }
    final_guide_id = start_guide_id
    if mode == "loop":
        prompt[end_guide_id] = {
            "inputs": {
                "positive": [start_guide_id, 0],
                "negative": [start_guide_id, 1],
                "vae": vae_source,
                "latent": [start_guide_id, 2],
                "image": image_source,
                "frame_idx": -1,
                "strength": strength,
            },
            "class_type": "LTXVAddGuide",
            "_meta": {"title": "AUTORIG_END_GUIDE_N_MINUS_1"},
        }
        final_guide_id = end_guide_id

    video_decode_inputs = prompt[video_decode_id]["inputs"]
    if video_decode_inputs.get("latents") != [separate_av_id, 0]:
        raise WorkflowBuildError(
            "Pinned video decoder must consume the video output of LTXVSeparateAVLatent"
        )
    prompt[crop_guides_id] = {
        "inputs": {
            "positive": [final_guide_id, 0],
            "negative": [final_guide_id, 1],
            "latent": [separate_av_id, 0],
        },
        "class_type": "LTXVCropGuides",
        "_meta": {"title": "AUTORIG_CROP_GUIDE_LATENTS"},
    }
    video_decode_inputs["latents"] = [crop_guides_id, 2]

    del prompt[image_condition_id]
    replacements = {
        (condition_id, 0): [final_guide_id, 0],
        (condition_id, 1): [final_guide_id, 1],
        (image_condition_id, 0): [final_guide_id, 2],
    }
    for node_id, node in prompt.items():
        if node_id in (start_guide_id, end_guide_id):
            continue
        for input_name, value in list(node["inputs"].items()):
            if _is_reference(value):
                replacement = replacements.get((str(value[0]), int(value[1])))
                if replacement:
                    node["inputs"][input_name] = replacement

    positive_id = str(prompt[condition_id]["inputs"]["positive"][0])
    negative_id = str(prompt[condition_id]["inputs"]["negative"][0])
    random_noise_ids = _node_ids(prompt, "RandomNoise")
    if len(random_noise_ids) != 1:
        raise WorkflowBuildError(
            f"Pinned active branch requires exactly one RandomNoise, found {random_noise_ids}"
        )

    _set_title(prompt, load_image_id, "AUTORIG_START_FRAME")
    _set_title(prompt, positive_id, "AUTORIG_POSITIVE_PROMPT")
    _set_title(prompt, negative_id, "AUTORIG_NEGATIVE_PROMPT")
    _set_title(prompt, video_latent_id, "AUTORIG_VIDEO_LATENT")
    _set_title(prompt, audio_latent_id, "AUTORIG_AUDIO_LATENT")
    _set_title(prompt, condition_id, "AUTORIG_INPUT_CONDITIONING")
    _set_title(prompt, random_noise_ids[0], "AUTORIG_SEED")
    _set_title(prompt, create_video_id, "AUTORIG_OUTPUT_VIDEO")
    _set_title(prompt, save_video_id, "AUTORIG_OUTPUT")

    if mode == "one_shot":
        prompt[positive_id]["inputs"]["text"] = (
            "A quiet cinematic image-to-video shot based on the reference image. "
            "The main subject performs one complete non-looping action and ends in "
            "its final pose. The camera remains completely static in place, with no "
            "pan, no tilt, no dolly, no zoom, and no scene cuts."
        )
        prompt[negative_id]["inputs"]["text"] = (
            "camera movement, pan, tilt, dolly, zoom, handheld shake, scene cut, "
            "loop, return to start, cartoon, low quality, distorted details"
        )

    default_frame_count = 97 if mode == "loop" else 65
    prompt[video_latent_id]["inputs"].update(
        {
            "width": 384,
            "height": 224,
            "length": default_frame_count,
        }
    )
    prompt[audio_latent_id]["inputs"]["frames_number"] = default_frame_count
    prompt[condition_id]["inputs"]["frame_rate"] = 24.0
    prompt[audio_latent_id]["inputs"]["frame_rate"] = 24
    prompt[create_video_id]["inputs"]["fps"] = 30.0
    prompt[save_video_id]["inputs"].update(
        {
            "filename_prefix": (
                "animation_fitting/unbound/candidate_ltx2_19b_v1"
                if mode == "loop"
                else "animation_fitting/unbound/candidate_oneshot_ltx2_19b_v1"
            ),
            "format": "mp4",
            "codec": "h264",
        }
    )
    return _prune_api_prompt(prompt)


def validate_api_prompt(
    prompt: Mapping[str, Any],
    object_info: Mapping[str, Mapping[str, Any]],
) -> None:
    if not isinstance(prompt, Mapping) or not prompt:
        raise WorkflowBuildError("API prompt must be a non-empty object")
    output_count = 0
    titles: set[str] = set()
    for raw_node_id, raw_node in prompt.items():
        node_id = str(raw_node_id)
        if not isinstance(raw_node, Mapping):
            raise WorkflowBuildError(f"API node {node_id} must be an object")
        class_type = str(raw_node.get("class_type") or "")
        info = object_info.get(class_type)
        if not isinstance(info, Mapping):
            raise WorkflowBuildError(f"API class_type {class_type!r} is absent from live /object_info")
        inputs = raw_node.get("inputs")
        if not isinstance(inputs, Mapping):
            raise WorkflowBuildError(f"API node {node_id} has no inputs object")
        specs = _expanded_input_specs(info, inputs)
        required = set((info.get("input") or {}).get("required") or {})
        missing = sorted(required - set(inputs))
        if missing:
            raise WorkflowBuildError(f"API node {node_id} {class_type} misses required inputs {missing}")
        unknown = sorted(set(inputs) - set(specs))
        if unknown:
            raise WorkflowBuildError(f"API node {node_id} {class_type} has unknown inputs {unknown}")
        for input_name, value in inputs.items():
            spec = specs[input_name]
            if _is_reference(value):
                source_id = str(value[0])
                source = prompt.get(source_id)
                if not isinstance(source, Mapping):
                    raise WorkflowBuildError(
                        f"API node {node_id}.{input_name} references missing node {source_id}"
                    )
                source_info = object_info.get(str(source.get("class_type") or "")) or {}
                outputs = source_info.get("output") or []
                slot = int(value[1])
                if slot < 0 or slot >= len(outputs):
                    raise WorkflowBuildError(
                        f"API node {node_id}.{input_name} references invalid output {source_id}:{slot}"
                    )
                _validate_connection_type(
                    str(outputs[slot]), _spec_type(spec), f"{source_id}:{slot} -> {node_id}.{input_name}"
                )
            else:
                _validate_literal(
                    value,
                    spec,
                    f"{node_id}.{input_name}",
                    allow_unlisted_combo=class_type == "LoadImage" and input_name == "image",
                )
        title = str((raw_node.get("_meta") or {}).get("title") or "").strip()
        if title:
            if title in titles:
                raise WorkflowBuildError(f"Duplicate API node title: {title}")
            titles.add(title)
        if info.get("output_node") is True:
            output_count += 1
    if output_count != 1:
        raise WorkflowBuildError(f"Pinned workflow requires one output node, found {output_count}")


def write_built_workflows(
    built: Mapping[str, BuiltWorkflow],
    output_dir: Path,
) -> Path:
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    manifest_rows = []
    for mode in ("loop", "one_shot"):
        item = built[mode]
        path = destination / item.workflow_name
        encoded = (
            json.dumps(item.prompt, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        path.write_bytes(encoded)
        manifest_rows.append(
            {
                "generation_mode_string": mode,
                "workflow_name_string": item.workflow_name,
                "workflow_fingerprint_sha256_string": item.fingerprint,
                "canonical_size_bytes_int": len(canonical_workflow_bytes(item.prompt)),
                "node_count_int": len(item.prompt),
            }
        )
    first = built["loop"]
    manifest = {
        "schema": "autorig.animation-fitting-built-workflows.v1",
        "source_workflow_sha256_string": first.source_sha256,
        "base_checkpoint_string": BASE_CHECKPOINT,
        "static_camera_lora_string": STATIC_CAMERA_LORA,
        "active_graph_rule_string": "reachable mode-0 nodes from the sole active output; mode-4 branches are omitted",
        "omitted_ui_node_ids_array": list(first.omitted_node_ids),
        "ignored_ui_widget_remainders_object": {
            key: list(value) for key, value in first.widget_remainders.items()
        },
        "workflows_array": manifest_rows,
    }
    manifest_path = destination / "workflow_manifest.v1.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def install_api_workflow(
    comfy_base_url: str,
    workflow_name: str,
    prompt: Mapping[str, Any],
    *,
    overwrite: bool = False,
    client: Optional[httpx.Client] = None,
) -> str:
    base_url = str(comfy_base_url or "").rstrip("/")
    encoded_name = quote(f"workflows/{workflow_name}", safe="")
    endpoint = f"{base_url}/api/userdata/{encoded_name}"
    owns_client = client is None
    http_client = client or httpx.Client(timeout=30.0, follow_redirects=True)
    expected = workflow_fingerprint(prompt)
    try:
        current = http_client.get(endpoint)
        if current.status_code == 200:
            try:
                current_payload = current.json()
            except json.JSONDecodeError as exc:
                raise WorkflowBuildError(f"Installed workflow {workflow_name} is not JSON") from exc
            current_fingerprint = workflow_fingerprint(current_payload)
            if current_fingerprint == expected:
                return expected
            if not overwrite:
                raise WorkflowBuildError(
                    f"Installed workflow {workflow_name} differs ({current_fingerprint}); use --overwrite"
                )
        elif current.status_code != 404:
            current.raise_for_status()
        response = http_client.post(
            endpoint,
            params={"overwrite": "true" if overwrite else "false"},
            content=json.dumps(prompt, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        verify = http_client.get(endpoint)
        verify.raise_for_status()
        installed = verify.json()
        installed_fingerprint = workflow_fingerprint(installed)
        if installed_fingerprint != expected:
            raise WorkflowBuildError(
                f"Installed workflow fingerprint mismatch: {installed_fingerprint} != {expected}"
            )
        return installed_fingerprint
    finally:
        if owns_client:
            http_client.close()


def _convert_node_inputs(
    node: Mapping[str, Any],
    nodes: Mapping[str, Mapping[str, Any]],
    links: Mapping[str, Sequence[Any]],
    info: Mapping[str, Any],
) -> tuple[Dict[str, Any], Tuple[Any, ...]]:
    widgets = list(node.get("widgets_values") or [])
    widget_index = 0
    ui_inputs = {
        str(item.get("name")): item
        for item in node.get("inputs") or []
        if isinstance(item, dict) and item.get("name") is not None
    }
    result: Dict[str, Any] = {}
    info_inputs = info.get("input") or {}
    input_order = info.get("input_order") or {}

    for section in ("required", "optional"):
        section_specs = info_inputs.get(section) or {}
        names = input_order.get(section) or list(section_specs)
        for name in names:
            spec = section_specs.get(name)
            if spec is None:
                continue
            ui_input = ui_inputs.get(str(name))
            linked = ui_input is not None and ui_input.get("link") is not None
            if _is_widget_spec(spec):
                value, widget_index = _consume_widget_value(
                    widgets, widget_index, spec, required=section == "required", label=f"{node.get('id')}.{name}"
                )
                if linked:
                    result[str(name)] = _resolve_ui_link(nodes, links, ui_input["link"])
                elif value is not _MISSING:
                    result[str(name)] = value
                if _spec_type(spec) == "COMFY_DYNAMICCOMBO_V3" and value is not _MISSING:
                    nested_specs = _dynamic_option_specs(spec, value)
                    for nested_name, nested_spec in nested_specs.items():
                        nested_value, widget_index = _consume_widget_value(
                            widgets,
                            widget_index,
                            nested_spec,
                            required=True,
                            label=f"{node.get('id')}.{nested_name}",
                        )
                        if nested_value is not _MISSING:
                            result[f"{name}.{nested_name}"] = nested_value
            elif linked:
                result[str(name)] = _resolve_ui_link(nodes, links, ui_input["link"])
            elif section == "required":
                raise WorkflowBuildError(
                    f"Required connection {node.get('id')}.{name} is not linked in the active graph"
                )
    return result, tuple(widgets[widget_index:])


def _resolve_ui_link(
    nodes: Mapping[str, Mapping[str, Any]],
    links: Mapping[str, Sequence[Any]],
    link_id: object,
) -> Any:
    origin_id, origin_slot = _link_origin(links, link_id)
    origin = nodes.get(origin_id)
    if not origin:
        raise WorkflowBuildError(f"Link {link_id} references missing origin node {origin_id}")
    node_type = str(origin.get("type") or "")
    if node_type in PRIMITIVE_NODE_TYPES:
        widgets = list(origin.get("widgets_values") or [])
        if not widgets:
            raise WorkflowBuildError(f"Primitive node {origin_id} has no value")
        return widgets[0]
    mode = _node_mode(origin)
    if mode == 4:
        linked_inputs = [
            item
            for item in origin.get("inputs") or []
            if isinstance(item, dict) and item.get("link") is not None
        ]
        if not linked_inputs:
            raise WorkflowBuildError(f"Bypassed node {origin_id} has no linked input")
        output_type = ""
        outputs = origin.get("outputs") or []
        if 0 <= origin_slot < len(outputs) and isinstance(outputs[origin_slot], dict):
            output_type = str(outputs[origin_slot].get("type") or "")
        compatible = [
            item for item in linked_inputs if _connection_types_compatible(str(item.get("type") or ""), output_type)
        ]
        selected = compatible[0] if len(compatible) == 1 else linked_inputs[0]
        return _resolve_ui_link(nodes, links, selected["link"])
    if mode != 0:
        raise WorkflowBuildError(f"Link {link_id} resolves through unsupported node mode {mode}")
    return [origin_id, origin_slot]


def _link_origin(
    links: Mapping[str, Sequence[Any]],
    link_id: object,
) -> tuple[str, int]:
    link = links.get(str(link_id))
    if not link or len(link) < 3:
        raise WorkflowBuildError(f"Missing UI link {link_id}")
    return str(link[1]), int(link[2])


def _expanded_input_specs(
    info: Mapping[str, Any], inputs: Mapping[str, Any]
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    info_inputs = info.get("input") or {}
    for section in ("required", "optional", "hidden"):
        section_specs = info_inputs.get(section) or {}
        result.update(section_specs)
        for name, spec in section_specs.items():
            if _spec_type(spec) == "COMFY_DYNAMICCOMBO_V3" and name in inputs:
                result.update(
                    {
                        f"{name}.{nested_name}": nested_spec
                        for nested_name, nested_spec in _dynamic_option_specs(
                            spec, inputs[name]
                        ).items()
                    }
                )
    return result


def _dynamic_option_specs(spec: Any, selected: object) -> Dict[str, Any]:
    metadata = _spec_metadata(spec)
    for option in metadata.get("options") or []:
        if isinstance(option, dict) and str(option.get("key")) == str(selected):
            inputs = option.get("inputs") or {}
            result: Dict[str, Any] = {}
            for section in ("required", "optional"):
                values = inputs.get(section) or {}
                if isinstance(values, dict):
                    result.update(values)
            return result
    raise WorkflowBuildError(f"Unknown dynamic combo option {selected!r}")


class _Missing:
    pass


_MISSING = _Missing()


def _consume_widget_value(
    widgets: Sequence[Any],
    index: int,
    spec: Any,
    *,
    required: bool,
    label: str,
) -> tuple[Any, int]:
    if index < len(widgets):
        return widgets[index], index + 1
    metadata = _spec_metadata(spec)
    if "default" in metadata:
        return metadata["default"], index
    if required:
        raise WorkflowBuildError(f"UI widget value is missing for {label}")
    return _MISSING, index


def _is_widget_spec(spec: Any) -> bool:
    kind = _spec_type(spec)
    return isinstance(_spec_head(spec), list) or kind in WIDGET_TYPES or kind == "COMFY_DYNAMICCOMBO_V3"


def _spec_head(spec: Any) -> Any:
    return spec[0] if isinstance(spec, list) and spec else None


def _spec_type(spec: Any) -> str:
    head = _spec_head(spec)
    return "COMBO" if isinstance(head, list) else str(head or "")


def _spec_metadata(spec: Any) -> Mapping[str, Any]:
    return spec[1] if isinstance(spec, list) and len(spec) > 1 and isinstance(spec[1], dict) else {}


def _validate_literal(
    value: object,
    spec: Any,
    label: str,
    *,
    allow_unlisted_combo: bool = False,
) -> None:
    kind = _spec_type(spec)
    head = _spec_head(spec)
    metadata = _spec_metadata(spec)
    if isinstance(head, list) and value not in head and not allow_unlisted_combo:
        raise WorkflowBuildError(f"{label} value {value!r} is not in live combo options")
    if kind == "COMBO":
        options = metadata.get("options")
        if isinstance(options, list) and value not in options and not allow_unlisted_combo:
            raise WorkflowBuildError(f"{label} value {value!r} is not in live combo options")
    elif kind == "INT" and (isinstance(value, bool) or not isinstance(value, int)):
        raise WorkflowBuildError(f"{label} must be INT, got {value!r}")
    elif kind == "FLOAT" and (isinstance(value, bool) or not isinstance(value, (int, float))):
        raise WorkflowBuildError(f"{label} must be FLOAT, got {value!r}")
    elif kind == "BOOLEAN" and not isinstance(value, bool):
        raise WorkflowBuildError(f"{label} must be BOOLEAN, got {value!r}")
    elif kind == "STRING" and not isinstance(value, str):
        raise WorkflowBuildError(f"{label} must be STRING, got {value!r}")


def _validate_connection_type(source: str, target: str, label: str) -> None:
    if not _connection_types_compatible(source, target):
        raise WorkflowBuildError(f"Incompatible live connection {label}: {source} -> {target}")


def _connection_types_compatible(source: str, target: str) -> bool:
    if source == target or source in CONNECTION_WILDCARDS or target in CONNECTION_WILDCARDS:
        return True
    return source in {part.strip() for part in target.split(",")}


def _validate_ltx_19b_static_contract(prompt: Mapping[str, Any]) -> None:
    checkpoint_id = _one_node_id(prompt, "CheckpointLoaderSimple")
    checkpoint = prompt[checkpoint_id]["inputs"].get("ckpt_name")
    if checkpoint != BASE_CHECKPOINT:
        raise WorkflowBuildError(f"Pinned workflow changed the 19B checkpoint: {checkpoint!r}")
    lora_id = _one_node_id(prompt, "LoraLoaderModelOnly")
    lora_inputs = prompt[lora_id]["inputs"]
    if lora_inputs.get("lora_name") != STATIC_CAMERA_LORA:
        raise WorkflowBuildError(f"Pinned workflow changed the static camera LoRA: {lora_inputs!r}")
    if float(lora_inputs.get("strength_model", 0.0)) != 1.0:
        raise WorkflowBuildError("Pinned static camera LoRA strength must stay 1.0")
    for required_type in (
        "KSamplerSelect",
        "ManualSigmas",
        "SamplerCustomAdvanced",
        "LTXVAddGuide",
        "LTXVCropGuides",
        "SaveVideo",
    ):
        if not _node_ids(prompt, required_type):
            raise WorkflowBuildError(f"Pinned workflow misses active {required_type}")
    save_id = _one_node_id(prompt, "SaveVideo")
    save_inputs = prompt[save_id]["inputs"]
    if save_inputs.get("format") != "mp4" or save_inputs.get("codec") != "h264":
        raise WorkflowBuildError("Pinned output must be MP4/H.264")


def _prune_api_prompt(prompt: Mapping[str, Any]) -> Dict[str, Any]:
    roots = [
        str(node_id)
        for node_id, node in prompt.items()
        if isinstance(node, Mapping) and node.get("class_type") == "SaveVideo"
    ]
    if len(roots) != 1:
        raise WorkflowBuildError(f"Expected one SaveVideo root before pruning, found {roots}")
    reachable: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in reachable:
            return
        node = prompt.get(node_id)
        if not isinstance(node, Mapping):
            raise WorkflowBuildError(f"API prompt references missing node {node_id}")
        reachable.add(node_id)
        for value in (node.get("inputs") or {}).values():
            if _is_reference(value):
                visit(str(value[0]))

    visit(roots[0])
    return {
        node_id: copy.deepcopy(prompt[node_id])
        for node_id in sorted(reachable, key=_node_sort_key)
    }


def _node_mode(node: Mapping[str, Any]) -> int:
    return int(node.get("mode", 0) or 0)


def _node_ids(prompt: Mapping[str, Any], class_type: str) -> Tuple[str, ...]:
    return tuple(
        str(node_id)
        for node_id, node in prompt.items()
        if isinstance(node, Mapping) and node.get("class_type") == class_type
    )


def _one_node_id(prompt: Mapping[str, Any], class_type: str) -> str:
    found = _node_ids(prompt, class_type)
    if len(found) != 1:
        raise WorkflowBuildError(f"Expected one {class_type}, found {found}")
    return found[0]


def _set_title(prompt: MutableMapping[str, Any], node_id: str, title: str) -> None:
    prompt[node_id]["_meta"] = {"title": title}


def _is_reference(value: object) -> bool:
    return (
        isinstance(value, list)
        and len(value) == 2
        and isinstance(value[0], str)
        and isinstance(value[1], int)
    )


def _node_sort_key(value: object) -> tuple[int, object]:
    token = str(value)
    return (0, int(token)) if re.fullmatch(r"[0-9]+", token) else (1, token)


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build pinned AutoRig LTX 19B animation-fitting API workflows from a Comfy UI graph."
    )
    parser.add_argument("--base", required=True, type=Path, help="Source Comfy UI workflow JSON")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "specs" / "workflows",
    )
    parser.add_argument("--comfy-url", default="http://127.0.0.1:8188")
    parser.add_argument("--install", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    source_path = args.base.resolve()
    source_bytes = source_path.read_bytes()
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    ui_workflow = load_ui_workflow(source_path)
    class_types = {
        str(node.get("type") or "")
        for node in ui_workflow.get("nodes") or []
        if isinstance(node, dict)
    }
    class_types.add("LTXVAddGuide")
    class_types.add("LTXVCropGuides")
    object_info = fetch_live_object_info(args.comfy_url, class_types)
    built = build_pinned_workflows(
        ui_workflow,
        object_info,
        source_sha256=source_sha256,
    )
    manifest_path = write_built_workflows(built, args.output_dir)
    if args.install:
        for mode in ("loop", "one_shot"):
            item = built[mode]
            installed = install_api_workflow(
                args.comfy_url,
                item.workflow_name,
                item.prompt,
                overwrite=args.overwrite,
            )
            if installed != item.fingerprint:
                raise WorkflowBuildError(f"Unexpected installed fingerprint for {item.workflow_name}")
    result = {
        "ok_bool": True,
        "source_workflow_string": str(source_path),
        "source_sha256_string": source_sha256,
        "manifest_path_string": str(manifest_path),
        "installed_bool": bool(args.install),
        "workflows_object": {
            mode: {
                "workflow_name_string": item.workflow_name,
                "workflow_fingerprint_sha256_string": item.fingerprint,
                "node_count_int": len(item.prompt),
            }
            for mode, item in built.items()
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

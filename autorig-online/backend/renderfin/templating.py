"""Workflow text templating ported from C# Adapter_Comfy.cs.

Workflow files are TEXT templates with $placeholders; some (gen_image.json) are
not valid JSON until numeric placeholders are substituted. Substitution order and
quirks mirror the C# server: JSON-escaped prompts, the glasses/glass strip, seed
randomization, and the post-parse normalization pass.
"""
from __future__ import annotations

import json
import random
import re
from typing import Any, Dict, Optional, Tuple

SEED_MAX = 574131870028331  # C# random seed upper bound
_GLASS_RE = re.compile(r"\bglass(?:es)?\b", re.IGNORECASE)
_SEED_KEYS = ("noise_seed", "seed")
MAX_PROMPT_CHARS = 12000
# The one ESRGAN model every FLUX image box carries. A template that asks for
# an upscaler must still parse when nothing was chosen.
DEFAULT_UPSCALE_MODEL = "4x_NMKD-Siax_200k.pth"
# Every clip this product delivers is 24 fps, so `frame_count` frames are
# `frame_count / 24` seconds of video wherever they were rendered. The LTXV
# 0.9.8 template shipped with the 25 the C# exporter wrote into it, which made
# the same 97 frames arrive 4% short of the length the caller asked for.
VIDEO_FPS = 24


def sanitize_prompt(text: str) -> str:
    """Preserve visual attributes and action instructions, with a bounded size."""
    text = text or ""
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text[:MAX_PROMPT_CHARS].strip()


def _ltxv_frames(frames: int, default: int = 121) -> int:
    """LTXV samplers accept 8*k+1 frames; 0 means "keep the template default"."""
    count = int(frames or 0)
    if count <= 0:
        return default
    return max(1, round((count - 1) / 8)) * 8 + 1


def _video_fps(fps: object, default: int = VIDEO_FPS) -> int:
    """The delivered frame rate; 0 or nonsense means the product's own 24."""
    try:
        rate = int(fps or 0)
    except (TypeError, ValueError):
        return default
    return rate if 1 <= rate <= 120 else default


def _json_escape(text: str) -> str:
    return json.dumps(text or "", ensure_ascii=False)[1:-1]


def render_workflow_text(
    template_text: str,
    *,
    width: int,
    height: int,
    prompt: str,
    negative_prompt: str,
    image_filename: str,
    image_end_filename: str = "",
    control_video_filename: str = "",
    output_prefix: str,
    workflow_type: str = "",
    frames: int = 0,
    fps: int = VIDEO_FPS,
    randomize_seeds: bool = True,
    seed: Optional[int] = None,
    checkpoint: str = "",
    lora: str = "",
    lora_strength: Optional[float] = None,
    pose_prompt: str = "",
    upscale_model: str = "",
) -> Dict[str, Any]:
    """Substitute placeholders, parse, normalize. Returns the workflow dict
    ready for POST /prompt."""
    text = template_text
    text = text.replace("$frames", str(_ltxv_frames(frames)))
    # Frame rate and frame count are one contract: a template that states its
    # own rate turns a requested length into a different number of seconds.
    text = text.replace("$fps", str(_video_fps(fps)))
    # The longer edge, for workflows that scale by it rather than by a
    # width and a height. Without this the template keeps a bare token
    # and does not parse.
    text = text.replace("$long_side", str(int(max(width, height))))
    text = text.replace("$width", str(int(width)))
    text = text.replace("$height", str(int(height)))
    text = text.replace(
        "$upscale_model",
        _json_escape((upscale_model or "").strip() or DEFAULT_UPSCALE_MODEL),
    )
    text = text.replace("$prompt", _json_escape(sanitize_prompt(prompt)))
    text = text.replace("$pose_prompt", _json_escape(sanitize_prompt(pose_prompt)))
    text = text.replace("$negative_prompt", _json_escape(sanitize_prompt(negative_prompt)))
    # $image_end must go first: "$image" is a prefix of it.
    text = text.replace("$image_end", _json_escape(image_end_filename or ""))
    text = text.replace("$control_video", _json_escape(control_video_filename or ""))
    text = text.replace("$image", _json_escape(image_filename or ""))
    # $output_url must go last: it is a prefix of $output_url_Isolated etc.
    text = text.replace("$output_url", _json_escape(output_prefix or ""))

    try:
        workflow = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"workflow template did not parse after substitution: {exc}") from exc

    if randomize_seeds:
        _randomize_seeds(workflow, seed)
    _normalize_workflow(workflow, width=width, height=height, workflow_type=workflow_type)
    if not (image_end_filename or "").strip():
        _prune_optional_guide(workflow)
    # Last, so a chosen model is not undone by normalisation or pruning.
    apply_model_choice(workflow, checkpoint=checkpoint, lora=lora,
                       lora_strength=lora_strength)
    return workflow


def _randomize_seeds(workflow: Dict[str, Any], seed: Optional[int] = None) -> None:
    """C# regex-replaces "noise_seed": N with a random long. Post-parse we also
    cover plain "seed" inputs (KSampler in image_to_3d.json)."""
    for node in workflow.values():
        if not isinstance(node, dict):
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue
        for key in _SEED_KEYS:
            if key in inputs and isinstance(inputs[key], (int, float)) and not isinstance(inputs[key], bool):
                inputs[key] = int(seed) if seed else random.randint(1, SEED_MAX)


def _normalize_workflow(
    workflow: Dict[str, Any], *, width: int, height: int, workflow_type: str
) -> None:
    """Port of Adapter_Comfy.NormalizeWorkflowForRuntime."""
    is_t_pose = (workflow_type or "").strip().lower() in ("t_pose", "t_poses")
    for node in workflow.values():
        if not isinstance(node, dict):
            continue
        class_type = node.get("class_type")
        inputs = node.get("inputs")
        if class_type == "HelperNodes_WidthHeight" and isinstance(inputs, dict):
            inputs["width"] = int(width)
            inputs["height"] = int(height)
        elif is_t_pose and class_type == "VAEDecodeTiled_TiledDiffusion" and isinstance(inputs, dict):
            # Tiled VAE decode is broken on the t_pose pipeline; rewrite in place
            # to a plain VAEDecode keeping only its samples/vae inputs.
            node["class_type"] = "VAEDecode"
            node["inputs"] = {
                "samples": inputs.get("samples"),
                "vae": inputs.get("vae"),
            }
            meta = node.get("_meta")
            if isinstance(meta, dict):
                meta["title"] = "VAE Decode"


OPTIONAL_GUIDE = "AIGent optional end-frame guide"


def _prune_optional_guide(workflow):
    """Drop the end-frame branch when no second image was given.

    The farm only schedules workflow names a node advertises, so the frame-to-frame guide lives
    inside the normal template. Without an end image its LoadImage would fail, and the whole
    chain is removed instead: consumers fall back to the guide it was chained from.
    """
    target = next((nid for nid, node in workflow.items()
                   if isinstance(node, dict) and (node.get("_meta") or {}).get("title") == OPTIONAL_GUIDE), None)
    if target is None:
        return
    inputs = workflow[target]["inputs"]
    fallback = next((inputs[key] for key in ("image1", "positive", "image") if isinstance(inputs.get(key), list)), None)
    if not fallback:
        return
    source = fallback[0]
    for node in workflow.values():
        if not isinstance(node, dict):
            continue
        for key, value in list((node.get("inputs") or {}).items()):
            if isinstance(value, list) and value and value[0] == target:
                node["inputs"][key] = [source, value[1]]
    workflow.pop(target, None)
    _restore_single_anchor(workflow)
    # Remove what only fed the dropped guide, walking back until nothing orphaned is left.
    while True:
        referenced = {value[0] for node in workflow.values() if isinstance(node, dict)
                      for value in (node.get("inputs") or {}).values()
                      if isinstance(value, list) and value}
        orphans = [nid for nid, node in workflow.items()
                   if isinstance(node, dict) and nid not in referenced
                   and (node.get("_meta") or {}).get("title", "").endswith("end frame")]
        if not orphans:
            return
        for nid in orphans:
            workflow.pop(nid, None)
        _restore_single_anchor(workflow)


def _restore_single_anchor(workflow):
    """With the end frame gone the sampler must condition on frame 0 only."""
    for node in workflow.values():
        if not isinstance(node, dict):
            continue
        indices = (node.get("inputs") or {}).get("optional_cond_indices")
        if isinstance(indices, str) and "-1" in indices:
            kept = [part.strip() for part in indices.split(",") if part.strip() not in ("-1", "")]
            node["inputs"]["optional_cond_indices"] = ",".join(kept) or "0"


# Which input on which loader names the model file. Keyed by class_type so a
# workflow can be re-saved with different node ids without breaking this.
CHECKPOINT_SLOTS = {
    "CheckpointLoaderSimple": "ckpt_name",
    "UNETLoader": "unet_name",
    "ImageOnlyCheckpointLoader": "ckpt_name",
    # ComfyUI-GGUF keeps its own loader rather than teaching UNETLoader about
    # .gguf, so a quantised model chosen on the node reaches nothing unless
    # that loader is named here too.
    "UnetLoaderGGUF": "unet_name",
}
LORA_SLOTS = {
    "LoraLoaderModelOnly": "lora_name",
    "LoraLoader": "lora_name",
}
POWER_LORA_LOADER = "Power Lora Loader (rgthree)"


def apply_model_choice(
    workflow: Dict[str, Any],
    *,
    checkpoint: str = "",
    lora: str = "",
    lora_strength: Optional[float] = None,
) -> Dict[str, list]:
    """Point the workflow's loaders at the chosen files.

    Done after parsing rather than by substituting a placeholder, because the
    templates were exported from ComfyUI with real file names already in them
    and adding placeholders to every one of them would be a bigger change than
    this. Returns what was actually swapped so a caller can tell the difference
    between "not asked for" and "asked for but this workflow has no such node".
    """
    changed: Dict[str, list] = {"checkpoint": [], "lora": []}
    if not checkpoint and not lora and lora_strength is None:
        return changed

    for node_id, node in list(workflow.items()):
        if not isinstance(node, dict):
            continue
        class_type = str(node.get("class_type") or "")
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            continue

        if checkpoint and class_type in CHECKPOINT_SLOTS:
            slot = CHECKPOINT_SLOTS[class_type]
            if slot in inputs:
                inputs[slot] = checkpoint
                changed["checkpoint"].append(node_id)

        if lora and class_type in LORA_SLOTS:
            slot = LORA_SLOTS[class_type]
            if slot in inputs:
                inputs[slot] = lora
                if lora_strength is not None and "strength_model" in inputs:
                    inputs["strength_model"] = float(lora_strength)
                changed["lora"].append(node_id)

        if class_type == POWER_LORA_LOADER:
            # rgthree keeps each LoRA as its own dict rather than a flat input.
            # Only the first slot is steered; the rest of the stack the workflow
            # ships with is left alone.
            entry = inputs.get("lora_1")
            if isinstance(entry, dict):
                if lora:
                    entry["lora"] = lora
                    entry["on"] = True
                    changed["lora"].append(node_id)
                if lora_strength is not None:
                    entry["strength"] = float(lora_strength)
    if lora and not changed["lora"]:
        checkpoint_node = next((node_id for node_id, node in workflow.items()
                                if isinstance(node, dict)
                                and node.get("class_type") == "CheckpointLoaderSimple"), None)
        if checkpoint_node is not None:
            node_id = "autorig_selected_lora"
            while node_id in workflow:
                node_id += "_"
            strength = float(lora_strength) if lora_strength is not None else 1.0
            workflow[node_id] = {
                "class_type": "LoraLoader",
                "inputs": {"lora_name": lora, "strength_model": strength,
                           "strength_clip": strength, "model": [checkpoint_node, 0],
                           "clip": [checkpoint_node, 1]},
            }
            for target_id, target in workflow.items():
                if target_id == node_id or not isinstance(target, dict):
                    continue
                inputs = target.get("inputs")
                if not isinstance(inputs, dict):
                    continue
                for key, value in list(inputs.items()):
                    if value == [checkpoint_node, 0]:
                        inputs[key] = [node_id, 0]
                    elif value == [checkpoint_node, 1]:
                        inputs[key] = [node_id, 1]
            changed["lora"].append(node_id)
    return changed


def workflow_placeholders(template_text: str) -> Tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"\$[a-z_]+", template_text))))

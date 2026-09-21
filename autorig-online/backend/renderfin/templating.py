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
MAX_PROMPT_CHARS = 2000


def sanitize_prompt(text: str) -> str:
    """Strip the words glasses/glass (C# parity) and clamp length."""
    text = _GLASS_RE.sub("", text or "")
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text[:MAX_PROMPT_CHARS].strip()


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
    output_prefix: str,
    workflow_type: str = "",
    randomize_seeds: bool = True,
    seed: Optional[int] = None,
    checkpoint: str = "",
    lora: str = "",
    lora_strength: Optional[float] = None,
) -> Dict[str, Any]:
    """Substitute placeholders, parse, normalize. Returns the workflow dict
    ready for POST /prompt."""
    text = template_text
    # The longer edge, for workflows that scale by it rather than by a
    # width and a height. Without this the template keeps a bare token
    # and does not parse.
    text = text.replace("$long_side", str(int(max(width, height))))
    text = text.replace("$width", str(int(width)))
    text = text.replace("$height", str(int(height)))
    text = text.replace("$prompt", _json_escape(sanitize_prompt(prompt)))
    text = text.replace("$negative_prompt", _json_escape(sanitize_prompt(negative_prompt)))
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


# Which input on which loader names the model file. Keyed by class_type so a
# workflow can be re-saved with different node ids without breaking this.
CHECKPOINT_SLOTS = {
    "CheckpointLoaderSimple": "ckpt_name",
    "UNETLoader": "unet_name",
    "ImageOnlyCheckpointLoader": "ckpt_name",
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

    for node_id, node in workflow.items():
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
    return changed


def workflow_placeholders(template_text: str) -> Tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"\$[a-z_]+", template_text))))

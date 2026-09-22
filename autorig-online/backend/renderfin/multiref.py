"""Several input pictures composed into one: the multi-reference image edit.

A person and an outfit, a character and a scene, a product and a model photo.
Two installed models take more than one picture natively in ComfyUI 0.37, and
they take them in different ways:

* FLUX.2 klein 4B chains ``ReferenceLatent`` nodes on its conditioning, one per
  picture, in order. Nothing in the model caps the count; four is where the
  8 GB boxes still render in a few minutes and where BFL's own examples stop
  improving, so four is the ceiling here.
* Qwen-Image-Edit-2511 reads up to three pictures through the ``image1`` ..
  ``image3`` sockets of ``TextEncodeQwenImageEditPlus``. That is the node's own
  limit, not a choice.

The order is the contract. A prompt says "the person from image 1 wearing the
jacket from image 2", so the pictures are built into the graph exactly in the
order the caller gave them, and only as many branches as there are pictures —
a placeholder LoadImage for an unused slot would fail the whole prompt.
"""
from __future__ import annotations

from typing import Any, Dict, List

# Renderfin `type` values. The public API names them; nothing else does.
TYPE_KLEIN = "image_multiref"
TYPE_QWEN = "qwen_image_edit_multi"

WORKFLOW_KLEIN = "gen_image_flux2_klein_multiref.json"
WORKFLOW_QWEN = "qwen_image_edit_multi.json"

MULTIREF_WORKFLOWS = {
    TYPE_KLEIN: WORKFLOW_KLEIN,
    TYPE_QWEN: WORKFLOW_QWEN,
}
MULTIREF_TYPES = frozenset(MULTIREF_WORKFLOWS)

# (minimum, maximum) pictures per template.
REFERENCE_LIMITS = {
    WORKFLOW_KLEIN: (1, 4),
    WORKFLOW_QWEN: (1, 3),
}


def is_multiref_workflow(workflow_file: str) -> bool:
    return workflow_file in REFERENCE_LIMITS


def check_reference_count(workflow_file: str, count: int) -> None:
    low, high = REFERENCE_LIMITS[workflow_file]
    if not low <= count <= high:
        raise ValueError(
            f"{workflow_file} takes {low} to {high} reference images, got {count}"
        )


def reference_megapixels(count: int) -> float:
    """Pixels each reference is brought to before it is encoded.

    Every reference becomes as many image tokens as the output itself at the
    same size, so four full-size ones quadruple the attention the 8 GB boxes
    have to hold. Two stay at a full megapixel; more share a smaller budget.
    """
    return 1.0 if count <= 2 else 0.75


def _anchor(workflow: Dict[str, Any], node_id: str, class_type: str) -> Dict[str, Any]:
    node = workflow.get(node_id)
    if not isinstance(node, dict) or node.get("class_type") != class_type:
        raise ValueError(f"multi-reference workflow needs a {class_type} node '{node_id}'")
    return node


def _load_and_scale(workflow: Dict[str, Any], prefix: str, index: int,
                    filename: str, megapixels: float, steps: int) -> str:
    image_node = f"{prefix}_{index}_image"
    scale_node = f"{prefix}_{index}_scale"
    workflow[image_node] = {
        "class_type": "LoadImage",
        "inputs": {"image": filename},
        "_meta": {"title": f"Reference {index}"},
    }
    workflow[scale_node] = {
        "class_type": "ImageScaleToTotalPixels",
        "inputs": {"image": [image_node, 0], "upscale_method": "area",
                   "megapixels": megapixels, "resolution_steps": steps},
    }
    return scale_node


def inject_klein_references(workflow: Dict[str, Any], filenames: List[str]) -> None:
    """Chain one ReferenceLatent per picture between the prompt and the guider."""
    check_reference_count(WORKFLOW_KLEIN, len(filenames))
    _anchor(workflow, "positive", "CLIPTextEncode")
    _anchor(workflow, "vae", "VAELoader")
    guider = _anchor(workflow, "guider", "BasicGuider")
    megapixels = reference_megapixels(len(filenames))
    conditioning: List[Any] = ["positive", 0]
    for index, filename in enumerate(filenames, start=1):
        scale_node = _load_and_scale(workflow, "multiref", index, filename, megapixels, 1)
        encode_node = f"multiref_{index}_encode"
        condition_node = f"multiref_{index}_conditioning"
        workflow[encode_node] = {
            "class_type": "VAEEncode",
            "inputs": {"pixels": [scale_node, 0], "vae": ["vae", 0]},
        }
        workflow[condition_node] = {
            "class_type": "ReferenceLatent",
            "inputs": {"conditioning": conditioning, "latent": [encode_node, 0]},
        }
        conditioning = [condition_node, 0]
    guider.setdefault("inputs", {})["conditioning"] = conditioning


def inject_qwen_references(workflow: Dict[str, Any], filenames: List[str]) -> None:
    """Wire the pictures into image1..imageN of both Qwen text encoders.

    The negative encoder gets the same pictures, as ComfyUI's own 2511
    template does: CFG then contrasts the prompt, not the presence of the
    references.
    """
    check_reference_count(WORKFLOW_QWEN, len(filenames))
    positive = _anchor(workflow, "positive", "TextEncodeQwenImageEditPlus")
    negative = _anchor(workflow, "negative", "TextEncodeQwenImageEditPlus")
    for node in (positive, negative):
        for slot in ("image1", "image2", "image3"):
            node.setdefault("inputs", {}).pop(slot, None)
    # The encoder resizes each picture itself (384 px square for the vision
    # tower, one megapixel for the reference latent); scaling first only keeps
    # a 6000 px phone photo from being decoded at full size on an 8 GB card.
    for index, filename in enumerate(filenames, start=1):
        scale_node = _load_and_scale(workflow, "multiref", index, filename, 1.0, 16)
        positive["inputs"][f"image{index}"] = [scale_node, 0]
        negative["inputs"][f"image{index}"] = [scale_node, 0]


def inject_references(workflow_file: str, workflow: Dict[str, Any],
                      filenames: List[str]) -> None:
    if workflow_file == WORKFLOW_KLEIN:
        inject_klein_references(workflow, filenames)
    elif workflow_file == WORKFLOW_QWEN:
        inject_qwen_references(workflow, filenames)
    else:
        raise ValueError(f"{workflow_file} does not take reference images")

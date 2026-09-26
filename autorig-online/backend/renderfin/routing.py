"""Workflow routing rules ported from C# Render.cs / RenderWorkflowRouting.cs / Adapter_Comfy.cs."""
from __future__ import annotations

import re
from typing import Optional, Tuple

from .models import RenderPrompt, RenderServer
from .multiref import MULTIREF_TYPES, MULTIREF_WORKFLOWS

SAFE_WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]+\.json$")

WORKFLOW_GEN_IMAGE = "gen_image.json"
WORKFLOW_GEN_IMAGE_SDXL = "gen_image_sdxl.json"
WORKFLOW_T_POSE = "t_pose.json"
WORKFLOW_Z_DEPTH = "gen_image_by_z_depth.json"
WORKFLOW_OPEN_POSE = "open_pose.json"
WORKFLOW_INPAINT = "inpaint.json"
WORKFLOW_IMAGE_TO_3D = "image_to_3d.json"
WORKFLOW_ANIMATION_DEFAULT = "gen_animation_by_url.json"

# --------------------------------------------------------------- enhancement
# Super-resolution, detail refinement and face repair. Each is a picture in and
# the same picture out, so they share the typed-image branch; only the template
# differs. The type name is the routing key, exactly as the control maps do it.
ENHANCE_WORKFLOWS = {
    "upscale_fast": "upscale_fast.json",
    "upscale_refine": "upscale_refine.json",
    "detail_tiled": "detail_tiled.json",
    "detail_plain": "detail_plain.json",
    "face_fix": "face_fix.json",
    "face_fix_skin": "face_fix_skin.json",
    # Upscale 2x node (2026-09-26): RealESRGAN x2, a picture or a clip frame by frame.
    "upscale_x2": "upscale_fast.json",
    "upscale_video_x2": "upscale_video_x2.json",
}
VIDEO_ENHANCE_TYPES = frozenset({"upscale_video_x2"})
ENHANCE_TYPES = frozenset(ENHANCE_WORKFLOWS)

# Which workers may take this work. Scheduling matches a token against a
# server's advertised `available_workflows`, and those lists are published by
# the farm boxes themselves, so a brand-new file name matches nothing and the
# job would never be dispatched. gen_image.json would match, but it also
# matches worker-4090, which has no ESRGAN weights and no TiledDiffusion and
# would fail every one of these. The canny-control token is advertised by the
# four image boxes (f5, f12, f15, Raptor) and by no video box, which is
# exactly the set that can run these templates.
ENHANCE_SCHEDULING_TOKEN = "gen_image_control_canny.json"

# A 2x or 4x enlargement is the whole point here, so these are not held to the
# 2048 px render ceiling that keeps ordinary generation inside 8 GB.
ENHANCE_MAX_SIDE = 4096

# ---------------------------------------------------------------- Qwen-Image
# One product node, two templates: text alone gives a picture, a picture plus
# text edits that picture. Both load a GGUF quantisation through ComfyUI-GGUF,
# which is the only reason a 20B MMDiT runs on the 8 GB image boxes at all.
QWEN_IMAGE_WORKFLOWS = {
    "qwen_image": "qwen_image_generate.json",
    "qwen_image_edit": "qwen_image_edit.json",
    # Qwen-Image-2.1 with Viggle's 6-step turbo LoRA (2026-09-26): one int8
    # transformer serves both, no CFG, a fixed 6-sigma schedule. The GGUF pair
    # above stays installed and selectable by `checkpoint` for rollback.
    "qwen_image21": "qwen_image21_generate.json",
    "qwen_image21_edit": "qwen_image21_edit.json",
}
QWEN_IMAGE_TYPES = frozenset(QWEN_IMAGE_WORKFLOWS)

# The boxes publish `available_workflows` themselves, so a file name invented
# here matches no worker and the job would never be dispatched. The canny
# control token names exactly the set allowed to run this: the four image
# boxes, and not worker-4090 — a single 24 GB card would win every dispatch
# and leave the parallel pair the owner asked for standing idle. Which of the
# four actually holds the GGUF is then settled by model_eligibility, from the
# worker's own /object_info and fail-closed.
QWEN_IMAGE_SCHEDULING_TOKEN = ENHANCE_SCHEDULING_TOKEN

# ------------------------------------------------------- multi-reference edit
# Several pictures composed into one (renderfin.multiref). Scheduled on the
# same token as Qwen-Image for the same reason: it names exactly the image
# boxes (f5, f15, Raptor) and not worker-4090, and model_eligibility then
# keeps the job on a box that holds the named checkpoint.
MULTIREF_SCHEDULING_TOKEN = ENHANCE_SCHEDULING_TOKEN

# Canonical LTX2 animation names advertised by workers; runtime file comes from
# the worker's workflow_overrides (RenderWorkflowRouting.ResolveRuntimeWorkflow).
CANONICAL_ANIMATION_WORKFLOWS = {
    "autorig_animal_loop_v1",
    "autorig_animal_one_shot_v1",
    "autorig_animal_loop_ltx2_19b_v1",
    "autorig_animal_oneshot_ltx2_19b_v1",
}

IMAGE_TYPES = {"z_depth", "t_pose", "t_poses", "open_pose", "inpaint", "material", "image_to_3d"}


def is_image_request(prompt: RenderPrompt) -> bool:
    """C# RenderTask ctor: image branch when image_url is empty OR type is set."""
    return not (prompt.image_url or "").strip() or bool((prompt.type or "").strip())


def output_extension(prompt: RenderPrompt) -> str:
    if (prompt.type or "").strip().lower() == "image_to_3d":
        return ".glb"
    if (prompt.type or "").strip().lower() in VIDEO_ENHANCE_TYPES:
        return ".mp4"
    return ".png" if is_image_request(prompt) else ".mp4"


def scheduling_token(prompt: RenderPrompt) -> str:
    """The workflow name used to match servers' available_workflows.

    C# quirk preserved: any typed image request is scheduled as gen_image.json;
    the actual template file is picked later from prompt.type. image_to_3d is
    new here and gets its own token so it only lands on capable workers (4090).
    """
    ptype = (prompt.type or "").strip().lower()
    if ptype in {"control_pose", "control_depth", "control_canny"}:
        return "gen_" + ptype + ".json"
    if ptype in ENHANCE_TYPES:
        return ENHANCE_SCHEDULING_TOKEN
    if ptype in QWEN_IMAGE_TYPES:
        return QWEN_IMAGE_SCHEDULING_TOKEN
    if ptype in MULTIREF_TYPES:
        return MULTIREF_SCHEDULING_TOKEN
    if ptype == "image_to_3d":
        return WORKFLOW_IMAGE_TO_3D
    if is_image_request(prompt):
        requested = (prompt.work_flow or "").strip()
        if (ptype in ("", "image") or ptype.startswith("image_control_")) \
                and requested and SAFE_WORKFLOW_RE.match(requested):
            return requested
        return WORKFLOW_GEN_IMAGE
    return select_animation_workflow(prompt.work_flow)


def select_animation_workflow(requested: str) -> str:
    requested = (requested or "").strip()
    if requested in CANONICAL_ANIMATION_WORKFLOWS:
        return requested
    if requested and SAFE_WORKFLOW_RE.match(requested):
        return requested
    return WORKFLOW_ANIMATION_DEFAULT


def select_image_workflow(prompt: RenderPrompt) -> Tuple[str, Optional[Tuple[int, int]]]:
    """Port of Adapter_Comfy.cs:334-373. Returns (workflow_file, forced_size|None)."""
    ptype = (prompt.type or "").strip().lower()
    image_url = (prompt.image_url or "").strip()
    has_aspect_ratio = (prompt.aspect_ratio or 0) > 0

    if ptype in {"control_pose", "control_depth", "control_canny"}:
        return "gen_" + ptype + ".json", None
    if ptype in ENHANCE_WORKFLOWS:
        return ENHANCE_WORKFLOWS[ptype], None
    if ptype in QWEN_IMAGE_WORKFLOWS:
        return QWEN_IMAGE_WORKFLOWS[ptype], None
    if ptype in MULTIREF_WORKFLOWS:
        return MULTIREF_WORKFLOWS[ptype], None
    if ptype == "image_to_3d":
        return WORKFLOW_IMAGE_TO_3D, None
    if ptype == "z_depth":
        return WORKFLOW_Z_DEPTH, None
    has_explicit_size = prompt.main_size_width > 0 and prompt.main_size_height > 0
    if ptype in ("t_pose", "t_poses") and not has_aspect_ratio:
        # The pose skeleton is square and the T-pose quality gate compares the
        # render against it, so a landscape default (the image API sends
        # 960x540) would be rejected every time. Only a square size is honoured.
        square = has_explicit_size and prompt.main_size_width == prompt.main_size_height
        return WORKFLOW_T_POSE, None if square else (1024, 1024)
    if ptype == "open_pose":
        return WORKFLOW_OPEN_POSE, None
    if ptype == "inpaint":
        return WORKFLOW_INPAINT, None
    if "sphere.png" in image_url:
        return WORKFLOW_Z_DEPTH, None
    requested = (prompt.work_flow or "").strip()
    if requested and SAFE_WORKFLOW_RE.match(requested):
        return requested, None
    return WORKFLOW_GEN_IMAGE, None


def resolve_workflow_file(prompt: RenderPrompt) -> Tuple[str, Optional[Tuple[int, int]]]:
    """Actual template file to run + optional forced (width, height)."""
    if is_image_request(prompt):
        return select_image_workflow(prompt)
    return select_animation_workflow(prompt.work_flow), None


def resolve_runtime_workflow(server: RenderServer, canonical: str) -> str:
    """Port of RenderWorkflowRouting.ResolveRuntimeWorkflow: apply the server's
    workflow_overrides mapping and validate the resulting file name."""
    runtime = (server.workflow_overrides or {}).get(canonical, canonical)
    runtime = (runtime or "").strip()
    if not runtime.endswith(".json"):
        runtime = f"{runtime}.json" if runtime else canonical
    if not SAFE_WORKFLOW_RE.match(runtime):
        raise ValueError(f"unsafe workflow name: {runtime!r}")
    return runtime


def server_can_run(server: RenderServer, token: str) -> bool:
    return token in (server.available_workflows or [])


def clamp_image_dims(width: int, height: int) -> Tuple[int, int]:
    """Image render size: defaults 960x540, preserves exact pixels to 2048."""

    def one(v: int, default: int) -> int:
        v = int(v or 0)
        if v <= 0:
            v = default
        v = max(64, min(2048, v))
        return v

    return one(width, 960), one(height, 540)


def clamp_enhance_dims(width: int, height: int) -> Tuple[int, int]:
    """Delivery size for an enhancement: the source size times the factor asked
    for, kept whole up to ENHANCE_MAX_SIDE rather than the 2048 render ceiling."""

    def one(v: int, default: int) -> int:
        v = int(v or 0)
        if v <= 0:
            v = default
        return max(64, min(ENHANCE_MAX_SIDE, v))

    return one(width, 960), one(height, 540)


def clamp_video_dims(width: int, height: int) -> Tuple[int, int]:
    """Keep requested delivery size; only the encoder needs even pixels.

    Model-specific /32 padding happens in runtime_settings, not here.
    """

    def one(v: int, default: int) -> int:
        v = int(v or 0)
        if v <= 0:
            v = default
        v = max(64, min(2048, v))
        return v if v % 2 == 0 else min(2048, v + 1)

    return one(width, 960), one(height, 540)

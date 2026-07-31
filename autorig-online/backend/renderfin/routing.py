"""Workflow routing rules ported from C# Render.cs / RenderWorkflowRouting.cs / Adapter_Comfy.cs."""
from __future__ import annotations

import re
from typing import Optional, Tuple

from .models import RenderPrompt, RenderServer

SAFE_WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]+\.json$")

WORKFLOW_GEN_IMAGE = "gen_image.json"
WORKFLOW_T_POSE = "t_pose.json"
WORKFLOW_Z_DEPTH = "gen_image_by_z_depth.json"
WORKFLOW_OPEN_POSE = "open_pose.json"
WORKFLOW_INPAINT = "inpaint.json"
WORKFLOW_IMAGE_TO_3D = "image_to_3d.json"
WORKFLOW_ANIMATION_DEFAULT = "gen_animation_by_url.json"

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
    return ".png" if is_image_request(prompt) else ".mp4"


def scheduling_token(prompt: RenderPrompt) -> str:
    """The workflow name used to match servers' available_workflows.

    C# quirk preserved: any typed image request is scheduled as gen_image.json;
    the actual template file is picked later from prompt.type. image_to_3d is
    new here and gets its own token so it only lands on capable workers (4090).
    """
    ptype = (prompt.type or "").strip().lower()
    if ptype == "image_to_3d":
        return WORKFLOW_IMAGE_TO_3D
    if is_image_request(prompt):
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

    if ptype == "image_to_3d":
        return WORKFLOW_IMAGE_TO_3D, None
    if ptype == "z_depth":
        return WORKFLOW_Z_DEPTH, None
    if ptype in ("t_pose", "t_poses") and not has_aspect_ratio:
        return WORKFLOW_T_POSE, (1024, 1024)
    if ptype == "open_pose":
        return WORKFLOW_OPEN_POSE, None
    if ptype == "inpaint":
        return WORKFLOW_INPAINT, None
    if "sphere.png" in image_url:
        return WORKFLOW_Z_DEPTH, None
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
    """C# final clamp for image workflows: 64-1024 per side, rounded to /32."""

    def one(v: int) -> int:
        v = int(v or 0)
        if v <= 0:
            v = 1024
        v = max(64, min(1024, v))
        return max(64, round(v / 32) * 32)

    return one(width), one(height)


def clamp_video_dims(width: int, height: int) -> Tuple[int, int]:
    """C# pinned-workflow clamp: 64-512, /32, defaults 384x224."""

    def one(v: int, default: int) -> int:
        v = int(v or 0)
        if v <= 0:
            v = default
        v = max(64, min(512, v))
        return max(64, round(v / 32) * 32)

    return one(width, 384), one(height, 224)

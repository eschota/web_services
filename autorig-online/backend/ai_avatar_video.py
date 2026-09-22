"""Owner-scoped saved Avatar animation through the verified Wan Animate 2 workflow."""
from __future__ import annotations

import math
import re
from typing import Callable, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

import ai_request_cache
import ai_services
from ai_avatar_assets import validate_import_url
from ai_avatars import AvatarOwner, AvatarStore, AvatarStoreError, resolve_avatar_identity
from ai_vision_api import RENDERFIN_BASE, SUBMIT_TIMEOUT_SECONDS
from renderfin.video_input import VideoInputError, validate_video_url


WORKFLOW = "gen_video_wan_animate2_by_url.json"
CHECKPOINT = "wan_animate_2_int8_convrot.safetensors"
MAX_PADDED_PIXELS = 524_288


class AvatarVideoRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    avatar: str
    avatar_secondary: Optional[str] = None
    control_video_url: str
    image_url: Optional[str] = None
    prompt: str = Field(default="", max_length=2000)
    width: int = Field(default=960, ge=256, le=2048)
    height: int = Field(default=540, ge=256, le=2048)
    frame_count: int = Field(default=97, ge=9, le=97)
    seed: int = Field(default=0, ge=0, le=9007199254740991)
    control_strength: float = Field(default=1.0, ge=0, le=1)

    @field_validator("frame_count")
    @classmethod
    def validate_frame_count(cls, value: int) -> int:
        if (value - 1) % 8:
            raise ValueError("frame_count must be 8k+1 in the range 9..97")
        return value

    @field_validator("width", "height")
    @classmethod
    def validate_even_delivery_dimension(cls, value: int) -> int:
        if value % 2:
            raise ValueError("video width and height must be even for exact delivery")
        return value


def _split_avatar_reference(value: str) -> tuple[str, Optional[int]]:
    match = re.fullmatch(r"(av_[a-f0-9]{24})(?:@([1-9][0-9]{0,5}))?", value or "")
    if not match:
        raise HTTPException(400, detail="Choose a saved Avatar version")
    return match[1], int(match[2]) if match[2] else None


def _validate_dimensions(width: int, height: int) -> None:
    padded_width = math.ceil(width / 32) * 32
    padded_height = math.ceil(height / 32) * 32
    if padded_width * padded_height > MAX_PADDED_PIXELS:
        raise HTTPException(status_code=400, detail={
            "error_string": "avatar_video_resolution_too_large",
            "message_string": (
                f"{width}x{height} pads internally to {padded_width}x{padded_height}, "
                f"above the verified 24 GB preset limit of {MAX_PADDED_PIXELS} pixels. "
                "Choose a smaller size; the server will not silently resize it.")})


def _resolve_profile(store: AvatarStore, selected: str, owner: AvatarOwner):
    avatar_id, version = _split_avatar_reference(selected)
    try:
        return resolve_avatar_identity(
            store, avatar_id, owner, version=version, pipeline_family="wan-animate-2")
    except AvatarStoreError as error:
        raise HTTPException(error.status_code, detail={
            "error_string": error.code, "message_string": error.message}) from None


def _primary_image(profile):
    images = [reference for reference in profile.references if reference.media_type == "image"]
    primary = next((reference for reference in images if reference.role == "body"), None)
    primary = primary or next((reference for reference in images if reference.role == "face"), None)
    primary = primary or (images[0] if images else None)
    if primary is None:
        raise HTTPException(400, detail="This Avatar needs a body or face image reference")
    try:
        validate_import_url(primary.canonical_url)
    except ValueError:
        raise HTTPException(400, detail="Import the Avatar reference into AutoRig before rendering") from None
    return primary


def _appearance_prompt(profiles) -> str:
    constraints = []
    for index, profile in enumerate(profiles, 1):
        clauses = [f"Character {index} canonical identity: {profile.identity_prompt}."]
        if str(profile.appearance or "").strip():
            clauses.append(f"Character {index} canonical appearance: {profile.appearance}.")
        if str(profile.wardrobe or "").strip():
            clauses.append(f"Character {index} canonical wardrobe: {profile.wardrobe}.")
        constraints.append(" ".join(clauses))
    return (
        "Keep each saved character visually consistent in every frame. "
        + " ".join(constraints)
        + " Background follows the reference image. The driving video controls "
          "motion and timing only; it must not change face, hair, appearance, or wardrobe."
    )


def build_avatar_video_router(owner_dependency: Callable, *, store=None) -> APIRouter:
    avatars = store or AvatarStore()
    router = APIRouter()

    @router.post("/api/ai/avatar-video")
    async def avatar_video(body: AvatarVideoRequest,
                           owner: AvatarOwner = Depends(owner_dependency)):
        if (ai_services.service("avatar_video") or {}).get("status") != "live":
            raise HTTPException(status_code=503, detail={
                "error_string": "avatar_video_not_live",
                "message_string": "Avatar video is awaiting its production runtime promotion"})
        _validate_dimensions(body.width, body.height)
        try:
            driver_url = validate_video_url(body.control_video_url)
        except (ValueError, VideoInputError) as error:
            raise HTTPException(status_code=400, detail=str(error)) from None

        profiles = [_resolve_profile(avatars, body.avatar, owner)]
        if body.avatar_secondary:
            if not body.image_url:
                raise HTTPException(status_code=400, detail={
                    "error_string": "composite_keyframe_required",
                    "message_string": (
                        "A second Avatar requires a provided composite keyframe "
                        "that already contains both saved characters")})
            profiles.append(_resolve_profile(avatars, body.avatar_secondary, owner))

        if body.image_url:
            try:
                keyframe_url = validate_import_url(body.image_url)
            except ValueError:
                raise HTTPException(400, detail="Upload the character keyframe to AutoRig first") from None
            primary = None
        else:
            primary = _primary_image(profiles[0])
            keyframe_url = primary.canonical_url

        receipts = []
        for index, profile in enumerate(profiles, 1):
            reference = primary if index == 1 and primary is not None else _primary_image(profile)
            receipts.append({
                "avatar_id": profile.avatar_id,
                "version": profile.version,
                "reference_sha256": reference.sha256,
            })

        pose_prompt = str(body.prompt or "").strip() or (
            "Follow only the poses, timing, and motion in the driving video; "
            "neutral expression; no added action."
        )
        payload = {
            "work_flow": WORKFLOW,
            "image_url": keyframe_url,
            "control_video_url": driver_url,
            "prompt": _appearance_prompt(profiles),
            "pose_prompt": pose_prompt,
            "main_size_width": body.width,
            "main_size_height": body.height,
            "frame_count": body.frame_count,
            "noise_seed": body.seed,
            "control_strength": body.control_strength,
            "checkpoint": CHECKPOINT,
            "steps": 6,
            "cfg": 1.0,
            "sampler": "lcm",
            "scheduler": "simple",
        }

        async def submit():
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(
                        RENDERFIN_BASE + "/api-render", json=payload,
                        timeout=SUBMIT_TIMEOUT_SECONDS)
                    response.raise_for_status()
                    accepted = response.json()
                except (httpx.HTTPError, ValueError):
                    raise HTTPException(
                        502, detail="The farm did not accept the Avatar video") from None
            if (not isinstance(accepted, dict) or not accepted.get("task_id")
                    or not accepted.get("output_url")):
                raise HTTPException(502, detail="The farm returned an incomplete Avatar video job")
            return {
                "success_bool": True,
                "task_id_string": accepted["task_id"],
                "video_url_string": accepted["output_url"],
                "poll_url_string": accepted["output_url"],
                "status_string": "pending",
                "finished_bool": False,
                "avatar_versions_array": receipts,
                "effective_params_object": {
                    "width": body.width, "height": body.height,
                    "frame_count": body.frame_count, "control_strength": body.control_strength,
                    "steps": 6, "cfg": 1.0, "sampler": "lcm",
                    "scheduler": "simple", "checkpoint": CHECKPOINT,
                    "work_flow": WORKFLOW,
                },
            }

        cache_payload = dict(
            payload,
            seed=body.seed,
            avatar_versions=receipts,
            owner=owner.model_dump(),
        )
        return await ai_request_cache.run_cached(
            "video", cache_payload, submit, namespace="avatar-video-wan2-v1")

    return router

"""Owner-scoped Avatar keyframes using ordered FLUX.2 reference conditioning."""
from __future__ import annotations

import re
from typing import Callable, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

import ai_request_cache
from ai_avatars import AvatarOwner, AvatarStore, AvatarStoreError, resolve_avatar_identity
from ai_avatar_assets import validate_import_url
from ai_vision_api import RENDERFIN_BASE, SUBMIT_TIMEOUT_SECONDS


class AvatarSceneRequest(BaseModel):
    avatar: str
    avatar_secondary: Optional[str] = None
    image_url: Optional[str] = None
    prompt: str = Field(min_length=1, max_length=6000)
    width: int = Field(default=960, ge=256, le=2048)
    height: int = Field(default=540, ge=256, le=2048)
    seed: int = Field(default=0, ge=0, le=9007199254740991)


def split_avatar_reference(value: str):
    match = re.fullmatch(r"(av_[a-f0-9]{24})(?:@([1-9][0-9]{0,5}))?", value or "")
    if not match:
        raise HTTPException(400, detail="Choose a saved Avatar version")
    return match[1], int(match[2]) if match[2] else None


def build_avatar_render_router(owner_dependency: Callable, *, store=None):
    router = APIRouter()
    avatars = store or AvatarStore()

    @router.post("/api/ai/avatar-image")
    async def avatar_image(body: AvatarSceneRequest, owner: AvatarOwner = Depends(owner_dependency)):
        profiles = []
        for selected in [body.avatar, body.avatar_secondary]:
            if not selected:
                continue
            avatar_id, version = split_avatar_reference(selected)
            try:
                profiles.append(resolve_avatar_identity(avatars, avatar_id, owner, version=version,
                                                        pipeline_family="flux2-klein-4b"))
            except AvatarStoreError as error:
                raise HTTPException(error.status_code, detail={"error_string": error.code,
                                      "message_string": error.message}) from None
        references, instructions, receipts = [], [], []
        for index, profile in enumerate(profiles, 1):
            images = [r for r in profile.references if r.media_type == "image"]
            if not images:
                raise HTTPException(400, detail="This Avatar needs an image reference")
            primary = next((r for r in images if r.role == "face"), images[0])
            references.append(primary.canonical_url)
            instructions.append(
                f"Reference image {len(references)} defines character {index}, {profile.display_name}. "
                f"Preserve this person's facial structure, age, hair and identity. "
                f"Identity: {profile.identity_prompt}. Appearance: {profile.appearance}. "
                f"Wardrobe: {profile.wardrobe}."
            )
            receipts.append({"avatar_id": profile.avatar_id, "version": profile.version,
                             "reference_sha256": primary.sha256})
        if body.image_url:
            try:
                validate_import_url(body.image_url)
            except ValueError:
                raise HTTPException(400, detail="Upload the scene reference to AutoRig first") from None
            references.append(body.image_url)
            instructions.append(f"Reference image {len(references)} defines the scene, composition, "
                                "camera and action. Replace its principal subject(s) with the "
                                "specified characters while preserving their separate identities.")
        instructions.append("Create one coherent photograph, not a collage or character sheet. "
                            "Scene instruction: " + body.prompt)
        payload = {"type": "image", "work_flow": "gen_image_flux2_avatar.json",
                   "prompt": "\n".join(instructions), "reference_image_urls": references,
                   "main_size_width": body.width, "main_size_height": body.height,
                   "noise_seed": body.seed, "steps": 4, "cfg": 1.0, "sampler": "euler",
                   "checkpoint": "flux-2-klein-4b.safetensors"}

        async def submit():
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(RENDERFIN_BASE + "/api-render", json=payload,
                                                 timeout=SUBMIT_TIMEOUT_SECONDS)
                    response.raise_for_status()
                    accepted = response.json()
                except (httpx.HTTPError, ValueError):
                    raise HTTPException(502, detail="The farm did not accept the Avatar keyframe") from None
            if not isinstance(accepted, dict) or not accepted.get("task_id") or not accepted.get("output_url"):
                raise HTTPException(502, detail="The farm returned an incomplete Avatar job")
            return {"success_bool": True, "task_id_string": accepted["task_id"],
                    "image_url_string": accepted["output_url"], "poll_url_string": accepted["output_url"],
                    "status_string": "pending", "finished_bool": False,
                    "avatar_versions_array": receipts,
                    "conditioning_string": "flux2_ordered_references",
                    "effective_params_object": {"width": body.width, "height": body.height, "steps": 4}}

        cache_payload = dict(payload, seed=body.seed, avatar_versions=receipts,
                             owner=owner.model_dump())
        return await ai_request_cache.run_cached("image", cache_payload, submit,
                                                 namespace="avatar-keyframes-v1")

    return router

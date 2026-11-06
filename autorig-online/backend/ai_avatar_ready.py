"""Avatar (ready or build) for /nodes (2026-09-27).

A saved Avatar (`av_...@N`) comes out on the same sockets as the Avatar builder
(avatar_string, the eight views, sheet, description), so a graph can use a
ready identity instead of building one. Only the Avatar's owner can read it,
exactly as everywhere else. With no Avatar wired, the picture/video is handed
to the Avatar builder and the editor follows that build job.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from ai_avatars import AvatarOwner, AvatarStore, AvatarStoreError

VIEW_SLOTS = ("front", "face_closeup", "full_body", "three_quarter_left", "three_quarter_right",
              "profile_left", "profile_right", "back")
INTERNAL_BASE = "http://127.0.0.1:8200"


class AvatarReadyRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    avatar: Optional[str] = None
    avatar_id: Optional[str] = None
    image_url: Optional[str] = None
    video_url: Optional[str] = None


def _split(value: str):
    match = re.fullmatch(r"\s*(av_[a-f0-9]{24})(?:@([1-9][0-9]{0,5}))?\s*", value or "")
    if not match:
        raise HTTPException(400, detail={"error_string": "bad_avatar",
                                         "message_string": "Give a saved Avatar as av_…@version"})
    return match[1], int(match[2]) if match[2] else None


def ready_outputs(profile) -> Dict[str, Any]:
    out: Dict[str, Any] = {"avatar_string": f"{profile.avatar_id}@{profile.version}"}
    for slot in VIEW_SLOTS:
        view = profile.views.get(slot)
        if view and view.canonical_url:
            out[f"{slot}_url_string"] = view.canonical_url
    if not out.get("front_url_string") or not out.get("full_body_url_string"):
        # Older Avatars carry only references: body stands in for the views.
        images = [r for r in profile.references if r.media_type == "image" and r.canonical_url]
        body = next((r for r in images if r.role == "body"), None) or (images[0] if images else None)
        face = next((r for r in images if r.role == "face"), None) or body
        if body:
            out.setdefault("front_url_string", body.canonical_url)
            out.setdefault("full_body_url_string", body.canonical_url)
        if face:
            out.setdefault("face_closeup_url_string", face.canonical_url)
    if profile.sheet and profile.sheet.canonical_url:
        out["sheet_url_string"] = profile.sheet.canonical_url
    source = next((r for r in profile.references if r.role == "face" and r.canonical_url), None)
    if source:
        out["source_frame_url_string"] = source.canonical_url
    out["description_string"] = "; ".join(filter(None, [profile.identity_prompt, profile.appearance,
                                                         profile.body, profile.wardrobe]))[:4000]
    return out


def build_avatar_ready_router(owner_dependency: Callable, *, store: Optional[AvatarStore] = None) -> APIRouter:
    avatars = store or AvatarStore()
    router = APIRouter()

    @router.post("/api/ai/avatar-ready")
    async def avatar_ready(body: AvatarReadyRequest, request: Request,
                           owner: AvatarOwner = Depends(owner_dependency)):
        chosen = (body.avatar or body.avatar_id or "").strip()
        if chosen:
            avatar_id, version = _split(chosen)
            try:
                profile = avatars.get(avatar_id, owner, version)
            except AvatarStoreError as error:
                message = error.message
                if error.code == "avatar_not_found":
                    message = ("Avatar not found for this session — a saved Avatar is private to the account "
                               "that made it; open the graph logged in as its owner")
                raise HTTPException(error.status_code, detail={"error_string": error.code,
                                                               "message_string": message}) from None
            return dict(ready_outputs(profile), success_bool=True, finished_bool=True,
                        status_string="completed", ready_bool=True, task_id_string="")
        if not (body.image_url or body.video_url):
            raise HTTPException(400, detail={"error_string": "avatar_ready_needs_input",
                                             "message_string": "Wire a saved Avatar, or a picture/video to build one"})
        # No saved Avatar: build one from the picture, as the same owner.
        payload = body.model_dump(exclude_none=True)
        payload.pop("avatar", None)
        payload.pop("avatar_id", None)
        headers = {"cookie": request.headers.get("cookie", "")}
        if request.headers.get("authorization"):
            headers["authorization"] = request.headers["authorization"]
        async with httpx.AsyncClient() as client:
            response = await client.post(INTERNAL_BASE + "/api/ai/avatar-build", json=payload,
                                         headers=headers, timeout=60)
        try:
            data = response.json()
        except ValueError:
            raise HTTPException(502, detail="the Avatar builder did not answer") from None
        if response.status_code >= 400:
            raise HTTPException(response.status_code, detail=data.get("detail", data))
        return data

    return router

"""Explicit U3D provider API, isolated from AutoRig's automatic YouTube channel."""
from __future__ import annotations

import asyncio
import hmac
import os
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession

from config import U3D_YOUTUBE_AGENT_KEYS, U3D_YOUTUBE_CLIENT_ID, U3D_YOUTUBE_CLIENT_SECRET, U3D_YOUTUBE_EXPECTED_CHANNEL_ID
from database import U3dYoutubeCredentials


MAX_UPLOAD_BYTES = max(1, int(os.getenv("YOUTUBE_API_UPLOAD_MAX_MB", "4096"))) * 1024 * 1024
ALLOWED_PRIVACY = {"private", "unlisted", "public"}


def _agent_key_from_headers(authorization: Optional[str], legacy_key: Optional[str]) -> Optional[str]:
    if authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    return legacy_key.strip() if legacy_key else None


def _agent_key_is_allowed(candidate: Optional[str]) -> bool:
    return bool(candidate) and any(
        hmac.compare_digest(candidate, allowed_key)
        for allowed_key in U3D_YOUTUBE_AGENT_KEYS
    )


def build_youtube_upload_api_router(require_admin, get_db) -> APIRouter:
    router = APIRouter(prefix="/api/youtube", tags=["YouTube"])

    @router.get("/status")
    async def provider_status(db: AsyncSession = Depends(get_db)):
        row = await db.get(U3dYoutubeCredentials, 1)
        return {
            "service": "u3d-youtube-upload",
            "channel": "@unlim3d",
            "channel_id": U3D_YOUTUBE_EXPECTED_CHANNEL_ID,
            "oauth_connected": bool(row and row.refresh_token),
            "oauth_client_configured": bool(U3D_YOUTUBE_CLIENT_ID and U3D_YOUTUBE_CLIENT_SECRET),
            "agent_access_configured": bool(U3D_YOUTUBE_AGENT_KEYS),
            "ready": bool(
                row
                and row.refresh_token
                and U3D_YOUTUBE_CLIENT_ID
                and U3D_YOUTUBE_CLIENT_SECRET
                and U3D_YOUTUBE_AGENT_KEYS
            ),
            "upload_endpoint": "https://autorig.online/dev/api/youtube/videos",
            "skill_url": "https://autorig.online/dev/youtube/skill.md",
        }

    @router.get("/agent-key")
    async def get_agent_key(admin=Depends(require_admin)):
        """Return the dedicated provider key to an authenticated AutoRig administrator."""
        if not U3D_YOUTUBE_AGENT_KEYS:
            raise HTTPException(status_code=503, detail="U3D agent access is not configured")
        return {
            "api_key": U3D_YOUTUBE_AGENT_KEYS[0],
            "authorization": "Authorization: Bearer <api_key>",
            "upload_endpoint": "https://autorig.online/dev/api/youtube/videos",
        }

    @router.get("/skill.md", response_class=PlainTextResponse)
    async def agent_skill():
        return """---
name: u3d-youtube-upload
description: Upload owner-approved Shorts and long videos to the U3d Indie Game Developer YouTube channel through AutoRig's isolated provider API.
---

# U3D YouTube Upload

Use this skill only when the owner explicitly asks to publish a specific video to `@unlim3d`.
Do not use AutoRig admin API keys. Obtain the dedicated U3D agent key from the owner or, while signed in as an AutoRig administrator, from `GET https://autorig.online/dev/api/youtube/agent-key`.

Send a multipart request to `POST https://autorig.online/dev/api/youtube/videos` with header `Authorization: Bearer <U3D_AGENT_KEY>` and fields:

- `file`: the video file
- `title`: required, maximum 100 characters
- `description`: optional, maximum 5000 characters
- `tags`: optional comma-separated tags, maximum 30
- `privacy_status`: `public`, `unlisted`, or `private`; default `public`

The same endpoint handles Shorts and long videos. YouTube classifies Shorts from the uploaded media; there is no Shorts flag. Treat success only as HTTP 201 with `channel_matches_expected: true`. Report the returned `url`, actual `privacy_status`, and `channel_id`. Never print, commit, or place the U3D agent key in logs or source files.
"""

    @router.post("/videos", status_code=201)
    async def upload_video(
        file: UploadFile = File(...),
        title: str = Form(..., min_length=1, max_length=100),
        description: str = Form("", max_length=5000),
        tags: str = Form("", max_length=500),
        privacy_status: str = Form("public"),
        authorization: Optional[str] = Header(default=None, alias="Authorization"),
        legacy_agent_key: Optional[str] = Header(default=None, alias="X-U3D-Agent-Key"),
        db: AsyncSession = Depends(get_db),
    ):
        """Upload to U3D only when the owner explicitly provisions a key for an agent."""
        agent_key = _agent_key_from_headers(authorization, legacy_agent_key)
        if not U3D_YOUTUBE_AGENT_KEYS or not _agent_key_is_allowed(agent_key):
            raise HTTPException(status_code=403, detail="U3D upload access is not enabled for this agent")
        content_type = (file.content_type or "").lower()
        if not (content_type.startswith("video/") or content_type == "application/octet-stream"):
            raise HTTPException(status_code=415, detail="Upload a video file")
        if privacy_status not in ALLOWED_PRIVACY:
            raise HTTPException(status_code=400, detail="privacy_status must be private, unlisted, or public")

        row = await db.get(U3dYoutubeCredentials, 1)
        refresh_token: Optional[str] = row.refresh_token if row else None
        if not refresh_token:
            raise HTTPException(status_code=503, detail="YouTube channel is not connected")

        cleaned_tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
        if len(cleaned_tags) > 30:
            raise HTTPException(status_code=400, detail="At most 30 comma-separated tags are allowed")

        try:
            # FastAPI/Starlette has already streamed multipart data into its
            # private, seekable UploadFile spool. Pass that same file directly
            # to Google's resumable uploader instead of making a second copy.
            file.file.seek(0, os.SEEK_END)
            size = file.file.tell()
            file.file.seek(0)
            if size > MAX_UPLOAD_BYTES:
                raise HTTPException(status_code=413, detail=f"Video exceeds {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit")
            if size == 0:
                raise HTTPException(status_code=400, detail="Video file is empty")

            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
            from googleapiclient.http import MediaIoBaseUpload
            from youtube_upload import YOUTUBE_UPLOAD_SCOPE

            def perform_upload() -> tuple[str, str, Optional[str]]:
                credentials = Credentials(
                    token=None,
                    refresh_token=refresh_token,
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=U3D_YOUTUBE_CLIENT_ID,
                    client_secret=U3D_YOUTUBE_CLIENT_SECRET,
                    scopes=[YOUTUBE_UPLOAD_SCOPE],
                )
                credentials.refresh(Request())
                service = build("youtube", "v3", credentials=credentials, cache_discovery=False)
                body = {
                    "snippet": {
                        "title": title.strip(),
                        "description": description,
                        "categoryId": "28",
                        "tags": cleaned_tags,
                    },
                    "status": {
                        "privacyStatus": privacy_status,
                        "selfDeclaredMadeForKids": False,
                    },
                }
                media = MediaIoBaseUpload(file.file, chunksize=8 * 1024 * 1024, resumable=True, mimetype="video/*")
                request = service.videos().insert(part="snippet,status", body=body, media_body=media)
                response = None
                while response is None:
                    _, response = request.next_chunk()
                video_id = response.get("id") if isinstance(response, dict) else None
                if not video_id:
                    raise RuntimeError("YouTube API returned no video id")
                status = response.get("status") or {}
                snippet = response.get("snippet") or {}
                return video_id, status.get("privacyStatus", privacy_status), snippet.get("channelId")

            video_id, actual_privacy, channel_id = await asyncio.to_thread(perform_upload)
            return {
                "ok": True,
                "video_id": video_id,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "privacy_status": actual_privacy,
                "requested_privacy_status": privacy_status,
                "channel_id": channel_id,
                "channel_matches_expected": channel_id == U3D_YOUTUBE_EXPECTED_CHANNEL_ID,
            }
        except HTTPException:
            raise
        except Exception as exc:
            # Do not return provider response bodies or OAuth details to API callers.
            print(f"[YouTube API] Upload failed: {type(exc).__name__}")
            raise HTTPException(status_code=502, detail="YouTube upload failed; check server logs") from exc
        finally:
            await file.close()

    return router

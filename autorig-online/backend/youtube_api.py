"""Admin API for uploading owner supplied videos to the connected YouTube channel."""
from __future__ import annotations

import asyncio
import os
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from config import YOUTUBE_EXPECTED_CHANNEL_ID, YOUTUBE_REFRESH_TOKEN
from database import YoutubeCredentials


MAX_UPLOAD_BYTES = max(1, int(os.getenv("YOUTUBE_API_UPLOAD_MAX_MB", "4096"))) * 1024 * 1024
ALLOWED_PRIVACY = {"private", "unlisted", "public"}


def build_youtube_upload_api_router(require_admin, get_db) -> APIRouter:
    router = APIRouter(prefix="/api/youtube", tags=["YouTube"])

    @router.post("/videos", status_code=201)
    async def upload_video(
        file: UploadFile = File(...),
        title: str = Form(..., min_length=1, max_length=100),
        description: str = Form("", max_length=5000),
        tags: str = Form("", max_length=500),
        privacy_status: str = Form("public"),
        admin=Depends(require_admin),
        db: AsyncSession = Depends(get_db),
    ):
        """Upload a video using the channel OAuth token; API keys must belong to an admin."""
        content_type = (file.content_type or "").lower()
        if not (content_type.startswith("video/") or content_type == "application/octet-stream"):
            raise HTTPException(status_code=415, detail="Upload a video file")
        if privacy_status not in ALLOWED_PRIVACY:
            raise HTTPException(status_code=400, detail="privacy_status must be private, unlisted, or public")

        row = await db.get(YoutubeCredentials, 1)
        refresh_token: Optional[str] = (row.refresh_token if row else None) or YOUTUBE_REFRESH_TOKEN or None
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
            from config import YOUTUBE_GOOGLE_CLIENT_ID, YOUTUBE_GOOGLE_CLIENT_SECRET
            from youtube_upload import YOUTUBE_UPLOAD_SCOPE

            def perform_upload() -> tuple[str, str, Optional[str]]:
                credentials = Credentials(
                    token=None,
                    refresh_token=refresh_token,
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=YOUTUBE_GOOGLE_CLIENT_ID,
                    client_secret=YOUTUBE_GOOGLE_CLIENT_SECRET,
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
                "channel_matches_expected": channel_id == YOUTUBE_EXPECTED_CHANNEL_ID,
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

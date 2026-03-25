"""
YouTube Data API: upload completed task videos when NSFW poster rating is safe.

Requires one-time admin OAuth (refresh token in youtube_credentials).
Uses GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET and YOUTUBE_OAUTH_REDIRECT_URI from config.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
import uuid
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

import httpx
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    APP_URL,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    YOUTUBE_OAUTH_REDIRECT_URI,
    YOUTUBE_REFRESH_TOKEN,
    YOUTUBE_UPLOAD_PRIVACY,
)
from database import AsyncSessionLocal, Task, YoutubeCredentials

YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload"

# Fixed title for all auto-uploaded showcase videos
YOUTUBE_VIDEO_TITLE = "autorig character"


def _build_youtube_description(task_id: str, input_url: Optional[str]) -> str:
    """
    English description: service pitch, links to AutoRig and this task, optional source URL.
    YouTube max description length 5000.
    """
    base = APP_URL.rstrip("/")
    task_link = f"{base}/static/task.html?id={task_id}"
    parts: list[str] = [
        "AutoRig Online turns your 3D models into production-ready character rigs: automatic skeleton placement, skinning, and animations you can use in Unity, Unreal Engine, Blender, and other DCC tools.",
        "",
        f"Website: {base}",
        f"This task (3D viewer & downloads): {task_link}",
    ]
    if input_url and input_url.strip():
        parts.extend(["", f"Source model: {input_url.strip()}"])
    parts.extend(
        [
            "",
            "#3D #rigging #AutoRig #glb #animation #character",
        ]
    )
    text = "\n".join(parts)
    return text[:5000]


def _youtube_error_needs_new_oauth(exc: BaseException) -> bool:
    """True when Google indicates the refresh token must be re-issued (admin re-auth)."""
    if isinstance(exc, RefreshError):
        return True
    if isinstance(exc, HttpError):
        try:
            if exc.resp.status == 401:
                return True
        except Exception:
            pass
    s = str(exc).lower()
    if "invalid_grant" in s:
        return True
    if "token has been expired" in s or "token has been revoked" in s:
        return True
    return False


async def _telegram_youtube_token_notice(detail: str) -> None:
    try:
        from telegram_bot import broadcast_youtube_token_refresh_needed

        await broadcast_youtube_token_refresh_needed(detail)
    except Exception as e:
        print(f"[YouTube] Telegram notify failed: {e}")


def build_youtube_authorize_url(state: str) -> str:
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": YOUTUBE_OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": YOUTUBE_UPLOAD_SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return f"https://accounts.google.com/o/oauth2/auth?{urlencode(params)}"


async def exchange_youtube_code_for_tokens(code: str) -> Optional[dict]:
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "redirect_uri": YOUTUBE_OAUTH_REDIRECT_URI,
                    "grant_type": "authorization_code",
                },
                timeout=30.0,
            )
            if response.status_code == 200:
                return response.json()
            print(f"[YouTube OAuth] token exchange failed: {response.status_code} {response.text[:500]}")
            return None
        except Exception as e:
            print(f"[YouTube OAuth] token exchange error: {e}")
            return None


async def save_youtube_refresh_token(db: AsyncSession, refresh_token: str) -> None:
    row = await db.get(YoutubeCredentials, 1)
    now = datetime.utcnow()
    if row:
        row.refresh_token = refresh_token
        row.updated_at = now
    else:
        db.add(YoutubeCredentials(id=1, refresh_token=refresh_token, updated_at=now))
    await db.commit()


def _youtube_credentials_from_db(refresh_token: str) -> Credentials:
    return Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=[YOUTUBE_UPLOAD_SCOPE],
    )


def _upload_video_file_blocking(
    *,
    file_path: str,
    title: str,
    description: str,
    refresh_token: str,
) -> str:
    creds = _youtube_credentials_from_db(refresh_token)
    creds.refresh(Request())
    youtube = build("youtube", "v3", credentials=creds, cache_discovery=False)
    media = MediaFileUpload(
        file_path,
        chunksize=1024 * 1024 * 8,
        resumable=True,
        mimetype="video/*",
    )
    body = {
        "snippet": {
            "title": title[:100],
            "description": description[:5000],
            "categoryId": "28",
        },
        "status": {
            "privacyStatus": YOUTUBE_UPLOAD_PRIVACY,
            "selfDeclaredMadeForKids": False,
        },
    }
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            pass
    # response is dict with id
    vid = response.get("id") if isinstance(response, dict) else None
    if not vid:
        raise RuntimeError("YouTube API returned no video id")
    return vid


async def run_youtube_upload_for_task(task_id: str) -> None:
    async with AsyncSessionLocal() as db:
        task = await db.scalar(select(Task).where(Task.id == task_id))
        if not task or task.status != "done":
            return
        if task.content_classified_at is None:
            return
        if task.youtube_video_id:
            return

        if task.content_rating == "safe":
            pass
        elif task.content_rating == "unknown":
            if task.youtube_upload_status is None:
                task.youtube_upload_status = "skipped"
                task.youtube_upload_error = "content_rating_unknown"
                task.updated_at = datetime.utcnow()
                await db.commit()
            return
        else:
            if task.youtube_upload_status is None:
                task.youtube_upload_status = "skipped"
                task.youtube_upload_error = "content_rating_not_safe"
                task.updated_at = datetime.utcnow()
                await db.commit()
            return

        if not task.video_ready or not task.video_url:
            return

        cred_row = await db.get(YoutubeCredentials, 1)
        refresh_token = None
        if cred_row and cred_row.refresh_token:
            refresh_token = cred_row.refresh_token
        elif YOUTUBE_REFRESH_TOKEN:
            refresh_token = YOUTUBE_REFRESH_TOKEN
        if not refresh_token:
            print(f"[YouTube] No channel credentials (DB or YOUTUBE_REFRESH_TOKEN); skip task {task_id}")
            return

        video_url = task.video_url.strip()
        if not video_url:
            return

        title = YOUTUBE_VIDEO_TITLE
        desc = _build_youtube_description(task.id, task.input_url)

        tmp_path = None
        try:
            async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
                resp = await client.get(video_url)
                resp.raise_for_status()
                data = resp.content
            suffix = ".mp4"
            low = video_url.lower().split("?", 1)[0]
            if low.endswith(".webm"):
                suffix = ".webm"
            elif low.endswith(".mov"):
                suffix = ".mov"
            fd, tmp_path = tempfile.mkstemp(suffix=suffix)
            os.close(fd)
            with open(tmp_path, "wb") as f:
                f.write(data)

            video_id = await asyncio.to_thread(
                _upload_video_file_blocking,
                file_path=tmp_path,
                title=title,
                description=desc,
                refresh_token=refresh_token,
            )

            task.youtube_video_id = video_id
            task.youtube_upload_status = "uploaded"
            task.youtube_upload_error = None
            task.youtube_uploaded_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            await db.commit()
            print(f"[YouTube] Uploaded task {task_id} as video {video_id}")
        except RefreshError as e:
            print(f"[YouTube] OAuth refresh failed task {task_id}: {e}")
            await _telegram_youtube_token_notice(str(e))
            await _mark_failed(task_id, f"oauth_refresh:{e}")
        except HttpError as e:
            err = str(e)[:2000]
            print(f"[YouTube] Upload HttpError task {task_id}: {err}")
            if _youtube_error_needs_new_oauth(e):
                await _telegram_youtube_token_notice(err)
            await _mark_failed(task_id, err)
        except Exception as e:
            print(f"[YouTube] Upload failed task {task_id}: {e}")
            if _youtube_error_needs_new_oauth(e):
                await _telegram_youtube_token_notice(str(e))
            await _mark_failed(task_id, str(e)[:2000])
        finally:
            if tmp_path and os.path.isfile(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass


async def _mark_failed(task_id: str, message: str) -> None:
    async with AsyncSessionLocal() as db:
        task = await db.scalar(select(Task).where(Task.id == task_id))
        if not task:
            return
        task.youtube_upload_status = "failed"
        task.youtube_upload_error = message
        task.updated_at = datetime.utcnow()
        await db.commit()


def schedule_youtube_upload_if_eligible(task_id: str) -> None:
    asyncio.create_task(run_youtube_upload_for_task(task_id))

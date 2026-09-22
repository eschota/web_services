"""Reference pictures for the multi-image edit, wherever they come from.

Every reference socket on /nodes takes a picture or a video. A video stands for
its first frame: an LTX clip wired into an edit means "this shot", and asking
people to add a frame-grab node between the two is ceremony. The frame is cut
by the same cached, origin-checked derivation /api/vision and the Video first
frame node use (ai_video_reference), so a clip is fetched and decoded once
however many edits read it.
"""
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlsplit

import httpx
from fastapi import HTTPException

logger = logging.getLogger(__name__)

VIDEO_EXTENSIONS = (".mp4", ".webm", ".mov", ".m4v", ".mkv")
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp")


def _extension(url: str) -> str:
    path = urlsplit(url).path.lower()
    dot = path.rfind(".")
    return path[dot:] if dot >= 0 and "/" not in path[dot:] else ""


async def is_video(url: str, client: Optional[httpx.AsyncClient] = None) -> bool:
    """Whether a reference URL names a video rather than a picture.

    The extension settles almost every case (the farm publishes .mp4 and
    .png). Only an address with neither kind of extension is asked for its
    content type, and an answer that does not come is read as a picture — the
    render then fails on its own terms rather than here.
    """
    url = str(url or "").strip()
    if not url.startswith(("http://", "https://")):
        return False
    extension = _extension(url)
    if extension in VIDEO_EXTENSIONS:
        return True
    if extension in IMAGE_EXTENSIONS:
        return False
    owned = client is None
    client = client or httpx.AsyncClient()
    try:
        response = await client.head(url, timeout=10.0, follow_redirects=True)
        return str(response.headers.get("content-type") or "").lower().startswith("video/")
    except Exception:
        return False
    finally:
        if owned:
            await client.aclose()


async def first_frame(url: str) -> str:
    """The published first frame of a video, as a durable PNG URL."""
    import ai_video_reference
    from pydantic import ValidationError

    try:
        reference = await ai_video_reference.reference_for_video(url, "first_frame")
    except ValidationError as exc:
        message = "; ".join(str(item.get("msg") or "") for item in exc.errors()) or str(exc)
        raise HTTPException(status_code=400, detail={
            "error_string": "invalid_video_reference",
            "message_string": f"The wired video cannot be read: {message}"}) from None
    picture = str((reference or {}).get("image_url_string") or "")
    if not picture:
        raise HTTPException(status_code=502, detail={
            "error_string": "video_reference_failed",
            "message_string": "The first frame of the wired video could not be taken"})
    return picture


async def as_picture(url: str, client: Optional[httpx.AsyncClient] = None) -> str:
    """The URL itself for a picture, its first frame for a video."""
    url = str(url or "").strip()
    if url and await is_video(url, client):
        return await first_frame(url)
    return url

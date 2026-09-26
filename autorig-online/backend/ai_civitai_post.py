"""Post a /nodes output to the owner's Civitai account (owner, 2026-09-27).

Civitai has no public write API. The website itself uploads a picture through
``/api/v1/image-upload`` (a Cloudflare direct-upload URL) and builds the post
with tRPC mutations (``post.create``, ``post.addImage``, ``post.update``,
``post.addTag``). The server's CIVITAI_API_TOKEN (account NoDeadLine, full
scope) is accepted by those tRPC routes, so the backend can post directly.

The internal API can change without notice, so every failure answers with a
manual path instead: the file to download, the text to paste and the page to
open. ``dry_run`` returns the planned requests and sends nothing.

Pictures go through the API. Clips and audio are not uploaded by this module
yet (Civitai's video upload is a separate multipart flow); they get the manual
path, audio muxed onto a still first so Civitai accepts it.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

CIVITAI_HOST = os.getenv("CIVITAI_POST_HOST", "https://civitai.red").rstrip("/")
CREATE_PAGE = CIVITAI_HOST + "/posts/create"
NSFW_LEVELS = ("None", "Soft", "Mature", "X")
IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp")
VIDEO_SUFFIXES = (".mp4", ".webm", ".mov", ".m4v")
AUDIO_SUFFIXES = (".mp3", ".wav", ".flac", ".ogg", ".m4a")
SCRATCH_DIR = Path(os.getenv("AUTORIG_SCRATCH_DIR", "/srv/autorig/data/var/civitai-post"))


class CivitaiResource(BaseModel):
    model_version_id: int = Field(..., ge=1)
    name: str = ""


class CivitaiPostRequest(BaseModel):
    media_url: str = Field(..., max_length=4096)
    title: str = Field("", max_length=300)
    description: str = Field("", max_length=12000)
    prompt: str = Field("", max_length=12000)
    tags: List[str] = Field(default_factory=list, max_length=20)
    nsfw_level: str = Field(..., description="None, Soft, Mature or X; the owner confirms it")
    resources: List[CivitaiResource] = Field(default_factory=list, max_length=20)
    publish: bool = False
    dry_run: bool = False
    poster_url: Optional[str] = Field(None, max_length=4096, description="Still for audio")


def _token() -> str:
    return str(os.getenv("CIVITAI_API_TOKEN") or "").strip()


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json",
            "User-Agent": "AutoRigNodes/1.0 (+https://autorig.online/nodes)", "x-client": "web"}


def _kind(url: str) -> str:
    path = str(url or "").split("?", 1)[0].split("#", 1)[0].lower()
    if path.endswith(VIDEO_SUFFIXES):
        return "video"
    if path.endswith(AUDIO_SUFFIXES):
        return "audio"
    return "image"


def _manual(body: CivitaiPostRequest, reason: str, download_url: str) -> Dict[str, Any]:
    tags = ", ".join(tag.strip() for tag in body.tags if tag.strip())
    details = "\n".join(part for part in [
        body.title.strip(), "", body.description.strip(),
        ("Prompt: " + body.prompt.strip()) if body.prompt.strip() else "",
        ("Tags: " + tags) if tags else "",
        "Rating: " + body.nsfw_level,
        "Resources: " + ", ".join(f"{CIVITAI_HOST}/model-versions/{item.model_version_id}"
                                  for item in body.resources) if body.resources else "",
    ] if part is not None)
    return {"success_bool": False, "manual_bool": True, "reason_string": reason,
            "open_url_string": CREATE_PAGE, "download_url_string": download_url,
            "details_string": details.strip()}


async def _trpc(client: httpx.AsyncClient, procedure: str, payload: Dict[str, Any]) -> Any:
    response = await client.post(f"{CIVITAI_HOST}/api/trpc/{procedure}", headers=_headers(),
                                 json={"json": payload}, timeout=60.0)
    data = response.json() if response.content else {}
    if response.status_code >= 400 or "error" in data:
        message = ((data.get("error") or {}).get("json") or {}).get("message") or f"HTTP {response.status_code}"
        raise RuntimeError(f"{procedure}: {message}")
    return ((data.get("result") or {}).get("data") or {}).get("json")


async def _mux_audio(client: httpx.AsyncClient, audio_url: str, poster_url: Optional[str]) -> Path:
    """Audio on a still (the poster, or black) -> mp4 Civitai accepts."""
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="civ-", dir=SCRATCH_DIR))
    audio = work / ("audio" + Path(audio_url.split("?", 1)[0]).suffix)
    audio.write_bytes((await client.get(audio_url, timeout=120.0, follow_redirects=True)).content)
    out = work / "music.mp4"
    if poster_url:
        still = work / "poster.png"
        still.write_bytes((await client.get(poster_url, timeout=60.0, follow_redirects=True)).content)
        video_in = ["-loop", "1", "-i", str(still)]
    else:
        video_in = ["-f", "lavfi", "-i", "color=c=black:s=1280x720:r=24"]
    cmd = ["ffmpeg", "-loglevel", "error", "-y", *video_in, "-i", str(audio), "-shortest",
           "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2", "-c:v", "libx264", "-pix_fmt", "yuv420p",
           "-c:a", "aac", "-b:a", "192k", str(out)]
    process = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.PIPE)
    _out, err = await process.communicate()
    if process.returncode:
        shutil.rmtree(work, ignore_errors=True)
        raise RuntimeError("ffmpeg: " + err.decode("utf-8", "replace")[-300:])
    return out


def _public_scratch(path: Path) -> str:
    """A muxed file the owner can download for the manual path."""
    public = Path("/srv/autorig/current/autorig-online/static/tmp-civitai")
    try:
        public.mkdir(parents=True, exist_ok=True)
        name = uuid.uuid4().hex + path.suffix
        shutil.copy(path, public / name)
        return "https://autorig.online/static/tmp-civitai/" + name
    except Exception:
        return ""


async def _post_image(client: httpx.AsyncClient, body: CivitaiPostRequest,
                      local: Optional[Path] = None) -> Dict[str, Any]:
    if local is not None:
        content = local.read_bytes()
        name = local.name
        mime = "video/mp4"
    else:
        picture = await client.get(body.media_url, timeout=300.0, follow_redirects=True)
        picture.raise_for_status()
        content = picture.content
        name = Path(body.media_url.split("?", 1)[0]).name or "image.png"
        mime = picture.headers.get("content-type", "image/png").split(";")[0]
    is_video = mime.startswith("video/") or name.lower().endswith(VIDEO_SUFFIXES)
    width = height = 0
    try:
        from io import BytesIO
        from PIL import Image
        with Image.open(BytesIO(content)) as image:
            width, height = image.size
    except Exception:
        pass
    upload = await client.post(f"{CIVITAI_HOST}/api/v1/image-upload", headers=_headers(),
                               json={"filename": name, "metadata": {}}, timeout=60.0)
    if upload.status_code >= 400:
        raise RuntimeError(f"image-upload: HTTP {upload.status_code}")
    ticket = upload.json()
    target = ticket.get("uploadURL") or ticket.get("uploadUrl")
    image_id = ticket.get("id")
    if not target or not image_id:
        raise RuntimeError("image-upload: no upload URL")
    # The ticket is a presigned object-storage URL: it takes a PUT of the raw
    # bytes (a multipart POST answered 501). A Cloudflare Images direct-upload
    # URL takes the multipart POST, so that is the second try.
    sent = await client.put(target, content=content, headers={"Content-Type": mime}, timeout=180.0)
    if sent.status_code >= 400:
        retry = await client.post(target, files={"file": (name, content, mime)}, timeout=180.0)
        if retry.status_code >= 400:
            raise RuntimeError(f"upload: PUT HTTP {sent.status_code}, POST HTTP {retry.status_code}")
    first_version = body.resources[0].model_version_id if body.resources else None
    post = await _trpc(client, "post.create", {"modelVersionId": first_version} if first_version else {})
    post_id = int(post["id"])
    meta = {"prompt": body.prompt} if body.prompt.strip() else {}
    if body.resources:
        meta["civitaiResources"] = [{"modelVersionId": item.model_version_id} for item in body.resources]
    await _trpc(client, "post.addImage", {
        "postId": post_id, "url": image_id, "name": name, "width": width, "height": height,
        "hash": None, "meta": meta or None, "index": 0, "mimeType": mime,
        "type": "video" if is_video else "image"})
    for tag in [tag.strip() for tag in body.tags if tag.strip()][:10]:
        try:
            await _trpc(client, "post.addTag", {"id": post_id, "name": tag})
        except Exception as error:  # a tag is not worth failing the post
            logger.info("civitai tag %s: %s", tag, error)
    update: Dict[str, Any] = {"id": post_id, "title": body.title.strip() or None,
                              "detail": body.description.strip() or None}
    if body.publish:
        update["publishedAt"] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    await _trpc(client, "post.update", update)
    return {"success_bool": True, "post_id_int": post_id, "draft_bool": not body.publish,
            "post_url_string": f"{CIVITAI_HOST}/posts/{post_id}" + ("" if body.publish else "/edit")}


def build_civitai_post_router(require_admin) -> APIRouter:
    router = APIRouter()

    @router.post("/api/ai/civitai/post")
    async def api_civitai_post(body: CivitaiPostRequest, _admin=Depends(require_admin)):
        if body.nsfw_level not in NSFW_LEVELS:
            raise HTTPException(status_code=400, detail={
                "error_string": "rating_required",
                "message_string": "Confirm the rating: None, Soft, Mature or X"})
        if not body.media_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail={
                "error_string": "media_required", "message_string": "Nothing to post"})
        kind = _kind(body.media_url)
        if body.dry_run:
            return {"success_bool": True, "dry_run_bool": True, "kind_string": kind, "host_string": CIVITAI_HOST,
                    "token_present_bool": bool(_token()),
                    "steps_array": (["POST /api/v1/image-upload", "POST <uploadURL> (file)", "trpc post.create",
                                     "trpc post.addImage", "trpc post.addTag x" + str(len(body.tags)),
                                     "trpc post.update" + (" publishedAt" if body.publish else " (draft)")]
                                    if kind != "audio" else ["mux audio onto a still -> mp4", "then as a clip"])}
        async with httpx.AsyncClient() as client:
            download = body.media_url
            local = None
            if kind == "audio":
                try:
                    local = await _mux_audio(client, body.media_url, body.poster_url)
                    download = _public_scratch(local) or body.media_url
                except Exception as error:
                    logger.warning("civitai audio mux failed: %s", error)
                    return _manual(body, "The music could not be put on a still: " + str(error)[:120], download)
            if not _token():
                return _manual(body, "No Civitai token on the server", download)
            try:
                return await _post_image(client, body, local)
            except Exception as error:
                logger.warning("civitai post failed: %s", error)
                return _manual(body, "Civitai did not accept the automatic post (" + str(error)[:200] +
                               "); finish it by hand", download)

    @router.post("/api/ai/civitai/post/{post_id}/delete")
    async def api_civitai_delete(post_id: int, _admin=Depends(require_admin)):
        async with httpx.AsyncClient() as client:
            await _trpc(client, "post.delete", {"id": int(post_id)})
        return {"success_bool": True, "post_id_int": int(post_id)}

    return router

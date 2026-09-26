"""Civitai links as media inputs for every render endpoint (and /nodes).

People paste whatever Civitai shows them: an image page
(``https://civitai.com/images/143788350``, the ``civitai.red`` / ``civitai.green``
mirrors, ``/posts/<id>``) or a CDN address with a preview transform
(``https://image.civitai.com/<key>/<uuid>/transcode=true,width=450,optimized=true/x.webm``).
A page is HTML and a transform is a width-limited preview, so the render
endpoints either failed ("moov atom not found", "host is not allowed") or
worked on a 450 px copy.

``resolve(url)`` turns any of those into the original file on
``image.civitai.com/<key>/<uuid>/original=true/<name>`` (Civitai API with the
server's own token for pages; a path rewrite for CDN transforms). Anything that
is not Civitai comes back unchanged.

``CivitaiMediaMiddleware`` applies it to the media fields of JSON bodies posted
to ``/api/`` before the handlers (and their pydantic host checks) see them, so
every endpoint - vision, avatar build, avatar video, image, video, Qwen,
ControlNet, video reference - takes Civitai links without its own code. A page
that resolves to a video posted in ``image_url`` moves to ``video_url`` when
the body has none (Vision and Avatar builder read a clip from there).

``GET /api/ai/media/resolve?url=`` exposes the same resolution to the editor and
to agents. The token is read from ``CIVITAI_API_TOKEN`` and never leaves the
server: responses carry only public CDN addresses.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

_PAGE_HOST = re.compile(r"(?:www\.)?civitai\.(?:com|red|green)", re.IGNORECASE)
_CDN_HOST = re.compile(r"image\.civitai\.(?:com|red|green)", re.IGNORECASE)
_IMAGE_PAGE = re.compile(r"^/images/(\d{1,12})(?:/|$)")
_POST_PAGE = re.compile(r"^/posts/(\d{1,12})(?:/|$)")
_CDN_PATH = re.compile(r"^/([A-Za-z0-9_-]+)/([0-9a-fA-F-]{36})/([^/]+)/([^/]+)$")
_VIDEO_EXT = re.compile(r"\.(mp4|webm|mov|m4v)$", re.IGNORECASE)
MEDIA_FIELDS = ("image_url", "video_url", "control_video_url", "image_url_end", "reference_image_urls",
                "reference_urls", "source_url")
_SKIP_PREFIXES = ("/api/ai/graphs", "/api/ai/media/resolve")
_MAX_BODY = 2 * 1024 * 1024
_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_CACHE_SECONDS = 3600.0


class CivitaiResolveError(ValueError):
    pass


def is_civitai(url: str) -> bool:
    host = (urlsplit(str(url or "").strip()).hostname or "").rstrip(".").lower()
    return bool(_PAGE_HOST.fullmatch(host) or _CDN_HOST.fullmatch(host))


def _headers() -> Dict[str, str]:
    token = str(os.getenv("CIVITAI_API_TOKEN") or "").strip()
    headers = {"User-Agent": "AutoRigMediaResolver/1.0 (+https://autorig.online/nodes)"}
    if token and all(33 <= ord(ch) <= 126 for ch in token):
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _original_cdn(url: str) -> Optional[str]:
    """image.civitai.* with any transform segment -> the same file at original=true."""
    parts = urlsplit(url)
    match = _CDN_PATH.match(parts.path)
    if not match:
        return None
    key, uid, _params, name = match.groups()
    return f"https://image.civitai.com/{key}/{uid}/original=true/{name}"


async def _api_items(client: httpx.AsyncClient, params: Dict[str, Any]) -> list:
    response = await client.get("https://civitai.com/api/v1/images",
                                params=dict(params, nsfw="X", limit=1), headers=_headers(), timeout=30.0)
    if response.status_code >= 400:
        raise CivitaiResolveError(f"Civitai answered HTTP {response.status_code}")
    return (response.json() or {}).get("items") or []


async def resolve(url: str, client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
    """Return {"url", "type": image|video|unknown, "source", "changed_bool"} for one link."""
    source = str(url or "").strip()
    if not is_civitai(source):
        return {"url": source, "type": "unknown", "source": source, "changed_bool": False}
    cached = _CACHE.get(source)
    if cached and time.time() - cached[0] < _CACHE_SECONDS:
        return cached[1]
    parts = urlsplit(source)
    host = (parts.hostname or "").lower()
    own_client = client is None
    client = client or httpx.AsyncClient(follow_redirects=True)
    try:
        if _CDN_HOST.fullmatch(host):
            resolved = _original_cdn(source) or source
            kind = "video" if _VIDEO_EXT.search(resolved) else "image"
            result = {"url": resolved, "type": kind, "source": source, "changed_bool": resolved != source}
        else:
            image_page = _IMAGE_PAGE.match(parts.path)
            post_page = _POST_PAGE.match(parts.path)
            if image_page:
                items = await _api_items(client, {"imageId": int(image_page.group(1))})
            elif post_page:
                items = await _api_items(client, {"postId": int(post_page.group(1))})
            else:
                raise CivitaiResolveError("only civitai /images/<id> and /posts/<id> pages carry media; "
                                          "for a model, open one of its images and paste that link")
            if not items:
                raise CivitaiResolveError("Civitai has no such image, or it is hidden from the API")
            item = items[0]
            media = str(item.get("url") or "")
            if not media.startswith("https://"):
                raise CivitaiResolveError("Civitai returned no media address")
            media = _original_cdn(media) or media
            kind = str(item.get("type") or "").lower()
            kind = kind if kind in ("image", "video") else ("video" if _VIDEO_EXT.search(media) else "image")
            result = {"url": media, "type": kind, "source": source, "changed_bool": True,
                      "width_int": item.get("width"), "height_int": item.get("height")}
    finally:
        if own_client:
            await client.aclose()
    _CACHE[source] = (time.time(), result)
    if len(_CACHE) > 2048:
        for stale in sorted(_CACHE, key=lambda k: _CACHE[k][0])[:512]:
            _CACHE.pop(stale, None)
    return result


async def rewrite_body(body: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Resolve every Civitai link in the media fields of one request body."""
    changed = False
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for field in MEDIA_FIELDS:
            value = body.get(field)
            if isinstance(value, str) and is_civitai(value):
                result = await resolve(value, client)
                body[field] = result["url"]
                changed = True
                if field == "image_url" and result["type"] == "video" and not body.get("video_url"):
                    body["video_url"] = body.pop("image_url")
            elif isinstance(value, list):
                out = []
                for item in value:
                    if isinstance(item, str) and is_civitai(item):
                        item = (await resolve(item, client))["url"]
                        changed = True
                    out.append(item)
                body[field] = out
    return body, changed


class CivitaiMediaMiddleware:
    """Pure ASGI: rewrites Civitai media links in JSON bodies posted to /api/."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if (scope.get("type") != "http" or scope.get("method") not in ("POST", "PUT")
                or not str(scope.get("path") or "").startswith("/api/")
                or str(scope.get("path") or "").startswith(_SKIP_PREFIXES)):
            return await self.app(scope, receive, send)
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers") or []}
        if "application/json" not in headers.get("content-type", ""):
            return await self.app(scope, receive, send)
        chunks, size, more = [], 0, True
        while more:
            message = await receive()
            if message["type"] != "http.request":
                return await self.app(scope, _replay([message]), send)
            chunk = message.get("body", b"")
            size += len(chunk)
            chunks.append(chunk)
            more = message.get("more_body", False)
            if size > _MAX_BODY:
                break
        raw = b"".join(chunks)
        if size > _MAX_BODY or b"civitai." not in raw:
            return await self.app(scope, _replay_body(raw, more, receive), send)
        try:
            body = json.loads(raw.decode("utf-8"))
            if isinstance(body, dict):
                body, changed = await rewrite_body(body)
                if changed:
                    raw = json.dumps(body).encode("utf-8")
                    scope = dict(scope)
                    scope["headers"] = [(k, v) for k, v in scope.get("headers") or []
                                        if k.lower() != b"content-length"]
                    scope["headers"].append((b"content-length", str(len(raw)).encode("ascii")))
        except CivitaiResolveError as error:
            return await _json_error(send, 400, "civitai_link_unresolved", str(error))
        except Exception:
            logger.exception("civitai media rewrite failed; passing the body through unchanged")
        return await self.app(scope, _replay_body(raw, False, receive), send)


def _replay(messages):
    queue = list(messages)

    async def receive():
        return queue.pop(0) if queue else {"type": "http.disconnect"}
    return receive


def _replay_body(raw: bytes, more: bool, upstream):
    sent = {"done": False}

    async def receive():
        if not sent["done"]:
            sent["done"] = True
            return {"type": "http.request", "body": raw, "more_body": more}
        if more:
            return await upstream()
        return await upstream()
    return receive


async def _json_error(send, status: int, code: str, message: str):
    payload = json.dumps({"detail": {"error_string": code, "message_string": message}}).encode("utf-8")
    await send({"type": "http.response.start", "status": status,
                "headers": [(b"content-type", b"application/json"),
                            (b"content-length", str(len(payload)).encode("ascii"))]})
    await send({"type": "http.response.body", "body": payload})


router = APIRouter()


@router.get("/api/ai/media/resolve")
async def resolve_media(url: str = Query(..., max_length=2048)):
    """A Civitai page or CDN preview -> the original media file (other links unchanged)."""
    try:
        result = await resolve(url)
    except CivitaiResolveError as error:
        raise HTTPException(status_code=400, detail={"error_string": "civitai_link_unresolved",
                                                     "message_string": str(error)})
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail={"error_string": "civitai_unreachable",
                                                     "message_string": type(error).__name__})
    return dict(result, success_bool=True)

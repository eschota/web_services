"""Durable original image assets used by AI avatar profiles."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import secrets
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, field_validator


DEFAULT_ASSET_DIR = Path(
    os.getenv("AUTORIG_AI_AVATAR_ASSET_DIR", "/srv/autorig/data/var/ai-avatar-assets")
)
DEFAULT_PUBLIC_BASE_URL = os.getenv("AUTORIG_PUBLIC_URL", "https://autorig.online").rstrip("/")
MAX_ASSET_BYTES = 12 * 1024 * 1024
MAX_ASSETS_PER_OWNER = 100
ASSET_ID_RE = re.compile(r"^[a-f0-9]{32}$")
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")

_FORMATS = {
    "PNG": ("png", "image/png", {".png"}),
    "JPEG": ("jpg", "image/jpeg", {".jpg", ".jpeg"}),
    "WEBP": ("webp", "image/webp", {".webp"}),
}
_ALLOWED_IMPORT_PREFIXES = (
    "/dev/api/scratch/",
    "/renderfin/render/",
    "/api/ai/avatar-assets/",
)


class ImportAssetRequest(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return validate_import_url(value)


def validate_import_url(value: str) -> str:
    if not value or len(value) > 2048:
        raise ValueError("url must be between 1 and 2048 characters")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "autorig.online"
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError("only trusted HTTPS autorig.online asset URLs are allowed")
    if not any(parsed.path.startswith(prefix) for prefix in _ALLOWED_IMPORT_PREFIXES):
        raise ValueError("url path is not an allowed AutoRig asset path")
    return value


def _owner_key(owner: Any) -> str:
    if isinstance(owner, Mapping):
        owner_type = owner.get("owner_type")
        owner_id = owner.get("owner_id")
    else:
        owner_type = getattr(owner, "owner_type", None)
        owner_id = getattr(owner, "owner_id", None)
    if owner_type not in ("user", "anon") or not owner_id:
        raise HTTPException(status_code=500, detail="Avatar asset owner is not resolved")
    return f"{owner_type}:{owner_id}"


def _inspect_image(data: bytes) -> tuple[str, str, int, int]:
    try:
        with Image.open(io.BytesIO(data)) as image:
            image.verify()
        with Image.open(io.BytesIO(data)) as image:
            image.load()
            actual_format = str(image.format or "").upper()
            width, height = image.size
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError) as exc:
        raise HTTPException(status_code=400, detail="File is not a valid PNG, JPEG or WebP image") from exc
    if actual_format not in _FORMATS:
        raise HTTPException(status_code=400, detail="Unsupported image format; use PNG, JPEG or WebP")
    if width < 1 or height < 1 or width > 16384 or height > 16384:
        raise HTTPException(status_code=400, detail="Image dimensions are outside the supported range")
    extension, mime_type, _ = _FORMATS[actual_format]
    return extension, mime_type, width, height


def _check_declared_type(filename: str, content_type: str | None, extension: str, mime_type: str) -> None:
    suffix = Path(filename or "").suffix.lower()
    allowed_suffixes = _FORMATS[{"png": "PNG", "jpg": "JPEG", "webp": "WEBP"}[extension]][2]
    if suffix and suffix not in allowed_suffixes:
        raise HTTPException(status_code=400, detail="Filename extension does not match image bytes")
    if content_type and content_type.lower() not in (mime_type, "application/octet-stream"):
        raise HTTPException(status_code=400, detail="Content-Type does not match image bytes")


class AvatarAssetStore:
    def __init__(
        self,
        root: str | os.PathLike[str] = DEFAULT_ASSET_DIR,
        *,
        public_base_url: str = DEFAULT_PUBLIC_BASE_URL,
        max_assets_per_owner: int = MAX_ASSETS_PER_OWNER,
    ) -> None:
        self.root = Path(root)
        self.public_base_url = public_base_url.rstrip("/")
        self.max_assets_per_owner = max_assets_per_owner
        self.assets_dir = self.root / "assets"
        self.owners_dir = self.root / "owners"
        self.assets_dir.mkdir(parents=True, exist_ok=True)
        self.owners_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def _owner_index_path(self, owner_key: str) -> Path:
        digest = hashlib.sha256(owner_key.encode("utf-8")).hexdigest()
        return self.owners_dir / f"{digest}.json"

    @staticmethod
    def _read_index(path: Path) -> dict[str, str]:
        if not path.exists():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=".index-", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(value, handle, ensure_ascii=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def put_bytes(
        self,
        owner: Any,
        data: bytes,
        *,
        filename: str = "",
        content_type: str | None = None,
        source_url: str | None = None,
    ) -> dict[str, Any]:
        if not data:
            raise HTTPException(status_code=400, detail="Image file is empty")
        if len(data) > MAX_ASSET_BYTES:
            raise HTTPException(status_code=413, detail="Image too large (maximum 12 MB)")
        extension, mime_type, width, height = _inspect_image(data)
        _check_declared_type(filename, content_type, extension, mime_type)
        digest = hashlib.sha256(data).hexdigest()
        owner_key = _owner_key(owner)
        index_path = self._owner_index_path(owner_key)

        with self._lock:
            index = self._read_index(index_path)
            existing_id = index.get(digest)
            if existing_id:
                existing = self.get_metadata(existing_id)
                if existing:
                    return existing
                index.pop(digest, None)
            if len(index) >= self.max_assets_per_owner:
                raise HTTPException(
                    status_code=409,
                    detail=f"Avatar asset quota reached ({self.max_assets_per_owner} assets)",
                )

            asset_id = secrets.token_hex(16)
            asset_dir = self.assets_dir / asset_id
            asset_dir.mkdir(mode=0o700)
            image_path = asset_dir / f"original.{extension}"
            fd, temp_name = tempfile.mkstemp(prefix=".upload-", suffix=".tmp", dir=asset_dir)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, image_path)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)

            canonical_url = (
                f"{self.public_base_url}/api/ai/avatar-assets/{asset_id}/{digest}.{extension}"
            )
            metadata = {
                "asset_id": asset_id,
                "role": "face",
                "media_type": "image",
                "canonical_url": canonical_url,
                "sha256": digest,
                "width": width,
                "height": height,
                "source_url": source_url,
                "mime_type": mime_type,
                "extension": extension,
            }
            self._atomic_json(asset_dir / "metadata.json", metadata)
            index[digest] = asset_id
            self._atomic_json(index_path, index)
            return metadata

    def get_metadata(self, asset_id: str) -> dict[str, Any] | None:
        if not ASSET_ID_RE.fullmatch(asset_id):
            return None
        path = self.assets_dir / asset_id / "metadata.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def resolve_capability(self, asset_id: str, sha256: str, extension: str) -> tuple[Path, str]:
        if not ASSET_ID_RE.fullmatch(asset_id) or not SHA256_RE.fullmatch(sha256):
            raise HTTPException(status_code=404, detail="Avatar asset not found")
        metadata = self.get_metadata(asset_id)
        if (
            not metadata
            or metadata.get("sha256") != sha256
            or metadata.get("extension") != extension
        ):
            raise HTTPException(status_code=404, detail="Avatar asset not found")
        path = self.assets_dir / asset_id / f"original.{extension}"
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Avatar asset not found")
        return path, str(metadata["mime_type"])


async def _read_upload(file: UploadFile) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        size += len(chunk)
        if size > MAX_ASSET_BYTES:
            raise HTTPException(status_code=413, detail="Image too large (maximum 12 MB)")
        chunks.append(chunk)
    return b"".join(chunks)


def _public_reference(metadata: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: metadata.get(key)
        for key in (
            "asset_id",
            "role",
            "media_type",
            "canonical_url",
            "sha256",
            "width",
            "height",
            "source_url",
        )
    }


def build_avatar_asset_router(
    owner_dependency: Callable,
    *,
    store: AvatarAssetStore | None = None,
    http_client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> APIRouter:
    asset_store = store or AvatarAssetStore()
    client_factory = http_client_factory or (
        lambda: httpx.AsyncClient(follow_redirects=False, timeout=httpx.Timeout(30.0))
    )
    router = APIRouter()

    @router.post("/api/ai/avatar-assets")
    async def upload_avatar_asset(
        file: UploadFile = File(...), owner: Any = Depends(owner_dependency)
    ) -> dict[str, Any]:
        data = await _read_upload(file)
        metadata = asset_store.put_bytes(
            owner,
            data,
            filename=file.filename or "",
            content_type=file.content_type,
            source_url=None,
        )
        return _public_reference(metadata)

    @router.post("/api/ai/avatar-assets/import")
    async def import_avatar_asset(
        request: ImportAssetRequest, owner: Any = Depends(owner_dependency)
    ) -> dict[str, Any]:
        url = validate_import_url(request.url)
        chunks: list[bytes] = []
        size = 0
        async with client_factory() as client:
            async with client.stream("GET", url) as response:
                if 300 <= response.status_code < 400:
                    raise HTTPException(status_code=400, detail="Redirects are not allowed")
                if response.status_code != 200:
                    raise HTTPException(status_code=400, detail="Source image could not be fetched")
                declared = response.headers.get("content-length")
                if declared:
                    try:
                        declared_size = int(declared)
                    except ValueError:
                        declared_size = 0
                    if declared_size > MAX_ASSET_BYTES:
                        raise HTTPException(status_code=413, detail="Image too large (maximum 12 MB)")
                async for chunk in response.aiter_bytes(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_ASSET_BYTES:
                        raise HTTPException(status_code=413, detail="Image too large (maximum 12 MB)")
                    chunks.append(chunk)
                content_type = response.headers.get("content-type", "").split(";", 1)[0]
        filename = Path(urlsplit(url).path).name
        metadata = asset_store.put_bytes(
            owner,
            b"".join(chunks),
            filename=filename,
            content_type=content_type,
            source_url=url,
        )
        return _public_reference(metadata)

    @router.get("/api/ai/avatar-assets/{asset_id}/{sha256}.{extension}")
    async def fetch_avatar_asset(asset_id: str, sha256: str, extension: str):
        path, mime_type = asset_store.resolve_capability(asset_id, sha256, extension)
        return FileResponse(
            path,
            media_type=mime_type,
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    return router

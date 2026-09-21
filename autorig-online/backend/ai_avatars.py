"""Durable, private and versioned identity profiles for AI productions.

An Avatar is production metadata, not a trained model.  It points at immutable
reference assets and records whether an optional adapter actually exists.  The
store deliberately lives outside release directories so a deployment cannot
erase a producer's cast.

``build_avatar_router`` takes the application's existing identity dependency.
That keeps session and API-key policy in ``main.py`` and avoids inventing a
second anonymous cookie here.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import pathlib
import re
import secrets
import tempfile
import threading
import time
from typing import Callable, Dict, List, Literal, Optional
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

AVATAR_DIR = pathlib.Path(
    os.getenv("AUTORIG_AI_AVATAR_DIR", "/srv/autorig/data/var/ai-avatars")
)
AVATAR_ID_RE = re.compile(r"^av_[a-f0-9]{24}$")
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
MAX_AVATARS_PER_OWNER = 100
MAX_PROFILE_BYTES = 256 * 1024


class AvatarOwner(BaseModel):
    """Identity resolved by the host application's normal auth dependency."""

    owner_type: Literal["user", "anon"]
    owner_id: str = Field(min_length=1, max_length=320)


async def avatar_owner_dependency() -> AvatarOwner:
    """Integration seam overridden by ``main.py`` with its normal identity.

    Keeping a fail-closed exported dependency also lets avatar-aware generation
    routes depend on exactly the same owner resolver as the registry routes.
    """

    raise HTTPException(status_code=500, detail={
        "error_string": "avatar_identity_not_configured",
        "message_string": "Avatar identity resolver is not configured",
    })


def _safe_https_url(value: Optional[str], *, field_name: str) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    if not value or len(value) > 2048:
        raise ValueError(f"{field_name} must be a non-empty URL of at most 2048 characters")
    parsed = urlsplit(value)
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"{field_name} must be an HTTPS URL without credentials")
    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        raise ValueError(f"{field_name} may not name a local host")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address and not address.is_global:
        raise ValueError(f"{field_name} may not name a private or local address")
    return value


class AvatarReference(BaseModel):
    """An immutable reference asset already promoted to durable storage.

    ``source_url`` preserves provenance.  Generation consumes ``canonical_url``;
    callers must copy scratch/expiring uploads to their persistent asset store
    before creating the profile and provide the checksum of those exact bytes.
    """

    asset_id: Optional[str] = Field(default=None, min_length=4, max_length=128)
    role: Literal["face", "body", "wardrobe", "style", "motion", "voice"]
    media_type: Literal["image", "video", "audio"]
    canonical_url: str
    sha256: str
    source_url: Optional[str] = None
    width: Optional[int] = Field(default=None, ge=1, le=16384)
    height: Optional[int] = Field(default=None, ge=1, le=16384)
    duration_seconds: Optional[float] = Field(default=None, gt=0, le=3600)
    note: str = Field(default="", max_length=500)

    @field_validator("canonical_url")
    @classmethod
    def validate_canonical_url(cls, value: str) -> str:
        return str(_safe_https_url(value, field_name="canonical_url"))

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: Optional[str]) -> Optional[str]:
        return _safe_https_url(value, field_name="source_url")

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        value = value.strip().lower()
        if not SHA256_RE.fullmatch(value):
            raise ValueError("sha256 must contain exactly 64 hexadecimal characters")
        return value

    @field_validator("asset_id")
    @classmethod
    def validate_asset_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{4,128}", value):
            raise ValueError("asset_id must contain only letters, digits, _ or -")
        return value

    @model_validator(mode="after")
    def validate_media_metadata(self):
        if self.media_type == "image" and (self.width is None or self.height is None):
            raise ValueError("image references require width and height")
        if self.media_type in {"video", "audio"} and self.duration_seconds is None:
            raise ValueError(f"{self.media_type} references require duration_seconds")
        return self


class AvatarAdapter(BaseModel):
    """Truthful state of an optional identity adapter."""

    status: Literal[
        "not_requested", "candidate", "training", "ready", "failed", "retired"
    ] = "not_requested"
    pipeline_family: Optional[str] = Field(default=None, max_length=100)
    artifact_url: Optional[str] = None
    sha256: Optional[str] = None
    trigger_token: Optional[str] = Field(default=None, max_length=100)
    note: str = Field(default="", max_length=1000)

    @field_validator("artifact_url")
    @classmethod
    def validate_artifact_url(cls, value: Optional[str]) -> Optional[str]:
        return _safe_https_url(value, field_name="artifact_url")

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip().lower()
        if not SHA256_RE.fullmatch(value):
            raise ValueError("adapter sha256 must contain exactly 64 hexadecimal characters")
        return value

    @model_validator(mode="after")
    def validate_ready_adapter(self):
        if self.status == "ready":
            if not self.pipeline_family or not self.artifact_url or not self.sha256:
                raise ValueError(
                    "a ready adapter requires pipeline_family, artifact_url and sha256"
                )
        return self


class AvatarProvenance(BaseModel):
    source_kind: Literal["uploaded", "generated", "mixed"] = "uploaded"
    source_task_ids: List[str] = Field(default_factory=list, max_length=32)
    source_model: str = Field(default="", max_length=200)
    source_workflow: str = Field(default="", max_length=200)
    parameters_sha256: Optional[str] = None
    note: str = Field(default="", max_length=1000)

    @field_validator("source_task_ids")
    @classmethod
    def validate_task_ids(cls, values: List[str]) -> List[str]:
        clean = []
        for value in values:
            value = str(value).strip()
            if not value or len(value) > 200:
                raise ValueError("source task ids must be 1-200 characters")
            clean.append(value)
        return clean

    @field_validator("parameters_sha256")
    @classmethod
    def validate_parameters_sha256(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip().lower()
        if not SHA256_RE.fullmatch(value):
            raise ValueError("parameters_sha256 must be a SHA-256 hex digest")
        return value


class AvatarDraft(BaseModel):
    display_name: str = Field(min_length=1, max_length=120)
    identity_prompt: str = Field(min_length=1, max_length=4000)
    appearance: str = Field(default="", max_length=4000)
    wardrobe: str = Field(default="", max_length=4000)
    negative_identity_prompt: str = Field(default="", max_length=2000)
    references: List[AvatarReference] = Field(min_length=1, max_length=12)
    provenance: AvatarProvenance = Field(default_factory=AvatarProvenance)
    adapter: AvatarAdapter = Field(default_factory=AvatarAdapter)

    @field_validator("display_name", "identity_prompt")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value may not be blank")
        return value

    @model_validator(mode="after")
    def validate_identity_material(self):
        if not any(ref.role in {"face", "body"} and ref.media_type in {"image", "video"}
                   for ref in self.references):
            raise ValueError("an Avatar requires at least one face or body image/video")
        keys = [(ref.canonical_url, ref.sha256, ref.role) for ref in self.references]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate Avatar references are not allowed")
        return self


class AvatarVersion(AvatarDraft):
    avatar_id: str
    version: int = Field(ge=1)
    created_at_unix_int: int = Field(ge=1)


class AvatarSummary(BaseModel):
    avatar_id: str
    display_name: str
    current_version: int
    created_at_unix_int: int
    updated_at_unix_int: int
    adapter_status: str
    reference_count: int


class AvatarStoreError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class AvatarStore:
    def __init__(self, root: pathlib.Path = AVATAR_DIR):
        self.root = pathlib.Path(root)
        self._lock = threading.RLock()

    def _path(self, avatar_id: str) -> pathlib.Path:
        if not AVATAR_ID_RE.fullmatch(str(avatar_id or "")):
            raise AvatarStoreError("bad_avatar_id", "Invalid Avatar id", 400)
        return self.root / f"{avatar_id}.json"

    @staticmethod
    def _owns(document: Dict[str, object], owner: AvatarOwner) -> bool:
        return (
            document.get("owner_type") == owner.owner_type
            and document.get("owner_id") == owner.owner_id
        )

    def _read(self, avatar_id: str) -> Dict[str, object]:
        path = self._path(avatar_id)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise AvatarStoreError("avatar_not_found", "Avatar not found", 404) from None
        except Exception:
            logger.exception("Could not read Avatar %s", avatar_id)
            raise AvatarStoreError("avatar_unreadable", "Avatar profile is unreadable", 500) from None
        if not isinstance(document, dict) or document.get("avatar_id") != avatar_id:
            raise AvatarStoreError("avatar_unreadable", "Avatar profile is unreadable", 500)
        return document

    def _read_owned(self, avatar_id: str, owner: AvatarOwner) -> Dict[str, object]:
        document = self._read(avatar_id)
        # A caller must not be able to distinguish somebody else's opaque id
        # from an id that does not exist.
        if not self._owns(document, owner):
            raise AvatarStoreError("avatar_not_found", "Avatar not found", 404)
        return document

    def _atomic_write(self, path: pathlib.Path, document: Dict[str, object]) -> None:
        payload = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
        if len(payload.encode("utf-8")) > MAX_PROFILE_BYTES:
            raise AvatarStoreError("avatar_too_large", "Avatar profile is too large", 400)
        self.root.mkdir(parents=True, exist_ok=True)
        tmp_name = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.root,
                prefix=f".{path.stem}.", suffix=".tmp", delete=False,
            ) as handle:
                tmp_name = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
        except AvatarStoreError:
            raise
        except Exception:
            logger.exception("Could not persist Avatar %s", path.stem)
            raise AvatarStoreError("avatar_not_saved", "Avatar profile was not saved", 500) from None
        finally:
            if tmp_name:
                try:
                    pathlib.Path(tmp_name).unlink(missing_ok=True)
                except Exception:
                    pass

    @staticmethod
    def _version(avatar_id: str, number: int, now: int, draft: AvatarDraft) -> Dict[str, object]:
        return AvatarVersion(
            avatar_id=avatar_id,
            version=number,
            created_at_unix_int=now,
            **draft.model_dump(),
        ).model_dump()

    def create(self, owner: AvatarOwner, draft: AvatarDraft) -> AvatarVersion:
        with self._lock:
            if len(self.list(owner)) >= MAX_AVATARS_PER_OWNER:
                raise AvatarStoreError("avatar_limit_reached", "Avatar profile limit reached", 400)
            now = int(time.time())
            while True:
                avatar_id = "av_" + secrets.token_hex(12)
                path = self._path(avatar_id)
                if not path.exists():
                    break
            version = self._version(avatar_id, 1, now, draft)
            document: Dict[str, object] = {
                "schema_version": 1,
                "avatar_id": avatar_id,
                "owner_type": owner.owner_type,
                "owner_id": owner.owner_id,
                "created_at_unix_int": now,
                "updated_at_unix_int": now,
                "current_version": 1,
                "versions": [version],
            }
            self._atomic_write(path, document)
            return AvatarVersion.model_validate(version)

    def update(self, avatar_id: str, owner: AvatarOwner, draft: AvatarDraft) -> AvatarVersion:
        with self._lock:
            document = self._read_owned(avatar_id, owner)
            versions = document.get("versions")
            if not isinstance(versions, list) or not versions:
                raise AvatarStoreError("avatar_unreadable", "Avatar profile is unreadable", 500)
            number = int(document.get("current_version") or 0) + 1
            now = max(int(time.time()), int(document.get("updated_at_unix_int") or 0))
            version = self._version(avatar_id, number, now, draft)
            versions.append(version)
            document["current_version"] = number
            document["updated_at_unix_int"] = now
            self._atomic_write(self._path(avatar_id), document)
            return AvatarVersion.model_validate(version)

    def get(self, avatar_id: str, owner: AvatarOwner, version: Optional[int] = None) -> AvatarVersion:
        with self._lock:
            document = self._read_owned(avatar_id, owner)
            number = int(version or document.get("current_version") or 0)
            if number < 1:
                raise AvatarStoreError("bad_avatar_version", "Invalid Avatar version", 400)
            for item in document.get("versions") or []:
                if int((item or {}).get("version") or 0) == number:
                    return AvatarVersion.model_validate(item)
            raise AvatarStoreError("avatar_version_not_found", "Avatar version not found", 404)

    def list(self, owner: AvatarOwner) -> List[AvatarSummary]:
        summaries: List[AvatarSummary] = []
        if not self.root.is_dir():
            return summaries
        with self._lock:
            for path in self.root.glob("av_*.json"):
                try:
                    document = json.loads(path.read_text(encoding="utf-8"))
                    if not isinstance(document, dict) or not self._owns(document, owner):
                        continue
                    number = int(document.get("current_version") or 0)
                    item = next(
                        version for version in document.get("versions") or []
                        if int((version or {}).get("version") or 0) == number
                    )
                    summaries.append(AvatarSummary(
                        avatar_id=str(document["avatar_id"]),
                        display_name=str(item["display_name"]),
                        current_version=number,
                        created_at_unix_int=int(document["created_at_unix_int"]),
                        updated_at_unix_int=int(document["updated_at_unix_int"]),
                        adapter_status=str((item.get("adapter") or {}).get("status") or "not_requested"),
                        reference_count=len(item.get("references") or []),
                    ))
                except Exception:
                    logger.warning("Skipping unreadable Avatar index entry %s", path)
        return sorted(summaries, key=lambda item: (-item.updated_at_unix_int, item.avatar_id))


def resolve_avatar_identity(
    store: AvatarStore,
    avatar_id: str,
    owner: AvatarOwner,
    *,
    version: Optional[int] = None,
    pipeline_family: Optional[str] = None,
    require_adapter: bool = False,
) -> AvatarVersion:
    """Resolve one exact identity version for a generation request.

    Reference conditioning is always available to a compatible workflow.  An
    adapter is used only when explicitly required and proven ``ready`` for the
    requested family; candidate/training states never masquerade as usable.
    """

    profile = store.get(avatar_id, owner, version)
    adapter = profile.adapter
    if require_adapter:
        if adapter.status != "ready":
            raise AvatarStoreError(
                "avatar_adapter_not_ready", "Avatar adapter is not ready", 409
            )
        if pipeline_family and adapter.pipeline_family != pipeline_family:
            raise AvatarStoreError(
                "avatar_adapter_incompatible",
                f"Avatar adapter is for '{adapter.pipeline_family}', not '{pipeline_family}'",
                409,
            )
    return profile


def _http_error(error: AvatarStoreError) -> HTTPException:
    return HTTPException(
        status_code=error.status_code,
        detail={"error_string": error.code, "message_string": error.message},
    )


def build_avatar_router(
    owner_dependency: Callable = avatar_owner_dependency,
    *,
    store: Optional[AvatarStore] = None,
) -> APIRouter:
    """Build routes using the host's existing authenticated/anonymous owner."""

    avatar_store = store or AvatarStore()
    router = APIRouter()

    @router.get("/api/ai/avatars")
    async def api_avatar_list(owner: AvatarOwner = Depends(owner_dependency)):
        return {"success_bool": True, "avatars_array": [
            item.model_dump() for item in avatar_store.list(owner)
        ]}

    @router.post("/api/ai/avatars", status_code=201)
    async def api_avatar_create(
        body: AvatarDraft,
        owner: AvatarOwner = Depends(owner_dependency),
    ):
        try:
            profile = avatar_store.create(owner, body)
        except AvatarStoreError as error:
            raise _http_error(error) from None
        return {"success_bool": True, "avatar_object": profile.model_dump()}

    @router.get("/api/ai/avatars/{avatar_id}")
    async def api_avatar_read(
        avatar_id: str,
        version: Optional[int] = None,
        owner: AvatarOwner = Depends(owner_dependency),
    ):
        try:
            profile = avatar_store.get(avatar_id, owner, version)
        except AvatarStoreError as error:
            raise _http_error(error) from None
        return {"success_bool": True, "avatar_object": profile.model_dump()}

    @router.patch("/api/ai/avatars/{avatar_id}")
    async def api_avatar_update(
        avatar_id: str,
        body: AvatarDraft,
        owner: AvatarOwner = Depends(owner_dependency),
    ):
        try:
            profile = avatar_store.update(avatar_id, owner, body)
        except AvatarStoreError as error:
            raise _http_error(error) from None
        return {"success_bool": True, "avatar_object": profile.model_dump()}

    @router.get("/api/ai/avatars/{avatar_id}/versions/{version}")
    async def api_avatar_version_read(
        avatar_id: str,
        version: int,
        owner: AvatarOwner = Depends(owner_dependency),
    ):
        try:
            profile = avatar_store.get(avatar_id, owner, version)
        except AvatarStoreError as error:
            raise _http_error(error) from None
        return {"success_bool": True, "avatar_object": profile.model_dump()}

    return router

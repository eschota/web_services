"""Owner-isolated API for the vehicle V3 image pipeline."""
from __future__ import annotations

import hashlib
import io
import json
import os
import secrets
from pathlib import Path
from typing import Any, Callable, Literal
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .store import PIPELINE_VERSION, RevisionConflict, VehicleStore

MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_IMAGE_SIDE = 8192
MAX_IMAGE_PIXELS = 32_000_000
PUBLIC_BASE = os.getenv("AUTORIG_PUBLIC_URL", "https://autorig.online").rstrip("/")
ALLOWED_SOURCE_PREFIXES = ("/api/ai/avatar-assets/", "/api/ai/video-references/", "/dev/api/scratch/", "/renderfin/render/")


class VehicleOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    working_side: Literal["auto", "left", "right"] = "auto"
    resolution: Literal[32, 64, 96, 128, 192, 256] = 192
    length_m: float | None = Field(default=None, ge=0.5, le=30)

    @field_validator("length_m", mode="before")
    @classmethod
    def zero_means_estimate(cls, value):
        return None if value in (0, 0.0, "0", "0.0", "") else value


class VehicleCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    image_url: str = Field(min_length=1, max_length=2048)
    stage: Literal["analyze"] = "analyze"
    options: VehicleOptions = Field(default_factory=VehicleOptions)


class PartAcceptanceEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")
    part_id: str = Field(min_length=1, max_length=160, pattern=r"^[A-Za-z0-9._:-]+$")
    original_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    prepared_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    mask_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ReferenceAcceptanceEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")
    original_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    prepared_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class VehicleAction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: Literal["prepare", "remove_wheels", "project_views", "depth_views", "fuse_voxels",
                    "cluster", "voxelize", "model", "rig", "configure", "accept_reference", "accept_parts", "retry"]
    part_ids: list[str] = Field(default_factory=list, max_length=128)
    provider: str = Field(default="blender", min_length=1, max_length=32)
    polygon_budget: int = Field(default=10000, ge=500, le=1_000_000)
    mode: Literal["split", "body"] = "split"
    options: dict[str, Any] = Field(default_factory=dict)
    expected_revision: int | None = Field(default=None, ge=0)
    part_evidence: list[PartAcceptanceEvidence] = Field(default_factory=list, max_length=128)
    reference_evidence: ReferenceAcceptanceEvidence | None = None

    @model_validator(mode="after")
    def validate_action_options(self):
        allowed = {
            "prepare": {"working_side", "length_m"},
            "remove_wheels": set(), "project_views": set(), "depth_views": set(), "fuse_voxels": set(),
            "voxelize": {"resolution", "working_side", "length_m"},
            "cluster": set(), "rig": set(), "retry": set(),
            "accept_parts": set(),
            "accept_reference": set(),
            "model": {"require_exact_parts_bool", "registration_tolerance_m_float"},
            "configure": {"wheel_updates", "wheels", "expected_wheel_revision"},
        }[self.action]
        unknown = set(self.options) - allowed
        if unknown:
            raise ValueError("unsupported vehicle action options: " + ", ".join(sorted(unknown)))
        if self.action == "accept_parts":
            if not self.part_ids or len(self.part_evidence) != len(self.part_ids):
                raise ValueError("accept_parts needs one evidence record per part id")
        if self.action == "accept_reference" and self.reference_evidence is None:
            raise ValueError("accept_reference needs exact original and prepared hashes")
        return self


def _safe_import_url(value: str) -> str:
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or parsed.hostname != "autorig.online" or parsed.port not in (None, 443)
            or parsed.username or parsed.password or parsed.fragment
            or not any(parsed.path.startswith(prefix) for prefix in ALLOWED_SOURCE_PREFIXES)):
        raise HTTPException(400, detail="Use an allowlisted AutoRig image URL")
    return value


def _validate_image(data: bytes, filename: str = "") -> tuple[str, str, int, int]:
    def invalid(message: str, status: int = 400):
        raise HTTPException(status, detail={"error_string": "invalid_source", "message_string": message})
    if not data: invalid("Image is empty")
    if len(data) > MAX_IMAGE_BYTES: invalid("Image is larger than 12 MB", 413)
    try:
        with Image.open(io.BytesIO(data)) as image:
            width, height = image.size; fmt = str(image.format or "").upper()
            if width < 32 or height < 32 or width > MAX_IMAGE_SIDE or height > MAX_IMAGE_SIDE or width * height > MAX_IMAGE_PIXELS:
                invalid("Image dimensions exceed the 8192-side or 32-megapixel limit")
            image.verify()
        with Image.open(io.BytesIO(data)) as image:
            image.load()
    except HTTPException: raise
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError) as exc:
        raise HTTPException(400, detail={"error_string": "invalid_source",
            "message_string": "Use a valid PNG or JPEG image"}) from exc
    if fmt not in {"PNG", "JPEG"}: invalid("Use a PNG or JPEG image")
    ext, mime = ("png", "image/png") if fmt == "PNG" else ("jpg", "image/jpeg")
    suffix = Path(filename).suffix.lower()
    if suffix and suffix not in ({".png"} if ext == "png" else {".jpg", ".jpeg"}):
        invalid("Filename extension does not match image bytes")
    return ext, mime, width, height


async def _download(url: str, factory: Callable[[], httpx.AsyncClient]) -> tuple[bytes, str]:
    _safe_import_url(url); chunks: list[bytes] = []; size = 0
    try:
        async with factory() as client:
            async with client.stream("GET", url) as response:
                if response.status_code != 200: raise HTTPException(400, detail="Source image could not be imported")
                if response.headers.get("location"): raise HTTPException(400, detail="Redirects are not allowed")
                for_download = response.headers.get("content-length")
                if for_download and int(for_download) > MAX_IMAGE_BYTES: raise HTTPException(413, detail="Image is larger than 12 MB")
                async for chunk in response.aiter_bytes(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_IMAGE_BYTES: raise HTTPException(413, detail="Image is larger than 12 MB")
                    chunks.append(chunk)
    except HTTPException: raise
    except (httpx.HTTPError, ValueError) as exc: raise HTTPException(502, detail="Source image import failed") from exc
    return b"".join(chunks), Path(urlsplit(url).path).name


def _response(value: dict[str, Any]) -> dict[str, Any]:
    keys = ("id", "status", "stage", "progress", "source_url", "analysis", "parts", "artifacts",
            "viewer_url", "stages", "error", "options", "revision", "original_url", "prepared_image_url",
            "source_sha_chain", "camera", "supported_model_providers")
    keys = keys + ("camera_attempt_int", "prepare_retry_available_bool", "reference_accepted_bool",
                   "reference_review_id", "reference_review_revision", "drive_blocker_string",
                   "wheel_removal_object", "body_cleanup_object", "multiview_object", "views_object", "fusion_object")
    keys = keys + ("remove_attempt_int", "body_cleanup_retry_available_bool", "view_attempts_object",
                   "project_views_retry_available_bool")
    keys = keys + ("wheel_rig", "wheel_rig_revision")
    out = {key: value.get(key) for key in keys}
    accepted = value.get("accepted_parts") or {}; camera = value.get("camera") or {}
    exact_camera = (camera.get("normalization_status_string") == "accepted" and
                    camera.get("projection_string") == "orthographic" and
                    abs(float(camera.get("yaw_deg_float", -999)) - 45.0) <= .5 and
                    abs(float(camera.get("pitch_deg_float", -999)) - 45.0) <= .5)
    parts = []
    for source in value.get("parts") or []:
        part = dict(source); part_id = str(part.get("part_id_string") or part.get("id") or "")
        review = accepted.get(part_id) or {}; bbox = part.get("source_bbox_px_array") or part.get("bbox_px_array") or []
        eligible = bool(exact_camera and part.get("mask_url") and part.get("mask_sha256") and len(bbox) == 4 and
                        float(bbox[2]) > float(bbox[0]) and float(bbox[3]) > float(bbox[1]) and
                        (part.get("shape_gate_object") or {}).get("accepted_bool") is not False)
        part["acceptance_eligible_bool"] = eligible
        if review and review.get("mask_sha256") == part.get("mask_sha256"):
            part.update(accepted_bool=True, review_id=review.get("review_id"),
                        review_revision=review.get("run_revision"))
        else: part["accepted_bool"] = False
        parts.append(part)
    out["parts"] = parts
    return out


def build_vehicle_router(get_avatar_owner: Callable, *, store: VehicleStore | None = None,
                         http_client_factory: Callable[[], httpx.AsyncClient] | None = None) -> APIRouter:
    state = store or VehicleStore()
    factory = http_client_factory or (lambda: httpx.AsyncClient(follow_redirects=False, timeout=30))
    router = APIRouter(prefix="/api/vehicle", tags=["vehicle-v3"])

    @router.post("/runs", status_code=202)
    async def create_run(request: Request, owner=Depends(get_avatar_owner)):
        content_type = request.headers.get("content-type", "").lower(); options = VehicleOptions(); source_origin = "upload"
        if content_type.startswith("multipart/form-data"):
            try:
                form = await request.form(max_part_size=MAX_IMAGE_BYTES)
            except TypeError:  # Starlette 0.36 on the production VPS has no per-part argument.
                form = await request.form()
            upload = form.get("image") or form.get("file")
            if upload is None or not hasattr(upload, "read"): raise HTTPException(400, detail="Multipart field image is required")
            try:
                data = await upload.read(MAX_IMAGE_BYTES + 1); filename = str(getattr(upload, "filename", "") or "")
            finally:
                await upload.close()
            if form.get("stage", "analyze") != "analyze": raise HTTPException(400, detail="Initial stage must be analyze")
            if form.get("options"):
                try: options = VehicleOptions.model_validate_json(str(form["options"]))
                except Exception as exc: raise HTTPException(422, detail="Invalid vehicle options") from exc
        else:
            try: body = VehicleCreate.model_validate(await request.json())
            except Exception as exc: raise HTTPException(422, detail="Invalid vehicle request") from exc
            data, filename = await _download(body.image_url, factory); options = body.options; source_origin = body.image_url
        ext, mime, width, height = _validate_image(data, filename)
        run_id = state.run_id(); token = secrets.token_urlsafe(32); run_dir = state.run_dir(run_id)
        source = run_dir / f"source.{ext}"; temporary = run_dir / f".source-{secrets.token_hex(8)}.tmp"
        with temporary.open("xb") as handle: handle.write(data); handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, source)
        source_url = f"{PUBLIC_BASE}/api/vehicle/assets/{run_id}/{token}/source.{ext}"
        document = {"id": run_id, "status": "queued", "stage": "analyze", "progress": 0,
                    "source_url": source_url, "original_url": source_url, "prepared_image_url": None,
                    "analysis": None, "parts": [],
                    "artifacts": {"overlay_url": None, "voxel_url": None, "voxel_preview_url": None},
                    "viewer_url": None, "stages": [], "error": None,
                    "options": options.model_dump(mode="json"), "revision": 0, "source_sha256": hashlib.sha256(data).hexdigest(),
                    "source": {"path": str(source), "origin": source_origin, "mime_type": mime, "width": width, "height": height},
                    "pipeline_version": PIPELINE_VERSION, "_capability": token}
        document["source_sha_chain"] = {"original_sha256": document["source_sha256"],
                                          "prepared_sha256": None, "pipeline_version": PIPELINE_VERSION}
        document["camera"] = None
        document["supported_model_providers"] = ["blender"]
        return _response(state.create(owner, document, document["source_sha256"]))

    @router.get("/runs/{run_id}")
    async def get_run(run_id: str, owner=Depends(get_avatar_owner)):
        try: return _response(state.get(run_id, owner))
        except (KeyError, ValueError): raise HTTPException(404, detail="Vehicle run not found") from None

    @router.post("/runs/{run_id}/actions", status_code=202)
    async def action(run_id: str, body: VehicleAction, owner=Depends(get_avatar_owner)):
        provider = str(body.provider)
        requested_paid = str(body.options.get("provider") or "").lower()
        if requested_paid in {"tripo", "tripo3d"} or provider not in {"blender", "farm"}:
            raise HTTPException(409, detail="Paid vehicle providers, including Tripo, are disabled")
        if body.action == "model" and provider == "farm":
            raise HTTPException(409, detail="vehicle_farm_model_provider_unavailable")
        if body.action == "accept_reference":
            try: value = state.accept_reference(run_id, owner, body.model_dump(mode="json"), body.expected_revision)
            except KeyError: raise HTTPException(404, detail="Vehicle run not found") from None
            except RevisionConflict as exc: raise HTTPException(409, detail=str(exc)) from None
            except ValueError as exc: raise HTTPException(400, detail=str(exc)) from None
            return _response(value)
        if body.action == "accept_parts":
            try: value = state.accept_parts(run_id, owner, body.model_dump(mode="json"), body.expected_revision)
            except KeyError: raise HTTPException(404, detail="Vehicle run not found") from None
            except RevisionConflict as exc: raise HTTPException(409, detail=str(exc)) from None
            except ValueError as exc: raise HTTPException(400, detail=str(exc)) from None
            return _response(value)
        try: value = state.enqueue(run_id, owner, body.model_dump(mode="json", exclude_none=False), body.expected_revision)
        except KeyError: raise HTTPException(404, detail="Vehicle run not found") from None
        except RevisionConflict as exc: raise HTTPException(409, detail=str(exc)) from None
        except ValueError as exc: raise HTTPException(400, detail=str(exc)) from None
        return _response(value)

    @router.get("/assets/{run_id}/{token}/{name:path}", include_in_schema=False)
    async def capability_asset(run_id: str, token: str, name: str):
        try: path = state.capability_path(run_id, token, name)
        except (KeyError, ValueError): raise HTTPException(404, detail="Vehicle asset not found") from None
        media = {".png": "image/png", ".jpg": "image/jpeg", ".json": "application/json", ".glb": "model/gltf-binary", ".npz": "application/octet-stream"}.get(path.suffix.lower(), "application/octet-stream")
        return FileResponse(path, media_type=media, headers={"Cache-Control": "private, max-age=300"})

    return router

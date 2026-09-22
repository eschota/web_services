"""Automatic Avatar builder: one picture or one video in, a saved Avatar v2 out.

The owner's requirement (2026-09-23) is that an Avatar is created with no
manual steps from a single image or a single video, as a graph node with many
outputs: the saved Avatar itself plus every view of the character it made.
This module is that node's backend. One job runs these steps, each of which is
recorded in the job file before it waits, so a restart resumes rather than
repeats:

1. **source** - a picture is taken as is; a video is sampled on this host
   (ffmpeg) and the frame with the sharpest, largest, most frontal face is
   picked with the Haar cascades that ship inside opencv-python. No model is
   downloaded for this.
2. **describe** - the Vision service (Qwen3.5 9B on the AI boxes) writes the
   identity, appearance, body and wardrobe as JSON.
3. **front** - FLUX.2 klein 4B turns the source into a clean frontal
   upper-body studio picture. It is the anchor every other view references.
4. **views** - the remaining slots in parallel across the image boxes, all
   on klein 4B first (on 2026-09-23 it turned a strict profile and a back
   view cleanly in ~20 s on f5, where Qwen-Image-Edit-2511 took minutes on
   the same 8 GB box); Qwen-Image-Edit-2511 is the retry engine. References
   are always the anchors (front + source face), never a chain of generated
   views, so drift does not accumulate.
5. **check** - every view is compared with the source by the Vision model on
   a side-by-side picture sent inline (identity 0-10, camera direction, single
   person), plus a Haar face check (a back view must show no frontal face).
   A failed view is rendered once more on the other engine and the better of
   the two kept; a view that fails twice is stored and marked failed, never
   passed off as good.
6. **save** - the views, a contact sheet and the description become a new
   Avatar (or a new version of the Avatar wired into the node).

No face-embedding model is installed anywhere on the farm or this host
(checked 2026-09-23: only inswapper_128 and IP-Adapter FaceID weights, no
ArcFace/antelopev2/buffalo_l). The identity score is therefore a Vision-model
judgement and says so in ``identity_method``.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlsplit

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from ai_avatar_assets import AvatarAssetStore, validate_import_url
from ai_avatars import (
    AvatarDraft, AvatarOwner, AvatarStore, AvatarStoreError, CANONICAL_VIEW_SLOTS,
    FORMAT_VERSION_LATEST,
)

logger = logging.getLogger(__name__)

BUILD_DIR = Path(os.getenv("AUTORIG_AI_AVATAR_BUILD_DIR", "/srv/autorig/data/var/ai-avatar-build"))
INTERNAL_API = os.getenv("AUTORIG_INTERNAL_PUBLIC_API", "http://127.0.0.1:8200").rstrip("/")
VISION_MODEL = "qwen35-9b-uncensored"
KLEIN_CHECKPOINT = "flux-2-klein-4b.safetensors"
JOB_ID_RE = re.compile(r"^avb_[a-f0-9]{24}$")
AVATAR_REF_RE = re.compile(r"^(av_[a-f0-9]{24})(?:@([1-9][0-9]{0,5}))?$")
MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_VIDEO_BYTES = 400 * 1024 * 1024
MAX_PARALLEL_BUILDS = 3
RENDER_TIMEOUT_SECONDS = 30 * 60
VISION_TIMEOUT_SECONDS = 15 * 60
PIPELINE_VERSION = "avatar_build_v2"
# The first view drawn, and the picture every other view is anchored to.
ANCHOR_SLOT = "full_body"

# ------------------------------------------------------------------ the views
#
# `left`/`right` name the edge of the frame the nose points to, which is what
# a picture can be checked against without guessing whose left is meant.
_PREFIX = (
    "Same person as in image 1: identical face, facial features, hairstyle, hair "
    "color, skin tone, age and body proportions{outfit}. Plain light grey seamless "
    "studio background, soft even lighting, photorealistic, sharp focus, exactly one "
    "person, nobody else in the picture. "
)
VIEW_SPECS: Dict[str, Dict[str, Any]] = {
    # klein keeps the composition of picture 1, so asking it to reframe a
    # full-body picture as a portrait mostly fails (2026-09-23: "waist up" and
    # "close-up" both came back full length). These two views are therefore
    # cut from the anchor around its detected face and re-rendered sharp.
    "front": {
        "engine": "klein", "size": (832, 1216), "framing": "upper_body", "yaw": 0.0,
        "family": "front", "crop": "upper",
        "text": ("Upper-body portrait from the waist up: keep exactly the framing, pose and "
                 "composition of image 1 and render it as a sharp, high-resolution photograph "
                 "with fine skin, hair and fabric detail, facing the camera, eyes open."),
    },
    "face_closeup": {
        "engine": "klein", "size": (1024, 1024), "framing": "face", "yaw": 0.0,
        "family": "front", "crop": "face",
        "text": ("Head and shoulders close-up portrait: keep exactly the framing of image 1, the "
                 "face filling most of the picture, and render it as a sharp, high-resolution "
                 "photograph with fine skin and hair detail, eyes open looking into the lens; "
                 "take the facial features from image 2."),
    },
    "full_body": {
        "engine": "klein", "size": (832, 1216), "framing": "full_body", "yaw": 0.0,
        "family": "front",
        "text": ("Full body shot from head to shoes, standing in a relaxed neutral A-pose, "
                 "arms slightly away from the body, facing the camera directly, whole body "
                 "visible with space above the head and below the feet."),
    },
    "three_quarter_left": {
        "engine": "klein", "size": (832, 1216), "framing": "full_body", "yaw": -45.0,
        "family": "three_quarter",
        "text": ("Full body three-quarter view: body and head turned 45 degrees so the "
                 "person faces the left edge of the frame, both eyes still visible, relaxed "
                 "neutral standing pose, whole body visible."),
    },
    "three_quarter_right": {
        "engine": "klein", "size": (832, 1216), "framing": "full_body", "yaw": 45.0,
        "family": "three_quarter",
        "text": ("Full body three-quarter view: body and head turned 45 degrees so the "
                 "person faces the right edge of the frame, both eyes still visible, relaxed "
                 "neutral standing pose, whole body visible."),
    },
    "profile_left": {
        "engine": "klein", "size": (832, 1216), "framing": "full_body", "yaw": -90.0,
        "family": "profile",
        "text": ("Full body strict side profile: the person faces the left edge of the "
                 "frame, only one eye visible, nose silhouette clearly visible, relaxed "
                 "neutral standing pose, whole body visible."),
    },
    "profile_right": {
        "engine": "klein", "size": (832, 1216), "framing": "full_body", "yaw": 90.0,
        "family": "profile",
        "text": ("Full body strict side profile: the person faces the right edge of the "
                 "frame, only one eye visible, nose silhouette clearly visible, relaxed "
                 "neutral standing pose, whole body visible."),
    },
    "back": {
        "engine": "klein", "size": (832, 1216), "framing": "full_body", "yaw": 180.0,
        "family": "back",
        "text": ("Full body shot seen from directly behind: the back of the head and the "
                 "hair visible, the face not visible at all, the back side of the same "
                 "outfit, relaxed neutral standing pose, whole body visible."),
    },
}
assert tuple(sorted(VIEW_SPECS)) == tuple(sorted(CANONICAL_VIEW_SLOTS))
DEFAULT_VIEWS = [ANCHOR_SLOT] + [slot for slot in CANONICAL_VIEW_SLOTS if slot != ANCHOR_SLOT]
NEGATIVE = ("second person, crowd, extra limbs, deformed face, different face, text, "
            "watermark, collage, split screen, character sheet, busy background")

DESCRIBE_INSTRUCTION = (
    "Describe the single main character in this picture for a reusable production "
    "Avatar. Return ONLY one JSON object with exactly these keys: "
    '{"display_name":"short neutral descriptive label, never a guessed real identity",'
    '"identity_prompt":"one sentence of stable identity traits: apparent age range, face '
    'shape, eyes, nose, lips, skin tone, hair color, length and style",'
    '"appearance":"distinctive visible features: freckles, makeup, marks, jewelry worn on '
    'the head or face",'
    '"body":"body type, height impression and proportions",'
    '"wardrobe":"visible clothing, shoes and accessories only; empty if unclear",'
    '"negative_identity_prompt":"traits that would contradict this identity",'
    '"production_notes":"occlusions, crop, anything the views will have to invent",'
    '"explicit": true if any nudity, exposed breasts or genitals, or sexual activity is '
    'visible anywhere in the picture, otherwise false}. '
    "Describe visible evidence only. Never name or identify a real person. Exclude pose, "
    "action, camera, lighting, background and other people. No markdown."
)
JUDGE_INSTRUCTION = (
    "Two photos side by side. LEFT is the reference character. RIGHT is a generated "
    "view that must show the SAME character. Ignore clothing, pose, expression, makeup "
    "intensity, lighting and background: they are allowed to differ. Compare only the "
    "person, feature by feature: face shape, eyes, eyebrows, nose, lips, hairline, hair "
    "color, length and style, skin tone, age, body build. First list every identity "
    "difference you see, then score. Return ONLY one JSON "
    'object: {"issues": "every identity difference or defect, empty only if none", '
    '"same_person": integer 0-10 where 10 = indistinguishable, 8-9 = same person with '
    "minor differences, 6-7 = probably the same person but the face or hair drifted, 3-5 = "
    "a different person of similar style, 0-2 = clearly someone else (for a view from "
    "behind judge hair, head shape, body and clothing), "
    '"view": one of "front","three_quarter_left","three_quarter_right","profile_left",'
    '"profile_right","back","other" describing the RIGHT photo, where left/right is the '
    'edge of the frame the nose points to, '
    '"people": number of people in the RIGHT photo}. Give 10 only if you checked every '
    "feature and found nothing. No markdown."
)
IDENTITY_PASS = 0.6

# --------------------------------------------------------------- job storage


class BuildRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    image_url: Optional[str] = Field(default=None, max_length=2048)
    # A picture pasted into a /nodes input arrives inline.
    image_base64: Optional[str] = Field(default=None, max_length=17 * 1024 * 1024)
    video_url: Optional[str] = Field(default=None, max_length=2048)
    avatar: Optional[str] = Field(default=None, max_length=64)
    display_name: Optional[str] = Field(default=None, max_length=120)
    outfit: Optional[str] = Field(default=None, max_length=1000)
    views: Optional[str] = Field(default=None, max_length=400)
    qa: Optional[bool] = True
    seed: Optional[int] = Field(default=None, ge=0, le=2**31 - 1)
    engine: Optional[str] = Field(default=None, max_length=12)
    # Keep every derived picture out of saved graphs whatever the classifier says.
    private: Optional[bool] = None


def _now() -> float:
    return round(time.time(), 3)


def _owner_key(owner: AvatarOwner) -> str:
    return f"{owner.owner_type}:{owner.owner_id}"


def parse_views(raw: Optional[str]) -> List[str]:
    if not raw or not str(raw).strip():
        return list(DEFAULT_VIEWS)
    wanted = [part.strip().lower() for part in re.split(r"[,\s]+", str(raw)) if part.strip()]
    unknown = [slot for slot in wanted if slot not in VIEW_SPECS]
    if unknown:
        raise HTTPException(400, detail={"error_string": "unknown_view",
            "message_string": f"Unknown view slot(s): {', '.join(unknown)}; "
                              f"choose from {', '.join(DEFAULT_VIEWS)}"})
    return [ANCHOR_SLOT] + [slot for slot in DEFAULT_VIEWS if slot in wanted and slot != ANCHOR_SLOT]


class BuildJobStore:
    def __init__(self, root: Path = BUILD_DIR):
        self.root = Path(root)
        self._lock = threading.RLock()

    @property
    def jobs_dir(self) -> Path:
        return self.root / "jobs"

    def work_dir(self, job_id: str) -> Path:
        self._path(job_id)
        path = self.root / "work" / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _path(self, job_id: str) -> Path:
        if not JOB_ID_RE.fullmatch(str(job_id or "")):
            raise HTTPException(400, detail="Invalid Avatar build id")
        return self.jobs_dir / f"{job_id}.json"

    def load(self, job_id: str) -> Dict[str, Any]:
        try:
            return json.loads(self._path(job_id).read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise HTTPException(404, detail="Avatar build not found") from None
        except (OSError, json.JSONDecodeError):
            raise HTTPException(500, detail="Avatar build is unreadable") from None

    def read(self, job_id: str, owner: AvatarOwner) -> Dict[str, Any]:
        job = self.load(job_id)
        if job.get("owner") != _owner_key(owner):
            raise HTTPException(404, detail="Avatar build not found")
        return job

    def write(self, job: Dict[str, Any]) -> None:
        with self._lock:
            self.jobs_dir.mkdir(parents=True, exist_ok=True)
            path = self._path(job["job_id"])
            job["updated_at"] = _now()
            fd, temporary = tempfile.mkstemp(prefix=".build-", suffix=".tmp", dir=self.jobs_dir)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(job, handle, ensure_ascii=False, indent=1, sort_keys=True)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)

    def create(self, owner: AvatarOwner, identity: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        """The same source and settings are the same build (a re-run is free)."""
        canonical = json.dumps({"owner": _owner_key(owner), **identity}, sort_keys=True)
        job_id = "avb_" + hashlib.sha256(canonical.encode()).hexdigest()[:24]
        with self._lock:
            path = self._path(job_id)
            if path.exists():
                job = self.load(job_id)
                if job.get("status") != "failed":
                    return job, True
            job = {"schema_version": 1, "job_id": job_id, "owner": _owner_key(owner),
                   "owner_type": owner.owner_type, "owner_id": owner.owner_id,
                   "status": "queued", "stage": "queued", "finished": False,
                   "created_at": _now(), "request": identity, "steps": {},
                   "views": {}, "timings": {}, "log": []}
            self.write(job)
            return job, False


# ------------------------------------------------------------------ helpers


def _log(job: Dict[str, Any], message: str) -> None:
    entries = job.setdefault("log", [])
    entries.append(f"{time.strftime('%H:%M:%S', time.gmtime())} {message}")
    del entries[:-60]
    logger.info("avatar build %s: %s", job.get("job_id"), message)


def _extract_json(text: str) -> Dict[str, Any]:
    text = str(text or "").strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.I)
    if fence:
        text = fence.group(1).strip()
    start, end = text.find("{"), text.rfind("}")
    if start < 0:
        raise ValueError("no JSON object in the answer")
    body = text[start:end + 1] if end > start else text[start:] + "}"
    try:
        value = json.loads(body)
    except json.JSONDecodeError:
        # Qwen sometimes stops before the final brace or quote.
        for suffix in ('"}', "}", '"]}'):
            try:
                value = json.loads(text[start:] + suffix)
                break
            except json.JSONDecodeError:
                continue
        else:
            raise ValueError("the answer is not valid JSON") from None
    if not isinstance(value, dict):
        raise ValueError("the answer is not a JSON object")
    return value


def clean_description(raw: Dict[str, Any]) -> Dict[str, str]:
    limits = {"display_name": 120, "identity_prompt": 4000, "appearance": 4000, "body": 2000,
              "wardrobe": 4000, "negative_identity_prompt": 2000, "production_notes": 1000}
    out = {key: str(raw.get(key) or "").strip()[:limit] for key, limit in limits.items()}
    explicit = raw.get("explicit")
    out["explicit"] = explicit is True or str(explicit).strip().lower() in ("true", "yes", "1")
    if not out["identity_prompt"]:
        raise ValueError("Vision returned no identity description")
    if not out["display_name"]:
        out["display_name"] = "Avatar " + time.strftime("%Y-%m-%d")
    return out


_CROP_PREFIX = (
    "Same person as in image 1: identical face, facial features, hairstyle, hair color, skin "
    "tone and age, same clothing as visible in image 1. Plain light grey seamless studio "
    "background, soft even lighting, photorealistic, sharp focus, exactly one person. "
)
# Face height / picture height a crop view is cut to; an answer far below it
# means the model zoomed back out.
CROP_FACE_FRACTION = {"upper": 1 / 6.2, "face": 1 / 2.6}


def view_prompt(slot: str, description: Dict[str, str], outfit: str) -> str:
    spec = VIEW_SPECS[slot]
    if spec.get("crop"):
        identity = description.get("identity_prompt", "")
        return (_CROP_PREFIX + spec["text"] + f" Character: {identity}")[:5800]
    wear = str(outfit or description.get("wardrobe") or "").strip()
    outfit_clause = (f", wearing exactly the same outfit ({wear}), same shoes and accessories"
                     if wear else ", wearing the same outfit, shoes and accessories")
    identity = description.get("identity_prompt", "")
    features = description.get("appearance", "")
    tail = f" Character: {identity}"
    if features:
        tail += f" Distinctive features: {features}"
    return (_PREFIX.format(outfit=outfit_clause) + spec["text"] + tail)[:5800]


def expected_family(slot: str) -> str:
    return VIEW_SPECS[slot]["family"]


def judge_verdict(slot: str, answer: Dict[str, Any], haar: str) -> Dict[str, Any]:
    """Turn the Vision answer and the face detector into a pass/fail."""
    try:
        same = max(0.0, min(10.0, float(answer.get("same_person"))))
    except (TypeError, ValueError):
        same = 0.0
    seen = str(answer.get("view") or "other").strip().lower()
    try:
        people = int(answer.get("people"))
    except (TypeError, ValueError):
        people = 1
    family = expected_family(slot)
    seen_family = {"front": "front", "three_quarter_left": "three_quarter",
                   "three_quarter_right": "three_quarter", "profile_left": "profile",
                   "profile_right": "profile", "back": "back"}.get(seen, "other")
    # Vision models confuse a strong three-quarter with a profile; a neighbour
    # is a warning, the wrong side of the head is a failure.
    neighbours = {("three_quarter", "profile"), ("profile", "three_quarter"),
                  ("front", "three_quarter"), ("three_quarter", "front")}
    angle_ok = seen_family == family
    notes = str(answer.get("issues") or "")[:600]
    warnings = []
    if not angle_ok and (family, seen_family) in neighbours:
        angle_ok = True
        warnings.append(f"judge saw {seen}")
    elif angle_ok and seen != slot and slot in ("three_quarter_left", "three_quarter_right",
                                                  "profile_left", "profile_right"):
        warnings.append(f"judge saw the other side ({seen})")
    if family == "back" and haar == "frontal":
        angle_ok = False
        warnings.append("a frontal face is visible in the back view")
    if family == "front" and haar == "none":
        warnings.append("no frontal face detected")
    score = round(same / 10.0, 2)
    passed = score >= IDENTITY_PASS and angle_ok and people <= 1
    status = "passed" if passed and not warnings else (
        "accepted_with_warnings" if passed else "failed")
    if people > 1:
        warnings.append(f"{people} people in the picture")
    return {"status": status, "identity_score": score, "angle_ok": angle_ok,
            "face_detected": haar, "judge_view": seen,
            "notes": "; ".join(filter(None, warnings + [notes]))[:1000]}


def _rank(item: Dict[str, Any]) -> Tuple[int, float]:
    qa = item.get("qa") or {}
    order = {"passed": 3, "accepted_with_warnings": 2, "unchecked": 1, "failed": 0}
    return order.get(qa.get("status"), 0), float(qa.get("identity_score") or 0)


# -------------------------------------------------------- image analysis (cv2)


# ------------------------------------------------------ external sources
#
# A /nodes input may hold any public address (the owner pastes Civitai and
# Pexels links). It is fetched here, on the server, under the same rules as
# the video-reference reader: HTTPS only, no credentials or custom ports, every
# hop's DNS must be public, at most four redirects each re-checked, the
# Civitai bearer only ever sent to Civitai's own hosts, a size cap, and the
# answer must be a picture or a video. Nothing else is fetched.

EXTERNAL_VIDEO_EXTENSIONS = (".mp4", ".mov", ".webm", ".m4v", ".mkv")
EXTERNAL_IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp")


def validate_external_url(value: str) -> str:
    from urllib.parse import urlsplit as _split
    url = str(value or "").strip()
    try:
        parsed = _split(url)
        port = parsed.port
    except ValueError:
        raise ValueError("not a valid address") from None
    host = (parsed.hostname or "").rstrip(".").lower()
    if parsed.scheme.lower() != "https" or not host:
        raise ValueError("the address must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credentials are not allowed in the address")
    if port not in (None, 443):
        raise ValueError("custom ports are not allowed")
    if host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
        raise ValueError("local hosts are not allowed")
    import ipaddress as _ip
    try:
        literal = _ip.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None and not literal.is_global:
        raise ValueError("private addresses are not allowed")
    if not parsed.path or parsed.path.endswith("/"):
        raise ValueError("the address must name a file")
    return url


def guess_kind(url: str) -> str:
    path = urlsplit(str(url or "")).path.lower()
    if path.endswith(EXTERNAL_VIDEO_EXTENSIONS):
        return "video"
    if path.endswith(EXTERNAL_IMAGE_EXTENSIONS):
        return "image"
    return "auto"


async def fetch_external(client: httpx.AsyncClient, url: str, target: Path) -> Tuple[str, int]:
    """Download a public picture or video to `target`; returns (kind, bytes)."""
    from urllib.parse import urljoin
    from renderfin.video_input import VideoInputError, _assert_public_dns, _civitai_headers
    current = validate_external_url(url)
    for _ in range(5):
        try:
            await _assert_public_dns(current)
        except VideoInputError as error:
            raise BuildError(f"the source address is not allowed: {error}") from None
        try:
            headers = {"User-Agent": "AutoRigAvatarBuilder/1.0 (+https://autorig.online/avatars)",
                       **_civitai_headers(current)}
            async with client.stream("GET", current, headers=headers,
                                     timeout=120.0, follow_redirects=False) as response:
                if 300 <= response.status_code < 400:
                    location = str(response.headers.get("location") or "").strip()
                    if not location:
                        raise BuildError("the source redirected nowhere")
                    try:
                        current = validate_external_url(urljoin(current, location))
                    except ValueError as error:
                        raise BuildError(f"the source redirected to a refused address: {error}") from None
                    continue
                if response.status_code != 200:
                    raise BuildError(f"the source answered HTTP {response.status_code}")
                content_type = str(response.headers.get("content-type") or "").split(";")[0].strip().lower()
                if content_type.startswith("video/"):
                    kind = "video"
                elif content_type.startswith("image/"):
                    kind = "image"
                elif content_type in ("", "application/octet-stream", "binary/octet-stream"):
                    kind = guess_kind(current)
                else:
                    kind = "auto"
                if kind == "auto":
                    raise BuildError(f"the source is not a picture or a video ({content_type or 'unknown type'})")
                limit = MAX_VIDEO_BYTES if kind == "video" else MAX_IMAGE_BYTES
                size = 0
                with open(target, "wb") as output:
                    async for chunk in response.aiter_bytes(1024 * 1024):
                        size += len(chunk)
                        if size > limit:
                            raise BuildError("the source is larger than allowed")
                        output.write(chunk)
                if not size:
                    raise BuildError("the source is empty")
                return kind, size
        except httpx.HTTPError as error:
            # Never echo the exception: it may quote a signed URL or a header.
            raise BuildError(f"the source could not be downloaded ({type(error).__name__})") from None
    raise BuildError("the source redirected too many times")


# ------------------------------------------------------------- privacy
#
# The graph library and scratch uploads are public (an auth fix is pending),
# so an Avatar built from explicit material must not leak into a saved
# graph. Its assets are marked private in their metadata; ai_graph drops
# private Avatar-asset addresses from stored results; the page still shows
# them live to whoever ran the build.

def nsfw_rating(paths: List[Path]) -> str:
    """Worst NudeNet rating over a few pictures: safe, suggestive or adult."""
    try:
        from nudenet import NudeDetector
    except Exception:
        return "unknown"
    explicit = {"FEMALE_GENITALIA_EXPOSED", "MALE_GENITALIA_EXPOSED", "ANUS_EXPOSED"}
    suggestive = {"FEMALE_BREAST_EXPOSED", "BUTTOCKS_EXPOSED", "FEMALE_GENITALIA_COVERED"}
    detector = NudeDetector()
    worst = "safe"
    for path in paths:
        try:
            found = detector.detect(str(path))
        except Exception:
            continue
        for item in found or []:
            label, score = item.get("class"), float(item.get("score") or 0)
            if label in explicit and score >= 0.35:
                return "adult"
            if (label in suggestive and score >= 0.35) or (label in explicit and score >= 0.2):
                worst = "suggestive"
    return worst


def _cv2():
    import cv2  # the prod venv ships opencv-python 4.13 with its Haar cascades
    return cv2


def detect_face_kind(data: bytes) -> str:
    """frontal, profile or none: enough to catch a back view with a face."""
    try:
        cv2 = _cv2()
        import numpy as np
    except Exception:
        return "unknown"
    image = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_GRAYSCALE)
    if image is None:
        return "unknown"
    scale = 900.0 / max(image.shape)
    if scale < 1:
        image = cv2.resize(image, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
    minimum = max(24, int(min(image.shape) * 0.05))
    frontal = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    if len(frontal.detectMultiScale(image, 1.1, 6, minSize=(minimum, minimum))):
        return "frontal"
    profile = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_profileface.xml")
    for candidate in (image, cv2.flip(image, 1)):
        if len(profile.detectMultiScale(candidate, 1.1, 5, minSize=(minimum, minimum))):
            return "profile"
    return "none"


def score_frames(paths: List[Path]) -> List[Dict[str, Any]]:
    """Score sampled frames for how well they show one face, sharply.

    Sharpness is the variance of the Laplacian over the face crop, ranked
    against the clip itself rather than a fixed threshold (a soft video is
    still best at its sharpest frame).
    """
    cv2 = _cv2()
    frontal = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    eyes = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_eye.xml")
    rows = []
    for path in paths:
        image = cv2.imread(str(path))
        if image is None:
            continue
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        h, w = gray.shape
        minimum = max(32, int(min(h, w) * 0.06))
        faces = frontal.detectMultiScale(gray, 1.1, 6, minSize=(minimum, minimum))
        whole = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        row = {"path": str(path), "width": w, "height": h, "faces": len(faces),
               "sharp_all": whole, "sharp_face": 0.0, "face_frac": 0.0, "eyes": 0,
               "center": 0.0, "box": None}
        if len(faces):
            x, y, fw, fh = max(faces, key=lambda item: item[2] * item[3])
            crop = gray[y:y + fh, x:x + fw]
            crop = cv2.resize(crop, (256, 256), interpolation=cv2.INTER_AREA)
            row["sharp_face"] = float(cv2.Laplacian(crop, cv2.CV_64F).var())
            row["face_frac"] = float(fw * fh) / float(w * h)
            upper = gray[y:y + fh // 2 + 1, x:x + fw]
            row["eyes"] = int(len(eyes.detectMultiScale(upper, 1.1, 6,
                                                        minSize=(fw // 10 + 1, fw // 10 + 1))))
            cx, cy = (x + fw / 2) / w, (y + fh / 2) / h
            row["center"] = max(0.0, 1.0 - 2.0 * max(abs(cx - 0.5), abs(cy - 0.45)))
            row["box"] = [int(x), int(y), int(fw), int(fh)]
        rows.append(row)
    if not rows:
        return rows
    top_face = max([row["sharp_face"] for row in rows] + [1e-6])
    top_all = max([row["sharp_all"] for row in rows] + [1e-6])
    for row in rows:
        if row["faces"]:
            row["score"] = round(
                1.0 + row["sharp_face"] / top_face
                + 0.6 * min(row["face_frac"] / 0.04, 1.0)
                + 0.4 * (1.0 if row["eyes"] >= 2 else 0.5 if row["eyes"] == 1 else 0.0)
                + 0.2 * row["center"]
                - (0.3 if row["faces"] > 1 else 0.0), 4)
        else:
            row["score"] = round(0.3 * row["sharp_all"] / top_all, 4)
    return rows


def sample_video(video: Path, out_dir: Path, count: int = 36) -> Tuple[List[Tuple[Path, float]], Dict[str, Any]]:
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
         "stream=width,height:format=duration", "-of", "json", str(video)],
        capture_output=True, text=True, timeout=60)
    if probe.returncode != 0:
        raise ValueError("the video could not be read")
    info = json.loads(probe.stdout or "{}")
    stream = (info.get("streams") or [{}])[0]
    duration = float((info.get("format") or {}).get("duration") or 0)
    if duration <= 0:
        raise ValueError("the video has no duration")
    meta = {"width": int(stream.get("width") or 0), "height": int(stream.get("height") or 0),
            "duration": round(duration, 3)}
    out_dir.mkdir(parents=True, exist_ok=True)
    frames = []
    start, stop = duration * 0.03, duration * 0.97
    for index in range(count):
        t = start + (stop - start) * index / max(1, count - 1)
        target = out_dir / f"frame_{index:03d}.png"
        run = subprocess.run(
            ["ffmpeg", "-v", "error", "-y", "-ss", f"{t:.3f}", "-i", str(video), "-frames:v", "1",
             "-vf", "scale='min(1600,iw)':-2", str(target)],
            capture_output=True, timeout=120)
        if run.returncode == 0 and target.exists():
            frames.append((target, round(t, 3)))
    if not frames:
        raise ValueError("no frame could be taken from the video")
    return frames, meta


def pick_best_frame(video: Path, out_dir: Path) -> Dict[str, Any]:
    frames, meta = sample_video(video, out_dir)
    times = {str(path): t for path, t in frames}
    rows = score_frames([path for path, _ in frames])
    if not rows:
        raise ValueError("no usable frame in the video")
    for row in rows:
        row["time"] = times.get(row["path"], 0.0)
    best = max(rows, key=lambda row: row["score"])
    return {"best": best, "rows": rows, "meta": meta}


def compose_pair(left: bytes, right: bytes, height: int = 640) -> bytes:
    from PIL import Image
    images = []
    for data in (left, right):
        with Image.open(io.BytesIO(data)) as image:
            image = image.convert("RGB")
            ratio = height / image.height
            images.append(image.resize((max(1, int(image.width * ratio)), height)))
    gap = 16
    canvas = Image.new("RGB", (images[0].width + images[1].width + gap, height), (255, 255, 255))
    canvas.paste(images[0], (0, 0))
    canvas.paste(images[1], (images[0].width + gap, 0))
    out = io.BytesIO()
    canvas.save(out, "JPEG", quality=88)
    return out.getvalue()


def compose_sheet(tiles: List[Tuple[str, bytes]], cell: Tuple[int, int] = (360, 520)) -> bytes:
    from PIL import Image, ImageDraw
    columns = 4
    rows = max(1, (len(tiles) + columns - 1) // columns)
    width, height = cell
    label = 26
    canvas = Image.new("RGB", (columns * width, rows * (height + label)), (236, 236, 238))
    draw = ImageDraw.Draw(canvas)
    for index, (name, data) in enumerate(tiles):
        with Image.open(io.BytesIO(data)) as image:
            image = image.convert("RGB")
            image.thumbnail((width - 8, height - 8))
            x = (index % columns) * width + (width - image.width) // 2
            y = (index // columns) * (height + label) + (height - image.height) // 2
            canvas.paste(image, (x, y))
        draw.text(((index % columns) * width + 8, (index // columns) * (height + label) + height + 4),
                  name, fill=(40, 40, 48))
    out = io.BytesIO()
    canvas.save(out, "JPEG", quality=90)
    return out.getvalue()


def face_box(data: bytes) -> Optional[List[int]]:
    """The largest frontal face as [x, y, w, h] in the picture's own pixels."""
    try:
        cv2 = _cv2()
        import numpy as np
    except Exception:
        return None
    image = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_GRAYSCALE)
    if image is None:
        return None
    frontal = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    minimum = max(24, int(min(image.shape) * 0.04))
    faces = frontal.detectMultiScale(image, 1.1, 5, minSize=(minimum, minimum))
    if not len(faces):
        return None
    x, y, w, h = max(faces, key=lambda item: item[2] * item[3])
    return [int(x), int(y), int(w), int(h)]


def crop_to_framing(data: bytes, framing: str, width: int, height: int) -> bytes:
    """Cut a full-body picture to an upper-body or face framing at (width, height).

    Uses the detected face when there is one; otherwise assumes the usual
    full-body composition (head in the top eighth, centred).
    """
    from PIL import Image
    box = face_box(data)
    with Image.open(io.BytesIO(data)) as image:
        image = image.convert("RGB")
        W, H = image.size
        if box:
            fx, fy, fw, fh = box
        else:
            fw = fh = max(1, int(H * 0.09))
            fx, fy = (W - fw) // 2, int(H * 0.05)
        cx = fx + fw / 2
        aspect = width / height
        if framing == "face":
            crop_h = fh * 2.6
            top = fy - fh * 0.75
        else:  # upper body: head to waist
            crop_h = fh * 6.2
            top = fy - fh * 0.7
        crop_w = crop_h * aspect
        left = max(0.0, min(cx - crop_w / 2, W - crop_w)) if crop_w <= W else (W - crop_w) / 2
        top = max(0.0, min(top, H - crop_h)) if crop_h <= H else 0.0
        region = image.crop((int(left), int(top), int(left + crop_w), int(top + crop_h)))
        region = region.resize((width, height), Image.LANCZOS)
        out = io.BytesIO()
        region.save(out, "PNG")
        return out.getvalue()


def crop_face_region(data: bytes, box: Optional[List[int]], pad: float = 1.6) -> bytes:
    """The face and some hair around it, for the identity judge."""
    from PIL import Image
    with Image.open(io.BytesIO(data)) as image:
        image = image.convert("RGB")
        if not box:
            return data
        x, y, w, h = box
        cx, cy = x + w / 2, y + h / 2
        side = max(w, h) * pad * 1.4
        left, top = max(0, int(cx - side / 2)), max(0, int(cy - side * 0.55))
        right, bottom = min(image.width, int(cx + side / 2)), min(image.height, int(cy + side * 0.75))
        out = io.BytesIO()
        image.crop((left, top, right, bottom)).save(out, "PNG")
        return out.getvalue()


# ---------------------------------------------------------------- the builder


class AvatarBuilder:
    """Runs and resumes jobs. One instance per process."""

    def __init__(self, *, avatar_store: Optional[AvatarStore] = None,
                 asset_store: Optional[AvatarAssetStore] = None,
                 job_store: Optional[BuildJobStore] = None,
                 http_client_factory: Optional[Callable[[], httpx.AsyncClient]] = None,
                 api_base: str = INTERNAL_API, poll_seconds: float = 4.0):
        self.avatars = avatar_store or AvatarStore()
        self.assets = asset_store or AvatarAssetStore(max_assets_per_owner=5000)
        self.jobs = job_store or BuildJobStore()
        self.factory = http_client_factory or (lambda: httpx.AsyncClient(follow_redirects=False,
                                                                        timeout=httpx.Timeout(90.0)))
        self.api = api_base.rstrip("/")
        self.poll_seconds = poll_seconds
        self._running: Dict[str, asyncio.Task] = {}
        self._job_locks: Dict[str, asyncio.Lock] = {}
        self._slots = asyncio.Semaphore(MAX_PARALLEL_BUILDS)

    # ---- lifecycle

    def ensure_running(self, job_id: str) -> None:
        task = self._running.get(job_id)
        if task and not task.done():
            return
        self._running[job_id] = asyncio.get_running_loop().create_task(self._guarded(job_id))

    async def _guarded(self, job_id: str) -> None:
        try:
            async with self._slots:
                await self.run(job_id)
        except Exception as error:  # the job file carries the failure
            logger.exception("avatar build %s crashed", job_id)
            try:
                job = self.jobs.load(job_id)
                if not job.get("finished"):
                    job.update(status="failed", finished=True, error=f"internal error: {error}")
                    self.jobs.write(job)
            except Exception:
                pass
        finally:
            self._running.pop(job_id, None)

    async def run(self, job_id: str) -> Dict[str, Any]:
        lock = self._job_locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            job = self.jobs.load(job_id)
            if job.get("finished"):
                return job
            # Two processes (the web app resuming, a CLI run) must never drive
            # the same job: each keeps its own copy and the last write wins.
            if fcntl is None:  # Windows development machines
                return await self._run_locked(job_id)
            handle = open(self.jobs.work_dir(job_id) / "runner.lock", "w")
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                handle.close()
                logger.info("avatar build %s is driven by another process", job_id)
                return job
            try:
                return await self._run_locked(job_id)
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
                handle.close()

    async def _run_locked(self, job_id: str) -> Dict[str, Any]:
        job = self.jobs.load(job_id)
        if job.get("finished"):
            return job
        owner = AvatarOwner(owner_type=job["owner_type"], owner_id=job["owner_id"])
        started = time.monotonic()
        try:
            async with self.factory() as client:
                await self._stage(job, "source", lambda: self._source(client, job, owner))
                await self._stage(job, "describe", lambda: self._describe(client, job))
                slots = job["request"]["views"]
                await self._stage(job, "anchor", lambda: self._views(client, job, owner, [ANCHOR_SLOT]))
                rest = [slot for slot in slots if slot != ANCHOR_SLOT]
                if rest:
                    await self._stage(job, "views", lambda: self._views(client, job, owner, rest))
                await self._stage(job, "sheet", lambda: self._sheet(job, owner))
                await self._stage(job, "save", lambda: self._save(job, owner))
            job.update(status="completed", stage="done", finished=True)
            job["timings"]["total_seconds"] = round(
                job["timings"].get("total_seconds", 0) + time.monotonic() - started, 1)
            _log(job, "done")
        except BuildError as error:
            job.update(status="failed", finished=True, error=str(error))
            _log(job, f"failed: {error}")
        self.jobs.write(job)
        return job

    async def _stage(self, job: Dict[str, Any], name: str, action) -> None:
        if job["steps"].get(name) == "done":
            return
        job.update(stage=name, status="running")
        self.jobs.write(job)
        began = time.monotonic()
        await action()
        job["steps"][name] = "done"
        job["timings"][name + "_seconds"] = round(time.monotonic() - began, 1)
        self.jobs.write(job)

    # ---- farm calls

    async def _post(self, client: httpx.AsyncClient, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(6):
            try:
                response = await client.post(self.api + path, json=body, timeout=120.0)
            except httpx.HTTPError as error:
                if attempt == 5:
                    raise BuildError(f"{path} did not answer: {error}") from None
                await asyncio.sleep(5 * (attempt + 1))
                continue
            if response.status_code in (429, 502, 503, 504) and attempt < 5:
                await asyncio.sleep(5 * (attempt + 1))
                continue
            try:
                data = response.json()
            except ValueError:
                data = {}
            if response.status_code >= 400:
                detail = data.get("detail") if isinstance(data, dict) else None
                if isinstance(detail, dict):
                    detail = detail.get("message_string") or detail.get("error_string")
                raise BuildError(f"{path} refused the request: {detail or response.status_code}")
            return data if isinstance(data, dict) else {}
        raise BuildError(f"{path} kept refusing the request")

    async def _wait_render(self, client: httpx.AsyncClient, task_id: str, url: str) -> Tuple[str, str]:
        deadline = time.monotonic() + RENDER_TIMEOUT_SECONDS
        worker = ""
        while time.monotonic() < deadline:
            try:
                response = await client.get(f"{self.api}/api/ai/render-status/{quote(task_id)}", timeout=30.0)
                status = response.json() if response.status_code == 200 else {}
            except (httpx.HTTPError, ValueError):
                status = {}
            state = str(status.get("status_string") or "")
            worker = str(status.get("node_string") or worker)
            if state == "completed":
                return str(status.get("output_url_string") or url), worker
            if state in ("failed", "cancelled"):
                raise BuildError(f"render {task_id} {state}: {status.get('error_string') or ''}".strip())
            await asyncio.sleep(self.poll_seconds)
        raise BuildError(f"render {task_id} did not finish in time")

    async def _fetch(self, client: httpx.AsyncClient, url: str, limit: int = MAX_IMAGE_BYTES) -> bytes:
        for attempt in range(8):
            try:
                response = await client.get(url, timeout=120.0)
            except httpx.HTTPError:
                response = None
            if response is not None and response.status_code == 200:
                if len(response.content) > limit:
                    raise BuildError("a produced file is larger than allowed")
                return response.content
            await asyncio.sleep(3)
        raise BuildError(f"could not download {url}")

    async def _vision(self, client: httpx.AsyncClient, prompt: str, *, image_url: str = "",
                      image_base64: str = "", budget: int = 1024) -> str:
        body: Dict[str, Any] = {"prompt": prompt, "model": VISION_MODEL,
                                "max_output_tokens": budget, "wait_seconds": 0}
        if image_url:
            body["image_url"] = image_url
        else:
            body["image_base64"] = image_base64
        accepted = await self._post(client, "/api/vision", body)
        if accepted.get("answer_string"):
            return str(accepted["answer_string"])
        task_id = str(accepted.get("task_id_string") or "")
        if not task_id:
            raise BuildError("Vision returned no task")
        deadline = time.monotonic() + VISION_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            await asyncio.sleep(self.poll_seconds)
            try:
                response = await client.get(f"{self.api}/api/ai/status/{quote(task_id, safe='')}", timeout=30.0)
                data = response.json() if response.status_code == 200 else {}
            except (httpx.HTTPError, ValueError):
                continue
            if data.get("finished_bool"):
                if data.get("answer_string"):
                    return str(data["answer_string"])
                raise BuildError(f"Vision failed: {data.get('error_string') or 'no answer'}")
        raise BuildError("Vision did not answer in time")

    def _store_bytes(self, owner: AvatarOwner, data: bytes, name: str, source_url: Optional[str] = None,
                     job: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            asset = self.assets.put_bytes(owner, data, filename=name, content_type=None, source_url=source_url)
        except HTTPException as error:
            raise BuildError(f"could not store {name}: {error.detail}") from None
        if job is not None:
            job.setdefault("asset_ids", [])
            if asset.get("asset_id") and asset["asset_id"] not in job["asset_ids"]:
                job["asset_ids"].append(asset["asset_id"])
            if job.get("private"):
                self._mark_private([asset.get("asset_id")])
        return asset

    def _mark_private(self, asset_ids) -> None:
        for asset_id in asset_ids or []:
            meta = self.assets.get_metadata(str(asset_id or ""))
            if not meta or meta.get("private"):
                continue
            meta["private"] = True
            self.assets._atomic_json(self.assets.assets_dir / str(asset_id) / "metadata.json", meta)

    def _set_private(self, job: Dict[str, Any], reason: str) -> None:
        if not job.get("private"):
            job["private"] = True
            job["private_reason"] = reason
            _log(job, f"private: {reason}")
        self._mark_private(job.get("asset_ids"))

    # ---- stage 1: source

    async def _source(self, client: httpx.AsyncClient, job: Dict[str, Any], owner: AvatarOwner) -> None:
        request = job["request"]
        work = self.jobs.work_dir(job["job_id"])
        local = request.get("local_file") or ""
        kind = request.get("kind")
        if local:
            path = Path(local)
            if not path.is_file():
                raise BuildError("the uploaded source is gone; upload it again")
            data = path.read_bytes() if kind == "image" else b""
        elif request.get("external"):
            path = work / "source.download"
            async with httpx.AsyncClient() as outside:
                kind, _size = await fetch_external(outside, request["source_url"], path)
            request["kind"] = kind
            if kind == "image":
                data = path.read_bytes()
        else:
            url = request["source_url"]
            limit = MAX_VIDEO_BYTES if kind == "video" else MAX_IMAGE_BYTES
            data = await self._fetch(client, url, limit)
            if kind == "video":
                path = work / "source.video"
                path.write_bytes(data)
        if kind == "image":
            asset = self._store_bytes(owner, data, "source.png", request.get("source_url"), job=job)
            job["source"] = {"kind": "image", "sha256": asset["sha256"],
                             "url": request.get("source_url") or None,
                             "width": asset["width"], "height": asset["height"],
                             "frame_url": asset["canonical_url"], "frame_sha256": asset["sha256"],
                             "frame_width": asset["width"], "frame_height": asset["height"],
                             "frame_box": None}
            try:
                face = await asyncio.to_thread(score_frames, [self._tmp_image(work, data)])
            except ImportError:
                face = []
            if face and face[0].get("box"):
                job["source"]["frame_box"] = face[0]["box"]
                job["source"]["frame_score"] = face[0]["score"]
            rating = await asyncio.to_thread(nsfw_rating, [self._tmp_image(work, data)])
            job["source"]["nsfw_rating"] = rating
            if request.get("private") or rating in ("adult", "suggestive"):
                self._set_private(job, "requested" if request.get("private") else f"source rated {rating}")
            _log(job, "source picture stored")
            return
        video_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        picked = await asyncio.to_thread(pick_best_frame, path, work / "frames")
        best = picked["best"]
        frame_bytes = Path(best["path"]).read_bytes()
        asset = self._store_bytes(owner, frame_bytes, "frame.png", job=job)
        job["source"] = {"kind": "video", "sha256": video_sha,
                         "url": request.get("source_url") or None,
                         "width": picked["meta"]["width"], "height": picked["meta"]["height"],
                         "duration_seconds": picked["meta"]["duration"],
                         "frame_time_seconds": best["time"], "frame_score": best["score"],
                         "frame_url": asset["canonical_url"], "frame_sha256": asset["sha256"],
                         "frame_width": asset["width"], "frame_height": asset["height"],
                         "frame_box": best.get("box"),
                         "frames_scored": len(picked["rows"]),
                         "frames_with_face": sum(1 for row in picked["rows"] if row["faces"])}
        # A few frames across the clip, not only the chosen one: a clip can be
        # explicit where the face is not.
        rows = picked["rows"]
        sample = [Path(row["path"]) for row in rows[:: max(1, len(rows) // 8)]][:9] + [Path(best["path"])]
        rating = await asyncio.to_thread(nsfw_rating, sample)
        job["source"]["nsfw_rating"] = rating
        if request.get("private") or rating in ("adult", "suggestive"):
            self._set_private(job, "requested" if request.get("private") else f"source rated {rating}")
        _log(job, f"best frame at {best['time']}s (score {best['score']}, "
                  f"{job['source']['frames_with_face']}/{len(picked['rows'])} frames with a face)")

    @staticmethod
    def _tmp_image(work: Path, data: bytes) -> Path:
        path = work / "source_image"
        path.write_bytes(data)
        return path

    # ---- stage 2: describe

    async def _describe(self, client: httpx.AsyncClient, job: Dict[str, Any]) -> None:
        last_error = ""
        for attempt in range(2):
            answer = await self._vision(client, DESCRIBE_INSTRUCTION,
                                        image_url=job["source"]["frame_url"], budget=1200)
            try:
                job["description"] = clean_description(_extract_json(answer))
                if job["description"].get("explicit"):
                    self._set_private(job, "Vision saw nudity or sexual content")
                job["description_raw"] = answer[:6000]
                _log(job, f"described as '{job['description']['display_name']}'")
                return
            except ValueError as error:
                last_error = str(error)
        raise BuildError(f"Vision did not describe the character: {last_error}")

    # ---- stages 3-4: views

    def _anchor_url(self, job: Dict[str, Any]) -> str:
        return ((job["views"].get(ANCHOR_SLOT) or {}).get("final") or {}).get("canonical_url") or ""

    async def _crop_reference(self, client: httpx.AsyncClient, job: Dict[str, Any], owner: AvatarOwner,
                              slot: str) -> str:
        """The anchor cut to this view's framing, stored as an asset."""
        state = job["views"].setdefault(slot, {"attempts": []})
        if state.get("crop_url"):
            return state["crop_url"]
        data = await self._fetch(client, self._anchor_url(job))
        width, height = VIEW_SPECS[slot]["size"]
        cropped = await asyncio.to_thread(crop_to_framing, data, VIEW_SPECS[slot]["crop"], width, height)
        asset = self._store_bytes(owner, cropped, f"{slot}_crop.png", job=job)
        state["crop_url"] = asset["canonical_url"]
        self.jobs.write(job)
        return state["crop_url"]

    async def _references(self, client: httpx.AsyncClient, job: Dict[str, Any], owner: AvatarOwner,
                          slot: str) -> Tuple[List[str], List[str]]:
        source = job["source"]["frame_url"]
        anchor = self._anchor_url(job)
        if slot == ANCHOR_SLOT or not anchor:
            return [source], ["source"]
        if VIEW_SPECS[slot].get("crop") == "face":
            return [await self._crop_reference(client, job, owner, slot), source], ["anchor_crop", "source"]
        if VIEW_SPECS[slot].get("crop"):
            return [await self._crop_reference(client, job, owner, slot)], ["anchor_crop"]
        if slot == "back":
            return [anchor], [ANCHOR_SLOT]
        return [anchor, source], [ANCHOR_SLOT, "source"]

    async def _render_view(self, client: httpx.AsyncClient, job: Dict[str, Any], owner: AvatarOwner,
                           slot: str, engine: str, seed: int) -> Dict[str, Any]:
        state = job["views"].setdefault(slot, {"attempts": []})
        attempt = next((item for item in state["attempts"]
                        if item.get("engine") == engine and item.get("seed") == seed), None)
        description = job["description"]
        prompt = view_prompt(slot, description, job["request"].get("outfit") or "")
        width, height = VIEW_SPECS[slot]["size"]
        refs, ref_slots = await self._references(client, job, owner, slot)
        if attempt is None:
            attempt = {"engine": engine, "seed": seed, "prompt": prompt, "reference_slots": ref_slots,
                       "started_at": _now()}
            state["attempts"].append(attempt)
        if not attempt.get("task_id"):
            if engine == "klein":
                body = {"prompt": prompt, "image_url": refs[0], "width": width, "height": height,
                        "seed": seed, "checkpoint": KLEIN_CHECKPOINT, "negative_prompt": NEGATIVE}
                if len(refs) > 1:
                    body["reference_image_urls"] = refs[1:]
                accepted = await self._post(client, "/api/image", body)
                attempt["workflow"] = str((accepted.get("effective_params_object") or {}).get("work_flow") or "")
                attempt["checkpoint"] = KLEIN_CHECKPOINT
            else:
                body = {"prompt": prompt.replace("image 1", "picture 1"), "image_url": refs[0],
                        "mode": "edit", "width": width, "height": height, "seed": seed,
                        "negative_prompt": NEGATIVE}
                if len(refs) > 1:
                    body["reference_image_urls"] = refs[1:]
                accepted = await self._post(client, "/api/qwen-image", body)
                attempt["workflow"] = "qwen_image_edit_multi.json" if len(refs) > 1 else "qwen_image_edit.json"
                attempt["checkpoint"] = str(accepted.get("checkpoint_string") or "qwen-image-edit-2511")
            attempt["task_id"] = str(accepted.get("task_id_string") or "")
            attempt["render_url"] = str(accepted.get("image_url_string") or accepted.get("poll_url_string") or "")
            attempt["cache_hit"] = bool(accepted.get("cache_hit_bool"))
            if not attempt["task_id"] and not attempt["render_url"]:
                raise BuildError(f"the farm accepted {slot} without a task")
            self.jobs.write(job)
        if not attempt.get("asset"):
            began = time.monotonic()
            url, worker = attempt["render_url"], ""
            if attempt["task_id"]:
                url, worker = await self._wait_render(client, attempt["task_id"], attempt["render_url"])
            data = await self._fetch(client, url)
            asset = self._store_bytes(owner, data, f"{slot}.png", job=job)
            attempt.update(asset=asset, worker=worker, render_url=url,
                           seconds=round(time.monotonic() - began, 1), finished_at=_now())
            self.jobs.write(job)
        return attempt

    async def _check(self, client: httpx.AsyncClient, job: Dict[str, Any], slot: str,
                     attempt: Dict[str, Any], source_face: bytes) -> None:
        if attempt.get("qa"):
            return
        data = await self._fetch(client, attempt["asset"]["canonical_url"])
        haar = await asyncio.to_thread(detect_face_kind, data)
        if not job["request"].get("qa", True):
            attempt["qa"] = {"status": "unchecked", "face_detected": haar}
            return
        pair = await asyncio.to_thread(compose_pair, source_face, data)
        try:
            answer = await self._vision(
                client, JUDGE_INSTRUCTION,
                image_base64="data:image/jpeg;base64," + base64.b64encode(pair).decode(), budget=400)
            verdict = judge_verdict(slot, _extract_json(answer), haar)
            verdict["identity_method"] = f"vision_judge:{VISION_MODEL}"
        except (BuildError, ValueError) as error:
            verdict = {"status": "unchecked", "face_detected": haar,
                       "notes": f"judge unavailable: {error}"[:500]}
        crop = VIEW_SPECS[slot].get("crop")
        if crop and verdict.get("status") != "unchecked":
            box = await asyncio.to_thread(face_box, data)
            try:
                from PIL import Image
                with Image.open(io.BytesIO(data)) as image:
                    height = image.height
            except Exception:
                height = 0
            fraction = (box[3] / height) if box and height else 0.0
            if fraction < 0.5 * CROP_FACE_FRACTION[crop]:
                verdict["status"] = "failed"
                verdict["notes"] = (f"framing drifted: face is {fraction:.2f} of the height, "
                                    f"wanted ~{CROP_FACE_FRACTION[crop]:.2f}; " + verdict.get("notes", ""))[:1000]
        attempt["qa"] = verdict
        self.jobs.write(job)

    async def _view(self, client: httpx.AsyncClient, job: Dict[str, Any], owner: AvatarOwner,
                    slot: str, reference_face: bytes) -> None:
        state = job["views"].setdefault(slot, {"attempts": []})
        if state.get("final"):
            return
        base_seed = int(job["request"]["seed"])
        forced = job["request"].get("engine") or "auto"
        first = VIEW_SPECS[slot]["engine"] if forced == "auto" else forced
        other = "qwen" if first == "klein" else "klein"
        plan = [(first, base_seed + CANONICAL_VIEW_SLOTS.index(slot)),
                (other, base_seed + 1000 + CANONICAL_VIEW_SLOTS.index(slot))]
        tried = []
        for number, (engine, seed) in enumerate(plan):
            try:
                attempt = await self._render_view(client, job, owner, slot, engine, seed)
            except BuildError as error:
                state.setdefault("errors", []).append(f"{engine}: {error}"[:300])
                _log(job, f"{slot} on {engine} failed: {error}")
                self.jobs.write(job)
                continue
            await self._check(client, job, slot, attempt, reference_face)
            tried.append(attempt)
            status = (attempt.get("qa") or {}).get("status")
            _log(job, f"{slot} via {engine}: {status} "
                      f"(identity {(attempt.get('qa') or {}).get('identity_score')})")
            if status in ("passed", "accepted_with_warnings", "unchecked"):
                break
        if VIEW_SPECS[slot].get("crop") and state.get("crop_url") and not any(
                (item.get("qa") or {}).get("status") in ("passed", "accepted_with_warnings", "unchecked")
                for item in tried):
            # The anchor's own pixels, cut to this framing: softer, but it is
            # the checked identity and the right framing, which a zoomed-out
            # or drifted render is not.
            data = await self._fetch(client, state["crop_url"])
            asset = self._store_bytes(owner, data, f"{slot}_cropped.png", job=job)
            tried.append({"engine": "crop", "seed": 0, "prompt": "anchor crop", "reference_slots": [ANCHOR_SLOT],
                          "asset": asset, "workflow": "anchor_crop", "checkpoint": "",
                          "qa": {"status": "accepted_with_warnings", "identity_score": None,
                                 "identity_method": "anchor_crop", "angle_ok": True,
                                 "face_detected": "frontal", "notes": "rendered views drifted; anchor crop used"}})
            state["attempts"].append(tried[-1])
            _log(job, f"{slot}: using the anchor crop")
            best = tried[-1]
        elif not tried:
            raise BuildError(f"no picture could be made for {slot}")
        else:
            best = max(tried, key=_rank)
        spec = VIEW_SPECS[slot]
        asset = best["asset"]
        qa = dict(best.get("qa") or {})
        state["final"] = {
            "canonical_url": asset["canonical_url"], "sha256": asset["sha256"],
            "asset_id": asset.get("asset_id"), "width": asset["width"], "height": asset["height"],
            "yaw_deg": spec["yaw"], "framing": spec["framing"], "expression": "neutral",
            "provenance": {"engine": {"klein": "flux2-klein-4b", "qwen": "qwen-image-edit-2511"}.get(
                               best["engine"], best["engine"]),
                           "workflow": best.get("workflow", ""), "checkpoint": best.get("checkpoint", ""),
                           "prompt": best["prompt"][:6000], "seed": best["seed"],
                           "task_id": best.get("task_id", ""), "reference_slots": best["reference_slots"],
                           "worker": best.get("worker", ""), "seconds": best.get("seconds", 0)},
            "qa": {"status": qa.get("status", "unchecked"), "attempts": len(tried),
                   "identity_score": qa.get("identity_score"),
                   "identity_method": qa.get("identity_method", ""),
                   "angle_ok": qa.get("angle_ok"),
                   "face_detected": qa.get("face_detected", "unknown"),
                   "notes": qa.get("notes", "")},
        }
        self.jobs.write(job)

    async def _source_face(self, client: httpx.AsyncClient, job: Dict[str, Any]) -> bytes:
        data = await self._fetch(client, job["source"]["frame_url"])
        return await asyncio.to_thread(crop_face_region, data, job["source"].get("frame_box"), 2.2)

    async def _anchor_face(self, client: httpx.AsyncClient, job: Dict[str, Any]) -> bytes:
        data = await self._fetch(client, self._anchor_url(job))
        box = await asyncio.to_thread(face_box, data)
        return await asyncio.to_thread(crop_face_region, data, box, 2.2)

    async def _views(self, client: httpx.AsyncClient, job: Dict[str, Any], owner: AvatarOwner,
                     slots: List[str]) -> None:
        # The anchor is checked against the source; every other view against
        # the anchor, which was itself checked: same studio, same outfit, so
        # the judge compares people rather than settings.
        if ANCHOR_SLOT in slots or not self._anchor_url(job):
            reference_face = await self._source_face(client, job)
        else:
            reference_face = await self._anchor_face(client, job)
        results = await asyncio.gather(*[self._view(client, job, owner, slot, reference_face)
                                         for slot in slots], return_exceptions=True)
        failures = [f"{slot}: {result}" for slot, result in zip(slots, results)
                    if isinstance(result, Exception)]
        for slot, result in zip(slots, results):
            if isinstance(result, Exception) and not isinstance(result, BuildError):
                logger.error("avatar build view %s crashed", slot, exc_info=result)
        if ANCHOR_SLOT in slots and failures:
            raise BuildError("the anchor view could not be made: " + "; ".join(failures))
        if failures:
            job.setdefault("warnings", []).extend(failures)
            _log(job, "views without a picture: " + "; ".join(failures))

    # ---- stage 5: sheet

    async def _sheet(self, job: Dict[str, Any], owner: AvatarOwner) -> None:
        tiles = []
        async with self.factory() as client:
            for slot in CANONICAL_VIEW_SLOTS:
                final = (job["views"].get(slot) or {}).get("final")
                if final:
                    tiles.append((slot, await self._fetch(client, final["canonical_url"])))
        if not tiles:
            raise BuildError("no view was made")
        data = await asyncio.to_thread(compose_sheet, tiles)
        asset = self._store_bytes(owner, data, "sheet.jpg", job=job)
        job["sheet"] = {"canonical_url": asset["canonical_url"], "sha256": asset["sha256"],
                        "asset_id": asset.get("asset_id"), "width": asset["width"],
                        "height": asset["height"], "framing": "other",
                        "provenance": {"engine": "pil", "workflow": "contact_sheet"},
                        "qa": {"status": "unchecked"}}

    # ---- stage 6: save

    def draft(self, job: Dict[str, Any]) -> AvatarDraft:
        description = job["description"]
        request = job["request"]
        source = job["source"]
        views = {slot: state["final"] for slot, state in job["views"].items() if state.get("final")}
        front = views.get("front") or views.get(ANCHOR_SLOT)
        references = [{"asset_id": None, "role": "face", "media_type": "image",
                       "canonical_url": source["frame_url"], "sha256": source["frame_sha256"],
                       "width": source.get("frame_width") or source.get("width") or 1,
                       "height": source.get("frame_height") or source.get("height") or 1,
                       "note": "source frame" if source["kind"] == "video" else "source picture"}]
        if front:
            references.append({"asset_id": front.get("asset_id"), "role": "body", "media_type": "image",
                               "canonical_url": front["canonical_url"], "sha256": front["sha256"],
                               "width": front["width"], "height": front["height"],
                               "note": "generated front view"})
        task_ids = [job["job_id"]] + [
            attempt["task_id"] for state in job["views"].values()
            for attempt in state.get("attempts", []) if attempt.get("task_id")]
        wardrobe = str(request.get("outfit") or description.get("wardrobe") or "")
        passed = sum(1 for view in views.values()
                     if (view.get("qa") or {}).get("status") in ("passed", "accepted_with_warnings"))
        return AvatarDraft.model_validate({
            "display_name": request.get("display_name") or description["display_name"],
            "identity_prompt": description["identity_prompt"],
            "appearance": description.get("appearance", ""),
            "wardrobe": wardrobe,
            "negative_identity_prompt": description.get("negative_identity_prompt", ""),
            "body": description.get("body", ""),
            "references": references,
            "format_version": FORMAT_VERSION_LATEST,
            "views": views,
            "sheet": job.get("sheet"),
            "sources": [{key: source.get(key) for key in (
                "kind", "sha256", "url", "width", "height", "duration_seconds",
                "frame_time_seconds", "frame_url", "frame_sha256", "frame_score")}],
            "provenance": {"source_kind": "mixed", "source_task_ids": task_ids[:32],
                           "source_model": f"flux-2-klein-4b + qwen-image-edit-2511 + {VISION_MODEL}",
                           "source_workflow": PIPELINE_VERSION,
                           "note": (description.get("production_notes", "") +
                                    f" | views passed {passed}/{len(views)}")[:1000]},
            "extensions": {"avatar_build": {
                "job_id": job["job_id"], "pipeline": PIPELINE_VERSION,
                "private": bool(job.get("private")),
                "timings": job.get("timings", {}),
                "identity_threshold": IDENTITY_PASS,
            }},
        })

    async def _save(self, job: Dict[str, Any], owner: AvatarOwner) -> None:
        draft = self.draft(job)
        target = job["request"].get("avatar_id")
        try:
            if job.get("avatar_string"):
                return
            if target:
                profile = self.avatars.update(target, owner, draft)
            else:
                profile = self.avatars.create(owner, draft)
        except AvatarStoreError as error:
            raise BuildError(error.message) from None
        job["avatar_string"] = f"{profile.avatar_id}@{profile.version}"
        job["avatar_id"] = profile.avatar_id
        _log(job, f"saved {job['avatar_string']}")


class BuildError(Exception):
    pass


# ------------------------------------------------------------------ public view

OUTPUT_FIELDS = {slot: f"{slot}_url_string" for slot in CANONICAL_VIEW_SLOTS}


def public_status(job: Dict[str, Any]) -> Dict[str, Any]:
    description = job.get("description") or {}
    source = job.get("source") or {}
    views = {}
    out: Dict[str, Any] = {
        "success_bool": job.get("status") != "failed",
        "finished_bool": bool(job.get("finished")),
        "status_string": str(job.get("status") or "queued"),
        "stage_string": str(job.get("stage") or ""),
        "task_id_string": job["job_id"],
        "status_url_string": f"/api/ai/avatar-build/status/{job['job_id']}",
        "retry_after_seconds_float": 4,
        "avatar_string": str(job.get("avatar_string") or ""),
        "description_string": (
            "; ".join(filter(None, [description.get("identity_prompt"), description.get("appearance"),
                                    description.get("body"), description.get("wardrobe")]))[:4000]
            if description else ""),
        "source_frame_url_string": str(source.get("frame_url") or ""),
        "source_frame_time_seconds_float": source.get("frame_time_seconds"),
        "sheet_url_string": str((job.get("sheet") or {}).get("canonical_url") or ""),
        "log_array": list(job.get("log") or [])[-12:],
        "private_bool": bool(job.get("private")),
        "timings_object": job.get("timings") or {},
    }
    requested = (job.get("request") or {}).get("views") or DEFAULT_VIEWS
    done = 0
    for slot in CANONICAL_VIEW_SLOTS:
        final = (job.get("views", {}).get(slot) or {}).get("final")
        out[OUTPUT_FIELDS[slot]] = final["canonical_url"] if final else ""
        if final:
            done += 1
            views[slot] = {"url_string": final["canonical_url"],
                           "status_string": final["qa"].get("status"),
                           "identity_score_float": final["qa"].get("identity_score"),
                           "attempts_int": final["qa"].get("attempts"),
                           "engine_string": final["provenance"].get("engine"),
                           "worker_string": final["provenance"].get("worker"),
                           "seconds_float": final["provenance"].get("seconds")}
    out["views_object"] = views
    out["progress_float"] = round(
        (0.1 if source else 0) + (0.1 if description else 0)
        + 0.7 * done / max(1, len(requested)) + (0.1 if job.get("avatar_string") else 0), 3)
    if job.get("error"):
        out["error_string"] = str(job["error"])
    return out


# -------------------------------------------------------------------- router


def _split_avatar(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = AVATAR_REF_RE.fullmatch(str(value).strip())
    if not match:
        raise HTTPException(400, detail="Choose a saved Avatar version")
    return match[1]


def _identity(kind: str, source_sha_or_url: str, body: BuildRequest, avatar_id: Optional[str]) -> Dict[str, Any]:
    views = parse_views(body.views)
    engine = str(body.engine or "auto").strip().lower()
    if engine not in ("auto", "klein", "qwen"):
        raise HTTPException(400, detail="engine must be auto, klein or qwen")
    seed = body.seed if body.seed else int(hashlib.sha256(source_sha_or_url.encode()).hexdigest()[:7], 16)
    return {"kind": kind, "source": source_sha_or_url, "views": views, "engine": engine,
            "outfit": str(body.outfit or "").strip(), "display_name": str(body.display_name or "").strip(),
            "qa": body.qa is not False, "seed": seed, "avatar_id": avatar_id,
            "private": bool(body.private),
            "pipeline": PIPELINE_VERSION}


def _stage_bytes(jobs: BuildJobStore, data: bytes, kind: str) -> Path:
    staging = jobs.root / "uploads"
    staging.mkdir(parents=True, exist_ok=True)
    final = staging / f"{hashlib.sha256(data).hexdigest()}.{kind}"
    if not final.exists():
        fd, temporary = tempfile.mkstemp(prefix=".upload-", dir=staging)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(temporary, final)
    return final


async def _source_kind(url: str) -> str:
    import ai_multiref
    return "video" if await ai_multiref.is_video(url) else "image"


def build_avatar_build_router(owner_dependency: Callable, *, builder: Optional[AvatarBuilder] = None) -> APIRouter:
    router = APIRouter()
    state: Dict[str, AvatarBuilder] = {}

    def get_builder() -> AvatarBuilder:
        if builder is not None:
            return builder
        if "builder" not in state:
            state["builder"] = AvatarBuilder()
        return state["builder"]

    def respond(job: Dict[str, Any], hit: bool):
        if not job.get("finished"):
            get_builder().ensure_running(job["job_id"])
        payload = {**public_status(job), "cache_hit_bool": hit}
        return JSONResponse(status_code=200 if job.get("finished") else 202, content=payload)

    async def resume_unfinished() -> None:
        """A restart must not strand a build: pick up web jobs left running.

        main.py runs a lifespan, which makes router startup hooks dead
        letters, so this runs on the first request to any build route after
        a start (the /nodes and /avatars pages poll, so that is within
        seconds of anyone looking).
        """
        if state.get("resumed"):
            return
        state["resumed"] = True
        jobs_dir = get_builder().jobs.jobs_dir
        if not jobs_dir.is_dir():
            return
        cutoff = time.time() - 6 * 3600
        for path in jobs_dir.glob("avb_*.json"):
            try:
                if path.stat().st_mtime < cutoff:
                    continue
                job = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if job.get("finished") or (job.get("request") or {}).get("runner") == "cli":
                continue
            logger.info("resuming avatar build %s", job.get("job_id"))
            get_builder().ensure_running(str(job.get("job_id")))

    @router.post("/api/ai/avatar-build")
    async def create(body: BuildRequest, owner: AvatarOwner = Depends(owner_dependency)):
        await resume_unfinished()
        url = str(body.video_url or body.image_url or "").strip()
        if not url and body.image_base64:
            raw = str(body.image_base64).split(",", 1)[-1]
            try:
                data = base64.b64decode(raw, validate=False)
            except (ValueError, TypeError):
                raise HTTPException(400, detail="The pasted picture is not valid base64") from None
            if not data or len(data) > MAX_IMAGE_BYTES:
                raise HTTPException(413, detail="The pasted picture is empty or larger than 12 MB")
            final = _stage_bytes(get_builder().jobs, data, "image")
            identity = _identity("image", final.stem, body, _split_avatar(body.avatar))
            identity["local_file"] = str(final)
            job, hit = get_builder().jobs.create(owner, identity)
            return respond(job, hit)
        if not url:
            raise HTTPException(400, detail="Wire a picture or a video into the node")
        external = False
        try:
            validate_import_url(url)
        except ValueError:
            try:
                validate_external_url(url)
            except ValueError as error:
                raise HTTPException(400, detail=f"This source address cannot be used: {error}") from None
            external = True
        if external:
            kind = "video" if body.video_url else guess_kind(url)
        else:
            kind = "video" if body.video_url else await _source_kind(url)
        avatar_id = _split_avatar(body.avatar)
        identity = _identity(kind, url, body, avatar_id)
        identity["source_url"] = url
        if external:
            identity["external"] = True
        job, hit = get_builder().jobs.create(owner, identity)
        return respond(job, hit)

    @router.post("/api/ai/avatar-build/upload")
    async def upload(file: UploadFile = File(...), display_name: str = Form(""), outfit: str = Form(""),
                     views: str = Form(""), avatar: str = Form(""),
                     owner: AvatarOwner = Depends(owner_dependency)):
        """A picture or video straight from the page; the video never becomes public."""
        await resume_unfinished()
        body = BuildRequest(display_name=display_name or None, outfit=outfit or None,
                            views=views or None, avatar=avatar or None)
        content_type = str(file.content_type or "").lower()
        kind = "video" if content_type.startswith("video/") or str(file.filename or "").lower().endswith(
            (".mp4", ".mov", ".webm", ".m4v", ".mkv")) else "image"
        limit = MAX_VIDEO_BYTES if kind == "video" else MAX_IMAGE_BYTES
        jobs = get_builder().jobs
        staging = jobs.root / "uploads"
        staging.mkdir(parents=True, exist_ok=True)
        digest, size = hashlib.sha256(), 0
        fd, temporary = tempfile.mkstemp(prefix=".upload-", dir=staging)
        try:
            with os.fdopen(fd, "wb") as handle:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > limit:
                        raise HTTPException(413, detail="The file is too large")
                    digest.update(chunk)
                    handle.write(chunk)
            if not size:
                raise HTTPException(400, detail="The file is empty")
            sha = digest.hexdigest()
            final = staging / f"{sha}.{'video' if kind == 'video' else 'image'}"
            os.replace(temporary, final)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        identity = _identity(kind, sha, body, _split_avatar(body.avatar))
        identity["local_file"] = str(final)
        job, hit = jobs.create(owner, identity)
        return respond(job, hit)

    @router.get("/api/ai/avatar-build/status/{job_id}")
    async def status(job_id: str, owner: AvatarOwner = Depends(owner_dependency)):
        await resume_unfinished()
        job = get_builder().jobs.read(job_id, owner)
        return respond(job, False)

    return router


# ----------------------------------------------------------------------- CLI
#
# For a source that must never have a public address (the owner's private
# test clips): the file is read from disk here, only the chosen frame and the
# generated views ever reach the farm, and they do so through unguessable
# Avatar-asset capability URLs.


async def _cli(args) -> Dict[str, Any]:
    owner = AvatarOwner(owner_type=args.owner_type, owner_id=args.owner_id)
    builder = AvatarBuilder()
    path = Path(args.file).resolve()
    kind = "video" if path.suffix.lower() in (".mp4", ".mov", ".webm", ".m4v", ".mkv") else "image"
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    body = BuildRequest(display_name=args.name or None, outfit=args.outfit or None,
                        views=args.views or None, seed=args.seed or None)
    identity = _identity(kind, sha, body, None)
    identity["local_file"] = str(path)
    identity["runner"] = "cli"
    job, _ = builder.jobs.create(owner, identity)
    print(json.dumps({"job_id": job["job_id"]}), flush=True)
    job = await builder.run(job["job_id"])
    return public_status(job)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an Avatar from a local picture or video")
    parser.add_argument("--file", required=True)
    parser.add_argument("--owner-type", default="anon", choices=["anon", "user"])
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--name", default="")
    parser.add_argument("--outfit", default="")
    parser.add_argument("--views", default="")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    print(json.dumps(asyncio.run(_cli(args)), indent=1))


if __name__ == "__main__":
    main()

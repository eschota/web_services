"""The picture catalogue of checkpoints and LoRAs the farm can run.

A model is recognised by what it produces, not by its file name, so the picker
shows the preview Civitai displays on the model page. Those previews are kept
on our own host: hot-linking breaks the moment an image URL changes or a
referrer is blocked, and a picker full of dead thumbnails is worse than one
with none.

The catalogue also says what the farm *cannot* run. A checkpoint for an
architecture none of the deployed workflows load still belongs in the list —
somebody asked for it and it is on the disk — but it has to say so rather than
sit in a dropdown that fails at render time.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import re
import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

logger = logging.getLogger(__name__)

router = APIRouter()

CATALOGUE_DIR = pathlib.Path(
    os.getenv("AUTORIG_AI_MODEL_DIR", "/srv/autorig/data/var/ai-models")
)
CATALOGUE_FILE = CATALOGUE_DIR / "model_catalogue.json"
PREVIEW_DIR = CATALOGUE_DIR / "previews"
SAFE_PREVIEW = re.compile(r"^[A-Za-z0-9._-]{1,200}\.jpg$")

# Refreshed from disk rather than cached forever: the catalogue is a file
# somebody drops a new model into, and a restart should not be the way to
# publish one.
_cache: List[Dict[str, object]] = []
_cache_at = 0.0
CACHE_TTL_SECONDS = 30.0


def entries() -> List[Dict[str, object]]:
    global _cache, _cache_at
    if _cache and (time.monotonic() - _cache_at) < CACHE_TTL_SECONDS:
        return _cache
    try:
        raw = json.loads(CATALOGUE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        logger.warning("No model catalogue at %s", CATALOGUE_FILE)
        raw = []
    except Exception:
        logger.exception("Could not read the model catalogue")
        raw = []
    _cache = []
    for entry in raw:
        if not isinstance(entry, dict) or not entry.get("file"):
            continue
        # Normalised here so no caller has to guess whether a key is present:
        # a catalogue hand-edited by somebody adding a model should not be able
        # to break a picker by leaving a field out.
        entry.setdefault("preview", "")
        entry.setdefault("services", [])
        entry.setdefault("usable", bool(entry.get("services")))
        entry.setdefault("triggers", [])
        _cache.append(entry)
    _cache_at = time.monotonic()
    return _cache


def for_service(service_id: str, kind: Optional[str] = None) -> List[Dict[str, object]]:
    """What this service can actually load, plus what it cannot and why."""
    out = []
    for entry in entries():
        if kind and entry.get("kind") != kind:
            continue
        services = entry.get("services") or []
        # An unusable entry is carried along for its own service family so the
        # picker can grey it out with the reason instead of hiding the fact.
        if entry.get("usable") and service_id not in services:
            continue
        if not entry.get("usable") and kind != entry.get("kind"):
            continue
        out.append(entry)
    return out


def known_file(name: str, kind: Optional[str] = None) -> Optional[Dict[str, object]]:
    """The catalogue entry for a file name, or nothing if we do not offer it.

    Requests name a file, and a file name goes into a workflow that a worker
    then loads off disk. Only names the catalogue lists are accepted, so a
    caller cannot steer a worker at an arbitrary path.
    """
    wanted = str(name or "").strip()
    if not wanted:
        return None
    for entry in entries():
        if entry.get("file") == wanted and (kind is None or entry.get("kind") == kind):
            return entry
    return None


@router.get("/api/ai/model-catalogue")
async def api_model_catalogue(service: Optional[str] = None):
    """Checkpoints and LoRAs with their pictures, optionally for one service."""
    all_entries = entries()
    if service:
        checkpoints = [e for e in all_entries
                       if e.get("kind") == "checkpoint"
                       and (service in (e.get("services") or []) or not e.get("usable"))]
        loras = [e for e in all_entries
                 if e.get("kind") == "lora" and service in (e.get("services") or [])]
    else:
        checkpoints = [e for e in all_entries if e.get("kind") == "checkpoint"]
        loras = [e for e in all_entries if e.get("kind") == "lora"]
    return {
        "success_bool": True,
        "service_string": service or "",
        "checkpoints_array": checkpoints,
        "loras_array": loras,
        "server_time_unix_int": int(time.time()),
    }


@router.get("/api/ai/model-preview/{name}")
async def api_model_preview(name: str):
    """One thumbnail. Served from our own disk, never proxied from Civitai."""
    if not SAFE_PREVIEW.match(name):
        raise HTTPException(status_code=400, detail={
            "error_string": "bad_preview_name",
            "message_string": "A preview name is a plain .jpg file name"})
    path = PREVIEW_DIR / name
    if not path.is_file():
        raise HTTPException(status_code=404, detail={
            "error_string": "preview_not_found",
            "message_string": f"No preview called '{name}'"})
    # Previews never change under a given name, so they can be cached hard.
    return FileResponse(path, media_type="image/jpeg",
                        headers={"Cache-Control": "public, max-age=604800"})

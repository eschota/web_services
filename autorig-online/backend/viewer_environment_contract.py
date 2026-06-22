"""
Viewer environment contract sent from AutoRig Online to converter workers.

The task viewer owns theme selection and lighting values. Workers receive an
immutable snapshot so preview rendering remains reproducible even if theme JSON
changes later.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

from config import APP_URL


VIEWER_ENVIRONMENT_SCHEMA_VERSION = 1
VIEWER_THEME_ASSET_VERSION = "20260516-16x9"
VIEWER_THEME_ROOT_DIR = Path(__file__).resolve().parent.parent / "static" / "env" / "backdrops"
VIEWER_THEME_VIEWER_DIR = VIEWER_THEME_ROOT_DIR / "viewer"


def _slugify_viewer_theme(value: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "viewer_theme"


def _read_json_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if path.is_file():
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else None
    except Exception:
        return None
    return None


def _dict_or_empty(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list_or_empty(value: Any) -> list:
    return list(value) if isinstance(value, list) else []


def _theme_id_from_selection(selection: Any) -> Optional[str]:
    if not isinstance(selection, dict):
        return None
    raw = selection.get("theme_id") or selection.get("id")
    theme_id = _slugify_viewer_theme(str(raw or ""))
    return theme_id if theme_id else None


def _absolute_site_url(path: str) -> str:
    base = str(APP_URL or "https://autorig.online").rstrip("/")
    return f"{base}{path}"


def build_viewer_environment_snapshot(selection: Any) -> Optional[Dict[str, Any]]:
    theme_id = _theme_id_from_selection(selection)
    if not theme_id:
        return None

    json_path = VIEWER_THEME_ROOT_DIR / f"{theme_id}.json"
    theme = _read_json_file(json_path)
    if not isinstance(theme, dict):
        return None

    viewer_path = VIEWER_THEME_VIEWER_DIR / f"{theme_id}.jpg"
    if not viewer_path.is_file():
        return None

    relative_src = f"/static/env/backdrops/viewer/{quote(theme_id)}.jpg?v={VIEWER_THEME_ASSET_VERSION}"
    return {
        "schema_version": VIEWER_ENVIRONMENT_SCHEMA_VERSION,
        "theme_id": theme_id,
        "source": "task.viewer_settings.viewer_theme_selection",
        "selection": dict(selection) if isinstance(selection, dict) else None,
        "background": {
            "mode": "image",
            "url": _absolute_site_url(relative_src),
            "src": relative_src,
            "asset_version": VIEWER_THEME_ASSET_VERSION,
        },
        "environment_settings": _dict_or_empty(theme.get("environment_settings")),
        "shadow_settings": _dict_or_empty(theme.get("shadow_settings")),
        "sun_settings": _dict_or_empty(theme.get("sun_settings")),
        "plane_color": str(theme.get("plane_color") or "#222222"),
        "camera_transform": _dict_or_empty(theme.get("camera_transform")),
        "semantic_tags": _list_or_empty(theme.get("semantic_tags")),
    }


def ensure_viewer_environment_in_settings(settings: Dict[str, Any]) -> bool:
    if not isinstance(settings, dict):
        return False
    selection = settings.get("viewer_theme_selection")
    theme_id = _theme_id_from_selection(selection)
    if not theme_id:
        return False

    existing = settings.get("viewer_environment")
    if (
        isinstance(existing, dict)
        and _slugify_viewer_theme(str(existing.get("theme_id") or "")) == theme_id
        and isinstance(existing.get("background"), dict)
        and existing["background"].get("url")
    ):
        return False

    snapshot = build_viewer_environment_snapshot(selection)
    if not snapshot:
        return False
    settings["viewer_environment"] = snapshot
    return True


def get_viewer_environment_from_settings(settings: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(settings, dict):
        return None
    env = settings.get("viewer_environment")
    if isinstance(env, dict) and env:
        return env
    snapshot = build_viewer_environment_snapshot(settings.get("viewer_theme_selection"))
    return snapshot if isinstance(snapshot, dict) and snapshot else None

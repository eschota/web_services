"""Viewer theme environment helpers for converter preview rendering."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote


DEFAULT_APP_URL = "https://autorig.online"
VIEWER_THEME_ASSET_VERSION = "20260516-16x9"
VIEWER_THEME_ROOT_DIR = Path(__file__).resolve().parent.parent / "static" / "env" / "backdrops"


def _slugify_viewer_theme(value: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "viewer_theme"


def _json_copy(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def _read_json_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _absolute_public_url(url: str, *, app_url: str = DEFAULT_APP_URL) -> str:
    raw = str(url or "").strip()
    if raw.startswith(("http://", "https://")):
        return raw
    base = (app_url or DEFAULT_APP_URL).rstrip("/")
    if raw.startswith("/"):
        return f"{base}{raw}"
    return f"{base}/{raw.lstrip('/')}"


def resolve_viewer_theme_item(
    theme_id: str,
    *,
    theme_root: Path = VIEWER_THEME_ROOT_DIR,
) -> Optional[Dict[str, Any]]:
    """Resolve a saved theme id to the same public item shape as /api/viewer-themes."""
    tid = _slugify_viewer_theme(theme_id)
    if not tid:
        return None

    json_path = theme_root / f"{tid}.json"
    if not json_path.is_file():
        return None

    item = _read_json_file(json_path)
    if not item:
        return None

    resolved_id = _slugify_viewer_theme(str(item.get("theme_id") or tid))
    viewer_path = theme_root / "viewer" / f"{resolved_id}.jpg"
    thumb_path = theme_root / "thumbs" / f"{resolved_id}.jpg"
    if not viewer_path.is_file() or not thumb_path.is_file():
        return None

    merged = dict(item)
    merged["theme_id"] = resolved_id
    merged["id"] = resolved_id
    image_filename = str(merged.get("image_filename") or f"{resolved_id}.jpg")
    merged["image_filename"] = image_filename
    merged["source_src"] = f"/static/env/backdrops/source/{quote(image_filename)}"
    merged["src"] = f"/static/env/backdrops/viewer/{quote(resolved_id)}.jpg?v={VIEWER_THEME_ASSET_VERSION}"
    merged["thumb_src"] = f"/static/env/backdrops/thumbs/{quote(resolved_id)}.jpg?v={VIEWER_THEME_ASSET_VERSION}"
    return merged


def build_viewer_environment_from_selection(
    selection: Optional[Dict[str, Any]],
    *,
    app_url: str = DEFAULT_APP_URL,
) -> Optional[Dict[str, Any]]:
    """Build the converter worker viewer_environment snapshot for a saved selection."""
    if not isinstance(selection, dict):
        return None
    theme_id = str(selection.get("theme_id") or selection.get("id") or "").strip()
    theme = resolve_viewer_theme_item(theme_id)
    if not theme:
        return None

    env: Dict[str, Any] = {
        "schema_version": int(theme.get("schema_version") or 1),
        "theme_id": str(theme.get("theme_id") or theme_id),
        "theme_name": str(theme.get("theme_name") or theme.get("theme_id") or theme_id),
        "background": {
            "url": _absolute_public_url(str(theme.get("src") or ""), app_url=app_url),
        },
        "background_fit": "cover",
    }

    for key in (
        "theme_short_description",
        "semantic_tags",
        "plane_color",
        "camera_transform",
        "environment_settings",
        "sun_settings",
        "shadow_settings",
    ):
        if key in theme:
            env[key] = _json_copy(theme[key])

    return env


def build_viewer_environment_from_settings(
    settings: Any,
    *,
    app_url: str = DEFAULT_APP_URL,
) -> Optional[Dict[str, Any]]:
    """Extract viewer_theme_selection from task.viewer_settings and build worker env."""
    data = settings
    if isinstance(settings, str):
        try:
            data = json.loads(settings or "{}")
        except Exception:
            return None
    if not isinstance(data, dict):
        return None
    selection = data.get("viewer_theme_selection")
    return build_viewer_environment_from_selection(selection, app_url=app_url)

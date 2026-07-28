from __future__ import annotations

import re
from typing import Any, Iterable, Optional, Tuple
from urllib.parse import unquote, urlsplit, urlunsplit


_LEGACY_FARM_ARTIFACT_HOST = re.compile(
    r"^converter-(f[1-9][0-9]*)\.freestock\.online$",
    re.IGNORECASE,
)
_LEGACY_ARTIFACT_PREFIX = "/converter/glb/"
_VIEWER_PREPARED_SUFFIX = "_model_prepared_viewer.glb"
_VIEWER_ANIMATIONS_SUFFIX = "_all_animations_viewer.glb"


def canonical_worker_artifact_url(url: str) -> str:
    """Route legacy farm artifact URLs through the dedicated files host."""
    value = str(url or "").strip()
    if not value:
        return value

    try:
        parsed = urlsplit(value)
    except ValueError:
        return value

    match = _LEGACY_FARM_ARTIFACT_HOST.fullmatch(parsed.hostname or "")
    if not match or not parsed.path.startswith(_LEGACY_ARTIFACT_PREFIX):
        return value

    relative_path = parsed.path[len(_LEGACY_ARTIFACT_PREFIX) :]
    if not relative_path:
        return value

    files_host = f"{match.group(1).lower()}.freestock.online"
    return urlunsplit(("https", files_host, f"/{relative_path}", parsed.query, parsed.fragment))


def viewer_artifact_kind(url: str) -> Optional[str]:
    """Return the private viewer artifact kind encoded by an artifact URL."""
    value = str(url or "").strip()
    if not value:
        return None
    try:
        basename = unquote(urlsplit(value).path).rsplit("/", 1)[-1].lower()
    except ValueError:
        return None
    if basename.endswith(_VIEWER_PREPARED_SUFFIX):
        return "prepared"
    if basename.endswith(_VIEWER_ANIMATIONS_SUFFIX):
        return "animations"
    return None


def is_viewer_artifact_url(url: str) -> bool:
    """True for preview-only GLBs which must not enter download inventories."""
    return viewer_artifact_kind(url) is not None


def parse_worker_artifact_payload(
    data: Any,
) -> Tuple[list[str], Optional[str], Optional[str]]:
    """
    Split a worker response into downloadable outputs and private viewer GLBs.

    New workers declare the URLs in dedicated fields. During a rolling farm
    update we also recognize viewer filenames inside ``output_urls`` while
    removing them from that public/downloadable list.
    """
    payload = data if isinstance(data, dict) else {}
    raw_outputs: Iterable[Any] = payload.get("output_urls") or []
    if isinstance(raw_outputs, (str, bytes)) or not isinstance(raw_outputs, Iterable):
        raw_outputs = []

    viewer_prepared = str(
        payload.get("viewer_prepared_glb_url")
        or payload.get("viewerPreparedGlbUrl")
        or ""
    ).strip() or None
    viewer_animations = str(
        payload.get("viewer_animations_glb_url")
        or payload.get("viewerAnimationsGlbUrl")
        or ""
    ).strip() or None

    outputs: list[str] = []
    seen: set[str] = set()
    for raw_url in raw_outputs:
        url = str(raw_url or "").strip()
        if not url:
            continue
        kind = viewer_artifact_kind(url)
        if kind == "prepared":
            viewer_prepared = viewer_prepared or url
            continue
        if kind == "animations":
            viewer_animations = viewer_animations or url
            continue
        if url not in seen:
            seen.add(url)
            outputs.append(url)

    return (
        outputs,
        canonical_worker_artifact_url(viewer_prepared) if viewer_prepared else None,
        canonical_worker_artifact_url(viewer_animations) if viewer_animations else None,
    )

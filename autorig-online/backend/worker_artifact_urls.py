from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit


_LEGACY_FARM_ARTIFACT_HOST = re.compile(
    r"^converter-(f[1-9][0-9]*)\.freestock\.online$",
    re.IGNORECASE,
)
_LEGACY_ARTIFACT_PREFIX = "/converter/glb/"


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

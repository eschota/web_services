"""Live, always-fresh task page (Task page · V3 agent, 2026-10-10).

The owner's First Commandment: every task page load runs the code an agent has
just written in production.  This module makes ``/task`` independent of the
release the backend process happened to start from:

* templates and layout partials are read per request from the live static
  roots, in the same order nginx uses for ``/static/``: the atomic overlay
  ``/srv/autorig/live/static`` first, then the current release;
* every ``/static`` JS/CSS reference in the page (``src``/``href`` attributes and
  quoted module specifiers in inline scripts) is stamped per request with
  ``?v=<first 10 hex of the file's SHA-1>``, the digest
  ``/srv/autorig/version_assets.py`` uses at release time.  Digests are cached by
  (device, inode, size, mtime), so a request costs one ``stat`` per asset;
* the classic page or the V3 shell is chosen per request from the live rollout
  file and the switch (``?v3=1`` / ``?v3=0``, cookie ``ar_task_v3``).

A new template, partial, script, stylesheet or rollout mode never needs a
backend restart.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Optional

log = logging.getLogger("task_page_live")

LIVE_OVERLAY = Path(os.getenv("AUTORIG_LIVE_STATIC_OVERLAY", "/srv/autorig/live/static"))
CURRENT_STATIC = Path(os.getenv("AUTORIG_CURRENT_STATIC", "/srv/autorig/current/autorig-online/static"))
BUNDLED_STATIC = Path(__file__).resolve().parent.parent / "static"
ROLLOUT_FILE = Path(os.getenv("AUTORIG_TASK_PAGE_ROLLOUT", "/srv/autorig/live/config/task-page.json"))

CLASSIC_TEMPLATE = "task.html"
V3_TEMPLATE = "task-v3.html"
SWITCH_COOKIE = "ar_task_v3"
PREVIEW_COOKIE = "ar_task_v3_preview"
MODES = ("off", "admin", "new", "all")

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
# A quoted /static JS or CSS path, optionally with a lone ?v=...: attributes and
# module specifiers alike.  Anything with more query parameters is left alone.
_ASSET_REF = re.compile(
    r"""(?P<q>["'])(?P<path>/static/(?P<rel>[A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:m?js|css)))"""
    r"""(?:\?v=[A-Za-z0-9._-]*)?(?P=q)"""
)
_MAX_HASHED_BYTES = 32 * 1024 * 1024
_DIGESTS: dict[str, tuple[tuple[int, int, int, int], str]] = {}


# --------------------------------------------------------------------------- static roots

def static_roots() -> list[Path]:
    """The overlay, then the current release, then the tree this process runs from."""
    roots: list[Path] = []
    for root in (LIVE_OVERLAY, CURRENT_STATIC, BUNDLED_STATIC):
        try:
            if root.is_dir() and root not in roots:
                roots.append(root)
        except OSError:
            continue
    return roots


def _clean_rel(rel: str) -> Optional[tuple[str, ...]]:
    parts = PurePosixPath(str(rel or "").lstrip("/")).parts
    if not parts or any(part in ("", ".", "..") or "\\" in part for part in parts):
        return None
    return parts


def resolve_static(rel: str) -> Optional[Path]:
    parts = _clean_rel(rel)
    if parts is None:
        return None
    for root in static_roots():
        candidate = root.joinpath(*parts)
        try:
            if candidate.is_file():
                return candidate
        except OSError:
            continue
    return None


def read_static_text(rel: str) -> str:
    path = resolve_static(rel)
    if path is None:
        raise FileNotFoundError(rel)
    return path.read_text(encoding="utf-8")


def static_digest(rel: str) -> Optional[str]:
    """First 10 hex of the SHA-1 of the file nginx serves for /static/<rel>."""
    path = resolve_static(rel)
    if path is None:
        return None
    try:
        st = path.stat()
    except OSError:
        return None
    key = (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
    cached = _DIGESTS.get(str(path))
    if cached and cached[0] == key:
        return cached[1]
    if st.st_size > _MAX_HASHED_BYTES:
        return None
    digest = hashlib.sha1()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        after = path.stat()
    except OSError:
        return None
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != key:
        return None  # replaced while hashing: leave the reference as it is this time
    value = digest.hexdigest()[:10]
    if len(_DIGESTS) > 4096:
        _DIGESTS.clear()
    _DIGESTS[str(path)] = (key, value)
    return value


def stamp_static_assets(html: str) -> str:
    """Point every /static JS/CSS reference at its current content."""
    def swap(match: "re.Match[str]") -> str:
        digest = static_digest(match.group("rel"))
        if not digest:
            return match.group(0)
        quote = match.group("q")
        return f"{quote}{match.group('path')}?v={digest}{quote}"

    return _ASSET_REF.sub(swap, html)


# --------------------------------------------------------------------------- layout

def _partial(name: str) -> str:
    try:
        return read_static_text(f"partials/{name}")
    except (OSError, UnicodeDecodeError):
        return ""


def inject_layout(html: str) -> str:
    """Server-rendered header/footer (SEO-critical links) from the live partials.

    Same placeholders and markup as main._inject_static_layout, which reads the
    partials of the release the process started from.
    """
    body = re.search(r"<body\b[^>]*>", html, flags=re.IGNORECASE)
    body_tag = body.group(0) if body else ""
    show_search = any(flag in body_tag for flag in (
        'data-layout-free3d-ribbon="1"', "data-layout-free3d-ribbon='1'",
        'data-layout-free3d-ribbon="true"', "data-layout-free3d-ribbon='true'",
    ))
    if '<div id="site-header"></div>' in html:
        header = _partial("site-header.html")
        if show_search:
            header = f"{header}\n{_partial('site-free3d-search.html')}"
        html = html.replace(
            '<div id="site-header"></div>',
            f'<div id="site-header" data-server-rendered="1">\n{header}\n</div>', 1)
    if '<div id="site-footer"></div>' in html:
        html = html.replace(
            '<div id="site-footer"></div>',
            f'<div id="site-footer" data-server-rendered="1">\n{_partial("site-footer.html")}\n</div>', 1)
    return html


_META_DESCRIPTION = re.compile(r'<meta name="description" content="([^"]*)">')


def fill_v3_description(html: str) -> str:
    """The V3 shell repeats the (already escaped) meta description as page text."""
    if "<!-- TASK_V3_DESCRIPTION -->" not in html:
        return html
    found = _META_DESCRIPTION.search(html)
    return html.replace("<!-- TASK_V3_DESCRIPTION -->", found.group(1) if found else "", 1)


def render_task_html(html: str) -> str:
    return stamp_static_assets(fill_v3_description(inject_layout(html)))


# --------------------------------------------------------------------------- rollout

_ROLLOUT_CACHE: dict[str, Any] = {"key": None, "value": None}


def _parse_time(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def rollout() -> dict[str, Any]:
    """The live rollout document; a missing or broken file means admin-only."""
    default = {"mode": "admin", "new_since": None, "preview_keys": (), "webapp": False}
    try:
        st = ROLLOUT_FILE.stat()
    except OSError:
        return default
    key = (st.st_ino, st.st_size, st.st_mtime_ns)
    if _ROLLOUT_CACHE["key"] == key and _ROLLOUT_CACHE["value"] is not None:
        return _ROLLOUT_CACHE["value"]
    try:
        data = json.loads(ROLLOUT_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("rollout document is not an object")
    except (OSError, ValueError) as exc:
        log.warning("task page rollout unreadable, admin-only: %s", exc)
        return default
    mode = str(data.get("mode") or "admin").strip().lower()
    keys = data.get("preview_keys") or ()
    value = {
        "mode": mode if mode in MODES else "admin",
        "new_since": _parse_time(data.get("new_since")),
        "preview_keys": tuple(str(k) for k in keys if isinstance(k, str) and len(k) >= 16),
        "webapp": bool(data.get("webapp")),
    }
    _ROLLOUT_CACHE.update(key=key, value=value)
    return value


def switch_preference(query: Any, cookies: Any) -> tuple[Optional[bool], Optional[str]]:
    """(preference, cookie to store) from ?v3= and the ar_task_v3 cookie."""
    raw = query.get("v3") if query is not None else None
    if raw is not None:
        flag = str(raw).strip().lower()
        if flag in ("1", "true", "yes", "on"):
            return True, "1"
        if flag in ("0", "false", "no", "off"):
            return False, "0"
    stored = cookies.get(SWITCH_COOKIE) if cookies is not None else None
    if stored == "1":
        return True, None
    if stored == "0":
        return False, None
    return None, None


def _preview_ok(cookies: Any, keys: Iterable[str]) -> bool:
    token = str((cookies.get(PREVIEW_COOKIE) if cookies is not None else "") or "")
    return bool(token) and any(hmac.compare_digest(token, key) for key in keys)


def choose_v3(*, mode: str, preference: Optional[bool], is_admin: bool, preview: bool,
              created_at: Optional[datetime], new_since: Optional[datetime],
              webapp: bool, webapp_allowed: bool, v3_task: bool = False) -> bool:
    """Whether this request gets the V3 shell.

    off   -> never;
    admin -> only an admin (or a preview cookie) who opted in;
    new   -> tasks created at/after new_since, plus anyone who opted in;
    all   -> every task.
    A task of the V3 conveyor (pipeline_kind v3) always opens in the V3 shell:
    the classic page has no viewer for it.  ?v3=0 / cookie 0 always wins.
    Telegram WebApp keeps the classic page until the rollout file sets
    "webapp": true, except for opted-in admins and V3 tasks.
    """
    if mode == "off" or preference is False:
        return False
    if v3_task:
        return True
    privileged = is_admin or preview
    if preference is True and privileged:
        return True
    if mode == "admin":
        return False
    if webapp and not webapp_allowed:
        return False
    if preference is True or mode == "all":
        return True
    if mode == "new" and created_at is not None and new_since is not None:
        created = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
        return created >= new_since
    return False


async def _task_facts(db: Any, task_model: Any, task_id: str) -> tuple[bool, Optional[datetime], bool]:
    """(exists, created_at, is a V3 conveyor task)."""
    from sqlalchemy import select

    columns = [task_model.id, task_model.created_at]
    kind = getattr(task_model, "pipeline_kind", None)
    if kind is not None:
        columns.append(kind)
    row = (await db.execute(select(*columns).where(task_model.id == task_id))).first()
    if row is None:
        return False, None, False
    v3_task = len(row) > 2 and str(row[2] or "").strip().lower() == "v3"
    return True, row[1], v3_task


async def task_template(request: Any, task_id: Optional[str], user: Any, db: Any, *,
                        is_admin_email: Callable[[Optional[str]], bool], task_model: Any = None) -> str:
    """Pick and read the /task template for this request (never raises for V3).

    main.task_page imports ``Task`` locally further down, so the model is not
    passed from there: it is imported here.
    """
    if task_model is None:
        from database import Task as task_model  # noqa: N813
    preference, cookie = switch_preference(request.query_params, request.cookies)
    state = getattr(request, "state", None)
    if state is not None and cookie is not None:
        state.task_page_switch_cookie = cookie
    name = CLASSIC_TEMPLATE
    try:
        candidate = str(task_id or "").strip().lower()
        classic_only = str(request.query_params.get("classic") or "") == "1"
        config = rollout()
        if not classic_only and config["mode"] != "off" and _UUID.fullmatch(candidate) \
                and resolve_static(V3_TEMPLATE) is not None:
            is_admin = bool(user is not None and is_admin_email(getattr(user, "email", None)))
            preview = _preview_ok(request.cookies, config["preview_keys"])
            exists, created_at, v3_task = await _task_facts(db, task_model, candidate)
            if exists and choose_v3(
                mode=config["mode"], preference=preference, is_admin=is_admin, preview=preview,
                created_at=created_at, new_since=config["new_since"],
                webapp=str(request.query_params.get("mode") or "") == "webapp",
                webapp_allowed=config["webapp"], v3_task=v3_task,
            ):
                name = V3_TEMPLATE
    except Exception as exc:  # the classic page must always render
        log.warning("task page V3 selection failed, classic page: %s", exc)
        name = CLASSIC_TEMPLATE
    if state is not None:
        state.task_page_template = name
    try:
        return read_static_text(name)
    except (OSError, UnicodeDecodeError):
        if name != CLASSIC_TEMPLATE:
            if state is not None:
                state.task_page_template = CLASSIC_TEMPLATE
            return read_static_text(CLASSIC_TEMPLATE)
        raise


def apply_switch_cookie(response: Any, request: Any) -> Any:
    if request is None:
        return response
    state = getattr(request, "state", None)
    value = getattr(state, "task_page_switch_cookie", None) if state is not None else None
    if value in ("0", "1"):
        response.set_cookie(SWITCH_COOKIE, value, max_age=365 * 86400, path="/",
                            secure=True, httponly=True, samesite="lax")
    template = getattr(state, "task_page_template", None) if state is not None else None
    if template:
        response.headers["X-AutoRig-Task-Page"] = "v3" if template == V3_TEMPLATE else "classic"
    return response


def describe() -> dict[str, Any]:
    """Non-sensitive live state for agents: mode and the current asset stamps."""
    config = rollout()
    assets = {}
    for rel in ("task-v3.html", "js/task-v3-shell.js", "css/task-v3.css", "task.html",
                "js/header.js", "js/site-layout.js", "css/styles.css"):
        path = resolve_static(rel)
        assets[rel] = {
            "v": static_digest(rel) if not rel.endswith(".html") else None,
            "source": None if path is None else ("overlay" if LIVE_OVERLAY in path.parents else "release"),
        }
    return {
        "schema": "autorig.task-page-live/1",
        "mode": config["mode"],
        "new_since": config["new_since"].isoformat() if config["new_since"] else None,
        "webapp": config["webapp"],
        "assets": assets,
    }


def is_task_uuid(value: Any) -> bool:
    try:
        return str(uuid.UUID(str(value))) == str(value)
    except (TypeError, ValueError, AttributeError):
        return False

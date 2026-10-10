"""Site modes by host: the SFW main site and the separate, gated adult domain.

Owner order, 2026-10-11:

    весь NSFW контент по всем регуляционным правилам стран перенести на
    отдельный защищенный домен ... сделай нам входной фильтр разделитель между
    доменами, а сайт подготовь к работе в разных режимах и перенеси /nodes
    полностью в /nsfw ... только с согласия и подтверждения возраста и
    авторизации через гугл oAuth

One backend serves both sites. The mode is chosen per request:

* ``main``  autorig.online. Adult tasks are never listed, linked or embedded
  (``main_list_filter``); with ``main_split_items`` a request for one gets a
  neutral page; with ``main_split_section`` the node editor family (/nodes,
  /workflows, /queue, /lora, /system_prompts and their APIs) answers with the
  neutral page / ``nsfw_domain_only`` instead.
* ``nsfw``  a host in ``nsfw_hosts``. Every request passes the gate before a
  byte is served: a Google sign-in session, a recorded 18+ and terms consent of
  the current version, and a country / US state outside the geo block list.
  nginx asks ``/api/age-gate/check`` (auth_request) before it serves a media
  file on that host.
* staging. Before the domain exists an admin on the main host can set the
  cookie ``autorig_site_mode`` (``/api/site-mode/stage?mode=nsfw`` or
  ``main-split``) and get either mode. The cookie does nothing for anyone else.

Requests from this machine to 127.0.0.1:8200 (autorig-mt, surabot, the film
director) are internal and never gated, so the agent and tool flows keep
working. Admin API keys pass the gate too (machine identity, no human to ask).

Config: ``/srv/autorig/live/config/site-modes.json`` (env AUTORIG_SITE_MODES_FILE),
re-read when it changes. Missing keys take DEFAULTS.

The geo list is a set of measures, not a claim of legal compliance; see the
``GEO_NOTES`` below and the report of 2026-10-11 for the open questions.
"""
from __future__ import annotations

import html as _html
import json
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urlsplit

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response

import geo_country

CONFIG_PATH_DEFAULT = "/srv/autorig/live/config/site-modes.json"
STAGE_COOKIE = "autorig_site_mode"
STAGE_MODES = ("nsfw", "main-split")
SESSION_COOKIE = "session"          # main.SESSION_COOKIE (Google OAuth session)
GATE_PATH = "/age-gate"
TERMS_PATH = "/adult-terms"
RTA_LABEL = "RTA-5042-1996-1400-1577-RTA"

# Jurisdictions where the law asks for more than a self-declared age before
# adult content is shown (or forbids it), which this site does not offer.
# A conservative default the owner can edit in the config file. Sources are in
# the 2026-10-11 report; this list is a measure, not legal advice.
GEO_NOTES = {
    "GB": "Online Safety Act 2023 Part 5 s.81: highly effective age assurance (Ofcom, since 2025-07-25)",
    "FR": "Loi SREN 2024 + Arcom referentiel: double-anonymity age verification (since 2025)",
    "IT": "Decreto Caivano + AGCOM 96/25/CONS: age verification (since 2025-11-12)",
    "DE": "JMStV: closed user group with age verification for pornography",
    "ES": "Ley 13/2022 art. 89: age verification on video-sharing platforms",
    "AU": "eSafety Phase 2 codes: age assurance for online pornography (since 2026-03-09)",
    "BR": "ECA Digital, Lei 15.211/2025: age verification beyond self-declaration (since 2026-03-17)",
    "RU": "UK RF art. 242: distribution of pornographic material is a crime",
    "CN": "pornography is illegal", "IR": "pornography is illegal", "SA": "pornography is illegal",
    "AE": "pornography is illegal", "QA": "pornography is illegal", "KW": "pornography is illegal",
    "OM": "pornography is illegal", "BH": "pornography is illegal", "PK": "pornography is illegal",
    "BD": "pornography is illegal", "IN": "IT Act s.67A / blocking orders", "ID": "pornography is illegal",
    "MY": "pornography is illegal", "TR": "blocked by law", "EG": "blocked by court order",
    "KP": "illegal", "SY": "illegal", "YE": "illegal", "AF": "illegal", "SD": "illegal",
    "BY": "illegal", "UZ": "illegal", "TM": "illegal", "VN": "illegal", "KZ": "illegal",
    "US": "state age-verification laws, see blocked_us_states",
}
# US states with an adult-site age-verification law in effect by 2026-10
# (after Free Speech Coalition v. Paxton, 2025-06-27).
US_AV_STATES = [
    "AL", "AR", "AZ", "FL", "GA", "IA", "ID", "IN", "KS", "KY", "LA", "MO", "MS", "MT", "NC", "ND",
    "NE", "OH", "OK", "SC", "SD", "TN", "TX", "UT", "VA", "WV", "WY",
]

DEFAULTS: Dict[str, Any] = {
    "main_hosts": ["autorig.online", "www.autorig.online"],
    "nsfw_hosts": [],                 # e.g. ["autorig.red"] once the domain is registered
    "nsfw_canonical_host": "",        # shown on the neutral page and used for links
    "main_list_filter": True,         # never list adult tasks on the main host
    "main_split_items": False,        # neutral page for an adult task on the main host
    "main_split_section": False,      # the node editor family leaves the main host
    "nsfw_ratings": ["adult"],
    "section_pages": ["/nodes", "/workflows", "/queue", "/lora", "/system_prompts"],
    "section_api": ["/api/ai/graphs", "/api/ai/graph/", "/api/ai/graph-archive", "/api/ai/civitai/",
                    "/api/ai/civitai-search", "/api/ai/queue/", "/api/ai/loras", "/api/ai/prompts"],
    "section_api_exempt": ["/api/ai/loras/sync/", "/api/ai/queue/clear"],
    "consent_version": "2026-10-11",
    "nsfw_noindex": True,
    "admin_geo_exempt": True,
    "geo": {
        "unknown_country": "block",   # "block" (fail closed) or "allow"
        "blocked_countries": sorted(k for k in GEO_NOTES if k != "US"),
        "blocked_us_states": US_AV_STATES,
        "us_unknown_state": "block",
    },
    "oauth_redirect_uri": {},         # {"autorig.red": "https://autorig.red/auth/callback"}
}

# Paths that answer before the gate on an adult host (the gate itself, sign-in,
# language, the terms). Nothing here carries adult content.
GATE_OPEN_PREFIXES = (
    GATE_PATH, TERMS_PATH, "/api/age-gate/", "/api/site-mode", "/auth/", "/api/language",
    "/api/me/language", "/favicon", "/robots.txt", "/static/css/", "/static/i18n/", "/static/fonts/",
)

_TASK_ITEM_RE = re.compile(
    r"^/(?:api/task|thumb|api/thumb|api/video|api/task-viewer|api/v3/task)/([0-9a-fA-F][0-9a-fA-F-]{7,63})(?:/|$)")


# ------------------------------------------------------------------ config
_CFG_LOCK = threading.Lock()
_CFG: Dict[str, Any] = {"mtime": None, "path": None, "data": None, "checked": 0.0}


def config_path() -> str:
    return os.environ.get("AUTORIG_SITE_MODES_FILE", "").strip() or CONFIG_PATH_DEFAULT


def _merge(base: Dict[str, Any], over: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in (over or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = value
    return out


def config() -> Dict[str, Any]:
    now = time.monotonic()
    path = config_path()
    with _CFG_LOCK:
        if _CFG["data"] is not None and _CFG["path"] == path and now - _CFG["checked"] < 5:
            return _CFG["data"]
        _CFG["checked"] = now
        try:
            mtime = os.stat(path).st_mtime
        except OSError:
            mtime = None
        if _CFG["data"] is not None and _CFG["path"] == path and _CFG["mtime"] == mtime:
            return _CFG["data"]
        over: Dict[str, Any] = {}
        if mtime is not None:
            try:
                loaded = json.loads(Path(path).read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    over = loaded
            except (OSError, ValueError) as exc:
                print(f"[site-mode] config {path} unreadable, using defaults: {exc}")
        data = _merge(DEFAULTS, over)
        for key in ("main_hosts", "nsfw_hosts"):
            data[key] = [str(h).strip().lower() for h in data.get(key) or [] if str(h).strip()]
        data["nsfw_ratings"] = [str(r).strip().lower() for r in data.get("nsfw_ratings") or []]
        _CFG.update(mtime=mtime, path=path, data=data)
        return data


def nsfw_host(cfg: Optional[Dict[str, Any]] = None) -> str:
    cfg = cfg or config()
    host = str(cfg.get("nsfw_canonical_host") or "").strip().lower()
    if host:
        return host
    hosts = cfg.get("nsfw_hosts") or []
    return hosts[0] if hosts else ""


# ------------------------------------------------------------------ request facts
def request_host(headers: Dict[str, str]) -> str:
    host = (headers.get("host") or "").strip().lower()
    if host.startswith("["):
        return host.split("]", 1)[0] + "]"
    return host.rsplit(":", 1)[0] if ":" in host else host


def is_internal(scope: Dict[str, Any], headers: Dict[str, str]) -> bool:
    """A call from this machine straight to uvicorn, not through nginx.

    nginx always sends the public Host plus X-Real-IP / X-Forwarded-For; the
    local services call http://127.0.0.1:8200 with neither."""
    client = scope.get("client") or ("", 0)
    if str(client[0]) not in ("127.0.0.1", "::1"):
        return False
    if headers.get("x-forwarded-for") or headers.get("x-real-ip"):
        return False
    return request_host(headers) in ("127.0.0.1", "localhost", "::1", "[::1]", "")


def client_ip(scope: Dict[str, Any], headers: Dict[str, str]) -> str:
    client = scope.get("client") or ("", 0)
    peer = str(client[0])
    if peer in ("127.0.0.1", "::1"):
        real = (headers.get("x-real-ip") or "").strip()
        if real:
            return real
        xff = (headers.get("x-forwarded-for") or "").split(",")[0].strip()
        if xff:
            return xff
    return peer


def _cookie(headers: Dict[str, str], name: str) -> str:
    for part in str(headers.get("cookie") or "").split(";"):
        k, _, v = part.strip().partition("=")
        if k == name:
            return v.strip().strip('"')
    return ""


def _header_map(scope: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in scope.get("headers") or []:
        try:
            out[k.decode("latin-1").lower()] = v.decode("latin-1")
        except Exception:
            continue
    return out


# ------------------------------------------------------------------ identity
_ID_CACHE: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
_ID_TTL = 30.0
_API_KEY_USER: Optional[Callable[[Request, Any], Awaitable[Any]]] = None
_IS_ADMIN: Callable[[Optional[str]], bool] = lambda _email: False


async def session_identity(token: str) -> Optional[Dict[str, Any]]:
    """{user_id, is_admin} for a Google OAuth session token (cached briefly)."""
    if not token:
        return None
    now = time.monotonic()
    hit = _ID_CACHE.get(token)
    if hit and now - hit[0] < _ID_TTL:
        return hit[1]
    from auth import get_user_by_session
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        user = await get_user_by_session(db, token)
    ident = None
    if user is not None:
        ident = {"user_id": int(user.id), "is_admin": bool(_IS_ADMIN(user.email)), "via": "session"}
    if len(_ID_CACHE) > 5000:
        _ID_CACHE.clear()
    _ID_CACHE[token] = (now, ident)
    return ident


async def api_key_identity(scope: Dict[str, Any], headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    if _API_KEY_USER is None:
        return None
    if not (headers.get("authorization") or headers.get("x-api-key")):
        return None
    from database import AsyncSessionLocal

    request = Request(scope)
    async with AsyncSessionLocal() as db:
        user = await _API_KEY_USER(request, db)
    if user is None:
        return None
    return {"user_id": int(user.id), "is_admin": bool(_IS_ADMIN(user.email)), "via": "api_key"}


async def identity(scope: Dict[str, Any], headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    ident = await session_identity(_cookie(headers, SESSION_COOKIE))
    if ident is None:
        ident = await api_key_identity(scope, headers)
    return ident


def forget_identity_cache() -> None:
    _ID_CACHE.clear()
    _CONSENT_CACHE.clear()


# ------------------------------------------------------------------ mode
async def resolve_mode(scope: Dict[str, Any], headers: Dict[str, str],
                       cfg: Optional[Dict[str, Any]] = None) -> Tuple[str, bool]:
    """(mode, staged): mode is "main", "nsfw" or "main-split"."""
    cfg = cfg or config()
    host = request_host(headers)
    if host and host in cfg["nsfw_hosts"]:
        return "nsfw", False
    staged = _cookie(headers, STAGE_COOKIE)
    if staged in STAGE_MODES:
        ident = await session_identity(_cookie(headers, SESSION_COOKIE))
        if ident and ident.get("is_admin"):
            return staged, True
    return "main", False


def mode_of(request: Request) -> str:
    return str(getattr(request.state, "site_mode", "") or "main")


# ------------------------------------------------------------------ consent store
_CONSENT_CACHE: Dict[int, Tuple[float, Optional[Dict[str, Any]]]] = {}
_CONSENT_TTL = 30.0
_TABLE_READY = False

_CREATE_SQL = (
    "CREATE TABLE IF NOT EXISTS adult_consents ("
    " id INTEGER PRIMARY KEY AUTOINCREMENT,"
    " user_id INTEGER NOT NULL,"
    " consent_version VARCHAR(32) NOT NULL,"
    " age_confirmed BOOLEAN NOT NULL,"
    " terms_accepted BOOLEAN NOT NULL,"
    " ip_country VARCHAR(8),"
    " site_host VARCHAR(255),"
    " created_at DATETIME NOT NULL,"
    " revoked_at DATETIME)"
)
_INDEX_SQL = "CREATE INDEX IF NOT EXISTS ix_adult_consents_user ON adult_consents (user_id)"


async def _ensure_table(db) -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    from sqlalchemy import text

    await db.execute(text(_CREATE_SQL))
    await db.execute(text(_INDEX_SQL))
    await db.commit()
    _TABLE_READY = True


async def current_consent(user_id: int, version: str) -> Optional[Dict[str, Any]]:
    now = time.monotonic()
    hit = _CONSENT_CACHE.get(user_id)
    if hit and now - hit[0] < _CONSENT_TTL and (hit[1] is None or hit[1]["consent_version"] == version):
        return hit[1]
    from sqlalchemy import text
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        await _ensure_table(db)
        row = (await db.execute(text(
            "SELECT id, consent_version, ip_country, created_at FROM adult_consents"
            " WHERE user_id = :u AND consent_version = :v AND revoked_at IS NULL"
            " AND age_confirmed AND terms_accepted ORDER BY id DESC LIMIT 1"),
            {"u": int(user_id), "v": version})).first()
    rec = None
    if row is not None:
        rec = {"id": int(row[0]), "consent_version": str(row[1]), "ip_country": row[2],
               "created_at": str(row[3])}
    _CONSENT_CACHE[user_id] = (now, rec)
    return rec


async def record_consent(user_id: int, version: str, country: Optional[str], host: str) -> Dict[str, Any]:
    from sqlalchemy import text
    from database import AsyncSessionLocal

    created = datetime.utcnow().replace(microsecond=0)
    async with AsyncSessionLocal() as db:
        await _ensure_table(db)
        await db.execute(text(
            "INSERT INTO adult_consents (user_id, consent_version, age_confirmed, terms_accepted,"
            " ip_country, site_host, created_at) VALUES (:u, :v, 1, 1, :c, :h, :t)"),
            {"u": int(user_id), "v": version, "c": (country or "ZZ")[:8], "h": host[:255], "t": created})
        await db.commit()
    _CONSENT_CACHE.pop(int(user_id), None)
    return {"consent_version": version, "ip_country": country or "ZZ", "created_at": created.isoformat() + "Z"}


async def withdraw_consent(user_id: int) -> int:
    from sqlalchemy import text
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        await _ensure_table(db)
        res = await db.execute(text(
            "UPDATE adult_consents SET revoked_at = :t WHERE user_id = :u AND revoked_at IS NULL"),
            {"u": int(user_id), "t": datetime.utcnow().replace(microsecond=0)})
        await db.commit()
    _CONSENT_CACHE.pop(int(user_id), None)
    return int(res.rowcount or 0)


# ------------------------------------------------------------------ geo
def geo_verdict(ip: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """{"country", "region", "blocked", "reason"} for the client IP."""
    cfg = cfg or config()
    geo = cfg.get("geo") or {}
    country, region = geo_country.locate(ip)
    out: Dict[str, Any] = {"country": country, "region": region, "blocked": False, "reason": ""}
    if not country:
        if str(geo.get("unknown_country", "block")) != "allow":
            out.update(blocked=True, reason="country_unknown")
        return out
    if country in {str(c).upper() for c in geo.get("blocked_countries") or []}:
        out.update(blocked=True, reason="country_" + country)
        return out
    if country == "US":
        states = {str(s).upper() for s in geo.get("blocked_us_states") or []}
        code = (region or "").upper().split("-")[-1] if region else ""
        if not code:
            if str(geo.get("us_unknown_state", "block")) != "allow" and states:
                out.update(blocked=True, reason="us_state_unknown")
        elif code in states:
            out.update(blocked=True, reason="us_state_" + code)
    return out


# ------------------------------------------------------------------ gate decision
async def gate_decision(scope: Dict[str, Any], headers: Dict[str, str],
                        cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Whether this request may see adult content: {"pass", "reason", ...}."""
    cfg = cfg or config()
    if is_internal(scope, headers):
        return {"pass": True, "reason": "internal"}
    ident = await identity(scope, headers)
    ip = client_ip(scope, headers)
    if ident is None:
        geo = geo_verdict(ip, cfg)
        return {"pass": False, "reason": "geo_blocked" if geo["blocked"] else "sign_in", "geo": geo}
    admin = bool(ident.get("is_admin"))
    geo = geo_verdict(ip, cfg)
    if geo["blocked"] and not (admin and cfg.get("admin_geo_exempt", True)):
        return {"pass": False, "reason": "geo_blocked", "geo": geo, "identity": ident}
    if admin and ident.get("via") == "api_key":
        return {"pass": True, "reason": "admin_api_key", "geo": geo, "identity": ident}
    consent = await current_consent(int(ident["user_id"]), str(cfg.get("consent_version")))
    if consent is None:
        return {"pass": False, "reason": "consent", "geo": geo, "identity": ident}
    return {"pass": True, "reason": "consent", "geo": geo, "identity": ident, "consent": consent}


def _is_gate_open(path: str) -> bool:
    return any(path == p.rstrip("/") or path.startswith(p) for p in GATE_OPEN_PREFIXES)


def in_section(path: str, cfg: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """"page" / "api" when the path belongs to the node editor family."""
    cfg = cfg or config()
    for exempt in cfg.get("section_api_exempt") or []:
        if path.startswith(exempt):
            return None
    for prefix in cfg.get("section_api") or []:
        if path == prefix.rstrip("/") or path.startswith(prefix):
            return "api"
    for page in cfg.get("section_pages") or []:
        if path == page or path.startswith(page.rstrip("/") + "/") or path == page + ".html" \
                or path == "/static" + page + ".html":
            return "page"
    return None


# ------------------------------------------------------------------ task ratings
_RATING_CACHE: Dict[str, Tuple[float, Optional[str]]] = {}
_RATING_TTL = 60.0


async def task_rating(task_id: str) -> Optional[str]:
    now = time.monotonic()
    hit = _RATING_CACHE.get(task_id)
    if hit and now - hit[0] < _RATING_TTL:
        return hit[1]
    from sqlalchemy import text
    from database import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        row = (await db.execute(text("SELECT content_rating FROM tasks WHERE id = :i"), {"i": task_id})).first()
    rating = str(row[0]).lower() if row is not None and row[0] else None
    if len(_RATING_CACHE) > 20000:
        _RATING_CACHE.clear()
    _RATING_CACHE[task_id] = (now, rating)
    return rating


def task_id_of(path: str, query: str) -> Optional[str]:
    if path in ("/task", "/task/"):
        qs = parse_qs(query or "")
        for key in ("id", "task_id", "task"):
            if qs.get(key):
                return qs[key][0].strip()[:64] or None
        return None
    m = _TASK_ITEM_RE.match(path)
    return m.group(1) if m else None


def listing_conditions(request: Optional[Request], Task) -> List[Any]:
    """SQL WHERE fragments that keep adult tasks out of listings on the main host."""
    cfg = config()
    if request is not None and mode_of(request) == "nsfw":
        return []
    if not cfg.get("main_list_filter", True):
        return []
    ratings = cfg.get("nsfw_ratings") or []
    if not ratings:
        return []
    from sqlalchemy import or_

    return [or_(Task.content_rating.is_(None), Task.content_rating.notin_(ratings))]


def is_nsfw_rating(rating: Optional[str]) -> bool:
    return bool(rating) and str(rating).lower() in config().get("nsfw_ratings", [])


def hides(request: Optional[Request], task: Any) -> bool:
    """True when this task must not be listed for this request (adult task on the main host)."""
    cfg = config()
    if request is not None and mode_of(request) == "nsfw":
        return False
    if not cfg.get("main_list_filter", True):
        return False
    return is_nsfw_rating(getattr(task, "content_rating", None))


# ------------------------------------------------------------------ text and pages
def _lang(headers: Dict[str, str], path: str) -> Tuple[str, str]:
    try:
        from user_language import language_dir, request_language_from_headers

        info = request_language_from_headers(headers, path=path)
        return info.ui, language_dir(info.ui)
    except Exception:  # noqa: BLE001
        return "en", "ltr"


def _t(key: str, lang: str, **kw: Any) -> str:
    try:
        from user_language import t

        text = t(key, lang)
    except Exception:  # noqa: BLE001
        text = key
    for k, v in kw.items():
        text = text.replace("{" + k + "}", str(v))
    return text


def error_detail(code: str, lang: str, **extra: Any) -> Dict[str, Any]:
    key = "error_" + code
    out = {"error_string": code, "message_string": _t(key, lang), "user_message_bool": True,
           "i18n_key_string": key, "language_string": lang}
    out.update(extra)
    return out


_CARD_CSS = (
    ".ag{--ag-card:#151522;--ag-fg:#f0f0f5;--ag-mute:#a3a7bd;--ag-acc:#e0245e;--ag-line:#2a2a3d;"
    "display:grid;place-items:center;padding:32px 16px;color:var(--ag-fg);font:16px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}"
    ".ag *{box-sizing:border-box}"
    ".ag .card{max-width:520px;width:100%;background:var(--ag-card);border:1px solid var(--ag-line);border-radius:16px;padding:28px}"
    ".ag .badge{display:inline-grid;place-items:center;width:56px;height:56px;border-radius:50%;border:3px solid var(--ag-acc);"
    "color:var(--ag-acc);font-weight:800;font-size:18px;margin-bottom:12px}"
    ".ag h1{font-size:22px;margin:0 0 10px;color:var(--ag-fg)}.ag p{margin:0 0 14px;color:var(--ag-mute)}"
    ".ag label{display:flex;gap:10px;align-items:flex-start;margin:0 0 12px;cursor:pointer}"
    ".ag input[type=checkbox]{width:20px;height:20px;margin-top:2px;flex:none;accent-color:var(--ag-acc)}"
    ".ag .row{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}"
    ".ag .btn{display:inline-flex;align-items:center;gap:8px;padding:11px 18px;border-radius:10px;border:1px solid var(--ag-line);"
    "background:transparent;color:var(--ag-fg);font:inherit;text-decoration:none;cursor:pointer}"
    ".ag .btn.primary{background:var(--ag-acc);border-color:var(--ag-acc);color:#fff;font-weight:600}"
    ".ag .note{font-size:13px;color:var(--ag-mute)}.ag a{color:#8ab4ff}.ag .btn.primary{color:#fff}"
    ".ag ol{color:var(--ag-mute);padding-inline-start:20px}"
    ".ag-stage{position:fixed;top:8px;inset-inline-end:8px;z-index:9999;background:#f59e0b;color:#111;font-size:12px;"
    "padding:3px 8px;border-radius:6px;text-decoration:none}"
)
_BODY_CSS = ("body{margin:0;min-height:100vh;background:#0b0b12;display:flex;flex-direction:column}"
             ".ag{flex:1}")


def _stage_badge(lang: str, staged: bool) -> str:
    if not staged:
        return ""
    return (f'<a class="ag-stage" href="/api/site-mode/stage?mode=off">'
            f'{_html.escape(_t("adult_staging_badge", lang))} ×</a>')


def _page(lang: str, direction: str, title: str, body: str, *, adult: bool, staged: bool = False) -> str:
    """A standalone page of the adult host (the gate, the terms, the geo notice)."""
    rating = (f'<meta name="rating" content="{RTA_LABEL}"><meta name="rating" content="adult">' if adult else "")
    return (
        f'<!doctype html><html lang="{lang}" dir="{direction}"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<meta name="robots" content="noindex, nofollow">{rating}'
        f"<title>{_html.escape(title)}</title><style>{_BODY_CSS}{_CARD_CSS}</style></head>"
        f'<body>{_stage_badge(lang, staged)}<main class="ag">{body}</main></body></html>'
    )


def _safe_next(value: Optional[str]) -> str:
    value = str(value or "/").strip()
    if not value.startswith("/") or value.startswith("//") or "\\" in value:
        return "/"
    if value.startswith(GATE_PATH):
        return "/"
    return value[:1024]


def gate_page(lang: str, direction: str, decision: Dict[str, Any], next_url: str, staged: bool) -> str:
    esc = _html.escape
    nxt = _safe_next(next_url)
    title = _t("adult_gate_title", lang)
    reason = decision.get("reason")
    if reason == "geo_blocked":
        body = (f'<div class="card"><div class="badge" dir="ltr">18+</div><h1>{esc(_t("adult_geo_title", lang))}</h1>'
                f'<p>{esc(_t("adult_geo_body", lang))}</p>'
                f'<div class="row"><a class="btn" href="https://autorig.online/">{esc(_t("adult_neutral_back", lang))}</a></div></div>')
        return _page(lang, direction, _t("adult_geo_title", lang), body, adult=True, staged=staged)
    terms = f'<a href="{TERMS_PATH}" target="_blank" rel="noopener">{esc(_t("adult_gate_terms_link", lang))}</a>'
    head = (f'<div class="card"><div class="badge" dir="ltr">18+</div><h1>{esc(title)}</h1>'
            f'<p>{esc(_t("adult_gate_lead", lang))}</p>')
    if reason == "sign_in":
        login = "/auth/login?" + urlencode({"next": GATE_PATH + "?" + urlencode({"next": nxt})})
        body = (head + f'<p class="note">{esc(_t("adult_gate_signin_note", lang))}</p>'
                f'<div class="row"><a class="btn primary" href="{esc(login, quote=True)}">'
                f'{esc(_t("adult_gate_signin", lang))}</a>'
                f'<a class="btn" href="https://autorig.online/">{esc(_t("adult_gate_leave", lang))}</a></div>'
                f'<p class="note" style="margin-top:16px">{terms}</p></div>')
        return _page(lang, direction, title, body, adult=True, staged=staged)
    body = (head +
            f'<form method="post" action="/api/age-gate/consent" id="ag">'
            f'<input type="hidden" name="next" value="{esc(nxt, quote=True)}">'
            f'<label><input type="checkbox" name="age_confirmed" value="1" required> '
            f'<span>{esc(_t("adult_gate_age", lang))}</span></label>'
            f'<label><input type="checkbox" name="terms_accepted" value="1" required> '
            f'<span>{esc(_t("adult_gate_terms", lang))} · {terms}</span></label>'
            f'<p class="note">{esc(_t("adult_gate_record_note", lang))}</p>'
            f'<div class="row"><button class="btn primary" type="submit">{esc(_t("adult_gate_enter", lang))}</button>'
            f'<a class="btn" href="https://autorig.online/">{esc(_t("adult_gate_leave", lang))}</a></div>'
            "</form></div>")
    return _page(lang, direction, title, body, adult=True, staged=staged)


def terms_page(lang: str, direction: str, staged: bool) -> str:
    esc = _html.escape
    items = "".join(f"<li>{esc(_t(f'adult_terms_{i}', lang))}</li>" for i in range(1, 8))
    body = (f'<div class="card"><div class="badge" dir="ltr">18+</div><h1>{esc(_t("adult_terms_title", lang))}</h1>'
            f'<ol>{items}</ol><p class="note">{esc(_t("adult_terms_version", lang, version=config().get("consent_version")))}</p>'
            f'<div class="row"><a class="btn" href="{GATE_PATH}">{esc(_t("adult_gate_title", lang))}</a></div></div>')
    return _page(lang, direction, _t("adult_terms_title", lang), body, adult=True, staged=staged)


def neutral_body(lang: str) -> str:
    esc = _html.escape
    host = nsfw_host()
    if host:
        text = _t("adult_neutral_body", lang, host=host)
        open_btn = (f'<a class="btn primary" href="https://{esc(host, quote=True)}/{GATE_PATH.lstrip("/")}" '
                    f'rel="nofollow noopener">{esc(_t("adult_neutral_open", lang, host=host))}</a>')
    else:
        text = _t("adult_neutral_body_pending", lang)
        open_btn = ""
    return (f'<div class="card"><div class="badge" dir="ltr">18+</div><h1>{esc(_t("adult_neutral_title", lang))}</h1>'
            f'<p>{esc(text)}</p><div class="row">{open_btn}'
            f'<a class="btn" href="/">{esc(_t("adult_neutral_back", lang))}</a></div></div>')


_LAYOUT: Optional[Callable[[str], str]] = None


def neutral_page(lang: str, direction: str, staged: bool) -> str:
    """The main host's neutral page: the site header and footer, no preview of anything."""
    try:
        from user_language import asset_version

        css = f'<link rel="stylesheet" href="/static/css/styles.css?v={asset_version("css/styles.css")}">'
    except Exception:  # noqa: BLE001
        css = '<link rel="stylesheet" href="/static/css/styles.css">'
    title = _t("adult_neutral_title", lang)
    doc = (
        f'<!doctype html><html lang="{lang}" dir="{direction}"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<meta name="robots" content="noindex, nofollow"><title>{_html.escape(title)} - AutoRig Online</title>'
        f"{css}<style>{_CARD_CSS}</style></head><body>{_stage_badge(lang, staged)}"
        '<div id="site-header"></div>'
        f'<main class="ag">{neutral_body(lang)}</main>'
        '<div id="site-footer"></div></body></html>'
    )
    if _LAYOUT is not None:
        try:
            doc = _LAYOUT(doc)
        except Exception as exc:  # noqa: BLE001 - the neutral page is never lost over the layout
            print(f"[site-mode] layout skipped: {exc}")
    return doc


# ------------------------------------------------------------------ middleware
class SiteModeMiddleware:
    """Picks the site mode per request and enforces the split and the gate."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            return await self.app(scope, receive, send)
        headers = _header_map(scope)
        path = scope.get("path") or "/"
        try:
            cfg = config()
            mode, staged = await resolve_mode(scope, headers, cfg)
        except Exception as exc:  # noqa: BLE001 - a config error never takes the site down
            print(f"[site-mode] mode resolution failed: {type(exc).__name__}: {exc}")
            cfg, mode, staged = DEFAULTS, "main", False
        state = scope.setdefault("state", {})
        state["site_mode"] = mode
        state["site_mode_staged"] = staged
        if mode == "nsfw":
            response = await self._adult(scope, headers, path, cfg, staged)
        else:
            response = await self._main(scope, headers, path, cfg, mode, staged)
        if response is not None:
            return await response(scope, receive, send)
        if mode != "nsfw":
            return await self.app(scope, receive, send)

        async def send_adult(message):
            if message.get("type") == "http.response.start":
                hdrs = list(message.get("headers") or [])
                have = {k.lower() for k, _ in hdrs}
                if b"rating" not in have:
                    hdrs.append((b"rating", RTA_LABEL.encode()))
                if cfg.get("nsfw_noindex", True) and b"x-robots-tag" not in have:
                    hdrs.append((b"x-robots-tag", b"noindex, nofollow"))
                message["headers"] = hdrs
            await send(message)

        return await self.app(scope, receive, send_adult)

    async def _main(self, scope, headers, path, cfg, mode, staged) -> Optional[Response]:
        split_section = cfg.get("main_split_section") or mode == "main-split"
        split_items = cfg.get("main_split_items") or mode == "main-split"
        if not (split_section or split_items):
            return None
        if is_internal(scope, headers):
            return None
        lang, direction = _lang(headers, path)
        if split_section:
            kind = in_section(path, cfg)
            if kind:
                return self._neutral(kind, lang, direction, staged)
        if split_items:
            task_id = task_id_of(path, scope.get("query_string", b"").decode("latin-1"))
            if task_id:
                try:
                    rating = await task_rating(task_id)
                except Exception as exc:  # noqa: BLE001
                    print(f"[site-mode] rating lookup failed: {exc}")
                    rating = None
                if rating and rating in cfg.get("nsfw_ratings", []):
                    kind = "page" if path.startswith("/task") else "api"
                    if path.startswith(("/thumb", "/api/thumb", "/api/video")):
                        return Response(status_code=404, headers={"Cache-Control": "no-store"})
                    return self._neutral(kind, lang, direction, staged)
        return None

    def _neutral(self, kind: str, lang: str, direction: str, staged: bool) -> Response:
        hdrs = {"X-Robots-Tag": "noindex, nofollow", "Cache-Control": "no-store"}
        if kind == "api":
            return JSONResponse(status_code=403, headers=hdrs, content={"detail": error_detail(
                "nsfw_domain_only", lang, nsfw_host_string=nsfw_host() or None)})
        return HTMLResponse(neutral_page(lang, direction, staged), status_code=200, headers=hdrs)

    async def _adult(self, scope, headers, path, cfg, staged) -> Optional[Response]:
        if path == "/robots.txt" and not staged:
            return PlainTextResponse("User-agent: *\nDisallow: /\n", headers={"Cache-Control": "public, max-age=3600"})
        if path == "/nsfw" or path == "/nsfw/":
            return RedirectResponse("/nodes", status_code=302)
        if path.startswith("/sitemap") and not staged:
            return Response(status_code=404)
        if _is_gate_open(path):
            return None
        try:
            decision = await gate_decision(scope, headers, cfg)
        except Exception as exc:  # noqa: BLE001 - the gate fails closed
            print(f"[site-mode] gate failed closed: {type(exc).__name__}: {exc}")
            decision = {"pass": False, "reason": "gate_error"}
        if decision.get("pass"):
            return None
        lang, direction = _lang(headers, path)
        method = str(scope.get("method") or "GET").upper()
        accept = headers.get("accept") or ""
        wants_html = method in ("GET", "HEAD") and not path.startswith("/api/") and (
            "text/html" in accept or "*/*" in accept or not accept)
        hdrs = {"Cache-Control": "no-store", "X-Robots-Tag": "noindex, nofollow", "Rating": RTA_LABEL}
        reason = decision.get("reason")
        if reason == "geo_blocked":
            if wants_html:
                return HTMLResponse(gate_page(lang, direction, decision, "/", staged), status_code=451, headers=hdrs)
            return JSONResponse(status_code=451, headers=hdrs, content={"detail": error_detail("adult_geo_blocked", lang)})
        if wants_html:
            query = scope.get("query_string", b"").decode("latin-1")
            nxt = path + ("?" + query if query else "")
            return RedirectResponse(GATE_PATH + "?" + urlencode({"next": nxt}), status_code=302, headers=hdrs)
        status = 401 if reason in ("sign_in", "gate_error") else 403
        return JSONResponse(status_code=status, headers=hdrs, content={"detail": error_detail(
            "adult_gate_required", lang, gate_url_string=GATE_PATH, reason_string=reason)})


# ------------------------------------------------------------------ routes
router = APIRouter(tags=["site-mode"])


def _req_headers(request: Request) -> Dict[str, str]:
    return _header_map(request.scope)


def _host_for_record(request: Request) -> str:
    return request_host(_req_headers(request)) or "unknown"


@router.get(GATE_PATH, include_in_schema=False)
async def age_gate_page(request: Request, next: str = "/"):
    headers = _req_headers(request)
    lang, direction = _lang(headers, request.url.path)
    staged = bool(getattr(request.state, "site_mode_staged", False))
    if mode_of(request) != "nsfw":
        return HTMLResponse(neutral_page(lang, direction, staged), headers={"X-Robots-Tag": "noindex, nofollow"})
    decision = await gate_decision(request.scope, headers)
    if decision.get("pass") and decision.get("reason") != "internal":
        return RedirectResponse(_safe_next(next), status_code=302)
    status = 451 if decision.get("reason") == "geo_blocked" else 200
    return HTMLResponse(gate_page(lang, direction, decision, next, staged), status_code=status,
                        headers={"Cache-Control": "no-store", "Rating": RTA_LABEL,
                                 "X-Robots-Tag": "noindex, nofollow"})


@router.get(TERMS_PATH, include_in_schema=False)
async def adult_terms_page(request: Request):
    lang, direction = _lang(_req_headers(request), request.url.path)
    return HTMLResponse(terms_page(lang, direction, bool(getattr(request.state, "site_mode_staged", False))),
                        headers={"X-Robots-Tag": "noindex, nofollow", "Rating": RTA_LABEL})


def _origin_ok(request: Request) -> bool:
    origin = request.headers.get("origin") or ""
    if not origin:
        return True                     # same-site form posts from old browsers; SameSite=Lax cookie still applies
    try:
        return request_host({"host": urlsplit(origin).netloc}) == request_host(_req_headers(request))
    except ValueError:
        return False


@router.post("/api/age-gate/consent")
async def age_gate_consent(request: Request):
    headers = _req_headers(request)
    lang, _ = _lang(headers, request.url.path)
    if mode_of(request) != "nsfw":
        raise HTTPException(status_code=404, detail=error_detail("nsfw_domain_only", lang))
    if not _origin_ok(request):
        raise HTTPException(status_code=403, detail=error_detail("adult_gate_required", lang))
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            body = await request.json()
        except ValueError:
            body = {}
        is_form = False
    else:
        form = await request.form()
        body = {k: form.get(k) for k in ("age_confirmed", "terms_accepted", "next")}
        is_form = True
    truthy = lambda v: str(v).strip().lower() in ("1", "true", "yes", "on")  # noqa: E731
    ident = await session_identity(_cookie(headers, SESSION_COOKIE))
    if ident is None:
        raise HTTPException(status_code=401, detail=error_detail("adult_gate_required", lang, reason_string="sign_in"))
    cfg = config()
    geo = geo_verdict(client_ip(request.scope, headers), cfg)
    if geo["blocked"] and not (ident.get("is_admin") and cfg.get("admin_geo_exempt", True)):
        raise HTTPException(status_code=451, detail=error_detail("adult_geo_blocked", lang))
    if not (truthy(body.get("age_confirmed")) and truthy(body.get("terms_accepted"))):
        raise HTTPException(status_code=400, detail=error_detail("adult_consent_incomplete", lang))
    rec = await record_consent(int(ident["user_id"]), str(cfg.get("consent_version")), geo.get("country"),
                               _host_for_record(request))
    if is_form:
        return RedirectResponse(_safe_next(body.get("next")), status_code=303)
    return {"success_bool": True, "consent_object": rec, "next_string": _safe_next(body.get("next"))}


@router.post("/api/age-gate/withdraw")
async def age_gate_withdraw(request: Request):
    headers = _req_headers(request)
    lang, _ = _lang(headers, request.url.path)
    if not _origin_ok(request):
        raise HTTPException(status_code=403, detail=error_detail("adult_gate_required", lang))
    ident = await session_identity(_cookie(headers, SESSION_COOKIE))
    if ident is None:
        raise HTTPException(status_code=401, detail=error_detail("adult_gate_required", lang, reason_string="sign_in"))
    return {"success_bool": True, "withdrawn_int": await withdraw_consent(int(ident["user_id"]))}


@router.get("/api/age-gate/status")
async def age_gate_status(request: Request):
    headers = _req_headers(request)
    cfg = config()
    mode = mode_of(request)
    decision = await gate_decision(request.scope, headers, cfg) if mode == "nsfw" else {"pass": None, "reason": "main"}
    geo = decision.get("geo") or {}
    return {
        "mode_string": mode,
        "staged_bool": bool(getattr(request.state, "site_mode_staged", False)),
        "pass_bool": decision.get("pass"),
        "reason_string": decision.get("reason"),
        "signed_in_bool": bool(decision.get("identity")),
        "consent_version_string": cfg.get("consent_version"),
        "consent_object": decision.get("consent"),
        "country_string": geo.get("country"),
        "geo_blocked_bool": bool(geo.get("blocked")),
    }


@router.get("/api/age-gate/check", include_in_schema=False)
async def age_gate_check(request: Request):
    """nginx auth_request: 204 lets a file through; 401 (sign in / consent) or 403 (region) stop it.

    auth_request treats any other status as an error, so the region block is 403 here."""
    headers = _req_headers(request)
    if mode_of(request) != "nsfw":
        return Response(status_code=204)
    decision = await gate_decision(request.scope, headers)
    if decision.get("pass"):
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return Response(status_code=403 if decision.get("reason") == "geo_blocked" else 401,
                    headers={"Cache-Control": "no-store"})


@router.get("/api/site-mode")
async def site_mode_info(request: Request):
    cfg = config()
    return {
        "mode_string": mode_of(request),
        "staged_bool": bool(getattr(request.state, "site_mode_staged", False)),
        "nsfw_host_string": nsfw_host(cfg) or None,
        "main_list_filter_bool": bool(cfg.get("main_list_filter")),
        "main_split_items_bool": bool(cfg.get("main_split_items")),
        "main_split_section_bool": bool(cfg.get("main_split_section")),
        "consent_version_string": cfg.get("consent_version"),
        "geo_database_object": geo_country.database_info(),
        "section_pages_array": cfg.get("section_pages"),
    }


def build_admin_router(require_admin) -> APIRouter:
    admin = APIRouter(tags=["site-mode"])

    @admin.get("/api/site-mode/stage")
    async def site_mode_stage(request: Request, mode: str = "nsfw", user=Depends(require_admin)):
        """Admin-only staging: see the adult mode or the enforced main split on this host."""
        target = GATE_PATH if mode == "nsfw" else "/nodes"
        response = RedirectResponse(target if mode in STAGE_MODES else "/", status_code=302)
        if mode in STAGE_MODES:
            response.set_cookie(STAGE_COOKIE, mode, max_age=12 * 3600, httponly=True, secure=True, samesite="lax")
        else:
            response.delete_cookie(STAGE_COOKIE)
        return response

    return admin


OAUTH_STATE_COOKIE = "ag_oauth_state"


def oauth_redirect_uri(request: Request, default: Optional[str]) -> Optional[str]:
    """The Google OAuth callback for this host: the adult domain signs in on itself.

    Returns ``default`` on the main host, so its sign-in is unchanged."""
    cfg = config()
    host = request_host(_req_headers(request))
    mapped = (cfg.get("oauth_redirect_uri") or {}).get(host)
    if mapped:
        return str(mapped)
    if host and host in cfg.get("nsfw_hosts", []):
        return f"https://{host}/auth/callback"
    return default


def google_auth_url(state: str, redirect_uri: str) -> str:
    """The same Google OAuth app as autorig.online (auth.get_google_auth_url), another callback."""
    from auth import GOOGLE_AUTH_URL
    from config import GOOGLE_CLIENT_ID

    return GOOGLE_AUTH_URL + "?" + urlencode({
        "client_id": GOOGLE_CLIENT_ID, "redirect_uri": redirect_uri, "response_type": "code",
        "scope": "openid email profile", "access_type": "online", "prompt": "select_account", "state": state})


async def exchange_code(code: str, redirect_uri: str) -> Optional[dict]:
    import httpx
    from auth import GOOGLE_TOKEN_URL
    from config import GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(GOOGLE_TOKEN_URL, data={
                "code": code, "client_id": GOOGLE_CLIENT_ID, "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": redirect_uri, "grant_type": "authorization_code"}, timeout=10.0)
        except Exception:  # noqa: BLE001
            return None
    return response.json() if response.status_code == 200 else None


def oauth_state_ok(request: Request) -> bool:
    state = request.query_params.get("state") or ""
    cookie = request.cookies.get(OAUTH_STATE_COOKIE) or ""
    import hmac

    return bool(state and cookie and hmac.compare_digest(state, cookie))


def install(app, *, require_admin, is_admin_email, api_key_user=None, layout=None) -> None:
    """main.py calls this once at import time."""
    global _API_KEY_USER, _IS_ADMIN, _LAYOUT
    _IS_ADMIN = is_admin_email
    _API_KEY_USER = api_key_user
    _LAYOUT = layout
    app.include_router(router)
    app.include_router(build_admin_router(require_admin))
    app.add_middleware(SiteModeMiddleware)

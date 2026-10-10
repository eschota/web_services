"""User language: one field the site, its API and its agents share (Localization · V3, 2026-10-10).

Owner, 2026-10-10: «мне надо чтобы и Агент понимал по английски и отвечал по английски тем у кого язык
английский, чтобы он это знал от апи сайта, и чтобы сайт был переведен на вот этот язык и локализован»
(the screenshot was a customer's Android Chrome in Persian).

Two answers are kept apart:
    code   the language to talk to this person in. Any ISO 639-1 code ("de" too, although the site has no
           German interface). Agents (Astra, support_ai, session agents) answer in it.
    ui     the site interface language actually rendered: en, ru, zh, hi, fa (fa is right to left).

Resolution, first match wins:
    explicit         the person's own choice: the language menu (cookie autorig_lang + POST /api/me/language),
                     stored on the account or the anonymous session
    url              /fa/..., /ru/... page URLs fix the interface language of that page
    browser          navigator.languages reported by the page (POST /api/me/language, explicit=false)
    accept_language  the browser's Accept-Language header
    task             the language recorded on a task when it was created (tasks.owner_language)
    default          en

API (machine-readable contract: GET /api/language):
    GET  /auth/me                  -> language {code, name, native_name, dir, ui, source, ...}, language_code
    GET  /api/me/language          the requester's language
    POST /api/me/language          {"language": "fa"} explicit choice; {"language": null} back to automatic;
                                   {"explicit": false, "browser_languages": [...]} only reports the browser
    GET  /api/task/{id}            -> owner_language, owner_language_code
    POST /api/support-chat/session -> language, language_string (stored on support_chat_sessions.language)
    GET  /api/language/resolve     ?task_id= | support_session_id= | email= | anon_id=  (admin or local agents)

Pages: the middleware serves /<lang>/<page> for ru, zh, hi, fa and localizes every HTML page on the server
(lang/dir, data-i18n text, hreflang, canonical), so crawl-critical links stay in the initial HTML.
"""
import contextvars
import hashlib
import html as _html
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

UI_LANGUAGES: Tuple[str, ...] = ("en", "ru", "zh", "hi", "fa")
PREFIX_LANGUAGES: Tuple[str, ...] = ("ru", "zh", "hi", "fa")
# Languages whose /<lang>/ pages are offered to search engines (hreflang, self canonical). zh and hi pages
# still have untranslated keys, so they stay reachable but are not advertised yet.
ADVERTISED_LANGUAGES: Tuple[str, ...] = ("en", "ru", "fa")
DEFAULT_LANGUAGE = "en"
COOKIE_NAME = "autorig_lang"
RTL_LANGUAGES = frozenset({"fa", "ar", "he", "ur", "ps", "ckb", "sd", "ug", "yi", "dv"})

# Pages whose main content is translated through data-i18n: the whole page follows the language direction.
FULL_PAGE_PATHS = frozenset({"/", "/gallery", "/buy-credits", "/how-it-works", "/dashboard", "/payment/success",
                             "/developers", "/guides"})
# Pages offered in every advertised language (hreflang cluster) and their SEO key prefix.
ADVERTISED_PATHS: Dict[str, str] = {
    "/": "home",
    "/gallery": "gallery",
    "/buy-credits": "buy",
    "/how-it-works": "hiw",
    "/developers": "developers",
}
SEO_KEYS: Dict[str, str] = {**ADVERTISED_PATHS, "/guides": "guides"}

LANGUAGE_NAMES: Dict[str, Tuple[str, str]] = {
    "en": ("English", "English"),
    "ru": ("Russian", "Русский"),
    "zh": ("Chinese", "中文"),
    "hi": ("Hindi", "हिन्दी"),
    "fa": ("Persian", "فارسی"),
    "ar": ("Arabic", "العربية"),
    "uk": ("Ukrainian", "Українська"),
    "be": ("Belarusian", "Беларуская"),
    "kk": ("Kazakh", "Қазақ тілі"),
    "uz": ("Uzbek", "Oʻzbekcha"),
    "az": ("Azerbaijani", "Azərbaycan dili"),
    "ka": ("Georgian", "ქართული"),
    "hy": ("Armenian", "Հայերեն"),
    "tr": ("Turkish", "Türkçe"),
    "de": ("German", "Deutsch"),
    "fr": ("French", "Français"),
    "es": ("Spanish", "Español"),
    "pt": ("Portuguese", "Português"),
    "it": ("Italian", "Italiano"),
    "nl": ("Dutch", "Nederlands"),
    "pl": ("Polish", "Polski"),
    "cs": ("Czech", "Čeština"),
    "sk": ("Slovak", "Slovenčina"),
    "ro": ("Romanian", "Română"),
    "hu": ("Hungarian", "Magyar"),
    "el": ("Greek", "Ελληνικά"),
    "bg": ("Bulgarian", "Български"),
    "sr": ("Serbian", "Српски"),
    "hr": ("Croatian", "Hrvatski"),
    "sv": ("Swedish", "Svenska"),
    "no": ("Norwegian", "Norsk"),
    "da": ("Danish", "Dansk"),
    "fi": ("Finnish", "Suomi"),
    "he": ("Hebrew", "עברית"),
    "ur": ("Urdu", "اردو"),
    "ps": ("Pashto", "پښتو"),
    "bn": ("Bengali", "বাংলা"),
    "ta": ("Tamil", "தமிழ்"),
    "ja": ("Japanese", "日本語"),
    "ko": ("Korean", "한국어"),
    "vi": ("Vietnamese", "Tiếng Việt"),
    "th": ("Thai", "ไทย"),
    "id": ("Indonesian", "Bahasa Indonesia"),
    "ms": ("Malay", "Bahasa Melayu"),
    "fil": ("Filipino", "Filipino"),
}
_ALIASES = {"pes": "fa", "prs": "fa", "per": "fa", "fas": "fa", "iw": "he", "in": "id", "tl": "fil",
            "zho": "zh", "chi": "zh", "cmn": "zh", "yue": "zh", "rus": "ru", "eng": "en", "hin": "hi",
            "nb": "no", "nn": "no", "ji": "yi"}
_LANG_RE = re.compile(r"^[a-z]{2,3}$")


# ----------------------------------------------------------------------------------------------- basics
def normalize_language(value: Any) -> Optional[str]:
    """'fa-IR' -> 'fa', 'zh_Hans_CN' -> 'zh', 'pes' -> 'fa'. Anything that is not a language tag -> None."""
    s = str(value or "").strip().lower().replace("_", "-")
    if not s or s == "*":
        return None
    primary = s.split("-", 1)[0]
    primary = _ALIASES.get(primary, primary)
    return primary if _LANG_RE.match(primary) else None


def parse_language_list(value: Any) -> List[str]:
    """'fa-IR,fa;q=0.9,en' or ['fa-IR', 'en'] -> ['fa', 'en'] (primary codes, priority order, no repeats)."""
    if isinstance(value, (list, tuple)):
        items = [str(x) for x in value]
    else:
        items = str(value or "").split(",")
    ranked = []
    for index, part in enumerate(items[:24]):
        bits = part.strip().split(";")
        q = 1.0
        for b in bits[1:]:
            b = b.strip()
            if b.startswith("q="):
                try:
                    q = float(b[2:])
                except ValueError:
                    q = 0.0
        code = normalize_language(bits[0])
        if code and q > 0:
            ranked.append((-q, index, code))
    ranked.sort()
    out: List[str] = []
    for _q, _i, code in ranked:
        if code not in out:
            out.append(code)
    return out


def ui_language_for(codes: Iterable[str]) -> str:
    for code in codes:
        if code in UI_LANGUAGES:
            return code
    return DEFAULT_LANGUAGE


def language_dir(code: Optional[str]) -> str:
    return "rtl" if (code or "") in RTL_LANGUAGES else "ltr"


def language_names(code: Optional[str]) -> Tuple[str, str]:
    c = normalize_language(code) or DEFAULT_LANGUAGE
    return LANGUAGE_NAMES.get(c, (c, c))


def language_instruction(code: Optional[str]) -> str:
    """One sentence for an agent's system prompt: which language to answer this person in."""
    c = normalize_language(code) or DEFAULT_LANGUAGE
    name, native = language_names(c)
    if c == "en":
        head = "Reply in English, the user's language"
    else:
        head = f"Reply in {name} ({native}), the user's language"
    if language_dir(c) == "rtl":
        head += "; it is written right to left, keep product and format names (AutoRig, GLB, FBX, Unity) in Latin"
    return head + ". If the user writes in another language, reply in the language of their message."


@dataclass
class LanguageInfo:
    code: str = DEFAULT_LANGUAGE
    ui: str = DEFAULT_LANGUAGE
    source: str = "default"
    explicit: Optional[str] = None
    detected: List[str] = field(default_factory=list)
    url_lang: Optional[str] = None
    path: str = "/"

    def payload(self) -> Dict[str, Any]:
        name, native = language_names(self.code)
        return {
            "code": self.code,
            "name": name,
            "native_name": native,
            "dir": language_dir(self.code),
            "ui": self.ui,
            "ui_dir": language_dir(self.ui),
            "source": self.source,
            "explicit": self.explicit,
            "detected": list(self.detected),
            "agent_instruction": language_instruction(self.code),
        }


def resolve_language(*, explicit: Any = None, url_lang: Any = None, browser: Any = None,
                     accept_language: Any = None, task_language: Any = None, stored_detected: Any = None,
                     path: str = "/") -> LanguageInfo:
    exp = normalize_language(explicit)
    url = normalize_language(url_lang)
    url = url if url in UI_LANGUAGES else None
    browser_codes = parse_language_list(browser) if browser else []
    accept_codes = parse_language_list(accept_language) if accept_language else []
    stored_codes = parse_language_list(stored_detected) if stored_detected else []
    detected = browser_codes or accept_codes or stored_codes
    task_code = normalize_language(task_language)
    if exp:
        code, source = exp, "explicit"
    elif browser_codes:
        code, source = browser_codes[0], "browser"
    elif accept_codes:
        code, source = accept_codes[0], "accept_language"
    elif task_code:
        code, source = task_code, "task"
    elif stored_codes:
        code, source = stored_codes[0], "profile"
    elif url:
        code, source = url, "url"
    else:
        code, source = DEFAULT_LANGUAGE, "default"
    if url:
        ui = url
    elif exp in UI_LANGUAGES:
        ui = exp
    elif code in UI_LANGUAGES:
        ui = code
    else:
        ui = ui_language_for(detected)
    return LanguageInfo(code=code, ui=ui, source=source, explicit=exp, detected=detected[:6],
                        url_lang=url, path=path or "/")


# --------------------------------------------------------------------------------- request context
_REQUEST_LANG: contextvars.ContextVar[Optional[LanguageInfo]] = contextvars.ContextVar("autorig_request_lang",
                                                                                       default=None)


def current_request_language() -> Optional[LanguageInfo]:
    return _REQUEST_LANG.get()


def _cookie_value(cookie_header: str, name: str) -> str:
    for part in str(cookie_header or "").split(";"):
        k, _, v = part.strip().partition("=")
        if k == name:
            return v.strip().strip('"')
    return ""


def request_language_from_headers(headers: Dict[str, str], *, url_lang: Optional[str] = None,
                                  path: str = "/") -> LanguageInfo:
    cookie_lang = normalize_language(_cookie_value(headers.get("cookie", ""), COOKIE_NAME))
    explicit = cookie_lang if cookie_lang in UI_LANGUAGES else None
    return resolve_language(explicit=explicit, url_lang=url_lang, accept_language=headers.get("accept-language"),
                            path=path)


def _header_map(scope) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in scope.get("headers") or []:
        try:
            out[k.decode("latin-1").lower()] = v.decode("latin-1")
        except Exception:
            continue
    return out


# Paths that never get a /<lang>/ page form.
_NO_PREFIX = ("/api/", "/auth/", "/static/", "/u/", "/renderfin/", "/dev", "/admin", "/thumb", "/m/",
              "/_", "/sitemap", "/robots.txt", "/favicon", "/llm", "/skill.md", "/airacing", "/oneclick",
              "/BingSiteAuth", "/793f81f63218433f87e43c0afd353c14.txt")
_PREFIX_RE = re.compile(r"^/(ru|zh|hi|fa)(/.*)?$")


class LanguageMiddleware:
    """Serves /<lang>/<page> as <page> in that language and records the request's language for the page
    renderer. HTML responses get Content-Language and Vary."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path") or "/"
        method = str(scope.get("method") or "GET").upper()
        url_lang = None
        m = _PREFIX_RE.match(path)
        if m and method in ("GET", "HEAD"):
            rest = m.group(2) or "/"
            if not rest.startswith(_NO_PREFIX):
                url_lang = m.group(1)
                scope = dict(scope)
                scope["path"] = rest
                raw = scope.get("raw_path")
                if isinstance(raw, (bytes, bytearray)) and raw.startswith(("/" + url_lang).encode()):
                    scope["raw_path"] = bytes(raw[len(url_lang) + 1:]) or b"/"
                else:
                    scope["raw_path"] = rest.encode("utf-8")
                scope["autorig_url_lang"] = url_lang
        try:
            info = request_language_from_headers(_header_map(scope), url_lang=url_lang, path=scope.get("path") or "/")
        except Exception:
            info = LanguageInfo(path=scope.get("path") or "/")
        token = _REQUEST_LANG.set(info)

        async def send_with_language(message):
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                ctype = b""
                vary_index = None
                for i, (k, v) in enumerate(headers):
                    lk = k.lower()
                    if lk == b"content-type":
                        ctype = v
                    elif lk == b"vary":
                        vary_index = i
                if ctype.startswith(b"text/html"):
                    headers.append((b"content-language", info.ui.encode("ascii", "ignore")))
                    if vary_index is None:
                        headers.append((b"vary", b"Accept-Language, Cookie"))
                    else:
                        k, v = headers[vary_index]
                        if b"accept-language" not in v.lower():
                            headers[vary_index] = (k, v + b", Accept-Language, Cookie")
                    message = dict(message)
                    message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_language)
        finally:
            _REQUEST_LANG.reset(token)


# ------------------------------------------------------------------------------------- translations
def _static_dir() -> Path:
    env = os.environ.get("AUTORIG_STATIC_DIR")
    if env and Path(env).is_dir():
        return Path(env)
    live = Path("/srv/autorig/current/autorig-online/static")
    if live.is_dir():
        return live
    return Path(__file__).resolve().parent.parent / "static"


_JSON_CACHE: Dict[str, Tuple[float, Dict[str, str]]] = {}


def _load_json(lang: str) -> Dict[str, str]:
    path = _static_dir() / "i18n" / f"{lang}.json"
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {}
    key = str(path)
    hit = _JSON_CACHE.get(key)
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        data = {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}
    except (OSError, ValueError):
        data = {}
    _JSON_CACHE[key] = (mtime, data)
    return data


def translations(lang: str, *, fallback: bool = True) -> Dict[str, str]:
    lang = lang if lang in UI_LANGUAGES else DEFAULT_LANGUAGE
    own = _load_json(lang)
    if not fallback or lang == DEFAULT_LANGUAGE:
        return dict(own)
    merged = dict(_load_json(DEFAULT_LANGUAGE))
    merged.update(own)
    return merged


def t(key: str, lang: Optional[str] = None, **replacements: Any) -> str:
    info = current_request_language()
    lang = lang or (info.ui if info else DEFAULT_LANGUAGE)
    text = translations(lang).get(key, key)
    for k, v in replacements.items():
        text = text.replace("{" + k + "}", str(v))
    return text


def user_error_detail(code: str, *, lang: Optional[str] = None, retry_after_seconds: Optional[int] = None,
                      key: Optional[str] = None) -> Dict[str, Any]:
    """An HTTPException detail written for the visitor: a stable code plus a sentence in their language.
    The frontend maps error_string to its own translation; other clients can show message_string."""
    info = current_request_language()
    ui = lang or (info.ui if info else DEFAULT_LANGUAGE)
    i18n_key = key or f"error_{code}"
    detail: Dict[str, Any] = {
        "error_string": code,
        "message_string": t(i18n_key, ui),
        "user_message_bool": True,
        "i18n_key_string": i18n_key,
        "language_string": ui,
    }
    if retry_after_seconds:
        detail["retry_after_seconds_int"] = int(retry_after_seconds)
    return detail


_ASSET_HASH: Dict[str, Tuple[float, str]] = {}


def asset_version(relative: str) -> str:
    path = _static_dir() / relative
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return "0"
    hit = _ASSET_HASH.get(relative)
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()[:10]
    except OSError:
        digest = "0"
    _ASSET_HASH[relative] = (mtime, digest)
    return digest


# ------------------------------------------------------------------------------- page localization
_TAG_RE = re.compile(r"<([a-zA-Z][\w:-]*)(\s[^<>]*)?>", re.S)
_SKIP_BLOCK_RE = re.compile(r"<(script|style|textarea)\b[^>]*>.*?</\1\s*>|<!--.*?-->", re.S | re.I)
_VOID = frozenset({"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source",
                   "track", "wbr"})
_SAFE_TAG_RE = re.compile(r"&lt;(/?)(strong|b|em|i|br|code|small)\s*/?&gt;", re.I)
_KEY_ATTR_RE = re.compile(r"(?:^|\s)data-i18n=\"([^\"]+)\"")
_ATTR_KEYS = (("data-i18n-placeholder", "placeholder"), ("data-i18n-title", "title"),
              ("data-i18n-aria-label", "aria-label"))


def _render_text(value: str) -> str:
    escaped = _html.escape(value, quote=False)
    if "&lt;" in escaped:
        escaped = _SAFE_TAG_RE.sub(lambda m: f"<{m.group(1)}{m.group(2).lower()}>", escaped)
    return escaped


def _set_attr(attrs: str, name: str, value: str) -> str:
    quoted = _html.escape(value, quote=True)
    pat = re.compile(r"(\s" + re.escape(name) + r")=\"[^\"]*\"")
    if pat.search(attrs):
        return pat.sub(lambda m: f'{m.group(1)}="{quoted}"', attrs, count=1)
    stripped = attrs.rstrip()
    if stripped.endswith("/"):
        return stripped[:-1].rstrip() + f' {name}="{quoted}" /'
    return attrs + f' {name}="{quoted}"'


def _find_close(doc: str, tag: str, start: int) -> Optional[Tuple[int, int]]:
    depth = 1
    pat = re.compile(r"<(/?)" + re.escape(tag) + r"(?=[\s>/])[^>]*>", re.I)
    for m in pat.finditer(doc, start):
        if m.group(1):
            depth -= 1
            if depth == 0:
                return m.start(), m.end()
        elif not m.group(0).endswith("/>"):
            depth += 1
    return None


def translate_html(doc: str, tr: Dict[str, str]) -> str:
    """Server-side data-i18n: the same keys the browser applies, so the first HTML is already localized."""
    skips = [(m.start(), m.end()) for m in _SKIP_BLOCK_RE.finditer(doc)]

    def skipped(pos: int) -> bool:
        for a, b in skips:
            if a <= pos < b:
                return True
            if a > pos:
                break
        return False

    edits: List[Tuple[int, int, str]] = []
    covered_until = -1
    for m in _TAG_RE.finditer(doc):
        start = m.start()
        attrs = m.group(2) or ""
        if "data-i18n" not in attrs or start < covered_until or skipped(start):
            continue
        tag = m.group(1).lower()
        new_attrs = attrs
        for marker, target in _ATTR_KEYS:
            km = re.search(r"(?:^|\s)" + re.escape(marker) + r"=\"([^\"]+)\"", attrs)
            if km and km.group(1) in tr:
                new_attrs = _set_attr(new_attrs, target, tr[km.group(1)])
        if new_attrs != attrs:
            edits.append((m.start(2), m.end(2), new_attrs))
        km = _KEY_ATTR_RE.search(attrs)
        if not km or tag in _VOID or attrs.rstrip().endswith("/"):
            continue
        key = km.group(1)
        if key not in tr:
            continue
        close = _find_close(doc, tag, m.end())
        if not close:
            continue
        edits.append((m.end(), close[0], _render_text(tr[key])))
        covered_until = close[0]
    for a, b, text in sorted(edits, key=lambda e: e[0], reverse=True):
        doc = doc[:a] + text + doc[b:]
    return doc


def _replace_title(doc: str, value: str) -> str:
    return re.sub(r"(<title\b[^>]*>)(.*?)(</title\s*>)", lambda m: m.group(1) + _html.escape(value, quote=False)
                  + m.group(3), doc, count=1, flags=re.S | re.I)


def _replace_meta(doc: str, attr: str, name: str, value: str) -> str:
    quoted = _html.escape(value, quote=True)
    pat = re.compile(r"(<meta\b[^>]*\b" + attr + r"=\"" + re.escape(name) + r"\"[^>]*\bcontent=\")([^\"]*)(\")",
                     re.I)
    if pat.search(doc):
        return pat.sub(lambda m: m.group(1) + quoted + m.group(3), doc, count=1)
    pat2 = re.compile(r"(<meta\b[^>]*\bcontent=\")([^\"]*)(\"[^>]*\b" + attr + r"=\"" + re.escape(name) + r"\")",
                      re.I)
    return pat2.sub(lambda m: m.group(1) + quoted + m.group(3), doc, count=1)


def _set_html_tag_attrs(doc: str, attrs: Dict[str, str]) -> str:
    m = re.search(r"<html\b([^>]*)>", doc, re.I)
    if not m:
        return doc
    inner = m.group(1)
    for name, value in attrs.items():
        inner = _set_attr(inner, name, value)
    return doc[:m.start()] + f"<html{inner}>" + doc[m.end():]


def _chrome_attrs(doc: str, lang: str, direction: str) -> str:
    for element_id in ("site-header", "site-footer"):
        doc = re.sub(r"<div id=\"" + element_id + r"\"([^>]*)>",
                     lambda m: "<div id=\"" + element_id + "\"" + _set_attr(_set_attr(m.group(1), "dir", direction),
                                                                         "lang", lang) + ">",
                     doc, count=1)
    return doc


def localized_url(path: str, lang: str, base: str) -> str:
    path = path or "/"
    if lang == DEFAULT_LANGUAGE or lang not in PREFIX_LANGUAGES:
        return f"{base}{path}"
    return f"{base}/{lang}{path if path != '/' else '/'}"


def _site_base() -> str:
    return (os.environ.get("APP_URL") or "https://autorig.online").rstrip("/")


def _page_lang(doc: str) -> str:
    m = re.search(r"<html\b[^>]*\blang=\"([^\"]+)\"", doc, re.I)
    return (normalize_language(m.group(1)) if m else None) or DEFAULT_LANGUAGE


def _rewrite_internal_links(doc: str, lang: str) -> str:
    def repl(m):
        href = m.group(2)
        bare = href.rstrip("/") or "/"
        if bare in ADVERTISED_PATHS:
            return f'{m.group(1)}"{localized_url(bare, lang, "")}"'
        return m.group(0)

    skips = [(mm.start(), mm.end()) for mm in _SKIP_BLOCK_RE.finditer(doc)]
    out, last = [], 0
    for a, b in skips:
        out.append(re.sub(r"(\shref=)\"(/[^\"#?]*)\"", repl, doc[last:a]))
        out.append(doc[a:b])
        last = b
    out.append(re.sub(r"(\shref=)\"(/[^\"#?]*)\"", repl, doc[last:]))
    return "".join(out)


def localize_page(doc: str, canonical_path: Optional[str] = None) -> str:
    """Called by main._inject_static_layout for every HTML page. Never raises."""
    info = current_request_language()
    if info is None or not isinstance(doc, str) or "<html" not in doc[:2000].lower():
        return doc
    try:
        return _localize(doc, info, canonical_path)
    except Exception as exc:  # noqa: BLE001 - a page is never lost over translation
        print(f"[i18n] page localization skipped: {type(exc).__name__}: {exc}")
        return doc


def _localize(doc: str, info: LanguageInfo, canonical_path: Optional[str]) -> str:
    path = info.path or canonical_path or "/"
    path = path.rstrip("/") or "/"
    ui = info.ui if info.ui in UI_LANGUAGES else DEFAULT_LANGUAGE
    direction = language_dir(ui)
    page_lang = _page_lang(doc)
    fixed = page_lang != DEFAULT_LANGUAGE          # a per-language article (guide-ru, rig article in hi, ...)
    scope = "page" if (path in FULL_PAGE_PATHS and not fixed) else "chrome"
    base = _site_base()

    if ui != DEFAULT_LANGUAGE:
        doc = translate_html(doc, translations(ui))
        doc = re.sub(r"(<span data-lang-current>)[^<]*(</span>)", lambda m: m.group(1) + ui.upper() + m.group(2),
                     doc, count=1)
        own = translations(ui, fallback=False)
        seo = SEO_KEYS.get(path)
        if seo and not fixed:
            title = own.get(f"seo_{seo}_title")
            desc = own.get(f"seo_{seo}_description")
            if title:
                doc = _replace_title(doc, title)
                doc = _replace_meta(doc, "property", "og:title", title)
                doc = _replace_meta(doc, "name", "twitter:title", title)
            if desc:
                doc = _replace_meta(doc, "name", "description", desc)
                doc = _replace_meta(doc, "property", "og:description", desc)
                doc = _replace_meta(doc, "name", "twitter:description", desc)

    if scope == "page":
        doc = _set_html_tag_attrs(doc, {"lang": ui, "dir": direction, "data-i18n-scope": "page"})
    else:
        doc = _set_html_tag_attrs(doc, {"data-i18n-scope": "chrome", "data-page-lang": page_lang})
        if ui != DEFAULT_LANGUAGE or fixed:
            doc = _chrome_attrs(doc, ui, direction)

    head: List[str] = []
    advertised_page = path in ADVERTISED_PATHS and not fixed
    if advertised_page:
        doc = re.sub(r"\s*<link\b[^>]*\brel=\"alternate\"[^>]*\bhreflang=\"[^\"]*\"[^>]*>", "", doc, flags=re.I)
        for lang in ADVERTISED_LANGUAGES:
            head.append(f'<link rel="alternate" hreflang="{lang}" href="{_html.escape(localized_url(path, lang, base), quote=True)}">')
        head.append(f'<link rel="alternate" hreflang="x-default" href="{_html.escape(localized_url(path, "en", base), quote=True)}">')
    canonical_target = None
    noindex = False
    if advertised_page and ui in ADVERTISED_LANGUAGES:
        canonical_target = localized_url(path, ui, base)
    elif info.url_lang:
        canonical_target = None if path == "/task" else localized_url(path, DEFAULT_LANGUAGE, base)
        noindex = True
    if canonical_target:
        quoted = _html.escape(canonical_target, quote=True)
        if re.search(r"<link\b[^>]*\brel=\"canonical\"", doc, re.I):
            doc = re.sub(r"(<link\b[^>]*\brel=\"canonical\"[^>]*\bhref=\")([^\"]*)(\")",
                         lambda m: m.group(1) + quoted + m.group(3), doc, count=1, flags=re.I)
        else:
            head.append(f'<link rel="canonical" href="{quoted}">')
    if noindex:
        if re.search(r"<meta\b[^>]*\bname=\"robots\"", doc, re.I):
            doc = _replace_meta(doc, "name", "robots", "noindex, follow")
        else:
            head.append('<meta name="robots" content="noindex, follow">')
    if ui != DEFAULT_LANGUAGE and scope == "page":
        locale = {"ru": "ru_RU", "zh": "zh_CN", "hi": "hi_IN", "fa": "fa_IR"}.get(ui)
        if locale and not re.search(r"og:locale\"", doc):
            head.append(f'<meta property="og:locale" content="{locale}">')
    if direction == "rtl":
        head.append('<link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin>')
        head.append(f'<link rel="stylesheet" id="autorig-rtl-css" href="/static/css/rtl.css?v={asset_version("css/rtl.css")}">')
    if info.url_lang:
        boot_source = "url"
    elif info.source == "explicit":
        boot_source = "explicit"
    elif ui in info.detected:
        boot_source = "accept_language"
    else:
        boot_source = "default"
    boot = {
        "lang": ui,
        "code": info.code,
        "dir": direction,
        "source": boot_source,
        "url_lang": info.url_lang,
        "scope": scope,
        "page_lang": page_lang,
        "supported": list(UI_LANGUAGES),
        "advertised": list(ADVERTISED_LANGUAGES),
    }
    boot_json = json.dumps(boot, ensure_ascii=True).replace("</", "<\\/")
    head.append(f"<script>window.__AUTORIG_LANG__={boot_json};</script>")
    block = "\n    " + "\n    ".join(head) + "\n"
    doc = re.sub(r"</head\s*>", lambda m: block + m.group(0), doc, count=1, flags=re.I)

    if info.url_lang and info.url_lang != DEFAULT_LANGUAGE:
        doc = _rewrite_internal_links(doc, info.url_lang)
    return doc


# ------------------------------------------------------------------------------------- database side
def _mapped(obj: Any, *names: str) -> bool:
    return obj is not None and all(hasattr(type(obj), n) for n in names)


def _detected_string(codes: Sequence[str]) -> Optional[str]:
    joined = ",".join(list(codes)[:6])
    return joined[:64] or None


async def subject_language(db, request=None, *, user=None, anon=None, record: bool = True,
                           browser: Any = None) -> LanguageInfo:
    """The language of a signed-in user or an anonymous visitor; records what the browser says."""
    subject = user if user is not None else anon
    headers = {k.lower(): v for k, v in (request.headers.items() if request is not None else [])}
    req = current_request_language() or request_language_from_headers(headers)
    explicit = normalize_language(getattr(subject, "preferred_language", None)) if subject is not None else None
    if not explicit and req.explicit:
        explicit = req.explicit
    info = resolve_language(explicit=explicit, browser=browser, accept_language=headers.get("accept-language"),
                            stored_detected=getattr(subject, "detected_language", None) if subject is not None else None,
                            path=req.path)
    if record and subject is not None and _mapped(subject, "detected_language", "language_updated_at"):
        detected = _detected_string(info.detected)
        changed = False
        if detected and detected != getattr(subject, "detected_language", None):
            subject.detected_language = detected
            changed = True
        if req.explicit and not getattr(subject, "preferred_language", None) and _mapped(subject, "preferred_language"):
            subject.preferred_language = req.explicit
            changed = True
        if changed:
            subject.language_updated_at = datetime.utcnow()
            try:
                await db.commit()
            except Exception as exc:  # noqa: BLE001
                print(f"[i18n] language record failed: {type(exc).__name__}: {exc}")
                try:
                    await db.rollback()
                except Exception:
                    pass
    return info


async def subject_language_fields(db, request=None, *, user=None, anon=None) -> Dict[str, Any]:
    """{'language': payload, 'language_code': code} for response models; never raises."""
    try:
        info = await subject_language(db, request, user=user, anon=anon)
        return {"language": info.payload(), "language_code": info.code}
    except Exception as exc:  # noqa: BLE001
        print(f"[i18n] subject language: {type(exc).__name__}: {exc}")
        return {"language": None, "language_code": None}


async def _anon_from_request(db, request):
    if request is None:
        return None
    anon_id = request.cookies.get("anon_id")
    if not anon_id:
        return None
    from sqlalchemy import select
    from database import AnonSession

    return (await db.execute(select(AnonSession).where(AnonSession.anon_id == anon_id))).scalar_one_or_none()


async def record_support_session_language(db, sess, request=None, *, user=None, widget_language: Any = None,
                                          browser_languages: Any = None, only_if_missing: bool = False) -> LanguageInfo:
    """Store the visitor's language on support_chat_sessions (language, language_source)."""
    if only_if_missing and getattr(sess, "language", None):
        code = normalize_language(sess.language) or DEFAULT_LANGUAGE
        return LanguageInfo(code=code, ui=code if code in UI_LANGUAGES else DEFAULT_LANGUAGE,
                            source=getattr(sess, "language_source", None) or "stored")
    anon = None
    if user is None:
        try:
            anon = await _anon_from_request(db, request)
        except Exception:
            anon = None
    info = await subject_language(db, request, user=user, anon=anon, browser=browser_languages or None)
    if info.source == "default" and normalize_language(widget_language):
        w = normalize_language(widget_language)
        info = LanguageInfo(code=w, ui=w if w in UI_LANGUAGES else DEFAULT_LANGUAGE, source="ui",
                            detected=info.detected)
    if _mapped(sess, "language", "language_source"):
        if sess.language != info.code or sess.language_source != info.source:
            sess.language = info.code
            sess.language_source = info.source
            try:
                await db.commit()
            except Exception as exc:  # noqa: BLE001
                print(f"[i18n] support language store failed: {type(exc).__name__}: {exc}")
                try:
                    await db.rollback()
                except Exception:
                    pass
    return info


def support_session_payload(sess) -> Optional[Dict[str, Any]]:
    code = normalize_language(getattr(sess, "language", None))
    if not code:
        return None
    info = LanguageInfo(code=code, ui=code if code in UI_LANGUAGES else DEFAULT_LANGUAGE,
                        source=getattr(sess, "language_source", None) or "stored")
    return info.payload()


async def support_session_language(db, session_id: int) -> Optional[Dict[str, Any]]:
    from sqlalchemy import select
    from database import SupportChatSession

    sess = (await db.execute(select(SupportChatSession).where(SupportChatSession.id == int(session_id)))
            ).scalar_one_or_none()
    return support_session_payload(sess) if sess is not None else None


def support_language_line(sess) -> str:
    """HTML line for the operator's Telegram topic: the visitor's language and what to answer in."""
    code = normalize_language(getattr(sess, "language", None))
    if not code:
        return ""
    name, native = language_names(code)
    label = name if native == name else f"{name} · {native}"
    return f"\n🗣 <b>{_html.escape(code)}</b> {_html.escape(label)} — reply in {_html.escape(name)}"


async def task_language(db, task) -> LanguageInfo:
    """The task owner's language: their explicit choice, else the language recorded when the task was created,
    else what their browser last said."""
    from sqlalchemy import select
    from database import AnonSession, User

    owner = None
    owner_type = str(getattr(task, "owner_type", "") or "")
    owner_id = str(getattr(task, "owner_id", "") or "")
    try:
        if owner_type == "user" and owner_id:
            owner = (await db.execute(select(User).where(User.email == owner_id))).scalar_one_or_none()
        elif owner_type == "anon" and owner_id:
            owner = (await db.execute(select(AnonSession).where(AnonSession.anon_id == owner_id))).scalar_one_or_none()
    except Exception:
        owner = None
    return resolve_language(
        explicit=getattr(owner, "preferred_language", None) if owner is not None else None,
        task_language=getattr(task, "owner_language", None),
        stored_detected=getattr(owner, "detected_language", None) if owner is not None else None,
    )


async def task_language_fields(db, task) -> Dict[str, Any]:
    try:
        info = await task_language(db, task)
        return {"owner_language": info.payload(), "owner_language_code": info.code}
    except Exception as exc:  # noqa: BLE001
        print(f"[i18n] task language: {type(exc).__name__}: {exc}")
        return {"owner_language": None, "owner_language_code": None}


def _task_before_insert(_mapper, _connection, target) -> None:
    """New tasks record the creating request's language (tasks.owner_language)."""
    try:
        if not _mapped(target, "owner_language") or getattr(target, "owner_language", None):
            return
        info = current_request_language()
        if info is not None and (info.source != "default" or info.detected):
            target.owner_language = info.code
    except Exception:
        pass


# ---------------------------------------------------------------------------------------------- API
CONTRACT: Dict[str, Any] = {
    "purpose_string": ("The language to talk to each AutoRig user in. Agents (Astra, support_ai, session agents) "
                       "read it from this API and never guess. Answer in language.code; if the user writes in "
                       "another language, answer in the language of their message."),
    "fields_json": {
        "code": "ISO 639-1 language to answer in (any language, e.g. de)",
        "name / native_name": "English and native name",
        "dir": "ltr | rtl for code",
        "ui": "site interface language in use: en | ru | zh | hi | fa",
        "source": "explicit | browser | accept_language | task | profile | url | ui | default",
        "explicit": "the user's own choice or null",
        "detected": "browser languages, priority order",
        "agent_instruction": "ready sentence for a system prompt",
    },
    "endpoints_json": {
        "GET /auth/me": "language, language_code for the signed-in user or the anonymous visitor",
        "GET /api/me/language": "the requester's language",
        "POST /api/me/language": "{language: 'fa'} explicit choice; {language: null} automatic; "
                                 "{explicit: false, browser_languages: [...]} report the browser",
        "GET /api/task/{task_id}": "owner_language, owner_language_code",
        "POST /api/support-chat/session": "language, language_string (support_chat_sessions.language)",
        "GET /api/language/resolve": "?task_id= | support_session_id= | email= | anon_id= (admin or local agents)",
    },
    "ui_languages": list(UI_LANGUAGES),
    "page_urls_string": "https://autorig.online/<ru|zh|hi|fa>/<page>; English has no prefix",
}


def _is_local_agent(request) -> bool:
    """A direct call to 127.0.0.1:8200 from an agent on this host (nginx always adds X-Real-IP)."""
    host = getattr(getattr(request, "client", None), "host", "") or ""
    h = request.headers
    return host in ("127.0.0.1", "::1") and not h.get("x-real-ip") and not h.get("x-forwarded-for")


def build_router(*, get_db, get_current_user, get_anon_session):
    from fastapi import APIRouter, Depends, HTTPException, Request, Response
    from pydantic import BaseModel, Field

    router = APIRouter()

    class LanguageUpdate(BaseModel):
        language: Optional[str] = Field(None, max_length=32)
        explicit: bool = True
        ui_language: Optional[str] = Field(None, max_length=32)
        browser_languages: Optional[List[str]] = Field(None, max_length=12)

    def _cookie(response: Response, code: Optional[str]) -> None:
        if code and code in UI_LANGUAGES:
            response.set_cookie(COOKIE_NAME, code, max_age=365 * 24 * 3600, samesite="lax", secure=True,
                                httponly=False, path="/")
        elif code is None:
            response.delete_cookie(COOKIE_NAME, path="/")

    @router.get("/api/language")
    async def api_language_contract():
        return CONTRACT

    @router.get("/api/me/language")
    async def api_me_language_get(request: Request, response: Response, user=Depends(get_current_user),
                                  db=Depends(get_db)):
        anon = None if user else await get_anon_session(request, response, db)
        info = await subject_language(db, request, user=user, anon=anon)
        return {"language": info.payload(), "language_code": info.code, "subject": "user" if user else "anon",
                "ui_languages": list(UI_LANGUAGES)}

    @router.post("/api/me/language")
    async def api_me_language_post(body: LanguageUpdate, request: Request, response: Response,
                                   user=Depends(get_current_user), db=Depends(get_db)):
        anon = None if user else await get_anon_session(request, response, db)
        subject = user if user is not None else anon
        if body.explicit:
            code = normalize_language(body.language) if body.language else None
            if body.language and not code:
                raise HTTPException(status_code=400, detail={"error_string": "bad_language",
                                                             "message_string": "language must be an ISO 639-1 code"})
            if _mapped(subject, "preferred_language", "language_updated_at"):
                subject.preferred_language = code
                subject.language_updated_at = datetime.utcnow()
                await db.commit()
            _cookie(response, code)
        info = await subject_language(db, request, user=user, anon=anon, browser=body.browser_languages or None)
        return {"language": info.payload(), "language_code": info.code, "subject": "user" if user else "anon",
                "stored_bool": _mapped(subject, "preferred_language")}

    @router.get("/api/language/resolve")
    async def api_language_resolve(request: Request, task_id: Optional[str] = None,
                                   support_session_id: Optional[int] = None, email: Optional[str] = None,
                                   anon_id: Optional[str] = None, user=Depends(get_current_user),
                                   db=Depends(get_db)):
        if not (_is_local_agent(request) or (user is not None and getattr(user, "is_admin", False))):
            raise HTTPException(status_code=403, detail="admin or local agent only")
        from sqlalchemy import select
        from database import AnonSession, SupportChatSession, Task, User

        if task_id:
            task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
            if task is None:
                raise HTTPException(status_code=404, detail="task not found")
            info = await task_language(db, task)
            return {"subject": "task", "task_id": task_id, "language": info.payload(), "language_code": info.code}
        if support_session_id is not None:
            sess = (await db.execute(select(SupportChatSession).where(
                SupportChatSession.id == int(support_session_id)))).scalar_one_or_none()
            if sess is None:
                raise HTTPException(status_code=404, detail="support session not found")
            payload = support_session_payload(sess)
            if payload is None and getattr(sess, "user_email", None):
                owner = (await db.execute(select(User).where(User.email == sess.user_email))).scalar_one_or_none()
                payload = resolve_language(explicit=getattr(owner, "preferred_language", None),
                                           stored_detected=getattr(owner, "detected_language", None)).payload()
            return {"subject": "support_session", "support_session_id": int(support_session_id),
                    "language": payload, "language_code": (payload or {}).get("code")}
        owner = None
        if email:
            owner = (await db.execute(select(User).where(User.email == email))).scalar_one_or_none()
        elif anon_id:
            owner = (await db.execute(select(AnonSession).where(AnonSession.anon_id == anon_id))).scalar_one_or_none()
        else:
            raise HTTPException(status_code=400, detail="pass task_id, support_session_id, email or anon_id")
        if owner is None:
            raise HTTPException(status_code=404, detail="not found")
        info = resolve_language(explicit=getattr(owner, "preferred_language", None),
                                stored_detected=getattr(owner, "detected_language", None))
        return {"subject": "user" if email else "anon", "language": info.payload(), "language_code": info.code}

    return router


def install(app, *, get_db, get_current_user, get_anon_session) -> None:
    """main.py calls this once at import time."""
    app.include_router(build_router(get_db=get_db, get_current_user=get_current_user,
                                    get_anon_session=get_anon_session))
    app.add_middleware(LanguageMiddleware)
    try:
        from sqlalchemy import event
        from database import Task

        if not event.contains(Task, "before_insert", _task_before_insert):
            event.listen(Task, "before_insert", _task_before_insert)
    except Exception as exc:  # noqa: BLE001
        print(f"[i18n] task language listener not installed: {exc}")
    print("[i18n] user language API, /<lang>/ pages and page localization installed")

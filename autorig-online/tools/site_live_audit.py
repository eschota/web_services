#!/usr/bin/env python3
"""Live audit of autorig.online (site refresh to V3, 2026-10-16). Re-run it after every deploy.

    py -3.11 tools/site_live_audit.py                      # audit https://autorig.online, write docs/site-v3-refresh/live-audit-<date>.{md,json}
    py -3.11 tools/site_live_audit.py --only /animal-rig   # only URLs containing the substring
    py -3.11 tools/site_live_audit.py --base https://staging.example --out-dir C:/tmp/audit --workers 4

What it audits (every URL of /sitemap/pages.xml, plus /ru /zh /hi /fa of the FULL_PAGE_PATHS pages):
  status, <html lang>/dir, data-i18n-scope, title (length, uniqueness), meta description (length), canonical (self-consistent
  for the language), hreflang cluster (each target answers 200 and points back), robots meta / X-Robots-Tag, og:image (200,
  an image, >= 1200x630 if JPEG/PNG), JSON-LD validity, exactly one <h1>, banned wording (V2, OpenPose, Animal V2,
  coming soon) outside scripts, broken /static/ links, and that the server-rendered header and footer are filled.

A language variant that the English page does not advertise through hreflang yet (the backend release with the new language
paths is pending) is expected to be noindex with the English canonical: it is reported as "pending", not as a failure.
KNOWN_PENDING lists other known open items with their reason; empty it when the release is live.
Exit code 1 when any non-pending HARD check fails. stdlib only.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import html
import json
import re
import struct
import sys
import urllib.error
import urllib.request
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

ROOT = Path(__file__).resolve().parents[1]
UA = "AutoRigLiveAudit/1.0 (+https://autorig.online)"
LANGS = ("ru", "zh", "hi", "fa")
FULL_PAGE_PATHS = ["/", "/gallery", "/buy-credits", "/how-it-works", "/developers", "/guides", "/faq", "/animal-rig",
                   "/blender-plugin", "/t-pose-rig"]
TEXT_PATHS = ("/llm.txt", "/llms.txt", "/skill.md")  # not HTML: status + content type only
# path regex -> reason. A failure on a matching URL is reported as pending, not as a failure.
KNOWN_PENDING: Dict[str, str] = {
    r"^/(ru|zh|hi|fa)/developers$": "backend release: '/dev' in LanguageMiddleware._NO_PREFIX swallows /developers",
}
BANNED = [
    ("V2", re.compile(r"\bV2\b")),
    ("Animal V2", re.compile(r"Animal\s+V2", re.I)),
    ("OpenPose", re.compile(r"open\s*-?\s*pose", re.I)),
    ("coming soon", re.compile(r"coming\s+soon", re.I)),
]
HARD, SOFT = "hard", "soft"
SEVERITY = {
    "status": HARD, "title_missing": HARD, "title_duplicate": HARD, "desc_missing": HARD, "canonical_missing": HARD,
    "canonical_mismatch": HARD, "hreflang_target": HARD, "hreflang_reciprocal": HARD, "robots_noindex": HARD,
    "og_missing": HARD, "og_status": HARD, "og_not_image": HARD, "ld_invalid": HARD, "h1_count": HARD,
    "banned_text": HARD, "broken_static": HARD, "chrome_missing": HARD, "lang_mismatch": HARD, "text_file": HARD,
    "title_long": SOFT, "desc_long": SOFT, "desc_short": SOFT, "og_small": SOFT, "ld_none": SOFT, "ld_context": SOFT,
}


# ----------------------------------------------------------------------------------------------- fetching
class Resp:
    def __init__(self, url, status, headers, body, final_url, error=""):
        self.url, self.status, self.headers, self.body, self.final_url, self.error = url, status, headers, body, final_url, error

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", "replace")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


_opener = urllib.request.build_opener(NoRedirect)
_cache: Dict[Tuple[str, str], Resp] = {}


def fetch(url: str, method: str = "GET", max_bytes: int = 3_000_000, timeout: int = 40) -> Resp:
    key = (method, url)
    if key in _cache:
        return _cache[key]
    last = Resp(url, 0, {}, b"", url, "no attempt")
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, method=method, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
            try:
                r = _opener.open(req, timeout=timeout)
            except urllib.error.HTTPError as e:
                r = e
            body = b"" if method == "HEAD" else r.read(max_bytes)
            last = Resp(url, r.status if hasattr(r, "status") else r.code, {k.lower(): v for k, v in r.headers.items()},
                        body, url)
            break
        except Exception as exc:  # noqa: BLE001
            last = Resp(url, 0, {}, b"", url, f"{type(exc).__name__}: {exc}")
    _cache[key] = last
    return last


def head_ok(url: str) -> Resp:
    r = fetch(url, "HEAD")
    if r.status in (0, 405, 501):
        r = fetch(url, "GET", max_bytes=400_000)
    return r


def image_size(data: bytes) -> Optional[Tuple[int, int, str]]:
    if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
        w, h = struct.unpack(">II", data[16:24])
        return w, h, "png"
    if data[:2] == b"\xff\xd8":
        i = 2
        while i + 9 < len(data):
            if data[i] != 0xFF:
                i += 1
                continue
            marker = data[i + 1]
            if marker in (0xD8, 0x01) or 0xD0 <= marker <= 0xD7:
                i += 2
                continue
            seg = struct.unpack(">H", data[i + 2:i + 4])[0]
            if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                h, w = struct.unpack(">HH", data[i + 5:i + 9])
                return w, h, "jpeg"
            i += 2 + seg
    return None


# ----------------------------------------------------------------------------------------------- parsing
class Page(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.html_attrs: Dict[str, str] = {}
        self.title = ""
        self._in_title = False
        self._in_head = False
        self._title_done = False
        self.metas: List[Dict[str, str]] = []
        self.links: List[Dict[str, str]] = []
        self.h1 = 0
        self.ld: List[str] = []
        self._ld = False
        self._skip = 0
        self.text: List[str] = []
        self.refs: List[Tuple[str, str]] = []
        self.chrome: Dict[str, Dict[str, Any]] = {}
        self._chrome_stack: List[Tuple[str, int]] = []
        self.depth = 0
        self.has_header_tag = False
        self.has_footer_tag = False
        self.lang_boot: Dict[str, Any] = {}
        self._script_buf: List[str] = []
        self._script_kind = ""

    def handle_starttag(self, tag, attrs):
        a = {k: (v or "") for k, v in attrs}
        self.depth += 1
        if tag == "html":
            self.html_attrs = a
        elif tag == "head":
            self._in_head = True
        elif tag == "title" and self._in_head and not self._title_done:
            self._in_title = True
        elif tag == "meta":
            self.metas.append(a)
        elif tag == "link":
            self.links.append(a)
        elif tag == "h1":
            self.h1 += 1
        elif tag == "header":
            self.has_header_tag = True
        elif tag == "footer":
            self.has_footer_tag = True
        elif tag in ("script", "style", "noscript"):
            self._skip += 1
            self._script_kind = tag
            self._script_buf = []
            if tag == "script" and a.get("type", "").lower() == "application/ld+json":
                self._ld = True
        if a.get("id") in ("site-header", "site-footer"):
            self.chrome[a["id"]] = {"server": a.get("data-server-rendered") == "1", "tags": 0}
            self._chrome_stack.append((a["id"], self.depth))
        elif self._chrome_stack and tag not in ("br", "img", "meta", "link", "input"):
            self.chrome[self._chrome_stack[-1][0]]["tags"] += 1
        for attr in ("href", "src", "poster"):
            if a.get(attr, "").startswith("/static/"):
                self.refs.append((tag, a[attr]))
        for attr in ("alt", "title", "aria-label"):
            if a.get(attr) and tag not in ("html",):
                self.text.append(a[attr])

    def handle_endtag(self, tag):
        if tag == "head":
            self._in_head = False
        if tag == "title" and self._in_title:
            self._in_title = False
            self._title_done = True
        elif tag in ("script", "style", "noscript"):
            if self._ld and tag == "script":
                self.ld.append("".join(self._script_buf))
                self._ld = False
            elif tag == "script":
                m = re.search(r"window\.__AUTORIG_LANG__\s*=\s*(\{.*?\});", "".join(self._script_buf), re.S)
                if m:
                    try:
                        self.lang_boot = json.loads(m.group(1))
                    except ValueError:
                        pass
            self._skip = max(0, self._skip - 1)
        if self._chrome_stack and self.depth <= self._chrome_stack[-1][1]:
            self._chrome_stack.pop()
        self.depth -= 1

    def handle_data(self, data):
        if self._in_title:
            self.title += data
        if self._skip:
            self._script_buf.append(data)
        else:
            self.text.append(data)

    def meta(self, key: str, name: str) -> Optional[str]:
        for m in self.metas:
            if m.get(key, "").lower() == name.lower() and "content" in m:
                return m["content"]
        return None


# ----------------------------------------------------------------------------------------------- audit of one URL
def lang_of(path: str) -> Tuple[str, str]:
    """(url language prefix or '', page path without it)."""
    m = re.match(r"^/(ru|zh|hi|fa)(/.*)?$", path)
    if m:
        return m.group(1), (m.group(2) or "/")
    return "", path


def expected_fixed_lang(path: str) -> str:
    m = re.search(r"-(ru|zh|hi)$", path)
    return m.group(1) if m else ""


def pending_reason(path: str) -> str:
    for rx, why in KNOWN_PENDING.items():
        if re.search(rx, path):
            return why
    return ""


def hreflang_of(p: Page) -> Dict[str, str]:
    out = {}
    for l in p.links:
        if l.get("rel", "").lower() == "alternate" and l.get("hreflang"):
            out[l["hreflang"].lower()] = l.get("href", "")
    return out


def canonical_of(p: Page) -> str:
    for l in p.links:
        if l.get("rel", "").lower() == "canonical":
            return l.get("href", "")
    return ""


def audit_page(url: str, base: str, check_assets: bool) -> Dict[str, Any]:
    path = urlparse(url).path or "/"
    r = fetch(url)
    res: Dict[str, Any] = {"url": url, "path": path, "status": r.status, "issues": [], "pending": [], "info": {}}
    reason = pending_reason(path)

    def issue(kind, detail=""):
        res["issues"].append({"kind": kind, "severity": SEVERITY.get(kind, HARD), "detail": detail})

    if path in TEXT_PATHS:
        res["type"] = "text"
        ct = r.headers.get("content-type", "")
        res["info"].update({"content_type": ct, "bytes": len(r.body)})
        if r.status != 200 or not r.body.strip():
            issue("text_file", f"status {r.status}, {len(r.body)} bytes")
        _settle(res, reason)
        return res
    res["type"] = "html"
    if r.status != 200:
        issue("status", f"HTTP {r.status} {r.error}".strip())
        _settle(res, reason)
        return res
    p = Page()
    try:
        p.feed(r.text)
    except Exception as exc:  # noqa: BLE001
        issue("h1_count", f"HTML parse error {exc}")
    url_lang, page_path = lang_of(path)
    adv_cluster: Dict[str, str] = {}
    res["url_lang"] = url_lang
    # is this variant advertised by the English page's hreflang cluster?
    advertised = True
    if url_lang:
        # Pending signature today: the variant is served as a noindex copy of the English page (canonical -> English,
        # <html lang> stays en). Once the backend release lists the path in ADVERTISED_PATHS this turns false.
        want_en = base + page_path
        canon_now = canonical_of(p).rstrip("/")
        robots_now = ((p.meta("name", "robots") or "") + r.headers.get("x-robots-tag", "")).lower()
        advertised = not ("noindex" in robots_now and canon_now == want_en.rstrip("/"))
    res["advertised"] = advertised

    # --- language / dir / scope
    html_lang = p.html_attrs.get("lang", "")
    html_dir = p.html_attrs.get("dir", "")
    scope = p.html_attrs.get("data-i18n-scope", "")
    boot = p.lang_boot or {}
    res["info"].update({"html_lang": html_lang, "dir": html_dir, "scope": scope or boot.get("scope", ""),
                        "page_lang": p.html_attrs.get("data-page-lang", "")})
    fixed = expected_fixed_lang(page_path)
    if url_lang and advertised:
        want_lang = url_lang
    elif url_lang and not advertised:
        want_lang = html_lang if html_lang in ("en", url_lang) else "en"  # noindex copy: en (chrome scope) or the page language
    else:
        want_lang = fixed or "en"
    if html_lang != want_lang:
        (res["pending"] if (url_lang and not advertised) else res["issues"]).append(
            {"kind": "lang_mismatch", "severity": HARD, "detail": f"<html lang={html_lang!r}>, expected {want_lang!r}"})
    if url_lang == "fa" and advertised and html_dir != "rtl":
        issue("lang_mismatch", f"/fa page without dir=rtl (dir={html_dir!r})")

    # --- title / description
    title = html.unescape(p.title).strip()
    desc = (p.meta("name", "description") or "").strip()
    res["title"], res["title_len"], res["desc_len"] = title, len(title), len(desc)
    if not title:
        issue("title_missing")
    elif len(title) > 60:
        issue("title_long", f"{len(title)} chars")
    if not desc:
        issue("desc_missing")
    elif len(desc) > 160:
        issue("desc_long", f"{len(desc)} chars")
    elif len(desc) < 70:
        issue("desc_short", f"{len(desc)} chars")

    # --- canonical
    canon = canonical_of(p)
    res["canonical"] = canon
    if url_lang and not advertised:
        want_canon = base + page_path
    else:
        want_canon = base + path
    if want_canon != base + "/" and want_canon.endswith("/"):
        want_canon = want_canon.rstrip("/")
    if not canon:
        issue("canonical_missing")
    elif canon.rstrip("/") != want_canon.rstrip("/"):
        (res["pending"] if (url_lang and not advertised) else res["issues"]).append(
            {"kind": "canonical_mismatch", "severity": HARD, "detail": f"canonical {canon}, expected {want_canon}"})

    # --- robots
    robots = (p.meta("name", "robots") or "").lower()
    xrobots = r.headers.get("x-robots-tag", "").lower()
    res["robots"] = robots or xrobots or "-"
    if "noindex" in robots or "noindex" in xrobots:
        rec = {"kind": "robots_noindex", "severity": HARD, "detail": f"robots={robots or xrobots}"}
        (res["pending"] if (url_lang and not advertised) else res["issues"]).append(rec)

    # --- hreflang cluster
    cluster = hreflang_of(p)
    res["hreflang"] = cluster
    for lg, target in cluster.items():
        tr = fetch(target)
        if tr.status != 200:
            why = pending_reason(urlparse(target).path)
            rec = {"kind": "hreflang_target", "severity": HARD, "detail": f"{lg} -> {target} answers {tr.status}"
                   + (f" | pending: {why}" if why else "")}
            (res["pending"] if why else res["issues"]).append(rec)
            continue
        tp = Page()
        tp.feed(tr.text)
        t_lang, t_page = lang_of(urlparse(target).path)
        t_robots = ((tp.meta("name", "robots") or "") + tr.headers.get("x-robots-tag", "")).lower()
        if t_lang and "noindex" in t_robots and canonical_of(tp).rstrip("/") == (base + t_page).rstrip("/"):
            res["pending"].append({"kind": "hreflang_target", "severity": HARD,
                                   "detail": f"{lg} -> {target} is a noindex copy until the language-path release"})
            continue
        back = hreflang_of(tp)
        if url.rstrip("/") not in {v.rstrip("/") for v in back.values()} and target.rstrip("/") != url.rstrip("/"):
            issue("hreflang_reciprocal", f"{target} does not list {url}")

    # --- og:image
    og = p.meta("property", "og:image")
    res["og_image"] = og or ""
    if not og:
        issue("og_missing")
    else:
        og_abs = urljoin(url, og)
        ir = fetch(og_abs, "GET", max_bytes=600_000)
        ct = ir.headers.get("content-type", "")
        if ir.status != 200:
            issue("og_status", f"{og_abs} answers {ir.status}")
        elif not ct.startswith("image/"):
            issue("og_not_image", f"{og_abs} content-type {ct}")
        else:
            sz = image_size(ir.body)
            res["og_size"] = f"{sz[0]}x{sz[1]}" if sz else "?"
            if sz and (sz[0] < 1200 or sz[1] < 630):
                issue("og_small", f"{og_abs} is {sz[0]}x{sz[1]}")

    # --- JSON-LD
    if not p.ld:
        issue("ld_none")
    for i, raw in enumerate(p.ld):
        try:
            doc = json.loads(raw)
        except ValueError as exc:
            issue("ld_invalid", f"block {i}: {exc}")
            continue
        items = doc if isinstance(doc, list) else [doc]
        if not any(isinstance(x, dict) and ("@context" in x or "@graph" in x) for x in items):
            issue("ld_context", f"block {i} has no @context")
    res["ld_blocks"] = len(p.ld)

    # --- h1
    res["h1"] = p.h1
    if p.h1 != 1:
        issue("h1_count", f"{p.h1} <h1>")

    # --- banned wording (visible text, title, description, alt/title/aria attributes)
    blob = " ".join(p.text) + " " + title + " " + desc
    blob = re.sub(r"\s+", " ", blob)
    hits = []
    for name, rx in BANNED:
        for m in rx.finditer(blob):
            hits.append(f"{name}: …{blob[max(0, m.start() - 40):m.end() + 40]}…")
            break
    res["banned"] = [h.split(":")[0] for h in hits]
    for h in hits:
        issue("banned_text", h)

    # --- broken /static links
    broken = []
    if check_assets:
        for tag, ref in sorted(set(p.refs)):
            clean = ref.split("#")[0].split("?")[0]
            ar = head_ok(urljoin(base, clean))
            if ar.status != 200:
                broken.append(f"{ref} ({ar.status})")
    res["broken_static"] = broken
    for b in broken:
        issue("broken_static", b)

    # --- chrome (server-rendered header and footer are filled)
    raw = r.text
    hdr = re.search(r'<div id="site-header"[^>]*>\s*<header\b', raw) is not None
    ftr = re.search(r'<div id="site-footer"[^>]*>\s*<footer\b', raw) is not None
    res["chrome"] = hdr and ftr
    raw_hint = "" if "__AUTORIG_LANG__" in raw else " (no backend injection at all: served as a raw file, nginx alias?)"
    if not hdr:
        issue("chrome_missing", "#site-header is empty or absent" + raw_hint)
    if not ftr:
        issue("chrome_missing", "#site-footer is empty or absent" + raw_hint)
    _settle(res, reason)
    return res


def _settle(res, reason):
    """Known-pending reasons turn every issue of a URL into pending."""
    if reason and res["issues"]:
        for i in res["issues"]:
            i["detail"] = (i["detail"] + " | pending: " + reason).strip(" |")
        res["pending"].extend(res["issues"])
        res["issues"] = []
    if res.get("url_lang") and not res.get("advertised", True) and res["pending"] and not res["issues"]:
        res["pending_reason"] = "language variant not advertised yet (backend release pending)"
    elif reason:
        res["pending_reason"] = reason


# ----------------------------------------------------------------------------------------------- driver
def sitemap_urls(base: str) -> List[str]:
    r = fetch(base + "/sitemap/pages.xml")
    if r.status != 200:
        raise SystemExit(f"cannot fetch sitemap/pages.xml: HTTP {r.status} {r.error}")
    return re.findall(r"<loc>([^<]+)</loc>", r.text)


def run(base: str, workers: int, only: str, assets: bool) -> Dict[str, Any]:
    urls = sitemap_urls(base)
    in_sitemap = set(urls)
    for p in FULL_PAGE_PATHS:
        for l in LANGS:
            u = base + "/" + l + (p if p != "/" else "/")
            if u not in in_sitemap:
                urls.append(u)
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    if only:
        uniq = [u for u in uniq if only in u]
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(lambda u: audit_page(u, base, assets), uniq))
    for r in results:
        r["in_sitemap"] = r["url"] in in_sitemap
    # title uniqueness (non-pending, same language group)
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        if r.get("type") == "html" and r.get("title") and not r["pending"] and r["status"] == 200 and r.get("advertised", True):
            groups.setdefault(r["title"], []).append(r)
    for t, rs in groups.items():
        if len(rs) > 1:
            for r in rs:
                r["issues"].append({"kind": "title_duplicate", "severity": HARD,
                                    "detail": "same title as " + ", ".join(x["path"] for x in rs if x is not r)[:200]})
    return {"base": base, "results": results}


def summarize(data: Dict[str, Any]) -> Dict[str, Any]:
    rs = data["results"]
    by_kind: Dict[str, int] = {}
    pend_kind: Dict[str, int] = {}
    hard = soft = 0
    failed_urls = set()
    for r in rs:
        for i in r["issues"]:
            by_kind[i["kind"]] = by_kind.get(i["kind"], 0) + 1
            if i["severity"] == HARD:
                hard += 1
                failed_urls.add(r["url"])
            else:
                soft += 1
        for i in r["pending"]:
            pend_kind[i["kind"]] = pend_kind.get(i["kind"], 0) + 1
    pending_urls = [r["url"] for r in rs if r["pending"] and not any(i["severity"] == HARD for i in r["issues"])]
    return {"pages": len(rs), "hard_failures": hard, "soft_warnings": soft, "pages_with_hard_failures": len(failed_urls),
            "pending_pages": len(pending_urls), "by_kind": dict(sorted(by_kind.items())),
            "pending_by_kind": dict(sorted(pend_kind.items()))}


def cell(r, key, default="-"):
    v = r.get(key)
    return default if v in (None, "") else str(v)


def status_of(r) -> str:
    if any(i["severity"] == HARD for i in r["issues"]):
        return "FAIL"
    if r["pending"]:
        return "pending"
    if r["issues"]:
        return "warn"
    return "ok"


def render_md(data: Dict[str, Any], summary: Dict[str, Any], when: str) -> str:
    rs = data["results"]
    L = [f"# Live audit {data['base']} — {when}", ""]
    L.append(f"**Summary:** {summary['pages']} pages, {summary['hard_failures']} hard failures on "
             f"{summary['pages_with_hard_failures']} pages, {summary['soft_warnings']} warnings, "
             f"{summary['pending_pages']} pages pending release. Failures by kind: "
             + (", ".join(f"{k} {v}" for k, v in summary["by_kind"].items()) or "none") + ". Pending by kind: "
             + (", ".join(f"{k} {v}" for k, v in summary["pending_by_kind"].items()) or "none") + ".")
    L += ["", "Legend: ok / warn (soft only) / pending (known open item or language variant not advertised yet, expected today) / FAIL.", ""]
    L.append("| URL | st | lang/dir | scope | title (len) | desc | canonical | hreflang | robots | og | LD | h1 | banned | static | chrome | result |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rs:
        if r.get("type") == "text":
            L.append(f"| {r['path']} | {r['status']} | - | - | - | - | - | - | - | - | - | - | - | - | - | {status_of(r)} |")
            continue
        i = r.get("info", {})
        canon = "self" if r.get("canonical", "").rstrip("/") == r["url"].rstrip("/") else ("→" + urlparse(r.get("canonical", "")).path if r.get("canonical") else "MISSING")
        cl = r.get("hreflang") or {}
        L.append("| {p} | {st} | {lg}/{d} | {sc} | {tl} | {dl} | {cn} | {hl} | {rb} | {og} | {ld} | {h1} | {bn} | {bs} | {ch} | {res} |".format(
            p=r["path"], st=r["status"], lg=cell(i, "html_lang"), d=cell(i, "dir"), sc=cell(i, "scope"),
            tl=r.get("title_len", "-"), dl=r.get("desc_len", "-"), cn=canon, hl=len(cl) or "-",
            rb=(r.get("robots") or "-")[:22], og=r.get("og_size", "-" if not r.get("og_image") else "?"),
            ld=r.get("ld_blocks", "-"), h1=r.get("h1", "-"), bn=",".join(r.get("banned", [])) or "-",
            bs=len(r.get("broken_static", [])) or "-", ch="yes" if r.get("chrome") else "NO", res=status_of(r)))
    L += ["", "## Findings", ""]
    def section(title, key):
        rows = [(r, i) for r in rs for i in r[key]]
        if not rows:
            return
        L.append(f"### {title}")
        L.append("")
        grouped: Dict[str, List[Tuple[Dict[str, Any], Dict[str, Any]]]] = {}
        for r, i in rows:
            grouped.setdefault(i["kind"], []).append((r, i))
        for kind, items in sorted(grouped.items()):
            L.append(f"- **{kind}** ({items[0][1]['severity']}, {len(items)}):")
            for r, i in items[:25]:
                L.append(f"  - `{r['path']}` — {i['detail']}")
            if len(items) > 25:
                L.append(f"  - … {len(items) - 25} more (see the JSON)")
        L.append("")
    section("Failures and warnings", "issues")
    section("Pending release (expected today)", "pending")
    return "\n".join(L) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="https://autorig.online")
    ap.add_argument("--out-dir", default=str(ROOT / "docs" / "site-v3-refresh"))
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--only", default="", help="audit only URLs containing this text")
    ap.add_argument("--no-assets", action="store_true", help="skip the /static link check")
    ap.add_argument("--date", default="", help="file date (default: local today)")
    a = ap.parse_args()
    base = a.base.rstrip("/")
    data = run(base, a.workers, a.only, not a.no_assets)
    summary = summarize(data)
    when = a.date or dt.datetime.now().strftime("%Y-%m-%d")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    data["summary"], data["generated"] = summary, stamp
    (out / f"live-audit-{when}.json").write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8", newline="\n")
    (out / f"live-audit-{when}.md").write_text(render_md(data, summary, stamp), encoding="utf-8", newline="\n")
    line = (f"{summary['pages']} pages, {summary['hard_failures']} hard failures ({summary['pages_with_hard_failures']} pages), "
            f"{summary['soft_warnings']} warnings, {summary['pending_pages']} pending; by kind: {summary['by_kind']}")
    print(line)
    print(f"written: {out / ('live-audit-' + when + '.md')}")
    return 1 if summary["hard_failures"] else 0


if __name__ == "__main__":
    sys.exit(main())

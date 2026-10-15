#!/usr/bin/env python3
"""Static checks of one site page for the V3 refresh review (lead's tool, 2026-10-16).

    py -3.11 tools/site_page_check.py static/index.html [--live /]  [--pending home]

Reports: parse errors (unclosed tags via html.parser balance), h1 count, title/description lengths, canonical, og/twitter,
hreflang, JSON-LD validity, images without alt/width/height, local src/href targets that do not exist on disk, data-i18n
keys missing from the shared i18n files plus the page's pending files, leftover V2 wording, and (with --live) a size diff
against the live HTML. Exit 1 when a hard check fails.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
import urllib.request
from html.parser import HTMLParser

ROOT = pathlib.Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
LANGS = ("en", "ru", "zh", "hi", "fa")
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
BAD_WORDS = re.compile(r"\bV2\b|OpenPose|animal rigging V2|Animal V2|coming soon", re.I)


class P(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.errors, self.h1, self.imgs, self.links, self.i18n, self.ld, self.meta, self.title = [], [], 0, [], [], [], [], {}, ""
        self._in_ld = False
        self._in_title = False
        self._ld_buf = ""

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag not in VOID:
            self.stack.append(tag)
        if tag == "h1":
            self.h1 += 1
        if tag == "img":
            self.imgs.append(a)
        if tag in ("a", "link") and a.get("href"):
            self.links.append((tag, a.get("href"), a.get("rel", ""), a.get("hreflang", "")))
        if tag == "img" and a.get("src"):
            self.links.append(("img", a["src"], "", ""))
        if tag == "script" and a.get("type") == "application/ld+json":
            self._in_ld = True
            self._ld_buf = ""
        if tag == "title":
            self._in_title = True
        if tag == "meta":
            k = a.get("name") or a.get("property")
            if k:
                self.meta[k] = a.get("content", "")
        for k in ("data-i18n", "data-i18n-placeholder", "data-i18n-title", "data-i18n-aria-label"):
            if a.get(k):
                self.i18n.append(a[k])

    def handle_endtag(self, tag):
        if tag in VOID:
            return
        if self.stack and self.stack[-1] == tag:
            self.stack.pop()
        elif tag in self.stack:
            while self.stack and self.stack[-1] != tag:
                self.errors.append(f"unclosed <{self.stack.pop()}> before </{tag}> at line {self.getpos()[0]}")
            self.stack.pop()
        else:
            self.errors.append(f"stray </{tag}> at line {self.getpos()[0]}")
        if tag == "script" and self._in_ld:
            self._in_ld = False
            self.ld.append(self._ld_buf)
        if tag == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_ld:
            self._ld_buf += data
        if self._in_title:
            self.title += data


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("page")
    ap.add_argument("--live", default="", help="live path to compare size with, e.g. /how-it-works")
    ap.add_argument("--pending", default="", help="pending i18n page slug(s), comma-separated")
    a = ap.parse_args()
    path = pathlib.Path(a.page)
    text = path.read_text(encoding="utf-8")
    p = P()
    p.feed(text)
    hard = []
    print(f"== {path.name}: {len(text)} bytes, {text.count(chr(10))} lines")
    for e in p.errors[:15]:
        print("  PARSE", e)
    if p.errors:
        hard.append(f"{len(p.errors)} parse problems")
    if p.stack:
        print("  PARSE unclosed at end:", p.stack[-8:])
        hard.append("unclosed tags at end")
    print(f"  h1: {p.h1}" + ("" if p.h1 == 1 else "  <-- must be exactly one"))
    if p.h1 != 1:
        hard.append("h1 count")
    t = p.title.strip()
    print(f"  title ({len(t)}): {t[:90]}")
    d = p.meta.get("description", "")
    print(f"  description ({len(d)}): {d[:100]}")
    if not (20 <= len(t) <= 70):
        print("  WARN title length")
    if not (50 <= len(d) <= 170):
        print("  WARN description length")
    for k in ("og:title", "og:description", "og:image", "twitter:card", "keywords"):
        if not p.meta.get(k):
            print(f"  WARN missing meta {k}")
    canon = [h for tag, h, rel, _ in p.links if tag == "link" and "canonical" in rel]
    print("  canonical:", canon or "MISSING")
    hl = sorted(set(l for tag, _, rel, l in p.links if tag == "link" and "alternate" in rel and l))
    print("  hreflang:", hl or "none")
    for i, block in enumerate(p.ld):
        try:
            doc = json.loads(block)
            print(f"  json-ld[{i}]: {doc.get('@type') if isinstance(doc, dict) else type(doc).__name__}")
        except ValueError as exc:
            print(f"  JSON-LD[{i}] INVALID: {exc}")
            hard.append("json-ld")
    bad_imgs = [im.get("src") for im in p.imgs if not im.get("alt") or not im.get("width") or not im.get("height")]
    if bad_imgs:
        print(f"  WARN {len(bad_imgs)} img without alt/width/height:", bad_imgs[:6])
    missing = []
    for tag, href, _, _ in p.links:
        if href.startswith("/static/"):
            f = ROOT / href.lstrip("/").split("?")[0]
            if not f.exists():
                missing.append(href)
    if missing:
        print(f"  MISSING local targets ({len(missing)}):", missing[:10])
        hard.append("missing local files")
    big = [(im.get("src"), (ROOT / im["src"].lstrip("/").split("?")[0]).stat().st_size) for im in p.imgs
           if im.get("src", "").startswith("/static/") and (ROOT / im["src"].lstrip("/").split("?")[0]).exists()
           and (ROOT / im["src"].lstrip("/").split("?")[0]).stat().st_size > 300_000]
    if big:
        print("  WARN images over 300 KB:", [(s, f"{b // 1024} KB") for s, b in big])
    shared = {l: json.loads((STATIC / "i18n" / f"{l}.json").read_text(encoding="utf-8")) for l in LANGS}
    pend = {l: {} for l in LANGS}
    for slug in [s for s in a.pending.split(",") if s]:
        for l in LANGS:
            f = STATIC / "i18n" / "pending" / f"{slug}.{l}.json"
            if f.exists():
                pend[l].update(json.loads(f.read_text(encoding="utf-8")))
            else:
                print(f"  WARN pending file missing: {f.name}")
    keys = sorted(set(p.i18n))
    lost = {l: [k for k in keys if k not in shared[l] and k not in pend[l]] for l in LANGS}
    print(f"  i18n keys on page: {len(keys)}; missing per language:", {l: len(v) for l, v in lost.items()})
    for l, v in lost.items():
        if v:
            print(f"    {l}: {v[:8]}")
            hard.append(f"i18n {l}")
    bad = BAD_WORDS.findall(re.sub(r"<script.*?</script>", "", text, flags=re.S))
    if bad:
        print("  WARN old wording:", sorted(set(bad))[:8])
    if a.live:
        try:
            live = urllib.request.urlopen("https://autorig.online" + a.live, timeout=20).read().decode("utf-8", "replace")
            print(f"  live {a.live}: {len(live)} bytes (file {len(text)}; the live page carries the injected header/footer)")
        except Exception as exc:  # noqa: BLE001
            print("  live fetch failed:", exc)
    print("  RESULT:", "FAIL " + ", ".join(hard) if hard else "OK")
    return 1 if hard else 0


if __name__ == "__main__":
    sys.exit(main())

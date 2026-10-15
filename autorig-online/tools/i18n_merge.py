#!/usr/bin/env python3
"""Merge the page agents' pending i18n keys into the shared static/i18n/<lang>.json files (site refresh to V3,
2026-10-16). Five agents work at once, so none of them edits the shared files: each writes
static/i18n/pending/<page>.<lang>.json (flat {key: text}); the lead runs this.

    py -3.11 tools/i18n_merge.py            # report what would change
    py -3.11 tools/i18n_merge.py --write    # merge, keep key order, atomic write
    py -3.11 tools/i18n_merge.py --write --drop key1,key2   # also remove keys the agents reported as dropped

Checks: every pending page has all five languages with the same key set; a pending key that overrides an existing
key is reported (allowed: the page's own text changed). Exit 1 on a check failure, nothing written.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

LANGS = ("en", "ru", "zh", "hi", "fa")
ROOT = pathlib.Path(__file__).resolve().parent.parent
I18N = ROOT / "static" / "i18n"
PENDING = I18N / "pending"


def load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def atomic_write(path: pathlib.Path, doc: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--drop", default="", help="comma-separated keys to remove from every language")
    ap.add_argument("--page", default="", help="merge only this page slug")
    args = ap.parse_args()
    pages: dict[str, dict[str, dict]] = {}
    for p in sorted(PENDING.glob("*.json")) if PENDING.is_dir() else []:
        stem = p.name[:-5]
        if "." not in stem:
            print(f"skip {p.name}: expected <page>.<lang>.json")
            continue
        page, lang = stem.rsplit(".", 1)
        if lang not in LANGS:
            print(f"skip {p.name}: unknown language {lang}")
            continue
        if args.page and page != args.page:
            continue
        try:
            doc = load(p)
        except ValueError as exc:
            print(f"FAIL {p.name}: {exc}")
            return 1
        if not isinstance(doc, dict) or any(not isinstance(v, str) for v in doc.values()):
            print(f"FAIL {p.name}: must be a flat object of strings")
            return 1
        pages.setdefault(page, {})[lang] = doc
    ok = True
    for page, by_lang in pages.items():
        missing = [l for l in LANGS if l not in by_lang]
        if missing:
            print(f"FAIL {page}: languages missing {missing}")
            ok = False
            continue
        keys = {l: set(d) for l, d in by_lang.items()}
        base = keys["en"]
        for l in LANGS[1:]:
            if keys[l] != base:
                print(f"FAIL {page}: {l} keys differ from en: only in en {sorted(base - keys[l])[:8]}, only in {l} {sorted(keys[l] - base)[:8]}")
                ok = False
        empty = [k for k, v in by_lang["en"].items() if not v.strip()]
        if empty:
            print(f"FAIL {page}: empty en values {empty[:8]}")
            ok = False
    if not ok:
        return 1
    drop = [k for k in args.drop.split(",") if k.strip()]
    for lang in LANGS:
        path = I18N / f"{lang}.json"
        doc = load(path)
        added = changed = 0
        for page, by_lang in pages.items():
            for k, v in by_lang[lang].items():
                if k not in doc:
                    added += 1
                elif doc[k] != v:
                    changed += 1
                doc[k] = v
        removed = 0
        for k in drop:
            if k in doc:
                del doc[k]
                removed += 1
        print(f"{lang}: +{added} ~{changed} -{removed} (total {len(doc)})")
        if args.write:
            atomic_write(path, doc)
    if args.write:
        print("written")
    return 0


if __name__ == "__main__":
    sys.exit(main())

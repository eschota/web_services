#!/usr/bin/env python3
"""Exposure matrix of one AutoRig task (spec docs/my-gallery-privacy-spec-20261017.md, section 3).

Run it from OUTSIDE the VPS (the owner's PC): every URL through which the task's model can be seen is requested as
anon (no cookie), other (a fresh anon cookie), owner (--owner-cookie) and admin (--admin-cookie, optional), and the
answer is compared with what the state expects:

* public  -> anon / other get what the owner gets (2xx);
* private -> anon / other get 404 (or 401/403/410) on every URL, the listings do not name the task; owner and admin 2xx;
* deleted -> every identity gets 404/410.

Bodies are never stored: files are asked with ``Range: bytes=0-0`` and at most 64 KB of any answer is read, only to
find ids (upload token, MT run) and to check listings for the task id.

    python privacy_exposure.py <task_id> --state private --owner-cookie "anon_id=..." [--ids ids.json] [--out m.json]
"""
from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import urllib.error
import urllib.request
import uuid
from typing import Dict, List, Optional, Tuple

BASE = "https://autorig.online"
RUN_RE = re.compile(r"(?<![0-9a-f])[0-9a-f]{20}(?![0-9a-f])")
UPLOAD_RE = re.compile(r"/u/([0-9a-fA-F-]{36})/([^\"?#\s]+)")
DENY = {401, 403, 404, 410}
CTX = ssl.create_default_context()


def fetch(path: str, cookie: Optional[str], *, full: bool = False) -> Tuple[int, bytes, Dict[str, str]]:
    req = urllib.request.Request(BASE + path, headers={"User-Agent": "autorig-privacy-exposure/1"})
    if cookie:
        req.add_header("Cookie", cookie)
    if not full:
        req.add_header("Range", "bytes=0-0")
    try:
        with urllib.request.urlopen(req, timeout=60, context=CTX) as resp:
            return resp.status, resp.read(65536), {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read(4096)
        except Exception:
            body = b""
        return exc.code, body, {k.lower(): v for k, v in (exc.headers or {}).items()}
    except Exception as exc:
        return -1, str(exc).encode()[:200], {}


def discover(task_id: str, cookie: Optional[str]) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    for path in (f"/api/task/{task_id}", f"/api/task/{task_id}/viewer-settings", f"/api/task/{task_id}/viewer"):
        code, body, _ = fetch(path, cookie, full=True)
        text = body.decode("utf-8", "replace")
        if code != 200:
            continue
        m = UPLOAD_RE.search(text)
        if m and "upload" not in ids:
            ids["upload"] = f"/u/{m.group(1)}/{m.group(2)}"
        for run in RUN_RE.findall(text):
            ids.setdefault("run", run)
    return ids


def url_classes(task_id: str, ids: Dict[str, str]) -> List[Tuple[str, str]]:
    t = task_id
    rows = [
        ("page", f"/task?id={t}"),
        ("page-ru", f"/ru/task?id={t}"),
        ("viewer-page", f"/viewer/{t}"),
        ("api-task", f"/api/task/{t}"),
        ("api-card", f"/api/task/{t}/card"),
        ("api-viewer", f"/api/task/{t}/viewer"),
        ("api-viewer-settings", f"/api/task/{t}/viewer-settings"),
        ("api-progress-log", f"/api/task/{t}/progress_log"),
        ("api-cached-files", f"/api/task/{t}/cached-files"),
        ("api-owner-tasks", f"/api/task/{t}/owner_tasks"),
        ("glb-model", f"/api/task/{t}/model.glb"),
        ("glb-prepared", f"/api/task/{t}/prepared.glb"),
        ("glb-animations", f"/api/task/{t}/animations.glb"),
        ("bundle-url", f"/api/task/{t}/downloads/bundle-url"),
        ("thumb", f"/thumb/{t}"),
        ("api-thumb", f"/api/thumb/{t}"),
        ("og-jpg", f"/og/task/{t}.jpg"),
        ("og-mp4", f"/og/task/{t}.mp4"),
        ("poster-webp(nginx)", f"/pw/{t}.webp"),
        ("api-video", f"/api/video/{t}"),
    ]
    if ids.get("upload"):
        rows.append(("upload-source(nginx)", ids["upload"]))
    run = ids.get("run")
    if run:
        rows += [
            ("mt-run(nginx->mt)", f"/api/mt/runs/{run}"),
            ("mt-rig", f"/api/mt/runs/{run}/rig"),
            ("mt-card", f"/api/mt/runs/{run}/card"),
            ("mt-view", f"/api/mt/view/{run}"),
            ("mt-v3-run", f"/api/mt/v3/runs/{run}"),
            ("mt-analysis", f"/api/mt/analysis/{run}"),
            ("mt-social-player", f"/api/mt/social-player/{run}"),
            ("mt-session-card", f"/api/mt/runs/{run}/session-card"),
        ]
    return rows


LISTINGS = [
    ("gallery-date", "/api/gallery?sort=date&per_page=60"),
    ("gallery-likes", "/api/gallery?sort=likes&per_page=60"),
    ("sitemap-gallery-1", "/sitemap/gallery/part/1.xml"),
    ("sitemap-videos", "/sitemap/videos.xml"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("task_id")
    ap.add_argument("--state", choices=("public", "private", "deleted"), required=True)
    ap.add_argument("--owner-cookie")
    ap.add_argument("--admin-cookie")
    ap.add_argument("--ids", help="JSON file with discovered ids (upload, run); written when missing")
    ap.add_argument("--out")
    args = ap.parse_args()

    ids: Dict[str, str] = {}
    if args.ids:
        try:
            ids = json.load(open(args.ids, encoding="utf-8"))
        except FileNotFoundError:
            ids = {}
    if not ids:
        ids = discover(args.task_id, args.owner_cookie)
        if args.ids:
            json.dump(ids, open(args.ids, "w", encoding="utf-8"), indent=1)

    who = {"anon": None, "other": f"anon_id={uuid.uuid4()}", "owner": args.owner_cookie, "admin": args.admin_cookie}
    rows = []
    for name, path in url_classes(args.task_id, ids):
        got = {}
        for ident, cookie in who.items():
            if ident in ("owner", "admin") and not cookie:
                got[ident] = None
                continue
            code, _, hdr = fetch(path, cookie)
            got[ident] = code
        rows.append({"class": name, "path": path, "got": got})

    # Pass rules per state.
    for row in rows:
        g = row["got"]
        ok = True
        if args.state == "public":
            ref = g.get("owner") if g.get("owner") is not None else g["anon"]
            ok = g["anon"] == ref and g["other"] == ref
        elif args.state == "private":
            ok = g["anon"] in DENY and g["other"] in DENY
            for priv in ("owner", "admin"):
                if g.get(priv) is not None and g[priv] in DENY:
                    row.setdefault("note", []).append(f"{priv} denied")
        else:
            ok = all(v in DENY for v in g.values() if v is not None)
        row["pass"] = ok

    listings = []
    for name, path in LISTINGS:
        code, body, _ = fetch(path, None, full=True)
        present = args.task_id in body.decode("utf-8", "replace")
        expect_absent = args.state != "public"
        listings.append({"class": name, "path": path, "code": code, "present": present,
                         "pass": (not present) if expect_absent else True})

    report = {"task_id": args.task_id, "state": args.state, "ids": ids, "rows": rows, "listings": listings,
              "pass": all(r["pass"] for r in rows) and all(r["pass"] for r in listings)}
    if args.out:
        json.dump(report, open(args.out, "w", encoding="utf-8"), indent=1)
    print(f"task {args.task_id} state={args.state} ids={ids}")
    print(f"{'class':24} {'anon':>5} {'other':>5} {'owner':>5} {'admin':>5}  pass")
    for r in rows:
        g = r["got"]
        print(f"{r['class']:24} {str(g['anon']):>5} {str(g['other']):>5} {str(g['owner']):>5} {str(g['admin']):>5}  "
              f"{'PASS' if r['pass'] else 'FAIL'} {' '.join(r.get('note', []))}")
    for l in listings:
        print(f"{l['class']:24} code={l['code']} present={l['present']}  {'PASS' if l['pass'] else 'FAIL'}")
    print("MATRIX", "PASS" if report["pass"] else "FAIL")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""AutoRig regression autotests and release gate (owner rule 2026-10-10, AGENTS.md «Every Release Passes the
Regression Autotests»: every model that once produced a bug is a case; every release passes the base suite with an
explicit PASS before `current` is repointed, MT code is switched or a converter node is restored).

One manifest (corpus.json), one runner (this file), the checks (checks.py), all in Git
(autorig-online/deploy/autotests), installed on the VPS in /srv/autorig/autotests (outside the release tree).

    sudo /srv/autorig/autotests/gate mt        [--tier base|extended] [--mt-file mt/x.py=/path ...]
    sudo /srv/autorig/autotests/gate backend   --release /srv/autorig/releases/<staged>
    sudo /srv/autorig/autotests/gate live                       # process checks against production
    sudo /srv/autorig/autotests/gate all --tier extended        # nightly
    sudo /srv/autorig/autotests/autotests.py mt-deploy --mt-file mt/x.py=/home/debian/x.py [--restart]
    sudo /srv/autorig/autotests/autotests.py artifact --glb rig.glb [--prepared p.glb] [--profile converter_default]
    sudo /srv/autorig/autotests/autotests.py sync [--write-manifest]   # copy the corpus inputs read-only, sha256
    sudo /srv/autorig/autotests/autotests.py card <report.json> [--post-dev]

Verdicts: PASS, FAIL, XFAIL (a known defect, still there: reason + owner agent), XPASS (a known defect no longer
seen: flip the case to pass), PENDING (its detector has not landed yet), ERROR (the check crashed). Exit 0 only when
nothing is FAIL or ERROR: that is the gate. Reports: /srv/autorig/data/autotests/reports/.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
MANIFEST = HERE / "corpus.json"
DATA = pathlib.Path(os.environ.get("AUTOTESTS_DATA", "/srv/autorig/data/autotests"))
CORPUS = DATA / "corpus"
REPORTS = DATA / "reports"
WORK = DATA / "work"
TREES = DATA / "mt-tree"
MT_PROD = pathlib.Path("/srv/autorig/data/motion_transfer")
PY = "/srv/autorig/venv/bin/python3"
USER = "autorig"
AGENT = "Autotests · V3"
PROJECT = "AutoRig V3"
DEV_SEND = "https://autorig.online/dev/api/send"
PATHS = ("mt", "backend", "converter", "live")
MT_SHARED_LINKS = ("animlib", "models", "texlib", "skeleton_db")      # read-only resources of the MT root
MT_SHARED_COPIES = ("fast_registry.json", "live-stats.json")          # written by the code under test: copies


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _chown(path: pathlib.Path) -> None:
    if os.geteuid() == 0:
        subprocess.run(["chown", "-R", f"{USER}:{USER}", str(path)], check=False)


# ------------------------------------------------------------------------------------------------ corpus
def cmd_sync(args) -> int:
    """Copy every corpus input READ-ONLY (0444) into the corpus dir, verified by sha256. Originals are only read."""
    man = load_manifest()
    CORPUS.mkdir(parents=True, exist_ok=True)
    os.chmod(CORPUS, 0o755)
    changed, bad = False, []
    for name, item in sorted(man["inputs"].items()):
        dest = CORPUS / name
        want = item.get("sha256")
        if dest.is_file() and (not want or sha256(dest) == want):
            have = sha256(dest)
            print(f"ok      {name} {have[:12]}")
        else:
            src = pathlib.Path(item["source"])
            if not src.is_file():
                bad.append(name)
                print(f"MISSING {name} <- {src}")
                continue
            got = sha256(src)
            if want and not item.get("derive") and got != want:
                bad.append(name)
                print(f"CHANGED {name}: source sha {got[:12]} != manifest {want[:12]} (not copied)")
                continue
            fd, tmp = tempfile.mkstemp(dir=CORPUS, prefix=f".{name}.", suffix=pathlib.Path(name).suffix)
            os.close(fd)
            if item.get("derive"):                         # e.g. assimp FBX -> GLB of a source skin; reads src only
                cmd = [a.format(src=str(src), dst=tmp) for a in item["derive"]]
                p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                if p.returncode:
                    os.unlink(tmp)
                    bad.append(name)
                    print(f"DERIVE FAILED {name}: {(p.stderr or p.stdout)[-300:]}")
                    continue
            else:
                shutil.copyfile(src, tmp)                  # reads the original, never writes it
            os.chmod(tmp, 0o444)
            os.replace(tmp, dest)
            have = sha256(dest)
            print(f"copied  {name} {have[:12]} <- {src}")
        if item.get("sha256") != have or item.get("bytes") != dest.stat().st_size:
            item["sha256"], item["bytes"] = have, dest.stat().st_size
            changed = True
    if changed and args.write_manifest:
        tmp = MANIFEST.with_suffix(".tmp")
        tmp.write_text(json.dumps(man, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        os.replace(tmp, MANIFEST)
        print(f"manifest updated: {MANIFEST} (mirror it into Git)")
    elif changed:
        print("manifest lacks sha256/bytes for some inputs: rerun with --write-manifest")
    return 1 if bad else 0


# ------------------------------------------------------------------------------------------------ MT tree under test
def stage_mt_tree(overlay: dict[str, str], label: str) -> tuple[pathlib.Path, dict]:
    """A private MT root: the production mt/ package copied (or a candidate with `overlay` {rel: file} on top),
    the shared read-only resources symlinked, the files the code writes copied. Returns (tree, prod sha256 of the
    overlaid files) so a deploy can refuse when production changed meanwhile."""
    tree = TREES / label
    if tree.exists():
        shutil.rmtree(tree)
    tree.mkdir(parents=True)
    subprocess.run(["rsync", "-a", "--exclude", "__pycache__", f"{MT_PROD}/mt/", f"{tree}/mt/"], check=True)
    base = {}
    for rel, src in overlay.items():
        rel = rel.strip("/")
        if not rel.startswith("mt/") or ".." in rel.split("/"):
            raise SystemExit(f"--mt-file target must be under mt/: {rel}")
        prod = MT_PROD / rel
        base[rel] = sha256(prod) if prod.is_file() else None
        dest = tree / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
    for name in MT_SHARED_LINKS:
        if (MT_PROD / name).exists():
            os.symlink(MT_PROD / name, tree / name)
    for name in MT_SHARED_COPIES:
        if (MT_PROD / name).is_file():
            shutil.copyfile(MT_PROD / name, tree / name)
    _chown(tree)
    return tree, base


# ------------------------------------------------------------------------------------------------ selection
def select(man: dict, path: str, tier: str, only: list[str] | None) -> list[dict]:
    tiers = ("base",) if tier == "base" else ("base", "extended")
    cases = []
    for case in man["cases"]:
        if only and case["id"] not in only:
            continue
        chosen = [c for c in case["checks"] if (path == "all" or path in c.get("paths", ["mt"]))
                  and c.get("tier", "base") in tiers]
        if not chosen:
            continue
        need = {c.get("on") for c in chosen if c.get("on")}
        steps = [c for c in case["checks"] if c["id"] in need and c not in chosen]
        ids = {c["id"] for c in steps + chosen}
        ordered = [c for c in case["checks"] if c["id"] in ids]
        for c in ordered:
            c["_step_only"] = c in steps
        cases.append({**case, "checks": ordered})
    return cases


# ------------------------------------------------------------------------------------------------ verdicts
OPS = {"<=": lambda a, b: a <= b, "<": lambda a, b: a < b, ">=": lambda a, b: a >= b, ">": lambda a, b: a > b,
       "==": lambda a, b: a == b, "!=": lambda a, b: a != b, "in": lambda a, b: a in b,
       "between": lambda a, b: b[0] <= a <= b[1], "outside": lambda a, b: not (b[0] <= a <= b[1])}


def judge(check: dict, row: dict) -> dict:
    expect = "xfail" if check.get("xfail") else "pass"
    out = {"case": row["case"], "check": check["id"], "kind": check["kind"], "role": check.get("role", "regression"),
           "paths": check.get("paths", ["mt"]), "tier": check.get("tier", "base"), "expect": expect,
           "wall_s": row.get("wall_s"), "metrics": row.get("metrics"), "failed": [], "step_only": check.get("_step_only")}
    if check.get("xfail"):
        out["xfail"] = check["xfail"]
    if "error" in row:
        out.update(status="ERROR", error=row["error"], trace=row.get("trace"))
        return out
    m = row.get("metrics") or {}
    if check["kind"] == "pending_detector" or m.get("pending"):
        out.update(status="PENDING", pending=check.get("pending") or {})
        return out
    for metric, (op, value) in (check.get("thresholds") or {}).items():
        if value == "CAL":                                 # not calibrated yet: measured, not judged
            out.setdefault("uncalibrated", {})[metric] = m.get(metric)
            continue
        have = m.get(metric)
        ok = have is not None and OPS[op](have, value)
        if not ok:
            out["failed"].append(f"{metric}={have!r} (want {op} {value!r})")
    passed = not out["failed"]
    out["status"] = ("XPASS" if passed else "XFAIL") if expect == "xfail" else ("PASS" if passed else "FAIL")
    return out


def blocking(r: dict) -> bool:
    return r["status"] in ("FAIL", "ERROR") and not r.get("step_only_ok")


# ------------------------------------------------------------------------------------------------ run
def run_case(case: dict, tree: pathlib.Path, release: pathlib.Path, work: pathlib.Path, man: dict) -> list[dict]:
    doc = {"id": case["id"], "checks": [{k: v for k, v in c.items() if not k.startswith("_")} for c in case["checks"]],
           "template_fingerprints": man.get("template_fingerprints") or []}
    cw = work / case["id"]
    cw.mkdir(parents=True, exist_ok=True)
    _chown(cw)
    env = [f"MT_TREE={tree}", f"AUTOTESTS_RELEASE={release}", f"AUTOTESTS_CORPUS={CORPUS}", f"AUTOTESTS_PY={PY}",
           "OMP_NUM_THREADS=1", "OPENBLAS_NUM_THREADS=1", "MKL_NUM_THREADS=1", "PATH=/usr/local/bin:/usr/bin:/bin",
           "HOME=/tmp", "LANG=C.UTF-8"]
    cmd = ["nice", "-n", "10", PY, "-P", str(HERE / "checks.py"), "--case", json.dumps(doc), "--work", str(cw)]
    if os.geteuid() == 0:
        cmd = ["sudo", "-n", "-u", USER, "env", *env, *cmd]
    else:
        cmd = ["env", *env, *cmd]
    limit = float(case.get("timeout_s", 1200))
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=limit, cwd=str(cw))
        rows = [json.loads(l) for l in p.stdout.splitlines() if l.startswith("{")]
        err = (p.stderr or "")[-800:] if p.returncode else ""
    except subprocess.TimeoutExpired:
        rows, err = [], f"case timed out after {limit:.0f} s"
    got = {r["check"]: r for r in rows}
    out = []
    for c in case["checks"]:
        row = got.get(c["id"]) or {"case": case["id"], "check": c["id"], "error": err or "no result (case process died)"}
        out.append(judge(c, row))
        if c.get("target"):                  # the goal of a known defect, beside its no-regression thresholds
            t = c["target"]
            goal = {**c, "id": c["id"] + "/target", "thresholds": t["thresholds"],
                    "xfail": None if t.get("expect") == "pass" else {k: t.get(k) for k in ("reason", "owner")}}
            out.append(judge(goal, {**row, "check": goal["id"]}))
    return out


def run_suite(path: str, tier: str, *, overlay=None, release=None, only=None, jobs=6, label=None) -> dict:
    man = load_manifest()
    t0 = time.time()
    stamp = now()
    label = label or f"{stamp}-{path}-{tier}"
    release = pathlib.Path(release or "/srv/autorig/current").resolve()
    cases = select(man, path, tier, only)
    no_tree = {"backend_unittests", "task_page_js", "classic_mirror_live", "rig_budget_live", "pending_detector"}
    needs_mt = any(c["kind"] not in no_tree for case in cases for c in case["checks"])
    tree, base = (stage_mt_tree(overlay or {}, label) if needs_mt else (MT_PROD, {}))
    work = WORK / label
    work.mkdir(parents=True, exist_ok=True)
    _chown(work)
    results = []
    order = sorted(cases, key=lambda c: -float(c.get("cost", 1)))           # the slow cases start first
    with cf.ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        futs = [pool.submit(run_case, c, tree, release, work, man) for c in order]
        for f in cf.as_completed(futs):
            results.extend(f.result())
    rank = {c["id"]: i for i, c in enumerate(man["cases"])}
    results.sort(key=lambda r: (rank.get(r["case"], 999), r["check"]))
    for r in results:                         # a dependency step pulled in only for another path is not judged
        if r.get("step_only") and r["status"] in ("FAIL", "XFAIL", "XPASS"):
            r["step_only_ok"] = True
    counts = {}
    for r in results:
        if r.get("step_only") and r["status"] != "ERROR":
            continue
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    gate = "PASS" if not any(blocking(r) for r in results) else "FAIL"
    report = {"schema": "autorig.autotests.report/1", "label": label, "path": path, "tier": tier,
              "gate": gate, "wall_s": round(time.time() - t0, 1), "counts": counts,
              "blocking": [f"{r['case']}/{r['check']}: {r['status']} {'; '.join(r['failed']) or r.get('error', '')}"
                           for r in results if blocking(r)],
              "mt_tree": str(tree), "mt_overlay": {k: sha256(v)[:16] for k, v in (overlay or {}).items()},
              "mt_overlay_base": base, "release": str(release), "manifest_sha256": sha256(MANIFEST)[:16],
              "at": stamp, "results": results}
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / f"{label}.json"
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, out)
    latest = REPORTS / f"latest-{path}.json"
    shutil.copyfile(out, latest.with_suffix(".tmp"))
    os.replace(latest.with_suffix(".tmp"), latest)
    report["report_path"] = str(out)
    if tree != MT_PROD and not overlay:
        shutil.rmtree(tree, ignore_errors=True)
    _prune()
    return report


def _prune(keep=12):
    for d in (WORK, TREES):
        if d.is_dir():
            items = sorted(d.iterdir(), key=lambda p: p.stat().st_mtime)
            for old in items[:-keep]:
                shutil.rmtree(old, ignore_errors=True)


def print_report(rep: dict) -> None:
    print(f"\n== autotests {rep['path']}/{rep['tier']}: GATE {rep['gate']}  ({rep['wall_s']} s, {rep['counts']})")
    for r in rep["results"]:
        if r.get("step_only") and r["status"] != "ERROR":
            continue
        why = "; ".join(r["failed"]) or r.get("error", "")
        if r["status"] in ("XFAIL", "XPASS", "PENDING"):
            x = r.get("xfail") or r.get("pending") or {}
            why = (why + " | " if why else "") + f"{x.get('reason', '')} [owner: {x.get('owner', '?')}]"
        print(f"  {r['status']:7s} {r['case']:28s} {r['check']:28s} {r.get('wall_s') or 0:7.1f}s  {why[:230]}")
    for b in rep["blocking"]:
        print(f"  BLOCKS RELEASE: {b}")
    print(f"report: {rep.get('report_path')}")


# ------------------------------------------------------------------------------------------------ card + DEV
def card(rep: dict, out: pathlib.Path) -> pathlib.Path:
    """One picture: cases x checks with the verdict colour, the gate and the wall time."""
    from PIL import Image, ImageDraw, ImageFont
    colors = {"PASS": (46, 160, 67), "FAIL": (207, 34, 46), "ERROR": (150, 20, 120), "XFAIL": (210, 150, 20),
              "XPASS": (40, 120, 220), "PENDING": (130, 130, 130)}
    rows = [r for r in rep["results"] if not (r.get("step_only") and r["status"] != "ERROR")]
    cases = []
    for r in rows:
        if r["case"] not in cases:
            cases.append(r["case"])
    by = {c: [r for r in rows if r["case"] == c] for c in cases}
    width_cols = max(len(v) for v in by.values()) if by else 1
    try:
        f = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 15)
        fb = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22)
        fs = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
    except OSError:
        f = fb = fs = ImageFont.load_default()
    cw, ch, left = 196, 46, 300
    W = left + cw * width_cols + 20
    H = 110 + ch * len(cases) + 70
    img = Image.new("RGB", (W, H), (250, 250, 250))
    d = ImageDraw.Draw(img)
    g = rep["gate"]
    d.text((16, 14), f"AutoRig autotests · {rep['path']}/{rep['tier']} · GATE {g}", fill=colors["PASS" if g == "PASS" else "FAIL"], font=fb)
    d.text((16, 48), f"{len(rows)} checks in {len(cases)} cases · {rep['wall_s']} s wall · {rep['at']} · "
                     + " ".join(f"{k} {v}" for k, v in sorted(rep["counts"].items())), fill=(60, 60, 60), font=f)
    y = 90
    for c in cases:
        d.text((16, y + 14), c[:34], fill=(20, 20, 20), font=f)
        for i, r in enumerate(by[c]):
            x = left + i * cw
            d.rectangle([x, y + 4, x + cw - 8, y + ch - 4], fill=colors.get(r["status"], (90, 90, 90)))
            d.text((x + 8, y + 7), f"{r['status']}  {r.get('wall_s') or 0:.0f}s", fill=(255, 255, 255), font=f)
            d.text((x + 8, y + 25), r["check"][:28], fill=(255, 255, 255), font=fs)
        y += ch
    x = 16
    for k in ("PASS", "FAIL", "XFAIL", "XPASS", "PENDING", "ERROR"):
        d.rectangle([x, y + 22, x + 16, y + 38], fill=colors[k])
        d.text((x + 22, y + 22), k, fill=(40, 40, 40), font=f)
        x += 120
    img.save(out)
    return out


def post_dev(png: pathlib.Path, caption: str) -> str:
    p = subprocess.run(["curl", "-s", "-m", "60", "-F", f"agent={AGENT}", "-F", f"project={PROJECT}",
                        "-F", f"caption={caption[:1000]}", "-F", f"file=@{png}", DEV_SEND],
                       capture_output=True, text=True)
    return p.stdout.strip()[:400]


def cmd_card(args) -> int:
    rep = json.loads(pathlib.Path(args.report).read_text(encoding="utf-8"))
    png = card(rep, pathlib.Path(args.report).with_suffix(".png"))
    print(png)
    if args.post_dev:
        print(post_dev(png, args.caption or f"Autotests {rep['path']}/{rep['tier']}: GATE {rep['gate']}, "
                                              f"{rep['wall_s']} s, {rep['counts']}"))
    return 0


# ------------------------------------------------------------------------------------------------ commands
def parse_overlay(items) -> dict:
    out = {}
    for item in items or []:
        rel, _, src = item.partition("=")
        if not src or not pathlib.Path(src).is_file():
            raise SystemExit(f"--mt-file wants mt/<rel>=<existing file>: {item}")
        out[rel.strip()] = str(pathlib.Path(src).resolve())
    return out


def cmd_gate(args) -> int:
    rep = run_suite(args.path, args.tier, overlay=parse_overlay(args.mt_file), release=args.release,
                    only=args.case, jobs=args.jobs)
    print_report(rep)
    if args.post_dev_on_fail and rep["gate"] != "PASS":
        args.post_dev = True
        args.caption = args.caption or ("Autotests nightly FAIL: " + "; ".join(rep["blocking"])[:600])
    if args.card or args.post_dev:
        png = card(rep, pathlib.Path(rep["report_path"]).with_suffix(".png"))
        print(f"card: {png}")
        if args.post_dev:
            print(post_dev(png, args.caption or f"Autotests {rep['path']}/{rep['tier']}: GATE {rep['gate']} · "
                                                  f"{rep['wall_s']} s · {rep['counts']}"))
    if args.json:
        print(json.dumps({k: rep[k] for k in ("gate", "wall_s", "counts", "blocking", "report_path")}))
    return 0 if rep["gate"] == "PASS" else 1


def mt_busy() -> list[str]:
    """What an autorig-mt restart would kill (the same test as Astra's priv mt-gate, plus V3 dispatch runs)."""
    busy = []
    for rj in (MT_PROD / "runs").glob("*/run.json"):
        try:
            st = json.loads(rj.read_text())
        except (OSError, ValueError):
            continue
        if st.get("status") in ("running", "queued") and time.time() - rj.stat().st_mtime < 6 * 3600:
            busy.append(f"run {rj.parent.name} {st.get('status')}")
    for bj in (MT_PROD / "runs").glob("*/branches/*/branch.json"):
        try:
            st = json.loads(bj.read_text())
        except (OSError, ValueError):
            continue
        if st.get("status") == "running" and time.time() - bj.stat().st_mtime < 6 * 3600:
            busy.append(f"branch {bj.parent.parent.parent.name}/{bj.parent.name}")
    try:
        con = sqlite3.connect(f"file:{MT_PROD / 'v3-dispatch' / 'dispatch.sqlite3'}?mode=ro", uri=True, timeout=5)
        for run_id, stage in con.execute("select run_id, stage from v3_runs where status in ('queued','running')"):
            busy.append(f"v3 {run_id} {stage}")
    except sqlite3.Error as exc:
        busy.append(f"v3 dispatch unreadable: {exc}")
    return busy


def cmd_mt_deploy(args) -> int:
    """Gate, then install: the candidate MT files are staged over a copy of production MT, the base suite runs on
    that tree, and only a PASS installs them (backup in mt.prev/, atomic replace, import check)."""
    if os.geteuid() != 0:
        raise SystemExit("run with sudo")
    overlay = parse_overlay(args.mt_file)
    if not overlay:
        raise SystemExit("nothing to deploy: --mt-file mt/<rel>=<file>")
    label = f"{now()}-mtdeploy"
    rep = run_suite("mt", args.tier, overlay=overlay, jobs=args.jobs, label=label)
    print_report(rep)
    if rep["gate"] != "PASS":
        print("NOT INSTALLED: the gate failed")
        return 1
    for rel, base in rep["mt_overlay_base"].items():
        cur = sha256(MT_PROD / rel) if (MT_PROD / rel).is_file() else None
        if cur != base:
            print(f"NOT INSTALLED: production {rel} changed while the suite ran ({str(base)[:12]} -> {str(cur)[:12]}); "
                  f"re-read it, re-apply your patch, run again")
            return 1
    stamp = now()
    prev = MT_PROD / "mt.prev"
    for rel, src in overlay.items():
        dest = MT_PROD / rel
        if dest.is_file():
            shutil.copy2(dest, prev / f"{dest.name}.{stamp}-autotests")
        fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
        os.close(fd)
        shutil.copyfile(src, tmp)
        os.chmod(tmp, 0o644)
        shutil.chown(tmp, USER, USER)
        os.replace(tmp, dest)
        print(f"installed {rel} {sha256(dest)[:12]} (previous: mt.prev/{dest.name}.{stamp}-autotests)")
    p = subprocess.run(["sudo", "-n", "-u", USER, PY, "-P", "-c", "import sys; sys.path.insert(0, '.'); import mt.service"],
                       cwd=str(MT_PROD), capture_output=True, text=True, timeout=180)
    if p.returncode:
        print("IMPORT FAILED after install, rolling back:\n" + p.stderr[-1500:])
        for rel in overlay:
            dest = MT_PROD / rel
            back = prev / f"{dest.name}.{stamp}-autotests"
            if back.is_file():
                fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.")
                os.close(fd)
                shutil.copyfile(back, tmp)
                shutil.chown(tmp, USER, USER)
                os.chmod(tmp, 0o644)
                os.replace(tmp, dest)
        return 1
    print("import mt.service: ok")
    if args.restart:
        busy = mt_busy()
        if busy and not args.force_restart:
            print("installed, NOT restarted: autorig-mt is busy (a restart kills these):\n  " + "\n  ".join(busy[:20]))
            print("restart later (batched) with: sudo systemctl restart autorig-mt")
            return 0
        subprocess.run(["systemctl", "restart", "autorig-mt.service"], check=False)
        time.sleep(4)
        st = subprocess.run(["systemctl", "is-active", "autorig-mt.service"], capture_output=True, text=True).stdout.strip()
        print(f"autorig-mt restarted: {st}" + (f" (killed: {busy})" if busy else ""))
    return 0


def cmd_artifact(args) -> int:
    """The converter-output checks on one rigged GLB (rig_canary.py calls this for every canary it runs)."""
    man = load_manifest()
    prof = man["profiles"][args.profile]
    files = {}
    tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="artifact-", dir=str(WORK) if os.access(WORK, os.W_OK) else None))
    os.chmod(tmpdir, 0o755)
    for key, src in (("glb", args.glb), ("prepared", args.prepared)):
        if src:
            dst = tmpdir / f"{key}.glb"
            shutil.copyfile(src, dst)
            files[key] = dst.name
    checks = []
    for c in prof["checks"]:
        if c["kind"] == "rest_drift" and "prepared" not in files:
            continue
        c = json.loads(json.dumps(c))
        c["input"] = files["glb"]
        if c["kind"] == "rest_drift":
            c["reference"] = files["prepared"]
        if args.words and c["kind"] == "prop_rigidity":
            c.setdefault("params", {})["words"] = args.words
        if c["kind"] == "prop_rigidity" and not (c.get("params") or {}).get("words"):
            continue
        for k, v in ((args.xfail or {}).get(c["id"]) or {}).items():
            c[k] = v
        checks.append(c)
    _chown(tmpdir)
    global CORPUS
    CORPUS = tmpdir
    case = {"id": args.case_id or "artifact", "checks": checks, "timeout_s": 600}
    rows = run_case(case, MT_PROD, pathlib.Path("/srv/autorig/current"), tmpdir / "work", man)
    gate = "PASS" if not any(blocking(r) for r in rows) else "FAIL"
    print(json.dumps({"gate": gate, "results": [{k: r.get(k) for k in ("check", "status", "failed", "metrics", "error")}
                                                for r in rows]}, ensure_ascii=False, default=str))
    shutil.rmtree(tmpdir, ignore_errors=True)
    return 0 if gate == "PASS" else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sync")
    s.add_argument("--write-manifest", action="store_true")
    g = sub.add_parser("gate")
    g.add_argument("path", choices=PATHS + ("all",))
    g.add_argument("--tier", choices=("base", "extended"), default="base")
    g.add_argument("--mt-file", action="append", help="mt/<rel>=<candidate file> (repeatable)")
    g.add_argument("--release", help="backend release dir under test (default /srv/autorig/current)")
    g.add_argument("--case", action="append")
    g.add_argument("--jobs", type=int, default=6)
    g.add_argument("--card", action="store_true")
    g.add_argument("--post-dev", action="store_true")
    g.add_argument("--caption")
    g.add_argument("--post-dev-on-fail", action="store_true", help="post the card to DEV only when the gate fails")
    g.add_argument("--json", action="store_true")
    d = sub.add_parser("mt-deploy")
    d.add_argument("--mt-file", action="append", required=True)
    d.add_argument("--tier", choices=("base", "extended"), default="base")
    d.add_argument("--jobs", type=int, default=6)
    d.add_argument("--restart", action="store_true")
    d.add_argument("--force-restart", action="store_true")
    a = sub.add_parser("artifact")
    a.add_argument("--glb", required=True)
    a.add_argument("--prepared")
    a.add_argument("--profile", default="converter_default")
    a.add_argument("--words", default="")
    a.add_argument("--case-id")
    a.add_argument("--xfail", type=json.loads, help='{"<check id>": {"xfail": {...}}} overrides')
    c = sub.add_parser("card")
    c.add_argument("report")
    c.add_argument("--post-dev", action="store_true")
    c.add_argument("--caption")
    args = ap.parse_args()
    return {"sync": cmd_sync, "gate": cmd_gate, "mt-deploy": cmd_mt_deploy, "artifact": cmd_artifact,
            "card": cmd_card}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())

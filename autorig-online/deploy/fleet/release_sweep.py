#!/usr/bin/env python3
"""Release and copy sweep (owner 2026-10-12: «на сервере не должно быть копий и релизов, их надо чистить при переходе
на новые версии»). Keeps the ACTIVE release + ONE previous for rollback, deletes the rest. Never touches data/ customer
files (sources, outputs, runs). Everything removed is logged to /var/log/autorig-release-sweep.log.

    release_sweep.py [releases|unity|mtprev|audits|all] [--dry-run]

  releases  /srv/autorig/releases/*: keep `current`, the newest other release (the rollback), any release a running
            process uses (cwd / exe / cmdline), any other symlink points at, and anything touched in the last 2 h
            (a staged release not yet repointed).
  unity     motion_transfer/unity/*: keep the build `test` points to, the previous `test` target (from
            .test-history.log), directories that are not builds (no index.html), and anything touched in 6 h.
  mtprev    motion_transfer/mt.prev/*: the last 2 backups per file.
  audits    audits/*/stage*: stage dirs older than 24 h.
Called by live_static.py promote after `current` moves, and by the cleanup timer.
"""
import os
import pathlib
import re
import shutil
import sys
import time

ROOT = pathlib.Path("/srv/autorig")
RELEASES = ROOT / "releases"
CURRENT = ROOT / "current"
MT = ROOT / "data" / "motion_transfer"
LOG = "/var/log/autorig-release-sweep.log"
DRY = "--dry-run" in sys.argv


def log(msg):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
    print(line, flush=True)
    try:
        with open(LOG, "a") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def newest(p: pathlib.Path) -> float:
    """Newest mtime/ctime of the directory itself and its first level (cheap; a release is hardlinked trees).
    ctime counts too: `cp -a` keeps the source's mtimes, so a release staged a minute ago has an old mtime
    (2026-10-13: the sweep deleted a freshly staged release, then `current` was repointed at it)."""
    try:
        st = p.lstat()
        m = max(st.st_mtime, st.st_ctime)
        for c in p.iterdir():
            cs = c.lstat()
            m = max(m, cs.st_mtime, cs.st_ctime)
        return m
    except OSError:
        return time.time()


def size_of(p: pathlib.Path) -> int:
    """Bytes this directory exclusively holds: files with a single link (hardlinked files free nothing)."""
    total = 0
    for root, _d, files in os.walk(p):
        for n in files:
            try:
                st = os.lstat(os.path.join(root, n))
                if st.st_nlink == 1:
                    total += st.st_size
            except OSError:
                pass
    return total


def remove(p: pathlib.Path, why: str) -> int:
    sz = size_of(p) if p.is_dir() and not p.is_symlink() else 0
    log(f"remove {p} ({why}) frees ~{sz / 2 ** 20:.0f} MB")
    if not DRY:
        if p.is_symlink() or p.is_file():
            p.unlink(missing_ok=True)
        else:
            shutil.rmtree(p, ignore_errors=True)
    return sz


def in_use_paths(prefix: str) -> set:
    used = set()
    pat = re.compile(re.escape(prefix) + r"/([^/\s\"']+)")
    for pid in filter(str.isdigit, os.listdir("/proc")):
        for what in ("cwd", "exe"):
            try:
                m = pat.match(os.readlink(f"/proc/{pid}/{what}"))
                if m:
                    used.add(m.group(1))
            except OSError:
                pass
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                for m in pat.finditer(fh.read().replace(b"\0", b" ").decode("utf-8", "replace")):
                    used.add(m.group(1))
        except OSError:
            pass
    return used


def sweep_releases() -> int:
    if not RELEASES.is_dir() or not CURRENT.exists():
        return 0
    cur = pathlib.Path(os.path.realpath(CURRENT))
    keep = {cur.name} | {x for x in os.environ.get("SWEEP_KEEP", "").split(",") if x}
    others = [d for d in RELEASES.iterdir() if d.is_dir() and not d.is_symlink() and d.name not in keep]
    others.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    if others:
        keep.add(others[0].name)                                   # the rollback
    keep |= in_use_paths(str(RELEASES))
    for link_dir in (ROOT, ROOT / "live", ROOT / "tools", ROOT / "fleet", ROOT / "data"):
        try:
            for l in link_dir.iterdir():
                if l.is_symlink():
                    t = os.readlink(l)
                    m = re.match(re.escape(str(RELEASES)) + r"/([^/]+)", t)
                    if m:
                        keep.add(m.group(1))
        except OSError:
            pass
    freed = 0
    for d in others:
        if d.name in keep or time.time() - newest(d) < 2 * 3600:
            continue
        live = {os.path.realpath(x) for x in (CURRENT, ROOT / "current.new") if os.path.lexists(x)}
        if str(d) in live or os.path.realpath(d) in live:            # repointed meanwhile
            continue
        freed += remove(d, "old release")
    log(f"releases: kept {len(keep)} ({cur.name} + rollback + in use), freed ~{freed / 2 ** 30:.2f} GB")
    return freed


def sweep_unity() -> int:
    u = MT / "unity"
    if not u.is_dir():
        return 0
    keep = set()
    t = u / "test"
    if t.is_symlink():
        keep.add(os.path.basename(os.readlink(t)))
    hist = u / ".test-history.log"
    if hist.is_file():
        tos = re.findall(r"-> (\S+) ", hist.read_text(errors="replace"))
        seen = []
        for p in reversed(tos):
            n = os.path.basename(p)
            if n not in seen:
                seen.append(n)
        keep |= set(seen[:2])                                     # the current target and the rollback line
    freed = 0
    for d in u.iterdir():
        if not d.is_dir() or d.is_symlink() or d.name in keep:
            continue
        if not (d / "index.html").is_file():
            continue                                                # not a build (scene-media ...)
        if time.time() - newest(d) < 6 * 3600:
            continue
        freed += remove(d, "unity build not referenced by test / its rollback")
    log(f"unity: kept {sorted(keep)}, freed ~{freed / 2 ** 20:.0f} MB")
    return freed


def sweep_mtprev() -> int:
    p = MT / "mt.prev"
    if not p.is_dir():
        return 0
    groups = {}
    for f in p.iterdir():
        if f.name == "__pycache__":
            continue
        m = re.match(r"(.+?\.py)", f.name) or re.match(r"([^.]+)", f.name)
        groups.setdefault(m.group(1), []).append(f)
    freed = 0
    for base, files in groups.items():
        files.sort(key=lambda f: f.lstat().st_mtime, reverse=True)
        for f in files[2:]:
            freed += f.lstat().st_size if f.is_file() else 0
            remove(f, f"mt.prev backup of {base}, older than the last 2")
    return freed


def sweep_audits() -> int:
    a = ROOT / "audits"
    freed = 0
    if a.is_dir():
        for d in a.glob("*/stage*"):
            if d.is_dir() and not d.is_symlink() and time.time() - newest(d) > 24 * 3600:
                freed += remove(d, "audit stage dir")
    return freed


def main():
    what = next((a for a in sys.argv[1:] if not a.startswith("-")), "all")
    fns = {"releases": sweep_releases, "unity": sweep_unity, "mtprev": sweep_mtprev, "audits": sweep_audits}
    total = 0
    for name, fn in fns.items():
        if what in ("all", name):
            total += fn()
    log(f"sweep {what}: ~{total / 2 ** 30:.2f} GB {'would be ' if DRY else ''}freed")


if __name__ == "__main__":
    main()

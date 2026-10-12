#!/usr/bin/env python3
"""VPS regenerable-data cleanup (owner 2026-10-12: the disk fell under the reserve and ALL new tasks paused for 10 h).

Runs from autorig-vps-cleanup.timer (root, nice 19, idle IO), every 30 minutes. Never deletes a customer source or
output. It only
  1. hardlinks byte-identical big files inside one MT run (rig/skin/v*, rig/joint_judge/**, texture/**): the content
     stays, the second copy's blocks are freed. A run's active rig/rigged.glb is only ever the KEEPER, never replaced;
  2. removes autotests work dirs older than 3 h (nothing running);
  3. removes agent scratch in /tmp older than 6 h (names bench*, work*, scratch*, autorig-*, *.tar*, *.zip, pc);
  4. removes farm job results older than 2 days that the census already ingested.
Everything is logged to /var/log/autorig-vps-cleanup.log with the freed bytes.

    vps_pressure_cleanup.py [--dry-run]
"""
import hashlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time

RUNS = pathlib.Path("/srv/autorig/data/motion_transfer/runs")
AT_WORK = pathlib.Path("/srv/autorig/data/autotests/work")
FARM_RESULTS = pathlib.Path("/srv/autorig/data/farmjobs/results")
TMP = pathlib.Path("/tmp")
LOG = "/var/log/autorig-vps-cleanup.log"
MIN_BYTES = 4 * 2 ** 20
DRY = "--dry-run" in sys.argv
SCRATCH = re.compile(r"(bench|work|scratch|^autorig-|\.tar|\.zip$|^pc$)", re.I)
SKIP_TMP = re.compile(r"^(systemd-private|ssh-|tmux|\.|snap)")


def log(msg: str):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
    print(line, flush=True)
    try:
        with open(LOG, "a") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def free_gb() -> float:
    u = shutil.disk_usage("/srv/autorig")
    return u.free / 2 ** 30


def sha(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(4 * 2 ** 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def newest_mtime(p: pathlib.Path) -> float:
    if p.is_file() or p.is_symlink():
        return p.lstat().st_mtime
    m = p.lstat().st_mtime
    for root, dirs, files in os.walk(p):
        for n in files + dirs:
            try:
                m = max(m, os.lstat(os.path.join(root, n)).st_mtime)
            except OSError:
                pass
    return m


def dedupe_run(rd: pathlib.Path) -> int:
    members = []                                    # (path, may_be_replaced)
    for pat, replace in (("rig/skin/v*/**/*", True), ("rig/joint_judge/**/*", True), ("texture/**/*", True),
                         ("rig/rigged.glb", False)):
        for f in rd.glob(pat):
            try:
                if f.is_file() and not f.is_symlink() and f.stat().st_size >= MIN_BYTES:
                    members.append((f, replace))
            except OSError:
                pass
    by_size = {}
    for f, rep in members:
        by_size.setdefault(f.stat().st_size, []).append((f, rep))
    freed = 0
    for size, group in by_size.items():
        if len(group) < 2:
            continue
        keepers = {}                                # sha -> keeper path
        group.sort(key=lambda x: (x[1], str(x[0])))  # non-replaceable first: they become keepers
        for f, rep in group:
            try:
                h = sha(f)
                k = keepers.get(h)
                if k is None:
                    keepers[h] = f
                    continue
                if not rep or os.stat(f).st_ino == os.stat(k).st_ino:
                    continue
                if DRY:
                    freed += size
                    continue
                tmp = f.with_name(f".dedupe.{os.getpid()}.tmp")
                os.link(k, tmp)
                os.replace(tmp, f)
                freed += size
            except OSError as exc:
                log(f"dedupe skip {f}: {exc}")
    return freed


def main():
    before = free_gb()
    log(f"start free={before:.1f} GB dry={DRY}")
    total = 0
    # 1. hardlink dedupe per run, newest first
    if RUNS.is_dir():
        for rd in sorted(RUNS.iterdir(), key=lambda p: -p.stat().st_mtime):
            if rd.is_dir():
                got = dedupe_run(rd)
                if got:
                    log(f"dedupe {rd.name}: {got / 2 ** 20:.0f} MB")
                total += got
    # 2. autotests work dirs older than 3 h, only when no autotests process runs
    busy = subprocess.run(["pgrep", "-f", "autotests.py"], capture_output=True).returncode == 0
    if AT_WORK.is_dir() and not busy:
        for d in AT_WORK.iterdir():
            if time.time() - newest_mtime(d) > 3 * 3600:
                size = sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) if d.is_dir() else d.stat().st_size
                log(f"autotests work {d.name}: {size / 2 ** 20:.0f} MB")
                if not DRY:
                    shutil.rmtree(d, ignore_errors=True) if d.is_dir() else d.unlink(missing_ok=True)
                total += size
    # 3. agent scratch in /tmp older than 6 h
    for d in TMP.iterdir():
        try:
            if SKIP_TMP.match(d.name) or not SCRATCH.search(d.name) or d.is_symlink() or (d.is_file() and not re.search(r".(tar[^.]*|zip|tgz)$", d.name)):
                continue
            if time.time() - newest_mtime(d) < 6 * 3600:
                continue
            size = sum(f.stat().st_size for f in d.rglob("*") if f.is_file()) if d.is_dir() else d.stat().st_size
            log(f"tmp {d.name}: {size / 2 ** 20:.0f} MB")
            if not DRY:
                shutil.rmtree(d, ignore_errors=True) if d.is_dir() else d.unlink(missing_ok=True)
            total += size
        except OSError:
            pass
    # 4. farm job results older than 2 days (the census / autotex already took what they need)
    if FARM_RESULTS.is_dir():
        for f in FARM_RESULTS.glob("*.zip"):
            if time.time() - f.stat().st_mtime > 2 * 86400:
                total += f.stat().st_size
                if not DRY:
                    f.unlink(missing_ok=True)
    log(f"done: {total / 2 ** 30:.2f} GB {'would be ' if DRY else ''}freed, free now {free_gb():.1f} GB (was {before:.1f})")


if __name__ == "__main__":
    main()

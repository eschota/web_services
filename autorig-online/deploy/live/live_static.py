#!/usr/bin/env python3
"""Live static edits for autorig.online (Task page · V3, 2026-10-10).

The owner's First Commandment: agents write straight to production and every new
task page load runs the newest code.  nginx serves /static/ from the overlay
/srv/autorig/live/static first, then from the current release, and the backend
reads the task page templates and partials in the same order.  JS/CSS without
an exact content stamp are served `no-cache`, so browsers revalidate them on
every load.

    sudo python3 /srv/autorig/tools/live_static.py put  <rel> <file>   # e.g. js/task-v3-shell.js /tmp/x.js
    sudo python3 /srv/autorig/tools/live_static.py rm   <rel>
    sudo python3 /srv/autorig/tools/live_static.py ls
    sudo python3 /srv/autorig/tools/live_static.py promote [--name <release>]
    sudo python3 /srv/autorig/tools/live_static.py gc

put writes the overlay atomically (temp file + rename in the same directory):
the next request sees the whole new file, never half of it.  It then promotes:
under one lock it stages a release beside `current` (`cp -al`, hardlinks), puts
every overlay file in with a fresh inode (a file shared by hardlink with an old
release is never modified in place), repoints `current` atomically and drops the
overlay copies that `current` now carries.  `--no-promote` keeps the change in
the overlay only.  Rolling back is repointing `current` at the release printed
as `previous`.  <rel> is a path under autorig-online/static.  Backend code is not
live-editable: it needs a release and a restart.  Mirror every change to Git.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath

ROOT = Path(os.environ.get("AUTORIG_ROOT", "/srv/autorig"))
LIVE = ROOT / "live"
OVERLAY = LIVE / "static"
JOURNAL = LIVE / "journal.jsonl"
LOCK = LIVE / ".promote.lock"
CURRENT = ROOT / "current"
RELEASES = ROOT / "releases"
STATIC_IN_RELEASE = PurePosixPath("autorig-online/static")
OWNER, GROUP = "autorig", "autorig"
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,120}$")
_FORBIDDEN_TOP = {"tasks", "glb_cache"}


def die(message: str) -> None:
    print(f"live_static: {message}", file=sys.stderr)
    sys.exit(2)


def clean_rel(rel: str) -> PurePosixPath:
    pure = PurePosixPath(str(rel).replace("\\", "/").lstrip("/"))
    if pure.parts and pure.parts[0] == "static":
        pure = PurePosixPath(*pure.parts[1:])
    if not pure.parts or any(p in ("", ".", "..") for p in pure.parts):
        die(f"bad path {rel!r}")
    if pure.parts[0] in _FORBIDDEN_TOP:
        die(f"{pure.parts[0]}/ is runtime data, not code")
    return pure


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ids() -> tuple[int, int]:
    return pwd.getpwnam(OWNER).pw_uid, grp.getgrnam(GROUP).gr_gid


def ensure_dirs() -> None:
    uid, gid = ids()
    for path in (LIVE, OVERLAY, LIVE / "config"):
        path.mkdir(parents=True, exist_ok=True)
        os.chown(path, uid, gid)
        os.chmod(path, 0o2755)


def journal(action: str, **fields) -> None:
    entry = {"at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "action": action,
             "actor": os.environ.get("LIVE_ACTOR") or os.environ.get("SUDO_USER") or pwd.getpwuid(os.getuid()).pw_name}
    entry.update(fields)
    with JOURNAL.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def atomic_copy(src: Path, dest: Path, mode: int = 0o664) -> None:
    """Copy src to dest through a temp file in dest's directory and one rename."""
    uid, gid = ids()
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".tmp", dir=str(dest.parent))
    try:
        with os.fdopen(fd, "wb") as out, src.open("rb") as inp:
            shutil.copyfileobj(inp, out, 1 << 20)
            out.flush()
            os.fsync(out.fileno())
        os.chown(tmp, uid, gid)
        os.chmod(tmp, mode)
        os.replace(tmp, dest)  # a fresh inode: other hardlinked releases keep theirs
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def overlay_files() -> list[PurePosixPath]:
    if not OVERLAY.is_dir():
        return []
    found = []
    for path in sorted(OVERLAY.rglob("*")):
        if path.is_file() and not path.name.startswith(".") and not path.name.endswith(".tmp"):
            found.append(PurePosixPath(path.relative_to(OVERLAY).as_posix()))
    return found


def in_release(release: Path, rel: PurePosixPath) -> Path:
    return release.joinpath(*STATIC_IN_RELEASE.parts, *rel.parts)


def differs(release: Path, rel: PurePosixPath) -> bool:
    target = in_release(release, rel)
    return not target.is_file() or sha256(target) != sha256(OVERLAY.joinpath(*rel.parts))


def repoint(release: Path) -> None:
    tmp = ROOT / f"current.live-{os.getpid()}"
    try:
        tmp.unlink()
    except FileNotFoundError:
        pass
    os.symlink(str(release), str(tmp))
    os.replace(str(tmp), str(CURRENT))


def promote(name: str | None = None) -> dict:
    ensure_dirs()
    with LOCK.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for _attempt in range(3):
            previous = CURRENT.resolve(strict=True)
            pending = [rel for rel in overlay_files() if differs(previous, rel)]
            if not pending:
                return {"promoted": [], "release": str(previous), "previous": str(previous)}
            stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            release_name = name or f"live-{stamp}"
            if not _NAME.fullmatch(release_name):
                die(f"bad release name {release_name!r}")
            staged = RELEASES / release_name
            if staged.exists():
                staged = RELEASES / f"{release_name}-{os.getpid()}"
            subprocess.run(["cp", "-al", str(previous), str(staged)], check=True)
            promoted = []
            for rel in pending:
                source = OVERLAY.joinpath(*rel.parts)
                target = in_release(staged, rel)
                mode = (target.stat().st_mode & 0o777) if target.is_file() else 0o664
                atomic_copy(source, target, mode)
                if sha256(target) != sha256(source):
                    die(f"staged copy of {rel} does not match the overlay")
                promoted.append({"rel": str(rel), "sha256": sha256(target)})
            if CURRENT.resolve(strict=True) != previous:
                # another agent switched `current` while we staged: stage again from it
                shutil.rmtree(staged)
                continue
            repoint(staged)
            journal("promote", release=staged.name, previous=previous.name, files=promoted)
            removed = gc()
            return {"promoted": promoted, "release": str(staged), "previous": str(previous), "overlay_cleared": removed}
        die("current kept moving; overlay left in place, run promote again")
    return {}


def gc() -> list[str]:
    """Drop overlay files that `current` already carries byte for byte."""
    current = CURRENT.resolve(strict=True)
    removed = []
    for rel in overlay_files():
        if not differs(current, rel):
            OVERLAY.joinpath(*rel.parts).unlink()
            removed.append(str(rel))
    for path in sorted(OVERLAY.rglob("*"), reverse=True):
        if path.is_dir():
            try:
                path.rmdir()
            except OSError:
                pass
    if removed:
        journal("gc", files=removed, release=current.name)
    return removed


def cmd_put(args) -> None:
    rel = clean_rel(args.rel)
    src = Path(args.file)
    if not src.is_file():
        die(f"no such file {src}")
    ensure_dirs()
    dest = OVERLAY.joinpath(*rel.parts)
    atomic_copy(src, dest)
    digest = sha256(dest)
    journal("put", rel=str(rel), sha256=digest, bytes=dest.stat().st_size)
    result = {"put": str(rel), "sha256": digest, "overlay": str(dest)}
    if not args.no_promote:
        result.update(promote(args.name))
    print(json.dumps(result, indent=1))


def cmd_rm(args) -> None:
    rel = clean_rel(args.rel)
    path = OVERLAY.joinpath(*rel.parts)
    if path.is_file():
        path.unlink()
        journal("rm", rel=str(rel))
    print(json.dumps({"removed": str(rel)}))


def cmd_ls(_args) -> None:
    current = CURRENT.resolve(strict=True)
    rows = []
    for rel in overlay_files():
        path = OVERLAY.joinpath(*rel.parts)
        target = in_release(current, rel)
        rows.append({"rel": str(rel), "sha256": sha256(path)[:16], "bytes": path.stat().st_size,
                     "mtime": dt.datetime.fromtimestamp(path.stat().st_mtime, dt.timezone.utc).isoformat(timespec="seconds"),
                     "current": "same" if target.is_file() and sha256(target) == sha256(path)
                     else ("differs" if target.is_file() else "absent")})
    print(json.dumps({"current": current.name, "overlay": rows}, indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    put = sub.add_parser("put")
    put.add_argument("rel")
    put.add_argument("file")
    put.add_argument("--no-promote", action="store_true")
    put.add_argument("--name")
    rm = sub.add_parser("rm")
    rm.add_argument("rel")
    sub.add_parser("ls")
    pro = sub.add_parser("promote")
    pro.add_argument("--name")
    sub.add_parser("gc")
    args = parser.parse_args()
    if os.geteuid() != 0:
        die("run with sudo")
    if args.cmd == "put":
        cmd_put(args)
    elif args.cmd == "rm":
        cmd_rm(args)
    elif args.cmd == "ls":
        cmd_ls(args)
    elif args.cmd == "promote":
        print(json.dumps(promote(args.name), indent=1))
    elif args.cmd == "gc":
        print(json.dumps({"overlay_cleared": gc()}))


if __name__ == "__main__":
    main()

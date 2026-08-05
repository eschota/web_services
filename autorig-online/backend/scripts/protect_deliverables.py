#!/usr/bin/env python3
"""Keep the last surviving copy of a user deliverable out of reach of cache eviction.

The converter workers purge their outputs, so for many finished tasks the copy in
``static/glb_cache`` is the ONLY one left - and that directory is a cache under
disk pressure. The pressure cleanup already refuses to evict an entry whose
upstream no longer serves it, but nothing protected those files from any other
delete path (a manual trim, a task purge, a stray glob).

This links every confirmed last-copy deliverable into /var/autorig/deliverables.
A hard link costs no extra disk: it is a second name for the same inode, so the
bytes survive even if the cache entry is removed, and ``restore`` puts the cache
entry back without touching the network.
"""
from __future__ import annotations
import json, os, sys, pathlib

CACHE = pathlib.Path("/root/autorig-online/static/glb_cache")
VAULT = pathlib.Path("/var/autorig/deliverables")
MEMO = pathlib.Path("/var/autorig/glb_cache_last_copy.json")
# Files a user paid for / cannot be regenerated without re-running the whole task.
DELIVERABLE_SUFFIXES = ("_all_animations_unity.fbx", "_all_animations.glb", "_animations.glb", "_prepared.glb")


def _memo_names() -> set:
    try:
        data = json.loads(MEMO.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, dict) else set()
    except Exception:
        return set()


def _is_deliverable(name: str) -> bool:
    return any(name.endswith(s) for s in DELIVERABLE_SUFFIXES)


def protect() -> int:
    VAULT.mkdir(parents=True, exist_ok=True)
    memo = _memo_names()
    linked = already = 0
    for entry in CACHE.iterdir():
        if not entry.is_file() or not _is_deliverable(entry.name):
            continue
        # Only last copies: linking everything would pin the whole cache and
        # leave the pressure cleanup with nothing it could actually free.
        if entry.name not in memo:
            continue
        target = VAULT / entry.name
        if target.exists():
            already += 1
            continue
        try:
            os.link(entry, target)   # same inode, no extra bytes
            linked += 1
        except OSError as exc:
            print(f"  link failed {entry.name}: {exc}")
    print(f"protected: linked={linked} already={already} vault={len(list(VAULT.iterdir()))}")
    return linked


def restore() -> int:
    if not VAULT.exists():
        return 0
    back = 0
    for entry in VAULT.iterdir():
        cached = CACHE / entry.name
        if cached.exists():
            continue
        try:
            os.link(entry, cached)
            back += 1
            print(f"  restored {entry.name}")
        except OSError as exc:
            print(f"  restore failed {entry.name}: {exc}")
    print(f"restored {back} cache entrie(s) from the vault")
    return back


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "protect"
    if mode == "restore":
        restore()
    else:
        protect()
        restore()

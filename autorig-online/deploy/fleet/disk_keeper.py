#!/usr/bin/env python3
"""Disk keeper (owner order 2026-10-14): «сделай автоочистку видео после загрузки на civitai, и поддерживай
автоматически место в определенном объеме чтобы не опускать дисковое хранилище сервера меньше 50гб свободных, как и
весь сервис».

Every 10 minutes (autorig-disk-keeper.timer, root, nice 19, idle IO):
  1. POSTED: every message whose 🔞 .red publish answered ok (the /nsfw and /dev bots' databases) loses its video
     files at once: the bot's own copy (media/<file>), the civitai upload copy (scratch/civitai-<uid>.*) and the
     scratch file its sidecar pointed at. Only video (.mp4 .webm .mov .mkv); the database rows, the sidecars, the
     Telegram messages and every picture stay. Done uids are remembered in /var/lib/autorig/disk_keeper.json.
  2. PRESSURE: while free space on /srv/autorig is under KEEP_GB (50), media files are deleted oldest first - video,
     then audio, then pictures - from the 24-hour retention roots with the same protections (media_retention.py:
     PROTECT paths, 3D models and archives never, renderfin files a task points at never), only files older than
     MIN_AGE_H (2 h, so no running job loses its output), until TARGET_GB (55) is free. Still under KEEP_GB after
     that -> one DEV alert per 3 h.
Log: /var/log/autorig-disk-keeper.log.
    disk_keeper.py [--dry-run]
"""
import json
import os
import pathlib
import re
import shutil
import sqlite3
import sys
import time
import urllib.request

sys.path.insert(0, "/srv/autorig/fleet")
import media_retention as mr  # noqa: E402  (its roots and protections)

KEEP_GB, TARGET_GB, MIN_AGE_H = 50.0, 55.0, 2.0
VIDEO = {".mp4", ".webm", ".mov", ".mkv", ".avi"}
AUDIO = {".mp3", ".wav", ".flac", ".m4a", ".ogg", ".aac"}
BOTS = [pathlib.Path("/srv/autorig/data/nsfwbot"), pathlib.Path("/srv/autorig/data/devbot")]
SCRATCH = {"/nsfw/api/scratch/": pathlib.Path("/srv/autorig/data/nsfwbot/scratch"),
           "/dev/api/scratch/": pathlib.Path("/srv/autorig/data/devbot/scratch")}
STATE = pathlib.Path("/var/lib/autorig/disk_keeper.json")
LOG = "/var/log/autorig-disk-keeper.log"
DRY = "--dry-run" in sys.argv


def log(msg):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
    print(line, flush=True)
    try:
        with open(LOG, "a") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def free_gb():
    return shutil.disk_usage("/srv/autorig").free / 2 ** 30


def drop(p):
    """Delete one file; its size, 0 when it was not there."""
    try:
        size = os.lstat(p).st_size
    except OSError:
        return 0
    if not DRY:
        try:
            os.unlink(p)
        except OSError:
            return 0
    return size


def scratch_file(url):
    for part, folder in SCRATCH.items():
        if part in (url or ""):
            name = url.split(part, 1)[1].split("?")[0]
            if name and "/" not in name:
                return folder / name
    return None


def posted(state):
    """The videos of every message already on civitai."""
    done = set(state.get("posted_done") or [])
    freed = files = 0
    for bot in BOTS:
        db = bot / "devbot.db"
        if not db.exists():
            continue
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=20)
            rows = con.execute("SELECT reply_to_uid, text FROM messages WHERE kind = 'publish' AND "
                               "text LIKE '%\"ok\": true%' AND reply_to_uid IS NOT NULL").fetchall()
            paths = {}
            for uid, _ in rows:
                if uid in done:
                    continue
                r = con.execute("SELECT media_path FROM messages WHERE uid = ?", (uid,)).fetchone()
                paths[uid] = r[0] if r else None
            con.close()
        except Exception as e:
            log(f"{db}: {e!r}"[:200])
            continue
        for uid, text in rows:
            if uid in done:
                continue
            victims = []
            if paths.get(uid):
                victims.append(bot / "media" / pathlib.Path(paths[uid]).name)
            m = re.search(r'"media_url":\s*"([^"]+)"', text or "")
            if m and scratch_file(m.group(1)):
                victims.append(scratch_file(m.group(1)))
            victims += list((bot / "scratch").glob(f"civitai-{uid}.*"))
            try:
                side = json.loads((bot / "media" / f"{uid}.source.json").read_text(encoding="utf-8"))
                if scratch_file(side.get("source_url")):
                    victims.append(scratch_file(side["source_url"]))
            except Exception:
                pass
            for v in {str(x) for x in victims}:
                if os.path.splitext(v)[1].lower() in VIDEO:
                    n = drop(v)
                    if n:
                        freed += n
                        files += 1
            done.add(uid)
    state["posted_done"] = sorted(done)[-20000:]
    if files:
        log(f"{'would delete' if DRY else 'deleted'} {files} posted videos, {freed / 1e9:.2f} GB")
    return freed


def pressure(state):
    """Oldest media first until TARGET_GB is free, while under KEEP_GB."""
    if free_gb() >= KEEP_GB:
        return 0
    now, refs = time.time(), mr.referenced()
    cands = []
    for root in mr.ROOTS:
        for dirpath, dirs, names in os.walk(root):
            rel = os.path.relpath(dirpath, mr.DATA)
            dirs[:] = [d for d in dirs if not mr.PROTECT.search(f"{rel}/{d}")]
            for n in names:
                p = os.path.join(dirpath, n)
                ext = os.path.splitext(n)[1].lower()
                if ext not in mr.MEDIA or os.path.islink(p):
                    continue
                try:
                    st = os.lstat(p)
                except OSError:
                    continue
                if now - st.st_mtime < MIN_AGE_H * 3600:
                    continue
                if p.startswith(mr.RF + "render/") and os.path.relpath(p, mr.RF + "render").replace(os.sep, "/") in refs:
                    continue
                rank = 0 if ext in VIDEO else 1 if ext in AUDIO else 2
                cands.append((rank, st.st_mtime, p, st.st_size))
    cands.sort()
    freed = files = 0
    need = (TARGET_GB - free_gb()) * 2 ** 30
    for rank, mtime, p, size in cands:
        if freed >= need:
            break
        n = drop(p)
        freed += n
        files += 1 if n else 0
    log(f"PRESSURE: free was under {KEEP_GB:.0f} GB; {'would delete' if DRY else 'deleted'} {files} media files, "
        f"{freed / 1e9:.2f} GB; free now {free_gb():.1f} GB")
    return freed


def alert(state):
    left = free_gb()
    if left >= KEEP_GB or time.time() - state.get("alerted_at", 0) < 3 * 3600 or DRY:
        return
    try:
        body = urllib.parse.urlencode({"agent": "Disk keeper", "project": "VPS", "kind": "text", "caption":
                                       f"⚠️ На сервере свободно {left:.1f} ГБ - меньше {KEEP_GB:.0f} ГБ даже после "
                                       f"очистки медиа. Нужна чистка вручную."}).encode()
        urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:8220/dev/api/send", data=body), timeout=30)
        state["alerted_at"] = time.time()
        log(f"ALERT sent: free {left:.1f} GB")
    except Exception as e:
        log(f"alert failed {e!r}"[:200])


def main():
    import urllib.parse  # noqa: F401 (for alert)
    try:
        state = json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        state = {}
    before = free_gb()
    posted(state)
    pressure(state)
    alert(state)
    if not DRY:
        STATE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        tmp.replace(STATE)
    log(f"free {before:.1f} -> {free_gb():.1f} GB")


if __name__ == "__main__":
    import urllib.parse  # noqa: E402,F401
    main()

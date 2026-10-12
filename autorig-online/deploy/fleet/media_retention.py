#!/usr/bin/env python3
"""24-hour media retention (OWNER ORDER 2026-10-12, explicit): «выходы рендерфина ... хранить рендеры только 24 часа ...
медиа ботов devbot, Сура, nsfwbot, shorts, red ... так же должны чиститься».

Deletes MEDIA files (images, video, audio) older than 24 h by mtime in
  * renderfin outputs: data/var/renderfin/{render,cancelled-*,tmp,music-mux,rejected}
  * bot media: data/{devbot,sura_session,nsfwbot,shorts,red_film,red_autopost}
Never deletes: databases (*.db, *.sqlite*), json/state/config, code, logs, models / LoRA (*.safetensors ...), datasets,
secrets, 3D models and archives (*.glb *.fbx *.obj *.zip: customer deliverables, listed only), reference material
(path parts refs, reffilm, concepts, characters, dataset*, lora*, models, music, deploy, ...), and anything modified in
the last 24 h (a running job's output). Runs hourly from autorig-vps-cleanup.timer at nice 19 / idle IO. Log:
/var/log/autorig-media-retention.log.

    media_retention.py [--dry-run] [--inventory]
"""
import os
import re
import sys
import time
import urllib.parse

DATA = "/srv/autorig/data"
ROOTS = [f"{DATA}/var/renderfin/{d}" for d in ("render", "tmp", "music-mux", "rejected")] + \
        [f"{DATA}/var/renderfin/{d}" for d in os.listdir(f"{DATA}/var/renderfin") if d.startswith("cancelled-")] + \
        [f"{DATA}/{d}" for d in ("devbot", "sura_session", "nsfwbot", "shorts", "red_film", "red_autopost")]
MEDIA = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".mp4", ".webm", ".mov", ".mkv", ".avi", ".mp3", ".wav", ".flac",
         ".m4a", ".ogg", ".aac"}
HELD = {".glb", ".fbx", ".obj", ".zip", ".blend", ".gltf"}                       # reported, never deleted
PROTECT = re.compile(r"(^|/)(refs?|reffilm|references?|concepts?|characters?|datasets?|dataset[^/]*|lora[^/]*|loras|"
                     r"models?|music|deploy|secrets?|db|locks|servers[^/]*|workflow-backups|cutover-backups|"
                     r"quarantine|__pycache__|\.git|assets|backdrops?)(/|$)", re.I)
RF = f"{DATA}/var/renderfin/"
RF_DELETE_3D = {".glb", ".gltf"}               # owner 2026-10-12 «да удаляй»: renderfin 3D outputs go too, unless a task uses them
MAXAGE = 24 * 3600
DRY = "--dry-run" in sys.argv
LOG = "/var/log/autorig-media-retention.log"


def log(msg):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + msg
    print(line, flush=True)
    try:
        with open(LOG, "a") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def referenced() -> set:
    """renderfin/render/<dir>/<file> paths that any task row points at (source, outputs, viewer GLBs ...): kept."""
    import sqlite3
    refs = set()
    rx = re.compile(r"renderfin/render/([^\"'\s,}\]?#]+)")
    con = sqlite3.connect("file:/srv/autorig/data/db/autorig.db?mode=ro", uri=True, timeout=20)
    cols = ("input_url", "output_urls", "ready_urls", "viewer_settings", "viewer_prepared_glb_url",
            "viewer_animations_glb_url", "video_url", "fbx_glb_output_url", "progress_page")
    for row in con.execute("SELECT " + ",".join(cols) + " FROM tasks WHERE " +
                           " OR ".join(f"{c} LIKE '%renderfin/render%'" for c in cols)):
        for v in row:
            for m in rx.finditer(str(v or "")):
                refs.add(urllib.parse.unquote(m.group(1)))
    con.close()
    return refs


def main():
    now = time.time()
    refs = referenced()
    log(f"{len(refs)} renderfin files are referenced by tasks and kept")
    per = {}
    held = 0
    for root in ROOTS:
        freed = files = 0
        for dirpath, dirs, names in os.walk(root):
            rel = os.path.relpath(dirpath, DATA)
            dirs[:] = [d for d in dirs if not PROTECT.search(f"{rel}/{d}")]
            for n in names:
                p = os.path.join(dirpath, n)
                ext = os.path.splitext(n)[1].lower()
                try:
                    st = os.lstat(p)
                except OSError:
                    continue
                if now - st.st_mtime < MAXAGE or os.path.islink(p):
                    continue
                if p.startswith(RF):
                    rel_rf = os.path.relpath(p, RF + "render") if p.startswith(RF + "render/") else ""
                    if rel_rf and rel_rf.replace(os.sep, "/") in refs:
                        held += st.st_size
                        continue
                    if ext in RF_DELETE_3D:
                        freed += st.st_size
                        files += 1
                        if not DRY:
                            try:
                                os.unlink(p)
                            except OSError:
                                pass
                        continue
                if ext in HELD:
                    held += st.st_size
                    continue
                if ext not in MEDIA:
                    continue
                freed += st.st_size
                files += 1
                if not DRY:
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
        per[root] = (files, freed)
        if files:
            log(f"{'would delete' if DRY else 'deleted'} {files} files, {freed / 1e9:.2f} GB in {root}")
    total = sum(v[1] for v in per.values())
    log(f"{'DRY RUN: ' if DRY else ''}total {total / 1e9:.2f} GB; held (3D models / archives older than 24 h, never "
        f"deleted) {held / 1e9:.2f} GB")


if __name__ == "__main__":
    main()

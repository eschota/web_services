#!/usr/bin/env python3
"""Small static card posters for every public gallery task (Gallery perf, 2026-10-15).

For each task listed by the public gallery API it writes /srv/autorig/data/static/posters-s/<task>.webp
(<= 360x640, WebP, tens of KB). nginx serves it at /pw/<task>.webp with a long immutable cache, and
backend thumb_url_for() hands out /pw/<task>.webp?v=<mtime hex> once the file exists. The source is the V3 capture
(posters-v3/<task>.jpg) when there is one, otherwise the legacy poster through the local /thumb/<task>.
A newer V3 capture regenerates the small file. Runs niced from a timer, one batch per run, and stops when the
box is busy (load above cores/2). Never deletes anything.
"""
import io, json, os, sys, time, urllib.request
from pathlib import Path
from PIL import Image

API = os.getenv("GALLERY_API", "http://127.0.0.1:8200")
V3 = Path(os.getenv("AUTORIG_V3_POSTER_DIR", "/srv/autorig/data/static/posters-v3"))
OUT = Path(os.getenv("AUTORIG_SMALL_POSTER_DIR", "/srv/autorig/data/static/posters-s"))
BATCH = int(os.getenv("GALLERY_SMALL_BATCH", "80"))
MAX_LOAD = float(os.getenv("GALLERY_SMALL_MAX_LOAD", str((os.cpu_count() or 4) / 2)))
BOX = (360, 640)


def busy() -> bool:
    return os.getloadavg()[0] > MAX_LOAD


def get(url: str, timeout=30) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def task_ids():
    seen, page = [], 1
    while page < 60:
        d = json.loads(get(f"{API}/api/gallery?per_page=60&sort=date&page={page}"))
        seen += [i["task_id"] for i in d.get("items", [])]
        if not d.get("has_more"):
            break
        page += 1
    return seen


def make(task_id: str) -> bool:
    dst = OUT / f"{task_id}.webp"
    src = V3 / f"{task_id}.jpg"
    have_v3 = src.is_file() and src.stat().st_size > 8000
    if dst.is_file():
        if not have_v3 or src.stat().st_mtime <= dst.stat().st_mtime:
            return False
    data = src.read_bytes() if have_v3 else get(f"{API}/thumb/{task_id}")
    im = Image.open(io.BytesIO(data))
    im.draft("RGB", (BOX[0] * 2, BOX[1] * 2))
    im = im.convert("RGB")
    im.thumbnail(BOX, Image.LANCZOS)
    tmp = OUT / f".{task_id}.tmp"
    im.save(tmp, "WEBP", quality=72, method=4)
    os.replace(tmp, dst)
    return True


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    if busy():
        print("busy, skip"); return 0
    done = failed = 0
    for tid in task_ids():
        if done >= BATCH:
            break
        if busy():
            print("load high, pause"); break
        try:
            if make(tid):
                done += 1
        except Exception as e:  # keep going: one broken poster must not stop the batch
            failed += 1
            print(f"{tid}: {type(e).__name__}: {e}", file=sys.stderr)
    print(f"made={done} failed={failed} total_files={len(list(OUT.glob('*.webp')))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

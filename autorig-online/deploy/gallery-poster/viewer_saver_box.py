"""viewer_saver.exe on the farm boxes (owner 2026-10-12): the preview is rendered by the Windows build of the same
Unity viewer, at full quality, offscreen; Chrome (viewer_capture.py) stays the fallback.

Owner: «вьювер и его сохранение файлов должно быть в полной графике … публичный .exe билд viewer_saver.exe …
вьювер и сейвер должны раскатываться при билде и деплое автоматически на воркеров».

Release (R:\\3d_video_motion_transfer\\tools\\viewer_release.ps1) publishes, from one commit:
  /api/mt/unity/<tag>/                      the WebGL viewer, with saver.json = the saver manifest of the same tag
  /api/mt/saver/<tag>/viewer_saver-<tag>.zip + manifest.json (tag, sha256, bytes, url, webgl_build)
  /api/mt/saver/manifest.json               the newest release

Box side (bundle "preview", runs under farm_runner.py; standard library only):
  %LOCALAPPDATA%\\AutoRig\\viewer-saver\\<tag>\\viewer_saver.exe   versioned dirs, sha256-verified, current.txt switched
                                                               atomically, the current one + 2 older kept
  python viewer_saver_box.py --prefetch [--tag T]     install T (default: the saver of the live viewer, else the
                                                      newest), write meta.json (the farm job saver_prefetch)
  import viewer_saver_box; viewer_saver_box.capture(url, seconds, fps, max_mb)
        -> meta dict, preview.mp4 + poster.jpg in cwd; None when the live viewer has no saver (Chrome then)

VPS side:
  python3 viewer_saver_box.py --enqueue-prefetch <tag>   one saver_prefetch job per farm box
  python3 viewer_saver_box.py --collect                  per-box saver versions -> /srv/autorig/data/viewer-saver/boxes.json
                                                         (read by /api/fleet: box.viewer_saver_object)
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import zipfile

SITE = os.environ.get("AUTORIG_SITE", "https://autorig.online")
HOME = pathlib.Path(os.environ.get("VIEWER_SAVER_HOME") or
                    (pathlib.Path(os.environ.get("LOCALAPPDATA") or pathlib.Path.home()) / "AutoRig" / "viewer-saver"))
KEEP_OLD = 2
TAG_RE = re.compile(r"^[A-Za-z0-9._-]{1,80}$")
VPS_DIR = pathlib.Path("/srv/autorig/data/viewer-saver")
FARM_BOXES = [b for b in os.getenv("GP_FARM_BOXES", "f7,f1,f2,f13,f15,f12").split(",") if b]   # f5: out of the fleet


def _get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "autorig-viewer-saver-box/1", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _json(url, timeout=30):
    try:
        return json.loads(_get(url, timeout).decode("utf-8"))
    except Exception:  # noqa: BLE001
        return None


def live_manifest(viewer_build="test"):
    """The saver of the viewer build the page runs (unity/<build>/saver.json); None: that viewer has no saver."""
    m = _json(f"{SITE}/api/mt/unity/{viewer_build}/saver.json")
    return m if isinstance(m, dict) and TAG_RE.match(str(m.get("tag") or "")) else None


def tag_manifest(tag):
    return _json(f"{SITE}/api/mt/saver/{tag}/manifest.json") if TAG_RE.match(tag or "") else None


# ------------------------------------------------------------------ box: install
class _Lock:
    """One installer at a time on the box (two preview jobs may start together)."""

    def __init__(self, path, wait=900):
        self.path, self.wait = path, wait

    def __enter__(self):
        end = time.time() + self.wait
        while True:
            try:
                fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()} {time.time()}".encode())
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    if time.time() - self.path.stat().st_mtime > 1800:      # stale: an installer that died
                        self.path.unlink()
                        continue
                except OSError:
                    pass
                if time.time() > end:
                    raise TimeoutError("viewer-saver install lock")
                time.sleep(2)

    def __exit__(self, *a):
        try:
            self.path.unlink()
        except OSError:
            pass


def current_tag():
    try:
        return (HOME / "current.txt").read_text().strip()
    except OSError:
        return ""


def installed():
    return sorted((d.name for d in HOME.glob("*") if d.is_dir() and (d / ".ok").is_file()),
                  key=lambda n: (HOME / n / ".ok").stat().st_mtime)


def ensure(manifest):
    """Install the manifest's saver (download, verify sha256, extract into a versioned dir), point current at it.
    Returns (exe path, info dict)."""
    tag = str(manifest["tag"])
    if not TAG_RE.match(tag):
        raise ValueError("bad tag")
    info = {"tag": tag, "downloaded": False}
    HOME.mkdir(parents=True, exist_ok=True)
    d = HOME / tag
    t0 = time.time()
    with _Lock(HOME / ".install.lock"):
        if not (d / ".ok").is_file():
            tmpzip = HOME / f".dl-{tag}.zip"
            h = hashlib.sha256()
            req = urllib.request.Request(manifest["url"], headers={"User-Agent": "autorig-viewer-saver-box/1"})
            with urllib.request.urlopen(req, timeout=120) as r, open(tmpzip, "wb") as f:
                while True:
                    b = r.read(1 << 20)
                    if not b:
                        break
                    h.update(b)
                    f.write(b)
            if h.hexdigest() != str(manifest["sha256"]).lower():
                tmpzip.unlink()
                raise ValueError(f"sha256 mismatch for {tag}")
            info["download_s"] = round(time.time() - t0, 1)
            stage = HOME / f".{tag}.tmp"
            shutil.rmtree(stage, ignore_errors=True)
            with zipfile.ZipFile(tmpzip) as z:
                for n in z.namelist():                                   # no path escapes
                    if n.startswith(("/", "\\")) or ".." in pathlib.PurePath(n.replace("\\", "/")).parts:
                        raise ValueError("bad zip member")
                z.extractall(stage)
            tmpzip.unlink()
            if not (stage / "viewer_saver.exe").is_file():
                shutil.rmtree(stage, ignore_errors=True)
                raise ValueError("zip has no viewer_saver.exe")
            (stage / ".ok").write_text(json.dumps(manifest))
            shutil.rmtree(d, ignore_errors=True)
            os.replace(stage, d)
            info["downloaded"] = True
            info["install_s"] = round(time.time() - t0, 1)
        if current_tag() != tag:
            tmp = HOME / ".current.txt.tmp"
            tmp.write_text(tag)
            os.replace(tmp, HOME / "current.txt")
        # keep current + the KEEP_OLD newest others
        old = [n for n in installed() if n != tag]
        for n in old[:-KEEP_OLD] if len(old) > KEEP_OLD else []:
            shutil.rmtree(HOME / n, ignore_errors=True)
        for junk in HOME.glob(".*.tmp"):
            if junk.is_dir() and time.time() - junk.stat().st_mtime > 3600:
                shutil.rmtree(junk, ignore_errors=True)
    info["versions"] = installed()
    info["current"] = current_tag()
    return d / "viewer_saver.exe", info


# ------------------------------------------------------------------ box: record
def record(exe, run, seconds=7.0, fps=30, max_mb=8.0, size="1080x1920", clip="", timeout=240, out_dir="."):
    """Run the saver offscreen (-batchmode: no window, the camera renders into a RenderTexture)."""
    out_dir = pathlib.Path(out_dir).resolve()
    try:
        from viewer_capture import ffmpeg_bin
        ff = ffmpeg_bin()
    except Exception:  # noqa: BLE001
        ff = shutil.which("ffmpeg") or "ffmpeg"
    status = out_dir / "saver_status.json"
    cmd = [str(exe), "-batchmode", "-logFile", str(out_dir / "saver_player.log"),
           "--run", run, "--out", str(out_dir / "preview.mp4"), "--poster", str(out_dir / "poster.jpg"),
           "--status", str(status), "--secs", str(seconds), "--fps", str(fps), "--maxmb", str(max_mb),
           "--size", size, "--timeout", str(timeout), "--ffmpeg", ff]
    if clip:
        cmd += ["--clip", clip]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, cwd=str(exe.parent), timeout=timeout + 200, capture_output=True,
                           creationflags=0x08000000 if os.name == "nt" else 0)
        code = p.returncode
    except subprocess.TimeoutExpired:
        code = 124
    try:
        st = json.loads(status.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        st = {"ok": False, "error": "no status"}
    st["exit"] = code
    st["wall_s"] = round(time.time() - t0, 1)
    return st


def capture(url, seconds=7.0, fps=30, max_mb=8.0):
    """viewer_capture.py's first choice. The saver must be the one released with the WebGL viewer the URL opens."""
    m = re.search(r"/api/mt/unity/([A-Za-z0-9._-]+)/index\.html\?(?:.*&)?run=([0-9a-f]{20})", url or "")
    if not m:
        return None
    build, run = m.group(1), m.group(2)
    manifest = live_manifest(build)
    if not manifest:
        return {"saver": None, "saver_skip": f"viewer build {build} has no saver"}
    exe, info = ensure(manifest)
    st = record(exe, run, seconds, fps, max_mb)
    meta = {"engine": "viewer_saver", "saver_tag": manifest["tag"], "saver_install": info, "saver": st,
            "url": url, "box": os.environ.get("FARM_BOX", "")}
    ok = st.get("ok") and os.path.isfile("preview.mp4") and os.path.getsize("preview.mp4") >= 100_000 \
        and os.path.isfile("poster.jpg")
    meta["ok"] = bool(ok)
    return meta


def prefetch(tag=None):
    t0 = time.time()
    manifest = (tag_manifest(tag) if tag else None) or live_manifest() or _json(f"{SITE}/api/mt/saver/manifest.json")
    meta = {"kind": "saver_prefetch", "box": os.environ.get("FARM_BOX", ""), "asked": tag, "at": time.time()}
    if not manifest:
        meta.update(ok=False, error="no manifest")
    else:
        try:
            _, info = ensure(manifest)
            meta.update(ok=True, **info)
        except Exception as exc:  # noqa: BLE001
            meta.update(ok=False, error=f"{type(exc).__name__}: {exc}"[:300], versions=installed(), current=current_tag())
    meta["seconds"] = round(time.time() - t0, 1)
    with open("meta.json", "w") as f:
        json.dump(meta, f, indent=1)
    print(json.dumps(meta))
    return 0 if meta.get("ok") else 1


# ------------------------------------------------------------------ VPS side
def _farm():
    sys.path.insert(0, "/srv/autorig/farmjobs")
    import farmjobs
    return farmjobs


def enqueue_prefetch(tag):
    fj = _farm()
    q = fj.Queue()
    ids = {}
    for box in FARM_BOXES:
        ids[box] = q.enqueue({"kind": "saver_prefetch", "priority": 5, "bundle": "preview", "boxes": [box],
                              "timeout": 900, "max_attempts": 3,
                              "cmd": ["{python}", "{bundle}/viewer_saver_box.py", "--prefetch", "--tag", tag]})
    VPS_DIR.mkdir(parents=True, exist_ok=True)
    (VPS_DIR / tag).mkdir(exist_ok=True)
    p = VPS_DIR / tag / "prefetch_jobs.json"
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps({"tag": tag, "enqueued": time.time(), "jobs": ids}, indent=1))
    os.replace(tmp, p)
    print(json.dumps(ids))


def collect():
    """Per-box saver state from the finished saver_prefetch / viewer_preview jobs (newest wins)."""
    fj = _farm()
    q = fj.Queue()
    out_p = VPS_DIR / "boxes.json"
    try:
        boxes = json.loads(out_p.read_text()).get("boxes", {})
    except (OSError, ValueError):
        boxes = {}
    rows = fj._db().execute("SELECT id, kind, state, box, finished, seconds FROM jobs WHERE kind IN "
                            "('saver_prefetch','viewer_preview') AND state IN ('done','error') AND box IS NOT NULL "
                            "ORDER BY finished DESC LIMIT 400").fetchall()
    seen = set()
    for r in rows:
        box = r["box"]
        if not box or box in seen:
            continue
        rp = q.result_path(r["id"])
        if not rp.is_file():
            continue
        try:
            with zipfile.ZipFile(rp) as z:
                meta = json.loads(z.read("meta.json")) if "meta.json" in z.namelist() else {}
        except (OSError, ValueError, zipfile.BadZipFile, KeyError):
            continue
        if r["kind"] == "viewer_preview" and meta.get("engine") != "viewer_saver" and "saver_install" not in meta:
            continue
        inst = meta.get("saver_install") or meta
        seen.add(box)
        boxes[box] = {"current_string": inst.get("current") or meta.get("saver_tag") or "",
                      "versions_array": inst.get("versions") or [],
                      "ok_bool": bool(meta.get("ok")), "error_string": meta.get("error") or "",
                      "job_string": r["id"], "kind_string": r["kind"],
                      "install_seconds_float": inst.get("install_s"),
                      "job_seconds_float": r["seconds"], "at_float": r["finished"]}
    try:
        latest = json.loads((VPS_DIR / "manifest.json").read_text())
    except (OSError, ValueError):
        latest = {}
    try:
        live = json.loads((pathlib.Path("/srv/autorig/data/motion_transfer/unity/test") / "saver.json").read_text())
    except (OSError, ValueError):
        live = {}
    doc = {"schema": "autorig.viewer-saver-boxes/1", "generated": time.time(), "latest_tag": latest.get("tag", ""),
           "live_viewer_saver_tag": live.get("tag", ""), "boxes": boxes}
    tmp = out_p.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, indent=1))
    os.replace(tmp, out_p)
    print(json.dumps(doc)[:1500])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefetch", action="store_true")
    ap.add_argument("--tag", default="")
    ap.add_argument("--enqueue-prefetch", metavar="TAG")
    ap.add_argument("--collect", action="store_true")
    a = ap.parse_args()
    if a.prefetch:
        return prefetch(a.tag or None)
    if a.enqueue_prefetch:
        if not TAG_RE.match(a.enqueue_prefetch):
            raise SystemExit("bad tag")
        return enqueue_prefetch(a.enqueue_prefetch)
    if a.collect:
        return collect()
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main() or 0)

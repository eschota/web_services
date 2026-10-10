"""Downloads · V3 export worker: rigged V3 GLB -> FBX in Blender, on request from the task page.

Pulls jobs from autorig.online (``GET /api/v3-export/worker/next``), fetches the job's GLB and the service's own
Blender script (``backend/v3_export_blender.py``, this job's version), runs Blender in the background, relays its
«PROGRESS» lines as the task page's percent and stage, uploads the FBX and reports. Standard library only, no open
ports (pull model), CPU only: it never takes VRAM from the box's GPU jobs.

    pythonw export_worker.py        key: %USERPROFILE%\\.secrets\\autorig-v3-export-worker.key
    env: BLENDER (blender.exe), AUTORIG_API (default https://autorig.online), V3X_WORK (scratch folder)
"""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

API = os.environ.get("AUTORIG_API", "https://autorig.online").rstrip("/")
BLENDER = os.environ.get("BLENDER", r"C:\Program Files\Blender Foundation\Blender 5.1\blender.exe")
KEY = (pathlib.Path.home() / ".secrets" / "autorig-v3-export-worker.key").read_text().strip()
WORK = os.environ.get("V3X_WORK") or None
UA = "autorig-v3-export-worker/1"
QUIET = {"creationflags": 0x08000000} if os.name == "nt" else {}       # CREATE_NO_WINDOW
LOG = pathlib.Path(os.environ.get("V3X_LOG") or (pathlib.Path.home() / "v3-export-worker.log"))


def log(*parts):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + " ".join(str(p) for p in parts)
    try:
        if LOG.exists() and LOG.stat().st_size > 2_000_000:
            LOG.replace(LOG.with_suffix(".old.log"))
        with LOG.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def request(method, path, body=None, headers=None, timeout=60, raw=False):
    stream = hasattr(body, "read")
    data = body if stream or isinstance(body, (bytes, bytearray)) or body is None else json.dumps(body).encode()
    hdrs = {"User-Agent": UA, "Authorization": f"Bearer {KEY}", **(headers or {})}
    if stream:
        hdrs["Content-Length"] = str(len(body))
    elif body is not None and not isinstance(body, (bytes, bytearray)):
        hdrs["Content-Type"] = "application/json"
    req = urllib.request.Request(API + path, data=data, method=method, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        payload = r.read()
        if raw:
            return r.status, payload
        return r.status, (json.loads(payload) if payload and r.status == 200 else None)


class Job:
    def __init__(self, doc):
        self.doc = doc
        self.headers = {"X-Lease": doc["lease"]}
        self.last = 0.0

    def progress(self, value, stage, force=False):
        now = time.time()
        if not force and now - self.last < 1.5:
            return
        self.last = now
        try:
            request("POST", f"/api/v3-export/worker/jobs/{self.doc['id']}/progress",
                    {"progress": round(value, 3), "stage": stage}, headers=self.headers, timeout=20)
        except (urllib.error.URLError, OSError) as exc:
            log("progress failed", self.doc["id"], exc)


class Upload:
    """The FBX as a request body that reports how much of it has been sent (0.9 .. 0.97 of the bar)."""

    def __init__(self, path, job):
        self.fh, self.size, self.sent, self.job = open(path, "rb"), path.stat().st_size, 0, job

    def __len__(self):
        return self.size

    def read(self, n=-1):
        block = self.fh.read(1 << 20 if n is None or n < 0 else min(n, 1 << 20))
        self.sent += len(block)
        self.job.progress(0.9 + 0.07 * self.sent / max(1, self.size), "upload")
        if not block:
            self.fh.close()
        return block


def run(doc):
    job = Job(doc)
    with tempfile.TemporaryDirectory(prefix="v3x_", dir=WORK) as tmp:
        tmp = pathlib.Path(tmp)
        job.progress(0.06, "download", force=True)
        _, script = request("GET", doc["script"], timeout=45, raw=True)   # a stalled link fails fast: the job goes back to the queue
        (tmp / "v3_export_blender.py").write_bytes(script)
        _, glb = request("GET", doc["source"], headers=job.headers, timeout=90, raw=True)
        (tmp / "rigged.glb").write_bytes(glb)
        (tmp / "spec.json").write_text(json.dumps(doc.get("spec") or {}), encoding="utf-8")
        job.progress(0.15, "import", force=True)
        out = tmp / "out"
        args = [BLENDER, "-b", "--factory-startup", "-P", str(tmp / "v3_export_blender.py"), "--",
                str(tmp / "rigged.glb"), str(out), str(tmp / "spec.json")]
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                encoding="utf-8", errors="replace", **QUIET)
        tail, report = [], {}
        killer = threading.Timer(900, proc.kill)
        killer.start()
        try:
            for line in proc.stdout:
                line = line.rstrip()
                tail = (tail + [line])[-40:]
                if line.startswith("PROGRESS "):
                    parts = line.split(" ", 2)
                    try:
                        p = float(parts[1])
                    except (IndexError, ValueError):
                        continue
                    job.progress(0.15 + 0.72 * p, parts[2] if len(parts) > 2 else "fbx")
                elif line.startswith("EXPORT "):
                    try:
                        report = json.loads(line[7:])
                    except ValueError:
                        report = {}
            proc.wait()
        finally:
            killer.cancel()
        names = list((report.get("files") or {}).keys()) or ["out.fbx"]
        fbx = out / names[0]                                # out.fbx | out.glb | out.blend (the spec's format)
        if proc.returncode != 0 or not fbx.is_file() or not (report.get("verify") or {}).get("ok"):
            raise RuntimeError(f"blender exit {proc.returncode}: " + " | ".join(tail[-6:])[-400:])
        job.progress(0.9, "upload", force=True)
        slim = {k: report.get(k) for k in ("seconds", "blender", "bones", "verify", "clips", "exported_clips")}
        request("PUT", f"/api/v3-export/worker/jobs/{doc['id']}/result", Upload(fbx, job),
                headers={**job.headers, "Content-Type": "application/octet-stream",
                         "X-Export-Report": json.dumps(slim, ensure_ascii=True)[:4000]}, timeout=900)
        return report


def main():
    log("v3 export worker ->", API, "blender", BLENDER)
    while True:
        try:
            status, doc = request("GET", "/api/v3-export/worker/next", timeout=30)
        except (urllib.error.URLError, OSError) as exc:
            log("poll failed:", exc)
            time.sleep(15)
            continue
        if status != 200 or not doc:
            time.sleep(3)
            continue
        log("job", doc["id"], doc.get("format"), doc.get("spec"))
        t0 = time.time()
        try:
            rep = run(doc)
            log("done", doc["id"], round(time.time() - t0, 1), "s", rep.get("files"))
        except Exception as exc:                                    # noqa: BLE001
            log("failed", doc["id"], repr(exc)[:400])
            try:
                request("POST", f"/api/v3-export/worker/jobs/{doc['id']}/fail", {"error": repr(exc)[:400]},
                        headers={"X-Lease": doc["lease"]}, timeout=30)
            except (urllib.error.URLError, OSError):
                pass


if __name__ == "__main__":
    sys.exit(main())

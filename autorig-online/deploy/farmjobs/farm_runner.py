"""AutoRig farm job runner (pull model, standard library only, one per farm box).

    pythonw farm_runner.py            home: %LOCALAPPDATA%\\AutoRig\\farm-runner (key, box.txt, bundles, work, log)

Polls https://autorig.online/api/farmjobs/next, runs the leased job in a scratch dir at IDLE priority and posts the
result zip back. It never takes work while the box's own jobs run: the box's converter (127.0.0.1:7000), ComfyUI
(8488/8188/8588/8988/8288) or a busy CPU make it answer "busy" (the VPS then hands out nothing and /api/fleet shows
why). A job in flight is also held (heartbeat only) never killed by this check: it runs at idle priority.
Owner order 2026-10-11: customer tasks first, background compute on the farm.
"""
import ctypes
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zipfile

API = os.environ.get("FARMJOBS_API", "https://autorig.online/api/farmjobs").rstrip("/")
HOME = pathlib.Path(os.environ.get("FARM_RUNNER_HOME") or
                    (pathlib.Path(os.environ.get("LOCALAPPDATA") or pathlib.Path.home()) / "AutoRig" / "farm-runner"))
POLL = 15
MAX_RESULT = 60 * 1024 * 1024
IDLE = 0x00000040
NO_WINDOW = 0x08000000
VERSION = "farm-runner/2026-10-11a"


def log(*parts):
    line = time.strftime("%Y-%m-%dT%H:%M:%SZ ", time.gmtime()) + " ".join(str(p) for p in parts)
    try:
        HOME.mkdir(parents=True, exist_ok=True)
        f = HOME / "runner.log"
        if f.exists() and f.stat().st_size > 1_000_000:
            f.replace(HOME / "runner.old.log")
        with f.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def read1(name, default=""):
    try:
        return (HOME / name).read_text().strip()
    except OSError:
        return default


BOX = read1("box.txt")
KEY = read1("key")


def call(method, path, body=None, headers=None, timeout=60, raw=False):
    data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
    req = urllib.request.Request(API + path, data=data, method=method)
    req.add_header("Authorization", "Bearer " + KEY)
    req.add_header("User-Agent", VERSION)
    if data is not None and not (headers or {}).get("Content-Type"):
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        payload = r.read()
    return payload if raw else json.loads(payload.decode() or "{}")


# ------------------------------------------------------------------ is the box busy with its own work?
def _get_json(url, timeout=3):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:                                                  # noqa: BLE001
        return None


class _FT(ctypes.Structure):
    _fields_ = [("lo", ctypes.c_uint32), ("hi", ctypes.c_uint32)]


def _cpu_times():
    if os.name != "nt":
        return None
    idle, kern, user = _FT(), _FT(), _FT()
    ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kern), ctypes.byref(user))
    f = lambda t: (t.hi << 32) | t.lo                                  # noqa: E731
    return f(idle), f(kern) + f(user)


_last_cpu = [None]


def cpu_busy_percent():
    cur = _cpu_times()
    prev, _last_cpu[0] = _last_cpu[0], cur
    if not cur or not prev:
        return 0
    didle, dtotal = cur[0] - prev[0], cur[1] - prev[1]
    return round(100 * (1 - didle / dtotal)) if dtotal > 0 else 0


def box_busy(own_load_ok_percent=70):
    """'' when free, else a short reason."""
    st = _get_json("http://127.0.0.1:7000/api-converter-glb/server-status")
    if st:
        n = len(st.get("processing_tasks") or []) + len(st.get("pending_tasks") or []) + int(st.get("queue_size") or 0)
        wc = st.get("workload_control") or {}
        if n or st.get("maintenance") or (wc.get("active") or None):
            return "converter busy"
        if (st.get("hunyuan") or {}).get("active_task"):
            return "hunyuan busy"
    for port in (8488, 8188, 8588, 8988, 8288, 8289):
        q = _get_json(f"http://127.0.0.1:{port}/queue", 2)
        if q and (q.get("queue_running") or q.get("queue_pending")):
            return f"comfyui busy :{port}"
    cpu = cpu_busy_percent()
    if cpu >= own_load_ok_percent and not RUNNING:
        return f"cpu {cpu}%"
    return ""


RUNNING = []                                                           # job ids in flight


# ------------------------------------------------------------------ one job
def bundle_dir(name, rev):
    root = HOME / "bundles" / name
    d = root / rev
    if (d / ".ok").exists():
        return d
    data = call("GET", f"/bundle/{name}.zip", raw=True, timeout=180)
    tmp = root / (rev + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    zp = tmp / "b.zip"
    zp.write_bytes(data)
    with zipfile.ZipFile(zp) as z:
        z.extractall(tmp)
    zp.unlink()
    (tmp / ".ok").write_text(rev)
    shutil.rmtree(d, ignore_errors=True)
    tmp.rename(d)
    for old in root.iterdir():                                         # keep the newest 2
        if old.name != rev and old.is_dir() and old.stat().st_mtime < time.time() - 3600:
            shutil.rmtree(old, ignore_errors=True)
    return d


def run_job(spec):
    jid = spec["id"]
    t0 = time.time()
    work = HOME / "work" / jid
    shutil.rmtree(work, ignore_errors=True)
    inp, out = work / "in", work / "out"
    inp.mkdir(parents=True)
    out.mkdir(parents=True)
    status, exit_code, error = "error", 1, ""
    stop = threading.Event()

    def beat():
        while not stop.wait(60):
            try:
                if call("POST", f"/jobs/{jid}/heartbeat", {}, timeout=20).get("state") == "cancelled":
                    log("job", jid, "cancelled by the queue")
            except Exception:                                          # noqa: BLE001
                pass

    threading.Thread(target=beat, daemon=True).start()
    try:
        bundles = {}
        for bname, brev in (spec.get("bundle_revs") or {}).items():
            bundles[bname] = str(bundle_dir(bname, brev))
        bundle = next(iter(bundles.values()), "")
        for name in (spec.get("inputs") or {}):
            (inp / name).write_bytes(call("GET", f"/jobs/{jid}/input/{name}", raw=True, timeout=300))
        sub = {"{python}": sys.executable, "{bundle}": bundle, "{in}": str(inp), "{out}": str(out)}
        sub.update({"{bundle_" + k + "}": v for k, v in bundles.items()})
        cmd = []
        for part in spec["cmd"]:
            for k, v in sub.items():
                part = part.replace(k, v)
            cmd.append(part)
        env = dict(os.environ)
        env.update({str(k): str(v) for k, v in (spec.get("env") or {}).items()})
        env.update(PYTHONPATH=os.pathsep.join(list(bundles.values()) + [env.get("PYTHONPATH", "")]), OMP_NUM_THREADS="2",
                   OPENBLAS_NUM_THREADS="2", MKL_NUM_THREADS="2", PYTHONUTF8="1", FARM_JOB_ID=jid,
                   FARM_BOX=BOX)
        log("job", jid, spec.get("kind"), "start")
        with open(out / "_stdout.txt", "wb") as so:
            p = subprocess.Popen(cmd, cwd=str(out), env=env, stdout=so, stderr=subprocess.STDOUT,
                                 creationflags=(IDLE | NO_WINDOW) if os.name == "nt" else 0)
            try:
                exit_code = p.wait(timeout=float(spec.get("timeout") or 900))
            except subprocess.TimeoutExpired:
                subprocess.run(["taskkill", "/PID", str(p.pid), "/T", "/F"], capture_output=True) if os.name == "nt" \
                    else p.kill()
                exit_code, error = 124, "timeout"
        status = "ok" if exit_code == 0 else "error"
        if exit_code and not error:
            error = f"exit {exit_code}"
    except Exception as exc:                                           # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"[:300]
    finally:
        stop.set()
    zp = work / "result.zip"
    with zipfile.ZipFile(zp, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(out.rglob("*")):
            if f.is_file():
                z.write(f, str(f.relative_to(out)))
    data = zp.read_bytes() if zp.stat().st_size <= MAX_RESULT else b""
    if not data:
        status, error = "error", "result larger than 60 MB"
    try:
        call("POST", f"/jobs/{jid}/finish", data, {"Content-Type": "application/zip", "X-Status": status,
             "X-Exit": str(exit_code), "X-Error": error.replace("\n", " "), "X-Seconds": str(round(time.time() - t0, 1))},
             timeout=300)
    except Exception as exc:                                           # noqa: BLE001
        log("job", jid, "finish failed", exc)
    log("job", jid, status, error, round(time.time() - t0, 1), "s")
    shutil.rmtree(work, ignore_errors=True)


def main():
    if not BOX or not KEY:
        log("box.txt / key missing in", HOME)
        sys.exit(1)
    lock = socket.socket()
    try:
        lock.bind(("127.0.0.1", 47262))
    except OSError:
        sys.exit(0)                                                    # another runner is alive
    log(VERSION, "box", BOX, "python", sys.version.split()[0])
    shutil.rmtree(HOME / "work", ignore_errors=True)
    cpu_busy_percent()
    while True:
        try:
            busy = "" if RUNNING else box_busy()
            r = call("POST", "/next", {"busy": busy, "running": RUNNING, "info": {"v": VERSION, "py": sys.version.split()[0]}})
            job = r.get("job")
            if job:
                RUNNING.append(job["id"])
                try:
                    run_job(job)
                finally:
                    RUNNING.remove(job["id"])
                continue
        except urllib.error.HTTPError as exc:
            log("api", exc.code)
        except Exception as exc:                                       # noqa: BLE001
            log("loop", repr(exc)[:200])
        time.sleep(POLL)


if __name__ == "__main__":
    main()

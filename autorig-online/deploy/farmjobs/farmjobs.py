#!/usr/bin/env python3
"""AutoRig farm jobs: a light pull queue on the VPS, the heavy compute on the farm boxes.

Owner order 2026-10-11: "distribute everything to the farm". Customer tasks come first, so the census, autotex
bake/guides and other background compute are queued here and PULLED by farm_runner.py on the boxes (which yields to
the box's own converter / ComfyUI work). The VPS only stores the queue, serves inputs and code bundles and keeps the
results.

  service   python3 farmjobs.py serve            127.0.0.1:8262, nginx: /api/farmjobs/ (box key)
  queue     python3 farmjobs.py enqueue job.json | status | jobs [state] | result <id> <out.zip> | cancel <id>
            python3 farmjobs.py newkey <box> [concurrency]     prints a box key once (only its sha256 is stored)
  library   import farmjobs; q = farmjobs.Queue(); jid = q.enqueue({...}); q.wait(jid, timeout)

A job spec (JSON):
  kind        free label ("census_one", "autotex_bake", ...)           priority  higher first (default 0)
  bundle      name of a code bundle (BUNDLES below): the box downloads and caches it by revision
  cmd         ["{python}", "-P", "-m", "mt.census_farm", "{in}", "{out}"]   placeholders {python} {bundle} {in} {out}
  inputs      {"name": "/abs/path/on/the/vps"}  (only under ALLOWED_INPUT_ROOTS), served to the box into {in}
  env         extra environment, timeout seconds (default 900), boxes: optional allow-list, max_attempts (default 2)
The box runs cmd (cmd[0] must be "{python}") with cwd {out}; everything the command leaves in {out} (<= 64 MB) comes
back as a zip: results/<id>.zip. Nothing here touches a customer task: the VPS side only reads the files it is told to.
"""
from __future__ import annotations

import hashlib
import hmac
import io
import json
import os
import pathlib
import sqlite3
import sys
import threading
import time
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

HOME = pathlib.Path(os.environ.get("FARMJOBS_HOME", "/srv/autorig/data/farmjobs"))
DB = HOME / "jobs.sqlite3"
RESULTS = HOME / "results"
KEYS = pathlib.Path(os.environ.get("FARMJOBS_KEYS", "/srv/autorig/secrets/farmjobs.json"))
BUNDLES = {                      # name -> (root, [sub paths], suffixes shipped)
    "mt": ("/srv/autorig/data/motion_transfer", ["mt"], (".py", ".json", ".txt")),
    "census": ("/srv/autorig/census", ["census.py"], (".py",)),
    "backend_min": ("/srv/autorig/current/autorig-online/backend", ["fbx_ascii.py", "v3_intake.py"], (".py",)),
}
ALLOWED_INPUT_ROOTS = ("/srv/autorig/data/",)
DENY_INPUT_PARTS = ("/secrets", "/db/", ".sqlite")
LEASE_SECONDS = 600
MAX_RESULT = 64 * 1024 * 1024
LOCK = threading.Lock()


def _now() -> float:
    return round(time.time(), 2)


def _db() -> sqlite3.Connection:
    HOME.mkdir(parents=True, exist_ok=True)
    RESULTS.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB, timeout=20, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, kind TEXT, priority INT DEFAULT 0, state TEXT,
        box TEXT, spec TEXT, created REAL, leased_at REAL, lease_until REAL, finished REAL, attempts INT DEFAULT 0,
        exit_code INT, error TEXT, seconds REAL, result_bytes INT, note TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state, priority DESC, created)")
    con.execute("""CREATE TABLE IF NOT EXISTS boxes(box TEXT PRIMARY KEY, seen REAL, busy TEXT, running TEXT, info TEXT,
        done INT DEFAULT 0, failed INT DEFAULT 0)""")
    return con


class Queue:
    """The VPS side as a library: enqueue / get / wait for a result."""

    def enqueue(self, spec: dict) -> str:
        check_spec(spec)
        jid = uuid.uuid4().hex[:20]
        with LOCK:
            _db().execute("INSERT INTO jobs(id,kind,priority,state,spec,created) VALUES(?,?,?,?,?,?)",
                          (jid, str(spec.get("kind") or "job")[:40], int(spec.get("priority") or 0), "queued",
                           json.dumps(spec), _now()))
        return jid

    def get(self, jid: str):
        r = _db().execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
        return dict(r) if r else None

    def result_path(self, jid: str) -> pathlib.Path:
        return RESULTS / f"{jid}.zip"

    def wait(self, jid: str, timeout: float = 3600, poll: float = 5):
        end = time.time() + timeout
        while time.time() < end:
            row = self.get(jid)
            if row and row["state"] in ("done", "error", "cancelled"):
                return row
            time.sleep(poll)
        return self.get(jid)

    def cancel(self, jid: str) -> None:
        _db().execute("UPDATE jobs SET state='cancelled', finished=? WHERE id=? AND state IN ('queued','leased')",
                      (_now(), jid))


def _bundles_of(spec: dict) -> list:
    return list(spec.get("bundles") or ([spec["bundle"]] if spec.get("bundle") else []))


def check_spec(spec: dict) -> None:
    cmd = spec.get("cmd")
    if not isinstance(cmd, list) or not cmd or cmd[0] != "{python}" or not all(isinstance(x, str) for x in cmd):
        raise ValueError('cmd must be a list of strings starting with "{python}"')
    for b in _bundles_of(spec):
        if b not in BUNDLES:
            raise ValueError("unknown bundle")
    for name, path in (spec.get("inputs") or {}).items():
        real = os.path.realpath(path)
        if not real.startswith(ALLOWED_INPUT_ROOTS) or any(p in real for p in DENY_INPUT_PARTS):
            raise ValueError(f"input {name}: path not allowed")
        if "/" in name or "\\" in name or name.startswith("."):
            raise ValueError("input name")


# ------------------------------------------------------------------ bundles
_BUNDLE_CACHE: dict = {}


def bundle_files(name: str):
    root, subs, suffixes = BUNDLES[name]
    out = []
    for sub in subs:
        p = pathlib.Path(root) / sub
        files = [p] if p.is_file() else sorted(p.rglob("*"))
        for f in files:
            if f.is_file() and f.suffix in suffixes and "__pycache__" not in f.parts and "static" not in f.parts:
                out.append(f)
    return root, out


def bundle_rev(name: str) -> str:
    root, files = bundle_files(name)
    h = hashlib.sha256()
    for f in files:
        st = f.stat()
        h.update(f"{f.relative_to(root)}:{st.st_size}:{int(st.st_mtime)}".encode())
    return h.hexdigest()[:16]


def bundle_zip(name: str):
    rev = bundle_rev(name)
    hit = _BUNDLE_CACHE.get(name)
    if hit and hit[0] == rev:
        return hit
    root, files = bundle_files(name)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, str(f.relative_to(root)))
    _BUNDLE_CACHE[name] = (rev, buf.getvalue())
    return _BUNDLE_CACHE[name]


# ------------------------------------------------------------------ HTTP (box side)
def _keys() -> dict:
    try:
        return json.loads(KEYS.read_text())
    except (OSError, ValueError):
        return {}


def auth_box(handler):
    token = handler.headers.get("Authorization", "")
    token = token[7:].strip() if token.lower().startswith("bearer ") else ""
    if not token:
        return None
    digest = hashlib.sha256(token.encode()).hexdigest()
    for box, cfg in (_keys().get("boxes") or {}).items():
        if hmac.compare_digest(str(cfg.get("sha256") or ""), digest) and not cfg.get("disabled"):
            return box
    return None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "autorig-farmjobs/1"

    def log_message(self, *a):
        pass

    def _send(self, code: int, body: bytes = b"", ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj):
        self._send(code, json.dumps(obj).encode())

    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length") or 0)
        if n > MAX_RESULT + 4096:
            raise ValueError("too large")
        return self.rfile.read(n) if n else b""

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def route(self, method: str):
        try:
            u = urlparse(self.path)
            parts = [p for p in u.path.split("/") if p]
            if parts[:2] != ["api", "farmjobs"]:
                return self._json(404, {"error": "not found"})
            parts = parts[2:]
            if method == "GET" and parts == ["status"]:
                return self._json(200, status())
            box = auth_box(self)
            if not box:
                self._body()
                return self._json(401, {"error": "box key required"})
            cfg = (_keys().get("boxes") or {}).get(box) or {}
            if method == "POST" and parts == ["next"]:
                req = json.loads(self._body() or b"{}")
                return self._json(200, lease_next(box, cfg, req))
            if method == "GET" and len(parts) == 3 and parts[0] == "bundle" and parts[2] == "rev":
                return self._json(200, {"rev": bundle_rev(parts[1])})
            if method == "GET" and len(parts) == 2 and parts[0] == "bundle":
                rev, data = bundle_zip(parts[1].removesuffix(".zip"))
                return self._send(200, data, "application/zip")
            if len(parts) >= 3 and parts[0] == "jobs":
                jid = parts[1]
                row = _db().execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
                if not row or row["box"] != box:
                    self._body()
                    return self._json(404, {"error": "not your job"})
                if method == "GET" and parts[2] == "input" and len(parts) == 4:
                    spec = json.loads(row["spec"])
                    path = (spec.get("inputs") or {}).get(parts[3])
                    if not path:
                        return self._json(404, {"error": "no such input"})
                    return self._send(200, pathlib.Path(path).read_bytes(), "application/octet-stream")
                if method == "POST" and parts[2] == "heartbeat":
                    self._body()
                    _db().execute("UPDATE jobs SET lease_until=? WHERE id=? AND state='leased'",
                                  (_now() + LEASE_SECONDS, jid))
                    st = _db().execute("SELECT state FROM jobs WHERE id=?", (jid,)).fetchone()["state"]
                    return self._json(200, {"state": st})
                if method == "POST" and parts[2] == "finish":
                    data = self._body()
                    ok = self.headers.get("X-Status") == "ok"
                    if data:
                        tmp = RESULTS / f"{jid}.zip.tmp"
                        tmp.write_bytes(data)
                        os.replace(tmp, RESULTS / f"{jid}.zip")
                    with LOCK:
                        _db().execute("UPDATE jobs SET state=?, finished=?, exit_code=?, error=?, seconds=?, "
                                      "result_bytes=? WHERE id=?",
                                      ("done" if ok else "error", _now(), int(self.headers.get("X-Exit") or 0),
                                       (self.headers.get("X-Error") or "")[:400],
                                       float(self.headers.get("X-Seconds") or 0), len(data), jid))
                        _db().execute("UPDATE boxes SET done=done+?, failed=failed+? WHERE box=?",
                                      (1 if ok else 0, 0 if ok else 1, box))
                    return self._json(200, {"ok": True})
            return self._json(404, {"error": "not found"})
        except Exception as exc:                                      # noqa: BLE001
            try:
                self._json(500, {"error": repr(exc)[:200]})
            except OSError:
                pass


def lease_next(box: str, cfg: dict, req: dict) -> dict:
    con = _db()
    busy = str(req.get("busy") or "")[:160]
    running = [str(x) for x in (req.get("running") or [])][:8]
    with LOCK:
        con.execute("INSERT INTO boxes(box,seen,busy,running,info) VALUES(?,?,?,?,?) ON CONFLICT(box) DO UPDATE SET "
                    "seen=excluded.seen, busy=excluded.busy, running=excluded.running, info=excluded.info",
                    (box, _now(), busy, json.dumps(running), json.dumps(req.get("info") or {})[:600]))
        # leases that ran out go back to the queue (or fail after max attempts)
        con.execute("UPDATE jobs SET state='queued', box=NULL WHERE state='leased' AND lease_until<?", (_now(),))
        if busy or cfg.get("paused"):
            return {"job": None, "reason": "busy" if busy else "paused"}
        if len(running) >= int(cfg.get("concurrency") or 1):
            return {"job": None, "reason": "concurrency"}
        rows = con.execute("SELECT * FROM jobs WHERE state='queued' ORDER BY priority DESC, created LIMIT 40").fetchall()
        for r in rows:
            spec = json.loads(r["spec"])
            if spec.get("boxes") and box not in spec["boxes"]:
                continue
            if r["attempts"] >= int(spec.get("max_attempts") or 2):
                con.execute("UPDATE jobs SET state='error', error='attempts exhausted', finished=? WHERE id=?",
                            (_now(), r["id"]))
                continue
            con.execute("UPDATE jobs SET state='leased', box=?, leased_at=?, lease_until=?, attempts=attempts+1 "
                        "WHERE id=?", (box, _now(), _now() + LEASE_SECONDS, r["id"]))
            spec["id"] = r["id"]
            spec["bundle_revs"] = {b: bundle_rev(b) for b in _bundles_of(spec)}
            return {"job": spec}
    return {"job": None, "reason": "empty"}


def status() -> dict:
    con = _db()
    counts = {r["state"]: r["n"] for r in con.execute("SELECT state, COUNT(*) n FROM jobs GROUP BY state")}
    boxes = []
    for r in con.execute("SELECT * FROM boxes ORDER BY box"):
        boxes.append({"box": r["box"], "seen_age_seconds": round(_now() - r["seen"], 1), "busy": r["busy"],
                      "running": json.loads(r["running"] or "[]"), "done": r["done"], "failed": r["failed"]})
    kinds = {r["kind"]: r["n"] for r in con.execute(
        "SELECT kind, COUNT(*) n FROM jobs WHERE state='queued' GROUP BY kind")}
    return {"counts": counts, "queued_by_kind": kinds, "boxes": boxes, "generated": _now()}


def serve(port: int = 8262):
    _db()
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main(argv=None):
    a = list(argv if argv is not None else sys.argv[1:])
    cmd = a[0] if a else "status"
    q = Queue()
    if cmd == "serve":
        serve()
    elif cmd == "enqueue":
        print(q.enqueue(json.loads(pathlib.Path(a[1]).read_text())))
    elif cmd == "status":
        print(json.dumps(status(), indent=1))
    elif cmd == "jobs":
        rows = _db().execute("SELECT id,kind,state,box,attempts,seconds,error FROM jobs " +
                             ("WHERE state=? " if len(a) > 1 else "") + "ORDER BY created DESC LIMIT 30",
                             tuple(a[1:2])).fetchall()
        for r in rows:
            print(dict(r))
    elif cmd == "result":
        pathlib.Path(a[2]).write_bytes(q.result_path(a[1]).read_bytes())
    elif cmd == "cancel":
        q.cancel(a[1])
    elif cmd == "newkey":
        key = uuid.uuid4().hex + uuid.uuid4().hex
        d = _keys() or {"boxes": {}}
        d.setdefault("boxes", {})[a[1]] = {"sha256": hashlib.sha256(key.encode()).hexdigest(),
                                           "concurrency": int(a[2]) if len(a) > 2 else 1}
        tmp = KEYS.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, indent=1))
        os.replace(tmp, KEYS)
        print(key)


if __name__ == "__main__":
    main()

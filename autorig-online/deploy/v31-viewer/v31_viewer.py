"""V3.1 viewer service: the part relabel, the fixture stream and the replay preference (Viewer V3.1, 2026-10-17).

Owner 2026-10-17: «визуализация найденных предметов одежды, обычных предметов, бороды, волос, класс персонажа — всё это
должно отображаться во вьювере по мере анализа интерактивно и последовательно, а при загрузке страницы проигрываться
последовательно». Spec: MT docs/v31-viewer-visualisation-spec-20261017.md. The task page draws the stages
(static/js/task-v31-stages.js); this small service holds the three things it writes or that are not in the run yet:

    GET  /api/mt/runs/{run}/v31/fixture?cursor=&sim=&t0=  the fixture event log of a run (built by build_fixture.py),
                                                          read by mt.live.read_events, the reader of the live route;
                                                          sim=<speed> with t0=<epoch s> releases the events at the
                                                          pace they were emitted (a live run, replayed in real time)
    GET  /api/mt/runs/{run}/v31/fx/{file}                 the fixture's payloads (ARV1 part masks)
    GET  /api/mt/runs/{run}/parts/labels                  {version, labels: {part: label}, versions: [...]}
    POST /api/mt/runs/{run}/parts/{part}/label {label}    the task owner or an administrator: a new analysis version
                                                          analysis/part_labels/v<N>.json (+ part_labels.json, the
                                                          current one); the previous versions stay; a live event
                                                          analysis/relabel (key live_part_relabel) in the run's log
    GET  /api/mt/v31/prefs   POST /api/mt/v31/prefs {replay: bool}   the person's own (mt.viewer_prefs people file)

It never writes anything else in a run, and never touches the run's own analysis files. 127.0.0.1:8277,
autorig-v31-viewer.service, nested nginx locations inside `location ^~ /api/mt/`.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pathlib
import re
import secrets
import sys
import threading
import time

MT = pathlib.Path(os.environ.get("MT_ROOT", "/srv/autorig/data/motion_transfer"))
sys.path.insert(0, str(MT))
from fastapi import FastAPI, HTTPException, Request  # noqa: E402
from fastapi.responses import FileResponse, JSONResponse  # noqa: E402

from mt import live  # noqa: E402  (read_events: the same reader as GET /api/mt/runs/{run}/live)

RUNS = MT / "runs"
FIX = pathlib.Path(os.environ.get("V31_FIXTURES", "/srv/autorig/data/v31-fixtures"))
PEOPLE = MT / "viewer_prefs" / "people"
KEYS = pathlib.Path("/srv/autorig/secrets/mt-api-keys.json")
SITE = "http://127.0.0.1:8200"
RUN = re.compile(r"^[0-9a-f]{20}$")
PART = re.compile(r"^[a-z][a-z0-9_:-]{0,47}$")
FILE = re.compile(r"^[A-Za-z0-9_.-]{1,120}\.(bin|json|png)$")
# the contract labels (rig contract V3.1 §1) plus the body's own parts as the analysis names them
LABELS = {"body", "torso", "head", "hair", "beard", "hat", "glove", "mitten", "coat", "cloak", "skirt", "boot", "wing",
          "tail", "ear", "arm", "leg", "hand", "foot", "prop"}
_LOCK = threading.Lock()
_ME: dict[str, tuple[float, dict]] = {}
_HITS: dict[str, list] = {}

app = FastAPI(title="autorig-v31-viewer", docs_url=None, redoc_url=None, openapi_url=None)


def _read(path, default=None):
    try:
        return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _write(path, doc) -> None:
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def _no_store(doc, status=200):
    return JSONResponse(doc, status_code=status, headers={"Cache-Control": "no-store"})


def _err(status, code, message):
    return _no_store({"detail": {"error_string": code, "message_string": message}}, status)


def _run_dir(run: str) -> pathlib.Path:
    d = RUNS / run
    if not RUN.fullmatch(run) or not d.is_dir():
        raise HTTPException(404, "no such run")
    return d


def _limited(request: Request, cap: int) -> bool:
    ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "?")
    now = time.time()
    hits = [t for t in _HITS.get(ip, []) if now - t < 60]
    _HITS[ip] = hits + [now]
    if len(_HITS) > 20000:
        _HITS.clear()
    return len(hits) >= cap


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:24]


async def _me(request: Request) -> dict:
    """{key, kind: account|anon|key|local|none, admin, id} the way mt.viewer_prefs identifies a person."""
    auth = request.headers.get("authorization", "")
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if token:
        digest = hashlib.sha256(token.encode()).hexdigest()
        for k in (_read(KEYS, {}) or {}).get("keys", []):
            if secrets.compare_digest(k.get("sha256", ""), digest):
                return {"key": "", "kind": "key", "admin": True, "id": k.get("name", "key")}
        raise HTTPException(403, "unknown key")
    host = request.client.host if request.client else ""
    if host in ("127.0.0.1", "::1") and not request.headers.get("x-real-ip"):
        return {"key": "", "kind": "local", "admin": True, "id": "local"}
    cookie = request.cookies.get("session")
    if cookie:
        k = _sha(cookie)
        hit = _ME.get(k)
        if not hit or time.time() - hit[0] > 60:
            import httpx
            try:
                async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
                    r = await client.get(f"{SITE}/auth/me", cookies={"session": cookie})
                me = r.json() if r.status_code == 200 else {}
            except (httpx.HTTPError, ValueError):
                me = {}
            user = (me or {}).get("user") or {}
            val = {"id": user.get("id"), "admin": user.get("is_admin") is True} \
                if me.get("authenticated") is True and user.get("id") not in (None, "") else {}
            hit = (time.time(), val)
            _ME[k] = hit
            if len(_ME) > 5000:
                _ME.clear()
        if hit[1].get("id") is not None:
            return {"key": "u" + _sha("autorig-user:" + str(hit[1]["id"])), "kind": "account",
                    "admin": hit[1]["admin"], "id": f"user:{hit[1]['id']}"}
    anon = request.cookies.get("anon_id") or ""
    if re.fullmatch(r"[A-Za-z0-9_-]{8,80}", anon):
        return {"key": "a" + _sha("autorig-anon:" + anon), "kind": "anon", "admin": False, "id": "anon"}
    return {"key": "", "kind": "none", "admin": False, "id": ""}


async def _is_task_owner(request: Request, task_id: str) -> bool:
    if not re.fullmatch(r"[0-9a-f-]{36}", task_id or ""):
        return False
    import httpx
    cookies = {k: v for k, v in request.cookies.items() if k in ("session", "anon_id")}
    try:
        async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
            r = await client.get(f"{SITE}/api/task/{task_id}", cookies=cookies)
        return r.status_code == 200 and r.json().get("is_owner") is True
    except (httpx.HTTPError, ValueError):
        return False


# ------------------------------------------------------------------------------------------------ the fixture stream

@app.get("/api/mt/runs/{run}/v31/fixture")
async def fixture(run: str, cursor: int = 0, sim: float = 0.0, t0: float = 0.0):
    d = FIX / run
    if not RUN.fullmatch(run) or not (d / live.DIR / live.EVENTS).is_file():
        raise HTTPException(404, "no fixture for this run")
    events, nxt, reset = await asyncio.to_thread(live.read_events, d, cursor)
    done = True
    if sim > 0 and t0 > 0 and events:                    # released at the pace they were emitted, sped up by `sim`
        first = _read(d / "first_t.json") or {}
        start = float(first.get("t") or 0)
        if not start:
            head, _, _ = await asyncio.to_thread(live.read_events, d, 0, 4096)
            start = float(head[0]["t"]) if head else 0
            _write(d / "first_t.json", {"t": start})
        horizon = start + (time.time() - t0) * sim
        cut = next((i for i, e in enumerate(events) if float(e.get("t") or 0) > horizon), None)
        if cut is not None:
            nxt = events[cut]["id"]
            events = events[:cut]
            done = False
    elif events and nxt < (d / live.DIR / live.EVENTS).stat().st_size:
        done = False
    st = _read(d / "phases.json") or {}
    return _no_store({"schema": live.SCHEMA, "run_id": run, "fixture": True, "cursor": nxt, "reset": reset,
                      "events": events, "done": done, "state": {"status": st.get("status") if done else "running"}})


@app.get("/api/mt/runs/{run}/v31/fx/{name}")
async def fixture_file(run: str, name: str):
    p = FIX / run / live.DIR / name
    if not RUN.fullmatch(run) or not FILE.fullmatch(name) or not p.is_file():
        raise HTTPException(404, "no such file")
    return FileResponse(p, media_type="application/octet-stream", headers={"Cache-Control": "public, max-age=300"})


# ------------------------------------------------------------------------------------------------ part labels

def _known_parts(d: pathlib.Path, run: str) -> dict[str, str]:
    """part id -> its label now: analysis/parts.json, the run's live_parts / live_garments / live_props events, the
    fixture's (a fixture run is relabelled as the run itself)."""
    out: dict[str, str] = {}
    for p in (_read(d / "analysis" / "parts.json", {}) or {}).get("parts", []):
        if isinstance(p, dict) and p.get("id"):
            out[str(p["id"])] = str(p.get("label") or p["id"])
    for path in (d / live.DIR / live.EVENTS, FIX / run / live.DIR / live.EVENTS):
        try:
            with open(path, "rb") as f:
                for raw in f:
                    if b'"live_parts"' not in raw and b'"live_garments"' not in raw and b'"live_props"' not in raw:
                        continue
                    try:
                        data = json.loads(raw).get("data") or {}
                    except ValueError:
                        continue
                    if data.get("id"):
                        out[str(data["id"])] = str(data.get("label") or data["id"])
        except OSError:
            continue
    return out


def _labels_doc(d: pathlib.Path) -> dict:
    return _read(d / "analysis" / "part_labels.json") or {"schema": "autorig.mt.part-labels/1", "version": 0,
                                                            "labels": {}, "versions": []}


@app.get("/api/mt/runs/{run}/parts/labels")
async def labels(run: str):
    d = _run_dir(run)
    doc = _labels_doc(d)
    return _no_store({**doc, "parts": _known_parts(d, run), "allowed": sorted(LABELS) + ["prop:<name>"]})


@app.post("/api/mt/runs/{run}/parts/{part}/label")
async def relabel(run: str, part: str, request: Request):
    d = _run_dir(run)
    if _limited(request, 30):
        return _err(429, "too_many_requests", "too many relabels, wait a minute")
    try:
        body = await request.json()
    except ValueError:
        body = {}
    new = str((body or {}).get("label") or "").strip().lower()
    if not PART.fullmatch(part):
        return _err(422, "bad_part", "bad part id")
    if not (new in LABELS or re.fullmatch(r"prop:[a-z][a-z0-9_-]{0,31}", new)):
        return _err(422, "bad_label", "the label is not in the rig contract")
    me = await _me(request)
    task_id = str((_read(d / "phases.json", {}) or {}).get("task_id") or "")
    if not me["admin"] and not await _is_task_owner(request, task_id):
        return _err(403, "owner_only", "only the task owner or an administrator relabels parts")
    known = _known_parts(d, run)
    if part not in known:
        return _err(404, "no_such_part", "no such part in this run's analysis")
    with _LOCK:
        doc = _labels_doc(d)
        before = doc["labels"].get(part) or known[part]
        version = int(doc.get("version") or 0) + 1
        change = {"version": version, "at": round(time.time(), 1), "by": me["id"] or me["kind"], "part": part,
                  "from": before, "to": new, "previous": version - 1}
        labels_now = {**doc["labels"], part: new}
        snap = {"schema": "autorig.mt.part-labels/1", "run": run, "version": version, "labels": labels_now,
                "change": change, "base": "analysis/parts.json"}
        _write(d / "analysis" / "part_labels" / f"v{version}.json", snap)
        doc.update(run=run, version=version, labels=labels_now, versions=(doc.get("versions") or []) + [change])
        _write(d / "analysis" / "part_labels.json", doc)
    live.of(d).emit("analysis", "relabel", key="live_part_relabel", params={"part": part, "from": before, "to": new},
                    data={"version": version, "id": part, "label": new, "from": before})
    return _no_store({"ok": True, "version": version, "part": part, "from": before, "label": new,
                      "labels": labels_now})


# ------------------------------------------------------------------------------------------------ replay preference

@app.get("/api/mt/v31/prefs")
async def prefs(request: Request):
    me = await _me(request)
    doc = _read(PEOPLE / f"{me['key']}.json", {}) if me["key"] else {}
    return _no_store({"person": me["kind"], "replay": (doc or {}).get("v31_replay", True)})


@app.post("/api/mt/v31/prefs")
async def prefs_save(request: Request):
    if _limited(request, 40):
        return _err(429, "too_many_requests", "too many saves")
    try:
        body = await request.json()
    except ValueError:
        body = {}
    if not isinstance((body or {}).get("replay"), bool):
        return _err(422, "bad_value", "replay must be true or false")
    me = await _me(request)
    if not me["key"]:
        return _no_store({"ok": True, "saved": False})
    from mt import viewer_prefs                           # the same people file as the scene and the weather
    await asyncio.to_thread(viewer_prefs.save_person, me["key"], v31_replay=body["replay"])
    return _no_store({"ok": True, "saved": True, "person": me["kind"], "replay": body["replay"]})


@app.get("/api/mt/v31/health")
async def health():
    return {"ok": True, "fixtures": sorted(p.name for p in FIX.iterdir() if p.is_dir()) if FIX.is_dir() else []}

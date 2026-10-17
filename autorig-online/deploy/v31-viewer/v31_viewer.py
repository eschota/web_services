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
    POST /api/mt/v31/telemetry                            the task page's frame-rate windows per stage (viewer-telemetry.js),
                                                          one JSON line per batch in /srv/autorig/data/var/viewer_telemetry/
    GET  /api/mt/v31/telemetry/summary?days=1&format=text administrators / local: per stage median / p10 fps, per device
                                                          class, the worst freezes

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


# ------------------------------------------------------------------------------------------------ viewer telemetry
# Owner 2026-10-17: «просто сделай чтобы вьювер логировал каждую сессию фпс в разных стадиях на сервер». The task page
# (static/js/viewer-telemetry.js) posts 1 s frame-rate windows tagged with the stage on screen; one JSON line per batch
# in /srv/autorig/data/var/viewer_telemetry/YYYY-MM-DD.jsonl (UTC). Stored: the ids the viewer already uses (task, run,
# a random session id), the person's viewer-prefs key (a hash, as mt.viewer_prefs makes it) and kind, the device class
# the page reports, the builds, the gate decisions and the windows. No IP, no user agent string, no cookies.
TEL_DIR = pathlib.Path(os.environ.get("V31_TELEMETRY_DIR", "/srv/autorig/data/var/viewer_telemetry"))
TEL_MAX = 96 * 1024
_TEL_LOCK = threading.Lock()
STAGE = re.compile(r"^[a-z0-9_:]{1,40}$")


def _num(v, lo=0.0, hi=1e6):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return round(min(hi, max(lo, f)), 2) if f == f else None


def _clean_tel(body: dict) -> dict:
    """Only the known fields, bounded."""
    def s(v, n=80):
        return str(v)[:n] if v is not None else ""
    wins = []
    for w in (body.get("windows") or [])[:400]:
        if not isinstance(w, list) or len(w) < 7 or not STAGE.fullmatch(str(w[1] or "")):
            continue
        wins.append([_num(w[0], 0, 864000), str(w[1]), int(_num(w[2], 0, 10000) or 0), _num(w[3], 0, 1000),
                     _num(w[4], 0, 1000), _num(w[5], 0, 1000), int(_num(w[6], 0, 600000) or 0)])
    dev = body.get("device") if isinstance(body.get("device"), dict) else None
    if dev:
        dev = {"gpu": s(dev.get("gpu"), 160), "cores": _num(dev.get("cores"), 0, 1024), "memory_gb": _num(dev.get("memory_gb"), 0, 4096),
               "screen": s(dev.get("screen"), 24), "viewport": s(dev.get("viewport"), 24), "mobile": bool(dev.get("mobile")),
               "browser": s(dev.get("browser"), 40), "platform": s(dev.get("platform"), 24)}
    gate = [[_num(g[0], 0, 864000000), s(g[1], 24), s(g[2], 120)] for g in (body.get("gate") or [])[:60]
            if isinstance(g, list) and len(g) >= 3]
    task = s(body.get("task_id"), 36)
    run = s(body.get("run_id"), 20)
    builds = body.get("builds") if isinstance(body.get("builds"), dict) else {}
    v31 = body.get("v31") if isinstance(body.get("v31"), dict) else None
    env = body.get("env") if isinstance(body.get("env"), dict) else None
    return {"sid": s(body.get("sid"), 32), "seq": int(_num(body.get("seq"), 0, 100000) or 0), "final": bool(body.get("final")),
            "task_id": task if re.fullmatch(r"[0-9a-f-]{36}", task) else "", "run_id": run if RUN.fullmatch(run) else "",
            "page": s(body.get("page"), 40), "builds": {k: s(builds.get(k), 60) for k in ("telemetry", "page", "v31", "unity")},
            "flags": body.get("flags") if isinstance(body.get("flags"), dict) and len(json.dumps(body.get("flags"))) < 300 else {},
            "device": dev, "uptime_s": _num(body.get("uptime_s"), 0, 864000),
            "v31": json.loads(json.dumps(v31)[:1500]) if v31 and len(json.dumps(v31)) < 1500 else None,
            "env": env if env and len(json.dumps(env)) < 1500 else None, "gate": gate, "windows": wins}


@app.post("/api/mt/v31/telemetry")
async def telemetry(request: Request):
    if _limited(request, 120):
        return _err(429, "too_many_requests", "too many telemetry batches")
    raw = await request.body()
    if len(raw) > TEL_MAX:
        return _err(413, "too_large", "telemetry batch over 96 KB")
    try:
        body = json.loads(raw.decode("utf-8") or "{}")
    except (ValueError, UnicodeDecodeError):
        return _err(422, "bad_json", "not JSON")
    if not isinstance(body, dict) or body.get("schema") != "autorig.viewer-telemetry/1" or not re.fullmatch(r"[0-9a-f]{8,32}", str(body.get("sid") or "")):
        return _err(422, "bad_batch", "not a viewer telemetry batch")
    me = await _me(request)
    row = {"at": round(time.time(), 1), "person": me["key"] or "", "kind": me["kind"], **_clean_tel(body)}
    line = (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    day = time.strftime("%Y-%m-%d", time.gmtime())
    TEL_DIR.mkdir(parents=True, exist_ok=True)
    with _TEL_LOCK:
        fd = os.open(TEL_DIR / f"{day}.jsonl", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, line)
        finally:
            os.close(fd)
    return _no_store({"ok": True, "windows": len(row["windows"])})


def _gpu_class(gpu: str, mobile: bool) -> str:
    g = (gpu or "").lower()
    m = re.search(r"(rtx|gtx|rx|radeon|arc|iris|uhd|hd graphics|apple m\d|apple gpu|adreno|mali|powervr|swiftshader|llvmpipe)[^,()]*", g)
    name = m.group(0).strip() if m else (g[:40] or "unknown")
    name = re.sub(r"\s+\(0x[0-9a-f]+\)|direct3d.*|vs_\d.*", "", name).strip()
    return ("mobile · " if mobile else "") + name[:48]


def _pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1)


@app.get("/api/mt/v31/telemetry/summary")
async def telemetry_summary(request: Request, days: int = 1, format: str = "json", task: str = ""):
    me = await _me(request)
    if not me["admin"]:
        return _err(403, "admin_only", "the telemetry summary is for administrators")
    days = max(1, min(31, int(days)))
    rows = []
    for k in range(days):
        p = TEL_DIR / (time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400 * k)) + ".jsonl")
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if task and r.get("task_id") != task:
                        continue
                    rows.append(r)
        except OSError:
            continue
    devices, sessions = {}, {}
    for r in rows:
        sid = r.get("sid")
        se = sessions.setdefault(sid, {"task": r.get("task_id"), "run": r.get("run_id"), "windows": 0, "device": None,
                                      "gate": [], "fixture": bool((r.get("flags") or {}).get("fixture"))})
        if r.get("device"):
            se["device"] = r["device"]
        se["gate"] += r.get("gate") or []
    by_stage, by_dev, freezes = {}, {}, []
    for r in rows:
        se = sessions[r.get("sid")]
        dev = se["device"] or {}
        cls = _gpu_class(dev.get("gpu", ""), bool(dev.get("mobile")))
        devices[cls] = devices.get(cls, set()) | {r.get("sid")}
        for w in r.get("windows") or []:
            t, stage, n, fps, p5, fmin, longest = w
            se["windows"] += 1
            st = by_stage.setdefault(stage, {"fps": [], "p5": [], "longest": [], "sessions": set()})
            st["fps"].append(fps or 0); st["p5"].append(p5 or 0); st["longest"].append(longest or 0); st["sessions"].add(r.get("sid"))
            dv = by_dev.setdefault(cls, {}).setdefault(stage.split(":")[0], [])
            dv.append(fps or 0)
            if longest and longest >= 250:
                freezes.append({"ms": longest, "stage": stage, "t_s": t, "task": se["task"], "device": cls, "sid": r.get("sid")})
    out_stage = {k: {"windows": len(v["fps"]), "sessions": len(v["sessions"]), "fps_median": _pct(v["fps"], 0.5),
                     "fps_p10": _pct(v["fps"], 0.1), "fps_p5_median": _pct(v["p5"], 0.5), "longest_frame_max_ms": max(v["longest"] or [0]),
                     "share_under_30": round(sum(1 for x in v["fps"] if x < 30) / max(1, len(v["fps"])), 3)}
                 for k, v in sorted(by_stage.items())}
    out_dev = {k: {"sessions": len(devices.get(k, ())), "stages": {s: {"windows": len(xs), "fps_median": _pct(xs, 0.5), "fps_p10": _pct(xs, 0.1)}
                                                                      for s, xs in sorted(v.items())}}
               for k, v in sorted(by_dev.items())}
    freezes.sort(key=lambda x: -x["ms"])
    doc = {"schema": "autorig.viewer-telemetry-summary/1", "days": days, "batches": len(rows), "sessions": len(sessions),
           "stages": out_stage, "devices": out_dev, "worst_freezes": freezes[:15]}
    if format != "text":
        return _no_store(doc)
    lines = [f"viewer telemetry · last {days} day(s) · {len(sessions)} sessions · {len(rows)} batches",
             "stage                     windows sess  fps_med fps_p10  p5_med  <30fps  worst_ms"]
    for k, v in out_stage.items():
        lines.append(f"{k:<25} {v['windows']:>7} {v['sessions']:>4} {v['fps_median'] or 0:>8} {v['fps_p10'] or 0:>7} "
                     f"{v['fps_p5_median'] or 0:>7} {v['share_under_30']:>7} {v['longest_frame_max_ms']:>9}")
    lines.append("device class                                  sessions  stage: fps_med/p10")
    for k, v in out_dev.items():
        st = "  ".join(f"{s}: {x['fps_median']}/{x['fps_p10']}" for s, x in v["stages"].items())
        lines.append(f"{k:<45} {v['sessions']:>8}  {st}")
    lines.append("worst freezes (ms · stage · t_s · task · device)")
    for fz in freezes[:10]:
        lines.append(f"{fz['ms']:>6} · {fz['stage']} · {fz['t_s']} · {fz['task'][:8]} · {fz['device']}")
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse("\n".join(lines) + "\n", headers={"Cache-Control": "no-store"})

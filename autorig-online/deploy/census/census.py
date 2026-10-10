#!/usr/bin/env python3
"""Model census · V3 (owner 2026-10-11: «Нам надо провести риг всех моделей в базе, рассортировать их по комплекциям
тел, категориям, определить самые популярные и заняться решением именно проблем из этой выборки»).

Every model customers gave the site is rigged with today's V3 rig path in an OFFLINE census mode, sorted by category
and body constitution, and the defects are counted per class. Nothing here touches a customer task, a version, a
gallery or a notification: the site DB and the MT runs are only read, everything is written under CENSUS
(/srv/autorig/data/census), the run dirs are census/runs/<sha[:20]>.

    census.py inventory                   tasks -> available source meshes, de-duplicated by sha256 -> census.sqlite3
    census.py pass1 [--limit N] [--class K] [--stale] [--sha S]
                                          the first pass, one model at a time (a child each), yields to live V3 runs
    census.py one <sha>                   one model (the child): convert, geometry, rig_first.build, fast analysis
                                          (--apply as the conveyor), library clips, arm clearance / limb collision,
                                          the triage defects, the constitution estimate -> runs/<rid>/census.json
    census.py report [--json]             classes (category x constitution) by frequency, pass rate, top defect
    census.py sheet <out.png> [--top N]   a contact sheet of the top classes with sample thumbnails

Run it as a transient unit (nice 19, idle IO, CPU 200 %, 5 GB without swap), e.g.
    sudo systemd-run --unit autorig-census --uid autorig --gid autorig -p Nice=19 -p IOSchedulingClass=idle \
        -p CPUQuota=200% -p MemoryMax=5G -p MemorySwapMax=0 -E PYTHONPATH=/srv/autorig/data/motion_transfer \
        /srv/autorig/venv/bin/python3 -P /srv/autorig/census/census.py pass1
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import traceback
from urllib.parse import unquote, urlparse

MT_ROOT = pathlib.Path(os.environ.get("MT_ROOT", "/srv/autorig/data/motion_transfer"))
CENSUS = pathlib.Path(os.environ.get("AUTORIG_CENSUS", "/srv/autorig/data/census"))
RUNS = CENSUS / "runs"
DB = CENSUS / "census.sqlite3"
SITE_DB = "/srv/autorig/data/db/autorig.db"
BACKEND = pathlib.Path("/srv/autorig/current/autorig-online/backend")
UPLOADS = pathlib.Path("/srv/autorig/data/var/uploads")
RENDERFIN = pathlib.Path("/srv/autorig/data/var/renderfin/render")
GLB_CACHE = pathlib.Path("/srv/autorig/data/static/glb_cache")
TASK_CACHE = pathlib.Path("/srv/autorig/data/static/tasks")
DISPATCH = MT_ROOT / "v3-dispatch" / "dispatch.sqlite3"
PY = sys.executable
MIN_DISK_GB = float(os.environ.get("CENSUS_MIN_DISK_GB", "40"))
CHILD_TIMEOUT = float(os.environ.get("CENSUS_CHILD_TIMEOUT", "900"))
LIMB_MAX_VERTS = int(os.environ.get("CENSUS_LIMB_MAX_VERTS", "400000"))
SCHEMA = "autorig.v3.census/1"
VERSION = 1


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)


def db():
    CENSUS.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS tasks(task_id TEXT PRIMARY KEY, created_at TEXT, status TEXT, pipeline_kind TEXT,
        input_type TEXT, input_name TEXT, title TEXT, keywords TEXT, origin TEXT, path TEXT, fmt TEXT, bytes INT,
        sha TEXT, original_sha TEXT, why_missing TEXT);
    CREATE TABLE IF NOT EXISTS hashes(path TEXT PRIMARY KEY, size INT, mtime REAL, sha TEXT);
    CREATE TABLE IF NOT EXISTS models(sha TEXT PRIMARY KEY, rid TEXT, path TEXT, fmt TEXT, origin TEXT, bytes INT,
        n_tasks INT, task_ids TEXT, first_created TEXT, words TEXT);
    CREATE TABLE IF NOT EXISTS census(sha TEXT PRIMARY KEY, rid TEXT, status TEXT, error TEXT, tools_rev TEXT,
        at REAL, seconds REAL, verts INT, tris INT, shells INT, geo_key TEXT, body_plan TEXT, bones INT,
        category TEXT, subcategory TEXT, subject TEXT, constitution TEXT, proportion TEXT, build TEXT,
        verdict TEXT, worst TEXT, defects TEXT, metrics TEXT, rig_s REAL, doc TEXT);
    CREATE TABLE IF NOT EXISTS vision(sha TEXT PRIMARY KEY, at REAL, model TEXT, answer TEXT, parsed TEXT);
    """)
    return con


def sha_file(path: pathlib.Path, con=None) -> str:
    st = path.stat()
    if con is not None:
        row = con.execute("SELECT sha FROM hashes WHERE path=? AND size=? AND mtime=?",
                          (str(path), st.st_size, st.st_mtime)).fetchone()
        if row:
            return row[0]
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    d = h.hexdigest()
    if con is not None:
        con.execute("INSERT OR REPLACE INTO hashes VALUES(?,?,?,?)", (str(path), st.st_size, st.st_mtime, d))
    return d


def sniff(path: pathlib.Path) -> str | None:
    try:
        with path.open("rb") as f:
            head = f.read(65536)
    except OSError:
        return None
    if head[:4] == b"glTF":
        return "glb"
    if head.startswith(b"Kaydara FBX Binary") or head.lstrip().startswith(b"; FBX"):
        return "fbx"
    if head[:8] == bytes.fromhex("89504e470d0a1a0a") or head[:3] == bytes.fromhex("ffd8ff"):
        return "image"
    if head[:2] == b"PK":
        return "zip"
    if b"\0" not in head[:4096] and re.search(rb"(?m)^\s*v\s+[-+0-9.eE]+\s+[-+0-9.eE]+", head):
        return "obj"
    if head.lstrip()[:1] == b"{" and b'"asset"' in head:
        return "gltf"
    return None


MESH = ("glb", "fbx", "obj")


# ---------------------------------------------------------------------------------------------- inventory
def candidates(t: dict, dispatch: dict, agents: dict) -> list[tuple[str, pathlib.Path]]:
    """(origin, path) in order of preference: the customer's original, the V3 source, the generated mesh, the
    converter's prepared mesh, the MT run's model."""
    out = []
    url = str(t["input_url"] or "")
    p = urlparse(url)
    parts = p.path.split("/")
    if p.netloc in ("autorig.online", "www.autorig.online", "") and len(parts) == 4 and parts[1] == "u":
        out.append(("upload", UPLOADS / unquote(parts[2]) / unquote(parts[3])))
    if p.path.startswith("/renderfin/render/"):
        out.append(("generated", RENDERFIN / unquote(p.path[len("/renderfin/render/"):])))
    for path in dispatch.get(t["id"], []):
        out.append(("v3_source", pathlib.Path(path)))
    out.append(("prepared", GLB_CACHE / f"{t['id']}_prepared.glb"))
    out.append(("prepared", TASK_CACHE / t["id"] / "model_prepared.glb"))
    run = agents.get(t["id"])
    if run:
        out.append(("mt_run", MT_ROOT / "runs" / run / "proj" / "model.glb"))
    return out


def inventory():
    con = db()
    site = ro(SITE_DB)
    site.row_factory = sqlite3.Row
    tasks = [dict(r) for r in site.execute(
        "SELECT id, created_at, status, pipeline_kind, input_type, input_url, poster_llm_title, poster_llm_keywords "
        "FROM tasks ORDER BY created_at")]
    dispatch = {}
    try:
        dc = ro(DISPATCH)
        for tid, path in dc.execute("SELECT r.task_id, s.stored_path FROM v3_runs r JOIN v3_sources s "
                                    "ON s.source_ref = r.source_ref ORDER BY r.attempt DESC"):
            dispatch.setdefault(tid, [])
            if path not in dispatch[tid]:
                dispatch[tid].append(path)
    except sqlite3.Error as exc:
        print("dispatch unreadable:", exc)
    agents = {}
    for f in (MT_ROOT / "task_agents").glob("*.json"):
        try:
            agents[f.stem] = json.loads(f.read_text()).get("run_id")
        except (OSError, ValueError):
            pass
    n = 0
    for t in tasks:
        chosen, why, original_sha = None, [], None
        for origin, path in candidates(t, dispatch, agents):
            if not path.is_file():
                why.append(f"{origin}:missing")
                continue
            fmt = sniff(path)
            if origin == "upload" and fmt in MESH:
                original_sha = sha_file(path, con)
            if fmt not in MESH:
                why.append(f"{origin}:{fmt}")
                continue
            chosen = (origin, path, fmt)
            break
        name = pathlib.PurePosixPath(urlparse(str(t["input_url"] or "")).path).name[:160]
        row = (t["id"], t["created_at"], t["status"], t["pipeline_kind"], t["input_type"], unquote(name),
               (t["poster_llm_title"] or "")[:200], (t["poster_llm_keywords"] or "")[:400])
        if chosen:
            origin, path, fmt = chosen
            s = original_sha if origin == "upload" else sha_file(path, con)
            con.execute("INSERT OR REPLACE INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        row + (origin, str(path), fmt, path.stat().st_size, s, original_sha, ""))
        else:
            con.execute("INSERT OR REPLACE INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        row + (None, None, None, None, None, original_sha, ",".join(why)[:300]))
        n += 1
        if n % 100 == 0:
            con.commit()
    con.commit()
    # unique models: one row per sha; popularity = the tasks that brought it
    con.execute("DELETE FROM models")
    groups = {}
    for tid, created, sha, path, fmt, origin, size, name, title, kw in con.execute(
            "SELECT task_id, created_at, sha, path, fmt, origin, bytes, input_name, title, keywords FROM tasks "
            "WHERE sha IS NOT NULL ORDER BY created_at"):
        g = groups.setdefault(sha, {"tasks": [], "path": path, "fmt": fmt, "origin": origin, "bytes": size,
                                    "first": created, "words": set()})
        g["tasks"].append(tid)
        for w in (name, title, kw):
            if w:
                g["words"].add(str(w)[:200])
    for sha, g in groups.items():
        con.execute("INSERT INTO models VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (sha, sha[:20], g["path"], g["fmt"], g["origin"], g["bytes"], len(g["tasks"]),
                     json.dumps(g["tasks"]), g["first"], " | ".join(sorted(g["words"]))[:1500]))
    con.commit()
    out = {"tasks": len(tasks),
           "with_source": con.execute("SELECT COUNT(*) FROM tasks WHERE sha IS NOT NULL").fetchone()[0],
           "unique_models": len(groups),
           "by_origin": dict(con.execute("SELECT origin, COUNT(*) FROM tasks GROUP BY origin").fetchall()),
           "by_fmt_unique": dict(con.execute("SELECT fmt, COUNT(*) FROM models GROUP BY fmt").fetchall()),
           "reuploads": sum(1 for g in groups.values() if len(g["tasks"]) > 1),
           "missing_why": dict(con.execute("SELECT substr(why_missing,1,60), COUNT(*) FROM tasks WHERE sha IS NULL "
                                           "GROUP BY 1 ORDER BY 2 DESC LIMIT 8").fetchall())}
    print(json.dumps(out, ensure_ascii=False, indent=1))
    return out


# ---------------------------------------------------------------------------------------------- one model (child)
SUBJECTS = [   # words of the file name, the LLM poster title and keywords -> a coarse subject (calibrated by Vision)
    ("robot_mech", r"\b(robot|mech|android|cyborg|droid|mecha|bot)\b"),
    ("knight_armor", r"\b(knight|armou?r|paladin|samurai|warrior|soldier|spartan|gladiator|templar)\b"),
    ("monster_creature", r"\b(monster|creature|demon|zombie|orc|goblin|troll|alien|beast|ghoul|undead|skeleton|"
                         r"mutant|dragon|ogre|imp|devil|werewolf)\b"),
    ("animal", r"\b(dog|cat|horse|wolf|fox|bear|lion|tiger|deer|cow|pig|rabbit|bunny|bird|chicken|duck|raccoon|"
               r"animal|quadruped|mouse|rat|monkey|ape|gorilla|frog|fish|shark|dino|dinosaur|raptor|t-rex|elephant)\b"),
    ("toy_chibi", r"\b(chibi|toy|cute|kawaii|cartoon|mascot|plush|funko|figurine|stylized|stylised|kid|baby)\b"),
    ("anime_girl", r"\b(anime|manga|waifu|vtuber)\b"),
    ("female", r"\b(woman|girl|female|lady|princess|queen|witch|goddess|maid|she|her|bikini)\b"),
    ("male", r"\b(man|boy|male|guy|king|prince|wizard|old man|businessman|he|his)\b"),
    ("vehicle_object", r"\b(car|truck|vehicle|tank|ship|plane|drone|weapon|gun|sword|chair|table|house|building|"
                       r"prop|rifle|bike|motorcycle)\b"),
    ("hands", r"\b(hand|hands|glove|gloves|arm|arms|fps)\b"),
]


def subject_of(words: str) -> str:
    w = " " + re.sub(r"[_\-.]+", " ", str(words or "").lower()) + " "
    for key, rx in SUBJECTS:
        if re.search(rx, w):
            return key
    return "unknown"


QUAD = r"\b(dog|cat|horse|wolf|fox|bear|lion|tiger|deer|cow|bull|pig|rabbit|raccoon|quadruped|mouse|rat|elephant|"        r"puppy|kitten|pony|goat|sheep|camel|giraffe|hippo|rhino|boar|panther|leopard|cheetah|hyena|donkey|lizard|"        r"crocodile|turtle|dinosaur|triceratops)\b"
BIPED_HINT = r"\b(humanoid|anthropomorphic|standing|warrior|knight|man|woman|girl|boy|person|human|character in|"              r"wearing|outfit|clothing|dress|suit|soldier|worker|robot|android|zombie|kid|child|lady|mage|wizard)\b"


def words_plan(words: str) -> str:
    """The body plan the task's words name (the LLM poster title and keywords, the file name): what the conveyor's
    Vision answer would rebuild a geometry-only 'root' rig with. '' when the words do not say."""
    w = " " + re.sub(r"[_\-.]+", " ", str(words or "").lower()) + " "
    if re.search(r"\b(glove|gloves|gauntlet|fps arms?|first person arms?)\b", w):
        return "hands"
    if re.search(QUAD, w) and not re.search(BIPED_HINT, w):
        return "quadruped"
    if re.search(BIPED_HINT, w) or re.search(r"\b(character|figure|mannequin|base mesh|avatar)\b", w):
        return "biped"
    return ""


def override(sha: str) -> dict:
    con = db()
    con.execute("CREATE TABLE IF NOT EXISTS overrides(sha TEXT PRIMARY KEY, plan TEXT, source TEXT, at REAL)")
    row = con.execute("SELECT plan, source FROM overrides WHERE sha=?", (sha,)).fetchone()
    con.close()
    return {"plan": row[0], "source": row[1]} if row else {}


def to_glb(src: pathlib.Path, fmt: str, dst: pathlib.Path) -> dict:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(f".model.{os.getpid()}.tmp")
    if fmt == "glb":
        shutil.copyfile(src, tmp)                                # a copy: tools may rewrite proj/model.glb
        os.replace(tmp, dst)
        return {"method": "copy"}
    sys.path.insert(0, str(BACKEND))
    if fmt == "fbx":
        import fbx_ascii
        rec = fbx_ascii.fbx_to_glb(src, dst)
        return {"method": rec.get("method")}
    if fmt == "obj":
        import v3_intake
        tmp.write_bytes(v3_intake.obj_to_glb(src.read_bytes()))
        os.replace(tmp, dst)
        return {"method": "obj_to_glb"}
    raise ValueError(f"format {fmt}")


def constitution(doc: dict, P) -> dict:
    """A geometric body-constitution estimate from the rig joints and the mesh (until the Body constitution agent's
    module lands): proportion by heads tall, build by torso girth, legs by hip height. Bipeds only."""
    import numpy as np
    plan = str(doc.get("body_plan") or "")
    if not plan.startswith("biped"):
        return {"constitution": plan or "none", "proportion": "", "build": ""}
    J = {b["name"]: np.asarray(b["head"], float) for b in doc.get("bones") or [] if b.get("head")}
    need = ("Hips", "Neck", "LeftArm", "RightArm")
    if not all(k in J for k in need):
        return {"constitution": "biped_unfitted", "proportion": "", "build": ""}
    y0, y1 = float(P[:, 1].min()), float(P[:, 1].max())
    H = max(y1 - y0, 1e-9)
    head_len = max(y1 - J["Neck"][1], 1e-9)
    heads = H / head_len
    lat = J["LeftArm"] - J["RightArm"]
    ax = 0 if abs(lat[0]) >= abs(lat[2]) else 2
    dx = 2 - ax
    span = abs(float(lat[ax]))
    cx, cz = float(J["Hips"][ax]), float(J["Hips"][dx])
    L = float(J["Neck"][1] - J["Hips"][1])                       # the torso band: above the hips, below the chest
    sel = (P[:, 1] > J["Hips"][1] + 0.15 * L) & (P[:, 1] < J["Hips"][1] + 0.6 * L)
    sel &= np.abs(P[:, ax] - cx) < max(0.45 * span, 0.08 * H)
    if sel.sum() >= 8:
        w = float(np.percentile(P[sel, ax], 95) - np.percentile(P[sel, ax], 5))
        d = float(np.percentile(P[sel, dx], 95) - np.percentile(P[sel, dx], 5))
    else:
        w = d = 0.0
    girth = (w * d) ** 0.5 / H if w and d else None
    legs = (float(J["Hips"][1]) - y0) / H
    shoulders = span / H
    prop = "chibi" if heads < 4.0 else ("stylized" if heads < 6.0 else "realistic")
    build = "" if girth is None else ("slim" if girth < 0.11 else ("average" if girth < 0.16 else "heavy"))
    return {"constitution": f"{prop}_{build or 'unknown'}", "proportion": prop, "build": build,
            "features": {"heads_tall": round(heads, 2), "torso_girth_H": None if girth is None else round(girth, 3),
                         "torso_w_H": round(w / H, 3), "torso_d_H": round(d / H, 3), "legs_H": round(legs, 3),
                         "shoulders_H": round(shoulders, 3), "height_units": round(H, 4)}}


def one(sha: str) -> dict:
    t0 = time.time()
    con = db()
    row = con.execute("SELECT rid, path, fmt, origin, words FROM models WHERE sha=?", (sha,)).fetchone()
    con.close()
    if not row:
        raise SystemExit(f"unknown sha {sha}")
    rid, path, fmt, origin, words = row
    rd = RUNS / rid
    if rd.exists():
        shutil.rmtree(rd)                                        # census dirs only: a fresh pass on today's tools
    (rd / "proj").mkdir(parents=True)
    out = {"schema": SCHEMA, "version": VERSION, "sha": sha, "rid": rid, "origin": origin, "fmt": fmt,
           "subject": subject_of(words), "timings": {}}
    T = out["timings"]
    sys.path.insert(0, str(MT_ROOT))
    os.chdir(MT_ROOT)
    try:
        t = time.time()
        out["convert"] = to_glb(pathlib.Path(path), fmt, rd / "proj" / "model.glb")
        T["convert"] = round(time.time() - t, 2)
        import numpy as np
        from mt import fastrig as F
        src = F.Source((rd / "proj" / "model.glb").read_bytes())
        P = src.positions
        ids, nw = F.weld(P)
        out.update(verts=int(len(P)), tris=int(len(src.faces)), welded=nw,
                   bbox=[[round(float(x), 4) for x in P.min(0)], [round(float(x), 4) for x in P.max(0)]])
        ext = np.ptp(P, 0)
        out["geo_key"] = f"{nw}:{len(src.faces)}:" + ",".join(f"{x / max(ext.max(), 1e-9):.3f}" for x in ext)
        del src
        # the census never grows the shared category registry
        from mt import fast_analysis as FA
        FA.ensure_category = lambda name, why="", based_on=None: "other"
        from mt import rig_first as R
        t = time.time()
        rt = {}
        doc = R.build(rd, "", timings=rt)
        out["first_plan"] = doc.get("body_plan")
        out["plan_source"] = "geometry"
        # the conveyor rebuilds the rig when Vision names another body plan; offline the census uses a Vision answer
        # of its own sample (overrides) or, for a geometry-only 'root', the task's words
        ov = override(sha)
        want = ov.get("plan") or (words_plan(words) if doc.get("body_plan") == "root" else "")
        if want and want != doc.get("body_plan"):
            doc = R.build(rd, "", plan=None if want == "hands" else want, timings=rt)
            out["plan_source"] = ov.get("source") or "words"
        T["rig_build"] = round(time.time() - t, 2)
        T["rig_steps"] = rt
        t = time.time()
        cls = {"what": " ".join(str(words or "").split("|")[:3])[:300]}
        fa = FA.run(rd, classify=cls)
        fa["applied"] = FA.apply_fixes(rd, fa)
        FA._atomic_json(rd / "analysis" / "fast.json", fa)
        T["fast_analysis"] = round(time.time() - t, 2)
        doc = json.loads((rd / "rig" / "rig.json").read_text())
        t = time.time()
        R.animate(rd, doc)
        doc.update(source="census", pass_="census")
        R._atomic_json(rd / "rig" / "rig.json", doc)
        T["clips"] = round(time.time() - t, 2)
        out["rig_s"] = round(T["rig_build"] + T["fast_analysis"] + T["clips"], 2)
        out.update(body_plan=doc.get("body_plan"), bones=len(doc.get("bones") or []), clips=len(doc.get("clips") or []),
                   forward_axis=doc.get("forward_axis"), category=(fa.get("category") or {}).get("category"),
                   subcategory=(fa.get("category") or {}).get("subcategory"))
        parts = FA._read_json(rd / "analysis" / "parts.json", {}) or {}
        out["shells"] = parts.get("shells") if isinstance(parts.get("shells"), int) else len(parts.get("shells") or [])
        out.update(constitution(doc, P))
        # the conveyor's limb step: arm clearance (limb collision report first, a better-only new version)
        if str(doc.get("body_plan") or "").startswith("biped") and out["verts"] <= LIMB_MAX_VERTS:
            t = time.time()
            env = {k: v for k, v in os.environ.items() if k != "OPENAI_API_KEY"}
            env.update(PYTHONPATH=str(MT_ROOT), OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2")
            p = subprocess.run([PY, "-P", "-m", "mt.arm_clearance", "--dir", str(rd), "--check"], cwd=str(MT_ROOT),
                               env=env, capture_output=True, text=True, timeout=600)
            T["arm_clearance"] = round(time.time() - t, 2)
            if p.returncode:
                out["arm_clearance_error"] = (p.stderr or p.stdout)[-300:]
            if not (rd / "analysis" / "limb_collision.json").is_file():
                subprocess.run([PY, "-P", "-m", "mt.limb_collision", "--dir", str(rd), "--source", "v3", "--no-emit"],
                               cwd=str(MT_ROOT), env=env, capture_output=True, text=True, timeout=600)
        # the triage's verdict on the census run (its rules, its classes)
        from mt import triage as TR
        TR.RUNS = RUNS
        TR.INDEX = CENSUS / "no_index"
        TR.poster_bad = lambda tid: None
        TR.manual_flags = lambda tid: []
        item = TR.triage_task({"id": rid, "status": "done", "pipeline_kind": "v3", "created_at": None,
                               "updated_at": None, "input_url": "", "poster_llm_title": ""},
                              {"session_json": json.dumps({"mt_run_id": rid}), "status": "done", "stage": "done"})
        D = [d for d in item.get("defects") or [] if not d["key"].startswith(("qa.", "run_", "poster_"))]
        if out["first_plan"] != doc.get("body_plan"):           # the first rig in the viewer had the wrong plan
            D.append({"key": "plan_geometry_wrong", "severity": 1,
                      "detail": f"geometry {out['first_plan']} -> {doc.get('body_plan')} ({out['plan_source']})"})
        out["defects"] = D
        out["verdict"] = {3: "defect", 2: "review", 1: "ok_notes", 0: "ok"}[max([d["severity"] for d in D] or [0])]
        out["worst"] = D[0]["key"] if D and D[0]["severity"] >= 2 else ""
        out["metrics"] = {k: item["metrics"].get(k) for k in ("fastrig", "rig_stretch", "limbs", "weights",
                                                               "bones_outside") if item["metrics"].get(k)}
        out["status"] = "ok"
    except Exception as exc:                                     # noqa: BLE001 - recorded as the model's verdict
        out.update(status="error", error=f"{type(exc).__name__}: {exc}"[:400], trace=traceback.format_exc()[-1500:])
    out["seconds"] = round(time.time() - t0, 2)
    (rd / "census.json").write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    # a census run keeps what the report and the sheet need; the clip GLB stays for the regression corpus
    for junk in ("labels", "stab/frames"):
        shutil.rmtree(rd / junk, ignore_errors=True)
    for junk in ["proj/model.glb", "tess/model_tess.glb", "rig/rigged_original.glb",
                 *[str(p.relative_to(rd)) for p in rd.glob("analysis/fast_backup/*.glb")],
                 *[str(p.relative_to(rd)) for p in rd.glob("rig/skin/v*/*.glb")]]:
        try:
            (rd / junk).unlink()                                 # copies only: the source stays where it was
        except OSError:
            pass
    return out


# ---------------------------------------------------------------------------------------------- the pass
def tools_rev() -> str:
    sys.path.insert(0, str(MT_ROOT))
    from mt import backfill
    return backfill.tools_rev(backfill.REPAIR_TOOLS)


def busy() -> list:
    sys.path.insert(0, str(MT_ROOT))
    from mt import backfill
    return backfill.live_busy()


def mem_ok() -> bool:
    sys.path.insert(0, str(MT_ROOT))
    from mt import backfill
    return backfill.mem_ok()


def record(con, out: dict, rev: str):
    con.execute("INSERT OR REPLACE INTO census VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
        out["sha"], out["rid"], out.get("status"), out.get("error"), rev, time.time(), out.get("seconds"),
        out.get("verts"), out.get("tris"), out.get("shells"), out.get("geo_key"), out.get("body_plan"),
        out.get("bones"), out.get("category"), out.get("subcategory"), out.get("subject"), out.get("constitution"),
        out.get("proportion"), out.get("build"), out.get("verdict"), out.get("worst"),
        json.dumps([d["key"] for d in out.get("defects") or []]), json.dumps(out.get("metrics") or {}, default=str),
        out.get("rig_s"), json.dumps({k: out.get(k) for k in ("features", "timings", "clips", "forward_axis", "first_plan", "plan_source",
                                                              "convert", "bbox", "welded", "arm_clearance_error")},
                                     default=str)))
    con.commit()


def pass1(limit=0, only_class="", stale=False, shas=None, order="popular"):
    con = db()
    rev = tools_rev()
    q = "SELECT m.sha FROM models m LEFT JOIN census c ON c.sha = m.sha WHERE 1=1"
    args = []
    if shas:
        q += f" AND m.sha IN ({','.join('?' * len(shas))})"
        args += shas
    elif stale:
        q += " AND (c.sha IS NULL OR c.tools_rev != ?)"
        args.append(rev)
    else:
        q += " AND c.sha IS NULL"
    if only_class:
        q += " AND (c.category || '/' || c.constitution) = ?"
        args.append(only_class)
    q += " ORDER BY m.n_tasks DESC, m.bytes ASC" if order == "popular" else " ORDER BY m.bytes ASC"
    todo = [r[0] for r in con.execute(q, args)]
    if limit:
        todo = todo[:limit]
    print(f"census pass1: {len(todo)} models, tools {rev}", flush=True)
    done = 0
    for sha in todo:
        while busy() or not mem_ok():                            # live customer runs first, always
            time.sleep(20)
        free = shutil.disk_usage(CENSUS).free / 1e9
        if free < MIN_DISK_GB:
            print(f"stop: {free:.0f} GB free < {MIN_DISK_GB}", flush=True)
            break
        t = time.time()
        try:
            p = subprocess.run([PY, "-P", __file__, "one", sha], capture_output=True, text=True,
                               timeout=CHILD_TIMEOUT, cwd=str(MT_ROOT),
                               env={**os.environ, "PYTHONPATH": str(MT_ROOT), "OMP_NUM_THREADS": "2",
                                    "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2"})
            rid = sha[:20]
            try:
                out = json.loads((RUNS / rid / "census.json").read_text())
                if p.returncode and out.get("status") == "ok":
                    out.update(status="error", error=f"exit {p.returncode}: {(p.stderr or '')[-200:]}")
            except (OSError, ValueError):
                out = {"sha": sha, "rid": rid, "status": "error",
                       "error": f"exit {p.returncode}: {(p.stderr or p.stdout or '')[-300:]}"}
        except subprocess.TimeoutExpired:
            out = {"sha": sha, "rid": sha[:20], "status": "timeout", "error": f"> {CHILD_TIMEOUT:.0f} s"}
        out.setdefault("seconds", round(time.time() - t, 1))
        record(con, out, rev)
        done += 1
        print(f"{done}/{len(todo)} {sha[:12]} {out.get('status')} {out.get('body_plan')} {out.get('category')} "
              f"{out.get('constitution')} {out.get('verdict')} {out.get('worst')} {out.get('seconds')}s", flush=True)


# ---------------------------------------------------------------------------------------------- Vision on a sample
VISION_Q = ("This is a 2x2 sheet of one 3D model seen from the front, its left side, the back and its right side. "
            "Answer exactly, one line each:\n"
            "WHAT: <what it is in 1-4 words>\n"
            "CATEGORY: <humanoid / quadruped / bird / insect / monster / vehicle / hands / object / other>\n"
            "STYLE: <realistic / stylized / cartoon / chibi / anime / low-poly>\n"
            "BUILD: <slim / average / heavy / muscular, or none if it is not a body>\n"
            "AGE: <child / teen / adult / old / none>\n"
            "SEX: <male / female / none>\n"
            "HEADS_TALL: <the body's height in head heights, a number, or none>\n"
            "POSE: <T-pose / A-pose / arms down / other pose / not a body>\n"
            "PROPS: <objects held or worn that are not the body, comma separated, or none>")
VISION_KEYS = ("WHAT", "CATEGORY", "STYLE", "BUILD", "AGE", "SEX", "HEADS_TALL", "POSE", "PROPS")


def parse_answer(text: str) -> dict:
    out = {}
    for line in str(text or "").splitlines():
        m = re.match(r"\s*\**\s*([A-Z_]+)\s*\**\s*:\s*(.+)", line)
        if m and m.group(1) in VISION_KEYS:
            out[m.group(1).lower()] = m.group(2).strip().strip("<>*").strip()[:120]
    return out


def sample(per_class=5, max_total=160) -> list[str]:
    """A stratified sample: up to per_class models of every class (category x constitution), popular first, then by
    defect (one flagged and one clean each where both exist)."""
    con = db()
    rows = con.execute("SELECT c.sha, c.category || '/' || c.constitution, c.verdict, m.n_tasks FROM census c "
                       "JOIN models m ON m.sha = c.sha LEFT JOIN vision v ON v.sha = c.sha "
                       "WHERE c.status = 'ok' AND v.sha IS NULL ORDER BY m.n_tasks DESC, c.at").fetchall()
    by = {}
    for sha, key, verdict, nt in rows:
        by.setdefault(key, {"bad": [], "good": []})["bad" if verdict in ("defect", "review") else "good"].append(sha)
    picks = []
    for key, g in sorted(by.items(), key=lambda kv: -(len(kv[1]["bad"]) + len(kv[1]["good"]))):
        take = []
        while len(take) < per_class and (g["bad"] or g["good"]):
            for side in ("bad", "good"):
                if g[side] and len(take) < per_class:
                    take.append(g[side].pop(0))
        picks += take
    return picks[:max_total]


def vision(per_class=5, max_total=160):
    import asyncio
    import httpx
    sys.path.insert(0, str(MT_ROOT))
    from mt import farm, full
    con = db()
    todo = sample(per_class, max_total)
    print(f"census vision: {len(todo)} models", flush=True)

    class _Run:
        root = MT_ROOT

    async def go():
        async with httpx.AsyncClient(follow_redirects=True) as client:
            for i, sha in enumerate(todo, 1):
                while busy() or not mem_ok():
                    await asyncio.sleep(20)
                rid = sha[:20]
                rd = RUNS / rid
                doc = json.loads((rd / "census.json").read_text())
                pj = rd / "vproj"
                sheet_png = pj / "sheet_lit.png"
                if not sheet_png.is_file():
                    p = subprocess.run([PY, "-P", "-m", "mt.project", "--glb", str(rd / "rig" / "rigged.glb"), "--out",
                                        str(pj), "--size", "384", f"--forward={doc.get('forward_axis') or '+z'}",
                                        "--passes", "lit"], cwd=str(MT_ROOT), capture_output=True, text=True,
                                       timeout=600, env={**os.environ, "PYTHONPATH": str(MT_ROOT)})
                    if p.returncode or not sheet_png.is_file():
                        print(f"{i} {sha[:12]} projection failed {(p.stderr or '')[-200:]}", flush=True)
                        continue
                try:
                    text = await farm.vision(client, full.cas_url(_Run, sheet_png), VISION_Q)
                except Exception as exc:                         # noqa: BLE001
                    text = ""
                    print(f"{i} {sha[:12]} vision error {exc!r}"[:200], flush=True)
                parsed = parse_answer(text)
                con.execute("INSERT OR REPLACE INTO vision VALUES(?,?,?,?,?)",
                            (sha, time.time(), "farm:qwen35-9b", text[:2000], json.dumps(parsed)))
                con.commit()
                print(f"{i}/{len(todo)} {sha[:12]} {doc.get('category')}/{doc.get('constitution')} -> "
                      f"{parsed.get('category')}/{parsed.get('style')}/{parsed.get('build')} {parsed.get('what')}",
                      flush=True)

    asyncio.run(go())


# ---------------------------------------------------------------------------------------------- the report
def report(as_json=False, min_n=1):
    con = db()
    n_models = con.execute("SELECT COUNT(*) FROM models").fetchone()[0]
    rows = con.execute("SELECT c.sha, c.status, c.category, c.constitution, c.subject, c.verdict, c.worst, c.defects, "
                       "c.rig_s, m.n_tasks, c.body_plan FROM census c JOIN models m ON m.sha = c.sha").fetchall()
    classes = {}
    tot = {"models": 0, "tasks": 0, "pass": 0, "error": 0}
    for sha, st, cat, cons, subj, verdict, worst, defects, rig_s, nt, plan in rows:
        key = f"{cat or 'error'}/{cons or '-'}" if st == "ok" else f"error/{st}"
        c = classes.setdefault(key, {"class": key, "models": 0, "tasks": 0, "pass": 0, "defects": {}, "subjects": {},
                                     "rig_s": [], "samples": []})
        c["models"] += 1
        c["tasks"] += nt or 1
        ok = st == "ok" and verdict in ("ok", "ok_notes")
        c["pass"] += ok
        tot["models"] += 1
        tot["tasks"] += nt or 1
        tot["pass"] += ok
        tot["error"] += st != "ok"
        c["subjects"][subj or "unknown"] = c["subjects"].get(subj or "unknown", 0) + 1
        if rig_s:
            c["rig_s"].append(rig_s)
        seen = set()
        for k in json.loads(defects or "[]"):
            base = k.split(".")[0]
            if base in seen or base in ("limb_unchecked", "bone_outside", "run_killed_retried", "sleeve_smear",
                                        "judge_fix", "rig_slow") and k != worst:
                continue
            seen.add(base)
            c["defects"][base] = c["defects"].get(base, 0) + 1
        if len(c["samples"]) < 6:
            c["samples"].append(sha[:20])
    out = []
    for c in sorted(classes.values(), key=lambda c: (-c["tasks"], -c["models"])):
        if c["models"] < min_n:
            continue
        top = sorted(c["defects"].items(), key=lambda kv: -kv[1])
        rs = sorted(c["rig_s"])
        out.append({**c, "pass_rate": round(c["pass"] / max(c["models"], 1), 3), "top_defects": top[:4],
                    "rig_s_p50": rs[len(rs) // 2] if rs else None, "rig_s_max": rs[-1] if rs else None,
                    "subjects": dict(sorted(c["subjects"].items(), key=lambda kv: -kv[1])[:4])})
    pairs = []
    for c in out:
        for d, n in c["top_defects"]:
            pairs.append({"class": c["class"], "defect": d, "models": n, "tasks_in_class": c["tasks"],
                          "share": round(n / c["models"], 3), "weight": n * (c["tasks"] / max(c["models"], 1))})
    pairs.sort(key=lambda p: -p["weight"])
    doc = {"schema": SCHEMA, "at": round(time.time(), 1), "models_total": n_models, "censused": tot,
           "classes": out, "top_pairs": pairs[:10]}
    (CENSUS / "report.json").write_text(json.dumps(doc, ensure_ascii=False, indent=1))
    if as_json:
        print(json.dumps(doc, ensure_ascii=False, indent=1))
    else:
        print(f"census: {tot['models']}/{n_models} models ({tot['tasks']} tasks), pass {tot['pass']}, "
              f"errors {tot['error']}")
        print(f"{'class':34} {'mod':>4} {'task':>5} {'pass':>5}  top defects")
        for c in out[:30]:
            print(f"{c['class'][:34]:34} {c['models']:4d} {c['tasks']:5d} {100 * c['pass_rate']:4.0f}%  "
                  + ", ".join(f"{d} {n}" for d, n in c["top_defects"][:3]))
        print("top pairs:")
        for p in pairs[:8]:
            print(f"  {p['class']:34} {p['defect']:20} {p['models']} models ({100 * p['share']:.0f}% of the class)")
    return doc


def sheet(path: str, top: int = 8, per: int = 6):
    """A contact sheet: one row per top class, sample thumbnails (the run's fast_before_after or weights preview)."""
    from PIL import Image, ImageDraw
    doc = json.loads((CENSUS / "report.json").read_text())
    classes = [c for c in doc["classes"] if not c["class"].startswith("error/")][:top]
    tw, th, lw = 220, 220, 300
    img = Image.new("RGB", (lw + per * tw, max(1, len(classes)) * th), (24, 24, 28))
    dr = ImageDraw.Draw(img)
    for i, c in enumerate(classes):
        y = i * th
        dr.text((10, y + 10), c["class"], fill=(240, 240, 240))
        dr.text((10, y + 34), f"{c['models']} models / {c['tasks']} tasks", fill=(200, 200, 200))
        dr.text((10, y + 54), f"pass {round(100 * c['pass_rate'])}%", fill=(120, 220, 120))
        for k, (d, n) in enumerate(c["top_defects"][:4]):
            dr.text((10, y + 80 + 20 * k), f"{d}: {n}", fill=(240, 150, 120))
        for j, rid in enumerate(c["samples"][:per]):
            rd = RUNS / rid
            pic = next((p for p in (rd / "analysis" / "census_thumb.png", rd / "rig" / "weights_preview.png",
                                    rd / "analysis" / "fast_before_after.png") if p.is_file()), None)
            if not pic:
                continue
            try:
                im = Image.open(pic).convert("RGB")
                im.thumbnail((tw - 6, th - 6))
                img.paste(im, (lw + j * tw + 3, y + 3))
            except OSError:
                pass
    img.save(path)
    print(path)


def main():
    a = sys.argv[1:]
    cmd = a[0] if a else "report"
    if cmd == "inventory":
        inventory()
    elif cmd == "one":
        print(json.dumps(one(a[1]), ensure_ascii=False, default=str)[:4000])
    elif cmd == "pass1":
        def opt(name, default=""):
            return a[a.index(name) + 1] if name in a else default
        pass1(limit=int(opt("--limit", "0")), only_class=opt("--class"), stale="--stale" in a,
              shas=[opt("--sha")] if "--sha" in a else None, order=opt("--order", "popular"))
    elif cmd == "vision":
        vision(per_class=int(a[a.index("--per") + 1]) if "--per" in a else 5,
               max_total=int(a[a.index("--max") + 1]) if "--max" in a else 160)
    elif cmd == "report":
        report(as_json="--json" in a)
    elif cmd == "sheet":
        sheet(a[1], top=int(a[a.index("--top") + 1]) if "--top" in a else 8)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()

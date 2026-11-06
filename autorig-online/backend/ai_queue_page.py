"""/queue - the phone's own gallery of finished renders (owner 2026-09-30).

The editor's strip shows the last 30 files of the open tab. This page goes as
deep as the render ledger, a page at a time, and carries the four buttons the
owner wants under a thumb: a new seed, post to Civitai now, post later (the
deferred queue), and sound on/off.

* GET  /api/ai/queue/items?limit&offset[&kind]  - a page of this caller's
  finished renders, newest first, read from the ledger in one query.
* POST /api/ai/queue/reseed {task_id_string}    - the same render again with a
  new seed. The graph binding is dropped from the copy, so an open editor
  cannot stand the new job down as "replaced by newer settings".

Owner 2026-09-30 ("это мой личный сайт, там нет других пользователей"): the
gallery shows every finished render of the farm, whatever device asked for it,
so the phone sees the same list as the desktop. Publishing is unchanged:
/api/ai/civitai/* needs the owner's admin session, and only files this farm
rendered can be posted at all.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger("autorig.queue_page")
router = APIRouter()

MAX_SCAN = 4000          # owner rows read before a page is cut
PAGE_MAX = 60
VIDEO_RE = re.compile(r"\.(mp4|webm|mov|m4v)(\?|$)", re.I)
AUDIO_RE = re.compile(r"\.(mp3|wav|flac|ogg|m4a)(\?|$)", re.I)
GRAPH_KEYS = ("graph_id", "node_id", "node_signature", "node_identity", "submit_session")
# Control-net preprocessors (owner 2026-09-30: "исключи Z_depth и другие
# control-net ноды"): gen_control_depth / _pose / _canny / _normal make the grey
# map a generator is steered by, not a result. A render *guided* by one
# (gen_image_control_depth and friends) is a real picture and stays.
CONTROL_SQL = "%gen_control_%"
CONTROL_RE = re.compile(r"gen_control_|preprocess", re.I)


def _is_control(row: Dict[str, Any]) -> bool:
    flow = str((row.get("prompt") or {}).get("work_flow") or row.get("workflow")
               or row.get("workflow_file") or "")
    return bool(CONTROL_RE.search(flow))
# Joined multi-scene clips (owner 2026-09-30): a list node that finished every
# shot joins them into one video, and that video belongs in this gallery beside
# the shots. The editor registers it here; the ledger knows nothing about it.
SUMMARY_STORE = Path(os.getenv("AUTORIG_QUEUE_SUMMARIES", "/srv/autorig/data/var/queue/summaries.json"))
SUMMARY_MAX = 400
# A take replaced by a new seed is not a result any more (owner 2026-09-30):
# its task id is retired here and the gallery skips it everywhere.
RETIRED_STORE = Path(os.getenv("AUTORIG_QUEUE_RETIRED", "/srv/autorig/data/var/queue/retired.json"))
RETIRED_MAX = 2000


def _retired() -> set:
    try:
        return set(json.loads(RETIRED_STORE.read_text(encoding="utf-8")).get("ids") or [])
    except Exception:
        return set()


def _retire(task_id: str) -> None:
    ids = [i for i in _retired() if i != task_id]
    ids.append(str(task_id))
    RETIRED_STORE.parent.mkdir(parents=True, exist_ok=True)
    tmp = RETIRED_STORE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"ids": ids[-RETIRED_MAX:]}), encoding="utf-8")
    tmp.replace(RETIRED_STORE)


def _summaries() -> List[Dict[str, Any]]:
    try:
        return list(json.loads(SUMMARY_STORE.read_text(encoding="utf-8")).get("items") or [])
    except Exception:
        return []


def _summaries_write(rows: List[Dict[str, Any]]) -> None:
    SUMMARY_STORE.parent.mkdir(parents=True, exist_ok=True)
    tmp = SUMMARY_STORE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"version": 1, "items": rows[-SUMMARY_MAX:]}, ensure_ascii=False), encoding="utf-8")
    tmp.replace(SUMMARY_STORE)


def _ledger_path() -> Optional[str]:
    try:
        from ai_pipelines_api import renderfin_db_path
        return renderfin_db_path()
    except Exception:
        return None


def _ledger_page(limit: int) -> List[Dict[str, Any]]:
    """The newest finished renders of the farm, newest first."""
    path = _ledger_path()
    if not path:
        return []
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    except Exception as error:
        logger.info("queue page: ledger unreachable: %s", error)
        return []
    out: List[Dict[str, Any]] = []
    try:
        for task_id, payload in connection.execute(
                "SELECT id, payload FROM render_tasks WHERE status = 'Done' "
                "AND payload NOT LIKE ? ORDER BY rowid DESC LIMIT ?",
                (CONTROL_SQL, int(limit))):
            try:
                row = json.loads(payload)
            except Exception:
                continue
            row["id"] = str(task_id)
            out.append(row)
    finally:
        connection.close()
    return out


def _ledger_count() -> int:
    path = _ledger_path()
    if not path:
        return 0
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
        try:
            return int(connection.execute(
                "SELECT COUNT(*) FROM render_tasks WHERE status = 'Done' "
                "AND payload NOT LIKE ?", (CONTROL_SQL,)).fetchone()[0])
        finally:
            connection.close()
    except Exception:
        return 0


def _ledger_rows(ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """The ledger payloads of these task ids (read-only, one query per chunk)."""
    path = _ledger_path()
    out: Dict[str, Dict[str, Any]] = {}
    if not path or not ids:
        return out
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    except Exception as error:
        logger.info("queue page: ledger unreachable: %s", error)
        return out
    try:
        for start in range(0, len(ids), 400):
            chunk = ids[start:start + 400]
            marks = ",".join("?" * len(chunk))
            for task_id, payload in connection.execute(
                    f"SELECT id, payload FROM render_tasks WHERE id IN ({marks})", chunk):
                try:
                    out[str(task_id)] = json.loads(payload)
                except Exception:
                    continue
    finally:
        connection.close()
    return out


def _kind(url: str) -> str:
    return "video" if VIDEO_RE.search(url) else "audio" if AUDIO_RE.search(url) else "image"


def _output_url(row: Dict[str, Any]) -> str:
    url = str(row.get("output_url") or "")
    ext = str(row.get("output_ext") or "")
    if url.endswith(".") and ext:
        url += ext.lstrip(".")
    return url


# A clip's own input picture (owner 2026-10-01): the phone shows it above the
# selected clip, with the same buttons, so the picture can get a new seed too.
# When the farm rendered that picture, its file name is its task id.
FARM_FILE_RE = re.compile(r"/renderfin/render/[^/]+/([0-9a-fA-F-]{36})\.(png|jpe?g|webp)(\?|$)")


def _resubmittable(prompt: Dict[str, Any]) -> bool:
    """The farm can run this request again: a workflow file, or a typed job (Qwen edits
    carry `type`, e.g. qwen_image21_edit, and an empty work_flow - 2026-10-01)."""
    return bool(prompt.get("work_flow") or prompt.get("type"))


def _attach_sources(page: List[Dict[str, Any]]) -> None:
    videos = [item for item in page if item.get("kind_string") == "video" and not item.get("summary_bool")]
    if not videos:
        return
    rows = _ledger_rows([item["task_id_string"] for item in videos])
    wanted: Dict[str, str] = {}
    for item in videos:
        source = str(((rows.get(item["task_id_string"]) or {}).get("prompt") or {}).get("image_url") or "")
        if not source.startswith(("http://", "https://")):
            continue
        item["source_url_string"] = source
        match = FARM_FILE_RE.search(source)
        if match:
            wanted[item["task_id_string"]] = match.group(1)
    found = _ledger_rows(sorted(set(wanted.values())))
    for item in videos:
        source_id = wanted.get(item["task_id_string"])
        row = found.get(source_id or "") or {}
        if not source_id or not row:
            continue
        prompt = row.get("prompt") or {}
        item["source_task_id_string"] = source_id
        item["source_seed_int"] = int(prompt.get("noise_seed") or 0)
        item["source_prompt_string"] = str(prompt.get("prompt") or "")[:400]
        item["source_reseed_bool"] = _resubmittable(prompt)


@router.get("/api/ai/queue/items")
async def api_queue_items(request: Request, limit: int = 24, offset: int = 0, kind: str = "all"):
    """A page of the farm's finished renders, newest first, with the joined films."""
    limit = max(1, min(PAGE_MAX, int(limit or 24)))
    offset = max(0, int(offset or 0))
    want = str(kind or "all").strip().lower()
    scan = min(offset + limit + 60, MAX_SCAN)
    retired = _retired()
    items: List[Dict[str, Any]] = []
    for row in _ledger_page(scan):
        url = _output_url(row)
        if not url or str(row.get("id") or "") in retired or _is_control(row):
            continue
        media = _kind(url)
        if want in ("video", "image", "audio") and media != want:
            continue
        items.append({"task_id_string": str(row.get("id") or ""), "url_string": url, "kind_string": media,
                      "at_unix_float": float(row.get("finished_at") or row.get("created_at") or 0),
                      "box_string": str(row.get("server_name") or ""),
                      "seed_int": int((row.get("prompt") or {}).get("noise_seed") or 0),
                      "prompt_string": str((row.get("prompt") or {}).get("prompt") or "")[:400],
                      "graph_id_string": str((row.get("prompt") or {}).get("graph_id") or "")})
    for row in _summaries():
        if not row.get("url"):
            continue
        if want in ("video", "image", "audio") and want != "video":
            continue
        items.append({"task_id_string": "summary:" + str(row.get("id") or ""), "url_string": str(row["url"]),
                      "kind_string": "video", "at_unix_float": float(row.get("at") or 0),
                      "box_string": "joined", "seed_int": 0,
                      "prompt_string": str(row.get("prompt") or "")[:400],
                      "graph_id_string": str(row.get("graph_id") or ""),
                      "summary_bool": True, "node_id_string": str(row.get("node_id") or ""),
                      "shots_int": int(row.get("shots") or 0),
                      "label_string": str(row.get("label") or "")})
    items.sort(key=lambda row: -float(row.get("at_unix_float") or 0))
    page = items[offset:offset + limit]
    _attach_sources(page)
    total = max(0, _ledger_count() - len(retired)) + len(_summaries())
    queue: Dict[str, Any] = {}
    try:
        async with httpx.AsyncClient() as client:
            fleet = (await client.get("http://127.0.0.1:8200/api/ai/fleet", timeout=8.0)).json()
            share = fleet.get("queue_object") or {}
            queue = {"queued_int": int(share.get("queued_int") or 0),
                     "running_int": int(share.get("running_int") or 0)}
    except Exception:
        queue = {}
    return {"success_bool": True, "items_array": page, "total_int": total,
            "offset_int": offset, "limit_int": limit,
            "has_more_bool": len(items) > offset + limit or scan < total,
            "queue_object": queue, "scanned_int": len(items),
            "server_time_unix_int": int(time.time())}


# ---------------------------------------------------------------- posting
# Owner 2026-09-30: "убери требование авторизации, кроме меня сюда никто не
# зайдёт". The phone posts through these two, without the admin session that
# /api/ai/civitai/* asks for. What stays: only a file this farm rendered can be
# posted (ai_civitai_post.is_ours), and Civitai's own daily limit.


def _post_body(row: "PostRow"):
    import ai_civitai_post as post

    if not post.is_ours(row.media_url_string):
        raise HTTPException(status_code=400, detail={
            "error_string": "not_our_file",
            "message_string": "Only files this farm rendered can be posted"})
    rating = str(row.nsfw_level_string or "R").upper()
    if rating not in post.NSFW_LEVELS:
        rating = "R"
    return post.CivitaiPostRequest(
        media_url=row.media_url_string, title="", description="", prompt=row.prompt_string or "",
        tags=[tag for tag in (row.tags_array or ["autorig"]) if tag][:5], nsfw_level=rating,
        publish=True, background=True, auto_meta=True, title_is_placeholder=True,
        generation={"seed": int(row.seed_int or 0)} if row.seed_int else {})


class PostRow(BaseModel):
    media_url_string: str = Field(..., min_length=8, max_length=4096)
    nsfw_level_string: str = Field("R", max_length=8)
    prompt_string: str = Field("", max_length=4000)
    tags_array: List[str] = Field(default_factory=list, max_length=10)
    seed_int: int = Field(0, ge=0, le=9007199254740991)


@router.post("/api/ai/queue/post")
async def api_queue_post(row: PostRow):
    """Post this file to Civitai now (no sign-in; this farm's files only)."""
    import ai_civitai_post as post

    state = post.limit_state()
    if state["limited_bool"]:
        return {"success_bool": False, "limited_bool": True, "reason_string": state["message_string"]}
    answer = await post.start_background_post(_post_body(row))
    return dict(answer, success_bool=True)


@router.post("/api/ai/queue/later")
async def api_queue_later(row: PostRow):
    """Put this file in the deferred queue (one post every 30 minutes)."""
    import ai_civitai_post as post

    return dict(post.schedule_add(_post_body(row)), success_bool=True)


@router.get("/api/ai/queue/marks")
async def api_queue_marks():
    """Which files are already posted or waiting in the deferred queue."""
    import ai_civitai_post as post

    queued = [str(item.get("media_url") or "") for item in (post._schedule().get("items") or [])]
    posted, failed = [], []
    for job in post.JOBS.values():
        url = str(job.get("media_url_string") or "")
        if not url:
            continue
        stage = str(job.get("stage") or "")
        if stage == "done":
            posted.append(url)
        elif stage in ("failed", "manual", "interrupted"):
            failed.append(url)
        else:
            queued.append(url)          # a post already running counts as taken
    return {"success_bool": True, "queued_array": sorted(set(queued)), "posted_array": sorted(set(posted)),
            "failed_array": sorted(set(failed) - set(posted) - set(queued)),
            "schedule_object": post.schedule_state()}


class SummaryRow(BaseModel):
    graph_id_string: str = Field("", max_length=64)
    node_id_string: str = Field("", max_length=64)
    url_string: str = Field(..., min_length=8, max_length=4096)
    shots_int: int = Field(0, ge=0, le=512)
    label_string: str = Field("", max_length=200)
    prompt_string: str = Field("", max_length=1000)


@router.post("/api/ai/queue/summary")
async def api_queue_summary(body: SummaryRow, request: Request):
    """The editor registers a joined multi-scene clip so the phone sees it too."""
    rows = [row for row in _summaries()
            if not (str(row.get("graph_id")) == body.graph_id_string
                    and str(row.get("node_id")) == body.node_id_string)]
    rows.append({"id": f"{body.graph_id_string}:{body.node_id_string}", "owner": "",
                 "graph_id": body.graph_id_string, "node_id": body.node_id_string,
                 "url": body.url_string, "shots": body.shots_int, "label": body.label_string,
                 "prompt": body.prompt_string, "at": time.time()})
    _summaries_write(rows)
    return {"success_bool": True, "count_int": len(rows)}


class GraphReseedRequest(BaseModel):
    graph_id_string: str = Field(..., min_length=4, max_length=64)
    node_id_string: str = Field(..., min_length=1, max_length=64)
    index_int: int = Field(-1, ge=-1, le=511)      # -1 = every scene


@router.post("/api/ai/queue/graph-reseed")
async def api_queue_graph_reseed(body: GraphReseedRequest):
    """Mark a node's scenes for a new seed; the open graph renders them again.

    The phone cannot render by itself - the browser tab drives the farm - so
    this writes the new seeds into the stored graph and drops those results.
    A tab open on that graph (Auto on) picks the change up and re-renders; the
    next Render does the same when no tab is open.
    """
    import pathlib as _pathlib

    graph_dir = _pathlib.Path(os.getenv("AUTORIG_AI_GRAPH_DIR", "/srv/autorig/data/var/ai-graphs"))
    path = graph_dir / f"{body.graph_id_string}.json"
    if not re.fullmatch(r"[A-Za-z0-9_-]{4,64}", body.graph_id_string) or not path.is_file():
        raise HTTPException(status_code=404, detail={"error_string": "graph_not_found"})
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        raise HTTPException(status_code=500, detail={"error_string": "graph_unreadable"}) from None
    graph = doc.get("graph_object") or doc
    result = (graph.get("results") or {}).get(body.node_id_string)
    items = (result or {}).get("items") if isinstance(result, dict) else None
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=409, detail={
            "error_string": "no_scenes", "message_string": "That node has no list of scenes"})
    touched = []
    for position, item in enumerate(items):
        if body.index_int >= 0 and position != body.index_int:
            continue
        if not isinstance(item, dict):
            continue
        item["seed_override"] = random.randint(1, 2147483000)
        item["status"] = "stale"
        item["sig"] = ""
        item["error"] = "new seed from the phone"
        touched.append(position)
    if not touched:
        raise HTTPException(status_code=409, detail={"error_string": "nothing_marked"})
    doc["revision_int"] = int(doc.get("revision_int") or 0) + 1
    doc["saved_at_unix_int"] = int(time.time())
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return {"success_bool": True, "scenes_array": touched, "revision_int": doc["revision_int"],
            "message_string": ("Marked " + ("every scene" if body.index_int < 0 else "scene " + str(body.index_int + 1)) +
                               " — the open graph renders it again")}


class ReseedRequest(BaseModel):
    task_id_string: str = Field(..., min_length=8, max_length=64)
    seed_int: Optional[int] = Field(None, ge=0, le=2147483000)


@router.post("/api/ai/queue/reseed")
async def api_queue_reseed(body: ReseedRequest, request: Request):
    """Render this task again with a new seed (the caller's own tasks only)."""
    import task_owner

    task_id = str(body.task_id_string).strip()
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", task_id):
        raise HTTPException(status_code=400, detail={"error_string": "bad_task"})
    caller = task_owner.scope_identity(request.scope)
    row = _ledger_rows([task_id]).get(task_id) or {}
    _retire(task_id)   # the take being replaced leaves the gallery at once
    payload = dict(row.get("prompt") or {})
    if not _resubmittable(payload):
        raise HTTPException(status_code=409, detail={
            "error_string": "no_request",
            "message_string": "The farm no longer keeps this render's request; render it from its graph"})
    seed = int(body.seed_int) if body.seed_int else random.randint(1, 2147483000)
    payload["noise_seed"] = seed
    # Without this the open editor would stand the copy down as a stale job of
    # that node ("replaced by newer settings"): it is nobody's node any more.
    for key in GRAPH_KEYS:
        payload.pop(key, None)
    base = os.getenv("RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8210").rstrip("/")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(base + "/api-render", json=payload, timeout=60.0)
    except Exception as error:
        logger.info("queue page: renderfin refused a reseed: %s", error)
        raise HTTPException(status_code=502, detail={
            "error_string": "farm_unreachable", "message_string": "The farm did not answer"}) from None
    if response.status_code not in (200, 202):
        raise HTTPException(status_code=502, detail={
            "error_string": "farm_rejected", "message_string": f"The farm answered HTTP {response.status_code}"})
    accepted = response.json() or {}
    new_id = str(accepted.get("task_id") or "")
    if not new_id:
        raise HTTPException(status_code=502, detail={"error_string": "farm_no_task"})
    try:
        task_owner.record(new_id, caller)
    except Exception:
        pass
    return {"success_bool": True, "task_id_string": new_id, "seed_int": seed,
            "output_url_string": str(accepted.get("output_url") or ""),
            "kind_string": _kind(str(accepted.get("output_url") or ""))}


@router.get("/api/ai/queue/task/{task_id}")
async def api_queue_task(task_id: str, request: Request):
    """Where a render is now: for the page to follow its own new seed."""
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", task_id):
        raise HTTPException(status_code=400, detail={"error_string": "bad_task"})
    row = _ledger_rows([task_id]).get(task_id) or {}
    status = str(row.get("status") or "unknown").lower()
    return {"success_bool": True, "task_id_string": task_id, "status_string": status,
            "done_bool": status == "done", "failed_bool": status == "error",
            "url_string": _output_url(row) if status == "done" else "",
            "box_string": str(row.get("server_name") or ""), "error_string": str(row.get("error") or "")[:200]}

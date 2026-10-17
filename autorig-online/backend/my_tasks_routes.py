"""«My models»: the owner's own gallery with per-card privacy and full deletion (owner 2026-10-17,
spec docs/my-gallery-privacy-spec-20261017.md).

* ``GET  /api/my/tasks``                 the caller's tasks (signed-in user by e-mail, or the anon cookie / API key)
* ``PATCH /api/my/tasks/{id}``           ``{is_public}``; private needs an active subscription (or admin)
* ``DELETE /api/my/tasks/{id}``          hide at once, erase after ``UNDO_SECONDS`` unless restored
* ``POST /api/my/tasks/{id}/restore``    undo a delete inside the window
* ``POST /api/my/tasks/bulk``            ``{ids, action: private|public|delete}``
* ``GET/POST /api/my/settings``          ``private_by_default`` for subscribers
* ``POST|PATCH /api/task/{id}/visibility``  the task page's existing button (same rules as PATCH)
* ``GET /my``                            the page

Only the owner or an admin may act on a task; anyone else gets 404, as if the task did not exist.
Every action is one line in the privacy audit (task_privacy.audit), never the model's content.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import delete as sa_delete, func, select, update

import task_privacy

UNDO_SECONDS = int(os.getenv("AUTORIG_DELETE_UNDO_SECONDS", "10"))
PENDING_PATH = Path(os.getenv("AUTORIG_PENDING_DELETE_PATH", "/srv/autorig/data/var/privacy/pending_delete.json"))
SETTINGS_PATH = Path(os.getenv("AUTORIG_PRIVACY_SETTINGS_PATH", "/srv/autorig/data/var/privacy/user_settings.json"))
DATA = Path("/srv/autorig/data")
MT_ROOT = DATA / "motion_transfer"
MAX_BULK = 200

_sweeper_started = False


# ------------------------------------------------------------------------------------------------ small stores
def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, path)


def private_by_default(email: Optional[str]) -> bool:
    """Subscribers' new models are private unless they switched it off (default on)."""
    if not email:
        return False
    prefs = _read_json(SETTINGS_PATH, {})
    row = prefs.get(email.strip().lower()) if isinstance(prefs, dict) else None
    if isinstance(row, dict) and "private_by_default" in row:
        return bool(row["private_by_default"])
    return True


def err(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"error_string": code, "message_string": message})


# ------------------------------------------------------------------------------------------------- install
def install(app, *, get_current_user: Callable, get_db: Callable, static_html_response: Callable,
            thumb_url_for: Callable, delete_artifacts: Callable, delete_record: Callable,
            is_admin_email: Callable, user_model, task_model) -> None:
    from subscription_access import user_has_active_subscription

    Task = task_model

    def caller(request: Request, user) -> Tuple[Optional[str], Optional[str], bool]:
        """(user e-mail, anon id, is admin)."""
        email = getattr(user, "email", None) if user is not None else None
        anon = getattr(request.state, "api_key_anon_id", None) or request.cookies.get("anon_id")
        return email, anon, bool(email and is_admin_email(email))

    def owns(task, email, anon, admin) -> bool:
        if admin:
            return True
        if task.owner_type == "user":
            return bool(email) and str(task.owner_id or "").lower() == email.lower()
        return task.owner_type == "anon" and bool(anon) and task.owner_id == anon

    def can_private(user, admin: bool) -> bool:
        return admin or user_has_active_subscription(user)

    async def load_owned(db, task_id: str, request: Request, user):
        email, anon, admin = caller(request, user)
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None or not owns(task, email, anon, admin):
            raise err(404, "task_not_found", "Not found")
        return task, email, admin

    def card(task, pending: Dict[str, Any]) -> Dict[str, Any]:
        done = task.status in ("done", "needs_review")
        return {
            "task_id": task.id,
            "status": task.status,
            "is_public": task.is_public is not False,
            "deleting": task.id in pending,
            "title": (task.poster_llm_title or "").strip() or None,
            "created_at": task.created_at.isoformat() + "Z" if task.created_at else None,
            "thumbnail_url": (f"/api/thumb/{task.id}" if task.is_public is False else thumb_url_for(task.id)) if done else None,
            "video_url": f"/api/video/{task.id}" if done else None,
            "content_rating": getattr(task, "content_rating", None),
            "pipeline_kind": getattr(task, "pipeline_kind", None),
        }

    async def set_visibility(db, task, public: bool, *, user, email, admin, by: str) -> None:
        if not public and not can_private(user, admin):
            raise err(402, "subscription_required", "Private models are part of the subscription")
        if (task.is_public is not False) == public:
            return
        task.is_public = public
        task.updated_at = datetime.utcnow()
        await db.commit()
        task_privacy.INDEX.invalidate()
        task_privacy.audit("set_public" if public else "set_private", task.id, by=by)
        if not public:
            asyncio.create_task(_telegram_cleanup(task.id, broadcast_only=True, reason="private"))

    # ------------------------------------------------------------------ list
    @app.get("/api/my/tasks")
    async def api_my_tasks(request: Request, page: int = 1, per_page: int = 24, user=Depends(get_current_user),
                           db=Depends(get_db)):
        _ensure_sweeper(delete_artifacts, delete_record, get_db_session=_session_factory())
        email, anon, admin = caller(request, user)
        per_page = max(1, min(int(per_page or 24), 96))
        page = max(1, int(page or 1))
        if email:
            cond = [Task.owner_type == "user", Task.owner_id == email]
        elif anon:
            cond = [Task.owner_type == "anon", Task.owner_id == anon]
        else:
            return {"items": [], "total": 0, "page": page, "per_page": per_page, "signed_in": False,
                    "can_private": False, "private_by_default": False, "undo_seconds": UNDO_SECONDS}
        total = (await db.execute(select(func.count()).select_from(Task).where(*cond))).scalar() or 0
        rows = (await db.execute(select(Task).where(*cond).order_by(Task.created_at.desc())
                                 .offset((page - 1) * per_page).limit(per_page))).scalars().all()
        pending = _read_json(PENDING_PATH, {})
        return {"items": [card(t, pending) for t in rows], "total": int(total), "page": page, "per_page": per_page,
                "signed_in": bool(email), "can_private": can_private(user, admin),
                "private_by_default": bool(email) and can_private(user, admin) and private_by_default(email),
                "undo_seconds": UNDO_SECONDS}

    # ------------------------------------------------------------------ privacy
    async def _patch(task_id: str, request: Request, user, db) -> Dict[str, Any]:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict) or not isinstance(body.get("is_public"), bool):
            raise err(400, "bad_request", "is_public must be true or false")
        task, email, admin = await load_owned(db, task_id, request, user)
        if task.id in _read_json(PENDING_PATH, {}):
            raise err(409, "task_deleting", "The model is being deleted")
        await set_visibility(db, task, body["is_public"], user=user, email=email, admin=admin,
                             by=email or "anon")
        return {"ok": True, "task_id": task.id, "is_public": task.is_public is not False,
                "can_manage_visibility": can_private(user, admin)}

    @app.patch("/api/my/tasks/{task_id}")
    async def api_my_task_patch(task_id: str, request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        return await _patch(task_id, request, user, db)

    @app.api_route("/api/task/{task_id}/visibility", methods=["POST", "PATCH"])
    async def api_task_visibility(task_id: str, request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        return await _patch(task_id, request, user, db)

    # ------------------------------------------------------------------ delete / restore
    async def _request_delete(db, task, by: str) -> Dict[str, Any]:
        pending = _read_json(PENDING_PATH, {})
        busy_since = task.updated_at or task.created_at
        if (task.id not in pending and task.status in ("created", "processing") and busy_since
                and (datetime.utcnow() - busy_since).total_seconds() < 7200):
            raise err(409, "task_busy", "The model is still being processed")
        if task.id not in pending:
            pending[task.id] = {"was_public": task.is_public is not False, "at": time.time(), "by": by,
                                "run_after": time.time() + UNDO_SECONDS}
            _write_json(PENDING_PATH, pending)
            task.is_public = False                      # hidden at once, like private
            task.updated_at = datetime.utcnow()
            await db.commit()
            task_privacy.INDEX.invalidate()
            task_privacy.audit("delete_requested", task.id, by=by)
        _ensure_sweeper(delete_artifacts, delete_record, get_db_session=_session_factory())
        return {"ok": True, "task_id": task.id, "undo_seconds": UNDO_SECONDS}

    @app.delete("/api/my/tasks/{task_id}")
    async def api_my_task_delete(task_id: str, request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        task, email, admin = await load_owned(db, task_id, request, user)
        return await _request_delete(db, task, email or "anon")

    @app.post("/api/my/tasks/{task_id}/restore")
    async def api_my_task_restore(task_id: str, request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        task, email, admin = await load_owned(db, task_id, request, user)
        pending = _read_json(PENDING_PATH, {})
        row = pending.pop(task.id, None)
        if row is None:
            raise err(409, "nothing_to_restore", "Nothing to restore")
        _write_json(PENDING_PATH, pending)
        task.is_public = bool(row.get("was_public", True))
        task.updated_at = datetime.utcnow()
        await db.commit()
        task_privacy.INDEX.invalidate()
        task_privacy.audit("delete_undone", task.id, by=email or "anon")
        return {"ok": True, "task_id": task.id, "is_public": task.is_public is not False}

    @app.post("/api/my/tasks/bulk")
    async def api_my_tasks_bulk(request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        try:
            body = await request.json()
        except Exception:
            body = {}
        ids = body.get("ids") if isinstance(body, dict) else None
        action = body.get("action") if isinstance(body, dict) else None
        if action not in ("private", "public", "delete") or not isinstance(ids, list) or not ids:
            raise err(400, "bad_request", "ids and action (private|public|delete) are required")
        if len(ids) > MAX_BULK:
            raise err(400, "too_many", f"At most {MAX_BULK} ids")
        email, anon, admin = caller(request, user)
        if action == "private" and not can_private(user, admin):
            raise err(402, "subscription_required", "Private models are part of the subscription")
        results = []
        for tid in dict.fromkeys(str(x) for x in ids):
            try:
                task, email, admin = await load_owned(db, tid, request, user)
                if action == "delete":
                    await _request_delete(db, task, email or "anon")
                else:
                    if task.id in _read_json(PENDING_PATH, {}):
                        raise err(409, "task_deleting", "The model is being deleted")
                    await set_visibility(db, task, action == "public", user=user, email=email, admin=admin,
                                         by=email or "anon")
                results.append({"task_id": tid, "ok": True})
            except HTTPException as exc:
                results.append({"task_id": tid, "ok": False, "status": exc.status_code,
                                "error_string": (exc.detail or {}).get("error_string") if isinstance(exc.detail, dict) else None})
        return {"ok": all(r["ok"] for r in results), "results": results, "undo_seconds": UNDO_SECONDS}

    # ------------------------------------------------------------------ settings
    @app.get("/api/my/settings")
    async def api_my_settings(request: Request, user=Depends(get_current_user)):
        email, anon, admin = caller(request, user)
        return {"signed_in": bool(email), "can_private": can_private(user, admin),
                "private_by_default": bool(email) and can_private(user, admin) and private_by_default(email)}

    @app.post("/api/my/settings")
    async def api_my_settings_save(request: Request, user=Depends(get_current_user)):
        email, anon, admin = caller(request, user)
        if not email:
            raise err(401, "sign_in_required", "Sign in required")
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict) or not isinstance(body.get("private_by_default"), bool):
            raise err(400, "bad_request", "private_by_default must be true or false")
        prefs = _read_json(SETTINGS_PATH, {})
        prefs = prefs if isinstance(prefs, dict) else {}
        prefs[email.lower()] = {"private_by_default": body["private_by_default"], "at": time.time()}
        _write_json(SETTINGS_PATH, prefs)
        task_privacy.audit("settings", "-", by=email, private_by_default=body["private_by_default"])
        return {"ok": True, "private_by_default": body["private_by_default"]}

    # ------------------------------------------------------------------ page
    @app.get("/my")
    async def my_models_page():
        response = static_html_response("my.html")
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        response.headers["Cache-Control"] = "private, no-store"
        return response

    task_privacy.ON_FIRST_REQUEST = lambda: _ensure_sweeper(delete_artifacts, delete_record,
                                                            get_db_session=_session_factory())
    app.state.my_tasks_full_delete = lambda tid, by="admin": _full_delete(tid, by, delete_artifacts, delete_record,
                                                                       _session_factory())


def _session_factory():
    from database import AsyncSessionLocal
    return AsyncSessionLocal


# ------------------------------------------------------------------------------------------- the eraser
def _ensure_sweeper(delete_artifacts, delete_record, *, get_db_session) -> None:
    global _sweeper_started
    if _sweeper_started:
        return
    _sweeper_started = True

    async def loop():
        while True:
            try:
                pending = _read_json(PENDING_PATH, {})
                now = time.time()
                for tid, row in list(pending.items()):
                    if float(row.get("run_after", 0)) <= now:
                        await _full_delete(tid, row.get("by") or "owner", delete_artifacts, delete_record, get_db_session)
                        fresh = _read_json(PENDING_PATH, {})
                        fresh.pop(tid, None)
                        _write_json(PENDING_PATH, fresh)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[privacy] delete sweeper: {type(exc).__name__}: {exc}")
            await asyncio.sleep(2)

    asyncio.create_task(loop())


def _runs_of(task_id: str) -> List[str]:
    task_privacy.INDEX.refresh(force=True)
    return [run for run, tid in task_privacy.INDEX.run_task.items() if tid == task_id.lower()]


def _rm(path: Path, report: Dict[str, Any]) -> None:
    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        elif path.exists() or path.is_symlink():
            path.unlink()
        else:
            return
        report["removed"] += 1
    except Exception as exc:
        report["failed"].append(f"{path.name}: {type(exc).__name__}")


def _source_shared(sha: str, task_id: str) -> bool:
    for run, tid in task_privacy.INDEX.run_task.items():
        if tid == task_id:
            continue
        try:
            data = json.loads((MT_ROOT / "runs" / run / "run.json").read_text(encoding="utf-8"))
            if ((data.get("request") or {}).get("source_sha256") or "") == sha:
                return True
        except Exception:
            continue
    return False


async def _full_delete(task_id: str, by: str, delete_artifacts, delete_record, get_db_session) -> Dict[str, Any]:
    """Erase a task everywhere we hold it: files (shared admin path + what it misses), MT runs, Telegram notices,
    DB rows. YouTube needs the channel owner (upload-only scope), so it is reported, not done."""
    from database import Task
    report: Dict[str, Any] = {"removed": 0, "failed": [], "runs": [], "telegram": {}, "youtube": None}
    async with get_db_session() as db:
        task = (await db.execute(select(Task).where(Task.id == task_id))).scalar_one_or_none()
        if task is None:
            task_privacy.audit("delete_skipped_missing", task_id, by=by)
            return report
        report["youtube"] = task.youtube_video_id or None
        runs = _runs_of(task_id)
        report["runs"] = runs
        shas = set()
        for run in runs:
            try:
                data = json.loads((MT_ROOT / "runs" / run / "run.json").read_text(encoding="utf-8"))
                sha = (data.get("request") or {}).get("source_sha256")
                if sha:
                    shas.add(str(sha))
            except Exception:
                pass
        report["telegram"] = await _telegram_cleanup(task_id, broadcast_only=False, reason="delete")
        try:
            n, _freed = delete_artifacts(task)       # task cache, videos, glb cache, the upload (admin path)
            report["removed"] += int(n or 0)
        except Exception as exc:
            report["failed"].append(f"artifacts: {type(exc).__name__}")
        for p in (DATA / "static/posters-v3" / f"{task_id}.jpg", DATA / "static/posters-s" / f"{task_id}.webp",
                  DATA / "static/previews-v3" / f"{task_id}.mp4", MT_ROOT / "triage/items" / f"{task_id}.json",
                  MT_ROOT / "task_agents" / task_id):
            _rm(p, report)
        for base, pattern in ((DATA / "artifact-cache", f"{task_id}*"), (Path("/var/autorig/preflight-renders"), f"{task_id}*"),
                              (DATA / "var/rig_defects", f"{task_id}.*"), (DATA / "static/og", f"{task_id}*")):
            try:
                for p in base.glob(pattern):
                    _rm(p, report)
            except Exception:
                pass
        for run in runs:
            _rm(MT_ROOT / "runs" / run, report)
        for sha in shas:
            if not _source_shared(sha, task_id):
                for p in (DATA / "v3-intake/sources").glob(f"{sha}.*"):
                    _rm(p, report)
        try:
            from database import TelegramNotification
            await db.execute(update(TelegramNotification).where(TelegramNotification.event_key == task_id)
                             .where(TelegramNotification.deleted_at.is_(None))
                             .values(deleted_at=datetime.utcnow()))
            await db.commit()
        except Exception:
            await db.rollback()
        await delete_record(db, task_id)
    task_privacy.INDEX.invalidate()
    task_privacy.audit("deleted", task_id, by=by, removed=report["removed"], failed=report["failed"],
                       runs=len(report["runs"]), telegram=report["telegram"], youtube_video=report["youtube"])
    return report


async def _telegram_cleanup(task_id: str, *, broadcast_only: bool, reason: str) -> Dict[str, int]:
    """Delete the bot's task notices. ``broadcast_only``: only group chats (the owner's own DM stays)."""
    out = {"deleted": 0, "gone": 0, "failed": 0, "no_message_id": 0}
    try:
        from database import AsyncSessionLocal, TelegramChat, TelegramNotification
        from telegram_bot import _get_token
        token = _get_token()
        async with AsyncSessionLocal() as db:
            rows = (await db.execute(select(TelegramNotification).where(TelegramNotification.event_key == task_id)
                                     .where(TelegramNotification.deleted_at.is_(None)))).scalars().all()
            private_chats = set()
            if broadcast_only:
                private_chats = {int(c) for (c,) in (await db.execute(
                    select(TelegramChat.chat_id).where(TelegramChat.chat_type == "private"))).all()}
            bot = None
            if token:
                from telegram import Bot
                bot = Bot(token=token)
            for rec in rows:
                if broadcast_only and (int(rec.chat_id) in private_chats or int(rec.chat_id) > 0):
                    continue
                if not rec.message_id:
                    out["no_message_id"] += 1
                    continue
                if bot is None:
                    out["failed"] += 1
                    continue
                try:
                    await bot.delete_message(chat_id=int(rec.chat_id), message_id=int(rec.message_id))
                    out["deleted"] += 1
                    rec.deleted_at = datetime.utcnow()
                except Exception as exc:
                    text = str(exc).lower()
                    if "not found" in text or "can't be deleted" in text:
                        out["gone"] += 1
                        rec.deleted_at = datetime.utcnow()
                    else:
                        out["failed"] += 1
            await db.commit()
    except Exception as exc:
        print(f"[privacy] telegram cleanup {task_id}: {type(exc).__name__}: {exc}")
        out["failed"] += 1
    task_privacy.audit(f"telegram_cleanup_{reason}", task_id, **out)
    return out

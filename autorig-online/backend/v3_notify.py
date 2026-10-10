"""Telegram notifications for V3 tasks (Downloads · V3, 2026-10-11).

The classic pipeline announced a task when it was sent to a converter (tasks.start_task_on_worker) and finished
it after the poster classification (content_moderation -> reserve_and_broadcast_task_done). A V3 task never goes
to a converter and has no poster, so after the V3-only switch (2026-10-10 17:00 UTC) the owner's channel got no
«New task started» and almost no «Task completed». This module gives V3 the same two messages:

* ``schedule_new(task_id)``: once per task (``telegram_new_notified_at``), from every V3 creation path (website,
  API, Telegram, retry, generation and normalization rows when they are bound).
* ``schedule_terminal(task_id, state)``: for ``done`` and ``needs_review`` it rates the content the way the
  classic path does (the same NudeNet classifier, on the preflight render or the run's front projection), then
  sends the classic «Task completed» with the V3 viewer link and the conveyor's phase timings. Killed-and-retried
  attempts never reach it (v3_runtime_mount.after_commit returns before). Failures keep the existing error message.
Notifications never raise into the caller.
"""
from __future__ import annotations

import asyncio
import html
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

MT_ROOT = Path(os.getenv("AUTORIG_MT_ROOT", "/srv/autorig/data/motion_transfer"))
_RATING_VERSION_SUFFIX = ":v3"


def _spawn(coro) -> None:
    try:
        asyncio.get_running_loop().create_task(coro)
    except RuntimeError:                                  # no loop (a script): run it here
        asyncio.run(coro)


def _settings(task: Any) -> dict:
    try:
        value = json.loads(getattr(task, "viewer_settings", None) or "{}")
    except (TypeError, ValueError):
        value = {}
    return value if isinstance(value, dict) else {}


def run_id_of(task: Any) -> Optional[str]:
    session = (_settings(task).get("v3") or {}).get("session") or {}
    run = str(session.get("mt_run_id") or "") if isinstance(session, dict) else ""
    return run if len(run) == 20 and all(c in "0123456789abcdef" for c in run) else None


# ---------------------------------------------------------------- new task
async def notify_new(task_id: str) -> bool:
    try:
        from sqlalchemy import select, update

        from database import AsyncSessionLocal, Task
        from telegram_bot import broadcast_new_task
        from tasks import _task_notification_theme_meta

        async with AsyncSessionLocal() as db:
            res = await db.execute(update(Task).where(Task.id == task_id, Task.telegram_new_notified_at.is_(None))
                                   .values(telegram_new_notified_at=datetime.utcnow()))
            await db.commit()
            if res.rowcount != 1:
                return False
            task = await db.scalar(select(Task).where(Task.id == task_id))
        if task is None:
            return False
        meta = _task_notification_theme_meta(task)
        print(f"[V3 notify] new task {task_id}")
        await broadcast_new_task(
            task.id, task.input_url, task.input_type, None,
            via_api=bool(getattr(task, "created_via_api", False)),
            title=meta.get("title") or None, theme_name=meta.get("theme_name") or None,
            poster_path=meta.get("poster_path") or None, detector_text=meta.get("detector_text") or None,
            source_preview_url=meta.get("source_preview_url") or None)
        return True
    except Exception as exc:                              # noqa: BLE001 - never stops task creation
        print(f"[V3 notify] new-task notification for {task_id} failed: {type(exc).__name__}: {exc}")
        return False


def schedule_new(task_id: str) -> None:
    _spawn(notify_new(str(task_id)))


# ---------------------------------------------------------------- content rating
def _rating_image(task: Any) -> Optional[bytes]:
    try:
        from content_moderation import _load_preflight_render_image_bytes

        data = _load_preflight_render_image_bytes(str(task.id))
        if data:
            return data
    except Exception:                                     # noqa: BLE001
        pass
    run = run_id_of(task)
    if run:
        for name in ("front_lit.png", "front_albedo.png", "sheet_lit.png"):
            path = MT_ROOT / "runs" / run / "proj" / name
            if path.is_file() and path.stat().st_size > 0:
                return path.read_bytes()
    return None


async def rate(task_id: str) -> str:
    """The classic NudeNet content rating, on the V3 run's own picture; 'unknown' when there is none."""
    from database import AsyncSessionLocal, Task

    async with AsyncSessionLocal() as db:
        task = await db.get(Task, task_id)
        if task is None:
            return "unknown"
        if task.content_classified_at is not None and task.content_rating:
            return str(task.content_rating)
        image = await asyncio.to_thread(_rating_image, task)
        rating, score, version = "unknown", None, "v3:no_image"
        if image:
            try:
                from content_moderation import CONTENT_CLASSIFIER_VERSION, classify_image_bytes

                rating, score = await asyncio.to_thread(classify_image_bytes, image)
                version = f"{CONTENT_CLASSIFIER_VERSION}{_RATING_VERSION_SUFFIX}"
            except Exception as exc:                      # noqa: BLE001
                print(f"[V3 notify] content rating of {task_id} failed: {exc}")
                version = "v3:classify_error"
        task.content_rating, task.content_score = rating, score
        task.content_classified_at = datetime.utcnow()
        task.content_classifier_version = version[:64]
        await db.commit()
        return rating


# ---------------------------------------------------------------- completion
def phase_line(task: Any) -> str:
    """«rig 0.3 s · retarget 0.3 s · qa 1.3 s · total 10.2 s» from the run's phases.json."""
    run = run_id_of(task)
    if not run:
        return ""
    try:
        doc = json.loads((MT_ROOT / "runs" / run / "phases.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ""
    parts = []
    for ph in doc.get("phases") or []:
        if isinstance(ph, dict) and ph.get("id") in ("analysis", "rig", "retarget", "qa") and \
                isinstance(ph.get("seconds"), (int, float)):
            parts.append(f"{ph['id']} {ph['seconds']:.1f} s")
    if isinstance(doc.get("seconds"), (int, float)):
        parts.append(f"V3 {doc['seconds']:.1f} s")
    return " · ".join(parts)


def extra_html(task: Any, state: str) -> str:
    from config import APP_URL

    lines = []
    if state == "needs_review":
        qa = (_settings(task).get("v3") or {}).get("qa") or {}
        reason = "; ".join(str(r) for r in (qa.get("reasons") or []) if r)[:200] or "QA"
        lines.append(f"🟡 <b>needs review</b> · {html.escape(reason)}")
    run = run_id_of(task)
    if run:
        url = f"{(APP_URL or '').rstrip('/')}/api/mt/unity/test/index.html?run={run}"
        lines.append(f'🧊 <a href="{html.escape(url)}">V3 viewer</a>')
    timing = phase_line(task)
    if timing:
        lines.append(f"⏱ {html.escape(timing)}")
    return "\n".join(lines)


async def notify_terminal(task_id: str, state: str) -> None:
    try:
        from database import AsyncSessionLocal, Task
        from telegram_bot import reserve_and_broadcast_task_done

        await rate(task_id)
        async with AsyncSessionLocal() as db:
            task = await db.get(Task, task_id)
            extra = extra_html(task, state) if task is not None else ""
        await reserve_and_broadcast_task_done(task_id, extra_html=extra, video_wait_seconds=0)
    except Exception as exc:                              # noqa: BLE001
        print(f"[V3 notify] done notification for {task_id} failed: {type(exc).__name__}: {exc}")


def schedule_terminal(task_id: str, state: str) -> None:
    _spawn(notify_terminal(str(task_id), str(state)))

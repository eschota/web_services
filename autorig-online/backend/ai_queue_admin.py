"""Standing the whole farm queue down, for an administrator only.

The render queue is shared by every page on the site, so a queue full of work
nobody wants any more costs everybody else their turn. The composition editor
can already cancel the jobs it started; this is the blunt version of the same
idea for the person who owns the farm: cancel everything that has not begun.

Nothing that is already rendering is touched. A job on a card has spent real
GPU minutes and its output is usually still wanted, and the queue's own cancel
would have to interrupt a worker to stop it — so "clear the queue" means the
queue, not the work.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

logger = logging.getLogger(__name__)

# The same cookie `/admin` reads. Kept here as a literal because the login flow
# that sets it lives in main.py, which must not be imported from a router.
SESSION_COOKIE = "session"
CLEAR_TIMEOUT_SECONDS = 60.0


async def viewer_is_admin(request: Request) -> bool:
    """Is the browser making this request signed in as an administrator?

    A soft question with a soft answer: this decides whether a button is drawn,
    never whether anything may happen. Every change of state goes through the
    dependency below, which is main.py's own admin check.
    """
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return False
    try:
        from auth import get_user_by_session
        from config import is_admin_email
        from database import AsyncSessionLocal

        async with AsyncSessionLocal() as db:
            user = await get_user_by_session(db, token)
        return bool(user is not None and is_admin_email(user.email))
    except Exception:
        # A status widget must never take a page down with it.
        logger.exception("Could not resolve the viewer's admin status")
        return False


async def clear_pending_renders(base_url: str) -> Dict[str, int]:
    """Ask the render service to drop everything that has not started."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            base_url.rstrip("/") + "/api-render/cancel-pending",
            timeout=CLEAR_TIMEOUT_SECONDS,
        )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("the render queue returned an unexpected answer")
    return {
        "cancelled_int": int(payload.get("cancelled_int") or 0),
        "running_untouched_int": int(payload.get("running_untouched_int") or 0),
        "pending_seen_int": int(payload.get("pending_seen_int") or 0),
    }


def build_queue_admin_router(require_admin: Callable[..., Any]) -> APIRouter:
    """The router, built around the caller's own admin dependency.

    Taking the dependency as an argument keeps this module importable from
    anywhere: main.py owns the session and the user table, and a router that
    imported main.py back would be a cycle.
    """
    router = APIRouter()

    @router.post("/api/ai/queue/clear")
    async def api_clear_queue(_admin=Depends(require_admin)) -> Dict[str, Any]:
        import ai_vision_api

        try:
            counts = await clear_pending_renders(ai_vision_api.RENDERFIN_BASE)
        except Exception as exc:
            logger.exception("Could not clear the render queue")
            raise HTTPException(
                status_code=502, detail=f"the render queue could not be reached: {exc}"
            ) from None
        return {
            "success_bool": True,
            "cancelled_int": counts["cancelled_int"],
            "running_untouched_int": counts["running_untouched_int"],
            "note_string": "Jobs already on a card are left to finish",
            "server_time_unix_int": int(time.time()),
        }

    @router.post("/api/ai/farm/reset")
    async def api_farm_reset(dry_run: int = 0, admin=Depends(require_admin)) -> Dict[str, Any]:
        """Admin: wipe the whole farm queue (queued and running) and the boxes' queues.

        dry_run=1 only lists what would be cancelled. Nothing in the result
        caches, the rendered files or the models is removed.
        """
        import ai_vision_api

        who = str(getattr(admin, "email", "") or "admin")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    ai_vision_api.RENDERFIN_BASE.rstrip("/") + "/api-render/reset",
                    params={"dry_run": 1 if dry_run else 0}, timeout=180.0)
            response.raise_for_status()
            result = response.json()
        except Exception as exc:
            logger.exception("Farm reset failed")
            raise HTTPException(status_code=502, detail=f"the render queue could not be reset: {exc}") from None
        if not dry_run:
            logger.warning("FARM RESET by %s: cancelled %s queued, %s running; boxes %s", who,
                           result.get("cancelled_queued_int"), result.get("cancelled_running_int"),
                           result.get("boxes_object"))
        result.pop("task_ids_array", None)
        result.update({"success_bool": True, "by_string": who, "server_time_unix_int": int(time.time())})
        return result

    @router.get("/api/ai/queue/admin")
    async def api_queue_admin(request: Request) -> Dict[str, Any]:
        """Whether this browser would be allowed to clear the queue."""
        return {"success_bool": True, "admin_bool": await viewer_is_admin(request)}

    return router

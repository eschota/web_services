"""Conversion funnel (owner 2026-10-12: «нам нужна конверсия, она сейчас ведётся на сайте?»).

* ``POST /api/funnel/event``: the pages report a funnel step (whitelisted names) with the visitor's first-touch
  source (cookie ``ar_src``: utm_source / referrer host / direct, set by static/js/funnel.js). One JSON line per
  event in ``/srv/autorig/data/var/funnel/<YYYY-MM-DD>.jsonl`` (O_APPEND, no personal data: a hashed visitor id).
  The pages send the same steps to GA4 with gtag.
* ``GET /api/admin/funnel?days=7``: per UTC day, the database facts (new visitors, sign-ups, tasks created and
  finished, checkout clicks, subscription purchases, completion emails sent and their clicks) and the page
  events by source. Admins only.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

FUNNEL_DIR = Path(os.getenv("AUTORIG_FUNNEL_DIR", "/srv/autorig/data/var/funnel"))
EVENTS = ("landing", "upload_start", "task_created", "task_create_failed", "download_click", "paywall_view",
          "checkout_start", "purchase", "sign_up_prompt")
_SRC = re.compile(r"[^a-z0-9._-]+")
_MAX_PER_MINUTE = 120
_RATE: dict[str, list[float]] = {}


def clean_source(value: Any) -> str:
    value = _SRC.sub("", str(value or "").strip().lower())[:40]
    return value or "direct"


def record(event: str, *, source: str = "direct", visitor: str = "", task_id: Optional[str] = None,
           props: Optional[dict] = None) -> bool:
    """One funnel event line (never raises)."""
    if event not in EVENTS:
        return False
    row = {"t": round(time.time(), 1), "e": event, "s": clean_source(source),
           "v": hashlib.sha256(str(visitor or "").encode()).hexdigest()[:16] if visitor else None}
    if task_id and re.fullmatch(r"[0-9a-f-]{36}", str(task_id)):
        row["task"] = str(task_id)
    if isinstance(props, dict):
        row["p"] = {str(k)[:24]: (v if isinstance(v, (int, float, bool)) else str(v)[:40])
                    for k, v in list(props.items())[:8]}
    try:
        FUNNEL_DIR.mkdir(parents=True, exist_ok=True)
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        line = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")
        fd = os.open(FUNNEL_DIR / f"{day}.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(fd, line)
        finally:
            os.close(fd)
        return True
    except OSError:
        return False


def page_events(days: int) -> dict:
    """{day: {source: {event: count, event+"_visitors": unique}}} from the JSONL files."""
    out: dict = {}
    today = datetime.now(timezone.utc).date()
    for i in range(days):
        day = (today - timedelta(days=i)).isoformat()
        counts: dict = defaultdict(lambda: defaultdict(int))
        uniq: dict = defaultdict(lambda: defaultdict(set))
        try:
            with open(FUNNEL_DIR / f"{day}.jsonl", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    counts[row.get("s") or "direct"][row.get("e")] += 1
                    if row.get("v"):
                        uniq[row.get("s") or "direct"][row.get("e")].add(row["v"])
        except OSError:
            pass
        out[day] = {src: {**ev, **{f"{e}_visitors": len(v) for e, v in uniq[src].items()}}
                    for src, ev in counts.items()}
    return out


DB_QUERIES = {
    "new_visitors": "select date(created_at) d, count(*) from anon_sessions where created_at >= :since group by d",
    "sign_ups": "select date(created_at) d, count(*) from users where created_at >= :since group by d",
    "tasks_created": "select date(created_at) d, count(*) from tasks where created_at >= :since group by d",
    "tasks_created_signed_in": "select date(created_at) d, count(*) from tasks where created_at >= :since "
                               "and owner_type = 'user' group by d",
    "v3_finished": "select date(updated_at) d, count(*) from tasks where updated_at >= :since "
                   "and pipeline_kind = 'v3' and status in ('done', 'needs_review') group by d",
    "checkout_start": "select date(created_at) d, count(*) from purchase_checkout_intents where created_at >= :since "
                      "group by d",
    "purchases": "select date(created_at) d, count(*) from gumroad_purchases where created_at >= :since "
                 "and coalesce(test, 0) = 0 and coalesce(refunded, 0) = 0 group by d",
    "subscription_purchases": "select date(created_at) d, count(*) from gumroad_purchases where created_at >= :since "
                              "and coalesce(test, 0) = 0 and coalesce(refunded, 0) = 0 "
                              "and product_permalink like 'autorig-unlimited%' group by d",
    "completion_emails_sent": "select date(sent_at) d, count(*) from task_completion_emails where sent_at >= :since "
                              "and status = 'sent' group by d",
    "email_clicks": "select date(clicked_at) d, count(*) from email_campaign_clicks where clicked_at >= :since "
                    "group by d",
    "email_delivery_events": "select date(created_at) d, count(*) from email_delivery_events "
                             "where created_at >= :since group by d",
}


def build_funnel_router(*, get_db: Callable[..., Any], get_current_user: Callable[..., Any],
                        is_admin_email: Callable[[Optional[str]], bool]) -> APIRouter:
    router = APIRouter()

    @router.post("/api/funnel/event")
    async def funnel_event(request: Request):
        try:
            body = await request.json()
        except ValueError:
            body = {}
        if not isinstance(body, dict) or body.get("event") not in EVENTS:
            return JSONResponse({"ok": False}, status_code=400)
        ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "")
        now = time.time()
        hits = [t for t in _RATE.get(ip, []) if now - t < 60]
        if len(hits) >= _MAX_PER_MINUTE:
            return JSONResponse({"ok": False}, status_code=429)
        _RATE[ip] = hits + [now]
        if len(_RATE) > 5000:
            _RATE.clear()
        visitor = request.cookies.get("anon_id") or request.cookies.get("session_token") or ip
        source = body.get("source") or request.cookies.get("ar_src") or "direct"
        ok = record(body["event"], source=source, visitor=visitor, task_id=body.get("task_id"),
                    props=body.get("props") if isinstance(body.get("props"), dict) else None)
        return JSONResponse({"ok": ok}, headers={"Cache-Control": "no-store"})

    @router.get("/api/admin/funnel")
    async def admin_funnel(days: int = 7, user: Any = Depends(get_current_user), db: Any = Depends(get_db)):
        if user is None or not is_admin_email(getattr(user, "email", None)):
            raise HTTPException(status_code=403, detail="admin only")
        days = max(1, min(int(days or 7), 90))
        since = (datetime.utcnow() - timedelta(days=days - 1)).strftime("%Y-%m-%d 00:00:00")
        facts: dict = defaultdict(dict)
        for name, sql in DB_QUERIES.items():
            try:
                rows = (await db.execute(text(sql), {"since": since})).all()
            except Exception as exc:  # a missing table or column never breaks the report
                facts["_errors"][name] = str(exc)[:120]
                continue
            for day, count in rows:
                facts[str(day)][name] = int(count)
        return JSONResponse({"schema": "autorig.funnel/1", "days": days, "since": since,
                             "db": dict(sorted(facts.items(), reverse=True)),
                             "pages": page_events(days),
                             "notes": {"pages": "page events by first-touch source (static/js/funnel.js); also sent "
                                                "to GA4 with gtag",
                                       "purchases": "gumroad_purchases (webhook), subscription = autorig-unlimited*"}},
                            headers={"Cache-Control": "no-store"})

    return router

"""Backfill of the V3 switch (owner OK 2026-10-12, option 2): ONE digest email per signed-in owner whose V3 tasks
finished since 2026-10-10 15:00 UTC without a completion email. Same guards (signed-in, not opted out, address not
invalid), throttled, every send logged to /srv/autorig/data/var/funnel/backfill-20261012.jsonl.
    python3 backfill.py            # dry run
    python3 backfill.py --send
"""
import asyncio
import json
import sys
import time
from collections import defaultdict

sys.path.insert(0, ".")
from sqlalchemy import text  # noqa: E402

from database import AsyncSessionLocal  # noqa: E402
import email_service  # noqa: E402
import v3_notify  # noqa: E402

SEND = "--send" in sys.argv
LOG = "/srv/autorig/data/var/funnel/backfill-20261012.jsonl"
SQL = """select t.id, t.owner_id, t.poster_llm_title, u.preferred_language, u.detected_language
from tasks t join users u on u.email = t.owner_id
where t.pipeline_kind = 'v3' and t.owner_type = 'user' and t.status in ('done', 'needs_review')
  and t.created_at >= '2026-10-10 15:00:00'
  and coalesce(u.email_task_completed, 1) = 1 and u.email_invalid_at is null
  and t.id not in (select task_id from task_completion_emails)
order by t.owner_id, t.created_at"""


async def main():
    async with AsyncSessionLocal() as db:
        rows = (await db.execute(text(SQL))).all()
    owners = defaultdict(list)
    meta = {}
    for task_id, owner, title, pref, detected in rows:
        owners[owner].append((task_id, title))
        meta[owner] = (pref, detected)
    print(f"{len(rows)} tasks, {len(owners)} owners, send={SEND}")
    sent = failed = 0
    for owner, tasks in owners.items():
        pref, detected = meta[owner]
        lang = v3_notify.owner_ui_language(type("U", (), {"preferred_language": pref, "detected_language": detected})())
        if not SEND:
            print(f"  would send {len(tasks)} task(s) lang={lang}")
            continue
        res = await email_service.send_task_digest_email(owner, tasks, lang=lang, campaign="backfill_20261012",
                                                         poster_dir=str(v3_notify.V3_POSTER_DIR))
        sent += bool(res.get("sent"))
        failed += not res.get("sent")
        with open(LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"t": round(time.time(), 1), "owner_hash": email_service._completion_email_recipient_hash(owner),
                                 "lang": lang, "tasks": res.get("tasks"), "sent": res.get("sent"),
                                 "error": res.get("error"), "id": res.get("id")}) + "\n")
        print(f"  {'sent' if res.get('sent') else 'FAILED'} {len(res.get('tasks') or [])} task(s) lang={lang} {res.get('error') or ''}")
        await asyncio.sleep(3)                     # throttle: sender reputation and the Resend rate limit
    print(f"done: sent={sent} failed={failed}")


asyncio.run(main())

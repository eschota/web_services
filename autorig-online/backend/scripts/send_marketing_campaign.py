#!/usr/bin/env python3
"""
Resumable marketing campaign sender for AutoRig.online.

Run from /root/autorig-online/backend:

  PYTHONPATH=. python3 scripts/send_marketing_campaign.py --dry-run
  PYTHONPATH=. python3 scripts/send_marketing_campaign.py --send-test admin@example.com --allow-missing-postal-address
  PYTHONPATH=. python3 scripts/send_marketing_campaign.py --yes-live --limit 25 --allow-missing-postal-address
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

_BACKEND = Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

try:
    from dotenv import load_dotenv

    load_dotenv("/etc/autorig-backend.env")
except Exception:
    pass

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from config import MARKETING_POSTAL_ADDRESS
from database import AsyncSessionLocal, EmailCampaignSend, User, init_db
from email_service import send_marketing_campaign_email


DEFAULT_CAMPAIGN_KEY = "autorig-v2-animal-humanoid-202605"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass(frozen=True)
class Recipient:
    user_id: int | None
    email: str
    email_hash: str


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def hash_email(email: str) -> str:
    return hashlib.sha256(normalize_email(email).encode("utf-8")).hexdigest()


def mask_email(email: str) -> str:
    normalized = normalize_email(email)
    if "@" not in normalized:
        return "***"
    local, domain = normalized.split("@", 1)
    if len(local) <= 2:
        masked_local = local[:1] + "*"
    else:
        masked_local = local[:2] + "***" + local[-1:]
    return masked_local + "@" + domain


def chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


async def load_recipients(db, campaign_key: str) -> tuple[list[Recipient], dict[str, int]]:
    rs = await db.execute(
        select(User)
        .where(User.email.is_not(None))
        .where(User.email != "")
        .where(User.email_task_completed.is_(True))
        .where(User.email_marketing_unsubscribed_at.is_(None))
        .where(User.email_invalid_at.is_(None))
        .order_by(User.id.asc())
    )
    seen: set[str] = set()
    all_recipients: list[Recipient] = []
    invalid_count = 0
    for user in rs.scalars().all():
        email = normalize_email(user.email)
        if not EMAIL_RE.match(email):
            invalid_count += 1
            continue
        if email in seen:
            continue
        seen.add(email)
        all_recipients.append(Recipient(user.id, email, hash_email(email)))

    existing: dict[str, str] = {}
    hashes = [r.email_hash for r in all_recipients]
    for part in chunks(hashes, 500):
        if not part:
            continue
        ers = await db.execute(
            select(EmailCampaignSend.email_hash, EmailCampaignSend.status)
            .where(EmailCampaignSend.campaign_key == campaign_key)
            .where(EmailCampaignSend.email_hash.in_(part))
        )
        for email_hash, status in ers.all():
            existing[email_hash] = status

    invalid_suppressed_rs = await db.execute(
        select(func.count(User.id)).where(User.email_invalid_at.is_not(None))
    )
    invalid_suppressed = int(invalid_suppressed_rs.scalar_one() or 0)

    status_rs = await db.execute(
        select(EmailCampaignSend.status, func.count(EmailCampaignSend.id))
        .where(EmailCampaignSend.campaign_key == campaign_key)
        .group_by(EmailCampaignSend.status)
    )
    status_counts = {status or "unknown": count for status, count in status_rs.all()}
    remaining = [r for r in all_recipients if r.email_hash not in existing]
    stats = {
        "eligible_total": len(all_recipients),
        "invalid_email_skipped": invalid_count,
        "invalid_suppressed": invalid_suppressed,
        "already_logged": len(existing),
        "already_sent": sum(1 for v in existing.values() if v == "sent"),
        "remaining": len(remaining),
        "status_counts": status_counts,
    }
    return remaining, stats


async def insert_send_row(db, campaign_key: str, recipient: Recipient) -> EmailCampaignSend | None:
    now = datetime.utcnow()
    row = EmailCampaignSend(
        campaign_key=campaign_key,
        user_id=recipient.user_id,
        email_hash=recipient.email_hash,
        status="sending",
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    try:
        await db.commit()
        await db.refresh(row)
        return row
    except IntegrityError:
        await db.rollback()
        return None


async def update_send_row(db, row: EmailCampaignSend, result: dict) -> None:
    now = datetime.utcnow()
    row.updated_at = now
    if result.get("ok"):
        row.status = "sent"
        row.sent_at = now
        row.provider_message_id = result.get("provider_message_id")
        row.error = None
    else:
        row.status = "error"
        row.error = (result.get("error") or "unknown error")[:4000]
    await db.commit()


async def run_dry(campaign_key: str, sample_count: int) -> int:
    await init_db()
    async with AsyncSessionLocal() as db:
        remaining, stats = await load_recipients(db, campaign_key)
    output = {
        "campaign_key": campaign_key,
        **stats,
        "sample_remaining_masked": [mask_email(r.email) for r in remaining[:sample_count]],
        "postal_address_configured": bool(MARKETING_POSTAL_ADDRESS),
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


async def run_test(campaign_key: str, email: str, allow_missing_postal_address: bool, force_test: bool) -> int:
    await init_db()
    normalized = normalize_email(email)
    if not EMAIL_RE.match(normalized):
        print(json.dumps({"recipient": mask_email(email), "ok": False, "error": "invalid email"}, indent=2, ensure_ascii=False))
        return 2

    email_hash = hash_email(normalized)
    test_campaign_key = campaign_key + "-test"
    async with AsyncSessionLocal() as db:
        user_id = None
        user_rs = await db.execute(
            select(User.id)
            .where(func.lower(func.trim(User.email)) == normalized)
            .order_by(User.id.asc())
            .limit(1)
        )
        user_id = user_rs.scalar_one_or_none()

        if not force_test:
            existing_rs = await db.execute(
                select(EmailCampaignSend.campaign_key, EmailCampaignSend.status, EmailCampaignSend.sent_at)
                .where(EmailCampaignSend.email_hash == email_hash)
                .where(EmailCampaignSend.campaign_key.in_([campaign_key, test_campaign_key]))
                .order_by(EmailCampaignSend.created_at.asc())
            )
            existing = existing_rs.first()
            if existing:
                print(json.dumps({
                    "recipient": mask_email(normalized),
                    "ok": True,
                    "skipped": True,
                    "reason": "already_logged_for_campaign",
                    "existing_campaign_key": existing[0],
                    "existing_status": existing[1],
                    "existing_sent_at": existing[2].isoformat() if existing[2] else None,
                    "hint": "Use --force-test only when an intentional duplicate test email is required.",
                }, indent=2, ensure_ascii=False))
                return 0

        recipient = Recipient(user_id=user_id, email=normalized, email_hash=email_hash)
        row = await insert_send_row(db, test_campaign_key, recipient)
        if row is None:
            print(json.dumps({
                "recipient": mask_email(normalized),
                "ok": True,
                "skipped": True,
                "reason": "test_send_already_logged",
                "campaign_key": test_campaign_key,
            }, indent=2, ensure_ascii=False))
            return 0

    result = await send_marketing_campaign_email(
        normalized,
        test_campaign_key,
        allow_missing_postal_address=allow_missing_postal_address,
    )
    async with AsyncSessionLocal() as db:
        db_row = await db.get(EmailCampaignSend, row.id)
        if db_row is not None:
            await update_send_row(db, db_row, result)
    print(json.dumps({"recipient": mask_email(email), **result}, indent=2, ensure_ascii=False))
    return 0 if result.get("ok") else 1


async def run_live(args) -> int:
    if not MARKETING_POSTAL_ADDRESS and not args.allow_missing_postal_address:
        print(
            "ERROR: MARKETING_POSTAL_ADDRESS is not configured; refusing live send. "
            "Pass --allow-missing-postal-address to accept this deliverability/compliance risk."
        )
        return 2
    if args.limit <= 0:
        print("ERROR: --limit must be greater than 0 for live sends.")
        return 2

    await init_db()
    async with AsyncSessionLocal() as db:
        remaining, stats = await load_recipients(db, args.campaign)
        batch = remaining[: args.limit]
        print(json.dumps({"campaign_key": args.campaign, **stats, "selected_for_this_run": len(batch)}, indent=2))

        recent: list[bool] = []
        sent = 0
        failed = 0
        skipped = 0
        for index, recipient in enumerate(batch, start=1):
            row = await insert_send_row(db, args.campaign, recipient)
            if row is None:
                skipped += 1
                print(f"[{index}/{len(batch)}] skip already logged {mask_email(recipient.email)}")
                continue

            result = await send_marketing_campaign_email(
                recipient.email,
                args.campaign,
                allow_missing_postal_address=args.allow_missing_postal_address,
            )
            await update_send_row(db, row, result)

            ok = bool(result.get("ok"))
            recent.append(ok)
            recent = recent[-50:]
            if ok:
                sent += 1
                print(f"[{index}/{len(batch)}] sent {mask_email(recipient.email)} id={result.get('provider_message_id') or '-'}")
            else:
                failed += 1
                error = (result.get("error") or "unknown error").replace("\n", " ")[:240]
                print(f"[{index}/{len(batch)}] error {mask_email(recipient.email)} {error}")
                if "429" in error or "rate limit" in error.lower():
                    print("ERROR: provider rate limit detected; stopping this run.")
                    break

            if len(recent) >= 5 and all(not x for x in recent[-5:]):
                print("ERROR: five consecutive send failures; stopping this run.")
                break
            if len(recent) >= 50 and (recent.count(False) / len(recent)) > 0.05:
                print("ERROR: failure rate above 5% in the last 50 sends; stopping this run.")
                break

            if index < len(batch):
                delay = max(0.0, args.delay_seconds + random.uniform(0, args.jitter_seconds))
                if delay:
                    await asyncio.sleep(delay)

        print(json.dumps({"sent": sent, "failed": failed, "skipped": skipped}, indent=2))
    return 0 if failed == 0 else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send the AutoRig V2 marketing campaign safely.")
    parser.add_argument("--campaign", default=DEFAULT_CAMPAIGN_KEY)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--send-test", metavar="EMAIL")
    parser.add_argument(
        "--force-test",
        action="store_true",
        help="Allow an intentional duplicate test email to an address already logged for this campaign.",
    )
    parser.add_argument("--yes-live", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--allow-missing-postal-address",
        action="store_true",
        help="Allow sending without MARKETING_POSTAL_ADDRESS. This is a compliance/deliverability risk.",
    )
    parser.add_argument("--delay-seconds", type=float, default=360.0)
    parser.add_argument("--jitter-seconds", type=float, default=0.0)
    parser.add_argument("--sample", type=int, default=5)
    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()
    if args.send_test:
        return await run_test(args.campaign, args.send_test, args.allow_missing_postal_address, args.force_test)
    if args.yes_live:
        return await run_live(args)
    return await run_dry(args.campaign, args.sample)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))

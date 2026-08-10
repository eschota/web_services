"""Tell Telegram the moment the vision credentials start running out.

Twice now a single dead OpenAI key has cost real product quietly. On
2026-08-09 the credit balance emptied at 07:56 UTC: 86 tasks went to the farm
with no metadata and 50 uploads were routed as "not riggable" and finished as
bare meshes. Nothing alerted. The outage ran ten hours and was found by a user
noticing the titles were wrong.

The balance itself cannot be read with the keys this server holds - OpenAI
answers 403 to /dashboard/billing/credit_grants (browser session key only) and
to /v1/organization/costs (needs an admin key with api.usage.read). So the
signal used here is the ladder in content_moderation._llm_candidates: if the
first credential fails on quota or auth and a later one answers, the service is
already running on its reserve. That is the warning, and it arrives before
anything is degraded rather than after. When no credential answers at all, that
is the outage itself and it is reported as critical.

Everything here is best-effort and synchronous on purpose: the callers run
inside asyncio.to_thread, where there is no event loop to await on, and an
alert that raises must never be the reason a generation fails.
"""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import List, Optional

import httpx

# One message per level per hour. The vision path runs on every task, so an
# unthrottled alert would arrive hundreds of times during a single outage.
ALERT_INTERVAL_SECONDS = float(os.getenv("AUTORIG_LLM_ALERT_INTERVAL", "3600"))
STAMP_DIR = Path(os.getenv("AUTORIG_LLM_ALERT_STAMP_DIR", "/var/autorig"))

LEVEL_DEGRADED = "degraded"
LEVEL_EXHAUSTED = "exhausted"

# Errors that mean "this credential is out of money or no longer valid", as
# opposed to a timeout or a bad request, which say nothing about the balance.
_CREDIT_MARKERS = (
    "insufficient_quota",
    "credit_balance_exhausted",
    "no credits remaining",
    "exceeded your current quota",
    "billing_hard_limit_reached",
)
_AUTH_MARKERS = (
    "invalid_api_key",
    "incorrect api key",
    "error code: 401",
)


def is_credit_error(text: str) -> bool:
    low = (text or "").lower()
    return any(m in low for m in _CREDIT_MARKERS)


def is_auth_error(text: str) -> bool:
    low = (text or "").lower()
    return any(m in low for m in _AUTH_MARKERS)


def _throttled(level: str) -> bool:
    """True when this level was already announced inside the interval.

    A file rather than a DB row: the callers are worker threads in more than
    one process, and a stamp is the one guard that holds across all of them
    without a write transaction.
    """
    stamp = STAMP_DIR / f"llm_credit_alert_{level}.stamp"
    now = time.time()
    try:
        if stamp.is_file() and (now - stamp.stat().st_mtime) < ALERT_INTERVAL_SECONDS:
            return True
        stamp.parent.mkdir(parents=True, exist_ok=True)
        stamp.write_text(str(int(now)), encoding="utf-8")
    except Exception:
        # Cannot throttle - better to send than to stay silent about money.
        return False
    return False


def _broadcast_chat_ids() -> List[int]:
    """Group chats subscribed to site notifications, never private DMs.

    Mirrors telegram_bot.get_broadcast_chat_ids, read synchronously so this can
    run off the event loop.
    """
    ids: List[int] = []
    raw = os.getenv("TELEGRAM_NOTIFICATION_CHAT_ID", "").strip()
    if raw:
        try:
            if int(raw):
                ids.append(int(raw))
        except ValueError:
            pass
    db_path = Path(__file__).resolve().parent / "db" / "autorig.db"
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        try:
            rows = db.execute(
                "SELECT chat_id, chat_type FROM telegram_chats WHERE is_active = 1"
            ).fetchall()
        finally:
            db.close()
        for chat_id, chat_type in rows:
            if str(chat_type or "").lower() == "private":
                continue
            if int(chat_id) not in ids:
                ids.append(int(chat_id))
    except Exception as exc:
        print(f"[LLMCredit] cannot read chat list: {exc}")
    return ids


def _send(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        print("[LLMCredit] no TELEGRAM_BOT_TOKEN, alert not sent")
        return
    chat_ids = _broadcast_chat_ids()
    if not chat_ids:
        print("[LLMCredit] no broadcast chats, alert not sent")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chat_id in chat_ids:
        try:
            resp = httpx.post(
                url,
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=15.0,
            )
            if resp.status_code != 200:
                print(f"[LLMCredit] chat {chat_id}: HTTP {resp.status_code} {resp.text[:120]}")
        except Exception as exc:
            print(f"[LLMCredit] chat {chat_id}: {exc}")


def warn_running_on_reserve(failed_label: str, working_label: str, detail: str) -> None:
    """A credential is out of money; a later one in the ladder covered for it.

    This is the early warning: nothing is broken yet, but the reserve is what
    is paying for the requests, and when it goes too the pipeline loses
    metadata and rigging routing.
    """
    if _throttled(LEVEL_DEGRADED):
        return
    kind = "кончились кредиты" if is_credit_error(detail) else "ключ отклонён"
    _send(
        "⚠️ <b>LLM: работаем на резервном ключе</b>\n"
        f"Основной (<code>{failed_label}</code>) — {kind}.\n"
        f"Запросы вытягивает <code>{working_label}</code>.\n"
        f"<code>{(detail or '')[:180]}</code>\n\n"
        "Пока всё работает, но резерв — последний. Пополните основной аккаунт, "
        "иначе следом отвалятся метаданные и риггинг."
    )
    print(f"[LLMCredit] alert sent: running on reserve ({failed_label} -> {working_label})")


def alert_all_credentials_down(detail: str) -> None:
    """Nothing in the ladder answered: the vision pipeline is down right now."""
    if _throttled(LEVEL_EXHAUSTED):
        return
    _send(
        "🚨 <b>LLM: не отвечает ни один ключ</b>\n"
        f"<code>{(detail or '')[:200]}</code>\n\n"
        "Сейчас это значит: у новых задач не будет метаданных (тайтлы "
        "выродятся в имя файла), а сайтовые генерации встанут в ожидание "
        "вместо риггинга. Нужно пополнить баланс или заменить ключ."
    )
    print("[LLMCredit] alert sent: every credential failed")


def report_balance(remaining_usd: float, threshold_usd: float) -> None:
    """Optional true low-balance warning, used when an admin key is available."""
    if _throttled(LEVEL_DEGRADED):
        return
    _send(
        "⚠️ <b>LLM: баланс заканчивается</b>\n"
        f"Осталось <b>${remaining_usd:.2f}</b> (порог ${threshold_usd:.2f}).\n\n"
        "Пополните, пока пайплайн не перешёл на резервный ключ."
    )
    print(f"[LLMCredit] alert sent: balance ${remaining_usd:.2f}")

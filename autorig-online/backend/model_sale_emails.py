from __future__ import annotations

from html import escape
from typing import Any

import resend

from config import APP_URL, EMAIL_FROM, RESEND_API_KEY


ADMIN_SALE_EMAIL = "eschota@gmail.com"


def _money(amount_cents: int) -> str:
    return f"${amount_cents / 100:.2f}"


def _task_url(task_id: str) -> str:
    return f"{APP_URL.rstrip('/')}/task?id={task_id}"


def _send(to_email: str, subject: str, html: str) -> tuple[bool, str | None]:
    if not RESEND_API_KEY:
        return False, "RESEND_API_KEY is not configured"
    try:
        resend.api_key = RESEND_API_KEY
        response = resend.Emails.send(
            {
                "from": f"AutoRig.online <{EMAIL_FROM}>",
                "to": [to_email],
                "subject": subject,
                "html": html,
            }
        )
        provider_id = response.get("id") if isinstance(response, dict) else getattr(response, "id", None)
        return True, str(provider_id) if provider_id else None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


async def send_new_offer_emails(
    *,
    task_id: str,
    author_email: str,
    buyer_email: str,
    amount_cents: int,
    accept_token: str,
) -> dict[str, Any]:
    amount = _money(amount_cents)
    task_url = _task_url(task_id)
    accept_url = f"{APP_URL.rstrip('/')}/model-sale/accept?token={accept_token}"
    author_html = f"""
    <div style="font-family:Arial,sans-serif;max-width:640px;margin:auto">
      <h2>New offer for your 3D model: {escape(amount)}</h2>
      <p><b>{escape(buyer_email)}</b> offered <b>{escape(amount)}</b> for the model in task
      <a href="{escape(task_url)}">{escape(task_id)}</a>.</p>
      <p>This is an offer only. No files or payment access will be transferred automatically.</p>
      <p><a href="{escape(accept_url)}" style="display:inline-block;padding:12px 20px;background:#6366f1;color:#fff;text-decoration:none;border-radius:8px">Review and accept offer</a></p>
      <p style="color:#666;font-size:12px">The acceptance link expires in 14 days. Opening it does not accept the offer.</p>
    </div>
    """
    admin_html = f"""
    <div style="font-family:Arial,sans-serif">
      <h2>New 3D model sale offer</h2>
      <p>Task: <a href="{escape(task_url)}">{escape(task_id)}</a></p>
      <p>Author: {escape(author_email)}<br>Buyer: {escape(buyer_email)}<br>Amount: <b>{escape(amount)}</b></p>
    </div>
    """
    author_ok, author_result = _send(
        author_email, f"{amount} offer for your 3D model - AutoRig.online", author_html
    )
    admin_ok, admin_result = _send(
        ADMIN_SALE_EMAIL, f"New model offer {amount} - {task_id}", admin_html
    )
    return {
        "author_sent": author_ok,
        "author_result": author_result,
        "admin_sent": admin_ok,
        "admin_result": admin_result,
    }


async def send_offer_accepted_emails(
    *,
    task_id: str,
    author_email: str,
    buyer_email: str,
    amount_cents: int,
) -> dict[str, Any]:
    amount = _money(amount_cents)
    task_url = _task_url(task_id)
    buyer_html = f"""
    <div style="font-family:Arial,sans-serif">
      <h2>Your {escape(amount)} offer was accepted</h2>
      <p>The author agreed to sell the model from task
      <a href="{escape(task_url)}">{escape(task_id)}</a>.</p>
      <p>AutoRig.online administration will contact you to complete payment and delivery. No download access has been granted yet.</p>
    </div>
    """
    admin_html = f"""
    <div style="font-family:Arial,sans-serif">
      <h2>3D model offer accepted</h2>
      <p>Task: <a href="{escape(task_url)}">{escape(task_id)}</a></p>
      <p>Author: {escape(author_email)}<br>Buyer: {escape(buyer_email)}<br>Amount: <b>{escape(amount)}</b></p>
      <p>Please complete the transaction manually. The website has not granted file access.</p>
    </div>
    """
    buyer_ok, buyer_result = _send(
        buyer_email, f"Your {amount} model offer was accepted - AutoRig.online", buyer_html
    )
    admin_ok, admin_result = _send(
        ADMIN_SALE_EMAIL, f"ACCEPTED model offer {amount} - {task_id}", admin_html
    )
    return {
        "buyer_sent": buyer_ok,
        "buyer_result": buyer_result,
        "admin_sent": admin_ok,
        "admin_result": admin_result,
    }

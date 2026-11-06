"""
Email Service for AutoRig Online
Uses Resend API for sending emails
"""
import asyncio
import base64
import hashlib
import httpx
import resend
from datetime import datetime, timedelta
from html import escape
from typing import Optional
from urllib.parse import quote

from sqlalchemy import or_, update

from config import RESEND_API_KEY, EMAIL_FROM, APP_URL, MARKETING_POSTAL_ADDRESS
from database import AsyncSessionLocal, TaskCompletionEmail
from unsubscribe_tokens import (
    build_unsubscribe_token,
    build_marketing_unsubscribe_token,
    build_campaign_click_token,
)


# Initialize Resend
resend.api_key = RESEND_API_KEY

_completion_email_claim_lock = asyncio.Lock()
_COMPLETION_EMAIL_RECLAIM_AFTER = timedelta(minutes=15)


def _completion_email_recipient_hash(email: str) -> str:
    return hashlib.sha256(str(email or "").strip().lower().encode("utf-8")).hexdigest()


async def _reserve_task_completion_email(task_id: str, to_email: str) -> bool:
    """Claim one completion email, allowing only stale/failed claims to retry."""
    now = datetime.utcnow()
    stale_before = now - _COMPLETION_EMAIL_RECLAIM_AFTER
    recipient_hash = _completion_email_recipient_hash(to_email)
    async with _completion_email_claim_lock:
        async with AsyncSessionLocal() as db:
            row = await db.get(TaskCompletionEmail, task_id)
            if row is None:
                db.add(
                    TaskCompletionEmail(
                        task_id=task_id,
                        recipient_hash=recipient_hash,
                        status="sending",
                        attempt_count=1,
                        claimed_at=now,
                        created_at=now,
                        updated_at=now,
                    )
                )
                try:
                    await db.commit()
                    return True
                except Exception:
                    # Another process may have inserted the same task-scoped
                    # claim. Re-read it and apply the same retry rules below.
                    await db.rollback()
                    row = await db.get(TaskCompletionEmail, task_id)

            if row is None or row.status == "sent":
                return False
            if row.status == "sending" and row.claimed_at and row.claimed_at > stale_before:
                return False

            result = await db.execute(
                update(TaskCompletionEmail)
                .where(TaskCompletionEmail.task_id == task_id)
                .where(
                    or_(
                        TaskCompletionEmail.status == "failed",
                        TaskCompletionEmail.claimed_at <= stale_before,
                    )
                )
                .values(
                    recipient_hash=recipient_hash,
                    status="sending",
                    attempt_count=TaskCompletionEmail.attempt_count + 1,
                    provider_message_id=None,
                    last_error=None,
                    claimed_at=now,
                    updated_at=now,
                )
            )
            await db.commit()
            return result.rowcount == 1


async def _finish_task_completion_email(
    task_id: str,
    *,
    sent: bool,
    provider_message_id: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    now = datetime.utcnow()
    async with AsyncSessionLocal() as db:
        await db.execute(
            update(TaskCompletionEmail)
            .where(TaskCompletionEmail.task_id == task_id)
            .values(
                status="sent" if sent else "failed",
                provider_message_id=provider_message_id,
                last_error=None if sent else str(error or "unknown email error")[:2000],
                sent_at=now if sent else None,
                updated_at=now,
            )
        )
        await db.commit()


async def download_image(image_url: str) -> Optional[bytes]:
    """Download image from URL and return bytes"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(image_url, timeout=30.0, follow_redirects=True)
            if response.status_code == 200:
                return response.content
    except Exception as e:
        print(f"[Email] Failed to download image: {e}")
    return None


EMAIL_EN = {
    "email_ready_subject": "🎉 Your 3D Model is Ready! - AutoRig.online",
    "email_ready_title": "🎉 Your 3D Model is Ready!",
    "email_ready_body": "Great news! Your auto-rigging task has been completed successfully. Your character is now ready with a full skeleton and animations.",
    "email_ready_cta": "View & Download Results",
    "email_ready_formats": "Download it as FBX for Unity and Unreal Engine, .blend for Blender, GLB, or a ZIP with every animation.",
    "email_dashboard": "Dashboard",
    "email_settings": "notification settings",
    "email_unsubscribe": "One-click unsubscribe",
    "email_visit": "Visit Website",
    "email_footer": "Automatic 3D Character Rigging",
    "email_preview_alt": "3D Model Preview",
}


def email_text(key: str, lang: Optional[str] = None) -> str:
    """An email string in the owner's interface language (static/i18n), English when it has no translation."""
    if lang and lang != "en":
        try:
            from user_language import translations

            value = translations(lang, fallback=False).get(key)
            if value:
                return value
        except Exception:                                     # noqa: BLE001 - English is always there
            pass
    return EMAIL_EN.get(key, key)


def get_email_html(
    task_id: str,
    has_image: bool = True,
    unsubscribe_url: Optional[str] = None,
    dashboard_url: Optional[str] = None,
    lang: Optional[str] = None,
    task_url: Optional[str] = None,
) -> str:
    """Generate HTML email template (``lang``: the owner's interface language; fa is right to left)."""
    T = lambda key: escape(email_text(key, lang))  # noqa: E731
    direction = "rtl" if lang == "fa" else "ltr"
    base = APP_URL.rstrip("/")
    task_url = task_url or f"{base}/task?id={task_id}"
    dash = dashboard_url or f"{base}/dashboard"
    unsub_block = ""
    if unsubscribe_url:
        unsub_block = f"""
                            <p style="color: #d0d4e0; font-size: 12px; text-align: center; margin: 20px 0 0 0; line-height: 1.5;">
                                <a href="{dash}" style="color: #a5b4fc; text-decoration: underline;">{T("email_dashboard")}</a>
                                <span style="color: #e8eaf0;"> — {T("email_settings")}</span>
                            </p>
                            <p style="color: #d0d4e0; font-size: 12px; text-align: center; margin: 10px 0 0 0;">
                                <a href="{unsubscribe_url}" style="color: #a5b4fc; text-decoration: underline;">{T("email_unsubscribe")}</a>
                            </p>
        """
    
    image_section = ""
    if has_image:
        image_section = f"""
        <div style="text-align: center; margin: 30px 0;">
            <a href="{task_url}" style="display: inline-block; text-decoration: none; border-radius: 12px;">
                <img src="cid:preview_image" alt="{T("email_preview_alt")}" border="0" style="display: block; max-width: 100%; height: auto; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.3);">
            </a>
        </div>
        """
    
    return f"""
<!DOCTYPE html>
<html lang="{escape(lang or 'en')}" dir="{direction}">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; background-color: #0a0a0f; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #0a0a0f; padding: 40px 20px;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color: #1a1a24; border-radius: 16px; overflow: hidden;">
                    <!-- Header -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); padding: 30px; text-align: center;">
                            <h1 style="color: #ffffff; margin: 0; font-size: 28px; font-weight: 700;">AutoRig.online</h1>
                        </td>
                    </tr>
                    
                    <!-- Content -->
                    <tr>
                        <td style="padding: 40px 30px;">
                            <h2 style="color: #f0f0f5; margin: 0 0 20px 0; font-size: 24px; text-align: center;">
                                {T("email_ready_title")}
                            </h2>
                            
                            <p style="color: #a0a0b0; font-size: 16px; line-height: 1.6; text-align: center; margin: 0 0 20px 0;">
                                {T("email_ready_body")}
                            </p>
                            
                            {image_section}
                            
                            <div style="text-align: center; margin: 30px 0;">
                                <a href="{task_url}" style="display: inline-block; background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); color: #ffffff; text-decoration: none; padding: 16px 40px; border-radius: 8px; font-size: 16px; font-weight: 600; box-shadow: 0 4px 15px rgba(99, 102, 241, 0.4);">
                                    {T("email_ready_cta")}
                                </a>
                            </div>
                            
                            <p style="color: #d0d4e0; font-size: 14px; text-align: center; margin: 30px 0 0 0; line-height: 1.5;">
                                {T("email_ready_formats")}
                            </p>
                            {unsub_block}
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color: #12121a; padding: 20px 30px; text-align: center;">
                            <p style="color: #c8ccd8; font-size: 12px; margin: 0; line-height: 1.5;">
                                © 2026 AutoRig.online — {T("email_footer")}
                            </p>
                            <p style="color: #c8ccd8; font-size: 12px; margin: 10px 0 0 0;">
                                <a href="{base}" style="color: #a5b4fc; text-decoration: none; font-weight: 600;">{T("email_visit")}</a>
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
"""


async def send_task_completed_email(
    to_email: str,
    task_id: str,
    guid: str,
    worker_base: str,
    lang: Optional[str] = None,
    campaign: str = "task_ready",
) -> bool:
    """
    Send task completion email with preview image.
    
    Args:
        to_email: Recipient email address
        task_id: Task ID for the link
        guid: Task GUID for image URL
        worker_base: Worker base URL (e.g., http://5.129.157.224:5267)
    
    Returns:
        True if email was sent successfully, False otherwise
    """
    if not RESEND_API_KEY:
        print("[Email] RESEND_API_KEY not configured, skipping email")
        return False
    
    if not to_email:
        print("[Email] No recipient email, skipping")
        return False

    if not await _reserve_task_completion_email(task_id, to_email):
        print(f"[Email] Completion email already reserved or sent for task {task_id}, skipping")
        return False
    
    try:
        base = APP_URL.rstrip("/")
        thumb_url = f"{base}/api/thumb/{task_id}"
        print(f"[Email] Trying video poster (thumb): {thumb_url}")
        image_data = await download_image(thumb_url)
        if image_data:
            print(f"[Email] Using video poster, size: {len(image_data)} bytes")
        elif not guid or not worker_base:                    # a V3 task: no converter preview to fall back to
            print("[Email] No preview image available")
        else:
            # Fallback: worker render preview (e.g. thumb not ready yet when mail is sent)
            image_url = (
                f"{worker_base}/converter/glb/{guid}/{guid}_100k/{guid}_VRayCam001_view.jpg"
            )
            print(f"[Email] Thumb unavailable, trying worker preview: {image_url}")
            image_data = await download_image(image_url)
            if image_data:
                print(f"[Email] Using worker preview, size: {len(image_data)} bytes")
            else:
                print("[Email] No preview image available")

        has_image = image_data is not None

        tok = build_unsubscribe_token(to_email)
        unsubscribe_url = f"{base}/unsubscribe?token={quote(tok, safe='')}"
        dashboard_url = f"{base}/dashboard"
        html_content = get_email_html(
            task_id, has_image, unsubscribe_url, dashboard_url=dashboard_url, lang=lang,
            task_url=tracked_task_url(task_id, to_email, campaign),
        )
        
        # Prepare email params
        email_params: resend.Emails.SendParams = {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": email_text("email_ready_subject", lang),
            "html": html_content,
        }
        
        # Add attachment if image was downloaded
        if has_image:
            email_params["attachments"] = [
                {
                    "filename": "preview.jpg",
                    "content": base64.b64encode(image_data).decode("utf-8"),
                    "content_id": "preview_image",
                }
            ]
        
        # Send email
        response = resend.Emails.send(
            email_params,
            {"idempotency_key": f"task-completed/{task_id}"},
        )
        provider_message_id = None
        if isinstance(response, dict):
            provider_message_id = response.get("id")
        else:
            provider_message_id = getattr(response, "id", None)
        await _finish_task_completion_email(
            task_id,
            sent=True,
            provider_message_id=str(provider_message_id or "") or None,
        )
        print(f"[Email] Sent successfully to {to_email}, response: {response}")
        return True

    except Exception as e:
        await _finish_task_completion_email(task_id, sent=False, error=str(e))
        print(f"[Email] Failed to send email to {to_email}: {e}")
        return False


def tracked_task_url(task_id: str, to_email: str, campaign: str) -> str:
    """The task link through our click redirect (/email/click records the click in email_campaign_clicks), which
    lands on /task?id=… with utm_source=email&utm_medium=task_ready|backfill&utm_campaign=<campaign>."""
    try:
        return _marketing_click_url(campaign, to_email, f"task:{task_id}")
    except Exception:                                           # noqa: BLE001 - a plain link is still a link
        return f"{APP_URL.rstrip('/')}/task?id={task_id}&utm_source=email&utm_medium=task_ready&utm_campaign={campaign}"


async def send_task_digest_email(to_email: str, tasks: list, lang: Optional[str] = None,
                                 campaign: str = "backfill_20261012", poster_dir: str = "") -> dict:
    """One email listing several finished tasks of one owner (the backfill of the V3 switch). Each task is reserved
    in task_completion_emails first, so a task never gets this and a normal completion email both.
    tasks: [(task_id, title or None)]. -> {"sent": bool, "tasks": [ids included], "error": str|None}"""
    import os as _os

    if not RESEND_API_KEY or not to_email:
        return {"sent": False, "tasks": [], "error": "not configured"}
    included = []
    for task_id, title in tasks:
        if await _reserve_task_completion_email(task_id, to_email):
            included.append((task_id, title))
    if not included:
        return {"sent": False, "tasks": [], "error": "already sent"}
    T = lambda key: escape(email_text(key, lang))  # noqa: E731
    base = APP_URL.rstrip("/")
    attachments, rows = [], []
    for i, (task_id, title) in enumerate(included):
        url = tracked_task_url(task_id, to_email, campaign)
        img = ""
        path = _os.path.join(poster_dir, f"{task_id}.jpg") if poster_dir else ""
        if path and i < 8 and _os.path.isfile(path) and _os.path.getsize(path) > 8000:
            cid = f"poster{i}"
            with open(path, "rb") as fh:
                attachments.append({"filename": f"{task_id[:8]}.jpg", "content": base64.b64encode(fh.read()).decode(),
                                    "content_id": cid})
            img = (f'<a href="{url}"><img src="cid:{cid}" alt="" width="260" style="display:block;width:260px;'
                   f'max-width:100%;border-radius:10px;margin:0 auto 8px"></a>')
        name = escape((title or "").strip()[:80] or f"AutoRig · {task_id[:8]}")
        rows.append(f'<td style="padding:10px;text-align:center;vertical-align:top;width:50%">{img}'
                    f'<a href="{url}" style="color:#a5b4fc;font-size:14px;text-decoration:none">{name}</a></td>')
    grid = "".join("<tr>" + "".join(rows[i:i + 2]) + "</tr>" for i in range(0, len(rows), 2))
    tok = build_unsubscribe_token(to_email)
    unsubscribe_url = f"{base}/unsubscribe?token={quote(tok, safe='')}"
    direction = "rtl" if lang == "fa" else "ltr"
    html_content = f"""<!DOCTYPE html><html lang="{escape(lang or 'en')}" dir="{direction}"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0a0a0f;font-family:'Segoe UI',Tahoma,Verdana,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0f;padding:32px 16px"><tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#1a1a24;border-radius:16px;overflow:hidden">
<tr><td style="background:linear-gradient(135deg,#6366f1 0%,#8b5cf6 100%);padding:26px;text-align:center">
<h1 style="color:#fff;margin:0;font-size:26px">AutoRig.online</h1></td></tr>
<tr><td style="padding:30px 24px">
<h2 style="color:#f0f0f5;margin:0 0 14px;font-size:22px;text-align:center">{T("email_digest_title")}</h2>
<p style="color:#a0a0b0;font-size:15px;line-height:1.6;text-align:center;margin:0 0 18px">{T("email_digest_body")}</p>
<table width="100%" cellpadding="0" cellspacing="0">{grid}</table>
<p style="color:#d0d4e0;font-size:13px;text-align:center;margin:22px 0 0">{T("email_ready_formats")}</p>
<p style="color:#d0d4e0;font-size:12px;text-align:center;margin:18px 0 0">
<a href="{base}/dashboard" style="color:#a5b4fc">{T("email_dashboard")}</a> — {T("email_settings")} ·
<a href="{unsubscribe_url}" style="color:#a5b4fc">{T("email_unsubscribe")}</a></p>
</td></tr></table></td></tr></table></body></html>"""
    params = {"from": f"AutoRig.online <{EMAIL_FROM}>", "to": [to_email],
              "subject": email_text("email_digest_subject", lang), "html": html_content}
    if attachments:
        params["attachments"] = attachments
    try:
        response = resend.Emails.send(params, {"idempotency_key": f"{campaign}/{_completion_email_recipient_hash(to_email)}"})
        message_id = response.get("id") if isinstance(response, dict) else getattr(response, "id", None)
        for task_id, _ in included:
            await _finish_task_completion_email(task_id, sent=True, provider_message_id=str(message_id or "") or None)
        print(f"[Email] Digest sent ({len(included)} tasks), id={message_id}")
        return {"sent": True, "tasks": [t for t, _ in included], "error": None, "id": message_id}
    except Exception as e:
        for task_id, _ in included:
            await _finish_task_completion_email(task_id, sent=False, error=str(e))
        print(f"[Email] Digest failed: {e}")
        return {"sent": False, "tasks": [t for t, _ in included], "error": str(e)[:200]}


async def send_test_email(to_email: str) -> bool:
    """Send a test email to verify configuration"""
    if not RESEND_API_KEY:
        print("[Email] RESEND_API_KEY not configured")
        return False
    
    try:
        base = APP_URL.rstrip("/")
        tt = build_unsubscribe_token(to_email)
        test_unsub = f"{base}/unsubscribe?token={quote(tt, safe='')}"
        test_dash = f"{base}/dashboard"
        email_params: resend.Emails.SendParams = {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": "✅ Test Email from AutoRig.online",
            "html": f"""
            <div style="font-family: Arial, sans-serif; padding: 20px; background: #1a1a24; color: #f0f0f5;">
                <h1 style="color: #6366f1;">AutoRig.online</h1>
                <p>This is a test email to verify that the email service is configured correctly.</p>
                <p>If you received this email, everything is working!</p>
                <p style="color: #c8ccd8; font-size: 12px; margin-top: 30px;">
                    Sent from <a href="{base}" style="color: #a5b4fc; font-weight: 600;">AutoRig.online</a>
                </p>
                <p style="color: #d0d4e0; font-size: 12px; margin-top: 16px;">
                    <a href="{test_dash}" style="color: #a5b4fc;">Dashboard</a>
                    <span style="color: #e8eaf0;"> — notification settings / отписка</span>
                </p>
                <p style="color: #d0d4e0; font-size: 12px; margin-top: 8px;">
                    <a href="{test_unsub}" style="color: #a5b4fc;">One-click unsubscribe</a>
                    <span style="color: #e8eaf0;"> · Мгновенная отписка</span>
                </p>
            </div>
            """,
        }
        
        response = resend.Emails.send(email_params)
        print(f"[Email] Test email sent to {to_email}, response: {response}")
        return True
        
    except Exception as e:
        print(f"[Email] Failed to send test email: {e}")
        return False


def _response_message_id(response) -> Optional[str]:
    if isinstance(response, dict):
        raw = response.get("id")
    else:
        raw = getattr(response, "id", None)
    return str(raw) if raw else None


def _marketing_sender_footer() -> str:
    if MARKETING_POSTAL_ADDRESS:
        return escape(MARKETING_POSTAL_ADDRESS)
    return f"No postal address provided. Contact: {escape(EMAIL_FROM)}"


def _marketing_click_url(campaign_key: str, email: str, link_key: str) -> str:
    token = build_campaign_click_token(campaign_key, email, link_key)
    return f"{APP_URL.rstrip('/')}/email/click?token={quote(token, safe='')}"


def _marketing_email_html(visible_unsubscribe_url: str, click_urls: dict) -> str:
    base = APP_URL.rstrip("/")
    animal_url = click_urls.get("animal_rig") or f"{base}/animal-rig"
    youtube_url = click_urls.get("youtube_short") or "https://www.youtube.com/shorts/vEn7laZijOI"
    home_url = click_urls.get("home") or f"{base}/"
    poster_url = f"{base}/static/images/email/autorig-v2-animal-rig-poster.jpg?v=20260516"
    sender_footer = _marketing_sender_footer()
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>AutoRig V2: animal rigging is live</title>
</head>
<body style="margin:0;padding:0;background:#0d0d1a;font-family:Arial,Helvetica,sans-serif;color:#f5f7ff;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#0d0d1a;padding:32px 16px;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="width:100%;max-width:600px;background:#171728;border:1px solid #2b2b44;border-radius:16px;overflow:hidden;">
          <tr>
            <td style="padding:32px 28px;background:linear-gradient(135deg,#191934 0%,#10101d 70%);">
              <p style="margin:0 0 10px;color:#facc15;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;">AutoRig V2</p>
              <h1 style="margin:0;color:#ffffff;font-size:30px;line-height:1.15;">Animal rigging is live</h1>
              <p style="margin:18px 0 0;color:#c9cde0;font-size:16px;line-height:1.6;">
                AutoRig.online now rigs animals and other non-humanoid 3D models. We also made the humanoid character rig stronger, cleaner, and more predictable.
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:0;background:#10101d;">
              <a href="{animal_url}" style="display:block;text-decoration:none;">
                <img src="{poster_url}" width="600" alt="AutoRig V2 animal rigging preview" style="display:block;width:100%;max-width:600px;height:auto;border:0;">
              </a>
            </td>
          </tr>
          <tr>
            <td style="padding:28px;">
              <p style="margin:0 0 18px;color:#d9dcec;font-size:15px;line-height:1.65;">
                The new V2 pipeline can handle creatures, quadrupeds, stylized animals, and hard-surface non-humanoid models. The classic humanoid workflow was improved too, so character rigs should be more useful for animation and game workflows.
              </p>
              <table cellpadding="0" cellspacing="0" style="margin:24px 0;">
                <tr>
                  <td style="background:#8b5cf6;border-radius:10px;">
                    <a href="{animal_url}" style="display:inline-block;padding:14px 20px;color:#ffffff;text-decoration:none;font-size:15px;font-weight:700;">See V2 animal rigging</a>
                  </td>
                  <td width="12"></td>
                  <td style="background:#25253a;border:1px solid #383857;border-radius:10px;">
                    <a href="{youtube_url}" style="display:inline-block;padding:13px 18px;color:#ffffff;text-decoration:none;font-size:15px;font-weight:700;">Watch the short</a>
                  </td>
                </tr>
              </table>
              <p style="margin:18px 0 0;color:#c9cde0;font-size:14px;line-height:1.6;">
                You can also upload a model and try the updated rigging flow directly on AutoRig.online.
              </p>
              <p style="margin:8px 0 0;">
                <a href="{home_url}" style="color:#a5b4fc;text-decoration:underline;font-size:14px;">Try AutoRig.online</a>
              </p>
            </td>
          </tr>
          <tr>
            <td style="padding:22px 28px;background:#11111f;border-top:1px solid #2b2b44;">
              <p style="margin:0;color:#aeb4c8;font-size:12px;line-height:1.6;">
                You are receiving this because you signed in to AutoRig.online and have not unsubscribed from email notifications.
              </p>
              <p style="margin:10px 0 0;color:#aeb4c8;font-size:12px;line-height:1.6;">
                <a href="{visible_unsubscribe_url}" style="color:#a5b4fc;text-decoration:underline;">Unsubscribe from marketing emails</a>
              </p>
              <p style="margin:10px 0 0;color:#8c93aa;font-size:11px;line-height:1.5;">
                AutoRig.online<br>{sender_footer}
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _marketing_email_text(visible_unsubscribe_url: str, click_urls: dict) -> str:
    base = APP_URL.rstrip("/")
    animal_url = click_urls.get("animal_rig") or f"{base}/animal-rig"
    youtube_url = click_urls.get("youtube_short") or "https://www.youtube.com/shorts/vEn7laZijOI"
    home_url = click_urls.get("home") or f"{base}/"
    return "\n".join(
        [
            "AutoRig V2: animal rigging is live",
            "",
            "AutoRig.online now rigs animals and other non-humanoid 3D models.",
            "We also made the humanoid character rig stronger, cleaner, and more predictable.",
            "",
            f"See V2 animal rigging: {animal_url}",
            f"Watch the short: {youtube_url}",
            f"Try AutoRig.online: {home_url}",
            "",
            "You are receiving this because you signed in to AutoRig.online and have not unsubscribed from email notifications.",
            f"Unsubscribe from marketing emails: {visible_unsubscribe_url}",
            "",
            "AutoRig.online",
            MARKETING_POSTAL_ADDRESS or f"No postal address provided. Contact: {EMAIL_FROM}",
        ]
    )


async def send_marketing_campaign_email(
    to_email: str,
    campaign_key: str,
    allow_missing_postal_address: bool = False,
) -> dict:
    """Send one AutoRig marketing campaign email with one-click unsubscribe headers."""
    if not RESEND_API_KEY:
        return {"ok": False, "provider_message_id": None, "error": "RESEND_API_KEY is not configured"}
    if not MARKETING_POSTAL_ADDRESS and not allow_missing_postal_address:
        return {"ok": False, "provider_message_id": None, "error": "MARKETING_POSTAL_ADDRESS is not configured"}
    if not to_email:
        return {"ok": False, "provider_message_id": None, "error": "recipient email is empty"}

    try:
        base = APP_URL.rstrip("/")
        token = build_marketing_unsubscribe_token(to_email)
        encoded_token = quote(token, safe="")
        one_click_unsubscribe_url = f"{base}/api/email/marketing-unsubscribe?token={encoded_token}"
        visible_unsubscribe_url = f"{base}/unsubscribe/marketing?token={encoded_token}"
        click_urls = {
            "animal_rig": _marketing_click_url(campaign_key, to_email, "animal_rig"),
            "youtube_short": _marketing_click_url(campaign_key, to_email, "youtube_short"),
            "home": _marketing_click_url(campaign_key, to_email, "home"),
        }
        email_hash = hashlib.sha256(to_email.strip().lower().encode("utf-8")).hexdigest()
        email_params: resend.Emails.SendParams = {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": "AutoRig V2: animal rigging is live",
            "html": _marketing_email_html(visible_unsubscribe_url, click_urls),
            "text": _marketing_email_text(visible_unsubscribe_url, click_urls),
            "headers": {
                "List-Unsubscribe": f"<{one_click_unsubscribe_url}>",
                "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
                "X-AutoRig-Campaign": campaign_key,
                "X-AutoRig-Recipient-Hash": email_hash,
            },
            "tags": [
                {"name": "kind", "value": "marketing"},
                {"name": "campaign", "value": campaign_key},
            ],
        }
        response = resend.Emails.send(email_params)
        return {
            "ok": True,
            "provider_message_id": _response_message_id(response),
            "error": None,
        }
    except Exception as e:
        return {"ok": False, "provider_message_id": None, "error": str(e)}


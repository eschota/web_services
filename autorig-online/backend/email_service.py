"""
Email Service for AutoRig Online
Uses Resend API for sending emails
"""
import base64
import hashlib
import httpx
import resend
from html import escape
from typing import Optional
from urllib.parse import quote

from config import RESEND_API_KEY, EMAIL_FROM, APP_URL, MARKETING_POSTAL_ADDRESS
from unsubscribe_tokens import build_unsubscribe_token, build_marketing_unsubscribe_token


# Initialize Resend
resend.api_key = RESEND_API_KEY


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


def get_email_html(
    task_id: str,
    has_image: bool = True,
    unsubscribe_url: Optional[str] = None,
    dashboard_url: Optional[str] = None,
) -> str:
    """Generate HTML email template"""
    base = APP_URL.rstrip("/")
    task_url = f"{base}/task?id={task_id}"
    dash = dashboard_url or f"{base}/dashboard"
    unsub_block = ""
    if unsubscribe_url:
        unsub_block = f"""
                            <p style="color: #d0d4e0; font-size: 12px; text-align: center; margin: 20px 0 0 0; line-height: 1.5;">
                                <a href="{dash}" style="color: #a5b4fc; text-decoration: underline;">Dashboard</a>
                                <span style="color: #e8eaf0;"> — notification settings / отписка от уведомлений</span>
                            </p>
                            <p style="color: #d0d4e0; font-size: 12px; text-align: center; margin: 10px 0 0 0;">
                                <a href="{unsubscribe_url}" style="color: #a5b4fc; text-decoration: underline;">One-click unsubscribe</a>
                                <span style="color: #e8eaf0;"> · Мгновенная отписка по ссылке</span>
                            </p>
        """
    
    image_section = ""
    if has_image:
        image_section = f"""
        <div style="text-align: center; margin: 30px 0;">
            <a href="{task_url}" style="display: inline-block; text-decoration: none; border-radius: 12px;">
                <img src="cid:preview_image" alt="3D Model Preview" border="0" style="display: block; max-width: 100%; height: auto; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.3);">
            </a>
        </div>
        """
    
    return f"""
<!DOCTYPE html>
<html>
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
                                🎉 Your 3D Model is Ready!
                            </h2>
                            
                            <p style="color: #a0a0b0; font-size: 16px; line-height: 1.6; text-align: center; margin: 0 0 20px 0;">
                                Great news! Your auto-rigging task has been completed successfully. 
                                Your character is now ready with a full skeleton and animations.
                            </p>
                            
                            {image_section}
                            
                            <div style="text-align: center; margin: 30px 0;">
                                <a href="{task_url}" style="display: inline-block; background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); color: #ffffff; text-decoration: none; padding: 16px 40px; border-radius: 8px; font-size: 16px; font-weight: 600; box-shadow: 0 4px 15px rgba(99, 102, 241, 0.4);">
                                    View &amp; Download Results
                                </a>
                            </div>
                            
                            <p style="color: #d0d4e0; font-size: 14px; text-align: center; margin: 30px 0 0 0; line-height: 1.5;">
                                Your files are available in multiple formats: 3ds Max, Maya, Cinema 4D, Unity, Unreal Engine, and more.
                            </p>
                            {unsub_block}
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color: #12121a; padding: 20px 30px; text-align: center;">
                            <p style="color: #c8ccd8; font-size: 12px; margin: 0; line-height: 1.5;">
                                © 2026 AutoRig.online — Automatic 3D Character Rigging
                            </p>
                            <p style="color: #c8ccd8; font-size: 12px; margin: 10px 0 0 0;">
                                <a href="{base}" style="color: #a5b4fc; text-decoration: none; font-weight: 600;">Visit Website</a>
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
    worker_base: str
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
    
    try:
        base = APP_URL.rstrip("/")
        thumb_url = f"{base}/api/thumb/{task_id}"
        print(f"[Email] Trying video poster (thumb): {thumb_url}")
        image_data = await download_image(thumb_url)
        if image_data:
            print(f"[Email] Using video poster, size: {len(image_data)} bytes")
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
            task_id, has_image, unsubscribe_url, dashboard_url=dashboard_url
        )
        
        # Prepare email params
        email_params: resend.Emails.SendParams = {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": "🎉 Your 3D Model is Ready! - AutoRig.online",
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
        response = resend.Emails.send(email_params)
        print(f"[Email] Sent successfully to {to_email}, response: {response}")
        return True
        
    except Exception as e:
        print(f"[Email] Failed to send email to {to_email}: {e}")
        return False


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


def _marketing_email_html(visible_unsubscribe_url: str) -> str:
    base = APP_URL.rstrip("/")
    animal_url = f"{base}/animal-rig"
    home_url = f"{base}/"
    poster_url = f"{base}/static/images/email/autorig-v2-animal-rig-poster.jpg?v=20260516"
    youtube_url = "https://www.youtube.com/shorts/vEn7laZijOI"
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


def _marketing_email_text(visible_unsubscribe_url: str) -> str:
    base = APP_URL.rstrip("/")
    return "\n".join(
        [
            "AutoRig V2: animal rigging is live",
            "",
            "AutoRig.online now rigs animals and other non-humanoid 3D models.",
            "We also made the humanoid character rig stronger, cleaner, and more predictable.",
            "",
            f"See V2 animal rigging: {base}/animal-rig",
            "Watch the short: https://www.youtube.com/shorts/vEn7laZijOI",
            f"Try AutoRig.online: {base}/",
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
        email_hash = hashlib.sha256(to_email.strip().lower().encode("utf-8")).hexdigest()
        email_params: resend.Emails.SendParams = {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": "AutoRig V2: animal rigging is live",
            "html": _marketing_email_html(visible_unsubscribe_url),
            "text": _marketing_email_text(visible_unsubscribe_url),
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


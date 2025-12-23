"""
Email Service for AutoRig Online
Uses Resend API for sending emails
"""
import base64
import httpx
import resend
from typing import Optional

from config import RESEND_API_KEY, EMAIL_FROM, APP_URL


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


def get_email_html(task_id: str, has_image: bool = True) -> str:
    """Generate HTML email template"""
    task_url = f"{APP_URL}/task?id={task_id}"
    
    image_section = ""
    if has_image:
        image_section = """
        <div style="text-align: center; margin: 30px 0;">
            <img src="cid:preview_image" alt="3D Model Preview" style="max-width: 100%; height: auto; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.3);">
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
                            
                            <p style="color: #606070; font-size: 14px; text-align: center; margin: 30px 0 0 0;">
                                Your files are available in multiple formats: 3ds Max, Maya, Cinema 4D, Unity, Unreal Engine, and more.
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background-color: #12121a; padding: 20px 30px; text-align: center;">
                            <p style="color: #606070; font-size: 12px; margin: 0;">
                                © 2025 AutoRig.online — Automatic 3D Character Rigging
                            </p>
                            <p style="color: #606070; font-size: 12px; margin: 10px 0 0 0;">
                                <a href="{APP_URL}" style="color: #6366f1; text-decoration: none;">Visit Website</a>
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
        # Build image URL: {worker_base}/converter/glb/{guid}/{guid}_100k/{guid}_VRayCam001_view.jpg
        image_url = f"{worker_base}/converter/glb/{guid}/{guid}_100k/{guid}_VRayCam001_view.jpg"
        print(f"[Email] Downloading preview image from: {image_url}")
        
        # Download image
        image_data = await download_image(image_url)
        has_image = image_data is not None
        
        if has_image:
            print(f"[Email] Image downloaded successfully, size: {len(image_data)} bytes")
        else:
            print("[Email] Failed to download image, sending email without preview")
        
        # Generate HTML
        html_content = get_email_html(task_id, has_image)
        
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
        email_params: resend.Emails.SendParams = {
            "from": f"AutoRig.online <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": "✅ Test Email from AutoRig.online",
            "html": f"""
            <div style="font-family: Arial, sans-serif; padding: 20px; background: #1a1a24; color: #f0f0f5;">
                <h1 style="color: #6366f1;">AutoRig.online</h1>
                <p>This is a test email to verify that the email service is configured correctly.</p>
                <p>If you received this email, everything is working!</p>
                <p style="color: #606070; font-size: 12px; margin-top: 30px;">
                    Sent from <a href="{APP_URL}" style="color: #6366f1;">AutoRig.online</a>
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


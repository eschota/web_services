"""
Configuration for AutoRig Online
"""
import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Application Settings
# =============================================================================
APP_NAME = "AutoRig Online"
APP_URL = os.getenv("APP_URL", "https://autorig.online")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production-very-secret-key-123")

# =============================================================================
# Database
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./db/autorig.db")

# =============================================================================
# Google OAuth2
# =============================================================================
GOOGLE_CLIENT_ID = os.getenv(
    "GOOGLE_CLIENT_ID",
    "your-google-client-id-here"
)
GOOGLE_CLIENT_SECRET = os.getenv(
    "GOOGLE_CLIENT_SECRET",
    "your-google-client-secret-here"
)
GOOGLE_REDIRECT_URI = os.getenv(
    "GOOGLE_REDIRECT_URI",
    f"{APP_URL}/auth/callback"
)

# =============================================================================
# Admin
# =============================================================================
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "eschota@gmail.com")  # Legacy, kept for compatibility
ADMIN_EMAILS = [
    "eschota@gmail.com",
    "vladkcg@gmail.com",
]

# =============================================================================
# Workers (Converters)
# =============================================================================
WORKERS = [
    "http://5.129.157.224:5132/api-converter-glb",
    "http://5.129.157.224:5279/api-converter-glb",
    "http://5.129.157.224:5131/api-converter-glb",
    "http://5.129.157.224:5533/api-converter-glb",
    "http://5.129.157.224:5267/api-converter-glb",
]

# =============================================================================
# Limits
# =============================================================================
ANON_FREE_LIMIT = 3  # Free conversions for anonymous users
USER_FREE_LIMIT = 30  # Total free credits after login (30 credits for all registered users)
USER_BONUS_AFTER_LOGIN = 27  # Additional credits after login (30 - max anon used)

# =============================================================================
# Upload Settings
# =============================================================================
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/var/autorig/uploads")
UPLOAD_TTL_HOURS = 24
MAX_UPLOAD_SIZE_MB = 100

# =============================================================================
# Viewer Defaults (3D viewer settings)
# =============================================================================
# Global default viewer settings JSON file (admin can overwrite via API).
VIEWER_DEFAULT_SETTINGS_PATH = os.getenv(
    "VIEWER_DEFAULT_SETTINGS_PATH",
    "/var/autorig/viewer_default_settings.json"
)

# =============================================================================
# Progress Check Settings
# =============================================================================
PROGRESS_BATCH_SIZE = 15  # Number of URLs to check per batch
PROGRESS_CONCURRENCY = 10  # Max concurrent HEAD requests
PROGRESS_CHECK_TIMEOUT = 5  # Timeout for HEAD requests in seconds

# =============================================================================
# Stale Task Detection & Auto-Restart
# =============================================================================
STALE_TASK_TIMEOUT_MINUTES = 10  # Task is "stale" if no progress for this long
MAX_TASK_RESTARTS = 3  # Maximum number of auto-restarts before marking as error
STALE_CHECK_INTERVAL_CYCLES = 2  # Check for stale tasks every N background worker cycles

# =============================================================================
# Rate Limiting
# =============================================================================
RATE_LIMIT_TASKS_PER_MINUTE = 5

# =============================================================================
# Email (Resend)
# =============================================================================
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "noreply@autorig.online")

# =============================================================================
# Gumroad
# =============================================================================
# Gumroad product_permalink -> credits mapping
GUMROAD_PRODUCT_CREDITS = {
    "autorig-100": 100,
    "autorig-500": 500,
    "autorig-1000": 1000,
}

# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "AutoRigOnlineBot")


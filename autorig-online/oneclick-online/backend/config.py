"""
Configuration for OneClick Online
"""
import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Application Settings
# =============================================================================
APP_NAME = "OneClick Online"
APP_URL = os.getenv("APP_URL", "https://oneclick3d.xyz")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
SECRET_KEY = os.getenv("SECRET_KEY", "change-me-in-production-very-secret-key-123")

# =============================================================================
# Database
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:////srv/oneclick/data/db/oneclick.db")

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
WORKERS = [w.strip() for w in os.getenv("ONECLICK_WORKERS", "http://127.0.0.1:15279/api-converter-glb,http://127.0.0.1:15131/api-converter-glb,http://127.0.0.1:15533/api-converter-glb,http://127.0.0.1:15267/api-converter-glb").split(",") if w.strip()]

# =============================================================================
# Limits
# =============================================================================
ANON_FREE_LIMIT = 0  # Free conversions for anonymous users
USER_FREE_LIMIT = 3  # Start credits for each newly registered user
USER_BONUS_AFTER_LOGIN = 27  # Additional credits after login (30 - max anon used)

# =============================================================================
# Upload Settings
# =============================================================================
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/srv/oneclick/data/uploads")
UPLOAD_TTL_HOURS = 24
MAX_UPLOAD_SIZE_MB = max(1, int(os.getenv("MAX_UPLOAD_SIZE_MB", "10240")))

# =============================================================================
# Viewer Defaults (3D viewer settings)
# =============================================================================
# Global default viewer settings JSON file (admin can overwrite via API).
VIEWER_DEFAULT_SETTINGS_PATH = os.getenv(
    "VIEWER_DEFAULT_SETTINGS_PATH",
    "/srv/oneclick/data/viewer_default_settings.json"
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
EMAIL_FROM = os.getenv("EMAIL_FROM", "noreply@oneclick3d.xyz")

# =============================================================================
# Gumroad
# =============================================================================
# Gumroad product_permalink -> credits mapping
GUMROAD_PRODUCT_CREDITS = {
    "oneclick-10-credits": 10,
    "oneclick-50-credits": 50,
    "oneclick-500-credits": 500,
}

# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "OneClickOnlineBot")

# =============================================================================
# Disk Cleanup Settings
# =============================================================================
MIN_FREE_SPACE_GB = int(os.getenv("MIN_FREE_SPACE_GB", "10"))  # Minimum free space to maintain
CLEANUP_CHECK_INTERVAL_CYCLES = 10  # Check disk space every N background worker cycles (~5 min)
CLEANUP_MIN_AGE_HOURS = 1  # Never delete files younger than this (safety for processing tasks)

# =============================================================================
# Search Webmaster Verification
# =============================================================================
# Prefer DNS verification in webmaster dashboards; file/meta methods are kept as fallback.
BING_MSVALIDATE_CODE = os.getenv("BING_MSVALIDATE_CODE", "")
BING_SITE_AUTH_XML = os.getenv("BING_SITE_AUTH_XML", "")

YANDEX_VERIFICATION_CODE = os.getenv("YANDEX_VERIFICATION_CODE", "")
YANDEX_VERIFICATION_FILENAME = os.getenv("YANDEX_VERIFICATION_FILENAME", "yandex_7bb48a0ce446816a.html")

BAIDU_VERIFICATION_CODE = os.getenv("BAIDU_VERIFICATION_CODE", "")
BAIDU_VERIFICATION_FILENAME = os.getenv("BAIDU_VERIFICATION_FILENAME", "")

# =============================================================================
# Git Webhook Auto-Deploy
# =============================================================================
GIT_WEBHOOK_SECRET = os.getenv("GIT_WEBHOOK_SECRET", "")
GIT_DEPLOY_REPO_PATH = os.getenv("GIT_DEPLOY_REPO_PATH", "/root")
GIT_DEPLOY_REMOTE = os.getenv("GIT_DEPLOY_REMOTE", "origin")
GIT_DEPLOY_BRANCH = os.getenv("GIT_DEPLOY_BRANCH", "main")
GIT_DEPLOY_SERVICE = os.getenv("GIT_DEPLOY_SERVICE", "oneclick.service")

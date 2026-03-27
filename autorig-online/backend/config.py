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

# YouTube Data API (same Google OAuth client; separate redirect URI + scope youtube.upload)
YOUTUBE_OAUTH_REDIRECT_URI = os.getenv(
    "YOUTUBE_OAUTH_REDIRECT_URI",
    f"{APP_URL.rstrip('/')}/api/oauth/youtube/callback",
)
# Auto-uploads are always public (not unlisted / not link-only). Not overridable via env.
YOUTUBE_UPLOAD_PRIVACY = "public"

# Optional: paste refresh token from OAuth (or use /api/admin/youtube/oauth/start + DB row)
YOUTUBE_REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN", "").strip()

# OpenAI (poster vision metadata for YouTube / task UI; set in production env only)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

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
ANON_FREE_LIMIT = 0  # Free conversions for anonymous users
USER_FREE_LIMIT = 0  # Total free credits after login (0 credits for all registered users)
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
GLOBAL_TASK_TIMEOUT_MINUTES = 180  # Hard timeout for any non-terminal task (3 hours)
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
    # Free3D products
    "free3d-10credits": 1000,
    "free3d-unlimitedsubscription": 999999,
    "free3d-unlimited": 999999,
    # Legacy AutoRig products (kept for backward compatibility)
    "autorig-100": 100,
    "autorig-500": 500,
    "autorig-1000": 1000,
}

# Gumroad product_permalinks (lowercase) that count toward /buy-credits donation progress
AUTORIG_DONATION_PRODUCT_KEYS = frozenset(
    k.strip().lower() for k in GUMROAD_PRODUCT_CREDITS if str(k).strip().lower().startswith("autorig-")
)

# Public donation thermometer on buy-credits (USD)
DONATION_GOAL_USD = int(os.getenv("DONATION_GOAL_USD", "1000"))
DONATION_BASELINE_USD = float(os.getenv("DONATION_BASELINE_USD", "19"))

# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "AutoRigOnlineBot")

# =============================================================================
# Disk Cleanup Settings
# =============================================================================
# When false (default): background worker does NOT purge gallery/no-asset rows; disk-pressure cleanup
# does NOT delete Task rows (only orphan cache/upload/video files). Systemd run_task_cleanup.py exits immediately.
# Set AUTOMATIC_TASK_DB_DELETION=1 to restore legacy automatic DB row deletion.
AUTOMATIC_TASK_DB_DELETION = os.getenv("AUTOMATIC_TASK_DB_DELETION", "0") == "1"

MIN_FREE_SPACE_GB = int(os.getenv("MIN_FREE_SPACE_GB", "10"))  # Minimum free space to maintain
CLEANUP_CHECK_INTERVAL_CYCLES = 10  # Check disk space every N background worker cycles (~5 min)
CLEANUP_MIN_AGE_HOURS = 1  # Never delete files younger than this (safety for processing tasks)

# Before each new task: try to reach at least this much free space on /
NEW_TASK_MIN_FREE_GB = int(os.getenv("NEW_TASK_MIN_FREE_GB", "2"))
# If ZIP purge is not enough: delete oldest done/error tasks, but free at most this many bytes from disk in that phase
NEW_TASK_PURGE_TASKS_MAX_FREED_GB = int(os.getenv("NEW_TASK_PURGE_TASKS_MAX_FREED_GB", "1"))

# Purge DB rows for terminal tasks that have neither video nor any thumbnail URL in ready/output lists
# (Used only as legacy env name; gallery purges are gated by GALLERY_DB_PURGE_INTERVAL_CYCLES below.)
NO_ASSETS_TASK_PURGE_INTERVAL_CYCLES = int(
    os.getenv("NO_ASSETS_TASK_PURGE_INTERVAL_CYCLES", "10")
)

# Gallery / task DB purge (upstream HTTP probe + no-poster purge): default once per week.
# Background worker sleeps BACKGROUND_WORKER_SLEEP_SEC between cycles (see main.py).
_BACKGROUND_WORKER_SLEEP_SEC = 30
_GALLERY_PURGE_DEFAULT_WEEK_SEC = 7 * 24 * 3600
GALLERY_DB_PURGE_INTERVAL_CYCLES = max(
    1,
    int(
        os.getenv(
            "GALLERY_DB_PURGE_INTERVAL_CYCLES",
            str(_GALLERY_PURGE_DEFAULT_WEEK_SEC // _BACKGROUND_WORKER_SLEEP_SEC),
        )
    ),
)

# HEAD/GET probe of worker URLs: gallery rows with deleted worker files still have JSON paths — purge in batches
GALLERY_UPSTREAM_PURGE_BATCH = int(os.getenv("GALLERY_UPSTREAM_PURGE_BATCH", "80"))
# How many upstream purge rounds per background cycle (each round processes up to BATCH rows)
GALLERY_UPSTREAM_PURGE_ROUNDS = int(os.getenv("GALLERY_UPSTREAM_PURGE_ROUNDS", "25"))

# =============================================================================
# Google Analytics 4 (Measurement Protocol)
# =============================================================================
GA_MEASUREMENT_ID = os.getenv("GA_MEASUREMENT_ID", "G-T4E781EHE4")
GA_API_SECRET = os.getenv("GA_API_SECRET", "")

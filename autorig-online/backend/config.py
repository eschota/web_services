"""
Configuration for AutoRig Online
"""
import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()
# Split production env files: systemd may list only one EnvironmentFile; merge optional fragments
# without overriding vars already set (e.g. by systemd).
_AUTORIG_EXTRA_DOTENV_FILES = (
    "/etc/autorig-online/environment",
    "/etc/autorig-backend.env",
    "/etc/autorig-telegram.env",
)
for _env_path in _AUTORIG_EXTRA_DOTENV_FILES:
    if os.path.isfile(_env_path):
        try:
            load_dotenv(_env_path, override=False)
        except OSError:
            # Service user (e.g. www-data) may lack read-bit on optional split env files.
            pass

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

_ADMIN_EMAILS_LOWER = frozenset(e.strip().lower() for e in ADMIN_EMAILS)


def is_admin_email(email: Optional[str]) -> bool:
    """True if email is in ADMIN_EMAILS (case-insensitive)."""
    if not email or not isinstance(email, str):
        return False
    return email.strip().lower() in _ADMIN_EMAILS_LOWER


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
# No progress this long → stalled-worker Telegram alert + included in stale-task heuristics (see tasks.py)
STALE_TASK_TIMEOUT_MINUTES = int(os.getenv("STALE_TASK_TIMEOUT_MINUTES", "30"))
# GET says total_active=0 and queue_size=0 but DB still has processing+0 ready — requeue after this (faster than JSON-only lost)
WORKER_IDLE_STALE_MINUTES = int(os.getenv("WORKER_IDLE_STALE_MINUTES", "2"))
# Align with typical worker single-job timeout (~2h); non-terminal tasks older than this -> error
GLOBAL_TASK_TIMEOUT_MINUTES = int(os.getenv("GLOBAL_TASK_TIMEOUT_MINUTES", "120"))
# processing with ready_count < total_count and no new file for this long -> requeue (worker may be stuck mid-pipeline)
PARTIAL_PROGRESS_STALE_MINUTES = int(os.getenv("PARTIAL_PROGRESS_STALE_MINUTES", "120"))
MAX_TASK_RESTARTS = 3  # Maximum number of auto-restarts before marking as error
STALE_CHECK_INTERVAL_CYCLES = int(os.getenv("STALE_CHECK_INTERVAL_CYCLES", "1"))
# Stuck processing (0% progress) longer than this -> auto requeue up to STUCK_HOUR_MAX_REQUEUES, then delete
STUCK_HOUR_MINUTES = int(os.getenv("STUCK_HOUR_MINUTES", "60"))
STUCK_HOUR_MAX_REQUEUES = int(os.getenv("STUCK_HOUR_MAX_REQUEUES", "3"))

# =============================================================================
# Rate Limiting
# =============================================================================
RATE_LIMIT_TASKS_PER_MINUTE = 5
# AI agent self-registration (POST /api/agents/register), per client IP
RATE_LIMIT_AGENT_REGISTER = os.getenv("RATE_LIMIT_AGENT_REGISTER", "15/hour")

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
# Direct crypto (buy-credits / agents); credits added manually after tx review
# =============================================================================
CRYPTO_DISCOUNT_FRACTION = float(os.getenv("CRYPTO_DISCOUNT_FRACTION", "0.2"))
CRYPTO_BTC_USD_RATE = float(os.getenv("CRYPTO_BTC_USD_RATE", "95000"))
# (gumroad_permalink_key, credits, list_price_usd)
AUTORIG_CRYPTO_TIERS: list[tuple[str, int, float]] = [
    ("autorig-100", 100, 10.0),
    ("autorig-500", 500, 30.0),
    ("autorig-1000", 1000, 100.0),
]
CRYPTO_ALLOWED_TIER_KEYS = frozenset(t[0] for t in AUTORIG_CRYPTO_TIERS)

# id must match API + frontend; warning is default EN (UI may localize by id)
CRYPTO_RECEIVE_NETWORKS: list[dict[str, str]] = [
    {
        "id": "usdt_trc20",
        "label": "USDT (TRC20 / Tron)",
        "asset": "USDT",
        "address": "TJgQSMo6vxKh9jxoQjAVf12m5HTpyPhCze",
        "warning": "Send only USDT TRC20 to this address. Sending assets on other networks or NFTs will result in permanent loss.",
    },
    {
        "id": "usdt_ton",
        "label": "USDT (TON)",
        "asset": "USDT",
        "address": "UQCd5N49Xpzw6CfYg89AnJMDtbdULHD1KWcisDdc1R7kSd5D",
        "warning": "Send only USDT TON to this address. Sending other assets, jettons, or NFTs will result in permanent loss.",
    },
    {
        "id": "usdt_sol",
        "label": "USDT (Solana)",
        "asset": "USDT",
        "address": "DPFN9HjM6Q2mr5e8wLm7ccmemzRKpWKXXe4K9UZCeCwb",
        "warning": "Send only USDT on Solana to this address. Sending assets on other networks or NFTs will result in permanent loss.",
    },
    {
        "id": "usdt_erc20",
        "label": "USDT (ERC20 / Ethereum)",
        "asset": "USDT",
        "address": "0xCfbd896042041fa1117bF53A1e0a45B2Bd84B6Cb",
        "warning": "Send only USDT ERC20 to this address. Sending assets on other networks or NFTs will result in permanent loss.",
    },
    {
        "id": "btc",
        "label": "Bitcoin (BTC)",
        "asset": "BTC",
        "address": "bc1qhtawy98e22rur8qz9wp9u0y7g7ur4d7904g3ek",
        "warning": "Send only Bitcoin (BTC) to this address. Sending assets on other networks or NFTs will result in permanent loss.",
    },
]
CRYPTO_ALLOWED_NETWORK_IDS = frozenset(n["id"] for n in CRYPTO_RECEIVE_NETWORKS)
RATE_LIMIT_CRYPTO_SUBMIT = os.getenv("RATE_LIMIT_CRYPTO_SUBMIT", "20/hour")

# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "AutoRigOnlineBot")

# Telegram supergroup for task notifications and support forum topics (same chat; bot must be admin for topics).
# Resolved at runtime: TELEGRAM_NOTIFICATION_CHAT_ID if set, else earliest group/supergroup row in telegram_chats.
_try_notif_raw = os.getenv("TELEGRAM_NOTIFICATION_CHAT_ID", "").strip()
try:
    TELEGRAM_NOTIFICATION_CHAT_ID: int | None = int(_try_notif_raw) if _try_notif_raw else None
except ValueError:
    TELEGRAM_NOTIFICATION_CHAT_ID = None

RATE_LIMIT_SUPPORT_CHAT_SESSION = os.getenv("RATE_LIMIT_SUPPORT_CHAT_SESSION", "60/minute")
RATE_LIMIT_SUPPORT_CHAT_MESSAGE = os.getenv("RATE_LIMIT_SUPPORT_CHAT_MESSAGE", "30/minute")
RATE_LIMIT_SUPPORT_CHAT_MESSAGES_POLL = os.getenv("RATE_LIMIT_SUPPORT_CHAT_MESSAGES_POLL", "120/minute")
SUPPORT_CHAT_MESSAGE_MAX_CHARS = int(os.getenv("SUPPORT_CHAT_MESSAGE_MAX_CHARS", "3800"))

# =============================================================================
# Disk Cleanup Settings
# =============================================================================
# When false (default): background worker does NOT purge gallery/no-asset rows; disk-pressure cleanup
# does NOT delete Task rows (only orphan cache/upload/video files). Systemd run_task_cleanup.py exits immediately.
# Set AUTOMATIC_TASK_DB_DELETION=1 to restore legacy automatic DB row deletion.
AUTOMATIC_TASK_DB_DELETION = os.getenv("AUTOMATIC_TASK_DB_DELETION", "0") == "1"

MIN_FREE_SPACE_GB = float(os.getenv("MIN_FREE_SPACE_GB", "10"))  # Minimum free space to maintain (background cleanup)
CLEANUP_CHECK_INTERVAL_CYCLES = 10  # Check disk space every N background worker cycles (~5 min)
CLEANUP_MIN_AGE_HOURS = 1  # Never delete files younger than this (safety for processing tasks)

# Before each new task: try to reach at least this much free space on /
NEW_TASK_MIN_FREE_GB = float(os.getenv("NEW_TASK_MIN_FREE_GB", "2.1"))
# If ZIP purge is not enough: delete oldest done/error tasks, but free at most this many GB from disk in that phase
# (must be >= typical gap to NEW_TASK_MIN_FREE_GB or Telegram low-disk alerts will repeat)
NEW_TASK_PURGE_TASKS_MAX_FREED_GB = float(os.getenv("NEW_TASK_PURGE_TASKS_MAX_FREED_GB", "8"))

# Max total size of static/tasks (task file cache); enforced before new task; admin can override in DB
TASK_CACHE_MAX_GB = float(os.getenv("TASK_CACHE_MAX_GB", "10.0"))

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

# =============================================================================
# Remote Namecheap DNS API (/api-name-cheap) — X-API-Key must match this value
# =============================================================================
NAMECHEAP_REMOTE_API_KEY = os.getenv(
    "NAMECHEAP_REMOTE_API_KEY",
    "ar_nc_remote_v1_k8m2n9p4q7r1s5t0u3w6x8y2z",
)
# Registrar (Namecheap) — same names as qwerty_vpn/scripts/namecheap_set_vpn_dns.py
NAMECHEAP_API_USER = os.getenv("NAMECHEAP_API_USER", "").strip()
NAMECHEAP_REGISTRAR_API_KEY = os.getenv("NAMECHEAP_API_KEY", "").strip()
NAMECHEAP_USERNAME = os.getenv("NAMECHEAP_USERNAME", NAMECHEAP_API_USER).strip()
NAMECHEAP_CLIENT_IP = os.getenv("NAMECHEAP_CLIENT_IP", "185.171.83.65").strip()
FACERIG_DNS_IP = os.getenv("FACERIG_DNS_IP", "185.171.83.65").strip()
# Comma-separated IPs allowed to call POST /api-name-cheap (in addition to X-API-Key). Empty = no IP check.
_nc_allow = os.getenv("NAMECHEAP_REMOTE_IP_ALLOWLIST", "").strip()
NAMECHEAP_REMOTE_IP_ALLOWLIST: frozenset[str] = frozenset(
    x.strip() for x in _nc_allow.split(",") if x.strip()
)

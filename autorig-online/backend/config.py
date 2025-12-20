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
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "eschota@gmail.com")

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
USER_FREE_LIMIT = 10  # Total free conversions after login (including anon used)
USER_BONUS_AFTER_LOGIN = 7  # Additional credits after login

# =============================================================================
# Upload Settings
# =============================================================================
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/var/autorig/uploads")
UPLOAD_TTL_HOURS = 24
MAX_UPLOAD_SIZE_MB = 100

# =============================================================================
# Progress Check Settings
# =============================================================================
PROGRESS_BATCH_SIZE = 15  # Number of URLs to check per batch
PROGRESS_CONCURRENCY = 10  # Max concurrent HEAD requests
PROGRESS_CHECK_TIMEOUT = 5  # Timeout for HEAD requests in seconds

# =============================================================================
# Rate Limiting
# =============================================================================
RATE_LIMIT_TASKS_PER_MINUTE = 5


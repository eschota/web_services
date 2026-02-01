"""
Configuration for CGTrader Submit Server
"""
import os
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Application Settings
# =============================================================================
APP_NAME = "CGTrader Submit Server"
APP_PORT = int(os.getenv("APP_PORT", "3701"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# =============================================================================
# Paths
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "db", "cgtrader.db")
TMP_DIR = os.path.join(BASE_DIR, "tmp")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
COOKIES_PATH = os.path.join(BASE_DIR, "db", "cgtrader_cookies.json")
MANUAL_COOKIES_PATH = os.path.join(BASE_DIR, "db", "cgtrader_cookies_manual.json")

# Ensure directories exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# =============================================================================
# CGTrader Credentials
# =============================================================================
CGTRADER_EMAIL = os.getenv("CGTRADER_EMAIL", "itisai3d@gmail.com")
CGTRADER_PASSWORD = os.getenv("CGTRADER_PASSWORD", "Zaebaliuzhe55")
CGTRADER_LOGIN_URL = "https://www.cgtrader.com/login"
CGTRADER_UPLOAD_URL = "https://www.cgtrader.com/profile/upload/batch"

# Manual authentication (if provided, skip automatic login)
CGTRADER_CSRF_TOKEN = os.getenv("CGTRADER_CSRF_TOKEN", "")
CGTRADER_SESSION_COOKIE = os.getenv("CGTRADER_SESSION_COOKIE", "")
CGTRADER_AUTH_TOKEN = os.getenv("CGTRADER_AUTH_TOKEN", "")

# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "8222979873:AAEpUHrwYm32GDVb_GHQ-58m-vlf9jffX-g"
)
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "-1003555866288"))

# =============================================================================
# OpenAI API
# =============================================================================
OPENAI_API_KEY = os.getenv(
    "OPENAI_API_KEY",
    "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"
)

# =============================================================================
# Task Settings
# =============================================================================
MAX_TASK_ATTEMPTS = 3
TASK_TIMEOUT_SECONDS = 600  # 10 minutes per task
DOWNLOAD_TIMEOUT_SECONDS = 1800  # 30 minutes for large downloads

# =============================================================================
# Chrome/Selenium Settings (Memory Optimized)
# =============================================================================
# Try different Chrome paths
CHROME_BINARY_PATH = "/usr/bin/google-chrome"  # Try Google Chrome first
if not os.path.exists(CHROME_BINARY_PATH):
    CHROME_BINARY_PATH = "/usr/bin/chromium-browser"
if not os.path.exists(CHROME_BINARY_PATH):
    CHROME_BINARY_PATH = "/snap/bin/chromium"

CHROMEDRIVER_PATH = "/usr/bin/chromedriver"

# Memory-optimized Chrome options
CHROME_OPTIONS = [
    "--headless=new",
    "--disable-gpu",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-extensions",
    "--disable-plugins",
    "--disable-images",  # Don't load images to save memory
    "--disable-javascript",  # Enable only when needed
    "--js-flags=--max-old-space-size=256",
    "--single-process",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--hide-scrollbars",
    "--metrics-recording-only",
    "--mute-audio",
    "--no-first-run",
    "--safebrowsing-disable-auto-update",
    "--window-size=1920,1080",
]

# Options to enable when we need JavaScript (for CGTrader upload)
CHROME_OPTIONS_WITH_JS = [
    "--headless=new",
    "--disable-gpu",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-extensions",
    "--disable-plugins",
    "--js-flags=--max-old-space-size=256",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--mute-audio",
    "--no-first-run",
    "--safebrowsing-disable-auto-update",
    "--window-size=1920,1080",
]

# =============================================================================
# CGTrader Form Defaults
# =============================================================================
CGTRADER_DEFAULT_PRICE = 37
CGTRADER_DEFAULT_LICENSE = "Royalty free"
CGTRADER_DEFAULT_CATEGORY = "Character"
CGTRADER_DEFAULT_SUBCATEGORY = "Man"

# =============================================================================
# Proxy Settings
# =============================================================================
PROXY_URL = os.getenv("PROXY_URL", "")  # http://user:pass@host:port (manual, overrides auto)

# Auto proxy manager settings
ENABLE_AUTO_PROXY = os.getenv("ENABLE_AUTO_PROXY", "true").lower() == "true"
PROXY_CHECK_INTERVAL = int(os.getenv("PROXY_CHECK_INTERVAL", "300"))  # seconds (5 minutes)
MAX_WORKING_PROXIES = int(os.getenv("MAX_WORKING_PROXIES", "50"))

def parse_proxy_url(proxy_url: str) -> Optional[Dict[str, str]]:
    """
    Parse proxy URL into dict format for requests.
    
    Args:
        proxy_url: Proxy URL in format http://user:pass@host:port
        
    Returns:
        Dict with 'http' and 'https' keys, or None if URL is empty/invalid
    """
    if not proxy_url or not proxy_url.strip():
        return None
    
    try:
        # Ensure proxy URL starts with http:// or https://
        if not proxy_url.startswith(("http://", "https://")):
            proxy_url = "http://" + proxy_url
        
        # Return dict format for requests
        return {
            "http": proxy_url,
            "https": proxy_url,
        }
    except Exception:
        return None

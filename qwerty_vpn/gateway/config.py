import os
import secrets

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Database
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{os.path.join(BASE_DIR, 'data', 'qwerty_vpn.db')}")

# Server
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5000"))

# This server's public IP
PUBLIC_IP = os.getenv("PUBLIC_IP", "185.171.83.65")

# 3proxy ports on this server
PROXY_HTTP_PORT = int(os.getenv("PROXY_HTTP_PORT", "49152"))
PROXY_SOCKS_PORT = int(os.getenv("PROXY_SOCKS_PORT", "49153"))

# API Key for extensions (generate once, store in env)
API_KEY = os.getenv("VPN_API_KEY", "qvpn_" + secrets.token_hex(24))

# Healthcheck intervals (seconds)
HEALTHCHECK_TCP_INTERVAL = int(os.getenv("HEALTHCHECK_TCP_INTERVAL", "30"))
HEALTHCHECK_HTTP_INTERVAL = int(os.getenv("HEALTHCHECK_HTTP_INTERVAL", "300"))

# Stats collection interval (seconds)
STATS_INTERVAL = int(os.getenv("STATS_INTERVAL", "60"))

# 3proxy config/log paths
PROXY_CONFIG_PATH = os.getenv("PROXY_CONFIG_PATH", os.path.join(BASE_DIR, "..", "proxy", "3proxy.cfg"))
PROXY_LOG_PATH = os.getenv("PROXY_LOG_PATH", "/var/log/3proxy/3proxy.log")
PROXY_PASSWD_PATH = os.getenv("PROXY_PASSWD_PATH", os.path.join(BASE_DIR, "..", "proxy", "passwd"))

# Gateway base URL (for extensions)
GATEWAY_BASE_URL = os.getenv("GATEWAY_BASE_URL", "https://autorig.online/vpn")

"""
Signed tokens for one-click email unsubscribe links (HMAC-SHA256).
"""
import base64
import hashlib
import hmac
import struct
from typing import Optional

from config import SECRET_KEY


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def build_unsubscribe_token(email: str) -> str:
    """URL-safe token encoding normalized email + HMAC signature."""
    e = _normalize_email(email).encode("utf-8")
    if len(e) > 65535:
        raise ValueError("email too long")
    secret = SECRET_KEY.encode("utf-8")
    sig = hmac.new(secret, e, hashlib.sha256).digest()
    raw = struct.pack("!H", len(e)) + e + sig
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def verify_unsubscribe_token(token: str) -> Optional[str]:
    """Returns normalized email if signature is valid, else None."""
    if not token or not isinstance(token, str):
        return None
    pad = "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(token + pad)
    except Exception:
        return None
    if len(raw) < 2 + 32:
        return None
    try:
        n = struct.unpack("!H", raw[:2])[0]
        e = raw[2 : 2 + n]
        sig = raw[2 + n :]
        if len(sig) != 32 or len(e) != n:
            return None
    except Exception:
        return None
    secret = SECRET_KEY.encode("utf-8")
    expected = hmac.new(secret, e, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        return e.decode("utf-8")
    except Exception:
        return None

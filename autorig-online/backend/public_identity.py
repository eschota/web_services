"""Public identity of an owner: handle, display name, author page. Never an e-mail (Gallery · V3, 2026-10-11).

Privacy fix, 2026-10-11: /api/gallery, /api/task/<id>/card and TaskCard handed the owner's e-mail to every visitor
(the badge showed its local part, `data-author` / `title` the whole address). Everything public now carries the same
handle the public chat and the author pages use: 10 hex of an HMAC (key /srv/autorig/secrets/public-chat.key, the
same one deploy/public-chat/public_chat.py reads, so /author/<handle> resolves), a display name (the site nickname,
else User-XXXX / Guest-XXXX from the handle) and the author page URL. The handle is not reversible to an e-mail,
a user id or an anon_id. `owner_for_handle` maps a handle back to its owner on the server only, for `?author=<handle>`.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import os

KEY_FILE = Path(os.getenv("AUTORIG_PUBLIC_CHAT_KEY_FILE", "/srv/autorig/secrets/public-chat.key"))
FALLBACK_KEY_FILE = Path(os.getenv("AUTORIG_PUBLIC_CHAT_FALLBACK_KEY", "/srv/autorig/data/public-chat/.hmac.key"))
HANDLE_RE = re.compile(r"^[0-9a-f]{10}$")
_KEY: Optional[bytes] = None
_INDEX: Tuple[float, Dict[str, Tuple[str, str]]] = (0.0, {})


def _key() -> bytes:
    global _KEY
    if _KEY is None:
        for path in (KEY_FILE, FALLBACK_KEY_FILE):
            try:
                raw = path.read_bytes().strip()
            except OSError:
                continue
            if len(raw) >= 32:
                _KEY = raw
                break
        else:
            _KEY = b"unavailable-" + os.urandom(16)      # handles are then not the chat's: never an e-mail either way
    return _KEY


def _hkey(value: str) -> str:
    return hmac.new(_key(), value.encode("utf-8"), hashlib.sha256).hexdigest()


def handle_of_author(author: str) -> str:
    return _hkey("tag:" + author)[:10]


def handle_for_user(user_id) -> str:
    return handle_of_author(_hkey(f"u:{user_id}"))


def hkey_for_email(email: str) -> str:
    """An HMAC stand-in for an e-mail (feedback authors without a user row); not the chat's author key."""
    return _hkey("e:" + str(email or "").strip().lower())


def handle_for_anon(anon_id: str) -> str:
    return handle_of_author(_hkey("a:" + str(anon_id or "").strip().lower()))


def display_name(handle: str, kind: str, nickname: Optional[str] = None) -> str:
    """The site nickname when it is a plain name, else User-XXXX / Guest-XXXX. Never an e-mail."""
    nick = str(nickname or "").strip()
    if kind == "user" and 2 <= len(nick) <= 24 and not re.search(r"[@/\\<>:]|https?|www\.", nick, re.I):
        return nick
    return f"{'User' if kind == 'user' else 'Guest'}-{handle[:4].upper()}"


def author_url(handle: str) -> str:
    return f"/author/{handle}"


def card_fields(kind: Optional[str], handle: Optional[str], nickname: Optional[str] = None) -> Dict[str, Optional[str]]:
    if not handle:
        return {"author_handle": None, "author_name": None, "author_url": None, "author_nickname": None}
    name = display_name(handle, kind or "guest", nickname)
    return {"author_handle": handle, "author_name": name, "author_url": author_url(handle), "author_nickname": name}


def looks_like_email(text) -> bool:
    return bool(re.search(r"[^\s@]+@[^\s@]+\.[^\s@]+", str(text or "")))


async def owner_for_handle(db, handle: str) -> Optional[Tuple[str, str]]:
    """(owner_type, owner_id) of a public handle, server side only; cached for five minutes."""
    global _INDEX
    handle = str(handle or "").strip().lower()
    if not HANDLE_RE.match(handle):
        return None
    at, index = _INDEX
    if not index or time.time() - at > 300:
        from sqlalchemy import select
        from database import Task, User
        index = {}
        for uid, email in (await db.execute(select(User.id, User.email))).all():
            if email:
                index[handle_for_user(uid)] = ("user", email)
        for (anon,) in (await db.execute(select(Task.owner_id).where(Task.owner_type == "anon").distinct())).all():
            if anon:
                index[handle_for_anon(anon)] = ("anon", anon)
        _INDEX = (time.time(), index)
    return index.get(handle)

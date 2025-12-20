"""
Google OAuth2 Authentication for AutoRig Online
"""
import secrets
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI,
    SECRET_KEY,
    ANON_FREE_LIMIT,
    USER_FREE_LIMIT
)
from database import User, AnonSession, Session


# =============================================================================
# Google OAuth2 URLs
# =============================================================================
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


# =============================================================================
# OAuth2 Flow
# =============================================================================
def get_google_auth_url(state: Optional[str] = None) -> str:
    """Generate Google OAuth2 authorization URL"""
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "consent",
    }
    if state:
        params["state"] = state
    
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


async def exchange_code_for_tokens(code: str) -> Optional[dict]:
    """Exchange authorization code for access tokens"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "code": code,
                    "client_id": GOOGLE_CLIENT_ID,
                    "client_secret": GOOGLE_CLIENT_SECRET,
                    "redirect_uri": GOOGLE_REDIRECT_URI,
                    "grant_type": "authorization_code",
                },
                timeout=10.0
            )
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None


async def get_google_user_info(access_token: str) -> Optional[dict]:
    """Get user info from Google"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10.0
            )
            
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None


# =============================================================================
# Session Management
# =============================================================================
def generate_session_token() -> str:
    """Generate a secure session token"""
    return secrets.token_hex(32)


async def create_session(db: AsyncSession, user_id: int, days: int = 30) -> str:
    """Create a new session for user"""
    token = generate_session_token()
    expires_at = datetime.utcnow() + timedelta(days=days)
    
    session = Session(
        token=token,
        user_id=user_id,
        expires_at=expires_at
    )
    db.add(session)
    await db.commit()
    
    return token


async def get_user_by_session(db: AsyncSession, token: str) -> Optional[User]:
    """Get user by session token"""
    if not token:
        return None
    
    result = await db.execute(
        select(Session).where(
            Session.token == token,
            Session.expires_at > datetime.utcnow()
        )
    )
    session = result.scalar_one_or_none()
    
    if not session:
        return None
    
    result = await db.execute(
        select(User).where(User.id == session.user_id)
    )
    return result.scalar_one_or_none()


async def delete_session(db: AsyncSession, token: str):
    """Delete a session (logout)"""
    result = await db.execute(
        select(Session).where(Session.token == token)
    )
    session = result.scalar_one_or_none()
    if session:
        await db.delete(session)
        await db.commit()


# =============================================================================
# User Management
# =============================================================================
async def get_or_create_user(
    db: AsyncSession, 
    email: str, 
    name: Optional[str] = None,
    picture: Optional[str] = None,
    anon_session: Optional[AnonSession] = None
) -> User:
    """Get existing user or create new one"""
    result = await db.execute(
        select(User).where(User.email == email)
    )
    user = result.scalar_one_or_none()
    
    if user:
        # Update last login
        user.last_login_at = datetime.utcnow()
        if name:
            user.name = name
        if picture:
            user.picture = picture
        await db.commit()
        return user
    
    # Create new user
    # Calculate initial balance based on anon usage
    anon_used = anon_session.free_used if anon_session else 0
    initial_balance = max(0, USER_FREE_LIMIT - anon_used)
    
    user = User(
        email=email,
        name=name,
        picture=picture,
        balance_credits=initial_balance
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    
    return user


# =============================================================================
# Anonymous Session Management
# =============================================================================
async def get_or_create_anon_session(
    db: AsyncSession, 
    anon_id: str
) -> AnonSession:
    """Get or create anonymous session"""
    result = await db.execute(
        select(AnonSession).where(AnonSession.anon_id == anon_id)
    )
    anon = result.scalar_one_or_none()
    
    if anon:
        anon.last_seen_at = datetime.utcnow()
        await db.commit()
        return anon
    
    anon = AnonSession(anon_id=anon_id)
    db.add(anon)
    await db.commit()
    await db.refresh(anon)
    
    return anon


async def increment_anon_usage(db: AsyncSession, anon_id: str) -> int:
    """Increment anonymous user's usage count, return new count"""
    result = await db.execute(
        select(AnonSession).where(AnonSession.anon_id == anon_id)
    )
    anon = result.scalar_one_or_none()
    
    if anon:
        anon.free_used += 1
        await db.commit()
        return anon.free_used
    
    return 0


# =============================================================================
# Credit/Limit Checking
# =============================================================================
def can_create_task_anon(anon_session: AnonSession) -> bool:
    """Check if anonymous user can create a task"""
    return anon_session.free_used < ANON_FREE_LIMIT


def can_create_task_user(user: User) -> bool:
    """Check if authenticated user can create a task"""
    return user.balance_credits > 0


def get_remaining_credits_anon(anon_session: AnonSession) -> int:
    """Get remaining free conversions for anonymous user"""
    return max(0, ANON_FREE_LIMIT - anon_session.free_used)


async def decrement_user_credits(db: AsyncSession, user: User) -> int:
    """Decrement user credits, return new balance"""
    if user.balance_credits > 0:
        user.balance_credits -= 1
        user.total_tasks += 1
        await db.commit()
    return user.balance_credits


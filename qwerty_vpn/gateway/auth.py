from fastapi import Request, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select

from database import async_session
from models import Client
from config import API_KEY

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)):
    """Verify API key from header. Checks master key and client keys."""
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API key")

    # Check master API key
    if api_key == API_KEY:
        return {"type": "master", "client_id": None}

    # Check client API keys
    async with async_session() as session:
        result = await session.execute(
            select(Client).where(Client.api_key == api_key)
        )
        client = result.scalar_one_or_none()
        if client:
            return {"type": "client", "client_id": client.client_id}

    raise HTTPException(status_code=403, detail="Invalid API key")


async def optional_api_key(api_key: str = Security(api_key_header)):
    """Optional API key - returns None if not provided."""
    if not api_key:
        return None
    try:
        return await verify_api_key(api_key)
    except HTTPException:
        return None

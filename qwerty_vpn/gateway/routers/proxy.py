"""GET /api/get-proxy - assign a proxy to a client."""

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from auth import verify_api_key
from models import Session as VPNSession
from services.balancer import Balancer

router = APIRouter()


@router.get("/api/get-proxy")
async def get_proxy(
    client_id: str = Query(..., description="Client identifier"),
    country: str = Query(None, description="Country code (e.g. US, DE, RU)"),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(verify_api_key),
):
    """Assign the best available proxy to the client."""
    vps = await Balancer.get_best_vps(db, country=country)

    if not vps:
        raise HTTPException(
            status_code=503,
            detail="No available VPS proxy nodes" + (f" for country {country}" if country else ""),
        )

    # Create session record
    session = VPNSession(
        client_id=1,  # Will be resolved properly with client lookup
        vps_id=vps.id,
        proxy_host=vps.ip,
        proxy_port=vps.proxy_port,
        started_at=datetime.utcnow(),
    )
    db.add(session)
    await db.commit()

    return {
        "proxy_host": vps.ip,
        "proxy_port": vps.proxy_port,
        "proxy_socks_port": vps.socks_port,
        "proxy_username": vps.proxy_username,
        "proxy_password": vps.proxy_password,
        "vps_id": vps.id,
        "country": vps.country,
    }

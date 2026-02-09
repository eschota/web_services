"""Admin endpoints - manage VPS nodes."""

from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from auth import verify_api_key
from models import VPSNode, VPSStatus

router = APIRouter()


class AddVPSRequest(BaseModel):
    ip: str
    country: str = "US"
    proxy_port: int = 49152
    socks_port: int = 49153
    max_capacity_gbps: float = 1.0
    weight: int = 1


class RemoveVPSRequest(BaseModel):
    vps_id: int


@router.post("/api/add-vps")
async def add_vps(
    body: AddVPSRequest,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(verify_api_key),
):
    """Register a new VPS proxy node."""
    # Check if already exists
    existing = await db.execute(
        select(VPSNode).where(VPSNode.ip == body.ip)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"VPS with IP {body.ip} already exists")

    node = VPSNode(
        ip=body.ip,
        country=body.country.upper(),
        proxy_port=body.proxy_port,
        socks_port=body.socks_port,
        proxy_username="",
        proxy_password="",
        status=VPSStatus.online,
        max_capacity_gbps=body.max_capacity_gbps,
        weight=body.weight,
    )
    db.add(node)
    await db.commit()
    await db.refresh(node)

    return {
        "status": "ok",
        "vps_id": node.id,
        "ip": node.ip,
        "country": node.country,
        "message": f"VPS node {node.ip} ({node.country}) added successfully",
    }


@router.post("/api/remove-vps")
async def remove_vps(
    body: RemoveVPSRequest,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(verify_api_key),
):
    """Remove a VPS proxy node."""
    result = await db.execute(
        select(VPSNode).where(VPSNode.id == body.vps_id)
    )
    node = result.scalar_one_or_none()
    if not node:
        raise HTTPException(status_code=404, detail=f"VPS {body.vps_id} not found")

    await db.delete(node)
    await db.commit()

    return {"status": "ok", "message": f"VPS {body.vps_id} ({node.ip}) removed"}


@router.get("/api/list-vps")
async def list_vps(
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(verify_api_key),
):
    """List all VPS nodes."""
    result = await db.execute(select(VPSNode))
    nodes = result.scalars().all()

    return [
        {
            "vps_id": n.id,
            "ip": n.ip,
            "country": n.country,
            "proxy_port": n.proxy_port,
            "socks_port": n.socks_port,
            "status": n.status.value,
            "weight": n.weight,
        }
        for n in nodes
    ]

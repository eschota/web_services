"""GET /api/status - system status for extension UI."""

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import VPSNode, VPSStat, VPSStatus, Session as VPNSession

router = APIRouter()


@router.get("/api/status")
async def get_status(db: AsyncSession = Depends(get_db)):
    """Return overall system status."""

    # Total VPS count
    total_result = await db.execute(select(func.count(VPSNode.id)))
    total_vps = total_result.scalar() or 0

    # Online VPS count
    online_result = await db.execute(
        select(func.count(VPSNode.id)).where(VPSNode.status == VPSStatus.online)
    )
    online_vps = online_result.scalar() or 0

    # Active sessions (proxies)
    active_result = await db.execute(
        select(func.count(VPNSession.id)).where(VPNSession.ended_at.is_(None))
    )
    active_proxies = active_result.scalar() or 0

    # Average traffic across all VPS (latest stats)
    avg_traffic_result = await db.execute(
        select(func.avg(VPSStat.traffic_gbps_current))
    )
    avg_traffic = avg_traffic_result.scalar() or 0.0

    return {
        "gateway_status": "up",
        "total_vps_count": total_vps,
        "online_vps_count": online_vps,
        "total_active_proxies": active_proxies,
        "average_traffic_gbps_total": round(avg_traffic, 6),
    }

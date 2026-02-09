"""GET /api/vps-stats - per-VPS statistics."""

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import VPSNode, VPSStat

router = APIRouter()


@router.get("/api/vps-stats")
async def get_vps_stats(db: AsyncSession = Depends(get_db)):
    """Return statistics for all VPS nodes."""

    result = await db.execute(select(VPSNode))
    nodes = result.scalars().all()

    stats_list = []
    for node in nodes:
        # Get latest stat
        stat_result = await db.execute(
            select(VPSStat)
            .where(VPSStat.vps_id == node.id)
            .order_by(VPSStat.timestamp.desc())
            .limit(1)
        )
        stat = stat_result.scalar_one_or_none()

        stats_list.append({
            "vps_id": node.id,
            "ip": node.ip,
            "country": node.country,
            "proxy_port": node.proxy_port,
            "socks_port": node.socks_port,
            "status": node.status.value if node.status else "unknown",
            "active_connections": stat.active_connections if stat else 0,
            "traffic_gbps_last_hour": stat.traffic_gbps_last_hour if stat else 0.0,
            "traffic_gbps_avg": stat.traffic_gbps_avg if stat else 0.0,
            "max_capacity_gbps": node.max_capacity_gbps,
        })

    return stats_list

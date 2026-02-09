"""Stats collector - gathers metrics from VPS nodes (local server)."""

import logging
import os
import re
from datetime import datetime, timedelta

import psutil
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from database import async_session
from models import VPSNode, VPSStat
from config import PROXY_LOG_PATH

logger = logging.getLogger("stats_collector")


def get_system_stats() -> dict:
    """Get current system CPU and memory stats."""
    return {
        "cpu_load": psutil.cpu_percent(interval=0.5),
        "memory_usage": psutil.virtual_memory().percent,
    }


def count_active_connections(proxy_port: int = 3128) -> int:
    """Count active connections to the proxy port."""
    count = 0
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if conn.laddr and conn.laddr.port == proxy_port and conn.status == "ESTABLISHED":
                count += 1
    except (psutil.AccessDenied, PermissionError):
        pass
    return count


def parse_3proxy_log_traffic(log_path: str, hours: float = 1.0) -> dict:
    """Parse 3proxy log to estimate traffic in the last N hours."""
    total_bytes_in = 0
    total_bytes_out = 0
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    if not os.path.exists(log_path):
        return {"bytes_in": 0, "bytes_out": 0}

    try:
        with open(log_path, "r") as f:
            for line in f:
                # 3proxy log format varies, try common patterns
                # Example: timestamp ... bytes_in bytes_out
                parts = line.strip().split()
                if len(parts) >= 6:
                    try:
                        total_bytes_in += int(parts[-2]) if parts[-2].isdigit() else 0
                        total_bytes_out += int(parts[-1]) if parts[-1].isdigit() else 0
                    except (ValueError, IndexError):
                        pass
    except Exception as e:
        logger.warning(f"Failed to parse 3proxy log: {e}")

    return {"bytes_in": total_bytes_in, "bytes_out": total_bytes_out}


def get_network_traffic_gbps() -> float:
    """Get current network throughput in Gbps using psutil."""
    counters1 = psutil.net_io_counters()
    import time
    time.sleep(1)
    counters2 = psutil.net_io_counters()

    bytes_per_sec = (
        (counters2.bytes_sent - counters1.bytes_sent) +
        (counters2.bytes_recv - counters1.bytes_recv)
    )
    gbps = (bytes_per_sec * 8) / 1_000_000_000
    return round(gbps, 6)


async def collect_stats():
    """Collect and store stats for all VPS nodes."""
    async with async_session() as db:
        result = await db.execute(select(VPSNode))
        nodes = result.scalars().all()

        sys_stats = get_system_stats()

        for node in nodes:
            active_conns = count_active_connections(node.proxy_port)
            current_gbps = get_network_traffic_gbps()

            # Calculate hourly average from recent stats
            hour_ago = datetime.utcnow() - timedelta(hours=1)
            avg_result = await db.execute(
                select(func.avg(VPSStat.traffic_gbps_current))
                .where(VPSStat.vps_id == node.id)
                .where(VPSStat.timestamp >= hour_ago)
            )
            avg_gbps = avg_result.scalar() or 0.0

            # Calculate overall average
            overall_avg_result = await db.execute(
                select(func.avg(VPSStat.traffic_gbps_current))
                .where(VPSStat.vps_id == node.id)
            )
            overall_avg = overall_avg_result.scalar() or 0.0

            stat = VPSStat(
                vps_id=node.id,
                active_connections=active_conns,
                traffic_gbps_current=current_gbps,
                traffic_gbps_last_hour=round(avg_gbps, 6),
                traffic_gbps_avg=round(overall_avg, 6),
                cpu_load=sys_stats["cpu_load"],
                memory_usage=sys_stats["memory_usage"],
                timestamp=datetime.utcnow(),
            )
            db.add(stat)

        await db.commit()

        # Cleanup old stats (keep last 24 hours)
        cutoff = datetime.utcnow() - timedelta(hours=24)
        await db.execute(
            VPSStat.__table__.delete().where(VPSStat.timestamp < cutoff)
        )
        await db.commit()

        logger.debug(f"Stats collected for {len(nodes)} nodes")

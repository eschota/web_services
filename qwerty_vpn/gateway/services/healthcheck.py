"""Healthcheck service - periodically checks VPS nodes."""

import asyncio
import socket
import logging
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import async_session
from models import VPSNode, VPSStatus

logger = logging.getLogger("healthcheck")


async def tcp_check(host: str, port: int, timeout: float = 5.0) -> bool:
    """Check if a TCP port is open."""
    try:
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        await loop.run_in_executor(None, sock.connect, (host, port))
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


async def http_proxy_check(
    host: str, port: int, timeout: float = 10.0
) -> bool:
    """Check if HTTP proxy works by making a request through it."""
    proxy_url = f"http://{host}:{port}"
    try:
        async with httpx.AsyncClient(
            proxy=proxy_url, timeout=timeout, verify=False
        ) as client:
            resp = await client.get("http://httpbin.org/ip")
            return resp.status_code == 200
    except Exception as e:
        logger.warning(f"HTTP proxy check failed for {host}:{port}: {e}")
        return False


async def check_all_nodes_tcp():
    """TCP healthcheck for all VPS nodes."""
    async with async_session() as db:
        result = await db.execute(select(VPSNode))
        nodes = result.scalars().all()

        for node in nodes:
            is_alive = await tcp_check(node.ip, node.proxy_port)

            old_status = node.status
            if is_alive:
                if node.status == VPSStatus.offline:
                    node.status = VPSStatus.online
                    logger.info(f"VPS {node.id} ({node.ip}) is now ONLINE")
            else:
                if node.status != VPSStatus.offline:
                    node.status = VPSStatus.offline
                    logger.warning(f"VPS {node.id} ({node.ip}) is now OFFLINE")

            node.updated_at = datetime.utcnow()

        await db.commit()


async def check_all_nodes_http():
    """Full HTTP proxy check for all VPS nodes."""
    async with async_session() as db:
        result = await db.execute(
            select(VPSNode).where(VPSNode.status != VPSStatus.offline)
        )
        nodes = result.scalars().all()

        for node in nodes:
            works = await http_proxy_check(node.ip, node.proxy_port)

            if not works and node.status == VPSStatus.online:
                node.status = VPSStatus.degraded
                logger.warning(f"VPS {node.id} ({node.ip}) is DEGRADED (proxy not working)")
            elif works and node.status == VPSStatus.degraded:
                node.status = VPSStatus.online
                logger.info(f"VPS {node.id} ({node.ip}) recovered to ONLINE")

            node.updated_at = datetime.utcnow()

        await db.commit()

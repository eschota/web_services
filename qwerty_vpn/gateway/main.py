"""QwertyStock VPN Gateway - main FastAPI application."""

import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import (
    API_KEY, PUBLIC_IP, PROXY_HTTP_PORT, PROXY_SOCKS_PORT,
    HEALTHCHECK_TCP_INTERVAL, HEALTHCHECK_HTTP_INTERVAL, STATS_INTERVAL,
)
from database import init_db, async_session
from models import VPSNode, VPSStat, Client, VPSStatus
from services.healthcheck import check_all_nodes_tcp, check_all_nodes_http
from services.stats_collector import collect_stats
from routers import proxy, status, vps_stats, domain_rules, admin

from sqlalchemy import select

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("gateway")

scheduler = AsyncIOScheduler()


async def seed_initial_data():
    """Create initial VPS node and test client if DB is empty."""
    async with async_session() as db:
        # Check if any VPS exists
        result = await db.execute(select(VPSNode).limit(1))
        if result.scalar() is None:
            proxy_user = "qvpn_user"
            proxy_pass = secrets.token_urlsafe(16)

            node = VPSNode(
                ip=PUBLIC_IP,
                country="NL",  # Netherlands (server location)
                proxy_port=PROXY_HTTP_PORT,
                socks_port=PROXY_SOCKS_PORT,
                proxy_username=proxy_user,
                proxy_password=proxy_pass,
                status=VPSStatus.online,
                max_capacity_gbps=1.0,
                weight=1,
            )
            db.add(node)
            await db.flush()

            logger.info(f"Created initial VPS node: {PUBLIC_IP}")
            logger.info(f"Proxy credentials: {proxy_user} / {proxy_pass}")

            # Write proxy credentials to 3proxy passwd file
            import os
            passwd_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "..", "proxy", "passwd"
            )
            os.makedirs(os.path.dirname(passwd_path), exist_ok=True)
            with open(passwd_path, "w") as f:
                # 3proxy passwd format: user:CL:password
                f.write(f"{proxy_user}:CL:{proxy_pass}\n")
            logger.info(f"Wrote proxy credentials to {passwd_path}")

        # Check if test client exists
        client_result = await db.execute(
            select(Client).where(Client.client_id == "test_client").limit(1)
        )
        if client_result.scalar() is None:
            client = Client(
                client_id="test_client",
                api_key=API_KEY,
                name="Test Client",
            )
            db.add(client)
            logger.info(f"Created test client with API key: {API_KEY}")

        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    logger.info("Starting QwertyStock VPN Gateway...")
    await init_db()
    await seed_initial_data()

    # Schedule background tasks
    scheduler.add_job(check_all_nodes_tcp, "interval", seconds=HEALTHCHECK_TCP_INTERVAL, id="tcp_check")
    scheduler.add_job(check_all_nodes_http, "interval", seconds=HEALTHCHECK_HTTP_INTERVAL, id="http_check")
    scheduler.add_job(collect_stats, "interval", seconds=STATS_INTERVAL, id="stats_collect")
    scheduler.start()
    logger.info("Background tasks scheduled")

    yield

    # Shutdown
    scheduler.shutdown(wait=False)
    logger.info("Gateway stopped")


app = FastAPI(
    title="QwertyStock VPN Gateway",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS - allow extensions
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(proxy.router, tags=["proxy"])
app.include_router(status.router, tags=["status"])
app.include_router(vps_stats.router, tags=["vps-stats"])
app.include_router(domain_rules.router, tags=["domain-rules"])
app.include_router(admin.router, tags=["admin"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "qwerty_vpn_gateway"}


@app.get("/api/info")
async def info():
    """Return gateway info including API key for initial setup."""
    return {
        "gateway": "QwertyStock VPN Gateway",
        "version": "1.0.0",
        "api_key": API_KEY,
        "base_url": "https://autorig.online/vpn",
    }

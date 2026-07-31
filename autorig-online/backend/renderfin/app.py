"""Renderfin service entrypoint: uvicorn renderfin.app:app --host 127.0.0.1 --port 8010"""
from __future__ import annotations

import shutil
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config
from .api import router
from .character_gen import CharacterGenManager
from .queue import RenderQueue
from .registry import ServerRegistry
from .telegram_delivery import TelegramDeliveryService


def _install_masks() -> None:
    """Copy packaged masks into the served render/masks dir (idempotent)."""
    target = config.RENDER_DIR / "masks"
    target.mkdir(parents=True, exist_ok=True)
    if not config.MASKS_DIR.is_dir():
        return
    for src in config.MASKS_DIR.iterdir():
        if not src.is_file():
            continue
        dst = target / src.name
        if not dst.is_file() or dst.stat().st_size != src.stat().st_size:
            shutil.copyfile(src, dst)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config.ensure_dirs()
    _install_masks()
    registry = ServerRegistry()
    queue = RenderQueue(registry)
    await queue.start()
    chargen = CharacterGenManager(queue)
    await chargen.start()
    delivery = TelegramDeliveryService(chargen)
    await delivery.start()
    app.state.registry = registry
    app.state.render_queue = queue
    app.state.character_gen = chargen
    app.state.delivery = delivery
    print(
        f"[Renderfin] up: {len(registry.all())} server(s), data dir {config.DATA_DIR}"
    )
    try:
        yield
    finally:
        await delivery.stop()
        await chargen.stop()
        await queue.stop()


app = FastAPI(title="AutoRig Renderfin", lifespan=lifespan)
app.include_router(router)

"""Lossless local and cross-process wakeups for the converter scheduler.

The web backend owns the asyncio queue and a loopback-only UDP listener.  The
Telegram process only sends the fixed datagram, so it never imports ``main`` or
creates a process-local event that the backend cannot observe.
"""
from __future__ import annotations

import asyncio
import os
import socket
from typing import Optional


WAKE_HOST = "127.0.0.1"
WAKE_PORT = int(os.getenv("AUTORIG_SCHEDULER_WAKE_PORT", "18765"))
WAKE_MESSAGE = b"autorig-scheduler-wake-v1"


def enqueue_wake(wake_queue: "asyncio.Queue[None]") -> None:
    """Coalesce wakeups without clearing an already-pending notification."""
    try:
        wake_queue.put_nowait(None)
    except asyncio.QueueFull:
        pass


def notify_scheduler(*, host: str = WAKE_HOST, port: int = WAKE_PORT) -> bool:
    """Notify the backend scheduler from any local process, best effort."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(WAKE_MESSAGE, (host, int(port)))
        return True
    except OSError as exc:
        print(f"[Priority] Scheduler UDP wake failed: {exc}")
        return False


class SchedulerWakeProtocol(asyncio.DatagramProtocol):
    def __init__(self, wake_queue: "asyncio.Queue[None]"):
        self.wake_queue = wake_queue

    def datagram_received(self, data: bytes, addr) -> None:
        if data == WAKE_MESSAGE:
            enqueue_wake(self.wake_queue)


async def start_wake_listener(
    wake_queue: "asyncio.Queue[None]",
    *,
    host: str = WAKE_HOST,
    port: int = WAKE_PORT,
) -> Optional[asyncio.DatagramTransport]:
    """Bind the loopback listener used by Telegram/Renderfin task creation."""
    loop = asyncio.get_running_loop()
    try:
        transport, _protocol = await loop.create_datagram_endpoint(
            lambda: SchedulerWakeProtocol(wake_queue),
            local_addr=(host, int(port)),
        )
    except OSError as exc:
        # The bounded database poll remains available, but never pretend that
        # cross-process signalling is healthy when the listener did not bind.
        print(f"[Priority] Scheduler UDP listener unavailable on {host}:{port}: {exc}")
        return None
    print(f"[Priority] Scheduler UDP wake listening on {host}:{port}")
    return transport


async def wait_for_wake(
    wake_queue: "asyncio.Queue[None]", *, timeout: float = 5.0
) -> bool:
    """Consume one pending wake; unlike Event.clear(), concurrent wakes survive."""
    try:
        await asyncio.wait_for(wake_queue.get(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False

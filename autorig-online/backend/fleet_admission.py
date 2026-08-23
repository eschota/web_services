"""Cross-process admission lock for shared converter/Hunyuan capacity.

AutoRig full conversion and Renderfin Hunyuan may run in different service
processes.  Both must take this lock before observing shared-worker capacity
and persist their accepted assignment, otherwise two background snapshots can
consume the last two idle full converters simultaneously.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional


def _default_lock_path() -> Path:
    configured = str(os.getenv("AUTORIG_FLEET_ADMISSION_LOCK") or "").strip()
    if configured:
        return Path(configured)
    if os.name == "nt":
        return Path(tempfile.gettempdir()) / "autorig-fleet-admission.lock"
    return Path("/srv/autorig/data/var/renderfin/locks/fleet-admission.lock")


class _NonBlockingFileLock:
    def __init__(self, path: Path):
        self.path = path
        self.stream: Optional[object] = None

    def try_acquire(self) -> bool:
        if self.stream is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.stream = open(self.path, "a+b")
            if os.name == "nt":
                self.stream.seek(0, os.SEEK_END)
                if self.stream.tell() == 0:
                    self.stream.write(b"0")
                    self.stream.flush()
                self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                self.stream.seek(0)
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (BlockingIOError, OSError):
            return False

    def release(self) -> None:
        if self.stream is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.stream.seek(0)
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
        finally:
            self.stream.close()
            self.stream = None


@asynccontextmanager
async def fleet_admission_lock() -> AsyncIterator[None]:
    lock = _NonBlockingFileLock(_default_lock_path())
    acquired = False
    try:
        while not acquired:
            acquired = lock.try_acquire()
            if not acquired:
                await asyncio.sleep(0.05)
        yield
    finally:
        if acquired:
            lock.release()
        elif lock.stream is not None:
            lock.stream.close()
            lock.stream = None

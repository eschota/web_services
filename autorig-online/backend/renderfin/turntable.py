"""Subprocess wrapper around tools/renderfin/glb_turntable.mjs."""
from __future__ import annotations

import asyncio
from pathlib import Path

from . import config


class TurntableError(RuntimeError):
    pass


async def render_turntable(glb_path: Path, out_path: Path, *, seconds: float = 0) -> Path:
    glb_path = Path(glb_path)
    out_path = Path(out_path)
    if not glb_path.is_file():
        raise TurntableError(f"glb not found: {glb_path}")
    script = Path(config.TURNTABLE_SCRIPT)
    if not script.is_file():
        raise TurntableError(f"turntable script not found: {script}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        config.TURNTABLE_NODE,
        str(script),
        "--glb", str(glb_path),
        "--output", str(out_path),
        "--seconds", str(seconds or config.TURNTABLE_SECONDS),
        "--ffmpeg", config.TURNTABLE_FFMPEG,
    ]
    if config.TURNTABLE_CHROME:
        cmd += ["--chrome", config.TURNTABLE_CHROME]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        stdout, _ = await asyncio.wait_for(
            proc.communicate(), timeout=config.TURNTABLE_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        proc.kill()
        raise TurntableError("turntable render timed out")
    tail = (stdout or b"").decode("utf-8", "replace")[-2000:]
    if proc.returncode != 0:
        raise TurntableError(f"turntable exited {proc.returncode}: {tail}")
    if not out_path.is_file() or out_path.stat().st_size < 1024:
        raise TurntableError(f"turntable produced no output: {tail}")
    return out_path

"""Subprocess wrapper around tools/renderfin/glb_turntable.mjs.

``render_turntable`` makes the 6-second orbit MP4 (character_gen stage 3).
``render_still`` / ``render_views`` make clean PNG stills of a GLB, e.g. the
front view the Regen pipeline hands to an image-edit model. Views follow the
browser preflight renderer in static/js/app.js: "front" looks at the model
from +Z (glTF's forward), "back" from -Z, "left" from -X, "right" from +X.
"""
from __future__ import annotations

import asyncio
import math
import os
import re
import signal
from pathlib import Path
from typing import Iterable

from . import config

STILL_VIEWS = ("front", "back", "left", "right", "top_side_45")
STILL_BACKGROUND = "#7f7f7f"  # mid-grey: white backgrounds make downstream matting worst
_BACKGROUND_PATTERN = re.compile(r"#[0-9a-f]{6}")
_MIN_OUTPUT_BYTES = 1024


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

    tail = await _run(cmd, label="turntable", timeout_message="turntable render timed out")
    if not out_path.is_file() or out_path.stat().st_size < _MIN_OUTPUT_BYTES:
        raise TurntableError(f"turntable produced no output: {tail}")
    return out_path


async def render_still(
    glb_path: Path,
    out_png: Path,
    *,
    view: str = "front",
    size: int = 1024,
    background: str = STILL_BACKGROUND,
    ortho: bool = True,
    margin: float = 0.08,
) -> Path:
    """Render one view of ``glb_path`` to exactly ``out_png``.

    The camera (orthographic by default) is fitted to the posed model so the
    silhouette is centred with ``margin`` of the frame clear on every side.
    ``background`` is ``"#rrggbb"`` or ``"transparent"`` (alpha PNG).
    """
    out_png = Path(out_png)
    (view,) = _check_views((view,))
    options = _still_options(size=size, background=background, ortho=ortho, margin=margin)
    cmd = _still_command(Path(glb_path), out_png, ["--still", view], options)
    await _run_stills(cmd, [out_png], options)
    return out_png


async def render_views(
    glb_path: Path,
    out_dir: Path,
    *,
    views: Iterable[str] = ("front", "back", "left", "right"),
    size: int = 1024,
    background: str = STILL_BACKGROUND,
    ortho: bool = True,
    margin: float = 0.08,
) -> dict[str, Path]:
    """Render several views in one browser session.

    Files are ``<out_dir>/<glb stem>_<view>.png``; returns ``{view: path}`` in
    the requested order. Options are as for :func:`render_still`.
    """
    glb_path = Path(glb_path)
    out_dir = Path(out_dir)
    names = _check_views(views)
    options = _still_options(size=size, background=background, ortho=ortho, margin=margin)
    outputs = {name: out_dir / f"{glb_path.stem}_{name}.png" for name in names}
    # The script names each file <output-stem>_<view>.png.
    cmd = _still_command(
        glb_path, out_dir / f"{glb_path.stem}.png", ["--views", ",".join(names)], options
    )
    await _run_stills(cmd, list(outputs.values()), options)
    return outputs


def _check_views(views: Iterable[str]) -> list[str]:
    if isinstance(views, str):
        views = views.split(",")
    names = [str(name).strip() for name in views]
    if not names:
        raise TurntableError("no still views requested")
    unknown = [name for name in names if name not in STILL_VIEWS]
    if unknown:
        raise TurntableError(
            f"unknown still view(s) {', '.join(unknown)}; expected {', '.join(STILL_VIEWS)}"
        )
    if len(set(names)) != len(names):
        raise TurntableError(f"still views repeat: {', '.join(names)}")
    return names


def _still_options(*, size, background, ortho, margin) -> dict:
    if isinstance(size, bool) or not isinstance(size, int) or not 64 <= size <= 2048:
        raise TurntableError(f"still size must be an integer in [64, 2048], got {size!r}")
    background = str(background).strip().lower()
    if background != "transparent" and not _BACKGROUND_PATTERN.fullmatch(background):
        raise TurntableError(
            f"still background must be '#rrggbb' or 'transparent', got {background!r}"
        )
    try:
        margin = float(margin)
    except (TypeError, ValueError):
        raise TurntableError(f"still margin must be a number, got {margin!r}") from None
    if not math.isfinite(margin) or not 0 <= margin <= 0.4:
        raise TurntableError(f"still margin must be in [0, 0.4], got {margin!r}")
    return {"size": size, "background": background, "ortho": bool(ortho), "margin": margin}


def _still_command(glb_path: Path, output: Path, selector: list[str], options: dict) -> list[str]:
    if not glb_path.is_file():
        raise TurntableError(f"glb not found: {glb_path}")
    script = Path(config.TURNTABLE_SCRIPT)
    if not script.is_file():
        raise TurntableError(f"turntable script not found: {script}")
    output.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        config.TURNTABLE_NODE,
        str(script),
        "--glb", str(glb_path),
        "--output", str(output),
        *selector,
        "--size", str(options["size"]),
        "--background", options["background"],
        "--ortho", "1" if options["ortho"] else "0",
        "--margin", f"{options['margin']:g}",
    ]
    if config.TURNTABLE_CHROME:
        cmd += ["--chrome", config.TURNTABLE_CHROME]
    return cmd


async def _run_stills(cmd: list[str], outputs: list[Path], options: dict) -> None:
    # A file left over from an earlier run must not pass for this one.
    for path in outputs:
        path.unlink(missing_ok=True)
    tail = await _run(cmd, label="still render", timeout_message="still render timed out")
    transparent = options["background"] == "transparent"
    for path in outputs:
        _verify_png(path, options["size"], tail, transparent=transparent)


def _verify_png(path: Path, size: int, tail: str, *, transparent: bool) -> None:
    """Never trust the exit code: the PNG must decode, at the requested size, and not be blank."""
    if not path.is_file() or path.stat().st_size < _MIN_OUTPUT_BYTES:
        raise TurntableError(f"still render produced no output {path.name}: {tail}")
    from PIL import Image

    try:
        with Image.open(path) as image:
            image_format, (width, height) = image.format, image.size
        if image_format == "PNG" and (width, height) == (size, size):
            with Image.open(path) as image:
                image.verify()  # chunk structure and CRCs
            with Image.open(path) as image:
                image.load()  # full decode: catches truncated pixel data
                has_alpha = "A" in image.getbands()
                if transparent:
                    blank = has_alpha and image.getchannel("A").getextrema()[1] == 0
                else:
                    blank = all(low == high for low, high in image.convert("RGB").getextrema())
    except Exception as exc:  # PIL raises OSError, SyntaxError, ValueError, ...
        raise TurntableError(
            f"still render wrote an unreadable image {path.name} ({exc}): {tail}"
        ) from exc
    if image_format != "PNG":
        raise TurntableError(f"still render {path.name} is {image_format}, not PNG: {tail}")
    if (width, height) != (size, size):
        raise TurntableError(
            f"still render {path.name} is {width}x{height}, expected {size}x{size}: {tail}"
        )
    if transparent and not has_alpha:
        raise TurntableError(f"still render {path.name} has no alpha channel: {tail}")
    if blank:
        raise TurntableError(f"still render {path.name} is blank: {tail}")


async def _run(cmd: list[str], *, label: str, timeout_message: str) -> str:
    """Run the renderer and return its output tail; raise on timeout or a non-zero exit.

    The script launches Chrome, so it runs in its own process group: a timeout
    kills Chrome with it instead of leaving it orphaned on the host.
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=os.name == "posix",
    )
    try:
        stdout, _ = await asyncio.wait_for(
            proc.communicate(), timeout=config.TURNTABLE_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        _kill_process_group(proc)
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        raise TurntableError(timeout_message)
    tail = (stdout or b"").decode("utf-8", "replace")[-2000:]
    if proc.returncode != 0:
        raise TurntableError(f"{label} exited {proc.returncode}: {tail}")
    return tail


def _kill_process_group(proc: asyncio.subprocess.Process) -> None:
    if os.name == "posix":
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            return
        except (ProcessLookupError, PermissionError):
            pass
    try:
        proc.kill()
    except ProcessLookupError:
        pass

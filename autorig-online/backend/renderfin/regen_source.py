"""Find the picture a regen starts from: the existing task's own model.

A regen re-poses a character that already exists as an AutoRig task. The edit
model needs one clean still of it, so this module finds the task's GLB - this
host's cache first, then the main app, then the public site - and, when no
model survives anywhere, the task's poster as a stand-in.

The main app answers every cached file with an EMPTY 200 carrying
X-Accel-Redirect, for nginx to fill in. Called directly on 127.0.0.1 that is
indistinguishable from a zero-byte model unless the header is read, so the
internal uri is mapped back to the file on this host.
"""
from __future__ import annotations

import io
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Union
from urllib.parse import unquote

import httpx
from PIL import Image

from . import config

# Views the still renderer can take that are worth re-posing from; the edit is
# always asked for a front view, so anything else also has to turn around.
REGEN_VIEWS = ("front", "back", "left", "right")
_POSTER_MAX_BYTES = 32 * 1024 * 1024
_MIN_POSTER_SIDE = 128
_MIN_STILL_SIDE = 256
# The backdrop the still renderer paints (#7f7f7f); a poster is letterboxed onto
# the same one so both sources reach the edit looking alike.
_BACKDROP = (127, 127, 127)


class RegenSourceError(RuntimeError):
    """No usable picture of the task could be made: this attempt is spent."""


class RegenSourceUnavailable(RegenSourceError):
    """The model most likely exists but could not be reached right now.

    The main app restarting or a deploy's 502 says nothing about the task, so
    the stage waits and asks again rather than settling for the poster.
    """


class _Transient(Exception):
    """This answer says nothing about the task (unreachable host, 5xx)."""


def normalize_task_id(value: str) -> str:
    """The canonical AutoRig task id, or ValueError.

    It is interpolated into URLs and cache paths, so nothing but a UUID gets
    through.
    """
    try:
        return str(uuid.UUID(str(value or "").strip()))
    except (ValueError, AttributeError, TypeError):
        raise ValueError(
            f"task_id must be an AutoRig task UUID, got {str(value)[:64]!r}"
        ) from None


def normalize_view(value: str) -> str:
    view = str(value or "").strip().lower() or "front"
    if view not in REGEN_VIEWS:
        raise ValueError(f"view must be one of: {', '.join(REGEN_VIEWS)}")
    return view


@dataclass(frozen=True)
class _Candidate:
    label: str
    url: str = ""
    path: Optional[Path] = None


def glb_candidates(task_id: str) -> List[_Candidate]:
    """Where the task's model may be, cheapest first.

    prepared.glb is what the viewer shows; model.glb is the converter's main
    output and often survives in the artifact cache when the prepared copy
    does not. The public site only matters when the main app is not on this
    host or not answering.
    """
    internal = config.MAIN_APP_INTERNAL_URL
    public = config.MAIN_APP_PUBLIC_URL
    return [
        _Candidate(
            "glb_cache prepared",
            path=config.MAIN_GLB_CACHE_DIR / f"{task_id}_prepared.glb",
        ),
        _Candidate("main app prepared.glb", url=f"{internal}/api/task/{task_id}/prepared.glb"),
        _Candidate("main app model.glb", url=f"{internal}/api/task/{task_id}/model.glb"),
        _Candidate("site prepared.glb", url=f"{public}/api/task/{task_id}/prepared.glb"),
        _Candidate("site model.glb", url=f"{public}/api/task/{task_id}/model.glb"),
    ]


def poster_candidates(task_id: str) -> List[_Candidate]:
    """The gallery poster first (the character, presented), then the browser
    preflight render, which is a top-down 3/4 view and a worse start."""
    internal = config.MAIN_APP_INTERNAL_URL
    public = config.MAIN_APP_PUBLIC_URL
    return [
        _Candidate("main app thumb", url=f"{internal}/api/thumb/{task_id}"),
        _Candidate("site thumb", url=f"{public}/api/thumb/{task_id}"),
        _Candidate(
            "preflight render",
            path=config.PREFLIGHT_RENDER_DIR / f"{task_id}.jpg",
        ),
    ]


def accel_local_path(internal_uri: str) -> Optional[Path]:
    """The file an X-Accel-Redirect uri names on this host, if it is here."""
    uri = str(internal_uri or "").split("?", 1)[0]
    for prefix, root in (
        ("/_autorig_glb_cache/", config.MAIN_GLB_CACHE_DIR),
        ("/_autorig_artifacts/", config.MAIN_ARTIFACT_CACHE_DIR),
    ):
        if not uri.startswith(prefix):
            continue
        base = Path(root).resolve()
        candidate = (base / unquote(uri[len(prefix):])).resolve()
        if base in candidate.parents and candidate.is_file():
            return candidate
        return None
    return None


def is_glb(path: Path) -> bool:
    """A complete binary glTF 2.0: magic, version and the declared length."""
    try:
        size = path.stat().st_size
        with path.open("rb") as stream:
            header = stream.read(12)
    except OSError:
        return False
    return (
        len(header) == 12
        and header[:4] == b"glTF"
        and int.from_bytes(header[4:8], "little") == 2
        and int.from_bytes(header[8:12], "little") == size
    )


def _read_small(path: Path, limit: int) -> Optional[bytes]:
    try:
        if not path.is_file() or path.stat().st_size > limit:
            return None
        return path.read_bytes()
    except OSError:
        return None


def _picture_size(data: bytes) -> Optional[Tuple[int, int]]:
    try:
        with Image.open(io.BytesIO(data)) as picture:
            picture.load()
            return picture.size
    except Exception:
        return None


async def _download(
    client: httpx.AsyncClient,
    url: str,
    *,
    max_bytes: int,
    dest: Optional[Path] = None,
) -> Union[Path, bytes, None]:
    """GET one candidate.

    Returns a local file (dest, or the one an X-Accel-Redirect names), the body
    when no dest is given, or None when this candidate simply does not have it.
    Raises _Transient when the answer says nothing about the task. Status codes
    are reported as "status N": the retry loop revives jobs whose error reads
    like an expired farm token ("HTTP 403"), which this is not.
    """
    try:
        async with client.stream(
            "GET", url, timeout=config.REGEN_FETCH_TIMEOUT_SECONDS
        ) as response:
            accel = response.headers.get("x-accel-redirect")
            if accel:
                local = accel_local_path(accel)
                if local is None or dest is not None:
                    return local
                return _read_small(local, max_bytes)
            if response.status_code >= 500:
                raise _Transient(f"status {response.status_code}")
            if response.status_code != 200:
                return None
            if dest is None:
                chunks: List[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        return None
                    chunks.append(chunk)
                return b"".join(chunks)
            dest.parent.mkdir(parents=True, exist_ok=True)
            part = dest.with_name(f".{dest.name}.{uuid.uuid4().hex}.part")
            total = 0
            try:
                with part.open("wb") as sink:
                    async for chunk in response.aiter_bytes(131072):
                        total += len(chunk)
                        if total > max_bytes:
                            return None
                        sink.write(chunk)
                os.replace(str(part), str(dest))
            finally:
                part.unlink(missing_ok=True)
            return dest
    except httpx.TransportError as exc:
        raise _Transient(type(exc).__name__) from exc


async def fetch_glb(
    client: httpx.AsyncClient, task_id: str, dest: Path
) -> Tuple[Path, str]:
    """The task's model as a file on this host, and where it came from.

    A downloaded model lands at dest; a cached one is returned where it lies
    and must not be deleted by the caller.
    """
    unreachable: List[str] = []
    for candidate in glb_candidates(task_id):
        if candidate.path is not None:
            if is_glb(candidate.path):
                return candidate.path, candidate.label
            continue
        try:
            found = await _download(
                client, candidate.url, max_bytes=config.REGEN_MAX_GLB_BYTES, dest=dest
            )
        except _Transient as exc:
            unreachable.append(f"{candidate.label}: {exc}")
            continue
        if isinstance(found, Path):
            if is_glb(found):
                return found, candidate.label
            if found == dest:
                dest.unlink(missing_ok=True)
    if unreachable:
        raise RegenSourceUnavailable(
            "model unreachable (" + "; ".join(unreachable[:3]) + ")"
        )
    raise RegenSourceError(f"task {task_id} has no model GLB")


async def fetch_poster(client: httpx.AsyncClient, task_id: str) -> Tuple[bytes, str]:
    """The task's poster bytes and where they came from."""
    unreachable: List[str] = []
    for candidate in poster_candidates(task_id):
        if candidate.path is not None:
            data = _read_small(candidate.path, _POSTER_MAX_BYTES)
        else:
            try:
                data = await _download(
                    client, candidate.url, max_bytes=_POSTER_MAX_BYTES
                )
            except _Transient as exc:
                unreachable.append(f"{candidate.label}: {exc}")
                continue
        size = _picture_size(data) if isinstance(data, bytes) else None
        if size and min(size) >= _MIN_POSTER_SIDE:
            return data, candidate.label
    if unreachable:
        raise RegenSourceUnavailable(
            "poster unreachable (" + "; ".join(unreachable[:3]) + ")"
        )
    raise RegenSourceError(f"task {task_id} has neither a model nor a poster")


def _save_png_atomic(picture: Image.Image, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    part = out.with_name(f".{out.name}.{uuid.uuid4().hex}.part")
    try:
        with part.open("wb") as sink:
            picture.save(sink, "PNG")
            sink.flush()
            os.fsync(sink.fileno())
        os.replace(str(part), str(out))
    finally:
        part.unlink(missing_ok=True)


def poster_to_png(data: bytes, out: Path, *, size: int) -> Path:
    """Letterbox a poster onto the square mid-grey canvas a still uses.

    The edit keeps its input's aspect, and a T-pose spans about its own
    height: a square frame is what leaves room for the hands and the feet.
    """
    try:
        with Image.open(io.BytesIO(data)) as source:
            source.load()
            picture = source.convert("RGBA")
    except Exception as exc:
        raise RegenSourceError(f"poster is not a readable picture: {exc}") from exc
    scale = min(size / picture.width, size / picture.height)
    fitted = picture.resize(
        (max(1, round(picture.width * scale)), max(1, round(picture.height * scale))),
        Image.Resampling.LANCZOS,
    )
    canvas = Image.new("RGB", (size, size), _BACKDROP)
    canvas.paste(
        fitted,
        ((size - fitted.width) // 2, (size - fitted.height) // 2),
        fitted,
    )
    _save_png_atomic(canvas, out)
    return out


def check_still(path: Path) -> None:
    """A still that is readable, big enough and not one flat colour.

    A model the renderer failed to show still yields a valid PNG - of the
    empty backdrop - and editing an empty backdrop costs a GPU run to learn
    nothing.
    """
    try:
        with Image.open(path) as picture:
            picture.load()
            size = picture.size
            low, high = picture.convert("L").getextrema()
    except Exception as exc:
        raise RegenSourceError(f"still render is not a readable picture: {exc}") from exc
    if min(size) < _MIN_STILL_SIDE:
        raise RegenSourceError(f"still render is too small: {size[0]}x{size[1]}")
    if high - low < 8:
        raise RegenSourceError("still render is blank: the model did not show up")

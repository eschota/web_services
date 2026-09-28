"""Deterministic, local-only input normalization before worker dispatch."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import struct
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

from config import APP_URL, NEW_TASK_MIN_FREE_GB, UPLOAD_DIR


MAX_INPUT_BYTES = 512 * 1024 * 1024
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_DECODED_BYTES = 512 * 1024 * 1024
KTX2_MAGIC = bytes.fromhex("ab4b5458203230bb0d0a1a0a")
NODE_EXE = os.getenv("MESHOPT_NODE_EXE", "/usr/bin/node")
KTX_EXE = os.getenv(
    "KTX_EXE", "/srv/autorig/tools/KTX-Software-4.4.2-Linux-x86_64/bin/ktx"
)
DECODER_SCRIPT = Path(__file__).resolve().parent / "scripts" / "decode_meshopt_glb.mjs"
_NORMALIZATION_LOCK = threading.Lock()


class InputNormalizationError(RuntimeError):
    def __init__(self, code: str, detail: str):
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(f"INPUT_ERROR[{self.code}]: {self.detail}")


class InputNormalizationDeferred(RuntimeError):
    """Retryable infrastructure/capacity condition; never consumes input retries."""

    def __init__(self, code: str, detail: str):
        self.code = str(code)
        self.detail = str(detail)
        super().__init__(f"input normalization capacity wait[{self.code}]: {self.detail}")


@dataclass(frozen=True)
class NormalizedInput:
    effective_url: str
    changed: bool
    record: dict


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _glb_document(path: Path) -> tuple[dict, int, int]:
    size = path.stat().st_size
    if size < 20 or size > MAX_INPUT_BYTES:
        raise InputNormalizationError("GLB_SIZE", "GLB size is outside policy")
    with path.open("rb") as stream:
        header = stream.read(20)
        if len(header) != 20 or header[:4] != b"glTF":
            raise InputNormalizationError("GLB_HEADER", "input is not a GLB 2.0 file")
        version, declared, json_length, json_type = struct.unpack_from("<IIII", header, 4)
        if version != 2 or declared != size or json_type != 0x4E4F534A:
            raise InputNormalizationError("GLB_HEADER", "invalid GLB header or JSON chunk")
        if json_length <= 0 or json_length > MAX_JSON_BYTES or 20 + json_length > size:
            raise InputNormalizationError("GLB_JSON", "GLB JSON chunk is outside policy")
        document = json.loads(stream.read(json_length).decode("utf-8").rstrip("\x00 "))
    if not isinstance(document, dict):
        raise InputNormalizationError("GLB_JSON", "GLB JSON root is not an object")
    decoded_bytes = 0
    meshopt_views = 0
    for view in document.get("bufferViews") or []:
        extension = (view.get("extensions") or {}).get("EXT_meshopt_compression") if isinstance(view, dict) else None
        if not isinstance(extension, dict):
            continue
        try:
            count = int(extension["count"])
            stride = int(extension["byteStride"])
            encoded_length = int(extension["byteLength"])
        except (KeyError, TypeError, ValueError) as exc:
            raise InputNormalizationError("MESHOPT_SCHEMA", "invalid meshopt bufferView") from exc
        mode = str(extension.get("mode") or "")
        if count <= 0 or stride <= 0 or encoded_length <= 0 or mode not in {
            "ATTRIBUTES", "TRIANGLES", "INDICES"
        }:
            raise InputNormalizationError("MESHOPT_SCHEMA", "invalid meshopt count/stride/mode")
        decoded_bytes += count * stride
        meshopt_views += 1
        if decoded_bytes > MAX_DECODED_BYTES:
            raise InputNormalizationError("MESHOPT_SIZE", "decoded meshopt buffers exceed policy")
    return document, meshopt_views, decoded_bytes


def _basis_image_sources(document: dict) -> set[int]:
    images = document.get("images") or []
    sources: set[int] = {
        index
        for index, image in enumerate(images)
        if isinstance(image, dict)
        and str(image.get("mimeType") or "").lower() == "image/ktx2"
    }
    for texture in document.get("textures") or []:
        extension = (
            (texture.get("extensions") or {}).get("KHR_texture_basisu")
            if isinstance(texture, dict)
            else None
        )
        if not isinstance(extension, dict):
            continue
        try:
            source = int(extension["source"])
        except (KeyError, TypeError, ValueError) as exc:
            raise InputNormalizationError(
                "KTX2_SCHEMA", "invalid KHR_texture_basisu source"
            ) from exc
        if source < 0 or source >= len(images):
            raise InputNormalizationError(
                "KTX2_SCHEMA", "KHR_texture_basisu source is outside images"
            )
        sources.add(source)
    return sources


def _basis_image_count(document: dict) -> int:
    return len(_basis_image_sources(document))


def _basis_decoded_bytes(path: Path, document: dict) -> int:
    sources = _basis_image_sources(document)
    if not sources:
        return 0
    views = document.get("bufferViews") or []
    images = document.get("images") or []
    with path.open("rb") as stream:
        stream.seek(12)
        json_length, json_type = struct.unpack("<II", stream.read(8))
        if json_type != 0x4E4F534A:
            raise InputNormalizationError("GLB_JSON", "GLB JSON chunk is missing")
        stream.seek(20 + json_length)
        binary_length, binary_type = struct.unpack("<II", stream.read(8))
        if binary_type != 0x004E4942:
            raise InputNormalizationError("GLB_BIN", "GLB BIN chunk is missing")
        binary_start = 28 + json_length
        total = 0
        for source in sources:
            image = images[source]
            try:
                view = views[int(image["bufferView"])]
                if int(view.get("buffer", 0)) != 0:
                    raise ValueError
                offset = int(view.get("byteOffset", 0))
                length = int(view["byteLength"])
            except (IndexError, KeyError, TypeError, ValueError) as exc:
                raise InputNormalizationError(
                    "KTX2_SCHEMA", "invalid KTX2 image bufferView"
                ) from exc
            if offset < 0 or length < 68 or offset + length > binary_length:
                raise InputNormalizationError(
                    "KTX2_SCHEMA", "KTX2 image bytes are outside the BIN chunk"
                )
            stream.seek(binary_start + offset)
            header = stream.read(68)
            if len(header) != 68 or header[:12] != KTX2_MAGIC:
                raise InputNormalizationError("KTX2_SCHEMA", "image is not KTX2")
            width, height, depth, layers, faces = struct.unpack_from(
                "<IIIII", header, 20
            )
            pixels = int(width) * int(height)
            if (
                width <= 0
                or height <= 0
                or width > 16384
                or height > 16384
                or depth > 1
                or layers > 1
                or faces != 1
                or pixels > 64 * 1024 * 1024
            ):
                raise InputNormalizationError(
                    "KTX2_SIZE", "KTX2 dimensions are outside policy"
                )
            total += pixels * 4
            if total > MAX_DECODED_BYTES:
                raise InputNormalizationError(
                    "KTX2_SIZE", "decoded KTX2 textures exceed policy"
                )
    return total


def _local_upload(input_url: str) -> tuple[Path, str, str] | None:
    value = str(input_url or "").strip()
    parsed = urlparse(value)
    app = urlparse(APP_URL)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != app.netloc.lower():
        return None
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if len(parts) != 3 or parts[0] != "u":
        return None
    token, filename = parts[1], parts[2]
    try:
        if str(uuid.UUID(token)) != token.lower():
            raise ValueError
    except ValueError as exc:
        raise InputNormalizationError("UPLOAD_PATH", "invalid upload token") from exc
    if Path(filename).name != filename or any(separator in filename for separator in ("/", "\\")):
        raise InputNormalizationError("UPLOAD_PATH", "unsafe upload filename")
    upload_root = Path(UPLOAD_DIR).resolve()
    directory = (upload_root / token).resolve()
    source = (directory / filename).resolve()
    if directory.parent != upload_root or source.parent != directory or not source.is_file():
        raise InputNormalizationError("UPLOAD_PATH", "local upload file is missing")
    return source, token, filename


def _atomic_json(path: Path, value: dict) -> None:
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
        _fsync_directory(path.parent)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry on POSIX; Windows has no equivalent handle."""
    if os.name != "posix":
        return
    directory = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


@contextmanager
def _source_lock(lock_path: Path):
    """Serialize one decoder per backend process and across POSIX processes."""
    with _NORMALIZATION_LOCK:
        lock_stream = lock_path.open("a+", encoding="utf-8")
        fcntl_module = None
        try:
            if os.name == "posix":
                import fcntl as fcntl_module

                fcntl_module.flock(lock_stream.fileno(), fcntl_module.LOCK_EX)
            yield
        finally:
            if fcntl_module is not None:
                fcntl_module.flock(lock_stream.fileno(), fcntl_module.LOCK_UN)
            lock_stream.close()


def normalize_local_meshopt(input_url: str) -> NormalizedInput:
    resolved = _local_upload(input_url)
    if resolved is None:
        return NormalizedInput(input_url, False, {})
    source, token, filename = resolved
    if source.suffix.lower() != ".glb":
        return NormalizedInput(input_url, False, {})
    document, meshopt_views, decoded_bytes = _glb_document(source)
    basis_images = _basis_image_count(document)
    basis_decoded_bytes = _basis_decoded_bytes(source, document)
    if meshopt_views == 0 and basis_images == 0:
        return NormalizedInput(input_url, False, {})

    original_sha = _sha256(source)
    derived_name = f"{source.stem}.normalized-{original_sha[:12]}.glb"
    derived = source.with_name(derived_name)
    manifest = derived.with_suffix(derived.suffix + ".normalization.json")
    lock_path = source.with_suffix(source.suffix + ".meshopt.lock")
    with _source_lock(lock_path):
        if derived.is_file() and manifest.is_file():
            try:
                record = json.loads(manifest.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                record = {}
            if (
                record.get("schema") == "autorig.input_normalization.v1"
                and record.get("original_sha256") == original_sha
                and record.get("derived_sha256") == _sha256(derived)
            ):
                return NormalizedInput(
                    f"{APP_URL.rstrip('/')}/u/{token}/{quote(derived_name)}",
                    True,
                    record,
                )

        estimated_output = (
            source.stat().st_size
            + decoded_bytes
            + basis_decoded_bytes
            + MAX_JSON_BYTES
        )
        free = shutil.disk_usage(source.parent).free
        reserve = int(float(NEW_TASK_MIN_FREE_GB) * 1024**3)
        if free - estimated_output < reserve:
            raise InputNormalizationDeferred(
                "DISK_RESERVE",
                f"meshopt decode would cross the {NEW_TASK_MIN_FREE_GB:.0f} GB reserve",
            )
        if not Path(NODE_EXE).is_file() or not DECODER_SCRIPT.is_file():
            raise InputNormalizationDeferred(
                "MESHOPT_RUNTIME", "meshopt decoder runtime is unavailable"
            )
        if basis_images and not Path(KTX_EXE).is_file():
            raise InputNormalizationDeferred(
                "KTX2_RUNTIME", "KTX2 decoder runtime is unavailable"
            )

        descriptor, temp_name = tempfile.mkstemp(
            prefix=f".{derived.name}.", suffix=".tmp", dir=source.parent
        )
        os.close(descriptor)
        os.unlink(temp_name)
        temp_path = Path(temp_name)
        try:
            try:
                completed = subprocess.run(
                    [NODE_EXE, str(DECODER_SCRIPT), str(source), str(temp_path)],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=360,
                    check=False,
                    env={**os.environ, "KTX_EXE": KTX_EXE},
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise InputNormalizationDeferred(
                    "MESHOPT_RUNTIME", f"decoder unavailable: {exc}"
                ) from exc
            if completed.returncode != 0 or not temp_path.is_file():
                detail = (completed.stderr or completed.stdout or "decoder failed").strip()
                raise InputNormalizationError("NORMALIZATION_DECODE", detail[-1000:])
            report = json.loads((completed.stdout or "").strip().splitlines()[-1])
            decoded_document, remaining_views, _decoded_size = _glb_document(temp_path)
            remaining_basis = _basis_image_count(decoded_document)
            if remaining_views or remaining_basis:
                raise InputNormalizationError(
                    "NORMALIZATION_INCOMPLETE",
                    "derived GLB still uses meshopt or KTX2 textures",
                )
            derived_sha = _sha256(temp_path)
            if report.get("input_sha256") != original_sha or report.get("output_sha256") != derived_sha:
                raise InputNormalizationError("MESHOPT_RECEIPT", "decoder checksum mismatch")
            if os.name == "posix":
                with temp_path.open("rb") as stream:
                    os.fsync(stream.fileno())
            os.replace(temp_path, derived)
            _fsync_directory(source.parent)
            record = {
                "schema": "autorig.input_normalization.v1",
                "type": "meshopt_decode",
                "original_url": input_url,
                "original_sha256": original_sha,
                "derived_url": f"{APP_URL.rstrip('/')}/u/{token}/{quote(derived_name)}",
                "derived_sha256": derived_sha,
                "decoder_version": report.get("decoder_version"),
                "decoded_views": int(report.get("decoded_views") or 0),
                "decoded_textures": int(report.get("decoded_textures") or 0),
                "texture_decoder_version": report.get("texture_decoder_version"),
                "original_bytes": source.stat().st_size,
                "derived_bytes": derived.stat().st_size,
            }
            _atomic_json(manifest, record)
            return NormalizedInput(record["derived_url"], True, record)
        finally:
            if temp_path.exists():
                temp_path.unlink()


# ---------------------------------------------------------------------------
# Remote GLB mirroring: external .glb links bypass meshopt/KTX2 normalization
# because it only understands local /u/ uploads. Mirror such sources into a
# deterministic local upload (uuid5 of the source URL) so the exact same
# normalization path applies, and the worker downloads from this host instead
# of a possibly-flaky external origin. Fail-open: any fetch problem keeps the
# original URL and the pre-mirror behavior.
# ---------------------------------------------------------------------------

MIRROR_TIMEOUT_SECONDS = float(os.getenv("INPUT_MIRROR_TIMEOUT_SECONDS", "300"))
_GLB_MAGIC = b"glTF"


def _mirror_remote_glb(input_url: str) -> NormalizedInput | None:
    value = str(input_url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return None
    app = urlparse(APP_URL)
    if parsed.netloc.lower() == app.netloc.lower():
        return None
    filename = Path(unquote(parsed.path)).name
    if not filename.lower().endswith(".glb"):
        return None
    if any(sep in filename for sep in ("/", "\\")) or filename.startswith("."):
        filename = "model.glb"

    token = str(uuid.uuid5(uuid.NAMESPACE_URL, value))
    upload_root = Path(UPLOAD_DIR).resolve()
    directory = upload_root / token
    target = directory / filename
    manifest = directory / f"{filename}.mirror.json"
    local_url = f"{APP_URL.rstrip('/')}/u/{token}/{quote(filename)}"

    if target.is_file() and manifest.is_file():
        try:
            record = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            record = {}
        if (
            record.get("schema") == "autorig.input_mirror.v1"
            and int(record.get("bytes") or 0) == target.stat().st_size
        ):
            return NormalizedInput(local_url, True, record)

    free = shutil.disk_usage(upload_root).free
    reserve = int(float(NEW_TASK_MIN_FREE_GB) * 1024**3)
    if free - MAX_INPUT_BYTES < reserve:
        raise InputNormalizationDeferred(
            "DISK_RESERVE",
            f"remote mirror would cross the {NEW_TASK_MIN_FREE_GB:.0f} GB reserve",
        )

    try:
        import httpx

        directory.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(
            prefix=f".{filename}.", suffix=".tmp", dir=directory
        )
        temp_path = Path(temp_name)
        received = 0
        digest = hashlib.sha256()
        try:
            with os.fdopen(descriptor, "wb") as stream, httpx.Client(
                follow_redirects=True, timeout=httpx.Timeout(30.0, read=60.0)
            ) as client:
                started = time.monotonic()
                with client.stream("GET", value) as response:
                    if response.status_code != 200:
                        raise OSError(f"source returned HTTP {response.status_code}")
                    head = b""
                    for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                        if len(head) < 4:
                            head += chunk[: 4 - len(head)]
                            if len(head) >= 4 and head[:4] != _GLB_MAGIC:
                                raise OSError("source is not a binary glTF file")
                        received += len(chunk)
                        if received > MAX_INPUT_BYTES:
                            raise OSError("source exceeds mirror size limit")
                        if time.monotonic() - started > MIRROR_TIMEOUT_SECONDS:
                            raise OSError("mirror download timed out")
                        digest.update(chunk)
                        stream.write(chunk)
            if received < 12:
                raise OSError("source returned a truncated GLB")
            os.replace(temp_path, target)
        finally:
            if temp_path.exists():
                temp_path.unlink()
    except InputNormalizationDeferred:
        raise
    except Exception as exc:
        print(f"[InputMirror] fail-open for {value}: {exc}")
        return None

    record = {
        "schema": "autorig.input_mirror.v1",
        "type": "remote_mirror",
        "original_url": value,
        "derived_url": local_url,
        "sha256": digest.hexdigest(),
        "bytes": received,
    }
    try:
        _atomic_json(manifest, record)
    except OSError as exc:
        print(f"[InputMirror] manifest write failed for {value}: {exc}")
    return NormalizedInput(local_url, True, record)


def normalize_task_input(input_url: str) -> NormalizedInput:
    """Mirror remote GLB links locally, then apply meshopt/KTX2 normalization.

    The returned record is the innermost transformation; a mirror-only result
    carries the mirror record so provenance survives in viewer_settings.
    """
    mirrored = _mirror_remote_glb(input_url)
    effective = mirrored.effective_url if mirrored is not None else input_url
    normalized = normalize_local_meshopt(effective)
    if normalized.changed:
        if mirrored is not None:
            normalized.record.setdefault("mirrored_from", input_url)
        return normalized
    if mirrored is not None:
        return mirrored
    return normalized

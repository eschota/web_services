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
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, unquote, urlparse

from config import APP_URL, NEW_TASK_MIN_FREE_GB, UPLOAD_DIR


MAX_INPUT_BYTES = 512 * 1024 * 1024
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_DECODED_BYTES = 512 * 1024 * 1024
NODE_EXE = os.getenv("MESHOPT_NODE_EXE", "/usr/bin/node")
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
    _document, meshopt_views, decoded_bytes = _glb_document(source)
    if meshopt_views == 0:
        return NormalizedInput(input_url, False, {})

    original_sha = _sha256(source)
    derived_name = f"{source.stem}.meshopt-decoded-{original_sha[:12]}.glb"
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

        estimated_output = source.stat().st_size + decoded_bytes + MAX_JSON_BYTES
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
                    timeout=180,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise InputNormalizationDeferred(
                    "MESHOPT_RUNTIME", f"decoder unavailable: {exc}"
                ) from exc
            if completed.returncode != 0 or not temp_path.is_file():
                detail = (completed.stderr or completed.stdout or "decoder failed").strip()
                raise InputNormalizationError("MESHOPT_DECODE", detail[-1000:])
            report = json.loads((completed.stdout or "").strip().splitlines()[-1])
            _decoded_document, remaining_views, _decoded_size = _glb_document(temp_path)
            if remaining_views:
                raise InputNormalizationError("MESHOPT_DECODE", "derived GLB still uses meshopt")
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
                "original_bytes": source.stat().st_size,
                "derived_bytes": derived.stat().st_size,
            }
            _atomic_json(manifest, record)
            return NormalizedInput(record["derived_url"], True, record)
        finally:
            if temp_path.exists():
                temp_path.unlink()

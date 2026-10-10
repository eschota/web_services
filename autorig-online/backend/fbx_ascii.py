"""ASCII FBX at intake -> GLB on the VPS (owner, 2026-10-10: «срочно правь»).

The converters reject ASCII FBX («INPUT_ERROR[FBX_ASCII_UNSUPPORTED]») and Blender
cannot import it at all, so an uploaded ASCII FBX is turned into a GLB here,
before the task is created, for both the legacy and the V3 admission:

1. Exporter repair.  Some exporters (AssetStudio, «Unity Studio by Chipicao»)
   omit the ``a:`` key on an array's first data line; every FBX parser fails on
   ``Name: *N {`` followed by bare numbers.  The key is inserted.
2. ``assimp export fixed.fbx out.glb -f glb2``.  Only the GLB exporter is used:
   assimp's binary-FBX writer produces files neither Blender nor assimp can read.

The original upload is never modified; the GLB is written beside it.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict

ASSIMP = os.getenv("ASSIMP_EXE", "/usr/bin/assimp")
TIMEOUT_S = float(os.getenv("AUTORIG_FBX_ASCII_TIMEOUT", "600"))
_ARRAY_HEAD = re.compile(r"^\s*[A-Za-z_]\w*: \*\d+ \{\s*$")


class FbxAsciiError(RuntimeError):
    """The ASCII FBX could not become a GLB; ``code`` is stable, the text is for logs only."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail = code, detail


def is_ascii_fbx(path: Path) -> bool:
    with Path(path).open("rb") as stream:
        head = stream.read(4096)
    head = head[3:] if head.startswith(b"\xef\xbb\xbf") else head
    return head.lstrip().startswith(b"; FBX")


def repair_ascii_fbx(text: str) -> tuple[str, int]:
    """Insert the missing ``a:`` key after array headers; returns (text, insertions)."""
    lines = text.splitlines()
    out, inserted, i = [], 0, 0
    while i < len(lines):
        out.append(lines[i])
        if _ARRAY_HEAD.match(lines[i]) and i + 1 < len(lines):
            nxt = lines[i + 1]
            body = nxt.lstrip()
            if body and not body.startswith("a:") and not body.startswith("}"):
                out.append(nxt[:len(nxt) - len(body)] + "a: " + body)
                inserted += 1
                i += 2
                continue
        i += 1
    return "\n".join(out) + "\n", inserted


def _glb_ok(path: Path) -> Dict[str, Any]:
    data = Path(path).read_bytes()
    if len(data) < 20 or data[:4] != b"glTF" or struct.unpack_from("<I", data, 4)[0] != 2 or \
            struct.unpack_from("<I", data, 8)[0] != len(data):
        raise FbxAsciiError("glb_invalid", "assimp did not write a complete GLB 2.0")
    json_len = struct.unpack_from("<I", data, 12)[0]
    doc = json.loads(data[20:20 + json_len].decode("utf-8").rstrip(" \t\r\n\x00"))
    if not any(mesh.get("primitives") for mesh in doc.get("meshes") or []):
        raise FbxAsciiError("glb_empty", "the converted GLB has no mesh")
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
            "meshes": len(doc.get("meshes") or []), "skins": len(doc.get("skins") or []),
            "animations": len(doc.get("animations") or []), "materials": len(doc.get("materials") or [])}


def ascii_fbx_to_glb(src: Path, dst: Path) -> Dict[str, Any]:
    """Repair + convert one ASCII FBX; writes ``dst`` atomically and returns a receipt."""
    if not is_ascii_fbx(src):
        raise FbxAsciiError("not_ascii_fbx", "the file is not an ASCII FBX")
    return fbx_to_glb(src, dst)


def fbx_to_glb(src: Path, dst: Path) -> Dict[str, Any]:
    """Any FBX -> GLB with assimp (ASCII first repaired); used directly by the V3 intake."""
    src, dst = Path(src), Path(dst)
    if not shutil.which(ASSIMP) and not Path(ASSIMP).is_file():
        raise FbxAsciiError("assimp_missing", f"{ASSIMP} is not installed")
    ascii_input, inserted = is_ascii_fbx(src), 0
    with tempfile.TemporaryDirectory(prefix="fbx-ascii-") as work:
        mid, out = Path(work) / "fixed.fbx", Path(work) / "out.glb"
        if ascii_input:
            text = src.read_bytes().decode("utf-8", "replace")
            if text.startswith("\ufeff"):
                text = text[1:]
            fixed, inserted = repair_ascii_fbx(text)
            mid.write_text(fixed, encoding="utf-8")
        else:
            shutil.copyfile(src, mid)
        try:
            run = subprocess.run([ASSIMP, "export", str(mid), str(out), "-f", "glb2"], capture_output=True,
                                 text=True, timeout=TIMEOUT_S)
        except subprocess.TimeoutExpired as exc:
            raise FbxAsciiError("assimp_timeout", f"no GLB within {int(TIMEOUT_S)} s") from exc
        if run.returncode or not out.is_file():
            raise FbxAsciiError("assimp_failed", (run.stderr or run.stdout or "")[-400:])
        facts = _glb_ok(out)
        tmp = dst.with_name(f".{dst.name}.tmp")
        shutil.copyfile(out, tmp)
        os.chmod(tmp, 0o644)
        os.replace(tmp, dst)
    return {"schema": "autorig.fbx-ascii-normalization/1",
            "method": "fbx_ascii_repair+assimp glb2" if ascii_input else "assimp glb2",
            "source_sha256": hashlib.sha256(src.read_bytes()).hexdigest(), "inserted_a_keys": inserted,
            "glb": dst.name, **facts}

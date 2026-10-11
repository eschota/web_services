"""Texturing · V3 at intake (owner 2026-10-11, «Every Model Gets Textures»).

* A model's own textures are never lost on the way into V3:
  - OBJ: its .mtl is read (Kd / d / Ns, map_Kd, map_Bump / bump / norm, map_Pr, map_Pm, map_Ke, map_d) and every
    texture found beside it is embedded into the GLB (PNG / JPEG as they are; TGA, BMP, TIFF, DDS, WebP ... -> PNG);
  - FBX / glTF converted by assimp: images left as external file references are embedded from the upload folder;
  - a ZIP upload is unpacked safely (no paths outside, size and count limits) and its model picked.
* Missing files are detected and named: the task page and the session agent ask the user for them.
  The user adds them (or a ZIP) to the same task; the source is normalised again with them and the task goes into a
  new attempt.
* Broken vertex normals (an export where all normals point one way, e.g. Roblox OBJ: every vn = 0 1 0) are rebuilt
  as smooth area-weighted normals; the viewer showed such models flat white.

The audit, ``textures``: {status: source_ok | missing_files | none, referenced, missing, embedded, normals}.
"""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import struct
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from fastapi import Request  # module level: the routes' annotations are resolved here (postponed annotations)

IMAGE_EXT = (".png", ".jpg", ".jpeg", ".tga", ".bmp", ".tif", ".tiff", ".dds", ".webp", ".gif", ".psd", ".exr",
             ".hdr", ".ktx", ".ktx2")
MODEL_EXT = (".glb", ".gltf", ".fbx", ".obj")
ZIP_MAX_FILES = 400
ZIP_MAX_BYTES = 1024 * 1024 * 1024
ADD_MAX_BYTES = 512 * 1024 * 1024


# --------------------------------------------------------------------------------------------- GLB helpers
def _read_glb(data: bytes):
    if data[:4] != b"glTF":
        raise ValueError("not a GLB")
    off, doc, bin_ = 12, None, b""
    total = struct.unpack_from("<I", data, 8)[0]
    while off < min(total, len(data)):
        n, kind = struct.unpack_from("<II", data, off)
        chunk = data[off + 8: off + 8 + n]
        if kind == 0x4E4F534A:
            doc = json.loads(chunk.decode("utf-8"))
        elif kind == 0x004E4942:
            bin_ = bytes(chunk)
        off += 8 + n
    if doc is None:
        raise ValueError("GLB without JSON")
    return doc, bytearray(bin_)


def _write_glb(doc: dict, bin_: bytearray) -> bytes:
    bin_ = bytes(bin_) + b"\0" * (-len(bin_) % 4)
    if doc.get("buffers"):
        doc["buffers"][0]["byteLength"] = len(bin_)
        doc["buffers"][0].pop("uri", None)
    elif bin_:
        doc["buffers"] = [{"byteLength": len(bin_)}]
    js = json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    js += b" " * (-len(js) % 4)
    body = struct.pack("<II", len(js), 0x4E4F534A) + js
    if bin_:
        body += struct.pack("<II", len(bin_), 0x004E4942) + bin_
    return b"glTF" + struct.pack("<II", 2, 12 + len(body)) + body


def _add_view(doc: dict, bin_: bytearray, raw: bytes, target: Optional[int] = None) -> int:
    bin_.extend(b"\0" * (-len(bin_) % 4))
    view = {"buffer": 0, "byteOffset": len(bin_), "byteLength": len(raw)}
    if target:
        view["target"] = target
    bin_.extend(raw)
    doc.setdefault("bufferViews", []).append(view)
    if not doc.get("buffers"):
        doc["buffers"] = [{"byteLength": 0}]
    return len(doc["bufferViews"]) - 1


def image_bytes(path: Path) -> Tuple[bytes, str]:
    """PNG / JPEG as they are; anything PIL can read -> PNG (8 bit; 16-bit greyscale scaled, not clipped)."""
    raw = path.read_bytes()
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return raw, "image/png"
    if raw[:3] == b"\xff\xd8\xff":
        return raw, "image/jpeg"
    from PIL import Image
    im = Image.open(io.BytesIO(raw))
    im.load()
    if im.mode in ("I;16", "I;16B", "I;16L", "I"):
        im = im.point(lambda v: v / 256).convert("L")
    elif im.mode == "F":
        im = im.point(lambda v: v * 255).convert("L")
    if im.mode not in ("RGB", "RGBA", "L", "LA"):
        im = im.convert("RGBA" if "A" in im.getbands() or "transparency" in im.info else "RGB")
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue(), "image/png"


def _add_texture(doc: dict, bin_: bytearray, path: Path, cache: dict) -> Optional[int]:
    key = str(path.resolve())
    if key in cache:
        return cache[key]
    try:
        raw, mime = image_bytes(path)
    except Exception as exc:                                   # noqa: BLE001 - an unreadable image is reported
        print(f"[V3 textures] {path.name}: unreadable ({exc})")
        cache[key] = None
        return None
    doc.setdefault("images", []).append({"bufferView": _add_view(doc, bin_, raw), "mimeType": mime,
                                         "name": path.stem[:80]})
    samplers = doc.setdefault("samplers", [])
    if not samplers:
        samplers.append({"magFilter": 9729, "minFilter": 9987, "wrapS": 10497, "wrapT": 10497})
    doc.setdefault("textures", []).append({"source": len(doc["images"]) - 1, "sampler": 0})
    cache[key] = len(doc["textures"]) - 1
    return cache[key]


def find_file(name: str, base: Path) -> Optional[Path]:
    """A referenced file by its base name (case-insensitive) in the upload folder or below it (an unpacked ZIP)."""
    want = Path(str(name).replace("\\", "/")).name.lower().strip().strip('"')
    if not want:
        return None
    direct = base / want
    if direct.is_file():
        return direct
    for p in sorted(base.rglob("*")):
        if p.is_file() and p.name.lower() == want:
            return p
    stem = Path(want).stem                                    # tex.tga referenced, tex.png shipped
    for p in sorted(base.rglob("*")):
        if p.is_file() and p.stem.lower() == stem and p.suffix.lower() in IMAGE_EXT:
            return p
    return None


# --------------------------------------------------------------------------------------------- OBJ + MTL
def parse_mtl(text: str) -> Dict[str, Dict[str, Any]]:
    mats: Dict[str, Dict[str, Any]] = {}
    cur: Optional[Dict[str, Any]] = None
    for raw in text.splitlines():
        parts = raw.strip().split()
        if not parts or parts[0].startswith("#"):
            continue
        key = parts[0]
        if key == "newmtl":
            cur = mats.setdefault(" ".join(parts[1:])[:120], {})
            continue
        if cur is None:
            continue
        low = key.lower()
        if low in ("kd", "ke", "ks") and len(parts) >= 4:
            try:
                cur[low] = [float(x) for x in parts[1:4]]
            except ValueError:
                pass
        elif low in ("d", "tr", "ns", "pr", "pm") and len(parts) >= 2:
            try:
                cur[low] = float(parts[1])
            except ValueError:
                pass
        elif low.startswith("map_") or low in ("bump", "norm", "disp", "refl"):
            # the file is the last token; options (-bm 1, -s ..) come before it
            cur[low] = parts[-1]
    return mats


def obj_texture_refs(obj_raw: bytes, base: Path) -> Dict[str, Any]:
    libs = [x.decode("utf-8", "replace").strip() for x in re.findall(rb"(?m)^\s*mtllib\s+(.+?)\s*$", obj_raw)]
    usemtl = sorted({x.decode("utf-8", "replace").strip()[:120]
                     for x in re.findall(rb"(?m)^\s*usemtl\s+(.+?)\s*$", obj_raw)})
    referenced, missing, mats = [], [], {}
    for lib in libs:
        referenced.append(Path(lib.replace("\\", "/")).name)
        p = find_file(lib, base)
        if p is None:
            missing.append(Path(lib.replace("\\", "/")).name)
            continue
        for name, m in parse_mtl(p.read_text(encoding="utf-8", errors="replace")).items():
            mats[name] = m
            for k, v in m.items():
                if isinstance(v, str):
                    referenced.append(Path(v.replace("\\", "/")).name)
                    if find_file(v, base) is None:
                        missing.append(Path(v.replace("\\", "/")).name)
    return {"mtllib": libs, "usemtl": usemtl, "materials": mats,
            "referenced": sorted(set(referenced)), "missing": sorted(set(missing))}


def apply_mtl(glb: bytes, refs: Dict[str, Any], base: Path) -> Tuple[bytes, List[str]]:
    """The OBJ's materials onto the obj_to_glb GLB (materials are named by usemtl)."""
    doc, bin_ = _read_glb(glb)
    cache: dict = {}
    embedded: List[str] = []
    for mat in doc.get("materials") or []:
        m = refs["materials"].get(mat.get("name"))
        if not m:
            continue
        pbr = mat.setdefault("pbrMetallicRoughness", {})
        alpha = m.get("d", 1.0 - m.get("tr", 0.0)) if ("d" in m or "tr" in m) else 1.0
        if "kd" in m:
            pbr["baseColorFactor"] = [max(0.0, min(1.0, c)) for c in m["kd"]] + [max(0.0, min(1.0, alpha))]
        if "pr" in m:
            pbr["roughnessFactor"] = max(0.0, min(1.0, m["pr"]))
        elif "ns" in m:
            pbr["roughnessFactor"] = round(max(0.05, min(1.0, (2.0 / (m["ns"] + 2.0)) ** 0.25)), 3)
        if "pm" in m:
            pbr["metallicFactor"] = max(0.0, min(1.0, m["pm"]))

        def tex(key):
            name = m.get(key)
            p = find_file(name, base) if name else None
            ti = _add_texture(doc, bin_, p, cache) if p else None
            if ti is not None:
                embedded.append(p.name)
            return ti
        ti = tex("map_kd")
        if ti is not None:
            pbr["baseColorTexture"] = {"index": ti}
            if "kd" in m and max(m["kd"]) < 0.05:          # Kd 0 with a map: the map is the colour
                pbr["baseColorFactor"] = [1.0, 1.0, 1.0, pbr.get("baseColorFactor", [1, 1, 1, 1])[3]]
        for key in ("norm", "map_bump", "bump"):
            ti = tex(key)
            if ti is not None:
                mat["normalTexture"] = {"index": ti}
                break
        ti = tex("map_ke")
        if ti is not None:
            mat["emissiveTexture"] = {"index": ti}
            mat["emissiveFactor"] = [1.0, 1.0, 1.0]
        if alpha < 0.999 or m.get("map_d"):
            mat["alphaMode"] = "BLEND"
    return _write_glb(doc, bin_), sorted(set(embedded))


# --------------------------------------------------------------------------------------------- normals
def fix_normals(glb: bytes) -> Tuple[bytes, Dict[str, Any]]:
    """Rebuild NORMAL when most file normals disagree with the geometry by more than 60° (either winding)."""
    import numpy as np

    doc, bin_ = _read_glb(glb)
    report = {"state": "ok", "bad_share": 0.0, "rebuilt": 0}
    acc_cache: Dict[int, Any] = {}
    buf = bytes(bin_)

    def read(i):
        if i in acc_cache:
            return acc_cache[i]
        a = doc["accessors"][i]
        v = doc["bufferViews"][a["bufferView"]]
        n = {"SCALAR": 1, "VEC2": 2, "VEC3": 3, "VEC4": 4}[a["type"]]
        dt = {5126: np.float32, 5125: np.uint32, 5123: np.uint16, 5121: np.uint8}[a["componentType"]]
        stride = v.get("byteStride") or np.dtype(dt).itemsize * n
        off = v.get("byteOffset", 0) + a.get("byteOffset", 0)
        arr = np.ndarray((a["count"], n), dt, buffer=buf, offset=off, strides=(stride, np.dtype(dt).itemsize))
        acc_cache[i] = arr.copy()
        return acc_cache[i]
    groups: Dict[Tuple[int, int], List[Any]] = {}
    for mesh in doc.get("meshes") or []:
        for prim in mesh.get("primitives") or []:
            at = prim.get("attributes") or {}
            if prim.get("mode", 4) != 4 or "NORMAL" not in at or "POSITION" not in at:
                continue
            groups.setdefault((at["POSITION"], at["NORMAL"]), []).append(prim)
    bad_all = n_all = 0
    plans = []
    for (pi, ni), prims in groups.items():
        try:
            P = read(pi).astype(np.float64)
            N = read(ni).astype(np.float64)
            tris = []
            for prim in prims:
                idx = read(prim["indices"]).reshape(-1).astype(np.int64) if "indices" in prim else \
                    np.arange(len(P), dtype=np.int64)
                tris.append(idx[: len(idx) // 3 * 3].reshape(-1, 3))
            F = np.concatenate(tris)
        except Exception:                                      # noqa: BLE001 - sparse / odd: leave it
            continue
        lo, hi = P.min(0), P.max(0)
        q = np.round((P - lo) / (max(float(np.max(hi - lo)), 1e-9) * 2e-6)).astype(np.int64)
        _, weld = np.unique(q, axis=0, return_inverse=True)
        weld = weld.reshape(-1)
        fn = np.cross(P[F[:, 1]] - P[F[:, 0]], P[F[:, 2]] - P[F[:, 0]])
        acc = np.zeros((weld.max() + 1, 3))
        for k in range(3):
            np.add.at(acc, weld[F[:, k]], fn)
        G = acc[weld]
        G /= np.maximum(np.linalg.norm(G, axis=1, keepdims=True), 1e-12)
        if float(np.einsum("ij,ij->i", G, P - P.mean(0)).mean()) < 0:
            G = -G
        used = np.zeros(len(P), bool)
        used[F.reshape(-1)] = True
        Nn = N / np.maximum(np.linalg.norm(N, axis=1, keepdims=True), 1e-12)
        d = np.einsum("ij,ij->i", Nn, G)[used]
        if not len(d):
            continue
        bad = (d > -0.5) if float(np.median(d)) < 0 else (np.abs(d) < 0.5)
        bad_all += int(bad.sum())
        n_all += len(d)
        if bad.mean() > 0.35:
            plans.append((prims, G.astype(np.float32)))
    report["bad_share"] = round(bad_all / max(n_all, 1), 3)
    if not plans:
        return glb, report
    for prims, G in plans:
        view = _add_view(doc, bin_, G.tobytes(), 34962)
        doc["accessors"].append({"bufferView": view, "componentType": 5126, "count": int(len(G)), "type": "VEC3"})
        for prim in prims:
            prim["attributes"]["NORMAL"] = len(doc["accessors"]) - 1
        report["rebuilt"] += int(len(G))
    report["state"] = "rebuilt"
    return _write_glb(doc, bin_), report


# --------------------------------------------------------------------------------------------- external images
def embed_external_images(glb: bytes, base: Path) -> Tuple[bytes, List[str], List[str]]:
    """Images left as file URIs (assimp's FBX / glTF export) embedded from the upload folder."""
    doc, bin_ = _read_glb(glb)
    embedded, missing = [], []
    for img in doc.get("images") or []:
        uri = str(img.get("uri") or "")
        if not uri or uri.startswith("data:") or "bufferView" in img:
            continue
        from urllib.parse import unquote
        p = find_file(unquote(uri), base)
        if p is None:
            missing.append(Path(unquote(uri).replace("\\", "/")).name)
            continue
        try:
            raw, mime = image_bytes(p)
        except Exception:                                      # noqa: BLE001
            missing.append(p.name)
            continue
        img.pop("uri", None)
        img["bufferView"] = _add_view(doc, bin_, raw)
        img["mimeType"] = mime
        embedded.append(p.name)
    if not embedded:
        return glb, embedded, sorted(set(missing))
    return _write_glb(doc, bin_), embedded, sorted(set(missing))


def glb_textured(glb: bytes) -> int:
    doc, _ = _read_glb(glb)
    n = 0
    for m in doc.get("materials") or []:
        s = json.dumps(m)
        if re.search(r'"(baseColorTexture|diffuseTexture|emissiveTexture|normalTexture)"', s):
            n += 1
    return n


def fbx_texture_refs(raw: bytes) -> List[str]:
    names = {x.decode("utf-8", "replace") for x in re.findall(
        rb"[\w\-. ]{1,80}\.(?:png|jpe?g|tga|tiff?|bmp|psd|dds|exr|webp)", raw, re.I)}
    return sorted({Path(n.replace("\\", "/")).name for n in names})


# --------------------------------------------------------------------------------------------- ZIP
def safe_extract(zip_path: Path, dest: Path) -> List[Path]:
    """Unpack a ZIP into dest: no absolute paths, no `..`, no links, at most ZIP_MAX_FILES files / ZIP_MAX_BYTES."""
    out: List[Path] = []
    total = 0
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    with zipfile.ZipFile(zip_path) as z:
        infos = [i for i in z.infolist() if not i.is_dir()]
        if len(infos) > ZIP_MAX_FILES:
            raise ValueError("too many files in the ZIP")
        for info in infos:
            name = info.filename.replace("\\", "/")
            if name.startswith("/") or ".." in name.split("/") or name.startswith("__MACOSX/"):
                continue
            if (info.external_attr >> 16) & 0o170000 == 0o120000:      # a symlink
                continue
            total += info.file_size
            if total > ZIP_MAX_BYTES:
                raise ValueError("the ZIP unpacks to more than 1 GB")
            target = (dest / name).resolve()
            if not str(target).startswith(str(root) + os.sep):
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with z.open(info) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            out.append(target)
    return out


def pick_model(files: List[Path]) -> Optional[Path]:
    """The model of a bundle: GLB > glTF > FBX > OBJ, the largest of its kind."""
    for ext in MODEL_EXT:
        cands = [p for p in files if p.suffix.lower() == ext]
        if cands:
            return max(cands, key=lambda p: p.stat().st_size)
    return None


# --------------------------------------------------------------------------------------------- one entry point
def normalize_textured(model: Path, base: Optional[Path] = None) -> Tuple[Optional[bytes], Dict[str, Any]]:
    """The textured GLB of an OBJ / FBX / glTF / GLB upload with its folder (None = the caller converts as before)
    and the texture audit. The folder is the upload folder (+ an unpacked ZIP and any files added later)."""
    base = Path(base or model.parent)
    ext = model.suffix.lower()
    audit: Dict[str, Any] = {"schema": "autorig.v3.texture-audit/1", "format": ext.lstrip(".")}
    glb: Optional[bytes] = None
    if ext == ".obj":
        from v3_intake import obj_to_glb
        raw = model.read_bytes()
        refs = obj_texture_refs(raw, base)
        glb = obj_to_glb(raw)
        glb, embedded = apply_mtl(glb, refs, base)
        audit.update(referenced=refs["referenced"], missing=refs["missing"], embedded=embedded,
                     mtllib=refs["mtllib"], materials=len(refs["usemtl"]))
    elif ext in (".fbx", ".gltf"):
        import fbx_ascii
        out = model.with_name(model.stem + ".v3.glb")
        try:
            if ext == ".fbx":
                audit["assimp"] = fbx_ascii.fbx_to_glb(model, out)
            else:                                              # in place: the .bin and images resolve beside it
                import subprocess
                import tempfile
                with tempfile.TemporaryDirectory(prefix="v3-gltf-") as work:
                    tmp = Path(work) / "out.glb"
                    run = subprocess.run([fbx_ascii.ASSIMP, "export", str(model.resolve()), str(tmp), "-f", "glb2"],
                                         capture_output=True, text=True, timeout=fbx_ascii.TIMEOUT_S,
                                         cwd=str(model.parent))
                    if run.returncode or not tmp.is_file():
                        raise RuntimeError((run.stderr or run.stdout or "")[-300:])
                    shutil.copyfile(tmp, out)
                import hashlib
                audit["assimp"] = {"method": "assimp glb2 (gltf)",
                                   "source_sha256": hashlib.sha256(model.read_bytes()).hexdigest()}
        except Exception as exc:                               # noqa: BLE001 - the converter path stays
            audit["assimp_error"] = str(exc)[:200]
            return None, audit
        glb = out.read_bytes()
        glb, embedded, missing = embed_external_images(glb, base)
        refs = fbx_texture_refs(model.read_bytes()) if ext == ".fbx" else []
        missing = sorted(set(missing) | {r for r in refs if find_file(r, base) is None})
        audit.update(referenced=sorted(set(refs) | set(embedded)), missing=missing, embedded=embedded)
    elif ext == ".glb":
        glb = model.read_bytes()
        glb, embedded, missing = embed_external_images(glb, base)
        audit.update(referenced=embedded + missing, missing=missing, embedded=embedded)
    else:
        return None, audit
    glb, nrep = fix_normals(glb)
    audit["normals"] = nrep
    textured = glb_textured(glb)
    audit["textured_materials"] = textured
    audit["status"] = "source_ok" if textured and not audit.get("missing") else \
        ("missing_files" if audit.get("missing") else "none")
    return glb, audit


def task_upload_dir(task) -> Optional[Path]:
    from v3_intake import local_upload_path
    try:
        settings = json.loads(task.viewer_settings or "{}")
    except (TypeError, ValueError):
        settings = {}
    url = ((settings.get("v3") or {}).get("intake") or {}).get("original_url") or task.input_url
    p = local_upload_path(url)
    return p.parent if p is not None else None


# --------------------------------------------------------------------------------------------- the task API
MT_RUNS = Path(os.getenv("AUTORIG_MT_RUNS", "/srv/autorig/data/motion_transfer/runs"))
ADD_EXT = IMAGE_EXT + (".mtl", ".zip", ".bin", ".gltf")
_NAME = re.compile(r"[^A-Za-z0-9._\- ()\[\]+]")


def _safe_name(name: str) -> str:
    base = Path(str(name or "").replace("\\", "/")).name.strip()
    base = _NAME.sub("_", base)[:120].lstrip(".")
    return base or "file"


def _mt_status(task) -> Optional[Dict[str, Any]]:
    try:
        settings = json.loads(task.viewer_settings or "{}")
    except (TypeError, ValueError):
        return None
    run = (((settings.get("v3") or {}).get("session") or {}).get("mt_run_id")) or ""
    if not re.fullmatch(r"[0-9a-f]{20}", str(run)):
        return None
    try:
        doc = json.loads((MT_RUNS / run / "analysis" / "textures.json").read_text())
    except (OSError, ValueError):
        return {"run": run}
    keep = ("status", "missing", "referenced", "flat", "uv", "normals", "autotex", "needs_texture", "at")
    return {"run": run, **{k: doc.get(k) for k in keep}}


def textures_view(task, *, owner: bool) -> Dict[str, Any]:
    try:
        settings = json.loads(task.viewer_settings or "{}")
    except (TypeError, ValueError):
        settings = {}
    v3 = settings.get("v3") or {}
    intake = (v3.get("intake") or {}).get("textures")
    mt = _mt_status(task)
    missing = list((intake or {}).get("missing") or (mt or {}).get("missing") or [])
    status = (intake or {}).get("status") or (mt or {}).get("status")
    return {"schema": "autorig.task-v3-textures/1", "task_id": task.id, "status": status, "missing": missing,
            "intake": intake, "model": mt, "can_add_files": bool(owner and task_upload_dir(task) is not None),
            "add_url": f"/api/task/{task.id}/v3-source-files", "accept": list(ADD_EXT),
            "autotex": (mt or {}).get("autotex"), "added": v3.get("source_files") or []}


async def new_source_attempt(db, task, data: bytes, normalization: Mapping[str, Any]) -> Dict[str, Any]:
    """The same task, a new attempt bound to a new exact source (the old attempt superseded, never deleted)."""
    from datetime import datetime

    from sqlalchemy import text as _sql

    from v3_intake import build_binding, glb_facts, store_source, texture_summary
    from v3_task_runtime import SETTLED_BINDING_STATES, SameTaskDbBindings

    bindings = SameTaskDbBindings()
    current = await bindings.latest(db, task.id)
    if current is None:
        raise ValueError("unbound")
    if current.state not in SETTLED_BINDING_STATES:
        raise RuntimeError("busy")
    facts = glb_facts(data)
    path, digest = store_source(data)
    if digest == current.source_sha256:
        return {"changed": False, "attempt": current.attempt, "source_sha256": digest}
    manifest = dict(current.source_manifest or {})
    changed = await db.execute(_sql(
        "UPDATE v3_dispatch_task_bindings SET state='superseded',updated_at=CURRENT_TIMESTAMP "
        "WHERE task_id=:t AND attempt=:a AND state=:s"), {"t": task.id, "a": current.attempt, "s": current.state})
    if getattr(changed, "rowcount", 1) != 1:
        raise RuntimeError("busy")
    binding = build_binding(task.id, path, digest, requested_intent=current.requested_intent,
                            origin=str(manifest.get("origin") or "website"),
                            filename=str(manifest.get("filename") or ""), original=manifest.get("original") or {},
                            normalization=normalization, facts=facts, receipt=current.upstream_receipt,
                            attempt=current.attempt + 1)
    await bindings.bind_in_task_transaction(db, binding)
    settings = json.loads(task.viewer_settings or "{}")
    v3 = dict(settings.get("v3") or {})
    intake = dict(v3.get("intake") or {})
    intake.update(source_sha256=digest, textures=texture_summary(normalization.get("textures")))
    v3.update(state="pending", stage="dispatch", progress=0.0, attempt=current.attempt + 1, error=None,
              intake=intake, updated_at=datetime.utcnow().isoformat() + "Z")
    settings["v3"] = v3
    task.viewer_settings = json.dumps(settings, ensure_ascii=False)
    task.status = "created"
    task.error_message = None
    task.updated_at = datetime.utcnow()
    await db.commit()
    print(f"[V3 textures] {task.id}: new source {digest[:12]} with the added files -> attempt {current.attempt + 1}")
    return {"changed": True, "attempt": current.attempt + 1, "source_sha256": digest}


def build_v3_textures_router(*, get_db, get_current_user, task_model, is_admin_email, effective_anon_id):
    import asyncio

    from fastapi import APIRouter, Depends, HTTPException
    from fastapi.responses import JSONResponse
    from sqlalchemy import select

    router = APIRouter()
    uuid_rx = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

    def err(status, code):
        from user_language import user_error_detail
        return HTTPException(status_code=status, detail=user_error_detail(code))

    async def load(task_id, request, user, db):
        from v3_viewer_routes import _can_access_task
        if not uuid_rx.fullmatch(str(task_id or "")):
            raise HTTPException(status_code=404, detail="Task not found")
        task = (await db.execute(select(task_model).where(task_model.id == task_id))).scalar_one_or_none()
        if task is None or not _can_access_task(task, is_public=bool(getattr(task, "is_public", False)),
                                                user=user, request=request, is_admin_email=is_admin_email):
            raise HTTPException(status_code=404, detail="Task not found")
        return task

    def is_owner(task, user, request) -> bool:
        email = getattr(user, "email", None) if user is not None else None
        if email and is_admin_email(email):
            return True
        anon = effective_anon_id(request)
        return bool((user is not None and task.owner_type == "user" and task.owner_id == email)
                    or (task.owner_type == "anon" and anon and task.owner_id == anon))

    @router.get("/api/task/{task_id}/v3-textures")
    async def v3_textures_get(task_id: str, request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        task = await load(task_id, request, user, db)
        return JSONResponse(textures_view(task, owner=is_owner(task, user, request)),
                            headers={"cache-control": "no-store"})

    @router.post("/api/task/{task_id}/v3-source-files")
    async def v3_source_files(task_id: str, request: Request, user=Depends(get_current_user), db=Depends(get_db)):
        """Texturing · V3: the missing texture / material files (or a ZIP) added to the same task; the source is
        normalised again with them and the task runs a new attempt. Nothing already uploaded is overwritten."""
        task = await load(task_id, request, user, db)
        if not is_owner(task, user, request):
            raise err(403, "source_files_not_owner")
        if str(getattr(task, "pipeline_kind", "") or "") != "v3":
            raise err(409, "source_files_not_v3")
        folder = task_upload_dir(task)
        if folder is None or not folder.is_dir():
            raise err(409, "source_files_no_upload")
        form = await request.form()
        items = [f for f in form.getlist("files") + form.getlist("file") if hasattr(f, "read")]
        if not items:
            raise err(400, "source_files_empty")
        saved, skipped, total = [], [], 0
        for up in items[:64]:
            name = _safe_name(getattr(up, "filename", "") or "")
            if not name.lower().endswith(ADD_EXT):
                skipped.append({"name": name, "why": "type"})
                continue
            target = folder / name
            if target.exists():                                   # never overwrite what the user uploaded
                skipped.append({"name": name, "why": "exists"})
                continue
            tmp = folder / f".{name}.{os.getpid()}.part"
            with open(tmp, "wb") as out:
                while True:
                    chunk = await up.read(1 << 20)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > ADD_MAX_BYTES:
                        out.close()
                        tmp.unlink(missing_ok=True)
                        raise err(413, "source_files_too_large")
                    out.write(chunk)
            os.replace(tmp, target)
            os.chmod(target, 0o644)
            if name.lower().endswith(".zip"):
                try:
                    await asyncio.to_thread(safe_extract, target, folder / (Path(name).stem + "_zip"))
                except Exception:                                 # noqa: BLE001
                    skipped.append({"name": name, "why": "zip_unreadable"})
                    continue
            saved.append(name)
        settings = json.loads(task.viewer_settings or "{}")
        v3 = dict(settings.get("v3") or {})
        v3["source_files"] = (v3.get("source_files") or [])[-40:] + [{"name": n} for n in saved]
        settings["v3"] = v3
        task.viewer_settings = json.dumps(settings, ensure_ascii=False)
        await db.commit()
        if not saved:
            return JSONResponse({"saved": [], "skipped": skipped, "changed": False},
                                headers={"cache-control": "no-store"})
        from v3_intake import local_upload_path, texture_summary
        model = local_upload_path((v3.get("intake") or {}).get("original_url") or task.input_url)
        if model is not None and model.suffix.lower() == ".zip":
            model = pick_model(sorted((model.parent / (model.stem + "_zip")).rglob("*")))
        if model is None or not model.is_file():
            raise err(409, "source_files_no_upload")
        data, audit = await asyncio.to_thread(normalize_textured, model, folder)
        if data is None:
            raise err(422, "source_files_unreadable")
        receipt = dict(audit.pop("assimp", None) or {})
        try:
            res = await new_source_attempt(db, task, data, {"method": "v3-intake:source-files", **receipt,
                                                            "textures": audit, "added": saved})
        except RuntimeError:
            raise err(409, "source_files_busy")
        except ValueError:
            raise err(409, "source_files_no_upload")
        return JSONResponse({"saved": saved, "skipped": skipped, "textures": texture_summary(audit), **res},
                            headers={"cache-control": "no-store"})

    return router

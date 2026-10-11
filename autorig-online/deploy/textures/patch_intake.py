"""Texturing · V3: anchored edits of backend/v3_intake.py (idempotent). python patch_intake.py <v3_intake.py>"""
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
raw = p.read_bytes()
crlf = b"\r\n" in raw
s = raw.decode("utf-8").replace("\r\n", "\n")
if "def _admit_zip(" in s:
    print("already patched")
    sys.exit(0)


def rep(old, new):
    global s
    assert s.count(old) == 1, old[:70]
    s = s.replace(old, new, 1)


rep('''    if head.startswith(b"Kaydara FBX Binary") or head.lstrip().startswith(b"; FBX"):
        return "fbx"''', '''    if head.startswith(b"Kaydara FBX Binary") or head.lstrip().startswith(b"; FBX"):
        return "fbx"
    if head[:4] == b"PK\\x03\\x04":                       # Texturing · V3: a model with its textures in a ZIP
        return "zip"''')
rep('''def _v3_settings(state: str, **intake: Any) -> Dict[str, Any]:''', '''def texture_summary(audit: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Texturing · V3: the part of the intake texture audit the task page and the session agent read."""
    if not audit:
        return None
    return {"status": audit.get("status"), "missing": list(audit.get("missing") or [])[:20],
            "referenced": list(audit.get("referenced") or [])[:20], "embedded": len(audit.get("embedded") or []),
            "normals": (audit.get("normals") or {}).get("state")}


def _v3_settings(state: str, **intake: Any) -> Dict[str, Any]:''')
rep('''        viewer_settings=_v3_settings("pending", origin=origin, format="glb", filename=filename[:200],
                                     source_sha256=digest, requested_intent=requested_intent))''', '''        viewer_settings=_v3_settings("pending", origin=origin, format="glb", filename=filename[:200],
                                     source_sha256=digest, requested_intent=requested_intent,
                                     textures=texture_summary((normalization or {}).get("textures"))))''')
rep('''        import fbx_ascii

        glb = Path(path).with_name(Path(path).stem + ".v3.glb")
        try:
            receipt = await asyncio.to_thread(fbx_ascii.fbx_to_glb, Path(path), glb)
        except Exception as exc:
            print(f"[V3 intake] local FBX -> GLB failed ({exc}); waiting for a converter")
        else:
            original["sha256"] = receipt["source_sha256"]
            return await admit_glb(db, data=glb.read_bytes(), original_url=original_url, filename=filename,
                                   owner_type=owner_type, owner_id=owner_id, origin=origin,
                                   created_via_api=created_via_api, requested_intent=requested_intent,
                                   input_type=input_type, input_bytes=input_bytes, original=original,
                                   normalization=receipt)''', '''        import v3_textures

        # Texturing · V3: assimp, then the textures beside it embedded and broken normals rebuilt
        try:
            data, audit = await asyncio.to_thread(v3_textures.normalize_textured, Path(path))
        except Exception as exc:                                  # noqa: BLE001
            data, audit = None, {"error": str(exc)[:200]}
        if data is None:
            print(f"[V3 intake] local FBX -> GLB failed ({audit.get('assimp_error') or audit.get('error')}); "
                  "waiting for a converter")
        else:
            receipt = dict(audit.pop("assimp", None) or {})
            original["sha256"] = receipt.get("source_sha256") or hashlib.sha256(Path(path).read_bytes()).hexdigest()
            return await admit_glb(db, data=data, original_url=original_url, filename=filename,
                                   owner_type=owner_type, owner_id=owner_id, origin=origin,
                                   created_via_api=created_via_api, requested_intent=requested_intent,
                                   input_type=input_type, input_bytes=input_bytes, original=original,
                                   normalization={**receipt, "textures": audit})
    if fmt == "zip":
        return await _admit_zip(db, path=Path(path), original=original, original_url=original_url,
                                filename=filename, owner_type=owner_type, owner_id=owner_id, origin=origin,
                                created_via_api=created_via_api, requested_intent=requested_intent,
                                input_type=input_type, input_bytes=input_bytes)''')
rep('''    raise V3IntakeError("unsupported_format", "V3 accepts GLB, FBX and OBJ meshes, images and video")
''', '''    raise V3IntakeError("unsupported_format", "V3 accepts GLB, FBX and OBJ meshes, images and video")


async def _admit_zip(db, *, path: Path, original: Dict[str, Any], original_url: str, filename: str,
                     owner_type: str, owner_id: str, origin: str, created_via_api: bool, requested_intent: str,
                     input_type: str, input_bytes: Optional[int]):
    """Texturing · V3: a ZIP with a model (GLB / glTF / FBX / OBJ) and its textures, unpacked beside the upload."""
    import v3_textures

    folder = path.with_name(path.stem + "_zip")
    try:
        files = await asyncio.to_thread(v3_textures.safe_extract, path, folder)
    except Exception as exc:                                      # noqa: BLE001 - a broken archive is the user's
        raise V3IntakeError("invalid_zip", f"the ZIP could not be unpacked: {exc}"[:300])
    model = v3_textures.pick_model(files)
    if model is None:
        raise V3IntakeError("zip_without_model", "the ZIP has no GLB, glTF, FBX or OBJ model")
    data, audit = await asyncio.to_thread(v3_textures.normalize_textured, model, path.parent)
    if data is None:
        raise V3IntakeError("normalization_failed", str(audit.get("assimp_error") or "the model could not be read"))
    receipt = dict(audit.pop("assimp", None) or {})
    original = {**original, "format": "zip", "model": str(model.relative_to(path.parent)),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    return await admit_glb(db, data=data, original_url=original_url, filename=filename, owner_type=owner_type,
                           owner_id=owner_id, origin=origin, created_via_api=created_via_api,
                           requested_intent=requested_intent, input_type=input_type, input_bytes=input_bytes,
                           original=original, normalization={"method": "v3-intake:zip", **receipt,
                                                             "textures": audit})
''')
rep('''    intake.update(source_sha256=digest, bound_at=datetime.utcnow().isoformat() + "Z", origin=origin,
                  requested_intent=requested_intent)''', '''    intake.update(source_sha256=digest, bound_at=datetime.utcnow().isoformat() + "Z", origin=origin,
                  requested_intent=requested_intent)
    if (normalization or {}).get("textures"):                    # Texturing · V3
        intake["textures"] = texture_summary(normalization["textures"])''')
rep('''                if intake.get("format") == "obj" and local is not None:
                    raw = local.read_bytes()
                    data = await asyncio.to_thread(obj_to_glb, raw)
                    normalization = {"method": "v3-intake:obj_to_glb", "obj_sha256": hashlib.sha256(raw).hexdigest()}''', '''                if intake.get("format") == "obj" and local is not None:
                    import v3_textures

                    raw = local.read_bytes()
                    # Texturing · V3: the .mtl and its textures beside the OBJ, broken normals rebuilt
                    data, audit = await asyncio.to_thread(v3_textures.normalize_textured, local)
                    normalization = {"method": "v3-intake:obj_to_glb", "obj_sha256": hashlib.sha256(raw).hexdigest(),
                                     "textures": audit}''')
out = s.replace("\n", "\r\n") if crlf else s
p.write_bytes(out.encode("utf-8"))
print("patched", "crlf" if crlf else "lf")

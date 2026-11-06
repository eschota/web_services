"""Safe read-only preflight for server-owned source URL prefixes."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote,urlsplit

@dataclass(frozen=True)
class OwnedSourceProbe:
    state:str
    prefix:bytes=b""
    size:int|None=None
    detail:str=""

def _origin(url):
    parsed=urlsplit(str(url or "").strip())
    if parsed.scheme not in ("http","https") or not parsed.hostname:return None
    try:port=parsed.port
    except ValueError:return None
    if (parsed.scheme,port) in (("http",80),("https",443)):port=None
    return parsed.scheme.lower(),parsed.hostname.lower(),port

def _candidate(url,app_url,upload_dir,renderfin_data_dir):
    parsed=urlsplit(str(url or "").strip())
    app_origin=_origin(app_url)
    if app_origin is None or _origin(url)!=app_origin:return None
    if parsed.username is not None or parsed.password is not None:return "unsafe",None,None
    raw=parsed.path or ""
    if "\\" in raw or any(marker in raw.lower() for marker in ("%2f","%5c","%00")):return "unsafe",None,None
    decoded=unquote(raw)
    if "\\" in decoded or "\x00" in decoded:return "unsafe",None,None
    segments=decoded.split("/")
    if any(segment in (".","..") for segment in segments):return "unsafe",None,None
    if decoded.startswith("/u/"):root=Path(upload_dir);tail=decoded[len("/u/"):]
    elif decoded.startswith("/renderfin/render/"):root=Path(renderfin_data_dir)/"render";tail=decoded[len("/renderfin/render/"):]
    else:return None
    if not tail or any(not segment for segment in tail.split("/")):return "unsafe",None,None
    try:
        resolved_root=root.resolve(strict=True)
        if not resolved_root.is_dir():return "internal",None,None
    except OSError:return "internal",None,None
    try:
        target=(resolved_root/tail).resolve(strict=False);target.relative_to(resolved_root)
    except ValueError:return "unsafe",None,None
    except OSError:return "internal",None,None
    return "owned",resolved_root,target

def _read(target):
    stat=target.stat()
    if not target.is_file():raise ValueError("owned source is not a regular file")
    with target.open("rb") as handle:prefix=handle.read(64)
    return prefix,int(stat.st_size)

async def probe_owned_source(url,*,app_url,upload_dir,renderfin_data_dir):
    candidate=_candidate(url,app_url,upload_dir,renderfin_data_dir)
    if candidate is None:return None
    state,_,target=candidate
    if state=="unsafe":return OwnedSourceProbe("unsafe",detail="owned source path is unsafe")
    if state=="internal":return OwnedSourceProbe("internal",detail="owned source root is unavailable")
    try:prefix,size=await asyncio.to_thread(_read,target);return OwnedSourceProbe("ok",prefix,size)
    except FileNotFoundError:return OwnedSourceProbe("missing",detail="source returned HTTP 404")
    except ValueError as exc:return OwnedSourceProbe("unsafe",detail=str(exc))
    except OSError as exc:return OwnedSourceProbe("transient",detail=f"owned source storage failed: {exc.__class__.__name__}")

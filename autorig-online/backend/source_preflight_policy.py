"""Typed source-preflight outcomes and retry policy."""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Iterator
import json
from urllib.parse import urljoin,urlsplit,urlunsplit

class SourceDisposition(str,Enum):
    AVAILABLE="available"
    TRANSIENT="transient"
    BOUNDED="bounded"
    PERMANENT="permanent"
    INTERNAL="internal"

@dataclass(frozen=True)
class SourcePreflightResult:
    available:bool
    detail:str=""
    disposition:SourceDisposition=SourceDisposition.AVAILABLE
    @property
    def permanent(self)->bool:return self.disposition is SourceDisposition.PERMANENT
    def __iter__(self)->Iterator[object]:
        yield self.available;yield self.detail;yield self.permanent

def normalize_source_preflight(value)->SourcePreflightResult:
    if isinstance(value,SourcePreflightResult):return value
    if isinstance(value,tuple) and len(value)==3:
        ok,detail,permanent=value
        return SourcePreflightResult(bool(ok),str(detail or ""),SourceDisposition.PERMANENT if permanent else SourceDisposition.BOUNDED)
    raise TypeError("invalid source preflight result")

def retry_delay_seconds(attempts:int)->int:
    schedule=(60,300,900,1800)
    return schedule[min(max(1,int(attempts))-1,len(schedule)-1)]

def normalized_http_origin(value:str)->str:
    parsed=urlsplit(str(value or "").strip())
    if parsed.scheme.lower() not in ("http","https") or not parsed.hostname or parsed.username is not None or parsed.password is not None:raise ValueError("redirect origin must be credential-free absolute http(s)")
    if parsed.path not in ("","/") or parsed.query or parsed.fragment:raise ValueError("redirect origin must not contain path/query/fragment")
    try:port=parsed.port
    except ValueError as exc:raise ValueError("redirect origin port is invalid") from exc
    if (parsed.scheme.lower(),port) in (("http",80),("https",443)):port=None
    host=parsed.hostname.lower();host=f"[{host}]" if ":" in host and not host.startswith("[") else host
    return urlunsplit((parsed.scheme.lower(),f"{host}:{port}" if port is not None else host,"","",""))

def redirect_allowlist(original_url:str,app_url:str,logical_worker_origins,explicit_raw:str=""):
    allowed={normalized_http_origin(urlunsplit((*urlsplit(original_url)[:2],"","",""))),normalized_http_origin(app_url)}
    allowed.update(normalized_http_origin(origin) for origin in logical_worker_origins)
    if str(explicit_raw or "").strip():
        try:extra=json.loads(explicit_raw)
        except json.JSONDecodeError as exc:raise ValueError("AUTORIG_SOURCE_REDIRECT_ORIGINS must be JSON") from exc
        if not isinstance(extra,list) or any(not isinstance(item,str) for item in extra):raise ValueError("AUTORIG_SOURCE_REDIRECT_ORIGINS must be a JSON origin list")
        allowed.update(normalized_http_origin(item) for item in extra)
    return frozenset(allowed)

def resolve_source_redirect(current_url:str,location:str,allowed_origins,seen):
    if not str(location or "").strip():raise ValueError("redirect Location is missing")
    destination=urljoin(current_url,location)
    parsed=urlsplit(destination)
    if parsed.scheme.lower() not in ("http","https") or not parsed.hostname or parsed.username is not None or parsed.password is not None:raise ValueError("redirect destination is unsafe")
    origin=normalized_http_origin(urlunsplit((parsed.scheme,parsed.netloc,"","","")))
    if origin not in allowed_origins:raise ValueError("redirect origin is not allowed")
    normalized=urlunsplit((parsed.scheme.lower(),parsed.netloc,parsed.path or "/",parsed.query,""))
    if normalized in seen:raise ValueError("redirect loop detected")
    return normalized

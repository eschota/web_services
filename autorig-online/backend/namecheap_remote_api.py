"""
Remote-controlled Namecheap DNS API for autorig.online (and same SLD/TLD pattern).

Mounted at GET/POST /api-name-cheap. Protected by NAMECHEAP_REMOTE_API_KEY (header X-API-Key).
Registrar credentials: NAMECHEAP_API_USER, NAMECHEAP_API_KEY in environment.
"""

from __future__ import annotations

import asyncio
import hmac
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from config import (
    APP_URL,
    FACERIG_DNS_IP,
    NAMECHEAP_API_USER,
    NAMECHEAP_CLIENT_IP,
    NAMECHEAP_REGISTRAR_API_KEY,
    NAMECHEAP_REMOTE_API_KEY,
    NAMECHEAP_REMOTE_IP_ALLOWLIST,
    NAMECHEAP_USERNAME,
)

NC_API = "https://api.namecheap.com/xml.response"

router = APIRouter(tags=["namecheap-remote"])


def _verify_remote_key(x_api_key: Optional[str]) -> None:
    if not x_api_key or not hmac.compare_digest(
        x_api_key.encode("utf-8"),
        NAMECHEAP_REMOTE_API_KEY.encode("utf-8"),
    ):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


def _client_ip(request: Request) -> str:
    """Real client IP when behind nginx (X-Forwarded-For / X-Real-IP)."""
    xff = (request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    xri = (request.headers.get("x-real-ip") or "").strip()
    if xri:
        return xri
    if request.client:
        return request.client.host or ""
    return ""


def _verify_ip_allowlist(request: Request) -> None:
    """If NAMECHEAP_REMOTE_IP_ALLOWLIST is non-empty, POST is only allowed from these IPs."""
    if not NAMECHEAP_REMOTE_IP_ALLOWLIST:
        return
    ip = _client_ip(request)
    if ip not in NAMECHEAP_REMOTE_IP_ALLOWLIST:
        raise HTTPException(
            status_code=403,
            detail=(
                "IP not in NAMECHEAP_REMOTE_IP_ALLOWLIST. "
                f"Your IP: {ip or '(unknown)'}. "
                "Set env on server: comma-separated IPv4/IPv6, e.g. 185.171.83.65,92.51.37.9"
            ),
        )


def _split_zone(domain_name: str) -> tuple[str, str]:
    parts = domain_name.strip().lower().rstrip(".").split(".")
    if len(parts) < 2:
        raise ValueError("domain_name must look like autorig.online")
    return parts[-2], parts[-1]


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if tag.startswith("{") else tag


def _parse_hosts(xml_bytes: bytes) -> list[dict[str, str]]:
    root = ET.fromstring(xml_bytes)

    def _child_text(el: ET.Element, *names: str) -> str:
        want = {n.lower() for n in names}
        for ch in el:
            if _strip_ns(ch.tag).lower() in want:
                return (ch.text or "").strip()
        return ""

    hosts: list[dict[str, str]] = []
    for el in root.iter():
        if _strip_ns(el.tag) != "host":
            continue
        if el.get("Name") is not None:
            hosts.append(
                {
                    "Name": el.get("Name", ""),
                    "Type": el.get("Type", ""),
                    "Address": el.get("Address", ""),
                    "MXPref": el.get("MXPref", "10"),
                    "TTL": el.get("TTL", "1800"),
                }
            )
            continue
        name = _child_text(el, "Name", "name")
        typ = _child_text(el, "Type", "type")
        addr = _child_text(el, "Address", "address")
        if not name and not typ:
            continue
        hosts.append(
            {
                "Name": name,
                "Type": typ,
                "Address": addr,
                "MXPref": _child_text(el, "MXPref", "mxpref") or "10",
                "TTL": _child_text(el, "TTL", "ttl") or "1800",
            }
        )
    return hosts


def _nc_params(
    command: str,
    extra: dict[str, str],
) -> dict[str, str]:
    u = NAMECHEAP_API_USER
    k = NAMECHEAP_REGISTRAR_API_KEY
    if not u or not k:
        raise HTTPException(
            status_code=503,
            detail="Namecheap registrar not configured (NAMECHEAP_API_USER / NAMECHEAP_API_KEY)",
        )
    return {
        "ApiUser": u,
        "ApiKey": k,
        "UserName": NAMECHEAP_USERNAME or u,
        "ClientIp": NAMECHEAP_CLIENT_IP,
        "Command": command,
        **extra,
    }


def nc_get_hosts(sld: str, tld: str) -> tuple[list[dict[str, str]], str]:
    params = _nc_params(
        "namecheap.domains.dns.getHosts",
        {"SLD": sld, "TLD": tld},
    )
    url = NC_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read()
    text = body.decode("utf-8", errors="replace")
    if 'Status="ERROR"' in text:
        raise HTTPException(status_code=502, detail={"namecheap_xml": text[:8000]})
    return _parse_hosts(body), text


def nc_set_hosts(sld: str, tld: str, hosts: list[dict[str, str]]) -> str:
    data = _nc_params(
        "namecheap.domains.dns.setHosts",
        {"SLD": sld, "TLD": tld},
    )
    for i, h in enumerate(hosts, start=1):
        data[f"HostName{i}"] = h["Name"]
        data[f"RecordType{i}"] = h["Type"]
        data[f"Address{i}"] = h["Address"]
        data[f"MXPref{i}"] = h.get("MXPref") or "10"
        data[f"TTL{i}"] = h.get("TTL") or "1800"
    encoded = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(NC_API, data=encoded, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read()
    text = body.decode("utf-8", errors="replace")
    if 'IsSuccess="true"' not in text and "IsSuccess='true'" not in text:
        if 'Status="ERROR"' in text or "<Error " in text:
            raise HTTPException(status_code=502, detail={"namecheap_xml": text[:8000]})
    return text


def _merge_upsert(
    existing: list[dict[str, str]],
    updates: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Replace records with same Name+Type key; append new."""
    key = lambda r: (r.get("Name", ""), r.get("Type", ""))
    m: dict[tuple[str, str], dict[str, str]] = {key(r): dict(r) for r in existing}
    for u in updates:
        m[key(u)] = u
    return list(m.values())


class NamecheapRecordIn(BaseModel):
    """Host label without the zone (e.g. facerig → facerig.autorig.online)."""

    host: str = Field(..., description='Subdomain label or "@" for root')
    record_type: str = Field("A", description="A, AAAA, CNAME, TXT, MX")
    address: str = Field(..., description="Target IP, hostname, or TXT body")
    ttl: str = Field("300", description="60–60000")


class NamecheapRemoteBody(BaseModel):
    domain_name: str = Field(
        ...,
        description="DNS zone, e.g. autorig.online",
        examples=["autorig.online"],
    )
    action: str = Field(
        "upsert_records",
        description="get_hosts | upsert_records | replace_all_records",
    )
    records: Optional[list[NamecheapRecordIn]] = Field(
        None,
        description="Required for upsert_records / replace_all_records",
    )


def ensure_facerig_on_startup() -> None:
    """Upsert facerig A → FACERIG_DNS_IP if registrar env is set."""
    if not NAMECHEAP_API_USER or not NAMECHEAP_REGISTRAR_API_KEY:
        print("[Namecheap DNS] Skip facerig: registrar credentials not set")
        return
    try:
        sld, tld = _split_zone("autorig.online")
        existing, _ = nc_get_hosts(sld, tld)
        merged = _merge_upsert(
            existing,
            [
                {
                    "Name": "facerig",
                    "Type": "A",
                    "Address": FACERIG_DNS_IP,
                    "MXPref": "10",
                    "TTL": "300",
                }
            ],
        )
        nc_set_hosts(sld, tld, merged)
        print(f"[Namecheap DNS] facerig.autorig.online → A {FACERIG_DNS_IP} OK")
    except Exception as e:
        print(f"[Namecheap DNS] facerig startup failed: {e}")


@router.get("/api-name-cheap")
async def namecheap_remote_docs(
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    """Public docs without key; with valid key — same + secret reminder."""
    base = APP_URL.rstrip("/")
    allow = sorted(NAMECHEAP_REMOTE_IP_ALLOWLIST)
    instructions = {
        "endpoint": f"{base}/api-name-cheap",
        "methods": ["GET", "POST"],
        "authentication": {
            "header": "X-API-Key",
            "value": "Same as NAMECHEAP_REMOTE_API_KEY on server (see config / env).",
        },
        "ip_allowlist_post": {
            "env": "NAMECHEAP_REMOTE_IP_ALLOWLIST",
            "behavior": (
                "If set (comma-separated IPs), POST /api-name-cheap is only accepted from those IPs "
                "(checked via X-Forwarded-For / X-Real-IP). GET stays public. Empty = no IP restriction."
            ),
            "configured_ips": allow if allow else None,
            "your_detected_ip": _client_ip(request) or None,
        },
        "namecheap_registrar_env": {
            "NAMECHEAP_API_USER": "Namecheap account login",
            "NAMECHEAP_API_KEY": "Namecheap API key (Profile → API)",
            "NAMECHEAP_USERNAME": "Optional; defaults to NAMECHEAP_API_USER",
            "NAMECHEAP_CLIENT_IP": "Must be whitelisted in Namecheap API; this server uses 185.171.83.65",
        },
        "post_json": {
            "domain_name": "autorig.online",
            "action": "get_hosts | upsert_records | replace_all_records",
            "records": [
                {
                    "host": "facerig",
                    "record_type": "A",
                    "address": "185.171.83.65",
                    "ttl": "300",
                }
            ],
        },
        "actions": {
            "get_hosts": "Returns current hosts from Namecheap (merge-safe read).",
            "upsert_records": "getHosts → merge by (host, type) → setHosts. Other records kept.",
            "replace_all_records": "Danger: setHosts with ONLY the records you send. Use only if you know what you are doing.",
        },
        "curl_examples": {
            "get_hosts": (
                f'curl -sS -H "X-API-Key: YOUR_REMOTE_KEY" '
                f'-H "Content-Type: application/json" '
                f'-d \'{{"domain_name":"autorig.online","action":"get_hosts"}}\' '
                f"-X POST {base}/api-name-cheap"
            ),
            "upsert": (
                f'curl -sS -H "X-API-Key: YOUR_REMOTE_KEY" '
                f'-H "Content-Type: application/json" '
                f'-d \'{{"domain_name":"autorig.online","action":"upsert_records",'
                f'"records":[{{"host":"facerig","record_type":"A","address":"185.171.83.65","ttl":"300"}}]}}\' '
                f"-X POST {base}/api-name-cheap"
            ),
        },
        "notes": [
            "setHosts replaces all records at Namecheap; this API re-sends existing + your changes for upsert_records.",
            "Zone format: domain_name must be the zone (autorig.online), not a FQDN of a record.",
            "host=facerig creates facerig.autorig.online when domain_name is autorig.online.",
        ],
    }
    if x_api_key and hmac.compare_digest(
        x_api_key.encode("utf-8"),
        NAMECHEAP_REMOTE_API_KEY.encode("utf-8"),
    ):
        instructions["authenticated"] = True
        instructions["remote_key_config"] = (
            "NAMECHEAP_REMOTE_API_KEY in /etc/autorig-backend.env or config default"
        )
    else:
        instructions["authenticated"] = False
        instructions["hint"] = "Send header X-API-Key to see authenticated=true and config hints."
    return instructions


@router.post("/api-name-cheap")
async def namecheap_remote_post(
    request: Request,
    body: NamecheapRemoteBody,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
):
    _verify_ip_allowlist(request)
    _verify_remote_key(x_api_key)
    sld, tld = _split_zone(body.domain_name)
    action = (body.action or "upsert_records").strip().lower()

    if action == "get_hosts":
        hosts, raw = await asyncio.to_thread(nc_get_hosts, sld, tld)
        return {"ok": True, "action": "get_hosts", "hosts": hosts, "raw_xml_preview": raw[:2000]}

    if action == "upsert_records":
        if not body.records:
            raise HTTPException(status_code=400, detail="records required for upsert_records")
        existing, _ = await asyncio.to_thread(nc_get_hosts, sld, tld)
        updates = []
        for r in body.records:
            updates.append(
                {
                    "Name": r.host.strip(),
                    "Type": r.record_type.strip().upper(),
                    "Address": r.address.strip(),
                    "MXPref": "10",
                    "TTL": r.ttl.strip(),
                }
            )
        merged = _merge_upsert(existing, updates)
        raw = await asyncio.to_thread(nc_set_hosts, sld, tld, merged)
        return {
            "ok": True,
            "action": "upsert_records",
            "updated_count": len(updates),
            "total_hosts_sent": len(merged),
            "raw_xml_preview": raw[:2000],
        }

    if action == "replace_all_records":
        if not body.records:
            raise HTTPException(status_code=400, detail="records required for replace_all_records")
        hosts = []
        for r in body.records:
            hosts.append(
                {
                    "Name": r.host.strip(),
                    "Type": r.record_type.strip().upper(),
                    "Address": r.address.strip(),
                    "MXPref": "10",
                    "TTL": r.ttl.strip(),
                }
            )
        raw = await asyncio.to_thread(nc_set_hosts, sld, tld, hosts)
        return {
            "ok": True,
            "action": "replace_all_records",
            "total_hosts_sent": len(hosts),
            "raw_xml_preview": raw[:2000],
        }

    raise HTTPException(status_code=400, detail=f"Unknown action: {body.action}")

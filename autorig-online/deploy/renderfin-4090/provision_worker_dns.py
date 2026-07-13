#!/usr/bin/env python3
"""Safely provision the worker-4090 Renderfin A record through Domain API."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request


DOMAIN = "qwertystock.com"
HOST = "worker-4090.renderfin"
TARGET = "37.187.57.177"


def request(base_url: str, token: str, method: str, path: str, body: dict | None = None) -> dict:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=payload,
        method=method,
        headers={
            "Content-Type": "application/json",
            "X-Qwertystock-Domain-Token": token,
            "X-Qwertystock-Caller": "autorig-renderfin-4090-provision",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:2000]
        raise RuntimeError(f"Domain API HTTP {exc.code} for {path}: {detail}") from exc


def exact_record(zone: dict) -> bool:
    return any(
        str(record.get("name")) == HOST
        and str(record.get("type")).upper() == "A"
        and str(record.get("value")) == TARGET
        for record in zone.get("records", [])
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8095")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    token = os.environ.get("API_TOKEN", "").strip()
    if not token:
        raise SystemExit("API_TOKEN is not set")

    zone_path = f"/domains/{DOMAIN}/records"
    zone = request(args.base_url, token, "GET", zone_path)
    print(f"zone_read hash={zone.get('zone_hash')} records={len(zone.get('records', []))}")
    if exact_record(zone):
        print(f"dns_unchanged {HOST}={TARGET}")
        return 0
    if not args.apply:
        print(f"dns_change_required {HOST}={TARGET}")
        return 2

    request(
        args.base_url,
        token,
        "POST",
        f"/domains/{DOMAIN}/backup",
        {"reason": "before_autorig_renderfin_4090"},
    )
    print("zone_backup_complete")

    change = {
        "records": [
            {"name": HOST, "type": "A", "value": TARGET, "ttl": 300},
        ],
    }
    preview = request(
        args.base_url,
        token,
        "POST",
        f"/domains/{DOMAIN}/records/preview",
        change,
    )
    conflicts = preview.get("diff", {}).get("conflicts", [])
    added = preview.get("diff", {}).get("added", [])
    if not preview.get("ok") or conflicts or len(added) != 1:
        raise RuntimeError(f"unsafe DNS preview: conflicts={len(conflicts)} added={len(added)}")
    print(f"zone_preview hash={preview.get('zone_hash')} added={len(added)} conflicts=0")

    apply_body = {**change, "zone_hash": preview["zone_hash"], "apply": True}
    result = request(
        args.base_url,
        token,
        "POST",
        f"/domains/{DOMAIN}/records/apply?apply=true",
        apply_body,
    )
    print(
        "zone_apply_complete "
        f"old_hash={result.get('old_zone_hash')} new_hash={result.get('new_zone_hash')}"
    )

    verified = request(args.base_url, token, "GET", zone_path)
    if not exact_record(verified):
        raise RuntimeError("DNS verification failed after apply")
    print(f"dns_verified {HOST}={TARGET} hash={verified.get('zone_hash')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Print AutoRig Blender plugin ABCD checkout health and conversion stats.

Run from production:
  cd /root/autorig-online/backend
  /root/autorig-online/venv/bin/python3 scripts/report_blender_plugin_abcd.py
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import text


BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)

ACCESS_LOG_DIR = "/var/log/nginx"
ACCESS_LOG_PREFIX = "access.log"
LOG_RE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] "(?P<method>\S+) (?P<target>\S+) [^"]+" '
    r'(?P<status>\d{3}) \S+ "(?P<referer>[^"]*)" "(?P<ua>[^"]*)"'
)
_ACCESS_EVENTS: list[tuple[datetime, str, str, str]] | None = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _status(command: list[str], timeout: int = 8) -> str:
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except Exception as exc:
        return f"error:{type(exc).__name__}"
    output = (result.stdout or result.stderr or "").strip().splitlines()
    return output[0].strip() if output else f"exit:{result.returncode}"


def _http_status(url: str, timeout: int = 8) -> str:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return str(resp.status)
    except urllib.error.HTTPError as exc:
        return str(exc.code)
    except Exception as exc:
        return f"error:{type(exc).__name__}"


def _disk_summary() -> str:
    usage = shutil.disk_usage("/")
    free_gb = usage.free / (1024**3)
    used_pct = ((usage.total - usage.free) / usage.total) * 100 if usage.total else 0.0
    return f"{free_gb:.2f} GB free, {used_pct:.1f}% used"


def _safe_product_keys() -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    from config import (
        AUTORIG_CRYPTO_TIERS,
        AUTORIG_DONATION_PRODUCT_KEYS,
        BLENDER_PLUGIN_AB_VARIANTS,
        GUMROAD_PRODUCT_CREDITS,
    )

    plugin_products = [(key, int(price)) for key, price in BLENDER_PLUGIN_AB_VARIANTS.items()]
    credit_prices = {str(key).strip().lower(): int(round(float(usd or 0))) for key, _credits, usd in AUTORIG_CRYPTO_TIERS}
    credit_products: list[tuple[str, int]] = []
    for key in GUMROAD_PRODUCT_CREDITS:
        normalized = str(key).strip().lower()
        if normalized in AUTORIG_DONATION_PRODUCT_KEYS:
            credit_products.append((normalized, int(credit_prices.get(normalized, 0))))
    return plugin_products, credit_products


def _access_log_paths() -> list[str]:
    if not os.path.isdir(ACCESS_LOG_DIR):
        return []
    paths = []
    for name in os.listdir(ACCESS_LOG_DIR):
        if name == ACCESS_LOG_PREFIX or name.startswith(f"{ACCESS_LOG_PREFIX}."):
            paths.append(os.path.join(ACCESS_LOG_DIR, name))
    return sorted(paths, key=lambda p: os.path.getmtime(p), reverse=True)


def _open_log(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


def _parse_nginx_time(value: str) -> datetime | None:
    try:
        parsed = datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z")
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _rate(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "n/a"
    return f"{(numerator / denominator) * 100:.1f}%"


def _new_log_stats(products: list[str]) -> dict[str, Any]:
    return {
        "visit_sessions": set(),
        "click_sessions_by_product": {key: set() for key in products},
    }


def _load_access_events(max_hours: int) -> list[tuple[datetime, str, str, str]]:
    plugin_products, credit_products = _safe_product_keys()
    plugin_keys = [key for key, _price in plugin_products]
    credit_keys = [key for key, _credits in credit_products]
    cutoff = _utc_now() - timedelta(hours=int(max_hours))
    cutoff_mtime = cutoff.timestamp() - 86400
    events: list[tuple[datetime, str, str, str]] = []

    for path in _access_log_paths():
        try:
            if os.path.getmtime(path) < cutoff_mtime:
                continue
        except Exception:
            pass
        try:
            fh = _open_log(path)
        except Exception:
            continue
        with fh:
            for line in fh:
                match = LOG_RE.match(line)
                if not match:
                    continue
                ts = _parse_nginx_time(match.group("time"))
                if ts is None:
                    continue
                if ts < cutoff:
                    continue
                try:
                    status = int(match.group("status"))
                except Exception:
                    continue
                if status >= 400:
                    continue
                ua = match.group("ua") or ""
                session_key = f"{match.group('ip')}|{ua}"
                path_only = urlsplit(match.group("target")).path.rstrip("/") or "/"

                if path_only == "/blender-plugin":
                    events.append((ts, "plugin_visit", "", session_key))
                elif path_only == "/buy-credits":
                    events.append((ts, "credit_visit", "", session_key))
                elif path_only == "/blender-plugin/checkout":
                    for key in plugin_keys:
                        events.append((ts, "plugin_click", key, session_key))
                elif path_only.startswith("/buy-credits/checkout/"):
                    key = path_only.rsplit("/", 1)[-1].strip().lower()
                    if key in credit_keys:
                        events.append((ts, "credit_click", key, session_key))
    return events


def _access_log_stats(hours: int) -> dict[str, dict[str, Any]]:
    global _ACCESS_EVENTS
    plugin_products, credit_products = _safe_product_keys()
    plugin_keys = [key for key, _price in plugin_products]
    credit_keys = [key for key, _credits in credit_products]
    stats = {
        "plugin": _new_log_stats(plugin_keys),
        "credits": _new_log_stats(credit_keys),
    }
    cutoff = _utc_now() - timedelta(hours=int(hours))
    if _ACCESS_EVENTS is None:
        _ACCESS_EVENTS = _load_access_events(hours)

    for ts, event_type, product, session_key in _ACCESS_EVENTS:
        if ts < cutoff:
            continue
        if event_type == "plugin_visit":
            stats["plugin"]["visit_sessions"].add(session_key)
        elif event_type == "credit_visit":
            stats["credits"]["visit_sessions"].add(session_key)
        elif event_type == "plugin_click" and product in stats["plugin"]["click_sessions_by_product"]:
            stats["plugin"]["click_sessions_by_product"][product].add(session_key)
        elif event_type == "credit_click" and product in stats["credits"]["click_sessions_by_product"]:
            stats["credits"]["click_sessions_by_product"][product].add(session_key)

    compact: dict[str, dict[str, Any]] = {}
    for kind, raw in stats.items():
        compact[kind] = {
            "visit_sessions": len(raw["visit_sessions"]),
            "click_sessions_by_product": {
                key: len(value) for key, value in raw["click_sessions_by_product"].items()
            },
        }
    return compact


def _journal_counts(hours: int) -> dict[str, int]:
    try:
        result = subprocess.run(
            [
                "journalctl",
                "-u",
                "autorig.service",
                "--since",
                f"{int(hours)} hours ago",
                "--no-pager",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=12,
        )
    except Exception:
        return {"asgi_errors": -1, "payment_errors": -1}
    asgi_errors = 0
    payment_errors = 0
    for line in (result.stdout or "").splitlines():
        lowered = line.lower()
        if "exception in asgi application" in lowered or "runtimeerror: no response returned" in lowered:
            asgi_errors += 1
        paymentish = (
            "gumroad" in lowered
            or "checkout" in lowered
            or "blender-plugin" in lowered
            or "api-gumroad" in lowered
        )
        errorish = (
            "error" in lowered
            or "exception" in lowered
            or "failed" in lowered
            or "traceback" in lowered
        )
        if paymentish and errorish:
            payment_errors += 1
    return {"asgi_errors": asgi_errors, "payment_errors": payment_errors}


async def _purchase_window_stats(
    hours: int,
    *,
    product_kind: str,
    products: list[tuple[str, int]],
) -> dict[str, dict[str, Any]]:
    from database import AsyncSessionLocal

    product_binds = {f"p{i}": key for i, (key, _price) in enumerate(products)}
    in_clause = ", ".join(f":p{i}" for i in range(len(products)))
    cutoff = _utc_now() - timedelta(hours=int(hours))
    bind = {"cutoff": cutoff, **product_binds}

    stats: dict[str, dict[str, Any]] = {
        key: {
            "price_usd": int(price),
            "clicks": 0,
            "click_users": 0,
            "visit_sessions": 0,
            "click_sessions": 0,
            "purchases": 0,
            "purchase_users": 0,
            "refunds": 0,
            "tests": 0,
            "revenue_cents": 0,
        }
        for key, price in products
    }

    async with AsyncSessionLocal() as db:
        click_rows = await db.execute(
            text(
                f"""
                SELECT product_permalink,
                       COUNT(*) AS clicks,
                       COUNT(DISTINCT lower(user_email)) AS click_users
                FROM purchase_checkout_intents
                WHERE product_kind = :product_kind
                  AND product_permalink IN ({in_clause})
                  AND created_at >= :cutoff
                GROUP BY product_permalink
                """
            ),
            {**bind, "product_kind": product_kind},
        )
        for product, clicks, click_users in click_rows:
            if product in stats:
                stats[product]["clicks"] = int(clicks or 0)
                stats[product]["click_users"] = int(click_users or 0)

        purchase_rows = await db.execute(
            text(
                f"""
                SELECT product_permalink,
                       SUM(CASE WHEN COALESCE(test, 0) = 0 AND COALESCE(refunded, 0) = 0 THEN 1 ELSE 0 END) AS purchases,
                       COUNT(DISTINCT CASE WHEN COALESCE(test, 0) = 0 AND COALESCE(refunded, 0) = 0 THEN lower(email) END) AS purchase_users,
                       SUM(CASE WHEN COALESCE(refunded, 0) = 1 THEN 1 ELSE 0 END) AS refunds,
                       SUM(CASE WHEN COALESCE(test, 0) = 1 THEN 1 ELSE 0 END) AS tests,
                       SUM(CASE WHEN COALESCE(test, 0) = 0 AND COALESCE(refunded, 0) = 0 THEN COALESCE(price, 0) ELSE 0 END) AS revenue_cents
                FROM gumroad_purchases
                WHERE product_permalink IN ({in_clause})
                  AND created_at >= :cutoff
                GROUP BY product_permalink
                """
            ),
            bind,
        )
        for product, purchases, purchase_users, refunds, tests, revenue_cents in purchase_rows:
            if product in stats:
                stats[product]["purchases"] = int(purchases or 0)
                stats[product]["purchase_users"] = int(purchase_users or 0)
                stats[product]["refunds"] = int(refunds or 0)
                stats[product]["tests"] = int(tests or 0)
                stats[product]["revenue_cents"] = int(revenue_cents or 0)

    return stats


async def _window_stats(hours: int) -> dict[str, dict[str, dict[str, Any]]]:
    plugin_products, credit_products = _safe_product_keys()
    log_stats = _access_log_stats(hours)
    plugin_stats = await _purchase_window_stats(hours, product_kind="plugin", products=plugin_products)
    credit_stats = await _purchase_window_stats(hours, product_kind="credits", products=credit_products)
    for key, row in plugin_stats.items():
        row["visit_sessions"] = int(log_stats["plugin"]["visit_sessions"])
        row["click_sessions"] = int(log_stats["plugin"]["click_sessions_by_product"].get(key, 0))
    for key, row in credit_stats.items():
        row["visit_sessions"] = int(log_stats["credits"]["visit_sessions"])
        row["click_sessions"] = int(log_stats["credits"]["click_sessions_by_product"].get(key, 0))
    return {"plugin": plugin_stats, "credits": credit_stats}


def _conversion(purchases: int, clicks: int) -> str:
    if clicks <= 0:
        return "n/a"
    return f"{(purchases / clicks) * 100:.1f}%"


def _money(cents: int) -> str:
    return f"${cents / 100:.2f}"


def _print_window(
    label: str,
    title: str,
    stats: dict[str, dict[str, Any]],
    unit: str,
    *,
    shared_click_sessions: bool = False,
) -> None:
    total_visits = max((row["visit_sessions"] for row in stats.values()), default=0)
    if shared_click_sessions:
        total_click_sessions = max((row["click_sessions"] for row in stats.values()), default=0)
    else:
        total_click_sessions = sum(row["click_sessions"] for row in stats.values())
    total_clicks = sum(row["clicks"] for row in stats.values())
    total_purchases = sum(row["purchases"] for row in stats.values())
    total_revenue = sum(row["revenue_cents"] for row in stats.values())
    print(f"\n{title} window: {label}")
    print(f"{unit} | visits | session_clicks | visit_to_click | clicks | buyers | click_to_purchase | revenue | refunds | tests | total_conversion")
    print("--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:")
    for key, row in stats.items():
        print(
            f"{key} (${row['price_usd']}) | {row['visit_sessions']} | {row['click_sessions']} "
            f"| {_rate(row['click_sessions'], row['visit_sessions'])} | {row['clicks']} "
            f"({row['click_users']} users) | {row['purchases']} ({row['purchase_users']} users) "
            f"| {_conversion(row['purchases'], row['clicks'])} | {_money(row['revenue_cents'])} "
            f"| {row['refunds']} | {row['tests']} | {_rate(row['purchases'], row['visit_sessions'])}"
        )
    print(
        f"TOTAL | {total_visits} | {total_click_sessions} | {_rate(total_click_sessions, total_visits)} "
        f"| {total_clicks} | {total_purchases} | {_conversion(total_purchases, total_clicks)} "
        f"| {_money(total_revenue)} | - | - | {_rate(total_purchases, total_visits)}"
    )


async def main() -> int:
    parser = argparse.ArgumentParser(description="AutoRig Blender plugin ABCD monitor report")
    parser.add_argument(
        "--windows",
        nargs="+",
        type=int,
        default=[3, 24, 168],
        help="Hour windows to report. Default: 3 24 168.",
    )
    args = parser.parse_args()
    global _ACCESS_EVENTS
    _ACCESS_EVENTS = _load_access_events(max(args.windows) if args.windows else 168)

    print("AutoRig Blender plugin ABCD monitor")
    print(f"generated_utc: {_utc_now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nHealth")
    print(f"autorig.service: {_status(['systemctl', 'is-active', 'autorig.service'])}")
    print(f"nginx.service: {_status(['systemctl', 'is-active', 'nginx.service'])}")
    print(f"disk: {_disk_summary()}")
    print(f"local_gallery_api_http: {_http_status('http://127.0.0.1:8000/api/gallery?per_page=1&sort=date')}")
    print(f"plugin_offer_api_http: {_http_status('http://127.0.0.1:8000/api/blender-plugin/offer')}")
    print(f"public_plugin_page_http: {_http_status('https://autorig.online/blender-plugin')}")
    log_window = max(1, min(int(args.windows[0]), 24)) if args.windows else 3
    journal = _journal_counts(log_window)
    print(f"journal_{log_window}h_asgi_errors: {journal['asgi_errors']}")
    print(f"journal_{log_window}h_payment_errors: {journal['payment_errors']}")

    for hours in args.windows:
        stats = await _window_stats(hours)
        _print_window(f"last {hours}h", "ABCD plugin", stats["plugin"], "variant", shared_click_sessions=True)
        _print_window(f"last {hours}h", "Credit packages", stats["credits"], "package")

    print("\nData sources")
    print("visits/session_clicks: nginx access.log unique ip+user-agent sessions for sales pages and checkout URLs")
    print("plugin clicks: purchase_checkout_intents where product_kind='plugin'")
    print("credit clicks: purchase_checkout_intents where product_kind='credits'")
    print("purchases: gumroad_purchases for matching product variants, excluding refunded/test rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

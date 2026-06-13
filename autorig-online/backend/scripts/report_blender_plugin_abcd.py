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
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text


BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.chdir(BACKEND)


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


async def _window_stats(hours: int) -> dict[str, dict[str, Any]]:
    from config import BLENDER_PLUGIN_AB_VARIANTS
    from database import AsyncSessionLocal

    variants = list(BLENDER_PLUGIN_AB_VARIANTS.items())
    product_binds = {f"p{i}": key for i, (key, _price) in enumerate(variants)}
    in_clause = ", ".join(f":p{i}" for i in range(len(variants)))
    cutoff = _utc_now() - timedelta(hours=int(hours))
    bind = {"cutoff": cutoff, **product_binds}

    stats: dict[str, dict[str, Any]] = {
        key: {
            "price_usd": int(price),
            "clicks": 0,
            "click_users": 0,
            "purchases": 0,
            "purchase_users": 0,
            "refunds": 0,
            "tests": 0,
            "revenue_cents": 0,
        }
        for key, price in variants
    }

    async with AsyncSessionLocal() as db:
        click_rows = await db.execute(
            text(
                f"""
                SELECT product_permalink,
                       COUNT(*) AS clicks,
                       COUNT(DISTINCT lower(user_email)) AS click_users
                FROM purchase_checkout_intents
                WHERE product_kind = 'plugin'
                  AND product_permalink IN ({in_clause})
                  AND created_at >= :cutoff
                GROUP BY product_permalink
                """
            ),
            bind,
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


def _conversion(purchases: int, clicks: int) -> str:
    if clicks <= 0:
        return "n/a"
    return f"{(purchases / clicks) * 100:.1f}%"


def _money(cents: int) -> str:
    return f"${cents / 100:.2f}"


def _print_window(label: str, stats: dict[str, dict[str, Any]]) -> None:
    total_clicks = sum(row["clicks"] for row in stats.values())
    total_purchases = sum(row["purchases"] for row in stats.values())
    total_revenue = sum(row["revenue_cents"] for row in stats.values())
    print(f"\nABCD window: {label}")
    print("variant | price | clicks | buyers | conv | revenue | refunds | tests")
    print("--- | ---: | ---: | ---: | ---: | ---: | ---: | ---:")
    for key, row in stats.items():
        print(
            f"{key} | ${row['price_usd']} | {row['clicks']} "
            f"({row['click_users']} users) | {row['purchases']} "
            f"({row['purchase_users']} users) | {_conversion(row['purchases'], row['clicks'])} "
            f"| {_money(row['revenue_cents'])} | {row['refunds']} | {row['tests']}"
        )
    print(
        f"TOTAL | - | {total_clicks} | {total_purchases} | "
        f"{_conversion(total_purchases, total_clicks)} | {_money(total_revenue)} | - | -"
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
        _print_window(f"last {hours}h", await _window_stats(hours))

    print("\nData sources")
    print("clicks: purchase_checkout_intents where product_kind='plugin'")
    print("purchases: gumroad_purchases for blender-plugin variants, excluding refunded/test rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

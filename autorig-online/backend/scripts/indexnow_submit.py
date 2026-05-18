#!/usr/bin/env python3
"""Submit changed AutoRig URLs to IndexNow."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable


DEFAULT_HOST = "autorig.online"
DEFAULT_KEY = "793f81f63218433f87e43c0afd353c14"
DEFAULT_ENDPOINT = "https://api.indexnow.org/indexnow"


def parse_sitemap(path: Path, limit: int | None = None) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(path)

    urls: list[str] = []
    seen: set[str] = set()

    def add(url: str) -> None:
        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    def read_one(sitemap_path: Path) -> None:
        tree = ET.parse(sitemap_path)
        root = tree.getroot()
        namespace = ""
        if root.tag.startswith("{"):
            namespace = root.tag.split("}", 1)[0] + "}"

        if root.tag.endswith("sitemapindex"):
            for loc in root.findall(f".//{namespace}sitemap/{namespace}loc"):
                if loc.text:
                    local_child = sitemap_path.parent / Path(loc.text.strip()).name
                    if local_child.exists():
                        read_one(local_child)
                if limit and len(urls) >= limit:
                    return
            return

        for loc in root.findall(f".//{namespace}url/{namespace}loc"):
            if loc.text:
                add(loc.text.strip())
            if limit and len(urls) >= limit:
                return

    read_one(path)
    return urls[:limit] if limit else urls


def normalize_urls(urls: Iterable[str], host: str) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for url in urls:
        url = url.strip()
        if not url:
            continue
        if url.startswith("/"):
            url = f"https://{host}{url}"
        if url not in seen:
            seen.add(url)
            normalized.append(url)
    return normalized


def submit_indexnow(
    *,
    endpoint: str,
    host: str,
    key: str,
    key_location: str,
    urls: list[str],
    dry_run: bool,
) -> int:
    payload = {
        "host": host,
        "key": key,
        "keyLocation": key_location,
        "urlList": urls,
    }
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

    if dry_run:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    request = urllib.request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            print(f"IndexNow status: {response.status}")
            if response_body:
                print(response_body)
            return 0 if response.status < 400 else 1
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        print(f"IndexNow status: {exc.code}", file=sys.stderr)
        if response_body:
            print(response_body, file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("urls", nargs="*", help="Absolute URLs or root-relative paths to submit.")
    parser.add_argument("--from-sitemap", type=Path, help="Read URLs from a local sitemap XML file.")
    parser.add_argument("--limit", type=int, help="Submit only the first N URLs.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--key", default=DEFAULT_KEY)
    parser.add_argument("--key-location")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    urls: list[str] = []
    if args.from_sitemap:
        urls.extend(parse_sitemap(args.from_sitemap, args.limit))
    urls.extend(args.urls)
    urls = normalize_urls(urls, args.host)
    if args.limit:
        urls = urls[: args.limit]
    if not urls:
        parser.error("provide URLs or --from-sitemap")

    key_location = args.key_location or f"https://{args.host}/{args.key}.txt"
    return submit_indexnow(
        endpoint=args.endpoint,
        host=args.host,
        key=args.key,
        key_location=key_location,
        urls=urls,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())

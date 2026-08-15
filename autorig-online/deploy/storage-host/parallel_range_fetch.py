#!/usr/bin/env python3
"""Resume a large migration download with verified parallel HTTP ranges."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import os
import re
import shutil
import time
import urllib.request
from pathlib import Path


MIB = 1024 * 1024


def request_range(url: str, start: int, end: int, *, timeout: int = 180) -> tuple[bytes, str]:
    request = urllib.request.Request(
        url,
        headers={"Range": f"bytes={start}-{end}", "Accept-Encoding": "identity"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        return body, str(response.headers.get("Content-Range") or "")


def probe_size(url: str) -> int:
    body, content_range = request_range(url, 0, 0)
    match = re.fullmatch(r"bytes 0-0/(\d+)", content_range, re.IGNORECASE)
    if len(body) != 1 or not match:
        raise RuntimeError("source did not return a valid bytes=0-0 response")
    size = int(match.group(1))
    if size <= 0:
        raise RuntimeError("source artifact is empty")
    return size


def fetch_part(
    url: str,
    part: Path,
    start: int,
    end: int,
    total: int,
    attempts: int,
) -> None:
    expected = end - start + 1
    if part.is_file() and part.stat().st_size == expected:
        return
    temporary = part.with_name(part.name + ".tmp")
    for attempt in range(1, attempts + 1):
        try:
            body, content_range = request_range(url, start, end)
            expected_range = f"bytes {start}-{end}/{total}"
            if len(body) != expected or content_range.lower() != expected_range:
                raise RuntimeError(
                    f"invalid range response {content_range!r}: {len(body)} != {expected}"
                )
            temporary.write_bytes(body)
            os.replace(temporary, part)
            return
        except Exception:
            temporary.unlink(missing_ok=True)
            if attempt >= attempts:
                raise
            time.sleep(min(30, 2**attempt))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("destination", type=Path)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--chunk-mib", type=int, default=64)
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--expected-sha256", default="")
    args = parser.parse_args()

    size = probe_size(args.url)
    chunk = max(8, args.chunk_mib) * MIB
    ranges = [
        (index, start, min(size - 1, start + chunk - 1))
        for index, start in enumerate(range(0, size, chunk))
    ]
    destination = args.destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    parts = destination.with_name(destination.name + ".parts")
    parts.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(64, args.connections))
    ) as executor:
        futures = {
            executor.submit(
                fetch_part,
                args.url,
                parts / f"{index:06d}.part",
                start,
                end,
                size,
                max(1, args.attempts),
            ): (index, start, end)
            for index, start, end in ranges
        }
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            future.result()
            completed += 1
            if completed == len(ranges) or completed % max(1, len(ranges) // 20) == 0:
                elapsed = max(0.001, time.monotonic() - started)
                cached = sum(path.stat().st_size for path in parts.glob("*.part"))
                print(
                    f"parts={completed}/{len(ranges)} bytes={cached}/{size} "
                    f"rate_mib_s={cached / MIB / elapsed:.2f}",
                    flush=True,
                )

    assembling = destination.with_name(destination.name + ".assembling")
    digest = hashlib.sha256()
    with assembling.open("wb") as output:
        for index, start, end in ranges:
            part = parts / f"{index:06d}.part"
            expected = end - start + 1
            if part.stat().st_size != expected:
                raise RuntimeError(f"part {index} has the wrong size")
            with part.open("rb") as stream:
                while True:
                    block = stream.read(8 * MIB)
                    if not block:
                        break
                    output.write(block)
                    digest.update(block)
        output.flush()
        os.fsync(output.fileno())
    if assembling.stat().st_size != size:
        raise RuntimeError("assembled artifact has the wrong size")
    actual_sha = digest.hexdigest()
    if args.expected_sha256 and actual_sha.lower() != args.expected_sha256.lower():
        raise RuntimeError(
            f"SHA-256 mismatch: expected {args.expected_sha256}, got {actual_sha}"
        )
    os.replace(assembling, destination)
    shutil.rmtree(parts)
    print(f"ready path={destination} size={size} sha256={actual_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

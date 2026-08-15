#!/usr/bin/env python3
"""Create a consistent SQLite snapshot using the SQLite backup API."""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()

    source = args.source.resolve(strict=True)
    destination = args.destination.resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    if temporary.exists():
        temporary.unlink()

    source_db = sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=60)
    target_db = sqlite3.connect(temporary, timeout=60)
    try:
        source_db.backup(target_db, pages=4096, sleep=0.05)
        row = target_db.execute("PRAGMA quick_check").fetchone()
        if not row or row[0] != "ok":
            raise RuntimeError(f"snapshot quick_check failed: {row!r}")
    finally:
        target_db.close()
        source_db.close()

    os.replace(temporary, destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

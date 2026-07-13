#!/usr/bin/env python3
"""Install one reverse-forward-only SSH key for the AutoRig 4090 worker."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("public_key", type=Path)
    parser.add_argument("--authorized-keys", type=Path, default=Path.home() / ".ssh" / "authorized_keys")
    parser.add_argument("--listen", default="localhost:19409")
    parser.add_argument("--marker", default="secs-autorig-4090")
    args = parser.parse_args()

    parts = args.public_key.read_text(encoding="utf-8").strip().split()
    if len(parts) < 2 or parts[0] not in {"ssh-ed25519", "ssh-rsa", "ecdsa-sha2-nistp256"}:
        raise SystemExit("invalid SSH public key")

    key_type, key_data = parts[:2]
    authorized_keys = args.authorized_keys.expanduser().resolve()
    existing = authorized_keys.read_text(encoding="utf-8").splitlines()
    options = (
        'command="/usr/bin/sleep infinity",restrict,port-forwarding,'
        f'permitlisten="{args.listen}"'
    )
    new_line = f"{options} {key_type} {key_data} {args.marker}"
    matching = [index for index, line in enumerate(existing) if args.marker in line]

    if len(matching) == 1 and existing[matching[0]] == new_line:
        print("restricted_key_unchanged")
        return 0
    if len(matching) > 1:
        raise SystemExit(f"refusing to replace duplicate marker {args.marker!r}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = authorized_keys.with_name(f"{authorized_keys.name}.bak.{stamp}")
    shutil.copy2(authorized_keys, backup)
    if matching:
        existing[matching[0]] = new_line
        state = "replaced"
    else:
        existing.append(new_line)
        state = "installed"
    authorized_keys.write_text("\n".join(existing) + "\n", encoding="utf-8")
    os.chmod(authorized_keys, 0o600)
    print(f"restricted_key_{state} backup={backup.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Install ComfyUI requirement distributions from a farm LAN peer.

The farm WAN is shared by every agent's downloads and can drop to ~20 KB/s,
so pip against PyPI is not an option. A peer with the same Python minor
version (3.12) packs its installed distributions, one zip per distribution
(paths relative to site-packages) plus index.json, into a folder its LAN model
server exposes. This script installs only distributions this interpreter does
not already have, following the peer's dependency graph, and never replaces an
installed one. Run with the target interpreter:

    python comfy1080_lan_deps.py <peer url of the transfer folder> <root dist> ...
"""
import importlib.metadata as md
import io
import json
import re
import sys
import sysconfig
import urllib.request
import zipfile


def norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def have(name: str) -> bool:
    try:
        md.distribution(name)
        return True
    except md.PackageNotFoundError:
        return False


def main() -> int:
    peer = sys.argv[1].rstrip("/")
    roots = [norm(n) for n in sys.argv[2:]]
    index = json.loads(urllib.request.urlopen(f"{peer}/index.json", timeout=30).read())
    site = sysconfig.get_paths()["purelib"]
    todo, seen, installed = list(roots), set(), []
    while todo:
        key = todo.pop()
        if key in seen:
            continue
        seen.add(key)
        entry = index.get(key)
        if entry is None:
            print(f"not packed by peer: {key}")
            continue
        if have(entry["name"]) or have(key):
            continue
        data = urllib.request.urlopen(f"{peer}/{key}.zip", timeout=600).read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(site)
        installed.append(f"{entry['name']}=={entry['version']}")
        todo.extend(entry.get("requires") or [])
    print("installed: " + (", ".join(installed) or "nothing"))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Merge the public chat's pchat_* strings into a site dictionary (Public chat · V3).

    python3 merge_i18n.py <i18n-pchat.json> <lang> <current static/i18n/<lang>.json> <output file>

Reads the CURRENT production dictionary (overlay first, then the release), adds or updates only the
pchat_* keys and writes it back in the site's own format (indent 2, UTF-8, trailing newline), so the
other agents' keys stay byte for byte as they are.
"""
import json
import sys


def main() -> None:
    pack_path, lang, src, out = sys.argv[1:5]
    pack = json.load(open(pack_path, encoding="utf-8"))[lang]
    raw = open(src, encoding="utf-8").read()
    data = json.loads(raw)
    if json.dumps(data, indent=2, ensure_ascii=False) + "\n" != raw:
        print(f"warning: {src} is not in the canonical format; it will be normalized", file=sys.stderr)
    changed = 0
    for key, value in pack.items():
        if not key.startswith("pchat_"):
            raise SystemExit(f"refusing non-pchat key {key}")
        if data.get(key) != value:
            data[key] = value
            changed += 1
    with open(out, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    print(f"{lang}: {changed} pchat keys added/updated, {len(data)} keys total")


if __name__ == "__main__":
    main()

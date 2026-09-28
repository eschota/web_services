#!/usr/bin/env python3
"""Cache-bust every /static script and stylesheet of a page by its content.

    version_assets.py <static dir> <page.html> [<page.html> ...]

Each src="/static/..." / href="/static/..." (.js, .css) gets ?v=<first 10 hex
of the file's SHA-1>, replacing any hand-written ?v=. Static files are served
`immutable` for a week, so a module changed without a new ?v= stayed stale in
browsers next to a newer ai-nodes.js (owner, 2026-09-28). Run it on the new
release before switching; it prints what changed.
"""
import hashlib
import pathlib
import re
import sys

PATTERN = re.compile(r'((?:src|href)=")(/static/[^"?#]+\.(?:js|css))(\?v=[^"]*)?(")')


def main() -> int:
    static = pathlib.Path(sys.argv[1])
    for page in sys.argv[2:]:
        path = pathlib.Path(page)
        text = path.read_text(encoding="utf-8")
        changed = []

        def swap(match: "re.Match[str]") -> str:
            file = static / match.group(2)[len("/static/"):]
            if not file.is_file():
                return match.group(0)
            digest = hashlib.sha1(file.read_bytes()).hexdigest()[:10]
            new = "?v=" + digest
            if (match.group(3) or "") != new:
                changed.append(match.group(2) + " " + new)
            return match.group(1) + match.group(2) + new + match.group(4)

        text = PATTERN.sub(swap, text)
        path.write_text(text, encoding="utf-8")
        print(page + ": " + (str(len(changed)) + " versioned" if changed else "unchanged"))
        for line in changed:
            print("  " + line)
    return 0


if __name__ == "__main__":
    sys.exit(main())

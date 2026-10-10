"""Add the public chat to the V3 task page template at two exact anchors (Public chat · V3).

    python3 patch_task_page.py <current task-v3.html> <output file>

Idempotent: a template that already carries the chat is written back unchanged. Only these two lines are
added; everything else in the shared template (Task page · V3, Viewer · V3, Scenes · V3) stays as it is.
"""
import sys

CSS_ANCHOR = '<link rel="stylesheet" href="/static/css/task-v3.css">'
CSS_LINE = '<link rel="stylesheet" href="/static/css/public-chat.css">'
JS_ANCHOR = '<script type="module" src="/static/js/task-v3-shell.js"></script>'
JS_LINE = '<script defer src="/static/js/public-chat.js"></script>'


def patch(html: str) -> str:
    for anchor, line in ((CSS_ANCHOR, CSS_LINE), (JS_ANCHOR, JS_LINE)):
        if line in html:
            continue
        if html.count(anchor) != 1:
            raise SystemExit(f"anchor not found exactly once: {anchor}")
        start = html.index(anchor)
        indent = html[html.rfind("\n", 0, start) + 1:start]
        html = html.replace(anchor, f"{anchor}\n{indent}{line}", 1)
    return html


def main() -> None:
    src, out = sys.argv[1:3]
    html = open(src, encoding="utf-8").read()
    result = patch(html)
    with open(out, "w", encoding="utf-8", newline="") as handle:
        handle.write(result)
    print("changed" if result != html else "unchanged")


if __name__ == "__main__":
    main()

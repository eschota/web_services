#!/usr/bin/env python3
"""
One-off: replace inline <header> / optional free3d / <footer class="site-footer"> with
#site-header / #site-footer and inject layout scripts before i18n.js.
Run from repo: python3 autorig-online/scripts/migrate_site_layout_html.py
"""
import re
from pathlib import Path

STATIC = Path(__file__).resolve().parent.parent / "static"
VERSION = "20260324-sitelayout"
LAYOUT_SCRIPTS = f"""    <script src="/static/js/header.js?v={VERSION}"></script>
    <script src="/static/js/footer.js?v={VERSION}"></script>
    <script src="/static/js/site-layout.js?v={VERSION}"></script>
"""

# Default: no Free3D ribbon; pathname-driven active nav
BODY_ATTRS = ' data-layout-free3d-init="none"'


def strip_header(html: str) -> str:
    return re.sub(
        r"<header\s+class=\"header\"[^>]*>.*?</header>\s*",
        "",
        html,
        count=1,
        flags=re.DOTALL,
    )


def strip_free3d(html: str) -> str:
    return re.sub(
        r"<section\s+class=\"free3d-search[^\"]*\"[^>]*>.*?</section>\s*",
        "",
        html,
        count=1,
        flags=re.DOTALL,
    )


def strip_footer(html: str) -> str:
    return re.sub(
        r"<footer\s+class=\"site-footer\"[^>]*>.*?</footer>\s*",
        "",
        html,
        count=1,
        flags=re.DOTALL,
    )


def ensure_body_attrs(html: str) -> str:
    def repl(m):
        tag = m.group(0)
        if "data-layout-free3d-ribbon" in tag:
            return tag
        if tag.rstrip().endswith(">"):
            return tag[:-1] + BODY_ATTRS + ">"
        return tag

    return re.sub(r"<body[^>]*>", repl, html, count=1)


def ensure_site_header_after_body(html: str) -> str:
    if 'id="site-header"' in html:
        return html

    def inject(m):
        return m.group(0) + "\n    <div id=\"site-header\"></div>\n"

    return re.sub(r"(<body[^>]*>)", inject, html, count=1)


def replace_footer_placeholder(html: str) -> str:
    if 'id="site-footer"' in html:
        return html
    if "<footer" in html:
        raise ValueError("footer still present")
    if "</main>" in html:
        return html.replace("</main>", "</main>\n    <div id=\"site-footer\"></div>", 1)
    if "</body>" in html:
        return html.replace("</body>", "    <div id=\"site-footer\"></div>\n</body>", 1)
    raise ValueError("no </main> or </body>")


def inject_layout_before_i18n(html: str) -> str:
    if f"site-layout.js?v={VERSION}" in html:
        return html
    m = re.search(
        r'(<script\s+src="/static/js/i18n\.js[^"]*"\s*>\s*</script>|<script\s+src="/static/js/i18n\.js[^"]*"\s*>)',
        html,
    )
    if not m:
        raise ValueError("no i18n.js script tag found")
    return html[: m.start()] + LAYOUT_SCRIPTS + html[m.start() :]


def process_file(path: Path) -> bool:
    raw = path.read_text(encoding="utf-8")
    if path.name in ("index.html", "task.html"):
        return False
    if 'id="site-header"' in raw and f"site-layout.js?v={VERSION}" in raw:
        return False

    html = raw
    html = strip_header(html)
    html = strip_free3d(html)
    html = strip_footer(html)
    html = ensure_body_attrs(html)
    html = ensure_site_header_after_body(html)
    html = replace_footer_placeholder(html)
    html = inject_layout_before_i18n(html)

    if html != raw:
        path.write_text(html, encoding="utf-8")
        return True
    return False


def main():
    changed = []
    for path in sorted(STATIC.glob("*.html")):
        try:
            if process_file(path):
                changed.append(path.name)
        except Exception as e:
            print("SKIP", path.name, e)
    for c in changed:
        print("OK", c)
    print("Total:", len(changed))


if __name__ == "__main__":
    main()

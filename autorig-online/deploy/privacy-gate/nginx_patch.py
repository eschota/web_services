"""Add the privacy gate to the autorig.online nginx site (owner 2026-10-17). Idempotent; exact anchors."""
import sys
P = sys.argv[1]
s = open(P, encoding="utf-8").read()
if "_autorig_privacy_gate" in s:
    print("already patched"); sys.exit(0)
GATE = """    # Private models (2026-10-17, backend/task_privacy.py): files nginx serves itself (/u/ sources, /pw/ posters,
    # /api/mt/ run files) are checked by the privacy gate; a private task is 404 for everyone but its owner / admins.
    location = /_autorig_privacy_gate {
        internal;
        proxy_pass http://127.0.0.1:8292/check;
        proxy_pass_request_body off;
        proxy_set_header Content-Length "";
        proxy_set_header X-Original-URI $request_uri;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_connect_timeout 2s;
        proxy_read_timeout 5s;
    }
    location @autorig_private_404 {
        add_header Cache-Control "no-store" always;
        add_header X-Robots-Tag "noindex, nofollow" always;
        return 404;
    }
"""
AUTH = ("        auth_request /_autorig_privacy_gate;\n"
        "        error_page 403 = @autorig_private_404;\n")
def sub(old, new):
    global s
    if s.count(old) != 1:
        sys.exit(f"ANCHOR x{s.count(old)}: {old!r}")
    s = s.replace(old, new)
sub("    # Gallery perf (2026-10-15): small WebP card posters, content-versioned by ?v=<mtime>.\n    location ^~ /pw/ {\n",
    GATE + "    # Gallery perf (2026-10-15): small WebP card posters, content-versioned by ?v=<mtime>.\n    location ^~ /pw/ {\n" + AUTH)
sub("    location /u/ {\n        alias /srv/autorig/data/var/uploads/;\n",
    "    location /u/ {\n" + AUTH + "        alias /srv/autorig/data/var/uploads/;\n")
sub("    location ^~ /api/mt/ {\n",
    "    location ^~ /api/mt/ {\n" + AUTH)
open(P + ".new", "w", encoding="utf-8").write(s)
print("patched ->", P + ".new")

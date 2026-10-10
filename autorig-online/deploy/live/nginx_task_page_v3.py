#!/usr/bin/env python3
"""Patch the autorig.online nginx site for the live task page (Task page · V3, 2026-10-10).

* /static/ is served from the live overlay first, then the current release;
* JS/CSS with an exact 10-hex content stamp stay immutable, every other JS/CSS
  revalidates on each load (no-cache + ETag/Last-Modified); other assets keep
  their week-long immutable caching;
* /api/task-viewer/ (the Unity viewer feed) gets the same read rate limit as /api/mt/.
"""
import datetime
import shutil
import sys
from pathlib import Path

SITE = Path("/etc/nginx/sites-available/autorig.online-storage")
BACKUP_DIR = Path("/srv/autorig/config-backups")
MARK = "autorig_static_cache_control"

MAPS = '''
# Task page V3 / live static (2026-10-10): JS and CSS whose ?v= is an exact
# 10-hex content stamp are immutable; any other JS/CSS revalidates on every load
# (no-cache + ETag/Last-Modified), so a live edit reaches the next page load.
map $uri $autorig_static_code {
    "~*\\.(?:m?js|css)$" 1;
    default 0;
}
map "$autorig_static_code:$arg_v" $autorig_static_cache_control {
    "~^1:[0-9a-f]{10}$" "public, max-age=604800, immutable";
    "~^1:" "no-cache";
    default "public, max-age=604800, immutable";
}
'''

OLD_STATIC = '''    location /static/ {
        alias /srv/autorig/current/autorig-online/static/;
        expires 7d;
        add_header Cache-Control "public, immutable";
    }
'''

NEW_STATIC = '''    # Live static (Task page · V3, 2026-10-10): the atomic overlay
    # /srv/autorig/live/static first (sudo python3 /srv/autorig/tools/live_static.py),
    # then the current release.
    location /static/ {
        root /srv/autorig;
        try_files /live$uri /current/autorig-online$uri =404;
        add_header Cache-Control $autorig_static_cache_control;
    }
'''

VIEWER_ANCHOR = '''    location = /api/mt {
'''

VIEWER = '''    # Unity viewer feed for a task's own models (task_page_v3_routes.py): the
    # viewer reads a burst of files at load, like /api/mt/.
    location ^~ /api/task-viewer/ {
        limit_req zone=autorig_ai_read_limit burst=300 nodelay;
        limit_req_status 429;
        proxy_pass http://127.0.0.1:8200;
        error_page 502 504 = @autorig_restarting_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 60s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }
'''

ZONE_ANCHOR = "limit_req_zone $binary_remote_addr zone=autorig_upload_limit:10m rate=2r/s;\n"


def main() -> int:
    text = SITE.read_text(encoding="utf-8")
    if MARK in text:
        print("already patched")
        return 0
    for anchor in (OLD_STATIC, VIEWER_ANCHOR, ZONE_ANCHOR):
        if text.count(anchor) != 1:
            print(f"anchor count {text.count(anchor)}: {anchor[:60]!r}")
            return 2
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = BACKUP_DIR / f"autorig.online-storage.pre-task-page-v3-{stamp}"
    shutil.copy2(SITE, backup)
    text = text.replace(ZONE_ANCHOR, ZONE_ANCHOR + MAPS, 1)
    text = text.replace(OLD_STATIC, NEW_STATIC, 1)
    text = text.replace(VIEWER_ANCHOR, VIEWER + VIEWER_ANCHOR, 1)
    tmp = SITE.with_name(SITE.name + ".task-page-v3.tmp")
    tmp.write_text(text, encoding="utf-8")
    shutil.copymode(SITE, tmp)
    tmp.replace(SITE)
    print(f"patched; backup {backup}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

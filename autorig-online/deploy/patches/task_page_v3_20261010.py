#!/usr/bin/env python3
"""Anchor patch for backend/main.py: the live task page and the V3 task view.

    python3 task_page_v3_20261010.py <path/to/main.py>

Idempotent and exact: every anchor must occur once, or nothing is written.
Works on the production copy, the Git HEAD blob and a working tree with other
agents' WIP alike (LF or CRLF).  Task page · V3 agent, 2026-10-10.
"""
import re
import sys
from pathlib import Path

MARK = "task_page_live.render_task_html"

PATCHES = [
    (
        '''def _task_html_response(html_content: str) -> HTMLResponse:
    return HTMLResponse(
        content=_inject_static_layout(html_content),
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
''',
        '''def _task_html_response(html_content: str, request: Optional[Request] = None) -> HTMLResponse:
    # Task page V3 (2026-10-10): the layout partials and every /static JS/CSS
    # reference are resolved per request from the live static roots (overlay,
    # then the current release) and content-stamped, so an agent's atomic live
    # edit reaches the next task page load without a restart (task_page_live.py).
    response = HTMLResponse(
        content=task_page_live.render_task_html(html_content),
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )
    return task_page_live.apply_switch_cookie(response, request)
''',
    ),
    (
        '''@app.get("/task")
async def task_page(
    id: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Serve task page with dynamic OG meta tags for Telegram/social sharing"""

    # Read base template
    task_html_path = STATIC_DIR / "task.html"
    html_content = task_html_path.read_text(encoding="utf-8")
''',
        '''@app.get("/task")
async def task_page(
    request: Request,
    id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    user: Optional[User] = Depends(get_current_user),
):
    """Serve task page with dynamic OG meta tags for Telegram/social sharing"""

    # Read base template: the classic page or the V3 Unity-viewer shell, chosen
    # per request (live rollout file + ?v3= switch), from the live static roots.
    html_content = await task_page_live.task_template(
        request, id, user, db, is_admin_email=is_admin_email,
    )
''',
    ),
    (
        '''        return HTMLResponse(content=_inject_static_layout(html_content))

    title_suffix = f" | AutoRig task {task_id[:8]}"''',
        '''        return HTMLResponse(content=task_page_live.render_task_html(html_content))

    title_suffix = f" | AutoRig task {task_id[:8]}"''',
    ),
    (
        '''    return _task_html_response(html_content)


@app.post("/api/task/{task_id}/purchase-intent")''',
        '''    return _task_html_response(html_content, request)


@app.post("/api/task/{task_id}/purchase-intent")''',
    ),
    (
        '''# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
''',
        '''# Task page V3 (Task page · V3 agent, 2026-10-10): the live task page, its
# state API and the feed that opens a task's own models in the Unity viewer.
import task_page_live
from task_page_v3_routes import build_task_page_v3_router

app.include_router(
    build_task_page_v3_router(
        get_db=get_db,
        get_current_user=get_current_user,
        task_model=Task,
        is_admin_email=is_admin_email,
        glb_cache_dir=GLB_CACHE_DIR,
    )
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
''',
    ),
]


def _anchor(old: str) -> "re.Pattern[str]":
    # Lines match exactly up to trailing blanks, which editors strip or keep, and
    # either line ending: a shared file can mix LF and CRLF, so nothing else in
    # it is re-encoded.
    lines = old.split("\n")
    parts = [re.escape(line.rstrip(" \t")) + "[ \t]*" for line in lines[:-1]]
    parts.append(re.escape(lines[-1]))
    return re.compile("\r?\n".join(parts))


def patch(text: str) -> str:
    if MARK in text:
        return text
    for old, new in PATCHES:
        found = list(_anchor(old).finditer(text))
        if len(found) != 1:
            raise SystemExit(f"anchor found {len(found)} times:\n{old[:160]}")
        start, end = found[0].span()
        replacement = new.replace("\n", "\r\n") if "\r\n" in found[0].group(0) else new
        text = text[:start] + replacement + text[end:]
    return text


def main() -> None:
    path = Path(sys.argv[1])
    raw = path.read_bytes().decode("utf-8")
    patched = patch(raw)
    if patched == raw:
        print(f"{path}: already patched")
        return
    path.write_bytes(patched.encode("utf-8"))
    print(f"{path}: patched")


if __name__ == "__main__":
    main()

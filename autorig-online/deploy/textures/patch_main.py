"""Texturing · V3: mount backend/v3_textures.py routes in main.py (idempotent). python patch_main.py <main.py>"""
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
raw = p.read_bytes()
crlf = b"\r\n" in raw
s = raw.decode("utf-8").replace("\r\n", "\n")
if "build_v3_textures_router" in s:
    print("already patched")
    sys.exit(0)
anchor = '''# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
'''
assert s.count(anchor) == 1, "anchor"
block = '''# Texturing · V3 (2026-10-11, «Every Model Gets Textures»): the texture status of a V3 task and the upload of the
# missing texture / material files (or a ZIP) into the same task, which then runs a new attempt with them.
from v3_textures import build_v3_textures_router

app.include_router(
    build_v3_textures_router(
        get_db=get_db,
        get_current_user=get_current_user,
        task_model=Task,
        is_admin_email=is_admin_email,
        effective_anon_id=_effective_anon_id,
    )
)

'''
s = s.replace(anchor, block + anchor, 1)
p.write_bytes((s.replace("\n", "\r\n") if crlf else s).encode("utf-8"))
print("patched")

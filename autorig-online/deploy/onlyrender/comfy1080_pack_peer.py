"""Pack ComfyUI 0.37.0 requirement distributions on a farm peer (f15) for comfy1080_lan_deps.py.

Run with the peer's python_embeded: python comfy1080_pack_peer.py <models dir>\_comfy1080_transfer
Writes one zip per distribution (files from its RECORD, paths relative to
site-packages) plus index.json; add comfyui-v0.37.0.zip with
`git archive --format=zip -o <dir>\comfyui-v0.37.0.zip v0.37.0`. Delete the folder afterwards.
"""
import importlib.metadata as md, json, os, re, sys, zipfile

OUT = sys.argv[1]
ROOTS = ["comfyui-frontend-package", "comfyui-workflow-templates", "comfyui-embedded-docs", "torchsde",
         "alembic", "SQLAlchemy", "av", "comfy-kitchen", "comfy-aimdo", "simpleeval", "blake3", "spandrel",
         "pydantic-settings", "PyOpenGL", "comfy-angle"]
SKIP = re.compile(r"^(torch|torchvision|torchaudio|triton|nvidia-.*)$", re.I)
os.makedirs(OUT, exist_ok=True)


def norm(n):
    return re.sub(r"[-_.]+", "-", n).lower()


def reqs(dist):
    out = []
    for r in dist.requires or []:
        if ";" in r and "extra" in r.split(";", 1)[1]:
            continue
        if ";" in r:
            marker = r.split(";", 1)[1]
            if "sys_platform" in marker and "win32" not in marker and "!=" not in marker:
                continue
            if "platform_system" in marker and "Windows" not in marker and "!=" not in marker:
                continue
        name = re.split(r"[\s;<>=!~\[(]", r.strip(), 1)[0]
        if name:
            out.append(name)
    return out


index, todo, seen = {}, list(ROOTS), set()
while todo:
    name = todo.pop()
    key = norm(name)
    if key in seen or SKIP.match(name):
        continue
    seen.add(key)
    try:
        dist = md.distribution(name)
    except md.PackageNotFoundError:
        print("absent on f15:", name)
        continue
    deps = reqs(dist)
    index[key] = {"name": dist.metadata["Name"], "version": dist.version, "requires": [norm(d) for d in deps]}
    base = dist.locate_file("")
    zpath = os.path.join(OUT, key + ".zip")
    n = 0
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as z:
        for f in dist.files or []:
            p = str(f)
            if p.startswith("..") or "__pycache__" in p or p.endswith(".pyc"):
                continue
            full = os.path.join(base, p)
            if os.path.isfile(full):
                z.write(full, p)
                n += 1
    index[key]["files"] = n
    index[key]["bytes"] = os.path.getsize(zpath)
    todo.extend(deps)
with open(os.path.join(OUT, "index.json"), "w") as fh:
    json.dump(index, fh, indent=1)
print(json.dumps({k: (v["version"], v["bytes"]) for k, v in index.items()}))

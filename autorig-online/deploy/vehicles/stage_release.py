"""Stage only the vehicle delta over fresh production; never promote automatically."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def replace_once(text: str, old: str, new: str):
    if text.count(old) != 1:
        raise RuntimeError(f"Expected one deployment anchor, found {text.count(old)}: {old[:100]}")
    return text.replace(old, new, 1)


def stage(bundle: Path):
    current = Path("/srv/autorig/current").resolve(strict=True)
    expected = json.loads((bundle / "baseline.json").read_text())
    for relative, wanted in expected.items():
        if digest(current / "autorig-online" / relative) != wanted.lower():
            raise RuntimeError(f"Production changed; rebase the narrow vehicle patch: {relative}")
    target = current.parent / ("vehicle-v3-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    shutil.copytree(current, target, copy_function=os.link, symlinks=True)
    before = {}
    after = {}

    def install(relative: str, data: bytes):
        path = target / "autorig-online" / relative
        before[relative] = digest(path) if path.is_file() else None
        write(path, data)
        after[relative] = digest(path)

    main_path = "backend/main.py"
    main = (target / "autorig-online" / main_path).read_text()
    main = replace_once(main, "from ai_pipelines_api import router as ai_pipelines_router",
                        "from ai_pipelines_api import router as ai_pipelines_router\nfrom vehicle_generation import build_vehicle_router")
    main = replace_once(main, "app.include_router(build_avatar_router(get_avatar_owner))",
                        "app.include_router(build_avatar_router(get_avatar_owner))\napp.include_router(build_vehicle_router(get_avatar_owner))")
    install(main_path, main.encode())
    catalogue_path = "backend/ai_services.py"
    catalogue = (target / "autorig-online" / catalogue_path).read_text()
    catalogue += ("\n# Vehicle stages extend the existing typed catalogue.\n"
                  "from vehicle_generation.catalogue import install as _install_vehicle_catalogue\n"
                  "import sys as _vehicle_catalogue_sys\n"
                  "_install_vehicle_catalogue(_vehicle_catalogue_sys.modules[__name__])\n")
    install(catalogue_path, catalogue.encode())
    graph_path = "backend/ai_graph.py"
    graph = (target / "autorig-online" / graph_path).read_text()
    graph = replace_once(graph, "def templates() -> List[Dict[str, object]]:\n",
                         "def templates() -> List[Dict[str, object]]:\n    from vehicle_generation.catalogue import graph_template as vehicle_graph_template\n")
    start = graph.index("def templates()")
    end = graph.index("\n\n", start)
    method = graph[start:end]
    close = method.rfind("    ]")
    if close < 0:
        raise RuntimeError("Graph template list anchor changed")
    graph = graph[:start] + method[:close] + "        vehicle_graph_template(),\n" + method[close:] + graph[end:]
    install(graph_path, graph.encode())
    for base in ("backend/vehicle_generation", "backend/tests", "static"):
        source = bundle / base
        if not source.exists():
            continue
        for path in sorted(source.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                install(path.relative_to(bundle).as_posix(), path.read_bytes())
    translations = bundle / "backend/vehicle_generation/i18n-additions.json"
    if translations.is_file():
        for language, extra in json.loads(translations.read_text()).items():
            relative = f"static/i18n/{language}.json"
            doc = json.loads((target / "autorig-online" / relative).read_text())
            doc.update(extra)
            install(relative, (json.dumps(doc, ensure_ascii=False, indent=2) + "\n").encode())
    # These checks concern the staged tree only. The production gate follows.
    backend = target / "autorig-online/backend"
    subprocess.run(["/srv/autorig/venv/bin/python3", "-m", "compileall", "-q",
                    str(backend / "vehicle_generation")], check=True)
    for name in ("ai-nodes.js", "ai-node-vehicle.js"):
        subprocess.run(["node", "--check", str(target / "autorig-online/static/js" / name)], check=True)
    record = {"schema": "autorig.vehicle.release/1", "previous": str(current), "release": str(target),
              "before": before, "after": after, "status": "staged_not_promoted"}
    write(bundle / "stage-receipt.json", json.dumps(record, indent=2).encode())
    print(json.dumps(record))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    stage(args.bundle.resolve(strict=True))

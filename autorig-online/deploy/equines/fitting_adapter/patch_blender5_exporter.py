"""Create a SHA-pinned Blender 5 compatible actionless exporter copy.

The September source remains untouched.  Only Scene compositor enable reads
and writes are routed through Blender's version-adaptive render API.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path


SOURCE_SHA256 = "a710c7c7a417cd6b042b761b3602103f0440e19066581af9616f684fb0828f54"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def patch(source: Path, output: Path) -> None:
    raw = source.read_bytes()
    if digest(raw) != SOURCE_SHA256:
        raise RuntimeError("render_actionless_bundle.py source SHA changed; review and repin")
    text = raw.decode("utf-8").replace("\r\n", "\n")
    if text.count("scene.use_nodes") != 6:
        raise RuntimeError("expected exactly six Scene.use_nodes references")
    anchor = "def compositor_tree(scene: bpy.types.Scene):\n"
    helper = '''def scene_compositing_enabled(scene: bpy.types.Scene) -> bool:
    """Blender 4 uses Scene.use_nodes; Blender 5 uses RenderSettings.use_compositing."""
    render = scene.render
    if hasattr(scene, "compositing_node_group"):
        return bool(render.use_compositing)
    return bool(scene.use_nodes)


def set_scene_compositing(scene: bpy.types.Scene, enabled: bool) -> None:
    render = scene.render
    if hasattr(scene, "compositing_node_group"):
        render.use_compositing = bool(enabled)
    else:
        scene.use_nodes = bool(enabled)


'''
    replacements = {
        "    scene.use_nodes = True\n": "    set_scene_compositing(scene, True)\n",
        "    scene.use_nodes = False\n": "    set_scene_compositing(scene, False)\n",
        '        "use_nodes": bool(scene.use_nodes),\n':
            '        "use_nodes": scene_compositing_enabled(scene),\n',
        '        scene.use_nodes = render_state["use_nodes"]\n':
            '        set_scene_compositing(scene, render_state["use_nodes"])\n',
    }
    expected = {"    scene.use_nodes = True\n": 1, "    scene.use_nodes = False\n": 3,
                '        "use_nodes": bool(scene.use_nodes),\n': 1,
                '        scene.use_nodes = render_state["use_nodes"]\n': 1}
    for old, new in replacements.items():
        if text.count(old) != expected[old]:
            raise RuntimeError(f"unexpected compositor statement count: {old.strip()}")
        text = text.replace(old, new)
    if text.count(anchor) != 1:
        raise RuntimeError("compositor_tree anchor changed")
    text = text.replace(anchor, helper + anchor)
    # The sole remaining occurrences live inside the adaptive helper's Blender 4 branches.
    if text.count("scene.use_nodes") != 2 or "scene.render.use_compositing" in text:
        raise RuntimeError("adaptive compositor patch invariant failed")
    compile(text, str(output), "exec")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8", newline="\n")
    print(f"source_sha256={SOURCE_SHA256}")
    print(f"output_sha256={digest(output.read_bytes())}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    args = ap.parse_args()
    patch(args.source.resolve(), args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

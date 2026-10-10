"""Render one clean RGBA picture of a GLB for a profile avatar (Public chat · V3, 2026-10-10).

    PYTHONPATH=/srv/autorig/data/motion_transfer python3 -P avatar_render.py <model.glb> <out.png> [size] [view]

Uses the Motion Transfer CPU rasterizer (mt.render, the same one that writes a run's proj/front_lit.png and
front_mask.png) as a read-only library: lit colour + coverage mask -> one RGBA PNG with a transparent
background. Runs in its own process with a time limit, so a heavy model never stalls the chat service.
"""
from __future__ import annotations

import sys
import time


def main() -> int:
    glb_path, out_path = sys.argv[1], sys.argv[2]
    size = int(sys.argv[3]) if len(sys.argv) > 3 else 512
    view_name = sys.argv[4] if len(sys.argv) > 4 else "front"
    t0 = time.time()
    import cv2
    import numpy as np
    from mt import render as R
    from mt.glb import load_glb
    from mt.project import detect_forward

    with open(glb_path, "rb") as handle:
        mesh = load_glb(handle.read())
    fwd = detect_forward(mesh, None)
    if view_name == "persp":
        cam = R.persp_camera(mesh.positions, size=size, yaw=R.FORWARD_YAW[fwd], name="persp")
    else:
        cam = R.cameras_for(mesh.positions, size=size, names=("front",), yaw=R.FORWARD_YAW[fwd],
                            frame_points=mesh.positions)["front"]
    view = R.render_view(mesh, cam)
    lit = np.asarray(R.lit_png(view), dtype=np.uint8)
    mask = np.asarray(R.mask_png(view), dtype=np.uint8)
    if mask.ndim == 3:
        mask = mask[..., 0]
    rgba = np.dstack([lit[..., :3], mask])
    cv2.imwrite(out_path, cv2.cvtColor(rgba, cv2.COLOR_RGBA2BGRA))
    print(f"ok {view_name} fwd={fwd} tris={len(mesh.faces)} {time.time() - t0:.2f}s cover={float((mask > 127).mean()):.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

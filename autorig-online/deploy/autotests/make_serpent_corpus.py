#!/usr/bin/env python3
"""Synthetic limbless models for the regression corpus (Serpent rig · V3, 2026-10-11): a tube swept along a curve
(rings of vertices, capped ends), written as GLB. Deterministic, no downloads, no customer data.

    sudo python3 make_serpent_corpus.py --out /srv/autorig/data/autotests/synthetic

Models:
    snake_coiled.glb   a snake lying in a flat spiral of two coils that touch (gap 4 % of the tube radius, separate
                       surfaces), the head end raised and thicker, the tail tapering to a tip
"""
import argparse
import json
import pathlib

import numpy as np

from make_hand_corpus import write_glb


def sweep(curve, radius, ring=24):
    """Rings of `ring` vertices around the curve (n x 3) with per-point radius; both ends capped."""
    n = len(curve)
    t = np.gradient(curve, axis=0)
    t /= np.linalg.norm(t, axis=1, keepdims=True)
    up = np.array([0.0, 1.0, 0.0])
    P, F = [], []
    prev = None
    for i in range(n):
        nrm = up - t[i] * (up @ t[i])
        if np.linalg.norm(nrm) < 1e-3:
            nrm = prev
        nrm = nrm / np.linalg.norm(nrm)
        prev = nrm
        bi = np.cross(t[i], nrm)
        a = np.linspace(0, 2 * np.pi, ring, endpoint=False)
        P.append(curve[i] + radius[i] * (np.cos(a)[:, None] * nrm + np.sin(a)[:, None] * bi))
    P = np.concatenate(P)
    for i in range(n - 1):
        for k in range(ring):
            a0, a1 = i * ring + k, i * ring + (k + 1) % ring
            b0, b1 = a0 + ring, a1 + ring
            F += [(a0, b1, b0), (a0, a1, b1)]          # outward normals
    c0, c1 = len(P), len(P) + 1
    P = np.vstack([P, curve[0] - t[0] * radius[0] * 0.5, curve[-1] + t[-1] * radius[-1] * 0.5])
    for k in range(ring):
        F.append((c0, k, (k + 1) % ring))
        last = (n - 1) * ring
        F.append((c1, last + (k + 1) % ring, last + k))
    return P.astype(np.float32), np.array(F, np.uint32)


def snake_coiled(r=0.03, turns=2.0, n=420):
    gap = 2.08 * r                                        # coil spacing: the tubes touch with a 4 % gap
    b = gap / (2 * np.pi)
    th = np.linspace(0.0, 2 * np.pi * turns, n)
    rho = 2.2 * r + b * th
    curve = np.stack([rho * np.cos(th), np.full(n, r), rho * np.sin(th)], 1)
    # the head (outer end) runs out straight and rises
    out = curve[-1]
    tan = curve[-1] - curve[-2]
    tan /= np.linalg.norm(tan)
    k = 70
    s = np.linspace(0, 1, k)[1:]
    head = out + tan * (s[:, None] * 10 * r) + np.array([0.0, 1.0, 0.0]) * (np.sin(s * np.pi / 2) ** 2 * 4 * r)[:, None]
    curve = np.vstack([curve, head])
    m = len(curve)
    u = np.linspace(0, 1, m)
    radius = r * (0.25 + 0.75 * np.clip(u / 0.25, 0, 1) ** 0.6)          # the tail tapers to a tip
    radius *= 1.0 + 0.35 * np.clip((u - 0.9) / 0.06, 0, 1) * np.clip((1.0 - u) / 0.04, 0, 1) ** 0.3   # a head bulb
    return sweep(curve, radius)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    P, F = snake_coiled()
    nbytes = write_glb(out / "snake_coiled.glb", P, F, "snake_coiled")
    print(json.dumps({"file": "snake_coiled.glb", "vertices": int(len(P)), "triangles": int(len(F)), "bytes": nbytes}))


if __name__ == "__main__":
    main()

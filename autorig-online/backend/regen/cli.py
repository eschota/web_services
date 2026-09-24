"""Command line for AutoRig Regen.

    python -m regen.cli decompose --dressed A.glb --body B.glb --out DIR \\
        [--hair-mask mask.png [--hair-mask-bbox C0,R0,C1,R1] [--hair-mask-mirror]] \\
        [--config overrides.json]

    python -m regen.cli synth --out DIR [--cape] [--resolution 1.0] [--seed 0]

``decompose`` prints a JSON summary on stdout. Exit codes: 0 success,
2 DecompositionError (message on stderr, the caller should fall back to
plain rigging), 1 bad arguments.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional


def _parse_bbox(text: str):
    values = [float(v) for v in text.replace(" ", "").split(",")]
    if len(values) != 4:
        raise argparse.ArgumentTypeError("expected C0,R0,C1,R1")
    return tuple(values)


def _decompose(args: argparse.Namespace) -> int:
    from .decompose import decompose
    from .errors import DecompositionError
    from .segment import HairMask

    from .config import coerce_config

    config = None
    if args.config:
        try:
            config = coerce_config(json.loads(Path(args.config).read_text(encoding="utf-8")))
        except (OSError, ValueError, TypeError) as exc:
            print(f"regen: bad --config: {exc}", file=sys.stderr)
            return 1
    hair_mask = None
    if args.hair_mask:
        try:
            hair_mask = HairMask.from_image(
                args.hair_mask, bbox_px=args.hair_mask_bbox, mirror=True if args.hair_mask_mirror else None
            )
        except Exception as exc:
            print(f"regen: cannot read hair mask: {exc}", file=sys.stderr)
            return 2
    try:
        result = decompose(args.dressed, args.body, args.out, hair_mask=hair_mask, config=config)
    except DecompositionError as exc:
        print(f"regen: decomposition failed: {exc}", file=sys.stderr)
        return 2
    doc = result.document
    summary = {
        "ok": True,
        "out_dir": str(result.out_dir),
        "decomposition": str(result.decomposition_path),
        "height": doc["height"],
        "parts": [
            {k: p[k] for k in ("name", "kind", "vertex_count", "attach", "rigid", "connection")} for p in doc["parts"]
        ],
        "groups": [
            {"name": g["name"], "connection": g["connection"], "preset": g["preset"], "chains": len(g["chains"])}
            for g in doc["groups"]
        ],
        "align_rms_relative": doc["diagnostics"]["align_rms_relative"],
        "align_inlier_ratio": doc["diagnostics"]["align_inlier_ratio"],
        "label_counts": doc["diagnostics"]["label_counts"],
        "warnings": doc["diagnostics"]["warnings"],
        "timings": result.timings,
    }
    print(json.dumps(summary, indent=1))
    return 0


def _synth(args: argparse.Namespace) -> int:
    from .synthetic import make_pair, write_pair

    pair = make_pair(seed=args.seed, resolution=args.resolution, cape=args.cape)
    dressed, body = write_pair(pair, args.out)
    print(
        json.dumps(
            {
                "dressed": str(dressed),
                "body": str(body),
                "dressed_vertices": int(len(pair.dressed.vertices)),
                "body_vertices": int(len(pair.body.vertices)),
                "applied_scale": pair.scale,
                "applied_rotation_deg": pair.rotation_deg,
                "body_to_dressed": [float(v) for v in pair.body_to_dressed.reshape(-1)],
            },
            indent=1,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m regen.cli", description="AutoRig Regen geometry core")
    sub = parser.add_subparsers(dest="command", required=True)

    dec = sub.add_parser("decompose", help="split a dressed character into body / hair / cloth parts")
    dec.add_argument("--dressed", required=True, help="dressed character GLB (image A reconstruction)")
    dec.add_argument("--body", required=True, help="base body GLB (image B reconstruction)")
    dec.add_argument("--out", required=True, help="output directory")
    dec.add_argument("--hair-mask", help="front-view hair mask image (white = hair)")
    dec.add_argument(
        "--hair-mask-bbox",
        type=_parse_bbox,
        help="pixel box C0,R0,C1,R1 the dressed mesh's X/Y bounds map onto (default: whole image)",
    )
    dec.add_argument("--hair-mask-mirror", action="store_true", help="the mask image's right side is world -X")
    dec.add_argument("--config", help="JSON file with RegenConfig overrides")
    dec.set_defaults(func=_decompose)

    syn = sub.add_parser("synth", help="write a synthetic dressed/body GLB pair")
    syn.add_argument("--out", required=True)
    syn.add_argument("--seed", type=int, default=0)
    syn.add_argument("--resolution", type=float, default=1.0)
    syn.add_argument("--cape", action="store_true")
    syn.set_defaults(func=_synth)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

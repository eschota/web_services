from __future__ import annotations

import argparse
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import shutil
import sys
from typing import Optional

from .derive_semantic_reference import derive_semantic_reference_cli
from .errors import FittingError
from .frames import extract_frames
from .observations import adapt_tracker_json, load_observations
from .optimizer import FittingConfig, fit_sequence
from .rig import load_rig_bundle
from .semantic_ltx_reference import SemanticLtxContractError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m animation_fitting",
        description="Fail-closed offline video-to-animal-skeleton fitting pipeline.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    extract = commands.add_parser("extract-frames", help="Extract and verify a contiguous PNG sequence")
    extract.add_argument("--video", required=True)
    extract.add_argument("--output-dir", required=True)
    extract.add_argument("--ffmpeg")
    extract.add_argument("--ffprobe")
    extract.add_argument("--fps", type=float)
    extract.add_argument("--overwrite", action="store_true")

    adapt = commands.add_parser("adapt-tracks", help="Convert an explicit tracker JSON export")
    adapt.add_argument("--adapter", choices=("cotracker", "tap"), required=True)
    adapt.add_argument("--input", required=True)
    adapt.add_argument("--anchor-map", required=True)
    adapt.add_argument("--output", required=True)
    adapt.add_argument("--layout", choices=("T,N,2", "N,T,2"), required=True)
    adapt.add_argument("--width", type=int, required=True)
    adapt.add_argument("--height", type=int, required=True)
    adapt.add_argument("--fps", type=float, required=True)
    adapt.add_argument("--visibility-threshold", type=float)

    validate = commands.add_parser("validate-bundle", help="Verify an actionless fitting bundle and hashes")
    validate.add_argument("--bundle", required=True)

    semantic = commands.add_parser(
        "derive-semantic-reference",
        help=(
            "Derive an immutable rig-semantic LTX conditioning PNG from an "
            "existing actionless bundle"
        ),
    )
    semantic.add_argument("--bundle", required=True)
    semantic.add_argument("--profile", required=True)
    semantic.add_argument("--output-dir", required=True)

    fit = commands.add_parser("fit", help="Run bounded temporal fitting and write every local bone transform")
    fit.add_argument("--bundle", required=True)
    fit.add_argument("--observations", required=True)
    fit.add_argument("--output", required=True)
    mode = fit.add_mutually_exclusive_group(required=True)
    mode.add_argument("--loop", action="store_true")
    mode.add_argument("--one-shot", action="store_true")
    fit.add_argument("--config")
    fit.add_argument("--allow-unbounded-joints", action="store_true")
    fit.add_argument("--active-bone", action="append", default=[])

    doctor = commands.add_parser("doctor", help="Check deterministic runtime dependencies")
    doctor.add_argument("--ffmpeg")
    doctor.add_argument("--ffprobe")
    doctor.add_argument("--tracker", choices=("none", "cotracker", "tap"), default="none")
    return parser


def _doctor(args: argparse.Namespace) -> int:
    dependencies = {}
    for module in ("numpy", "scipy", "PIL"):
        dependencies[module] = importlib.util.find_spec(module) is not None
    ffmpeg = args.ffmpeg or shutil.which("ffmpeg")
    ffprobe = args.ffprobe or shutil.which("ffprobe")
    if ffmpeg and not Path(ffmpeg).is_file():
        ffmpeg = None
    if ffprobe and not Path(ffprobe).is_file():
        ffprobe = None
    if not ffprobe:
        sibling = Path(ffmpeg).with_name("ffprobe.exe") if ffmpeg else None
        ffprobe = str(sibling) if sibling and sibling.is_file() else None
    dependencies["ffmpeg"] = ffmpeg
    dependencies["ffprobe"] = ffprobe
    tracker = {"requested": args.tracker, "available": True, "install": None, "contract": None}
    if args.tracker == "cotracker":
        tracker["available"] = importlib.util.find_spec("cotracker") is not None
        tracker["install"] = (
            "git clone https://github.com/facebookresearch/co-tracker.git && "
            "cd co-tracker && python -m pip install -e ."
        )
        tracker["contract"] = (
            "Run the official tracker separately, export tracks/visibility JSON, then use "
            "adapt-tracks --adapter cotracker with an explicit anchor map. License review is required: "
            "the official repository is predominantly CC-BY-NC."
        )
    elif args.tracker == "tap":
        tracker["available"] = importlib.util.find_spec("tapnet") is not None
        tracker["install"] = (
            "git clone https://github.com/google-deepmind/tapnet.git && "
            "cd tapnet && python -m pip install -e ."
        )
        tracker["contract"] = (
            "Run the official TAP implementation separately, export tracks/occluded JSON, then use "
            "adapt-tracks --adapter tap with an explicit anchor map."
        )
    ok = all(bool(dependencies[module]) for module in ("numpy", "scipy", "PIL"))
    ok = ok and bool(ffmpeg) and bool(ffprobe) and bool(tracker["available"])
    output = {"ok": ok, "dependencies": dependencies, "tracker": tracker}
    if not ffmpeg or not ffprobe:
        output["ffmpeg_command"] = (
            "Pass --ffmpeg C:\\path\\to\\ffmpeg.exe --ffprobe C:\\path\\to\\ffprobe.exe, "
            "or add both executables to PATH."
        )
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if ok else 2


def main(argv: Optional[list[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "extract-frames":
            result = extract_frames(
                args.video,
                args.output_dir,
                ffmpeg=args.ffmpeg,
                ffprobe=args.ffprobe,
                fps=args.fps,
                overwrite=args.overwrite,
            )
            print(
                json.dumps(
                    {
                        "manifest": str(result.manifest_path),
                        "frame_count": result.frame_count,
                        "width": result.width,
                        "height": result.height,
                        "fps": result.fps,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "adapt-tracks":
            output = adapt_tracker_json(
                args.input,
                adapter=args.adapter,
                anchor_map_path=args.anchor_map,
                output_path=args.output,
                layout=args.layout,
                width=args.width,
                height=args.height,
                fps=args.fps,
                visibility_threshold=args.visibility_threshold,
            )
            print(json.dumps({"observations": str(output)}, indent=2))
            return 0
        if args.command == "validate-bundle":
            rig = load_rig_bundle(args.bundle)
            print(
                json.dumps(
                    {
                        "bundle": str(rig.metadata_path),
                        "sha256": rig.metadata_sha256,
                        "bone_count": len(rig.bones),
                        "anchor_count": len(rig.anchors),
                        "camera": [rig.camera.width, rig.camera.height],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "derive-semantic-reference":
            result = derive_semantic_reference_cli(
                args.bundle,
                args.profile,
                args.output_dir,
            )
            print(
                json.dumps(
                    {
                        "output_dir": str(result.output_dir),
                        "semantic_png": str(result.semantic_path),
                        "semantic_sha256": result.semantic_sha256,
                        "derivation_manifest": str(result.derivation_manifest_path),
                        "immutable_manifest": str(result.immutable_manifest_path),
                        "immutable_manifest_sha256": result.immutable_manifest_sha256,
                        "ltx_generation_authorized": False,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "fit":
            config = FittingConfig.from_json(args.config) if args.config else FittingConfig()
            if args.allow_unbounded_joints:
                config = replace(config, allow_unbounded_joints=True)
            if args.active_bone:
                config = replace(config, active_bones=tuple(args.active_bone))
            rig = load_rig_bundle(args.bundle)
            observations = load_observations(args.observations)
            result = fit_sequence(rig, observations, loop=bool(args.loop), config=config)
            output = result.save(args.output)
            print(
                json.dumps(
                    {
                        "output": str(output),
                        "optimizer": result.optimizer,
                        "qa": result.qa,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "doctor":
            return _doctor(args)
    except (FittingError, SemanticLtxContractError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

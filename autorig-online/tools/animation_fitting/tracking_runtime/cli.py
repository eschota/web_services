from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

from ..contact_profile import load_contact_profile, validate_contact_profile_bundle
from ..errors import FittingError
from ..rig import load_rig_bundle
from .core import (
    AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256,
    ObservationRuntimeConfig,
    run_observation_pipeline,
    select_anchor_seeds,
)
from .official_backends import (
    Sam2VideoMaskBackend,
    TapNextPPBackend,
    VideoDepthAnythingSmallBackend,
)
from .runtime_lock import load_runtime_lock


DEFAULT_RUNTIME_ROOT = Path(
    os.environ.get(
        "AUTORIG_FITTING_RUNTIME_ROOT",
        r"R:\ComfyUI-data\autorig-fitting\runtimes",
    )
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m animation_fitting.tracking_runtime",
        description="Pinned fail-closed horse/animal video observation runtime.",
    )
    parser.add_argument("--runtime-root", default=str(DEFAULT_RUNTIME_ROOT))
    parser.add_argument("--runtime-lock")
    commands = parser.add_subparsers(dest="command", required=True)

    doctor = commands.add_parser(
        "doctor", help="Verify official commits, licenses, weights, and CUDA"
    )
    doctor.add_argument("--device", default="cuda")
    doctor.add_argument("--with-depth", action="store_true")

    seeds = commands.add_parser(
        "seeds", help="Print deterministic canonical surface-anchor seeds"
    )
    seeds.add_argument("--bundle", required=True)
    seeds.add_argument("--max-tracks", type=int, default=64)
    seeds.add_argument("--contact-profile")

    observe = commands.add_parser(
        "observe", help="Create observations.json/npz/masks/diagnostics"
    )
    observe.add_argument("--video", required=True)
    observe.add_argument("--bundle", required=True)
    observe.add_argument("--output-dir", required=True)
    observe.add_argument("--device", default="cuda")
    observe.add_argument("--allow-cpu", action="store_true")
    observe.add_argument("--with-depth", action="store_true")
    observe.add_argument("--contact-profile")
    observe.add_argument("--loop", action="store_true")
    observe.add_argument(
        "--browser-endpoint-guide-bundle",
        help=(
            "Opt-in authorized v11 static-scene or v12 recovery-guide browser "
            "bundle whose pinned frame 0 replaces canonical RGB only for "
            "first-frame alignment"
        ),
    )
    observe.add_argument(
        "--browser-endpoint-guide-manifest-sha256",
        help="Exact lowercase SHA-256 of <guide bundle>/immutable_manifest.json",
    )
    observe.add_argument("--ffprobe")
    observe.add_argument("--min-alignment-correlation", type=float, default=0.65)
    observe.add_argument("--min-visible-ratio", type=float, default=0.35)
    observe.add_argument("--min-visible-confidence", type=float, default=0.05)
    observe.add_argument("--min-median-visible-confidence", type=float, default=0.50)
    observe.add_argument("--min-track-mask-ratio", type=float, default=0.55)
    return parser


def _browser_reference_cli_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    bundle = args.browser_endpoint_guide_bundle
    manifest_sha256 = args.browser_endpoint_guide_manifest_sha256
    if bundle is None and manifest_sha256 is None:
        return {}
    if bundle is None or manifest_sha256 is None:
        raise FittingError(
            "Browser endpoint guide override requires both bundle and manifest SHA-256"
        )
    if re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None:
        raise FittingError(
            "Browser endpoint guide manifest SHA-256 must be exact lowercase hex"
        )
    if manifest_sha256 not in AUTHORIZED_BROWSER_GUIDE_MANIFEST_SHA256:
        raise FittingError(
            "Browser endpoint guide manifest SHA-256 is not authorized by this runtime"
        )
    root = Path(bundle).resolve()
    if not root.is_dir() or not (root / "immutable_manifest.json").is_file():
        raise FittingError(
            f"Browser endpoint guide bundle/manifest does not exist: {root}"
        )
    return {
        "browser_endpoint_guide_bundle": str(root),
        "browser_endpoint_guide_manifest_sha256": manifest_sha256,
    }


def _paths(runtime_root: Path) -> dict[str, Path]:
    return {
        "tapnet": runtime_root / "tapnet",
        "sam2": runtime_root / "sam2",
        "video_depth_anything": runtime_root / "video-depth-anything",
        "tapnextpp": runtime_root / "checkpoints" / "tapnextpp_ckpt.pt",
        "sam2_hiera_tiny": runtime_root / "checkpoints" / "sam2.1_hiera_tiny.pt",
        "video_depth_anything_small": runtime_root
        / "checkpoints"
        / "video_depth_anything_vits.pth",
    }


def _module_version(name: str) -> str | None:
    try:
        module = importlib.import_module(name)
    except Exception:
        return None
    declared = getattr(module, "__version__", None)
    if declared is not None:
        return str(declared)
    distribution = {
        "PIL": "Pillow",
        "cv2": "opencv-python-headless",
        "hydra": "hydra-core",
    }.get(name, name)
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return "installed"


def _doctor(
    runtime_root: Path, lock_path: str | None, device: str, with_depth: bool
) -> int:
    lock = load_runtime_lock(lock_path)
    paths = _paths(runtime_root)
    checks: dict[str, Any] = {"runtime_root": str(runtime_root), "lock": str(lock.path)}
    ok = True
    for name in ("tapnet", "sam2", "video_depth_anything"):
        try:
            checks[name] = lock.verify_repo(name, paths[name])
        except FittingError as exc:
            checks[name] = {"error": str(exc)}
            ok = False
    checkpoint_names = ["tapnextpp", "sam2_hiera_tiny"]
    if with_depth:
        checkpoint_names.append("video_depth_anything_small")
    for name in checkpoint_names:
        try:
            pin = lock.checkpoints[name]
            checks[name] = lock.verify_checkpoint(
                name,
                paths[name],
                license_repo=paths[pin.license_source_repo],
            )
        except FittingError as exc:
            checks[name] = {"error": str(exc)}
            ok = False
    checks["python"] = {
        name: _module_version(name)
        for name in (
            "torch",
            "torchvision",
            "numpy",
            "cv2",
            "PIL",
            "scipy",
            "einops",
            "hydra",
            "iopath",
            "tqdm",
        )
    }
    checks["python"]["python"] = f"{sys.version_info.major}.{sys.version_info.minor}"
    for name, expected in lock.python.items():
        actual = checks["python"].get(name)
        if actual != expected:
            checks["python"].setdefault("mismatches", {})[name] = {
                "expected": expected,
                "actual": actual,
            }
            ok = False
    try:
        import torch

        target = torch.device(device)
        cuda_ok = target.type != "cuda" or torch.cuda.is_available()
        checks["gpu"] = {
            "requested": str(target),
            "cuda_available": torch.cuda.is_available(),
            "cuda_runtime": torch.version.cuda,
            "name": torch.cuda.get_device_name(0)
            if torch.cuda.is_available()
            else None,
        }
        ok = ok and cuda_ok
    except Exception as exc:
        checks["gpu"] = {"error": str(exc)}
        ok = False
    checks["ok"] = ok
    print(json.dumps(checks, indent=2, sort_keys=True))
    return 0 if ok else 2


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    runtime_root = Path(args.runtime_root).resolve()
    try:
        if args.command == "doctor":
            return _doctor(
                runtime_root, args.runtime_lock, args.device, bool(args.with_depth)
            )
        if args.command == "seeds":
            profile = (
                load_contact_profile(args.contact_profile)
                if args.contact_profile is not None
                else None
            )
            if profile is not None:
                rig = load_rig_bundle(args.bundle)
                validate_contact_profile_bundle(
                    profile,
                    rig_metadata=rig.metadata,
                    anchors=rig.anchors,
                )
            seeds = select_anchor_seeds(
                args.bundle,
                max_tracks=args.max_tracks,
                priority_anchor_ids=()
                if profile is None
                else profile.priority_anchor_ids,
            )
            print(
                json.dumps(
                    {
                        "track_count": len(seeds.track_ids),
                        "bundle_sha256": seeds.bundle_sha256,
                        "immutable_manifest_sha256": seeds.immutable_manifest_sha256,
                        "tracks": [
                            {
                                "track_id": track_id,
                                "anchor_id": anchor_id,
                                "x": float(point[0]),
                                "y": float(point[1]),
                            }
                            for track_id, anchor_id, point in zip(
                                seeds.track_ids, seeds.anchor_ids, seeds.points_xy
                            )
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "observe":
            browser_reference_kwargs = _browser_reference_cli_kwargs(args)
            if browser_reference_kwargs and not args.loop:
                raise FittingError(
                    "Browser endpoint guide override is accepted only with --loop"
                )
            lock = load_runtime_lock(args.runtime_lock)
            paths = _paths(runtime_root)
            require_cuda = not bool(args.allow_cpu)
            tracker = TapNextPPBackend(
                paths["tapnet"],
                paths["tapnextpp"],
                lock,
                device=args.device,
                require_cuda=require_cuda,
            )
            segmenter = Sam2VideoMaskBackend(
                paths["sam2"],
                paths["sam2_hiera_tiny"],
                lock,
                device=args.device,
                require_cuda=require_cuda,
            )
            depth = (
                VideoDepthAnythingSmallBackend(
                    paths["video_depth_anything"],
                    paths["video_depth_anything_small"],
                    lock,
                    device=args.device,
                    require_cuda=require_cuda,
                )
                if args.with_depth
                else None
            )
            config = ObservationRuntimeConfig(
                loop=bool(args.loop),
                min_alignment_correlation=args.min_alignment_correlation,
                min_visible_ratio=args.min_visible_ratio,
                min_visible_confidence=args.min_visible_confidence,
                min_median_visible_confidence=args.min_median_visible_confidence,
                min_visible_track_inside_mask_ratio=args.min_track_mask_ratio,
            )
            output = run_observation_pipeline(
                video=args.video,
                bundle=args.bundle,
                output_dir=args.output_dir,
                tracker=tracker,
                segmenter=segmenter,
                depth_backend=depth,
                contact_profile=args.contact_profile,
                config=config,
                ffprobe=args.ffprobe,
                **browser_reference_kwargs,
            )
            print(json.dumps({"observations": str(output)}, indent=2))
            return 0
    except FittingError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

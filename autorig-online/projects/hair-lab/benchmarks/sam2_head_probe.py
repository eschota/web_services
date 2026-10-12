"""CPU-only SAM2 head-mask probe on hash-bound real AutoRig closeups.

Seeds come from the previous diagnostic mask, NOT held-out reviewer anchors.
No model download, production mutation, training or automatic Apply occurs here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
from pathlib import Path

import cv2
import numpy as np
import torch
from PIL import Image
from sam2.build_sam import build_sam2
from sam2.sam2_image_predictor import SAM2ImagePredictor

VIEWS = ("front", "left", "back", "right")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def points_from_mask(mask, count, separation=45):
    distance = cv2.distanceTransform(mask.astype(np.uint8), cv2.DIST_L2, 5)
    result = []
    for _ in range(count):
        _, value, _, pos = cv2.minMaxLoc(distance)
        if value < 4:
            break
        result.append(pos)
        cv2.circle(distance, pos, separation, 0, -1)
    return result


def run(args):
    if args.out.exists():
        raise ValueError("Output revision already exists")
    receipt_path = args.closeup / "receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if sha(args.source) != receipt["source_sha256"]:
        raise ValueError("Source/closeup hash mismatch")
    for name, item in receipt["files"].items():
        if Path(name).name != name or sha(args.closeup / name) != item["sha256"]:
            raise ValueError("Closeup changed: " + name)
    checkpoint_hash = sha(args.checkpoint)
    if checkpoint_hash != args.checkpoint_sha:
        raise ValueError("Model checkpoint hash mismatch")
    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    start = time.perf_counter()
    model = build_sam2("configs/sam2.1/sam2.1_hiera_t.yaml", str(args.checkpoint),
                      device="cpu", apply_postprocessing=False)
    predictor = SAM2ImagePredictor(model, max_hole_area=0, max_sprinkle_area=0)
    load_seconds = time.perf_counter() - start
    args.out.mkdir(parents=True)
    result = {"schema": "autorig.hair.sam2-head-probe/1", "status": "diagnostic_not_accepted",
              "source_sha256": sha(args.source), "closeup_receipt_sha256": sha(receipt_path),
              "model_checkpoint_sha256": checkpoint_hash, "model_source_commit": args.model_commit,
              "device": "cpu", "torch": torch.__version__, "python": platform.python_version(),
              "threads": args.threads, "load_seconds": load_seconds, "views": {},
              "automatic_apply_allowed": False, "trained": False,
              "limitations": ["Seeds derive from unaccepted GrabCut observation, not semantic truth",
                  "SAM scores are model scores, not measured hair accuracy",
                  "No CUDA/postprocessing used; CPU timings are not GPU claims"]}
    overlays = []
    with torch.inference_mode():
        for view in VIEWS:
            image = np.array(Image.open(args.closeup / (view + "_lit.png")).convert("RGB"))
            initial_path = args.seeds / ("mask_" + view + ".png")
            seed_mask = np.array(Image.open(initial_path).convert("L")) > 0
            rgb = np.array(Image.open(args.closeup / (view + "_tri.png")).convert("RGB"))
            valid = np.any(rgb > 0, axis=2)
            if seed_mask.shape != valid.shape:
                raise ValueError("Seed/camera dimensions differ")
            positive = points_from_mask(seed_mask & valid, 6)
            # Body/neck pixels otherwise consume all negative prompts because they
            # have more area than the face. Reserve negatives inside the hair's
            # observed vertical range; no manual held-out points are reused.
            yy, xx = np.where(seed_mask & valid)
            inner = np.zeros(valid.shape, dtype=bool)
            if len(xx):
                end_y = max(int(yy.min()) + 1, int(yy.max() - .05 * (yy.max() - yy.min())))
                inner[int(yy.min()):end_y, int(xx.min()):int(xx.max()) + 1] = True
            negative = points_from_mask(valid & ~seed_mask & inner, 4, separation=35)
            negative += points_from_mask(valid & ~seed_mask & ~inner, 2)
            negative += points_from_mask(~valid, 3, separation=100)
            if not positive or not negative:
                raise ValueError("Insufficient prompt evidence: " + view)
            coords = np.array(positive + negative, dtype=np.float32)
            labels = np.array([1] * len(positive) + [0] * len(negative), dtype=np.int32)
            before = time.perf_counter()
            predictor.set_image(image)
            encoded = time.perf_counter()
            masks, scores, _ = predictor.predict(point_coords=coords, point_labels=labels,
                                                multimask_output=True)
            after = time.perf_counter()
            if not np.isfinite(masks).all() or not np.isin(masks, (0, 1)).all():
                raise ValueError("Predictor returned non-binary masks")
            masks = masks.astype(bool)
            candidates = []
            for index, (mask, score) in enumerate(zip(masks, scores)):
                prompt_matches = [bool(mask[int(y), int(x)]) == bool(label)
                    for (x, y), label in zip(coords, labels)]
                candidates.append({"index": index, "score": float(score),
                    "prompt_matches": sum(prompt_matches), "prompt_count": len(labels),
                    "raw_pixels": int(mask.sum()), "valid_pixels": int((mask & valid).sum())})
                Image.fromarray(mask.astype(np.uint8) * 255).save(args.out / f"{view}_candidate_{index}.png")
            eligible = [item for item in candidates if item["prompt_matches"] == item["prompt_count"]]
            winner = max(eligible, key=lambda item: item["score"]) if eligible else None
            final = (masks[winner["index"]] & valid) if winner else np.zeros(valid.shape, dtype=bool)
            Image.fromarray(final.astype(np.uint8) * 255).save(args.out / f"mask_{view}.png")
            overlay = image.copy()
            overlay[final] = overlay[final] // 2 + np.array([100, 0, 100], np.uint8)
            for (x, y), label in zip(coords, labels):
                cv2.circle(overlay, (int(x), int(y)), 4, (0, 255, 0) if label else (255, 40, 20), -1)
            overlays.append(overlay)
            result["views"][view] = {"seed_mask_sha256": sha(initial_path),
                "positive_points": positive, "negative_points": negative,
                "encoder_seconds": encoded - before, "decoder_seconds": after - encoded,
                "candidates": candidates, "winner": winner, "output_pixels": int(final.sum())}
            print(json.dumps({"view": view, "seconds": after-before,
                              "winner": winner, "pixels": int(final.sum())}), flush=True)
    sheet = np.concatenate((np.concatenate(overlays[:2], axis=1),
                            np.concatenate(overlays[2:], axis=1)), axis=0)
    Image.fromarray(sheet).save(args.out / "sam2_overlay.png")
    result["files"] = {p.name: sha(p) for p in sorted(args.out.glob("*.png"))}
    (args.out / "receipt.json").write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("source", "closeup", "seeds", "checkpoint", "out"):
        parser.add_argument("--" + key, required=True, type=Path)
    parser.add_argument("--checkpoint-sha", required=True)
    parser.add_argument("--model-commit", required=True)
    parser.add_argument("--threads", type=int, default=8, choices=range(1, 17))
    run(parser.parse_args())

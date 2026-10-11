#!/usr/bin/env python3
"""Build inspectable source-view + head-weight ROI mask sheets for real cases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import cv2
import numpy as np


def label(image, text, color=(0, 210, 255)):
    cv2.putText(image, text, (8, 24), cv2.FONT_HERSHEY_SIMPLEX, 0.62, color, 2, cv2.LINE_AA)
    return image


def fit(image, width, height):
    scale = min(width / image.shape[1], height / image.shape[0])
    resized = cv2.resize(image, (max(1, round(image.shape[1] * scale)), max(1, round(image.shape[0] * scale))), interpolation=cv2.INTER_AREA)
    canvas = np.zeros((height, width, 3), dtype=np.uint8)
    y = (height - resized.shape[0]) // 2
    x = (width - resized.shape[1]) // 2
    canvas[y:y + resized.shape[0], x:x + resized.shape[1]] = resized
    return canvas


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    cases = json.loads(args.manifest.read_text(encoding="utf-8"))["cases"]
    for case in cases:
        case_id = case["case_id"]
        case_dir = args.work_dir / case_id
        output_dir = case_dir / "benchmark-output"
        lit_tiles = []
        for filename in ("front_lit.png", "back_lit.png"):
            image = cv2.imread(str(case_dir / filename), cv2.IMREAD_COLOR)
            if image is None:
                image = np.zeros((256, 384, 3), dtype=np.uint8)
            # Visual crop only. Stored view labels are not treated as camera truth.
            crop = image[: max(1, round(image.shape[0] * 0.48)), :]
            lit_tiles.append(label(fit(crop, 384, 256), f"stored {filename} top crop"))
        mask_tiles = []
        for name in ("front", "back", "left", "right", "top", "bottom"):
            mask = cv2.imread(str(output_dir / f"{name}_mask.png"), cv2.IMREAD_GRAYSCALE)
            tile = cv2.cvtColor(mask, cv2.COLOR_GRAY2BGR)
            mask_tiles.append(label(fit(tile, 256, 256), f"head-weight ROI {name}"))
        sheet = cv2.vconcat((cv2.hconcat(lit_tiles), cv2.hconcat(mask_tiles[:3]), cv2.hconcat(mask_tiles[3:])))
        cv2.imwrite(str(output_dir / "inspection_source_and_head_roi.png"), sheet)


if __name__ == "__main__":
    main()

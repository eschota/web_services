from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from animation_fitting.tracking_runtime.independent_foreground_gate import (
    IndependentForegroundGateConfig,
    derive_video_motion_prompt,
    evaluate_independent_foreground,
)
from animation_fitting.tracking_runtime.models import VideoFrames


def _video(frames: np.ndarray) -> VideoFrames:
    return VideoFrames(
        source=Path("synthetic.mp4"),
        source_sha256="0" * 64,
        frames_bgr=frames,
        fps=24.0,
        ffprobe={},
    )


def test_motion_prompt_uses_only_video_pixels_and_is_deterministic() -> None:
    frames = np.full((12, 96, 144, 3), 90, dtype=np.uint8)
    for frame in range(len(frames)):
        x0 = 36 + frame
        frames[frame, 28:76, x0 : x0 + 38] = 230
    config = IndependentForegroundGateConfig()
    first = derive_video_motion_prompt(_video(frames), config)
    second = derive_video_motion_prompt(_video(frames.copy()), config)
    assert np.array_equal(first.box_xyxy, second.box_xyxy)
    assert np.array_equal(first.component_mask, second.component_mask)
    assert first.provenance["canonical_geometry_used"] is False
    assert first.provenance["canonical_mask_used"] is False
    assert first.provenance["semantic_seeds_used"] is False
    x0, y0, x1, y1 = first.box_xyxy
    assert x0 <= 36
    assert y0 <= 28
    assert x1 >= 36 + 11 + 37
    assert y1 >= 75


def test_independent_foreground_passes_strict_silhouette_contract() -> None:
    canonical = np.zeros((80, 120), dtype=bool)
    canonical[18:68, 30:92] = True
    independent = np.zeros_like(canonical)
    independent[19:69, 31:93] = True
    points = np.asarray(
        [(35, 25), (45, 35), (60, 45), (75, 55), (88, 64)],
        dtype=np.float32,
    )
    config = IndependentForegroundGateConfig(
        min_mask_fraction=0.10,
        max_mask_fraction=0.50,
    )
    metrics, failures = evaluate_independent_foreground(
        independent_mask=independent,
        backend_score=0.92,
        canonical_mask=canonical,
        semantic_points_xy=points,
        config=config,
    )
    assert failures == []
    assert metrics["silhouette"]["iou"] > 0.90
    assert metrics["semantic_seeds"]["inside_tolerant_ratio"] == 1.0


def test_independent_foreground_fails_without_canonical_overlap() -> None:
    canonical = np.zeros((80, 120), dtype=bool)
    canonical[12:42, 12:52] = True
    independent = np.zeros_like(canonical)
    independent[45:75, 65:105] = True
    points = np.asarray([(20, 20), (30, 30), (45, 35)], dtype=np.float32)
    metrics, failures = evaluate_independent_foreground(
        independent_mask=independent,
        backend_score=0.95,
        canonical_mask=canonical,
        semantic_points_xy=points,
        config=IndependentForegroundGateConfig(),
    )
    assert metrics["silhouette"]["iou"] == 0.0
    assert "independent_silhouette_iou" in failures
    assert "independent_canonical_recall" in failures
    assert "independent_precision" in failures
    assert "independent_seed_tolerant_membership" in failures


def test_motion_prompt_fails_closed_for_static_video() -> None:
    frames = np.full((12, 96, 144, 3), 90, dtype=np.uint8)
    with pytest.raises(Exception, match="no independently measurable foreground motion"):
        derive_video_motion_prompt(_video(frames), IndependentForegroundGateConfig())

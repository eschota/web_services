from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import numpy as np


@dataclass(frozen=True)
class VideoFrames:
    source: Path
    source_sha256: str
    frames_bgr: np.ndarray
    fps: float
    ffprobe: dict[str, Any]

    @property
    def frame_count(self) -> int:
        return int(self.frames_bgr.shape[0])

    @property
    def height(self) -> int:
        return int(self.frames_bgr.shape[1])

    @property
    def width(self) -> int:
        return int(self.frames_bgr.shape[2])


@dataclass(frozen=True)
class SeedSet:
    track_ids: tuple[str, ...]
    anchor_ids: tuple[str, ...]
    points_xy: np.ndarray
    canonical_mask: np.ndarray
    reference_rgb: np.ndarray
    bundle_sha256: str
    immutable_manifest_sha256: str


@dataclass(frozen=True)
class TrackResult:
    points_xy: np.ndarray
    visible: np.ndarray
    confidence: np.ndarray
    provenance: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MaskResult:
    masks: np.ndarray
    confidence: np.ndarray | None = None
    provenance: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DepthResult:
    relative_depth: np.ndarray
    provenance: dict[str, Any] = field(default_factory=dict)


class TrackerBackend(Protocol):
    def track(self, video: VideoFrames, seeds: SeedSet) -> TrackResult: ...


class MaskBackend(Protocol):
    def segment(self, video: VideoFrames, initial_mask: np.ndarray) -> MaskResult: ...


class DepthBackend(Protocol):
    def infer(self, video: VideoFrames) -> DepthResult: ...

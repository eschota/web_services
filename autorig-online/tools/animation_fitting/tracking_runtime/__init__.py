"""Pinned video-observation runtime for animal Animation Fitting.

The package intentionally keeps model imports lazy. Importing it never mutates
the ComfyUI Python environment or downloads model weights.
"""

from .core import ObservationRuntimeConfig, run_observation_pipeline, select_anchor_seeds
from .models import DepthResult, MaskResult, SeedSet, TrackResult, VideoFrames
from .runtime_lock import RuntimeLock, load_runtime_lock

__all__ = [
    "DepthResult",
    "MaskResult",
    "ObservationRuntimeConfig",
    "RuntimeLock",
    "SeedSet",
    "TrackResult",
    "VideoFrames",
    "load_runtime_lock",
    "run_observation_pipeline",
    "select_anchor_seeds",
]

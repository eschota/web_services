from __future__ import annotations

from dataclasses import dataclass
import gc
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

import numpy as np

from ..errors import ContractError, DependencyUnavailableError
from .models import DepthResult, MaskResult, SeedSet, TrackResult, VideoFrames
from .runtime_lock import RuntimeLock


_CUBLAS_WORKSPACE_CONFIG = ":4096:8"


def _prepend_import_root(path: Path) -> None:
    value = str(path.resolve())
    if value in sys.path:
        sys.path.remove(value)
    sys.path.insert(0, value)


def _torch(device: str, *, require_cuda: bool) -> Any:
    configured_workspace = os.environ.get("CUBLAS_WORKSPACE_CONFIG")
    if configured_workspace not in (None, _CUBLAS_WORKSPACE_CONFIG):
        raise DependencyUnavailableError(
            "CUBLAS_WORKSPACE_CONFIG must be unset or exactly "
            f"{_CUBLAS_WORKSPACE_CONFIG!r}; got {configured_workspace!r}"
        )
    os.environ["CUBLAS_WORKSPACE_CONFIG"] = _CUBLAS_WORKSPACE_CONFIG
    try:
        import torch
    except ImportError as exc:
        raise DependencyUnavailableError("PyTorch is unavailable in the isolated tracking venv") from exc
    target = torch.device(device)
    if target.type == "cuda" and not torch.cuda.is_available():
        raise DependencyUnavailableError("CUDA was requested but PyTorch cannot access a GPU")
    if require_cuda and target.type != "cuda":
        raise DependencyUnavailableError("This production backend requires CUDA; pass an explicit test backend for CPU")
    torch.manual_seed(0)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(0)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.set_float32_matmul_precision("highest")
    torch.use_deterministic_algorithms(True, warn_only=False)
    return torch


@dataclass
class TapNextPPBackend:
    repo: Path
    checkpoint: Path
    lock: RuntimeLock
    device: str = "cuda"
    half_precision: bool = True
    require_cuda: bool = True

    def __post_init__(self) -> None:
        self.repo = Path(self.repo).resolve()
        self.checkpoint = Path(self.checkpoint).resolve()
        self.repo_provenance = self.lock.verify_repo("tapnet", self.repo)
        self.checkpoint_provenance = self.lock.verify_checkpoint(
            "tapnextpp", self.checkpoint, license_repo=self.repo
        )

    def track(self, video: VideoFrames, seeds: SeedSet) -> TrackResult:
        torch = _torch(self.device, require_cuda=self.require_cuda)
        _prepend_import_root(self.repo)
        try:
            from tapnet.tapnext import tapnext_torch_utils
            from tapnet.tapnextpp.votsp2026 import utils
            from tapnet.tapnextpp.votsp2026.model import TAPNextPP
        except Exception as exc:
            raise DependencyUnavailableError(f"Pinned TAPNext++ import failed: {exc}") from exc
        device = torch.device(self.device)
        model = TAPNextPP.from_checkpoint(
            self.checkpoint,
            device=device,
            half_precision=self.half_precision,
            compile_model=False,
            input_resolution=256,
        )
        frame_count, query_count = video.frame_count, len(seeds.track_ids)
        points = np.empty((frame_count, query_count, 2), dtype=np.float32)
        visible = np.empty((frame_count, query_count), dtype=bool)
        confidence = np.empty((frame_count, query_count), dtype=np.float32)
        state = None
        dtype = torch.float16 if self.half_precision and device.type == "cuda" else torch.float32
        try:
            for frame_index, frame_bgr in enumerate(video.frames_bgr):
                frame_t = utils.preprocess_frame(frame_bgr, device, model.input_resolution)
                query_t = None
                if state is None:
                    model_points = utils.display_to_model(
                        seeds.points_xy,
                        video.height,
                        video.width,
                        model.MODEL_SIZE,
                    )
                    query_t = utils.make_query_tensor(model_points, device)
                context = (
                    torch.amp.autocast("cuda", dtype=dtype)
                    if device.type == "cuda"
                    else torch.amp.autocast("cpu", enabled=False)
                )
                with torch.inference_mode(), context:
                    tracks_t, logits_t, visible_logits_t, state = model._model(
                        video=frame_t,
                        query_points=query_t,
                        state=state,
                    )
                    certainty_t = tapnext_torch_utils.tracker_certainty(tracks_t, logits_t)
                    visibility_probability_t = torch.sigmoid(visible_logits_t)
                    confidence_t = torch.clamp(certainty_t * visibility_probability_t, 0.0, 1.0)
                model_xy = tracks_t[0, 0].detach().cpu().float().numpy()[:, ::-1].copy()
                points[frame_index] = utils.model_to_display(
                    model_xy,
                    video.height,
                    video.width,
                    model.MODEL_SIZE,
                )
                visible[frame_index] = (visible_logits_t[0, 0, :, 0] > 0).detach().cpu().numpy()
                confidence[frame_index] = confidence_t[0, 0, :, 0].detach().cpu().float().numpy()
        finally:
            del model
            del state
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        return TrackResult(
            points_xy=points,
            visible=visible,
            confidence=confidence,
            provenance={
                "backend": "google-deepmind-tapnextpp-online",
                "repo": self.repo_provenance,
                "checkpoint": self.checkpoint_provenance,
                "device": str(device),
                "half_precision": self.half_precision,
                "torch": torch.__version__,
                "deterministic_algorithms": True,
                "cublas_workspace_config": _CUBLAS_WORKSPACE_CONFIG,
                "query_count": query_count,
                "input_resolution": 256,
            },
        )


@dataclass
class Sam2VideoMaskBackend:
    repo: Path
    checkpoint: Path
    lock: RuntimeLock
    device: str = "cuda"
    require_cuda: bool = True

    def __post_init__(self) -> None:
        self.repo = Path(self.repo).resolve()
        self.checkpoint = Path(self.checkpoint).resolve()
        self.repo_provenance = self.lock.verify_repo("sam2", self.repo)
        self.checkpoint_provenance = self.lock.verify_checkpoint(
            "sam2_hiera_tiny", self.checkpoint, license_repo=self.repo
        )

    def segment(self, video: VideoFrames, initial_mask: np.ndarray) -> MaskResult:
        torch = _torch(self.device, require_cuda=self.require_cuda)
        _prepend_import_root(self.repo)
        try:
            import cv2
            from sam2.build_sam import build_sam2_video_predictor
        except Exception as exc:
            raise DependencyUnavailableError(f"Pinned SAM 2 import failed: {exc}") from exc
        device = torch.device(self.device)
        predictor = build_sam2_video_predictor(
            "configs/sam2.1/sam2.1_hiera_t.yaml",
            str(self.checkpoint),
            device=device,
            apply_postprocessing=False,
        )
        masks = np.zeros((video.frame_count, video.height, video.width), dtype=bool)
        confidence = np.zeros(video.frame_count, dtype=np.float32)
        state = None
        try:
            with tempfile.TemporaryDirectory(prefix="autorig-sam2-frames-") as frame_root:
                frame_dir = Path(frame_root)
                for index, frame in enumerate(video.frames_bgr):
                    path = frame_dir / f"{index:06d}.jpg"
                    if not cv2.imwrite(str(path), frame, [cv2.IMWRITE_JPEG_QUALITY, 97]):
                        raise ContractError(f"Cannot write SAM 2 staging frame: {path}")
                with torch.inference_mode():
                    state = predictor.init_state(
                        video_path=str(frame_dir),
                        offload_video_to_cpu=True,
                        offload_state_to_cpu=False,
                        async_loading_frames=False,
                    )
                    predictor.add_new_mask(
                        inference_state=state,
                        frame_idx=0,
                        obj_id=1,
                        mask=np.asarray(initial_mask, dtype=bool),
                    )
                    seen: set[int] = set()
                    for frame_index, object_ids, logits in predictor.propagate_in_video(state):
                        normalized_ids = [int(value) for value in object_ids]
                        if 1 not in normalized_ids:
                            raise ContractError(f"SAM 2 lost object id 1 at frame {frame_index}")
                        object_index = normalized_ids.index(1)
                        score = logits[object_index, 0]
                        masks[frame_index] = (score > 0).detach().cpu().numpy()
                        foreground = score[score > 0]
                        confidence[frame_index] = (
                            float(torch.sigmoid(foreground).mean().detach().cpu())
                            if foreground.numel()
                            else 0.0
                        )
                        seen.add(int(frame_index))
                    expected = set(range(video.frame_count))
                    if seen != expected:
                        raise ContractError(
                            f"SAM 2 output frames are incomplete; missing={sorted(expected - seen)}"
                        )
        finally:
            if state is not None:
                try:
                    predictor.reset_state(state)
                except Exception:
                    pass
            del predictor
            del state
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        return MaskResult(
            masks=masks,
            confidence=confidence,
            provenance={
                "backend": "facebookresearch-sam2.1-video",
                "model": "sam2.1_hiera_tiny",
                "repo": self.repo_provenance,
                "checkpoint": self.checkpoint_provenance,
                "device": str(device),
                "torch": torch.__version__,
                "seed": "canonical_reference_mask_frame_0",
                "jpeg_staging_quality": 97,
                "postprocessing": False,
            },
        )


@dataclass
class VideoDepthAnythingSmallBackend:
    repo: Path
    checkpoint: Path
    lock: RuntimeLock
    device: str = "cuda"
    require_cuda: bool = True
    input_size: int = 518

    def __post_init__(self) -> None:
        self.repo = Path(self.repo).resolve()
        self.checkpoint = Path(self.checkpoint).resolve()
        self.repo_provenance = self.lock.verify_repo("video_depth_anything", self.repo)
        self.checkpoint_provenance = self.lock.verify_checkpoint(
            "video_depth_anything_small", self.checkpoint, license_repo=self.repo
        )

    def infer(self, video: VideoFrames) -> DepthResult:
        torch = _torch(self.device, require_cuda=self.require_cuda)
        _prepend_import_root(self.repo)
        try:
            import cv2
            from video_depth_anything.video_depth import VideoDepthAnything
        except Exception as exc:
            raise DependencyUnavailableError(f"Pinned Video Depth Anything import failed: {exc}") from exc
        device = torch.device(self.device)
        model = VideoDepthAnything(
            encoder="vits",
            features=64,
            out_channels=[48, 96, 192, 384],
            metric=False,
        )
        state = torch.load(self.checkpoint, map_location="cpu", weights_only=True)
        model.load_state_dict(state, strict=True)
        model = model.to(device).eval()
        frames_rgb = np.stack(
            [cv2.cvtColor(frame, cv2.COLOR_BGR2RGB) for frame in video.frames_bgr]
        )
        try:
            depths, returned_fps = model.infer_video_depth(
                frames_rgb,
                video.fps,
                input_size=self.input_size,
                device=str(device),
                fp32=True,
            )
        finally:
            del model
            del state
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        if abs(float(returned_fps) - video.fps) > 1e-6:
            raise ContractError(
                f"Video Depth Anything changed FPS from {video.fps} to {returned_fps}"
            )
        return DepthResult(
            relative_depth=np.asarray(depths, dtype=np.float32),
            provenance={
                "backend": "depthanything-video-depth-anything-small",
                "encoder": "vits",
                "metric": False,
                "repo": self.repo_provenance,
                "checkpoint": self.checkpoint_provenance,
                "device": str(device),
                "torch": torch.__version__,
                "input_size": self.input_size,
                "fp32": True,
            },
        )

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
import json
import math
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from .errors import ContractError, DependencyUnavailableError, OptimizationError
from .math3d import flatten_matrix, quaternion_xyzw_from_matrix
from .observations import ObservationSet
from .rig import Camera, RigBundle


FITTED_ANIMATION_SCHEMA = "autorig-fitted-animation.v1"


@dataclass(frozen=True)
class FittingConfig:
    reprojection_weight: float = 1.0
    depth_weight: float = 1.0
    silhouette_weight: float = 0.25
    joint_rest_weight: float = 0.0025
    joint_velocity_weight: float = 0.02
    joint_acceleration_weight: float = 0.05
    root_velocity_weight: float = 0.01
    root_acceleration_weight: float = 0.025
    contact_height_weight: float = 2.0
    contact_slide_weight: float = 2.0
    loop_pose_weight: float = 2.0
    loop_velocity_weight: float = 1.0
    robust_loss: str = "soft_l1"
    f_scale: float = 3.0
    max_nfev: int = 300
    max_variables: int = 12000
    allow_unbounded_joints: bool = False
    active_bones: Optional[tuple[str, ...]] = None

    def validate(self) -> None:
        weight_names = [name for name in asdict(self) if name.endswith("_weight")]
        for name in weight_names:
            value = float(getattr(self, name))
            if not np.isfinite(value) or value < 0.0:
                raise ContractError(
                    f"Fitting weight {name} must be finite and non-negative"
                )
        if self.robust_loss not in {"linear", "soft_l1", "huber", "cauchy", "arctan"}:
            raise ContractError(
                f"Unsupported scipy least_squares loss: {self.robust_loss}"
            )
        if not np.isfinite(self.f_scale) or self.f_scale <= 0.0:
            raise ContractError("f_scale must be finite and positive")
        if (
            isinstance(self.max_nfev, bool)
            or not isinstance(self.max_nfev, int)
            or isinstance(self.max_variables, bool)
            or not isinstance(self.max_variables, int)
            or self.max_nfev <= 0
            or self.max_variables <= 0
        ):
            raise ContractError("max_nfev and max_variables must be positive")
        if not isinstance(self.allow_unbounded_joints, bool):
            raise ContractError("allow_unbounded_joints must be boolean")
        if self.active_bones is not None:
            if not isinstance(self.active_bones, tuple) or any(
                not isinstance(name, str) or not name for name in self.active_bones
            ):
                raise ContractError(
                    "active_bones must be a tuple of non-empty strings or null"
                )
            if len(set(self.active_bones)) != len(self.active_bones):
                raise ContractError("active_bones must be unique")

    @classmethod
    def from_json(cls, path: str | Path) -> "FittingConfig":
        source = Path(path).resolve()
        try:
            payload = json.loads(source.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ContractError(f"Invalid optimizer config {source}: {exc}") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema") != "autorig-fitting-config.v1"
        ):
            raise ContractError(
                "Optimizer config schema must be autorig-fitting-config.v1"
            )
        values = dict(payload)
        values.pop("schema", None)
        allowed = {field.name for field in fields(cls)}
        unknown = set(values).difference(allowed)
        if unknown:
            raise ContractError(f"Unknown optimizer config fields: {sorted(unknown)}")
        if "active_bones" in values and values["active_bones"] is not None:
            if not isinstance(values["active_bones"], list):
                raise ContractError("active_bones must be an array or null")
            values["active_bones"] = tuple(
                str(value) for value in values["active_bones"]
            )
        config = cls(**values)
        config.validate()
        return config


@dataclass(frozen=True)
class JointDof:
    bone: str
    axis: int
    lower: float
    upper: float

    @property
    def name(self) -> str:
        return f"{self.bone}.{('x', 'y', 'z')[self.axis]}"


@dataclass
class FittingResult:
    rig: RigBundle
    observations: ObservationSet
    config: FittingConfig
    loop: bool
    active_bones: tuple[str, ...]
    dofs: tuple[JointDof, ...]
    parameters: np.ndarray
    optimizer: dict
    qa: dict

    @property
    def frame_width(self) -> int:
        return 6 + len(self.dofs)

    def frame_parameters(self, frame: int) -> np.ndarray:
        return self.parameters.reshape(self.observations.frame_count, self.frame_width)[
            frame
        ]

    def frame_state(
        self, frame: int
    ) -> tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
        values = self.frame_parameters(frame)
        joint_eulers = {
            name: np.zeros(3, dtype=np.float64) for name in self.active_bones
        }
        for index, dof in enumerate(self.dofs):
            joint_eulers[dof.bone][dof.axis] = values[6 + index]
        return self.rig.forward_kinematics(values[:3], values[3:6], joint_eulers)

    def to_dict(self) -> dict:
        frames_payload = []
        for frame in range(self.observations.frame_count):
            values = self.frame_parameters(frame)
            world, local = self.frame_state(frame)
            bones_payload = {}
            for name in self.rig.bone_order:
                matrix = local[name]
                bones_payload[name] = {
                    "parent": self.rig.bones[name].parent,
                    "local_matrix": flatten_matrix(matrix),
                    "local_translation": [float(value) for value in matrix[:3, 3]],
                    "local_rotation_xyzw": quaternion_xyzw_from_matrix(matrix),
                }
            frames_payload.append(
                {
                    "frame": frame,
                    "time_seconds": frame / self.observations.fps,
                    "root_translation": [float(value) for value in values[:3]],
                    "root_rotation_rotvec": [float(value) for value in values[3:6]],
                    "bones": bones_payload,
                }
            )
        return {
            "schema": FITTED_ANIMATION_SCHEMA,
            "rig_bundle": {
                "path": str(self.rig.metadata_path),
                "sha256": self.rig.metadata_sha256,
            },
            "observations": {
                "path": str(self.observations.path),
                "sha256": self.observations.sha256,
                "provenance": self.observations.provenance,
            },
            "frame_count": self.observations.frame_count,
            "fps": self.observations.fps,
            "duration_seconds": (
                (self.observations.frame_count - 1) / self.observations.fps
                if self.observations.frame_count > 1
                else 0.0
            ),
            "loop": self.loop,
            "transform_contract": {
                "schema": "autorig-fitted-transform-contract.v1",
                "source_armature_name": self.rig.armature_name,
                "source_armature_world_matrix": flatten_matrix(self.rig.armature_world),
                "root_local_matrix_space": "WORLD",
                "child_local_matrix_space": "PARENT_BONE",
                "rotation_channel": "QUATERNION",
                "scale_animation": False,
                "translation_policy": {
                    "mode": "root_only",
                    "bones": [
                        name
                        for name in self.rig.bone_order
                        if self.rig.bones[name].parent is None
                    ],
                },
            },
            "active_bones": list(self.active_bones),
            "degrees_of_freedom": [dof.name for dof in self.dofs],
            "config": _json_safe(asdict(self.config)),
            "optimizer": _json_safe(self.optimizer),
            "qa": _json_safe(self.qa),
            "frames": frames_payload,
        }

    def save(self, path: str | Path) -> Path:
        destination = Path(path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=True, allow_nan=False)
            + "\n",
            encoding="utf-8",
        )
        temporary.replace(destination)
        return destination


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(item) for item in value.tolist()]
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if np.isfinite(number) else None
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    return value


def _build_dofs(
    rig: RigBundle, active_bones: tuple[str, ...], allow_unbounded: bool
) -> tuple[JointDof, ...]:
    result: list[JointDof] = []
    for name in active_bones:
        bounds = rig.bones[name].joint_bounds
        for axis in range(3):
            if bounds.constrained_axes[axis]:
                result.append(
                    JointDof(
                        name, axis, float(bounds.lower[axis]), float(bounds.upper[axis])
                    )
                )
            elif allow_unbounded:
                # Euler coordinates are represented on their principal interval; this is
                # a numerical representation bound, not an anatomical QA claim.
                result.append(JointDof(name, axis, -math.pi, math.pi))
    if not result:
        raise ContractError(
            "No optimizable joint axes remain. Export LOCAL LIMIT_ROTATION constraints "
            "or explicitly enable allow_unbounded_joints."
        )
    return tuple(result)


def _observation_camera(camera: Camera, *, width: int, height: int) -> Camera:
    if (
        isinstance(width, bool)
        or not isinstance(width, int)
        or isinstance(height, bool)
        or not isinstance(height, int)
        or width < 1
        or height < 1
    ):
        raise ContractError(
            "Observation camera resolution must contain positive integers"
        )
    scale_x = width / camera.width
    scale_y = height / camera.height
    if not math.isclose(scale_x, scale_y, rel_tol=1e-12, abs_tol=1e-12):
        raise ContractError(
            f"Observation resolution {width}x{height} changes the immutable camera aspect "
            f"ratio from {camera.width}x{camera.height}; arbitrary intrinsics are not allowed"
        )
    return Camera(
        width=width,
        height=height,
        fx=camera.fx * scale_x,
        fy=camera.fy * scale_y,
        cx=camera.cx * scale_x,
        cy=camera.cy * scale_y,
        world_to_camera=camera.world_to_camera,
    )


def _camera_contract(source: Camera, observation: Camera) -> dict[str, Any]:
    return {
        "mode": "immutable_bundle_intrinsics_uniformly_scaled",
        "source_resolution": [source.width, source.height],
        "observation_resolution": [observation.width, observation.height],
        "scale": observation.width / source.width,
        "source_intrinsics": {
            "fx": source.fx,
            "fy": source.fy,
            "cx": source.cx,
            "cy": source.cy,
        },
        "intrinsics": {
            "fx": observation.fx,
            "fy": observation.fy,
            "cx": observation.cx,
            "cy": observation.cy,
        },
        "world_to_camera": "immutable_bundle_exact",
    }


def _validate_contracts(rig: RigBundle, observations: ObservationSet) -> Camera:
    camera = _observation_camera(
        rig.camera,
        width=observations.width,
        height=observations.height,
    )
    track_by_anchor = observations.track_by_anchor
    unknown = set(track_by_anchor).difference(rig.anchors)
    if unknown:
        raise ContractError(
            f"Observations reference unknown rig anchors: {sorted(unknown)}"
        )
    contact_unknown = {
        contact.anchor_id for contact in observations.contacts
    }.difference(rig.anchors)
    if contact_unknown:
        raise ContractError(
            f"Contacts reference unknown rig anchors: {sorted(contact_unknown)}"
        )
    if observations.frame_count < 2:
        raise ContractError("Fitting requires at least two frames")
    return camera


def _decode_states(
    vector: np.ndarray,
    rig: RigBundle,
    frame_count: int,
    active_bones: tuple[str, ...],
    dofs: tuple[JointDof, ...],
) -> tuple[
    np.ndarray,
    list[Dict[str, np.ndarray]],
    list[Dict[str, np.ndarray]],
    list[Dict[str, np.ndarray]],
]:
    values = vector.reshape(frame_count, 6 + len(dofs))
    worlds: list[Dict[str, np.ndarray]] = []
    locals_: list[Dict[str, np.ndarray]] = []
    anchors: list[Dict[str, np.ndarray]] = []
    for frame in range(frame_count):
        joint_eulers = {name: np.zeros(3, dtype=np.float64) for name in active_bones}
        for index, dof in enumerate(dofs):
            joint_eulers[dof.bone][dof.axis] = values[frame, 6 + index]
        world, local = rig.forward_kinematics(
            values[frame, :3], values[frame, 3:6], joint_eulers
        )
        worlds.append(world)
        locals_.append(local)
        anchors.append(rig.anchor_positions(world))
    return values, worlds, locals_, anchors


def _project_safe(camera: Camera, point: np.ndarray) -> tuple[np.ndarray, float, float]:
    homogeneous = np.concatenate((point, np.ones(1, dtype=np.float64)))
    camera_point = (camera.world_to_camera @ homogeneous)[:3]
    depth = -float(camera_point[2])
    safe_depth = max(depth, 1e-4)
    xy = np.asarray(
        (
            camera.fx * float(camera_point[0]) / safe_depth + camera.cx,
            camera.cy - camera.fy * float(camera_point[1]) / safe_depth,
        ),
        dtype=np.float64,
    )
    behind_penalty = max(1e-4 - depth, 0.0) * ((camera.fx + camera.fy) * 0.5)
    return xy, depth, behind_penalty


def _sample_distance(field: np.ndarray, xy: np.ndarray) -> float:
    height, width = field.shape
    x, y = float(xy[0]), float(xy[1])
    clipped_x = min(max(x, 0.0), width - 1.0)
    clipped_y = min(max(y, 0.0), height - 1.0)
    x0, y0 = int(math.floor(clipped_x)), int(math.floor(clipped_y))
    x1, y1 = min(x0 + 1, width - 1), min(y0 + 1, height - 1)
    tx, ty = clipped_x - x0, clipped_y - y0
    sampled = (
        (1.0 - tx) * (1.0 - ty) * field[y0, x0]
        + tx * (1.0 - ty) * field[y0, x1]
        + (1.0 - tx) * ty * field[y1, x0]
        + tx * ty * field[y1, x1]
    )
    boundary = math.hypot(x - clipped_x, y - clipped_y)
    return float(sampled + boundary)


def _depth_targets(observations: ObservationSet) -> Dict[tuple[int, str], float]:
    result: Dict[tuple[int, str], float] = {}
    for track in observations.tracks:
        for point in track.points:
            depth = observations.depths.get(point.frame)
            if not point.visible or depth is None:
                continue
            x, y = int(round(float(point.xy[0]))), int(round(float(point.xy[1])))
            if (
                0 <= x < observations.width
                and 0 <= y < observations.height
                and depth.valid[y, x]
            ):
                result[(point.frame, track.anchor_id)] = float(depth.camera_depth[y, x])
    return result


def _residual_function(
    rig: RigBundle,
    camera: Camera,
    observations: ObservationSet,
    config: FittingConfig,
    active_bones: tuple[str, ...],
    dofs: tuple[JointDof, ...],
    loop: bool,
):
    depth_targets = _depth_targets(observations)
    tracked_anchors = observations.anchor_ids
    n = rig.ground_normal

    def residual(vector: np.ndarray) -> np.ndarray:
        values, _, _, anchors = _decode_states(
            vector, rig, observations.frame_count, active_bones, dofs
        )
        rows: list[float] = []

        reprojection_scale = math.sqrt(config.reprojection_weight)
        if reprojection_scale:
            for track in observations.tracks:
                for point in track.points:
                    if not point.visible:
                        continue
                    predicted, _, behind = _project_safe(
                        camera, anchors[point.frame][track.anchor_id]
                    )
                    confidence = 1.0 if point.confidence is None else point.confidence
                    scale = reprojection_scale * math.sqrt(confidence)
                    rows.extend(((predicted - point.xy) * scale).tolist())
                    rows.append(behind * scale)

        depth_scale = math.sqrt(config.depth_weight)
        if depth_scale:
            for (frame, anchor_id), target in depth_targets.items():
                _, predicted_depth, _ = _project_safe(camera, anchors[frame][anchor_id])
                rows.append((predicted_depth - target) * depth_scale)

        silhouette_scale = math.sqrt(config.silhouette_weight)
        if silhouette_scale:
            for frame, silhouette in observations.silhouettes.items():
                for anchor_id in tracked_anchors:
                    predicted, _, behind = _project_safe(
                        camera, anchors[frame][anchor_id]
                    )
                    rows.append(
                        (
                            _sample_distance(silhouette.outside_distance, predicted)
                            + behind
                        )
                        * silhouette_scale
                    )

        joint_values = values[:, 6:]
        if config.joint_rest_weight:
            rows.extend(
                (joint_values * math.sqrt(config.joint_rest_weight))
                .reshape(-1)
                .tolist()
            )
        if observations.frame_count > 1:
            root_velocity = np.diff(values[:, :6], axis=0)
            joint_velocity = np.diff(joint_values, axis=0)
            if config.root_velocity_weight:
                rows.extend(
                    (root_velocity * math.sqrt(config.root_velocity_weight))
                    .reshape(-1)
                    .tolist()
                )
            if config.joint_velocity_weight:
                rows.extend(
                    (joint_velocity * math.sqrt(config.joint_velocity_weight))
                    .reshape(-1)
                    .tolist()
                )
        if observations.frame_count > 2:
            root_accel = np.diff(values[:, :6], n=2, axis=0)
            joint_accel = np.diff(joint_values, n=2, axis=0)
            if config.root_acceleration_weight:
                rows.extend(
                    (root_accel * math.sqrt(config.root_acceleration_weight))
                    .reshape(-1)
                    .tolist()
                )
            if config.joint_acceleration_weight:
                rows.extend(
                    (joint_accel * math.sqrt(config.joint_acceleration_weight))
                    .reshape(-1)
                    .tolist()
                )

        for contact in observations.contacts:
            height_scale = math.sqrt(config.contact_height_weight * contact.weight)
            slide_scale = math.sqrt(config.contact_slide_weight * contact.weight)
            height = (
                rig.ground_height
                if contact.ground_height is None
                else contact.ground_height
            )
            if height_scale:
                for frame in contact.frames:
                    rows.append(
                        (float(np.dot(n, anchors[frame][contact.anchor_id])) - height)
                        * height_scale
                    )
            if slide_scale:
                for first, second in zip(contact.frames, contact.frames[1:]):
                    if second != first + 1:
                        continue
                    delta = (
                        anchors[second][contact.anchor_id]
                        - anchors[first][contact.anchor_id]
                    )
                    tangent = delta - n * float(np.dot(n, delta))
                    rows.extend((tangent * slide_scale).tolist())

        if loop:
            if config.loop_pose_weight:
                rows.extend(
                    (
                        (values[-1] - values[0]) * math.sqrt(config.loop_pose_weight)
                    ).tolist()
                )
            if observations.frame_count > 2 and config.loop_velocity_weight:
                first_velocity = values[1] - values[0]
                last_velocity = values[-1] - values[-2]
                rows.extend(
                    (
                        (last_velocity - first_velocity)
                        * math.sqrt(config.loop_velocity_weight)
                    ).tolist()
                )
        if not rows:
            raise OptimizationError(
                "All fitting residual weights are zero and no objective remains"
            )
        result = np.asarray(rows, dtype=np.float64)
        if not np.all(np.isfinite(result)):
            raise OptimizationError("Optimizer produced a non-finite residual")
        return result

    return residual


def _jacobian_sparsity(
    observations: ObservationSet,
    config: FittingConfig,
    dofs: tuple[JointDof, ...],
    loop: bool,
):
    """Conservative temporal sparsity pattern for finite-difference grouping.

    Each measurement is marked against all parameters in the frame(s) it can
    touch. This intentionally over-approximates kinematic ancestry but avoids a
    dense F*P numerical Jacobian on real clips.
    """
    try:
        from scipy.sparse import lil_matrix
    except ImportError as exc:
        raise DependencyUnavailableError(
            "SciPy is required for sparse temporal fitting. Install it with: python -m pip install scipy"
        ) from exc
    frame_count = observations.frame_count
    frame_width = 6 + len(dofs)
    dependencies: list[tuple[int, ...]] = []

    if config.reprojection_weight:
        for track in observations.tracks:
            for point in track.points:
                if point.visible:
                    dependencies.extend([(point.frame,)] * 3)
    if config.depth_weight:
        for frame, _ in _depth_targets(observations):
            dependencies.append((frame,))
    if config.silhouette_weight:
        for frame in observations.silhouettes:
            dependencies.extend([(frame,)] * len(observations.anchor_ids))
    if config.joint_rest_weight:
        for frame in range(frame_count):
            dependencies.extend([(frame,)] * len(dofs))
    if frame_count > 1:
        if config.root_velocity_weight:
            for frame in range(frame_count - 1):
                dependencies.extend([(frame, frame + 1)] * 6)
        if config.joint_velocity_weight:
            for frame in range(frame_count - 1):
                dependencies.extend([(frame, frame + 1)] * len(dofs))
    if frame_count > 2:
        if config.root_acceleration_weight:
            for frame in range(frame_count - 2):
                dependencies.extend([(frame, frame + 1, frame + 2)] * 6)
        if config.joint_acceleration_weight:
            for frame in range(frame_count - 2):
                dependencies.extend([(frame, frame + 1, frame + 2)] * len(dofs))
    for contact in observations.contacts:
        if config.contact_height_weight:
            dependencies.extend((frame,) for frame in contact.frames)
        if config.contact_slide_weight:
            for first, second in zip(contact.frames, contact.frames[1:]):
                if second == first + 1:
                    dependencies.extend([(first, second)] * 3)
    if loop:
        if config.loop_pose_weight:
            dependencies.extend([(0, frame_count - 1)] * frame_width)
        if frame_count > 2 and config.loop_velocity_weight:
            dependencies.extend(
                [(0, 1, frame_count - 2, frame_count - 1)] * frame_width
            )

    matrix = lil_matrix((len(dependencies), frame_count * frame_width), dtype=np.int8)
    for row, frames_used in enumerate(dependencies):
        for frame in frames_used:
            start = frame * frame_width
            matrix[row, start : start + frame_width] = 1
    return matrix.tocsr()


def _summary(values: list[float]) -> Optional[dict]:
    if not values:
        return None
    array = np.asarray(values, dtype=np.float64)
    return {
        "count": int(array.size),
        "mean": float(np.mean(array)),
        "rmse": float(math.sqrt(float(np.mean(array * array)))),
        "median": float(np.median(array)),
        "p95": float(np.percentile(array, 95)),
        "max": float(np.max(array)),
    }


def _compute_qa(
    vector: np.ndarray,
    rig: RigBundle,
    camera: Camera,
    observations: ObservationSet,
    active_bones: tuple[str, ...],
    dofs: tuple[JointDof, ...],
    optimizer: dict,
    loop: bool,
) -> dict:
    values, worlds, _, anchors = _decode_states(
        vector, rig, observations.frame_count, active_bones, dofs
    )
    reprojection_errors: list[float] = []
    visible_samples = 0
    behind_camera = 0
    per_track = []
    for track in observations.tracks:
        track_errors: list[float] = []
        track_visible = 0
        for point in track.points:
            if not point.visible:
                continue
            visible_samples += 1
            track_visible += 1
            predicted, depth, _ = _project_safe(
                camera, anchors[point.frame][track.anchor_id]
            )
            if depth <= 0.0:
                behind_camera += 1
                continue
            error = float(np.linalg.norm(predicted - point.xy))
            reprojection_errors.append(error)
            track_errors.append(error)
        per_track.append(
            {
                "track_id": track.id,
                "anchor_id": track.anchor_id,
                "sample_count": len(track.points),
                "visible_count": track_visible,
                "visibility_fraction": track_visible / len(track.points),
                "reprojection_px": _summary(track_errors),
            }
        )

    depth_errors: list[float] = []
    for (frame, anchor_id), target in _depth_targets(observations).items():
        _, predicted_depth, _ = _project_safe(camera, anchors[frame][anchor_id])
        depth_errors.append(predicted_depth - target)

    silhouette_distances: list[float] = []
    for frame, silhouette in observations.silhouettes.items():
        for anchor_id in observations.anchor_ids:
            predicted, _, _ = _project_safe(camera, anchors[frame][anchor_id])
            silhouette_distances.append(
                _sample_distance(silhouette.outside_distance, predicted)
            )

    contact_height_errors: list[float] = []
    contact_slides: list[float] = []
    n = rig.ground_normal
    for contact in observations.contacts:
        height = (
            rig.ground_height
            if contact.ground_height is None
            else contact.ground_height
        )
        for frame in contact.frames:
            contact_height_errors.append(
                float(np.dot(n, anchors[frame][contact.anchor_id])) - height
            )
        for first, second in zip(contact.frames, contact.frames[1:]):
            if second != first + 1:
                continue
            delta = (
                anchors[second][contact.anchor_id] - anchors[first][contact.anchor_id]
            )
            tangent = delta - n * float(np.dot(n, delta))
            contact_slides.append(float(np.linalg.norm(tangent)))

    joint_violations: list[float] = []
    for index, dof in enumerate(dofs):
        axis_values = values[:, 6 + index]
        joint_violations.extend(np.maximum(dof.lower - axis_values, 0.0).tolist())
        joint_violations.extend(np.maximum(axis_values - dof.upper, 0.0).tolist())

    morphology_errors: list[float] = []
    for name in rig.bone_order:
        bone = rig.bones[name]
        if bone.parent is None:
            continue
        rest_distance = float(
            np.linalg.norm(
                bone.rest_world[:3, 3] - rig.bones[bone.parent].rest_world[:3, 3]
            )
        )
        for frame in range(observations.frame_count):
            current = float(
                np.linalg.norm(
                    worlds[frame][name][:3, 3] - worlds[frame][bone.parent][:3, 3]
                )
            )
            morphology_errors.append(current - rest_distance)

    root_jerk = (
        np.diff(values[:, :3], n=3, axis=0)
        if observations.frame_count > 3
        else np.empty((0, 3))
    )
    joint_jerk = (
        np.diff(values[:, 6:], n=3, axis=0)
        if observations.frame_count > 3
        else np.empty((0, len(dofs)))
    )
    loop_metrics = None
    if loop:
        loop_metrics = {
            "parameter_pose_l2": float(np.linalg.norm(values[-1] - values[0])),
            "root_translation_l2": float(
                np.linalg.norm(values[-1, :3] - values[0, :3])
            ),
            "root_rotation_rotvec_l2": float(
                np.linalg.norm(values[-1, 3:6] - values[0, 3:6])
            ),
            "joint_euler_l2": float(np.linalg.norm(values[-1, 6:] - values[0, 6:])),
            "velocity_l2": (
                float(
                    np.linalg.norm((values[-1] - values[-2]) - (values[1] - values[0]))
                )
                if observations.frame_count > 2
                else None
            ),
        }
    return {
        "decision": None,
        "decision_note": "Raw measurements only; no pass/fail threshold or confidence is inferred.",
        "camera_projection": _camera_contract(rig.camera, camera),
        "optimizer": optimizer,
        "tracks": {
            "track_count": len(observations.tracks),
            "sample_count": sum(len(track.points) for track in observations.tracks),
            "visible_count": visible_samples,
            "visibility_fraction": visible_samples
            / sum(len(track.points) for track in observations.tracks),
            "behind_camera_visible_count": behind_camera,
            "reprojection_px": _summary(reprojection_errors),
            "per_track": per_track,
        },
        "depth_camera_units_error": _summary(depth_errors),
        "silhouette_outside_distance_px": _summary(silhouette_distances),
        "contact_height_error_world_units": _summary(contact_height_errors),
        "contact_tangent_slide_world_units": _summary(contact_slides),
        "joint_limit_violation_radians": _summary(joint_violations),
        "root_translation_jerk_world_units_per_frame3": _summary(
            np.linalg.norm(root_jerk, axis=1).tolist()
        ),
        "joint_euler_jerk_radians_per_frame3": _summary(
            np.linalg.norm(joint_jerk, axis=1).tolist()
        ),
        "bone_origin_distance_error_world_units": _summary(morphology_errors),
        "loop_seam": loop_metrics,
    }


def fit_sequence(
    rig: RigBundle,
    observations: ObservationSet,
    *,
    loop: bool,
    config: Optional[FittingConfig] = None,
) -> FittingResult:
    if not isinstance(loop, bool):
        raise ContractError("loop must be explicitly true or false")
    config = config or FittingConfig()
    config.validate()
    camera = _validate_contracts(rig, observations)
    active_bones = rig.select_active_bones(
        observations.anchor_ids,
        explicit=config.active_bones,
        require_joint_limits=not config.allow_unbounded_joints,
    )
    dofs = _build_dofs(rig, active_bones, config.allow_unbounded_joints)
    frame_width = 6 + len(dofs)
    variable_count = observations.frame_count * frame_width
    if variable_count > config.max_variables:
        raise ContractError(
            f"Optimization would use {variable_count} variables, exceeding max_variables={config.max_variables}. "
            "Reduce active_bones/frame count or intentionally raise max_variables in the config."
        )
    lower_frame = np.concatenate(
        (
            np.full(3, -np.inf),
            np.full(3, -math.pi),
            np.asarray([dof.lower for dof in dofs]),
        )
    )
    upper_frame = np.concatenate(
        (
            np.full(3, np.inf),
            np.full(3, math.pi),
            np.asarray([dof.upper for dof in dofs]),
        )
    )
    lower = np.tile(lower_frame, observations.frame_count)
    upper = np.tile(upper_frame, observations.frame_count)
    initial = np.clip(np.zeros(variable_count, dtype=np.float64), lower, upper)
    objective = _residual_function(
        rig,
        camera,
        observations,
        config,
        active_bones,
        dofs,
        loop,
    )
    jac_sparsity = _jacobian_sparsity(observations, config, dofs, loop)
    initial_residual_count = int(objective(initial).size)
    if jac_sparsity.shape[0] != initial_residual_count:
        raise OptimizationError(
            "Internal residual/Jacobian contract mismatch: "
            f"{initial_residual_count} residuals vs {jac_sparsity.shape[0]} sparse rows"
        )
    try:
        from scipy.optimize import least_squares
    except ImportError as exc:
        raise DependencyUnavailableError(
            "SciPy is required for temporal constrained fitting. Install it with: python -m pip install scipy"
        ) from exc
    try:
        result = least_squares(
            objective,
            initial,
            bounds=(lower, upper),
            method="trf",
            loss=config.robust_loss,
            f_scale=config.f_scale,
            max_nfev=config.max_nfev,
            jac_sparsity=jac_sparsity,
        )
    except (ValueError, RuntimeError, OptimizationError) as exc:
        raise OptimizationError(f"Temporal fitting failed: {exc}") from exc
    success = bool(result.success)
    status = int(result.status)
    if not success or status <= 0:
        raise OptimizationError(
            "Temporal fitting did not converge: "
            f"success={success}, status={status}, nfev={int(result.nfev)}, "
            f"message={str(result.message)!r}"
        )
    if not np.all(np.isfinite(result.x)) or not np.isfinite(result.cost):
        raise OptimizationError("Temporal fitting returned non-finite parameters/cost")
    optimizer = {
        "method": "scipy.optimize.least_squares.trf",
        "success": success,
        "status": status,
        "message": str(result.message),
        "cost": float(result.cost),
        "optimality": float(result.optimality),
        "active_mask_nonzero": int(np.count_nonzero(result.active_mask)),
        "function_evaluations": int(result.nfev),
        "jacobian_evaluations": int(result.njev) if result.njev is not None else None,
        "variable_count": variable_count,
        "residual_count": int(result.fun.size),
        "jacobian_sparsity_nonzero": int(jac_sparsity.nnz),
    }
    qa = _compute_qa(
        result.x,
        rig,
        camera,
        observations,
        active_bones,
        dofs,
        optimizer,
        loop,
    )
    return FittingResult(
        rig=rig,
        observations=observations,
        config=config,
        loop=loop,
        active_bones=active_bones,
        dofs=dofs,
        parameters=result.x,
        optimizer=optimizer,
        qa=qa,
    )

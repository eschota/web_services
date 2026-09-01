from __future__ import annotations

import math
from typing import Iterable

import numpy as np


def matrix4(values: Iterable[float], *, field: str) -> np.ndarray:
    array = np.asarray(list(values), dtype=np.float64)
    if array.shape != (16,) or not np.all(np.isfinite(array)):
        raise ValueError(f"{field} must contain 16 finite matrix values")
    return array.reshape(4, 4)


def normalize_vector(values: Iterable[float], *, field: str) -> np.ndarray:
    vector = np.asarray(list(values), dtype=np.float64)
    if vector.shape != (3,) or not np.all(np.isfinite(vector)):
        raise ValueError(f"{field} must contain three finite values")
    length = float(np.linalg.norm(vector))
    if length <= 1e-12:
        raise ValueError(f"{field} must be non-zero")
    return vector / length


def translation_matrix(translation: np.ndarray) -> np.ndarray:
    matrix = np.eye(4, dtype=np.float64)
    matrix[:3, 3] = np.asarray(translation, dtype=np.float64)
    return matrix


def rotation_matrix_xyz(euler: np.ndarray) -> np.ndarray:
    x, y, z = (float(value) for value in euler)
    cx, sx = math.cos(x), math.sin(x)
    cy, sy = math.cos(y), math.sin(y)
    cz, sz = math.cos(z), math.sin(z)
    rx = np.array(((1, 0, 0), (0, cx, -sx), (0, sx, cx)), dtype=np.float64)
    ry = np.array(((cy, 0, sy), (0, 1, 0), (-sy, 0, cy)), dtype=np.float64)
    rz = np.array(((cz, -sz, 0), (sz, cz, 0), (0, 0, 1)), dtype=np.float64)
    matrix = np.eye(4, dtype=np.float64)
    matrix[:3, :3] = rz @ ry @ rx
    return matrix


def rotation_matrix_rotvec(rotvec: np.ndarray) -> np.ndarray:
    vector = np.asarray(rotvec, dtype=np.float64)
    angle = float(np.linalg.norm(vector))
    matrix = np.eye(4, dtype=np.float64)
    if angle <= 1e-12:
        return matrix
    axis = vector / angle
    x, y, z = axis
    skew = np.array(((0, -z, y), (z, 0, -x), (-y, x, 0)), dtype=np.float64)
    rotation = np.eye(3) + math.sin(angle) * skew + (1.0 - math.cos(angle)) * (skew @ skew)
    matrix[:3, :3] = rotation
    return matrix


def quaternion_xyzw_from_matrix(matrix: np.ndarray) -> list[float]:
    linear = np.asarray(matrix, dtype=np.float64)[:3, :3]
    # Bundle matrices can contain uniform object scale. Export a pure normalized
    # rotation quaternion while keeping the complete local_matrix authoritative.
    u, _, vh = np.linalg.svd(linear)
    rotation = u @ vh
    if np.linalg.det(rotation) < 0.0:
        u[:, -1] *= -1.0
        rotation = u @ vh
    trace = float(np.trace(rotation))
    if trace > 0.0:
        s = math.sqrt(trace + 1.0) * 2.0
        qw = 0.25 * s
        qx = (rotation[2, 1] - rotation[1, 2]) / s
        qy = (rotation[0, 2] - rotation[2, 0]) / s
        qz = (rotation[1, 0] - rotation[0, 1]) / s
    else:
        index = int(np.argmax(np.diag(rotation)))
        if index == 0:
            s = math.sqrt(1.0 + rotation[0, 0] - rotation[1, 1] - rotation[2, 2]) * 2.0
            qw = (rotation[2, 1] - rotation[1, 2]) / s
            qx = 0.25 * s
            qy = (rotation[0, 1] + rotation[1, 0]) / s
            qz = (rotation[0, 2] + rotation[2, 0]) / s
        elif index == 1:
            s = math.sqrt(1.0 + rotation[1, 1] - rotation[0, 0] - rotation[2, 2]) * 2.0
            qw = (rotation[0, 2] - rotation[2, 0]) / s
            qx = (rotation[0, 1] + rotation[1, 0]) / s
            qy = 0.25 * s
            qz = (rotation[1, 2] + rotation[2, 1]) / s
        else:
            s = math.sqrt(1.0 + rotation[2, 2] - rotation[0, 0] - rotation[1, 1]) * 2.0
            qw = (rotation[1, 0] - rotation[0, 1]) / s
            qx = (rotation[0, 2] + rotation[2, 0]) / s
            qy = (rotation[1, 2] + rotation[2, 1]) / s
            qz = 0.25 * s
    quaternion = np.asarray((qx, qy, qz, qw), dtype=np.float64)
    quaternion /= max(float(np.linalg.norm(quaternion)), 1e-12)
    return [float(value) for value in quaternion]


def transform_point(matrix: np.ndarray, point: np.ndarray) -> np.ndarray:
    homogeneous = np.concatenate((np.asarray(point, dtype=np.float64), np.ones(1)))
    return (matrix @ homogeneous)[:3]


def flatten_matrix(matrix: np.ndarray) -> list[float]:
    return [float(value) for value in np.asarray(matrix, dtype=np.float64).reshape(-1)]

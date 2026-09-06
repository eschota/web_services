"""Scale-independent dorsal surface measurements for authored quadrupeds.

The authoring coordinate convention is Y along the body and Z upwards.
Angles between adjacent surface-centroid segments are invariant to global
translation and rotation. They are regional surface measures, not vertebral
joint angles or an anatomical quality verdict.
"""
import numpy as np


def dorsal_landmarks(rest_points, ordered_bone_tails):
    points = np.asarray(rest_points, dtype=float)
    tails = np.asarray(ordered_bone_tails, dtype=float)
    if (points.ndim != 2 or points.shape[1] != 3 or
            tails.ndim != 2 or tails.shape[1] != 3 or len(tails) < 3 or
            not np.isfinite(points).all() or not np.isfinite(tails).all()):
        raise ValueError('Finite 3D surface and at least three ordered spine tails required')
    longitudinal_steps = np.diff(tails[:, 1])
    if not (np.all(longitudinal_steps > 0) or np.all(longitudinal_steps < 0)):
        raise ValueError('Spine surface landmarks need strictly ordered longitudinal stations')
    half_width = float(np.ptp(tails[:, 1])) / 12
    landmarks = []
    for y in tails[:, 1]:
        band = np.flatnonzero(np.abs(points[:, 1] - y) <= half_width)
        if len(band) < 10:
            raise ValueError('Insufficient surface vertices at a declared spine station')
        upper = band[points[band, 2] >= np.quantile(points[band, 2], .90)]
        landmarks.append(upper)
    # Reject coincident/degenerate centroids before evaluating any animation.
    dorsal_angles_degrees(points, landmarks)
    return landmarks


def dorsal_angles_degrees(points, landmarks):
    points = np.asarray(points, dtype=float)
    centres = np.asarray([points[ids].mean(axis=0) for ids in landmarks])
    segments = np.diff(centres, axis=0)
    lengths = np.linalg.norm(segments, axis=1)
    if len(lengths) < 2 or not np.isfinite(lengths).all() or np.any(lengths <= 0):
        raise ValueError('Degenerate dorsal surface segments')
    unit = segments / lengths[:, None]
    cosines = np.sum(unit[:-1] * unit[1:], axis=1)
    return np.degrees(np.arccos(np.clip(cosines, -1, 1)))


def compare_dorsal_curves(full, candidate, tolerance_degrees=.5):
    full = np.asarray(full, dtype=float)
    candidate = np.asarray(candidate, dtype=float)
    if (full.ndim != 2 or min(full.shape) < 1 or full.shape != candidate.shape or
            not np.isfinite(full).all() or not np.isfinite(candidate).all() or
            not np.isfinite(tolerance_degrees) or tolerance_degrees <= 0):
        raise ValueError('Matching finite sampled dorsal curves and positive tolerance required')
    error = np.abs(full - candidate)
    return {
        'metric': 'angles between dorsal surface-centroid segments; rigid-motion invariant',
        'sample_count': len(full),
        'full_span_degrees': np.ptp(full, axis=0).tolist(),
        'candidate_span_degrees': np.ptp(candidate, axis=0).tolist(),
        'max_angle_error_degrees': float(error.max()),
        'rms_angle_error_degrees': float(np.sqrt(np.mean(error ** 2))),
        'tolerance_degrees': float(tolerance_degrees),
        'tolerance_scope': 'compression fidelity, not anatomical or motion-quality approval',
        'passed': bool(error.max() <= tolerance_degrees),
    }

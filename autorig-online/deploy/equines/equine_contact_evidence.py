#!/usr/bin/env python3
"""Measured per-hoof contact evidence from source-plane constraints.

Every hoof/frame is classified independently.  No gait order, opposite-leg
phase, hidden-point interpolation, mirroring, or ground-confidence claim is
introduced here.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping

import numpy as np


SCHEMA = "autorig.equine-measured-contact-evidence.v1"
STATES = ("OBSERVED_CONTACT", "OBSERVED_SWING", "PENDING")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class ContactEvidenceError(ValueError):
    pass


def options_get(options: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    specs = {
        "min_constraint_confidence": (0.50, 0.01, 1.0, "Minimum original tracker confidence for observed evidence."),
        "lower_envelope_quantile": (0.15, 0.0, 0.40, "Per-hoof measured height quantile used as its lower envelope."),
        "contact_height_band_ratio": (0.025, 0.001, 0.15, "Maximum height above measured lower envelope / source height for contact."),
        "contact_max_speed_ratio": (0.10, 0.001, 2.0, "Maximum measured 3D speed / source height per second for contact."),
        "swing_min_height_ratio": (0.055, 0.005, 0.50, "Minimum height above measured lower envelope / source height for swing."),
        "swing_min_speed_ratio": (0.18, 0.005, 4.0, "Minimum measured 3D speed / source height per second for low swing evidence."),
        "max_plane_residual_ratio": (0.02, 0.0, 0.10, "Maximum source-plane intersection residual accepted as measured."),
        "min_observed_frames_for_envelope": (4, 2, 10000, "Minimum visible confident frames needed to estimate a hoof envelope."),
    }
    supplied = dict(options or {})
    unknown = sorted(set(supplied) - set(specs))
    if unknown: raise ContactEvidenceError("unknown options: " + ", ".join(unknown))
    result = {}
    for name, (default, low, high, doc) in specs.items():
        value = supplied.get(name, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ContactEvidenceError(f"{name} must be finite")
        if name == "min_observed_frames_for_envelope":
            if not isinstance(value, int):
                raise ContactEvidenceError("min_observed_frames_for_envelope must be an integer")
            value = int(value)
        else:
            value = float(value)
        if not low <= value <= high: raise ContactEvidenceError(f"{name} must be in [{low}, {high}]")
        result[name] = {"value": value, "default": default, "min": low, "max": high, "doc": doc}
    if result["swing_min_height_ratio"]["value"] <= result["contact_height_band_ratio"]["value"]:
        raise ContactEvidenceError("swing_min_height_ratio must exceed contact_height_band_ratio")
    return result


def _values(options): return {key: row["value"] for key, row in options_get(options).items()}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""): digest.update(block)
    return digest.hexdigest()


def _unit(value: Any, field: str) -> np.ndarray:
    vector = np.asarray(value, dtype=np.float64)
    if vector.shape != (3,) or not np.all(np.isfinite(vector)):
        raise ContactEvidenceError(f"{field} must be a finite 3-vector")
    length = float(np.linalg.norm(vector))
    if length < 1e-9: raise ContactEvidenceError(f"{field} is degenerate")
    return vector / length


def build_contact_evidence(*, constraints_path: str | Path, constraints_sha256: str,
                           options: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Classify direct source measurements; ambiguous evidence stays PENDING."""
    opt = _values(options)
    path = Path(constraints_path).resolve()
    expected = str(constraints_sha256 or "").lower()
    if not SHA_RE.fullmatch(expected) or not path.is_file() or _sha(path) != expected:
        raise ContactEvidenceError("constraints SHA-256 mismatch or file missing")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContactEvidenceError(f"invalid constraints JSON: {exc}") from exc
    if (not isinstance(document, Mapping) or document.get("schema") != "autorig.equine-sagittal-limb-constraints.v1" or
            document.get("state") != "fit_candidate_constraints" or
            document.get("accepted_observations") is not False or document.get("animation_qa_pass") is not False):
        raise ContactEvidenceError("input is not a non-accepted source-plane constraint document")
    timing = document.get("timing") if isinstance(document.get("timing"), Mapping) else {}
    frame_count, fps = timing.get("frame_count"), timing.get("fps")
    source_indices = timing.get("source_frame_indices")
    provenance = document.get("provenance") if isinstance(document.get("provenance"), Mapping) else {}
    source_video_sha = provenance.get("source_video_sha256")
    if (isinstance(frame_count, bool) or not isinstance(frame_count, int) or frame_count < 2 or
            isinstance(fps, bool) or not isinstance(fps, (int, float)) or not math.isfinite(float(fps)) or fps <= 0 or
            not isinstance(source_indices, list) or len(source_indices) != frame_count or
            any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in source_indices) or
            any(b <= a for a, b in zip(source_indices, source_indices[1:])) or
            timing.get("source_video_sha256") != source_video_sha or
            not isinstance(source_video_sha, str) or SHA_RE.fullmatch(source_video_sha) is None or
            timing.get("retimed") is not False):
        raise ContactEvidenceError("constraint timing is invalid")
    if timing.get("full_source") is True and source_indices != list(range(frame_count)):
        raise ContactEvidenceError("full-source timing must use identity frame mapping")
    source_height = document.get("source_height")
    if isinstance(source_height, bool) or not isinstance(source_height, (int, float)) or not math.isfinite(float(source_height)) or source_height <= 0:
        raise ContactEvidenceError("positive source_height is required")
    source_height = float(source_height)
    axes = document.get("axes") if isinstance(document.get("axes"), Mapping) else {}
    up = _unit(axes.get("up"), "axes.up")
    hoofs = document.get("constraints")
    if not isinstance(hoofs, Mapping) or set(hoofs) != {"LF", "RF", "LH", "RH"}:
        raise ContactEvidenceError("exact four source hoof constraint tracks are required")
    output, pending = {}, []
    for leg in ("LF", "RF", "LH", "RH"):
        track = hoofs[leg]
        if (not isinstance(track, Mapping) or track.get("derivation") != "derived_from_source_plane_constraint" or
                track.get("generated") is not False or track.get("mirrored") is not False):
            raise ContactEvidenceError(f"{leg} is not an original source-plane constraint track")
        rows = track.get("constraints")
        if not isinstance(rows, list): raise ContactEvidenceError(f"{leg} constraints missing")
        by_frame = {}
        for row in rows:
            if not isinstance(row, Mapping): raise ContactEvidenceError(f"{leg} invalid constraint row")
            frame = row.get("frame")
            if isinstance(frame, bool) or not isinstance(frame, int) or not 0 <= frame < frame_count or frame in by_frame:
                raise ContactEvidenceError(f"{leg} frame identity invalid/duplicate")
            position = np.asarray(row.get("position"), dtype=np.float64)
            confidence, residual = row.get("confidence"), row.get("plane_residual_ratio")
            if (position.shape != (3,) or not np.all(np.isfinite(position)) or
                    isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not math.isfinite(float(confidence)) or
                    isinstance(residual, bool) or not isinstance(residual, (int, float)) or not math.isfinite(float(residual))):
                raise ContactEvidenceError(f"{leg} constraint values are invalid")
            if row.get("source_frame") != source_indices[frame]:
                raise ContactEvidenceError(f"{leg} source-frame binding mismatch")
            by_frame[frame] = (position, float(confidence), float(residual))
        usable = {frame: value for frame, value in by_frame.items()
                  if value[1] >= opt["min_constraint_confidence"] and value[2] <= opt["max_plane_residual_ratio"]}
        if len(usable) < opt["min_observed_frames_for_envelope"]:
            envelope = None
            pending.append(f"insufficient_measured_envelope:{leg}")
        else:
            envelope = float(np.quantile([float(np.dot(up, value[0])) for value in usable.values()],
                                         opt["lower_envelope_quantile"]))
        speeds = {}
        for frame, (position, _, _) in usable.items():
            candidates = []
            for other in (frame - 1, frame + 1):
                if other in usable and source_indices[other] - source_indices[frame] in {-1, 1}:
                    dt = abs(source_indices[other] - source_indices[frame]) / float(fps)
                    candidates.append(float(np.linalg.norm(usable[other][0] - position)) / dt / source_height)
            if candidates: speeds[frame] = min(candidates)
        frames = []
        for frame in range(frame_count):
            base = {"frame": frame, "source_frame": source_indices[frame], "state": "PENDING"}
            if frame in by_frame:
                base.update({"confidence": by_frame[frame][1],
                             "plane_residual_ratio": by_frame[frame][2]})
            if frame not in usable:
                base["reason"] = "hidden_weak_or_invalid_source_constraint"
            elif envelope is None:
                base["reason"] = "insufficient_measured_envelope"
            elif frame not in speeds:
                base["reason"] = "no_adjacent_observed_speed"
            else:
                position, confidence, residual = usable[frame]
                height_ratio = (float(np.dot(up, position)) - envelope) / source_height
                speed_ratio = speeds[frame]
                base.update({"height_above_lower_envelope_ratio": height_ratio,
                             "speed_ratio_per_second": speed_ratio})
                if height_ratio <= opt["contact_height_band_ratio"] and speed_ratio <= opt["contact_max_speed_ratio"]:
                    base["state"] = "OBSERVED_CONTACT"
                elif (height_ratio >= opt["swing_min_height_ratio"] or
                      speed_ratio >= opt["swing_min_speed_ratio"]):
                    base["state"] = "OBSERVED_SWING"
                else:
                    base["reason"] = "measured_but_ambiguous"
            if base["state"] == "PENDING": pending.append(f"pending_contact:{leg}:{frame}")
            frames.append(base)
        output[leg] = {"source_anchor_id": track.get("source_anchor_id"),
                       "lower_envelope_height": envelope, "frames": frames}
    return {
        "schema": SCHEMA, "state": "measured_source_contact_evidence",
        "accepted_observations": False, "animation_qa_pass": False,
        "hooves": output, "pending": sorted(set(pending)), "options": opt,
        "timing": dict(timing),
        "source_ground_candidate": {"video_observed_confidence": False,
                                    "used_for_classification": False,
                                    "note": "lower envelope is per-hoof measured evidence, not a video ground plane"},
        "provenance": {"constraints_sha256": expected,
                       "source_video_sha256": provenance.get("source_video_sha256"),
                       "observations_sha256": provenance.get("observations_sha256"),
                       "raw_npz_sha256": provenance.get("raw_npz_sha256"),
                       "method": "per_hoof_lower_envelope_and_adjacent_source_plane_constraint_speed",
                       "opposite_leg_inference": False, "mirroring": False,
                       "hidden_point_interpolation": False, "gait_phase_prior": False},
    }


__all__ = ["ContactEvidenceError", "SCHEMA", "STATES", "build_contact_evidence", "options_get"]

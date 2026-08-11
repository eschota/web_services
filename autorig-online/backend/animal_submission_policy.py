"""Fail-closed policy for public animal-rig submissions."""
from __future__ import annotations

from typing import Any


def animal_detection_accepted(
    detection: Any,
    *,
    default_threshold: float = 0.62,
) -> bool:
    """Return the AI verdict without treating a user's preset choice as approval."""
    if not isinstance(detection, dict):
        return False
    if detection.get("experimental_admin_override_bool") is True:
        return True
    if detection.get("riggable_bool") is False:
        return False
    if str(detection.get("animal_type") or detection.get("animal_type_string") or "").strip().lower() == "unsupported":
        return False
    if str(detection.get("status_string") or "").strip().lower() == "unsupported":
        return False
    if detection.get("animal_decision_accepted_bool") is True:
        return True
    if "animal_decision_accepted_bool" in detection:
        return False
    if detection.get("accepted") is True and not (
        detection.get("manual_selection") is True or detection.get("user_selected_bool") is True
    ):
        return True
    try:
        weight = float(detection.get("animal_decision_weight_float") or 0.0)
    except Exception:
        weight = 0.0
    try:
        threshold = float(detection.get("animal_decision_threshold_float") or default_threshold)
    except Exception:
        threshold = default_threshold
    return weight >= threshold


def animal_rejection_code(detection: Any) -> str:
    if not isinstance(detection, dict):
        return "animal_detection_missing"
    explicit = str(
        detection.get("rejection_code")
        or detection.get("animal_decision_rejected_reason_string")
        or ""
    ).strip()
    if explicit:
        return explicit[:128]
    if detection.get("riggable_bool") is False:
        return "not_riggable"
    return "animal_detection_rejected"


def detected_animal_type(detection: Any) -> str:
    if not isinstance(detection, dict):
        return ""
    for key in (
        "animal_type",
        "animal_type_string",
        "candidate_animal_type_string",
        "selected_type_string",
    ):
        value = str(detection.get(key) or "").strip().lower()
        if value and value not in {"humanoid", "unsupported"}:
            return value
    return ""

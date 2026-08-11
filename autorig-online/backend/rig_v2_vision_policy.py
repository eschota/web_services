"""Pure parsing helpers for Rig V2 vision responses."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List


def extract_vision_assessment(text: str, allowed: List[str]) -> Dict[str, Any]:
    raw = (text or "").strip()
    candidates = [raw]
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        candidates.insert(0, match.group(0))

    for candidate in candidates:
        try:
            data = json.loads(candidate)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        animal = str(data.get("animal_type") or data.get("animal_type_string") or "").strip().lower()
        riggable = data.get("riggable_bool")
        topology = str(data.get("body_topology") or "unknown").strip().lower()[:64] or "unknown"
        rejection_code = str(data.get("rejection_code") or "").strip().lower()[:128]
        if animal == "unsupported" or riggable is False:
            return {
                "success_bool": True,
                "status_string": "unsupported",
                "animal_type_string": "unsupported",
                "confidence_float": _confidence(data),
                "riggable_bool": False,
                "body_topology": topology,
                "rejection_code": rejection_code or "unsupported_body_topology",
            }
        if animal in allowed:
            return {
                "success_bool": True,
                "status_string": "ok",
                "animal_type_string": animal,
                "confidence_float": _confidence(data),
                "riggable_bool": True if riggable is None else bool(riggable),
                "body_topology": topology,
                "rejection_code": rejection_code,
            }

    lowered = raw.lower()
    if re.search(r"\bunsupported\b", lowered):
        return {
            "success_bool": True,
            "status_string": "unsupported",
            "animal_type_string": "unsupported",
            "confidence_float": 1.0,
            "riggable_bool": False,
            "body_topology": "unknown",
            "rejection_code": "unsupported_body_topology",
        }
    for animal in allowed:
        if re.search(rf"\b{re.escape(animal)}\b", lowered):
            return {
                "success_bool": True,
                "status_string": "ok",
                "animal_type_string": animal,
                "confidence_float": 1.0,
                "riggable_bool": True,
                "body_topology": "unknown",
                "rejection_code": "",
            }
    return {
        "success_bool": False,
        "status_string": "vision_failed",
        "animal_type_string": "",
        "confidence_float": 0.0,
        "riggable_bool": False,
        "body_topology": "unknown",
        "rejection_code": "invalid_vision_response",
    }


def _confidence(data: Dict[str, Any]) -> float:
    value = data.get("confidence_float")
    if value is None:
        value = data.get("confidence")
    if value is None:
        value = data.get("weight_float")
    if value is None:
        value = 1.0
    try:
        confidence = float(value)
    except Exception:
        confidence = 1.0
    return max(0.0, min(1.0, confidence))

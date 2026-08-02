"""Validation for persisted 3D viewer theme lighting values."""

from __future__ import annotations

import math
from typing import Any, Dict


VIEWER_THEME_LIGHTING_LIMITS = {
    "environment_intensity": (0.0, 2.0),
    "reflection_intensity": (0.0, 4.0),
    "effective_environment_intensity": (0.0, 4.0),
    "sun_intensity": (0.0, 3.5),
}


def _finite_number(value: Any, label: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{label} must be a finite number")
    return number


def _bounded(value: Any, label: str, limits: tuple[float, float]) -> float:
    number = _finite_number(value, label)
    minimum, maximum = limits
    if number < minimum or number > maximum:
        raise ValueError(f"{label} must be between {minimum:g} and {maximum:g}")
    return number


def validate_viewer_theme_lighting(theme: Dict[str, Any]) -> Dict[str, float]:
    """Fail closed when a theme can overdrive physically based materials."""

    if not isinstance(theme, dict):
        raise ValueError("viewer theme must be an object")
    environment = theme.get("environment_settings")
    sun = theme.get("sun_settings")
    if not isinstance(environment, dict):
        raise ValueError("environment_settings must be an object")
    if not isinstance(sun, dict):
        raise ValueError("sun_settings must be an object")

    env_intensity = _bounded(
        environment.get("intensity"),
        "environment_settings.intensity",
        VIEWER_THEME_LIGHTING_LIMITS["environment_intensity"],
    )
    reflection_intensity = _bounded(
        environment.get("reflection_intensity"),
        "environment_settings.reflection_intensity",
        VIEWER_THEME_LIGHTING_LIMITS["reflection_intensity"],
    )
    effective = env_intensity * reflection_intensity
    _bounded(
        effective,
        "effective environment intensity",
        VIEWER_THEME_LIGHTING_LIMITS["effective_environment_intensity"],
    )
    sun_intensity = _bounded(
        sun.get("intensity"),
        "sun_settings.intensity",
        VIEWER_THEME_LIGHTING_LIMITS["sun_intensity"],
    )
    return {
        "environment_intensity": env_intensity,
        "reflection_intensity": reflection_intensity,
        "effective_environment_intensity": effective,
        "sun_intensity": sun_intensity,
    }

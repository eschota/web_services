"""
Vision helper for 3D viewer backdrop themes.

It is intentionally best-effort: if the vision provider is missing or fails, the
server falls back to deterministic filename-based theme metadata.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Dict, Optional

import httpx


VIEWER_THEME_VISION_PROMPT = (
    "Analyze this 16:9 3D viewer backdrop image and return only valid JSON. "
    "Create metadata for placing a rigged 3D model into this scene. Required shape: "
    "{\"theme_id\":\"short_snake_case_slug\",\"theme_name\":\"Human Name\","
    "\"theme_short_description\":\"one sentence\","
    "\"semantic_tags\":[\"tag\"],"
    "\"plane_color\":\"#rrggbb\","
    "\"shadow_settings\":{\"opacity\":0.5,\"softness\":6.0,\"sun_multiplier\":2.0,\"shadow_y_offset\":0.005},"
    "\"environment_settings\":{\"mode\":\"image\",\"source\":\"backdrop\",\"intensity\":1.0,\"reflection_intensity\":3.0},"
    "\"sun_settings\":{\"rotation\":-75,\"inclination\":45,\"intensity\":2.2},"
    "\"camera_transform\":{\"position\":{\"x\":2.5,\"y\":1.8,\"z\":3.0},\"target\":{\"x\":0,\"y\":0.8,\"z\":0},\"fov\":45}}. "
    "Theme id must be lowercase snake_case and semantic, for example alien_planet or dog_park_yard. "
    "Tags must include useful 3D-model matching concepts such as dog, pet, robot, astronaut, fantasy, forest, farm, studio, ancient, jungle, vehicle, mech. "
    "Choose plane_color from the visible ground color. Tune shadows and sun to match the scene mood."
)


def _image_to_data_url(image_path: Path) -> str:
    suffix = image_path.suffix.lower()
    mime = "image/png" if suffix == ".png" else "image/webp" if suffix == ".webp" else "image/jpeg"
    data = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{data}"


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    candidates = [raw]
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        candidates.insert(0, raw[start:end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _coerce_float(value: Any, fallback: float, low: float, high: float) -> float:
    try:
        n = float(value)
    except Exception:
        n = fallback
    return max(low, min(high, n))


def _coerce_hex(value: Any, fallback: str) -> str:
    s = str(value or "").strip()
    if len(s) == 7 and s.startswith("#"):
        try:
            int(s[1:], 16)
            return s.lower()
        except Exception:
            pass
    return fallback


def coerce_viewer_theme_metadata(raw: Dict[str, Any], fallback_id: str) -> Dict[str, Any]:
    shadow = raw.get("shadow_settings") if isinstance(raw.get("shadow_settings"), dict) else {}
    env = raw.get("environment_settings") if isinstance(raw.get("environment_settings"), dict) else {}
    sun = raw.get("sun_settings") if isinstance(raw.get("sun_settings"), dict) else {}
    cam = raw.get("camera_transform") if isinstance(raw.get("camera_transform"), dict) else {}
    pos = cam.get("position") if isinstance(cam.get("position"), dict) else {}
    target = cam.get("target") if isinstance(cam.get("target"), dict) else {}
    tags = raw.get("semantic_tags") if isinstance(raw.get("semantic_tags"), list) else []
    tags = [str(x).strip().lower() for x in tags if str(x).strip()][:16]
    return {
        "theme_id": str(raw.get("theme_id") or fallback_id).strip().lower().replace("-", "_"),
        "theme_name": str(raw.get("theme_name") or fallback_id.replace("_", " ").title()).strip(),
        "theme_short_description": str(raw.get("theme_short_description") or "").strip()[:300],
        "semantic_tags": tags or [fallback_id],
        "plane_color": _coerce_hex(raw.get("plane_color"), "#6d7d8c"),
        "shadow_settings": {
            "opacity": _coerce_float(shadow.get("opacity"), 0.5, 0.08, 0.95),
            "softness": _coerce_float(shadow.get("softness"), 6.0, 0.0, 10.0),
            "sun_multiplier": _coerce_float(shadow.get("sun_multiplier"), 2.0, 0.35, 3.5),
            "shadow_y_offset": _coerce_float(shadow.get("shadow_y_offset"), 0.005, 0.0001, 0.08),
        },
        "environment_settings": {
            "mode": "image",
            "source": "backdrop",
            "intensity": _coerce_float(env.get("intensity"), 1.0, 0.0, 10.0),
            "reflection_intensity": _coerce_float(env.get("reflection_intensity"), 3.0, 0.0, 10.0),
        },
        "sun_settings": {
            "rotation": _coerce_float(sun.get("rotation"), -75, -180, 180),
            "inclination": _coerce_float(sun.get("inclination"), 45, 5, 85),
            "intensity": _coerce_float(sun.get("intensity"), 2.2, 0.2, 5.0),
        },
        "camera_transform": {
            "position": {
                "x": _coerce_float(pos.get("x"), 2.5, -20, 20),
                "y": _coerce_float(pos.get("y"), 1.8, -20, 20),
                "z": _coerce_float(pos.get("z"), 3.0, -20, 20),
            },
            "target": {
                "x": _coerce_float(target.get("x"), 0.0, -20, 20),
                "y": _coerce_float(target.get("y"), 0.8, -20, 20),
                "z": _coerce_float(target.get("z"), 0.0, -20, 20),
            },
            "fov": _coerce_float(cam.get("fov"), 45, 20, 75),
        },
    }


def analyze_backdrop_theme_with_openai(*, image_path: Path, cfg: Dict[str, Any], fallback_id: str) -> Optional[Dict[str, Any]]:
    api_key = str(cfg.get("open_AI_api_key") or cfg.get("open_ai_api_key") or "").strip()
    api_url = str(cfg.get("open_ai_api_url_string") or "https://api.openai.com/v1/chat/completions").strip()
    if not api_key or not api_url:
        return None
    model = str(cfg.get("open_ai_viewer_theme_vision_model_string") or cfg.get("open_ai_vision_model_string") or "gpt-4o-mini").strip()
    payload: Dict[str, Any] = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": VIEWER_THEME_VISION_PROMPT},
                    {"type": "image_url", "image_url": {"url": _image_to_data_url(image_path), "detail": "high"}},
                ],
            }
        ],
    }
    if model.startswith(("gpt-5", "o3", "o4")):
        payload["max_completion_tokens"] = 1400
    else:
        payload["temperature"] = 0.1
        payload["max_tokens"] = 1400
    with httpx.Client(timeout=90.0, follow_redirects=True) as client:
        resp = client.post(api_url, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}, json=payload)
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI theme vision HTTP {resp.status_code}: {resp.text[:240]}")
    data = resp.json()
    content = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
    if isinstance(content, list):
        content = " ".join(str(x.get("text") or x) if isinstance(x, dict) else str(x) for x in content)
    parsed = _extract_json_object(str(content))
    if not parsed:
        raise RuntimeError("OpenAI theme vision returned non-JSON content")
    out = coerce_viewer_theme_metadata(parsed, fallback_id)
    out["vision_provider_string"] = f"openai:{model}"
    return out

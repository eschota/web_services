"""
Vision → structured LTX prompts for motion-reference videos used by animation fitting.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

IDLE_LTX_VISION_PROMPT_JSON = Path(__file__).resolve().parent / "idle_ltx_vision_prompt.json"

IDLE_LTX_FRAME_COUNT_DEFAULT = 41
IDLE_LTX_VARIANT_COUNT = 4

IDLE_LTX_DEFAULT_NEGATIVE_PROMPT = (
    "camera movement, moving camera, orbit camera, rotating camera, camera pan, camera tilt, "
    "camera zoom, dolly shot, tracking shot, handheld camera, camera shake, viewpoint change, "
    "reframing, dynamic camera, cinematic camera move, crane shot, jib shot, push in, pull out, "
    "push-in, pull-back, dolly in, dolly out, moving closer, moving away, camera forward movement, "
    "camera backward movement, changing camera distance, parallax shift, background drift, lens zoom, "
    "rack focus, subject rotation, body turning, scene change, "
    "background movement, morphing, melting, extra legs, extra head, distorted anatomy, broken face, "
    "random letters, alphabet wall, unreadable text, fake signage, posters, text, watermark, logo"
)

IDLE_LTX_USER_PROMPT_DEFAULT = (
    "Create four locked-camera LTX motion-reference prompts for idle, walk, run, and die variants. "
    "These videos will be targets for inverse skeletal animation fitting, so motion must be clean, centered, and bone-fit friendly. "
    "Every prompt must start with a single locked-off tripod shot, static frame, fixed viewpoint. "
    "The camera is bolted down and never moves, the camera-subject distance never changes, and the background stays pixel-locked."
)

IDLE_LTX_STATIC_CAMERA_SENTENCE = (
    "Single locked-off tripod shot. Static frame. Fixed viewpoint. The camera is bolted down and never moves. "
    "No push-in, no pull-back, no dolly in, no dolly out, no zoom, no pan, no tilt, no orbit, no tracking, "
    "no handheld shake, no reframing. The distance between camera and subject never changes. "
    "The entire frame, including the selected theme backdrop, stays pixel-locked with zero parallax."
)
IDLE_LTX_FIRST_FRAME_SENTENCE = (
    "Use the provided first frame as the visual source and preserve its subject, environment, props, lighting, "
    "materials, body pose, silhouette, framing, and background layout."
)
IDLE_LTX_NO_TEXT_SENTENCE = (
    "Do not add random letters, alphabet walls, unreadable text, fake signage, posters, watermarks, logos, "
    "or any typography that is not already visible in the provided first frame."
)


def _with_hard_camera_lock(prompt: str) -> str:
    body = _as_str(prompt)
    if not body:
        return f"{IDLE_LTX_FIRST_FRAME_SENTENCE} {IDLE_LTX_NO_TEXT_SENTENCE} {IDLE_LTX_STATIC_CAMERA_SENTENCE}"
    lock = IDLE_LTX_STATIC_CAMERA_SENTENCE
    first = IDLE_LTX_FIRST_FRAME_SENTENCE
    pieces: List[str] = []
    if first.lower() not in body.lower():
        pieces.append(first)
    if IDLE_LTX_NO_TEXT_SENTENCE.lower() not in body.lower():
        pieces.append(IDLE_LTX_NO_TEXT_SENTENCE)
    if lock.lower() not in body.lower():
        pieces.append(lock)
    pieces.append(body)
    return " ".join(pieces)


def load_vision_json_task() -> str:
    """Load vision instruction text from JSON on every call (edit file to tune prompts without code changes)."""
    try:
        raw_text = IDLE_LTX_VISION_PROMPT_JSON.read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise RuntimeError(f"Idle LTX vision prompt file missing: {IDLE_LTX_VISION_PROMPT_JSON}") from e
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in {IDLE_LTX_VISION_PROMPT_JSON}: {e}") from e
    if not isinstance(data, dict):
        raise RuntimeError(f"{IDLE_LTX_VISION_PROMPT_JSON} must contain a JSON object")
    s = str(data.get("vision_json_task_string") or "").strip()
    if not s:
        raise RuntimeError(
            f"{IDLE_LTX_VISION_PROMPT_JSON} must define non-empty string key vision_json_task_string"
        )
    return s


def _strip_json_fences(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9]*\s*", "", t)
        t = re.sub(r"\s*```\s*$", "", t)
    return t.strip()


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    t = _strip_json_fences(text)
    try:
        obj = json.loads(t)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}\s*$", t)
    if m:
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _as_float(x: Any, default: float = 0.5) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _as_str(x: Any) -> str:
    return str(x or "").strip()


def map_compact_vision_to_internal_shape(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map new compact LTX vision JSON (base_prompt_string, variants_array, …) to legacy keys."""
    if _as_str(raw.get("ltx_base_prompt_string")):
        return raw
    base = _as_str(raw.get("base_prompt_string"))
    vars_c = raw.get("variants_array")
    if not base or not isinstance(vars_c, list):
        return raw

    out_variants: List[Dict[str, str]] = []
    for i in range(IDLE_LTX_VARIANT_COUNT):
        if i < len(vars_c) and isinstance(vars_c[i], dict):
            out_variants.append(
                {
                    "variant_name_string": _as_str(vars_c[i].get("variant_name_string")) or f"variant_{i}",
                    "prompt_string": _as_str(vars_c[i].get("prompt_string")),
                }
            )
        else:
            out_variants.append({"variant_name_string": f"variant_{i}", "prompt_string": ""})

    subject = _as_str(raw.get("detected_subject_string"))
    try:
        conf = float(raw.get("confidence_float"))
    except Exception:
        conf = 0.5
    conf = max(0.0, min(1.0, conf))
    scene = _as_str(raw.get("scene_string"))
    appearance = _as_str(raw.get("appearance_string"))
    contact = _as_str(raw.get("contact_points_string"))
    model_desc = ", ".join(x for x in [appearance, scene] if x) or subject

    merged = dict(raw)
    merged["detected_species_string"] = subject
    merged["species_confidence_float"] = conf
    merged["model_description_string"] = model_desc
    merged["initial_pose_string"] = scene
    merged["camera_description_string"] = "static camera"
    merged["visible_body_parts_string_array"] = [contact] if contact else []
    merged["safe_idle_motions_string_array"] = ["idle in place"]
    merged["forbidden_motions_string_array"] = []
    merged["ltx_base_prompt_string"] = base
    merged["ltx_negative_prompt_string"] = IDLE_LTX_DEFAULT_NEGATIVE_PROMPT
    merged["ltx_variants_array"] = out_variants
    return merged


def validate_vision_prompts_from_model(raw: Dict[str, Any]) -> None:
    """Fail if the vision model did not return full prompts (no silent template fill for video)."""
    if not _as_str(raw.get("ltx_base_prompt_string")):
        raise RuntimeError("Vision model returned empty ltx_base_prompt_string")
    if not _as_str(raw.get("ltx_negative_prompt_string")):
        raise RuntimeError("Vision model returned empty ltx_negative_prompt_string")
    vars_ = raw.get("ltx_variants_array")
    if not isinstance(vars_, list) or len(vars_) < IDLE_LTX_VARIANT_COUNT:
        raise RuntimeError(
            f"Vision model must return ltx_variants_array with at least {IDLE_LTX_VARIANT_COUNT} entries"
        )
    for i in range(IDLE_LTX_VARIANT_COUNT):
        item = vars_[i]
        if not isinstance(item, dict) or not _as_str(item.get("prompt_string")):
            raise RuntimeError(f"Vision model returned empty prompt_string for variant index {i}")


def coerce_vision_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure required keys and 4 variants with non-empty prompts when possible."""
    species = _as_str(raw.get("detected_species_string")) or "unknown creature 3D model"
    conf = _as_float(raw.get("species_confidence_float"), 0.5)
    conf = max(0.0, min(1.0, conf))
    model_desc = _as_str(raw.get("model_description_string")) or species
    parts = raw.get("visible_body_parts_string_array")
    if not isinstance(parts, list):
        parts = []
    parts = [str(p).strip() for p in parts if str(p).strip()]
    initial_pose = _as_str(raw.get("initial_pose_string"))
    cam_desc = _as_str(raw.get("camera_description_string")) or "locked static full-body framing"
    safe = raw.get("safe_idle_motions_string_array")
    if not isinstance(safe, list):
        safe = []
    safe = [str(s).strip() for s in safe if str(s).strip()]
    if not safe:
        safe = ["subtle breathing in place"]
    forb = raw.get("forbidden_motions_string_array")
    if not isinstance(forb, list):
        forb = []
    forb = [str(s).strip() for s in forb if str(s).strip()]
    if not forb:
        forb = ["walking", "stepping", "camera movement"]

    neg = _as_str(raw.get("ltx_negative_prompt_string")) or IDLE_LTX_DEFAULT_NEGATIVE_PROMPT
    if "camera movement" not in neg.lower() or "camera zoom" not in neg.lower():
        neg = f"{neg}, {IDLE_LTX_DEFAULT_NEGATIVE_PROMPT}"
    neg = neg[:2000]
    base = _as_str(raw.get("ltx_base_prompt_string"))
    motions_joined = ", ".join(safe[:8])
    if not base:
        base = (
            f"Use the provided first frame as the visual source. A full-body {species} 3D character is visible: {model_desc}. "
            f"Preserve the same environment, props, lighting, shadows, materials, framing, and background layout from the first frame. "
            f"Single locked-off tripod shot. Static frame. Fixed viewpoint. The camera is bolted down and never moves. "
            f"The {species} performs a very subtle idle loop in place: {motions_joined}. "
            "The feet, hooves, paws, or body contact points stay planted and the root stays anchored. "
            "No camera movement, no subject travel, no scene change, no invented text, and no new background objects."
        )
    base = _with_hard_camera_lock(base)
    base = base[:2400]

    variants_in = raw.get("ltx_variants_array")
    default_names = ["idle", "walk", "run", "die"]
    variants: List[Dict[str, str]] = []
    if isinstance(variants_in, list):
        for i in range(IDLE_LTX_VARIANT_COUNT):
            item = variants_in[i] if i < len(variants_in) and isinstance(variants_in[i], dict) else {}
            vn = _as_str(item.get("variant_name_string")) or default_names[i]
            ps = _as_str(item.get("prompt_string"))
            variants.append({"variant_name_string": vn, "prompt_string": ps})

    while len(variants) < IDLE_LTX_VARIANT_COUNT:
        variants.append({"variant_name_string": default_names[len(variants)], "prompt_string": ""})

    suffixes = [
        "Idle variant: subtle breathing and small natural motion in place; no locomotion.",
        "Walk variant: walking-in-place motion while the character stays centered in the frame.",
        "Run variant: running-in-place motion with more energy while the character stays centered.",
        "Die variant: fall or collapse in place into a defeated pose; no scene change.",
    ]
    for i, sf in enumerate(suffixes):
        if not _as_str(variants[i]["prompt_string"]):
            variants[i]["prompt_string"] = f"{base} {sf}"
        variants[i]["variant_name_string"] = default_names[i]
        variants[i]["prompt_string"] = _with_hard_camera_lock(variants[i]["prompt_string"])[:2400]

    return {
        "detected_species_string": species,
        "species_confidence_float": conf,
        "model_description_string": model_desc,
        "visible_body_parts_string_array": parts,
        "initial_pose_string": initial_pose,
        "camera_description_string": cam_desc,
        "safe_idle_motions_string_array": safe,
        "forbidden_motions_string_array": forb,
        "ltx_base_prompt_string": base,
        "ltx_negative_prompt_string": neg,
        "ltx_variants_array": variants[:IDLE_LTX_VARIANT_COUNT],
    }


async def _openai_vision_analyze(
    *,
    cfg: Dict[str, Any],
    app_url: str,
    image_url_string: str,
    user_prompt_string: str,
) -> Tuple[Dict[str, Any], str]:
    api_key = str(cfg.get("open_AI_api_key") or cfg.get("open_ai_api_key") or "").strip()
    api_url = str(cfg.get("open_ai_api_url_string") or "").strip()
    if not api_key or not api_url:
        raise RuntimeError("OpenAI API not configured for Idle LTX vision")

    model = str(cfg.get("open_ai_idle_ltx_vision_model_string") or "gpt-4o-mini").strip()
    vision_task = load_vision_json_task()
    user_block = f"User task (constraints): {user_prompt_string}\n\n{vision_task}"
    payload: Dict[str, Any] = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_block},
                    {"type": "image_url", "image_url": {"url": image_url_string, "detail": "high"}},
                ],
            }
        ],
    }
    if model.startswith(("gpt-5", "o3", "o4")):
        payload["max_completion_tokens"] = 2500
    else:
        payload["temperature"] = 0.2
        payload["max_tokens"] = 2500

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.post(api_url, headers=headers, json=payload)
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI vision HTTP {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    content = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
    if isinstance(content, list):
        content = " ".join(
            str(part.get("text") or part) if isinstance(part, dict) else str(part) for part in content
        )
    parsed = extract_json_object(str(content))
    if not parsed:
        raise RuntimeError("OpenAI vision returned non-JSON content")
    parsed = map_compact_vision_to_internal_shape(parsed)
    validate_vision_prompts_from_model(parsed)
    return coerce_vision_result(parsed), f"openai:{model}"


async def _openrouter_vision_analyze(
    *,
    cfg: Dict[str, Any],
    app_url: str,
    image_url_string: str,
    user_prompt_string: str,
) -> Tuple[Dict[str, Any], str]:
    api_key = str(cfg.get("open_router_api_key") or "").strip()
    api_url = str(cfg.get("open_router_api_url_string") or "").strip()
    if not api_key or not api_url:
        raise RuntimeError("OpenRouter not configured for Idle LTX vision")

    model = str(cfg.get("open_router_idle_ltx_vision_model_string") or "openai/gpt-4o-mini").strip()
    vision_task = load_vision_json_task()
    user_block = f"User task (constraints): {user_prompt_string}\n\n{vision_task}\nReturn ONLY raw JSON, no markdown."
    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 2500,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_block},
                    {"type": "image_url", "image_url": {"url": image_url_string}},
                ],
            }
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": (app_url or "https://autorig.online").rstrip("/"),
        "X-Title": "AutoRig Idle LTX Vision",
    }
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.post(api_url, headers=headers, json=payload)
    if resp.status_code != 200:
        raise RuntimeError(f"OpenRouter vision HTTP {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    content = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
    if isinstance(content, list):
        content = " ".join(
            str(part.get("text") or part) if isinstance(part, dict) else str(part) for part in content
        )
    parsed = extract_json_object(str(content))
    if not parsed:
        raise RuntimeError("OpenRouter vision returned non-JSON content")
    parsed = map_compact_vision_to_internal_shape(parsed)
    validate_vision_prompts_from_model(parsed)
    return coerce_vision_result(parsed), f"openrouter:{model}"


class VisionPromptAnalyzer:
    """Analyzes the Renderfin-hosted reference still and returns coerced LTX prompt JSON."""

    @staticmethod
    async def analyze(
        *,
        cfg: Dict[str, Any],
        app_url: str,
        image_url_string: str,
        user_prompt_string: str,
    ) -> Tuple[Dict[str, Any], str]:
        upt = (user_prompt_string or "").strip() or IDLE_LTX_USER_PROMPT_DEFAULT
        last_err = ""
        # Prefer OpenAI when both available (reliable json_object).
        if str(cfg.get("open_AI_api_key") or cfg.get("open_ai_api_key") or "").strip() and str(
            cfg.get("open_ai_api_url_string") or ""
        ).strip():
            try:
                return await _openai_vision_analyze(
                    cfg=cfg,
                    app_url=app_url,
                    image_url_string=image_url_string,
                    user_prompt_string=upt,
                )
            except Exception as e:
                last_err = str(e)
        if str(cfg.get("open_router_api_key") or "").strip():
            try:
                return await _openrouter_vision_analyze(
                    cfg=cfg,
                    app_url=app_url,
                    image_url_string=image_url_string,
                    user_prompt_string=upt,
                )
            except Exception as e:
                last_err = f"{last_err}; {e}" if last_err else str(e)
        raise RuntimeError(last_err or "No vision API configured (OpenAI or OpenRouter required for Idle LTX)")

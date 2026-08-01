"""Flux render prompt generation + body-type mask selection for the Telegram
"Сгенерировать" button.

Shared by the bot process and the backend (both run from /root/autorig-online/backend).
LLM path mirrors idle_ltx_vision.py: OpenAI first, OpenRouter fallback, instruction
text loaded per-call from render_prompt_instruction.json so it is tunable without
code changes. A deterministic template + keyword heuristic covers LLM outages.
"""
from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx

RENDERFIN_PUBLIC_BASE_URL = os.getenv(
    "RENDERFIN_PUBLIC_BASE_URL", "https://autorig.online/renderfin"
).rstrip("/")
RENDERFIN_INTERNAL_URL = os.getenv(
    "RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8010/renderfin"
).rstrip("/")

_INSTRUCTION_PATH = Path(__file__).resolve().parent / "render_prompt_instruction.json"
PREFLIGHT_RENDER_DIR = Path("/var/autorig/preflight-renders")

BODY_TYPES = ("normal", "long", "fat", "dwarf", "goblin")

_BODY_TYPE_KEYWORDS = (
    ("fat", re.compile(r"\b(fat|chubby|obese|round|plump|heavy|bulky|big[- ]?belly)\b", re.I)),
    ("dwarf", re.compile(r"\b(dwarf|dwarven|short|stocky|gnome|halfling)\b", re.I)),
    ("goblin", re.compile(r"\b(goblin|imp|thin|skinny|slender|lanky[- ]?thin|wiry|gremlin)\b", re.I)),
    ("long", re.compile(r"\b(tall|long|lanky|elongated|slim giant|giraffe|stretch)\b", re.I)),
)

DEFAULT_NEGATIVE_PROMPT = (
    "blurry, low quality, multiple characters, cropped, text, watermark, logo, "
    "weapon in hand, side view, back view, dynamic pose, sitting, crouching, bent arms"
)


@dataclass
class RenderGenPlan:
    prompt: str
    negative_prompt: str
    body_type: str
    mask_url: str
    source: str  # "llm" | "template"


def mask_url_for_body_type(body_type: str) -> str:
    body_type = (body_type or "normal").strip().lower()
    if body_type not in BODY_TYPES:
        body_type = "normal"
    name = "t_pose.jpg" if body_type == "normal" else f"t_pose_{body_type}.jpg"
    return f"{RENDERFIN_PUBLIC_BASE_URL}/render/masks/{name}"


def body_type_from_keywords(text: str) -> str:
    for body_type, pattern in _BODY_TYPE_KEYWORDS:
        if pattern.search(text or ""):
            return body_type
    return "normal"


def _load_instruction() -> str:
    try:
        data = json.loads(_INSTRUCTION_PATH.read_text(encoding="utf-8"))
        return str(data.get("instruction") or "").strip()
    except Exception as exc:
        print(f"[RenderPrompting] instruction load failed: {exc}")
        return ""


def _task_metadata_summary(task: Any) -> Dict[str, str]:
    """Pull the prompt-relevant fields off a Task ORM object (best effort)."""
    meta: Dict[str, str] = {}
    for attr, key in (
        ("poster_llm_title", "title"),
        ("poster_llm_description", "description"),
        ("poster_llm_keywords", "keywords"),
        ("input_type", "input_type"),
    ):
        value = getattr(task, attr, None)
        if value:
            meta[key] = str(value)
    try:
        from tasks import _animal_type_from_detection_meta, _task_notification_theme_meta

        theme = _task_notification_theme_meta(task) or {}
        if theme.get("detector_text"):
            meta["detector"] = str(theme["detector_text"])
        if theme.get("theme_name"):
            meta["theme"] = str(theme["theme_name"])
        settings = getattr(task, "viewer_settings", None) or {}
        detection = settings.get("rig_v2_animal_detection") if isinstance(settings, dict) else None
        animal = _animal_type_from_detection_meta(detection)
        if animal:
            meta["animal_type"] = str(animal)
    except Exception:
        pass
    return meta


def _poster_data_url(task_id: str) -> Optional[str]:
    path = PREFLIGHT_RENDER_DIR / f"{task_id}.jpg"
    try:
        if path.is_file() and path.stat().st_size > 0:
            encoded = base64.b64encode(path.read_bytes()).decode("ascii")
            return f"data:image/jpeg;base64,{encoded}"
    except Exception as exc:
        print(f"[RenderPrompting] poster read failed: {exc}")
    return None


def build_template_plan(meta: Dict[str, str]) -> RenderGenPlan:
    """Deterministic fallback when no LLM is available."""
    title = meta.get("title") or meta.get("animal_type") or meta.get("detector") or "stylized 3d character"
    description = (meta.get("description") or "").strip()
    keywords = (meta.get("keywords") or "").strip()
    parts = [f"full body character concept of {title.strip()}"]
    if description:
        parts.append(description[:400])
    if keywords:
        parts.append(keywords[:200])
    parts.append(
        "full body, T-pose, arms stretched horizontally, front view, character sheet, "
        "neutral studio background, even lighting, stylized 3D game character, "
        "high detail, PBR materials"
    )
    body_type = body_type_from_keywords(" ".join([title, description, keywords]))
    return RenderGenPlan(
        prompt=", ".join(parts)[:1800],
        negative_prompt=DEFAULT_NEGATIVE_PROMPT,
        body_type=body_type,
        mask_url=mask_url_for_body_type(body_type),
        source="template",
    )


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    text = (text or "").strip()
    if not text:
        return None
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _plan_from_llm_json(parsed: Dict[str, Any]) -> Optional[RenderGenPlan]:
    prompt = str(parsed.get("flux_prompt") or "").strip()
    if len(prompt) < 20:
        return None
    negative = str(parsed.get("negative_prompt") or "").strip() or DEFAULT_NEGATIVE_PROMPT
    body_type = str(parsed.get("body_type") or "normal").strip().lower()
    if body_type not in BODY_TYPES:
        body_type = "normal"
    return RenderGenPlan(
        prompt=prompt[:1800],
        negative_prompt=negative[:800],
        body_type=body_type,
        mask_url=mask_url_for_body_type(body_type),
        source="llm",
    )


async def _llm_generate(meta: Dict[str, str], poster_data_url: Optional[str]) -> Optional[RenderGenPlan]:
    instruction = _load_instruction()
    if not instruction:
        return None
    meta_text = "\n".join(f"{key}: {value}" for key, value in meta.items()) or "(no metadata)"
    user_text = f"{instruction}\n\nModel metadata:\n{meta_text}"

    content: Any = [{"type": "text", "text": user_text}]
    if poster_data_url:
        content.append({"type": "image_url", "image_url": {"url": poster_data_url, "detail": "low"}})

    attempts = []
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    if openai_key:
        attempts.append(
            (
                os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions").strip(),
                openai_key,
                os.getenv("OPENAI_RENDER_PROMPT_MODEL", "gpt-4o-mini").strip(),
                {},
            )
        )
    openrouter_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if openrouter_key:
        attempts.append(
            (
                os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions").strip(),
                openrouter_key,
                os.getenv("OPENROUTER_RENDER_PROMPT_MODEL", "openai/gpt-4o-mini").strip(),
                {"HTTP-Referer": "https://autorig.online", "X-Title": "AutoRig Render Prompt"},
            )
        )

    for api_url, api_key, model, extra_headers in attempts:
        payload = {
            "model": model,
            "temperature": 0.4,
            "max_tokens": 900,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "user", "content": content}],
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        headers.update(extra_headers)
        try:
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                resp = await client.post(api_url, headers=headers, json=payload)
            if resp.status_code != 200:
                print(f"[RenderPrompting] LLM HTTP {resp.status_code}: {resp.text[:200]}")
                continue
            data = resp.json()
            raw = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
            if isinstance(raw, list):
                raw = " ".join(
                    str(p.get("text") or p) if isinstance(p, dict) else str(p) for p in raw
                )
            parsed = _extract_json_object(str(raw))
            if not parsed:
                print("[RenderPrompting] LLM returned non-JSON content")
                continue
            plan = _plan_from_llm_json(parsed)
            if plan:
                return plan
        except Exception as exc:
            print(f"[RenderPrompting] LLM call failed ({model}): {exc}")
    return None


async def build_render_request(task_id: str) -> RenderGenPlan:
    """Load task metadata and produce the Flux prompt + body-type mask plan."""
    task = None
    try:
        from database import AsyncSessionLocal, Task
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
    except Exception as exc:
        print(f"[RenderPrompting] task load failed for {task_id}: {exc}")

    meta = _task_metadata_summary(task) if task is not None else {}
    poster = _poster_data_url(task_id)

    plan = await _llm_generate(meta, poster)
    if plan is not None:
        return plan
    return build_template_plan(meta)


async def start_character_gen(
    plan: RenderGenPlan,
    *,
    source_task_id: str = "",
    user_name: str = "autorig-bot",
    telegram_chat_id: int = 0,
) -> str:
    """POST the plan to the renderfin character-gen pipeline. Returns job_id."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{RENDERFIN_INTERNAL_URL}/api-character-gen",
            json={
                "prompt": plan.prompt,
                "negative_prompt": plan.negative_prompt,
                "mask_url": plan.mask_url,
                "user_name": user_name,
                "source_task_id": source_task_id,
                "telegram_chat_id": telegram_chat_id,
            },
        )
    if resp.status_code != 200:
        raise RuntimeError(f"character-gen start failed: HTTP {resp.status_code} {resp.text[:300]}")
    job_id = str(resp.json().get("job_id") or "")
    if not job_id:
        raise RuntimeError("character-gen start returned no job_id")
    return job_id


async def poll_character_gen(job_id: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}")
    if resp.status_code != 200:
        raise RuntimeError(f"character-gen status failed: HTTP {resp.status_code}")
    return resp.json()


async def list_active_character_gen_jobs() -> list:
    """Jobs still in flight (used by the bot to re-attach watchers on startup)."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(f"{RENDERFIN_INTERNAL_URL}/api-character-gen")
        if resp.status_code != 200:
            return []
        return list(resp.json().get("jobs") or [])
    except Exception as exc:
        print(f"[RenderPrompting] active job list failed: {exc}")
        return []


async def set_character_gen_telegram_context(
    job_id: str, *, chat_id: int = 0, message_id: int = 0, status_message_id: int = 0
) -> None:
    """Tell renderfin where to deliver: the chat, the review message and the
    interim status message it should clean up once the result is out."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            await client.post(
                f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/telegram-context",
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "status_message_id": status_message_id,
                },
            )
    except Exception as exc:
        print(f"[RenderPrompting] telegram context update failed: {exc}")


async def resume_character_gen(job_id: str) -> Dict[str, Any]:
    """Retry a failed job from its furthest completed stage."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/resume")
    if resp.status_code != 200:
        raise RuntimeError(f"character-gen resume failed: HTTP {resp.status_code}")
    return resp.json()


async def approve_character_gen_image(job_id: str) -> Dict[str, Any]:
    """Approve the Flux render; pipeline continues to the 3D stage."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/approve-image")
    if resp.status_code != 200:
        raise RuntimeError(f"character-gen approve failed: HTTP {resp.status_code}")
    return resp.json()


async def regenerate_character_gen_image(job_id: str) -> Dict[str, Any]:
    """Re-render the Flux image with a fresh seed (same prompt/mask)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/regenerate-image")
    if resp.status_code != 200:
        raise RuntimeError(f"character-gen regenerate failed: HTTP {resp.status_code}")
    return resp.json()


async def discard_character_gen(job_id: str) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/discard")
    if resp.status_code != 200:
        raise RuntimeError(f"character-gen discard failed: HTTP {resp.status_code}")
    return resp.json()


async def mark_character_gen_submitted(job_id: str) -> None:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/mark-submitted")
    except Exception as exc:
        print(f"[RenderPrompting] mark-submitted failed: {exc}")

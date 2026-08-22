"""Flux render prompt generation + body-type mask selection for the Telegram
"Коллекция ×15" button and legacy single-character callers.

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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

RENDERFIN_PUBLIC_BASE_URL = os.getenv(
    "RENDERFIN_PUBLIC_BASE_URL", "https://autorig.online/renderfin"
).rstrip("/")
RENDERFIN_INTERNAL_URL = os.getenv(
    "RENDERFIN_INTERNAL_URL", "http://127.0.0.1:8010/renderfin"
).rstrip("/")

_INSTRUCTION_PATH = Path(__file__).resolve().parent / "render_prompt_instruction.json"
_COLLECTION_INSTRUCTION_PATH = (
    Path(__file__).resolve().parent / "render_collection_prompt_instruction.json"
)
PREFLIGHT_RENDER_DIR = Path("/var/autorig/preflight-renders")
COLLECTION_SIZE = 15

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
    # Second style of the SAME character, so the operator picks a look rather
    # than a different character. Both variants deliberately share one mask:
    # a different pose skeleton would change the build too.
    prompt_b: str = ""


@dataclass
class CollectionMemberPlan:
    index: int
    title: str
    prompt: str
    negative_prompt: str
    body_type: str
    mask_url: str


@dataclass
class RenderCollectionPlan:
    collection_guid: str
    collection_title: str
    collection_description: str
    collection_tags: List[str] = field(default_factory=list)
    members: List[CollectionMemberPlan] = field(default_factory=list)
    source: str = "llm"  # "llm" | "template"


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
    """The single character-description instruction."""
    try:
        data = json.loads(_INSTRUCTION_PATH.read_text(encoding="utf-8"))
        return str(data.get("instruction") or "").strip()
    except Exception as exc:
        print(f"[RenderPrompting] instruction load failed: {exc}")
        return ""


def _load_collection_instruction() -> str:
    """Instruction for one coherent, diverse 15-character roster."""
    try:
        data = json.loads(_COLLECTION_INSTRUCTION_PATH.read_text(encoding="utf-8"))
        return str(data.get("instruction") or "").strip()
    except Exception as exc:
        print(f"[RenderPrompting] collection instruction load failed: {exc}")
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


# Fixed sentences wrapped around the character description. Every clause here
# is a measured fix, not decoration:
#   - no "character sheet"/"turnaround": Flux builds a multi-panel layout with
#     annotation text and the 3D stage fails on it
#   - no "silhouette": Flux renders a literal black cutout
#   - no "even lighting"/"studio"/"white background": the first reads as no form
#     shadow at all, the others pull a floor sweep and a contact shadow that
#     RMBG cuts through, leaving a dirty edge at the ankles
#   - the margin clause: the pose skeletons put fingertips within 31-91px of the
#     frame edge, and a clipped hand reconstructs as a truncated stump
#   - "arms straight out to the sides", never "stretched", which reads as a
#     stretch deformation on the limbs
_BASE_STYLE_PHRASE = "full-body stylized 3D game character render"
_LOWPOLY_STYLE_PHRASE = "full-body low-poly cartoon 3D game character render"

_POSE_SENTENCE = (
    "It stands alone in a strict T-pose, arms straight out to the sides, hands open "
    "and empty, legs straight and slightly apart, seen head-on at eye level, whole "
    "figure inside the frame with clear margin past the fingertips and open backdrop "
    "between the arms and torso and between the legs."
)

_BASE_TAIL = (
    "Broad soft frontal light with balanced fill on both sides and a faint edge light "
    "lifting the figure off a plain flat mid-grey backdrop, shadowless with no ground "
    "plane and no vignette, bilaterally symmetric, sharp deep focus, no perspective "
    "distortion, no text."
)

# The base style gets material contrast; the cartoon style gets the opposite -
# fewer, larger, flatter faces. Naming triangles explicitly is what makes FLUX
# actually drop the polygon count instead of rendering a smooth model in bright
# colours, and coarse geometry is also what Hunyuan3D reconstructs best.
_BASE_MATERIALS = (
    "Every surface is fully opaque with a clearly stated finish, matte woven fabric "
    "against satin worn leather and brushed metal fittings, so the materials read "
    "apart from each other."
)

_LOWPOLY_MATERIALS = (
    "Built from very few large flat triangles, visible triangular facets across every "
    "surface with hard creased edges between them, each facet catching the light as "
    "one flat tone, chunky blocky limbs, mitten hands with a separated thumb, simple "
    "rounded boots, all shapes reduced to their coarsest form with no fine detail, no "
    "fabric weave, no wrinkles and no small parts."
)

_LOWPOLY_TAIL = (
    "Low-poly flat-shaded cartoon game asset, faceted triangular geometry, bold "
    "saturated colour blocking in flat blocks with hard boundaries, high contrast, "
    "smooth uncluttered opaque surfaces. "
) + _BASE_TAIL


def compose_prompt(subject: str, outfit: str, *, lowpoly: bool = False) -> str:
    """Wrap one character description in one style.

    Both styles are built from the SAME subject and outfit on purpose. Writing
    the two prompts independently produced two different characters - a slim
    teenager in a white puffer next to a chunky figure in a red parka - which
    makes the choice meaningless: the operator is supposed to pick a style, not
    a character.
    """
    subject = (subject or "").strip().rstrip(",.") or "a stylized humanoid character"
    outfit = (outfit or "").strip().rstrip(",.")
    who = f"{subject} wearing {outfit}" if outfit else subject
    style = _LOWPOLY_STYLE_PHRASE if lowpoly else _BASE_STYLE_PHRASE
    materials = _LOWPOLY_MATERIALS if lowpoly else _BASE_MATERIALS
    tail = _LOWPOLY_TAIL if lowpoly else _BASE_TAIL
    return " ".join([f"{who}, {style}.", _POSE_SENTENCE, materials, tail])[:1800]

# Marketplace boilerplate describes the file, not the character, and eats the
# text budget that CLIP-L actually reads.
_JUNK_RE = re.compile(
    r"\b(rigged|animated|game[- ]?ready|low[- ]?poly|high[- ]?poly|pbr|textures?|uv|"
    r"unwrapped|fbx|obj|glb|gltf|blend(?:er)?|maya|mixamo|unity|unreal|free|download|"
    r"pack|asset|sale|polycount|topology|quads|tris|4k|8k|hd|v\d+|20\d\d)\b",
    re.I,
)


def _clean_metadata_text(text: str) -> str:
    """Strip store boilerplate, credits and years out of scraped metadata."""
    text = re.sub(r"\bby\s+[A-Za-z0-9_.-]+", " ", text or "", flags=re.I)
    text = _JUNK_RE.sub(" ", text)
    text = re.sub(r"[|_/\\]+", " ", text)
    words = [w for w in re.split(r"[\s,]+", text) if len(w) > 1]
    # duplicated tokens get weighted by T5 and drown the subject
    seen, kept = set(), []
    for word in words:
        low = word.lower()
        if low in seen:
            continue
        seen.add(low)
        kept.append(word)
    return " ".join(kept).strip(" ,.-")


def lowpoly_variant(prompt: str) -> str:
    """Restate an already-composed base prompt in the cartoon style.

    Only used when a prompt arrives without its parts (an old job, a caller
    that supplied free text). The normal path composes both styles from the
    same subject via compose_prompt.
    """
    subject = (prompt or "").split(".")[0].strip().rstrip(",")
    subject = subject.replace(_BASE_STYLE_PHRASE, "").strip().rstrip(",")
    return compose_prompt(subject, "", lowpoly=True)


def build_template_plan(meta: Dict[str, str]) -> RenderGenPlan:
    """Deterministic fallback when no LLM is available.

    Composes the same way the LLM path does, so both variants still describe
    one character even when nobody could write a better description.
    """
    title = meta.get("title") or meta.get("animal_type") or meta.get("detector") or "stylized 3d character"
    description = (meta.get("description") or "").strip()
    keywords = (meta.get("keywords") or "").strip()
    body_type = body_type_from_keywords(" ".join([title, description, keywords]))

    subject = f"a {_clean_metadata_text(title) or 'stylized humanoid'}"
    detail = _clean_metadata_text(f"{description} {keywords}")[:200]
    if detail:
        subject += f", {detail}"
    outfit = "close-fitting clothing in muted colours with hair a compact shape close to the head"
    return RenderGenPlan(
        prompt=compose_prompt(subject, outfit),
        prompt_b=compose_prompt(subject, outfit, lowpoly=True),
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
    """One character description becomes both style prompts."""
    subject = str(parsed.get("subject") or "").strip()
    outfit = str(parsed.get("outfit") or "").strip()
    if len(subject) < 15:
        # tolerate the older single-prompt shape rather than dropping to the
        # template, which knows nothing about the character
        legacy = str(parsed.get("flux_prompt") or "").strip()
        if len(legacy) < 20:
            return None
        subject, outfit = legacy, ""

    body_type = str(parsed.get("body_type") or "normal").strip().lower()
    if body_type not in BODY_TYPES:
        body_type = "normal"
    # negative_prompt is inert: t_pose.json has no $negative_prompt placeholder
    # and the workflow zeroes the negative conditioning before sampling. It is
    # still filled so the other workflows that do read it keep working.
    negative = str(parsed.get("negative_prompt") or "").strip() or DEFAULT_NEGATIVE_PROMPT
    return RenderGenPlan(
        prompt=compose_prompt(subject, outfit),
        prompt_b=compose_prompt(subject, outfit, lowpoly=True),
        negative_prompt=negative[:800],
        body_type=body_type,
        mask_url=mask_url_for_body_type(body_type),
        source="llm",
    )


def _clean_collection_tags(value: Any) -> List[str]:
    if isinstance(value, str):
        raw = re.split(r"[,;|]", value)
    elif isinstance(value, list):
        raw = value
    else:
        raw = []
    result: List[str] = []
    seen = set()
    for item in raw:
        tag = re.sub(r"\s+", " ", str(item or "").strip()).strip("# ,.;")
        key = tag.casefold()
        if len(tag) < 2 or key in seen:
            continue
        seen.add(key)
        result.append(tag[:64])
        if len(result) >= 20:
            break
    return result


def _collection_from_llm_json(
    parsed: Dict[str, Any], *, collection_guid: str
) -> Optional[RenderCollectionPlan]:
    """Validate the all-at-once collection response and compose render prompts."""
    title = re.sub(r"\s+", " ", str(parsed.get("collection_title") or "").strip())
    description = re.sub(
        r"\s+", " ", str(parsed.get("collection_description") or "").strip()
    )
    tags = _clean_collection_tags(parsed.get("collection_tags"))
    raw_members = parsed.get("members")
    if (
        len(title) < 3
        or len(description) < 20
        or len(tags) < 5
        or not isinstance(raw_members, list)
        or len(raw_members) != COLLECTION_SIZE
    ):
        return None

    members: List[CollectionMemberPlan] = []
    seen_titles = set()
    seen_subjects = set()
    for index, raw in enumerate(raw_members, start=1):
        if not isinstance(raw, dict):
            return None
        member_title = re.sub(r"\s+", " ", str(raw.get("title") or "").strip())
        subject = re.sub(r"\s+", " ", str(raw.get("subject") or "").strip())
        outfit = re.sub(r"\s+", " ", str(raw.get("outfit") or "").strip())
        body_type = str(raw.get("body_type") or "normal").strip().lower()
        if body_type not in BODY_TYPES:
            body_type = "normal"
        title_key = member_title.casefold()
        subject_key = subject.casefold()
        if (
            len(member_title) < 3
            or len(subject) < 15
            or title_key in seen_titles
            or subject_key in seen_subjects
        ):
            return None
        seen_titles.add(title_key)
        seen_subjects.add(subject_key)
        members.append(
            CollectionMemberPlan(
                index=index,
                title=member_title[:120],
                prompt=compose_prompt(subject, outfit),
                negative_prompt=DEFAULT_NEGATIVE_PROMPT,
                body_type=body_type,
                mask_url=mask_url_for_body_type(body_type),
            )
        )

    return RenderCollectionPlan(
        collection_guid=collection_guid,
        collection_title=title[:256],
        collection_description=description[:2000],
        collection_tags=tags,
        members=members,
        source="llm",
    )


_FALLBACK_ROSTER = (
    ("Lead Woman", "an adult woman lead with an athletic balanced build", "normal"),
    ("Lead Man", "an adult man guardian with broad shoulders", "normal"),
    ("Young Woman", "a young adult woman scout with a slim agile build", "goblin"),
    ("Young Man", "a young adult man artisan with a lean practical build", "normal"),
    ("Grandmother", "an elderly woman keeper with a compact sturdy build", "dwarf"),
    ("Grandfather", "an elderly man scholar with a tall weathered build", "long"),
    ("Teen Girl", "a teenage girl courier with a light energetic build", "normal"),
    ("Teen Boy", "a teenage boy apprentice with a short wiry build", "goblin"),
    ("Little Hero", "a childlike mascot hero with safe simplified proportions", "dwarf"),
    ("Heavy Veteran", "a heavy-set veteran with a powerful rounded silhouette", "fat"),
    ("Tall Watcher", "a very tall slender watcher with elongated proportions", "long"),
    ("Compact Mechanic", "a short stocky mechanic with oversized work gloves", "dwarf"),
    ("Weathered Wanderer", "a weathered nonbinary wanderer with an asymmetric outfit", "normal"),
    ("Anthropomorphic Companion", "an upright anthropomorphic animal companion", "goblin"),
    ("Mystic Elder", "an ageless mystical counterpart with a distinct regal silhouette", "normal"),
)


def build_template_collection(
    meta: Dict[str, str], *, collection_guid: str
) -> RenderCollectionPlan:
    """Deterministic emergency roster; normal production uses the vision LLM."""
    anchor = _clean_metadata_text(
        meta.get("title") or meta.get("animal_type") or meta.get("detector") or "stylized character"
    ) or "stylized character"
    detail = _clean_metadata_text(
        f"{meta.get('description', '')} {meta.get('keywords', '')}"
    )[:140]
    members: List[CollectionMemberPlan] = []
    for index, (title, archetype, body_type) in enumerate(_FALLBACK_ROSTER, start=1):
        subject = f"{archetype}, belonging to the same {anchor} themed world"
        if detail:
            subject += f", sharing these visual motifs: {detail}"
        outfit = (
            f"a unique role-appropriate variation of the {anchor} collection palette, "
            "with compact opaque accessories and empty hands"
        )
        members.append(
            CollectionMemberPlan(
                index=index,
                title=f"{anchor.title()} — {title}"[:120],
                prompt=compose_prompt(subject, outfit),
                negative_prompt=DEFAULT_NEGATIVE_PROMPT,
                body_type=body_type,
                mask_url=mask_url_for_body_type(body_type),
            )
        )
    return RenderCollectionPlan(
        collection_guid=collection_guid,
        collection_title=f"{anchor.title()} Character Collection"[:256],
        collection_description=(
            f"A cohesive collection of {COLLECTION_SIZE} distinct characters derived from the "
            f"core theme, world and visual language of {anchor}, with varied ages, gender "
            "presentation, roles and silhouettes."
        ),
        collection_tags=_clean_collection_tags(
            [anchor, "character collection", "stylized", "3D", "game character", "themed pack"]
        ),
        members=members,
        source="template",
    )


VISION_CONFIG_PATH = Path(
    os.getenv("AUTORIG_VISION_CONFIG", "/root/autorig/ai_vision_animal_type_detect.json")
)


def _vision_config() -> Dict[str, Any]:
    """Credentials the web server itself uses for its vision calls.

    The env vars are the primary source, but they have gone stale before while
    this file stayed current, so it is read as a second set of candidates
    rather than only when the env is empty.
    """
    try:
        data = json.loads(VISION_CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _llm_attempts() -> list:
    """(url, key, model, extra_headers) candidates, best first, deduped by key."""
    config = _vision_config()
    openai_url = os.getenv("OPENAI_API_URL", "").strip() or str(
        config.get("open_ai_api_url_string") or ""
    ).strip() or "https://api.openai.com/v1/chat/completions"
    openrouter_url = os.getenv("OPENROUTER_API_URL", "").strip() or str(
        config.get("open_router_api_url_string") or ""
    ).strip() or "https://openrouter.ai/api/v1/chat/completions"
    openai_model = os.getenv("OPENAI_RENDER_PROMPT_MODEL", "gpt-4o-mini").strip()
    openrouter_model = os.getenv("OPENROUTER_RENDER_PROMPT_MODEL", "openai/gpt-4o-mini").strip()
    referer = {"HTTP-Referer": "https://autorig.online", "X-Title": "AutoRig Render Prompt"}

    candidates = [
        (openai_url, os.getenv("OPENAI_API_KEY", "").strip(), openai_model, {}),
        (openrouter_url, os.getenv("OPENROUTER_API_KEY", "").strip(), openrouter_model, referer),
        (openai_url, str(config.get("open_AI_api_key") or "").strip(), openai_model, {}),
        (openrouter_url, str(config.get("open_router_api_key") or "").strip(), openrouter_model, referer),
    ]
    attempts, seen = [], set()
    for url, key, model, headers in candidates:
        if not key or (url, key) in seen:
            continue
        seen.add((url, key))
        attempts.append((url, key, model, headers))
    return attempts


async def _llm_generate(
    meta: Dict[str, str], poster_data_url: Optional[str]
) -> Optional[RenderGenPlan]:
    instruction = _load_instruction()
    if not instruction:
        return None
    meta_text = "\n".join(f"{key}: {value}" for key, value in meta.items()) or "(no metadata)"
    user_text = f"{instruction}\n\nModel metadata:\n{meta_text}"

    content: Any = [{"type": "text", "text": user_text}]
    if poster_data_url:
        content.append({"type": "image_url", "image_url": {"url": poster_data_url, "detail": "low"}})

    attempts = _llm_attempts()
    if not attempts:
        print("[RenderPrompting] no LLM credentials; falling back to the template")
        return None

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


async def _llm_generate_collection(
    meta: Dict[str, str],
    poster_data_url: Optional[str],
    *,
    collection_guid: str,
) -> Optional[RenderCollectionPlan]:
    """Ask vision once for the collection concept, metadata and all 15 members."""
    instruction = _load_collection_instruction()
    if not instruction:
        return None
    meta_text = "\n".join(f"{key}: {value}" for key, value in meta.items()) or "(no metadata)"
    content: Any = [
        {
            "type": "text",
            "text": f"{instruction}\n\nReference model metadata:\n{meta_text}",
        }
    ]
    if poster_data_url:
        content.append(
            {"type": "image_url", "image_url": {"url": poster_data_url, "detail": "high"}}
        )

    attempts = _llm_attempts()
    if not attempts:
        print("[RenderPrompting] no LLM credentials; using collection template")
        return None

    for api_url, api_key, model, extra_headers in attempts:
        payload = {
            "model": model,
            "temperature": 0.65,
            "max_tokens": 6000,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "user", "content": content}],
        }
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        headers.update(extra_headers)
        try:
            async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
                resp = await client.post(api_url, headers=headers, json=payload)
            if resp.status_code != 200:
                print(
                    f"[RenderPrompting] collection LLM HTTP {resp.status_code}: "
                    f"{resp.text[:200]}"
                )
                continue
            data = resp.json()
            raw = (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
            if isinstance(raw, list):
                raw = " ".join(
                    str(part.get("text") or part) if isinstance(part, dict) else str(part)
                    for part in raw
                )
            parsed = _extract_json_object(str(raw))
            plan = (
                _collection_from_llm_json(parsed, collection_guid=collection_guid)
                if parsed
                else None
            )
            if plan is not None:
                return plan
            print("[RenderPrompting] collection LLM response failed the 15-member contract")
        except Exception as exc:
            print(f"[RenderPrompting] collection LLM call failed ({model}): {exc}")
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

    # ONE call: the character is described once and rendered in two styles, so
    # the two images show the same character. Two calls invented two different
    # characters, which made the choice meaningless.
    plan = await _llm_generate(meta, poster)
    if plan is None:
        plan = build_template_plan(meta)
    return plan


async def build_render_collection(task_id: str) -> RenderCollectionPlan:
    """Build one durable collection id and all 15 member prompts in one LLM call."""
    import uuid

    task = None
    try:
        from database import AsyncSessionLocal, Task
        from sqlalchemy import select

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Task).where(Task.id == task_id))
            task = result.scalar_one_or_none()
    except Exception as exc:
        print(f"[RenderPrompting] task load failed for collection {task_id}: {exc}")

    meta = _task_metadata_summary(task) if task is not None else {}
    poster = _poster_data_url(task_id)
    collection_guid = str(uuid.uuid4())
    plan = await _llm_generate_collection(
        meta,
        poster,
        collection_guid=collection_guid,
    )
    if plan is None:
        plan = build_template_collection(meta, collection_guid=collection_guid)
    return plan


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
                "prompt_b": plan.prompt_b,
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


async def start_character_collection(
    plan: RenderCollectionPlan,
    *,
    source_task_id: str = "",
    user_name: str = "autorig-bot",
    telegram_chat_id: int = 0,
    telegram_status_message_id: int = 0,
) -> List[str]:
    """Atomically hand one 15-member collection to the persisted render queue."""
    body = {
        "collection_guid": plan.collection_guid,
        "collection_title": plan.collection_title,
        "collection_description": plan.collection_description,
        "collection_tags": list(plan.collection_tags),
        "user_name": user_name,
        "source_task_id": source_task_id,
        "telegram_chat_id": telegram_chat_id,
        "telegram_status_message_id": telegram_status_message_id,
        "members": [
            {
                "index": member.index,
                "title": member.title,
                "prompt": member.prompt,
                # Deliberately one render per member. There is no useful
                # 15-times approval step; a coherent collection is the choice.
                "prompt_b": "",
                "negative_prompt": member.negative_prompt,
                "mask_url": member.mask_url,
            }
            for member in plan.members
        ],
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{RENDERFIN_INTERNAL_URL}/api-character-gen/collection",
            json=body,
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"character collection start failed: HTTP {resp.status_code} {resp.text[:300]}"
        )
    payload = resp.json()
    job_ids = [str(item.get("job_id") or "") for item in payload.get("jobs") or []]
    job_ids = [job_id for job_id in job_ids if job_id]
    if len(job_ids) != COLLECTION_SIZE:
        raise RuntimeError(
            f"character collection start returned {len(job_ids)}/{COLLECTION_SIZE} jobs"
        )
    return job_ids


async def start_character_gen_from_image(
    image_url: str,
    *,
    source_task_id: str = "",
    user_name: str = "autorig-bot",
) -> str:
    """Start a 3D generation from a user-supplied picture. Returns job_id."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{RENDERFIN_INTERNAL_URL}/api-character-gen/from-image",
            json={
                "image_url": image_url,
                "user_name": user_name,
                "source_task_id": source_task_id,
            },
        )
    if resp.status_code != 200:
        raise RuntimeError(
            f"character-gen from-image failed: HTTP {resp.status_code} {resp.text[:300]}"
        )
    job_id = str(resp.json().get("job_id") or "")
    if not job_id:
        raise RuntimeError("character-gen from-image returned no job_id")
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


async def approve_character_gen_image(job_id: str, *, variant: str = "a") -> Dict[str, Any]:
    """Approve one rendered variant; the pipeline continues to the 3D stage."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/approve-image",
            params={"variant": variant},
        )
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


async def mark_character_gen_submitted(job_id: str, task_id: str = "") -> None:
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            await client.post(
                f"{RENDERFIN_INTERNAL_URL}/api-character-gen/{job_id}/mark-submitted",
                params={"task_id": task_id} if task_id else None,
            )
    except Exception as exc:
        print(f"[RenderPrompting] mark-submitted failed: {exc}")


async def sweep_character_gen_chats() -> int:
    """Empty the private chats; the next delivery pass re-posts the live queue."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(f"{RENDERFIN_INTERNAL_URL}/api-character-gen/sweep-chats")
    if resp.status_code != 200:
        raise RuntimeError(f"sweep failed: HTTP {resp.status_code} {resp.text[:200]}")
    return int(resp.json().get("removed") or 0)


async def cleanup_character_gen_chat(task_id: str) -> int:
    """Ask renderfin to remove a finished job's cards from the private chat.

    Best effort by design: a chat that cannot be tidied is not a reason to
    hold up anything the user is actually waiting for.
    """
    if not task_id:
        return 0
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{RENDERFIN_INTERNAL_URL}/api-character-gen/cleanup-for-task/{task_id}"
            )
        if resp.status_code != 200:
            return 0
        return int(resp.json().get("cleaned") or 0)
    except Exception as exc:
        print(f"[RenderPrompting] chat cleanup failed for task {task_id}: {exc}")
        return 0

"""What a model is wearing and how its hair falls, judged once at upload.

The rig pipelines now split at the start. A model whose hair and clothes will
not swing gets the ordinary rig; one with long hair, a skirt, a cape or a tail
is offered the AI pipeline that animates them. The split has to be decided
before any rigging starts, so it is made from the same four renders the browser
already takes for the rig-type check, tiled into one picture and judged in a
single call.

Measured on twelve site models (24.09.2026): ten judged right, and both misses
went the safe way — a model offered the AI pipeline it did not need, never one
denied it. The user makes the final choice either way.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import httpx

PROMPT = """The picture shows one 3D character from four sides: front, back,
left and right. Report what a rigger needs to know before animating it.

Answer with one JSON object and nothing else:
{
 "hair": "none" | "short" | "long",
 "hair_note": "a few words, e.g. ponytail to the waist, spiky and short, hidden under a helmet",
 "headwear": true | false,
 "loose_clothing": ["skirt" | "dress" | "cape" | "coat_tail" | "robe" | "scarf" | "loose_sleeves"],
 "tail": true | false,
 "pose": "t_pose" | "a_pose" | "other",
 "pose_note": "a few words when the pose is other, e.g. arms down, sitting, fists raised"
}
"t_pose": arms straight out to the sides, level with the shoulders. "a_pose":
arms straight but angled down about 30 to 50 degrees. Anything else is "other".
"short" hair barely moves: a buzz cut, a crop, tight spikes. "long" hair would
swing when the character moves: shoulder length or longer, ponytails, braids,
a mane. Hair hidden under a helmet or hood is "none" with headwear true.
List loose clothing only if it hangs free of the body. An empty list is fine."""

# Cloth that swings free of the body and needs its own simulation. Sleeves are
# left out on purpose: the model calls a T-shirt's sleeves "loose", and a
# T-shirt should not push anyone toward the paid pipeline.
SWINGING = ("skirt", "dress", "cape", "coat_tail", "robe", "scarf")
HAIR_VALUES = ("none", "short", "long")
POSE_VALUES = ("t_pose", "a_pose", "other")

LABELS = {
    "long hair": "long hair",
    "skirt": "skirt",
    "dress": "dress",
    "cape": "cape",
    "coat_tail": "coat tails",
    "robe": "robe",
    "scarf": "scarf",
    "tail": "tail",
}


def parse(text: str) -> Dict[str, Any]:
    """The model's JSON, reduced to the fields and values this module knows."""
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("no JSON object in the answer")
    raw = json.loads(text[start:end + 1])
    hair = str(raw.get("hair") or "").strip().lower()
    pose = str(raw.get("pose") or "").strip().lower().replace("-", "_").replace(" ", "_")
    clothing = raw.get("loose_clothing") or []
    if not isinstance(clothing, list):
        clothing = [clothing]
    return {
        "hair": hair if hair in HAIR_VALUES else "unknown",
        "hair_note": str(raw.get("hair_note") or "")[:120],
        "headwear": bool(raw.get("headwear")),
        "loose_clothing": sorted({str(c).strip().lower() for c in clothing if str(c).strip()})[:8],
        "tail": bool(raw.get("tail")),
        "pose": pose if pose in POSE_VALUES else "unknown",
        "pose_note": str(raw.get("pose_note") or "")[:120],
    }


def decide(assessment: Dict[str, Any]) -> Dict[str, Any]:
    """Which pipeline to recommend, and the reasons in words a user reads."""
    reasons: List[str] = []
    if assessment.get("hair") == "long":
        reasons.append(LABELS["long hair"])
    reasons += [LABELS[c] for c in assessment.get("loose_clothing", []) if c in SWINGING]
    if assessment.get("tail"):
        reasons.append(LABELS["tail"])
    # A pose that is not a T-pose is flagged on its own: the AI pipeline can
    # regenerate the pose, but the ordinary rig copes with an A-pose, so it is a
    # marker for the user, not a reason by itself.
    pose = assessment.get("pose")
    return {"recommended_pipeline": "ai_pro" if reasons else "simple", "reasons": reasons,
            "pose_warning": pose == "other",
            "not_t_pose": pose in ("a_pose", "other")}


NOTE_LANGUAGES = {"ru": "Russian", "en": "English", "zh": "Chinese", "hi": "Hindi"}


FOUR_SIDES = "The picture shows one 3D character from four sides: front, back,\nleft and right"
FRONT_ONLY = ("The picture shows one 3D character from the front. It was painted from the "
              "model's shape alone, so judge by shapes and by where colours change")


async def assess(*, api_url: str, api_key: str, model: str, image_data_url: str,
                 lang: str = "en", front_only: bool = False) -> Dict[str, Any]:
    # The notes are shown on the page as they come; the labels stay English
    # because the page translates those itself.
    language = NOTE_LANGUAGES.get(str(lang or "")[:2].lower(), "English")
    base = PROMPT.replace(FOUR_SIDES, FRONT_ONLY) if front_only else PROMPT
    prompt = base + (f"\nWrite hair_note and pose_note in {language}; "
                     "keep every other value exactly as listed.")
    payload: Dict[str, Any] = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                # Hair is a small part of a full-body picture; "low" detail
                # downsamples it into the collar.
                {"type": "image_url", "image_url": {"url": image_data_url, "detail": "high"}},
            ],
        }],
    }
    if model.startswith(("gpt-5", "o3", "o4")):
        payload["max_completion_tokens"] = 1200
    else:
        payload["temperature"] = 0
        payload["max_tokens"] = 300
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.post(api_url, json=payload,
                                 headers={"Authorization": f"Bearer {api_key}"})
    if resp.status_code != 200:
        raise RuntimeError(f"vision HTTP {resp.status_code}: {resp.text[:200]}")
    content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    if isinstance(content, list):
        content = " ".join(str(p.get("text") if isinstance(p, dict) else p) for p in content)
    result = parse(str(content))
    return {**result, **decide(result)}


MODEL_PATTERN = re.compile(r"^[A-Za-z0-9._:/-]{1,160}$")


# --------------------------------------------------------------- untextured
# A model with no textures renders as grey clay, and on clay long hair and a
# robe are the same grey. The farm is asked to paint the character back from
# its Z-depth (ControlNet depth, Z-Image), and the painting is what gets judged:
# the silhouette is the model's own, the colours only separate its parts.
DEPTH_PROMPT = ("a 3D game character, full body, front view, detailed colourful outfit, "
                "hair, skin and every piece of clothing in clearly different colours, "
                "studio lighting, plain grey background")
DEPTH_NEGATIVE = "monochrome, grayscale, white statue, plaster, depth map, blurry"
DEPTH_POLL_SECONDS = 150


async def paint_from_depth(*, site_base: str, depth_url: str,
                           width: int = 576, height: int = 1024) -> str:
    """Ask the farm to repaint the character from its depth map; return the picture URL."""
    import asyncio
    import time as _time

    body = {"prompt": DEPTH_PROMPT, "negative_prompt": DEPTH_NEGATIVE,
            "control_depth": depth_url,
            # Measured on the sorceress: at the default strength and partial
            # denoise the farm hands the depth map back almost untouched.
            "control_strength": 0.75, "control_end": 0.8, "creativity": 1.0,
            "width": width, "height": height}
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(f"{site_base}/api/image", json=body)
        if resp.status_code != 200:
            raise RuntimeError(f"image farm HTTP {resp.status_code}: {resp.text[:200]}")
        url = str(resp.json().get("image_url_string") or "")
        if not url:
            raise RuntimeError("image farm returned no picture address")
        deadline = _time.time() + DEPTH_POLL_SECONDS
        while _time.time() < deadline:
            probe = await client.get(url)
            if probe.status_code == 200 and probe.headers.get("content-type", "").startswith("image"):
                return url
            await asyncio.sleep(4)
    raise RuntimeError("the farm did not paint the model in time")

"""The typed text of a generator node edits the prompt wired into it (owner, 2026-09-28).

Image, Video and Qwen-Image take `prompt_edit` (an instruction, any language)
and `prompt_translate` (default on):
  - prompt and prompt_edit -> the Text nodes' LLM applies the instruction to
    the prompt and the result (English) is what renders;
  - only a prompt, not English, prompt_translate on -> translated to English;
  - an English prompt about people, prompt_translate on -> the LLM gives every
    face a vivid emotion (owner, 2026-09-29: "faces are always soulless and
    static"), and a clip also gets what we hear;
  - otherwise the prompt is unchanged.
Every LLM step above also applies the emotion rule. Answers are cached per
(prompt, instruction, mode), so a re-render does not ask the LLM again. LoRA
triggers are added afterwards by the caller. prompt_translate off leaves the
prompt exactly as written.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from typing import Dict, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

LOCAL = "http://127.0.0.1:8200"
MODEL = "qwen35-9b-uncensored"   # the Text nodes' default, uncensored, reasoning off
# LoRA trigger words and <lora:...> tags must come through word for word (owner,
# 2026-09-29): a trigger phrase is what the LoRA was trained on, and a
# paraphrase of it ("lies prone ... in the prone bone position") does not fire
# it. The selected LoRAs' own triggers are put in front after this step anyway.
def system_for(name: str, default: str) -> str:
    """A standing system prompt, live-editable on /system_prompts."""
    try:
        import prompt_library
        return prompt_library.rule(name, "") or default
    except Exception:
        return default


EDIT_SYSTEM = ("You edit image/video generation prompts. Apply the user's instruction to the prompt: change only "
               "what the instruction asks, keep everything else (subject, setting, style, camera) and every "
               "<image1>/<image2>/<image3> picture tag exactly, keep every <lora:...> tag and every LoRA trigger "
               "word or phrase exactly as written, and put a phrase the instruction quotes or asks to add into the "
               "prompt word for word, never paraphrased, output the final prompt in English only, no commentary.")
TRANSLATE_SYSTEM = ("You translate image/video generation prompts into English. Keep every detail, order, "
                    "<lora:...> tag and <imageN> picture tag exactly, and keep LoRA trigger words and phrases and "
                    "every phrase already in English word for word; output only the translated prompt, no commentary.")
# Vivid emotions by default, in every generator's prompt (owner, 2026-09-29).
# Second pass the same day: "she laughs too much" - teasing and flirting, a
# wink, her tongue out, but not all of them in one picture. 2026-09-30: the
# tongue stuck out in every generation - it is out of the examples.
EXPRESSIONS = ("a teasing smirk", "a sly flirty glance", "a playful wink", "fluttering her eyelashes",
               "a slow sultry smile", "biting her lip", "blowing a kiss", "a mischievous grin")
EXPRESSION_RULE_HEAD = ("Faces must never be blank, neutral or static: give every person in the prompt ONE vivid, "
                        "playful facial expression that fits the moment, named right next to that person. Prefer "
                        "teasing and flirting: ")
EXPRESSION_RULE_TAIL = (". Pick one of them per person (in a video at most two, one after the other), never a list "
                        "of them, and do not add laughing, giggling or a stuck-out tongue unless the prompt asks for it. If the prompt already names an emotion, keep "
                        "it as it is; never make a face calm, neutral or serious unless the prompt asks for exactly "
                        "that. The expression is added to what already happens, never instead of it (owner "
                        "2026-09-30: the smiles were crowding out the plot): keep every action, every person and "
                        "every position of the prompt exactly as written, in the same order, and never shorten, "
                        "soften or drop them to make room for a face.")
EXPRESSION_RULE = EXPRESSION_RULE_HEAD + ", ".join(EXPRESSIONS) + EXPRESSION_RULE_TAIL


def expression_rule(text: str = "") -> str:
    """The rule with its examples starting at a different place for every prompt.

    The model takes the first example: every shot got "a teasing smirk"
    (2026-09-29). The order follows the prompt's hash, so the same prompt
    always gets the same rule and the answer cache still hits.
    """
    if not text:
        return EXPRESSION_RULE
    start = int(hashlib.sha256(text.encode("utf-8")).hexdigest(), 16) % len(EXPRESSIONS)
    return EXPRESSION_RULE_HEAD + ", ".join(EXPRESSIONS[start:] + EXPRESSIONS[:start]) + EXPRESSION_RULE_TAIL
# LTX-2 voices only what its prompt describes: without this a clip comes out
# near-silent (measured 2026-09-29: -57 dB mean on a 10Eros render).
SOUND_RULE = ("This prompt is for a video with sound: unless the prompt already describes what we hear, end it "
              "with one short sentence starting 'Sound:' that says what is heard - breathing, moans, gasps, wet "
              "sounds, skin, fabric and the room. Owner 2026-09-30: the actors kept talking in shots that have no "
              "dialogue. Nobody speaks unless the prompt itself says someone speaks: never add speech, quoted "
              "words, whispered phrases, names or dialogue, and no laughing or giggling unless the prompt has it.")
EXPRESS_SYSTEM = ("You improve image/video generation prompts. {rules} Change nothing else: keep the subject, "
                  "every person, pose, action, setting, style, camera, framing and the order of the text; keep "
                  "every <image1>/<image2>/<image3> picture tag, every <lora:...> tag and every LoRA trigger word "
                  "or phrase exactly as written, and keep the first word of the prompt as it is. If the prompt "
                  "has no people or faces, return it unchanged. Output only the final prompt in English, no "
                  "commentary.")
# A prompt that names no person or face goes out without an LLM step.
PEOPLE = re.compile(r"\b(she|her|hers|herself|he|him|his|himself|they|them|woman|women|man|men|girl|girls|boy|"
                    r"boys|lady|ladies|guy|guys|person|people|face|faces|portrait|selfie|actor|actress|character|"
                    r"couple|sura|mouth|lips|smil\w*|laugh\w*|kiss\w*|moan\w*|expression)\b", re.IGNORECASE)
_cache: Dict[str, str] = {}
_MAX_CACHE = 2000


def is_english(text: str) -> bool:
    letters = re.findall(r"[^\W\d_]", str(text or ""))
    if not letters:
        return True
    latin = sum(1 for ch in letters if ch.isascii())
    return latin / len(letters) > 0.9


def usable(answer: str, text: str) -> bool:
    """False for an answer that is not a prompt: nearly all one symbol ("????", seen
    2026-09-29 right after a restart), fewer than two words, or a runaway length."""
    compact = "".join(str(answer or "").split())
    if not compact or len(re.findall(r"[^\W\d_]{2,}", answer)) < 2:
        return False
    char = max(set(compact), key=compact.count)
    if not char.isalnum() and compact.count(char) >= 0.5 * len(compact):
        return False
    return len(answer) <= 4 * len(text) + 600


def rules_for(kind: str, text: str = "") -> str:
    """The standing rules every LLM step of a generator prompt adds.

    Either rule can be rewritten on /system_prompts; an edited expression rule
    is used as it stands, without the rotation of its examples.
    """
    try:
        import prompt_library
        expression = prompt_library.rule("expression", "") or expression_rule(text)
        sound = prompt_library.rule("sound", "") or SOUND_RULE
    except Exception:
        expression, sound = expression_rule(text), SOUND_RULE
    return expression + (" " + sound if kind == "video" else "")


async def _ask(system: str, user: str) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.post(LOCAL + "/api/text2text", json={
            "system_prompt": system, "prompt": user, "model": MODEL, "structured": True,
            "wait_seconds": 60}, timeout=90.0)
        data = response.json() if response.content else {}
        deadline = time.monotonic() + 300
        while not data.get("answer_string") and time.monotonic() < deadline:
            task = data.get("task_id_string")
            if not task or data.get("error_string"):
                break
            await asyncio.sleep(2)
            data = (await client.get(f"{LOCAL}/api/ai/status/{task}", timeout=30.0)).json()
    return str(data.get("answer_string") or "").strip().strip('"').strip()


async def resolve(prompt: Optional[str], edit: Optional[str], translate: Optional[bool] = True,
                  kind: str = "image") -> Tuple[str, str]:
    """(prompt to render, what was done: '', 'edited', 'translated' or 'expressive').

    `kind` is 'image' (pictures and Qwen edits) or 'video' (clips also get what
    we hear).
    """
    text = str(prompt or "").strip()
    instruction = str(edit or "").strip()
    translate = True if translate is None else bool(translate)
    rules = rules_for(kind, text + "\x00" + instruction)
    if text and instruction:
        mode, system, user = ("edited", EDIT_SYSTEM + " " + rules,
                              "PROMPT:\n" + text + "\n\nINSTRUCTION:\n" + instruction)
    else:
        if not text and instruction:
            # Only the typed text: it is the prompt itself.
            text = instruction
        if not text or not translate:
            return text, ""
        if not is_english(text):
            mode, system, user = "translated", TRANSLATE_SYSTEM + " " + rules, text
        elif PEOPLE.search(text):
            mode, system, user = "expressive", EXPRESS_SYSTEM.replace("{rules}", rules), text
        else:
            return text, ""
    key = hashlib.sha256((mode + "\x00" + system + "\x00" + user).encode("utf-8")).hexdigest()
    if key in _cache:
        return _cache[key], mode
    answer = ""
    for attempt in range(2):
        try:
            answer = await _ask(system, user)
        except Exception:
            logger.exception("prompt edit failed")
            answer = ""
        if usable(answer, text):
            break
        logger.warning("prompt %s: unusable LLM answer %r, %s", mode, str(answer)[:60],
                       "asking again" if attempt == 0 else "rendering the prompt as it was")
        answer = ""
    if not answer:
        # The LLM did not answer: render what we have rather than fail the job.
        return (text + (", " + instruction if mode == "edited" else "")), mode + "-fallback"
    answer = strip_added_lora_tags(answer, text + " " + instruction)
    if len(_cache) > _MAX_CACHE:
        _cache.clear()
    _cache[key] = answer
    return answer, mode


_LORA_TAG = re.compile(r"<\s*(lora|lyco|locon)\s*:[^<>]*>", re.IGNORECASE)


def strip_added_lora_tags(answer: str, source: str) -> str:
    """A LoRA tag the LLM made up is dropped (lead 2026-10-20: the expressive rewrite of a parts-isolate prompt
    answered «<lora:beard_style_v1:0.7> …» and /api/qwen-image refused the whole job as unknown_lora, cached). Tags the
    caller wrote stay: only an answer with tags where the source had none is cleaned."""
    text = str(answer or "")
    if not _LORA_TAG.search(text) or _LORA_TAG.search(str(source or "")):
        return text
    return re.sub(r"\s{2,}", " ", _LORA_TAG.sub("", text)).strip()

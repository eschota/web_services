"""The typed text of a generator node edits the prompt wired into it (owner, 2026-09-28).

Image, Video and Qwen-Image take `prompt_edit` (an instruction, any language)
and `prompt_translate` (default on):
  - prompt and prompt_edit -> the Text nodes' LLM applies the instruction to
    the prompt and the result (English) is what renders;
  - only a prompt, not English, prompt_translate on -> translated to English;
  - otherwise the prompt is unchanged.
Answers are cached per (prompt, instruction, translate), so a re-render does
not ask the LLM again. LoRA triggers are added afterwards by the caller.
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
EDIT_SYSTEM = ("You edit image/video generation prompts. Apply the user's instruction to the prompt: change only "
               "what the instruction asks, keep everything else (subject, setting, style, camera), output the final "
               "prompt in English only, no commentary.")
TRANSLATE_SYSTEM = ("You translate image/video generation prompts into English. Keep every detail, order and "
                    "<lora:...> tag exactly; output only the translated prompt, no commentary.")
_cache: Dict[str, str] = {}
_MAX_CACHE = 2000


def is_english(text: str) -> bool:
    letters = re.findall(r"[^\W\d_]", str(text or ""))
    if not letters:
        return True
    latin = sum(1 for ch in letters if ch.isascii())
    return latin / len(letters) > 0.9


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


async def resolve(prompt: Optional[str], edit: Optional[str], translate: Optional[bool] = True) -> Tuple[str, str]:
    """(prompt to render, what was done: '', 'edited' or 'translated')."""
    text = str(prompt or "").strip()
    instruction = str(edit or "").strip()
    translate = True if translate is None else bool(translate)
    if text and instruction:
        mode, system, user = "edited", EDIT_SYSTEM, "PROMPT:\n" + text + "\n\nINSTRUCTION:\n" + instruction
    elif not text and instruction:
        # Only the typed text: it is the prompt itself.
        text = instruction
        if not translate or is_english(text):
            return text, ""
        mode, system, user = "translated", TRANSLATE_SYSTEM, text
    elif text and translate and not is_english(text):
        mode, system, user = "translated", TRANSLATE_SYSTEM, text
    else:
        return text, ""
    key = hashlib.sha256((mode + "\x00" + system + "\x00" + user).encode("utf-8")).hexdigest()
    if key in _cache:
        return _cache[key], mode
    try:
        answer = await _ask(system, user)
    except Exception:
        logger.exception("prompt edit failed")
        answer = ""
    if not answer:
        # The LLM did not answer: render what we have rather than fail the job.
        return (text + (", " + instruction if mode == "edited" else "")), mode + "-fallback"
    if len(_cache) > _MAX_CACHE:
        _cache.clear()
    _cache[key] = answer
    return answer, mode

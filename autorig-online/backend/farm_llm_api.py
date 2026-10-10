"""OpenAI chat-completions shape in front of the fleet's own Qwen (owner 2026-10-11: no paid LLM APIs).

Every consumer in the backend that used to POST an OpenAI-style ``chat/completions`` payload (poster metadata,
rig-type vision, appearance check, viewer-theme pick, idle-LTX prompts, render prompting, generation routing) is
pointed here while ``AUTORIG_PAID_LLM`` is off (see paid_llm.py). The shim turns the payload into the site's own
``/api/vision`` (a picture is present) or ``/api/text2text`` (text only) call, waits for a free ai-node, and answers
in the chat-completions shape, so the callers' parsing and fall-backs stay as they were.

It adds no capability that /api/vision and /api/text2text do not already offer to anyone, so it needs no key.
Only the first picture of a request is read; ``response_format: json_object`` becomes a standing instruction and
the answer is cut down to its outermost JSON object. A farm that cannot answer gives an OpenAI-style 502 error,
which every caller already treats as «the LLM is unavailable» (template / heuristic result).
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import paid_llm

router = APIRouter()

LOCAL = os.getenv("AUTORIG_FARM_LLM_LOCAL", "http://127.0.0.1:8200").rstrip("/")
MODEL_LABEL = "qwen35-9b"
TOTAL_WAIT_SECONDS = 175.0       # how long a caller is held, waiting for a free node included
MIN_TOKENS, MAX_TOKENS = 256, 4096   # the model may think before it answers: never starve it
PROMPT_LIMIT, SYSTEM_LIMIT = 7900, 3900
JSON_RULE = ("Answer with exactly one valid JSON object and nothing else: no markdown fences, "
             "no commentary before or after it.")


class FarmUnavailable(RuntimeError):
    pass


def _text_of(content: Any) -> Tuple[str, List[str]]:
    """(text, [image urls]) of one chat message content."""
    if isinstance(content, str):
        return content, []
    texts: List[str] = []
    images: List[str] = []
    for part in content or []:
        if isinstance(part, str):
            texts.append(part)
        elif isinstance(part, dict):
            kind = part.get("type")
            if kind in ("text", "input_text"):
                texts.append(str(part.get("text") or ""))
            elif kind in ("image_url", "input_image"):
                raw = part.get("image_url")
                url = raw.get("url") if isinstance(raw, dict) else raw
                if url:
                    images.append(str(url))
    return "\n".join(t for t in texts if t), images


def split_messages(messages: List[Dict[str, Any]]) -> Tuple[str, str, Optional[str]]:
    system_parts: List[str] = []
    user_parts: List[str] = []
    image: Optional[str] = None
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        text, images = _text_of(message.get("content"))
        if message.get("role") in ("system", "developer"):
            if text:
                system_parts.append(text)
        else:
            if text:
                user_parts.append(text)
            if images and image is None:
                image = images[0]
    return "\n\n".join(system_parts), "\n\n".join(user_parts), image


def only_json_object(text: str) -> str:
    """The outermost {...} of the answer when it parses; the answer itself otherwise."""
    start, end = text.find("{"), text.rfind("}")
    if 0 <= start < end:
        candidate = text[start:end + 1]
        try:
            json.loads(candidate)
            return candidate
        except ValueError:
            pass
    return text


def _fit(system: str, user: str) -> Tuple[str, str]:
    """The farm takes <= 8000 chars of prompt and <= 4000 of system prompt; keep the tail of a long prompt."""
    if len(user) > PROMPT_LIMIT:
        spill, user = user[:len(user) - PROMPT_LIMIT], user[len(user) - PROMPT_LIMIT:]
        system = (system + "\n\n" + spill).strip()
    if len(system) > SYSTEM_LIMIT:
        system = system[:SYSTEM_LIMIT]
    return system, user


async def ask_farm(*, prompt: str, system: str = "", image: Optional[str] = None,
                   max_tokens: int = 1024, total_wait: float = TOTAL_WAIT_SECONDS) -> str:
    """One question to the fleet's Qwen through the site's own API; the answer text, or FarmUnavailable."""
    deadline = time.monotonic() + total_wait
    system, prompt = _fit(system, prompt)
    body: Dict[str, Any] = {"prompt": prompt or "Describe the picture.", "max_output_tokens": max_tokens}
    if system:
        body["system_prompt"] = system
    if image:
        path = "/api/vision"
        if image.startswith("data:"):
            body["image_base64"] = image
        else:
            body["image_url"] = image
    else:
        path = "/api/text2text"
    last = "no answer"
    async with httpx.AsyncClient(timeout=httpx.Timeout(210.0, connect=10.0)) as client:
        doc: Dict[str, Any] = {}
        while True:
            remaining = deadline - time.monotonic()
            if remaining < 5:
                raise FarmUnavailable(last)
            body["wait_seconds"] = min(150.0, remaining - 3)
            try:
                resp = await client.post(LOCAL + path, json=body)
            except httpx.HTTPError as exc:
                raise FarmUnavailable(f"farm api unreachable: {type(exc).__name__}") from None
            if resp.status_code in (429, 502, 503, 504):
                # No free ai-node right now: wait for one instead of failing the caller.
                last = f"HTTP {resp.status_code}: no free ai-node"
                await asyncio.sleep(6)
                continue
            if resp.status_code != 200:
                raise FarmUnavailable(f"HTTP {resp.status_code}: {resp.text[:160]}")
            doc = resp.json()
            break
        task = str(doc.get("task_id_string") or "")
        while not doc.get("finished_bool") and task and time.monotonic() < deadline:
            await asyncio.sleep(3)
            try:
                status = await client.get(f"{LOCAL}/api/ai/status/{task}", timeout=60)
                doc = status.json() if status.status_code == 200 else doc
            except (httpx.HTTPError, ValueError):
                continue
    answer = str(doc.get("answer_string") or "").strip()
    if answer and str(doc.get("status_string") or "") in ("completed", ""):
        return answer
    raise FarmUnavailable(str(doc.get("error_string") or doc.get("status_string") or "no answer")[:200])


async def ask_openrouter_free(messages: List[Dict[str, Any]], *, wants_json: bool, max_tokens: int) -> str:
    """Fallback when the farm cannot answer: OpenRouter FREE models only (owner 2026-10-11), within the daily allowance."""
    cred = paid_llm.openrouter_free()
    if not cred:
        raise FarmUnavailable("no OpenRouter free credential")
    key, url, models = cred
    msgs = list(messages)
    if wants_json:
        msgs = [{"role": "system", "content": JSON_RULE}] + msgs
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json",
               "HTTP-Referer": "https://autorig.online", "X-Title": "AutoRig farm fallback"}
    last = "no free model answered"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for model in models[:3]:
            if not model.endswith(":free"):     # belt and braces: free_only() already filtered
                continue
            if not paid_llm.openrouter_free_take():
                last = "OpenRouter free daily allowance spent"
                break
            try:
                resp = await client.post(url, headers=headers, json={
                    "model": model, "messages": msgs, "max_tokens": max_tokens, "temperature": 0.2})
            except httpx.HTTPError as exc:
                last = f"{model}: {type(exc).__name__}"
                continue
            if resp.status_code != 200:
                last = f"{model}: HTTP {resp.status_code}"
                continue
            try:
                content = resp.json()["choices"][0]["message"]["content"]
            except (KeyError, IndexError, ValueError, TypeError):
                content = ""
            if isinstance(content, list):
                content = " ".join(str(p.get("text") or "") if isinstance(p, dict) else str(p) for p in content)
            if str(content).strip():
                return str(content).strip()
            last = f"{model}: empty answer"
    raise FarmUnavailable(last)


def _completion(text: str) -> Dict[str, Any]:
    return {
        "id": "chatcmpl-farm-" + uuid.uuid4().hex[:12],
        "object": "chat.completion",
        "created": int(time.time()),
        "model": MODEL_LABEL,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def _error(status: int, message: str, code: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {
        "message": message, "type": "farm_llm_error", "code": code}})


@router.get("/api/farm-llm/models")
@router.get("/api/farm-llm/v1/models")
async def farm_llm_models():
    return {"object": "list", "data": [{"id": MODEL_LABEL, "object": "model", "owned_by": "autorig-fleet"}]}


@router.post("/api/farm-llm/chat/completions")
@router.post("/api/farm-llm/v1/chat/completions")
async def farm_llm_chat(request: Request):
    try:
        body = await request.json()
    except ValueError:
        return _error(400, "invalid JSON body", "bad_request")
    if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
        return _error(400, "messages must be a list", "bad_request")
    system, user, image = split_messages(body["messages"])
    wants_json = str((body.get("response_format") or {}).get("type") or "") in ("json_object", "json_schema")
    if wants_json:
        system = (system + "\n\n" + JSON_RULE).strip()
    asked = body.get("max_completion_tokens") or body.get("max_tokens") or 1024
    try:
        tokens = max(MIN_TOKENS, min(MAX_TOKENS, int(asked)))
    except (TypeError, ValueError):
        tokens = 1024
    try:
        text = await ask_farm(prompt=user, system=system, image=image, max_tokens=tokens)
    except FarmUnavailable as exc:
        print(f"[FarmLLM] unavailable: {exc}; trying OpenRouter free")
        try:
            text = await ask_openrouter_free(body["messages"], wants_json=wants_json, max_tokens=tokens)
        except FarmUnavailable as exc2:
            print(f"[FarmLLM] OpenRouter free unavailable: {exc2}")
            return _error(502, f"the fleet's LLM could not answer: {exc}; {exc2}", "farm_unavailable")
    return _completion(only_json_object(text) if wants_json else text)

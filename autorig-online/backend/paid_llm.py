"""The paid-LLM switch (owner 2026-10-11: «платные нам нельзя использовать, будем думать как обходиться
возможностями флота»).

``AUTORIG_PAID_LLM`` decides whether any code of the web backend may call a paid language-model API (OpenAI,
OpenRouter, DeepSeek, Anthropic). It is OFF unless the variable is exactly one of 1/on/true/yes/allow, so a
service that never heard of the switch is safe too. While it is off:

* ``scrub_environ()`` (called by config.py and render_prompting.py at import) removes the paid keys from the
  process environment, so no ``os.getenv("OPENAI_API_KEY")`` anywhere can find one;
* every consumer that is driven by the web server's vision config goes through ``vision_cfg()``, which swaps
  OpenAI / OpenRouter for the farm shim (farm_llm_api.py: the OpenAI chat-completions shape in front of
  /api/vision and /api/text2text, i.e. the fleet's own Qwen);
* OpenRouter ``:free`` models (owner decision 2026-10-11) are the only external LLM still allowed: ``free_only()``
  drops every other model id, ``openrouter_free()`` hands out the key from the web server's vision config, and
  ``openrouter_free_take()`` keeps the free tier's daily request allowance (AUTORIG_OPENROUTER_FREE_DAILY, 40).
  They serve the quality-critical, low-volume rig-type check first and the farm shim as its fallback;
  everything high-volume goes to the farm first;
* ``farm_candidate()`` is the one pseudo-credential the OpenAI SDK users (content_moderation) are given.

Nothing is deleted from the secrets files: ``AUTORIG_PAID_LLM=on`` in the service environment plus a restart
brings the paid path back exactly as it was.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

FLAG = "AUTORIG_PAID_LLM"
PAID_ENV_KEYS = ("OPENAI_API_KEY", "OPENROUTER_API_KEY", "DEEPSEEK_API_KEY", "ANTHROPIC_API_KEY")
# The shim lives in the backend itself (farm_llm_api.py); callers reach it over loopback like any API client.
FARM_BASE = os.getenv("AUTORIG_FARM_LLM_URL", "http://127.0.0.1:8200/api/farm-llm").rstrip("/")
FARM_CHAT_URL = FARM_BASE + "/chat/completions"
FARM_MODELS_URL = FARM_BASE + "/models"
FARM_MODEL = "qwen35-9b"
FARM_KEY = "farm"           # not a secret: the shim ignores it, the OpenAI SDK only insists on a non-empty one

_VISION_KEYS_TO_DROP = ("open_AI_api_key", "open_ai_api_key", "open_router_api_key", "open_AI_api_key_string")
_MODEL_KEYS = (
    "open_ai_vision_model_string",
    "open_ai_strong_vision_model_string",
    "open_ai_viewer_theme_vision_model_string",
    "open_ai_idle_ltx_vision_model_string",
)


def enabled() -> bool:
    """True only when the owner has switched paid LLM calls back on."""
    return os.environ.get(FLAG, "").strip().lower() in ("1", "on", "true", "yes", "allow")


def scrub_environ() -> None:
    """Forget the paid keys in this process while the switch is off (the files are not touched)."""
    if enabled():
        return
    for name in PAID_ENV_KEYS:
        os.environ.pop(name, None)


def farm_candidate() -> Tuple[str, str, str, str, Dict[str, str]]:
    """(label, api_key, base_url, model, extra_headers) for OpenAI-SDK callers, pointing at the farm shim."""
    return ("farm", FARM_KEY, FARM_BASE, FARM_MODEL, {})


def vision_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """The web server's vision config, with the paid endpoints replaced by the farm shim while the switch is off.

    The OpenAI-shaped keys stay (the consumers read them), but they now name the shim, a dummy key and the
    fleet's model, and the OpenRouter key is gone. A copy is returned; the file is never rewritten.
    """
    if enabled():
        return cfg
    out = dict(cfg or {})
    # The OpenRouter key survives under a name only the :free rig-type check reads (it enforces :free ids).
    if out.get("open_router_api_key"):
        out["open_router_free_key"] = out["open_router_api_key"]
    for name in _VISION_KEYS_TO_DROP:
        out.pop(name, None)
    out["open_AI_api_key"] = FARM_KEY
    out["open_ai_api_url_string"] = FARM_CHAT_URL
    out["open_ai_models_url_string"] = FARM_MODELS_URL
    for name in _MODEL_KEYS:
        out[name] = FARM_MODEL
    out["paid_llm_off_bool"] = True
    return out


# ----------------------------------------------------------------------------------- OpenRouter, free models only
OPENROUTER_BUDGET_FILE = Path(os.getenv("AUTORIG_OPENROUTER_BUDGET_FILE", "/var/autorig/openrouter_free_budget.json"))
OPENROUTER_DAILY_CAP = int(os.getenv("AUTORIG_OPENROUTER_FREE_DAILY", "40") or 40)
_budget_mem: Dict[str, Any] = {"day": "", "n": 0}


def free_only(models: List[str]) -> List[str]:
    """Only OpenRouter ids ending in ``:free``; a paid id never gets through (owner 2026-10-11)."""
    return [m for m in (str(x).strip() for x in models or []) if m.endswith(":free")]


def openrouter_free() -> Optional[Tuple[str, str, List[str]]]:
    """(api_key, completions_url, free_model_ids) from the web server's vision config, or None."""
    path = os.getenv("AUTORIG_VISION_CONFIG", "/srv/autorig/secrets/ai_vision_animal_type_detect.json")
    try:
        cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    key = str(cfg.get("open_router_api_key") or "").strip()
    models = free_only(cfg.get("open_router_free_vision_models_array") or [])
    url = str(cfg.get("open_router_api_url_string") or "https://openrouter.ai/api/v1/chat/completions").strip()
    if not key or not models:
        return None
    return key, url, models


def openrouter_free_take() -> bool:
    """Count one OpenRouter request against today's allowance; False when the allowance is spent."""
    today = time.strftime("%Y-%m-%d", time.gmtime())
    state = dict(_budget_mem)
    try:
        disk = json.loads(OPENROUTER_BUDGET_FILE.read_text(encoding="utf-8"))
        if disk.get("day") == today:
            state = {"day": today, "n": max(int(disk.get("n", 0)), state["n"] if state["day"] == today else 0)}
    except (OSError, ValueError):
        pass
    if state.get("day") != today:
        state = {"day": today, "n": 0}
    if state["n"] >= OPENROUTER_DAILY_CAP:
        _budget_mem.update(state)
        return False
    state["n"] += 1
    _budget_mem.update(state)
    try:
        OPENROUTER_BUDGET_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = OPENROUTER_BUDGET_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        tmp.replace(OPENROUTER_BUDGET_FILE)
    except OSError:
        pass
    return True

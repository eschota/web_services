"""Scheduling tiers for the farm (owner 2026-11-06).

«чёткий балансер, который все задачи раскидает с приоритетом для М+, потом
AutoRig, и только потом Renderfin-генерация картинок/видео».

One table for every place that ranks work: the Renderfin queue (dispatch
order, held boxes, preemption), the Vision/Text node picker in ai_vision_api,
and the workload class a Renderfin task reports to the central broker.

    tier         rank   broker class
    mplus        0      ai_vision
    autorig      1      autorig_interactive
    generation   2      comfy
    background   3      collection_background

A request is tagged by ``X-Autorig-Priority``. ``autorig`` (and any other
plain label) is trusted only from inside the host, as since 2026-10-15: no
``X-Real-IP`` / ``X-Forwarded-For``, so a visitor's browser cannot claim it.
``mplus`` comes from M+ servers on the internet, so it needs a shared secret:
``X-Priority-Key`` must equal one of the comma-separated keys in the env
``RENDERFIN_PRIORITY_KEYS``. Key values are never logged.

``RENDERFIN_TIER_PRIORITY=0`` is the rollback: every caller then runs its
2026-10-15 code path (AutoRig first, nothing else ranked).
"""
from __future__ import annotations

import hmac
import logging
import os
import re
import time
from typing import Any, Dict, Mapping, Optional

from . import config

logger = logging.getLogger(__name__)

TIER_MPLUS = "mplus"
TIER_AUTORIG = "autorig"
TIER_GENERATION = "generation"
TIER_BACKGROUND = "background"

TIER_RANK: Dict[str, int] = {
    TIER_MPLUS: 0,
    TIER_AUTORIG: 1,
    TIER_GENERATION: 2,
    TIER_BACKGROUND: 3,
}
TIERS = tuple(sorted(TIER_RANK, key=TIER_RANK.get))

TIER_WORKLOAD_CLASS: Dict[str, str] = {
    TIER_MPLUS: "ai_vision",
    TIER_AUTORIG: "autorig_interactive",
    TIER_GENERATION: "comfy",
    TIER_BACKGROUND: "collection_background",
}

# The broker lets the Renderfin principal acquire only these classes
# (workload_broker._principal_allows); anything else would be refused with a
# 403 that the queue counts as a submit failure.
BROKER_RENDERFIN_CLASSES = frozenset({"comfy", "hunyuan", "collection_background"})

PRIORITY_KEYS_ENV = "RENDERFIN_PRIORITY_KEYS"
# A key shorter than this is ignored: an empty or toy key must never open M+.
MIN_KEY_CHARS = 16

_CLASS = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
_REJECT_LOG_INTERVAL = 300.0
_last_reject_log = 0.0


def enabled() -> bool:
    return bool(config.TIER_PRIORITY)


def rank(tier: str) -> int:
    return TIER_RANK.get(str(tier or ""), TIER_RANK[TIER_GENERATION])


def workload_class(tier: str) -> str:
    return TIER_WORKLOAD_CLASS.get(str(tier or ""), "comfy")


def broker_lease_class(workload_class_name: str) -> str:
    """The class Renderfin may actually ask the broker for.

    The tier's own class still orders boxes inside Renderfin (server_role_rank);
    the broker only ever sees comfy / hunyuan / collection_background until
    its principal table allows more.
    """
    value = str(workload_class_name or "comfy")
    return value if value in BROKER_RENDERFIN_CLASSES else "comfy"


# ---------- tasks ----------

def legacy_is_autorig(task: Any) -> bool:
    """The 2026-10-15 rule, unchanged: an AutoRig task is marked by the farm
    client or comes under an AutoRig user name; collection bulk never is."""
    if not config.AUTORIG_PRIORITY:
        return False
    if str(getattr(task, "queue_class", "") or "") == "collection_background":
        return False
    prompt = getattr(task, "prompt", None)
    if str(getattr(prompt, "priority_class", "") or "").strip().lower() == "autorig":
        return True
    return str(getattr(prompt, "user_name", "") or "").strip() in config.AUTORIG_USER_NAMES


def task_tier(task: Any) -> str:
    """The tier of a queued render task (RenderTask or anything shaped like it)."""
    if str(getattr(task, "queue_class", "") or "") == "collection_background":
        return TIER_BACKGROUND
    if not enabled():
        return TIER_AUTORIG if legacy_is_autorig(task) else TIER_GENERATION
    prompt = getattr(task, "prompt", None)
    klass = str(getattr(prompt, "priority_class", "") or "").strip().lower()
    if klass == TIER_MPLUS:
        return TIER_MPLUS
    if legacy_is_autorig(task):
        return TIER_AUTORIG
    return TIER_GENERATION


def tier_boxes(tier: str) -> frozenset:
    """Renderfin Comfy boxes held for a tier (RENDERFIN_TIER_BOXES)."""
    configured = config.TIER_BOXES.get(tier)
    if configured is not None:
        return frozenset(configured)
    if tier == TIER_AUTORIG:
        return frozenset(config.AUTORIG_PRIORITY_BOXES)
    return frozenset()


def vision_boxes(tier: str) -> frozenset:
    """Vision/Text nodes pinned for a tier (RENDERFIN_TIER_VISION_BOXES), lower-case."""
    return frozenset(str(b).strip().lower() for b in config.TIER_VISION_BOXES.get(tier, ()) if str(b).strip())


# ---------- requests ----------

def _header(headers: Mapping[str, Any], name: str) -> str:
    try:
        value = headers.get(name)
        if value is None:
            value = headers.get(name.title())
    except Exception:
        value = None
    return str(value or "").strip()


def _forwarded(headers: Mapping[str, Any]) -> bool:
    return bool(_header(headers, "x-real-ip") or _header(headers, "x-forwarded-for"))


def _configured_keys() -> list:
    raw = os.getenv(PRIORITY_KEYS_ENV, "")
    return [k.strip() for k in raw.split(",") if len(k.strip()) >= MIN_KEY_CHARS]


def priority_key_valid(presented: str) -> bool:
    """Constant-time check of X-Priority-Key against RENDERFIN_PRIORITY_KEYS."""
    presented = str(presented or "").strip()
    if len(presented) < MIN_KEY_CHARS:
        return False
    ok = False
    for key in _configured_keys():
        # Every key is compared, so timing does not say which one matched.
        if hmac.compare_digest(presented.encode("utf-8"), key.encode("utf-8")):
            ok = True
    return ok


def _note_rejected(reason: str) -> None:
    global _last_reject_log
    now = time.monotonic()
    if now - _last_reject_log >= _REJECT_LOG_INTERVAL:
        _last_reject_log = now
        logger.warning("X-Autorig-Priority: mplus refused (%s); the request runs untagged", reason)


def priority_class_from_headers(headers: Mapping[str, Any]) -> str:
    """The priority_class a request may carry, '' when it gets none.

    Tier rules (RENDERFIN_TIER_PRIORITY on):
    * mplus      - only with a valid X-Priority-Key, wherever it comes from;
    * autorig    - only from inside the host (no proxy headers), as before;
    * other label (red_autopost, ...) - inside the host only, a plain label.
    With the switch off this is the 2026-10-16 rule verbatim.
    """
    prio = _header(headers, "x-autorig-priority").lower()
    if not prio:
        return ""
    internal = not _forwarded(headers)
    if prio == TIER_MPLUS and enabled():
        if priority_key_valid(_header(headers, "x-priority-key")):
            return TIER_MPLUS
        _note_rejected("no valid X-Priority-Key")
        return ""
    if prio == "autorig":
        return "autorig" if internal else ""
    if _CLASS.match(prio) and internal:
        return prio
    return ""


def request_tier(headers: Mapping[str, Any]) -> str:
    """The tier of an incoming Vision/Text request."""
    if not enabled():
        return TIER_GENERATION
    klass = priority_class_from_headers(headers)
    if klass == TIER_MPLUS:
        return TIER_MPLUS
    if klass == "autorig" and config.AUTORIG_PRIORITY:
        return TIER_AUTORIG
    return TIER_GENERATION


def sanitize_priority_class(value: str, headers: Mapping[str, Any]) -> str:
    """For a body that names its own priority_class (POST /renderfin/api-render):
    a forwarded request may not claim mplus without the key."""
    klass = str(value or "").strip().lower()
    if klass == TIER_MPLUS and enabled() and _forwarded(headers):
        if not priority_key_valid(_header(headers, "x-priority-key")):
            _note_rejected("priority_class=mplus in a forwarded render body")
            return ""
    return str(value or "")


def describe() -> Dict[str, Any]:
    """Non-secret configuration summary (fleet / health)."""
    return {
        "enabled_bool": enabled(),
        "tiers_array": list(TIERS),
        "priority_keys_configured_int": len(_configured_keys()),
        "tier_boxes_object": {t: sorted(tier_boxes(t)) for t in TIERS if tier_boxes(t)},
        "vision_boxes_object": {t: sorted(vision_boxes(t)) for t in TIERS if vision_boxes(t)},
        "vision_warm_minutes_float": float(config.VISION_WARM_MINUTES or 0),
    }


def tier_of_prompt_class(priority_class: Optional[str]) -> str:
    klass = str(priority_class or "").strip().lower()
    if klass == TIER_MPLUS:
        return TIER_MPLUS
    if klass == "autorig":
        return TIER_AUTORIG
    return TIER_GENERATION

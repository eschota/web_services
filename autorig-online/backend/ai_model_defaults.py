"""Resolve catalogue-backed render defaults for a selected checkpoint/LoRA."""
from __future__ import annotations

from typing import Dict, Iterable, Mapping, Optional, Tuple


SAMPLER_ALIASES = {
    "euler": "euler",
    "euler a": "euler_ancestral",
    "euler ancestral": "euler_ancestral",
    "dpm++ 2m": "dpmpp_2m",
    "dpm++ 2m sde": "dpmpp_2m_sde",
    "dpmpp_2m": "dpmpp_2m",
    "dpmpp_2m_sde": "dpmpp_2m_sde",
    "euler_ancestral": "euler_ancestral",
    "euler_ancestral_cfg_pp": "euler_ancestral_cfg_pp",
}
SCHEDULER_ALIASES = {
    "karras": "karras",
    "normal": "normal",
    "simple": "simple",
}
MODEL_FILE_ALIASES = {
    "ltx10eros_v14_2989633.safetensors": "ltx10eros_v14_2989669.safetensors",
    "cyberrealisticPony_v180Coreshift_2764472.safetensors":
        "CyberRealisticPony_V18.0_F16.safetensors",
}
FAMILY_WORKFLOWS = {
    "pony": "gen_image_sdxl.json",
    "sdxl": "gen_image_sdxl.json",
    "flux": "gen_image.json",
    "flux2": "gen_image_flux2_klein.json",
}


def canonical_file(name: object) -> str:
    value = str(name or "").strip()
    return MODEL_FILE_ALIASES.get(value, value)


def control_workflow(family: str, channel: str) -> str:
    family = str(family or "").strip().lower()
    channel = str(channel or "").strip().lower()
    if channel not in {"pose", "depth", "canny"}:
        raise ValueError("unsupported ControlNet channel")
    if family in {"pony", "sdxl"}:
        return f"gen_image_sdxl_control_{channel}.json"
    if family == "flux":
        return f"gen_image_control_{channel}.json"
    raise ValueError("ControlNet generation requires a Pony/SDXL or Flux.1 model")


def model_family(entry: Optional[Mapping[str, object]]) -> str:
    if not entry:
        return ""
    family = str(entry.get("family") or "").strip().lower()
    base = str(entry.get("base") or "").strip().lower()
    if "ltxv 2.3" in base or "ltx 2.3" in base:
        return "ltx23"
    if "ltx" in base:
        return "ltx"
    if "flux.2" in base or "flux 2" in base:
        return "flux2"
    if "flux.1" in base or "flux 1" in base:
        return "flux"
    if family:
        return family
    if "pony" in base:
        return "pony"
    if "sdxl" in base or "stable diffusion xl" in base:
        return "sdxl"
    return ""


def compatible(checkpoint: Optional[Mapping[str, object]],
               lora: Optional[Mapping[str, object]]) -> bool:
    """Whether the selected files share a model architecture.

    Pony is an SDXL derivative, so Pony and SDXL LoRAs are compatible. Other
    families must match exactly; an omitted checkpoint means the workflow's
    own default and is validated by the service catalogue instead.
    """
    left, right = model_family(checkpoint), model_family(lora)
    if not left or not right:
        return True
    if {left, right} <= {"pony", "sdxl"}:
        return True
    return left == right


def family_default_checkpoint(
    entries: Iterable[Mapping[str, object]],
    lora: Optional[Mapping[str, object]],
    service: str,
) -> Optional[Mapping[str, object]]:
    """Return an explicitly declared base for a LoRA family.

    A catalogue's first checkpoint is a display-order choice, not a model
    compatibility rule. Only an entry marked for this family may resolve a
    missing checkpoint, which keeps worker-specific workflow defaults from
    silently changing the model.
    """
    family = model_family(lora)
    service = str(service or "").strip().lower()
    if not family or not service:
        return None
    for entry in entries:
        if (
            str(entry.get("kind") or "").strip().lower() == "checkpoint"
            and bool(entry.get("usable"))
            and service in (entry.get("services") or [])
            and family in (entry.get("default_for_families") or [])
            and compatible(entry, lora)
        ):
            return entry
    return None


def _normalise_recommended(raw: object) -> Dict[str, object]:
    if not isinstance(raw, Mapping):
        return {}
    out: Dict[str, object] = {}
    for key in ("steps", "cfg", "strength", "clip_skip"):
        value = raw.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            out[key] = value
    sampler = str(raw.get("sampler") or "").strip().lower()
    scheduler = str(raw.get("scheduler") or "").strip().lower()
    if sampler in SAMPLER_ALIASES:
        out["sampler"] = SAMPLER_ALIASES[sampler]
    if scheduler in SCHEDULER_ALIASES:
        out["scheduler"] = SCHEDULER_ALIASES[scheduler]
    return out


def resolve(checkpoint: Optional[Mapping[str, object]],
            lora: Optional[Mapping[str, object]],
            explicit: Mapping[str, object]) -> Dict[str, object]:
    """Merge author-backed recommendations, then caller overrides.

    Width and height deliberately stay outside this resolver: the product's
    default output is 960x540, while a caller may still explicitly request a
    different size. Example-image dimensions are provenance, not a universal
    author recommendation for every render.
    """
    if not compatible(checkpoint, lora):
        raise ValueError("checkpoint and LoRA use different model families")
    effective: Dict[str, object] = {}
    for entry in (checkpoint, lora):
        if entry and entry.get("recommended_from"):
            recommended = _normalise_recommended(entry.get("recommended"))
            if (str(entry.get("kind") or "").strip().lower() == "lora"
                    and entry.get("sampling_recommendations_compatible") is False):
                # The LoRA page's sampler/steps describe its training base
                # (Flux Dev for our current Flux.1 adapters), not the installed
                # four-step Schnell base. Strength still belongs to the LoRA.
                recommended = {key: value for key, value in recommended.items()
                               if key == "strength"}
            effective.update(recommended)
    for key, value in explicit.items():
        if value is not None and value != "":
            effective[key] = value
    explicit_sampler = str(explicit.get("sampler") or "").strip().lower()
    explicit_scheduler = str(explicit.get("scheduler") or "").strip().lower()
    if explicit_sampler in SAMPLER_ALIASES:
        effective["sampler"] = SAMPLER_ALIASES[explicit_sampler]
    if explicit_scheduler in SCHEDULER_ALIASES:
        effective["scheduler"] = SCHEDULER_ALIASES[explicit_scheduler]
    if lora and "lora_strength" not in effective and "strength" in effective:
        effective["lora_strength"] = effective.pop("strength")
    else:
        effective.pop("strength", None)
    if lora and not effective.get("lora_strength"):
        # The existing UI uses zero for "automatic", not for disabling the
        # selected adapter. A selected LoRA must never silently run at zero.
        effective["lora_strength"] = 1.0
    workflow = str((checkpoint or {}).get("workflow") or (lora or {}).get("workflow") or "").strip()
    family = model_family(checkpoint) or model_family(lora)
    if workflow:
        effective["work_flow"] = workflow
    elif family in FAMILY_WORKFLOWS:
        effective["work_flow"] = FAMILY_WORKFLOWS[family]
    return effective


def add_triggers(prompt: str, entries: Iterable[Optional[Mapping[str, object]]]) -> str:
    """Prefix missing catalogue trigger words once, case-insensitively."""
    text = str(prompt or "").strip()
    folded = text.casefold()
    missing = []
    for entry in entries:
        if not entry:
            continue
        for trigger in entry.get("triggers") or []:
            trigger = str(trigger or "").strip()
            if trigger and trigger.casefold() not in folded and trigger not in missing:
                missing.append(trigger)
    return ", ".join(missing + ([text] if text else []))

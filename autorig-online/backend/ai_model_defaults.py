"""Resolve catalogue-backed render defaults for a selected checkpoint/LoRA."""
from __future__ import annotations

from typing import Dict, Iterable, Mapping, Optional, Tuple


SAMPLER_ALIASES = {
    "euler": "euler",
    "euler a": "euler_ancestral",
    "euler ancestral": "euler_ancestral",
    "dpm++ 2m": "dpmpp_2m",
    "dpm++ 2m sde": "dpmpp_2m_sde",
    "dpm++ sde": "dpmpp_sde",
    "dpmpp_sde": "dpmpp_sde",
    "dpmpp_2m": "dpmpp_2m",
    "dpmpp_2m_sde": "dpmpp_2m_sde",
    "euler_ancestral": "euler_ancestral",
    "euler_ancestral_cfg_pp": "euler_ancestral_cfg_pp",
}
SCHEDULER_ALIASES = {
    "karras": "karras",
    "normal": "normal",
    "simple": "simple",
    "sgm uniform": "sgm_uniform",
    "sgm_uniform": "sgm_uniform",
    "beta": "beta",
}
MODEL_FILE_ALIASES = {
    "ltx10eros_v14_2989633.safetensors": "ltx10eros_v14_2989669.safetensors",
    "cyberrealisticPony_v180Coreshift_2764472.safetensors":
        "CyberRealisticPony_V18.0_F16.safetensors",
}
FAMILY_WORKFLOWS = {
    "pony": "gen_image_sdxl.json",
    "sdxl": "gen_image_sdxl.json",
    "illustrious": "gen_image_sdxl.json",
    "noobai": "gen_image_sdxl.json",
    "flux": "gen_image.json",
    "flux2": "gen_image_flux2_klein.json",
}
# Video architectures whose catalogue entry names its own family because the
# base string alone cannot separate them: "LTX-2 19B" and "LTXV 13B 0.9.8" both
# contain "ltx", which would collapse them into each other and into the legacy
# 0.9.1 adapters. Keeping them apart is what stops an LTX 2.3 LoRA from being
# accepted onto a checkpoint that was never trained with it.
# "qwen_image" is declared for the same reason from the other direction: the
# base string of a Qwen-Image checkpoint says nothing the heuristics below
# recognise, and an undeclared family is an empty family, which `compatible`
# reads as "no opinion" and would let a FLUX or LTX LoRA onto a Qwen model.
DECLARED_FAMILIES = frozenset({"ltx2", "ltx098", "qwen_image", "ltx25", "zimage",
                               "flux2_9b", "flux2_dev", "krea2", "minimax_h3", "wan22_i2v_a14b",
                               "wan22_t2v_a14b", "wan22_5b", "wan21_14b", "wan21_1b"})
# Families built on the SDXL UNet and text encoders. Their LoRAs load onto one
# another's checkpoints (Civitai's own generator mixes them); whether a Pony
# LoRA looks right on an Illustrious model is a matter of taste, not of shape.
SDXL_FAMILIES = frozenset({"pony", "sdxl", "illustrious", "noobai"})
# (checkpoint family, LoRA family) pairs that load although they differ.
FORWARD_COMPATIBLE_LORAS = frozenset({("ltx25", "ltx23")})


def canonical_file(name: object) -> str:
    value = str(name or "").strip()
    return MODEL_FILE_ALIASES.get(value, value)


def control_workflow(family: str, channel: str) -> str:
    family = str(family or "").strip().lower()
    channel = str(channel or "").strip().lower()
    if channel not in {"pose", "depth", "canny"}:
        raise ValueError("unsupported ControlNet channel")
    if family in SDXL_FAMILIES:
        return f"gen_image_sdxl_control_{channel}.json"
    if family == "flux":
        return f"gen_image_control_{channel}.json"
    raise ValueError("ControlNet generation requires a Pony/SDXL or Flux.1 model")


def wan_family(base: str) -> str:
    """Wan variants are separate architectures (A14B experts, 5B, 2.1 14B)."""
    b = str(base or "").lower().replace(" ", "")
    if "2.2" in b:
        if "5b" in b:
            return "wan22_5b"
        if "i2v" in b:
            return "wan22_i2v_a14b"
        return "wan22_t2v_a14b"
    if "1.3b" in b:
        return "wan21_1b"
    return "wan21_14b"


def model_family(entry: Optional[Mapping[str, object]]) -> str:
    if not entry:
        return ""
    family = str(entry.get("family") or "").strip().lower()
    base = str(entry.get("base") or "").strip().lower()
    if family in DECLARED_FAMILIES:
        return family
    if "krea 2" in base or "krea2" in base:
        return "krea2"
    if "minimax" in base:
        return "minimax_h3"
    if "z-image" in base or "zimage" in base or "z image" in base:
        return "zimage"
    if "wan" in base and ("2.2" in base or "2.1" in base or "14b" in base or "wan video" in base):
        return wan_family(base)
    if any(tag in base for tag in ("ltx 2.5", "ltxv 2.5", "ltx-2.5", "ltx2.5", "ltxv2.5")):
        return "ltx25"
    if "klein 9b" in base:
        return "flux2_9b"
    if "flux.2 d" in base or "flux 2 dev" in base or "flux.2 dev" in base:
        return "flux2_dev"
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
    if "illustrious" in base:
        return "illustrious"
    if "noobai" in base:
        return "noobai"
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
    if {left, right} <= SDXL_FAMILIES:
        return True
    # Lightricks: LTX-2.5 keeps the 2.3 layout, and 2.3 LoRAs and IC-LoRAs
    # load onto it unchanged (the official 2.5 Union-Control workflow ships
    # the 2.3 adapter). The reverse is not promised, so it stays refused.
    if (left, right) in FORWARD_COMPATIBLE_LORAS:
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
            policy = entry.get("sampling_policy") or {}
            if policy.get("auto_steps") and policy.get("auto_reason"):
                # Product quality policy is separate from the author's exact
                # example. It must never be represented as an author maximum.
                recommended["steps"] = int(policy["auto_steps"])
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
    policy = (checkpoint or {}).get("sampling_policy") or {}
    fixed_steps = policy.get("fixed_steps")
    if fixed_steps and effective.get("steps") != fixed_steps:
        raise ValueError(
            f"This distilled workflow uses a fixed {fixed_steps}-step schedule; "
            "select Auto or that exact number of steps")
    if policy.get("steps_max") and effective.get("steps", 0) > policy["steps_max"]:
        raise ValueError(
            f"This model supports at most {policy['steps_max']} sampling steps; "
            "select Auto or a value within the model's range")
    if policy.get("cfg_mode") == "fixed":
        fixed_cfg = float(policy.get("cfg_value", 1))
        if explicit.get("cfg") not in (None, "", 0, fixed_cfg):
            raise ValueError(
                f"This distilled workflow has fixed CFG {fixed_cfg:g}; "
                "a different CFG would not be applied")
        effective["cfg"] = fixed_cfg
    if policy.get("scheduler_mode") == "native":
        if explicit_scheduler:
            raise ValueError(
                f"This workflow uses {policy.get('scheduler_label', 'its native schedule')}; "
                "choose Automatic instead of a generic scheduler")
        effective.pop("scheduler", None)
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

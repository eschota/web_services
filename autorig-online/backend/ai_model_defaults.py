"""Resolve catalogue-backed render defaults for a selected checkpoint/LoRA."""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Mapping, Optional, Tuple


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
    "cyberrealisticPony_v180Coreshift_2764472.safetensors":
        "CyberRealisticPony_V18.0_F16.safetensors",
}
FAMILY_WORKFLOWS = {
    "pony": "gen_image_sdxl.json",
    "sdxl": "gen_image_sdxl.json",
    "illustrious": "gen_image_sdxl.json",
    "noobai": "gen_image_sdxl.json",
    "flux2": "gen_image_flux2_klein.json",
    # Z-Image Turbo replaced FLUX.1 Schnell behind the same farm token, so
    # every saved graph and public request that named gen_image.json keeps
    # working. Krea 2 is the quality tier with its own token.
    "zimage": "gen_image.json",
    "krea2": "gen_image_krea2.json",
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
# LTX-2.5 LoRAs on LTX-10Eros (an LTX 2.3 model, the farm's only LTX since
# 2026-09-29): same DiT layout, every key of the BTS Movie Set LoRA loaded
# ("lora key not loaded": 0) and the clip rendered cleanly on worker-4090.
FORWARD_COMPATIBLE_LORAS = frozenset({("ltx25", "ltx23"), ("ltx23", "ltx25")})


# Video checkpoints retired by the model swaps of 2026-09-23 (every older LTX
# -> LTX-2.5) and 2026-09-29 (owner: LTX-2.5 -> LTX-10Eros v1.4, MiniMax H3 ->
# H3 Eros Max). A saved node or caller that still names one renders on the
# current model of its line. Kept out of MODEL_FILE_ALIASES on purpose:
# renderfin treats those as byte-identical files and would load a retired
# file that is still on a box's disk.
LTX10EROS_CHECKPOINT = "ltx10eros_v14_2989669.safetensors"
H3_EROS_MAX_CHECKPOINT = "h3ErosMax_beta5_3185144.safetensors"
# Retired 2026-09-29; the name stays importable for callers that pin it.
LTX25_CHECKPOINT = "ltx-2.5-22b-distilled-transformer-comfy-int8-convrot.safetensors"
RETIRED_MODEL_REPLACEMENTS = {
    LTX25_CHECKPOINT: LTX10EROS_CHECKPOINT,
    "ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors": LTX10EROS_CHECKPOINT,
    "ltx-2.3-22b-distilled-fp8.safetensors": LTX10EROS_CHECKPOINT,
    # The 1.6 GB Civitai "Other" file of 10Eros v1.4 is not the model (and not
    # the same bytes, so it must not be a MODEL_FILE_ALIASES entry).
    "ltx10eros_v14_2989633.safetensors": LTX10EROS_CHECKPOINT,
    "ltx-2-19b-distilled-fp8.safetensors": LTX10EROS_CHECKPOINT,
    "ltxv-13b-0.9.8-distilled-fp8.safetensors": LTX10EROS_CHECKPOINT,
    "ltxv-13b-0.9.8-distilled.safetensors": LTX10EROS_CHECKPOINT,
    "ltxv-13b-0.9.8-dev-fp8.safetensors": LTX10EROS_CHECKPOINT,
    # MiniMax H3 base builds (pruned int8, the unpruned int8 before it, and the
    # INT8/INT4 community build) render on H3 Eros Max.
    "minimax_h3_fl2va_pruned_int8_convrot.safetensors": H3_EROS_MAX_CHECKPOINT,
    "minimax_h3_fl2va_int8_convrot.safetensors": H3_EROS_MAX_CHECKPOINT,
    "minimaxH3INT8INT4_flREF2VAPruned_3091888.safetensors": H3_EROS_MAX_CHECKPOINT,
}


def canonical_file(name: object) -> str:
    value = str(name or "").strip()
    value = RETIRED_MODEL_REPLACEMENTS.get(value, value)
    return MODEL_FILE_ALIASES.get(value, value)


def control_workflow(family: str, channel: str) -> str:
    family = str(family or "").strip().lower()
    channel = str(channel or "").strip().lower()
    if channel not in {"pose", "depth", "canny"}:
        raise ValueError("unsupported ControlNet channel")
    if family in SDXL_FAMILIES:
        return f"gen_image_sdxl_control_{channel}.json"
    if family == "zimage":
        return f"gen_image_control_{channel}.json"
    raise ValueError("ControlNet generation requires a Pony/SDXL or Z-Image model")


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


def qwen_line(entry: Optional[Mapping[str, object]]) -> str:
    """Which Qwen-Image line an entry belongs to: "2.1" or "classic".

    Qwen-Image 2.1 is a new architecture; LoRAs for the original Qwen-Image,
    Qwen-Image-Edit (2509/2511) and 2512 do not load onto it, nor the reverse
    (owner, 2026-09-28).
    """
    if not entry:
        return ""
    if str(entry.get("qwen_image_generation") or "").strip() == "2.1":
        return "2.1"
    text = (str(entry.get("base") or "") + " " + str(entry.get("title") or "")).lower()
    compact = text.replace(" ", "").replace("-", "").replace("_", "")
    if "qwenimage2.1" in compact or "qwen2.1" in compact or "qwenimage21" in compact:
        return "2.1"
    return "classic"


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
    # the 2.3 adapter). The reverse (2.5 LoRAs on the 2.3-based 10Eros) was
    # measured on 2026-09-29: every key loads.
    if (left, right) in FORWARD_COMPATIBLE_LORAS:
        return True
    if left == right == "qwen_image":
        return qwen_line(checkpoint) == qwen_line(lora)
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


# Trigger words. A word from Civitai's own trained-words list is a token, and a
# longer entry there is a note, hence 60 characters. A trigger an author wrote
# in the description, or an administrator set, may be a whole sentence ("a man
# and woman are having sex in the prone bone position.", owner 2026-09-29).
TRIGGER_MAX_CHARS = 60
SENTENCE_TRIGGER_MAX_CHARS = 200


def primary_trigger(words: Iterable[object], max_chars: int = TRIGGER_MAX_CHARS) -> str:
    """The first usable trigger word of a LoRA, or "".

    Placeholders authors put in the trained-words list ("see LoRA
    description", "head_swap: FACE: [Face Description]") are not words to
    prepend, so they are skipped.
    """
    for word in words or []:
        text = str(word or "").strip()
        low = text.casefold()
        if (not text or len(text) > max_chars or any(ch in text for ch in "[]{}<>:")
                or low.startswith("see ") or "description" in low):
            continue
        return text
    return ""


def normalise_phrase(text: object) -> str:
    """A trigger as it is compared: lower case, punctuation and spacing ignored."""
    return " ".join(re.sub(r"[\W_]+", " ", str(text or "").casefold()).split())


def _holds(haystack: str, needle: str) -> bool:
    """Whether a normalised phrase occurs in a normalised text, as whole words."""
    return bool(needle) and f" {needle} " in f" {haystack} "


def is_sentence_trigger(text: object) -> bool:
    """A trigger written as a sentence stands in the prompt as its own sentence."""
    return str(text or "").rstrip().endswith((".", "!", "?", "。", "…"))


def split_trigger_text(text: object) -> List[str]:
    """A node's `lora_trigger_text`: phrases one per line or separated by "|"."""
    return [part.strip() for part in re.split(r"[\r\n|]+", str(text or "")) if part.strip()]


def join_prompt(pieces: Iterable[str], prompt: str) -> str:
    """Triggers in front of the prompt: after a sentence a space, after a token ", "."""
    out = ""
    for piece in [piece for piece in pieces if piece] + ([prompt] if prompt else []):
        if not out:
            out = piece
        elif is_sentence_trigger(out):
            out += " " + piece
        else:
            out += ", " + piece
    return out


def apply_triggers(prompt: str, entries: Iterable[Optional[Mapping[str, object]]],
                   extra: Iterable[str] = ()) -> Tuple[str, List[str]]:
    """(the prompt with triggers in front, the triggers that were put there).

    Owner rules 2026-09-28 / 2026-09-29: each selected entry's primary trigger
    word, in order, then the node's own extra phrases verbatim, all in front of
    the prompt once the prompt edit and translation are done. A phrase the
    prompt already holds, or one already put there, is not added again; the
    comparison ignores case, punctuation and spacing, so a prompt with "...in
    the prone bone position" already holds "...in the prone bone position.".
    """
    text = str(prompt or "").strip()
    held = normalise_phrase(text)
    added: List[str] = []
    keys: List[str] = []

    def consider(phrase: str) -> None:
        key = normalise_phrase(phrase)
        if not key or _holds(held, key) or any(_holds(other, key) for other in keys):
            return
        added.append(phrase)
        keys.append(key)

    for entry in entries:
        if entry:
            consider(primary_trigger(entry.get("triggers") or [], SENTENCE_TRIGGER_MAX_CHARS))
    for phrase in extra or ():
        consider(str(phrase or "").strip())
    return join_prompt(added, text), added


def add_triggers(prompt: str, entries: Iterable[Optional[Mapping[str, object]]]) -> str:
    """Each entry's primary trigger word in front of the prompt, once (apply_triggers)."""
    return apply_triggers(prompt, entries)[0]

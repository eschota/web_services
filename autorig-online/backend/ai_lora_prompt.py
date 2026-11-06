"""LoRA stacks and Civitai/A1111-style `<lora:NAME:WEIGHT>` prompt tags.

A LoRA can reach a render three ways, and all three end up in one ordered
stack of `{file, strength_model, strength_clip}`:

1. the node's single `lora` / `lora_strength` fields (the original contract,
   left exactly as it was so saved graphs render the same picture);
2. the node's `loras` stack: a list of `{name, strength, strength_clip}` from
   the API, or a string of tags (`<lora:a:0.8> <lora:b:0.6>`) from the node UI;
3. tags written into the positive prompt, the way Civitai and A1111 users write
   them.

Precedence, highest first: a prompt tag, then the node stack, then the single
`lora` field. A tag that names a LoRA already in the stack replaces its
strength in place; a tag that names the single `lora` replaces `lora_strength`
and does not add a second copy. The stack is applied in order, first entry
nearest the checkpoint: node stack entries first, then new prompt tags in the
order they appear.

Tags are removed from the text before it reaches the text encoder. Removing a
tag also removes the separator it leaves behind (`a, <lora:x>, b` becomes
`a, b`), so a prompt with tags and the same prompt written without them encode
to the same conditioning. Nothing else in the prompt is touched.

Tag grammar (A1111's, which Civitai copies):

    <lora:NAME>                  both strengths 1.0
    <lora:NAME:W>                both strengths W
    <lora:NAME:TE:UNET>          text-encoder strength TE, model strength UNET
    <lora:NAME:te=0.5:unet=1>    the same, named
    <lyco:...> / <locon:...>     aliases of <lora:...>

A name is resolved against the LoRA catalogue by file name, file stem, an alias
(the Civitai model / version name) or the catalogue title, case-insensitively.
A name that matches nothing, or more than one LoRA, is an error: a tag that
silently rendered without its LoRA would be the worst possible outcome.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

MAX_STACK = 8
MIN_STRENGTH = -4.0
MAX_STRENGTH = 4.0

TAG_RE = re.compile(r"<\s*(lora|lyco|locon)\s*:\s*([^<>]*?)\s*>", re.IGNORECASE)
_NUMBER_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)$")
# Placeholder used while tags are cut out, chosen so it cannot occur in a prompt.
_HOLE = "\u0000"


class LoraSyntaxError(ValueError):
    """A tag or stack entry that cannot be read."""


class LoraResolutionError(ValueError):
    """A LoRA name that names nothing, or more than one thing, in the catalogue."""

    def __init__(self, message: str, *, name: str = "", suggestions: Sequence[str] = ()):
        super().__init__(message)
        self.name = name
        self.suggestions = list(suggestions)


@dataclass
class LoraRef:
    """One requested LoRA before it is resolved to a catalogue file."""

    name: str
    strength_model: float = 1.0
    strength_clip: float = 1.0
    source: str = "stack"  # "stack" or "prompt"


@dataclass
class ResolvedLora:
    file: str
    strength_model: float
    strength_clip: float
    entry: Mapping[str, object] = field(default_factory=dict)
    source: str = "stack"

    def as_payload(self) -> dict:
        return {"name": self.file, "strength_model": self.strength_model,
                "strength_clip": self.strength_clip}


def _strength(raw: str, what: str) -> float:
    text = str(raw).strip()
    if not _NUMBER_RE.match(text):
        raise LoraSyntaxError(f"'{raw}' is not a number ({what})")
    value = float(text)
    if not MIN_STRENGTH <= value <= MAX_STRENGTH:
        raise LoraSyntaxError(
            f"{what} {value:g} is outside {MIN_STRENGTH:g}..{MAX_STRENGTH:g}")
    return value


def _parse_tag_body(body: str) -> LoraRef:
    parts = [part.strip() for part in body.split(":")]
    name = parts[0] if parts else ""
    if not name:
        raise LoraSyntaxError("a <lora:...> tag has no name")
    te: Optional[float] = None
    unet: Optional[float] = None
    positional: List[str] = []
    for part in parts[1:]:
        if not part:
            continue
        if "=" in part:
            key, _, value = part.partition("=")
            key = key.strip().lower()
            if key == "te":
                te = _strength(value, "text-encoder strength")
            elif key == "unet":
                unet = _strength(value, "model strength")
            elif key == "dyn":
                continue  # A1111 dynamic-rank hint; no ComfyUI equivalent.
            else:
                raise LoraSyntaxError(f"unknown <lora> argument '{key}'")
        else:
            positional.append(part)
    if len(positional) > 3:
        raise LoraSyntaxError(f"<lora:{name}> has too many values")
    if positional:
        te = _strength(positional[0], "strength") if te is None else te
    if len(positional) >= 2:
        unet = _strength(positional[1], "model strength") if unet is None else unet
    if te is None and unet is None:
        te = unet = 1.0
    elif te is None:
        te = unet
    elif unet is None:
        unet = te
    return LoraRef(name=name, strength_model=float(unet), strength_clip=float(te),
                   source="prompt")


def parse_prompt(text: str) -> Tuple[str, List[LoraRef]]:
    """Cut `<lora:...>` tags out of a prompt; return (clean text, tags in order)."""
    text = str(text or "")
    refs: List[LoraRef] = []

    def take(match: "re.Match[str]") -> str:
        refs.append(_parse_tag_body(match.group(2)))
        return _HOLE

    holed = TAG_RE.sub(take, text)
    if not refs:
        return text, []
    lines: List[str] = []
    for line in holed.split("\n"):
        if _HOLE not in line:
            lines.append(line)
            continue
        # Collapse each run of holes (and the blanks between them) into one.
        line = re.sub(_HOLE + r"(?:[ \t,]*" + _HOLE + r")+", _HOLE, line)
        # A hole followed by a separator goes with it: "a, <x>, b" -> "a, b",
        # "<x>, a" -> "a".
        line = re.sub(r"[ \t]*" + _HOLE + r"[ \t]*,", "", line)
        # A hole after a separator at the end of the line: "a, <x>" -> "a".
        line = re.sub(r"[ \t]*,[ \t]*" + _HOLE + r"[ \t]*$", "", line)
        # Anywhere else the hole and the blanks around it become one space.
        line = re.sub(r"[ \t]*" + _HOLE + r"[ \t]*", " ", line).strip(" \t")
        if line:
            lines.append(line)
    return "\n".join(lines).strip(), refs


def parse_stack(value: object) -> List[LoraRef]:
    """A node's `loras` value: a tag string, or a list of dicts / tag strings."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        leftover, refs = parse_prompt(value)
        if leftover.strip(" \t\n,"):
            raise LoraSyntaxError(
                "the LoRA stack holds text that is not a <lora:NAME:WEIGHT> tag: "
                f"'{leftover[:80]}'")
        for ref in refs:
            ref.source = "stack"
        return refs
    if not isinstance(value, (list, tuple)):
        raise LoraSyntaxError("loras must be a list or a string of <lora:...> tags")
    refs: List[LoraRef] = []
    for item in value:
        if isinstance(item, str):
            refs.extend(parse_stack(item))
            continue
        if not isinstance(item, Mapping):
            raise LoraSyntaxError("each loras entry must be an object or a tag")
        name = str(item.get("name") or item.get("file") or item.get("lora") or "").strip()
        if not name:
            raise LoraSyntaxError("a loras entry has no name")
        raw_model = item.get("strength_model", item.get("strength", 1.0))
        model = _strength(str(1.0 if raw_model is None else raw_model), "strength")
        raw_clip = item.get("strength_clip")
        clip = model if raw_clip is None or raw_clip == "" else _strength(str(raw_clip), "clip strength")
        refs.append(LoraRef(name=name, strength_model=model, strength_clip=clip))
    return refs


def _norm(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).casefold()


def _stem(file_name: str) -> str:
    base = file_name.replace("\\", "/").rsplit("/", 1)[-1]
    return base.rsplit(".", 1)[0] if "." in base else base


def _keys(entry: Mapping[str, object]) -> List[str]:
    file_name = str(entry.get("file") or "")
    keys = [file_name, _stem(file_name), str(entry.get("title") or "")]
    keys.extend(str(alias) for alias in (entry.get("aliases") or []))
    return [_norm(key) for key in keys if str(key or "").strip()]


def resolve_name(name: str, loras: Iterable[Mapping[str, object]]) -> Mapping[str, object]:
    """The one catalogue LoRA a user-supplied name means."""
    wanted = _norm(name)
    entries = [entry for entry in loras if entry.get("kind", "lora") == "lora"]
    # Exact file name wins outright, so a file called like another LoRA's
    # title cannot become ambiguous.
    exact = [entry for entry in entries if _norm(entry.get("file")) == wanted]
    if len(exact) == 1:
        return exact[0]
    matches = [entry for entry in entries if wanted in _keys(entry)]
    unique = {str(entry.get("file")): entry for entry in matches}
    if len(unique) == 1:
        return next(iter(unique.values()))
    if len(unique) > 1:
        raise LoraResolutionError(
            f"LoRA '{name}' is ambiguous: {', '.join(sorted(unique))}",
            name=name, suggestions=sorted(unique))
    pool = sorted({_stem(str(entry.get("file") or "")) for entry in entries})
    close = difflib.get_close_matches(name, pool, n=3, cutoff=0.5)
    hint = f"; did you mean {', '.join(close)}?" if close else ""
    raise LoraResolutionError(
        f"No LoRA called '{name}' is installed{hint} Add it at /lora first.",
        name=name, suggestions=close)


def build_stack(
    *,
    stack_refs: Sequence[LoraRef],
    prompt_refs: Sequence[LoraRef],
    loras: Sequence[Mapping[str, object]],
    single_lora: str = "",
) -> Tuple[List[ResolvedLora], Optional[float]]:
    """Resolve and merge; returns (stack, override for the single lora's strength).

    Entries whose both strengths are zero are dropped after validation: ComfyUI
    and A1111 both treat such a LoRA as absent, and keeping it would only make
    the dispatcher demand a file that changes nothing.
    """
    ordered: "dict[str, ResolvedLora]" = {}
    single_override: Optional[float] = None
    single = str(single_lora or "").strip()
    for ref in list(stack_refs) + list(prompt_refs):
        entry = resolve_name(ref.name, loras)
        file_name = str(entry.get("file"))
        if single and file_name == single:
            # The single-LoRA slot keeps its own loader; a tag only restates
            # how strong it is.
            single_override = ref.strength_model
            continue
        if file_name in ordered:
            if ref.source == "prompt":
                ordered[file_name].strength_model = ref.strength_model
                ordered[file_name].strength_clip = ref.strength_clip
                ordered[file_name].source = "prompt"
            continue
        ordered[file_name] = ResolvedLora(
            file=file_name, strength_model=ref.strength_model,
            strength_clip=ref.strength_clip, entry=entry, source=ref.source)
    stack = [item for item in ordered.values()
             if item.strength_model != 0 or item.strength_clip != 0]
    if len(stack) > MAX_STACK:
        raise LoraSyntaxError(f"at most {MAX_STACK} LoRAs can be stacked; got {len(stack)}")
    return stack, single_override


def to_tags(stack: Iterable[Mapping[str, object]]) -> str:
    """The canonical tag string for a stack (what the node UI stores)."""
    out = []
    for item in stack:
        name = _stem(str(item.get("name") or item.get("file") or ""))
        model = float(item.get("strength_model", item.get("strength", 1.0)))
        clip = float(item.get("strength_clip", model))
        if clip == model:
            out.append(f"<lora:{name}:{model:g}>")
        else:
            out.append(f"<lora:{name}:{clip:g}:{model:g}>")
    return " ".join(out)

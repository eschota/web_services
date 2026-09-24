"""Qwen-Image-Edit instructions for re-posing an existing AutoRig character.

A regen starts from a still of the task's own model, so these are EDIT
instructions: they say what to change and what to keep, never who the
character is - the picture carries that. Like the Flux prompts, they feed an
image-to-3D reconstructor, so the rules in
.claude/skills/renderfin-pipeline/references/prompts.md apply: no sheet or
turnaround wording, no "silhouette", no white or studio background, no even or
flat lighting, never "stretched" arms, no quality boosters. The workflow has no
negative prompt, so every exclusion is written as a positive statement.
templating strips the words "glass"/"glasses" from every prompt; write
"spectacles" or "see-through" instead.

Two variants of the same character, rendered together so the operator picks
one, the same way the Flux flow offers two styles:
  a - a faithful T-pose: only the pose changes
  b - the same T-pose, cleaned up for 3D: hair in a few solid locks, crisp
      clothing, no props and nothing see-through or fuzzy
"""
from __future__ import annotations

from typing import Tuple

# Load-bearing clauses, each a measured 3D failure: open space between the arms
# and the torso and between the legs is the main defence against limbs fusing
# to the body in the mesh; a margin past the fingertips and feet keeps a hand
# from being clipped into a stump; a plain mid-grey backdrop gives the matting
# stage contrast without the floor sweep and contact shadow a "studio" pulls.
_POSE = (
    "Pose: a strict T-pose, both arms held straight out to the sides at shoulder "
    "height, palms facing down, fingers slightly spread apart, hands open and "
    "empty; legs straight and a little apart, both feet flat on the same level. "
    "Leave clear open space between each arm and the torso and between the two "
    "legs, so no limb touches or overlaps the body."
)

_FRAMING = (
    "Framing: the whole figure from the top of the head to the soles of the feet, "
    "seen straight from the front at chest height, centred, with a clear margin "
    "of empty backdrop around the fingertips, the head and the feet."
)

_LIGHT = (
    "Backdrop and light: a plain flat mid-grey backdrop with nothing else in the "
    "frame and no ground shadow, a soft frontal key light with a gentle fill from "
    "both sides so every form reads, sharp focus."
)

_KEEP = (
    "Keep the same character: the identical face, identity, age, body "
    "proportions and build, outfit, colours, materials and hairstyle."
)

# b simplifies the hair and the edges, so it keeps their design, not their detail
_KEEP_CLEAN = (
    "Keep the same character: the identical face, identity, age, body "
    "proportions and build, the same outfit design and colours, and the same "
    "overall shape and colour of the hair."
)

_CLEAN = (
    "Clean it up for 3D modelling: gather the hair into a few large solid locks "
    "that sit close to the head; give every piece of clothing crisp, clean edges "
    "and solid opaque surfaces; remove all weapons, props and held items; turn "
    "anything see-through or fuzzy, such as veils, smoke, particles, loose fur or "
    "feathers, into solid opaque shapes in the same colours."
)

_FACING = {
    "front": "",
    "back": "Image 1 shows the character from behind; turn it around to face the viewer.",
    "left": "Image 1 shows the character from its side; turn it to face the viewer.",
    "right": "Image 1 shows the character from its side; turn it to face the viewer.",
}

# v2, NOT used in v1: strip a character down to a clean base body for cloth
# re-fitting. The model runs as local open weights with no safety checker, so it
# cannot refuse: asked for a "bare" or "naked" body it would render nudity, and
# nothing between the farm and the public gallery would stop it. So the prompt
# never asks for skin. A seamless grey mannequin bodysuit and a bald head give
# the fitting stage the same thing - the body's shape without clothing or hair
# volume - with nothing to moderate.
BASE_BODY_PROMPT = " ".join(
    [
        "Redraw the character from image 1 as a plain base body in the same "
        "clean T-pose, keeping the same face, body proportions and build.",
        "Dress it in one seamless, skin-tight, matte mid-grey mannequin bodysuit "
        "that covers the whole body from the neck down to the wrists and ankles, "
        "with grey gloves and grey socks, and give it a smooth bald head. Every "
        "garment, armour piece, accessory, prop and strand of hair is gone; the "
        "suit shows only the shape of the body underneath.",
        _POSE,
        _FRAMING,
        _LIGHT,
    ]
)


def build_regen_prompts(view: str = "front") -> Tuple[str, str]:
    """The faithful (a) and clean-for-3D (b) instructions for one source view."""
    facing = _FACING.get((view or "front").strip().lower(), _FACING["front"])
    lead_a = "Redraw the character from image 1 standing in a clean T-pose."
    lead_b = (
        "Redraw the character from image 1 standing in a clean T-pose, as a tidy "
        "base for a 3D model."
    )
    faithful = " ".join(
        part for part in (lead_a, facing, _KEEP, _POSE, _FRAMING, _LIGHT) if part
    )
    clean = " ".join(
        part
        for part in (lead_b, facing, _KEEP_CLEAN, _CLEAN, _POSE, _FRAMING, _LIGHT)
        if part
    )
    return faithful, clean

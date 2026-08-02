# Render prompts

The renders feed an image-to-3D reconstructor, so the image is geometry input,
not artwork: a pretty render that reconstructs badly is a failure.

## One character, two styles

A single LLM call returns `subject`, `outfit` (with a colour attached to every
named part) and `body_type`. Both prompts are composed from that same
description by `compose_prompt()` in `render_prompting.py`, and both use the
same pose mask.

This structure is the whole point. Writing the two prompts as two independent
calls produced two different characters — a slim teenager in a white puffer
beside a chunky figure in a red parka — which makes the choice meaningless: the
operator is picking a style, not a character. A test asserts every
identity-carrying phrase appears in both prompts so they cannot drift apart
again.

The model is told not to write lighting, background, pose, framing or material
finish. The pipeline adds those per style; anything the model says about them is
another way for the two renders to diverge.

The cartoon style asks for what it actually wants: very few large flat
triangles, visible triangular facets with hard creased edges, mitten hands, no
fine detail. Naming triangles is what makes Flux drop the polygon count instead
of rendering a smooth model in bright colours — and coarse geometry is what the
3D stage reconstructs best anyway.

## Anti-patterns

These are measured, not stylistic preferences. A regression test
(`BANNED_TOKENS` in `tests/test_render_prompting.py`) keeps them out.

| Token | What it does |
|---|---|
| `character sheet`, `turnaround`, `model sheet` | Flux builds a multi-panel layout with thumbnails and annotation text; it survives matting and the 3D stage fails on it |
| `silhouette` | renders a literal black cutout figure |
| `even lighting`, `flat lighting` | read as no key, no falloff, no form shadow — the literal cause of dead, flat renders |
| `studio background` | pulls a floor sweep and a contact shadow; RMBG cuts through the shadow and leaves a dirty edge at the ankles |
| `white background` | makes Flux hazy and gives the matting stage the least contrast |
| `neutral background` | desaturates the whole frame, not just the backdrop |
| `arms stretched` | reads as stretch-deformation on the limbs |
| `PBR materials`, `high detail` | name no surface, so everything renders as one uniform semi-gloss plastic |
| `masterpiece`, `8k`, `trending on artstation`, `octane` | no-ops on Flux that push toward an over-processed plastic look |
| `bokeh`, `depth of field` | a soft contour becomes bloated mushy geometry |

Instead: name 2–4 surfaces with contrasting roughness, attach a colour to each
named part, and describe broad frontal light plus balanced fill plus a faint
edge light against a mid-value backdrop.

Two clauses are load-bearing and are in the fixed pose sentence:

- **frame margin** — the pose skeletons put fingertips within 31–91 px of the
  frame edge, and a clipped hand reconstructs as a truncated stump
- **open backdrop between the arms and torso** — the strongest defence against
  arms fusing to the torso in the mesh

## Metadata hygiene

Asset titles are mostly noise about the file. `_clean_metadata_text()` strips
author credits, years and marketplace vocabulary (rigged, FBX, Unity, PBR,
low poly, 4K…) and de-duplicates tokens, because repeated tokens get weighted by
T5 and drown the subject.

Before this, prompts read `full body character concept of Meshy Ai Chibi T Pose
Baby 0801063212 Texture Rigged Character` — the model rendered a marketplace
thumbnail rather than a character.

## What the pose masks actually do

Measured on the five `t_pose*.jpg` assets:

| Mask | Head fraction | Face keypoints |
|---|---|---|
| normal | 0.19 | no (28 px at hand level) |
| long | 0.21 | no (14 px) |
| fat | 0.22 | **yes** (57 px reaching head crown) |
| dwarf | 0.22 | **yes** (66 px) |
| goblin | 0.30 | no (29 px) |

Two conclusions the naming hides:

- normal/long/fat/dwarf sit in a 0.19–0.22 band — effectively one skeleton.
  Only goblin is genuinely different.
- Only **fat** and **dwarf** carry openpose face keypoints. The others hand
  ControlNet a head that is one stick and a nose dot, which is why head scale
  and facial orientation wander on the default path.

So prefer fat or dwarf for anything stout, chibi, big-headed or cartoonish;
reserve goblin for small wiry creatures with an oversized head; treat long as
near-identical to normal.

The masks are JPEGs of thin lines on black, and the ringing around 2–3 px
skeleton lines is read by the ControlNet as structure. Re-exporting them as PNG
is a pure-win asset change that has not been done yet.

## Workflow parameters worth revisiting

Researched but **not changed** — each alters the render and needs an A/B with a
pinned seed (`RenderPrompt.noise_seed`; both samplers re-randomise otherwise):

- node 222 refine uses `dpmpp_2m`/`karras`, wrong for a flow-matching model; the
  base pass already uses `simple`
- node 222 `start_at_step=1` of 6 re-noises to ~0.8 and regenerates the
  character at 2× resolution rather than refining it
- node 211 TiledDiffusion tiles 544 px over a 2048 latent: 16 tiles each drawing
  a whole character with the full subject prompt
- node 300 RMBG computes its mask at 1024 for a 2048 image, so the alpha the 3D
  stage reads is upsampled and stair-stepped

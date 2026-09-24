# AutoRig cloth manifest, version 1

The contract between the program that makes a character (AutoRig Regen) and
the runtimes that animate its hair and clothing (the AutoRig Cloth Unity
package, the web viewer, a Blender bake). A producer writes one JSON file next
to the rigged model; a runtime reads it and builds the simulation without any
manual setup.

Machine-readable schema: [`cloth-manifest.v1.schema.json`](cloth-manifest.v1.schema.json).

## Design rules

1. **Everything is addressed by bone name.** Positions are derived from bone
   world positions at runtime, never stored as coordinates. That makes the
   file immune to axis conventions (glTF Y-up right-handed, FBX Z-up, Unity
   Y-up left-handed) and to the scale an importer applies.
2. **Lengths are in the model's authored meters** and are rescaled at runtime
   through `calibration` (see below).
3. **Unity `JsonUtility` must be able to parse it.** So: no arrays of arrays,
   no dictionaries, no optional nested objects. Lists are arrays of objects;
   an absent string is `""`, an absent number is `0`. A reader cannot tell a
   missing number from a written 0, so producers write every field of every
   preset.
4. **Unknown fields are ignored**, so a v1 reader accepts files written by a
   newer producer that only added fields. A change in meaning bumps `version`.

## File

Name it `<model file stem>.autorig-cloth.json` and place it next to the model
(`hero.fbx` → `hero.autorig-cloth.json`).

```json
{
  "format": "autorig.cloth",
  "version": 1,
  "generator": "autorig-regen/0.1.0",
  "units": "meters",
  "calibration": { "bone_a": "Hips", "bone_b": "Head", "distance": 0.62 },
  "presets": [
    { "name": "hair", "gravity": 1.0, "damping": 0.12, "stiffness": 0.25,
      "angle_limit_deg": 70, "stretch": 0.0, "connection_stiffness": 0.0,
      "radius": 0.015, "radius_tip": 0.008, "inertia_move": 0.6,
      "inertia_rotate": 0.6, "drag": 0.02, "wind": 1.0 }
  ],
  "groups": [
    { "name": "hair_back", "kind": "hair", "attach_bone": "Head",
      "preset": "hair", "connection": "none", "collider_tags": ["body"],
      "chains": [
        { "bones": ["hair_back_00_0", "hair_back_00_1", "hair_back_00_end"] },
        { "bones": ["hair_back_01_0", "hair_back_01_1", "hair_back_01_end"] }
      ] }
  ],
  "colliders": [
    { "name": "head", "tag": "body", "shape": "sphere", "bone": "Head",
      "to_bone": "", "t": 0.35, "radius": 0.10, "radius_to": 0.0 },
    { "name": "thigh_l", "tag": "body", "shape": "capsule", "bone": "LeftUpLeg",
      "to_bone": "LeftLeg", "t": 0.0, "radius": 0.075, "radius_to": 0.060 }
  ]
}
```

## Fields

### Top level

| Field | Type | Meaning |
|---|---|---|
| `format` | string | Always `"autorig.cloth"`. A reader rejects anything else. |
| `version` | int | `1`. A reader rejects a higher major version it does not know. |
| `generator` | string | Free text, `name/version` of the producer. |
| `units` | string | Always `"meters"` in v1. |
| `calibration` | object | Two bones and the distance between them in the authored model. |
| `presets` | array | Named parameter sets referenced by groups. |
| `groups` | array | Simulated parts: one group is one set of chains that move together. |
| `colliders` | array | Body shapes the chains collide with. |

### `calibration`

A runtime measures the world distance between `bone_a` and `bone_b` on the
imported character and divides it by `distance`. Every length in the file
(`radius`, `radius_tip`, `radius_to`) is multiplied by that factor. A
bone-to-bone distance is invariant to translation, rotation and axis
convention, which a height or bounding box is not. If either bone is missing,
the factor is 1 and the runtime warns.

### `presets[]`

All numbers are dimensionless unless a unit is given. **Every field is
required**: `JsonUtility` reads a missing number as 0, so a partial preset
would silently simulate with no gravity or no collision radius. A runtime
warns when a preset looks partial. Values in brackets are the `default`
parameters of [`builtin-presets.v1.json`](builtin-presets.v1.json), used for a
preset name that is neither in the file nor built in.

| Field | Range | Meaning |
|---|---|---|
| `name` | string | Referenced by `groups[].preset`. |
| `gravity` | 0–2 [1] | Multiplier of 9.81 m/s². |
| `damping` | 0–1 [0.1] | Share of velocity removed per 1/60 s. |
| `stiffness` | 0–1 [0.2] | Share of the angle back to the animated pose direction recovered per 1/60 s. It turns links; it never changes their length. |
| `angle_limit_deg` | 0–180 [0] | Hard limit of deviation from the animated direction; 0 = no limit. |
| `stretch` | 0–1 [0] | Allowed stretch of links along a chain; 0 = inextensible. |
| `connection_stiffness` | 0–1 [0.5] | Stiffness of links between neighbouring chains (see `connection`). |
| `radius` | meters [0.02] | Collision radius of a chain's first joint. |
| `radius_tip` | meters [0] | Collision radius of the last joint; linearly interpolated in between. 0 = same as `radius`, as with `radius_to`, so a tip radius of exactly zero cannot be expressed. |
| `inertia_move` | 0–1 [0.7] | How much of the character's world translation the cloth feels. 1 = pure world-space physics, 0 = none (moves rigidly with the character). |
| `inertia_rotate` | 0–1 [0.7] | Same for the character's world rotation. |
| `drag` | 0–1 [0.02] | Air drag. |
| `wind` | 0–1 [1] | Influence of wind zones. |

Built-in preset names a runtime must know even when the file does not define
them: `hair`, `hair_stiff`, `skirt`, `cape`, `ribbon`, `tail`, `accessory`.
Their values live in one place, [`builtin-presets.v1.json`](builtin-presets.v1.json),
and producer and runtime tests both compare against that file. A file's own
preset with a built-in name overrides the built-in values. A group whose
`preset` is empty gets the built-in preset for its `kind` (the file's
`kind_fallback`: hair → `hair`, cloth → `skirt`, tail → `tail`,
accessory → `accessory`), with a warning.

### `groups[]`

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Unique within the file. |
| `kind` | string | `hair`, `cloth`, `tail`, `accessory`. Informational; runtimes may pick defaults by it. |
| `attach_bone` | string | The animated bone the group hangs from. Every chain's first bone is a descendant of it. |
| `preset` | string | A `presets[].name` or a built-in preset name. |
| `connection` | string | `none` — chains are independent (hair strands). `open` — joint *k* of chain *i* is linked to joint *k* of chain *i+1* (a cape). `loop` — as `open`, plus the last chain links to the first (a skirt). |
| `collider_tags` | string[] | Colliders whose `tag` is in this list affect the group. Empty = all colliders. |
| `chains[].bones` | string[] | Bone names ordered root → tip. |

**Chain order matters** for `open`/`loop`: chains are listed in angular order
around the attach bone, so neighbours in the list are neighbours in space.

**The last bone of every chain is an end joint.** Its position is simulated,
its rotation is not written, and it should carry no skin weight. Producers
therefore emit N deforming bones plus one `_end` bone per chain. The first bone
of a chain keeps its animated position (it is attached to its parent) and has
its rotation simulated; every later joint is a free particle.

Chains in one `open`/`loop` group should have the same number of bones. When
they do not, links are made only up to the length of the shortest chain in the
whole group. A `loop` of exactly two chains behaves as `open`: closing it would
link the same pair twice.

### `colliders[]`

| Field | Type | Meaning |
|---|---|---|
| `name` | string | Unique within the file. |
| `tag` | string | Matched against `groups[].collider_tags`. |
| `shape` | string | `sphere` or `capsule`. |
| `bone` | string | Anchor bone. |
| `to_bone` | string | Second bone. Required for `capsule`; optional for `sphere`. |
| `t` | 0–1 | Sphere only: centre at `lerp(bone, to_bone, t)`, or at `bone` when `to_bone` is `""`. |
| `radius` | meters | Sphere radius, or capsule radius at `bone`. |
| `radius_to` | meters | Capsule radius at `to_bone`. 0 = same as `radius`. A runtime without tapered capsules uses the larger of the two. |

A collider follows its bones every frame, so it animates with the character.

## Runtime behaviour a reader must implement

- Resolve every bone by exact name first, then by name with any namespace
  prefix stripped on both sides (`mixamorig:Hips` → `Hips`,
  `Armature|Hips` → `Hips`). When several bones match after stripping, the
  first in depth-first hierarchy order wins, with a warning. A group with an
  unresolvable bone is skipped with a warning; the rest still load.
- Read the animated pose first (after the Animator), simulate, then write
  rotations back — so an animation clip without the chain bones still plays
  and the chains swing on top of it.
- Reset (teleport) when the attach bone moves more than 3× the calibrated
  `bone_a`–`bone_b` distance (about 2 m on an adult with Hips–Head), or
  rotates more than 90°, within one frame. Running at 10 m/s is 0.17 m per frame at 60 fps, so this
  never fires on real movement.

## Versioning

v1 readers must accept any `version: 1` file. Adding a field is not a version
change. Changing the meaning or unit of an existing field is.

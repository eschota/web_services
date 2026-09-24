# AutoRig Cloth

AutoRig Cloth animates the secondary motion of rigged characters: hair, skirts, capes, coat tails,
tails, ribbons, ears and other danglers. It simulates **bone chains** (not meshes), so it works with
any skinned character and costs very little. Characters rigged by [AutoRig.online](https://autorig.online)
come with an `.autorig-cloth.json` manifest that describes their chains, presets and body colliders;
the package reads it and builds the whole setup automatically.

Version 0.1 (preview) — Unity 2021.3 or newer, any render pipeline, no package dependencies.

- [Installation](#installation)
- [Quick start](#quick-start)
- [Components](#components)
- [Parameters and built-in presets](#parameters-and-built-in-presets)
- [The manifest workflow](#the-manifest-workflow)
- [How a frame is simulated](#how-a-frame-is-simulated)
- [Scripting](#scripting)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)

## Installation

Package Manager → **+** → **Add package from git URL…**:

```text
https://github.com/eschota/web_services.git?path=/autorig-cloth/package
```

or copy the `package` folder into your project's `Packages/` folder (any folder name works). The
demo is in Package Manager → AutoRig Cloth → Samples → **Procedural Demo** → Import.

## Quick start

### An AutoRig character (with a manifest)

1. Import the rigged model and the `<model>.autorig-cloth.json` file next to it
   (`hero.fbx` → `hero.autorig-cloth.json`), and place the character in a scene.
2. Select the character root and choose **Tools → AutoRig Cloth → Apply Manifest to Selected
   Character**. The manifest next to the model is found automatically; otherwise you are asked for
   the file.
3. Press Play.

Alternatively add **AutoRig Cloth Setup** to the character root, assign the manifest and click
**Apply Manifest** — or leave **Apply On Awake** on to build the cloth when the character spawns.

### Any other character (no manifest)

1. Select the character root.
2. **Tools → AutoRig Cloth → Auto-Detect Chains on Selected Character** finds chains by bone name
   (hair, ponytail, braid, bang, skirt, cape, cloak, coat, tail, ribbon, scarf, sleeve, ear) and
   creates groups with suitable presets. Skirts that go all the way round become `Loop` groups,
   capes `Open` groups, hair independent strands. Bones mapped by a humanoid avatar are never used.
3. **Tools → AutoRig Cloth → Build Humanoid Colliders on Selected Character** adds a head sphere,
   chest / spine / hips capsules, tapered arm and leg capsules and hand spheres, sized from the
   avatar's bone lengths (humanoid rigs only).
4. Press Play and tune the groups.

### Manual setup

1. Add a **Cloth Bone Group** to any object (the character root is a good place).
2. Set **Attach Bone** to the animated bone the chains hang from (Head for hair, Hips for a skirt).
3. Add the chain root bones to **Chain Roots** and click **Build Chains From Roots**. Each chain
   follows single-child descendants from its root. With **Create End Joints** on, a chain whose last
   bone has no child (and is not already named `…_end`) gets a new child object, `<bone>_end`, at
   the extrapolated tip, so that bone's rotation can be simulated too (one more segment along the
   last bone; half the parent-bone distance along the bone's local Y axis for a single-bone chain).
   Auto-Detect Chains does the same.
4. Pick a **Connection** and a preset, and add colliders (**Cloth Sphere / Capsule / Plane
   Collider**) to the body bones.

### The demo

Create an empty scene, add an empty GameObject, add **Cloth Demo** (from the imported sample) and
press Play: a procedural character walks in a circle, stops, turns and jumps while its skirt, hair
and cape swing, collide with the legs and body, and flutter in the wind.

## Components

### Cloth Bone Group

One set of chains that move together.

| Field | Meaning |
|---|---|
| Attach Bone | The animated bone the chains hang from. Its motion drives inertia and teleport detection. Empty = the parent of the first chain root. |
| Connection | **None**: independent chains (hair). **Open**: joint *k* of each chain is linked to joint *k* of the next (a cape). **Loop**: Open plus last-to-first (a skirt). List Open/Loop chains in order around the attach bone. |
| Chains | Bones from root to end joint. The **root** keeps its animated position and gets its rotation simulated; later joints are free particles; the **end joint** is simulated but never rotated and should carry no skin weight. Each bone must be a descendant of the previous one. |
| Preset Source | **Built-in** preset name, a **Cloth Preset** asset, or **Custom** values. |
| Collider Tags | Only colliders with one of these tags affect the group. Empty = all colliders. |
| Radius Scale | Multiplies the preset radii. Radii are in this object's space (they follow its scale). |
| Teleport Distance | If the attach bone moves farther than this in one frame (or turns more than 90°), the cloth snaps to the animated pose. |
| Blend Weight | 0 = animated pose, 1 = fully simulated. |

In Play mode the inspector shows the simulated joint count and **Reset Simulation** / **Rebuild**
buttons. Parameter changes apply immediately; after changing chains or the connection at runtime
call `Rebuild()`. Disabling a group returns its bones to their rest pose.

### Colliders

All colliders have a **Tag** (default `body`) and follow their transform every frame. Radii are in
the transform's local units, multiplied by the largest axis of its lossy scale.

- **Cloth Sphere Collider** — centre offset and radius. Optionally **Towards** another bone and a
  blend: the centre then sits between the two (the manifest's `to_bone` / `t`).
- **Cloth Capsule Collider** — two ends and a radius at each (**End Radius** 0 = same as Radius).
  The second end is a local offset or follows **End Bone** (for limbs, the next joint).
- **Cloth Plane Collider** — an infinite plane through the object (floors, walls); cloth stays on
  the side the local normal points to.

### Cloth Wind

A wind zone. **Global** blows along the object's forward axis everywhere; **Sphere Directional**
does so inside a radius; **Sphere Radial** blows outward from the centre (a fan, a blast). Both
sphere modes fade linearly to zero at the edge. **Strength** is an acceleration in m/s²
(2–4 is a breeze), **Turbulence** adds gusts and swirl from a deterministic noise field, and
**Frequency** sets how fast the gusts change. Each group feels the zones at its attach bone, scaled
by its preset's `wind` value.

### Cloth Preset

A ScriptableObject (**Create → AutoRig Cloth → Cloth Preset**) holding the parameters below, shared
by any number of groups and editable in Play mode. The inspector can load the values of any
built-in preset as a starting point.

### AutoRig Cloth Setup

Holds a character's manifest and applies it (inspector button, or **Apply On Awake** for
characters spawned at runtime). The inspector also offers **Auto-Detect Chains**, **Build Humanoid
Colliders** and **Reset Simulation**, and selecting it draws every group and collider of the
character in the Scene view.

### AutoRig Cloth Manager

Created automatically in the DontDestroyOnLoad scene when the first group, collider or wind zone is
enabled in Play mode; never add it by hand. `AutoRigClothManager.Existing.Settings` exposes the global solver
settings: step rate (90 Hz), maximum steps per frame (4), constraint iterations (4) and gravity
(0, −9.81, 0).

## Parameters and built-in presets

All values are dimensionless unless noted. "Per 1/60 s" values are converted to the actual step
length, so behaviour does not depend on the frame rate or the step rate.

| Parameter | Range | Default | Meaning |
|---|---|---|---|
| gravity | 0–2 | 1 | Multiplier of gravity. |
| damping | 0–1 | 0.1 | Share of velocity removed per 1/60 s. |
| stiffness | 0–1 | 0.2 | Pull back toward the animated direction: share of the deviation removed per 1/60 s. 80 % of the correction is applied without adding velocity, so the chain returns with a soft bounce instead of buzzing. |
| angle limit | 0–180° | 0 | Hard limit of a joint's deviation from its animated direction; 0 = none. |
| stretch | 0–1 | 0 | Allowed stretch and compression of links along a chain while simulating (bones keep their length on screen). |
| connection stiffness | 0–1 | 0.5 | Stiffness of links between neighbouring chains, per 1/60 s. |
| radius | metres | 0.02 | Collision radius of the first joint of a chain. |
| radius tip | metres | = radius | Collision radius of the last joint (0 = same as radius); interpolated in between. |
| inertia move | 0–1 | 0.7 | Share of the attach bone's translation the cloth feels: 1 = pure world-space physics (trails behind), 0 = moves rigidly with the character. |
| inertia rotate | 0–1 | 0.7 | The same for the attach bone's rotation. |
| drag | 0–1 | 0.02 | Quadratic air drag: grows with speed, taming fast whipping while leaving slow sway lively. |
| wind | 0–1 | 1 | Influence of wind zones. |

The defaults are what the manifest specification prescribes for an unknown preset name. The
built-in presets (lengths for an adult-sized character):

| Preset | gravity | damping | stiffness | angle | stretch | conn. | radius → tip | inertia move / rot. | drag | wind | Character |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `hair` | 1.0 | 0.12 | 0.25 | 70° | 0 | 0 | 0.015 → 0.008 | 0.6 / 0.6 | 0.02 | 1.0 | Long hair: trails ~20° when running, settles in about half a second. |
| `hair_stiff` | 0.5 | 0.20 | 0.50 | 30° | 0 | 0 | 0.015 → 0.010 | 0.4 / 0.4 | 0.02 | 0.5 | Short or styled hair, bangs: keeps its shape. |
| `skirt` | 1.0 | 0.15 | 0.30 | 60° | 0.05 | 0.6 | 0.030 → 0.025 | 0.6 / 0.5 | 0.03 | 0.7 | Skirts and coat tails: keeps the silhouette, pushed by the legs. |
| `cape` | 1.0 | 0.08 | 0.08 | 110° | 0.03 | 0.5 | 0.035 | 0.8 / 0.75 | 0.05 | 1.0 | Capes and cloaks: loose and heavy, flies back when running. |
| `ribbon` | 0.8 | 0.06 | 0.05 | off | 0 | 0.3 | 0.010 | 0.85 / 0.85 | 0.08 | 1.0 | Ribbons, scarf ends, strings: very light. |
| `tail` | 0.4 | 0.18 | 0.45 | 45° | 0 | 0 | 0.040 → 0.015 | 0.5 / 0.45 | 0.02 | 0.2 | Animal tails: springy, partly self-supporting. |
| `accessory` | 1.0 | 0.25 | 0.35 | 50° | 0 | 0 | 0.012 | 0.5 / 0.5 | 0.02 | 0.3 | Earrings, pendants, animal ears. |

The `hair` values are identical to the example preset in the manifest specification, so a file
that defines `hair` that way and one that relies on the built-in behave the same.

## The manifest workflow

AutoRig Regen writes `<model>.autorig-cloth.json` next to the rigged FBX/GLB (format
`autorig.cloth`, version 1 — see `autorig-cloth/spec/cloth-manifest-v1.md`). Applying it:

1. **Checks the header.** `format` must be `autorig.cloth` and `version` 1; anything else is
   rejected without touching the scene. Unknown fields are ignored.
2. **Finds bones by name** — exact name first, then with namespace prefixes stripped on both sides
   (`mixamorig:Hips` ↔ `Hips`, `Armature|Hips` ↔ `Hips`). If several bones match, the first in the
   hierarchy wins and a warning is reported.
3. **Calibrates.** The world distance between `calibration.bone_a` and `bone_b` divided by the
   authored `distance` scales every length in the file (radii). If a calibration bone is missing the
   factor is 1 and a warning is reported.
4. **Replaces its previous output.** Groups and colliders a previous application created (and only
   those — hand-made components are never touched) are removed first, so applying is idempotent.
5. **Adds colliders** to the bones they are anchored to: spheres (centre between `bone` and
   `to_bone` at `t`) and tapered capsules (`bone` → `to_bone`, `radius` → `radius_to`).
6. **Creates groups** under a child object `AutoRigCloth` of the character root, one GameObject per
   group. A group whose attach bone or any chain bone cannot be found is skipped with a warning;
   the rest still load. Presets are looked up in the file first, then among the built-in presets;
   an unknown name uses the defaults above. The teleport distance is 3 × the calibrated bone
   distance (about 2 m on an adult).

A field missing from the file reads as 0 (or empty): `JsonUtility` runs no field initializers, and
the specification defines an absent number as 0. The bracket defaults apply only to preset *names*
that are not found, so a producer must write every field of a preset it defines; the loader warns
when a preset looks partially written (zero damping, radius and inertia).

The result is reported in the console and in the Setup inspector. Everything is an ordinary
component afterwards: tweak it, or save it with the prefab and turn **Apply On Awake** off.

Manifests are parsed with Unity's `JsonUtility`. The data classes (`ClothManifest`,
`ManifestGroup`, …) are plain serializable classes in `AutoRig.Cloth.Core`, usable from your own
tools.

## How a frame is simulated

1. **Update** (manager, execution order 32000): every chain bone gets its rest local rotation and
   position back. An Animator that does not animate these bones therefore cannot pick up last
   frame's simulated pose (no feedback drift); one that does simply overwrites them.
2. **Animator**: the animation is evaluated as usual.
3. **LateUpdate** (manager, execution order 32000, after your scripts' LateUpdate): the animated
   pose and the attach bones are read; colliders and wind are updated; the solver advances by
   `Time.deltaTime` in fixed 90 Hz steps (at most 4 per frame; slower frames run in slow motion
   instead of exploding); finally each chain is written back root to tip, turning every bone so it
   points at its simulated child. Only rotations are written.

Per step the solver moves the attach frame's non-felt share of motion rigidly (inertia), snaps to
the animated pose on a teleport, integrates (Verlet with damping, drag, gravity, wind), runs 4
constraint iterations (shape restoration, links along chains with the stretch allowance, the angle
limit, links between chains, collisions), and ends with a follow-the-leader pass that restores exact
link lengths root to tip and pushes each joint out of colliders once more. Collision wins over
length in that last pass: a joint can end up at most its push distance away from its rest length,
and its children are placed from its corrected position, so the error never accumulates.

The displayed pose is interpolated between the last two steps (smooth at any frame rate) and every
chain is attached exactly to its animated root. While `Time.timeScale` is 0 nothing is simulated,
but the cloth follows its character rigidly so it stays attached if the paused character is moved.

Scripts that read chain bones in LateUpdate before the manager see the animated pose; objects
parented to chain bones follow the simulation automatically. Nothing is allocated per frame.

## Scripting

```csharp
using AutoRig.Cloth;
using UnityEngine;

public class SpawnHero : MonoBehaviour
{
    public GameObject heroPrefab;
    public TextAsset heroManifest;

    void Start()
    {
        GameObject hero = Instantiate(heroPrefab);
        ClothSetupReport report = ClothManifestLoader.Apply(hero, heroManifest.text);
        if (!report.Succeeded) Debug.LogError(report);
    }

    public void TeleportHero(GameObject hero, Vector3 position)
    {
        hero.transform.position = position;
        hero.GetComponent<AutoRigClothSetup>().ResetSimulation(); // or ClothBoneGroup.ResetSimulation()
    }
}
```

Other entry points: `HumanoidColliderBuilder.Build(animator)`, `ChainDetector.Detect(root, animator)`
followed by `ChainDetector.CreateGroups(...)`, `ClothBoneGroup.SetChains(...)`,
`ClothBoneGroup.BuildChainsFromRoots(...)`. The solver itself (`AutoRig.Cloth.Core.ClothSolver`)
is plain C# with no Unity dependency and can be driven directly.

## Limitations

Version 0.1 is a preview:

- Bone chains only: no mesh (vertex) cloth, no self-collision and no collision between different
  groups. Planned: MeshCloth through a proxy mesh.
- Single-threaded managed code: about 0.5–1.5 ms per step for 2,000 joints with three colliders,
  measured under .NET 8 on a shared build machine; expect more under Mono. A typical character has
  100–300 joints. Burst / Jobs is planned.
- A group whose chains hang from another group's chain bones follows it one frame late.
- Animators in **Animate Physics** update mode: chain bones that such an Animator animates are
  overridden by the rest-pose restore; use the Normal update mode.
- Rotation write-back assumes uniformly scaled bones.
- Colliders are pushed out one at a time; a joint squeezed between overlapping colliders may end a
  step slightly inside one of them.
- No friction and no distance-based level of detail or culling yet.

## Troubleshooting

- **Nothing moves.** The simulation runs only in Play mode. Check the group inspector for warnings
  (chains need two or more bones, each a descendant of the previous one) and that its Blend Weight
  is above 0.
- **Hair or a skirt is pushed away from the body.** Colliders are larger than the body mesh, or the
  rest pose of the chains is inside a collider. Reduce the collider radii or the preset radius, or
  remove the collider's tag from the group's Collider Tags.
- **Cloth goes through the legs.** Add colliders to the legs (Build Humanoid Colliders) and make
  sure the group's Collider Tags include their tag (or leave the list empty).
- **The last bone of a chain does not move.** It is the end joint. Add an end joint below it
  (Create End Joints) so it can be rotated.
- **The chain bones are missing from the hierarchy.** The model was imported with *Optimize Game
  Objects*: add the chain bones to *Extra Transforms to Expose* in the model's Rig import settings,
  or turn the option off.
- **The cloth snaps when the character is moved by a script.** A move farther than the teleport
  distance in one frame is treated as a teleport; call `ResetSimulation()` yourself after
  intentional jumps, or raise the distance.

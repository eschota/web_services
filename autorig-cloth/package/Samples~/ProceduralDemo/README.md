# Procedural Demo

Shows AutoRig Cloth working without any assets.

1. Create a new, empty scene.
2. Create an empty GameObject and add **Cloth Demo** (Add Component → AutoRig Cloth → Samples →
   Cloth Demo).
3. Press Play.

`ClothDemo` builds a character out of primitives at runtime — a capsule body, swinging legs and a
head — and dresses it in three cloth groups:

| Group | Chains | Connection | Preset | Hangs from |
|---|---|---|---|---|
| Skirt | 8 × (4 bones + end joint) | Loop | `skirt` | Hips |
| Hair | 10 × (4 bones + end joint) | None | `hair` | Head |
| Cape | 5 × (5 bones + end joint) | Open | `cape` | Chest |

Body, head and leg colliders (tag `body`) and a floor plane keep the cloth outside the body. The
character walks around a circle, stops and starts again, turns and jumps, and a gusty wind zone
blows across the scene. Skirt and cape are drawn as double-sided strips over their chains
(`ChainStripMesh`), hair as thin cylinders parented to its bones.

Options on the component: walking speed, circle radius, jump interval and height, stop-and-go,
wind, and whether to create a camera, a light and a floor.

Select **Skirt**, **Hair** or **Cape** under the demo character in Play mode to see the simulated
joints, collision radii and links drawn in the Scene view, and tweak their parameters live.

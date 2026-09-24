# AutoRig Cloth

Secondary motion for rigged characters in Unity: hair, skirts, capes, coat tails, tails, ribbons and
accessories simulated on **bone chains**. Characters rigged by [AutoRig.online](https://autorig.online)
ship with an `.autorig-cloth.json` manifest; AutoRig Cloth reads it and sets everything up — chains,
presets and body colliders — with one click or automatically at runtime.

- Works with any skinned character and any render pipeline; Unity 2021.3+; no dependencies.
- `None` (hair), `Open` (cape) and `Loop` (skirt) chain connections, angle limits, stretch,
  inertia, teleport detection, turbulent wind.
- Sphere, tapered capsule and plane colliders with tag filtering; body colliders built from a
  humanoid avatar in one click; chains detected from bone names.
- Deterministic, allocation-free solver core in plain C#, simulated at a fixed 90 Hz and
  interpolated for smooth motion at any frame rate.

## Install

Package Manager → **+** → **Add package from git URL…**

```text
https://github.com/eschota/web_services.git?path=/autorig-cloth/package
```

## Use

1. Put the character in a scene with its `<model>.autorig-cloth.json` next to the model asset.
2. Select the character and run **Tools → AutoRig Cloth → Apply Manifest to Selected Character**.
3. Press Play.

No manifest? Use **Auto-Detect Chains** and **Build Humanoid Colliders** from the same menu, or set
up a **Cloth Bone Group** by hand. Import the **Procedural Demo** sample to see it working in an
empty scene.

Full guide: [Documentation~/index.md](Documentation~/index.md).

Copyright © 2026 AutoRig.online. All rights reserved — see [LICENSE.md](LICENSE.md).

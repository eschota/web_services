# Changelog

All notable changes to this package are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the package uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-09-24

First preview.

### Added

- Pure C# solver core (`AutoRig.Cloth.Core`): position-based Verlet simulation of bone chains at a
  fixed 90 Hz with an accumulator, interpolated animated pose, attach-frame inertia, teleport
  detection, shape restoration, angle limits, stretch allowance, links between neighbouring chains
  (`none` / `open` / `loop`), sphere, tapered capsule and plane colliders with tag masks, drag,
  gravity and turbulent wind. Deterministic and allocation-free per step.
- `ClothBoneGroup`, `ClothSphereCollider`, `ClothCapsuleCollider`, `ClothPlaneCollider`,
  `ClothWind`, `ClothPreset` and the automatically created `AutoRigClothManager`.
- Built-in presets `hair`, `hair_stiff`, `skirt`, `cape`, `ribbon`, `tail`, `accessory`.
- AutoRig cloth manifest v1 reader (`ClothManifestLoader`, `AutoRigClothSetup`) with bone-name
  resolution, calibration scaling and idempotent re-application.
- `HumanoidColliderBuilder` (body colliders from a humanoid avatar) and `ChainDetector`
  (hair / skirt / cape / tail … chains found by bone name).
- Inspectors, `Tools/AutoRig Cloth` menu with Undo, scene gizmos.
- Procedural Demo sample.

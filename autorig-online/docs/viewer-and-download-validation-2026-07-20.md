# AutoRig viewer and download validation - 2026-07-20

Production commit tested: `3b00c41df51f04139f0d374ae5d2064118d2ef1f`.

## Viewer performance

| Factor | Before | After | Impact |
| --- | ---: | ---: | --- |
| Bone correction | ~67% sampled CPU | Removed from runtime | Main CPU bottleneck eliminated |
| Viewports | 4 | 3 | Perspective plus Top and Front |
| Secondary renders | Every frame | Staggered by quality profile | Removes periodic p95 spikes |
| DPR | Unbounded device DPR | 1.25 / 1.0 / 0.8 / 0.65 | Largest scalable GPU and upload cost |
| Bloom and shadows | Always available | Disabled progressively | Preserves geometry while reducing GPU work |
| Current dominant call | Bone updates | `texSubImage2D` (6942 ms/10 s profile) | Texture upload is now the primary cost |
| Model complexity | 4 panes, correction overlay | 31 meshes, 71,765 GLB vertices | Geometry remains unchanged |
| DOM | Not measured | 1,094 elements / 3,883 CDP nodes | Secondary page cost, not the frame bottleneck |
| FPS / p95 | 8-9 / 133-167 ms | 39-44 / 34-38 ms | Acceptance target passed |

The automatic quality controller steps through `high -> balanced -> low -> emergency`. It degrades after sustained low FPS or high p95 latency, ignores loading/resize/animation-switch transients, and recovers one level at a time only after a 12-second healthy window. Failed recovery probes roll back.

## Browser QA

| Check | Result |
| --- | --- |
| Desktop 1477x912 | Pass, 44 FPS, p95 36 ms |
| Compact desktop 1016x912 | Pass, no horizontal overflow, 44 FPS, p95 34 ms |
| Mobile 390x844 | Pass, rail collapsed by default, 39 FPS, p95 38 ms |
| Viewer layout | Pass, exactly three non-overlapping panes |
| Animation rail | Pass, selection and playback work inside viewer |
| Rig type selector | Pass, 13 icon-only controls in one row with tooltips |
| Video | Pass, autoplay + loop + muted; 498x885 media fills 498x885 container |
| Bone/fitting UI | Absent |
| Bone/fitting API | `410 Gone` |
| Animation catalog | Pass, final ranged sweep returned `200` for all 39 ready previews |

## Artifact validation

Task `32958302-e3b5-48e8-8df7-5fb30962aacf` supplied the humanoid files. Task `bf20f918-f04c-4bab-80b6-2e3fedb53069` supplied the rabbit files. Tests ran headless on F1 without changing its converter or queue.

| Artifact | Blender 4.3.2 | Blender 5.1.0 | Content validation |
| --- | --- | --- | --- |
| Humanoid animations FBX | Pass | Pass | 30 meshes, 70,613 vertices, 84 bones, animated action |
| Humanoid animations GLB | Pass | Pass | 31 meshes, 71,765 vertices, 85 bones, animated action |
| Humanoid all-animations `.blend` | **Fail** | Pass | 5.1: 30 meshes, 306 bones, 26 actions |
| Prepared GLB | Pass | Pass | 30 meshes, 71,719 vertices, 30 materials, packed images |
| Rigged `.blend` | **Fail** | Pass | 5.1: model, materials and armature present |
| Custom-only FBX | Pass | Pass | 84-bone armature, one animated action, no model payload |
| Custom + base ZIP | Pass | Pass | Two FBX files; exact 84-name skeleton match |
| Rabbit pack FBX | Pass | Pass | Model, material, 63-bone armature and animated packed action |
| Rabbit animations GLB | Pass | Pass | Two named actions (`Rabbit_default`, `Rabbit_hop`) |
| Rabbit rigged `.blend` | **Fail** | Pass | 5.1: model, materials, 313-bone rig, two actions |
| Unity package | Archive pass | Archive pass | Unity asset/meta/pathname entries present |
| MP4 | `ffprobe` pass | `ffprobe` pass | H.264, 540x960, 30 FPS, 20.03 s |
| Full bundle ZIP | Archive pass | Archive pass | Seven expected production artifacts |

The three Blender 4.3 failures are production-format compatibility failures, not corrupt downloads. The files are Blender 5.x native or zstd-compressed Blender files and open correctly in 5.1. They cannot be made 4.3-compatible in the AutoRig web release; the converter export pipeline must generate an additional 4.3-native `.blend` using Blender 4.3.

Machine reports:

- `blender-4.3-validation-2026-07-20.json`
- `blender-5.1-validation-2026-07-20.json`
- `viewer-performance-2026-07-20.json`

# Real V3 media pool for the site refresh (2026-10-16)

Every file is a real production output of AutoRig V3, taken from the run directories on the VPS. Resize/crop for the
pages (hero ≤ 1600 px, cards ≤ 800 px, ≤ 300 KB); keep the source line in the page report.

| file | what | source |
|---|---|---|
| bird-stand-sheet.png, bird-stand.gif, bird-weights.png | bird category: rig check sheet (T-pose + clip frames, front/side/top), stand animation, skin weights | run 48bc7952 (task 395b0c1d, «green bird of prey») |
| hand-stand-sheet.png, hand-stand.gif | hands category: per-finger rig, hand clips | run d3ef5ccb (task 96f98e36, «low-poly human hand») |
| horse-028d1acd…, horse-75550318… | equine category: white horse, stand sheet and gait gif | runs 028d1acd, 75550318 («white horse: quadruped») |
| rider-weights.png | biped: skin weights preview of a motorcycle rider | run ede89a95 (task b13b5a46) |
| car-motorhome-input.png | vehicle conveyor input picture | run c91d4e7f (task c651e1f4, «Vehicle corpus · rv_motorhome») |
| car-motorhome-cut.png | the body with the wheels cut and the shell completed | same |
| car-motorhome-shell.png | the bare shell (no openables, no glass) | same |
| car-motorhome-parts-map.png | Object-ID segmentation map | same |
| car-motorhome-parts-sheet.png | parts review sheet (SAM masks per part) | same |
| car-motorhome-isolated-sheet.png | split body / suspension / wheel + every isolated and restored part (door, hood, glass, light, pipe, bumper) | same |
| scene-river-glade.jpg, scene-cathedral.jpg, scene-sponza.jpg, scene-cfa2e6a1-jungle-ruins.jpg | viewer environment scenes (baked lighting, probes) | unity/scene-media review renders |

Not here yet (ask the lead): viewer screenshots of the blueprint side view, the Parts panel, the session agent chat.

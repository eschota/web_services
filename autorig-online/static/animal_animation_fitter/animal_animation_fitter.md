# Animal animation fitter (browser MVP)

Fits a short skeletal animation to match frames from a reference MP4 (e.g. LTX output): stochastic coordinate descent on bone Euler rotations with silhouette + grayscale similarity.

## How to use on task page

1. Open an **animal** task with the standalone viewer.
2. **Idle animation Generator**: captures a **768×448** JPEG, sends it to `/api/task/{id}/idle-animation/start`, polls status until the MP4 is ready.
3. **Fit skeletal animation**: paste the proxied URL (`/api/task/.../idle-animation/proxy-video?url=...`) or any same-origin MP4, tune convergence / time / quality, run **Generate / Fit Animation From Video**, then **Apply** or **Export JSON**.

## Parameters

- `target_convergence_percent_float`: stop early when similarity reaches this (approximate).
- `max_compute_time_seconds_float`: wall time budget for the optimizer loop.
- `quality_level_string`: `fast` | `balanced` | `high` — controls compare resolution and mutation size.

## Convergence

`convergence_percent` is `score * 100` where `score` blends silhouette IoU proxy (55%) and grayscale similarity (45%) on downscaled frames.

## Limits

- Bone names are heuristic; rigs vary.
- LTX may change lighting/colours — rely mostly on silhouette.
- `THREE.AnimationMixer` clip paths may require tuning per rig; JSON export is always available.

## Improvements

Edge metric, temporal smoothing, seam closure, Web Worker scoring, WebGPU (optional).

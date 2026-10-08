# Low-resolution motion-analysis benchmark

`benchmark_lowres_motion.py` is an opt-in, offline profiling tool. It does not
modify Motion Transfer, submit farm work, call customer APIs, or replace the
original clip and projection artifacts.

It measures, separately, video decode, resize, grayscale conversion, and four
DIS optical-flow calculations per frame transition at 256, 384, and 512 pixels
per 2x2 tile. A fixed normalized 8x8 probe grid per tile is integrated through
the flow to report endpoint drift and path-length ratios against the clip's
native tile resolution. This is a numerical consistency metric, not anatomical
joint accuracy. It also writes low-resolution categorical projection derivatives:

- masks and triangle IDs remain PNG files;
- resizing is nearest-neighbour only;
- every output label must already exist in the source image;
- camera `px_world`, image size, and sheet tile offsets are scaled together;
- exact video frame rate, time base, dimensions, duration, input hashes, and
  derivative hashes are recorded in `benchmark.json`.

OpenCV is capped at two CPU threads. Each requested size gets a discarded
warm-up followed by three measured runs in rotated order; reported timings are
medians. Per-stage timings exclude engine initialization, while `wall_total_ms`
includes the whole decode/track pass. Sheet masks preserve their 2x2 layout;
individual per-view masks and triangle maps remain square.

Example against an existing completed run:

```bash
python3 benchmark_lowres_motion.py \
  --video /path/to/run/motion/run_raw.mp4 \
  --projection-dir /path/to/run/proj \
  --output /path/to/a/fresh/audit-directory \
  --sizes 256,384,512 --max-frames 32
```

Use a fresh output directory. Do not encode label images into H.264/MP4: lossy
chroma conversion can create or merge categorical IDs. The RGB clip may remain
compressed because it is only the visual source for tracking.

## Measured snapshot (2026-10-08 UTC)

An existing completed 141-frame, 768x768, 24 fps synchronized clip was profiled
on the production CPU (Ryzen 9 5900X), with OpenCV capped to two threads. Each
tile was natively 384 pixels. Medians of three runs:

| Tile | Measured CPU time | ms/transition | DIS share | Endpoint drift vs native, p95 |
|---:|---:|---:|---:|---:|
| 256 | 2.219 s | 15.85 | 90.5% | 129.29 native pixels |
| 384 (native) | 4.233 s | 30.24 | 97.7% | 0 |
| 512 | 6.804 s | 48.60 | 97.4% | 132.83 native pixels |

Thus 256 cut the measured decode/prepare/flow time by 47.6% versus native 384,
but the diagnostic trajectories were not equivalent. It is a performance
candidate, not a quality-approved default. Upscaling to 512 was slower and did
not improve agreement. DIS optical flow is the clear CPU hotspot.

Across 33 mask/triangle derivatives, no new label was invented. At 256, PNG
bytes were 31.2% of the originals, but the worst triangle map retained only
43.6% of source IDs; at 384 the corresponding figures were 61.2% and 70.8%.
This is expected sampling loss and is why originals remain canonical.

Receipt SHA-256:
`371333ee458aaac584cbb5cc5f5218717ebaa58b91ea63301f4f9e35b99332d1`.

### Actual joint-tracker gate

The same completed clip was also processed end-to-end by the existing joint
tracker at native 384-pixel tiles and at 256-pixel tiles. This separate audit
did not import or modify the private Motion Transfer source:

- native repeat: 9.592 s;
- 256: 5.352 s (44.2% faster for tracking and encoding);
- current-artifact downscale to lossless FFV1: 0.589 s, making the experimental
  total 5.941 s (38.1% faster); direct small-video generation would avoid it;
- all 4,935 joint/frame samples remained valid;
- against the native repeat, 3D position drift as a fraction of the reference
  trajectory bounding-box diagonal was p50 0.00438, p95 0.11057, max 0.37782;
- native repeat noise versus the saved baseline was p95 0.00669, so the 256
  divergence is materially larger than repeat variability;
- frames 70 and 140 visibly contain severe limb-track failures at both sizes.

The 256 path is therefore a measured speed optimization candidate, but it must
not become the default until the semantic tracker is improved and validated on
multiple clips. Aggregate reprojection and valid-joint rates were almost equal
at both sizes and did not expose the visible limb failures; they are insufficient
as the only quality gate.

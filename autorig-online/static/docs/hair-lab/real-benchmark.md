# Hair Lab — five real V3 models

Measured 2026-10-12 on the owner's RTX 4090 without stopping existing GPU
work. All five inputs are public-safe real V3 tasks retained in terminal
`needs_review`. The test uses each current `rigged.glb`, imports it through
Blender 5.2, applies object/world transforms, selects faces influenced by the
current `Head` group at a maximum vertex weight of at least 0.20, then renders
six orthographic diagnostic views at 256 px.

This is a **head-weight ROI benchmark**, not hair reconstruction. No case has
an accepted source-bound hair layer. Existing hair must be preserved. The
head-weight mask may include face, neck, accessories, clothing or hair that is
weighted to `Head`. The stored front/back labels are not treated as camera
truth; case `cd073629c54afde29b03` has independently observed mislabeled view
semantics and an incorrect original Vision color description.

| Case | Head ROI v/t | Blender import + head weights | CPU diagnostic projections | CPU hull p50/p90 | CUDA hull p50/p90 |
|---|---:|---:|---:|---:|---:|
| `a97ba8826602619b0d49` | 1,335 / 1,994 | 2.267 s | 1.469 s | 10.421 / 10.783 ms | 0.906 / 1.363 ms |
| `4fc873310d1c4a6ca22b` | 10,015 / 7,412 | 2.234 s | 0.908 s | 10.334 / 11.168 ms | 0.923 / 1.355 ms |
| `cd073629c54afde29b03` | 19,503 / 6,501 | 2.650 s | 0.858 s | 11.647 / 12.167 ms | 0.915 / 1.363 ms |
| `f141874551bdea41cb90` | 1,740 / 1,228 | 2.002 s | 0.718 s | 11.658 / 12.553 ms | 0.907 / 1.334 ms |
| `ad037dcd4e6df731bf07` | 439,792 / 785,121 | 14.618 s | 15.138 s | 11.864 / 12.616 ms | 0.921 / 1.204 ms |

The CPU and CUDA visual-hull occupancy/surface outputs match exactly in every
case. Twenty-five synchronized samples were recorded per device and case.
The retained run happened while the GPU was already at 100% utilization, so
these are contended live-machine observations rather than isolated laboratory
numbers.

## Actual CUDA raster replacement

`nvdiffrast`, PyTorch3D and Kaolin are not installed. The existing Hunyuan
portable runtime already has CuPy 13.6, so a bounded CUDA triangle rasterizer
was implemented without installing dependencies. It uses pixel-center
barycentrics, interpolated depth, signed bases for all six cameras and an
atomic z-buffer. Module compilation took 185.290 ms in the retained run.

| Case | ROI triangles | Same-process CPU core | CUDA p50/p90 | Separate-component sum: H2D+alloc+first raster+D2H | CPU-mask IoU mean |
|---|---:|---:|---:|---:|---:|
| `a97ba8826602619b0d49` | 1,994 | 33.530 ms | 0.952 / 1.538 ms | 2.454 ms | 0.9750 |
| `4fc873310d1c4a6ca22b` | 7,412 | 116.751 ms | 2.028 / 2.522 ms | 4.056 ms | 0.9743 |
| `cd073629c54afde29b03` | 6,501 | 103.815 ms | 1.026 / 1.893 ms | 2.787 ms | 0.9760 |
| `f141874551bdea41cb90` | 1,228 | 20.684 ms | 0.959 / 1.498 ms | 1.739 ms | 0.9897 |
| `ad037dcd4e6df731bf07` | 785,121 | 12,348.967 ms | 0.880 / 2.878 ms | 9.320 ms | 0.9806 |

The fair same-process core comparison for the heavy case is 12,348.967 ms on
the OpenCV CPU baseline and 0.880 ms CUDA p50. This large kernel-only gap does
not make the Hair Lab pipeline sub-millisecond: cold Blender import and
head-weight extraction still took 14.618 s for that model. The next useful
optimization is a bounded raw-GLB skin accessor that reproduces the current
rig transforms and `Head` weights without launching Blender.

IoU is measured against a same-process OpenCV integer-fill silhouette. The CUDA
result uses a different, more precise pixel fill rule and a real interpolated
z-buffer, so IoU below one is expected. Neither triangle-ID output is an
authoritative GLB backprojection map: Blender may reorder imported geometry,
and the CPU baseline used mean-depth painter ordering. IDs are diagnostic
ROI-local IDs only.

Geometry-derived guide generation was skipped for all five cases because no
exact hair layer passed the source-bound isolation gate. Running it on the
whole head ROI would produce a fast number but would not test real hair
guides.

Machine-readable receipts:

- `real-benchmark.json` — SHA-256 `e0e64e7a3e47ce845846c415aef33c1481a6082081dc422eef2f82408942499a` at the measured run.
- `gpu-raster-benchmark.rejected-v1.json` — superseded because it mixed viewport conventions and compared process wall time with a warm kernel.
- `gpu-raster-benchmark-v2.json` — SHA-256 `b7fb77ce8e4c69185f8589946fdfa59899b6bdba4eb6d68fe96987a857fe386e`.
- `cases.json` — SHA-256 `de8c35cf8501861b17350467e6f9eff1ca420f98e39c68366efcce4677038314`.

Each case has `benchmark-output/inspection_source_and_head_roi.png`, the six
masks, diagnostic triangle maps, Blender extraction receipt and NPZ geometry.

The v2 correctness gate passes a full-frame quad, reverse winding and overlap,
an exact depth tie (higher diagnostic triangle ID wins), partly offscreen and
degenerate faces. Every real case also passes an independent scalar z-buffer
comparison on a deterministic 128-triangle subset: mask and winning triangle
IDs match, with maximum absolute normalized-depth error
`1.1920928955078125e-7`. Invalid
indices, empty faces, non-finite vertices and invalid extents fail closed.

The raw v2 JSON keeps the legacy field name
`cold_same_work_excluding_module_compile_and_file_io_ms`. Its value is a sum
of separately observed H2D, allocation, first raster and D2H components. D2H
was measured after the warm loop, and allocator/runtime state was already warm
across later cases, so this field is not a contiguous cold end-to-end timing.

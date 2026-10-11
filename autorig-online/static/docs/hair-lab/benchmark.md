# Hair Lab geometry benchmark — 2026-10-12

This is a reproducible microbenchmark of two synthetic geometry kernels. It
does not measure image analysis, hairstyle reconstruction, textures, dynamics,
or end-to-end quality. The six masks describe one internally consistent
orthographic synthetic head/hair envelope; they are not analyzer output.

The retained run used an RTX 4090 with driver 610.88 and PyTorch 2.11.0 cu130.
The machine had 6,214 MiB of live free VRAM and 19% GPU utilization before the
run. Existing GPU work was not interrupted. Peak CUDA allocation was 160.4 MiB
and peak reservation was 244 MiB. CPU timing used 16 PyTorch threads on a
32-logical-CPU AMD system.

Each p50 and p90 has 25 synchronized samples.

| Kernel | Case | CPU p50 / p90 | CUDA p50 / p90 | Output |
|---|---:|---:|---:|---:|
| 6-view visual hull + surface band | 128^3 | 1.564 / 1.749 ms | 0.272 / 0.492 ms | 0.51M occupied voxels |
| 6-view visual hull + surface band | 192^3 | 4.685 / 5.012 ms | 0.275 / 0.533 ms | 1.72M occupied voxels |
| 6-view visual hull + surface band | 256^3 | 11.789 / 12.411 ms | 0.422 / 0.643 ms | 4.10M occupied voxels |
| guides to ribbon mesh | 100 x 16 samples | 0.474 / 0.495 ms | 1.249 / 1.632 ms | 3,200 vertices |
| guides to ribbon mesh | 500 x 16 samples | 1.462 / 1.637 ms | 1.069 / 1.489 ms | 16,000 vertices |
| guides to ribbon mesh | 2,000 x 16 samples | 3.098 / 3.667 ms | 1.154 / 1.539 ms | 64,000 vertices |

Sixteen samples form fifteen spline segments. Small ribbon jobs favor CPU or a
client implementation because GPU dispatch overhead dominates. Batched 2,000
guide jobs benefit from GPU execution. The 256^3 visual hull is about 28 times
faster on CUDA in this run, so mask fusion and field seeding are strong GPU
candidates.

The integrity check reproduced the hull and surface bit-for-bit on CPU and
CUDA. Ribbon indices were identical; vertices agreed within 5.97e-8, were
finite, and all triangle indices were in range.

Reproduce with:

```powershell
R:\ComfyUI_windows_portable\python_embeded\python.exe `
  autorig-online\projects\hair-lab\benchmarks\hair_geometry_benchmark.py `
  --output .work\hair-lab-20261012\gpu-benchmark.json --repeats 25
```

Benchmark source SHA-256:
`3cb2279c6292a3f82f5ad44892d2b9793dba82ab07cff24d94fbd60862b7365f`.
Raw retained result SHA-256:
`6f5b1dfc67f8a772c7a2e84bbca53bbf5849078954fc0ce2de353a47d9db7018`.

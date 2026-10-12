# Direct GLB Head-weight reader — five real V3 rigs

Observed 2026-10-12. This is a bounded reader for the current rig artifact, not
a hair classifier or hair generator. It reads embedded GLB 2.0 accessors,
accumulated node transforms, skin joint tables, `JOINTS_0` and `WEIGHTS_0`,
normalizes skin weights as a glTF client does and admits a triangle when at
least one vertex has normalized `Head` weight >= 0.20.

The output retains `(mesh_id, primitive_id, local_triangle_id)` provenance.
It fails closed on external buffers, required extensions, compressed or sparse
accessors, morph targets, non-triangle topology, invalid bounds and excessive
input/geometry sizes. It does not map this ROI to source-upload vertex IDs and
does not classify the ROI as hair.

| Case | Rigged GLB | Direct v/t | Fresh process wall* | In-process p50/p90 | Blender reference extraction |
|---|---:|---:|---:|---:|---:|
| `a97ba8826602619b0d49` | 1.32 MB | 1,335 / 1,994 | 139.15 ms | 3.30 / 3.44 ms | 2.267 s |
| `4fc873310d1c4a6ca22b` | 4.69 MB | 10,015 / 7,509 | 165.54 ms | 13.91 / 14.74 ms | 2.234 s |
| `cd073629c54afde29b03` | 32.69 MB | 19,503 / 6,501 | 238.65 ms | 87.89 / 89.42 ms | 2.650 s |
| `f141874551bdea41cb90` | 4.69 MB | 1,740 / 1,228 | 143.34 ms | 7.99 / 8.46 ms | 2.002 s |
| `ad037dcd4e6df731bf07` | 78.73 MB | 439,792 / 785,121 | 1,702.73 ms | 357.08 / 364.98 ms | 14.618 s |

\* Fresh process wall includes Python startup and output writes. The OS cache
state was not controlled, so it is not a cold-disk benchmark. In-process
figures use five reads and exclude subprocess startup/output serialization.

## Blender comparison

Four cases reproduce the Blender reference geometry within the declared
tolerances. Position matching is order-independent in both directions at
`1e-6`. Triangle comparison uses order-independent centroid plus sorted-edge
descriptors at `1e-5`, while array equality is also recorded when ordering is
preserved.

`4fc873310d1c4a6ca22b` deliberately remains a mismatch: the direct reader
preserves 7,509 exact primitive triangles, while Blender exposes 7,412. The 97
additional direct triangles are 97 repeated geometric faces at `1e-6`; the
Blender reference contains zero repeated faces. Every reference triangle has
an exact descriptor match in the direct result and all 10,015 selected vertex
positions match exactly. The reader does not delete those source triangles to
force parity because their exact primitive/local-triangle provenance is part
of the contract.

For the 785,121-triangle case, face arrays are equal and bidirectional position
distance is at most `9.31322574615479e-10`. An older quantized multiset hash
can differ at rounding-cell boundaries; the order-independent geometric
descriptor comparison passes with maximum distance about `1e-9`.

The result therefore reports `all_position_and_triangle_parity=false`: four
cases match Blender geometrically, and one has a fully explained structural
difference caused by Blender removing repeated faces. This is not hidden or
reclassified as success.

## Receipts

- Manifest SHA-256: `de8c35cf8501861b17350467e6f9eff1ca420f98e39c68366efcce4677038314`
- Benchmark JSON SHA-256: `326c2c15b827b73a2b91f15580e097fbb912ee670e8859e1d016220d5a5cd4e6`
- Reader source SHA-256: `3932a6a0a6003b6616281e9ca56b7aee1926e4778d2ab2911ba0dfd55038bfb5`
- Benchmark source SHA-256: `e32a43e3f888b96e7b837a084bad06c2a0cf6fbf1ebb2e42416706bfafec2e50`
- Focused malformed-input tests SHA-256: `d15522128071005f347aea27c27015625b4b9620e25ced640d027ff65d3851a5`

The heavy model's fresh-process wall falls from the retained Blender reference
of 14.618 seconds to 1.703 seconds under the observed OS-cache state. The next
integration step is to feed this exact current-rig ROI directly into the CUDA
projection stage while keeping the existing fail-closed format gates.

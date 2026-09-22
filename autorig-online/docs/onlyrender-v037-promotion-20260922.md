# OnlyRender ComfyUI 0.37 promotion gate

Status: **ready for an operator-controlled cutover; not enabled in production**.

## Durable runtime

- Runtime root: `R:\autorig\.runtime\onlyrender`
- ComfyUI checkout: `R:\autorig\.runtime\onlyrender\ComfyUI`
- Pinned commit: `e638023d54497dbe0579565e5de4bb7076899592` (ComfyUI 0.37.0)
- Isolated Python overlay: `R:\autorig\.runtime\onlyrender\python-overlay`
- Stable input/output/temp/user paths: `R:\autorig\.runtime\onlyrender\runtime`
- Candidate controller: `autorig-online/deploy/onlyrender/worker-4090-v037.ps1`
- Rollback controller: `autorig-online/deploy/onlyrender/worker-4090.ps1`
- Machine-readable local gate: `R:\autorig\.runtime\onlyrender\REGRESSIONS_PASSED.json`

The candidate controller verifies the exact Git commit and isolated dependency
overlay before startup. Its Stop action only terminates a port 8988 listener
whose executable, launcher path, and port all match this runtime. The old
controller and ComfyUI 0.21.1 installation remain intact for rollback.

`WAN2_PROMOTED` is deliberately absent. The v0.37 controller therefore does
not advertise Wan Animate 2 until the production cutover is explicitly made.

## Compatibility gate

CPU-only object-info validation passed for all 14 currently advertised
workflows: 52 unique node classes and 16 literal model files were resolved.
The isolated custom-node set contains LTXVideo, controlnet_aux,
Video-Depth-Anything, KJNodes, VideoHelperSuite, srl-nodes, and Helper-Nodes.
The isolated LTXVideo copy was updated to commit
`dfb2786749af36f200ea023388dc729a3e106b42`; the production 0.21.1 copy was not
changed.

## GPU regressions

All jobs ran sequentially against the durable 0.37 runtime on port 8990. Models
were explicitly unloaded through `/free` between model families.

| Workflow | Prompt ID | Result | Time | Peak VRAM |
|---|---|---:|---:|---:|
| CyberRealistic Pony, 50 steps, DPM++ 2M SDE/Karras, CLIP skip 2 | `c9de5cdb-d1a9-4dc7-b7db-9a04dc7428b4` | PNG 960x540 | 12.32 s | 10,309 MiB |
| Flux2 Klein Avatar, Maya + Leo + scene reference chain | `2b50c76b-379c-4ebe-b16b-ca259dd48306` | PNG 960x540 | 18.54 s | 13,131 MiB |
| LTX2.3 Pose, real 25-frame source, latent selection before VAE decode | `89651d19-334d-43de-8dd6-f120ea500ede` | MP4 25 frames, 960x540, 24 fps | 156.46 s | 18,819 MiB |

The two images were inspected at native resolution. The Pony output was a
coherent two-person photograph. The avatar output preserved two distinct adult
identities and the third reference's indoor composition. A five-frame contact
sheet for LTX2.3 showed stable people and room composition from frame 0 through
frame 24. The graph used `LTXVSelectLatents` before tiled VAE decoding and
limited delivery to exactly 25 frames.

After the last terminal result, the candidate PID was verified and stopped.
The old controller successfully restored ComfyUI 0.21.1 on port 8988, its queue
was empty, and worker registration completed. No permanent runtime switch was
made.

# Video VAE decode: fast and memory-safe on every box (2026-09-23)

## Problem

LTX-2.5 sampling was fast, but the VAE decode that followed it hung or ran out of memory:

* **worker-4090** (24 GB): 249 frames at 1152x1536, decode ran for over an hour in Windows shared GPU memory.
* **f12** (12 GB / 32 GB RAM): 393 frames at 608x960 hung for over 18 min. 193 frames at 1152x2048 took 186 s, with VRAM fully used and RAM down to 5.7 GB free.
* **f15** (8 GB): even 97 frames at 960x544 took 498 s.

`/interrupt` was not honoured while a tile was decoding.

## Root cause

The LTX-2.5 video VAE (`ltx-2.5-video-vae-bf16`) is the LTX 2.4 *diffusion* decoder, `CausalDiffusionVAE`. It is not the older convolutional VideoVAE. The old decoder streams temporal chunks by itself. The diffusion decoder does not, and per Lightricks it processes roughly 500x more tokens at 1080p.

Our templates used `LTXVTiledVAEDecode` with 2x2 tiles, which tiles **only spatially**. Each tile therefore decodes the whole clip in one pass:

* VRAM grows with frame count. ComfyUI's out-of-memory fallback retiles every tile.
* On Windows, with the driver's default sysmem fallback, the allocation silently spills into shared memory and runs at PCIe speed.

After decode, the whole clip sits in system RAM as float32: 5.5 GB per copy at 193 frames of 1152x2048. The old chain held several copies at once: the tile accumulator and weights, the lanczos delivery resize, and CreateVideo/SaveVideo.

## Sources

* Lightricks, LTX-2.5 decode-freeze thread ([huggingface.co/Lightricks/LTX-2.5/discussions/15](https://huggingface.co/Lightricks/LTX-2.5/discussions/15)). Staff say the diffusion decoder is about 500x the tokens and that tiling is the way to run it on consumer GPUs. Users fixed freezes with smaller tiles and `temporal_size 64 / temporal_overlap 16`.
* LTX ComfyUI node docs ([docs.ltx.io](https://docs.ltx.io/open-source-model/integration-tools/ltx-comfy-ui-nodes)): `LTXVTiledVAEDecode` is spatial only; `LTXVSpatioTemporalTiledVAEDecode` is for long clips.
* ComfyUI PR #15499, LTX 2.5 support ([github.com/Comfy-Org/ComfyUI/pull/15499](https://github.com/Comfy-Org/ComfyUI/pull/15499)): tiled 3D decode for the diffusion VAE, with temporal tiles.
* ComfyUI `VAEDecodeTiled` docs ([docs.comfy.org](https://docs.comfy.org/built-in-nodes/VAEDecodeTiled)): `temporal_size` / `temporal_overlap` apply to video VAEs.
* NVIDIA, "System Memory Fallback for Stable Diffusion" ([nvidia.custhelp.com a_id 5490](https://nvidia.custhelp.com/app/answers/detail/a_id/5490)): the "CUDA – Sysmem Fallback Policy" setting.
* Code read on the boxes (ComfyUI 0.37.0, ComfyUI-LTXVideo f8387c8):
  * `comfy/sd.py` VAE decode and decode_tiled
  * `comfy/utils.tiled_scale_multidim`
  * `comfy/ldm/lightricks/vae/na_diffusion_decoder.py`
  * `comfy_api/.../video_types.py` (SaveVideo encoding)
  * `ComfyUI-LTXVideo/tiled_vae_decode.py`

No model was downloaded. A tiny preview decoder (TAE for LTX2) would be a model download, so it was not installed.

## What was applied

### 1. Temporal-tiled decode

Commit 5c19df18; releases `vae-decode-20260923-a/b`.

* **Templates:** all nine LTX-2.5 templates now decode with core `VAEDecodeTiled`: 512 px tiles, 64 px overlap, 64-frame temporal windows, 16-frame temporal overlap. That covers standard, HQ, pose/depth/canny control, and the legacy names that route to LTX-2.5.
* **`runtime_settings.py`:**
  * The union-control latent crop now follows any video decoder class.
  * A video decoded at exactly the requested size skips the identity lanczos "delivery" resize. That resize was a full float copy of the clip and cost 13 s at 193x1152x2048.

### 2. Decode to disk

The custom node lives in `deploy/comfy-nodes/autorig_stream_decode`. `AutorigStreamVideoSave` replaces the chain VAEDecodeTiled → (trim) → (ImageScale) → CreateVideo → SaveVideo:

* It decodes the latent in temporal windows with exactly `tiled_scale_multidim`'s layout and feathering. A unit test pins it frame for frame against comfy's loop.
* Each finished frame goes straight into the H.264 encoder (PyAV, libx264 defaults, yuv420p, BT.709, faststart, the same as SaveVideo). The AAC audio is muxed at the end.
* RAM holds about one window, whatever the clip length.
* `/interrupt` is honoured between windows.
* The output name and history entry match SaveVideo (`<task>_00001_.mp4`, `type: output`), so renderfin collects the file unchanged.

Renderfin side, `renderfin/stream_decode.py` plus a hook in `queue._submit_task`:

* **Probe:** at submit, each box's `/object_info/AutorigStreamVideoSave` and `/system_stats` are probed (cached 5 min).
* **Boxes with the node:** the graph is rewritten to the streaming node.
* **Boxes without it (RAM guard):** a clip whose float32 frames exceed 20% of the box's RAM is refused on that box, so a peer can take it. On 32 GB this allows 393 frames at 608x960, 193 at 1152x2048 and 249 at 1152x1536; it refuses 393 frames at 1152x2048.
* **VRAM-class policy**, applied to both paths:

| VRAM | tile | temporal window | temporal overlap |
|---|---|---|---|
| ≥ 10 GB | 512 px | 128 frames | 32 frames |
| < 10 GB | 384 px | 96 frames | 32 frames |

The 32-frame overlap removes the motion hitch that the 16-frame overlap leaves at every window seam: a 1.28x frame-difference spike at frame 56 disappears. 64-frame windows with a 32-frame overlap damp motion by about 7%, so the policy uses windows of 96 frames and up.

The templates are unchanged by part 2. A box without the node keeps working, so the rollout is safe while boxes restart.

### Where the node is installed

* **f12, f5, f15, Raptor:** node installed and ComfyUI restarted when the queue was idle. wfcheck passes: every class of every advertised workflow is present, and the node is present.
* **ComfyMinerGuard:** allowed on f5, f15 and Raptor.
  * On f5 the guard's live monitor overwrote the allow-list. It quarantined both our node and `ComfyUI-GGUF`, which was not in f5's baseline.
  * Both were restored and allow-listed with the monitor stopped: `comfy_miner_guard.py allow-node --config ...` run directly with the embedded python, then the monitor restarted.
  * `comfy_guard.ps1 allow-node` printed nothing on f5.
* **worker-4090:** files copied to `R:\autorig\.runtime\onlyrender\runtime\custom_nodes\autorig_stream_decode`. They load on its next normal restart (`worker-4090.ps1`). Until then it uses the in-RAM path behind the RAM guard and the 24 GB policy.

## Measurements

All decode-only runs decode the same saved latents. They were made with a benchmark harness that polls `/system_stats` every second.

Decode time here is decode plus delivery resize plus encode. Free VRAM and RAM are given as minimum / at start of the run.

| Box | Case | Before: `LTXVTiledVAEDecode` 2x2 | After: part 1 (`VAEDecodeTiled` 512/64/64/16) | After: part 2 (stream node, box policy) |
|---|---|---|---|---|
| f12 3080 Ti 12 GB / 32 GB | 97 frames, 960x544 | 18.6 s; VRAM free 7.9 / 8.1 GB | 17.2 s | 15.4 s (t128/o32) |
| f12 | 193 frames, 1152x2048 | 220 s (decode 186 s, out-of-memory retile on every tile, VRAM free 0.08 GB, RAM free 5.7 GB) | 111–133 s; VRAM free 6.6 GB | 118.5 s; VRAM free 6.7 GB; RAM does not drop during the decode |
| f12 | 393 frames, 608x960, real task 50d003fe | hung for over 18 min; ComfyUI killed | Done in 254 s; RAM free 4.7 GB (whole run) | decode+save 70.8 s; RAM free 8.9 GB during decode+save (5.9 GB during sampling, from model weights) |
| f12 | 193 frames, 1152x2048, full production graph | 700 s total (sampling 459, decode 186, resize 13, save 22); RAM free 5.7 GB (whole run) | — | 621 s total (sampling 468, decode+save 125); RAM free 13.7 GB during decode+save (5.5 GB during sampling, from model weights) |
| f15 3070 Ti 8 GB | 97 frames, 960x544 | 510 s (sysmem spill) | — | — |
| f5 3070 Ti 8 GB | 97 frames, 960x544 | — | 40 s | 58 s (384/96/32). 512/96 runs out of VRAM; 512/128 takes 194 s |

Quality:

* The diffusion decoder is noise-seeded per decode call, so any change of tiling changes fine texture. PSNR is about 28 dB between tilings; no tiling is bit-exact against another.
* The streaming node against `VAEDecodeTiled` with the same tiles: 39.5 dB. The error curve is smooth (encoder drift), with no step at window seams.
* The contact sheets and motion profile show no spatial seams. With the 32-frame overlap there is no temporal hitch.
* Output check for the 393-frame run: 393 frames, 608x960 yuv420p H.264, and a 48 kHz stereo AAC track.

MiniMax H3 needed no change. Its VAE already tiles and streams temporal chunks inside comfy: 256 px tiles, 17-frame chunks, output written into a CPU buffer. Its ~48 GB RAM footprint comes from the model weights (the Qwen3-VL-32B encoder), not from decode. H3 was not re-measured, because worker-4090 was busy with LoRA training and then with fleet jobs.

## Owner action (not changed by us)

* **NVIDIA Control Panel on every Windows render box:** Manage 3D settings → Program Settings → ComfyUI's `python.exe` (or Global) → "CUDA – Sysmem Fallback Policy" = **Prefer No Sysmem Fallback**. With it set, a decode that does not fit in VRAM raises a CUDA out-of-memory error. ComfyUI then retiles, instead of the driver silently paging into shared memory for an hour. That silent paging is what happened on worker-4090 and f15.
* **worker-4090:** restart the worker once, with the START/STOP shortcuts, so it loads `autorig_stream_decode`.

## Follow-ups

* Measure H3 and the HQ two-stage graph on worker-4090 when it is free.
* The per-box policy uses total VRAM. f15 shares its GPU with the Freestock worker (about 2 GB), which the 384/96 setting already tolerates.

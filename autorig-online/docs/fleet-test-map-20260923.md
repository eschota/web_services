# Fleet test map: model × box (2026-09-23)

Owner's rule: placement decisions come only from this map. A box is **acceptable
for a model when its warm time is at most 5× the worker-4090 warm time**, it
finishes without OOM, and free system RAM never drops below 2 GB (the RAM guard
posts `/interrupt` at < 2 GB).

Legend: ✅ ≤ 5× and stable · ⚠️ passes only with a setting that is not live yet, or is
at the limit · ❌ > 5×, guard trip or crash · — model not installed on the box.

## How it was measured

* Each graph went straight to the box's own ComfyUI (VPS tunnels f5 :18488,
  f15 :18588, f12 :18288 via its GPU arbiter, Raptor :8288 via its arbiter,
  worker-4090 local :8988). Each box was set offline in renderfin during its window.
* Graphs are the production templates, filled by the production code
  (`renderfin.templating.render_workflow_text` + `apply_runtime_settings` +
  `multiref.inject_references`). The only exception is the H3 test graph, a copy of
  `gen_video_minimax_h3_by_url.json` with the optional end-frame guide removed
  (start frame only), 960×544.
* Same inputs, prompt and seed (43) on every box. Video start image: 960×540
  `ltx25_woman.png`, which the video agent also used. Images: 1024². Z-Image pose uses the
  OpenPose map `a1f7ab03…`. Multi-ref (2 refs) uses `a6674df6…` (person) and
  `3d048fb9…` (outfit).
* **cold** = first run after other models (loaders not cached). **warm** = the same
  graph again with the start image under a new file name: loaders stay cached and
  everything after them re-runs. Times are ComfyUI execution time (queue wait
  excluded). Sampling and decode come from per-node websocket timings.
* RAM/VRAM come from `/system_stats` polled every 2 s. "min RAM free" is the worst
  free system RAM seen, and "peak VRAM" is total minus the lowest free VRAM (it
  includes other processes on the card).
* Quality: SSIM (ffmpeg, all frames) of the same-seed output against the 4090 output,
  plus 6-frame contact sheets (VPS `~/fleetmap/sheets/`, outputs in `~/fleetmap/out/`).

### Frame grids (the owner's 960×540 / 150 f case)

* **MiniMax H3** only takes 17k+5 frames (`nodes_minimax_h3.py:align_frame_count`).
  150 snaps **up to 158 frames (6.6 s at 24 fps)**, and the production path does the same.
  Its canvas is a multiple of 32, so the test ran at 960×544; production delivers 960×540
  after an ImageScale.
* **LTX-2.5** takes 8k+1 frames: 150 becomes **153 frames**, delivered at 960×540.

## Boxes

| Box | GPU / system RAM | Runtime during the map |
|---|---|---|
| worker-4090 (reference) | RTX 4090 24 GB / 63 GB | torch 2.11+cu130, ComfyUI 0.37, `--reserve-vram 3 --vram-headroom 3` (added today after the sysmem-fallback incident below) |
| f12 | RTX 3080 Ti 12 GB / 32 GB | torch 2.9.1+cu130, driver 610.62 |
| Raptor | RTX 3080 Ti 12 GB / 64 GB, PCIe gen3 | **torch 2.7.1+cu128 → 2.9.1+cu130 today**, driver 610.62 |
| f15 | RTX 3070 Ti 8 GB / 32 GB | **torch 2.7.1 → 2.9.1+cu130 and driver 576.02 → 610.62 today** |
| f5 | RTX 3070 Ti 8 GB / 32 GB | **torch 2.7.1 → 2.9.1+cu130 today**, driver 591.86 |

### The finding that changed the map: torch 2.7 disabled DynamicVRAM on the farm

Until today, f5, f15 and Raptor ran PyTorch 2.7.1. At every start ComfyUI 0.37 logged
"Unsupported Pytorch detected. DynamicVRAM support requires Pytorch version 2.8 or
later … Falling back to legacy ModelPatcher". On the legacy patcher the weights sit in
system RAM ("loaded partially … 14085 MB offloaded") instead of streaming from the NVMe
disks (all farm model disks are NVMe, so fast_disk=True applies once DynamicVRAM is on).
So the 22B video models either exhausted the 32 GB boxes or crawled on Raptor.
f12 (torch 2.9.1) and the 4090 (2.11) never had this problem.

The upgrade path followed one rule: download once per site, then copy over the LAN. The
wheels came to f15 from download.pytorch.org once (sha256 of the torch wheel =
`cd3232a5…`, identical to f12's install) and reached f5 and Raptor over the farm LAN.

On every box the old torch/torchvision/torchaudio folders were copied first to
`D:\fleetmap\torch_backup\sp` along with `freeze_before.txt`. Rollback is one command:
`powershell -File D:\fleetmap\torch_backup\restore_torch.ps1` (stop ComfyUI first).

After the upgrade every box logs "DynamicVRAM support detected and enabled". The node
class count is unchanged (f15 2224, f5 2396, Raptor 2098), `wfcheck37.py` finds no
advertised workflow with a missing class, and numpy is unchanged (2.3.4 on f5/f15,
2.2.6 on Raptor, as before). The only custom-node import failure is
`comfyui-reactor`, which already failed before the upgrade.

f15's driver: the official `610.62-desktop-win10-win11-64bit-international-dch-whql.exe`
(978,406,136 bytes, Authenticode "NVIDIA Corporation" valid, sha256 `DFE395BB…`) was run
with `-s -noreboot -noeula -clean`, followed by a reboot. The installer returned exit
code 1, but the driver is bound (pnputil: `nv_dispi.inf` 32.0.16.1062 = 610.62) and CUDA
works. **`nvidia-smi.exe` is not in System32 on f15** (it exists only under
`DriverStore\FileRepository\nv_dispi.inf_amd64_6f3cfb…`). After the reboot, the
autologon, ComfyUI (Startup shortcut), the LAN model server, advertise, LoRA sync, the
queue watchdog and the VPS tunnel all came back within about 1 minute.
`renderfin_pinger` reports 0xE0434352, the same code as before the reboot.

## Summary matrix (warm time, × vs worker-4090 warm)

| Model (case) | worker-4090 | f12 | Raptor | f15 | f5 |
|---|---|---|---|---|---|
| **MiniMax H3**, 158 f 960×544 | 81.6 s (ref) | ✅ 137 s · 1.7× | ✅ 127 s · 1.6× | ✅ 192 s · 2.4× | ✅ 192 s · 2.4× (after reboot) |
| **LTX-2.5** std, 153 f 960×540 (owner case) | 39.1 s (ref) | ✅ 66 s · 1.7× | ✅ 66 s · 1.7× | ⚠️ 130 s · 3.3× only with `--cache-ram 6`; default launcher trips the RAM guard | ✅ 92 s · 2.4× (after reboot, default launcher) |
| LTX-2.5 std, 97 f 960×540 | 46.0 s (ref) | ✅ 81 s · 1.7× | ✅ 43 s · 0.9× | ✅ 93 s · 2.0× | ❌ blocked: host RAM |
| LTX-2.5 HQ (two-stage), 97 f | 22.9 s (ref) | ✅ 75 s · 3.3× | ✅ 47 s · 2.0× | ❌ 117 s · 5.1× | ❌ blocked |
| Krea 2 Turbo 1024² | 7.7 s (ref) | ✅ 25 s · 3.2× | ✅ 17 s · 2.2× | — | — |
| Z-Image Turbo 1024² | 5.0 s (ref) | ✅ 8.2 s · 1.6× | ✅ 8.6 s · 1.7× | ✅ 12.8 s · 2.6× | ⚠️ 23.5 s · 4.7× (1 of 2 runs tripped the guard) |
| Z-Image + ControlNet pose 1024² | 8.1 s (ref) | ✅ 16 s · 2.0× | ✅ 14 s · 1.8× | ✅ 21 s · 2.6× | ❌ guard trip |
| FLUX.2 klein 4B multi-ref (2 refs) | 5.5 s (ref) | — | ✅ 9.8 s · 1.8× | ✅ 14.8 s · 2.7× | ⚠️ 15.5 s · 2.8× (cold run tripped the guard) |
| Qwen-Image-Edit-2511 Q3_K_S (2 refs, 20 steps) | — (no ComfyUI-GGUF) | — (no file, no GGUF node) | ✅ 305 s (no reference; the fastest box) | ❌ ~380 s, then the guard trips at step 16/20 (1.4 GB free) | ❌ guard trip |

Recommended advertisement (for the video agent, via the coordinator; nothing has
been advertised by this map):

| Box | H3 (`gen_video_minimax_h3_by_url.json`) | LTX-2.5 std | LTX-2.5 HQ |
|---|---|---|---|
| worker-4090 | yes | yes | yes |
| f12 | yes | yes | yes |
| Raptor | yes (new: H3 files are there, sha-checked) | yes | yes |
| f15 | yes | yes, **after** its launcher gets `--cache-ram 6` | no (5.1×) |
| f5 | yes | yes (default launcher; `--cache-ram 6` optional, 96 s) | not measured after reboot (f15 twin: 5.1×, so no) |

## Per-model tables

"cold / warm" are execution seconds. Sampling and decode are from the warm run.
RAM = worst free system RAM (GB). VRAM = peak used (GB, whole card).

### MiniMax H3 fl2va pruned int8 + turbo 4-step LoRA, 158 f 960×544

| Box | cold | warm | × | sampling | decode (video) | min RAM free | peak VRAM | Stability | SSIM vs 4090 |
|---|---|---|---|---|---|---|---|---|---|
| worker-4090 | 111 s (no reserve) / 80 s* | 81.6 s | 1.0 | 56.6 s | 15.8 s | 19.3 of 63 | 20.2 of 24 | ok | ref |
| f12 | 139 s | 137 s | 1.7 | 86.8 s | 24.5 s | 5.2 of 32 | 11.4 of 12 | ok | 0.976 |
| Raptor (torch 2.9.1) | 142 s | 127 s | 1.6 | 86.5 s | 26.6 s | 23.6 of 64 | 11.8 of 12 | ok | 0.976 |
| f15 (torch 2.9.1) | 198 s | 192 s | 2.4 | 132 s | 37.0 s | 4.7 of 32 | 7.7 of 8 | ok | 0.738 (same shot, see note) |
| f15 (torch 2.7.1, before) | guard trip in the text encoder (1.9 GB free) | | | | | | | ❌ | |
| f5 (before reboot) | guard trip: 1.4-1.9 GB free before the model even loads | | | | | | | ❌ host RAM | |
| f5 (after reboot, torch 2.9.1) | 197 s | 192 s | 2.4 | 133.5 s | 37.2 s | 6.7 of 32 | 7.9 of 8 | ok | 0.778 |

\* 80 s: that 4090 run found the loaders already warm from a production H3 job.

The 4090 with `--reserve-vram 0` (before 06:15 local) ran a production H3 job at
567 s/step because the driver spilled 4.3 GB of ComfyUI allocations into shared
system memory (WDDM sysmem fallback; desktop apps hold about 2 GB of the 24 GB). The
worker now starts with `--reserve-vram 3 --vram-headroom 3` and the same graph runs at
about 14 s/step. The NVIDIA "CUDA - Sysmem Fallback Policy = Prefer No Sysmem Fallback"
setting would stop the silent slowdown for good; that is the owner's call.

Quality: f12 and Raptor (same GPU model) give **bit-identical** output (SSIM 1.000), and it
matches the 4090 at 0.976. f15 (8 GB, a different offload split) differs more by SSIM, but
the contact sheet shows the same shot, motion and face identity. H3 holds the face across
all 158 frames on every box.

### LTX-2.5 distilled int8, 153 f 960×540 (owner case)

| Box | cold | warm | × | sampling | decode | min RAM free | peak VRAM | Stability | SSIM vs 4090 |
|---|---|---|---|---|---|---|---|---|---|
| worker-4090 | 38.1 s | 39.1 s | 1.0 | 22.1 s | 12.8 s | 29.8 | 19.9 | ok | ref |
| f12 | 103 s | 66 s | 1.7 | 36.5 s | 18.7 s | 6.4 | 11.5 | ok | 0.717 |
| Raptor (torch 2.9.1) | 119 s | 66 s | 1.7 | 34.8 s | 18.0 s | 27.4 | 11.4 | ok | 0.717 |
| Raptor (torch 2.7.1, before) | 309 s | 330 s | 8.4 | 112 s | 204 s | 13.8 | 11.1 | ok but ❌ by time | 0.711 |
| f15, default launcher | sampling done (72 s), then the RAM guard trips (1.94 GB free) | | | | | 1.9 | 7.5 | ❌ | |
| f15, `--cache-ram 6` | 179 s | 130 s | 3.3 | 93.2 s | 25.5 s | 3.2 | 7.2 | ok | 0.721 |
| f15 (torch 2.7.1, before) | guard trip in 4 s (0.9 GB free) | | | | | | | ❌ | |
| f5 (before reboot) | guard trip | | | | | 1.5 | | ❌ host RAM | |
| f5 (after reboot), default launcher | 144 s | 92 s | 2.4 | 55.3 s | 25.6 s | 6.4 | 7.6 | ok | 0.721 (bit-identical to f15) |
| f5 (after reboot), `--cache-ram 6` | 150 s | 96.5 s | 2.5 | – | – | 5.1 | 7.6 | ok | |

`--cache-ram 6` is ComfyUI 0.37's RAM-pressure cache: it drops cached models once free
RAM falls below 6 GB, so the Gemma text encoder and the DiT are not both held while the
decode runs. It needed no download and did not slow H3 on f15 (194 s vs 192 s). The
decode is already temporal-tiled (`VAEDecodeTiled` 512/64, temporal 64/16), injected by
the deployed decode change. Nothing else was needed: the larger pagefile and a smaller
Gemma build were not required.

Quality: the 3080 Ti boxes are bit-identical to each other (f12 vs Raptor SSIM 1.000).
Same-seed LTX output on Ampere differs from Ada (SSIM 0.72). This is a different sample,
not a degraded one: the 4090 does the push-in to a close smile, while on the Ampere boxes
hair sweeps over the face. Face identity drifts on every box with LTX. H3 does not drift.

### LTX-2.5 standard 97 f and HQ two-stage 97 f (960×540)

| Box | std cold / warm | std × | HQ cold / warm | HQ × | min RAM free (std / HQ) |
|---|---|---|---|---|---|
| worker-4090 | 45.5 / 46.0 s | 1.0 | 48.0 / 22.9 s | 1.0 | 22.4 / 28.3 |
| f12 | 64.8 / 80.5 s | 1.7 | 90.6 / 74.9 s | 3.3 | 6.5 / 6.3 |
| Raptor (2.9.1) | 96.7 / 42.9 s | 0.9 | 53.1 / 46.9 s | 2.0 | 30.4 / 32.5 |
| Raptor (2.7.1, before) | 255 / 426 s | 9.3 | – | – | 13.1 |
| f15 (2.9.1, default launcher) | 146 / 93 s | 2.0 | 124 / 117 s | 5.1 | 3.0 / 2.7 |
| f5 | guard trip | – | – | – | 1.9 |

Earlier numbers by the video agent (`docs/ltx25-migration-20260923.md`) agree: f12
90 s cold / 50 s warm, 4090 46 s. They also recorded f15 at 49 frames taking 811 s with
252 MB of RAM free, which was the torch 2.7 legacy path.

### Krea 2 Turbo fp8, 1024², 8 steps

| Box | cold | warm | × | min RAM free | peak VRAM | SSIM vs 4090 |
|---|---|---|---|---|---|---|
| worker-4090 | 14.4 s | 7.7 s | 1.0 | 37.7 | 19.2 | ref |
| f12 | 27.5 s | 25.0 s | 3.2 | 13.5 | 11.4 | 0.766 |
| Raptor (2.9.1) | 27.8 s | 17.2 s | 2.2 | 46.5 | 11.2 | 0.766 |
| Raptor (2.7.1, before) | 57.4 s | 22.1 s | 2.9 | 31.5 | 10.8 | |
| f15, f5 | — `krea2_turbo_fp8_scaled` not installed (image agent's placement) | | | | | |

### Z-Image Turbo fp8, 1024², 8 steps, and with Fun ControlNet Union 2.1 pose

| Box | t2i cold / warm | × | pose cold / warm | × | min RAM free | SSIM vs 4090 (t2i / pose) |
|---|---|---|---|---|---|---|
| worker-4090 | 11.6 / 5.0 s | 1.0 | 10.5 / 8.1 s | 1.0 | 30.2 | ref |
| f12 | 15.2 / 8.2 s | 1.6 | 27.6 / 16.0 s | 2.0 | 13.6 | 0.969 / 0.996 |
| Raptor (2.9.1) | 20.2 / 8.6 s | 1.7 | 33.7 / 14.4 s | 1.8 | 34.3 | 0.969 / 0.996 |
| Raptor (2.7.1, before) | 74 / 9.8 s | 2.0 | – | – | 31.2 | |
| f15 (2.9.1) | 26.0 / 12.8 s | 2.6 | 39.3 / 21.1 s | 2.6 | 2.8 | 0.975 / 0.994 |
| f5 (2.9.1) | guard trip / 23.5 s | 4.7 | guard trip both | ❌ | 0.2-2.0 | |

### FLUX.2 klein 4B multi-reference, 2 refs, 1024², 4 steps

| Box | cold | warm | × | min RAM free | SSIM vs 4090 |
|---|---|---|---|---|---|
| worker-4090 | 23.1 s | 5.5 s | 1.0 | 36.8 | ref |
| Raptor (2.9.1) | 70.5 s | 9.8 s | 1.8 | 45.8 | 0.996 |
| f15 (2.9.1) | 59.2 s | 14.8 s | 2.7 | 8.0 | 0.998 |
| f5 (2.9.1) | guard trip | 15.5 s | 2.8 | 2.0 | |
| f12 | — klein 4B, its fp4 Qwen3 encoder and the FLUX.2 VAE are not installed | | | | |

### Qwen-Image-Edit-2511 GGUF Q3_K_S, 2 refs, 1024², 20 steps, CFG 2.5

| Box | cold | warm | min RAM free | Result |
|---|---|---|---|---|
| Raptor (2.9.1) | 313 s | 305 s | 39.7 | ok (sampling 294 s; no Lightning LoRA in the template) |
| f15 (2.9.1) | 159 s, guard trip at 0.6 GB | – | 0.6 | ❌ |
| f15, `--cache-ram 6` | about 380 s, guard trip at step 16/20 (1.4 GB) | stopped by hand after 27 min at step 13/20 (VRAM thrash, 6 MB free) | 1.4 | ❌ |
| f5 | guard trip | – | 0.2 | ❌ |
| worker-4090, f12 | — no `UnetLoaderGGUF` node (and no file on f12) | | | |

With no 4090 reference, the rule cannot be applied as written. Raptor is the only box that
runs it, at about 5 minutes an image. The template's 20 steps without the installed
`Qwen-Image-Edit-2511-Lightning-4steps` LoRA is the obvious lever; that is the image agent's call.

## f5: host RAM (resolved by a reboot)

After the torch upgrade f5 matched f15 in software but had only about 1.8 GB of RAM free at
idle: **`AdGuardVpnSvc` held 9.3 GB working set** (a leak after 66 days of uptime), so almost
every job tripped the 2 GB guard. The owner had f5 rebooted. Afterwards AdGuard used 0.06 GB
and 26 GB of RAM was free. The video tests were then re-run: H3 158 f warm 192 s (2.4×) and
LTX-2.5 153 f warm 92 s (2.4×), both on the **default launcher** with at least 6.4 GB free.
f5's LTX output is bit-identical to f15's, which is the same card.
f15 only needs `--cache-ram 6` because of its own resident load: the Freestock embeddings
worker, and more. The image rows for f5 are from before the reboot. Re-run them the next
time f5 is idle.

## Distribution done for this map (download once per site)

* H3 weights for the farm: downloaded once from `Comfy-Org/MiniMax-H3` and
  `lightx2v/Minimax-h3-Turbo` onto f15. Parallel 256 MB ranges were needed because the
  single-stream curl with `--retry` rewinds to zero on every reset.
  Copied f15 → f5 and f15 → Raptor over the farm LAN (`http://192.168.0.115:18998`).
  sha256 checked on every box:
  pruned DiT `e889202c…`, TE `35a88d51…`, video VAE `7c1f1314…`, audio VAE `8e505d95…`,
  turbo LoRA `c8168ebc…`.
* Home site: worker-4090 `C:\AIModels` → f12 over the home LAN (scp, about 55 MB/s),
  sha256 checked.
* torch 2.9.1+cu130 wheels: once onto f15, then over the LAN to f5 and Raptor. NVIDIA
  610.62: once onto f15 (only f15 needed it).
* The `AutoRig Models LAN Server` task on f15 died on the first request both times it
  was started (05:05, 06:08; the clients got a connection reset, and the task result is 1).
  The likely cause: the script runs with `$ErrorActionPreference = 'Stop'` and pipes
  python's stderr (where `http.server` writes its access log) through `*>>`. For the copies
  I ran a plain `python -m http.server` bound to 192.168.0.115 instead. Since the reboot
  the task is running again, but it will probably die the same way. Fix: drop `Stop`, or
  let cmd redirect stderr to a file.

## Needed changes (for the owning agents; not made here)

1. **f15 (and f5 once its RAM is fixed): add `--cache-ram 6` to the ComfyUI launcher.**
   This is what makes LTX-2.5 at 150 f fit in 32 GB. Then advertise
   `gen_animation_ltx25_by_url.json` and `gen_video_minimax_h3_by_url.json` on f15.
2. Advertise H3 on Raptor (files present). Keep LTX-2.5 std + HQ on Raptor and f12.
3. The H3 production template needs no change for 960×540/150 f (158 f and 960×544 are
   chosen automatically). The test copy dropped only the optional end guide.
4. f15: `nvidia-smi.exe` is missing from System32 after the driver install. Re-running
   the 610.62 installer without `-clean` should restore it.
5. f5: fix the AdGuard VPN memory use (owner), then re-run this map for f5.
6. worker-4090: keep `--reserve-vram 3`, or set "Prefer No Sysmem Fallback".

## GTX 1080 Ti converter boxes (f1, f2, f7, f11, f13): image generation, rejected

Pilot on f11 (GTX 1080 Ti 11 GB, driver 576.80, 32 GB RAM) on 2026-09-23. It ran an
isolated ComfyUI 0.37.0 (`C:\AI\ComfyUI1080`) on a copy of the Hunyuan runtime's Python
(torch 2.8.0+cu126, which ships sm_61). Scripts: `deploy/onlyrender/comfy1080*` (commit
e3dbedce). Tests: 1024², the approved files, warm runs, and the same `comfy1080_bench.py`
run against worker-4090's ComfyUI as the reference.

| Test (1024²) | f11 1080 Ti | worker-4090 | Ratio | ≤5× |
|---|---|---|---|---|
| Z-Image Turbo t2i, 8 steps | 89–92 s (100–165 s cold) | 5.3–6.1 s | ~15× | FAIL |
| FLUX.2 klein 4B t2i, 4 steps | 64–66 s | 13.0–14.3 s | ~4.7× | borderline |
| klein, 1 reference (edit) | 106 s | 15.0 s | ~7× | FAIL |
| klein, 2 references | 130–166 s | 17.1 s | ~8–10× | FAIL |

On f11, VRAM peaked at 9.5–10.8 GB, which is the whole card: an image job cannot share the
GPU with the LLM (Bonsai takes ~9.3 GB) or with Hunyuan. RAM peaked at 8–13 GB. The images
were correct (Z-Image, klein, and the 2-reference composite were checked).

**Pascal finding: PyTorch SDPA and xformers inside ComfyUI reset the display driver.**
With ComfyUI's default attention (PyTorch SDPA, or the xformers that ships in the Hunyuan
Python), the first Z-Image sampling step aborts the process in `scaled_dot_product_attention`,
and Windows logs nvlddmkm Xid 13 ("Graphics Exception") plus a TDR (Display 4101,
LiveKernelEvent 141). This happened twice on f11 and does not depend on fp32/fp16 or on
DynamicVRAM. The same SDPA calls run fine in a standalone script, so the root cause is not
isolated. The stable set is `--use-quad-cross-attention --force-fp32 --disable-xformers
--disable-cuda-graphs --disable-comfy-compiler --disable-async-offload`, with DynamicVRAM
left on (without it Z-Image loads only half its weights and cold runs take ~165 s). fp16
is no faster than fp32 on Pascal. Z-Image is compute-bound at ~10 s/step, so a GGUF or
fp16 file would not bring it under 5×.

**Decision (owner, 2026-09-23): no image generation on the 1080 Ti boxes.** Nothing was
registered in renderfin and no tunnel was added. On f11 the worker cannot start: no
scheduled task, a `C:\ProgramData\AutoRig\comfy1080\DISABLED` flag, no process. The
converter is healthy (build 60101f4b, drift clean, idle, LLM installed and not resident).
The GPU guard (`comfy1080_guard.py`: gate on converter idle, 423 `gpu_leased`, preempt +
hidden history so renderfin requeues without spending an attempt) was verified live and
stays in the repo for reference only.

Removal candidate (34.3 GB on f11 C:), owner's call. Run on f11 as an administrator:

```powershell
Remove-Item -Recurse -Force C:\AI\ComfyUI1080, C:\ProgramData\AutoRig\comfy1080
```

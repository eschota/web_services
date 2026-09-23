# Converter farm disk cleanup, 2026-09-23

Boxes covered: f1, f2, f7, f11 and f13, the GTX 1080 Ti converter and LLM
boxes. This was a read-only inventory. The agent deleted nothing and moved
nothing. The owner runs one script per box from
`autorig-online/deploy/converter-cleanup/`. Each script is also staged on its
box at `C:\ProgramData\AutoRigCleanup\<box>-cleanup-20260923.ps1`, and the
staged copy's SHA-256 matches the repo copy.

## Headline

Most of the space goes to one thing. On 19 Sep 2026, between 17:16 and 00:46,
someone ran a file-search script (`C:\Users\Public\rc_find3.ps1`, looking for
`cat_runner`, `multik`, `wondercat`...) on f1, f2, f11 and f13. It wrote full
recursive listings of `C:\` to `C:\Users\Public\list_C.txt`: **65.0 GB on f1,
27.0 GB on f2, 43.0 GB on f11 and 43.6 GB on f13** (plus `rc_C.txt`, 0.1 to
0.25 GB). The files are plain text and hold path lines only (checked head and
tail). Nothing has them open and nothing references them. They explain f1's
0.9 to 4.6 GB free, and they are not in the converter's storage path, so the
converter's own LRU could never reclaim them.

The converter's built-in cleanup is working as designed.
`periodic_cleanup()` in `webserver_converter_glb.py` runs hourly. When free
space drops below 50 GB (hard-coded `min_free_bytes`), it evicts the oldest
top-level items in `C:\NDLWebServerBuild\wwwroot\converter\glb`. It never
evicts items younger than `GLB_CLEANUP_MIN_ITEM_AGE_HOURS` (24 h) or tasks
that are still active. On f1 and f13 that LRU has already cut task storage
down to the last ~24 h. **No tuning is recommended.** Lowering the 24 h floor
would only shorten the download window. Once the dumps are gone, free space
rises above 50 GB and the LRU stops evicting, so finished deliverables stay
on the farm longer.

## Expected result (dry runs at 07:30 to 07:45 UTC)

| box | free now | freed by default `-Apply` | free after (approx.) | optional `-IncludeRecycleBin` |
|---|---:|---:|---:|---:|
| f1  |   4.0 GB | **80.1 GB** | ~84 GB | 0 |
| f2  |  49.9 GB | **40.1 GB** | ~90 GB | +10.1 GB |
| f7  | 100.6 GB | **5.3 GB** | ~106 GB | +4.2 GB |
| f11 |  81.2 GB | **52.9 GB** | ~134 GB | +1.4 GB |
| f13 |  14.5 GB | **56.6 GB** | ~71 GB | 0 |
| **total** | | **~235 GB** | | +15.7 GB |

f1 alone gets back about 80 GB, far more than the ~1.1 GB that the blocked
LLM system-prompt deploy and the image-generation pilot need.

## Owner commands (one line per box, run from the owner PC)

Each run deletes, logs every removed path and the freed GB, and prints a
summary. Drop `-Apply` for a dry run. The script refuses to delete while the
converter reports processing, pending or queued tasks. It waits up to 45
minutes and rechecks before each category (`-WaitIdleMinutes 120` extends the
wait). f1 and f13 are busy most of the time, so if a run ends with `REFUSE`,
rerun it or use a longer wait.

```bash
ssh autorig-vps "sudo -n ssh -i /srv/autorig/secrets/ssh/renderfin_farm_tunnel -p 45132 user@5.129.157.224 powershell -NoProfile -ExecutionPolicy Bypass -File C:/ProgramData/AutoRigCleanup/f1-cleanup-20260923.ps1 -Apply"
ssh autorig-vps "sudo -n ssh -i /srv/autorig/secrets/ssh/renderfin_farm_tunnel -p 45279 user@5.129.157.224 powershell -NoProfile -ExecutionPolicy Bypass -File C:/ProgramData/AutoRigCleanup/f2-cleanup-20260923.ps1 -Apply"
ssh -i ~/.ssh/glb_converter_farm -p 45131 autorigrescueadmin@5.129.157.224 "powershell -NoProfile -ExecutionPolicy Bypass -File C:/ProgramData/AutoRigCleanup/f7-cleanup-20260923.ps1 -Apply"
ssh autorig-vps "sudo -n ssh -i /srv/autorig/secrets/ssh/renderfin_farm_tunnel -p 45533 user@5.129.157.224 powershell -NoProfile -ExecutionPolicy Bypass -File C:/ProgramData/AutoRigCleanup/f11-cleanup-20260923.ps1 -Apply"
ssh autorig-vps "sudo -n ssh -i /srv/autorig/secrets/ssh/renderfin_farm_tunnel -p 45267 user@5.129.157.224 powershell -NoProfile -ExecutionPolicy Bypass -File C:/ProgramData/AutoRigCleanup/f13-cleanup-20260923.ps1 -Apply"
```

Append ` -IncludeRecycleBin` inside the quotes to empty the `user` Recycle Bin
as well. That is an owner decision: f2's bin holds 168k items (10.1 GB)
deleted up to 2026-06-24.

Logs: `C:\ProgramData\AutoRigCleanup\<box>-cleanup-<timestamp>-APPLY.log`.
Dry-run logs from this audit are there too.

Post-run health check (from the owner PC):

```bash
scp autorig-online/deploy/converter-cleanup/verify-farm-health.sh autorig-vps:/tmp/ && ssh autorig-vps bash /tmp/verify-farm-health.sh
```

Baseline before cleanup: all 5 VPS tunnels were up; drift was clean(0)
everywhere; server version was `24_08_2026_06_hunyuan_slot_accounting`;
preflight was healthy on f1, f2, f7 and f13, and `not_run` on f11. The LLM was
installed and enabled on f1, f2, f11 and f13 (f7 has none). Disk use was 99.3%
on f1, 97.1% on f13, 89.2% on f2, 82.6% on f11 and 78.4% on f7. One
pre-existing oddity: public `https://converter-f11.freestock.online/...`
answers "Node tunnel is offline". The autorig VPS tunnel (127.0.0.1:15533)
works.

## What each script deletes (default categories)

All paths are explicit, with fixed roots. A file is skipped if it is a
reparse point, resolves outside its root, or is open for writing by another
process. Read-only handles such as nginx `open_file_cache` do not count as
open for writing, and Windows removes the file when they close, exactly as
the converter's own `rmtree` does.

| # | category | exact path / pattern | guard | why safe |
|---|---|---|---|---|
| 1 | diagnostic dumps | `C:\Users\Public\list_C.txt`, `rc_C.txt`, `rc_find3.ps1` (f1, f2, f11, f13) | not modified for 1 day | one-off search output from 19 Sep, not referenced |
| 1 | stale log (f11 only) | `C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer\occonvert.log` (2.33 GB) | not modified for 30 days (last write 2026-06-22), not open | dead OCConvert log |
| 1b | audit scratch | `C:\ProgramData\cc-inventory\*` | none | KB-sized inventory scripts from this audit |
| 2 | Blender autosaves | `C:\Users\user\AppData\Local\Temp\*_autosave.blend` | untouched for 6 h | written by converter Blender runs (`*_model_prepared*_autosave.blend`, f7 `startup_scene_*_autosave.blend`); never read back |
| 3 | OpenPose glog logs | `...\Temp\OpenPose.*.log.*` | older than 24 h | one INFO/WARNING log per OpenPose run, closed when the run ends |
| 4 | WER crash dumps | `C:\Users\user\AppData\Local\CrashDumps\*.dmp` | older than 14 days | f7 Unity dumps from the session-0 incident fixed on 22 Sep; f11 3ds Max/Unity dumps from 24 Aug; f13 python/toolbag. The f11 llama-server dump from 22 Sep is kept |
| 5 | pip cache | `C:\Users\user\AppData\Local\pip\cache\**` | skipped while any pip process runs | download/wheel cache only; installed packages are untouched |
| 6 | converter ZIP bundles mirrored on autorig.online | `C:\NDLWebServerBuild\wwwroot\converter\glb\<guid>.zip` (+ `.zip.meta.json`), explicit GUID list | size **and SHA-256** must equal the copy in `/srv/autorig/data/artifact-cache/<task>/`; skipped if the task is active or the zip is being written | the site serves downloads from the artifact cache first (`lookup_cached_artifact` in `main.py`), and `run_retention` never deletes the last long-lived copy of a zip; the farm copy is redundant |
| 7 | Recycle Bin (opt-in) | `C:\$Recycle.Bin\<user SID>\**` | `-IncludeRecycleBin` only | owner decision |

About the ZIP list (category 6): the VPS side was checked on 2026-09-23. It
used `autorig.db` (`tasks.guid`) and each artifact-cache `manifest.json`
entry whose `source_url` ends in `/<guid>.zip` on that box, with the file
present and its size matching. Mirrored ZIPs: f1 29 (10.81 GB), f2 11
(5.64 GB), f13 14 (6.57 GB), f7 0, f11 0. Only whole `.zip` bundles are
touched. The task folders stay, because the VPS manifests mirror only ~9 of
~108 files per task.

## Per-box inventory

Sizes are in GB, as of 2026-09-23 ~07:20 UTC. "Keep" means the item is not in
the script.

### f1 (F1-PC): C: 465 GB, 0.9 to 4.6 GB free (fluctuating as the LRU evicts)

| category | GB | safe? | why |
|---|---:|---|---|
| `C:\Users\Public\list_C.txt` + `rc_C.txt` | 65.27 | **yes** | search dumps (see headline) |
| Temp: Blender autosaves (346 files, 16 to 23 Sep) | 2.39 | **yes** | converter leftovers |
| Temp: OpenPose glog logs (7808 files) | 1.67 | **yes** | per-run logs |
| converter ZIPs mirrored on VPS (29) | 10.81 | **yes** | byte-identical copy on autorig.online |
| converter task storage: 34 dirs + 4 unmirrored zips, all < 48 h | ~26 | keep | converter LRU territory; some files exist only here |
| Unity project `Library` caches (OC_VR 14.1, OC_Android_APK 12.7, OC_WebBuild 5.7, HDRP_Scenes_to_Video 3.7, OC_HDRP 3.2, HDRP_Generate_Video 2.9, OC_URP 2.1) | 43.9 | no | regenerable, but deleting forces an hours-long reimport; HDRP_Generate_Video runs on every rig task. Owner call for the OC_* projects if OCConvert is retired |
| `%LOCALAPPDATA%\AutoRig\migration-backups\historical-runtime-git_20260811T204621Z` | 5.84 | **no** | may be the only copy of the deployed build history (23698a66 and others are missing from GitHub) |
| `%LOCALAPPDATA%\AutoRig\deploy\release*`, `rollback`, `staging` | ~0.4 | no | deploy protocol v3 state; tiny |
| `C:\AI\HY3D2` (68.8), `.cache\huggingface` (9.3), `ProgramData\AutoRig\ai-vision` (15.7) | 93.8 | excluded | models, owned by the model-cleanup agent |
| `AppData\LocalLow\Unity\Caches` | 6.95 | report | Unity AssetBundle cache from 2025-07; owner call |
| Freestock app `runtime\update_staging` + `update_downloads` | 5.14 | report | other app's updater (Freestock), not converter |
| NVIDIA DXCache / GLCache | 5.06 | report | driver shader cache, in use, regrows (cap it in the NVIDIA control panel) |
| Cursor roaming (`workspaceStorage` embeddings, `state.vscdb.backup`) | 4.18 | report | owner's editor |
| `Downloads` installers (NVIDIA 576.80 driver 0.81, PowerToys, etc.) | 2.28 | report | owner's files |
| `converter.log` 0.65 + `scheduled_converter_service.log` 0.45 | 1.10 | no | open by the running converter; needs rotation in code (plain `FileHandler`) |
| nginx logs (`access.log` + rotated .gz) | 0.82 | no | managed by the `AutoRig Nginx Access Log Rotation F1` scheduled task |
| `C:\$WinREAgent` | 3.93 | report | Windows Update staging; see Windows section |
| shadow copies (VSS) | 5.28 | keep | used for the f12 recovery; cap 9.31 GB |
| other owner projects (MascotFarm 5.0, PhysicallRagdollFarm 5.3, VR_TEST_2 8.8, VR_TEST 1.7, autorig_preview_repair 1.6, openpose 1.5) | ~24 | out of scope | not converter |
| Recycle Bin, WER, Minidump, SoftwareDistribution | ~0 | n/a | empty |

### f2 (F2-PC): C: 465 GB, 49.9 GB free

| category | GB | safe? | why |
|---|---:|---|---|
| `list_C.txt` + `rc_C.txt` | 27.23 | **yes** | search dumps |
| Temp: Blender autosaves (276, 2 to 23 Sep) | 2.51 | **yes** | |
| Temp: OpenPose logs (6980) | 1.49 | **yes** | |
| pip cache (950 files) | 3.27 | **yes** | |
| converter ZIPs mirrored on VPS (11) | 5.64 | **yes** | sha-verified |
| converter task storage: 132 dirs 86.8 + 101 unmirrored zips ~43.7; 3 to 23 Sep | ~130 | keep | 55 done tasks have VPS cache `expired`, so the farm is the last copy; LRU manages it |
| Recycle Bin (`user`, 168k items, to 2026-06-24) | 10.10 | opt-in | owner decision |
| migration-backups historical-runtime-git | 6.95 | no | history backup |
| Unity HDRP_Generate_Video Library 2.8, test_builds OC_VR backup 0.7 | 3.5 | no | pipeline project / build artefact |
| `C:\Windows\Logs\CBS` | 1.22 | report | Windows; cleanmgr |
| `$WinREAgent` | 3.93 | report | Windows Update staging |
| `AppData\Roaming\mamba\pkgs` (pytorch tarball 1.35) | ~1.4 | report | conda package cache of an owner environment |
| NVIDIA DXCache/GLCache | 3.0 | report | |

### f7 (F7-PC): C: 466 GB, 100.6 GB free; D: 200 GB free

| category | GB | safe? | why |
|---|---:|---|---|
| Temp: Blender autosaves (289, 12 Aug to 22 Sep) | 1.06 | **yes** | |
| Temp: OpenPose logs (2276) | 0.47 | **yes** | |
| CrashDumps (10 Unity dumps, 2 Sep) | 0.68 | **yes** | session-0 Unity crash, fixed 22 Sep |
| pip cache (661) | 3.04 | **yes** | |
| converter task storage: 280 dirs 99.6 + 80 zips 33.8, 8 Aug to 22 Sep | ~133 | keep | no VPS mirror for any (81 done with no cache, 19 expired); last copies. 19 `.hunyuan_stage_*` dirs are empty (0 B) |
| Recycle Bin (`user`, 140k items) | 4.22 | opt-in | owner decision |
| `C:\Windows\LiveKernelReports` (ResourceTimeout dumps, July) | 1.75 | report | Windows; cleanmgr "System error memory dump files" |
| Cursor: `state.vscdb.corrupted.*` 2.23, `.cursor\snapshots` pack 4.54 | 6.8 | report | owner's editor |
| migration-backups historical-runtime-git | 6.75 | no | |
| Temp older than 7 d (`t5avp2yk` 0.40, `gradio` 0.30, `*.ply` 0.12) | 0.9 | not scripted | unknown owners; small |
| `$WinREAgent` 3.93, CBS logs 1.22 | 5.2 | report | Windows |
| UE 5.7 DerivedDataCache 1.69 (Program Files) | 1.7 | excluded | Program Files |

### f11 (F11-PC): C: 465 GB, 81.2 GB free

| category | GB | safe? | why |
|---|---:|---|---|
| `list_C.txt` + `rc_C.txt` | 43.08 | **yes** | search dumps |
| `occonvert.log` (stale since 2026-06-22, not open) | 2.33 | **yes** | dead log |
| Temp: Blender autosaves (239, 12 to 19 Sep) | 2.10 | **yes** | |
| Temp: OpenPose logs (6326) | 1.35 | **yes** | |
| CrashDumps > 14 d (3ds Max 1.09, Unity, powershell; 24 Aug) | 1.45 | **yes** | the llama-server dump from 22 Sep is kept |
| pip cache (382) | 2.58 | **yes** | |
| converter task storage: 31 dirs 22.3 + 27 zips 14.5, 18 to 19 Sep | ~37 | keep | all VPS caches `expired`, so the farm is the last copy |
| `pagefile.sys` | 40.0 | report | system setting; could be capped (e.g. 16 GB) by the owner |
| `C:\AI\ComfyUI1080` (download `qwen_3_4b.safetensors.part` in progress) | - | excluded | the image-generation agent is installing right now |
| Recycle Bin (241 items) | 1.38 | opt-in | |
| migration-backups historical-runtime-git | 7.53 | no | |
| Unity OC_URP 2.6 / OC_VR 2.0 / HDRP_Generate_Video 2.7 Library | 7.3 | no | |
| Downloads installers (two NVIDIA drivers 1.6, Docker 0.57...) | 2.84 | report | owner's files |

### f13 (F13-PC): C: 465 GB, 14.5 GB free

| category | GB | safe? | why |
|---|---:|---|---|
| `list_C.txt` + `rc_C.txt` | 43.72 | **yes** | search dumps |
| Temp: Blender autosaves (177) | 1.76 | **yes** | |
| Temp: OpenPose logs (4162) | 0.89 | **yes** | |
| CrashDumps > 14 d (python 0.51, toolbag) | 1.06 | **yes** | |
| pip cache (522) | 2.59 | **yes** | |
| converter ZIPs mirrored on VPS (13 present, 1 already evicted) | 6.57 | **yes** | sha-verified |
| converter task storage: 27 dirs 28.9, all < 24 h | ~29 | keep | LRU is already at its 24 h floor |
| `pagefile.sys` | 38.6 | report | system setting |
| `AppData\LocalLow\Unity\Caches` | 3.41 | report | |
| `SoftwareDistribution\Download` (17k files since 2024) | 0.83 | report | Windows Update; cleanmgr |
| migration-backups historical-runtime-git | 6.75 | no | |
| `$WinREAgent` | 4.00 | report | |

## Windows Update / system (report only; never delete WinSxS by hand)

On every box, run these from an elevated prompt when the box is idle:

```text
Dism.exe /Online /Cleanup-Image /AnalyzeComponentStore
Dism.exe /Online /Cleanup-Image /StartComponentCleanup      (no /ResetBase)
cleanmgr.exe /d C:   -> "Clean up system files": Windows Update Cleanup,
                        System error memory dump files, Delivery Optimization
```

These cover `$WinREAgent` (~3.9 GB on f1, f2, f7 and f13), CBS logs (1.2 GB
on f2 and f7), f7 LiveKernelReports (1.75 GB) and f13 SoftwareDistribution
(0.83 GB). Windows Update reboots f7 without warning, so run these at a quiet
time.

## Follow-ups (code, not cleanup)

* Converter `converter.log` / `scheduled_converter_service.log` grow without
  rotation (0.3 to 0.65 GB each). Switch to `RotatingFileHandler`.
* Converter Blender runs leave `*_autosave.blend` in %TEMP% (~2.5 GB per week
  per box). Disable autosave in the converter's Blender launch (for example,
  `bpy.context.preferences.filepaths.use_auto_save_temporary_files = False` in
  the startup script).
* OpenPose writes glog files to %TEMP% on every run (~1.5 GB per week).
  Launch it with `--logtostderr` or point `GLOG_log_dir` at a rotated folder.
* Whoever runs farm-wide file searches should write their output to a
  size-capped file, not to `C:\Users\Public`. The 19 Sep search consumed
  ~180 GB across four boxes and pushed f1 to 0.9 GB free.

# Video Avatar — progress and evidence ledger, 2026-09-22

This document records current evidence and open acceptance gates for the full Video Avatar goal. It is not a completion claim. A successful API response, installed model, still image, or isolated canary does not qualify the product by itself.

## Goal held unchanged

1. Select 5–10 suitable videos with adult people, reproduce their plot and action as closely as the available pipelines allow, tune prompts and controls, compare results, and reject pipelines that damage faces, hands, anatomy, people count, action, camera behavior, or temporal coherence.
2. Provide a server-persisted reusable `Avatar` that can carry the same person into story generation driven by other photos or videos.
3. Create one adult woman Avatar and one adult man Avatar, then use QwertyStock Search API video references to render a coherent cross-shot story with both recurring characters.

## Research and executable specifications

- [Persistent character research](character-consistency-research-20260922.md) compares multi-reference conditioning, adapters, identity LoRA, face embeddings, and the proposed persistent character profile.
- [Video Avatar research](video-avatar-research-20260922.md) defines the executable Wan Animate and LTX 2.3 Union-Control routes, hardware constraints, benchmark policy, persistent Avatar schema, and rejection policy.
- [Reference selection manifest](../deploy/ai-models/video-reference-benchmark.json) records the QwertyStock Search API contract, eight selected catalog videos, the user-provided ninth video, licensing limits, and a proposed two-avatar story.
- [Benchmark run manifest](../deploy/ai-models/video-benchmark-runs.json) contains nine observed-frame prompts and the fixed LTX 2.3 baseline cases.
- Benchmark harness: `autorig-online/tools/video_benchmark.py` prepares bounded source segments, submits resumably, polls without duplicate GPU spending, builds comparison artifacts, and creates manual-review records.

## Current production and implementation state

| Area | Current evidence | Status |
|---|---|---|
| Production | Release `P` is current. The tested node UI behavior introduced in `K1` is carried forward. Release `P` adds short-source video normalization and the repaired LTX guide/predecode path; deployment does not turn canary evidence into media-quality acceptance. | active, not full-goal acceptance |
| Reference inputs | Nine four-second source segments and five-frame contact sheets exist under `.codex_tmp/avatar-video-20260922/reference-review`. Prompts were written from the observed frames. | prepared |
| Benchmark manifest | Nine sources, one LTX 2.3 first-frame baseline, nine cases; fixed seed `9221001`; 97 frames; 960×540 landscape and 540×960 portrait cases. | validated structurally |
| Benchmark batch | Six of nine baseline jobs completed. The first three are recorded in [the sampled baseline review](video-baseline-review-20260922.md). Cases 4–6 received an all-97-frame inspection in [the second-batch review](video-baseline-next3-review-20260922.md). Three cases remain prepared but unrendered. | six completed; no normal-speed continuous acceptance |
| Avatar storage | Browser-created profiles persisted across reload: Maya `av_1c0510ac1ba21445555d2ab1@1` and Leo `av_465777c59b4ec81448c591c6@1`. | persistence demonstrated |
| Avatar canonical images | Maya: `/renderfin/render/default_user/c50e5b12-fe98-4d9a-aee5-197ed4d972b6.png`; Leo: `/renderfin/render/default_user/b9651d0f-7fd7-4e22-82dd-ed87a3820579.png`. | stored inputs, not video acceptance |
| Multi-reference image identity | Raptor canaries `0a56defa` and `015a049c`, plus 4090 canary `71fc1ec1`, passed qualitative still-image identity inspection. Evidence includes `.codex_tmp/avatar_identity_maya_cafe.png` and related artifacts. | image-only evidence; no video identity pass |
| LTX controls | Pose, Depth, and Canny each completed a 25-frame execution canary. Pose also completed an exact 97-frame canary. Release `P` normalizes short sources before guide construction, and a repaired predecode/guide owner-graph run completed as task `2f83c954-dcce-4fd9-96fd-450f5d4c7cf5` from graph `6decd42fc85d`. | live executable path; quality remains capability- and scene-specific |
| Wan Animate | The earlier 25-frame proof exposed tattoo-like driver leakage. A separate Wan-Animate-2 97-frame 960×540 isolated candidate completed in 310.12 seconds without OOM and passed all-frame candidate inspection. Its candidate process was stopped and no live capability was registered. | measured candidate; live promotion pending |
| UI production | Incremental execution, exact-model display, ETA/status, per-node share, graph duplication, A/B anchor and bounded result history are deployed in `K1`. | deployed; bounded production QA recorded |
| Review navigation | Production QA exercised output preview/overlay, current-result sharing, graph copy/reload, and saved-history recovery. | working for tested cases; complete benchmark navigation still open |

## Known render evidence

### Rejected contract result

The first meeting baseline task `b0c1…` produced an incorrect 512×512 output. It is rejected as a contract failure and cannot contribute any quality score.

Rejection reason:

- requested landscape output dimensions were not honored;
- comparing action, faces, or anatomy after a contract failure would produce misleading pipeline evidence.

### Corrected delivery contract, quality still open

Backend-fix task `5c50be57-fa77-4889-8bc1-2551d91c558a` was verified as:

- 960×540;
- 97 decoded frames;
- 24 fps.

This proves the corrected delivery shape and timing only. Continuous visual review is not accepted: the contact evidence shows the later subject standing with the face out of shot and material action drift. The result therefore remains unaccepted for story or Avatar use.

### First three baseline outputs

The first three resumable cases completed with the intended 97-frame contract:

- `walk-and-address-camera`, task `8758e460-1470-4031-8431-50b30a25119c`:
  540×960, 24 fps, 97 frames. Sampled frames preserve the man, hat,
  sunglasses, beard and low selfie composition, but the face expression and
  hand path diverge from the source.
- `seated-speaking`, task `4cf7270f-5e8e-4d69-9f24-a951e514517f`:
  960×540, 24 fps, 97 frames. It is the strongest sampled first-frame result;
  person count, broad identity, static composition and the broad two-hand
  speaking action survive, while exact timing and finger detail remain open.
- `single-hand-gesture`, task `1c42361c-3e40-407b-8438-a4e79438b134`:
  960×540, 24 fps, 97 frames. It is rejected for action fidelity under this
  configuration because the source/prompt thumbs-down becomes thumbs-up.

[The detailed sampled review](video-baseline-review-20260922.md) uses 24 source,
24 generated and 12 paired frames per case. It is not continuous-playback
acceptance and cannot exclude defects between samples.

## Source observations that affect interpretation

The benchmark does not trust catalog titles as ground truth. The actual sampled frames established these constraints:

- `walk-and-address-camera`: sampled action is a low-angle close selfie with hand gestures; walking is not visible in the selected interval.
- `rear-view-walk`: the person appears only at the beginning, then the camera tilts toward a tower. This is mainly a camera-motion stress case, not a facial-identity case.
- `two-person-walk`: a large straw hat and inflatable ring heavily occlude both bodies and most facial detail.
- `single-hand-gesture`: only an arm and hand are visible, so it is a hand-anatomy test.
- `user-meeting`: approximately nine adults are visible. It is an extreme group stress case and must not qualify a two-avatar pipeline.

These limitations are encoded in the run manifest and must remain visible in reports.

## Manual acceptance ledger

Sparse five-frame evidence leaves quality fields `null`. An exhaustive review of
all 97 output frames against the aligned source may record provisional numeric
scores, as cases 4–6 now do, but normal-speed playback is still required for
final temporal acceptance. The first three rows carry only their earlier
sampled disposition; those labels guide the next experiment but do not close
acceptance.

| Case | Faces | Hands/anatomy | Identity | People count | Action | Camera | Temporal | Disposition |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| walk-and-address-camera | null | null | null | null | null | null | null | sampled: retune/control needed; continuous review pending |
| seated-speaking | null | null | null | null | null | null | null | sampled: strongest baseline candidate; continuous review pending |
| single-hand-gesture | null | null | null | null | null | null | null | sampled reject for first-frame action fidelity; continuous review still unperformed |
| face-object-interaction | 4 | 4 | 4 | 5 | 5 | 5 | 4 | all-frame candidate; normal-speed review pending |
| rear-view-walk | null | null | null | null | null | null | null | pending render/review |
| two-person-cafe-conversation | 3 | 3 | 3 | 5 | 2 | 5 | 4 | rejected for action reenactment |
| two-person-walk | null | null | null | null | null | null | null | pending render/review |
| seated-couple-object-action | 4 | 4 | 4 | 5 | 5 | 5 | 4 | all-frame candidate; normal-speed review pending |
| user-meeting | null | null | null | null | null | null | null | pending render/review |

The review record must include defects and a final disposition of `candidate`, `accepted`, or `rejected`. A pipeline is rejected for the affected capability when any principal face becomes unrecognizable, limbs or digits appear/disappear/fuse, identities swap, principal people count changes, props interpenetrate bodies, the requested action is absent or reverses, camera behavior materially changes, or the scene flickers/melts.

## Avatar acceptance gates

Maya and Leo currently prove profile persistence and canonical reference storage. They do not yet prove reusable video identity.

Before an Avatar can be called reusable for production, each profile must pass:

1. Reload and version resolution from server storage.
2. Single-person still-image identity across multiple scenes and angles.
3. Single-person video identity across speaking, gesturing, walking, and object interaction.
4. Face and hand review through continuous playback.
5. Stable clothing/body traits when they are locked by the profile.
6. Driving-video action preservation without importing the source actor's identity.
7. Repeatability from the same immutable profile revision and recorded pipeline parameters.

The existing Raptor and 4090 multi-reference canaries cover gate 2 qualitatively. Gates 3–7 remain open.

## Control-pipeline gates

LTX 2.3 Union Pose, Depth, and Canny have each completed a 25-frame end-to-end execution canary using the declared control route. Pose additionally completed an exact 97-frame canary. These results satisfy the initial execution gate only. Each channel still needs source/output side-by-side review for faces, hands, identity, action, camera and temporal coherence on applicable benchmark cases.

Two public 97-frame control results provide additional bounded evidence:

- Hand Canny task `7d8c2a6a-5d4a-4fc0-a212-4d2f889a5630` used 960×540,
  24 fps, 97 frames, control strength `0.9`, and fixed seed `9221001`.
  Sampled frames retain thumbs-down for most of the clip, improving semantic
  action over the first-frame baseline, but the hand disappears by frame 96
  and fingers become soft/fused. **Rejected for quality.** Evidence is under
  `.codex_tmp/avatar-video-20260922/public-hand-canny*`.
- Seated Pose task `036873da-2664-4738-a968-dc7cbc985729` used 960×540,
  24 fps, 97 frames, control strength `0.85`, and the same seed. Five sampled
  frames show a stable face, striped shirt, two hands and the intended broad
  action. **Sampled candidate only**; this is not continuous-playback
  acceptance. Evidence is under
  `.codex_tmp/avatar-video-20260922/public-seated-pose*`.

These two cases show why controls cannot be admitted as a single blanket
quality claim: Pose can improve broad human action in one scene while Canny can
retain a silhouette yet still fail hand anatomy and end-frame persistence.

Release `P` also closes two execution-contract defects. Short driving sources
are normalized to the requested LTX `8k+1` frame contract before the guide is
built, and the guide/crop chain now selects delivery latents before video VAE
decode. The isolated predecode canary records
`predecode_crop_fix_pass`. Owner graph `6decd42fc85d` subsequently produced the
97-frame video task `2f83c954-dcce-4fd9-96fd-450f5d4c7cf5`. This proves the
repaired route executes through the owner graph; it does not supersede the
scene-specific quality verdicts above.

The earlier Wan proof remains unaccepted because an unwanted tattoo-like detail appeared on the generated forearm, showing that appearance from the driving actor can leak into the target. A newer Wan-Animate-2 isolated candidate completed 97 frames at 960×540 and 24 FPS in 310.12 seconds. Its all-frame inspection found coherent Maya identity, clothing, background, face and two hands with no frozen continuation seam. Peak measurements were 19,619 MiB GPU memory and 56,897 MiB system RAM. This is a measured candidate, not a live product capability: the candidate process was stopped, the stock worker was restored, and production promotion remains pending.

The benchmark comparison is incomplete until at least the viable LTX first-frame/control routes and Wan Animate have comparable artifacts for the applicable source cases. Pipelines that fail a hard gate must be removed from the user-facing production choices for that capability.

## Two-avatar story gate

The proposed story is recorded in the reference manifest, but no final story shot has been rendered. Story production may start only after Maya and Leo each pass single-avatar video identity. Dual-character scenes then require additional proof that:

- the two identities do not swap or blend;
- both faces and bodies remain stable when one actor occludes the other;
- eyelines, object handoffs, and shared props remain coherent;
- wardrobe and scene continuity survive shot boundaries;
- each shot has an output MP4 link, source graph link, exact Avatar revisions, source video ID/URL, prompt, controls, seed, and model versions;
- the final edit is watched continuously as one story, not accepted as isolated still frames.

Current final-story status: **not rendered**.

## Remaining work in execution order

1. Watch the three completed baseline MP4s continuously beside their sources and record formal manual scores without replacing the existing sampled review.
2. Render and review the remaining three reference cases with bounded concurrency.
3. Continue the proven LTX Pose, Depth and Canny routes on applicable benchmark cases. Preserve the seated Pose result as a sampled candidate and exclude the current hand Canny result from quality-qualified choices.
4. Compare the Wan Animate candidate against the applicable LTX results, with explicit checks for driver appearance leakage.
5. Reject weak pipelines per capability rather than averaging hard failures into a cosmetic score.
6. Exercise Maya and Leo independently across the accepted motion routes and record versioned Avatar evidence.
7. Render the dual-avatar interaction shots, verify identity separation, and assemble the coherent story.
8. Extend production browser QA from the already verified K1 interactions to complete benchmark navigation and longer-running active-task recovery.
9. Close this ledger only after every manual field has evidence and the complete two-avatar story passes continuous review. The story has not been rendered.

# AutoRig Hair Lab — integration contract and benchmark hooks

Status: research and integration proposal, 2026-10-12. This document records what exists in the current source and what must be built. It is not a claim that Hair Lab is live.

## Product boundary

Hair Lab is an optional, asynchronous V3 task tool that becomes available only after the task has:

1. an immutable source/model version;
2. completed base projections and fast analysis;
3. a viewer-ready base rig (the 60-second rig target is not extended by Hair Lab);
4. a head/head-surface candidate with source-bound evidence.

The user and the per-task session agent call the same tool and receive the same versioned result. The UI offers `keep` plus five **style presets**. The backend separately chooses or is explicitly given one of five **generation methods**. Style and method are independent fields: `chunky_2000s` can come from a fitted library mesh or procedural cards, while `ponytail_braid` can come from cards, strands converted to cards, or a Hunyuan reference. `keep` records an explicit no-change decision and performs no GPU job.

Proposed styles:

| `style_preset` | Intended result | Typical method | Fallback |
|---|---|---|---|
| `short_layered` | short layered low-poly cards, side part/fringe options | `library_fit` or `meshflow_cards` | `hunyuan_reference` then constrained reconstruction |
| `medium_bob` | medium layered or bob hair | `meshflow_cards` | `voxel_field` |
| `long_layered` | long straight/wavy hair with grouped locks | `strand_to_card` | `hunyuan_reference` then retopology |
| `ponytail_braid` | ponytail, pigtails or braid | `strand_to_card` with explicit tail/braid chains | `voxel_field` |
| `chunky_2000s` | coarse PS2/Source-era polygonal locks with texture strands | `library_fit` or `meshflow_cards` | `hunyuan_reference` for concept only |

Generation methods are a separate enum:

| `generation_method` | Contract |
|---|---|
| `library_fit` | Fit a permitted, versioned low-poly hair template to the scalp domain; fastest and deterministic. |
| `meshflow_cards` | Build an orientation field over the scalp and expand guide paths into card strips/chunks. |
| `voxel_field` | Build a coarse occupancy/direction volume, extract locks and convert them to bounded low-poly geometry. |
| `strand_to_card` | Generate a sparse guide-strand set, cluster it and bake groups to cards; no strand groom ships to the viewer. |
| `hunyuan_reference` | Generate a farm-side hairstyle candidate/reference, then retopologize/reconstruct into the accepted representation; never publish raw output without the same gates. |

The first production milestone should target the user's requested 2000s-game look: readable silhouette, a small number of polygonal locks/cards, strand texture and restrained specular response. It should not target strand-level groom realism.

## Existing contracts that can be reused

### V3 analyzer outputs

`R:/3d_video_motion_transfer/mt/render.py` already produces registered multi-view passes with exact `cameras.json` pixel-to-world mapping: lit, albedo, z-depth, metric depth16, camera-space normals, mask and RGB24 triangle IDs. `mt/kit.py` and `mt/full.py` publish 2x2 projection sheets and phase media. `mt/backproject.py` can fuse edited maps back to mesh vertices through triangle IDs.

`mt/prompts.py` already asks Vision for hair length/style and includes a dedicated hair part label. `mt/full.py` records `classify.hair`, includes hair in parts/material analysis, and obtains per-material physics labels such as `HAIR` with stiffness/damping text. `mt/fast_analysis.py` carries the hair description into category analysis.

These are useful observations, but they do not prove a scalp surface or authorize geometry replacement.

### Source hierarchy and layer evidence

`mt/model_layer_inventory.py` builds a source-hash-bound node/mesh/primitive inventory. Names are retained as untrusted weak hints only. `mt/layer_vision_qa.py` accepts exactly four hash-bound views (`whole_front`, `whole_back`, `isolation_front`, `isolation_back`) and classifies exact primitive IDs as `body`, `garment`, `hair`, `accessory` or `unknown`. A non-unknown hair observation requires both isolation views, explicit evidence and sufficient confidence. It permits no mutation.

This is the correct admission gate for existing hair. Hair Lab must never decide that a mesh is hair from its node/material name alone.

### Existing hair/layer rig work

`mt/layer_rig.py` can build disjoint secondary skeletons for a selected clothing/hair layer from source-bound mesh/object/material selectors and root/free maps. `mt/surface_attachments.py` defines exact moving-surface constraints for layer roots. The output contract states that runtime evaluates attachment roots and a secondary-chain solver, while exporters bake those transforms into clips.

The source also contains focused audit/research tests (`tests/test_audit_hair_rig.py`, `test_build_explicit_hair_root_candidates.py`, `test_build_red_hair_contamination_repair.py`). These prove fail-closed helpers, not accepted production hair generation. The canonical MT handoff contains an older checkpoint that reported full body/clothing/hair acceptance as **0/5**. That number is historical evidence of the unfinished research slice, not a fresh 2026-10-12 production acceptance audit; current production must be measured again before quoting a present pass rate.

### Task agent and tool catalogue

`mt/agent.py` already exposes one session agent per task and includes `graph`, `parts`, `look`, `viewer` and `catalogue` tools. `mt/tools.py` is the small list rendered as icons under the viewer chat; `GET /api/mt/tools` publishes it. The live combined registry at `https://autorig.online/dev/tools.json` is generated by `mt/astra/catalog.py` from session tools, graph tools and farm services.

A new feature is not complete until `hair_lab` appears in that live catalogue. Adding only a viewer icon or only a `/nodes` graph would leave the agent and user on different contracts.

### `/nodes` graph shape

The current graph is a saved JSON document defined in `autorig-online/backend/ai_graph.py`:

```json
{
  "name": "...",
  "render_quality": "preview|fast|normal|highquality",
  "nodes": [
    {"id":"...","kind":"input|service","service":"...","entity_type":"...","value":"...","x":0,"y":0,"params":{}}
  ],
  "links": [
    {"from":"node","output":"field","to":"node","input":"field"}
  ],
  "results": {}
}
```

Sockets come from `autorig-online/backend/ai_services.py`. Its current public vocabulary is `text`, `image`, `video`, `avatar`, `model3d`, `control_pose`, `control_depth`, `control_canny`. There are no typed `projection_bundle`, `mesh_selection`, `hair_asset`, `hair_rig` or `hair_validation` entities today. Existing media handoffs are URL strings; that is insufficient for mutation-sensitive geometry without additional hash/version bindings.

`mt/graph_kit.py` is the closest example: it creates input projection-sheet nodes, Vision, text joins, Qwen image edits and videos. Vertex fusion and bone tracking deliberately remain outside `/nodes`. Hair Lab should preserve that division: `/nodes` may produce concepts and masks; an MT worker performs source-bound geometry, binding, simulation setup and validation.

## Required Hair Lab contract

### Tool call

Add one canonical session tool and graph entry:

```json
{
  "name": "hair_lab",
  "op": "status|analyze|generate|apply|undo",
  "style_preset": "keep|short_layered|medium_bob|long_layered|ponytail_braid|chunky_2000s",
  "generation_method": "auto|library_fit|meshflow_cards|voxel_field|strand_to_card|hunyuan_reference",
  "options": {
    "density": 0.0,
    "length": 0.0,
    "volume": 0.0,
    "curl": 0.0,
    "stiffness": 0.0,
    "damping": 0.0,
    "max_cards": 0,
    "texture_resolution": 0,
    "seed": 0
  },
  "expected_task_id": "uuid",
  "expected_run_id": "20-hex",
  "expected_source_sha256": "64-hex",
  "expected_base_rig_version": "version"
}
```

Every option that changes results must be range-checked, returned by `status`, recorded in the receipt and available to both the user and agent. Defaults are versioned; only an owner/admin action changes category defaults. `apply` creates a new model/rig version, preserves the previous version for `undo`, and never edits the source artifact.

### Input bundle

Proposed schema `autorig.hair-lab.input/1`:

- task/run/source identity: task ID, run ID, source SHA-256, base rig artifact SHA-256 and rig version;
- analyzer revision and immutable hashes for `classify`, layer inventory and layer Vision QA;
- registered camera bundle SHA-256;
- per-view URLs plus SHA-256 for lit, albedo, depth16, normals, mask, triangle ID, material ID, object ID and parts ID;
- exact head/neck bone IDs and rest matrices from the current rig;
- exact candidate head primitive IDs and existing-hair primitive IDs;
- user/agent request, mode and bounded options;
- generator and solver versions.

Missing or mismatched hashes yield `needs_review`; they never silently fall back to filename or hierarchy-name matching.

### Graph stages

The graph should expose progress as durable stages, each independently resumable:

1. `head_observe`: Vision describes head, hair type, silhouette, parting, fringe, ponytail/braid and obstructions from registered views.
2. `hair_existing`: layer Vision QA identifies exact existing-hair primitives; `keep`, replace and augment remain distinct intents.
3. `scalp_domain`: derive a conservative scalp/head attachment domain from geometry, head/neck rig and visible evidence. Face, ears, eyes, hats and accessories are exclusion masks.
4. `style_plan`: create a compact style descriptor and five preview choices. Image generation is optional and asynchronous.
5. `guide_field`: reconstruct guide curves or a coarse oriented point/voxel field in registered 3D.
6. `cards_mesh`: GPU/client compute expands guides into bounded cards/chunks, builds UVs and an atlas assignment.
7. `texture`: bake/generate albedo, alpha, normal and roughness. Texture provenance is bound to the selected style/version.
8. `hair_rig`: attach roots to the moving head surface, create a small set of disjoint secondary chains, assign normalized weights.
9. `hair_validate`: static silhouette, head/face/ear clearance, animation sweep, root stability, stretch, penetration and detached-component checks.
10. `publish`: write a new immutable GLB/rig version, viewer manifest and receipt; notify task progress without blocking the base task.

Suggested result states: `queued`, `analyzing`, `choices_ready`, `generating`, `rigging`, `validating`, `ready`, `needs_review`, `failed`, `cancelled`. A service restart must resume from the last hash-bound stage.

## Viewer integration

The current viewer template fetches `/api/mt/tools` and renders tool icons near the chat. Add a head/hair icon only when base analysis and the rig are ready. Clicking opens a compact world-space card anchored near the model's head, with:

- current hair observation and confidence;
- `keep` and five style cards;
- generate/apply/undo controls;
- an expandable advanced panel for the bounded options;
- progress driven by the same Hair Lab receipt/status used by the agent;
- before/after toggle and animation test clips after validation.

The icon is a view/controller for `hair_lab`; it must not contain a separate mutation path. Viewer requests and agent calls use the same idempotency key: `hair-lab:<task>:<source-sha>:<base-rig-version>:<style-preset>:<generation-method>:<options-sha>`.

## `/nodes` domain rule

Current `R:/autorig/AGENTS.md` places the node editor family (`/nodes`, `/workflows`, `/queue`, `/lora`, `/system_prompts` and APIs) on the separate adult domain behind authentication, age consent and geo gates. The main `autorig.online` task viewer must not link to or embed that editor.

Hair Lab can still use the graph engine internally if either:

1. the task session tool submits a pre-approved server-side graph template and exposes only bounded Hair Lab parameters; or
2. the graph is edited on the gated node domain and published as a versioned template that the main task invokes.

Moving the general `/nodes` editor back onto the SFW task page would conflict with the current owner rule and is outside this feature. The Hair Lab endpoint must also reject arbitrary service/node injection from task viewers.

## Execution placement and performance targets

The public fleet snapshot checked once on 2026-10-12 (snapshot timestamp 2026-10-11T22:39:11Z) reported V3-ready `f1`, `f2`, `f7`, `f13`, `f5`; AI-ready `f7`, `f12`, `raptor`; Hunyuan-ready `f7`, `f12`. New tasks were paused in that snapshot, so it is evidence of roles, not permission to schedule a test.

Recommended split:

- client or converter GPU: depth/normal/ID reprojection, scalp distance field, guide/card expansion, UV packing, animation preview;
- AI node: optional Vision/Qwen style observation, mask refinement and texture reference;
- Hunyuan: fallback candidate/reference for difficult styles, never the default fast route;
- MT/Blender worker: authoritative GLB assembly/export and offline validation when client output is insufficient.

Initial budgets, to validate rather than advertise. Every report must show `queue_seconds`, `cold_start_seconds`, `warm_compute_seconds`, `validation_seconds` and `total_seconds` separately. A warm result means required models and kernels are already resident; cold start includes process and model load. Queue time is never folded into compute performance.

- choices from cached analysis: ≤2 s;
- procedural guide/card mesh from existing inputs: ≤5 s on an accepted GPU, ≤10 s CPU fallback;
- atlas generation/bake: ≤10 s when no AI edit is requested;
- rig + four preview motions: ≤5 s;
- complete procedural Hair Lab result: p50 ≤15 s, p90 ≤30 s after worker pickup, excluding queue and optional exports;
- AI/Hunyuan fallback reports separate queue and generation time and never delays the base viewer/rig.

The speed gate ends when the new GLB/rig version is validated and visible in the viewer. FBX, Unity package, thumbnails, social video and other exports run afterward and are reported separately. Network upload/download, cold model acquisition and first-time asset installation are also separate scopes; a benchmark must state when any occurred instead of hiding them inside warm compute.

## Existing free-farm concept request (discovery only)

No concept job was submitted during this research. Two original head concepts can be requested through the existing farm-backed image endpoint without a paid provider:

`POST https://autorig.online/api/qwen-image`

Minimum independent request per head:

```json
{
  "prompt": "an original low-poly 2000s game character head, neutral expression, clean three-quarter view, plain background, clearly readable polygonal hairstyle, no copyrighted character",
  "mode": "generate",
  "width": 768,
  "height": 768,
  "seed": 1001,
  "wait_seconds": 5
}
```

Submit the second head as a second request with a distinct prompt and fixed seed, for example `1002`. Do not use the supplied game screenshots as image inputs; they are visual requirements, not authorized training or edit sources. The endpoint currently accepts `prompt`, `mode`, `negative_prompt`, `width`, `height`, `steps`, `cfg`, `seed`, `checkpoint`, `wait_seconds`, and up to three reference images for edit mode. Generation mode ignores reference images. Live discovery reported generation checkpoints `qwen_image_2.1_int8_convrot.safetensors` and `qwen-image-2512-Q3_K_S.gguf`; omitting `checkpoint` selects the first installed compatible model. A successful submission returns a published image URL and, when asynchronous, `task_id_string`; poll `GET /api/ai/render-status/{task_id}` until the URL serves bytes. This is a farm job, so record queue and render time separately.

Optional image-to-3D of an accepted concept uses the authenticated internal queue and can be forced to the free farm:

`POST /api/gen3d/jobs` with the existing MT Bearer credential and at minimum:

```json
{
  "image_url": "https://...accepted-concept.png",
  "purpose": "concept",
  "quality": "draft",
  "urgency": "background",
  "backend": "farm",
  "farm_quality": "draft",
  "background_method": "auto",
  "label": "hair-lab concept head A"
}
```

`image_url` must be HTTPS. `backend: farm` prevents paid routing; the live policy also had cloud disabled at discovery time. The endpoint requires authentication and reports farm capacity dynamically. At the observed snapshot no box was `capable`, although `f7` was Hunyuan-ready and busy, so a real 3D request would wait or be refused according to the live gate. Refresh `GET /api/gen3d` immediately before any future submission. The two 2D Qwen concept requests do not depend on this 3D capacity.

## Dataset manifest

Keep the dataset as a source-hash-bound manifest, not copied customer records. Proposed `autorig.hair-dataset/1` row:

```json
{
  "case_id": "stable slug",
  "source_sha256": "...",
  "permission_class": "owned|licensed-for-training|regression-only",
  "style_tags": ["short", "side_part", "chunky_2000s"],
  "geometry_tags": ["separate_hair_mesh", "fused_mesh", "bald_source"],
  "difficulty_tags": ["ears", "hat", "long_braid", "shoulder_contact"],
  "ground_truth": {
    "scalp_mask_sha256": "...",
    "hair_mask_sha256": "...",
    "root_map_sha256": "...",
    "guide_curves_sha256": "...",
    "accepted_asset_sha256": "..."
  },
  "split": "train|validation|regression",
  "allowed_uses": ["research", "benchmark"]
}
```

Start with authored/synthetic heads and explicitly permitted assets. Real customer tasks can enter only the regression set under existing retention/permission rules; do not infer training permission from upload. Avoid reference frames whose game/media rights do not permit model training; use them as visual requirements, not dataset rows.

Coverage matrix for the first benchmark: bald/short/medium/long; straight/wavy/curly; ponytail/braid/pigtails; separate and fused hair; hats/headphones/ears/horns; dark/light/translucent textures; shoulders/face intersections; at least two head proportions and both stylized and realistic topology.

## Benchmark and acceptance hooks

Every candidate records stage wall/GPU time, peak VRAM/RAM, input/output bytes, triangle/card count, texture size and all exact hashes. Report p50/p90 with the number of measured cases.

Geometry metrics:

- scalp root coverage and root-to-head distance distribution;
- face/eye/ear exclusion violations;
- head and shoulder penetration over rest plus at least four clips;
- detached cards/components and inverted/degenerate triangles;
- card count, overdraw estimate and screen-space silhouette agreement in registered views;
- atlas occupancy and alpha-edge coverage;
- LOD triangle/texture reduction and silhouette loss.

Rig/dynamics metrics:

- root world drift relative to the moving head surface;
- maximum stretch/compression per chain;
- angular velocity/acceleration spikes;
- penetration frames and maximum depth;
- return-to-rest time, damping and clip-to-clip determinism;
- export/reload parity in GLB, FBX and the Unity package.

Visual metrics require fixed registered cameras and owner-visible clips: rest turntable, head yaw/pitch, run, jump/stop and wind impulse. Vision can flag defects but cannot alone accept the result. A five-model gate is the minimum first release gate; each mode then needs its own regression cases.

Release readiness requires all of the following:

- `hair_lab` is listed at `/dev/tools` and returned by `/api/mt/tools`;
- the user and agent paths produce the same idempotency/version receipt;
- base rig/viewer readiness remains within its existing 60-second budget;
- restart/resume and undo preserve every prior version;
- no arbitrary `/nodes` graph is accepted from the SFW task page;
- five real permitted models pass the numeric and visual gates with no hidden fallback;
- the historical 0/5 checkpoint is superseded by a fresh dated five-model audit with recorded PASS/FAIL receipts, not a source-code claim.

## Concrete implementation TODO

1. Define new typed graph entities and exact hash/version schemas; keep compatibility with current graph JSON.
2. Add server-side Hair Lab graph template and restricted executor; do not expose arbitrary node injection to task viewers.
3. Add `hair_lab` to `mt/agent.py`, `mt/tools.py`, `/api/mt/tools` and the live `/dev/tools` catalogue.
4. Build the registered head/scalp input bundle from analyzer outputs and layer Vision QA.
5. Implement the procedural `chunky_2000s` path first: guide field, cards/chunks, atlas and PBR texture.
6. Integrate source-bound layer roots with `layer_rig.py`/`surface_attachments.py`; implement and verify the secondary-chain runtime/export path that current source only describes.
7. Add the head icon and six-choice UI to the viewer using the canonical tool/status endpoint.
8. Add immutable apply/undo model versions and task progress/events.
9. Create the permission-aware dataset manifest and benchmark runner; add every failed model as a regression case.
10. Run the five-model production gate, publish visual evidence, then consider broader mode rollout.


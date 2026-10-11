# AutoRig Hair Lab: reconstruction and low-poly card research

Research snapshot: 2026-10-12. This is a source review and an implementation recommendation. It does not claim that Hair Lab is implemented or deployed.

## Decision

No published system found in this review guarantees all three properties at once for an arbitrary incoming AutoRig model: faithful reconstruction from the model projections, game-ready low-poly textured cards, and end-to-end completion in seconds. The fastest credible design is therefore a hybrid:

1. detect and classify the existing hair from AutoRig's registered head views, depth, normals, masks and object/material/triangle IDs;
2. fit a small proprietary hairstyle basis or generate a coarse scalp orientation field and guide curves on the GPU;
3. convert those guides to cards with the ideas in Strands2Cards;
4. bake a small alpha/tangent/normal/AO atlas and attach the cards to a few secondary hair chains;
5. reserve slow neural reconstruction and Hunyuan generation for asynchronous fallback or dataset authoring.

This is the only route supported by current evidence that can plausibly meet a seconds-scale product budget while retaining predictable topology, triangle count, UVs, LoDs and dynamics. Neural Haircut, MonoHair, HairStep and Im2Haircut are useful quality references and dataset generators, not interactive production paths.

## What the cited systems actually output

| System | Input and output | Published timing | Practical meaning for AutoRig | License/status checked 2026-10-12 |
|---|---|---:|---|---|
| [Strands2Cards (SIGGRAPH Asia 2025)](https://kenji-tojo.github.io/publications/strns2cards/) | Existing strand groom -> clustered wisps, polygon strips and semi-transparent textures | On RTX 4090: Jewfro, 106K strands: 1.6 s clustering + 6.1 s texture + 6.8 s fitting = **14.5 s**; big_wavy1, 172K strands: 5.3 + 9.0 + 6.4 = **20.7 s**. These are generation times from Table 1, not frame rendering. | Best direct engineering reference for the cards stage. It does not reconstruct hair from images and still needs strands/guides first. Parameters such as cluster count and strip resolution are user-specified; tubular and mixed high-variance wisps remain limitations. | [Public code repository](https://github.com/kenji-tojo/strns2cards) contains no LICENSE file as of this review, so it is not safe to copy into a commercial service without permission. Its sample data is derived from CT2Hair, whose repository is CC BY-NC 4.0. Reimplement the published ideas or obtain a license. |
| [DiffHairCard (SIGGRAPH Asia 2025)](https://arxiv.org/abs/2505.18805) | Existing 40K-strand groom -> 50–400 cards, shared textures, optional crossed cards and scalp cap | RTX 3090: about **46 min** for 100 cards/32 textures; about **1 h** for 400 cards. Runtime is generation, not rendering. A 512-card result simulates in Unreal at **0.12 ms/frame**. | Strong quality and LoD reference, but its two-stage differentiable optimization misses the seconds budget by two orders of magnitude. Useful as an offline teacher or quality oracle. It assumes a strand groom and uniform hair color; dense/coily hair needs many cards. | Paper identified; no official source/license was found in the primary sources reviewed. Treat as paper-only until an explicit commercial license appears. |
| [Hair-GAN](https://www.sciencedirect.com/science/article/pii/S2468502X18300652) | Single image + aligned bust/depth/orientation -> 3D occupancy/orientation field -> strands | GTX 1080 Ti, unoptimized pipeline: under 3 s parsing, about 1 s volume inference, under 30 s final strand synthesis: roughly **<34 s** total. | The closest published seconds-scale structural pipeline. It is old and single-view; hidden-side shape comes from its training prior. Its occupancy/orientation-field split is a good architecture for our own fast network, but its reported timing is not a quality guarantee on arbitrary stylized meshes. | Paper is open access. No production-ready official repository/weights with a commercial license were established in this review. Reproduce the representation with owned/licensed data rather than embedding unverified weights. |
| [HairNet (ECCV 2018)](https://openaccess.thecvf.com/content_ECCV_2018/html/Yi_Zhou_Single-view_Hair_Reconstruction_ECCV_2018_paper.html) | Single image orientation field -> compact hairstyle code -> about 30K 3D strands | Paper describes real-time inference and about 1000x speedup over retrieval/refinement, but the accessible primary page does not give a hardware-normalized end-to-end timing including input parsing and export. | Useful proof that direct regression can generate dense strands quickly. It predates newer diverse/curly-hair methods and inherits strong synthetic-prior/hidden-view ambiguity. Do not turn “real-time” into a seconds SLA without our own benchmark. | Paper/source lineage only in this review; no verified commercial checkpoint license. |
| [HairStep (CVPR 2023)](https://github.com/GAP-LAB-CUHK-SZ/HairStep) | Single portrait -> strand map + depth map -> about 30K strands | Later authors report **3–5 min on RTX 3090** for 30K strands; not interactive. | Better intermediate representation than a noisy 2D orientation map and useful for an offline teacher. The public README warns that its SAM replacement for the unreleased original matting method sometimes fails. | Code, HiSa/HiDa data and checkpoints are [CC BY-NC 4.0](https://raw.githubusercontent.com/GAP-LAB-CUHK-SZ/HairStep/main/LICENSE); README also says non-commercial research only. Cannot be the shipped paid-service implementation or training set without a separate license. |
| [NeuralHDHair](https://arxiv.org/abs/2205.04175) | Single image -> implicit 3D occupancy/orientation -> parallel strand growing | Paper presents efficient parallel growth, but no trustworthy end-to-end number was recovered here that includes all preprocessing. | Valuable architectural reference for parallel GPU strand growth from an implicit field. It does not directly output cards, UVs or a game rig. | Verify every component, checkpoint and dataset license before use; this review does not establish commercial permission. |
| [Neural Haircut (ICCV 2023)](https://openaccess.thecvf.com/content/ICCV2023/papers/Sklyarova_Neural_Haircut_Prior-Guided_Strand-Based_Hair_Reconstruction_ICCV_2023_paper.pdf) | Monocular video or multi-view images -> volumetric hair/bust -> optimized strand groom | **Three days per subject on one RTX 4090**: one day for stage 1 and two days for stage 2. | High-fidelity research reference only. It disproves a seconds-scale assumption for optimization-heavy personalized strands. | [Official repository](https://github.com/Vanessik/NeuralHaircut) says code/model are for scientific research under its license. Not a commercial production dependency. |
| [MonoHair (CVPR 2024)](https://keyuwu-cs.github.io/MonoHair/) | Monocular video -> Instant-NGP exterior, multi-view orientation optimization, inferred interior, strands | Paper reports **4–6 h** per subject on RTX 3090-class hardware versus 3–4 days for Neural Haircut. | Better hairstyle diversity and curls than Neural Haircut, still unsuitable for the interactive path. Its registered multi-view fusion is relevant to an offline teacher using AutoRig projections. | [Official code](https://github.com/KeyuWu-CS/MonoHair) and [LICENSE](https://github.com/KeyuWu-CS/MonoHair/blob/master/LICENSE.txt) are CC BY-NC 4.0. Not suitable for a paid service without another agreement. |
| [Im2Haircut (ICCV 2025)](https://im2haircut.is.tue.mpg.de/) | Single image -> prior strands, then per-image optimization | 400 optimization steps take about **10 min on A100**. | Better single-image quality reference, not a fast user-facing generator. Uses external segmentation, direction, camera and depth estimators. | [Official code](https://github.com/Vanessik/Im2Haircut) is CC BY-NC-SA 4.0 plus 3DGS terms. Not a closed commercial backend dependency without separate permission. |
| [HairFastGAN (NeurIPS 2024)](https://github.com/AIRI-Institute/HairFastGAN) | Face photo + hairstyle/color references -> edited **2D image** | Under **1 s on NVIDIA V100** for its hardest shape+color transfer scenario | Excellent optional reference-image composer: it can create a desired front-view target for later geometry fitting. It produces no 3D field, strands, cards, UVs or dynamics, so it is not a hair generator by itself. | Repository code is MIT, but it depends on StyleGAN/FFHQ and several prior projects/checkpoints. Audit all model/data terms separately before commercial deployment. |
| [HairCLIP](https://github.com/wtybest/HairCLIP) / [HairCLIPv2](https://github.com/wtybest/HairCLIPv2) | Text/reference/mask/sketch -> edited **2D portrait** | No relevant 3D construction timing | Same role as HairFastGAN: make or edit reference projections; do not count it as reconstructed geometry. Cross-view consistency must be established separately. | HairCLIP repository is LGPL-2.1; dependent weights and datasets require their own review. |
| [Digital Salon](https://github.com/digital-salon/Digital-Salon) | Text retrieval from a precomputed hairstyle library, interactive GPU grooming, physics and rendering | Described and demonstrated as real-time/interactive; no paper number establishes an arbitrary-image-to-new-groom latency. | Strong product reference for retrieval + deformation + simulation. Its “generation” starts from a library, which is precisely why it can be interactive. Its CUDA simulation and UI concepts are useful after we own a licensed library. | Code is [MIT](https://raw.githubusercontent.com/digital-salon/Digital-Salon/main/LICENSE), but the release replaces Adobe internal data with USC-HairSalon renderings and omits its head for licensing reasons. Code permission does not grant dataset/asset permission. |
| [CHARM (SIGGRAPH Asia 2025)](https://github.com/hyz317/CHARM) | Image or point cloud -> autoregressive control-point hair cards for anime styles | No verified end-to-end timing suitable for our SLA was published in the sources reviewed. | The most relevant learned representation for stylized polygonal locks/cards. Its five-parameter-per-control-point representation is a good design reference for a proprietary 2000s-style card basis. Its domain is anime hair, not arbitrary realistic hairstyles. | Code is GPL-3.0. Raw AnimeHair models cannot be redistributed due to policy restrictions; the authors point to VRoid data with separate terms. Do not train a commercial model on it until every data right is resolved. |
| [PERM](https://github.com/c-he/perm) | Parametric multi-style strand representation; can be fitted to a single image | Retrieval/fitting is reported by later work as hours, not seconds. | Useful compact basis concept, potentially distillable into our own licensed hairstyle basis. Public single-view results include depth failures and weak curly/kinky reconstruction. | Repository code is MIT, but its README says the single-view pipeline and models trained on Adobe internal data require an individual release license. Hair20k/USC-derived data must be reviewed separately. |

### Timing distinction that must stay in benchmarks

The 30 FPS Strands2Cards demo is rendering **342 already-generated hairstyles** on an M3 Pro. It is not a claim that 342 hairstyles were generated at 30 FPS. Likewise DiffHairCard's 0.12 ms/frame is simulation of completed cards; its card construction takes 46–60 minutes. Hair Lab receipts must report at least: analysis, guide reconstruction, card construction, atlas baking, rigging, validation and viewer publication separately.

## Structural reconstruction versus game cards

The two outputs solve different problems:

- A strand/field reconstruction estimates hairstyle volume, roots and local flow. It preserves direction and is the natural source for physics, but tens of thousands of strands are too heavy for the requested 2000s-game look and do not directly provide a conventional GLB card asset.
- A card generator compresses known strands into textured polygon strips. It gives bounded triangles, shared texture atlases and LoDs, but cannot infer the back of a hairstyle or distinguish bangs from the face when the input guides are wrong.
- A shell or voxel-only hair mesh is quick and robust for chunky stylization, but does not encode strand flow well enough for convincing alpha-textured cards or flexible dynamics unless an orientation field is added.

The stable internal interchange should therefore be an **oriented scalp field plus sparse guide splines**, not a million-point cloud and not a final triangle soup. Suggested minimum schema per sample: position, tangent, confidence, root/scalp coordinate, visible-view bitset, style group and collision class. From that representation the service can emit chunky meshes, hair cards, physics guides and LoDs without rerunning Vision.

## Five practical generator strategies

### 1. Licensed library retrieval and GPU fitting — default fast mode

Classify length, part, fringe, volume, curl, ponytail/braid and silhouette from the registered head views. Retrieve several candidates from an AutoRig-owned or commercially licensed groom library. Fit scalp roots and guide control points to the current head, then regenerate cards and atlas.

- Expected advantage: deterministic topology and the best chance of a 2–10 s response after models are resident.
- Quality: high for styles represented in the library; controllable LoD and dynamics.
- Failure mode: uncommon silhouettes become a nearest style rather than a faithful reconstruction.
- Evidence: Digital Salon obtains interactivity by retrieval plus GPU grooming; learned systems that reconstruct arbitrary hidden structure are much slower or rely strongly on priors.

### 2. Direct proprietary field-to-cards network — target production mode

Fuse AutoRig's registered front/back/side depth, normal, hair mask, material/object IDs and head geometry into a scalp-aligned occupancy and orientation field. Predict sparse guide splines and confidence in one pass. Grow guides in parallel on GPU, then run a simplified Strands2Cards-like clustering/fitting stage.

- Expected advantage: personalized silhouette and flow without hours of per-subject optimization.
- Quality: potentially better than retrieval after enough owned training data; confidence exposes uncertain hidden regions.
- Failure mode: training coverage, braids/coils, accessories and fused hair/body topology. Hidden views remain prior-driven.
- Evidence: Hair-GAN reports about 1 s for the field network and under 30 s for old unoptimized strand synthesis; HairNet and NeuralHDHair establish direct regression/parallel growth; Strands2Cards establishes 14–21 s for high-count strand-to-card conversion on 4090.
- Engineering target, not a published fact: replace dense strand synthesis with 100–500 guides and fuse the card stage so p50 can approach 5–15 s. Measure it before advertising.

### 3. Coarse shell/voxel to stylized chunks — fastest 2000s preset

Build a conservative hair shell from multi-view depth/masks, extract a scalp distance field, divide the shell by parting/flow regions, and create 20–100 thick polygonal locks or crossed cards. Bake visible color into a compact atlas and synthesize strand normal/alpha detail procedurally.

- Expected advantage: few GPU kernels, no neural 3D generator, client/WebGPU-compatible in principle, and visually aligned with PS2/Source-era characters.
- Quality: strong silhouette and stable shading at low polygon count; deliberately less realistic.
- Failure mode: fine loose strands, complex braids and long layered hair; bad masks produce visible cutouts.
- Use: ship this preset first because it matches the stated art direction and can be validated without claiming high-fidelity strand reconstruction.

### 4. 2D reference editing plus constrained 3D fitting — creative preset mode

Use HairFastGAN/HairCLIP-like functionality or the farm's local Qwen image editor to create consistent hairstyle targets from the original registered views. Reconstruct only with source cameras and geometry IDs, then fit library guides or the proprietary field model.

- Expected advantage: gives the user five clear visual choices and supports style/color transfer.
- Quality: good concept fidelity when edits preserve head pose and silhouette.
- Failure mode: independently edited views disagree; 2D models hallucinate occluded geometry. A single edited portrait must never be treated as 3D proof.
- Rule: output images are references. Only registered multi-view agreement plus geometry validation can publish cards.

### 5. Slow strand reconstruction or local Hunyuan fallback — asynchronous only

For difficult styles, generate a hair-only proposal on the farm or run a higher-quality research-style reconstruction, retopologize it to an oriented field/guides, then convert to cards. Keep it behind a separate queue and show its real ETA.

- Expected advantage: broader topology and a path for dataset authoring.
- Quality: useful for novel silhouettes after validation and repair.
- Failure mode: minutes/hours of latency, uncertain topology and cross-view consistency, licensing restrictions for most academic checkpoints.
- Rule: it never blocks base V3 analysis/rig readiness and never silently replaces the fast path.

## Recommended five user presets

The UI's five options should select algorithms and constraints, not only prompt words:

1. `short_cards`: hair cap + 50–120 cards, high stiffness, small secondary motion;
2. `layered_2000s`: 30–100 chunky locks/cards, painted strand texture, strongest silhouette preservation;
3. `medium_bob`: 100–250 cards, front/side layers and 2–4 secondary chains;
4. `long_flow`: 200–500 cards, grouped guide ribbons and shoulder/body collision;
5. `ponytail_braid`: scalp cap plus explicit tail/braid control chains, crossed cards or tubular chunks where planar cards fail.

`keep` remains a sixth non-generation choice. Existing hair must first be identified as exact source-bound primitives; hierarchy names are only weak evidence.

## Dataset and rights boundary

The supplied game screenshots and YouTube references define art direction. They do not establish permission to copy characters, extract their meshes/textures or use their frames as a training dataset. Build the first dataset from:

- AutoRig-authored procedural heads and hairstyles;
- assets with explicit commercial training/derivative rights;
- commissioned low-poly card grooms with source files;
- synthetic variations of those owned assets;
- customer models only when an explicit consent record permits training, otherwise regression-only evaluation.

Do not use HairStep/MonoHair/Neural Haircut/Im2Haircut checkpoints or CT2Hair samples in the paid service under their current non-commercial/research terms. Strands2Cards has no repository license; its algorithm can guide a clean implementation, but its code is not an import candidate. CHARM's raw data is unavailable for redistribution and its GPL code affects distribution choices. Digital Salon and HairFastGAN have permissive code licenses, but their data, third-party checkpoints and bundled assets still require a component-level audit.

Each dataset row needs source hash, rights owner, allowed uses, expiry/withdrawal status, style tags, geometry type, and train/validation/regression split. A model trained from untracked reference images is not releaseable evidence.

## Benchmark needed before selecting the production path

Create a rights-cleared set with at least five examples for each major failure dimension: short/medium/long; straight/wavy/curly/coily; bangs/side part/ponytail/braid; separate hair mesh/fused mesh/bald head; hats/ears/horns/headphones; realistic and stylized proportions.

Record, for every run:

- queue, preprocessing, inference, guide growth, card fitting, atlas, rig, validation and publication wall/GPU times;
- peak VRAM/RAM and GPU model;
- guide/card/triangle counts, atlas bytes and estimated overdraw;
- registered-view silhouette IoU, depth error and tangent agreement;
- root-to-scalp distance, face/ear/eye violations and shoulder/body penetration across clips;
- atlas occupancy, alpha coverage and LoD silhouette loss;
- before/after fixed-camera media plus source/output hashes.

The first decision gate should compare strategy 1 (retrieval/fitting), strategy 2 (field-to-cards) and strategy 3 (stylized chunks) on the same inputs. Speed targets remain hypotheses until the measured p50/p90 and sample count are published. A useful starting target is p50 <= 15 s and p90 <= 30 s after pickup for procedural modes, while fallback generation reports its own queue and runtime.

## Recommended implementation sequence

1. Define the oriented scalp-field/guide schema and exact registered input bundle.
2. Implement `layered_2000s` as deterministic shell/field -> chunks/cards, because it directly matches the requested art style and requires no restricted research model.
3. Implement a clean strand/guide-to-card converter informed by Strands2Cards; benchmark clustering, texture and fitting separately.
4. Author or license a compact groom basis and add retrieval/fitting.
5. Train the proprietary field predictor only on rights-cleared data; use slow research methods as quality references, not embedded dependencies.
6. Add secondary chains, collision and fixed animation validation.
7. Add reference-image editing and Hunyuan as asynchronous fallback after the deterministic path passes the dataset gate.

The most defensible immediate prototype is therefore: registered AutoRig layers -> conservative hair mask and scalp shell -> flow field -> 30–100 chunky locks/cards -> procedural strand atlas -> 2–4 secondary chains. It can reproduce the intended early-2000s game look, exposes every unstable parameter as a tool option, and creates the measurements needed before training a larger generator.

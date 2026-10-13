# Rig quality feature catalogue (2026-10-14)

This is a research catalogue of techniques that make an automatic character rig
better. A rig here means skeleton placement, skin weights and animation
retarget. It is written for a production auto-rigger that takes arbitrary user
meshes and must rig within the 60 s budget ("Rig Within One Minute" in
`AGENTS.md`) on CPU/GPU workers. The meshes include bipeds, quadrupeds, birds
and monsters, from low-poly to 300k vertices, with props in hands, cloth, hair
cards, unwelded seams and multiple shells.

## How to read the tables

* **Fixes** names the defect classes from the brief:
  * leak: vertex leakage to the wrong limb;
  * tear: tearing at unwelded seams;
  * bone-out: bones outside the mesh;
  * limb-in-body;
  * collapse: knee or elbow collapse;
  * candy: candy-wrapper twist;
  * slide: foot sliding;
  * stretch: stretched edges in clips;
  * prop: props bound into the body;
  * orient: wrong orientation or up axis.
* **Cost** is for a 50k-vertex mesh unless a source states something else.
  * "reported" means the number comes from the cited source, on that source's
    hardware.
  * "est." is an engineering estimate from the algorithm's complexity. It is
    not a measurement and must be verified on the farm before it is relied on.
  * "n/r" means the source reports no timing.
* **60 s fit**:
  * **YES**: fits inside the rig path.
  * **PROXY**: fits only on a decimated or voxel proxy, or with a parameter
    cap.
  * **BG**: background or offline only, for example while building a library.
  * **NO**: not useful for this product.
* Every URL below was fetched during this research, with WebFetch or curl.
  Some well-known papers had no fetchable page, for example the Geodesic Voxel
  Binding paper on diglib.eg.org, which answered 403. Those are named without
  a URL, or through a fetched page that documents them, such as the Maya help
  page for GVB.

---

## 1. Mesh preprocessing

| Technique | Fixes | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Weld for skinning only.** Merge coincident positions (eps ≈ 1e-5 to 1e-4 of the bbox diagonal), ignoring UV and normal splits. Compute weights on the welded "skin topology", then copy them to every split copy of a vertex. Assert that the weight difference between coincident vertices is 0. | tear (the main cause: UV/normal-split duplicates get different weights from a surface solve) | Blender *Merge by Distance* / Weld modifier: https://docs.blender.org/manual/en/latest/modeling/meshes/editing/mesh/cleanup.html ; trimesh `merge_tex` / `merge_norm` ("merged regardless of UV coordinates", "ignoring different normals"): https://trimesh.org/trimesh.base.html ; PyMeshLab *Merge Close Vertices* (default 1/10000 of the bbox diagonal): https://pymeshlab.readthedocs.io/en/latest/filter_list.html | est. < 0.1 s (hash or KD-tree) | **YES**, mandatory |
| **Snap mismatched borders / remove T-vertices.** Close cracks between patches that are tessellated differently. | tear, holes that break inside/outside tests | PyMeshLab `meshing_snap_mismatched_borders`, `meshing_remove_t_vertices`: https://pymeshlab.readthedocs.io/en/latest/filter_list.html | est. < 1 s | **YES** (conditional: only when open-boundary loops pair up) |
| **Shell (connected-component) analysis and classification.** Classify each shell as body, overlapping layer (cloth, armour, hair cards), small rigid part (eyes, teeth, buttons, plates) or held prop. Each class gets its own binding strategy (sections 3 and 6). | prop, leak into accessories, stretched rigid parts | PyMeshLab `meshing_remove_connected_component_by_diameter` / `_by_face_number`: https://pymeshlab.readthedocs.io/en/latest/filter_list.html ; AccuRIG: "segregates skin weights by detecting individual surfaces and tracing back to their rightful parent bones": https://cdn-www.reallusion.com/auto-rig/accurig | est. < 0.2 s | **YES** |
| **Volumetric closing with an unsigned distance field.** Convert all shells into one UDF/level set, then dilate by r and erode by r. Gaps smaller than 2r close into one solid; real gaps (between legs, arm↔torso) stay open when r is below half the gap. | disconnected shells, non-watertight inputs; feeds voxel geodesics without bridging limbs | OpenVDB `meshToUnsignedDistanceField`, `meshToLevelSet`; SDF conversion needs "a closed surface but not necessarily a manifold surface": https://openvdb.org/documentation/doxygen/MeshToVolume_8h.html | est. 0.5–2 s at 256³ (sparse, threaded) | **YES** |
| **Alpha wrapping.** A watertight, 2-manifold, intersection-free envelope of any triangle soup, used only as an analysis proxy, never as output. | non-manifold, holes, self-intersections, multi-shell input to heat/SDF/MCF | CGAL 3D Alpha Wrapping (timings charted on Thingi10k; smaller alpha means more time and memory): https://doc.cgal.org/latest/Alpha_wrap_3/index.html ; also PyMeshLab `generate_alpha_wrap`: https://pymeshlab.readthedocs.io/en/latest/filter_list.html | est. 1–10 s, depending on alpha (1/50–1/150 of the diagonal) | **PROXY**: cap alpha; alpha too large fuses legs |
| **ManifoldPlus.** Turns a soup into a watertight manifold by extracting octree faces, then a projection-based optimisation. | non-manifold / soup → watertight proxy | https://github.com/hjwdzh/manifoldplus (`--depth 8`) | n/r; est. 1–5 s at depth 8 | **PROXY** |
| **MeshFix / PyMeshFix.** Repairs holes, self-intersections, degenerate and non-manifold elements into one watertight mesh. | holes, non-manifold, self-intersections | https://github.com/pyvista/pymeshfix (it targets scanned meshes and may drop small components, so run it on the body shell only) | n/r; est. 1–5 s | **PROXY** (body shell only) |
| **fTetWild.** Robust tetrahedralisation of soups within an envelope, needed only if BBW-style volumetric weights are used. | volume needed for BBW / volumetric Laplacians | https://github.com/wildmeshing/fTetWild | n/r; est. tens of seconds at 50k | **BG** |
| **Hole filling (capped).** Fill boundary loops up to N sides. This is the same family as the existing cut+cap lids. | inside/outside errors at open necks, sleeves and eye sockets; heat-solver failures | Blender *Fill Holes* ("Sides" limit): https://docs.blender.org/manual/en/latest/modeling/meshes/editing/mesh/cleanup.html ; PyMeshLab `meshing_close_holes` | est. < 0.5 s | **YES** |
| **Generalized / fast winding number inside test.** A robust inside/outside answer for soups, holes and self-intersections. Use it for voxel interior classification, the SDF sign, the bone-in-mesh check and joint centring. | bone-out, limb-in-body detection, wrong interior on open meshes | Fast Winding Numbers for Soups and Clouds (Barill et al. 2018), in libigl as `igl::fast_winding_number`: https://dgp.toronto.edu/projects/fast-winding-numbers | est. 0.3–1 s for 128³ queries, 2–5 s for 256³ (tree-based, threaded) | **YES** |
| **Robust Laplacian for non-manifold meshes.** An intrinsic-Delaunay "tufted cover" Laplacian with mollification. It works on non-manifold meshes and point clouds. Use it for any surface heat diffusion or smoothing. | heat-diffusion failures on bad meshes (the Blender error "Bone Heat Weighting: failed to find solution for one or more bones" is commonly reported on such meshes: https://blenderartists.org/t/bone-heat-weighting-failed-to-find-solution-for-one-or-more-bones/701412) | Sharp & Crane 2020, `robust_laplacian`: https://github.com/nmwsharp/robust-laplacians-py | n/r; est. < 1 s build at 50k | **YES** |
| **Proxy decimation for analysis.** Run skeleton and weight solves on a 5–20k-vertex proxy, then transfer weights back by closest point plus barycentric interpolation. Lock borders so shells do not merge. | 300k-vertex meshes breaking the budget | meshoptimizer `meshopt_simplify`, `meshopt_SimplifyLockBorder`, `meshopt_simplifyWithAttributes`, `meshopt_simplifySloppy`: https://meshoptimizer.org/ ; RigNet itself needs a 1k–5k-vertex remesh: https://github.com/zhan-xu/RigNet | est. < 0.5 s for 300k triangles | **YES** |
| **Held-prop detection from file structure.** Separate OBJ `g`/`o`/`usemtl` groups, glTF nodes/meshes and FBX nodes become candidates. A candidate is a prop when it is a separate shell, it intersects or sits inside the hand region, it is roughly rigid (high aspect, low curvature variation, "metal/wood" material names), and it continues outside the silhouette of the body. | prop (the sword bound into the body), bones growing into the prop | OBJ grouping statements: http://paulbourke.net/dataformats/obj/ ; glTF node/mesh structure: https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html ; AccuRIG "differentiates the hand from the props being held so auto-generated bones don't extend into inanimate objects": https://cdn-www.reallusion.com/auto-rig/accurig | est. < 0.2 s | **YES** |
| **Held-prop / part detection on fused meshes.** When the prop is fused into one shell with the hand, cluster a part feature field and cut along the part boundary. | prop on single-shell AI meshes | PartField (NVIDIA, ICCV 2025; "orders of magnitude faster", feed-forward): https://research.nvidia.com/labs/toronto-ai/partfield-release/ , code https://github.com/nv-tlabs/PartField | n/r; est. 1–3 s GPU | **YES** on GPU workers |
| **Up axis / front canonicalisation.** PCA reduces the orientation to a few candidates. A learned orienter, or the existing Vision step on 6 renders, picks one of the 24 octahedral flips. | orient (and it removes the need for a 24-candidate OpenPose sweep) | Symmetry-Robust 3D Orientation Estimation (two stages: quotient regression, then a 24-way "flipper"; 89.2 % up-axis accuracy on ShapeNet): https://arxiv.org/html/2410.02101v4 , code https://github.com/cscarv/3d-orienter | est. < 1 s GPU; Vision pick a few s | **YES** |
| **Reflective symmetry plane.** Detect the plane with a network or with PCA plus reflection ICP. Use it later for joint mirroring and weight mirroring. | asymmetric skeletons, L/R weight asymmetry | PRS-Net ("hundreds of times faster" than sampling methods, MIT licence): https://github.com/IGLICT/PRS-NET | est. < 0.5 s | **YES** |

---

## 2. Skeleton placement

| Technique | Fixes | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Template embedding (Pinocchio).** Approximate the medial surface with packed spheres, build a graph, run a discrete optimisation of the template embedding with learned penalties, then continuous refinement. | wrong topology on bipeds/quadrupeds, bones off-centre | Baran & Popović 2007: https://people.csail.mit.edu/ibaran/papers/2007-SIGGRAPH-Pinocchio.pdf ; RigAnything measures Pinocchio at ~40 s per shape: https://arxiv.org/html/2502.09615v2 | reported ~40 s per shape | **PROXY**: reuse the penalty idea (symmetry, bone length, joint-in-sphere) on the existing voxel trace, not the whole program |
| **Voxel thinning skeleton** (Lee 1994 3-D thinning) **with a medial distance (radius) per voxel.** Prune spurs by the ratio of branch length to radius, then match the graph to the category template. | spurious branches (fingers, hair, cloth folds), missing radius for joint centring | scikit-image `skeletonize` (Lee's method selected automatically for 3-D) and `medial_axis` (returns the distance transform): https://scikit-image.org/docs/0.25.x/auto_examples/edges/plot_skeleton.html | est. 1–3 s at 128–192³ | **YES** (already in place; add radius-based pruning) |
| **Mean curvature flow skeleton.** Contract the surface to a medially centred curve skeleton, with a map from skeleton nodes to vertices that is useful as a segmentation prior. | off-centre skeleton, segmentation for weights | Tagliasacchi et al. 2012; CGAL needs a closed single component; reported 2,775 v 0.46 s, 11,112 v 1.26 s, 44,460 v 5.64 s, 177,852 v 26.26 s (i7-2820QM): https://doc.cgal.org/latest/Surface_mesh_skeletonization/ | reported ≈ 5.6 s at 44k; ≈ 1.3 s on an 11k proxy | **PROXY** (on the alpha-wrap proxy) |
| **2-D pose prior from rendered views.** Detect 2-D joints on several renders and triangulate an initial 3-D skeleton, which a mesh network then refines. | wrong joint placement on stylised humanoids | HumanRig (CVPR 2025): PGSE uses 2-D joints from rendered views, then MSMAN refines; dataset of 11,434 T-posed meshes: https://arxiv.org/abs/2412.02317v1 , code https://github.com/c8241998/HumanRig | n/r | **YES** if limited to 2–4 canonical views after orientation, not a 24-candidate sweep |
| **Make-It-Animatable.** Predicts bones, blend weights and pose-to-rest transforms from a particle autoencoder; meshes and 3DGS input. | humanoid rig in < 1 s, A-pose/odd-pose input | CVPR 2025, "less than one second": https://jasongzy.github.io/Make-It-Animatable/ , code https://github.com/jasongzy/Make-It-Animatable | reported < 1 s (GPU) | **YES** (bipeds, GPU) |
| **UniRig.** Autoregressive skeleton tree tokens, plus bone-point cross-attention skinning; humans, animals and objects. Its own caveat: skinning "may degrade significantly if the skeleton is inaccurate", e.g. missing tail or wing bones. | unknown categories, monsters | SIGGRAPH 2025, MIT licence, ≥ 8 GB VRAM: https://github.com/VAST-AI-Research/UniRig , paper https://arxiv.org/abs/2504.12451 | n/r; est. a few s to tens of s on GPU | **YES** (GPU, as a second opinion) |
| **MagicArticulate.** Autoregressive skeleton, plus functional-diffusion skinning with a volumetric geodesic prior. | non-template categories | CVPR 2025: https://chaoyuesong.github.io/MagicArticulate , code https://github.com/Seed3D/MagicArticulate | Puppeteer reports its own skeleton at 1.5 s, "1.6× faster than MagicArticulate" (≈ 2.4 s), and its skinning 45× faster | **YES** (GPU) |
| **Puppeteer.** Joint-token autoregressive skeleton; topology-aware joint attention (skeletal graph distances) for skinning; optional video-guided animation optimisation. | leakage between topologically distant bones; skinning accuracy | NeurIPS 2025: skeleton 1.5 s, skinning 0.032 s; skin precision 79.7 % vs MagicArticulate 68.1 % vs RigNet 65.7 %: https://arxiv.org/html/2508.10898v1 , project https://chaoyuesong.github.io/Puppeteer | reported 1.5 s + 0.03 s (GPU) | **YES** (GPU) |
| **RigAnything.** Template-free autoregressive joints with BFS order, diffusion sampling of positions and softmax skinning. Trained on biped, quadruped, avian, marine and insect shapes. | non-humanoid categories | SIGGRAPH 2025 (Adobe): ~2 s per shape on A100 vs RigNet ~120 s, TARig ~40 s, Pinocchio ~40 s: https://arxiv.org/html/2502.09615v2 | reported ~2 s (A100) | **YES** if weights are obtainable; no public code was confirmed in this research |
| **Anymate.** A 230K rigged-asset dataset with baseline joint, connectivity and skinning modules. | training data for our own models | SIGGRAPH 2025: https://arxiv.org/abs/2505.06227v1 | n/r | **BG** (data and baselines) |
| **RigNet.** GNN joints, connectivity and skinning; needs a 1k–5k-vertex remesh. | — | SIGGRAPH 2020: https://github.com/zhan-xu/RigNet , https://zhan-xu.github.io/rig-net/ ; ~120 s per shape (RigAnything) | reported ~120 s | **NO** (too slow, superseded) |
| **TARig.** Humanoid adaptive template plus boneflow; the authors recommend refining weights with Maya GVB. | — | https://github.com/jingma-git/TARig-1 (GPLv3); ~40 s on A100 per RigAnything | reported ~40 s | **NO** |
| **Neural Blend Shapes.** Learns skeleton, skinning and pose-dependent corrective shapes for T-posed characters. | collapse at joints (correctives) | SIGGRAPH 2021: https://peizhuoli.github.io/neural-blend-shapes/ , code https://github.com/PeizhuoLi/neural-blend-shapes | n/r | **BG** (humanoid; research-grade) |
| **Articulate-Anything.** Builds rigid articulated objects as URDF (drawers, laptops). | — | ICLR 2025: https://articulate-anything.github.io/ | — | **NO** for characters (props with hinges only) |
| **Symmetry enforcement.** Reflect L/R joint pairs across the detected plane, average them, and snap the spine and neck to the plane. | asymmetric shoulders/hips, L/R weight mismatch | Plane from PRS-Net: https://github.com/IGLICT/PRS-NET ; practice | est. < 0.05 s | **YES** |
| **Joint centring with SDF and cross-sections.** For each limb joint, take the section perpendicular to the bone and keep the loop that contains the bone. Move the joint to its centroid, or to the local SDF maximum from the medial radius. Check with a winding-number sample along each bone (fraction outside should be 0). Elbows and knees get a small offset to the back of the limb (hinge side). | bone-out, off-centre elbows/knees, collapse caused by joints too close to the surface | Inside test: https://dgp.toronto.edu/projects/fast-winding-numbers ; SDF: https://openvdb.org/documentation/doxygen/MeshToVolume_8h.html ; Pinocchio centres joints on medial spheres (paper above); Maya GVB requires "all joint influences … enclosed inside the mesh volume": https://help.autodesk.com/cloudhelp/2018/ENU/MayaLT-CharacterAnimation/files/GUID-5EFDB81B-E332-4D6C-B1BB-0B989AD2F2C7.htm | est. < 0.2 s | **YES** |
| **Category templates: quadrupeds.** SMAL (41 toy scans; lions, cats, dogs, horses, cows, hippos); hSMAL for horses (37 horse toys, gait/lameness); 3D-Fauna adaptive quadruped skeleton (8 spine bones, 3 per leg, foot joints from the vertex distribution). | wrong topology on quadrupeds, leg count/order, spine segmentation | https://smal.is.tue.mpg.de/ , https://arxiv.org/abs/1611.07700 ; https://arxiv.org/abs/2106.10102 ; https://arxiv.org/html/2401.02400v2 , https://kyleleey.github.io/3DFauna/ | template fit est. < 1 s | **YES** (as skeleton conventions; check the SMAL licence, which its page does not state) |
| **Category templates: birds.** AVES, an articulated bird template with a multi-species shape space from 17 species. | wing/leg/tail topology on birds | https://arxiv.org/abs/2105.09396v1 , https://yufu-wang.github.io/aves/ | est. < 1 s | **YES** (as a convention) |
| **Finger separation.** Shrink finger voxel volumes before generating hand joints, so close fingers separate. | fused fingers, palm-to-finger leaks | AccuRIG: https://cdn-www.reallusion.com/auto-rig/accurig | est. < 0.2 s | **YES** |

---

## 3. Skin weights

| Technique | Fixes | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Bone heat (heat equilibrium).** Solve (−Δ + H) wᵢ = H pᵢ on the surface. H holds the inverse squared distance to the nearest *visible* bone; pᵢ is 1 where bone i is nearest. | smooth falloff; better than envelopes | Baran & Popović 2007: https://people.csail.mit.edu/ibaran/papers/2007-SIGGRAPH-Pinocchio.pdf ; Blender *With Automatic Weights* ("bone heat"), with overlap warnings: https://docs.blender.org/manual/en/latest/animation/armatures/skinning/parenting.html | est. 1–3 s (one sparse factorisation, then a back-solve per bone) | **YES** only on welded topology with a robust Laplacian; it is a surface method, so it still leaks across touching shells |
| **Geodesic voxel binding (GVB).** Voxelise all shells as one solid (interior = non-exterior voxels). Weight = falloff of the geodesic distance from each bone's voxels through interior voxels only. | leak across gaps (legs, arm↔torso, palm↔thigh), multi-shell, non-manifold, non-watertight | Dionne & de Lasa 2013 (paper page answered 403). Maya docs: it handles "non-manifold, non-watertight, intersecting triangles or multiple connected components"; multiple meshes are "treated as a single volume"; joints must be inside: https://help.autodesk.com/cloudhelp/2018/ENU/MayaLT-CharacterAnimation/files/GUID-5EFDB81B-E332-4D6C-B1BB-0B989AD2F2C7.htm | est. 1–5 s at 128–256³ with a multi-source Dijkstra/fast-marching sweep in numba/C++ | **YES**, the strongest candidate. No open-source implementation was found, so it has to be built |
| **Voxel heat diffusion.** Bones emit heat that diffuses with loss through a ray-tested solid voxel volume of all sub-meshes. | the same as GVB; robust on broken meshes | Voxel Heat Diffuse Skinning (commercial Blender add-on): default resolution 128, adaptive mode "5 seconds", standard mode 80+ s at 600; known weakness at lips and toes (narrow seams): https://superhivemarket.com/products/voxel-heat-diffuse-skinning | reported 4–5 s adaptive | **YES** (the method; we implement it ourselves) |
| **Gap-preserving voxel resolution.** Pick the voxel size from the narrowest real gap: inner thighs, armpits, between fingers, between a held prop and the palm. Use adaptive or local refinement there. | leak at narrow seams; the main tuning knob of GVB/heat | Voxel Heat add-on "Subdivide Depth" adaptive subdivision (page above); AccuRIG finger-voxel shrinking: https://cdn-www.reallusion.com/auto-rig/accurig | est. +0.5–2 s | **YES** |
| **Bounded biharmonic weights.** Minimise Laplacian energy subject to bounds, giving shape-aware, local weights for bones, points and cages. | smoothness, locality, no negative weights | Jacobson et al. 2011: https://www.alecjacobson.com/weblog/1784.html (needs a tet mesh and a QP solver) | est. 10–60 s at 50k (tetrahedralisation plus QP) | **BG** / **PROXY** at ≤ 5k vertices |
| **Learned weight prior.** RigNet, UniRig (bone-point cross-attention), MagicArticulate (geodesic-prior diffusion), Puppeteer (topology-aware attention), Make-It-Animatable. Use the result as a prior blended with the geodesic weights, never alone. | stylised anatomy where geometric falloff is wrong (shoulders, hips) | URLs in section 2 | reported 0.03 s (Puppeteer) to ~1.4 s (MagicArticulate, derived from Puppeteer's 45× claim) | **YES** (GPU) |
| **Skeleton-graph adjacency mask.** A vertex may only carry weight from bones within k hops (k = 1–2) of the bone whose region it lies in. Regions come from a geodesic Voronoi of bone segments in the voxel volume. Zero the rest, then renormalise. | leak to the wrong limb (palm→thigh, arm→torso, leg→leg) at no cost | Puppeteer's skinning attention uses "relative positional encodings derived from skeletal graph distances": https://arxiv.org/html/2508.10898v1 ; the hard mask itself is practice | est. < 0.1 s | **YES** |
| **Capsule flood fill.** Grow a region from each bone's capsule through interior voxels until it meets another region. Used as a hard gate and as a QA reference. | leak; detects props and cloth inside the wrong region | practice (GVB variant) | est. < 0.5 s | **YES** |
| **Post-processing chain.** Prune weights under a threshold (0.01–0.02); limit total influences, lowest first; normalise to a sum of 1; mirror across the symmetry plane; smooth only on the welded topology. | noise, more than 4 influences, L/R asymmetry, tear after smoothing | Blender weight tools *Normalize All*, *Limit Total* ("removes lowest weights first"), *Mirror* (needs a perfectly symmetric rest pose; *Topology Mirror* for near-symmetric meshes): https://docs.blender.org/manual/en/latest/sculpt_paint/weight_paint/editing.html ; UE `SkinWeightModifier.enforce_max_influences`, `prune_all_weights`, `normalize_all_weights`: https://dev.epicgames.com/documentation/en-us/unreal-engine/python-api/class/SkinWeightModifier ; glTF: "Client implementations MAY support only a single set of up to four weights and joints": https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html | est. < 0.2 s | **YES**, mandatory. Keep ≤ 4 influences for the web viewer |
| **Robust skin weights transfer** to overlapping shells (cloth, armour, hair cards). Copy weights only where the closest body point passes the distance and normal-angle checks, then inpaint the rest by interpolation. | leak to cloth, garment tearing, layers that pass through each other | Abdrashitov et al., SIGGRAPH Asia 2023: https://www.dgp.toronto.edu/~rinat/projects/RobustSkinWeightsTransfer , Python code https://github.com/rin-23/RobustSkinWeightsTransferCode | n/r; est. 1–3 s (sparse solve on the shell) | **YES** |
| **Rigid-shell binding.** Small or rigid shells (eyes, buttons, plates, buckles, held props) get a single bone at weight 1.0. That bone is the dominant bone of the body region they sit in, or the hand for props. | stretch on rigid parts, prop | AccuRIG surface segregation: https://cdn-www.reallusion.com/auto-rig/accurig | est. < 0.05 s | **YES** |
| **Surface geodesics by the heat method.** One prefactorisation, then a near-linear solve per bone seed. Use it for single-shell, clean meshes, or as a tie-breaker inside a voxel region. | leak within one shell along the surface | Crane et al. 2013, "an order of magnitude faster": https://arxiv.org/abs/1204.6216 ; `potpourri3d.MeshHeatMethodDistanceSolver`: https://github.com/nmwsharp/potpourri3d | est. 1–2 s factorisation + ~0.05 s per bone | **YES** (as a supplement; it cannot cross shells) |
| **Envelope weights.** Distance to the bone's influence capsule (today's per-joint capture radius is this family). | — | Blender *With Envelope Weights*: https://docs.blender.org/manual/en/latest/animation/armatures/skinning/parenting.html | trivial | keep only as a fallback or a gate, not as primary weights |

---

## 4. Deformation model

| Technique | Fixes | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Linear blend skinning (baseline).** | — (causes collapse and candy) | glTF skinning is LBS: per-vertex matrices are "weighted linear sums of the joint transformation matrices": https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html | runtime only | the export format, so all fixes below must also survive as LBS data or as extra bones |
| **Dual quaternion skinning.** | candy, volume loss at twists (bulges at bends) | Kavan et al. 2007, "solves the artifacts of linear blend skinning at minimal additional cost": https://dcgi.fel.cvut.cz/en/publications/2007/kavan-i3d-sdq/ | runtime only | **YES** in our own viewer shader; glTF/FBX consumers still play LBS |
| **Optimised centres of rotation (CoR).** Precompute one CoR per vertex from rest pose and weights; at runtime blend rotations with a quaternion and translate by the CoR. | collapse ("collapsing elbows"), candy, joint bulges | Le & Hodgins 2016: https://la.disneyresearch.com/publication/skinning-with-optimized-cors/ ; C++/GLSL implementation (GLM only, CPU multithreaded + vertex shaders): https://github.com/pmbittner/OptimisedCentresOfRotationSkinning | n/r; est. 1–5 s precompute at 50k (each vertex integrates over similar-weight triangles) | **YES** for the viewer (store CoR as a vertex attribute); not portable to plain glTF |
| **Twist and helper joints.** Split the upper arm, forearm and thigh into 2–3 segments that each take a fraction of the child's twist. Add elbow/knee helper joints that rotate at half the joint angle. | candy, collapse, and portable to every LBS consumer | Jacobson & Sorkine 2011, Stretchable and Twistable Bones (an extra scalar weight per bone fixes candy-wrapper): https://alecjacobson.com/weblog/2104.html ; UE Pose Driver uses swing/twist angle distance methods for corrective drivers: https://dev.epicgames.com/documentation/en-us/unreal-engine/pose-driver-in-unreal-engine | est. < 0.2 s (bones, plus a weight split along the bone axis) | **YES** (the most portable fix) |
| **Delta Mush / Corrective Smooth.** Smooth the deformed mesh and add back the rest-pose smoothing delta in the surface frame. Used on all R&H rigs since 2010. | collapse, crumpling at joints, noisy weights | Mancewicz et al. 2014: https://history.siggraph.org/?p=138877 ; Blender *Corrective Smooth* (Simple / Length Weight, Scale for volume loss): https://docs.blender.org/manual/en/latest/modeling/modifiers/deform/corrective_smooth.html | runtime: est. 2–10 ms per frame at 50k (CPU) | **YES** for preview renders, QA and baked clips; not in glTF |
| **Direct Delta Mush.** A direct (non-iterative) form with separate control of bending vs twisting, plus skin sliding. | the same as Delta Mush, real-time | Le & Lewis 2019: https://ea.com/seed/news/siggraph2019-direct-delta-mush | n/r | **BG** (viewer research item) |
| **Pose-space deformation / RBF correctives.** Corrective shapes or bones keyed by joint angles. | collapse at the shoulder/elbow/knee, muscle shape | Lewis et al. 2000: https://history.siggraph.org/?p=118265 ; UE Pose Driver (RBF; additive or interpolative): https://dev.epicgames.com/documentation/en-us/unreal-engine/pose-driver-in-unreal-engine ; learned correctives: https://peizhuoli.github.io/neural-blend-shapes/ | authoring needs example poses | **BG** (only generic per-category correctives are realistic) |
| **Implicit skinning.** Correct LBS/DQS positions with composed implicit surfaces per bone; handles skin contact and bulges without collision detection. | limb-in-body at deep flexion, collapse | Vaillant et al. 2013: https://imagine.inrialpes.fr/people/Damien.Rohmer/documents/publications/13_siggraph_implicit_skinning/13_siggraph_implicit_skinning.html | runtime, GPU | **NO** for export; it inspires the push-out pass below |
| **Joint-based collision push-out.** Give each bone segment a capsule (radius from the medial distance). After skinning each clip frame, push vertices of other regions out of the capsules of non-adjacent bones, or reject the frame in QA. | limb-in-body (arm through torso), knees through each other in clips | practice, after implicit skinning (above) and contact-aware retargeting (section 5) | est. < 5 ms per frame | **YES** as QA; as a bake step on background clips |

---

## 5. Animation retarget and QA

| Technique | Fixes | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Chain-based retarget with a matched retarget pose.** Map source chains to target chains (spine, neck, L/R arm, L/R leg, tail, wings). Match the rest poses first (T vs A pose). Scale pelvis/root motion in proportion. | limb-in-body after retarget (A/T mismatch), slide caused by wrong root scale | UE IK Retargeter: chains, fuzzy chain mapping, the pelvis gives "root motion … transferred in a proportional way", editable retarget pose: https://dev.epicgames.com/documentation/en-us/unreal-engine/ik-rig-animation-retargeting-in-unreal-engine | est. < 0.5 s per clip | **YES** |
| **Speed-based foot planting (IK foot locking).** Compute a speed curve per foot. Below a threshold the foot's IK goal is pinned; a full-body IK solves the rest. | slide | UE "Fix foot sliding with IK Retargeter" (speed curves, Speed Threshold, IK goals pinned): https://dev.epicgames.com/documentation/unreal-engine/fix-foot-sliding-with-ik-retargeter-in-unreal-engine | est. < 0.2 s per clip | **YES** |
| **Limb-length scaling and stride normalisation.** Scale root translation by the target/source leg-length ratio (hip height for bipeds, withers height for quadrupeds). Scale IK foot targets about the hip so the step length per leg length matches the source. | slide, overstretched legs ("stretch" in clips), floating feet | root proportionality per UE (above); the stride ratio is practice | est. < 0.1 s | **YES** |
| **Contact-aware retarget.** Detect self-contacts and ground contacts in the source, then optimise the target to keep them and reduce self-penetration, conditioned on the geometry. | limb-in-body, hands through torso, slide | Villegas et al., ICCV 2021: https://arxiv.org/abs/2109.07431v1 | n/r | **BG** as a full method; take the penetration loss as a post-pass |
| **Skeleton-aware networks.** Unpaired retarget between homeomorphic skeletons via skeletal pooling to a primal skeleton. | retarget across differently sampled chains (2 vs 3 spine bones, extra twist bones) | Aberman et al., SIGGRAPH 2020: https://arxiv.org/abs/2005.05732 , https://deepmotionediting.github.io/retargeting | trained model | **BG** |
| **Any-topology motion generation.** A transformer diffusion over joints with textual joint descriptions; it generalises with "as few as three training examples per topology". | missing clips for new categories (monsters, birds, insects) | AnyTop 2025 (Truebones Zoo, 70 skeletons): https://arxiv.org/html/2502.17327v2 , https://anytop2025.github.io/Anytop-page | GPU minutes per batch | **BG** (library building) |
| **Video → motion for an arbitrary rigged asset.** Predict 3-D joint trajectories from a monocular video plus the asset, then IK-fit to the asset's rotations (BVH). | the per-category library from generated reference videos (the owner's "conveyor" order) | MoCapAnything: bipeds, quadrupeds, birds, reptiles, non-biological rigs: https://arxiv.org/html/2512.10881v2 , https://animotionlab.github.io/MoCapAnything/ | n/r | **BG** |
| **Video-guided rig animation optimisation.** Optimise per-frame joint rotations and root to match a generated video (RGB, mask, flow, depth, CoTracker tracks, smoothness). | the per-category library | Puppeteer: ~20 min per object (10k vertices, 50 frames): https://arxiv.org/html/2508.10898v1 | reported ~20 min | **BG** |
| **Human video → world-grounded motion.** Gravity-view coordinates; reduces foot sliding; outputs SMPL-X in the world frame. | biped library clips from generated videos | GVHMR, SIGGRAPH Asia 2024: https://github.com/zju3dv/GVHMR | n/r | **BG** |
| **Animal shape and gait from video.** hSMAL horse model fitted to video for gait (lameness) analysis; 3D-Fauna gives feed-forward articulated quadrupeds from one image. | quadruped/equine gait priors | https://arxiv.org/abs/2106.10102 ; https://arxiv.org/html/2401.02400v2 | seconds (3D-Fauna, feed-forward) | **BG** |
| **Multi-view video diffusion 4-D animation** of a static 3-D asset (reconstruct motion from generated multi-view video, then 4D-SDS). | an alternative source of reference motion | Animate3D, NeurIPS 2024: https://arxiv.org/abs/2407.11398v1 , https://animate3d.github.io/ | GPU minutes | **BG** |
| **Per-category motion libraries.** Store clips once per category on a canonical category skeleton, then retarget to each model. | the "one-offs" problem; consistent quality per category | Truebones Zoo, "over 75 Animated Animals with Textures, in .FBX format" (licence terms not stated on the page): https://truebones.gumroad.com/p/free-truebones-zoo-over-75-animated-animals-with-textures-in-fbx-format ; skeleton conventions from SMAL / AVES (section 2) | — | **YES** at run time (retarget only) |
| **Numeric clip QA: foot skate.** Toe speed on frames where toe height < 0.05 m; the skate ratio is the fraction of contact frames with toe speed > 0.2 m/s. Contact consistency uses speed 0.15 m/s and height 0.10 m. Scale the thresholds by leg length for non-human sizes. | slide | NVIDIA Kimodo benchmark metrics: https://research.nvidia.com/labs/sil/projects/kimodo/docs/benchmark/metrics.html | est. < 0.05 s per clip | **YES** |
| **Numeric clip QA: geometry.** All are practice; none is a cited standard. They are: per-edge length ratio vs rest (p99 and fraction > 1.3); signed volume ratio of the closed proxy per frame; self-intersection count (BVH triangle-triangle between non-adjacent regions); bone-in-mesh fraction per frame (winding number on bone samples); rigid-shell strain (must be ≈ 0); joint-angle limits per category. | stretch, limb-in-body, collapse, bone-out | inside test: https://dgp.toronto.edu/projects/fast-winding-numbers | est. 0.1–1 s per clip at 50k (sampled frames) | **YES** |

---

## 6. Multi-object rigs

| Technique | Fixes | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Prop as a rigid child node of the hand joint.** No skin at all; the prop node is parented under the hand bone (or a `prop_R` socket bone) with its rest offset. | prop (sword in the body), prop stretch | glTF tutorial: rigid objects follow joints as child nodes of joint nodes: https://github.khronos.org/glTF-Tutorials/gltfTutorial/gltfTutorial_020_Skins.html | ~0 s | **YES** |
| **Prop as a separate skinned primitive** with 100 % weight on the hand/socket bone in the same skin. Use it when the exporter needs one skinned object. | prop | glTF spec: "Only the joint transforms are applied to the skinned mesh; the transform of the skinned mesh node MUST be ignored": https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html | ~0 s | **YES** |
| **Socket / attachment bone.** A `prop_R`/`prop_L` bone under the hand, oriented along the grip axis (the principal axis of the prop shell). It allows swapping, dropping or re-gripping the prop in clips without re-skinning. | prop orientation errors during hand animation | practice; the rigid child-node rule above | est. < 0.05 s | **YES** |
| **Influence limits per primitive.** Up to 4 joints per vertex per set (`JOINTS_0`/`WEIGHTS_0`); `JOINTS_1` for more, but clients may ignore it. | weights silently dropped in viewers ("leak" that appears only in the viewer) | https://registry.khronos.org/glTF/specs/2.0/glTF-2.0.html | — | **YES** (enforce 4) |
| **Cloth layer.** Robust weight transfer from the body proxy, then Delta Mush/corrective smooth in preview renders, plus the capsule push-out QA. | cloth through the body, cloth tear | https://www.dgp.toronto.edu/~rinat/projects/RobustSkinWeightsTransfer ; https://docs.blender.org/manual/en/latest/modeling/modifiers/deform/corrective_smooth.html | est. 1–3 s | **YES** |
| **Hair cards, tails, capes as spring-bone chains.** Generate a joint chain along each card or strand cluster with stiffness, gravity and drag, plus sphere/capsule colliders on the head and body. | hair through the head; stiff hair; leak of hair weights into the neck | VRM 1.0 SpringBone (joints, springs, sphere/capsule colliders, collider groups as a glTF extension): https://vrm.dev/univrm1/vrm1_tutorial/springbone | est. < 0.5 s generation | **YES** (secondary layer; the viewer simulates it) |
| **Multi-mesh characters handled as one volume** for weights (GVB, voxel heat), while each shell keeps its own primitive. | leak between cloth and body, gaps between parts | Maya GVB "all shapes are treated as a single volume": https://help.autodesk.com/cloudhelp/2018/ENU/MayaLT-CharacterAnimation/files/GUID-5EFDB81B-E332-4D6C-B1BB-0B989AD2F2C7.htm ; AccuRIG multi-mesh with cloth: https://cdn-www.reallusion.com/auto-rig/accurig | in the GVB cost | **YES** |

---

## 7. Validation renders and QA visuals

All rows in this table are practice. Only the colour conventions are cited.

| Technique | Fixes (makes visible) | Source / URL | Cost | 60 s fit |
|---|---|---|---|---|
| **Per-bone weight heatmap** (blue = 0, red = 1) for chosen bones: hands, upper arms, thighs, spine. | leak, tear | Blender weight colour code (cold/hot, "blue means unweighted and red means fully weighted"): https://docs.blender.org/manual/en/latest/sculpt_paint/weight_paint/introduction.html | est. 0.2 s per view (GPU raster) | **YES** |
| **Dominant-bone categorical map.** Each bone gets a distinct colour; a leak shows as a foreign colour island, for example a thigh colour on the palm. Boundary lines are drawn where the dominant bone changes. | leak, prop binding, cloth binding | practice | est. 0.2 s per view | **YES** |
| **Normal-colour and position-colour shaders on deformed poses.** Normal RGB shows seams opening (colour discontinuity) and shading collapse. World-position RGB shows stretched edges and detached shells. | tear, stretch, collapse | practice | est. 0.2 s per view | **YES** |
| **4-view contact sheet × range-of-motion poses** (front, side, back, top). Poses: rest, elbows and knees flexed 120°, wrists and forearms twisted 90°, legs in a split, spine bent. Bones drawn in x-ray. | collapse, candy, limb-in-body, bone-out | practice | est. 2–4 s for 16 tiles | **YES** (feeds the textured Vision rig validation) |
| **Gait metrics overlay.** Step length / leg length, stance width / hip width, elbow width / shoulder width, and foot slide per contact phase over one cycle, plotted next to the GIF. | slide, splayed or collapsed gaits, arms in the body | foot thresholds from https://research.nvidia.com/labs/sil/projects/kimodo/docs/benchmark/metrics.html ; the ratios are practice | est. < 0.1 s | **YES** |
| **Before/after metric deltas** on the same poses and cameras for every tool run. | prevents silent regressions | `AGENTS.md` "Tools Have Options" | — | **YES** |

---

## Top 15: highest expected impact

This ranking assumes the rigger already has:

* a voxel trace skeleton;
* template bipeds and quadrupeds;
* Vision category classification;
* LBS skinning with a per-joint capture radius;
* a detach check with cut+cap lids;
* library clip retarget with numeric edge-stretch QA.

The order weighs the defect classes seen in production first. The cases named
in `AGENTS.md` include:

* the sword bound into the body;
* the arm through the torso;
* palm weights on the torso;
* a bone outside the mesh;
* the 60 s budget broken by the OpenPose sweep and three binds.

1. **Replace capture-radius weights with geodesic voxel binding (GVB / voxel heat) on one solid of all shells.**
   * Weights fall off with the geodesic distance through *interior* voxels, so
     they cannot jump across the gap between the legs or between the arm and
     the torso, or from the palm to the thigh.
   * It works on non-manifold, open, multi-shell meshes, and it is one pass
     instead of three Blender binds.
   * est. 1–5 s at 128–256³.
   * Maya GVB docs and the Voxel Heat add-on (4–5 s adaptive) are the
     references.
2. **Weld-for-skinning with weights copied to coincident vertices, plus a zero-difference assert.**
   * It removes seam tearing at UV and normal splits at the source.
   * < 0.1 s.
3. **Skeleton-graph adjacency mask with a geodesic region flood fill.**
   * A vertex may only take weight from bones within 1–2 hops of its region's
     bone.
   * It kills cross-limb leakage deterministically, including palm→torso and
     leg→leg.
   * < 0.5 s.
4. **Shell classification, rigid binding and held-prop separation.**
   * Sources: file groups (OBJ `g`/`o`/`usemtl`, glTF nodes), geometric
     heuristics, and PartField on fused meshes.
   * The prop becomes a rigid child of a `prop_R` socket bone; eyes, buttons
     and plates bind rigidly to one bone.
   * Fixes the sword-in-body class and stops bones growing into props.
   * < 0.5 s, plus about 2 s on GPU when PartField is needed.
5. **Joint centring by SDF and cross-section, with a winding-number bone-in-mesh assert.**
   * Every joint is moved to the centroid of the section that contains its
     bone, or to the medial maximum.
   * Each bone is sampled with the fast winding number; any sample outside
     fails the rig.
   * Fixes bone-out and off-centre elbows and knees, which feed collapse.
   * < 0.3 s.
6. **Canonicalise orientation and symmetry before skeleton fitting.**
   * Steps: PCA, then a learned orienter or a Vision pick over the 24
     octahedral flips, then the symmetry plane from PRS-Net or reflection ICP.
   * This replaces the 90 s OpenPose sweep over 24 candidates with 1–4 views.
   * It gives mirrored joints and mirrored weights for free.
   * About 1–3 s, and it saves most of the 90 s.
7. **Robust skin weights transfer for overlapping layers** (cloth, armour, hair cards) from the body proxy, using the distance and normal checks and then inpainting.
   * It replaces nearest-surface copying, which drags the cloth into the
     wrong limb.
   * est. 1–3 s.
8. **Analysis proxies: an alpha-wrap / UDF-closing solid plus decimation.**
   * All volumetric and medial computations run on a watertight proxy of
     5–20k vertices. Weights go back to the 300k render mesh by closest point.
   * Alpha is capped so real gaps stay open.
   * This keeps 300k meshes and broken AI meshes inside the budget.
   * About 2–10 s.
9. **Twist and helper joints** (2–3 twist segments per upper arm, forearm and thigh; half-angle elbow and knee helpers).
   * It is the only candy-wrapper and collapse fix that survives export to
     plain glTF/FBX LBS.
   * Optionally add optimised centres of rotation, or DQS in our own viewer
     shader.
   * < 0.5 s.
10. **Retarget foot planting by speed curves, plus root scale by leg-length ratio and stride normalisation.**
    * Directly fixes foot sliding and over-stretched legs in library clips.
    * < 0.5 s per clip.
11. **Weight post-processing chain as one deterministic, option-driven tool.**
    * Steps: prune under 0.01–0.02, limit to 4 lowest-first, normalise, mirror,
      then smooth on the welded topology only.
    * Over 4 influences, glTF viewers may drop the extras.
    * < 0.2 s.
12. **Extended numeric clip QA in the release gate and per run.** Add these to the existing edge stretch:
    * foot-skate ratio (toe speed > 0.2 m/s while toe height < 0.05 m,
      scaled by leg length);
    * self-intersection between non-adjacent regions (limb-in-body);
    * bone-in-mesh per frame;
    * volume ratio;
    * rigid-shell strain;
    * joint-limit violations.
    * < 1 s per clip.
13. **A learned skeleton and weights as a second opinion on GPU workers.**
    * Puppeteer (1.5 s + 0.03 s), MagicArticulate, UniRig, and
      Make-It-Animatable (< 1 s, bipeds).
    * Use it when the template fit score is low (monsters, unusual anatomy),
      and blend its weights as a prior inside the GVB adjacency mask, never
      alone.
14. **QA visuals for Vision and the owner.**
    * A dominant-bone categorical map, normal and position shaders on
      range-of-motion poses, and 4-view contact sheets.
    * Leaks and tears become visible to Vision in one frame.
    * The textured rig-validation rule still applies.
    * About 2–4 s, in the background after the viewer shows the rig.
15. **Category skeleton conventions and libraries.**
    * Skeleton conventions: SMAL/hSMAL, 3D-Fauna (8 spine + 3 per leg), AVES.
    * Per-category clip libraries built offline: Truebones Zoo as a seed
      (licence to be checked), plus generated reference videos turned into
      motion through MoCapAnything, GVHMR or video-guided optimisation.
    * Matches the "conveyor, not one-offs" order; run time is retarget only.

Honourable mentions, not in the top 15 because of export portability or
cost:

* Delta Mush / Corrective Smooth for preview renders;
* optimised CoR as a viewer-only shader;
* capsule push-out baked into background clips;
* BBW on a ≤ 5k proxy;
* contact-aware retarget losses.

## Gaps and caveats found during research

* **"Anything-to-Rig"** did not resolve to a distinct publication. The closest
  2025 works are RigAnything (Adobe, SIGGRAPH 2025), UniRig, Puppeteer,
  MagicArticulate and Anymate, all listed above.
* **Geodesic Voxel Binding** has no open-source implementation that this
  research could confirm, and the paper page answered 403. The Maya docs and
  the commercial Voxel Heat add-on describe its behaviour.
* **RigAnything:** no public code repository was confirmed.
* **Licences:** the SMAL and Truebones licences are not stated on the fetched
  pages. UniRig is MIT; TARig is GPLv3.
* **Mixamo requirements:** the Adobe FAQ answered 403. A third-party guide
  states the usual constraints: a single manifold mesh, no floating parts,
  T/A pose, symmetric, centred, < 100k triangles, < 50 MB:
  https://www.neural4d.com/how-tos/how-to-prepare-3d-models-for-auto-rigging-and-animation .
  Those constraints are exactly what sections 1 and 6 remove for our input
  range.
* **Costs:** all "est." costs are unverified. Measure them on a farm box
  against the regression corpus before they are relied on for the 60 s budget.

# Horse leg proportions and contact review

The user rejected the server native gallop as a flying horse and requested
anatomical references and photographs. Successful binding and agreement with
our target coordinates are not anatomical acceptance. The native gallop stays
unapproved; further anatomy changes are being evaluated offline.

## Reference definitions

* [Matsuura et al. (2008)](https://doi.org/10.1294/jes.19.9): 35 horses of several
  breeds/sizes, four groups. Humerus/radius means are 0.77-0.81;
  radius/metacarpus means are 1.60-1.69. Side photographs used external markers;
  radius/metacarpus were also measured on the horses. Figure 1 separates the
  distal-radius marker from the proximal-metacarpus marker.
* [Mostafa and Elemmawy (2020)](https://doi.org/10.1294/jes.31.23): the 17 normal
  jumping Thoroughbreds have lateral forearm/cannon means 45.5/28.8 cm, about
  1.58. Figure 1 shows frontal and lateral skin-marker measurement photographs.

Both original figure pages were rendered and visually checked in project
`work/horse2-anatomy-audit/references`. These external points are not identical
to internal joint centres. The ratios provide comparison evidence, not a
universal clamp for every horse or stylized mesh. Hind ratios vary more.

[Nauwelaerts et al. (2011)](https://pmc.ncbi.nlm.nih.gov/articles/PMC3089746/)
describes measurements between articular-surface centres in dissected limbs;
its centre-of-mass percentages are not segment-length proportions.
[UMN veterinary anatomy](https://pressbooks.umn.edu/largeanimalanatomy/chapter/thoracic-limb-forelimb/)
and [UMN conformation](https://extension.umn.edu/agriculture/animals-and-livestock/horse/conformation-of-the-horse)
provide anatomical/stance context. An ARP bone named `clavicle` must not
automatically be measured as the entire equine scapula.

## Corrected identity and contact audit

Use `horse2-anatomy-audit-v3.json`, SHA-256
`7531a2a5b38ce4ed7c19a205b737c39dd7ae635f03bc28d22f117b86b2d5f219`.
Earlier paths through `thigh_stretch.head` are superseded: this point belongs
to a short stretch/twist subsegment near the distal joint. The verified fore
elbow is `thigh_twist.head`. Do not add overlapping ARP bone lengths to infer
anatomical limb length.

The tested rig path is `c_thigh_b.head -> thigh_twist.head -> leg_stretch.head
-> foot.head -> toes_01.head -> toes_01.tail`, plus measured rigid offset to
the selected hoof surface. Both hind `c_thigh_b` roots coincide at the pelvis
centre; these are not two anatomically established hip centres.

Conservative fore reach reserves in REST are 32.4/27.5 mm. In some native-gallop
samples they fall to -24.0/-43.9 mm. Pose scale channels are one and object world
scale is accounted for. A weight-derived surface heuristic finds each fore
hoof near ground in 1 of 19 source samples, versus 7 of 19 per hind hoof.
This is a rig/contact diagnostic, not gait ground truth or veterinary advice.
Source playback is 24 FPS with a 0.75-second cycle; frame zero is deeply posed,
not REST. Correct proportions alone do not prove valid gallop contacts.

## Isolated carpus experiment

The known P1 compact full-mesh rig was copied and rebound after moving only
the fore carpus along its elbow-carpus-fetlock polyline to a trial Euclidean
radius/cannon ratio of 1.62. Shoulder, elbow and fetlock positions remain fixed.
No production profile or shared default was changed.

| Side | Radius before/after (mm) | Cannon before/after (mm) | Ratio before/after | Carpus shift (mm) | Path change (mm) |
| --- | --- | --- | --- | --- | --- |
| Left | 149.22 / 181.12 | 143.72 / 111.80 | 1.038 / 1.620 | 31.92 | -0.014 |
| Right | 155.36 / 188.57 | 149.62 / 116.40 | 1.038 / 1.620 | 33.22 | -0.012 |

Distances use the normalized model scale. The experiment redistributes two
segments; it does not lengthen the entire leg or solve the gallop trajectory.
Original mesh positions, UVs, corner normals, materials, all hind bones and
proximal/distal endpoints remain unchanged. Fresh exact-weld Heat Map binding
covers all 119,996 render vertices.

Artifacts are in project `work/horse2-anatomy-audit/carpus-v1`. New actionless
source SHA-256: `2419ce54b66945e9ec1f34fad4e26a598f2dd0b907147a6465443cb2ce6344ba`.
The REST comparison is `outputs/horse-leg-proportions-comparison.png`, also
published in DEV as `81a8f0b7aa39`. Both panels show the same mesh; the overlay
identifies the changed joint.

Eight contact-controlled clips were rebuilt at 30 FPS. Their four-influence
weights were recomputed for the exact new geometry and clip manifest. Actual
Blender comparison over 2,280 quarter-frame samples passes with maximum skin
deviation 2.691 mm and no sample above the fixed 3 mm gate. Maximum hoof target
error is 0.727 mm for trot and 0.381 mm for forward walk. All 16 actual GLB/FBX
reimports pass. Optimized Blend SHA-256:
`1b3ac9849535cc21189d596cb4010553e743523eef6b033a577b23681508df9c`;
GLB: `e12a6f346039a73facc78325cb224a809cdd0df0da588179ee283a2688d208d2`.

The same-action walk comparison is published in DEV as `5a08e4c4b0fa` and
delivered as `outputs/horse-leg-walk-comparison.mp4`: 96 frames, three cycles,
30 FPS, 1920x540. It compares the prior and new proportion candidates on the
same contact-controlled walk, rather than conflating a gait change with a
proportion change. The DEV-only reproducible ratio helper is preserved in
converter main `23232fb57706063852194de2a3ccd60aac94df07`; it has no runtime
import or setting effect.

## P3 separated hip and stifle experiment

Canonical Horse_2 inspection establishes that `c_thigh_b` is the femur and
starts from a separate hip on each side. The fitted model instead places both
roots at the same pelvis point. The generic fitter's shared back-guide point
explains this collapse; it is not the canonical skeleton's intended topology.
The canonical lengths, scaled by the original bbox factor, match the source
lengths captured in Stage 3. Although reading mutable edit-bone lengths is a
potential ordering concern, it did not cause this case's proportions.

An isolated P3 candidate separates the hips by 122.94 mm using the canonical
femur directions, then solves anterior stifles at trial femur/tibia 0.85 and
metatarsus/tibia 0.65. These are explicit hypotheses informed by the external
marker studies above, not universal internal-joint ratios. Hocks, fetlocks,
toes and all forelimb joints remain fixed. New hip/stifle points are inside
the mesh with 14.6–23.8 mm two-sided surface clearance. Sixteen proximal hind
and twist bones change; mesh positions, topology, normals, both UV layers,
materials and hierarchy remain unchanged.

Candidate source SHA-256:
`a2de1c8d84fd9038a90d5013c253a7d0271f3d9d5b80ac4dc1394ced6ee62fbc`.
Evidence is under project `work/horse2-anatomy-audit/hind-experiment-v1`.
The REST side/rear comparison is published in DEV as `fac5826fd3f0` and
delivered as `outputs/horse-hind-joints-comparison.png`. This is an anatomy
candidate, without user approval of the resulting gait.

## Binding and whole-skeleton acceptance gap

P3's ten draft actions pass representation fidelity checks, but an independent
spatial audit finds an unacceptable anatomical problem. In the intersection
of Y at/below the 5th vertex percentile and Z at/above the 75th vertex
percentile in REST, all 4,883 vertices are dominated by
`clavicle.r`. The full native weights have the same contamination before
four-influence reduction. Head motion relative to the root is only 1.91° over
the run cycle and the head has no clavicle ancestor. The head-region motion
differs from rigid head-bone motion by up to 121.9 mm RMSE.

The blueprint also places `head.x` from approximately Y=-0.153, Z=0.488 to
Y=-0.285, Z=0.378, below the actual high/front head surface. The subneck chain
spans only Z=0.440–0.481. This requires investigation of neck/head fitting
before any weight-only repair. The audit is
`head-neck-binding-audit.json`, SHA-256
`6afe76621f0fc8337e7e5f410473ed0cf06c6094b78ade8ba4d9e7f9bc57582c`.
The anatomical diagnosis, rather than compression fidelity, is the acceptance
gate.

The original semantic graph retains the complete 55-point neck curve: `seg_17`
from node 390 to 413, then reversed `seg_10` from 413 to 88, total arc
0.422678 m. Node 88 is a cranial junction with separate branches to upper
cranial node 97 and muzzle node 106. Node 413's side branch leads directly to
torso `center_core` node `mj_1`. The final fork scan ignores nodes that are
themselves core, but currently treats this core-directed side branch as a
cranial fork and discards the continuation. Stage 3 then applies usage 0.8
even to a neck-only fork guide, shortening the already bounded path again.

The earlier accepted shoulder-layout P1 also has this contamination. Its
local forelimb experiment removed all old weights and rebound the entire
mesh while head/neck bones remained misplaced. The primary F11 task
independently reproduced the same failure. Do not attribute first occurrence
solely to the F11 rollout. The diagnostic overlay was published in DEV as
`eb168ea5aa86` and delivered as
`outputs/horse-head-placement-diagnostic.png`.

A first P4 trial distributed the neck chain along the restored curve and
placed the head base at node 88. The neck endpoints passed containment, but
the preserved head vector put its tail outside the cranial mesh. The
pre-bind gate rejected it: no candidate Blend, new weights or bind attempt
was produced. An isolated follow-up tests rotation toward measured muzzle
node 106 while preserving the head-bone length.

### P4b geometry and head-motion result

P4b passed the independent saved-Blend geometry checks. Exactly ten declared
head/neck/ear bones changed; all other bones and every checked mesh attribute
remain identical to P3. The head bone retains its 171.456 mm length and ends
9.98 mm before the measured muzzle point. All neck/head endpoints and sampled
shafts lie inside the mesh, with exact chain continuity. Source SHA-256:
`8fef6c6af9e51902b070a345c964a55b0051bcba4d4569914b7179916a9d789d`.
The old/new skeleton overlay is in DEV `27018fbd03a0` and
`outputs/horse-head-neck-correction.png`.

After a fresh full bind, both independent head-side regions have zero clavicle
influence and complete head/neck/ear ownership. All four hoof weight regions
are unchanged. Ten draft actions were rebuilt and applied in Blender. Actual
run evaluation over 21 keys shows maximum head-region rigid-motion residual
of 1.407 mm RMSE against the head bone, versus P3's 121.874 mm. Against the
clavicle it is now 125.527 mm: the region follows the head instead of the
shoulder. This is independent anatomical deformation evidence, separate from
compression fidelity.

The initial blanket upper-neck clavicle threshold proved too restrictive.
Most residual clavicle mass (83.69%) lies in a shoulder/neck-base transition
where the affected vertices are closer to clavicle shafts. The remaining
16.31% and side asymmetry need localized assessment, rather than zeroing every
clavicle weight in a broad quantile mask.

Ear placement remains unfinished. One right ear shaft leaves the mesh. An
initial cranial slice excluded the highest ear vertices and incorrectly
suggested inseparable ears; a full-model height sweep disproved that result.
At Z >= 0.86, two distinct caps contain 53 and 39 welded vertices, with tips
near [0.09053, -0.40768, 0.87489] and [0.02696, -0.43531, 0.87348]. They merge
between Z=0.84 and 0.85. Use component descent to identify separate stems;
do not mirror or adopt the earlier cropped-region extrema. Isolated 0.1-radian
ear rotations move the muzzle by at most 0.531 mm and the body by 0.919 mm, so
the broad head-region ear fraction alone does not prove wholesale face drag.

Diagnostic rendering uses P4b's full native weights (up to nine influences),
from `fullskin-review.blend`, SHA-256
`ea94eb099a199c37f036500b68e5e4bddb84bbbeed559c30a4f898be9826e20e`.
It is not a validated four-influence game export. Recompute weight reduction
and final format/deformation QA after the remaining anatomy changes.

The side run diagnostic is published in DEV `7c231b443a2b` and delivered as
`outputs/horse-run-corrected-head-neck.mp4`: 120 decoded frames, six cycles,
30 FPS, 4.0 seconds, 960x540. All 20 unique phases were inspected. The preview
camera now reserves space above the restored head/ears so the caption does
not obscure anatomy. The first overlapping-caption render was stopped and
retained as diagnostic frames; the completed second render is the delivered
video. This is a draft gait/deformation review, not gameplay approval.

### P4c ear placement experiments

Following each top cap down to the first *mutual* component merge was not a
valid ear-base detector. The left cap had already merged with a cranial crest,
inflating its inferred centerline to 64 mm versus 34 mm on the right. The
first P4c trial was rejected before binding: the left proximal ear shaft
crossed outside the mesh even though its endpoints were inside. Front and
three-quarter views confirmed the erroneous crest attachment. Preserve
`ear-fit-v1` and `p4c-rig` as rejected evidence; do not encode this spurious
twofold asymmetry into a rig profile.

V2 instead tracks separate closed cross-section loops from each cap. At
Z=0.842, both ear loops are stable and a third crest loop remains separate.
At Z=0.840 they join a skull loop, with 4.22/5.06-fold area increases and
39.5/45.7 mm centroid jumps. The last clean loops therefore supply separate
stems. The interior tips come from a high closed section, rather than a
surface extremum. All 44 samples along the four straight bone shafts pass
three-ray containment, and the revised overlays keep both chains within their
visible ear protrusions. No symmetry or manual vertex painting is used.

The resulting `p4c-v2-rig` source is
`649b56c2af117789262b16dae6d2b18e59d5c096458e45c162620dcb6aad1570`.
Only four ear bones change from P4b. Full native skin SHA-256:
`76be1020f932dbf13547a38cea25e2a624125514aa7a048271e379718bf2125d`.
The mesh attributes and all head/neck/limb bones remain unchanged. Initial
near-axis ear regions have complete same-side ownership without opposite-ear,
head or clavicle weights. This is candidate-builder evidence; independent
region selection, isolated deformation and the new exact clip/export checks
must still pass before acceptance.

### P4d local ear-weight correction

P4c-v2's bones passed, but its fresh native Heat Map weights still coupled
proximal ear rotation to the skull and muzzle. Independent 0.1-radian tests
found skull motion up to 2.368 mm and muzzle motion up to 1.634 mm, while
neck/body and hoof preservation passed. The candidate therefore failed the
unchanged 1 mm remote-motion gate. Moving the bones alone did not close this
binding defect.

An initial geodesic collar also failed: it reached 80 original vertices on
the separately identified cranial crest, with support up to 0.834. The
accepted mask uses competing geodesic distances to clean ear seeds versus
crest/opposite-ear seeds, multiplied by a smooth C2 radial taper. The collar
radius is half the measured ear centerline length. Ear seeds retain support
one; crest/opposite-ear seeds, remote head controls and positions outside the
collar have support zero. The discarded ear weight is transferred to the
verified closest deform ancestor outside the ear chain, `head.x` here.

The mask metadata initially confused geometry and weight-file hashes. That
record is retained as rejected evidence. Use only
`ear-locality-v2/metadata-weights-bound.json`: the actual P4c-v2 weight hash
is verified, and its position array matches the geometry used for geodesics
exactly (`array_equal`, maximum difference zero). Geometry source and weight
source identities are recorded separately.

P4d full-native source SHA-256:
`9392288af2206090ba9e4b2ed02e7215cdca454fa6e4f40eec76f55ee124cb57`.
Full weights:
`ce9c73f3aa1d9ac80aa265b0347b7d155bce607775ad76a9900a835cbadcfa2c`.
Independent saved-Blender validation confirms zero skeleton/mesh changes from
P4c-v2, exact preservation of all 86 unrelated weight columns and every hoof
region, and changes only in four ear groups plus the head group. All remote
regions, including opposite ear, skull, muzzle, neck and body, remain exactly
stationary under each isolated 0.1-radian ear rotation. Own ears move as
expected. This closes the full-native locality correction.

The provisional naive top-four approximation does **not** inherit that exact
column-preservation result: truncation and renormalization change 4,706
unrelated values, including neck/subneck weights by up to 0.119392. Keep its
exactness failure and pending deformation fidelity visible. The full-native
Blend intentionally retains up to nine weights; `authoring-input4.blend`
is a separate provisional input for the four-weight authoring interface.
Its SHA-256 is
`4c9c15ab690267331287914829f745e5aa8a5598f6f1dd1c9a3ebb03b09b4075`.
It is not the final game asset. New optimized weights must be checked against
the corrected full-native baseline over the exact new ten-action manifest,
followed by independent locality checks and actual format reimports.

The reusable DEV helper is merged into converter main
`f4a7aaecb0ea0bbaa52e5cd386e8b70910be00d4`. Seven behavioral tests cover
competitive support, padded bone-index zero, source row-mass residuals,
shuffled palettes, generic ancestry, duplicate names, overlapping seeds and
malformed provenance. An independent recomputation with the final helper
matches P4d's saved full weights exactly. It has no runtime import or deploy
effect. Full-native correction status was published in DEV `4c0ec4eaed7f`.

P4d's new exact ten-action reduction passes the fixed holdout gate with
maximum error 2.234661 mm over 144,955,168 sample/vertex pairs and no sample
above 3 mm. The reusable Blender verifier then passed 2,426 actual quarter
frames with maximum error 2.249555 mm, unchanged mesh/material fingerprints
and at most four influences. Contact errors remain visible: run 1.770 mm,
sprint 4.741 mm, forward walk 0.704 mm, trot 0.761 mm. Sprint still needs gait
and contact refinement despite passing the existing 6 mm engineering gate.

Independent validation of the optimized Blend and NPZ also passes: no mesh
or skeleton changes, all hoof weight arrays exact, finite normalized weights,
own-ear motion present, and zero motion in opposite ear, skull, muzzle, neck
and body during the supported ear-base isolation tests. `ear_02.r` has merged
with a parent equivalence class valid for this exact ten-clip set, so separate
control of that distal ear bone is not promised. Adding new actions requires
recomputing reduction and its checks.

The full-native ear comparison is published in DEV `180653d5e793` and saved as
`outputs/horse-ear-binding-comparison.mp4`: 90 frames, 30 FPS, three seconds,
1280x690. Both versions use identical 0.1-radian motion and a temporary matte
diagnostic display; original asset materials are preserved. Cropped or
overexposed earlier review renders remain internal and are not delivered.

All twenty P4d GLB/FBX reimports now pass at every key and half-frame.
Maximum source/import surface difference is 9.61 microns for GLB and
1.32 microns for FBX. Reference arrays used 584,698,464 bytes. The optimized
Blend is 12,301,438 bytes, SHA-256
`4fca6799ffc70b5d3b2bba14f7bdce85aa1ca158a84b4f94b08b2d784be77b5c`.
The self-contained GLB is 12,268,372 bytes, SHA-256
`918d5ca7a30fa71a6a46f5d78e53f63a5afe32616792ca26dda4cf6f691d85cd`.
It contains exactly ten named animations and only first-set joint/weight
attributes. The review copy is `outputs/horse-gameplay-p4d.glb`, with a compact
validation record beside it. This closes the current candidate's binding,
four-weight fidelity and format checks; it does not establish complete gait,
controller or cross-model readiness.

### Reusable neck truncation correction

Converter main `03a13a2c7aed91cad73e45d2ba4eac79391a4bfe` is pushed and clean.
Stage 2 ignores an off-path fork only when every side neighbor belongs to
torso core. Mixed core/non-core forks retain the existing stop behavior.
New diagnostics cap detail at 16 rows and eight neighbors, retaining total
and truncated counts. Stage 3 uses full length for a fork guide; terminal-tip
guides retain configured usage and the virtual head slot. Finite explicit
stop points must match the raw endpoint before mutation; older guides without
that optional point use a recorded compatibility path.

Six graph regressions pass, including the core-side/cranial-fork combination
and long-path truncation counts. Eleven actual Blender cases verify full
fork usage, terminal 0/0.8/1, legacy metadata, boolean fork override and
malformed/nonfinite/mismatched inputs with unchanged bone geometry, hierarchy
and rolls on failure. Related forelimb and bind regressions pass, including
four actual Blender proxy cases. The source correction is not deployed.
It deliberately does not add P4b's head rotation or complete ear fitting to
runtime; those still require a reusable geometry contract and verification.

Consequently, previous coverage, compression and export PASS results establish
finite data and fidelity to their baseline only. They do not establish correct
anatomical skin assignment. Whole-skeleton placement, independent body-region
influence checks and visible mesh deformation must pass before the gait is
accepted. Remaining work includes this correction, gallop polish, other
gameplay actions, transitions and validation on distinct models/species.

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

Remaining work includes anatomical identification of hind hip/stifle controls,
shoulder/scapula placement and usable reach, contact-aware gallop authoring,
then remaining gameplay actions and controller transitions. This ratio
experiment is not complete anatomical or gameplay approval.

# P6 regional spine articulation

The user identified a wooden-looking back in the P5 gallop. Direct inspection
confirmed that `root.x` and `spine_01.x` through `spine_03.x` had one constant
local transform in every P5 clip. P5's exact-clip reducer therefore merged
some of their equivalent skin transforms. Adding spine keys to that optimized
asset would not restore the intended full-native deformation.

The P4d native reference already has a usable deforming chain and distributed
body weights. Pelvis `root.x` and `spine_01.x` are siblings under the export
root; `spine_02.x` and `spine_03.x` continue the anterior chain. Isolated small
rotations deform the full mesh locally. No rest-bone or mesh change/rebind is
needed for this experiment. Native weights are retained as the reference and
the changed clip bank must be reduced and validated anew.

## Motion contract

`spine_motion` is an explicit optional gait profile. The initial model,
`hind_protraction_sagittal`, drives regional sagittal rotations from the same
bounded hind-hoof paths used by the leg solver. It normalizes by the known
path excursion without clipping the signal. Pelvis and three spine controls
have separate amplitudes and small phase delays. All rotations precede limb
IK, so the original hoof targets and joint limits remain enforceable.

Canter measurements show coordinated spinal motion related to limb
protraction/retraction, with the largest relative flexion-extension in the
lumbosacral region. See [Faber et al., 2001](https://pubmed.ncbi.nlm.nih.gov/11721556/).
A separate treadmill study reports changing lumbosacral excursion with canter
speed. See [Johnson and Moore-Colyer, 2009](https://pubmed.ncbi.nlm.nih.gov/19469240/).
These are guides to restrained regional movement, not direct angular targets
for the coarse rig controls; those controls are not individual vertebrae.

The selected `horse_p6_spine.experimental.v2.json` profile uses pelvis -2.5°,
spine amplitudes [1.5°, 1.5°, -2.5°], and phase delays [0.015, 0.030, 0.045].
The anterior counterrotation limits forequarter coupling while lumbar and
pelvic articulation remain active. This first implementation is sagittal and
targets run/sprint; other gaits and lateral/axial motion need their own verified
profiles. Existing profiles without this field retain their old behavior.

## Verified native result and limitations

Evidence is in project `work/horse-spine-review`. P6v2's full-native Blend is
`v2/new-fullskin.blend`, SHA-256
`8f0ae417c13c25520b02a57be6935b574e0c07387ba845d1fbd5bdf395ee3ffc`.
The rest skeleton, hierarchy, mesh positions and attributes match P4d/P5.
Run/sprint targets, contacts and all leg joint bounds remain unchanged.

An initial rotation audit used ranges of absolute quaternion angle and
under-reported movement of a bone whose rest transform was near 180°. That
metric is superseded. Sign-invariant pairwise quaternion distances correctly
match authored values: pelvis about 4.60–4.63°, spine01 2.77–2.78°, spine02
2.78–2.79°, spine03 4.60–4.66°. P5 is zero for all four controls. Independent
root-removed dorsal surface measurements retain additional non-rigid motion,
so this is not just a different whole-body tilt.

The first candidate increased a shoulder/chest edge from 3.885 mm in REST to
13.014 mm at sprint frame 5; the same edge was 9.354 mm in P5. Counterrotation
reduces it to 11.061 mm. Dorsal max/p99 edge stretch is below P5, but the
remaining localized shoulder strain is explicitly retained as a limitation.
A continuous surface alone does not prove good skin deformation.

The diagnostic video removes global export-root motion in memory and projects
the actual regional controls onto a camera-facing plane. It is labelled as a
diagnostic, not as literal vertebrae or a grounded gameplay preview. Original
asset materials and geometry remain unchanged. The comparison is delivered
as `outputs/horse-spine-p6v2-comparison.mp4`, also in DEV `db0a390086b2`.

An additional -4.5° anterior counterrotation trial reduced the tracked edge
to 10.500 mm, but also reduced the first dorsal curve span to 20.325° in run
and 19.505° in sprint, close to/below P5. A fixed right-side shoulder crop
did not establish a clear shape improvement. That trial remains unselected;
do not optimize a single edge length at the expense of the requested back
articulation. The exact candidate and comparison are retained in
`counterrotation-sweep/minus4p5`.

## Final P6v2 export checks

The ten-clip bank has been freshly reduced from the full native reference.
The eight clips other than run/sprint remain byte-identical to P5. Numerical
holdout maximum error is 2.909324 mm with no samples over 3 mm. Actual Blender
evaluation covers 2,426 poses at quarter-frame spacing: maximum skin error
2.911477 mm, maximum hoof contact error 3.628251 mm. These distances describe
the normalized model, not the dimensions of a life-size horse. All twenty
GLB/FBX reimports passed. Mesh geometry, rest bones and packed materials are
unchanged; independent ear locality remains clean.

All four spine/pelvis controls are now separate transform-equivalence
classes. Independent surface measurements confirm that the four-weight skin
retains the articulated native surface motion. The deliverable GLB SHA-256 is
`f915066ab882ce52d471b0a0c13a970158b7ca15216a6d170a87f20b16861acd`.
The full-body comparison is `outputs/horse-gallop-spine-fullbody-p6v2.mp4`;
unlike the earlier torso diagnostic, it includes authored global body motion
and the moving floor reference.

## Reusable deformation gate

`spine_surface_retention.py` adds a regional check to
`validate_quadruped_skin_blender.py` for clips declaring spinal articulation.
It selects upper dorsal surface stations in the existing Y-body/Z-up
coordinate convention and compares angles between adjacent centroid
segments at every sampled pose. The measure is invariant to whole-body
translation/rotation. Ordered longitudinal stations are required; shuffled
bone keys, missing stations and degenerate segments fail closed.

The maximum per-pose full-versus-compressed angular difference must stay
within 0.5°. This is an engineering compression-fidelity tolerance, not an
anatomical approval threshold. P6v2 passes at 0.122139° for run and 0.113500°
for sprint. A similar mean shape or a preserved motion range alone does not
satisfy this check. `--validation-only` runs the complete QA without creating
duplicate exports. It was used for the additional check of the unchanged
P6v2 asset; its original twenty reimports remain applicable.

Fifty-two authoring/body/timing/surface tests pass in total, including local
articulation, loop closure, preserved planted-foot targets and limits,
malformed profiles, rigid-motion invariance, lost-articulation rejection and
shuffled station rejection. P6v2 remains an offline candidate: ten clips
(including five idle clips) do not complete jumps, reactions, transitions,
other morphologies or the production controller pipeline.

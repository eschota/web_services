import copy
import unittest
import hashlib
import json

import numpy as np
import pytest

from animation_fitting.quadruped_clip_semantics import (
    FEET,
    apply_reference_actor_translation,
    require_v1_clip,
    require_v1_export_report,
    validate_v2_clip,
    V2_SEMANTIC_FIELDS,
    validate_profile_pin_formats,
    verify_profile_sources,
)


def fixture(mode="one_shot", seam="end_pose", action="jump"):
    blueprint = {"schema": "autorig-quadruped-authoring-rig.v1", "bones": [
        {"name": "root.x", "parent": None, "head": [0, 0, 0], "rest_local": np.eye(4).ravel().tolist()},
        {"name": "spine.x", "parent": "root.x", "head": [0, 0, 1]},
    ], "meshes": [{"name": "horse", "vertices": [{"point": [0, 0, 0]} for _ in range(8)], "faces": []}]}
    actor = [[0.1 * i, 0, 0] for i in range(4)]
    states = [True, False, False, True]
    targets = [[-actor[i][0], 0, 0] if states[i] else [0.2, 0, 0.2] for i in range(4)]
    frames = []
    for i in range(4):
        root_translation = [0, 0, 0] if mode == "loop" else [0, 0, 0.01 * i]
        frames.append({"time": i / 30, "bones": {
            "root.x": {"translation": root_translation, "rotation": [1, 0, 0, 0], "scale": [1, 1, 1]},
            "spine.x": {"translation": [0, 0, 0], "rotation": [-1, 0, 0, 0] if mode == "loop" and i == 3 else [1, 0, 0, 0], "scale": [1, 1, 1]},
        }})
    clip = {
        "schema": "autorig-authored-quadruped-clip.v2", "action": action,
        "timing": {"fps": 30, "sample_count": 4, "interval_count": 3},
        "playback": {"mode": mode, "seam_policy": seam},
        "motion": {"world_owner": "controller", "pose_root": "root.x", "pose_space": "actor_local",
                   "baked_actor_translation": False, "pose_root_offsets": [f["bones"]["root.x"]["translation"] for f in frames]},
        "reference_actor_motion": {"mode": "one_shot", "translations": actor},
        "ground": {"space": "reference_world", "height": 0, "tolerance": 1e-6},
        "frames": frames,
        "contacts": {foot: list(states) for foot in FEET},
        "hoof_targets": {foot: copy.deepcopy(targets) for foot in FEET},
        "surface_anchors": {foot: {"sole_vertices": [0, 1], "foot_vertices": [0, 1, 2]} for foot in FEET},
        "entry_contacts": {foot: True for foot in FEET},
        "phases": [{"kind": "support", "start": 0, "end": 1}, {"kind": "flight", "start": 1, "end": 3}, {"kind": "support", "start": 3, "end": 4}],
        "events": ([{"foot": foot, "kind": "liftoff", "sample": 1} for foot in FEET]
                   + [{"foot": foot, "kind": "touchdown", "sample": 3} for foot in FEET]),
    }
    # Events are sample-major, like the contract validator.
    clip["events"] = [{"foot": foot, "kind": kind, "sample": sample}
                      for sample, kind in ((1, "liftoff"), (3, "touchdown")) for foot in FEET]
    return clip, blueprint


class ClipSemanticsTests(unittest.TestCase):
    def test_placeholder_recipe_pins_are_rejected(self):
        names=('gameplay_profile_sha256','jump_profile_sha256','gameplay_profile_contract_sha256','jump_profile_contract_sha256')
        valid={name:'a'*64 for name in names}
        for value in ('1', True, None, 'g'*64):
            with self.assertRaises(ValueError):
                validate_profile_pin_formats({**valid,'jump_profile_sha256':value})
        for name in names:
            missing=dict(valid);missing.pop(name)
            with self.assertRaises(ValueError):validate_profile_pin_formats(missing)
        validate_profile_pin_formats(valid)

    def test_valid_one_shot_returns_readonly_context_without_mutation(self):
        clip, blueprint = fixture()
        original = copy.deepcopy(clip)
        context = validate_v2_clip(clip, blueprint)
        self.assertEqual(clip, original)
        self.assertEqual(context.sample_count, 4)
        self.assertFalse(context.targets[FEET[0]].flags.writeable)
        with self.assertRaises(TypeError):
            context.contacts["new"] = ()

    def test_v1_guards_are_exact(self):
        clip = {"schema": "autorig-authored-quadruped-clip.v1"}
        report = {"schema": "autorig-quadruped-export-candidate.v1"}
        self.assertIs(require_v1_clip(clip), clip)
        self.assertIs(require_v1_export_report(report), report)
        for bad in ({}, {"schema": "autorig-authored-quadruped-clip.v2"}, None):
            with self.assertRaises(ValueError): require_v1_clip(bad)
        with self.assertRaises(ValueError): require_v1_export_report({"schema": "other"})

    def test_actor_translation_is_once_only_and_nonmutating(self):
        points = np.array([[1.0, 2.0, 3.0]])
        before = points.copy()
        world, space = apply_reference_actor_translation(points, [4, 0, -1], sample_space="actor_local")
        np.testing.assert_array_equal(world, [[5, 2, 2]])
        np.testing.assert_array_equal(points, before)
        self.assertEqual(space, "reference_world")
        self.assertFalse(world.flags.writeable)
        with self.assertRaises(ValueError):
            apply_reference_actor_translation(world, [4, 0, -1], sample_space=space)

    def test_loop_seam_accepts_quaternion_sign_and_one_shot_needs_no_seam(self):
        loop, blueprint = fixture(mode="loop", seam="match", action="idle")
        validate_v2_clip(loop, blueprint)
        one_shot, blueprint = fixture()
        one_shot["frames"][-1]["bones"]["spine.x"]["translation"] = [9, 0, 0]
        validate_v2_clip(one_shot, blueprint)

    def test_all_air_hold_jump_air_is_valid_but_contact_is_rejected(self):
        clip, blueprint = fixture(mode="hold", action="jump_air")
        clip["contacts"] = {foot: [False] * 4 for foot in FEET}
        clip["entry_contacts"] = {foot: False for foot in FEET}
        clip["events"] = []
        clip["phases"] = [{"kind": "flight", "start": 0, "end": 4}]
        validate_v2_clip(clip, blueprint)
        clip["contacts"][FEET[0]][-1] = True
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)

    def test_rejects_bad_semantics_and_numeric_data(self):
        mutations = [
            lambda c, b: c.update(action="../jump"),
            lambda c, b: c["timing"].update(fps=True),
            lambda c, b: c["frames"][1].update(time=0.2),
            lambda c, b: c["playback"].update(mode="repeat"),
            lambda c, b: c["contacts"].pop(FEET[0]),
            lambda c, b: c["contacts"][FEET[0]].__setitem__(0, 1),
            lambda c, b: c["surface_anchors"][FEET[0]]["sole_vertices"].__setitem__(0, -1),
            lambda c, b: c["reference_actor_motion"]["translations"][0].__setitem__(0, float("inf")),
            lambda c, b: c["motion"]["pose_root_offsets"].__setitem__(1, [1, 0, 0]),
            lambda c, b: c["frames"][0]["bones"]["root.x"].update(rotation=[2, 0, 0, 0]),
            lambda c, b: c["frames"][0]["bones"]["root.x"].update(scale=[1, 0, 1]),
            lambda c, b: c["frames"][0]["bones"]["root.x"].update(translation=[True, 0, 0]),
            lambda c, b: c["ground"].update(height="0"),
            lambda c, b: c["events"].clear(),
            lambda c, b: c["phases"][0].update(kind="any"),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                clip, blueprint = fixture()
                mutate(clip, blueprint)
                with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)

    def test_rejects_planted_height_and_world_slide(self):
        clip, blueprint = fixture()
        clip["hoof_targets"][FEET[0]][0][2] = 0.01
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)
        clip, blueprint = fixture()
        clip["contacts"][FEET[0]] = [True, True, False, True]
        clip["hoof_targets"][FEET[0]][1] = [3, 0, 0]
        clip["events"] = [{"foot": foot, "kind": kind, "sample": sample}
                          for sample, kind in ((1, "liftoff"), (3, "touchdown")) for foot in FEET if foot != FEET[0]]
        clip["phases"] = [{"kind": "support", "start": 0, "end": 2}, {"kind": "flight", "start": 2, "end": 3}, {"kind": "support", "start": 3, "end": 4}]
        with self.assertRaisesRegex(ValueError, "slides"):
            validate_v2_clip(clip, blueprint)

    def test_incoming_entry_can_create_liftoff_at_sample_zero(self):
        clip, blueprint = fixture()
        clip["contacts"] = {foot: [False] * 4 for foot in FEET}
        clip["entry_contacts"] = {foot: True for foot in FEET}
        clip["events"] = [{"foot": foot, "kind": "liftoff", "sample": 0} for foot in FEET]
        clip["phases"] = [{"kind": "flight", "start": 0, "end": 4}]
        validate_v2_clip(clip, blueprint)

    def test_playback_pairs_and_loop_contact_seam_are_strict(self):
        clip, blueprint = fixture()
        clip["playback"] = {"mode": "loop", "seam_policy": "end_pose"}
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)
        clip, blueprint = fixture(mode="loop", seam="match", action="idle")
        clip["entry_contacts"][FEET[0]] = False
        clip["events"].insert(0, {"foot": FEET[0], "kind": "touchdown", "sample": 0})
        with self.assertRaisesRegex(ValueError, "contact state"):
            validate_v2_clip(clip, blueprint)

    def test_blueprint_root_sample_cap_and_ground_cap_are_enforced(self):
        clip, blueprint = fixture()
        blueprint["bones"][0]["rest_local"][3] = 0.1
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)
        clip, blueprint = fixture()
        clip["frames"] *= 901
        with self.assertRaisesRegex(ValueError, "2..3601"):
            validate_v2_clip(clip, blueprint)
        clip, blueprint = fixture()
        clip["ground"]["tolerance"] = 0.007
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)

    def test_legacy_schema_cannot_hide_unsupported_v2_semantics(self):
        for key in V2_SEMANTIC_FIELDS:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    require_v1_clip({"schema":"autorig-authored-quadruped-clip.v1", key:None})
                with self.assertRaises(ValueError):
                    require_v1_export_report({"schema":"autorig-quadruped-export-candidate.v1", "clips":[{key:None}]})

    def test_boolean_or_float_event_indices_are_not_integer_events(self):
        for bad in (True, 1.0):
            clip, blueprint = fixture()
            clip["events"][0]["sample"] = bad
            with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)

    def test_actor_translation_rejects_mixed_boolean_geometry(self):
        with self.assertRaises(ValueError):
            apply_reference_actor_translation([[True, 2, 3]], [0, 0, 0], sample_space="actor_local")

    def test_root_matrix_must_be_rigid_affine(self):
        for matrix in (np.diag([0.,0.,0.,1.]), np.diag([1.,1.,1.,2.]), np.diag([-1.,1.,1.,1.])):
            clip, blueprint = fixture()
            blueprint["bones"][0]["rest_local"] = matrix.ravel().tolist()
            with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)

    def test_anchors_require_one_mesh_and_a_structural_foot_subset(self):
        clip, blueprint = fixture()
        blueprint["meshes"].append(copy.deepcopy(blueprint["meshes"][0]))
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)
        for bad in ([[0]], [7]):
            clip, blueprint = fixture()
            clip["surface_anchors"][FEET[0]]["sole_vertices"] = bad
            with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)
        clip, blueprint = fixture()
        clip["surface_anchors"][FEET[0]]["foot_vertices"] = [999]
        with self.assertRaises(ValueError): validate_v2_clip(clip, blueprint)


def test_recipe_files_and_canonical_content_are_authenticated(tmp_path):
    clip={'profile_sources':{}}
    for name in ('gameplay_profile','jump_profile'):
        recipe={'fixture':True}
        if name=='jump_profile':recipe['landing_preload_height_fraction']=0.
        path=tmp_path/(name+'.json');path.write_text(json.dumps(recipe))
        clip['profile_sources'][name]=str(path)
        clip[name+'_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
        clip[name+'_contract_sha256']=hashlib.sha256(json.dumps(recipe,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    verify_profile_sources(clip)
    path=tmp_path/'jump_profile.json';path.write_text('{"different": true}')
    with pytest.raises(ValueError,match='file changed'):verify_profile_sources(clip)
    clip['jump_profile_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='content contract'):verify_profile_sources(clip)


if __name__ == "__main__":
    unittest.main()

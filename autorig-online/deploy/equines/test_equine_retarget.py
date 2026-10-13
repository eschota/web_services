import copy
import math
import unittest

import equine_library_release as release
import equine_retarget as rt


def skeleton(height=2.0, forward=(0, 0, 1), spine_length=1.0):
    return {
        "height": height, "ground_baseline": 0.0, "forward_axis": list(forward), "up_axis":[0,1,0],
        "bones": [
            {"name": "root", "parent": None, "rest_translation": [0, 1, 0],
             "rest_rotation": [0, 0, 0, 1], "rest_scale": [1, 1, 1],
             "head": [0, 0, 0], "tail": [0, 1, 0]},
            {"name": "spine", "parent": "root", "rest_translation": [0, spine_length, 0],
             "rest_rotation": [0, 0, 0, 1], "rest_scale": [1, 1, 1],
             "head": [0, 1, 0], "tail": [0, 1 + spine_length, 0]},
        ],
        "geometry": {"mesh": "unchanged"}, "materials": ["unchanged"],
        "weights": {"checksum": "unchanged"},
    }


def entry(action="walk_forward", frames=3, forward=(0, 0, 1), rotation_values=None):
    if rotation_values is None:
        rotation_values = [[0, 0, 0, 1], [0, 0, math.sin(math.pi/8), math.cos(math.pi/8)], [0, 0, 0, 1]]
    return {
        "clip": action, "loop": True,
        "action_metadata": {"frame_count": frames, "fps": 30.0},
        "reference_skeleton": skeleton(forward=forward),
        "tracks": {
            "root.translation": {"bone": "root", "property": "translation", "type": "translation",
                                 "values": [[0, 1, 0], [0.5, 1, 0], [1, 1, 0]][:frames]},
            "spine.rotation": {"bone": "spine", "property": "rotation", "type": "quaternion",
                               "values": rotation_values},
        },
    }


MAP = {"root": "root", "spine": "spine"}


class RetargetTests(unittest.TestCase):
    def test_ap_rig_adapter_uses_glb_rest_trs(self):
        class FakeRig:
            H = 2.0
            doc = {"bones": skeleton()["bones"]}
            rest = {bone["name"]: (bone["rest_translation"], bone["rest_rotation"], bone["rest_scale"])
                    for bone in skeleton()["bones"]}
            @staticmethod
            def forward(): return [0, 0, 1]
        adapted = rt.target_from_ap_rig(FakeRig())
        self.assertEqual(adapted["bones"][1]["rest_translation"], [0, 1.0, 0])

    def test_identity_calibration_preserves_absolute_local_motion(self):
        source = entry()
        clip = rt.retarget_entry(source, skeleton(), MAP)
        for actual, expected in zip(clip["rotations"]["spine"], source["tracks"]["spine.rotation"]["values"]):
            for av, ev in zip(actual, expected): self.assertAlmostEqual(av, ev, places=6)
        self.assertEqual(clip["hips_translation"], source["tracks"]["root.translation"]["values"])
        self.assertEqual(clip["duration"], 2 / 30)

    def test_ninety_degree_forward_calibration_rotates_delta_axis(self):
        qz = [0, 0, math.sin(math.pi/8), math.cos(math.pi/8)]
        source = entry(rotation_values=[[0, 0, 0, 1], qz, [0, 0, 0, 1]])
        target = skeleton(forward=(1, 0, 0))
        clip = rt.retarget_entry(source, target, MAP)
        middle = clip["rotations"]["spine"][1]
        self.assertGreater(abs(middle[2]), 0.3)  # same bone-local delta in calibrated frames
        self.assertAlmostEqual(clip["hips_translation"][-1][0], 0.0, places=6)
        self.assertAlmostEqual(abs(clip["hips_translation"][-1][2]), 1.0, places=6)

    def test_forward_parallel_horizontal_bone_uses_explicit_up_and_preserves_bend(self):
        source=entry(); target=skeleton()
        for value in (source["reference_skeleton"],target):
            spine=next(b for b in value["bones"] if b["name"]=="spine")
            spine["head"]=[0,1,0]; spine["tail"]=[0,1,1]
        clip=rt.retarget_entry(source,target,MAP)
        self.assertIn("spine",clip["retarget_metadata"]["source_anatomical_up_fallback_bones"])
        self.assertIn("spine",clip["retarget_metadata"]["target_anatomical_up_fallback_bones"])
        rest=clip["rotations"]["spine"][0]; bend=clip["rotations"]["spine"][1]
        self.assertAlmostEqual(abs(sum(a*b for a,b in zip(rest,[0,0,0,1]))),1.0,places=6)
        self.assertLess(abs(sum(a*b for a,b in zip(rest,bend))),0.999)

    def test_different_proportions_keep_target_bone_lengths_and_static_translations(self):
        source = entry()
        source["tracks"]["spine.translation"] = {
            "bone": "spine", "property": "translation", "type": "translation",
            "values": [[0, 1, 0], [0.2, 1.3, 0], [0, 1, 0]],
        }
        target = skeleton(height=3.0, spine_length=1.75)
        clip = rt.retarget_entry(source, target, MAP)
        self.assertEqual(clip["translations"]["spine"], [[0, 1.75, 0]] * 3)
        self.assertEqual(clip["retarget_metadata"]["target_bone_lengths_before"],
                         clip["retarget_metadata"]["target_bone_lengths_after"])

    def test_quaternion_sign_continuity(self):
        values = [[0, 0, 0, 1], [0, 0, 0, -1], [0, 0, 0, 1]]
        clip = rt.retarget_entry(entry(rotation_values=values), skeleton(), MAP)
        output = clip["rotations"]["spine"]
        for left, right in zip(output, output[1:]):
            self.assertGreaterEqual(sum(a*b for a, b in zip(left, right)), 0.0)

    def test_root_motion_rescales_by_height_and_preserves_grounded_rest(self):
        clip = rt.retarget_entry(entry(), skeleton(height=4.0), MAP)
        self.assertAlmostEqual(clip["hips_translation"][-1][0], 2.0)
        self.assertEqual(clip["hips_translation"][0][1], 1.0)
        self.assertAlmostEqual(clip["retarget_metadata"]["target_root_travel"], 2.0)

    def test_target_document_is_never_mutated(self):
        target = skeleton(height=3.0, spine_length=1.4)
        before = copy.deepcopy(target)
        rt.retarget_entry(entry(), target, MAP)
        self.assertEqual(target, before)
        self.assertEqual(target["geometry"], before["geometry"])
        self.assertEqual(target["materials"], before["materials"])
        self.assertEqual(target["weights"], before["weights"])

    def test_missing_bone_singular_frame_nan_and_legacy_curves_fail(self):
        with self.assertRaisesRegex(rt.RetargetError, "bone_map missing"):
            rt.retarget_entry(entry(), skeleton(), {"root": "root"})
        singular = skeleton(); singular["bones"][1]["tail"] = singular["bones"][1]["head"]
        with self.assertRaisesRegex(rt.RetargetError, "singular"):
            rt.retarget_entry(entry(), singular, MAP)
        nan_entry = entry(); nan_entry["tracks"]["root.translation"]["values"][1][0] = float("nan")
        with self.assertRaisesRegex(rt.RetargetError, "non-finite"):
            rt.retarget_entry(nan_entry, skeleton(), MAP)
        legacy = entry(); legacy.pop("tracks"); legacy["curves"] = {"spine": [0, 1, 0]}
        with self.assertRaisesRegex(rt.RetargetError, "legacy curves"):
            rt.retarget_entry(legacy, skeleton(), MAP)
        missing_up=entry(); missing_up["reference_skeleton"].pop("up_axis")
        with self.assertRaisesRegex(rt.RetargetError,"up_axis"):
            rt.retarget_entry(missing_up,skeleton(),MAP)

    def test_full_34_library_is_atomic_and_complete(self):
        rows = [entry(action=action) for action in release.BASE_ACTIONS]
        clips = rt.retarget_library(rows, skeleton(), MAP, release.BASE_ACTIONS)
        self.assertEqual([clip["id"] for clip in clips], list(release.BASE_ACTIONS))
        with self.assertRaisesRegex(rt.RetargetError, "complete required"):
            rt.retarget_library(rows[:-1], skeleton(), MAP, release.BASE_ACTIONS)

    def test_named_options_are_ranged(self):
        self.assertIn("root_motion_scale", rt.options_get()["values"])
        with self.assertRaisesRegex(rt.RetargetError, "outside"):
            rt.retarget_entry(entry(), skeleton(), MAP, {"root_motion_scale": 100})


if __name__ == "__main__":
    unittest.main()

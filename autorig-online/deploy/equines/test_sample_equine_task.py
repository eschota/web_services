import json, struct, tempfile, unittest
from pathlib import Path

import numpy as np

import sample_equine_task as sampler


def fixture(path, *, morph=False, omit=None, explicit_loop=None):
    names = ["Hips", "Head", "Tail_End"] + [name for chain in sampler.LEGS.values() for name in chain]
    if omit:
        names.remove(omit)
    nodes = []
    positions = {
        "Hips": [0, 0, 0], "Head": [0, 2, 0], "Tail_End": [0, 1, -1],
        "LeftFrontFoot": [.5, 0, .8], "RightFrontFoot": [-.5, 0, .8],
        "LeftHindFoot": [.5, 0, -.8], "RightHindFoot": [-.5, 0, -.8],
    }
    for name in names:
        nodes.append({"name": name, "translation": positions.get(name, [0, 0, 0])})
    joints = list(range(len(nodes)))
    mesh_node = len(nodes)
    nodes.append({"name": "ZebraMesh", "mesh": 0, "skin": 0})
    vertex_names = ["LeftFrontFoot", "RightFrontFoot", "LeftHindFoot", "RightHindFoot", "Head", "Hips"]
    pos = np.asarray([positions[name] for name in vertex_names], np.float32)
    joint = np.zeros((6, 4), np.uint16)
    for i, name in enumerate(vertex_names):
        joint[i, 0] = names.index(name)
    weight = np.zeros((6, 4), np.float32); weight[:, 0] = 1
    ibm = []
    for node in nodes[:-1]:
        matrix = np.eye(4, dtype=np.float32); matrix[:3, 3] = -np.asarray(node.get("translation", [0, 0, 0]))
        ibm.append(matrix.T.reshape(-1))
    times = np.asarray([0, 1], np.float32)
    hips = np.asarray([[0, 0, 0], [0, 0, .2]], np.float32)
    lf = np.asarray([positions["LeftFrontFoot"], [.5, -.5, .8]], np.float32)
    arrays = [pos, joint, weight, np.asarray(ibm, np.float32), times, hips, lf]
    blob = bytearray(); views = []; accessors = []
    types = [(5126, "VEC3"), (5123, "VEC4"), (5126, "VEC4"), (5126, "MAT4"),
             (5126, "SCALAR"), (5126, "VEC3"), (5126, "VEC3")]
    for array, (component, kind) in zip(arrays, types):
        while len(blob) % 4: blob.append(0)
        start = len(blob); raw = array.tobytes(); blob.extend(raw)
        views.append({"buffer": 0, "byteOffset": start, "byteLength": len(raw)})
        accessors.append({"bufferView": len(views)-1, "componentType": component, "count": len(array), "type": kind})
    primitive = {"attributes": {"POSITION": 0, "JOINTS_0": 1, "WEIGHTS_0": 2}}
    if morph:
        primitive["targets"] = [{"POSITION": 0}]
    animation = {"name": "Walk", "samplers": [{"input": 4, "output": 5, "interpolation": "LINEAR"},
                                                    {"input": 4, "output": 6, "interpolation": "LINEAR"}],
                 "channels": [{"sampler": 0, "target": {"node": 0, "path": "translation"}},
                              {"sampler": 1, "target": {"node": names.index("LeftFrontFoot"), "path": "translation"}}]}
    if explicit_loop is not None:
        animation["extras"] = {"loop": explicit_loop}
    document = {
        "asset": {"version": "2.0"}, "buffers": [{"byteLength": len(blob)}],
        "bufferViews": views, "accessors": accessors, "nodes": nodes,
        "meshes": [{"primitives": [primitive]}], "skins": [{"joints": joints, "inverseBindMatrices": 3}],
        "animations": [animation],
        "scenes": [{"nodes": [0, mesh_node]}], "scene": 0,
    }
    js = json.dumps(document, separators=(",", ":")).encode()
    js += b" " * (-len(js) % 4); blob.extend(b"\0" * (-len(blob) % 4))
    raw = struct.pack("<4sII", b"glTF", 2, 12+8+len(js)+8+len(blob))
    raw += struct.pack("<II", len(js), 0x4E4F534A) + js
    raw += struct.pack("<II", len(blob), 0x004E4942) + blob
    path.write_bytes(raw)
    (path.parent / "rig.json").write_text(json.dumps({"forward_axis": "+z"}))


class SamplerTests(unittest.TestCase):
    def test_actual_lbs_tracks_are_returned_but_video_identity_stays_pending(self):
        with tempfile.TemporaryDirectory() as td:
            run = Path(td); (run / "rig").mkdir(); fixture(run / "rig" / "rigged.glb")
            result = sampler.sample(run, "task-1", "Walk", fps=12, frame_cap=20)
            self.assertEqual(result["schema"], "autorig.equine-task-sample/1")
            self.assertEqual(result["source"]["sha256"], sampler.sha(run / "rig" / "rigged.glb"))
            self.assertEqual(set(result["sample"]["hooves"]), set(sampler.LEGS))
            self.assertEqual(len(result["sample"]["times"]), 13)
            self.assertTrue(result["provenance"]["rendered_rig_geometry_measured"])
            self.assertFalse(result["provenance"]["observed_motion"])
            self.assertFalse(result["evaluation"]["pass"])
            self.assertEqual(len([x for x in result["evaluation"]["pending"] if x.startswith("observed_hoof_identity")]), 4)
            for hoof in result["sample"]["hooves"].values():
                self.assertEqual(len(hoof["positions"]), 13)
                self.assertFalse(hoof["identity_observed"])
                self.assertIsNone(hoof["mirrored"])
                self.assertIn("vertex", hoof["anchor"])
            self.assertEqual(result["sample"]["ground"]["inference"], "minimum selected bind/rest sole vertex Y")
            self.assertLess(result["sample"]["hooves"]["LF"]["positions"][-1][1], 0)
            self.assertIn("ground_plane_confidence", result["evaluation"]["pending"])
            self.assertEqual(result["sample"]["axes"]["right"], [-1.0, 0.0, 0.0])
            self.assertEqual(result["sample"]["axes"]["forward"], [0.0, 0.0, 1.0])
            self.assertTrue(result["sample"]["axes"]["source_supported"])
            self.assertEqual(result["sampling"]["options"]["min_chain_weight"]["value"], .7)

    def test_morph_and_missing_exact_chain_fail_closed(self):
        for kwargs, text in (({"morph": True}, "morph"), ({"omit": "RightHindToe_End"}, "required unique node")):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as td:
                run = Path(td); (run / "rig").mkdir(); fixture(run / "rig" / "rigged.glb", **kwargs)
                with self.assertRaisesRegex(ValueError, text):
                    sampler.sample(run, "task-1", "Walk", fps=12, frame_cap=20)

    def test_rotation_uses_shortest_path_slerp(self):
        a = np.asarray([0, 0, 0, 1.0]); b = np.asarray([0, 1.0, 0, 0])
        middle = sampler.slerp(a, b, .5)
        self.assertAlmostEqual(float(np.linalg.norm(middle)), 1.0)
        self.assertAlmostEqual(abs(float(middle[1])), 2**-.5, places=6)

    def test_options_are_documented_and_range_checked(self):
        opts = sampler.options_get({"min_chain_weight": .8, "sole_band_height_ratio": .02})
        self.assertEqual(opts["min_chain_weight"]["value"], .8)
        self.assertTrue(opts["sole_band_height_ratio"]["doc"])
        with self.assertRaises(ValueError): sampler.options_get({"min_chain_weight": .49})
        with self.assertRaises(ValueError): sampler.options_get({"sole_band_height_ratio": .051})

    def test_missing_forward_metadata_never_guesses_an_axis(self):
        with tempfile.TemporaryDirectory() as td:
            run = Path(td); (run / "rig").mkdir(); fixture(run / "rig" / "rigged.glb")
            (run / "rig" / "rig.json").unlink()
            result = sampler.sample(run, "task-1", "Walk", fps=12, frame_cap=20)
            self.assertFalse(result["sample"]["axes"]["source_supported"])
            self.assertEqual(result["sample"]["axes"]["forward_source"], "pending")
            self.assertIn("source_supported_3d_axes", result["evaluation"]["pending"])

    def test_explicit_non_loop_walk_survives_clip_name_policy(self):
        with tempfile.TemporaryDirectory() as td:
            run = Path(td); (run / "rig").mkdir(); fixture(run / "rig" / "rigged.glb", explicit_loop=False)
            result = sampler.sample(run, "task-1", "Walk", fps=12, frame_cap=20)
            self.assertFalse(result["sample"]["loop"])
            self.assertEqual(result["sample"]["clip_type"], "one_shot")
            self.assertEqual(result["sample"]["loop_metadata"]["source"], "animation.extras.loop")
            self.assertNotIn("loop_metadata_source", result["evaluation"]["pending"])

    def test_float32_duration_keeps_requested_24fps_frame_count(self):
        duration = float(np.float32(5.833333492))
        self.assertEqual(sampler.frame_count(duration, 24, 600), 141)
        times = np.linspace(0.0, duration, sampler.frame_count(duration, 24, 600))
        self.assertEqual(float(times[-1]), duration)


if __name__ == "__main__":
    unittest.main()

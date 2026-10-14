import json
import pathlib
import struct
import tempfile
import unittest

from vehicle_generation.blender_vehicle_adapter import VehicleReceiptError, build, normalized_receipt


def receipt():
    return {
        "run_id_string": "car-test",
        "source_sha256_string": "a" * 64,
        "coordinate_system_object": {"units_string": "meters", "up_axis_string": "Z", "forward_axis_string": "-Y",
                                     "source_to_common_matrix_4x4_array": [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]},
        "body_object": {"dimensions_xyz_array": [4.2, 1.8, 1.35]},
        "parts_array": [
            {"part_id_string": "wheel.axle_0.left.0", "semantic_string": "wheel", "axle_index_int": 0, "side_string": "left", "pivot_xyz_array": [-0.82, -1.25, 0.42], "dimensions_xyz_array": [0.28, 0.72, 0.72]},
            {"part_id_string": "wheel.axle_0.right.0", "semantic_string": "wheel", "axle_index_int": 0, "side_string": "right", "pivot_xyz_array": [0.82, -1.25, 0.42], "dimensions_xyz_array": [0.28, 0.72, 0.72]},
            {"part_id_string": "door.left.0", "semantic_string": "door", "pivot_xyz_array": [-0.91, 0.1, 0.9], "dimensions_xyz_array": [0.08, 1.1, 0.82]},
        ],
    }


class ReceiptTests(unittest.TestCase):
    def test_axle_pair_is_forced_to_one_mesh_group(self):
        parsed = normalized_receipt(receipt())
        wheels = [p for p in parsed["parts"] if p["semantic"] == "wheel"]
        self.assertEqual(wheels[0]["mesh_group"], wheels[1]["mesh_group"])

    def test_mismatched_axle_dimensions_fail_closed(self):
        doc = receipt()
        doc["parts_array"][1]["dimensions_xyz_array"] = [0.3, 0.9, 0.9]
        with self.assertRaisesRegex(VehicleReceiptError, "dimensions disagree"):
            normalized_receipt(doc)

    def test_missing_registered_transform_fails(self):
        doc = receipt()
        del doc["parts_array"][0]["pivot_xyz_array"]
        doc["parts_array"][0]["modeled_bool"] = True
        with self.assertRaisesRegex(VehicleReceiptError, "pivot and dimensions"):
            normalized_receipt(doc)

    def test_api_aliases_do_not_silently_become_full_vehicle(self):
        doc = receipt()
        doc["mode"] = "split"
        doc["part_ids"] = ["door.left.0"]
        doc["polygon_budget"] = 900
        parsed = normalized_receipt(doc)
        self.assertEqual(parsed["assembly_mode"], "selected_parts")
        self.assertEqual(parsed["selected_part_ids"], ["door.left.0"])
        self.assertEqual(parsed["polygon_budget"], 900)

    def test_live_pipeline_nested_matrix_registers_canonical_wheels(self):
        doc = receipt()
        doc["coordinate_system_object"]["source_to_common_matrix_4x4_array"] = [
            [0, 0.008, 0, -1.6], [0.01, 0, 0, -4], [0, 0, -0.01, 3], [0, 0, 0, 1]]
        doc["body_object"] = {"bounds_min_xyz_array": [-1.6, -4, 0],
                              "bounds_max_xyz_array": [1.6, 4, 3]}
        doc["parts_array"] = [
            {"part_id_string": "wheel.axle_0.left.0", "semantic_string": "wheel", "axle_index_int": 0,
             "side_string": "left", "mesh_group_id_string": "wheel.axle_0", "source_bbox_px_array": [90, 190, 170, 270]},
            {"part_id_string": "wheel.axle_0.right.0", "semantic_string": "wheel", "axle_index_int": 0,
             "side_string": "right", "mesh_group_id_string": "wheel.axle_0", "source_bbox_px_array": [90, 190, 170, 270]},
        ]
        parsed = normalized_receipt(doc)
        wheels = parsed["parts"]
        self.assertEqual(wheels[0]["position"][0], -1.6)
        self.assertEqual(wheels[1]["position"][0], 1.6)
        self.assertEqual(wheels[0]["dimensions"], wheels[1]["dimensions"])

    def test_canonical_wheels_inherit_source_bbox_by_mask_path(self):
        doc = receipt()
        doc["body_object"] = {"bounds_min_xyz_array": [-2, -.9, 0], "bounds_max_xyz_array": [2, .9, 1.5]}
        doc["parts_array"] = [
            {"part_id_string": "wheel.0", "semantic_string": "wheel", "source_mask_path_string": "masks/w0.png",
             "source_bbox_px_array": [20, 30, 60, 70], "axle_index_int": 0},
            {"part_id_string": "wheel.axle_0.left.0", "semantic_string": "wheel", "source_mask_path_string": "masks/w0.png",
             "mesh_group_id_string": "wheel.axle_0", "side_string": "left", "axle_index_int": 0},
            {"part_id_string": "wheel.axle_0.right.0", "semantic_string": "wheel", "source_mask_path_string": "masks/w0.png",
             "mesh_group_id_string": "wheel.axle_0", "side_string": "right", "axle_index_int": 0},
        ]
        parsed = normalized_receipt(doc)
        self.assertEqual([p["id"] for p in parsed["parts"]],
                         ["wheel.axle_0.left.0", "wheel.axle_0.right.0"])

    def test_published_gltf_part_transform_converts_to_blender_frame(self):
        doc = receipt()
        doc["coordinate_system_object"] = {"units_string": "meters", "up_axis_string": "Y",
            "forward_axis_string": "+Z", "source_to_common_matrix_4x4_array":
            [[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]]}
        doc["parts_array"] = [{"part_id_string": "door.left.0", "semantic_string": "door",
            "pivot_xyz_array": [-.8, .7, 1.2], "dimensions_xyz_array": [.08, .9, 1.1]}]
        parsed = normalized_receipt(doc)
        self.assertEqual(parsed["parts"][0]["position"], [-.8, -1.2, .7])
        self.assertEqual(parsed["parts"][0]["dimensions"], [.08, 1.1, .9])


class BlenderRoundTripTests(unittest.TestCase):
    def test_glb_keeps_door_separate_and_reuses_wheel_meshes(self):
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp)
            source = root / "receipt.json"
            source.write_text(json.dumps(receipt()), encoding="utf-8")
            report = build(source, root / "out")
            self.assertTrue(report["separate_parts_bool"])
            self.assertTrue(report["collision_proxy_only_bool"])
            self.assertFalse(report["production_ready_bool"])
            self.assertIn("door.left.0", report["node_names_array"])
            glb = pathlib.Path(report["model_path_string"]).read_bytes()
            self.assertEqual(glb[:4], b"glTF")
            json_len, json_type = struct.unpack_from("<II", glb, 12)
            self.assertEqual(json_type, 0x4E4F534A)
            doc = json.loads(glb[20:20 + json_len].decode("utf-8"))
            wheel_nodes = [n for n in doc["nodes"] if n.get("name", "").startswith("wheel.axle_0")]
            wheel_mesh_nodes = [n for n in wheel_nodes if "mesh" in n]
            wheel_roots = [n for n in wheel_nodes if "mesh" not in n]
            self.assertEqual(len(wheel_roots), 2)
            self.assertEqual(len(wheel_mesh_nodes), 4)
            self.assertEqual(len({n["mesh"] for n in wheel_mesh_nodes}), 2)
            translations = {tuple(round(v, 3) for v in n.get("translation", [0, 0, 0])) for n in wheel_roots}
            self.assertEqual(translations, {(-0.82, 0.42, 1.25), (0.82, 0.42, 1.25)})
            self.assertGreaterEqual(len(doc.get("materials", [])), 3)
            self.assertEqual(report["output_coordinate_system_object"]["up_axis_string"], "Y")
            self.assertEqual(report["accepted_part_counts_object"]["wheel"], 2)
            self.assertTrue(report["part_count_match_bool"])

    def test_selected_part_mode_exports_only_stable_replacement_node(self):
        doc = receipt()
        doc["assembly_mode_string"] = "selected_parts"
        doc["selected_part_ids_array"] = ["door.left.0"]
        doc["polygon_budget_int"] = 1000
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp)
            source = root / "receipt.json"
            source.write_text(json.dumps(doc), encoding="utf-8")
            report = build(source, root / "out")
            self.assertEqual(report["replacement_node_ids_array"], ["door.left.0"])
            self.assertEqual(report["node_names_array"], ["door.left.0"])
            self.assertLessEqual(report["polygon_count_int"], 1000)

    def test_each_source_bound_pipe_exports_as_its_own_object(self):
        doc = receipt()
        doc["parts_array"] = [
            {"part_id_string": "exhaust.left.0", "semantic_string": "exhaust", "source_bound_bool": True,
             "pivot_xyz_array": [0, 0, 0], "dimensions_xyz_array": [.08, .5, .08],
             "path_xyz_array": [[-.4, -.5, .4], [-.4, 0, .45], [-.4, .5, .5]],
             "cross_section_radius_m_float": .04},
            {"part_id_string": "exhaust.right.0", "semantic_string": "exhaust", "source_bound_bool": True,
             "pivot_xyz_array": [0, 0, 0], "dimensions_xyz_array": [.08, .5, .08],
             "path_xyz_array": [[.4, -.5, .4], [.4, 0, .45], [.4, .5, .5]],
             "cross_section_radius_m_float": .04},
        ]
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp)
            source = root / "receipt.json"
            source.write_text(json.dumps(doc), encoding="utf-8")
            report = build(source, root / "out")
            self.assertEqual(report["accepted_part_counts_object"]["exhaust"], 2)
            self.assertEqual(report["exported_part_counts_object"]["exhaust"], 2)
            self.assertIn("exhaust.left.0", report["node_names_array"])
            self.assertIn("exhaust.right.0", report["node_names_array"])

    def test_actual_polygon_budget_returns_needs_review(self):
        doc = receipt()
        doc["polygon_budget_int"] = 64
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp)
            source = root / "receipt.json"
            source.write_text(json.dumps(doc), encoding="utf-8")
            report = build(source, root / "out")
            self.assertEqual(report["status_string"], "needs_review")
            self.assertGreater(report["polygon_count_int"], report["polygon_budget_int"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import hashlib
import io
import json
import pathlib
import sys
import tempfile
import unittest

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
from pydantic import ValidationError

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from ai_avatars import AvatarOwner
from vehicle_generation.api import VehicleOptions, _validate_image, build_vehicle_router
from vehicle_generation.store import RevisionConflict, VehicleStore, feature_identity
from vehicle_generation.worker import (_model_options, _mt_options, _preview_geometry_path, _record_model_artifact,
                                       _multiview_source, _prepare_multiview_workspace, _validate_manifest,
                                       _validated_wheel_doc)

try:
    import multipart  # noqa: F401
    HAVE_MULTIPART = True
except ImportError:
    HAVE_MULTIPART = False


class VehicleStoreTests(unittest.TestCase):
    def test_multiview_uses_hash_bound_prepared_source(self):
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp); data = b"prepared"; digest = hashlib.sha256(data).hexdigest()
            (root / "prepared.png").write_bytes(data)
            (root / "manifest.json").write_text(json.dumps({"prepared_image_object": {
                "path_string": "prepared.png", "sha256_string": digest}}), encoding="utf-8")
            document = {"source_sha_chain": {"prepared_sha256": digest}}
            self.assertEqual(_multiview_source(root, document), (root / "prepared.png").resolve())
            document["source_sha_chain"]["prepared_sha256"] = "0" * 64
            with self.assertRaisesRegex(Exception, "hash chain"):_multiview_source(root, document)

    def test_multiview_workspace_is_bound_to_original_and_prepared(self):
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp); source = root / "prepared.png"; source.write_bytes(b"prepared")
            prepared = hashlib.sha256(source.read_bytes()).hexdigest(); document = {"id": "veh_test",
                "source_sha_chain": {"original_sha256": "a" * 64, "prepared_sha256": prepared},
                "options": {"resolution": 192}, "analysis": {"estimated_dimensions_m_object": {
                    "length_float": 4, "width_float": 2, "height_float": 1.5}}}
            workspace = _prepare_multiview_workspace(root, document, source)
            manifest = json.loads((workspace / "manifest.json").read_text())
            self.assertEqual(manifest["original_source_sha256_string"], "a" * 64)
            self.assertEqual(manifest["prepared_source_sha256_string"], prepared)
            self.assertEqual(manifest["body_cleanup_contract_string"], "body-cleanup-v1")

    def test_strict_multiview_stage_receipts(self):
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp); artifacts = []
            def artifact(name, data):
                (root / name).write_bytes(data); digest = hashlib.sha256(data).hexdigest()
                artifacts.append({"path_string": name, "sha256_string": digest}); return digest
            removal_sha = artifact("wheel-free.png", b"wheel-free")
            primary_camera_sha = artifact("primary-camera.json", b"primary-camera")
            views = []
            for view_id in ("top", "front", "left", "back"):
                image_sha = artifact(f"{view_id}.png", (view_id + "-image").encode())
                depth_sha = artifact(f"{view_id}-depth.png", (view_id + "-depth").encode())
                camera_sha = artifact(f"{view_id}-camera.json", (view_id + "-camera").encode())
                views.append({"id_string": view_id,
                    "reference_object": {"path_string": f"{view_id}.png", "sha256_string": image_sha,
                                         "wheel_free_bool": True, "source_sha256_string": removal_sha},
                    "camera_object": {"projection_string": "orthographic", "coordinate_space_string": "model_local_gltf",
                                      "path_string": f"{view_id}-camera.json", "sha256_string": camera_sha},
                    "depth_object": {"path_string": f"{view_id}-depth.png", "sha256_string": depth_sha,
                                     "source_sha256_string": image_sha}})
            fused = {}
            for kind in ("npz", "json", "glb"):
                fused[f"{kind}_path_string"] = f"fused.{kind}"
                fused[f"{kind}_sha256_string"] = artifact(f"fused.{kind}", ("fused-" + kind).encode())
            for view in views:
                view["depth_object"].update(convention_string="DepthAnythingV2-relative-near-white",
                                             near_is_white_bool=True, metric_bool=False)
            fused.update(resolution_int=256, voxel_count_int=149999, coordinate_space_string="model_local_gltf",
                         symmetry_object={"pair_symmetry_error_m_float": 0}, quality_object={
                             "fixed_resolution_bool": True, "requested_resolution_int": 256,
                             "centered_x_lattice_bool": True, "provenance_mirrored_from_working_side_bool": True,
                             "leave_one_view_out_object": {"accepted_bool": True, "per_view_residuals_object": {}}})
            manifest = {"schema_string": "autorig.vehicle.multiview/1", "status_string": "complete",
                "source_sha256_string": "b" * 64, "original_source_sha256_string": "a" * 64,
                "prepared_source_sha256_string": "b" * 64, "prepared_image_object": {"sha256_string": "b" * 64},
                "wheel_removal_object": {"status_string": "complete", "path_string": "wheel-free.png",
                                         "output_sha256_string": removal_sha, "wheel_free_bool": True},
                "body_cleanup_object": {"schema_string": "autorig.vehicle.body-cleanup/1",
                    "contract_version_string": "body-cleanup-v1", "status_string": "complete",
                    "source_sha256_string": "b" * 64, "path_string": "wheel-free.png",
                    "output_sha256_string": removal_sha, "wheels_removed_bool": True,
                    "equipment_removed_bool": True, "cargo_removed_bool": True,
                    "semantic_counts_object": {"wheels_int": 0, "equipment_int": 0, "cargo_int": 0},
                    "excluded_part_ids_array": ["wheel.0", "gun.0", "cargo.0"]},
                "primary_reference_object": {"role_string": "display_only", "fusion_member_bool": False,
                    "reference_object": {"path_string": "wheel-free.png", "sha256_string": removal_sha},
                    "camera_object": {"path_string": "primary-camera.json", "sha256_string": primary_camera_sha,
                        "schema_string": "autorig.vehicle.primary-camera/1", "coordinate_space_string": "model_local_gltf",
                        "projection_string": "orthographic"}},
                "views_array": views, "fused_voxels_object": fused, "artifacts_array": artifacts,
                "working_side_string": "image_right", "canonical_working_side_string": "positive_x",
                "dimensions_m_object": {
                    "length_float": 4.0, "width_float": 2.0, "height_float": 1.5},
                "source_bindings_object": {"body_cleanup_source_sha256_string": removal_sha},
                "default_view_string": "front", "shared_model_transform_4x4_array": [1, 0, 0, 0, 0, 1, 0, 0,
                                                                                       0, 0, 1, 0, 0, 0, 0, 1]}
            manifest["stage_string"] = "remove_wheels"; manifest["status_string"] = "needs_review"
            manifest["body_cleanup_object"]["status_string"] = "needs_review"
            manifest["body_cleanup_object"]["semantic_counts_object"]["wheels_int"] = 2
            manifest["provider_tasks_array"] = [{"schema_string": "autorig.vehicle.provider-task/1",
                "pipeline_stage_string": "remove_wheels", "service_string": "farm", "task_id_string": "remove-task",
                "status_string": "completed", "node_string": "f7", "worker_string": "vehicle-worker",
                "input_sha256_string": "c" * 64, "output_sha256_string": "d" * 64}]
            self.assertEqual(_validate_manifest(root, manifest, "a" * 64, "remove_wheels", 256)["status_string"],
                             "needs_review")
            manifest["status_string"] = "complete"; manifest["body_cleanup_object"]["status_string"] = "complete"
            manifest["body_cleanup_object"]["semantic_counts_object"]["wheels_int"] = 0
            for stage in ("remove_wheels", "project_views", "depth_views", "fuse_voxels"):
                manifest["stage_string"] = stage
                manifest["provider_tasks_array"] = [{"schema_string": "autorig.vehicle.provider-task/1",
                    "pipeline_stage_string": stage, "service_string": "farm", "task_id_string": stage + "-task",
                    "status_string": "completed", "node_string": "f7", "worker_string": "vehicle-worker",
                    "input_sha256_string": "c" * 64, "output_sha256_string": "d" * 64}]
                self.assertEqual(_validate_manifest(root, manifest, "a" * 64, stage, 256)["stage_string"], stage)

    def test_image_dimensions_are_rejected_before_full_decode(self):
        output = io.BytesIO(); Image.new("RGB", (9000, 10), (1, 2, 3)).save(output, format="PNG")
        with self.assertRaises(HTTPException) as caught: _validate_image(output.getvalue(), "wide.png")
        self.assertEqual(caught.exception.detail["error_string"], "invalid_source")

    def test_owner_reference_camera_authority_preserves_measured_estimate(self):
        original = "a" * 64; prepared_bytes = b"accepted prepared reference"; prepared = hashlib.sha256(prepared_bytes).hexdigest()
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp); (root / "prepared_ortho45.png").write_bytes(prepared_bytes)
            manifest = {"stage_string": "prepare", "status_string": "complete", "source_sha256_string": prepared,
                "original_source_sha256_string": original, "prepared_source_sha256_string": prepared,
                "prepared_image_object": {"path_string": "prepared_ortho45.png", "sha256_string": prepared},
                "camera_object": {"normalization_status_string": "accepted", "projection_string": "orthographic",
                    "yaw_deg_float": 45.0, "pitch_deg_float": 45.0, "authority_string": "owner_reference",
                    "measured_camera_object": {"projection_string": "unverified", "yaw_deg_float": 42.0,
                                               "pitch_deg_float": 18.0}},
                "artifacts_array": [{"path_string": "prepared_ortho45.png", "sha256_string": prepared}]}
            checked = _validate_manifest(root, manifest, original, "prepare")
        self.assertEqual(checked["camera_object"]["authority_string"], "owner_reference")
        self.assertEqual(checked["camera_object"]["measured_camera_object"]["yaw_deg_float"], 42.0)

    def test_geometry_option_boundaries_match_mt(self):
        self.assertEqual(VehicleOptions(resolution=256, length_m=30).model_dump()["resolution"], 256)
        self.assertEqual(VehicleOptions().resolution, 192)
        with self.assertRaises(ValidationError): VehicleOptions(resolution=200)
        with self.assertRaises(ValidationError): VehicleOptions(length_m=30.01)

    def test_only_qa_ready_canonical_wheels_enable_drive(self):
        rows = [{"part_id_string": f"wheel.{i}", "axle_index_int": i // 2,
                 "side_string": "left" if i % 2 == 0 else "right", "center_xyz_array": [(-1) ** i, .4, i // 2],
                 "radius_m_float": .4, "width_m_float": .25, "axis_xyz_array": [1, 0, 0],
                 "steer_bool": i < 2, "mesh_group_id_string": f"axle.{i // 2}"} for i in range(4)]
        manifest = {"source_sha256_string": "a" * 64, "prepared_source_sha256_string": "a" * 64,
            "validated_wheel_rig_object": {"status_string": "ready", "source_sha256_string": "a" * 64,
                "coordinate_space_string": "gltf_right_handed_y_up_forward_plus_z", "wheels_array": rows,
                "qa_object": {"ready_bool": True, "supported_drive_profile_bool": True,
                    "expected_wheel_count_int": 4, "expected_axle_count_int": 2,
                    "expected_axle_wheel_counts_array": [2, 2], "steering_axle_indices_array": [0]}},
            "wheel_free_body_object": {"glb_path_string": "body_voxels.glb", "excludes_wheels_bool": True},
            "artifacts_array": []}
        doc = _validated_wheel_doc(manifest)
        self.assertEqual(len(doc["wheels"]), 4)
        manifest["validated_wheel_rig_object"]["wheels_array"] = rows[:3]
        manifest["validated_wheel_rig_object"]["qa_object"].update(
            expected_wheel_count_int=3, expected_axle_wheel_counts_array=[2, 1])
        self.assertEqual(len(_validated_wheel_doc(manifest)["wheels"]), 3)
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp); body = b"wheel-free-body"; (root / "body_voxels.glb").write_bytes(body)
            (root / "voxels.glb").write_bytes(b"fused-analysis")
            manifest["artifacts_array"] = [{"path_string": "body_voxels.glb",
                                             "sha256_string": hashlib.sha256(body).hexdigest()}]
            self.assertEqual(_preview_geometry_path(root, manifest).name, "body_voxels.glb")
        manifest["validated_wheel_rig_object"]["qa_object"]["ready_bool"] = False
        self.assertIsNone(_validated_wheel_doc(manifest))
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp); (root / "voxels.glb").write_bytes(b"analysis")
            self.assertEqual(_preview_geometry_path(root, manifest).name, "voxels.glb")

    def test_geometry_feature_identity_preserves_distinct_runs_and_options(self):
        base = feature_identity("veh_a", "a" * 64, "b" * 64, "c" * 64, "d" * 64)
        self.assertNotEqual(base, feature_identity("veh_b", "a" * 64, "b" * 64, "c" * 64, "d" * 64))
        self.assertNotEqual(base, feature_identity("veh_a", "a" * 64, "b" * 64, "e" * 64, "d" * 64))

    def test_selected_part_model_versions_preserve_other_parts(self):
        document = {"revision": 4, "artifacts": {}}
        first = {"version_relative_path_string": "models/r4-a", "report_object": {"replacement_node_ids_array": ["pipe.0"]}}
        _record_model_artifact(document, {"mode": "split", "part_ids": ["pipe.0"], "polygon_budget": 4000,
                                          "provider": "blender"}, first, "https://autorig.online/cap")
        document["revision"] = 5
        second = {"version_relative_path_string": "models/r5-b", "report_object": {"replacement_node_ids_array": ["wheel.0"]}}
        _record_model_artifact(document, {"mode": "split", "part_ids": ["wheel.0"], "polygon_budget": 8000,
                                          "provider": "blender"}, second, "https://autorig.online/cap")
        parts = document["artifacts"]["model_parts"]
        self.assertEqual(set(parts), {"pipe.0", "wheel.0"})
        self.assertIn("models/r4-a", parts["pipe.0"]["model_url"])
        self.assertIn("models/r5-b", parts["wheel.0"]["model_url"])
        self.assertTrue(document["rig_invalidated_bool"])

    def test_api_options_are_normalized_to_mt_wire_contract(self):
        analysis = {"working_side_string": "image_right", "canonical_working_side_string": "positive_x",
                    "estimated_dimensions_m_object": {
            "length_float": 30, "width_float": 3, "height_float": 4}}
        self.assertEqual(_mt_options({"working_side": "left", "resolution": 256, "length_m": 30.0,
                                      "base_url_string": "https://attacker.example", "source_path": "/etc/passwd"},
                                     "veh_test", analysis, True),
                         {"run_id_string": "veh_test", "voxel_resolution_int": 256, "max_voxel_points_int": 150000,
                          "base_url_string": "https://autorig.online", "resolution_int": 256,
                          "working_side_string": "image_left", "canonical_working_side_string": "negative_x",
                          "target_length_m_float": 30.0,
                          "dimensions_m_object": {"length_float": 30.0, "width_float": 3.0, "height_float": 4.0}})
        auto = _mt_options({"working_side": "auto", "resolution": 192, "length_m": None}, "veh_auto", analysis, True)
        self.assertEqual(auto["working_side_string"], "image_right")
        self.assertEqual(auto["canonical_working_side_string"], "positive_x")
        self.assertEqual(_model_options({"mode": "split", "part_ids": ["door.left"], "polygon_budget": 24000,
                                         "options": {"registration_tolerance_m_float": .01}}),
                         {"registration_tolerance_m_float": .01, "mode_string": "selected_parts",
                          "selected_part_ids_array": ["door.left"], "polygon_budget_int": 24000})
        self.assertEqual(_model_options({"mode": "body", "part_ids": [], "polygon_budget": 10000, "options": {}})["mode_string"],
                         "body_only")

    def test_actual_analyze_receipt_is_accepted_without_voxel_space(self):
        fixtures = pathlib.Path(__file__).parent / "fixtures"
        manifest = json.loads((fixtures / "vehicle_analyze_manifest.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            root = pathlib.Path(tmp)
            inventory = (fixtures / "vehicle_analyze_inventory.json").read_text(encoding="utf-8").replace("\r\n", "\n")
            (root / "inventory.json").write_bytes(inventory.rstrip("\n").encode("utf-8"))
            validated = _validate_manifest(root, manifest, manifest["source_sha256_string"], "analyze")
        self.assertEqual(validated["status_string"], "complete")

    def test_owner_binding_lease_and_restart_reclaim(self):
        with tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent) as tmp:
            store = VehicleStore(pathlib.Path(tmp)); owner = AvatarOwner(owner_type="user", owner_id="alice")
            run_id = store.run_id(); source = store.run_dir(run_id) / "source.png"; source.write_bytes(png_bytes())
            value = {"id": run_id, "status": "queued", "stage": "analyze", "progress": 0,
                     "source_url": f"https://autorig.online/api/vehicle/assets/{run_id}/secret/source.png",
                     "analysis": None, "parts": [], "artifacts": {}, "viewer_url": "/nodes", "stages": [],
                     "error": None, "options": {"working_side": "auto", "resolution": 64, "length_m": None},
                     "revision": 0, "source_sha256": "a" * 64, "source": {"path": str(source)},
                     "pipeline_version": "test", "_capability": "secret"}
            created = store.create(owner, value, "a" * 64)
            self.assertEqual(created["stages"][0]["status"], "queued")
            with self.assertRaises(KeyError):
                store.get(run_id, AvatarOwner(owner_type="user", owner_id="bob"))
            first = store.claim(lease_seconds=0); second = store.claim(lease_seconds=30)
            self.assertEqual(first["id"], second["id"])
            with self.assertRaises(RevisionConflict):
                store.fail(first, "stale worker must not clobber the reclaimed run")
            store.progress(second, "vision", 23, {"detected": "vehicle"})
            current = store.get(run_id, owner)
            self.assertEqual(current["progress"], 23)
            self.assertEqual(current["status"], "running")
            self.assertEqual(current["stage"], "analyze")
            current.update(status="completed", stage="analyze", progress=100, revision=current["revision"] + 1)
            analyze = {"analysis_object": {"category_string": "combat car", "title_string": "War Rig",
                       "estimated_dimensions_m_object": {"length_float": 4.5}, "materials_array": [{"name": "steel"}],
                       "objects_array": [{"semantic": "body"}]}}
            store.finish(second, current, analyze)
            with self.assertRaisesRegex(ValueError, "needs_camera_preparation"):
                store.enqueue(run_id, owner, {"action": "voxelize", "options": {}}, current["revision"])
            store.enqueue(run_id, owner, {"action": "prepare", "options": {}}, current["revision"])
            prepare_job = store.claim(); prepared = store.get(run_id, owner)
            prepared["prepared_image_url"] = "https://autorig.online/prepared.png"
            prepared["camera"] = {"normalization_status_string": "needs_review", "projection_string": "unverified",
                                  "yaw_deg_float": 30.0, "pitch_deg_float": 15.0}
            prepared["source_sha_chain"] = {"original_sha256": "a" * 64, "prepared_sha256": "b" * 64,
                                                "pipeline_version": "test"}
            prepared.update(status="needs_review", stage="prepare", progress=100, revision=prepared["revision"] + 1)
            store.finish(prepare_job, prepared, {"status_string": "completed", "prepared_image_object": {
                "path_string": "prepared_45.png", "sha256_string": "b" * 64}})
            review = store.get(run_id, owner)
            retried = store.enqueue(run_id, owner, {"action": "retry", "options": {}}, review["revision"])
            self.assertEqual(retried["camera_attempt_int"], 1)
            self.assertTrue(retried["prepare_retry_available_bool"])
            with self.assertRaises(RevisionConflict):
                store.enqueue(run_id, owner, {"action": "retry", "options": {}}, review["revision"])
            retry_job = store.claim(); prepared = store.get(run_id, owner)
            prepared.update(status="needs_review", stage="prepare", progress=100, revision=prepared["revision"] + 1)
            store.finish(retry_job, prepared, {"status_string": "completed", "prepared_image_object": {
                "path_string": "prepared_45.png", "sha256_string": "b" * 64}})
            prepared = store.get(run_id, owner)
            ref_evidence = {"original_sha256": "a" * 64, "prepared_sha256": "b" * 64}
            with self.assertRaises(KeyError):
                store.accept_reference(run_id, AvatarOwner(owner_type="user", owner_id="bob"),
                                       {"reference_evidence": ref_evidence}, prepared["revision"])
            with self.assertRaisesRegex(ValueError, "stale or forged"):
                store.accept_reference(run_id, owner, {"reference_evidence": dict(ref_evidence, prepared_sha256="d" * 64)},
                                       prepared["revision"])
            prepared = store.accept_reference(run_id, owner, {"reference_evidence": ref_evidence}, prepared["revision"])
            self.assertTrue(prepared["reference_accepted_bool"])
            with self.assertRaisesRegex(ValueError, "fixed when the run is created"):
                store.enqueue(run_id, owner, {"action": "remove_wheels", "options": {"resolution": 128}}, prepared["revision"])
            stage_doc = prepared
            for action in ("remove_wheels", "project_views", "depth_views"):
                store.enqueue(run_id, owner, {"action": action, "options": {}}, stage_doc["revision"])
                stage_job = store.claim(); stage_doc = store.get(run_id, owner)
                stage_doc.update(status="completed", stage=action, progress=100, revision=stage_doc["revision"] + 1)
                store.finish(stage_job, stage_doc, {"status_string": "completed"})
            store.enqueue(run_id, owner, {"action": "fuse_voxels", "options": {}}, stage_doc["revision"])
            fusion_job = store.claim(); clustered = store.get(run_id, owner)
            clustered.update(status="completed", stage="fuse_voxels", progress=100, revision=clustered["revision"] + 1)
            store.finish(fusion_job, clustered, {"parts_array": [{"part_id_string": "door.left"}],
                "geometry_options_sha256_string": "e" * 64,
                "fused_voxels_object": {"glb_path_string": "fused.glb"},
                "artifacts_array": [{"path_string": "fused.glb", "sha256_string": "f" * 64}]})
            with store.connect() as db:
                feature = db.execute("SELECT * FROM vehicle_features WHERE source_sha256=?", ("a" * 64,)).fetchone()
                receipts = db.execute("SELECT COUNT(*) FROM vehicle_stage_receipts WHERE source_sha256=?", ("a" * 64,)).fetchone()[0]
                candidates = db.execute("SELECT COUNT(*) FROM vehicle_prepare_candidates WHERE run_id=?", (run_id,)).fetchone()[0]
                versions = db.execute("SELECT COUNT(*) FROM vehicle_feature_versions WHERE run_id=?", (run_id,)).fetchone()[0]
            self.assertEqual(feature["category"], "combat car")
            self.assertEqual(json.loads(feature["dimensions_json"])["length_float"], 4.5)
            self.assertEqual(json.loads(feature["materials_json"])[0]["name"], "steel")
            self.assertEqual(json.loads(feature["objects_json"])[0]["part_id_string"], "door.left")
            self.assertEqual(receipts, 6)
            self.assertEqual(candidates, 2)
            self.assertEqual(versions, 1)
            after_voxel = store.get(run_id, owner)
            store.enqueue(run_id, owner, {"action": "cluster", "options": {}}, after_voxel["revision"])
            cluster_stage = store.claim(); cluster_doc = store.get(run_id, owner)
            cluster_doc["parts"] = [{"part_id_string": "pipe.0", "source_bbox_px_array": [1, 2, 30, 40],
                                     "mask_url": "https://autorig.online/masks/pipe.png", "mask_sha256": "c" * 64}]
            cluster_doc.update(status="needs_review", stage="cluster", progress=100, revision=cluster_doc["revision"] + 1)
            store.finish(cluster_stage, cluster_doc, {"status_string": "needs_review", "parts_array": [{"part_id_string": "pipe.0"}]})
            after_cluster = store.get(run_id, owner)
            evidence = {"part_id": "pipe.0", "original_sha256": "a" * 64,
                        "prepared_sha256": "b" * 64, "mask_sha256": "c" * 64}
            with self.assertRaises(KeyError):
                store.accept_parts(run_id, AvatarOwner(owner_type="user", owner_id="bob"),
                                   {"part_ids": ["pipe.0"], "part_evidence": [evidence]}, after_cluster["revision"])
            forged = dict(evidence, mask_sha256="d" * 64)
            with self.assertRaisesRegex(ValueError, "stale or forged"):
                store.accept_parts(run_id, owner, {"part_ids": ["pipe.0"], "part_evidence": [forged]}, after_cluster["revision"])
            accepted = store.accept_parts(run_id, owner, {"part_ids": ["pipe.0"], "part_evidence": [evidence]}, after_cluster["revision"])
            with self.assertRaises(RevisionConflict):
                store.accept_parts(run_id, owner, {"part_ids": ["pipe.0"], "part_evidence": [evidence]}, after_cluster["revision"])
            self.assertTrue(accepted["parts"][0]["accepted_bool"])
            store.enqueue(run_id, owner, {"action": "model", "mode": "split", "part_ids": ["pipe.0"], "options": {}}, accepted["revision"])
            model_stage = store.claim(); model_doc = store.get(run_id, owner)
            model_doc.update(status="completed", stage="model", progress=100, revision=model_doc["revision"] + 1)
            store.finish(model_stage, model_doc, {"status_string": "completed"})
            repeated = store.enqueue(run_id, owner, {"action": "model", "mode": "split", "part_ids": ["pipe.0"], "options": {}}, model_doc["revision"])
            self.assertEqual(repeated["stage"], "model")


def png_bytes() -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (64, 48), (120, 30, 20)).save(output, format="PNG")
    return output.getvalue()


@unittest.skipUnless(HAVE_MULTIPART, "python-multipart is installed on the production VPS")
class VehiclePipelineApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=pathlib.Path(__file__).parent)
        self.store = VehicleStore(pathlib.Path(self.temp.name))

        async def owner(x_owner: str = Header(...)):
            return AvatarOwner(owner_type="user", owner_id=x_owner)

        app = FastAPI()
        app.include_router(build_vehicle_router(owner, store=self.store))
        self.client = TestClient(app)

    def tearDown(self):
        self.temp.cleanup()

    def create(self, owner="alice"):
        response = self.client.post("/api/vehicle/runs", headers={"x-owner": owner},
                                    files={"image": ("car.png", png_bytes(), "image/png")})
        self.assertEqual(response.status_code, 202, response.text)
        return response.json()

    def test_upload_contract_and_owner_isolation(self):
        run = self.create()
        self.assertEqual(set(run), {"id", "status", "stage", "progress", "source_url", "analysis", "parts",
                                    "artifacts", "viewer_url", "stages", "error", "options", "revision",
                                    "original_url", "prepared_image_url", "source_sha_chain", "camera",
                                    "supported_model_providers", "camera_attempt_int", "prepare_retry_available_bool",
                                    "reference_accepted_bool", "reference_review_id", "reference_review_revision",
                                    "drive_blocker_string", "wheel_removal_object", "multiview_object",
                                    "body_cleanup_object", "views_object", "fusion_object", "remove_attempt_int",
                                    "body_cleanup_retry_available_bool", "view_attempts_object",
                                    "project_views_retry_available_bool", "wheel_rig", "wheel_rig_revision"})
        self.assertEqual(run["original_url"], run["source_url"])
        self.assertIsNone(run["prepared_image_url"])
        self.assertEqual(run["source_sha_chain"]["original_sha256"], hashlib.sha256(png_bytes()).hexdigest())
        self.assertIsNone(run["camera"])
        self.assertEqual(run["supported_model_providers"], ["blender"])
        self.assertEqual(self.client.get(f"/api/vehicle/runs/{run['id']}", headers={"x-owner": "bob"}).status_code, 404)
        source_path = self.store.run_dir(run["id"]) / "source.png"
        self.assertEqual(source_path.read_bytes(), png_bytes())

    def test_rejects_non_image_and_paid_provider(self):
        response = self.client.post("/api/vehicle/runs", headers={"x-owner": "alice"},
                                    files={"image": ("car.png", b"not an image", "image/png")})
        self.assertEqual(response.status_code, 400)
        run = self.create()
        # Initial analyze stage is active, so clear it as a test fixture.
        with self.store.connect() as db:
            db.execute("UPDATE vehicle_runs SET active_job_id=NULL WHERE id=?", (run["id"],))
        response = self.client.post(f"/api/vehicle/runs/{run['id']}/actions", headers={"x-owner": "alice"},
                                    json={"action": "model", "provider": "tripo", "options": {}, "expected_revision": 0})
        self.assertEqual(response.status_code, 409)
        self.assertIn("Tripo", response.text)
        response = self.client.post(f"/api/vehicle/runs/{run['id']}/actions", headers={"x-owner": "alice"},
                                    json={"action": "prepare", "options": {"base_url_string": "https://attacker.example"}})
        self.assertEqual(response.status_code, 422)

    def test_capability_is_unguessable_and_has_no_listing(self):
        run = self.create()
        path = run["source_url"].split("autorig.online", 1)[1]
        self.assertEqual(self.client.get(path).status_code, 200)
        pieces = path.split("/")
        pieces[-2] = "wrong-token"
        self.assertEqual(self.client.get("/".join(pieces)).status_code, 404)
        self.assertEqual(self.client.get(f"/api/vehicle/assets/{run['id']}").status_code, 404)

    def test_single_active_stage_and_revision_cas(self):
        run = self.create(); owner = AvatarOwner(owner_type="user", owner_id="alice")
        with self.assertRaises(RevisionConflict):
            self.store.enqueue(run["id"], owner, {"action": "cluster", "options": {}}, 0)
        job = self.store.claim(lease_seconds=0)
        self.assertIsNotNone(job)
        # An expired lease is reclaimed by the same single-consumer protocol.
        reclaimed = self.store.claim(lease_seconds=30)
        self.assertEqual(reclaimed["id"], job["id"])


if __name__ == "__main__":
    unittest.main()

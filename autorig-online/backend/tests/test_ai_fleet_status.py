import unittest

import ai_fleet


class FleetStatusTests(unittest.TestCase):
    def test_new_image_workflows_are_images_not_video(self):
        for workflow in ("gen_image_flux2_klein.json", "gen_image_sdxl.json",
                         "gen_image_sdxl_control_pose.json"):
            self.assertEqual(ai_fleet._service_of_workflow(workflow), "image")

    def test_control_extractors_have_their_own_activity(self):
        self.assertEqual(ai_fleet._service_of_workflow("gen_control_depth.json"),
                         "control")

    def test_pending_is_queued_and_rendering_is_active(self):
        counts = ai_fleet._running_from_renderfin([
            {"id": "p", "status": "Pending", "workflow": "gen_image.json"},
            {"id": "r", "status": "Rendering", "workflow": "gen_image.json",
             "render_server_name": "f12"},
        ])
        self.assertEqual(counts["image"], {"active": 1, "queued": 1})

    def test_converter_3d_load_is_not_reported_as_vision_and_text(self):
        running = {"3dmodel": 1}
        self.assertNotIn("vision", running)
        self.assertNotIn("text", running)

    def test_task_is_found_by_server_when_registry_held_id_is_stale(self):
        task = {"id": "task-1", "status": "Rendering",
                "workflow": "gen_image_sdxl.json", "render_server_name": "worker-4090"}
        node = ai_fleet.render_node(
            {"render_server_name": "worker-4090", "status": "online"},
            {}, {"worker-4090": task})
        self.assertTrue(node["busy"])
        self.assertEqual(node["task_id"], "task-1")
        self.assertEqual(node["activity"], "image")

    def test_physical_duplicates_are_merged(self):
        nodes = ai_fleet._merge_physical_nodes([
            {"id": "f12", "kind": "ai", "online": True, "busy": False,
             "activity": "", "activities": [], "queue_depth": 0},
            {"id": "f12", "kind": "render", "online": True, "busy": True,
             "activity": "image", "activities": ["image"], "queue_depth": 1,
             "task_id": "x", "task_status": "Rendering", "workflow": "gen_image.json",
             "assigned_worker": "f12"},
            {"id": "ryzen-server", "kind": "ai", "online": True, "busy": False,
             "activity": "", "activities": [], "queue_depth": 0},
            {"id": "Raptor", "kind": "render", "online": True, "busy": False,
             "activity": "", "activities": [], "queue_depth": 0},
        ])
        self.assertEqual([n["id"] for n in nodes], ["f12", "ryzen-server"])
        self.assertTrue(nodes[0]["busy"])
        self.assertEqual(nodes[0]["task_id"], "x")

    def test_queue_eta_uses_only_compatible_workers(self):
        tasks = [
            {"id": "done", "status": "Done", "workflow": "image-a.json",
             "started_at": 100, "finished_at": 200},
            {"id": "active", "status": "Rendering", "workflow": "image-a.json",
             "render_server_name": "a", "started_at": 160},
            {"id": "q1", "status": "Pending", "workflow": "image-a.json", "created_at": 1},
            {"id": "q2", "status": "Pending", "workflow": "image-a.json", "created_at": 2},
            {"id": "old", "status": "Pending", "workflow": "does_not_exist.json", "created_at": 0},
        ]
        servers = [
            {"render_server_name": "a", "status": "online",
             "available_workflows": ["image-a.json"]},
            {"render_server_name": "b", "status": "online",
             "available_workflows": ["image-a.json"]},
            {"render_server_name": "c", "status": "online",
             "available_workflows": ["other.json"]},
        ]
        summary = ai_fleet._queue_summary(tasks, servers, {}, now=200)
        self.assertEqual(summary["running_int"], 1)
        self.assertEqual(summary["queued_int"], 2)
        self.assertEqual(summary["blocked_int"], 1)
        self.assertEqual(summary["eta_seconds_float"], 160.0)
        self.assertEqual(summary["estimate_kind_string"], "measured")
        self.assertEqual(summary["sample_count_int"], 1)

    def test_only_blocked_work_has_unknown_eta(self):
        summary = ai_fleet._queue_summary(
            [{"status": "Pending", "workflow": "retired.json"}],
            [{"render_server_name": "a", "status": "online",
              "available_workflows": ["current.json"]}], {}, now=100)
        self.assertEqual(summary["queued_int"], 0)
        self.assertEqual(summary["blocked_int"], 1)
        self.assertIsNone(summary["eta_seconds_float"])
        self.assertEqual(summary["estimate_kind_string"], "unknown")

    def test_fallback_eta_is_labelled_and_coarsely_rounded(self):
        summary = ai_fleet._queue_summary(
            [{"status": "Pending", "workflow": "gen_image_new.json"}],
            [{"render_server_name": "a", "status": "online",
              "available_workflows": ["gen_image_new.json"]}], {}, now=100)
        self.assertEqual(summary["eta_seconds_float"], 100.0)
        self.assertEqual(summary["estimate_kind_string"], "fallback")
        self.assertEqual(summary["sample_count_int"], 0)


if __name__ == "__main__":
    unittest.main()

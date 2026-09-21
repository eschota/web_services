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


if __name__ == "__main__":
    unittest.main()

"""Viewer backdrop transport contract tests."""
import unittest

from viewer_environment import build_viewer_environment_from_settings
from worker_payloads import build_worker_task_payload


class ViewerEnvironmentPayloadTests(unittest.TestCase):
    def test_ancient_ruins_resolves_to_absolute_viewer_backdrop_url(self):
        env = build_viewer_environment_from_settings(
            {"viewer_theme_selection": {"theme_id": "ancient_ruins"}},
            app_url="https://autorig.online",
        )

        self.assertIsNotNone(env)
        self.assertEqual(env["theme_id"], "ancient_ruins")
        self.assertEqual(env["background_fit"], "cover")
        self.assertEqual(
            env["background"]["url"],
            "https://autorig.online/static/env/backdrops/viewer/ancient_ruins.jpg?v=20260516-16x9",
        )

    def test_worker_payload_uses_viewer_environment_not_inpaint_background_url(self):
        env = build_viewer_environment_from_settings(
            {"viewer_theme_selection": {"theme_id": "ancient_ruins"}},
            app_url="https://autorig.online",
        )
        payload = build_worker_task_payload(
            "https://autorig.online/u/example/model.fbx",
            "t_pose",
            pipeline_kind="rig",
            viewer_environment=env,
        )

        self.assertIn("viewer_environment", payload)
        self.assertNotIn("background_url", payload)
        self.assertEqual(
            payload["viewer_environment"]["background"]["url"],
            "https://autorig.online/static/env/backdrops/viewer/ancient_ruins.jpg?v=20260516-16x9",
        )

    def test_convert_payload_omits_viewer_environment(self):
        payload = build_worker_task_payload(
            "https://autorig.online/u/example/model.glb",
            "t_pose",
            pipeline_kind="convert",
            viewer_environment={"theme_id": "ancient_ruins"},
        )

        self.assertNotIn("viewer_environment", payload)
        self.assertNotIn("background_url", payload)


if __name__ == "__main__":
    unittest.main()

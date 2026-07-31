import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from renderfin import config


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            patch.object(config, "DATA_DIR", root),
            patch.object(config, "RENDER_DIR", root / "render"),
            patch.object(config, "DB_DIR", root / "db"),
            patch.object(config, "TMP_DIR", root / "tmp"),
            patch.object(config, "SERVERS_DIR", root / "servers"),
            patch.object(config, "DB_PATH", root / "db" / "renderfin.db"),
        ]
        for p in self.patches:
            p.start()
        from renderfin.app import app  # noqa: WPS433 (import after patching)

        self.client = TestClient(app)
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def test_health(self):
        resp = self.client.get("/renderfin/health")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["ok"])

    def test_render_prompt_returns_immediate_output_url(self):
        resp = self.client.post(
            "/renderfin/api-render",
            json={
                "prompt": "armored knight",
                "type": "t_pose",
                "image_url": "https://www.autorig.online/renderfin/render/masks/t_pose.jpg",
                "user_name": "smoke",
            },
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("/render/smoke/", data["output_url"])
        self.assertTrue(data["output_url"].endswith(".png"))

        status = self.client.get(
            "/renderfin/api-render-get-task-by-url", params={"url": data["output_url"]}
        )
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()[0]["status"], "Pending")

    def test_image_to_3d_output_ext(self):
        resp = self.client.post(
            "/renderfin/api-render",
            json={"type": "image_to_3d", "image_url": "https://h/iso.png", "user_name": "bot"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["output_url"].endswith(".glb"))

    def test_empty_body_rejected(self):
        resp = self.client.post("/renderfin/api-render", json={})
        self.assertEqual(resp.status_code, 400)

    def test_server_registration(self):
        resp = self.client.post(
            "/renderfin/api-render",
            json={
                "render_server_name": "raptor",
                "render_server_url": "http://5.129.157.224:8288",
                "gpu_name": "3080ti",
                "status": "online",
                "available_workflows": ["gen_image.json"],
                "render_operation": "add_server",
            },
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["ok"])

        dashboard = self.client.get("/renderfin/api-render")
        servers = dashboard.json()["servers"]
        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0]["render_server_name"], "raptor")

        resp = self.client.post(
            "/renderfin/api-render",
            json={"render_server_name": "raptor", "render_operation": "delete_server"},
        )
        self.assertTrue(resp.json()["ok"])
        self.assertEqual(len(self.client.get("/renderfin/api-render").json()["servers"]), 0)

    def test_get_task_by_url_404(self):
        resp = self.client.get(
            "/renderfin/api-render-get-task-by-url", params={"url": "https://nope/x.png"}
        )
        self.assertEqual(resp.status_code, 404)

    def test_character_gen_create_and_status(self):
        resp = self.client.post(
            "/renderfin/api-character-gen",
            json={"prompt": "orc warrior", "user_name": "bot"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["stage"], "flux_render")
        job_id = data["job_id"]

        status = self.client.get(f"/renderfin/api-character-gen/{job_id}")
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["stage"], "flux_render")

        discard = self.client.post(f"/renderfin/api-character-gen/{job_id}/discard")
        self.assertEqual(discard.status_code, 200)
        self.assertEqual(discard.json()["stage"], "discarded")

    def test_character_gen_404(self):
        self.assertEqual(
            self.client.get("/renderfin/api-character-gen/deadbeef").status_code, 404
        )


if __name__ == "__main__":
    unittest.main()

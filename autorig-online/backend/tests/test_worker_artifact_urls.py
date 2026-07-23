import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from worker_artifact_urls import canonical_worker_artifact_url


class WorkerArtifactUrlTests(unittest.TestCase):
    def test_rewrites_legacy_farm_artifact_url_to_files_host(self):
        self.assertEqual(
            canonical_worker_artifact_url(
                "https://converter-f13.freestock.online/converter/glb/guid/model.glb"
            ),
            "https://f13.freestock.online/guid/model.glb",
        )

    def test_preserves_query_string(self):
        self.assertEqual(
            canonical_worker_artifact_url(
                "https://converter-f2.freestock.online/converter/glb/guid/model.glb?v=7"
            ),
            "https://f2.freestock.online/guid/model.glb?v=7",
        )

    def test_leaves_api_and_non_farm_urls_unchanged(self):
        values = (
            "https://converter-f13.freestock.online/api-converter-glb/server-status",
            "https://renderfin.com/converter/glb/guid/model.glb",
            "/api/task/task-id/prepared.glb",
        )
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(canonical_worker_artifact_url(value), value)


if __name__ == "__main__":
    unittest.main()

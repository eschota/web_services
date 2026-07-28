import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from worker_artifact_urls import (
    canonical_worker_artifact_url,
    is_viewer_artifact_url,
    parse_worker_artifact_payload,
)


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

    def test_worker_payload_keeps_viewer_urls_out_of_download_outputs(self):
        guid = "226c54c6-8570-410c-b3cf-ddad22bd4e5b"
        regular = (
            f"https://converter-f13.freestock.online/converter/glb/{guid}/"
            f"{guid}_model_prepared.glb"
        )
        viewer_prepared = (
            f"https://converter-f13.freestock.online/converter/glb/{guid}/"
            f"{guid}_model_prepared_viewer.glb"
        )
        viewer_animations = (
            f"https://converter-f13.freestock.online/converter/glb/{guid}/"
            f"{guid}_all_animations_viewer.glb"
        )
        outputs, prepared, animations = parse_worker_artifact_payload(
            {
                "output_urls": [regular, viewer_prepared, viewer_animations],
            }
        )

        self.assertEqual(outputs, [regular])
        self.assertEqual(
            prepared,
            f"https://f13.freestock.online/{guid}/{guid}_model_prepared_viewer.glb",
        )
        self.assertEqual(
            animations,
            f"https://f13.freestock.online/{guid}/{guid}_all_animations_viewer.glb",
        )
        self.assertTrue(is_viewer_artifact_url(viewer_prepared))
        self.assertTrue(is_viewer_artifact_url(viewer_animations))
        self.assertFalse(is_viewer_artifact_url(regular))

    def test_dedicated_viewer_fields_are_optional_and_canonicalized(self):
        outputs, prepared, animations = parse_worker_artifact_payload(
            {
                "output_urls": ["https://worker.invalid/result.zip"],
                "viewer_prepared_glb_url": (
                    "https://converter-f2.freestock.online/converter/glb/guid/"
                    "guid_model_prepared_viewer.glb"
                ),
                "viewer_animations_glb_url": (
                    "https://converter-f2.freestock.online/converter/glb/guid/"
                    "guid_all_animations_viewer.glb"
                ),
            }
        )
        self.assertEqual(outputs, ["https://worker.invalid/result.zip"])
        self.assertEqual(prepared, "https://f2.freestock.online/guid/guid_model_prepared_viewer.glb")
        self.assertEqual(animations, "https://f2.freestock.online/guid/guid_all_animations_viewer.glb")


if __name__ == "__main__":
    unittest.main()

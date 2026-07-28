import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main
import tasks


GUID = "226c54c6-8570-410c-b3cf-ddad22bd4e5b"


class _ModelFilesResponse:
    status_code = 200
    content = b"{}"

    @staticmethod
    def json():
        return {
            "folders": {
                "root": {
                    "files": [
                        {
                            "name": f"{GUID}_model_prepared_viewer.glb",
                            "rel_path": f"{GUID}_model_prepared_viewer.glb",
                        },
                        {
                            "name": f"{GUID}_all_animations_viewer.glb",
                            "rel_path": f"{GUID}_all_animations_viewer.glb",
                        },
                        {
                            "name": f"{GUID}_model_prepared.glb",
                            "rel_path": f"{GUID}_model_prepared.glb",
                        },
                    ],
                },
            },
        }


class _ModelFilesClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, *_args, **_kwargs):
        return _ModelFilesResponse()


class ViewerArtifactReconciliationTests(unittest.IsolatedAsyncioTestCase):
    async def test_model_files_separates_viewer_glbs_from_download_counts(self):
        task = SimpleNamespace(
            guid=GUID,
            worker_api="https://converter-f13.freestock.online/api-converter-glb",
        )
        with patch.object(tasks.httpx, "AsyncClient", return_value=_ModelFilesClient()):
            outputs, prepared, animations = await tasks._fetch_concrete_worker_artifacts(task)

        self.assertEqual(
            outputs,
            [
                (
                    "https://converter-f13.freestock.online/converter/glb/"
                    f"{GUID}/{GUID}_model_prepared.glb"
                ),
            ],
        )
        self.assertEqual(
            prepared,
            f"https://f13.freestock.online/{GUID}/{GUID}_model_prepared_viewer.glb",
        )
        self.assertEqual(
            animations,
            f"https://f13.freestock.online/{GUID}/{GUID}_all_animations_viewer.glb",
        )

    def test_prepared_ready_accepts_declared_viewer_url_or_valid_cache(self):
        task = SimpleNamespace(
            id="task-id",
            ready_urls=[],
            fbx_glb_ready=False,
            viewer_prepared_glb_url="https://worker.invalid/model_prepared_viewer.glb",
        )
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "GLB_CACHE_DIR",
            Path(tmp),
        ):
            self.assertTrue(main._task_prepared_glb_ready(task))
            task.viewer_prepared_glb_url = None
            self.assertFalse(main._task_prepared_glb_ready(task))
            (Path(tmp) / "task-id_prepared_viewer.glb").write_bytes(
                b"glTF" + (2).to_bytes(4, "little") + (12).to_bytes(4, "little")
            )
            self.assertTrue(main._task_prepared_glb_ready(task))


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import main


GUID = "f6da62e7-8eff-48a4-a29a-b29a9ce32101"


def _task():
    filenames = (
        "all_animations.blend",
        "all_animations_unity.fbx",
        "hdrp.unitypackage",
        "model_prepared.glb",
        "model_prepared_rigged.blend",
        "rigged.blend",
        "video.mp4",
        "video_poster.jpg",
        "video_small.mp4",
    )
    urls = [f"https://worker.invalid/converter/glb/{GUID}/{GUID}_{name}" for name in filenames]
    return SimpleNamespace(
        id="2780320d-250e-4cfd-913d-161f5e3ebf03",
        guid=GUID,
        ready_urls=urls,
        output_urls=urls,
        status="done",
        worker_api="https://worker.invalid/api-converter-glb",
        progress_page=None,
    )


def _request(*, anon_id=None):
    headers = []
    if anon_id:
        headers.append((b"cookie", f"{main.ANON_COOKIE}={anon_id}".encode("ascii")))
    return Request({"type": "http", "method": "GET", "path": "/", "headers": headers})


class _WorkerMetaResponse:
    status_code = 200

    @staticmethod
    def json():
        return {"file_count": 83, "zip_size": 278044959}


class _WorkerMetaClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, *_args, **_kwargs):
        return _WorkerMetaResponse()


class _RangeResponse:
    status_code = 206

    def __init__(self, start, payload, total):
        self.payload = payload
        self.headers = {
            "Content-Range": f"bytes {start}-{start + len(payload) - 1}/{total}"
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def aiter_bytes(self, _chunk_size):
        yield self.payload


class _RangeClient:
    def __init__(self, payload):
        self.payload = payload

    def stream(self, _method, _url, *, headers, **_kwargs):
        start, end = (
            int(value)
            for value in headers["Range"].removeprefix("bytes=").split("-", 1)
        )
        return _RangeResponse(start, self.payload[start:end + 1], len(self.payload))


class TaskBundleDownloadTests(unittest.IsolatedAsyncioTestCase):
    def test_download_access_for_registered_owner_admin_and_anonymous_owner(self):
        registered_task = SimpleNamespace(owner_type="user", owner_id="owner@example.com")
        main._require_task_download_access(
            task=registered_task,
            user=SimpleNamespace(email="owner@example.com"),
            request=_request(),
        )
        main._require_task_download_access(
            task=registered_task,
            user=SimpleNamespace(email="eschota@gmail.com"),
            request=_request(),
        )
        anonymous_task = SimpleNamespace(owner_type="anon", owner_id="anon-owner")
        main._require_task_download_access(
            task=anonymous_task,
            user=None,
            request=_request(anon_id="anon-owner"),
        )

    def test_download_access_rejects_guest_and_other_anonymous_visitor(self):
        task = SimpleNamespace(owner_type="anon", owner_id="anon-owner")
        with self.assertRaises(HTTPException) as guest:
            main._require_task_download_access(task=task, user=None, request=_request())
        self.assertEqual(guest.exception.status_code, 401)
        with self.assertRaises(HTTPException) as stranger:
            main._require_task_download_access(
                task=task,
                user=None,
                request=_request(anon_id="different-anon"),
            )
        self.assertEqual(stranger.exception.status_code, 403)

    def test_primary_downloads_exclude_preview_media(self):
        urls = main._task_primary_download_urls(_task())
        self.assertEqual(len(urls), 6)
        self.assertFalse(any("_video" in url for url in urls))

    def test_primary_downloads_keep_pipeline_specific_json_artifacts(self):
        task = _task()
        task.ready_urls = task.ready_urls + [
            f"https://worker.invalid/converter/glb/{GUID}/{GUID}_skeleton.json",
            f"https://worker.invalid/converter/glb/{GUID}/{GUID}_rig_preview.mp4",
        ]
        task.output_urls = task.ready_urls
        urls = main._task_primary_download_urls(task)
        self.assertTrue(any(url.endswith("_skeleton.json") for url in urls))
        self.assertFalse(any(url.endswith("_rig_preview.mp4") for url in urls))

    async def test_worker_internal_count_is_replaced_with_primary_count(self):
        task = _task()
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(main, "TASK_CACHE_DIR", Path(tmp)),
            patch.object(main, "_worker_bundle_zip_available", AsyncMock(return_value=True)),
            patch.object(main.httpx, "AsyncClient", return_value=_WorkerMetaClient()),
        ):
            meta = await main._load_task_bundle_meta(task)
        self.assertTrue(meta["bundle_file_count_ready"])
        self.assertEqual(meta["bundle_file_count"], 6)
        self.assertEqual(meta["bundle_file_count_source"], "worker_meta")

    async def test_large_worker_file_can_be_reassembled_from_ranges(self):
        payload = b"0123456789abcdef"
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "artifact.bin"
            size = await main._download_worker_file_by_ranges(
                _RangeClient(payload),
                "https://worker.invalid/artifact.bin",
                destination,
                chunk_bytes=5,
            )
            self.assertEqual(size, len(payload))
            self.assertEqual(destination.read_bytes(), payload)

    async def test_fallback_zip_contains_only_six_primary_files(self):
        task = _task()
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / task.id
            cache_dir.mkdir()
            for name in main._task_primary_download_names(task):
                (cache_dir / name).write_bytes(name.encode("utf-8"))
            (cache_dir / "video.mp4").write_bytes(b"preview")
            with (
                patch.object(main, "TASK_CACHE_DIR", Path(tmp)),
                patch.object(main, "cache_task_files", AsyncMock(return_value={"cached": True, "files": []})),
            ):
                response = await main._build_task_bundle_zip_from_cache(task)
                archive_path = Path(response.path)
                try:
                    with zipfile.ZipFile(archive_path) as archive:
                        names = archive.namelist()
                    self.assertEqual(len(names), 6)
                    self.assertNotIn("video.mp4", names)
                finally:
                    archive_path.unlink(missing_ok=True)

    def test_complete_primary_cache_is_detected_without_preview_files(self):
        task = _task()
        with tempfile.TemporaryDirectory() as tmp, patch.object(main, "TASK_CACHE_DIR", Path(tmp)):
            cache_dir = Path(tmp) / task.id
            cache_dir.mkdir()
            for name in main._task_primary_download_names(task):
                (cache_dir / name).write_bytes(b"primary")
            self.assertTrue(main._has_complete_primary_task_cache(task))
            (cache_dir / "rigged.blend").unlink()
            self.assertFalse(main._has_complete_primary_task_cache(task))

    async def test_fallback_without_primary_files_is_controlled_404(self):
        task = _task()
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(main, "TASK_CACHE_DIR", Path(tmp)),
            patch.object(main, "cache_task_files", AsyncMock(return_value={"cached": False, "files": []})),
        ):
            with self.assertRaises(HTTPException) as raised:
                await main._build_task_bundle_zip_from_cache(task)
        self.assertEqual(raised.exception.status_code, 404)
        self.assertIn("no primary task files", raised.exception.detail)

    async def test_notification_failure_never_breaks_download(self):
        with patch(
            "telegram_bot.broadcast_full_bundle_download",
            AsyncMock(side_effect=RuntimeError("telegram unavailable")),
        ):
            await main._notify_task_bundle_download("task-id", "anonymous owner")


if __name__ == "__main__":
    unittest.main()

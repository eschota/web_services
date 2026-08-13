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


class _BufferedRangeResponse:
    status_code = 206

    def __init__(self, payload, start, end):
        self._payload = payload[start:end + 1]
        self.headers = {"Content-Range": f"bytes {start}-{end}/{len(payload)}"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def aread(self):
        return self._payload


class _BufferedRangeClient:
    payload = b""

    def __init__(self, *_args, **_kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def stream(self, _method, _url, *, headers):
        start, end = (
            int(value)
            for value in headers["Range"].removeprefix("bytes=").split("-", 1)
        )
        return _BufferedRangeResponse(self.payload, start, end)


class TaskBundleDownloadTests(unittest.IsolatedAsyncioTestCase):
    def test_single_range_parser_supports_full_explicit_and_suffix_requests(self):
        self.assertEqual(main._parse_single_http_byte_range(None, 10), (0, 9, False))
        self.assertEqual(main._parse_single_http_byte_range("bytes=2-5", 10), (2, 5, True))
        self.assertEqual(main._parse_single_http_byte_range("bytes=-3", 10), (7, 9, True))
        with self.assertRaises(ValueError):
            main._parse_single_http_byte_range("bytes=20-30", 10)

    async def test_large_bundle_stream_is_reassembled_from_verified_ranges(self):
        payload = b"0123456789abcdef"
        _BufferedRangeClient.payload = payload
        with patch.object(main.httpx, "AsyncClient", _BufferedRangeClient):
            chunks = [
                chunk
                async for chunk in main._iter_worker_file_ranges(
                    "https://worker.invalid/bundle.zip",
                    0,
                    len(payload) - 1,
                    total_size=len(payload),
                    chunk_bytes=5,
                )
            ]
        self.assertEqual(b"".join(chunks), payload)
        self.assertEqual([len(chunk) for chunk in chunks], [5, 5, 5, 1])

    async def test_bundle_proxy_exposes_resume_headers_without_full_upstream_get(self):
        request = Request({
            "type": "http",
            "method": "GET",
            "path": "/bundle.zip",
            "headers": [(b"range", b"bytes=2-5")],
        })

        async def body():
            yield b"2345"

        with (
            patch.object(
                main,
                "_probe_worker_file_range",
                AsyncMock(return_value={"total_size": 10, "content_type": "application/zip"}),
            ),
            patch.object(main, "_iter_worker_file_ranges", return_value=body()),
        ):
            response = await main._proxy_worker_bundle_by_ranges(
                "https://worker.invalid/bundle.zip",
                "bundle.zip",
                request,
            )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers["content-range"], "bytes 2-5/10")
        self.assertEqual(response.headers["content-length"], "4")
        self.assertEqual(b"".join([chunk async for chunk in response.body_iterator]), b"2345")

    def test_preserved_task_cache_is_not_evictable(self):
        with tempfile.TemporaryDirectory() as tmp:
            task_dir = Path(tmp) / "task-id"
            task_dir.mkdir()
            self.assertFalse(main._task_cache_dir_is_preserved(task_dir))
            (task_dir / main.TASK_CACHE_PRESERVE_MARKER).write_text(
                "non-regenerable owner download cache\n",
                encoding="utf-8",
            )
            self.assertTrue(main._task_cache_dir_is_preserved(task_dir))

    def test_bundle_zip_purge_skips_preserved_task_cache(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            main,
            "TASK_CACHE_DIR",
            Path(tmp),
        ):
            preserved_dir = Path(tmp) / "preserved-task"
            preserved_meta = preserved_dir / ".meta"
            preserved_meta.mkdir(parents=True)
            (preserved_dir / main.TASK_CACHE_PRESERVE_MARKER).write_text(
                "preserve\n",
                encoding="utf-8",
            )
            preserved_zip = preserved_meta / "primary-bundle.zip"
            preserved_zip.write_bytes(b"preserved")

            regular_dir = Path(tmp) / "regular-task"
            regular_dir.mkdir()
            regular_zip = regular_dir / "primary-bundle.zip"
            regular_zip.write_bytes(b"regenerable")

            deleted, freed = main.purge_task_cache_bundle_zips()

            self.assertEqual(deleted, 1)
            self.assertEqual(freed, len(b"regenerable"))
            self.assertTrue(preserved_zip.is_file())
            self.assertFalse(regular_zip.exists())

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

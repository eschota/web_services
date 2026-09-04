import json
import io
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
        input_url="https://autorig.online/u/source/model.glb",
    )


def _request(*, anon_id=None, range_header=None):
    headers = []
    if anon_id:
        headers.append((b"cookie", f"{main.ANON_COOKIE}={anon_id}".encode("ascii")))
    if range_header:
        headers.append((b"range", range_header.encode("ascii")))
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
    async def test_model_glb_streams_exact_range_from_central_deliverable_zip(self):
        task = _task()
        payload = (
            b"glTF"
            + (2).to_bytes(4, "little")
            + (20).to_bytes(4, "little")
            + b"payload!"
        )
        entry = {
            "member_size": len(payload),
            "prefix": payload,
            "etag": '"zip-test"',
        }

        def iter_member(_entry, *, start, end):
            yield payload[start:end + 1]

        with (
            patch.object(main, "get_task_by_id", AsyncMock(return_value=task)),
            patch.object(main, "lookup_cached_artifact", return_value=None),
            patch.object(main, "lookup_cached_archive_member", return_value=entry),
            patch.object(main, "iter_cached_archive_member", side_effect=iter_member),
            patch.object(main, "_proxy_model_file", AsyncMock()) as worker_proxy,
        ):
            response = await main.api_proxy_model_glb(
                task.id,
                _request(range_header="bytes=0-3"),
                db=object(),
            )
            body = b"".join([chunk async for chunk in response.body_iterator])

        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.headers["content-range"], f"bytes 0-3/{len(payload)}")
        self.assertEqual(response.headers["x-artifact-cache"], "archive-member")
        self.assertEqual(body, b"glTF")
        worker_proxy.assert_not_awaited()

    async def test_rig_json_is_recovered_with_exact_collection_identity(self):
        task = _task()
        task.status = "done"
        task.collection_guid = "collection-1"
        task.collection_index = 14
        raw = json.dumps(
            {
                "collection_guid": task.collection_guid,
                "collection_index": task.collection_index,
                "collection_title": "Mystical Fantasy Beings",
            }
        ).encode("utf-8")
        entry = {
            "member_size": len(raw),
            "prefix": raw,
            "etag": '"zip-rig"',
        }
        with (
            patch.object(main, "get_task_by_id", AsyncMock(return_value=task)),
            patch.object(main, "lookup_cached_artifact", return_value=None),
            patch.object(main, "lookup_cached_archive_member", return_value=entry),
            patch.object(main, "read_cached_archive_member", return_value=raw),
        ):
            response = await main.api_task_rig_json(
                task.id,
                _request(),
                db=object(),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-artifact-cache"], "archive-member")
        self.assertEqual(json.loads(response.body), json.loads(raw))

    async def test_rig_json_fails_closed_on_collection_mismatch(self):
        task = _task()
        task.status = "done"
        task.collection_guid = "collection-1"
        task.collection_index = 14
        raw = b'{"collection_guid":"other","collection_index":14}'
        entry = {"member_size": len(raw), "prefix": raw, "etag": '"zip-rig"'}
        with (
            patch.object(main, "get_task_by_id", AsyncMock(return_value=task)),
            patch.object(main, "lookup_cached_artifact", return_value=None),
            patch.object(main, "lookup_cached_archive_member", return_value=entry),
            patch.object(main, "read_cached_archive_member", return_value=raw),
        ):
            with self.assertRaises(HTTPException) as raised:
                await main.api_task_rig_json(task.id, _request(), db=object())

        self.assertEqual(raised.exception.status_code, 503)

    async def test_animations_glb_prefers_central_cached_100k_before_worker(self):
        task = _task()
        task.viewer_animations_glb_url = None
        durable = {
            "path": Path("central/hero_100k/hero_all_animations.glb"),
            "internal_uri": "/_autorig_artifacts/task/hero_100k/animations.glb",
        }

        def cache_lookup(_task_id, **kwargs):
            if kwargs.get("relative_path_fragment") == "_100k/":
                return durable
            return None

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(main, "GLB_CACHE_DIR", Path(tmp)),
            patch.object(main, "get_task_by_id", AsyncMock(return_value=task)),
            patch.object(main, "lookup_cached_artifact", side_effect=cache_lookup) as lookup,
            patch.object(main, "_validate_viewer_animation_glb_file", return_value=True),
            patch.object(main, "_get_cached_glb", AsyncMock()) as worker_cache,
        ):
            response = await main.api_proxy_animations_glb(
                task.id,
                _request(),
                db=object(),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-artifact-cache"], "hit")
        self.assertEqual(lookup.call_args.kwargs["relative_path_fragment"], "_100k/")
        worker_cache.assert_not_awaited()

    def test_worker_bundle_prefers_declared_nested_zip_and_keeps_legacy_fallback(self):
        task = _task()
        nested = f"https://worker.invalid/converter/glb/{GUID}/{GUID}.zip"
        task.ready_urls.append(nested)
        task.output_urls.append(nested)

        self.assertEqual(main.resolve_worker_full_bundle_zip_url(task), nested)

        task.ready_urls = [url for url in task.ready_urls if url != nested]
        task.output_urls = [url for url in task.output_urls if url != nested]
        self.assertEqual(
            main.resolve_worker_full_bundle_zip_url(task),
            f"https://worker.invalid/converter/glb/{GUID}.zip",
        )

    async def test_artifact_discovery_caches_declared_bundle_only_once(self):
        task = _task()
        nested = f"https://worker.invalid/converter/glb/{GUID}/{GUID}.zip"
        task.ready_urls.append(nested)
        task.output_urls.append(nested)
        task.viewer_prepared_glb_url = None
        task.viewer_animations_glb_url = None
        task.video_url = None

        with (
            patch.object(
                main,
                "_fetch_worker_model_files",
                AsyncMock(return_value=(False, [], None, "not needed")),
            ),
            patch.object(main, "resolve_poster_url_for_task", return_value=None),
        ):
            sources = await main._discover_task_artifact_sources(task)

        matching = [source for source in sources if source.url == nested]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].role, "full_bundle")
        self.assertEqual(matching[0].relative_path, f"deliverables/{GUID}.zip")

    async def test_artifact_discovery_does_not_require_synthesized_bundle(self):
        task = _task()
        task.viewer_prepared_glb_url = None
        task.viewer_animations_glb_url = None
        task.video_url = None

        with (
            patch.object(
                main,
                "_fetch_worker_model_files",
                AsyncMock(return_value=(False, [], None, "not needed")),
            ),
            patch.object(main, "resolve_poster_url_for_task", return_value=None),
        ):
            sources = await main._discover_task_artifact_sources(task)

        self.assertFalse(any(source.role == "full_bundle" for source in sources))

    async def test_cached_files_quotes_durable_urls_without_local_task_directory(self):
        task = SimpleNamespace(
            id="durable-only-task",
            guid=GUID,
            status="done",
            artifact_cache_status="ready",
        )

        def cached_entry(_task_id, *, source_url=None, role=None):
            if source_url:
                return {"size": 123}
            if role == "full_bundle":
                return {"size": 456}
            return None

        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(main, "TASK_CACHE_DIR", Path(tmp)),
            patch.object(main, "get_task_by_id", AsyncMock(return_value=task)),
            patch.object(main, "_require_task_download_access"),
            patch.object(main, "_task_primary_download_names", return_value=set()),
            patch.object(main, "_materialize_task_recovery_files", return_value={}),
            patch.object(main, "_task_primary_download_urls", return_value=["https://worker.invalid/model"]),
            patch.object(main, "lookup_cached_artifact", side_effect=cached_entry),
            patch.object(main, "_clean_filename_for_cache", return_value="model file.zip"),
        ):
            result = await main.api_task_cached_files(
                task.id,
                _request(),
                user=SimpleNamespace(email="admin@example.com"),
                db=object(),
            )

        self.assertTrue(result["cached"])
        self.assertEqual(result["files"][0]["url"], "/api/file/durable-only-task/download/model%20file.zip")
        self.assertEqual(result["bundle_total_size"], 456)

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

    @staticmethod
    def _finished_zip():
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("model.bin", b"x" * 100000)
            archive.comment = b"worker bundle"
        return stream.getvalue()

    async def test_bundle_probe_requires_final_zip_footer_and_preserves_etag(self):
        payload = self._finished_zip()
        seen_headers = []

        class Client(_BufferedRangeClient):
            def stream(self, *args, headers):
                seen_headers.append(dict(headers))
                response = super().stream(*args, headers=headers)
                response.headers["ETag"] = '"final-zip"'
                return response

        Client.payload = payload
        with patch.object(main.httpx, "AsyncClient", Client):
            probe = await main._probe_worker_file_range("https://worker.invalid/bundle.zip")
        self.assertEqual(probe["total_size"], len(payload))
        self.assertEqual(probe["etag"], '"final-zip"')
        self.assertEqual(seen_headers[0]["Range"], "bytes=0-0")
        self.assertEqual(seen_headers[1]["If-Match"], '"final-zip"')
        self.assertTrue(all(h["Accept-Encoding"] == "identity" for h in seen_headers))

    async def test_unfinished_zip_is_rejected_before_response_headers(self):
        _BufferedRangeClient.payload = self._finished_zip()[:-40]
        with patch.object(main.httpx, "AsyncClient", _BufferedRangeClient):
            with self.assertRaises(HTTPException) as caught:
                await main._probe_worker_file_range("https://worker.invalid/bundle.zip")
        self.assertEqual(caught.exception.status_code, 503)

    async def test_growing_zip_is_rejected_even_if_range_endpoint_answers(self):
        payload = self._finished_zip()

        class GrowingClient(_BufferedRangeClient):
            calls = 0

            def stream(self, *args, headers):
                self.calls += 1
                self.payload = payload if self.calls == 1 else payload + b"growing"
                return super().stream(*args, headers=headers)

        with patch.object(main.httpx, "AsyncClient", GrowingClient):
            with self.assertRaises(HTTPException) as caught:
                await main._probe_worker_file_range("https://worker.invalid/bundle.zip")
        self.assertEqual(caught.exception.status_code, 503)

    async def test_stream_pins_the_probed_representation(self):
        seen_headers = []

        class Client(_BufferedRangeClient):
            payload = b"1234567890"

            def stream(self, *args, headers):
                seen_headers.append(dict(headers))
                return super().stream(*args, headers=headers)

        with patch.object(main.httpx, "AsyncClient", Client):
            chunks = [chunk async for chunk in main._iter_worker_file_ranges(
                "https://worker.invalid/bundle.zip", 0, 9,
                total_size=10, chunk_bytes=4, etag='"final-zip"',
            )]
        self.assertEqual(b"".join(chunks), b"1234567890")
        self.assertTrue(all(h["If-Match"] == '"final-zip"' for h in seen_headers))

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
                patch.object(main, "GLB_CACHE_DIR", Path(tmp) / "glb"),
                patch.object(main, "_RECOVERY_DELIVERABLES_DIR", Path(tmp) / "deliverables"),
                patch.object(main, "cache_task_files", AsyncMock(return_value={"cached": True, "files": []})),
            ):
                response = await main._build_task_bundle_zip_from_cache(task)
                archive_path = cache_dir / ".meta" / "primary-bundle.zip"
                try:
                    with zipfile.ZipFile(archive_path) as archive:
                        names = archive.namelist()
                    self.assertEqual(len(names), 6)
                    self.assertNotIn("video.mp4", names)
                    self.assertEqual(
                        response.headers["x-accel-redirect"],
                        f"/_autorig_task_cache/{task.id}/.meta/primary-bundle.zip",
                    )
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
            patch.object(main, "GLB_CACHE_DIR", Path(tmp) / "glb"),
            patch.object(main, "_RECOVERY_DELIVERABLES_DIR", Path(tmp) / "deliverables"),
            patch.object(main, "cache_task_files", AsyncMock(return_value={"cached": False, "files": []})),
        ):
            with self.assertRaises(HTTPException) as raised:
                await main._build_task_bundle_zip_from_cache(task)
        self.assertEqual(raised.exception.status_code, 404)
        self.assertIn("no longer stored", raised.exception.detail)

    async def test_protected_recovery_glbs_replace_expired_worker_bundle(self):
        task = _task()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache_root = root / "tasks"
            glb_root = root / "glb"
            deliverables_root = root / "deliverables"
            glb_root.mkdir()
            deliverables_root.mkdir()
            payload = b"glTF" + b"\x02\x00\x00\x00" + b"\x0c\x00\x00\x00"
            (glb_root / f"{task.id}_prepared.glb").write_bytes(payload)
            (deliverables_root / f"{task.id}_animations.glb").write_bytes(payload + b"animation")

            with (
                patch.object(main, "TASK_CACHE_DIR", cache_root),
                patch.object(main, "GLB_CACHE_DIR", glb_root),
                patch.object(main, "_RECOVERY_DELIVERABLES_DIR", deliverables_root),
                patch.object(main, "cache_task_files", AsyncMock()) as cache_mock,
            ):
                recovery = await main.task_download_recovery_state(task)
                response = await main._build_task_bundle_zip_from_cache(task)
                archive_path = cache_root / task.id / ".meta" / "primary-bundle.zip"
                with zipfile.ZipFile(archive_path) as archive:
                    names = set(archive.namelist())

            self.assertFalse(recovery["downloads_expired"])
            self.assertEqual(names, {"model_prepared.glb", "all_animations.glb"})
            self.assertEqual(response.headers["accept-ranges"], "bytes")
            cache_mock.assert_not_awaited()

    async def test_notification_failure_never_breaks_download(self):
        with patch(
            "telegram_bot.broadcast_full_bundle_download",
            AsyncMock(side_effect=RuntimeError("telegram unavailable")),
        ):
            await main._notify_task_bundle_download("task-id", "anonymous owner")


if __name__ == "__main__":
    unittest.main()

import asyncio
import io
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from PIL import Image
from pydantic import ValidationError

import ai_video_reference


class VideoReferenceTests(unittest.TestCase):
    def test_request_rejects_bad_view_url_and_frame_count(self):
        with self.assertRaises(ValidationError):
            ai_video_reference.VideoReferenceRequest(
                video_url="https://evil.test/a.mp4", view="first_frame"
            )
        with self.assertRaises(ValidationError):
            ai_video_reference.VideoReferenceRequest(
                video_url="https://pvs1.microstock.plus/a.mp4", view="faces"
            )
        with self.assertRaises(ValidationError):
            ai_video_reference.VideoReferenceRequest(
                video_url="https://pvs1.microstock.plus/a.mp4", frame_count=96
            )

    def test_capability_path_requires_id_and_matching_hash(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(
            ai_video_reference, "ASSET_DIR", Path(folder)
        ):
            data = b"png bytes"
            stored = ai_video_reference._store_png(data, {"width_int": 1, "height_int": 1})
            path = ai_video_reference._resolve_asset(
                stored["asset_id_string"], stored["sha256_string"]
            )
            self.assertEqual(path.read_bytes(), data)
            with self.assertRaises(HTTPException):
                ai_video_reference._resolve_asset(
                    stored["asset_id_string"], "0" * 64
                )

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg unavailable")
    def test_first_frame_and_storyboard_from_synthetic_video(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, patch.object(
                ai_video_reference, "ASSET_DIR", Path(folder) / "assets-root"
            ), patch.object(
                ai_video_reference, "PUBLIC_BASE_URL", "https://autorig.online"
            ):
                source = Path(folder) / "source.mp4"
                created = subprocess.run([
                    "ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                    "-i", "testsrc=size=320x180:rate=24:duration=4.041667",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source),
                ], capture_output=True, check=False)
                self.assertEqual(created.returncode, 0, created.stderr.decode(errors="replace"))
                source_bytes = source.read_bytes()

                async def fake_prepare(_client, _url, frame_count, fps=24, *, allow_shorter=False):
                    self.assertEqual(frame_count, 97)
                    self.assertEqual(fps, 24)
                    self.assertTrue(allow_shorter)
                    return "control.mp4", source_bytes

                with patch.object(ai_video_reference, "download_prepare_video", fake_prepare):
                    first = await ai_video_reference._derive_reference(
                        ai_video_reference.VideoReferenceRequest(
                            video_url="https://pvs1.microstock.plus/a.mp4",
                            view="first_frame",
                        )
                    )
                    storyboard = await ai_video_reference._derive_reference(
                        ai_video_reference.VideoReferenceRequest(
                            video_url="https://pvs1.microstock.plus/a.mp4",
                            view="storyboard",
                        )
                    )
                self.assertTrue(first["finished_bool"])
                self.assertEqual((first["width_int"], first["height_int"]), (320, 180))
                self.assertEqual(storyboard["frame_indices_int_array"], [0, 24, 48, 72, 96])
                self.assertEqual(storyboard["timepoints_seconds_float_array"], [0, 1, 2, 3, 4])
                self.assertEqual((storyboard["width_int"], storyboard["height_int"]), (1920, 216))
                self.assertNotEqual(first["asset_id_string"], storyboard["asset_id_string"])
                image_path = ai_video_reference._resolve_asset(
                    storyboard["asset_id_string"], storyboard["sha256_string"]
                )
                with Image.open(image_path) as image:
                    self.assertEqual(image.format, "PNG")

        asyncio.run(scenario())

    def test_endpoint_cache_key_includes_view_frames_and_source_fingerprint(self):
        async def scenario():
            body = ai_video_reference.VideoReferenceRequest(
                video_url="https://pvs1.microstock.plus/a.mp4",
                view="storyboard", frame_count=97,
            )
            captured = {}

            async def fake_cache(service, payload, callback, *, namespace):
                captured.update({"service": service, "payload": payload, "namespace": namespace})
                return {"finished_bool": True}

            import ai_request_cache
            with patch.object(ai_request_cache, "run_cached", fake_cache):
                result = await ai_video_reference.api_video_reference(body)
            self.assertTrue(result["finished_bool"])
            self.assertEqual(captured["service"], "control")
            self.assertEqual(captured["namespace"], "video-reference-v2")
            self.assertEqual(captured["payload"]["view"], "storyboard")
            self.assertEqual(captured["payload"]["frame_count"], 97)
            self.assertRegex(captured["payload"]["source_fingerprint_string"], r"^[a-f0-9]{64}$")

        asyncio.run(scenario())

    def test_asset_capability_supports_head_for_ui_polling(self):
        route = next(
            row for row in ai_video_reference.router.routes
            if row.path == "/api/ai/video-references/{asset_id}/{sha256}.png"
        )
        self.assertEqual(route.methods, {"GET", "HEAD"})

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg unavailable")
    def test_first_frame_accepts_shorter_normalized_reference(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, patch.object(
                ai_video_reference, "ASSET_DIR", Path(folder) / "assets-root"
            ), patch.object(
                ai_video_reference, "PUBLIC_BASE_URL", "https://autorig.online"
            ):
                source = Path(folder) / "short.mp4"
                made = subprocess.run([
                    "ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                    "-i", "testsrc=size=320x180:rate=24:duration=1",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source),
                ], capture_output=True, check=False)
                self.assertEqual(made.returncode, 0, made.stderr.decode(errors="replace"))

                async def fake_prepare(_client, _url, frame_count, fps=24, *, allow_shorter=False):
                    self.assertTrue(allow_shorter)
                    return "short.mp4", source.read_bytes()

                with patch.object(ai_video_reference, "download_prepare_video", fake_prepare):
                    result = await ai_video_reference._derive_reference(
                        ai_video_reference.VideoReferenceRequest(
                            video_url="https://pvs1.microstock.plus/short.mp4",
                            view="first_frame", frame_count=97,
                        )
                    )
                self.assertTrue(result["finished_bool"])
                self.assertEqual(result["duration_seconds_float"], 1.0)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

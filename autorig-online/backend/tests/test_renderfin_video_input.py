import asyncio
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from renderfin import config
from renderfin import video_input


class _StreamResponse:
    def __init__(self, status=200, chunks=(), headers=None):
        self.status_code = status
        self._chunks = list(chunks)
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def aiter_bytes(self, _size):
        for chunk in self._chunks:
            yield chunk


class _Client:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def stream(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.response


class VideoInputPolicyTests(unittest.TestCase):
    def test_url_allowlist(self):
        accepted = [
            "https://autorig.online/dev/api/scratch/a.mp4",
            "https://autorig.online/renderfin/render/user/a.mp4?token=public",
            "https://autorig.online/api/ai/avatar-assets/a.mp4",
            "https://pvs1.microstock.plus/path/a.mp4",
            "https://pvs9.microstock.plus/path/a.mp4",
        ]
        for url in accepted:
            self.assertEqual(video_input._validated_url(url), url)
        rejected = [
            "http://autorig.online/dev/api/scratch/a.mp4",
            "https://user:pass@autorig.online/dev/api/scratch/a.mp4",
            "https://autorig.online:443/dev/api/scratch/a.mp4",
            "https://autorig.online/private/a.mp4",
            "https://pvs0.microstock.plus/a.mp4",
            "https://pvs10.microstock.plus/a.mp4",
            "https://pvs1.microstock.plus.evil.test/a.mp4",
            "https://127.0.0.1/a.mp4",
        ]
        for url in rejected:
            with self.assertRaises(video_input.VideoInputError, msg=url):
                video_input._validated_url(url)

    def test_frame_contract(self):
        for count in (9, 17, 97, 393):
            self.assertEqual(video_input._validated_frame_count(count, 24), (count, 24))
        for count in (8, 10, 394):
            with self.assertRaises(video_input.VideoInputError):
                video_input._validated_frame_count(count, 24)
        with self.assertRaises(video_input.VideoInputError):
            video_input._validated_frame_count(97, 30)

    def test_redirect_and_stream_limit_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "source.mp4"
            client = _Client(_StreamResponse(status=302))
            with self.assertRaises(video_input.VideoInputError):
                asyncio.run(video_input._download(client, "https://pvs1.microstock.plus/a.mp4", target))
            client = _Client(_StreamResponse(chunks=(b"a" * 5, b"b" * 6)))
            with patch.object(video_input, "MAX_VIDEO_BYTES", 10):
                with self.assertRaises(video_input.VideoInputError):
                    asyncio.run(video_input._download(client, "https://pvs1.microstock.plus/a.mp4", target))

    def test_preparation_cleans_only_its_unique_folder(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, patch.object(
                config, "RENDER_DIR", Path(folder) / "render"
            ):
                sibling = config.RENDER_DIR / ".video-input-temp" / "keep"
                sibling.mkdir(parents=True)
                (sibling / "marker").write_text("keep", encoding="utf-8")

                async def fake_download(_client, _url, target):
                    target.write_bytes(b"source")

                calls = []

                async def fake_probe(path, *, count_frames=False):
                    return {
                        "format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2", "duration": "4.1"},
                        "streams": [{"codec_type": "video", "codec_name": "h264",
                                     "pix_fmt": "yuv420p",
                                     "width": 960, "height": 540,
                                     "nb_read_frames": "97" if count_frames else "N/A"}],
                    }

                async def fake_process(*argv):
                    calls.append(argv)
                    Path(argv[-1]).write_bytes(b"prepared")
                    return b""

                with patch.object(video_input, "_download", fake_download), \
                     patch.object(video_input, "_probe", fake_probe), \
                     patch.object(video_input, "_run_process", fake_process):
                    name, payload = await video_input.download_prepare_video(
                        object(), "https://pvs1.microstock.plus/a.mp4", 97
                    )
                self.assertRegex(name, r"^control-[0-9a-f]{32}\.mp4$")
                self.assertEqual(payload, b"prepared")
                self.assertTrue((sibling / "marker").is_file())
                children = sorted(path.name for path in sibling.parent.iterdir())
                self.assertEqual(children, ["keep"])
                self.assertIn("-frames:v", calls[0])
                self.assertNotIn("-c:v copy", " ".join(calls[0]))

        asyncio.run(scenario())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg unavailable")
    def test_real_ffmpeg_fixture_has_exact_contract(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, patch.object(
                config, "RENDER_DIR", Path(folder) / "render"
            ):
                source = Path(folder) / "fixture.mp4"
                made = subprocess.run([
                    "ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                    "-i", "testsrc=size=322x242:rate=30:duration=1",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source),
                ], capture_output=True, check=False)
                self.assertEqual(made.returncode, 0, made.stderr.decode(errors="replace"))

                async def fake_download(_client, _url, target):
                    shutil.copyfile(source, target)

                with patch.object(video_input, "_download", fake_download):
                    name, payload = await video_input.download_prepare_video(
                        object(), "https://pvs1.microstock.plus/fixture.mp4", 17
                    )
                result = Path(folder) / name
                result.write_bytes(payload)
                probe = await video_input._probe(result, count_frames=True)
                stream = video_input._validate_mp4_probe(probe)
                self.assertEqual(stream["codec_name"], "h264")
                self.assertEqual(stream["pix_fmt"], "yuv420p")
                self.assertEqual(int(stream["nb_read_frames"]), 17)
                self.assertLessEqual(int(stream["width"]), 2048)
                self.assertLessEqual(int(stream["height"]), 2048)

        asyncio.run(scenario())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg unavailable")
    def test_real_four_second_25fps_source_holds_tail_for_inclusive_97(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, patch.object(
                config, "RENDER_DIR", Path(folder) / "render"
            ):
                source = Path(folder) / "four-seconds.mp4"
                made = subprocess.run([
                    "ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                    "-i", "testsrc=size=320x180:rate=25:duration=4",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source),
                ], capture_output=True, check=False)
                self.assertEqual(made.returncode, 0, made.stderr.decode(errors="replace"))

                async def fake_download(_client, _url, target):
                    shutil.copyfile(source, target)

                with patch.object(video_input, "_download", fake_download):
                    name, payload = await video_input.download_prepare_video(
                        object(), "https://pvs1.microstock.plus/four.mp4", 97
                    )
                result = Path(folder) / name
                result.write_bytes(payload)
                probe = await video_input._probe(result, count_frames=True)
                self.assertEqual(int(video_input._video_stream(probe)["nb_read_frames"]), 97)
                self.assertIn("held final frame 1 time(s)",
                              ((probe.get("format") or {}).get("tags") or {}).get("comment", ""))

        asyncio.run(scenario())

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "ffmpeg unavailable")
    def test_allow_shorter_reference_keeps_real_duration_without_padding(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, patch.object(
                config, "RENDER_DIR", Path(folder) / "render"
            ):
                source = Path(folder) / "short.mp4"
                made = subprocess.run([
                    "ffmpeg", "-v", "error", "-y", "-f", "lavfi",
                    "-i", "testsrc=size=320x180:rate=25:duration=1",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", str(source),
                ], capture_output=True, check=False)
                self.assertEqual(made.returncode, 0, made.stderr.decode(errors="replace"))

                async def fake_download(_client, _url, target):
                    shutil.copyfile(source, target)

                with patch.object(video_input, "_download", fake_download):
                    name, payload = await video_input.download_prepare_video(
                        object(), "https://pvs1.microstock.plus/short.mp4", 97,
                        allow_shorter=True,
                    )
                result = Path(folder) / name
                result.write_bytes(payload)
                probe = await video_input._probe(result, count_frames=True)
                self.assertEqual(int(video_input._video_stream(probe)["nb_read_frames"]), 24)
                self.assertIn("without tail padding",
                              ((probe.get("format") or {}).get("tags") or {}).get("comment", ""))

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

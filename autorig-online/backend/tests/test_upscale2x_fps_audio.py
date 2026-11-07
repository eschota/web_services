"""Upscale 2x keeps the clip's own frame rate and its original audio (2026-11-07).

A 30 fps clip came back at 24 fps with ComfyUI's AAC mono 69 kbps re-encode.
Now the control clip is prepared at the source's own rate, and the finished
render gets the source's first audio stream copied back unchanged.
"""
import asyncio
import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

from renderfin import config, video_input
from renderfin.models import RenderPrompt, RenderTask, TASK_DONE, TASK_PENDING
from renderfin.queue import (
    RenderQueue,
    _drop_upscale_source,
    _upscale_source_path,
)

HAVE_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))
CLIP_URL = "https://pvs1.microstock.plus/clip.mp4"


def _probe_row(*, rate="30/1", avg="30/1", nb_frames="300", duration="10.0",
               fmt_duration="10.02", decoded="300"):
    def fake_probe(path, *, count_frames=False):
        stream = {"codec_type": "video", "codec_name": "h264", "pix_fmt": "yuv420p",
                  "width": 640, "height": 360, "r_frame_rate": rate, "avg_frame_rate": avg,
                  "nb_frames": nb_frames, "duration": duration,
                  "nb_read_frames": decoded if count_frames else "N/A"}
        return {"format": {"format_name": "mov,mp4,m4a,3gp,3g2,mj2", "duration": fmt_duration},
                "streams": [stream]}
    return fake_probe


class _Patched:
    """download_prepare_video with the network, ffprobe and ffmpeg replaced."""

    def __init__(self, probe):
        self.calls = []
        self.probe = probe

    async def download(self, _client, _url, target):
        target.write_bytes(b"original clip bytes")

    async def probe_async(self, path, *, count_frames=False):
        return self.probe(path, count_frames=count_frames)

    async def process(self, *argv, timeout=0):
        self.calls.append(argv)
        Path(argv[-1]).write_bytes(b"prepared")
        return b""

    def __enter__(self):
        self.patches = [
            patch.object(video_input, "_download", self.download),
            patch.object(video_input, "_probe", self.probe_async),
            patch.object(video_input, "_run_process", self.process),
        ]
        for item in self.patches:
            item.start()
        return self

    def __exit__(self, *exc):
        for item in reversed(self.patches):
            item.stop()


class FrameRateTests(unittest.TestCase):
    def test_exact_rational_rate_is_kept(self):
        rate = video_input.source_frame_rate(
            {"r_frame_rate": "30000/1001", "avg_frame_rate": "30000/1001"})
        self.assertEqual(rate, Fraction(30000, 1001))
        self.assertEqual(video_input.rate_text(rate), "30000/1001")
        self.assertEqual(video_input.rate_text(Fraction(30)), "30")

    def test_variable_rate_clip_uses_measured_average(self):
        # Phone clip: nominal rate is a timebase artefact, the average is real.
        rate = video_input.source_frame_rate(
            {"r_frame_rate": "90000/1", "avg_frame_rate": "2987/100"})
        self.assertAlmostEqual(float(rate), 29.87, places=2)
        self.assertLessEqual(rate.denominator, 1001)

    def test_unreadable_or_absurd_rate_is_refused(self):
        with self.assertRaises(video_input.VideoInputError):
            video_input.source_frame_rate({"r_frame_rate": "0/0", "avg_frame_rate": "0/0"})
        with self.assertRaises(video_input.VideoInputError):
            video_input.source_frame_rate({"r_frame_rate": "240/1", "avg_frame_rate": "240/1"})

    def test_frame_count_prefers_container_count_then_ceil_of_duration(self):
        self.assertEqual(video_input.source_frame_count(
            {"nb_frames": "301"}, 10.0, Fraction(30)), 301)
        self.assertEqual(video_input.source_frame_count(
            {"duration": "10.0"}, 0, Fraction(30)), 300)
        self.assertEqual(video_input.source_frame_count(
            {"duration": "10.01"}, 0, Fraction(30)), 301)
        # webm: no stream duration, the container's is used
        self.assertEqual(video_input.source_frame_count(
            {}, 4.0, Fraction(60000, 1001)), 240)


class PrepareAtSourceRateTests(unittest.TestCase):
    def run_prepare(self, probe, **kwargs):
        async def scenario():
            with tempfile.TemporaryDirectory() as folder, \
                    patch.object(config, "RENDER_DIR", Path(folder) / "render"), \
                    _Patched(probe) as patched:
                keep = Path(folder) / "kept" / "source"
                if kwargs.pop("keep", False):
                    kwargs["keep_source_at"] = keep
                name, data = await video_input.download_prepare_video(
                    object(), CLIP_URL, 393, allow_shorter=True, keep_audio=True, **kwargs)
                kept = keep.read_bytes() if keep.is_file() else None
                return patched.calls, kept
        return asyncio.run(scenario())

    def test_source_fps_is_passed_to_the_normaliser(self):
        calls, kept = self.run_prepare(_probe_row(rate="30000/1001", avg="30000/1001",
                                                  nb_frames="300", decoded="300"),
                                       source_fps=True, keep=True)
        argv = list(calls[0])
        vf = argv[argv.index("-vf") + 1]
        self.assertTrue(vf.startswith("fps=30000/1001,"), vf)
        self.assertEqual(argv[argv.index("-frames:v") + 1], "300")
        self.assertEqual(kept, b"original clip bytes")

    def test_default_path_still_normalises_to_24(self):
        calls, kept = self.run_prepare(_probe_row(decoded="240"))
        argv = list(calls[0])
        self.assertTrue(argv[argv.index("-vf") + 1].startswith("fps=24,"))
        self.assertEqual(argv[argv.index("-frames:v") + 1], "393")
        self.assertIsNone(kept)

    def test_clip_over_the_frame_cap_is_refused_not_trimmed(self):
        with self.assertRaises(video_input.VideoInputError) as caught:
            self.run_prepare(_probe_row(rate="60/1", avg="60/1", nb_frames="600",
                                        duration="10.0", fmt_duration="10.0"),
                             source_fps=True)
        message = str(caught.exception)
        self.assertIn("600 frames", message)
        self.assertIn("393 frames", message)
        self.assertIn("split it first", message)

    def test_max_duration_rule_still_applies_first(self):
        with self.assertRaises(video_input.VideoInputError) as caught:
            self.run_prepare(_probe_row(nb_frames="510", fmt_duration="17.0"), source_fps=True)
        self.assertIn("16.4 s", str(caught.exception))


AAC_STEREO = {"codec": "aac", "channels": 2, "sample_rate": 48000, "bit_rate": 192000}


class RemuxCommandTests(unittest.TestCase):
    def build(self, audio, **kw):
        return video_input.build_remux_command(
            Path("r.mp4"), Path("s.mp4"), Path("o.mp4"), source_audio=audio, duration=10.0, **kw)

    def test_copy_both_streams_cut_to_render_length(self):
        argv, mode = self.build(AAC_STEREO)
        self.assertEqual(mode, "copy")
        line = " ".join(argv)
        self.assertIn("-i r.mp4 -i s.mp4", line)
        self.assertIn("-map 0:v:0 -c:v copy", line)
        self.assertIn("-map 1:a:0 -c:a copy", line)
        self.assertIn("-t 10.000000", line)
        self.assertIn("-movflags +faststart", line)
        self.assertEqual(argv[-1], "o.mp4")

    def test_audio_mp4_cannot_carry_unchanged_is_refused_not_reencoded(self):
        for codec in ("pcm_s16le", "vorbis", "flac"):
            with self.assertRaises(video_input.VideoInputError):
                self.build({"codec": codec, "channels": 2, "sample_rate": 48000, "bit_rate": None})

    def test_comfy_metadata_is_dropped(self):
        argv, _ = self.build(AAC_STEREO)
        self.assertIn("-map_metadata -1", " ".join(argv))

    def test_source_without_audio_gives_silent_output(self):
        argv, mode = self.build(None)
        self.assertEqual(mode, "none")
        self.assertIn("-an", argv)
        self.assertNotIn("1:a:0?", argv)

class OutputMetaTests(unittest.TestCase):
    def test_media_meta_shape(self):
        meta = video_input.media_meta({
            "format": {"duration": "10.03"},
            "streams": [
                {"codec_type": "video", "width": 1280, "height": 720,
                 "avg_frame_rate": "30000/1001", "duration": "10.010000"},
                {"codec_type": "audio", "codec_name": "aac", "channels": 2,
                 "sample_rate": "48000", "bit_rate": "192000"},
            ]})
        self.assertEqual(meta, {
            "w": 1280, "h": 720, "fps": 29.97, "fps_string": "30000/1001", "duration": 10.01,
            "audio": {"codec": "aac", "channels": 2, "sample_rate": 48000, "bit_rate": 192000},
        })

    def test_silent_clip_has_null_audio(self):
        meta = video_input.media_meta({"streams": [
            {"codec_type": "video", "width": 2, "height": 2, "avg_frame_rate": "30/1",
             "nb_frames": "60"}]})
        self.assertIsNone(meta["audio"])
        self.assertEqual(meta["duration"], 2.0)

    def test_row_carries_output_meta_through_persistence(self):
        task = RenderTask(prompt=RenderPrompt(prompt="upscale 2x", type="upscale_video_x2"))
        self.assertIsNone(task.public_dict()["output_meta"])
        task.output_meta = {"w": 2, "h": 2, "fps": 30.0, "duration": 1.0, "audio": None}
        again = RenderTask(**json.loads(task.model_dump_json()))
        self.assertEqual(again.public_dict()["output_meta"], task.output_meta)


class _QueueEnv:
    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [patch.object(config, "TMP_DIR", root / "tmp"),
                        patch.object(config, "RENDER_DIR", root / "render")]
        for item in self.patches:
            item.start()
        self.root = root
        return self

    def __exit__(self, *exc):
        for item in reversed(self.patches):
            item.stop()
        self.tmp.cleanup()


def _upscale_task():
    return RenderTask(prompt=RenderPrompt(prompt="upscale 2x", type="upscale_video_x2",
                                          control_video_url=CLIP_URL, frame_count=393,
                                          user_name="u"),
                      output_ext=".mp4")


class QueueHookTests(unittest.TestCase):
    def test_sweep_and_error_remove_orphans_only(self):
        async def scenario():
            with _QueueEnv():
                queue = RenderQueue(registry=None, client=None)
                live, done = _upscale_task(), _upscale_task()
                done.status = TASK_DONE
                live.status = TASK_PENDING
                queue._tasks = {live.id: live, done.id: done}
                for task in (live, done):
                    path = _upscale_source_path(task.id)
                    path.parent.mkdir(parents=True)
                    path.write_bytes(b"x")
                removed = queue._sweep_upscale_sources()
                kept = _upscale_source_path(live.id).is_file()
                gone = not _upscale_source_path(done.id).parent.exists()
                await queue._fail(live, "cancelled")
                after_fail = _upscale_source_path(live.id).parent.exists()
                return removed, kept, gone, after_fail

        removed, kept, gone, after_fail = asyncio.run(scenario())
        self.assertEqual(removed, 1)
        self.assertTrue(kept)
        self.assertTrue(gone)
        self.assertFalse(after_fail)

    def test_unsafe_task_id_never_names_a_directory(self):
        with self.assertRaises(ValueError):
            _upscale_source_path("../x")
        _drop_upscale_source("../x")  # no exception, nothing removed


def _ffmpeg(*args):
    done = subprocess.run(["ffmpeg", "-v", "error", "-y", *args], capture_output=True)
    if done.returncode:
        raise AssertionError(done.stderr.decode(errors="replace"))


def _audio_packets_md5(path):
    done = subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-map", "0:a:0",
                           "-c", "copy", "-f", "data", "-"], capture_output=True, check=True)
    return hashlib.md5(done.stdout).hexdigest(), len(done.stdout)


def _ffprobe(path):
    done = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                           "stream=codec_type,codec_name,channels,sample_rate,bit_rate,"
                           "r_frame_rate,avg_frame_rate,width,height",
                           "-of", "json", str(path)], capture_output=True, check=True)
    streams = json.loads(done.stdout)["streams"]
    return ({s["codec_type"]: s for s in streams})


@unittest.skipUnless(HAVE_FFMPEG, "ffmpeg unavailable")
class RealFfmpegRemuxTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.source = self.dir / "source.mp4"
        _ffmpeg("-f", "lavfi", "-i", "testsrc2=size=320x240:rate=30:duration=10",
                "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=10",
                "-filter_complex", "[1:a]aformat=channel_layouts=stereo[a]",
                "-map", "0:v", "-map", "[a]",
                "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2",
                str(self.source))

    def tearDown(self):
        self.tmp.cleanup()

    def render(self, *, with_audio):
        """What ComfyUI hands back: the clip 2x at 30 fps, mono re-encode or no sound."""
        rendered = self.dir / ("rendered_a.mp4" if with_audio else "rendered.mp4")
        audio = (["-map", "0:a", "-c:a", "aac", "-b:a", "69k", "-ac", "1"]
                 if with_audio else ["-an"])
        _ffmpeg("-i", str(self.source), "-map", "0:v", "-vf", "scale=640:480",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", *audio, str(rendered))
        return rendered

    def check_remux(self, rendered):
        output = self.dir / f"out_{rendered.stem}.mp4"
        meta = asyncio.run(video_input.remux_source_audio(rendered, self.source, output))
        src, out = _ffprobe(self.source), _ffprobe(output)
        self.assertEqual(out["video"]["avg_frame_rate"], "30/1")
        self.assertEqual((out["video"]["width"], out["video"]["height"]), (640, 480))
        for key in ("codec_name", "channels", "sample_rate", "bit_rate"):
            self.assertEqual(out["audio"][key], src["audio"][key], key)
        self.assertEqual(_audio_packets_md5(output), _audio_packets_md5(self.source))
        self.assertEqual(meta["fps"], 30.0)
        self.assertEqual((meta["w"], meta["h"]), (640, 480))
        self.assertAlmostEqual(meta["duration"], 10.0, delta=0.05)
        self.assertEqual(meta["audio"], {"codec": "aac", "channels": 2, "sample_rate": 48000,
                                         "bit_rate": int(src["audio"]["bit_rate"])})
        self.assertEqual(meta["audio_source"], "original")
        # faststart: moov before mdat
        data = output.read_bytes()
        self.assertNotEqual(data.find(b"moov"), -1)
        self.assertLess(data.find(b"moov"), data.find(b"mdat"))

    def test_rendered_without_audio_gets_the_original_track(self):
        self.check_remux(self.render(with_audio=False))

    def test_rendered_mono_reencode_is_replaced_by_the_original_track(self):
        self.check_remux(self.render(with_audio=True))

    def test_pcm_mov_source_is_refused(self):
        mov = self.dir / "source.mov"
        _ffmpeg("-i", str(self.source), "-map", "0:v", "-map", "0:a", "-c:v", "copy",
                "-c:a", "pcm_s16le", str(mov))
        with self.assertRaises(video_input.VideoInputError):
            asyncio.run(video_input.remux_source_audio(
                self.render(with_audio=False), mov, self.dir / "out_pcm.mp4"))

    def test_silent_source_gives_silent_output(self):
        silent = self.dir / "silent.mp4"
        _ffmpeg("-i", str(self.source), "-map", "0:v", "-c", "copy", str(silent))
        rendered = self.render(with_audio=True)
        output = self.dir / "out_silent.mp4"
        meta = asyncio.run(video_input.remux_source_audio(rendered, silent, output))
        self.assertNotIn("audio", _ffprobe(output))
        self.assertIsNone(meta["audio"])
        self.assertEqual(meta["audio_source"], "none")

    def test_prepare_keeps_30fps_and_full_length(self):
        async def scenario():
            async def fake_download(_client, _url, target):
                shutil.copyfile(self.source, target)
            kept = self.dir / "kept" / "source"
            with patch.object(config, "RENDER_DIR", self.dir / "render"), \
                    patch.object(video_input, "_download", fake_download):
                name, data = await video_input.download_prepare_video(
                    object(), CLIP_URL, 393, allow_shorter=True, keep_audio=True,
                    source_fps=True, keep_source_at=kept)
            prepared = self.dir / name
            prepared.write_bytes(data)
            return prepared, kept
        prepared, kept = asyncio.run(scenario())
        self.assertEqual(kept.read_bytes(), self.source.read_bytes())
        self.assertEqual(_ffprobe(prepared)["video"]["avg_frame_rate"], "30/1")
        frames = subprocess.run(["ffprobe", "-v", "error", "-count_frames", "-select_streams",
                                 "v:0", "-show_entries", "stream=nb_read_frames", "-of",
                                 "csv=p=0", str(prepared)], capture_output=True, check=True)
        self.assertEqual(int(frames.stdout.strip()), 300)


if __name__ == "__main__":
    unittest.main()

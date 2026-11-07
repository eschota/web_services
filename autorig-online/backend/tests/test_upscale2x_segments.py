"""Upscale 2x of a clip in segments: planning, splitting, joining, audio (2026-11-07)."""
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from renderfin import config, upscale_video, video_input
from renderfin.models import (
    TASK_DONE, TASK_ERROR, TASK_PENDING, TASK_RENDERING, RenderPrompt, RenderServer, RenderTask,
)
from renderfin.queue import RenderQueue

HAVE_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))
CLIP_URL = "https://pvs1.microstock.plus/clip.mp4"


def _probe(*, w=1920, h=1080, rate="30/1", duration="20.0", nb_frames="600",
           audio="aac", fmt="mov,mp4,m4a,3gp,3g2,mj2"):
    streams = [{"codec_type": "video", "codec_name": "h264", "width": w, "height": h,
                "r_frame_rate": rate, "avg_frame_rate": rate, "nb_frames": nb_frames,
                "duration": duration}]
    if audio:
        streams.append({"codec_type": "audio", "codec_name": audio, "channels": 2,
                        "sample_rate": "48000", "bit_rate": "192000"})
    return {"format": {"format_name": fmt, "duration": duration}, "streams": streams}


class _Env:
    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.patches = [patch.object(config, "TMP_DIR", self.root / "tmp"),
                        patch.object(config, "RENDER_DIR", self.root / "render")]
        for item in self.patches:
            item.start()
        return self

    def __exit__(self, *exc):
        for item in reversed(self.patches):
            item.stop()
        self.tmp.cleanup()


def _inspect(probe, size=1000):
    async def fake_probe(_path):
        return probe

    async def scenario():
        with _Env() as env:
            clip = env.root / "clip"
            clip.write_bytes(b"x" * size)
            with patch.object(video_input, "probe_media", fake_probe):
                return await upscale_video.inspect(clip)
    return asyncio.run(scenario())


class PlanningTests(unittest.TestCase):
    def test_pixel_budget_bounds_the_segment(self):
        with patch.dict(os.environ, {"RENDERFIN_UPSCALE_SEGMENT_MEGAPIXELS": ""}):
            self.assertEqual(upscale_video.frames_per_segment(3840, 2160), 30)   # 1080p source
            self.assertEqual(upscale_video.frames_per_segment(2560, 1440), 67)   # 720p
            self.assertEqual(upscale_video.frames_per_segment(1920, 1080), 120)  # 540p
            self.assertEqual(upscale_video.frames_per_segment(640, 480), 390)    # capped
        with patch.dict(os.environ, {"RENDERFIN_UPSCALE_SEGMENT_MEGAPIXELS": "500"}):
            self.assertEqual(upscale_video.frames_per_segment(3840, 2160), 60)
        self.assertEqual(upscale_video.frames_per_segment(4096, 4096, megapixels=1), 1)

    def test_segments_cover_every_frame_once(self):
        for frames, per in ((1800, 30), (751, 130), (1, 390), (390, 390), (391, 390)):
            bounds = upscale_video.segment_bounds(frames, per)
            self.assertEqual(bounds[0][0], 0)
            self.assertEqual(bounds[-1][1], frames)
            for (a, b), (c, _d) in zip(bounds, bounds[1:]):
                self.assertEqual(b, c)                 # no gap, no overlap
                self.assertEqual(b - a, per)
            self.assertEqual(sum(b - a for a, b in bounds), frames)

    def test_plan_of_an_admitted_clip(self):
        plan = _inspect(_probe(w=1921, h=1081))
        self.assertEqual((plan["width_int"], plan["height_int"]), (1920, 1080))  # even crop
        self.assertEqual((plan["out_width_int"], plan["out_height_int"]), (3840, 2160))
        self.assertEqual(plan["fps_string"], "30")
        self.assertEqual(plan["frames_per_segment_int"], 30)
        self.assertEqual(plan["segments_int"], 20)
        self.assertFalse(plan["ready_bool"])
        self.assertEqual(plan["audio"]["codec"], "aac")

    def test_refusals_carry_their_code(self):
        cases = [
            (dict(duration="61.0", nb_frames="1830"), "video_too_long"),
            (dict(rate="60/1", duration="40.0", nb_frames="2400"), "video_too_long"),
            (dict(w=2560, h=1440), "video_too_large"),
            (dict(audio="pcm_s16le"), "unsupported_codec"),
            (dict(audio="vorbis", fmt="matroska,webm"), "unsupported_codec"),
            (dict(fmt="png_pipe"), "unsupported_codec"),
            (dict(rate="0/0"), "unsupported_codec"),
        ]
        for kwargs, code in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(upscale_video.UpscaleError) as caught:
                    _inspect(_probe(**kwargs))
                self.assertEqual(caught.exception.code, code)
                self.assertTrue(str(caught.exception).startswith(code + ": "))

    def test_over_200_mb_is_too_large(self):
        with patch.object(upscale_video, "MAX_BYTES", 10):
            with self.assertRaises(upscale_video.UpscaleError) as caught:
                _inspect(_probe(), size=11)
        self.assertEqual(caught.exception.code, "video_too_large")

    def test_download_errors_are_coded(self):
        async def run(error):
            async def fake_download(_client, _url, _target, limit=0):
                self.assertEqual(limit, upscale_video.MAX_BYTES)
                raise video_input.VideoInputError(error)
            with _Env() as env, patch.object(video_input, "_download_once", fake_download):
                await upscale_video.admit(object(), CLIP_URL, env.root / "s")
        for error, code in (("control video exceeds the 100 MB limit", "video_too_large"),
                            ("control video download returned HTTP 404", "video_unavailable")):
            with self.assertRaises(upscale_video.UpscaleError) as caught:
                asyncio.run(run(error))
            self.assertEqual(caught.exception.code, code)
        with self.assertRaises(upscale_video.UpscaleError) as caught:
            asyncio.run(upscale_video.admit(object(), "https://evil.example/a.mp4", Path("x")))
        self.assertEqual(caught.exception.code, "video_unavailable")


class CommandTests(unittest.TestCase):
    PLAN = {"frames_per_segment_int": 30, "fps_string": "30000/1001",
            "width_int": 1920, "height_int": 1080}

    def test_split_cuts_on_exact_frames_at_the_source_rate(self):
        argv = upscale_video.split_command(Path("src"), Path("dir"), self.PLAN)
        line = " ".join(argv)
        self.assertIn("-vf fps=30000/1001,crop=1920:1080:0:0,format=yuv420p", line)
        self.assertIn("-force_key_frames expr:eq(mod(n,30),0)", line)
        points = argv[argv.index("-segment_frames") + 1].split(",")
        self.assertEqual(points[:3], ["30", "60", "90"])
        self.assertGreater(int(points[-1]), upscale_video.MAX_FRAMES)
        self.assertIn("-an", argv)
        self.assertIn("-reset_timestamps 1", line)
        self.assertTrue(argv[-1].endswith("in_%04d.mp4"))

    def test_concat_is_lossless(self):
        listing = upscale_video.concat_list([Path("/w/out_0000.mp4"), Path("/w/out_0001.mp4")])
        self.assertEqual(listing, "file 'out_0000.mp4'\nfile 'out_0001.mp4'\n")
        line = " ".join(upscale_video.concat_command(Path("l.txt"), Path("j.mp4")))
        self.assertIn("-f concat -safe 0 -i l.txt -map 0:v:0 -c copy", line)


class RowFieldTests(unittest.TestCase):
    def test_progress_fields(self):
        task = RenderTask(prompt=RenderPrompt(prompt="upscale 2x", type="upscale_video_x2"))
        task.status, task.segments_int, task.segments_done_int = TASK_RENDERING, 12, 3
        task.stage_string = "upscaling segment 4/12"
        row = task.public_dict()
        self.assertEqual(row["segments_int"], 12)
        self.assertEqual(row["segments_done_int"], 3)
        self.assertEqual(row["segment_index_int"], 4)
        self.assertEqual(row["progress_float"], 0.25)
        self.assertEqual(row["stage_string"], "upscaling segment 4/12")
        again = RenderTask(**json.loads(task.model_dump_json()))
        self.assertEqual(again.public_dict()["segment_index_int"], 4)
        task.status = TASK_DONE
        self.assertEqual(task.public_dict()["progress_float"], 1.0)
        self.assertEqual(task.public_dict()["segment_index_int"], 0)


def _task(segments=3):
    task = RenderTask(prompt=RenderPrompt(prompt="upscale 2x", type="upscale_video_x2",
                                          control_video_url=CLIP_URL, user_name="u"),
                      workflow_file="upscale_video_x2.json", output_ext=".mp4")
    task.upscale_plan = {"ready_bool": True, "segments_int": segments, "frames_int": 3 * segments,
                         "segment_frames_array": [3] * segments, "out_width_int": 4,
                         "out_height_int": 4, "fps_string": "30"}
    task.segments_int = segments
    task.status = TASK_RENDERING
    task.comfy_prompt_id = "p0"
    return task


class QueueFlowTests(unittest.TestCase):
    def server(self):
        return RenderServer(render_server_name="box", render_server_url="http://127.0.0.1:9")

    def test_a_middle_piece_goes_straight_to_the_next_submit(self):
        submitted = []

        async def fake_store(task_id, plan, index, data):
            self.assertEqual(data, b"piece")
            return Path("x")

        async def fake_submit(task, server):
            submitted.append((task.segments_done_int, task.comfy_prompt_id, task.status))
            task.status = TASK_RENDERING
            task.comfy_prompt_id = "p1"

        async def scenario():
            with _Env() as env:
                queue = RenderQueue(registry=None, client=object())
                task = _task()
                queue._tasks[task.id] = task
                out = env.root / "render" / "u" / f"{task.id}.mp4"
                with patch.object(upscale_video, "store_output", fake_store), \
                        patch.object(queue, "_submit_task", fake_submit):
                    done = await queue._finish_upscale_piece(task, self.server(), b"piece", out)
                return task, done, out.exists()

        task, done, published = asyncio.run(scenario())
        self.assertFalse(done)
        self.assertFalse(published)
        # Rendering throughout (the pump can never dispatch it twice), new prompt id
        self.assertEqual(submitted, [(1, "", TASK_RENDERING)])
        self.assertIn("p0", task.retired_comfy_prompt_ids)
        self.assertEqual(task.stage_string, "upscaling segment 2/3")

    def test_a_failed_next_submit_returns_the_task_to_the_queue(self):
        async def fake_store(*_a):
            return Path("x")

        async def broken_submit(task, server):
            raise RuntimeError("upload failed")

        async def scenario():
            with _Env() as env:
                queue = RenderQueue(registry=None, client=object())
                task = _task()
                with patch.object(upscale_video, "store_output", fake_store), \
                        patch.object(queue, "_submit_task", broken_submit):
                    await queue._finish_upscale_piece(task, self.server(), b"p", env.root / "o.mp4")
                return task

        task = asyncio.run(scenario())
        self.assertEqual(task.status, TASK_PENDING)
        self.assertEqual(task.segments_done_int, 1)

    def test_last_piece_publishes_the_assembled_clip(self):
        async def fake_store(*_a):
            return Path("x")

        async def fake_assemble(task_id, plan, output):
            output.write_bytes(b"final clip")
            return {"fps": 30.0, "audio_source": "original", "segments_int": 3}

        async def scenario():
            with _Env() as env:
                queue = RenderQueue(registry=None, client=object())
                task = _task()
                task.segments_done_int = 2
                upscale_video.work_dir(task.id).mkdir(parents=True)
                out = env.root / "render" / "u" / f"{task.id}.mp4"
                with patch.object(upscale_video, "store_output", fake_store), \
                        patch.object(upscale_video, "assemble", fake_assemble):
                    done = await queue._finish_upscale_piece(task, self.server(), b"p", out)
                return task, done, out.read_bytes(), upscale_video.work_dir(task.id).exists()

        task, done, data, work_left = asyncio.run(scenario())
        self.assertTrue(done)
        self.assertEqual(data, b"final clip")
        self.assertEqual(task.output_meta["audio_source"], "original")
        self.assertEqual(task.segments_done_int, 3)
        self.assertFalse(work_left)

    def test_failed_remux_fails_the_task_and_keeps_the_picture_off_the_output_url(self):
        async def fake_store(*_a):
            return Path("x")

        async def broken_assemble(task_id, plan, output):
            (upscale_video.work_dir(task_id) / "joined.mp4").write_bytes(b"joined")
            raise upscale_video.UpscaleError("audio_remux_failed", "ffmpeg said no")

        async def scenario():
            with _Env() as env:
                queue = RenderQueue(registry=None, client=None)
                task = _task(segments=1)
                upscale_video.work_dir(task.id).mkdir(parents=True)
                out = env.root / "render" / "u" / f"{task.id}.mp4"
                with patch.object(upscale_video, "store_output", fake_store), \
                        patch.object(upscale_video, "assemble", broken_assemble):
                    done = await queue._finish_upscale_piece(task, self.server(), b"p", out)
                kept = upscale_video.failed_root() / task.id / "joined.mp4"
                return task, done, out.exists(), kept.read_bytes() if kept.is_file() else None

        task, done, published, kept = asyncio.run(scenario())
        self.assertFalse(done)
        self.assertFalse(published)
        self.assertEqual(task.status, TASK_ERROR)
        self.assertTrue(task.error.startswith("audio_remux_failed: "), task.error)
        self.assertEqual(kept, b"joined")

    def test_a_piece_with_the_wrong_frame_count_fails_the_task(self):
        async def scenario():
            with _Env() as env:
                queue = RenderQueue(registry=None, client=None)
                task = _task()
                upscale_video.work_dir(task.id).mkdir(parents=True)

                async def counts(_path):
                    return (2, 4, 4)
                with patch.object(upscale_video, "packet_count", counts):
                    await queue._finish_upscale_piece(task, self.server(), b"p", env.root / "o.mp4")
                return task

        task = asyncio.run(scenario())
        self.assertEqual(task.status, TASK_ERROR)
        self.assertTrue(task.error.startswith("upscale_segment_failed: "), task.error)

    def test_dispatch_waits_for_the_split(self):
        task = _task()
        task.upscale_plan["ready_bool"] = False
        self.assertTrue(RenderQueue._upscale_waiting_for_split(task))
        task.upscale_plan["ready_bool"] = True
        self.assertFalse(RenderQueue._upscale_waiting_for_split(task))
        other = RenderTask(prompt=RenderPrompt(prompt="x", type="t_pose"))
        self.assertFalse(RenderQueue._upscale_waiting_for_split(other))


class SubmitRefusalTests(unittest.TestCase):
    def test_renderfin_answers_422_with_the_code(self):
        from renderfin import api

        async def refuse(_client, _url, _target):
            raise upscale_video.UpscaleError("video_too_long", "the clip is 75.0 s; the limit is 60 s")

        class _App:
            class state:
                render_queue = RenderQueue(registry=None, client=object())

        class _Request:
            app = _App

        async def scenario():
            with _Env():
                with patch.object(upscale_video, "admit", refuse):
                    await api._enqueue_upscale_video(
                        _Request, RenderPrompt(prompt="u", type="upscale_video_x2",
                                               control_video_url=CLIP_URL))

        with self.assertRaises(HTTPException) as caught:
            asyncio.run(scenario())
        self.assertEqual(caught.exception.status_code, 422)
        self.assertEqual(caught.exception.detail, {
            "error_string": "video_too_long",
            "message_string": "the clip is 75.0 s; the limit is 60 s"})

    def test_admitted_clip_is_attached_before_the_task_is_visible(self):
        from renderfin import api

        plan = {"segments_int": 4, "frames_estimate_int": 120, "fps_string": "30"}

        async def admit(_client, _url, target):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"clip")
            return dict(plan)

        queue = RenderQueue(registry=None, client=object())
        split_requests = []
        queue._ensure_upscale_split = split_requests.append

        class _App:
            class state:
                render_queue = queue

        class _Request:
            app = _App

        async def scenario():
            with _Env():
                with patch.object(upscale_video, "admit", admit):
                    answer = await api._enqueue_upscale_video(
                        _Request, RenderPrompt(prompt="u", type="upscale_video_x2",
                                               control_video_url=CLIP_URL))
                task = queue.get(answer["task_id"])
                return answer, task, upscale_video.source_path(task.id).read_bytes()

        answer, task, kept = asyncio.run(scenario())
        self.assertEqual(answer["segments_int"], 4)
        self.assertEqual(kept, b"clip")
        self.assertEqual(task.upscale_plan["segments_int"], 4)
        self.assertEqual(split_requests, [task])


class BackendPassThroughTests(unittest.TestCase):
    def test_upscale2x_returns_renderfins_coded_refusal(self):
        import httpx
        import ai_enhance_api

        seen = {}

        def handler(request):
            seen["timeout"] = request.extensions.get("timeout")
            return httpx.Response(422, json={"detail": {
                "error_string": "unsupported_codec", "message_string": "audio is pcm_s16le"}})

        real_client = httpx.AsyncClient

        def client_factory(*args, **kwargs):
            return real_client(transport=httpx.MockTransport(handler))

        body = ai_enhance_api.Upscale2xRequest(video_url=CLIP_URL)
        with patch.object(ai_enhance_api.httpx, "AsyncClient", client_factory):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(ai_enhance_api._uncached_upscale2x(body))
        self.assertEqual(caught.exception.status_code, 422)
        self.assertEqual(caught.exception.detail["error_string"], "unsupported_codec")
        self.assertEqual(seen["timeout"]["read"], ai_enhance_api.VIDEO_SUBMIT_TIMEOUT_SECONDS)


def _ffmpeg(*args):
    done = subprocess.run(["ffmpeg", "-v", "error", "-y", *args], capture_output=True)
    if done.returncode:
        raise AssertionError(done.stderr.decode(errors="replace"))


def _audio_md5(path):
    done = subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-map", "0:a:0", "-c", "copy",
                           "-f", "data", "-"], capture_output=True, check=True)
    return hashlib.md5(done.stdout).hexdigest()


def _stream(path, kind):
    done = subprocess.run(["ffprobe", "-v", "error", "-count_packets", "-show_entries",
                           "stream=codec_type,codec_name,channels,sample_rate,bit_rate,"
                           "avg_frame_rate,width,height,duration,nb_read_packets",
                           "-of", "json", str(path)], capture_output=True, check=True)
    return next(s for s in json.loads(done.stdout)["streams"] if s["codec_type"] == kind)


@unittest.skipUnless(HAVE_FFMPEG, "ffmpeg unavailable")
class RealSegmentedUpscaleTests(unittest.TestCase):
    """A 25 s 30 fps clip with sound, split, 'enlarged' piece by piece, joined."""

    def run_pipeline(self, rate="30", seconds=25):
        async def scenario():
            with _Env() as env, patch.dict(os.environ, {"RENDERFIN_UPSCALE_SEGMENT_MEGAPIXELS": "40"}):
                source = env.root / "source.mp4"
                _ffmpeg("-f", "lavfi", "-i", f"testsrc2=size=320x240:rate={rate}:duration={seconds}",
                        "-f", "lavfi", "-i", f"sine=frequency=440:sample_rate=48000:duration={seconds}",
                        "-filter_complex", "[1:a]aformat=channel_layouts=stereo[a]",
                        "-map", "0:v", "-map", "[a]", "-c:v", "libx264", "-pix_fmt", "yuv420p",
                        "-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2", str(source))
                task_id = "task-1"
                upscale_video.work_dir(task_id).mkdir(parents=True)
                shutil.copyfile(source, upscale_video.source_path(task_id))
                plan = await upscale_video.inspect(upscale_video.source_path(task_id))
                plan = await upscale_video.split(task_id, plan)
                for index in range(plan["segments_int"]):
                    rendered = env.root / f"rendered_{index}.mp4"
                    # As SaveVideo would: the piece 2x, H.264, the piece's own rate, mono sound
                    # nobody asked for (it must not survive).
                    _ffmpeg("-i", str(upscale_video.input_path(task_id, index)),
                            "-f", "lavfi", "-i", "anullsrc=r=22050:cl=mono", "-shortest",
                            "-map", "0:v", "-map", "1:a", "-vf", "scale=iw*2:ih*2",
                            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "69k",
                            str(rendered))
                    await upscale_video.store_output(task_id, plan, index, rendered.read_bytes())
                final = env.root / "final.mp4"
                meta = await upscale_video.assemble(task_id, plan, final)
                out_v, src_v = _stream(final, "video"), _stream(source, "video")
                out_a, src_a = _stream(final, "audio"), _stream(source, "audio")
                return plan, meta, out_v, src_v, out_a, src_a, _audio_md5(final), _audio_md5(source)
        return asyncio.run(scenario())

    def test_frames_fps_duration_and_audio_survive(self):
        plan, meta, out_v, src_v, out_a, src_a, out_md5, src_md5 = self.run_pipeline()
        self.assertEqual(plan["frames_per_segment_int"], 130)   # 40 MP / (640x480)
        self.assertEqual(plan["segments_int"], 6)
        self.assertEqual(plan["segment_frames_array"], [130, 130, 130, 130, 130, 100])
        self.assertEqual(out_v["avg_frame_rate"], src_v["avg_frame_rate"])
        self.assertEqual(int(out_v["nb_read_packets"]), int(src_v["nb_read_packets"]))
        self.assertEqual((out_v["width"], out_v["height"]), (640, 480))
        self.assertLessEqual(abs(float(out_v["duration"]) - float(src_v["duration"])), 1 / 30)
        for key in ("codec_name", "channels", "sample_rate", "bit_rate"):
            self.assertEqual(out_a[key], src_a[key], key)
        self.assertEqual(out_md5, src_md5)
        self.assertEqual(meta["segments_int"], 6)
        self.assertEqual(meta["frames_int"], 750)
        self.assertEqual(meta["fps"], 30.0)
        self.assertEqual(meta["audio_source"], "original")
        self.assertEqual(meta["audio"]["codec"], "aac")

    def test_ntsc_rate_keeps_every_frame(self):
        plan, meta, out_v, src_v, *_rest, out_md5, src_md5 = self.run_pipeline(rate="30000/1001", seconds=9)
        self.assertEqual(plan["fps_string"], "30000/1001")
        self.assertEqual(int(out_v["nb_read_packets"]), int(src_v["nb_read_packets"]))
        self.assertLessEqual(abs(float(out_v["duration"]) - float(src_v["duration"])), 1001 / 30000)

    def test_pcm_mov_is_refused_at_admission(self):
        async def scenario():
            with _Env() as env:
                mov = env.root / "pcm.mov"
                _ffmpeg("-f", "lavfi", "-i", "testsrc2=size=160x120:rate=30:duration=1",
                        "-f", "lavfi", "-i", "sine=sample_rate=48000:duration=1",
                        "-c:v", "libx264", "-c:a", "pcm_s16le", str(mov))
                await upscale_video.inspect(mov)
        with self.assertRaises(upscale_video.UpscaleError) as caught:
            asyncio.run(scenario())
        self.assertEqual(caught.exception.code, "unsupported_codec")


if __name__ == "__main__":
    unittest.main()

"""Media edit on the ai-node (2026-11-07): validation, the Freestock ffmpeg port, a fake ffmpeg (progress, NVENC
fallback, cancel), the real ffmpeg when present (tiny lavfi clips), the HTTP endpoints and the selftest.

    python -m unittest test_media_edit -v
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ai_node  # noqa: E402

REAL_FFMPEG, REAL_FFPROBE = ai_node.find_ffmpeg()
TOKEN = "media-test-token-0123456789"


def spec(**payload):
    body = {"source_url": "https://cdn.example.com/a.mp4", "media_kind": "video"}
    body.update(payload)
    return ai_node.parse_media_edit_request(body)


VIDEO_INFO = {"codec": "h264", "codec_tag": "avc1", "pix_fmt": "yuv420p", "width": 1920, "height": 1080,
              "fps": 30.0, "duration": 10.0, "bit_rate": "8000000", "rotation_deg": 0.0, "has_video": True,
              "has_audio": True, "audio_codec": "aac", "format_name": "mov,mp4"}


# ------------------------------------------------------------------- validation
class ValidationTests(unittest.TestCase):
    def test_contract(self):
        s = spec(operation={"smartcrop": {"target_aspect": 0.5625, "anchors": [{"t": 3, "x": 0.8, "y": 0.5},
                                                                               {"t": 0, "x": 0.2, "y": 0.5}]},
                            "speed": {"factor": 2, "interpolate": True}},
                 output={"container": "mp4", "keep_audio": True}, client_ref="mplus:1:2:vertical", tier="mplus")
        self.assertEqual(s["smartcrop"]["anchors"][0][0], 0)
        self.assertEqual((s["speed"], s["interpolate"], s["keep_audio"], s["tier"]), (2.0, True, True, "mplus"))
        self.assertFalse(spec(operation={"cut_frame_at_seconds": 1}, output={"keep_audio": True})["keep_audio"])

    def test_refusals(self):
        for payload in ({"media_kind": "x"}, {"operation": {"speed": {"factor": 0}}},
                        {"media_kind": "image", "operation": {"cut_frame_at_seconds": 1}},
                        {"operation": {"crop": {"w": 1.5, "h": 2}}}, {"output": {"codec": "hevc"}},
                        {"operation": {"smartcrop": {"units": "pt"}}}, {"operation": {"rotate_rad": "x"}}):
            with self.assertRaises(ai_node.RequestError, msg=payload):
                spec(**payload)

    def test_source_url_guard(self):
        for url in ("http://cdn.example.com/a.mp4", "https://127.0.0.1/a.mp4", "https://localhost/a",
                    "https://example.com:8443/a"):
            with self.assertRaises(ai_node.RequestError, msg=url):
                ai_node.validate_media_source_url(url)
        ai_node.validate_media_source_url("http://127.0.0.1:9/a.mp4", allow_private=True)


# ---------------------------------------------------------------------- filters
class FilterTests(unittest.TestCase):
    def test_flips_and_quarter_turns_are_exact(self):
        self.assertEqual(ai_node.media_filters(spec(media_kind="image", operation={"hflip": True, "vflip": True}),
                                               VIDEO_INFO), ["hflip", "vflip"])
        self.assertEqual(ai_node._rotation_filters(1.5708), ["transpose=clock"])
        self.assertEqual(ai_node._rotation_filters(-1.5708), ["transpose=cclock"])
        self.assertEqual(ai_node._rotation_filters(4.712389), ["transpose=cclock"])
        self.assertEqual(ai_node._rotation_filters(3.14159265), ["hflip", "vflip"])
        self.assertEqual(ai_node._rotation_filters(0.0), [])
        self.assertEqual(ai_node._rotation_filters(0.3), ["rotate=0.3:ow=rotw(0.3):oh=roth(0.3)"])

    def test_stored_rotation_is_freestocks_rule(self):
        phone = dict(VIDEO_INFO, rotation_deg=-90.0)
        self.assertEqual(ai_node.effective_rotation(spec(operation={"rotate_rad": 1.5707963}), phone), 1.5707963)
        stored = spec(operation={"rotate_rad": 1.5707963, "rotation_relative_to": "stored"})
        self.assertAlmostEqual(abs(ai_node.effective_rotation(stored, phone)), 3.14159265, places=5)
        self.assertEqual(ai_node.display_size(phone), (1080, 1920))

    def test_crop_scale_order_and_even_sizes(self):
        filters = ai_node.media_filters(spec(media_kind="image", operation={
            "crop": {"w": 1531, "h": 2721, "x": 10, "y": 0}, "scale": {"w": 1081, "h": 1921}, "hflip": True}),
            dict(VIDEO_INFO, width=4000, height=3000))
        self.assertEqual(filters, ["crop=1530:2720:10:0", "scale=1080:1920", "hflip"])
        with self.assertRaises(ai_node.NodeError):
            ai_node.media_filters(spec(media_kind="image", operation={"crop": {"w": 3000, "h": 10, "x": 0}}),
                                  VIDEO_INFO)

    def test_speed_and_minterpolate_rules(self):
        s = spec(operation={"speed": {"factor": 2.0, "interpolate": True}})
        filters = ai_node.media_filters(s, VIDEO_INFO, source_bytes=10 ** 6)
        self.assertEqual(filters[0], "setpts=0.500000*PTS")
        self.assertEqual(filters[1], "minterpolate='mi_mode=mci:mc_mode=aobmc:vsbmc=1:fps=60'")
        self.assertEqual(ai_node.output_frame_rate(s, VIDEO_INFO), 60.0)
        self.assertEqual(len(ai_node.media_filters(s, VIDEO_INFO, source_bytes=600 * 1024 ** 2)), 1)   # > 512 MB
        self.assertEqual(len(ai_node.media_filters(s, dict(VIDEO_INFO, fps=60.0))), 1)                 # > 30 fps
        self.assertEqual(ai_node.output_frame_rate(s, dict(VIDEO_INFO, fps=25.0)), 0.0)
        self.assertEqual(ai_node.audio_filters(4.0), ["atempo=2.0", "atempo=2.000000"])
        self.assertEqual(ai_node.audio_filters(0.25), ["atempo=0.5", "atempo=0.500000"])

    def test_smartcrop_expression(self):
        smart = spec(operation={"smartcrop": {"target_aspect": 0.5625, "anchors": [
            {"t": 0, "x": 0.25, "y": 0.5}, {"t": 10, "x": 0.75, "y": 0.5}]}})["smartcrop"]
        expr = ai_node.smartcrop_filter(smart, 1920, 1080)
        # 1080 * 9/16 = 607.5 -> 608 wide (Freestock rounding); x from 480-303.75 to 1440-303.75 = 176.25 -> 1136.25.
        self.assertTrue(expr.startswith("crop=608:1080:if(lt(t\\,10)\\,176.25+960*("), expr)
        self.assertTrue(expr.endswith(":0"))
        still = ai_node.smartcrop_filter(smart, 1920, 1080, at=5.0)
        self.assertEqual(still, "crop=608:1080:656.25:0")          # halfway: smoothstep(0.5) = 0.5
        centred = ai_node.smartcrop_filter({"target_aspect": 0.5625, "anchors": []}, 1920, 1080)
        self.assertEqual(centred, "crop=608:1080:656.25:0")
        clamped = ai_node.smartcrop_filter({"target_aspect": 0.5625, "units": "norm",
                                            "anchors": [(0, 1.0, 0.5)]}, 1920, 1080)
        self.assertEqual(clamped, "crop=608:1080:1312:0")        # never past the right edge
        three = ai_node.smartcrop_filter({"target_aspect": 0.5625, "units": "px",
                                          "anchors": [(0, 500, 0), (4, 900, 0), (8, 900, 0)]}, 1920, 1080)
        self.assertEqual(three.count("if(lt(t"), 2)


# --------------------------------------------------------------------- encoders
class EncoderTests(unittest.TestCase):
    def test_h264_plan_with_nvenc_and_fallback(self):
        s = spec(operation={"speed": {"factor": 2, "interpolate": False}}, output={"container": "mp4"})
        command, fallback, label = ai_node.build_media_commands("ffmpeg", s, VIDEO_INFO, "src.mp4", "out.mp4")
        self.assertEqual(label, "h264_nvenc")
        joined = " ".join(command)
        self.assertIn("-c:v h264_nvenc -preset:v p7 -tune:v hq -rc:v cbr -b:v 8000000", joined)
        self.assertIn("-r 60", joined)
        self.assertIn("-an", command)
        self.assertIn("-movflags +faststart", joined)
        self.assertIn("-c:v libx264 -preset:v slow -tune:v film -b:v 8000000", " ".join(fallback))
        self.assertNotIn("h264_nvenc", fallback)

    def test_prores_source_stays_prores(self):
        info = dict(VIDEO_INFO, codec="prores", codec_tag="apch")
        s = spec(operation={"hflip": True})
        self.assertEqual(ai_node.resolve_container(s, info, "src.bin"), "mov")
        command, fallback, label = ai_node.build_media_commands("ffmpeg", s, info, "src.mov", "out.mov")
        self.assertEqual((label, fallback), ("prores_ks", None))
        self.assertIn("-c:v prores_ks -qscale:v 4 -pix_fmt yuv422p10le -profile:v 3", " ".join(command))
        info4444 = dict(info, codec_tag="ap4h")
        self.assertIn("yuv444p10le", ai_node.prores_args(info4444))

    def test_audio_kept_with_atempo(self):
        s = spec(operation={"speed": {"factor": 2}}, output={"keep_audio": True})
        command, _, _ = ai_node.build_media_commands("ffmpeg", s, VIDEO_INFO, "s.mp4", "o.mp4")
        self.assertIn("0:a:0?", command)
        self.assertIn("atempo=2.000000", command)
        same = spec(operation={"hflip": True}, output={"keep_audio": True})
        command, _, _ = ai_node.build_media_commands("ffmpeg", same, VIDEO_INFO, "s.mp4", "o.mp4")
        self.assertIn("-c:a copy", " ".join(command))

    def test_cut_frame_and_image_commands(self):
        s = spec(operation={"cut_frame_at_seconds": 12.5})
        self.assertEqual(ai_node.resolve_container(s, VIDEO_INFO, "s.mp4"), "jpg")
        command, fallback, label = ai_node.build_media_commands("ffmpeg", s, VIDEO_INFO, "s.mp4", "o.jpg")
        joined = " ".join(command)
        self.assertLess(command.index("-ss"), command.index("-i"))
        self.assertIn("-c:v mjpeg -f image2 -fps_mode passthrough -qscale:v 2", joined)
        self.assertIn("-frames:v 1", joined)
        image = spec(media_kind="image", operation={"rotate_rad": 1.5708})
        self.assertEqual(ai_node.resolve_container(image, {"codec": "png"}, "s.png"), "png")
        command, _, label = ai_node.build_media_commands("ffmpeg", image, {"width": 10, "height": 20}, "s.png",
                                                         "o.png")
        self.assertEqual(label, "image")
        self.assertIn("transpose=clock", command)

    def test_banner_probe(self):
        banner = """Input #0, mov,mp4,m4a,3gp,3g2,mj2, from 'x.mov':
  Duration: 00:00:12.50, start: 0.000000, bitrate: 150000 kb/s
  Stream #0:0[0x1](und): Video: prores (HQ) (apch / 0x68637061), yuv422p10le(tv, bt709, progressive), 1920x1080, 147000 kb/s, SAR 1:1 DAR 16:9, 29.97 fps, 29.97 tbr, 30k tbn (default)
      Side data:
        displaymatrix: rotation of -90.00 degrees
  Stream #0:1[0x2](und): Audio: pcm_s16le (sowt / 0x74776F73), 48000 Hz, stereo, s16, 1536 kb/s (default)
"""
        info = ai_node._probe_from_banner(banner)
        self.assertEqual((info["codec"], info["codec_tag"], info["width"], info["height"]),
                         ("prores", "apch", 1920, 1080))
        self.assertEqual((info["fps"], info["duration"], info["bit_rate"]), (29.97, 12.5, "147000000"))
        self.assertEqual((info["rotation_deg"], info["has_audio"], info["audio_codec"]), (-90.0, True, "pcm_s16le"))


# ----------------------------------------------------------------- fake ffmpeg
FAKE_FFMPEG = r'''
import os, sys, time
args = sys.argv[1:]
mode = os.environ.get("FAKE_FFMPEG_MODE", "ok")
if "-i" in args and args[-1] == args[args.index("-i") + 1]:
    sys.stderr.write("Input #0, mov,mp4, from 'x':\n  Duration: 00:00:04.00, start: 0.0, bitrate: 900 kb/s\n"
                     "  Stream #0:0: Video: h264 (High) (avc1 / 0x31637661), yuv420p, 640x360, 800 kb/s, 30 fps\n")
    sys.exit(1)
out = args[-1]
if mode == "nvenc_refused" and "h264_nvenc" in args:
    sys.stderr.write("[h264_nvenc @ 0x1] OpenEncodeSessionEx failed: incompatible client key (21)\n")
    sys.exit(1)
if mode == "fail":
    sys.stderr.write("Error: something broke\n")
    sys.exit(1)
for step in range(1, 5):
    sys.stdout.write("frame=%d\nout_time_us=%d\nprogress=continue\n" % (step * 15, step * 500000))
    sys.stdout.flush()
    time.sleep(float(os.environ.get("FAKE_FFMPEG_STEP", "0.05")))
with open(out, "wb") as handle:
    handle.write(b"fake-output " + " ".join(args).encode())
sys.stdout.write("progress=end\n")
'''


def make_fake_ffmpeg(folder: str) -> str:
    script = os.path.join(folder, "fake_ffmpeg.py")
    with open(script, "w", encoding="utf-8") as handle:
        handle.write(FAKE_FFMPEG)
    if os.name == "nt":
        wrapper = os.path.join(folder, "ffmpeg.cmd")
        with open(wrapper, "w", encoding="utf-8") as handle:
            handle.write(f'@"{sys.executable}" "{script}" %*\r\n')
    else:
        wrapper = os.path.join(folder, "ffmpeg")
        with open(wrapper, "w", encoding="utf-8") as handle:
            handle.write(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
        os.chmod(wrapper, 0o755)
    return wrapper


class FakeFfmpegTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ffmpeg = make_fake_ffmpeg(self.tmp)
        self.source = os.path.join(self.tmp, "source.mp4")
        with open(self.source, "wb") as handle:
            handle.write(b"x" * 1000)
        os.environ.pop("FAKE_FFMPEG_MODE", None)

    def tearDown(self):
        os.environ.pop("FAKE_FFMPEG_MODE", None)
        os.environ.pop("FAKE_FFMPEG_STEP", None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def task(self, **payload):
        task = ai_node.MediaTask(spec(**payload))
        task.work_dir = os.path.join(self.tmp, "work-" + task.task_id[:6])
        os.makedirs(task.work_dir)
        task.started_at = time.time()
        return task

    def execute(self, task):
        ai_node.execute_media_task(task, ffmpeg=self.ffmpeg, ffprobe="", max_source_bytes=10 ** 9,
                                   timeout_seconds=60, allow_private=True, use_nvenc=True,
                                   local_source=self.source)

    def test_progress_and_output(self):
        task = self.task(operation={"hflip": True})
        seen = []
        original = ai_node.run_ffmpeg

        def spy(command, **kw):
            update = kw["on_progress"]

            def record(fraction):
                seen.append(fraction)
                update(fraction)
            kw["on_progress"] = record
            return original(command, **kw)

        with unittest.mock.patch.object(ai_node, "run_ffmpeg", side_effect=spy):
            self.execute(task)
        self.assertEqual(seen, [0.125, 0.25, 0.375, 0.5])           # out_time / 4 s duration
        self.assertTrue(task.output_path.endswith("result.mp4"))
        self.assertEqual(task.encoder, "h264_nvenc")
        self.assertGreater(task.progress, 50)

    def test_nvenc_refusal_falls_back_to_libx264(self):
        os.environ["FAKE_FFMPEG_MODE"] = "nvenc_refused"
        task = self.task(operation={"vflip": True})
        self.execute(task)
        self.assertEqual(task.encoder, "libx264 (nvenc fallback)")
        with open(task.output_path, "rb") as handle:
            self.assertIn(b"libx264", handle.read())

    def test_other_failure_is_reported(self):
        os.environ["FAKE_FFMPEG_MODE"] = "fail"
        with self.assertRaises(ai_node.NodeError) as ctx:
            self.execute(self.task(operation={"vflip": True}))
        self.assertIn("something broke", str(ctx.exception))

    def test_cancel_kills_ffmpeg(self):
        os.environ["FAKE_FFMPEG_STEP"] = "2"
        task = self.task(operation={"hflip": True})
        threading.Timer(0.5, task.cancel.set).start()
        started = time.time()
        with self.assertRaises(ai_node.MediaCancelled):
            self.execute(task)
        self.assertLess(time.time() - started, 6)


# ----------------------------------------------------------------- real ffmpeg
@unittest.skipUnless(REAL_FFMPEG, "ffmpeg not installed")
class RealFfmpegTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.clip = os.path.join(cls.tmp, "clip.mp4")
        subprocess.run([REAL_FFMPEG, "-hide_banner", "-y", "-f", "lavfi", "-i",
                        "testsrc2=size=1280x720:rate=30:duration=3", "-f", "lavfi", "-i",
                        "sine=frequency=440:duration=3", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-b:v", "2M",
                        "-c:a", "aac", "-shortest", cls.clip], check=True, capture_output=True)
        cls.picture = os.path.join(cls.tmp, "pic.png")
        subprocess.run([REAL_FFMPEG, "-hide_banner", "-y", "-f", "lavfi", "-i", "testsrc2=size=640x480",
                        "-frames:v", "1", cls.picture], check=True, capture_output=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def run_task(self, source, **payload):
        body = {"source_url": "https://x.invalid/a", "media_kind": "video"}
        body.update(payload)
        task = ai_node.MediaTask(ai_node.parse_media_edit_request(body))
        task.work_dir = tempfile.mkdtemp(dir=self.tmp)
        task.started_at = time.time()
        ai_node.execute_media_task(task, ffmpeg=REAL_FFMPEG, ffprobe=REAL_FFPROBE, max_source_bytes=10 ** 9,
                                   timeout_seconds=300, allow_private=True, use_nvenc=True, local_source=source)
        return task

    def test_image_hflip_and_rotate90(self):
        flip = self.run_task(self.picture, media_kind="image", operation={"hflip": True})
        self.assertEqual((flip.output_meta["w"], flip.output_meta["h"], flip.output_meta["container"]),
                         (640, 480, "png"))
        turn = self.run_task(self.picture, media_kind="image", operation={"rotate_rad": 1.5708},
                             output={"container": "jpg"})
        self.assertEqual((turn.output_meta["w"], turn.output_meta["h"], turn.output_meta["container"]),
                         (480, 640, "jpg"))

    def test_vertical_smartcrop_two_anchors(self):
        task = self.run_task(self.clip, operation={"smartcrop": {
            "mode": "anchor_camera", "target_aspect": 0.5625,
            "anchors": [{"t": 0, "x": 0.2, "y": 0.5}, {"t": 3, "x": 0.8, "y": 0.5}]}}, output={"container": "mp4"})
        meta = task.output_meta
        self.assertEqual((meta["w"], meta["h"]), (404, 720))
        self.assertAlmostEqual(meta["duration"], 3.0, delta=0.2)
        self.assertFalse(meta["audio"])                               # dropped by default

    def test_speed_x2_and_keep_audio(self):
        task = self.run_task(self.clip, operation={"speed": {"factor": 2.0, "interpolate": False}},
                             output={"keep_audio": True})
        meta = task.output_meta
        self.assertAlmostEqual(meta["duration"], 1.5, delta=0.2)
        self.assertAlmostEqual(meta["fps"], 60.0, delta=0.5)
        self.assertTrue(meta["audio"])

    def test_cut_frame(self):
        task = self.run_task(self.clip, operation={"cut_frame_at_seconds": 1.5})
        self.assertEqual((task.output_meta["w"], task.output_meta["h"], task.output_meta["container"]),
                         (1280, 720, "jpg"))
        with self.assertRaises(ai_node.NodeError):
            self.run_task(self.clip, operation={"cut_frame_at_seconds": 30})

    def test_selftest(self):
        result = ai_node.run_media_selftest(REAL_FFMPEG, REAL_FFPROBE, self.tmp)
        self.assertTrue(result["ok"], result)
        self.assertIn(result["encoder"], ("h264_nvenc", "libx264 (nvenc fallback)"))
        broken = ai_node.run_media_selftest("", "", self.tmp)
        self.assertFalse(broken["ok"])


# ------------------------------------------------------------------- HTTP node
class _Quiet(SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@unittest.skipUnless(REAL_FFMPEG, "ffmpeg not installed")
class NodeHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        site = os.path.join(cls.tmp, "site")
        os.makedirs(site)
        subprocess.run([REAL_FFMPEG, "-hide_banner", "-y", "-f", "lavfi", "-i",
                        "testsrc2=size=640x360:rate=30:duration=2", "-c:v", "libx264", "-pix_fmt", "yuv420p",
                        os.path.join(site, "clip.mp4")], check=True, capture_output=True)
        handler = lambda *a, **k: _Quiet(*a, directory=site, **k)  # noqa: E731
        cls.files = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=cls.files.serve_forever, daemon=True).start()
        with open(os.path.join(cls.tmp, "token.txt"), "w") as handle:
            handle.write(TOKEN)
        cfg = ai_node.Config({"model_id": "qwen35-9b-uncensored", "llama_server": "llama-server",
                              "comfy_url": "", "token_file": "token.txt", "log_file": "",
                              "media_edit_allow_private_urls": True, "media_edit_selftest_on_start": False,
                              "media_edit_parallel": 2}, cls.tmp)
        cls.node = ai_node.Node(cfg)
        cls.node.media.start()
        cls.server = ai_node.AiNodeHTTPServer(("127.0.0.1", 0), ai_node.Handler, cls.node)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}{ai_node.API_PREFIX}"
        cls.clip_url = f"http://127.0.0.1:{cls.files.server_address[1]}/clip.mp4"

    @classmethod
    def tearDownClass(cls):
        cls.node.media.shutdown()
        cls.server.shutdown()
        cls.files.shutdown()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def call(self, method, path, body=None, token=TOKEN):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(self.base + path, data=data, method=method,
                                         headers={"Authorization": f"Bearer {token}",
                                                  "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read()
                return response.status, (json.loads(raw) if response.headers.get_content_type()
                                         == "application/json" else raw)
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    def test_1_not_capable_before_the_selftest(self):
        code, body = self.call("POST", "/media-edit", {"source_url": self.clip_url, "media_kind": "video"})
        self.assertEqual((code, body["error"]), (503, "media_edit_unavailable"))
        code, status = self.call("GET", "/server-status")
        self.assertNotIn("media_edit", status["capabilities"])
        self.assertEqual(self.call("GET", "/media-edit-selftest", token="wrong")[0], 401)

    def test_2_selftest_then_job_end_to_end(self):
        code, result = self.call("GET", "/media-edit-selftest?run=1")
        self.assertEqual(code, 200)
        self.assertTrue(result["ok"], result)
        code, status = self.call("GET", "/server-status")
        self.assertIn("media_edit", status["capabilities"])
        self.assertEqual(status["media_edit"]["free_slots"], 2)
        self.assertEqual(status["tasks_summary"]["processing"], 0)
        code, accepted = self.call("POST", "/media-edit", {
            "source_url": self.clip_url, "media_kind": "video", "tier": "mplus",
            "operation": {"vflip": True, "speed": {"factor": 2.0, "interpolate": False}},
            "output": {"container": "mp4"}})
        self.assertEqual(code, 202, accepted)
        task_id = accepted["task_id"]
        deadline = time.time() + 120
        while time.time() < deadline:
            code, state = self.call("GET", f"/media-edit/status/{task_id}")
            if state["status"] not in ("Pending", "Processing"):
                break
            time.sleep(0.3)
        self.assertEqual(state["status"], "Completed", state)
        self.assertTrue(state["output_ready"])
        self.assertAlmostEqual(state["output_meta"]["duration"], 1.0, delta=0.2)
        code, data = self.call("GET", f"/media-edit/output/{task_id}")
        self.assertEqual(code, 200)
        self.assertEqual(len(data), state["output_meta"]["bytes"])
        self.assertEqual(sorted(os.listdir(os.path.join(self.node.cfg.media_edit_work_dir, task_id))),
                         ["result.mp4"])                                # the source is gone already
        self.assertEqual(self.call("POST", f"/media-edit/release/{task_id}", {})[0], 200)
        self.assertFalse(os.path.exists(os.path.join(self.node.cfg.media_edit_work_dir, task_id)))
        self.assertEqual(self.call("GET", f"/media-edit/output/{task_id}")[0], 404)

    def test_3_bad_request_and_cancel(self):
        code, body = self.call("POST", "/media-edit", {"source_url": self.clip_url, "media_kind": "image",
                                                       "operation": {"speed": {"factor": 2}}})
        self.assertEqual(code, 400)
        if not self.node.media.capable():
            self.node.media.selftest = {"ok": True}
        code, accepted = self.call("POST", "/media-edit", {"source_url": self.clip_url, "media_kind": "video",
                                                           "operation": {"speed": {"factor": 0.1,
                                                                                   "interpolate": True}}})
        self.assertEqual(code, 202)
        time.sleep(0.5)
        code, state = self.call("POST", f"/media-edit/cancel/{accepted['task_id']}", {})
        self.assertEqual(code, 200)
        deadline = time.time() + 30
        while time.time() < deadline:
            state = self.call("GET", f"/media-edit/status/{accepted['task_id']}")[1]
            if state["status"] == "Cancelled":
                break
            time.sleep(0.2)
        self.assertEqual(state["status"], "Cancelled")
        self.assertEqual(self.call("GET", "/media-edit/status/nope")[0], 404)


if __name__ == "__main__":
    import unittest.mock  # noqa: F401
    unittest.main()

"""Video prefix guide for MiniMax H3 (2026-10-13).

A request may name a clip the new video continues: renderfin cuts that clip's
last 17k+5 frames with their sound and anchors them at frame 0 of the H3 latent
(MiniMaxH3AddGuide clip guide). Pinned here: without the field every H3 render
is unchanged (the optional prefix branch is pruned from the one template the
farm advertises), with it the guide is wired to the clip's frames and sound,
the frame count snaps to the guide's grid, the cut keeps the LAST frames, and
the public API accepts the prefix for H3 only.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from renderfin import config, video_input  # noqa: E402
from renderfin.config import WORKFLOWS_DIR  # noqa: E402
from renderfin.models import RenderPrompt  # noqa: E402
from renderfin.templating import OPTIONAL_PREFIX_GUIDE, render_workflow_text  # noqa: E402

TEMPLATE = "gen_video_minimax_h3_by_url.json"
PLAIN_NODES = ("model", "turbo_lora", "clip", "video_vae", "audio_vae", "start_frame", "start_scale", "size",
               "i2v", "noise", "sampler", "scheduler", "guider", "sample", "decode_video", "decode_audio",
               "create_video", "save")
END_NODES = ("end_frame", "end_scale", "end_guide")
PREFIX_NODES = ("prefix_video", "prefix_parts", "prefix_scale", "prefix_guide")


def _render(image_end="", prefix=""):
    text = (WORKFLOWS_DIR / TEMPLATE).read_text(encoding="utf-8")
    return render_workflow_text(text, width=960, height=540, prompt="the scene goes on", negative_prompt="",
                                image_filename="in.png", image_end_filename=image_end,
                                video_prefix_filename=prefix, output_prefix="task-1", frames=141,
                                randomize_seeds=False)


def _order(workflow, *groups):
    wanted = [n for n in ("model", "turbo_lora", "clip", "video_vae", "audio_vae", "start_frame", "start_scale",
                          "size", "i2v", *END_NODES, *PREFIX_NODES, "noise", "sampler", "scheduler", "guider",
                          "sample", "decode_video", "decode_audio", "create_video", "save")
              if any(n in g for g in groups)]
    return list(workflow.keys()) == wanted


class PrefixTemplateTests(unittest.TestCase):
    def test_without_a_prefix_the_branch_is_pruned_and_the_rest_is_as_before(self):
        plain = _render()
        self.assertTrue(_order(plain, PLAIN_NODES), list(plain))
        self.assertEqual(plain["guider"]["inputs"]["conditioning"], ["i2v", 0])
        with_end = _render(image_end="last.png")
        self.assertTrue(_order(with_end, PLAIN_NODES, END_NODES), list(with_end))
        self.assertEqual(with_end["guider"]["inputs"]["conditioning"], ["end_guide", 0])
        self.assertEqual(with_end["end_guide"]["inputs"]["positive"], ["i2v", 0])
        for wf in (plain, with_end):
            self.assertFalse([n for n in wf.values() if n.get("class_type") in ("LoadVideo", "GetVideoComponents")])
            self.assertFalse([n for n in wf.values() if (n.get("_meta") or {}).get("title") == OPTIONAL_PREFIX_GUIDE])
            self.assertFalse([n for n in wf.values() if "prefix" in (n.get("_meta") or {}).get("title", "")])
            self.assertNotIn("$video_prefix", repr(wf))

    def test_with_a_prefix_the_guide_takes_the_clips_frames_and_sound(self):
        wf = _render(image_end="last.png", prefix="prefix-abc.mp4")
        self.assertTrue(_order(wf, PLAIN_NODES, END_NODES, PREFIX_NODES), list(wf))
        self.assertEqual(wf["prefix_video"]["class_type"], "LoadVideo")
        self.assertEqual(wf["prefix_video"]["inputs"]["file"], "prefix-abc.mp4")
        self.assertEqual(wf["prefix_parts"]["inputs"]["video"], ["prefix_video", 0])
        self.assertEqual(wf["prefix_scale"]["inputs"]["image"], ["prefix_parts", 0])
        guide = wf["prefix_guide"]["inputs"]
        self.assertEqual(guide["frame_idx"], 0)
        self.assertEqual(guide["image"], ["prefix_scale", 0])
        self.assertEqual(guide["audio"], ["prefix_parts", 1])
        self.assertEqual(guide["audio_vae"], ["audio_vae", 0])
        self.assertEqual(guide["vae"], ["video_vae", 0])
        self.assertEqual(guide["latent"], ["i2v", 1])
        self.assertEqual(guide["positive"], ["end_guide", 0])
        self.assertEqual(wf["guider"]["inputs"]["conditioning"], ["prefix_guide", 0])
        self.assertEqual(wf["end_guide"]["inputs"]["frame_idx"], -1)

    def test_a_prefix_without_an_end_frame_hangs_off_frame_zero(self):
        wf = _render(prefix="prefix-abc.mp4")
        self.assertTrue(_order(wf, PLAIN_NODES, PREFIX_NODES), list(wf))
        self.assertEqual(wf["prefix_guide"]["inputs"]["positive"], ["i2v", 0])
        self.assertEqual(wf["guider"]["inputs"]["conditioning"], ["prefix_guide", 0])
        self.assertFalse([n for n in wf.values() if "end frame" in (n.get("_meta") or {}).get("title", "")])


class PrefixRequestModelTests(unittest.TestCase):
    def test_defaults_and_clamp(self):
        self.assertEqual(RenderPrompt().video_prefix_url, "")
        self.assertEqual(RenderPrompt().video_prefix_frames, 22)
        self.assertEqual(RenderPrompt(video_prefix_frames=0).video_prefix_frames, 22)
        self.assertEqual(RenderPrompt(video_prefix_frames=3).video_prefix_frames, 5)
        self.assertEqual(RenderPrompt(video_prefix_frames=500).video_prefix_frames, 124)
        self.assertEqual(RenderPrompt(video_prefix_frames=39).video_prefix_frames, 39)

    def test_the_fields_survive_the_api_filter_and_persistence(self):
        body = {"prompt": "x", "video_prefix_url": "https://autorig.online/dev/api/scratch/a.mp4",
                "video_prefix_frames": 39, "unknown": 1}
        prompt = RenderPrompt(**{k: v for k, v in body.items() if k in RenderPrompt.model_fields})
        again = RenderPrompt.model_validate_json(prompt.model_dump_json())
        self.assertEqual((again.video_prefix_url, again.video_prefix_frames), (body["video_prefix_url"], 39))


class PrefixCutTests(unittest.TestCase):
    def test_frame_count_snaps_down_to_the_guide_grid(self):
        cases = {22: 22, 23: 22, 38: 22, 39: 39, 56: 56, 124: 124, 130: 124, 21: 5, 5: 5, 4: 5, 0: 5, None: 5}
        for given, expected in cases.items():
            self.assertEqual(video_input.prefix_frame_count(given), expected, given)

    @unittest.skipUnless(shutil.which(video_input.FFMPEG_BIN) and shutil.which(video_input.FFPROBE_BIN),
                         "ffmpeg is not installed")
    def test_the_last_frames_and_their_sound_are_kept(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "source.mp4"
            # 2 s = 48 frames whose luma is 5 * frame index, with a tone, 24 fps
            # (-t rather than -frames:v/-shortest: those stop the AAC stream after a few frames)
            subprocess.run([video_input.FFMPEG_BIN, "-v", "error", "-y",
                            "-f", "lavfi", "-i", "color=c=black:s=64x64:r=24:d=2",
                            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=2",
                            "-vf", "geq=lum='N*5':cb=128:cr=128", "-t", "2",
                            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(source)],
                           check=True)

            async def fake_download(client, url, target):
                shutil.copyfile(source, target)

            with patch.object(video_input, "_download", AsyncMock(side_effect=fake_download)), \
                    patch.object(config, "RENDER_DIR", root / "render"):
                name, data = asyncio.run(video_input.download_prepare_prefix(
                    object(), "https://autorig.online/dev/api/scratch/source.mp4", 22))
            self.assertTrue(name.startswith("prefix-") and name.endswith(".mp4"))
            out = root / "out.mp4"
            out.write_bytes(data)
            probe = subprocess.run([video_input.FFPROBE_BIN, "-v", "error", "-count_frames", "-show_entries",
                                    "stream=codec_type,codec_name,nb_read_frames", "-of", "csv=p=0", str(out)],
                                   capture_output=True, text=True, check=True).stdout
            self.assertIn("video,h264,22", probe.replace("h264,video", "video,h264"))
            self.assertIn("aac", probe)
            raw = subprocess.run([video_input.FFMPEG_BIN, "-v", "error", "-i", str(out), "-frames:v", "1",
                                  "-f", "rawvideo", "-pix_fmt", "gray", "-"], capture_output=True, check=True).stdout
            first_luma = sum(raw) / max(len(raw), 1)
            # the first kept frame is source frame 26 (48 - 22): luma 130, within codec tolerance
            self.assertAlmostEqual(first_luma, 130, delta=6)
            self.assertFalse((root / "render" / ".video-input-temp").exists() and
                             any((root / "render" / ".video-input-temp").iterdir()))


class PrefixApiTests(unittest.TestCase):
    def test_only_h3_takes_a_prefix_and_the_url_is_admitted(self):
        import ai_vision_api
        from fastapi import HTTPException
        none = SimpleNamespace(video_prefix_url=None, video_prefix_frames=None)
        self.assertEqual(ai_vision_api._video_prefix_fields(none, "minimax_h3"), {})
        self.assertEqual(ai_vision_api._video_prefix_fields(none, "ltx23"), {})
        body = SimpleNamespace(video_prefix_url="https://autorig.online/dev/api/scratch/a.mp4", video_prefix_frames=None)
        self.assertEqual(ai_vision_api._video_prefix_fields(body, "minimax_h3"),
                         {"video_prefix_url": body.video_prefix_url, "video_prefix_frames": 22})
        body.video_prefix_frames = 39
        self.assertEqual(ai_vision_api._video_prefix_fields(body, "minimax_h3")["video_prefix_frames"], 39)
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._video_prefix_fields(body, "ltx23")
        self.assertEqual(caught.exception.detail["error_string"], "unsupported_video_prefix")
        body.video_prefix_url = "http://autorig.online/dev/api/scratch/a.mp4"
        with self.assertRaises(HTTPException) as caught:
            ai_vision_api._video_prefix_fields(body, "minimax_h3")
        self.assertEqual(caught.exception.detail["error_string"], "invalid_video_prefix")

    def test_the_request_model_carries_the_fields(self):
        import ai_vision_api
        fields = ai_vision_api.VideoRequest.model_fields
        self.assertIn("video_prefix_url", fields)
        self.assertIn("video_prefix_frames", fields)
        self.assertIsNone(ai_vision_api.VideoRequest(image_url="https://x/a.png").video_prefix_url)


if __name__ == "__main__":
    unittest.main()

"""A video the Vision service can actually read: one labelled contact sheet.

The farm models read pictures, so "describe this video" has to become "describe
this picture of the video". The picture is only honest if it reaches the end of
the clip and says which moment each tile is, which is what these tests pin.

The end-to-end cases need ffmpeg. They build their own clip rather than
depending on a fixture, and skip where ffmpeg is absent.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import ai_video_reference as ref  # noqa: E402
import ai_vision_api as api  # noqa: E402

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")
SOURCE_URL = "https://autorig.online/renderfin/render/default_user/test-clip.mp4"


def _synthetic_clip(target: Path) -> None:
    """Four seconds with one hard cut in the middle, so scenes are detectable."""
    subprocess.run(
        [FFMPEG, "-v", "error", "-y",
         "-f", "lavfi", "-i", "testsrc=duration=2:size=320x240:rate=10",
         "-f", "lavfi", "-i", "color=c=red:duration=2:size=320x240:rate=10",
         "-filter_complex", "[0:v][1:v]concat=n=2:v=1[out]",
         "-map", "[out]", "-pix_fmt", "yuv420p", str(target)],
        check=True, capture_output=True,
    )


class FramePlacementTests(unittest.TestCase):
    """Pure placement logic — no ffmpeg, so it runs everywhere."""

    def test_the_sheet_always_reaches_the_end_of_the_video(self):
        times = ref._contact_sheet_times(12.0, [])
        self.assertEqual(times[0], 0.0)
        self.assertGreater(times[-1], 11.0)

    def test_a_video_with_no_scene_changes_is_still_covered_evenly(self):
        times = ref._contact_sheet_times(10.0, [])
        self.assertGreaterEqual(len(times), ref.CONTACT_SHEET_MIN_FRAMES)
        self.assertEqual(times, sorted(times))

    def test_the_strongest_changes_win_the_middle_slots(self):
        scenes = [(2.0, 0.9), (5.0, 0.8), (7.5, 0.7), (2.02, 0.95)]
        times = ref._contact_sheet_times(10.0, scenes)
        self.assertIn(2.02, times)
        self.assertIn(5.0, times)
        # 2.0 sits on top of 2.02 and buys nothing, so it is not spent a tile.
        self.assertNotIn(2.0, times)

    def test_a_cut_heavy_video_is_capped_so_the_tiles_stay_readable(self):
        scenes = [(index * 0.4, 0.9) for index in range(1, 60)]
        times = ref._contact_sheet_times(24.0, scenes)
        self.assertLessEqual(len(times), ref.CONTACT_SHEET_MAX_FRAMES)
        self.assertGreaterEqual(len(times), ref.CONTACT_SHEET_MIN_FRAMES)

    def test_a_one_frame_source_does_not_ask_for_an_impossible_tile(self):
        times = ref._contact_sheet_times(0.04, [])
        self.assertTrue(times)
        self.assertTrue(all(value >= 0 for value in times))

    def test_the_prompt_prefix_names_the_span_and_asks_for_the_whole_video(self):
        prefix = ref.contact_sheet_prompt_prefix(8, [0.0, 1.0, 3.9], 3.94)
        self.assertIn("8 chronological frames", prefix)
        self.assertIn("beginning to the end", prefix)
        self.assertIn("3.9", prefix)


@unittest.skipUnless(FFMPEG and FFPROBE, "ffmpeg/ffprobe are not installed here")
class ContactSheetTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.clip = root / "clip.mp4"
        _synthetic_clip(self.clip)
        self.assets = root / "assets-root"
        self.assets.mkdir()
        patch = mock.patch.object(ref, "ASSET_DIR", self.assets)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self.temp.cleanup)

        clip = self.clip

        async def fake_download(client, url, target):
            target.write_bytes(clip.read_bytes())
            return {}

        download = mock.patch.object(ref, "download_source_video", fake_download)
        download.start()
        self.addCleanup(download.stop)

    async def test_the_source_is_probed_before_any_frame_is_cut(self):
        probe = await ref._probe_source(self.clip)
        self.assertAlmostEqual(probe["duration"], 4.0, delta=0.4)
        self.assertEqual(probe["width"], 320)
        self.assertEqual(probe["height"], 240)
        self.assertGreater(probe["fps"], 0)

    async def test_the_hard_cut_in_the_middle_is_found(self):
        scenes = await ref._scene_change_times(self.clip, 4.0)
        self.assertTrue(scenes, "no scene change detected in a clip that has one")
        self.assertTrue(any(1.5 < moment < 2.5 for moment, _score in scenes))

    async def test_a_contact_sheet_covers_the_clip_and_says_so(self):
        result = await ref._derive_contact_sheet(
            ref.VideoReferenceRequest(video_url=SOURCE_URL, view="contact_sheet"))
        self.assertTrue(result["success_bool"])
        self.assertGreaterEqual(result["frame_count_int"], ref.CONTACT_SHEET_MIN_FRAMES)
        self.assertLessEqual(result["frame_count_int"], ref.CONTACT_SHEET_MAX_FRAMES)
        points = result["timepoints_seconds_float_array"]
        self.assertEqual(points[0], 0.0)
        self.assertGreater(points[-1], 3.0)
        self.assertIn("chronological frames", result["prompt_prefix_string"])
        self.assertLessEqual(result["width_int"], 2048)
        self.assertLessEqual(result["height_int"], 2048)
        stored = self.assets / "assets" / result["asset_id_string"] / (
            result["sha256_string"] + ".png")
        self.assertTrue(stored.is_file())
        self.assertGreater(stored.stat().st_size, 1000)

    async def test_a_portrait_clip_still_fits_inside_the_png_bounds(self):
        subprocess.run(
            [FFMPEG, "-v", "error", "-y", "-f", "lavfi",
             "-i", "testsrc=duration=3:size=240x640:rate=10",
             "-pix_fmt", "yuv420p", str(self.clip)],
            check=True, capture_output=True,
        )
        result = await ref._derive_contact_sheet(
            ref.VideoReferenceRequest(video_url=SOURCE_URL, view="contact_sheet"))
        self.assertLessEqual(result["width_int"], 2048)
        self.assertLessEqual(result["height_int"], 2048)


class VisionVideoRequestTests(unittest.TestCase):
    def test_a_video_url_outside_the_allow_list_is_refused_at_the_door(self):
        with self.assertRaises(ValueError):
            api.VisionRequest(prompt="What happens?",
                              video_url="https://evil.test/clip.mp4")

    def test_an_allowed_video_url_is_accepted_without_an_image(self):
        body = api.VisionRequest(prompt="What happens?", video_url=SOURCE_URL)
        self.assertEqual(body.video_url, SOURCE_URL)
        self.assertEqual(body.video_mode, "storyboard")

    def test_an_unknown_video_mode_is_refused(self):
        with self.assertRaises(ValueError):
            api.VisionRequest(prompt="x", video_url=SOURCE_URL, video_mode="montage")

    def test_neither_a_picture_nor_a_video_is_still_an_error(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as caught:
            asyncio.run(api._uncached_api_vision(None, api.VisionRequest(prompt="x")))
        self.assertEqual(caught.exception.detail["error_string"], "image_required")

    def test_a_video_is_turned_into_a_sheet_and_the_prompt_says_what_it_is(self):
        async def fake_reference(video_url, view):
            self.assertEqual(view, "contact_sheet")
            self.assertEqual(video_url, SOURCE_URL)
            return {"image_url_string": "https://autorig.online/api/ai/video-references/a/b.png",
                    "prompt_prefix_string": "These are 8 chronological frames"}

        run = mock.AsyncMock(return_value={"success_bool": True})
        with mock.patch.object(ref, "reference_for_video", fake_reference), \
             mock.patch.object(api, "_run", run):
            asyncio.run(api._uncached_api_vision(None, api.VisionRequest(
                prompt="What happens?", video_url=SOURCE_URL)))
        payload = run.call_args.args[2]
        self.assertEqual(payload["image_url"],
                         "https://autorig.online/api/ai/video-references/a/b.png")
        self.assertTrue(payload["prompt"].startswith("These are 8 chronological frames"))
        self.assertIn("What happens?", payload["prompt"])

    def test_the_frame_mode_asks_for_the_first_frame_instead(self):
        seen = {}

        async def fake_reference(video_url, view):
            seen["view"] = view
            return {"image_url_string": "https://autorig.online/api/ai/video-references/a/b.png"}

        run = mock.AsyncMock(return_value={"success_bool": True})
        with mock.patch.object(ref, "reference_for_video", fake_reference), \
             mock.patch.object(api, "_run", run):
            asyncio.run(api._uncached_api_vision(None, api.VisionRequest(
                prompt="What is in the first shot?", video_url=SOURCE_URL,
                video_mode="frame")))
        self.assertEqual(seen["view"], "first_frame")
        self.assertEqual(run.call_args.args[2]["prompt"], "What is in the first shot?")

    def test_a_wired_image_still_wins_over_a_wired_video(self):
        run = mock.AsyncMock(return_value={"success_bool": True})
        with mock.patch.object(api, "_run", run):
            asyncio.run(api._uncached_api_vision(None, api.VisionRequest(
                prompt="What is this?", image_url="https://example.test/a.png",
                video_url=SOURCE_URL)))
        self.assertEqual(run.call_args.args[2]["image_url"], "https://example.test/a.png")

    def test_the_vision_service_declares_a_video_input(self):
        import ai_services
        fields = {item["field"]: item for item in ai_services.service("vision")["inputs"]}
        self.assertIn("video_url", fields)
        self.assertEqual(fields["video_url"]["type"], ai_services.VIDEO)
        self.assertFalse(fields["video_url"]["required"])
        # The image stays the declared requirement so /vision still starts on
        # its own when a picture is handed to it.
        self.assertTrue(fields["image"]["required"])
        modes = {option["value"] for item in ai_services.params_for("vision")
                 if item["name"] == "video_mode" for option in item["options"]}
        self.assertEqual(modes, {"storyboard", "frame"})


if __name__ == "__main__":
    unittest.main()

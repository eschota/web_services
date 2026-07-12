import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import youtube_upload


class YoutubeUploadSourceTests(unittest.TestCase):
    def test_backend_video_cache_is_preferred(self):
        with tempfile.TemporaryDirectory(prefix="autorig-youtube-test-") as tmp:
            root = Path(tmp)
            video_cache = root / "videos"
            task_cache = root / "tasks"
            video_cache.mkdir()
            task_cache.mkdir()
            cached_video = video_cache / "task-1.mp4"
            cached_video.write_bytes(b"video-bytes")
            task = SimpleNamespace(id="task-1", input_type="t_pose")

            with (
                patch.object(youtube_upload, "YOUTUBE_VIDEO_CACHE_DIR", video_cache),
                patch.object(youtube_upload, "YOUTUBE_TASK_CACHE_DIR", task_cache),
            ):
                candidates = youtube_upload._task_youtube_local_video_candidates(task)

            self.assertEqual(candidates, [cached_video])

    def test_animal_task_prefers_cached_rig_preview(self):
        with tempfile.TemporaryDirectory(prefix="autorig-youtube-test-") as tmp:
            root = Path(tmp)
            video_cache = root / "videos"
            task_cache = root / "tasks"
            video_cache.mkdir()
            task_dir = task_cache / "task-2"
            task_dir.mkdir(parents=True)
            rig_preview = task_dir / "rig_preview.mp4"
            generic_video = task_dir / "video.mp4"
            rig_preview.write_bytes(b"rig-preview")
            generic_video.write_bytes(b"generic-video")
            task = SimpleNamespace(id="task-2", input_type="animal")

            with (
                patch.object(youtube_upload, "YOUTUBE_VIDEO_CACHE_DIR", video_cache),
                patch.object(youtube_upload, "YOUTUBE_TASK_CACHE_DIR", task_cache),
            ):
                candidates = youtube_upload._task_youtube_local_video_candidates(task)

            self.assertEqual(candidates[:2], [rig_preview, generic_video])

    def test_upload_limit_error_is_retryable(self):
        error = RuntimeError(
            "The user has exceeded the number of videos they may upload. "
            "reason: uploadLimitExceeded"
        )
        self.assertTrue(youtube_upload._youtube_error_is_upload_limit(error))
        self.assertFalse(youtube_upload._youtube_error_is_upload_limit(RuntimeError("quotaExceeded")))


if __name__ == "__main__":
    unittest.main()

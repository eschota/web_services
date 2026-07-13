import tempfile
import unittest
from pathlib import Path

from animation_fitting.storage import (
    ImmutableArtifactError,
    ImmutableArtifactStore,
    WorkerBusyError,
)


class AnimationFittingStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = ImmutableArtifactStore(Path(self.temp_dir.name))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_raw_video_is_content_addressed_and_idempotently_immutable(self):
        payload = b"mp4-test-payload" * 8
        first = self.store.store_raw_video(payload)
        second = self.store.store_raw_video(payload)
        self.assertEqual(first, second)
        self.assertEqual(first.path.read_bytes(), payload)
        self.assertEqual(first.path.suffix, ".mp4")

    def test_frame_index_cannot_be_replaced_with_different_bytes(self):
        raw = self.store.store_raw_video(b"video" * 16)
        first = self.store.store_frame(raw.sha256, 0, b"png-one")
        duplicate = self.store.store_frame(raw.sha256, 0, b"png-one")
        self.assertEqual(first, duplicate)
        with self.assertRaisesRegex(ImmutableArtifactError, "collision"):
            self.store.store_frame(raw.sha256, 0, b"png-two")

    def test_job_state_is_append_only_and_latest_is_last_revision(self):
        first = self.store.append_job_state("job-1", {"status_string": "submitting"})
        second = self.store.append_job_state("job-1", {"status_string": "completed"})
        self.assertEqual(first.name, "000001.json")
        self.assertEqual(second.name, "000002.json")
        self.assertEqual(self.store.latest_job_state("job-1")["status_string"], "completed")
        self.assertEqual(self.store.latest_job_state("job-1")["sequence_int"], 2)

    def test_worker_lease_enforces_one_job_per_gpu_and_releases(self):
        with self.store.worker_lease("http://127.0.0.1:8188", "owner-one") as lock_path:
            self.assertTrue(lock_path.exists())
            with self.assertRaises(WorkerBusyError):
                with self.store.worker_lease("http://127.0.0.1:8188", "owner-two"):
                    pass
        self.assertFalse(lock_path.exists())
        with self.store.worker_lease("http://127.0.0.1:8188", "owner-two"):
            pass


if __name__ == "__main__":
    unittest.main()

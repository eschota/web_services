import unittest

from animation_fitting.smoke_run import validate_ffprobe_video


def _probe(*, codec="h264", fps="30/1", frames="49"):
    return {
        "streams": [
            {
                "codec_type": "video",
                "codec_name": codec,
                "avg_frame_rate": fps,
                "r_frame_rate": fps,
                "nb_read_frames": frames,
                "width": 384,
                "height": 224,
            }
        ]
    }


class AnimationFittingSmokeRunTests(unittest.TestCase):
    def test_ffprobe_gate_accepts_exact_h264_30fps_frame_contract(self):
        summary = validate_ffprobe_video(_probe(), expected_frame_count=49)
        self.assertEqual(summary["video_codec_string"], "h264")
        self.assertEqual(summary["video_fps_int"], 30)
        self.assertEqual(summary["video_frame_count_int"], 49)

    def test_ffprobe_gate_rejects_wrong_rate(self):
        with self.assertRaisesRegex(RuntimeError, "Expected 30 fps"):
            validate_ffprobe_video(_probe(fps="24/1"), expected_frame_count=49)

    def test_ffprobe_gate_rejects_wrong_frame_count(self):
        with self.assertRaisesRegex(RuntimeError, "Expected 49 ffprobe frames"):
            validate_ffprobe_video(_probe(frames="48"), expected_frame_count=49)


if __name__ == "__main__":
    unittest.main()

import json
import sys
import unittest
from pathlib import Path


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from animation_corrections import (  # noqa: E402
    AnimationCorrectionValidationError,
    canonical_json,
    payload_sha256,
    validate_animation_corrections,
)


class AnimationCorrectionValidationTests(unittest.TestCase):
    def test_normalizes_global_and_per_clip_layers(self):
        payload = validate_animation_corrections(
            {
                "schemaVersion": 1,
                "skeletonSignature": "horse-v1",
                "global": {
                    "root[0]/tail[0]": {
                        "rotationDeg": [0, 10, -5],
                        "positionPct": [0, 0, 0],
                        "motionScale": 0.5,
                    }
                },
                "clips": {
                    "walk_forward": {
                        "root[0]/tail[0]": {"motionScale": 0.25, "enabled": False}
                    }
                },
            }
        )

        self.assertEqual(payload["schemaVersion"], 1)
        self.assertEqual(payload["skeletonSignature"], "horse-v1")
        self.assertEqual(payload["global"]["root[0]/tail[0]"]["rotationDeg"], [0.0, 10.0, -5.0])
        self.assertEqual(payload["clips"]["walk_forward"]["root[0]/tail[0]"]["motionScale"], 0.25)
        self.assertFalse(payload["clips"]["walk_forward"]["root[0]/tail[0]"]["enabled"])

    def test_rejects_out_of_range_and_non_finite_values(self):
        invalid_values = [
            {"rotationDeg": [181, 0, 0]},
            {"positionPct": [0, -101, 0]},
            {"motionScale": 2.01},
            {"motionScale": float("nan")},
        ]
        for correction in invalid_values:
            with self.subTest(correction=correction):
                with self.assertRaises(AnimationCorrectionValidationError):
                    validate_animation_corrections({"global": {"root[0]": correction}})

    def test_rejects_unknown_schema(self):
        with self.assertRaises(AnimationCorrectionValidationError):
            validate_animation_corrections({"schemaVersion": 2})

    def test_canonical_digest_is_key_order_independent(self):
        left = validate_animation_corrections(
            {"global": {"b": {"motionScale": 0.5}, "a": {"motionScale": 1.5}}}
        )
        right = json.loads(json.dumps(left))
        right["global"] = {"a": right["global"]["a"], "b": right["global"]["b"]}
        self.assertEqual(canonical_json(left), canonical_json(right))
        self.assertEqual(payload_sha256(left), payload_sha256(right))

    def test_public_draft_publish_and_callback_routes_are_wired(self):
        source = (BACKEND / "main.py").read_text(encoding="utf-8")
        self.assertIn('@app.get("/api/task/{task_id}/animation-corrections")', source)
        self.assertIn('@app.put("/api/task/{task_id}/animation-corrections")', source)
        self.assertIn('@app.post("/api/task/{task_id}/animation-corrections/publish")', source)
        self.assertIn(
            '@app.post("/api/internal/task/{task_id}/animation-corrections/export/{revision}")',
            source,
        )
        self.assertIn("BackgroundTask(dispatch_animation_correction_export, task_id, revision)", source)
        self.assertIn('response.headers["Cache-Control"] = "no-store"', source)



if __name__ == "__main__":
    unittest.main()

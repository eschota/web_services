import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import equine_contact_evidence as evidence


def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()


class ContactEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=str(Path(__file__).parent)); self.root = Path(self.temp.name)
        self.path = self.root / "constraints.json"

    def tearDown(self): self.temp.cleanup()

    def write(self, positions_by_leg, missing=None):
        missing = missing or {}
        constraints = {}
        for leg in ("LF", "RF", "LH", "RH"):
            rows = []
            for frame, position in enumerate(positions_by_leg[leg]):
                if frame in missing.get(leg, set()): continue
                rows.append({"frame": frame, "source_frame": frame, "position": position,
                             "confidence": .9, "plane_residual_ratio": .001})
            constraints[leg] = {"source_anchor_id": leg + ":1",
                                "derivation": "derived_from_source_plane_constraint",
                                "generated": False, "mirrored": False, "constraints": rows}
        doc = {"schema": "autorig.equine-sagittal-limb-constraints.v1",
               "state": "fit_candidate_constraints", "accepted_observations": False,
               "animation_qa_pass": False, "source_height": 2.0,
               "axes": {"up": [0, 1, 0]},
               "timing": {"frame_count": 8, "fps": 10.0, "source_frame_indices": list(range(8)),
                          "source_video_sha256": "a" * 64, "verified": True,
                          "source": "receipt", "full_source": True,
                          "retimed": False, "loop_declared": False},
               "constraints": constraints,
               "provenance": {"source_video_sha256": "a" * 64,
                              "observations_sha256": "b" * 64, "raw_npz_sha256": "c" * 64}}
        self.path.write_text(json.dumps(doc), encoding="utf-8")

    def base(self):
        noise = [0, .001, -.001, .001, 0, -.001, .001, 0]
        return {leg: [[0, value, 0] for value in noise] for leg in ("LF", "RF", "LH", "RH")}

    def test_stationary_lower_envelope_with_small_noise_is_observed_contact(self):
        self.write(self.base())
        result = evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))
        for leg in result["hooves"]:
            self.assertTrue(all(row["state"] == "OBSERVED_CONTACT" for row in result["hooves"][leg]["frames"]))
        self.assertFalse(result["source_ground_candidate"]["video_observed_confidence"])
        self.assertFalse(result["provenance"]["gait_phase_prior"])
        self.assertEqual(result["provenance"]["raw_npz_sha256"], "c" * 64)
        self.assertEqual(result["provenance"]["method"],
                         "per_hoof_lower_envelope_and_adjacent_source_plane_constraint_speed")

    def test_fast_translation_is_swing_not_false_contact(self):
        values = self.base()
        values["LF"] = [[frame * .1, 0, 0] for frame in range(8)]
        self.write(values)
        result = evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))
        states = [row["state"] for row in result["hooves"]["LF"]["frames"]]
        self.assertEqual(set(states), {"OBSERVED_SWING"})
        self.assertTrue(all(row["state"] == "OBSERVED_CONTACT" for row in result["hooves"]["RF"]["frames"]))

    def test_occluded_frame_and_adjacent_speed_gap_stay_pending(self):
        self.write(self.base(), {"LH": {3}})
        result = evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))
        frames = result["hooves"]["LH"]["frames"]
        self.assertEqual(frames[3]["state"], "PENDING")
        self.assertIn("hidden_weak_or_invalid", frames[3]["reason"])
        self.assertTrue(any(value.startswith("pending_contact:LH:3") for value in result["pending"]))

    def test_no_cross_leg_synthesis_when_one_hoof_has_too_few_measurements(self):
        self.write(self.base(), {"RF": {0, 1, 2, 3, 4, 5}})
        result = evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))
        self.assertTrue(all(row["state"] == "PENDING" for row in result["hooves"]["RF"]["frames"]))
        self.assertTrue(all(row["state"] == "OBSERVED_CONTACT" for row in result["hooves"]["LF"]["frames"]))
        self.assertFalse(result["provenance"]["opposite_leg_inference"])

    def test_options_are_documented_and_range_checked(self):
        self.assertTrue(all(row["doc"] for row in evidence.options_get().values()))
        with self.assertRaises(evidence.ContactEvidenceError):
            evidence.options_get({"contact_height_band_ratio": .1, "swing_min_height_ratio": .05})
        for invalid in (4.7, True):
            with self.assertRaisesRegex(evidence.ContactEvidenceError, "must be an integer|must be finite"):
                evidence.options_get({"min_observed_frames_for_envelope": invalid})

    def test_malformed_source_frame_mapping_or_video_binding_fails(self):
        self.write(self.base())
        document = json.loads(self.path.read_text())
        document["timing"]["source_frame_indices"] = [0, 1, 2, 2, 4, 5, 6, 7]
        self.path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(evidence.ContactEvidenceError, "timing"):
            evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))
        document["timing"]["source_frame_indices"] = list(range(8))
        document["timing"]["source_video_sha256"] = "d" * 64
        self.path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(evidence.ContactEvidenceError, "timing"):
            evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))

    def test_retimed_constraints_cannot_be_contact_evidence(self):
        self.write(self.base())
        document = json.loads(self.path.read_text()); document["timing"]["retimed"] = True
        self.path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(evidence.ContactEvidenceError, "timing"):
            evidence.build_contact_evidence(constraints_path=self.path, constraints_sha256=sha(self.path))


if __name__ == "__main__": unittest.main()

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

import equine_raw_observations as raw


def write(path, value): path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def array_sha(value):
    value = np.ascontiguousarray(value); digest = hashlib.sha256()
    digest.update(str(value.dtype).encode()); digest.update(str(value.shape).encode()); digest.update(value.tobytes())
    return digest.hexdigest()


class RawObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=str(Path(__file__).parent)); self.root = Path(self.temp.name)
        self.video = self.root / "reference.mp4"; self.video.write_bytes(b"exact-source-video")
        self.surface = self.root / "surface_anchors.json"
        self.anchor_ids = ["LeftFrontToe:1", "RightFrontToe:2", "LeftHindToe:3", "RightHindToe:4"]
        write(self.surface, {"bones": [{"bone": value.split(":")[0],
                                        "points": [{"vertex_id": int(value.split(":")[1])}]}
                                       for value in self.anchor_ids] +
                                      [{"bone": "Hips", "points": [{"vertex_id": 9}]}]})
        self.bundle = self.root / "fitting_bundle.json"
        write(self.bundle, {"schema": "autorig-actionless-fitting-bundle.v1", "source": {"sha256": "a" * 64},
                            "artifacts": {"surface_anchors": {"sha256": sha(self.surface)}}})
        self.manifest = self.root / "immutable_manifest.json"
        write(self.manifest, {"schema": "autorig-fitting-immutable-bundle.v1",
                              "bundle_manifest": {"sha256": sha(self.bundle)},
                              "files": [{"filename": "surface_anchors.json", "sha256": sha(self.surface)}]})
        self.anchor = self.root / "anchor.json"
        write(self.anchor, {"schema": "autorig.equine-fitting-anchor-map/1", "source_sha256": "a" * 64,
                            "bundle_manifest_sha256": sha(self.manifest),
                            "tracks": [{"track_id": str(i), "anchor_id": value}
                                       for i, value in enumerate(self.anchor_ids)]})
        points = np.arange(5 * 4 * 2, dtype=np.float32).reshape(5, 4, 2)
        visible = np.ones((5, 4), dtype=bool); visible[2, 0] = False
        confidence = np.full((5, 4), .8, np.float32)
        seeds = points[0].copy()
        self.npz = self.root / "tracks.npz"
        np.savez_compressed(self.npz, points_xy=points, visible=visible, confidence=confidence,
                            anchor_ids=np.asarray(self.anchor_ids), seed_points_xy=seeds)
        self.scope = "c" * 64
        self.meta = self.root / "tracks.json"
        write(self.meta, {"schema": "autorig.equine-raw-measurements/1", "stage": "tracks",
                          "binding": {"scope_sha256": self.scope, "video_sha256": sha(self.video),
                                      "frame_count": 5, "width": 64, "height": 32,
                                      "anchor_ids": self.anchor_ids, "seed_points_sha256": array_sha(seeds)},
                          "data_sha256": sha(self.npz), "provenance": {"backend": "tap-next"},
                          "calibration_accepted": False, "animation_quality_approved": False})

    def tearDown(self): self.temp.cleanup()

    def call(self, video=None):
        video = video or self.video
        return raw.convert_raw_measurements(
            raw_meta_path=self.meta, raw_meta_sha256=sha(self.meta), raw_npz_path=self.npz,
            raw_npz_sha256=sha(self.npz), source_video_path=video, source_video_sha256=sha(video),
            source_scope_sha256=self.scope, fps=24.0, anchor_map_path=self.anchor,
            anchor_map_sha256=sha(self.anchor), surface_anchors_path=self.surface,
            surface_anchors_sha256=sha(self.surface), fitting_bundle_path=self.bundle,
            fitting_bundle_sha256=sha(self.bundle), immutable_manifest_path=self.manifest,
            immutable_manifest_sha256=sha(self.manifest))

    def test_exports_original_raw_arrays_without_acceptance_claim(self):
        result = self.call()
        self.assertEqual(result["schema"], "autorig.equine-raw-2d-measurements.v1")
        self.assertFalse(result["calibration_accepted"]); self.assertFalse(result["camera_z_accepted"])
        self.assertFalse(result["accepted_observations"]); self.assertFalse(result["animation_quality_approved"])
        self.assertFalse(result["tracks"][0]["points"][2]["visible"])
        self.assertFalse(result["provenance"]["points_generated"])
        self.assertEqual(result["timing"]["source_frame_indices"], [0, 1, 2, 3, 4])
        self.assertEqual(result["timing"]["fps"], 24.0)
        self.assertFalse(result["timing"]["verified"])
        self.assertIn("unverified_source_fps", result["pending"])
        self.assertEqual(set(result["provenance"]["raw_array_sha256"]),
                         {"points_xy", "visible", "confidence", "anchor_ids", "seed_points_xy"})

    def test_corrupted_npz_is_rejected_by_stage_binding(self):
        original = sha(self.npz); self.npz.write_bytes(self.npz.read_bytes() + b"corrupt")
        with self.assertRaisesRegex(raw.RawObservationError, "does not bind"):
            raw.convert_raw_measurements(
                raw_meta_path=self.meta, raw_meta_sha256=sha(self.meta), raw_npz_path=self.npz,
                raw_npz_sha256=sha(self.npz), source_video_path=self.video, source_video_sha256=sha(self.video),
                source_scope_sha256=self.scope, fps=24, anchor_map_path=self.anchor,
                anchor_map_sha256=sha(self.anchor), surface_anchors_path=self.surface,
                surface_anchors_sha256=sha(self.surface), fitting_bundle_path=self.bundle,
                fitting_bundle_sha256=sha(self.bundle), immutable_manifest_path=self.manifest,
                immutable_manifest_sha256=sha(self.manifest))
        self.assertNotEqual(original, sha(self.npz))

    def test_anchor_identity_swap_is_rejected(self):
        with np.load(self.npz, allow_pickle=False) as archive: values = {key: archive[key] for key in archive.files}
        values["anchor_ids"] = values["anchor_ids"].copy(); values["anchor_ids"][[0, 1]] = values["anchor_ids"][[1, 0]]
        np.savez_compressed(self.npz, **values)
        meta = json.loads(self.meta.read_text()); meta["data_sha256"] = sha(self.npz); write(self.meta, meta)
        with self.assertRaisesRegex(raw.RawObservationError, "anchor order differ"): self.call()

    def test_extra_track_is_preserved_when_bound_to_immutable_surface_anchor(self):
        with np.load(self.npz, allow_pickle=False) as archive: values = {key: archive[key] for key in archive.files}
        values["points_xy"] = np.concatenate([values["points_xy"], values["points_xy"][:, :1]], axis=1)
        values["visible"] = np.concatenate([values["visible"], values["visible"][:, :1]], axis=1)
        values["confidence"] = np.concatenate([values["confidence"], values["confidence"][:, :1]], axis=1)
        values["anchor_ids"] = np.append(values["anchor_ids"], "Hips:9")
        values["seed_points_xy"] = np.concatenate([values["seed_points_xy"], values["seed_points_xy"][:1]], axis=0)
        np.savez_compressed(self.npz, **values)
        meta = json.loads(self.meta.read_text()); meta["data_sha256"] = sha(self.npz)
        meta["binding"]["anchor_ids"].append("Hips:9")
        meta["binding"]["seed_points_sha256"] = array_sha(values["seed_points_xy"]); write(self.meta, meta)
        result = self.call()
        self.assertEqual(len(result["tracks"]), 5)
        self.assertIsNone(result["tracks"][-1]["priority_semantic_id"])
        self.assertEqual(result["provenance"]["track_selection"]["raw_track_count"], 5)
        self.assertEqual(result["provenance"]["track_selection"]["priority_present_count"], 4)

    def test_different_video_and_fake_accepted_flag_are_rejected(self):
        other = self.root / "other.mp4"; other.write_bytes(b"different-video")
        with self.assertRaisesRegex(raw.RawObservationError, "source video mismatch"): self.call(other)
        meta = json.loads(self.meta.read_text()); meta["animation_quality_approved"] = True; write(self.meta, meta)
        with self.assertRaisesRegex(raw.RawObservationError, "acceptance flags"): self.call()

    def test_fresh_writer_never_overwrites_output(self):
        destination = self.root / "derived" / "raw-observations.json"
        raw.write_raw_measurements(self.call(), destination)
        with self.assertRaisesRegex(raw.RawObservationError, "already exists"):
            raw.write_raw_measurements(self.call(), destination)

    def test_pinned_video_timing_receipt_verifies_fps(self):
        receipt = {"schema": "autorig.video-timing-evidence.v1", "source_video_sha256": sha(self.video),
                   "frame_count": 5, "fps": 24.0, "receipt_id": "ffprobe-video-1"}
        result = raw.convert_raw_measurements(
            raw_meta_path=self.meta, raw_meta_sha256=sha(self.meta), raw_npz_path=self.npz,
            raw_npz_sha256=sha(self.npz), source_video_path=self.video, source_video_sha256=sha(self.video),
            source_scope_sha256=self.scope, fps=24.0, video_metadata_receipt=receipt,
            anchor_map_path=self.anchor, anchor_map_sha256=sha(self.anchor),
            surface_anchors_path=self.surface, surface_anchors_sha256=sha(self.surface),
            fitting_bundle_path=self.bundle, fitting_bundle_sha256=sha(self.bundle),
            immutable_manifest_path=self.manifest, immutable_manifest_sha256=sha(self.manifest))
        self.assertTrue(result["timing"]["verified"])
        self.assertEqual(result["pending"], [])

    def test_invalid_timing_or_crop_mapping_fails(self):
        with self.assertRaises(raw.RawObservationError):
            raw.convert_raw_measurements(
                raw_meta_path=self.meta, raw_meta_sha256=sha(self.meta), raw_npz_path=self.npz,
                raw_npz_sha256=sha(self.npz), source_video_path=self.video, source_video_sha256=sha(self.video),
                source_scope_sha256=self.scope, fps=0, anchor_map_path=self.anchor,
                anchor_map_sha256=sha(self.anchor), surface_anchors_path=self.surface,
                surface_anchors_sha256=sha(self.surface), fitting_bundle_path=self.bundle,
                fitting_bundle_sha256=sha(self.bundle), immutable_manifest_path=self.manifest,
                immutable_manifest_sha256=sha(self.manifest))
        with self.assertRaisesRegex(raw.RawObservationError, "source_frame_indices"):
            raw.convert_raw_measurements(
                raw_meta_path=self.meta, raw_meta_sha256=sha(self.meta), raw_npz_path=self.npz,
                raw_npz_sha256=sha(self.npz), source_video_path=self.video, source_video_sha256=sha(self.video),
                source_scope_sha256=self.scope, fps=24, source_frame_indices=[0, 1, 1, 3, 4],
                anchor_map_path=self.anchor, anchor_map_sha256=sha(self.anchor),
                surface_anchors_path=self.surface, surface_anchors_sha256=sha(self.surface),
                fitting_bundle_path=self.bundle, fitting_bundle_sha256=sha(self.bundle),
                immutable_manifest_path=self.manifest, immutable_manifest_sha256=sha(self.manifest))


if __name__ == "__main__": unittest.main()

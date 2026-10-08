import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("mt_run_manifest", ROOT / "tools" / "v3_viewer" / "mt_run_manifest.py")
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


RUN = "0123456789abcdefabcd"
TASK = "11111111-2222-3333-4444-555555555555"


def url(path):
    return f"https://autorig.online/api/mt/files/{RUN}/{path}"


def fixture():
    run = {"run_id": RUN, "kind": "full", "status": "partial", "created_at": 10.0, "finished_at": 15.0,
           "seconds": 5.0, "error": "map failed", "request": {"task_id": TASK, "frames": 361},
           "stages": {"projections": {"status": "done", "seconds": 1.25},
                      "map:parts": {"status": "failed", "error": "cancelled"}},
           "outputs": {"proj": {"proj/front_depth.png": url("proj/front_depth.png"),
                                  "proj/sheet_lit.png": url("proj/sheet_lit.png"),
                                  "proj/model.glb": url("proj/model.glb"),
                                  "proj/manifest.json": url("proj/manifest.json")},
                       "labels": {"labels/parts/labels.glb": url("labels/parts/labels.glb"),
                                  "labels/parts/labels.json": url("labels/parts/labels.json")},
                       "motion": {"motion/run.mp4": url("motion/run.mp4")},
                       "track": {"track/run/joints3d.json": url("track/run/joints3d.json")},
                       "vehicle": {"vehicle/rig.json": url("vehicle/rig.json"),
                                   "vehicle/body.glb": url("vehicle/body.glb")},
                       "root": {"phases.json": url("phases.json")}}}
    sha = "a" * 64
    documents = {"proj/manifest.json": {"schema": "autorig.motion-transfer.projections/1", "task_id": TASK,
                                         "glb_sha256": sha},
                 "labels/parts/labels.json": {"schema": "autorig.motion-transfer.labels/1",
                                               "legend": [{"index": 0, "name": "body", "rgb": [1,2,3]}]},
                 "track/run/joints3d.json": {"schema": "autorig.motion-transfer.joints3d/1", "fps": 24,
                                              "frames": 1, "frame": "glTF model frame (+Y up), same units as the model",
                                              "positions": [[[0,0,0],[0,1,0]]], "valid": [[True,True]],
                                              "joints": [[0,0,0],[0,1,0]], "bones": [[0,1]],
                                              "source_sha256": sha, "coordinate_space": "model_local_gltf",
                                              "matrix_to_model": [1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1]},
                 "phases.json": {"schema": "autorig.mt.phases/1", "run_id": RUN, "status": "running",
                                  "phases": [{"id": "projections", "status": "done"}]}}
    return run, documents


class MotionTransferManifestTests(unittest.TestCase):
    def test_normalizes_partial_run_without_promoting_missing_work(self):
        run, docs = fixture()
        result = MOD.normalize_run(run, documents=docs)
        self.assertEqual(result["schema"], MOD.SCHEMA)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["lifecycle"]["processing_status"], "terminal")
        self.assertIsNone(result["lifecycle"]["processing_duration_ms"])
        self.assertEqual(result["lifecycle"]["wall_duration_ms"], 5000.0)
        self.assertEqual(result["stages"][0]["duration_ms"], 1250.0)
        self.assertEqual(result["projections"][0]["view"], "front")
        self.assertEqual(result["sheets"][0]["pass"], "lit")
        self.assertEqual({item["type"] for item in result["models"]}, {"source_model", "vertex_labels", "separated_model"})
        self.assertTrue(result["tracks3d"][0]["ready"])
        self.assertEqual(result["tracks3d"][0]["source_sha256"], "a" * 64)
        self.assertTrue(result["outputs"]["phases"]["ready"])
        self.assertTrue(result["outputs"]["vehicle"])
        self.assertEqual(result["dag"][0], {"id":"source","status":"done","depends_on":[],"branch":"fast_geometry"})
        planned={item["id"]:item for item in result["dag"]}
        self.assertEqual(planned["stabilized_pose"]["status"],"planned")
        self.assertEqual(planned["diorama_image"]["artifact_type"],"image_2d")
        self.assertEqual(planned["diorama_image"]["status"],"planned")
        self.assertEqual(planned["final_video"]["status"],"planned")
        self.assertNotIn("diorama_image",result["scope"]["available"])

    def test_missing_projection_provenance_keeps_tracks_unready(self):
        run, docs = fixture(); docs.pop("proj/manifest.json")
        result = MOD.normalize_run(run, documents=docs)
        self.assertIsNone(result["source_sha256"])
        self.assertEqual(result["provenance"]["status"], "missing")
        self.assertFalse(result["tracks3d"][0]["ready"])

    def test_rejects_cross_origin_mismatched_and_unbounded_outputs(self):
        run, docs = fixture()
        run["outputs"]["proj"]["proj/front_depth.png"] = "https://example.com/file.png"
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs)
        run, docs = fixture(); run["outputs"] = {"x": {f"x/{i}.bin": url(f"x/{i}.bin") for i in range(MOD.MAX_FILES + 1)}}
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs)

    def test_rejects_projection_identity_and_path_attacks(self):
        run, docs = fixture(); docs["proj/manifest.json"]["schema"]="wrong"
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs)
        run, docs = fixture(); docs["proj/manifest.json"].pop("task_id")
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs)
        run, docs = fixture()
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs,
            file_meta={"proj/model.glb":{"sha256":"b"*64}})
        run, docs = fixture(); run["outputs"]["x"]={"x/%2e%2e/evil.json":url("x/%2e%2e/evil.json")}
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs)

    def test_rejects_hostile_legend_and_leaves_unbound_tracks_unvalidated(self):
        run, docs = fixture(); docs["labels/parts/labels.json"]["legend"]=[{"index":i,"name":"x","rgb":[0,0,0]} for i in range(65)]
        with self.assertRaises(MOD.ManifestError): MOD.normalize_run(run, documents=docs)
        run, docs = fixture(); docs["track/run/joints3d.json"].pop("source_sha256")
        result=MOD.normalize_run(run,documents=docs)
        self.assertFalse(result["tracks3d"][0]["ready"])
        self.assertEqual(result["tracks3d"][0]["status"],"available_unvalidated")
        run, docs = fixture(); track=docs["track/run/joints3d.json"]
        track.update(frames=0,positions=[],valid=[],fps=float("nan"),bones=[[0,99]])
        result=MOD.normalize_run(run,documents=docs)
        self.assertFalse(result["tracks3d"][0]["ready"])
        self.assertEqual(result["tracks3d"][0]["status"],"available_unvalidated")

    def test_queue_and_running_are_distinct_from_terminal(self):
        run, docs = fixture(); run.update(status="queued", stages={}, outputs={}); run.pop("seconds")
        result = MOD.normalize_run(run, documents={})
        self.assertEqual(result["lifecycle"]["queue_status"], "queued")
        self.assertEqual(result["lifecycle"]["processing_status"], "waiting")
        self.assertFalse(result["ready"])
        run["status"] = "running"
        result = MOD.normalize_run(run, documents={})
        self.assertEqual(result["lifecycle"]["processing_status"], "running")
        run["stages"]={"bad_bool":{"status":"done","seconds":True},"bad_nan":{"status":"done","seconds":float("nan")}}
        result=MOD.normalize_run(run,documents={})
        self.assertTrue(all(item["duration_ms"] is None for item in result["stages"]))


if __name__ == "__main__":
    unittest.main()

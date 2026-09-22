import argparse
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[1] / "video_benchmark.py"
SPEC = importlib.util.spec_from_file_location("video_benchmark", MODULE_PATH)
video_benchmark = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(video_benchmark)


class VideoBenchmarkTests(unittest.TestCase):
    def test_cases_require_fixed_seed(self):
        with self.assertRaises(video_benchmark.BenchmarkError):
            video_benchmark._manifest_cases({
                "sources_array": [{"id_string": "s", "url_string": "https://example.test/a.mp4"}],
                "pipelines_array": [{"id_string": "p", "request_object": {}}],
            })

    def test_explicit_cases_merge_request_without_mutating_pipeline(self):
        manifest = {
            "sources_array": [{"id_string": "s", "url_string": "https://example.test/a.mp4",
                               "prompt_string": "source prompt"}],
            "pipelines_array": [{"id_string": "p", "request_object": {"seed": 7, "steps": 8}}],
            "cases_array": [{"id_string": "c", "source_id_string": "s", "pipeline_id_string": "p",
                             "request_object": {"steps": 10}}],
        }
        cases = video_benchmark._manifest_cases(manifest)
        self.assertEqual(cases[0]["request"], {"seed": 7, "steps": 10, "prompt": "source prompt"})
        self.assertEqual(manifest["pipelines_array"][0]["request_object"]["steps"], 8)

    def test_submit_rejects_external_endpoint(self):
        with self.assertRaises(video_benchmark.BenchmarkError):
            video_benchmark._api_endpoint({"endpoint_string": "https://other.test/api/video"})
        self.assertEqual(video_benchmark._api_endpoint({}), "/api/video")

    def test_submit_persists_accepted_id_and_timeout_is_uncertain(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "schema_version_int": 1,
                "sources_array": [{"id_string": "s", "url_string": "https://example.test/a.mp4"}],
                "pipelines_array": [{"id_string": "p", "request_object": {"seed": 7}}],
            }), encoding="utf-8")
            output = MODULE_PATH.parents[2] / ".codex_tmp" / ("video-benchmark-test-" + root.name)
            args = argparse.Namespace(manifest=manifest, output_dir=output, base_url="https://example.test",
                                      token="", ffmpeg="ffmpeg", ffprobe="ffprobe", max_inflight=1,
                                      submit_limit=1, max_wait_seconds=0, poll_interval_seconds=1,
                                      http_timeout=1, max_segment_seconds=8, max_source_bytes=1024,
                                      case_id=[], prompt_overrides=None)
            try:
                runner = video_benchmark.Benchmark(args)
                case = runner.cases[0]
                first = runner._source_paths(case)["first"]
                first.parent.mkdir(parents=True)
                first.write_bytes(b"png")
                with patch.object(video_benchmark, "_request_json",
                                  return_value=(202, {"task_id_string": "abc", "status_string": "pending"})):
                    runner._submit(case)
                on_disk = json.loads(runner.state_path.read_text(encoding="utf-8"))
                self.assertEqual(on_disk["cases_object"]["s--p"]["task_id_string"], "abc")

                runner.state["cases_object"]["s--p"] = {"status_string": "prepared", "attempt_count_int": 0}
                with patch.object(video_benchmark, "_request_json", side_effect=TimeoutError("late")):
                    runner._submit(case)
                row = runner.state["cases_object"]["s--p"]
                self.assertEqual(row["status_string"], "submit_uncertain")
                self.assertNotIn("task_id_string", row)
            finally:
                shutil.rmtree(output, ignore_errors=True)

    def test_transient_poll_error_keeps_task_resumable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "sources_array": [{"id_string": "s", "url_string": "https://example.test/a.mp4"}],
                "pipelines_array": [{"id_string": "p", "request_object": {"seed": 7}}],
            }), encoding="utf-8")
            output = MODULE_PATH.parents[2] / ".codex_tmp" / ("video-benchmark-test-" + root.name)
            args = argparse.Namespace(manifest=manifest, output_dir=output, base_url="https://example.test",
                                      token="", ffmpeg="ffmpeg", ffprobe="ffprobe", max_inflight=1,
                                      submit_limit=1, max_wait_seconds=0, poll_interval_seconds=1,
                                      http_timeout=1, max_segment_seconds=8, max_source_bytes=1024,
                                      case_id=[], prompt_overrides=None)
            try:
                runner = video_benchmark.Benchmark(args)
                row = runner._case_state(runner.cases[0])
                row.update({"task_id_string": "abc", "status_string": "pending"})
                with patch.object(video_benchmark, "_request_json",
                                  side_effect=video_benchmark.BenchmarkError("invalid JSON")):
                    runner.poll_once()
                self.assertEqual(row["task_id_string"], "abc")
                self.assertEqual(row["status_string"], "pending")
                self.assertIn("last_poll_error_string", row)
            finally:
                shutil.rmtree(output, ignore_errors=True)

    def test_case_filter_and_prompt_override_are_exact(self):
        with tempfile.TemporaryDirectory(dir=MODULE_PATH.parents[2] / ".codex_tmp") as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "sources_array": [
                    {"id_string": "s1", "url_string": "https://example.test/1.mp4"},
                    {"id_string": "s2", "url_string": "https://example.test/2.mp4"},
                ],
                "pipelines_array": [{"id_string": "p", "request_object": {"seed": 7}}],
            }), encoding="utf-8")
            overrides = root / "overrides.json"
            overrides.write_text(json.dumps({"s2--p": "reviewed exact prompt"}), encoding="utf-8")
            args = argparse.Namespace(
                manifest=manifest, output_dir=root / "out", base_url="https://example.test",
                token="", ffmpeg="ffmpeg", ffprobe="ffprobe", max_inflight=1,
                submit_limit=1, max_wait_seconds=0, poll_interval_seconds=1,
                http_timeout=1, max_segment_seconds=8, max_source_bytes=1024,
                case_id=["s2--p"], prompt_overrides=overrides,
            )
            runner = video_benchmark.Benchmark(args)
            self.assertEqual([case["id"] for case in runner.cases], ["s2--p"])
            self.assertEqual(runner.cases[0]["request"]["prompt"], "reviewed exact prompt")

            args.case_id = ["missing--p"]
            with self.assertRaises(video_benchmark.BenchmarkError):
                video_benchmark.Benchmark(args)


if __name__ == "__main__":
    unittest.main()

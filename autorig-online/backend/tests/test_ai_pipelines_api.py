"""The support matrix: who can run what, and what it cost in the last day."""

import json
import os
import pathlib
import sqlite3
import tempfile
import time
import unittest

import ai_pipelines_api as pipelines


def _server(name, workflows, status="online", gpu="RTX", queue=0, holding=None):
    return {"render_server_name": name, "available_workflows": list(workflows),
            "status": status, "gpu_name": gpu, "queue_size": queue,
            "current_render_task": holding}


def _checkpoint(file_name, workflow, service="image", validated=None):
    entry = {"kind": "checkpoint", "file": file_name, "workflow": workflow,
             "services": [service], "usable": True, "title": file_name}
    if validated is not None:
        entry["validated_workers"] = list(validated)
    return entry


class SchedulingTokenTests(unittest.TestCase):
    def test_a_template_is_normally_its_own_token(self):
        self.assertEqual(pipelines.scheduling_token("gen_image.json"), "gen_image.json")

    def test_enhancement_templates_are_dispatched_under_the_canny_token(self):
        # The farm boxes never advertised upscale_fast.json, so matching on
        # the file name would mean these jobs were never scheduled anywhere.
        for workflow in ("upscale_fast.json", "face_fix.json", "detail_tiled.json"):
            self.assertEqual(pipelines.scheduling_token(workflow),
                             pipelines.ENHANCE_SCHEDULING_TOKEN)

    def test_qwen_image_templates_are_dispatched_under_the_canny_token(self):
        for name in ("qwen_image_generate.json", "qwen_image_edit.json"):
            self.assertEqual(pipelines.scheduling_token(name),
                             "gen_image_control_canny.json")

    def test_the_two_names_of_the_ryzen_box_are_one_computer(self):
        self.assertEqual(pipelines.canonical_computer("ryzen-server"),
                         pipelines.canonical_computer("Raptor"))


class LedgerReadingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = pathlib.Path(self.tmp.name) / "renderfin.db"
        connection = sqlite3.connect(self.db)
        connection.execute(
            "CREATE TABLE render_tasks (id TEXT PRIMARY KEY, payload TEXT, "
            "status TEXT, created_at REAL)")
        self.now = time.time()
        rows = [
            ("a", "gen_image.json", "f5", "Done", self.now - 300, self.now - 240),
            ("b", "gen_image.json", "f5", "Done", self.now - 200, self.now - 100),
            ("c", "gen_image.json", "worker-4090", "Error", 0, self.now - 90),
            # Older than the window: the page promises the last 24 hours.
            ("d", "gen_image.json", "f5", "Done", self.now - 200000,
             self.now - 199000),
        ]
        for task_id, workflow, server, status, started, finished in rows:
            created = started or finished
            payload = {"workflow_file": workflow, "server_name": server,
                       "status": status, "started_at": started,
                       "finished_at": finished, "prompt": {"checkpoint": "", "type": ""}}
            connection.execute(
                "INSERT INTO render_tasks VALUES (?,?,?,?)",
                (task_id, json.dumps(payload), status, created))
        connection.commit()
        connection.close()

    def test_only_the_last_day_is_read(self):
        jobs, problem = pipelines.read_render_jobs(self.now - pipelines.WINDOW_SECONDS,
                                                   path=self.db)
        self.assertEqual(problem, "")
        self.assertEqual(len(jobs), 3)

    def test_a_missing_ledger_is_reported_rather_than_raised(self):
        jobs, problem = pipelines.read_render_jobs(
            0, path=pathlib.Path(self.tmp.name) / "nope.db")
        self.assertEqual(jobs, [])
        self.assertTrue(problem)

    def test_cells_carry_the_average_and_the_median_of_real_runs(self):
        matrix = pipelines.build_matrix(
            now=self.now,
            render_jobs=pipelines.read_render_jobs(
                self.now - pipelines.WINDOW_SECONDS, path=self.db)[0],
            servers=[_server("f5", ["gen_image.json"]),
                     _server("worker-4090", ["gen_image_sdxl.json"])],
            converters=[], catalogue=[], workflow_files=["gen_image.json"])
        row = next(r for r in matrix["pipelines_array"] if r["id"] == "gen_image.json")
        f5 = row["computers_object"]["f5"]
        self.assertEqual((f5["jobs_int"], f5["ok_int"]), (2, 2))
        # 60 s and 100 s of real time on the card.
        self.assertEqual(f5["avg_seconds_float"], 80.0)
        self.assertEqual(f5["median_seconds_float"], 80.0)
        self.assertTrue(f5["capable_bool"])
        # The 4090 ran one and failed it, and does not advertise this template.
        four = row["computers_object"]["worker-4090"]
        self.assertEqual((four["jobs_int"], four["failed_int"]), (1, 1))
        self.assertFalse(four["capable_bool"])
        # A timing key is simply absent when nothing finished there.
        self.assertIsNone(four.get("avg_seconds_float"))


class MatrixShapeTests(unittest.TestCase):
    def setUp(self):
        self.now = time.time()
        self.servers = [
            _server("f5", ["gen_image.json", "gen_image_control_canny.json"]),
            _server("worker-4090", ["gen_animation_ltx23_by_url.json"]),
            _server("f15", ["gen_image.json"], status="offline"),
        ]
        self.converters = [
            {"name": "f13", "node": "f13", "hunyuan_bool": True, "ai_bool": True,
             "mode_string": "full", "parked_reason_string": ""},
            {"name": "f7", "node": "f7", "hunyuan_bool": False, "ai_bool": True,
             "mode_string": "full", "parked_reason_string": "Unity export broken"},
        ]
        self.catalogue = [
            _checkpoint("ltx-2.3.safetensors", "gen_animation_ltx23_by_url.json",
                        "video", ["worker-4090"]),
        ]

    def matrix(self, **kwargs):
        options = dict(now=self.now, render_jobs=[], ai_jobs=[],
                       servers=self.servers, converters=self.converters,
                       catalogue=self.catalogue, workflow_files=["upscale_fast.json"],
                       ai_models={"f13": ["bonsai2-27b"], "f7": ["bonsai2-27b"]})
        options.update(kwargs)
        return pipelines.build_matrix(**options)

    def test_every_box_is_a_column_whichever_registry_named_it(self):
        columns = {column["id"] for column in self.matrix()["computers_array"]}
        self.assertEqual(columns, {"f5", "worker-4090", "f15", "f13", "f7"})

    def test_an_offline_box_says_so(self):
        column = next(c for c in self.matrix()["computers_array"] if c["id"] == "f15")
        self.assertFalse(column["online_bool"])
        self.assertEqual(column["state_string"], "offline")

    def test_an_enhancement_row_is_capable_where_the_canny_token_lives(self):
        row = next(r for r in self.matrix()["pipelines_array"]
                   if r["id"] == "upscale_fast.json")
        self.assertTrue(row["computers_object"]["f5"]["capable_bool"])
        # The 4090 carries no ESRGAN weights and does not advertise the token.
        self.assertNotIn("worker-4090", row["computers_object"])
        self.assertIn("upscale", row["services_array"])

    def test_a_box_that_is_down_is_capable_but_not_ready(self):
        row = next(r for r in self.matrix()["pipelines_array"]
                   if r["id"] == "gen_image.json")
        # f15 carries the template and is not answering: those are different
        # facts and the page says so with different words.
        self.assertTrue(row["computers_object"]["f15"]["capable_bool"])
        self.assertFalse(row["computers_object"]["f15"]["ready_bool"])
        self.assertTrue(row["computers_object"]["f5"]["ready_bool"])

    def test_a_converter_with_no_language_model_is_not_on_the_vision_row(self):
        rows = {row["id"]: row for row in
                self.matrix(ai_models={"f13": ["bonsai2-27b"]})["pipelines_array"]}
        self.assertTrue(rows["ai_vision"]["computers_object"]["f13"]["capable_bool"])
        self.assertNotIn("f7", rows["ai_vision"]["computers_object"])

    def test_work_still_on_the_card_is_counted_without_a_time(self):
        jobs = [{"workflow": "gen_image.json", "computer": "f5",
                 "status": "Rendering", "started_at": self.now - 30,
                 "finished_at": 0, "checkpoint": "", "type": "",
                 "created_at": self.now - 40}]
        row = next(r for r in self.matrix(render_jobs=jobs)["pipelines_array"]
                   if r["id"] == "gen_image.json")
        cell = row["computers_object"]["f5"]
        self.assertEqual((cell["jobs_int"], cell["running_int"]), (1, 1))
        self.assertIsNone(cell.get("avg_seconds_float"))

    def test_a_finished_job_outranks_a_stale_validation_list(self):
        # The catalogue says only the 4090 holds this model; f5 finished one
        # an hour ago, which settles it.
        catalogue = [_checkpoint("ltx-2.3.safetensors", "gen_image.json",
                                 "image", ["worker-4090"])]
        jobs = [{"workflow": "gen_image.json", "computer": "f5", "status": "Done",
                 "started_at": self.now - 300, "finished_at": self.now - 240,
                 "checkpoint": "ltx-2.3.safetensors", "type": "",
                 "created_at": self.now - 310}]
        row = next(r for r in self.matrix(catalogue=catalogue, render_jobs=jobs)
                   ["pipelines_array"] if r["id"] == "gen_image.json")
        self.assertTrue(row["computers_object"]["f5"]["model_ready_bool"])

    def test_a_row_with_no_jobs_reports_no_time_rather_than_a_guess(self):
        row = next(r for r in self.matrix()["pipelines_array"]
                   if r["id"] == "upscale_fast.json")
        self.assertEqual(row["jobs_int"], 0)
        self.assertIsNone(row["avg_seconds_float"])
        self.assertIsNone(row["computers_object"]["f5"].get("avg_seconds_float"))

    def test_the_non_workflow_services_are_rows_too(self):
        rows = {row["id"]: row for row in self.matrix()["pipelines_array"]}
        self.assertIn("hunyuan_3d", rows)
        self.assertIn("ai_vision", rows)
        # Parked for Hunyuan, still fine for reading pictures.
        self.assertNotIn("f7", rows["hunyuan_3d"]["computers_object"])
        self.assertTrue(rows["ai_vision"]["computers_object"]["f7"]["capable_bool"])

    def test_vision_jobs_land_on_the_node_that_answered(self):
        ai_jobs = [{"service": "vision", "computer": "f13", "state": "completed",
                    "elapsed_seconds": 20.0, "created_at": self.now - 100,
                    "updated_at": self.now - 80},
                   {"service": "avatar_from_image", "computer": "f13",
                    "state": "completed", "elapsed_seconds": 40.0,
                    "created_at": self.now - 60, "updated_at": self.now - 20}]
        row = next(r for r in self.matrix(ai_jobs=ai_jobs)["pipelines_array"]
                   if r["id"] == "ai_vision")
        self.assertEqual(row["computers_object"]["f13"]["jobs_int"], 2)
        self.assertEqual(row["computers_object"]["f13"]["avg_seconds_float"], 30.0)

    def test_a_cancelled_job_that_never_reached_a_box_is_not_blamed_on_one(self):
        jobs = [{"workflow": "gen_image.json", "computer": "", "status": "Error",
                 "started_at": 0, "finished_at": self.now - 10, "checkpoint": "",
                 "type": "", "created_at": self.now - 20}]
        row = next(r for r in self.matrix(render_jobs=jobs)["pipelines_array"]
                   if r["id"] == "gen_image.json")
        self.assertEqual(row["computers_object"]["f5"]["jobs_int"], 0)
        self.assertEqual((row["unassigned_object"] or {}).get("failed_int"), 1)

    def test_a_checkpoint_bound_row_says_which_box_holds_the_model(self):
        row = next(r for r in self.matrix()["pipelines_array"]
                   if r["id"] == "gen_animation_ltx23_by_url.json")
        held = row["computers_object"]["worker-4090"]
        self.assertTrue(held["model_ready_bool"])
        self.assertEqual(held["checkpoints_array"], ["ltx-2.3.safetensors"])
        # f5 carries neither the template nor the model, and never tried: the
        # page draws a dash, so there is nothing to send.
        self.assertNotIn("f5", row["computers_object"])

    def test_a_qwen_image_row_names_its_service_and_the_canny_dispatch(self):
        catalogue = [_checkpoint("qwen-image-2512-Q3_K_S.gguf",
                                 "qwen_image_generate.json", "qwen_image", ["f5"])]
        row = next(r for r in self.matrix(catalogue=catalogue)["pipelines_array"]
                   if r["id"] == "qwen_image_generate.json")
        self.assertEqual(row["dispatch_token_string"], "gen_image_control_canny.json")
        self.assertIn("qwen_image", row["services_array"])

    def test_empty_cells_are_left_out_of_the_answer(self):
        row = next(r for r in self.matrix()["pipelines_array"]
                   if r["id"] == "upscale_fast.json")
        self.assertEqual(set(row["computers_object"]), {"f5"})

    def test_registry_files_are_read_off_disk(self):
        with tempfile.TemporaryDirectory() as folder:
            path = pathlib.Path(folder)
            (path / "f5.json").write_text(json.dumps(_server("f5", ["gen_image.json"])),
                                          encoding="utf-8")
            (path / "broken.json").write_text("{not json", encoding="utf-8")
            servers = pipelines.load_servers(path)
        self.assertEqual([server["render_server_name"] for server in servers], ["f5"])


class CapacityTests(unittest.TestCase):
    """The number the node editor puts in a header."""

    def setUp(self):
        self.servers = [
            _server("f5", ["gen_image.json", "gen_animation_by_url.json"]),
            _server("f15", ["gen_image.json", "gen_animation_by_url.json"]),
            _server("worker-4090", ["gen_image.json",
                                    "gen_animation_ltx23_by_url.json"]),
        ]
        self.nodes = [
            {"id": "f5", "online": True, "busy": False},
            {"id": "f15", "online": True, "busy": True},
            {"id": "worker-4090", "online": True, "busy": False},
            {"id": "f13", "online": True, "busy": False},
        ]
        self.converters = [
            {"name": "f13", "node": "f13", "hunyuan_bool": True, "ai_bool": True,
             "mode_string": "full", "parked_reason_string": ""},
            {"name": "f11", "node": "f11", "hunyuan_bool": False, "ai_bool": True,
             "mode_string": "full", "parked_reason_string": "torch crash"},
        ]
        self.catalogue = [
            _checkpoint("ltx-2.3.safetensors", "gen_animation_ltx23_by_url.json",
                        "video", ["worker-4090"]),
            _checkpoint("ltxv-13b.safetensors", "gen_animation_by_url.json",
                        "video", ["f5", "f15"]),
        ]

    def capacity(self, **kwargs):
        options = dict(ai_models_by_node={"f13": ["bonsai2-27b"]},
                       converters=self.converters, catalogue=self.catalogue)
        options.update(kwargs)
        return pipelines.capacity_object(self.nodes, self.servers, **options)

    def test_image_counts_every_box_carrying_the_template(self):
        image = self.capacity()["image"]
        self.assertEqual(image["total_int"], 3)
        # f15 is holding a job, so it is capable but not free.
        self.assertEqual(image["idle_int"], 2)
        self.assertEqual(image["computers_array"], ["f15", "f5", "worker-4090"])

    def test_an_offline_box_cannot_take_work_however_it_is_configured(self):
        nodes = [dict(node, online=False) if node["id"] == "f5" else node
                 for node in self.nodes]
        capacity = pipelines.capacity_object(
            nodes, self.servers, ai_models_by_node={}, converters=self.converters,
            catalogue=self.catalogue)
        self.assertEqual(capacity["image"]["total_int"], 2)

    def test_a_video_checkpoint_narrows_the_answer_to_the_boxes_that_hold_it(self):
        video = self.capacity()["video"]
        # The default animation template runs on both farm boxes.
        self.assertEqual(video["total_int"], 2)
        fast = video["checkpoints_object"]["ltxv-13b.safetensors"]
        self.assertEqual(fast["computers_array"], ["f15", "f5"])
        slow = video["checkpoints_object"]["ltx-2.3.safetensors"]
        self.assertEqual(slow["computers_array"], ["worker-4090"])
        self.assertEqual(slow["idle_int"], 1)

    def test_3d_needs_a_converter_that_is_still_allowed_to_run_hunyuan(self):
        self.assertEqual(self.capacity()["3dmodel"]["computers_array"], ["f13"])

    def test_vision_needs_a_node_that_actually_carries_a_vision_model(self):
        capacity = self.capacity()
        self.assertEqual(capacity["vision"]["computers_array"], ["f13"])
        # An Avatar built from a picture is a Vision job, so it has the same
        # answer rather than a made-up one of its own.
        self.assertEqual(capacity["avatar_from_image"]["total_int"],
                         capacity["vision"]["total_int"])
        self.assertEqual(self.capacity(ai_models_by_node={})["vision"]["total_int"], 0)

    def test_frame_extraction_runs_here_and_says_so(self):
        frame = self.capacity()["video_frame"]
        self.assertEqual(frame["kind_string"], "local")
        self.assertEqual(frame["computers_array"], ["this server"])

    def test_qwen_image_counts_the_boxes_holding_a_gguf_behind_the_canny_token(self):
        servers = self.servers + [_server("Raptor", ["gen_image_control_canny.json"]),
                                  _server("f12", ["gen_image_control_canny.json"])]
        nodes = self.nodes + [{"id": "ryzen-server", "online": True, "busy": False},
                              {"id": "f12", "online": True, "busy": False}]
        catalogue = self.catalogue + [
            _checkpoint("qwen-image-2512-Q3_K_S.gguf", "qwen_image_generate.json",
                        "qwen_image", ["Raptor"]),
            _checkpoint("qwen-image-edit-2511-Q3_K_S.gguf", "qwen_image_edit.json",
                        "qwen_image", ["Raptor", "f12"])]
        capacity = pipelines.capacity_object(
            nodes, servers, ai_models_by_node={}, converters=self.converters,
            catalogue=catalogue)
        qwen = capacity["qwen_image"]
        held = qwen["checkpoints_object"]
        self.assertEqual(held["qwen-image-2512-Q3_K_S.gguf"]["computers_array"], ["Raptor"])
        self.assertEqual(held["qwen-image-edit-2511-Q3_K_S.gguf"]["computers_array"],
                         ["f12", "Raptor"])
        # No model chosen: whichever GGUF the mode picks, only a box holding
        # one can take the job, however many advertise the canny token.
        self.assertEqual(qwen["computers_array"], ["f12", "Raptor"])
        self.assertEqual(qwen["idle_int"], 2)

    def test_enhancement_nodes_share_the_canny_token(self):
        servers = self.servers + [_server("Raptor", ["gen_image_control_canny.json"])]
        nodes = self.nodes + [{"id": "ryzen-server", "online": True, "busy": False}]
        capacity = pipelines.capacity_object(
            nodes, servers, ai_models_by_node={}, converters=self.converters,
            catalogue=self.catalogue)
        for service_id in ("upscale", "detail_enhance", "face_fix"):
            self.assertEqual(capacity[service_id]["computers_array"], ["Raptor"])


if __name__ == "__main__":
    unittest.main()

import asyncio
import base64
import copy
import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx
import cv2
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware

from animation_fitting.specs import load_animation_fitting_specs
from animation_fitting.storage import StoredArtifact
from animation_fitting_routes import (
    MAX_REQUEST_BODY_BYTES,
    PIPELINE_VERSION,
    SEMANTIC_CAPTURE_SCHEMA,
    SEMANTIC_CONTRACT_SHA256,
    SEMANTIC_LABELS,
    SEMANTIC_PALETTE,
    SEMANTIC_PALETTE_SRGB,
    SEMANTIC_PROFILE_ID,
    SEMANTIC_RESOLUTION,
    SEMANTIC_RIG_TYPE,
    SemanticAnimationFittingStore,
    SingleProcessAnimationFittingExecutor,
    canonical_json_sha256,
    create_animation_fitting_router,
    register_animation_fitting_routes,
)


def _jpeg_bytes(marker=b"reference", *, width=768, height=448):
    image = np.zeros((int(height), int(width), 3), dtype=np.uint8)
    marker_digest = hashlib.sha256(bytes(marker)).digest()
    image[:8, :8, :] = np.frombuffer(marker_digest[:3], dtype=np.uint8)
    encoded, jpeg = cv2.imencode(
        ".jpg",
        image,
        [int(cv2.IMWRITE_JPEG_QUALITY), 95],
    )
    if not encoded:
        raise AssertionError("OpenCV failed to create the semantic JPEG fixture")
    return jpeg.tobytes()


def _semantic_metadata():
    polylines = {
        "fore_left": [[160, 130], [150, 250], [145, 390]],
        "fore_right": [[260, 130], [270, 250], [275, 390]],
        "hind_left": [[510, 130], [500, 250], [495, 390]],
        "hind_right": [[610, 130], [620, 250], [625, 390]],
    }
    return {
        "schema": SEMANTIC_CAPTURE_SCHEMA,
        "profile_id_string": SEMANTIC_PROFILE_ID,
        "rig_type_string": SEMANTIC_RIG_TYPE,
        "composition_string": "canonical_rgb_contain_with_semantic_bone_overlay",
        "source_resolution_array": [1024, 1024],
        "reference_resolution_array": list(SEMANTIC_RESOLUTION),
        "viewer_contain_object": {
            "scale_float": 0.4375,
            "offset_x_float": 160.0,
            "offset_y_float": 0.0,
            "draw_width_float": 448.0,
            "draw_height_float": 448.0,
        },
        "semantic_legend_object": {
            "color_space_string": "linear_rgb",
            "labels_array": list(SEMANTIC_LABELS),
            "palette_linear_object": {
                label: list(SEMANTIC_PALETTE[label]) for label in SEMANTIC_LABELS
            },
            "palette_srgb_byte_object": {
                label: list(SEMANTIC_PALETTE_SRGB[label]) for label in SEMANTIC_LABELS
            },
        },
        "overlay_object": {
            "underlay_srgb_byte_array": [178, 185, 195],
            "underlay_width_px_float": 34,
            "semantic_width_px_float": 20,
            "line_cap_string": "round",
            "line_join_string": "round",
            "jpeg_quality_float": 1,
            "polylines_object": polylines,
        },
    }


def _job_body(action_id, *, jpeg=None, metadata=None):
    jpeg = bytes(jpeg if jpeg is not None else _jpeg_bytes())
    metadata = copy.deepcopy(metadata if metadata is not None else _semantic_metadata())
    return {
        "pipeline_version_string": PIPELINE_VERSION,
        "action_id_string": action_id,
        "semantic_capture_object": {
            "frame_jpeg_data_url_string": (
                "data:image/jpeg;base64," + base64.b64encode(jpeg).decode("ascii")
            ),
            "frame_jpeg_sha256_string": hashlib.sha256(jpeg).hexdigest(),
            "metadata_object": metadata,
            "metadata_sha256_string": canonical_json_sha256(metadata),
            "semantic_contract_sha256_string": SEMANTIC_CONTRACT_SHA256,
        },
    }


class _FakeDurableSubmitter:
    def __init__(self):
        self.submissions = []
        self.accept_calls = []

    async def __call__(self, job_id, handler):
        self.accept_calls.append(job_id)
        if not any(existing_id == job_id for existing_id, _ in self.submissions):
            self.submissions.append((job_id, handler))

    async def drain(self):
        pending, self.submissions = self.submissions, []
        for job_id, handler in pending:
            await handler(job_id)


class _FakeOrchestrator:
    def __init__(self, store, specs):
        self.store = store
        self.specs = specs
        self.calls = []
        self.videos = {}
        self.active_runs = 0
        self.max_concurrent_runs = 0
        self.execution_order = []

    async def run_candidate(self, **kwargs):
        action = self.specs.action(kwargs["action_id"])
        worker = kwargs["worker"]
        self.calls.append({**kwargs, "worker_mode_string": worker.mode})
        self.active_runs += 1
        self.max_concurrent_runs = max(self.max_concurrent_runs, self.active_runs)
        self.execution_order.append(f"start:{action.action_id}")
        try:
            await asyncio.sleep(0.01)
            video_bytes = (
                b"\x00\x00\x00\x18ftypmp42semantic-"
                + action.action_id.encode("ascii")
                + bytes(range(32))
            )
            raw_video = self.store.artifacts.store_raw_video(video_bytes)
            self.videos[action.action_id] = video_bytes
            frames = tuple(
                StoredArtifact(
                    sha256=hashlib.sha256(f"{action.action_id}:{index}".encode()).hexdigest(),
                    path=self.store.artifacts.frames_root / action.action_id / f"{index}.png",
                    size_bytes=1,
                )
                for index in range(action.frame_count)
            )
            return SimpleNamespace(
                job_id=f"orchestrator-{action.action_id}",
                prompt_id=f"prompt-{action.action_id}",
                raw_video=raw_video,
                frames=frames,
            )
        finally:
            self.execution_order.append(f"end:{action.action_id}")
            self.active_runs -= 1


class AnimationFittingRoutesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store_root = Path(self.temp_dir.name) / "semantic-store"
        self.store = SemanticAnimationFittingStore(self.store_root)
        self.specs = load_animation_fitting_specs()
        self.submitter = _FakeDurableSubmitter()
        self.orchestrator = _FakeOrchestrator(self.store, self.specs)
        self.tasks = {
            "animal-a": SimpleNamespace(
                id="animal-a",
                input_type="animal",
                owner_id="alice",
                animal_type="horse",
            ),
            "animal-b": SimpleNamespace(
                id="animal-b",
                input_type="animal",
                owner_id="bob",
                animal_type="horse",
            ),
            "character-a": SimpleNamespace(
                id="character-a",
                input_type="character",
                owner_id="alice",
            ),
        }

        async def get_task(task_id):
            return self.tasks.get(task_id)

        async def authorize_task(request, task):
            identity = request.headers.get("x-test-identity", "").strip()
            if not identity:
                raise HTTPException(status_code=401, detail="Authentication required")
            if identity != "admin" and identity != task.owner_id:
                raise HTTPException(status_code=403, detail="Task access denied")

        self.get_task = get_task
        self.authorize_task = authorize_task

        app = FastAPI()
        app.add_middleware(GZipMiddleware, minimum_size=1)
        router = create_animation_fitting_router(
            get_task=get_task,
            authorize_task=authorize_task,
            submit_job=self.submitter,
            store=self.store,
            orchestrator=self.orchestrator,
            worker_for_mode=lambda mode: SimpleNamespace(mode=mode),
            specs=self.specs,
        )
        self.service = getattr(router, "animation_fitting_service")
        app.include_router(router)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        )

    async def asyncTearDown(self):
        await self.client.aclose()
        self.temp_dir.cleanup()

    @staticmethod
    def _headers(identity="alice", **extra):
        return {"x-test-identity": identity, **extra}

    async def _post(self, action_id, *, body=None, identity="alice", task_id="animal-a"):
        return await self.client.post(
            f"/api/task/{task_id}/animation-fitting/v1/jobs",
            headers=self._headers(identity),
            json=body if body is not None else _job_body(action_id),
        )

    async def test_capabilities_enforce_auth_animal_scope_and_do_not_mutate_store(self):
        url = "/api/task/animal-a/animation-fitting/v1/capabilities"
        self.assertFalse(self.store_root.exists())

        missing_identity = await self.client.get(url)
        self.assertEqual(missing_identity.status_code, 401)
        foreign = await self.client.get(url, headers=self._headers("bob"))
        self.assertEqual(foreign.status_code, 403)
        missing_task = await self.client.get(
            "/api/task/missing/animation-fitting/v1/capabilities",
            headers=self._headers(),
        )
        self.assertEqual(missing_task.status_code, 404)
        non_animal = await self.client.get(
            "/api/task/character-a/animation-fitting/v1/capabilities",
            headers=self._headers(),
        )
        self.assertEqual(non_animal.status_code, 400)

        owner = await self.client.get(url, headers=self._headers())
        admin = await self.client.get(url, headers=self._headers("admin"))
        self.assertEqual(owner.status_code, 200)
        self.assertEqual(admin.status_code, 200)
        payload = owner.json()
        self.assertEqual(payload["pipeline_version_string"], PIPELINE_VERSION)
        self.assertFalse(payload["mutates_legacy_idle_ltx_bool"])
        self.assertEqual(len(payload["actions_array"]), 30)
        actions = {row["action_id_string"]: row for row in payload["actions_array"]}
        self.assertEqual(
            (
                actions["walk_forward"]["generation_mode_string"],
                actions["walk_forward"]["frame_count_int"],
            ),
            ("loop", 49),
        )
        self.assertEqual(
            (actions["death"]["generation_mode_string"], actions["death"]["frame_count_int"]),
            ("one_shot", 65),
        )
        self.assertFalse(self.store_root.exists(), "capability GET must not create store files")

    async def test_post_rejects_unknown_version_action_jpeg_metadata_and_caller_overrides(self):
        cases = []

        unknown_version = _job_body("walk_forward")
        unknown_version["pipeline_version_string"] = "semantic-comfy-browser-v2"
        cases.append(unknown_version)

        cases.append(_job_body("not_an_action"))

        bad_jpeg = _job_body("walk_forward")
        bad_bytes = b"not-a-jpeg"
        capture = bad_jpeg["semantic_capture_object"]
        capture["frame_jpeg_data_url_string"] = (
            "data:image/jpeg;base64," + base64.b64encode(bad_bytes).decode("ascii")
        )
        capture["frame_jpeg_sha256_string"] = hashlib.sha256(bad_bytes).hexdigest()
        cases.append(bad_jpeg)

        bad_jpeg_sha = _job_body("walk_forward")
        bad_jpeg_sha["semantic_capture_object"]["frame_jpeg_sha256_string"] = "0" * 64
        cases.append(bad_jpeg_sha)

        wrong_jpeg_resolution = _job_body(
            "walk_forward",
            jpeg=_jpeg_bytes(b"wrong-size", width=512, height=512),
        )
        cases.append(wrong_jpeg_resolution)

        bad_profile = _job_body("walk_forward")
        bad_profile["semantic_capture_object"]["metadata_object"]["profile_id_string"] = "horse_2.other"
        bad_profile["semantic_capture_object"]["metadata_sha256_string"] = canonical_json_sha256(
            bad_profile["semantic_capture_object"]["metadata_object"]
        )
        cases.append(bad_profile)

        bad_resolution = _job_body("walk_forward")
        bad_resolution["semantic_capture_object"]["metadata_object"]["reference_resolution_array"] = [512, 512]
        bad_resolution["semantic_capture_object"]["metadata_sha256_string"] = canonical_json_sha256(
            bad_resolution["semantic_capture_object"]["metadata_object"]
        )
        cases.append(bad_resolution)

        bad_palette = _job_body("walk_forward")
        bad_palette["semantic_capture_object"]["metadata_object"]["semantic_legend_object"][
            "palette_linear_object"
        ]["fore_left"] = [1, 1, 1]
        bad_palette["semantic_capture_object"]["metadata_sha256_string"] = canonical_json_sha256(
            bad_palette["semantic_capture_object"]["metadata_object"]
        )
        cases.append(bad_palette)

        bad_point = _job_body("walk_forward")
        bad_point["semantic_capture_object"]["metadata_object"]["overlay_object"][
            "polylines_object"
        ]["fore_left"][0] = [float("inf"), 10]
        # JSON cannot carry infinity through the ASGI client, so use an invalid
        # dimensional point to cover the same server-side geometry gate.
        bad_point["semantic_capture_object"]["metadata_object"]["overlay_object"][
            "polylines_object"
        ]["fore_left"][0] = [10]
        bad_point["semantic_capture_object"]["metadata_sha256_string"] = canonical_json_sha256(
            bad_point["semantic_capture_object"]["metadata_object"]
        )
        cases.append(bad_point)

        bad_metadata_sha = _job_body("walk_forward")
        bad_metadata_sha["semantic_capture_object"]["metadata_sha256_string"] = "f" * 64
        cases.append(bad_metadata_sha)

        bad_contract_sha = _job_body("walk_forward")
        bad_contract_sha["semantic_capture_object"]["semantic_contract_sha256_string"] = "e" * 64
        cases.append(bad_contract_sha)

        caller_frame_override = _job_body("walk_forward")
        caller_frame_override["frame_count_int"] = 1
        cases.append(caller_frame_override)

        caller_mode_override = _job_body("death")
        caller_mode_override["generation_mode_string"] = "loop"
        cases.append(caller_mode_override)

        numeric_motion_notes = _job_body("walk_forward")
        numeric_motion_notes["motion_notes_string"] = 123
        cases.append(numeric_motion_notes)

        too_many_points = _job_body("walk_forward")
        too_many_points["semantic_capture_object"]["metadata_object"]["overlay_object"][
            "polylines_object"
        ]["fore_left"] = [[index, index] for index in range(65)]
        too_many_points["semantic_capture_object"]["metadata_sha256_string"] = canonical_json_sha256(
            too_many_points["semantic_capture_object"]["metadata_object"]
        )
        cases.append(too_many_points)

        for index, body in enumerate(cases):
            with self.subTest(case=index):
                response = await self._post("walk_forward", body=body)
                self.assertEqual(response.status_code, 400, response.text)

        self.assertEqual(self.submitter.submissions, [])
        self.assertFalse(self.store_root.exists(), "invalid requests must fail before store mutation")

    async def test_identical_request_is_idempotent_and_metadata_collision_fails_closed(self):
        body = _job_body("walk_forward")
        first = await self._post("walk_forward", body=body)
        duplicate = await self._post("walk_forward", body=copy.deepcopy(body))
        self.assertEqual(first.status_code, 202, first.text)
        self.assertEqual(duplicate.status_code, 200, duplicate.text)
        first_json, duplicate_json = first.json(), duplicate.json()
        self.assertEqual(first_json["job_id_string"], duplicate_json["job_id_string"])
        self.assertFalse(first_json["idempotent_replay_bool"])
        self.assertTrue(duplicate_json["idempotent_replay_bool"])
        self.assertEqual(len(self.submitter.submissions), 1)
        self.assertEqual(len(list(self.store.requests_root.glob("*.json"))), 1)
        self.assertFalse(any(self.store_root.rglob("idle_ltx_references.json")))

        changed_metadata = copy.deepcopy(body)
        points = changed_metadata["semantic_capture_object"]["metadata_object"][
            "overlay_object"
        ]["polylines_object"]["fore_left"]
        points[0] = [points[0][0] + 1, points[0][1]]
        changed_metadata["semantic_capture_object"]["metadata_sha256_string"] = canonical_json_sha256(
            changed_metadata["semantic_capture_object"]["metadata_object"]
        )
        conflict = await self._post("walk_forward", body=changed_metadata)
        self.assertEqual(conflict.status_code, 409, conflict.text)
        self.assertEqual(len(self.submitter.submissions), 1)

    async def test_crash_window_is_startup_recoverable_and_double_delivery_runs_once(self):
        body = _job_body("walk_forward", jpeg=_jpeg_bytes(b"crash-recovery"))
        original_append = self.store.append_job_state

        class SimulatedProcessCrash(BaseException):
            pass

        def crash_before_queued_state(job_id, payload):
            self.store.append_job_state = original_append
            raise SimulatedProcessCrash()

        self.store.append_job_state = crash_before_queued_state
        with self.assertRaises(SimulatedProcessCrash):
            await self.service.create_job("animal-a", self.tasks["animal-a"], body)
        self.assertEqual(self.submitter.submissions, [])
        self.assertEqual(len(list(self.store.requests_root.glob("*.json"))), 1)

        fresh_orchestrator = _FakeOrchestrator(self.store, self.specs)
        fresh_router = create_animation_fitting_router(
            get_task=self.get_task,
            authorize_task=self.authorize_task,
            store=self.store,
            orchestrator=fresh_orchestrator,
            worker_for_mode=lambda mode: SimpleNamespace(mode=mode),
            specs=self.specs,
        )
        fresh_service = getattr(fresh_router, "animation_fitting_service")
        fresh_executor = getattr(fresh_router, "animation_fitting_executor")
        self.assertIsInstance(fresh_executor, SingleProcessAnimationFittingExecutor)
        recovered = await fresh_service.recover_jobs()
        self.assertEqual(len(recovered), 1)
        await fresh_executor.wait_for_idle()
        self.assertEqual(await fresh_executor.active_job_ids(), ())
        self.assertEqual(len(fresh_orchestrator.calls), 1)

        retry = await self._post("walk_forward", body=body)
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertTrue(retry.json()["idempotent_replay_bool"])
        self.assertEqual(self.submitter.submissions, [])

        job_id = recovered[0]
        await asyncio.gather(fresh_service.run_job(job_id), fresh_service.run_job(job_id))
        self.assertEqual(len(fresh_orchestrator.calls), 1)
        self.assertEqual(self.store.latest_job_state(job_id)["status_string"], "ready")

    async def test_default_executor_concurrent_posts_run_once_and_cleanup(self):
        store = SemanticAnimationFittingStore(Path(self.temp_dir.name) / "default-executor-store")
        orchestrator = _FakeOrchestrator(store, self.specs)
        app = FastAPI()
        router = create_animation_fitting_router(
            get_task=self.get_task,
            authorize_task=self.authorize_task,
            store=store,
            orchestrator=orchestrator,
            worker_for_mode=lambda mode: SimpleNamespace(mode=mode),
            specs=self.specs,
        )
        service = getattr(router, "animation_fitting_service")
        executor = getattr(router, "animation_fitting_executor")
        self.assertIsInstance(executor, SingleProcessAnimationFittingExecutor)
        app.include_router(router)
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://executor-test",
        )
        try:
            body = _job_body("run", jpeg=_jpeg_bytes(b"executor-concurrent"))
            first, second = await asyncio.gather(
                client.post(
                    "/api/task/animal-a/animation-fitting/v1/jobs",
                    headers=self._headers(),
                    json=body,
                ),
                client.post(
                    "/api/task/animal-a/animation-fitting/v1/jobs",
                    headers=self._headers(),
                    json=copy.deepcopy(body),
                ),
            )
            self.assertEqual(sorted((first.status_code, second.status_code)), [200, 202])
            await executor.wait_for_idle()
            self.assertEqual(len(orchestrator.calls), 1)
            self.assertEqual(await executor.active_job_ids(), ())
            self.assertEqual(executor.last_errors, {})

            job_id = first.json()["job_id_string"]
            self.assertTrue(await executor.submit(job_id, service.run_job))
            await executor.wait_for_idle()
            self.assertEqual(len(orchestrator.calls), 1, "terminal ready state skips redelivery")
            self.assertEqual(await executor.active_job_ids(), ())

            walk_body = _job_body(
                "walk_forward",
                jpeg=_jpeg_bytes(b"executor-walk"),
            )
            death_body = _job_body(
                "death",
                jpeg=_jpeg_bytes(b"executor-death"),
            )
            walk, death = await asyncio.gather(
                client.post(
                    "/api/task/animal-a/animation-fitting/v1/jobs",
                    headers=self._headers(),
                    json=walk_body,
                ),
                client.post(
                    "/api/task/animal-a/animation-fitting/v1/jobs",
                    headers=self._headers(),
                    json=death_body,
                ),
            )
            self.assertEqual((walk.status_code, death.status_code), (202, 202))
            await executor.wait_for_idle()
            self.assertEqual(orchestrator.max_concurrent_runs, 1)
            self.assertEqual(
                {row["action_id"] for row in orchestrator.calls},
                {"run", "walk_forward", "death"},
            )
            self.assertEqual(
                service.job_payload("animal-a", walk.json()["job_id_string"])["status_string"],
                "ready",
            )
            self.assertEqual(
                service.job_payload("animal-a", death.json()["job_id_string"])["status_string"],
                "ready",
            )
            self.assertEqual(await executor.active_job_ids(), ())
            self.assertEqual(executor.last_errors, {})
        finally:
            await client.aclose()

    async def test_default_executor_deduplicates_active_job_consumes_errors_and_reuses_key(self):
        executor = SingleProcessAnimationFittingExecutor()
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def blocked_handler(job_id):
            calls.append(job_id)
            entered.set()
            await release.wait()

        first, duplicate = await asyncio.gather(
            executor.submit("job-key", blocked_handler),
            executor.submit("job-key", blocked_handler),
        )
        self.assertEqual(sorted((first, duplicate)), [False, True])
        await entered.wait()
        self.assertEqual(await executor.active_job_ids(), ("job-key",))
        self.assertEqual(calls, ["job-key"])
        release.set()
        await executor.wait_for_idle()
        self.assertEqual(await executor.active_job_ids(), ())

        async def failing_handler(job_id):
            raise RuntimeError(f"boom {job_id}")

        self.assertTrue(await executor.submit("job-key", failing_handler))
        await executor.wait_for_idle()
        self.assertEqual(await executor.active_job_ids(), ())
        self.assertIn("RuntimeError: boom job-key", executor.last_errors["job-key"])

    async def test_register_exposes_default_executor_and_recovery_service_on_app_state(self):
        store = SemanticAnimationFittingStore(Path(self.temp_dir.name) / "registered-store")
        app = FastAPI()
        router = register_animation_fitting_routes(
            app,
            get_task=self.get_task,
            authorize_task=self.authorize_task,
            store=store,
            orchestrator=_FakeOrchestrator(store, self.specs),
            worker_for_mode=lambda mode: SimpleNamespace(mode=mode),
            specs=self.specs,
        )
        service = getattr(router, "animation_fitting_service")
        executor = getattr(router, "animation_fitting_executor")
        self.assertIs(app.state.animation_fitting_route_service, service)
        self.assertIs(app.state.animation_fitting_executor, executor)
        self.assertIs(service.executor, executor)
        self.assertIsInstance(executor, SingleProcessAnimationFittingExecutor)

    async def test_request_body_limit_is_enforced_before_json_allocation(self):
        response = await self.client.post(
            "/api/task/animal-a/animation-fitting/v1/jobs",
            headers=self._headers(),
            content=b"{" + (b" " * MAX_REQUEST_BODY_BYTES),
        )
        self.assertEqual(response.status_code, 413)

        async def chunked_oversize_body():
            midpoint = MAX_REQUEST_BODY_BYTES // 2
            yield b"{" + (b" " * midpoint)
            yield b" " * (MAX_REQUEST_BODY_BYTES - midpoint)

        chunked = await self.client.post(
            "/api/task/animal-a/animation-fitting/v1/jobs",
            headers=self._headers(),
            content=chunked_oversize_body(),
        )
        self.assertEqual(chunked.status_code, 413)
        self.assertEqual(self.submitter.submissions, [])

    async def test_loop_and_one_shot_are_derived_from_specs_and_run_through_orchestrator(self):
        walk = await self._post(
            "walk_forward",
            body=_job_body("walk_forward", jpeg=_jpeg_bytes(b"walk")),
        )
        death = await self._post("death", body=_job_body("death", jpeg=_jpeg_bytes(b"death")))
        self.assertEqual((walk.status_code, death.status_code), (202, 202))

        loop_frames = self.specs.workflow_for_action("walk_forward").conditioned_frames
        one_shot_frames = self.specs.workflow_for_action("death").conditioned_frames
        self.assertEqual([row["role_string"] for row in loop_frames], ["start", "end"])
        self.assertTrue(loop_frames[1]["reuse_start_image_bool"])
        self.assertEqual([row["role_string"] for row in one_shot_frames], ["start"])

        await self.submitter.drain()
        calls = {row["action_id"]: row for row in self.orchestrator.calls}
        self.assertEqual(calls["walk_forward"]["worker_mode_string"], "loop")
        self.assertEqual(calls["death"]["worker_mode_string"], "one_shot")
        self.assertEqual(calls["walk_forward"]["candidate_index"], 0)
        self.assertEqual(calls["death"]["species"], "horse")

        walk_status = await self.client.get(
            f"/api/task/animal-a/animation-fitting/v1/jobs/{walk.json()['job_id_string']}",
            headers=self._headers(),
        )
        death_status = await self.client.get(
            f"/api/task/animal-a/animation-fitting/v1/jobs/{death.json()['job_id_string']}",
            headers=self._headers(),
        )
        self.assertEqual(walk_status.json()["status_string"], "ready")
        self.assertEqual(walk_status.json()["frame_count_int"], 49)
        self.assertEqual(death_status.json()["status_string"], "ready")
        self.assertEqual(death_status.json()["frame_count_int"], 65)

    async def test_status_and_seekable_video_are_owner_scoped_and_integrity_pinned(self):
        created = await self._post("run", body=_job_body("run", jpeg=_jpeg_bytes(b"run")))
        self.assertEqual(created.status_code, 202, created.text)
        job_id = created.json()["job_id_string"]
        status_url = f"/api/task/animal-a/animation-fitting/v1/jobs/{job_id}"
        video_url = status_url + "/video"

        pending_video = await self.client.get(video_url, headers=self._headers())
        self.assertEqual(pending_video.status_code, 409)
        await self.submitter.drain()

        foreign = await self.client.get(status_url, headers=self._headers("bob"))
        self.assertEqual(foreign.status_code, 403)
        cross_task = await self.client.get(
            f"/api/task/animal-b/animation-fitting/v1/jobs/{job_id}",
            headers=self._headers("bob"),
        )
        self.assertEqual(cross_task.status_code, 404)

        status = await self.client.get(status_url, headers=self._headers())
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["status_string"], "ready")
        self.assertNotIn("raw_video_path_string", status.text)
        expected = self.orchestrator.videos["run"]
        digest = hashlib.sha256(expected).hexdigest()

        full = await self.client.get(video_url, headers=self._headers())
        self.assertEqual(full.status_code, 200)
        self.assertEqual(full.content, expected)
        self.assertEqual(full.headers["accept-ranges"], "bytes")
        self.assertEqual(full.headers["content-length"], str(len(expected)))
        self.assertEqual(full.headers["content-encoding"], "identity")
        self.assertEqual(full.headers["etag"], f'"{digest}"')

        ranges = [
            ("bytes=2-5", 2, 5),
            ("bytes=6-", 6, len(expected) - 1),
            ("bytes=-4", len(expected) - 4, len(expected) - 1),
        ]
        for range_header, start, end in ranges:
            with self.subTest(range_header=range_header):
                response = await self.client.get(
                    video_url,
                    headers=self._headers(range=range_header),
                )
                self.assertEqual(response.status_code, 206, response.text)
                self.assertEqual(response.content, expected[start:end + 1])
                self.assertEqual(
                    response.headers["content-range"],
                    f"bytes {start}-{end}/{len(expected)}",
                )
                self.assertEqual(response.headers["content-length"], str(end - start + 1))
                self.assertEqual(response.headers["content-encoding"], "identity")

        for range_header in ("bytes=0-1,3-4", "bytes=999-", "bytes=-0", "bytes=abc"):
            with self.subTest(invalid_range=range_header):
                response = await self.client.get(
                    video_url,
                    headers=self._headers(range=range_header),
                )
                self.assertEqual(response.status_code, 416)
                self.assertEqual(response.headers["content-range"], f"bytes */{len(expected)}")

        artifact = self.store.ready_video(job_id)
        artifact.path.write_bytes(b"corrupt")
        corrupt = await self.client.get(video_url, headers=self._headers())
        self.assertEqual(corrupt.status_code, 409)


if __name__ == "__main__":
    unittest.main()

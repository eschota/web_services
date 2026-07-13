import json
import tempfile
import unittest
from pathlib import Path

from animation_fitting.comfy import (
    ComfyOutputFile,
    ComfySubmission,
    ComfyWorker,
    deterministic_prompt_id,
    workflow_fingerprint,
)
from animation_fitting.orchestrator import AnimationFittingOrchestrator, CandidateAssessment
from animation_fitting.specs import load_animation_fitting_specs
from animation_fitting.storage import ImmutableArtifactStore


def _api_prompt_for(workflow):
    path = (
        Path(__file__).resolve().parents[1]
        / "animation_fitting"
        / "specs"
        / "workflows"
        / workflow.workflow_name
    )
    return json.loads(path.read_text(encoding="utf-8"))


class _FakeFrameExtractor:
    def extract_and_store(self, raw_video, store, *, expected_frame_count):
        return tuple(
            store.store_frame(raw_video.sha256, index, b"png" + index.to_bytes(4, "big"))
            for index in range(expected_frame_count)
        )


class _FakeComfyClient:
    def __init__(self, workflow, api_prompt):
        self.workflow = workflow
        self.api_prompt = api_prompt
        self.bound_prompt = None
        self.closed = False
        self.idempotency_key = ""

    async def fetch_api_workflow(self):
        return self.api_prompt, workflow_fingerprint(self.api_prompt)

    async def queue_load(self):
        return 0

    async def upload_reference_image(self, path):
        return "autorig_animation_fitting/reference.png"

    async def submit(self, prompt, idempotency_key):
        self.bound_prompt = prompt
        self.idempotency_key = idempotency_key
        return ComfySubmission(
            prompt_id=deterministic_prompt_id(idempotency_key),
            client_id="test-client",
            resumed_existing_bool=False,
        )

    async def wait_for_output(self, prompt_id):
        return {}, ComfyOutputFile("candidate.mp4", "animation_fitting", "output")

    async def download_output(self, output):
        return b"raw-mp4-output" * 8

    async def close(self):
        self.closed = True


class AnimationFittingCandidatePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.orchestrator = AnimationFittingOrchestrator(
            ImmutableArtifactStore(Path(self.temp_dir.name))
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_initial_eight_are_deterministic_and_retry_stops_at_sixteen(self):
        first = self.orchestrator.initial_candidate_plan("task-1", "run")
        second = self.orchestrator.initial_candidate_plan("task-1", "run")
        self.assertEqual(first, second)
        self.assertEqual(tuple(item.candidate_index for item in first), tuple(range(8)))
        rejected = tuple(
            CandidateAssessment(item.candidate_id, item.candidate_index, False, 0.2, ("gate",))
            for item in first
        )
        retry = self.orchestrator.next_candidate_plan("task-1", "run", rejected)
        self.assertEqual(tuple(item.candidate_index for item in retry), tuple(range(8, 16)))
        rejected_all = rejected + tuple(
            CandidateAssessment(item.candidate_id, item.candidate_index, False, 0.2, ("gate",))
            for item in retry
        )
        self.assertEqual(self.orchestrator.next_candidate_plan("task-1", "run", rejected_all), ())

    def test_three_accepted_candidates_stop_retry_and_top3_are_deterministic(self):
        plans = self.orchestrator.initial_candidate_plan("task-2", "idle_neutral")
        assessments = tuple(
            CandidateAssessment(
                item.candidate_id,
                item.candidate_index,
                item.candidate_index < 3,
                0.5 if item.candidate_index != 1 else 0.9,
                (),
            )
            for item in plans
        )
        self.assertEqual(self.orchestrator.next_candidate_plan("task-2", "idle_neutral", assessments), ())
        self.assertEqual(
            tuple(item.candidate_index for item in self.orchestrator.top_candidates(assessments)),
            (1, 0, 2),
        )

    def test_loop_seam_gate_applies_only_to_loop_actions(self):
        loop_plan = self.orchestrator.initial_candidate_plan("task-3", "run")[0]
        one_shot_plan = self.orchestrator.initial_candidate_plan("task-3", "death")[0]
        common = {
            key: True for key in self.orchestrator.specs.qa.hard_gate_metric_keys
        }
        loop_assessment = self.orchestrator.assess_candidate(loop_plan, common)
        one_shot_assessment = self.orchestrator.assess_candidate(one_shot_plan, common)
        self.assertFalse(loop_assessment.accepted_bool)
        self.assertIn("loop_seam_ok", loop_assessment.failed_gates)
        self.assertTrue(one_shot_assessment.accepted_bool)


class AnimationFittingRunCandidateTests(unittest.IsolatedAsyncioTestCase):
    async def test_loop_and_one_shot_persist_route_and_immutable_artifacts(self):
        specs = load_animation_fitting_specs()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            reference = temp_root / "horse.png"
            reference.write_bytes(b"reference-png")
            clients = []

            def factory(worker):
                profile = specs.workflows[
                    "loop" if "_loop_" in worker.workflow_name else "one_shot"
                ]
                client = _FakeComfyClient(profile, _api_prompt_for(profile))
                clients.append(client)
                return client

            store = ImmutableArtifactStore(temp_root / "artifacts")
            orchestrator = AnimationFittingOrchestrator(
                store,
                specs=specs,
                frame_extractor=_FakeFrameExtractor(),
                client_factory=factory,
            )

            results = []
            for action_id in ("walk_forward", "turn_left_90"):
                profile = specs.workflow_for_action(action_id)
                api_prompt = _api_prompt_for(profile)
                worker = ComfyWorker(
                    "local-4090",
                    "http://127.0.0.1:8188",
                    profile.workflow_name,
                    workflow_fingerprint(api_prompt),
                )
                result = await orchestrator.run_candidate(
                    task_id="horse-task",
                    action_id=action_id,
                    candidate_index=0,
                    species="horse",
                    reference_frame_path=reference,
                    worker=worker,
                )
                results.append(result)
                state = store.latest_job_state(result.job_id)
                self.assertEqual(state["status_string"], "completed")
                self.assertEqual(state["worker_base_url_string"], worker.base_url)
                self.assertEqual(state["workflow_name_string"], worker.workflow_name)
                self.assertEqual(
                    state["workflow_fingerprint_string"], worker.expected_workflow_fingerprint
                )
                self.assertEqual(state["prompt_id_string"], result.prompt_id)
                self.assertEqual(state["raw_video_sha256_string"], result.raw_video.sha256)
                self.assertEqual(len(result.frames), specs.action(action_id).frame_count)
                self.assertEqual(len(list((store.jobs_root / result.job_id).glob("*.json"))), 3)

            loop_prompt = clients[0].bound_prompt
            loop_guides = [
                node
                for node in loop_prompt.values()
                if node["class_type"] == "LTXVAddGuide"
            ]
            self.assertEqual(len(loop_guides), 2)
            self.assertEqual(
                {node["inputs"]["frame_idx"] for node in loop_guides},
                {-1, 0},
            )
            self.assertEqual(
                len({tuple(node["inputs"]["image"]) for node in loop_guides}),
                1,
            )
            self.assertFalse(
                any(
                    node.get("class_type") == "LTXVAddGuide"
                    and node.get("inputs", {}).get("frame_idx") == -1
                    for node in clients[1].bound_prompt.values()
                )
            )
            self.assertTrue(all(client.closed for client in clients))
            self.assertTrue(all(result.raw_video.path.exists() for result in results))
            self.assertFalse(any(store.worker_locks_root.glob("*.lock")))


if __name__ == "__main__":
    unittest.main()

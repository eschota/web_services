import json
import multiprocessing
import pathlib
import tempfile
import unittest

import equine_library_release as registry


SHA_A = "a" * 64
SHA_B = "b" * 64
TASK_A = "11111111-1111-4111-8111-111111111111"
TASK_B = "22222222-2222-4222-8222-222222222222"


def entries():
    rows = []
    for action in registry.BASE_ACTIONS:
        contract = registry.ACTION_BY_ID[action]
        frames = contract[3]
        translations = [[i / max(1, frames - 1), 0, 0] for i in range(frames)]
        quaternions = [[0, 0, 0, 1] for _ in range(frames)]
        row = {
            "clip": action,
            "quality": "ok",
            "loop": action in registry.LOOP_ACTIONS,
            "type": "loop" if action in registry.LOOP_ACTIONS else "one_shot",
            "qa": qa(action),
            "action_metadata": {"action": action, "category": contract[1], "frame_count": frames,
                                "start_pose": contract[4], "end_pose": contract[5],
                                "fps": 30.0, "duration_s": (frames - 1) / 30.0,
                                "coordinate_system": "right_handed_y_up", "track_space": "local"},
            "reference_skeleton": {
                "height": 2.0, "ground_baseline": 0.0, "forward_axis": [0, 0, 1],
                "bones": [
                    {"name": "root", "parent": None, "rest_translation": [0, 1, 0],
                     "rest_rotation": [0, 0, 0, 1], "rest_scale": [1, 1, 1],
                     "head": [0, 0, 0], "tail": [0, 1, 0]},
                    {"name": "spine", "parent": "root", "rest_translation": [0, 1, 0],
                     "rest_rotation": [0, 0, 0, 1], "rest_scale": [1, 1, 1],
                     "head": [0, 1, 0], "tail": [0, 2, 0]},
                ],
            },
            "tracks": {
                "root.translation": {"bone": "root", "property": "translation", "type": "translation", "values": translations},
                "spine.rotation": {"bone": "spine", "property": "rotation", "type": "quaternion", "values": quaternions},
            },
        }
        if action in registry.LOCOMOTION_ACTIONS:
            row["gait_qa"] = gait_qa()
        rows.append(row)
    return rows


def qa(action):
    return {"schema": "equid.qa.v1", "pass": True,
            "gates": {gate: True for gate in registry.ACTION_QA_GATES[action]},
            "metrics": {"stretch_ratio": 1.01, "penetration_ratio": 0.0}}


def gait_qa():
    return {"schema": "equid.gait-qa.v1", "pass": True,
            "gates": {"contacts": True, "timing": True},
            "metrics": {"duty_factor": 0.5, "phase_error": 0.01}}


def task_evidence(release, task=TASK_A, rig=SHA_B, media_count=1):
    clips = {}
    for name in release["required_actions"]:
        clips[name] = {"qa": qa(name)}
        if name in registry.LOCOMOTION_ACTIONS:
            clips[name]["gait_qa"] = gait_qa()
    return {
        "task_id": task,
        "run_id": "run-1",
        "rig_glb_sha256": rig,
        "release_hash": release["release_hash"],
        "retarget_sha256": release["retarget_sha256"],
        "clips": clips,
        "media_receipts": [
            {"uid": f"media-{task[:8]}-{i}", "agent": "equine", "send_receipt": f"sent-{task[:8]}-{i}"}
            for i in range(media_count)
        ],
    }


def verdict(evidence, media_uid, text="Принять ✅", inbox="inbox-1", uid="verdict-1", user_id="owner-1"):
    return {"inbox_id": inbox, "uid": uid, "iso": "2026-10-13T10:00:00Z", "author_kind": "user",
            "user_id": user_id, "user_name": "Owner", "kind": "verdict", "text": text,
            "about": {"uid": media_uid, "agent": "equine", "kind": "media", "text": "task validation",
                      "media_url": "https://autorig.online/media/example.mp4"}}


class FakeResponse:
    def __init__(self, value):
        self.data = json.dumps(value).encode()
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self): return self.data


def create_worker(root, queue):
    try:
        queue.put(registry.create_candidate(root, entries(), [TASK_A], SHA_A)["release_hash"])
    except Exception as exc:
        queue.put(type(exc).__name__ + ":" + str(exc))


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=str(pathlib.Path(__file__).parent))
        self.root = pathlib.Path(self.tmp.name) / "state"
    def tearDown(self):
        self.tmp.cleanup()

    def candidate(self, tasks=(TASK_A,)):
        return registry.create_candidate(self.root, entries(), tasks, SHA_A)

    def test_rejects_missing_partial_and_quality_null(self):
        with self.assertRaisesRegex(registry.RegistryError, "missing required"):
            registry.create_candidate(self.root, entries()[:-1], [TASK_A], SHA_A)
        bad = entries(); bad[0]["quality"] = None
        with self.assertRaisesRegex(registry.RegistryError, "exactly 'ok'"):
            registry.create_candidate(self.root, bad, [TASK_A], SHA_A)
        bad = entries(); bad[0]["tracks"]["root.translation"]["values"] = [[0, 0, 0]]
        with self.assertRaisesRegex(registry.RegistryError, "track length"):
            registry.create_candidate(self.root, bad, [TASK_A], SHA_A)

    def test_huge_metrics_do_not_self_certify_pass_and_quaternions_are_checked(self):
        bad = entries(); bad[0]["qa"]["pass"] = False; bad[0]["qa"]["metrics"]["stretch_ratio"] = 999999
        with self.assertRaisesRegex(registry.RegistryError, "pass must be true"):
            registry.create_candidate(self.root, bad, [TASK_A], SHA_A)
        bad = entries(); bad[0]["tracks"]["spine.rotation"]["values"][0] = [0, 0, 0, 2]
        with self.assertRaisesRegex(registry.RegistryError, "not normalized"):
            registry.create_candidate(self.root, bad, [TASK_A], SHA_A)

    def test_rejects_unsafe_ids_and_paths(self):
        with self.assertRaises(registry.RegistryError):
            registry.create_candidate(self.root, entries(), ["../task"], SHA_A)
        release = self.candidate()
        evidence = task_evidence(release); evidence["run_id"] = "../escape"
        with self.assertRaisesRegex(registry.RegistryError, "unsafe run_id"):
            registry.add_evidence(self.root, evidence)

    def test_partial_stale_and_improve_approvals_block(self):
        release = self.candidate((TASK_A, TASK_B))
        evidence = registry.add_evidence(self.root, task_evidence(release))
        row = verdict(evidence, evidence["media_receipts"][0]["uid"], "Улучшить 🔁")
        registry.sync_verdicts(self.root, "https://autorig.online/dev/api/inbox", "equine", lambda *a, **k: FakeResponse([row]))
        plan = registry.promotion_plan(self.root, release["release_hash"])
        self.assertFalse(plan["ready"])
        self.assertIn("all_media_not_accepted", plan["tasks"][0]["problems"])
        with self.assertRaises(registry.RegistryError):
            registry.promote(self.root, release["release_hash"])

    def test_stale_rig_or_media_verdict_is_ignored(self):
        release = self.candidate()
        evidence = registry.add_evidence(self.root, task_evidence(release))
        row = verdict(evidence, "old-media")
        result = registry.sync_verdicts(self.root, "https://autorig.online/dev/api/inbox", "equine", lambda *a, **k: FakeResponse({"items": [row]}))
        self.assertEqual(result["count"], 0)
        self.assertFalse(registry.promotion_plan(self.root, release["release_hash"])["ready"])

    def test_canonical_dev_only(self):
        with self.assertRaisesRegex(registry.RegistryError, "canonical"):
            registry.sync_verdicts(self.root, "https://example.com/dev/api/inbox", "equine")

    def test_accept_promote_resolve_and_preserve_previous(self):
        first = self.candidate()
        evidence = registry.add_evidence(self.root, task_evidence(first, media_count=2))
        rows = [verdict(evidence, receipt["uid"], inbox=f"inbox-{i}", uid=f"verdict-{i}")
                for i, receipt in enumerate(evidence["media_receipts"])]
        registry.sync_verdicts(self.root, "https://autorig.online/dev/api/inbox", "equine",
                               lambda *a, **k: FakeResponse(rows), trusted_owner_ids=["owner-1"])
        pointer = registry.promote(self.root, first["release_hash"])
        self.assertIsNone(pointer["previous_release_hash"])
        self.assertEqual(len(registry.resolve_runtime(self.root, "лошадь", SHA_A)), 34)
        self.assertEqual(registry.resolve_runtime(self.root, "deer", SHA_A), [])
        self.assertEqual(registry.resolve_runtime(self.root, "horse", SHA_B), [])

    def test_later_improve_overrides_accept_and_stale_revision_cannot_approve(self):
        release = self.candidate()
        old = registry.add_evidence(self.root, task_evidence(release))
        old_uid = old["media_receipts"][0]["uid"]
        newer_input = task_evidence(release, rig=SHA_A)
        newer_input["media_receipts"][0]["uid"] = "media-new-revision"
        newer = registry.add_evidence(self.root, newer_input)
        rows = [
            verdict(old, old_uid, inbox="1", uid="old-accept"),
            verdict(newer, "media-new-revision", inbox="2", uid="new-accept"),
            verdict(newer, "media-new-revision", text="Улучшить 🔁", inbox="3", uid="new-improve"),
        ]
        registry.sync_verdicts(self.root, "https://autorig.online/dev/api/inbox", "equine", lambda *a, **k: FakeResponse(rows))
        self.assertFalse(registry.promotion_plan(self.root, release["release_hash"])["ready"])

    def test_untrusted_owner_is_ignored(self):
        release = self.candidate()
        evidence = registry.add_evidence(self.root, task_evidence(release))
        row = verdict(evidence, evidence["media_receipts"][0]["uid"], user_id="stranger")
        result = registry.sync_verdicts(self.root, "https://autorig.online/dev/api/inbox", "equine",
                                        lambda *a, **k: FakeResponse([row]), trusted_owner_ids=["owner-1"])
        self.assertEqual(result["count"], 0)

    def test_false_positive_words_do_not_select_equids(self):
        for value in ("seahorse", "horseshoe", "donkeykong", "camel", "red deer", "cowboy", "коньяк"):
            self.assertNotEqual(registry.classify_profile(value), "equid")
        self.assertEqual(registry.classify_profile("young zebra model"), "equid")

    def test_profiles_are_separate_and_ambiguous_words_fail_closed(self):
        self.assertEqual(registry.classify_profile("red deer"), "cervid")
        self.assertEqual(registry.classify_profile("двугорбый верблюд"), "camelid")
        self.assertEqual(registry.classify_profile("large bison"), "bovid")
        self.assertEqual(registry.classify_profile("wild boar"), "suid")
        self.assertIsNone(registry.classify_profile("horse and deer"))
        equid = self.candidate()
        evidence = registry.add_evidence(self.root, task_evidence(equid))
        row = verdict(evidence, evidence["media_receipts"][0]["uid"])
        registry.sync_verdicts(self.root, "https://autorig.online/dev/api/inbox", "equine", lambda *a, **k: FakeResponse([row]))
        registry.promote(self.root, equid["release_hash"])
        self.assertEqual(registry.resolve_runtime(self.root, "deer", SHA_A), [])

    def test_fourteen_clip_subset_cannot_be_promoted(self):
        subset = entries()[:14]
        with self.assertRaisesRegex(registry.RegistryError, "missing required"):
            registry.create_candidate(self.root, subset, [TASK_A], SHA_A)

    def test_wrong_loop_and_endpoint_contracts_are_rejected(self):
        bad = entries()
        jump_air = next(row for row in bad if row["clip"] == "jump_air")
        jump_air["loop"] = False
        jump_air["type"] = "one_shot"
        with self.assertRaisesRegex(registry.RegistryError, "loop must be true"):
            registry.create_candidate(self.root, bad, [TASK_A], SHA_A)
        bad = entries()
        death = next(row for row in bad if row["clip"] == "death")
        death["action_metadata"]["end_pose"] = "default_pose"
        with self.assertRaisesRegex(registry.RegistryError, "start/end pose"):
            registry.create_candidate(self.root, bad, [TASK_A], SHA_A)

    def test_concurrent_create_is_one_immutable_release(self):
        queue = multiprocessing.Queue()
        procs = [multiprocessing.Process(target=create_worker, args=(str(self.root), queue)) for _ in range(4)]
        for proc in procs: proc.start()
        for proc in procs: proc.join(10)
        values = [queue.get(timeout=2) for _ in procs]
        self.assertEqual(len(set(values)), 1, values)
        self.assertEqual(len(list((self.root / "releases").glob("*.json"))), 1)


if __name__ == "__main__":
    unittest.main()

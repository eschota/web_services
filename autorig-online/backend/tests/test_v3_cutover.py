import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from v3_cutover import (CutoverNotReady, WorkerReadiness, REQUIRED_STAGES,
                       REQUIRED_EXPORTS, TASK_ROUTES, new_task_pipeline,
                       require_v3_only_ready)


class CutoverTests(unittest.TestCase):
    def ready(self, **override):
        args = dict(workers={"f1": WorkerReadiness("f1", "a" * 64,
                    "autorig.v3.dispatch/2", REQUIRED_STAGES, REQUIRED_EXPORTS, True, True)},
                    required_workers=frozenset({"f1"}), connected_routes=TASK_ROUTES,
                    callback_verified=True, source_binding_verified=True,
                    subscription_export_gate_verified=True)
        args.update(override)
        return args

    def test_complete_contract(self):
        require_v3_only_ready(**self.ready())

    def test_unavailable_node_not_silently_dropped(self):
        with self.assertRaisesRegex(CutoverNotReady, "f13"):
            require_v3_only_ready(**self.ready(required_workers=frozenset({"f1", "f13"})))

    def test_every_creation_route_required(self):
        for route in TASK_ROUTES:
            with self.subTest(route=route), self.assertRaises(CutoverNotReady):
                require_v3_only_ready(**self.ready(connected_routes=TASK_ROUTES - {route}))

    def test_serving_identity_is_required(self):
        worker = WorkerReadiness("f1", "version3", "autorig.v3.dispatch/2",
                                 REQUIRED_STAGES, REQUIRED_EXPORTS, True, True)
        with self.assertRaisesRegex(CutoverNotReady, "identity"):
            require_v3_only_ready(**self.ready(workers={"f1": worker}))

    def test_missing_stages_exports_or_durability_rejected(self):
        for stage in REQUIRED_STAGES:
            worker = WorkerReadiness("f1", "a" * 64, "autorig.v3.dispatch/2",
                                     REQUIRED_STAGES - {stage}, REQUIRED_EXPORTS, True, True)
            with self.subTest(stage=stage), self.assertRaises(CutoverNotReady):
                require_v3_only_ready(**self.ready(workers={"f1": worker}))
        for field in ("callback_verified", "source_binding_verified", "subscription_export_gate_verified"):
            with self.subTest(field=field), self.assertRaises(CutoverNotReady):
                require_v3_only_ready(**self.ready(**{field: False}))

    def test_no_legacy_fallback_when_disabled(self):
        with self.assertRaises(CutoverNotReady):
            new_task_pipeline(cutover_enabled=False, intent="rig")

    def test_intent_and_version_are_separate(self):
        for intent in ("rig", "convert", "generate", "accessory"):
            self.assertEqual(new_task_pipeline(cutover_enabled=True, intent=intent), ("v3", intent))

    def test_unknown_intent_rejected(self):
        with self.assertRaises(ValueError):
            new_task_pipeline(cutover_enabled=True, intent="legacy_animal")


if __name__ == "__main__":
    unittest.main()

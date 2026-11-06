import ast
import os
from pathlib import Path
import types
import unittest


TASKS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tasks.py")


def _load_gate():
    source = Path(TASKS_PATH).read_text(encoding="utf-8")
    tree = ast.parse(source)
    gate = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_legacy_primary_completion_gate"
    )
    module = ast.Module(
        body=[
            ast.ImportFrom(
                module="__future__",
                names=[ast.alias(name="annotations")],
                level=0,
            ),
            gate,
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    namespace = {}
    exec(compile(module, TASKS_PATH, "exec"), namespace)
    return namespace["_legacy_primary_completion_gate"], source


class LegacyCompletionGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        gate, cls.source = _load_gate()
        cls.gate = staticmethod(gate)
        cls.task = types.SimpleNamespace(worker_task_id="primary-worker-task")

    def test_all_ready_processing_pending_and_unknown_remain_non_success(self):
        for status in ("Processing", "Pending", "Queued", "", None):
            with self.subTest(status=status):
                self.assertEqual(
                    self.gate(self.task, {"status": status}, all_urls_ready=True),
                    (False, None),
                )

    def test_only_explicit_completed_with_all_urls_ready_succeeds(self):
        self.assertEqual(
            self.gate(self.task, {"status": "Completed"}, all_urls_ready=True),
            (True, None),
        )
        self.assertEqual(
            self.gate(self.task, {"status": "Completed"}, all_urls_ready=False),
            (False, None),
        )

    def test_failed_is_terminal_non_success(self):
        self.assertEqual(
            self.gate(
                self.task,
                {"status": "Failed", "error": "converter failed"},
                all_urls_ready=True,
            ),
            (False, "converter failed"),
        )

    def test_unreachable_or_missing_exact_binding_is_non_success(self):
        self.assertEqual(
            self.gate(self.task, None, all_urls_ready=True),
            (False, None),
        )
        unbound = types.SimpleNamespace(worker_task_id=None)
        self.assertEqual(
            self.gate(unbound, {"status": "Completed"}, all_urls_ready=True),
            (False, None),
        )

    def test_update_path_uses_gate_for_both_legacy_completion_branches(self):
        tree = ast.parse(self.source)
        update = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "update_task_progress"
        )
        calls = [
            node
            for node in ast.walk(update)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_legacy_primary_completion_gate"
        ]
        self.assertEqual(len(calls), 3)
        ready_args = [
            next(
                keyword.value
                for keyword in call.keywords
                if keyword.arg == "all_urls_ready"
            )
            for call in calls
        ]
        self.assertTrue(any(isinstance(value, ast.Constant) and value.value is False for value in ready_args))
        self.assertTrue(any(isinstance(value, ast.Constant) and value.value is True for value in ready_args))
        self.assertTrue(any(isinstance(value, ast.Name) and value.id == "concrete_outputs_complete" for value in ready_args))
        rendered = ast.unparse(update)
        self.assertGreaterEqual(rendered.count("legacy_worker_completed"), 4)
        self.assertIn("Worker failed: {legacy_worker_failure}", rendered)


if __name__ == "__main__":
    unittest.main()

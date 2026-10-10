import ast
import asyncio
import hashlib
import json
import pathlib
import sys
import tempfile
import types
import unittest
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

BACKEND = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from v3_task_runtime import SameTaskDbBindings, normalize_task_binding


TASKS = BACKEND / "tasks.py"
SOURCE = b"normalized-glb"
SHA = hashlib.sha256(SOURCE).hexdigest()
TASK_ID = "dfc5ccce-3e0c-4729-99dc-286eb23dac88"


class FakeTask:
    def __init__(self, **values): self.__dict__.update(values)


class FakeDb:
    def __init__(self): self.added = []; self.commits = 0; self.rollbacks = 0; self.refreshed = []
    def add(self, row): self.added.append(row)
    async def commit(self): self.commits += 1
    async def refresh(self, row): self.refreshed.append(row)
    async def flush(self): pass
    async def rollback(self): self.rollbacks += 1; self.added.clear()


def load_functions(*names):
    tree = ast.parse(TASKS.read_text(encoding="utf-8"))
    wanted = []
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "V3_RUNTIME_OWNED"
                for target in node.targets):
            wanted.append(node)
    module = ast.Module(body=wanted, type_ignores=[])
    ns = {"asyncio": asyncio, "json": json, "uuid": uuid, "datetime": datetime,
          "Enum": Enum, "Any": Any, "Optional": Optional, "Tuple": tuple,
          "Dict": dict, "Task": FakeTask, "AsyncSession": object,
          "normalize_task_type": lambda value: value,
          "normalize_queue_class": lambda value: value,
          "notify_scheduler": lambda: None,
          "preemption_in_progress": lambda task: False}
    exec(compile(ast.fix_missing_locations(module), str(TASKS), "exec"), ns)
    return ns


class V3TaskCreationTests(unittest.IsolatedAsyncioTestCase):
    def make_source(self):
        work = BACKEND / ".work" / "v3-task-creation-tests"
        work.mkdir(parents=True, exist_ok=True)
        temp = tempfile.TemporaryDirectory(dir=work)
        path = pathlib.Path(temp.name) / "source.glb"; path.write_bytes(SOURCE)
        manifest = {"schema": "autorig.v3.source/1", "intent": "rig", "path": str(path),
                    "sha256": SHA, "bytes": len(SOURCE)}
        return temp, path, manifest

    async def test_v3_task_and_binding_use_one_commit_and_exact_uuid(self):
        ns = load_functions("TaskDispatchOwner", "V3RuntimeOwned", "task_dispatch_owner",
                            "create_conversion_task")
        temp, path, manifest = self.make_source()
        with temp:
            binding = normalize_task_binding(task_id=TASK_ID, source_sha256=SHA,
                                             source_manifest=manifest, requested_intent="rig")
            db = FakeDb(); bound = []
            original = SameTaskDbBindings.bind_in_task_transaction
            async def bind(_self, got_db, got_binding):
                self.assertIs(got_db, db); bound.append(got_binding)
            SameTaskDbBindings.bind_in_task_transaction = bind
            main = types.SimpleNamespace(ensure_disk_headroom_for_new_task=lambda db: asyncio.sleep(0),
                                         enforce_task_cache_max_size=lambda db: asyncio.sleep(0))
            old = sys.modules.get("main"); sys.modules["main"] = main
            try:
                task, error = await ns["create_conversion_task"](
                    db, "private://normalized", "t_pose", "anon", "owner",
                    pipeline_kind="v3", v3_binding=binding)
            finally:
                SameTaskDbBindings.bind_in_task_transaction = original
                if old is None: sys.modules.pop("main", None)
                else: sys.modules["main"] = old
            self.assertIsNone(error)
            self.assertEqual((task.id, task.pipeline_kind, db.commits), (TASK_ID, "v3", 1))
            self.assertEqual(bound, [binding])

    async def test_v3_binding_failure_rolls_back_task_and_never_commits(self):
        ns = load_functions("TaskDispatchOwner", "V3RuntimeOwned", "task_dispatch_owner",
                            "create_conversion_task")
        temp, path, manifest = self.make_source()
        with temp:
            binding = normalize_task_binding(task_id=TASK_ID, source_sha256=SHA,
                                             source_manifest=manifest, requested_intent="rig")
            db = FakeDb(); original = SameTaskDbBindings.bind_in_task_transaction
            async def fail(*_args): raise RuntimeError("binding insert failed")
            SameTaskDbBindings.bind_in_task_transaction = fail
            main = types.SimpleNamespace(ensure_disk_headroom_for_new_task=lambda db: asyncio.sleep(0),
                                         enforce_task_cache_max_size=lambda db: asyncio.sleep(0))
            old = sys.modules.get("main"); sys.modules["main"] = main
            try:
                with self.assertRaisesRegex(RuntimeError, "binding insert failed"):
                    await ns["create_conversion_task"](
                        db, "private://normalized", "t_pose", "anon", "owner",
                        pipeline_kind="v3", v3_binding=binding)
            finally:
                SameTaskDbBindings.bind_in_task_transaction = original
                if old is None: sys.modules.pop("main", None)
                else: sys.modules["main"] = old
            self.assertEqual((db.commits, db.rollbacks, db.added), (0, 1, []))

    async def test_unbound_v3_is_rejected_before_task_write(self):
        ns = load_functions("TaskDispatchOwner", "V3RuntimeOwned", "task_dispatch_owner",
                            "create_conversion_task")
        db = FakeDb()
        with self.assertRaisesRegex(ValueError, "validated TaskBinding"):
            await ns["create_conversion_task"](db, "x", "t_pose", "anon", "o", pipeline_kind="v3")
        self.assertEqual((db.added, db.commits), ([], 0))

    async def test_legacy_dispatch_progress_and_resets_refuse_v3_without_calls(self):
        ns = load_functions("TaskDispatchOwner", "V3RuntimeOwned", "task_dispatch_owner",
                            "start_task_on_worker", "update_task_progress",
                            "admin_requeue_task_to_created", "reset_stale_task")
        task = FakeTask(id=TASK_ID, pipeline_kind="v3", status="created")
        db = FakeDb()
        returned, sentinel = await ns["start_task_on_worker"](db, task, "https://legacy")
        self.assertIs(returned, task)
        self.assertEqual(sentinel.owner, ns["TaskDispatchOwner"].V3_RUNTIME)
        self.assertIs(await ns["update_task_progress"](db, task), task)
        self.assertFalse(await ns["admin_requeue_task_to_created"](db, task))
        self.assertFalse(await ns["reset_stale_task"](db, task))
        self.assertEqual(db.commits, 0)

    async def test_old_rig_creation_is_unchanged_and_has_no_binding(self):
        ns = load_functions("TaskDispatchOwner", "V3RuntimeOwned", "task_dispatch_owner",
                            "create_conversion_task")
        db = FakeDb()
        main = types.SimpleNamespace(ensure_disk_headroom_for_new_task=lambda db: asyncio.sleep(0),
                                     enforce_task_cache_max_size=lambda db: asyncio.sleep(0))
        old = sys.modules.get("main"); sys.modules["main"] = main
        try:
            task, _ = await ns["create_conversion_task"](
                db, "https://example.test/model.glb", "t_pose", "anon", "owner",
                pipeline_kind="rig")
        finally:
            if old is None: sys.modules.pop("main", None)
            else: sys.modules["main"] = old
        self.assertEqual((task.pipeline_kind, db.commits), ("rig", 1))
        self.assertNotEqual(task.id, TASK_ID)


if __name__ == "__main__":
    unittest.main()

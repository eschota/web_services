"""AutoRig tasks have priority on worker-4090 (owner rule, 2026-10-15).

«приоритет авториг задач для 4090 должен быть отдан … я предупрежу очередь видео
генератора, чтобы он не пугался что 4090 будет приоритетно считать авториг задачи»

Three things the queue must do: dispatch AutoRig work first, keep the 4090 free
of other work while AutoRig work waits, and take the 4090 from a running
non-AutoRig render once an AutoRig task has waited long enough.
"""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from renderfin import config, workload_lease
from renderfin.models import (
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderServer,
    RenderTask,
)
from renderfin.queue import RenderQueue, dispatch_order, is_autorig
from renderfin.registry import ServerRegistry


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def _task(task_id, created_at, *, user_name="tester", priority_class="", status=TASK_PENDING,
          workflow="gen_image.json", queue_class="interactive"):
    return RenderTask(
        id=task_id,
        prompt=RenderPrompt(prompt="a car", user_name=user_name, priority_class=priority_class),
        status=status, created_at=created_at, workflow=workflow, queue_class=queue_class,
    )


class IsAutoRigTests(unittest.TestCase):
    def test_marked_or_autorig_user_name(self):
        self.assertTrue(is_autorig(_task("a", 1.0, priority_class="autorig")))
        self.assertTrue(is_autorig(_task("b", 1.0, user_name="site")))
        self.assertFalse(is_autorig(_task("c", 1.0, user_name="sura-bot")))
        # AutoRig's own bulk never takes priority
        self.assertFalse(is_autorig(_task("d", 1.0, user_name="site", queue_class="collection_background")))

    def test_switch_off(self):
        with patch.object(config, "AUTORIG_PRIORITY", False):
            self.assertFalse(is_autorig(_task("a", 1.0, priority_class="autorig")))


class DispatchOrderTests(unittest.TestCase):
    def test_autorig_first_oldest_first_then_the_usual_order(self):
        now = 1000.0
        tasks = [
            _task("bot-new", now - 5, user_name="sura-bot"),
            _task("rig-2", now - 20, priority_class="autorig"),
            _task("bot-old", now - 60, user_name="sura-bot"),
            _task("rig-1", now - 40, user_name="site"),
            _task("running", now - 90, user_name="site", status=TASK_RENDERING),
        ]
        order = [t.id for t in dispatch_order(tasks, now=now)]
        self.assertEqual(order[:2], ["rig-1", "rig-2"])
        self.assertEqual(set(order[2:]), {"bot-new", "bot-old"})

    def test_without_lifo_autorig_still_first(self):
        with patch.object(config, "LIFO", False):
            tasks = [_task("bot", 1.0, user_name="x"), _task("rig", 2.0, priority_class="autorig")]
            self.assertEqual([t.id for t in dispatch_order(tasks)], ["rig", "bot"])


def _server(name, workflows=("gen_image.json",)):
    return RenderServer(render_server_name=name, render_server_url=f"http://{name}:8188",
                        status="online", available_workflows=list(workflows))


async def _noop(*args, **kwargs):
    return None


class ReservationAndPreemptionTests(unittest.TestCase):
    def _queue(self, root):
        registry = ServerRegistry()
        registry.save(_server("worker-4090"))
        registry.save(_server("f15"))
        return RenderQueue(registry, db_path=Path(root) / "renderfin.db")

    def _env(self, root):
        return [patch.object(config, "DATA_DIR", Path(root)),
                patch.object(config, "RENDER_DIR", Path(root) / "render"),
                patch.object(config, "DB_DIR", Path(root) / "db"),
                patch.object(config, "TMP_DIR", Path(root) / "tmp"),
                patch.object(config, "SERVERS_DIR", Path(root) / "servers"),
                patch.object(config, "DB_PATH", Path(root) / "db" / "renderfin.db"),
                patch.object(config, "AUTORIG_PRIORITY_BOXES", frozenset({"worker-4090"})),
                patch.object(config, "OOM_RECOVERY_ENABLED", False)]

    def test_4090_is_reserved_while_autorig_waits(self):
        with tempfile.TemporaryDirectory() as root:
            patches = self._env(root)
            for p in patches:
                p.start()
            try:
                queue = self._queue(root)
                rig = _task("rig", 10.0, priority_class="autorig")
                bot = _task("bot", 5.0, user_name="sura-bot")
                queue._tasks = {rig.id: rig, bot.id: bot}
                box = queue.registry.get("worker-4090")
                other = queue.registry.get("f15")
                now = 100.0
                self.assertEqual(queue._exclusion_reason(box, bot.workflow, {}, None, now, task=bot),
                                 ("reserved-autorig", "rig"))
                self.assertIsNone(queue._exclusion_reason(other, bot.workflow, {}, None, now, task=bot))
                self.assertIsNone(queue._exclusion_reason(box, rig.workflow, {}, None, now, task=rig))
                # nothing AutoRig waiting: the 4090 takes bot work again
                rig.status = TASK_RENDERING
                self.assertIsNone(queue._exclusion_reason(box, bot.workflow, {}, None, now, task=bot))
            finally:
                for p in patches:
                    p.stop()

    def test_waiting_autorig_takes_the_4090_from_a_bot_render(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as root:
                patches = self._env(root) + [patch.object(config, "AUTORIG_PREEMPT_SECONDS", 45.0)]
                for p in patches:
                    p.start()
                try:
                    queue = self._queue(root)
                    await queue.start()
                    try:
                        import time as _t
                        victim = _task("video", _t.time() - 300, user_name="sura-bot", status=TASK_RENDERING)
                        victim.server_name = "worker-4090"
                        victim.comfy_prompt_id = "p-1"
                        young = _task("rig-young", _t.time() - 5, priority_class="autorig")
                        old = _task("rig-old", _t.time() - 120, priority_class="autorig")
                        queue._tasks = {victim.id: victim, young.id: young, old.id: old}
                        freed = []

                        async def fake_free(box, prompt_id):
                            freed.append((box, prompt_id))
                            return "interrupted and freed"

                        queue._free_box_after_restart = fake_free
                        self.assertFalse(await queue._preempt_for_autorig(young))   # too young
                        self.assertTrue(await queue._preempt_for_autorig(old))
                        self.assertFalse(await queue._preempt_for_autorig(old))     # nothing left to take
                    finally:
                        await queue.stop()
                    return freed, victim
                finally:
                    for p in patches:
                        p.stop()

        with patch.object(workload_lease, "cancel_waiter", new=_noop), \
             patch.object(workload_lease, "release", new=_noop):
            freed, victim = run(scenario())
        self.assertEqual(freed, [("worker-4090", "p-1")])
        self.assertEqual(victim.status, TASK_PENDING)
        self.assertEqual(victim.server_name, "")
        self.assertEqual(victim.notice, config.AUTORIG_PREEMPTED_NOTICE)
        self.assertEqual(victim.restart_resumes, 0)
        self.assertIn("p-1", victim.retired_comfy_prompt_ids)


if __name__ == "__main__":
    unittest.main()

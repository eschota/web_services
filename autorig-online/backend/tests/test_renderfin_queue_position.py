"""Where a waiting render is in the line, and clearing the line.

Two questions the farm could not answer before: "how long is this actually
going to be" (which needs a rank, not the word "queued") and "give me my cards
back" (which must never stop a render that has already started).
"""

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from renderfin import config, workload_lease
from renderfin.models import (
    TASK_DONE,
    TASK_ERROR,
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderTask,
)
from renderfin.queue import RenderQueue, pending_queue_position
from renderfin.registry import ServerRegistry


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def _task(task_id: str, status: str, created_at: float) -> RenderTask:
    return RenderTask(
        id=task_id,
        prompt=RenderPrompt(prompt="a cat", user_name="tester"),
        status=status,
        created_at=created_at,
    )


class PendingQueuePositionTests(unittest.TestCase):
    def test_rank_follows_submission_order_not_storage_order(self):
        tasks = [
            _task("c", TASK_PENDING, 300.0),
            _task("a", TASK_PENDING, 100.0),
            _task("b", TASK_PENDING, 200.0),
        ]
        self.assertEqual(pending_queue_position(tasks, "a"),
                         {"queue_position_int": 1, "queue_length_int": 3})
        self.assertEqual(pending_queue_position(tasks, "b"),
                         {"queue_position_int": 2, "queue_length_int": 3})
        self.assertEqual(pending_queue_position(tasks, "c"),
                         {"queue_position_int": 3, "queue_length_int": 3})

    def test_only_waiting_tasks_are_counted_or_ranked(self):
        tasks = [
            _task("running", TASK_RENDERING, 50.0),
            _task("done", TASK_DONE, 60.0),
            _task("failed", TASK_ERROR, 70.0),
            _task("waiting", TASK_PENDING, 80.0),
            _task("later", TASK_PENDING, 90.0),
        ]
        self.assertEqual(pending_queue_position(tasks, "waiting"),
                         {"queue_position_int": 1, "queue_length_int": 2})
        # A render that has a card is not behind anything, so it has no place.
        self.assertEqual(pending_queue_position(tasks, "running"),
                         {"queue_position_int": 0, "queue_length_int": 2})
        self.assertEqual(pending_queue_position(tasks, "done"),
                         {"queue_position_int": 0, "queue_length_int": 2})
        self.assertEqual(pending_queue_position(tasks, "nobody"),
                         {"queue_position_int": 0, "queue_length_int": 2})

    def test_identical_submission_times_still_produce_one_order(self):
        tasks = [_task("b", TASK_PENDING, 10.0), _task("a", TASK_PENDING, 10.0)]
        first = pending_queue_position(tasks, "a")["queue_position_int"]
        second = pending_queue_position(tasks, "b")["queue_position_int"]
        self.assertEqual({first, second}, {1, 2})
        self.assertEqual(pending_queue_position(list(reversed(tasks)), "a"),
                         pending_queue_position(tasks, "a"))

    def test_an_empty_farm_reports_no_queue(self):
        self.assertEqual(pending_queue_position([], "anything"),
                         {"queue_position_int": 0, "queue_length_int": 0})


class ClearQueueTests(unittest.TestCase):
    """Clearing the queue stands down waiting work and only waiting work."""

    def _queue(self, tmp: Path) -> RenderQueue:
        return RenderQueue(ServerRegistry(), db_path=tmp / "renderfin.db")

    def test_pending_go_and_rendering_stays(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as root:
                with patch.object(config, "DATA_DIR", Path(root)), \
                     patch.object(config, "RENDER_DIR", Path(root) / "render"), \
                     patch.object(config, "DB_DIR", Path(root) / "db"), \
                     patch.object(config, "TMP_DIR", Path(root) / "tmp"), \
                     patch.object(config, "SERVERS_DIR", Path(root) / "servers"), \
                     patch.object(config, "DB_PATH", Path(root) / "db" / "renderfin.db"):
                    queue = self._queue(Path(root))
                    await queue.start()
                    try:
                        waiting = [
                            await queue.enqueue(
                                RenderPrompt(prompt=f"job {index}", user_name="tester")
                            )
                            for index in range(4)
                        ]
                        # One of them has already been handed to a card.
                        busy = waiting[1]
                        busy.status = TASK_RENDERING
                        busy.server_name = "f13"
                        busy.comfy_prompt_id = "prompt-1"
                        finished = waiting[2]
                        finished.status = TASK_DONE

                        result = await queue.cancel_all_pending()
                    finally:
                        await queue.stop()
                    states = {task.id: task.status for task in queue.all_tasks()}
                    return result, states, busy.id, finished.id, waiting

        with patch.object(workload_lease, "cancel_waiter", new=_noop), \
             patch.object(workload_lease, "release", new=_noop):
            result, states, busy_id, finished_id, waiting = run(scenario())

        self.assertEqual(result["cancelled_int"], 2)
        self.assertEqual(result["pending_seen_int"], 2)
        self.assertEqual(result["running_untouched_int"], 1)
        self.assertEqual(states[busy_id], TASK_RENDERING)
        self.assertEqual(states[finished_id], TASK_DONE)
        self.assertEqual(states[waiting[0].id], TASK_ERROR)
        self.assertEqual(states[waiting[3].id], TASK_ERROR)

    def test_clearing_an_empty_queue_changes_nothing(self):
        async def scenario():
            with tempfile.TemporaryDirectory() as root:
                with patch.object(config, "DATA_DIR", Path(root)), \
                     patch.object(config, "RENDER_DIR", Path(root) / "render"), \
                     patch.object(config, "DB_DIR", Path(root) / "db"), \
                     patch.object(config, "TMP_DIR", Path(root) / "tmp"), \
                     patch.object(config, "SERVERS_DIR", Path(root) / "servers"), \
                     patch.object(config, "DB_PATH", Path(root) / "db" / "renderfin.db"):
                    queue = self._queue(Path(root))
                    await queue.start()
                    try:
                        return await queue.cancel_all_pending()
                    finally:
                        await queue.stop()

        result = run(scenario())
        self.assertEqual(result["cancelled_int"], 0)
        self.assertEqual(result["running_untouched_int"], 0)


async def _noop(*args, **kwargs):
    return {}


if __name__ == "__main__":
    unittest.main()

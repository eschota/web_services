import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from renderfin import config
from renderfin.models import (
    TASK_DONE,
    TASK_ERROR,
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderServer,
)
from renderfin.queue import RenderQueue
from renderfin.registry import ServerRegistry


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _Env:
    """Temp data dirs patched into renderfin.config."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.patches = [
            patch.object(config, "DATA_DIR", root),
            patch.object(config, "RENDER_DIR", root / "render"),
            patch.object(config, "DB_DIR", root / "db"),
            patch.object(config, "TMP_DIR", root / "tmp"),
            patch.object(config, "SERVERS_DIR", root / "servers"),
            patch.object(config, "DB_PATH", root / "db" / "renderfin.db"),
            patch.object(config, "DISPATCH_INTERVAL_SECONDS", 0.0),
        ]

    def __enter__(self):
        for p in self.patches:
            p.start()
        config.ensure_dirs()
        return self

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()


def _server(name="raptor", workflows=("gen_image.json",)):
    return RenderServer(
        render_server_name=name,
        render_server_url="http://5.129.157.224:8288",
        status="online",
        available_workflows=list(workflows),
    )


class QueueDispatchTests(unittest.TestCase):
    def test_one_in_flight_per_server_and_token_rule(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    submitted = []

                    async def fake_submit(task, server):
                        submitted.append((task.id, server.render_server_name))
                        task.server_name = server.render_server_name
                        task.status = TASK_RENDERING
                        task.started_at = 1e18  # never times out in this test
                        await queue._persist(task)

                    t1 = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    t2 = await queue.enqueue(
                        RenderPrompt(prompt="b", type="t_pose", image_url="https://h/m.jpg")
                    )
                    t3 = await queue.enqueue(
                        RenderPrompt(type="image_to_3d", image_url="https://h/i.png")
                    )
                    self.assertEqual(t1.workflow, "gen_image.json")
                    self.assertEqual(t3.workflow, "image_to_3d.json")

                    with patch.object(queue, "_submit_task", side_effect=fake_submit):
                        with patch.object(queue, "_refresh_servers"):
                            with patch.object(queue, "_poll_rendering"):
                                await queue.tick()
                                await queue.tick()
                                await queue.tick()

                    # only one server slot -> only the first t_pose dispatched;
                    # image_to_3d never dispatches (no server advertises it)
                    self.assertEqual([s[0] for s in submitted], [t1.id])
                    self.assertEqual(queue.get(t2.id).status, TASK_PENDING)
                    self.assertEqual(queue.get(t3.id).status, TASK_PENDING)
                finally:
                    await queue.stop()

        run(scenario())

    def test_timeout_marks_error_and_frees_server(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                registry.save(_server())
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.started_at = 1.0  # long ago
                    await queue._persist(task)

                    await queue._poll_rendering()
                    self.assertEqual(task.status, TASK_ERROR)
                    self.assertIn("timeout", task.error)
                    self.assertEqual(queue._busy_servers(), {})
                finally:
                    await queue.stop()

        run(scenario())

    def test_restart_resurrects_rendering_as_pending(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                task = await queue.enqueue(
                    RenderPrompt(prompt="a", type="t_pose", image_url="https://h/m.jpg")
                )
                task.status = TASK_RENDERING
                task.server_name = "raptor"
                task.comfy_prompt_id = "p1"
                await queue._persist(task)
                await queue.stop()

                queue2 = RenderQueue(registry, db_path=config.DB_PATH)
                await queue2.start()
                try:
                    revived = queue2.get(task.id)
                    self.assertIsNotNone(revived)
                    self.assertEqual(revived.status, TASK_PENDING)
                    self.assertEqual(revived.server_name, "")
                finally:
                    await queue2.stop()

        run(scenario())

    def test_finish_saves_primary_and_isolated(self):
        async def scenario():
            with _Env():
                registry = ServerRegistry()
                server = _server()
                registry.save(server)
                queue = RenderQueue(registry, db_path=config.DB_PATH)
                await queue.start()
                queue._pump_task.cancel()
                try:
                    task = await queue.enqueue(
                        RenderPrompt(
                            prompt="a", type="t_pose",
                            image_url="https://h/m.jpg", user_name="bot",
                        )
                    )
                    task.status = TASK_RENDERING
                    task.server_name = "raptor"
                    task.started_at = 100.0

                    entry = {
                        "outputs": {
                            "9": {"images": [
                                {"filename": f"{task.id}_00001_.png", "subfolder": "", "type": "output"}
                            ]},
                            "301": {"images": [
                                {"filename": f"{task.id}_Isolated_00001_.png", "subfolder": "", "type": "output"}
                            ]},
                        }
                    }

                    async def fake_download(client, srv, artifact):
                        return b"PNGDATA-" + artifact["filename"].encode()

                    with patch(
                        "renderfin.comfy_adapter.download_artifact", side_effect=fake_download
                    ):
                        await queue._finish(task, server, entry)

                    self.assertEqual(task.status, TASK_DONE)
                    out = Path(task.output_path)
                    self.assertTrue(out.is_file())
                    self.assertIn(f"{task.id}_00001_", out.read_bytes().decode())
                    self.assertIn("isolated", task.extra_outputs)
                    iso = config.RENDER_DIR / "bot" / f"{task.id}_Isolated.png"
                    self.assertTrue(iso.is_file())
                    self.assertIn("_Isolated_", iso.read_bytes().decode())
                finally:
                    await queue.stop()

        run(scenario())


if __name__ == "__main__":
    unittest.main()

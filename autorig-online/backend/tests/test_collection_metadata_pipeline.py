import asyncio
import json
import sys
import types
import unittest
from unittest.mock import patch

import httpx

import workers
import tasks


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class CollectionWorkerPayloadTests(unittest.TestCase):
    def test_collection_metadata_is_persisted_on_conversion_task(self):
        async def scenario():
            class FakeDb:
                task = None

                def add(self, task):
                    self.task = task

                async def commit(self):
                    return None

                async def refresh(self, task):
                    return None

            async def no_op(_db):
                return None

            fake_main = types.SimpleNamespace(
                ensure_disk_headroom_for_new_task=no_op,
                enforce_task_cache_max_size=no_op,
            )
            db = FakeDb()
            with patch.dict(sys.modules, {"main": fake_main}), patch.object(
                tasks, "notify_scheduler"
            ) as wake:
                task, error = await tasks.create_conversion_task(
                    db,
                    "https://autorig.online/renderfin/render/bot/member.glb",
                    "t_pose",
                    "anon",
                    "telegram-bot",
                    pipeline_kind="convert",
                    collection_metadata={
                        "collection_guid": "11111111-2222-3333-4444-555566667777",
                        "collection_title": "Afterlight Zombie Neighbors",
                        "collection_description": "Fifteen distinct undead neighbors.",
                        "collection_tags": ["zombie", "undead", "collection"],
                        "collection_index": 7,
                        "collection_size": 15,
                        "collection_member_title": "The Bicycle Courier",
                    },
                )
            wake.assert_called_once_with()
            self.assertIsNone(error)
            self.assertIs(task, db.task)
            self.assertEqual(task.collection_guid, "11111111-2222-3333-4444-555566667777")
            self.assertEqual(json.loads(task.collection_tags), ["zombie", "undead", "collection"])
            self.assertEqual((task.collection_index, task.collection_size), (7, 15))
            # Metadata alone never demotes a manual/API submit.
            self.assertEqual(task.queue_class, "interactive")

        run(scenario())

    def test_collection_fields_are_sent_as_top_level_worker_request(self):
        async def scenario():
            captured = {}

            def handler(request: httpx.Request) -> httpx.Response:
                captured.update(json.loads(request.content))
                return httpx.Response(
                    200,
                    json={
                        "task_id": "worker-task-1",
                        "progress_page": (
                            "https://worker.test/converter/glb/"
                            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/index.html"
                        ),
                        "output_urls": [],
                    },
                )

            transport = httpx.MockTransport(handler)
            real_client = httpx.AsyncClient

            def patched_client(*args, **kwargs):
                kwargs["transport"] = transport
                return real_client(*args, **kwargs)

            metadata = {
                "collection_guid": "11111111-2222-3333-4444-555566667777",
                "collection_title": "Afterlight Zombie Neighbors",
                "collection_description": "Fifteen distinct undead neighbors.",
                "collection_tags": ["zombie", "undead", "collection"],
                "collection_index": 7,
                "collection_size": 15,
                "collection_member_title": "The Bicycle Courier",
            }
            with patch.object(workers.httpx, "AsyncClient", side_effect=patched_client):
                result = await workers.send_task_to_worker(
                    "https://worker.test/api-converter-glb",
                    "https://autorig.online/renderfin/render/bot/member.glb",
                    "t_pose",
                    pipeline_kind="convert",
                    metadata=metadata,
                    backend_task_id="backend-task-7",
                    queue_class="collection_background",
                )
            self.assertTrue(result.success)
            for key, value in metadata.items():
                self.assertEqual(captured[key], value)
            self.assertEqual(captured["backend_task_id"], "backend-task-7")
            self.assertEqual(captured["queue_class"], "collection_background")

        run(scenario())


if __name__ == "__main__":
    unittest.main()

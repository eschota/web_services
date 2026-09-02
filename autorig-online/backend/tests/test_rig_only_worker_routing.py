"""Capability-aware routing for AutoRig-only converter boxes (f5/f15).

A ``rig_only`` worker runs ``pipeline_kind == "rig"`` and nothing else: it has no
retopo / 3ds Max / Maya / C4D toolchain, so a ``convert`` task must never reach
it, and it must not count as full-converter capacity in the cross-pipeline
reserve.
"""
import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import task_priority
import workers
from renderfin import config as renderfin_config
from renderfin import hunyuan_client


RIG_ONLY_STATUS = {
    "maintenance": False,
    "capabilities": {
        "mode": "only_rig",
        "autorig": True,
        "legacy_conversion": False,
        "only_fbx": False,
        "occonvert": False,
        "hunyuan_image_to_3d": False,
    },
}
FULL_STATUS = {
    "maintenance": False,
    "capabilities": {
        "mode": "full",
        "autorig": True,
        "legacy_conversion": True,
        "occonvert": True,
    },
}


def queued(task_id, pipeline_kind):
    return SimpleNamespace(
        id=task_id,
        pipeline_kind=pipeline_kind,
        status="created",
        source_attempt_count=0,
    )


class WorkerPoolParsingTests(unittest.TestCase):
    def test_rig_only_pool_spellings_are_recognized(self):
        for value in ("rig_only", "RIG_ONLY", " rig-only ", "only_rig"):
            self.assertTrue(workers.pool_is_rig_only(value), value)

    def test_unknown_or_missing_pool_stays_a_full_converter(self):
        for value in (None, "", "full_converter", "shared_gpu", "hunyuan_only"):
            self.assertFalse(workers.pool_is_rig_only(value), value)
            self.assertEqual(
                workers.normalize_worker_pool(value),
                workers.WORKER_POOL_FULL_CONVERTER,
            )

    def test_pipeline_kind_defaults_to_rig_like_the_dispatcher(self):
        self.assertEqual(workers.normalize_pipeline_kind("convert"), "convert")
        for value in (None, "", "generate", "RIG"):
            self.assertEqual(workers.normalize_pipeline_kind(value), "rig")


class WorkerCapabilityTelemetryTests(unittest.TestCase):
    def test_only_rig_mode_refuses_convert_and_keeps_rig(self):
        caps = workers.parse_worker_pipeline_capabilities(RIG_ONLY_STATUS)
        self.assertTrue(caps.rig)
        self.assertFalse(caps.convert)
        self.assertTrue(caps.accepts("rig"))
        self.assertFalse(caps.accepts("convert"))

    def test_legacy_conversion_false_alone_is_enough(self):
        caps = workers.parse_worker_pipeline_capabilities(
            {"capabilities": {"mode": "", "legacy_conversion": False}}
        )
        self.assertTrue(caps.rig)
        self.assertFalse(caps.convert)

    def test_full_and_legacy_workers_keep_both_pipelines(self):
        self.assertEqual(
            workers.parse_worker_pipeline_capabilities(FULL_STATUS),
            workers.FULL_PIPELINE_CAPABILITIES,
        )
        for payload in ({}, {"server_version": "legacy"}, None):
            self.assertEqual(
                workers.parse_worker_pipeline_capabilities(payload),
                workers.FULL_PIPELINE_CAPABILITIES,
            )

    def test_hunyuan_only_node_takes_neither_pipeline(self):
        caps = workers.parse_worker_pipeline_capabilities(
            {"capabilities": {"mode": "hunyuan_only", "legacy_conversion": False}}
        )
        self.assertFalse(caps.rig)
        self.assertFalse(caps.convert)

    def test_dispatch_admission_carries_capabilities(self):
        admission = workers.parse_worker_dispatch_admission(
            dict(RIG_ONLY_STATUS, disk_free_gb=100), min_free_disk_gb=25
        )
        self.assertTrue(admission.allowed)
        self.assertFalse(admission.pipeline_capabilities.convert)

    def test_registry_pool_marks_rig_only_without_any_telemetry(self):
        rig_only = SimpleNamespace(url="http://127.0.0.1:15488/api-converter-glb")
        full = SimpleNamespace(url="http://127.0.0.1:15131/api-converter-glb")
        workers.mark_rig_only_workers(
            [rig_only, full], {"http://127.0.0.1:15488/api-converter-glb"}
        )
        self.assertFalse(workers.worker_accepts_pipeline_kind(rig_only, "convert"))
        self.assertTrue(workers.worker_accepts_pipeline_kind(rig_only, "rig"))
        self.assertTrue(workers.worker_accepts_pipeline_kind(full, "convert"))

    def test_rig_only_worker_urls_read_the_enabled_pool_rows(self):
        import database

        async def scenario():
            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:", poolclass=StaticPool
            )
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with engine.begin() as connection:
                await connection.run_sync(database.WorkerEndpoint.__table__.create)
            async with sessions() as session:
                session.add_all([
                    database.WorkerEndpoint(
                        url="http://127.0.0.1:15488/api-converter-glb",
                        enabled=True,
                        pool="rig_only",
                        role="autorig_primary",
                    ),
                    database.WorkerEndpoint(
                        url="http://127.0.0.1:15131/api-converter-glb",
                        enabled=True,
                        pool="full_converter",
                        role="autorig_primary",
                    ),
                    database.WorkerEndpoint(
                        url="http://127.0.0.1:15999/api-converter-glb",
                        enabled=False,
                        pool="rig_only",
                        role="autorig_primary",
                    ),
                ])
                await session.commit()
                found = await workers.get_rig_only_worker_urls(session)
            await engine.dispose()
            return found

        self.assertEqual(
            asyncio.run(scenario()),
            {"http://127.0.0.1:15488/api-converter-glb"},
        )


class RigOnlyDispatchSelectionTests(unittest.TestCase):
    """The scheduler composition from ``main._dispatch_priority_queue``."""

    @staticmethod
    def dispatch(worker, candidates, dispatched):
        async def attempt(task):
            dispatched.append((worker.url, task.id))
            task.status = "processing"
            return task, None

        return asyncio.run(
            task_priority.dispatch_fifo_candidate(
                candidates,
                attempt,
                eligible=lambda task: workers.worker_accepts_pipeline_kind(
                    worker, getattr(task, "pipeline_kind", None)
                ),
            )
        )

    def setUp(self):
        self.rig_only = SimpleNamespace(
            url="http://127.0.0.1:15488/api-converter-glb",
            pipeline_capabilities=workers.RIG_ONLY_PIPELINE_CAPABILITIES,
        )
        self.full = SimpleNamespace(
            url="http://127.0.0.1:15131/api-converter-glb",
            pipeline_capabilities=workers.FULL_PIPELINE_CAPABILITIES,
        )

    def test_convert_head_skips_rig_only_worker_and_next_rig_task_runs(self):
        convert = queued("convert-oldest", "convert")
        rig = queued("rig-newer", "rig")
        candidates = [convert, rig]
        dispatched = []

        self.assertTrue(self.dispatch(self.rig_only, candidates, dispatched))
        self.assertEqual(dispatched, [(self.rig_only.url, "rig-newer")])
        # The convert row keeps its FIFO position and waits for a full converter.
        self.assertEqual([task.id for task in candidates], ["convert-oldest"])

        self.assertTrue(self.dispatch(self.full, candidates, dispatched))
        self.assertEqual(dispatched[-1], (self.full.url, "convert-oldest"))
        self.assertEqual(candidates, [])

    def test_rig_only_worker_never_takes_a_convert_only_queue(self):
        candidates = [queued("convert-1", "convert"), queued("convert-2", "convert")]
        dispatched = []
        self.assertFalse(self.dispatch(self.rig_only, candidates, dispatched))
        self.assertEqual(dispatched, [])
        self.assertEqual([task.id for task in candidates], ["convert-1", "convert-2"])

    def test_skipped_tasks_keep_their_relative_order(self):
        candidates = [
            queued("convert-1", "convert"),
            queued("convert-2", "convert"),
            queued("rig-1", "rig"),
            queued("convert-3", "convert"),
        ]
        dispatched = []
        self.assertTrue(self.dispatch(self.rig_only, candidates, dispatched))
        self.assertEqual(dispatched, [(self.rig_only.url, "rig-1")])
        self.assertEqual(
            [task.id for task in candidates],
            ["convert-1", "convert-2", "convert-3"],
        )

    def test_task_with_no_pipeline_kind_is_a_rig_task(self):
        candidates = [SimpleNamespace(id="legacy", status="created", source_attempt_count=0)]
        dispatched = []
        self.assertTrue(self.dispatch(self.rig_only, candidates, dispatched))
        self.assertEqual(dispatched, [(self.rig_only.url, "legacy")])

    def test_select_best_worker_skips_rig_only_node_for_convert(self):
        rig_only_url = "http://127.0.0.1:15488/api-converter-glb"
        full_url = "http://127.0.0.1:15131/api-converter-glb"
        available = [
            workers.WorkerInfo(url=rig_only_url, available=True, load=0),
            workers.WorkerInfo(url=full_url, available=True, load=0),
        ]

        async def fake_filter(rows, client=None):
            for row in rows:
                setattr(
                    row,
                    "pipeline_capabilities",
                    workers.parse_worker_pipeline_capabilities(
                        RIG_ONLY_STATUS if row.url == rig_only_url else FULL_STATUS
                    ),
                )
            return list(rows)

        def run_select(pipeline_kind):
            with (
                patch.object(
                    workers,
                    "get_configured_workers_with_weight",
                    AsyncMock(return_value=[(rig_only_url, 5), (full_url, 0)]),
                ),
                patch.object(
                    workers, "get_all_workers_status", AsyncMock(return_value=available)
                ),
                patch.object(
                    workers, "get_rig_only_worker_urls", AsyncMock(return_value=set())
                ),
                patch.object(
                    workers,
                    "get_backend_worker_processing_counts",
                    AsyncMock(return_value={}),
                ),
                patch.object(workers, "filter_workers_for_dispatch", fake_filter),
            ):
                return asyncio.run(workers.select_best_worker(pipeline_kind=pipeline_kind))

        # The rig-only node has the higher weight, so only capability routing
        # can keep the convert task off it.
        self.assertEqual(run_select("convert"), full_url)
        self.assertEqual(run_select("rig"), rig_only_url)


class RigOnlyCapacityExclusionTests(unittest.TestCase):
    def test_status_is_full_converter_rejects_only_rig_mode(self):
        self.assertFalse(
            hunyuan_client._status_is_full_converter({
                "capabilities": {"mode": "only_rig", "legacy_conversion": False},
                "feature_flags": {
                    "converter_capability_mode": "full",
                    "legacy_conversion_enabled": True,
                },
            })
        )
        self.assertTrue(
            hunyuan_client._status_is_full_converter({
                "capabilities": {"mode": "full", "legacy_conversion": True},
                "feature_flags": {
                    "converter_capability_mode": "full",
                    "legacy_conversion_enabled": True,
                },
            })
        )

    @staticmethod
    def _build_db(directory: str, rows, *, with_pool_column: bool) -> Path:
        database = Path(directory) / "autorig.db"
        connection = sqlite3.connect(database)
        try:
            if with_pool_column:
                connection.execute(
                    "CREATE TABLE worker_endpoints (url TEXT, enabled INTEGER, pool TEXT)"
                )
                connection.executemany(
                    "INSERT INTO worker_endpoints VALUES (?, 1, ?)", rows
                )
            else:
                connection.execute(
                    "CREATE TABLE worker_endpoints (url TEXT, enabled INTEGER)"
                )
                connection.executemany(
                    "INSERT INTO worker_endpoints VALUES (?, 1)",
                    [(url,) for url, _pool in rows],
                )
            connection.commit()
        finally:
            connection.close()
        return database

    def test_registry_excludes_rig_only_rows(self):
        rows = [
            ("https://converter-f1.freestock.online/api-converter-glb", "full_converter"),
            ("http://127.0.0.1:15488/api-converter-glb", "rig_only"),
            ("https://converter-f15.freestock.online/api-converter-glb", "rig_only"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            database = self._build_db(tmp, rows, with_pool_column=True)
            with patch.object(
                renderfin_config, "AUTORIG_QUEUE_DB_PATH", database
            ):
                registry = hunyuan_client.full_converter_registry([])
        self.assertEqual([worker["name"] for worker in registry], ["f1"])

    def test_registry_still_reads_a_schema_without_the_pool_column(self):
        rows = [
            ("https://converter-f1.freestock.online/api-converter-glb", None),
            ("https://converter-f2.freestock.online/api-converter-glb", None),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            database = self._build_db(tmp, rows, with_pool_column=False)
            with patch.object(
                renderfin_config, "AUTORIG_QUEUE_DB_PATH", database
            ):
                registry = hunyuan_client.full_converter_registry([])
        self.assertEqual(
            sorted(worker["name"] for worker in registry), ["f1", "f2"]
        )


if __name__ == "__main__":
    unittest.main()

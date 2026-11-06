"""Three scheduling tiers: M+ > AutoRig > generation (> background) — owner 2026-11-06.

«чёткий балансер, который все задачи раскидает с приоритетом для М+, потом AutoRig,
и только потом Renderfin-генерация картинок/видео; держать прогретыми нужные ноды».
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import patch

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from renderfin import config, priority, workload_lease  # noqa: E402
from renderfin.models import (  # noqa: E402
    TASK_PENDING,
    TASK_RENDERING,
    RenderPrompt,
    RenderServer,
    RenderTask,
)
from renderfin.queue import RenderQueue, dispatch_order, is_autorig  # noqa: E402
from renderfin.registry import ServerRegistry  # noqa: E402

KEY = "k" * 32
OTHER_KEY = "z" * 40


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def _task(task_id, created_at, *, user_name="tester", priority_class="", status=TASK_PENDING,
          workflow="gen_image.json", queue_class="interactive", preemptions=0):
    return RenderTask(
        id=task_id,
        prompt=RenderPrompt(prompt="a car", user_name=user_name, priority_class=priority_class),
        status=status, created_at=created_at, workflow=workflow, queue_class=queue_class,
        preemptions=preemptions,
    )


# ---------------------------------------------------------------- priority.py

class TaskTierTests(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(priority.task_tier(_task("m", 1, priority_class="mplus")), "mplus")
        self.assertEqual(priority.task_tier(_task("a", 1, priority_class="autorig")), "autorig")
        self.assertEqual(priority.task_tier(_task("s", 1, user_name="site")), "autorig")
        self.assertEqual(priority.task_tier(_task("g", 1, user_name="sura-bot")), "generation")
        self.assertEqual(priority.task_tier(_task("l", 1, priority_class="red_autopost")), "generation")
        self.assertEqual(priority.task_tier(_task("b", 1, priority_class="mplus",
                                                  queue_class="collection_background")), "background")

    def test_rank_and_broker_classes(self):
        self.assertEqual(list(priority.TIERS), ["mplus", "autorig", "generation", "background"])
        self.assertEqual(priority.workload_class("mplus"), "ai_vision")
        self.assertEqual(priority.workload_class("autorig"), "autorig_interactive")
        self.assertEqual(priority.workload_class("generation"), "comfy")
        self.assertEqual(priority.workload_class("background"), "collection_background")
        # the broker refuses Renderfin anything but these
        self.assertEqual(priority.broker_lease_class("ai_vision"), "comfy")
        self.assertEqual(priority.broker_lease_class("autorig_interactive"), "comfy")
        self.assertEqual(priority.broker_lease_class("collection_background"), "collection_background")
        self.assertEqual(priority.broker_lease_class("comfy"), "comfy")

    def test_switch_off_is_the_old_rule(self):
        with patch.object(config, "TIER_PRIORITY", False):
            self.assertEqual(priority.task_tier(_task("m", 1, priority_class="mplus")), "generation")
            self.assertEqual(priority.task_tier(_task("s", 1, user_name="site")), "autorig")
        # is_autorig stays the 2026-10-15 rule either way
        self.assertTrue(is_autorig(_task("s", 1, user_name="site")))
        self.assertFalse(is_autorig(_task("m", 1, priority_class="mplus")))

    def test_tier_boxes_config(self):
        self.assertEqual(config.parse_tier_boxes("mplus=f13-ai,f7-ai"), {"mplus": {"f13-ai", "f7-ai"}})
        self.assertEqual(config.parse_tier_boxes("mplus=f15;autorig=worker-4090,f12"),
                         {"mplus": {"f15"}, "autorig": {"worker-4090", "f12"}})
        self.assertEqual(config.parse_tier_boxes(""), {})
        with patch.object(config, "TIER_BOXES", {}), \
             patch.object(config, "AUTORIG_PRIORITY_BOXES", frozenset({"worker-4090"})):
            self.assertEqual(priority.tier_boxes("autorig"), frozenset({"worker-4090"}))
            self.assertEqual(priority.tier_boxes("mplus"), frozenset())


class HeaderTrustTests(unittest.TestCase):
    def setUp(self):
        p = patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": f"{KEY}, {OTHER_KEY} ,short"})
        p.start()
        self.addCleanup(p.stop)

    def cls(self, headers):
        return priority.priority_class_from_headers({k.lower(): v for k, v in headers.items()})

    def test_mplus_from_outside_with_a_valid_key(self):
        outside = {"X-Real-IP": "1.2.3.4", "X-Autorig-Priority": "mplus"}
        self.assertEqual(self.cls(dict(outside, **{"X-Priority-Key": KEY})), "mplus")
        self.assertEqual(self.cls(dict(outside, **{"X-Priority-Key": OTHER_KEY})), "mplus")

    def test_mplus_refused_without_the_key(self):
        outside = {"X-Real-IP": "1.2.3.4", "X-Autorig-Priority": "mplus"}
        self.assertEqual(self.cls(outside), "")
        self.assertEqual(self.cls(dict(outside, **{"X-Priority-Key": "wrong" * 8})), "")
        self.assertEqual(self.cls(dict(outside, **{"X-Priority-Key": "short"})), "")   # under 16 chars
        # inside the host the key is still required for mplus
        self.assertEqual(self.cls({"X-Autorig-Priority": "mplus"}), "")
        with patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": ""}):
            self.assertEqual(self.cls(dict(outside, **{"X-Priority-Key": KEY})), "")

    def test_autorig_and_labels_keep_the_inside_rule(self):
        self.assertEqual(self.cls({"X-Autorig-Priority": "autorig"}), "autorig")
        self.assertEqual(self.cls({"X-Autorig-Priority": "autorig", "X-Forwarded-For": "1.2.3.4"}), "")
        # a valid M+ key does not open autorig from outside
        self.assertEqual(self.cls({"X-Autorig-Priority": "autorig", "X-Real-IP": "1.2.3.4",
                                   "X-Priority-Key": KEY}), "")
        self.assertEqual(self.cls({"X-Autorig-Priority": "red_autopost"}), "red_autopost")
        self.assertEqual(self.cls({"X-Autorig-Priority": "red_autopost", "X-Real-IP": "1.2.3.4"}), "")

    def test_request_tier(self):
        self.assertEqual(priority.request_tier({"x-autorig-priority": "mplus", "x-real-ip": "1.1.1.1",
                                                "x-priority-key": KEY}), "mplus")
        self.assertEqual(priority.request_tier({"x-autorig-priority": "autorig"}), "autorig")
        self.assertEqual(priority.request_tier({}), "generation")

    def test_switch_off_is_the_2026_10_16_rule(self):
        with patch.object(config, "TIER_PRIORITY", False):
            self.assertEqual(self.cls({"X-Autorig-Priority": "mplus", "X-Real-IP": "1.1.1.1",
                                       "X-Priority-Key": KEY}), "")
            self.assertEqual(self.cls({"X-Autorig-Priority": "mplus"}), "mplus")   # a plain internal label
            self.assertEqual(self.cls({"X-Autorig-Priority": "autorig"}), "autorig")

    def test_render_body_cannot_claim_mplus_through_nginx(self):
        forwarded = {"x-real-ip": "1.2.3.4"}
        self.assertEqual(priority.sanitize_priority_class("mplus", forwarded), "")
        self.assertEqual(priority.sanitize_priority_class("mplus", dict(forwarded, **{"x-priority-key": KEY})), "mplus")
        self.assertEqual(priority.sanitize_priority_class("mplus", {}), "mplus")         # the site's own loopback call
        self.assertEqual(priority.sanitize_priority_class("red_autopost", forwarded), "red_autopost")

    def test_describe_never_shows_a_key(self):
        text = repr(priority.describe())
        self.assertNotIn(KEY, text)
        self.assertNotIn(OTHER_KEY, text)
        self.assertEqual(priority.describe()["priority_keys_configured_int"], 2)


class MiddlewareTests(unittest.TestCase):
    """The task-owner middleware puts the trusted class in the request context."""

    def setUp(self):
        import ai_graph_context
        import task_owner
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        for p in (patch.object(task_owner, "DB_PATH", Path(self._dir.name) / "owners.sqlite3"),
                  patch.object(task_owner, "_conn", None),
                  patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": KEY})):
            p.start()
            self.addCleanup(p.stop)
        app = FastAPI()
        self.seen = {}

        @app.post("/api/render-me")
        async def render_me():
            self.seen["fields"] = ai_graph_context.fields()
            return {"success_bool": True}

        app.add_middleware(task_owner.TaskOwnerMiddleware)
        self.client = TestClient(app)

    def test_key_accepted(self):
        self.client.post("/api/render-me", json={}, headers={
            "X-Real-IP": "5.6.7.8", "X-Autorig-Priority": "mplus", "X-Priority-Key": KEY})
        self.assertEqual(self.seen["fields"], {"priority_class": "mplus"})

    def test_key_rejected(self):
        self.client.post("/api/render-me", json={}, headers={
            "X-Real-IP": "5.6.7.8", "X-Autorig-Priority": "mplus", "X-Priority-Key": "nope" * 8})
        self.assertEqual(self.seen["fields"], {})

    def test_internal_autorig(self):
        self.client.post("/api/render-me", json={}, headers={"X-Autorig-Priority": "autorig"})
        self.assertEqual(self.seen["fields"], {"priority_class": "autorig"})


# ---------------------------------------------------------------- queue order

class TieredDispatchOrderTests(unittest.TestCase):
    def test_tier_first_then_the_usual_order(self):
        now = 10_000.0
        tasks = [
            _task("gen-new", now - 5, user_name="sura-bot"),
            _task("bulk", now - 50, user_name="site", queue_class="collection_background"),
            _task("rig", now - 30, priority_class="autorig"),
            _task("m-2", now - 10, priority_class="mplus"),
            _task("gen-old", now - 120, user_name="sura-bot"),
            _task("m-1", now - 20, priority_class="mplus"),
            _task("running", now - 90, priority_class="mplus", status=TASK_RENDERING),
        ]
        order = [t.id for t in dispatch_order(tasks, now=now)]
        # mplus oldest first, then AutoRig, then generation (LIFO bursts), background last
        self.assertEqual(order, ["m-1", "m-2", "rig", "gen-new", "gen-old", "bulk"])

    def test_starving_generation_stays_below_higher_tiers(self):
        now = 10_000.0
        tasks = [
            _task("gen-starving", now - (config.STARVE_MINUTES + 5) * 60, user_name="x"),
            _task("gen-new", now - 1, user_name="x"),
            _task("m", now - 1, priority_class="mplus"),
        ]
        self.assertEqual([t.id for t in dispatch_order(tasks, now=now)], ["m", "gen-starving", "gen-new"])

    def test_starved_background_joins_generations_starving(self):
        now = 10_000.0
        tasks = [
            _task("gen-new", now - 1, user_name="x"),
            _task("bulk-old", now - (config.STARVE_MINUTES + 1) * 60, user_name="x",
                  queue_class="collection_background"),
            _task("bulk-new", now - 2, user_name="x", queue_class="collection_background"),
        ]
        self.assertEqual([t.id for t in dispatch_order(tasks, now=now)], ["bulk-old", "gen-new", "bulk-new"])

    def test_twice_preempted_goes_first_in_its_tier(self):
        now = 10_000.0
        tasks = [
            _task("gen-new", now - 1, user_name="x"),
            _task("gen-bumped", now - 60, user_name="x", preemptions=2),
        ]
        self.assertEqual([t.id for t in dispatch_order(tasks, now=now)], ["gen-bumped", "gen-new"])

    def test_switch_off_restores_the_old_order(self):
        now = 10_000.0
        tasks = [
            _task("gen", now - 5, user_name="x"),
            _task("bulk", now - 1, user_name="x", queue_class="collection_background"),
            _task("m", now - 3, priority_class="mplus"),
            _task("rig", now - 40, priority_class="autorig"),
        ]
        with patch.object(config, "TIER_PRIORITY", False):
            order = [t.id for t in dispatch_order(tasks, now=now)]
        self.assertEqual(order[0], "rig")
        # one submission burst in its own order: mplus is not ranked and
        # background is not pushed last when tiers are off
        self.assertEqual(order[1:], ["gen", "m", "bulk"])


# ---------------------------------------------------------------- queue boxes

def _server(name, workflows=("gen_image.json", "gen_video_minimax_h3_by_url.json", "image_to_3d.json"),
            avg=100.0):
    return RenderServer(render_server_name=name, render_server_url=f"http://{name}:8188",
                        status="online", available_workflows=list(workflows), average_render_time=avg)


async def _noop(*args, **kwargs):
    return None


class _QueueCase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        root = Path(self._dir.name)
        for p in (patch.object(config, "DATA_DIR", root),
                  patch.object(config, "RENDER_DIR", root / "render"),
                  patch.object(config, "DB_DIR", root / "db"),
                  patch.object(config, "TMP_DIR", root / "tmp"),
                  patch.object(config, "SERVERS_DIR", root / "servers"),
                  patch.object(config, "DB_PATH", root / "db" / "renderfin.db"),
                  patch.object(config, "AUTORIG_PRIORITY_BOXES", frozenset({"worker-4090"})),
                  patch.object(config, "TIER_BOXES", {"mplus": {"f15"}}),
                  patch.object(config, "TIER_PREEMPT_SECONDS", {"mplus": 15.0}),
                  patch.object(config, "AUTORIG_PREEMPT_SECONDS", 45.0),
                  patch.object(config, "OOM_RECOVERY_ENABLED", False)):
            p.start()
            self.addCleanup(p.stop)
        registry = ServerRegistry()
        for name in ("worker-4090", "f15", "f12"):
            registry.save(_server(name))
        self.queue = RenderQueue(registry, db_path=root / "renderfin.db")
        self.freed = []

        async def fake_free(box, prompt_id):
            self.freed.append((box, prompt_id))
            return "interrupted and freed"

        self.queue._free_box_after_restart = fake_free

        async def fake_persist(task):
            return None

        self.queue._persist = fake_persist


class ReservationTests(_QueueCase):
    def test_mplus_box_is_held_while_mplus_waits(self):
        m = _task("m", 10.0, priority_class="mplus")
        gen = _task("gen", 5.0, user_name="x")
        rig = _task("rig", 5.0, priority_class="autorig")
        self.queue._tasks = {t.id: t for t in (m, gen, rig)}
        f15 = self.queue.registry.get("f15")
        f12 = self.queue.registry.get("f12")
        self.assertEqual(self.queue._exclusion_reason(f15, gen.workflow, {}, None, 100.0, task=gen),
                         ("reserved-mplus", "m"))
        self.assertEqual(self.queue._exclusion_reason(f15, rig.workflow, {}, None, 100.0, task=rig),
                         ("reserved-mplus", "m"))
        self.assertIsNone(self.queue._exclusion_reason(f15, m.workflow, {}, None, 100.0, task=m))
        self.assertIsNone(self.queue._exclusion_reason(f12, gen.workflow, {}, None, 100.0, task=gen))

    def test_4090_still_held_for_autorig_but_not_against_mplus(self):
        rig = _task("rig", 10.0, priority_class="autorig")
        gen = _task("gen", 5.0, user_name="x")
        m = _task("m", 5.0, priority_class="mplus")
        self.queue._tasks = {t.id: t for t in (rig, gen, m)}
        box = self.queue.registry.get("worker-4090")
        self.assertEqual(self.queue._exclusion_reason(box, gen.workflow, {}, None, 100.0, task=gen),
                         ("reserved-autorig", "rig"))
        self.assertIsNone(self.queue._exclusion_reason(box, m.workflow, {}, None, 100.0, task=m))

    def test_pick_server_steers_mplus_to_its_box_and_others_away(self):
        m = _task("m", 10.0, priority_class="mplus")
        gen = _task("gen", 5.0, user_name="x")
        self.queue._tasks = {gen.id: gen}
        depths = {"worker-4090": 0, "f15": 0, "f12": 0}
        self.assertNotEqual(self.queue._pick_server(gen.workflow, depths, gen).render_server_name, "f15")
        self.queue._tasks = {m.id: m}
        self.assertEqual(self.queue._pick_server(m.workflow, depths, m).render_server_name, "f15")


class PreemptionTests(_QueueCase):
    def _running(self, task_id, box, *, started_ago=10.0, **kw):
        t = _task(task_id, time.time() - 300, status=TASK_RENDERING, **kw)
        t.server_name = box
        t.comfy_prompt_id = f"p-{task_id}"
        t.started_at = time.time() - started_ago
        return t

    def _go(self, task):
        with patch.object(workload_lease, "cancel_waiter", new=_noop), \
             patch.object(workload_lease, "release", new=_noop):
            return run(self.queue._preempt_for_tier(task))

    def test_mplus_takes_its_box_from_generation_after_15_s(self):
        victim = self._running("gen", "f15", user_name="x")
        young = _task("m-young", time.time() - 5, priority_class="mplus")
        old = _task("m-old", time.time() - 20, priority_class="mplus")
        self.queue._tasks = {t.id: t for t in (victim, young, old)}
        self.assertFalse(self._go(young))
        self.assertTrue(self._go(old))
        self.assertEqual(self.freed, [("f15", "p-gen")])
        self.assertEqual(victim.status, TASK_PENDING)
        self.assertEqual(victim.preemptions, 1)
        self.assertEqual(victim.notice, config.MPLUS_PREEMPTED_NOTICE)
        self.assertIn("preempted-for-mplus:m-old@f15", victim.recovery_log)

    def test_one_preemption_per_box_per_interval(self):
        a = self._running("gen-a", "f15", user_name="x")
        self.queue._tasks = {a.id: a}
        m = _task("m", time.time() - 20, priority_class="mplus")
        self.queue._tasks[m.id] = m
        self.assertTrue(self._go(m))
        b = self._running("gen-b", "f15", user_name="x")
        self.queue._tasks[b.id] = b
        self.assertFalse(self._go(m))
        self.queue._tier_preempted_at["f15"] -= config.TIER_PREEMPT_BOX_INTERVAL_SECONDS + 1
        self.assertTrue(self._go(m))

    def test_victim_guards(self):
        m = _task("m", time.time() - 20, priority_class="mplus")
        cases = {
            "nearly-done": self._running("v1", "f15", user_name="x", started_ago=80.0),   # 80 % of avg 100 s
            "preempted-enough": self._running("v2", "f15", user_name="x", preemptions=2),
            "3d": self._running("v3", "f15", user_name="x", workflow="image_to_3d.json"),
        }
        near_restart = self._running("v4", "f15", user_name="x")
        near_restart.restart_resumes = config.RESTART_RESUME_MAX - 1
        cases["near-restart-cap"] = near_restart
        near_submit = self._running("v5", "f15", user_name="x")
        near_submit.submit_failures = 2
        cases["near-submit-cap"] = near_submit
        for why, victim in cases.items():
            with self.subTest(why=why):
                self.queue._tasks = {victim.id: victim, m.id: m}
                self.queue._tier_preempted_at.clear()
                self.assertFalse(self._go(m))
                self.assertEqual(victim.status, TASK_RENDERING)

    def test_strictly_lower_tier_only(self):
        # mplus may take an AutoRig render on an mplus box ...
        rig = self._running("rig", "f15", priority_class="autorig")
        m = _task("m", time.time() - 20, priority_class="mplus")
        self.queue._tasks = {rig.id: rig, m.id: m}
        self.assertTrue(self._go(m))
        # ... but nothing preempts mplus, and generation never preempts
        self.queue._tier_preempted_at.clear()
        mv = self._running("mv", "worker-4090", priority_class="mplus")
        r2 = _task("r2", time.time() - 100, priority_class="autorig")
        g = _task("g", time.time() - 10_000, user_name="x")
        self.queue._tasks = {mv.id: mv, r2.id: r2, g.id: g}
        self.assertFalse(self._go(r2))
        self.assertFalse(self._go(g))

    def test_autorig_on_the_4090_as_before(self):
        victim = self._running("video", "worker-4090", user_name="sura-bot")
        old = _task("rig", time.time() - 120, priority_class="autorig")
        young = _task("rig-young", time.time() - 5, priority_class="autorig")
        self.queue._tasks = {t.id: t for t in (victim, old, young)}
        self.assertFalse(self._go(young))
        self.assertTrue(self._go(old))
        self.assertEqual(victim.notice, config.AUTORIG_PREEMPTED_NOTICE)
        self.assertEqual(victim.restart_resumes, 0)

    def test_h3_video_goes_last(self):
        h3 = self._running("h3", "f15", user_name="x", workflow="gen_video_minimax_h3_by_url.json")
        img = self._running("img", "f15", user_name="x", started_ago=50.0)
        m = _task("m", time.time() - 20, priority_class="mplus")
        self.queue._tasks = {t.id: t for t in (h3, img, m)}
        self.assertTrue(self._go(m))
        self.assertEqual(img.status, TASK_PENDING)
        self.assertEqual(h3.status, TASK_RENDERING)

    def test_switch_off_runs_the_old_autorig_preemption(self):
        victim = self._running("gen", "f15", user_name="x")
        m = _task("m", time.time() - 600, priority_class="mplus")
        self.queue._tasks = {victim.id: victim, m.id: m}
        with patch.object(config, "TIER_PRIORITY", False):
            self.assertFalse(self._go(m))
        self.assertEqual(victim.status, TASK_RENDERING)


class EnqueueAndBrokerTests(_QueueCase):
    def test_workload_class_follows_the_tier(self):
        async def scenario():
            await self.queue.start()
            try:
                m = await self.queue.enqueue(RenderPrompt(prompt="x", priority_class="mplus"))
                r = await self.queue.enqueue(RenderPrompt(prompt="x", priority_class="autorig"))
                g = await self.queue.enqueue(RenderPrompt(prompt="x"))
                b = await self.queue.enqueue(RenderPrompt(prompt="x"), queue_class="collection_background")
                return m, r, g, b
            finally:
                await self.queue.stop()

        m, r, g, b = run(scenario())
        self.assertEqual((m.workload_class, r.workload_class, g.workload_class, b.workload_class),
                         ("ai_vision", "autorig_interactive", "comfy", "collection_background"))
        self.assertEqual(m.public_dict()["tier_string"], "mplus")

    def test_workload_class_unchanged_when_off(self):
        async def scenario():
            await self.queue.start()
            try:
                return await self.queue.enqueue(RenderPrompt(prompt="x", priority_class="mplus"))
            finally:
                await self.queue.stop()

        with patch.object(config, "TIER_PRIORITY", False):
            m = run(scenario())
        self.assertEqual(m.workload_class, "comfy")

    def test_unverified_identity_fallback(self):
        f15 = self.queue.registry.get("f15")
        with patch.object(workload_lease, "enabled", return_value=True), \
             patch.object(workload_lease, "managed_server", return_value=True):
            with patch.object(config, "WORKLOAD_BROKER_UNVERIFIED", "offline"):
                self.assertFalse(self.queue._broker_identity_fallback(f15, False))
            with patch.object(config, "WORKLOAD_BROKER_UNVERIFIED", "unmanaged"):
                self.assertTrue(self.queue._broker_identity_fallback(f15, False))
                self.assertIn("f15", self.queue._broker_unmanaged)
                # a leased task bound to the box keeps it fail-closed
                leased = _task("leased", 1.0, status=TASK_RENDERING)
                leased.server_name = "f15"
                leased.workload_lease_id = "lease-1"
                self.queue._tasks = {leased.id: leased}
                self.assertFalse(self.queue._broker_identity_fallback(f15, False))
                self.assertNotIn("f15", self.queue._broker_unmanaged)
                self.assertTrue(self.queue._broker_identity_fallback(f15, True))
        # broker off: nothing changes
        self.assertFalse(self.queue._broker_identity_fallback(f15, False))


# ---------------------------------------------------------------- vision nodes

class VisionPinningTests(unittest.TestCase):
    def setUp(self):
        import ai_vision_api
        self.api = ai_vision_api
        for p in (patch.object(config, "TIER_VISION_BOXES", {"mplus": {"f13-ai", "f7-ai"}}),
                  patch.dict(ai_vision_api._AI_MPLUS_INFLIGHT, clear=True),
                  patch.dict(ai_vision_api._AI_RECENT_PICKS, clear=True),
                  patch.dict(ai_vision_api._AI_SUBMITTED, clear=True)):
            p.start()
            self.addCleanup(p.stop)
        self.workers = [{"name": n, "url": f"http://{n}", "token": "t", "physical_node": n}
                        for n in ("f11-ai", "f13-ai", "f2-ai", "f7-ai")]
        self.loads = {"f11-ai": 0, "f13-ai": 0, "f2-ai": 0, "f7-ai": 0}
        self.activities = {}

    def _pick(self, tier=None):
        async def probe(client, worker):
            name = worker["physical_node"]
            return True, {"load": self.loads[name], "models": ["qwen35-9b-uncensored"],
                          "loaded": "qwen35-9b-uncensored", "activities": self.activities.get(name, [])}

        async def go():
            with mock.patch.object(self.api, "_load_ai_workers", return_value=self.workers), \
                 mock.patch.object(self.api, "_probe_for_ai", probe):
                kwargs = {"tier": tier} if tier else {}
                return await self.api._pick_worker(None, "qwen35-9b-uncensored", **kwargs)

        return run(go())["physical_node"]

    def test_mplus_prefers_pinned_nodes(self):
        self.assertIn(self._pick("mplus"), {"f13-ai", "f7-ai"})
        # a pinned node behind a conversion is skipped for M+
        self.activities = {"f13-ai": ["conversion"], "f7-ai": ["conversion"]}
        self.assertIn(self._pick("mplus"), {"f11-ai", "f2-ai"})

    def test_lower_tiers_avoid_a_pinned_node_with_mplus_in_flight(self):
        self.loads = {"f11-ai": 1, "f13-ai": 0, "f2-ai": 1, "f7-ai": 1}
        self.assertEqual(self._pick("generation"), "f13-ai")   # idle and nothing of M+ on it
        self.api._note_mplus("f13-ai.t1", self.workers[1])
        self.api._AI_RECENT_PICKS.clear()
        self.assertNotEqual(self._pick("generation"), "f13-ai")
        self.api._note_finished("f13-ai.t1", "Completed")
        self.api._AI_RECENT_PICKS.clear()
        self.assertEqual(self._pick("generation"), "f13-ai")

    def test_nothing_changes_without_pins_or_with_tiers_off(self):
        self.loads = {"f11-ai": 0, "f13-ai": 2, "f2-ai": 2, "f7-ai": 2}
        with patch.object(config, "TIER_VISION_BOXES", {}):
            self.assertEqual(self._pick("mplus"), "f11-ai")
        with patch.object(config, "TIER_PRIORITY", False):
            self.assertEqual(self._pick("mplus"), "f11-ai")

    def test_request_headers_reach_the_picker(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        import ai_request_cache

        seen = {}

        async def fake_pick(client, model_id=None, **kwargs):
            seen["tier"] = kwargs.get("tier")
            return self.workers[1], {"models": [model_id or ""], "load": 0}

        async def fake_submit(client, worker, path, payload):
            return "abc"

        app = FastAPI()
        app.include_router(self.api.router)
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        with mock.patch.object(ai_request_cache, "_default_cache",
                               ai_request_cache.AIRequestCache(Path(folder.name) / "c.sqlite3")), \
             mock.patch.object(self.api, "_pick_worker", fake_pick), \
             mock.patch.object(self.api, "_submit", fake_submit), \
             patch.dict(os.environ, {"RENDERFIN_PRIORITY_KEYS": KEY}):
            client = TestClient(app)
            client.post("/api/text2text", json={"prompt": "hi 1"}, headers={
                "X-Real-IP": "1.2.3.4", "X-Autorig-Priority": "mplus", "X-Priority-Key": KEY})
            self.assertEqual(seen["tier"], "mplus")
            self.assertIn("f13-ai.abc", self.api._AI_MPLUS_INFLIGHT)
            client.post("/api/text2text", json={"prompt": "hi 2"}, headers={
                "X-Real-IP": "1.2.3.4", "X-Autorig-Priority": "mplus", "X-Priority-Key": "bad" * 8})
            self.assertEqual(seen["tier"], "generation")


class VisionWarmTests(unittest.TestCase):
    def test_only_idle_cold_pinned_nodes_are_pinged(self):
        import ai_vision_api as api
        workers = [{"name": n, "url": f"http://{n}", "token": "t", "physical_node": n}
                   for n in ("f13-ai", "f7-ai", "f11-ai", "f2-ai")]
        infos = {
            "f13-ai": {"load": 0, "models": [api.DEFAULT_MODEL_ID], "loaded": ""},                 # cold, idle -> ping
            "f7-ai": {"load": 0, "models": [api.DEFAULT_MODEL_ID], "loaded": api.DEFAULT_MODEL_ID},  # warm
            "f11-ai": {"load": 0, "models": [api.DEFAULT_MODEL_ID], "loaded": ""},                 # not pinned
            "f2-ai": {"load": 3, "models": [api.DEFAULT_MODEL_ID], "loaded": ""},                  # pinned but busy
        }
        sent = []

        async def probe(client, worker):
            return True, dict(infos[worker["physical_node"]])

        async def submit(client, worker, path, payload):
            sent.append((worker["physical_node"], path, payload["model"]))
            return "w1"

        async def go():
            with mock.patch.object(config, "TIER_VISION_BOXES", {"mplus": {"f13-ai", "f7-ai", "f2-ai"}}), \
                 mock.patch.object(api, "_load_ai_workers", return_value=workers), \
                 mock.patch.object(api, "_probe_for_ai", probe), \
                 mock.patch.object(api, "_submit", submit), \
                 patch.dict(api._AI_SUBMITTED, clear=True), \
                 patch.dict(api._AI_RECENT_PICKS, clear=True):
                return await api._warm_pinned_once(None)

        self.assertEqual(run(go()), ["f13-ai"])
        self.assertEqual(sent, [("f13-ai", "/text2text", api.DEFAULT_MODEL_ID)])

    def test_warmer_off_by_default(self):
        import ai_vision_api as api
        with mock.patch.object(config, "VISION_WARM_MINUTES", 0):
            self.assertIsNone(api.start_vision_warmer())


# ---------------------------------------------------------------- observability

class FleetTierTests(unittest.TestCase):
    def test_queue_object_has_a_tier_breakdown(self):
        import ai_fleet
        tasks = [
            {"status": "Rendering", "render_server_name": "f15", "workflow": "gen_image.json",
             "started_at": 1.0, "tier_string": "mplus"},
            {"status": "Pending", "workflow": "gen_image.json", "created_at": 2.0, "tier_string": "generation"},
            {"status": "Pending", "workflow": "gen_image.json", "created_at": 3.0, "tier_string": "generation"},
            {"status": "Pending", "workflow": "gen_image.json", "created_at": 4.0},
        ]
        servers = [{"render_server_name": "f15", "status": "online", "available_workflows": ["gen_image.json"]}]
        summary = ai_fleet._queue_summary(tasks, servers, {}, now=10.0)
        self.assertEqual(summary["by_tier_object"], {
            "mplus": {"running_int": 1, "waiting_int": 0},
            "generation": {"running_int": 0, "waiting_int": 2},
            "unknown": {"running_int": 0, "waiting_int": 1},
        })


if __name__ == "__main__":
    unittest.main()

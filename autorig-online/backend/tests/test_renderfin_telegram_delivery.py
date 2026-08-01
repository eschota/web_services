import asyncio
import json
import unittest
from unittest.mock import patch

import httpx

from renderfin import config, telegram_delivery
from renderfin.models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_READY,
    CharacterGenJob,
)
from renderfin.telegram_delivery import (
    DELIVERY_FAILED,
    DELIVERY_IMAGE,
    DELIVERY_MODEL,
    TelegramDeliveryService,
    pending_delivery,
)


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeManager:
    """Stand-in for CharacterGenManager with the two methods delivery needs."""

    def __init__(self, jobs):
        self.jobs = {j.id: j for j in jobs}
        self.marks = []

    def all_jobs(self):
        return list(self.jobs.values())

    def stats(self):
        return {"total": len(self.jobs), "current_24h": 7, "previous_24h": 5, "delta_24h": 2}

    async def mark_delivered(self, job_id, kind, marker, *, message_id=0, clear_status_message=False):
        job = self.jobs[job_id]
        delivered = dict(job.delivered or {})
        delivered[kind] = marker or ""
        job.delivered = delivered
        if clear_status_message:
            job.telegram_status_message_id = 0
        self.marks.append((job_id, kind, marker))
        return job


def _job(**kw) -> CharacterGenJob:
    base = dict(prompt="orc warrior", telegram_chat_id=777)
    base.update(kw)
    return CharacterGenJob(**base)


class PendingDeliveryTests(unittest.TestCase):
    def test_awaiting_image_is_pending_until_delivered(self):
        job = _job(stage=CHARGEN_STAGE_AWAITING_IMAGE, image_url="https://x/a.png")
        self.assertEqual(pending_delivery(job), DELIVERY_IMAGE)
        job.delivered = {DELIVERY_IMAGE: "https://x/a.png"}
        self.assertIsNone(pending_delivery(job))

    def test_regenerated_image_is_delivered_again(self):
        job = _job(
            stage=CHARGEN_STAGE_AWAITING_IMAGE,
            image_url="https://x/b.png",
            delivered={DELIVERY_IMAGE: "https://x/a.png"},
        )
        self.assertEqual(pending_delivery(job), DELIVERY_IMAGE)

    def test_ready_and_failed_states(self):
        ready = _job(stage=CHARGEN_STAGE_READY, video_url="https://x/v.mp4")
        self.assertEqual(pending_delivery(ready), DELIVERY_MODEL)
        failed = _job(stage=CHARGEN_STAGE_FAILED, error="boom")
        self.assertEqual(pending_delivery(failed), DELIVERY_FAILED)
        # a second, different failure is delivered again
        failed.delivered = {DELIVERY_FAILED: "boom"}
        self.assertIsNone(pending_delivery(failed))
        failed.error = "another boom"
        self.assertEqual(pending_delivery(failed), DELIVERY_FAILED)

    def test_in_progress_and_chatless_jobs_are_never_pending(self):
        self.assertIsNone(pending_delivery(_job(stage=CHARGEN_STAGE_HUNYUAN)))
        self.assertIsNone(
            pending_delivery(
                _job(stage=CHARGEN_STAGE_READY, video_url="https://x/v.mp4", telegram_chat_id=0)
            )
        )


class DeliveryTickTests(unittest.TestCase):
    def _service(self, jobs, handler):
        manager = _FakeManager(jobs)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return TelegramDeliveryService(manager, client=client), manager, client

    def test_image_review_sent_with_buttons(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append((request.url.path, dict(httpx.QueryParams(request.content.decode()))))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 500}})

            job = _job(
                stage=CHARGEN_STAGE_AWAITING_IMAGE,
                image_url="https://x/a.png",
                isolated_url="https://x/a_Isolated.png",
                telegram_status_message_id=99,
            )
            service, manager, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()

            paths = [p for p, _ in sent]
            self.assertIn("/botT/sendPhoto", paths)
            # the interim status message is cleaned up
            self.assertIn("/botT/deleteMessage", paths)
            payload = dict(sent[0][1])
            self.assertEqual(payload["photo"], "https://x/a.png")
            markup = json.loads(payload["reply_markup"])
            data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
            self.assertEqual(data, [f"rfa:{job.id}", f"rfr:{job.id}", f"rfd:{job.id}"])
            self.assertEqual(manager.marks[0][1], DELIVERY_IMAGE)
            self.assertEqual(job.telegram_status_message_id, 0)

        run(scenario())

    def test_model_video_sent_with_submit_buttons(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append((request.url.path, dict(httpx.QueryParams(request.content.decode()))))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 501}})

            job = _job(
                stage=CHARGEN_STAGE_READY,
                video_url="https://x/v.mp4",
                glb_url="https://x/m.glb",
            )
            service, manager, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()

            self.assertEqual(sent[0][0], "/botT/sendVideo")
            payload = dict(sent[0][1])
            self.assertEqual(payload["video"], "https://x/v.mp4")
            markup = json.loads(payload["reply_markup"])
            data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
            self.assertEqual(data, [f"rfs:{job.id}", f"rfd:{job.id}"])
            self.assertEqual(job.delivered[DELIVERY_MODEL], "https://x/v.mp4")

        run(scenario())

    def test_failure_sent_with_recovery_buttons(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append((request.url.path, dict(httpx.QueryParams(request.content.decode()))))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 502}})

            job = _job(stage=CHARGEN_STAGE_FAILED, error="generation timed out")
            service, _, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()

            self.assertEqual(sent[0][0], "/botT/sendMessage")
            markup = json.loads(dict(sent[0][1])["reply_markup"])
            data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
            self.assertEqual(data, [f"rfe:{job.id}", f"rfr:{job.id}", f"rfd:{job.id}"])

        run(scenario())

    def test_delivery_is_not_repeated(self):
        async def scenario():
            calls = {"n": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("sendVideo"):
                    calls["n"] += 1
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

            job = _job(stage=CHARGEN_STAGE_READY, video_url="https://x/v.mp4", glb_url="https://x/m.glb")
            service, _, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
                await service.tick()
                await service.tick()
            await client.aclose()
            self.assertEqual(calls["n"], 1)

        run(scenario())

    def test_failed_send_is_retried_until_it_succeeds(self):
        """The user must get the result no matter what — a Telegram outage
        must not drop the delivery."""

        async def scenario():
            attempts = {"n": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("sendVideo"):
                    attempts["n"] += 1
                    if attempts["n"] < 3:
                        return httpx.Response(502, text="bad gateway")
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

            job = _job(stage=CHARGEN_STAGE_READY, video_url="https://x/v.mp4", glb_url="https://x/m.glb")
            service, manager, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                with patch.object(config, "DELIVERY_TICK_SECONDS", 0):
                    await service.tick()   # 502
                    await service.tick()   # 502
                    await service.tick()   # ok
            await client.aclose()
            self.assertEqual(attempts["n"], 3)
            self.assertEqual(manager.marks[-1][1], DELIVERY_MODEL)
            self.assertIsNone(pending_delivery(job))

        run(scenario())

    def test_backoff_defers_but_never_drops(self):
        async def scenario():
            attempts = {"n": 0}

            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("sendMessage"):
                    attempts["n"] += 1
                    return httpx.Response(500, text="boom")
                return httpx.Response(200, json={"ok": True, "result": {}})

            job = _job(stage=CHARGEN_STAGE_FAILED, error="boom")
            service, _, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                with patch.object(config, "DELIVERY_TICK_SECONDS", 5):
                    await service.tick()
                    # backoff is in effect: the next tick must not hammer the API
                    await service.tick()
            await client.aclose()
            self.assertEqual(attempts["n"], 1)
            # still owed, so a later tick will try again
            self.assertEqual(pending_delivery(job), DELIVERY_FAILED)

        run(scenario())

    def test_telegram_rejection_body_is_treated_as_failure(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={"ok": False, "description": "chat not found"})

            job = _job(stage=CHARGEN_STAGE_READY, video_url="https://x/v.mp4", glb_url="https://x/m.glb")
            service, manager, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()
            self.assertEqual(manager.marks, [])
            self.assertEqual(pending_delivery(job), DELIVERY_MODEL)

        run(scenario())


class ConfigTests(unittest.TestCase):
    def test_disabled_without_token(self):
        with patch.object(config, "TELEGRAM_BOT_TOKEN", ""):
            self.assertFalse(telegram_delivery.is_configured())
        with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
            self.assertTrue(telegram_delivery.is_configured())


if __name__ == "__main__":
    unittest.main()


class RetryNoticeTests(unittest.TestCase):
    def test_scheduled_retry_is_reported_once_per_attempt(self):
        """A long silent wait is indistinguishable from a hang: each automatic
        retry tells the owner, but only once per attempt."""

        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append(dict(httpx.QueryParams(request.content.decode())))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})

            from renderfin.models import CHARGEN_STAGE_HUNYUAN
            from renderfin.telegram_delivery import DELIVERY_RETRY

            job = _job(
                stage=CHARGEN_STAGE_HUNYUAN,
                retry_at=9e9,
                last_error="worker rebooted",
                attempts={CHARGEN_STAGE_HUNYUAN: 1},
            )
            self.assertEqual(pending_delivery(job), DELIVERY_RETRY)

            manager = _FakeManager([job])
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            service = TelegramDeliveryService(manager, client=client)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
                await service.tick()   # same attempt: no second message
            await client.aclose()

            self.assertEqual(len(sent), 1)
            self.assertIn("Повторяю", sent[0]["text"])
            self.assertIn("3D-модель", sent[0]["text"])
            self.assertIsNone(pending_delivery(job))

            # a further attempt is reported again
            job.attempts = {CHARGEN_STAGE_HUNYUAN: 2}
            self.assertEqual(pending_delivery(job), DELIVERY_RETRY)

        run(scenario())

    def test_no_retry_notice_without_a_scheduled_retry(self):
        from renderfin.models import CHARGEN_STAGE_HUNYUAN

        job = _job(stage=CHARGEN_STAGE_HUNYUAN, last_error="", retry_at=0)
        self.assertIsNone(pending_delivery(job))


class RunningNumberTests(unittest.TestCase):
    def test_caption_carries_the_number_and_daily_throughput(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append(dict(httpx.QueryParams(request.content.decode())))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

            job = _job(
                seq=42,
                stage=CHARGEN_STAGE_READY,
                video_url="https://x/v.mp4",
                glb_url="https://x/m.glb",
            )
            manager = _FakeManager([job])
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            service = TelegramDeliveryService(manager, client=client)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()
            caption = sent[0]["caption"]
            self.assertIn("#42", caption)
            self.assertIn("24ч 7", caption)
            self.assertIn("+2", caption)

        run(scenario())

    def test_trend_arrows_follow_the_delta(self):
        from renderfin.telegram_delivery import format_stats

        job = _job(seq=5)
        self.assertIn("🟢⇈", format_stats(job, {"current_24h": 20, "delta_24h": 12}))
        self.assertIn("🟢↗", format_stats(job, {"current_24h": 9, "delta_24h": 3}))
        self.assertIn("⚪→", format_stats(job, {"current_24h": 9, "delta_24h": 0}))
        self.assertIn("🔴↘", format_stats(job, {"current_24h": 4, "delta_24h": -2}))
        self.assertIn("🔴⇊", format_stats(job, {"current_24h": 1, "delta_24h": -11}))
        self.assertEqual(format_stats(job, None), "#5")

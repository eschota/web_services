import asyncio
import json
import time
import unittest
from unittest.mock import patch

import httpx

from renderfin import config, telegram_delivery
from renderfin.models import (
    CHARGEN_STAGE_AWAITING_IMAGE,
    CHARGEN_STAGE_FAILED,
    CHARGEN_STAGE_DISCARDED,
    CHARGEN_STAGE_HUNYUAN,
    CHARGEN_STAGE_READY,
    CharacterGenJob,
    SentMessage,
)
from renderfin.telegram_delivery import (
    DELIVERY_FAILED,
    DELIVERY_IMAGE,
    DELIVERY_MODEL,
    DELIVERY_PROGRESS,
    TelegramDeliveryService,
    pending_delivery,
)


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeManager:
    """Stand-in for CharacterGenManager with the methods delivery needs."""

    def __init__(self, jobs):
        self.jobs = {j.id: j for j in jobs}
        self.marks = []

    def all_jobs(self):
        return list(self.jobs.values())

    def stats(self):
        return {"total": len(self.jobs), "current_24h": 7, "previous_24h": 5, "delta_24h": 2}

    async def record_messages(self, job_id, message_ids, at=0.0, kind=""):
        job = self.jobs[job_id]
        stamp = at or time.time()
        known = {m.id for m in job.telegram_messages}
        job.telegram_messages = list(job.telegram_messages) + [
            SentMessage(id=int(m), at=stamp, kind=kind)
            for m in message_ids
            if int(m or 0) and int(m) not in known
        ]
        return job

    async def set_status_message(self, job_id, message_id):
        self.jobs[job_id].telegram_status_message_id = int(message_id or 0)
        return self.jobs[job_id]

    async def set_telegram_messages(self, job_id, messages, *, undeletable=None):
        job = self.jobs[job_id]
        job.telegram_messages = list(messages)
        if undeletable is not None:
            job.telegram_undeletable = list(undeletable)
        return job

    async def mark_delivered(self, job_id, kind, marker, *, message_id=0, clear_status_message=False):
        job = self.jobs[job_id]
        delivered = dict(job.delivered or {})
        delivered[kind] = marker or ""
        job.delivered = delivered
        if message_id:
            job.telegram_message_id = int(message_id)
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
        # an in-flight job owes its one progress line
        self.assertEqual(pending_delivery(_job(stage=CHARGEN_STAGE_HUNYUAN)), DELIVERY_PROGRESS)
        running = _job(stage=CHARGEN_STAGE_HUNYUAN)
        running.delivered = {DELIVERY_PROGRESS: telegram_delivery._progress_marker(running)}
        self.assertIsNone(pending_delivery(running))
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
            payload = dict(sent[0][1])
            self.assertEqual(payload["photo"], "https://x/a.png")
            markup = json.loads(payload["reply_markup"])
            data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
            self.assertEqual(data, [f"rfa:{job.id}:a", f"rfr:{job.id}", f"rfd:{job.id}"])
            # the interim progress line survives: it is the job's one line
            self.assertNotIn("/botT/deleteMessage", paths[1:])
            self.assertEqual(manager.marks[0][1], DELIVERY_IMAGE)

        run(scenario())

    def test_two_variants_sent_as_a_group_with_a_button_each(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append((request.url.path, dict(httpx.QueryParams(request.content.decode()))))
                # sendMediaGroup answers with a list of messages, not one message
                if request.url.path.endswith("sendMediaGroup"):
                    return httpx.Response(
                        200, json={"ok": True, "result": [{"message_id": 500}, {"message_id": 501}]}
                    )
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 502}})

            job = _job(
                stage=CHARGEN_STAGE_AWAITING_IMAGE,
                image_url="https://x/a.png",
                isolated_url="https://x/a_Isolated.png",
                image_url_b="https://x/b.png",
                isolated_url_b="https://x/b_Isolated.png",
            )
            service, manager, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()

            paths = [p for p, _ in sent]
            self.assertIn("/botT/sendMediaGroup", paths)
            group = json.loads(dict(sent[0][1])["media"])
            self.assertEqual([m["media"] for m in group], ["https://x/a.png", "https://x/b.png"])

            choice = dict(sent[1][1])
            markup = json.loads(choice["reply_markup"])
            data = [b["callback_data"] for row in markup["inline_keyboard"] for b in row]
            self.assertEqual(
                data,
                [f"rfa:{job.id}:a", f"rfa:{job.id}:b", f"rfr:{job.id}", f"rfd:{job.id}"],
            )
            self.assertEqual(manager.marks[0][1], DELIVERY_IMAGE)

        run(scenario())

    def test_second_variant_alone_is_a_new_delivery(self):
        """A regenerated pair must not be suppressed by the first pair's marker."""
        job = _job(
            stage=CHARGEN_STAGE_AWAITING_IMAGE,
            image_url="https://x/a.png",
            image_url_b="https://x/b.png",
        )
        job.delivered[DELIVERY_IMAGE] = "https://x/a.png|https://x/old-b.png"
        self.assertEqual(pending_delivery(job), DELIVERY_IMAGE)
        # a review delivered before two-variant rendering keeps its bare marker
        single = _job(stage=CHARGEN_STAGE_AWAITING_IMAGE, image_url="https://x/a.png")
        single.delivered[DELIVERY_IMAGE] = "https://x/a.png"
        self.assertIsNone(pending_delivery(single))
        job.delivered[DELIVERY_IMAGE] = "https://x/a.png|https://x/b.png"
        self.assertIsNone(pending_delivery(job))

    def test_model_video_carries_no_buttons(self):
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
            # choosing a variant is the only decision: nothing to press here
            self.assertNotIn("reply_markup", payload)
            self.assertEqual(job.delivered[DELIVERY_MODEL], "https://x/v.mp4")

        run(scenario())

    def test_failure_carries_no_buttons(self):
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
            payload = dict(sent[0][1])
            # nothing to press: the pipeline retries by itself, and a dead
            # button in a finished chat is exactly the clutter being removed
            self.assertNotIn("reply_markup", payload)
            self.assertIn("timed out", payload["text"])

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


class ProgressLineTests(unittest.TestCase):
    """A running job keeps exactly one line, rewritten as it moves."""

    def test_a_retry_rewrites_the_line_instead_of_sending_another(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append((request.url.path, dict(httpx.QueryParams(request.content.decode()))))
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})

            job = _job(stage=CHARGEN_STAGE_HUNYUAN)
            manager = _FakeManager([job])
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            service = TelegramDeliveryService(manager, client=client)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()          # first line
                await service.tick()          # unchanged: nothing to say
                job.attempts = {CHARGEN_STAGE_HUNYUAN: 1}
                job.retry_at = 9e9
                job.last_error = "worker rebooted"
                await service.tick()          # a retry: same message, new text
            await client.aclose()

            methods = [path.rsplit("/", 1)[-1] for path, _ in sent]
            self.assertEqual(methods, ["sendMessage", "editMessageText"])
            self.assertIn("попытка 2", sent[1][1]["text"])
            self.assertEqual(int(sent[1][1]["message_id"]), 7)

        run(scenario())

    def test_the_line_says_which_stage_is_running(self):
        job = _job(stage=CHARGEN_STAGE_HUNYUAN)
        self.assertIn("3D-модель", telegram_delivery.progress_text(job))

    def test_a_finished_job_owes_no_line(self):
        for stage in (CHARGEN_STAGE_DISCARDED, CHARGEN_STAGE_AWAITING_IMAGE):
            job = _job(stage=stage)
            self.assertNotEqual(pending_delivery(job), DELIVERY_PROGRESS)

    def test_a_lost_progress_message_is_sent_again(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append(request.url.path.rsplit("/", 1)[-1])
                if request.url.path.endswith("editMessageText"):
                    return httpx.Response(400, json={"ok": False, "description": "message to edit not found"})
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})

            job = _job(stage=CHARGEN_STAGE_HUNYUAN, telegram_status_message_id=5)
            manager = _FakeManager([job])
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            service = TelegramDeliveryService(manager, client=client)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()
            self.assertEqual(sent, ["editMessageText", "sendMessage"])
            self.assertEqual(job.telegram_status_message_id, 9)

        run(scenario())


class StaleCardTests(unittest.TestCase):
    """A card whose moment has passed does not stay in the chat."""

    def test_the_variant_choice_goes_once_a_variant_is_chosen(self):
        job = _job(stage=CHARGEN_STAGE_HUNYUAN)
        self.assertIn(DELIVERY_IMAGE, telegram_delivery.stale_kinds(job))
        waiting = _job(stage=CHARGEN_STAGE_AWAITING_IMAGE)
        self.assertNotIn(DELIVERY_IMAGE, telegram_delivery.stale_kinds(waiting))

    def test_a_failure_notice_goes_when_the_stage_runs_again(self):
        job = _job(stage=CHARGEN_STAGE_HUNYUAN)
        self.assertIn(DELIVERY_FAILED, telegram_delivery.stale_kinds(job))
        failed = _job(stage=CHARGEN_STAGE_FAILED, error="boom")
        self.assertNotIn(DELIVERY_FAILED, telegram_delivery.stale_kinds(failed))

    def test_the_progress_line_is_never_stale_while_running(self):
        for stage in (CHARGEN_STAGE_HUNYUAN, CHARGEN_STAGE_AWAITING_IMAGE, CHARGEN_STAGE_FAILED):
            self.assertNotIn(DELIVERY_PROGRESS, telegram_delivery.stale_kinds(_job(stage=stage)))

    def test_only_the_stale_kind_is_deleted(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                sent.append(dict(httpx.QueryParams(request.content.decode())))
                return httpx.Response(200, json={"ok": True, "result": True})

            job = _job(stage=CHARGEN_STAGE_HUNYUAN)
            job.telegram_messages = [
                SentMessage(id=1, at=time.time(), kind=DELIVERY_PROGRESS),
                SentMessage(id=2, at=time.time(), kind=DELIVERY_IMAGE),
                SentMessage(id=3, at=time.time(), kind=DELIVERY_IMAGE),
            ]
            job.delivered = {DELIVERY_PROGRESS: telegram_delivery._progress_marker(job)}
            manager = _FakeManager([job])
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            service = TelegramDeliveryService(manager, client=client)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
            await client.aclose()

            self.assertEqual([int(p["message_id"]) for p in sent], [2, 3])
            self.assertEqual([m.id for m in job.telegram_messages], [1])

        run(scenario())


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


class ChatCleanupTests(unittest.TestCase):
    """A finished job takes its own cards back out of a private chat."""

    def _service(self, jobs, handler):
        manager = _FakeManager(jobs)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return TelegramDeliveryService(manager, client=client), manager, client

    @staticmethod
    def _recorder(sent, ok=True):
        def handler(request: httpx.Request) -> httpx.Response:
            sent.append(dict(httpx.QueryParams(request.content.decode())))
            if ok:
                return httpx.Response(200, json={"ok": True, "result": True})
            return httpx.Response(400, json={"ok": False, "description": "message can't be deleted"})

        return handler

    def _job_with_messages(self, ids, **kw):
        job = _job(**kw)
        job.telegram_messages = [SentMessage(id=i, at=time.time()) for i in ids]
        return job

    def test_discarded_job_messages_are_deleted(self):
        async def scenario():
            sent = []
            job = self._job_with_messages([11, 12, 13], stage=CHARGEN_STAGE_DISCARDED)
            service, manager, client = self._service([job], self._recorder(sent))
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
            await client.aclose()
            self.assertEqual([int(p["message_id"]) for p in sent], [11, 12, 13])
            self.assertEqual(job.telegram_messages, [])

        run(scenario())

    def test_group_chats_are_never_cleaned(self):
        async def scenario():
            sent = []
            # negative ids are groups and supergroups: a shared log we do not rewrite
            job = self._job_with_messages([11], stage=CHARGEN_STAGE_DISCARDED, telegram_chat_id=-100123)
            service, manager, client = self._service([job], self._recorder(sent))
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
            await client.aclose()
            self.assertEqual(sent, [])
            self.assertEqual(len(job.telegram_messages), 1)

        run(scenario())

    def test_actionable_stages_keep_their_cards(self):
        """ready and failed are not active either, and carry the only buttons."""

        async def scenario():
            sent = []
            jobs = [
                self._job_with_messages([1], stage=CHARGEN_STAGE_READY, video_url="https://x/v.mp4"),
                self._job_with_messages([2], stage=CHARGEN_STAGE_FAILED, error="boom"),
                self._job_with_messages([3], stage=CHARGEN_STAGE_AWAITING_IMAGE, image_url="https://x/a.png"),
                self._job_with_messages([4], stage="submitted"),
                self._job_with_messages([5], stage=CHARGEN_STAGE_HUNYUAN),
            ]
            service, manager, client = self._service(jobs, self._recorder(sent))
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
            await client.aclose()
            self.assertEqual(sent, [], "no actionable or in-flight card may be deleted")

        run(scenario())

    def test_a_message_past_the_window_is_dropped_without_asking(self):
        async def scenario():
            sent = []
            job = _job(stage=CHARGEN_STAGE_DISCARDED)
            job.telegram_messages = [
                SentMessage(id=21, at=time.time() - 60 * 3600),  # older than 48h
                SentMessage(id=22, at=time.time()),
            ]
            service, manager, client = self._service([job], self._recorder(sent))
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
            await client.aclose()
            self.assertEqual([int(p["message_id"]) for p in sent], [22])
            self.assertIn(21, job.telegram_undeletable)
            self.assertEqual(job.telegram_messages, [])

        run(scenario())

    def test_a_refused_delete_is_not_retried_forever(self):
        async def scenario():
            sent = []
            job = self._job_with_messages([31], stage=CHARGEN_STAGE_DISCARDED)
            service, manager, client = self._service([job], self._recorder(sent, ok=False))
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
                await service.cleanup_tick()
            await client.aclose()
            self.assertEqual(len(sent), 1, "a permanent refusal must be asked once")
            self.assertIn(31, job.telegram_undeletable)

        run(scenario())

    def test_cleanup_failure_cannot_stop_another_job(self):
        async def scenario():
            sent = []

            def handler(request: httpx.Request) -> httpx.Response:
                payload = dict(httpx.QueryParams(request.content.decode()))
                sent.append(payload)
                if int(payload["message_id"]) == 41:
                    raise httpx.ConnectError("boom")
                return httpx.Response(200, json={"ok": True, "result": True})

            jobs = [
                self._job_with_messages([41], stage=CHARGEN_STAGE_DISCARDED),
                self._job_with_messages([42], stage=CHARGEN_STAGE_DISCARDED),
            ]
            service, manager, client = self._service(jobs, handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.cleanup_tick()
            await client.aclose()
            self.assertIn(42, [int(p["message_id"]) for p in sent])

        run(scenario())

    def test_ids_of_a_two_variant_album_are_all_recorded(self):
        async def scenario():
            def handler(request: httpx.Request) -> httpx.Response:
                if request.url.path.endswith("sendMediaGroup"):
                    return httpx.Response(
                        200, json={"ok": True, "result": [{"message_id": 61}, {"message_id": 62}]}
                    )
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 63}})

            job = _job(
                stage=CHARGEN_STAGE_AWAITING_IMAGE,
                image_url="https://x/a.png",
                image_url_b="https://x/b.png",
            )
            service, manager, client = self._service([job], handler)
            with patch.object(config, "TELEGRAM_BOT_TOKEN", "T"):
                await service.tick()
            await client.aclose()
            # both album photos and the chooser, or the album survives cleanup
            self.assertEqual([m.id for m in job.telegram_messages], [61, 62, 63])
            self.assertEqual(job.telegram_message_id, 63)

        run(scenario())


class PrivateChatTests(unittest.TestCase):
    def test_only_positive_ids_are_private(self):
        self.assertTrue(telegram_delivery.is_private_chat(777))
        self.assertFalse(telegram_delivery.is_private_chat(-1001234))
        self.assertFalse(telegram_delivery.is_private_chat(0))

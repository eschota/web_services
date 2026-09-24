"""The ♻️ Regen button on done notifications (telegram_bot.py) and its client.

callback_data is capped at 64 bytes and the handler registration and the
parser share one pattern constant, as _APPROVE_PATTERN does (gotchas.md).
"""
import inspect
import json
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx

import render_prompting
import telegram_bot

from test_telegram_generate_button import _FakeQuery, run

TASK_ID = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"


class RegenCallbackDataTests(unittest.TestCase):
    def test_callback_data_fits_64_bytes_and_parses_back(self):
        data = telegram_bot._regen_task_callback_data(TASK_ID)
        self.assertEqual(data, f"rgx:{TASK_ID}")
        self.assertLessEqual(len(data.encode("utf-8")), 64)
        self.assertEqual(re.match(telegram_bot._REGEN_TASK_PATTERN, data).group(1), TASK_ID)

    def test_the_parser_refuses_anything_but_one_task_id(self):
        for data in (
            "rgx:../../etc",
            "rgx:",
            f"rfg:{TASK_ID}",
            f"rgx:{TASK_ID}:b",
            f"rgx:{TASK_ID}/x",
            f" rgx:{TASK_ID}",
        ):
            with self.subTest(data=data):
                self.assertIsNone(re.match(telegram_bot._REGEN_TASK_PATTERN, data))

    def test_registration_and_parser_share_the_pattern(self):
        self.assertIn(
            "CallbackQueryHandler(_handle_regen_task_callback, pattern=_REGEN_TASK_PATTERN)",
            inspect.getsource(telegram_bot.run_polling),
        )
        self.assertIn(
            "re.match(_REGEN_TASK_PATTERN, query.data)",
            inspect.getsource(telegram_bot._handle_regen_task_callback),
        )


class DoneNotificationMarkupTests(unittest.TestCase):
    def test_regen_sits_next_to_the_collection_button(self):
        async def scenario():
            bot = Mock()
            bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=1))
            bot.delete_message = AsyncMock()

            def no_session():
                raise RuntimeError("no database in this test")

            with patch.object(telegram_bot, "_get_token", return_value="T"), \
                    patch("telegram.Bot", return_value=bot), \
                    patch.object(telegram_bot, "_task_telegram_metrics", AsyncMock(return_value={})), \
                    patch.object(telegram_bot, "AsyncSessionLocal", side_effect=no_session), \
                    patch.object(telegram_bot.os.path, "exists", return_value=False), \
                    patch.object(
                        telegram_bot, "_download_video_from_worker",
                        AsyncMock(return_value=(None, 0, "missing")),
                    ), \
                    patch.object(telegram_bot, "get_broadcast_chat_ids", AsyncMock(return_value=[777])), \
                    patch.object(telegram_bot, "private_chats_awaiting_task", AsyncMock(return_value=[])), \
                    patch.object(telegram_bot, "peek_notification_message_id", AsyncMock(return_value=None)), \
                    patch.object(telegram_bot, "pop_notification_message_id", AsyncMock(return_value=None)), \
                    patch.object(telegram_bot, "reserve_notification", AsyncMock(return_value=True)), \
                    patch.object(telegram_bot, "mark_task_done_notification_sent", AsyncMock()), \
                    patch.object(telegram_bot, "_cleanup_generation_chat", AsyncMock()):
                await telegram_bot.broadcast_task_done(TASK_ID)

            markup = bot.send_message.await_args.kwargs["reply_markup"]
            row = markup.inline_keyboard[0]
            self.assertEqual(
                [(b.text, b.callback_data) for b in row],
                [
                    ("🎨 Коллекция ×15", f"rfg:{TASK_ID}"),
                    ("♻️ Regen", f"rgx:{TASK_ID}"),
                    ("📦 Сабмитить", f"rfc:{TASK_ID}"),
                ],
            )

        run(scenario())


class RegenCallbackTests(unittest.TestCase):
    def _press(self, query, *, reserved=True, bot=None):
        async def scenario():
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=bot or AsyncMock())
            spawned = []
            starter = AsyncMock()
            with patch.object(telegram_bot, "reserve_notification", AsyncMock(return_value=reserved)) as res, \
                    patch.object(telegram_bot, "_run_regen", starter), \
                    patch.object(
                        telegram_bot.asyncio, "create_task",
                        side_effect=lambda coro: (spawned.append(coro), coro.close())[0],
                    ):
                await telegram_bot._handle_regen_task_callback(update, context)
            return res, starter, spawned, context.bot

        return run(scenario())

    def test_a_press_reserves_the_task_and_starts_the_regen_in_the_dm(self):
        bot = AsyncMock()
        bot.send_message.return_value = SimpleNamespace(message_id=99)
        query = _FakeQuery(f"rgx:{TASK_ID}", chat_id=-1001, user_id=555)
        res, starter, spawned, bot = self._press(query, bot=bot)
        res.assert_awaited_once_with(-1001, "renderfin_regen", TASK_ID)
        self.assertEqual(len(spawned), 1)
        # status to the presser's DM; the reservation is released in the group
        starter.assert_called_once_with(bot, 555, TASK_ID, 99, -1001)
        self.assertEqual(bot.send_message.await_args.kwargs["chat_id"], 555)
        self.assertIn("c8691854", bot.send_message.await_args.kwargs["text"])
        self.assertIn("личку", query.answers[0])

    def test_a_press_in_the_bots_own_chat_replies_there(self):
        bot = AsyncMock()
        bot.send_message.return_value = SimpleNamespace(message_id=77)
        query = _FakeQuery(f"rgx:{TASK_ID}", chat_id=555, user_id=555, message_id=42)
        _, starter, spawned, bot = self._press(query, bot=bot)
        starter.assert_called_once_with(bot, 555, TASK_ID, 77, 555)
        self.assertEqual(bot.send_message.await_args.kwargs["reply_to_message_id"], 42)

    def test_a_second_press_is_answered_politely(self):
        query = _FakeQuery(f"rgx:{TASK_ID}")
        _, starter, spawned, bot = self._press(query, reserved=False)
        self.assertEqual(spawned, [])
        starter.assert_not_called()
        bot.send_message.assert_not_awaited()
        self.assertIn("уже запущен", query.answers[0])

    def test_bad_callback_data_is_refused(self):
        query = _FakeQuery("rgx:../../etc")
        res, starter, spawned, _ = self._press(query)
        res.assert_not_awaited()
        self.assertEqual(spawned, [])
        self.assertIn("Некорректные", query.answers[0])


class RunRegenTests(unittest.TestCase):
    def test_success_reports_the_job_in_the_status_message(self):
        async def scenario():
            bot = AsyncMock()
            release = AsyncMock()
            start = AsyncMock(return_value={"job_id": "j-1", "seq": 42, "stage": "regen_source"})
            with patch.object(render_prompting, "start_character_regen", start), \
                    patch.object(telegram_bot, "release_notification", release):
                await telegram_bot._run_regen(bot, 555, TASK_ID, 99, -1001)
            start.assert_awaited_once_with(TASK_ID, telegram_chat_id=555)
            release.assert_not_awaited()
            text = bot.edit_message_text.await_args.kwargs["text"]
            self.assertIn("Regen задачи c8691854", text)
            self.assertIn("#42", text)

        run(scenario())

    def test_failure_releases_the_reservation_where_it_was_taken(self):
        async def scenario():
            bot = AsyncMock()
            release = AsyncMock()
            start = AsyncMock(side_effect=RuntimeError("renderfin down"))
            with patch.object(render_prompting, "start_character_regen", start), \
                    patch.object(telegram_bot, "release_notification", release):
                await telegram_bot._run_regen(bot, 555, TASK_ID, 99, -1001)
            release.assert_awaited_once_with(-1001, "renderfin_regen", TASK_ID)
            self.assertIn("Regen не запустился", bot.edit_message_text.await_args.kwargs["text"])

        run(scenario())

    def test_discarding_a_regen_frees_the_button_again(self):
        async def scenario():
            query = _FakeQuery("rfd:11111111-2222-3333-4444-555566667777")
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=AsyncMock())
            release = AsyncMock()
            with patch.object(
                render_prompting, "discard_character_gen",
                AsyncMock(return_value={"source_task_id": TASK_ID, "kind": "regen"}),
            ), patch.object(telegram_bot, "release_notification", release):
                await telegram_bot._handle_delete_callback(update, context)
            released = [call.args for call in release.await_args_list]
            self.assertIn((777, "renderfin_regen", TASK_ID), released)
            self.assertIn((777, "renderfin_gen", TASK_ID), released)

        run(scenario())


class StartCharacterRegenClientTests(unittest.TestCase):
    def _call(self, handler, **kwargs):
        async def scenario():
            transport = httpx.MockTransport(handler)
            real_client = httpx.AsyncClient

            def patched_client(*args, **kw):
                kw["transport"] = transport
                return real_client(*args, **kw)

            with patch.object(render_prompting.httpx, "AsyncClient", side_effect=patched_client):
                return await render_prompting.start_character_regen(TASK_ID, **kwargs)

        return run(scenario())

    def test_posts_the_task_to_the_regen_endpoint(self):
        seen = {}

        def handler(request):
            seen["path"] = request.url.path
            seen["body"] = json.loads(request.content)
            return httpx.Response(200, json={"job_id": "j-1", "seq": 3, "stage": "regen_source"})

        payload = self._call(handler, telegram_chat_id=555)
        self.assertTrue(seen["path"].endswith("/renderfin/api-character-gen/regen"))
        self.assertEqual(
            seen["body"],
            {"task_id": TASK_ID, "view": "front", "user_name": "autorig-bot", "telegram_chat_id": 555},
        )
        self.assertEqual(payload["job_id"], "j-1")

    def test_a_refusal_is_raised(self):
        with self.assertRaises(RuntimeError) as raised:
            self._call(lambda request: httpx.Response(400, json={"detail": "task_id must be a UUID"}))
        self.assertIn("HTTP 400", str(raised.exception))


if __name__ == "__main__":
    unittest.main()

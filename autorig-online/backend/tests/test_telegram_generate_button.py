import asyncio
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import telegram_bot


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeQuery:
    def __init__(self, data, chat_id=777, message_id=42):
        self.data = data
        self.message = SimpleNamespace(
            chat=SimpleNamespace(id=chat_id), message_id=message_id
        )
        self.answers = []

    async def answer(self, text="", **kw):
        self.answers.append(text)


class CallbackDataTests(unittest.TestCase):
    def test_callback_data_fits_64_bytes(self):
        task_id = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"
        for prefix in ("rfg", "rfs", "rfd"):
            data = f"{prefix}:{task_id}"
            self.assertLessEqual(len(data.encode("utf-8")), 64)
            self.assertRegex(data, rf"^{prefix}:[0-9a-fA-F-]{{8,64}}$")

    def test_handler_patterns_match(self):
        task_id = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"
        self.assertTrue(re.match(r"^rfg:[0-9a-fA-F-]{8,64}$", f"rfg:{task_id}"))
        self.assertFalse(re.match(r"^rfg:[0-9a-fA-F-]{8,64}$", "rfg:../evil"))


class GenerateCallbackTests(unittest.TestCase):
    def test_duplicate_press_answers_politely(self):
        async def scenario():
            query = _FakeQuery("rfg:c8691854-bdd2-4503-9281-fdc8cafdb0d7")
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=AsyncMock())
            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=False)) as res:
                await telegram_bot._handle_generate_callback(update, context)
            res.assert_awaited_once_with(777, "renderfin_gen", "c8691854-bdd2-4503-9281-fdc8cafdb0d7")
            self.assertIn("уже запущена", query.answers[0])
            context.bot.send_message.assert_not_awaited()

        run(scenario())

    def test_happy_press_reserves_and_spawns(self):
        async def scenario():
            query = _FakeQuery("rfg:c8691854-bdd2-4503-9281-fdc8cafdb0d7")
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            bot.send_message.return_value = SimpleNamespace(message_id=99)
            context = SimpleNamespace(bot=bot)
            spawned = []
            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(telegram_bot.asyncio, "create_task", side_effect=lambda coro: (spawned.append(coro), coro.close())[0]):
                    await telegram_bot._handle_generate_callback(update, context)
            self.assertEqual(len(spawned), 1)
            bot.send_message.assert_awaited_once()
            kwargs = bot.send_message.await_args.kwargs
            self.assertEqual(kwargs["chat_id"], 777)
            self.assertEqual(kwargs["reply_to_message_id"], 42)

        run(scenario())

    def test_bad_callback_data_rejected(self):
        async def scenario():
            query = _FakeQuery("rfg:../../etc")
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=AsyncMock())
            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock()) as res:
                await telegram_bot._handle_generate_callback(update, context)
            res.assert_not_awaited()

        run(scenario())


class RunGenerationTests(unittest.TestCase):
    def test_failure_releases_reservation(self):
        async def scenario():
            bot = AsyncMock()
            release = AsyncMock()
            import render_prompting

            with patch.object(render_prompting, "build_render_request", new=AsyncMock(side_effect=RuntimeError("llm down"))):
                with patch.object(telegram_bot, "release_notification", new=release):
                    await telegram_bot._run_generation(bot, 777, "task-1", 42, 99)
            release.assert_awaited_once_with(777, "renderfin_gen", "task-1")
            bot.edit_message_text.assert_awaited()
            text = bot.edit_message_text.await_args.kwargs["text"]
            self.assertIn("Ошибка генерации", text)

        run(scenario())

    def test_image_phase_sends_photo_with_validation_buttons(self):
        async def scenario():
            bot = AsyncMock()
            import render_prompting
            from render_prompting import RenderGenPlan

            plan = RenderGenPlan(
                prompt="a detailed orc warrior full body T-pose front view",
                negative_prompt="blurry",
                body_type="normal",
                mask_url="https://x/render/masks/t_pose.jpg",
                source="llm",
            )
            statuses = iter([
                {"stage": "awaiting_image_approval",
                 "prompt": "a detailed orc warrior full body T-pose front view",
                 "image_url": "https://x/render/bot/f1.png",
                 "isolated_url": "https://x/render/bot/f1_Isolated.png",
                 "source_task_id": "task-1"},
            ])

            with patch.object(render_prompting, "build_render_request", new=AsyncMock(return_value=plan)):
                with patch.object(render_prompting, "start_character_gen", new=AsyncMock(return_value="j1")):
                    with patch.object(render_prompting, "poll_character_gen", new=AsyncMock(side_effect=lambda jid: next(statuses))):
                        with patch.object(telegram_bot, "_download_bytes", new=AsyncMock(return_value=b"PNGDATA")):
                            with patch.object(telegram_bot, "CHARGEN_POLL_INTERVAL_SECONDS", 0):
                                await telegram_bot._run_generation(bot, 777, "task-1", 42, 99)

            bot.send_photo.assert_awaited_once()
            kwargs = bot.send_photo.await_args.kwargs
            self.assertEqual(kwargs["reply_to_message_id"], 42)
            buttons = [b for row in kwargs["reply_markup"].inline_keyboard for b in row]
            self.assertEqual(
                [b.callback_data for b in buttons], ["rfa:j1", "rfr:j1", "rfd:j1"]
            )
            bot.send_video.assert_not_awaited()

        run(scenario())

    def test_model_phase_sends_video_with_review_buttons(self):
        async def scenario():
            bot = AsyncMock()
            import render_prompting

            statuses = iter([
                {"stage": "ready", "prompt": "orc",
                 "video_url": "https://x/render/bot/j1_turntable.mp4",
                 "glb_url": "https://x/render/bot/j1.glb"},
            ])
            with patch.object(render_prompting, "poll_character_gen", new=AsyncMock(side_effect=lambda jid: next(statuses))):
                with patch.object(telegram_bot, "_download_bytes", new=AsyncMock(return_value=b"MP4DATA")):
                    with patch.object(telegram_bot, "CHARGEN_POLL_INTERVAL_SECONDS", 0):
                        await telegram_bot._watch_model_phase(bot, 777, "j1", 55)

            bot.send_video.assert_awaited_once()
            kwargs = bot.send_video.await_args.kwargs
            self.assertEqual(kwargs["reply_to_message_id"], 55)
            buttons = [b for row in kwargs["reply_markup"].inline_keyboard for b in row]
            self.assertEqual([b.callback_data for b in buttons], ["rfs:j1", "rfd:j1"])

        run(scenario())

    def test_model_phase_failure_offers_regenerate(self):
        async def scenario():
            bot = AsyncMock()
            import render_prompting

            statuses = iter([
                {"stage": "failed", "prompt": "orc", "error": "boom"},
            ])
            with patch.object(render_prompting, "poll_character_gen", new=AsyncMock(side_effect=lambda jid: next(statuses))):
                with patch.object(telegram_bot, "CHARGEN_POLL_INTERVAL_SECONDS", 0):
                    await telegram_bot._watch_model_phase(bot, 777, "j1", 55)

            bot.edit_message_caption.assert_awaited()
            kwargs = bot.edit_message_caption.await_args.kwargs
            self.assertIn("3D не удалось", kwargs["caption"])
            buttons = [b for row in kwargs["reply_markup"].inline_keyboard for b in row]
            self.assertEqual([b.callback_data for b in buttons], ["rfr:j1", "rfd:j1"])
            bot.send_video.assert_not_awaited()

        run(scenario())


class ApproveRegenCallbackTests(unittest.TestCase):
    def test_approve_transitions_and_spawns_model_watch(self):
        async def scenario():
            query = _FakeQuery("rfa:11111111-2222-3333-4444-555566667777", message_id=55)
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            import render_prompting

            spawned = []
            with patch.object(render_prompting, "approve_character_gen_image",
                              new=AsyncMock(return_value={"transitioned": True, "stage": "hunyuan"})):
                with patch.object(telegram_bot.asyncio, "create_task",
                                  side_effect=lambda coro: (spawned.append(coro), coro.close())[0]):
                    await telegram_bot._handle_approve_callback(update, context)
            self.assertEqual(len(spawned), 1)
            bot.edit_message_caption.assert_awaited_once()
            self.assertIn("Генерируем 3D", query.answers[0])

        run(scenario())

    def test_approve_double_press_refused(self):
        async def scenario():
            query = _FakeQuery("rfa:11111111-2222-3333-4444-555566667777", message_id=55)
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=AsyncMock())
            import render_prompting

            with patch.object(render_prompting, "approve_character_gen_image",
                              new=AsyncMock(return_value={"transitioned": False, "stage": "hunyuan"})):
                await telegram_bot._handle_approve_callback(update, context)
            self.assertIn("Уже в работе", query.answers[0])
            context.bot.edit_message_caption.assert_not_awaited()

        run(scenario())

    def test_regen_transitions_and_spawns_image_watch(self):
        async def scenario():
            query = _FakeQuery("rfr:11111111-2222-3333-4444-555566667777", message_id=55)
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            import render_prompting

            spawned = []
            with patch.object(render_prompting, "regenerate_character_gen_image",
                              new=AsyncMock(return_value={"transitioned": True, "stage": "flux_render"})):
                with patch.object(telegram_bot.asyncio, "create_task",
                                  side_effect=lambda coro: (spawned.append(coro), coro.close())[0]):
                    await telegram_bot._handle_regen_callback(update, context)
            self.assertEqual(len(spawned), 1)
            self.assertIn("Перегенерируем", query.answers[0])

        run(scenario())


class SubmitCallbackTests(unittest.TestCase):
    def test_submit_creates_task_and_marks_job(self):
        async def scenario():
            query = _FakeQuery("rfs:11111111-2222-3333-4444-555566667777")
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            import render_prompting

            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(render_prompting, "poll_character_gen",
                                  new=AsyncMock(return_value={"glb_url": "https://x/render/bot/h1.glb"})):
                    with patch.object(telegram_bot, "_submit_generated_model",
                                      new=AsyncMock(return_value=("new-task-id", None))) as submit:
                        with patch.object(render_prompting, "mark_character_gen_submitted",
                                          new=AsyncMock()) as mark:
                            await telegram_bot._handle_submit_callback(update, context)
            submit.assert_awaited_once_with("https://x/render/bot/h1.glb")
            mark.assert_awaited_once_with("11111111-2222-3333-4444-555566667777")
            bot.edit_message_caption.assert_awaited_once()
            self.assertIsNone(bot.edit_message_caption.await_args.kwargs["reply_markup"])

        run(scenario())

    def test_submit_failure_releases_reservation(self):
        async def scenario():
            query = _FakeQuery("rfs:11111111-2222-3333-4444-555566667777")
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            release = AsyncMock()
            import render_prompting

            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(telegram_bot, "release_notification", new=release):
                    with patch.object(render_prompting, "poll_character_gen",
                                      new=AsyncMock(side_effect=RuntimeError("gone"))):
                        await telegram_bot._handle_submit_callback(update, context)
            release.assert_awaited_once()

        run(scenario())


class DeleteCallbackTests(unittest.TestCase):
    def test_delete_discards_and_removes_message(self):
        async def scenario():
            query = _FakeQuery("rfd:11111111-2222-3333-4444-555566667777")
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            import render_prompting

            with patch.object(render_prompting, "discard_character_gen", new=AsyncMock()) as discard:
                await telegram_bot._handle_delete_callback(update, context)
            discard.assert_awaited_once_with("11111111-2222-3333-4444-555566667777")
            bot.delete_message.assert_awaited_once_with(chat_id=777, message_id=42)

        run(scenario())


if __name__ == "__main__":
    unittest.main()

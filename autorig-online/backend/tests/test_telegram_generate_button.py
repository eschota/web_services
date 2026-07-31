import asyncio
import re
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import telegram_bot


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


class _FakeQuery:
    def __init__(self, data, chat_id=777, message_id=42, user_id=555):
        self.data = data
        self.message = SimpleNamespace(
            chat=SimpleNamespace(id=chat_id), message_id=message_id
        )
        self.from_user = SimpleNamespace(id=user_id) if user_id else None
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
            # the status message goes to the presser's DM, not the group
            self.assertEqual(kwargs["chat_id"], 555)
            self.assertIn("личку", query.answers[0])

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

    def test_generation_creates_job_and_registers_chat(self):
        """The bot only has to create the job: renderfin owns delivery."""

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
            ctx = {}

            async def fake_ctx(job_id, **kwargs):
                ctx.update({"job_id": job_id, **kwargs})

            with patch.object(render_prompting, "build_render_request", new=AsyncMock(return_value=plan)):
                with patch.object(render_prompting, "start_character_gen", new=AsyncMock(return_value="j1")) as start:
                    with patch.object(render_prompting, "set_character_gen_telegram_context", new=fake_ctx):
                        await telegram_bot._run_generation(bot, 777, "task-1", 42, 99)

            self.assertEqual(start.await_args.kwargs["telegram_chat_id"], 777)
            self.assertEqual(start.await_args.kwargs["source_task_id"], "task-1")
            self.assertEqual(ctx, {"job_id": "j1", "chat_id": 777, "status_message_id": 99})
            # no result is sent from the bot process
            bot.send_photo.assert_not_awaited()
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

            with patch.object(render_prompting, "approve_character_gen_image",
                              new=AsyncMock(return_value={"transitioned": True, "stage": "hunyuan"})) as approve:
                await telegram_bot._handle_approve_callback(update, context)
            approve.assert_awaited_once_with("11111111-2222-3333-4444-555566667777")
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

            with patch.object(render_prompting, "regenerate_character_gen_image",
                              new=AsyncMock(return_value={"transitioned": True, "stage": "flux_render"})) as regen:
                await telegram_bot._handle_regen_callback(update, context)
            regen.assert_awaited_once_with("11111111-2222-3333-4444-555566667777")
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
            self.assertIn("полный пайплайн", bot.edit_message_caption.await_args.kwargs["caption"])
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


class RestartReportTests(unittest.TestCase):
    def test_restart_reports_in_flight_jobs_without_sending(self):
        """Delivery belongs to renderfin; the bot must not re-send anything."""

        async def scenario():
            import render_prompting

            jobs = [
                {"job_id": "j-flux", "stage": "flux_render", "telegram_chat_id": 777},
                {"job_id": "j-3d", "stage": "hunyuan", "telegram_chat_id": 777},
            ]
            bot = AsyncMock()
            with patch.object(render_prompting, "list_active_character_gen_jobs",
                              new=AsyncMock(return_value=jobs)):
                await telegram_bot._reattach_chargen_watchers(bot)
            bot.send_message.assert_not_awaited()
            bot.send_photo.assert_not_awaited()

        run(scenario())

    def test_restart_survives_api_failure(self):
        async def scenario():
            import render_prompting

            with patch.object(render_prompting, "list_active_character_gen_jobs",
                              new=AsyncMock(side_effect=RuntimeError("renderfin down"))):
                with patch.object(telegram_bot.asyncio, "sleep", new=AsyncMock()):
                    await telegram_bot._reattach_chargen_watchers(AsyncMock())

        run(scenario())


class ResumeCallbackTests(unittest.TestCase):
    def test_resume_retries_3d_stage(self):
        async def scenario():
            query = _FakeQuery("rfe:11111111-2222-3333-4444-555566667777", message_id=55)
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=AsyncMock())
            import render_prompting

            with patch.object(render_prompting, "resume_character_gen",
                              new=AsyncMock(return_value={"transitioned": True, "stage": "hunyuan"})) as resume:
                await telegram_bot._handle_resume_callback(update, context)
            resume.assert_awaited_once_with("11111111-2222-3333-4444-555566667777")
            self.assertIn("Продолжаем", query.answers[0])

        run(scenario())

    def test_resume_refused_when_not_failed(self):
        async def scenario():
            query = _FakeQuery("rfe:11111111-2222-3333-4444-555566667777", message_id=55)
            update = SimpleNamespace(callback_query=query)
            context = SimpleNamespace(bot=AsyncMock())
            import render_prompting

            with patch.object(render_prompting, "resume_character_gen",
                              new=AsyncMock(return_value={"transitioned": False, "stage": "ready"})):
                await telegram_bot._handle_resume_callback(update, context)
            self.assertIn("нельзя", query.answers[0])

        run(scenario())


class SubmitPipelineKindTests(unittest.TestCase):
    def test_submit_uses_convert_pipeline_for_retopology(self):
        """pipeline_kind must be 'convert': 'rig' is only_rig and skips retopology."""

        async def scenario():
            captured = {}

            class _FakeTask:
                id = "new-task-id"

            async def fake_create(db, **kwargs):
                captured.update(kwargs)
                return _FakeTask(), None

            class _FakeSession:
                async def __aenter__(self):
                    return object()

                async def __aexit__(self, *a):
                    return False

            import tasks as tasks_module

            with patch.object(telegram_bot, "AsyncSessionLocal", _FakeSession):
                with patch.object(tasks_module, "create_conversion_task", new=fake_create):
                    task_id, error = await telegram_bot._submit_generated_model(
                        "https://x/render/bot/model.glb"
                    )
            self.assertEqual(task_id, "new-task-id")
            self.assertIsNone(error)
            self.assertEqual(captured["pipeline_kind"], "convert")
            self.assertEqual(captured["task_type"], "t_pose")
            self.assertTrue(captured["input_url"].endswith(".glb"))

        run(scenario())


class DeleteCallbackTests(unittest.TestCase):
    def test_delete_discards_and_removes_message(self):
        async def scenario():
            query = _FakeQuery("rfd:11111111-2222-3333-4444-555566667777")
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            import render_prompting

            release = AsyncMock()
            with patch.object(render_prompting, "discard_character_gen",
                              new=AsyncMock(return_value={"source_task_id": "task-1"})) as discard:
                with patch.object(telegram_bot, "release_notification", new=release):
                    await telegram_bot._handle_delete_callback(update, context)
            discard.assert_awaited_once_with("11111111-2222-3333-4444-555566667777")
            bot.delete_message.assert_awaited_once_with(chat_id=777, message_id=42)
            # the 🎨 button must work again after a cancel
            released = [c.args for c in release.await_args_list]
            self.assertIn((777, "renderfin_gen", "task-1"), released)

        run(scenario())


if __name__ == "__main__":
    unittest.main()


class SubmitReplyThreadingTests(unittest.TestCase):
    def test_submit_stores_reply_target(self):
        """The completion notice must land under the message the user acted on."""

        async def scenario():
            query = _FakeQuery("rfs:11111111-2222-3333-4444-555566667777", message_id=4242)
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            context = SimpleNamespace(bot=bot)
            import render_prompting

            remembered = {}

            async def fake_remember(chat_id, task_id, message_id):
                remembered.update(
                    {"chat_id": chat_id, "task_id": task_id, "message_id": message_id}
                )

            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(render_prompting, "poll_character_gen",
                                  new=AsyncMock(return_value={"glb_url": "https://x/m.glb"})):
                    with patch.object(telegram_bot, "_submit_generated_model",
                                      new=AsyncMock(return_value=("new-task", None))):
                        with patch.object(render_prompting, "mark_character_gen_submitted", new=AsyncMock()):
                            with patch.object(telegram_bot, "remember_task_reply_target", new=fake_remember):
                                await telegram_bot._handle_submit_callback(update, context)

            self.assertEqual(
                remembered, {"chat_id": 777, "task_id": "new-task", "message_id": 4242}
            )

        run(scenario())


class DmRoutingTests(unittest.TestCase):
    def test_results_go_to_the_pressers_dm(self):
        async def scenario():
            query = _FakeQuery("rfg:c8691854-bdd2-4503-9281-fdc8cafdb0d7", chat_id=-100777, user_id=555)
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            bot.send_message.return_value = SimpleNamespace(message_id=9001)
            context = SimpleNamespace(bot=bot)
            spawned = []
            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(telegram_bot.asyncio, "create_task",
                                  side_effect=lambda coro: (spawned.append(coro), coro.close())[0]):
                    await telegram_bot._handle_generate_callback(update, context)
            self.assertEqual(bot.send_message.await_args.kwargs["chat_id"], 555)
            self.assertEqual(len(spawned), 1)

        run(scenario())

    def test_falls_back_to_origin_chat_when_dm_is_closed(self):
        async def scenario():
            query = _FakeQuery("rfg:c8691854-bdd2-4503-9281-fdc8cafdb0d7", chat_id=-100777, user_id=555)
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            calls = {"n": 0}

            async def send(**kwargs):
                calls["n"] += 1
                if kwargs["chat_id"] == 555:
                    raise RuntimeError("Forbidden: bot can't initiate conversation with a user")
                return SimpleNamespace(message_id=9002)

            bot.send_message = AsyncMock(side_effect=send)
            context = SimpleNamespace(bot=bot)
            spawned = []
            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(telegram_bot.asyncio, "create_task",
                                  side_effect=lambda coro: (spawned.append(coro), coro.close())[0]):
                    await telegram_bot._handle_generate_callback(update, context)
            # tried the DM, then posted in the originating chat
            self.assertEqual(calls["n"], 2)
            self.assertEqual(bot.send_message.await_args.kwargs["chat_id"], -100777)
            self.assertEqual(len(spawned), 1)

        run(scenario())

    def test_dm_press_stays_in_the_dm(self):
        async def scenario():
            # pressing inside the DM itself: chat id == user id, no second send
            query = _FakeQuery("rfg:c8691854-bdd2-4503-9281-fdc8cafdb0d7", chat_id=555, user_id=555)
            update = SimpleNamespace(callback_query=query)
            bot = AsyncMock()
            bot.send_message.return_value = SimpleNamespace(message_id=9003)
            context = SimpleNamespace(bot=bot)
            with patch.object(telegram_bot, "reserve_notification", new=AsyncMock(return_value=True)):
                with patch.object(telegram_bot.asyncio, "create_task",
                                  side_effect=lambda coro: coro.close()):
                    await telegram_bot._handle_generate_callback(update, context)
            bot.send_message.assert_awaited_once()
            self.assertEqual(bot.send_message.await_args.kwargs["chat_id"], 555)
            self.assertEqual(bot.send_message.await_args.kwargs["reply_to_message_id"], 42)

        run(scenario())


class NotificationReservationTests(unittest.TestCase):
    def test_concurrent_reservations_are_serialized(self):
        """sqlite runs on a StaticPool: all sessions share one transaction, so a
        sibling's rollback used to discard another chat's pending INSERT and the
        hourly guard silently vanished for that chat."""

        async def scenario():
            active = {"now": 0, "max": 0}

            async def fake_locked(chat_id, event_type, event_key):
                active["now"] += 1
                active["max"] = max(active["max"], active["now"])
                await asyncio.sleep(0.01)
                active["now"] -= 1
                return True

            with patch.object(telegram_bot, "_reserve_notification_locked", new=fake_locked):
                await asyncio.gather(*[
                    telegram_bot.reserve_notification(cid, "disk_pressure", "pressure_x")
                    for cid in range(6)
                ])
            self.assertEqual(active["max"], 1)

        run(scenario())

from __future__ import annotations

import json
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import animation_fitting_telegram_approval as review
import animation_fitting_telegram_bot as review_bot


VIDEO_SHA = "a" * 64
MACHINE_QA_SHA = "c" * 64
OWNER = 424242


class AnimationFittingTelegramApprovalTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ledger = Path(self.temp_dir.name) / "review.jsonl"

    def tearDown(self):
        self.temp_dir.cleanup()

    def binding(self):
        return review.make_candidate_binding(
            "horse/idle-fidget/f15/97f@30:immutable-candidate-v1",
            VIDEO_SHA,
            media_kind="fitted_preview",
            machine_qa_sha256=MACHINE_QA_SHA,
        )

    def register(self, *, message_id=77):
        binding = self.binding()
        event = review.register_sent_candidate(
            self.ledger,
            binding,
            chat_id=OWNER,
            message_id=message_id,
            sent_at="2026-07-17T00:00:00.000Z",
        )
        return binding, event

    def decide(self, binding, *, action=review.APPROVE, query="q1", message_id=77, **overrides):
        params = dict(
            callback_data_value=review.callback_data(binding, action),
            callback_query_id=query,
            chat_id=OWNER,
            user_id=OWNER,
            message_id=message_id,
            chat_type="private",
            owner_chat_id=OWNER,
            owner_user_id=OWNER,
            decided_at="2026-07-17T00:01:00.000Z",
        )
        params.update(overrides)
        return review.record_decision(self.ledger, **params)

    def lines(self):
        return [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]

    def test_keyboard_has_exact_russian_actions_and_short_stable_hash_binding(self):
        first = self.binding()
        same = self.binding()
        other = review.make_candidate_binding(
            first.candidate_id,
            "b" * 64,
            media_kind="fitted_preview",
            machine_qa_sha256=MACHINE_QA_SHA,
        )
        self.assertEqual(first, same)
        self.assertNotEqual(first.callback_key, other.callback_key)
        keyboard = review.inline_keyboard_payload(first)["inline_keyboard"][0]
        self.assertEqual([button["text"] for button in keyboard], ["✅ Утвердить", "❌ Отклонить"])
        self.assertEqual(review.parse_callback_data(keyboard[0]["callback_data"]), (review.APPROVE, first.callback_key))
        self.assertEqual(review.parse_callback_data(keyboard[1]["callback_data"]), (review.REJECT, first.callback_key))
        self.assertTrue(all(len(button["callback_data"].encode("utf-8")) <= 64 for button in keyboard))

    def test_sent_binding_is_append_only_and_idempotent(self):
        binding, first = self.register()
        second = review.register_sent_candidate(
            self.ledger, binding, chat_id=OWNER, message_id=77, sent_at="later"
        )
        self.assertEqual(first, second)
        self.assertEqual(len(self.lines()), 1)
        self.assertEqual(first["candidateId"], binding.candidate_id)
        self.assertEqual(first["videoSha256"], VIDEO_SHA)
        self.assertIs(first["automaticLibraryApproval"], False)

    def test_approve_records_owner_message_time_hash_without_library_admission(self):
        binding, _ = self.register()
        result = self.decide(binding)
        self.assertTrue(result.changed)
        self.assertFalse(result.duplicate)
        record = result.record
        self.assertEqual(record["decision"], "approve")
        self.assertEqual(record["chatId"], OWNER)
        self.assertEqual(record["userId"], OWNER)
        self.assertEqual(record["messageId"], 77)
        self.assertEqual(record["decidedAt"], "2026-07-17T00:01:00.000Z")
        self.assertEqual(record["candidateId"], binding.candidate_id)
        self.assertEqual(record["videoSha256"], VIDEO_SHA)
        self.assertEqual(record["machineQaSha256"], MACHINE_QA_SHA)
        self.assertEqual(record["reviewStage"], "fitted_visual_qa")
        self.assertEqual(record["decisionMeaning"], "human_visual_pass")
        self.assertEqual(record["nextPipelineAction"], "run_release_qa_and_export")
        self.assertIs(record["automaticLibraryApproval"], False)
        self.assertIs(record["catalogAdmission"], False)
        self.assertIs(record["requiresFittingAndReleaseQa"], True)
        self.assertIsNone(record["supersedesDecisionId"])

    def test_duplicate_callback_and_same_decision_do_not_append(self):
        binding, _ = self.register()
        first = self.decide(binding, query="q1")
        exact_replay = self.decide(binding, query="q1")
        same_choice_new_query = self.decide(binding, query="q2")
        self.assertTrue(exact_replay.duplicate)
        self.assertTrue(same_choice_new_query.duplicate)
        self.assertEqual(first.record, exact_replay.record)
        self.assertEqual(first.record, same_choice_new_query.record)
        self.assertEqual(len(self.lines()), 2)

    def test_changed_decision_appends_and_supersedes_prior(self):
        binding, _ = self.register()
        approved = self.decide(binding, query="q1")
        rejected = self.decide(binding, action=review.REJECT, query="q2")
        self.assertTrue(rejected.changed)
        self.assertEqual(rejected.record["decision"], "reject")
        self.assertEqual(rejected.record["nextPipelineAction"], "stop_candidate")
        self.assertIs(rejected.record["requiresFittingAndReleaseQa"], False)
        self.assertEqual(rejected.record["supersedesDecisionId"], approved.record["decisionId"])
        self.assertEqual(len(self.lines()), 3)

    def test_non_owner_or_non_private_decision_fails_closed_without_append(self):
        cases = [
            {"chat_type": "group"},
            {"chat_id": OWNER + 1},
            {"user_id": OWNER + 1},
            {"owner_chat_id": OWNER + 1},
            {"owner_user_id": OWNER + 1},
        ]
        for index, overrides in enumerate(cases):
            with self.subTest(overrides=overrides):
                ledger = Path(self.temp_dir.name) / f"unauthorized-{index}.jsonl"
                self.ledger = ledger
                binding, _ = self.register()
                with self.assertRaises(review.UnauthorizedReviewer):
                    self.decide(binding, **overrides)
                self.assertEqual(len(self.lines()), 1)

    def test_callback_must_match_registered_telegram_message(self):
        binding, _ = self.register()
        with self.assertRaises(review.UnauthorizedReviewer):
            self.decide(binding, message_id=999)
        self.assertEqual(len(self.lines()), 1)

    def test_unknown_candidate_and_callback_id_replay_mismatch_fail_closed(self):
        binding, _ = self.register()
        unknown = review.make_candidate_binding("other", "b" * 64)
        with self.assertRaises(review.UnknownCandidate):
            self.decide(unknown)
        self.decide(binding, query="same-id")
        with self.assertRaisesRegex(review.ReviewError, "replay"):
            self.decide(binding, action=review.REJECT, query="same-id")
        self.assertEqual(len(self.lines()), 2)

    def test_caption_replaces_previous_marker_and_obeys_telegram_limit(self):
        record = {
            "decision": "approve",
            "reviewStage": "fitted_visual_qa",
            "decidedAt": "2026-07-17T00:01:00.000Z",
        }
        approved = review.decision_caption("x" * 1100, record)
        self.assertLessEqual(len(approved), 1024)
        self.assertTrue(approved.endswith("✅ Ручной visual QA: PASS · 2026-07-17T00:01:00.000Z"))
        rejected = review.decision_caption(
            approved,
            {
                "decision": "reject",
                "reviewStage": "fitted_visual_qa",
                "decidedAt": "2026-07-17T00:02:00.000Z",
            },
        )
        self.assertNotIn("✅ Ручной visual QA", rejected)
        self.assertTrue(rejected.endswith("❌ Ручной visual QA: FAIL · 2026-07-17T00:02:00.000Z"))

    def test_raw_approval_only_selects_fitting_reference(self):
        raw = review.make_candidate_binding("horse/death-d02", "d" * 64)
        review.register_sent_candidate(self.ledger, raw, chat_id=OWNER, message_id=77)
        result = self.decide(raw)
        self.assertEqual(result.record["reviewStage"], "reference_selection")
        self.assertEqual(result.record["decisionMeaning"], "approved_as_fitting_reference")
        self.assertEqual(result.record["nextPipelineAction"], "fit_candidate")
        self.assertIsNone(result.record["machineQaSha256"])
        self.assertIs(result.record["catalogAdmission"], False)

    def test_fitted_preview_requires_machine_qa_sha(self):
        with self.assertRaises(ValueError):
            review.make_candidate_binding("horse/idle", VIDEO_SHA, media_kind="fitted_preview")
        with self.assertRaises(ValueError):
            review.make_candidate_binding(
                "horse/raw",
                VIDEO_SHA,
                media_kind="raw_reference",
                machine_qa_sha256=MACHINE_QA_SHA,
            )

    def test_dedicated_bot_polling_registers_callback_handler_and_canonical_sender(self):
        source = (BACKEND / "animation_fitting_telegram_bot.py").read_text(encoding="utf-8")
        self.assertIn("CallbackQueryHandler", source)
        self.assertIn('pattern=r"^af1:[ar]:[0-9a-f]{24}$"', source)
        self.assertIn("send_review_video", source)
        self.assertIn("attach_review_buttons", source)
        self.assertIn("ANIMATION_FITTING_TELEGRAM_BOT_TOKEN", source)
        self.assertNotIn('os.getenv("TELEGRAM_BOT_TOKEN"', source)
        self.assertIn("reply_markup=markup", source)

    def test_user_visible_sources_contain_real_utf8_not_common_mojibake(self):
        sources = "\n".join(
            (BACKEND / name).read_text(encoding="utf-8")
            for name in (
                "animation_fitting_telegram_approval.py",
                "animation_fitting_telegram_bot.py",
            )
        )
        for marker in ("\u0420\u0408", "\u0421\u201a", "\u0432\u045a", "\u0432\u045c"):
            self.assertNotIn(marker, sources)
        for label in (
            "✅ Утвердить",
            "❌ Отклонить",
            "✅ Утверждено как референс для фиттинга",
            "❌ Отклонено как референс для фиттинга",
            "✅ Ручной visual QA: PASS",
            "❌ Ручной visual QA: FAIL",
        ):
            self.assertIn(label, sources)

    def test_main_redacts_top_level_exception_message_and_traceback(self):
        secret = "1234567890:synthetic-token-must-not-reach-journal"

        async def fail_without_logging(_args):
            raise RuntimeError(secret)

        parser = SimpleNamespace(parse_args=lambda: SimpleNamespace(command="poll"))
        stderr = io.StringIO()
        with (
            patch.object(review_bot, "_parser", return_value=parser),
            patch.object(review_bot, "_run_cli", new=fail_without_logging),
            patch.object(sys, "stderr", stderr),
            self.assertRaises(SystemExit) as raised,
        ):
            review_bot.main()

        self.assertEqual(raised.exception.code, 1)
        output = stderr.getvalue()
        self.assertEqual(output, "[AnimationFittingTelegram] Fatal: RuntimeError\n")
        self.assertNotIn(secret, output)
        self.assertNotIn("Traceback", output)


class _FakeButton:
    def __init__(self, text, callback_data):
        self.text = text
        self.callback_data = callback_data


class _FakeMarkup:
    def __init__(self, inline_keyboard):
        self.inline_keyboard = inline_keyboard


class _FakeMessage:
    def __init__(self, message_id=88, *, caption="candidate", chat_id=OWNER):
        self.message_id = message_id
        self.caption = caption
        self.chat = SimpleNamespace(id=chat_id, type="private")
        self.reply_markup = None
        self.replies = []
        self.delete_calls = 0
        self.fail_delete = False

    async def reply_text(self, text):
        self.replies.append(text)

    async def delete(self):
        self.delete_calls += 1
        if self.fail_delete:
            raise RuntimeError("simulated Telegram delete failure")
        return True


class _FakeBot:
    instances = []
    updates = []
    fail_edit = False

    def __init__(self, token):
        self.token = token
        self.sent = []
        self.edited = []
        type(self).instances.append(self)

    async def send_video(self, **kwargs):
        self.sent.append(kwargs)
        return _FakeMessage()

    async def edit_message_reply_markup(self, **kwargs):
        if type(self).fail_edit:
            raise RuntimeError("simulated Telegram edit failure with secret-looking text")
        self.edited.append(("markup", kwargs))
        return _FakeMessage(message_id=kwargs["message_id"])

    async def edit_message_caption(self, **kwargs):
        if type(self).fail_edit:
            raise RuntimeError("simulated Telegram edit failure with secret-looking text")
        self.edited.append(("caption", kwargs))
        return _FakeMessage(message_id=kwargs["message_id"])

    async def get_updates(self, **kwargs):
        return list(type(self).updates)


class _FakeQuery:
    def __init__(
        self,
        *,
        data,
        message,
        user_id=OWNER,
        query_id="callback-1",
        fail_answer=False,
    ):
        self.data = data
        self.message = message
        self.from_user = SimpleNamespace(id=user_id)
        self.id = query_id
        self.answers = []
        self.caption_edits = []
        self.fail_answer = fail_answer

    async def answer(self, text, show_alert=False):
        self.answers.append((text, show_alert))
        if self.fail_answer:
            raise RuntimeError("simulated Telegram callback answer failure")

    async def edit_message_caption(self, **kwargs):
        self.caption_edits.append(kwargs)


class AnimationFittingTelegramBotAsyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ledger = Path(self.temp_dir.name) / "review.jsonl"
        self.video = Path(self.temp_dir.name) / "candidate.mp4"
        self.video.write_bytes(b"immutable-video-content")
        _FakeBot.instances.clear()
        _FakeBot.updates = []
        _FakeBot.fail_edit = False
        self.telegram_patch = patch.dict(
            sys.modules,
            {"telegram": SimpleNamespace(
                Bot=_FakeBot,
                InlineKeyboardButton=_FakeButton,
                InlineKeyboardMarkup=_FakeMarkup,
            )},
        )
        self.telegram_patch.start()
        self.env_patch = patch.dict(os.environ, {
            "ANIMATION_FITTING_TELEGRAM_BOT_TOKEN": "test-only-token",
            "ANIMATION_FITTING_TELEGRAM_OWNER_CHAT_ID": str(OWNER),
            "ANIMATION_FITTING_TELEGRAM_OWNER_USER_ID": str(OWNER),
            "ANIMATION_FITTING_TELEGRAM_REVIEW_LEDGER": str(self.ledger),
        }, clear=False)
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()
        self.telegram_patch.stop()
        self.temp_dir.cleanup()

    async def test_sender_attaches_exact_keyboard_and_registers_full_video_hash(self):
        message, binding = await review_bot.send_review_video(
            video_path=self.video,
            candidate_id="horse/death-d02",
            caption="Horse Death D02",
            expected_video_sha256=review.sha256_file(self.video),
            ledger_path=self.ledger,
        )
        self.assertEqual(message.message_id, 88)
        bot = _FakeBot.instances[-1]
        self.assertEqual(bot.token, "test-only-token")
        markup = bot.sent[0]["reply_markup"]
        self.assertEqual([button.text for button in markup.inline_keyboard[0]], ["✅ Утвердить", "❌ Отклонить"])
        event = json.loads(self.ledger.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(event["videoSha256"], binding.video_sha256)
        self.assertEqual(event["reviewStage"], "reference_selection")

    async def test_attach_registers_existing_message_without_resending_video(self):
        digest = review.sha256_file(self.video)
        result, binding = await review_bot.attach_review_buttons(
            video_path=self.video,
            expected_video_sha256=digest,
            candidate_id="horse/jump-01-r02",
            message_id=42,
            ledger_path=self.ledger,
        )
        self.assertEqual(result.message_id, 42)
        bot = _FakeBot.instances[-1]
        self.assertEqual(bot.sent, [])
        self.assertEqual(bot.edited[0][0], "markup")
        event = json.loads(self.ledger.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(event["messageId"], 42)
        self.assertEqual(event["callbackKey"], binding.callback_key)

    async def test_attach_failure_is_append_audited_without_exception_text(self):
        _FakeBot.fail_edit = True
        with self.assertRaises(RuntimeError):
            await review_bot.attach_review_buttons(
                video_path=self.video,
                expected_video_sha256=review.sha256_file(self.video),
                candidate_id="horse/death-d02",
                message_id=43,
                ledger_path=self.ledger,
            )
        events = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([event["event"] for event in events], ["candidate_sent", "candidate_markup_failed"])
        self.assertEqual(events[-1]["errorType"], "RuntimeError")
        self.assertNotIn("secret-looking", json.dumps(events))

    async def test_callback_handler_records_decision_answers_and_deletes_queue_message(self):
        binding = review.make_candidate_binding("horse/death-d02", review.sha256_file(self.video))
        review.register_sent_candidate(self.ledger, binding, chat_id=OWNER, message_id=45)
        message = _FakeMessage(message_id=45, caption="Horse Death D02")
        message.reply_markup = _FakeMarkup([])
        query = _FakeQuery(data=review.callback_data(binding, review.APPROVE), message=message)
        await review_bot.handle_review_callback(SimpleNamespace(callback_query=query), None)
        self.assertEqual(query.answers, [("Референс утверждён", False)])
        self.assertEqual(message.delete_calls, 1)
        self.assertEqual(query.caption_edits, [])
        self.assertEqual(message.replies, [])
        events = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events[-1]["decisionMeaning"], "approved_as_fitting_reference")

    async def test_callback_reject_also_deletes_queue_message(self):
        binding = review.make_candidate_binding(
            "horse/fitted/walk-01",
            review.sha256_file(self.video),
            media_kind="fitted_preview",
            machine_qa_sha256=MACHINE_QA_SHA,
        )
        review.register_sent_candidate(self.ledger, binding, chat_id=OWNER, message_id=46)
        message = _FakeMessage(message_id=46, caption="Horse Walk fitted")
        query = _FakeQuery(
            data=review.callback_data(binding, review.REJECT),
            message=message,
            query_id="callback-reject",
        )
        await review_bot.handle_review_callback(SimpleNamespace(callback_query=query), None)
        self.assertEqual(query.answers, [("Visual QA: FAIL", False)])
        self.assertEqual(message.delete_calls, 1)
        self.assertEqual(query.caption_edits, [])
        self.assertEqual(message.replies, [])
        events = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events[-1]["decisionMeaning"], "human_visual_fail")

    async def test_callback_answer_failure_still_deletes_after_persisting_decision(self):
        binding = review.make_candidate_binding("horse/idle-01", review.sha256_file(self.video))
        review.register_sent_candidate(self.ledger, binding, chat_id=OWNER, message_id=47)
        message = _FakeMessage(message_id=47)
        query = _FakeQuery(
            data=review.callback_data(binding, review.APPROVE),
            message=message,
            query_id="callback-answer-fails",
            fail_answer=True,
        )
        await review_bot.handle_review_callback(SimpleNamespace(callback_query=query), None)
        self.assertEqual(message.delete_calls, 1)
        self.assertEqual(message.replies, [])
        events = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events[-1]["event"], "decision")
        self.assertEqual(events[-1]["decisionMeaning"], "approved_as_fitting_reference")

    async def test_permanent_delete_failure_retries_three_times_without_losing_decision_or_spam(self):
        binding = review.make_candidate_binding("horse/trot-01", review.sha256_file(self.video))
        review.register_sent_candidate(self.ledger, binding, chat_id=OWNER, message_id=48)
        message = _FakeMessage(message_id=48)
        message.fail_delete = True
        query = _FakeQuery(
            data=review.callback_data(binding, review.REJECT),
            message=message,
            query_id="callback-delete-fails",
        )
        with patch.object(review_bot, "DELETE_RETRY_DELAYS_SECONDS", (0.0, 0.0, 0.0)):
            await review_bot.handle_review_callback(SimpleNamespace(callback_query=query), None)
        self.assertEqual(message.delete_calls, 3)
        self.assertEqual(message.replies, [])
        self.assertEqual(query.caption_edits, [])
        events = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(events[-1]["event"], "decision")
        self.assertEqual(events[-1]["decisionMeaning"], "rejected_as_fitting_reference")

    async def test_unauthorized_and_unknown_callbacks_never_delete_message(self):
        binding = review.make_candidate_binding("horse/jump-01", review.sha256_file(self.video))
        review.register_sent_candidate(self.ledger, binding, chat_id=OWNER, message_id=49)

        unauthorized_message = _FakeMessage(message_id=49)
        unauthorized = _FakeQuery(
            data=review.callback_data(binding, review.APPROVE),
            message=unauthorized_message,
            user_id=OWNER + 1,
            query_id="callback-unauthorized",
        )
        await review_bot.handle_review_callback(SimpleNamespace(callback_query=unauthorized), None)
        self.assertEqual(unauthorized_message.delete_calls, 0)

        unknown_binding = review.make_candidate_binding(
            "horse/not-registered", review.sha256_file(self.video)
        )
        unknown_message = _FakeMessage(message_id=50)
        unknown = _FakeQuery(
            data=review.callback_data(unknown_binding, review.REJECT),
            message=unknown_message,
            query_id="callback-unknown",
        )
        await review_bot.handle_review_callback(SimpleNamespace(callback_query=unknown), None)
        self.assertEqual(unknown_message.delete_calls, 0)

        events = [json.loads(line) for line in self.ledger.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([event["event"] for event in events], ["candidate_sent"])

    async def test_discover_owner_requires_exactly_one_private_identity(self):
        _FakeBot.updates = [SimpleNamespace(
            effective_chat=SimpleNamespace(id=OWNER, type="private"),
            effective_user=SimpleNamespace(id=OWNER, username="owner"),
            effective_message=SimpleNamespace(text="/start"),
        )]
        self.assertEqual(
            await review_bot.discover_owner(),
            {"chatId": OWNER, "userId": OWNER, "username": "owner"},
        )
        _FakeBot.updates.append(SimpleNamespace(
            effective_chat=SimpleNamespace(id=OWNER + 1, type="private"),
            effective_user=SimpleNamespace(id=OWNER + 1, username="other"),
            effective_message=SimpleNamespace(text="/start"),
        ))
        with self.assertRaisesRegex(review.ReviewError, "exactly one"):
            await review_bot.discover_owner()


if __name__ == "__main__":
    unittest.main()

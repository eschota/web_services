"""Defect buttons on every task notice (owner 2026-10-14): the four buttons, the press -> MT defect report, the notice
edit, the fallback report file and the top-5 similar lines. No Telegram, no network, no customer writes."""
import asyncio
import json
import os
import re
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import telegram_bot as bot
import telegram_defects as D
import telegram_task_completion as delivery

TASK = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"
RUN = "0123456789abcdef0123"
ROWS = [{"task": "11111111-2222-3333-4444-5555555555%02d" % i, "run": f"{i:020d}", "score": 0.99 - i / 100,
         "matched": "both" if i == 0 else "img", "img": 0.95, "pose": 0.5, "what": "horse"} for i in range(5)]


class Buttons(unittest.TestCase):
    def test_four_buttons_replace_collection_and_submit(self):
        rm = D.markup(TASK)
        rows = rm.inline_keyboard
        self.assertEqual([r[0].text for r in rows], ["Неправильный трансформ модели", "Разрыв меша после скининга",
                                                     "Слишком широкие локти", "Непонятная ошибка"])
        for (kind, _label), row in zip(D.KINDS, rows):
            data = row[0].callback_data
            self.assertEqual(data, f"rd:{kind}:{TASK}")
            self.assertLessEqual(len(data.encode("utf-8")), 64)
            self.assertRegex(data, D.CALLBACK_PATTERN)
        self.assertRegex(f"rd:tear:{TASK[:8]}", D.CALLBACK_PATTERN)          # the compact form is accepted
        self.assertNotRegex("rd:tear:../../x", D.CALLBACK_PATTERN)
        self.assertNotRegex(f"rd:other:{TASK}", D.CALLBACK_PATTERN)
        self.assertTrue(D.markup(TASK, {"tear"}).inline_keyboard[1][0].text.startswith("🔎"))

    def test_run_id_of_task(self):
        t = SimpleNamespace(viewer_settings=json.dumps({"v3": {"session": {"mt_run_id": RUN}}}))
        self.assertEqual(D.run_id_of_task(t), RUN)
        t = SimpleNamespace(viewer_settings=None, viewer_prepared_glb_url=f"https://autorig.online/api/mt/files/{RUN}/rig/x.glb")
        self.assertEqual(D.run_id_of_task(t), RUN)
        self.assertIsNone(D.run_id_of_task(SimpleNamespace()))

    def test_similar_lines_fit_the_caption(self):
        line = D.similar_line(ROWS[0])
        self.assertIn("🔗 похожие: 0.99 both", line)
        self.assertIn(f'href="https://autorig.online/task?id={ROWS[0]["task"]}"', line)
        lines = "\n".join(D.similar_line(r) for r in ROWS)
        self.assertEqual(D.with_lines("x", lines).count("похожие"), 5)
        long = "y" * 900
        out = D.with_lines(long, lines)
        self.assertLessEqual(D.visible_len(out), 1024)
        self.assertLess(out.count("похожие"), 5)

    def test_similar_rows_without_run_or_service(self):
        self.assertEqual(asyncio.run(D.similar_rows(None)), [])
        with patch.object(D, "MT_BASE", "http://127.0.0.1:9"):
            self.assertEqual(asyncio.run(D.similar_rows(RUN, timeout=0.5)), [])


class _Resp:
    def __init__(self, code, body):
        self.status_code, self._b = code, body

    def json(self):
        return self._b


class _Client:
    calls = []
    code = 200

    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, json=None):
        _Client.calls.append((url, json))
        if _Client.code is None:
            raise OSError("down")
        return _Resp(_Client.code, {"ok": True, "started": 4242})


def _query(kind="tear"):
    msg = SimpleNamespace(chat=SimpleNamespace(id=-100, type="supergroup"), message_id=55, caption="✅ Task completed",
                          caption_html="✅ <b>Task completed</b>", reply_markup=D.markup(TASK))
    q = SimpleNamespace(data=f"rd:{kind}:{TASK}", message=msg, answer=AsyncMock(),
                        from_user=SimpleNamespace(id=7, first_name="Owner", last_name="", username="own"))
    ctx = SimpleNamespace(bot=SimpleNamespace(edit_message_caption=AsyncMock(), edit_message_text=AsyncMock(),
                                              edit_message_reply_markup=AsyncMock(), send_message=AsyncMock()))
    return SimpleNamespace(callback_query=q), ctx


class Press(unittest.TestCase):
    def setUp(self):
        _Client.calls, _Client.code = [], 200
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(D, "_resolve_task", AsyncMock(return_value=(TASK, RUN))))
        self.stack.enter_context(patch("httpx.AsyncClient", _Client))
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.stack.enter_context(patch.object(D, "REPORT_DIR", Path(self.tmp.name)))

    def test_press_posts_report_and_marks_notice(self):
        upd, ctx = _query("elbows")
        asyncio.run(D.on_callback(upd, ctx))
        url, body = _Client.calls[0]
        self.assertTrue(url.endswith(f"/api/mt/defect-reports/{TASK}/elbows/press"))
        self.assertEqual((body["run_id"], body["chat_id"], body["message_id"], body["by"]["id"]), (RUN, -100, 55, 7))
        upd.callback_query.answer.assert_awaited_once()
        kw = ctx.bot.edit_message_caption.call_args.kwargs
        self.assertIn("🔎 разбор: elbows · в работе", kw["caption"])
        self.assertTrue(kw["reply_markup"].inline_keyboard[2][0].text.startswith("🔎"))
        self.assertEqual(os.listdir(self.tmp.name), [])          # MT wrote it, not the bot

    def test_mt_down_writes_the_report_file(self):
        _Client.code = None
        upd, ctx = _query("transform")
        asyncio.run(D.on_callback(upd, ctx))
        rec = json.loads((Path(self.tmp.name) / f"{TASK}.transform.json").read_text(encoding="utf-8"))
        self.assertEqual((rec["status"], rec["kind"], rec["run_id"], rec["chat_id"], rec["message_id"]),
                         ("new", "transform", RUN, -100, 55))
        self.assertEqual(rec["checks"]["state"], "pending_mt")
        self.assertEqual(rec["by"]["id"], 7)


class DB:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass

    async def execute(self, *a):
        task = SimpleNamespace(owner_type="anon", status="done", content_rating="safe", guid=None, worker_api=None,
                               collection_title=None, collection_member_title=None, collection_index=None,
                               collection_size=None, viewer_settings=json.dumps({"v3": {"session": {"mt_run_id": RUN}}}))
        return SimpleNamespace(scalar_one_or_none=lambda: task)


class Notice(unittest.IsolatedAsyncioTestCase):
    async def test_notice_has_defect_buttons_and_similar_lines_for_operators_only(self):
        with tempfile.TemporaryDirectory() as d, ExitStack() as st:
            video = Path(d) / "v.mp4"
            video.write_bytes(b"fixture")
            api = SimpleNamespace(send_video=AsyncMock(return_value=SimpleNamespace(message_id=7)),
                                  send_photo=AsyncMock(), send_message=AsyncMock())
            st.enter_context(patch("telegram.Bot", return_value=api))
            st.enter_context(patch.object(delivery, "AsyncSessionLocal", return_value=DB()))
            st.enter_context(patch.object(D, "similar_rows", AsyncMock(return_value=ROWS)))

            async def retry(factory, **kw):
                return await factory()
            helpers = {"_get_token": lambda: "fixture-token", "_task_url": lambda _: "https://autorig.online/task?id=x",
                       "_task_telegram_metrics": AsyncMock(return_value={}), "_format_task_metrics": lambda _: "#m",
                       "get_broadcast_chat_ids": AsyncMock(return_value=[123]),
                       "private_chats_awaiting_task": AsyncMock(return_value=[999]),
                       "pop_notification_message_id": AsyncMock(return_value=None),
                       "peek_notification_message_id": AsyncMock(return_value=None),
                       "reserve_notification": AsyncMock(return_value=True), "mark_task_done_notification_sent": AsyncMock(),
                       "_cleanup_generation_chat": AsyncMock(), "_send_with_retry": retry,
                       "attach_notification_message_id": AsyncMock(),
                       "_download_video_from_worker": AsyncMock(return_value=(str(video), 0, "fixture"))}
            for k, v in helpers.items():
                st.enter_context(patch.object(bot, k, v))
            await delivery.broadcast_task_done(TASK)
            calls = {c.kwargs["chat_id"]: c.kwargs for c in api.send_video.await_args_list}
            bot.attach_notification_message_id.assert_any_await(123, "task_done", TASK, 7)
            self.assertEqual(set(calls), {123, 999})
            self.assertEqual(calls[123]["caption"].count("🔗 похожие:"), 5)
            self.assertNotIn("похожие", calls[999]["caption"])
            for kw in calls.values():
                data = [r[0].callback_data for r in kw["reply_markup"].inline_keyboard]
                self.assertEqual([x.split(":")[1] for x in data], ["transform", "tear", "elbows", "unknown"])
                self.assertFalse(any(re.match(r"^rf[gc]:", x) for x in data))


if __name__ == "__main__":
    unittest.main()

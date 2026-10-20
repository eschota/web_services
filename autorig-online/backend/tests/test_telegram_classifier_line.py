"""The classifier's verdict is the first line of every AutoRig task notice (owner 2026-10-20, register §15: «сверху тут
сначала пиши Классификатор как определил объект»). Fixture class.json / category.json for agree / disagree / missing,
and the notice itself: first line in the operators' chat (ru) and in the customer's private chat (owner language).
No Telegram, no network, no customer writes."""
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import telegram_bot as bot
import telegram_classifier_line as C
import telegram_defects as D
import telegram_task_completion as delivery

TASK = "c8691854-bdd2-4503-9281-fdc8cafdb0d7"
AGREE_RUN, SPLIT_RUN, NONE_RUN = "5a9ca4f4e6886d89e5a6", "0000000000000000beef", "ffffffffffffffffffff"
NEIGH = [{"task": "t%d" % i, "category": c, "what": "chicken"}
         for i, c in enumerate(["bird", "bird", "humanoid", "quadruped", "bird", "humanoid", "bird"])]
FIXTURES = {
    AGREE_RUN: (
        {"schema": "autorig.mt.class/1", "category": "bird", "variant": None, "confidence": 1.0, "agree": True,
         "needs_review": False, "voters": {"voxels": {"vote": "legs", "legs": "two"},
                                           "vision": {"vote": "legs", "legs": "two"},
                                           "neighbours": {"vote": "legs", "neighbours": NEIGH}},
         "limbs": {"verdict": "biped", "voters": {"voxels": {"vote": "two", "arms": 2}}}},
        {"category": "bird", "body": "bird", "what": "yellow <chick>", "source": "vision"}),
    SPLIT_RUN: (
        {"schema": "autorig.mt.class/1", "category": "legless", "variant": "tail", "confidence": 0.55, "agree": False,
         "needs_review": True, "voters": {"voxels": {"vote": "legless", "legs": "none"},
                                          "vision": {"vote": "legs", "legs": "two"},
                                          "neighbours": {"vote": "legs", "neighbours": []}}},
        {"category": "humanoid", "body": "biped", "what": "demon"}),
}


def _write(root: Path):
    for run, (cls, cat) in FIXTURES.items():
        a = root / run / "analysis"
        a.mkdir(parents=True)
        (a / "class.json").write_text(json.dumps(cls), encoding="utf-8")
        (a / "category.json").write_text(json.dumps(cat), encoding="utf-8")
    (root / NONE_RUN / "analysis").mkdir(parents=True)         # a run without class.json


class Line(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        _write(self.root)

    def test_agree(self):
        s = C.line(AGREE_RUN, "ru", self.root)
        self.assertTrue(s.startswith("🧭 <b>Классификатор:</b> bird · yellow &lt;chick&gt; · уверенность 1.0"), s)
        self.assertIn("уверенность 1.0 · голоса согласны", s)
        self.assertNotIn("руки", s)                                # no arms field on a non-biped category
        self.assertNotIn("biped", s)
        self.assertIn("(воксели 2 ноги · Vision 2 ноги · соседи bird 4/7)", s)
        self.assertNotIn("⚠️", s)
        self.assertNotIn("{", s)                                   # never raw JSON
        e = C.line(AGREE_RUN, "en", self.root)
        self.assertTrue(e.startswith("🧭 <b>Classifier:</b> bird"), e)
        self.assertIn("confidence 1.0 · voters agree", e)
        self.assertIn("voxels 2 legs", e)

    def test_disagree_low_confidence_is_flagged(self):
        s = C.line(SPLIT_RUN, "ru", self.root)
        self.assertTrue(s.startswith("⚠️ 🧭 <b>Классификатор:</b> humanoid/tail · классификатор legless · demon"), s)
        self.assertIn("тело biped", s)
        self.assertIn("уверенность 0.55 · голоса расходятся · на проверку", s)
        self.assertIn("воксели без ног · Vision 2 ноги", s)
        low = dict(FIXTURES[AGREE_RUN][0], confidence=0.6)        # agree, but under 0.7
        self.assertTrue(C.render(low, FIXTURES[AGREE_RUN][1], "ru").startswith("⚠️ 🧭"))
        # «unknown» with two voters abstaining (run 684122b4, 2026-10-20): confidence 1.0 / agree, still flagged
        lone = dict(FIXTURES[AGREE_RUN][0], category="unknown",
                    voters={"voxels": {"vote": "legs", "legs": "two"}, "vision": {"vote": "abstain"},
                            "neighbours": {"vote": "abstain"}})
        s = C.render(lone, {"category": "humanoid", "body": "biped", "what": "cartoonish 3D character"}, "ru")
        self.assertTrue(s.startswith("⚠️ 🧭 <b>Классификатор:</b> humanoid · классификатор unknown"), s)
        self.assertIn("руки: 2", s)                                # top-level arms record (voxel hands)
        self.assertIn("(воксели 2 ноги · Vision нет голоса · соседи нет голоса)", s)

    def test_arms_on_bipeds_only(self):
        santa = {"category": "humanoid", "confidence": 1.0, "agree": True, "needs_review": False,
                 "voters": {"voxels": {"vote": "legs", "legs": "two"}, "vision": {"vote": "legs", "legs": "two"},
                            "neighbours": {"vote": "legs", "neighbours": []}},
                 "arms": {"voxel_hands": 2, "vision_arms": 2, "ok": True},
                 "limbs": {"verdict": "biped", "voters": {"voxels": {"vote": "abstain", "arms": 0},
                                                          "analyser": {"vote": "two", "arms": 2},
                                                          "vision": {"vote": "two", "arms": 2}}}}
        cat = {"category": "humanoid", "body": "biped", "what": "Santa Claus"}
        s = C.render(santa, cat, "ru")
        self.assertTrue(s.startswith("🧭 <b>Классификатор:</b> humanoid · Santa Claus · тело biped · руки: 2 · "), s)
        self.assertIn("arms: 2 · confidence", C.render(santa, cat, "en"))
        split = dict(santa, limbs={"verdict": "biped", "voters": {"voxels": {"vote": "two", "arms": 2},
                                                                   "analyser": {"vote": "two", "arms": 0},
                                                                   "vision": {"vote": "two", "arms": 2}}})
        s = C.render(split, cat, "ru")
        self.assertTrue(s.startswith("⚠️ 🧭"), s)
        self.assertIn("⚠️ руки: 2 (воксели 2 · анализатор 0 · Vision 2)", s)
        three = dict(santa, limbs={"verdict": "biped", "voters": {"voxels": {"vote": "two", "arms": 6},
                                                                   "analyser": {"vote": "two", "arms": 1},
                                                                   "vision": {"vote": "two", "arms": 2}}})
        self.assertIn("⚠️ руки: ? (воксели 6 · анализатор 1 · Vision 2)", C.render(three, cat, "ru"))
        gazelle = dict(santa, category="quadruped", arms={"voxel_hands": 0, "vision_arms": 0})
        s = C.render(gazelle, {"category": "quadruped", "body": "quadruped", "what": "gazelle"}, "ru")
        self.assertNotIn("руки", s)
        self.assertNotIn("biped", s)

    def test_missing(self):
        for run in (NONE_RUN, "1234567890abcdef1234", None, "../etc"):
            self.assertEqual(C.line(run, "ru", self.root), "🧭 <b>Классификатор:</b> нет данных")
        self.assertEqual(C.line(None, "de", self.root), "🧭 <b>Classifier:</b> no data")
        self.assertEqual(C.lang_of("ru-RU"), "ru")
        self.assertEqual(C.lang_of("fa"), "en")


class DB:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass

    async def execute(self, *a):
        task = SimpleNamespace(owner_type="anon", owner_id="", owner_language="ru", status="done",
                               content_rating="safe", guid=None, worker_api=None, collection_title=None,
                               collection_member_title=None, collection_index=None, collection_size=None,
                               viewer_settings=json.dumps({"v3": {"session": {"mt_run_id": AGREE_RUN}}}))
        return SimpleNamespace(scalar_one_or_none=lambda: task)


class Notice(unittest.IsolatedAsyncioTestCase):
    async def test_first_line_in_operator_and_private_chats(self):
        with tempfile.TemporaryDirectory() as d, ExitStack() as st:
            root = Path(d) / "runs"
            _write(root)
            video = Path(d) / "v.mp4"
            video.write_bytes(b"fixture")
            api = SimpleNamespace(send_video=AsyncMock(return_value=SimpleNamespace(message_id=7)),
                                  send_photo=AsyncMock(), send_message=AsyncMock())
            st.enter_context(patch("telegram.Bot", return_value=api))
            st.enter_context(patch.object(delivery, "AsyncSessionLocal", return_value=DB()))
            st.enter_context(patch.object(C, "RUNS_DIR", root))
            st.enter_context(patch.object(D, "similar_rows", AsyncMock(return_value=[])))

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
            calls = {c.kwargs["chat_id"]: c.kwargs["caption"] for c in api.send_video.await_args_list}
            self.assertEqual(set(calls), {123, 999})
            self.assertTrue(calls[123].startswith("🧭 <b>Классификатор:</b> bird"), calls[123])
            self.assertIn("\n✅ <b>Task completed</b>", calls[123])
            first = calls[999].split("\n", 1)[0]
            self.assertTrue(first.startswith("🧭 <b>"), first)       # the owner's language (ru here, else en)
            self.assertIn("bird", first)


if __name__ == "__main__":
    unittest.main()

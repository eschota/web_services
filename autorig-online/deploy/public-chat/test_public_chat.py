"""Tests for the public chat service (Public chat · V3). Run: python -m unittest test_public_chat -v
(from deploy/public-chat; needs starlette + httpx, as in /srv/autorig/venv)."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]            # autorig-online/

TMP = Path(tempfile.mkdtemp(prefix="pchat-test-"))
SITE = TMP / "site.db"
os.environ.update({
    "PCHAT_DATA": str(TMP / "data"),
    "PCHAT_SITE_DB": str(SITE),
    "PCHAT_CONFIG": str(TMP / "live" / "public-chat.json"),
    "PCHAT_KEY_FILE": str(TMP / "missing.key"),
    "PCHAT_AGENT_TOKENS": str(TMP / "agents.json"),
    "PCHAT_BACKEND": "http://127.0.0.1:9",
    "PCHAT_ADMIN_EMAILS": "boss@example.com",
    "PCHAT_BACKEND_CODE": str(REPO / "backend"),
    "PCHAT_STATIC_ROOTS": str(REPO / "static"),
    "AUTORIG_STATIC_DIR": str(REPO / "static"),
    "PCHAT_AVATARS": str(TMP / "avatars"),
    "PCHAT_MT_ROOT": str(TMP / "mt"),
    "PCHAT_GLB_CACHE": str(TMP / "glb"),
    "PCHAT_TASK_CACHE": str(TMP / "tasks"),
})
sys.path.insert(0, str(Path(__file__).resolve().parent))
import public_chat as pc  # noqa: E402

TASK = "2d67cf20-5331-4838-b1e7-b20379717b01"
TASK_B = "aaaaaaaa-5331-4838-b1e7-b20379717b01"        # a guest's (ANON_B) public model
TASK_N = "bbbbbbbb-5331-4838-b1e7-b20379717b01"        # user 3's public model
TASK_ADULT = "cccccccc-5331-4838-b1e7-b20379717b01"    # user 3's adult model: never listed
PRIVATE = "11111111-2222-3333-4444-555555555555"
ANON_A = "99e98ffe-7f21-4d47-ac8d-e76cf2cb1173"
ANON_B = "0ba814ff-6df8-4dbf-94a9-153bdc3d3188"
ANON_C = "33345678-1234-4234-8234-123456789012"
ANON_D = "44345678-1234-4234-8234-123456789012"
USER_TOKEN = "ab" * 32
ADMIN_TOKEN = "cd" * 32


def make_site_db() -> None:
    con = sqlite3.connect(SITE)
    con.executescript("""
    CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT, nickname TEXT,
                        preferred_language TEXT, detected_language TEXT);
    CREATE TABLE sessions (token TEXT PRIMARY KEY, user_id INTEGER, created_at TEXT, expires_at TEXT);
    CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT, is_public INTEGER, content_rating TEXT,
                        poster_llm_title TEXT, ready_urls TEXT, output_urls TEXT, owner_type TEXT, owner_id TEXT, video_ready INTEGER DEFAULT 1, created_at TEXT, input_url TEXT,
                        viewer_prepared_glb_url TEXT);
    CREATE TABLE anon_sessions (anon_id TEXT PRIMARY KEY, preferred_language TEXT, detected_language TEXT);
    """)
    con.execute("INSERT INTO anon_sessions VALUES (?, NULL, 'de-DE,en')", (ANON_A,))
    con.execute("INSERT INTO users VALUES (1, 'someone@example.com', 'Maria Petrova', NULL, 'fa', 'fa,en')")
    con.execute("INSERT INTO users VALUES (2, 'boss@example.com', 'Escho Boss', NULL, NULL, 'ru')")
    con.execute("INSERT INTO sessions VALUES (?, 1, '2026-10-01 00:00:00', '2099-01-01 00:00:00.000000')",
                (USER_TOKEN,))
    con.execute("INSERT INTO sessions VALUES (?, 2, '2026-10-01 00:00:00', '2099-01-01 00:00:00.000000')",
                (ADMIN_TOKEN,))
    con.execute("INSERT INTO users VALUES (3, 'nobody@example.com', 'Secret Name', NULL, NULL, 'de')")
    con.execute("INSERT INTO users VALUES (4, 'nick@example.com', 'Real Name', 'ArtistNick', NULL, 'en')")

    def task(tid, public, rating, title, otype, owner, created, url=None):
        con.execute("INSERT INTO tasks(id, status, is_public, content_rating, poster_llm_title, owner_type, owner_id,"
                    " created_at, input_url, ready_urls) VALUES (?, 'done', ?, ?, ?, ?, ?, ?, ?, ?)",
                    (tid, public, rating, title, otype, owner, created, url, '["x_video_poster.jpg"]'))

    task(TASK, 1, "safe", "Knight in armor", "user", "someone@example.com", "2026-10-02 10:00:00")
    task(PRIVATE, 0, "safe", "Hidden", "user", "someone@example.com", "2026-10-03 10:00:00")
    task(TASK_B, 1, "safe", "Guest knight", "anon", ANON_B, "2026-10-04 10:00:00")
    task(TASK_N, 1, "safe", "Nick <b>robot</b>", "user", "nick@example.com", "2026-10-05 10:00:00", "https://x/y.glb")
    task("dddddddd-5331-4838-b1e7-b20379717b01", 1, "safe", "Nick robot again", "user", "nick@example.com",
         "2026-10-06 10:00:00", "https://x/y.glb")                       # same upload converted twice: one card
    task(TASK_ADULT, 1, "adult", "Adult model", "user", "nick@example.com", "2026-10-07 10:00:00")
    task("eeeeeeee-5331-4838-b1e7-b20379717b01", 1, "safe", "Quiet one", "user", "nobody@example.com",
         "2026-10-08 10:00:00")
    con.commit()
    con.close()


make_site_db()


class TextPolicy(unittest.TestCase):
    def test_clean_text(self):
        self.assertEqual(pc.clean_text("  hi​ there  "), "hi there")
        self.assertEqual(pc.clean_text("a\n\n\n\nb"), "a\n\nb")
        self.assertEqual(pc.clean_text("!!!!!!!!!!!!!!"), "!!!!!!")
        self.assertEqual(pc.clean_text("‮evil"), "evil")
        # Persian keeps its zero-width non-joiner
        self.assertIn("‌", pc.clean_text("می‌خواهم"))
        many = "\n".join(str(i) for i in range(20))
        self.assertEqual(len(pc.clean_text(many).split("\n")), pc.MAX_LINES)

    def test_blocked_true(self):
        for text in ("иди нахуй", "ты пиздец", "заебал уже", "бля, опять", "f.u.c.k you", "FUUUCK",
                     "visit my onlyfans", "best casino bonus", "xуй", "mother fucker", "you are a retard",
                     "pidor", "کیر", "похуй", "охуенно", "нихуя себе"):
            self.assertTrue(pc.blocked(text), text)

    def test_blocked_false(self):
        for text in ("Hello! Nice model", "Как сделать риг?", "ребята, привет", "небо красивое", "хлеба купить",
                     "вебинар завтра", "художник", "Xue Mei likes it", "retargeting works", "scunthorpe",
                     "Хуан из Испании", "обеда не будет", "хочу похудеть", "страхуй машину", "سلام، مدل زیبایی است", "这个模型很好", "Kira model"):
            self.assertFalse(pc.blocked(text), text)

    def test_detect_lang(self):
        self.assertEqual(pc.detect_lang("سلام دوستان، چطورید؟", "en"), "fa")
        self.assertEqual(pc.detect_lang("مرحبا", "en"), "ar")
        self.assertEqual(pc.detect_lang("Привет всем", "en"), "ru")
        self.assertEqual(pc.detect_lang("Привіт усім, як справи? їжак", "en"), "uk")
        self.assertEqual(pc.detect_lang("你好", "en"), "zh")
        self.assertEqual(pc.detect_lang("नमस्ते", "en"), "hi")
        self.assertEqual(pc.detect_lang("hello there", "fa"), "en")
        self.assertEqual(pc.detect_lang("hola amigos", "es"), "es")
        self.assertEqual(pc.detect_lang("👍", "fa"), "fa")

    def test_links(self):
        rep = pc.scan_links(f"look https://autorig.online/task?id={TASK} and www.spam.xyz/buy")
        self.assertEqual(rep.task_ids, [TASK])
        self.assertEqual(rep.external, ["www.spam.xyz/buy"])
        rep = pc.scan_links(f"autorig.online/fa/task?id={TASK}&x=1")
        self.assertEqual(rep.task_ids, [TASK])
        self.assertEqual(rep.external, [])
        self.assertEqual(pc.scan_links("join t.me/somechannel").invites, 1)
        self.assertEqual(pc.scan_links("version 1.2 is out, see example.com").external, ["example.com"])

    def test_astra_addressing(self):
        for text in ("@Astra how do I rig?", "Астра, привет", "бот, помоги", "hey bot what is this",
                     "آسترا کمک کن", "Астре вопрос"):
            self.assertTrue(pc.addressed_to_astra(text), text)
        for text in ("robot model", "both of them", "abbot", "astronaut", "работа"):
            self.assertFalse(pc.addressed_to_astra(text), text)

    def test_nickname(self):
        self.assertEqual(pc.valid_nickname("Maria"), "Maria")
        self.assertEqual(pc.valid_nickname("مریم"), "مریم")
        for bad in ("Astra", "admin", "AutoRig team", "x", "http://a.b", "пидор", "Escho", "12345"):
            self.assertIsNone(pc.valid_nickname(bad), bad)

    def test_mod_answer(self):
        v = pc.parse_mod_answer("1: OK\n2: SPAM\n3: **ABUSE**\n9: OK\nnoise", 3)
        self.assertEqual(v, {1: "OK", 2: "SPAM", 3: "ABUSE"})

    def test_limiter(self):
        lim = pc.Limiter()
        rules = [(2, 10.0)]
        self.assertEqual(lim.check("k", rules), 0)
        self.assertEqual(lim.check("k", rules), 0)
        self.assertGreater(lim.check("k", rules), 0)


class Api(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from starlette.testclient import TestClient
        Path(os.environ["PCHAT_CONFIG"]).parent.mkdir(parents=True, exist_ok=True)
        Path(os.environ["PCHAT_CONFIG"]).write_text(json.dumps({"llm_moderation": False, "astra": "off"}))
        Path(os.environ["PCHAT_AGENT_TOKENS"]).write_text(json.dumps(
            {"agents": [{"name": "astra", "sha256": hashlib.sha256(b"agent-secret").hexdigest()}]}))
        cls.ctx = TestClient(pc.app, base_url="https://autorig.online")
        cls.client = cls.ctx.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.ctx.__exit__(None, None, None)

    def post(self, path, body, cookies=None, headers=None):
        h = {"Content-Type": "application/json", "X-Real-IP": "203.0.113.7"}
        h.update(headers or {})
        self.client.cookies.clear()
        for k, v in (cookies or {}).items():
            self.client.cookies.set(k, v)
        return self.client.post(path, content=json.dumps(body), headers=h)

    def get(self, path, cookies=None):
        self.client.cookies.clear()
        for k, v in (cookies or {}).items():
            self.client.cookies.set(k, v)
        return self.client.get(path, headers={"X-Real-IP": "203.0.113.7"})

    def test_01_state_guest_and_user(self):
        doc = self.get(f"/api/public-chat/state?task={TASK}", {"anon_id": ANON_A}).json()
        self.assertTrue(doc["enabled"])
        self.assertEqual(doc["me"]["kind"], "guest")
        self.assertTrue(doc["me"]["name"].startswith("Guest-"))
        self.assertEqual(doc["me"]["lang"], "de")
        self.assertNotIn(ANON_A, json.dumps(doc))
        self.assertEqual([r["id"] for r in doc["rooms"]], ["general", f"task:{TASK}"])
        self.assertTrue(doc["rooms"][1]["available"])
        doc = self.get("/api/public-chat/state", {"session": USER_TOKEN}).json()
        self.assertEqual(doc["me"]["kind"], "user")
        self.assertEqual(doc["me"]["name"], "Maria")
        self.assertEqual(doc["me"]["lang"], "fa")
        self.assertNotIn("someone@example.com", json.dumps(doc))
        doc = self.get(f"/api/public-chat/state?task={PRIVATE}", {"anon_id": ANON_A}).json()
        self.assertFalse(doc["rooms"][1]["available"])

    def test_02_post_and_read(self):
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "Hello everyone!", "lang": "en"},
                      {"anon_id": ANON_A})
        self.assertEqual(r.status_code, 200, r.text)
        msg = r.json()["message"]
        self.assertEqual(msg["author"]["kind"], "guest")
        self.assertEqual(msg["lang"], "en")
        r = self.post("/api/public-chat/messages",
                      {"room": "general", "text": f"Look https://autorig.online/task?id={TASK}", "lang": "fa"},
                      {"session": USER_TOKEN})
        self.assertEqual(r.status_code, 200, r.text)
        card = r.json()["message"]["cards"][0]
        self.assertEqual(card["title"], "Knight in armor")
        self.assertEqual(card["thumb"], f"/thumb/{TASK}")
        doc = self.get("/api/public-chat/messages?room=general").json()
        texts = [m["text"] for m in doc["messages"]]
        self.assertIn("Hello everyone!", texts)
        self.assertNotIn("ip_hash", json.dumps(doc))

    def test_03_filters(self):
        cases = [
            ({"text": ""}, "empty"),
            ({"text": "abcdefghij " * 60}, "too_long"),
            ({"text": "buy now at cheap-pills.xyz"}, "links_not_allowed"),
            ({"text": "join t.me/spam"}, "links_not_allowed"),
            ({"text": "заебали все"}, "blocked_words"),
        ]
        for body, code in cases:
            body = dict(body, room="general")
            r = self.post("/api/public-chat/messages", body, {"anon_id": ANON_B})
            self.assertEqual(r.json()["detail"]["error_string"], code, body)
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "same text twice"}, {"anon_id": ANON_B})
        self.assertEqual(r.status_code, 200)
        time.sleep(0.01)
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "Same text TWICE!"}, {"anon_id": ANON_B})
        self.assertEqual(r.json()["detail"]["error_string"], "duplicate")

    def test_04_identity_origin_room(self):
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "hi"})
        self.assertEqual(r.status_code, 401)
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "hi"}, {"anon_id": ANON_A},
                      {"Origin": "https://evil.example"})
        self.assertEqual(r.status_code, 403)
        r = self.post("/api/public-chat/messages", {"room": f"task:{PRIVATE}", "text": "hi"}, {"anon_id": ANON_A})
        self.assertEqual(r.json()["detail"]["error_string"], "room_unavailable")

    def test_05_rate_limit(self):
        anon = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        codes = []
        for i in range(6):
            r = self.post("/api/public-chat/messages", {"room": "general", "text": f"msg number {i} here"},
                          {"anon_id": anon}, {"X-Real-IP": "198.51.100.1"})
            codes.append(r.status_code)
        self.assertIn(429, codes)

    def test_06_report_hides(self):
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "annoying message"},
                      {"anon_id": ANON_A})
        mid = r.json()["message"]["id"]
        self.assertEqual(self.post("/api/public-chat/report", {"id": mid}, {"anon_id": ANON_A}).status_code, 400)
        self.post("/api/public-chat/report", {"id": mid}, {"anon_id": ANON_B})
        r = self.post("/api/public-chat/report", {"id": mid}, {"session": USER_TOKEN})
        self.assertTrue(r.json()["hidden"])  # a guest's message hides at two reports
        ids = [m["id"] for m in self.get("/api/public-chat/messages?room=general").json()["messages"]]
        self.assertNotIn(mid, ids)

    def test_07_admin(self):
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "to be deleted soon"},
                      {"anon_id": ANON_B})
        mid = r.json()["message"]["id"]
        self.assertEqual(self.post("/api/public-chat/admin/delete", {"id": mid}, {"session": USER_TOKEN}).status_code,
                         403)
        self.assertEqual(self.post("/api/public-chat/admin/delete", {"id": mid}, {"session": ADMIN_TOKEN}).status_code,
                         200)
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "another one from b"},
                      {"anon_id": ANON_B})
        mid = r.json()["message"]["id"]
        self.post("/api/public-chat/admin/mute", {"id": mid, "minutes": 5}, {"session": ADMIN_TOKEN})
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "can I still talk"},
                      {"anon_id": ANON_B})
        self.assertEqual(r.json()["detail"]["error_string"], "muted")
        # the kill switch
        r = self.post("/api/public-chat/admin/config", {"enabled": False}, {"session": ADMIN_TOKEN})
        self.assertFalse(r.json()["config"]["enabled"])
        self.assertFalse(self.get("/api/public-chat/state", {"anon_id": ANON_A}).json()["enabled"])
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "is it closed"}, {"anon_id": ANON_A})
        self.assertEqual(r.json()["detail"]["error_string"], "chat_disabled")
        self.assertEqual(self.get("/api/public-chat/stream?rooms=general").status_code, 204)
        self.post("/api/public-chat/admin/config", {"enabled": True}, {"session": ADMIN_TOKEN})
        self.assertTrue(self.get("/api/public-chat/state").json()["enabled"])
        saved = json.loads(Path(os.environ["PCHAT_CONFIG"]).read_text())
        self.assertTrue(saved["enabled"])
        self.assertEqual(saved["history"][-1]["changes"], {"enabled": True})

    def test_08_agent_reply(self):
        r = self.post("/api/public-chat/agent/reply", {"room": "general", "text": "Hi, I am Astra"})
        self.assertEqual(r.status_code, 401)
        r = self.post("/api/public-chat/agent/reply", {"room": "general", "text": "Hi, I am Astra"},
                      headers={"Authorization": "Bearer agent-secret"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["message"]["author"]["kind"], "astra")

    def test_09_rename(self):
        r = self.post("/api/public-chat/me", {"name": "Astra"}, {"session": USER_TOKEN})
        self.assertEqual(r.json()["detail"]["error_string"], "bad_name")
        r = self.post("/api/public-chat/me", {"name": "Masha 3D"}, {"session": USER_TOKEN})
        self.assertEqual(r.json()["me"]["name"], "Masha 3D")
        self.assertEqual(self.post("/api/public-chat/me", {"name": "Guesty"}, {"anon_id": ANON_A}).status_code, 403)

    def test_10_translate_cached(self):
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "translate me please"},
                      {"anon_id": "12345678-1234-4234-8234-123456789012"})
        mid = r.json()["message"]["id"]
        pc.store().save_translation(mid, "fa", "لطفا مرا ترجمه کن")
        r = self.post("/api/public-chat/translate", {"id": mid, "lang": "fa"}, {"anon_id": ANON_A})
        self.assertEqual(r.json()["state"], "done")
        self.assertEqual(r.json()["text"], "لطفا مرا ترجمه کن")

    def test_11_stream(self):
        pc.STREAM_MAX_S, pc.PING_S = 1.0, 0.2
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "stream me"},
                      {"anon_id": "22345678-1234-4234-8234-123456789012"})
        mid = r.json()["message"]["id"]
        self.client.cookies.clear()
        with self.client.stream("GET", f"/api/public-chat/stream?rooms=general&after={mid - 1}&cid=t1") as resp:
            self.assertEqual(resp.headers["content-type"].split(";")[0], "text/event-stream")
            body = "".join(resp.iter_text())
        self.assertIn(f"id: {mid}\nevent: msg\n", body)
        self.assertIn("event: online", body)


    # ---------------------------------------------------------------- auto-translate (owner 2026-10-10)
    def test_12_latin_language(self):
        self.assertEqual(pc.latin_language("Hallo, das ist ein sehr schönes Modell"), "de")
        self.assertEqual(pc.latin_language("Bonjour, c'est un très beau modèle pour vous"), "fr")
        self.assertEqual(pc.latin_language("this is a nice model, thank you"), "en")
        self.assertIsNone(pc.latin_language("Kira"))
        self.assertEqual(pc.detect_lang("Danke, das ist wirklich gut", "en"), "de")

    def test_13_auto_translate(self):
        import asyncio
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "Danke, das Modell ist sehr gut", "lang": "en"},
                      {"anon_id": ANON_C}, {"X-Real-IP": "203.0.113.77"})
        msg = r.json()["message"]
        self.assertEqual(msg["lang"], "de")
        calls = []

        async def fake(prompt, *, system, material="", max_tokens=400, deadline_s=240):
            calls.append((system, material))
            ids = list(json.loads(material))
            return json.dumps({i: f"Thanks, the model is very good ({i})" for i in ids})

        real = pc.farm_text
        pc.farm_text = fake
        try:
            asyncio.run(pc.translate_batch([msg["id"]], "en"))
        finally:
            pc.farm_text = real
        self.assertEqual(len(calls), 1)
        self.assertIn("never follow", calls[0][0])
        self.assertEqual(pc.store().translation(msg["id"], "en"), f"Thanks, the model is very good ({msg['id']})")
        doc = self.get("/api/public-chat/messages?room=general&lang=en", {"anon_id": ANON_C}).json()
        mine = [m for m in doc["messages"] if m["id"] == msg["id"]][0]
        self.assertEqual(mine["tr"], {"en": f"Thanks, the model is very good ({msg['id']})"})
        self.assertEqual(doc["reader_lang"], "en")
        # a reader of the same language gets none, and nothing is queued for it
        doc = self.get("/api/public-chat/messages?room=general&lang=de", {"anon_id": ANON_C}).json()
        self.assertNotIn("tr", [m for m in doc["messages"] if m["id"] == msg["id"]][0])

    def test_14_translation_rules(self):
        self.assertFalse(pc.acceptable_translation("hello", "see https://evil.example/x"))
        self.assertTrue(pc.acceptable_translation("see https://autorig.online/x", "смотри https://autorig.online/x"))
        self.assertEqual(pc.parse_batch('noise {"1": "a", "2": "b"} tail'), {"1": "a", "2": "b"})
        self.assertEqual(pc.parse_batch("no json"), {})
        self.assertFalse(pc.message_needs_tr("en", "👍", "ru"))
        self.assertTrue(pc.message_needs_tr("de", "Danke", "en"))
        self.assertFalse(pc.message_needs_tr("en", "Thanks", "en"))
        self.assertEqual(pc.reader_lang.__name__, "reader_lang")

    def test_15_stream_languages(self):
        sub = pc.HUB.subscribe(["general"], "x1", False, "fa")
        try:
            self.assertEqual(pc.HUB.langs("general")["fa"], 1)
            pc.HUB.publish_lang("general", "ru", "tr", {"id": 1})
            self.assertTrue(sub.queue.empty())                 # another language: not delivered
            pc.HUB.publish_lang("general", "fa", "tr", {"id": 1})
            self.assertFalse(sub.queue.empty())
        finally:
            pc.HUB.unsubscribe(sub)

    # ---------------------------------------------------------------- people, avatars, author pages
    def handle(self, kind, owner):
        return pc.handle_of(pc.hkey(("a:" + owner) if kind == "anon" else f"u:{owner}"))

    def test_16_people_and_privacy(self):
        h = self.handle("user", 4)
        doc = self.get(f"/api/people/{h}").json()
        self.assertEqual(doc["name"], "ArtistNick")
        self.assertEqual(doc["models"], 1)                     # duplicate upload merged, adult one never listed
        self.assertEqual(doc["url"], f"/author/{h}")
        self.assertNotIn("nick@example.com", json.dumps(doc))
        g = self.handle("anon", ANON_B)
        doc = self.get(f"/api/people/{g}").json()
        self.assertEqual(doc["kind"], "guest")
        self.assertTrue(doc["name"].startswith("Guest-"))
        self.assertNotIn(ANON_B, json.dumps(doc))
        # a user who never wrote in the chat and set no nickname does not get their real first name published
        n = self.handle("user", 3)
        self.assertTrue(self.get(f"/api/people/{n}").json()["name"].startswith("User-"))
        self.assertEqual(self.get("/api/people/0123456789").status_code, 404)
        me = self.get("/api/people/me", {"anon_id": ANON_B}).json()
        self.assertEqual(me["handle"], g)
        self.assertEqual(self.get("/api/people/me").status_code, 404)
        # resolve is for agents on this host only (the test client is not one)
        self.assertEqual(self.get("/api/people/resolve?email=nick@example.com").status_code, 403)

    def test_17_author_page(self):
        h = self.handle("user", 4)
        r = self.get(f"/author/{h}")
        self.assertEqual(r.status_code, 200)
        body = r.text
        self.assertIn("<h1", body)
        self.assertIn("ArtistNick", body)
        self.assertIn('href="/task?id=dddddddd-5331-4838-b1e7-b20379717b01"', body)
        self.assertNotIn(TASK_ADULT, body)
        self.assertNotIn(TASK_N, body)                          # the older duplicate upload is not listed twice
        self.assertIn('class="header"', body)                  # shared header is in the initial HTML
        self.assertIn('class="footer-grid"', body)
        self.assertIn(f'<link rel="canonical" href="https://autorig.online/author/{h}">', body)
        self.assertIn('hreflang="fa"', body)
        self.assertIn('content="index, follow"', body)
        self.assertNotIn("nick@example.com", body)
        self.assertIn("/thumb/dddddddd-5331-4838-b1e7-b20379717b01", body)
        fa = self.get(f"/fa/author/{h}")
        self.assertEqual(fa.status_code, 200)
        self.assertIn('dir="rtl"', fa.text)
        self.assertIn('lang="fa"', fa.text)
        self.assertIn("مدل‌های سه‌بعدی", fa.text)
        self.assertEqual(self.get(f"/de/author/{h}").status_code, 404)
        self.assertEqual(self.get("/author/0123456789").status_code, 404)
        self.assertEqual(self.get(f"/author/{h}?page=5").status_code, 404)
        guest = self.get(f"/author/{self.handle('anon', ANON_B)}")
        self.assertIn("Guest-", guest.text)
        self.assertIn('content="noindex, follow"', guest.text)
        self.assertNotIn(ANON_B, guest.text)

    def test_18_avatar_endpoint(self):
        h = self.handle("user", 4)
        author = pc.hkey("u:4")
        pc.AVATAR_DIR.mkdir(parents=True, exist_ok=True)
        for size in pc.AV_SIZES:
            pc.avatar_file(TASK_N, size).write_bytes(b"RIFF\x00\x00\x00\x00WEBP")
        pc.AV[author] = TASK_N
        r = self.get(f"/api/avatar/{h}?s=64&v={TASK_N[:8]}")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers["content-type"], "image/webp")
        self.assertIn("immutable", r.headers["cache-control"])
        self.assertEqual(pc.avatar_url(author, 64), f"/api/avatar/{h}?s=64&v={TASK_N[:8]}")
        self.assertIn(f'/api/avatar/{h}?s=256', self.get(f"/author/{h}").text)
        self.assertEqual(self.get("/api/avatar/zzz").status_code, 404)
        # an author whose model cannot be rendered (no source files) simply has no avatar
        pc.AV[pc.hkey("u:3")] = None
        r = self.get(f"/api/avatar/{self.handle('user', 3)}")
        self.assertEqual(r.status_code, 404)
        # messages carry the handle and the avatar url, never an id
        r = self.post("/api/public-chat/messages", {"room": "general", "text": "avatar test message"},
                      {"anon_id": ANON_D}, {"X-Real-IP": "203.0.113.78"})
        self.assertEqual(r.status_code, 200, r.text)
        author_doc = r.json()["message"]["author"]
        self.assertEqual(author_doc["handle"], self.handle("anon", ANON_D))
        self.assertNotIn(ANON_D, json.dumps(r.json()))


class AvatarGeometry(unittest.TestCase):
    """The circle crop: the model's alpha bounding shape fits the circle exactly, centred, with a small margin."""

    def test_circle_is_tight_and_centred(self):
        try:
            import numpy as np
            import avatar_make
        except Exception as exc:  # noqa: BLE001
            self.skipTest(f"numpy/opencv not available: {exc}")
        rgba = np.zeros((400, 300, 4), np.uint8)
        rgba[60:340, 120:180, :3] = (200, 120, 60)             # a tall figure, off centre
        rgba[60:340, 120:180, 3] = 255
        out, info = avatar_make.circle_avatar(rgba)
        self.assertEqual(out.shape, (avatar_make.MASTER, avatar_make.MASTER, 4))
        self.assertEqual(out[0, 0, 3], 0)                      # outside the circle is transparent
        self.assertEqual(out[avatar_make.MASTER // 2, avatar_make.MASTER // 2, 3], 255)
        cx, cy, r = info["circle_px"]
        self.assertAlmostEqual(cx, 149.5, delta=1.5)
        self.assertAlmostEqual(cy, 199.5, delta=1.5)
        self.assertLess(abs(r - 140.0), 3.0)                   # half the figure's height: the minimal circle
        fig = out[..., :3].astype(int)
        orange = (abs(fig[..., 0] - 200) < 12) & (abs(fig[..., 1] - 120) < 12) & (out[..., 3] == 255)
        ys, xs = np.nonzero(orange)
        # the figure spans the circle top to bottom, leaving only the margin
        self.assertGreater(ys.max() - ys.min(), avatar_make.MASTER * 0.88)
        self.assertLess(ys.max() - ys.min(), avatar_make.MASTER * 0.95)


if __name__ == "__main__":
    unittest.main()

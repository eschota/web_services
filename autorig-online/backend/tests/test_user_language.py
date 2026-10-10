"""User language field, /<lang>/ pages and server-side page localization (Localization · V3)."""
import asyncio
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import user_language as ul  # noqa: E402


PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <title>AI Auto Rigging | AutoRig</title>
    <meta name="description" content="English description">
    <meta property="og:title" content="English og">
    <link rel="canonical" href="https://autorig.online/">
</head>
<body>
<div id="site-header" data-server-rendered="1">
<a href="/" class="logo">Logo</a>
<a href="/gallery" class="nav-link" data-i18n="nav_gallery">Gallery</a>
<a href="/faq">FAQ</a>
<button class="lang-btn"><span data-lang-current>EN</span></button>
</div>
<main>
<h1 data-i18n="hero_title">Automatic rigging</h1>
<li data-i18n="hiw_tpose_orientation"><strong>Model orientation:</strong> normalization</li>
<div data-i18n="outer"><span data-i18n="inner">inner</span> outer</div>
<input type="text" data-i18n-placeholder="search_ph" placeholder="search">
<p data-i18n="missing_key">Kept as is</p>
<script>const tpl = '<span data-i18n="hero_title">Automatic rigging</span>';</script>
</main>
<div id="site-footer" data-server-rendered="1"><a href="/how-it-works">How</a></div>
</body>
</html>
"""


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        (self.tmp / "i18n").mkdir()
        (self.tmp / "css").mkdir()
        (self.tmp / "css" / "rtl.css").write_text("[dir=rtl]{}", encoding="utf-8")
        en = {"nav_gallery": "Gallery", "hero_title": "Automatic rigging", "outer": "Outer",
              "inner": "Inner", "search_ph": "search", "hiw_tpose_orientation": "<strong>Model:</strong> x",
              "error_storage_paused": "New uploads are paused."}
        fa = {"nav_gallery": "گالری", "hero_title": "ریگینگ <خودکار> & سریع", "outer": "بیرونی",
              "inner": "درونی", "search_ph": "جست‌وجو", "hiw_tpose_orientation": "<strong>جهت مدل:</strong> <i>x</i><script>",
              "error_storage_paused": "آپلودهای جدید متوقف شده‌اند.",
              "seo_home_title": "عنوان فارسی", "seo_home_description": "توضیح \"فارسی\""}
        (self.tmp / "i18n" / "en.json").write_text(json.dumps(en, ensure_ascii=False), encoding="utf-8")
        (self.tmp / "i18n" / "fa.json").write_text(json.dumps(fa, ensure_ascii=False), encoding="utf-8")
        (self.tmp / "i18n" / "zh.json").write_text(json.dumps({"nav_gallery": "画廊"}, ensure_ascii=False),
                                                     encoding="utf-8")
        os.environ["AUTORIG_STATIC_DIR"] = str(self.tmp)
        os.environ["APP_URL"] = "https://autorig.online"
        ul._JSON_CACHE.clear()
        ul._ASSET_HASH.clear()

    def tearDown(self):
        os.environ.pop("AUTORIG_STATIC_DIR", None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def render(self, info):
        token = ul._REQUEST_LANG.set(info)
        try:
            return ul.localize_page(PAGE, "/")
        finally:
            ul._REQUEST_LANG.reset(token)


class Parsing(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(ul.normalize_language("fa-IR"), "fa")
        self.assertEqual(ul.normalize_language("zh_Hans_CN"), "zh")
        self.assertEqual(ul.normalize_language("pes"), "fa")
        self.assertIsNone(ul.normalize_language("*"))
        self.assertIsNone(ul.normalize_language("<script>"))

    def test_accept_language_order(self):
        self.assertEqual(ul.parse_language_list("en;q=0.5, fa-IR, fa;q=0.9, de;q=0"), ["fa", "en"])
        self.assertEqual(ul.parse_language_list(["fa-IR", "fa", "en-US"]), ["fa", "en"])

    def test_resolution(self):
        info = ul.resolve_language(accept_language="fa-IR,fa;q=0.9,en-US;q=0.8")
        self.assertEqual((info.code, info.ui, info.source), ("fa", "fa", "accept_language"))
        info = ul.resolve_language(explicit="en", accept_language="fa-IR")
        self.assertEqual((info.code, info.ui, info.source), ("fa", "en", "accept_language"))
        info = ul.resolve_language(explicit="ru")                 # nothing known about the browser
        self.assertEqual((info.code, info.ui, info.source), ("ru", "ru", "explicit"))
        info = ul.resolve_language(accept_language="de-DE,de;q=0.9,en;q=0.8")
        self.assertEqual((info.code, info.ui), ("de", "en"))   # answer in German, English interface
        info = ul.resolve_language(url_lang="fa", accept_language="en")
        self.assertEqual((info.code, info.ui), ("en", "fa"))
        info = ul.resolve_language(task_language="fa")
        self.assertEqual((info.code, info.source), ("fa", "task"))
        self.assertEqual(ul.resolve_language().payload()["code"], "en")

    def test_payload_and_instruction(self):
        p = ul.resolve_language(explicit="fa").payload()
        self.assertEqual((p["dir"], p["native_name"]), ("rtl", "فارسی"))
        self.assertIn("Persian (فارسی)", p["agent_instruction"])
        self.assertIn("English", ul.language_instruction("en"))


class Pages(Base):
    def test_persian_page(self):
        info = ul.resolve_language(accept_language="fa-IR", path="/")
        out = self.render(info)
        self.assertIn('<html lang="fa" dir="rtl" data-i18n-scope="page">', out)
        self.assertIn('data-i18n="nav_gallery">گالری</a>', out)
        self.assertIn("ریگینگ &lt;خودکار&gt; &amp; سریع", out)                  # escaped text
        self.assertIn("<strong>جهت مدل:</strong> <i>x</i>&lt;script&gt;", out)   # only safe tags survive
        self.assertIn('data-i18n="outer">بیرونی</div>', out)                    # outer element wins
        self.assertIn('placeholder="جست‌وجو"', out)
        self.assertIn("Kept as is", out)                                         # missing key keeps text
        self.assertIn("const tpl = '<span data-i18n=\"hero_title\">Automatic rigging</span>'", out)  # JS untouched
        self.assertIn("<span data-lang-current>FA</span>", out)
        self.assertIn("<title>عنوان فارسی</title>", out)
        self.assertIn('content="توضیح &quot;فارسی&quot;"', out)
        self.assertIn('hreflang="fa" href="https://autorig.online/fa/"', out)
        self.assertIn('hreflang="x-default" href="https://autorig.online/"', out)
        self.assertIn('rel="canonical" href="https://autorig.online/fa/"', out)
        self.assertIn("/static/css/rtl.css?v=", out)
        self.assertIn('"lang": "fa"', out)
        self.assertNotIn('href="/fa/gallery"', out)                               # no prefix: links unchanged

    def test_prefixed_url_rewrites_links(self):
        info = ul.resolve_language(url_lang="fa", path="/")
        out = self.render(info)
        self.assertIn('href="/fa/gallery"', out)
        self.assertIn('href="/fa/"', out)
        self.assertIn('href="/fa/how-it-works"', out)
        self.assertIn('href="/faq"', out)                                         # not advertised: unchanged
        self.assertIn('"source": "url"', out)

    def test_english_page_keeps_text(self):
        out = self.render(ul.resolve_language(path="/"))
        self.assertIn('data-i18n="hero_title">Automatic rigging</h1>', out)
        self.assertIn('<html lang="en" dir="ltr" data-i18n-scope="page">', out)
        self.assertIn('rel="canonical" href="https://autorig.online/"', out)
        self.assertIn('hreflang="ru" href="https://autorig.online/ru/"', out)
        self.assertNotIn("rtl.css", out)

    def test_unadvertised_prefix_is_noindex(self):
        token = ul._REQUEST_LANG.set(ul.resolve_language(url_lang="zh", path="/faq"))
        try:
            out = ul.localize_page(PAGE, "/faq")
        finally:
            ul._REQUEST_LANG.reset(token)
        self.assertIn('name="robots" content="noindex, follow"', out)
        self.assertIn('rel="canonical" href="https://autorig.online/faq"', out)

    def test_all_five_languages_advertised(self):
        out = self.render(ul.resolve_language(url_lang="zh", path="/"))
        for lang in ("en", "ru", "zh", "hi", "fa"):
            self.assertIn(f'hreflang="{lang}"', out)
        self.assertIn('rel="canonical" href="https://autorig.online/zh/"', out)

    def test_chrome_scope_page(self):
        token = ul._REQUEST_LANG.set(ul.resolve_language(explicit="fa", path="/faq"))
        try:
            out = ul.localize_page(PAGE, "/faq")
        finally:
            ul._REQUEST_LANG.reset(token)
        self.assertIn('data-i18n-scope="chrome"', out)
        self.assertIn('<div id="site-header" data-server-rendered="1" dir="rtl" lang="fa">', out)
        self.assertNotIn('<html lang="fa"', out)

    def test_no_request_no_change(self):
        self.assertEqual(ul.localize_page(PAGE, "/"), PAGE)

    def test_user_error_detail(self):
        token = ul._REQUEST_LANG.set(ul.resolve_language(explicit="fa"))
        try:
            detail = ul.user_error_detail("storage_paused", retry_after_seconds=600)
        finally:
            ul._REQUEST_LANG.reset(token)
        self.assertEqual(detail["error_string"], "storage_paused")
        self.assertEqual(detail["message_string"], "آپلودهای جدید متوقف شده‌اند.")
        self.assertTrue(detail["user_message_bool"])
        self.assertEqual(detail["retry_after_seconds_int"], 600)
        self.assertEqual(ul.user_error_detail("storage_paused", lang="en")["message_string"],
                         "New uploads are paused.")


class Middleware(unittest.TestCase):
    def run_scope(self, path, headers=()):
        seen = {}

        async def app(scope, receive, send):
            seen["path"] = scope["path"]
            seen["info"] = ul.current_request_language()
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", b"text/html; charset=utf-8")]})
            await send({"type": "http.response.body", "body": b"ok"})

        sent = []

        async def send(message):
            sent.append(message)

        async def receive():
            return {"type": "http.request"}

        mw = ul.LanguageMiddleware(app)
        scope = {"type": "http", "method": "GET", "path": path, "raw_path": path.encode(),
                 "headers": [(k.encode(), v.encode()) for k, v in headers]}
        asyncio.run(mw(scope, receive, send))
        return seen, dict(sent[0]["headers"])

    def test_prefix_rewrite(self):
        seen, headers = self.run_scope("/fa/gallery")
        self.assertEqual(seen["path"], "/gallery")
        self.assertEqual((seen["info"].ui, seen["info"].url_lang), ("fa", "fa"))
        self.assertEqual(headers[b"content-language"], b"fa")
        self.assertIn(b"Accept-Language", headers[b"vary"])

    def test_root_prefix_and_api_untouched(self):
        self.assertEqual(self.run_scope("/fa")[0]["path"], "/")
        self.assertEqual(self.run_scope("/fa/api/me/language")[0]["path"], "/fa/api/me/language")
        self.assertEqual(self.run_scope("/fax")[0]["path"], "/fax")

    def test_cookie_and_accept_language(self):
        seen, _ = self.run_scope("/", [("cookie", "a=1; autorig_lang=ru"), ("accept-language", "fa-IR")])
        # the menu choice sets the page language, the browser locale stays the language to answer in
        self.assertEqual((seen["info"].ui, seen["info"].explicit, seen["info"].code), ("ru", "ru", "fa"))
        seen, _ = self.run_scope("/", [("accept-language", "fa-IR,fa;q=0.9")])
        self.assertEqual((seen["info"].ui, seen["info"].code), ("fa", "fa"))
        self.assertIsNone(ul.current_request_language())          # reset after the request


class BrowserLocaleFirst(unittest.TestCase):
    """Owner 2026-10-10: agents answer in the native language of the person, from the browser locale;
    a chat binds to its owner. EN / RU / FA / HE / DE browsers."""

    CASES = {
        "en": ("en-US,en;q=0.9", "en", "en", "English", "ltr"),
        "ru": ("ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "ru", "ru", "Russian", "ltr"),
        "fa": ("fa-IR,fa;q=0.9,en-US;q=0.8", "fa", "fa", "Persian", "rtl"),
        "he": ("he-IL,he;q=0.9,en;q=0.8", "he", "en", "Hebrew", "rtl"),
        "de": ("de-DE,de;q=0.9,en;q=0.8", "de", "en", "German", "ltr"),
    }

    def test_each_browser_gets_its_own_language(self):
        for label, (header, code, ui, name, direction) in self.CASES.items():
            with self.subTest(browser=label):
                info = ul.resolve_language(accept_language=header)
                p = info.payload()
                self.assertEqual((p["code"], p["ui"], p["name"], p["dir"]), (code, ui, name, direction))
                self.assertIn(name, p["agent_instruction"])

    def test_navigator_languages_win_over_accept_language(self):
        info = ul.resolve_language(browser=["he-IL", "he"], accept_language="en-US")
        self.assertEqual((info.code, info.source), ("he", "browser"))

    def test_persian_is_never_hebrew(self):
        self.assertEqual(ul.normalize_language("fa-IR"), "fa")
        self.assertEqual(ul.normalize_language("pes"), "fa")
        self.assertEqual(ul.normalize_language("iw"), "he")
        fa = ul.language_instruction("fa")
        self.assertIn("Persian", fa)
        self.assertNotIn("Hebrew", fa)
        he = ul.language_instruction("he")
        self.assertIn("Hebrew", he)
        self.assertNotIn("Persian", he)

    def test_interface_choice_does_not_change_the_reply_language(self):
        # The bug of 2026-10-10: a Russian browser with the menu set to Persian got Persian answers.
        info = ul.resolve_language(explicit="fa", accept_language="ru-RU,ru;q=0.9")
        self.assertEqual((info.code, info.ui, info.source), ("ru", "fa", "accept_language"))
        info = ul.resolve_language(explicit="fa", url_lang="fa", stored_detected="ru,en")
        self.assertEqual((info.code, info.ui, info.source), ("ru", "fa", "profile"))

    def test_page_url_language_does_not_change_the_reply_language(self):
        info = ul.resolve_language(url_lang="fa", accept_language="de-DE")
        self.assertEqual((info.code, info.ui), ("de", "fa"))

    def test_task_owner_language_only_without_any_browser_data(self):
        info = ul.resolve_language(task_language="fa", stored_detected="en")
        self.assertEqual(info.code, "en")
        self.assertEqual(ul.resolve_language(task_language="fa").code, "fa")

    def test_middleware_ui_follows_menu_but_code_follows_browser(self):
        info = ul.request_language_from_headers({"cookie": "autorig_lang=fa", "accept-language": "ru-RU,ru"})
        self.assertEqual((info.ui, info.code), ("fa", "ru"))


class _Sess:
    language = None
    language_source = None


class _Db:
    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def execute(self, *_a, **_k):
        raise RuntimeError("no database in this test")


class _Req:
    def __init__(self, accept, cookie=""):
        self.headers = {"accept-language": accept, "cookie": cookie}
        self.cookies = {}


class SupportChatBinding(unittest.TestCase):
    def bind(self, sess, accept, browser=None, widget=None, only_if_missing=False):
        return asyncio.run(ul.record_support_session_language(
            _Db(), sess, _Req(accept), user=None, widget_language=widget, browser_languages=browser,
            only_if_missing=only_if_missing))

    def test_each_visitor_chat_keeps_its_owner_language(self):
        chats = {}
        for label, (header, code, *_rest) in BrowserLocaleFirst.CASES.items():
            sess = _Sess()
            self.bind(sess, header)
            chats[label] = sess
            self.assertEqual(sess.language, code, label)
        # A message from someone else never re-binds an existing chat.
        self.bind(chats["fa"], "en-US,en", only_if_missing=True)
        self.assertEqual(chats["fa"].language, "fa")
        self.assertEqual(chats["ru"].language, "ru")

    def test_widget_reported_browser_languages_win(self):
        sess = _Sess()
        self.bind(sess, "en-US", browser="fa-IR,fa,en-US", widget="en")
        self.assertEqual((sess.language, sess.language_source), ("fa", "browser"))


class SupportLine(unittest.TestCase):
    def test_line(self):
        class S:
            language = "fa"
        line = ul.support_language_line(S())
        self.assertIn("<b>fa</b>", line)
        self.assertIn("reply in Persian", line)
        S.language = None
        self.assertEqual(ul.support_language_line(S()), "")


if __name__ == "__main__":
    unittest.main()

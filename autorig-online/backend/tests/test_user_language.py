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
        self.assertEqual((info.code, info.ui, info.source), ("en", "en", "explicit"))
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
        out = self.render(ul.resolve_language(url_lang="zh", path="/"))
        self.assertIn('name="robots" content="noindex, follow"', out)
        self.assertIn('rel="canonical" href="https://autorig.online/"', out)

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
        self.assertEqual((seen["info"].ui, seen["info"].source), ("ru", "explicit"))
        seen, _ = self.run_scope("/", [("accept-language", "fa-IR,fa;q=0.9")])
        self.assertEqual((seen["info"].ui, seen["info"].code), ("fa", "fa"))
        self.assertIsNone(ul.current_request_language())          # reset after the request


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

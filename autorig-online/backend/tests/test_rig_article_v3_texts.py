"""Per-animal rig pages (/dog-auto-rig ... /humanoid-auto-rig, -ru/-zh/-hi) describe the V3 conveyor.

main.py needs the whole backend stack, so this test executes only the rig-article block of main.py (from
RIG_ARTICLE_LANGS to _rig_article_response) in a small namespace. RIG_ARTICLE_MAIN_PY points it at another copy
(for example a patched one) of main.py. Apply tools/patch_rig_articles_v3.py first: these checks describe the
patched texts.
"""
import html
import json
import os
import re
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

MAIN_PY = Path(os.environ.get("RIG_ARTICLE_MAIN_PY") or Path(__file__).resolve().parent.parent / "main.py")
BAD = re.compile(r"\bV2\b|OpenPose|T-pose or|clear T-pose|coming soon", re.I)
PROMISED_NUMBERS = re.compile(r"\b\d+\s*(?:seconds|minutes|sec|мин|секунд)\b|\b\d+\s*[-–]\s*\d+\s*(?:minutes|min)\b", re.I)


def load_block():
    src = MAIN_PY.read_text(encoding="utf-8").replace("\r\n", "\n")
    a = src.index("RIG_ARTICLE_LANGS: Dict[str, Dict[str, str]] = {")
    b = src.index("async def _rig_article_response(")
    ns: Dict[str, Any] = {
        "Dict": Dict, "Any": Any, "List": List, "Optional": Optional, "Tuple": Tuple,
        "html": html, "json": json, "re": re,
        "APP_URL": "https://autorig.online",
        "Task": object, "AsyncSession": object,
        "select": None, "or_": None, "func": None,
        "_gallery_task_has_poster_sql": lambda: None,
        "_gallery_rig_icon_key": lambda task: "humanoid",
    }
    exec(compile(src[a:b], str(MAIN_PY), "exec"), ns)
    return ns


class RigArticleV3Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ns = load_block()

    def test_every_page_is_v3(self):
        ns = self.ns
        langs = list(ns["RIG_ARTICLE_LANGS"])
        self.assertEqual(set(langs), {"en", "ru", "zh", "hi"})
        for key in ns["RIG_ARTICLE_TYPES"]:
            for lang in langs:
                page = ns["_build_rig_article_html"](key, lang, [])
                with self.subTest(key=key, lang=lang):
                    self.assertEqual(len(re.findall(r"<h1[ >]", page)), 1)
                    title = re.search(r"<title>(.*?)</title>", page, re.S).group(1)
                    desc = html.unescape(re.search(r'name="description" content="([^"]*)"', page).group(1))
                    self.assertLessEqual(len(html.unescape(title)), 60)
                    self.assertLessEqual(len(desc), 160)
                    self.assertIsNone(BAD.search(page), BAD.search(page))
                    self.assertIsNone(PROMISED_NUMBERS.search(re.sub(r"<script.*?</script>", "", page, flags=re.S)))
                    for block in re.findall(r'<script type="application/ld\+json">(.*?)</script>', page, re.S):
                        json.loads(block)
                    text = ns["_rig_article_text"](key, lang)
                    self.assertEqual(len(text["faq"]), 3)

    def test_category_per_animal(self):
        ns = self.ns
        text = lambda k: ns["_rig_article_text"](k, "en")["lead"]
        for key in ("horse", "deer"):
            self.assertIn("scapula, elbow, mid-neck and leg profile", text(key))
            self.assertIn("planted-hoof", text(key))
        self.assertIn("low-body quadruped", text("turtle"))
        self.assertIn("no separate turtle template", text("turtle"))
        self.assertIn("biped category", text("humanoid"))
        for key in ("dog", "bear", "cat", "cow", "elephant", "giraffe", "mouse", "pig", "rabbit"):
            self.assertIn("quadruped category", text(key))
            self.assertNotIn("scapula", text(key))

    def test_seo_mechanism_and_keys_kept(self):
        ns = self.ns
        text = ns["_rig_article_text"]("horse", "en")
        for k in ("title", "description", "h1", "lead", "workflow", "prep", "label", "shape", "use", "faq", "ui", "keywords"):
            self.assertIn(k, text)
        self.assertTrue(text["title"].endswith("| AutoRig.online"))
        self.assertEqual(ns["_rig_article_path"]("horse", "ru"), "/horse-auto-rig-ru")


if __name__ == "__main__":
    unittest.main()

"""Lightweight contract checks for Idle LTX split pipeline (no DB, no httpx)."""
import re
import unittest


def _user_slug(task_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", f"autorig_{task_id}")[:56] or "autorig"


class IdleLtxUserSlugTests(unittest.TestCase):
    def test_slug_contains_task_prefix(self):
        tid = "030cea38-bc48-438c-bbe3-9c81166837f9"
        s = _user_slug(tid)
        self.assertTrue(s.startswith("autorig_"))
        self.assertIn("030cea38", s)

    def test_slug_render_variant_body_must_match(self):
        """Document: render-variant checks user_name_string == this slug."""
        tid = "abc"
        self.assertEqual(_user_slug(tid), "autorig_abc")


if __name__ == "__main__":
    unittest.main()

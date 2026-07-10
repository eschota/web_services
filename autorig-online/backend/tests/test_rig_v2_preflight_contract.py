from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
APP_JS = ROOT / "static" / "js" / "app.js"


class RigV2PreflightContractTests(unittest.TestCase):
    def test_multiview_map_does_not_pass_index_as_captured_image(self):
        source = APP_JS.read_text(encoding="utf-8")
        self.assertIn(
            "views.slice(1).map((view) => analyze(view))",
            source,
        )
        self.assertNotIn("views.slice(1).map(analyze)", source)


if __name__ == "__main__":
    unittest.main()

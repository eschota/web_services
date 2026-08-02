import json
import math
import sys
import unittest
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = BACKEND_DIR.parent / "static"
sys.path.insert(0, str(BACKEND_DIR))

from viewer_theme_contract import validate_viewer_theme_lighting  # noqa: E402


class ViewerThemeContractTests(unittest.TestCase):
    def valid_theme(self):
        return {
            "environment_settings": {"intensity": 1.0, "reflection_intensity": 3.0},
            "sun_settings": {"intensity": 2.45},
        }

    def test_validates_sane_ancient_ruins_and_all_theme_files(self):
        theme_root = STATIC_DIR / "env" / "backdrops"
        paths = sorted(theme_root.glob("*.json"))
        self.assertTrue(paths)
        for path in paths:
            with self.subTest(theme=path.name):
                data = json.loads(path.read_text(encoding="utf-8"))
                validate_viewer_theme_lighting(data)

        ancient = json.loads((theme_root / "ancient_ruins.json").read_text(encoding="utf-8"))
        checked = validate_viewer_theme_lighting(ancient)
        self.assertEqual(checked["environment_intensity"], 1.0)
        self.assertEqual(checked["reflection_intensity"], 3.0)
        self.assertEqual(checked["sun_intensity"], 2.45)

    def test_rejects_nonfinite_and_individually_out_of_range_values(self):
        for field, value in (
            ("environment", math.inf),
            ("environment", -0.01),
            ("environment", 2.01),
            ("reflection", math.nan),
            ("reflection", -0.01),
            ("reflection", 4.01),
            ("sun", -0.01),
            ("sun", 3.51),
        ):
            with self.subTest(field=field, value=value):
                theme = self.valid_theme()
                if field == "environment":
                    theme["environment_settings"]["intensity"] = value
                elif field == "reflection":
                    theme["environment_settings"]["reflection_intensity"] = value
                else:
                    theme["sun_settings"]["intensity"] = value
                with self.assertRaises(ValueError):
                    validate_viewer_theme_lighting(theme)

    def test_rejects_effective_environment_above_four(self):
        theme = self.valid_theme()
        theme["environment_settings"] = {"intensity": 2.0, "reflection_intensity": 2.01}
        with self.assertRaisesRegex(ValueError, "effective environment intensity"):
            validate_viewer_theme_lighting(theme)

    def test_main_load_and_save_paths_call_validator(self):
        source = (BACKEND_DIR / "main.py").read_text(encoding="utf-8")
        load_start = source.index("def _load_viewer_theme_json")
        list_start = source.index("def _viewer_theme_source_images", load_start)
        load_source = source[load_start:list_start]
        self.assertIn("validate_viewer_theme_lighting(data)", load_source)

        save_start = source.index("async def api_admin_save_viewer_theme")
        score_start = source.index("def _viewer_theme_score_text", save_start)
        save_source = source[save_start:score_start]
        self.assertLess(
            save_source.index("validate_viewer_theme_lighting(merged)"),
            save_source.index("_atomic_write_json_file"),
        )
    def test_public_viewer_glb_headers_force_inline_disposition(self):
        source = (BACKEND_DIR / "main.py").read_text(encoding="utf-8")
        headers_start = source.index("def _glb_viewer_headers")
        proxy_start = source.index("async def _proxy_model_file", headers_start)
        headers_source = source[headers_start:proxy_start]
        self.assertIn('"Content-Disposition": "inline"', headers_source)


if __name__ == "__main__":
    unittest.main()

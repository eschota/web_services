from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
APP_JS = ROOT / "static" / "js" / "app.js"
WORKERS_PY = ROOT / "backend" / "workers.py"


class RigV2PreflightContractTests(unittest.TestCase):
    def test_multiview_map_does_not_pass_index_as_captured_image(self):
        source = APP_JS.read_text(encoding="utf-8")
        self.assertIn(
            "views.slice(1).map((view) => analyze(view))",
            source,
        )
        self.assertNotIn("views.slice(1).map(analyze)", source)

    def test_all_disabled_workers_are_an_intentional_maintenance_drain(self):
        source = WORKERS_PY.read_text(encoding="utf-8")
        self.assertIn(
            "configured_count_res = await db.execute(select(func.count(WorkerEndpoint.id)))",
            source,
        )
        self.assertIn("if int(configured_count_res.scalar() or 0) > 0:", source)
        self.assertIn("return []", source)


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from pathlib import Path


class CustomAnimationMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.anim_dir = Path(__file__).resolve().parents[2] / "static" / "all_animations"
        cls.manifest_path = cls.anim_dir / "manifest.json"
        cls.glb_files = sorted(cls.anim_dir.glob("*.glb"))
        cls.json_files = sorted(
            p for p in cls.anim_dir.glob("*.json") if p.name != "manifest.json"
        )
        cls.manifest = json.loads(cls.manifest_path.read_text(encoding="utf-8"))

    def test_every_glb_has_json_pair(self):
        missing = []
        for glb in self.glb_files:
            json_pair = glb.with_suffix(".json")
            if not json_pair.exists():
                missing.append(glb.name)
        self.assertEqual(missing, [], f"Missing JSON for files: {missing}")

    def test_manifest_pricing_rules(self):
        pricing = self.manifest.get("pricing", {})
        self.assertEqual(pricing.get("single_animation_credits"), 1)
        self.assertEqual(pricing.get("all_animations_credits"), 10)
        self.assertEqual(str(pricing.get("download_format", "")).lower(), "fbx")

    def test_manifest_has_unique_animation_ids(self):
        items = self.manifest.get("animations", [])
        ids = [item.get("id") for item in items]
        self.assertEqual(len(ids), len(set(ids)), "Animation IDs must be unique")

    def test_manifest_entries_are_fbx_single_credit(self):
        items = self.manifest.get("animations", [])
        bad = []
        for item in items:
            if item.get("credits") != 1 or str(item.get("format", "")).lower() != "fbx":
                bad.append(item.get("id"))
        self.assertEqual(bad, [], f"Invalid pricing/format in: {bad}")


if __name__ == "__main__":
    unittest.main()

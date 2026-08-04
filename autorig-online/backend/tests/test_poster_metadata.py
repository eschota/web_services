"""Unit tests for poster keyword helpers (Free3D query + keyword padding)."""
import unittest
from unittest import mock

import content_moderation
from content_moderation import build_free3d_query_from_keywords
from content_moderation import build_free3d_similar_query
from content_moderation import _normalize_keyword_list


class PosterMetadataHelpersTests(unittest.TestCase):
    def test_build_free3d_query_empty(self):
        self.assertIsNone(build_free3d_query_from_keywords(None))
        self.assertIsNone(build_free3d_query_from_keywords([]))

    def test_build_free3d_query_three_max(self):
        q = build_free3d_query_from_keywords(["warrior", "fantasy", "game", "extra"])
        self.assertEqual(q, "warrior fantasy game")

    def test_build_free3d_query_dedupe_case_insensitive(self):
        q = build_free3d_query_from_keywords(["Rig", "rig", "Unity"])
        self.assertEqual(q, "Rig Unity")

    def test_normalize_keyword_list_length_25(self):
        short = ["a", "b"]
        out = _normalize_keyword_list(short)
        self.assertEqual(len(out), 25)
        self.assertTrue(all(isinstance(x, str) and x for x in out))

    def test_build_free3d_similar_query_empty(self):
        self.assertIsNone(build_free3d_similar_query(None, None))
        self.assertIsNone(build_free3d_similar_query("", []))

    def test_build_free3d_similar_query_title_and_keywords(self):
        q = build_free3d_similar_query(
            "Soldier in gas mask with sword",
            ["gas mask", "military", "grenade", "unity"],
        )
        self.assertIn("Soldier", q)
        self.assertIn("gas mask", q)

    def test_build_free3d_similar_query_skips_redundant_keywords(self):
        q = build_free3d_similar_query("Gas mask soldier", ["gas mask", "sword"])
        self.assertEqual(q, "Gas mask soldier sword")

    def test_build_free3d_similar_query_keywords_only_fallback(self):
        q = build_free3d_similar_query("", ["warrior", "fantasy", "extra"])
        self.assertEqual(q, "warrior fantasy extra")

    def test_build_free3d_similar_query_max_len(self):
        long_title = "word " * 80
        q = build_free3d_similar_query(long_title, ["a", "b"], max_len=50)
        self.assertLessEqual(len(q), 50)

    def test_renderfin_input_maps_to_turntable(self):
        self.assertEqual(
            content_moderation._renderfin_turntable_path_from_input_url(
                "https://autorig.online/renderfin/render/job/model.glb?download=1"
            ),
            "/var/autorig/renderfin/render/job/model_turntable.mp4",
        )
        self.assertIsNone(
            content_moderation._renderfin_turntable_path_from_input_url(
                "https://autorig.online/static/tasks/model.glb"
            )
        )

    @mock.patch.object(content_moderation, "_preview_image_for_metadata", return_value=b"jpeg")
    @mock.patch.object(content_moderation, "analyze_poster_llm_metadata")
    def test_pre_convert_metadata_builds_worker_payload(self, analyze, _preview):
        analyze.return_value = {
            "title": "Sci-Fi Soldier",
            "description": "Armored character.",
            "keywords": [f"keyword-{i}" for i in range(25)],
            "category": "Characters",
            "subcategory": "Sci-Fi",
        }
        payload = content_moderation.build_pre_convert_metadata_sync(
            "task-1", "https://example.invalid/model.glb"
        )
        self.assertEqual(payload["title"], "Sci-Fi Soldier")
        self.assertEqual(payload["category"], "Characters")
        self.assertEqual(payload["subcategory"], "Sci-Fi")
        self.assertEqual(payload["image_description"]["keywords"], analyze.return_value["keywords"])


if __name__ == "__main__":
    unittest.main()

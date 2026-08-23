import json
import tempfile
import unittest
from pathlib import Path

from renderfarmerbot import ConverterStatus, FarmSnapshot, StateStore, format_snapshot


def snapshot(**overrides):
    values = {
        "converter_queue": {
            "ok": True,
            "total_active": 0,
            "total_pending": 0,
            "total_queue": 0,
            "available_workers": 4,
            "total_workers": 4,
        },
        "renderfin": {
            "ok": True,
            "servers": 5,
            "pending": 0,
            "rendering": 0,
            "hunyuan_config_error": False,
            "hunyuan_pools": {
                "dedicated": ["f12", "raptor"],
                "shared_converter": ["f1", "f2", "f11", "f13"],
                "shared_reserved": 1,
                "ordinary_conversion_waiting": False,
            },
        },
        "converters": [
            ConverterStatus(name="F1", online=True, healthy=True, completed=20),
            ConverterStatus(name="F7", enabled=False),
        ],
        "disk_free_gb": 86.2,
        "disk_used_percent": 85.1,
        "checked_at": "2026-08-23T16:00:00+07:00",
        "errors": [],
    }
    values.update(overrides)
    return FarmSnapshot(**values)


class SnapshotTests(unittest.TestCase):
    def test_fingerprint_ignores_timestamp_and_disk_noise_inside_band(self):
        first = snapshot()
        second = snapshot(
            checked_at="2026-08-23T16:01:00+07:00",
            disk_free_gb=85.7,
            disk_used_percent=85.2,
        )
        self.assertEqual(first.fingerprint(), second.fingerprint())

    def test_fingerprint_changes_for_queue_or_storage_health(self):
        first = snapshot()
        queued = dict(first.converter_queue)
        queued["total_queue"] = 1
        self.assertNotEqual(first.fingerprint(), snapshot(converter_queue=queued).fingerprint())
        self.assertNotEqual(first.fingerprint(), snapshot(disk_free_gb=9.9).fingerprint())

    def test_status_uses_current_topology_without_duplicate_timestamp(self):
        text = format_snapshot(snapshot())
        self.assertIn("F7:</b> parked / disabled", text)
        self.assertIn("Dedicated: f12, raptor", text)
        self.assertIn("Converter reserve: 1", text)
        self.assertEqual(text.count("State snapshot:"), 1)
        self.assertNotIn("renderfin.com/api-render", text)


class StateStoreTests(unittest.TestCase):
    def test_legacy_import_reuses_dashboard_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "chats.json").write_text(json.dumps({"chats": [101, 202]}), encoding="utf-8")
            (root / "sessions.json").write_text(
                json.dumps(
                    {
                        "messages": [
                            {"chat_id": 101, "message_id": 11, "type": "status"},
                            {"chat_id": 101, "message_id": 12, "type": "results"},
                            {"chat_id": 202, "message_id": 21, "type": "status"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            store = StateStore(temp_dir)
            self.assertEqual(store.chats, [101, 202])
            self.assertEqual(store.message_id(101), 11)
            self.assertEqual(store.message_id(202), 21)
            self.assertTrue((root / "state.json").exists())

    def test_delivery_fingerprint_persists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StateStore(temp_dir)
            store.subscribe(101)
            store.mark_delivered(101, 55, "abc")
            reloaded = StateStore(temp_dir)
            self.assertEqual(reloaded.message_id(101), 55)
            self.assertEqual(reloaded.fingerprint(101), "abc")


if __name__ == "__main__":
    unittest.main()

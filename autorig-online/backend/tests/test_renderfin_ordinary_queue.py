import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from renderfin import config, hunyuan_client


class OrdinaryQueueTests(unittest.TestCase):
    def test_generation_and_v3_rows_never_pause_the_shared_hunyuan_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "autorig.db"
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE tasks(id TEXT, status TEXT, queue_class TEXT, pipeline_kind TEXT)")
            db.executemany("INSERT INTO tasks VALUES(?,?,?,?)", [
                ("gen", "processing", "interactive", "generate"),  # waits for Hunyuan itself
                ("v3", "created", "interactive", "v3"),            # runs on Motion Transfer
                ("old", "done", "interactive", "rig"),
            ])
            db.commit()
            db.close()
            with patch.object(config, "AUTORIG_QUEUE_DB_PATH", path):
                hunyuan_client._ORDINARY_QUEUE_CACHE = (0.0, False)
                self.assertFalse(hunyuan_client.ordinary_conversion_waiting(force_refresh=True))
                db = sqlite3.connect(path)
                db.execute("INSERT INTO tasks VALUES('rig', 'created', 'interactive', 'rig')")
                db.commit()
                db.close()
                self.assertTrue(hunyuan_client.ordinary_conversion_waiting(force_refresh=True))


if __name__ == "__main__":
    unittest.main()

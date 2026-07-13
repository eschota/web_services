"""Four-viewport backdrop manifest and derivative contract tests."""
from __future__ import annotations

import json
import struct
import unittest
from pathlib import Path
from urllib.parse import urlsplit


BACKDROP_ROOT = Path(__file__).resolve().parents[2] / "static" / "env" / "backdrops"
VIEW_IDS = ("perspective", "top", "front", "left")
JPEG_SOF_MARKERS = {
    0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
    0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
}


def jpeg_dimensions(path: Path) -> tuple[int, int]:
    """Read JPEG dimensions without adding Pillow to the backend test runtime."""
    with path.open("rb") as stream:
        if stream.read(2) != b"\xff\xd8":
            raise AssertionError(f"Not a JPEG file: {path}")
        while True:
            lead = stream.read(1)
            if not lead:
                break
            if lead != b"\xff":
                continue
            marker_byte = stream.read(1)
            while marker_byte == b"\xff":
                marker_byte = stream.read(1)
            if not marker_byte:
                break
            marker = marker_byte[0]
            if marker in (0x01, 0xD8, 0xD9) or 0xD0 <= marker <= 0xD7:
                continue
            length_raw = stream.read(2)
            if len(length_raw) != 2:
                break
            segment_length = struct.unpack(">H", length_raw)[0]
            if marker in JPEG_SOF_MARKERS:
                frame = stream.read(5)
                if len(frame) != 5:
                    break
                height, width = struct.unpack(">HH", frame[1:5])
                return width, height
            stream.seek(max(0, segment_length - 2), 1)
    raise AssertionError(f"JPEG dimensions were not found: {path}")


class SplitViewerBackdropTests(unittest.TestCase):
    def test_every_theme_has_complete_directional_backdrops(self):
        manifests = sorted(BACKDROP_ROOT.glob("*.json"))
        self.assertEqual(len(manifests), 14)

        for manifest_path in manifests:
            with self.subTest(theme=manifest_path.stem):
                theme = json.loads(manifest_path.read_text(encoding="utf-8"))
                sources = theme.get("viewport_srcs")
                self.assertIsInstance(sources, dict)
                self.assertEqual(set(sources), set(VIEW_IDS))

                for view_id in VIEW_IDS:
                    public_path = urlsplit(sources[view_id]).path
                    prefix = "/static/env/backdrops/"
                    self.assertTrue(public_path.startswith(prefix))
                    viewer_path = BACKDROP_ROOT / public_path.removeprefix(prefix)
                    self.assertTrue(viewer_path.is_file(), viewer_path)
                    self.assertEqual(jpeg_dimensions(viewer_path), (1280, 720))

                    if view_id == "perspective":
                        continue
                    source_path = BACKDROP_ROOT / "source" / view_id / f"{manifest_path.stem}.jpg"
                    thumb_path = BACKDROP_ROOT / "thumbs" / view_id / f"{manifest_path.stem}.jpg"
                    self.assertTrue(source_path.is_file(), source_path)
                    self.assertTrue(thumb_path.is_file(), thumb_path)
                    self.assertEqual(jpeg_dimensions(source_path), (1600, 900))
                    self.assertEqual(jpeg_dimensions(thumb_path), (160, 90))


if __name__ == "__main__":
    unittest.main()

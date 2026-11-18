"""Synthetic smoke checks for image import and vector proposals."""

from io import BytesIO
import unittest
from unittest.mock import patch

import cv2 as cv
import numpy as np
from PIL import Image

from recognition import RecognitionError, Token, _openings, recognize


def sample_floor():
    image = np.full((900, 1200, 3), 255, np.uint8)
    ink = (34, 32, 28)
    for y in (205, 219):
        cv.line(image, (165, y), (330, y), ink, 3)
        cv.line(image, (380, y), (1080, y), ink, 3)
    for y in (710, 724):
        cv.line(image, (165, y), (1080, y), ink, 3)
    for x in (165, 177, 1068, 1080):
        cv.line(image, (x, 205), (x, 724), ink, 3)
    cv.line(image, (330, 219), (330, 260), ink, 3)
    cv.ellipse(image, (330, 219), (50, 43), 0, 0, 90, ink, 2)
    cv.rectangle(image, (640, 724), (702, 739), ink, 2)
    cv.line(image, (640, 732), (702, 732), ink, 2)
    for y in range(440, 509, 12):
        cv.line(image, (492, y), (563, y), ink, 3)
    return Image.fromarray(cv.cvtColor(image, cv.COLOR_BGR2RGB))


def encode(image, fmt, **options):
    target = BytesIO()
    image.save(target, format=fmt, **options)
    return target.getvalue()


class TestRecognition(unittest.TestCase):
    def test_rasters_detect_shapes_without_inventing_measurements(self):
        image = sample_floor()
        with patch("recognition.shutil.which", return_value=None), patch.dict("os.environ", {"TESSDATA_PREFIX": ""}):
            for fmt in ("JPEG", "PNG", "TIFF"):
                with self.subTest(fmt=fmt):
                    result = recognize(encode(image, fmt), f"floor.{fmt.lower()}")
                    self.assertEqual(len(result["floors"]), 1)
                    floor = result["floors"][0]
                    self.assertEqual(len(floor["rooms"]), 1)
                    self.assertEqual(floor["source_bbox_px"][2:], [floor["image_width"], floor["image_height"]])
                    room = floor["rooms"][0]
                    self.assertEqual(
                        {(opening["wall"], opening["type"]) for opening in room["openings"]},
                        {("bottom", "WINDOW_STD"), ("top", "DOOR_SINGLE")},
                    )
                    self.assertIn("STAIRS", {symbol["type"] for symbol in room["symbols"]})
                    self.assertTrue(all(value is None for value in room["raw_measurements_cm"].values()))

    def test_pdf_keeps_page_order_and_blank_review_layer(self):
        image = sample_floor()
        blank = Image.new("RGB", image.size, "white")
        data = encode(image, "PDF", save_all=True, append_images=[blank])
        with patch("recognition.shutil.which", return_value=None), patch.dict("os.environ", {"TESSDATA_PREFIX": ""}):
            result = recognize(data, "test.pdf")
        self.assertEqual([floor["source_page"] for floor in result["floors"]], [1, 2])
        self.assertTrue(result["floors"][0]["rooms"])
        self.assertFalse(result["floors"][1]["rooms"])

    def test_elevation_is_kept_as_reference_not_room(self):
        image = np.full((900, 1200, 3), 255, np.uint8)
        roof = np.array([(220, 690), (600, 310), (980, 690)], dtype=np.int32)
        cv.polylines(image, [roof], True, (40, 40, 40), 3)
        data = cv.imencode(".png", image)[1].tobytes()
        with patch("recognition.shutil.which", return_value=None), patch.dict("os.environ", {"TESSDATA_PREFIX": ""}):
            result = recognize(data, "roof.png")
        self.assertFalse(result["floors"][0]["rooms"])

    def test_nearby_glazing_label_switches_door_class(self):
        room = {"box": (50, 50, 550, 350)}
        mark = (230, 45, 285, 57)
        label = Token("створ", (229, 19, 274, 32), 0.76, 0)
        openings = _openings(room, [], [("top", mark)], [], [label], 900)
        self.assertEqual([opening["type"] for opening in openings], ["DOOR_GLAZED"])

    def test_corrupt_document_returns_user_error(self):
        with self.assertRaises(RecognitionError):
            recognize(b"%PDF-1.4\n", "broken.pdf")
        with self.assertRaises(RecognitionError):
            recognize(b"invalid", "broken.png")


if __name__ == "__main__":
    unittest.main()

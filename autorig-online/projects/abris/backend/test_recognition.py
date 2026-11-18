"""Synthetic smoke checks for image import and vector proposals."""

from io import BytesIO
import unittest
from unittest.mock import patch

import cv2 as cv
import numpy as np
from PIL import Image

from recognition import RecognitionError, Token, _Tesseract, _ocr_header, _openings, recognize


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


def angled_plan_with_section():
    image = np.full((900, 1200, 3), 255, np.uint8)
    cv.rectangle(image, (120, 160), (670, 680), (30, 30, 30), 3)
    label = np.full((105, 155), 255, np.uint8)
    cv.putText(label, "405", (33, 64), cv.FONT_HERSHEY_SIMPLEX, 1.28, 20, 3)
    tilt = cv.getRotationMatrix2D((77, 52), 29, 1)
    label = cv.warpAffine(label, tilt, (155, 105), borderValue=255)
    image[220:325, 300:455] = cv.cvtColor(label, cv.COLOR_GRAY2BGR)
    roof = np.array([(780, 595), (880, 455), (980, 595)], dtype=np.int32)
    cv.polylines(image, [roof], True, (40, 40, 40), 3)
    cv.putText(image, "h = 208", (965, 548), cv.FONT_HERSHEY_SIMPLEX, 0.75, (30, 30, 30), 2)
    return cv.imencode(".png", image)[1].tobytes()


class TestRecognition(unittest.TestCase):
    def test_rasters_detect_shapes_without_inventing_measurements(self):
        image = sample_floor()
        with patch("recognition.shutil.which", return_value=None), patch.dict(
            "os.environ", {"TESSDATA_PREFIX": ""}
        ):
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
        with patch("recognition.shutil.which", return_value=None), patch.dict(
            "os.environ", {"TESSDATA_PREFIX": ""}
        ):
            result = recognize(data, "test.pdf")
        self.assertEqual([floor["source_page"] for floor in result["floors"]], [1, 2])
        self.assertTrue(result["floors"][0]["rooms"])
        self.assertFalse(result["floors"][1]["rooms"])

    def test_elevation_is_kept_as_reference_not_room(self):
        image = np.full((900, 1200, 3), 255, np.uint8)
        roof = np.array([(220, 690), (600, 310), (980, 690)], dtype=np.int32)
        cv.polylines(image, [roof], True, (40, 40, 40), 3)
        data = cv.imencode(".png", image)[1].tobytes()
        with patch("recognition.shutil.which", return_value=None), patch.dict(
            "os.environ", {"TESSDATA_PREFIX": ""}
        ):
            result = recognize(data, "roof.png")
        self.assertFalse(result["floors"][0]["rooms"])
        self.assertEqual(result["project_meta"]["section_hints"][0]["type"], "SECTION_HINT")
        self.assertIsNone(result["project_meta"]["section_hints"][0]["height_cm"])

    def test_angle_correction_and_height_ocr_with_tesseract(self):
        if not _Tesseract().available:
            self.skipTest("Tesseract language data unavailable")
        result = recognize(angled_plan_with_section(), "tilted.png")
        self.assertEqual(len(result["floors"]), 1)
        self.assertEqual(len(result["floors"][0]["rooms"]), 1)
        room = result["floors"][0]["rooms"][0]
        self.assertEqual(room["raw_measurements_cm"]["top"], 405)
        self.assertLessEqual(abs(room["measurement_rotations_deg"]["top"] - 29), 8)
        self.assertLess(room["measurement_confidences"]["top"], 0.60)
        x1, y1, x2, y2 = room["raw_measurement_boxes_px"]["top"]
        self.assertGreater(x2, x1)
        self.assertGreater(y2, y1)
        hints = result["project_meta"]["section_hints"]
        self.assertEqual(len(hints), 1)
        self.assertEqual(hints[0]["type"], "SECTION_HINT")
        self.assertEqual(hints[0]["height_cm"], 208)

    def test_customer_heading_case_is_independent_of_name_case(self):
        image = np.full((700, 900, 3), 255, np.uint8)
        for title in ("Заказчик:", "ЗАКАЗЧИК:", "заказчик:"):
            with self.subTest(title=title):
                ocr = _Tesseract()
                ocr.available = True
                words = [
                    (text, 0.99, (offset * 100, 10, 96, 16))
                    for offset, text in enumerate((title, "Примеров", "Иван", "Петрович"))
                ]
                with patch.object(ocr, "words", return_value=words):
                    self.assertEqual(_ocr_header(image, 100, ocr)["customer"], "Примеров Иван Петрович")

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

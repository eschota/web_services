import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image, ImageDraw

from renderfin.image_quality import (
    RenderArtifactQualityError,
    archive_rejected_bundle,
    validate_tpose_bundle,
)


SIZE = (256, 256)


def _encode(image, image_format="PNG", **save_options):
    output = io.BytesIO()
    image.save(output, image_format, **save_options)
    return output.getvalue()


def _primary(color=(35, 80, 140)):
    image = Image.new("RGB", SIZE, color)
    draw = ImageDraw.Draw(image)
    draw.ellipse((72, 24, 184, 136), fill=(210, 170, 120))
    draw.rectangle((96, 120, 160, 240), fill=(80, 160, 90))
    return image


def _isolated(*, rectangle=(64, 32, 192, 224), mode="RGBA"):
    image = Image.new(mode, SIZE, (0, 0, 0, 0) if mode == "RGBA" else 0)
    if mode == "RGBA":
        draw = ImageDraw.Draw(image)
        draw.rectangle(rectangle, fill=(120, 180, 80, 255))
    return image


class RenderfinImageQualityTests(unittest.TestCase):
    def test_valid_bundle_returns_strict_json_safe_metrics(self):
        primary = _encode(_primary())
        isolated = _encode(_isolated())
        reference = _encode(Image.new("RGB", SIZE, (0, 0, 0)), "JPEG", quality=90)

        report = validate_tpose_bundle(primary, isolated, reference)

        self.assertTrue(report["passed"])
        self.assertEqual(report["schema"], "renderfin.tpose_bundle_quality.v1")
        self.assertGreater(
            report["isolated"]["alpha_foreground_occupancy"], 0.02
        )
        self.assertLess(
            report["isolated"]["alpha_foreground_occupancy"], 0.90
        )
        self.assertFalse(
            report["control_mask_comparison"]["is_control_mask_echo"]
        )
        json.dumps(report, allow_nan=False)

    def test_control_mask_echo_is_detected_across_jpeg_and_png_encodings(self):
        control = _primary((12, 12, 12))
        jpeg = _encode(control, "JPEG", quality=96, subsampling=0)
        # Re-encode the decoded JPEG pixels as PNG: payload and format differ,
        # while the visual control-pose content is exactly the same.
        with Image.open(io.BytesIO(jpeg)) as decoded:
            primary = _encode(decoded.convert("RGB"), "PNG")
        isolated = _encode(_isolated())

        with self.assertRaises(RenderArtifactQualityError) as raised:
            validate_tpose_bundle(primary, isolated, jpeg)

        error = raised.exception
        self.assertEqual(error.machine_code, "primary_matches_control_mask")
        comparison = error.report["control_mask_comparison"]
        self.assertLessEqual(comparison["rgb_mae_normalized"], 2.0 / 255.0)
        self.assertGreaterEqual(comparison["pixels_within_5_fraction"], 0.995)
        json.dumps(error.to_dict(), allow_nan=False)

    def test_undecodable_and_too_small_images_fail_closed(self):
        isolated = _encode(_isolated())
        with self.assertRaises(RenderArtifactQualityError) as undecodable:
            validate_tpose_bundle(b"not an image", isolated)
        self.assertEqual(undecodable.exception.machine_code, "primary_decode_failed")

        tiny = _encode(Image.new("RGB", (64, 64), "blue"))
        with self.assertRaises(RenderArtifactQualityError) as too_small:
            validate_tpose_bundle(tiny, isolated)
        self.assertEqual(
            too_small.exception.machine_code, "primary_dimensions_too_small"
        )

    def test_isolated_requires_real_rgba_alpha(self):
        primary = _encode(_primary())
        rgb_isolated = _encode(Image.new("RGB", SIZE, "white"))

        with self.assertRaises(RenderArtifactQualityError) as raised:
            validate_tpose_bundle(primary, rgb_isolated)

        self.assertEqual(raised.exception.machine_code, "isolated_rgba_required")

    def test_isolated_alpha_occupancy_bounds_are_enforced(self):
        primary = _encode(_primary())
        empty = _encode(Image.new("RGBA", SIZE, (0, 0, 0, 0)))
        full = _encode(Image.new("RGBA", SIZE, (1, 2, 3, 255)))

        with self.assertRaises(RenderArtifactQualityError) as too_low:
            validate_tpose_bundle(primary, empty)
        self.assertEqual(
            too_low.exception.machine_code,
            "isolated_foreground_occupancy_too_low",
        )

        with self.assertRaises(RenderArtifactQualityError) as too_high:
            validate_tpose_bundle(primary, full)
        self.assertEqual(
            too_high.exception.machine_code,
            "isolated_foreground_occupancy_too_high",
        )

    def test_isolated_dimensions_must_match_primary(self):
        primary = _encode(_primary())
        isolated = _encode(
            Image.new("RGBA", (320, 256), (0, 0, 0, 0))
        )

        with self.assertRaises(RenderArtifactQualityError) as raised:
            validate_tpose_bundle(primary, isolated)

        self.assertEqual(
            raised.exception.machine_code, "isolated_dimensions_mismatch"
        )

    def test_archive_preserves_exact_bytes_report_and_is_collision_safe(self):
        primary = b"exact-primary\x00\xff"
        isolated = b"exact-isolated\x01\xfe"
        reference = b"exact-reference\x02\xfd"
        report = {
            "schema": "renderfin.tpose_bundle_quality.v1",
            "passed": False,
            "failure": {"machine_code": "test_rejection"},
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = archive_rejected_bundle(
                root,
                primary_bytes=primary,
                isolated_bytes=isolated,
                reference_bytes=reference,
                report=report,
                label="task/id unsafe",
            )
            second = archive_rejected_bundle(
                root,
                primary_bytes=primary,
                isolated_bytes=isolated,
                reference_bytes=reference,
                report=report,
                label="task/id unsafe",
            )

            self.assertNotEqual(first, second)
            self.assertEqual((first / "primary.bin").read_bytes(), primary)
            self.assertEqual((first / "isolated.bin").read_bytes(), isolated)
            self.assertEqual((first / "reference.bin").read_bytes(), reference)
            self.assertEqual(
                json.loads((first / "report.json").read_text(encoding="utf-8")),
                report,
            )
            manifest = json.loads(
                (first / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                manifest["files"]["primary.bin"]["sha256"],
                hashlib.sha256(primary).hexdigest(),
            )
            self.assertFalse(any(path.name.endswith(".part") for path in first.iterdir()))


if __name__ == "__main__":
    unittest.main()

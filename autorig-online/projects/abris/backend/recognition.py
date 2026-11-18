"""Raster import and conservative proposals for the Abris review editor."""

import base64
import csv
import io
import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import cv2 as cv
import numpy as np
from PIL import Image, ImageOps

MAX_FILE_BYTES = 24 * 1024 * 1024
MAX_PAGES = 12
MAX_RASTER_PIXELS = 40_000_000
LONG_EDGE = 1536
SIDES = ("top", "bottom", "left", "right")


class RecognitionError(ValueError):
    """An unreadable document or one outside the supported processing limits."""


@dataclass(frozen=True)
class Token:
    text: str
    box: tuple
    confidence: float
    rotation_deg: int

    @property
    def vertical(self):
        return self.rotation_deg != 0


def _pages(data):
    if data.startswith(b"%PDF-"):
        try:
            import pypdfium2 as pdfium

            pdf = pdfium.PdfDocument(data)
            try:
                if not 1 <= len(pdf) <= MAX_PAGES:
                    raise RecognitionError(f"Допустимо от 1 до {MAX_PAGES} страниц.")
                for page_number in range(len(pdf)):
                    page = pdf[page_number]
                    try:
                        width, height = page.get_size()
                        if min(width, height) <= 0 or not math.isfinite(width + height):
                            raise RecognitionError("Некорректный размер страницы PDF.")
                        bitmap = page.render(
                            scale=LONG_EDGE / max(width, height), fill_color=(255, 255, 255, 255)
                        )
                        try:
                            yield cv.cvtColor(np.asarray(bitmap.to_pil().convert("RGB")), cv.COLOR_RGB2BGR)
                        finally:
                            bitmap.close()
                    finally:
                        page.close()
            finally:
                pdf.close()
        except (RuntimeError, ValueError, OSError) as error:
            if isinstance(error, RecognitionError):
                raise
            raise RecognitionError("PDF не читается или защищён паролем.") from error
        return

    try:
        with Image.open(io.BytesIO(data)) as source:
            if source.format not in ("JPEG", "PNG", "TIFF"):
                raise RecognitionError("Ожидается файл JPG, PNG, TIFF или PDF.")
            frame_count = source.n_frames if source.format == "TIFF" else 1
            if frame_count > MAX_PAGES:
                raise RecognitionError(f"Допустимо до {MAX_PAGES} страниц.")
            for frame_number in range(frame_count):
                source.seek(frame_number)
                width, height = source.size
                if width * height > MAX_RASTER_PIXELS:
                    raise RecognitionError("Разрешение скана превышает 40 мегапикселей.")
                if min(width, height) < 64 or max(width, height) / min(width, height) > 32:
                    raise RecognitionError("Некорректные пропорции изображения.")
                frame = ImageOps.exif_transpose(source)
                frame.thumbnail((LONG_EDGE, LONG_EDGE), Image.Resampling.LANCZOS)
                if "A" in frame.getbands() or "transparency" in frame.info:
                    rgba = frame.convert("RGBA")
                    frame = Image.alpha_composite(Image.new("RGBA", rgba.size, "white"), rgba)
                yield cv.cvtColor(np.asarray(frame.convert("RGB")), cv.COLOR_RGB2BGR)
    except (OSError, ValueError, Image.DecompressionBombError) as error:
        if isinstance(error, RecognitionError):
            raise
        raise RecognitionError("Изображение не читается или повреждено.") from error


def _rectify(image):
    height, width = image.shape[:2]
    small_scale = min(1, 900 / max(width, height))
    small = cv.resize(image, None, fx=small_scale, fy=small_scale, interpolation=cv.INTER_AREA)
    gray = cv.cvtColor(small, cv.COLOR_BGR2GRAY)
    blurred = cv.GaussianBlur(gray, (5, 5), 0)
    edges = cv.Canny(blurred, 40, 120)
    edges = cv.morphologyEx(edges, cv.MORPH_CLOSE, np.ones((5, 5), np.uint8))
    contours, _ = cv.findContours(edges, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)
    for contour in sorted(contours, key=cv.contourArea, reverse=True)[:8]:
        area_ratio = cv.contourArea(contour) / gray.size
        if area_ratio < 0.40:
            break
        if area_ratio > 0.97:
            continue
        corners = cv.approxPolyDP(contour, 0.025 * cv.arcLength(contour, True), True)
        if len(corners) != 4 or not cv.isContourConvex(corners):
            continue
        coords = corners[:, 0].astype(np.float32) / small_scale
        sums, diffs = coords.sum(axis=1), np.diff(coords, axis=1).ravel()
        ordered = coords[[np.argmin(sums), np.argmin(diffs), np.argmax(sums), np.argmax(diffs)]]
        if len(np.unique(ordered, axis=0)) != 4:
            continue
        tl, tr, br, bl = ordered
        dw = int(max(np.linalg.norm(tr - tl), np.linalg.norm(br - bl)))
        dh = int(max(np.linalg.norm(bl - tl), np.linalg.norm(br - tr)))
        if min(dw, dh) < LONG_EDGE / 8:
            continue
        inner = np.zeros_like(gray)
        cv.fillConvexPoly(inner, (ordered * small_scale).astype(np.int32), 255)
        surround = cv.dilate(inner, np.ones((21, 21), np.uint8)) & (255 - inner)
        if cv.countNonZero(surround) < gray.size * 0.01:
            continue
        paper, edge_background = cv.mean(gray, inner)[0], cv.mean(gray, surround)[0]
        if paper < 160 or paper - edge_background < 21:
            continue
        dest = np.float32([[0, 0], [dw - 1, 0], [dw - 1, dh - 1], [0, dh - 1]])
        return cv.warpPerspective(image, cv.getPerspectiveTransform(ordered, dest), (dw, dh))
    return image


def _white_balance(image):
    # The lightest paper pixels supply a neutral reference, independent of ink hue.
    gray = cv.cvtColor(image, cv.COLOR_BGR2GRAY)
    sample = image[::6, ::6]
    paper = sample[gray[::6, ::6] >= np.percentile(gray[::6, ::6], 86)]
    if paper.size == 0:
        return image
    gains = np.clip(249 / np.maximum(170, np.median(paper, axis=0)), 0.9, 1.35)
    return np.clip(image * gains, 0, 255).astype(np.uint8)


def _ink_mask(image):
    gray = cv.cvtColor(image, cv.COLOR_BGR2GRAY)
    kernel = max(13, round(max(gray.shape) / 60) | 1)
    paper = cv.morphologyEx(gray, cv.MORPH_CLOSE, np.ones((kernel, kernel), np.uint8))
    # Local luminance rejects pale grid lines and illumination gradients.
    return ((cv.subtract(paper, gray) > 27) & (gray < 220)).astype(np.uint8) * 255


def _deskew(image, ink):
    height, width = ink.shape
    minimum = round(max(height, width) * 0.13)
    lines = cv.HoughLinesP(ink, 1, np.pi / 180, 75, minLineLength=minimum, maxLineGap=18)
    if lines is None:
        return image
    angles = []
    for x1, y1, x2, y2 in lines.reshape(-1, 4):
        angle = math.degrees(math.atan2(int(y2) - int(y1), int(x2) - int(x1)))
        angle = (angle + 45) % 90 - 45
        if abs(angle) <= 10:
            angles.append(angle)
    if len(angles) < 4:
        return image
    angle = float(np.median(angles))
    if abs(angle) <= 0.65:
        return image
    rotation = cv.getRotationMatrix2D((width / 2, height / 2), angle, 1)
    return cv.warpAffine(image, rotation, (width, height), borderValue=(255, 255, 255))


def _wall_mask(ink, fraction):
    size = max(ink.shape)
    walls = np.zeros_like(ink)
    for vertical in (False, True):
        oriented = ink.T.copy() if vertical else ink
        # Tolerate hand wobble and narrow breaks, then reject short marks.
        soft = cv.dilate(oriented, np.ones((max(3, round(size * 0.003)), 1), np.uint8))
        joined = cv.morphologyEx(soft, cv.MORPH_CLOSE, np.ones((1, max(3, round(size * 0.008))), np.uint8))
        strong = cv.morphologyEx(joined, cv.MORPH_OPEN, np.ones((1, max(15, round(size * fraction))), np.uint8))
        walls |= strong.T if vertical else strong
    return walls


def _rect_overlap(a, b):
    dx = max(0, min(a[2], b[2]) - max(a[0], b[0]))
    dy = max(0, min(a[3], b[3]) - max(a[1], b[1]))
    return dx * dy


def _find_rooms(ink):
    height, width = ink.shape
    long_edge = max(height, width)
    perimeters = _wall_mask(ink, 0.035)
    proposals = []
    for close_factor, minimum_line in ((0.007, 0.035), (0.016, 0.035), (0.024, 0.045), (0.046, 0.045)):
        if close_factor > 0.025 and proposals:
            break
        walls = perimeters if minimum_line == 0.035 else _wall_mask(ink, minimum_line)
        kernel_size = max(5, round(long_edge * close_factor) | 1)
        closed = cv.morphologyEx(walls, cv.MORPH_CLOSE, np.ones((kernel_size, kernel_size), np.uint8))
        _, labels, stats, _ = cv.connectedComponentsWithStats(255 - closed, connectivity=4)
        for label, (x, y, w, h, area) in enumerate(stats[1:].tolist(), 1):
            if x < 2 or y < 2 or x + w > width - 2 or y + h > height - 2:
                continue
            if w < long_edge * 0.040 or h < long_edge * 0.040 or area < width * height * 0.0018:
                continue
            if max(w / h, h / w) > 9 or area / (w * h) < 0.48:
                continue
            bounds = (x, y, x + w, y + h)
            if any(_rect_overlap(bounds, room["box"]) > 0.22 * min(w * h, room["area"]) for room in proposals):
                continue
            shape = (labels[y : y + h, x : x + w] == label).astype(np.uint8)
            contours, _ = cv.findContours(shape, cv.RETR_EXTERNAL, cv.CHAIN_APPROX_SIMPLE)
            contour = max(contours, key=cv.contourArea)
            approx = cv.approxPolyDP(contour, 0.019 * cv.arcLength(contour, True), True)[:, 0]
            # Triangular elevation details have no four reasonably orthogonal corners.
            if len(approx) < 4:
                continue
            proposals.append(
                {
                    "box": bounds,
                    "area": w * h,
                    "confidence": round(
                        min(0.82, (0.65 if close_factor == 0.007 else 0.45) + 0.18 * area / (w * h)), 2
                    ),
                }
            )
            if len(proposals) >= 80:
                return proposals, perimeters
    return proposals, perimeters


def _group_rooms(rooms, size):
    groups = []
    remaining = list(rooms)
    while remaining:
        group = [remaining.pop(0)]
        for room in group:
            a = room["box"]
            adjoined = []
            for other in remaining:
                b = other["box"]
                dx = min(a[2], b[2]) - max(a[0], b[0])
                dy = min(a[3], b[3]) - max(a[1], b[1])
                shared_x = dx >= min(a[2] - a[0], b[2] - b[0]) * 0.28
                shared_y = dy >= min(a[3] - a[1], b[3] - b[1]) * 0.28
                if (shared_x and dy >= -size * 0.043) or (shared_y and dx >= -size * 0.043):
                    adjoined.append(other)
            group.extend(adjoined)
            remaining = [other for other in remaining if other not in adjoined]
        groups.append(group)
    return sorted(
        groups,
        key=lambda group: (
            min(group_item["box"][1] for group_item in group) // max(1, int(size * 0.09)),
            min(group_item["box"][0] for group_item in group),
        ),
    )


class _Tesseract:
    def __init__(self):
        self.binary = shutil.which("tesseract")
        self.module = None
        self.data = os.environ.get("TESSDATA_PREFIX")
        if not self.binary and self.data:
            try:
                import tesserocr

                self.module = tesserocr
            except ImportError:
                pass
        self.available = bool(self.binary or self.module)
        self.warned = False
        self.language = self._language() if self.available else "eng"

    def _language(self):
        if self.data and (Path(self.data) / "rus.traineddata").is_file():
            return "rus+eng"
        if self.binary:
            try:
                result = subprocess.run(
                    [self.binary, "--list-langs"], capture_output=True, timeout=3, check=False
                )
                if "rus" in result.stdout.decode("utf-8", errors="replace").splitlines():
                    return "rus+eng"
            except (OSError, subprocess.TimeoutExpired):
                self.warned = True
        return "eng"

    def _cli(self, grayscale, psm, lang, numbers):
        encoded = cv.imencode(".png", grayscale)[1].tobytes()
        arguments = [self.binary, "stdin", "stdout", "-l", lang, "--psm", str(psm), "--dpi", "300"]
        if numbers:
            arguments.extend(("-c", "tessedit_char_whitelist=0123456789"))
        arguments.append("tsv")
        env = {**os.environ, "OMP_THREAD_LIMIT": "1"}
        result = subprocess.run(arguments, input=encoded, capture_output=True, timeout=5, check=False, env=env)
        if result.returncode:
            raise RuntimeError("OCR process failed")
        lines = csv.DictReader(io.StringIO(result.stdout.decode("utf-8", errors="replace")), delimiter="\t")
        for line in lines:
            text = line.get("text")
            if text and line["conf"] and float(line["conf"]) >= 0:
                yield text.strip(), float(line["conf"]) / 100, (
                    int(line["left"]),
                    int(line["top"]),
                    int(line["width"]),
                    int(line["height"]),
                )

    def _in_process(self, grayscale, psm, lang, numbers):
        tess = self.module
        with tess.PyTessBaseAPI(path=self.data, lang=lang, psm=psm) as api:
            if numbers:
                api.SetVariable("tessedit_char_whitelist", "0123456789")
            api.SetImage(Image.fromarray(grayscale))
            api.SetSourceResolution(300)
            api.Recognize()
            iterator = api.GetIterator()
            if iterator is None:
                return
            for word in tess.iterate_level(iterator, tess.RIL.WORD):
                box = word.BoundingBox(tess.RIL.WORD)
                if not box:
                    continue
                try:
                    text = word.GetUTF8Text(tess.RIL.WORD)
                except RuntimeError:
                    continue
                if text and text.strip():
                    x1, y1, x2, y2 = box
                    yield text.strip(), word.Confidence(tess.RIL.WORD) / 100, (x1, y1, x2 - x1, y2 - y1)

    def words(self, grayscale, psm, numbers=False):
        if not self.available or self.warned:
            return []
        lang = "eng" if numbers else self.language
        try:
            engine = self._cli if self.binary else self._in_process
            return list(engine(grayscale, psm, lang, numbers))
        except (ValueError, OSError, RuntimeError, subprocess.TimeoutExpired):
            self.warned = True
            return []


def _ocr_header(image, bottom, ocr):
    if not ocr.available or bottom <= image.shape[0] * 0.075:
        return {}
    gray = cv.cvtColor(image[:bottom], cv.COLOR_BGR2GRAY)
    scale = max(1, min(2, 2300 / max(gray.shape)))
    gray = cv.resize(gray, None, fx=scale, fy=scale, interpolation=cv.INTER_CUBIC)
    lines = []
    for token, confidence, (x, y, w, h) in ocr.words(gray, 6):
        center = y + h / 2
        nearest = next((line for line in lines if abs(line[0] - center) < max(3, h * 0.5)), None)
        if nearest:
            nearest[1].append((x, token))
        else:
            lines.append((center, [(x, token)]))
    content = "\n".join(" ".join(word for _, word in sorted(line)) for _, line in sorted(lines))
    cadastral = re.search(r"\b\d{2}\s*:\s*\d{2}\s*:\s*\d{6,7}\s*:\s*\d+\b", content)
    contract = re.search(
        r"(?:договор|договорa)\s*(?:№|N[%o°]?)?\s*([А-ЯA-ZЁ0-9][А-ЯA-ZЁ0-9/-]{3,30})", content, re.I
    )
    address = re.search(r"адрес\s*:\s*([^\n]+)", content, re.I)
    customer = re.search(r"заказчик\s*:\s*((?:[А-ЯЁ][а-яё-]+\s+){1,2}[А-ЯЁ][а-яё-]+)", content)
    result = {}
    if cadastral:
        result["cadastral_number"] = re.sub(r"\s+", "", cadastral.group())
    if contract:
        result["contract_number"] = contract.group(1)
    if address:
        result["address"] = address.group(1).strip()[:180]
    if customer:
        result["customer"] = customer.group(1).strip()
    return result


def _rotate_box(box, height, width, rotation):
    x1, y1, x2, y2 = box
    if rotation == 1:
        return width - y2, x1, width - y1, x2
    if rotation == -1:
        return y1, height - x2, y2, height - x1
    return box


def _htr():
    if not os.environ.get("ABRIS_HTR_MODEL"):
        return None, None
    try:
        from train_handwriting import load_model, predict

        return load_model(), predict
    except (ImportError, OSError, ValueError):
        return None, None


def _ocr_tokens(image, ink, walls, ocr):
    if not ocr.available or ocr.warned:
        return []
    gray = cv.cvtColor(image, cv.COLOR_BGR2GRAY)
    background = cv.morphologyEx(gray, cv.MORPH_CLOSE, np.ones((23, 23), np.uint8))
    normalized = cv.divide(gray, background, scale=255)
    normalized[walls > 0] = 255
    cleaned = ink & (255 - walls)
    cleaned = cv.morphologyEx(cleaned, cv.MORPH_CLOSE, np.ones((2, 2), np.uint8))
    height, width = ink.shape
    size = max(height, width)
    tokens = []
    model, predict = _htr()
    scale = min(2, 2450 / size)
    for rotation in (0, 1, -1):
        for source in (normalized, 255 - cleaned):
            oriented = np.rot90(source, rotation)
            resized = cv.resize(oriented, None, fx=scale, fy=scale, interpolation=cv.INTER_CUBIC)
            for text, confidence, (x, y, w, h) in ocr.words(resized, 11, numbers=True):
                if confidence < 0.19 or not re.fullmatch(r"\d{2,4}", text):
                    continue
                x1, y1, x2, y2 = (round(v / scale) for v in (x, y, x + w, y + h))
                if not 0.004 * size <= y2 - y1 <= 0.06 * size or not 0.86 <= (x2 - x1) / max(1, y2 - y1) <= 5:
                    continue
                if model is not None:
                    suggestion = predict(model, oriented[y1:y2, x1:x2])
                    if suggestion and suggestion[0] == text:
                        confidence = min(confidence + 0.08, 0.94)
                if not 8 <= int(text) <= 2500:
                    continue
                box = _rotate_box((x1, y1, x2, y2), height, width, rotation)
                token = Token(text, box, min(0.79, round(confidence, 2)), -90 * rotation)
                overlaps = [
                    i
                    for i, existing in enumerate(tokens)
                    if existing.vertical == token.vertical
                    and _rect_overlap(existing.box, box) > 0.54 * (x2 - x1) * (y2 - y1)
                ]
                if not overlaps:
                    tokens.append(token)
                elif tokens[overlaps[0]].confidence < token.confidence:
                    tokens[overlaps[0]] = token
    return tokens


def _semantic_labels(image, walls, ocr):
    if not ocr.available or ocr.warned or "rus" not in ocr.language:
        return []
    gray = cv.cvtColor(image, cv.COLOR_BGR2GRAY)
    gray[walls > 0] = 255
    tokens = []
    for rotation in (0, 1, -1):
        rotated = np.rot90(gray, rotation)
        resized = cv.resize(rotated, None, fx=1.3, fy=1.3, interpolation=cv.INTER_CUBIC)
        for text, confidence, (x, y, w, h) in ocr.words(resized, 11):
            if confidence < 0.18 or not re.fullmatch(r"створ[а-яё.]*|ст\.|остекл[а-яё.]*", text, re.I):
                continue
            box = _rotate_box(tuple(round(p / 1.3) for p in (x, y, x + w, y + h)), *gray.shape, rotation)
            tokens.append(Token(text, box, min(0.79, round(confidence, 2)), -90 * rotation))
    return tokens


def _measurements(room, tokens, openings):
    x1, y1, x2, y2 = room["box"]
    width, height = x2 - x1, y2 - y1
    measurements = dict.fromkeys(SIDES)
    boxes = {}
    confidences = {}
    rotations = {}
    available = [
        token
        for token in tokens
        if not any(
            _rect_overlap(token.box, opening["label_box"]) > 0
            for opening in openings
            if opening.get("label_box")
        )
    ]
    for side in SIDES:
        vertical = side in ("left", "right")
        candidates = []
        for token in available:
            if vertical != token.vertical or int(token.text) < 100:
                continue
            px = (token.box[0] + token.box[2]) / 2
            py = (token.box[1] + token.box[3]) / 2
            u, v = (px - x1) / width, (py - y1) / height
            if not (0.035 < u < 0.965 and 0.047 < v < 0.953):
                continue
            perpendicular = u if vertical else v
            along = v if vertical else u
            if side in ("bottom", "right"):
                perpendicular = 1 - perpendicular
            if perpendicular >= 0.49:
                continue
            score = token.confidence - 0.46 * abs(perpendicular - 0.19) - 0.29 * abs(along - 0.5)
            candidates.append((score, token))
        if candidates:
            _, selected = max(candidates, key=lambda pair: pair[0])
            measurements[side] = int(selected.text)
            boxes[side] = selected.box
            confidences[side] = selected.confidence
            rotations[side] = selected.rotation_deg
    return measurements, boxes, confidences, rotations


def _window_marks(ink):
    height, width = ink.shape
    size = max(height, width)
    joined = cv.morphologyEx(ink, cv.MORPH_CLOSE, np.ones((3, 3), np.uint8))
    contours, _ = cv.findContours(joined, cv.RETR_LIST, cv.CHAIN_APPROX_SIMPLE)
    marks = []
    for contour in contours:
        x, y, w, h = cv.boundingRect(contour)
        short, long = sorted((w, h))
        if not 0.016 * size <= long <= 0.074 * size or not 3 <= short <= 0.017 * size or long / short < 3:
            continue
        if cv.contourArea(contour) / (w * h) < 0.45:
            continue
        shape = cv.approxPolyDP(contour, 0.019 * cv.arcLength(contour, True), True)
        if not 4 <= len(shape) <= 7:
            continue
        marks.append((x, y, x + w, y + h))
    return marks


def _center(box):
    return (box[0] + box[2]) / 2, (box[1] + box[3]) / 2


def _runs(values):
    boundaries = np.flatnonzero(np.diff(np.r_[False, values, False])).tolist()
    return zip(boundaries[::2], boundaries[1::2])


def _swing_leaf(section, span):
    crop = section[: span + 1]
    if min(crop.shape) < 9 or cv.countNonZero(crop) < span * 0.8:
        return False
    edge = max(3, round(span * 0.15))
    for patch in (crop, crop[:, ::-1]):
        near_hinge = patch[:, :edge]
        active_rows = np.any(near_hinge > 0, axis=1)
        if np.count_nonzero(active_rows) < span * 0.42:
            continue
        # A perpendicular leaf and a curved or diagonal swing inside the opening.
        deeper = patch[edge:span, edge : span - edge]
        if np.count_nonzero(np.any(deeper, axis=1)) >= span * 0.22:
            return True
    # A leaf is sometimes drawn diagonally from the hinge instead of a quarter arc.
    segments = cv.HoughLinesP(
        crop, 1, np.pi / 90, max(7, span // 5), minLineLength=max(8, span // 2), maxLineGap=4
    )
    if segments is None:
        return False
    for x1, y1, x2, y2 in segments.reshape(-1, 4):
        if abs(int(y2) - int(y1)) < span * 0.33 or abs(int(x2) - int(x1)) < span * 0.24:
            continue
        near = min(((int(x1), int(y1)), (int(x2), int(y2))), key=lambda p: p[1])
        if near[1] <= edge and min(near[0], crop.shape[1] - near[0]) <= edge:
            return True
    return False


def _door_marks(room, ink, walls):
    x1, y1, x2, y2 = room["box"]
    size = max(ink.shape)
    strip = max(3, round(size * 0.004))
    minimum = max(14, round(size * 0.012))
    doors = []
    sides = [
        ("top", walls, ink, y1, x1, x2, 1),
        ("bottom", walls, ink, y2, x1, x2, -1),
        ("left", walls.T, ink.T, x1, y1, y2, 1),
        ("right", walls.T, ink.T, x2, y1, y2, -1),
    ]
    for side, barriers, ink_grid, edge, start, end, sign in sides:
        if edge < strip or edge + strip >= barriers.shape[0]:
            continue
        occupied = np.any(barriers[edge - strip : edge + strip + 1, start:end], axis=0)
        if end - start < minimum * 3:
            continue
        for a, b in _runs(~occupied):
            span = b - a
            if span < minimum or span > min((end - start) * 0.45, size * 0.065):
                continue
            if a < minimum * 0.6 or b > end - start - minimum * 0.6:
                continue
            along_a, along_b = start + a, start + b
            margin = max(3, span // 8)
            from_x, to_x = along_a - margin, along_b + margin
            interior = np.arange(edge + 2 * sign, edge + (span + 2) * sign, sign)
            interior = interior[(interior >= 0) & (interior < ink_grid.shape[0])]
            region = ink_grid[interior, from_x:to_x]
            if len(interior) < span * 0.8 or not _swing_leaf(region, span):
                continue
            across_a, across_b = max(0, edge - strip), min(ink_grid.shape[0], edge + strip + 1)
            box = (
                (across_a, along_a, across_b, along_b)
                if side in ("left", "right")
                else (along_a, across_a, along_b, across_b)
            )
            doors.append((side, box))
    return doors


def _stairs_marks(ink):
    size = max(ink.shape)
    longest = round(size * 0.12)
    shortest = max(12, round(size * 0.015))
    marks = []
    for vertical in (False, True):
        image = ink.T.copy() if vertical else ink
        strokes = cv.morphologyEx(image, cv.MORPH_OPEN, np.ones((1, shortest), np.uint8))
        _, _, stats, _ = cv.connectedComponentsWithStats(strokes, connectivity=8)
        selected = [
            (x, y + w_h / 2, x + w)
            for x, y, w, w_h, _ in stats[1:].tolist()
            if shortest <= w <= longest and w_h < max(7, w * 0.16)
        ]
        selected.sort(key=lambda line: line[1])
        for i, line in enumerate(selected):
            left, row, right = line
            length = right - left
            nearby = [
                candidate
                for candidate in selected[i + 1 :]
                if 0 < candidate[1] - row < min(length * 1.8, size * 0.083)
                and abs(candidate[2] - candidate[0] - length) < length * 0.28
                and min(right, candidate[2]) - max(left, candidate[0]) >= length * 0.72
            ]
            if len(nearby) < 3:
                continue
            rows = sorted({round(r) for _, r, _ in [line, *nearby]})
            gaps = np.diff(rows)
            if len(gaps) < 3:
                continue
            width = float(np.median(gaps))
            if not max(4, size * 0.003) <= width <= size * 0.023:
                continue
            if np.count_nonzero(np.abs(gaps - width) <= max(2, width * 0.28)) < max(
                3, math.ceil(len(gaps) * 0.7)
            ):
                continue
            a = (left, rows[0], right, rows[-1])
            box = (a[1], a[0], a[3], a[2]) if vertical else a
            if any(_rect_overlap(box, old) > (box[2] - box[0]) * (box[3] - box[1]) * 0.5 for old in marks):
                continue
            marks.append(box)
    return marks


def _add_symbols(room, stair_marks):
    x1, y1, x2, y2 = room["box"]
    result = []
    for box in stair_marks:
        width, height = box[2] - box[0], box[3] - box[1]
        cx, cy = _center(box)
        if not (x1 < cx < x2 and y1 < cy < y2):
            continue
        if width > (x2 - x1) * 0.8 or height > (y2 - y1) * 0.8:
            continue
        result.append({"type": "STAIRS", "bbox_px": list(box), "confidence": 0.62})
    return result[:4]


def _openings(room, window_marks, door_marks, tokens, labels, size):
    x1, y1, x2, y2 = room["box"]
    width, height = x2 - x1, y2 - y1
    found = []
    for mark in window_marks:
        mx, my = _center(mark)
        vertical = mark[3] - mark[1] > mark[2] - mark[0]
        if vertical:
            if not y1 + height * 0.065 < my < y2 - height * 0.065:
                continue
            side, edge = min((("left", x1), ("right", x2)), key=lambda pair: abs(mx - pair[1]))
            inside = mx - edge if side == "left" else edge - mx
            distance, t = abs(mx - edge), (my - y1) / height
        else:
            if not x1 + width * 0.065 < mx < x2 - width * 0.065:
                continue
            side, edge = min((("top", y1), ("bottom", y2)), key=lambda pair: abs(my - pair[1]))
            inside = my - edge if side == "top" else edge - my
            distance, t = abs(my - edge), (mx - x1) / width
        if (
            inside > size * 0.006
            or distance > size * 0.019
            or any(other["wall"] == side and abs(other["t"] - t) < 0.075 for other in found)
        ):
            continue
        if (vertical and mark[3] - mark[1] > height * 0.65) or (
            not vertical and mark[2] - mark[0] > width * 0.65
        ):
            continue
        best = None
        best_score = size * 0.031
        for token in tokens:
            if token.vertical != vertical or not 45 <= int(token.text) <= 270:
                continue
            tx, ty = (token.box[0] + token.box[2]) / 2, (token.box[1] + token.box[3]) / 2
            along, across = (abs(ty - my), abs(tx - mx)) if vertical else (abs(tx - mx), abs(ty - my))
            score = 2 * along + across - 8 * token.confidence
            if score < best_score:
                best, best_score = token, score
        glazed = any(
            math.hypot(mx - cx, my - cy) < size * 0.037 for cx, cy in (_center(label.box) for label in labels)
        )
        found.append(
            {
                "wall": side,
                "type": "DOOR_GLAZED" if glazed else "WINDOW_STD",
                "width_cm": int(best.text) if best else None,
                "offset_cm": None,
                "t": round(t, 4),
                "confidence": min(0.62, best.confidence) if best else 0.42,
                "label_box": best.box if best else None,
                "box": mark,
            }
        )
    for side, mark in door_marks:
        mx, my = _center(mark)
        vertical = side in ("left", "right")
        along = my if vertical else mx
        start, end = (y1, y2) if vertical else (x1, x2)
        t = (along - start) / (end - start)
        if any(w["wall"] == side and abs(w["t"] - t) < 0.09 for w in found):
            # Ink fully bridging a gap in multiple parallel strokes is stronger window evidence.
            continue
        best, best_dist = None, size * 0.049
        for token in tokens:
            if token.vertical != vertical or not 45 <= int(token.text) <= 240:
                continue
            cx, cy = _center(token.box)
            distance = math.hypot(mx - cx, my - cy)
            if distance < best_dist:
                best, best_dist = token, distance
        glazed = any(
            math.hypot(mx - cx, my - cy) < size * 0.051 for cx, cy in (_center(label.box) for label in labels)
        )
        found.append(
            {
                "wall": side,
                "type": "DOOR_GLAZED" if glazed else "DOOR_SINGLE",
                "width_cm": int(best.text) if best else None,
                "offset_cm": None,
                "t": round(t, 4),
                "confidence": min(0.67, best.confidence) if best else 0.55,
                "label_box": best.box if best else None,
                "box": mark,
            }
        )
    return found[:20]


def _opening_offsets(room, openings, tokens, measurements, used_boxes, size):
    """Attach an offset only where a spare value fits both the object and its wall."""
    x1, y1, x2, y2 = room["box"]
    for opening in openings:
        width = opening["width_cm"]
        if width is None:
            continue
        wall = opening["wall"]
        vertical = wall in ("left", "right")
        opposite = {"top": "bottom", "bottom": "top", "left": "right", "right": "left"}[wall]
        length = measurements[wall] or measurements[opposite]
        if length is None or length <= width:
            continue
        box = opening["box"]
        mx, my = _center(box)
        near_axis = mx if vertical else my
        along_start, along_end = (y1, y2) if vertical else (x1, x2)
        left_edge, right_edge = (box[1], box[3]) if vertical else (box[0], box[2])
        candidates = []
        for token in tokens:
            value = int(token.text)
            if token.vertical != vertical or not 8 <= value <= length - width:
                continue
            if token.confidence < 0.45 or any(_rect_overlap(token.box, used) > 0 for used in used_boxes):
                continue
            cx, cy = _center(token.box)
            axis, along = (cx, cy) if vertical else (cy, cx)
            if abs(axis - near_axis) > size * 0.021:
                continue
            if along_start < along < left_edge - size * 0.003:
                field, approximate = "offset_cm", length * (left_edge - along_start) / max(
                    1, along_end - along_start
                )
            elif right_edge + size * 0.003 < along < along_end:
                field, approximate = "offset_right_cm", length * (along_end - right_edge) / max(
                    1, along_end - along_start
                )
            else:
                continue
            if abs(approximate - value) <= max(20, length * 0.16):
                candidates.append((abs(approximate - value) / length - token.confidence * 0.1, field, token))
        if len(candidates) == 1:
            _, field, token = candidates[0]
            opening[field] = int(token.text)
            used_boxes.append(token.box)


def _corner_post(ink, cx, cy, side_h, side_v, size):
    pad = round(size * 0.029)
    minimum = max(8, round(size * 0.009))
    patch_x = cx if side_h == "left" else cx - pad
    patch_y = cy if side_v == "top" else cy - pad
    if patch_x < 0 or patch_y < 0 or patch_x + pad > ink.shape[1] or patch_y + pad > ink.shape[0]:
        return None
    patch = ink[patch_y : patch_y + pad, patch_x : patch_x + pad] > 0
    if side_h == "left":
        patch = patch[:, ::-1]
    if side_v == "top":
        patch = patch[::-1]
    best, box = 0.76, None
    for top in range(3, pad - minimum):
        for left in range(3, pad - minimum):
            width, height = pad - left, pad - top
            if not 0.65 <= width / height <= 1.6:
                continue
            across = np.mean(np.any(patch[top - 2 : top + 3, left:pad], axis=0))
            vertical = np.mean(np.any(patch[top:pad, left - 2 : left + 3], axis=1))
            if min(across, vertical) <= best:
                continue
            best = min(across, vertical)
            other_x = cx + width if side_h == "left" else cx - width
            other_y = cy + height if side_v == "top" else cy - height
            box = [min(cx, other_x), min(cy, other_y), max(cx, other_x), max(cy, other_y)]
    return box


def _candidates(room, group, ink):
    """Recognize the combination of a thin open edge and a corner post."""
    x1, y1, x2, y2 = room["box"]
    size = max(ink.shape)
    if len(group) < 2:
        return False, []
    attached = []
    for other in group:
        if other is room:
            continue
        a, b, c, d = other["box"]
        if min(x2, c) - max(x1, a) > (x2 - x1) * 0.32:
            if -size * 0.008 <= y1 - d < size * 0.04:
                attached.append("top")
            if -size * 0.008 <= b - y2 < size * 0.04:
                attached.append("bottom")
        if min(y2, d) - max(y1, b) > (y2 - y1) * 0.32:
            if -size * 0.008 <= x1 - c < size * 0.04:
                attached.append("left")
            if -size * 0.008 <= a - x2 < size * 0.04:
                attached.append("right")
    attached = set(attached)
    if len(attached) < 2:
        return False, []
    for side_v, cy in (("top", y1), ("bottom", y2)):
        for side_h, cx in (("left", x1), ("right", x2)):
            if side_v in attached or side_h in attached:
                continue
            pad = max(10, round(size * 0.028))
            xa, ya, xb, yb = (
                max(0, cx - pad),
                max(0, cy - pad),
                min(ink.shape[1], cx + pad),
                min(ink.shape[0], cy + pad),
            )
            crop = ink[ya:yb, xa:xb]
            contours, _ = cv.findContours(crop, cv.RETR_LIST, cv.CHAIN_APPROX_SIMPLE)
            for contour in contours:
                x, y, w, h = cv.boundingRect(contour)
                if not 0.006 * size <= w <= 0.024 * size or not 0.006 * size <= h <= 0.024 * size:
                    continue
                if x == 0 or y == 0 or x + w == crop.shape[1] or y + h == crop.shape[0]:
                    continue
                if cv.contourArea(contour) / (w * h) < 0.67 or not 0.65 <= w / h <= 1.5:
                    continue
                if abs(xa + x + w / 2 - cx) < pad * 0.8 and abs(ya + y + h / 2 - cy) < pad * 0.8:
                    return True, [
                        {
                            "type": "COLUMN",
                            "bbox_px": [xa + x, ya + y, xa + x + w, ya + y + h],
                            "confidence": 0.54,
                        }
                    ]
            post = _corner_post(ink, cx, cy, side_h, side_v, size)
            if post:
                return True, [{"type": "COLUMN", "bbox_px": post, "confidence": 0.5}]
    return False, []


def _ocr_title(image, group, ocr):
    if not group or not ocr.available:
        return None
    size = max(image.shape[:2])
    x1 = min(room["box"][0] for room in group)
    y1 = min(room["box"][1] for room in group)
    x2 = max(room["box"][2] for room in group)
    start = max(0, y1 - round(size * 0.068))
    end = max(start, y1 - round(size * 0.014))
    if end - start < 18:
        return None
    patch = cv.cvtColor(image[start:end, x1:x2], cv.COLOR_BGR2GRAY)
    enlarged = cv.resize(patch, None, fx=1.7, fy=1.7, interpolation=cv.INTER_CUBIC)
    text = " ".join(token for token, confidence, _ in ocr.words(enlarged, 11) if confidence > 0.25)
    if re.search(r"\bманс", text, re.I):
        return "Этаж мансардный"
    floor = re.search(r"(?:эт(?:аж)?\.?\s*([1-9]\d?)\b|\b([1-9]\d?)\s*эт(?:аж)?)", text, re.I)
    if floor:
        return f"Этаж {floor.group(1) or floor.group(2)}"
    return None


def _box_local(box, x, y):
    return [box[0] - x, box[1] - y, box[2] - x, box[3] - y]


def _make_floor(
    image, ink, walls, group, tokens, labels, window_marks, stair_marks, ocr, floor_number, page_number
):
    height, width = ink.shape
    size = max(height, width)
    x1, y1, x2, y2 = 0, 0, width, height
    if group:
        pad = max(24, round(size * 0.054))
        x1 = max(0, min(room["box"][0] for room in group) - pad)
        y1 = max(0, min(room["box"][1] for room in group) - pad)
        x2 = min(width, max(room["box"][2] for room in group) + pad)
        y2 = min(height, max(room["box"][3] for room in group) + pad)
    selected = image[y1:y2, x1:x2]
    jpg = cv.imencode(".jpg", selected, [cv.IMWRITE_JPEG_QUALITY, 87])[1].tobytes()
    result = {
        "floor_id": f"floor_{floor_number}",
        "title": _ocr_title(image, group, ocr) or f"Этаж {floor_number}",
        "source_page": page_number,
        "source_bbox_px": [0, 0, x2 - x1, y2 - y1],
        "page_bbox_px": [x1, y1, x2, y2],
        "image_width": x2 - x1,
        "image_height": y2 - y1,
        "image": "data:image/jpeg;base64," + base64.b64encode(jpg).decode("ascii"),
        "rooms": [],
        "walls": [],
    }
    for index, room in enumerate(
        sorted(group, key=lambda item: (item["box"][1] // max(1, round(size * 0.033)), item["box"][0])), 1
    ):
        room_id = f"f{floor_number}_r{index}"
        candidates = _openings(room, window_marks, _door_marks(room, ink, walls), tokens, labels, size)
        measurements, boxes, confidences, rotations = _measurements(room, tokens, candidates)
        used_boxes = list(boxes.values()) + [item["label_box"] for item in candidates if item["label_box"]]
        _opening_offsets(room, candidates, tokens, measurements, used_boxes, size)
        openings = []
        for opening_number, opening in enumerate(candidates, 1):
            box = opening.pop("box")
            label = opening.pop("label_box")
            openings.append(
                {
                    "id": f"{room_id}_op{opening_number}",
                    **opening,
                    "bbox_px": _box_local(box, x1, y1),
                    "width_bbox_px": _box_local(label, x1, y1) if label else None,
                }
            )
        unheated, symbols = _candidates(room, group, ink)
        symbols.extend(_add_symbols(room, stair_marks))
        room_x1, room_y1, room_x2, room_y2 = room["box"]
        boxes = {side: _box_local(box, x1, y1) for side, box in boxes.items()}
        result["rooms"].append(
            {
                "id": room_id,
                "bbox_px": _box_local(room["box"], x1, y1),
                "raw_measurements_cm": measurements,
                "raw_measurement_boxes_px": boxes,
                "measurement_bboxes_px": boxes,
                "measurement_confidences": confidences,
                "measurement_rotations_deg": rotations,
                "confidence": room["confidence"],
                "is_heated": not unheated,
                "openings": openings,
                "symbols": [
                    {
                        "id": f"{room_id}_symbol{i}",
                        **symbol,
                        "bbox_px": _box_local(symbol["bbox_px"], x1, y1),
                        "u": round((_center(symbol["bbox_px"])[0] - room_x1) / (room_x2 - room_x1), 4),
                        "v": round((_center(symbol["bbox_px"])[1] - room_y1) / (room_y2 - room_y1), 4),
                        "width_cm": None,
                        "depth_cm": None,
                    }
                    for i, symbol in enumerate(symbols, 1)
                ],
            }
        )
    return result


def recognize(data: bytes, filename: str) -> dict:
    """Import a document and return editable, low-confidence raster-derived suggestions."""
    if not data or len(data) > MAX_FILE_BYTES:
        raise RecognitionError("Выберите файл размером до 24 МБ.")
    project_meta = {}
    floors = []
    ocr = _Tesseract()
    has_candidates = False
    for page_number, page in enumerate(_pages(data), 1):
        height, width = page.shape[:2]
        if min(width, height) < 32:
            raise RecognitionError("Страница слишком узкая для распознавания.")
        scale = LONG_EDGE / max(width, height)
        if scale > 1.025:
            page = cv.resize(page, None, fx=scale, fy=scale, interpolation=cv.INTER_CUBIC)
        page = _white_balance(_rectify(page))
        ink = _ink_mask(page)
        deskewed = _deskew(page, ink)
        if page is not deskewed:
            page, ink = deskewed, _ink_mask(deskewed)
        rooms, walls = _find_rooms(ink)
        if rooms:
            has_candidates = True
        if page_number == 1:
            first_y = min((room["box"][1] for room in rooms), default=ink.shape[0] // 4)
            project_meta = _ocr_header(page, first_y, ocr)
        text_walls = _wall_mask(ink, 0.075) if rooms else None
        tokens = _ocr_tokens(page, ink, text_walls, ocr) if rooms else []
        window_marks = _window_marks(ink) if rooms else []
        stair_marks = _stairs_marks(ink) if rooms else []
        labels = _semantic_labels(page, text_walls, ocr) if rooms else []
        groups = _group_rooms(rooms, max(ink.shape)) or [[]]
        for group in groups:
            if len(floors) >= 24:
                raise RecognitionError("Найдено слишком много фрагментов. Загрузите страницы отдельно.")
            floor_number = len(floors) + 1
            floor = _make_floor(
                page,
                ink,
                walls,
                group,
                tokens,
                labels,
                window_marks,
                stair_marks,
                ocr,
                floor_number,
                page_number,
            )
            if any(previous["title"] == floor["title"] for previous in floors):
                floor["title"] = f"Этаж {floor_number}"
            floors.append(floor)
    warnings = []
    if has_candidates:
        warnings.append("Проверьте контуры, проёмы и предложенные промеры по исходному скану.")
    else:
        warnings.append("Замкнутых помещений не найдено. Разметьте план по подложке вручную.")
    if not ocr.available or ocr.warned:
        warnings.append("Модуль чтения текста недоступен. Введите размеры вручную.")
    return {"project_meta": project_meta, "floors": floors, "warnings": warnings}

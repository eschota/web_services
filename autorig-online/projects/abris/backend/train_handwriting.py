"""Train and evaluate optional numeric handwriting recognition on local data.

Each JSON file contains version 1 and samples with image (a PNG data URL),
label (the verified digits) and group (the ID of the original drawing).
Use the same group for all crops from one drawing. Keep crops upright and tight.

    python train_handwriting.py ./corrections --output ./handwriting.npz

Set ABRIS_HTR_MODEL to the model's path in the recognition process to enable it.
"""

import argparse
import base64
import binascii
import hashlib
import io
import json
import math
import os
import re
import struct
import tempfile
from functools import lru_cache
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import cv2 as cv
import numpy as np


VERSION = 1
DIGITS = re.compile(r"[0-9]{1,5}\Z")
PNG_PREFIX = "data:image/png;base64,"
MAX_PNG = 512 * 1024
MAX_DATASET = 64 * 1024 * 1024
MAX_SAMPLES = 12000
TEMPLATES_PER_DIGIT = 128
FEATURE_COUNT = 320


def _gradient_histogram(image):
    horizontal = cv.Sobel(image, cv.CV_32F, 1, 0)
    vertical = cv.Sobel(image, cv.CV_32F, 0, 1)
    strength, angle = cv.cartToPolar(horizontal, vertical)
    direction = np.rint(angle * (8 / np.pi)).astype(np.int32) % 8
    # A 4 by 4 grid of cells, with eight unsigned directions per cell.
    rows, columns = np.indices(image.shape)
    bins = (rows // 8 * 4 + columns // 6) * 8 + direction
    return np.bincount(bins.ravel(), weights=strength.ravel(), minlength=128).astype(np.float32)


def _digit_features(gray):
    if gray.ndim != 2 or min(gray.shape) < 5 or max(gray.shape) > 2048:
        return None
    _, ink = cv.threshold(gray, 0, 255, cv.THRESH_BINARY_INV | cv.THRESH_OTSU)
    count, components, stats, _ = cv.connectedComponentsWithStats(ink, connectivity=8)
    if count < 2:
        return None
    ink_height = max(stats[1:, cv.CC_STAT_HEIGHT])
    keep = (stats[:, cv.CC_STAT_AREA] >= max(3, ink_height * .25))
    keep &= stats[:, cv.CC_STAT_HEIGHT] >= max(2, ink_height * .14)
    keep[0] = False
    ink = np.where(keep[components], 255, 0).astype(np.uint8)
    occupied = np.any(ink, axis=0).astype(np.int8)
    edges = np.flatnonzero(np.diff(np.r_[0, occupied, 0]))
    if not 1 <= len(edges) // 2 <= 5:
        return None
    result = []
    for x1, x2 in zip(edges[::2], edges[1::2]):
        ys, _ = np.nonzero(ink[:, x1:x2])
        glyph = ink[ys.min():ys.max() + 1, x1:x2]
        height, width = glyph.shape
        if height < ink_height * .43 or not width <= height * 1.05:
            return None
        scale = min(20 / width, 28 / height)
        w, h = max(1, round(width * scale)), max(1, round(height * scale))
        small = cv.resize(glyph, (w, h), interpolation=cv.INTER_AREA if scale < 1 else cv.INTER_LINEAR)
        canvas = np.zeros((32, 24), np.uint8)
        y, x = (32 - h) // 2, (24 - w) // 2
        canvas[y:y+h, x:x+w] = small
        bitmap = cv.resize(canvas, (12, 16), interpolation=cv.INTER_AREA).ravel() / np.float32(255)
        gradient = _gradient_histogram(canvas)
        bitmap /= max(np.linalg.norm(bitmap), .01)
        gradient /= max(np.linalg.norm(gradient), .01)
        result.append(np.concatenate((gradient * .8, bitmap * .6)))
    return np.asarray(result, dtype=np.float32)


@lru_cache(maxsize=1)
def _load(path, signature):
    try:
        with ZipFile(path) as zip_model:
            expected = {"version.npy", "features.npy", "labels.npy"}
            if set(zip_model.namelist()) != expected:
                return None
            if sum(item.file_size for item in zip_model.infolist()) > 3 * 1024 * 1024:
                return None
        with np.load(path, allow_pickle=False) as model:
            if model["version"].ndim != 0 or model["version"].item() != VERSION:
                return None
            features, labels = model["features"], model["labels"]
            if features.dtype != np.float32 or labels.dtype != np.uint8:
                return None
            if labels.ndim != 1 or not 20 <= labels.size <= 10 * TEMPLATES_PER_DIGIT:
                return None
            if features.shape != (labels.size, FEATURE_COUNT) or not np.isfinite(features).all():
                return None
            if np.any(np.bincount(labels, minlength=10)[:10] < 2) or np.max(labels) > 9:
                return None
            return {digit: features[labels == digit] for digit in range(10)}
    except (BadZipFile, EOFError, OSError, ValueError):
        return None


def load_model():
    path = os.environ.get("ABRIS_HTR_MODEL")
    return _load_path(Path(path)) if path else None


def _load_path(path):
    try:
        stat = path.stat()
    except OSError:
        return None
    # Readers reopen a model after atomic promotion without restarting the app.
    return _load(path, (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size))


def _predict_features(model, word_features):
    if word_features is None:
        return None
    digits = []
    confidence = 1.
    for glyph in word_features:
        ranked = sorted(
            (float(np.min(np.sum((templates - glyph) ** 2, axis=1))), digit)
            for digit, templates in model.items()
        )
        (best, digit), (second, _) = ranked[:2]
        margin = (second - best) / max(second, .0001)
        if best > .65 or margin < .25:
            return None
        digits.append(str(digit))
        confidence = min(confidence, margin)
    return "".join(digits), confidence


def predict(model, gray_strip):
    """Return a proposed digit string and the minimum class margin, or abstain."""
    return _predict_features(model, _digit_features(gray_strip))


def _decode_png(data_url):
    if not isinstance(data_url, str) or not data_url.startswith(PNG_PREFIX):
        return None
    encoded = data_url[len(PNG_PREFIX):]
    if len(encoded) > MAX_PNG * 4 // 3:
        return None
    try:
        data = base64.b64decode(encoded, validate=True)
    except binascii.Error:
        return None
    if not data.startswith(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR") or len(data) < 24:
        return None
    width, height = struct.unpack(">II", data[16:24])
    if not (5 <= width <= 2048 and 5 <= height <= 2048 and width * height <= 262144):
        return None
    try:
        return cv.imdecode(np.frombuffer(data, dtype=np.uint8), cv.IMREAD_GRAYSCALE)
    except cv.error:
        return None


def _dataset(directory):
    samples = []
    invalid = 0
    seen = {}
    byte_count = 0
    for path in sorted(directory.glob("*.json")):
        if path.is_symlink() or not path.is_file():
            continue
        byte_count += path.stat().st_size
        if byte_count > MAX_DATASET:
            raise ValueError("The input directory exceeds 64 MiB.")
        document = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(document, dict) or document.get("version") != VERSION:
            raise ValueError("Correction files must use version 1.")
        records = document.get("samples")
        if not isinstance(records, list):
            raise ValueError("A correction file has no samples array.")
        if len(records) + len(samples) + invalid > MAX_SAMPLES:
            raise ValueError("The input directory exceeds 12000 samples.")
        for record in records:
            if not isinstance(record, dict):
                invalid += 1
                continue
            label, group = record.get("label"), record.get("group")
            if not (isinstance(label, str) and DIGITS.fullmatch(label)
                    and isinstance(group, str) and group.strip() and len(group) <= 200):
                invalid += 1
                continue
            gray = _decode_png(record.get("image"))
            if gray is None:
                invalid += 1
                continue
            digest = hashlib.sha256(gray.tobytes() + struct.pack(">II", *gray.shape)).digest()
            if digest in seen:
                if seen[digest] != (group, label):
                    raise ValueError("An identical crop occurs with conflicting labels or groups.")
                invalid += 1
                continue
            seen[digest] = (group, label)
            samples.append((group, label, _digit_features(gray)))
    return samples, invalid


def _heldout_group(group):
    digest = hashlib.sha256(b"abris-holdout-v1\0" + group.encode()).digest()
    return int.from_bytes(digest[:8], "big") % 5 == 0


def _split_and_train(samples):
    held_groups = {group for group, _, _ in samples if _heldout_group(group)}
    if not held_groups:
        raise ValueError("The fixed evaluation split has no drawing groups. Add more independent drawings.")
    templates = [[] for _ in range(10)]
    held_out = []
    training_count = 0
    train_groups = set()
    for group, label, features in samples:
        if group in held_groups:
            held_out.append((group, label, features))
        elif features is not None and len(label) == len(features):
            train_groups.add(group)
            training_count += 1
            for digit, glyph in zip(label, features):
                templates[int(digit)].append(glyph)
    if any(len(digit) < 2 for digit in templates):
        raise ValueError("Training groups must contain at least two segmented examples of every digit.")
    model = {}
    for digit, examples in enumerate(templates):
        selected = np.linspace(0, len(examples) - 1, min(len(examples), TEMPLATES_PER_DIGIT), dtype=int)
        model[digit] = np.asarray(examples, dtype=np.float32)[selected]
    return model, held_out, {"train_groups": len(train_groups), "train_samples_used": training_count}


def _evaluate(model, held_out):
    accepted = 0
    per_group = {}
    for group, label, features in held_out:
        result = _predict_features(model, features)
        stats = per_group.setdefault(group, [0, 0])
        stats[0] += 1
        accepted += result is not None
        stats[1] += result is not None and result[0] == label
    exact = sum(value[1] for value in per_group.values())
    group_accuracy = sum(correct / total for total, correct in per_group.values()) / len(per_group)
    return {
        "heldout_groups": len(per_group),
        "heldout_samples": len(held_out),
        "heldout_accepted": accepted,
        "heldout_exact": exact,
        "heldout_exact_accuracy": round(exact / len(held_out), 4),
        "heldout_mean_group_accuracy": round(group_accuracy, 4),
    }


def _serialize(model):
    labels = np.repeat(np.arange(10, dtype=np.uint8), [len(model[digit]) for digit in range(10)])
    features = np.concatenate(list(model.values()))
    data = io.BytesIO()
    np.savez_compressed(data, version=np.array(VERSION), features=features, labels=labels)
    return data.getbuffer()


def _promote(path, model, held_out, stats):
    if stats["train_groups"] < 20 or stats["train_samples_used"] < 200:
        return "too_few_training_samples"
    held_count = stats["heldout_samples"]
    if stats["heldout_groups"] < 8 or held_count < 100:
        return "too_few_heldout_samples"
    if min(stats["heldout_exact_accuracy"], stats["heldout_mean_group_accuracy"]) < .88:
        return "accuracy_below_threshold"
    if path.exists():
        incumbent = _load_path(path)
        if incumbent is None:
            return "incumbent_invalid"
        previous = _evaluate(incumbent, held_out)
        improvement = stats["heldout_exact"] - previous["heldout_exact"]
        if improvement < math.ceil(held_count * .02):
            return "accuracy_not_improved"
        if stats["heldout_mean_group_accuracy"] < previous["heldout_mean_group_accuracy"]:
            return "group_accuracy_regressed"
    stage = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", prefix=".model-", dir=path.parent, delete=False) as handle:
            stage = Path(handle.name)
            stage.chmod(0o640)
            handle.write(_serialize(model))
            handle.flush()
            os.fsync(handle.fileno())
        stage.replace(path)
    finally:
        if stage is not None:
            stage.unlink(missing_ok=True)
    return "promoted"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Folder containing operator-exported correction JSON files")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--output", type=Path, help="New local .npz model path")
    mode.add_argument("--promote", type=Path, help="Evaluate and conditionally replace the managed model")
    options = parser.parse_args()
    if not options.directory.is_dir():
        parser.error("The correction directory does not exist.")
    try:
        samples, invalid = _dataset(options.directory)
        model, held_out, training = _split_and_train(samples)
        stats = {**training, **_evaluate(model, held_out), "samples_skipped": invalid}
        if options.output:
            with options.output.open("xb") as handle:
                handle.write(_serialize(model))
        if options.promote:
            stats["promotion"] = _promote(options.promote, model, held_out, stats)
    except json.JSONDecodeError:
        parser.error("A correction file contains invalid JSON.")
    except FileExistsError:
        parser.error("The output already exists.")
    except ValueError as error:
        parser.error(str(error))
    except OSError:
        parser.error("Reading the dataset or writing the model failed.")
    print(json.dumps(stats))


if __name__ == "__main__":
    main()

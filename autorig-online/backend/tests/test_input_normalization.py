import json
import struct
from pathlib import Path
from unittest.mock import patch

import pytest

import input_normalization as normalizer


def write_glb(path: Path, document: dict, binary: bytes = b"\0\0\0\0"):
    encoded = json.dumps(document, separators=(",", ":")).encode("utf-8")
    encoded += b" " * ((4 - len(encoded) % 4) % 4)
    binary += b"\0" * ((4 - len(binary) % 4) % 4)
    total = 12 + 8 + len(encoded) + 8 + len(binary)
    path.write_bytes(
        b"glTF"
        + struct.pack("<II", 2, total)
        + struct.pack("<II", len(encoded), 0x4E4F534A)
        + encoded
        + struct.pack("<II", len(binary), 0x004E4942)
        + binary
    )


def test_local_uncompressed_glb_is_unchanged(tmp_path):
    token = "11111111-2222-3333-4444-555566667777"
    directory = tmp_path / token
    directory.mkdir()
    source = directory / "model.glb"
    write_glb(source, {"asset": {"version": "2.0"}, "buffers": [{"byteLength": 4}]})
    url = f"https://autorig.online/u/{token}/model.glb"
    with patch.object(normalizer, "UPLOAD_DIR", str(tmp_path)):
        result = normalizer.normalize_local_meshopt(url)
    assert result.effective_url == url
    assert result.changed is False


def test_meshopt_decoded_size_is_bounded(tmp_path):
    source = tmp_path / "huge.glb"
    write_glb(
        source,
        {
            "asset": {"version": "2.0"},
            "buffers": [{"byteLength": 4}],
            "bufferViews": [
                {
                    "extensions": {
                        "EXT_meshopt_compression": {
                            "buffer": 0,
                            "byteOffset": 0,
                            "byteLength": 4,
                            "count": normalizer.MAX_DECODED_BYTES,
                            "byteStride": 2,
                            "mode": "ATTRIBUTES",
                        }
                    }
                }
            ],
        },
    )
    with pytest.raises(normalizer.InputNormalizationError, match="MESHOPT_SIZE"):
        normalizer._glb_document(source)


def test_upload_path_rejects_non_uuid_token(tmp_path):
    with patch.object(normalizer, "UPLOAD_DIR", str(tmp_path)):
        with pytest.raises(normalizer.InputNormalizationError, match="UPLOAD_PATH"):
            normalizer._local_upload("https://autorig.online/u/not-a-uuid/model.glb")


def test_basis_texture_sources_are_counted_and_validated():
    document = {
        "images": [{"bufferView": 0, "mimeType": "image/ktx2"}],
        "textures": [{"extensions": {"KHR_texture_basisu": {"source": 0}}}],
    }
    assert normalizer._basis_image_count(document) == 1
    document["textures"][0]["extensions"]["KHR_texture_basisu"]["source"] = 3
    with pytest.raises(normalizer.InputNormalizationError, match="KTX2_SCHEMA"):
        normalizer._basis_image_count(document)


def test_missing_runtime_is_retryable_without_input_failure(tmp_path):
    token = "11111111-2222-3333-4444-555566667777"
    directory = tmp_path / token
    directory.mkdir()
    source = directory / "model.glb"
    write_glb(
        source,
        {
            "asset": {"version": "2.0"},
            "buffers": [{"byteLength": 4}],
            "bufferViews": [
                {
                    "extensions": {
                        "EXT_meshopt_compression": {
                            "buffer": 0,
                            "byteOffset": 0,
                            "byteLength": 4,
                            "count": 1,
                            "byteStride": 4,
                            "mode": "ATTRIBUTES",
                        }
                    }
                }
            ],
        },
    )
    url = f"https://autorig.online/u/{token}/model.glb"
    with (
        patch.object(normalizer, "UPLOAD_DIR", str(tmp_path)),
        patch.object(normalizer, "NODE_EXE", str(tmp_path / "missing-node")),
        patch.object(normalizer, "NEW_TASK_MIN_FREE_GB", 0),
    ):
        with pytest.raises(
            normalizer.InputNormalizationDeferred, match="MESHOPT_RUNTIME"
        ):
            normalizer.normalize_local_meshopt(url)

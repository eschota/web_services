"""Inputs the server writes for a task must be readable by nginx (0644).

A link-created task failed with "source returned HTTP 403" because the mirror
wrote its copy through mkstemp (0600) and nginx could not serve it.
"""
import os
import stat
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import input_normalization as normalizer

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")

GLB = b"glTF" + b"\x02\x00\x00\x00" + (20).to_bytes(4, "little") + b"\0" * 8


class FakeResponse:
    status_code = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_bytes(self, chunk_size=0):
        yield GLB


class FakeClient:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def stream(self, method, url):
        return FakeResponse()


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def mirror(tmp_path):
    import httpx
    with patch.object(normalizer, "UPLOAD_DIR", str(tmp_path)), \
            patch.object(normalizer, "NEW_TASK_MIN_FREE_GB", 0), \
            patch.object(httpx, "Client", FakeClient):
        return normalizer._mirror_remote_glb("https://example.com/files/model.glb")


def test_mirrored_glb_is_world_readable(tmp_path):
    result = mirror(tmp_path)
    assert result is not None and result.changed
    target = next(tmp_path.glob("*/model.glb"))
    assert mode(target) & 0o044 == 0o044, oct(mode(target))


def test_cached_mirror_from_old_code_is_repaired(tmp_path):
    mirror(tmp_path)
    target = next(tmp_path.glob("*/model.glb"))
    os.chmod(target, 0o600)
    mirror(tmp_path)
    assert mode(target) & 0o044 == 0o044, oct(mode(target))

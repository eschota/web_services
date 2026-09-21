import io
from pathlib import Path

import pytest
from fastapi import HTTPException
from PIL import Image

from ai_avatar_assets import (
    MAX_ASSET_BYTES,
    AvatarAssetStore,
    validate_import_url,
)


def _image_bytes(format_name: str = "PNG", size=(7, 5), color=(20, 40, 80)) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", size, color).save(output, format=format_name)
    return output.getvalue()


def _owner(value: str) -> dict[str, str]:
    return {"owner_type": "user", "owner_id": value}


def test_original_bytes_hash_dimensions_and_owner_dedup(tmp_path: Path) -> None:
    store = AvatarAssetStore(tmp_path, public_base_url="https://autorig.online")
    data = _image_bytes("PNG")
    first = store.put_bytes(_owner("a"), data, filename="face.png", content_type="image/png")
    second = store.put_bytes(_owner("a"), data, filename="face.png", content_type="image/png")

    assert first["asset_id"] == second["asset_id"]
    assert first["width"] == 7 and first["height"] == 5
    stored, mime = store.resolve_capability(first["asset_id"], first["sha256"], "png")
    assert stored.read_bytes() == data
    assert mime == "image/png"


def test_same_hash_different_owner_gets_different_capability(tmp_path: Path) -> None:
    store = AvatarAssetStore(tmp_path)
    data = _image_bytes()
    first = store.put_bytes(_owner("a"), data, filename="x.png", content_type="image/png")
    second = store.put_bytes(_owner("b"), data, filename="x.png", content_type="image/png")
    assert first["sha256"] == second["sha256"]
    assert first["asset_id"] != second["asset_id"]
    assert first["canonical_url"] != second["canonical_url"]


def test_upload_bounds_and_format_spoof(tmp_path: Path) -> None:
    store = AvatarAssetStore(tmp_path)
    with pytest.raises(HTTPException) as too_large:
        store.put_bytes(_owner("a"), b"x" * (MAX_ASSET_BYTES + 1), filename="x.png")
    assert too_large.value.status_code == 413

    jpeg = _image_bytes("JPEG")
    with pytest.raises(HTTPException) as spoofed:
        store.put_bytes(_owner("a"), jpeg, filename="fake.png", content_type="image/png")
    assert spoofed.value.status_code == 400


def test_capability_rejects_url_escape_and_wrong_hash(tmp_path: Path) -> None:
    store = AvatarAssetStore(tmp_path)
    asset = store.put_bytes(_owner("a"), _image_bytes(), filename="x.png")
    for asset_id, digest, extension in (
        ("../escape", asset["sha256"], "png"),
        (asset["asset_id"], "0" * 64, "png"),
        (asset["asset_id"], asset["sha256"], "../png"),
    ):
        with pytest.raises(HTTPException) as missing:
            store.resolve_capability(asset_id, digest, extension)
        assert missing.value.status_code == 404


def test_owner_quota_counts_unique_assets_only(tmp_path: Path) -> None:
    store = AvatarAssetStore(tmp_path, max_assets_per_owner=1)
    first_data = _image_bytes(color=(1, 2, 3))
    store.put_bytes(_owner("a"), first_data, filename="one.png")
    store.put_bytes(_owner("a"), first_data, filename="one-again.png")
    with pytest.raises(HTTPException) as quota:
        store.put_bytes(_owner("a"), _image_bytes(color=(3, 2, 1)), filename="two.png")
    assert quota.value.status_code == 409
    # Quota is owner-scoped, not global.
    store.put_bytes(_owner("b"), _image_bytes(color=(3, 2, 1)), filename="two.png")


@pytest.mark.parametrize(
    "url",
    [
        "http://autorig.online/renderfin/render/a.png",
        "https://evil.example/renderfin/render/a.png",
        "https://autorig.online.evil.example/renderfin/render/a.png",
        "https://autorig.online/private/a.png",
        "https://user:pass@autorig.online/renderfin/render/a.png",
        "https://autorig.online:444/renderfin/render/a.png",
    ],
)
def test_import_ssrf_allowlist_rejects_untrusted_urls(url: str) -> None:
    with pytest.raises(ValueError):
        validate_import_url(url)


def test_import_allowlist_accepts_only_named_paths() -> None:
    assert validate_import_url(
        "https://autorig.online/dev/api/scratch/task/source.png"
    )
    assert validate_import_url(
        "https://autorig.online/renderfin/render/user/result.webp"
    )
    assert validate_import_url(
        "https://autorig.online/api/ai/avatar-assets/" + "a" * 32 + "/" + "b" * 64 + ".png"
    )

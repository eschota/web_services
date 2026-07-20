#!/usr/bin/env python3
"""Regenerate site logo + favicon assets from Human.png (RGBA). Run from repo root or this dir."""
from __future__ import annotations

import base64
import io
from pathlib import Path

from PIL import Image

SRC = Path(__file__).resolve().parent / "Human.png"
ICONS_OUT = Path(__file__).resolve().parent
SITE_LOGO = Path(__file__).resolve().parents[1] / "images" / "logo"


def fit_contain(im: Image.Image, canvas_w: int, canvas_h: int, margin: float = 0.08) -> Image.Image:
    """Scale RGBA `im` uniformly, center on transparent canvas."""
    im = im.convert("RGBA")
    sw, sh = im.size
    inner_w = int(canvas_w * (1 - 2 * margin))
    inner_h = int(canvas_h * (1 - 2 * margin))
    inner_w = max(inner_w, 1)
    inner_h = max(inner_h, 1)
    scale = min(inner_w / sw, inner_h / sh)
    nw, nh = max(1, int(sw * scale)), max(1, int(sh * scale))
    resized = im.resize((nw, nh), Image.Resampling.LANCZOS)
    canvas = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
    ox = (canvas_w - nw) // 2
    oy = (canvas_h - nh) // 2
    canvas.paste(resized, (ox, oy), resized)
    return canvas


def fit_square(im: Image.Image, size: int, margin: float = 0.06) -> Image.Image:
    return fit_contain(im, size, size, margin=margin)


def ico_write(path: Path, source: Image.Image, sizes: tuple[int, ...]) -> None:
    images: list[Image.Image] = []
    for s in sizes:
        fi = fit_square(source, s).resize((s, s), Image.Resampling.LANCZOS)
        images.append(fi)
    images[0].save(
        path,
        format="ICO",
        sizes=[(x.width, x.height) for x in images],
        append_images=images[1:],
    )


def png_data_url(im: Image.Image) -> str:
    buf = io.BytesIO()
    im.save(buf, format="PNG", optimize=True)
    b64 = base64.standard_b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def write_favicon_svg(path: Path, im32: Image.Image) -> None:
    """Self-contained SVG favicon referencing embedded 32px PNG."""
    href = png_data_url(im32)
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 32 32">
  <image width="32" height="32" xlink:href="{href}"/>
</svg>
"""
    path.write_text(svg, encoding="utf-8")


def main() -> None:
    if not SRC.is_file():
        raise SystemExit(f"Missing source: {SRC}")

    im = Image.open(SRC).convert("RGBA")
    SITE_LOGO.mkdir(parents=True, exist_ok=True)

    # Archive / source-of-truth copies (Icons_png) with explicit suffixes
    header_1x = fit_contain(im, 360, 240)
    header_2x = fit_contain(im, 720, 480)
    header_1x.save(ICONS_OUT / "Human_brand_header_360x240.png", optimize=True)
    header_2x.save(ICONS_OUT / "Human_brand_header_720x480@2x.png", optimize=True)

    sq256 = fit_square(im, 256)
    sq512 = fit_square(im, 512)
    sq256.save(ICONS_OUT / "Human_brand_square_256.png", optimize=True)
    sq512.save(ICONS_OUT / "Human_brand_square_512@2x.png", optimize=True)

    for s in (16, 32, 48):
        fav = fit_square(im, s)
        fav.save(ICONS_OUT / f"Human_brand_favicon_{s}x{s}.png", optimize=True)

    apple = fit_square(im, 180, margin=0.04)
    apple.save(ICONS_OUT / "Human_brand_apple_touch_180.png", optimize=True)

    ico_write(ICONS_OUT / "Human_brand_favicon.ico", im, (16, 32, 48))

    # Live site assets (images/logo)
    header_1x.save(SITE_LOGO / "autorig-logo.png", optimize=True)
    header_2x.save(SITE_LOGO / "autorig-logo@2x.png", optimize=True)
    sq256.save(SITE_LOGO / "_logo.png", optimize=True)
    sq512.save(SITE_LOGO / "_logo@2x.png", optimize=True)

    fit_square(im, 16).save(SITE_LOGO / "favicon-16.png", optimize=True)
    im32 = fit_square(im, 32)
    im32.save(SITE_LOGO / "favicon-32.png", optimize=True)
    fit_square(im, 48).save(SITE_LOGO / "favicon-48.png", optimize=True)
    apple.save(SITE_LOGO / "apple-touch-icon.png", optimize=True)
    ico_write(SITE_LOGO / "favicon.ico", im, (16, 32, 48))
    write_favicon_svg(SITE_LOGO / "favicon.svg", im32)

    print("Wrote brand PNG/ICO/SVG to", SITE_LOGO, "and archive copies to", ICONS_OUT)


if __name__ == "__main__":
    main()

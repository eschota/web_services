"""Image utilities for RenderFin MCP server."""

import io
from pathlib import Path

from PIL import Image


def convert_png_to_jpg(png_bytes: bytes, quality: int = 85) -> bytes:
    """Convert PNG bytes to JPG bytes.
    
    Args:
        png_bytes: Raw PNG image data.
        quality: JPEG quality (1-100, default 85 for good balance).
    
    Returns:
        JPEG image data as bytes.
    """
    img = Image.open(io.BytesIO(png_bytes))
    
    # Handle transparency: convert RGBA/LA/P to RGB with white background
    if img.mode in ('RGBA', 'LA', 'P'):
        background = Image.new('RGB', img.size, (255, 255, 255))
        if img.mode == 'P':
            img = img.convert('RGBA')
        # Paste with alpha mask if available
        if img.mode == 'RGBA':
            background.paste(img, mask=img.split()[3])  # Alpha channel
        else:
            background.paste(img)
        img = background
    elif img.mode != 'RGB':
        img = img.convert('RGB')
    
    # Save as JPEG with optimization
    output = io.BytesIO()
    img.save(output, format='JPEG', quality=quality, optimize=True)
    return output.getvalue()


def ensure_jpg_extension(path: str) -> str:
    """Ensure the file path has .jpg extension.
    
    Args:
        path: Original file path (may have .png or other extension).
    
    Returns:
        Path with .jpg extension.
    """
    p = Path(path)
    # If already .jpg or .jpeg, return as-is
    if p.suffix.lower() in ('.jpg', '.jpeg'):
        return path
    # Replace extension with .jpg
    return str(p.with_suffix('.jpg'))

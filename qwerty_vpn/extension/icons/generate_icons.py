"""Generate simple SVG-based PNG icons for the extension."""
import struct
import zlib

def create_png(width, height, rgba_data):
    """Create a minimal PNG file from RGBA data."""
    def chunk(chunk_type, data):
        c = chunk_type + data
        crc = struct.pack('>I', zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack('>I', len(data)) + c + crc

    header = b'\x89PNG\r\n\x1a\n'
    ihdr = chunk(b'IHDR', struct.pack('>IIBBBBB', width, height, 8, 6, 0, 0, 0))

    raw_data = b''
    for y in range(height):
        raw_data += b'\x00'  # filter byte
        for x in range(width):
            idx = (y * width + x) * 4
            raw_data += bytes(rgba_data[idx:idx+4])

    idat = chunk(b'IDAT', zlib.compress(raw_data, 9))
    iend = chunk(b'IEND', b'')

    return header + ihdr + idat + iend

def draw_shield_icon(size):
    """Draw a simple shield/VPN icon."""
    pixels = [0] * (size * size * 4)

    cx, cy = size // 2, size // 2
    r = size * 0.4

    for y in range(size):
        for x in range(size):
            idx = (y * size + x) * 4
            # Shield shape
            dx = (x - cx) / r
            dy = (y - cy * 0.85) / (r * 1.2)

            in_shield = False
            if dy >= -0.8 and dy <= 1.0:
                # Top: rounded
                if dy < 0:
                    w = 1.0
                    if abs(dx) <= w:
                        in_shield = True
                else:
                    # Bottom: narrows to point
                    w = 1.0 - dy * 0.9
                    if abs(dx) <= w:
                        in_shield = True

            if in_shield:
                # Gradient: top purple to bottom blue
                t = (dy + 0.8) / 1.8
                r_c = int(108 + (80 - 108) * t)
                g_c = int(92 + (120 - 92) * t)
                b_c = int(231 + (255 - 231) * t)
                pixels[idx] = max(0, min(255, r_c))
                pixels[idx+1] = max(0, min(255, g_c))
                pixels[idx+2] = max(0, min(255, b_c))
                pixels[idx+3] = 255

                # Letter "Q" check mark area
                check_dy = (y - cy) / r
                check_dx = (x - cx) / r
                if abs(check_dx) < 0.25 and abs(check_dy) < 0.35:
                    pixels[idx] = 255
                    pixels[idx+1] = 255
                    pixels[idx+2] = 255
                    pixels[idx+3] = 255

    return pixels

for s in [16, 32, 48, 128]:
    pixels = draw_shield_icon(s)
    png_data = create_png(s, s, pixels)
    with open(f'/root/qwerty_vpn/extension/icons/icon{s}.png', 'wb') as f:
        f.write(png_data)
    print(f'Generated icon{s}.png')


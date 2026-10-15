#!/usr/bin/env python3
"""Draws the seven missing rig-type icons (site refresh to V3, 2026-10-16) with Pillow only.

    py -3.11 tools/draw_rig_icons_v3.py [--out static/Icons_png] [--preview preview.png]

Writes Bird.png, Serpent.png, Monster.png, Multilimb.png, Hands.png, Vehicle.png and Other.png (1024x1024 RGBA, the
size and framing of Human.png / Dog.png...): the same glass cube (hexagon outline, cyan to violet edge glow, soft
white halo, floor and back-wall gradients), a soft white-lavender silhouette and a glowing skeleton with ring joints.
Everything is vector shapes (polygons, ellipses, thick polylines) rendered at 2x and downsampled; no downloads, no AI art.
"""
import argparse
import math
import os

from PIL import Image, ImageChops, ImageDraw, ImageFilter

SIZE = 1024
SS = 2                       # supersampling
S = SIZE * SS

# Cube geometry measured on Human.png / Dog.png (alpha bbox 123..932 x 63..1000 including the halo).
T, UR, LR, B, LL, UL = (512, 87), (906, 262), (906, 770), (512, 973), (118 + 29, 770), (118 + 29, 262)
CU, CL = (512, 388), (512, 600)       # upper and lower inner corners
CYAN, VIOLET = (60, 214, 240), (146, 92, 232)


def sc(p):
    return (p[0] * SS, p[1] * SS)


def lerp(a, b, t):
    return tuple(int(round(a[i] + (b[i] - a[i]) * t)) for i in range(len(a)))


def new_layer():
    return Image.new("RGBA", (S, S), (0, 0, 0, 0))


def poly_mask(points):
    m = Image.new("L", (S, S), 0)
    ImageDraw.Draw(m).polygon([sc(p) for p in points], fill=255)
    return m


def hgrad(c0, c1, x0=146, x1=906):
    row = Image.new("RGB", (S, 1))
    px = row.load()
    for x in range(S):
        t = min(1.0, max(0.0, (x / SS - x0) / (x1 - x0)))
        px[x, 0] = lerp(c0, c1, t)
    return row.resize((S, S))


def vgrad(c0, c1, y0, y1):
    col = Image.new("RGB", (1, S))
    px = col.load()
    for y in range(S):
        t = min(1.0, max(0.0, (y / SS - y0) / (y1 - y0)))
        px[0, y] = lerp(c0, c1, t)
    return col.resize((S, S))


def paste(base, color_img, mask, alpha=255):
    if alpha != 255:
        mask = mask.point(lambda v: v * alpha // 255)
    base.paste(color_img.convert("RGBA"), (0, 0), mask)


def cube_background():
    img = new_layer()
    hexa = [T, UR, LR, B, LL, UL]
    hex_mask = poly_mask(hexa)
    # outer halo: white glow around the hexagon
    halo = hex_mask.filter(ImageFilter.MaxFilter(2 * SS * 2 + 1)).filter(ImageFilter.GaussianBlur(8 * SS))
    glow = Image.new("RGBA", (S, S), (255, 255, 255, 0))
    glow.putalpha(halo.point(lambda v: int(v * 0.85)))
    img.alpha_composite(glow)
    # walls: left bluish, right lavender, horizontal gradient
    paste(img, hgrad((196, 228, 253), (230, 218, 253)), hex_mask, 245)
    # top (ceiling) is lighter
    top = poly_mask([T, UR, CU, UL])
    paste(img, vgrad((246, 250, 255), (226, 238, 254), 87, 388), top, 150)
    # floor: deeper blue to lavender with a soft reflection
    floor = poly_mask([CL, LR, B, LL])
    paste(img, hgrad((178, 214, 252), (214, 196, 250)), floor, 235)
    # inner edges (white, soft)
    d = ImageDraw.Draw(img)
    for a, b_ in ((T, CU), (CU, UL), (CU, UR), (CL, LL), (CL, LR), (CL, B), (UL, LL), (UR, LR), (CU, CL)):
        w = 5 if (a, b_) != (CU, CL) else 3
        d.line([sc(a), sc(b_)], fill=(255, 255, 255, 190 if w == 5 else 90), width=w * SS)
    # glowing outline: cyan on the left, violet on the right
    ring = Image.new("L", (S, S), 0)
    ImageDraw.Draw(ring).line([sc(p) for p in hexa + [hexa[0]]], fill=255, width=11 * SS, joint="curve")
    paste(img, hgrad(CYAN, VIOLET), ring, 235)
    blur = ring.filter(ImageFilter.GaussianBlur(9 * SS))
    paste(img, hgrad(CYAN, VIOLET), blur, 110)
    return img


def circle_pts(cx, cy, rx, ry, angle=0.0, n=72):
    a = math.radians(angle)
    pts = []
    for i in range(n):
        t = 2 * math.pi * i / n
        x, y = rx * math.cos(t), ry * math.sin(t)
        pts.append((cx + x * math.cos(a) - y * math.sin(a), cy + x * math.sin(a) + y * math.cos(a)))
    return pts


def catmull(points, per=14):
    pts = [points[0]] + list(points) + [points[-1]]
    out = []
    for i in range(1, len(pts) - 2):
        p0, p1, p2, p3 = pts[i - 1], pts[i], pts[i + 1], pts[i + 2]
        for k in range(per):
            t = k / per
            out.append(tuple(0.5 * ((2 * p1[j]) + (-p0[j] + p2[j]) * t + (2 * p0[j] - 5 * p1[j] + 4 * p2[j] - p3[j]) * t * t
                                    + (-p0[j] + 3 * p1[j] - 3 * p2[j] + p3[j]) * t ** 3) for j in range(2)))
    out.append(points[-1])
    return out


class Shape:
    """Silhouette = union of polygons, thick round lines and darker detail strokes."""

    def __init__(self):
        self.mask = Image.new("L", (S, S), 0)
        self.d = ImageDraw.Draw(self.mask)
        self.detail = Image.new("L", (S, S), 0)
        self.dd = ImageDraw.Draw(self.detail)

    def poly(self, pts):
        self.d.polygon([sc(p) for p in pts], fill=255)

    def ellipse(self, cx, cy, rx, ry, angle=0.0):
        self.poly(circle_pts(cx, cy, rx, ry, angle))

    def line(self, pts, w, round_ends=True):
        pts = [sc(p) for p in pts]
        self.d.line(pts, fill=255, width=int(w * SS), joint="curve")
        if round_ends:
            r = w * SS / 2
            for x, y in (pts[0], pts[-1]):
                self.d.ellipse([x - r, y - r, x + r, y + r], fill=255)

    def taper(self, pts, w0, w1):
        n = len(pts)
        for i in range(n - 1):
            w = w0 + (w1 - w0) * i / max(1, n - 2)
            self.line([pts[i], pts[i + 1]], w)

    def mark(self, pts, w=6, closed=False):
        pts = [sc(p) for p in (pts + [pts[0]] if closed else pts)]
        self.dd.line(pts, fill=255, width=int(w * SS), joint="curve")

    def dot(self, cx, cy, r):
        x, y = sc((cx, cy))
        self.dd.ellipse([x - r * SS, y - r * SS, x + r * SS, y + r * SS], fill=255)


def render_shape(img, shape, floor_y=790, shadow_w=240):
    # contact shadow on the floor
    sh = Image.new("L", (S, S), 0)
    ImageDraw.Draw(sh).ellipse([sc((512 - shadow_w, floor_y - 26)), sc((512 + shadow_w, floor_y + 34))], fill=255)
    sh = sh.filter(ImageFilter.GaussianBlur(20 * SS))
    paste(img, Image.new("RGB", (S, S), (150, 150, 235)), sh, 120)
    m = shape.mask
    # white halo around the figure, then soft form shading: white on top, lavender toward the bottom
    halo = m.filter(ImageFilter.MaxFilter(2 * SS * 4 + 1)).filter(ImageFilter.GaussianBlur(8 * SS))
    paste(img, Image.new("RGB", (S, S), (255, 255, 255)), halo, 200)
    body = vgrad((255, 255, 255), (222, 228, 252), 250, 820)
    paste(img, body, m, 255)
    inner = m.filter(ImageFilter.MinFilter(2 * SS * 9 + 1)).filter(ImageFilter.GaussianBlur(10 * SS))
    rim = ImageChops.subtract(m, inner)
    paste(img, Image.new("RGB", (S, S), (196, 204, 246)), rim, 120)
    # detail strokes (eyes, mouth, windows, panel lines)
    det = ImageChops.multiply(shape.detail, m)
    paste(img, Image.new("RGB", (S, S), (176, 186, 240)), det, 210)


def skeleton(img, chains, r=19):
    """chains: list of polylines in 1024 coordinates; every vertex gets a ring joint."""
    layer = new_layer()
    d = ImageDraw.Draw(layer)
    mask = Image.new("L", (S, S), 0)
    md = ImageDraw.Draw(mask)
    for pts in chains:
        md.line([sc(p) for p in pts], fill=255, width=9 * SS, joint="curve")
    glow = mask.filter(ImageFilter.GaussianBlur(7 * SS))
    paste(layer, hgrad((90, 200, 255), (160, 110, 245)), glow, 150)
    paste(layer, hgrad((38, 196, 248), (132, 88, 232)), mask, 255)
    core = Image.new("L", (S, S), 0)
    cd = ImageDraw.Draw(core)
    for pts in chains:
        cd.line([sc(p) for p in pts], fill=255, width=3 * SS, joint="curve")
    paste(layer, Image.new("RGB", (S, S), (236, 250, 255)), core, 230)
    d = ImageDraw.Draw(layer)
    seen = set()
    for pts in chains:
        for p in pts:
            if p in seen:
                continue
            seen.add(p)
            x, y = sc(p)
            t = min(1.0, max(0.0, (p[0] - 146) / 760))
            col = lerp((30, 190, 248), (132, 88, 232), t) + (255,)
            d.ellipse([x - (r + 5) * SS, y - (r + 5) * SS, x + (r + 5) * SS, y + (r + 5) * SS], fill=col)
            d.ellipse([x - (r - 4) * SS, y - (r - 4) * SS, x + (r - 4) * SS, y + (r - 4) * SS], fill=(255, 255, 255, 255))
    img.alpha_composite(layer)


# ---------------------------------------------------------------- silhouettes (1024 coordinates)
def bird():
    s = Shape()
    s.ellipse(520, 590, 150, 104, -14)                        # body
    s.line([(445, 525), (405, 480)], 80)                      # neck
    s.ellipse(392, 468, 54, 50)                               # head
    s.poly([(346, 452), (346, 486), (282, 480)])              # beak
    s.poly([(650, 600), (790, 636), (826, 704), (744, 690), (660, 652)])                  # tail
    s.poly([(468, 560), (540, 446), (690, 416), (770, 470), (668, 566), (556, 622)])      # folded wing
    s.line([(545, 680), (532, 756)], 20)                      # legs
    s.line([(595, 676), (604, 756)], 20)
    s.line([(532, 756), (500, 770)], 14)
    s.line([(604, 756), (636, 770)], 14)
    s.dot(380, 460, 9)
    s.mark([(560, 540), (640, 500), (720, 484)], 5)           # feather lines
    s.mark([(540, 580), (630, 548), (700, 540)], 5)
    chains = [[(392, 468), (440, 540), (520, 600), (650, 632), (792, 682)],
              [(500, 545), (606, 478), (726, 456)],
              [(545, 660), (532, 756)],
              [(595, 656), (604, 756)]]
    return s, chains


def serpent():
    s = Shape()
    path = catmull([(330, 790), (440, 770), (560, 745), (650, 690), (630, 615), (520, 585), (430, 535),
                    (455, 465), (545, 435), (620, 385), (590, 320)])
    s.taper(path, 20, 66)
    s.ellipse(585, 322, 62, 44, -50)                          # head
    s.line([(556, 290), (520, 262)], 7)                       # tongue
    s.line([(520, 262), (498, 258)], 5)
    s.line([(520, 262), (508, 284)], 5)
    s.dot(598, 314, 9)
    s.mark(path[::9], 4)
    pts = [path[i] for i in range(0, len(path), 15)] + [path[-1]]
    return s, [[(int(x), int(y)) for x, y in pts]]


def monster():
    s = Shape()
    s.ellipse(512, 560, 152, 170)                             # body
    s.ellipse(512, 410, 105, 92)                              # head
    s.poly([(432, 360), (396, 262), (476, 340)])              # horns
    s.poly([(592, 360), (628, 262), (548, 340)])
    s.line([(392, 520), (330, 580), (292, 640)], 54)          # arms
    s.line([(632, 520), (694, 580), (732, 640)], 54)
    s.poly([(284, 640), (258, 690), (306, 668)])              # claws
    s.poly([(740, 640), (766, 690), (718, 668)])
    s.line([(455, 700), (440, 790)], 72)                      # legs
    s.line([(569, 700), (584, 790)], 72)
    for x in (462, 512, 562):                                 # back spikes
        s.poly([(x - 20, 330), (x, 280), (x + 20, 330)])
    s.dot(470, 405, 10)
    s.dot(554, 405, 10)
    s.mark([(458, 450), (480, 470), (500, 448), (524, 470), (546, 448), (566, 470)], 7)    # toothy mouth
    chains = [[(512, 410), (512, 520), (512, 690)],
              [(440, 510), (352, 570), (296, 640)],
              [(584, 510), (672, 570), (728, 640)],
              [(460, 690), (444, 790)],
              [(564, 690), (580, 790)]]
    return s, chains


def multilimb():
    s = Shape()
    s.ellipse(512, 650, 118, 100)                             # abdomen
    s.ellipse(512, 530, 62, 56)                               # head / thorax
    spine = [(512, 530), (512, 585), (512, 640), (512, 695)]
    legs = []
    spread = ((150, -150), (190, -70), (200, 30), (165, 120))
    for side in (-1, 1):
        for i, (dx, dy) in enumerate(spread):
            sx, sy = spine[i]
            knee = (512 + side * (dx - 15), sy + dy - 55)
            foot = (512 + side * (dx + 40), sy + dy + 70 - 25 * i)
            legs.append([(sx, sy), knee, foot])
    for p in legs:
        s.line(p, 24)
    s.dot(490, 520, 8)
    s.dot(534, 520, 8)
    chains = [spine] + legs
    return s, chains


def hands():
    s = Shape()
    s.poly([(440, 850), (584, 850), (598, 730), (426, 730)])  # wrist
    s.poly(circle_pts(512, 655, 128, 108))                    # palm
    finger = [((430, 610), (414, 480), (404, 392)), ((486, 596), (484, 440), (486, 340)),
              ((542, 596), (556, 446), (568, 360)), ((596, 612), (620, 508), (640, 432))]
    for f in finger:
        s.line(list(f), 50)
    thumb = [(406, 700), (342, 650), (300, 574)]
    s.line(thumb, 52)
    chains = [[(512, 800), (512, 690)]] + [[(f[0][0], f[0][1]), (f[1][0], f[1][1]), (f[2][0], f[2][1])] for f in finger] + [thumb]
    return s, chains


def vehicle():
    s = Shape()
    k, cx, cy = 1.35, 524, 690

    def f(p):
        return (cx + (p[0] - cx) * k, cy - 60 + (p[1] - cy) * k)

    body = [(300, 700), (300, 628), (352, 600), (420, 596), (468, 520), (600, 520), (668, 596), (738, 612), (748, 700)]
    s.poly([f(p) for p in body])
    for hub in ((400, 712), (646, 712)):
        x, y = f(hub)
        s.ellipse(x, y, 76 * k, 76 * k)
    s.mark([f(p) for p in [(480, 536), (456, 592), (600, 592), (588, 536), (480, 536)]], 8)    # windows
    s.mark([f(p) for p in [(528, 540), (528, 594)]], 7)
    s.mark([f(p) for p in [(330, 650), (720, 650)]], 6)                                       # beltline
    for hub in ((400, 712), (646, 712)):
        x, y = f(hub)
        s.dot(x, y, 28)
    h1, h2, mid, top, fr, rr = f((400, 712)), f((646, 712)), f((520, 650)), f((528, 560)), f((330, 650)), f((720, 650))
    chains = [[h1, mid, h2], [mid, top], [h1, fr], [h2, rr]]
    return s, chains


def other():
    s = Shape()
    cx, cy, r = 512, 590, 215
    hexa = [(cx, cy - r), (cx + r * 0.87, cy - r / 2), (cx + r * 0.87, cy + r / 2), (cx, cy + r), (cx - r * 0.87, cy + r / 2),
            (cx - r * 0.87, cy - r / 2)]
    s.poly(hexa)                                              # a generic prop: faceted block
    s.mark([(cx, cy - r), (cx, cy), (cx + r * 0.87, cy - r / 2)], 6)
    s.mark([(cx, cy), (cx - r * 0.87, cy - r / 2)], 6)
    s.mark([(cx, cy), (cx, cy + r)], 6)
    chains = [[(cx - 110, cy + 80), (cx, cy), (cx + 110, cy + 80)], [(cx, cy), (cx, cy - 120)]]
    return s, chains


ICONS = {"Bird": bird, "Serpent": serpent, "Monster": monster, "Multilimb": multilimb, "Hands": hands,
         "Vehicle": vehicle, "Other": other}


def build(name):
    img = cube_background()
    shape, chains = ICONS[name]()
    render_shape(img, shape)
    skeleton(img, [[(int(x), int(y)) for x, y in c] for c in chains])
    return img.resize((SIZE, SIZE), Image.LANCZOS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "static", "Icons_png"))
    ap.add_argument("--preview", default="")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    thumbs = []
    for name in ICONS:
        im = build(name)
        path = os.path.join(args.out, name + ".png")
        im.save(path, optimize=True)
        print(name, im.size, os.path.getsize(path) // 1024, "KB")
        thumbs.append(im)
    if args.preview:
        sheet = Image.new("RGBA", (4 * 400, 2 * 400), (40, 40, 50, 255))
        for i, im in enumerate(thumbs):
            sheet.alpha_composite(im.resize((400, 400), Image.LANCZOS), ((i % 4) * 400, (i // 4) * 400))
        sheet.convert("RGB").save(args.preview, quality=88)


if __name__ == "__main__":
    main()

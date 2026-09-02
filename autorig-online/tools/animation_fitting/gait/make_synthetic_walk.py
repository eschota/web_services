# -*- coding: utf-8 -*-
"""Synthetic continuous side-view walk with the horse_2 palette: 97 frames, 25 fps,
period 31.4 frames (non-integer), lateral 4-beat order hn->fn->hf->ff, duty 0.6,
treadmill drift in stance, standing start (first 16 frames accelerate)."""
import subprocess, sys, shutil
import numpy as np
import cv2

OUT = sys.argv[1]
T, W, H = 97, 768, 448
P = 31.4
DUTY = 0.6
RAMP = 16
STRIDE = 60.0

def lin_to_srgb(c):
    c = np.asarray(c, float)
    return np.where(c <= 0.0031308, c * 12.92, 1.055 * np.power(np.clip(c, 1e-9, None), 1 / 2.4) - 0.055)

PAL = {"body": (0.46, 0.50, 0.56), "fore_near": (0.00, 0.85, 1.00), "fore_far": (0.12, 0.22, 1.00),
       "hind_near": (1.00, 0.72, 0.02), "hind_far": (1.00, 0.08, 0.55)}
BGR = {k: tuple(int(x) for x in np.round(lin_to_srgb(v) * 255)[::-1]) for k, v in PAL.items()}

LEGS = {  # phase of touchdown, hip, ground y, lift, thickness
    "hind_near": dict(ph=0.00, hip=(500, 250), gy=375, lift=40, th=18),
    "fore_near": dict(ph=0.25, hip=(300, 250), gy=375, lift=40, th=18),
    "hind_far":  dict(ph=0.50, hip=(490, 238), gy=357, lift=32, th=14),
    "fore_far":  dict(ph=0.75, hip=(290, 238), gy=357, lift=32, th=14),
}

def smoothstep(v):
    v = np.clip(v, 0, 1); return v * v * (3 - 2 * v)

rng = np.random.default_rng(3)
# global phase with standing start
phase = np.zeros(T)
for t in range(1, T):
    phase[t] = phase[t - 1] + smoothstep((t - 1) / RAMP) / P
amp = smoothstep(np.arange(T) / RAMP)

def hoof_pos(leg, t):
    L = LEGS[leg]
    u = (phase[t] - L["ph"]) % 1.0
    hx = L["hip"][0]
    if u < DUTY:  # stance: slide backward
        x = hx + STRIDE / 2 - (u / DUTY) * STRIDE
        y = L["gy"]
    else:
        v = (u - DUTY) / (1 - DUTY)
        x = hx - STRIDE / 2 + smoothstep(v) * STRIDE
        y = L["gy"] - L["lift"] * np.sin(np.pi * v)
    x = hx + (x - hx) * amp[t]
    y = L["gy"] + (y - L["gy"]) * amp[t]
    return x + rng.normal(0, 0.3), y + rng.normal(0, 0.3)

def draw_leg(img, leg, t):
    L = LEGS[leg]
    hx, hy = L["hip"]
    x, y = hoof_pos(leg, t)
    u = (phase[t] - L["ph"]) % 1.0
    bend = 0 if u < DUTY else 22 * np.sin(np.pi * (u - DUTY) / (1 - DUTY)) * amp[t]
    kx = (hx + x) / 2 + (bend if leg.startswith("hind") else -bend)
    ky = (hy + y) / 2 + 8
    pts = np.array([[hx, hy], [kx, ky], [x, y]], np.int32)
    cv2.polylines(img, [pts], False, BGR[leg], L["th"], cv2.LINE_AA)
    cv2.circle(img, (int(round(x)), int(round(y))), L["th"] // 2 + 1, BGR[leg], -1, cv2.LINE_AA)

frames = []
for t in range(T):
    img = np.zeros((H, W, 3), np.uint8)
    grad = np.linspace(200, 175, H)[:, None]
    img[:] = np.stack([grad + 8, grad + 4, grad], -1).astype(np.uint8)  # bluish-gray like renders
    cv2.rectangle(img, (0, 330), (W, H), (205, 205, 200), -1)              # floor (BGR)
    for leg in ("fore_far", "hind_far"):
        draw_leg(img, leg, t)
    bob = 4 * np.sin(4 * np.pi * phase[t]) * amp[t]
    cv2.ellipse(img, (400, int(round(232 + bob))), (175, 62), 0, 0, 360, BGR["body"], -1, cv2.LINE_AA)
    cv2.ellipse(img, (215, int(round(150 + bob))), (40, 70), 20, 0, 360, BGR["body"], -1, cv2.LINE_AA)  # neck/head
    for leg in ("hind_near", "fore_near"):
        draw_leg(img, leg, t)
    frames.append(img)

ff = shutil.which("ffmpeg")
cmd = [ff, "-y", "-loglevel", "error", "-f", "rawvideo", "-pix_fmt", "bgr24", "-s", f"{W}x{H}", "-r", "25",
       "-i", "-", "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "14", OUT]
p = subprocess.Popen(cmd, stdin=subprocess.PIPE)
for f in frames:
    p.stdin.write(f.tobytes())
p.stdin.close(); p.wait()
print("wrote", OUT, "period", P, "ramp", RAMP, "expected steady ~", RAMP)

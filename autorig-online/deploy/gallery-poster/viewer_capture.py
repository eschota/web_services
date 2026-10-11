"""Viewer preview capture on a farm box (farm job «viewer_preview», owner 2026-10-12).

Owner: «записывать видео пролёт вокруг бегущей модели в 30 фпс … на ферме … запуская браузер вместе с текущим
вьювером … дождавшись пока он полностью со всеми эффектами загрузится … вертикальным под shorts … балансируй качество».

Runs in the farm job's out dir (cwd). Opens the real V3 Unity viewer page with ui=0 in Chrome (Playwright, the box's
installed Chrome, GPU/WebGL), waits for the model and the scene, plays the rig's locomotion clip (Running → Walking →
its own locomotion → the first clip), orbits the camera and records the WebGL canvas at 30 fps (captureStream +
MediaRecorder), then encodes 1080x1920 H.264 (+faststart, size-capped for Telegram/og:video) and a poster frame.

    python viewer_capture.py --url <viewer url> [--seconds 7] [--fps 30] [--max-mb 8]
Writes preview.mp4, poster.jpg, meta.json. Standard library + Playwright (installed on first use with pip --user).
"""
import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import time

W, H = 1080, 1920
CLIP_ORDER = ("running", "run", "walking", "walk")
LOCO = ("gallop", "trot", "canter", "slither", "crawl", "fly", "swim", "hop", "run", "walk")


def ensure_playwright():
    try:
        import playwright  # noqa: F401
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "--user", "--quiet", "playwright==1.48.0"],
                       check=True, timeout=600)
        import site
        sys.path.append(site.getusersitepackages())


def ffmpeg_bin():
    found = shutil.which("ffmpeg")
    winget = os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\WinGet\Links\ffmpeg.exe")
    if found or os.path.exists(winget):
        return found or winget
    try:                                         # a box without ffmpeg (f15): the static build of imageio-ffmpeg
        import imageio_ffmpeg
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "--user", "--quiet", "imageio-ffmpeg"],
                       check=True, timeout=600)
        import site
        sys.path.append(site.getusersitepackages())
        import imageio_ffmpeg
    return imageio_ffmpeg.get_ffmpeg_exe()


def chrome_path():
    for p in (r"C:\Program Files\Google\Chrome\Application\chrome.exe",
              r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
              r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"):
        if os.path.exists(p):
            return p
    return None


INIT = r"""
(() => {
  window.__ev = [];
  const prev = (window.autorigUnity && window.autorigUnity.onEvent) || null;
  window.autorigUnity = window.autorigUnity || {};
  window.__last = {};
  window.autorigUnity.onEvent = function (n, j) { window.__ev.push([n, Date.now()]); window.__last[n] = j; if (prev) try { prev(n, j); } catch (e) {} };
})();
"""

RECORD = r"""
async ({seconds, fps, clip, yaw0, sweep, pitch, capture}) => {
  const api = window.autorigUnity;
  const canvas = document.getElementById('unity-canvas') || document.querySelector('canvas');
  if (capture) {                         // the viewer's capture mode drives clip, scene, quality and the orbit itself
    const stream = canvas.captureStream(fps);
    const mime = MediaRecorder.isTypeSupported('video/webm;codecs=vp9') ? 'video/webm;codecs=vp9' : 'video/webm';
    const rec = new MediaRecorder(stream, {mimeType: mime, videoBitsPerSecond: 24000000});
    const chunks = [];
    rec.ondataavailable = e => { if (e.data && e.data.size) chunks.push(e.data); };
    rec.start(250);
    await new Promise(r => setTimeout(r, seconds * 1000));
    rec.stop();
    await new Promise(r => { rec.onstop = r; });
    const buf = new Uint8Array(await new Blob(chunks, {type: mime}).arrayBuffer());
    let s = ''; const step = 0x8000;
    for (let i = 0; i < buf.length; i += step) s += String.fromCharCode.apply(null, buf.subarray(i, i + step));
    return {b64: btoa(s), w: canvas.width, h: canvas.height, mime, capture: true, clip_now: (api.state || {}).clip};
  }
  try { api.SetAutoCamera(0); } catch (e) {}
  if (clip) { try { api.PlayClip(clip); } catch (e) {} }
  await new Promise(r => setTimeout(r, 800));
  // the rig layer plays the clip; show it as the plain model: full shading channel, no bone overlay
  try { api.SetChannel('0'); } catch (e) {}
  if ((api.state || {}).bones) { try { api.send('HotKey', 'F8'); } catch (e) {} }
  await new Promise(r => setTimeout(r, 1500));
  let cam = {};
  try { cam = JSON.parse(window.__last.camera || '{}') || {}; } catch (e) {}
  const dist = cam.dist || cam.distance || null;
  const stream = canvas.captureStream(fps);
  const mime = MediaRecorder.isTypeSupported('video/webm;codecs=vp9') ? 'video/webm;codecs=vp9' : 'video/webm';
  const rec = new MediaRecorder(stream, {mimeType: mime, videoBitsPerSecond: 24000000});
  const chunks = [];
  rec.ondataavailable = e => { if (e.data && e.data.size) chunks.push(e.data); };
  let t0 = performance.now() + 900;
  const orbit = setInterval(() => {
    const k = Math.max(0, Math.min(1, (performance.now() - t0) / (seconds * 1000)));
    const c = {yaw: yaw0 + sweep * k, pitch: pitch, seconds: 0.12};
    if (dist) c.dist = dist;
    try { api.SetCamera(c); } catch (e) {}
  }, 50);
  try { const c0 = {yaw: yaw0, pitch: pitch, seconds: 0.6}; if (dist) c0.dist = dist; api.SetCamera(c0); } catch (e) {}
  await new Promise(r => setTimeout(r, 900));                // the orbit's first pose before the first frame
  rec.start(250);
  await new Promise(r => setTimeout(r, seconds * 1000));
  rec.stop();
  clearInterval(orbit);
  await new Promise(r => { rec.onstop = r; });
  const blob = new Blob(chunks, {type: mime});
  const buf = new Uint8Array(await blob.arrayBuffer());
  let s = ''; const step = 0x8000;
  for (let i = 0; i < buf.length; i += step) s += String.fromCharCode.apply(null, buf.subarray(i, i + step));
  return {b64: btoa(s), w: canvas.width, h: canvas.height, mime, cam, layer: (api.state || {}).layer, clip_now: (api.state || {}).clip, bones: (api.state || {}).bones};
}
"""


def pick_clip(clips):
    names = [c if isinstance(c, str) else (c or {}).get("name", "") for c in clips or []]
    low = {n.lower(): n for n in names if n}
    for want in CLIP_ORDER:
        if want in low:
            return low[want]
    for n in names:
        if any(w in n.lower() for w in LOCO) and n.lower() != "rig_check":
            return n
    real = [n for n in names if n and n.lower() != "rig_check"]
    return (real or names or [None])[0]


def encode(src, out_mp4, poster, seconds, fps, max_mb):
    ff = ffmpeg_bin()
    vf = f"scale={W}:{H}:force_original_aspect_ratio=increase:flags=lanczos,crop={W}:{H},fps={fps}"
    for crf in (20, 23, 26, 29):
        subprocess.run([ff, "-v", "error", "-y", "-i", src, "-vf", vf, "-t", str(seconds), "-c:v", "libx264",
                        "-preset", "slow", "-crf", str(crf), "-profile:v", "high", "-pix_fmt", "yuv420p",
                        "-maxrate", "12M", "-bufsize", "24M", "-movflags", "+faststart", "-an", out_mp4],
                       check=True, timeout=900)
        if os.path.getsize(out_mp4) <= max_mb * 1024 * 1024:
            break
    subprocess.run([ff, "-v", "error", "-y", "-ss", str(min(2.0, seconds / 3)), "-i", out_mp4, "-frames:v", "1",
                    "-q:v", "2", poster], check=True, timeout=120)
    return crf


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--seconds", type=float, default=7.0)
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--max-mb", type=float, default=8.0)
    ap.add_argument("--load-timeout", type=float, default=180.0)
    ap.add_argument("--settle", type=float, default=8.0, help="seconds after the model loads, for scene and effects")
    a = ap.parse_args()
    meta = {"url": a.url, "started": time.time()}
    ensure_playwright()
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            executable_path=chrome_path(), headless=True,
            args=["--headless=new", "--use-angle=d3d11", "--enable-gpu", "--ignore-gpu-blocklist",
                  "--enable-unsafe-swiftshader", "--autoplay-policy=no-user-gesture-required",
                  "--disable-background-timer-throttling", "--disable-renderer-backgrounding"])
        ctx = browser.new_context(viewport={"width": W, "height": H}, device_scale_factor=1)
        ctx.add_init_script(INIT)
        page = ctx.new_page()
        page.goto(a.url, wait_until="domcontentloaded", timeout=120000)
        t0 = time.time()
        loaded = False
        # scene-gate.js (viewer load order V3): black until the presentation (voxels, erosion, bones) ends, then the
        # session's scene; window.autorigEnv.t marks presented / scene_go / unblack.
        capture = False
        while time.time() - t0 < a.load_timeout:
            if page.evaluate("() => !!window.autorigCaptureReady"):     # the viewer's capture=1 mode (blueprint-r3)
                loaded = capture = True
                break
            env_t = page.evaluate("() => (window.autorigEnv && window.autorigEnv.t) || {}")
            if "capture=1" in a.url and time.time() - t0 < a.load_timeout * 0.8:
                time.sleep(1.0)
                continue
            if env_t.get("unblack") is not None or (env_t.get("scene_go") is not None
                                                    and time.time() - t0 > (env_t["scene_go"] / 1000.0) + 20):
                loaded = True
                break
            time.sleep(1.0)
        meta["load_seconds"] = round(time.time() - t0, 1)
        meta["env_t"] = page.evaluate("() => (window.autorigEnv && window.autorigEnv.t) || {}")
        meta["scene"] = page.evaluate("() => (window.autorigEnv && window.autorigEnv.scene) || null")
        if not loaded:
            raise SystemExit("viewer did not reach its scene: " + json.dumps(meta)[:500])
        if not capture:
            try:
                page.wait_for_load_state("networkidle", timeout=int(a.settle * 4000))
            except Exception:  # noqa: BLE001
                pass
            time.sleep(a.settle)
        meta["capture_mode"] = capture
        st = page.evaluate("() => (window.autorigUnity && window.autorigUnity.state) || {}")
        clip = pick_clip(st.get("clips") or [])
        meta.update(clip=clip, clips=[c if isinstance(c, str) else c.get("name") for c in (st.get("clips") or [])][:30])
        res = page.evaluate(RECORD, {"seconds": a.seconds + 0.6, "fps": a.fps, "clip": clip, "yaw0": -40.0,
                                     "sweep": 300.0, "pitch": 12.0, "capture": capture})
        meta.update(canvas=[res["w"], res["h"]], mime=res["mime"], camera=res.get("cam"), layer=res.get("layer"),
                    clip_now=res.get("clip_now"))
        with open("raw.webm", "wb") as f:
            f.write(base64.b64decode(res["b64"]))
        browser.close()
    meta["crf"] = encode("raw.webm", "preview.mp4", "poster.jpg", a.seconds, a.fps, a.max_mb)
    os.remove("raw.webm")
    meta["bytes"] = os.path.getsize("preview.mp4")
    meta["seconds_total"] = round(time.time() - meta["started"], 1)
    with open("meta.json", "w") as f:
        json.dump(meta, f, indent=1)
    print(json.dumps(meta)[:800])


if __name__ == "__main__":
    main()

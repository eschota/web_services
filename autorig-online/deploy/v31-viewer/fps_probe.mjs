// Viewer V3.1 presentation probe (2026-10-17): frame rate and long tasks of the task page while the stages play, in a
// separate headless Chrome on this machine (GPU, no background throttling), driven over the DevTools protocol.
//
//   node fps_probe.mjs <url> [seconds=40] [port=9333]
//
// Prints one JSON line: per 5 s window the frame rate of the page (requestAnimationFrame of the top window, which the
// Unity iframe shares), the long tasks (> 50 ms) of the top page and of the viewer iframe, the [v31-gate] decisions,
// the scene gate's timings (autorigEnv.t) and the WebGL renderer.
import { spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import path from 'node:path';

const [url, secArg, portArg, modeArg] = process.argv.slice(2);
const MODE = modeArg || '';   // 'warm3d': load once to warm the cache, load again and replay in 3D once Unity is in
const SECONDS = Number(secArg || 40), PORT = Number(portArg || 9333);
const CHROME = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
const HEADED = process.env.HEADED === '1';   // a real window (headless WebGL readback distorts Unity frame rates)
const dir = mkdtempSync(path.join('R:/autorig/.work', 'v31chrome-'));
const chrome = spawn(CHROME, [`--remote-debugging-port=${PORT}`, `--user-data-dir=${dir}`, ...(HEADED ? ['--window-position=40,40', '--new-window'] : ['--headless=new']),
  '--window-size=1600,900', '--disable-background-timer-throttling', '--disable-renderer-backgrounding',
  '--disable-backgrounding-occluded-windows', '--enable-gpu', '--use-angle=d3d11', '--ignore-gpu-blocklist',
  '--no-first-run', '--no-default-browser-check', '--mute-audio', 'about:blank'], { stdio: 'ignore' });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ws, id = 0;
const waiters = new Map();
const errors = [];
async function connect() {
  for (let i = 0; i < 50; i++) {
    try {
      const list = await (await fetch(`http://127.0.0.1:${PORT}/json/list`)).json();
      const page = list.find((t) => t.type === 'page');
      if (page) { ws = new WebSocket(page.webSocketDebuggerUrl); break; }
    } catch (e) { /* not up yet */ }
    await sleep(200);
  }
  await new Promise((r) => ws.addEventListener('open', r, { once: true }));
  ws.addEventListener('message', (m) => {
    const d = JSON.parse(m.data);
    if (d.id && waiters.has(d.id)) { waiters.get(d.id)(d); waiters.delete(d.id); }
    else if (d.method === 'Runtime.exceptionThrown') errors.push((d.params.exceptionDetails.exception || {}).description || d.params.exceptionDetails.text);
    else if (d.method === 'Runtime.consoleAPICalled' && (d.params.type === 'error' || /v31-gate|scene-gate/.test(JSON.stringify(d.params.args).slice(0, 200))))
      errors.push(d.params.type + ' ' + d.params.args.map((a) => a.value || a.description || '').join(' ').slice(0, 300));
  });
}
function cdp(method, params = {}) {
  const i = ++id;
  ws.send(JSON.stringify({ id: i, method, params }));
  return new Promise((r) => waiters.set(i, r));
}
async function evaluate(expr) {
  const r = await cdp('Runtime.evaluate', { expression: expr, awaitPromise: true, returnByValue: true });
  return r.result && r.result.result ? r.result.result.value : r;
}
const INSTALL = `(() => {
  const lt = window.__lt = [];
  try { new PerformanceObserver((l) => l.getEntries().forEach((e) => lt.push([Math.round(e.startTime), Math.round(e.duration)]))).observe({ type: 'longtask', buffered: true }); } catch (e) {}
  if (window.top === window) {
    const fr = window.__fr = [];
    const tick = (t) => { fr.push(t); requestAnimationFrame(tick); };
    requestAnimationFrame(tick);
  }
})();`;
try {
  await connect();
  await cdp('Page.enable');
  await cdp('Runtime.enable');
  await cdp('Page.addScriptToEvaluateOnNewDocument', { source: INSTALL });
  const out = { url, mode: MODE, windows: [] };
  if (MODE === 'warm3d' || MODE === 'steps3d') {
    await cdp('Page.navigate', { url });
    await sleep(45000);
    await cdp('Page.navigate', { url: 'about:blank' });
    await sleep(500);
  }
  await cdp('Page.navigate', { url });
  if (MODE === 'warm3d') {
    for (let i = 0; i < 120; i++) {
      await sleep(250);
      const ok = await evaluate(`(() => { try { const w = document.getElementById('tv3-viewer').contentWindow; const v = w.document.getElementById('veil'); return !!(v && v.classList.contains('off') && window.autorigV31 && window.autorigV31.kinds && window.autorigV31.kinds().length && window.autorigV31.unity && window.autorigV31.unity().u3d); } catch (e) { return false; } })()`);
      if (ok) { out.replay3d_at = await evaluate(`(() => { window.autorigV31.play(0); return Math.round(performance.now()); })()`); break; }
    }
  }
  if (MODE === 'steps3d') {
    // warm, then hold the 3D replay at given steps and measure each for 5 s
    for (let i = 0; i < 160; i++) {
      await sleep(250);
      const ok = await evaluate(`(() => { try { const w = document.getElementById('tv3-viewer').contentWindow; const v = w.document.getElementById('veil'); return !!(v && v.classList.contains('off') && window.autorigV31.unity().u3d && window.autorigV31.kinds().length); } catch (e) { return false; } })()`);
      if (ok) break;
    }
    await evaluate(`(() => { window.autorigV31.play(0); window.autorigV31.seek(0); return 1; })()`);
    out.steps = [];
    for (const k of ['idle', 'vox', 'vox96', 'erosion', 'frame', 'part', 'part9', 'cls', 'bones', 'weights', 'pose', 'qa']) {
      const at = await evaluate(`(() => { const ks = window.autorigV31.kinds(); let n;
        if ('${k}' === 'part9') n = ks.lastIndexOf('part') + 1; else if ('${k}' === 'idle') n = 0; else if ('${k}' === 'vox96') n = ks.lastIndexOf('vox') + 1; else n = ks.indexOf('${k}') + 1;
        window.autorigV31.seek(n); return n; })()`);
      await sleep(1500);
      const m = await evaluate(`new Promise((r) => { const fr = []; const t0 = performance.now(); (function L(t) { fr.push(t); if (t - t0 < 4000) requestAnimationFrame(L); else { const g = []; for (let i = 1; i < fr.length; i++) g.push(fr[i] - fr[i - 1]); g.sort((a, b) => a - b); r({ fps: Math.round(fr.length / 4 * 10) / 10, p95: Math.round(g[Math.floor(g.length * 0.95)] || 0) }); } })(t0); })`);
      out.steps.push({ step: k, at, ...m, u: await evaluate(`JSON.stringify(window.autorigV31.unity().sent).slice(0, 300)`) });
    }
  }
  for (let s = 5; s <= SECONDS; s += 5) {
    await sleep(5000);
    const w = await evaluate(`(() => {
      const now = performance.now(), fr = window.__fr || [];
      const win = fr.filter((t) => now - t < 5000);
      let gaps = []; for (let i = 1; i < win.length; i++) gaps.push(win[i] - win[i - 1]);
      gaps.sort((a, b) => a - b);
      const f = document.getElementById('tv3-viewer');
      let lti = []; try { lti = (f.contentWindow.__lt || []).filter((x) => now - (x[0] + f.contentWindow.performance.timeOrigin - performance.timeOrigin) < 5000); } catch (e) {}
      const ltt = (window.__lt || []).filter((x) => now - x[0] < 5000);
      const V = window.autorigV31 || {};
      let env = null; try { env = f.contentWindow.autorigEnv; } catch (e) {}
      return { t: Math.round(now), fps: Math.round(win.length / 5 * 10) / 10, p95_frame_ms: gaps.length ? Math.round(gaps[Math.floor(gaps.length * 0.95)]) : null,
        max_frame_ms: gaps.length ? Math.round(gaps[gaps.length - 1]) : null,
        longtasks_top: ltt.length, longtask_top_ms: ltt.reduce((a, x) => a + x[1], 0),
        longtasks_viewer: lti.length, longtask_viewer_ms: lti.reduce((a, x) => a + x[1], 0),
        u3d: V.unity ? V.unity().replay3d : null, step: V.idx != null ? V.idx + '/' + (V.kinds ? V.kinds().length : '?') : null, mode: V.mode || null,
        gate_open: V.gate ? V.gate().open : null, scene: env ? (env.scene || null) : null, env_t: env ? env.t : null };
    })()`);
    out.windows.push(w);
    if (process.env.SHOTS) {
      const shot = await cdp('Page.captureScreenshot', { format: 'jpeg', quality: 70 });
      const { writeFileSync, mkdirSync } = await import('node:fs');
      mkdirSync(process.env.SHOTS, { recursive: true });
      if (shot.result) writeFileSync(path.join(process.env.SHOTS, `t${String(s).padStart(3, '0')}.jpg`), Buffer.from(shot.result.data, 'base64'));
    }
  }
  out.gate = await evaluate(`(window.autorigV31 && window.autorigV31.gateLog || []).map((r) => r.t + ' ' + r.what + ' · ' + r.why)`);
  out.renderer = await evaluate(`(() => { try { const g = document.createElement('canvas').getContext('webgl2'); const x = g.getExtension('WEBGL_debug_renderer_info'); return g.getParameter(x.UNMASKED_RENDERER_WEBGL); } catch (e) { return String(e); } })()`);
  out.v31_t = await evaluate(`window.autorigV31 ? window.autorigV31.t : null`);
  out.viewer = await evaluate(`(() => { try { const w = document.getElementById('tv3-viewer').contentWindow; const vs = w.autorigUnity && w.autorigUnity.viewerState ? w.autorigUnity.viewerState() : null; return { src: w.location.href, veil: (w.document.getElementById('veil') || {}).className, ready: vs && vs.ready, rigged: vs && vs.rigged, hold: w.autorigEnvHold }; } catch (e) { return String(e); } })()`);
  out.big = await evaluate(`(() => { const f = document.getElementById('tv3-viewer').contentWindow; const rows = [];
    for (const [who, w] of [['top', window], ['viewer', f]]) { try { w.performance.getEntriesByType('resource').forEach((e) => { if (e.transferSize > 60000 || e.encodedBodySize > 60000) rows.push([who, Math.round(e.startTime), Math.round(e.duration), Math.round((e.encodedBodySize || e.transferSize) / 1024) + ' KB', e.name.slice(0, 140)]); }); } catch (x) {} }
    return rows.sort((a, b) => a[1] - b[1]); })()`);
  out.console = errors.slice(-40);
  console.log(JSON.stringify(out));
} finally {
  try { ws && ws.close(); } catch (e) { /* closed */ }
  chrome.kill();
}

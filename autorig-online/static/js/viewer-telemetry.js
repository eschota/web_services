// Viewer telemetry (2026-10-17). Owner: «просто сделай чтобы вьювер логировал каждую сессию фпс в разных стадиях на
// сервер». The task page samples its frame rate with requestAnimationFrame (the Unity viewer iframe is same-origin and
// shares the frame loop) in 1 s windows, tags each window with the stage on screen and sends the windows in batches to
// POST /api/mt/v31/telemetry (every 15 s, and with sendBeacon when the page goes away). Stored per day as JSONL on the
// server (autorig-v31-viewer, /srv/autorig/data/var/viewer_telemetry/); read-out GET /api/mt/v31/telemetry/summary.
//
// A window: [t_s, stage, frames, fps_mean, fps_p5, fps_min, longest_ms]. Stages:
//   page_open                 before the viewer or the presentation shows anything
//   pres2d:<step> / pres3d:<step> / live2d:<step> / live3d:<step>   the V3.1 presentation (task-v31-stages.js): the
//                             2D splat or the Unity 3D replay, per step (voxels, erosion, frame, parts, class, bones,
//                             weights, pose, layers, clips, qa, done)
//   unity_loading             the presentation is over, the Unity build or the mesh is still loading
//   viewer_black              the viewer's black stage, the model in, the environment scene not started yet
//   scene_loading             the gate is open, the environment scene is coming in
//   scene_clip / scene_idle   the scene is in, a clip plays / is paused
//   viewer                    a viewer without the V3.1 presentation (no live log, a picture task …)
// Only the ids the viewer already uses go out (task, run, a random session id); the person is identified on the server
// from the same cookies as the viewer settings. Hidden tabs are not sampled. Off: ?vtel=0.
(function () {
  'use strict';
  const Q = new URLSearchParams(location.search);
  if (Q.get('vtel') === '0' || window.__autorigTelemetry) return;
  const BUILD = 'vtel-20261017.1';
  const ENDPOINT = '/api/mt/v31/telemetry';
  const T0 = performance.now();
  const sid = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()).slice(2) + Date.now()).replace(/-/g, '').slice(0, 24);
  const taskId = String(Q.get('id') || '').toLowerCase();
  const viewer = () => document.getElementById('tv3-viewer');
  const vwin = () => { try { const v = viewer(); return v && v.contentWindow; } catch (e) { return null; } };
  const T = window.__autorigTelemetry = { build: BUILD, sid, windows: [], sent: 0, batches: 0, errors: 0, stage: () => stage() };

  // ------------------------------------------------------------------ device class (once)
  function device() {
    let gpu = '';
    try {
      const c = document.createElement('canvas');
      const g = c.getContext('webgl2') || c.getContext('webgl');
      const x = g && g.getExtension('WEBGL_debug_renderer_info');
      gpu = x ? String(g.getParameter(x.UNMASKED_RENDERER_WEBGL) || '') : (g ? String(g.getParameter(g.RENDERER) || '') : 'no-webgl');
      const lose = g && g.getExtension('WEBGL_lose_context');
      if (lose) lose.loseContext();
    } catch (e) { gpu = 'error'; }
    const mobile = /Mobi|Android|iPhone|iPad/i.test(navigator.userAgent) || (matchMedia('(pointer:coarse)').matches && Math.min(screen.width, screen.height) < 820);
    const ua = navigator.userAgentData && navigator.userAgentData.brands ? navigator.userAgentData.brands.filter((b) => !/Not/i.test(b.brand)).map((b) => `${b.brand} ${b.version}`).slice(-1)[0] : '';
    return {
      gpu: gpu.slice(0, 160), cores: navigator.hardwareConcurrency || null, memory_gb: navigator.deviceMemory || null,
      screen: `${screen.width}x${screen.height}@${Math.round((devicePixelRatio || 1) * 100) / 100}`,
      viewport: `${innerWidth}x${innerHeight}`, mobile: !!mobile, browser: ua || (navigator.userAgent.match(/(Firefox|Edg|Chrome|Safari)\/[\d.]+/) || [''])[0],
      platform: (navigator.userAgentData && navigator.userAgentData.platform) || navigator.platform || '',
    };
  }
  const DEV = device();

  // ------------------------------------------------------------------ the stage on screen
  const ev = { sceneLoaded: false, sceneAt: 0 };
  function hook() {
    const w = vwin();
    if (!w || w.__vtelHook) return;
    try {
      w.__vtelHook = true;
      w.addEventListener('autorig-unity', (e) => {
        const d = e.detail || {};
        if (d.name === 'baked_scene') {
          let data = null;
          try { data = d.json ? JSON.parse(d.json) : null; } catch (x) { data = null; }
          if (data && data.action === 'loaded') { ev.sceneLoaded = true; ev.sceneAt = performance.now(); }
        } else if (d.name === 'environment') {
          let data = null;
          try { data = d.json ? JSON.parse(d.json) : null; } catch (x) { data = null; }
          if (data && data.state === 'own') { ev.sceneLoaded = true; ev.sceneAt = performance.now(); }
        }
      });
    } catch (e) { /* cross-origin */ }
  }
  function stage() {
    const V = window.autorigV31;
    const w = vwin();
    let env = null, vs = null;
    try { env = w && w.autorigEnv; } catch (e) { env = null; }
    const si = V && typeof V.stageInfo === 'function' ? V.stageInfo() : null;
    const gate = window.autorigSceneGate;
    if (si && (si.replay || (si.live && !(gate && gate.open)))) {
      const pre = si.live && !si.replay ? 'live' : 'pres';
      const three = si.replay3d || (si.docked && !si.map);
      return `${pre}${three ? '3d' : '2d'}:${si.kind || 'start'}`;
    }
    const went = env && env.t && env.t.scene_go != null;
    let veil = false;
    try { const v = w && w.document.getElementById('veil'); veil = !!(v && v.classList.contains('off')); } catch (e) { veil = false; }
    if (si && !veil && !went) return 'unity_loading';     // the presentation is over, the Unity build / mesh still coming
    if (gate && !gate.open && si) return 'viewer_black';
    if (went && !ev.sceneLoaded) {
      try { vs = w.autorigUnity && w.autorigUnity.viewerState ? w.autorigUnity.viewerState() : null; } catch (e) { vs = null; }
      const busy = vs && vs.loading && Object.keys(vs.loading).length;
      if (busy || performance.now() - (T0 + env.t.scene_go) < 30000) return 'scene_loading';
    }
    if (went || ev.sceneLoaded) {
      if (!vs) { try { vs = w.autorigUnity && w.autorigUnity.viewerState ? w.autorigUnity.viewerState() : null; } catch (e) { vs = null; } }
      return vs && vs.rigged && !vs.paused ? 'scene_clip' : 'scene_idle';
    }
    let veilOff = false;
    try { const veil = w && w.document.getElementById('veil'); veilOff = !!(veil && veil.classList.contains('off')); } catch (e) { veilOff = false; }
    if (V && V.mode === 'none') return veilOff ? 'viewer' : 'page_open';
    if (!veilOff && !(si && si.steps)) return 'page_open';
    return si ? 'viewer_black' : 'viewer';
  }

  // ------------------------------------------------------------------ the sampler: 1 s windows of rAF intervals
  let last = 0, frames = [], winStart = 0, curStage = '', hookTick = 0;
  function tick(now) {
    requestAnimationFrame(tick);
    if (document.visibilityState !== 'visible') { last = 0; frames = []; winStart = 0; return; }
    if (last) frames.push(now - last);
    last = now;
    if (!winStart) { winStart = now; curStage = stage(); return; }
    if (now - winStart < 1000) return;
    if ((hookTick++ & 3) === 0) hook();
    close(now);
  }
  function close(now) {
    if (frames.length) {
      const span = now - winStart;
      const sorted = frames.slice().sort((a, b) => a - b);
      const p95 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
      const longest = sorted[sorted.length - 1];
      T.windows.push([Math.round((winStart - T0) / 100) / 10, curStage, frames.length,
        Math.round(frames.length / span * 10000) / 10, Math.round(1000 / p95 * 10) / 10, Math.round(1000 / longest * 10) / 10, Math.round(longest)]);
    }
    frames = [];
    winStart = now;
    curStage = stage();
  }
  requestAnimationFrame(tick);

  // ------------------------------------------------------------------ batches
  let gateSent = 0;
  function payload(final) {
    const V = window.autorigV31;
    const w = vwin();
    let unityBuild = '', env = null;
    try {
      const s = w && [...w.document.scripts].map((x) => x.src).find((u) => /Build\/[0-9a-f]+\.loader\.js/.test(u));
      unityBuild = s ? s.replace(/^.*\/unity\/([^/]+)\/Build\/([0-9a-f]{8}).*$/, '$1/$2') : '';
      env = w && w.autorigEnv ? { t: w.autorigEnv.t, gate: w.autorigEnv.gate || null, scene: w.autorigEnv.scene || null } : null;
    } catch (e) { /* the viewer is loading */ }
    let runId = (V && V.run) || '';
    if (!runId) { try { runId = new URL(viewer().src, location.origin).searchParams.get('run') || ''; } catch (e) { runId = ''; } }
    const gl = (V && V.gateLog) || [];
    const gate = gl.slice(gateSent).map((r) => [r.t, r.what, String(r.why || '').slice(0, 120)]);
    gateSent = gl.length;
    const win = T.windows.splice(0);
    return {
      schema: 'autorig.viewer-telemetry/1', sid, seq: T.batches, final: !!final, task_id: /^[0-9a-f-]{36}$/.test(taskId) ? taskId : '',
      run_id: /^[0-9a-f]{20}$/.test(runId) ? runId : '', page: location.pathname.slice(0, 40),
      builds: { telemetry: BUILD, page: (window.autorigTaskPage && window.autorigTaskPage.build) || '', v31: (V && V.build) || '', unity: unityBuild },
      flags: { fixture: Q.get('v31fixture') === '1', sim: Number(Q.get('v31sim') || 0) || 0, v31_off: Q.get('v31') === '0' },
      device: T.batches === 0 || final ? DEV : undefined, uptime_s: Math.round((performance.now() - T0) / 100) / 10,
      v31: V ? { mode: V.mode, t: V.t, steps: V.kinds ? V.kinds().length : null } : null, env, gate, windows: win,
    };
  }
  function send(final) {
    const body = payload(final);
    if (!body.windows.length && !body.gate.length && T.batches > 0 && !final) return;
    T.batches += 1;
    const json = JSON.stringify(body);
    T.sent += body.windows.length;
    T.last = body;
    if (final && navigator.sendBeacon) {
      try { if (navigator.sendBeacon(ENDPOINT, new Blob([json], { type: 'application/json' }))) return; } catch (e) { /* fall through */ }
    }
    fetch(ENDPOINT, { method: 'POST', credentials: 'same-origin', keepalive: true, headers: { 'Content-Type': 'application/json' }, body: json })
      .catch(() => { T.errors += 1; });
  }
  setInterval(() => send(false), 15000);
  let gone = false;
  const bye = () => { if (gone) return; gone = true; if (winStart) close(performance.now()); send(true); };
  addEventListener('pagehide', bye);
  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'hidden') { if (winStart) close(performance.now()); send(false); } });
}());

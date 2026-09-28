/*
 * Autosave, and save-then-reload when the site is updated (owner, 2026-09-28).
 *
 * - Autosave: 10 s after the last change to a graph that has a link, a PUT
 *   with its revision (never a new graph). A stale revision (changed in
 *   another tab / by an agent) is not overwritten: the canvas is kept as a
 *   local backup and saving stops.
 * - Update detection: /api/ai/build (backend start time + live editor build)
 *   polled every 15 s, the editor_outdated refusal, or the backend being away.
 *   Then: wait until nobody is typing or dragging (idle 3 s), show
 *   "Site updated — reloading in 5 s (Cancel)", save, wait for the backend,
 *   reload, and put the viewport and the selection back.
 */
(function () {
  'use strict';
  const H = () => window.AINodesHost;
  const KEY_VIEW = 'autoreload.view';
  const backupKey = id => 'autoreload.backup.' + id;
  let baseline = null;
  let lastChange = 0;
  let saving = false;
  let stopped = false;
  let known = null;       // {start, build} the page started with
  let pending = false;    // a reload is wanted
  let lastInput = Date.now();
  let dragging = false;

  const store = {
    get: (k, session) => { try { return (session ? sessionStorage : localStorage).getItem(k); } catch (_) { return null; } },
    set: (k, v, session) => { try { (session ? sessionStorage : localStorage).setItem(k, v); return true; } catch (_) { return false; } },
    del: (k, session) => { try { (session ? sessionStorage : localStorage).removeItem(k); } catch (_) { /* ignore */ } }
  };

  function toast(text) { if (H() && H().toast) H().toast(text); }

  // ---------------------------------------------------------------- autosave
  async function save(reason) {
    const host = H();
    if (!host || !host.graphId() || saving || stopped) return true;
    const signature = host.signature();
    if (signature === baseline) return true;
    if (host.stale()) { keepBackup(signature); stopped = true; return false; }
    saving = true;
    try {
      const {response, data} = await host.save();
      if (response.ok) { baseline = host.signature(); return true; }
      const code = ((data || {}).detail || {}).error_string;
      if (code === 'graph_stale' || response.status === 409 || response.status === 428) {
        keepBackup(signature);
        stopped = true;
        toast('This graph was changed elsewhere; your latest changes are kept in this browser (not saved over it).');
      }
      return false;
    } catch (_) {
      return false;   // offline / restarting: try again later
    } finally {
      saving = false;
    }
  }

  function keepBackup(signature) {
    const host = H();
    if (!host || !host.graphId()) return;
    store.set(backupKey(host.graphId()), JSON.stringify({at: Date.now(), graph: signature}));
  }

  setInterval(() => {
    const host = H();
    if (!host || !host.graphId() || stopped) return;
    let signature;
    try { signature = host.signature(); } catch (_) { return; }
    if (baseline === null) { baseline = signature; return; }
    if (signature !== baseline) {
      if (!lastChange) lastChange = Date.now();
      if (Date.now() - lastChange >= 10000 && idle(3000)) { lastChange = 0; save('autosave'); }
    } else {
      lastChange = 0;
    }
  }, 4000);

  // ---------------------------------------------------------------- idleness
  ['keydown', 'pointerdown', 'wheel'].forEach(type => document.addEventListener(type, () => { lastInput = Date.now(); }, true));
  document.addEventListener('pointerdown', () => { dragging = true; }, true);
  document.addEventListener('pointerup', () => { dragging = false; lastInput = Date.now(); }, true);
  function typing() {
    const el = document.activeElement;
    return !!(el && (/^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName) || el.isContentEditable));
  }
  function idle(ms) { return !dragging && !typing() && Date.now() - lastInput >= ms && !document.querySelector('dialog[open]'); }

  // --------------------------------------------------------- update detection
  async function probe() {
    try {
      const r = await fetch('/api/ai/build', {cache: 'no-store'});
      if (!r.ok) return null;
      return await r.json();
    } catch (_) { return null; }
  }

  // Every script / stylesheet version the live /nodes page links: any static
  // release changes it, even one that leaves ai-nodes.js alone.
  async function pageFingerprint() {
    try {
      const r = await fetch('/nodes', {cache: 'no-store'});
      if (!r.ok) return null;
      return ((await r.text()).match(/\?v=[0-9a-zA-Z_.-]+/g) || []).join('|');
    } catch (_) { return null; }
  }
  let ownFingerprint = null;   // the page as served when this tab started watching

  async function watch() {
    const now = await probe();
    if (now && now.started_at_float) {
      if (!known) known = {start: now.started_at_float, build: now.build_string};
      else if (now.started_at_float !== known.start || now.build_string !== known.build) { requestReload(); return; }
      const page = await pageFingerprint();
      if (page && ownFingerprint === null) ownFingerprint = page;
      else if (page && page !== ownFingerprint) requestReload();
    }
  }
  setInterval(watch, 20000);
  setTimeout(watch, 2000);
  // The editor was refused as outdated (ai-nodes.js shows its banner too).
  new MutationObserver(() => { if (document.getElementById('editor-outdated')) requestReload(); })
    .observe(document.body || document.documentElement, {childList: true});

  function requestReload() {
    if (pending) return;
    pending = true;
    const waitIdle = () => {
      if (!idle(3000)) { setTimeout(waitIdle, 1000); return; }
      countdown(5);
    };
    waitIdle();
  }

  function countdown(seconds) {
    const old = document.getElementById('autoreload-toast');
    if (old) old.remove();
    const bar = document.createElement('div');
    bar.id = 'autoreload-toast';
    bar.setAttribute('role', 'status');
    bar.style.cssText = 'position:fixed;z-index:10002;left:50%;bottom:24px;transform:translateX(-50%);display:flex;gap:12px;align-items:center;' +
      'padding:10px 12px 10px 16px;border-radius:12px;background:#1e1b4b;border:1px solid #818cf8;color:#fff;font:600 14px system-ui;box-shadow:0 8px 30px rgba(0,0,0,.5)';
    bar.innerHTML = '<span></span><button type="button" style="padding:6px 14px;border:1px solid #a5b4fc;border-radius:8px;background:transparent;color:#fff;cursor:pointer">Cancel</button>';
    document.body.appendChild(bar);
    let left = seconds, cancelled = false;
    const label = bar.querySelector('span');
    bar.querySelector('button').addEventListener('click', () => {
      cancelled = true; bar.remove(); pending = false;
      known = null;   // re-arm on the next detection
    });
    const tick = async () => {
      if (cancelled) return;
      if (!idle(0) && typing()) { label.textContent = 'Site updated — waiting until you finish typing…'; setTimeout(tick, 1000); return; }
      if (left > 0) { label.textContent = 'Site updated — reloading in ' + left + ' s'; left -= 1; setTimeout(tick, 1000); return; }
      label.textContent = 'Saving your graph…';
      await save('reload');
      label.textContent = 'Waiting for the site…';
      for (let i = 0; i < 120 && !cancelled; i++) {
        const ok = await probe();
        if (ok && ok.started_at_float) break;
        await new Promise(r => setTimeout(r, 1000));
      }
      if (cancelled) return;
      rememberView();
      location.reload();
    };
    tick();
  }

  // ------------------------------------------------ viewport across the reload
  function rememberView() {
    const host = H();
    if (!host) return;
    store.set(KEY_VIEW, JSON.stringify({graph: host.graphId(), view: host.viewport(), selection: host.selection(),
                                        at: Date.now()}), true);
  }
  function restoreView() {
    const raw = store.get(KEY_VIEW, true);
    if (!raw) return;
    store.del(KEY_VIEW, true);
    let saved;
    try { saved = JSON.parse(raw); } catch (_) { return; }
    if (!saved || Date.now() - saved.at > 120000) return;
    const apply = tries => {
      const host = H();
      if (!host || String(host.graphId()) !== String(saved.graph)) { if (tries < 40) setTimeout(() => apply(tries + 1), 250); return; }
      // After the page's own fit-to-window has run.
      setTimeout(() => {
        host.setViewport(saved.view);
        if (saved.selection && saved.selection.length) host.select(saved.selection);
        toast('Reloaded after a site update — your graph was saved.');
      }, 1800);
    };
    apply(0);
  }
  function offerBackup() {
    const host = H();
    if (!host || !host.graphId()) { setTimeout(offerBackup, 1000); return; }
    const raw = store.get(backupKey(host.graphId()));
    if (!raw) return;
    const saved = JSON.parse(raw);
    const bar = document.createElement('div');
    bar.style.cssText = 'position:fixed;z-index:10002;left:50%;bottom:24px;transform:translateX(-50%);display:flex;gap:10px;align-items:center;' +
      'padding:10px 12px 10px 16px;border-radius:12px;background:#1e1b4b;border:1px solid #818cf8;color:#fff;font:600 13px system-ui;box-shadow:0 8px 30px rgba(0,0,0,.5)';
    bar.innerHTML = '<span></span><button type="button" data-a="restore">Restore</button><button type="button" data-a="discard">Discard</button>';
    bar.querySelector('span').textContent = 'Your unsaved changes (' + new Date(saved.at).toLocaleTimeString() +
      ') were kept — the saved graph was newer, so they were not written over it.';
    bar.querySelectorAll('button').forEach(b => { b.style.cssText = 'padding:6px 12px;border:1px solid #a5b4fc;border-radius:8px;background:transparent;color:#fff;cursor:pointer'; });
    bar.addEventListener('click', event => {
      const action = event.target.dataset && event.target.dataset.a;
      if (action === 'restore') {
        try { host.load(JSON.parse(saved.graph)); toast('Your kept changes are on the canvas. Use "Duplicate graph" to save them as a copy.'); } catch (_) { toast('The kept copy could not be read.'); }
      }
      if (action === 'restore' || action === 'discard') { store.del(backupKey(host.graphId())); bar.remove(); }
    });
    document.body.appendChild(bar);
  }
  window.AIAutoReload = {
    backup: () => { const host = H(); const raw = host && store.get(backupKey(host.graphId())); return raw ? JSON.parse(raw).graph : null; },
    discardBackup: () => { const host = H(); if (host) store.del(backupKey(host.graphId())); },
    saveNow: () => save('manual'),
    simulateUpdate: () => requestReload()
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', () => { restoreView(); setTimeout(offerBackup, 3000); });
  else { restoreView(); setTimeout(offerBackup, 3000); }
})();
// release marker 1790607101

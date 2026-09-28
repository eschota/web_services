/*
 * Node cards tinted by their status (owner, 2026-09-29): rendering green with
 * a soft pulse, queued / waiting yellow, failed red, stale / skipped /
 * cancelled a grey-amber outline, done neutral with a short teal flash. Read
 * from each node's own status line (.nstate) — class swaps only, no layout.
 */
(function () {
  'use strict';
  const style = document.createElement('style');
  style.textContent = `
  .drawflow .drawflow-node.st-running { box-shadow: 0 0 0 2px rgba(74,222,128,.75), 0 0 22px rgba(74,222,128,.28) !important; animation: st-pulse 1.6s ease-in-out infinite; }
  .drawflow .drawflow-node.st-running .nhead { background: linear-gradient(90deg, rgba(74,222,128,.22), rgba(74,222,128,0) 70%); }
  .drawflow .drawflow-node.st-queued { box-shadow: 0 0 0 2px rgba(250,204,21,.65), 0 0 16px rgba(250,204,21,.18) !important; }
  .drawflow .drawflow-node.st-queued .nhead { background: linear-gradient(90deg, rgba(250,204,21,.18), rgba(250,204,21,0) 70%); }
  .drawflow .drawflow-node.st-failed { box-shadow: 0 0 0 2px rgba(251,113,133,.8), 0 0 18px rgba(251,113,133,.25) !important; }
  .drawflow .drawflow-node.st-failed .nhead { background: linear-gradient(90deg, rgba(251,113,133,.24), rgba(251,113,133,0) 70%); }
  .drawflow .drawflow-node.st-stale { box-shadow: 0 0 0 1.5px rgba(214,178,110,.55) !important; }
  .drawflow .drawflow-node.st-done-flash { box-shadow: 0 0 0 2px rgba(45,212,191,.8), 0 0 20px rgba(45,212,191,.3) !important; transition: box-shadow 2s ease-out; }
  .drawflow .drawflow-node.st-done { transition: box-shadow 2s ease-out; }
  @keyframes st-pulse { 50% { box-shadow: 0 0 0 2px rgba(74,222,128,.45), 0 0 8px rgba(74,222,128,.12); } }
  @media (prefers-reduced-motion: reduce) { .drawflow .drawflow-node.st-running { animation: none; } }`;
  document.head.appendChild(style);

  const CLASSES = ['st-running', 'st-queued', 'st-failed', 'st-stale', 'st-done'];
  function statusOf(state) {
    const text = (state.textContent || '').toLowerCase();
    const cls = state.className || '';
    if (/failed|error|could not|refused/.test(text) || /\bfailed\b/.test(cls)) return 'st-failed';
    if (/queued|waiting|in this run|sending|submitting|retrying/.test(text)) return 'st-queued';
    if (/rendering|running|x9 · \d\/9|uploading|starting/.test(text) || (/\brunning\b/.test(cls) && !/queued|waiting/.test(text))) return 'st-running';
    if (/changed|stale|skipped|cancelled|replaced|interrupted|press render/.test(text)) return 'st-stale';
    if (/done|cached|continued|ready/.test(text) || /\bdone\b/.test(cls)) return 'st-done';
    return '';
  }
  function paint(node) {
    const state = node.querySelector('.nstate');
    if (!state) return;
    const next = statusOf(state);
    const had = CLASSES.find(c => node.classList.contains(c)) || '';
    if (had === next) return;
    CLASSES.forEach(c => { if (c !== next) node.classList.remove(c); });
    if (next) node.classList.add(next);
    // A render that has just finished glows teal for a moment.
    if (next === 'st-done' && (had === 'st-running' || had === 'st-queued')) {
      node.classList.add('st-done-flash');
      setTimeout(() => node.classList.remove('st-done-flash'), 60);
    }
  }
  const pending = new Set();
  let frame = 0;
  function schedule(node) {
    if (!node) return;
    pending.add(node);
    if (!frame) frame = requestAnimationFrame(() => { frame = 0; pending.forEach(paint); pending.clear(); });
  }
  function start() {
    const canvas = document.getElementById('canvas');
    if (!canvas) { setTimeout(start, 500); return; }
    canvas.querySelectorAll('.drawflow-node').forEach(schedule);
    new MutationObserver(records => records.forEach(r => {
      const target = r.target.nodeType === 1 ? r.target : r.target.parentElement;
      const node = target && target.closest && target.closest('.drawflow-node');
      if (node && (r.type !== 'childList' || (target.closest('.nstate')) || r.addedNodes.length)) schedule(node);
    })).observe(canvas, {subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['class']});
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();
})();

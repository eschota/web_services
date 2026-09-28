/*
 * One undo / redo for every graph edit in /nodes (owner, 2026-09-28).
 *
 * Steps are snapshots of the graph DOCUMENT (nodes, params, links, layout —
 * never run results or status): a change is recorded once the graph has been
 * still for 0.6 s, so a slider drag or a burst of typing is one step, and
 * nothing is recorded while a text field has focus (its own Ctrl+Z stays the
 * browser's text undo; the edit becomes a step on blur). 50 steps.
 * Ctrl+Z undo · Ctrl+Shift+Z / Ctrl+Y redo · a new edit clears redo. Undo keeps
 * the current results and the view, and the graph is left dirty, so autosave
 * stores the undone state.
 */
(function () {
  'use strict';
  const LIMIT = 50;
  const H = () => window.AINodesHost;
  const undoStack = [];   // {doc: string, label}
  const redoStack = [];
  let current = null;     // the document as last recorded
  let candidate = null, candidateAt = 0;
  let applying = false;

  function doc() {
    const host = H();
    if (!host || !host.signature) return null;
    try { return host.signature(); } catch (_) { return null; }
  }
  function typing() {
    const el = document.activeElement;
    return !!(el && (/^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName) || el.isContentEditable) && el.closest && el.closest('#canvas'));
  }

  function describe(before, after) {
    try {
      const a = JSON.parse(before), b = JSON.parse(after);
      const na = (a.nodes || []).length, nb = (b.nodes || []).length;
      const la = (a.links || []).length, lb = (b.links || []).length;
      if (nb > na) return nb - na === 1 ? 'add node' : 'add ' + (nb - na) + ' nodes';
      if (nb < na) return na - nb === 1 ? 'delete node' : 'delete ' + (na - nb) + ' nodes';
      if (lb > la) return 'connect';
      if (lb < la) return 'disconnect';
      const byId = new Map((a.nodes || []).map(n => [n.id, n]));
      let moved = 0, edited = 0;
      (b.nodes || []).forEach(n => {
        const o = byId.get(n.id);
        if (!o) return;
        if (o.x !== n.x || o.y !== n.y) moved += 1;
        if (JSON.stringify(o.params) !== JSON.stringify(n.params) || o.value !== n.value) edited += 1;
      });
      if (edited) return edited === 1 ? 'edit node' : 'edit ' + edited + ' nodes';
      if (moved) return moved === 1 ? 'move node' : 'move ' + moved + ' nodes';
      return 'edit graph';
    } catch (_) { return 'edit'; }
  }

  // Record: a changed document that has held still for 0.6 s.
  setInterval(() => {
    if (applying || typing()) return;
    const now = doc();
    if (now === null) return;
    if (current === null) { current = now; return; }
    if (now === current) { candidate = null; return; }
    if (now !== candidate) { candidate = now; candidateAt = Date.now(); return; }
    if (Date.now() - candidateAt < 700) return;
    undoStack.push({doc: current, label: describe(current, now)});
    if (undoStack.length > LIMIT) undoStack.shift();
    redoStack.length = 0;
    current = now;
    candidate = null;
  }, 500);

  function restore(target) {
    const host = H();
    if (!host || !host.full || !host.load) return false;
    applying = true;
    try {
      const live = host.full();
      const next = JSON.parse(target);
      next.results = live.results || {};   // results are not part of undo
      const view = host.viewport ? host.viewport() : null;
      const selection = host.selection ? host.selection() : [];
      host.load(next);
      // loadGraph fits the view; put the user's view back after it.
      [60, 400, 1200].forEach(ms => setTimeout(() => { if (view && host.setViewport) host.setViewport(view); }, ms));
      if (selection && selection.length && host.select) setTimeout(() => host.select(selection), 450);
    } finally {
      setTimeout(() => { current = doc(); candidate = null; applying = false; }, 1300);
    }
    return true;
  }

  function undo() {
    // An edit still settling is committed first, so it is what gets undone.
    const now = doc();
    if (now !== null && current !== null && now !== current) {
      undoStack.push({doc: current, label: describe(current, now)});
      current = now;
    }
    const step = undoStack.pop();
    if (!step) { toast('Nothing to undo'); return; }
    redoStack.push({doc: current, label: step.label});
    restore(step.doc);
    current = step.doc;
    toast('Undo: ' + step.label);
  }
  function redo() {
    const step = redoStack.pop();
    if (!step) { toast('Nothing to redo'); return; }
    undoStack.push({doc: current, label: step.label});
    restore(step.doc);
    current = step.doc;
    toast('Redo: ' + step.label);
  }
  function toast(text) { const host = H(); if (host && host.toast) host.toast(text); }

  document.addEventListener('keydown', event => {
    if (!(event.ctrlKey || event.metaKey) || event.altKey) return;
    const z = event.code === 'KeyZ', y = event.code === 'KeyY';
    if (!z && !y) return;
    if (typing() || (event.target && event.target.closest && event.target.closest('input, textarea, select, [contenteditable="true"]'))) return;
    if (document.querySelector('dialog[open]')) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    if (applying) return;
    if (y || (z && event.shiftKey)) redo(); else undo();
  }, true);

  window.AIUndo = {undo, redo, depth: () => ({undo: undoStack.length, redo: redoStack.length}),
                   push: () => {}};   // old per-gesture stacks: now covered by snapshots
})();

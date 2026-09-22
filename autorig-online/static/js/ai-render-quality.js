/**
 * Render quality: one graph-wide scale for every width/height a run sends.
 *
 *   preview ÷4 · fast ÷2 · normal ×1 (default) · highquality ×2
 *
 * Node params keep showing the base (normal) size; the scale is applied only
 * when a request body is built (ai-nodes.js bodyFor), so it is part of each
 * node's execution signature and results for another mode are never reused
 * for this one. Scaled sizes are rounded to a multiple of 32 — the padding
 * the render workflows (LTX, Krea, Z-Image) apply anyway — and kept inside the
 * limits the backend validates (256–2048 per side, and the padded-area cap of
 * the Avatar video preset), shrinking or growing both sides together so the
 * aspect ratio survives. A size that had to be limited says so in the node
 * badge tooltip.
 */
(function (global) {
  'use strict';

  const MODES = Object.freeze(['preview', 'fast', 'normal', 'highquality']);
  const FACTORS = Object.freeze({preview: 0.25, fast: 0.5, normal: 1, highquality: 2});
  const LABELS = Object.freeze({preview: '¼', fast: '½', normal: '1×', highquality: '2×'});
  const TITLES = Object.freeze({
    preview: 'Preview — every size ÷4',
    fast: 'Fast — every size ÷2',
    normal: 'Normal — sizes as set',
    highquality: 'High quality — every size ×2'
  });
  const MULTIPLE = 32;
  const DEFAULT_MIN = 256;
  const DEFAULT_MAX = 2048;
  // ai_avatar_video.MAX_PADDED_PIXELS: width and height padded to /32.
  const PADDED_AREA = Object.freeze({avatar_video: 524288});

  function normalize(mode) {
    mode = String(mode || '').trim().toLowerCase();
    return MODES.includes(mode) ? mode : 'normal';
  }

  function factor(mode) { return FACTORS[normalize(mode)]; }

  function padded(value) { return Math.ceil(value / MULTIPLE) * MULTIPLE; }

  /** The limits the backend enforces for a service's width/height. */
  function limitsFor(serviceId, entry) {
    const params = (entry && entry.params_array) || [];
    const find = name => params.find(item => item.name === name) || {};
    const width = find('width');
    const height = find('height');
    const number = value => (Number.isFinite(Number(value)) && value !== '' && value !== null ? Number(value) : null);
    const min = Math.max(number(width.min) ?? DEFAULT_MIN, number(height.min) ?? DEFAULT_MIN, DEFAULT_MIN);
    const max = Math.min(number(width.max) ?? DEFAULT_MAX, number(height.max) ?? DEFAULT_MAX, DEFAULT_MAX);
    return {min, max, paddedArea: PADDED_AREA[serviceId] || 0, multiple: MULTIPLE};
  }

  function roundTo(value, limits) {
    const step = limits.multiple;
    let out = Math.round(value / step) * step;
    const low = Math.ceil(limits.min / step) * step;
    const high = Math.floor(limits.max / step) * step;
    return Math.min(high, Math.max(low, out));
  }

  /**
   * Scale one width/height pair. Returns the sent size and any limit that was
   * applied ('min', 'max', 'area'); a factor of 1 returns the input untouched,
   * so normal-mode requests stay byte-identical to what they always were.
   */
  function scaleSize(width, height, mode, limits) {
    const f = factor(mode);
    const w0 = Number(width);
    const h0 = Number(height);
    if (f === 1 || !(w0 > 0) || !(h0 > 0)) return {width: w0, height: h0, limited: []};
    limits = limits || {min: DEFAULT_MIN, max: DEFAULT_MAX, paddedArea: 0, multiple: MULTIPLE};
    const limited = [];
    let w = w0 * f;
    let h = h0 * f;
    // Largest side over the cap: shrink both.
    if (Math.max(w, h) > limits.max) {
      const s = limits.max / Math.max(w, h); w *= s; h *= s; limited.push('max');
    }
    // Smallest side under the floor: grow both, as far as the cap allows.
    if (Math.min(w, h) < limits.min) {
      const s = Math.min(limits.min / Math.min(w, h), limits.max / Math.max(w, h));
      w *= s; h *= s; limited.push('min');
    }
    if (limits.paddedArea && padded(w) * padded(h) > limits.paddedArea) {
      const s = Math.sqrt(limits.paddedArea / (padded(w) * padded(h)));
      w *= s; h *= s; limited.push('area');
    }
    let rw = roundTo(w, limits);
    let rh = roundTo(h, limits);
    while (limits.paddedArea && rw * rh > limits.paddedArea) {
      if (rw >= rh && rw - limits.multiple >= limits.min) rw -= limits.multiple;
      else if (rh - limits.multiple >= limits.min) rh -= limits.multiple;
      else break;
    }
    return {width: rw, height: rh, limited};
  }

  function limitText(result, limits) {
    const words = {min: 'raised to the ' + limits.min + ' px minimum',
                   max: 'limited to ' + limits.max + ' px',
                   area: 'limited to the ' + limits.paddedArea + '-pixel preset area'};
    return (result.limited || []).map(key => words[key]).join('; ');
  }

  /** Scale a request body in place; returns what happened (or null). */
  function applyToBody(serviceId, body, mode, entry) {
    if (!body || factor(mode) === 1) return null;
    const w = Number(body.width);
    const h = Number(body.height);
    const limits = limitsFor(serviceId, entry);
    if (w > 0 && h > 0) {
      const result = scaleSize(w, h, mode, limits);
      body.width = result.width; body.height = result.height;
      return result;
    }
    // Only one side set: scale it alone within the same limits.
    ['width', 'height'].forEach(name => {
      const value = Number(body[name]);
      if (value > 0) body[name] = roundTo(Math.min(limits.max, Math.max(limits.min, value * factor(mode))), limits);
    });
    return null;
  }

  /* ------------------------------------------------------------ the UI */

  function installToolbar(options) {
    const host = options.host;
    if (!host) return null;
    const group = document.createElement('span');
    group.className = 'rq-group';
    group.setAttribute('role', 'radiogroup');
    group.setAttribute('aria-label', 'Render quality');
    const buttons = MODES.map(mode => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'rq-btn';
      button.dataset.quality = mode;
      button.textContent = LABELS[mode];
      button.title = TITLES[mode];
      button.setAttribute('role', 'radio');
      button.setAttribute('aria-label', TITLES[mode]);
      button.addEventListener('click', () => options.set(mode));
      group.appendChild(button);
      return button;
    });
    host.parentNode.insertBefore(group, host.nextSibling);
    function paint() {
      const current = normalize(options.get());
      buttons.forEach(button => {
        const on = button.dataset.quality === current;
        button.classList.toggle('on', on);
        button.setAttribute('aria-checked', on ? 'true' : 'false');
      });
    }
    paint();
    return {paint};
  }

  /** A small header badge with the size that will actually be sent. */
  function installBadges(options) {
    const canvas = options.canvas;
    if (!canvas) return null;
    function refresh() {
      const mode = normalize(options.getQuality());
      canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(element => {
        const header = element.querySelector('.nhead');
        if (!header) return;
        let badge = header.querySelector('.nqual');
        const width = element.querySelector('[data-param="width"]');
        const height = element.querySelector('[data-param="height"]');
        const id = element.id.slice(5);
        const service = (options.getMeta(id) || {}).service;
        const w = width ? Number(width.value) : 0;
        const h = height ? Number(height.value) : 0;
        if (mode === 'normal' || !service || !(w > 0) || !(h > 0)) { if (badge) badge.remove(); return; }
        const limits = limitsFor(service, options.serviceById(service));
        const result = scaleSize(w, h, mode, limits);
        if (!badge) {
          badge = document.createElement('i');
          badge.className = 'nqual';
          const anchor = header.querySelector('.node-display-mode');
          header.insertBefore(badge, anchor || null);
        }
        badge.textContent = result.width + '×' + result.height;
        badge.classList.toggle('limited', result.limited.length > 0);
        const note = limitText(result, limits);
        badge.title = TITLES[mode] + ': ' + w + '×' + h + ' → ' + result.width + '×' + result.height +
          (note ? ' (' + note + ')' : '');
      });
    }
    let queued = false;
    function schedule() {
      if (queued) return;
      queued = true;
      setTimeout(() => { queued = false; refresh(); }, 50);
    }
    canvas.addEventListener('input', event => {
      const name = event.target && event.target.dataset && event.target.dataset.param;
      if (name === 'width' || name === 'height') schedule();
    });
    canvas.addEventListener('change', event => {
      const name = event.target && event.target.dataset && event.target.dataset.param;
      if (name === 'width' || name === 'height') schedule();
    });
    new MutationObserver(records => {
      // Drawflow adds a wrapper (.parent-node) around each node.
      if (records.some(record => [...record.addedNodes].some(node => node.nodeType === 1 &&
          ((node.classList && node.classList.contains('drawflow-node')) ||
           (node.querySelector && node.querySelector('.drawflow-node')))))) schedule();
    }).observe(canvas, {childList: true, subtree: true});
    schedule();
    return {refresh: schedule};
  }

  global.AIRenderQuality = Object.freeze({
    MODES, FACTORS, LABELS, TITLES, normalize, factor, limitsFor, scaleSize, applyToBody,
    installToolbar, installBadges
  });
})(typeof window !== 'undefined' ? window : globalThis);

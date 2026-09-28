/*
 * One gate for every picture and clip the node editor shows (owner,
 * 2026-09-28: graph 73ff opened with 800+ requests — full-size PNGs from
 * history strips, X9 and list cells, eager MP4s).
 *
 * Loaded first on /nodes. Setting `src` on an <img> or <video> no longer
 * starts a download; the element loads when it is on screen, then:
 *   - pictures inside the canvas / strips get a cached thumbnail
 *     (/api/ai/thumb?w=…) sized to how they are shown; dialogs (lightbox,
 *     preview) get the original;
 *   - clips load metadata only (preload="metadata", first frame), never the
 *     whole file, unless something plays them;
 *   - at most 6 media downloads run at once; the same address is never asked
 *     for twice in a row by the gate; a 404 becomes a placeholder, not retries.
 * The gate changes only what an element DISPLAYS, never a value the graph
 * uses (owner, 2026-09-28: a 960x1472 render read as "160x245"): `src` and
 * `currentSrc` read the original address, and `naturalWidth/Height` report
 * the ORIGINAL's pixel size (from /api/ai/media-size, cached per address) —
 * a thumbnail is shown only once that size is known, else the original is.
 * An element that is never put on the page (a size probe) loads the
 * original at once.
 */
(function () {
  'use strict';
  if (!/^\/nodes(\/|$)/.test(location.pathname)) return;
  if (new URLSearchParams(location.search).get('media') === 'eager') return;   // escape hatch

  const MAX_ACTIVE = 6;
  const play = HTMLMediaElement.prototype.play;
  const THUMBABLE = /^https:\/\/(autorig\.online\/|image\.civitai\.com\/)/;
  const PICTURE = /\.(png|jpe?g|webp)(\?|#|$)/i;
  const imgSrc = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, 'src');
  const mediaSrc = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
  const imgSetAttr = Element.prototype.setAttribute;
  const ORIG = Symbol('orig');
  let active = 0;
  const queue = [];
  // A failed address is retried after 10 s: a render's file is often linked
  // a moment before it lands, and a permanent "failed" kept nodes blank until
  // a reload (owner, 2026-09-28).
  const failed = new Map();   // url -> time of failure
  const FAIL_TTL = 10000;
  const LOAD_TIMEOUT = 15000;
  const isFailed = url => { const at = failed.get(url); if (!at) return false; if (Date.now() - at > FAIL_TTL) { failed.delete(url); return false; } return true; };

  function inDialog(el) { return !!(el.closest && el.closest('dialog, .alb, #media-preview, .tf-modal, .mpick-panel')); }

  function thumbFor(el, url) {
    if (el.tagName !== 'IMG' || !THUMBABLE.test(url) || !PICTURE.test(url) || url.includes('/api/ai/thumb')) return url;
    if (inDialog(el)) return url;
    // Shown size on screen, times the pixel ratio; the canvas zoom is already in it.
    const box = el.getBoundingClientRect();
    const need = Math.max(box.width, box.height, 64) * (window.devicePixelRatio || 1);
    const w = need <= 160 ? 160 : need <= 320 ? 320 : need <= 640 ? 640 : 1024;
    return '/api/ai/thumb?w=' + w + '&url=' + encodeURIComponent(url);
  }

  function pump() {
    while (active < MAX_ACTIVE && queue.length) {
      const el = queue.shift();
      if (!el.isConnected || !el[ORIG]) continue;
      start(el);
    }
  }

  // Original pixel sizes, per address (one request each).
  const sizes = new Map();
  function originalSize(url) {
    if (!sizes.has(url)) {
      sizes.set(url, fetch('/api/ai/media-size?url=' + encodeURIComponent(url))
        .then(r => r.ok ? r.json() : null)
        .then(d => d && d.width_int && d.height_int ? {w: d.width_int, h: d.height_int} : null)
        .catch(() => null)
        // Not known yet (the file may not have landed): ask again next time.
        .then(size => { if (!size) sizes.delete(url); return size; }));
    }
    return sizes.get(url);
  }
  const SHOWN = Symbol('shown');   // the original size of a thumbnail on show

  function start(el) {
    const url = el[ORIG];
    if (isFailed(url)) {
      el.classList.add('media-missing');
      setTimeout(() => { if (el[ORIG] === url && el.isConnected) { el.classList.remove('media-missing'); queue.push(el); pump(); } }, FAIL_TTL + 50);
      return;
    }
    el.classList.remove('media-missing');
    active += 1;
    let done = false;
    const finish = ok => {
      if (done) return;
      done = true;
      active -= 1;
      if (!ok) { failed.set(url, Date.now()); el.classList.add('media-missing'); }
      pump();
    };
    if (el.tagName === 'IMG') {
      el.addEventListener('load', () => finish(true), {once: true});
      el.addEventListener('error', () => {
        // A thumbnail that failed: the original once, then give up for 10 s.
        if (el[ORIG] === url && el[SHOWN] && !el._triedOriginal) { el._triedOriginal = true; el[SHOWN] = null; imgSrc.set.call(el, url); return; }
        finish(false);
      });
      // A load that never answers frees its slot and shows the original.
      setTimeout(() => { if (!done && el[ORIG] === url) { el[SHOWN] = null; imgSrc.set.call(el, url); finish(true); } }, LOAD_TIMEOUT);
      const thumb = thumbFor(el, url);
      if (thumb === url) { el[SHOWN] = null; imgSrc.set.call(el, url); return; }
      // The thumbnail goes on only once the original's size is known, so any
      // load handler reading naturalWidth sees the real size.
      originalSize(url).then(size => {
        if (el[ORIG] !== url) return;
        el[SHOWN] = size;
        imgSrc.set.call(el, size ? thumb : url);
      });
    } else {
      if (!el.autoplay && el.preload !== 'none') el.preload = 'metadata';
      if (!el.autoplay && !el.getAttribute('preload')) el.preload = 'metadata';
      el.addEventListener('loadedmetadata', () => finish(true), {once: true});
      el.addEventListener('error', () => finish(false), {once: true});
      setTimeout(() => finish(true), LOAD_TIMEOUT);   // a slow clip never blocks the queue
      mediaSrc.set.call(el, url);
    }
  }

  const seen = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (!entry.isIntersecting) return;
      seen.unobserve(entry.target);
      const el = entry.target;
      if (!settled && !inDialog(el)) { small.add(el); return; }   // the graph is still being laid out / fitted
      if (inDialog(el) || el.autoplay) start(el);   // opened on purpose: now
      else if (el.tagName === 'VIDEO' && tooSmall(el)) small.add(el);   // a speck at this zoom: wait
      else { queue.push(el); pump(); }
    });
  }, {rootMargin: '300px'});

  // A clip drawn smaller than 140 px on screen (a zoomed-out graph) waits
  // until it is shown bigger or hovered; a picture thumbnail is cheap, a clip is not.
  const small = new Set();
  // Nothing loads during the first 1.8 s: a graph opens at one zoom and is
  // then fitted to the window, and what is a big clip before the fit is a
  // speck after it.
  let settled = false;
  setTimeout(() => { settled = true; }, 1800);
  function tooSmall(el) {
    if (el.tagName !== 'VIDEO') return false;
    const r = el.getBoundingClientRect(); return Math.max(r.width, r.height) < 140;
  }
  setInterval(() => {
    small.forEach(el => {
      if (!el.isConnected) { small.delete(el); return; }
      const r = el.getBoundingClientRect();
      const visible = r.bottom > 0 && r.right > 0 && r.top < innerHeight && r.left < innerWidth;
      if (settled && visible && !tooSmall(el)) {
        small.delete(el); queue.push(el); pump();
        if (el.autoplay || el.loop) el.addEventListener('loadedmetadata', () => { play.call(el).catch(() => {}); }, {once: true});
      }
    });
  }, 800);
  document.addEventListener('pointerover', event => {
    const el = event.target && event.target.closest && event.target.closest('video');
    if (el && small.has(el)) { small.delete(el); queue.unshift(el); pump(); }
  }, true);

  // Identical GETs of the model catalogue / settings / link resolver are asked
  // once per page (a 65-node graph asked model-settings 28 times).
  const nativeFetch = window.fetch.bind(window);
  const shared = new Map();
  window.fetch = function (input, init) {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const method = String((init && init.method) || (input && input.method) || 'GET').toUpperCase();
    if (method === 'GET' && /\/api\/ai\/(model-settings|model-catalogue|media\/resolve)\?/.test(url)) {
      const key = new URL(url, location.href).href;
      const hit = shared.get(key);
      if (hit && Date.now() - hit.at < 60000) return hit.promise.then(response => response.clone());
      const promise = nativeFetch(input, init);
      shared.set(key, {at: Date.now(), promise});
      promise.catch(() => shared.delete(key));
      return promise.then(response => response.clone());
    }
    return nativeFetch(input, init);
  };

  function defer(el, url) {
    el[ORIG] = url;
    el[SHOWN] = null;
    el._triedOriginal = false;
    failed.delete(url);   // a new result at an address that failed before: try again
    // A detached picture with an onload handler is a size probe: the original, now.
    if (el.tagName === 'IMG' && el.onload && !el.isConnected) { imgSrc.set.call(el, url); return; }
    // Watched once it is on the page. Elements are often built first and put
    // in later (or thrown away by a repaint), so a detached one is looked at
    // again; only one still detached after 3 s is a probe (a size read) and
    // loads — a clip then only its metadata.
    const waits = [0, 150, 600, 1500];
    const check = step => {
      if (el[ORIG] !== url) return;
      if (el.isConnected) {
        // A new src on an element already on screen (a finished render in a
        // node): load now, ahead of the queue — no wait for the observer.
        const r = el.getBoundingClientRect();
        const onScreen = r.width > 0 && r.bottom > -300 && r.right > -300 && r.top < innerHeight + 300 && r.left < innerWidth + 300;
        if (settled && onScreen && !(el.tagName === 'VIDEO' && tooSmall(el))) {
          seen.unobserve(el); small.delete(el);
          if (active < MAX_ACTIVE + 2) start(el); else { queue.unshift(el); pump(); }
          return;
        }
        seen.observe(el);
        return;
      }
      if (step + 1 < waits.length) { setTimeout(() => check(step + 1), waits[step + 1] - waits[step]); return; }
      if (el.tagName === 'IMG') imgSrc.set.call(el, url);
      // A clip size / length probe listens through onloadedmetadata; a clip a
      // repaint built and threw away does not, and is never fetched.
      else if (el.onloadedmetadata) { el.preload = 'metadata'; mediaSrc.set.call(el, url); }
    };
    setTimeout(() => check(0), 0);
  }

  function gated(url) {
    const text = String(url || '');
    return /^https?:/.test(text) || text.startsWith('/renderfin/') || text.startsWith('/dev/api/scratch/');
  }

  function patch(proto, desc) {
    Object.defineProperty(proto, 'src', {
      configurable: true, enumerable: true,
      get() { return this[ORIG] || desc.get.call(this); },
      set(value) {
        if (!gated(value)) { this[ORIG] = null; return desc.set.call(this, value); }
        const url = new URL(String(value), location.href).href;
        if (this[ORIG] === url) return;
        defer(this, url);
      }
    });
  }
  patch(HTMLImageElement.prototype, imgSrc);
  const natW = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, 'naturalWidth');
  const natH = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, 'naturalHeight');
  const cur = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, 'currentSrc');
  Object.defineProperty(HTMLImageElement.prototype, 'naturalWidth', {configurable: true, enumerable: true,
    get() { return this[SHOWN] ? this[SHOWN].w : natW.get.call(this); }});
  Object.defineProperty(HTMLImageElement.prototype, 'naturalHeight', {configurable: true, enumerable: true,
    get() { return this[SHOWN] ? this[SHOWN].h : natH.get.call(this); }});
  Object.defineProperty(HTMLImageElement.prototype, 'currentSrc', {configurable: true, enumerable: true,
    get() { return this[ORIG] || cur.get.call(this); }});
  patch(HTMLMediaElement.prototype, mediaSrc);
  Element.prototype.setAttribute = function (name, value) {
    if ((this instanceof HTMLImageElement || this instanceof HTMLMediaElement) && String(name).toLowerCase() === 'src' && gated(value)) {
      this.src = value;
      return;
    }
    return imgSetAttr.call(this, name, value);
  };
  // Markup written as HTML (innerHTML '<video src=…>') never passes the
  // setter: catch it as it lands, before the clip starts loading.
  const removeAttr = Element.prototype.removeAttribute;
  function adopt(el) {
    if (el[ORIG] || inDialog(el)) return;
    const url = el.getAttribute('src');
    if (!gated(url)) return;
    if (el.tagName === 'VIDEO') {
      removeAttr.call(el, 'src');
      el.preload = 'none';
      if (typeof el.load === 'function') el.load();
      defer(el, new URL(url, location.href).href);
    }
  }
  new MutationObserver(records => records.forEach(record => record.addedNodes.forEach(node => {
    if (node.nodeType !== 1) return;
    if (node.tagName === 'VIDEO') adopt(node);
    if (node.querySelectorAll) node.querySelectorAll('video[src]').forEach(adopt);
  }))).observe(document.documentElement, {childList: true, subtree: true});

  // play() on a clip that is still waiting: load it now.
  HTMLMediaElement.prototype.play = function () {
    if (this[ORIG] && !mediaSrc.get.call(this)) {
      // Autoplaying node previews ask for play() as soon as they exist; a
      // clip that is a speck on screen (or before the graph is fitted) waits.
      if (!inDialog(this) && (!settled || tooSmall(this) || !this.isConnected)) {
        if (this.isConnected) small.add(this);
        return Promise.resolve();
      }
      seen.unobserve(this); small.delete(this); mediaSrc.set.call(this, this[ORIG]);
    }
    return play.apply(this, arguments);
  };

  const style = document.createElement('style');
  style.textContent = 'img.media-missing,video.media-missing{background:repeating-linear-gradient(45deg,rgba(255,255,255,.04) 0 8px,rgba(255,255,255,.08) 8px 16px)!important;min-height:24px}';
  document.head.appendChild(style);

  window.AIMediaGovernor = {stats: () => ({active, queued: queue.length, failed: failed.size})};
})();

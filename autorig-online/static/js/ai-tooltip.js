/*
 * Custom tooltips for /nodes (owner, 2026-09-28). Every title="…" on the page
 * — sockets, node buttons, the dock, the quick bar, pickers — is moved to
 * data-tip as it appears, so the browser's small native tooltip never shows,
 * and one themed tooltip shows it instead:
 *   dark, rounded, 14 px text (13-17 px after the canvas zoom), wraps at
 *   360 px, 300 ms delay, placed above and flipped below / clamped at the
 *   edges, shown for keyboard focus too, one element so it never flickers.
 * The lightbox (.alb) keeps its own tooltip with key hints.
 */
(function () {
  'use strict';
  const DELAY = 300;
  const SKIP = 'option, optgroup, iframe, .alb, .alb *';

  const style = document.createElement('style');
  style.textContent = `
  .aitip { position: fixed; z-index: 2147483000; left: 0; top: 0; max-width: 360px; padding: 7px 10px; border-radius: 10px;
    background: rgba(16,17,28,.97); color: var(--text-primary, #f0f0f5); border: 1px solid rgba(255,255,255,.12);
    box-shadow: 0 10px 30px rgba(0,0,0,.45); font: 500 14px/1.4 Inter, system-ui, sans-serif; white-space: pre-line;
    overflow-wrap: anywhere; pointer-events: none; opacity: 0; transform: translateY(2px);
    transition: opacity .12s ease, transform .12s ease; }
  .aitip.on { opacity: 1; transform: none; }
  .aitip::after { content: ''; position: absolute; left: var(--ax, 50%); width: 8px; height: 8px; margin-left: -5px;
    background: inherit; border: inherit; border-width: 0 1px 1px 0; transform: rotate(45deg); bottom: -5px; }
  .aitip.below::after { bottom: auto; top: -5px; border-width: 1px 0 0 1px; }
  @media (prefers-reduced-motion: reduce) { .aitip { transition: none; transform: none; } }`;
  (document.head || document.documentElement).appendChild(style);

  // title -> data-tip; a button or icon with no words keeps its name for screen readers.
  function convert(element) {
    if (!element || element.nodeType !== 1 || !element.hasAttribute('title')) return;
    if (element.matches(SKIP) || element.namespaceURI === 'http://www.w3.org/2000/svg') return;
    const text = element.getAttribute('title');
    element.removeAttribute('title');
    if (!text) { element.removeAttribute('data-tip'); return; }
    element.setAttribute('data-tip', text);
    if (!element.hasAttribute('aria-label') && !element.hasAttribute('aria-labelledby') &&
        !String(element.textContent || '').trim().replace(/[^\p{L}\p{N}]/gu, '')) element.setAttribute('aria-label', text);
  }
  function sweep(root) {
    if (!root || root.nodeType !== 1) return;
    convert(root);
    root.querySelectorAll('[title]').forEach(convert);
  }
  const observer = new MutationObserver(records => {
    for (const record of records) {
      if (record.type === 'attributes') convert(record.target);
      else record.addedNodes.forEach(sweep);
    }
    if (current && current.isConnected && current.dataset.tip !== shownText && tip.classList.contains('on')) place(current);
  });
  observer.observe(document.documentElement, {subtree: true, childList: true, attributes: true, attributeFilter: ['title']});
  const start = () => sweep(document.body);
  if (document.body) start(); else document.addEventListener('DOMContentLoaded', start);

  const tip = document.createElement('div');
  tip.className = 'aitip';
  tip.setAttribute('role', 'tooltip');
  tip.id = 'aitip';
  let current = null, timer = 0, shownText = '';
  const attach = () => { if (!tip.isConnected) document.body.appendChild(tip); };

  function canvasZoom(target) {
    const canvas = target.closest && target.closest('.drawflow');
    if (!canvas) return 1;
    const matrix = getComputedStyle(canvas).transform;
    const m = /matrix\(([^,]+)/.exec(matrix || '');
    return m ? Math.abs(parseFloat(m[1])) || 1 : 1;
  }

  function place(target) {
    attach();
    shownText = target.dataset.tip || '';
    tip.textContent = shownText;
    // Readable at every canvas zoom: 14 px at 100 %, grows a little when
    // zoomed in, never under 13 px when zoomed out.
    const size = Math.max(13, Math.min(17, 14 * Math.sqrt(canvasZoom(target))));
    tip.style.fontSize = size.toFixed(1) + 'px';
    tip.style.maxWidth = Math.min(360, window.innerWidth - 16) + 'px';
    tip.classList.remove('below');
    tip.style.left = '0px'; tip.style.top = '0px';
    const box = target.getBoundingClientRect();
    const own = tip.getBoundingClientRect();
    const gap = 9;
    let top = box.top - own.height - gap;
    if (top < 6) { top = box.bottom + gap; tip.classList.add('below'); }
    if (top + own.height > window.innerHeight - 6) top = Math.max(6, window.innerHeight - own.height - 6);
    const centre = box.left + box.width / 2;
    const left = Math.min(window.innerWidth - own.width - 6, Math.max(6, centre - own.width / 2));
    tip.style.left = Math.round(left) + 'px';
    tip.style.top = Math.round(top) + 'px';
    tip.style.setProperty('--ax', Math.max(12, Math.min(own.width - 12, centre - left)) + 'px');
  }

  function show(target, immediate) {
    clearTimeout(timer);
    if (current && current !== target) current.removeAttribute('aria-describedby');
    current = target;
    const go = () => {
      if (!current || !current.isConnected || !current.dataset.tip) return hide();
      place(current);
      current.setAttribute('aria-describedby', 'aitip');
      tip.classList.add('on');
    };
    // Moving from one tip to the next while one shows: no second wait.
    if (immediate || tip.classList.contains('on')) go(); else timer = setTimeout(go, DELAY);
  }
  function hide() {
    clearTimeout(timer);
    if (current) current.removeAttribute('aria-describedby');
    current = null;
    tip.classList.remove('on');
  }
  const targetOf = node => {
    const el = node && node.closest ? node.closest('[data-tip]') : null;
    return el && !el.closest('.alb') ? el : null;
  };

  document.addEventListener('pointerover', event => {
    if (event.pointerType === 'touch') return;
    const target = targetOf(event.target);
    if (target === current) return;
    if (target) show(target); else hide();
  }, true);
  document.addEventListener('pointerout', event => {
    if (!current) return;
    const to = event.relatedTarget;
    if (to && current.contains(to)) return;   // still inside the same element: no flicker
    if (targetOf(to)) return;                 // pointerover picks the next one
    hide();
  }, true);
  document.addEventListener('focusin', event => {
    const target = targetOf(event.target);
    if (target && event.target.matches(':focus-visible')) show(target); else if (!target) hide();
  }, true);
  document.addEventListener('focusout', event => { if (current && event.target === current) hide(); }, true);
  ['pointerdown', 'wheel', 'scroll'].forEach(type => document.addEventListener(type, () => { if (current) hide(); }, {capture: true, passive: true}));
  document.addEventListener('keydown', event => { if (event.key === 'Escape' && current) hide(); }, true);
  window.addEventListener('blur', hide);

  window.AITooltip = {convert: sweep, hide};
})();

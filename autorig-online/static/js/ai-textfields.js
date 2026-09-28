/*
 * Node text fields (owner, 2026-09-28): every textarea in a node (Text in,
 * prompts, Vision question, system prompts, notes, Avoid) and every read-only
 * answer (.ntext) gets
 *   - twice the old type size (16 px at zoom 1; never under 13 px on screen
 *     when the canvas is zoomed out), comfortable line height and padding;
 *   - a fixed height inside the node (owner correction, same day: text never
 *     changes the node's size); longer text scrolls inside the field with the
 *     wheel / touchpad / keys, with no visible scrollbar and a soft fade at
 *     the edge where more text is; the wheel scrolls the text only when there
 *     is more text that way, otherwise it zooms the canvas as usual;
 *   - the full width of the node;
 *   - ⧉ copy, ⤢ open in a large editor (Esc closes, Ctrl+Enter applies), Aa
 *     monospace (off by default, remembered), and a character / word count;
 *   - editing that never drags the node: pointer presses inside the text stay
 *     in the text (caret, word / line selection, drag-select); the canvas
 *     shortcuts already stand aside while a text field has focus, so Ctrl+A /
 *     C / V / X / Z / Y are the browser's own, and so is per-field undo.
 */
(function () {
  'use strict';

  const BASE = 16;          // px at canvas zoom 1 (was 11.5)
  const MIN_ON_SCREEN = 13; // px after the canvas zoom
  const MAX_FRACTION = 0.6; // of the window height, before a field scrolls

  const style = document.createElement('style');
  style.textContent = `
  #canvas .drawflow { --tf-size: ${BASE}px; }
  .tf-wrap { position: relative; display: block; width: 100%; flex: 1 1 100%; min-width: 0; box-sizing: border-box; }
  .nparam > .tf-wrap { flex-basis: 100%; }
  .nout > .tf-wrap, .ninput > .tf-wrap { width: 100%; }
  .drawflow .drawflow-node:not([data-display-mode="small"]) textarea.tf, .drawflow .drawflow-node:not([data-display-mode="small"]) .ntext.tf {
    font-size: var(--tf-size) !important; line-height: 1.5 !important; padding: 10px 12px 22px !important;
    border-radius: 10px !important; box-sizing: border-box; width: 100%; resize: none !important;
    overflow-y: auto !important; max-height: none !important; -webkit-line-clamp: unset !important; display: block !important;
    height: 150px; min-height: 48px; scrollbar-width: none; overscroll-behavior: contain;
    font-family: Inter, system-ui, -apple-system, "Segoe UI", sans-serif;
    user-select: text; cursor: text; }
  .drawflow .drawflow-node .ntext.tf { height: 190px !important; }
  .drawflow .drawflow-node .tf::-webkit-scrollbar { display: none; }
  .drawflow .drawflow-node .tf.tf-more-down { -webkit-mask-image: linear-gradient(to bottom, #000 calc(100% - 2.2em), transparent);
    mask-image: linear-gradient(to bottom, #000 calc(100% - 2.2em), transparent); }
  .drawflow .drawflow-node .tf.tf-more-up { -webkit-mask-image: linear-gradient(to top, #000 calc(100% - 2.2em), transparent);
    mask-image: linear-gradient(to top, #000 calc(100% - 2.2em), transparent); }
  .drawflow .drawflow-node .tf.tf-more-up.tf-more-down { -webkit-mask-image: linear-gradient(to bottom, transparent, #000 2.2em, #000 calc(100% - 2.2em), transparent);
    mask-image: linear-gradient(to bottom, transparent, #000 2.2em, #000 calc(100% - 2.2em), transparent); }
  .drawflow .drawflow-node .tf.tf-mono { font-family: ui-monospace, SFMono-Regular, Consolas, monospace !important; }
  .drawflow .drawflow-node textarea.tf::placeholder { color: rgba(203,213,245,.55); opacity: 1; }
  .drawflow .drawflow-node textarea.tf:focus { outline: 2px solid rgba(129,140,248,.7); outline-offset: 0; }
  .tf-tools { position: absolute; top: 4px; right: 4px; display: flex; gap: 2px; opacity: .35; transition: opacity .15s; z-index: 2; }
  .tf-wrap:hover .tf-tools, .tf-wrap:focus-within .tf-tools { opacity: 1; }
  .tf-tools button { width: 24px; height: 22px; padding: 0; border: 0; border-radius: 6px; cursor: pointer;
    background: rgba(20,22,40,.85); color: #c7d2fe; font: 600 12px/22px system-ui; }
  .tf-tools button:hover { background: rgba(99,102,241,.45); color: #fff; }
  .tf-tools button.on { color: #fbbf24; }
  .tf-count { position: absolute; right: 10px; bottom: 4px; font: 10px/1 system-ui; color: rgba(203,213,245,.5);
    pointer-events: none; z-index: 2; }
  dialog.tf-modal { width: min(1100px, 94vw); height: min(80vh, 900px); max-width: none; max-height: none; padding: 0; border: 1px solid rgba(255,255,255,.14);
    border-radius: 14px; background: #0f1020; color: #eef; box-shadow: 0 24px 80px rgba(0,0,0,.6); }
  dialog.tf-modal::backdrop { background: rgba(4,5,14,.65); }
  .tf-modal form { display: flex; flex-direction: column; height: 100%; }
  .tf-modal header { display: flex; align-items: center; gap: 8px; padding: 10px 14px; border-bottom: 1px solid rgba(255,255,255,.08); font: 600 14px system-ui; }
  .tf-modal header span { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .tf-modal header small { color: #9aa0b5; font-weight: 400; }
  .tf-modal textarea { flex: 1; margin: 0; border: 0; resize: none; background: #0b0c18; color: inherit; padding: 16px 18px;
    font: 18px/1.55 Inter, system-ui, sans-serif; outline: none; }
  .tf-modal textarea.tf-mono { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 16px; }
  .tf-modal footer { display: flex; gap: 8px; justify-content: flex-end; padding: 10px 14px; border-top: 1px solid rgba(255,255,255,.08); }
  .tf-modal button { padding: 7px 14px; border-radius: 9px; border: 1px solid rgba(255,255,255,.18); background: rgba(255,255,255,.06); color: inherit; cursor: pointer; font: 13px system-ui; }
  .tf-modal button.tf-apply { background: #6366f1; border-color: #6366f1; color: #fff; }`;
  document.head.appendChild(style);

  let mono = false;
  try { mono = localStorage.getItem('tf.mono') === '1'; } catch (_) { /* private mode */ }

  function zoom() {
    const flow = document.querySelector('#canvas .drawflow');
    const m = flow && /matrix\(([^,]+)/.exec(getComputedStyle(flow).transform || '');
    return m ? Math.abs(parseFloat(m[1])) || 1 : 1;
  }

  let lastZoom = 0;
  function applyZoom() {
    const z = zoom();
    if (Math.abs(z - lastZoom) < 0.01) return;
    lastZoom = z;
    const size = Math.min(32, Math.max(BASE, MIN_ON_SCREEN / z));
    const flow = document.querySelector('#canvas .drawflow');
    if (flow) flow.style.setProperty('--tf-size', size.toFixed(1) + 'px');
    document.querySelectorAll('#canvas .tf').forEach(fit);
  }

  function counts(text) {
    const t = String(text || '');
    const words = (t.trim().match(/\S+/g) || []).length;
    return t.length + ' ch · ' + words + ' w';
  }

  function textOf(el) { return el.tagName === 'TEXTAREA' ? el.value : el.innerText; }

  function fit(el) {
    if (!el.isConnected) return;
    // The height is the node's, not the text's: only the edge fades and the
    // count follow the text.
    const up = el.scrollTop > 1;
    const down = el.scrollTop + el.clientHeight < el.scrollHeight - 1;
    el.classList.toggle('tf-more-up', up);
    el.classList.toggle('tf-more-down', down);
    const wrap = el.parentElement;
    const count = wrap && wrap.querySelector(':scope > .tf-count');
    if (count) count.textContent = counts(textOf(el));
  }

  function toast(message) {
    if (window.AINodesToast) return window.AINodesToast(message);
    const box = document.querySelector('.toast');
    if (box) { box.textContent = message; box.classList.add('shown'); setTimeout(() => box.classList.remove('shown'), 1600); }
  }

  function copy(text) {
    const done = () => toast('Copied ' + counts(text) + '.');
    if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text).then(done, () => legacy(text, done));
    legacy(text, done);
  }
  function legacy(text, done) {
    const area = document.createElement('textarea');
    area.value = text; area.style.cssText = 'position:fixed;opacity:0';
    document.body.appendChild(area); area.select();
    try { document.execCommand('copy'); done(); } finally { area.remove(); }
  }

  let modal = null;
  function openModal(el) {
    if (!modal) {
      modal = document.createElement('dialog');
      modal.className = 'tf-modal';
      modal.innerHTML = '<form method="dialog"><header><span></span><small></small>' +
        '<button type="button" data-a="mono" title="Monospace">Aa</button><button type="button" data-a="copy" title="Copy">⧉</button></header>' +
        '<textarea spellcheck="true"></textarea><footer><button type="button" data-a="cancel">Cancel (Esc)</button>' +
        '<button type="button" data-a="apply" class="tf-apply">Apply (Ctrl+Enter)</button></footer></form>';
      document.body.appendChild(modal);
      const area = modal.querySelector('textarea');
      const small = modal.querySelector('header small');
      area.addEventListener('input', () => { small.textContent = counts(area.value); });
      modal.addEventListener('click', event => {
        const a = event.target.closest('[data-a]');
        if (!a) return;
        if (a.dataset.a === 'cancel') modal.close();
        if (a.dataset.a === 'copy') copy(area.value);
        if (a.dataset.a === 'mono') { area.classList.toggle('tf-mono'); }
        if (a.dataset.a === 'apply') apply();
      });
      modal.addEventListener('keydown', event => {
        event.stopPropagation();
        if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); apply(); }
      });
      const apply = () => {
        const target = modal._target;
        if (target && target.tagName === 'TEXTAREA' && !target.readOnly && !target.disabled) {
          target.focus();
          // Through the editing command, so the field's own undo keeps it.
          target.select();
          if (!document.execCommand('insertText', false, modal.querySelector('textarea').value)) {
            target.value = modal.querySelector('textarea').value;
          }
          target.dispatchEvent(new Event('input', {bubbles: true}));
          target.dispatchEvent(new Event('change', {bubbles: true}));
          fit(target);
        }
        modal.close();
      };
    }
    const area = modal.querySelector('textarea');
    const editable = el.tagName === 'TEXTAREA' && !el.readOnly && !el.disabled;
    modal._target = el;
    area.value = textOf(el);
    area.readOnly = !editable;
    area.placeholder = el.placeholder || '';
    area.classList.toggle('tf-mono', mono);
    const node = el.closest('.drawflow-node');
    const label = (el.closest('label') && el.closest('label').querySelector('span')) ;
    modal.querySelector('header span').textContent = ((node && node.querySelector('.nhead b') || {}).textContent || 'Text') +
      (label ? ' · ' + label.textContent : '') + (editable ? '' : ' (read-only)');
    modal.querySelector('header small').textContent = counts(area.value);
    modal.querySelector('[data-a="apply"]').hidden = !editable;
    modal.showModal();
    area.focus();
  }

  function enhance(el) {
    if (el._tf || !el.closest('#canvas .drawflow-node')) return;
    if (el.tagName === 'TEXTAREA' && (el.type === 'hidden' || el.style.display === 'none')) return;
    el._tf = true;
    el.classList.add('tf');
    if (mono && el.tagName === 'TEXTAREA') el.classList.add('tf-mono');
    const wrap = document.createElement('div');
    wrap.className = 'tf-wrap';
    el.parentNode.insertBefore(wrap, el);
    wrap.appendChild(el);
    const tools = document.createElement('div');
    tools.className = 'tf-tools';
    tools.innerHTML = '<button type="button" data-t="mono" title="Monospace on/off (all fields)">Aa</button>' +
      '<button type="button" data-t="copy" title="Copy the text">⧉</button>' +
      '<button type="button" data-t="expand" title="Open in a large editor (Esc closes, Ctrl+Enter applies)">⤢</button>';
    wrap.appendChild(tools);
    const count = document.createElement('span');
    count.className = 'tf-count';
    wrap.appendChild(count);
    ['mousedown', 'pointerdown', 'touchstart', 'dblclick'].forEach(type => {
      tools.addEventListener(type, event => event.stopPropagation());
      // Selecting text never drags the node: the press stays in the text.
      el.addEventListener(type, event => { if (type !== 'dblclick') event.stopPropagation(); });
    });
    tools.addEventListener('click', event => {
      const b = event.target.closest('[data-t]');
      if (!b) return;
      event.preventDefault(); event.stopPropagation();
      if (b.dataset.t === 'copy') copy(textOf(el));
      if (b.dataset.t === 'expand') openModal(el);
      if (b.dataset.t === 'mono') {
        mono = !mono;
        try { localStorage.setItem('tf.mono', mono ? '1' : '0'); } catch (_) { /* private mode */ }
        document.querySelectorAll('#canvas textarea.tf').forEach(t => { t.classList.toggle('tf-mono', mono); fit(t); });
        document.querySelectorAll('.tf-tools [data-t="mono"]').forEach(x => x.classList.toggle('on', mono));
      }
    });
    tools.querySelector('[data-t="mono"]').classList.toggle('on', mono);
    if (el.tagName === 'TEXTAREA') {
      el.addEventListener('input', () => fit(el));
      // Paste as plain text is what a textarea does already; keep it that way
      // and keep the canvas out of it.
      el.addEventListener('paste', event => event.stopPropagation());
      // A value set by code (a wired prompt, a restored graph) fits too.
      const desc = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');
      Object.defineProperty(el, 'value', {configurable: true, get() { return desc.get.call(this); },
        set(v) { desc.set.call(this, v); requestAnimationFrame(() => fit(el)); }});
    } else {
      new MutationObserver(() => fit(el)).observe(el, {childList: true, characterData: true, subtree: true});
    }
    el.addEventListener('scroll', () => fit(el), {passive: true});
    // The wheel scrolls the text only when there is more text that way;
    // otherwise it reaches the canvas (zoom) untouched.
    el.addEventListener('wheel', event => {
      const dy = event.deltaY;
      const canDown = el.scrollTop + el.clientHeight < el.scrollHeight - 1;
      const canUp = el.scrollTop > 1;
      if ((dy > 0 && canDown) || (dy < 0 && canUp)) event.stopPropagation();
    }, {passive: true});
    requestAnimationFrame(() => fit(el));
  }

  function sweep(root) {
    if (!root || root.nodeType !== 1) return;
    if (root.matches && root.matches('textarea, .ntext')) enhance(root);
    root.querySelectorAll && root.querySelectorAll('textarea, .ntext').forEach(enhance);
  }

  function start() {
    const canvas = document.getElementById('canvas');
    if (!canvas) return;
    sweep(canvas);
    new MutationObserver(records => records.forEach(r => r.addedNodes.forEach(sweep)))
      .observe(canvas, {childList: true, subtree: true});
    // Zoom changes: the wheel, the zoom buttons, fit view.
    canvas.addEventListener('wheel', () => requestAnimationFrame(applyZoom), {passive: true});
    setInterval(applyZoom, 700);
    window.addEventListener('resize', () => { lastZoom = 0; applyZoom(); });
    applyZoom();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start); else start();

  window.AITextFields = {refresh: () => { lastZoom = 0; applyZoom(); }, open: openModal};
})();

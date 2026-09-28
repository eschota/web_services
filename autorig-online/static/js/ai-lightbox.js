/*
 * One lightbox for the node editor (owner, 2026-09-28): X9 cells, list /
 * segment cells, Extract Frames, the Recent gallery, the 3D viewer and plain
 * previews all open this component.
 *
 *   AILightbox.open({
 *     count: () => n,                 // live: re-read on every paint
 *     item: i => ({url, kind: 'image'|'video'|'audio'|'model'|'text', thumb,
 *                  status: 'done'|'running'|'queued'|'error', seed, locked,
 *                  used, error, text, render(stage) -> element}),
 *     title: i => 'Segment 1/3 · S1 start',
 *     start: 0,
 *     actions: {reseed(i), use(i), lock(i), post(i), copy: true, download: true,
 *               extra: [{glyph, label, key, run(i), hidden(i)}]},
 *     onShow(i)
 *   });
 *
 * Glass toolbar that hides after 2 s idle, edge arrows, title chip, close,
 * filmstrip with per-item status, wheel / pinch zoom with pan, F fit / 1:1,
 * swipe, focus trap, Esc, prefers-reduced-motion, 390 px wide screens.
 */
(function () {
  'use strict';

  const CSS = `
  .alb { position: fixed; inset: 0; width: 100vw; height: 100dvh; max-width: none; max-height: none; margin: 0; padding: 0;
    border: 0; background: rgba(6,7,14,.94); color: var(--text-primary, #f0f0f5); overflow: hidden;
    font: 13px/1.3 Inter, system-ui, sans-serif; }
  .alb::backdrop { background: rgba(4,5,12,.7); }
  .alb[open] { animation: alb-in .18s ease-out; }
  @keyframes alb-in { from { opacity: 0; } to { opacity: 1; } }
  .alb-stage { position: absolute; inset: 0 0 var(--alb-strip-h, 84px) 0; display: flex; align-items: center; justify-content: center;
    overflow: hidden; touch-action: none; cursor: default; }
  .alb.alb-zoomed .alb-stage { cursor: grab; }
  .alb.alb-panning .alb-stage { cursor: grabbing; }
  .alb-media { max-width: 100%; max-height: 100%; display: block; transform-origin: 0 0; user-select: none; -webkit-user-drag: none;
    transition: opacity .18s ease; }
  .alb-media.alb-loading { opacity: .25; }
  .alb-stage > .alb-free { width: min(1200px, 100%); height: 100%; }
  .alb-note { max-width: min(720px, 90vw); padding: 18px 20px; border-radius: 14px; background: var(--bg-card, rgba(26,26,36,.75));
    border: 1px solid rgba(255,255,255,.1); white-space: pre-wrap; color: var(--text-secondary, #9aa0b5); text-align: center; }
  .alb-note.alb-err { color: #fb7185; }
  .alb-spin { width: 34px; height: 34px; border-radius: 50%; border: 3px solid rgba(255,255,255,.15); border-top-color: var(--accent, #6366f1);
    animation: alb-spin 1s linear infinite; margin: 0 auto 10px; }
  @keyframes alb-spin { to { transform: rotate(360deg); } }

  .alb-chrome { transition: opacity .25s ease, transform .25s ease; }
  .alb.alb-idle .alb-chrome { opacity: 0; pointer-events: none; }
  .alb.alb-idle .alb-bar { transform: translate(-50%, 12px); }
  .alb.alb-idle { cursor: none; }

  .alb-glass { background: rgba(18,18,28,.62); -webkit-backdrop-filter: blur(16px) saturate(140%); backdrop-filter: blur(16px) saturate(140%);
    border: 1px solid rgba(255,255,255,.1); box-shadow: 0 10px 34px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.06); }
  .alb-title { position: absolute; top: 14px; left: 14px; max-width: calc(100vw - 90px); padding: 7px 12px; border-radius: 999px;
    font-weight: 600; font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; z-index: 3; }
  .alb-close { position: absolute; top: 12px; right: 12px; width: 40px; height: 40px; border-radius: 50%; z-index: 3;
    font-size: 22px; line-height: 1; color: inherit; cursor: pointer; display: grid; place-items: center; }
  .alb-close:hover { background: rgba(255,255,255,.14); }

  .alb-edge { position: absolute; top: 70px; bottom: calc(var(--alb-strip-h, 84px) + 70px); width: min(14vw, 120px); z-index: 2;
    border: 0; background: transparent; color: #fff; cursor: pointer; display: flex; align-items: center; padding: 0 14px; }
  .alb-edge.prev { left: 0; justify-content: flex-start; }
  .alb-edge.next { right: 0; justify-content: flex-end; }
  .alb-edge span { width: 46px; height: 46px; border-radius: 50%; display: grid; place-items: center; font-size: 20px;
    opacity: .55; transition: opacity .15s ease, transform .15s ease, background .15s ease; }
  .alb-edge:hover span, .alb-edge:focus-visible span { opacity: 1; transform: scale(1.06); background: rgba(40,40,58,.8); }
  .alb.alb-single .alb-edge { display: none; }

  .alb-bar { position: absolute; left: 50%; bottom: calc(var(--alb-strip-h, 84px) + 12px); transform: translateX(-50%);
    display: flex; align-items: center; gap: 2px; padding: 5px; border-radius: 16px; z-index: 3; max-width: calc(100vw - 20px);
    overflow-x: auto; scrollbar-width: none; }
  .alb-bar::-webkit-scrollbar { display: none; }
  .alb-sep { width: 1px; height: 22px; background: rgba(255,255,255,.12); margin: 0 4px; flex: 0 0 auto; }
  .alb-btn { position: relative; flex: 0 0 auto; min-width: 38px; height: 38px; padding: 0 9px; border: 0; border-radius: 11px;
    background: transparent; color: inherit; font: 600 13px/1 Inter, system-ui, sans-serif; cursor: pointer;
    display: inline-flex; align-items: center; justify-content: center; gap: 6px; text-decoration: none;
    transition: background .12s ease, color .12s ease; }
  .alb-btn svg { width: 18px; height: 18px; stroke: currentColor; fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
  .alb-btn:hover { background: rgba(255,255,255,.1); }
  .alb-btn:focus-visible, .alb-close:focus-visible, .alb-edge:focus-visible, .alb-thumb:focus-visible, .alb-seed:focus-visible
    { outline: 2px solid var(--accent-hover, #818cf8); outline-offset: 1px; }
  .alb-btn:disabled { opacity: .35; cursor: default; background: transparent; }
  .alb-btn.alb-primary { background: var(--accent, #6366f1); color: #fff; padding: 0 13px; }
  .alb-btn.alb-primary:hover { background: var(--accent-hover, #818cf8); }
  .alb-btn.alb-primary.alb-on { background: #16a34a; }
  .alb-btn.alb-on:not(.alb-primary) { background: rgba(245,158,11,.2); color: #fbbf24; }
  .alb-btn[hidden] { display: none; }
  .alb-count { padding: 0 6px; font-variant-numeric: tabular-nums; color: var(--text-secondary, #9aa0b5); white-space: nowrap; flex: 0 0 auto; }
  .alb-seed { flex: 0 0 auto; height: 26px; padding: 0 8px; border-radius: 8px; border: 1px solid rgba(255,255,255,.12);
    background: rgba(0,0,0,.3); color: var(--text-secondary, #9aa0b5); font: 12px ui-monospace, SFMono-Regular, Consolas, monospace; cursor: copy; }
  .alb-seed:hover { color: #fff; }
  .alb-seed.alb-locked { color: #fbbf24; border-color: rgba(251,191,36,.45); }
  .alb-seed[hidden] { display: none; }

  .alb-tip { position: fixed; z-index: 10; padding: 5px 8px; border-radius: 8px; background: rgba(10,10,18,.95); color: #fff;
    font: 500 14px Inter, system-ui, sans-serif; white-space: nowrap; pointer-events: none; border: 1px solid rgba(255,255,255,.12);
    transform: translate(-50%, -100%); opacity: 0; transition: opacity .12s ease; }
  .alb-tip.alb-show { opacity: 1; }
  .alb-tip kbd { margin-left: 6px; padding: 1px 5px; border-radius: 4px; background: rgba(255,255,255,.14); font: 11px ui-monospace, Consolas, monospace; }

  .alb-strip { position: absolute; left: 0; right: 0; bottom: 0; height: var(--alb-strip-h, 84px); display: flex; gap: 6px;
    align-items: center; padding: 8px 12px; overflow-x: auto; overflow-y: hidden; background: rgba(8,8,16,.7);
    border-top: 1px solid rgba(255,255,255,.07); scrollbar-width: thin; }
  .alb.alb-single .alb-strip { display: none; }
  .alb.alb-single { --alb-strip-h: 0px; }
  .alb-thumb { position: relative; flex: 0 0 auto; height: 100%; aspect-ratio: var(--ar, 1); border-radius: 8px; overflow: hidden; padding: 0;
    border: 2px solid transparent; background: rgba(255,255,255,.05); cursor: pointer; opacity: .6;
    transition: opacity .15s ease, border-color .15s ease, transform .15s ease; }
  .alb-thumb:hover { opacity: .95; }
  .alb-thumb.alb-cur { opacity: 1; border-color: var(--accent-hover, #818cf8); box-shadow: 0 0 0 3px var(--accent-glow, rgba(99,102,241,.3)); }
  .alb-thumb img, .alb-thumb video { width: 100%; height: 100%; object-fit: cover; display: block; pointer-events: none; }
  .alb-thumb .alb-ph { position: absolute; inset: 0; display: grid; place-items: center; font-size: 16px; color: var(--text-secondary, #9aa0b5); }
  .alb-thumb i { position: absolute; left: 3px; top: 3px; padding: 0 4px; border-radius: 4px; background: rgba(0,0,0,.6); font: 700 9px system-ui; font-style: normal; color: #fff; }
  .alb-dot { position: absolute; right: 4px; bottom: 4px; width: 8px; height: 8px; border-radius: 50%; box-shadow: 0 0 0 2px rgba(0,0,0,.55); }
  .alb-dot.done { background: #4ade80; } .alb-dot.error { background: #fb7185; }
  .alb-dot.running { background: #fbbf24; animation: alb-pulse 1s ease-in-out infinite; } .alb-dot.queued { background: #94a3b8; }
  .alb-dot.used::after { content: '✓'; position: absolute; left: -14px; top: -4px; font: 700 10px system-ui; color: #4ade80; }
  @keyframes alb-pulse { 50% { opacity: .35; } }

  .alb-toast { position: absolute; left: 50%; top: 16px; transform: translateX(-50%); padding: 7px 12px; border-radius: 10px; z-index: 4;
    opacity: 0; transition: opacity .2s ease; pointer-events: none; }
  .alb-toast.alb-show { opacity: 1; }

  .alb.alb-info-open .alb-stage { right: 360px; }
  .alb.alb-info-open .alb-edge.next { right: 360px; }
  .alb-info { position: absolute; top: 64px; right: 12px; bottom: calc(var(--alb-strip-h, 84px) + 12px); width: 336px; z-index: 3;
    border-radius: 14px; padding: 12px 14px; overflow: auto; font-size: 12.5px; line-height: 1.45; display: none; }
  .alb.alb-info-open .alb-info { display: block; }
  .alb-info h4 { margin: 0 0 6px; font-size: 13px; }
  .alb-info .alb-prompt { white-space: pre-wrap; max-height: 30vh; overflow: auto; padding: 8px 10px; border-radius: 9px;
    background: rgba(0,0,0,.35); border: 1px solid rgba(255,255,255,.08); font-size: 12.5px; }
  .alb-info table { width: 100%; border-collapse: collapse; margin-top: 8px; }
  .alb-info th { text-align: left; color: var(--text-secondary, #9aa0b5); font-weight: 500; padding: 2px 8px 2px 0; vertical-align: top; white-space: nowrap; }
  .alb-info td { padding: 2px 0; word-break: break-word; }
  .alb-info .alb-acts { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
  .alb-info .alb-acts button { padding: 6px 10px; border-radius: 8px; border: 1px solid rgba(255,255,255,.16); background: rgba(255,255,255,.06);
    color: inherit; cursor: pointer; font: 600 12px system-ui; }
  .alb-info .alb-acts button:hover { background: rgba(99,102,241,.35); }
  .alb-info .alb-muted { color: var(--text-secondary, #9aa0b5); }
  .alb-dlmenu { position: absolute; z-index: 6; min-width: 230px; padding: 6px; border-radius: 12px; display: none; }
  .alb-dlmenu.on { display: block; }
  .alb-dlmenu button { display: block; width: 100%; text-align: left; padding: 8px 10px; border: 0; border-radius: 8px; background: transparent;
    color: inherit; cursor: pointer; font: 600 13px system-ui; }
  .alb-dlmenu button:hover { background: rgba(255,255,255,.1); }
  .alb-dlmenu button:disabled { opacity: .45; cursor: default; }
  .alb-dlmenu small { display: block; font-weight: 400; color: var(--text-secondary, #9aa0b5); }
  @media (max-width: 1100px) { .alb.alb-info-open .alb-stage, .alb.alb-info-open .alb-edge.next { right: 0; }
    .alb-info { left: 12px; right: 12px; width: auto; top: auto; max-height: 45vh; } }
  @media (max-width: 600px) {
    .alb { --alb-strip-h: 64px; }
    .alb-btn { min-width: 36px; height: 36px; padding: 0 7px; }
    .alb-btn .alb-lbl { display: none; }
    .alb-btn.alb-primary { padding: 0 10px; }
    .alb-count { display: none; }
    .alb-edge { width: 48px; padding: 0 4px; }
    .alb-edge span { width: 36px; height: 36px; font-size: 16px; }
    .alb-title { max-width: calc(100vw - 76px); font-size: 12px; }
    .alb-bar { bottom: calc(var(--alb-strip-h) + 8px); }
  }
  @media (prefers-reduced-motion: reduce) {
    .alb[open], .alb-spin, .alb-dot.running { animation: none; }
    .alb-chrome, .alb-media, .alb-edge span, .alb-thumb, .alb-btn, .alb-tip, .alb-toast { transition: none; }
  }`;

  const ICON = {
    prev: '<path d="M15 18l-6-6 6-6"/>', next: '<path d="M9 18l6-6-6-6"/>',
    reseed: '<rect x="3" y="3" width="18" height="18" rx="4"/><circle cx="8.5" cy="8.5" r="1.2" fill="currentColor"/><circle cx="15.5" cy="15.5" r="1.2" fill="currentColor"/><circle cx="15.5" cy="8.5" r="1.2" fill="currentColor"/><circle cx="8.5" cy="15.5" r="1.2" fill="currentColor"/>',
    use: '<path d="M5 12.5l4.5 4.5L19 7.5"/>',
    lock: '<rect x="5" y="11" width="14" height="10" rx="2"/><path d="M8 11V8a4 4 0 0 1 8 0v3"/>',
    unlock: '<rect x="5" y="11" width="14" height="10" rx="2"/><path d="M8 11V8a4 4 0 0 1 7.5-2"/>',
    fit: '<path d="M4 9V4h5M20 9V4h-5M4 15v5h5M20 15v5h-5"/>',
    one: '<rect x="3" y="5" width="18" height="14" rx="2"/><text x="12" y="15.2" text-anchor="middle" font-size="7.5" font-weight="700" font-family="system-ui,sans-serif" fill="currentColor" stroke="none">1:1</text>',
    info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7.5v.5"/>',
    download: '<path d="M12 4v11M7 10l5 5 5-5M5 20h14"/>',
    link: '<path d="M10 14a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1 1"/><path d="M14 10a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1-1"/>',
    post: '<path d="M12 19V6M6 12l6-6 6 6"/>'
  };
  const svg = name => '<svg viewBox="0 0 24 24" aria-hidden="true">' + ICON[name] + '</svg>';
  const reduced = () => window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const isVideo = url => /\.(mp4|webm|mov|m4v)(\?|#|$)/i.test(String(url || ''));
  const isAudio = url => /\.(mp3|wav|ogg|m4a|flac)(\?|#|$)/i.test(String(url || ''));
  const thumbUrl = (url, w) => /^https:\/\/(autorig\.online\/|image\.civitai\.com\/)/.test(url) && !isVideo(url) && !isAudio(url) && !/\.(glb|gltf)(\?|#|$)/i.test(url)
    ? '/api/ai/thumb?w=' + w + '&url=' + encodeURIComponent(url) : url;

  let dialog = null, state = null;

  function copy(text) {
    if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text);
    const area = document.createElement('textarea');
    area.value = text; area.style.cssText = 'position:fixed;opacity:0';
    dialog.appendChild(area); area.select();
    try { document.execCommand('copy'); } finally { area.remove(); }
    return Promise.resolve();
  }

  function flash(message) {
    const node = dialog.querySelector('.alb-toast');
    node.textContent = message;
    node.classList.add('alb-show');
    clearTimeout(node._t);
    node._t = setTimeout(() => node.classList.remove('alb-show'), 1800);
  }

  function build() {
    const style = document.createElement('style');
    style.id = 'alb-style';
    style.textContent = CSS;
    document.head.appendChild(style);
    dialog = document.createElement('dialog');
    dialog.className = 'alb';
    dialog.setAttribute('aria-label', 'Media viewer');
    const btn = (act, icon, label, key, extra) => '<button type="button" class="alb-btn ' + (extra || '') + '" data-alb="' + act + '" data-tip="' + label +
      '" data-key="' + (key || '') + '" aria-label="' + label + (key ? ' (' + key + ')' : '') + '">' + svg(icon) +
      (act === 'use' ? '<span class="alb-lbl">Use this</span>' : '') + '</button>';
    dialog.innerHTML =
      '<div class="alb-stage"></div>' +
      '<button type="button" class="alb-edge prev alb-chrome" data-alb="prev" aria-label="Previous (←)" tabindex="-1"><span class="alb-glass">' + svg('prev').replace('<svg', '<svg width="22" height="22" style="stroke:currentColor;fill:none;stroke-width:2.2"') + '</span></button>' +
      '<button type="button" class="alb-edge next alb-chrome" data-alb="next" aria-label="Next (→)" tabindex="-1"><span class="alb-glass">' + svg('next').replace('<svg', '<svg width="22" height="22" style="stroke:currentColor;fill:none;stroke-width:2.2"') + '</span></button>' +
      '<div class="alb-title alb-glass alb-chrome" aria-live="polite"></div>' +
      '<button type="button" class="alb-close alb-glass alb-chrome" data-alb="close" aria-label="Close (Esc)">×</button>' +
      '<div class="alb-toast alb-glass" role="status"></div>' +
      '<div class="alb-bar alb-glass alb-chrome" role="toolbar" aria-label="Viewer tools">' +
        btn('prev', 'prev', 'Previous', '←') + '<span class="alb-count"></span>' + btn('next', 'next', 'Next', '→') +
        '<span class="alb-sep" data-grp="seed"></span>' +
        btn('reseed', 'reseed', 'New seed & re-render', 'R') +
        btn('use', 'use', 'Use this', 'Enter', 'alb-primary') +
        btn('lock', 'lock', 'Lock seed', 'L') +
        '<button type="button" class="alb-seed" data-alb="seed" data-tip="Copy seed"></button>' +
        '<span class="alb-sep"></span>' +
        btn('fit', 'fit', 'Fit / 1:1 (wheel or pinch to zoom)', 'F') +
        '<button type="button" class="alb-btn" data-alb="download" data-tip="Download" data-key="D" aria-label="Download (D)">' + svg('download') + '</button>' +
        btn('copy', 'link', 'Copy link', 'C') +
        btn('post', 'post', 'Post to Civitai (draft)', '') +
        '<span class="alb-extra" style="display:contents"></span>' +
        '<span class="alb-sep"></span>' + btn('info', 'info', 'Info & parameters', 'I') +
      '</div>' +
      '<div class="alb-strip alb-chrome" role="listbox" aria-label="Items"></div>' +
      '<aside class="alb-info alb-glass" aria-label="Info and parameters"></aside>' +
      '<div class="alb-dlmenu alb-glass" role="menu"></div>' +
      '<div class="alb-tip" role="tooltip"></div>';
    document.body.appendChild(dialog);
    wire();
  }

  /* ------------------------------------------------------------ zoom & pan */
  const view = {z: 1, x: 0, y: 0, fitW: 0, fitH: 0};
  function media() { return dialog.querySelector('.alb-media'); }
  function layout() {
    const node = media();
    if (!node || node.tagName === 'AUDIO') return;
    node.style.transform = 'translate(' + view.x + 'px,' + view.y + 'px) scale(' + view.z + ')';
    dialog.classList.toggle('alb-zoomed', view.z > 1.001);
  }
  function fit() {
    const node = media();
    view.z = 1;
    if (!node) return;
    node.style.transform = '';
    const stage = dialog.querySelector('.alb-stage').getBoundingClientRect();
    const box = node.getBoundingClientRect();
    view.fitW = box.width; view.fitH = box.height;
    view.x = 0; view.y = 0;
    // transform-origin is 0 0: keep the fitted picture where flexbox put it.
    node.style.position = 'absolute';
    node.style.left = (box.left - stage.left) + 'px';
    node.style.top = (box.top - stage.top) + 'px';
    node.style.width = box.width + 'px'; node.style.height = box.height + 'px';
    node.style.maxWidth = 'none'; node.style.maxHeight = 'none';
    layout();
  }
  function zoomAt(factor, clientX, clientY) {
    const node = media();
    if (!node || node.classList.contains('alb-free') || node.tagName === 'AUDIO') return;
    if (!view.fitW) fit();
    const stage = dialog.querySelector('.alb-stage').getBoundingClientRect();
    const left = parseFloat(node.style.left) || 0, top = parseFloat(node.style.top) || 0;
    const next = Math.min(16, Math.max(1, view.z * factor));
    const px = clientX - stage.left - left, py = clientY - stage.top - top;
    view.x = px - (px - view.x) * next / view.z;
    view.y = py - (py - view.y) * next / view.z;
    view.z = next;
    if (next === 1) { view.x = 0; view.y = 0; }
    layout();
  }
  function toggleFit() {
    const node = media();
    if (!node || node.classList.contains('alb-free')) return;
    if (!view.fitW) fit();
    const natural = node.naturalWidth || node.videoWidth || 0;
    const stage = dialog.querySelector('.alb-stage').getBoundingClientRect();
    if (view.z > 1.001 || !natural) { view.z = 1; view.x = 0; view.y = 0; layout(); }
    else zoomAt(Math.max(1.0001, natural / view.fitW), stage.left + stage.width / 2, stage.top + stage.height / 2);
    syncFitButton();
  }
  function syncFitButton() {
    const button = dialog.querySelector('[data-alb="fit"]');
    button.innerHTML = svg(view.z > 1.001 ? 'fit' : 'one');
    button.dataset.tip = view.z > 1.001 ? 'Fit to screen' : 'Actual size 1:1';
  }

  /* ------------------------------------------------------------- painting */
  function current() { return state.item(state.index) || {}; }
  function count() { return Math.max(0, Number(state.count()) || 0); }

  function paintStage() {
    const item = current();
    const stage = dialog.querySelector('.alb-stage');
    const old = stage.querySelector('video,audio');
    if (old) old.pause();
    stage.innerHTML = '';
    view.z = 1; view.x = 0; view.y = 0; view.fitW = 0;
    dialog.classList.remove('alb-zoomed');
    const status = item.status || 'done';
    state.shown = signature(item);
    if (status !== 'done' || (!item.url && !item.render && !item.text)) {
      const note = document.createElement('div');
      note.className = 'alb-note' + (status === 'error' ? ' alb-err' : '');
      if (status === 'running' || status === 'queued') note.innerHTML = '<div class="alb-spin"></div>';
      note.appendChild(document.createTextNode(status === 'error' ? ('Failed' + (item.error ? ': ' + item.error : '')) :
        status === 'done' ? 'Nothing to show' : (status === 'queued' ? 'Queued…' : 'Rendering…') + (item.seed ? '  seed ' + item.seed : '')));
      stage.appendChild(note);
      return;
    }
    if (item.render) {
      const host = document.createElement('div');
      host.className = 'alb-media alb-free';
      stage.appendChild(host);
      Promise.resolve(item.render(host)).catch(error => { host.textContent = String(error && error.message || error); });
      return;
    }
    if (item.kind === 'text' || (!item.url && item.text)) {
      const note = document.createElement('div');
      note.className = 'alb-note';
      note.style.textAlign = 'left';
      note.textContent = item.text || item.url;
      stage.appendChild(note);
      return;
    }
    const kind = item.kind || (isVideo(item.url) ? 'video' : isAudio(item.url) ? 'audio' : 'image');
    const node = document.createElement(kind === 'video' ? 'video' : kind === 'audio' ? 'audio' : 'img');
    node.className = 'alb-media alb-loading';
    node.draggable = false;
    if (kind === 'image') {
      node.alt = state.title(state.index) || 'Preview';
      node.decoding = 'async';
      node.addEventListener('load', () => { node.classList.remove('alb-loading'); fit(); }, {once: true});
      node.addEventListener('error', () => node.classList.remove('alb-loading'), {once: true});
    } else {
      node.controls = kind === 'audio';
      node.autoplay = true; node.loop = true; node.playsInline = true; node.muted = kind === 'video';
      node.addEventListener('loadedmetadata', () => { node.classList.remove('alb-loading'); if (kind === 'video') fit(); }, {once: true});
      if (kind === 'video') node.addEventListener('click', () => { if (view.z <= 1.001 && !state.dragged) { node.paused ? node.play().catch(() => {}) : node.pause(); } });
    }
    node.src = item.url;
    stage.appendChild(node);
    if (kind !== 'image') node.play && node.play().catch(() => {});
  }

  function signature(item) { return [item.status, item.url, item.text, item.error].join('|'); }

  function paintChrome() {
    const item = current();
    const n = count();
    const actions = state.actions || {};
    dialog.classList.toggle('alb-single', n <= 1);
    dialog.querySelector('.alb-title').textContent = state.title(state.index) || '';
    dialog.querySelector('.alb-count').textContent = (state.index + 1) + ' / ' + n;
    const q = name => dialog.querySelector('[data-alb="' + name + '"]');
    q('prev').hidden = q('next').hidden = n <= 1;
    const done = (item.status || 'done') === 'done';
    q('reseed').hidden = !actions.reseed || (actions.reseedHidden && actions.reseedHidden(state.index));
    q('use').hidden = !actions.use;
    q('use').disabled = !done;
    q('use').classList.toggle('alb-on', !!item.used);
    q('use').querySelector('.alb-lbl').textContent = item.used ? 'In use' : 'Use this';
    q('use').dataset.tip = item.used ? 'This is the output' : (actions.useTip || 'Use this');
    const lock = q('lock');
    lock.hidden = !actions.lock;
    lock.classList.toggle('alb-on', !!item.locked);
    lock.innerHTML = svg(item.locked ? 'lock' : 'unlock');
    lock.dataset.tip = item.locked ? 'Seed locked — click to unlock' : 'Lock seed';
    lock.setAttribute('aria-pressed', item.locked ? 'true' : 'false');
    const seed = q('seed');
    seed.hidden = !item.seed;
    seed.textContent = item.seed ? '#' + item.seed : '';
    seed.classList.toggle('alb-locked', !!item.locked);
    dialog.querySelector('[data-grp="seed"]').hidden = !(actions.reseed || actions.use || actions.lock || item.seed);
    const url = item.url && /^https?:|^\//.test(item.url) ? item.url : '';
    const download = q('download');
    download.hidden = actions.download === false || !url;
    q('copy').hidden = actions.copy === false || !url;
    q('post').hidden = !actions.post || (actions.postHidden && actions.postHidden(state.index));
    q('fit').hidden = !done || !!item.render || item.kind === 'audio' || item.kind === 'text';
    syncFitButton();
    q('info').classList.toggle('alb-on', dialog.classList.contains('alb-info-open'));
    paintInfo();
    const extra = dialog.querySelector('.alb-extra');
    extra.innerHTML = '';
    (actions.extra || []).forEach((entry, index) => {
      if (entry.hidden && entry.hidden(state.index)) return;
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'alb-btn';
      button.dataset.alb = 'extra';
      button.dataset.extra = String(index);
      button.dataset.tip = entry.label;
      button.dataset.key = entry.key || '';
      button.setAttribute('aria-label', entry.label);
      button.textContent = entry.glyph;
      extra.appendChild(button);
    });
  }

  function paintStrip(full) {
    const strip = dialog.querySelector('.alb-strip');
    const n = count();
    if (n <= 1) { strip.innerHTML = ''; return; }
    if (full || strip.children.length !== n) {
      strip.innerHTML = '';
      for (let index = 0; index < n; index += 1) {
        const cell = document.createElement('button');
        cell.type = 'button';
        cell.className = 'alb-thumb';
        cell.dataset.index = String(index);
        cell.setAttribute('role', 'option');
        cell.style.setProperty('--ar', String(state.aspect || 1));
        strip.appendChild(cell);
      }
    }
    Array.from(strip.children).forEach((cell, index) => {
      const item = state.item(index) || {};
      const status = item.status || 'done';
      const sig = signature(item) + '|' + !!item.used;
      cell.classList.toggle('alb-cur', index === state.index);
      cell.setAttribute('aria-selected', index === state.index ? 'true' : 'false');
      cell.tabIndex = index === state.index ? 0 : -1;
      if (cell._sig === sig) return;
      cell._sig = sig;
      cell.innerHTML = '';
      cell.setAttribute('aria-label', (state.title(index) || 'Item ' + (index + 1)) + ' · ' + status);
      cell.title = state.title(index) || '';
      const src = item.thumb || item.poster || (status === 'done' && item.url && !isAudio(item.url) && !/\.(glb|gltf)(\?|#|$)/i.test(item.url) ? item.url : '');
      if (src && isVideo(src)) {
        const clip = document.createElement('video');
        clip.muted = true; clip.preload = 'metadata'; clip.src = src + '#t=0.1';
        clip.addEventListener('loadedmetadata', () => aspectFrom(clip.videoWidth, clip.videoHeight), {once: true});
        cell.appendChild(clip);
      } else if (src) {
        const img = document.createElement('img');
        img.loading = 'lazy'; img.decoding = 'async'; img.alt = '';
        img.src = thumbUrl(src, 200);
        img.addEventListener('load', () => aspectFrom(img.naturalWidth, img.naturalHeight), {once: true});
        cell.appendChild(img);
      } else {
        const ph = document.createElement('span');
        ph.className = 'alb-ph';
        ph.textContent = item.kind === 'model' ? '◆' : item.kind === 'audio' ? '♪' : item.kind === 'text' ? '¶' : status === 'error' ? '!' : '…';
        cell.appendChild(ph);
      }
      const tag = document.createElement('i');
      tag.textContent = String(index + 1);
      cell.appendChild(tag);
      const dot = document.createElement('span');
      dot.className = 'alb-dot ' + (status === 'failed' ? 'error' : status) + (item.used ? ' used' : '');
      cell.appendChild(dot);
    });
    const cur = strip.children[state.index];
    if (cur) {
      const left = cur.offsetLeft - strip.clientWidth / 2 + cur.offsetWidth / 2;
      strip.scrollTo({left, behavior: reduced() ? 'auto' : 'smooth'});
    }
  }

  function aspectFrom(w, h) {
    if (!w || !h || state.aspectMeasured) return;
    state.aspectMeasured = true;
    state.aspect = Math.min(2.4, Math.max(0.4, w / h));
    dialog.querySelectorAll('.alb-thumb').forEach(cell => cell.style.setProperty('--ar', String(state.aspect)));
  }

  function show(index) {
    const n = count();
    if (!n) return;
    state.index = ((index % n) + n) % n;
    paintStage();
    paintChrome();
    paintStrip(false);
    if (state.onShow) state.onShow(state.index);
  }

  function refresh() {
    if (!dialog || !dialog.open || !state) return;
    const n = count();
    if (!n) { dialog.close(); return; }
    if (state.index >= n) state.index = n - 1;
    if (signature(current()) !== state.shown) paintStage();
    paintChrome();
    paintStrip(false);
  }

  /* ------------------------------------------------------------ info panel */
  const esc = value => String(value).replace(/[&<>"]/g, ch => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'})[ch]);
  const fetched = new Map();   // url -> {size, type, w, h} ; task -> render status
  function lookup(key, fn) {
    if (!fetched.has(key)) fetched.set(key, fn().catch(() => null));
    return fetched.get(key);
  }
  function paintInfo() {
    const panel = dialog.querySelector('.alb-info');
    if (!dialog.classList.contains('alb-info-open')) return;
    const item = current();
    const info = item.info || {};
    const used = info.params || {};
    const url = item.url || '';
    const row = (label, value, id) => '<tr><th>' + esc(label) + '</th><td' + (id ? ' data-f="' + id + '"' : '') + '>' +
      (value === undefined || value === null || value === '' ? '<span class="alb-muted">—</span>' : esc(value)) + '</td></tr>';
    const loras = [used.lora ? used.lora + (used.lora_strength ? ' · ' + used.lora_strength : '') : '']
      .concat(Array.isArray(used.loras) ? used.loras.map(l => (l.name || l.file) + ' · ' + (l.strength_model ?? l.weight ?? '')) : [])
      .concat(typeof used.loras === 'string' && used.loras ? [used.loras] : []).filter(Boolean).join(', ');
    const acts = [];
    if (used.prompt) acts.push('<button type="button" data-i="copy">Copy prompt</button>');
    if (Object.keys(used).length) acts.push('<button type="button" data-i="json">Copy params JSON</button>');
    if (used.prompt && state.actions.usePrompt) acts.push('<button type="button" data-i="use">Use this prompt</button>');
    if (state.actions.post) acts.push('<button type="button" data-i="post">Post to Civitai</button>');
    if (state.actions.openNode) acts.push('<button type="button" data-i="node">Open node</button>');
    panel.innerHTML =
      '<h4>' + esc(info.node || state.title(state.index) || 'Result') + '</h4>' +
      '<div class="alb-muted" style="margin-bottom:8px">' + esc(used.checkpoint || used.model || info.model || '') + '</div>' +
      (used.prompt ? '<div class="alb-muted">Final prompt</div><div class="alb-prompt">' + esc(used.prompt) + '</div>' :
        '<div class="alb-muted">No parameters were recorded for this result (rendered before they were kept, or not a render).</div>') +
      '<table>' +
      row('Negative', used.negative_prompt) + row('Seed', used.seed ?? item.seed) + row('Steps', used.steps) + row('CFG', used.cfg) +
      row('Sampler', [used.sampler, used.scheduler].filter(Boolean).join(' · ')) + row('LoRAs', loras) +
      row('Size', '', 'size') + row('File', '', 'file') + row('Frames', used.frame_count) +
      row('Render box', '', 'box') + row('Render time', '', 'time') + row('Task', used.task_id) + row('Created', used.recorded_at ? new Date(used.recorded_at).toLocaleString() : '') +
      (info.graphUrl ? '<tr><th>Graph</th><td><a href="' + esc(info.graphUrl) + '" target="_blank" rel="noopener">open</a></td></tr>' : '') +
      (info.civitaiUrl ? '<tr><th>Civitai</th><td><a href="' + esc(info.civitaiUrl) + '" target="_blank" rel="noopener">post</a></td></tr>' : '') +
      '</table><div class="alb-acts">' + acts.join('') + '</div>';
    panel.onclick = event => {
      const b = event.target.closest('[data-i]');
      if (!b) return;
      if (b.dataset.i === 'copy') copy(used.prompt).then(() => flash('Prompt copied'));
      if (b.dataset.i === 'json') copy(JSON.stringify(used, null, 2)).then(() => flash('Parameters copied'));
      if (b.dataset.i === 'use') { state.actions.usePrompt(state.index, used.prompt); flash('Prompt placed in the node'); }
      if (b.dataset.i === 'post') { dialog.close(); state.actions.post(state.index); }
      if (b.dataset.i === 'node') { dialog.close(); state.actions.openNode(state.index); }
    };
    const set = (field, text) => { const cell = panel.querySelector('[data-f="' + field + '"]'); if (cell && text) cell.textContent = text; };
    const index = state.index;
    if (/^https?:|^\//.test(url)) {
      const abs = new URL(url, location.href).href;
      if (!/\.(mp4|webm|mov)(\?|#|$)/i.test(abs)) {
        lookup('size:' + abs, () => fetch('/api/ai/media-size?url=' + encodeURIComponent(abs)).then(r => r.ok ? r.json() : null))
          .then(d => { if (d && state.index === index) set('size', d.width_int + ' × ' + d.height_int); });
      } else {
        const clip = dialog.querySelector('.alb-stage video');
        if (clip && clip.videoWidth) set('size', clip.videoWidth + ' × ' + clip.videoHeight + (clip.duration ? ' · ' + clip.duration.toFixed(1) + ' s' : ''));
      }
      if (new URL(abs).origin === location.origin) {
        lookup('head:' + abs, () => fetch(abs, {method: 'HEAD'}).then(r => r.ok ? {len: Number(r.headers.get('content-length')) || 0, type: r.headers.get('content-type') || ''} : null))
          .then(d => { if (d && state.index === index) set('file', (d.type.split(';')[0] || '') + (d.len ? ' · ' + (d.len / 1048576).toFixed(2) + ' MB' : '')); });
      }
    }
    if (used.task_id) {
      lookup('task:' + used.task_id, () => fetch('/api/ai/render-status/' + encodeURIComponent(used.task_id)).then(r => r.ok ? r.json() : null))
        .then(d => {
          if (!d || state.index !== index) return;
          set('box', d.node_string || '');
          if (d.started_at_unix_float && d.created_at_unix_float) set('time', 'queued ' + Math.round(d.started_at_unix_float - d.created_at_unix_float) + ' s' +
            (used.render_seconds ? ' · rendered ' + Math.round(used.render_seconds) + ' s' : ''));
        });
    }
  }

  /* -------------------------------------------------------------- downloads */
  function fileBase() {
    const item = current();
    const used = (item.info || {}).params || {};
    const name = String((item.info || {}).node || state.title(state.index) || 'autorig').toLowerCase()
      .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 40) || 'autorig';
    const seed = used.seed ?? item.seed;
    return name + (seed ? '_seed' + seed : '');
  }
  function toggleDownloadMenu() {
    const menu = dialog.querySelector('.alb-dlmenu');
    if (menu.classList.contains('on')) { menu.classList.remove('on'); return; }
    const item = current();
    const url = item.url || '';
    const video = /\.(mp4|webm|mov)(\?|#|$)/i.test(url) || item.kind === 'video';
    const same = (() => { try { return new URL(url, location.href).origin === location.origin; } catch (_) { return false; } })();
    const ext = (url.split('?')[0].match(/\.([a-z0-9]{3,4})$/i) || [, video ? 'mp4' : 'png'])[1].toUpperCase();
    menu.innerHTML = '<button type="button" data-as="original">Original (' + esc(ext) + ')<small>the file exactly as rendered</small></button>' +
      (video
        ? '<button type="button" data-as="frame"' + (same ? '' : ' disabled') + '>First frame as JPG<small>' + (same ? 'full size' : 'only for files on this site') + '</small></button>'
        : '<button type="button" data-as="jpg"' + (same ? '' : ' disabled') + '>JPG, high quality<small>' + (same ? 'full size, quality 95' : 'only for files on this site') + '</small></button>');
    const b = dialog.querySelector('[data-alb="download"]').getBoundingClientRect();
    menu.style.left = Math.max(8, Math.min(innerWidth - 250, b.left - 90)) + 'px';
    menu.style.bottom = (innerHeight - b.top + 8) + 'px';
    menu.classList.add('on');
  }
  function save(blobOrUrl, name) {
    const a = document.createElement('a');
    a.href = typeof blobOrUrl === 'string' ? blobOrUrl : URL.createObjectURL(blobOrUrl);
    a.download = name;
    document.body.appendChild(a); a.click(); a.remove();
    if (typeof blobOrUrl !== 'string') setTimeout(() => URL.revokeObjectURL(a.href), 30000);
  }
  async function downloadAs(kind) {
    dialog.querySelector('.alb-dlmenu').classList.remove('on');
    const item = current();
    const url = new URL(item.url, location.href).href;
    try {
      if (kind === 'original') {
        const ext = (url.split('?')[0].match(/\.([a-z0-9]{3,4})$/i) || [, 'png'])[1];
        const blob = await (await fetch(url)).blob();
        return save(blob, fileBase() + '.' + ext);
      }
      flash('Preparing the full-size JPG…');
      const canvas = document.createElement('canvas');
      if (kind === 'jpg') {
        // A detached Image with an onload handler loads the ORIGINAL (the
        // media gate never swaps it for a thumbnail).
        const img = new Image();
        await new Promise((ok, fail) => { img.onload = ok; img.onerror = fail; img.src = url; });
        canvas.width = img.naturalWidth; canvas.height = img.naturalHeight;
        canvas.getContext('2d').drawImage(img, 0, 0);
      } else {
        const clip = document.createElement('video');
        clip.muted = true; clip.preload = 'auto';
        await new Promise((ok, fail) => { clip.onloadeddata = ok; clip.onerror = fail; clip.src = url; });
        await new Promise(ok => { clip.onseeked = ok; clip.currentTime = 0.05; });
        canvas.width = clip.videoWidth; canvas.height = clip.videoHeight;
        canvas.getContext('2d').drawImage(clip, 0, 0);
      }
      const blob = await new Promise(ok => canvas.toBlob(ok, 'image/jpeg', 0.95));
      save(blob, fileBase() + '_' + canvas.width + 'x' + canvas.height + (kind === 'frame' ? '_frame1' : '') + '.jpg');
    } catch (error) {
      flash('Download failed: ' + (error && error.message || 'the file could not be read'));
    }
  }

  /* ----------------------------------------------------------- interaction */
  function run(action, source) {
    const actions = state.actions || {};
    const item = current();
    const i = state.index;
    if (action === 'prev') return show(i - 1);
    if (action === 'next') return show(i + 1);
    if (action === 'close') return dialog.close();
    if (action === 'fit') return toggleFit();
    if (action === 'seed' && item.seed) return copy(String(item.seed)).then(() => flash('Seed ' + item.seed + ' copied'));
    if (action === 'copy' && item.url) return copy(new URL(item.url, location.href).href).then(() => flash('Link copied'));
    if (action === 'download' && item.url) return toggleDownloadMenu();
    if (action === 'info') {
      dialog.classList.toggle('alb-info-open');
      try { localStorage.setItem('alb.info', dialog.classList.contains('alb-info-open') ? '1' : '0'); } catch (_) { /* private */ }
      paintChrome();
      return;
    }
    if (action === 'reseed' && actions.reseed && !dialog.querySelector('[data-alb="reseed"]').hidden) { actions.reseed(i); flash('New seed — rendering'); return later(); }
    if (action === 'use' && actions.use && (item.status || 'done') === 'done') { actions.use(i); return later(); }
    if (action === 'lock' && actions.lock) { actions.lock(i); flash(current().locked ? 'Seed locked' : 'Seed unlocked'); return later(); }
    if (action === 'post' && actions.post && !dialog.querySelector('[data-alb="post"]').hidden) { dialog.close(); actions.post(i); }
  }
  function later() { setTimeout(refresh, 0); }

  function wake() {
    dialog.classList.remove('alb-idle');
    clearTimeout(state.idleTimer);
    state.idleTimer = setTimeout(() => {
      // Keep the chrome while the pointer or keyboard focus is on it.
      if (dialog.querySelector('.alb-chrome:hover') || (document.activeElement && document.activeElement.closest &&
          document.activeElement.closest('.alb-bar,.alb-strip') && state.keyboard)) return wake();
      dialog.classList.add('alb-idle');
      hideTip();
    }, 2000);
  }

  function showTip(target) {
    const tip = dialog.querySelector('.alb-tip');
    const label = target.dataset.tip;
    if (!label) return;
    tip.innerHTML = '';
    tip.appendChild(document.createTextNode(label));
    if (target.dataset.key) { const kbd = document.createElement('kbd'); kbd.textContent = target.dataset.key; tip.appendChild(kbd); }
    const box = target.getBoundingClientRect();
    tip.style.left = Math.min(window.innerWidth - 80, Math.max(80, box.left + box.width / 2)) + 'px';
    tip.style.top = (box.top - 8) + 'px';
    tip.classList.add('alb-show');
  }
  function hideTip() { const tip = dialog && dialog.querySelector('.alb-tip'); if (tip) tip.classList.remove('alb-show'); }

  function wire() {
    const stage = dialog.querySelector('.alb-stage');
    dialog.addEventListener('click', event => {
      const target = event.target.closest('[data-alb],.alb-thumb,[data-as]');
      if (target && target.dataset && target.dataset.as) { event.preventDefault(); downloadAs(target.dataset.as); return; }
      if (!target) return;
      if (target.classList.contains('alb-thumb')) { show(Number(target.dataset.index)); return; }
      const action = target.dataset.alb;
      if (action === 'extra') { const entry = state.actions.extra[Number(target.dataset.extra)]; if (entry) { entry.run(state.index); later(); } return; }
      if (action === 'dl') { downloadAs(target.dataset.as); return; }
      event.preventDefault();
      run(action, 'click');
    });
    dialog.addEventListener('pointerover', event => { const t = event.target.closest('[data-tip]'); if (t) showTip(t); });
    dialog.addEventListener('pointerout', event => { const t = event.target.closest('[data-tip]'); if (t) hideTip(); });
    dialog.addEventListener('focusin', event => { const t = event.target.closest('[data-tip]'); if (t && state.keyboard) showTip(t); });
    dialog.addEventListener('focusout', hideTip);
    dialog.addEventListener('pointermove', () => { state.keyboard = false; wake(); });
    dialog.addEventListener('pointerdown', () => { state.keyboard = false; wake(); });
    dialog.addEventListener('keydown', event => {
      state.keyboard = true;
      wake();
      if (event.target.closest && event.target.closest('input,textarea,select,[contenteditable]')) return;
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      const key = event.key;
      const map = {ArrowLeft: 'prev', ArrowRight: 'next'};
      const code = {KeyR: 'reseed', KeyL: 'lock', KeyF: 'fit', KeyD: 'download', KeyC: 'copy', KeyI: 'info'};
      let action = map[key] || code[event.code];
      if (key === 'Enter' && !(event.target.closest && event.target.closest('button,a'))) action = 'use';
      if (key === 'Tab') { trap(event); return; }
      if (!action) {
        const extra = (state.actions.extra || []).findIndex(entry => entry.key && entry.key.toLowerCase() === key.toLowerCase());
        if (extra >= 0) { event.preventDefault(); state.actions.extra[extra].run(state.index); later(); }
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      run(action, 'key');
    });
    dialog.addEventListener('close', () => {
      const clip = dialog.querySelector('.alb-stage video, .alb-stage audio');
      if (clip) clip.pause();
      dialog.querySelector('.alb-stage').innerHTML = '';
      clearTimeout(state && state.idleTimer);
      clearInterval(state && state.poll);
      hideTip();
      if (state && state.returnFocus && state.returnFocus.focus) try { state.returnFocus.focus({preventScroll: true}); } catch (e) { /* gone */ }
      if (state && state.onClose) state.onClose();
    });
    // Click on the empty backdrop of the stage closes (not after a pan).
    stage.addEventListener('click', event => {
      if (event.target === stage && !state.dragged && view.z <= 1.001) dialog.close();
    });
    stage.addEventListener('wheel', event => {
      event.preventDefault();
      zoomAt(Math.exp(-event.deltaY * (event.deltaMode === 1 ? 0.05 : 0.0022)), event.clientX, event.clientY);
      syncFitButton();
    }, {passive: false});
    stage.addEventListener('dblclick', event => {
      if (view.z > 1.001) { view.z = 1; view.x = 0; view.y = 0; layout(); } else zoomAt(2.5, event.clientX, event.clientY);
      syncFitButton();
    });
    // Pointers: one = pan (zoomed) or swipe (fitted); two = pinch.
    const pointers = new Map();
    let pinch = null, drag = null;
    stage.addEventListener('pointerdown', event => {
      if (event.target.closest('.alb-free')) return;
      pointers.set(event.pointerId, {x: event.clientX, y: event.clientY});
      stage.setPointerCapture(event.pointerId);
      state.dragged = false;
      if (pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        pinch = {d: Math.hypot(a.x - b.x, a.y - b.y), z: view.z};
        drag = null;
      } else drag = {x: event.clientX, y: event.clientY, vx: view.x, vy: view.y, t: Date.now()};
    });
    stage.addEventListener('pointermove', event => {
      if (!pointers.has(event.pointerId)) return;
      pointers.set(event.pointerId, {x: event.clientX, y: event.clientY});
      if (pinch && pointers.size === 2) {
        const [a, b] = [...pointers.values()];
        const d = Math.hypot(a.x - b.x, a.y - b.y);
        zoomAt((pinch.z * d / pinch.d) / view.z, (a.x + b.x) / 2, (a.y + b.y) / 2);
        state.dragged = true;
        return;
      }
      if (!drag) return;
      const dx = event.clientX - drag.x, dy = event.clientY - drag.y;
      if (Math.abs(dx) + Math.abs(dy) > 4) state.dragged = true;
      if (view.z > 1.001) {
        dialog.classList.add('alb-panning');
        view.x = drag.vx + dx; view.y = drag.vy + dy; layout();
      } else if (media() && event.pointerType !== 'mouse') {
        media().style.transform = 'translateX(' + dx + 'px)';
      }
    });
    const end = event => {
      if (!pointers.has(event.pointerId)) return;
      pointers.delete(event.pointerId);
      dialog.classList.remove('alb-panning');
      if (pointers.size < 2) pinch = null;
      if (drag && view.z <= 1.001 && pointers.size === 0) {
        const dx = event.clientX - drag.x, dy = event.clientY - drag.y;
        const node = media();
        if (node) node.style.transform = '';
        if (Math.abs(dx) > 50 && Math.abs(dx) > Math.abs(dy) * 1.5 && Date.now() - drag.t < 800) show(state.index + (dx < 0 ? 1 : -1));
      }
      drag = null;
      syncFitButton();
      setTimeout(() => { state.dragged = false; }, 0);
    };
    stage.addEventListener('pointerup', end);
    stage.addEventListener('pointercancel', end);
    window.addEventListener('resize', () => { if (dialog.open && view.z <= 1.001 && media()) { const n = media(); ['position', 'left', 'top', 'width', 'height', 'maxWidth', 'maxHeight'].forEach(k => { n.style[k] = ''; }); view.fitW = 0; fit(); } });
  }

  function trap(event) {
    const focusable = [...dialog.querySelectorAll('button:not([hidden]):not([disabled]):not([tabindex="-1"]),a[href]:not([hidden]),.alb-thumb.alb-cur,video[controls],audio[controls]')]
      .filter(node => node.offsetParent !== null || node.getClientRects().length);
    if (!focusable.length) return;
    const first = focusable[0], last = focusable[focusable.length - 1];
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  }

  function open(options) {
    if (!dialog) build();
    if (dialog.open) dialog.close();
    const items = Array.isArray(options.items) ? options.items : null;
    state = {
      count: options.count || (() => (items ? items.length : 1)),
      item: options.item || (i => (items ? items[i] : Object.assign({}, options, {kind: options.kind_ || ''}))),
      title: options.title || (i => (items && items[i] && items[i].title) || ''),
      actions: options.actions || {},
      onShow: options.onShow, onClose: options.onClose,
      index: 0, aspect: options.aspect || 1, aspectMeasured: !!options.aspect,
      returnFocus: document.activeElement, keyboard: false
    };
    dialog.dataset.kind = options.kind || '';
    let infoPref = null;
    try { infoPref = localStorage.getItem('alb.info'); } catch (_) { /* private */ }
    dialog.classList.toggle('alb-info-open', infoPref === null ? innerWidth > 1100 : infoPref === '1');
    paintStrip(true);
    dialog.showModal();
    show(Number(options.start) || 0);
    dialog.querySelector('.alb-close').focus({preventScroll: true});
    wake();
    // Items that are still rendering: repaint when they change.
    state.poll = setInterval(refresh, 1000);
    return {refresh, close: () => dialog.close(), show, get index() { return state.index; }, get open() { return dialog.open; }};
  }

  window.AILightbox = {open, refresh, isOpen: () => !!(dialog && dialog.open)};
})();

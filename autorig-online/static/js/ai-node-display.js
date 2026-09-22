/**
 * Compact / basic / full presentation modes for the node editor.
 *
 * The module never rebuilds node HTML. Controls are only hidden with CSS, so
 * model pickers, manually entered values and live render state survive every
 * mode switch. Persistence belongs to the host through `onModeChange` because
 * graph serialisation lives in ai-nodes.js.
 */
(function (global) {
  'use strict';

  const MODES = Object.freeze(['small', 'medium', 'all']);
  const LABELS = Object.freeze({ small: 'S', medium: 'M', all: 'A' });
  const TITLES = Object.freeze({
    small: 'Small: preview and status',
    medium: 'Medium: essential settings',
    all: 'All settings'
  });
  const BASIC_PARAMS = new Set([
    'checkpoint', 'lora', 'width', 'height', 'seed',
    'frames', 'frame_count', 'duration', 'duration_seconds'
  ]);

  function normalizeMode(value, fallback) {
    value = String(value || '').toLowerCase();
    return MODES.includes(value) ? value : (MODES.includes(fallback) ? fallback : 'all');
  }

  /**
   * After the next frame, or shortly after — whichever the page allows.
   *
   * The wait exists to let the host finish attaching a node's metadata, and a
   * frame is the natural moment for it. A tab that is not painting never gets
   * a frame, though, and a node left without its controls is worse than one
   * prepared a few milliseconds early. Everything scheduled here is
   * idempotent, so running twice costs nothing.
   */
  function soon(callback) {
    if (typeof requestAnimationFrame === 'function') requestAnimationFrame(callback);
    if (typeof setTimeout === 'function') setTimeout(callback, 0);
  }

  function nodeId(element) {
    return element && String(element.id || '').replace(/^node-/, '');
  }

  function nodeElement(canvas, idOrElement) {
    if (idOrElement && idOrElement.nodeType === 1) return idOrElement;
    const raw = String(idOrElement == null ? '' : idOrElement).replace(/^node-/, '');
    if (!raw) return null;
    try { return canvas.querySelector('#node-' + CSS.escape(raw)); }
    catch (_) { return document.getElementById('node-' + raw); }
  }

  function storedMode(element, getMeta, fallback) {
    const id = nodeId(element);
    const meta = (getMeta && getMeta(id)) || {};
    const graphNode = meta.params || meta.data || {};
    return normalizeMode(
      element.dataset.displayMode || meta.displayMode || meta.display_mode ||
      graphNode._display_mode || graphNode.display_mode,
      fallback
    );
  }

  function installStyles() {
    if (document.getElementById('ai-node-display-style')) return;
    const style = document.createElement('style');
    style.id = 'ai-node-display-style';
    style.textContent = [
      '.drawflow .drawflow-node[data-display-mode]{transition:width .16s ease,min-height .16s ease}',
      '.drawflow .drawflow-node[data-display-mode="small"]{width:220px!important;min-height:var(--display-port-floor,74px)}',
      '.drawflow .drawflow-node[data-display-mode="medium"]{width:305px!important}',
      '.drawflow .drawflow-node[data-display-mode="all"]{width:370px!important}',
      '.drawflow .drawflow-node .nhead{gap:7px}',
      '.drawflow .drawflow-node .nhead b{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}',
      '.node-display-mode{flex:0 0 auto;margin-left:auto;width:24px;height:22px;padding:0;border:1px solid rgba(255,255,255,.18);border-radius:6px;background:rgba(255,255,255,.06);color:#c7b9ff;font:700 10px/20px Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;cursor:pointer}',
      '.node-display-mode:hover,.node-display-mode:focus-visible{background:rgba(123,92,255,.2);border-color:#7b5cff;outline:none}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nslow{display:none}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nports,',
      '.drawflow .drawflow-node[data-display-mode="small"] .nparams,',
      '.drawflow .drawflow-node[data-display-mode="small"] .nrec{display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="small"] .ninput>*:not(img):not(video){display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="small"] .ninput:not(:has(img:not([hidden]),video:not([hidden]))){display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="small"] .ninput{padding:5px 10px 9px}',
      '.drawflow .drawflow-node[data-display-mode="small"] .ninput img,.drawflow .drawflow-node[data-display-mode="small"] .ninput video{width:100%;max-height:210px;object-fit:contain;background:#090a14;margin:0;border-radius:8px}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nout{padding:3px 10px 10px}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nout:not(:has(img,video)){display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nout>*:not(img):not(video){display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nout img,',
      '.drawflow .drawflow-node[data-display-mode="small"] .nout video{width:100%;max-height:230px;object-fit:contain;background:#090a14}',
      '.drawflow .drawflow-node[data-display-mode="small"] .nstate:empty,',
      '.drawflow .drawflow-node[data-display-mode="small"] .nprog:empty{display:none}',
      '.drawflow .drawflow-node[data-display-mode="medium"] .nparams.display-no-basics{display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="medium"] .nparams .nparam:not(.display-basic-param){display:none!important}',
      '.drawflow .drawflow-node[data-display-mode="medium"] .nparams>summary::before{content:"⚙ "}',
      '.drawflow .drawflow-node[data-display-mode="medium"] .nparams>summary{font-size:0}',
      '.drawflow .drawflow-node[data-display-mode="medium"] .nparams>summary::after{content:"Basic settings";font-size:11.5px}',
      '.drawflow .drawflow-node[data-display-mode="all"] .nparams .nparam{display:flex}',
      '.drawflow .drawflow-node[data-display-mode="all"] .nparams .nparam.nparam-wide{display:block}'
    ].join('\n');
    document.head.appendChild(style);
  }

  function markBasicParams(element) {
    const details = element.querySelector('.nparams');
    if (!details) return;
    let count = 0;
    details.querySelectorAll('.nparam').forEach(row => {
      const control = row.querySelector('[data-param]');
      const name = control && control.dataset.param;
      const basic = BASIC_PARAMS.has(String(name || ''));
      row.classList.toggle('display-basic-param', basic);
      if (basic) count += 1;
    });
    details.classList.toggle('display-no-basics', count === 0);
  }

  function portFloor(element) {
    const inputs = element.querySelectorAll(':scope > .inputs > .input').length;
    const outputs = element.querySelectorAll(':scope > .outputs > .output').length;
    const rows = Math.max(inputs, outputs, 1);
    // alignPorts starts sockets below the 46 px header on a 26 px pitch.
    return Math.max(74, 58 + rows * 26);
  }

  function updateButton(element, mode) {
    const button = element.querySelector('.node-display-mode');
    if (!button) return;
    button.textContent = LABELS[mode];
    button.title = TITLES[mode] + '. Click or double-click the header to change mode; ' +
      'hold Shift to change every node in the graph.';
    button.setAttribute('aria-label', TITLES[mode] +
      '. Activate to use the next display mode, or hold Shift to apply it to every node.');
    button.setAttribute('aria-pressed', mode === 'all' ? 'true' : 'false');
  }

  function applyMode(element, mode, options) {
    if (!element) return null;
    options = options || {};
    mode = normalizeMode(mode, options.fallback || 'all');
    element.dataset.displayMode = mode;
    const outputs = element.querySelector(':scope > .outputs');
    const inputCount = element.querySelectorAll(':scope > .inputs > .input').length;
    if (outputs) outputs.style.marginTop = (52 + (mode === 'small' ? 0 : inputCount * 26)) + 'px';
    element.style.setProperty('--display-port-floor', portFloor(element) + 'px');
    markBasicParams(element);
    const details = element.querySelector('.nparams');
    if (details) details.open = mode !== 'small';
    updateButton(element, mode);
    if (typeof options.updateConnections === 'function') {
      soon(() => options.updateConnections(nodeId(element)));
    }
    return mode;
  }

  function install(options) {
    options = options || {};
    const editor = options.editor;
    const canvas = options.canvas;
    const getMeta = options.getMeta || function () { return null; };
    const onModeChange = options.onModeChange || function () {};
    const fallback = normalizeMode(options.defaultMode, 'all');
    if (!editor || !canvas) throw new Error('AINodeDisplay.install requires editor and canvas');
    installStyles();

    const resizeObservers = new Map();
    let stopped = false;

    function updateConnections(id) {
      if (!id || typeof editor.updateConnectionNodes !== 'function') return;
      editor.updateConnectionNodes('node-' + id);
    }

    function ensureButton(element) {
      const header = element.querySelector('.nhead');
      if (!header || header.querySelector('.node-display-mode')) return;
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'node-display-mode';
      button.addEventListener('pointerdown', event => event.stopPropagation());
      button.addEventListener('dblclick', event => event.stopPropagation());
      button.addEventListener('click', event => {
        event.preventDefault(); event.stopPropagation();
        if (event.shiftKey) cycleAll(element, true); else cycle(element, true);
      });
      header.appendChild(button);
    }

    function watchSize(element) {
      const id = nodeId(element);
      if (!id || resizeObservers.has(id) || typeof ResizeObserver === 'undefined') return;
      let previousWidth = 0;
      let previousHeight = 0;
      const observer = new ResizeObserver(entries => {
        const rect = entries[0] && entries[0].contentRect;
        if (!rect || (Math.abs(rect.width - previousWidth) < .5 && Math.abs(rect.height - previousHeight) < .5)) return;
        previousWidth = rect.width; previousHeight = rect.height;
        requestAnimationFrame(() => updateConnections(id));
      });
      observer.observe(element);
      resizeObservers.set(id, observer);
    }

    function prepare(element) {
      if (!element || !element.matches('.drawflow-node[id^="node-"]')) return null;
      ensureButton(element);
      watchSize(element);
      return applyMode(element, storedMode(element, getMeta, fallback), { fallback, updateConnections });
    }

    function setMode(idOrElement, mode, persist) {
      const element = nodeElement(canvas, idOrElement);
      if (!element) return null;
      const applied = applyMode(element, mode, { fallback, updateConnections });
      if (persist !== false) onModeChange(nodeId(element), applied, getMeta(nodeId(element)) || null);
      return applied;
    }

    function getMode(idOrElement) {
      const element = nodeElement(canvas, idOrElement);
      return element ? normalizeMode(element.dataset.displayMode, fallback) : null;
    }

    function cycle(idOrElement, persist) {
      const current = getMode(idOrElement) || fallback;
      return setMode(idOrElement, MODES[(MODES.indexOf(current) + 1) % MODES.length], persist);
    }

    function setModeAll(mode, persist) {
      const applied = normalizeMode(mode, fallback);
      canvas.querySelectorAll('.drawflow-node[id^="node-"]')
        .forEach(element => setMode(element, applied, persist));
      return applied;
    }

    /**
     * Shift takes the whole graph with it.
     *
     * The next mode is read from the node that was clicked, so the one under
     * the cursor behaves exactly as it would without Shift and the rest simply
     * join it. Twenty nodes are otherwise twenty clicks.
     */
    function cycleAll(idOrElement, persist) {
      const current = getMode(idOrElement) || fallback;
      return setModeAll(MODES[(MODES.indexOf(current) + 1) % MODES.length], persist);
    }

    function refresh(idOrElement) {
      if (idOrElement != null) return prepare(nodeElement(canvas, idOrElement));
      canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(prepare);
      return null;
    }

    function onDoubleClick(event) {
      if (event.target.closest('button,input,select,textarea,a,label')) return;
      const header = event.target.closest('.nhead');
      const element = header && header.closest('.drawflow-node[id^="node-"]');
      if (!element || !canvas.contains(element)) return;
      event.preventDefault(); event.stopPropagation();
      cycle(element, true);
    }
    canvas.addEventListener('dblclick', onDoubleClick, true);

    const mutations = new MutationObserver(records => {
      if (stopped) return;
      records.forEach(record => record.addedNodes.forEach(added => {
        if (added.nodeType !== 1) return;
        if (added.matches && added.matches('.drawflow-node[id^="node-"]')) soon(() => prepare(added));
        else if (added.querySelectorAll) added.querySelectorAll('.drawflow-node[id^="node-"]').forEach(element => soon(() => prepare(element)));
      }));
    });
    mutations.observe(canvas, { childList: true, subtree: true });

    // Drawflow emits this before some hosts finish attaching their own metadata,
    // so defer one frame and let storedMode read the final meta object.
    const onCreated = id => soon(() => prepare(nodeElement(canvas, id)));
    if (typeof editor.on === 'function') editor.on('nodeCreated', onCreated);
    const scaleSockets = () => canvas.style.setProperty('--socket-zoom-scale',
      String(Math.max(1, Math.min(1.8, 1 / Math.sqrt(Number(editor.zoom) || 1)))));
    if (typeof editor.on === 'function') editor.on('zoom', scaleSockets);
    scaleSockets();
    refresh();

    return {
      applyNode: (idOrElement, mode) => setMode(idOrElement, mode, false),
      setMode,
      setModeAll: mode => setModeAll(mode, true),
      getMode,
      cycle: idOrElement => cycle(idOrElement, true),
      cycleAll: idOrElement => cycleAll(idOrElement, true),
      refresh,
      destroy: function () {
        stopped = true;
        canvas.removeEventListener('dblclick', onDoubleClick, true);
        mutations.disconnect();
        resizeObservers.forEach(observer => observer.disconnect());
        resizeObservers.clear();
      }
    };
  }

  global.AINodeDisplay = Object.freeze({
    MODES: MODES,
    BASIC_PARAMS: BASIC_PARAMS,
    normalizeMode: normalizeMode,
    applyMode: applyMode,
    install: install
  });
})(window);

/**
 * Typed sockets for the node editor: colour by what a socket carries, and
 * make wiring easy from far away.
 *
 * - Every socket gets `data-socket-type` (and `data-socket-also` for a socket
 *   that also takes another type, e.g. a picture socket that reads a video's
 *   first frame); every wire gets `data-wire-type`. The colours live in CSS
 *   variables on the page (`--t-text`, `--t-image`, ...).
 * - While a wire is being dragged, every input that would accept it glows and
 *   everything else greys out. Dropping on a grey socket is refused before
 *   Drawflow creates the link; dropping near a glowing one snaps to it, with
 *   a radius that grows as the camera pulls back.
 *
 * The host supplies the facts: `socketTypes(id)` and `linkAllowed(link)`,
 * which is the same rule the editor applies when a link is created (and the
 * server applies when the graph is saved). This module only reads the DOM and
 * Drawflow's data; it never edits the graph itself.
 */
(function (global) {
  'use strict';

  const TYPE_ORDER = Object.freeze([
    'text', 'image', 'video', 'avatar', 'model3d',
    'control_pose', 'control_depth', 'control_canny', 'audio'
  ]);

  /** The server's rule (ai_graph.validate): same type, or listed in also_accepts. */
  function typeAccepts(produced, accepted, alsoAccepts) {
    produced = String(produced || '');
    if (!produced || !accepted) return false;
    return produced === String(accepted) ||
      (alsoAccepts || []).map(String).includes(produced);
  }

  /** Drop radius in screen pixels: a far-away camera needs a forgiving target. */
  function snapRadius(zoom) {
    const value = Number(zoom);
    const z = Number.isFinite(value) && value > 0 ? value : 1;
    return Math.round(Math.min(110, Math.max(26, 34 / Math.sqrt(z))));
  }

  /** Nearest candidate centre within the radius, or null. */
  function nearest(candidates, x, y, radius) {
    let best = null;
    let bestDistance = radius * radius;
    for (const item of candidates) {
      const dx = item.x - x;
      const dy = item.y - y;
      const distance = dx * dx + dy * dy;
      if (distance <= bestDistance) { best = item; bestDistance = distance; }
    }
    return best;
  }

  /** Would a link from `sourceId` into `targetId` close a loop? */
  function wouldLoop(data, sourceId, targetId) {
    sourceId = String(sourceId); targetId = String(targetId);
    if (sourceId === targetId) return true;
    const seen = new Set([targetId]);
    const stack = [targetId];
    while (stack.length) {
      const node = data[stack.pop()];
      if (!node) continue;
      for (const port of Object.values(node.outputs || {})) {
        for (const link of port.connections || []) {
          const next = String(link.node);
          if (next === sourceId) return true;
          if (!seen.has(next)) { seen.add(next); stack.push(next); }
        }
      }
    }
    return false;
  }

  function portNumber(className) {
    const match = /_(\d+)$/.exec(String(className || ''));
    return match ? parseInt(match[1], 10) : 0;
  }

  function classValue(classList, prefix) {
    for (const name of classList || []) {
      if (String(name).startsWith(prefix)) return String(name).slice(prefix.length);
    }
    return '';
  }

  function install(options) {
    options = options || {};
    const editor = options.editor;
    const canvas = options.canvas;
    const socketTypes = options.socketTypes || (() => null);
    const linkAllowed = options.linkAllowed || (() => true);
    const entityTypes = options.entityTypes || [];
    if (!editor || !canvas) return null;

    const titles = {};
    entityTypes.forEach(item => { titles[item.id] = item.title || item.id; });
    const titleOf = type => titles[type] || String(type || '').replace(/_/g, ' ');

    /* ------------------------------------------------------ painting */

    function paintNode(element) {
      if (!element || !element.id) return;
      const id = element.id.replace(/^node-/, '');
      const types = socketTypes(id);
      if (!types) return;
      element.querySelectorAll(':scope > .inputs > .input').forEach(socket => {
        const item = types.inputs[portNumber(socket.classList[1]) - 1];
        if (!item) return;
        socket.dataset.socketType = item.type || '';
        const also = (item.also || []).filter(value => value !== item.type);
        if (also.length) socket.dataset.socketAlso = also[0];
        else delete socket.dataset.socketAlso;
        socket.title = [titleOf(item.type)].concat(also.map(titleOf)).join(' / ') +
          (item.title ? ' · ' + item.title : '');
      });
      element.querySelectorAll(':scope > .outputs > .output').forEach(socket => {
        const type = types.outputs[portNumber(socket.classList[1]) - 1];
        if (!type) return;
        socket.dataset.socketType = type;
        socket.title = titleOf(type);
      });
    }

    function paintWire(wire) {
      const source = classValue(wire.classList, 'node_out_node-');
      if (!source) return;
      const output = [...wire.classList].find(name => /^output_\d+$/.test(name));
      const types = socketTypes(source);
      const type = types && types.outputs[portNumber(output) - 1];
      if (type && wire.getAttribute('data-wire-type') !== type) wire.setAttribute('data-wire-type', type);
    }

    let paintQueued = false;
    function repaintAll() {
      paintQueued = false;
      canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(paintNode);
      canvas.querySelectorAll('svg.connection').forEach(paintWire);
    }
    function scheduleRepaint() {
      if (paintQueued) return;
      paintQueued = true;
      setTimeout(repaintAll, 0);
    }

    /* -------------------------------------------------- drag feedback */

    let wiring = null;   // {sourceId, outputClass, type, candidates, target}

    function clearWiring() {
      if (!wiring) return;
      canvas.classList.remove('wiring');
      canvas.querySelectorAll('.sock-ok, .sock-no, .sock-target').forEach(socket =>
        socket.classList.remove('sock-ok', 'sock-no', 'sock-target'));
      canvas.querySelectorAll('.drawflow-node.wire-none').forEach(node => node.classList.remove('wire-none'));
      wiring = null;
    }

    function startWiring(detail) {
      clearWiring();
      if (!detail) return;
      const sourceId = String(detail.output_id);
      const outputClass = String(detail.output_class);
      const types = socketTypes(sourceId);
      const type = types ? types.outputs[portNumber(outputClass) - 1] : '';
      if (editor.connection_ele && type) editor.connection_ele.setAttribute('data-wire-type', type);
      const data = ((editor.drawflow || {}).drawflow || {})[editor.module || 'Home'];
      const nodes = (data && data.data) || {};
      const candidates = [];
      canvas.classList.add('wiring');
      canvas.style.setProperty('--wiring-color', 'var(--t-' + (type || 'none') + ', #94a3b8)');
      canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(element => {
        const targetId = element.id.slice(5);
        const loops = wouldLoop(nodes, sourceId, targetId);
        let any = false;
        element.querySelectorAll(':scope > .inputs > .input').forEach(socket => {
          if (socket.offsetParent === null) return; // a hidden reference socket
          const ok = !loops && linkAllowed({
            output_id: sourceId, output_class: outputClass,
            input_id: targetId, input_class: socket.classList[1]
          });
          socket.classList.add(ok ? 'sock-ok' : 'sock-no');
          if (ok) {
            any = true;
            const rect = socket.getBoundingClientRect();
            candidates.push({socket, x: rect.left + rect.width / 2, y: rect.top + rect.height / 2});
          }
        });
        if (!any && targetId !== sourceId) element.classList.add('wire-none');
      });
      wiring = {sourceId, outputClass, type, candidates, target: null};
    }

    function targetFor(event) {
      const direct = event.target && event.target.closest && event.target.closest('.input');
      if (direct && direct.classList.contains('sock-ok')) return direct;
      const hit = nearest(wiring.candidates, event.clientX, event.clientY, snapRadius(editor.zoom));
      if (hit) return hit.socket;
      return direct || null;
    }

    let lastMove = null;
    let moveScheduled = false;
    function onMove(event) {
      if (!wiring) return;
      lastMove = {target: event.target, clientX: event.clientX, clientY: event.clientY};
      if (moveScheduled) return;
      moveScheduled = true;
      requestAnimationFrame(() => {
        moveScheduled = false;
        const last = lastMove;
        if (!wiring || !last) return;
        const target = targetFor(last);
        const next = target && target.classList.contains('sock-ok') ? target : null;
        if (next !== wiring.target) {
          if (wiring.target) wiring.target.classList.remove('sock-target');
          if (next) next.classList.add('sock-target');
          wiring.target = next;
        }
      });
    }

    function refuse(socket) {
      socket.classList.remove('sock-refused');
      void socket.offsetWidth;
      socket.classList.add('sock-refused');
      setTimeout(() => socket.classList.remove('sock-refused'), 450);
    }

    /**
     * Runs before Drawflow's own mouseup (capture on the same container).
     * Drawflow decides what a drop means from `event.target`, so the drop is
     * re-issued on the socket it should have hit — or on the bare canvas,
     * which Drawflow treats as "cancel".
     */
    function onDrop(event) {
      if (event._aiSocketsRedirected || !wiring || editor.connection !== true) return;
      const target = targetFor(event);
      const direct = event.target && event.target.closest && event.target.closest('.input');
      let redirect = null;
      if (target && target.classList.contains('sock-ok')) {
        if (target !== event.target) redirect = target;
      } else if (direct) {
        refuse(direct);
        redirect = editor.precanvas;
      }
      if (!redirect) return;
      event.stopImmediatePropagation();
      event.preventDefault();
      const again = new MouseEvent('mouseup', {
        bubbles: true, cancelable: true, clientX: event.clientX, clientY: event.clientY,
        button: event.button, shiftKey: event.shiftKey, ctrlKey: event.ctrlKey
      });
      again._aiSocketsRedirected = true;
      redirect.dispatchEvent(again);
    }

    canvas.addEventListener('mouseup', onDrop, true);
    canvas.addEventListener('mousemove', onMove, true);
    // After Drawflow: whatever happened, the drag is over.
    const endLater = () => setTimeout(clearWiring, 0);
    document.addEventListener('mouseup', endLater);
    document.addEventListener('touchend', endLater);

    if (typeof editor.on === 'function') {
      editor.on('connectionStart', startWiring);
      editor.on('connectionCancel', clearWiring);
      editor.on('connectionCreated', () => { clearWiring(); scheduleRepaint(); });
      editor.on('nodeCreated', scheduleRepaint);
      editor.on('import', scheduleRepaint);
    }
    // Reference sockets appear and disappear; a new wire is a new <svg>.
    const observer = new MutationObserver(records => {
      for (const record of records) {
        for (const added of record.addedNodes) {
          if (added.nodeType === 1 && (added.matches('svg.connection, .drawflow-node') ||
              added.classList.contains('input') || added.classList.contains('output'))) {
            scheduleRepaint();
            return;
          }
        }
      }
    });
    observer.observe(editor.precanvas || canvas, {childList: true, subtree: true});
    scheduleRepaint();

    // A narrow node ellipsizes a long option; hovering shows it whole.
    canvas.addEventListener('pointerover', event => {
      const select = event.target && event.target.closest && event.target.closest('.drawflow-node select');
      if (!select) return;
      if (select.dataset.helpTitle === undefined) select.dataset.helpTitle = select.title || '';
      const option = select.selectedOptions && select.selectedOptions[0];
      const text = option ? option.textContent.trim() : '';
      select.title = [text, select.dataset.helpTitle].filter(Boolean).join(' — ');
    });

    installLegend(canvas.parentElement || canvas, entityTypes);

    return {
      repaint: repaintAll,
      isWiring: () => !!wiring,
      destroy() {
        observer.disconnect();
        canvas.removeEventListener('mouseup', onDrop, true);
        canvas.removeEventListener('mousemove', onMove, true);
        document.removeEventListener('mouseup', endLater);
        document.removeEventListener('touchend', endLater);
      }
    };
  }

  /** A row of coloured dots, one per type; the name is in the tooltip. */
  function installLegend(host, entityTypes) {
    if (!host || host.querySelector('.socket-legend')) return;
    const known = new Map((entityTypes || []).map(item => [item.id, item]));
    const legend = document.createElement('div');
    legend.className = 'socket-legend';
    legend.setAttribute('role', 'list');
    legend.setAttribute('aria-label', 'Socket colours');
    TYPE_ORDER.filter(type => known.has(type)).forEach(type => {
      const item = known.get(type);
      const chip = document.createElement('span');
      chip.setAttribute('role', 'listitem');
      chip.dataset.socketType = type;
      chip.title = (item.title || type) + (item.carries ? ' — ' + item.carries : '');
      chip.innerHTML = '<i></i>' + (item.icon ? '<b>' + item.icon + '</b>' : '');
      legend.appendChild(chip);
    });
    host.appendChild(legend);
  }

  global.AINodeSockets = Object.freeze({
    install, typeAccepts, snapRadius, nearest, wouldLoop, TYPE_ORDER
  });
})(typeof window !== 'undefined' ? window : globalThis);

(function () {
  'use strict';

  const MIME = 'application/x-autorig-nodes';
  const TEXT_PREFIX = 'AUTORIG_NODES_V1:';
  const VERSION = 1;
  const MAX_CLIPBOARD_BYTES = 2 * 1024 * 1024;
  const PRIORITY_PARAMS = ['width', 'height', 'frame_count'];

  function editableTarget(target) {
    if (!target || !target.closest) return false;
    return !!target.closest('input, textarea, select, button, [contenteditable="true"], dialog, .mpick-panel, .media-stage');
  }

  function numericId(element) {
    const id = String(element && element.id || '');
    return id.indexOf('node-') === 0 ? id.slice(5) : '';
  }

  function clamp(value, low, high) {
    return Math.max(low, Math.min(high, value));
  }

  function safeJsonValue(value, depth) {
    if (depth > 5) return false;
    if (typeof value === 'number') return Number.isFinite(value);
    if (value === null || ['string', 'boolean'].includes(typeof value)) return true;
    if (Array.isArray(value)) return value.length <= 200 && value.every(item => safeJsonValue(item, depth + 1));
    if (!value || typeof value !== 'object') return false;
    return Object.keys(value).length <= 200 && Object.keys(value).every(key =>
      !['__proto__', 'prototype', 'constructor'].includes(key) && safeJsonValue(value[key], depth + 1));
  }

  function install(options) {
    options = options || {};
    const editor = options.editor;
    const canvas = options.canvas;
    const getMeta = options.getMeta;
    const addInputNode = options.addInputNode;
    const addServiceNode = options.addServiceNode;
    const exportGraph = options.exportGraph;
    const toast = typeof options.toast === 'function' ? options.toast : function () {};
    const nodeLimit = clamp(Number(options.nodeLimit) || 200, 1, 1000);
    if (!editor || !canvas || !getMeta || !addInputNode || !addServiceNode || !exportGraph) {
      throw new Error('AINodeGroups.install requires editor, canvas and node helpers');
    }

    const selected = new Set();
    let spaceDown = false;
    let marquee = null;
    let groupDrag = null;
    let pan = null;
    let menu = null;
    let pasteCount = 0;
    const imageSizeCache = new Map();
    const pageKey = location.origin + location.pathname + location.search;

    injectStyles();
    canvas.tabIndex = 0;

    function nodeElement(id) {
      return canvas.querySelector('#node-' + CSS.escape(String(id)));
    }

    function allNodeElements() {
      return Array.from(canvas.querySelectorAll('.drawflow-node[id^="node-"]'));
    }

    function pruneSelection() {
      selected.forEach(id => { if (!nodeElement(id)) selected.delete(id); });
    }

    function paintSelection() {
      pruneSelection();
      allNodeElements().forEach(element => {
        element.classList.toggle('multi-selected', selected.has(numericId(element)));
      });
    }

    function clearSelection() {
      selected.clear();
      paintSelection();
    }

    function selectOnly(id) {
      selected.clear();
      if (id) selected.add(String(id));
      paintSelection();
    }

    function toggleSelection(id) {
      id = String(id);
      if (selected.has(id)) selected.delete(id); else selected.add(id);
      paintSelection();
    }

    function selectAll() {
      selected.clear();
      allNodeElements().slice(0, nodeLimit).forEach(element => selected.add(numericId(element)));
      paintSelection();
    }

    function beginMarquee(event) {
      closeMenu();
      if (!(event.ctrlKey || event.metaKey)) clearSelection();
      const overlay = document.createElement('div');
      overlay.className = 'ai-group-marquee';
      document.body.appendChild(overlay);
      marquee = {
        startX: event.clientX, startY: event.clientY, overlay,
        additive: !!(event.ctrlKey || event.metaKey), before: new Set(selected)
      };
      updateMarquee(event);
      document.addEventListener('mousemove', updateMarquee, true);
      document.addEventListener('mouseup', endMarquee, true);
    }

    function updateMarquee(event) {
      if (!marquee) return;
      const left = Math.min(marquee.startX, event.clientX);
      const top = Math.min(marquee.startY, event.clientY);
      const right = Math.max(marquee.startX, event.clientX);
      const bottom = Math.max(marquee.startY, event.clientY);
      Object.assign(marquee.overlay.style, {
        left: left + 'px', top: top + 'px', width: (right - left) + 'px', height: (bottom - top) + 'px'
      });
      selected.clear();
      if (marquee.additive) marquee.before.forEach(id => selected.add(id));
      allNodeElements().forEach(element => {
        const box = element.getBoundingClientRect();
        if (box.right >= left && box.left <= right && box.bottom >= top && box.top <= bottom) {
          selected.add(numericId(element));
        }
      });
      paintSelection();
    }

    function endMarquee(event) {
      if (!marquee) return;
      updateMarquee(event);
      marquee.overlay.remove();
      marquee = null;
      document.removeEventListener('mousemove', updateMarquee, true);
      document.removeEventListener('mouseup', endMarquee, true);
    }

    function beginGroupDrag(event, node) {
      const ids = Array.from(selected);
      const data = editor.export().drawflow.Home.data;
      groupDrag = {
        startX: event.clientX,
        startY: event.clientY,
        positions: new Map(ids.map(id => [id, { x: data[id].pos_x, y: data[id].pos_y }]))
      };
      document.addEventListener('mousemove', updateGroupDrag, true);
      document.addEventListener('mouseup', endGroupDrag, true);
    }

    function updateGroupDrag(event) {
      if (!groupDrag) return;
      event.preventDefault();
      const zoom = Number(editor.zoom) || 1;
      const dx = (event.clientX - groupDrag.startX) / zoom;
      const dy = (event.clientY - groupDrag.startY) / zoom;
      const data = editor.drawflow.drawflow[editor.module].data;
      groupDrag.positions.forEach((position, id) => {
        const element = nodeElement(id);
        if (!element || !data[id]) return;
        const x = position.x + dx;
        const y = position.y + dy;
        element.style.left = x + 'px';
        element.style.top = y + 'px';
        data[id].pos_x = x;
        data[id].pos_y = y;
        editor.updateConnectionNodes('node-' + id);
      });
    }

    function endGroupDrag(event) {
      if (!groupDrag) return;
      updateGroupDrag(event);
      groupDrag = null;
      document.removeEventListener('mousemove', updateGroupDrag, true);
      document.removeEventListener('mouseup', endGroupDrag, true);
    }

    function onMouseDown(event) {
      const node = event.target.closest && event.target.closest('.drawflow-node');
      if (!editableTarget(event.target)) canvas.focus({preventScroll:true});
      const rightPan = event.button === 2 && !node && !editableTarget(event.target);
      if (rightPan || event.button === 1 || (event.button === 0 && spaceDown && !editableTarget(event.target))) {
        event.preventDefault(); event.stopImmediatePropagation();
        closeMenu();
        pan = {x:event.clientX, y:event.clientY, left:editor.canvas_x, top:editor.canvas_y};
        const move = moveEvent => {
          moveEvent.preventDefault(); moveEvent.stopImmediatePropagation();
          editor.canvas_x = pan.left + moveEvent.clientX - pan.x;
          editor.canvas_y = pan.top + moveEvent.clientY - pan.y;
          editor.precanvas.style.transform = 'translate(' + editor.canvas_x + 'px, ' + editor.canvas_y + 'px) scale(' + editor.zoom + ')';
        };
        const up = upEvent => { upEvent.preventDefault(); upEvent.stopImmediatePropagation(); pan=null; document.removeEventListener('mousemove',move,true); document.removeEventListener('mouseup',up,true); };
        document.addEventListener('mousemove',move,true); document.addEventListener('mouseup',up,true);
        return;
      }
      // Drawflow selects connections on mousedown regardless of button.
      // Right clicks belong exclusively to our pan or node context menu.
      if (event.button === 2 && !editableTarget(event.target)) {
        event.preventDefault(); event.stopImmediatePropagation(); return;
      }
      if (event.button !== 0) return;
      if (!node) {
        if (event.target.closest && event.target.closest('.connection, .main-path')) return;
        event.preventDefault();
        event.stopImmediatePropagation();
        beginMarquee(event);
        return;
      }
      const id = numericId(node);
      if (event.ctrlKey || event.metaKey) {
        event.preventDefault();
        event.stopImmediatePropagation();
        toggleSelection(id);
        return;
      }
      const onHeader = event.target.closest('.nhead') && !editableTarget(event.target);
      if (onHeader && selected.has(id) && selected.size > 1) {
        event.preventDefault();
        event.stopImmediatePropagation();
        beginGroupDrag(event, node);
        return;
      }
      if (!selected.has(id) || selected.size !== 1) selectOnly(id);
    }

    function safeGraph() {
      const graph = exportGraph();
      return graph && Array.isArray(graph.nodes) && Array.isArray(graph.links) ? graph : null;
    }

    function clipboardPayload() {
      pruneSelection();
      if (!selected.size) return null;
      const graph = safeGraph();
      if (!graph) return null;
      const ids = new Set(Array.from(selected, String));
      const nodes = graph.nodes.filter(node => ids.has(String(node.id))).slice(0, nodeLimit).map(node => {
        if (node.kind === 'input') {
          return { id: String(node.id), kind: 'input', entity_type: String(node.entity_type || ''),
            value: typeof node.value === 'string' && !node.value.startsWith('data:') ? node.value : '',
            x: Number(node.x) || 0, y: Number(node.y) || 0, params: {_display_mode: node.params && node.params._display_mode} };
        }
        return { id: String(node.id), kind: 'service', service: String(node.service || ''),
          x: Number(node.x) || 0, y: Number(node.y) || 0,
          params: node.params && typeof node.params === 'object' ? JSON.parse(JSON.stringify(node.params)) : {} };
      });
      const kept = new Set(nodes.map(node => node.id));
      const links = graph.links.filter(link => kept.has(String(link.from)) && kept.has(String(link.to))).map(link => ({
        from: String(link.from), to: String(link.to), output: String(link.output || ''), input: String(link.input || '')
      }));
      return { version: VERSION, source: pageKey, nodes, links };
    }

    function writeClipboard(event) {
      const payload = clipboardPayload();
      if (!payload) return false;
      const text = TEXT_PREFIX + JSON.stringify(payload);
      if (text.length > MAX_CLIPBOARD_BYTES) { toast('Selection is too large to copy.'); return false; }
      if (event && event.clipboardData) {
        event.preventDefault();
        event.clipboardData.setData(MIME, JSON.stringify(payload));
        event.clipboardData.setData('text/plain', text);
        toast(payload.nodes.length + ' nodes copied.');
        return true;
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(() => toast(payload.nodes.length + ' nodes copied.'),
          () => toast('Clipboard access was refused.'));
        return true;
      }
      return false;
    }

    function parsePayload(raw) {
      if (typeof raw !== 'string' || raw.length > MAX_CLIPBOARD_BYTES) return null;
      if (raw.indexOf(TEXT_PREFIX) === 0) raw = raw.slice(TEXT_PREFIX.length);
      let payload;
      try { payload = JSON.parse(raw); } catch (_) { return null; }
      if (!payload || payload.version !== VERSION || !Array.isArray(payload.nodes) || !Array.isArray(payload.links)) return null;
      if (!payload.nodes.length || payload.nodes.length > nodeLimit) return null;
      const ids = new Set();
      for (const node of payload.nodes) {
        if (!node || typeof node.id !== 'string' || ids.has(node.id)) return null;
        ids.add(node.id);
        if (node.kind === 'input') {
          if (typeof node.entity_type !== 'string' || !node.entity_type || typeof node.value !== 'string') return null;
        } else if (node.kind === 'service') {
          if (typeof node.service !== 'string' || !/^[a-z0-9_-]{1,80}$/i.test(node.service) ||
              !node.params || typeof node.params !== 'object' || Array.isArray(node.params) || !safeJsonValue(node.params, 0)) return null;
        } else return null;
        if (!Number.isFinite(Number(node.x)) || !Number.isFinite(Number(node.y))) return null;
      }
      for (const link of payload.links) {
        if (!link || !ids.has(String(link.from)) || !ids.has(String(link.to)) ||
            typeof link.output !== 'string' || typeof link.input !== 'string') return null;
      }
      return payload;
    }

    function pastePayload(payload) {
      const currentCount = allNodeElements().length;
      if (currentCount + payload.nodes.length > nodeLimit) {
        toast('The graph is limited to ' + nodeLimit + ' nodes.'); return;
      }
      const samePage = payload.source === pageKey;
      const xs = payload.nodes.map(node => Number(node.x));
      const ys = payload.nodes.map(node => Number(node.y));
      const minX = Math.min.apply(null, xs), minY = Math.min.apply(null, ys);
      const maxX = Math.max.apply(null, xs), maxY = Math.max.apply(null, ys);
      let offsetX = 80 * (++pasteCount), offsetY = 80 * pasteCount;
      if (!samePage) {
        const bounds = canvas.getBoundingClientRect();
        const centerX = (bounds.width / 2 - Number(editor.canvas_x || 0)) / (Number(editor.zoom) || 1);
        const centerY = (bounds.height / 2 - Number(editor.canvas_y || 0)) / (Number(editor.zoom) || 1);
        offsetX = centerX - (minX + maxX) / 2;
        offsetY = centerY - (minY + maxY) / 2;
      }
      const mapping = new Map();
      for (const node of payload.nodes) {
        let id = null;
        if (node.kind === 'input') id = addInputNode(node.entity_type, Number(node.x) + offsetX, Number(node.y) + offsetY, node.value, node.params);
        else id = addServiceNode(node.service, Number(node.x) + offsetX, Number(node.y) + offsetY, JSON.parse(JSON.stringify(node.params)));
        if (!id) {
          mapping.forEach(newId => editor.removeNodeId('node-' + newId));
          toast('Paste contains an unavailable node type.');
          return;
        }
        mapping.set(node.id, String(id));
      }
      for (const link of payload.links) {
        const from = mapping.get(String(link.from)), to = mapping.get(String(link.to));
        const fromMeta = from && getMeta(from), toMeta = to && getMeta(to);
        if (!fromMeta || !toMeta) continue;
        const outIndex = (fromMeta.outFields || []).indexOf(link.output);
        const inIndex = (toMeta.inFields || []).indexOf(link.input);
        if (outIndex < 0 || inIndex < 0) continue;
        editor.addConnection(from, to, 'output_' + (outIndex + 1), 'input_' + (inIndex + 1));
      }
      selected.clear();
      mapping.forEach(id => selected.add(id));
      paintSelection();
      toast(mapping.size + ' nodes pasted.');
    }

    function onPaste(event) {
      if (editableTarget(event.target)) return;
      const items = Array.from(event.clipboardData && event.clipboardData.items || []);
      if (items.some(item => String(item.type || '').indexOf('image/') === 0)) return;
      const raw = (event.clipboardData && (event.clipboardData.getData(MIME) || event.clipboardData.getData('text/plain'))) || '';
      const payload = parsePayload(raw);
      if (!payload) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      pastePayload(payload);
    }

    function parameterFields() {
      const records = new Map();
      Array.from(selected).forEach(id => {
        const node = nodeElement(id);
        if (!node) return;
        node.querySelectorAll('[data-param]').forEach(control => {
          const name = String(control.dataset.param || '');
          if (!name || control.type === 'hidden' || control.type === 'file' || /prompt|model|checkpoint|lora/i.test(name)) return;
          if (!['number', 'range', 'select-one'].includes(control.type)) return;
          if (!records.has(name)) records.set(name, []);
          records.get(name).push(control);
        });
      });
      return Array.from(records, ([name, controls]) => ({ name, controls })).sort((a, b) => {
        const ai = PRIORITY_PARAMS.indexOf(a.name), bi = PRIORITY_PARAMS.indexOf(b.name);
        if (ai >= 0 || bi >= 0) return (ai < 0 ? 99 : ai) - (bi < 0 ? 99 : bi);
        return a.name.localeCompare(b.name);
      });
    }

    function imageDimensions(url) {
      url = String(url || '').trim();
      if (!/^(https?:|data:image\/)/i.test(url)) return Promise.reject(new Error('Image source has no readable URL.'));
      if (!imageSizeCache.has(url)) {
        imageSizeCache.set(url, new Promise((resolve, reject) => {
          const image = new Image();
          image.onload = () => image.naturalWidth && image.naturalHeight
            ? resolve({ width: image.naturalWidth, height: image.naturalHeight, url })
            : reject(new Error('Image has no dimensions.'));
          image.onerror = () => reject(new Error('Could not read the input image size.'));
          image.src = url;
        }).catch(error => { imageSizeCache.delete(url); throw error; }));
      }
      return imageSizeCache.get(url);
    }

    function upstreamCandidates(graph, targetId) {
      const nodes = new Map(graph.nodes.map(node => [String(node.id), node]));
      const incoming = new Map();
      graph.links.forEach(link => {
        const to = String(link.to), from = String(link.from);
        if (!incoming.has(to)) incoming.set(to, []);
        incoming.get(to).push(from);
      });
      const queue = (incoming.get(String(targetId)) || []).map(id => ({ id, distance: 1 }));
      const visited = new Set([String(targetId)]);
      const originals = [], generated = [];
      while (queue.length && visited.size <= nodeLimit) {
        const current = queue.shift();
        if (visited.has(current.id)) continue;
        visited.add(current.id);
        const node = nodes.get(current.id);
        if (!node) continue;
        if (node.kind === 'input' && node.entity_type === 'image') {
          const element = nodeElement(current.id);
          const liveValue = element && element.querySelector('[data-value]');
          originals.push({ distance: current.distance, url: (liveValue && liveValue.value) || node.value || '' });
        } else if (!selected.has(current.id)) {
          const result = graph.results && graph.results[current.id];
          const params = node.params || {};
          if (result && result.status === 'done' && result.type === 'image' && result.value) {
            generated.push({ distance: current.distance, url: result.value });
          } else if (Number.isFinite(Number(params.width)) && Number.isFinite(Number(params.height))) {
            generated.push({ distance: current.distance, width: Number(params.width), height: Number(params.height) });
          }
        }
        (incoming.get(current.id) || []).forEach(id => queue.push({ id, distance: current.distance + 1 }));
      }
      return { originals: originals.sort((a, b) => a.distance - b.distance),
        generated: generated.sort((a, b) => a.distance - b.distance) };
    }

    async function resolveInputDimensions(graph, targetId) {
      const candidates = upstreamCandidates(graph, targetId);
      let lastError = null;
      for (const candidate of candidates.originals.concat(candidates.generated)) {
        if (candidate.width && candidate.height) return candidate;
        try { return await imageDimensions(candidate.url); } catch (error) { lastError = error; }
      }
      throw lastError || new Error('No upstream image was found for this node.');
    }

    function setDimensionControl(control, value) {
      const previous = control.value;
      if (control.tagName === 'SELECT' && !Array.from(control.options).some(option => option.value === String(value))) {
        control.add(new Option(String(value), String(value)));
      }
      control.value = String(value);
      if (!control.checkValidity()) { control.value = previous; return false; }
      control.dispatchEvent(new Event('input', { bubbles: true }));
      control.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    }

    async function matchInputImageSizes(button, fields) {
      if (button.disabled) return;
      button.disabled = true;
      const originalText = button.textContent;
      button.textContent = 'Matching…';
      const graph = safeGraph();
      let applied = 0, missing = 0, outside = 0, evenAdjusted = 0;
      try {
        for (const id of Array.from(selected)) {
          const node = nodeElement(id);
          const widthControl = node && node.querySelector('[data-param="width"]');
          const heightControl = node && node.querySelector('[data-param="height"]');
          if (!widthControl || !heightControl) continue;
          let dimensions;
          try { dimensions = await resolveInputDimensions(graph, id); }
          catch (_) { missing += 1; continue; }
          let width = Math.round(Number(dimensions.width));
          let height = Math.round(Number(dimensions.height));
          if (width < 256 || width > 2048 || height < 256 || height > 2048) { outside += 1; continue; }
          const item = getMeta(id) || {};
          if (item.service === 'video') {
            const evenWidth = Math.round(width / 2) * 2;
            const evenHeight = Math.round(height / 2) * 2;
            if (evenWidth !== width || evenHeight !== height) evenAdjusted += 1;
            width = evenWidth; height = evenHeight;
          }
          if (setDimensionControl(widthControl, width) && setDimensionControl(heightControl, height)) applied += 1;
        }
        fields.filter(field => field.name === 'width' || field.name === 'height').forEach(field => {
          const bulk = menu && menu.querySelector('[data-bulk-param="' + CSS.escape(field.name) + '"]');
          if (!bulk) return;
          const values = new Set(field.controls.map(control => control.value));
          bulk.value = values.size === 1 ? field.controls[0].value : '';
        });
        const notes = [];
        if (outside) notes.push('not applied to ' + outside + ' nodes: input size is outside 256–2048 px');
        if (missing) notes.push('no upstream image for ' + missing + ' nodes');
        if (evenAdjusted) notes.push(evenAdjusted + ' video nodes rounded to even pixels');
        toast(applied + ' nodes matched input image size' + (notes.length ? '; ' + notes.join('; ') : '') + '.');
      } finally {
        button.disabled = false;
        button.textContent = originalText;
      }
    }

    function closeMenu() {
      if (menu) menu.remove();
      menu = null;
    }

    function openMenu(event) {
      closeMenu();
      const fields = parameterFields();
      menu = document.createElement('form');
      menu.className = 'ai-group-menu';
      menu.setAttribute('role', 'dialog');
      menu.setAttribute('aria-label', 'Edit selected nodes');
      const title = document.createElement('strong');
      title.textContent = 'Edit ' + selected.size + ' selected nodes';
      menu.appendChild(title);
      const dirty = new Set();
      fields.forEach(field => {
        const label = document.createElement('label');
        const caption = document.createElement('span');
        caption.textContent = field.name.replace(/_/g, ' ') + ' (' + field.controls.length + ' nodes)';
        const source = field.controls[0];
        let control;
        if (source.tagName === 'SELECT' && !['width','height'].includes(field.name)) {
          control = document.createElement('select');
          const blank = document.createElement('option'); blank.value = ''; blank.textContent = 'Mixed / unchanged';
          control.appendChild(blank);
          Array.from(source.options).filter(option => field.controls.every(target =>
            target.tagName !== 'SELECT' || Array.from(target.options).some(item => item.value === option.value))).forEach(option => {
            const copy = document.createElement('option'); copy.value = option.value; copy.textContent = option.textContent;
            control.appendChild(copy);
          });
        } else {
          control = document.createElement('input'); control.type = 'number';
          ['min', 'max', 'step'].forEach(name => { if (source[name] !== '') control[name] = source[name]; });
          if (['width','height'].includes(field.name)) { control.min='256'; control.max='2048'; control.step='1'; }
          control.placeholder = 'Mixed';
        }
        const values = new Set(field.controls.map(item => item.value));
        if (values.size === 1) control.value = field.controls[0].value; else control.value = '';
        control.dataset.bulkParam = field.name;
        control.addEventListener('input', () => dirty.add(field.name));
        control.addEventListener('change', () => dirty.add(field.name));
        label.append(caption, control); menu.appendChild(label);
      });
      if (!fields.length) {
        const empty = document.createElement('p'); empty.textContent = 'No shared numeric or select parameters.'; menu.appendChild(empty);
      }
      const dimensionTargets = Array.from(selected).filter(id => {
        const node = nodeElement(id);
        return node && node.querySelector('[data-param="width"]') && node.querySelector('[data-param="height"]');
      }).length;
      if (dimensionTargets) {
        const match = document.createElement('button');
        match.type = 'button';
        match.className = 'ai-group-match-size';
        match.textContent = 'Match input image size';
        match.title = 'Use the nearest upstream original image. Video dimensions are rounded to even pixels.';
        match.addEventListener('click', () => matchInputImageSizes(match, fields));
        menu.appendChild(match);
        const note = document.createElement('small');
        note.textContent = 'Applies to ' + dimensionTargets + ' selected nodes. Videos use even dimensions.';
        menu.appendChild(note);
      }
      const actions = document.createElement('div'); actions.className = 'ai-group-menu-actions';
      const apply = document.createElement('button'); apply.type = 'submit'; apply.textContent = 'Apply';
      const cancel = document.createElement('button'); cancel.type = 'button'; cancel.textContent = 'Cancel'; cancel.addEventListener('click', closeMenu);
      actions.append(apply, cancel); menu.appendChild(actions);
      menu.addEventListener('submit', submitEvent => {
        submitEvent.preventDefault();
        fields.forEach(field => {
          if (!dirty.has(field.name)) return;
          const value = menu.querySelector('[data-bulk-param="' + CSS.escape(field.name) + '"]').value;
          if (value === '') return;
          field.controls.forEach(control => {
            const previous = control.value;
            if (control.tagName === 'SELECT' && ['width','height'].includes(field.name) &&
                !Array.from(control.options).some(option => option.value === value)) {
              control.add(new Option(value, value));
            }
            control.value = value;
            if (!control.checkValidity()) { control.value = previous; return; }
            control.dispatchEvent(new Event('input', { bubbles: true }));
            control.dispatchEvent(new Event('change', { bubbles: true }));
          });
        });
        closeMenu(); toast('Selected node parameters updated.');
      });
      document.body.appendChild(menu);
      const box = menu.getBoundingClientRect();
      menu.style.left = clamp(event.clientX, 8, window.innerWidth - box.width - 8) + 'px';
      menu.style.top = clamp(event.clientY, 8, window.innerHeight - box.height - 8) + 'px';
      const first = menu.querySelector('input, select, button'); if (first) first.focus();
    }

    function onContextMenu(event) {
      if (editableTarget(event.target)) return;
      const node = event.target.closest && event.target.closest('.drawflow-node');
      event.preventDefault(); event.stopImmediatePropagation();
      if (!node) return;
      const id = numericId(node);
      if (!selected.has(id)) selectOnly(id);
      openMenu(event);
    }

    function onKeyDown(event) {
      if (event.key === ' ') spaceDown = true;
      if (event.key === 'Escape') { closeMenu(); return; }
      if (editableTarget(event.target)) return;
      const command = event.ctrlKey || event.metaKey;
      if (command && event.key.toLowerCase() === 'a') { event.preventDefault(); selectAll(); }
      if (command && event.key.toLowerCase() === 'c' && selected.size) {
        event.preventDefault();
        if (!document.execCommand('copy')) writeClipboard(null);
      }
    }

    function onKeyUp(event) { if (event.key === ' ') spaceDown = false; }
    function onOutsidePointer(event) { if (menu && !menu.contains(event.target)) closeMenu(); }

    canvas.addEventListener('mousedown', onMouseDown, true);
    canvas.addEventListener('contextmenu', onContextMenu, true);
    document.addEventListener('keydown', onKeyDown, true);
    document.addEventListener('keyup', onKeyUp, true);
    function onCopy(event) {
      if (editableTarget(event.target) || editableTarget(document.activeElement)) return;
      writeClipboard(event);
    }

    document.addEventListener('copy', onCopy, true);
    document.addEventListener('paste', onPaste, true);
    document.addEventListener('mousedown', onOutsidePointer, true);

    return {
      selected,
      clearSelection,
      selectAll,
      destroy: function () {
        closeMenu();
        canvas.removeEventListener('mousedown', onMouseDown, true);
        canvas.removeEventListener('contextmenu', onContextMenu, true);
        document.removeEventListener('keydown', onKeyDown, true);
        document.removeEventListener('keyup', onKeyUp, true);
        document.removeEventListener('copy', onCopy, true);
        document.removeEventListener('paste', onPaste, true);
        document.removeEventListener('mousedown', onOutsidePointer, true);
      }
    };
  }

  function injectStyles() {
    if (document.getElementById('ai-node-groups-style')) return;
    const style = document.createElement('style');
    style.id = 'ai-node-groups-style';
    style.textContent = [
      '.drawflow .drawflow-node.multi-selected{outline:2px solid #22d3ee;outline-offset:3px;box-shadow:0 0 0 1px rgba(34,211,238,.35),0 12px 30px rgba(0,0,0,.28)}',
      '.ai-group-marquee{position:fixed;z-index:9998;border:1px solid #38bdf8;background:rgba(56,189,248,.15);pointer-events:none}',
      '.ai-group-menu{position:fixed;z-index:10020;min-width:260px;max-height:min(520px,85vh);overflow:auto;padding:12px;border:1px solid #334155;border-radius:10px;background:#0f172a;color:#e2e8f0;box-shadow:0 18px 55px rgba(0,0,0,.5);display:grid;gap:9px}',
      '.ai-group-menu label{display:grid;grid-template-columns:minmax(120px,1fr) minmax(90px,130px);gap:10px;align-items:center;text-transform:capitalize;font-size:12px}',
      '.ai-group-menu input,.ai-group-menu select{min-width:0;background:#111827;color:#e2e8f0;border:1px solid #475569;border-radius:6px;padding:6px}',
      '.ai-group-menu-actions{display:flex;justify-content:flex-end;gap:8px;padding-top:4px}',
      '.ai-group-menu button{border:1px solid #475569;border-radius:6px;background:#1e293b;color:#e2e8f0;padding:6px 10px}'
    ].join('');
    document.head.appendChild(style);
  }

  window.AINodeGroups = { install: install };
})();

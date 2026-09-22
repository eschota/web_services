/** A/B source comparison and lightweight per-node render history. */
(function (global) {
  'use strict';

  const MAX_HISTORY = 5;

  function visualRecord(record) {
    return !!record && ['done', 'stale'].includes(record.status) && !!record.value &&
      (record.type === 'image' || record.type === 'video' || String(record.type || '').indexOf('control_') === 0);
  }

  function inferType(url, fallback) {
    return /\.(?:mp4|webm|mov)(?:$|[?#])/i.test(String(url || '')) ? 'video'
      : (String(fallback || '').startsWith('control_') ? 'image' : (fallback || 'image'));
  }

  function strictHttpUrl(value) {
    try {
      value = String(value || '').trim();
      if (!/^https?:\/\//i.test(value)) return '';
      const url = new URL(value);
      return /^https?:$/.test(url.protocol) && !url.username && !url.password ? url.href : '';
    } catch (_) { return ''; }
  }

  /** Carry history through running/failed states and add the previous visual
   * result once. The current value always remains the caller's new record. */
  function enhanceRecord(newRecord, previousRecord) {
    const next = Object.assign({}, newRecord || {});
    const prior = previousRecord || {};
    const entries = [];
    const timestamp = item => Number(item && item.created_at) || 0;
    const add = item => {
      if (!item || !item.value) return;
      const mediaType = inferType(item.value, item.type === 'video' ? 'video' : 'image');
      if (visualRecord(next) && item.value === next.value) return;
      if (entries.some(existing => existing.value === item.value)) return;
      entries.push({
        type: mediaType, value: String(item.value),
        input_reference_url: String(item.input_reference_url || ''),
        created_at: Number(item.created_at) || Math.floor(Date.parse(String(item.created_at || '')) / 1000) || Math.floor(Date.now() / 1000)
      });
    };
    if (visualRecord(prior)) add({
      type: prior.type === 'video' ? 'video' : 'image', value: prior.value,
      input_reference_url: prior.input_reference_url || '', created_at: prior.created_at
    });
    const saved = (Array.isArray(prior.history) ? prior.history : [])
      .concat(Array.isArray(next.history) ? next.history : [])
      .sort((a, b) => timestamp(b) - timestamp(a));
    saved.forEach(add);
    next.history = entries.sort((a, b) => timestamp(b) - timestamp(a)).slice(0, MAX_HISTORY);
    if (!next.input_reference_url && prior.input_reference_url) next.input_reference_url = prior.input_reference_url;
    return next;
  }

  function install(options) {
    options = options || {};
    const canvas = options.canvas;
    const getMeta = options.getMeta || function () { return null; };
    const getGraph = options.getGraph || function () { return null; };
    const getResult = options.getResult || function () { return null; };
    const getAnchorId = options.getAnchorId || function () { return null; };
    const setAnchorId = options.setAnchorId || function () {};
    const toast = typeof options.toast === 'function' ? options.toast : function () {};
    if (!canvas) throw new Error('AINodeCompare.install requires canvas');

    installStyles();
    let observer = null;
    let stopped = false;
    let hoverOverlay = null;
    const resizeObservers = new Map();

    function idOf(element) { return String(element && element.id || '').replace(/^node-/, ''); }
    function nodeElement(id) {
      try { return canvas.querySelector('#node-' + CSS.escape(String(id))); }
      catch (_) { return null; }
    }
    function graphNodes(graph) { return new Map(((graph && graph.nodes) || []).map(node => [String(node.id), node])); }

    const httpUrl = strictHttpUrl;

    function inputImageFromNode(node, graph) {
      if (!node || node.kind !== 'input' || node.entity_type !== 'image') return '';
      return httpUrl(node.value);
    }

    function resolveReference(nodeId, graphSnapshot) {
      const graph = graphSnapshot || getGraph() || {};
      const nodes = graphNodes(graph);
      const start = String(nodeId);
      const own = inputImageFromNode(nodes.get(start), graph);
      if (own) return own;
      const incoming = new Map();
      (graph.links || []).forEach(link => {
        const to = String(link.to);
        if (!incoming.has(to)) incoming.set(to, []);
        incoming.get(to).push(String(link.from));
      });
      const queue = (incoming.get(start) || []).slice();
      const visited = new Set([start]);
      let derivedImage = '';
      while (queue.length && visited.size <= 500) {
        const id = queue.shift();
        if (visited.has(id)) continue;
        visited.add(id);
        const found = inputImageFromNode(nodes.get(id), graph);
        if (found) return found;
        const result = graph.results && graph.results[id];
        if (result && result.type === 'image' && result.value) {
          if (nodes.get(id)?.service === 'video_frame') derivedImage = httpUrl(result.value);
          else if (!derivedImage) derivedImage = httpUrl(result.value);
        }
        (incoming.get(id) || []).forEach(parent => queue.push(parent));
      }
      return derivedImage;
    }

    function currentVisual(id) {
      const record = getResult(String(id));
      if (visualRecord(record) && httpUrl(record.value)) {
        return { type: inferType(record.value, record.type === 'video' ? 'video' : 'image'), value: httpUrl(record.value) };
      }
      const element = nodeElement(id);
      const meta = getMeta(String(id)) || {};
      if (!element) return null;
      if (meta.kind === 'input' || element.classList.contains('input-node')) {
        if (!['image', 'video', 'avatar'].includes(meta.entityType)) return null;
        const preview = element.querySelector('.ninput [data-preview]:not([hidden])');
        const value = element.querySelector('.ninput [data-value]');
        const url = preview ? httpUrl(preview.currentSrc || preview.src) : httpUrl(value && value.value);
        return url ? { type: inferType(url, meta.entityType === 'video' ? 'video' : 'image'), value: url } : null;
      }
      const video = element.querySelector('.nout video.preview-expandable');
      const image = element.querySelector('.nout img.preview-expandable');
      const url = video ? httpUrl(video.currentSrc || video.src) : (image ? httpUrl(image.currentSrc || image.src) : '');
      return url ? { type: video ? 'video' : 'image', value: url } : null;
    }

    function automaticReference(id) {
      const record = getResult(String(id));
      const visual = currentVisual(id);
      const visibleRecord = record && record.value === visual?.value ? record
        : (record?.history || []).find(item => item.value === visual?.value);
      const url = httpUrl(visibleRecord && visibleRecord.input_reference_url) || resolveReference(id, getGraph());
      return url ? { type: inferType(url, 'image'), value: url } : null;
    }

    function anchorReference() {
      const id = getAnchorId();
      if (id == null || id === '') return null;
      return currentVisual(String(id)) || (function () {
        const url = resolveReference(String(id), getGraph());
        return url ? { type: inferType(url, 'image'), value: url } : null;
      })();
    }

    function comparisonReference(id) { return getAnchorId() ? anchorReference() : automaticReference(id); }

    function setAnchor(id) {
      id = String(id || '');
      const visual = currentVisual(id) || (function () {
        const url = resolveReference(id, getGraph());
        return url ? { type: inferType(url, 'image'), value: url } : null;
      })();
      if (!visual) {
        toast('This node has no visual input, output, or upstream image to use as A.');
        return false;
      }
      setAnchorId(id);
      refresh();
      toast('Comparison A set from this node.');
      return true;
    }

    function clearAnchor() {
      setAnchorId(null);
      refresh();
      toast('Comparison A cleared; each node will use its own upstream source.');
    }

    function mainMedia(element) {
      return element.querySelector('.nout > img.preview-expandable, .nout > video.preview-expandable, .ninput > img[data-preview]:not([hidden]), .ninput > video');
    }

    function mediaBox(element) {
      const media = mainMedia(element);
      if (!media) return null;
      const nodeRect = element.getBoundingClientRect();
      const mediaRect = media.getBoundingClientRect();
      const scaleX = nodeRect.width && element.offsetWidth ? nodeRect.width / element.offsetWidth : 1;
      const scaleY = nodeRect.height && element.offsetHeight ? nodeRect.height / element.offsetHeight : scaleX;
      return {
        media: media,
        left: (mediaRect.left - nodeRect.left) / (scaleX || 1),
        top: (mediaRect.top - nodeRect.top) / (scaleY || 1),
        width: mediaRect.width / (scaleX || 1),
        height: mediaRect.height / (scaleY || 1)
      };
    }

    function positionControls(element) {
      const box = mediaBox(element);
      if (!box) return;
      const ab = element.querySelector(':scope > .node-compare-ab');
      const history = element.querySelector(':scope > .node-history-strip');
      if (ab) {
        ab.style.left = Math.max(box.left + 4, box.left + box.width - ab.offsetWidth - 6) + 'px';
        ab.style.top = Math.max(box.top + 4, box.top + box.height - ab.offsetHeight - 6) + 'px';
      }
      if (history) {
        history.style.left = (box.left + 6) + 'px';
        history.style.top = Math.max(box.top + 4, box.top + box.height - history.offsetHeight - 6) + 'px';
        history.style.maxWidth = Math.max(36, box.width - (ab ? ab.offsetWidth : 0) - 24) + 'px';
      }
    }

    function watchGeometry(element) {
      if (typeof ResizeObserver === 'undefined') return;
      const id = idOf(element);
      const media = mainMedia(element);
      const existing = resizeObservers.get(id);
      if (existing && existing.media === media) return;
      if (existing) existing.observer.disconnect();
      const resize = new ResizeObserver(() => requestAnimationFrame(() => {
        if (!stopped && element.isConnected) positionControls(element);
      }));
      resize.observe(element);
      if (media) resize.observe(media);
      resizeObservers.set(id, { observer: resize, media: media });
      if (media && !media.complete) media.addEventListener('load', () => positionControls(element), { once: true });
      if (media && media.tagName === 'VIDEO') media.addEventListener('loadedmetadata', () => positionControls(element), { once: true });
    }

    function forgetGeometry(element) {
      const entry = resizeObservers.get(idOf(element));
      if (entry) entry.observer.disconnect();
      resizeObservers.delete(idOf(element));
    }

    function removeOverlay() {
      if (!hoverOverlay) return;
      const video = hoverOverlay.querySelector('video');
      if (video) video.pause();
      hoverOverlay.remove();
      hoverOverlay = null;
    }

    function showOverlay(element, media, label) {
      removeOverlay();
      const box = mediaBox(element);
      if (!box || !media || !httpUrl(media.value)) return;
      const overlay = document.createElement('div');
      overlay.className = 'node-compare-overlay';
      overlay.setAttribute('aria-label', label);
      let visual;
      if (media.type === 'video') {
        visual = document.createElement('video'); visual.src = media.value;
        visual.muted = true; visual.loop = true; visual.autoplay = true; visual.playsInline = true;
      } else {
        visual = document.createElement('img'); visual.src = media.value; visual.alt = label;
      }
      const badge = document.createElement('span'); badge.textContent = label;
      // The same pixel-size badge the node's own preview carries, so hovering
      // A against B compares two sizes as well as two pictures.
      const size = document.createElement('span');
      size.className = 'node-compare-res';
      const showSize = () => {
        const width = visual.naturalWidth || visual.videoWidth || 0;
        const height = visual.naturalHeight || visual.videoHeight || 0;
        size.textContent = width && height ? width + '×' + height : '';
      };
      visual.addEventListener('load', showSize);
      visual.addEventListener('loadedmetadata', showSize);
      showSize();
      Object.assign(overlay.style, {
        left: box.left + 'px', top: box.top + 'px', width: box.width + 'px', height: box.height + 'px'
      });
      overlay.append(visual, badge, size); element.appendChild(overlay); hoverOverlay = overlay;
      if (visual.tagName === 'VIDEO') visual.play().catch(function () {});
    }

    function historyEntries(record) {
      const seen = new Set();
      return (Array.isArray(record && record.history) ? record.history : [])
        .slice().sort((a, b) => Number(b.created_at || 0) - Number(a.created_at || 0)).reduce((items, item) => {
        const url = httpUrl(item && item.value);
        if (!url || seen.has(url)) return items;
        seen.add(url); items.push(Object.assign({}, item, { value: url })); return items;
      }, []).slice(0, MAX_HISTORY);
    }

    function interaction(button, enter, leave) {
      button.addEventListener('mouseenter', enter);
      button.addEventListener('focus', enter);
      button.addEventListener('mouseleave', leave);
      button.addEventListener('blur', leave);
      button.addEventListener('pointerdown', event => event.stopPropagation());
      button.addEventListener('dblclick', event => event.stopPropagation());
    }

    function makeHistoryButton(element, item, index) {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'node-history-item';
      const parsed = Number(item.created_at) ? Number(item.created_at) * 1000 : Date.parse(String(item.created_at || ''));
      const date = Number.isFinite(parsed) ? new Date(parsed).toLocaleString() : 'Previous result';
      button.title = 'Previous result ' + (index + 1) + ' · ' + date;
      button.setAttribute('aria-label', button.title + '. Hold focus or hover to preview.');
      if (inferType(item.value, item.type) === 'video') {
        const video = document.createElement('video'); video.src = item.value; video.muted = true;
        video.preload = 'metadata'; video.playsInline = true;
        video.addEventListener('loadedmetadata', () => { try { video.currentTime = Math.min(.05, video.duration || .05); } catch (_) {} }, { once: true });
        button.appendChild(video);
      } else {
        const image = document.createElement('img'); image.src = item.value; image.alt = ''; image.loading = 'lazy'; button.appendChild(image);
      }
      interaction(button,
        () => showOverlay(element, { type: inferType(item.value, item.type), value: item.value }, 'History ' + (index + 1)),
        removeOverlay);
      return button;
    }

    function prepare(element) {
      if (!element || !element.matches('.drawflow-node[id^="node-"]')) return;
      const id = idOf(element);
      const old = element.querySelectorAll(':scope > .node-compare-ab, :scope > .node-history-strip, .node-anchor-badge');
      old.forEach(item => item.remove());
      const current = currentVisual(id);
      const record = getResult(id) || {};
      const reference = current && comparisonReference(id);
      if (current && reference && reference.value !== current.value) {
        const controls = document.createElement('div'); controls.className = 'node-compare-ab';
        const a = document.createElement('button'); a.type = 'button'; a.textContent = 'A';
        a.title = 'Hold to preview comparison A'; a.setAttribute('aria-label', a.title);
        const b = document.createElement('span'); b.textContent = 'B'; b.title = 'Current result B';
        interaction(a, () => showOverlay(element, reference, 'A'), removeOverlay);
        controls.append(a, b); element.appendChild(controls);
      }
      const history = historyEntries(record);
      if (current && history.length) {
        const strip = document.createElement('div'); strip.className = 'node-history-strip';
        strip.setAttribute('aria-label', 'Previous outputs');
        history.forEach((item, index) => strip.appendChild(makeHistoryButton(element, item, index)));
        element.appendChild(strip);
      }
      if (String(getAnchorId() || '') === id) {
        const header = element.querySelector('.nhead');
        if (header) {
          const badge = document.createElement('button'); badge.type = 'button'; badge.className = 'node-anchor-badge';
          badge.textContent = 'A'; badge.title = 'Global comparison A. Click to clear.';
          badge.setAttribute('aria-label', badge.title); badge.addEventListener('pointerdown', event => event.stopPropagation());
          badge.addEventListener('click', event => { event.preventDefault(); event.stopPropagation(); clearAnchor(); });
          header.appendChild(badge);
        }
      }
      watchGeometry(element);
      requestAnimationFrame(() => positionControls(element));
    }

    function refresh(id) {
      removeOverlay();
      if (id != null) {
        const element = nodeElement(id); if (element) prepare(element);
      } else canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(prepare);
    }

    observer = new MutationObserver(records => {
      if (stopped) return;
      const affected = new Set();
      records.forEach(record => {
        const changed = Array.from(record.addedNodes || []).concat(Array.from(record.removedNodes || []));
        const onlyOurUi = changed.length && changed.every(item => item.nodeType !== 1 ||
          item.matches('.node-compare-ab,.node-history-strip,.node-compare-overlay,.node-anchor-badge'));
        if (onlyOurUi) return;
        record.removedNodes.forEach(removed => {
          if (removed.nodeType !== 1) return;
          if (removed.matches && removed.matches('.drawflow-node[id^="node-"]')) forgetGeometry(removed);
          if (removed.querySelectorAll) removed.querySelectorAll('.drawflow-node[id^="node-"]').forEach(forgetGeometry);
        });
        if (record.target.closest) {
          const node = record.target.closest('.drawflow-node[id^="node-"]');
          if (node && !record.target.closest('.node-compare-ab,.node-history-strip,.node-compare-overlay,.node-anchor-badge')) affected.add(node);
        }
        record.addedNodes.forEach(added => {
          if (added.nodeType !== 1) return;
          if (added.matches && added.matches('.drawflow-node[id^="node-"]')) affected.add(added);
        });
      });
      affected.forEach(prepare);
      const anchor = String(getAnchorId() || '');
      if (anchor && Array.from(affected).some(element => idOf(element) === anchor)) {
        canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(element => {
          if (idOf(element) !== anchor) prepare(element);
        });
      }
      if (anchor) requestAnimationFrame(() => {
        if (!stopped && !nodeElement(anchor)) {
          setAnchorId(null);
          toast('Comparison A was cleared because its node was deleted.');
        }
      });
    });
    observer.observe(canvas, { childList: true, subtree: true, attributes: true, attributeFilter: ['src', 'hidden'] });
    refresh();

    return {
      resolveReference: resolveReference,
      setAnchor: setAnchor,
      clearAnchor: clearAnchor,
      refresh: refresh,
      enhanceRecord: enhanceRecord,
      destroy: function () {
        stopped = true; removeOverlay();
        if (observer) observer.disconnect();
        resizeObservers.forEach(entry => entry.observer.disconnect());
        resizeObservers.clear();
        canvas.querySelectorAll('.node-compare-ab,.node-history-strip,.node-anchor-badge').forEach(item => item.remove());
      }
    };
  }

  function installStyles() {
    if (document.getElementById('ai-node-compare-style')) return;
    const style = document.createElement('style'); style.id = 'ai-node-compare-style';
    style.textContent = [
      '.drawflow .drawflow-node{--compare-control-bg:rgba(8,9,20,.88)}',
      '.drawflow .drawflow-node .nout,.drawflow .drawflow-node .ninput{position:relative}',
      '.node-compare-ab{position:absolute;z-index:8;display:flex;border:1px solid rgba(255,255,255,.22);border-radius:7px;overflow:hidden;background:var(--compare-control-bg)}',
      '.node-compare-ab button,.node-compare-ab span{width:25px;height:23px;padding:0;border:0;background:transparent;color:#cbd5e1;font:700 11px/23px Inter,sans-serif;text-align:center}',
      '.node-compare-ab button{color:#67e8f9;cursor:zoom-in}.node-compare-ab button:hover,.node-compare-ab button:focus-visible{background:rgba(56,189,248,.22);outline:none}',
      '.node-compare-ab span{background:rgba(123,92,255,.28);color:#ddd6fe}',
      '.node-history-strip{position:absolute;z-index:8;display:flex;gap:3px;padding:3px;border:1px solid rgba(255,255,255,.18);border-radius:7px;background:var(--compare-control-bg);overflow:hidden}',
      '.node-history-item{flex:0 1 29px;min-width:0;width:29px;height:25px;padding:0;overflow:hidden;border:1px solid transparent;border-radius:4px;background:#111827;cursor:zoom-in}',
      '.node-history-item:hover,.node-history-item:focus-visible{border-color:#38bdf8;outline:none}',
      '.node-history-item img,.node-history-item video{display:block;width:100%;height:100%;object-fit:cover;pointer-events:none}',
      '.node-compare-overlay{position:absolute;z-index:7;display:flex;align-items:center;justify-content:center;overflow:hidden;border-radius:8px;background:#080914;pointer-events:none}',
      '.node-compare-overlay img,.node-compare-overlay video{display:block;width:100%;height:100%;max-height:none!important;object-fit:contain;background:#080914}',
      '.node-compare-overlay>span{position:absolute;right:6px;top:6px;padding:3px 6px;border-radius:5px;background:rgba(8,9,20,.82);color:#67e8f9;font:700 10px Inter,sans-serif}',
      '.node-compare-overlay>span.node-compare-res{left:6px;right:auto;color:#f1f5ff;font-variant-numeric:tabular-nums;border:1px solid rgba(255,255,255,.14)}',
      '.node-compare-overlay>span.node-compare-res:empty{display:none}',
      '.node-anchor-badge{flex:0 0 auto;width:22px;height:22px;margin-left:3px;padding:0;border:1px solid #38bdf8;border-radius:6px;background:rgba(56,189,248,.2);color:#67e8f9;font:800 11px/20px Inter,sans-serif;cursor:pointer}',
      '.node-anchor-badge:hover,.node-anchor-badge:focus-visible{background:rgba(56,189,248,.35);outline:none}'
    ].join('\n');
    document.head.appendChild(style);
  }

  global.AINodeCompare = Object.freeze({
    install: install, enhanceRecord: enhanceRecord, isHttpUrl: strictHttpUrl,
    visualRecord: visualRecord, MAX_HISTORY: MAX_HISTORY
  });
})(window);

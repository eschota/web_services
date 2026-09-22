(function (global) {
  'use strict';

  const MIME = 'application/x-autorig-node';

  function graphPoint(clientX, clientY, rect, canvasX, canvasY, zoom) {
    const scale = Number(zoom) > 0 ? Number(zoom) : 1;
    return {
      x: (Number(clientX) - rect.left - Number(canvasX || 0)) / scale,
      y: (Number(clientY) - rect.top - Number(canvasY || 0)) / scale
    };
  }

  function viewportCenter(rect, canvasX, canvasY, zoom) {
    return graphPoint(rect.left + rect.width / 2, rect.top + rect.height / 2,
      rect, canvasX, canvasY, zoom);
  }

  function hasNodeMime(event) {
    return Array.from(event.dataTransfer && event.dataTransfer.types || []).includes(MIME);
  }

  function install(options) {
    const editor = options.editor;
    const canvas = options.canvas;
    let dragged = false;

    function pointAt(clientX, clientY) {
      return graphPoint(clientX, clientY, canvas.getBoundingClientRect(),
        editor.canvas_x, editor.canvas_y, editor.zoom);
    }

    /**
     * A click drops the node on the point, not beside it.
     *
     * Centring needs the node's measured size, which exists a frame after it
     * is created. A tab that is not painting never gets that frame, so a short
     * timer runs the same step; it recomputes from the drop point every time,
     * so running it twice lands in exactly the same place.
     */
    function create(spec, point, centerAfterCreate) {
      const id = options.createNode(spec, point.x, point.y);
      if (id == null || !centerAfterCreate) return id;
      const centre = () => {
        const element = options.getNodeElement(id);
        if (!element) return;
        const box = element.getBoundingClientRect();
        if (!box.width || !box.height) return;
        const scale = Number(editor.zoom) > 0 ? Number(editor.zoom) : 1;
        options.moveNode(id, point.x - box.width / scale / 2, point.y - box.height / scale / 2);
      };
      requestAnimationFrame(() => requestAnimationFrame(centre));
      setTimeout(centre, 60);
      return id;
    }

    function bindPaletteButton(button, spec) {
      button.draggable = true;
      button.addEventListener('dragstart', event => {
        dragged = true;
        event.dataTransfer.effectAllowed = 'copy';
        event.dataTransfer.setData(MIME, JSON.stringify(spec));
        event.dataTransfer.setData('text/plain', spec.title || spec.type || spec.service || 'node');
      });
      button.addEventListener('dragend', () => {
        setTimeout(() => { dragged = false; }, 0);
      });
      button.addEventListener('click', () => {
        if (dragged || button.disabled) return;
        const rect = canvas.getBoundingClientRect();
        create(spec, viewportCenter(rect, editor.canvas_x, editor.canvas_y, editor.zoom), true);
      });
    }

    canvas.addEventListener('dragover', event => {
      if (!hasNodeMime(event)) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = 'copy';
    });
    canvas.addEventListener('drop', event => {
      if (!hasNodeMime(event)) return;
      event.preventDefault();
      event.stopPropagation();
      let spec;
      try { spec = JSON.parse(event.dataTransfer.getData(MIME)); } catch (_) { return; }
      if (!spec || !['input', 'service'].includes(spec.kind)) return;
      create(spec, pointAt(event.clientX, event.clientY), false);
    });

    return {bindPaletteButton, pointAt};
  }

  global.AINodePlacement = {install, graphPoint, viewportCenter, MIME};
})(window);

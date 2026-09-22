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

  /**
   * A ghost the cursor is actually holding.
   *
   * The browser's own drag image is a snapshot of the dragged element, and a
   * tool button carries its tooltip — which is anchored to the strip, not to
   * the button, so the snapshot was as wide as the whole toolbar and the icon
   * inside it sat hundreds of pixels to the left of the pointer. A small
   * square built for the purpose has no such surprises: the pointer holds its
   * centre, which is also where the node lands.
   */
  const GHOST_SIZE = 38;

  function buildGhost(documentRef, button) {
    const ghost = documentRef.createElement('div');
    const icon = button.querySelector && button.querySelector('.ticon');
    ghost.textContent = (icon && icon.textContent) || '+';
    ghost.setAttribute('aria-hidden', 'true');
    // Off-screen rather than hidden: a drag image that is not being rendered
    // is not captured at all, and the drag starts with no ghost.
    ghost.style.cssText = 'position:fixed;top:-1000px;left:-1000px;z-index:-1;' +
      'display:flex;align-items:center;justify-content:center;pointer-events:none;' +
      'width:' + GHOST_SIZE + 'px;height:' + GHOST_SIZE + 'px;border-radius:10px;' +
      'font-size:19px;line-height:1;background:#1b1c33;border:1px solid #38bdf8;color:#eef;';
    documentRef.body.appendChild(ghost);
    return ghost;
  }

  function install(options) {
    const editor = options.editor;
    const canvas = options.canvas;
    const documentRef = options.document || (canvas && canvas.ownerDocument) || document;
    let dragged = false;
    let ghost = null;

    function dropGhost() {
      if (ghost && ghost.parentNode) ghost.parentNode.removeChild(ghost);
      ghost = null;
    }

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
        dropGhost();
        ghost = buildGhost(documentRef, button);
        if (event.dataTransfer.setDragImage) {
          event.dataTransfer.setDragImage(ghost, GHOST_SIZE / 2, GHOST_SIZE / 2);
        }
      });
      button.addEventListener('dragend', () => {
        dropGhost();
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
      dropGhost();
      // The pointer holds the middle of the ghost, so it lands the middle of
      // the node — the same rule a click on the same button follows.
      create(spec, pointAt(event.clientX, event.clientY), true);
    });

    return {bindPaletteButton, pointAt};
  }

  global.AINodePlacement = {install, graphPoint, viewportCenter, buildGhost, MIME, GHOST_SIZE};
})(window);

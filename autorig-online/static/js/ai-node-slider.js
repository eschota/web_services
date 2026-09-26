/*
 * Node sliders (2026-09-26): one component for every ranged parameter.
 *
 * The native <input type=range> stays in the DOM as the value holder (it is
 * what readParams/applyParams and the change listeners read), hidden behind a
 * custom track. The track owns the pointer: pointer capture, and mousedown /
 * touchstart stopped so Drawflow never drags the node or pans the canvas.
 * Positions come from getBoundingClientRect on every move, so a zoomed canvas
 * (0.5x, 2x) maps the pointer exactly.
 *
 *   drag / click on track   set value (snapped to min + n*step: frames 9..393 -> 8n+1)
 *   Shift                   fine (step / 10 when the step allows, else 1 step)
 *   Ctrl / Alt              coarse (10 steps)
 *   wheel while hovered     +-1 step (Shift fine, Ctrl coarse); canvas zoom elsewhere
 *   arrows / PgUp / PgDn    when focused; Home / End = min / max
 *   double-click value      type an exact number
 *   right-click or reset    back to the default
 */
(function (global) {
  'use strict';

  const DESCRIPTOR = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');

  function decimals(step) {
    const text = String(step);
    return text.includes('.') ? text.split('.')[1].length : 0;
  }

  function enhance(input) {
    if (!input || input._aislider || input.type !== 'range') return null;
    // Read live: the editor narrows min/max per model after the node exists.
    let min = 0, max = 100, step = 1, places = 0, origin = 0;
    function bounds() {
      min = Number(input.min || 0);
      max = Number(input.max || 100);
      step = Number(input.step) > 0 ? Number(input.step) : 1;
      places = Math.max(decimals(step), decimals(min));
      // The grid starts at min, as the browser's own range does: frames with
      // min 9 step 8 are 8n+1 (LTX); a model range (MiniMax H3 124-362) keeps its base.
      origin = min;
      box.setAttribute('aria-valuemin', String(min));
      box.setAttribute('aria-valuemax', String(max));
    }
    const fallback = input.getAttribute('value');
    const defaultValue = fallback == null || fallback === '' ? min : Number(fallback);

    const box = document.createElement('span');
    box.className = 'aislider';
    box.tabIndex = 0;
    box.setAttribute('role', 'slider');
    box.setAttribute('aria-label', input.title || input.dataset.param || 'value');
    box.title = (input.title ? input.title + ' — ' : '') +
      'drag · Shift fine · Ctrl coarse · wheel · double-click to type · right-click resets';
    const track = document.createElement('span');
    track.className = 'aislider-track';
    const fill = document.createElement('span');
    fill.className = 'aislider-fill';
    const thumb = document.createElement('span');
    thumb.className = 'aislider-thumb';
    track.append(fill, thumb);
    box.appendChild(track);

    // Inputs that already have an <output> readout keep it (it knows "auto");
    // the others get a label of their own.
    let label = input.parentElement && input.parentElement.querySelector('output[data-for="' + (input.dataset.param || '') + '"]');
    if (!label || !input.dataset.param) {
      label = document.createElement('span');
      label.className = 'aislider-value';
      input._aisliderOwnLabel = true;
    }
    label.classList.add('aislider-editable');
    label.title = 'Double-click to type an exact value';
    const reset = document.createElement('button');
    bounds();
    reset.type = 'button';
    reset.className = 'aislider-reset';
    reset.textContent = '↺';
    reset.title = 'Reset to ' + defaultValue;

    input.classList.add('aislider-native');
    input.tabIndex = -1;
    input.after(box);
    if (input._aisliderOwnLabel) box.after(label);
    label.after(reset);

    function clamp(value) { return Math.min(max, Math.max(min, value)); }
    function snap(value, unit) {
      bounds();
      const grid = unit || step;
      const base = unit && unit < step ? 0 : origin;
      let snapped = base + Math.round((value - base) / grid) * grid;
      // Stay on the grid inside the range (8n+1 between a model's min and max).
      while (snapped < min) snapped += grid;
      while (snapped > max) snapped -= grid;
      if (snapped < min) snapped = min;
      return Number(snapped.toFixed(Math.max(places, decimals(grid))));
    }
    function paint() {
      bounds();
      const value = Number(DESCRIPTOR.get.call(input));
      const ratio = max > min ? (clamp(value) - min) / (max - min) : 0;
      fill.style.width = (ratio * 100) + '%';
      thumb.style.left = (ratio * 100) + '%';
      box.setAttribute('aria-valuenow', String(value));
      if (input._aisliderOwnLabel) label.textContent = String(value);
      reset.classList.toggle('dirty', value !== defaultValue);
    }
    // applyParams and other code set .value directly; keep the track in step.
    Object.defineProperty(input, 'value', {
      configurable: true,
      get() { return DESCRIPTOR.get.call(input); },
      set(value) { DESCRIPTOR.set.call(input, value); paint(); }
    });
    function commit(value, final) {
      const before = DESCRIPTOR.get.call(input);
      DESCRIPTOR.set.call(input, String(value));
      paint();
      if (DESCRIPTOR.get.call(input) !== before) input.dispatchEvent(new Event('input', {bubbles: true}));
      if (final) input.dispatchEvent(new Event('change', {bubbles: true}));
    }
    function fineUnit() { return step >= 1 ? step : step / 10 >= 0.001 ? step / 10 : step; }
    function unitFor(event) {
      if (event.ctrlKey || event.altKey || event.metaKey) return step * 10;
      if (event.shiftKey) return fineUnit();
      return step;
    }
    function fromPointer(event) {
      const rect = track.getBoundingClientRect();
      const ratio = rect.width > 0 ? (event.clientX - rect.left) / rect.width : 0;
      const unit = event.shiftKey ? fineUnit() : step;
      return snap(min + Math.min(1, Math.max(0, ratio)) * (max - min), unit);
    }

    // Keep Drawflow's mousedown/touchstart handlers off the slider entirely.
    ['mousedown', 'touchstart', 'dblclick', 'click', 'contextmenu'].forEach(type =>
      [box, label, reset].forEach(element => element.addEventListener(type, event => event.stopPropagation())));

    let dragging = false;
    box.addEventListener('pointerdown', event => {
      if (event.button !== undefined && event.button !== 0) return;
      event.preventDefault();
      event.stopPropagation();
      box.focus({preventScroll: true});
      dragging = true;
      box.classList.add('dragging');
      try { box.setPointerCapture(event.pointerId); } catch (error) { /* synthetic */ }
      commit(fromPointer(event), false);
    });
    box.addEventListener('pointermove', event => {
      if (!dragging) return;
      event.preventDefault();
      event.stopPropagation();
      commit(fromPointer(event), false);
    });
    const end = event => {
      if (!dragging) return;
      dragging = false;
      box.classList.remove('dragging');
      try { box.releasePointerCapture(event.pointerId); } catch (error) { /* already */ }
      commit(DESCRIPTOR.get.call(input), true);
    };
    box.addEventListener('pointerup', end);
    box.addEventListener('pointercancel', end);
    box.style.touchAction = 'none';

    box.addEventListener('wheel', event => {
      event.preventDefault();
      event.stopPropagation();
      const direction = (event.deltaY || event.deltaX) < 0 ? 1 : -1;
      const unit = unitFor(event);
      commit(snap(Number(DESCRIPTOR.get.call(input)) + direction * unit, unit), true);
    }, {passive: false});

    box.addEventListener('keydown', event => {
      const value = Number(DESCRIPTOR.get.call(input));
      const unit = unitFor(event);
      let next = null;
      if (event.key === 'ArrowRight' || event.key === 'ArrowUp') next = value + unit;
      else if (event.key === 'ArrowLeft' || event.key === 'ArrowDown') next = value - unit;
      else if (event.key === 'PageUp') next = value + step * 10;
      else if (event.key === 'PageDown') next = value - step * 10;
      else if (event.key === 'Home') next = min;
      else if (event.key === 'End') next = max;
      else if (event.key === 'Enter') { editExact(); event.preventDefault(); return; }
      if (next == null) return;
      event.preventDefault();
      event.stopPropagation();
      commit(snap(next, unit), true);
    });

    function resetToDefault(event) {
      if (event) { event.preventDefault(); event.stopPropagation(); }
      commit(snap(defaultValue), true);
    }
    box.addEventListener('contextmenu', resetToDefault);
    reset.addEventListener('click', resetToDefault);

    function editExact() {
      if (box._editing) return;
      box._editing = true;
      const field = document.createElement('input');
      field.type = 'text';
      field.inputMode = 'decimal';
      field.className = 'aislider-edit';
      field.value = String(DESCRIPTOR.get.call(input));
      label.after(field);
      label.hidden = true;
      field.focus();
      field.select();
      ['mousedown', 'pointerdown', 'touchstart', 'wheel', 'dblclick'].forEach(type =>
        field.addEventListener(type, event => event.stopPropagation()));
      let done = false;
      const finish = keep => {
        if (done) return;
        done = true;
        box._editing = false;
        const typed = Number(String(field.value).replace(',', '.'));
        field.remove();
        label.hidden = false;
        if (keep && Number.isFinite(typed)) commit(snap(typed, fineUnit()), true);
        box.focus({preventScroll: true});
      };
      field.addEventListener('keydown', event => {
        event.stopPropagation();
        if (event.key === 'Enter') finish(true);
        if (event.key === 'Escape') finish(false);
      });
      field.addEventListener('blur', () => finish(true));
    }
    label.addEventListener('dblclick', event => { event.preventDefault(); editExact(); });
    box.addEventListener('dblclick', event => { event.preventDefault(); editExact(); });

    input._aislider = {paint, box, label, reset, defaultValue};
    paint();
    return input._aislider;
  }

  function enhanceWithin(root) {
    if (!root || !root.querySelectorAll) return;
    if (root.matches && root.matches('input[type=range]')) enhance(root);
    root.querySelectorAll('input[type=range]').forEach(enhance);
  }

  /** Every range input that appears under `host` becomes a node slider. */
  function watch(host) {
    if (!host) return;
    enhanceWithin(host);
    new MutationObserver(records => records.forEach(record =>
      record.addedNodes.forEach(node => { if (node.nodeType === 1) enhanceWithin(node); })))
      .observe(host, {childList: true, subtree: true});
  }

  global.AISlider = {enhance, enhanceWithin, watch};
})(window);

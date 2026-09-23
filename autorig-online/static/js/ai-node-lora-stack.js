/**
 * Several LoRAs on one Image / Video node, as a column of slots.
 *
 * Slot 1 is the node's original single-LoRA contract: the `lora` picker and
 * the `lora_strength` value, untouched, so a saved graph renders exactly as it
 * did. Slots 2..8 are written into the `loras` param as canonical
 * `<lora:NAME:WEIGHT>` tags, the format the backend already parses
 * (ai_lora_prompt.parse_stack; precedence prompt tag > stack > single lora).
 * A slot appears once the one above it holds a LoRA, like the numbered
 * reference sockets.
 *
 * Each slot: the LoRA picker (AIEntities.modelPicker — its family and
 * readiness rules, `ready_workers` vs the checkpoint's `runnable_workers`),
 * a compact weight, a remove icon, and its own readiness note. LoRAs of
 * another model family are not offered at all.
 *
 * The module only reads and writes the node's own controls; persistence and
 * the render signature stay with ai-nodes.js through the `[data-param]`
 * fields it already reads.
 */
(function (global) {
  'use strict';

  const MAX_SLOTS = 8;
  const MIN_WEIGHT = -2;
  const MAX_WEIGHT = 2;
  const DEFAULT_WEIGHT = 0.8;
  const TAG_RE = /<\s*(?:lora|lyco|locon)\s*:\s*([^<>:]+?)\s*(?::\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*)?>/gi;

  function stem(file) {
    const base = String(file || '').replace(/\\/g, '/').split('/').pop();
    return base.includes('.') ? base.slice(0, base.lastIndexOf('.')) : base;
  }

  function clampWeight(value, fallback) {
    const number = Number(value);
    if (!Number.isFinite(number)) return fallback;
    return Math.max(MIN_WEIGHT, Math.min(MAX_WEIGHT, Math.round(number * 100) / 100));
  }

  /**
   * Read a stack string into slots, or null when it holds anything this UI
   * cannot show faithfully (split text-encoder/model weights, named weights,
   * stray text) — then the raw field stays as it was.
   */
  function parseTags(text) {
    const value = String(text || '');
    const items = [];
    const leftover = value.replace(TAG_RE, (match, name, weight) => {
      items.push({name: name.trim(), weight: weight === undefined ? 1 : Number(weight)});
      return ' ';
    });
    if (leftover.replace(/[\s,]+/g, '')) return null;
    return items;
  }

  function formatWeight(weight) {
    return String(Math.round(Number(weight) * 100) / 100);
  }

  function toTags(items) {
    return items.filter(item => item && item.file)
      .map(item => '<lora:' + stem(item.file) + ':' + formatWeight(item.weight) + '>')
      .join(' ');
  }

  function norm(value) { return String(value || '').trim().replace(/\s+/g, ' ').toLowerCase(); }

  /** The catalogue file a tag name means (file, stem, title or alias). */
  function resolveFile(name, loras) {
    const wanted = norm(name);
    const exact = (loras || []).find(entry => norm(entry.file) === wanted);
    if (exact) return exact.file;
    const match = (loras || []).filter(entry =>
      [entry.file, stem(entry.file), entry.title].concat(entry.aliases || []).map(norm).includes(wanted));
    return match.length === 1 ? match[0].file : '';
  }

  /** Why this LoRA cannot render on this checkpoint yet ('' when it can). */
  function readiness(entry, checkpoint) {
    if (!entry) return '';
    if (entry.usable === false) return entry.unusable_reason || 'not on any render computer yet';
    const runners = checkpoint && Array.isArray(checkpoint.runnable_workers) ? checkpoint.runnable_workers : null;
    const ready = Array.isArray(entry.ready_workers) ? entry.ready_workers : null;
    if (runners && ready && !ready.some(box => runners.includes(box))) {
      return 'waiting for the render computers to download it';
    }
    return '';
  }

  function familiesMatch(left, right) {
    if (!left || !right || left === right) return true;
    const sdxl = ['pony', 'sdxl'];
    return sdxl.includes(left) && sdxl.includes(right);
  }

  /** The Civitai model a LoRA version belongs to (from its page URL). */
  function civitaiModel(entry) {
    const match = /\/models\/(\d+)/.exec(String((entry && entry.page) || ''));
    return match ? match[1] : '';
  }

  /** The checkpoint a node will actually run: its own choice, else the service default. */
  function effectiveCheckpoint(catalogue, service, file) {
    const list = (catalogue && catalogue.checkpoints_array) || [];
    if (file) return list.find(item => item.file === file || (item.legacy_files || []).includes(file)) || null;
    return list.find(item => (item.default_for_services || []).includes(service) || item.default === true) || null;
  }

  /** Why a LoRA cannot go with this checkpoint ('' when it can). */
  function familyMismatch(entry, checkpoint) {
    if (!entry || !checkpoint || familiesMatch(checkpoint.family, entry.family)) return '';
    return 'not for ' + (checkpoint.base || checkpoint.title || checkpoint.family);
  }

  /** Another version of the same Civitai model made for this checkpoint's family. */
  function familySwap(entry, checkpoint, loras, service) {
    const model = civitaiModel(entry);
    if (!model || !checkpoint) return null;
    return (loras || []).find(other => other.file !== entry.file && civitaiModel(other) === model &&
      familiesMatch(checkpoint.family, other.family) && other.family &&
      (!service || !other.services || other.services.includes(service)) && other.usable !== false) || null;
  }

  const catalogues = new Map();

  /**
   * Drop, from a request about to be sent, every LoRA that does not fit the
   * node's checkpoint, so the render runs with the ones that do instead of
   * failing with a 400. Returns the files it dropped. A stack string this
   * module cannot read is left for the server to judge.
   */
  function filterBody(service, body) {
    const catalogue = catalogues.get(service);
    if (!body || !catalogue) return [];
    const checkpoint = effectiveCheckpoint(catalogue, service, body.checkpoint);
    if (!checkpoint) return [];
    const list = catalogue.loras_array || [];
    const dropped = [];
    if (body.lora) {
      const entry = list.find(item => item.file === body.lora);
      if (familyMismatch(entry, checkpoint)) {
        dropped.push(body.lora);
        delete body.lora; delete body.lora_strength;
      }
    }
    if (typeof body.loras === 'string' && body.loras.trim()) {
      const items = parseTags(body.loras);
      if (items) {
        const kept = items.filter(item => {
          const file = resolveFile(item.name, list);
          const entry = list.find(value => value.file === file);
          if (familyMismatch(entry, checkpoint)) { dropped.push(file); return false; }
          return true;
        });
        if (kept.length !== items.length) {
          const text = kept.map(item => '<lora:' + item.name + ':' + formatWeight(item.weight) + '>').join(' ');
          if (text) body.loras = text; else delete body.loras;
        }
      }
    }
    return dropped;
  }

  /** Re-issue value writes made by code (applyParams, the graph agent) as a hook. */
  function watchValue(input, onSet) {
    const proto = Object.getPrototypeOf(input);
    const descriptor = Object.getOwnPropertyDescriptor(proto, 'value') ||
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
    if (!descriptor || input._lstackWatched) return;
    input._lstackWatched = true;
    Object.defineProperty(input, 'value', {
      configurable: true,
      get() { return descriptor.get.call(this); },
      set(next) { descriptor.set.call(this, next); onSet(next); }
    });
  }

  function install(options) {
    options = options || {};
    const canvas = options.canvas;
    const getMeta = options.getMeta || (() => null);
    const entities = () => global.AIEntities;
    if (!canvas) return null;

    function nodeId(element) { return String(element.id || '').replace(/^node-/, ''); }

    function enhance(element) {
      if (!element || element._lstack) return;
      const field = element.querySelector('input[data-param="loras"]');
      const firstSlot = element.querySelector('.mpick-slot[data-model-param="lora"]');
      const firstHidden = element.querySelector('input[data-param="lora"]');
      if (!field || !firstSlot || !firstHidden || !entities() || !entities().modelPicker) return;
      const service = (getMeta(nodeId(element)) || {}).service;
      if (!service) return;
      const row = field.closest('.nparam');
      const firstRow = firstSlot.closest('.nparam');
      const strength = element.querySelector('input[data-param="lora_strength"]');
      const strengthRow = strength && strength.closest('.nparam');
      const state = {
        element, service, field, row, firstSlot, firstHidden, strength,
        slots: [], catalogue: null, raw: false, writing: false
      };
      element._lstack = state;

      // Slot 1 lives in the original `lora` row; its weight is a proxy for
      // `lora_strength` (0 keeps meaning "the workflow's own strength").
      firstRow.classList.add('lstack-first');
      const firstBox = document.createElement('span');
      firstBox.className = 'lslot lslot-1';
      firstSlot.parentNode.insertBefore(firstBox, firstSlot);
      firstBox.appendChild(firstSlot);
      const firstWeight = weightInput(0, 2, strength ? strength.value : 0, 'Weight · 0 = workflow default');
      firstBox.appendChild(firstWeight);
      const firstRemove = removeButton();
      firstBox.appendChild(firstRemove);
      const firstNote = document.createElement('small');
      firstNote.className = 'lslot-note';
      firstRow.appendChild(firstNote);
      if (strengthRow) strengthRow.classList.add('lstack-hidden');
      firstWeight.addEventListener('change', () => {
        if (!strength) return;
        strength.value = String(clampWeight(firstWeight.value, 0) < 0 ? 0 : clampWeight(firstWeight.value, 0));
        firstWeight.value = strength.value;
        strength.dispatchEvent(new Event('input', {bubbles: true}));
        strength.dispatchEvent(new Event('change', {bubbles: true}));
      });
      if (strength) watchValue(strength, next => { if (document.activeElement !== firstWeight) firstWeight.value = next; });
      firstRemove.addEventListener('click', event => {
        event.preventDefault(); event.stopPropagation();
        const picker = firstSlot._picker;
        if (picker) picker.value = '';
        firstHidden.value = '';
        firstHidden.dispatchEvent(new Event('change', {bubbles: true}));
        // The stack moves up: slot 2 becomes the single LoRA, so removing the
        // first never silently drops the rest.
        const next = state.slots.find(slot => slot.file);
        if (next) {
          if (picker) picker.value = next.file;
          firstHidden.value = next.file;
          firstHidden.dispatchEvent(new Event('change', {bubbles: true}));
          if (strength) {
            strength.value = String(Math.max(0, clampWeight(next.weight, DEFAULT_WEIGHT)));
            strength.dispatchEvent(new Event('change', {bubbles: true}));
          }
          next.file = '';
          writeStack(state);
        }
        render(state);
      });
      watchValue(firstHidden, () => render(state));
      firstSlot.addEventListener('click', () => setTimeout(() => filterPanel(firstSlot), 0), true);

      // The stack row: the raw tag field and its "+" menu stay in the DOM
      // (they are what gets saved) but the slots replace them on screen.
      row.classList.add('lstack-row');
      const list = document.createElement('div');
      list.className = 'lstack-list';
      row.appendChild(list);
      state.list = list;
      watchValue(field, () => { if (!state.writing) { readField(state); render(state); } });

      entities().loadModels(service).then(data => {
        state.catalogue = data || {};
        catalogues.set(service, state.catalogue);
        readField(state);
        render(state);
      }).catch(() => { state.catalogue = {}; readField(state); render(state); });
    }

    function weightInput(min, max, value, title) {
      const input = document.createElement('input');
      input.type = 'number';
      input.className = 'lslot-weight';
      input.min = String(min); input.max = String(max); input.step = '0.05';
      input.value = String(value);
      input.title = title;
      input.setAttribute('aria-label', title);
      return input;
    }

    function removeButton() {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'lslot-remove';
      button.textContent = '×';
      button.title = 'Remove this LoRA';
      button.setAttribute('aria-label', 'Remove this LoRA');
      return button;
    }

    function swapButton() {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'lslot-swap';
      button.textContent = '⇄';
      button.hidden = true;
      return button;
    }

    /** Incompatible → ⛔ note (+ swap when another version fits); else readiness. */
    function markSlot(state, box, note, entry, checkpoint, onSwap) {
      let swap = box.querySelector('.lslot-swap');
      if (!swap) {
        swap = swapButton();
        box.insertBefore(swap, box.querySelector('.lslot-remove'));
        swap.addEventListener('click', event => {
          event.preventDefault(); event.stopPropagation();
          if (swap._target) swap._onSwap(swap._target);
        });
      }
      const mismatch = familyMismatch(entry, checkpoint);
      const alternative = mismatch ? familySwap(entry, checkpoint, loras(state), state.service) : null;
      swap._target = alternative ? alternative.file : '';
      swap._onSwap = onSwap;
      swap.hidden = !alternative;
      if (alternative) {
        swap.title = 'Use the ' + (alternative.base || alternative.family) + ' version: ' +
          (alternative.title || alternative.file) + (alternative.version ? ' · ' + alternative.version : '');
        swap.setAttribute('aria-label', swap.title);
      }
      box.classList.toggle('lslot-bad', !!mismatch);
      note.classList.toggle('lslot-note-bad', !!mismatch);
      if (mismatch) {
        note.textContent = '⛔ ' + mismatch;
        note.title = (entry.title || entry.file) + ' is ' + (entry.base || entry.family) +
          '; it is ' + mismatch + ' and is left out of the render.';
        note.hidden = false;
        return;
      }
      setNote(note, readiness(entry, checkpoint));
    }

    function loras(state) { return (state.catalogue && state.catalogue.loras_array) || []; }

    function checkpointEntry(state) {
      const value = (state.element.querySelector('[data-param="checkpoint"]') || {}).value || '';
      return effectiveCheckpoint(state.catalogue, state.service, value);
    }

    function readField(state) {
      const items = parseTags(state.field.value);
      state.raw = items === null;
      state.slots.forEach(slot => slot.box.remove());
      state.slots = [];
      if (state.raw) return;
      items.slice(0, MAX_SLOTS - 1).forEach(item => {
        state.slots.push(makeSlot(state, resolveFile(item.name, loras(state)) || item.name, item.weight));
      });
    }

    function writeStack(state) {
      state.writing = true;
      state.field.value = toTags(state.slots.map(slot => ({file: slot.file, weight: slot.weight})));
      state.writing = false;
      state.field.dispatchEvent(new Event('input', {bubbles: true}));
      state.field.dispatchEvent(new Event('change', {bubbles: true}));
    }

    function recommendedWeight(state, file) {
      const entry = loras(state).find(item => item.file === file);
      const value = entry && entry.recommended && Number(entry.recommended.strength);
      return Number.isFinite(value) && value !== 0 ? clampWeight(value, DEFAULT_WEIGHT) : DEFAULT_WEIGHT;
    }

    function makeSlot(state, file, weight) {
      const slot = {file: file || '', weight: clampWeight(weight, DEFAULT_WEIGHT)};
      const box = document.createElement('div');
      box.className = 'lslot';
      const host = document.createElement('span');
      host.className = 'lslot-pick';
      const weightBox = weightInput(MIN_WEIGHT, MAX_WEIGHT, slot.weight, 'Weight (−2…2)');
      const remove = removeButton();
      const note = document.createElement('small');
      note.className = 'lslot-note';
      box.append(host, weightBox, remove, note);
      slot.box = box; slot.note = note; slot.host = host; slot.weightBox = weightBox;
      slot.picker = entities().modelPicker(host, state.service, 'loras', {
        value: slot.file,
        onChange: value => {
          const fresh = !slot.file && value;
          slot.file = value || '';
          if (fresh) { slot.weight = recommendedWeight(state, value); weightBox.value = String(slot.weight); }
          if (!slot.file) { state.slots.splice(state.slots.indexOf(slot), 1); box.remove(); }
          writeStack(state);
          render(state);
        }
      });
      host.addEventListener('click', () => setTimeout(() => filterPanel(host), 0), true);
      weightBox.addEventListener('change', () => {
        slot.weight = clampWeight(weightBox.value, slot.weight);
        weightBox.value = String(slot.weight);
        if (slot.file) writeStack(state);
      });
      remove.addEventListener('click', event => {
        event.preventDefault(); event.stopPropagation();
        const index = state.slots.indexOf(slot);
        if (index >= 0) state.slots.splice(index, 1);
        box.remove();
        if (slot.file) writeStack(state);
        render(state);
      });
      return slot;
    }

    /** LoRAs of another family are not offered; ones already stacked neither. */
    function filterPanel(host) {
      const element = host.closest('.drawflow-node');
      const state = element && element._lstack;
      const panel = host.querySelector('.mpick-panel');
      if (!state || !panel || panel.hidden) return;
      const checkpoint = checkpointEntry(state);
      const family = checkpoint && checkpoint.family;
      const taken = new Set([state.firstHidden.value].concat(state.slots.map(slot => slot.file)).filter(Boolean));
      const own = host.querySelector('.mpick-item.chosen');
      panel.querySelectorAll('.mpick-item[data-model-file]').forEach(item => {
        const file = item.dataset.modelFile;
        if (!file) return;
        const entry = loras(state).find(value => value.file === file);
        const foreign = entry && !familiesMatch(family, entry.family);
        const duplicate = taken.has(file) && item !== own;
        item.hidden = !!(foreign || duplicate);
      });
    }

    function render(state) {
      if (!state || !state.list) return;
      const firstFile = state.firstHidden.value;
      const checkpoint = checkpointEntry(state);
      const firstEntry = loras(state).find(item => item.file === firstFile);
      const firstNote = state.element.querySelector('.lstack-first .lslot-note');
      const firstBox = state.element.querySelector('.lslot-1');
      if (firstNote && firstBox) markSlot(state, firstBox, firstNote, firstEntry, checkpoint, file => {
        const picker = state.firstSlot._picker;
        if (picker) picker.value = file;
        state.firstHidden.value = file;
        state.firstHidden.dispatchEvent(new Event('change', {bubbles: true}));
        render(state);
      });
      const firstRemove = state.element.querySelector('.lslot-1 .lslot-remove');
      if (firstRemove) firstRemove.hidden = !firstFile;
      const firstWeight = state.element.querySelector('.lslot-1 .lslot-weight');
      if (firstWeight) firstWeight.hidden = !firstFile;

      state.row.classList.toggle('lstack-raw', state.raw);
      if (state.raw) return;
      // Every filled slot, then one empty slot when the one above is filled.
      const filled = state.slots.filter(slot => slot.file);
      // Keep one empty slot alive rather than remounting a picker each time.
      let spare = state.slots.find(slot => !slot.file && slot.box.isConnected) || null;
      state.slots.filter(slot => !slot.file && slot !== spare).forEach(slot => slot.box.remove());
      const wantEmpty = !!firstFile && filled.length < MAX_SLOTS - 1;
      if (!wantEmpty && spare) { spare.box.remove(); spare = null; }
      if (wantEmpty && !spare) spare = makeSlot(state, '', DEFAULT_WEIGHT);
      state.slots = spare ? filled.concat([spare]) : filled;
      state.slots.forEach(slot => {
        state.list.appendChild(slot.box);
        const entry = loras(state).find(item => item.file === slot.file);
        if (slot.file) markSlot(state, slot.box, slot.note, entry, checkpoint, file => {
          slot.file = file;
          if (slot.picker) slot.picker.value = file;
          writeStack(state);
          render(state);
        });
        else { setNote(slot.note, ''); slot.box.classList.remove('lslot-bad'); slot.note.classList.remove('lslot-note-bad');
               const swap = slot.box.querySelector('.lslot-swap'); if (swap) swap.hidden = true; }
        slot.box.querySelector('.lslot-remove').hidden = !slot.file;
        slot.weightBox.hidden = !slot.file;
      });
      state.row.classList.toggle('lstack-empty', !state.slots.length);
    }

    function setNote(note, text) {
      note.textContent = text ? '⏳ ' + text : '';
      note.title = text || '';
      note.hidden = !text;
    }

    function enhanceAll() {
      canvas.querySelectorAll('.drawflow-node[id^="node-"]').forEach(enhance);
    }

    let queued = false;
    function schedule() {
      if (queued) return;
      queued = true;
      setTimeout(() => { queued = false; enhanceAll(); }, 30);
    }
    new MutationObserver(records => {
      for (const record of records) {
        for (const added of record.addedNodes) {
          if (added.nodeType === 1 && (added.matches('.drawflow-node') || added.querySelector && added.querySelector('.drawflow-node'))) {
            schedule(); return;
          }
        }
      }
    }).observe(canvas, {childList: true, subtree: true});
    // A checkpoint change changes which LoRAs fit and where they are ready.
    canvas.addEventListener('change', event => {
      const control = event.target;
      if (!control || !control.dataset || control.dataset.param !== 'checkpoint') return;
      const element = control.closest('.drawflow-node');
      if (element && element._lstack) render(element._lstack);
    });
    schedule();
    if (entities() && entities().loadModels) {
      ['image', 'video'].forEach(service => entities().loadModels(service)
        .then(data => { if (data && !catalogues.has(service)) catalogues.set(service, data); })
        .catch(() => {}));
    }

    return {refresh: enhanceAll};
  }

  global.AINodeLoraStack = Object.freeze({
    install, parseTags, toTags, resolveFile, readiness, familiesMatch, stem, clampWeight,
    filterBody, familyMismatch, familySwap, civitaiModel, effectiveCheckpoint,
    _setCatalogue: (service, data) => catalogues.set(service, data),
    MAX_SLOTS, MIN_WEIGHT, MAX_WEIGHT, DEFAULT_WEIGHT
  });
})(typeof window !== 'undefined' ? window : globalThis);

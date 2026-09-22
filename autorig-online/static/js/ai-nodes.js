/**
 * The node editor behind /nodes.
 *
 * Everything the editor knows about a service comes from `/api/ai/services`:
 * its inputs, its outputs and the parameters it accepts. Nothing about vision
 * or video is written down here, so a service added to the catalogue appears
 * in the palette with working controls without this file changing.
 *
 * Running a composition is done here in the browser, by calling the same
 * public endpoints a person would. That keeps the graph honest — it can never
 * ask for more than the services already offer — and it means a long render is
 * watched the same way the single-service pages watch one.
 */
(function () {
  'use strict';

  const KIND_INPUT = 'input';
  const KIND_SERVICE = 'service';
  const ROW_HEIGHT = 26;      // one port row; the port dots are spaced to match
  const HEADER_HEIGHT = 46;

  let editor = null;
  let nodeDisplay = null;
  let nodeGroups = null;
  let graphAgent = null;
  let graphBridge = null;
  let graphInstanceId = '';
  let comparisonAnchorId = '';
  let nodeCompare = null;
  let catalogue = null;
  let models = [];
  // Drawflow addresses ports by index, the catalogue by field name. This holds
  // the translation for every node on the canvas.
  const nodeMeta = new Map();

  /** Drawflow reports node ids as numbers in some callbacks and strings in
   *  others, so every read and write of the map goes through these two. */
  function meta(id) { return nodeMeta.get(String(id)); }
  function setMeta(id, value) { nodeMeta.set(String(id), value); }

  // What this run has produced so far, keyed by node id. Kept on the server
  // alongside the graph so a deep link shows the run and not only the wiring:
  // somebody handed the link while a clip renders should see it arrive.
  const runState = new Map();
  // Incremental runner state. A task is identified by the node, the exact
  // resolved request and the canvas incarnation that launched it. This lets a
  // second Render click append changed/new work without duplicating anything
  // already active.
  const activeExecutions = new Map();
  const completedExecutions = new Map();
  const continuableResults = new Map();
  const restoredExecutions = new Map();
  const desiredSignatures = new Map();
  const nodeRunVersions = new Map();
  const latestPlannedRequests = new Map();
  const runRequests = new Set();
  let nextRunRequestId = 0;
  let canvasEpoch = 1;
  const pendingImageUploads = new Map();
  const pendingModelSelections = new Map();
  let graphId = null;
  let resultsTimer = null;
  // Set by Cancel. It only stops work that has not been handed to the farm
  // yet: a job already on a card is left to finish, because killing it wastes
  // the GPU minutes it has already spent and the result is wanted anyway.

  function stableJson(value) {
    if (Array.isArray(value)) return '[' + value.map(stableJson).join(',') + ']';
    if (value && typeof value === 'object') {
      return '{' + Object.keys(value).sort().map(key =>
        JSON.stringify(key) + ':' + stableJson(value[key])).join(',') + '}';
    }
    return JSON.stringify(value);
  }

  function executionKey(epoch, id, signature) {
    return epoch + ':' + String(id) + ':' + signature;
  }

  function reusableCompleted(service, body) {
    if (['vision', 'text', 'video_frame', 'video_storyboard'].includes(service) ||
        service.startsWith('control_')) return true;
    if (!/image|video|animation|render/.test(service)) return true;
    return ![undefined, null, '', 0, '0'].includes(body.seed);
  }

  function executionIsCurrent(execution) {
    return execution.epoch === canvasEpoch &&
      meta(execution.id) && nodeElement(execution.id) &&
      desiredSignatures.get(String(execution.id)) === execution.signature &&
      nodeRunVersions.get(String(execution.id)) === execution.version;
  }

  function finishTaskTracker(task, ok, execution, progress) {
    if (!task) return;
    const current = executionIsCurrent(execution);
    task.finish(ok);
    if (current || !progress) return;
    progress.classList.remove('done', 'failed', 'queued', 'running');
    const record = runState.get(String(execution.id));
    if (record?.status === 'done') progress.classList.add('done');
    else if (record?.status === 'failed') progress.classList.add('failed');
    else {
      const wanted = desiredSignatures.get(String(execution.id));
      if (wanted && activeExecutions.has(executionKey(canvasEpoch, execution.id, wanted))) {
        progress.classList.add('running');
      }
    }
  }

  function forgetNodes(ids) {
    let anchorCleared = false;
    (ids || []).map(String).forEach(id => {
      activeExecutions.forEach((execution, key) => {
        if (String(execution.id) === id) activeExecutions.delete(key);
      });
      nodeMeta.delete(id);
      if (comparisonAnchorId === id) { comparisonAnchorId = ''; anchorCleared = true; }
      runState.delete(id);
      continuableResults.delete(id);
      const restored = restoredExecutions.get(id);
      if (restored) restored.invalidated = true;
      restoredExecutions.delete(id);
      pendingImageUploads.delete(id);
      desiredSignatures.delete(id);
      completedExecutions.delete(id);
      latestPlannedRequests.delete(id);
      nodeRunVersions.set(id, (nodeRunVersions.get(id) || 0) + 1);
    });
    if (anchorCleared && nodeCompare) requestAnimationFrame(() => nodeCompare.refresh());
    pushResults();
    refreshRunningControls();
  }

  function invalidateNodeAndDownstream(startId) {
    if (!editor) return;
    const graph = graphFromCanvas();
    const affected = new Set([String(startId)]);
    let changed = true;
    while (changed) {
      changed = false;
      graph.links.forEach(link => {
        if (affected.has(String(link.from)) && !affected.has(String(link.to))) {
          affected.add(String(link.to));
          changed = true;
        }
      });
    }
    affected.forEach(id => {
      desiredSignatures.delete(id);
      latestPlannedRequests.delete(id);
      nodeRunVersions.set(id, (nodeRunVersions.get(id) || 0) + 1);
      const prior = runState.get(id);
      if (prior && ['done', 'stale'].includes(prior.status) && prior.value) {
        runState.set(id, {...prior, status:'stale'});
      } else if (prior?.history?.length) {
        const last = prior.history[0];
        runState.set(id, {...last, status:'stale', task_id:'', history:prior.history.slice(1)});
      } else runState.delete(id);
      continuableResults.delete(id);
      const restored = restoredExecutions.get(id);
      if (restored) restored.invalidated = true;
      restoredExecutions.delete(id);
      const element = nodeElement(id);
      if (element && meta(id)?.kind === KIND_SERVICE) {
        const state = element.querySelector('.nstate');
        if (state) {
          state.textContent = 'changed — render to update';
          state.className = 'nstate';
        }
      }
    });
    pushResults();
  }

  function resetCanvasExecutionState() {
    canvasEpoch += 1;
    restoredExecutions.forEach(execution => { execution.invalidated = true; });
    restoredExecutions.clear();
    continuableResults.clear();
    desiredSignatures.clear();
    completedExecutions.clear();
    nodeRunVersions.clear();
    latestPlannedRequests.clear();
  }

  function recordResult(id, record) {
    if (nodeCompare) record = nodeCompare.enhanceRecord(record, runState.get(String(id)));
    runState.set(String(id), record);
    if (nodeCompare) requestAnimationFrame(() => nodeCompare.refresh(String(id)));
    if (record.status === 'done' && record.value) {
      continuableResults.set(String(id), {type:record.type, value:record.value,
        task_id:record.task_id || ''});
    } else if (record.status === 'failed') {
      continuableResults.delete(String(id));
    }
    pushResults();
  }

  /** Written a beat after the change so a burst of finishes is one request. */
  function pushResults() {
    if (!graphId) return;
    clearTimeout(resultsTimer);
    resultsTimer = setTimeout(() => {
      const body = {};
      runState.forEach((value, key) => { body[key] = value; });
      fetch('/api/ai/graphs/' + encodeURIComponent(graphId) + '/results', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      }).catch(() => {});
    }, 800);
  }

  /* ------------------------------------------------------------- catalogue */

  async function loadCatalogue() {
    if (catalogue) return catalogue;
    const [services, modelList] = await Promise.all([
      fetch('/api/ai/services').then(r => r.json()),
      fetch('/api/ai/models').then(r => r.json()).catch(() => ({}))
    ]);
    catalogue = services;
    models = modelList.models_array || modelList.models || [];
    return catalogue;
  }

  function serviceById(id) {
    return (catalogue.services_array || []).find(s => s.id === id);
  }

  function typeIcon(type) {
    const entry = (catalogue.entity_types_array || []).find(t => t.id === type);
    return entry ? entry.icon : '•';
  }

  /* ------------------------------------------------------------- node HTML */

  function paramControl(param) {
    const name = escapeAttr(param.name);
    const help = param.help ? ` title="${escapeAttr(param.help)}"` : '';
    let control;
    if (param.type === 'select') {
      const options = param.source === 'ai_models'
        ? models.map(m => ({ value: m.id, title: m.title + (m.uncensored ? ' · uncensored' : '') }))
        : (param.options || []);
      control = `<select data-param="${name}"${help}>` + options.map(option =>
        `<option value="${escapeAttr(option.value)}"${String(option.value) === String(param.default) ? ' selected' : ''}>${escapeHtml(option.title)}</option>`
      ).join('') + '</select>';
    } else if (param.type === 'range') {
      // Zero on these sliders means "leave the workflow's own value", so it is
      // shown as `auto` rather than as a number that looks like a setting.
      control = `<input type="range" data-param="${name}" min="${param.min}" max="${param.max}" `
              + `step="${param.step}" value="${param.default}"${help}>`
              + `<output data-for="${name}">${rangeLabel(param.default)}</output>`;
    } else if (param.type === 'number') {
      control = `<input type="number" data-param="${name}" min="${param.min}" max="${param.max}" `
              + `step="${param.step || 1}" value="${param.default}"${help}>`;
    } else if (param.type === 'model') {
      // Filled in after the node exists: the picker needs a live element to
      // mount into, which the HTML string cannot give it.
      control = `<span class="mpick-slot" data-model-param="${name}" `
              + `data-model-source="${escapeAttr(param.source || 'loras')}"></span>`
              + `<input type="hidden" data-param="${name}" value="">`;
    } else if (param.type === 'textarea') {
      control = `<textarea data-param="${name}" rows="3"${help}>${escapeHtml(param.default || '')}</textarea>`;
    } else {
      control = `<input type="text" data-param="${name}" value="${escapeAttr(param.default || '')}"${help}>`;
    }
    const wide = param.type === 'textarea' ? ' nparam-wide' : '';
    return `<label class="nparam${wide}"><span>${escapeHtml(param.title)}</span>${control}</label>`;
  }

  function rangeLabel(value) {
    return Number(value) === 0 ? 'auto' : String(value);
  }

  function serviceNodeHtml(entry) {
    const inputs = entry.inputs || [];
    const outputs = entry.outputs || [];
    const params = entry.params_array || [];
    const slow = entry.slow ? '<em class="nslow">minutes</em>' : '';
    return `
      <div class="nhead"><b>${escapeHtml(entry.title)}</b>${slow}</div>
      <div class="nports">
        ${inputs.map(item => `<div class="prow pin">${typeIcon(item.type)} ${escapeHtml(item.title || item.field)}${item.required ? '' : ' <i>optional</i>'}</div>`).join('')}
        ${outputs.map(item => `<div class="prow pout">${escapeHtml(item.title || item.field)} ${typeIcon(item.type)}</div>`).join('')}
      </div>
      ${params.length ? `<details class="nparams"><summary>Settings</summary>${params.map(paramControl).join('')}</details>` : ''}
      <div class="nrec"></div>
      <div class="nstate"></div>
      <div class="nprog task-prog"></div>
      <div class="nout"></div>`;
  }

  function inputNodeHtml(entityType) {
    if (entityType === 'avatar') return '<div class="nhead"><b>Avatar</b></div>'
      + '<div class="nports"><div class="prow pout">character 👤</div></div>'
      + '<div class="ninput"><select data-value aria-label="Saved Avatar"><option value="">Choose an Avatar…</option></select>'
      + '<a href="/avatars" target="_blank" rel="noopener">Manage Avatars</a><img data-preview hidden alt="Avatar reference"></div>'
      + '<div class="nstate"></div>';
    const isText = entityType === 'text';
    const isVideo = entityType === 'video';
    const field = isText
      ? '<textarea data-value rows="3" placeholder="Type the text…"></textarea>'
      : '<input type="text" data-value placeholder="https://… or drop a file">'
        + `<input type="file" data-file accept="${isVideo ? 'video/mp4,video/webm,video/quicktime' : 'image/*'}" hidden>`
        + '<button type="button" data-pick class="npick">Choose a file</button>'
        + (isVideo
          ? '<video data-preview muted autoplay loop playsinline hidden></video>'
          : '<img data-preview alt="" hidden>');
    const title = isText ? 'Text in' : isVideo ? 'Video in' : 'Image in';
    const output = isText ? 'text' : isVideo ? 'video' : 'image';
    return `
      <div class="nhead"><b>${title}</b></div>
      <div class="nports"><div class="prow pout">${output} ${typeIcon(entityType)}</div></div>
      <div class="ninput">${field}</div>
      <div class="nstate"></div>`;
  }

  /* ------------------------------------------------------------ node adding */

  function addServiceNode(serviceId, x, y, params) {
    const entry = serviceById(serviceId);
    if (!entry) return null;
    const inputs = entry.inputs || [];
    const outputs = entry.outputs || [];
    const id = editor.addNode(
      serviceId, inputs.length, outputs.length, x, y,
      'ainode svc-' + serviceId, { service: serviceId }, serviceNodeHtml(Object.assign({}, entry, {title: (params && params._label) || entry.title}))
    );
    setMeta(id, {
      kind: KIND_SERVICE,
      service: serviceId,
      displayMode: params && params._display_mode,
      label: (params && params._label) || '',
      inFields: inputs.map(i => i.field),
      outFields: outputs.map(o => o.field)
    });
    alignPorts(id, inputs.length, outputs.length);
    mountModelPickers(id, serviceId);
    if (params) applyParams(id, params);
    materializeDefaultModel(id);
    return id;
  }

  function addInputNode(entityType, x, y, value, params) {
    if (!(catalogue.entity_types_array || []).some(item => item.id === entityType)) return null;
    const id = editor.addNode(
      'input-' + entityType, 0, 1, x, y,
      'ainode input-node', { entity_type: entityType }, inputNodeHtml(entityType)
    );
    setMeta(id, { kind: KIND_INPUT, entityType: entityType, displayMode: params && params._display_mode, inFields: [], outFields: ['value'] });
    alignPorts(id, 0, 1);
    if (value) {
      const field = nodeElement(id).querySelector('[data-value]');
      if (field) {
        if (entityType === 'avatar') field.add(new Option(value, value));
        field.value = value;
      }
    }
    wireInputNode(id, entityType);
    return id;
  }

  /** Turn every `model` parameter on a node into a picture dropdown. */
  function mountModelPickers(id, serviceId) {
    const element = nodeElement(id);
    if (!element || !window.AIEntities || !window.AIEntities.modelPicker) return;
    element.querySelectorAll('.mpick-slot').forEach(slot => {
      const name = slot.dataset.modelParam;
      const hidden = element.querySelector('input[data-param="' + CSS.escape(name) + '"]');
      const picker = window.AIEntities.modelPicker(slot, serviceId, slot.dataset.modelSource, {
        value: hidden ? hidden.value : '',
        onChange: (value, entry, reason) => {
          if (name === 'checkpoint' && value && !modelAcceptsConnectedControls(id, entry)) {
            picker.value = hidden ? hidden.value : '';
            if (!reason?.materialized) toast('This model has not been validated with the connected ControlNet channel. Use a compatible model or disconnect the control.');
            return;
          }
          if (hidden) hidden.value = value;
          if (reason && reason.materialized) return;
          if (name === 'checkpoint') {
            const loraField = element.querySelector('[data-param="lora"]');
            const loraPicker = element.querySelector('[data-model-param="lora"]')?._picker;
            const left = entry?.family, right = loraPicker?.entry?.family;
            if (loraField?.value && left && right && left !== right &&
                !(['pony','sdxl'].includes(left) && ['pony','sdxl'].includes(right))) {
              loraField.value = ''; loraPicker.value = '';
              toast('The previous LoRA belongs to another model family and was cleared.');
            }
          }
          const pending = applyRecommended(id, entry);
          pendingModelSelections.set(String(id), pending);
          pending.finally(() => { if(pendingModelSelections.get(String(id))===pending) pendingModelSelections.delete(String(id)); });
        }
      });
      slot._picker = picker;
    });
  }

  /**
   * Put the author's own settings on the node when their model is chosen.
   *
   * These come off the model's Civitai page — mostly from the metadata of the
   * example images, which is what the author actually ran. Only values this
   * service has a control for are applied; the rest (sampler, CFG) are shown
   * in the picker but there is nowhere here to put them.
   *
   * A value the person has already changed by hand is left alone. Choosing a
   * model should not quietly undo a decision they made.
   */
  const RECOMMENDED_TO_PARAM = {
    steps: 'steps', strength: 'lora_strength'
  };

  async function applyRecommended(id, entry) {
    const element = nodeElement(id);
    if (!element) return;
    const generation = (element._settingsGeneration || 0) + 1;
    element._settingsGeneration = generation;
    const serviceId = meta(id).service;
    const selection = new URLSearchParams({ service: serviceId });
    ['checkpoint', 'lora'].forEach(name => {
      const control = element.querySelector('[data-param="' + name + '"]');
      if (control && control.value) selection.set(name, control.value);
    });
    addWorkflowSelectionContext(id, selection);
    try {
      const response = await fetch('/api/ai/model-settings?' + selection);
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail?.message_string || 'Settings unavailable');
      if (element._settingsGeneration !== generation) return;
      const checkpointControl = element.querySelector('[data-param="checkpoint"]');
      if (checkpointControl && !checkpointControl.value && data.checkpoint_string) {
        checkpointControl.value = data.checkpoint_string;
        const picker = element.querySelector('[data-model-param="checkpoint"]')?._picker;
        if (picker) picker.value = data.checkpoint_string;
      }
      const effective = data.effective_params_object || {};
      entry = { recommended: effective };
    } catch (error) {
      const note = element.querySelector('.nrec');
      if (note) note.textContent = error.message;
      return;
    }
    const applied = [];
    Object.keys(entry.recommended).forEach(key => {
      const name = RECOMMENDED_TO_PARAM[key] || ({cfg:'cfg', sampler:'sampler', scheduler:'scheduler', lora_strength:'lora_strength'})[key];
      if (!name) return;
      const control = element.querySelector('[data-param="' + CSS.escape(name) + '"]');
      if (!control || control.dataset.touched === 'yes') return;
      const value = entry.recommended[key];
      // A select only takes a value it actually offers.
      if (control.tagName === 'SELECT' &&
          ![...control.options].some(o => String(o.value) === String(value))) return;
      control.value = value;
      const readout = element.querySelector('[data-for="' + CSS.escape(name) + '"]');
      if (readout) readout.textContent = rangeLabel(value);
      applied.push(name + ' ' + value);
    });
    const note = element.querySelector('.nrec');
    if (note) {
      note.textContent = applied.length
        ? 'Applied settings: ' + applied.join(', ')
        : (Object.keys(entry.recommended).length
            ? 'the model page suggests ' +
              Object.keys(entry.recommended).sort()
                .map(k => k + ' ' + entry.recommended[k]).join(', ')
            : 'this model publishes no recommended settings');
    }
  }

  function nodeElement(id) {
    return document.getElementById('node-' + id);
  }

  /**
   * Drawflow stacks the port dots in one column centred on the node; the
   * labels live in the body. Spacing both on the same pitch is what makes a
   * dot sit beside the name of the thing it carries.
   */
  function alignPorts(id, inCount, outCount) {
    const element = nodeElement(id);
    if (!element) return;
    const inputs = element.querySelector('.inputs');
    const outputs = element.querySelector('.outputs');
    if (inputs) inputs.style.marginTop = (HEADER_HEIGHT + 6) + 'px';
    if (outputs) outputs.style.marginTop = (HEADER_HEIGHT + 6 + inCount * ROW_HEIGHT) + 'px';
  }

  function wireInputNode(id, entityType) {
    const element = nodeElement(id);
    if (element && entityType === 'avatar') {
      const select = element.querySelector('[data-value]');
      const state = element.querySelector('.nstate');
      const preview = element.querySelector('[data-preview]');
      const update = async () => {
        if (!select.value) { preview.hidden = true; return; }
        const [avatarId, version] = select.value.split('@');
        try {
          const response = await fetch('/api/ai/avatars/' + encodeURIComponent(avatarId) + (version ? '?version=' + encodeURIComponent(version) : ''));
          const data = await response.json();
          if (!response.ok || !data.avatar_object) throw new Error('Avatar is unavailable in this account');
          const avatar = data.avatar_object;
          const pinned = avatar.avatar_id + '@' + avatar.version;
          if (!Array.from(select.options).some(item => item.value === pinned)) select.add(new Option(avatar.display_name + ' · v' + avatar.version, pinned));
          select.value = pinned;
          const ref = avatar.references.find(item => item.media_type === 'image' && item.role === 'face') || avatar.references.find(item => item.media_type === 'image');
          preview.hidden = !ref;
          if (ref) preview.src = ref.canonical_url;
          state.textContent = 'Saved character · version ' + avatar.version;
          state.className = 'nstate done';
        } catch (error) { state.textContent = error.message; state.className = 'nstate failed'; }
      };
      select.addEventListener('change', update);
      preview.addEventListener('click', () => { if (!preview.hidden) openPreview('image', preview.src); });
      fetch('/api/ai/avatars').then(response => response.json()).then(data => {
        for (const item of data.avatars_array || []) {
          const key = item.avatar_id + '@' + item.current_version;
          if (!Array.from(select.options).some(option => option.value === key)) select.add(new Option(item.display_name + ' · v' + item.current_version, key));
        }
        update();
      }).catch(() => { state.textContent = 'Could not load Avatars'; });
      return;
    }
    if (!element || !['image', 'video'].includes(entityType)) return;
    const isVideo = entityType === 'video';
    const file = element.querySelector('[data-file]');
    const pick = element.querySelector('[data-pick]');
    const preview = element.querySelector('[data-preview]');
    const text = element.querySelector('[data-value]');
    preview.classList.add('preview-expandable');
    preview.title = 'Click to enlarge';
    preview.addEventListener('click', event => {
      event.stopPropagation();
      if (preview.src) openPreview(isVideo ? 'video' : 'image', preview.src);
    });
    if (/^(https?:\/\/|data:image\/)/.test(text.value.trim())) {
      preview.src = text.value.trim();
      preview.hidden = false;
      if (isVideo) preview.play().catch(() => {});
    }
    pick.addEventListener('click', () => file.click());
    async function acceptMedia(chosen) {
      const expected = isVideo ? 'video/' : 'image/';
      if (!chosen || !chosen.type.startsWith(expected)) return;
      const limit = (isVideo ? 100 : 12) * 1024 * 1024;
      if (chosen.size > limit) {
        toast(`${isVideo ? 'Videos' : 'Images'} must be at most ${isVideo ? 100 : 12} MB.`);
        return;
      }
      const generation = (element._uploadGeneration || 0) + 1;
      element._uploadGeneration = generation;
      const status = element.querySelector('.nstate');
      status.textContent = `uploading ${isVideo ? 'video' : 'image'}...`;
      status.className = 'nstate running';
      text.value = '';
      if (isVideo) {
        if (element._previewObjectUrl) URL.revokeObjectURL(element._previewObjectUrl);
        element._previewObjectUrl = URL.createObjectURL(chosen);
        preview.src = element._previewObjectUrl;
        preview.hidden = false;
        preview.play().catch(() => {});
      } else {
        const reader = new FileReader();
        reader.onload = () => {
          if (element._uploadGeneration !== generation) return;
          preview.src = reader.result;
          preview.hidden = false;
        };
        reader.readAsDataURL(chosen);
      }
      const form = new FormData();
      form.append('file', chosen, chosen.name || (isVideo ? 'input.mp4' : 'clipboard.png'));
      const upload = fetch('/dev/api/scratch', {method: 'POST', body: form})
        .then(async response => {
          const data = await response.json();
          if (!response.ok || !data.url) throw new Error(`The ${isVideo ? 'video' : 'image'} upload failed`);
          if (element._uploadGeneration !== generation) return;
          text.value = data.url;
          preview.src = data.url;
          status.textContent = `${isVideo ? 'video' : 'image'} ready`;
          status.className = 'nstate done';
        }).catch(error => {
          if (element._uploadGeneration !== generation) return;
          status.textContent = error.message;
          status.className = 'nstate failed';
        }).finally(() => {
          if (pendingImageUploads.get(String(id)) === upload) pendingImageUploads.delete(String(id));
        });
      pendingImageUploads.set(String(id), upload);
      await upload;
    }
    if (isVideo) element._acceptVideo = acceptMedia;
    else element._acceptImage = acceptMedia;
    element.tabIndex = 0;
    element.querySelector('.npick').textContent = isVideo ? 'Choose a video file' : 'Choose a file or Ctrl+V';
    file.addEventListener('change', event => acceptMedia(event.target.files[0]));
    element.addEventListener('dragover', event => event.preventDefault());
    element.addEventListener('drop', event => {
      event.preventDefault();
      acceptMedia([...event.dataTransfer.files].find(item => item.type.startsWith(isVideo ? 'video/' : 'image/')));
    });
    text.addEventListener('change', () => {
      if (/^https?:\/\//.test(text.value.trim())) {
        preview.src = text.value.trim();
        preview.hidden = false;
        if (isVideo) preview.play().catch(() => {});
      }
    });
  }

  function applyParams(id, params) {
    const element = nodeElement(id);
    if (!element) return;
    Object.keys(params || {}).forEach(name => {
      const control = element.querySelector('[data-param="' + CSS.escape(name) + '"]');
      if (!control) return;
      if (['width','height'].includes(name) && control.tagName === 'SELECT' &&
          ![...control.options].some(option => option.value === String(params[name]))) {
        control.add(new Option(String(params[name]), String(params[name])));
      }
      control.value = params[name];
      const slot = element.querySelector('.mpick-slot[data-model-param="' + CSS.escape(name) + '"]');
      if (slot && slot._picker) slot._picker.value = params[name];
      const readout = element.querySelector('[data-for="' + CSS.escape(name) + '"]');
      if (readout) readout.textContent = rangeLabel(params[name]);
    });
  }

  function readParams(id) {
    const element = nodeElement(id);
    const values = {};
    if (meta(id)?.label) values._label = meta(id).label;
    if (meta(id)?.displayMode) values._display_mode = meta(id).displayMode;
    if (!element) return values;
    element.querySelectorAll('[data-param]').forEach(control => {
      const raw = control.value;
      values[control.dataset.param] =
        (control.type === 'range' || control.type === 'number') ? Number(raw) : raw;
    });
    return values;
  }

  /* -------------------------------------------------------- type-safe links */

  function portIndex(className) {
    return parseInt(String(className).split('_')[1], 10) - 1;
  }

  function linkTypes(connection) {
    const from = meta(connection.output_id);
    const to = meta(connection.input_id);
    if (!from || !to) return null;
    const outField = from.outFields[portIndex(connection.output_class)];
    const inField = to.inFields[portIndex(connection.input_class)];
    const produced = from.kind === KIND_INPUT
      ? from.entityType
      : (serviceById(from.service).outputs.find(o => o.field === outField) || {}).type;
    const accepted = (serviceById(to.service).inputs.find(i => i.field === inField) || {}).type;
    return { outField, inField, produced, accepted };
  }

  function onConnectionCreated(connection) {
    const info = linkTypes(connection);
    if (info && info.produced === info.accepted) {
      if (info.produced.startsWith('control_')) {
        const element = nodeElement(connection.input_id);
        const slot = element && element.querySelector('[data-model-param="checkpoint"]');
        const entry = slot && slot._picker && slot._picker.entry;
        const channels = (entry && entry.control_channels) || [];
        const node = editor.getNodeFromId(connection.input_id);
        const targetMeta = meta(connection.input_id);
        const controlLinks = targetMeta.inFields.reduce((count, field, index) =>
          count + (field.startsWith('control_') ? ((node.inputs['input_' + (index + 1)] || {}).connections || []).length : 0), 0);
        if ((entry && !channels.includes(info.produced.slice(8))) || controlLinks > 1) {
          editor.removeSingleConnection(connection.output_id, connection.input_id, connection.output_class, connection.input_class);
          toast(controlLinks > 1 ? 'Use a separate Image node for each ControlNet channel.' : 'Choose a model validated for this ControlNet channel first.');
          return;
        }
      }
      replaceOlderInput(connection);
      invalidateNodeAndDownstream(connection.input_id);
      return;
    }
    // A wrong wire is removed rather than left to fail at render time, when
    // the person has already waited for everything upstream of it.
    editor.removeSingleConnection(connection.output_id, connection.input_id,
                                  connection.output_class, connection.input_class);
    toast(info
      ? `That socket carries ${info.produced || 'nothing'}, and this one takes ${info.accepted || 'nothing'}.`
      : 'Those two cannot be connected.');
  }

  /**
   * One wire per socket: a new one replaces what was there.
   *
   * Every input here stands for a single field in a request — one picture to
   * look at, one prompt to draw — so two wires into it is not a richer input,
   * it is an ambiguity the runner would have to break arbitrarily. Drawflow
   * allows the second wire happily, which left sockets quietly holding two
   * and the graph doing whichever the resolver happened to read last.
   *
   * Dropping the older one is what dragging a new wire onto an occupied
   * socket is understood to mean everywhere else.
   */
  function replaceOlderInput(connection) {
    const target = editor.getNodeFromId(connection.input_id);
    if (!target) return;
    const port = (target.inputs || {})[connection.input_class];
    if (!port || !Array.isArray(port.connections) || port.connections.length < 2) return;
    let replaced = 0;
    port.connections.slice().forEach(existing => {
      const sameWire = String(existing.node) === String(connection.output_id) &&
                       String(existing.input) === String(connection.output_class);
      if (sameWire) return;
      editor.removeSingleConnection(existing.node, connection.input_id,
                                    existing.input, connection.input_class);
      replaced += 1;
    });
    if (replaced) toast('Replaced what was wired into that socket.');
  }

  /* ------------------------------------------------------------ the runner */

  /**
   * How each service is called and how its result is recognised as finished.
   *
   * They differ genuinely: the language models can hold the connection open,
   * the render farm publishes a URL that fills in later, and Hunyuan has a
   * status endpoint of its own. Rather than pretend to one shape, each says
   * what it does.
   */
  // None of these hold the connection open. A graph run wants the task id back
  // at once so the node can say it is running and the deep link can carry the
  // id; waiting on the submit would leave both blank for minutes.
  const RUNNERS = {
    avatar_image: { api: '/api/ai/avatar-image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video_frame: { api: '/api/ai/video-reference', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video_storyboard: { api: '/api/ai/video-reference', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video_control: { api: '/api/ai/video', finish: pollForFile, field: 'video_url_string', type: 'video' },
    vision: { api: '/api/vision', finish: pollAiStatus, field: 'answer_string', type: 'text' },
    text: { api: '/api/text2text', finish: pollAiStatus, field: 'answer_string', type: 'text' },
    image: { api: '/api/image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video: { api: '/api/video', finish: pollForFile, field: 'video_url_string', type: 'video' },
    '3dmodel': { api: '/api/3dmodel', finish: poll3dStatus, field: 'model_url_string', type: 'model3d' }
  };
  ['pose', 'depth', 'canny'].forEach(channel => {
    RUNNERS['control_' + channel] = { api: '/api/controlnet', finish: pollForFile,
      field: 'image_url_string', type: 'control_' + channel };
  });

  /**
   * Turn a rejection into something a person can act on.
   *
   * FastAPI reports a validation failure as a *list* under `detail`, so
   * reading `detail.message_string` found nothing and the node showed a bare
   * "HTTP 422" — which says a field was wrong but not which one.
   */
  function describeError(body, status) {
    const detail = (body || {}).detail;
    if (Array.isArray(detail)) {
      const parts = detail.map(item => {
        const where = (item.loc || []).filter(x => x !== 'body').join('.');
        return (where ? where + ': ' : '') + (item.msg || 'invalid');
      });
      return parts.join('; ') || ('HTTP ' + status);
    }
    if (detail && typeof detail === 'object') {
      return detail.message_string || detail.error_string || ('HTTP ' + status);
    }
    if (typeof detail === 'string' && detail) return detail;
    return 'HTTP ' + status;
  }

  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

  // Submissions are cheap HTTP calls but a large graph can make dozens at the
  // same instant. Space their starts and bound requests waiting for response
  // headers; the permit is released on HTTP acceptance, not after the GPU job.
  const submitQueue = [];
  let submitInFlight = 0;
  let submitLastStartedAt = 0;
  let submitPumpTimer = null;
  const SUBMIT_START_GAP_MS = 250;
  const SUBMIT_MAX_IN_FLIGHT = 4;

  function pumpSubmitQueue() {
    if (!submitQueue.length || submitInFlight >= SUBMIT_MAX_IN_FLIGHT) return;
    const wait = Math.max(0, SUBMIT_START_GAP_MS - (Date.now() - submitLastStartedAt));
    if (wait > 0) {
      if (!submitPumpTimer) {
        submitPumpTimer = setTimeout(() => {
          submitPumpTimer = null;
          pumpSubmitQueue();
        }, wait);
      }
      return;
    }
    const entry = submitQueue.shift();
    submitInFlight += 1;
    submitLastStartedAt = Date.now();
    Promise.resolve().then(entry.start).then(entry.resolve, entry.reject).finally(() => {
      submitInFlight -= 1;
      pumpSubmitQueue();
    });
    // Arrange the next spaced start even while this request is in flight.
    pumpSubmitQueue();
  }

  function materializeDefaultModel(id) {
    const element = nodeElement(id);
    const checkpoint = element && element.querySelector('[data-param="checkpoint"]');
    const serviceId = meta(id)?.service;
    if (!checkpoint || checkpoint.value || !['image', 'video'].includes(serviceId)) return;
    const lora = element.querySelector('[data-param="lora"]');
    const expectedLora = lora ? lora.value : '';
    const query = new URLSearchParams({service:serviceId});
    if (expectedLora) query.set('lora', expectedLora);
    const promise = Promise.resolve().then(() => {
      addWorkflowSelectionContext(id, query);
      return fetch('/api/ai/model-settings?' + query);
    }).then(async response => {
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail?.message_string || 'Choose a compatible model');
      if (!element.isConnected || checkpoint.value || (lora?.value || '') !== expectedLora) return;
      if (data.checkpoint_string) {
        checkpoint.value = data.checkpoint_string;
        const picker = element.querySelector('[data-model-param="checkpoint"]')?._picker;
        if (picker) picker.value = data.checkpoint_string;
      }
    }).catch(error => {
      if (element.isConnected && !checkpoint.value) element.querySelector('.nrec').textContent = error.message;
    }).finally(() => {
      if (pendingModelSelections.get(String(id)) === promise) pendingModelSelections.delete(String(id));
    });
    pendingModelSelections.set(String(id), promise);
  }

  async function prepareGraphSnapshot() {
    await Promise.all([...pendingImageUploads.values(), ...pendingModelSelections.values()]);
  }

  function addWorkflowSelectionContext(id, query) {
    const data = editor.getNodeFromId(id);
    const fields = meta(id)?.inFields || [];
    fields.forEach((field, index) => {
      if (field.startsWith('control_') && data?.inputs?.['input_' + (index + 1)]?.connections?.length)
        query.set('control_channel', field.slice(8));
    });
    const mode = nodeElement(id)?.querySelector('[data-param="mode"]')?.value;
    if (mode) query.set('mode', mode);
  }

  function pacedSubmitFetch(url, options) {
    return new Promise((resolve, reject) => {
      submitQueue.push({start: () => fetch(url, options), resolve, reject});
      pumpSubmitQueue();
    });
  }

  async function readJsonResponse(response) {
    const text = await response.text();
    if (!text.trim()) return {data:null, text:''};
    try {
      return {data:JSON.parse(text), text};
    } catch (error) {
      return {data:null, text};
    }
  }

  function retryAfterMilliseconds(response) {
    const raw = response.headers?.get?.('Retry-After');
    if (!raw) return 1000;
    const seconds = Number(raw);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
    const at = Date.parse(raw);
    return Number.isFinite(at) ? Math.max(0, at - Date.now()) : 1000;
  }

  function nonJsonHttpError(response, text) {
    const status = Number(response.status) || 0;
    const kind = /^\s*</.test(text || '') ? 'an HTML error page' : 'a non-JSON response';
    const ambiguous = [502, 504].includes(status)
      ? ' It was not retried because the farm may already have accepted the task.' : '';
    return `HTTP ${status || 'error'} — the service returned ${kind}.${ambiguous}`;
  }

  async function submitJson(url, body, onRetry) {
    for (let attempt = 0; attempt <= 6; attempt += 1) {
      let response;
      try {
        response = await pacedSubmitFetch(url, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(body)
        });
      } catch (error) {
        throw new Error('Could not connect to the service. The request was not retried because its acceptance is unknown.');
      }
      let parsed;
      try {
        parsed = await readJsonResponse(response);
      } catch (error) {
        throw new Error('The service response could not be read. It was not retried because task acceptance is unknown.');
      }
      if (response.status === 429 && attempt < 6) {
        const delay = Math.min(15000, retryAfterMilliseconds(response)) +
          Math.floor(Math.random() * 126);
        if (onRetry) onRetry({attempt:attempt + 1, delay});
        await sleep(delay);
        continue;
      }
      if (!response.ok) {
        if (parsed.data) throw new Error(describeError(parsed.data, response.status));
        throw new Error(nonJsonHttpError(response, parsed.text));
      }
      if (!parsed.data || typeof parsed.data !== 'object' || Array.isArray(parsed.data)) {
        throw new Error(nonJsonHttpError(response, parsed.text));
      }
      return parsed.data;
    }
    throw new Error('The service is still rate limited after 6 retries. Try Render again shortly.');
  }

  function taskStateReporter(element, tracker, accepted, execution) {
    return data => {
      if (execution && !executionIsCurrent(execution)) return;
      const status = String(data.status_string || data.status || 'queued').toLowerCase();
      const worker = data.node_string || data.render_server_name || accepted.node_string ||
        (String(accepted.task_id_string || '').includes('.') ? accepted.task_id_string.split('.')[0] : '');
      const active = ['rendering', 'processing', 'running', 'in_progress', 'generating', 'converting'].includes(status);
      const phase = data.stage_string || data.stage || (active ? 'rendering' : 'queued');
      element.textContent = (active ? phase : status === 'completed' ? 'completed' : 'queued') +
        (worker ? ' · ' + worker : ' — waiting for a worker');
      if (tracker && tracker.setState) tracker.setState({active, worker, phase,
        startedAt: data.started_at_unix_float || 0});
      document.dispatchEvent(new CustomEvent('ai-task-status', {detail: {worker, status}}));
    };
  }

  async function pollAiStatus(accepted, runner, report) {
    if (accepted[runner.field]) return accepted[runner.field];
    const id = accepted.task_id_string;
    for (let attempt = 0; attempt < 120; attempt++) {
      await sleep(3000);
      const data = await fetch('/api/ai/status/' + encodeURIComponent(id)).then(r => r.json()).catch(() => null);
      if (!data) continue;
      if (report) report(data);
      if (data.finished_bool) {
        if (data[runner.field]) return data[runner.field];
        throw new Error(data.error_string || 'the model returned nothing');
      }
    }
    throw new Error('the answer did not arrive in time');
  }

  async function pollForFile(accepted, runner, report) {
    const url = accepted[runner.field];
    if (!url) throw new Error('the farm accepted the job without an output address');
    // The farm publishes where the file will be before it exists, so the file
    // appearing is the completion signal. Video is allowed half an hour.
    for (let attempt = 0; attempt < 800; attempt++) {
      if (accepted.task_id_string) {
        const status = await fetch('/api/ai/render-status/' + encodeURIComponent(accepted.task_id_string))
          .then(response => response.ok ? response.json() : null).catch(() => null);
        if (status) {
          if (report) report(status);
          if (status.status_string === 'failed' || status.status_string === 'cancelled') {
            throw new Error(status.error_string || status.status_string);
          }
          if (status.status_string === 'completed') return status.output_url_string || url;
        }
      }
      const probe = await fetch(url, { method: 'HEAD' }).catch(() => null);
      if (probe && probe.ok) return url;
      await sleep(2500);
    }
    throw new Error('the render did not land in time; it may still be running');
  }

  async function poll3dStatus(accepted, runner, report) {
    const id = accepted.task_id_string;
    for (let attempt = 0; attempt < 400; attempt++) {
      await sleep(5000);
      const data = await fetch('/api/3dmodel/status/' + encodeURIComponent(id))
        .then(r => r.json()).catch(() => null);
      if (!data) continue;
      if (report) report(data);
      if (data.finished_bool) {
        if (data.model_url_string) return data.model_url_string;
        throw new Error(data.error_string || 'the node did not produce a model');
      }
    }
    throw new Error('the model did not arrive in time');
  }

  function bodyFor(serviceId, resolved, params) {
    const body = {};
    if (serviceId.startsWith('control_')) body.channel = serviceId.slice('control_'.length);
    if (serviceId === 'video_frame') body.view = 'first_frame';
    if (serviceId === 'video_storyboard') body.view = 'storyboard';
    Object.keys(params || {}).forEach(name => {
      if (name.startsWith('_')) return;
      const value = params[name];
      // A zero or an empty string here means "leave the workflow's own value",
      // so it is left out rather than sent as an override.
      if (value !== '' && (value !== 0 || name.startsWith('control_')) && value !== null && value !== undefined) body[name] = value;
    });
    Object.keys(resolved).forEach(field => {
      const value = resolved[field];
      if (field === 'image' || field === 'image_url_end') {
        const key = field === 'image' ? 'image_url' : 'image_url_end';
        const inline = field === 'image' ? 'image_base64' : 'image_base64_end';
        if (String(value).startsWith('data:')) body[inline] = value; else body[key] = value;
      } else {
        body[field] = value;
      }
    });
    return body;
  }

  // The language models charge their reasoning to the answer budget, so a
  // budget that is merely a bit small produces no answer at all rather than a
  // short one. One retry at double covers the case the per-model default does
  // not, and says it is doing so instead of looking like a stall.
  const BUDGET_EXHAUSTED = 'model_spent_its_budget_thinking';

  async function runServiceNode(id, resolved, params, execution, budget) {
    const attempt = budget ? 1 : 0;
    const node = meta(id);
    if (!node) throw new Error('node was removed');
    const runner = RUNNERS[node.service];
    const element = nodeElement(id);
    if (!element) throw new Error('node was removed');
    const state = element.querySelector('.nstate');
    const progress = element.querySelector('.nprog, .task-prog');
    const outBox = element.querySelector('.nout');
    if (executionIsCurrent(execution)) {
      state.textContent = 'sending…';
      state.className = 'nstate running';
      // Keep the last successful preview visible while its replacement renders.
    }
    let task = null;
    try {
      task = window.AIEntities ? window.AIEntities.startTask(progress, node.service) : null;
      const submitBody = Object.assign(
        bodyFor(node.service, resolved, params),
        budget ? {max_output_tokens:budget} : {});
      const accepted = await submitJson(runner.api, submitBody, retry => {
        if (!executionIsCurrent(execution)) return;
        state.textContent = `queued by the site — retrying in ${Math.ceil(retry.delay / 1000)}s`;
        state.className = 'nstate running';
      });
      execution.taskId = accepted.task_id_string || '';
      if (executionIsCurrent(execution)) state.textContent = 'queued — waiting for a worker';
      // Recorded before the wait, not after: the whole point is that a link
      // opened mid-render knows which task to carry on watching.
      if (executionIsCurrent(execution)) {
        recordResult(id, {
          status: 'running', type: runner.type,
          value: accepted[runner.field] || '',
          task_id: accepted.task_id_string || '',
          input_reference_url: execution.inputReference || '',
          started_at: Date.now() / 1000
        });
      }
      let value;
      try {
        value = await runner.finish(accepted, runner,
          taskStateReporter(state, task, accepted, execution));
      } catch (error) {
        if (!attempt && String(error.message || '').indexOf(BUDGET_EXHAUSTED) !== -1) {
          if (!executionIsCurrent(execution)) throw error;
          if (executionIsCurrent(execution)) {
            state.textContent = 'the answer budget ran out — retrying with more';
          }
          const body = bodyFor(node.service, resolved, params);
          const bigger = (Number(body.max_output_tokens) || 1024) * 2;
          if (task) task.clear();
          return runServiceNode(id, resolved, params, execution, Math.min(bigger, 8192));
        }
        throw error;
      }
      finishTaskTracker(task, true, execution, progress);
      if (executionIsCurrent(execution)) {
        state.textContent = accepted.cache_hit_bool ? 'cached' : 'done';
        state.className = 'nstate done';
        showResult(outBox, runner.type, value);
        recordResult(id, { status: 'done', type: runner.type, value: value,
                           input_reference_url: execution.inputReference || '',
                           task_id: accepted.task_id_string || '' });
      }
      return { type: runner.type, value: value };
    } catch (error) {
      finishTaskTracker(task, false, execution, progress);
      if (executionIsCurrent(execution)) {
        state.textContent = String(error.message || error);
        state.className = 'nstate failed';
        recordResult(id, { status: 'failed', type: runner.type, value: '',
                           error: String(error.message || error) });
      }
      throw error;
    }
  }

  function showResult(host, type, value) {
    host.innerHTML = '';
    if (type === 'text') {
      const block = document.createElement('div');
      block.className = 'ntext';
      block.textContent = value;
      host.appendChild(block);
      } else if (type === 'image' || type.startsWith('control_')) {
      const picture = document.createElement('img');
        picture.src = value;
        picture.classList.add('preview-expandable');
        picture.title = 'Click to enlarge';
        picture.addEventListener('click', event => { event.stopPropagation(); openPreview('image', value); });
      host.appendChild(picture);
    } else if (type === 'video') {
      const clip = document.createElement('video');
      clip.src = value;
        clip.controls = false;
      clip.loop = true;
      clip.muted = true;
        clip.autoplay = true;
        clip.playsInline = true;
        clip.classList.add('preview-expandable');
        clip.title = 'Click to enlarge';
        clip.tabIndex = 0;
        clip.addEventListener('keydown', event => {
          if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openPreview('video', value); }
        });
        clip.addEventListener('click', event => { event.preventDefault(); event.stopPropagation(); openPreview('video', value); });
        host.appendChild(clip);
        clip.play().catch(() => {});
    }
    const link = document.createElement('a');
    link.href = value;
    link.target = '_blank';
    link.rel = 'noopener';
    link.className = 'nlink';
    link.textContent = type === 'text' ? '' : 'open';
    if (link.textContent) host.appendChild(link);
  }

  function modelAcceptsConnectedControls(id, entry) {
    const node = editor.getNodeFromId(id);
    const item = meta(id);
    if (!node || !item) return true;
    return item.inFields.every((field, index) => {
      const connected = ((node.inputs['input_' + (index + 1)] || {}).connections || []).length;
      return !field.startsWith('control_') || !connected || ((entry || {}).control_channels || []).includes(field.slice(8));
    });
  }

  function openPreview(type, url) {
    let dialog = document.getElementById('media-preview');
    if (!dialog) {
      dialog = document.createElement('dialog');
      dialog.id = 'media-preview';
      dialog.setAttribute('aria-label', 'Media preview');
      document.body.appendChild(dialog);
      dialog.addEventListener('click', event => {
        if (event.target === dialog || event.target.classList.contains('media-stage')) dialog.close();
      });
      dialog.addEventListener('close', () => {
        dialog.querySelectorAll('video').forEach(video => video.pause());
        dialog.innerHTML = '';
      });
    }
    dialog.innerHTML = '';
    const close = document.createElement('button');
    close.type = 'button';
    close.className = 'media-close';
    close.setAttribute('aria-label', 'Close preview (Escape)');
    close.textContent = '\u00d7';
    close.addEventListener('click', () => dialog.close());
    const stage = document.createElement('div');
    stage.className = 'media-stage';
    const media = document.createElement(type === 'video' ? 'video' : 'img');
    media.src = url;
    if (type === 'video') {
      media.autoplay = true; media.loop = true; media.muted = true;
      media.playsInline = true; media.controls = true;
    } else {
      media.alt = 'Expanded preview';
      media.addEventListener('click', () => dialog.close());
    }
    stage.appendChild(media);
    dialog.append(stage, close);
    dialog.showModal();
    close.focus();
    if (type === 'video') media.play().catch(() => {});
  }

  function installWheelZoom() {
    const canvas = document.getElementById('canvas');
    editor.zoom_min = 0.15;
    editor.zoom_max = 2.5;
    canvas.addEventListener('wheel', event => {
      if (event.target.closest('input, textarea, select, .mpick-panel, .ntext')) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      const bounds = canvas.getBoundingClientRect();
      const x = event.clientX - bounds.left;
      const y = event.clientY - bounds.top;
      const oldZoom = editor.zoom;
      const nextZoom = Math.max(editor.zoom_min, Math.min(editor.zoom_max, oldZoom * Math.exp(-event.deltaY * 0.0015)));
      editor.canvas_x = x - (x - editor.canvas_x) * nextZoom / oldZoom;
      editor.canvas_y = y - (y - editor.canvas_y) * nextZoom / oldZoom;
      editor.zoom = nextZoom;
      editor.zoom_last_value = nextZoom;
      editor.precanvas.style.transform = `translate(${editor.canvas_x}px, ${editor.canvas_y}px) scale(${nextZoom})`;
      editor.dispatch('zoom', nextZoom);
    }, { passive: false, capture: true });
  }

  function graphFromCanvas() {
    const data = editor.export().drawflow.Home.data;
    const nodes = [];
    const links = [];
    Object.keys(data).forEach(id => {
      const raw = data[id];
      const node = meta(id);
      if (!node) return;
      if (node.kind === KIND_INPUT) {
        const field = nodeElement(id) && nodeElement(id).querySelector('[data-value]');
        nodes.push({
          id: String(id), kind: KIND_INPUT, entity_type: node.entityType,
          // A pasted data URL would bloat a shared link, so only an address is
          // carried; a file chosen by hand is deliberately not saved.
          value: field && !String(field.value).startsWith('data:') ? field.value : '',
          x: raw.pos_x, y: raw.pos_y, params: {_display_mode: node.displayMode || 'medium'}
        });
      } else {
        nodes.push({
          id: String(id), kind: KIND_SERVICE, service: node.service,
          x: raw.pos_x, y: raw.pos_y, params: readParams(id)
        });
      }
      Object.keys(raw.inputs || {}).forEach(inputClass => {
        (raw.inputs[inputClass].connections || []).forEach(connection => {
          const fromMeta = meta(connection.node);
          if (!fromMeta) return;
          links.push({
            from: String(connection.node),
            output: fromMeta.outFields[portIndex(connection.input)] || 'value',
            to: String(id),
            input: node.inFields[portIndex(inputClass)]
          });
        });
      });
    });
    const results = {};
    runState.forEach((value, key) => { results[key] = value; });
    return { name: document.getElementById('graph-name').value.trim() || 'Untitled',
             instance_id: graphInstanceId, comparison_anchor_id: comparisonAnchorId,
             nodes, links, results };
  }

  /** Nodes in an order where everything a node needs has already run. */
  function executionOrder(graph) {
    const incoming = new Map(graph.nodes.map(node => [node.id, 0]));
    graph.links.forEach(link => incoming.set(link.to, (incoming.get(link.to) || 0) + 1));
    const ready = graph.nodes.filter(node => !incoming.get(node.id)).map(node => node.id);
    const order = [];
    while (ready.length) {
      const id = ready.shift();
      order.push(id);
      graph.links.filter(link => link.from === id).forEach(link => {
        incoming.set(link.to, incoming.get(link.to) - 1);
        if (incoming.get(link.to) === 0) ready.push(link.to);
      });
    }
    return order;
  }

  /** Cancel is only offered while there is something to cancel. */
  function setRunning(running) {
    const cancel = document.getElementById('cancel');
    const carry = document.getElementById('continue');
    const run = document.getElementById('run');
    if (cancel) cancel.hidden = !running;
    if (carry) carry.hidden = running;
    if (run) {
      run.disabled = false;
      run.textContent = running ? 'Add to queue' : 'Render';
    }
  }

  function refreshRunningControls() {
    setRunning(runRequests.size > 0 || activeExecutions.size > 0 || restoredExecutions.size > 0);
  }

  function markState(id, message, className) {
    const element = nodeElement(id);
    if (!element) return;
    const state = element.querySelector('.nstate');
    state.textContent = message;
    state.className = className;
  }

  /** A run needs a link to publish its progress to, so one is made up front. */
  async function ensureSaved() {
    try {
      const response = await fetch('/api/ai/graphs', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(graphFromCanvas())
      });
      const data = await response.json();
      if (response.ok) {
        graphId = data.graph_id_string;
        history.replaceState(null, '', data.deep_link_string);
        document.dispatchEvent(new CustomEvent('ai-graph-saved', {detail:{graphId}}));
      }
    } catch (error) { /* a run is still worth doing without a link */ }
  }

  function startIncrementalService(id, node, resolved, params, signature, epoch, keepDone, graphSnapshot) {
    const idString = String(id);
    const key = executionKey(epoch, idString, signature);
    desiredSignatures.set(idString, signature);

    const restored = restoredExecutions.get(idString);
    if (restored && !restored.invalidated && restored.epoch === epoch) {
      return restored.promise;
    }

    const active = activeExecutions.get(key);
    if (active) {
      nodeRunVersions.set(idString, active.version);
      return active.promise;
    }

    const continued = keepDone && continuableResults.get(idString);
    if (continued) {
      const element = nodeElement(idString);
      if (element && epoch === canvasEpoch) {
        const state = element.querySelector('.nstate');
        state.textContent = 'continued';
        state.className = 'nstate done';
        showResult(element.querySelector('.nout'), continued.type, continued.value);
      }
      return Promise.resolve({type:continued.type, value:continued.value});
    }

    const requestBody = bodyFor(node.service, resolved, params);
    const completed = (completedExecutions.get(idString) || new Map()).get(signature);
    if (completed && reusableCompleted(node.service, requestBody)) {
      const element = nodeElement(idString);
      if (element && epoch === canvasEpoch) {
        const state = element.querySelector('.nstate');
        state.textContent = 'cached';
        state.className = 'nstate done';
        showResult(element.querySelector('.nout'), completed.type, completed.value);
        recordResult(idString, {status:'done', type:completed.type, value:completed.value,
          input_reference_url:completed.input_reference_url || (nodeCompare?.resolveReference(idString, graphSnapshot) || resolved.image || ''),
          task_id:completed.task_id || ''});
      }
      return Promise.resolve({type: completed.type, value: completed.value});
    }

    const version = (nodeRunVersions.get(idString) || 0) + 1;
    nodeRunVersions.set(idString, version);
    continuableResults.delete(idString);
    const execution = {id:idString, signature, epoch, version, taskId:'',
      inputReference:nodeCompare?.resolveReference(idString, graphSnapshot) || resolved.image || ''};
    const promise = runServiceNode(idString, resolved, params, execution)
      .then(result => {
        if (epoch === canvasEpoch && reusableCompleted(node.service, requestBody)) {
          const bucket = completedExecutions.get(idString) || new Map();
          bucket.set(signature, {...result, task_id:execution.taskId || '', input_reference_url:execution.inputReference || ''});
          // Keep a few useful variants without allowing a long editing session
          // to grow memory forever.
          while (bucket.size > 8) bucket.delete(bucket.keys().next().value);
          completedExecutions.set(idString, bucket);
        }
        return result;
      })
      .finally(() => {
        activeExecutions.delete(key);
        refreshRunningControls();
      });
    execution.promise = promise;
    activeExecutions.set(key, execution);
    refreshRunningControls();
    return promise;
  }

  /** Add the current graph snapshot to the live queue. */
  async function runGraph(keepDone) {
    await prepareGraphSnapshot();
    const graph = graphFromCanvas();
    const epoch = canvasEpoch;
    const order = executionOrder(graph);
    if (order.length !== graph.nodes.length) {
      toast('The wiring loops back on itself, so there is no order to run it in.');
      return;
    }
    const inputValues = new Map();
    graph.nodes.filter(node => node.kind === KIND_INPUT).forEach(node => {
      const field = nodeElement(node.id)?.querySelector('[data-value]');
      inputValues.set(node.id, field ? field.value.trim() : '');
    });
    const token = {cancelled:false, id:++nextRunRequestId};
    graph.nodes.filter(node => node.kind === KIND_SERVICE).forEach(node => {
      latestPlannedRequests.set(String(node.id), token.id);
    });
    runRequests.add(token);
    refreshRunningControls();
    await ensureSaved();
    // Nodes whose inputs are all ready run together: the two clips and the 3D
    // model come off the same picture and there is no reason to queue them.
    const byId = new Map(graph.nodes.map(node => [node.id, node]));
    try {
      const pending = new Map();
      for (const id of order) {
        const node = byId.get(id);
        const feeds = graph.links.filter(link => link.to === id);
        const start = (async () => {
          const upstreamRecords = await Promise.all(feeds.map(link => pending.get(link.from)));
          const superseded = node.kind === KIND_SERVICE &&
            latestPlannedRequests.get(String(id)) !== token.id;
          if (superseded || epoch !== canvasEpoch || !meta(id)) {
            return {ok:false, superseded:true};
          }
          if (token.cancelled) {
            markState(id, 'cancelled before it started', 'nstate');
            return {ok:false, cancelled:true};
          }
          if (upstreamRecords.some(record => !record || !record.ok)) {
            if (epoch === canvasEpoch && meta(id)) {
              markState(id, 'skipped — what it needed did not arrive', 'nstate failed');
            }
            return {ok:false};
          }
          if (node.kind === KIND_INPUT) {
            const value = inputValues.get(id) || '';
            if (!value) return {ok:false, error:'empty input'};
            return {ok:true, result:{type:node.entity_type, value}};
          }
          const resolved = {};
          feeds.forEach((link, index) => {
            const upstream = upstreamRecords[index]?.result;
            if (upstream) resolved[link.input] = upstream.value;
          });
          const params = {...(node.params || {})};
          const requestBody = bodyFor(node.service, resolved, params);
          const signature = stableJson({service:node.service, body:requestBody});
          try {
            const result = await startIncrementalService(
              id, node, resolved, params, signature, epoch, keepDone, graph);
            if (!graph.results) graph.results = {};
            graph.results[id] = {status:'done', type:result.type, value:result.value};
            return {ok:true, result};
          } catch (error) {
            return {ok:false, error:String(error.message || error)};
          }
        })();
        pending.set(id, start);
      }
      const settled = await Promise.all(pending.values());
      const serviceIds = order.filter(id => byId.get(id)?.kind === KIND_SERVICE);
      const produced = serviceIds.filter(id => settled[order.indexOf(id)]?.ok).length;
      const failed = serviceIds.length - produced;
      if (!settled.some(record => record?.superseded)) {
        toast(failed
          ? `${produced} of ${serviceIds.length} steps finished; ${failed} did not.`
          : `All ${serviceIds.length} steps finished.`);
      }
    } finally {
      runRequests.delete(token);
      refreshRunningControls();
    }
  }

  /**
   * Stop the composition without touching what the farm has already begun.
   *
   * Steps not yet submitted are dropped, and anything already queued on the
   * farm but not yet picked up is asked to stand down. A job that has started
   * rendering is deliberately left alone.
   */
  async function cancelRun() {
    runRequests.forEach(token => { token.cancelled = true; });
    toast('Cancelling — anything already rendering will finish.');
    const ids = [...activeExecutions.values()].map(execution => execution.taskId).filter(Boolean);
    runState.forEach(record => {
      if (record.status === 'running' && record.task_id) ids.push(record.task_id);
    });
    const uniqueIds = [...new Set(ids)];
    if (!uniqueIds.length) return;
    try {
      const response = await fetch('/api/ai/cancel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task_ids: uniqueIds })
      });
      const data = await response.json();
      if (response.ok) {
        toast(data.cancelled_int
          ? `${data.cancelled_int} queued job(s) stood down; ${data.running_int} already rendering and left to finish.`
          : `Nothing was still queued; ${data.running_int} already rendering and left to finish.`);
      }
    } catch (error) { /* the local stop already happened */ }
  }

  /**
   * Throw away what these compositions have left on the site's disk.
   *
   * The farm nodes clean up after themselves; this is the server's own cache
   * of saved graphs and the pictures and clips they produced.
   */
  async function purgeCache() {
    try {
      const response = await fetch('/api/ai/request-cache', {method:'DELETE'});
      const data = await response.json();
      if (!response.ok) throw new Error('The request cache could not be cleared');
      completedExecutions.clear();
      continuableResults.clear();
      toast(`Cleared ${data.entries_removed_int} cached request(s). Render will compute fresh results.`);
    } catch (error) { toast(error.message); }
  }

  /* -------------------------------------------------------- load and save */

  /**
   * Put a stored run back on the canvas.
   *
   * A node that had finished shows what it produced. A node that was still
   * running when the link was made is picked up where it was left: the same
   * poller is started again from the stored task, so a link opened while a
   * clip renders fills in when the clip lands rather than staying frozen.
   */
  function restoreResults(results, mapping) {
    Object.keys(results || {}).forEach(originalId => {
      const record = results[originalId];
      const id = mapping.get(originalId);
      const element = id && nodeElement(id);
      if (!element) return;
      const state = element.querySelector('.nstate');
      const outBox = element.querySelector('.nout');
      runState.set(String(id), record);
      if (['done', 'stale'].includes(record.status) && record.value) {
        if (record.status === 'done') continuableResults.set(String(id), {type:record.type, value:record.value,
          task_id:record.task_id || ''});
        state.textContent = record.status === 'stale' ? 'changed — render to update' : 'done';
        state.className = record.status === 'stale' ? 'nstate' : 'nstate done';
        showResult(outBox, record.type, record.value);
        return;
      }
      if (record.status === 'failed') {
        state.textContent = record.error || 'failed';
        state.className = 'nstate failed';
        return;
      }
      if (record.status !== 'running') return;
      const registration = {id:String(id), epoch:canvasEpoch, invalidated:false, promise:null};
      registration.promise = resumeNode(id, record, registration)
        .finally(() => {
          if (restoredExecutions.get(String(id)) === registration) {
            restoredExecutions.delete(String(id));
          }
          refreshRunningControls();
        });
      restoredExecutions.set(String(id), registration);
      refreshRunningControls();
      registration.promise.catch(() => {});
    });
  }

  async function resumeNode(id, record, registration) {
    const epoch = canvasEpoch;
    const stillHere = () => !registration.invalidated && epoch === canvasEpoch &&
      meta(id) && nodeElement(id);
    const node = meta(id);
    if (!node) return;
    const runner = RUNNERS[node.service];
    const element = nodeElement(id);
    const state = element.querySelector('.nstate');
    const outBox = element.querySelector('.nout');
    state.textContent = 'still running on the farm…';
    state.className = 'nstate running';
    const task = window.AIEntities
      ? window.AIEntities.startTask(element.querySelector('.nprog, .task-prog'), node.service)
      : null;
    // The pollers only need what the submit returned, and that is exactly
    // what was stored, so the same code finishes the job.
    const accepted = { task_id_string: record.task_id };
    accepted[runner.field] = record.value || '';
    try {
      const reporter = taskStateReporter(state, task, accepted);
      const value = await runner.finish(accepted, runner, data => {
        if (stillHere()) reporter(data);
      });
      if (!stillHere()) return;
      state.textContent = accepted.cache_hit_bool ? 'cached' : 'done';
      state.className = 'nstate done';
      if (task) task.finish(true);
      showResult(outBox, runner.type, value);
      recordResult(id, { status: 'done', type: runner.type, value: value,
                         input_reference_url:record.input_reference_url || '', history:record.history || [],
                         task_id: record.task_id || '' });
      return {type:runner.type, value};
    } catch (error) {
      if (!stillHere()) throw error;
      state.textContent = String(error.message || error);
      state.className = 'nstate failed';
      if (task) task.finish(false);
      recordResult(id, { status: 'failed', type: runner.type, value: '',
                         error: String(error.message || error) });
      throw error;
    }
  }

  function loadGraph(graph) {
    resetCanvasExecutionState();
    editor.clear();
    nodeMeta.clear();
    runState.clear();
    graphInstanceId = graph.instance_id || '';
    comparisonAnchorId = '';
    document.getElementById('graph-name').value = graph.name || 'Untitled';
    const mapping = new Map();
    (graph.nodes || []).forEach(node => {
      const id = node.kind === KIND_INPUT
        ? addInputNode(node.entity_type, node.x, node.y, node.value, node.params)
        : addServiceNode(node.service, node.x, node.y, node.params);
      if (id) mapping.set(node.id, id);
    });
    (graph.links || []).forEach(link => {
      const from = mapping.get(link.from);
      const to = mapping.get(link.to);
      if (!from || !to) return;
      const fromMeta = meta(from);
      const toMeta = meta(to);
      const outIndex = Math.max(0, fromMeta.outFields.indexOf(link.output));
      const inIndex = toMeta.inFields.indexOf(link.input);
      if (inIndex < 0) return;
      editor.addConnection(from, to, 'output_' + (outIndex + 1), 'input_' + (inIndex + 1));
    });
    comparisonAnchorId = String(mapping.get(graph.comparison_anchor_id) || '');
    restoreResults(graph.results, mapping);
    refreshRunningControls();
  }

  /**
   * Copy, with a fallback.
   *
   * `navigator.clipboard` needs the click still to count as a user gesture,
   * and awaiting a save first can spend that. When it refuses, a hidden
   * textarea and `execCommand` still work, and if even that fails the link is
   * put on screen so it can be copied by hand rather than silently lost.
   */
  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text).then(() => true, () => legacyCopy(text));
    }
    return Promise.resolve(legacyCopy(text));
  }

  function legacyCopy(text) {
    const holder = document.createElement('textarea');
    holder.value = text;
    holder.setAttribute('readonly', '');
    holder.style.cssText = 'position:fixed;top:0;left:0;opacity:0';
    document.body.appendChild(holder);
    holder.select();
    let ok = false;
    try { ok = document.execCommand('copy'); } catch (error) { ok = false; }
    document.body.removeChild(holder);
    return ok;
  }

  async function saveGraph() {
    await prepareGraphSnapshot();
    // If the graph already has a link, the click copies it at once and the
    // save follows: the copy then happens while the gesture is still live.
    if (graphId) {
      const known = location.origin + '/nodes?g=' + graphId;
      const copied = await copyText(known);
      toast(copied ? 'Deep link copied: ' + known : 'Deep link: ' + known);
    }
    const graph = graphFromCanvas();
    const response = await fetch('/api/ai/graphs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(graph)
    });
    const data = await response.json();
    if (!response.ok) {
      const detail = data.detail || {};
      toast(detail.message_string || 'The graph was not saved.');
      return;
    }
    const changed = graphId !== data.graph_id_string;
    graphId = data.graph_id_string;
    const link = location.origin + data.deep_link_string;
    history.replaceState(null, '', data.deep_link_string);
    document.dispatchEvent(new CustomEvent('ai-graph-saved', {detail:{graphId}}));
    if (!changed) return;
    const copied = await copyText(link);
    toast(copied ? 'Deep link copied: ' + link : 'Deep link (copy it): ' + link);
  }

  async function duplicateGraph() {
    const button = document.getElementById('duplicate-graph');
    button.disabled = true;
    try {
      await prepareGraphSnapshot();
      const response = await fetch('/api/ai/graphs/duplicate', {method:'POST',
        headers:{'Content-Type':'application/json'}, body:JSON.stringify({currentGraph:graphFromCanvas()})});
      const data = await response.json();
      if (!response.ok || !data.graph_object) throw new Error(data.detail?.message_string || 'The graph copy was not saved');
      // The copy contains this exact canvas. Keep its live nodes and running
      // task subscriptions in place; only its independent identity changes.
      graphInstanceId = data.graph_object.instance_id || '';
      graphId = data.graph_id_string;
      history.replaceState(null, '', data.deep_link_string);
      document.dispatchEvent(new CustomEvent('ai-graph-saved', {detail:{graphId}}));
      toast('Independent graph copy created: ' + location.origin + data.deep_link_string);
    } catch (error) { toast(error.message); }
    finally { button.disabled = false; }
  }

  /* ------------------------------------------------------------- the shell */

  function toast(message) {
    const host = document.getElementById('toast');
    host.textContent = message;
    host.className = 'toast shown';
    clearTimeout(host._timer);
    host._timer = setTimeout(() => { host.className = 'toast'; }, 6000);
  }

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  function escapeAttr(value) {
    return escapeHtml(value).replace(/"/g, '&quot;');
  }

  function buildPalette() {
    const host = document.getElementById('palette');
    host.innerHTML = '';
    const heading = document.createElement('div');
    heading.className = 'pgroup';
    heading.textContent = 'Sources';
    host.appendChild(heading);
    [['image', 'Image in'], ['video', 'Video in'], ['text', 'Text in'], ['avatar', 'Avatar']].forEach(([type, title]) => {
      host.appendChild(paletteButton(title, typeIcon(type), '', () =>
        addInputNode(type, 60 + editor.canvas_x * -1, 80, '')));
    });
    const services = document.createElement('div');
    services.className = 'pgroup';
    services.textContent = 'Tools';
    host.appendChild(services);
    (catalogue.services_array || []).forEach(entry => {
      const disabled = entry.status !== 'live';
      const button = paletteButton(entry.title, typeIcon((entry.produces_array || [])[0]),
                                   entry.summary, () => addServiceNode(entry.id, 140, 120, null));
      if (disabled) { button.disabled = true; button.title = 'Not wired up yet'; }
      host.appendChild(button);
    });
  }

  function paletteButton(title, icon, help, onClick) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'pitem';
    button.innerHTML = `<span class="picon">${icon}</span><span><b>${escapeHtml(title)}</b>`
                     + (help ? `<i>${escapeHtml(help)}</i>` : '') + '</span>';
    button.addEventListener('click', onClick);
    return button;
  }

  async function boot() {
    await loadCatalogue();
    editor = new Drawflow(document.getElementById('canvas'));
    editor.reroute = true;
    editor.start();
    if (window.AINodeDisplay) nodeDisplay = window.AINodeDisplay.install({editor,
      canvas:document.getElementById('canvas'), getMeta:meta, defaultMode:'medium',
      onModeChange:(id, mode) => { const value=meta(id); if(value) value.displayMode=mode; if(nodeCompare) nodeCompare.refresh(id); }});
    if (window.AINodeShare) window.AINodeShare.install({canvas:document.getElementById('canvas'), getMeta:meta, toast});
    installWheelZoom();
    if (window.AINodeGroups) nodeGroups = window.AINodeGroups.install({editor,
      canvas:document.getElementById('canvas'), getMeta:meta, addInputNode,
      addServiceNode, exportGraph:graphFromCanvas, toast, nodeLimit:200,
      onNodesRemoved:forgetNodes,
      onSetComparisonAnchor:id => nodeCompare && nodeCompare.setAnchor(id)});
    document.addEventListener('paste', event => {
      const item = [...(event.clipboardData?.items || [])].find(value => value.type.startsWith('image/'));
      if (!item) return;
      const current = event.target.closest && event.target.closest('.drawflow-node');
      const target = current || document.querySelector('#canvas .drawflow-node.selected');
      if (!target || !target._acceptImage) { toast('Select an Image in node to paste an image.'); return; }
      event.preventDefault();
      target._acceptImage(item.getAsFile());
    });
    editor.on('connectionCreated', onConnectionCreated);
    editor.on('connectionRemoved', connection => {
      if (connection && connection.input_id != null) {
        invalidateNodeAndDownstream(connection.input_id);
      }
    });
    editor.on('nodeRemoved', id => forgetNodes([id]));
    // A range's number is only useful if it is shown next to the slider.
    document.getElementById('canvas').addEventListener('change', event => {
      if (event.target.dataset && event.target.dataset.param) {
        event.target.dataset.touched = 'yes';
      }
      const node = event.target.closest && event.target.closest('.drawflow-node');
      if (node && (event.target.dataset?.param || event.target.dataset?.value !== undefined)) {
        invalidateNodeAndDownstream(node.id.replace(/^node-/, ''));
      }
    });
    document.getElementById('canvas').addEventListener('input', event => {
      if (event.target.dataset && event.target.dataset.param) {
        event.target.dataset.touched = 'yes';
      }
      const node = event.target.closest && event.target.closest('.drawflow-node');
      if (node && (event.target.dataset?.param || event.target.dataset?.value !== undefined)) {
        invalidateNodeAndDownstream(node.id.replace(/^node-/, ''));
      }
      if (event.target.type !== 'range') return;
      const readout = event.target.parentElement.querySelector('output');
      if (readout) readout.textContent = rangeLabel(event.target.value);
    });

    buildPalette();
    // The strip says what the whole farm is doing, coloured by the kind of
    // work, which on this page matters more than on any single-service one:
    // a composition is waiting on several sorts of job at once.
    if (window.AIEntities) {
      window.AIEntities.mountFleet(document.getElementById('fleet'), 'image');
    }
    document.getElementById('run').addEventListener('click', () => runGraph(false));
    document.getElementById('continue').addEventListener('click', () => runGraph(true));
    document.getElementById('save').addEventListener('click', saveGraph);
    document.getElementById('duplicate-graph').addEventListener('click', duplicateGraph);
    document.getElementById('cancel').addEventListener('click', cancelRun);
    document.getElementById('purge').addEventListener('click', purgeCache);
    document.getElementById('clear').addEventListener('click', () => {
      resetCanvasExecutionState();
      editor.clear();
      nodeMeta.clear();
      runState.clear();
      graphId = null;
      graphInstanceId = ''; comparisonAnchorId = '';
      history.replaceState(null, '', '/nodes');
    });
    setRunning(false);

    const templates = await fetch('/api/ai/graph/templates').then(r => r.json()).catch(() => ({}));
    const picker = document.getElementById('templates');
    (templates.templates_array || []).forEach(template => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'tmpl';
      button.innerHTML = `<b>${escapeHtml(template.title)}</b><i>${escapeHtml(template.summary)}</i>`;
      button.addEventListener('click', () => loadGraph(template.graph));
      picker.appendChild(button);
    });

    const wanted = new URLSearchParams(location.search).get('g');
    const wantedAvatar = new URLSearchParams(location.search).get('avatar');
    if (wanted) {
      const data = await fetch('/api/ai/graphs/' + encodeURIComponent(wanted))
        .then(r => r.json()).catch(() => null);
      if (data && data.success_bool) {
        if (!data.template_bool) graphId = data.graph_id_string;
        loadGraph(data.graph_object);
      } else {
        toast('That link does not open a graph any more.');
      }
    } else if (wantedAvatar) {
      resetCanvasExecutionState();
      editor.clear(); nodeMeta.clear(); runState.clear();
      document.getElementById('graph-name').value = 'Avatar production';
      addInputNode('avatar', 60, 100, wantedAvatar);
    } else if ((templates.templates_array || []).length) {
      loadGraph(templates.templates_array[0].graph);
    }
    if (window.AINodeCompare) nodeCompare = window.AINodeCompare.install({
      canvas:document.getElementById('canvas'), getMeta:meta, getGraph:graphFromCanvas,
      getResult:id => runState.get(String(id)), getAnchorId:() => comparisonAnchorId,
      setAnchorId:id => { comparisonAnchorId = String(id || ''); }, toast});
    if (window.AIGraphBridge) graphBridge = window.AIGraphBridge.create({
      editor, getGraph:graphFromCanvas, addServiceNode, addInputNode, applyParams, getMeta:meta,
      forgetNodes, invalidateNodeAndDownstream, nodeDisplay, applyRecommended,
      setGraphName:name => { document.getElementById('graph-name').value = name; }, toast});
    if (window.AIGraphAgent && graphBridge) graphAgent = window.AIGraphAgent.install({
      getGraph:graphFromCanvas, getCatalogue:() => catalogue,
      getSelectedIds:() => Array.from(nodeGroups?.selected || []),
      prepareGraph:prepareGraphSnapshot, applyGraph:graphBridge.applyGraph,
      saveGraph:async () => { await prepareGraphSnapshot(); await ensureSaved(); return graphId; }});
  }

  window.addEventListener('DOMContentLoaded', () => {
    boot().catch(error => toast('The editor could not start: ' + error.message));
  });
})();

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
  let nodePlacement = null;
  let nodePipelines = null;
  let graphAgent = null;
  let graphBridge = null;
  let graphInstanceId = '';
  // Graph-wide render quality (ai-render-quality.js): scales every width and
  // height at submit time; node params keep the base size.
  let renderQuality = 'normal';
  let renderQualityToolbar = null;
  let renderQualityBadges = null;
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

  /**
   * Did the page write this value, or did the person?
   *
   * Follow-input-size rewrites width and height whenever a graph is loaded or
   * rewired, and it announces those writes with the same events a typed value
   * fires. The canvas answers such an event by invalidating the node and
   * everything downstream of it, so merely opening a saved composition threw
   * away every finished picture in it and stopped following a run that was
   * still in flight. A control marked by its own code is still read and
   * redrawn; it is simply not treated as an edit.
   */
  function programmaticParamEvent(event) {
    const target = event && event.target;
    return !!(target && target.dataset && target.dataset.silentUpdate === 'yes');
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
        task_id:record.task_id || '', outputs:record.outputs || null});
      // A finished step is new information about size: a ControlNet map that
      // has just landed is what the image below it must now be drawn at.
      if (nodeGroups && nodeGroups.refreshSizes) nodeGroups.refreshSizes();
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
        `<option value="${escapeAttr(option.value)}"${String(option.value) === String(param.default) ? ' selected' : ''}${option.disabled ? ' disabled' : ''}>${escapeHtml(option.title)}</option>`
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
    } else if (param.type === 'lora_stack') {
      // Civitai/A1111 tags, applied in order. The same tags also work inside
      // the prompt; the server parses both the same way.
      control = `<input type="text" data-param="${name}" value="${escapeAttr(param.default || '')}" `
              + `placeholder="&lt;lora:name:0.8&gt;" spellcheck="false" autocomplete="off"${help}>`
              + `<select class="lstack-add" title="Add a LoRA to the stack"><option value="">+</option></select>`;
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

  function updateSamplingReadout(control) {
    if (!control) return;
    const element = control.closest('.drawflow-node');
    const readout = element && element.querySelector('[data-for="' + CSS.escape(control.dataset.param || '') + '"]');
    if (!readout) return;
    if (control.dataset.policyFixedLabel) readout.textContent = control.dataset.policyFixedLabel;
    else if (Number(control.value) === 0 && control.dataset.policyAutoLabel) readout.textContent = control.dataset.policyAutoLabel;
    else readout.textContent = rangeLabel(control.value);
  }

  function samplingNote(control, text) {
    if (!control) return;
    const row = control.closest('.nparam');
    if (!row) return;
    let note = row.querySelector('.sampling-policy-note');
    if (!text) { if (note) note.remove(); return; }
    if (!note) {
      note = document.createElement('small');
      note.className = 'sampling-policy-note';
      note.style.cssText = 'display:block;flex:1 0 100%;color:#8ab4ff;font-size:10px;text-align:right';
      row.appendChild(note);
    }
    note.textContent = text;
  }

  function unlockPolicyControl(control, emptyValue) {
    if (!control) return;
    if (control.dataset.policyGenerated !== undefined &&
        String(control.value) === control.dataset.policyGenerated) control.value = emptyValue;
    control.disabled = false;
    delete control.dataset.policyGenerated;
    delete control.dataset.policyFixedLabel;
    delete control.dataset.policyAutoLabel;
    samplingNote(control, '');
  }

  function applySamplingPolicy(id, policy) {
    const element = nodeElement(id);
    if (!element) return [];
    policy = policy || {};
    const steps = element.querySelector('[data-param="steps"]');
    const cfg = element.querySelector('[data-param="cfg"]');
    const scheduler = element.querySelector('[data-param="scheduler"]');
    const descriptions = [];

    unlockPolicyControl(steps, '0');
    unlockPolicyControl(cfg, '0');
    unlockPolicyControl(scheduler, '');
    if (steps) {
      if (!steps.dataset.originalMin) steps.dataset.originalMin = steps.min;
      if (!steps.dataset.originalMax) steps.dataset.originalMax = steps.max;
      // Zero remains the Auto sentinel even when explicit manual steps start at 1.
      steps.min = '0';
      steps.max = policy.steps_max != null ? String(policy.steps_max) : steps.dataset.originalMax;
      const autoSteps = Number(policy.auto_steps);
      if (autoSteps > 0) steps.dataset.policyAutoLabel = 'Auto (' + autoSteps + ')';
      const fixedSteps = Number(policy.fixed_steps);
      if (fixedSteps > 0) {
        steps.value = String(fixedSteps); steps.disabled = true;
        steps.dataset.policyGenerated = String(fixedSteps);
        steps.dataset.policyFixedLabel = 'Fixed (' + fixedSteps + ')';
        samplingNote(steps, 'Workflow requires exactly ' + fixedSteps + ' steps');
        descriptions.push('steps fixed ' + fixedSteps);
      } else if (autoSteps > 0) {
        samplingNote(steps, 'Auto uses ' + autoSteps + ' steps; manual values stay explicit');
        descriptions.push('steps Auto ' + autoSteps);
      }
      updateSamplingReadout(steps);
    }
    if (cfg && policy.cfg_mode === 'fixed') {
      const value = Number.isFinite(Number(policy.cfg_value)) ? Number(policy.cfg_value) : 1;
      cfg.value = String(value); cfg.disabled = true; cfg.dataset.policyGenerated = String(value);
      samplingNote(cfg, 'Fixed CFG ' + value + (value === 1 ? ' · no extra guidance' : ''));
      descriptions.push('CFG fixed ' + value);
    }
    if (scheduler && policy.scheduler_mode === 'native') {
      scheduler.value = ''; scheduler.disabled = true; scheduler.dataset.policyGenerated = '';
      const label = policy.scheduler_label || 'Native scheduler';
      samplingNote(scheduler, label); descriptions.push(label);
    }
    element._samplingPolicy = policy;
    return descriptions;
  }

  function refreshModeOptions(id, checkpointEntry) {
    const mode = nodeElement(id)?.querySelector('[data-param="mode"]');
    if (!mode) return;
    const family = String(checkpointEntry?.family || '').toLowerCase();
    // The typed modes run their own Z-Image graphs (FLUX.1 until 2026-09-23).
    const zimageOnly = new Set(['z_depth', 't_pose', 'open_pose', 'inpaint']);
    Array.from(mode.options).forEach(option => {
      if (zimageOnly.has(option.value)) {
        option.disabled = !!family && family !== 'zimage';
        option.title = option.disabled ? 'Z-Image checkpoints only' : '';
      }
    });
    // Keep a restored legacy choice visible even when disabled. Submission
    // validation will explain why it cannot run; silently changing it to Plain
    // would alter the user's graph.
    if (mode.selectedOptions[0]?.disabled) {
      samplingNote(mode, 'Unavailable for the selected checkpoint family');
    } else samplingNote(mode, '');
  }

  function serviceNodeHtml(entry) {
    const inputs = entry.inputs || [];
    const outputs = entry.outputs || [];
    const params = entry.params_array || [];
    const slow = entry.slow ? '<em class="nslow">minutes</em>' : '';
    return `
      <div class="nhead"><b>${escapeHtml(entry.title)}</b>${slow}</div>
      <div class="nports">
        ${inputs.map(inputRowHtml).join('')}
        ${outputs.map(item => `<div class="prow pout">${escapeHtml(item.title || item.field)} ${typeIcon(item.type)}</div>`).join('')}
      </div>
      ${params.length ? `<details class="nparams"><summary>Settings</summary>${params.map(paramControl).join('')}</details>` : ''}
      <div class="nrec"></div>
      <div class="nstate"></div>
      <div class="nprog task-prog"></div>
      <div class="nout"></div>`;
  }

  /* --------------------------------------------------- reference sockets */

  // A multi-reference node (FLUX.2 klein, Qwen-Image-Edit) numbers its
  // picture sockets 1..N because the prompt counts them ("the jacket from
  // image 2"). Socket 1 is the node's ordinary `image`; the rest appear one at
  // a time as the previous one is wired. Each also takes a video, which the
  // server reads as its first frame.
  const REFERENCE_FIELD = /^reference_(\d+)$/;
  const MULTIREF_CHECKPOINT = 'flux-2-klein-4b.safetensors';

  function inputRowHtml(item) {
    const video = (item.also_accepts || []).includes('video');
    if (item.ref_index) {
      const tip = `Image ${item.ref_index}` + (video ? ' — picture, or a video (its first frame)' : '');
      return `<div class="prow pin pref" data-ref="${item.ref_index}" title="${escapeAttr(tip)}">`
        + `${typeIcon(item.type)}<b class="refn">${item.ref_index}</b>`
        + (video ? `<span class="refvid" aria-label="video: first frame">${typeIcon('video')}</span>` : '')
        + '</div>';
    }
    return `<div class="prow pin">${typeIcon(item.type)} ${escapeHtml(item.title || item.field)}${item.required ? '' : ' <i>optional</i>'}</div>`;
  }

  function referenceIndex(field) {
    if (field === 'image') return 1;
    const match = REFERENCE_FIELD.exec(String(field || ''));
    return match ? parseInt(match[1], 10) : 0;
  }

  /** Which reference sockets to show: every wired one, and the next free one. */
  function visibleReferenceSockets(entries, connected) {
    let last = 0;
    entries.forEach(item => {
      if (item.ref_index && connected[item.field]) last = Math.max(last, item.ref_index);
    });
    const shown = {};
    entries.forEach(item => {
      if (!item.ref_index) return;
      shown[item.field] = item.ref_index <= 1 || !!connected[item.field] || item.ref_index <= last + 1;
    });
    return shown;
  }

  /** Extra reference fields in socket order, for the request's list. */
  function referenceList(resolved) {
    return Object.keys(resolved || {})
      .map(field => ({field, index: referenceIndex(field)}))
      .filter(item => item.index >= 2 && resolved[item.field])
      .sort((a, b) => a.index - b.index)
      .map(item => resolved[item.field]);
  }

  function refreshReferenceSockets(id) {
    const node = meta(id);
    const element = nodeElement(id);
    if (!node || node.kind !== KIND_SERVICE || !element) return;
    const entries = (serviceById(node.service) || {}).inputs || [];
    if (!entries.some(item => item.ref_index >= 2)) return;
    const data = editor.getNodeFromId(id);
    const connected = {};
    node.inFields.forEach((field, index) => {
      connected[field] = !!(((data && data.inputs || {})['input_' + (index + 1)] || {}).connections || []).length;
    });
    const shown = visibleReferenceSockets(entries, connected);
    const rows = element.querySelectorAll('.nports .prow.pin');
    let visible = 0;
    node.inFields.forEach((field, index) => {
      const entry = entries.find(item => item.field === field) || {};
      const show = entry.ref_index ? shown[field] !== false : true;
      const port = element.querySelector('.inputs .input_' + (index + 1));
      if (port) {
        port.style.display = show ? '' : 'none';
        if ((entry.also_accepts || []).includes('video')) {
          port.classList.add('accepts-video');
          port.title = `Image ${entry.ref_index || ''} · picture or video (first frame)`.replace('  ', ' ');
        }
      }
      if (rows[index]) rows[index].style.display = show ? '' : 'none';
      if (show) visible += 1;
    });
    alignPorts(id, visible, node.outFields.length);
    try { editor.updateConnectionNodes('node-' + id); } catch (error) { /* not drawn yet */ }
  }

  /**
   * A second picture wired into an Image node needs a model that composes
   * several: FLUX.2 klein. Switching for the person is kinder than refusing
   * the wire, and the toast says what changed.
   */
  function ensureMultiReferenceModel(id, inField) {
    const node = meta(id);
    if (!node || node.service !== 'image' || referenceIndex(inField) < 2) return;
    const element = nodeElement(id);
    const hidden = element && element.querySelector('[data-param="checkpoint"]');
    if (!hidden) return;
    const picker = element.querySelector('[data-model-param="checkpoint"]')?._picker;
    const family = picker && picker.entry && picker.entry.family;
    if (family === 'flux2' || (!family && /klein/i.test(hidden.value || ''))) return;
    if (picker) picker.value = MULTIREF_CHECKPOINT;
    const entry = picker && picker.entry;
    if (entry) {
      applySamplingPolicy(id, entry.sampling_policy_object || entry.sampling_policy || {});
      refreshModeOptions(id, entry);
    }
    hidden.value = MULTIREF_CHECKPOINT;
    hidden.dispatchEvent(new Event('change', {bubbles:true}));
    toast('Several pictures → FLUX.2 klein 4B');
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
    const parameterNames = new Set((entry.params_array || []).map(item => item.name));
    const hasDimensions = parameterNames.has('width') && parameterNames.has('height');
    const id = editor.addNode(
      serviceId, inputs.length, outputs.length, x, y,
      'ainode svc-' + serviceId, { service: serviceId }, serviceNodeHtml(Object.assign({}, entry, {title: (params && params._label) || entry.title}))
    );
    setMeta(id, {
      kind: KIND_SERVICE,
      service: serviceId,
      displayMode: params && params._display_mode,
      label: (params && params._label) || '',
      followInputSize: hasDimensions ? (!params || params._follow_input_size !== false) : undefined,
      disabled: !!(params && params._disabled),
      // A node that can carry a standing instruction is born with the default
      // one, so a graph saved before this existed opens with it too.
      systemPrompt: entry.system_prompt_capable
        ? (params && typeof params._system_prompt === 'string'
          ? params._system_prompt : String(entry.system_prompt_default || ''))
        : undefined,
      inFields: inputs.map(i => i.field),
      outFields: outputs.map(o => o.field)
    });
    applySystemPromptMarker(id);
    alignPorts(id, inputs.length, outputs.length);
    refreshReferenceSockets(id);
    mountModelPickers(id, serviceId);
    if (params) applyParams(id, params);
    if (params && params._disabled) applyBypass(id, true);
    materializeDefaultModel(id);
    return id;
  }

  function addInputNode(entityType, x, y, value, params) {
    if (!(catalogue.entity_types_array || []).some(item => item.id === entityType)) return null;
    const id = editor.addNode(
      'input-' + entityType, 0, 1, x, y,
      'ainode input-node', { entity_type: entityType }, inputNodeHtml(entityType)
    );
    setMeta(id, { kind: KIND_INPUT, entityType: entityType, displayMode: params && params._display_mode,
                  disabled: !!(params && params._disabled), inFields: [], outFields: ['value'] });
    alignPorts(id, 0, 1);
    if (params && params._disabled) applyBypass(id, true);
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
        onReady: (value, entry) => {
          if (name === 'checkpoint' && entry) {
            applySamplingPolicy(id, entry.sampling_policy_object || entry.sampling_policy || {});
            refreshModeOptions(id, entry);
            const pending = applyRecommended(id, entry, { annotateOnly: true });
            pendingModelSelections.set(String(id), pending);
            pending.finally(() => {
              if (pendingModelSelections.get(String(id)) === pending) pendingModelSelections.delete(String(id));
            });
          }
        },
        onChange: (value, entry, reason) => {
          if (name === 'checkpoint' && value && !modelAcceptsConnectedControls(id, entry)) {
            picker.value = hidden ? hidden.value : '';
            if (!reason?.materialized) toastControlRefusal(connectedControlChannel(id) || 'control', serviceId);
            return;
          }
          if (hidden) hidden.value = value;
          if (reason && reason.materialized) return;
          if (name === 'checkpoint') {
            applySamplingPolicy(id, entry?.sampling_policy_object || entry?.sampling_policy || {});
            refreshModeOptions(id, entry);
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
   * example images, which is what the author actually ran. Runtime sampling
   * policy remains authoritative: Auto stays the zero sentinel and fixed
   * workflow knobs are shown read-only instead of impersonating author input.
   *
   * A value the person has already changed by hand is left alone. Choosing a
   * model should not quietly undo a decision they made.
   */
  const RECOMMENDED_TO_PARAM = {
    steps: 'steps', strength: 'lora_strength'
  };

  function annotateAutoSampling(id, effective, policy) {
    const element = nodeElement(id);
    if (!element) return [];
    const notes = [];
    [['cfg', 'CFG'], ['sampler', 'Sampler'], ['scheduler', 'Scheduler']].forEach(([name, title]) => {
      const control = element.querySelector('[data-param="' + name + '"]');
      if (!control || control.disabled || ![0, '0', ''].includes(control.value)) return;
      const value = effective && effective[name];
      if (value === undefined || value === null || value === '') return;
      const option = control.tagName === 'SELECT'
        ? Array.from(control.options).find(item => String(item.value) === String(value))
        : null;
      const shown = option ? option.textContent : value;
      samplingNote(control, 'Auto (' + shown + ')');
      notes.push(title + ' Auto ' + shown);
    });
    return notes;
  }

  async function applyRecommended(id, entry, options) {
    options = options || {};
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
    let policyApplied = [];
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
      const samplingPolicy = data.sampling_policy_object || {};
      policyApplied = applySamplingPolicy(id, samplingPolicy);
      policyApplied = policyApplied.concat(annotateAutoSampling(id, effective, samplingPolicy));
      entry = { recommended: effective };
    } catch (error) {
      const note = element.querySelector('.nrec');
      if (note) note.textContent = error.message;
      return;
    }
    const applied = [];
    Object.keys(entry.recommended).forEach(key => {
      if (options.annotateOnly) return;
      const name = RECOMMENDED_TO_PARAM[key] || ({cfg:'cfg', sampler:'sampler', scheduler:'scheduler', lora_strength:'lora_strength'})[key];
      if (!name) return;
      // Sampling values remain Auto sentinels or the person's explicit saved
      // choices. Effective backend values describe what Auto will resolve to;
      // materialising them here would turn Auto into a stale manual override.
      if (['steps', 'cfg', 'sampler', 'scheduler'].includes(name)) return;
      const control = element.querySelector('[data-param="' + CSS.escape(name) + '"]');
      if (!control || control.dataset.touched === 'yes') return;
      const value = entry.recommended[key];
      // A select only takes a value it actually offers.
      if (control.tagName === 'SELECT' &&
          ![...control.options].some(o => String(o.value) === String(value))) return;
      control.value = value;
      const readout = element.querySelector('[data-for="' + CSS.escape(name) + '"]');
      if (readout) updateSamplingReadout(control);
      applied.push(name + ' ' + value);
    });
    const note = element.querySelector('.nrec');
    if (note) {
      note.textContent = applied.length || policyApplied.length
        ? 'Applied settings: ' + policyApplied.concat(applied).join(', ')
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

  /* ------------------------------------------ how many can run it right now */

  /**
   * The number in the top-right of a node's header.
   *
   * It is how many computers could take this job this moment, and it comes
   * from the fleet's own `capacity_object`, which is computed from the same
   * rules as the support matrix on /models: a box has to be reachable, carry
   * the template, and — once a checkpoint is chosen — carry that model file.
   * Choosing a video model is exactly the case this exists for: the same node
   * goes from four computers to one the moment a 22B checkpoint is picked,
   * and nothing on the node used to say so.
   *
   * A service with no farm behind it (frame extraction runs on this host)
   * gets no badge at all rather than a "1" that means something else.
   */
  let fleetCapacity = null;
  let capacityPaintTimer = 0;

  function capacityFor(id) {
    const item = meta(id);
    if (!fleetCapacity || !item || item.kind !== KIND_SERVICE) return null;
    const entry = fleetCapacity[item.service];
    if (!entry || entry.kind_string === 'local') return null;
    const element = nodeElement(id);
    const picker = element && element.querySelector('[data-param="checkpoint"]');
    const chosen = picker ? String(picker.value || '') : '';
    const forModel = chosen && (entry.checkpoints_object || {})[chosen];
    return forModel ? Object.assign({checkpoint: chosen}, forModel) : entry;
  }

  function capacityTitle(entry) {
    const model = entry.checkpoint ? ' with ' + entry.checkpoint : '';
    const total = Number(entry.total_int) || 0;
    if (!total) return 'No computer can run this right now' + model + '.';
    return total + ' computer' + (total === 1 ? '' : 's') + ' can run this now' +
      model + ': ' + (entry.computers_array || []).join(', ') + '; ' +
      (Number(entry.idle_int) || 0) + ' idle';
  }

  function applyCapacityBadge(id) {
    const element = nodeElement(id);
    const head = element && element.querySelector('.nhead');
    if (!head) return false;
    const entry = capacityFor(id);
    let tag = head.querySelector('.ncap');
    if (!entry) { if (tag) tag.remove(); return false; }
    if (!tag) {
      tag = document.createElement('em');
      tag.className = 'ncap';
      head.appendChild(tag);
    }
    const total = Number(entry.total_int) || 0;
    tag.textContent = String(total);
    tag.title = capacityTitle(entry);
    tag.classList.toggle('none', !total);
    tag.classList.toggle('free', total > 0 && (Number(entry.idle_int) || 0) > 0);
    return true;
  }

  function paintCapacityBadges() {
    nodeMeta.forEach((item, id) => {
      if (item && item.kind === KIND_SERVICE) applyCapacityBadge(id);
    });
  }

  function refreshCapacity() {
    if (!window.AIEntities || !window.AIEntities.getFleet) return Promise.resolve();
    return window.AIEntities.getFleet().then(data => {
      fleetCapacity = (data && data.capacity_object) || null;
      paintCapacityBadges();
    }).catch(() => {});
  }

  function startCapacityBadges() {
    refreshCapacity();
    // The fleet strip already polls this and the answer is cached for five
    // seconds, so riding along costs nothing on the wire.
    setInterval(() => { if (!document.hidden) refreshCapacity(); }, 5000);
    // A checkpoint is what moves a node from one set of computers to another,
    // so the number follows the picker rather than the clock.
    document.addEventListener('change', event => {
      const target = event.target;
      if (!target || !target.dataset || target.dataset.param !== 'checkpoint') return;
      const node = target.closest && target.closest('.ainode');
      if (node && node.id) applyCapacityBadge(node.id.replace(/^node-/, ''));
    }, true);
    // A node added, pasted or restored has a header the moment it is on the
    // canvas; painting on the next frame beats waiting for the next poll.
    const stage = document.getElementById('canvas');
    if (stage && window.MutationObserver) {
      new MutationObserver(() => {
        clearTimeout(capacityPaintTimer);
        capacityPaintTimer = setTimeout(paintCapacityBadges, 180);
      }).observe(stage, {childList: true, subtree: true});
    }
  }

  /**
   * The socket on a node that hands out a picture, if it has one.
   *
   * Only a plain `image` counts. A ControlNet node publishes a pose or a depth
   * map, which no image input accepts, so offering it the picture functions
   * would only build wires the editor immediately refuses.
   */
  function imageOutputField(id) {
    const node = meta(id);
    if (!node) return '';
    if (node.kind === KIND_INPUT) return node.entityType === 'image' ? 'value' : '';
    const entry = serviceById(node.service);
    const output = ((entry && entry.outputs) || []).find(item => item.type === 'image');
    return output ? output.field : '';
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
          const existingOption = Array.from(select.options).find(item => item.value === pinned);
          if (existingOption) existingOption.textContent = avatar.display_name + ' · v' + avatar.version;
          else select.add(new Option(avatar.display_name + ' · v' + avatar.version, pinned));
          select.value = pinned;
          const ref = avatar.references.find(item => item.media_type === 'image' && item.role === 'face') || avatar.references.find(item => item.media_type === 'image');
          preview.hidden = !ref;
          if (ref) preview.src = ref.canonical_url;
          markResolution(element.querySelector('.ninput'), preview);
          state.textContent = 'Saved character · version ' + avatar.version;
          state.className = 'nstate done';
        } catch (error) { state.textContent = error.message; state.className = 'nstate failed'; }
      };
      select.addEventListener('change', update);
      preview.addEventListener('click', () => { if (!preview.hidden) openPreview('image', preview.src); });
      fetch('/api/ai/avatars').then(response => response.json()).then(data => {
        for (const item of data.avatars_array || []) {
          const key = item.avatar_id + '@' + item.current_version;
          const existingOption = Array.from(select.options).find(option => option.value === key);
          if (existingOption) existingOption.textContent = item.display_name + ' · v' + item.current_version;
          else select.add(new Option(item.display_name + ' · v' + item.current_version, key));
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
    const previewHost = element.querySelector('.ninput');
    markResolution(previewHost, preview);
    if (/^(https?:\/\/|data:image\/)/.test(text.value.trim())) {
      preview.src = text.value.trim();
      preview.hidden = false;
      if (isVideo) preview.play().catch(() => {});
      markResolution(previewHost, preview);
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
        markResolution(previewHost, preview);
        preview.play().catch(() => {});
      } else {
        const reader = new FileReader();
        reader.onload = () => {
          if (element._uploadGeneration !== generation) return;
          preview.src = reader.result;
          preview.hidden = false;
          markResolution(previewHost, preview);
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
          text.dispatchEvent(new Event('change', {bubbles:true}));
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
    let previewTimer = null;
    const refreshInputPreview = () => {
      clearTimeout(previewTimer);
      if (/^https?:\/\//.test(text.value.trim())) {
        preview.src = text.value.trim();
        preview.hidden = false;
        if (isVideo) preview.play().catch(() => {});
      } else {
        if (isVideo) preview.pause();
        preview.removeAttribute('src');
        preview.hidden = true;
      }
    };
    text.addEventListener('change', refreshInputPreview);
    text.addEventListener('input', () => {
      clearTimeout(previewTimer);
      previewTimer = setTimeout(refreshInputPreview, 180);
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
      if (readout) updateSamplingReadout(control);
    });
  }

  const loraMenuCache = new Map();

  function fillLoraStackMenu(select) {
    const node = select.closest('.drawflow-node');
    const service = node ? (meta(node.id.replace(/^node-/, '')) || {}).service : '';
    select.dataset.filled = '1';
    if (!loraMenuCache.has(service)) {
      loraMenuCache.set(service, fetch('/api/ai/model-catalogue?service=' + encodeURIComponent(service || 'image'))
        .then(r => r.json()).then(body => (body.loras_array || []).filter(entry => entry.usable))
        .catch(() => []));
    }
    loraMenuCache.get(service).then(loras => {
      loras.forEach(entry => {
        const stem = String(entry.file || '').replace(/\.[^.]+$/, '');
        const weight = (entry.recommended || {}).strength || 1;
        const option = new Option(`${entry.title || stem} · ${entry.base || entry.family || ''}`,
                                  `<lora:${stem}:${weight}>`);
        option.title = entry.file;
        select.add(option);
      });
    });
  }

  function readParams(id) {
    const element = nodeElement(id);
    const values = {};
    if (meta(id)?.label) values._label = meta(id).label;
    if (meta(id)?.displayMode) values._display_mode = meta(id).displayMode;
    if (typeof meta(id)?.followInputSize === 'boolean') {
      values._follow_input_size = meta(id).followInputSize;
    }
    if (meta(id)?.disabled) values._disabled = true;
    if (systemPromptService(id)) values._system_prompt = systemPromptOf(id);
    if (!element) return values;
    element.querySelectorAll('[data-param]').forEach(control => {
      const raw = control.value;
      values[control.dataset.param] =
        (control.type === 'range' || control.type === 'number') ? Number(raw) : raw;
    });
    return values;
  }

  /* ------------------------------------------------------------- bypassing */

  /**
   * A node that stays on the canvas but is not run.
   *
   * Deleting a step to try the composition without it costs the wiring, and
   * the wiring is the expensive part to rebuild. Bypass keeps the node, its
   * settings and every wire attached to it, and takes it out of the run: the
   * node is dimmed, and everything downstream of it is reported as skipped
   * rather than started and failed, because a step whose input never arrives
   * did not fail — it was never asked.
   *
   * The flag rides in `params._disabled` so it goes wherever the node goes:
   * saved links, copy and paste, and the graph agent's edits.
   */
  function isBypassed(id) {
    const item = meta(id);
    return !!(item && item.disabled);
  }

  function applyBypass(id, disabled) {
    const item = meta(id);
    const element = nodeElement(id);
    if (!item || !element) return false;
    item.disabled = !!disabled;
    element.classList.toggle('bypassed', !!disabled);
    // Drawflow wraps the node body in `.drawflow_content_node`, so the header
    // is a descendant of the node element rather than a child of it.
    const head = element.querySelector('.nhead');
    let tag = head && head.querySelector('.nbypass');
    if (disabled && head && !tag) {
      tag = document.createElement('em');
      tag.className = 'nbypass';
      tag.textContent = 'bypassed';
      const heading = head.querySelector('b');
      head.insertBefore(tag, heading ? heading.nextSibling : head.firstChild);
    }
    if (!disabled && tag) tag.remove();
    return true;
  }

  /** Ctrl+P and the context menu both land here. */
  function toggleBypass(ids) {
    const list = [...new Set((ids || []).map(String))].filter(id => meta(id) && nodeElement(id));
    if (!list.length) {
      toast('Select a node first — Ctrl+P then takes it out of the run.');
      return false;
    }
    // A mixed selection is bypassed as a whole; a fully bypassed one comes back.
    const enable = list.every(isBypassed);
    list.forEach(id => { applyBypass(id, !enable); invalidateNodeAndDownstream(id); });
    const count = list.length + (list.length === 1 ? ' node' : ' nodes');
    toast(enable
      ? count + ' back in the run.'
      : count + ' bypassed — Render will skip it and everything that needs it.');
    return true;
  }

  /* ------------------------------------------------------- system prompts */

  /**
   * The standing instruction a node gives its model.
   *
   * It is not a question about this picture, it is how this node is to answer
   * every question: plain text, English, prompt only. Kept on the node rather
   * than in a global setting because two Vision nodes in the same graph
   * routinely want different answers — one writing an image prompt, one
   * describing a photo for a person to read.
   *
   * It rides in `params._system_prompt`, so it survives saving, reloading,
   * duplicating, copy-and-paste and the graph agent's edits. It is never part
   * of the node's output: what the next node reads is the answer alone.
   */
  function systemPromptService(id) {
    const item = meta(id);
    if (!item || item.kind !== KIND_SERVICE) return null;
    const entry = serviceById(item.service);
    return entry && entry.system_prompt_capable ? entry : null;
  }

  function defaultSystemPrompt(id) {
    const entry = systemPromptService(id);
    return entry ? String(entry.system_prompt_default || '') : '';
  }

  function systemPromptOf(id) {
    const item = meta(id);
    if (!item || !systemPromptService(id)) return '';
    return typeof item.systemPrompt === 'string'
      ? item.systemPrompt : defaultSystemPrompt(id);
  }

  /** A quiet "S" on the header when this node no longer says the usual thing. */
  function applySystemPromptMarker(id) {
    const element = nodeElement(id);
    if (!element || !systemPromptService(id)) return;
    const head = element.querySelector('.nhead');
    if (!head) return;
    const text = systemPromptOf(id);
    const custom = text.trim() !== defaultSystemPrompt(id).trim();
    let tag = head.querySelector('.nsysprompt');
    if (custom && !tag) {
      tag = document.createElement('em');
      tag.className = 'nsysprompt';
      tag.textContent = 'S';
      const heading = head.querySelector('b');
      head.insertBefore(tag, heading ? heading.nextSibling : head.firstChild);
    }
    if (!custom && tag) { tag.remove(); return; }
    if (tag) {
      const preview = text.trim().slice(0, 80);
      tag.title = 'Custom system prompt: ' + preview + (text.trim().length > 80 ? '…' : '');
    }
  }

  function setSystemPrompt(id, text) {
    const item = meta(id);
    if (!item || !systemPromptService(id)) return false;
    const next = String(text == null ? '' : text);
    if (systemPromptOf(id) === next) return false;
    item.systemPrompt = next;
    applySystemPromptMarker(id);
    // A standing instruction is part of the request, so an answer produced
    // under the old one is no longer this node's answer.
    invalidateNodeAndDownstream(id);
    return true;
  }

  /** The nodes in a selection this dialog can act on. */
  function systemPromptTargets(ids) {
    return [...new Set((ids || []).map(String))].filter(id => systemPromptService(id));
  }

  function openSystemPromptEditor(ids) {
    const targets = systemPromptTargets(ids);
    if (!targets.length) {
      toast('Only Vision and Text nodes carry a system prompt.');
      return false;
    }
    const first = targets[0];
    const fallback = defaultSystemPrompt(first);
    let dialog = document.getElementById('system-prompt-editor');
    if (!dialog) {
      dialog = document.createElement('dialog');
      dialog.id = 'system-prompt-editor';
      dialog.setAttribute('aria-label', 'System prompt');
      document.body.appendChild(dialog);
    }
    dialog.innerHTML = '';
    const form = document.createElement('form');
    form.method = 'dialog';
    const title = document.createElement('strong');
    title.textContent = targets.length === 1
      ? 'System prompt · ' + (serviceById(meta(first).service) || {}).title
      : 'System prompt · ' + targets.length + ' nodes';
    const help = document.createElement('p');
    help.className = 'sysprompt-help';
    help.textContent = 'Standing instructions for the model. It is never part '
      + 'of the answer this node passes on.';
    const area = document.createElement('textarea');
    area.rows = 7;
    area.value = systemPromptOf(first);
    area.setAttribute('aria-label', 'System prompt text');
    const row = document.createElement('div');
    row.className = 'sysprompt-buttons';
    const reset = document.createElement('button');
    reset.type = 'button';
    reset.className = 'sysprompt-reset';
    reset.textContent = 'Reset to default';
    reset.addEventListener('click', () => { area.value = fallback; area.focus(); });
    const cancel = document.createElement('button');
    cancel.type = 'button';
    cancel.textContent = 'Cancel';
    cancel.addEventListener('click', () => dialog.close());
    const save = document.createElement('button');
    save.type = 'button';
    save.className = 'sysprompt-save';
    save.textContent = 'Save';
    save.addEventListener('click', () => {
      const changed = targets.filter(id => setSystemPrompt(id, area.value)).length;
      dialog.close();
      if (changed) {
        toast(changed === 1 ? 'System prompt updated.'
          : changed + ' nodes now share this system prompt.');
      }
    });
    row.append(reset, cancel, save);
    form.append(title, help, area, row);
    dialog.appendChild(form);
    dialog.showModal();
    area.focus();
    area.setSelectionRange(area.value.length, area.value.length);
    return true;
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
    const input = serviceById(to.service).inputs.find(i => i.field === inField) || {};
    const accepted = input.type;
    const alsoAccepts = input.also_accepts || [];
    return { outField, inField, produced, accepted, alsoAccepts };
  }

  /**
   * What each socket of a node carries, for the typed socket colours and the
   * drag highlighting (ai-node-sockets.js).
   */
  function socketTypes(id) {
    const node = meta(id);
    if (!node) return null;
    if (node.kind === KIND_INPUT) return {inputs: [], outputs: [node.entityType]};
    const entry = serviceById(node.service) || {};
    return {
      inputs: node.inFields.map(field => {
        const item = (entry.inputs || []).find(value => value.field === field) || {};
        return {type: item.type || '', also: item.also_accepts || [], title: item.title || field};
      }),
      outputs: node.outFields.map(field =>
        ((entry.outputs || []).find(value => value.field === field) || {}).type || '')
    };
  }

  /**
   * Whether a wire would be kept: the same checks onConnectionCreated makes
   * after the fact (type or also_accepts, then the ControlNet family rule),
   * asked before the drop so incompatible sockets can grey out.
   */
  function linkAllowed(connection) {
    if (String(connection.output_id) === String(connection.input_id)) return false;
    if ((meta(connection.input_id) || {}).kind !== KIND_SERVICE) return false;
    const info = linkTypes(connection);
    if (!info || !info.produced) return false;
    if (!(info.produced === info.accepted || info.alsoAccepts.includes(info.produced))) return false;
    if (info.produced.startsWith('control_')) {
      const element = nodeElement(connection.input_id);
      const slot = element && element.querySelector('[data-model-param="checkpoint"]');
      const entry = slot && slot._picker && slot._picker.entry;
      const sourceService = serviceById((meta(connection.output_id) || {}).service || info.produced);
      return controlChannelAccepted(info.produced.slice(8), entry, sourceService);
    }
    return true;
  }

  function onConnectionCreated(connection) {
    const info = linkTypes(connection);
    if (info && (info.produced === info.accepted || (info.produced && info.alsoAccepts.includes(info.produced)))) {
      if (info.produced.startsWith('control_')) {
        const channel = info.produced.slice(8);
        const element = nodeElement(connection.input_id);
        const slot = element && element.querySelector('[data-model-param="checkpoint"]');
        const entry = slot && slot._picker && slot._picker.entry;
        const node = editor.getNodeFromId(connection.input_id);
        const targetMeta = meta(connection.input_id);
        const sourceService = serviceById((meta(connection.output_id) || {}).service || info.produced);
        const controlLinks = targetMeta.inFields.reduce((count, field, index) =>
          count + (field.startsWith('control_') ? ((node.inputs['input_' + (index + 1)] || {}).connections || []).length : 0), 0);
        const accepted = controlChannelAccepted(channel, entry, sourceService);
        if (!accepted || controlLinks > 1) {
          editor.removeSingleConnection(connection.output_id, connection.input_id, connection.output_class, connection.input_class);
          if (controlLinks > 1) toast('Use a separate Image node for each ControlNet channel.');
          else toastControlRefusal(channel, targetMeta.service);
          return;
        }
      }
      replaceOlderInput(connection);
      ensureMultiReferenceModel(connection.input_id, info.inField);
      refreshReferenceSockets(connection.input_id);
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
    // One job, many answers: the Avatar plus every view it drew (ai_avatar_build).
    avatar_build: { api: '/api/ai/avatar-build', finish: pollAvatarBuild, field: 'avatar_string', type: 'avatar', multi: true },
    avatar_image: { api: '/api/ai/avatar-image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    avatar_video: { api: '/api/ai/avatar-video', finish: pollForFile, field: 'video_url_string', type: 'video' },
    video_frame: { api: '/api/ai/video-reference', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video_storyboard: { api: '/api/ai/video-reference', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video_control: { api: '/api/video', finish: pollForFile, field: 'video_url_string', type: 'video' },
    vision: { api: '/api/vision', finish: pollAiStatus, field: 'answer_string', type: 'text' },
    text: { api: '/api/text2text', finish: pollAiStatus, field: 'answer_string', type: 'text' },
    image: { api: '/api/image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video: { api: '/api/video', finish: pollForFile, field: 'video_url_string', type: 'video' },
    // Enhancement: a picture in, the same picture out, published at a URL the
    // farm fills in later — exactly the ControlNet shape.
    upscale: { api: '/api/upscale', finish: pollForFile, field: 'image_url_string', type: 'image' },
    detail_enhance: { api: '/api/detail', finish: pollForFile, field: 'image_url_string', type: 'image' },
    face_fix: { api: '/api/facefix', finish: pollForFile, field: 'image_url_string', type: 'image' },
    // Draws or rewrites depending on whether a picture is wired in; the
    // endpoint reads the wiring, so the runner is the ordinary picture shape.
    qwen_image: { api: '/api/qwen-image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    '3dmodel': { api: '/api/3dmodel', finish: poll3dStatus, field: 'model_url_string', type: 'model3d' }
  };
  ['pose', 'depth', 'canny'].forEach(channel => {
    RUNNERS['control_' + channel] = { api: '/api/controlnet', finish: pollForFile,
      field: 'image_url_string', type: 'control_' + channel };
  });

  function runnerFor(serviceId) {
    const runner = RUNNERS[serviceId];
    if (!runner) {
      throw new Error('This service was updated after this page loaded. Reload the page and try again.');
    }
    return runner;
  }

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

  /**
   * One status line per node.
   *
   * The progress tracker (AIEntities.startTask) writes its own caption under
   * the bar — "f12 · ~1m 29s estimated", "Queued · f12 · 2nd · ~3m" — which
   * repeated what the node's state line already said. The caption is hidden
   * on this page and folded into the state line instead:
   * "rendering · f12 · ~1m 29s".
   */
  function compactStatus(etaText, phase) {
    const text = String(etaText || '').trim();
    if (!text) return '';
    if (/^queued\b/i.test(text)) return 'queued' + text.slice(6);
    const parts = text.split(' · ')
      .map(part => part.replace(/\s+estimated$/, '').trim())
      .filter(part => part && part.toLowerCase() !== 'rendering');
    return [phase || 'rendering'].concat(parts).join(' · ');
  }

  function syncStatusLine(node) {
    if (!node || !node.querySelector) return;
    const state = node.querySelector('.nstate');
    const eta = node.querySelector('.nprog .task-eta');
    const bar = eta && eta.closest('.nprog');
    if (!state || !bar || !(bar.classList.contains('running') || bar.classList.contains('queued'))) return;
    const text = compactStatus(eta.textContent, state.dataset.phase);
    if (text && state.textContent !== text) state.textContent = text;
  }

  /**
   * Keep the state line in step with the tracker, and carry the (hidden)
   * applied-settings note as the state line's tooltip.
   */
  function installStatusLines(canvas) {
    if (!canvas || typeof MutationObserver === 'undefined') return;
    const pending = new Set();
    let queued = false;
    const flush = () => {
      queued = false;
      pending.forEach(node => {
        syncStatusLine(node);
        const note = node.querySelector('.nrec');
        const state = node.querySelector('.nstate');
        if (note && state) state.title = note.textContent || '';
      });
      pending.clear();
    };
    new MutationObserver(records => {
      for (const record of records) {
        const target = record.target.nodeType === 1 ? record.target : record.target.parentElement;
        if (!target || !target.closest) continue;
        const holder = target.closest('.task-eta, .nrec');
        const node = holder && holder.closest('.drawflow-node');
        if (node) pending.add(node);
      }
      if (pending.size && !queued) { queued = true; setTimeout(flush, 60); }
    }).observe(canvas, {subtree: true, childList: true, characterData: true});
  }

  function taskStateReporter(element, tracker, accepted, execution) {
    return data => {
      if (execution && !executionIsCurrent(execution)) return;
      const status = String(data.status_string || data.status || 'queued').toLowerCase();
      const worker = data.node_string || data.render_server_name || accepted.node_string ||
        (String(accepted.task_id_string || '').includes('.') ? accepted.task_id_string.split('.')[0] : '');
      const active = ['rendering', 'processing', 'running', 'in_progress', 'generating', 'converting'].includes(status);
      const phase = data.stage_string || data.stage || (active ? 'rendering' : 'queued');
      // "waiting for a worker" is what we used to say because it was all we
      // knew; the farm now says where in the line the job actually is.
      const place = !active && window.AIEntities
        ? window.AIEntities.queuePlaceLabel(data.queue_position_int, data.queue_length_int) : '';
      element.textContent = (active ? phase : status === 'completed' ? 'completed' : 'queued') +
        (place ? ' · ' + place : '') +
        (worker ? ' · ' + worker : place ? '' : ' — waiting for a worker');
      element.dataset.phase = active ? phase : 'queued';
      if (tracker && tracker.setState) tracker.setState({active, worker, phase,
        startedAt: data.started_at_unix_float || 0,
        queuePosition: data.queue_position_int, queueLength: data.queue_length_int});
      syncStatusLine(element.closest && element.closest('.drawflow-node'));
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

  /**
   * A node with several outputs finishes with {value, outputs}: `value` is its
   * first output (what a single-socket reader gets), `outputs` every output by
   * field. Everything else in the page still sees a plain value.
   */
  function splitMulti(finished) {
    if (finished && typeof finished === 'object' && !Array.isArray(finished) &&
        Object.prototype.hasOwnProperty.call(finished, 'outputs')) {
      return {value: finished.value, outputs: finished.outputs || null};
    }
    return {value: finished, outputs: null};
  }

  /** What a wire carries: the named output when the node kept them apart. */
  function outputValue(upstream, field) {
    const outputs = upstream && upstream.outputs;
    if (outputs && field && Object.prototype.hasOwnProperty.call(outputs, field)) return outputs[field];
    return upstream ? upstream.value : '';
  }

  const AVATAR_BUILD_FIELDS = ['avatar_string', 'front_url_string', 'face_closeup_url_string',
    'full_body_url_string', 'three_quarter_left_url_string', 'three_quarter_right_url_string',
    'profile_left_url_string', 'profile_right_url_string', 'back_url_string', 'sheet_url_string',
    'source_frame_url_string', 'description_string'];

  function avatarBuildOutputs(data) {
    const outputs = {};
    AVATAR_BUILD_FIELDS.forEach(field => { if (data && data[field]) outputs[field] = String(data[field]); });
    return outputs;
  }

  async function pollAvatarBuild(accepted, runner, report) {
    const id = String(accepted.task_id_string || '');
    if (!/^avb_[a-f0-9]{24}$/.test(id)) throw new Error('the Avatar builder returned no job');
    let data = accepted;
    for (let attempt = 0; attempt < 1500; attempt++) {
      if (data && data.finished_bool) {
        if (data.success_bool === false || String(data.status_string) === 'failed') {
          throw new Error(data.error_string || 'the Avatar build failed');
        }
        if (!data.avatar_string) throw new Error('the Avatar build finished without saving');
        return {value: data.avatar_string, outputs: avatarBuildOutputs(data)};
      }
      await sleep(Math.max(2, Math.min(10, Number(data && data.retry_after_seconds_float) || 4)) * 1000);
      const response = await fetch('/api/ai/avatar-build/status/' + encodeURIComponent(id)).catch(() => null);
      if (!response || response.status >= 500) continue;
      const parsed = await readJsonResponse(response);
      if (!response.ok) {
        if (parsed.data) throw new Error(describeError(parsed.data, response.status));
        throw new Error('HTTP ' + response.status);
      }
      data = parsed.data || {};
      if (report && !data.finished_bool) {
        const views = Object.keys(data.views_object || {}).length;
        report({status_string: 'running', node_string: 'avatar',
                stage_string: (data.stage_string || 'building') + (views ? ' · ' + views + ' views' : '')});
      }
    }
    throw new Error('the Avatar build did not finish in time');
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
    // Pictures 2..N of a multi-reference node travel as one ordered list; the
    // socket number is the order the prompt counts them in.
    const references = Object.keys(resolved)
      .map(field => ({field, match: /^reference_(\d+)$/.exec(field)}))
      .filter(item => item.match && resolved[item.field])
      .sort((a, b) => Number(a.match[1]) - Number(b.match[1]))
      .map(item => resolved[item.field]);
    if (references.length) body.reference_image_urls = references;
    Object.keys(resolved).forEach(field => {
      const value = resolved[field];
      if (/^reference_\d+$/.test(field)) return;
      if (field === 'image' || field === 'image_url_end') {
        const key = field === 'image' ? 'image_url' : 'image_url_end';
        const inline = field === 'image' ? 'image_base64' : 'image_base64_end';
        if (String(value).startsWith('data:')) body[inline] = value; else body[key] = value;
      } else {
        body[field] = value;
      }
    });
    // A node that carries a standing instruction always asks for the answer
    // alone: one JSON object in, `output_text` out. The instruction travels as
    // its own field so it never lands in the text the next node reads, and it
    // is part of the request signature, so editing it re-runs the node.
    const declaration = catalogue ? serviceById(serviceId) : null;
    if (declaration && declaration.system_prompt_capable) {
      const standing = (params || {})._system_prompt;
      const text = String(typeof standing === 'string'
        ? standing : (declaration.system_prompt_default || ''));
      if (text.trim()) body.system_prompt = text;
      body.structured = true;
    }
    // A LoRA of another model family (left in a slot when the checkpoint
    // changed) is left out, so the render runs with the ones that fit; the
    // slot says so inline (ai-node-lora-stack.js).
    if (typeof window !== 'undefined' && window.AINodeLoraStack && window.AINodeLoraStack.filterBody) {
      window.AINodeLoraStack.filterBody(serviceId, body);
    }
    // Render quality: every width/height scaled, rounded and clamped here, so
    // the scaled size is what the signature records and what the server gets.
    if (typeof window !== 'undefined' && window.AIRenderQuality && renderQuality !== 'normal') {
      window.AIRenderQuality.applyToBody(serviceId, body, renderQuality, declaration);
    }
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
    let runner = null;
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
      runner = runnerFor(node.service);
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
      let outputs = null;
      try {
        ({value, outputs} = splitMulti(await runner.finish(accepted, runner,
          taskStateReporter(state, task, accepted, execution))));
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
        showResult(outBox, runner.type, value, outputs);
        recordResult(id, Object.assign({ status: 'done', type: runner.type, value: value,
                           input_reference_url: execution.inputReference || '',
                           task_id: accepted.task_id_string || '' }, outputs ? {outputs} : {}));
      }
      return outputs ? { type: runner.type, value: value, outputs } : { type: runner.type, value: value };
    } catch (error) {
      finishTaskTracker(task, false, execution, progress);
      if (executionIsCurrent(execution)) {
        state.textContent = String(error.message || error);
        state.className = 'nstate failed';
        recordResult(id, { status: 'failed', type: runner?.type || '', value: '',
                           error: String(error.message || error) });
      }
      throw error;
    }
  }

  /* ------------------------------------------------------ preview metadata */

  /**
   * The pixel size of a preview, in its top-left corner.
   *
   * Read off the media itself once the browser has it, never from the width
   * and height parameters: those say what was asked for, and the interesting
   * question — especially with a ControlNet map or a model that rounds to its
   * own grid — is what actually came back. The gallery at /workflows shows the
   * same badge in the same corner, so the two pages agree on what a size looks
   * like. The badge never takes a click: the picture underneath it opens the
   * full-size preview and must keep doing so at every pixel.
   */
  function markResolution(host, media) {
    if (!host || !media) return null;
    let badge = host.querySelector(':scope > .nres');
    if (!badge) {
      badge = document.createElement('span');
      badge.className = 'nres';
      host.insertBefore(badge, host.firstChild);
    }
    const show = () => {
      const width = media.naturalWidth || media.videoWidth || 0;
      const height = media.naturalHeight || media.videoHeight || 0;
      // `complete`/`readyState` go back to false the moment a new source is
      // set, so a changed picture blanks the badge instead of showing the
      // size of the one it replaced.
      const ready = media.tagName === 'VIDEO' ? media.readyState >= 1 : media.complete;
      badge.textContent = ready && width && height && !media.hidden
        ? width + '×' + height : '';
      // An input box holds the address field and the file button above its
      // preview, so the corner of the box is not the corner of the picture.
      // The badge is placed against the media itself, wherever that sits.
      badge.style.top = (media.offsetTop + 6) + 'px';
      badge.style.left = (media.offsetLeft + 6) + 'px';
    };
    // An input preview is re-marked every time its picture changes, so the
    // listeners are bound once and read whichever badge is current.
    media._resolutionShow = show;
    if (!media._resolutionBound) {
      media._resolutionBound = true;
      ['load', 'loadedmetadata', 'emptied', 'error'].forEach(name =>
        media.addEventListener(name, () => media._resolutionShow && media._resolutionShow()));
      // Compact mode hides the address field, which moves the picture up.
      if (typeof ResizeObserver === 'function') {
        const watcher = new ResizeObserver(() => media._resolutionShow && media._resolutionShow());
        watcher.observe(host);
        watcher.observe(media);
      }
    }
    show();
    return badge;
  }

  /** Every picture a multi-output node made, small, each opening full size. */
  function showOutputs(host, outputs) {
    const pictures = Object.keys(outputs || {}).filter(field =>
      /_url_string$/.test(field) && /^https?:[/][/]/.test(String(outputs[field])));
    if (!pictures.length) return;
    const grid = document.createElement('div');
    grid.className = 'nviews';
    grid.style.cssText = 'display:grid;grid-template-columns:repeat(4,1fr);gap:3px;margin-top:4px';
    pictures.forEach(field => {
      const url = String(outputs[field]);
      const picture = document.createElement('img');
      picture.src = url;
      picture.loading = 'lazy';
      picture.alt = picture.title = field.replace(/_url_string$/, '').replace(/_/g, ' ');
      picture.style.cssText = 'width:100%;aspect-ratio:2/3;object-fit:cover;border-radius:4px;cursor:zoom-in;background:#111';
      picture.classList.add('preview-expandable');
      picture.addEventListener('click', event => { event.stopPropagation(); openPreview('image', url); });
      grid.appendChild(picture);
    });
    host.appendChild(grid);
  }

  function showResult(host, type, value, outputs) {
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
      markResolution(host, picture);
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
        markResolution(host, clip);
        clip.play().catch(() => {});
    }
    if (outputs && typeof outputs === 'object') showOutputs(host, outputs);
    const link = document.createElement('a');
    link.href = value;
    link.target = '_blank';
    link.rel = 'noopener';
    link.className = 'nlink';
    // The preview itself opens full size; no separate Open link.
    link.textContent = '';
    if (link.textContent) host.appendChild(link);
  }

  /* --------------------------------------------- control-channel agreement */

  /**
   * Whether a checkpoint may be handed a control map of this channel.
   *
   * This is the server's rule, written out once so the page cannot be stricter
   * than the thing that actually runs the job. The backend accepts the wire
   * when the checkpoint names the channel itself *or* when its family is one
   * the extractor says it has been validated against; the page used to demand
   * the first alone, which is why Depth stopped attaching to every FLUX
   * checkpoint even though the render behind it would have worked.
   *
   * No checkpoint chosen means no opinion: the service's own deployed default
   * workflow decides, exactly as the validator does.
   */
  function controlChannelAccepted(channel, checkpoint, controlService) {
    if (!checkpoint) return true;
    const named = ((checkpoint.control_channels) || []).map(value => String(value));
    if (named.includes(String(channel))) return true;
    const families = (((controlService || {}).compatible_image_families) || [])
      .map(value => String(value).toLowerCase());
    return families.includes(String(checkpoint.family || '').toLowerCase());
  }

  /** The checkpoints this channel would have connected to, for the refusal. */
  function controlChannelModels(channel, controlService, checkpoints) {
    return (checkpoints || [])
      .filter(item => item && item.usable !== false &&
                      controlChannelAccepted(channel, item, controlService))
      .map(item => String(item.title || item.base || item.file || item.id || ''))
      .filter(Boolean);
  }

  function controlRefusalMessage(channel, names) {
    const title = String(channel || '').charAt(0).toUpperCase() + String(channel || '').slice(1);
    if (!names || !names.length) {
      return title + ' control needs a model validated for it. Choose one of the '
           + 'checkpoints the catalogue lists for this channel, or disconnect the control.';
    }
    return title + ' control needs a model validated for it: '
         + names.slice(0, 4).join(', ') + (names.length > 4 ? '…' : '.');
  }

  /**
   * Say no, and say what would have worked.
   *
   * The catalogue the picker reads is already cached by the time any node has
   * a checkpoint on it, so the second toast normally lands in the same frame;
   * the first one is there for the rare cold start.
   */
  function toastControlRefusal(channel, serviceId) {
    const controlService = serviceById('control_' + channel);
    toast(controlRefusalMessage(channel, null));
    if (!(window.AIEntities && window.AIEntities.loadModels)) return;
    window.AIEntities.loadModels(serviceId || 'image').then(data => {
      const names = controlChannelModels(channel, controlService,
                                         (data && data.checkpoints_array) || []);
      if (names.length) toast(controlRefusalMessage(channel, names));
    }).catch(() => {});
  }

  /** The first control channel actually wired into this node, if any. */
  function connectedControlChannel(id) {
    const node = editor.getNodeFromId(id);
    const item = meta(id);
    if (!node || !item) return '';
    const field = item.inFields.find((name, index) => name.startsWith('control_') &&
      ((node.inputs['input_' + (index + 1)] || {}).connections || []).length);
    return field ? field.slice(8) : '';
  }

  function modelAcceptsConnectedControls(id, entry) {
    const node = editor.getNodeFromId(id);
    const item = meta(id);
    if (!node || !item) return true;
    return item.inFields.every((field, index) => {
      const connected = ((node.inputs['input_' + (index + 1)] || {}).connections || []).length;
      if (!field.startsWith('control_') || !connected) return true;
      // The input field and the extractor service share a name: `control_depth`
      // is both the socket and the service whose families are consulted.
      return controlChannelAccepted(field.slice(8), entry, serviceById(field));
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

  /**
   * How large a socket should be drawn, as a fraction of its unzoomed size.
   *
   * A socket is a target for the mouse rather than part of the picture, so its
   * size on screen should not follow the camera. Pulling back, it grows — as
   * the square root, which is what the display modes did before this became
   * one function — or a dot on a distant node is unclickable. Closing in, it
   * shrinks toward a constant size on screen, because at full size it covers
   * the node's own text. Both ends are clamped and the halves meet at 1. The
   * result is rounded, because a wheel that moves the zoom by a hair must not
   * force every connection in the graph to be recomputed.
   */
  const SOCKET_SCALE_MIN = 0.55;
  const SOCKET_SCALE_MAX = 1.8;

  function socketScaleForZoom(zoom) {
    const value = Number(zoom);
    if (!Number.isFinite(value) || value <= 0) return 1;
    const wanted = value < 1 ? 1 / Math.sqrt(value) : 1 / value;
    const clamped = Math.min(SOCKET_SCALE_MAX, Math.max(SOCKET_SCALE_MIN, wanted));
    return Math.round(clamped * 100) / 100;
  }

  let socketScaleApplied = null;

  /**
   * Resize the sockets and tell Drawflow where the wires now end.
   *
   * The size is a layout size, not a transform, so `offsetWidth` moves with it
   * and the wire ends exactly on the dot at every zoom — but Drawflow only
   * recomputes a connection when its node moves, and this moves the sockets
   * without moving anything else, so the graph is asked to catch up.
   */
  function applySocketScale(zoom) {
    const scale = socketScaleForZoom(zoom);
    if (scale === socketScaleApplied) return scale;
    socketScaleApplied = scale;
    // On the canvas, not the document: that is where this has always been set
    // from, and a value on the document root would lose to it.
    const canvas = document.getElementById('canvas');
    if (canvas) canvas.style.setProperty('--socket-zoom-scale', String(scale));
    if (editor) {
      document.querySelectorAll('#canvas .drawflow-node').forEach(node => {
        try { editor.updateConnectionNodes(node.id); } catch (_) {}
      });
    }
    return scale;
  }

  function installWheelZoom() {
    const canvas = document.getElementById('canvas');
    editor.zoom_min = 0.15;
    editor.zoom_max = 2.5;
    applySocketScale(editor.zoom);
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
      applySocketScale(nextZoom);
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
          x: raw.pos_x, y: raw.pos_y,
          params: node.disabled
            ? {_display_mode: node.displayMode || 'medium', _disabled: true}
            : {_display_mode: node.displayMode || 'medium'}
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
             render_quality: renderQuality,
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

  /**
   * Save the canvas as one document.
   *
   * A graph that already has a link is updated under it (PUT), so editing and
   * rendering never add library entries; the link in the address bar stays
   * the same. Only the first save creates a document, and it gets a fresh
   * instance id so two people starting from the same template do not share
   * one. The server refuses an unknown id (404) — then this is a new
   * document after all. Only "Duplicate graph" makes a copy.
   */
  async function persistGraph() {
    const graph = graphFromCanvas();
    if (graphId) {
      const response = await fetch('/api/ai/graphs/' + encodeURIComponent(graphId), {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(graph)
      });
      if (response.status !== 404 && response.status !== 409) {
        return { response, data: await response.json().catch(() => ({})), created: false };
      }
    }
    if (!graphInstanceId) {
      graphInstanceId = (window.crypto && crypto.randomUUID)
        ? crypto.randomUUID()
        : 'g' + Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
      graph.instance_id = graphInstanceId;
    }
    const response = await fetch('/api/ai/graphs', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(graph)
    });
    return { response, data: await response.json().catch(() => ({})), created: true };
  }

  /** The quality rides in the address too (?q=), so a shared link opens in it. */
  function syncQualityUrl() {
    try {
      const url = new URL(location.href);
      if (renderQuality === 'normal') url.searchParams.delete('q');
      else url.searchParams.set('q', renderQuality);
      history.replaceState(null, '', url.pathname + url.search);
    } catch (error) { /* an address that cannot be parsed keeps what it had */ }
  }

  function setRenderQuality(mode) {
    const quality = window.AIRenderQuality ? window.AIRenderQuality.normalize(mode) : 'normal';
    renderQuality = quality;
    if (renderQualityToolbar) renderQualityToolbar.paint();
    if (renderQualityBadges) renderQualityBadges.refresh();
    syncQualityUrl();
    return quality;
  }

  function adoptSavedId(data) {
    graphId = data.graph_id_string;
    history.replaceState(null, '', data.deep_link_string);
    syncQualityUrl();
    document.dispatchEvent(new CustomEvent('ai-graph-saved', {detail:{graphId}}));
  }

  /** A run needs a link to publish its progress to, so one is made up front. */
  async function ensureSaved() {
    try {
      const { response, data } = await persistGraph();
      if (response.ok) adoptSavedId(data);
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
        showResult(element.querySelector('.nout'), continued.type, continued.value, continued.outputs);
      }
      return Promise.resolve({type:continued.type, value:continued.value, outputs:continued.outputs || null});
    }

    const requestBody = bodyFor(node.service, resolved, params);
    const completed = (completedExecutions.get(idString) || new Map()).get(signature);
    if (completed && reusableCompleted(node.service, requestBody)) {
      const element = nodeElement(idString);
      if (element && epoch === canvasEpoch) {
        const state = element.querySelector('.nstate');
        state.textContent = 'cached';
        state.className = 'nstate done';
        showResult(element.querySelector('.nout'), completed.type, completed.value, completed.outputs);
        recordResult(idString, {status:'done', type:completed.type, value:completed.value,
          input_reference_url:completed.input_reference_url || (nodeCompare?.resolveReference(idString, graphSnapshot) || resolved.image || ''),
          task_id:completed.task_id || '', outputs:completed.outputs || undefined});
      }
      return Promise.resolve({type: completed.type, value: completed.value, outputs: completed.outputs || null});
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
          // A bypassed node is treated as absent: it never starts, and every
          // step that reads its output is reported as skipped rather than run.
          if ((node.params || {})._disabled) {
            if (epoch === canvasEpoch && meta(id)) markState(id, 'bypassed — not run', 'nstate');
            return {ok:false, bypassed:true};
          }
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
            const bypassed = upstreamRecords.some(record => record && record.bypassed);
            if (epoch === canvasEpoch && meta(id)) {
              markState(id, bypassed
                ? 'skipped — something it needs is bypassed'
                : 'skipped — what it needed did not arrive', bypassed ? 'nstate' : 'nstate failed');
            }
            return {ok:false, bypassed:bypassed};
          }
          if (node.kind === KIND_INPUT) {
            const value = inputValues.get(id) || '';
            if (!value) return {ok:false, error:'empty input'};
            return {ok:true, result:{type:node.entity_type, value}};
          }
          const resolved = {};
          feeds.forEach((link, index) => {
            const upstream = upstreamRecords[index]?.result;
            if (upstream) resolved[link.input] = outputValue(upstream, link.output);
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
      const skipped = serviceIds.filter(id => settled[order.indexOf(id)]?.bypassed).length;
      const failed = serviceIds.length - produced - skipped;
      if (!settled.some(record => record?.superseded)) {
        const asked = serviceIds.length - skipped;
        const aside = skipped ? `; ${skipped} skipped as bypassed` : '';
        toast(!asked
          ? `Nothing to run — ${skipped} step${skipped === 1 ? ' is' : 's are'} bypassed.`
          : failed
            ? `${produced} of ${asked} steps finished; ${failed} did not${aside}.`
            : `All ${asked} steps finished${aside}.`);
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
        if (record.status === 'done') continuableResults.set(String(id), {type:record.type, value:record.value, outputs:record.outputs || null,
          task_id:record.task_id || ''});
        state.textContent = record.status === 'stale' ? 'changed — render to update' : 'done';
        state.className = record.status === 'stale' ? 'nstate' : 'nstate done';
        showResult(outBox, record.type, record.value, record.outputs);
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
    let runner;
    const element = nodeElement(id);
    const state = element.querySelector('.nstate');
    const outBox = element.querySelector('.nout');
    state.textContent = 'still running on the farm…';
    state.className = 'nstate running';
    const task = window.AIEntities
      ? window.AIEntities.startTask(element.querySelector('.nprog, .task-prog'), node.service)
      : null;
    try {
      runner = runnerFor(node.service);
    } catch (error) {
      state.textContent = error.message;
      state.className = 'nstate failed';
      if (task) task.finish(false);
      recordResult(id, {status:'failed', type:'', value:'', error:error.message});
      throw error;
    }
    // The pollers only need what the submit returned, and that is exactly
    // what was stored, so the same code finishes the job.
    const accepted = { task_id_string: record.task_id };
    accepted[runner.field] = record.value || '';
    try {
      const reporter = taskStateReporter(state, task, accepted);
      const {value, outputs} = splitMulti(await runner.finish(accepted, runner, data => {
        if (stillHere()) reporter(data);
      }));
      if (!stillHere()) return;
      state.textContent = accepted.cache_hit_bool ? 'cached' : 'done';
      state.className = 'nstate done';
      if (task) task.finish(true);
      showResult(outBox, runner.type, value, outputs);
      recordResult(id, Object.assign({ status: 'done', type: runner.type, value: value,
                         input_reference_url:record.input_reference_url || '', history:record.history || [],
                         task_id: record.task_id || '' }, outputs ? {outputs} : {}));
      return outputs ? {type:runner.type, value, outputs} : {type:runner.type, value};
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
    setRenderQuality(graph.render_quality || 'normal');
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
    // A graph that opens half off-screen looks empty. The canvas has just been
    // replaced wholesale, so there is no pan of anyone's to preserve.
    scheduleFitView();
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
    const { response, data } = await persistGraph();
    if (!response.ok) {
      const detail = data.detail || {};
      toast(detail.message_string || 'The graph was not saved.');
      return;
    }
    const changed = graphId !== data.graph_id_string;
    adoptSavedId(data);
    const link = location.origin + data.deep_link_string;
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

  const SOURCE_HELP = {
    image: 'A picture to start from: paste, drop a file or give an address.',
    video: 'A clip to start from: drop a file or give an address.',
    text: 'Words to hand to a service as a prompt.',
    avatar: 'One of your saved characters, pinned to a version.'
  };

  /**
   * One glyph per tool, because the icon is now the whole button.
   *
   * The catalogue only says what a service produces, and four different tools
   * produce a picture; four identical buttons would be a worse palette than
   * the list this replaced. A service with no glyph here still gets the icon
   * of the thing it makes, so a new service appears without an edit.
   */
  const TOOL_ICONS = {
    'input:image': '🏞️', 'input:video': '📹', 'input:text': '✏️', 'input:avatar': '👤',
    vision: '👁️', text: '📝', image: '🖼️', video: '🎬', '3dmodel': '🧊',
    video_frame: '⏮️', video_storyboard: '🎞️', video_control: '🏃',
    avatar_image: '🎭', avatar_video: '📽️', avatar_from_image: '🪪',
    upscale: '🔎', detail_enhance: '✨', face_fix: '🙂', upscale_video: '📺',
    qwen_image: '🖌️'
  };

  function toolIcon(key, fallbackType) {
    return TOOL_ICONS[key] || typeIcon(fallbackType);
  }

  /**
   * A compact strip of icons over the top-left of the canvas.
   *
   * Tools are the thing a person reaches for constantly; a column of named
   * cards took a quarter of the window to say what a row of icons says in two
   * lines. The name and the one-line description are still there, on hover and
   * in the accessible name, so nothing is lost but the space.
   */
  function buildPalette() {
    const host = document.getElementById('palette');
    if (!host) return;
    host.innerHTML = '';
    [['image', 'Image in'], ['video', 'Video in'], ['text', 'Text in'], ['avatar', 'Avatar']].forEach(([type, title]) => {
      host.appendChild(paletteButton(title, toolIcon('input:' + type, type), SOURCE_HELP[type] || '',
        {kind:'input', type, title}));
    });
    host.appendChild(document.createElement('hr'));
    (catalogue.services_array || []).forEach(entry => {
      const button = paletteButton(entry.title,
                                   toolIcon(entry.id, (entry.produces_array || [])[0]),
                                   entry.summary,
                                   {kind:'service', service:entry.id, title:entry.title});
      if (entry.status !== 'live') {
        // A service can say *why* it is not callable. "Not wired up yet" is a
        // fine default, but "the card has no weights for it" is the answer to
        // the question the greyed-out button actually raises.
        const why = entry.blocked_reason || 'Not wired up yet.';
        button.disabled = true;
        // A disabled button still starts a drag, and dropping it on the canvas
        // made a node no runner knows how to call.
        button.draggable = false;
        button.setAttribute('aria-label', entry.title + '. ' + why);
        const note = button.querySelector('.ttip i');
        if (note) note.textContent = why;
      }
      host.appendChild(button);
    });
    host.appendChild(document.createElement('hr'));
    host.appendChild(actionButton('Arrange', '▦',
      'Lay the selected nodes out in columns by depth, or the whole graph when nothing is selected.',
      () => arrangeNodes(Array.from(nodeGroups?.selected || []))));
    host.appendChild(actionButton('Fit view', '⤢',
      'Frame the whole composition: zoom and pan so every node is on screen.',
      () => {
        if (!document.querySelector('#canvas .drawflow-node')) {
          toast('There is nothing on the canvas to frame yet.');
        } else fitView();
      }));
  }

  /** Lay the graph out, then frame what the layout produced. */
  function arrangeNodes(ids) {
    if (!nodePipelines) return;
    nodePipelines.arrange(ids);
    scheduleFitView();
  }

  function toolButton(title, icon, help) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'titem';
    button.setAttribute('aria-label', help ? title + '. ' + help : title);
    button.innerHTML = `<span class="ticon" aria-hidden="true">${icon}</span>`
                     + `<span class="ttip"><b>${escapeHtml(title)}</b>`
                     + `<i>${escapeHtml(help || '')}</i></span>`;
    return button;
  }

  function paletteButton(title, icon, help, placement) {
    const button = toolButton(title, icon, help);
    if (nodePlacement) nodePlacement.bindPaletteButton(button, placement);
    // The drag image is taken from the button, and a tooltip open under the
    // cursor would be dragged along with it.
    button.addEventListener('dragstart', () => button.classList.add('dragging'));
    button.addEventListener('dragend', () => button.classList.remove('dragging'));
    return button;
  }

  function actionButton(title, icon, help, onClick) {
    const button = toolButton(title, icon, help);
    button.classList.add('taction');
    button.addEventListener('click', onClick);
    return button;
  }

  /**
   * Move a node and have the graph agree that it moved.
   *
   * `editor.getNodeFromId` hands back a deep copy, so writing a position into
   * it changes nothing that is saved or exported: the node would slide on
   * screen and snap back on reload. The live record is the one Drawflow keeps
   * per module, which is also what a group drag writes to.
   */
  function moveNodeTo(id, x, y) {
    const module = editor.drawflow.drawflow[editor.module];
    const data = module && module.data[id];
    const element = nodeElement(id);
    if (!data || !element) return;
    data.pos_x = x;
    data.pos_y = y;
    element.style.left = x + 'px';
    element.style.top = y + 'px';
    editor.updateConnectionNodes('node-' + id);
  }

  /**
   * Start the view clear of the tool strip.
   *
   * The strip floats over the top-left corner, which is exactly where a graph
   * that begins at the origin draws its first node. Panning the camera once,
   * at startup, costs nothing and stops the first thing a person sees from
   * being half-hidden; a graph already framed somewhere else is left alone.
   */
  /**
   * Where the camera has to sit for a whole graph to be on screen.
   *
   * Plain arithmetic over boxes in graph coordinates, so the framing can be
   * checked without a canvas. The tool strip floats over the top-left of the
   * stage, so the band it occupies is taken out of the picture rather than
   * drawn over; and nothing is ever magnified past its natural size, because a
   * two-node composition blown up to fill a monitor reads as a mistake rather
   * than as a fit.
   */
  function fitTransform(boxes, viewport) {
    const list = (boxes || []).filter(box =>
      Number.isFinite(Number(box.x)) && Number.isFinite(Number(box.y)));
    if (!list.length) return null;
    const options = viewport || {};
    const pad = Number(options.padding) > 0 ? Number(options.padding) : 40;
    const top = Math.max(0, Number(options.top) || 0);
    const minZoom = Number(options.minZoom) > 0 ? Number(options.minZoom) : 0.15;
    const maxZoom = Number(options.maxZoom) > 0 ? Number(options.maxZoom) : 1;
    const left = Math.min(...list.map(box => Number(box.x)));
    const upper = Math.min(...list.map(box => Number(box.y)));
    const right = Math.max(...list.map(box => Number(box.x) + (Number(box.width) || 0)));
    const lower = Math.max(...list.map(box => Number(box.y) + (Number(box.height) || 0)));
    const width = Math.max(1, right - left);
    const height = Math.max(1, lower - upper);
    const roomX = Math.max(1, (Number(options.width) || 0) - pad * 2);
    const roomY = Math.max(1, (Number(options.height) || 0) - top - pad * 2);
    const zoom = Math.min(maxZoom, Math.max(minZoom, Math.min(roomX / width, roomY / height)));
    return {
      zoom,
      x: pad + (roomX - width * zoom) / 2 - left * zoom,
      y: top + pad + (roomY - height * zoom) / 2 - upper * zoom
    };
  }

  /** Every node's box in graph coordinates, which is what `offset*` reports. */
  function nodeBoxes() {
    const module = editor && editor.drawflow.drawflow[editor.module];
    const data = (module && module.data) || {};
    return Object.keys(data).map(id => {
      const element = nodeElement(id);
      return {
        x: Number(data[id].pos_x) || 0,
        y: Number(data[id].pos_y) || 0,
        width: (element && element.offsetWidth) || 244,
        height: (element && element.offsetHeight) || 120
      };
    });
  }

  /** How much of the stage the floating tool strip is sitting on. */
  function toolStripHeight() {
    const tools = document.getElementById('palette');
    if (!tools) return 0;
    // Measured before the strip has finished wrapping, this comes back as one
    // icon per row — a 800px band that would push the graph off the bottom of
    // the screen. Two rows of icons is about 90px, so anything past a quarter
    // of the window is a measurement, not a toolbar.
    return Math.min(Math.round(tools.getBoundingClientRect().height) + 20,
                    Math.round(window.innerHeight / 4) || 140);
  }

  /**
   * Put the whole graph on screen, clear of the tools.
   *
   * Only ever on demand or when the canvas is replaced wholesale; a view that
   * re-framed itself would fight whoever is panning it.
   */
  function fitView() {
    if (!editor) return false;
    const stage = document.getElementById('canvas');
    if (!stage) return false;
    const bounds = stage.getBoundingClientRect();
    // A tab that has never been painted has no layout: every rectangle comes
    // back as zero, and a camera computed from zeros frames nothing at all.
    // Opening a graph in a background tab did exactly that.
    if (bounds.width < 2 || bounds.height < 2) return false;
    const boxes = nodeBoxes();
    if (!boxes.length) { offsetViewBelowTools(); return true; }
    const view = fitTransform(boxes, {
      width: bounds.width, height: bounds.height, top: toolStripHeight(),
      padding: 36, minZoom: editor.zoom_min || 0.15, maxZoom: 1
    });
    if (!view) return false;
    editor.canvas_x = view.x;
    editor.canvas_y = view.y;
    editor.zoom = view.zoom;
    editor.zoom_last_value = view.zoom;
    editor.precanvas.style.transform =
      `translate(${view.x}px, ${view.y}px) scale(${view.zoom})`;
    applySocketScale(view.zoom);
    editor.dispatch('zoom', view.zoom);
    return true;
  }

  let fitViewFrame = 0;
  let fitViewTimer = null;
  let fitViewTries = 0;

  /**
   * A node's height is only known a frame after it is built, and a tab that is
   * not on screen never gets that frame — nor any layout to measure — so a
   * timer runs the same step, and keeps running it until there is something
   * to measure. Whichever gets there first cancels the other, and fitting
   * twice would in any case land in exactly the same place.
   */
  function attemptFitView() {
    clearTimeout(fitViewTimer);
    fitViewTimer = null;
    if (fitView() || fitViewTries++ > 20) return;
    fitViewTimer = setTimeout(attemptFitView, 150);
  }

  function scheduleFitView() {
    cancelAnimationFrame(fitViewFrame);
    clearTimeout(fitViewTimer);
    fitViewTries = 0;
    fitViewFrame = requestAnimationFrame(attemptFitView);
    fitViewTimer = setTimeout(attemptFitView, 140);
  }

  function offsetViewBelowTools() {
    const tools = document.getElementById('palette');
    if (!tools || !editor || editor.canvas_x || editor.canvas_y) return;
    editor.canvas_x = 20;
    // Measured before the strip has finished wrapping, this comes back as one
    // icon per row — a 800px push that leaves the canvas looking empty on a
    // graph that is simply above the fold. Two rows of icons is about 90px, so
    // anything past a quarter of the window is a measurement, not a toolbar.
    const measured = Math.round(tools.getBoundingClientRect().height) + 26;
    editor.canvas_y = Math.min(measured, Math.round(window.innerHeight / 4) || 140);
    editor.precanvas.style.transform =
      `translate(${editor.canvas_x}px, ${editor.canvas_y}px) scale(${editor.zoom})`;
  }

  async function boot() {
    await loadCatalogue();
    editor = new Drawflow(document.getElementById('canvas'));
    editor.reroute = true;
    editor.start();
    if (window.AINodePlacement) nodePlacement = window.AINodePlacement.install({
      editor, canvas:document.getElementById('canvas'),
      createNode:(spec, x, y) => spec.kind === 'input'
        ? addInputNode(spec.type, x, y, '')
        : addServiceNode(spec.service, x, y, null),
      getNodeElement:nodeElement,
      moveNode:moveNodeTo
    });
    if (window.AINodeDisplay) nodeDisplay = window.AINodeDisplay.install({editor,
      canvas:document.getElementById('canvas'), getMeta:meta, defaultMode:'medium',
      onModeChange:(id, mode) => { const value=meta(id); if(value) value.displayMode=mode; if(nodeCompare) nodeCompare.refresh(id); }});
    if (window.AINodeShare) window.AINodeShare.install({canvas:document.getElementById('canvas'), getMeta:meta, toast});
    if (window.AINodeSockets) window.AINodeSockets.install({editor, canvas:document.getElementById('canvas'),
      socketTypes, linkAllowed, entityTypes:catalogue.entity_types_array || []});
    installStatusLines(document.getElementById('canvas'));
    if (window.AIRenderQuality) {
      renderQualityToolbar = window.AIRenderQuality.installToolbar({
        host: document.getElementById('run'), get: () => renderQuality, set: setRenderQuality});
      renderQualityBadges = window.AIRenderQuality.installBadges({
        canvas: document.getElementById('canvas'), getMeta: meta, serviceById,
        getQuality: () => renderQuality});
    }
    if (window.AINodeLoraStack) window.AINodeLoraStack.install({canvas:document.getElementById('canvas'), getMeta:meta});
    installWheelZoom();
    if (window.AINodePipelines && window.AIEntities) nodePipelines = window.AINodePipelines.install({
      editor, getMeta:meta, addServiceNode, getNodeElement:nodeElement, moveNode:moveNodeTo,
      exportGraph:graphFromCanvas, imageOutput:imageOutputField, nodeLimit:200, toast,
      loadModels:service => window.AIEntities.loadModels(service),
      onNodesAdded:ids => { if (nodeGroups) nodeGroups.selectIds(ids); }});
    if (window.AINodeGroups) nodeGroups = window.AINodeGroups.install({editor,
      canvas:document.getElementById('canvas'), getMeta:meta, addInputNode,
      addServiceNode, exportGraph:graphFromCanvas, toast, nodeLimit:200,
      onNodesRemoved:forgetNodes,
      nodeFunctions:id => nodePipelines ? nodePipelines.functionsFor(id) : [],
      onArrange:ids => arrangeNodes(ids),
      onToggleBypass:toggleBypass,
      onEditSystemPrompt:ids => openSystemPromptEditor(ids),
      systemPromptTargets,
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
        refreshReferenceSockets(connection.input_id);
        invalidateNodeAndDownstream(connection.input_id);
      }
    });
    editor.on('nodeRemoved', id => forgetNodes([id]));
    // LoRA stack "+" menu: filled on first open with the LoRAs this service
    // can load right now; picking one appends its tag to the stack field.
    document.getElementById('canvas').addEventListener('pointerdown', event => {
      const select = event.target.closest && event.target.closest('select.lstack-add');
      if (select && !select.dataset.filled) fillLoraStackMenu(select);
    }, true);
    document.getElementById('canvas').addEventListener('focusin', event => {
      const select = event.target.closest && event.target.closest('select.lstack-add');
      if (select && !select.dataset.filled) fillLoraStackMenu(select);
    });
    document.getElementById('canvas').addEventListener('change', event => {
      const select = event.target.closest && event.target.closest('select.lstack-add');
      if (!select || !select.value) return;
      const field = select.parentElement.querySelector('input[data-param]');
      if (field) {
        field.value = (field.value.trim() + ' ' + select.value).trim();
        field.dispatchEvent(new Event('input', { bubbles: true }));
        field.dispatchEvent(new Event('change', { bubbles: true }));
      }
      select.value = '';
    });
    // A range's number is only useful if it is shown next to the slider.
    document.getElementById('canvas').addEventListener('change', event => {
      if (programmaticParamEvent(event)) return;
      if (event.target.dataset && event.target.dataset.param) {
        event.target.dataset.touched = 'yes';
      }
      const node = event.target.closest && event.target.closest('.drawflow-node');
      if (node && (event.target.dataset?.param || event.target.dataset?.value !== undefined)) {
        invalidateNodeAndDownstream(node.id.replace(/^node-/, ''));
      }
    });
    document.getElementById('canvas').addEventListener('input', event => {
      const silent = programmaticParamEvent(event);
      if (!silent && event.target.dataset && event.target.dataset.param) {
        event.target.dataset.touched = 'yes';
      }
      const node = event.target.closest && event.target.closest('.drawflow-node');
      if (!silent && node && (event.target.dataset?.param || event.target.dataset?.value !== undefined)) {
        invalidateNodeAndDownstream(node.id.replace(/^node-/, ''));
      }
      if (event.target.type !== 'range') return;
      const readout = event.target.parentElement.querySelector('output');
      if (readout) updateSamplingReadout(event.target);
    });

    buildPalette();
    // A dropdown that stays open after a click elsewhere reads as stuck.
    const compositions = document.getElementById('compositions');
    if (compositions) {
      document.addEventListener('mousedown', event => {
        if (compositions.open && !compositions.contains(event.target)) compositions.open = false;
      }, true);
    }
    // The strip says what the whole farm is doing, coloured by the kind of
    // work, which on this page matters more than on any single-service one:
    // a composition is waiting on several sorts of job at once.
    if (window.AIEntities) {
      window.AIEntities.mountFleet(document.getElementById('fleet'), 'image');
      startCapacityBadges();
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
      syncQualityUrl();
    });
    setRunning(false);

    const templates = await fetch('/api/ai/graph/templates').then(r => r.json()).catch(() => ({}));
    const picker = document.getElementById('templates');
    (templates.templates_array || []).forEach(template => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'tmpl';
      button.innerHTML = `<b>${escapeHtml(template.title)}</b><i>${escapeHtml(template.summary)}</i>`;
      button.addEventListener('click', () => {
        // A template starts a new document; saving it must not overwrite
        // the graph that was open before.
        graphId = null;
        history.replaceState(null, '', '/nodes');
        loadGraph(template.graph);
        const drop = document.getElementById('compositions');
        if (drop) drop.open = false;
      });
      picker.appendChild(button);
    });

    const wanted = new URLSearchParams(location.search).get('g');
    const wantedAvatar = new URLSearchParams(location.search).get('avatar');
    const wantedQuality = new URLSearchParams(location.search).get('q');
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
    if (wantedQuality) setRenderQuality(wantedQuality);
    scheduleFitView();
    if (window.AINodeCompare) nodeCompare = window.AINodeCompare.install({
      canvas:document.getElementById('canvas'), getMeta:meta, getGraph:graphFromCanvas,
      getResult:id => runState.get(String(id)), getAnchorId:() => comparisonAnchorId,
      setAnchorId:id => { comparisonAnchorId = String(id || ''); }, toast});
    if (window.AIGraphBridge) graphBridge = window.AIGraphBridge.create({
      editor, getGraph:graphFromCanvas, addServiceNode, addInputNode, applyParams, getMeta:meta,
      forgetNodes, invalidateNodeAndDownstream, nodeDisplay, applyRecommended, applyBypass,
      applySystemPrompt:setSystemPrompt,
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

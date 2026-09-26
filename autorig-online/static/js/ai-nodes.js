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
  // New documents start as drafts (every size / 4); a saved graph keeps its own.
  const NEW_GRAPH_QUALITY = 'preview';
  let renderQuality = NEW_GRAPH_QUALITY;
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
  // The store's revision this tab opened; a save based on an older one is
  // refused (409 graph_stale) instead of overwriting somebody's newer graph.
  let graphRevision = null;
  let graphStale = false;
  // Branch isolation: the node isolated and every node's bypass state before.
  let isolation = null;
  // Nodes this page could not draw (a service it does not know yet) are kept
  // verbatim, with their wires and results, and written back on every save.
  let unplacedNodes = [];
  let unplacedLinks = [];
  let unplacedResults = {};
  let loadMapping = new Map();
  const LEGACY_MEDIA_INPUTS = ['image', 'video'];
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
  /** Drawflow id -> the id the stored graph knows the node by. */
  function storedIdMap() {
    const map = new Map();
    const used = new Set();
    nodeMeta.forEach((item, id) => {
      if (item && item.storedId && !used.has(item.storedId)) { map.set(String(id), item.storedId); used.add(item.storedId); }
    });
    unplacedNodes.forEach(item => used.add(String(item.id)));
    nodeMeta.forEach((item, id) => {
      if (map.has(String(id))) return;
      let candidate = String(id);
      while (used.has(candidate)) candidate = 'n' + candidate;
      map.set(String(id), candidate);
      used.add(candidate);
      if (item) item.storedId = candidate;
    });
    return map;
  }

  /** The canvas graph with stored ids (nodes, links, results, anchors). */
  function toStoredIds(graph) {
    const map = storedIdMap();
    const to = id => map.get(String(id)) || String(id);
    const out = Object.assign({}, graph);
    out.nodes = (graph.nodes || []).map(node => Object.assign({}, node, {id: to(node.id)}));
    out.links = (graph.links || []).map(link => Object.assign({}, link, {from: to(link.from), to: to(link.to)}));
    out.results = {};
    Object.keys(graph.results || {}).forEach(key => { out.results[to(key)] = graph.results[key]; });
    if (graph.comparison_anchor_id) out.comparison_anchor_id = to(graph.comparison_anchor_id);
    if (graph.isolation) {
      const prior = {};
      Object.keys(graph.isolation.prior || {}).forEach(key => { prior[to(key)] = graph.isolation.prior[key]; });
      out.isolation = {target: to(graph.isolation.target), prior};
    }
    return out;
  }

  function pushResults() {
    if (!graphId) return;
    clearTimeout(resultsTimer);
    resultsTimer = setTimeout(() => {
      const body = {};
      const map = storedIdMap();
      runState.forEach((value, key) => { body[map.get(String(key)) || key] = value; });
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
    } else if (param.type === 'number' && !['seed', 'width', 'height'].includes(param.name)
               && param.min != null && param.max != null && Number(param.max) - Number(param.min) <= 100000) {
      // Bounded numbers (frames, CFG, token budget) are sliders like the rest.
      control = `<input type="range" data-param="${name}" min="${param.min}" max="${param.max}" `
              + `step="${param.step || 1}" value="${param.default}"${help}>`
              + `<output data-for="${name}">${rangeLabel(param.default)}</output>`;
    } else if (param.type === 'number') {
      control = `<input type="number" data-param="${name}" min="${param.min}" max="${param.max}" `
              + `step="${param.step || 1}" value="${param.default}"${help}>`
              + (param.name === 'seed'
                ? `<button type="button" class="nseed-rand" data-seed-for="${name}" title="Random seed">🎲</button>` : '');
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

  // A multi-reference node (Qwen-Image 2.1 turbo, the one edit model) numbers its
  // picture sockets 1..N because the prompt counts them ("the jacket from
  // image 2"). Socket 1 is the node's ordinary `image`; the rest appear one at
  // a time as the previous one is wired. Each also takes a video, which the
  // server reads as its first frame.
  const REFERENCE_FIELD = /^reference_(\d+)$/;

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
    if (!entries.some(item => item.ref_index >= 2 || item.hide_when_empty)) return;
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
      // A socket kept only for older graphs appears once one wires it.
      const show = entry.hide_when_empty ? !!connected[field]
                 : (entry.ref_index ? shown[field] !== false : true);
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
   * A second picture wired into an Image node makes it an edit, and since
   * 2026-09-26 the farm has one edit model: Qwen-Image 2.1 turbo (3 pictures).
   * The server redirects the request; the toast says so once per node.
   */
  function ensureMultiReferenceModel(id, inField) {
    const node = meta(id);
    if (!node || node.service !== 'image' || referenceIndex(inField) < 2) return;
    if (node._editToast) return;
    node._editToast = true;
    toast('Several pictures → edit on Qwen-Image 2.1 turbo (up to 3)');
  }

  function inputNodeHtml(entityType) {
    if (entityType === 'media') return `
      <div class="nhead"><b>Media in</b></div>
      <div class="nports"><div class="prow pout">image / video ${typeIcon('media')}</div></div>
      <div class="ninput"><input type="text" data-value placeholder="Any link (Civitai too), drop a file or Ctrl+V">
        <input type="file" data-file accept="image/*,video/mp4,video/webm,video/quicktime" hidden>
        <button type="button" data-pick class="npick">Choose a file or Ctrl+V</button>
        <img data-preview alt="" hidden><video data-vpreview muted autoplay loop playsinline hidden></video></div>
      <div class="nstate"></div>`;
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
      followInputSize: hasDimensions ? sizeFollowsInput(entry, params) : undefined,
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
    addIsolateButton(id);
    if (X9_SERVICES.has(serviceId)) addX9Button(id, !!(params && params._x9));
    alignPorts(id, inputs.length, outputs.length);
    refreshReferenceSockets(id);
    mountModelPickers(id, serviceId);
    if (params) applyParams(id, params);
    if (params && params._disabled) applyBypass(id, true);
    materializeDefaultModel(id);
    // A Qwen-Image node saved with no model runs on the farm's one edit model;
    // show that instead of "Choose a model" (owner, 2026-09-27).
    if (serviceId === 'qwen_image' && !(params && String(params.checkpoint || '').trim())) {
      setTimeout(() => {
        const hidden = nodeElement(id) && nodeElement(id).querySelector('[data-param="checkpoint"]');
        if (hidden && !hidden.value) applyParams(id, {checkpoint: QWEN_DEFAULT_CHECKPOINT});
      }, 600);
    }
    return id;
  }

  /**
   * Auto (follow the input) or manual size, for a node being drawn.
   * `_size_auto` is the saved choice; a graph from before it existed follows
   * its input only if its size is one of the old stock defaults (owner rule).
   */
  const STOCK_SIZES = new Set(['960x540', '960x640', '540x960', '640x960', '1024x1024', '1024x576', '576x1024',
    '768x768', '512x512', '1280x720', '720x1280', '832x480', '480x832', '1024x768', '768x1024']);
  function sizeFollowsInput(entry, params) {
    if (!params) return true;
    if (params._follow_input_size === false) return false;
    if (typeof params._size_auto === 'boolean') return params._size_auto;
    // Saved by the earlier follow mode and never edited by hand (an edit
    // wrote false): it was following its input, so it keeps doing so.
    if (params._follow_input_size === true) return true;
    const w = Number(params.width), h = Number(params.height);
    if (!(w > 0 && h > 0)) return true;
    const find = name => ((entry.params_array || []).find(item => item.name === name) || {}).default;
    return STOCK_SIZES.has(w + 'x' + h) || (Number(find('width')) === w && Number(find('height')) === h);
  }

  const QWEN_DEFAULT_CHECKPOINT = 'qwen_image_2.1_int8_convrot.safetensors';

  function addInputNode(entityType, x, y, value, params) {
    // Image in / Video in were folded into one Media node; old graphs,
    // pasted groups and agent edits open as Media with value and wires kept.
    if (LEGACY_MEDIA_INPUTS.includes(entityType) &&
        (catalogue.entity_types_array || []).some(item => item.id === 'media')) entityType = 'media';
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
  function commitModelValue(hidden, value, notify) {
    if (!hidden) return;
    hidden.value = value;
    // Re-assert after picker policy/recommendation work. Production QA caught
    // the visible card changing while this request field stayed empty.
    if (notify) hidden.dispatchEvent(new Event('change', {bubbles:true}));
  }

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
          commitModelValue(hidden, value, false);
          if (reason && reason.materialized) {
            commitModelValue(hidden, value, true);
            return;
          }
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
          commitModelValue(hidden, value, true);
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
    if (node.kind === KIND_INPUT) return ['image', 'media'].includes(node.entityType) ? 'value' : '';
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

  /** Civitai pages and CDN links, resolved once per address. */
  const mediaResolveCache = new Map();
  function isCivitai(url) {
    try { return /(^|\.)civitai\.(com|red|green)$/i.test(new URL(url).hostname); } catch (error) { return false; }
  }
  function resolveMediaLink(url) {
    if (!isCivitai(url)) return Promise.resolve({url, type: looksLikeVideo(url) ? 'video' : 'image'});
    if (!mediaResolveCache.has(url)) {
      mediaResolveCache.set(url, fetch('/api/ai/media/resolve?url=' + encodeURIComponent(url))
        .then(async response => {
          const data = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error((data.detail && data.detail.message_string) || 'The link could not be resolved');
          const resolved = data.url_string || data.url || url;
          const type = data.type_string || data.type || (looksLikeVideo(resolved) ? 'video' : 'image');
          return {url: resolved, type: type === 'video' ? 'video' : 'image'};
        })
        .catch(error => { mediaResolveCache.delete(url); throw error; }));
    }
    return mediaResolveCache.get(url);
  }

  /** A clip's first frame, for a socket that takes only a picture. */
  const firstFrameCache = new Map();
  function firstFrameOf(videoUrl) {
    if (!firstFrameCache.has(videoUrl)) {
      const runner = RUNNERS.video_frame;
      firstFrameCache.set(videoUrl, submitJson(runner.api, {video_url: videoUrl, view: 'first_frame'})
        .then(accepted => runner.finish(accepted, runner, null))
        .then(finished => splitMulti(finished).value)
        .catch(error => { firstFrameCache.delete(videoUrl); throw error; }));
    }
    return firstFrameCache.get(videoUrl);
  }

  const CLIP_AS_PICTURE = new Set(['image', 'qwen_image']);

  function socketAcceptsVideo(serviceId, field) {
    if (CLIP_AS_PICTURE.has(serviceId)) return false;
    const entry = catalogue ? serviceById(serviceId) : null;
    const input = ((entry || {}).inputs || []).find(item => item.field === field);
    return !!input && (input.type === 'video' || (input.also_accepts || []).includes('video'));
  }

  /**
   * What a Media node hands one socket: a video consumer gets the clip, a
   * picture consumer gets the picture or, for a clip, its first frame.
   */
  async function adaptMediaValue(value, serviceId, field) {
    const text = String(value || '').trim();
    if (!text || text.startsWith('data:image/')) return text;
    let url = text;
    let kind = looksLikeVideo(text) ? 'video' : 'image';
    if (text.startsWith('data:video/')) kind = 'video';
    else if (isCivitai(text)) ({url, type: kind} = await resolveMediaLink(text));
    if (kind !== 'video' || socketAcceptsVideo(serviceId, field)) return url;
    if (url.startsWith('data:')) throw new Error('Upload the clip first; an inline video has no first frame yet');
    return firstFrameOf(url);
  }

  function wireMediaNode(id, element) {
    const file = element.querySelector('[data-file]');
    const pick = element.querySelector('[data-pick]');
    const picture = element.querySelector('[data-preview]');
    const clip = element.querySelector('[data-vpreview]');
    const text = element.querySelector('[data-value]');
    const status = element.querySelector('.nstate');
    const host = element.querySelector('.ninput');
    [picture, clip].forEach(item => {
      item.classList.add('preview-expandable');
      item.title = 'Click to enlarge';
      item.addEventListener('click', event => {
        event.stopPropagation();
        if (item.src) openPreview(item === clip ? 'video' : 'image', item.src);
      });
    });
    function show(url, kind) {
      const active = kind === 'video' ? clip : picture;
      const other = kind === 'video' ? picture : clip;
      other.hidden = true;
      if (other === clip) clip.pause();
      other.removeAttribute('src');
      if (!url) { active.hidden = true; active.removeAttribute('src'); return; }
      active.src = url;
      active.hidden = false;
      if (active === clip) clip.play().catch(() => {});
      markResolution(host, active);
    }
    let generation = 0;
    async function refresh() {
      const value = text.value.trim();
      const mine = ++generation;
      if (!/^(https?:\/\/|data:(image|video)\/)/.test(value)) { show('', 'image'); return; }
      if (value.startsWith('data:')) { show(value, value.startsWith('data:video/') ? 'video' : 'image'); return; }
      if (!isCivitai(value)) { show(value, looksLikeVideo(value) ? 'video' : 'image'); return; }
      status.textContent = 'resolving link…';
      status.className = 'nstate running';
      try {
        const resolved = await resolveMediaLink(value);
        if (mine !== generation) return;
        show(resolved.url, resolved.type);
        status.textContent = resolved.type === 'video' ? 'video · picture sockets get its first frame' : 'image';
        status.className = 'nstate done';
      } catch (error) {
        if (mine !== generation) return;
        show('', 'image');
        status.textContent = error.message;
        status.className = 'nstate failed';
      }
    }
    async function acceptMedia(chosen) {
      if (!chosen || !/^(image|video)\//.test(chosen.type || '')) return;
      const isVideo = chosen.type.startsWith('video/');
      const limit = (isVideo ? 100 : 12) * 1024 * 1024;
      if (chosen.size > limit) { toast(`${isVideo ? 'Videos' : 'Images'} must be at most ${isVideo ? 100 : 12} MB.`); return; }
      const mine = (element._uploadGeneration || 0) + 1;
      element._uploadGeneration = mine;
      generation += 1;
      status.textContent = `uploading ${isVideo ? 'video' : 'image'}...`;
      status.className = 'nstate running';
      text.value = '';
      if (element._previewObjectUrl) URL.revokeObjectURL(element._previewObjectUrl);
      element._previewObjectUrl = URL.createObjectURL(chosen);
      show(element._previewObjectUrl, isVideo ? 'video' : 'image');
      const form = new FormData();
      form.append('file', chosen, chosen.name || (isVideo ? 'input.mp4' : 'clipboard.png'));
      const upload = fetch('/dev/api/scratch', {method: 'POST', body: form})
        .then(async response => {
          const data = await response.json();
          if (!response.ok || !data.url) throw new Error(`The ${isVideo ? 'video' : 'image'} upload failed`);
          if (element._uploadGeneration !== mine) return;
          text.value = data.url;
          text.dispatchEvent(new Event('change', {bubbles: true}));
          status.textContent = `${isVideo ? 'video' : 'image'} ready`;
          status.className = 'nstate done';
        }).catch(error => {
          if (element._uploadGeneration !== mine) return;
          status.textContent = error.message;
          status.className = 'nstate failed';
        }).finally(() => {
          if (pendingImageUploads.get(String(id)) === upload) pendingImageUploads.delete(String(id));
        });
      pendingImageUploads.set(String(id), upload);
      await upload;
    }
    function acceptText(value) {
      const link = String(value || '').trim();
      if (!/^(https?:\/\/|data:(image|video)\/)/.test(link)) return false;
      text.value = link;
      text.dispatchEvent(new Event('change', {bubbles: true}));
      return true;
    }
    element._acceptImage = acceptMedia;
    element._acceptVideo = acceptMedia;
    element._acceptMedia = acceptMedia;
    element._acceptText = acceptText;
    element.tabIndex = 0;
    pick.addEventListener('click', () => file.click());
    file.addEventListener('change', event => acceptMedia(event.target.files[0]));
    element.addEventListener('dragover', event => event.preventDefault());
    element.addEventListener('drop', event => {
      event.preventDefault();
      const dropped = [...event.dataTransfer.files].find(item => /^(image|video)\//.test(item.type));
      if (dropped) { acceptMedia(dropped); return; }
      acceptText(event.dataTransfer.getData('text/uri-list') || event.dataTransfer.getData('text/plain'));
    });
    let timer = null;
    text.addEventListener('change', refresh);
    text.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(refresh, 250); });
    refresh();
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
    if (element && entityType === 'media') { wireMediaNode(id, element); return; }
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
    const cached = loraMenuCache.get(service);
    if (cached && Date.now() - cached.at > 60000) loraMenuCache.delete(service);
    if (!loraMenuCache.has(service)) {
      loraMenuCache.set(service, fetch('/api/ai/model-catalogue?service=' + encodeURIComponent(service || 'image'))
        .then(r => r.json()).then(body => (body.loras_array || []).filter(entry => entry.usable))
        .catch(() => []));
      loraMenuCache.get(service).at = Date.now();
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
      values._size_auto = meta(id).followInputSize;
    }
    if (meta(id)?.disabled) values._disabled = true;
    if (meta(id)?.x9) values._x9 = true;
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
      toast('Select a node first — B (or Ctrl+B) then takes it out of the run.');
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

  /* ------------------------------------------------------ branch isolation */

  /** Every node the given one reads from, itself included. */
  function upstreamOf(id) {
    const data = editor.export().drawflow.Home.data;
    const seen = new Set();
    const queue = [String(id)];
    while (queue.length) {
      const current = queue.pop();
      if (seen.has(current) || !data[current]) continue;
      seen.add(current);
      Object.values(data[current].inputs || {}).forEach(input =>
        (input.connections || []).forEach(connection => queue.push(String(connection.node))));
    }
    return seen;
  }

  function paintIsolation() {
    let banner = document.getElementById('isolation-banner');
    document.querySelectorAll('#canvas .niso').forEach(button =>
      button.classList.toggle('on', !!isolation && button.dataset.node === String(isolation.target)));
    if (!isolation) { if (banner) banner.remove(); return; }
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'isolation-banner';
      banner.style.cssText = 'position:absolute;bottom:14px;left:50%;transform:translateX(-50%);z-index:20;' +
        'background:#7c3aed;color:#fff;padding:6px 12px;border-radius:8px;font:600 13px system-ui;cursor:pointer;' +
        'box-shadow:0 2px 10px rgba(0,0,0,.35)';
      banner.title = 'Click, or press I (or Ctrl+I), to restore every node';
      banner.addEventListener('click', () => toggleIsolation());
      const host = document.getElementById('canvas');
      (host && host.parentElement ? host.parentElement : document.body).appendChild(banner);
    }
    const item = meta(isolation.target) || {};
    const element = nodeElement(isolation.target);
    const heading = element && element.querySelector('.nhead b');
    const name = item.label || (heading && heading.textContent) || ('node ' + isolation.target);
    banner.textContent = 'Isolated: ' + name + ' — I or Ctrl+I (or click) to restore';
  }

  function restoreIsolation(quiet) {
    if (!isolation) return false;
    Object.entries(isolation.prior || {}).forEach(([id, wasBypassed]) => {
      if (!meta(id) || !nodeElement(id)) return;
      if (isBypassed(id) !== !!wasBypassed) { applyBypass(id, !!wasBypassed); invalidateNodeAndDownstream(id); }
    });
    isolation = null;
    paintIsolation();
    if (!quiet) toast('Isolation lifted — every node is back as it was.');
    return true;
  }

  /**
   * Ctrl+I on a selected node: bypass everything that is not upstream of it,
   * so Render computes that branch only. Again (or the banner) restores each
   * node's own earlier state, including nodes that were bypassed before.
   */
  function toggleIsolation(targetId) {
    const target = targetId != null ? String(targetId) : null;
    if (isolation && (!target || target === String(isolation.target))) return restoreIsolation();
    if (!target || !meta(target) || !nodeElement(target)) {
      toast('Select a node first — I (or Ctrl+I) then isolates its branch.');
      return false;
    }
    if (isolation) restoreIsolation(true);
    const keep = upstreamOf(target);
    const prior = {};
    nodeMeta.forEach((item, id) => { if (item) prior[String(id)] = !!item.disabled; });
    Object.keys(prior).forEach(id => {
      if (!keep.has(id) && !prior[id]) { applyBypass(id, true); invalidateNodeAndDownstream(id); }
      // Everything the target needs runs, even if it was bypassed before;
      // restoring puts it back as it was.
      if (keep.has(id) && prior[id]) { applyBypass(id, false); invalidateNodeAndDownstream(id); }
    });
    isolation = {target, prior};
    paintIsolation();
    toast('Branch isolated: ' + keep.size + ' node(s) will run. I or Ctrl+I restores.');
    return true;
  }

  /** Ids selected on the canvas (the groups module owns selection). */
  function selectedIds() {
    if (nodeGroups && nodeGroups.selected && nodeGroups.selected.size) return [...nodeGroups.selected].map(String);
    const element = document.querySelector('#canvas .drawflow-node.selected');
    return element && element.id ? [element.id.replace(/^node-/, '')] : [];
  }

  function selectedNodeId() {
    const ids = selectedIds();
    if (ids.length === 1) return ids[0];
    const element = document.querySelector('#canvas .drawflow-node.selected');
    if (element && element.id) return element.id.replace(/^node-/, '');
    return editor && editor.node_selected && editor.node_selected.id
      ? String(editor.node_selected.id).replace(/^node-/, '') : null;
  }

  function addIsolateButton(id) {
    const element = nodeElement(id);
    const head = element && element.querySelector('.nhead');
    if (!head || head.querySelector('.niso')) return;
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'niso';
    button.dataset.node = String(id);
    button.textContent = '◎';
    button.title = 'Isolate this branch: run only what this node needs (I or Ctrl+I)';
    button.setAttribute('aria-label', 'Isolate branch');
    button.style.cssText = 'margin-left:4px;border:0;background:transparent;color:inherit;cursor:pointer;font-size:13px;padding:0 3px;opacity:.75';
    button.addEventListener('mousedown', event => event.stopPropagation());
    button.addEventListener('click', event => { event.stopPropagation(); toggleIsolation(id); });
    head.appendChild(button);
  }

  /**
   * Node hotkeys, on window in the capture phase so nothing on the page sees
   * them first. Plain letters work everywhere (no browser or extension owns
   * them): I isolate, B or M bypass. Ctrl+I / Ctrl+B / Ctrl+M / Ctrl+P too.
   * Ignored while typing.
   */
  const HOTKEYS = {isolate: 'I or Ctrl+I', bypass: 'B / M or Ctrl+B / Ctrl+P'};
  function typingIn(target) {
    return !!(target && ((/^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName || '') && !target.readOnly) ||
      target.isContentEditable || (target.closest && target.closest('dialog, .mpick-panel'))));
  }
  function hotkeyAction(event) {
    if (event.altKey || event.shiftKey) return null;
    const key = String(event.key || '').toLowerCase();
    const command = event.ctrlKey || event.metaKey;
    if (key === 'i') return 'isolate';
    if (key === 'b' || key === 'm') return 'bypass';
    if (key === 'p' && command) return 'bypass';
    return null;
  }
  window.addEventListener('keydown', event => {
    if (!document.getElementById('canvas')) return;
    const action = hotkeyAction(event);
    if (!action) return;
    if (typingIn(event.target) || typingIn(document.activeElement)) {
      // Ctrl+P must still never open the print dialog on this page.
      if (action === 'bypass' && (event.ctrlKey || event.metaKey)) event.preventDefault();
      return;
    }
    event.preventDefault();
    event.stopImmediatePropagation();
    if (action === 'isolate') {
      const id = selectedNodeId();
      if (isolation && (!id || id === String(isolation.target))) toggleIsolation();
      else toggleIsolation(id);
    } else {
      toggleBypass(selectedIds());
    }
    paintQuickbar();
  }, true);

  /* ------------------------------------------------------- media throttle */

  /**
   * A graph of 25 nodes holds dozens of looping clips and full-size pictures.
   * Only clips that are on screen and big enough to see play; pictures decode
   * lazily and off the main thread (2026-09-27, owner: node actions slow).
   */
  function installMediaThrottle(canvas) {
    if (!canvas || typeof IntersectionObserver === 'undefined') return;
    const visible = new WeakMap();
    const worth = video => {
      const rect = video.getBoundingClientRect();
      return visible.get(video) && rect.width >= 80 && !document.hidden;
    };
    const apply = video => {
      if (worth(video)) { if (video.paused) video.play().catch(() => {}); }
      else if (!video.paused) video.pause();
    };
    const io = new IntersectionObserver(entries => entries.forEach(entry => {
      visible.set(entry.target, entry.isIntersecting);
      apply(entry.target);
    }), {threshold: 0.2});
    const prep = element => {
      if (element.tagName === 'IMG') {
        if (element.loading !== 'lazy') element.loading = 'lazy';
        element.decoding = 'async';
      } else if (element.tagName === 'VIDEO' && !element._throttled) {
        element._throttled = true;
        element.preload = 'metadata';
        element.removeAttribute('autoplay');
        element.autoplay = false;
        // Code that calls play() on a new source must not wake a clip nobody sees.
        element.addEventListener('play', () => { if (!worth(element)) element.pause(); });
        io.observe(element);
      }
    };
    canvas.querySelectorAll('img, video').forEach(prep);
    new MutationObserver(records => records.forEach(record => record.addedNodes.forEach(node => {
      if (node.nodeType !== 1) return;
      if (node.tagName === 'IMG' || node.tagName === 'VIDEO') prep(node);
      if (node.querySelectorAll) node.querySelectorAll('img, video').forEach(prep);
    }))).observe(canvas, {childList: true, subtree: true});
    let timer = null;
    const recheck = () => { clearTimeout(timer); timer = setTimeout(() => canvas.querySelectorAll('video').forEach(apply), 200); };
    document.addEventListener('visibilitychange', recheck);
    if (editor && typeof editor.on === 'function') editor.on('zoom', recheck);
  }

  /* ------------------------------------------------------------------- X9 */

  /**
   * X9 (owner, 2026-09-27): one Render runs an image or video node with nine
   * seeds in parallel. The grid shows all nine; the cell last opened in the
   * lightbox is the node's output. An X9 node fed by an X9 node pairs them:
   * picture i -> clip i.
   */
  const X9_SERVICES = new Set(['image', 'qwen_image', 'video', 'video_control']);

  function addX9Button(id, on) {
    const element = nodeElement(id);
    const head = element && element.querySelector('.nhead');
    const item = meta(id);
    if (!head || !item || head.querySelector('.nx9')) return;
    item.x9 = !!on;
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'nx9';
    button.textContent = 'X9';
    button.title = 'X9: one Render = 9 seeds in parallel, shown as a 3×3 grid; the cell you open last is the output';
    const paint = () => {
      button.style.cssText = 'margin-left:4px;border:1px solid rgba(255,255,255,.25);border-radius:6px;cursor:pointer;' +
        'font:700 10px system-ui;padding:1px 5px;' + (item.x9 ? 'background:#f59e0b;color:#111;border-color:#f59e0b' : 'background:transparent;color:inherit;opacity:.7');
    };
    paint();
    ['mousedown', 'pointerdown', 'touchstart', 'dblclick'].forEach(type => button.addEventListener(type, event => event.stopPropagation()));
    button.addEventListener('click', event => {
      event.stopPropagation();
      item.x9 = !item.x9;
      paint();
      invalidateNodeAndDownstream(id);
      toast(item.x9 ? 'X9 on: the next Render makes 9 variants.' : 'X9 off.');
    });
    head.appendChild(button);
  }

  function x9Record(id) {
    const record = runState.get(String(id));
    return record && Array.isArray(record.x9) && record.x9.length ? record : null;
  }

  function x9Pick(record) {
    if (!record) return -1;
    if (record.pick >= 0 && record.x9[record.pick] && record.x9[record.pick].value) return record.pick;
    return record.x9.findIndex(cell => cell.status === 'done' && cell.value);
  }

  function paintX9(id) {
    const element = nodeElement(id);
    const record = x9Record(id);
    if (!element || !record) return;
    const host = element.querySelector('.nout');
    if (!host) return;
    const params = readParams(id);
    const w = Number(params.width) || 16, h = Number(params.height) || 9;
    const portrait = h > w;
    const pick = x9Pick(record);
    let grid = host.querySelector(':scope > .nx9grid');
    if (!grid) {
      host.innerHTML = '';
      grid = document.createElement('div');
      grid.className = 'nx9grid';
      host.appendChild(grid);
    }
    grid.style.cssText = 'display:grid;grid-template-columns:repeat(3,1fr);gap:3px;margin:0 auto;' +
      'width:100%;max-width:' + (portrait ? 220 : 330) + 'px';
    grid.innerHTML = '';
    record.x9.forEach((cell, index) => {
      const box = document.createElement('div');
      box.className = 'nx9cell';
      box.style.cssText = 'position:relative;aspect-ratio:' + w + '/' + h + ';border-radius:4px;overflow:hidden;cursor:pointer;' +
        'background:rgba(255,255,255,.06);outline:' + (index === pick ? '2px solid #f59e0b' : '1px solid rgba(255,255,255,.12)');
      box.title = 'Seed ' + cell.seed + ' · ' + cell.status + (cell.error ? ': ' + cell.error : '') + ' — click to open';
      if (cell.status === 'done' && cell.value) {
        const media = document.createElement(looksLikeVideo(cell.value) ? 'video' : 'img');
        media.src = cell.value;
        media.style.cssText = 'width:100%;height:100%;object-fit:cover;display:block';
        if (media.tagName === 'VIDEO') { media.muted = true; media.loop = true; media.preload = 'metadata'; media.playsInline = true; }
        else { media.loading = 'lazy'; media.decoding = 'async'; media.alt = 'Seed ' + cell.seed; }
        box.appendChild(media);
      } else {
        const label = document.createElement('span');
        label.textContent = cell.status === 'error' ? '⚠ error' : cell.status;
        label.style.cssText = 'position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:10px;opacity:.8;' +
          (cell.status === 'error' ? 'color:#fb7185' : '');
        box.appendChild(label);
      }
      const tag = document.createElement('i');
      tag.textContent = String(index + 1);
      tag.style.cssText = 'position:absolute;left:3px;top:2px;font:600 9px system-ui;font-style:normal;color:#fff;text-shadow:0 0 3px #000';
      box.appendChild(tag);
      ['mousedown', 'pointerdown'].forEach(type => box.addEventListener(type, event => event.stopPropagation()));
      box.addEventListener('click', event => { event.stopPropagation(); openX9Lightbox(id, index); });
      grid.appendChild(box);
    });
  }

  /** The chosen cell becomes the node's output; nodes downstream re-run on it. */
  function setX9Pick(id, index) {
    const record = x9Record(id);
    if (!record || !record.x9[index] || record.x9[index].status !== 'done') return;
    if (record.pick === index && record.value === record.x9[index].value) return;
    record.pick = index;
    record.value = record.x9[index].value;
    recordResult(id, record);
    continuableResults.set(String(id), {type: record.type, value: record.value, outputs: null, task_id: ''});
    paintX9(id);
    const graph = graphFromCanvas();
    // An X9 node downstream used all nine (cell i -> cell i), so a new pick
    // changes nothing for it; a normal node reads the pick and must re-run.
    graph.links.filter(link => String(link.from) === String(id) && !(meta(link.to) || {}).x9)
      .forEach(link => invalidateNodeAndDownstream(link.to));
  }

  function openX9Lightbox(id, start) {
    const record = x9Record(id);
    if (!record) return;
    let dialog = document.getElementById('x9-lightbox');
    if (!dialog) {
      dialog = document.createElement('dialog');
      dialog.id = 'x9-lightbox';
      dialog.style.cssText = 'max-width:96vw;max-height:96vh;padding:10px;border:0;border-radius:12px;background:#0d0e1c;color:#fff';
      dialog.innerHTML = '<div class="x9stage" style="display:flex;align-items:center;justify-content:center;min-width:300px;min-height:200px"></div>' +
        '<div style="display:flex;gap:8px;align-items:center;justify-content:center;margin-top:8px;font:13px system-ui">' +
        '<button type="button" data-x9="prev" title="Previous (←)">←</button><span class="x9cap"></span>' +
        '<button type="button" data-x9="next" title="Next (→)">→</button>' +
        '<button type="button" data-x9="use" title="Use this one as the node output">Use this</button>' +
        '<button type="button" data-x9="close" title="Close (Esc)">✕</button></div>';
      document.body.appendChild(dialog);
      dialog.addEventListener('click', event => {
        const action = event.target && event.target.dataset && event.target.dataset.x9;
        if (action === 'prev') dialog._show(dialog._index - 1);
        if (action === 'next') dialog._show(dialog._index + 1);
        if (action === 'use') { setX9Pick(dialog._node, dialog._index); toast('Cell ' + (dialog._index + 1) + ' is the output.'); }
        if (action === 'close' || event.target === dialog) dialog.close();
      });
      dialog.addEventListener('keydown', event => {
        if (event.key === 'ArrowLeft') { event.preventDefault(); dialog._show(dialog._index - 1); }
        if (event.key === 'ArrowRight') { event.preventDefault(); dialog._show(dialog._index + 1); }
      });
      dialog.addEventListener('close', () => { const clip = dialog.querySelector('video'); if (clip) clip.pause(); });
    }
    dialog._node = String(id);
    dialog._show = index => {
      const current = x9Record(dialog._node);
      if (!current) return;
      const count = current.x9.length;
      index = ((index % count) + count) % count;
      dialog._index = index;
      const cell = current.x9[index];
      const stage = dialog.querySelector('.x9stage');
      stage.innerHTML = '';
      if (cell.status === 'done' && cell.value) {
        const media = document.createElement(looksLikeVideo(cell.value) ? 'video' : 'img');
        media.src = cell.value;
        media.style.cssText = 'max-width:92vw;max-height:80vh;display:block';
        if (media.tagName === 'VIDEO') { media.controls = true; media.autoplay = true; media.loop = true; media.muted = true; }
        stage.appendChild(media);
        // The last cell looked at is the node's output (owner rule).
        setX9Pick(dialog._node, index);
      } else {
        stage.textContent = cell.status === 'error' ? ('Seed ' + cell.seed + ' failed: ' + cell.error) : ('Seed ' + cell.seed + ': ' + cell.status);
      }
      dialog.querySelector('.x9cap').textContent = (index + 1) + ' / ' + count + ' · seed ' + cell.seed +
        (x9Pick(current) === index ? ' · output' : '');
    };
    if (!dialog.open) dialog.showModal();
    dialog._show(start);
  }

  /** Nine seeds of one node, in parallel; `fan` pairs cell i with upstream cell i. */
  async function runX9(id, node, resolved, fan, params, epoch, keepDone) {
    const runner = runnerFor(node.service);
    const element = nodeElement(id);
    const state = element && element.querySelector('.nstate');
    const base = Number(params.seed) > 0 ? Number(params.seed) : Math.floor(Math.random() * 2147483000);
    const bodies = [];
    for (let index = 0; index < 9; index += 1) {
      const inputs = {...resolved};
      Object.keys(fan).forEach(field => { if (fan[field][index]) inputs[field] = fan[field][index]; });
      bodies.push(bodyFor(node.service, inputs, {...params, seed: base + index}));
    }
    const signature = stableJson({x9: bodies.map(body => ({...body, seed: Number(params.seed) > 0 ? body.seed : 0}))});
    const previous = x9Record(id);
    if (keepDone && previous && previous.x9sig === signature && previous.x9.every(cell => cell.status === 'done')) {
      paintX9(id);
      if (state) { state.textContent = 'continued · X9'; state.className = 'nstate done'; }
      return {type: previous.type, value: previous.value, x9: previous.x9.map(cell => cell.value)};
    }
    const cells = bodies.map(body => ({seed: body.seed, status: 'queued', value: '', error: ''}));
    const record = {status: 'running', type: runnerType(runner, ''), value: '', x9: cells,
                    pick: previous ? previous.pick : -1, x9sig: signature, started_at: Date.now() / 1000};
    runState.set(String(id), record);
    paintX9(id);
    const report = () => {
      if (epoch !== canvasEpoch || !nodeElement(id)) return;
      const done = cells.filter(cell => cell.status === 'done').length;
      const failed = cells.filter(cell => cell.status === 'error').length;
      const running = cells.filter(cell => cell.status === 'running').length;
      if (state) {
        state.textContent = 'X9 · ' + done + '/9 done' + (running ? ' · ' + running + ' rendering' : '') + (failed ? ' · ' + failed + ' failed' : '');
        state.className = 'nstate running';
      }
      paintX9(id);
    };
    await Promise.all(bodies.map(async (body, index) => {
      const cell = cells[index];
      const post = body._post_upscale;
      delete body._post_upscale;
      try {
        const accepted = await submitJson(runner.api, body);
        cell.status = 'running';
        report();
        let {value} = splitMulti(await runner.finish(accepted, runner, null));
        if (post && value) value = await upscaleClip2x(value, null);
        if (!value) throw new Error('no result');
        cell.value = value;
        cell.status = 'done';
        cell.type = runnerType(runner, value);
      } catch (error) {
        cell.status = 'error';
        cell.error = String(error.message || error).slice(0, 300);
      }
      report();
    }));
    const pick = x9Pick(record);
    if (pick < 0) {
      record.status = 'failed';
      record.error = 'all 9 seeds failed: ' + (cells[0].error || '');
      recordResult(id, record);
      if (state) { state.textContent = record.error; state.className = 'nstate failed'; }
      throw new Error(record.error);
    }
    record.status = 'done';
    record.pick = pick;
    record.value = cells[pick].value;
    record.type = runnerType(runner, record.value);
    recordResult(id, record);
    paintX9(id);
    if (state) {
      const failed = cells.filter(cell => cell.status === 'error').length;
      state.textContent = 'done · X9' + (failed ? ' (' + failed + ' failed)' : '') + ' — click a cell to view / choose';
      state.className = 'nstate done';
    }
    return {type: record.type, value: record.value, x9: cells.map(cell => cell.value)};
  }

  /* ------------------------------------------------------- post to Civitai */

  /**
   * "Post to Civitai" on a node's output (owner only, 2026-09-27). The server
   * posts a picture with its own token (draft by default); a clip, music, or
   * any refusal comes back as a manual path: download, copy, open the page.
   */
  let civitaiAdmin = null;
  function civitaiIsAdmin() {
    if (civitaiAdmin === null) {
      civitaiAdmin = fetch('/auth/me', {credentials: 'same-origin'}).then(r => r.ok ? r.json() : null)
        .then(data => !!(data && data.user && data.user.is_admin)).catch(() => false);
    }
    return civitaiAdmin;
  }

  function nodePromptText(id) {
    const graph = graphFromCanvas();
    const link = graph.links.find(item => String(item.to) === String(id) && item.input === 'prompt');
    if (!link) return '';
    const from = graph.nodes.find(node => String(node.id) === String(link.from));
    if (from && from.kind === KIND_INPUT) return String(from.value || '');
    const record = runState.get(String(link.from));
    return record && record.value ? String(record.value) : '';
  }

  async function nodeResources(id) {
    const params = readParams(id);
    const item = meta(id) || {};
    const files = [params.checkpoint, params.lora].filter(Boolean).map(String);
    String(params.loras || '').replace(/<lora:([^:>]+)/g, (_, name) => { files.push(name.trim()); return ''; });
    // Curated models the catalogue does not tag with a Civitai version.
    const KNOWN = {
      'z_image_turbo_fp8_e4m3fn.safetensors': [2442439, 'Z-Image Turbo'],
      'krea2_turbo_fp8_scaled.safetensors': [3091481, 'Krea 2 Turbo'],
      'qwen-image-2512-Q3_K_S.gguf': [2552908, 'Qwen-Image-2512'],
      'minimax_h3_fl2va_pruned_int8_convrot.safetensors': [3216500, 'MiniMax H3']
    };
    const known = files.filter(file => KNOWN[file]).map(file => ({model_version_id: KNOWN[file][0], name: KNOWN[file][1]}));
    if (!files.length || !window.AIEntities) return known;
    try {
      const data = await window.AIEntities.loadModels(item.service || 'image');
      const all = [].concat((data && data.checkpoints_array) || [], (data && data.loras_array) || []);
      const found = files.map(file => all.find(entry => entry.file === file || (entry.file || '').replace(/\.safetensors$/, '') === file))
        .filter(entry => entry && entry.source_version_id)
        .map(entry => ({model_version_id: Number(entry.source_version_id), name: entry.title || entry.file}));
      return known.concat(found.filter(item => !known.some(k => k.model_version_id === item.model_version_id)));
    } catch (error) { return known; }
  }

  async function openCivitaiDialog(id) {
    const record = runState.get(String(id));
    const url = record && record.value;
    if (!url || !/^https?:/.test(url)) { toast('Render the node first.'); return; }
    const item = meta(id) || {};
    const params = readParams(id);
    const resources = await nodeResources(id);
    const prompt = nodePromptText(id);
    const risky = /porn|nsfw|xxx|hentai|nude|lewd/i.test([params.checkpoint, params.lora, params.loras, prompt].join(' '));
    let dialog = document.getElementById('civitai-post');
    if (dialog) dialog.remove();
    dialog = document.createElement('dialog');
    dialog.id = 'civitai-post';
    dialog.className = 'civ-dialog';
    const esc = value => escapeHtml(String(value || ''));
    dialog.innerHTML = `<form method="dialog" style="display:grid;gap:8px">
      <b style="font-size:15px">Post to Civitai (NoDeadLine)</b>
      <label>Title<input name="title" style="width:100%" value="${esc(item.label || (serviceById(item.service) || {}).title || '')}"></label>
      <label>Description<textarea name="description" rows="4" style="width:100%">${esc(prompt)}</textarea></label>
      <label>Tags (comma separated)<input name="tags" style="width:100%" value="autorig, ${esc(item.service || '')}"></label>
      <label>Rating (required)<select name="rating" required>
        <option value="">— choose —</option><option${risky ? '' : ' selected'}>None</option><option>Soft</option><option>Mature</option><option${risky ? ' selected' : ''}>X</option></select></label>
      <label><input type="checkbox" name="confirm" required> I checked the rating (suggested from the model/LoRA and prompt)</label>
      <div>Resources: ${resources.length ? resources.map(r => `<a href="https://civitai.red/model-versions/${r.model_version_id}" target="_blank" rel="noopener">${esc(r.name)}</a>`).join(', ') : '<i>none detected</i>'}</div>
      <label><input type="radio" name="publish" value="draft" checked> Save as draft (recommended)</label>
      <label><input type="radio" name="publish" value="publish"> Publish now</label>
      <div class="civ-out" style="white-space:pre-wrap"></div>
      <div style="display:flex;gap:8px;justify-content:flex-end"><button value="cancel">Close</button><button type="button" class="civ-go">Post</button></div></form>`;
    document.body.appendChild(dialog);
    ['mousedown', 'pointerdown', 'keydown'].forEach(type => dialog.addEventListener(type, event => event.stopPropagation()));
    const form = dialog.querySelector('form');
    const out = dialog.querySelector('.civ-out');
    dialog.querySelector('.civ-go').addEventListener('click', async () => {
      if (!form.rating.value || !form.confirm.checked) { out.textContent = 'Choose the rating and confirm it.'; return; }
      const publish = form.publish.value === 'publish';
      if (publish && !window.confirm('Publish this publicly on Civitai now?')) return;
      out.textContent = 'Posting…';
      const body = {media_url: url, title: form.title.value, description: form.description.value, prompt,
        tags: form.tags.value.split(',').map(tag => tag.trim()).filter(Boolean), nsfw_level: form.rating.value,
        resources: resources.map(r => ({model_version_id: r.model_version_id, name: r.name})), publish};
      try {
        const response = await fetch('/api/ai/civitai/post', {method: 'POST', credentials: 'same-origin',
          headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)});
        const data = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error((data.detail && (data.detail.message_string || data.detail)) || ('HTTP ' + response.status));
        if (data.success_bool && data.post_url_string) {
          out.innerHTML = (data.warning_string ? '<b style="color:#fb7185">' + esc(data.warning_string) + '</b><br>' : '') +
            (data.draft_bool ? 'Draft saved: ' : 'Posted: ') + `<a href="${esc(data.post_url_string)}" target="_blank" rel="noopener">${esc(data.post_url_string)}</a>`;
          const current = runState.get(String(id));
          if (current) { current.civitai_url = data.post_url_string; recordResult(id, current); }
        } else {
          out.innerHTML = esc(data.reason_string || 'Post by hand:') + `<br><a href="${esc(data.download_url_string)}" target="_blank" rel="noopener" download>Download the file</a> · ` +
            `<a href="${esc(data.open_url_string)}" target="_blank" rel="noopener">Open Civitai's post page</a> · <button type="button" class="civ-copy">Copy details</button>`;
          const copy = out.querySelector('.civ-copy');
          if (copy) copy.addEventListener('click', () => copyText(data.details_string || '').then(() => toast('Details copied.')));
        }
      } catch (error) { out.textContent = 'Failed: ' + error.message; }
    });
    dialog.showModal();
  }

  function attachCivitaiButton(host) {
    if (!host || host.querySelector(':scope > .civ-btn')) return;
    const node = host.closest('.drawflow-node');
    if (!node) return;
    civitaiIsAdmin().then(admin => {
      if (!admin || host.querySelector(':scope > .civ-btn')) return;
      if (!host.querySelector('img, video, audio, .nx9grid')) return;
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'civ-btn';
      button.textContent = 'C↑';
      button.title = 'Post to Civitai (NoDeadLine) — draft by default';
      button.style.cssText = 'position:absolute;top:4px;right:4px;z-index:3;font:700 10px system-ui;padding:2px 5px;border-radius:6px;' +
        'border:1px solid rgba(255,255,255,.3);background:rgba(10,12,30,.8);color:#7dd3fc;cursor:pointer';
      ['mousedown', 'pointerdown', 'dblclick'].forEach(type => button.addEventListener(type, event => event.stopPropagation()));
      button.addEventListener('click', event => { event.stopPropagation(); openCivitaiDialog(node.id.replace(/^node-/, '')); });
      if (getComputedStyle(host).position === 'static') host.style.position = 'relative';
      host.appendChild(button);
    });
  }
  if (typeof MutationObserver !== 'undefined') {
    new MutationObserver(records => records.forEach(record => record.addedNodes.forEach(added => {
      if (added.nodeType !== 1) return;
      const host = added.classList && added.classList.contains('nout') ? added : (added.closest && added.closest('.nout'));
      if (host) attachCivitaiButton(host);
      if (added.querySelectorAll) added.querySelectorAll('.nout').forEach(attachCivitaiButton);
    }))).observe(document.documentElement, {childList: true, subtree: true});
  }

  /* ---------------------------------------------------------- quick toolbar */

  let quickbar = null;
  function quickButton(glyph, title, onClick) {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = glyph;
    button.title = title;
    button.setAttribute('aria-label', title);
    button.style.cssText = 'min-width:34px;height:30px;border:0;border-radius:7px;background:transparent;color:inherit;' +
      'font-size:16px;cursor:pointer;padding:0 6px';
    ['mousedown', 'pointerdown', 'touchstart', 'dblclick'].forEach(type =>
      button.addEventListener(type, event => { event.stopPropagation(); if (type !== 'touchstart') event.preventDefault(); }));
    button.addEventListener('click', event => { event.stopPropagation(); event.preventDefault(); onClick(); paintQuickbar(); });
    return button;
  }
  function ensureQuickbar() {
    if (quickbar) return quickbar;
    quickbar = document.createElement('div');
    quickbar.id = 'node-quickbar';
    quickbar.style.cssText = 'position:fixed;z-index:40;display:none;gap:2px;padding:3px;border-radius:10px;' +
      'background:#1f1b33;color:#fff;box-shadow:0 4px 14px rgba(0,0,0,.45);border:1px solid rgba(255,255,255,.12)';
    const id = () => selectedNodeId();
    quickbar._isolate = quickButton('◎', 'Isolate this branch / restore (' + HOTKEYS.isolate + ')', () => {
      const target = id();
      if (isolation && String(isolation.target) === String(target)) toggleIsolation(); else toggleIsolation(target);
    });
    quickbar._bypass = quickButton('⏻', 'Enable / disable (bypass) this node (' + HOTKEYS.bypass + ')', () => toggleBypass([id()]));
    quickbar._run = quickButton('▶', 'Run this branch: isolate it and render (keeps finished results)', () => {
      const target = id();
      if (!target) return;
      if (!isolation || String(isolation.target) !== String(target)) toggleIsolation(target);
      runGraph(true);
    });
    quickbar.append(quickbar._isolate, quickbar._bypass, quickbar._run);
    document.body.appendChild(quickbar);
    return quickbar;
  }
  function paintQuickbar() {
    const bar = ensureQuickbar();
    const ids = selectedIds();
    const element = ids.length === 1 ? nodeElement(ids[0]) : null;
    if (!element || !meta(ids[0])) { bar.style.display = 'none'; return; }
    const rect = element.getBoundingClientRect();
    const canvasRect = document.getElementById('canvas').getBoundingClientRect();
    if (rect.bottom < canvasRect.top || rect.top > canvasRect.bottom || rect.right < canvasRect.left || rect.left > canvasRect.right) {
      bar.style.display = 'none'; return;
    }
    const isService = meta(ids[0]).kind === KIND_SERVICE;
    bar._isolate.style.display = isService ? '' : 'none';
    bar._run.style.display = isService ? '' : 'none';
    const isolatedHere = !!isolation && String(isolation.target) === String(ids[0]);
    bar._isolate.style.background = isolatedHere ? '#7c3aed' : 'transparent';
    bar._isolate.title = (isolatedHere ? 'Restore every node' : 'Isolate this branch') + ' (' + HOTKEYS.isolate + ')';
    const off = isBypassed(ids[0]);
    bar._bypass.style.background = off ? '#6b7280' : 'transparent';
    bar._bypass.title = (off ? 'Enable this node' : 'Disable (bypass) this node') + ' (' + HOTKEYS.bypass + ')';
    bar.style.display = 'flex';
    const width = bar.offsetWidth || 110;
    bar.style.left = Math.max(canvasRect.left + 4, Math.min(rect.left + rect.width / 2 - width / 2, canvasRect.right - width - 4)) + 'px';
    bar.style.top = Math.max(canvasRect.top + 4, rect.top - 40) + 'px';
  }
  setInterval(() => {
    if (typeof editor === 'undefined' || !editor) return;
    if (!quickbar && !selectedIds().length) return;
    if (quickbar && quickbar.style.display === 'none' && !selectedIds().length) return;
    paintQuickbar();
  }, 150);

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
  /** Same type, listed in also_accepts, or a Media node into a picture/clip socket. */
  function typeFits(info) {
    if (!info || !info.produced) return false;
    if (info.produced === info.accepted || info.alsoAccepts.includes(info.produced)) return true;
    // A control map is a plain PNG: any picture socket takes it as a reference
    // (bodyFor tells the model what it is). Native control sockets are typed.
    if (info.produced.startsWith('control_') &&
        (info.accepted === 'image' || info.alsoAccepts.includes('image'))) return true;
    return info.produced === 'media' && (['image', 'video'].includes(info.accepted) ||
      info.alsoAccepts.includes('image') || info.alsoAccepts.includes('video'));
  }

  function linkAllowed(connection) {
    if (String(connection.output_id) === String(connection.input_id)) return false;
    if ((meta(connection.input_id) || {}).kind !== KIND_SERVICE) return false;
    const info = linkTypes(connection);
    if (!info || !info.produced) return false;
    if (!typeFits(info)) return false;
    if (info.produced.startsWith('control_') && String(info.accepted || '').startsWith('control_')) {
      const element = nodeElement(connection.input_id);
      const slot = element && element.querySelector('[data-model-param="checkpoint"]');
      const entry = slot && slot._picker && slot._picker.entry;
      const sourceService = serviceById((meta(connection.output_id) || {}).service || info.produced);
      const ok = controlChannelAccepted(info.produced.slice(8), entry, sourceService);
      const socket = element && element.querySelector('.inputs .' + connection.input_class);
      if (socket) {
        if (socket.dataset.typeTitle === undefined) socket.dataset.typeTitle = socket.title || '';
        socket.title = ok ? socket.dataset.typeTitle : ((entry && (entry.title || entry.file)) || 'This model') +
          ': no native ' + info.produced.slice(8) + ' ControlNet. Click this socket to switch the model to one ' +
          'that has it (Z-Image Turbo), or drop the map on a picture socket to use it as a reference image.';
        if (ok) delete socket.dataset.switchTo; else socket.dataset.switchTo = info.produced.slice(8);
      }
      return ok;
    }
    return true;
  }

  function onConnectionCreated(connection) {
    const info = linkTypes(connection);
    if (typeFits(info)) {
      if (info.produced.startsWith('control_') && String(info.inField || '').startsWith('control_')) {
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
    avatar_from_image: { api: '/api/ai/avatar-from-image', finish: pollAvatarStatus, field: 'avatar_string', type: 'avatar' },
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
    // Picture or clip in, the same x2 out; the type follows what came back.
    upscale2x: { api: '/api/upscale2x', finish: async (accepted, runner, report) => {
      const value = await pollForFile(accepted, runner, report);
      return {value, outputs: looksLikeVideo(value) ? {video_url_string: value} : {image_url_string: value}};
    }, field: 'output_url_string', type: 'auto' },
    detail_enhance: { api: '/api/detail', finish: pollForFile, field: 'image_url_string', type: 'image' },
    face_fix: { api: '/api/facefix', finish: pollForFile, field: 'image_url_string', type: 'image' },
    // Draws or rewrites depending on whether a picture is wired in; the
    // endpoint reads the wiring, so the runner is the ordinary picture shape.
    qwen_image: { api: '/api/qwen-image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    '3dmodel': { api: '/api/3dmodel', finish: poll3dStatus, field: 'model_url_string', type: 'model3d' },
    // Stable Audio 3 (2026-09-26): the audio file, and for a clip the clip
    // with the music under it (muxed by the server once the audio exists).
    music: { api: '/api/music', finish: async (accepted, runner, report) => {
      const value = await pollForFile(accepted, runner, report);
      const outputs = {audio_url_string: value};
      if (accepted.prompt_string) outputs.prompt_string = String(accepted.prompt_string);
      if (accepted.video_url_string) {
        const clip = String(accepted.video_url_string);
        for (let attempt = 0; attempt < 60; attempt++) {
          const probe = await fetch(clip, { method: 'HEAD' }).catch(() => null);
          if (probe && probe.ok) { outputs.video_url_string = clip; break; }
          await sleep(3000);
        }
      }
      return {value, outputs};
    }, field: 'audio_url_string', type: 'audio' }
  };
  ['pose', 'depth', 'canny'].forEach(channel => {
    RUNNERS['control_' + channel] = { api: '/api/controlnet', finish: pollForFile,
      field: 'image_url_string', type: 'control_' + channel };
  });

  /** A runner's result type; 'auto' reads it off the address (picture or clip). */
  function runnerType(runner, value) {
    if (!runner || runner.type !== 'auto') return runner ? runner.type : 'image';
    return looksLikeVideo(value) ? 'video' : 'image';
  }

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

  function avatarStatusUrl(raw, taskId) {
    const fallback = '/api/ai/avatar-from-image/status/' + encodeURIComponent(taskId || '');
    const parsed = new URL(raw || fallback, location.origin);
    if (parsed.origin !== location.origin ||
        !parsed.pathname.startsWith('/api/ai/avatar-from-image/status/')) {
      throw new Error('Avatar creation returned an unsafe status address.');
    }
    return parsed.pathname + parsed.search;
  }

  async function pollAvatarStatus(accepted, runner, report) {
    if (accepted[runner.field]) return accepted[runner.field];
    if (accepted.finished_bool) {
      throw new Error(accepted.error_string || 'Avatar creation finished without a saved profile.');
    }
    let url = avatarStatusUrl(accepted.status_url_string, accepted.task_id_string);
    let retrySeconds = Number(accepted.retry_after_seconds_float) || 2;
    for (let attempt = 0; attempt < 480; attempt++) {
      await sleep(Math.max(0.5, Math.min(10, retrySeconds)) * 1000);
      let response;
      try { response = await fetch(url); } catch (_) { continue; }
      if (response.status >= 500) continue;
      const parsed = await readJsonResponse(response);
      if (!response.ok) {
        if (parsed.data) throw new Error(describeError(parsed.data, response.status));
        throw new Error(nonJsonHttpError(response, parsed.text));
      }
      const data = parsed.data;
      if (!data || typeof data !== 'object') continue;
      if (report) report(data);
      if (data.status_url_string) url = avatarStatusUrl(data.status_url_string, accepted.task_id_string);
      retrySeconds = Number(data.retry_after_seconds_float) || 2;
      if (!data.finished_bool) continue;
      if (data.success_bool === false || String(data.status_string).toLowerCase() === 'failed') {
        throw new Error(data.error_string || 'Avatar creation failed.');
      }
      if (data[runner.field]) return data[runner.field];
      throw new Error('Avatar creation finished without a saved profile.');
    }
    throw new Error('Avatar creation did not arrive in time');
  }

  async function pollForFile(accepted, runner, report) {
    const url = accepted[runner.field];
    if (!url) throw new Error('the farm accepted the job without an output address');
    // The farm publishes where the file will be before it exists, so the file
    // appearing is the completion signal. Video is allowed half an hour.
    let missing = 0;
    for (let attempt = 0; attempt < 800; attempt++) {
      if (accepted.task_id_string) {
        const status = await fetch('/api/ai/render-status/' + encodeURIComponent(accepted.task_id_string))
          .then(response => response.status === 404 ? {gone: true} : (response.ok ? response.json() : null)).catch(() => null);
        // The farm no longer knows the task (cancelled and purged, or from an
        // old session): stop waiting unless the file is already there.
        if (status && status.gone) {
          missing += 1;
          if (missing >= 3) {
            const landed = await fetch(url, { method: 'HEAD' }).catch(() => null);
            if (landed && landed.ok) return url;
            throw new Error('the farm no longer has this task (cancelled or expired) — Render again');
          }
          await sleep(2500);
          continue;
        }
        missing = 0;
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

  /** Whether this socket was declared to take a clip as well as a picture. */
  function socketTakesVideo(serviceId, field) {
    const entry = catalogue ? serviceById(serviceId) : null;
    const input = ((entry || {}).inputs || []).find(item => item.field === field);
    return !!input && (input.also_accepts || []).includes('video');
  }

  /** What arrived, judged by the address the farm published it at. */
  function looksLikeVideo(value) {
    const text = String(value || '');
    if (text.startsWith('data:')) return text.slice(5, 25).startsWith('video/');
    return /\.(mp4|webm|mov|m4v)(\?|#|$)/i.test(text);
  }

  /** Auto-size nodes take the size of what actually arrived on the primary socket. */
  async function followInputSizeAtRun(node, resolved, params) {
    if (!params || params.width == null || params.height == null) return;
    if (params._follow_input_size === false || params._size_auto === false) return;
    if (!nodeGroups || !nodeGroups.mediaDimensions) return;
    const order = ['image', 'image_url_end', 'video_url', 'control_video_url', 'source', 'source_url'];
    let source = order.map(field => resolved[field]).find(value => typeof value === 'string' && /^(https?:|data:)/.test(value));
    if (!source) source = Object.keys(resolved).filter(field => !/^control_(pose|depth|canny)$/.test(field))
      .map(field => resolved[field]).find(value => typeof value === 'string' && /^(https?:|data:)/.test(value) &&
        /\.(png|jpe?g|webp|gif|mp4|webm|mov|m4v)(\?|#|$)|^data:(image|video)\//i.test(value));
    if (!source) return;
    try {
      const size = nodeGroups.fitDimensions(await nodeGroups.mediaDimensions(source), node.service);
      if (size) { params.width = size.width; params.height = size.height; }
    } catch (error) { /* keep the canvas size */ }
  }

  const HALF_HD_LONG = 960;
  const VIDEO_SAMPLERS = new Set(['video', 'video_control', 'avatar_video']);

  /** Clip at half-HD -> x2 through /api/upscale2x (the post step of a big target). */
  async function upscaleClip2x(url, state) {
    if (state) { state.textContent = 'upscaling 2× (half-HD render → target size)…'; state.className = 'nstate running'; }
    const accepted = await submitJson('/api/upscale2x', {video_url: url});
    return pollForFile(accepted, {field: 'output_url_string', type: 'video'}, null);
  }

  const MAP_RULES = {
    pose: 'an OpenPose skeleton map: pose the person exactly like it, same place and size in the frame',
    depth: 'a depth map (near = white) of the layout only: keep its perspective and every object\'s place',
    canny: 'an edge map of the layout only: follow its edges for the room, furniture and framing',
    normal: 'a grey shaded render of the scene geometry: use it only for shapes, layout and surface orientation, not for colours or lighting'
  };
  const MAP_PLACEHOLDER = ' Any person in the map is only a placeholder: draw the character from the other picture in that place instead.';
  /** Where the grey-shaded copy of a farm-made normal map is served. */
  function shadedNormalUrl(url) {
    const match = /\/renderfin\/render\/[^/]+\/([0-9a-f-]{36})\.png(?:[?#]|$)/i.exec(String(url || ''));
    return match ? location.origin + '/api/ai/normal-shade/' + match[1] + '.png' : '';
  }
  function mapChannels() {
    const channels = new Map();
    runState.forEach(record => {
      const type = String((record && record.type) || '');
      if (type.startsWith('control_') && record.value) channels.set(String(record.value), type.slice(8));
    });
    return channels;
  }
  /**
   * Qwen-Image copies picture 1 when it is an edge map and keeps the
   * person drawn in it (2026-09-27). The maps therefore go after the ordinary
   * pictures, and "image N" in the prompt is renumbered to match.
   */
  function mapsLast(serviceId, resolved) {
    if (serviceId !== 'qwen_image' || !resolved) return resolved;
    const channels = mapChannels();
    const fields = ['image', 'reference_2', 'reference_3'].filter(field => resolved[field]);
    // Measured: only an edge map needs to go last; a pose map and the shaded
    // normal work best as picture 1, where they also set the framing.
    const isMap = field => channels.get(String(resolved[field])) === 'canny';
    if (!fields.some(isMap) || fields.every(isMap)) return resolved;
    const ordered = fields.filter(field => !isMap(field)).concat(fields.filter(isMap));
    if (ordered.every((field, index) => field === fields[index])) return resolved;
    const out = Object.assign({}, resolved);
    const renumber = {};
    ['image', 'reference_2', 'reference_3'].forEach(field => delete out[field]);
    ordered.forEach((field, index) => {
      out[index === 0 ? 'image' : 'reference_' + (index + 1)] = resolved[field];
      renumber[fields.indexOf(field) + 1] = index + 1;
    });
    if (typeof out.prompt === 'string') {
      out.prompt = out.prompt.replace(/\b(image|picture)\s+([1-3])\b/gi,
        (whole, word, n) => word + ' ' + (renumber[n] || n));
    }
    return out;
  }
  /** "Image N is a pose map: ..." for every control map among the pictures. */
  function controlMapHints(resolved) {
    const channels = mapChannels();
    if (!channels.size) return '';
    const hints = [];
    Object.keys(resolved || {}).forEach(field => {
      const match = field === 'image' ? ['', '1'] : /^reference_(\d+)$/.exec(field);
      const channel = match && channels.get(String(resolved[field]));
      if (channel && MAP_RULES[channel]) {
        hints.push('Image ' + match[1] + ' is ' + MAP_RULES[channel] + '; do not draw the map itself.' +
                   (channel === 'pose' ? '' : MAP_PLACEHOLDER));
      }
    });
    return hints.join(' ');
  }

  function bodyFor(serviceId, resolved, params) {
    const body = {};
    // An empty LoRA slot carries a strength and nothing to apply it to.
    if (params && !String(params.lora || '').trim() && 'lora_strength' in params) {
      params = Object.assign({}, params);
      delete params.lora_strength;
    }
    resolved = mapsLast(serviceId, resolved);
    const mapHints = controlMapHints(resolved);
    // A normal map travels as its grey shaded copy: the RGB leaks into renders.
    if (mapHints) {
      const channels = mapChannels();
      resolved = Object.assign({}, resolved);
      Object.keys(resolved).forEach(field => {
        if ((field === 'image' || /^reference_\d+$/.test(field)) &&
            channels.get(String(resolved[field])) === 'normal') {
          resolved[field] = shadedNormalUrl(resolved[field]) || resolved[field];
        }
      });
    }
    if (serviceId === 'video' && resolved && !resolved.image && resolved.image_url_end) {
      resolved = Object.assign({}, resolved, {image: resolved.image_url_end});
      delete resolved.image_url_end;
    }
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
        // A socket that declares both kinds routes by what arrived: the same
        // wire carries a picture to `image_url` and a clip to `video_url`.
        // Read off the value, not off a type passed alongside, so a retry and
        // a graph reopened from a link behave the same as the first run.
        // Qwen-Image and Image have no video_url field: a clip goes as image_url and
        // the server reads its first frame (sent as video_url it was dropped, and
        // the edit failed "needs a picture", 2026-09-27).
        if (field === 'image' && !CLIP_AS_PICTURE.has(serviceId) && socketTakesVideo(serviceId, 'image') && looksLikeVideo(value)) {
          body.video_url = value;
          return;
        }
        const key = field === 'image' ? 'image_url' : 'image_url_end';
        const inline = field === 'image' ? 'image_base64' : 'image_base64_end';
        if (String(value).startsWith('data:')) body[inline] = value; else body[key] = value;
      } else {
        body[field] = value;
      }
    });
    // A control map wired into a picture socket is a reference, not a photo:
    // say which picture it is and what to take from it.
    if (mapHints) {
      if (typeof body.prompt === 'string' && body.prompt.trim()) body.prompt = body.prompt.trim() + ' ' + mapHints;
      else if (serviceId === 'vision' || serviceId === 'text') body._map_hint = mapHints;
    }
    // A node that carries a standing instruction always asks for the answer
    // alone: one JSON object in, `output_text` out. The instruction travels as
    // its own field so it never lands in the text the next node reads, and it
    // is part of the request signature, so editing it re-runs the node.
    const declaration = catalogue ? serviceById(serviceId) : null;
    if (declaration && declaration.system_prompt_capable) {
      const standing = (params || {})._system_prompt;
      const text = String(typeof standing === 'string'
        ? standing : (declaration.system_prompt_default || ''));
      const withHint = [text.trim(), body._map_hint || ''].filter(Boolean).join(' ');
      if (withHint) body.system_prompt = withHint;
      // Image and Video read their standing instruction only when the text is
      // empty; they are not answer-writing services.
      if (!['image', 'video'].includes(serviceId)) body.structured = true;
    }
    delete body._map_hint;
    // A LoRA of another model family (left in a slot when the checkpoint
    // changed) is left out, so the render runs with the ones that fit; the
    // slot says so inline (ai-node-lora-stack.js).
    if (typeof window !== 'undefined' && window.AINodeLoraStack && window.AINodeLoraStack.filterBody) {
      window.AINodeLoraStack.filterBody(serviceId, body);
    }
    // A service that takes the quality itself (the Avatar builder sizes its
    // own views) gets the graph's unless its node picked one.
    if (declaration && (declaration.params_array || []).some(item => item.name === 'render_quality') &&
        !body.render_quality) body.render_quality = renderQuality;
    // Render quality: every width/height scaled, rounded and clamped here, so
    // the scaled size is what the signature records and what the server gets.
    if (typeof window !== 'undefined' && window.AIRenderQuality && renderQuality !== 'normal') {
      window.AIRenderQuality.applyToBody(serviceId, body, renderQuality, declaration);
    }
    // Owner rule: video samples within half-HD at every quality. A larger
    // target (Full with a big manual size, 2x) renders at half-HD and is then
    // enlarged 2x by the Upscale 2x service; 1280x1920x297 frames ran a 24 GB
    // card out of memory (2026-09-27).
    if (VIDEO_SAMPLERS.has(serviceId) && Number(body.width) > 0 && Number(body.height) > 0) {
      const w = Number(body.width), h = Number(body.height);
      if (Math.max(w, h) > HALF_HD_LONG) {
        const s = HALF_HD_LONG / Math.max(w, h);
        body.width = Math.max(256, Math.round(w * s / 32) * 32);
        body.height = Math.max(256, Math.round(h * s / 32) * 32);
        body._post_upscale = 2;
      }
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
      const postUpscale = submitBody._post_upscale;
      delete submitBody._post_upscale;
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
          status: 'running', type: runnerType(runner, accepted[runner.field] || ''),
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
        if (postUpscale && value) {
          value = await upscaleClip2x(value, executionIsCurrent(execution) ? state : null);
          if (outputs && outputs.video_url_string) outputs.video_url_string = value;
        }
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
        showResult(outBox, runnerType(runner, value), value, outputs);
        recordResult(id, Object.assign({ status: 'done', type: runnerType(runner, value), value: value,
                           input_reference_url: execution.inputReference || '',
                           task_id: accepted.task_id_string || '' }, outputs ? {outputs} : {}));
      }
      return outputs ? { type: runnerType(runner, value), value: value, outputs } : { type: runnerType(runner, value), value: value };
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
    } else if (type === 'avatar') {
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
    } else if (type === 'audio') {
      const player = document.createElement('audio');
      player.src = value;
      player.controls = true;
      player.preload = 'metadata';
      player.className = 'naudio';
      player.style.width = '100%';
      player.addEventListener('click', event => event.stopPropagation());
      player.addEventListener('mousedown', event => event.stopPropagation());
      host.appendChild(player);
      const extra = outputs || {};
      if (extra.video_url_string) {
        const clip = document.createElement('video');
        clip.src = String(extra.video_url_string);
        clip.controls = true;
        clip.playsInline = true;
        clip.preload = 'metadata';
        clip.style.cssText = 'width:100%;margin-top:4px;border-radius:4px;background:#111';
        clip.addEventListener('click', event => event.stopPropagation());
        clip.addEventListener('mousedown', event => event.stopPropagation());
        host.appendChild(clip);
      }
      if (extra.prompt_string) {
        const block = document.createElement('div');
        block.className = 'ntext';
        block.style.cssText = 'font-size:11px;opacity:.75;margin-top:4px';
        block.textContent = String(extra.prompt_string);
        host.appendChild(block);
      }
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
    if (outputs && typeof outputs === 'object' && type !== 'audio') showOutputs(host, outputs);
    const link = document.createElement('a');
    link.href = type === 'avatar' ? '/avatars' : value;
    link.target = '_blank';
    link.rel = 'noopener';
    link.className = 'nlink';
    // The preview itself opens full size; only an Avatar keeps a link, to
    // its profile page, which the preview cannot stand for.
    link.textContent = type === 'avatar' ? 'open profile' : '';
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
  /** One click on a refused control socket: pick a model with that ControlNet. */
  document.addEventListener('click', event => {
    const socket = event.target && event.target.closest && event.target.closest('.input[data-switch-to]');
    if (!socket || !window.AIEntities || !window.AIEntities.loadModels) return;
    const element = socket.closest('.drawflow-node');
    const slot = element && element.querySelector('[data-model-param="checkpoint"]');
    const channel = socket.dataset.switchTo;
    if (!slot || !slot._picker) return;
    event.stopPropagation();
    const id = element.id.replace(/^node-/, '');
    window.AIEntities.loadModels((meta(id) || {}).service || 'image').then(data => {
      const controlService = serviceById('control_' + channel);
      const target = ((data && data.checkpoints_array) || []).find(item => item && item.usable !== false &&
        ((item.control_channels || []).map(String).includes(channel)));
      if (!target) { toast('No model on the fleet has a native ' + channel + ' ControlNet.'); return; }
      const item = slot.querySelector('.mpick-item[data-model-file="' + CSS.escape(target.file) + '"]');
      if (item) item.click(); else slot._picker.value = target.file;
      delete socket.dataset.switchTo;
      socket.title = socket.dataset.typeTitle || '';
      toast('Model switched to ' + (target.title || target.file) + ': it has a native ' + channel + ' ControlNet.');
      void controlService;
    }).catch(() => {});
  }, true);

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
      if (event.target.closest('input, textarea, select, .mpick-panel, .ntext, .aislider')) return;
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
    if (unplacedNodes.length) {
      // Written back untouched; wires to drawn nodes follow those nodes' ids.
      const live = new Set(nodes.map(item => item.id));
      const keptIds = new Map();
      unplacedNodes.forEach(item => {
        let keptId = String(item.id);
        while (live.has(keptId)) keptId = 'kept_' + keptId;
        live.add(keptId);
        keptIds.set(String(item.id), keptId);
        nodes.push(Object.assign({}, item, {id: keptId}));
        if (unplacedResults[item.id]) results[keptId] = unplacedResults[item.id];
      });
      const endpoint = original => {
        if (keptIds.has(String(original))) return keptIds.get(String(original));
        const drawn = loadMapping.get(original);
        return drawn != null && meta(drawn) ? String(drawn) : null;
      };
      unplacedLinks.forEach(link => {
        const from = endpoint(link.from);
        const to = endpoint(link.to);
        if (from && to) links.push(Object.assign({}, link, {from, to}));
      });
    }
    return { name: document.getElementById('graph-name').value.trim() || 'Untitled',
             instance_id: graphInstanceId, comparison_anchor_id: comparisonAnchorId,
             render_quality: renderQuality,
             isolation: isolation ? {target: String(isolation.target), prior: Object.assign({}, isolation.prior)} : null,
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

  /** A farm error in words; the raw text goes behind a details toggle. */
  function humanizeFailure(state) {
    // Once rewritten, the state holds a <details>; never touch it again (the
    // observer sees our own edit, and the raw text still matches below).
    if (!state || state._humanizing || state.querySelector('details.nerr')) return;
    const text = state.textContent || '';
    let human = '', raw = '';
    const marker = text.indexOf(' | details: ');
    if (marker > 0) { human = text.slice(0, marker); raw = text.slice(marker + 12); }
    else if (/comfy error: \{/.test(text)) {
      raw = text;
      const type = (/"exception_type": "([^"]+)"/.exec(text) || [])[1] || '';
      const node = (/"node_type": "([^"]+)"/.exec(text) || [])[1] || '';
      human = /OutOfMemory|out of memory/i.test(text)
        ? 'Out of GPU memory — lower the size or the frame count'
        : 'The render failed' + (node ? ' in ' + node : '') + (type ? ' (' + type + ')' : '');
    } else return;
    state._humanizing = true;
    state.textContent = human + ' ';
    const box = document.createElement('details');
    box.className = 'nerr';
    box.style.cssText = 'display:inline;font-size:11px;opacity:.8';
    const summary = document.createElement('summary');
    summary.textContent = 'details';
    summary.style.cursor = 'pointer';
    const pre = document.createElement('pre');
    pre.textContent = raw;
    pre.style.cssText = 'white-space:pre-wrap;max-height:160px;overflow:auto;user-select:text';
    ['mousedown', 'pointerdown'].forEach(type => box.addEventListener(type, event => event.stopPropagation()));
    box.append(summary, pre);
    state.appendChild(box);
    state._humanizing = false;
  }
  if (typeof MutationObserver !== 'undefined') {
    new MutationObserver(records => records.forEach(record => {
      const target = record.target.nodeType === 1 ? record.target : record.target.parentElement;
      const state = target && target.closest && target.closest('.nstate');
      if (state && state.classList.contains('failed')) humanizeFailure(state);
    })).observe(document.documentElement, {subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['class']});
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
    const graph = toStoredIds(graphFromCanvas());
    if (graphId) {
      if (graphStale) {
        return { response: {ok: false, status: 409}, data: {detail: {error_string: 'graph_stale',
          message_string: 'This graph was changed elsewhere; reload the page before saving.'}}, created: false };
      }
      const headers = { 'Content-Type': 'application/json' };
      if (graphRevision != null) headers['X-Graph-Revision'] = String(graphRevision);
      const response = await fetch('/api/ai/graphs/' + encodeURIComponent(graphId), {
        method: 'PUT', headers, body: JSON.stringify(graph)
      });
      const data = await response.json().catch(() => ({}));
      if (response.status === 409 && (data.detail || {}).error_string === 'graph_stale') {
        graphStale = true;
        if (window.confirm('This graph was changed in another tab or by an agent. Your tab holds an older copy and was not saved.\n\nReload now to get the latest version?')) {
          location.reload();
        }
        return { response, data, created: false };
      }
      if (response.status !== 404 && response.status !== 409) {
        if (response.ok && data.revision_int != null) graphRevision = data.revision_int;
        return { response, data, created: false };
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
    const data = await response.json().catch(() => ({}));
    if (response.ok && data.revision_int != null) graphRevision = data.revision_int;
    return { response, data, created: true };
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
    if (nodeGroups && nodeGroups.refreshSizes) nodeGroups.refreshSizes();
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
      // A task carried over from before the page opened may be gone (cancelled
      // on the farm, purged). Then this Render submits it afresh instead of
      // leaving everything downstream waiting (owner, 2026-09-27).
      return restored.promise.catch(error => {
        restored.invalidated = true;
        if (restoredExecutions.get(idString) === restored) restoredExecutions.delete(idString);
        toast('A render from before was gone (' + String(error.message || error).slice(0, 60) + ') — submitting it again.');
        return startIncrementalService(id, node, resolved, params, signature, epoch, keepDone, graphSnapshot);
      });
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
            const missing = feeds.map((link, index) => {
              const record = upstreamRecords[index];
              if (record && record.ok) return '';
              const from = byId.get(link.from) || {};
              const name = ((from.params || {})._label) || from.service || from.entity_type || ('node ' + link.from);
              const why = !record ? 'not in the run'
                : record.bypassed ? 'bypassed'
                : record.cancelled ? 'cancelled'
                : record.superseded ? 'superseded by a newer run'
                : (record.error || 'failed');
              return link.input + ' ← ' + name + ': ' + String(why).slice(0, 120);
            }).filter(Boolean);
            const detail = missing.join('; ');
            if (epoch === canvasEpoch && meta(id)) {
              markState(id, (bypassed ? 'skipped — needs a bypassed node: ' : 'skipped — input did not arrive: ') + detail,
                bypassed ? 'nstate' : 'nstate failed');
            }
            return {ok:false, bypassed:bypassed, error:'upstream ' + detail};
          }
          if (node.kind === KIND_INPUT) {
            const value = inputValues.get(id) || '';
            if (!value) return {ok:false, error:'empty input'};
            const type = node.entity_type === 'media'
              ? (looksLikeVideo(value) ? 'video' : 'image') : node.entity_type;
            return {ok:true, result:{type, value, media:node.entity_type === 'media'}};
          }
          const resolved = {};
          const fan = {};
          for (let index = 0; index < feeds.length; index += 1) {
            const link = feeds[index];
            const upstream = upstreamRecords[index]?.result;
            if (!upstream) continue;
            if (Array.isArray(upstream.x9) && (!link.output || /url_string$|^value$/.test(link.output))) fan[link.input] = upstream.x9;
            let value = outputValue(upstream, link.output);
            if (upstream.media) {
              try {
                value = await adaptMediaValue(value, node.service, link.input);
              } catch (error) {
                markState(id, 'media input: ' + error.message, 'nstate failed');
                return {ok:false, error:error.message};
              }
            }
            resolved[link.input] = value;
          }
          const params = {...(node.params || {})};
          await followInputSizeAtRun(node, resolved, params);
          if (params._x9 && X9_SERVICES.has(node.service)) {
            try {
              const result = await runX9(id, node, resolved, fan, params, epoch, keepDone);
              if (!graph.results) graph.results = {};
              graph.results[id] = {status:'done', type:result.type, value:result.value};
              return {ok:true, result};
            } catch (error) {
              return {ok:false, error:String(error.message || error)};
            }
          }
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

  /** Administrator: wipe the whole farm queue after an explicit, typed confirmation. */
  async function resetFarm() {
    let preview;
    try {
      const response = await fetch('/api/ai/farm/reset?dry_run=1', {method: 'POST'});
      preview = await response.json();
      if (!response.ok) throw new Error(preview.detail || 'not allowed');
    } catch (error) { toast('Farm reset is not available: ' + error.message); return; }
    const list = object => Object.entries(object || {}).map(([k, v]) => '  ' + k + ': ' + v).join('\n') || '  none';
    const text = 'RESET THE WHOLE FARM\n\n' +
      'Queued: ' + preview.queued_int + '   Running: ' + preview.running_int + '\n\n' +
      'Running on boxes:\n' + list(preview.running_by_box_object) + '\n\n' +
      'Jobs by owner:\n' + list(preview.by_owner_object) + '\n\n' +
      'Every one of them (every graph, user and agent) is cancelled, and the ComfyUI queue on ' +
      (preview.boxes_array || []).join(', ') + ' is cleared. Results, caches and models stay.\n\n' +
      'Type RESET to confirm:';
    if ((window.prompt(text, '') || '').trim().toUpperCase() !== 'RESET') { toast('Farm reset cancelled.'); return; }
    runRequests.forEach(token => { token.cancelled = true; });
    try {
      const response = await fetch('/api/ai/farm/reset', {method: 'POST'});
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || 'the reset failed');
      const boxes = Object.values(data.boxes_object || {}).filter(value => value === 'cleared').length;
      toast('Farm reset: cancelled ' + data.cancelled_queued_int + ' queued, ' +
            data.cancelled_running_int + ' running on ' + boxes + ' boxes.');
    } catch (error) { toast('Farm reset failed: ' + error.message); }
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
      // An X9 set saved mid-render has no single task to resume (its nine
      // tasks ran in the tab that started them): keep the cells, say so.
      if (Array.isArray(record.x9) && record.x9.length && record.status === 'running') {
        record.status = record.x9.some(cell => cell.status === 'done') ? 'stale' : 'failed';
        record.error = record.status === 'failed' ? 'X9 was interrupted — Render again' : '';
      }
      if (Array.isArray(record.x9) && record.x9.length && record.status === 'failed') {
        state.textContent = record.error || 'X9 was interrupted — Render again';
        state.className = 'nstate';
        requestAnimationFrame(() => paintX9(id));
        return;
      }
      if (Array.isArray(record.x9) && record.x9.length && ['done', 'stale'].includes(record.status)) {
        if (record.status === 'done') continuableResults.set(String(id), {type:record.type, value:record.value, outputs:null, task_id:''});
        state.textContent = record.status === 'stale' ? 'changed — render to update' : 'done · X9 — click a cell to view / choose';
        state.className = record.status === 'stale' ? 'nstate' : 'nstate done';
        requestAnimationFrame(() => paintX9(id));
        return;
      }
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
      if (!record.value && !record.task_id) {
        state.textContent = 'interrupted — Render again';
        state.className = 'nstate';
        return;
      }
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
      showResult(outBox, runnerType(runner, value), value, outputs);
      recordResult(id, Object.assign({ status: 'done', type: runnerType(runner, value), value: value,
                         input_reference_url:record.input_reference_url || '', history:record.history || [],
                         task_id: record.task_id || '' }, outputs ? {outputs} : {}));
      return outputs ? {type:runnerType(runner, value), value, outputs} : {type:runnerType(runner, value), value};
    } catch (error) {
      if (!stillHere()) throw error;
      state.textContent = String(error.message || error);
      state.className = 'nstate failed';
      if (task) task.finish(false);
      recordResult(id, { status: 'failed', type: runnerType(runner, ''), value: '',
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
    unplacedNodes = [];
    unplacedLinks = [];
    unplacedResults = {};
    (graph.nodes || []).forEach(node => {
      const id = node.kind === KIND_INPUT
        ? addInputNode(node.entity_type, node.x, node.y, node.value, node.params)
        : addServiceNode(node.service, node.x, node.y, node.params);
      if (id) {
        mapping.set(node.id, id);
        // Stored ids stay what they were: API patches, caches and results
        // are keyed by them (owner, 2026-09-27). Drawflow numbers only exist
        // inside this page.
        const item = meta(id);
        if (item) item.storedId = String(node.id);
      }
      else {
        unplacedNodes.push(JSON.parse(JSON.stringify(node)));
        if (graph.results && graph.results[node.id]) unplacedResults[node.id] = graph.results[node.id];
      }
    });
    loadMapping = mapping;
    if (unplacedNodes.length) {
      toast(unplacedNodes.length + ' node(s) of a type this page does not know are kept as they are — reload the page to see them.');
    }
    (graph.links || []).forEach(link => {
      const from = mapping.get(link.from);
      const to = mapping.get(link.to);
      if (!from || !to) {
        const kept = new Set(unplacedNodes.map(item => String(item.id)));
        if (kept.has(String(link.from)) || kept.has(String(link.to))) unplacedLinks.push(Object.assign({}, link));
        return;
      }
      const fromMeta = meta(from);
      const toMeta = meta(to);
      const outIndex = Math.max(0, fromMeta.outFields.indexOf(link.output));
      const inIndex = toMeta.inFields.indexOf(link.input);
      if (inIndex < 0) return;
      editor.addConnection(from, to, 'output_' + (outIndex + 1), 'input_' + (inIndex + 1));
    });
    comparisonAnchorId = String(mapping.get(graph.comparison_anchor_id) || '');
    isolation = null;
    if (graph.isolation && graph.isolation.target != null && mapping.get(String(graph.isolation.target)) != null) {
      const prior = {};
      Object.entries(graph.isolation.prior || {}).forEach(([old, was]) => {
        const now = mapping.get(String(old));
        if (now != null) prior[String(now)] = !!was;
      });
      isolation = {target: String(mapping.get(String(graph.isolation.target))), prior};
    }
    paintIsolation();
    restoreResults(graph.results, mapping);
    if (nodeGroups && nodeGroups.refreshSizes) {
      nodeGroups.refreshSizes();
      // Model pickers and system-prompt markers finish a beat later on some
      // nodes (Qwen-Image); settle sizes again once they have.
      [1500, 4000].forEach(delay => setTimeout(() => nodeGroups.refreshSizes(), delay));
    }
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
    media: 'Any picture or clip: a link (Civitai pages too), a file, or Ctrl+V. Picture sockets get a clip\'s first frame.',
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
    upscale2x: '⏫', music: '🎵', 'input:media': '🏞️', 'input:image': '🏞️', 'input:video': '📹', 'input:text': '✏️', 'input:avatar': '👤',
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
  /**
   * The tool dock (owner, 2026-09-27): a fixed strip at the bottom of the page,
   * icon + name for every node, grouped by category, Media in first. Fixed
   * cell sizes, no wrapping, no scrolling. When a window is too narrow for all
   * groups, the dock shows category tabs and one group at a time.
   */
  const DOCK_GROUPS = [
    ['Inputs', ['input:media', 'input:text', 'input:avatar']],
    ['Vision / Text', ['vision', 'text']],
    ['Image', ['image', 'qwen_image', 'upscale2x', 'upscale', 'detail_enhance', 'face_fix']],
    ['Video', ['video', 'video_frame', 'video_storyboard', 'video_control', 'upscale_video']],
    ['Avatars', ['avatar_build', 'avatar_image', 'avatar_video']],
    ['Control maps', ['control_pose', 'control_depth', 'control_canny', 'control_normal']],
    ['Audio', ['music']],
    ['Utility', ['3dmodel', 'action:arrange', 'action:fit', 'action:assistant']]
  ];
  const DOCK_LABELS = {
    'input:media': 'Media in', 'input:text': 'Text in', 'input:avatar': 'Avatar',
    vision: 'Vision', text: 'Text', image: 'Image', qwen_image: 'Qwen-Image', upscale2x: 'Upscale 2×',
    upscale: 'Upscale', detail_enhance: 'Detail', face_fix: 'Face fix', video: 'Video',
    video_frame: 'First frame', video_storyboard: 'Storyboard', video_control: 'Motion transfer',
    upscale_video: 'Upscale video', avatar_build: 'Avatar builder', avatar_image: 'Avatar scene',
    avatar_video: 'Avatar video', avatar_from_image: 'Avatar from picture', control_pose: 'Pose', control_depth: 'Depth',
    control_canny: 'Canny', control_normal: 'Normal', music: 'Music', '3dmodel': '3D model',
    'action:arrange': 'Arrange', 'action:fit': 'Fit view', 'action:assistant': 'Assistant'
  };
  let dockTab = 0;

  function dockItem(key) {
    if (key.startsWith('input:')) {
      const type = key.slice(6);
      const title = DOCK_LABELS[key];
      return paletteButton(title, toolIcon(key, type), SOURCE_HELP[type] || '', {kind: 'input', type, title});
    }
    if (key === 'action:arrange') return actionButton('Arrange', '▦',
      'Lay the selected nodes out in columns by depth, or the whole graph when nothing is selected.',
      () => arrangeNodes(Array.from(nodeGroups?.selected || [])));
    if (key === 'action:fit') return actionButton('Fit view', '⤢',
      'Frame the whole composition: zoom and pan so every node is on screen.',
      () => { if (!document.querySelector('#canvas .drawflow-node')) toast('There is nothing on the canvas to frame yet.'); else fitView(); });
    if (key === 'action:assistant') return actionButton('Assistant', '💬',
      'Show or hide the graph assistant (describe a change in words).',
      () => document.body.classList.toggle('aga-shown'));
    const entry = serviceById(key);
    if (!entry) return null;
    const button = paletteButton(entry.title, toolIcon(entry.id, (entry.produces_array || [])[0]), entry.summary,
      {kind: 'service', service: entry.id, title: entry.title});
    if (entry.status !== 'live') {
      const why = entry.blocked_reason || 'Not wired up yet.';
      button.disabled = true;
      button.draggable = false;
      button.setAttribute('aria-label', entry.title + '. ' + why);
      const note = button.querySelector('.ttip i');
      if (note) note.textContent = why;
    }
    return button;
  }

  function buildPalette() {
    const host = document.getElementById('palette');
    if (!host) return;
    host.innerHTML = '';
    const placed = new Set();
    const groups = DOCK_GROUPS.map(([name, keys]) => [name, keys.slice()]);
    // A service the groups do not name yet still gets a place (Utility).
    (catalogue.services_array || []).forEach(entry => {
      if (!groups.some(([, keys]) => keys.includes(entry.id))) groups[groups.length - 1][1].unshift(entry.id);
    });
    const tabs = document.createElement('div');
    tabs.className = 'dock-tabs';
    // Tabs hold short names: a phone shows every category in one row.
    const SHORT = {'Vision / Text': 'V/T', 'Control maps': 'Maps', 'Avatars': 'Avatar', 'Utility': 'More'};
    const row = document.createElement('div');
    row.className = 'dock-row';
    groups.forEach(([name, keys], index) => {
      const group = document.createElement('div');
      group.className = 'dock-group';
      group.dataset.group = String(index);
      group.title = name;
      keys.forEach(key => {
        if (placed.has(key)) return;
        const item = dockItem(key);
        if (!item) return;
        placed.add(key);
        const label = document.createElement('span');
        label.className = 'tlabel';
        label.textContent = DOCK_LABELS[key] || (serviceById(key) || {}).title || key;
        item.appendChild(label);
        group.appendChild(item);
      });
      if (!group.children.length) return;
      row.appendChild(group);
      const tab = document.createElement('button');
      tab.type = 'button';
      tab.className = 'dock-tab';
      tab.textContent = window.innerWidth < 600 ? (SHORT[name] || name) : name;
      tab.title = name;
      tab.dataset.group = String(index);
      tab.addEventListener('click', () => { dockTab = index; layoutDock(); });
      tabs.appendChild(tab);
    });
    host.append(tabs, row);
    layoutDock();
  }

  /** All groups in one row if they fit, else tabs + the chosen group. */
  function layoutDock() {
    const host = document.getElementById('palette');
    if (!host) return;
    // The stage ends where the dock begins, whatever height the top bar wraps to.
    const shell = document.querySelector('.shell');
    if (shell) {
      const top = shell.getBoundingClientRect().top + window.scrollY;
      shell.style.height = Math.max(240, window.innerHeight - top - host.offsetHeight) + 'px';
    }
    const groups = [...host.querySelectorAll('.dock-group')];
    host.classList.remove('dock-tabbed');
    groups.forEach(group => { group.hidden = false; });
    const needed = groups.reduce((sum, group) => sum + group.scrollWidth + 14, 0);
    const tabbed = needed > host.clientWidth - 8;
    host.classList.toggle('dock-tabbed', tabbed);
    if (!tabbed) return;
    const visible = groups.some(group => group.dataset.group === String(dockTab)) ? String(dockTab) : groups[0].dataset.group;
    groups.forEach(group => { group.hidden = group.dataset.group !== visible; });
    host.querySelectorAll('.dock-tab').forEach(tab => tab.classList.toggle('on', tab.dataset.group === visible));
  }
  let dockResizeTimer = null;
  window.addEventListener('resize', () => { clearTimeout(dockResizeTimer); dockResizeTimer = setTimeout(layoutDock, 120); });

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
    // The dock sits below the stage now, not over it.
    return tools && getComputedStyle(tools).position === 'fixed' ? 0 :
      Math.min(Math.round(tools.getBoundingClientRect().height) + 20, Math.round(window.innerHeight / 4) || 140);
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
    if (getComputedStyle(tools).position === 'fixed') return;
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
    installMediaThrottle(document.getElementById('canvas'));
    if (window.AINodePipelines && window.AIEntities) nodePipelines = window.AINodePipelines.install({
      editor, getMeta:meta, addServiceNode, getNodeElement:nodeElement, moveNode:moveNodeTo,
      exportGraph:graphFromCanvas, imageOutput:imageOutputField, nodeLimit:200, toast,
      loadModels:service => window.AIEntities.loadModels(service),
      onNodesAdded:ids => { if (nodeGroups) nodeGroups.selectIds(ids); }});
    if (window.AINodeGroups) nodeGroups = window.AINodeGroups.install({editor,
      canvas:document.getElementById('canvas'), getMeta:meta, addInputNode,
      addServiceNode, exportGraph:graphFromCanvas, toast, nodeLimit:200,
      getQuality:() => renderQuality, serviceById, invalidate:id => invalidateNodeAndDownstream(id),
      onNodesRemoved:forgetNodes,
      nodeFunctions:id => nodePipelines ? nodePipelines.functionsFor(id) : [],
      onArrange:ids => arrangeNodes(ids),
      onToggleBypass:toggleBypass,
      onEditSystemPrompt:ids => openSystemPromptEditor(ids),
      systemPromptTargets,
      onSetComparisonAnchor:id => nodeCompare && nodeCompare.setAnchor(id)});
    document.addEventListener('paste', event => {
      const items = [...(event.clipboardData?.items || [])];
      const item = items.find(value => value.kind === 'file' && /^(image|video)\//.test(value.type));
      const current = event.target.closest && event.target.closest('.drawflow-node');
      const target = current || document.querySelector('#canvas .drawflow-node.selected');
      if (item) {
        if (!target || !target._acceptImage) { toast('Select a Media in node to paste an image or video.'); return; }
        event.preventDefault();
        target._acceptImage(item.getAsFile());
        return;
      }
      // A copied link lands in the selected Media node; typing into a field
      // keeps the browser's own paste.
      const editing = event.target && /^(INPUT|TEXTAREA|SELECT)$/.test(event.target.tagName || '');
      if (editing || !target || !target._acceptText) return;
      const link = event.clipboardData ? event.clipboardData.getData('text/plain') : '';
      if (target._acceptText(link)) event.preventDefault();
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
    if (window.AISlider) window.AISlider.watch(document.getElementById('canvas'));
    document.addEventListener('click', event => {
      const dice = event.target.closest && event.target.closest('.nseed-rand');
      if (!dice) return;
      event.stopPropagation();
      const field = dice.parentElement.querySelector('[data-param="' + dice.dataset.seedFor + '"]');
      if (!field) return;
      const top = Math.min(Number(field.max) || 2147483647, 2147483647);
      field.value = String(Math.floor(Math.random() * top));
      field.dispatchEvent(new Event('input', {bubbles: true}));
      field.dispatchEvent(new Event('change', {bubbles: true}));
    }, true);
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
    const farmReset = document.getElementById('farm-reset');
    if (farmReset) {
      farmReset.addEventListener('click', resetFarm);
      fetch('/api/ai/queue/admin').then(r => r.json())
        .then(data => { farmReset.hidden = !(data && data.admin_bool); }).catch(() => {});
    }
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
        loadGraph(Object.assign({render_quality: NEW_GRAPH_QUALITY}, template.graph));
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
        if (!data.template_bool) {
          graphId = data.graph_id_string;
          graphRevision = data.revision_int != null ? data.revision_int : null;
        }
        loadGraph(data.template_bool
          ? Object.assign({render_quality: NEW_GRAPH_QUALITY}, data.graph_object)
          : data.graph_object);
      } else {
        toast('That link does not open a graph any more.');
      }
    } else if (wantedAvatar) {
      resetCanvasExecutionState();
      editor.clear(); nodeMeta.clear(); runState.clear();
      setRenderQuality(NEW_GRAPH_QUALITY);
      document.getElementById('graph-name').value = 'Avatar production';
      addInputNode('avatar', 60, 100, wantedAvatar);
    } else if ((templates.templates_array || []).length) {
      loadGraph(Object.assign({render_quality: NEW_GRAPH_QUALITY}, templates.templates_array[0].graph));
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

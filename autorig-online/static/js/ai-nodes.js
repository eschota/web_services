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
  let graphId = null;
  let resultsTimer = null;

  function recordResult(id, record) {
    runState.set(String(id), record);
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
    } else {
      control = `<input type="text" data-param="${name}" value="${escapeAttr(param.default || '')}"${help}>`;
    }
    return `<label class="nparam"><span>${escapeHtml(param.title)}</span>${control}</label>`;
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
      <div class="nstate"></div>
      <div class="nprog task-prog"></div>
      <div class="nout"></div>`;
  }

  function inputNodeHtml(entityType) {
    const isText = entityType === 'text';
    const field = isText
      ? '<textarea data-value rows="3" placeholder="Type the text…"></textarea>'
      : '<input type="text" data-value placeholder="https://… or drop a file">'
        + '<input type="file" data-file accept="image/*" hidden>'
        + '<button type="button" data-pick class="npick">Choose a file</button>'
        + '<img data-preview alt="" hidden>';
    return `
      <div class="nhead"><b>${isText ? 'Text in' : 'Image in'}</b></div>
      <div class="nports"><div class="prow pout">${isText ? 'text' : 'image'} ${typeIcon(entityType)}</div></div>
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
      'ainode svc-' + serviceId, { service: serviceId }, serviceNodeHtml(entry)
    );
    setMeta(id, {
      kind: KIND_SERVICE,
      service: serviceId,
      inFields: inputs.map(i => i.field),
      outFields: outputs.map(o => o.field)
    });
    alignPorts(id, inputs.length, outputs.length);
    if (params) applyParams(id, params);
    return id;
  }

  function addInputNode(entityType, x, y, value) {
    const id = editor.addNode(
      'input-' + entityType, 0, 1, x, y,
      'ainode input-node', { entity_type: entityType }, inputNodeHtml(entityType)
    );
    setMeta(id, { kind: KIND_INPUT, entityType: entityType, inFields: [], outFields: ['value'] });
    alignPorts(id, 0, 1);
    if (value) {
      const field = nodeElement(id).querySelector('[data-value]');
      if (field) field.value = value;
    }
    wireInputNode(id, entityType);
    return id;
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
    if (!element || entityType !== 'image') return;
    const file = element.querySelector('[data-file]');
    const pick = element.querySelector('[data-pick]');
    const preview = element.querySelector('[data-preview]');
    const text = element.querySelector('[data-value]');
    pick.addEventListener('click', () => file.click());
    file.addEventListener('change', event => {
      const chosen = event.target.files[0];
      if (!chosen) return;
      const reader = new FileReader();
      reader.onload = () => {
        // Held as a data URL and sent inline; the API publishes it and the
        // rest of the graph then works with an ordinary address.
        text.value = reader.result;
        preview.src = reader.result;
        preview.hidden = false;
      };
      reader.readAsDataURL(chosen);
    });
    text.addEventListener('change', () => {
      if (/^https?:\/\//.test(text.value.trim())) {
        preview.src = text.value.trim();
        preview.hidden = false;
      }
    });
  }

  function applyParams(id, params) {
    const element = nodeElement(id);
    if (!element) return;
    Object.keys(params || {}).forEach(name => {
      const control = element.querySelector('[data-param="' + CSS.escape(name) + '"]');
      if (!control) return;
      control.value = params[name];
      const readout = element.querySelector('[data-for="' + CSS.escape(name) + '"]');
      if (readout) readout.textContent = rangeLabel(params[name]);
    });
  }

  function readParams(id) {
    const element = nodeElement(id);
    const values = {};
    if (!element) return values;
    element.querySelectorAll('[data-param]').forEach(control => {
      const raw = control.value;
      values[control.dataset.param] = control.type === 'range' || control.type === 'number'
        ? Number(raw) : raw;
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
    if (info && info.produced === info.accepted) return;
    // A wrong wire is removed rather than left to fail at render time, when
    // the person has already waited for everything upstream of it.
    editor.removeSingleConnection(connection.output_id, connection.input_id,
                                  connection.output_class, connection.input_class);
    toast(info
      ? `That socket carries ${info.produced || 'nothing'}, and this one takes ${info.accepted || 'nothing'}.`
      : 'Those two cannot be connected.');
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
    vision: { api: '/api/vision', finish: pollAiStatus, field: 'answer_string', type: 'text' },
    text: { api: '/api/text2text', finish: pollAiStatus, field: 'answer_string', type: 'text' },
    image: { api: '/api/image', finish: pollForFile, field: 'image_url_string', type: 'image' },
    video: { api: '/api/video', finish: pollForFile, field: 'video_url_string', type: 'video' },
    '3dmodel': { api: '/api/3dmodel', finish: poll3dStatus, field: 'model_url_string', type: 'model3d' }
  };

  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

  async function pollAiStatus(accepted, runner) {
    if (accepted[runner.field]) return accepted[runner.field];
    const id = accepted.task_id_string;
    for (let attempt = 0; attempt < 120; attempt++) {
      await sleep(3000);
      const data = await fetch('/api/ai/status/' + encodeURIComponent(id)).then(r => r.json()).catch(() => null);
      if (!data) continue;
      if (data.finished_bool) {
        if (data[runner.field]) return data[runner.field];
        throw new Error(data.error_string || 'the model returned nothing');
      }
    }
    throw new Error('the answer did not arrive in time');
  }

  async function pollForFile(accepted, runner) {
    const url = accepted[runner.field];
    if (!url) throw new Error('the farm accepted the job without an output address');
    // The farm publishes where the file will be before it exists, so the file
    // appearing is the completion signal. Video is allowed half an hour.
    for (let attempt = 0; attempt < 400; attempt++) {
      await sleep(5000);
      const probe = await fetch(url, { method: 'HEAD' }).catch(() => null);
      if (probe && probe.ok) return url;
    }
    throw new Error('the render did not land in time; it may still be running');
  }

  async function poll3dStatus(accepted) {
    const id = accepted.task_id_string;
    for (let attempt = 0; attempt < 400; attempt++) {
      await sleep(5000);
      const data = await fetch('/api/3dmodel/status/' + encodeURIComponent(id))
        .then(r => r.json()).catch(() => null);
      if (!data) continue;
      if (data.finished_bool) {
        if (data.model_url_string) return data.model_url_string;
        throw new Error(data.error_string || 'the node did not produce a model');
      }
    }
    throw new Error('the model did not arrive in time');
  }

  function bodyFor(serviceId, resolved, params) {
    const body = {};
    Object.keys(params || {}).forEach(name => {
      const value = params[name];
      // A zero or an empty string here means "leave the workflow's own value",
      // so it is left out rather than sent as an override.
      if (value !== '' && value !== 0 && value !== null && value !== undefined) body[name] = value;
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

  async function runServiceNode(id, resolved) {
    const node = meta(id);
    const runner = RUNNERS[node.service];
    const element = nodeElement(id);
    const state = element.querySelector('.nstate');
    const progress = element.querySelector('.nprog');
    const outBox = element.querySelector('.nout');
    state.textContent = 'sending…';
    state.className = 'nstate running';
    outBox.innerHTML = '';
    const task = window.AIEntities ? window.AIEntities.startTask(progress, node.service) : null;
    try {
      const response = await fetch(runner.api, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(bodyFor(node.service, resolved, readParams(id)))
      });
      const accepted = await response.json();
      if (!response.ok) {
        const detail = accepted.detail || {};
        throw new Error(detail.message_string || detail.error_string || ('HTTP ' + response.status));
      }
      state.textContent = 'running on the farm…';
      // Recorded before the wait, not after: the whole point is that a link
      // opened mid-render knows which task to carry on watching.
      recordResult(id, {
        status: 'running', type: runner.type,
        value: accepted[runner.field] || '',
        task_id: accepted.task_id_string || '',
        started_at: Date.now() / 1000
      });
      const value = await runner.finish(accepted, runner);
      state.textContent = 'done';
      state.className = 'nstate done';
      if (task) task.finish(true);
      showResult(outBox, runner.type, value);
      recordResult(id, { status: 'done', type: runner.type, value: value,
                         task_id: accepted.task_id_string || '' });
      return { type: runner.type, value: value };
    } catch (error) {
      state.textContent = String(error.message || error);
      state.className = 'nstate failed';
      if (task) task.finish(false);
      recordResult(id, { status: 'failed', type: runner.type, value: '',
                         error: String(error.message || error) });
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
    } else if (type === 'image') {
      const picture = document.createElement('img');
      picture.src = value;
      host.appendChild(picture);
    } else if (type === 'video') {
      const clip = document.createElement('video');
      clip.src = value;
      clip.controls = true;
      clip.loop = true;
      clip.muted = true;
      clip.autoplay = true;
      host.appendChild(clip);
    }
    const link = document.createElement('a');
    link.href = value;
    link.target = '_blank';
    link.rel = 'noopener';
    link.className = 'nlink';
    link.textContent = type === 'text' ? '' : 'open';
    if (link.textContent) host.appendChild(link);
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
          x: raw.pos_x, y: raw.pos_y, params: {}
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

  /** A run needs a link to publish its progress to, so one is made up front. */
  async function ensureSaved() {
    if (graphId) return;
    try {
      const response = await fetch('/api/ai/graphs', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(graphFromCanvas())
      });
      const data = await response.json();
      if (response.ok) {
        graphId = data.graph_id_string;
        history.replaceState(null, '', data.deep_link_string);
      }
    } catch (error) { /* a run is still worth doing without a link */ }
  }

  async function runGraph() {
    const graph = graphFromCanvas();
    const order = executionOrder(graph);
    if (order.length !== graph.nodes.length) {
      toast('The wiring loops back on itself, so there is no order to run it in.');
      return;
    }
    const results = new Map();
    const failed = new Set();
    const button = document.getElementById('run');
    button.disabled = true;
    button.textContent = 'Rendering…';
    runState.clear();
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
          await Promise.all(feeds.map(link => pending.get(link.from)));
          if (feeds.some(link => failed.has(link.from))) {
            failed.add(id);
            const element = nodeElement(id);
            if (element) {
              const state = element.querySelector('.nstate');
              state.textContent = 'skipped — what it needed did not arrive';
              state.className = 'nstate failed';
            }
            return;
          }
          if (node.kind === KIND_INPUT) {
            const field = nodeElement(id).querySelector('[data-value]');
            const value = field ? field.value.trim() : '';
            if (!value) { failed.add(id); throw new Error('empty input'); }
            results.set(id, { type: node.entity_type, value: value });
            return;
          }
          const resolved = {};
          feeds.forEach(link => {
            const upstream = results.get(link.from);
            if (upstream) resolved[link.input] = upstream.value;
          });
          try {
            results.set(id, await runServiceNode(id, resolved));
          } catch (error) {
            failed.add(id);
          }
        })();
        pending.set(id, start);
      }
      await Promise.all(pending.values());
    } finally {
      button.disabled = false;
      button.textContent = 'Render';
    }
    const steps = graph.nodes.filter(node => node.kind === KIND_SERVICE).length;
    const produced = graph.nodes.filter(
      node => node.kind === KIND_SERVICE && results.has(node.id)).length;
    toast(failed.size
      ? `${produced} of ${steps} steps finished; ${failed.size} did not.`
      : `All ${steps} steps finished.`);
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
      if (record.status === 'done' && record.value) {
        state.textContent = 'done';
        state.className = 'nstate done';
        showResult(outBox, record.type, record.value);
        return;
      }
      if (record.status === 'failed') {
        state.textContent = record.error || 'failed';
        state.className = 'nstate failed';
        return;
      }
      if (record.status !== 'running') return;
      resumeNode(id, record).catch(() => {});
    });
  }

  async function resumeNode(id, record) {
    const node = meta(id);
    if (!node) return;
    const runner = RUNNERS[node.service];
    const element = nodeElement(id);
    const state = element.querySelector('.nstate');
    const outBox = element.querySelector('.nout');
    state.textContent = 'still running on the farm…';
    state.className = 'nstate running';
    const task = window.AIEntities
      ? window.AIEntities.startTask(element.querySelector('.nprog'), node.service)
      : null;
    // The pollers only need what the submit returned, and that is exactly
    // what was stored, so the same code finishes the job.
    const accepted = { task_id_string: record.task_id };
    accepted[runner.field] = record.value || '';
    try {
      const value = await runner.finish(accepted, runner);
      state.textContent = 'done';
      state.className = 'nstate done';
      if (task) task.finish(true);
      showResult(outBox, runner.type, value);
      recordResult(id, { status: 'done', type: runner.type, value: value,
                         task_id: record.task_id || '' });
    } catch (error) {
      state.textContent = String(error.message || error);
      state.className = 'nstate failed';
      if (task) task.finish(false);
      recordResult(id, { status: 'failed', type: runner.type, value: '',
                         error: String(error.message || error) });
    }
  }

  function loadGraph(graph) {
    editor.clear();
    nodeMeta.clear();
    runState.clear();
    document.getElementById('graph-name').value = graph.name || 'Untitled';
    const mapping = new Map();
    (graph.nodes || []).forEach(node => {
      const id = node.kind === KIND_INPUT
        ? addInputNode(node.entity_type, node.x, node.y, node.value)
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
    restoreResults(graph.results, mapping);
  }

  async function saveGraph() {
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
    graphId = data.graph_id_string;
    const link = location.origin + data.deep_link_string;
    history.replaceState(null, '', data.deep_link_string);
    navigator.clipboard.writeText(link).then(
      () => toast('Deep link copied: ' + link),
      () => toast('Deep link: ' + link)
    );
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
    [['image', 'Image in'], ['text', 'Text in']].forEach(([type, title]) => {
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
    editor.on('connectionCreated', onConnectionCreated);
    // A range's number is only useful if it is shown next to the slider.
    document.getElementById('canvas').addEventListener('input', event => {
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
    document.getElementById('run').addEventListener('click', runGraph);
    document.getElementById('save').addEventListener('click', saveGraph);
    document.getElementById('clear').addEventListener('click', () => {
      editor.clear();
      nodeMeta.clear();
    });

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
    if (wanted) {
      const data = await fetch('/api/ai/graphs/' + encodeURIComponent(wanted))
        .then(r => r.json()).catch(() => null);
      if (data && data.success_bool) {
        if (!data.template_bool) graphId = data.graph_id_string;
        loadGraph(data.graph_object);
      } else {
        toast('That link does not open a graph any more.');
      }
    } else if ((templates.templates_array || []).length) {
      loadGraph(templates.templates_array[0].graph);
    }
  }

  window.addEventListener('DOMContentLoaded', () => {
    boot().catch(error => toast('The editor could not start: ' + error.message));
  });
})();

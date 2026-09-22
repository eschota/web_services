/**
 * Apply backend-validated AI graph edits without replacing the live canvas.
 *
 * The validator is authoritative for types, cycles, parameters and models.
 * This bridge is deliberately only an in-place renderer for that result: it
 * never saves or runs a graph, never imports old result records, and refuses
 * to apply when the person changed the graph after the AI request began.
 */
(function (global) {
  'use strict';

  function clone(value) {
    return value == null ? value : JSON.parse(JSON.stringify(value));
  }

  function stable(value) {
    if (Array.isArray(value)) return '[' + value.map(stable).join(',') + ']';
    if (value && typeof value === 'object') {
      return '{' + Object.keys(value).sort().map(key =>
        JSON.stringify(key) + ':' + stable(value[key])).join(',') + '}';
    }
    return JSON.stringify(value);
  }

  const NUMERIC_PARAMS = new Set([
    'width', 'height', 'steps', 'cfg', 'seed', 'frames', 'frame_count',
    'duration', 'duration_seconds', 'creativity', 'lora_strength',
    'control_strength', 'control_start', 'control_end', 'max_output_tokens',
    'clip_skip'
  ]);

  function normalizedParams(params) {
    const result = {};
    Object.keys(params || {}).forEach(name => {
      const value = params[name];
      if (NUMERIC_PARAMS.has(name) && value !== '' && value != null &&
          Number.isFinite(Number(value))) result[name] = Number(value);
      else result[name] = clone(value);
    });
    // Missing is the persisted legacy form of the current default-follow
    // behaviour. Compare it as true so applying/reloading does not look like
    // an edit merely because the live canvas serialises the explicit marker.
    if ('width' in result && 'height' in result && !('_follow_input_size' in result)) {
      result._follow_input_size = true;
    }
    return result;
  }

  function graphForCompare(graph) {
    graph = graph || {};
    const nodes = (graph.nodes || []).map(node => ({
      id: String(node.id),
      kind: String(node.kind || 'service'),
      service: node.service == null ? null : String(node.service),
      entity_type: node.entity_type == null ? null : String(node.entity_type),
      value: String(node.value == null ? '' : node.value),
      x: Number(node.x) || 0,
      y: Number(node.y) || 0,
      params: normalizedParams(node.params || {})
    })).sort((a, b) => a.id.localeCompare(b.id));
    const links = (graph.links || []).map(link => ({
      from: String(link.from), output: String(link.output),
      to: String(link.to), input: String(link.input)
    })).sort((a, b) => linkKey(a).localeCompare(linkKey(b)));
    const compared = {name:String(graph.name || 'Untitled'), nodes, links};
    if (graph.instance_id != null) compared.instance_id = String(graph.instance_id);
    return compared;
  }

  function sameGraph(left, right) {
    return stable(graphForCompare(left)) === stable(graphForCompare(right));
  }

  function linkKey(link) {
    return [link.from, link.output, link.to, link.input].map(String).join('\u001f');
  }

  function identityMatches(current, wanted) {
    if (!current || !wanted || String(current.kind) !== String(wanted.kind)) return false;
    if (wanted.kind === 'input') {
      return String(current.entity_type || '') === String(wanted.entity_type || '');
    }
    return String(current.service || '') === String(wanted.service || '');
  }

  function paramsEqual(left, right) {
    return stable(normalizedParams(left || {})) === stable(normalizedParams(right || {}));
  }

  function specifiedParamsMatch(actual, specified) {
    const normalizedActual = normalizedParams(actual || {});
    const normalizedSpecified = normalizedParams(specified || {});
    return Object.keys(normalizedSpecified).every(name =>
      stable(normalizedActual[name]) === stable(normalizedSpecified[name]));
  }

  function explicitParams(operations) {
    const byNode = new Map();
    (operations || []).forEach(operation => {
      if (!operation || !operation.op) return;
      let id = operation.id;
      let values = null;
      if (operation.op === 'update_params') values = operation.values || {};
      if (operation.op === 'add_node' && operation.node) {
        id = operation.node.id;
        values = operation.node.params || {};
      }
      if (id == null || !values) return;
      const names = byNode.get(String(id)) || new Set();
      Object.keys(values).forEach(name => names.add(name));
      byNode.set(String(id), names);
    });
    return byNode;
  }

  function changedModelFields(before, after) {
    return ['checkpoint', 'lora'].filter(name =>
      String((before || {})[name] || '') !== String((after || {})[name] || ''));
  }

  function create(options) {
    options = options || {};
    const editor = options.editor;
    const getGraph = options.getGraph;
    const addServiceNode = options.addServiceNode;
    const addInputNode = options.addInputNode;
    const applyParams = options.applyParams || function () {};
    const getMeta = options.getMeta || function () { return null; };
    const forgetNodes = options.forgetNodes || function () {};
    const invalidateNodeAndDownstream = options.invalidateNodeAndDownstream || function () {};
    const nodeDisplay = options.nodeDisplay || null;
    const applyRecommended = options.applyRecommended || null;
    const applyBypass = typeof options.applyBypass === 'function' ? options.applyBypass : null;
    const setGraphName = options.setGraphName || function () {};
    const toast = options.toast || function () {};

    if (!editor || typeof getGraph !== 'function' ||
        typeof addServiceNode !== 'function' || typeof addInputNode !== 'function') {
      throw new Error('AIGraphBridge.create requires editor, getGraph, addServiceNode and addInputNode');
    }

    function graphChangedError() {
      const error = new Error('The graph changed while AI was preparing this edit. Review the latest graph and ask again.');
      error.code = 'graph_changed';
      return error;
    }

    function nodeElement(id) {
      if (typeof document === 'undefined') return null;
      return document.getElementById('node-' + id);
    }

    function drawflowData() {
      if (editor.drawflow && editor.module && editor.drawflow.drawflow[editor.module]) {
        return editor.drawflow.drawflow[editor.module].data;
      }
      const exported = editor.export && editor.export();
      return exported && exported.drawflow && exported.drawflow.Home
        ? exported.drawflow.Home.data : {};
    }

    function setPosition(id, x, y) {
      const data = drawflowData();
      const row = data && data[id];
      if (!row) return;
      x = Number(x) || 0; y = Number(y) || 0;
      row.pos_x = x; row.pos_y = y;
      const element = nodeElement(id);
      if (element) { element.style.left = x + 'px'; element.style.top = y + 'px'; }
      if (typeof editor.updateConnectionNodes === 'function') {
        editor.updateConnectionNodes('node-' + id);
      }
    }

    function setInputValue(id, node) {
      const element = nodeElement(id);
      const field = element && element.querySelector('[data-value]');
      if (!field) return;
      const value = node.value == null ? '' : String(node.value);
      if (field.tagName === 'SELECT' && value &&
          !Array.from(field.options || []).some(option => option.value === value)) {
        const option = field.ownerDocument.createElement('option');
        option.value = value; option.textContent = value;
        field.appendChild(option);
      }
      field.value = value;
      if (typeof field.dispatchEvent === 'function' && typeof Event !== 'undefined') {
        field.dispatchEvent(new Event('change', {bubbles:true}));
      }
      const preview = element.querySelector('[data-preview]');
      if (preview && /^https?:\/\//.test(value)) {
        preview.src = value;
        preview.hidden = false;
        if (preview.tagName === 'VIDEO' && preview.play) preview.play().catch(function () {});
      } else if (preview) {
        preview.hidden = true;
      }
    }

    function applyPresentation(id, node) {
      const metadata = getMeta(id);
      const params = node.params || {};
      const previousLabel = metadata && String(metadata.label || '');
      if (metadata) {
        metadata.label = String(params._label || '');
        metadata.displayMode = params._display_mode || metadata.displayMode;
        if (node.kind === 'service' &&
            (typeof metadata.followInputSize === 'boolean' ||
             ('width' in params && 'height' in params) || '_follow_input_size' in params)) {
          metadata.followInputSize = params._follow_input_size !== false;
        }
        // The graph handed to the agent already carries the flag, and the
        // server merges an edit into the params it was given, so what comes
        // back is authoritative in both directions: present means bypassed,
        // absent means the node is in the run again.
        metadata.disabled = params._disabled === true;
      }
      if (applyBypass) applyBypass(id, params._disabled === true);
      const element = nodeElement(id);
      const heading = element && element.querySelector('.nhead b');
      if (heading) {
        if (!heading.dataset.aiDefaultHeading) {
          const fallback = node.kind === 'input'
            ? ({image:'Image in', video:'Video in', text:'Text in', avatar:'Avatar'}[node.entity_type] || 'Input')
            : String(node.service || 'Node').replace(/^control_/, 'ControlNet - ').replace(/_/g, ' ')
              .replace(/^3dmodel$/i, '3D model').replace(/\b\w/g, value => value.toUpperCase());
          heading.dataset.aiDefaultHeading = previousLabel ? fallback : heading.textContent;
        }
        heading.textContent = params._label
          ? String(params._label) : heading.dataset.aiDefaultHeading;
      }
      if (nodeDisplay && typeof nodeDisplay.applyNode === 'function' && params._display_mode) {
        nodeDisplay.applyNode(id, params._display_mode);
      }
    }

    function markExplicitControls(id, names) {
      const element = nodeElement(id);
      if (!element) return;
      (names || []).forEach(name => {
        if (name === 'checkpoint' || name === 'lora' || name === '_label' || name === '_display_mode') return;
        let control = null;
        try { control = element.querySelector('[data-param="' + CSS.escape(name) + '"]'); }
        catch (_) { control = element.querySelector('[data-param="' + name.replace(/"/g, '') + '"]'); }
        if (control) control.dataset.touched = 'yes';
      });
    }

    function portClass(id, field, direction) {
      const metadata = getMeta(id);
      if (!metadata) throw new Error('No node metadata for ' + id);
      const fields = direction === 'output' ? metadata.outFields : metadata.inFields;
      const index = (fields || []).indexOf(field);
      if (index < 0) throw new Error('No ' + direction + ' field ' + field + ' on node ' + id);
      return (direction === 'output' ? 'output_' : 'input_') + (index + 1);
    }

    function removeConnection(link) {
      editor.removeSingleConnection(
        String(link.from), String(link.to),
        portClass(String(link.from), link.output, 'output'),
        portClass(String(link.to), link.input, 'input'));
    }

    function addConnection(link) {
      editor.addConnection(
        String(link.from), String(link.to),
        portClass(String(link.from), link.output, 'output'),
        portClass(String(link.to), link.input, 'input'));
    }

    async function applyGraph(validatedGraph, context) {
      context = context || {};
      if (!validatedGraph || !Array.isArray(validatedGraph.nodes) || !Array.isArray(validatedGraph.links)) {
        throw new Error('The graph validator returned an incomplete graph.');
      }
      const baseGraph = context.baseGraph || {};
      const currentBefore = getGraph();
      if (!sameGraph(currentBefore, baseGraph)) throw graphChangedError();

      const currentById = new Map((currentBefore.nodes || []).map(node => [String(node.id), node]));
      const wantedById = new Map(validatedGraph.nodes.map(node => [String(node.id), node]));
      const mapping = new Map();
      const removed = [];
      const added = [];
      const reused = [];
      const explicitByNode = explicitParams(context.operations || []);

      currentById.forEach((node, id) => {
        const wanted = wantedById.get(id);
        if (wanted && identityMatches(node, wanted)) {
          mapping.set(id, id);
          reused.push(id);
        } else {
          removed.push(id);
        }
      });

      // Removing a node also removes its Drawflow connections. Forget runtime
      // ownership first, so a farm task that later finishes cannot write into
      // a replacement node that happens to receive the same visual position.
      if (removed.length) forgetNodes(removed);
      removed.forEach(id => editor.removeNodeId('node-' + id));

      for (const node of validatedGraph.nodes) {
        const wantedId = String(node.id);
        if (mapping.has(wantedId)) continue;
        const actualId = node.kind === 'input'
          ? addInputNode(node.entity_type, node.x, node.y, node.value || '', node.params || {})
          : addServiceNode(node.service, node.x, node.y, node.params || {});
        if (actualId == null) throw new Error('Could not add validated node ' + wantedId);
        mapping.set(wantedId, String(actualId));
        added.push(String(actualId));
      }

      const recommendationJobs = [];
      for (const node of validatedGraph.nodes) {
        const wantedId = String(node.id);
        const actualId = mapping.get(wantedId);
        const previous = currentById.get(wantedId);
        const paramsChanged = !previous || !paramsEqual(previous.params, node.params);
        const valueChanged = node.kind === 'input' &&
          (!previous || String(previous.value || '') !== String(node.value || ''));
        if (node.kind === 'service' && paramsChanged) {
          applyParams(actualId, clone(node.params || {}));
          markExplicitControls(actualId, explicitByNode.get(wantedId));
        }
        if (node.kind === 'input' && valueChanged) setInputValue(actualId, node);
        setPosition(actualId, node.x, node.y);
        applyPresentation(actualId, node);

        const modelChanges = node.kind === 'service'
          ? changedModelFields(previous && previous.params, node.params) : [];
        if (modelChanges.length && typeof applyRecommended === 'function') {
          // applyRecommended reads the now-current checkpoint/LoRA controls.
          // Explicit operation fields were marked touched above, and its own
          // mapping never changes width/height.
          recommendationJobs.push(Promise.resolve(applyRecommended(actualId, {})));
        }
      }

      const desiredLinks = validatedGraph.links.map(link => ({
        from: mapping.get(String(link.from)), output: String(link.output),
        to: mapping.get(String(link.to)), input: String(link.input)
      }));
      if (desiredLinks.some(link => !link.from || !link.to)) {
        throw new Error('A validated connection refers to a node that was not created.');
      }
      const desiredKeys = new Set(desiredLinks.map(linkKey));
      const currentLinks = (getGraph().links || []).map(link => ({
        from:String(link.from), output:String(link.output),
        to:String(link.to), input:String(link.input)
      }));
      const currentKeys = new Set(currentLinks.map(linkKey));
      currentLinks.filter(link => !desiredKeys.has(linkKey(link))).forEach(removeConnection);
      desiredLinks.filter(link => !currentKeys.has(linkKey(link))).forEach(addConnection);

      setGraphName(String(validatedGraph.name || 'Untitled'));

      const invalidated = [];
      (context.invalidatedIds || []).forEach(wantedId => {
        const actualId = mapping.get(String(wantedId));
        if (!actualId) return;
        invalidated.push(String(actualId));
        invalidateNodeAndDownstream(actualId);
      });
      const appliedGraph = getGraph();
      const appliedById = new Map((appliedGraph.nodes || []).map(node => [String(node.id), node]));
      const expectedTranslated = {
        name: validatedGraph.name,
        nodes: validatedGraph.nodes.map(node => {
          const wantedId = String(node.id);
          const actualId = mapping.get(wantedId);
          const translated = Object.assign({}, clone(node), {id:actualId});
          const actual = appliedById.get(String(actualId));
          // A newly created Drawflow service materialises every UI default,
          // while the validated add_node operation is allowed to state only
          // the parameters it cares about.  Keep those harmless defaults in
          // the expected representation, but first prove every specified
          // value really landed on the control.
          if (!currentById.has(wantedId) || !identityMatches(currentById.get(wantedId), node)) {
            if (!actual || !specifiedParamsMatch(actual.params, node.params)) {
              const error = new Error('A specified parameter was not applied to new node ' + wantedId + '.');
              error.code = 'graph_apply_mismatch';
              throw error;
            }
            translated.params = clone(actual.params || {});
          }
          return translated;
        }),
        links: desiredLinks,
        instance_id: appliedGraph.instance_id
      };
      if (!sameGraph(appliedGraph, expectedTranslated)) {
        const error = new Error('The validated edit could not be represented exactly on the canvas. Reload the graph before retrying.');
        error.code = 'graph_apply_mismatch';
        throw error;
      }
      // Recommendation fetches may materialise additional safe defaults that
      // were not themselves part of the validator response.  First prove the
      // validated graph was represented exactly, then let those asynchronous
      // defaults fill only untouched controls.
      await Promise.all(recommendationJobs);
      toast('AI graph edit applied. Review it before rendering.');
      const finalGraph = getGraph();
      return {
        graph: finalGraph,
        idMapping: Object.fromEntries(mapping),
        addedIds: added,
        removedIds: removed,
        reusedIds: reused,
        invalidatedIds: invalidated
      };
    }

    return Object.freeze({applyGraph:applyGraph});
  }

  create.graphForCompare = graphForCompare;
  create.sameGraph = sameGraph;
  global.AIGraphBridge = Object.freeze({create:create});
})(typeof window !== 'undefined' ? window : globalThis);

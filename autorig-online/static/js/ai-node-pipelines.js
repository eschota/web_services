/**
 * Bulk pipeline functions and tidy layout for the node editor.
 *
 * "All video pipelines from this image" is one Video node per runnable
 * combination of checkpoint and compatible LoRA, each already wired to the
 * picture it starts from. The combinations are read from the live model
 * catalogue, so a model added on the farm appears here without this file
 * changing, and a model the farm cannot run never does.
 *
 * Layout lives here too because the two belong together: creating a dozen
 * nodes at once is only useful if they land somewhere a person can read them.
 * Both halves are written as plain functions over sizes and links so they can
 * be tested without a browser; the DOM work is confined to `install`.
 */
(function (global) {
  'use strict';

  const DEFAULT_GAP_X = 64;
  const DEFAULT_GAP_Y = 36;
  const DEFAULT_MAX_HEIGHT = 1800;
  const MAX_NAME = 34;

  function usableModel(entry) {
    return !!entry && entry.usable !== false && !!entry.file;
  }

  /** The shortest thing that still identifies a model on a node header. */
  function modelName(entry) {
    const name = String((entry && (entry.title || entry.file)) || '')
      .replace(/\.safetensors$/i, '').trim();
    return name.length > MAX_NAME ? name.slice(0, MAX_NAME - 1) + '…' : name;
  }

  /**
   * A LoRA trained on another base does not merely look wrong, it refuses to
   * load, so sharing the checkpoint family is a hard filter and not a
   * preference. A LoRA with no family at all is never assumed compatible.
   */
  function sameFamily(lora, checkpoint) {
    const left = String((lora && lora.family) || '');
    const right = String((checkpoint && checkpoint.family) || '');
    return !!left && left === right;
  }

  /** Every runnable checkpoint, once alone and once per LoRA of its family. */
  function videoPipelines(catalogue) {
    const checkpoints = ((catalogue && catalogue.checkpoints_array) || []).filter(usableModel);
    const loras = ((catalogue && catalogue.loras_array) || []).filter(usableModel);
    const combinations = [];
    checkpoints.forEach(checkpoint => {
      combinations.push({ checkpoint: checkpoint.file, lora: '', label: modelName(checkpoint) });
      loras.filter(lora => sameFamily(lora, checkpoint)).forEach(lora => {
        combinations.push({
          checkpoint: checkpoint.file, lora: lora.file,
          label: modelName(checkpoint) + ' + ' + modelName(lora)
        });
      });
    });
    return combinations;
  }

  /**
   * What one combination puts on a Video node.
   *
   * Steps, CFG, sampler and scheduler are deliberately absent: each model
   * carries its own author schedule, and the node already resolves those from
   * the catalogue. Width and height are absent for the same reason — a new
   * node follows the size of the image it is wired to until somebody types a
   * size by hand.
   */
  function pipelineParams(combination, options) {
    options = options || {};
    const params = {
      _label: (options.prefix || 'Video') + ' · ' + combination.label,
      checkpoint: combination.checkpoint || '',
      lora: combination.lora || ''
    };
    if (options.displayMode) params._display_mode = options.displayMode;
    return params;
  }

  function gapOf(value, fallback) {
    return Number.isFinite(Number(value)) ? Number(value) : fallback;
  }

  function defaultColumns(count) {
    return Math.max(1, Math.min(4, Math.ceil(Math.sqrt(Math.max(1, count)))));
  }

  /**
   * A grid whose columns are as wide as their widest node and whose rows are
   * as tall as their tallest, so nothing overlaps whatever display mode the
   * nodes are in.
   */
  function gridPositions(sizes, options) {
    options = options || {};
    sizes = Array.isArray(sizes) ? sizes : [];
    if (!sizes.length) return [];
    const gapX = gapOf(options.gapX, DEFAULT_GAP_X);
    const gapY = gapOf(options.gapY, DEFAULT_GAP_Y);
    const columns = Math.max(1, Math.min(
      Math.round(Number(options.columns)) || defaultColumns(sizes.length), sizes.length));
    const columnWidth = [];
    const rowHeight = [];
    sizes.forEach((size, index) => {
      const column = index % columns;
      const row = Math.floor(index / columns);
      columnWidth[column] = Math.max(columnWidth[column] || 0, Number(size.width) || 0);
      rowHeight[row] = Math.max(rowHeight[row] || 0, Number(size.height) || 0);
    });
    const left = [];
    const top = [];
    let x = Number(options.x) || 0;
    columnWidth.forEach((width, index) => { left[index] = x; x += width + gapX; });
    let y = Number(options.y) || 0;
    rowHeight.forEach((height, index) => { top[index] = y; y += height + gapY; });
    return sizes.map((size, index) => ({
      id: String(size.id),
      x: left[index % columns],
      y: top[Math.floor(index / columns)]
    }));
  }

  /**
   * Split one depth column into side-by-side stacks of a readable height.
   *
   * Fourteen video pipelines all hang off the same picture, so they share a
   * depth and would otherwise become a single column several screens tall.
   * Breaking that into stacks keeps the depth order — every stack still sits
   * left of the next depth — and keeps the block on screen.
   */
  function stacksOf(nodes, maxHeight, gapY) {
    const total = nodes.reduce((sum, node) => sum + node.height, 0) +
      gapY * Math.max(0, nodes.length - 1);
    const parts = Math.max(1, Math.ceil(total / Math.max(1, maxHeight)));
    const size = Math.ceil(nodes.length / parts);
    const stacks = [];
    for (let index = 0; index < nodes.length; index += size) {
      stacks.push(nodes.slice(index, index + size));
    }
    return stacks;
  }

  /**
   * Columns by graph depth: everything a node waits for stands to its left.
   *
   * Nodes keep their relative vertical order inside a column so a graph a
   * person has already read does not reshuffle itself under them.
   */
  function arrangeByDepth(graph, options) {
    options = options || {};
    const gapX = gapOf(options.gapX, DEFAULT_GAP_X);
    const gapY = gapOf(options.gapY, DEFAULT_GAP_Y);
    const nodes = ((graph && graph.nodes) || []).map(node => ({
      id: String(node.id),
      width: Math.max(1, Number(node.width) || 0),
      height: Math.max(1, Number(node.height) || 0),
      x: Number(node.x) || 0,
      y: Number(node.y) || 0
    }));
    if (!nodes.length) return [];
    const known = new Set(nodes.map(node => node.id));
    const parents = new Map(nodes.map(node => [node.id, []]));
    ((graph && graph.links) || []).forEach(link => {
      const from = String(link.from);
      const to = String(link.to);
      if (from === to || !known.has(from) || !known.has(to)) return;
      parents.get(to).push(from);
    });
    const depth = new Map(nodes.map(node => [node.id, 0]));
    // One pass per node is the longest path an acyclic graph can hold, and a
    // cycle simply stops improving, so this can never spin.
    for (let pass = 0; pass < nodes.length; pass++) {
      let changed = false;
      nodes.forEach(node => {
        const wanted = parents.get(node.id).reduce(
          (deepest, parent) => Math.max(deepest, depth.get(parent) + 1), 0);
        if (wanted > depth.get(node.id)) { depth.set(node.id, wanted); changed = true; }
      });
      if (!changed) break;
    }
    const columns = [];
    nodes.slice()
      .sort((a, b) => (a.y - b.y) || (a.x - b.x) || a.id.localeCompare(b.id))
      .forEach(node => {
        const index = depth.get(node.id);
        (columns[index] = columns[index] || []).push(node);
      });
    const maxHeight = Number(options.maxHeight) > 0
      ? Number(options.maxHeight) : DEFAULT_MAX_HEIGHT;
    const positions = [];
    const originY = Number(options.y) || 0;
    let x = Number(options.x) || 0;
    columns.forEach(column => {
      if (!column || !column.length) return;
      stacksOf(column, maxHeight, gapY).forEach(stack => {
        let y = originY;
        let width = 0;
        stack.forEach(node => {
          positions.push({ id: node.id, x: x, y: y });
          y += node.height + gapY;
          width = Math.max(width, node.width);
        });
        x += width + gapX;
      });
    });
    return positions;
  }

  /* ------------------------------------------------------------ DOM binding */

  function install(options) {
    options = options || {};
    const editor = options.editor;
    const getMeta = options.getMeta;
    const addServiceNode = options.addServiceNode;
    const getNodeElement = options.getNodeElement;
    const moveNode = options.moveNode;
    const exportGraph = options.exportGraph;
    const loadModels = options.loadModels;
    const imageOutput = options.imageOutput;
    const onNodesAdded = typeof options.onNodesAdded === 'function' ? options.onNodesAdded : function () {};
    const toast = typeof options.toast === 'function' ? options.toast : function () {};
    const nodeLimit = Math.max(1, Number(options.nodeLimit) || 200);
    if (!editor || !getMeta || !addServiceNode || !getNodeElement || !moveNode ||
        !exportGraph || !loadModels || !imageOutput) {
      throw new Error('AINodePipelines.install requires the editor and its node helpers');
    }
    let busy = false;

    // `offsetWidth` is the laid-out size in graph coordinates; a bounding box
    // would be multiplied by the canvas zoom and place everything wrongly.
    function nodeSize(id) {
      const element = getNodeElement(id);
      return {
        width: (element && element.offsetWidth) || 260,
        height: (element && element.offsetHeight) || 170
      };
    }

    function connect(fromId, outputIndex, toId, field) {
      const target = getMeta(toId);
      const inputIndex = ((target && target.inFields) || []).indexOf(field);
      if (inputIndex < 0) return false;
      editor.addConnection(fromId, toId, 'output_' + (outputIndex + 1), 'input_' + (inputIndex + 1));
      return true;
    }

    function place(ids, origin) {
      const sizes = ids.map(id => Object.assign({ id: id }, nodeSize(id)));
      gridPositions(sizes, { x: origin.x, y: origin.y, gapX: options.gapX, gapY: options.gapY })
        .forEach(position => moveNode(position.id, Math.round(position.x), Math.round(position.y)));
    }

    /**
     * More than once, because a node grows a little as its model preview
     * arrives, and because a tab that is not painting never gets a frame to
     * measure in. Every pass recomputes the whole block, so the last one wins.
     */
    function placeWhenMeasured(ids, origin) {
      const run = () => place(ids, origin);
      requestAnimationFrame(() => requestAnimationFrame(run));
      setTimeout(run, 80);
      setTimeout(run, 700);
    }

    function arrange(ids) {
      const graph = exportGraph();
      if (!graph || !Array.isArray(graph.nodes)) return 0;
      const wanted = new Set((ids || []).map(String));
      const chosen = graph.nodes.filter(node => !wanted.size || wanted.has(String(node.id)));
      if (chosen.length < 2) {
        toast('Select at least two nodes to arrange, or clear the selection to arrange everything.');
        return 0;
      }
      const kept = new Set(chosen.map(node => String(node.id)));
      const sized = chosen.map(node => Object.assign({
        id: String(node.id), x: Number(node.x) || 0, y: Number(node.y) || 0
      }, nodeSize(node.id)));
      const positions = arrangeByDepth({
        nodes: sized,
        links: (graph.links || []).filter(link =>
          kept.has(String(link.from)) && kept.has(String(link.to)))
      }, {
        x: Math.min.apply(null, sized.map(node => node.x)),
        y: Math.min.apply(null, sized.map(node => node.y)),
        gapX: options.gapX, gapY: options.gapY
      });
      positions.forEach(position =>
        moveNode(position.id, Math.round(position.x), Math.round(position.y)));
      toast(positions.length + ' nodes arranged in columns by depth.');
      return positions.length;
    }

    async function createVideoPipelines(sourceId, settings) {
      settings = settings || {};
      if (busy) return null;
      const sourceMeta = getMeta(sourceId);
      const outputField = imageOutput(sourceId);
      if (!sourceMeta || !outputField) { toast('That node does not produce an image.'); return null; }
      const outputIndex = (sourceMeta.outFields || []).indexOf(outputField);
      if (outputIndex < 0) { toast('That node does not produce an image.'); return null; }
      busy = true;
      try {
        const catalogue = await loadModels('video');
        const combinations = videoPipelines(catalogue);
        if (!combinations.length) {
          toast('No video model the farm can run is available right now.');
          return null;
        }
        const graph = exportGraph();
        const source = (graph.nodes || []).find(node => String(node.id) === String(sourceId));
        if (!source) { toast('That node is no longer on the canvas.'); return null; }
        if ((graph.nodes || []).length + combinations.length > nodeLimit) {
          toast('The graph is limited to ' + nodeLimit + ' nodes; ' + combinations.length +
                ' more would not fit.');
          return null;
        }
        const origin = {
          x: (Number(source.x) || 0) + nodeSize(sourceId).width + 130,
          y: Number(source.y) || 0
        };
        const created = [];
        let wired = 0;
        combinations.forEach((combination, index) => {
          const params = pipelineParams(combination, {
            prefix: settings.prefix || 'Video',
            displayMode: sourceMeta.displayMode
          });
          const id = addServiceNode('video', origin.x, origin.y + index * 30, params);
          if (id == null) return;
          created.push(String(id));
          if (connect(sourceId, outputIndex, id, 'image')) wired += 1;
          if (settings.endFrame) connect(sourceId, outputIndex, id, 'image_url_end');
        });
        if (!created.length) { toast('The Video service is not available right now.'); return null; }
        placeWhenMeasured(created, origin);
        onNodesAdded(created);
        toast(created.length + ' video pipelines created' +
              (settings.endFrame ? ', first frame also used as the last frame' : '') +
              (wired < created.length ? '; ' + (created.length - wired) + ' could not be wired' : '') + '.');
        return created;
      } catch (error) {
        toast('The video model catalogue could not be read: ' + (error && error.message || error));
        return null;
      } finally { busy = false; }
    }

    /** What the context menu offers for the node it was opened on. */
    function functionsFor(id) {
      if (!imageOutput(id)) return [];
      return [
        {
          label: 'All video pipelines',
          title: 'One Video node per runnable checkpoint and compatible LoRA, each wired to this image',
          run: () => createVideoPipelines(id, { endFrame: false })
        },
        {
          label: 'All video pipelines, frame-to-frame',
          title: 'The same, with this image as both the first and the last frame, so each clip loops',
          run: () => createVideoPipelines(id, { endFrame: true, prefix: 'Loop' })
        }
      ];
    }

    return { functionsFor, createVideoPipelines, arrange };
  }

  global.AINodePipelines = {
    videoPipelines: videoPipelines,
    pipelineParams: pipelineParams,
    gridPositions: gridPositions,
    arrangeByDepth: arrangeByDepth,
    install: install
  };
})(window);

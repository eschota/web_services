import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.join(here, '..', 'ai-node-pipelines.js');
const context = {window: {}};
vm.runInNewContext(fs.readFileSync(sourcePath, 'utf8'), context, {filename: sourcePath});
const api = context.window.AINodePipelines;

// The module runs in its own realm, so its arrays and objects do not share a
// prototype with this file's literals; compare the plain data instead.
const plain = value => JSON.parse(JSON.stringify(value));

const CATALOGUE = {
  checkpoints_array: [
    {file: 'ltx-2.3-distilled.safetensors', title: 'LTX 2.3 distilled 1.1', family: 'ltx23', usable: true},
    {file: 'ltx10eros_v14.safetensors', title: 'LTX 10Eros', family: 'ltx23', usable: true},
    {file: 'retired.safetensors', title: 'Retired', family: 'ltx23', usable: false}
  ],
  loras_array: [
    {file: 'Pixar_Toon.safetensors', title: 'LTX 2.3 - Pixar CGI Toon Style', family: 'ltx23', usable: true},
    {file: 'mvmt_lora_v2_600.safetensors', title: 'Better Human Motion', family: 'ltx23', usable: true},
    {file: 'DreamLTXV.safetensors', title: 'DreamLTXV', family: 'ltx', usable: false},
    {file: 'FluxStyle.safetensors', title: 'Flux Style', family: 'flux', usable: true},
    {file: 'homeless.safetensors', title: 'No family', family: '', usable: true}
  ]
};

function rectangles(positions, sizes) {
  const size = new Map(sizes.map(item => [String(item.id), item]));
  return positions.map(position => {
    const item = size.get(String(position.id));
    return {id: position.id, left: position.x, top: position.y,
      right: position.x + item.width, bottom: position.y + item.height};
  });
}

function overlaps(boxes) {
  for (let i = 0; i < boxes.length; i++) {
    for (let j = i + 1; j < boxes.length; j++) {
      const a = boxes[i], b = boxes[j];
      if (a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom) {
        return [a.id, b.id];
      }
    }
  }
  return null;
}

test('every runnable checkpoint appears alone and with each LoRA of its own family', () => {
  const combinations = api.videoPipelines(CATALOGUE);
  assert.equal(combinations.length, 6);
  assert.deepEqual(plain(combinations.map(item => item.checkpoint + '|' + item.lora)), [
    'ltx-2.3-distilled.safetensors|',
    'ltx-2.3-distilled.safetensors|Pixar_Toon.safetensors',
    'ltx-2.3-distilled.safetensors|mvmt_lora_v2_600.safetensors',
    'ltx10eros_v14.safetensors|',
    'ltx10eros_v14.safetensors|Pixar_Toon.safetensors',
    'ltx10eros_v14.safetensors|mvmt_lora_v2_600.safetensors'
  ]);
});

test('a LoRA of another family, an unrunnable one, and one with no family are never offered', () => {
  const files = api.videoPipelines(CATALOGUE).map(item => item.lora);
  ['FluxStyle.safetensors', 'DreamLTXV.safetensors', 'homeless.safetensors']
    .forEach(file => assert.equal(files.includes(file), false, file + ' must not be paired'));
  assert.equal(api.videoPipelines(CATALOGUE).some(item =>
    item.checkpoint === 'retired.safetensors'), false);
});

test('an empty or malformed catalogue produces no pipelines instead of throwing', () => {
  assert.deepEqual(plain(api.videoPipelines(null)), []);
  assert.deepEqual(plain(api.videoPipelines({})), []);
  assert.deepEqual(plain(api.videoPipelines({checkpoints_array: [{title: 'no file', family: 'ltx23'}]})), []);
});

test('labels name the checkpoint and the LoRA, and sampling is left automatic', () => {
  const combinations = api.videoPipelines(CATALOGUE);
  const params = api.pipelineParams(combinations[1], {prefix: 'Video', displayMode: 'small'});
  assert.equal(params._label, 'Video · LTX 2.3 distilled 1.1 + LTX 2.3 - Pixar CGI Toon Style');
  assert.equal(params.checkpoint, 'ltx-2.3-distilled.safetensors');
  assert.equal(params.lora, 'Pixar_Toon.safetensors');
  assert.equal(params._display_mode, 'small');
  ['steps', 'cfg', 'sampler', 'scheduler', 'width', 'height', 'lora_strength']
    .forEach(name => assert.equal(name in params, false, name + ' must stay automatic'));
});

test('a plain checkpoint node clears the LoRA rather than inheriting a stale one', () => {
  const params = api.pipelineParams(api.videoPipelines(CATALOGUE)[0], {prefix: 'Loop'});
  assert.equal(params.lora, '');
  assert.equal(params._label, 'Loop · LTX 2.3 distilled 1.1');
  assert.equal('_display_mode' in params, false);
});

test('a created block is a grid of column-wide and row-tall cells that never overlap', () => {
  const sizes = Array.from({length: 7}, (_, index) => ({
    id: 'n' + index, width: index === 3 ? 380 : 300, height: index === 1 ? 260 : 180
  }));
  const positions = api.gridPositions(sizes, {x: 500, y: 120, columns: 3, gapX: 40, gapY: 20});
  assert.equal(positions.length, 7);
  assert.equal(positions[0].x, 500);
  assert.equal(positions[0].y, 120);
  // Row one is as tall as node 1, and column two is as wide as node 3.
  assert.equal(positions[3].y, 120 + 260 + 20);
  assert.equal(positions[4].x, 500 + 380 + 40);
  assert.equal(overlaps(rectangles(positions, sizes)), null);
});

test('grid columns default to a square-ish block and never exceed the node count', () => {
  const one = api.gridPositions([{id: 'a', width: 100, height: 50}], {});
  assert.deepEqual(plain(one), [{id: 'a', x: 0, y: 0}]);
  const many = api.gridPositions(Array.from({length: 14}, (_, index) =>
    ({id: 'n' + index, width: 100, height: 50})), {x: 0, y: 0, gapX: 10, gapY: 10});
  assert.equal(new Set(many.map(item => item.x)).size, 4);
  assert.equal(api.gridPositions([], {}).length, 0);
});

test('arrange puts every node one column right of the deepest thing it waits for', () => {
  const nodes = [
    {id: 'source', width: 240, height: 200, x: 900, y: 40},
    {id: 'video-a', width: 300, height: 180, x: 100, y: 10},
    {id: 'video-b', width: 300, height: 180, x: 120, y: 400},
    {id: 'frame', width: 260, height: 160, x: 60, y: 900}
  ];
  const links = [
    {from: 'source', to: 'video-a'}, {from: 'source', to: 'video-b'},
    {from: 'video-b', to: 'frame'}
  ];
  const positions = api.arrangeByDepth({nodes, links}, {x: 0, y: 0, gapX: 50, gapY: 30});
  const at = new Map(positions.map(item => [item.id, item]));
  assert.equal(at.get('source').x, 0);
  assert.equal(at.get('video-a').x, 240 + 50);
  assert.equal(at.get('video-b').x, 240 + 50);
  assert.equal(at.get('frame').x, 240 + 50 + 300 + 50);
  // Inside a column the earlier node keeps the higher place.
  assert.equal(at.get('video-a').y, 0);
  assert.equal(at.get('video-b').y, 180 + 30);
  assert.equal(overlaps(rectangles(positions, nodes)), null);
});

test('arrange starts at the origin it is given and keeps unlinked nodes in the first column', () => {
  const nodes = [
    {id: 'lonely-b', width: 200, height: 100, x: 0, y: 500},
    {id: 'lonely-a', width: 200, height: 100, x: 0, y: 10}
  ];
  const positions = api.arrangeByDepth({nodes, links: []}, {x: 64, y: 88, gapY: 12});
  assert.deepEqual(plain(positions), [
    {id: 'lonely-a', x: 64, y: 88},
    {id: 'lonely-b', x: 64, y: 88 + 100 + 12}
  ]);
});

test('a cycle and a link to a node outside the selection cannot hang or shift the layout', () => {
  const nodes = [
    {id: 'a', width: 100, height: 50, x: 0, y: 0},
    {id: 'b', width: 100, height: 50, x: 0, y: 100}
  ];
  const positions = api.arrangeByDepth({nodes, links: [
    {from: 'a', to: 'b'}, {from: 'b', to: 'a'}, {from: 'a', to: 'a'},
    {from: 'outside', to: 'b'}, {from: 'b', to: 'outside'}
  ]}, {x: 0, y: 0, gapX: 20, gapY: 20});
  assert.equal(positions.length, 2);
  assert.equal(overlaps(rectangles(positions, nodes)), null);
  assert.deepEqual(plain(api.arrangeByDepth({nodes: [], links: []}, {})), []);
});

/* ---------------------------------------------------- created nodes and wires */

// The module reaches for these when it lays a freshly created block out.
context.requestAnimationFrame = callback => { callback(); return 1; };
context.setTimeout = callback => { callback(); return 1; };

function host(options) {
  options = options || {};
  const state = {
    connections: [], created: [], moved: [], toasts: [], added: [],
    nodes: options.nodes || [{id: 'src', kind: 'input', x: 100, y: 60}],
    meta: new Map([['src', {kind: 'input', entityType: 'image', displayMode: 'medium',
      inFields: [], outFields: ['value']}]])
  };
  let next = 0;
  const install = api.install({
    editor: {
      addConnection: (from, to, outputClass, inputClass) =>
        state.connections.push([String(from), String(to), outputClass, inputClass])
    },
    getMeta: id => state.meta.get(String(id)) || null,
    addServiceNode: (service, x, y, params) => {
      if (options.refuse) return null;
      const id = 'v' + (++next);
      state.created.push({id, service, x, y, params});
      state.nodes.push({id, kind: 'service', service, x, y, params});
      state.meta.set(id, {kind: 'service', service,
        inFields: ['image', 'image_url_end', 'prompt'], outFields: ['video_url_string']});
      return id;
    },
    getNodeElement: id => ({offsetWidth: id === 'src' ? 240 : 300, offsetHeight: 180}),
    moveNode: (id, x, y) => state.moved.push({id: String(id), x, y}),
    exportGraph: () => ({nodes: state.nodes.slice(), links: []}),
    imageOutput: id => (state.meta.get(String(id)) || {}).kind === 'input' ? 'value' : '',
    loadModels: async () => (options.catalogue === undefined ? CATALOGUE : options.catalogue),
    nodeLimit: options.nodeLimit || 200,
    toast: message => state.toasts.push(message),
    onNodesAdded: ids => state.added.push(ids.slice())
  });
  return {api: install, state};
}

test('a pipeline run creates one wired Video node per combination', async () => {
  const {api: pipelines, state} = host();
  const created = await pipelines.createVideoPipelines('src', {endFrame: false});
  assert.equal(created.length, 6);
  assert.equal(state.created.length, 6);
  assert.ok(state.created.every(node => node.service === 'video'));
  // The source publishes its picture on output 1; the Video node takes it on
  // its first input, which is `image`.
  assert.deepEqual(plain(state.connections), [
    ['src', 'v1', 'output_1', 'input_1'], ['src', 'v2', 'output_1', 'input_1'],
    ['src', 'v3', 'output_1', 'input_1'], ['src', 'v4', 'output_1', 'input_1'],
    ['src', 'v5', 'output_1', 'input_1'], ['src', 'v6', 'output_1', 'input_1']
  ]);
  assert.deepEqual(plain(state.added), [['v1', 'v2', 'v3', 'v4', 'v5', 'v6']]);
});

test('frame-to-frame also wires the same picture into the last-frame socket', async () => {
  const {api: pipelines, state} = host();
  await pipelines.createVideoPipelines('src', {endFrame: true, prefix: 'Loop'});
  assert.equal(state.connections.length, 12);
  assert.equal(state.connections.filter(item => item[3] === 'input_2').length, 6);
  state.connections.filter(item => item[3] === 'input_2')
    .forEach(item => assert.equal(item[0], 'src'));
  assert.ok(state.created.every(node => node.params._label.startsWith('Loop · ')));
});

test('created nodes carry their checkpoint and LoRA and land in a measured grid', async () => {
  const {api: pipelines, state} = host();
  await pipelines.createVideoPipelines('src', {});
  assert.equal(state.created[0].params.checkpoint, 'ltx-2.3-distilled.safetensors');
  assert.equal(state.created[1].params.lora, 'Pixar_Toon.safetensors');
  assert.equal(state.created[0].params._display_mode, 'medium');
  // Placed to the right of a 240 px source that sits at x = 100.
  const first = state.moved.find(item => item.id === 'v1');
  assert.equal(first.x, 100 + 240 + 130);
  assert.equal(first.y, 60);
  assert.equal(new Set(state.moved.map(item => item.id)).size, 6);
});

test('a graph already at its node limit is left untouched', async () => {
  const {api: pipelines, state} = host({nodeLimit: 4});
  assert.equal(await pipelines.createVideoPipelines('src', {}), null);
  assert.equal(state.created.length, 0);
  assert.match(state.toasts.join(' '), /limited to 4 nodes/);
});

test('functions are offered only where a picture actually comes out', () => {
  const {api: pipelines, state} = host();
  assert.equal(pipelines.functionsFor('src').length, 2);
  state.meta.set('other', {kind: 'service', service: 'text', inFields: [], outFields: ['answer_string']});
  assert.equal(pipelines.functionsFor('other').length, 0);
  assert.deepEqual(plain(pipelines.functionsFor('src').map(item => item.label)),
    ['All video pipelines', 'All video pipelines, frame-to-frame']);
});

test('an empty model catalogue says so instead of creating empty nodes', async () => {
  const {api: pipelines, state} = host({catalogue: {checkpoints_array: [], loras_array: []}});
  assert.equal(await pipelines.createVideoPipelines('src', {}), null);
  assert.equal(state.created.length, 0);
  assert.match(state.toasts.join(' '), /No video model/);
});

test('arrange moves the whole graph when nothing is selected and refuses a single node', () => {
  const {api: pipelines, state} = host({nodes: [
    {id: 'src', kind: 'input', x: 400, y: 300},
    {id: 'b', kind: 'service', x: 900, y: 80}
  ]});
  state.meta.set('b', {kind: 'service', service: 'video', inFields: [], outFields: []});
  assert.equal(pipelines.arrange([]), 2);
  assert.equal(state.moved.length, 2);
  state.moved.length = 0;
  assert.equal(pipelines.arrange(['src']), 0);
  assert.equal(state.moved.length, 0);
  assert.match(state.toasts.join(' '), /at least two nodes/);
});

test('a wide fan-out becomes several stacks instead of one screen-tall column', () => {
  const source = {id: 'src', width: 240, height: 200, x: 0, y: 0};
  const fan = Array.from({length: 14}, (_, index) =>
    ({id: 'v' + index, width: 300, height: 520, x: 900, y: index * 10}));
  const links = fan.map(node => ({from: 'src', to: node.id}));
  const positions = api.arrangeByDepth({nodes: [source].concat(fan), links},
    {x: 0, y: 0, gapX: 50, gapY: 30, maxHeight: 1800});
  const at = new Map(positions.map(item => [item.id, item]));
  const stacks = new Set(fan.map(node => at.get(node.id).x));
  assert.equal(stacks.size, 5, 'fourteen 520 px nodes need five stacks under an 1800 px budget');
  // Every stack still stands to the right of the node they all wait for.
  fan.forEach(node => assert.ok(at.get(node.id).x >= 240 + 50));
  const tallest = Math.max(...positions.map(item =>
    item.y + (item.id === 'src' ? source.height : 520)));
  assert.ok(tallest <= 1900, 'the arranged block stays a readable height, was ' + tallest);
  assert.equal(overlaps(rectangles(positions, [source].concat(fan))), null);
});

test('a column that already fits is left as one stack', () => {
  const nodes = Array.from({length: 4}, (_, index) =>
    ({id: 'n' + index, width: 200, height: 150, x: 0, y: index}));
  const positions = api.arrangeByDepth({nodes, links: []}, {x: 10, y: 10, gapY: 20});
  assert.equal(new Set(positions.map(item => item.x)).size, 1);
});

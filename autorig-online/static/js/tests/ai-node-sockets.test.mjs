import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));

function load(file, extra = {}) {
  const context = {globalThis: null, ...extra};
  context.globalThis = context;
  context.window = context;
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.resolve(here, '..', file), 'utf8'), context);
  return context;
}

const sockets = load('ai-node-sockets.js').AINodeSockets;

test('the type rule mirrors ai_graph.validate: same type or also_accepts', () => {
  assert.equal(sockets.typeAccepts('image', 'image', []), true);
  assert.equal(sockets.typeAccepts('text', 'image', []), false);
  // A multi-reference picture socket reads a video's first frame.
  assert.equal(sockets.typeAccepts('video', 'image', ['video']), true);
  assert.equal(sockets.typeAccepts('video', 'image', []), false);
  assert.equal(sockets.typeAccepts('image', 'video', ['image']), true);
  assert.equal(sockets.typeAccepts('control_depth', 'control_depth', []), true);
  assert.equal(sockets.typeAccepts('control_depth', 'image', []), false);
  assert.equal(sockets.typeAccepts('', 'image', []), false);
  assert.equal(sockets.typeAccepts('image', '', []), false);
});

test('the rule agrees with the backend catalogue for every service socket', () => {
  // ai_graph.validate: produced == accepted or produced in also_accepts.
  const catalogue = fs.readFileSync(path.resolve(here, '..', '..', '..', 'backend', 'ai_services.py'), 'utf8');
  assert.match(catalogue, /"also_accepts"/);
  const graph = fs.readFileSync(path.resolve(here, '..', '..', '..', 'backend', 'ai_graph.py'), 'utf8');
  assert.match(graph, /produced != accepted and produced not in _input_also_accepts/);
});

test('the drop radius grows as the camera pulls back, within bounds', () => {
  assert.equal(sockets.snapRadius(1), 34);
  assert.ok(sockets.snapRadius(0.25) > sockets.snapRadius(1));
  assert.equal(sockets.snapRadius(0.01), 110);
  assert.equal(sockets.snapRadius(4), 26);
  assert.equal(sockets.snapRadius(NaN), 34);
});

test('a drop snaps to the nearest candidate inside the radius only', () => {
  const candidates = [{id: 'a', x: 100, y: 100}, {id: 'b', x: 130, y: 100}];
  assert.equal(sockets.nearest(candidates, 120, 100, 40).id, 'b');
  assert.equal(sockets.nearest(candidates, 95, 102, 40).id, 'a');
  assert.equal(sockets.nearest(candidates, 300, 300, 40), null);
});

test('a wire that would close a loop is refused', () => {
  const data = {
    1: {outputs: {output_1: {connections: [{node: '2'}]}}},
    2: {outputs: {output_1: {connections: [{node: '3'}]}}},
    3: {outputs: {}},
  };
  assert.equal(sockets.wouldLoop(data, '3', '1'), true);
  assert.equal(sockets.wouldLoop(data, '1', '3'), false);
  assert.equal(sockets.wouldLoop(data, '2', '2'), true);
});

test('every entity type the server declares has a colour on the page', () => {
  const services = fs.readFileSync(path.resolve(here, '..', '..', '..', 'backend', 'ai_services.py'), 'utf8');
  const page = fs.readFileSync(path.resolve(here, '..', '..', 'nodes.html'), 'utf8');
  const block = services.slice(services.indexOf('ENTITY_TYPES'));
  const ids = [...block.matchAll(/"id":\s*([A-Z_0-9]+|"[a-z_0-9]+")/g)].slice(0, 12).map(m => m[1]);
  assert.ok(ids.length > 0);
  for (const type of sockets.TYPE_ORDER) {
    assert.match(page, new RegExp('--t-' + type + ':'), type);
    assert.match(page, new RegExp('\\[data-socket-type="' + type + '"\\]'), type);
  }
});

/* ------------------------------------------------ double-click guard */

function fakeElement(selectors, node) {
  return {
    closest(query) {
      const wanted = query.split(',').map(value => value.trim());
      for (const selector of selectors) if (wanted.includes(selector)) return {selector};
      if (query.startsWith('.drawflow-node')) return node;
      return null;
    },
    contains() { return false; },
  };
}

const display = load('ai-node-display.js', {document: {activeElement: null, body: {}}}).AINodeDisplay;

test('double-click anywhere on a node body resizes it', () => {
  const node = {id: 'node-4'};
  assert.equal(display.doubleClickTarget(fakeElement(['.nhead'], node)), node);
  assert.equal(display.doubleClickTarget(fakeElement(['.nports', '.prow'], node)), node);
  assert.equal(display.doubleClickTarget(fakeElement(['.nstate'], node)), node);
});

test('double-click on controls, sockets, media and text answers is left alone', () => {
  const node = {id: 'node-4'};
  for (const selector of ['input', 'textarea', 'select', 'button', '.mpick', '.input', '.output',
                          'img', 'video', '.ntext', 'summary', 'a', '[contenteditable]']) {
    assert.equal(display.doubleClickTarget(fakeElement([selector], node)), null, selector);
  }
});

test('double-click outside any node (a wire, the canvas) is left to Drawflow', () => {
  assert.equal(display.doubleClickTarget(fakeElement([], null)), null);
  assert.equal(display.doubleClickTarget(null), null);
});

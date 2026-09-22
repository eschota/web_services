import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '..', 'ai-node-placement.js'), 'utf8');
// The module schedules the "centre the node once it has a size" step on a
// frame and on a timer; both are captured so a test can run them on demand.
const frames = [];
const timers = [];
const context = {
  window: {},
  document: undefined,
  requestAnimationFrame: callback => { frames.push(callback); return frames.length; },
  cancelAnimationFrame: () => {},
  setTimeout: callback => { timers.push(callback); return timers.length; }
};
vm.runInNewContext(source, context);

function flushScheduled() {
  const pending = frames.splice(0).concat(timers.splice(0));
  pending.forEach(callback => callback());
}
const placement = context.window.AINodePlacement;

test('drop coordinates invert canvas pan and zoom', () => {
  const point = placement.graphPoint(500, 700,
    {left:100, top:300, width:800, height:600}, -200, 80, 2);
  assert.deepEqual(JSON.parse(JSON.stringify(point)), {x:300, y:160});
});

test('viewport center uses the actually visible canvas rectangle', () => {
  const point = placement.viewportCenter(
    {left:100, top:300, width:800, height:600}, -200, 80, 2);
  assert.deepEqual(JSON.parse(JSON.stringify(point)), {x:300, y:110});
});

test('document scroll is already represented by client coordinates and rect', () => {
  const before = placement.graphPoint(420, 360,
    {left:20, top:60, width:800, height:600}, 40, -20, 1.25);
  const afterScroll = placement.graphPoint(420, 160,
    {left:20, top:-140, width:800, height:600}, 40, -20, 1.25);
  assert.deepEqual(JSON.parse(JSON.stringify(afterScroll)),
    JSON.parse(JSON.stringify(before)));
});

test('invalid zoom safely falls back to one', () => {
  const point = placement.graphPoint(210, 120,
    {left:10, top:20, width:400, height:200}, 50, 10, 0);
  assert.deepEqual(JSON.parse(JSON.stringify(point)), {x:150, y:90});
});

test('palette drag MIME is private to node placement', () => {
  assert.equal(placement.MIME, 'application/x-autorig-node');
});


/**
 * A dragged tool has to be under the cursor, not beside it.
 *
 * The browser's own drag image is a snapshot of the button, and a tool button
 * carries a tooltip anchored to the toolbar rather than to itself, so the
 * snapshot was toolbar-wide and the icon landed hundreds of pixels to the
 * left of the pointer.
 */
function domStub() {
  const made = [];
  const body = {children: [], appendChild(node) { this.children.push(node); node.parentNode = this; },
    removeChild(node) { this.children = this.children.filter(item => item !== node); node.parentNode = null; }};
  return {
    body,
    made,
    createElement() {
      const node = {style: {}, attributes: {}, textContent: '', parentNode: null,
        setAttribute(name, value) { this.attributes[name] = value; }};
      made.push(node);
      return node;
    }
  };
}

function elementStub(extra = {}) {
  const handlers = new Map();
  return {
    handlers,
    addEventListener(type, handler) { handlers.set(type, handler); },
    getBoundingClientRect: () => ({left: 0, top: 0, width: 900, height: 600}),
    querySelector: () => null,
    ...extra
  };
}

function dragHarness() {
  const documentRef = domStub();
  const canvas = elementStub();
  const created = [];
  const moved = [];
  const placement = context.window.AINodePlacement.install({
    editor: {canvas_x: 0, canvas_y: 0, zoom: 1},
    canvas, document: documentRef,
    createNode: (spec, x, y) => { created.push({spec, x, y}); return 'n1'; },
    getNodeElement: () => ({getBoundingClientRect: () => ({width: 244, height: 120})}),
    moveNode: (id, x, y) => moved.push({id, x, y})
  });
  const button = elementStub({querySelector: () => ({textContent: '🖼️'})});
  placement.bindPaletteButton(button, {kind: 'service', service: 'image', title: 'Image'});
  return {documentRef, canvas, button, created, moved, placement};
}

test('the drag ghost is a small square the cursor holds by its middle', () => {
  const {documentRef, button} = dragHarness();
  const calls = [];
  const dataTransfer = {types: [], setData() {}, setDragImage: (...args) => calls.push(args)};
  button.handlers.get('dragstart')({dataTransfer});
  assert.equal(calls.length, 1);
  const [ghost, x, y] = calls[0];
  const size = context.window.AINodePlacement.GHOST_SIZE;
  assert.equal(x, size / 2);
  assert.equal(y, size / 2);
  assert.equal(ghost.textContent, '🖼️');
  assert.ok(ghost.style.cssText.includes('width:' + size + 'px'));
  // It has to be in the document and rendered, or there is no ghost at all.
  assert.ok(documentRef.body.children.includes(ghost));
});

test('the ghost is taken out of the page when the drag ends', () => {
  const {documentRef, button} = dragHarness();
  button.handlers.get('dragstart')({dataTransfer: {types: [], setData() {}, setDragImage() {}}});
  assert.equal(documentRef.body.children.length, 1);
  button.handlers.get('dragend')();
  assert.equal(documentRef.body.children.length, 0);
});

test('a drop centres the node on the cursor, like a click on the same tool', () => {
  const {canvas, button, created, moved, documentRef} = dragHarness();
  button.handlers.get('dragstart')({dataTransfer: {types: [], setData() {}, setDragImage() {}}});
  canvas.handlers.get('drop')({
    preventDefault() {}, stopPropagation() {},
    clientX: 300, clientY: 200,
    dataTransfer: {
      types: [context.window.AINodePlacement.MIME],
      getData: () => JSON.stringify({kind: 'service', service: 'image'})
    }
  });
  assert.deepEqual(JSON.parse(JSON.stringify(created)),
    [{spec: {kind: 'service', service: 'image'}, x: 300, y: 200}]);
  // The ghost does not outlive the drop.
  assert.equal(documentRef.body.children.length, 0);
  // Once the node has a measured size it is pulled back by half of it, so the
  // pointer that was holding the middle of the ghost holds the middle of it.
  flushScheduled();
  assert.deepEqual(JSON.parse(JSON.stringify(moved[0])),
    {id: 'n1', x: 300 - 244 / 2, y: 200 - 120 / 2});
});

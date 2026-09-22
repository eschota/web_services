/**
 * The camera and the sockets: arithmetic the canvas depends on.
 *
 * All three functions are pure, so they are sliced out of the editor and run
 * on their own rather than through a browser.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const sourcePath = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-nodes.js');
const source = fs.readFileSync(sourcePath, 'utf8');

function slice(from, to, expose) {
  const start = source.indexOf(from);
  const end = source.indexOf(to, start);
  assert.ok(start >= 0 && end > start, 'could not find ' + from);
  return vm.runInNewContext(source.slice(start, end) + '; ' + expose, {});
}

const socketScaleForZoom = slice('const SOCKET_SCALE_MIN',
  '  let socketScaleApplied', 'socketScaleForZoom');
const fitTransform = slice('  function fitTransform(boxes, viewport) {',
  '  /** Every node', 'fitTransform');
const programmaticParamEvent = slice('  function programmaticParamEvent(event) {',
  '  function invalidateNodeAndDownstream', 'programmaticParamEvent');


test('closing in on the graph shrinks the sockets off the node text', () => {
  assert.equal(socketScaleForZoom(1), 1);
  assert.equal(socketScaleForZoom(1.25), 0.8);
  // 1/1.6 is 0.625: above the floor, so it is honoured, to a hundredth.
  assert.equal(socketScaleForZoom(1.6), 0.63);
});

test('pulling back grows them, gently, so a distant dot stays clickable', () => {
  // The square root of the zoom, which is what the display modes always did.
  assert.equal(socketScaleForZoom(0.5), 1.41);
  assert.equal(socketScaleForZoom(0.25), 1.8);
});

test('both ends are clamped, so a socket is never huge and never a speck', () => {
  assert.equal(socketScaleForZoom(2.5), 0.55);
  assert.equal(socketScaleForZoom(10), 0.55);
  assert.equal(socketScaleForZoom(0.15), 1.8);
  assert.equal(socketScaleForZoom(0.01), 1.8);
});

test('the two halves meet, so there is no jump at one to one', () => {
  assert.ok(Math.abs(socketScaleForZoom(0.99) - 1) <= 0.011);
  assert.ok(Math.abs(socketScaleForZoom(1.01) - 1) <= 0.011);
});

test('a nonsensical zoom leaves the sockets at their drawn size', () => {
  assert.equal(socketScaleForZoom(0), 1);
  assert.equal(socketScaleForZoom(-2), 1);
  assert.equal(socketScaleForZoom(NaN), 1);
  assert.equal(socketScaleForZoom(undefined), 1);
});


const stage = {width: 1200, height: 800, top: 100, padding: 40, minZoom: 0.15, maxZoom: 1};

test('a graph wider than the stage is scaled down until all of it fits', () => {
  const view = fitTransform([{x: 0, y: 0, width: 2240, height: 400}], stage);
  // 1120 of usable width over 2240 of graph.
  assert.equal(view.zoom, 0.5);
  assert.equal(view.x, 40);
});

test('the framed graph is centred in the room left below the tools', () => {
  const view = fitTransform([{x: 0, y: 0, width: 200, height: 100}], stage);
  assert.equal(view.zoom, 1);
  assert.equal(view.x, 40 + (1120 - 200) / 2);
  assert.equal(view.y, 100 + 40 + (620 - 100) / 2);
});

test('nothing is ever magnified past its natural size', () => {
  const view = fitTransform([{x: 0, y: 0, width: 10, height: 10}], {...stage});
  assert.equal(view.zoom, 1);
});

test('the frame covers every node, wherever the graph starts', () => {
  const boxes = [
    {x: -400, y: -200, width: 244, height: 120},
    {x: 600, y: 900, width: 244, height: 300}
  ];
  const view = fitTransform(boxes, stage);
  const left = -400 * view.zoom + view.x;
  const top = -200 * view.zoom + view.y;
  const right = (600 + 244) * view.zoom + view.x;
  const bottom = (900 + 300) * view.zoom + view.y;
  assert.ok(left >= 40 - 1e-9, 'left edge inside the padding');
  assert.ok(top >= 100 + 40 - 1e-9, 'top edge below the tool strip');
  assert.ok(right <= 1200 - 40 + 1e-9, 'right edge inside the padding');
  assert.ok(bottom <= 800 - 40 + 1e-9, 'bottom edge inside the padding');
});

test('a graph far bigger than the stage still stops at the minimum zoom', () => {
  const view = fitTransform([{x: 0, y: 0, width: 400000, height: 4000}],
    {...stage, minZoom: 0.15});
  assert.equal(view.zoom, 0.15);
});

test('an empty canvas has nothing to frame', () => {
  assert.equal(fitTransform([], stage), null);
  assert.equal(fitTransform(null, stage), null);
  assert.equal(fitTransform([{x: 'somewhere', y: 0, width: 10, height: 10}], stage), null);
});


test('a value the page wrote is not an edit', () => {
  assert.equal(programmaticParamEvent({target: {dataset: {silentUpdate: 'yes'}}}), true);
  assert.equal(programmaticParamEvent({target: {dataset: {param: 'width'}}}), false);
  assert.equal(programmaticParamEvent({target: {}}), false);
  assert.equal(programmaticParamEvent({}), false);
  assert.equal(programmaticParamEvent(null), false);
});

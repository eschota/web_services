import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '..', 'ai-node-placement.js'), 'utf8');
const context = {window: {}};
vm.runInNewContext(source, context);
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

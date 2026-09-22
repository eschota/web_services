import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const sourcePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-node-wires.js');

function load() {
  const context = {window:{}, console};
  vm.runInNewContext(fs.readFileSync(sourcePath, 'utf8'), context, {filename:sourcePath});
  return context.window.AINodeWires;
}

test('endpoint ids are read from Drawflow connection classes', () => {
  const api = load();
  const classes = ['connection', 'node_in_node-12', 'node_out_node-3', 'output_1', 'input_2'];
  assert.equal(api.targetNodeId(classes), '12');
  assert.equal(api.sourceNodeId(classes), '3');
  assert.equal(api.targetNodeId(['connection']), null);
});

test('a wire into a running node is running; between finished nodes it is done', () => {
  const api = load();
  assert.equal(api.wireState('done', 'running'), api.RUNNING_CLASS);
  assert.equal(api.wireState('', 'running'), api.RUNNING_CLASS);
  assert.equal(api.wireState('done', 'done'), api.DONE_CLASS);
  assert.equal(api.wireState('done', ''), '');
  assert.equal(api.wireState('running', ''), '');
  assert.equal(api.wireState('done', 'failed'), '');
});

test('install is a no-op without a canvas or MutationObserver', () => {
  const api = load();
  assert.equal(api.install({canvas:null}), null);
  assert.equal(api.install({canvas:{}}), null);
});

/**
 * Shift-clicking a wire has to name that exact connection to Drawflow.
 *
 * Drawflow writes the two nodes and the two sockets onto the connection's own
 * element as classes; nothing else on the page identifies one wire among
 * several between the same pair of nodes. Reading them by prefix rather than
 * by position keeps the deletion working if Drawflow ever reorders the list.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';


const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, '..', 'ai-node-groups.js');

function load() {
  let source = fs.readFileSync(sourcePath, 'utf8');
  source = source.replace(
    '  window.AINodeGroups = { install: install };',
    '  window.AINodeGroups = { install: install }; window.__connectionTest = {connectionParts};');
  const context = {window: {}, console};
  vm.runInNewContext(source, context, {filename: sourcePath});
  return context.window.__connectionTest;
}

function element(classes) {
  return {classList: classes};
}

test('a connection element names its two nodes and its two sockets', () => {
  const api = load();
  assert.deepEqual({...api.connectionParts(element(
    ['connection', 'node_in_node-12', 'node_out_node-7', 'output_2', 'input_3']))},
    {inputId: '12', outputId: '7', outputClass: 'output_2', inputClass: 'input_3'});
});

test('the order of the class list is not load-bearing', () => {
  const api = load();
  assert.deepEqual({...api.connectionParts(element(
    ['input_1', 'node_out_node-4', 'connection', 'output_1', 'node_in_node-5']))},
    {inputId: '5', outputId: '4', outputClass: 'output_1', inputClass: 'input_1'});
});

test('anything that is not a full connection is refused rather than guessed', () => {
  const api = load();
  assert.equal(api.connectionParts(element(['connection', 'node_in_node-1', 'output_1'])), null);
  assert.equal(api.connectionParts(element(['drawflow-node', 'selected'])), null);
  assert.equal(api.connectionParts(element([])), null);
  assert.equal(api.connectionParts(null), null);
  assert.equal(api.connectionParts(undefined), null);
});

test('node ids are read whole, including multi-digit ones', () => {
  const api = load();
  const parts = api.connectionParts(element(
    ['connection', 'node_in_node-102', 'node_out_node-99', 'output_11', 'input_10']));
  assert.equal(parts.inputId, '102');
  assert.equal(parts.outputId, '99');
  assert.equal(parts.outputClass, 'output_11');
  assert.equal(parts.inputClass, 'input_10');
});

test('a finished control map outranks the original it was extracted from', () => {
  // The bucket order lives in resolveInputDimensions, which needs a live
  // canvas; the source is the contract, so it is asserted directly.
  const source = fs.readFileSync(sourcePath, 'utf8');
  assert.match(source, /candidates\.controls\.concat\(candidates\.originals, candidates\.generated\)/);
  // Only a finished result joins a bucket: a stale map must not pin the size
  // of a node whose input has just changed.
  assert.match(source, /const finished = result && result\.status === 'done' && result\.value;/);
});

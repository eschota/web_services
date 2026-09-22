import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';


const sourcePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-node-groups.js');

function load() {
  let source = fs.readFileSync(sourcePath, 'utf8');
  source = source.replace(
    '  window.AINodeGroups = { install: install };',
    '  window.AINodeGroups = { install: install }; window.__followSizeTest = {followsInputSize, targetDimensions};');
  const context = {window:{}, console};
  vm.runInNewContext(source, context, {filename:sourcePath});
  return context.window.__followSizeTest;
}

test('missing follow marker defaults to live input-size following', () => {
  const api = load();
  assert.equal(api.followsInputSize({width:960, height:540}), true);
  assert.equal(api.followsInputSize({_follow_input_size:true}), true);
  assert.equal(api.followsInputSize({_follow_input_size:false}), false);
});

test('input dimensions preserve exact image sizes and make video sizes even', () => {
  const api = load();
  assert.deepEqual({...api.targetDimensions({width:1215, height:833}, 'image')},
    {width:1215, height:833, evenAdjusted:false});
  assert.deepEqual({...api.targetDimensions({width:1215, height:833}, 'video')},
    {width:1216, height:834, evenAdjusted:true});
});

test('unsupported dimensions are not silently downscaled', () => {
  const api = load();
  assert.equal(api.targetDimensions({width:4096, height:2160}, 'image'), null);
  assert.equal(api.targetDimensions({width:255, height:540}, 'image'), null);
  assert.equal(api.targetDimensions({width:'not-a-size', height:540}, 'image'), null);
});


/**
 * The writer that follows an upstream picture, on its own.
 *
 * It is sliced out of the module because it lives inside `install`, where a
 * test would need a canvas, an editor and a graph to reach it.
 */
function dimensionWriter() {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('    function setDimensionControl(control, value) {');
  const end = source.indexOf('    async function refreshFollowingSizes()', start);
  const seen = new Set();
  const context = {
    internalDimensionControls: seen,
    Option: function () {},
    Event: class { constructor(type, init) { this.type = type; Object.assign(this, init); } }
  };
  const write = vm.runInNewContext(source.slice(start, end) + '; setDimensionControl',
    context, {filename: sourcePath});
  return {write, seen};
}

function numberControl(value) {
  return {
    tagName: 'INPUT', value: String(value), dataset: {param: 'width'}, events: [],
    checkValidity() { return true; },
    dispatchEvent(event) {
      this.events.push({type: event.type, silent: this.dataset.silentUpdate});
      return true;
    }
  };
}

test('a size that is already in the box is not announced as a change', () => {
  const {write} = dimensionWriter();
  const control = numberControl(1024);
  assert.equal(write(control, 1024), true);
  assert.deepEqual(control.events, []);
  assert.equal(control.value, '1024');
});

test('numeric and string forms of the same size are the same size', () => {
  const {write} = dimensionWriter();
  const control = numberControl('768');
  assert.equal(write(control, 768), true);
  assert.deepEqual(control.events, []);
});

test('a real change is announced, and marked as not typed by anyone', () => {
  const {write, seen} = dimensionWriter();
  const control = numberControl(1024);
  assert.equal(write(control, 512), true);
  assert.equal(control.value, '512');
  assert.deepEqual(control.events.map(entry => entry.type), ['input', 'change']);
  assert.ok(control.events.every(entry => entry.silent === 'yes'));
  // The mark is temporary: a later edit by a person must look like one.
  assert.equal(control.dataset.silentUpdate, undefined);
  assert.equal(seen.size, 0);
});

test('a size the control rejects is rolled back and reported as not applied', () => {
  const {write} = dimensionWriter();
  const control = numberControl(1024);
  control.checkValidity = () => false;
  assert.equal(write(control, 9999), false);
  assert.equal(control.value, '1024');
  assert.deepEqual(control.events, []);
});

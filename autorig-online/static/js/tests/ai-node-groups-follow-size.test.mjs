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

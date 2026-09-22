import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const here = path.dirname(fileURLToPath(import.meta.url));
const context = {};
context.globalThis = context;
vm.createContext(context);
vm.runInContext(fs.readFileSync(path.resolve(here, '..', 'ai-render-quality.js'), 'utf8'), context);
const rq = context.AIRenderQuality;
const plain = value => JSON.parse(JSON.stringify(value));

const video = {params_array: [
  {name: 'width', type: 'number', min: 256, max: 2048}, {name: 'height', type: 'number', min: 256, max: 2048},
  {name: 'frame_count', type: 'range', min: 9, max: 393}]};
const image = {params_array: [{name: 'width', type: 'select'}, {name: 'height', type: 'select'}]};

test('the four modes and their factors; anything else is normal', () => {
  assert.deepEqual(plain(rq.MODES), ['preview', 'fast', 'normal', 'highquality']);
  assert.equal(rq.factor('preview'), 0.25);
  assert.equal(rq.factor('fast'), 0.5);
  assert.equal(rq.factor('normal'), 1);
  assert.equal(rq.factor('highquality'), 2);
  assert.equal(rq.normalize('ULTRA'), 'normal');
  assert.equal(rq.normalize(undefined), 'normal');
});

test('normal leaves a request byte-identical', () => {
  const body = {width: 961, height: 541, frame_count: 97};
  assert.equal(rq.applyToBody('video', body, 'normal', video), null);
  assert.deepEqual(plain(body), {width: 961, height: 541, frame_count: 97});
});

test('fast halves and rounds to /32; frames are untouched', () => {
  const body = {width: 1920, height: 1088, frame_count: 121, prompt: 'x'};
  rq.applyToBody('video', body, 'fast', video);
  assert.deepEqual(plain(body), {width: 960, height: 544, frame_count: 121, prompt: 'x'});
});

test('preview of 960x540 hits the 256 floor and keeps the aspect ratio', () => {
  const result = rq.scaleSize(960, 540, 'preview', rq.limitsFor('video', video));
  assert.equal(result.height, 256);
  assert.equal(result.width, 448);
  assert.deepEqual(plain(result.limited), ['min']);
});

test('highquality doubles within the 2048 cap, shrinking both sides together', () => {
  assert.deepEqual(plain(rq.scaleSize(960, 540, 'highquality', rq.limitsFor('video', video))),
    {width: 1920, height: 1088, limited: []});
  const capped = rq.scaleSize(1536, 1024, 'highquality', rq.limitsFor('image', image));
  assert.equal(capped.width, 2048);
  assert.equal(capped.height, 1376);
  assert.deepEqual(plain(capped.limited), ['max']);
});

test('the Avatar video preset area is respected after /32 padding', () => {
  const result = rq.scaleSize(960, 540, 'highquality', rq.limitsFor('avatar_video', video));
  assert.ok(result.width * result.height <= 524288, JSON.stringify(result));
  assert.ok(Math.ceil(result.width / 32) * 32 * Math.ceil(result.height / 32) * 32 <= 524288);
  assert.ok(result.limited.includes('area'));
});

test('every scaled size is a multiple of 32 inside 256..2048', () => {
  for (const mode of ['preview', 'fast', 'highquality']) {
    for (const [w, h] of [[960, 540], [1024, 1024], [540, 960], [1344, 768], [256, 2048], [333, 777]]) {
      const r = rq.scaleSize(w, h, mode, rq.limitsFor('video', video));
      for (const v of [r.width, r.height]) {
        assert.equal(v % 32, 0, `${mode} ${w}x${h} -> ${v}`);
        assert.ok(v >= 256 && v <= 2048, `${mode} ${w}x${h} -> ${v}`);
      }
    }
  }
});

test('a body without a size (edit follows the source) is left alone', () => {
  const body = {prompt: 'x'};
  rq.applyToBody('image', body, 'preview', image);
  assert.deepEqual(plain(body), {prompt: 'x'});
});

test('ai-nodes.js scales in bodyFor, so the signature records the scaled size', () => {
  const source = fs.readFileSync(path.resolve(here, '..', 'ai-nodes.js'), 'utf8');
  const bodyFor = source.slice(source.indexOf('  function bodyFor('), source.indexOf('  const BUDGET_EXHAUSTED'));
  assert.match(bodyFor, /AIRenderQuality\.applyToBody\(serviceId, body, renderQuality, declaration\)/);
  assert.match(source, /const signature = stableJson\(\{service:node\.service, body:requestBody\}\);/);
  assert.match(source, /render_quality: renderQuality,/);
});

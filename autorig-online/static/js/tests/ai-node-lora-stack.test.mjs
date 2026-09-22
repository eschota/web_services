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
vm.runInContext(fs.readFileSync(path.resolve(here, '..', 'ai-node-lora-stack.js'), 'utf8'), context);
const stack = context.AINodeLoraStack;

const catalogue = [
  {file: 'bts_movie_set.safetensors', title: 'BTS Movie Set', family: 'ltx2', aliases: ['BTS set v2']},
  {file: 'bounce_ltx23.safetensors', title: 'Bounce', family: 'ltx2'},
  {file: 'film_grain.safetensors', title: 'Film grain', family: 'zimage'},
];

test('slots write the tag string the backend parses, in order', () => {
  assert.equal(stack.toTags([
    {file: 'bts_movie_set.safetensors', weight: 0.8},
    {file: 'bounce_ltx23.safetensors', weight: -0.35},
    {file: '', weight: 1},
  ]), '<lora:bts_movie_set:0.8> <lora:bounce_ltx23:-0.35>');
  assert.equal(stack.toTags([]), '');
});

test('a saved stack string reads back into slots', () => {
  const items = stack.parseTags('<lora:bts_movie_set:0.8> <lora:Bounce>, <LYCO:film_grain:1.25>');
  assert.deepEqual(JSON.parse(JSON.stringify(items)), [
    {name: 'bts_movie_set', weight: 0.8}, {name: 'Bounce', weight: 1}, {name: 'film_grain', weight: 1.25}]);
  assert.deepEqual(JSON.parse(JSON.stringify(stack.parseTags(''))), []);
});

test('a stack the slots cannot show faithfully stays as raw text', () => {
  assert.equal(stack.parseTags('<lora:a:0.5:1>'), null);      // split TE/UNET weights
  assert.equal(stack.parseTags('<lora:a:te=0.5>'), null);      // named weights
  assert.equal(stack.parseTags('cinematic <lora:a:1>'), null); // stray text
});

test('tag names resolve like the backend: file, stem, title, alias; ambiguity is no match', () => {
  assert.equal(stack.resolveFile('bts_movie_set', catalogue), 'bts_movie_set.safetensors');
  assert.equal(stack.resolveFile('BTS Movie Set', catalogue), 'bts_movie_set.safetensors');
  assert.equal(stack.resolveFile('bts set v2', catalogue), 'bts_movie_set.safetensors');
  assert.equal(stack.resolveFile('bounce_ltx23.safetensors', catalogue), 'bounce_ltx23.safetensors');
  assert.equal(stack.resolveFile('missing', catalogue), '');
  assert.equal(stack.resolveFile('x', [{file: 'x.safetensors'}, {file: 'y.safetensors', title: 'x'}]), '');
});

test('readiness: usable, downloading, or on no computer that runs this checkpoint', () => {
  const checkpoint = {runnable_workers: ['f5', 'raptor']};
  assert.equal(stack.readiness({usable: true, ready_workers: ['f5']}, checkpoint), '');
  assert.equal(stack.readiness({usable: true, ready_workers: ['f12']}, checkpoint),
    'waiting for the render computers to download it');
  assert.equal(stack.readiness({usable: false, unusable_reason: 'still downloading'}, checkpoint), 'still downloading');
  assert.equal(stack.readiness({usable: true}, checkpoint), '');
  assert.equal(stack.readiness(null, checkpoint), '');
});

test('only LoRAs of the checkpoint family are offered (SDXL and Pony mix)', () => {
  assert.equal(stack.familiesMatch('ltx2', 'ltx2'), true);
  assert.equal(stack.familiesMatch('ltx2', 'zimage'), false);
  assert.equal(stack.familiesMatch('sdxl', 'pony'), true);
  assert.equal(stack.familiesMatch('', 'zimage'), true);
});

test('weights stay in the range the node offers', () => {
  assert.equal(stack.clampWeight(3, 0.8), 2);
  assert.equal(stack.clampWeight(-5, 0.8), -2);
  assert.equal(stack.clampWeight('abc', 0.8), 0.8);
  assert.equal(stack.clampWeight(0.333, 0.8), 0.33);
  assert.equal(stack.MAX_SLOTS, 8);
});

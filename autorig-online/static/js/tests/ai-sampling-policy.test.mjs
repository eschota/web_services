import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '..', 'ai-entities.js'), 'utf8');
const sandbox = {
  window: {}, URL, URLSearchParams, Date,
  localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
  sessionStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
  document: { getElementById() { return {}; } },
  location: { origin: 'https://autorig.online' },
  fetch: async () => ({ ok: true, json: async () => ({}) }),
  setTimeout, clearTimeout, setInterval, clearInterval,
};
vm.runInNewContext(source, sandbox, { filename: 'ai-entities.js' });
const entities = sandbox.window.AIEntities;

test('runtime sampling policy labels Auto separately from a fixed workflow', () => {
  const pony = entities.samplingPresentation({
    sampling_policy: { auto_steps: 50, steps_min: 1, steps_max: 60 },
    recommended: { steps: 30, cfg: 5 },
  }, 'checkpoints');
  assert.equal(pony.policy_text, 'Auto preset · 50 steps');
  assert.equal(pony.recommended.steps, undefined);
  assert.match(pony.author_sampling_text, /steps 30/);

  const klein = entities.samplingPresentation({
    sampling_policy: { auto_steps: 4, fixed_steps: 4, cfg_mode: 'fixed', cfg_value: 1,
      scheduler_mode: 'native', scheduler_label: 'FLUX.2 native' },
  }, 'checkpoints');
  assert.match(klein.policy_text, /Fixed 4 steps/);
  assert.match(klein.policy_text, /CFG 1 \(no extra guidance\)/);
  assert.match(klein.policy_text, /FLUX\.2 native/);
});

test('incompatible LoRA sampling examples are hidden while strength remains visible', () => {
  const lora = entities.samplingPresentation({
    sampling_recommendations_compatible: false,
    recommended: { steps: 30, cfg: 7, scheduler: 'karras', strength: 0.8 },
  }, 'loras');
  assert.deepEqual(Object.keys(lora.recommended), ['strength']);
  assert.equal(lora.recommended.strength, 0.8);
  assert.match(lora.note, /inherits the selected base model/i);
});

test('node runtime keeps the zero Auto sentinel and locks only fixed policy knobs', () => {
  const nodes = fs.readFileSync(path.join(here, '..', 'ai-nodes.js'), 'utf8');
  assert.match(nodes, /steps\.min = '0'/);
  assert.match(nodes, /Number\(policy\.fixed_steps\)/);
  assert.match(nodes, /policy\.cfg_mode === 'fixed'/);
  assert.match(nodes, /policy\.scheduler_mode === 'native'/);
  assert.match(nodes, /\['steps', 'cfg', 'sampler', 'scheduler'\]\.includes\(name\)/);
  assert.match(nodes, /option\.disabled \? ' disabled'/);
  assert.match(nodes, /fluxOnly\.has\(option\.value\)/);
  assert.match(nodes, /family !== 'flux'/);
  assert.doesNotMatch(nodes, /BasicGuider/);
  assert.match(nodes, /annotateOnly: true/);
});

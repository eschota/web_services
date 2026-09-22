/**
 * The page's ControlNet acceptance rule is the server's rule.
 *
 * The editor refuses a control wire before it is drawn, which is only helpful
 * while it refuses exactly what the backend validator would have refused. When
 * the page was the stricter of the two, Depth stopped attaching to every FLUX
 * checkpoint even though the render behind the wire would have run.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';


const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, '..', 'ai-nodes.js');
const backend = path.resolve(here, '..', '..', '..', 'backend');

function load() {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('function controlChannelAccepted(');
  const end = source.indexOf('function toastControlRefusal(');
  assert.ok(start > 0 && end > start, 'the control-channel helpers must stay sliceable');
  return vm.runInNewContext(source.slice(start, end) +
    '; ({controlChannelAccepted, controlChannelModels, controlRefusalMessage})', {});
}

// The production catalogue, as /api/ai/model-catalogue?service=image serves it.
const PONY = {id: 'CyberRealisticPony_V18.0_F16.safetensors', title: 'CyberRealistic Pony',
              family: 'pony', control_channels: ['pose', 'depth', 'canny'], usable: true};
const SCHNELL = {id: 'flux1-schnell.safetensors', title: 'FLUX.1 Schnell',
                 family: 'flux', usable: true};
const SCHNELL_FP8 = {id: 'flux1-schnell-fp8.safetensors', title: 'FLUX.1 Schnell FP8 checkpoint',
                     family: 'flux', usable: true};
const KLEIN = {id: 'flux-2-klein-4b.safetensors', title: 'FLUX.2 klein 4B',
               family: 'flux2', usable: true};
const DEPTH = {id: 'control_depth', compatible_image_families: ['flux']};


test('a checkpoint that names the channel is accepted whatever its family', () => {
  const api = load();
  assert.equal(api.controlChannelAccepted('depth', PONY, DEPTH), true);
  assert.equal(api.controlChannelAccepted('pose', PONY, {id: 'control_pose'}), true);
});

test('a compatible family is accepted even with no channels declared', () => {
  const api = load();
  assert.equal(api.controlChannelAccepted('depth', SCHNELL, DEPTH), true);
  assert.equal(api.controlChannelAccepted('depth', SCHNELL_FP8, DEPTH), true);
  assert.equal(api.controlChannelAccepted('depth', {family: 'FLUX'}, DEPTH), true);
});

test('neither the channel nor the family leaves the wire refused', () => {
  const api = load();
  assert.equal(api.controlChannelAccepted('depth', KLEIN, DEPTH), false);
  assert.equal(api.controlChannelAccepted('depth', PONY, {compatible_image_families: []}), true);
  assert.equal(api.controlChannelAccepted('depth', KLEIN, {}), false);
  assert.equal(api.controlChannelAccepted('depth', KLEIN, null), false);
});

test('no checkpoint chosen means the deployed default workflow decides', () => {
  const api = load();
  assert.equal(api.controlChannelAccepted('depth', null, DEPTH), true);
  assert.equal(api.controlChannelAccepted('depth', undefined, DEPTH), true);
});

test('the refusal names the models that would have connected', () => {
  const api = load();
  const names = api.controlChannelModels('depth', DEPTH, [KLEIN, SCHNELL, PONY, SCHNELL_FP8]);
  assert.deepEqual(names, ['FLUX.1 Schnell', 'CyberRealistic Pony', 'FLUX.1 Schnell FP8 checkpoint']);
  const message = api.controlRefusalMessage('depth', names);
  assert.match(message, /^Depth control needs a model validated for it: /);
  assert.match(message, /CyberRealistic Pony/);
  assert.equal(api.controlChannelModels('depth', DEPTH, [Object.assign({}, KLEIN)]).length, 0);
  // An unusable entry is never offered as a way out.
  assert.deepEqual(api.controlChannelModels('depth', DEPTH,
    [Object.assign({}, SCHNELL, {usable: false})]), []);
  assert.match(api.controlRefusalMessage('depth', []), /needs a model validated for it\./);
});

test('the backend validator consults the same two declarations', () => {
  const validator = fs.readFileSync(path.join(backend, 'ai_graph_edits.py'), 'utf8');
  const rule = validator.slice(validator.indexOf('def _validate_catalogue_and_controls'));
  assert.match(rule, /checkpoint\.get\("control_channels"\)/);
  assert.match(rule, /source_service\.get\("compatible_image_families"\)/);
  assert.match(rule, /channel not in explicit_channels and family not in compatible_families/);
  // And the catalogue really does declare the families the page now reads.
  const services = fs.readFileSync(path.join(backend, 'ai_services.py'), 'utf8');
  assert.match(services, /"compatible_image_families": \["flux"\]/);
});

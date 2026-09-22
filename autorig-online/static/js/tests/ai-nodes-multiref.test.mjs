/**
 * Numbered reference sockets on multi-reference image nodes.
 *
 * The prompt counts pictures ("the jacket from image 2"), so the page must
 * send them in socket order, show sockets one at a time as they are wired,
 * and say which number each one is.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, '..', 'ai-nodes.js');

function load() {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('const REFERENCE_FIELD =');
  const end = source.indexOf('function refreshReferenceSockets(');
  assert.ok(start > 0 && end > start, 'the reference-socket helpers must stay sliceable');
  const context = {
    typeIcon: type => (type === 'video' ? '🎬' : '🖼'),
    escapeAttr: value => String(value).replace(/"/g, '&quot;'),
    escapeHtml: value => String(value),
  };
  return vm.runInNewContext(source.slice(start, end) +
    '; ({referenceIndex, visibleReferenceSockets, referenceList, inputRowHtml, MULTIREF_CHECKPOINT})', context);
}

const IMAGE_INPUTS = [
  {field: 'prompt', type: 'text'},
  {field: 'image', type: 'image', ref_index: 1, also_accepts: ['video']},
  {field: 'control_pose', type: 'control_pose'},
  {field: 'reference_2', type: 'image', ref_index: 2, also_accepts: ['video']},
  {field: 'reference_3', type: 'image', ref_index: 3, also_accepts: ['video']},
  {field: 'reference_4', type: 'image', ref_index: 4, also_accepts: ['video']},
];

test('sockets appear one at a time as the previous one is wired', () => {
  const api = load();
  assert.deepEqual({...api.visibleReferenceSockets(IMAGE_INPUTS, {})},
    {image: true, reference_2: false, reference_3: false, reference_4: false});
  assert.deepEqual({...api.visibleReferenceSockets(IMAGE_INPUTS, {image: true})},
    {image: true, reference_2: true, reference_3: false, reference_4: false});
  assert.deepEqual({...api.visibleReferenceSockets(IMAGE_INPUTS, {image: true, reference_2: true})},
    {image: true, reference_2: true, reference_3: true, reference_4: false});
  // A wired later socket keeps every socket before it visible.
  assert.deepEqual({...api.visibleReferenceSockets(IMAGE_INPUTS, {reference_3: true})},
    {image: true, reference_2: true, reference_3: true, reference_4: true});
});

test('extra pictures are sent in socket order, gaps closed', () => {
  const api = load();
  const list = api.referenceList({prompt: 'p', image: 'a.png', reference_4: 'd.png',
                                  reference_2: 'b.mp4', reference_3: ''});
  assert.deepEqual([...list], ['b.mp4', 'd.png']);
  assert.equal(api.referenceIndex('image'), 1);
  assert.equal(api.referenceIndex('reference_3'), 3);
  assert.equal(api.referenceIndex('control_pose'), 0);
});

test('a reference row is an icon, its number and a video hint', () => {
  const api = load();
  const row = api.inputRowHtml(IMAGE_INPUTS[3]);
  assert.match(row, /class="refn">2</);
  assert.match(row, /first frame/);
  assert.match(api.inputRowHtml(IMAGE_INPUTS[0]), /prow pin">/);
  assert.equal(api.MULTIREF_CHECKPOINT, 'flux-2-klein-4b.safetensors');
});

test('the request carries picture 1 as image_url and the rest as an ordered list', () => {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('  function bodyFor(serviceId, resolved, params) {');
  const end = source.indexOf('  const BUDGET_EXHAUSTED', start);
  const services = [{id: 'image'}];
  const bodyFor = vm.runInNewContext(source.slice(start, end) + '; bodyFor', {
    catalogue: {services_array: services},
    serviceById: id => services.find(entry => entry.id === id),
  });
  const body = bodyFor('image', {prompt: 'p', image: 'https://e/a.png',
                                 reference_3: 'https://e/c.mp4', reference_2: 'https://e/b.png'},
                       {checkpoint: 'flux-2-klein-4b.safetensors'});
  assert.equal(body.image_url, 'https://e/a.png');
  assert.deepEqual([...body.reference_image_urls], ['https://e/b.png', 'https://e/c.mp4']);
  assert.equal(body.reference_2, undefined);
  assert.equal(bodyFor('image', {prompt: 'p'}, {}).reference_image_urls, undefined);
});

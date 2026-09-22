/**
 * The LoRA stack field on image/video nodes.
 *
 * The stack is stored as Civitai-style tags (`<lora:name:0.8> <lora:b:0.6>`)
 * in a plain text param, so saved graphs, copy/paste and the graph agent carry
 * it without a new data shape, and the server parses it with the same parser
 * it uses for tags written in the prompt.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '..', 'ai-nodes.js'), 'utf8');

function slice(from, to) {
  const start = source.indexOf(from);
  const end = source.indexOf(to, start);
  assert.ok(start > 0 && end > start, `${from} must stay sliceable`);
  return source.slice(start, end);
}

const escapeHtml = s => String(s).replace(/[&<>"']/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]));

test('a lora_stack param renders a tag field and an add menu', () => {
  const api = vm.runInNewContext(slice('function paramControl(', 'function updateSamplingReadout(') +
    '; ({paramControl})', {escapeAttr: escapeHtml, escapeHtml, models: []});
  const html = api.paramControl({name: 'loras', title: 'LoRA stack', type: 'lora_stack', default: '',
                                 help: '<lora:NAME:WEIGHT> tags'});
  assert.match(html, /<input type="text" data-param="loras"/);
  assert.match(html, /placeholder="&lt;lora:name:0.8&gt;"/);
  assert.match(html, /<select class="lstack-add"/);
  // The menu is not a param: readParams must not pick it up.
  assert.doesNotMatch(html, /<select[^>]*data-param/);
});

test('the add menu offers only loadable LoRAs of the node\'s service, as tags', async () => {
  const requested = [];
  const options = [];
  const select = {
    dataset: {},
    closest: () => ({id: 'node-7'}),
    add: option => options.push(option),
  };
  const context = {
    meta: id => (id === '7' ? {service: 'image'} : null),
    fetch: url => {
      requested.push(url);
      return Promise.resolve({json: () => Promise.resolve({loras_array: [
        {file: 'add-detail-xl.safetensors', title: 'Detail Tweaker XL', base: 'SDXL 1.0', usable: true},
        {file: 'darth-vader-pxl.safetensors', title: 'Darth Vader', base: 'Pony', usable: true,
         recommended: {strength: 0.8}},
        {file: 'pending.safetensors', title: 'Pending', usable: false},
      ]})});
    },
    Option: function (text, value) { this.text = text; this.value = value; },
    encodeURIComponent,
  };
  const api = vm.runInNewContext(slice('const loraMenuCache', 'function readParams(') +
    '; ({fillLoraStackMenu})', context);
  api.fillLoraStackMenu(select);
  await new Promise(resolve => setTimeout(resolve, 0));
  await new Promise(resolve => setTimeout(resolve, 0));
  assert.deepEqual(requested, ['/api/ai/model-catalogue?service=image']);
  assert.equal(select.dataset.filled, '1');
  assert.deepEqual(options.map(o => o.value), ['<lora:add-detail-xl:1>', '<lora:darth-vader-pxl:0.8>']);
});

test('the image and video services declare the stack param', () => {
  const services = fs.readFileSync(path.resolve(here, '..', '..', '..', 'backend', 'ai_services.py'), 'utf8');
  const declarations = services.match(/"name": "loras", "title": "LoRA stack", "type": "lora_stack"/g) || [];
  assert.equal(declarations.length, 2);
});

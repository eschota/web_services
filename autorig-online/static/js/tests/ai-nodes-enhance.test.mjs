import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';


const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, '..', 'ai-nodes.js');
const cataloguePath = path.resolve(here, '..', '..', '..', 'backend', 'ai_services.py');

function runners() {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('const RUNNERS = {');
  const end = source.indexOf("['pose', 'depth', 'canny'].forEach", start);
  return vm.runInNewContext(source.slice(start, end) + '; RUNNERS', {
    pollForFile() {}, pollAiStatus() {}, pollAvatarStatus() {}, poll3dStatus() {},
  });
}

function declaredApi(catalogue, serviceId) {
  const match = catalogue.match(
    new RegExp(`"id": "${serviceId}"[\\s\\S]*?"api": "([^"]+)"`));
  return match && match[1];
}


test('each enhancement node calls the endpoint its service declares', () => {
  const map = runners();
  const catalogue = fs.readFileSync(cataloguePath, 'utf8');
  for (const serviceId of ['upscale', 'detail_enhance', 'face_fix']) {
    assert.ok(map[serviceId], `${serviceId} has no runner`);
    assert.equal(map[serviceId].api, declaredApi(catalogue, serviceId),
                 `${serviceId} runner and catalogue disagree`);
  }
});


test('enhancement results are pictures, so they chain into anything taking one', () => {
  const map = runners();
  for (const serviceId of ['upscale', 'detail_enhance', 'face_fix']) {
    assert.equal(map[serviceId].type, 'image');
    assert.equal(map[serviceId].field, 'image_url_string');
    // A farm job publishes a URL that fills in later; holding the connection
    // open is what the language-model runners do and would stall a graph.
    assert.equal(map[serviceId].finish.name, 'pollForFile');
  }
});


test('the palette explains a planned node instead of a generic note', () => {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const head = source.indexOf("if (entry.status !== 'live') {");
  assert.ok(head > 0, 'the palette no longer disables non-live services');
  const open = source.indexOf('{', head);
  let depth = 0;
  let close = open;
  while (close < source.length) {
    if (source[close] === '{') depth += 1;
    else if (source[close] === '}' && --depth === 0) break;
    close += 1;
  }
  const body = source.slice(open + 1, close);
  assert.match(body, /entry\.blocked_reason/);

  const scope = {
    entry: {title: 'Upscale video', status: 'planned',
            blocked_reason: 'worker-4090 has no ESRGAN weights.'},
    button: {
      disabled: false, draggable: true, label: '',
      setAttribute(_name, value) { this.label = value; },
      querySelector() { return scope.note; },
    },
    note: {textContent: ''},
  };
  vm.runInNewContext(body, scope);
  assert.equal(scope.button.disabled, true);
  // A disabled button still fires dragstart; dropping it made an uncallable node.
  assert.equal(scope.button.draggable, false);
  assert.equal(scope.note.textContent, 'worker-4090 has no ESRGAN weights.');
  assert.match(scope.button.label, /no ESRGAN weights/);
});


test('a picture result renders as a preview for every enhancement node', () => {
  // showResult keys off the runner type, so an image type is all these need;
  // this guards the pairing rather than re-testing the DOM helper.
  const map = runners();
  const source = fs.readFileSync(sourcePath, 'utf8');
  assert.match(source, /type === 'image' \|\| type\.startsWith\('control_'\)/);
  assert.deepEqual(
    [...new Set(['upscale', 'detail_enhance', 'face_fix'].map(id => map[id].type))],
    ['image']);
});

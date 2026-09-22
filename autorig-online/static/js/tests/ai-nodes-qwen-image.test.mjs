import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';


const here = path.dirname(fileURLToPath(import.meta.url));
const sourcePath = path.resolve(here, '..', 'ai-nodes.js');
const cataloguePath = path.resolve(here, '..', '..', '..', 'backend', 'ai_services.py');
const pagePath = path.resolve(here, '..', '..', 'nodes.html');

function slice(source, startMarker, endMarker) {
  const start = source.indexOf(startMarker);
  assert.ok(start > 0, `${startMarker} is gone from ai-nodes.js`);
  const end = source.indexOf(endMarker, start);
  assert.ok(end > start, `${endMarker} is gone from ai-nodes.js`);
  return source.slice(start, end);
}

function runners() {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const body = slice(source, 'const RUNNERS = {', "['pose', 'depth', 'canny'].forEach");
  return vm.runInNewContext(body + '; RUNNERS', {
    pollForFile() {}, pollAiStatus() {}, pollAvatarStatus() {}, pollAvatarBuild() {}, poll3dStatus() {},
  });
}

function toolIcons() {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const body = slice(source, 'const TOOL_ICONS = {', 'function toolIcon(');
  return vm.runInNewContext(body + '; TOOL_ICONS', {});
}

function declaredApi(catalogue, serviceId) {
  const match = catalogue.match(
    new RegExp(`"id": "${serviceId}"[\\s\\S]*?"api": "([^"]+)"`));
  return match && match[1];
}


test('the Qwen-Image node calls the endpoint its service declares', () => {
  const runner = runners().qwen_image;
  assert.ok(runner, 'qwen_image has no runner, so the node would refuse to run');
  const catalogue = fs.readFileSync(cataloguePath, 'utf8');
  assert.equal(runner.api, declaredApi(catalogue, 'qwen_image'));
});


test('its result is an ordinary picture, so it chains into anything taking one', () => {
  const runner = runners().qwen_image;
  assert.equal(runner.type, 'image');
  assert.equal(runner.field, 'image_url_string');
  // The farm publishes a URL that fills in minutes later. Holding the
  // connection open is what the language-model runners do, and on a render
  // this slow it would stall every other node in the graph.
  assert.equal(runner.finish.name, 'pollForFile');
});


test('one node, not two: generating and editing share a runner', () => {
  const map = runners();
  assert.equal(map.qwen_image_edit, undefined,
               'a separate edit runner means the wiring no longer chooses the model');
});


test('the palette gives it a glyph of its own', () => {
  // Without one it falls back to the icon of what it produces, and the
  // toolbar would show a fifth identical picture button.
  const icons = toolIcons();
  assert.ok(icons.qwen_image, 'qwen_image has no icon');
  const duplicates = Object.entries(icons)
    .filter(([key, glyph]) => key !== 'qwen_image' && glyph === icons.qwen_image);
  assert.deepEqual(duplicates, []);
});


test('the service takes a prompt and an optional picture', () => {
  const catalogue = fs.readFileSync(cataloguePath, 'utf8');
  const start = catalogue.indexOf('"id": "qwen_image"');
  assert.ok(start > 0, 'qwen_image is no longer in the service catalogue');
  const block = catalogue.slice(start, catalogue.indexOf('})', start));
  assert.match(block, /"field": "prompt", "required": True/);
  assert.match(block, /"field": "image", "required": False/);
});


test('the page asks for a build of ai-nodes.js that knows this node', () => {
  const page = fs.readFileSync(pagePath, 'utf8');
  const match = page.match(/ai-nodes\.js\?v=([^"]+)/);
  assert.ok(match, 'nodes.html no longer versions ai-nodes.js');
  // A stale buster is how a new node reaches nobody: browsers keep the file
  // they already have and its palette has never heard of the service. The
  // name is not pinned — other people bump this line too — only the fact
  // that it moved off the build that predates this node.
  assert.notEqual(match[1], '20260922-cap1');
});

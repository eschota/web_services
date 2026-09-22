import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';


const here = path.dirname(fileURLToPath(import.meta.url));
const nodesPath = path.resolve(here, '..', 'ai-nodes.js');
const groupsPath = path.resolve(here, '..', 'ai-node-groups.js');
const bridgePath = path.resolve(here, '..', 'ai-graph-bridge.js');
const cataloguePath = path.resolve(here, '..', '..', '..', 'backend', 'ai_services.py');


/**
 * `bodyFor`, lifted out of the module.
 *
 * It lives inside the editor's IIFE next to the canvas, the editor instance
 * and the whole graph, none of which a test of request shaping needs; the one
 * thing it does reach for is the service catalogue, which is injected.
 */
function requestBuilder(services) {
  const source = fs.readFileSync(nodesPath, 'utf8');
  const start = source.indexOf('  function bodyFor(serviceId, resolved, params) {');
  assert.ok(start > 0, 'bodyFor is no longer where the test expects it');
  const end = source.indexOf('  const BUDGET_EXHAUSTED', start);
  const context = {
    // The real module refuses to consult a catalogue it has not loaded yet,
    // so the slice needs both the flag and the lookup.
    catalogue: {services_array: services},
    serviceById(id) { return services.find(entry => entry.id === id); }
  };
  return vm.runInNewContext(source.slice(start, end) + '; bodyFor', context,
                            {filename:nodesPath});
}

const VISION = {
  id: 'vision', system_prompt_capable: true,
  system_prompt_default: 'Always write only the output_text answer as plain text.'
};
const IMAGE = {id: 'image'};


test('a Vision node always asks for the answer alone, never the wrapper', () => {
  const bodyFor = requestBuilder([VISION, IMAGE]);
  const body = bodyFor('vision', {image: 'https://example.test/a.png'},
                       {prompt: 'What is this?', _system_prompt: VISION.system_prompt_default});
  assert.equal(body.structured, true);
  assert.equal(body.system_prompt, VISION.system_prompt_default);
  assert.equal(body.image_url, 'https://example.test/a.png');
  assert.equal(body.prompt, 'What is this?');
});


test('an edited system prompt is what actually goes to the model', () => {
  const bodyFor = requestBuilder([VISION, IMAGE]);
  const body = bodyFor('vision', {}, {prompt: 'x', _system_prompt: 'Answer in Latin.'});
  assert.equal(body.system_prompt, 'Answer in Latin.');
});


test('a node saved before system prompts existed still sends the default', () => {
  const bodyFor = requestBuilder([VISION, IMAGE]);
  const body = bodyFor('vision', {}, {prompt: 'x'});
  assert.equal(body.system_prompt, VISION.system_prompt_default);
  assert.equal(body.structured, true);
});


test('the standing instruction never leaks into the request as a parameter', () => {
  const bodyFor = requestBuilder([VISION, IMAGE]);
  const body = bodyFor('vision', {}, {prompt: 'x', _system_prompt: 'Be terse.'});
  assert.equal(body._system_prompt, undefined);
});


test('a node that does not declare the capability is left exactly as it was', () => {
  const bodyFor = requestBuilder([VISION, IMAGE]);
  const body = bodyFor('image', {}, {prompt: 'a cat', seed: 4});
  assert.equal(body.structured, undefined);
  assert.equal(body.system_prompt, undefined);
  assert.equal(body.prompt, 'a cat');
});


test('a wired video reaches the vision endpoint as its own field', () => {
  const bodyFor = requestBuilder([VISION, IMAGE]);
  const body = bodyFor('vision', {video_url: 'https://autorig.online/dev/api/scratch/c.mp4'},
                       {prompt: 'What happens?', video_mode: 'storyboard'});
  assert.equal(body.video_url, 'https://autorig.online/dev/api/scratch/c.mp4');
  assert.equal(body.video_mode, 'storyboard');
  assert.equal(body.image_url, undefined);
});


test('the catalogue and the editor agree on which nodes carry a system prompt', () => {
  const catalogue = fs.readFileSync(cataloguePath, 'utf8');
  assert.match(catalogue, /SYSTEM_PROMPT_DEFAULT = \(/);
  for (const serviceId of ['vision', 'text']) {
    const block = catalogue.slice(catalogue.indexOf(`"id": "${serviceId}"`));
    const declaration = block.slice(0, block.indexOf('"outputs"'));
    assert.match(declaration, /"system_prompt_capable": True/,
                 `${serviceId} no longer declares the capability`);
    assert.match(declaration, /"system_prompt_default": SYSTEM_PROMPT_DEFAULT/,
                 `${serviceId} no longer ships the default`);
  }
});


test('the vision node declares a video socket next to its image socket', () => {
  const catalogue = fs.readFileSync(cataloguePath, 'utf8');
  const block = catalogue.slice(catalogue.indexOf('"id": "vision"'));
  const inputs = block.slice(0, block.indexOf('"outputs"'));
  assert.match(inputs, /\{"type": VIDEO, "field": "video_url", "required": False/);
  assert.match(inputs, /\{"type": IMAGE, "field": "image"/);
});


test('the editor keeps the prompt on the node, never in its answer', () => {
  const source = fs.readFileSync(nodesPath, 'utf8');
  // readParams is what a save, a copy and the agent's snapshot all go through.
  const readParams = source.slice(source.indexOf('  function readParams(id) {'));
  assert.match(readParams.slice(0, readParams.indexOf('\n  }')),
               /values\._system_prompt = systemPromptOf\(id\)/);
  // The marker is derived from the default, so an untouched node stays plain.
  assert.match(source, /const custom = text\.trim\(\) !== defaultSystemPrompt\(id\)\.trim\(\)/);
  // Editing it must make the node's old answer stale like any other setting.
  const setter = source.slice(source.indexOf('  function setSystemPrompt(id, text) {'));
  assert.match(setter.slice(0, setter.indexOf('\n  }')),
               /invalidateNodeAndDownstream\(id\)/);
});


test('a duplicated or pasted node carries its system prompt with it', () => {
  const source = fs.readFileSync(groupsPath, 'utf8');
  const payload = source.slice(source.indexOf('    function clipboardPayload() {'));
  // Service nodes are copied with every parameter, `_system_prompt` included;
  // a narrower copy here is what would silently drop it.
  assert.match(payload.slice(0, payload.indexOf('const kept =')),
               /params: node\.params && typeof node\.params === 'object'/);
});


test('the context menu offers the editor only where a node can use one', () => {
  const source = fs.readFileSync(groupsPath, 'utf8');
  assert.match(source, /commandButton\('System prompt…'/);
  assert.match(source, /systemPromptTargets\(\[String\(contextNodeId\), \.\.\.Array\.from\(selected\)\]\)/);
  assert.match(source, /if \(promptTargets\.length\) \{/);
});


test('an agent edit that names a system prompt is applied to the node', () => {
  const source = fs.readFileSync(bridgePath, 'utf8');
  assert.match(source, /typeof params\._system_prompt === 'string'/);
  assert.match(source, /applySystemPrompt\(id, params\._system_prompt\)/);
  // An edit that does not mention it must leave the node's own one alone.
  const guard = source.slice(source.indexOf('if (applySystemPrompt'));
  assert.match(guard.slice(0, 260), /node\.kind === 'service'/);
});

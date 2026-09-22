import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';


const sourcePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-graph-agent.js');

function load() {
  let source = fs.readFileSync(sourcePath, 'utf8');
  source = source.replace(
    '  window.AIGraphAgent = {install};',
    '  window.AIGraphAgent = {install}; window.__agentTest = {compactGraph, compactCatalogue, parseProposal};');
  const context = {window:{}, URL, URLSearchParams, console};
  vm.runInNewContext(source, context, {filename:sourcePath});
  return context.window.__agentTest;
}

test('compact graph omits results and media bytes while retaining every node id', () => {
  const api = load();
  const huge = 'data:image/png;base64,' + 'x'.repeat(90000);
  const nodes = Array.from({length:85}, (_, index) => ({
    id:'n' + index, kind:index ? 'service' : 'input',
    entity_type:index ? undefined : 'image', service:index ? 'image' : undefined,
    value:index ? '' : huge, params:{prompt:'p' + index}, x:index, y:0
  }));
  const compact = api.compactGraph({name:'Large', nodes, links:[], results:{n1:{value:huge}}}, ['n4']);
  assert.equal(compact.all_node_ids.length, 85);
  assert.deepEqual(Array.from(compact.nodes, node => node.id), ['n4']);
  assert.match(compact.context_scope, /selected 1 of 85/);
  assert.equal(JSON.stringify(compact).includes('base64'), false);
  assert.equal(JSON.stringify(compact).includes('results'), false);
});

test('proposal accepts only the operation JSON contract', () => {
  const api = load();
  const graph = {nodes:[{id:'a', kind:'input', entity_type:'text', value:'hello'}], links:[]};
  const accepted = api.parseProposal(JSON.stringify({
    message:'Rename it', operations:[{op:'rename_graph', name:'Story'}]
  }), graph);
  assert.equal(accepted.operations[0].op, 'rename_graph');
  assert.throws(() => api.parseProposal(JSON.stringify({
    message:'bad', operations:[{op:'run_script', code:'alert(1)'}]
  }), graph), /not allowed/);
  assert.throws(() => api.parseProposal(JSON.stringify({
    message:'bad', operations:[], javascript:'alert(1)'
  }), graph), /unsupported fields/);
});

test('proposal rejects invented URLs and unsafe schemes but permits an existing graph URL', () => {
  const api = load();
  const existing = 'https://autorig.online/dev/api/scratch/existing.png';
  const graph = {nodes:[{id:'a', kind:'input', entity_type:'image', value:existing}], links:[]};
  assert.doesNotThrow(() => api.parseProposal(JSON.stringify({
    message:'reuse', operations:[{op:'set_input', id:'a', value:existing}]
  }), graph));
  assert.throws(() => api.parseProposal(JSON.stringify({
    message:'invent', operations:[{op:'set_input', id:'a', value:'https://evil.example/x.png'}]
  }), graph), /invented an external URL/);
  assert.throws(() => api.parseProposal(JSON.stringify({
    message:'unsafe', operations:[{op:'set_input', id:'a', value:'javascript:alert(1)'}]
  }), graph), /unsafe value/);
});

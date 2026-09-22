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
    '  window.AIGraphAgent = {install}; window.__agentTest = {compactGraph, compactCatalogue, parseProposal, buildAgentInput, modelBudget, reasoningRetryBudget, modelRequest, systemPrompt:SYSTEM_PROMPT};');
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

test('standing graph instructions stay in the system role, separate from graph data', () => {
  const api = load();
  const input = JSON.stringify({graph:{nodes:[]}, user_request:'Add an image node'});
  const request = api.modelRequest(api.systemPrompt, input, {id:'bonsai2-27b'}, 2048);
  assert.equal(request.system_prompt, api.systemPrompt);
  assert.equal(request.input, input);
  assert.equal(request.prompt, undefined);
  assert.equal(request.model, 'bonsai2-27b');
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

test('two-node Canny edit keeps both nodes before compacting a large catalogue', () => {
  const api = load();
  const graph = {
    name:'Canny QA',
    nodes:[
      {id:'source', kind:'input', entity_type:'image', value:'https://autorig.online/dev/api/scratch/source.png', x:0, y:0, params:{}},
      {id:'canny', kind:'service', service:'control_canny', x:400, y:200,
       params:{low_threshold:100, high_threshold:200}}
    ],
    links:[{from:'source', output:'value', to:'canny', input:'image'}]
  };
  const services = Array.from({length:90}, (_, index) => ({
    id:index === 42 ? 'control_canny' : 'service_' + index,
    title:'Service ' + index,
    status:'live',
    inputs:[{field:'image', type:'image', required:true}],
    outputs:[{field:'result', type:'image'}],
    params_array:Array.from({length:8}, (_unused, param) => ({
      name:index === 42 && param === 0 ? 'low_threshold' : 'parameter_' + param,
      type:'number', min:0, max:2048,
      options:Array.from({length:30}, (_x, option) => ({value:'option_' + option}))
    }))
  }));
  const catalogue = {
    entity_types_array:[{id:'image', title:'Image'}], services_array:services
  };
  const model = {context_tokens:4096, max_output_tokens:2048};
  const built = api.buildAgentInput('Move the Canny node to the right', model,
    graph, catalogue, [], []);
  const payload = JSON.parse(built.encoded);
  assert.deepEqual(Array.from(payload.graph.nodes, node => node.id), ['source', 'canny']);
  assert.deepEqual(Array.from(payload.graph.all_node_ids), ['source', 'canny']);
  const canny = payload.catalogue.services.find(service => service.id === 'control_canny');
  assert.ok(canny);
  assert.ok((canny.params || []).some(param => param.name === 'low_threshold'));
  assert.ok(api.systemPrompt.length + 20 + built.encoded.length < 8000);
});

test('reasoning exhaustion gets one larger budget and never loops', () => {
  const api = load();
  assert.equal(api.reasoningRetryBudget(
    'model_spent_its_budget_thinking: no answer', 2048, false), 4096);
  assert.equal(api.reasoningRetryBudget(
    'model_spent_its_budget_thinking: no answer', 4096, true), null);
  assert.equal(api.reasoningRetryBudget('worker unavailable', 2048, false), null);
});

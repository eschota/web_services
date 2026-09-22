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
    '  window.AIGraphAgent = {install}; window.__agentTest = {compactGraph, compactCatalogue, parseProposal, buildAgentInput, modelBudget, reasoningRetryBudget, modelRequest, mentionedIds, systemPrompt:SYSTEM_PROMPT};');
  const context = {window:{}, URL, URLSearchParams, console};
  vm.runInNewContext(source, context, {filename:sourcePath});
  return context.window.__agentTest;
}

const BONSAI = {id:'bonsai2-27b', context_tokens:4096, max_output_tokens:2048, reasons_first:true,
  graph_agent_instruction_role:'system'};
const QWEN = {id:'qwen35-9b-uncensored', context_tokens:8192, reasons_first:false,
  graph_agent_instruction_role:'prompt'};

test('compact graph omits results and media bytes while indexing every node', () => {
  const api = load();
  const huge = 'data:image/png;base64,' + 'x'.repeat(90000);
  const nodes = Array.from({length:85}, (_, index) => ({
    id:'n' + index, kind:index ? 'service' : 'input',
    entity_type:index ? undefined : 'image', service:index ? 'image' : undefined,
    value:index ? '' : huge, params:{prompt:'p' + index, lora:'', steps:0}, x:index, y:0
  }));
  const compact = api.compactGraph({name:'Large', nodes, links:[], results:{n1:{value:huge}}}, ['n4'], '', null);
  assert.equal(compact.index.split(',').length, 85);
  assert.match(compact.index, /^n0:input\/image,n1:image,/);
  assert.deepEqual(Array.from(compact.nodes, node => node.id), ['n4']);
  assert.match(compact.context_scope, /details for 1 of 85/);
  assert.equal(JSON.stringify(compact.nodes[0].params), JSON.stringify({prompt:'p4'}));
  assert.equal(JSON.stringify(compact).includes('base64'), false);
  assert.equal(JSON.stringify(compact).includes('results'), false);
});

test('ids named in the request and their neighbours get details, the rest stays in the index', () => {
  const api = load();
  const nodes = Array.from({length:30}, (_, index) => ({
    id:String(index + 1), kind:'service', service:index % 3 ? 'image' : 'video', params:{}, x:0, y:index
  }));
  nodes.unshift({id:'src', kind:'input', entity_type:'image', value:'https://autorig.online/a.png', x:0, y:0});
  const links = [
    {from:'src', output:'value', to:'26', input:'image'},
    {from:'26', output:'video_url_string', to:'27', input:'image'},
    {from:'src', output:'value', to:'3', input:'image'}
  ];
  assert.deepEqual(Array.from(api.mentionedIds('скопируй ноду 26 десять раз', nodes.map(node => node.id))), ['26']);
  // "10" is also an existing id: a number in the request costs one extra
  // detailed node rather than risking a named node being left out.
  const compact = api.compactGraph({name:'g', nodes, links}, [], 'Make 10 copies of node 26 with different loras', null);
  assert.deepEqual(new Set(compact.nodes.map(node => node.id)), new Set(['src', '10', '26', '27']));
  assert.equal(compact.links.length, 3);
  assert.match(compact.context_scope, /details for 4 of 31/);
  const whole = api.compactGraph({name:'g', nodes, links}, [], 'rename the graph', null);
  assert.equal(whole.nodes.length, 31);
  assert.match(whole.context_scope, /whole graph/);
});

test('proposal accepts only the operation JSON contract, including clone_nodes', () => {
  const api = load();
  const graph = {nodes:[{id:'a', kind:'input', entity_type:'text', value:'hello'}], links:[]};
  const accepted = api.parseProposal(JSON.stringify({
    message:'Rename it', operations:[{op:'rename_graph', name:'Story'},
      {op:'clone_nodes', ids:['a'], variants:[{a:{value:'hi'}}]}]
  }), graph);
  assert.equal(accepted.operations[0].op, 'rename_graph');
  assert.equal(accepted.operations[1].op, 'clone_nodes');
  assert.throws(() => api.parseProposal(JSON.stringify({
    message:'bad', operations:[{op:'run_script', code:'alert(1)'}]
  }), graph), /not allowed/);
  assert.throws(() => api.parseProposal(JSON.stringify({
    message:'bad', operations:[], javascript:'alert(1)'
  }), graph), /unsupported fields/);
});

test('a cut-off JSON answer is reported as truncated and prose around JSON is tolerated', () => {
  const api = load();
  const graph = {nodes:[], links:[]};
  let failure = null;
  try { api.parseProposal('{"message":"x","operations":[{"op":"rename_graph","na', graph); }
  catch (error) { failure = error; }
  assert.ok(failure && failure.truncated === true);
  let plain = null;
  try { api.parseProposal('not json at all', graph); }
  catch (error) { plain = error; }
  assert.ok(plain && !plain.truncated);
  const wrapped = api.parseProposal('Here you go:\n{"message":"ok","operations":[]}\nDone.', graph);
  assert.equal(wrapped.message, 'ok');
});

test('standing instructions use the system role only where the worker verified it', () => {
  const api = load();
  const input = JSON.stringify({graph:{nodes:[]}, user_request:'Add an image node'});
  const system = api.modelRequest(api.systemPrompt, input, BONSAI, -1);
  assert.equal(system.system_prompt, api.systemPrompt);
  assert.equal(system.input, input);
  assert.equal(system.prompt, undefined);
  assert.equal(system.model, 'bonsai2-27b');
  const prompt = api.modelRequest(api.systemPrompt, input, QWEN, -1);
  assert.equal(prompt.prompt, api.systemPrompt);
  assert.equal(prompt.system_prompt, undefined);
  assert.equal(prompt.max_output_tokens, -1);
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
  const built = api.buildAgentInput('Move the Canny node to the right', BONSAI,
    graph, catalogue, [], []);
  const payload = JSON.parse(built.encoded);
  assert.deepEqual(Array.from(payload.graph.nodes, node => node.id), ['source', 'canny']);
  assert.equal(payload.graph.index, 'source:input/image,canny:control_canny');
  const canny = payload.catalogue.services.control_canny;
  assert.ok(canny);
  assert.match(canny.params, /low_threshold/);
  assert.equal(Object.keys(payload.catalogue.services).length, 1);
  assert.match(payload.catalogue.other_services, /service_0/);
  assert.ok(api.systemPrompt.length + 20 + built.encoded.length < 8000);
  assert.equal(built.outputTokens, -1);
});

test('current model catalogue fits a two-node image graph with every usable image filename', () => {
  const api = load();
  const cataloguePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)),
    '..', '..', '..', 'deploy', 'ai-models', 'model_catalogue.json');
  const currentModels = JSON.parse(fs.readFileSync(cataloguePath, 'utf8'));
  const imageModels = currentModels.filter(item =>
    item.usable && (item.services || []).includes('image'));
  assert.equal(currentModels.length, 18);
  assert.equal(imageModels.length, 9);
  const graph = {name:'All image models', nodes:[
    {id:'prompt', kind:'input', entity_type:'text', value:'A neutral studio scene', x:0, y:0, params:{}},
    {id:'image', kind:'service', service:'image', x:360, y:0,
     params:{width:960, height:540, steps:0, cfg:0, sampler:'', scheduler:''}}
  ], links:[{from:'prompt', output:'value', to:'image', input:'prompt'}], results:{}};
  const catalogue = {
    entity_types_array:[{id:'text', title:'Text'}, {id:'image', title:'Image'}],
    services_array:[{id:'image', title:'Image', status:'live',
      inputs:[{field:'prompt', type:'text', required:true}],
      outputs:[{field:'image_url_string', type:'image'}], params_array:[]}],
    models_array:currentModels
  };
  const built = api.buildAgentInput('Add every available image model', BONSAI, graph, catalogue, [], []);
  const payload = JSON.parse(built.encoded);
  assert.deepEqual(Array.from(payload.graph.nodes, node => node.id), ['prompt', 'image']);
  for (const item of imageModels) {
    assert.ok(payload.catalogue.models.some(line => line.includes(item.file)), item.file);
  }
  assert.ok(payload.catalogue.models.every(line => /^(checkpoint|lora) \S+ \S+: /.test(line)));
  assert.ok(api.systemPrompt.length + 20 + built.encoded.length < 8000);
});

test('a ten-variant video request on a chain fits the 4k window with room to answer', () => {
  const api = load();
  const graph = {name:'image to ten videos', nodes:[
    {id:'1', kind:'input', entity_type:'image', value:'https://autorig.online/dev/api/scratch/a.png', x:0, y:0, params:{}},
    {id:'2', kind:'service', service:'vision', x:300, y:0, params:{model:'qwen35-9b-uncensored', prompt:'Describe the picture', max_output_tokens:0}},
    {id:'3', kind:'service', service:'text', x:600, y:0, params:{model:'qwen35-9b-uncensored', prompt:'Write a 50-word video prompt', max_output_tokens:0}},
    {id:'4', kind:'service', service:'video', x:900, y:0, params:{width:540, height:960, checkpoint:'ltx-2.3-22b-distilled-1.1_transformer_only_fp8_scaled.safetensors', lora:'', lora_strength:0, frame_count:97, negative_prompt:'', cfg:0, sampler:'', scheduler:'', steps:0, creativity:0, seed:0}}
  ], links:[
    {from:'1', output:'value', to:'2', input:'image'}, {from:'2', output:'answer_string', to:'3', input:'input'},
    {from:'3', output:'answer_string', to:'4', input:'prompt'}, {from:'1', output:'value', to:'4', input:'image'}
  ], results:{}};
  const cataloguePath = path.resolve(path.dirname(fileURLToPath(import.meta.url)),
    '..', '..', '..', 'deploy', 'ai-models', 'model_catalogue.json');
  const models = JSON.parse(fs.readFileSync(cataloguePath, 'utf8'));
  const service = (id, params) => ({id, title:id, status:'live', inputs:[{field:'image', type:'image'}, {field:'prompt', type:'text'}],
    outputs:[{field:'out', type:'video'}], params_array:params});
  const catalogue = {entity_types_array:[{id:'text'}, {id:'image'}, {id:'video'}], models_array:models,
    services_array:[
      service('vision', [{name:'model', type:'select', options:[{value:'bonsai2-27b'}, {value:'qwen35-9b-uncensored'}]}, {name:'prompt', type:'textarea'}, {name:'max_output_tokens', type:'number', min:0, max:4096}]),
      service('text', [{name:'model', type:'select', options:[{value:'bonsai2-27b'}, {value:'qwen35-9b-uncensored'}]}, {name:'prompt', type:'textarea'}, {name:'max_output_tokens', type:'number', min:0, max:4096}]),
      service('video', [{name:'width', type:'number', min:256, max:2048}, {name:'height', type:'number', min:256, max:2048}, {name:'checkpoint', type:'model'}, {name:'lora', type:'model'},
        {name:'lora_strength', type:'range', min:0, max:1.5}, {name:'frame_count', type:'range', min:9, max:393}, {name:'negative_prompt', type:'text'}, {name:'cfg', type:'number', min:0, max:30},
        {name:'sampler', type:'select', options:[{value:''}, {value:'euler'}]}, {name:'scheduler', type:'select', options:[{value:''}, {value:'simple'}]}, {name:'steps', type:'range', min:0, max:60},
        {name:'creativity', type:'range', min:0, max:1}, {name:'seed', type:'number', min:0}]),
      service('image', []), service('3dmodel', []), service('control_pose', [])
    ]};
  const request = 'Сделай из пары нод 3 и 4 десять вариантов с разными лорами, моделями и длительностью';
  const built = api.buildAgentInput(request, BONSAI, graph, catalogue, [], [
    {role:'user', text:'earlier question'}, {role:'assistant', text:'earlier answer'}]);
  const payload = JSON.parse(built.encoded);
  assert.equal(payload.graph.nodes.length, 4);
  assert.ok(payload.catalogue.models.every(line => /video/.test(line)));
  assert.equal(payload.catalogue.services.image, undefined);
  assert.ok(payload.catalogue.services.video.params.includes('frame_count 9-393 8n+1@24fps'));
  assert.ok(built.encoded.length < api.modelBudget(BONSAI).inputChars);
  assert.ok(api.modelBudget(QWEN).inputChars > api.modelBudget(BONSAI).inputChars);
});

test('trailing commas are repaired and an attempt nonce defeats the request cache', () => {
  const api = load();
  const graph = {nodes:[{id:'a', kind:'input', entity_type:'text', value:'x'}], links:[]};
  const fixed = api.parseProposal('{"message":"ok","operations":[{"op":"rename_graph","name":"n",},],}', graph);
  assert.equal(fixed.operations.length, 1);
  const catalogue = {entity_types_array:[], services_array:[]};
  const first = api.buildAgentInput('rename it', QWEN, graph, catalogue, [], [], 'abc');
  const second = api.buildAgentInput('rename it', QWEN, graph, catalogue, [], [], 'abd');
  assert.notEqual(first.encoded, second.encoded);
  assert.equal(JSON.parse(first.encoded).attempt, 'abc');
  assert.equal(JSON.parse(api.buildAgentInput('rename it', QWEN, graph, catalogue, [], []).encoded).attempt, undefined);
});

test('Russian requests select services by keyword and an empty graph gets the core services', () => {
  const api = load();
  const services = ['image', 'video', 'text', 'vision', '3dmodel', 'avatar_image', 'control_pose'].map(id => ({
    id, title:id, status:'live', inputs:[], outputs:[], params_array:[{name:'p', type:'text'}]}));
  const catalogue = {entity_types_array:[{id:'image'}], services_array:services, models_array:[]};
  const empty = {name:'new', nodes:[], links:[]};
  const russian = JSON.parse(api.buildAgentInput('сделай секвенцию с текстовой генерацией и видео приветствия персонажа', QWEN, empty, catalogue, [], []).encoded);
  assert.ok(russian.catalogue.services.text && russian.catalogue.services.video && russian.catalogue.services.avatar_image);
  const vague = JSON.parse(api.buildAgentInput('сделай что-нибудь красивое', QWEN, empty, catalogue, [], []).encoded);
  assert.deepEqual(Object.keys(vague.catalogue.services).sort(), ['image', 'text', 'video', 'vision']);
});

test('an answer that stops a bracket or two short is closed when its last value is whole', () => {
  const api = load();
  const graph = {nodes:[{id:'a', kind:'input', entity_type:'text', value:'x'}], links:[]};
  const cut = '{"message":"ok","operations":[{"op":"clone_nodes","ids":["a"],"variants":[{"a":{"value":"v1"}},{"a":{"value":"v2"}}]}';
  const parsed = api.parseProposal(cut, graph);
  assert.equal(parsed.operations[0].variants.length, 2);
  let failure = null;
  try { api.parseProposal('{"message":"ok","operations":[{"op":"clone_nodes","ids":["a"],"variants":[{"a":{"value":"v', graph); }
  catch (error) { failure = error; }
  assert.ok(failure && failure.truncated);
});

test('the standing instructions name the single output socket of input nodes', () => {
  const api = load();
  assert.match(api.systemPrompt, /output socket named "value"/);
  assert.ok(api.systemPrompt.length < 2000);
});

test('reasoning exhaustion gets one larger budget and never loops', () => {
  const api = load();
  assert.equal(api.reasoningRetryBudget(
    'model_spent_its_budget_thinking: no answer', 2048, false), -1);
  assert.equal(api.reasoningRetryBudget(
    'model_spent_its_budget_thinking: no answer', 4096, true), null);
  assert.equal(api.reasoningRetryBudget('worker unavailable', 2048, false), null);
  assert.equal(api.reasoningRetryBudget('model_spent_its_budget_thinking', -1, false), null);
});

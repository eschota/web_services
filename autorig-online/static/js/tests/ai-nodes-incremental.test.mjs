import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';


const sourcePath = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-nodes.js');

test('motion video runner calls the endpoint declared by the backend service', () => {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('const RUNNERS = {');
  const end = source.indexOf("['pose', 'depth', 'canny'].forEach", start);
  const runners = vm.runInNewContext(source.slice(start, end) + '; RUNNERS', {
    pollForFile() {}, pollAiStatus() {}, poll3dStatus() {},
  });
  const catalogue = fs.readFileSync(path.resolve(path.dirname(sourcePath),
    '..', '..', 'backend', 'ai_services.py'), 'utf8');
  const declaredApi = catalogue.match(/"id": "video_control"[\s\S]*?"api": "([^"]+)"/)[1];
  assert.equal(runners.video_control.api, declaredApi);
  assert.equal(runners.video_control.api, runners.video.api);
});


function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}


function graph(nodes, links = []) {
  return {name: 'test', nodes, links, results: {}};
}


function input(id, value = 'prompt') {
  return {id, kind: 'input', entity_type: 'text', value, params: {}};
}


function service(id, params = {}) {
  return {id, kind: 'service', service: 'image', params};
}


function harness(graphs) {
  const controls = {
    run: {disabled: false, textContent: ''},
    cancel: {hidden: true},
    continue: {hidden: false},
    toast: {textContent: '', className: '', _timer: null},
  };
  const inputValues = new Map();
  for (const candidate of graphs) {
    for (const node of candidate.nodes) {
      if (node.kind === 'input') inputValues.set(node.id, node.value);
    }
  }
  const inputElements = new Map([...inputValues].map(([id, value]) => [
    'node-' + id,
    {querySelector(selector) {
      if (selector === '[data-value]') return {value};
      return null;
    }},
  ]));
  let graphIndex = 0;
  const calls = [];
  const jobs = [];
  const context = {
    console,
    setTimeout(fn, milliseconds) {
      return milliseconds >= 1000 ? 0 : setTimeout(fn, milliseconds);
    },
    clearTimeout,
    Promise,
    URL,
    URLSearchParams,
    CSS: {escape: value => value},
    history: {replaceState() {}},
    location: {origin: 'https://autorig.online', search: ''},
    fetch: async () => ({ok: true, json: async () => ({})}),
    document: {
      getElementById(id) { return controls[id] || inputElements.get(id) || null; },
      createElement() { return {}; },
      dispatchEvent() {},
      addEventListener() {},
    },
    CustomEvent: class {},
    window: {addEventListener() {}},
    __testGraph() {
      return graphs[Math.min(graphIndex++, graphs.length - 1)];
    },
    async __testEnsureSaved() {},
    __testRunServiceNode(id) {
      calls.push(String(id));
      const job = deferred();
      jobs.push({id: String(id), ...job});
      return job.promise;
    },
  };
  context.globalThis = context;

  let source = fs.readFileSync(sourcePath, 'utf8');
  source = source.replace(
    '    const graph = graphFromCanvas();\n    const epoch = canvasEpoch;',
    '    const graph = globalThis.__testGraph();\n    const epoch = canvasEpoch;');
  source = source.replace('    await ensureSaved();', '    await globalThis.__testEnsureSaved();');
  source = source.replace(
    '    const promise = runServiceNode(idString, resolved, params, execution)',
    '    const promise = globalThis.__testRunServiceNode(idString, resolved, params, execution)');
  source = source.replace(
    "  window.addEventListener('DOMContentLoaded', () => {",
    `  window.__aiNodesTest = {
      runGraph, cancelRun, resetCanvasExecutionState,
      submitJson,
      continuableResults, restoredExecutions, completedExecutions,
      activeExecutions, runRequests, setMeta
    };
  window.addEventListener('DOMContentLoaded', () => {`);
  vm.runInNewContext(source, context, {filename: sourcePath});
  const api = context.window.__aiNodesTest;
  for (const candidate of graphs) {
    for (const node of candidate.nodes) {
      api.setMeta(node.id, node.kind === 'input'
        ? {kind: 'input', entityType: node.entity_type, inFields: [], outFields: ['value']}
        : {kind: 'service', service: node.service, inFields: ['prompt'], outFields: ['image_url_string']});
    }
  }
  return {api, calls, jobs, controls, setFetch(fn) { context.fetch = fn; }};
}


async function tick() {
  await new Promise(resolve => setImmediate(resolve));
}


test('two Render clicks join one identical active node submission', async () => {
  const g = graph([input('a'), service('b', {seed: 41})], [
    {from: 'a', to: 'b', input: 'prompt'},
  ]);
  const h = harness([g, g]);
  const first = h.api.runGraph(false);
  await tick();
  const second = h.api.runGraph(false);
  await tick();
  assert.deepEqual(h.calls, ['b']);
  h.jobs[0].resolve({type: 'image', value: 'https://result/one.png'});
  await Promise.all([first, second]);
  assert.equal(h.controls.run.disabled, false);
});


test('new click supersedes an old child waiting on the same active upstream', async () => {
  const firstGraph = graph([input('a'), service('b', {seed: 9}), service('c', {seed: 10})], [
    {from: 'a', to: 'b', input: 'prompt'},
    {from: 'b', to: 'c', input: 'image'},
  ]);
  const changedGraph = graph([input('a'), service('b', {seed: 9}), service('c', {seed: 11})], firstGraph.links);
  const h = harness([firstGraph, changedGraph]);
  const oldRun = h.api.runGraph(false);
  await tick();
  const newRun = h.api.runGraph(false);
  await tick();
  assert.deepEqual(h.calls, ['b']);
  h.jobs[0].resolve({type: 'image', value: 'https://result/upstream.png'});
  await tick();
  assert.deepEqual(h.calls, ['b', 'c']);
  h.jobs[1].resolve({type: 'image', value: 'https://result/new-child.png'});
  await Promise.all([oldRun, newRun]);
  assert.equal(h.calls.filter(id => id === 'c').length, 1);
});


test('Cancel while waiting prevents an unsent downstream node', async () => {
  const g = graph([input('a'), service('b', {seed: 1}), service('c', {seed: 2})], [
    {from: 'a', to: 'b', input: 'prompt'},
    {from: 'b', to: 'c', input: 'image'},
  ]);
  const h = harness([g]);
  const run = h.api.runGraph(false);
  await tick();
  await h.api.cancelRun();
  h.jobs[0].resolve({type: 'image', value: 'https://result/upstream.png'});
  await run;
  assert.deepEqual(h.calls, ['b']);
});


test('Continue reuses an untouched completed seed-zero node', async () => {
  const g = graph([input('a'), service('b', {seed: 0})], [
    {from: 'a', to: 'b', input: 'prompt'},
  ]);
  const h = harness([g]);
  h.api.continuableResults.set('b', {
    type: 'image', value: 'https://result/saved.png', task_id: 'saved-task',
  });
  await h.api.runGraph(true);
  assert.deepEqual(h.calls, []);
});


test('Render joins a restored running seed-zero node instead of posting', async () => {
  const g = graph([input('a'), service('b', {seed: 0})], [
    {from: 'a', to: 'b', input: 'prompt'},
  ]);
  const h = harness([g]);
  const restored = deferred();
  h.api.restoredExecutions.set('b', {
    epoch: 1, invalidated: false, promise: restored.promise,
  });
  const run = h.api.runGraph(false);
  await tick();
  assert.deepEqual(h.calls, []);
  restored.resolve({type: 'image', value: 'https://result/restored.png'});
  await run;
});


test('canvas reset prevents an old completion entering the completed cache', async () => {
  const g = graph([input('a'), service('b', {seed: 77})], [
    {from: 'a', to: 'b', input: 'prompt'},
  ]);
  const h = harness([g]);
  const run = h.api.runGraph(false);
  await tick();
  h.api.resetCanvasExecutionState();
  h.jobs[0].resolve({type: 'image', value: 'https://result/old.png'});
  await run;
  assert.equal(h.api.completedExecutions.size, 0);
});


function response(status, body, headers = {}) {
  const normalized = new Map(Object.entries(headers).map(([key, value]) =>
    [key.toLowerCase(), String(value)]));
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: {get(name) { return normalized.get(String(name).toLowerCase()) || null; }},
    async text() { return body; },
  };
}


test('submit retries a known-unaccepted 429 and preserves the exact body', async () => {
  const h = harness([graph([input('a')])]);
  const bodies = [];
  let attempt = 0;
  h.setFetch(async (_url, options) => {
    bodies.push(options.body);
    attempt += 1;
    return attempt === 1
      ? response(429, '{"detail":"rate limited"}', {'Retry-After': '0'})
      : response(202, '{"task_id_string":"accepted-1","image_url_string":"https://result/1.png"}');
  });
  const accepted = await h.api.submitJson('/api/ai/image', {prompt:'same', seed:0});
  assert.equal(accepted.task_id_string, 'accepted-1');
  assert.equal(bodies.length, 2);
  assert.equal(bodies[0], bodies[1]);
});


test('HTML 502 becomes a clear error and is never retried', async () => {
  const h = harness([graph([input('a')])]);
  let calls = 0;
  h.setFetch(async () => {
    calls += 1;
    return response(502, '<html><title>Bad Gateway</title></html>');
  });
  await assert.rejects(
    h.api.submitJson('/api/ai/image', {prompt:'ambiguous', seed:0}),
    error => /HTTP 502/.test(error.message) && /not retried/.test(error.message) &&
      !/Unexpected token/.test(error.message));
  assert.equal(calls, 1);
});


test('a burst starts at most four submits in flight and spaces starts', async () => {
  const h = harness([graph([input('a')])]);
  const starts = [];
  let inFlight = 0;
  let maximum = 0;
  h.setFetch(async () => {
    starts.push(Date.now());
    inFlight += 1;
    maximum = Math.max(maximum, inFlight);
    await new Promise(resolve => setTimeout(resolve, 700));
    inFlight -= 1;
    return response(202, '{"task_id_string":"ok"}');
  });
  await Promise.all(Array.from({length: 9}, (_, index) =>
    h.api.submitJson('/api/ai/image', {prompt:'burst-' + index, seed:index + 1})));
  assert.equal(starts.length, 9);
  assert.ok(maximum <= 4, `maximum in flight was ${maximum}`);
  for (let index = 1; index < starts.length; index += 1) {
    assert.ok(starts[index] - starts[index - 1] >= 225,
      `starts ${index - 1}/${index} were only ${starts[index] - starts[index - 1]}ms apart`);
  }
});

import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const sourcePath = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)), '..', 'ai-nodes.js');

function saver(initialId, instance, responses) {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf('  async function persistGraph()');
  const end = source.indexOf('  /** A run needs a link', start);
  assert.ok(start > 0 && end > start, 'persistGraph block found');
  const calls = [];
  const urls = [];
  const context = {
    graphId: initialId, graphInstanceId: instance,
    graphFromCanvas: () => ({name: 'g', instance_id: context.graphInstanceId, nodes: [], links: []}),
    fetch: async (url, options) => {
      calls.push({url, method: options.method, body: JSON.parse(options.body)});
      const [status, data] = responses.shift();
      return {status, ok: status < 400, json: async () => data};
    },
    history: {replaceState: (_a, _b, url) => urls.push(url)},
    document: {dispatchEvent() {}},
    CustomEvent: class { constructor(name, init) { this.name = name; this.detail = init.detail; } },
    window: {crypto: {randomUUID: () => 'fresh-instance'}},
    crypto: {randomUUID: () => 'fresh-instance'},
    Date, Math,
  };
  vm.createContext(context);
  vm.runInContext(
    'var graphId = this.graphId, graphInstanceId = this.graphInstanceId;\n' +
    source.slice(start, end) +
    '\nthis.run = async () => { const r = await persistGraph(); if (r.response.ok) adoptSavedId(r.data);' +
    ' return {r, graphId, graphInstanceId}; };', context);
  return {context, calls, urls};
}

test('a graph with a link is updated in place, never re-created', async () => {
  const {context, calls, urls} = saver('abc123def456', '', [
    [200, {graph_id_string: 'abc123def456', deep_link_string: '/nodes?g=abc123def456'}],
  ]);
  const out = await context.run();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, 'PUT');
  assert.equal(calls[0].url, '/api/ai/graphs/abc123def456');
  assert.equal(out.graphId, 'abc123def456');
  assert.deepEqual(urls, ['/nodes?g=abc123def456']);
  // A legacy graph keeps its empty instance: the link is its identity now.
  assert.equal(calls[0].body.instance_id, '');
});

test('the first save creates one document with a fresh instance id', async () => {
  const {context, calls} = saver(null, '', [
    [200, {graph_id_string: 'new000000001', deep_link_string: '/nodes?g=new000000001'}],
  ]);
  const out = await context.run();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, 'POST');
  assert.equal(calls[0].url, '/api/ai/graphs');
  assert.equal(calls[0].body.instance_id, 'fresh-instance');
  assert.equal(out.graphInstanceId, 'fresh-instance');
  assert.equal(out.graphId, 'new000000001');
});

test('a link the store no longer knows falls back to creating a document', async () => {
  const {context, calls} = saver('gone00000000', 'kept-instance', [
    [404, {detail: {error_string: 'graph_not_found'}}],
    [200, {graph_id_string: 'new000000002', deep_link_string: '/nodes?g=new000000002'}],
  ]);
  const out = await context.run();
  assert.deepEqual(calls.map(call => call.method), ['PUT', 'POST']);
  assert.equal(calls[1].body.instance_id, 'kept-instance');
  assert.equal(out.graphId, 'new000000002');
});

test('a refused edit is reported, not silently saved as a copy', async () => {
  const {context, calls} = saver('abc123def456', '', [
    [400, {detail: {message_string: 'type mismatch'}}],
  ]);
  const out = await context.run();
  assert.equal(calls.length, 1);
  assert.equal(out.r.response.ok, false);
  assert.equal(out.graphId, 'abc123def456');
});

test('picking a template detaches the page from the open graph', () => {
  const source = fs.readFileSync(sourcePath, 'utf8');
  const start = source.indexOf("button.className = 'tmpl';");
  const block = source.slice(start, source.indexOf('picker.appendChild(button);', start));
  assert.match(block, /graphId = null;[\s\S]*replaceState\(null, '', '\/nodes'\)[\s\S]*loadGraph\(template\.graph\)/);
});

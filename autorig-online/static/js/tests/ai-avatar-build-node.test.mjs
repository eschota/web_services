/**
 * The Avatar builder node: one job, many outputs.
 *
 * Every view it draws sits on its own socket, so a wire from "Back" must
 * carry the back view and not the Avatar id, and a reopened link must keep
 * every output, not only the first.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '..', 'ai-nodes.js'), 'utf8');

function load(fetchImpl) {
  const start = source.indexOf('  function splitMulti(finished) {');
  const end = source.indexOf('  async function poll3dStatus(', start);
  assert.ok(start > 0 && end > start, 'the multi-output helpers must stay sliceable');
  const context = {
    fetch: fetchImpl || (async () => { throw new Error('no fetch'); }),
    sleep: async () => {},
    readJsonResponse: async response => ({data: await response.json(), text: ''}),
    describeError: (body, status) => (body && body.detail) || ('HTTP ' + status),
    encodeURIComponent,
  };
  return vm.runInNewContext(source.slice(start, end) +
    '; ({splitMulti, outputValue, avatarBuildOutputs, pollAvatarBuild})', context);
}

test('a runner may finish with {value, outputs}; a plain value stays plain', () => {
  const api = load();
  assert.deepEqual({...api.splitMulti('https://x/a.png')}, {value: 'https://x/a.png', outputs: null});
  const multi = api.splitMulti({value: 'av_1@1', outputs: {back_url_string: 'https://x/b.png'}});
  assert.equal(multi.value, 'av_1@1');
  assert.equal(multi.outputs.back_url_string, 'https://x/b.png');
});

test('a wire reads the output it is plugged into', () => {
  const api = load();
  const upstream = {type: 'avatar', value: 'av_1@1',
                    outputs: {avatar_string: 'av_1@1', back_url_string: 'https://x/b.png'}};
  assert.equal(api.outputValue(upstream, 'back_url_string'), 'https://x/b.png');
  assert.equal(api.outputValue(upstream, 'avatar_string'), 'av_1@1');
  // Single-output nodes never had outputs: the value, whatever the field.
  assert.equal(api.outputValue({type: 'image', value: 'https://x/i.png'}, 'image_url_string'),
               'https://x/i.png');
});

test('the builder poller waits for the job and returns every view', async () => {
  const id = 'avb_' + 'a'.repeat(24);
  const answers = [
    {finished_bool: false, stage_string: 'views', views_object: {front: {}}},
    {finished_bool: true, success_bool: true, status_string: 'completed', avatar_string: 'av_x@2',
     front_url_string: 'https://autorig.online/f.png', back_url_string: 'https://autorig.online/b.png',
     description_string: 'adult woman', error_string: ''},
  ];
  const calls = [];
  const api = load(async url => {
    calls.push(url);
    const body = answers.shift();
    return {ok: true, status: 200, json: async () => body};
  });
  const reports = [];
  const result = await api.pollAvatarBuild({task_id_string: id, finished_bool: false}, {},
                                           data => reports.push(data));
  assert.equal(result.value, 'av_x@2');
  assert.equal(result.outputs.back_url_string, 'https://autorig.online/b.png');
  assert.equal(result.outputs.description_string, 'adult woman');
  assert.equal(result.outputs.profile_left_url_string, undefined);
  assert.equal(calls[0], '/api/ai/avatar-build/status/' + id);
  assert.match(reports[0].stage_string, /views · 1 views/);
});

test('a failed build is an error, not an empty Avatar', async () => {
  const api = load();
  await assert.rejects(api.pollAvatarBuild({task_id_string: 'avb_' + 'b'.repeat(24), finished_bool: true,
                                            success_bool: false, status_string: 'failed',
                                            error_string: 'the front view could not be made'}, {}, null),
                       /front view/);
  await assert.rejects(api.pollAvatarBuild({task_id_string: 'nope'}, {}, null), /no job/);
});

test('the node wiring uses the helpers on every path', () => {
  assert.match(source, /avatar_build: \{ api: '\/api\/ai\/avatar-build', finish: pollAvatarBuild/);
  assert.match(source, /resolved\[link\.input\] = outputValue\(upstream, link\.output\)/);
  assert.match(source, /showResult\(outBox, record\.type, record\.value, record\.outputs\)/);
});

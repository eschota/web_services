import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import vm from 'node:vm';
import { resolveGameProjectRoot } from '../tools/game-project-root.mjs';

async function createHarness() {
  const gameRoot = await resolveGameProjectRoot();
  const source = await readFile(
    resolve(gameRoot, 'Assets', 'SandFlowOnline', 'Voice', 'Plugins', 'WebGL', 'SandFlowVoice.jslib'),
    'utf8',
  );
  const messages = [];
  let injectedScript;
  const context = vm.createContext({
    LibraryManager: { library: {} },
    mergeInto: (target, additions) => Object.assign(target, additions),
    UTF8ToString: (value) => value,
    Module: {
      streamingAssetsUrl: '/sandflow/StreamingAssets',
      SendMessage: (...args) => messages.push(args),
    },
    document: {
      createElement: () => ({}),
      head: { appendChild: (script) => { injectedScript = script; } },
    },
  });
  vm.runInContext(source, context);
  context.SFVoiceLoader = context.LibraryManager.library.$SFVoiceLoader;
  return { context, library: context.LibraryManager.library, messages, getScript: () => injectedScript };
}

function completeApi(calls) {
  const api = {};
  for (const name of [
    'initialize', 'connect', 'enableMicrophone', 'disableMicrophone',
    'setPushToTalkPressed', 'setSelfMuted', 'setRemoteMuted',
    'setRemoteVolume', 'setRemoteBlocked', 'startAudio', 'disconnect',
  ]) api[name] = (...args) => calls.push([name, ...args]);
  return api;
}

test('jslib loader rejects a bundle missing any required export', async () => {
  const harness = await createHarness();
  harness.library.SFVoice_Initialize('VoiceReceiver', 1);
  harness.context.SandFlowVoiceWeb = { initialize() {} };
  harness.getScript().onload();
  assert.equal(harness.context.SFVoiceLoader.failed, true);
  assert.equal(harness.context.SFVoiceLoader.queue.length, 0);
  assert.deepEqual(harness.messages, [
    ['VoiceReceiver', 'OnSandFlowVoiceError', 'bundle-api-missing'],
    ['VoiceReceiver', 'OnSandFlowVoiceState', 'disabled'],
  ]);
  assert.equal(harness.library.SFVoice_Initialize('VoiceReceiver', 1), 0, 'failed loader cannot claim a fresh initialized adapter');
  assert.equal(harness.context.SFVoiceLoader.queue.length, 0);
});

test('jslib loader bounds pending operations and never replays after overflow', async () => {
  const harness = await createHarness();
  const calls = [];
  harness.library.SFVoice_Initialize('VoiceReceiver', 1);
  harness.library.SFVoice_EnableMicrophone();
  for (let index = 0; index < 63; index++) harness.library.SFVoice_Connect('0123456789abcdef0123456789abcdef');
  assert.equal(harness.context.SFVoiceLoader.failed, true);
  assert.equal(harness.context.SFVoiceLoader.queue.length, 0);
  assert.deepEqual(harness.messages, [
    ['VoiceReceiver', 'OnSandFlowVoiceError', 'bundle-queue-overflow'],
    ['VoiceReceiver', 'OnSandFlowVoiceState', 'disabled'],
  ]);

  harness.context.SandFlowVoiceWeb = completeApi(calls);
  harness.getScript().onload();
  assert.deepEqual(calls, [], 'failed queue must never replay after a late script load');
});

test('jslib loader never queues or replays PTT edges while bundle loads', async () => {
  const harness = await createHarness();
  const calls = [];
  harness.library.SFVoice_Initialize('VoiceReceiver', 1);
  harness.library.SFVoice_EnableMicrophone();
  harness.library.SFVoice_SetPushToTalk(1);
  harness.library.SFVoice_SetPushToTalk(0);
  harness.library.SFVoice_Connect('0123456789abcdef0123456789abcdef');
  assert.equal(harness.context.SFVoiceLoader.queue.length, 3);

  harness.context.SandFlowVoiceWeb = completeApi(calls);
  harness.getScript().onload();
  assert.deepEqual(calls, [
    ['initialize', 'VoiceReceiver', 1],
    ['enableMicrophone'],
    ['connect', '0123456789abcdef0123456789abcdef'],
  ]);
  assert.equal(calls.some(([name]) => name === 'setPushToTalkPressed'), false);
});

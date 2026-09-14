import assert from 'node:assert/strict';
import test from 'node:test';
import { UnityVoiceBridge } from '../.work/dist/unityBridge.js';

class MockController {
  calls = [];
  async switchMembership(request) { this.calls.push(['connect', request]); }
  enableMicrophone() { this.calls.push(['enableMicrophone']); }
  async disableMicrophone() { this.calls.push(['disableMicrophone']); }
  async setPushToTalkPressed(value) { this.calls.push(['ptt', value]); }
  async setSelfMuted(value) { this.calls.push(['selfMuted', value]); }
  setRemoteMuted(id, value) { this.calls.push(['remoteMuted', id, value]); }
  setRemoteVolume(id, value) { this.calls.push(['remoteVolume', id, value]); }
  setRemoteBlocked(id, value) { this.calls.push(['remoteBlocked', id, value]); }
  async startAudioFromUserGesture() { this.calls.push(['startAudio']); }
  async disconnect() { this.calls.push(['disconnect']); }
}

test('Unity bridge fails closed when disabled or outside pinned production origin', () => {
  let creates = 0;
  const messages = [];
  const dependencies = {
    origin: 'https://evil.example',
    sendMessage: (...args) => messages.push(args),
    createController: () => { creates++; return new MockController(); },
  };
  const bridge = new UnityVoiceBridge(dependencies);
  assert.equal(bridge.initialize('VoiceObject', true), false);
  bridge.enableMicrophone();
  bridge.setPushToTalkPressed(true);
  assert.equal(creates, 0);
  assert.deepEqual(messages, [['VoiceObject', 'OnSandFlowVoiceState', 'disabled']]);
});

test('Unity bridge performs no microphone action before explicit gesture methods', async () => {
  const controller = new MockController();
  let callbacks;
  const bridge = new UnityVoiceBridge({
    origin: 'https://autorig.online',
    sendMessage: () => {},
    createController: (value) => { callbacks = value; return controller; },
  });
  assert.equal(bridge.initialize('VoiceObject', true), true);
  bridge.connect('0123456789abcdef0123456789abcdef');
  await Promise.resolve();
  assert.deepEqual(controller.calls, [['connect', { sandboxId: '0123456789abcdef0123456789abcdef' }]]);

  bridge.enableMicrophone();
  bridge.setPushToTalkPressed(true);
  await Promise.resolve();
  assert.deepEqual(controller.calls.slice(-2), [['enableMicrophone'], ['ptt', true]]);
  assert.ok(callbacks);
});

test('Unity bridge forwards only bounded callback payloads', () => {
  const controller = new MockController();
  const messages = [];
  let callbacks;
  const bridge = new UnityVoiceBridge({
    origin: 'https://autorig.online',
    sendMessage: (...args) => messages.push(args),
    createController: (value) => { callbacks = value; return controller; },
  });
  bridge.initialize('VoiceObject', true);
  callbacks.onStateChanged('connected');
  callbacks.onMicrophoneChanged({ consented: true, selfMuted: false, transmitting: true });
  callbacks.onActiveSpeakersChanged(['steam:1']);
  callbacks.onError('microphone-failed', new Error('must not cross bridge'));
  assert.deepEqual(messages, [
    ['VoiceObject', 'OnSandFlowVoiceState', 'connected'],
    ['VoiceObject', 'OnSandFlowVoiceMicrophone', '{"consented":true,"selfMuted":false,"transmitting":true}'],
    ['VoiceObject', 'OnSandFlowVoiceActiveSpeakers', '{"participantIds":["steam:1"]}'],
    ['VoiceObject', 'OnSandFlowVoiceError', 'microphone-failed'],
  ]);
});

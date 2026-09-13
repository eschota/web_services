import assert from 'node:assert/strict';
import test from 'node:test';
import { VoiceMediaController } from '../.work/dist/VoiceMediaController.js';
import { HttpVoiceTokenProvider } from '../.work/dist/httpTokenProvider.js';
import {
  SANDFLOW_AUDIO_CAPTURE_OPTIONS,
  SANDFLOW_AUDIO_PUBLISH_DEFAULTS,
} from '../.work/dist/liveKitBrowserAdapter.js';
import { MicrophonePublicationGate } from '../.work/dist/microphonePublicationGate.js';

const NOW = Date.parse('2026-09-14T00:00:00Z');
const WORLD_A = '0123456789abcdef0123456789abcdef';
const WORLD_B = 'fedcba9876543210fedcba9876543210';

function credentials(sandboxId, suffix = 'all', token = `token-${sandboxId}`) {
  return {
    serverUrl: 'wss://autorig.online/sandflow/voice',
    token,
    roomName: `sf:${sandboxId}:${suffix}`,
    participantId: 'steam:76561198000000000',
    expiresAt: '2026-09-14T00:05:00Z',
  };
}

class MockTokenProvider {
  constructor(values) { this.values = [...values]; this.requests = []; }
  async getToken(request, signal) {
    this.requests.push({ request, signal });
    const value = this.values.shift();
    if (!value) throw new Error('No token response queued.');
    return typeof value === 'function' ? value(request, signal) : value;
  }
}

class MockRoom {
  handlers = new Map();
  microphoneCalls = [];
  connectCalls = [];
  disconnectCalls = 0;
  startAudioCalls = 0;
  microphoneImplementation = async () => {};

  async connect(serverUrl, token) { this.connectCalls.push({ serverUrl, token }); }
  async disconnect() { this.disconnectCalls++; }
  async setMicrophoneEnabled(enabled) {
    this.microphoneCalls.push(enabled);
    await this.microphoneImplementation(enabled);
  }
  async stopMicrophone() {
    this.microphoneCalls.push(false);
    await this.microphoneImplementation(false);
  }
  async startAudio() { this.startAudioCalls++; }
  on(event, listener) {
    const listeners = this.handlers.get(event) ?? new Set();
    listeners.add(listener);
    this.handlers.set(event, listeners);
    return () => listeners.delete(listener);
  }
  emit(event, value) { for (const listener of this.handlers.get(event) ?? []) listener(value); }
}

class MockRoomFactory {
  constructor() { this.rooms = []; }
  create() { const room = new MockRoom(); this.rooms.push(room); return room; }
}

class MockRemoteAudio {
  constructor(participantId = 'steam:remote', trackId = 'TR_remote01') {
    this.participantId = participantId;
    this.trackId = trackId;
    this.attachCalls = 0;
    this.detachCalls = 0;
    this.volumes = [];
  }
  attach() { this.attachCalls++; }
  detach() { this.detachCalls++; }
  setVolume(volume) { this.volumes.push(volume); }
}

function createController(tokenValues, callbacks = {}) {
  const roomFactory = new MockRoomFactory();
  const tokenProvider = new MockTokenProvider(tokenValues);
  const controller = new VoiceMediaController({ roomFactory, tokenProvider, callbacks, now: () => NOW });
  return { controller, roomFactory, tokenProvider };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

test('connects receive-only and PTT is opt-in', async () => {
  const { controller, roomFactory } = createController([credentials(WORLD_A)]);
  await controller.switchMembership({ sandboxId: WORLD_A });
  const room = roomFactory.rooms[0];
  assert.deepEqual(room.connectCalls, [{ serverUrl: 'wss://autorig.online/sandflow/voice', token: `token-${WORLD_A}` }]);
  assert.equal(room.microphoneCalls.includes(true), false, 'connection must never capture or publish');

  controller.enableMicrophone();
  assert.equal(room.microphoneCalls.includes(true), false, 'consent only arms PTT');
  await controller.setPushToTalkPressed(true);
  assert.equal(controller.transmitting, true);
  assert.equal(room.microphoneCalls.at(-1), true);
  await controller.setPushToTalkPressed(false);
  assert.equal(controller.transmitting, false);
  assert.equal(room.microphoneCalls.at(-1), false);

  await controller.setSelfMuted(true);
  await controller.setPushToTalkPressed(true);
  assert.equal(controller.transmitting, false, 'self mute overrides PTT');
  await controller.disconnect();
  assert.equal(room.disconnectCalls, 1);
  assert.equal(room.microphoneCalls.at(-1), false);
});

test('disconnect wins an in-flight microphone enable race', async () => {
  const { controller, roomFactory } = createController([credentials(WORLD_A)]);
  await controller.switchMembership({ sandboxId: WORLD_A });
  controller.enableMicrophone();
  const room = roomFactory.rooms[0];
  const gate = deferred();
  room.microphoneImplementation = (enabled) => enabled ? gate.promise : Promise.resolve();

  const pressing = controller.setPushToTalkPressed(true);
  await Promise.resolve();
  const disconnecting = controller.disconnect();
  gate.resolve();
  await pressing;
  await disconnecting;
  assert.equal(room.microphoneCalls.at(-1), false, 'stale enable must be followed by disable');
  assert.equal(controller.transmitting, false);
  assert.equal(controller.state, 'idle');
});

test('microphone permission denial stays receive-only and reports failure', async () => {
  const errors = [];
  const { controller, roomFactory } = createController([credentials(WORLD_A)], {
    onError: (code) => errors.push(code),
  });
  await controller.switchMembership({ sandboxId: WORLD_A });
  controller.enableMicrophone();
  const room = roomFactory.rooms[0];
  room.microphoneImplementation = async (enabled) => { if (enabled) throw new DOMException('denied', 'NotAllowedError'); };
  await assert.rejects(controller.setPushToTalkPressed(true));
  assert.equal(controller.transmitting, false);
  assert.equal(controller.state, 'connected');
  assert.equal(errors.at(-1), 'microphone-failed');
});

test('membership transition disconnects old team before using fresh server token', async () => {
  const { controller, roomFactory, tokenProvider } = createController([
    credentials(WORLD_A, 'team:0', 'team-zero-token'),
    credentials(WORLD_A, 'team:1', 'team-one-token'),
  ]);
  await controller.switchMembership({ sandboxId: WORLD_A });
  const oldRoom = roomFactory.rooms[0];
  controller.enableMicrophone();
  await controller.setPushToTalkPressed(true);
  await controller.switchMembership({ sandboxId: WORLD_A });
  const newRoom = roomFactory.rooms[1];

  assert.equal(oldRoom.microphoneCalls.at(-1), false);
  assert.equal(oldRoom.disconnectCalls, 1);
  assert.equal(newRoom.connectCalls[0].token, 'team-one-token');
  assert.equal(newRoom.microphoneCalls.includes(true), false, 'team transition resets PTT');
  assert.equal(tokenProvider.requests.length, 2);
});

test('stale or cross-sandbox token responses are rejected', async () => {
  const { controller, roomFactory } = createController([credentials(WORLD_B)]);
  await assert.rejects(controller.switchMembership({ sandboxId: WORLD_A }));
  assert.equal(roomFactory.rooms.length, 0);
  assert.equal(controller.state, 'error');
});

test('remote mute volume block and active-speaker hooks are local', async () => {
  const speakers = [];
  const { controller, roomFactory } = createController([credentials(WORLD_A)], {
    onActiveSpeakersChanged: (ids) => speakers.push([...ids]),
  });
  await controller.switchMembership({ sandboxId: WORLD_A });
  const room = roomFactory.rooms[0];
  const audio = new MockRemoteAudio();
  room.emit('trackSubscribed', audio);
  assert.equal(audio.attachCalls, 1);
  assert.equal(audio.volumes.at(-1), 1);

  controller.setRemoteVolume(audio.participantId, 0.4);
  assert.equal(audio.volumes.at(-1), 0.4);
  controller.setRemoteMuted(audio.participantId, true);
  assert.equal(audio.volumes.at(-1), 0);
  controller.setRemoteMuted(audio.participantId, false);
  assert.equal(audio.volumes.at(-1), 0.4);
  controller.setRemoteBlocked(audio.participantId, true);
  assert.equal(audio.detachCalls > 0, true);
  room.emit('activeSpeakersChanged', [audio.participantId, 'steam:other']);
  assert.deepEqual(speakers.at(-1), ['steam:other']);
  controller.setRemoteBlocked(audio.participantId, false);
  assert.equal(audio.volumes.at(-1), 0.4);
});

test('reconnect restores controller state without bypassing current mic intent', async () => {
  const states = [];
  const { controller, roomFactory } = createController([credentials(WORLD_A)], {
    onStateChanged: (state) => states.push(state),
  });
  await controller.switchMembership({ sandboxId: WORLD_A });
  const room = roomFactory.rooms[0];
  controller.enableMicrophone();
  await controller.setPushToTalkPressed(false);
  room.emit('reconnecting');
  assert.equal(controller.state, 'reconnecting');
  room.emit('reconnected');
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(controller.state, 'connected');
  assert.equal(room.microphoneCalls.at(-1), false);
  assert.deepEqual(states.slice(-2), ['reconnecting', 'connected']);
});

test('disconnect and team switch do not wait for a never-resolving microphone prompt', async () => {
  const { controller, roomFactory } = createController([
    credentials(WORLD_A, 'team:0'),
    credentials(WORLD_A, 'team:1'),
  ]);
  await controller.switchMembership({ sandboxId: WORLD_A });
  const firstRoom = roomFactory.rooms[0];
  controller.enableMicrophone();
  const never = new Promise(() => {});
  firstRoom.microphoneImplementation = (enabled) => enabled ? never : Promise.resolve();
  void controller.setPushToTalkPressed(true);
  await Promise.resolve();
  await Promise.resolve();
  await Promise.race([
    controller.setPushToTalkPressed(false),
    new Promise((_, reject) => setTimeout(() => reject(new Error('PTT release blocked on microphone prompt')), 100)),
  ]);
  assert.equal(controller.transmitting, false);
  assert.equal(firstRoom.microphoneCalls.at(-1), false, 'PTT release requests stop without waiting for enable');

  await Promise.race([
    controller.switchMembership({ sandboxId: WORLD_A }),
    new Promise((_, reject) => setTimeout(() => reject(new Error('team switch blocked on microphone prompt')), 100)),
  ]);
  assert.equal(firstRoom.disconnectCalls, 1);
  assert.equal(roomFactory.rooms.length, 2);
  assert.equal(controller.state, 'connected');

  await Promise.race([
    controller.disconnect(),
    new Promise((_, reject) => setTimeout(() => reject(new Error('disconnect blocked on microphone prompt')), 100)),
  ]);
  assert.equal(controller.state, 'idle');
});

test('HTTP token provider rejects every noncanonical path and invalid world before fetch', async () => {
  let fetchCalls = 0;
  const fakeFetch = async () => {
    fetchCalls++;
    return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
  };
  for (const apiBaseUrl of [
    '//evil.example/sandflow/api/v1',
    '/\\evil.example/sandflow/api/v1',
    '/sandflow/api/v1?next=//evil.example',
    '/sandflow/api/v1#fragment',
    '/sandflow/api/%76%31',
    '/sandflow/../api/v1',
    '/sandflow/api/v1/',
  ]) {
    assert.throws(() => new HttpVoiceTokenProvider({ apiBaseUrl, fetch: fakeFetch }));
  }

  const provider = new HttpVoiceTokenProvider({ apiBaseUrl: '/sandflow/api/v1', fetch: fakeFetch });
  await assert.rejects(provider.getToken({ sandboxId: '../evil' }, new AbortController().signal));
  assert.equal(fetchCalls, 0);

  const badBearer = new HttpVoiceTokenProvider({
    apiBaseUrl: '/sandflow/api/v1',
    fetch: fakeFetch,
    getBearerSession: () => 'not-a-session\r\nX-Evil: yes',
  });
  await assert.rejects(badBearer.getToken({ sandboxId: WORLD_A }, new AbortController().signal));
  assert.equal(fetchCalls, 0);
});

test('credentials cannot redirect media to another secure websocket host', async () => {
  const redirected = { ...credentials(WORLD_A), serverUrl: 'wss://evil.example/voice' };
  const { controller, roomFactory } = createController([redirected]);
  await assert.rejects(controller.switchMembership({ sandboxId: WORLD_A }));
  assert.equal(roomFactory.rooms.length, 0);
});

test('pinned SDK audio options stay mono speech Opus-oriented with supported recovery knobs', () => {
  assert.deepEqual(SANDFLOW_AUDIO_CAPTURE_OPTIONS.channelCount, { exact: 1 });
  assert.equal(SANDFLOW_AUDIO_CAPTURE_OPTIONS.echoCancellation, true);
  assert.equal(SANDFLOW_AUDIO_CAPTURE_OPTIONS.noiseSuppression, true);
  assert.equal(SANDFLOW_AUDIO_CAPTURE_OPTIONS.autoGainControl, true);
  assert.equal(SANDFLOW_AUDIO_PUBLISH_DEFAULTS.audioPreset.maxBitrate, 32_000);
  assert.equal(SANDFLOW_AUDIO_PUBLISH_DEFAULTS.forceStereo, false);
  assert.equal(SANDFLOW_AUDIO_PUBLISH_DEFAULTS.dtx, true);
  assert.equal(SANDFLOW_AUDIO_PUBLISH_DEFAULTS.red, true);
  assert.equal(SANDFLOW_AUDIO_PUBLISH_DEFAULTS.preConnectBuffer, false);
  assert.equal('videoCodec' in SANDFLOW_AUDIO_PUBLISH_DEFAULTS, false);
});

test('publication gate stops a capture revoked while permission is pending without publishing', async () => {
  const gate = new MicrophonePublicationGate();
  const capture = deferred();
  let publishCalls = 0;
  let stopPublishedCalls = 0;
  const acquiredTrack = { stopCalls: 0, stop() { this.stopCalls++; } };
  const enabling = gate.captureThenPublish(
    () => false,
    () => capture.promise,
    async () => { publishCalls++; },
    async () => { stopPublishedCalls++; },
  );

  gate.invalidate();
  capture.resolve(acquiredTrack);
  assert.equal(await enabling, false);
  assert.equal(acquiredTrack.stopCalls, 1);
  assert.equal(publishCalls, 0, 'revoked capture must never reach publishTrack');
  assert.equal(stopPublishedCalls, 0, 'nothing was published, so no remote teardown is needed');
});

test('publication gate coalesces repeated enable and skips an existing publication', async () => {
  const gate = new MicrophonePublicationGate();
  const capture = deferred();
  let captureCalls = 0;
  let publishCalls = 0;
  let published = false;
  const acquire = () => { captureCalls++; return capture.promise; };
  const publish = async () => { publishCalls++; published = true; };
  const stopPublished = async () => { published = false; };

  const first = gate.captureThenPublish(() => published, acquire, publish, stopPublished);
  const repeated = gate.captureThenPublish(() => published, acquire, publish, stopPublished);
  assert.equal(captureCalls, 1, 'concurrent repeated enable must share one acquisition');
  capture.resolve({ stop() {} });
  assert.equal(await first, true);
  assert.equal(await repeated, true);
  assert.equal(publishCalls, 1);

  assert.equal(await gate.captureThenPublish(() => published, acquire, publish, stopPublished), true);
  assert.equal(captureCalls, 1, 'published microphone must not be reacquired');
  assert.equal(publishCalls, 1, 'published microphone must not be republished');
});

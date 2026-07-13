import assert from 'node:assert/strict';
import test from 'node:test';

import {
    AnimationPlaylistController,
    buildAnimationPlaylist,
} from '../animation-playlist-controller.js';

class FakeMixer {
    constructor() {
        this.listeners = new Map();
        this.timeScale = 1;
    }

    addEventListener(type, listener) {
        this.listeners.set(type, listener);
    }

    removeEventListener(type, listener) {
        if (this.listeners.get(type) === listener) this.listeners.delete(type);
    }

    finish(action) {
        this.listeners.get('finished')?.({ action });
    }
}

const fixtureClips = [
    { name: 'default_pose', duration: 0.05 },
    { name: 'run', duration: 0.8 },
    { name: 'idle_neutral', duration: 1.5 },
    { name: 'death', duration: 1.2 },
];

const fixtureManifest = {
    clips: [
        { id: 'idle_neutral', order: 0, loop: true },
        { id: 'run', order: 1, loop: true },
        { id: 'death', order: 2, loop: false, end_pose_id: 'death_end' },
        { id: 'default_pose', order: 3, loop: false, pose: true },
    ],
};

test('manifest order is authoritative and poses remain selectable but not autoplayed', () => {
    const entries = buildAnimationPlaylist(fixtureClips, fixtureManifest);
    assert.deepEqual(entries.map((entry) => entry.id), [
        'idle_neutral', 'run', 'death', 'default_pose',
    ]);
    assert.deepEqual(entries.filter((entry) => entry.autoplay).map((entry) => entry.id), [
        'idle_neutral', 'run', 'death',
    ]);
});

test('embedded order is preserved without a manifest', () => {
    const entries = buildAnimationPlaylist(fixtureClips);
    assert.deepEqual(entries.map((entry) => entry.name), fixtureClips.map((clip) => clip.name));
    assert.equal(entries[0].autoplay, false);
    assert.equal(entries[1].loop, true);
});

test('autoplay plays every moving clip once and wraps on mixer finished', () => {
    const mixer = new FakeMixer();
    const played = [];
    let actionIndex = 0;
    const controller = new AnimationPlaylistController({
        playClip(name, options) {
            const action = { id: ++actionIndex, name };
            played.push({ name, options, action });
            return action;
        },
    });
    controller.configure({ mixer, clips: fixtureClips, manifest: fixtureManifest });

    assert.equal(controller.startAutoplay(), true);
    assert.equal(played[0].name, 'idle_neutral');
    assert.equal(played[0].options.loopOnce, true);
    mixer.finish(played[0].action);
    mixer.finish(played[1].action);
    mixer.finish(played[2].action);
    assert.deepEqual(played.map((item) => item.name), [
        'idle_neutral', 'run', 'death', 'idle_neutral',
    ]);
});

test('unrelated finished actions do not advance the shared index', () => {
    const mixer = new FakeMixer();
    const played = [];
    const controller = new AnimationPlaylistController({
        playClip(name) {
            const action = { name };
            played.push(action);
            return action;
        },
    });
    controller.configure({ mixer, clips: fixtureClips, manifest: fixtureManifest });
    controller.startAutoplay();
    mixer.finish({ name: 'other' });
    assert.deepEqual(played.map((item) => item.name), ['idle_neutral']);
});

test('any user interaction makes manual mode sticky until reload/controller recreation', () => {
    const mixer = new FakeMixer();
    const played = [];
    const controller = new AnimationPlaylistController({
        playClip(name, options) {
            const action = { name };
            played.push({ name, options, action });
            return action;
        },
    });
    controller.configure({ mixer, clips: fixtureClips, manifest: fixtureManifest });
    controller.startAutoplay();
    controller.markInteraction('wheel');
    mixer.finish(played[0].action);
    assert.equal(played.length, 1);
    assert.equal(controller.startAutoplay(), false);

    controller.configure({ mixer, clips: fixtureClips, manifest: fixtureManifest });
    assert.equal(controller.startAutoplay(), false);
    assert.equal(controller.manualPlay('run'), true);
    assert.equal(played.at(-1).options.loopOnce, false);
});

test('terminal transitions avoid crossfade and restore root before each clip', () => {
    const mixer = new FakeMixer();
    const calls = [];
    const controller = new AnimationPlaylistController({
        restoreRoot(context) {
            calls.push({ type: 'restore', next: context.next.id });
        },
        playClip(name, options) {
            const action = { name };
            calls.push({ type: 'play', name, fade: options.fade });
            return action;
        },
    });
    controller.configure({ mixer, clips: fixtureClips, manifest: fixtureManifest });
    controller.manualPlay('run');
    controller.manualPlay('death');
    assert.deepEqual(calls, [
        { type: 'restore', next: 'run' },
        { type: 'play', name: 'run', fade: 0.2 },
        { type: 'restore', next: 'death' },
        { type: 'play', name: 'death', fade: 0 },
    ]);
});

test('visibility suspension pauses and resumes autoplay without entering manual mode', () => {
    const mixer = new FakeMixer();
    const played = [];
    const controller = new AnimationPlaylistController({
        playClip(name) {
            const action = { name };
            played.push(action);
            return action;
        },
    });
    controller.configure({ mixer, clips: fixtureClips, manifest: fixtureManifest });
    controller.startAutoplay();

    assert.equal(controller.suspend(), true);
    assert.equal(mixer.timeScale, 0);
    assert.equal(controller.manualMode, false);
    mixer.finish(played[0]);
    assert.deepEqual(played.map((item) => item.name), ['idle_neutral']);

    assert.equal(controller.resume(), true);
    assert.equal(mixer.timeScale, 1);
    mixer.finish(played[0]);
    assert.deepEqual(played.map((item) => item.name), ['idle_neutral', 'run']);
});

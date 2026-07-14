import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
    TaskAnimationFittingPanel,
    buildThreeAnimationClip,
    decodeVideoFramesExact,
    flattenQaMetrics,
    mapHorseSemanticPalette,
} from '../task-animation-fitting-panel.js';

const NEAR_FAR_PALETTE = {
    fore_near: [0, 0.85, 1],
    fore_far: [0.12, 0.22, 1],
    hind_near: [1, 0.72, 0.02],
    hind_far: [1, 0.08, 0.55],
};

class FakeQuaternionTrack {
    constructor(name, times, values) {
        this.name = name;
        this.times = times;
        this.values = values;
        this.kind = 'quaternion';
    }
}

class FakeVectorTrack {
    constructor(name, times, values) {
        this.name = name;
        this.times = times;
        this.values = values;
        this.kind = 'vector';
    }
}

class FakeAnimationClip {
    constructor(name, duration, tracks) {
        this.name = name;
        this.duration = duration;
        this.tracks = tracks;
    }
}

const THREE = {
    QuaternionKeyframeTrack: FakeQuaternionTrack,
    VectorKeyframeTrack: FakeVectorTrack,
    AnimationClip: FakeAnimationClip,
};

function fittedResult(frameCount = 3) {
    return {
        schema: 'autorig-browser-fitted-animation.v1',
        frameCount,
        fps: 30,
        durationSeconds: (frameCount - 1) / 30,
        loop: true,
        tracks: [
            {
                bone: 'fore_leg.L',
                times: [0, 1 / 30, 2 / 30],
                values: [
                    0, 0, 0, 1,
                    0, 0.1, 0, 0.995,
                    0, 0, 0, 1,
                ],
            },
            {
                name: 'hind_leg.R.quaternion',
                bone: 'hind_leg.R',
                times: [0, 1 / 30, 2 / 30],
                values: [
                    0, 0, 0, 1,
                    0.1, 0, 0, 0.995,
                    0, 0, 0, 1,
                ],
            },
        ],
        positionTracks: [{
            bone: 'fore_leg.L',
            times: [0, 1 / 30, 2 / 30],
            values: [0, 0, 0, 0.01, 0, 0, 0, 0, 0],
        }],
        rootTrack: {
            bone: 'Horse_root',
            times: [0, 1 / 30, 2 / 30],
            values: [0, 0, 0, 0, 0.01, 0, 0, 0, 0],
        },
        qa: {
            loop: { closureError: 0 },
            contacts: { maximumHoofSlidePx: 1.25 },
            accepted: true,
        },
    };
}

function decodedFrames(count) {
    return Array.from({ length: count }, (_, index) => ({
        index,
        timestampSeconds: index / 30,
        width: 1,
        height: 1,
        data: new Uint8ClampedArray([index, 0, 0, 255]),
    }));
}

function semanticFrames(count = 5) {
    const srgb = {
        fore_left: [0, 237, 255],
        fore_right: [97, 129, 255],
        hind_left: [255, 221, 39],
        hind_right: [255, 80, 196],
    };
    const xByLabel = { fore_left: 7, fore_right: 15, hind_left: 25, hind_right: 33 };
    return Array.from({ length: count }, (_, frameIndex) => {
        const width = 40;
        const height = 30;
        const data = new Uint8ClampedArray(width * height * 4);
        for (let pixel = 0; pixel < width * height; pixel += 1) {
            data[pixel * 4] = 150;
            data[pixel * 4 + 1] = 150;
            data[pixel * 4 + 2] = 150;
            data[pixel * 4 + 3] = 255;
        }
        Object.entries(xByLabel).forEach(([label, baseX], labelIndex) => {
            const phase = (frameIndex / (count - 1)) * Math.PI * 2 + labelIndex * Math.PI / 2;
            const xOffset = Math.round(Math.sin(phase));
            for (let y = 7; y <= 26; y += 1) {
                for (let x = baseX + xOffset; x <= baseX + xOffset + 3; x += 1) {
                    const offset = (y * width + x) * 4;
                    data[offset] = srgb[label][0];
                    data[offset + 1] = srgb[label][1];
                    data[offset + 2] = srgb[label][2];
                }
            }
        });
        return { index: frameIndex, timestampSeconds: frameIndex / 30, width, height, data };
    });
}

function browserSkeleton() {
    const xByLabel = { fore_left: 8.5, fore_right: 16.5, hind_left: 26.5, hind_right: 34.5 };
    return {
        schema: 'autorig-browser-fitting-skeleton.v1',
        limbs: Object.fromEntries(Object.entries(xByLabel).map(([label, x]) => [label, {
            joints: [
                {
                    bone: `${label}_upper`,
                    restStart: [x, 8],
                    restEnd: [x, 16.5],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: -1.5,
                    maxAngle: 1.5,
                },
                {
                    bone: `${label}_lower`,
                    restStart: [x, 16.5],
                    restEnd: [x, 25],
                    restQuaternion: [0, 0, 0, 1],
                    rotationAxis: [0, 0, 1],
                    minAngle: -1.8,
                    maxAngle: 1.8,
                },
            ],
            trackedJointIndex: 1,
        }])),
    };
}

test('Horse semantic palette maps canonical near/far colors to left/right identities', () => {
    assert.deepEqual(mapHorseSemanticPalette(NEAR_FAR_PALETTE), {
        fore_left: NEAR_FAR_PALETTE.fore_near,
        fore_right: NEAR_FAR_PALETTE.fore_far,
        hind_left: NEAR_FAR_PALETTE.hind_near,
        hind_right: NEAR_FAR_PALETTE.hind_far,
    });

    const manifest = {
        semantic_profile: { palette_linear: NEAR_FAR_PALETTE },
        classification: {
            near_far_assignment: {
                fore: { near_source_group: 'fore_right', far_source_group: 'fore_left' },
                hind: { near_source_group: 'hind_left', far_source_group: 'hind_right' },
            },
        },
    };
    assert.deepEqual(mapHorseSemanticPalette(manifest), {
        fore_right: NEAR_FAR_PALETTE.fore_near,
        fore_left: NEAR_FAR_PALETTE.fore_far,
        hind_left: NEAR_FAR_PALETTE.hind_near,
        hind_right: NEAR_FAR_PALETTE.hind_far,
    });
    assert.throws(
        () => mapHorseSemanticPalette(NEAR_FAR_PALETTE, {
            foreNearSourceGroup: 'fore_left',
            foreFarSourceGroup: 'fore_left',
        }),
        /map once to left and right/,
    );
});

test('fitted JSON becomes one Three AnimationClip with quaternion, deform-position and root tracks', () => {
    const clip = buildThreeAnimationClip(fittedResult(), THREE, { clipName: 'Horse_Walk_BrowserFit' });
    assert.equal(clip.name, 'Horse_Walk_BrowserFit');
    assert.equal(clip.duration, 2 / 30);
    assert.deepEqual(clip.tracks.map((track) => track.name), [
        'fore_leg.L.quaternion',
        'hind_leg.R.quaternion',
        'fore_leg.L.position',
        'Horse_root.position',
    ]);
    assert.deepEqual(clip.tracks.map((track) => track.kind), ['quaternion', 'quaternion', 'vector', 'vector']);
    assert.equal(clip.userData.autorigAnimationFitting.frameCount, 3);
    assert.equal(clip.userData.autorigAnimationFitting.qa.accepted, true);

    const invalid = fittedResult();
    invalid.tracks[0].values.pop();
    assert.throws(() => buildThreeAnimationClip(invalid, THREE), /4 values per keyframe/);
    assert.throws(
        () => buildThreeAnimationClip({ ...fittedResult(), schema: 'unknown.v1' }, THREE),
        /unsupported fitted animation schema/,
    );
});

class FakeVideo {
    constructor() {
        this.duration = 5;
        this.videoWidth = 2;
        this.videoHeight = 1;
        this.readyState = 4;
        this._currentTime = 0;
        this.listeners = new Map();
        this.drawnTimes = [];
    }

    get currentTime() {
        return this._currentTime;
    }

    set currentTime(value) {
        this._currentTime = Number(value);
        queueMicrotask(() => this.dispatch('seeked'));
    }

    addEventListener(name, callback) {
        if (!this.listeners.has(name)) this.listeners.set(name, new Set());
        this.listeners.get(name).add(callback);
    }

    removeEventListener(name, callback) {
        this.listeners.get(name)?.delete(callback);
    }

    dispatch(name) {
        for (const callback of [...(this.listeners.get(name) || [])]) callback();
    }

    load() {}

    pause() {}

    removeAttribute() {}
}

function fakeCanvas(video) {
    const context = {
        drawImage() {
            video.drawnTimes.push(video.currentTime);
        },
        getImageData(_x, _y, width, height) {
            const data = new Uint8ClampedArray(width * height * 4);
            data[0] = Math.round(video.currentTime * 10);
            data[3] = 255;
            return { data };
        },
    };
    return {
        width: 0,
        height: 0,
        getContext() { return context; },
    };
}

test('MP4 decoder samples exactly N distinct presentation times and rejects cross-origin URLs', async () => {
    const video = new FakeVideo();
    const progress = [];
    const decoded = await decodeVideoFramesExact('/semantic-horse.mp4', {
        frameCount: 5,
        fps: 1,
        video,
        canvas: fakeCanvas(video),
        location: { href: 'https://autorig.online/task/1', origin: 'https://autorig.online' },
        onProgress(event) { progress.push(event); },
    });
    assert.equal(decoded.frames.length, 5);
    assert.deepEqual(video.drawnTimes, [0, 1, 2, 3, 4]);
    assert.deepEqual(decoded.frames.map((frame) => frame.timestampSeconds), [0, 1, 2, 3, 4]);
    assert.equal(progress.length, 5);
    assert.equal(progress.at(-1).progress, 1);

    await assert.rejects(
        decodeVideoFramesExact('/legacy-41-frame.mp4', {
            frameCount: 49,
            fps: 30,
            video: Object.assign(new FakeVideo(), { duration: 41 / 30 }),
            canvas: fakeCanvas(new FakeVideo()),
            location: { href: 'https://autorig.online/task/1', origin: 'https://autorig.online' },
        }),
        /frame contract mismatch/,
    );

    await assert.rejects(
        decodeVideoFramesExact('https://foreign.example/horse.mp4', {
            frameCount: 5,
            video: new FakeVideo(),
            canvas: fakeCanvas(new FakeVideo()),
            location: { href: 'https://autorig.online/task/1', origin: 'https://autorig.online' },
        }),
        /same-origin/,
    );
});

test('pure panel pipeline decodes, tracks, solves, builds clip and reports QA without a mixer', async () => {
    const calls = [];
    const statuses = [];
    const ready = [];
    const metrics = [];
    const scrubs = [];
    const skeleton = { limbs: { fore_left: {}, fore_right: {}, hind_left: {}, hind_right: {} } };
    const panel = new TaskAnimationFittingPanel({
        THREE,
        frameCount: 3,
        fps: 30,
        skeleton,
        palette: NEAR_FAR_PALETTE,
        decoder: async (_source, options) => {
            calls.push('decode');
            options.onProgress({ frameIndex: 2, progress: 1 });
            return { frames: decodedFrames(3) };
        },
        tracker(frames, palette, options) {
            calls.push('track');
            assert.equal(frames.length, 3);
            assert.deepEqual(palette.fore_left, NEAR_FAR_PALETTE.fore_near);
            assert.equal(options.fps, 30);
            return { schema: 'autorig-fitting-observations.v1', frame_count: 3, tracks: [] };
        },
        solver({ skeleton: receivedSkeleton, observations, options }) {
            calls.push('solve');
            assert.equal(receivedSkeleton, skeleton);
            assert.equal(observations.frame_count, 3);
            assert.equal(options.contactWeight, 2);
            return fittedResult();
        },
        solveOptions: { contactWeight: 2 },
        clipName: 'Horse_Walk_BrowserFit',
        onStatus(status) { statuses.push(status); },
        onMetrics(qa) { metrics.push(qa); },
        onScrub(event) { scrubs.push(event.index); },
        onClipReady(clip, context) { ready.push({ clip, context }); },
    });

    const result = await panel.fitSource('/semantic-horse.mp4');
    assert.deepEqual(calls, ['decode', 'track', 'solve']);
    assert.equal(result.clip.name, 'Horse_Walk_BrowserFit');
    assert.equal(result.frames.length, 3);
    assert.equal(ready.length, 1);
    assert.equal(ready[0].context.observations.frame_count, 3);
    assert.equal(metrics[0].accepted, true);
    assert.equal(statuses.at(-1).stage, 'ready');
    assert.equal(statuses.at(-1).progress, 1);
    assert.deepEqual(scrubs, [0]);
    assert.equal(panel.scrub(99).index, 2);
    assert.deepEqual(scrubs, [0, 2]);
});

test('default dynamic tracker and browser core interoperate through the panel contract', async () => {
    const panel = new TaskAnimationFittingPanel({
        THREE,
        frameCount: 5,
        fps: 30,
        skeleton: browserSkeleton(),
        palette: NEAR_FAR_PALETTE,
        decoder: async () => ({ frames: semanticFrames(5) }),
        trackerOptions: { minimumPixels: 40, colorTolerance: 0.12 },
        solveOptions: { loop: true, smoothingRadius: 0 },
    });
    const result = await panel.fitSource('/semantic-horse.mp4');
    assert.equal(result.result.schema, 'autorig-browser-fitted-animation.v1');
    assert.equal(result.observations.tracks.length, 12);
    assert.equal(result.clip.tracks.length, 8);
    assert.equal(result.result.qa.maximumBoneLengthErrorPx < 1e-8, true);
    assert.equal(result.result.qa.loopEndpointError, 0);
});

test('panel fails closed when decoder does not return the exact requested frame count', async () => {
    let trackerCalled = false;
    const statuses = [];
    const panel = new TaskAnimationFittingPanel({
        THREE,
        frameCount: 3,
        skeleton: { limbs: {} },
        palette: NEAR_FAR_PALETTE,
        decoder: async () => ({ frames: decodedFrames(2) }),
        tracker() { trackerCalled = true; },
        solver() { return fittedResult(); },
        onStatus(status) { statuses.push(status); },
    });
    await assert.rejects(panel.fitSource('/too-short.mp4'), /expected exactly 3/);
    assert.equal(trackerCalled, false);
    assert.equal(statuses.at(-1).stage, 'failed');
    assert.equal(statuses.at(-1).error, true);
});

test('QA metrics flatten deterministically and panel owns no mixer or render loop', async () => {
    assert.deepEqual(flattenQaMetrics({ z: true, loop: { error: 0.1 }, count: 4 }), [
        { key: 'count', value: 4 },
        { key: 'loop.error', value: 0.1 },
        { key: 'z', value: true },
    ]);
    const source = await readFile(new URL('../task-animation-fitting-panel.js', import.meta.url), 'utf8');
    assert.doesNotMatch(source, /new\s+(?:THREE\.)?AnimationMixer/);
    assert.doesNotMatch(source, /requestAnimationFrame|setInterval\s*\(/);
    assert.match(source, /import\('\.\/animation-fitting-browser-core\.js'\)/);
    assert.match(source, /buildSemanticObservations/);
});

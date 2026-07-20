import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

async function importSource(relativePath) {
    const source = await readFile(new URL(relativePath, import.meta.url), 'utf8');
    return import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
}

const adaptive = await importSource('../task-adaptive-quality.js');

test('degrades after three seconds below 29 FPS', () => {
    let state = adaptive.createAdaptiveQualityState({ mode: 'high' });
    let result;
    for (let second = 1; second <= 3; second += 1) {
        result = adaptive.sampleAdaptiveQuality(state, { fps: 28, p95FrameTime: 36, now: second * 1000 });
        state = result.state;
    }
    assert.equal(state.mode, 'balanced');
    assert.equal(result.change?.to, 'balanced');
});

test('degrades after two seconds below 20 FPS', () => {
    let state = adaptive.createAdaptiveQualityState({ mode: 'balanced' });
    state = adaptive.sampleAdaptiveQuality(state, { fps: 19, p95FrameTime: 60, now: 1000 }).state;
    const result = adaptive.sampleAdaptiveQuality(state, { fps: 19, p95FrameTime: 60, now: 2000 });
    assert.equal(result.state.mode, 'low');
});

test('degrades after sustained p95 frame time above 42 ms', () => {
    let state = adaptive.createAdaptiveQualityState({ mode: 'low' });
    let result;
    for (let second = 1; second <= 3; second += 1) {
        result = adaptive.sampleAdaptiveQuality(state, { fps: 34, p95FrameTime: 48, now: second * 1000 });
        state = result.state;
    }
    assert.equal(state.mode, 'emergency');
    assert.equal(result.change?.to, 'emergency');
});

test('recovers one level only after twelve healthy seconds', () => {
    let state = adaptive.createAdaptiveQualityState({ mode: 'low' });
    let result;
    for (let second = 1; second <= 12; second += 1) {
        result = adaptive.sampleAdaptiveQuality(state, { fps: 58, p95FrameTime: 17, now: second * 1000 });
        state = result.state;
    }
    assert.equal(state.mode, 'balanced');
    assert.equal(result.change?.reason, 'fps-recover-58');
});

test('rolls back a failed recovery probe', () => {
    let state = adaptive.createAdaptiveQualityState({ mode: 'low' });
    for (let second = 1; second <= 12; second += 1) {
        state = adaptive.sampleAdaptiveQuality(state, { fps: 58, p95FrameTime: 17, now: second * 1000 }).state;
    }
    const result = adaptive.sampleAdaptiveQuality(state, { fps: 25, p95FrameTime: 44, now: 14500 });
    assert.equal(result.state.mode, 'low');
    assert.match(result.change?.reason || '', /^recovery-rollback-/);
});

test('suppression ignores transient heavy-operation frames', () => {
    let state = adaptive.createAdaptiveQualityState({ mode: 'high' });
    state = adaptive.suppressAdaptiveQuality(state, 1000, 2000);
    state = adaptive.sampleAdaptiveQuality(state, { fps: 8, p95FrameTime: 120, now: 2000 }).state;
    assert.equal(state.lowSeconds, 0);
    assert.equal(state.mode, 'high');
});

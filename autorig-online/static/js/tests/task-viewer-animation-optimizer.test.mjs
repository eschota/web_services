import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

async function importSource(relativePath) {
    const source = await readFile(new URL(relativePath, import.meta.url), 'utf8');
    return import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
}

const optimizer = await importSource('../task-viewer-animation-optimizer.js');

function attribute(values, itemSize) {
    return {
        array: values,
        itemSize,
        count: values.length / itemSize,
        getX(index) { return this.array[index * itemSize]; },
        getY(index) { return this.array[(index * itemSize) + 1]; },
        getZ(index) { return this.array[(index * itemSize) + 2]; },
        getW(index) { return this.array[(index * itemSize) + 3]; },
    };
}

function bone(name, parent = null) {
    return {
        name,
        parent,
        isBone: true,
        position: { x: 0, y: 0, z: 0 },
        quaternion: { x: 0, y: 0, z: 0, w: 1 },
        scale: { x: 1, y: 1, z: 1 },
    };
}

function clip(name, tracks) {
    return {
        name,
        tracks,
        clone() {
            return { ...this, tracks: [...this.tracks], clone: this.clone };
        },
    };
}

function modelFixture({ invalidIndex = false, duplicateHelperName = false } = {}) {
    const root = bone('Root');
    const deform = bone('Deform', root);
    const helper = bone('UnusedControl', root);
    const bones = [root, deform, helper];
    const mesh = {
        name: 'Mesh',
        isSkinnedMesh: true,
        skeleton: { bones },
        geometry: {
            attributes: {
                skinIndex: attribute([invalidIndex ? 99 : 1, 0, 0, 0], 4),
                skinWeight: attribute([1, 0, 0, 0], 4),
            },
        },
    };
    const nodes = [root, deform, helper, mesh];
    if (duplicateHelperName) nodes.push({ name: 'UnusedControl', isBone: false });
    return {
        traverse(callback) { nodes.forEach(callback); },
    };
}

const parseTrackName = (name) => {
    const [nodeName, propertyName] = name.split('.');
    return { nodeName, propertyName };
};

test('keeps weighted bones and ancestors while dropping only unique unused bone tracks', () => {
    const clips = [clip('Walk', [
        { name: 'Root.quaternion' },
        { name: 'Deform.quaternion' },
        { name: 'UnusedControl.quaternion' },
        { name: 'SceneNode.position' },
    ])];
    const result = optimizer.optimizeAnimationClipsForViewer(clips, modelFixture(), { parseTrackName });
    assert.equal(result.reliable, true);
    assert.equal(result.droppedTracks, 1);
    assert.deepEqual(result.clips[0].tracks.map((track) => track.name), [
        'Root.quaternion',
        'Deform.quaternion',
        'SceneNode.position',
    ]);
    assert.equal(result.requiredBoneCount, 2);
});

test('fails open for invalid skin indices', () => {
    const clips = [clip('Walk', [{ name: 'UnusedControl.quaternion' }])];
    const result = optimizer.optimizeAnimationClipsForViewer(clips, modelFixture({ invalidIndex: true }), { parseTrackName });
    assert.equal(result.reliable, false);
    assert.equal(result.droppedTracks, 0);
    assert.equal(result.clips[0], clips[0]);
});

test('keeps tracks with ambiguous node names', () => {
    const clips = [clip('Walk', [{ name: 'UnusedControl.quaternion' }])];
    const result = optimizer.optimizeAnimationClipsForViewer(
        clips,
        modelFixture({ duplicateHelperName: true }),
        { parseTrackName },
    );
    assert.equal(result.reliable, true);
    assert.equal(result.droppedTracks, 0);
    assert.equal(result.clips[0], clips[0]);
});

test('keeps unknown and non-bone targets unchanged', () => {
    const clips = [clip('Walk', [
        { name: 'Camera.position' },
        { name: 'Mesh.morphTargetInfluences[0]' },
    ])];
    const result = optimizer.optimizeAnimationClipsForViewer(clips, modelFixture(), { parseTrackName });
    assert.equal(result.reliable, true);
    assert.equal(result.droppedTracks, 0);
    assert.equal(result.clips[0], clips[0]);
});

function keyframeTrack(name, times, values, valueSize) {
    return {
        name,
        times: new Float32Array(times),
        values: new Float32Array(values),
        getValueSize() { return valueSize; },
        clone() {
            return keyframeTrack(this.name, [...this.times], [...this.values], valueSize);
        },
    };
}

test('drops a constant weighted-bone track only when it equals the rest transform', () => {
    const restTrack = keyframeTrack(
        'Deform.quaternion',
        [0, 1],
        [0, 0, 0, 1, 0, 0, 0, 1],
        4,
    );
    const clips = [clip('Idle', [restTrack, { name: 'Root.quaternion' }])];
    const result = optimizer.optimizeAnimationClipsForViewer(clips, modelFixture(), { parseTrackName });
    assert.equal(result.reliable, true);
    assert.equal(result.restTracksDropped, 1);
    assert.deepEqual(result.clips[0].tracks.map((track) => track.name), ['Root.quaternion']);
});

test('collapses a constant non-rest track to one key without changing its value', () => {
    const nonRestTrack = keyframeTrack(
        'Deform.position',
        [0, 0.5, 1],
        [2, 3, 4, 2, 3, 4, 2, 3, 4],
        3,
    );
    const clips = [clip('Pose', [nonRestTrack, { name: 'Root.quaternion' }])];
    const result = optimizer.optimizeAnimationClipsForViewer(clips, modelFixture(), { parseTrackName });
    assert.equal(result.reliable, true);
    assert.equal(result.constantTracksCollapsed, 1);
    assert.equal(result.clips[0].tracks[0].times.length, 1);
    assert.deepEqual([...result.clips[0].tracks[0].values], [2, 3, 4]);
});

test('keeps unsupported constant properties unchanged', () => {
    const track = keyframeTrack('Deform.weights', [0, 1], [0.5, 0.5], 1);
    const clips = [clip('Weights', [track])];
    const result = optimizer.optimizeAnimationClipsForViewer(clips, modelFixture(), { parseTrackName });
    assert.equal(result.droppedTracks, 0);
    assert.equal(result.clips[0], clips[0]);
});

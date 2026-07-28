import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

async function importSource(relativePath) {
    const source = await readFile(new URL(relativePath, import.meta.url), 'utf8');
    return import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
}

const { optimizeAnimationClipsForViewer } = await importSource('../task-viewer-animation-optimizer.js');

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

function attribute(values) {
    return {
        array: values,
        itemSize: 4,
        count: values.length / 4,
        getX(index) { return this.array[index * 4]; },
        getY(index) { return this.array[(index * 4) + 1]; },
        getZ(index) { return this.array[(index * 4) + 2]; },
        getW(index) { return this.array[(index * 4) + 3]; },
    };
}

test('keeps bones and ancestors that drive rigid renderable descendants', () => {
    const root = bone('Root');
    const weighted = bone('Weighted', root);
    const accessoryParent = bone('AccessoryParent', root);
    const accessoryLeaf = bone('AccessoryLeaf', accessoryParent);
    const lightBone = bone('LightBone', root);
    const unused = bone('UnusedControl', root);
    const bones = [root, weighted, accessoryParent, accessoryLeaf, lightBone, unused];

    const skinnedMesh = {
        name: 'Body',
        isMesh: true,
        isSkinnedMesh: true,
        skeleton: { bones },
        geometry: {
            attributes: {
                skinIndex: attribute([1, 0, 0, 0]),
                skinWeight: attribute([1, 0, 0, 0]),
            },
        },
    };
    const rigidAccessory = {
        name: 'RigidAccessory',
        isMesh: true,
        parent: accessoryLeaf,
    };
    const attachedLight = {
        name: 'AttachedLight',
        isLight: true,
        parent: lightBone,
    };
    const nodes = [...bones, skinnedMesh, rigidAccessory, attachedLight];
    const model = { traverse(callback) { nodes.forEach(callback); } };
    const clip = {
        name: 'Walk',
        tracks: bones.map((item) => ({ name: `${item.name}.quaternion` })),
        clone() { return { ...this, tracks: [...this.tracks], clone: this.clone }; },
    };

    const result = optimizeAnimationClipsForViewer([clip], model, {
        parseTrackName(name) {
            const [nodeName, propertyName] = name.split('.');
            return { nodeName, propertyName };
        },
    });

    assert.equal(result.reliable, true);
    assert.deepEqual(result.clips[0].tracks.map((track) => track.name), [
        'Root.quaternion',
        'Weighted.quaternion',
        'AccessoryParent.quaternion',
        'AccessoryLeaf.quaternion',
        'LightBone.quaternion',
    ]);
    assert.equal(result.droppedTracks, 1);
});

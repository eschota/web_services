import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../animation-playlist-controller.js', import.meta.url), 'utf8');
const playlist = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

class ScalarVector {
    constructor(y) {
        this.y = y;
    }

    clone() {
        return new ScalarVector(this.y);
    }

    copy(other) {
        this.y = other.y;
        return this;
    }
}

test('clip switches restore the refreshed grounded root snapshot', () => {
    const root = {
        position: new ScalarVector(8),
        quaternion: new ScalarVector(0),
        scale: new ScalarVector(1),
        updateMatrixWorld() {},
    };
    let snapshot = playlist.captureRootTransform(root);
    root.position.y = 0;
    snapshot = playlist.captureRootTransform(root);

    const played = [];
    const controller = new playlist.AnimationPlaylistController({
        restoreRoot: () => playlist.restoreRootTransform(root, snapshot),
        playClip: (name) => {
            played.push(name);
            return { name };
        },
    });
    controller.configure({
        clips: [
            { name: 'idle', duration: 1 },
            { name: 'walk', duration: 1 },
        ],
    });

    root.position.y = 12;
    assert.equal(controller.startAutoplay('idle'), true);
    assert.equal(root.position.y, 0);

    root.position.y = 5;
    assert.equal(controller.manualPlay('walk'), true);
    assert.equal(root.position.y, 0);
    assert.deepEqual(played, ['idle', 'walk']);
});

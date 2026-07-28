import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

async function importSource(relativePath) {
    const source = await readFile(new URL(relativePath, import.meta.url), 'utf8');
    return import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
}

const performanceHelpers = await importSource('../task-viewer-performance.js');

function texture(width = 4096, height = 4096) {
    const calls = { offset: 0, repeat: 0 };
    return {
        image: { width, height },
        offset: { set() { calls.offset += 1; } },
        repeat: { set() { calls.repeat += 1; } },
        userData: {},
        calls,
        version: 7,
    };
}

test('backdrop cover mutates UV transforms only when image or viewport aspect changes', () => {
    const tex = texture();
    assert.equal(performanceHelpers.updateBackdropCover(tex, 16 / 9), true);
    assert.equal(performanceHelpers.updateBackdropCover(tex, 16 / 9), false);
    assert.deepEqual(tex.calls, { offset: 1, repeat: 1 });
    assert.equal(tex.version, 7);

    assert.equal(performanceHelpers.updateBackdropCover(tex, 4 / 3), true);
    assert.deepEqual(tex.calls, { offset: 2, repeat: 2 });
    assert.equal(tex.version, 7);
});

test('rail cadence is time based for every viewer profile', () => {
    assert.equal(Math.round(performanceHelpers.secondaryViewportIntervalMs({ secondaryViewportFps: 30 })), 33);
    assert.equal(Math.round(performanceHelpers.secondaryViewportIntervalMs({ secondaryViewportFps: 15 })), 67);
    assert.equal(performanceHelpers.secondaryViewportIntervalMs({ secondaryViewportFps: 5 }), 200);
    assert.equal(performanceHelpers.secondaryViewportIntervalMs({ secondaryViewportFps: 2 }), 500);
});

test('software WebGL renderer detection covers SwiftShader and leaves hardware GPUs alone', () => {
    assert.equal(performanceHelpers.detectSoftwareWebGL({ renderer: 'ANGLE (Google, Vulkan 1.3 SwiftShader)' }).software, true);
    assert.equal(performanceHelpers.detectSoftwareWebGL({ renderer: 'llvmpipe (LLVM 17.0)' }).software, true);
    assert.equal(performanceHelpers.detectSoftwareWebGL({ renderer: 'ANGLE (NVIDIA GeForce RTX 4070)' }).software, false);
});

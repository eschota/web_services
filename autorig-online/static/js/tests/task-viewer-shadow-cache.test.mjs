import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

async function importSource(relativePath) {
    const source = await readFile(new URL(relativePath, import.meta.url), 'utf8');
    return import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
}

const performanceHelpers = await importSource('../task-viewer-performance.js');

function rootWithMatrix() {
    return {
        matrixWorld: { elements: [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1] },
        updateMatrixWorld() {},
    };
}

test('shadow bounds are reused while the caster root transform is unchanged', () => {
    const cache = new WeakMap();
    const root = rootWithMatrix();
    let computes = 0;
    const compute = () => ({ id: ++computes });
    const first = performanceHelpers.cachedRootBounds(cache, root, compute);
    const second = performanceHelpers.cachedRootBounds(cache, root, compute);
    assert.equal(first.cached, false);
    assert.equal(second.cached, true);
    assert.equal(second.value, first.value);
    assert.equal(computes, 1);
});

test('shadow bounds are recomputed after the caster root transform changes', () => {
    const cache = new WeakMap();
    const root = rootWithMatrix();
    let computes = 0;
    const compute = () => ({ id: ++computes });
    performanceHelpers.cachedRootBounds(cache, root, compute);
    root.matrixWorld.elements[12] = 2;
    const changed = performanceHelpers.cachedRootBounds(cache, root, compute);
    assert.equal(changed.cached, false);
    assert.equal(changed.value.id, 2);
});

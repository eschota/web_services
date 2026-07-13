import assert from 'node:assert/strict';
import test from 'node:test';

import {
    activateSplitViewport,
    normalizeSplitViewportState,
    splitViewportAtPoint,
    splitViewportNdc,
    splitViewportRects,
} from '../task-split-viewer.js';

test('default layout is Perspective plus Top, Front and Left rail', () => {
    const state = normalizeSplitViewportState();
    assert.deepEqual(state, {
        mainView: 'perspective',
        railViews: ['top', 'front', 'left'],
        maximizedView: null,
    });
    assert.deepEqual(splitViewportRects(state, 1000, 600), [
        { id: 'perspective', x: 0, y: 0, width: 720, height: 600, role: 'main' },
        { id: 'top', x: 720, y: 0, width: 280, height: 200, role: 'rail' },
        { id: 'front', x: 720, y: 200, width: 280, height: 200, role: 'rail' },
        { id: 'left', x: 720, y: 400, width: 280, height: 200, role: 'rail' },
    ]);
});

test('small viewport maximizes and swaps into the main slot when restored', () => {
    const maximized = activateSplitViewport(normalizeSplitViewportState(), 'top');
    assert.equal(maximized.mainView, 'top');
    assert.deepEqual(maximized.railViews, ['perspective', 'front', 'left']);
    assert.equal(maximized.maximizedView, 'top');
    assert.deepEqual(splitViewportRects(maximized, 1000, 600), [
        { id: 'top', x: 0, y: 0, width: 1000, height: 600, role: 'maximized' },
    ]);

    const restored = activateSplitViewport(maximized, 'top');
    assert.equal(restored.mainView, 'top');
    assert.deepEqual(restored.railViews, ['perspective', 'front', 'left']);
    assert.equal(restored.maximizedView, null);
});

test('clicking the main viewport maximizes without changing slot order', () => {
    const initial = normalizeSplitViewportState();
    const maximized = activateSplitViewport(initial, 'perspective');
    assert.equal(maximized.maximizedView, 'perspective');
    assert.equal(maximized.mainView, 'perspective');
    assert.deepEqual(maximized.railViews, ['top', 'front', 'left']);
});

test('hit testing and NDC use the local viewport rectangle', () => {
    const rects = splitViewportRects(normalizeSplitViewportState(), 1000, 600);
    const front = splitViewportAtPoint(rects, 850, 250);
    assert.equal(front.id, 'front');
    assert.deepEqual(splitViewportNdc(front, 860, 300), { x: 0, y: 0 });
});

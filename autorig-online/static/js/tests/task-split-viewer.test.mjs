import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../task-split-viewer.js', import.meta.url), 'utf8');
const split = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);

test('normalizes to perspective plus two secondary views', () => {
    const state = split.normalizeSplitViewportState({ railViews: ['top', 'front', 'left'] });
    assert.deepEqual(state, { mainView: 'perspective', railViews: ['top', 'front'], maximizedView: null });
});

test('returns exactly three non-overlapping viewport rectangles', () => {
    const rects = split.splitViewportRects({}, 1000, 600);
    assert.equal(rects.length, 3);
    assert.deepEqual(rects.map((rect) => rect.id), ['perspective', 'top', 'front']);
    assert.equal(rects[1].height + rects[2].height, 600);
    assert.equal(rects[0].width + rects[1].width, 1000);
});

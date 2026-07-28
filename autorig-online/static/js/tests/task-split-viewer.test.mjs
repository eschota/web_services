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

test('anchors viewer mode controls to the bottom center of the main viewport', () => {
    const rects = split.splitViewportRects({}, 1000, 600);
    assert.deepEqual(split.splitViewportControlAnchor(rects, 1000, 600), {
        viewId: 'perspective',
        centerX: 360,
        bottom: 12,
        width: 720,
    });

    const maximized = split.splitViewportRects({ maximizedView: 'front' }, 1000, 600);
    assert.deepEqual(split.splitViewportControlAnchor(maximized, 1000, 600), {
        viewId: 'front',
        centerX: 500,
        bottom: 12,
        width: 1000,
    });
});

test('computes a stable ground transform without mutating invalid bounds', () => {
    assert.deepEqual(split.viewerGroundTransform({
        min: { x: -1, y: -2, z: -3 },
        max: { x: 3, y: 6, z: 5 },
    }), { x: -1, y: 2, z: -1 });
    assert.deepEqual(split.viewerGroundTransform({
        min: { x: -1, y: -2, z: -3 },
        max: { x: 3, y: 6, z: 5 },
    }, false), { x: 0, y: 2, z: 0 });
    assert.equal(split.viewerGroundTransform({ min: {}, max: {} }), null);
});

test('keeps all projected top/front bounds inside the padded frustum', () => {
    const size = { x: 2, y: 4, z: 1 };
    const aspect = 280 / 300;
    const padding = 1.9;
    const tanVertical = Math.tan(45 * Math.PI / 360);
    const tanHorizontal = tanVertical * aspect;

    for (const viewId of ['top', 'front']) {
        const distance = split.viewerPerspectiveFitDistance(size, aspect, 45, viewId, padding);
        const horizontalSpan = size.x;
        const verticalSpan = viewId === 'top' ? size.z : size.y;
        const depthSpan = viewId === 'top' ? size.y : size.z;
        const closestDepth = distance - depthSpan / 2;
        assert.ok(closestDepth > 0);
        assert.ok(
            (horizontalSpan / 2) / (closestDepth * tanHorizontal) <= 1 / padding + 1e-9,
            `${viewId} horizontal bounds must fit`,
        );
        assert.ok(
            (verticalSpan / 2) / (closestDepth * tanVertical) <= 1 / padding + 1e-9,
            `${viewId} vertical bounds must fit`,
        );
    }

    const perspectiveAspect = 720 / 600;
    const perspectivePadding = 1.35;
    const perspectiveDistance = split.viewerPerspectiveFitDistance(size, perspectiveAspect, 45, 'perspective', perspectivePadding);
    const radius = Math.hypot(size.x, size.y, size.z) / 2;
    const limitingHalfFov = Math.min(
        Math.atan(tanVertical),
        Math.atan(tanVertical * perspectiveAspect),
    );
    assert.ok(
        radius / (perspectiveDistance * Math.sin(limitingHalfFov)) <= 1 / perspectivePadding + 1e-9,
        'perspective bounding sphere must fit',
    );
});

test('normalizes render-target viewport dimensions across DPR values', () => {
    for (const dpr of [0.4, 1, 1.25, 1.5, 2]) {
        const logicalWidth = 280;
        const logicalHeight = 300;
        const targetWidth = Math.round(logicalWidth * dpr);
        const targetHeight = Math.round(logicalHeight * dpr);
        const viewport = split.logicalRenderTargetViewportSize(targetWidth, targetHeight, dpr);
        assert.ok(Math.abs(viewport.width * dpr - targetWidth) < 0.001);
        assert.ok(Math.abs(viewport.height * dpr - targetHeight) < 0.001);
    }
});

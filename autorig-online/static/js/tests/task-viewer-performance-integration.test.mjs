import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

test('main viewer uses cached rail render targets without preserving the default framebuffer', () => {
    assert.match(taskHtml, /preserveDrawingBuffer:\s*false/);
    assert.match(taskHtml, /function ensureSplitViewportRailTarget/);
    assert.match(taskHtml, /function blitSplitViewportRailTarget/);
    assert.match(taskHtml, /renderSplitViewportRect\(rect,\s*false,\s*target\)/);
    assert.match(taskHtml, /blitSplitViewportRailTarget\(rect,\s*target\)/);
});

test('only the perspective viewport may refresh the shadow map', () => {
    assert.match(taskHtml, /const isPrimaryView = rect\.id === 'perspective'/);
    assert.match(taskHtml, /SPLIT_VIEWPORT_SHADOW_REFRESH_MS = 200/);
    assert.match(taskHtml, /renderer\.shadowMap\.autoUpdate = false/);
});

test('viewer exposes quality, p95 timing, and the localized software WebGL warning', () => {
    assert.match(taskHtml, /FPS · \$\{qualityBadge\} · \$\{p95Label\}/);
    assert.match(taskHtml, /viewer-gpu-hint/);
    assert.match(taskHtml, /viewer_software_webgl_hint/);
    assert.match(taskHtml, /mainViewerSoftwareWebGL = gpuInfo\.software/);
});

test('emergency quality lowers internal resolution enough for software WebGL', () => {
    assert.match(
        taskHtml,
        /emergency:\s*\{[\s\S]*?pixelRatioCap:\s*0\.4[\s\S]*?secondaryViewportFps:\s*2/,
    );
    assert.match(
        taskHtml,
        /return Math\.max\(0\.4,\s*Math\.min\(dpr,\s*getMainViewerQualityProfile\(\)\.pixelRatioCap\)\)/,
    );
});

test('animation clips are optimized before every viewer mixer path', () => {
    assert.match(taskHtml, /optimizeViewerAnimationClips\(gltf\.animations \|\| \[\], model, `\$\{label\} GLB`\)/);
    assert.match(taskHtml, /optimizeViewerAnimationClips\(model\.animations \|\| \[\], model, `\$\{label\} FBX`\)/);
    assert.match(taskHtml, /optimizeViewerAnimationClips\(clips, currentModel, 'apply clips'\)/);
});

test('backdrop cover updates are delegated to the cached UV helper', () => {
    const functionStart = taskHtml.indexOf('function updateScreenBackdropCover');
    const functionEnd = taskHtml.indexOf('\n            function ', functionStart + 1);
    const functionBody = taskHtml.slice(functionStart, functionEnd);
    assert.match(functionBody, /updateBackdropCover\(tex, viewAspect\)/);
    assert.doesNotMatch(functionBody, /needsUpdate/);
});

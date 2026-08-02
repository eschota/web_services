import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

test('rail render targets convert physical pixels back to logical viewport units', () => {
    assert.match(source, /logicalRenderTargetViewportSize\(\s*renderTarget\.width,\s*renderTarget\.height,\s*renderer\.getPixelRatio\(\)/s);
    assert.match(source, /renderer\.setViewport\(0,\s*0,\s*logicalViewport\.width,\s*logicalViewport\.height\)/);
    assert.doesNotMatch(source, /renderer\.setViewport\(0,\s*0,\s*renderTarget\.width,\s*renderTarget\.height\)/);
});

test('animated preview is precisely grounded and reframed after the mixer is ready', () => {
    const glbMixerIndex = source.indexOf('mixer = animations.length ? new THREE.AnimationMixer(model) : null;');
    const glbFramingIndex = source.indexOf('snapCurrentModelToGround({ centerXZ: true, force: true, precise: true });', glbMixerIndex);
    const glbSnapshotIndex = source.indexOf('animationPlaylistRootSnapshots.set(framingModel, captureRootTransform(framingModel));', glbFramingIndex);
    const glbFitIndex = source.indexOf('fitCameraToModelBounds(framingModel, `animated pose ${label}`', glbFramingIndex);
    assert.ok(glbMixerIndex >= 0);
    assert.ok(glbFramingIndex > glbMixerIndex);
    assert.ok(glbSnapshotIndex > glbFramingIndex);
    assert.ok(glbFitIndex > glbFramingIndex);
    assert.match(source, /autorigViewerPreciseBoundsInfo\s*=\s*animatedBoundsInfo/);
    assert.match(source, /fitSplitAuxiliaryCameras\(currentModel,\s*cachedBoundsInfo\)/);
    assert.doesNotMatch(source, /fitCameraToModelBounds\(currentModel,\s*'split viewport layout'/);
    assert.match(source, /updateShadowCatcherPlaneForModel\(currentModel,\s*'perspective',\s*false,\s*groundedBox\)/);
});

test('animation cache swaps restore grounded root and reuse precise framing bounds', () => {
    assert.match(source, /cacheKey === 'animations'[\s\S]*animationPlaylistRootSnapshots\.get\(currentModel\)[\s\S]*autorigViewerPreciseBoundsInfo/);
    assert.match(source, /restoreRootTransform\(currentModel,\s*groundedSnapshot\)/);
    assert.match(source, /cachedBoundsInfo\s*=\s*preciseBoundsInfo/);
});

test('mode controls are bottom-centered and animal-only restrictions stay visible', () => {
    assert.match(source, /id="viewer-rigtype-wrap" class="viewer-mode-dock" data-split-viewport-no-activate/);
    assert.match(source, /\.viewer-mode-dock\s*\{[\s\S]*bottom:\s*0\.75rem;[\s\S]*transform:\s*translateX\(-50%\);[\s\S]*z-index:\s*40;/);
    assert.match(source, /splitViewportControlAnchor\([\s\S]*modeDock\.style\.left[\s\S]*modeDock\.style\.bottom/);
    assert.match(source, /taskIsAnimalForPlayMode\(taskState\)[\s\S]*allowed:\s*false,\s*hidden:\s*false,/);
    assert.match(source, /button\.classList\.toggle\('is-disabled',\s*!allowed\)/);
    assert.match(source, /button\.removeAttribute\('disabled'\)/);
    assert.match(source, /\.animation-rail:not\(\.is-collapsed\)\s*\{\s*bottom:\s*3\.75rem;/);
    assert.match(source, /playModeBtn\?\.addEventListener\('click'/);
    assert.match(source, /ragdollModeBtn\?\.addEventListener\('click'/);
});

test('animation rail stays interactive on narrow screens and keeps accessibility state synchronized', () => {
    assert.doesNotMatch(source, /#model-viewer-wrap,\s*#custom-animations-wrap,\s*#blueprint-viewer-card/);
    assert.doesNotMatch(source, /#custom-animations-wrap,\s*#blueprint-viewer-card\s*\{\s*position:\s*static\s*!important/);
    assert.match(source, /\.animation-rail\s*\{[\s\S]*position:\s*absolute;[\s\S]*z-index:\s*35;/);
    assert.match(source, /const setAnimationRailCollapsed = \(collapsed\) => \{/);
    assert.match(source, /collapseBtn\.setAttribute\('aria-expanded',\s*String\(!collapsed\)\)/);
    assert.match(source, /aria-expanded="true"/);
});

test('fullscreen keeps the viewer overlay and controls in the fullscreen tree', () => {
    assert.match(source, /const targetElement = document\.getElementById\('model-viewer-container'\) \|\| host;/);
    assert.doesNotMatch(source, /const targetElement = host;/);
    assert.match(source, /#model-viewer-container:fullscreen,[\s\S]*height:\s*100vh\s*!important;/);
});

test('perspective uses its actual split rect and auxiliary views use projected axes', () => {
    assert.match(source, /const perspectiveRect = splitViewportController\?\.getRect\?\.\('perspective'\)/);
    assert.match(source, /viewerPerspectiveFitDistance\(\s*boundsInfo\.size,\s*aspect,\s*viewCamera\.fov,\s*viewId,\s*1\.9/s);
    assert.match(source, /viewerPerspectiveFitDistance\(size,\s*camera\.aspect,\s*nextFov,\s*'perspective',\s*padding\)/);
});

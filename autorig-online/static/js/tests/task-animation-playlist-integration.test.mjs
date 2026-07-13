import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');
const splitController = await readFile(new URL('../task-split-viewer.js', import.meta.url), 'utf8');

test('task viewer loads one shared manifest-driven playlist controller', () => {
    assert.match(taskHtml, /from '\/static\/js\/animation-playlist-controller\.js\?v=1'/);
    assert.equal((taskHtml.match(/new AnimationPlaylistController\(/g) || []).length, 1);
    assert.match(taskHtml, /\/api\/task\/\$\{encodeURIComponent\(taskId\)\}\/animation-manifest/);
    assert.match(taskHtml, /configureAnimationPlaylistForCurrentModel\(\);/);
    assert.doesNotMatch(splitController, /AnimationMixer/);
});

test('all model and clip replacement paths reconfigure the shared playlist', () => {
    const configureCalls = taskHtml.match(/configureAnimationPlaylistForCurrentModel\(/g) || [];
    // Declaration + applyAnimationClips + cache swap + GLTF + FBX.
    assert.ok(configureCalls.length >= 5, `expected at least five integration points, got ${configureCalls.length}`);
    assert.match(taskHtml, /animationPlaylist\.manualPlay\(sel\.value\)/);
    assert.match(taskHtml, /animationPlaylist\.manualPause\(\)/);
});

test('viewer interaction is sticky manual mode while visibility only suspends', () => {
    for (const reason of ['viewer-pointer', 'viewer-touch', 'viewer-wheel', 'viewer-keyboard', 'viewer-mode', 'orbit-controls']) {
        assert.match(taskHtml, new RegExp(reason));
    }
    assert.match(taskHtml, /if \(document\.hidden\) animationPlaylist\.suspend\(\)/);
    assert.match(taskHtml, /else animationPlaylist\.resume\(\)/);
});

test('catalog has no independent 5.2 second animation timer', () => {
    assert.doesNotMatch(taskHtml, /\},\s*5200\s*\)/);
    assert.match(taskHtml, /Catalog cards only mirror its/);
    assert.match(taskHtml, /syncAnimationPlaylistClip/);
});

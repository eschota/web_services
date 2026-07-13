import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
    animationPreviewCandidates,
    preferredMovingClip,
    shouldApplyCatalogPreview,
    shouldLoadExternalFbxPreview,
} from '../animation-preview-policy.js';

const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

test('prefers the first moving clip over a zero-duration base pose', () => {
    const basePose = { name: 'Horse_default', duration: 1 / 24 };
    const gallop = { name: 'Horse_gallop', duration: 0.75 };
    assert.equal(preferredMovingClip([basePose, gallop]), gallop);
});

test('falls back to the first clip when the asset only contains poses', () => {
    const basePose = { name: 'Horse_default', duration: 0 };
    assert.equal(preferredMovingClip([basePose]), basePose);
    assert.equal(preferredMovingClip([]), null);
});

test('builds stable unique embedded-clip candidates', () => {
    assert.deepEqual(animationPreviewCandidates({
        action_name: 'Horse_gallop',
        name: 'Horse Gallop',
        id: 'horse_gallop',
        file_name: 'Horse_gallop',
    }), ['Horse_gallop', 'Horse Gallop']);
});

test('does not auto-apply catalog previews over the canonical viewer asset', () => {
    assert.equal(shouldApplyCatalogPreview({ automatic: true }), false);
    assert.equal(shouldApplyCatalogPreview({ automatic: false }), true);
});

test('never applies flat-baked FBX clips to a hierarchical animal GLB', () => {
    assert.equal(shouldLoadExternalFbxPreview({
        previewUrl: '/animations/horse.fbx',
        isAnimalTask: true,
    }), false);
});

test('keeps explicit external FBX fallback for non-animal tasks only', () => {
    assert.equal(shouldLoadExternalFbxPreview({
        previewUrl: '/animations/walk.fbx',
        isAnimalTask: false,
        embeddedMatched: false,
        automatic: false,
    }), true);
    assert.equal(shouldLoadExternalFbxPreview({
        previewUrl: '/animations/walk.fbx',
        isAnimalTask: false,
        embeddedMatched: true,
        automatic: false,
    }), false);
    assert.equal(shouldLoadExternalFbxPreview({
        previewUrl: '/animations/walk.fbx',
        isAnimalTask: false,
        embeddedMatched: false,
        automatic: true,
    }), false);
});

test('centralizes cloned controls in the shared playlist integration', () => {
    const cloneSites = taskHtml.match(/sel = newSel;/g) || [];
    assert.equal(cloneSites.length, 1);
    assert.match(taskHtml, /if \(preferredEntry\) sel\.value = preferredEntry\.name;/);
    assert.match(taskHtml, /configureAnimationPlaylistForCurrentModel\(\);/);
    assert.doesNotMatch(taskHtml, /Setting up animation UI controls/);
});

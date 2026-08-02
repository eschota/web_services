import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';


const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '../../task.html'), 'utf8');

test('task viewer uses bounded PBR lighting controls and runtime intensity', () => {
    assert.match(source, /from '\/static\/js\/viewer-pbr-safety\.js\?v=1'/);
    assert.match(source, /from '\/static\/js\/rig-editor\.js\?v=69'/);
    assert.match(source, /id="light-main-slider" min="0" max="3\.5"/);
    assert.match(source, /id="light-env-slider" min="0" max="2"/);
    assert.match(source, /id="light-reflect-slider" min="0" max="4"/);
    assert.match(source, /safeViewerMaterialEnvironmentIntensity\(/);
});

test('automatic theme and model reload paths preserve loaded task lighting', () => {
    assert.match(source, /let viewerTaskLightingLoaded = false;/);
    assert.match(source, /viewerTaskLightingLoaded = true;/);
    assert.match(
        source,
        /applyViewerTheme\(match, reason, \{ preserveLighting: viewerTaskLightingLoaded \}\)/,
    );
    assert.match(
        source,
        /viewerThemeRuntimeApply\(activeViewerTheme, `model load \$\{label\}`, \{ preserveLighting: viewerTaskLightingLoaded \}\)/,
    );
    assert.match(source, /const preserveLighting = options\.preserveLighting === true;/);
});

test('authored PBR exits before textureless fallback and keeps every texture slot untouched', () => {
    const materialStart = source.indexOf('function applyImprovedMaterials(model)');
    const materialEnd = source.indexOf('const LOAD_MODEL_DEFAULT_TIMEOUT_MS', materialStart);
    const materialSource = source.slice(materialStart, materialEnd);
    assert.ok(materialStart >= 0 && materialEnd > materialStart);
    const authoredExit = materialSource.indexOf('if (hasTexture) return;');
    const texturelessSanitize = materialSource.indexOf('sanitizeViewerPbrMaterial(mat);');
    assert.ok(authoredExit >= 0);
    assert.ok(texturelessSanitize > authoredExit);
    assert.match(materialSource, /sanitizeViewerPbrMaterial\(mat\)/);
    assert.doesNotMatch(materialSource, /mat\.(?:map|normalMap|metalnessMap|roughnessMap)\s*=/);
});

test('shared default skinned materials use a visible metallic fallback without synthetic AO', () => {
    assert.match(source, /TEXTURELESS_SKINNED_FALLBACK_COLOR = new THREE\.Color\(0xa9cfe3\)/);
    assert.match(source, /meshes\.size >= 2 && isDefaultColor/);
    assert.match(source, /mat\.metalness = TEXTURELESS_SKINNED_FALLBACK_METALNESS/);
    assert.match(source, /mat\.roughness = TEXTURELESS_SKINNED_FALLBACK_ROUGHNESS/);
    assert.match(source, /detachSyntheticAOFromFallbackMaterials\(model, sharedDefaultSkinnedMaterials\)/);
    assert.match(source, /material\.aoMap = null/);
    assert.match(source, /key !== 'aoMap' \|\| texture !== syntheticAOTexture/);
});

test('GLB candidate is prepared before the working model is removed', () => {
    const loadStart = source.indexOf('async function loadModel(');
    const loadEnd = source.indexOf('// Load FBX model with animations', loadStart);
    const loadSource = source.slice(loadStart, loadEnd);
    assert.ok(loadSource.indexOf('prepareVertexPbrForViewer(model, `load ${label}`') >= 0);
    assert.ok(loadSource.indexOf('prepareVertexPbrForViewer(model, `load ${label}`') < loadSource.indexOf('scene.remove(currentModel)'));
});

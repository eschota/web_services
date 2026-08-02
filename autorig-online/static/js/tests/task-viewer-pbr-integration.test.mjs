import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';


const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.resolve(here, '../../task.html'), 'utf8');

test('task viewer uses bounded PBR lighting controls and runtime intensity', () => {
    assert.match(source, /from '\/static\/js\/viewer-pbr-safety\.js\?v=1'/);
    assert.match(source, /from '\/static\/js\/rig-editor\.js\?v=68'/);
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

test('Unity parity specular cap runs without remapping texture slots', () => {
    const materialStart = source.indexOf('function applyImprovedMaterials(model)');
    const materialEnd = source.indexOf('const LOAD_MODEL_DEFAULT_TIMEOUT_MS', materialStart);
    const materialSource = source.slice(materialStart, materialEnd);
    assert.ok(materialStart >= 0 && materialEnd > materialStart);
    assert.match(materialSource, /sanitizeViewerPbrMaterial\(mat\)/);
    assert.doesNotMatch(materialSource, /mat\.(?:map|normalMap|metalnessMap|roughnessMap)\s*=/);
});

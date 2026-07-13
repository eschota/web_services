import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

test('task viewer imports the appearance transfer helper and current Vertex PBR cache key', () => {
    assert.match(taskHtml, /from '\/static\/js\/appearance-material-transfer\.js\?v=1'/);
    assert.match(taskHtml, /from '\/static\/js\/vertex-pbr-material\.js\?v=3'/);
});

test('GLTF loaders pass asset extras into the Vertex PBR capability gate', () => {
    assert.match(
        taskHtml,
        /prepareVertexPbrForViewer\(model, `load \$\{label\}`, gltf\?\.parser\?\.json\?\.asset \|\| null\)/,
    );
    assert.match(taskHtml, /FBX prepared\.glb appearance donor/);
    assert.match(taskHtml, /gltf\?\.parser\?\.json\?\.asset \|\| null/);
});

test('canonical animation FBX is fully hydrated before it can replace the current model', () => {
    const start = taskHtml.indexOf('async function loadFBX(');
    const end = taskHtml.indexOf('window.TaskVariantPreviewBridge', start);
    const loadFbx = taskHtml.slice(start, end);
    const hydration = loadFbx.indexOf('await hydrateCanonicalAnimationFbxAppearance(model, label)');
    const sceneInsertion = loadFbx.indexOf('scene.add(model)');

    assert.ok(start >= 0 && end > start, 'loadFBX block must exist');
    assert.ok(hydration >= 0, 'canonical FBX hydration hook must exist');
    assert.ok(sceneInsertion > hydration, 'hydration must finish before scene insertion');
    assert.match(loadFbx, /isCanonicalTaskAnimationFbxUrl\(fbxUrl\)/);
    assert.match(loadFbx, /Refusing unhydrated animation FBX/);
});

test('transfer is fail-closed and retains a serializable viewer diagnostic report', () => {
    assert.match(taskHtml, /model\.userData\.autorigAppearanceTransferReport = report/);
    assert.match(taskHtml, /if \(!report\.complete\)/);
    assert.match(taskHtml, /report\.ambiguous\.length/);
    assert.match(taskHtml, /Vertex PBR donor requires FBX COLOR_0 vec4/);
    assert.match(taskHtml, /Vertex PBR donor requires FBX TEXCOORD_1/);
});

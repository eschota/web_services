import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

import {
    transferAppearanceMaterials,
} from '../appearance-material-transfer.js';
import {
    TransformManager,
    ViewerControls,
} from '../rig-editor.js';

function material(name) {
    return {
        name,
        needsUpdate: false,
        clone() {
            return material(`${name}-clone`);
        },
    };
}

function mesh(name, vertexCount, meshMaterial) {
    return {
        isMesh: true,
        name,
        material: meshMaterial,
        geometry: {
            attributes: {
                position: { count: vertexCount },
            },
            groups: [],
        },
    };
}

function root(meshes) {
    return {
        traverse(callback) {
            meshes.forEach(callback);
        },
    };
}

test('shared material post-processing is marked before the first shader compile', () => {
    const controls = Object.create(ViewerControls.prototype);
    const shared = {
        name: 'shared',
        type: 'MeshStandardMaterial',
        userData: {},
        onBeforeCompile: null,
    };

    controls.injectPostProcessing(shared);
    const firstCompiler = shared.onBeforeCompile;
    assert.equal(shared._postProcInjected, true);
    assert.equal(typeof firstCompiler, 'function');

    controls.injectPostProcessing(shared);
    assert.equal(shared.onBeforeCompile, firstCompiler);
});

test('ambiguous appearance meshes keep their FBX materials while unique meshes hydrate', () => {
    const originalA = material('fbx-a');
    const originalB = material('fbx-b');
    const originalUnique = material('fbx-unique');
    const donorA = material('donor-a');
    const donorB = material('donor-b');
    const donorUnique = material('donor-unique');
    const targetA = mesh('Body.001', 10, originalA);
    const targetB = mesh('Body.002', 10, originalB);
    const targetUnique = mesh('Head', 20, originalUnique);

    const report = transferAppearanceMaterials(
        root([targetA, targetB, targetUnique]),
        root([
            mesh('Body.101', 10, donorA),
            mesh('Body.102', 10, donorB),
            mesh('Head', 20, donorUnique),
        ]),
        { abortOnAmbiguous: false },
    );

    assert.equal(report.aborted, false);
    assert.equal(report.ambiguous.length, 2);
    assert.equal(report.transferredMeshCount, 1);
    assert.equal(targetA.material, originalA);
    assert.equal(targetB.material, originalB);
    assert.notEqual(targetUnique.material, originalUnique);
    assert.equal(targetUnique.material.name, 'donor-unique-clone');
});

test('disabled transform interaction cannot recreate selection boxes on model swap or Q', () => {
    const calls = {
        enable: 0,
        disable: 0,
        deselect: 0,
        hide: 0,
    };
    const manager = new TransformManager({
        scene: {},
        camera: {},
        renderer: { domElement: { style: {} } },
        controls: { enabled: false },
    });
    manager.selectionSystem = {
        enableSelectionMode() { calls.enable += 1; },
        disableSelectionMode() { calls.disable += 1; },
        deselectAll() { calls.deselect += 1; },
        update() {},
        getSelected() { return null; },
    };
    manager.gizmoLoader = {
        hideAllGizmos() { calls.hide += 1; },
    };

    manager.setInteractionEnabled(false);
    manager.setModel({ name: 'animation model' });
    assert.equal(manager.handleKeyDown('q'), false);
    assert.equal(calls.enable, 0);
    assert.ok(calls.disable >= 2);
    assert.equal(manager.controls.enabled, true);

    manager.setInteractionEnabled(true);
    assert.equal(calls.enable, 1);
});

test('task viewer keeps valid canonical FBX playable and disables editor overlays in preview modes', async () => {
    const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');
    assert.match(taskHtml, /transformManager\?\.setInteractionEnabled\?\.\(visible\)/);
    assert.match(taskHtml, /abortOnAmbiguous:\s*false/);
    assert.match(taskHtml, /Using original animation FBX appearance/);
    assert.doesNotMatch(taskHtml, /Refusing unhydrated animation FBX/);
    assert.match(taskHtml, /Refusing invalid animation FBX/);
    assert.match(taskHtml, /animation-preview-v4/);
    assert.doesNotMatch(taskHtml, /animation-preview-v3/);
});

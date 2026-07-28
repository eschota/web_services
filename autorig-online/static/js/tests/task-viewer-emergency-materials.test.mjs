import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const taskHtml = await readFile(new URL('../../task.html', import.meta.url), 'utf8');

function emergencyMaterialController() {
    const start = taskHtml.indexOf('function restoreMainViewerEmergencyMaterials');
    const end = taskHtml.indexOf('function prepareMainViewerWebGLContext', start);
    assert.ok(start >= 0 && end > start, 'Emergency material helpers must be extractable');
    const helpers = taskHtml.slice(start, end);

    class MeshBasicMaterial {
        constructor(params) {
            Object.assign(this, params);
            this.userData = {};
            this.disposeCount = 0;
        }

        dispose() {
            this.disposeCount += 1;
        }
    }

    const THREE = {
        MeshBasicMaterial,
        Color: class Color {
            constructor(value) {
                this.value = value;
            }
        },
    };
    const build = new Function(
        'THREE',
        'isSecsVertexPbrMaterial',
        `
            let mainViewerEmergencyMaterialCache = new WeakMap();
            let mainViewerEmergencyMaterialRecords = [];
            const mainViewerEmergencyMaterials = new Set();
            let mainViewerQualityMode = 'emergency';
            let currentModel = null;
            ${helpers}
            return {
                sync: syncMainViewerEmergencyMaterials,
                restore: restoreMainViewerEmergencyMaterials,
            };
        `,
    );
    return {
        MeshBasicMaterial,
        ...build(THREE, () => false),
    };
}

function pbrMaterial() {
    return {
        type: 'MeshPhysicalMaterial',
        isMeshStandardMaterial: true,
        isMeshPhysicalMaterial: true,
        color: { clone: () => ({ clonedColor: true }) },
        map: { id: 'base-color' },
        alphaMap: { id: 'unused-opaque-alpha' },
        normalMap: { id: 'normal' },
        roughnessMap: { id: 'roughness' },
        metalnessMap: { id: 'metalness' },
        aoMap: { id: 'ao' },
        envMap: { id: 'environment' },
        transparent: false,
        opacity: 1,
        alphaTest: 0,
        side: 2,
        depthTest: true,
        depthWrite: true,
        vertexColors: false,
        wireframe: false,
        blending: 1,
        premultipliedAlpha: false,
        colorWrite: true,
        polygonOffset: false,
        polygonOffsetFactor: 0,
        polygonOffsetUnits: 0,
        clippingPlanes: null,
        clipIntersection: false,
        clipShadows: false,
        userData: { source: true },
    };
}

function rootFor(...meshes) {
    return {
        traverse(callback) {
            meshes.forEach(callback);
        },
    };
}

test('Emergency proxies share materials, preserve arrays, restore identities, and dispose once', () => {
    const controller = emergencyMaterialController();
    const shared = pbrMaterial();
    const unsupported = { type: 'ShaderMaterial', isShaderMaterial: true };
    const skeleton = { id: 'skeleton' };
    const morphAttributes = { position: [{ id: 'morph' }] };
    const first = {
        isMesh: true,
        isSkinnedMesh: true,
        material: shared,
        skeleton,
        geometry: { morphAttributes },
    };
    const originalArray = [shared, unsupported];
    const second = {
        isMesh: true,
        material: originalArray,
        skeleton,
        geometry: { morphAttributes },
    };
    const third = { isMesh: true, material: shared };
    const rootA = rootFor(first, second);
    const rootB = rootFor(third);

    controller.sync(rootA);
    const proxy = first.material;
    assert.ok(proxy instanceof controller.MeshBasicMaterial);
    assert.equal(second.material[0], proxy, 'shared source material must reuse one proxy');
    assert.equal(second.material[1], unsupported, 'unsupported shader material must remain exact');
    assert.equal(proxy.map, shared.map);
    assert.equal(proxy.alphaMap, null, 'opaque material does not sample an unused alpha map');
    assert.equal(proxy.normalMap, undefined);
    assert.equal(proxy.roughnessMap, undefined);
    assert.equal(proxy.metalnessMap, undefined);
    assert.equal(proxy.aoMap, undefined);
    assert.equal(proxy.envMap, undefined);
    assert.equal(first.skeleton, skeleton);
    assert.equal(first.geometry.morphAttributes, morphAttributes);

    controller.sync(rootB);
    assert.equal(third.material, proxy, 'shared proxy must also be reused across active roots');
    controller.restore(rootA);
    assert.equal(first.material, shared);
    assert.equal(second.material, originalArray, 'material-array identity must restore exactly');
    assert.equal(third.material, proxy, 'other active root must be retained');
    assert.equal(proxy.disposeCount, 0, 'retained shared proxy must not be disposed early');

    controller.restore(rootB);
    assert.equal(third.material, shared);
    assert.equal(proxy.disposeCount, 1, 'shared proxy must be disposed exactly once');

    controller.sync(rootA);
    const replacementAfterReentry = first.material;
    assert.notEqual(replacementAfterReentry, proxy, 're-entry must not reuse a disposed proxy');
    controller.restore();
    assert.equal(replacementAfterReentry.disposeCount, 1);
    assert.equal(first.material, shared);
    assert.equal(second.material, originalArray);
});

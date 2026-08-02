import assert from 'node:assert/strict';
import test from 'node:test';

import {
    MaterialChannel,
    ViewerControls,
    materialHasAuthoredTextureMaps,
    materialNeedsSyntheticAO,
    modelNeedsSyntheticAO,
} from '../rig-editor.js';


function mesh(material) {
    return { isMesh: true, material };
}

function model(children) {
    return {
        traverse(callback) {
            children.forEach(callback);
        },
    };
}

function pbrMaterial(overrides = {}) {
    return {
        isMeshStandardMaterial: true,
        isMeshPhysicalMaterial: false,
        userData: {},
        ...overrides,
    };
}

function viewerHarness() {
    const controls = Object.create(ViewerControls.prototype);
    Object.assign(controls, {
        originalMaterials: new Map(),
        bakedAOTexture: null,
        channelUniforms: { debugMode: { value: 0 } },
        currentRotationPreset: 'none',
        modelFlipped: false,
        materialChannel: MaterialChannel.PBR,
        injectPostProcessing() {},
        alignModelToGround() {},
        applyMaterialChannel() {},
    });
    return controls;
}

test('author PBR texture slots make synthetic AO ineligible', () => {
    const baseColor = { id: 'base-color' };
    const normal = { id: 'normal' };
    const metallicRoughness = { id: 'metallic-roughness' };
    const material = pbrMaterial({
        map: baseColor,
        normalMap: normal,
        roughnessMap: metallicRoughness,
        metalnessMap: metallicRoughness,
    });

    assert.equal(materialHasAuthoredTextureMaps(material), true);
    assert.equal(materialNeedsSyntheticAO(material), false);
    assert.equal(modelNeedsSyntheticAO(model([mesh(material)])), false);
});

test('ViewerControls skips AO bake and preserves authored texture slots', () => {
    const baseColor = { id: 'base-color' };
    const normal = { id: 'normal' };
    const metallicRoughness = { id: 'metallic-roughness' };
    const material = pbrMaterial({
        map: baseColor,
        normalMap: normal,
        roughnessMap: metallicRoughness,
        metalnessMap: metallicRoughness,
    });
    const controls = viewerHarness();
    let bakeCalls = 0;
    controls.bakeAOForModel = () => { bakeCalls += 1; };

    controls.setModel(model([mesh(material)]));

    assert.equal(bakeCalls, 0);
    assert.equal(material.aoMap, undefined);
    assert.equal(material.map, baseColor);
    assert.equal(material.normalMap, normal);
    assert.equal(material.roughnessMap, metallicRoughness);
    assert.equal(material.metalnessMap, metallicRoughness);
});

test('textureless fallback still receives the synthetic AO texture', () => {
    const material = pbrMaterial();
    const controls = viewerHarness();
    const bakedAO = { id: 'synthetic-ao' };
    let bakeCalls = 0;
    controls.bakeAOForModel = () => {
        bakeCalls += 1;
        controls.bakedAOTexture = bakedAO;
    };

    controls.setModel(model([mesh(material)]));

    assert.equal(bakeCalls, 1);
    assert.equal(material.aoMap, bakedAO);
    assert.equal(material.aoMapIntensity, 1);
});

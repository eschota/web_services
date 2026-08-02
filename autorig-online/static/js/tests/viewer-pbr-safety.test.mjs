import assert from 'node:assert/strict';
import test from 'node:test';

import {
    VIEWER_PBR_LIGHTING_LIMITS,
    safeViewerMaterialEnvironmentIntensity,
    sanitizeViewerLightingValues,
    sanitizeViewerPbrMaterial,
} from '../viewer-pbr-safety.js';

test('viewer lighting values are finite and bounded', () => {
    assert.deepEqual(VIEWER_PBR_LIGHTING_LIMITS, {
        environment: { min: 0, max: 2 },
        reflection: { min: 0, max: 4 },
        sun: { min: 0, max: 3.5 },
        materialEnvironment: { min: 0, max: 4 },
    });
    assert.deepEqual(
        sanitizeViewerLightingValues({
            environmentIntensity: 10,
            reflectionIntensity: Number.POSITIVE_INFINITY,
            sunIntensity: -1,
        }),
        { environmentIntensity: 2, reflectionIntensity: 3, sunIntensity: 0 },
    );
});

test('effective material environment intensity never exceeds four', () => {
    assert.equal(safeViewerMaterialEnvironmentIntensity(2, 4, 1), 4);
    assert.equal(safeViewerMaterialEnvironmentIntensity(1, 3, 1), 3);
    assert.equal(safeViewerMaterialEnvironmentIntensity(-2, 4, 1), 0);
});

test('Unity parity policy clamps only KHR specular strength and color', () => {
    const baseColorMap = { id: 'base' };
    const normalMap = { id: 'normal' };
    const metalnessMap = { id: 'metallic-roughness' };
    const material = {
        map: baseColorMap,
        normalMap,
        metalnessMap,
        specularColor: { r: 2, g: 0.5, b: -0.25 },
        specularIntensity: 3,
        needsUpdate: false,
    };

    assert.equal(sanitizeViewerPbrMaterial(material), 2);
    assert.deepEqual(material.specularColor, { r: 1, g: 0.5, b: 0 });
    assert.equal(material.specularIntensity, 1);
    assert.equal(material.map, baseColorMap);
    assert.equal(material.normalMap, normalMap);
    assert.equal(material.metalnessMap, metalnessMap);
    assert.equal(material.needsUpdate, true);
});

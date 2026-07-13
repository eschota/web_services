import assert from 'node:assert/strict';
import test from 'node:test';

import {
    cloneAppearanceMaterial,
    normalizeAppearanceMeshName,
    transferAppearanceMaterials,
} from '../appearance-material-transfer.js';

function material(name, values = {}) {
    const instance = {
        name,
        isMeshStandardMaterial: true,
        ...values,
    };
    instance.clone = () => ({ ...instance, clonedFrom: instance });
    return instance;
}

function mesh(name, vertexCount, meshMaterial, attributes = {}) {
    return {
        isMesh: true,
        name,
        material: meshMaterial,
        geometry: {
            groups: [],
            attributes: {
                position: { count: vertexCount, itemSize: 3 },
                ...attributes,
            },
        },
    };
}

function root(...meshes) {
    return {
        traverse(callback) {
            callback(this);
            meshes.forEach(callback);
        },
    };
}

test('normalizes FBX namespaces and Blender duplicate suffixes', () => {
    assert.equal(normalizeAppearanceMeshName('Model::textured_mesh.obj.002'), 'texturedmeshobj');
    assert.equal(normalizeAppearanceMeshName('textured_meshobj002'), 'texturedmeshobj');
    assert.equal(normalizeAppearanceMeshName('Armature|Horse-Coat_001'), 'horsecoat');
    assert.equal(normalizeAppearanceMeshName('  GrEy Coat  '), 'greycoat');
});

test('adapts cloned GLB textures to FBX UV orientation without losing PBR metadata', () => {
    const sharedMetalRough = {
        colorSpace: '',
        flipY: false,
        channel: 0,
        clone() {
            return { ...this, sourceTexture: this };
        },
    };
    const baseColor = {
        colorSpace: 'srgb',
        flipY: false,
        channel: 0,
        clone() {
            return { ...this, sourceTexture: this };
        },
    };
    const donor = material('PBR_Material', {
        map: baseColor,
        metalnessMap: sharedMetalRough,
        roughnessMap: sharedMetalRough,
        transparent: true,
        opacity: 0.4,
        alphaTest: 0.25,
    });

    const cloned = cloneAppearanceMaterial(donor, { textureFlipY: true });

    assert.notEqual(cloned, donor);
    assert.notEqual(cloned.map, baseColor);
    assert.equal(cloned.map.sourceTexture, baseColor);
    assert.equal(cloned.map.colorSpace, 'srgb');
    assert.equal(cloned.map.flipY, true);
    assert.equal(cloned.map.needsUpdate, true);
    assert.equal(cloned.metalnessMap, cloned.roughnessMap);
    assert.equal(cloned.metalnessMap.sourceTexture, sharedMetalRough);
    assert.equal(sharedMetalRough.flipY, false);
    assert.equal(cloned.transparent, true);
    assert.equal(cloned.opacity, 0.4);
    assert.equal(cloned.alphaTest, 0.25);
});

test('uses exact name and vertex count and preserves native PBR appearance fields', () => {
    const baseColor = { colorSpace: 'srgb', flipY: false, channel: 0 };
    const metalRough = { colorSpace: '', flipY: false, channel: 0 };
    const donorMaterial = material('PBR_Material', {
        map: baseColor,
        metalnessMap: metalRough,
        roughnessMap: metalRough,
        transparent: true,
        opacity: 0.72,
        alphaTest: 0.31,
        depthWrite: false,
    });
    const target = mesh('Horse', 24000, material('black-fbx'), { uv: { count: 24000, itemSize: 2 } });
    const donor = mesh('Horse', 24000, donorMaterial, { uv: { count: 24000, itemSize: 2 } });

    const report = transferAppearanceMaterials(root(target), root(donor));

    assert.equal(report.complete, true);
    assert.equal(report.transferred[0].method, 'exact-name-vertex-count');
    assert.notEqual(target.material, donorMaterial);
    assert.equal(target.material.clonedFrom, donorMaterial);
    assert.equal(target.material.map, baseColor);
    assert.equal(target.material.map.colorSpace, 'srgb');
    assert.equal(target.material.metalnessMap, metalRough);
    assert.equal(target.material.roughnessMap, metalRough);
    assert.equal(target.material.transparent, true);
    assert.equal(target.material.opacity, 0.72);
    assert.equal(target.material.alphaTest, 0.31);
    assert.equal(target.material.depthWrite, false);
    assert.equal(target.material.needsUpdate, true);
});

test('matches the live Three.js Arabian name drift even when topology counts differ', () => {
    const target = mesh('textured_meshobj002', 120000, material('black-fbx'), {
        uv: { count: 120000, itemSize: 2 },
    });
    const donorMaterial = material('PBR_Material', {
        map: { colorSpace: 'srgb', channel: 0 },
        metalnessMap: { channel: 0 },
        roughnessMap: { channel: 0 },
    });
    const donor = mesh('textured_meshobj', 119970, donorMaterial, {
        uv: { count: 119970, itemSize: 2 },
    });

    const report = transferAppearanceMaterials(root(target), root(donor));

    assert.equal(report.complete, true);
    assert.equal(report.transferred[0].method, 'normalized-name');
    assert.equal(target.material.map.colorSpace, 'srgb');
});

test('falls back to a vertex count only when it is unique on both sides', () => {
    const target = mesh('FBXBody', 321, material('target'));
    const donor = mesh('GLBBody', 321, material('donor'));
    const report = transferAppearanceMaterials(root(target), root(donor));
    assert.equal(report.complete, true);
    assert.equal(report.transferred[0].method, 'unique-vertex-count');
});

test('changes only material assignment and leaves FBX geometry, skin and animations authoritative', () => {
    const targetGeometry = {
        groups: [],
        attributes: { position: { count: 10, itemSize: 3 } },
    };
    const targetSkeleton = { id: 'fbx-skeleton' };
    const targetAnimations = [{ name: 'Walk' }];
    const target = mesh('Horse', 10, material('target'));
    target.geometry = targetGeometry;
    target.skeleton = targetSkeleton;
    const targetRoot = root(target);
    targetRoot.animations = targetAnimations;

    const donor = mesh('Horse', 10, material('donor'));
    donor.geometry.sourceOnly = true;
    donor.skeleton = { id: 'glb-skeleton' };
    const donorRoot = root(donor);
    donorRoot.animations = [{ name: 'DonorPose' }];

    const report = transferAppearanceMaterials(targetRoot, donorRoot);

    assert.equal(report.complete, true);
    assert.equal(target.geometry, targetGeometry);
    assert.equal(target.skeleton, targetSkeleton);
    assert.equal(targetRoot.animations, targetAnimations);
    assert.deepEqual(targetRoot.animations.map((clip) => clip.name), ['Walk']);
});

test('aborts atomically on an ambiguous normalized name', () => {
    const firstTargetMaterial = material('first-target');
    const secondTargetMaterial = material('second-target');
    const targetA = mesh('Horse.003', 30, firstTargetMaterial);
    const targetB = mesh('Cube', 8, secondTargetMaterial);
    const donorA = mesh('Horse.001', 10, material('horse-a'));
    const donorB = mesh('Horse.002', 20, material('horse-b'));
    const donorCube = mesh('Cube', 8, material('cube'));

    const report = transferAppearanceMaterials(root(targetA, targetB), root(donorA, donorB, donorCube));

    assert.equal(report.aborted, true);
    assert.equal(report.abortedReason, 'ambiguous-mesh-match');
    assert.equal(report.transferredMeshCount, 0);
    assert.equal(targetA.material, firstTargetMaterial);
    assert.equal(targetB.material, secondTargetMaterial);
});

test('rejects a textured source when the FBX target has no required UV channel', () => {
    const targetMaterial = material('target');
    const target = mesh('Horse', 10, targetMaterial);
    const donor = mesh('Horse', 10, material('donor', { map: { channel: 0 } }), {
        uv: { count: 10, itemSize: 2 },
    });

    const report = transferAppearanceMaterials(root(target), root(donor));

    assert.equal(report.complete, false);
    assert.equal(report.transferredMeshCount, 0);
    assert.match(report.incompatible[0].reason, /requires (?:full-length )?uv/);
    assert.equal(target.material, targetMaterial);
});

test('fails closed when material slots or target group indices cannot preserve assignment', () => {
    const targetSlotA = material('target-a');
    const targetSlotB = material('target-b');
    const sourceSlot = material('source');
    const slotMismatchTarget = mesh('Horse', 10, [targetSlotA, targetSlotB]);
    const source = mesh('Horse', 10, sourceSlot);

    const slotReport = transferAppearanceMaterials(root(slotMismatchTarget), root(source));
    assert.equal(slotReport.transferredMeshCount, 0);
    assert.match(slotReport.incompatible[0].reason, /material slot count differs/);
    assert.deepEqual(slotMismatchTarget.material, [targetSlotA, targetSlotB]);

    const invalidGroupTarget = mesh('Horse', 10, targetSlotA);
    invalidGroupTarget.geometry.groups = [{ start: 0, count: 3, materialIndex: 1 }];
    const groupReport = transferAppearanceMaterials(root(invalidGroupTarget), root(source));
    assert.equal(groupReport.transferredMeshCount, 0);
    assert.match(groupReport.incompatible[0].reason, /invalid materialIndex 1/);
    assert.equal(invalidGroupTarget.material, targetSlotA);
});

test('requires UV and vertex-color attributes to cover every target position', () => {
    const targetMaterial = material('target');
    const shortUvTarget = mesh('Horse', 10, targetMaterial, {
        uv: { count: 9, itemSize: 2 },
    });
    const texturedSource = mesh('Horse', 10, material('source', { map: { channel: 0 } }), {
        uv: { count: 10, itemSize: 2 },
    });
    const uvReport = transferAppearanceMaterials(root(shortUvTarget), root(texturedSource));
    assert.equal(uvReport.transferredMeshCount, 0);
    assert.match(uvReport.incompatible[0].reason, /full-length uv/);

    const shortColorTarget = mesh('Horse', 10, targetMaterial, {
        color: { count: 9, itemSize: 4 },
    });
    const vertexColorSource = mesh('Horse', 10, material('source', { vertexColors: true }), {
        color: { count: 10, itemSize: 4 },
    });
    const colorReport = transferAppearanceMaterials(root(shortColorTarget), root(vertexColorSource));
    assert.equal(colorReport.transferredMeshCount, 0);
    assert.match(colorReport.incompatible[0].reason, /full-length COLOR_0/);
});

test('lets the caller enforce the Vertex PBR COLOR_0 and TEXCOORD_1 contract', () => {
    const target = mesh('Horse', 10, material('target'), {
        color: { count: 10, itemSize: 4 },
    });
    const donor = mesh('Horse', 10, material('vertex-pbr'));

    const report = transferAppearanceMaterials(root(target), root(donor), {
        validateMatch: ({ targetMesh }) => targetMesh.geometry.attributes.uv1
            ? true
            : { compatible: false, reason: 'Vertex PBR requires TEXCOORD_1' },
    });

    assert.equal(report.complete, false);
    assert.equal(report.transferredMeshCount, 0);
    assert.match(report.incompatible[0].reason, /TEXCOORD_1/);
});

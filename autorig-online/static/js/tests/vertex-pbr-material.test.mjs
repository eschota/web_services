import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const moduleSource = await readFile(new URL('../vertex-pbr-material.js', import.meta.url), 'utf8');
const vertexPbr = await import(`data:text/javascript;base64,${Buffer.from(moduleSource).toString('base64')}`);

let materialId = 0;

class FakeVector2 {
    constructor(x = 1, y = 1) {
        this.x = x;
        this.y = y;
    }

    copy(other) {
        this.x = other.x;
        this.y = other.y;
        return this;
    }
}

class FakeMeshStandardMaterial {
    constructor(params = {}) {
        Object.assign(this, params);
        this.uuid = `material-${++materialId}`;
        this.name = '';
        this.userData = {};
        this.normalScale = new FakeVector2();
        this.isMeshStandardMaterial = true;
        this.type = 'MeshStandardMaterial';
        this.onBeforeCompile = () => {};
    }

    customProgramCacheKey() {
        return this.onBeforeCompile.toString();
    }
}

const THREE = {
    MeshStandardMaterial: FakeMeshStandardMaterial,
};

function attribute(itemSize) {
    return { itemSize, count: 3 };
}

function makeSourceMaterial(overrides = {}) {
    return {
        uuid: `source-${++materialId}`,
        name: 'SourceMaterial',
        userData: {},
        side: 2,
        depthTest: true,
        depthWrite: true,
        envMapIntensity: 1,
        normalScale: new FakeVector2(0.8, 0.7),
        ...overrides,
    };
}

function makeModel({ marker = 'material', attributes = {}, material = null } = {}) {
    const sourceMaterial = material || makeSourceMaterial();
    if (marker === 'material') {
        sourceMaterial.userData.secsVertexPbrProfile = vertexPbr.SECS_VERTEX_PBR_PROFILE;
    }
    const mesh = {
        isMesh: true,
        name: 'CharacterMesh',
        uuid: 'mesh-1',
        userData: marker === 'mesh' ? { secsVertexPbrProfile: vertexPbr.SECS_VERTEX_PBR_PROFILE } : {},
        geometry: {
            attributes: {
                position: attribute(3),
                normal: attribute(3),
                color: attribute(4),
                uv1: attribute(2),
                ...attributes,
            },
        },
        material: sourceMaterial,
        parent: null,
    };
    const model = {
        name: 'Root',
        userData: marker === 'root' ? { secsVertexPbrProfile: vertexPbr.SECS_VERTEX_PBR_PROFILE } : {},
        parent: null,
        children: [mesh],
        traverse(callback) {
            callback(this);
            callback(mesh);
        },
    };
    mesh.parent = model;
    return { model, mesh, sourceMaterial };
}

function fakeShader() {
    return {
        vertexShader: `
#include <common>
void main() {
    #include <color_vertex>
}`,
        fragmentShader: `
#include <common>
void main() {
    vec4 diffuseColor = vec4(1.0);
    #include <color_fragment>
    #include <roughnessmap_fragment>
    #include <metalnessmap_fragment>
    vec3 outgoingLight = vec3(1.0);
    #include <opaque_fragment>
}`,
    };
}

test('structurally detects unmarked COLOR_0 and TEXCOORD_1 without texture maps', () => {
    const { model, mesh, sourceMaterial } = makeModel({ marker: null });
    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    assert.equal(report.profile, vertexPbr.SECS_VERTEX_PBR_PROFILE);
    assert.equal(report.detection, 'structural');
    assert.notEqual(mesh.material, sourceMaterial);
});

test('preserves legacy textured PBR materials without COLOR_0 and TEXCOORD_1', () => {
    const baseColorTexture = { id: 'base-color' };
    const metallicRoughnessTexture = { id: 'metallic-roughness' };
    const { model, mesh, sourceMaterial } = makeModel({
        marker: null,
        attributes: { color: undefined, uv1: undefined },
        material: makeSourceMaterial({
            map: baseColorTexture,
            metalnessMap: metallicRoughnessTexture,
            roughnessMap: metallicRoughnessTexture,
        }),
    });
    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    assert.equal(report.profile, null);
    assert.equal(report.detection, null);
    assert.equal(report.configuredMeshCount, 0);
    assert.equal(mesh.material, sourceMaterial);
    assert.equal(mesh.material.map, baseColorTexture);
    assert.equal(mesh.material.metalnessMap, metallicRoughnessTexture);
    assert.equal(mesh.material.roughnessMap, metallicRoughnessTexture);
});

test('legacy and physical texture maps veto structural detection', () => {
    for (const textureKey of ['map', 'iridescenceMap', 'anisotropyMap', 'envMap']) {
        const { model, mesh, sourceMaterial } = makeModel({
            marker: null,
            material: makeSourceMaterial({ [textureKey]: { id: textureKey } }),
        });
        const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
        assert.equal(report.profile, null, textureKey);
        assert.equal(mesh.material, sourceMaterial, textureKey);
    }
});

test('structural detection is all-or-nothing for a mixed legacy scene', () => {
    const structural = makeModel({ marker: null });
    const legacy = makeModel({
        marker: null,
        attributes: { color: undefined, uv1: undefined },
        material: makeSourceMaterial({ map: { id: 'legacy-base-color' } }),
    });
    structural.model.children.push(legacy.mesh);
    legacy.mesh.parent = structural.model;
    structural.model.traverse = (callback) => {
        callback(structural.model);
        structural.model.children.forEach(callback);
    };

    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, structural.model);
    assert.equal(report.profile, null);
    assert.equal(structural.mesh.material, structural.sourceMaterial);
    assert.equal(legacy.mesh.material, legacy.sourceMaterial);
});

test('accepts the versioned profile from glTF asset extras', () => {
    const { model, mesh, sourceMaterial } = makeModel({ marker: null });
    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model, {
        asset: { extras: { secsVertexPbr: vertexPbr.SECS_VERTEX_PBR_PROFILE } },
    });
    assert.equal(report.profile, vertexPbr.SECS_VERTEX_PBR_PROFILE);
    assert.equal(report.detection, 'declared');
    assert.notEqual(mesh.material, sourceMaterial);
});

test('fails closed when a declared root has no renderable meshes', () => {
    const model = {
        userData: { secsVertexPbrProfile: vertexPbr.SECS_VERTEX_PBR_PROFILE },
        parent: null,
        traverse(callback) {
            callback(this);
        },
    };
    assert.throws(
        () => vertexPbr.prepareSecsVertexPbrModel(THREE, model),
        /profile marker exists but no renderable meshes were found/,
    );
});

test('configures a clean MeshStandardMaterial for a valid profile', () => {
    const { model, mesh, sourceMaterial } = makeModel();
    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    assert.equal(report.valid, true);
    assert.equal(report.profiledMeshCount, 1);
    assert.equal(report.configuredMaterialCount, 1);
    assert.notEqual(mesh.material, sourceMaterial);
    assert.equal(mesh.material.vertexColors, true);
    assert.equal(mesh.material.metalness, 1);
    assert.equal(mesh.material.roughness, 1);
    assert.equal(mesh.material.transparent, false);
    assert.equal(mesh.material.userData.autorigVertexPbrProfile, vertexPbr.SECS_VERTEX_PBR_PROFILE);
});

test('validation is two-phase and leaves source materials untouched', () => {
    const { model, mesh, sourceMaterial } = makeModel({
        attributes: { color: undefined, uv1: undefined },
    });
    assert.throws(
        () => vertexPbr.prepareSecsVertexPbrModel(THREE, model),
        (error) => error instanceof vertexPbr.SecsVertexPbrValidationError &&
            error.report.errors.some((item) => item.includes('missing color')) &&
            error.report.errors.some((item) => item.includes('missing uv1')),
    );
    assert.equal(mesh.material, sourceMaterial);
});

test('rejects texture maps other than the optional normal map', () => {
    const { model } = makeModel({ material: makeSourceMaterial({ map: { id: 'base-color' } }) });
    model.children[0].material.userData.secsVertexPbrProfile = vertexPbr.SECS_VERTEX_PBR_PROFILE;
    assert.throws(
        () => vertexPbr.prepareSecsVertexPbrModel(THREE, model),
        /texture map is not allowed/,
    );
});

test('preserves an optional normal map only when it uses TEXCOORD_2', () => {
    const normalMap = { channel: 2 };
    const { model, mesh } = makeModel({
        material: makeSourceMaterial({
            normalMap,
            userData: { secsVertexPbrProfile: vertexPbr.SECS_VERTEX_PBR_PROFILE },
        }),
        attributes: { uv2: attribute(2) },
    });
    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    assert.equal(report.valid, true);
    assert.equal(mesh.material.normalMap, normalMap);
    assert.equal(mesh.material.normalScale.x, 0.8);
    assert.equal(mesh.material.normalScale.y, 0.7);

    const invalid = makeModel({
        material: makeSourceMaterial({
            normalMap: { channel: 0 },
            userData: { secsVertexPbrProfile: vertexPbr.SECS_VERTEX_PBR_PROFILE },
        }),
    });
    assert.throws(
        () => vertexPbr.prepareSecsVertexPbrModel(THREE, invalid.model),
        /normalMap must use TEXCOORD_2/,
    );
});

test('preparation is idempotent', () => {
    const { model, mesh } = makeModel({ marker: 'mesh' });
    vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    const configured = mesh.material;
    const second = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    assert.equal(mesh.material, configured);
    assert.equal(second.configuredMaterialCount, 0);
    assert.equal(second.configuredMeshCount, 1);
});

test('shader patch decodes base color, AO, metallic and roughness exactly once', () => {
    const shader = fakeShader();
    vertexPbr.patchSecsVertexPbrShader(shader);
    const firstVertex = shader.vertexShader;
    const firstFragment = shader.fragmentShader;

    assert.match(firstVertex, /vSecsMetalRough = uv1/);
    assert.match(firstFragment, /diffuseColor\.rgb \*= vColor\.rgb/);
    assert.match(firstFragment, /roughnessFactor = clamp\(vSecsMetalRough\.y/);
    assert.match(firstFragment, /metalnessFactor = clamp\(vSecsMetalRough\.x/);
    assert.match(firstFragment, /mix\(0\.46, 1\.0, clamp\(vColor\.a/);
    assert.match(firstFragment, /mix\(0\.46[\s\S]*#include <opaque_fragment>/);

    vertexPbr.patchSecsVertexPbrShader(shader);
    assert.equal(shader.vertexShader, firstVertex);
    assert.equal(shader.fragmentShader, firstFragment);
});

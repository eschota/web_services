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
    #include <output_fragment>
}`,
    };
}

test('does not structurally auto-detect unmarked COLOR_0 and TEXCOORD_1', () => {
    const { model, mesh, sourceMaterial } = makeModel({ marker: null });
    const report = vertexPbr.prepareSecsVertexPbrModel(THREE, model);
    assert.equal(report.profile, null);
    assert.equal(mesh.material, sourceMaterial);
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

    vertexPbr.patchSecsVertexPbrShader(shader);
    assert.equal(shader.vertexShader, firstVertex);
    assert.equal(shader.fragmentShader, firstFragment);
});

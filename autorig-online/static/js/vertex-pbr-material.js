export const SECS_VERTEX_PBR_PROFILE = 'secs.vertexPbr.v1';

const SHADER_REVISION = 'autorig-secs-vertex-pbr-r2';
const DISALLOWED_TEXTURE_KEYS = [
    'map',
    'aoMap',
    'roughnessMap',
    'metalnessMap',
    'emissiveMap',
    'alphaMap',
    'bumpMap',
    'displacementMap',
    'lightMap',
    'specularMap',
    'specularColorMap',
    'specularIntensityMap',
    'clearcoatMap',
    'clearcoatNormalMap',
    'clearcoatRoughnessMap',
    'sheenColorMap',
    'sheenRoughnessMap',
    'iridescenceMap',
    'iridescenceThicknessMap',
    'anisotropyMap',
    'transmissionMap',
    'thicknessMap',
    'envMap',
    'gradientMap',
    'matcap',
];

export class SecsVertexPbrValidationError extends Error {
    constructor(report) {
        const details = Array.isArray(report?.errors) && report.errors.length
            ? report.errors.join('; ')
            : 'unknown contract error';
        super(`Invalid ${SECS_VERTEX_PBR_PROFILE} asset: ${details}`);
        this.name = 'SecsVertexPbrValidationError';
        this.report = report;
    }
}

function profileFromUserData(value) {
    const markers = [
        value?.userData?.secsVertexPbrProfile,
        value?.userData?.secsVertexPbr,
        value?.userData?.asset?.extras?.secsVertexPbr,
        value?.userData?.gltfAsset?.extras?.secsVertexPbr,
    ];
    return markers.some((marker) => (
        marker === SECS_VERTEX_PBR_PROFILE ||
        marker?.profile === SECS_VERTEX_PBR_PROFILE
    )) ? SECS_VERTEX_PBR_PROFILE : null;
}

function profileFromAssetMetadata(asset) {
    const markers = [
        asset?.extras?.secsVertexPbr,
        asset?.extras?.secsVertexPbrProfile,
        asset?.asset?.extras?.secsVertexPbr,
        asset?.asset?.extras?.secsVertexPbrProfile,
        asset?.secsVertexPbr,
        asset?.secsVertexPbrProfile,
    ];
    return markers.some((marker) => (
        marker === SECS_VERTEX_PBR_PROFILE ||
        marker?.profile === SECS_VERTEX_PBR_PROFILE
    )) ? SECS_VERTEX_PBR_PROFILE : null;
}

function hasProfileInAncestry(object) {
    let current = object;
    while (current) {
        if (profileFromUserData(current)) return true;
        current = current.parent || null;
    }
    return false;
}

export function isSecsVertexPbrMaterial(material) {
    return !!material && (
        material.userData?.autorigVertexPbrProfile === SECS_VERTEX_PBR_PROFILE ||
        profileFromUserData(material) === SECS_VERTEX_PBR_PROFILE
    );
}

function meshUsesVertexPbrProfile(mesh, materials) {
    return hasProfileInAncestry(mesh) || materials.some((material) => isSecsVertexPbrMaterial(material));
}

function materialHasDisallowedTexture(material) {
    return DISALLOWED_TEXTURE_KEYS.some((key) => !!material?.[key]);
}

function attributeIssue(geometry, name, minItemSize, exactItemSize = null) {
    const attribute = geometry?.attributes?.[name];
    if (!attribute) return `missing ${name}`;
    if (exactItemSize !== null && Number(attribute.itemSize) !== exactItemSize) {
        return `${name} must have itemSize ${exactItemSize}`;
    }
    if (Number(attribute.itemSize) < minItemSize) {
        return `${name} must have itemSize >= ${minItemSize}`;
    }
    return null;
}

function meshLabel(mesh, fallbackIndex) {
    return String(mesh?.name || mesh?.uuid || `mesh-${fallbackIndex}`);
}

function collectRenderableMeshes(model) {
    const meshes = [];
    model?.traverse?.((object) => {
        if (!object?.isMesh || !object.material) return;
        const materials = (Array.isArray(object.material) ? object.material : [object.material]).filter(Boolean);
        if (!materials.length) return;
        meshes.push({ mesh: object, materials });
    });
    return meshes;
}

function hasRequiredStructuralLayout({ mesh, materials }) {
    const geometry = mesh?.geometry;
    if (attributeIssue(geometry, 'position', 3)) return false;
    if (attributeIssue(geometry, 'normal', 3)) return false;
    if (attributeIssue(geometry, 'color', 4, 4)) return false;
    if (attributeIssue(geometry, 'uv1', 2)) return false;
    if (materials.some((material) => materialHasDisallowedTexture(material))) return false;
    return materials.every((material) => {
        if (!material.normalMap) return true;
        return Number(material.normalMap.channel) === 2 && !attributeIssue(geometry, 'uv2', 2);
    });
}

export function detectSecsVertexPbrCapability(model, { asset = null } = {}) {
    const renderableMeshes = collectRenderableMeshes(model);
    const assetHasProfile = profileFromAssetMetadata(asset) === SECS_VERTEX_PBR_PROFILE ||
        profileFromAssetMetadata(model?.userData) === SECS_VERTEX_PBR_PROFILE;
    const rootHasProfile = hasProfileInAncestry(model);
    const declarationCoversModel = assetHasProfile || rootHasProfile;
    const declaredMeshes = renderableMeshes.filter(({ mesh, materials }) => (
        declarationCoversModel || meshUsesVertexPbrProfile(mesh, materials)
    ));

    if (declarationCoversModel || declaredMeshes.length > 0) {
        return {
            profile: SECS_VERTEX_PBR_PROFILE,
            detection: 'declared',
            renderableMeshes,
            profiledMeshes: declarationCoversModel ? renderableMeshes : declaredMeshes,
        };
    }

    const structurallyCompatible = renderableMeshes.length > 0 &&
        renderableMeshes.every((entry) => hasRequiredStructuralLayout(entry));
    return {
        profile: structurallyCompatible ? SECS_VERTEX_PBR_PROFILE : null,
        detection: structurallyCompatible ? 'structural' : null,
        renderableMeshes,
        profiledMeshes: structurallyCompatible ? renderableMeshes : [],
    };
}

function validateProfiledMeshes(profiledMeshes) {
    const errors = [];

    profiledMeshes.forEach(({ mesh, materials }, meshIndex) => {
        const label = meshLabel(mesh, meshIndex);
        const geometry = mesh.geometry;
        const issues = [
            attributeIssue(geometry, 'position', 3),
            attributeIssue(geometry, 'normal', 3),
            attributeIssue(geometry, 'color', 4, 4),
            attributeIssue(geometry, 'uv1', 2),
        ].filter(Boolean);
        issues.forEach((issue) => errors.push(`${label}: ${issue}`));

        materials.forEach((material, materialIndex) => {
            const materialLabel = String(material.name || `${label}/material-${materialIndex}`);
            DISALLOWED_TEXTURE_KEYS.forEach((key) => {
                if (material[key]) errors.push(`${materialLabel}: texture ${key} is not allowed`);
            });
            if (material.normalMap) {
                if (Number(material.normalMap.channel) !== 2) {
                    errors.push(`${materialLabel}: normalMap must use TEXCOORD_2`);
                }
                const uv2Issue = attributeIssue(geometry, 'uv2', 2);
                if (uv2Issue) errors.push(`${label}: optional normalMap requires TEXCOORD_2`);
            }
        });
    });

    return errors;
}

function replaceRequired(source, anchor, replacement, stage) {
    if (!source.includes(anchor)) {
        throw new Error(`${SECS_VERTEX_PBR_PROFILE} shader anchor missing: ${stage}`);
    }
    return source.replace(anchor, replacement);
}

export function patchSecsVertexPbrShader(shader) {
    if (!shader?.vertexShader || !shader?.fragmentShader) {
        throw new Error(`${SECS_VERTEX_PBR_PROFILE} requires a standard Three.js shader`);
    }
    if (shader.vertexShader.includes(SHADER_REVISION)) return shader;

    shader.vertexShader = replaceRequired(
        shader.vertexShader,
        '#include <common>',
        `#include <common>
// ${SHADER_REVISION}
varying vec2 vSecsMetalRough;
#ifndef USE_UV1
attribute vec2 uv1;
#endif`,
        'vertex common',
    );
    shader.vertexShader = replaceRequired(
        shader.vertexShader,
        '#include <color_vertex>',
        `#include <color_vertex>
vSecsMetalRough = uv1;`,
        'vertex attributes',
    );

    shader.fragmentShader = replaceRequired(
        shader.fragmentShader,
        '#include <common>',
        `#include <common>
// ${SHADER_REVISION}
varying vec2 vSecsMetalRough;`,
        'fragment common',
    );
    shader.fragmentShader = replaceRequired(
        shader.fragmentShader,
        '#include <color_fragment>',
        `#if defined( USE_COLOR_ALPHA )
    diffuseColor.rgb *= vColor.rgb;
#elif defined( USE_COLOR )
    diffuseColor.rgb *= vColor;
#endif`,
        'base color',
    );
    shader.fragmentShader = replaceRequired(
        shader.fragmentShader,
        '#include <roughnessmap_fragment>',
        `#include <roughnessmap_fragment>
roughnessFactor = clamp(vSecsMetalRough.y, 0.0, 1.0);`,
        'roughness',
    );
    shader.fragmentShader = replaceRequired(
        shader.fragmentShader,
        '#include <metalnessmap_fragment>',
        `#include <metalnessmap_fragment>
metalnessFactor = clamp(vSecsMetalRough.x, 0.0, 1.0);`,
        'metalness',
    );
    // Three.js r154+ renamed output_fragment to opaque_fragment. Apply AO to
    // outgoingLight immediately before the standard output stage so tone
    // mapping, color-space conversion and fog remain owned by Three.js.
    const outputAnchor = shader.fragmentShader.includes('#include <opaque_fragment>')
        ? '#include <opaque_fragment>'
        : '#include <output_fragment>';
    shader.fragmentShader = replaceRequired(
        shader.fragmentShader,
        outputAnchor,
        `outgoingLight *= mix(0.46, 1.0, clamp(vColor.a, 0.0, 1.0));
${outputAnchor}`,
        'ambient occlusion',
    );
    return shader;
}

function installShaderPatch(material) {
    if (material.userData?.autorigVertexPbrShaderInstalled) return;

    const previousOnBeforeCompile = material.onBeforeCompile;
    const previousProgramCacheKey = typeof material.customProgramCacheKey === 'function'
        ? material.customProgramCacheKey.bind(material)
        : null;

    material.onBeforeCompile = function onBeforeCompileSecsVertexPbr(shader, renderer) {
        if (typeof previousOnBeforeCompile === 'function') {
            previousOnBeforeCompile.call(this, shader, renderer);
        }
        patchSecsVertexPbrShader(shader);
        this.userData.autorigVertexPbrShader = shader;
    };
    material.customProgramCacheKey = function customProgramCacheKeySecsVertexPbr() {
        const previous = previousProgramCacheKey ? previousProgramCacheKey() : '';
        return `${previous}|${SHADER_REVISION}`;
    };
    material.userData.autorigVertexPbrShaderInstalled = true;
    material.needsUpdate = true;
}

function createRuntimeMaterial(THREE, sourceMaterial) {
    const runtimeMaterial = new THREE.MeshStandardMaterial({
        color: 0xffffff,
        metalness: 1,
        roughness: 1,
        vertexColors: true,
        normalMap: sourceMaterial.normalMap || null,
        side: sourceMaterial.side,
        depthTest: sourceMaterial.depthTest !== false,
        depthWrite: sourceMaterial.depthWrite !== false,
        transparent: false,
        opacity: 1,
        alphaTest: 0,
        envMapIntensity: Number.isFinite(sourceMaterial.envMapIntensity)
            ? sourceMaterial.envMapIntensity
            : 1,
    });
    runtimeMaterial.name = sourceMaterial.name || '';
    runtimeMaterial.flatShading = !!sourceMaterial.flatShading;
    if (sourceMaterial.normalScale && runtimeMaterial.normalScale?.copy) {
        runtimeMaterial.normalScale.copy(sourceMaterial.normalScale);
    }
    runtimeMaterial.userData = {
        ...(sourceMaterial.userData || {}),
        secsVertexPbrProfile: SECS_VERTEX_PBR_PROFILE,
        secsRuntimeShader: 'vertex-base-ao-uv1-metal-rough',
        autorigVertexPbrProfile: SECS_VERTEX_PBR_PROFILE,
        autorigVertexPbrConfigured: true,
    };
    installShaderPatch(runtimeMaterial);
    return runtimeMaterial;
}

export function prepareSecsVertexPbrModel(THREE, model, { throwOnError = true, asset = null } = {}) {
    if (!THREE?.MeshStandardMaterial) {
        throw new Error('Three.js MeshStandardMaterial is required for Vertex PBR');
    }
    if (!model?.traverse) {
        throw new Error('A traversable Three.js model is required for Vertex PBR');
    }

    const capability = detectSecsVertexPbrCapability(model, { asset });
    const profiledMeshes = capability.profiledMeshes;
    const rootHasProfile = capability.detection === 'declared' && (
        hasProfileInAncestry(model) ||
        profileFromAssetMetadata(asset) === SECS_VERTEX_PBR_PROFILE ||
        profileFromAssetMetadata(model?.userData) === SECS_VERTEX_PBR_PROFILE
    );
    const errors = validateProfiledMeshes(profiledMeshes);
    if (rootHasProfile && profiledMeshes.length === 0) {
        errors.push('profile marker exists but no renderable meshes were found');
    }

    const report = {
        profile: capability.profile,
        detection: capability.detection,
        profiledMeshCount: profiledMeshes.length,
        configuredMeshCount: 0,
        configuredMaterialCount: 0,
        valid: errors.length === 0,
        errors,
    };
    model.userData = model.userData || {};
    model.userData.autorigVertexPbrReport = report;

    if (!report.profile) return report;
    if (!report.valid) {
        if (throwOnError) throw new SecsVertexPbrValidationError(report);
        return report;
    }

    profiledMeshes.forEach(({ mesh, materials }) => {
        const nextMaterials = materials.map((sourceMaterial) => {
            if (sourceMaterial.userData?.autorigVertexPbrConfigured) {
                installShaderPatch(sourceMaterial);
                return sourceMaterial;
            }
            report.configuredMaterialCount += 1;
            return createRuntimeMaterial(THREE, sourceMaterial);
        });
        mesh.material = Array.isArray(mesh.material) ? nextMaterials : nextMaterials[0];
        report.configuredMeshCount += 1;
    });

    model.userData.autorigVertexPbrReport = report;
    return report;
}

#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';

import { validateImmutableInputs } from './browser_fit_canary.mjs';

const TOOL_FILE = fileURLToPath(import.meta.url);
const TOOL_DIRECTORY = path.dirname(TOOL_FILE);
const AUTORIG_ONLINE = path.resolve(TOOL_DIRECTORY, '..', '..');
const ADAPTER_FILE = path.join(AUTORIG_ONLINE, 'static', 'js', 'animation-fitting-three-adapter.js');
const AUTHOR_FILE = path.join(AUTORIG_ONLINE, 'static', 'js', 'animation-fitting-horse-swing-guide-author.js');
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const WIDTH = 768;
const HEIGHT = 448;
const GUIDE_FRAMES = Object.freeze([0, 6, 18, 30, 42, 48]);
const SWING_FRAMES = Object.freeze([6, 18, 30, 42]);
const RECOVERY_FRAMES = Object.freeze([12, 24, 36]);
const MIXED_V10_SCENE_CONTRACT = 'v10_reference_endpoints_browser_intermediates';
const UNIFIED_V11_SCENE_CONTRACT = 'v11_unified_browser_static_scene_v1';
const RECOVERY_V12_SCENE_CONTRACT = 'v12_unified_browser_recovery_guides_v1';
const HORSE_LIMB_ORDER = Object.freeze(['hind_left', 'fore_left', 'hind_right', 'fore_right']);

export const HORSE_V12_RECOVERY_GUIDE_PLAN = Object.freeze([
    Object.freeze({ frameIndex: 0, role: 'actionless_default_cycle_origin', swingLimb: null, strength: 0.8 }),
    Object.freeze({ frameIndex: 6, role: 'hind_left_single_hoof_swing_apex', swingLimb: 'hind_left', strength: 0.7 }),
    Object.freeze({ frameIndex: 12, role: 'four_hoof_recovery_after_hind_left', swingLimb: null, strength: 0.85 }),
    Object.freeze({ frameIndex: 18, role: 'fore_left_single_hoof_swing_apex', swingLimb: 'fore_left', strength: 0.7 }),
    Object.freeze({ frameIndex: 24, role: 'four_hoof_recovery_after_fore_left', swingLimb: null, strength: 0.85 }),
    Object.freeze({ frameIndex: 30, role: 'hind_right_single_hoof_swing_apex', swingLimb: 'hind_right', strength: 0.7 }),
    Object.freeze({ frameIndex: 36, role: 'four_hoof_recovery_after_hind_right', swingLimb: null, strength: 0.85 }),
    Object.freeze({ frameIndex: 42, role: 'fore_right_single_hoof_swing_apex', swingLimb: 'fore_right', strength: 0.7 }),
    Object.freeze({ frameIndex: 48, role: 'actionless_default_cycle_endpoint', swingLimb: null, strength: 0.8 }),
]);

export function buildHorseV12ContactCueVisibilityPlan() {
    return HORSE_V12_RECOVERY_GUIDE_PLAN.map((guide) => ({
        frameIndex: guide.frameIndex,
        visibleLimbs: HORSE_LIMB_ORDER.filter((limb) => limb !== guide.swingLimb),
        hiddenLimbs: guide.swingLimb ? [guide.swingLimb] : [],
        visibleCueCount: guide.swingLimb ? 3 : 4,
        hiddenCueCount: guide.swingLimb ? 1 : 0,
    }));
}

function fail(message) {
    throw new Error(message);
}

function nonEmpty(value, field) {
    const result = String(value || '').trim();
    if (!result) fail(`${field} is required`);
    return result;
}

function existingFile(value, field) {
    const result = path.resolve(nonEmpty(value, field));
    if (!fs.statSync(result, { throwIfNoEntry: false })?.isFile()) fail(`${field} is not a file: ${result}`);
    return result;
}

function existingDirectory(value, field) {
    const result = path.resolve(nonEmpty(value, field));
    if (!fs.statSync(result, { throwIfNoEntry: false })?.isDirectory()) fail(`${field} is not a directory: ${result}`);
    return result;
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function sha256File(filename) {
    return sha256Buffer(fs.readFileSync(filename));
}

function pinFile(filename, extra = {}) {
    const stats = fs.statSync(filename);
    return {
        filename: path.basename(filename),
        bytes: stats.size,
        sha256: sha256File(filename),
        ...extra,
    };
}

function readJson(filename, field) {
    try {
        return JSON.parse(fs.readFileSync(filename, 'utf8'));
    } catch (error) {
        fail(`${field} is not valid JSON: ${error.message}`);
    }
}

function readGzipJson(filename, field) {
    try {
        return JSON.parse(zlib.gunzipSync(fs.readFileSync(filename)).toString('utf8'));
    } catch (error) {
        fail(`${field} is not valid gzip JSON: ${error.message}`);
    }
}

function writeJson(filename, value) {
    const buffer = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
    fs.writeFileSync(filename, buffer, { flag: 'wx' });
}

function outputDirectory(value) {
    const result = path.resolve(nonEmpty(value, 'output'));
    if (fs.existsSync(result)) fail(`output must not already exist: ${result}`);
    return result;
}

function parseArguments(argv) {
    const result = {};
    for (let index = 0; index < argv.length; index += 1) {
        const argument = argv[index];
        if (argument === '--synthetic-smoke') {
            result.syntheticSmoke = true;
            continue;
        }
        if (!argument.startsWith('--')) fail(`unexpected argument: ${argument}`);
        const key = argument.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) fail(`${argument} requires a value`);
        result[key] = argv[++index];
    }
    return result;
}

function guideSceneContract(value) {
    const result = String(value || MIXED_V10_SCENE_CONTRACT).trim();
    if (
        result !== MIXED_V10_SCENE_CONTRACT
        && result !== UNIFIED_V11_SCENE_CONTRACT
        && result !== RECOVERY_V12_SCENE_CONTRACT
    ) {
        fail(
            `scene-contract must be ${MIXED_V10_SCENE_CONTRACT}, ${UNIFIED_V11_SCENE_CONTRACT}, or ${RECOVERY_V12_SCENE_CONTRACT}, got ${result}`,
        );
    }
    return result;
}

export function browserGuideSceneProfile(value) {
    const sceneContract = guideSceneContract(value);
    const recoveryGuides = sceneContract === RECOVERY_V12_SCENE_CONTRACT;
    return {
        sceneContract,
        unifiedBrowserScene: sceneContract !== MIXED_V10_SCENE_CONTRACT,
        recoveryGuides,
        guideFrames: recoveryGuides
            ? HORSE_V12_RECOVERY_GUIDE_PLAN.map((guide) => guide.frameIndex)
            : [...GUIDE_FRAMES],
        recoveryFrames: recoveryGuides ? [...RECOVERY_FRAMES] : [],
        deterministicContactCues: recoveryGuides,
        shadowsEnabled: false,
    };
}

function mime(filename) {
    if (filename.endsWith('.js')) return 'text/javascript; charset=utf-8';
    if (filename.endsWith('.json')) return 'application/json; charset=utf-8';
    if (filename.endsWith('.html')) return 'text/html; charset=utf-8';
    return 'application/octet-stream';
}

function harnessHtml() {
    return `<!doctype html>
<html><head><meta charset="utf-8"><style>
html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#717b86}canvas{display:block}
</style></head><body><script type="module">
import * as THREE from '/three.module.js';
import { buildHorse2BrowserFittingSkeleton, bakeFittedAnimationToThreeHierarchyClip, createViewerToLtxProjection } from '/adapter.js';
import { authorHorseV10SwingGuidePoses, verifyHorseV10PostBakeHoofProjections } from '/author.js';

const config = await (await fetch('/config.json', { cache: 'no-store' })).json();
const LIMB_ORDER = Object.freeze(['hind_left', 'fore_left', 'hind_right', 'fore_right']);
const V12_SCENE_CONTRACT = 'v12_unified_browser_recovery_guides_v1';
const STATIC_SCENE = Object.freeze({
    clearColorHex: 0x717b86,
    backgroundHex: 0x717b86,
    outputColorSpace: 'SRGBColorSpace',
    toneMapping: 'ACESFilmicToneMapping',
    toneMappingExposure: 1.1,
    shadowsEnabled: false,
    hemisphere: Object.freeze({ skyHex: 0xe9f1ff, groundHex: 0x3f4650, intensity: 2.1 }),
    key: Object.freeze({ colorHex: 0xffffff, intensity: 3.5, position: Object.freeze([4.5, -5.5, 8.5]) }),
    ground: Object.freeze({ colorHex: 0xb8c3cc, roughness: 0.92, metalness: 0, size: 50 }),
    contactCues: Object.freeze({
        enabled: config.sceneContract === V12_SCENE_CONTRACT,
        implementation: 'static_rest_hoof_radial_alpha_planes',
        colorHex: 0x53616b,
        opacity: 0.24,
        widthWorld: 0.22,
        lengthWorld: 0.38,
        featherInner: 0.18,
        featherOuter: 0.98,
        groundOffsetWorld: 0.002,
    }),
});

function matrix4(values, field) {
    if (!Array.isArray(values) || values.length !== 16 || values.some((value) => !Number.isFinite(Number(value)))) {
        throw new Error(field + ' must contain 16 finite numbers');
    }
    return new THREE.Matrix4().set(...values.map(Number));
}

function buildBundleModel(sourceSkeleton) {
    if (!Array.isArray(sourceSkeleton.armatures) || sourceSkeleton.armatures.length !== 1) {
        throw new Error('source skeleton must contain one armature');
    }
    const armature = sourceSkeleton.armatures[0];
    const model = new THREE.Group();
    model.name = 'AutoRig_Browser_Horse_Guide_Model';
    const armatureMatrix = matrix4(armature.matrix_world, 'armature.matrix_world');
    armatureMatrix.decompose(model.position, model.quaternion, model.scale);
    const bones = new Map();
    for (const source of armature.bones) {
        if (bones.has(source.name)) throw new Error('duplicate source bone ' + source.name);
        const bone = new THREE.Bone();
        bone.name = source.name;
        bone.userData.use_deform = source.use_deform === true;
        bone.userData.tailWorld = new THREE.Vector3(...source.tail_local).applyMatrix4(armatureMatrix).toArray();
        matrix4(source.parent ? source.parent_relative_matrix : source.matrix_local, source.name + '.local')
            .decompose(bone.position, bone.quaternion, bone.scale);
        bones.set(source.name, bone);
    }
    for (const source of armature.bones) {
        const bone = bones.get(source.name);
        if (source.parent) {
            const parent = bones.get(source.parent);
            if (!parent) throw new Error('missing parent ' + source.parent);
            parent.add(bone);
        } else {
            model.add(bone);
        }
    }
    model.updateWorldMatrix(true, true);
    let maximumHeadReconstructionErrorWorld = 0;
    for (const source of armature.bones) {
        const expected = new THREE.Vector3(...source.head_local).applyMatrix4(armatureMatrix);
        const actual = bones.get(source.name).getWorldPosition(new THREE.Vector3());
        maximumHeadReconstructionErrorWorld = Math.max(maximumHeadReconstructionErrorWorld, actual.distanceTo(expected));
    }
    return { model, bones, sourceBones: armature.bones, maximumHeadReconstructionErrorWorld };
}

function buildBundleCamera(contract) {
    const [width, height] = contract.resolution;
    const { fx, fy, cx, cy } = contract.intrinsics;
    const near = 0.01;
    const far = 1000;
    const camera = new THREE.PerspectiveCamera();
    camera.matrixAutoUpdate = false;
    camera.matrix.copy(matrix4(contract.camera_to_world, 'camera_to_world'));
    camera.matrixWorld.copy(camera.matrix);
    camera.matrixWorldInverse.copy(matrix4(contract.world_to_camera, 'world_to_camera'));
    camera.projectionMatrix.set(
        2 * fx / width, 0, 1 - 2 * cx / width, 0,
        0, 2 * fy / height, 2 * cy / height - 1, 0,
        0, 0, (far + near) / (near - far), 2 * far * near / (near - far),
        0, 0, -1, 0,
    );
    camera.projectionMatrixInverse.copy(camera.projectionMatrix).invert();
    camera.updateProjectionMatrix = () => {};
    camera.updateWorldMatrix(true, false);
    return camera;
}

function buildSkinnedMesh(modelState, weights, topology) {
    if (!Array.isArray(weights.vertices) || weights.vertices.length !== 344) {
        throw new Error('skin weights must contain exactly 344 Horse_2 vertices');
    }
    const vertices = [...weights.vertices].sort((a, b) => a.vertex_index - b.vertex_index);
    vertices.forEach((vertex, index) => {
        if (vertex.vertex_index !== index || vertex.vertex_id !== index) throw new Error('skin vertices must be dense and ordered');
    });
    const boneOrder = modelState.sourceBones.map((source) => modelState.bones.get(source.name));
    const boneIndex = new Map(boneOrder.map((bone, index) => [bone.name, index]));
    const positions = [];
    const skinIndices = [];
    const skinWeights = [];
    for (const vertex of vertices) {
        positions.push(...vertex.local.map(Number));
        const influences = vertex.weights
            .filter((entry) => Number(entry.weight) > 0)
            .sort((a, b) => Number(b.weight) - Number(a.weight))
            .slice(0, 4);
        if (!influences.length) throw new Error('vertex ' + vertex.vertex_index + ' has no positive skin influence');
        const sum = influences.reduce((total, entry) => total + Number(entry.weight), 0);
        while (influences.length < 4) influences.push({ bone: influences[0].bone, weight: 0 });
        for (const influence of influences) {
            if (!boneIndex.has(influence.bone)) throw new Error('skin influence bone is missing: ' + influence.bone);
            skinIndices.push(boneIndex.get(influence.bone));
            skinWeights.push(Number(influence.weight) / sum);
        }
    }
    if (!Array.isArray(topology.faces) || topology.faces.length !== 258) {
        throw new Error('surface topology must contain exactly 258 Horse_2 faces');
    }
    const indices = [];
    for (const face of topology.faces) {
        const ids = face.vertex_ids;
        if (!Array.isArray(ids) || ids.length < 3) throw new Error('surface face has fewer than 3 vertices');
        for (let index = 1; index < ids.length - 1; index += 1) indices.push(ids[0], ids[index], ids[index + 1]);
    }
    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
    geometry.setAttribute('skinIndex', new THREE.Uint16BufferAttribute(skinIndices, 4));
    geometry.setAttribute('skinWeight', new THREE.Float32BufferAttribute(skinWeights, 4));
    geometry.setIndex(indices);
    geometry.computeVertexNormals();
    geometry.computeBoundingSphere();
    const material = new THREE.MeshStandardMaterial({
        color: 0xe8e8e8,
        roughness: 0.74,
        metalness: 0,
        flatShading: true,
        side: THREE.DoubleSide,
    });
    const mesh = new THREE.SkinnedMesh(geometry, material);
    mesh.name = 'Horse_geo_browser_344v';
    // LTX pose guides must encode skeletal motion, not a second changing
    // signal from hard WebGL shadow-map wedges. The immutable endpoints retain
    // the canonical reference render and its matte shadow byte-for-byte.
    mesh.castShadow = false;
    mesh.receiveShadow = false;
    const skeleton = new THREE.Skeleton(boneOrder);
    modelState.model.add(mesh);
    modelState.model.updateWorldMatrix(true, true);
    mesh.bind(skeleton, new THREE.Matrix4());
    return { mesh, skeleton, triangleCount: indices.length / 3 };
}

function webglInfo(renderer) {
    const gl = renderer.getContext();
    const debug = gl.getExtension('WEBGL_debug_renderer_info');
    return {
        isWebGL2: renderer.capabilities.isWebGL2 === true,
        version: gl.getParameter(gl.VERSION),
        shadingLanguageVersion: gl.getParameter(gl.SHADING_LANGUAGE_VERSION),
        vendor: debug ? gl.getParameter(debug.UNMASKED_VENDOR_WEBGL) : gl.getParameter(gl.VENDOR),
        renderer: debug ? gl.getParameter(debug.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER),
        threeRevision: THREE.REVISION,
    };
}

function makeRenderer(width, height) {
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, preserveDrawingBuffer: true });
    renderer.setPixelRatio(1);
    renderer.setSize(width, height, false);
    renderer.setClearColor(STATIC_SCENE.clearColorHex, 1);
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = STATIC_SCENE.toneMappingExposure;
    renderer.shadowMap.enabled = STATIC_SCENE.shadowsEnabled;
    document.body.replaceChildren(renderer.domElement);
    return renderer;
}

function makeScene(model, groundHeight) {
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(STATIC_SCENE.backgroundHex);
    scene.add(model);
    scene.add(new THREE.HemisphereLight(
        STATIC_SCENE.hemisphere.skyHex,
        STATIC_SCENE.hemisphere.groundHex,
        STATIC_SCENE.hemisphere.intensity,
    ));
    const key = new THREE.DirectionalLight(STATIC_SCENE.key.colorHex, STATIC_SCENE.key.intensity);
    key.position.set(...STATIC_SCENE.key.position);
    key.castShadow = false;
    scene.add(key);
    scene.add(key.target);
    const ground = new THREE.Mesh(
        new THREE.PlaneGeometry(STATIC_SCENE.ground.size, STATIC_SCENE.ground.size),
        new THREE.MeshStandardMaterial({
            color: STATIC_SCENE.ground.colorHex,
            roughness: STATIC_SCENE.ground.roughness,
            metalness: STATIC_SCENE.ground.metalness,
        }),
    );
    ground.position.z = Number(groundHeight);
    ground.receiveShadow = false;
    scene.add(ground);
    return scene;
}

function cloneValue(value) {
    return JSON.parse(JSON.stringify(value));
}

function buildV12RecoveryPoseContract(base) {
    if (config.sceneContract !== V12_SCENE_CONTRACT) return base;
    if (!Array.isArray(config.guidePlan) || config.guidePlan.length !== 9) {
        throw new Error('v12 recovery guide plan must contain exactly nine guides');
    }
    const authoredByTargetFrame = new Map(base.guides.map((guide, index) => [
        guide.frameIndex,
        { guide, fitted: base.fitted.frames[index], qa: base.qa.guides[index] },
    ]));
    const rest = authoredByTargetFrame.get(0);
    if (!rest) throw new Error('v12 recovery authoring requires the actionless frame 0');
    const guides = config.guidePlan.map((guide, authoredIndex) => {
        const source = guide.swingLimb ? authoredByTargetFrame.get(guide.frameIndex) : rest;
        if (!source) throw new Error('missing authored swing source for v12 guide ' + guide.frameIndex);
        return {
            ...guide,
            authoredClipFrame: authoredIndex,
            authoredClipTimeSeconds: authoredIndex,
            sourceAuthoredGuideFrame: source.guide.frameIndex,
        };
    });
    const fittedFrames = guides.map((guide, frame) => {
        const source = guide.swingLimb ? authoredByTargetFrame.get(guide.frameIndex) : rest;
        return { frame, limbs: cloneValue(source.fitted.limbs) };
    });
    const qaGuides = guides.map((guide) => {
        const source = guide.swingLimb ? authoredByTargetFrame.get(guide.frameIndex) : rest;
        return {
            ...cloneValue(source.qa),
            frameIndex: guide.frameIndex,
            swingLimb: guide.swingLimb,
            stanceLimbs: LIMB_ORDER.filter((limb) => limb !== guide.swingLimb),
            stanceHoofCount: guide.swingLimb ? 3 : 4,
        };
    });
    return {
        ...base,
        schema: 'autorig-browser-horse-recovery-guide-poses.v1',
        status: 'recovery_pose_contract_ready_not_rendered',
        guideFrameIndices: guides.map((guide) => guide.frameIndex),
        guides,
        fitted: {
            ...base.fitted,
            fps: 1,
            frameCount: fittedFrames.length,
            durationSeconds: fittedFrames.length - 1,
            frames: fittedFrames,
        },
        qa: {
            ...base.qa,
            recoveryGuideCount: 3,
            guides: qaGuides,
        },
    };
}

function addDeterministicContactCues(scene, terminalBones, groundHeight) {
    if (config.sceneContract !== V12_SCENE_CONTRACT) return null;
    const cue = STATIC_SCENE.contactCues;
    const geometry = new THREE.PlaneGeometry(cue.widthWorld, cue.lengthWorld, 1, 1);
    const material = new THREE.ShaderMaterial({
        transparent: true,
        depthWrite: false,
        depthTest: true,
        toneMapped: false,
        uniforms: {
            cueColor: { value: new THREE.Color(cue.colorHex) },
            cueOpacity: { value: cue.opacity },
            featherInner: { value: cue.featherInner },
            featherOuter: { value: cue.featherOuter },
        },
        vertexShader: 'varying vec2 vUv; void main(){vUv=uv;gl_Position=projectionMatrix*modelViewMatrix*vec4(position,1.0);}',
        fragmentShader: 'varying vec2 vUv; uniform vec3 cueColor; uniform float cueOpacity; uniform float featherInner; uniform float featherOuter; void main(){float r=length((vUv-0.5)*2.0);float a=cueOpacity*(1.0-smoothstep(featherInner,featherOuter,r));gl_FragColor=vec4(cueColor,a);}',
    });
    const restPositions = {};
    const meshesByLimb = {};
    for (const limb of LIMB_ORDER) {
        const world = terminalBones[limb].getWorldPosition(new THREE.Vector3());
        const plane = new THREE.Mesh(geometry, material);
        plane.name = 'AutoRig_V12_Contact_Cue_' + limb;
        plane.position.set(world.x, world.y, Number(groundHeight) + cue.groundOffsetWorld);
        plane.renderOrder = 1;
        plane.frustumCulled = false;
        scene.add(plane);
        restPositions[limb] = plane.position.toArray();
        meshesByLimb[limb] = plane;
    }
    return {
        meshesByLimb,
        metadata: {
            ...cue,
            count: LIMB_ORDER.length,
            staticRestPositionsWorld: restPositions,
            shadowMapUsed: false,
            perGuideVisibility: true,
        },
    };
}

function applyContactCueVisibility(runtime, frameIndex) {
    if (!runtime) return null;
    const expected = config.contactCueVisibilityPlan?.find((row) => row.frameIndex === frameIndex);
    if (!expected) throw new Error('missing contact-cue visibility contract for guide ' + frameIndex);
    for (const limb of LIMB_ORDER) {
        runtime.meshesByLimb[limb].visible = expected.visibleLimbs.includes(limb);
    }
    const visibleLimbs = LIMB_ORDER.filter((limb) => runtime.meshesByLimb[limb].visible);
    const hiddenLimbs = LIMB_ORDER.filter((limb) => !runtime.meshesByLimb[limb].visible);
    return {
        frameIndex,
        visibleLimbs,
        hiddenLimbs,
        visibleCueCount: visibleLimbs.length,
        hiddenCueCount: hiddenLimbs.length,
    };
}

function verifyV12ContactCueVisibility(poseContract, projectedHoovesByGuide) {
    const guides = projectedHoovesByGuide.map((row, index) => {
        const guide = poseContract.guides[index];
        const actual = row.contactCueVisibility;
        const expectedVisible = LIMB_ORDER.filter((limb) => limb !== guide.swingLimb);
        const expectedHidden = guide.swingLimb ? [guide.swingLimb] : [];
        if (
            !actual
            || actual.frameIndex !== guide.frameIndex
            || JSON.stringify(actual.visibleLimbs) !== JSON.stringify(expectedVisible)
            || JSON.stringify(actual.hiddenLimbs) !== JSON.stringify(expectedHidden)
            || actual.visibleCueCount !== expectedVisible.length
            || actual.hiddenCueCount !== expectedHidden.length
        ) {
            throw new Error('v12 contact-cue visibility does not match stance at guide ' + guide.frameIndex);
        }
        return {
            frameIndex: guide.frameIndex,
            swingLimb: guide.swingLimb,
            visibleLimbs: [...actual.visibleLimbs],
            hiddenLimbs: [...actual.hiddenLimbs],
            visibleCueCount: actual.visibleCueCount,
            hiddenCueCount: actual.hiddenCueCount,
            exactlyMatchesStance: true,
        };
    });
    return {
        schema: 'autorig-browser-contact-cue-visibility-qa.v1',
        status: 'PASS',
        perGuideVisibility: true,
        swingGuidesHideExactlyOneCue: guides.filter((guide) => guide.swingLimb).every((guide) => (
            guide.visibleCueCount === 3
            && guide.hiddenCueCount === 1
            && guide.hiddenLimbs[0] === guide.swingLimb
        )),
        stanceGuidesShowAllFourCues: guides.filter((guide) => !guide.swingLimb).every((guide) => (
            guide.visibleCueCount === 4 && guide.hiddenCueCount === 0
        )),
        guides,
    };
}

function verifyV12PostBakeHoofProjections(poseContract, projectedHoovesByGuide, options = {}) {
    if (poseContract.schema !== 'autorig-browser-horse-recovery-guide-poses.v1') {
        throw new Error('v12 recovery pose contract schema is invalid');
    }
    if (!Array.isArray(projectedHoovesByGuide) || projectedHoovesByGuide.length !== 9) {
        throw new Error('v12 recovery post-bake QA requires nine projected guide rows');
    }
    const maximumStanceErrorPx = Number(options.maximumStanceErrorPx ?? 1);
    const maximumRequestedErrorPx = Number(options.maximumRequestedErrorPx ?? 3);
    const minimumSwingLiftPx = Number(options.minimumSwingLiftPx ?? 5);
    const rest = projectedHoovesByGuide[0].hooves;
    const qaGuides = projectedHoovesByGuide.map((row, index) => {
        const guide = poseContract.guides[index];
        const desired = poseContract.fitted.frames[index];
        if (row.frameIndex !== guide.frameIndex) throw new Error('v12 projected guide order changed');
        const stanceLimbs = LIMB_ORDER.filter((limb) => limb !== guide.swingLimb);
        const stanceErrors = Object.fromEntries(stanceLimbs.map((limb) => [
            limb,
            Math.hypot(row.hooves[limb][0] - rest[limb][0], row.hooves[limb][1] - rest[limb][1]),
        ]));
        const requestedErrors = Object.fromEntries(LIMB_ORDER.map((limb) => {
            const target = desired.limbs[limb].points.at(-1);
            return [limb, Math.hypot(row.hooves[limb][0] - target[0], row.hooves[limb][1] - target[1])];
        }));
        const maximumStance = Math.max(0, ...Object.values(stanceErrors));
        const maximumRequested = Math.max(...Object.values(requestedErrors));
        if (maximumStance > maximumStanceErrorPx) throw new Error('v12 post-bake stance hoof error exceeds tolerance');
        if (maximumRequested > maximumRequestedErrorPx) throw new Error('v12 post-bake requested hoof error exceeds tolerance');
        const swingLift = guide.swingLimb
            ? rest[guide.swingLimb][1] - row.hooves[guide.swingLimb][1]
            : 0;
        if (guide.swingLimb && swingLift < minimumSwingLiftPx) throw new Error('v12 post-bake swing lift is too small');
        return {
            frameIndex: guide.frameIndex,
            swingLimb: guide.swingLimb,
            stanceLimbs,
            stanceHoofCount: stanceLimbs.length,
            maximumStanceErrorPx: maximumStance,
            maximumRequestedErrorPx: maximumRequested,
            swingHoofLiftPx: swingLift,
        };
    });
    const endpointMaximumErrorPx = Math.max(...LIMB_ORDER.map((limb) => Math.hypot(
        projectedHoovesByGuide[0].hooves[limb][0] - projectedHoovesByGuide.at(-1).hooves[limb][0],
        projectedHoovesByGuide[0].hooves[limb][1] - projectedHoovesByGuide.at(-1).hooves[limb][1],
    )));
    if (endpointMaximumErrorPx > maximumStanceErrorPx) throw new Error('v12 post-bake endpoints differ');
    return {
        status: 'PASS',
        hierarchyBakeVerified: true,
        minimumStanceHooves: 3,
        recoveryGuideCount: 3,
        maximumStanceErrorPx,
        maximumRequestedErrorPx,
        minimumSwingLiftPx,
        endpointMaximumErrorPx,
        guides: qaGuides,
    };
}

async function initializeReal() {
    const modelState = buildBundleModel(config.sourceSkeleton);
    const camera = buildBundleCamera(config.fittingBundle.camera);
    const skin = buildSkinnedMesh(modelState, config.skinWeights, config.surfaceTopology);
    const fittingSkeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model: modelState.model,
        camera,
        sourceViewport: config.fittingBundle.camera.resolution,
        referenceResolution: config.fittingBundle.camera.resolution,
        outputResolution: [${WIDTH}, ${HEIGHT}],
        includePositionMappings: 'auto',
    });
    const basePoseContract = authorHorseV10SwingGuidePoses({
        skeleton: fittingSkeleton,
        candidateA: config.candidateA,
        candidateB: config.candidateB,
    });
    const poseContract = buildV12RecoveryPoseContract(basePoseContract);
    const hierarchy = bakeFittedAnimationToThreeHierarchyClip({
        THREE,
        model: modelState.model,
        camera,
        skeleton: fittingSkeleton,
        fitted: poseContract.fitted,
        outputResolution: [${WIDTH}, ${HEIGHT}],
        name: config.sceneContract === V12_SCENE_CONTRACT
            ? 'Horse_Walk_v12_Browser_Recovery_Guides'
            : 'Horse_Walk_v10_Browser_Swing_Guides',
    });
    if (hierarchy.clip.validate() !== true) throw new Error('Three hierarchy clip validation failed');
    const scene = makeScene(modelState.model, config.fittingBundle.ground_plane.height);
    const renderer = makeRenderer(${WIDTH}, ${HEIGHT});
    const mixer = new THREE.AnimationMixer(modelState.model);
    const action = mixer.clipAction(hierarchy.clip);
    action.setLoop(THREE.LoopOnce, 1);
    action.clampWhenFinished = true;
    action.play();
    const applyAuthoredTime = (timeSeconds) => {
        // Sampling frame 48 completes a LoopOnce action. Explicitly re-enable
        // it before every non-monotonic CDP render so later frame requests do
        // not silently reuse the endpoint pose.
        action.enabled = true;
        action.paused = false;
        action.setEffectiveWeight(1);
        mixer.setTime(timeSeconds);
    };
    const projection = createViewerToLtxProjection({
        sourceViewport: config.fittingBundle.camera.resolution,
        referenceResolution: config.fittingBundle.camera.resolution,
        outputResolution: [${WIDTH}, ${HEIGHT}],
    });
    const terminalBones = Object.fromEntries(Object.entries(fittingSkeleton.limbs).map(([limb, value]) => [
        limb,
        modelState.bones.get(value.terminalBone),
    ]));
    applyAuthoredTime(0);
    modelState.model.updateWorldMatrix(true, true);
    const contactCueRuntime = addDeterministicContactCues(
        scene,
        terminalBones,
        config.fittingBundle.ground_plane.height,
    );
    const sample = (guide) => {
        applyAuthoredTime(guide.authoredClipTimeSeconds);
        modelState.model.updateWorldMatrix(true, true);
        const contactCueVisibility = applyContactCueVisibility(contactCueRuntime, guide.frameIndex);
        const hooves = Object.fromEntries(Object.entries(terminalBones).map(([limb, bone]) => {
            const world = bone.getWorldPosition(new THREE.Vector3());
            const ndc = world.project(camera);
            return [limb, projection.ndcToOutput([ndc.x, ndc.y, ndc.z])];
        }));
        return { frameIndex: guide.frameIndex, hooves, contactCueVisibility };
    };
    const projectedHoovesByGuide = poseContract.guides.map(sample);
    const postBakeOptions = {
        maximumStanceErrorPx: 1,
        // The actual canary worst-case requested terminal error is 2.27 px
        // (2.45 px across every fitted chain head). Three pixels is therefore
        // a strict, measured guard with no broad visual-error allowance.
        maximumRequestedErrorPx: 3,
        minimumSwingLiftPx: 5,
    };
    const postBakeQa = config.sceneContract === V12_SCENE_CONTRACT
        ? verifyV12PostBakeHoofProjections(poseContract, projectedHoovesByGuide, postBakeOptions)
        : verifyHorseV10PostBakeHoofProjections({ poseContract, projectedHoovesByGuide, ...postBakeOptions });
    const contactCueQa = config.sceneContract === V12_SCENE_CONTRACT
        ? verifyV12ContactCueVisibility(poseContract, projectedHoovesByGuide)
        : null;
    const info = webglInfo(renderer);
    window.__renderGuide = async (frameIndex) => {
        const guide = poseContract.guides.find((value) => value.frameIndex === frameIndex);
        if (!guide) throw new Error('unknown guide frame ' + frameIndex);
        applyAuthoredTime(guide.authoredClipTimeSeconds);
        modelState.model.updateWorldMatrix(true, true);
        applyContactCueVisibility(contactCueRuntime, guide.frameIndex);
        renderer.render(scene, camera);
        await new Promise((resolve) => requestAnimationFrame(resolve));
        renderer.render(scene, camera);
        return {
            frameIndex,
            width: renderer.domElement.width,
            height: renderer.domElement.height,
            dataUrl: renderer.domElement.toDataURL('image/png'),
        };
    };
    window.__AUTORIG_RESULT__ = {
        mode: 'real',
        poseContract,
        hierarchyQa: hierarchy.qa,
        postBakeQa,
        contactCueQa,
        webgl: info,
        staticScene: {
            contract: config.sceneContract,
            cameraSource: 'immutable_fitting_bundle',
            ...STATIC_SCENE,
            contactCues: contactCueRuntime?.metadata || null,
        },
        model: {
            sourceBoneCount: modelState.sourceBones.length,
            vertexCount: skin.mesh.geometry.getAttribute('position').count,
            sourceFaceCount: config.surfaceTopology.faces.length,
            triangleCount: skin.triangleCount,
            skinBoneCount: skin.skeleton.bones.length,
            maximumHeadReconstructionErrorWorld: modelState.maximumHeadReconstructionErrorWorld,
        },
    };
}

async function initializeSmoke() {
    const renderer = makeRenderer(64, 64);
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x102030);
    const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 10);
    camera.position.z = 2;
    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute('position', new THREE.Float32BufferAttribute([-0.8,-0.7,0, 0.8,-0.7,0, 0,0.8,0], 3));
    const triangle = new THREE.Mesh(geometry, new THREE.MeshBasicMaterial({ color: 0x50e090 }));
    scene.add(triangle);
    renderer.render(scene, camera);
    window.__renderGuide = async () => ({
        frameIndex: 0,
        width: renderer.domElement.width,
        height: renderer.domElement.height,
        dataUrl: renderer.domElement.toDataURL('image/png'),
    });
    window.__AUTORIG_RESULT__ = { mode: 'synthetic-smoke', webgl: webglInfo(renderer), vertexCount: 3 };
}

try {
    if (config.mode === 'synthetic-smoke') await initializeSmoke();
    else await initializeReal();
    window.__AUTORIG_READY__ = true;
} catch (error) {
    window.__AUTORIG_ERROR__ = String(error?.stack || error);
    console.error(error);
}
</script></body></html>`;
}

function startHarnessServer({ config, threeModule }) {
    const routes = new Map([
        ['/index.html', { type: 'buffer', body: Buffer.from(harnessHtml(), 'utf8'), contentType: 'text/html; charset=utf-8' }],
        ['/config.json', { type: 'buffer', body: Buffer.from(JSON.stringify(config), 'utf8'), contentType: 'application/json; charset=utf-8' }],
        ['/three.module.js', { type: 'file', filename: threeModule }],
        ['/adapter.js', { type: 'file', filename: ADAPTER_FILE }],
        ['/author.js', { type: 'file', filename: AUTHOR_FILE }],
    ]);
    const server = http.createServer((request, response) => {
        const route = routes.get(new URL(request.url, 'http://127.0.0.1').pathname);
        response.setHeader('Cache-Control', 'no-store');
        if (!route) {
            response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
            response.end('not found');
            return;
        }
        if (route.type === 'file') {
            response.writeHead(200, { 'Content-Type': mime(route.filename) });
            fs.createReadStream(route.filename).pipe(response);
        } else {
            response.writeHead(200, { 'Content-Type': route.contentType });
            response.end(route.body);
        }
    });
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => resolve({
            server,
            url: `http://127.0.0.1:${server.address().port}/index.html`,
        }));
    });
}

class CdpClient {
    constructor(url) {
        this.socket = new WebSocket(url);
        this.nextId = 1;
        this.pending = new Map();
        this.events = [];
        this.socket.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (message.id) {
                const pending = this.pending.get(message.id);
                if (!pending) return;
                this.pending.delete(message.id);
                if (message.error) pending.reject(new Error(message.error.message));
                else pending.resolve(message.result || {});
            } else {
                this.events.push(message);
            }
        };
    }

    async open() {
        if (this.socket.readyState === WebSocket.OPEN) return;
        await new Promise((resolve, reject) => {
            this.socket.onopen = resolve;
            this.socket.onerror = () => reject(new Error('CDP WebSocket connection failed'));
        });
    }

    command(method, params = {}) {
        const id = this.nextId++;
        return new Promise((resolve, reject) => {
            this.pending.set(id, { resolve, reject });
            this.socket.send(JSON.stringify({ id, method, params }));
        });
    }

    close() {
        this.socket.close();
    }
}

function delay(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function launchChrome(chromeExecutable) {
    const profileDirectory = fs.mkdtempSync(path.join(process.env.TEMP || process.cwd(), 'autorig-horse-guide-chrome-'));
    const args = [
        '--headless=new',
        '--use-angle=swiftshader',
        '--enable-webgl',
        '--ignore-gpu-blocklist',
        '--disable-background-networking',
        '--disable-component-update',
        '--disable-default-apps',
        '--disable-extensions',
        '--disable-sync',
        '--no-first-run',
        '--no-default-browser-check',
        '--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0',
        `--user-data-dir=${profileDirectory}`,
        'about:blank',
    ];
    const child = spawn(chromeExecutable, args, { stdio: ['ignore', 'ignore', 'pipe'], windowsHide: true });
    let stderr = '';
    let websocketUrl = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => {
        stderr += chunk;
        const match = stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/);
        if (match) websocketUrl = match[1];
    });
    const started = Date.now();
    while (!websocketUrl && Date.now() - started < 15000) {
        if (child.exitCode != null) fail(`Chrome exited before CDP startup (${child.exitCode}): ${stderr}`);
        await delay(50);
    }
    if (!websocketUrl) fail(`Chrome did not expose CDP within 15 seconds: ${stderr}`);
    const endpoint = new URL(websocketUrl);
    const pages = await (await fetch(`http://${endpoint.host}/json/list`)).json();
    const page = pages.find((value) => value.type === 'page');
    if (!page?.webSocketDebuggerUrl) fail('Chrome did not expose an initial page target');
    return { child, profileDirectory, stderr: () => stderr, pageWebSocketUrl: page.webSocketDebuggerUrl };
}

async function stopChrome(runtime) {
    if (!runtime) return;
    try {
        if (runtime.child.exitCode == null) runtime.child.kill();
        await Promise.race([
            new Promise((resolve) => runtime.child.once('exit', resolve)),
            delay(3000),
        ]);
        if (runtime.child.exitCode == null) runtime.child.kill('SIGKILL');
    } finally {
        fs.rmSync(runtime.profileDirectory, { recursive: true, force: true });
    }
}

async function evaluate(client, expression) {
    const result = await client.command('Runtime.evaluate', {
        expression,
        awaitPromise: true,
        returnByValue: true,
    });
    if (result.exceptionDetails) fail(`browser evaluation failed: ${result.exceptionDetails.text}`);
    return result.result?.value;
}

async function runHarnessInChrome({ chromeExecutable, url, guideFrames }) {
    let runtime;
    let client;
    try {
        runtime = await launchChrome(chromeExecutable);
        client = new CdpClient(runtime.pageWebSocketUrl);
        await client.open();
        await client.command('Page.enable');
        await client.command('Runtime.enable');
        await client.command('Emulation.setDeviceMetricsOverride', {
            width: WIDTH,
            height: HEIGHT,
            deviceScaleFactor: 1,
            mobile: false,
        });
        await client.command('Page.navigate', { url });
        const started = Date.now();
        let result;
        while (Date.now() - started < 30000) {
            const state = await evaluate(client, `({ready:window.__AUTORIG_READY__===true,error:window.__AUTORIG_ERROR__||null,result:window.__AUTORIG_RESULT__||null})`);
            if (state?.error) fail(`browser harness failed: ${state.error}`);
            if (state?.ready) {
                result = state.result;
                break;
            }
            await delay(100);
        }
        if (!result) fail(`browser harness did not become ready within 30 seconds: ${runtime.stderr()}`);
        const renders = [];
        for (const frameIndex of guideFrames) {
            const rendered = await evaluate(client, `window.__renderGuide(${Number(frameIndex)})`);
            if (!rendered?.dataUrl?.startsWith('data:image/png;base64,')) fail(`guide ${frameIndex} did not return PNG data`);
            renders.push(rendered);
        }
        const version = await client.command('Browser.getVersion');
        return { result, renders, browserVersion: version };
    } finally {
        client?.close();
        await stopChrome(runtime);
    }
}

function pngDimensions(buffer) {
    const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
    if (buffer.length < 24 || !buffer.subarray(0, 8).equals(signature)) fail('rendered output is not PNG');
    return [buffer.readUInt32BE(16), buffer.readUInt32BE(20)];
}

function paeth(left, above, upperLeft) {
    const prediction = left + above - upperLeft;
    const leftDistance = Math.abs(prediction - left);
    const aboveDistance = Math.abs(prediction - above);
    const upperLeftDistance = Math.abs(prediction - upperLeft);
    if (leftDistance <= aboveDistance && leftDistance <= upperLeftDistance) return left;
    if (aboveDistance <= upperLeftDistance) return above;
    return upperLeft;
}

export function decodeOpaqueRgbPng(buffer, field = 'guide PNG') {
    const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
    if (!Buffer.isBuffer(buffer) || buffer.length < 33 || !buffer.subarray(0, 8).equals(signature)) {
        fail(`${field} is not a PNG`);
    }
    let offset = 8;
    let width = null;
    let height = null;
    let channels = null;
    const compressed = [];
    while (offset < buffer.length) {
        if (offset + 12 > buffer.length) fail(`${field} has a truncated PNG chunk`);
        const length = buffer.readUInt32BE(offset);
        const type = buffer.toString('ascii', offset + 4, offset + 8);
        const dataStart = offset + 8;
        const dataEnd = dataStart + length;
        if (dataEnd + 4 > buffer.length) fail(`${field} has a truncated ${type} chunk`);
        const data = buffer.subarray(dataStart, dataEnd);
        if (type === 'IHDR') {
            if (length !== 13) fail(`${field} has an invalid IHDR`);
            width = data.readUInt32BE(0);
            height = data.readUInt32BE(4);
            const bitDepth = data[8];
            const colorType = data[9];
            const compression = data[10];
            const filter = data[11];
            const interlace = data[12];
            if (
                bitDepth !== 8
                || (colorType !== 2 && colorType !== 6)
                || compression !== 0
                || filter !== 0
                || interlace !== 0
            ) {
                fail(`${field} must be a non-interlaced 8-bit RGB/RGBA PNG`);
            }
            channels = colorType === 6 ? 4 : 3;
        } else if (type === 'IDAT') {
            compressed.push(data);
        } else if (type === 'IEND') {
            break;
        }
        offset = dataEnd + 4;
    }
    if (!width || !height || !channels || !compressed.length) fail(`${field} is missing PNG image data`);
    let encoded;
    try {
        encoded = zlib.inflateSync(Buffer.concat(compressed));
    } catch (error) {
        fail(`${field} has invalid compressed image data: ${error.message}`);
    }
    const stride = width * channels;
    if (encoded.length !== height * (stride + 1)) fail(`${field} decoded byte count is invalid`);
    const scanlines = Buffer.alloc(width * height * channels);
    for (let y = 0; y < height; y += 1) {
        const filter = encoded[y * (stride + 1)];
        const source = y * (stride + 1) + 1;
        const target = y * stride;
        for (let x = 0; x < stride; x += 1) {
            const raw = encoded[source + x];
            const left = x >= channels ? scanlines[target + x - channels] : 0;
            const above = y ? scanlines[target - stride + x] : 0;
            const upperLeft = y && x >= channels ? scanlines[target - stride + x - channels] : 0;
            let value;
            if (filter === 0) value = raw;
            else if (filter === 1) value = raw + left;
            else if (filter === 2) value = raw + above;
            else if (filter === 3) value = raw + Math.floor((left + above) / 2);
            else if (filter === 4) value = raw + paeth(left, above, upperLeft);
            else fail(`${field} uses unsupported PNG filter ${filter}`);
            scanlines[target + x] = value & 0xff;
        }
    }
    const rgb = Buffer.alloc(width * height * 3);
    let opaque = true;
    for (let pixel = 0; pixel < width * height; pixel += 1) {
        const source = pixel * channels;
        const target = pixel * 3;
        rgb[target] = scanlines[source];
        rgb[target + 1] = scanlines[source + 1];
        rgb[target + 2] = scanlines[source + 2];
        if (channels === 4 && scanlines[source + 3] !== 255) opaque = false;
    }
    if (!opaque) fail(`${field} must be fully opaque`);
    return { width, height, rgb };
}

function luma(r, g, b) {
    return (54 * r + 183 * g + 19 * b) / 256;
}

export function analyzeStaticSceneGuideFrames(frames, options = {}) {
    const expectedFrameIndices = Array.isArray(options.expectedFrameIndices)
        ? options.expectedFrameIndices.map(Number)
        : [...GUIDE_FRAMES];
    if (
        !expectedFrameIndices.length
        || expectedFrameIndices.some((frame, index) => !Number.isInteger(frame) || (index && frame <= expectedFrameIndices[index - 1]))
    ) {
        fail('static-scene expected frame indices must be strictly increasing integers');
    }
    if (!Array.isArray(frames) || frames.length !== expectedFrameIndices.length) {
        fail(`static-scene QA requires exactly ${expectedFrameIndices.length} guide frames`);
    }
    const borderWidth = Number(options.borderWidth ?? 32);
    const maximumFullFrameMeanLumaRange = Number(options.maximumFullFrameMeanLumaRange ?? 0.5);
    const nearBlackThreshold = Number(options.nearBlackThreshold ?? 64);
    const maximumNearBlackFraction = Number(options.maximumNearBlackFraction ?? 0.001);
    if (!Number.isInteger(borderWidth) || borderWidth <= 0) fail('static-scene border width is invalid');
    const decoded = frames.map((frame, index) => {
        if (Number(frame.frameIndex) !== expectedFrameIndices[index]) {
            fail(`static-scene guide frame order must be ${expectedFrameIndices.join(',')}`);
        }
        const image = frame.decoded || decodeOpaqueRgbPng(frame.buffer, `guide frame ${frame.frameIndex}`);
        if (image.width !== WIDTH || image.height !== HEIGHT || image.rgb.length !== WIDTH * HEIGHT * 3) {
            fail(`guide frame ${frame.frameIndex} must decode to ${WIDTH}x${HEIGHT} RGB`);
        }
        return { ...frame, image };
    });
    if (borderWidth * 2 >= WIDTH || borderWidth * 2 >= HEIGHT) fail('static-scene border width is too large');
    const baseline = decoded[0].image.rgb;
    let backgroundSamplePixels = 0;
    let maximumBackgroundChannelDelta = 0;
    const guideStats = decoded.map(({ frameIndex, image }) => {
        let fullLumaSum = 0;
        let backgroundLumaSum = 0;
        let backgroundPixels = 0;
        let nearBlackPixels = 0;
        for (let y = 0; y < HEIGHT; y += 1) {
            for (let x = 0; x < WIDTH; x += 1) {
                const pixel = y * WIDTH + x;
                const offset = pixel * 3;
                const r = image.rgb[offset];
                const g = image.rgb[offset + 1];
                const b = image.rgb[offset + 2];
                fullLumaSum += luma(r, g, b);
                if (Math.max(r, g, b) <= nearBlackThreshold) nearBlackPixels += 1;
                if (x < borderWidth || x >= WIDTH - borderWidth || y < borderWidth || y >= HEIGHT - borderWidth) {
                    backgroundPixels += 1;
                    backgroundLumaSum += luma(r, g, b);
                    maximumBackgroundChannelDelta = Math.max(
                        maximumBackgroundChannelDelta,
                        Math.abs(r - baseline[offset]),
                        Math.abs(g - baseline[offset + 1]),
                        Math.abs(b - baseline[offset + 2]),
                    );
                }
            }
        }
        backgroundSamplePixels = backgroundPixels;
        return {
            frame_index_int: frameIndex,
            full_frame_mean_luma_float: fullLumaSum / (WIDTH * HEIGHT),
            background_mean_luma_float: backgroundLumaSum / backgroundPixels,
            near_black_pixel_fraction_float: nearBlackPixels / (WIDTH * HEIGHT),
        };
    });
    const fullLumas = guideStats.map((row) => row.full_frame_mean_luma_float);
    const backgroundLumas = guideStats.map((row) => row.background_mean_luma_float);
    const fullFrameMeanLumaRange = Math.max(...fullLumas) - Math.min(...fullLumas);
    const backgroundMeanLumaRange = Math.max(...backgroundLumas) - Math.min(...backgroundLumas);
    const maximumObservedNearBlackFraction = Math.max(...guideStats.map((row) => row.near_black_pixel_fraction_float));
    const endpointByteIdentical = Buffer.isBuffer(frames[0].buffer)
        && Buffer.isBuffer(frames.at(-1).buffer)
        && frames[0].buffer.equals(frames.at(-1).buffer);
    const status = (
        endpointByteIdentical
        && maximumBackgroundChannelDelta === 0
        && backgroundMeanLumaRange === 0
        && fullFrameMeanLumaRange <= maximumFullFrameMeanLumaRange
        && maximumObservedNearBlackFraction <= maximumNearBlackFraction
    ) ? 'PASS' : 'FAIL';
    const report = {
        schema: 'autorig-browser-static-scene-qa.v1',
        status,
        expected_frame_indices_array: expectedFrameIndices,
        decoded_rgb_statistics_bool: true,
        endpoint_byte_identical_bool: endpointByteIdentical,
        border_width_int: borderWidth,
        background_sample_pixels_int: backgroundSamplePixels,
        maximum_background_channel_delta_int: maximumBackgroundChannelDelta,
        background_mean_luma_range_float: backgroundMeanLumaRange,
        maximum_background_mean_luma_range_float: 0,
        full_frame_mean_luma_range_float: fullFrameMeanLumaRange,
        maximum_full_frame_mean_luma_range_float: maximumFullFrameMeanLumaRange,
        near_black_threshold_int: nearBlackThreshold,
        maximum_near_black_pixel_fraction_float: maximumObservedNearBlackFraction,
        allowed_near_black_pixel_fraction_float: maximumNearBlackFraction,
        guides_array: guideStats,
    };
    if (status !== 'PASS' && options.failClosed !== false) {
        fail(`unified browser static-scene QA failed: ${JSON.stringify(report)}`);
    }
    return report;
}

function sourceVideoPin(observations, label) {
    const filename = existingFile(observations.provenance?.source_video, `${label} source video`);
    const pin = pinFile(filename, { path: filename });
    const expected = String(observations.provenance?.source_video_sha256 || '').toLowerCase();
    if (!SHA256_PATTERN.test(expected) || pin.sha256 !== expected) fail(`${label} source video SHA-256 mismatch`);
    return pin;
}

function immutableEntry(manifest, filename) {
    const entry = manifest.files.find((value) => value.filename === filename);
    if (!entry) fail(`immutable manifest does not pin ${filename}`);
    return { filename, bytes: entry.bytes, sha256: entry.sha256 };
}

async function runSyntheticSmoke(config) {
    const output = outputDirectory(config.output);
    const chromeExecutable = existingFile(config.chrome, 'chrome');
    const threeModule = existingFile(config.three, 'three');
    const { server, url } = await startHarnessServer({ config: { mode: 'synthetic-smoke' }, threeModule });
    try {
        const browser = await runHarnessInChrome({ chromeExecutable, url, guideFrames: [0] });
        const render = browser.renders[0];
        const png = Buffer.from(render.dataUrl.slice('data:image/png;base64,'.length), 'base64');
        const dimensions = pngDimensions(png);
        if (dimensions[0] !== 64 || dimensions[1] !== 64) fail(`synthetic PNG dimensions are ${dimensions.join('x')}`);
        fs.mkdirSync(output, { recursive: false });
        const pngPath = path.join(output, 'synthetic-webgl-smoke.png');
        fs.writeFileSync(pngPath, png, { flag: 'wx' });
        const report = {
            schema: 'autorig-browser-webgl-synthetic-smoke.v1',
            status: 'PASS',
            browserOnly: true,
            blenderUsed: false,
            chrome: pinFile(chromeExecutable, { product: browser.browserVersion.product, protocolVersion: browser.browserVersion.protocolVersion }),
            three: pinFile(threeModule, { revision: browser.result.webgl.threeRevision }),
            webgl: browser.result.webgl,
            model: { vertexCount: browser.result.vertexCount },
            output: pinFile(pngPath, { width: dimensions[0], height: dimensions[1] }),
        };
        writeJson(path.join(output, 'report.json'), report);
        return { output, report };
    } finally {
        await new Promise((resolve) => server.close(resolve));
    }
}

async function runReal(config) {
    const output = outputDirectory(config.output);
    const sceneProfile = browserGuideSceneProfile(config.sceneContract);
    const {
        sceneContract,
        unifiedBrowserScene,
        recoveryGuides,
        guideFrames,
        recoveryFrames,
        deterministicContactCues,
    } = sceneProfile;
    const bundleDirectory = existingDirectory(config.bundle, 'bundle');
    const candidateAPath = existingFile(config.candidateA, 'candidate-a');
    const candidateBPath = existingFile(config.candidateB, 'candidate-b');
    const chromeExecutable = existingFile(config.chrome, 'chrome');
    const threeModule = existingFile(config.three, 'three');
    const candidateAValidated = validateImmutableInputs({ bundleDirectory, observationsPath: candidateAPath });
    const candidateBValidated = validateImmutableInputs({ bundleDirectory, observationsPath: candidateBPath });
    if (candidateAValidated.integrity.fittingBundleSha256 !== candidateBValidated.integrity.fittingBundleSha256) {
        fail('candidate observations do not pin the same fitting bundle');
    }
    const manifest = candidateAValidated.immutableManifest;
    const topologyEntry = immutableEntry(manifest, 'surface_topology.json.gz');
    const weightsEntry = immutableEntry(manifest, 'skin_weights.json.gz');
    const referenceEntry = immutableEntry(manifest, 'reference_rgb.png');
    const topologyPath = path.join(bundleDirectory, topologyEntry.filename);
    const weightsPath = path.join(bundleDirectory, weightsEntry.filename);
    const referencePath = path.join(bundleDirectory, referenceEntry.filename);
    const sourceVideoA = sourceVideoPin(candidateAValidated.observations, 'candidate A');
    const sourceVideoB = sourceVideoPin(candidateBValidated.observations, 'candidate B');
    const harnessConfig = {
        mode: 'real',
        sceneContract,
        fittingBundle: candidateAValidated.fittingBundle,
        sourceSkeleton: candidateAValidated.skeleton,
        surfaceTopology: readGzipJson(topologyPath, 'surface topology'),
        skinWeights: readGzipJson(weightsPath, 'skin weights'),
        candidateA: candidateAValidated.observations,
        candidateB: candidateBValidated.observations,
        guidePlan: recoveryGuides ? HORSE_V12_RECOVERY_GUIDE_PLAN : null,
        contactCueVisibilityPlan: recoveryGuides ? buildHorseV12ContactCueVisibilityPlan() : null,
    };
    const { server, url } = await startHarnessServer({ config: harnessConfig, threeModule });
    let browser;
    try {
        browser = await runHarnessInChrome({
            chromeExecutable,
            url,
            guideFrames: unifiedBrowserScene ? guideFrames : SWING_FRAMES,
        });
    } finally {
        await new Promise((resolve) => server.close(resolve));
    }
    if (browser.result.mode !== 'real') fail('browser returned the wrong harness mode');
    if (browser.result.webgl.threeRevision !== '160') fail(`renderer used Three r${browser.result.webgl.threeRevision}, expected r160`);
    if (browser.result.model.vertexCount !== 344) fail('browser did not render the 344-vertex Horse_2 mesh');
    if (browser.result.model.sourceFaceCount !== 258) fail('browser did not use all 258 source faces');
    if (browser.result.postBakeQa?.status !== 'PASS') fail('post-bake hoof QA did not pass');
    if (recoveryGuides && browser.result.contactCueQa?.status !== 'PASS') {
        fail('v12 per-guide contact-cue visibility QA did not pass');
    }
    if (browser.result.postBakeQa.guides.filter((guide) => guide.swingLimb).some((guide) => guide.stanceHoofCount !== 3)) {
        fail('a swing guide did not retain exactly three stance hooves');
    }
    const staging = `${output}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
    fs.mkdirSync(staging, { recursive: false });
    try {
        const reference = fs.readFileSync(referencePath);
        const referenceSha = sha256Buffer(reference);
        if (reference.length !== referenceEntry.bytes || referenceSha !== referenceEntry.sha256) fail('reference RGB pin changed after validation');
        const renderByFrame = new Map(browser.renders.map((render) => [render.frameIndex, render]));
        const guidePins = [];
        const guideBuffers = [];
        for (const frameIndex of guideFrames) {
            const filename = `guide_${String(frameIndex).padStart(3, '0')}.png`;
            const destination = path.join(staging, filename);
            const buffer = !unifiedBrowserScene && (frameIndex === 0 || frameIndex === 48)
                ? reference
                : Buffer.from(renderByFrame.get(frameIndex).dataUrl.slice('data:image/png;base64,'.length), 'base64');
            const [width, height] = pngDimensions(buffer);
            if (width !== WIDTH || height !== HEIGHT) fail(`guide ${frameIndex} is ${width}x${height}, expected ${WIDTH}x${HEIGHT}`);
            fs.writeFileSync(destination, buffer, { flag: 'wx' });
            const guide = browser.result.poseContract.guides.find((value) => value.frameIndex === frameIndex);
            guideBuffers.push({ frameIndex, buffer });
            guidePins.push(pinFile(destination, {
                frameIndex,
                role: guide.role,
                swingLimb: guide.swingLimb,
                strength: guide.strength,
                width,
                height,
                renderSource: unifiedBrowserScene || (frameIndex !== 0 && frameIndex !== 48)
                    ? 'browser_threejs'
                    : 'immutable_reference_rgb',
                byteIdenticalReferenceCopy: !unifiedBrowserScene && (frameIndex === 0 || frameIndex === 48),
            }));
        }
        if (guidePins[0].sha256 !== guidePins.at(-1).sha256) {
            fail('frame 0 and frame 48 are not byte-identical cycle endpoints');
        }
        if (!unifiedBrowserScene && (guidePins[0].sha256 !== referenceSha || guidePins.at(-1).sha256 !== referenceSha)) {
            fail('v10 frame 0 and frame 48 are not byte-identical reference copies');
        }
        if (unifiedBrowserScene && guidePins.some((guide) => guide.renderSource !== 'browser_threejs')) {
            fail('unified static-scene guides must all come from the same browser renderer');
        }
        const swingHashes = guidePins.filter((guide) => SWING_FRAMES.includes(guide.frameIndex)).map((guide) => guide.sha256);
        if (new Set(swingHashes).size !== SWING_FRAMES.length || swingHashes.includes(guidePins[0].sha256)) {
            fail(`all four swing guide PNGs must be distinct from each other and the cycle endpoint: ${JSON.stringify(
                guidePins.map((guide) => [guide.frameIndex, guide.sha256]),
            )}`);
        }
        const recoveryGuidePins = guidePins.filter((guide) => recoveryFrames.includes(guide.frameIndex));
        if (recoveryGuides && recoveryGuidePins.some((guide) => guide.sha256 !== guidePins[0].sha256)) {
            fail('v12 four-hoof recovery guides must be byte-identical to the actionless cycle endpoint');
        }
        const sceneQa = unifiedBrowserScene
            ? analyzeStaticSceneGuideFrames(guideBuffers, { expectedFrameIndices: guideFrames })
            : null;
        const poseContract = {
            ...browser.result.poseContract,
            status: recoveryGuides
                ? 'PASS_RENDERED_UNIFIED_BROWSER_RECOVERY_GUIDES'
                : unifiedBrowserScene
                    ? 'PASS_RENDERED_UNIFIED_BROWSER_STATIC_SCENE_GUIDES'
                : 'PASS_RENDERED_BROWSER_GUIDES',
            renderer: {
                implementation: 'chromium_webgl_three_r160',
                webgl: browser.result.webgl,
            },
            sceneContract,
            staticScene: browser.result.staticScene,
            browserRendererRequired: unifiedBrowserScene,
            recoveryGuideFrames: recoveryFrames,
            deterministicContactCues,
            postBakeQa: browser.result.postBakeQa,
            contactCueQa: browser.result.contactCueQa,
            hierarchyQa: browser.result.hierarchyQa,
            staticSceneQa: sceneQa,
        };
        const posePath = path.join(staging, 'pose_contract.json');
        writeJson(posePath, poseContract);
        const framesArray = guidePins.map((guide) => ({
            frame_index_int: guide.frameIndex,
            filename_string: guide.filename,
            sha256_string: guide.sha256,
            bytes_int: guide.bytes,
            strength_float: guide.strength,
        }));
        const manifestValue = {
            schema: recoveryGuides
                ? 'autorig-browser-ltx-recovery-guide-bundle.v1'
                : unifiedBrowserScene
                    ? 'autorig-browser-ltx-static-scene-guide-bundle.v1'
                : 'autorig-browser-ltx-guide-bundle.v1',
            status: 'PASS',
            approvedForAnimationLibrary: false,
            browserOnly: true,
            blenderUsed: false,
            rigType: 'HORSE_2',
            resolution: [WIDTH, HEIGHT],
            source_reference_sha256_string: referenceSha,
            source_reference_is_guide_bool: !unifiedBrowserScene,
            endpoint_guide_sha256_string: guidePins[0].sha256,
            cycle_frame_count_int: 49,
            guide_count_int: guidePins.length,
            recovery_frame_indices_array: recoveryFrames,
            recovery_guides_byte_identical_endpoint_bool: recoveryGuides
                ? recoveryGuidePins.every((guide) => guide.sha256 === guidePins[0].sha256)
                : null,
            renderer_object: {
                renderer_string: 'browser_threejs',
                blender_used_bool: false,
                scene_contract_string: sceneContract,
                all_guide_frames_browser_rendered_bool: unifiedBrowserScene,
                shadows_enabled_bool: false,
                deterministic_contact_cues_bool: deterministicContactCues,
                per_guide_contact_cue_visibility_bool: recoveryGuides,
                contact_cue_implementation_string: recoveryGuides
                    ? 'static_rest_hoof_radial_alpha_planes'
                    : null,
            },
            frames_array: framesArray,
            source: {
                sourceModelSha256: candidateAValidated.integrity.sourceModelSha256,
                immutableManifest: pinFile(candidateAValidated.immutableManifestPath),
                fittingBundle: pinFile(candidateAValidated.fittingBundlePath),
                skeleton: pinFile(candidateAValidated.skeletonPath),
                surfaceTopology: topologyEntry,
                skinWeights: weightsEntry,
                referenceRgb: referenceEntry,
            },
            observations: {
                candidateA: pinFile(candidateAPath, { sourceVideo: sourceVideoA }),
                candidateB: pinFile(candidateBPath, { sourceVideo: sourceVideoB }),
            },
            renderer: {
                chrome: pinFile(chromeExecutable, {
                    product: browser.browserVersion.product,
                    protocolVersion: browser.browserVersion.protocolVersion,
                    userAgent: browser.browserVersion.userAgent,
                }),
                three: pinFile(threeModule, { revision: browser.result.webgl.threeRevision }),
                adapter: pinFile(ADAPTER_FILE),
                author: pinFile(AUTHOR_FILE),
                cli: pinFile(TOOL_FILE),
                webgl: browser.result.webgl,
            },
            model: browser.result.model,
            hierarchyQa: browser.result.hierarchyQa,
            postBakeQa: browser.result.postBakeQa,
            contactCueQa: browser.result.contactCueQa,
            staticSceneQa: sceneQa,
            staticSceneRenderer: browser.result.staticScene,
            poseContract: pinFile(posePath),
            guides: guidePins,
        };
        writeJson(path.join(staging, 'immutable_manifest.json'), manifestValue);
        fs.renameSync(staging, output);
        return { output, manifest: manifestValue };
    } catch (error) {
        fs.rmSync(staging, { recursive: true, force: true });
        throw error;
    }
}

export async function main(argv = process.argv.slice(2)) {
    const config = parseArguments(argv);
    return config.syntheticSmoke ? runSyntheticSmoke(config) : runReal(config);
}

if (path.resolve(process.argv[1] || '') === path.resolve(TOOL_FILE)) {
    main().then((result) => {
        process.stdout.write(`${JSON.stringify({ status: 'PASS', output: result.output }, null, 2)}\n`);
    }).catch((error) => {
        process.stderr.write(`${String(error?.stack || error)}\n`);
        process.exitCode = 1;
    });
}

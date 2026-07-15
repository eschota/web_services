#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import { fitBrowserAnimation } from '../../static/js/animation-fitting-browser-core.js';
import { prepareRgbObservationsForBrowser } from '../../static/js/animation-fitting-rgb-observation-bridge.js';
import {
    bakeFittedAnimationToThreeHierarchyClip,
    buildHorse2BrowserFittingSkeleton,
    createViewerToLtxProjection,
} from '../../static/js/animation-fitting-three-adapter.js';

const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const IMMUTABLE_MANIFEST_SCHEMA = 'autorig-fitting-immutable-copy.v1';
const FITTING_BUNDLE_SCHEMA = 'autorig-actionless-fitting-bundle.v1';
const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';

export const BROWSER_FIT_CANARY_DEFAULTS = Object.freeze({
    minimumVisibleRatio: 0.7,
    positionMappings: 'auto',
    fit: Object.freeze({
        loop: true,
        iterations: 64,
        tolerance: 0.05,
        jointAttraction: 0.15,
        smoothingRadius: 1,
        loopBlendFrames: 4,
    }),
    gates: Object.freeze({
        maximumHeadReconstructionErrorWorld: 1e-5,
        maximumRestSeedAlignmentErrorPx: 2,
        maximumFinalMeanTargetErrorPx: 3,
        maximumTargetErrorPx: 15,
        maximumBoneLengthErrorPx: 1e-6,
        maximumJointLimitViolationRad: 1e-6,
        maximumContactSlidePx: 2,
        maximumLoopEndpointError: 1e-6,
        maximumSegmentLengthDriftWorld: 1e-6,
        maximumHierarchyBakeReprojectionErrorPx: 1e-6,
        maximumRequestedFittedPointErrorPx: 2,
        maximumUnreachablePixelRayRatio: 0.1,
        requireTargetErrorImprovement: true,
        requireOrderedDeformHeads: true,
        requireFourLimbContacts: false,
    }),
});

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function array(value, field) {
    if (!Array.isArray(value)) throw new Error(`${field} must be an array`);
    return value;
}

function finite(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) throw new Error(`${field} must be finite`);
    return result;
}

function positive(value, field) {
    const result = finite(value, field);
    if (result <= 0) throw new Error(`${field} must be positive`);
    return result;
}

function integer(value, field, minimum = 0) {
    const result = Number(value);
    if (!Number.isInteger(result) || result < minimum) {
        throw new Error(`${field} must be an integer >= ${minimum}`);
    }
    return result;
}

function nonEmptyString(value, field) {
    if (typeof value !== 'string' || !value.trim()) throw new Error(`${field} must be a non-empty string`);
    return value.trim();
}

function sha256(value, field) {
    const result = nonEmptyString(value, field).toLowerCase();
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be lowercase SHA-256`);
    return result;
}

function sha256File(filename) {
    const hash = crypto.createHash('sha256');
    hash.update(fs.readFileSync(filename));
    return hash.digest('hex');
}

function readJson(filename, field) {
    let parsed;
    try {
        parsed = JSON.parse(fs.readFileSync(filename, 'utf8'));
    } catch (error) {
        throw new Error(`${field} is not valid JSON: ${error.message}`);
    }
    return object(parsed, field);
}

function existingDirectory(value, field) {
    const result = path.resolve(nonEmptyString(value, field));
    let stats;
    try {
        stats = fs.statSync(result);
    } catch (error) {
        throw new Error(`${field} does not exist: ${result}`);
    }
    if (!stats.isDirectory()) throw new Error(`${field} must be a directory: ${result}`);
    return result;
}

function existingFile(value, field) {
    const result = path.resolve(nonEmptyString(value, field));
    let stats;
    try {
        stats = fs.statSync(result);
    } catch (error) {
        throw new Error(`${field} does not exist: ${result}`);
    }
    if (!stats.isFile()) throw new Error(`${field} must be a file: ${result}`);
    return result;
}

function safeBundleFilename(value, field) {
    const result = nonEmptyString(value, field);
    if (path.isAbsolute(result) || path.basename(result) !== result || result === '.' || result === '..') {
        throw new Error(`${field} must be a bundle-root filename without path traversal`);
    }
    return result;
}

function normalizedManifestEntry(value, index) {
    const entry = object(value, `immutableManifest.files[${index}]`);
    return {
        filename: safeBundleFilename(entry.filename, `immutableManifest.files[${index}].filename`),
        bytes: integer(entry.bytes, `immutableManifest.files[${index}].bytes`),
        sha256: sha256(entry.sha256, `immutableManifest.files[${index}].sha256`),
    };
}

function assertArtifactPinned(bundle, manifestEntries, key) {
    const artifact = object(object(bundle.artifacts, 'fittingBundle.artifacts')[key], `fittingBundle.artifacts.${key}`);
    const filename = safeBundleFilename(artifact.filename, `fittingBundle.artifacts.${key}.filename`);
    const entry = manifestEntries.get(filename);
    if (!entry) throw new Error(`fittingBundle artifact ${key} is absent from immutable manifest`);
    if (entry.sha256 !== sha256(artifact.sha256, `fittingBundle.artifacts.${key}.sha256`)) {
        throw new Error(`fittingBundle artifact ${key} SHA-256 does not match immutable manifest`);
    }
    if (entry.bytes !== integer(artifact.bytes, `fittingBundle.artifacts.${key}.bytes`)) {
        throw new Error(`fittingBundle artifact ${key} byte count does not match immutable manifest`);
    }
    return filename;
}

/**
 * Validate every file pinned by an actionless bundle before importing Three.js
 * or invoking the browser solver. Extra bundle-root files are rejected so an
 * old or partially replaced bundle cannot silently feed the canary.
 */
export function validateImmutableInputs({ bundleDirectory: directoryValue, observationsPath: observationsValue }) {
    const bundleDirectory = existingDirectory(directoryValue, 'bundleDirectory');
    const observationsPath = existingFile(observationsValue, 'observationsPath');
    const immutableManifestPath = path.join(bundleDirectory, 'immutable_manifest.json');
    const immutableManifest = readJson(immutableManifestPath, 'immutableManifest');
    if (immutableManifest.schema !== IMMUTABLE_MANIFEST_SCHEMA) {
        throw new Error(`immutableManifest.schema must be ${IMMUTABLE_MANIFEST_SCHEMA}`);
    }
    const entries = array(immutableManifest.files, 'immutableManifest.files').map(normalizedManifestEntry);
    const byFilename = new Map();
    entries.forEach((entry) => {
        if (byFilename.has(entry.filename)) throw new Error(`duplicate immutable manifest file ${entry.filename}`);
        byFilename.set(entry.filename, entry);
    });
    if (integer(immutableManifest.bundle_file_count, 'immutableManifest.bundle_file_count', 1) !== entries.length) {
        throw new Error('immutableManifest.bundle_file_count does not match files');
    }
    const listedTotal = entries.reduce((sum, entry) => sum + entry.bytes, 0);
    if (integer(immutableManifest.bundle_total_bytes, 'immutableManifest.bundle_total_bytes', 1) !== listedTotal) {
        throw new Error('immutableManifest.bundle_total_bytes does not match files');
    }
    entries.forEach((entry) => {
        const filename = existingFile(path.join(bundleDirectory, entry.filename), `bundle file ${entry.filename}`);
        const stats = fs.statSync(filename);
        if (stats.size !== entry.bytes) throw new Error(`bundle file ${entry.filename} byte count mismatch`);
        if (sha256File(filename) !== entry.sha256) throw new Error(`bundle file ${entry.filename} SHA-256 mismatch`);
    });
    const actualBundleFiles = fs.readdirSync(bundleDirectory, { withFileTypes: true })
        .filter((entry) => entry.isFile() && entry.name !== 'immutable_manifest.json')
        .map((entry) => entry.name)
        .sort();
    const listedBundleFiles = [...byFilename.keys()].sort();
    if (JSON.stringify(actualBundleFiles) !== JSON.stringify(listedBundleFiles)) {
        throw new Error('bundle root files do not exactly match immutableManifest.files');
    }

    const bundleManifest = object(immutableManifest.bundle_manifest, 'immutableManifest.bundle_manifest');
    const fittingBundleFilename = safeBundleFilename(bundleManifest.filename, 'immutableManifest.bundle_manifest.filename');
    const fittingBundleEntry = byFilename.get(fittingBundleFilename);
    if (!fittingBundleEntry) throw new Error('immutable bundle manifest file is not pinned in files');
    if (fittingBundleEntry.sha256 !== sha256(bundleManifest.sha256, 'immutableManifest.bundle_manifest.sha256')) {
        throw new Error('immutable bundle manifest SHA-256 does not match files');
    }
    const fittingBundlePath = path.join(bundleDirectory, fittingBundleFilename);
    const fittingBundle = readJson(fittingBundlePath, 'fittingBundle');
    if (fittingBundle.schema !== FITTING_BUNDLE_SCHEMA) {
        throw new Error(`fittingBundle.schema must be ${FITTING_BUNDLE_SCHEMA}`);
    }
    const skeletonFilename = assertArtifactPinned(fittingBundle, byFilename, 'skeleton');
    const surfaceAnchorsFilename = assertArtifactPinned(fittingBundle, byFilename, 'surface_anchors');
    const skeletonPath = path.join(bundleDirectory, skeletonFilename);
    const surfaceAnchorsPath = path.join(bundleDirectory, surfaceAnchorsFilename);
    const source = object(fittingBundle.source, 'fittingBundle.source');
    const sourceModelSha256 = sha256(source.sha256, 'fittingBundle.source.sha256');
    const camera = object(fittingBundle.camera, 'fittingBundle.camera');
    const resolution = array(camera.resolution, 'fittingBundle.camera.resolution');
    if (resolution.length !== 2) throw new Error('fittingBundle.camera.resolution must have two values');
    resolution.forEach((value, index) => integer(value, `fittingBundle.camera.resolution[${index}]`, 1));

    const observations = readJson(observationsPath, 'observations');
    if (observations.schema !== OBSERVATION_SCHEMA) {
        throw new Error(`observations.schema must be ${OBSERVATION_SCHEMA}`);
    }
    const provenance = object(observations.provenance, 'observations.provenance');
    const immutableManifestSha256 = sha256File(immutableManifestPath);
    if (sha256(provenance.immutable_manifest_sha256, 'observations.provenance.immutable_manifest_sha256') !== immutableManifestSha256) {
        throw new Error('observations immutable manifest SHA-256 pin does not match bundle');
    }
    if (sha256(provenance.bundle_sha256, 'observations.provenance.bundle_sha256') !== fittingBundleEntry.sha256) {
        throw new Error('observations bundle SHA-256 pin does not match fitting bundle');
    }
    const sourceVideoSha256 = sha256(provenance.source_video_sha256, 'observations.provenance.source_video_sha256');

    return {
        bundleDirectory,
        observationsPath,
        fittingBundlePath,
        immutableManifestPath,
        skeletonPath,
        surfaceAnchorsPath,
        fittingBundle,
        immutableManifest,
        skeleton: readJson(skeletonPath, 'sourceSkeleton'),
        surfaceAnchors: readJson(surfaceAnchorsPath, 'surfaceAnchors'),
        observations,
        integrity: {
            immutableManifestSha256,
            fittingBundleSha256: fittingBundleEntry.sha256,
            skeletonSha256: byFilename.get(skeletonFilename).sha256,
            surfaceAnchorsSha256: byFilename.get(surfaceAnchorsFilename).sha256,
            observationsSha256: sha256File(observationsPath),
            sourceVideoSha256,
            sourceModelSha256,
            bundleFileCount: entries.length,
            bundleTotalBytes: listedTotal,
        },
    };
}

function matrix4(THREE, values, field) {
    const source = array(values, field);
    if (source.length !== 16 || source.some((value) => !Number.isFinite(Number(value)))) {
        throw new Error(`${field} must contain 16 finite values`);
    }
    return new THREE.Matrix4().set(...source.map(Number));
}

function buildBundleModel(THREE, sourceSkeleton) {
    const armatures = array(sourceSkeleton.armatures, 'sourceSkeleton.armatures');
    if (armatures.length !== 1) throw new Error('sourceSkeleton must contain exactly one armature');
    const armature = object(armatures[0], 'sourceSkeleton.armatures[0]');
    const sourceBones = array(armature.bones, 'sourceSkeleton.armatures[0].bones');
    if (!sourceBones.length) throw new Error('sourceSkeleton armature has no bones');
    const model = new THREE.Group();
    model.name = 'AutoRig_Browser_Fit_Immutable_Bundle';
    const armatureMatrix = matrix4(THREE, armature.matrix_world, 'armature.matrix_world');
    armatureMatrix.decompose(model.position, model.quaternion, model.scale);
    const bones = new Map();
    sourceBones.forEach((sourceValue, index) => {
        const source = object(sourceValue, `armature.bones[${index}]`);
        const name = nonEmptyString(source.name, `armature.bones[${index}].name`);
        if (bones.has(name)) throw new Error(`sourceSkeleton has duplicate bone ${name}`);
        const bone = new THREE.Bone();
        bone.name = name;
        bone.userData.use_deform = source.use_deform === true;
        const tailWorld = new THREE.Vector3(...array(source.tail_local, `${name}.tail_local`).map(Number))
            .applyMatrix4(armatureMatrix);
        bone.userData.tailWorld = tailWorld.toArray();
        const localValues = source.parent ? source.parent_relative_matrix : source.matrix_local;
        matrix4(THREE, localValues, `${name}.local`).decompose(bone.position, bone.quaternion, bone.scale);
        bones.set(name, bone);
    });
    sourceBones.forEach((source) => {
        const bone = bones.get(source.name);
        if (source.parent) {
            const parent = bones.get(source.parent);
            if (!parent) throw new Error(`sourceSkeleton bone ${source.name} has missing parent ${source.parent}`);
            parent.add(bone);
        } else {
            model.add(bone);
        }
    });
    model.updateWorldMatrix(true, true);
    let maximumHeadReconstructionErrorWorld = 0;
    sourceBones.forEach((source) => {
        const expected = new THREE.Vector3(...array(source.head_local, `${source.name}.head_local`).map(Number))
            .applyMatrix4(armatureMatrix);
        const actual = bones.get(source.name).getWorldPosition(new THREE.Vector3());
        maximumHeadReconstructionErrorWorld = Math.max(
            maximumHeadReconstructionErrorWorld,
            actual.distanceTo(expected),
        );
    });
    return {
        model,
        bones,
        armatureName: nonEmptyString(armature.name, 'armature.name'),
        sourceBoneCount: sourceBones.length,
        maximumHeadReconstructionErrorWorld,
    };
}

function buildBundleCamera(THREE, fittingBundle) {
    const contract = object(fittingBundle.camera, 'fittingBundle.camera');
    const [widthValue, heightValue] = array(contract.resolution, 'camera.resolution');
    const width = integer(widthValue, 'camera.resolution[0]', 1);
    const height = integer(heightValue, 'camera.resolution[1]', 1);
    const intrinsics = object(contract.intrinsics, 'camera.intrinsics');
    const fx = positive(intrinsics.fx, 'camera.intrinsics.fx');
    const fy = positive(intrinsics.fy, 'camera.intrinsics.fy');
    const cx = finite(intrinsics.cx, 'camera.intrinsics.cx');
    const cy = finite(intrinsics.cy, 'camera.intrinsics.cy');
    const near = 0.01;
    const far = 1000;
    const camera = new THREE.PerspectiveCamera();
    camera.matrixAutoUpdate = false;
    camera.matrix.copy(matrix4(THREE, contract.camera_to_world, 'camera.camera_to_world'));
    camera.matrixWorld.copy(camera.matrix);
    camera.matrixWorldInverse.copy(matrix4(THREE, contract.world_to_camera, 'camera.world_to_camera'));
    const computedInverse = camera.matrixWorld.clone().invert();
    const inverseError = Math.max(...computedInverse.elements.map((value, index) => (
        Math.abs(value - camera.matrixWorldInverse.elements[index])
    )));
    if (inverseError > 1e-4) throw new Error(`camera world matrices disagree by ${inverseError}`);
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

function resolveThreeModule(value) {
    const specifier = nonEmptyString(value, 'threeModule');
    if (/^file:/i.test(specifier)) return specifier;
    if (path.isAbsolute(specifier)) return pathToFileURL(existingFile(specifier, 'threeModule')).href;
    if (/^[a-z]+:/i.test(specifier)) {
        throw new Error('threeModule must be a local path or file: URL; network imports are not accepted');
    }
    return pathToFileURL(existingFile(specifier, 'threeModule')).href;
}

function selectedRestSeedAlignment({ THREE, camera, skeleton, surfaceAnchors, observations, prepared }) {
    const anchorWorldById = new Map();
    array(surfaceAnchors.bones, 'surfaceAnchors.bones').forEach((entryValue, entryIndex) => {
        const entry = object(entryValue, `surfaceAnchors.bones[${entryIndex}]`);
        const bone = nonEmptyString(entry.bone, `surfaceAnchors.bones[${entryIndex}].bone`);
        array(entry.points, `surfaceAnchors.bones[${entryIndex}].points`).forEach((pointValue, pointIndex) => {
            const point = object(pointValue, `surfaceAnchors.${bone}.points[${pointIndex}]`);
            const id = `${bone}:${integer(point.vertex_id, `surfaceAnchors.${bone}.vertex_id`)}`;
            if (anchorWorldById.has(id)) throw new Error(`surfaceAnchors has duplicate anchor ${id}`);
            const world = array(point.world, `surfaceAnchors.${id}.world`).map((value, axis) => finite(value, `${id}.world[${axis}]`));
            if (world.length !== 3) throw new Error(`surfaceAnchors.${id}.world must contain 3 values`);
            anchorWorldById.set(id, world);
        });
    });
    const sourceTrackById = new Map();
    array(observations.tracks, 'observations.tracks').forEach((track) => {
        const id = nonEmptyString(track.id, 'observations track id');
        if (sourceTrackById.has(id)) throw new Error(`observations has duplicate track ${id}`);
        sourceTrackById.set(id, track);
    });
    const projection = createViewerToLtxProjection({
        sourceViewport: skeleton.projection.sourceViewport,
        referenceResolution: skeleton.projection.referenceResolution,
        outputResolution: skeleton.projection.outputResolution,
    });
    const errors = prepared.provenance.browser_rgb_bridge.mappings.map((mapping) => {
        const world = anchorWorldById.get(mapping.sourceAnchorId);
        if (!world) throw new Error(`selected source anchor ${mapping.sourceAnchorId} is absent from surfaceAnchors`);
        const track = sourceTrackById.get(mapping.sourceTrackId);
        if (!track) throw new Error(`selected source track ${mapping.sourceTrackId} is absent from observations`);
        const queryFrame = integer(track.query_frame, `${track.id}.query_frame`);
        const query = object(array(track.points, `${track.id}.points`)[queryFrame], `${track.id}.queryPoint`);
        const ndc = new THREE.Vector3(...world).project(camera);
        const expected = projection.ndcToOutput([ndc.x, ndc.y, ndc.z]);
        const errorPx = Math.hypot(expected[0] - finite(query.x, `${track.id}.query.x`), expected[1] - finite(query.y, `${track.id}.query.y`));
        return {
            semanticAnchorId: mapping.semanticAnchorId,
            sourceAnchorId: mapping.sourceAnchorId,
            sourceTrackId: mapping.sourceTrackId,
            errorPx,
        };
    });
    if (!errors.length) throw new Error('browser bridge selected no RGB anchor mappings');
    return {
        samples: errors.length,
        meanErrorPx: errors.reduce((sum, item) => sum + item.errorPx, 0) / errors.length,
        maximumErrorPx: Math.max(...errors.map((item) => item.errorPx)),
        errors,
    };
}

function gate(name, actual, maximum, results) {
    const passed = Number.isFinite(actual) && actual <= maximum;
    results.push({ name, passed, actual, comparator: '<=', threshold: maximum });
}

/** Evaluate only browser fit/bridge/hierarchy gates, never final gait approval. */
export function evaluateBrowserFitGates({
    maximumHeadReconstructionErrorWorld,
    restSeedAlignment,
    prepared,
    fitted,
    hierarchyQa,
    hierarchyRayCount,
    clipValid,
    allTracksBound,
    minimumTargetSamples,
    gates: gateOverrides = {},
}) {
    const gates = { ...BROWSER_FIT_CANARY_DEFAULTS.gates, ...gateOverrides };
    const results = [];
    gate('head_reconstruction_world', maximumHeadReconstructionErrorWorld, gates.maximumHeadReconstructionErrorWorld, results);
    gate('rest_seed_alignment_px', restSeedAlignment.maximumErrorPx, gates.maximumRestSeedAlignmentErrorPx, results);
    gate('final_mean_target_error_px', fitted.qa.finalMeanTargetErrorPx, gates.maximumFinalMeanTargetErrorPx, results);
    gate('maximum_target_error_px', fitted.qa.maximumTargetErrorPx, gates.maximumTargetErrorPx, results);
    gate('bone_length_error_px', fitted.qa.maximumBoneLengthErrorPx, gates.maximumBoneLengthErrorPx, results);
    gate('joint_limit_violation_rad', fitted.qa.maximumJointLimitViolationRad, gates.maximumJointLimitViolationRad, results);
    gate('contact_slide_px', fitted.qa.maximumContactSlidePx, gates.maximumContactSlidePx, results);
    gate('loop_endpoint_error', fitted.qa.loopEndpointError, gates.maximumLoopEndpointError, results);
    gate('hierarchy_segment_drift_world', hierarchyQa.maximumSegmentLengthDriftWorld, gates.maximumSegmentLengthDriftWorld, results);
    gate('hierarchy_reprojection_error_px', hierarchyQa.maximumHierarchyBakeReprojectionErrorPx, gates.maximumHierarchyBakeReprojectionErrorPx, results);
    gate('requested_fitted_point_error_px', hierarchyQa.maximumRequestedFittedPointErrorPx, gates.maximumRequestedFittedPointErrorPx, results);
    gate(
        'unreachable_pixel_ray_ratio',
        hierarchyQa.unreachablePixelRays / Math.max(hierarchyRayCount, 1),
        gates.maximumUnreachablePixelRayRatio,
        results,
    );
    results.push({
        name: 'target_sample_coverage',
        passed: Number.isFinite(fitted.qa.targetSamples) && fitted.qa.targetSamples >= minimumTargetSamples,
        actual: fitted.qa.targetSamples,
        comparator: '>=',
        threshold: minimumTargetSamples,
    });
    results.push({
        name: 'target_error_improved',
        passed: gates.requireTargetErrorImprovement !== true
            || fitted.qa.finalMeanTargetErrorPx <= fitted.qa.initialMeanTargetErrorPx,
        actual: fitted.qa.finalMeanTargetErrorPx,
        comparator: '<=',
        threshold: fitted.qa.initialMeanTargetErrorPx,
    });
    results.push({
        name: 'ordered_deform_heads',
        passed: gates.requireOrderedDeformHeads !== true || fitted.qa.targetMode === 'ordered_deform_heads',
        actual: fitted.qa.targetMode,
        comparator: '===',
        threshold: 'ordered_deform_heads',
    });
    const contactLimbs = new Set((prepared.contacts || []).map((contact) => String(contact.anchor_id).split('.')[0]));
    results.push({
        name: 'four_limb_contacts',
        passed: gates.requireFourLimbContacts !== true || contactLimbs.size === 4,
        actual: contactLimbs.size,
        comparator: '===',
        threshold: 4,
        enforced: gates.requireFourLimbContacts === true,
    });
    results.push({ name: 'three_clip_validate', passed: clipValid === true, actual: clipValid, comparator: '===', threshold: true });
    results.push({ name: 'three_tracks_bound', passed: allTracksBound === true, actual: allTracksBound, comparator: '===', threshold: true });
    return {
        passed: results.every((result) => result.passed),
        results,
        thresholds: gates,
    };
}

function assertOutputDirectory(outputValue) {
    const outputDirectory = path.resolve(nonEmptyString(outputValue, 'outputDirectory'));
    if (fs.existsSync(outputDirectory)) {
        if (!fs.statSync(outputDirectory).isDirectory()) throw new Error(`outputDirectory is not a directory: ${outputDirectory}`);
        if (fs.readdirSync(outputDirectory).length) {
            throw new Error(`outputDirectory must be absent or empty: ${outputDirectory}`);
        }
    }
    return outputDirectory;
}

function writeJsonAtomic(filename, value) {
    const temporary = `${filename}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
    fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
    fs.renameSync(temporary, filename);
}

function serializeThreeClip(THREE, clip) {
    if (typeof THREE.AnimationClip?.toJSON !== 'function') {
        throw new Error('THREE.AnimationClip.toJSON() is required to emit a hierarchy clip');
    }
    return THREE.AnimationClip.toJSON(clip);
}

/** Run the reusable Node/V8 implementation of the browser fitting canary. */
export async function runBrowserFitCanary(configuration, dependencies = {}) {
    const config = object(configuration, 'configuration');
    const outputDirectory = assertOutputDirectory(config.outputDirectory);
    const validated = validateImmutableInputs({
        bundleDirectory: config.bundleDirectory,
        observationsPath: config.observationsPath,
    });
    const THREE = dependencies.THREE || await import(resolveThreeModule(config.threeModule));
    const requiredConstructors = ['Matrix4', 'Vector3', 'Group', 'Bone', 'PerspectiveCamera', 'AnimationClip'];
    requiredConstructors.forEach((name) => {
        if (typeof THREE[name] !== 'function') throw new Error(`threeModule is missing THREE.${name}`);
    });
    const modelState = buildBundleModel(THREE, validated.skeleton);
    const camera = buildBundleCamera(THREE, validated.fittingBundle);
    const observations = validated.observations;
    const frameCount = integer(observations.frame_count, 'observations.frame_count', 2);
    const width = integer(observations.width, 'observations.width', 1);
    const height = integer(observations.height, 'observations.height', 1);
    const minimumVisibleRatio = positive(
        config.minimumVisibleRatio ?? BROWSER_FIT_CANARY_DEFAULTS.minimumVisibleRatio,
        'minimumVisibleRatio',
    );
    if (minimumVisibleRatio > 1) throw new Error('minimumVisibleRatio must be <= 1');
    const minimumVisiblePoints = Math.ceil(frameCount * minimumVisibleRatio);
    const positionMappings = config.positionMappings ?? BROWSER_FIT_CANARY_DEFAULTS.positionMappings;
    if (!['auto', 'all', false].includes(positionMappings)) {
        throw new Error('positionMappings must be auto, all, or false');
    }
    const skeleton = buildHorse2BrowserFittingSkeleton({
        THREE,
        model: modelState.model,
        camera,
        sourceViewport: validated.fittingBundle.camera.resolution,
        referenceResolution: validated.fittingBundle.camera.resolution,
        outputResolution: [width, height],
        includePositionMappings: positionMappings,
    });
    const cameraContract = {
        outputResolution: [width, height],
        bundleSha256: validated.integrity.fittingBundleSha256,
        immutableManifestSha256: validated.integrity.immutableManifestSha256,
    };
    const prepared = prepareRgbObservationsForBrowser({
        observations,
        skeleton,
        cameraContract,
        minimumVisiblePoints,
    });
    const restSeedAlignment = selectedRestSeedAlignment({
        THREE,
        camera,
        skeleton,
        surfaceAnchors: validated.surfaceAnchors,
        observations,
        prepared,
    });
    const fitOptions = {
        ...BROWSER_FIT_CANARY_DEFAULTS.fit,
        ...(config.fit || {}),
    };
    const fitted = fitBrowserAnimation({ skeleton, observations: prepared, options: fitOptions });
    const clipName = nonEmptyString(config.clipName || 'Horse_LTX_Browser_Fit_Canary', 'clipName');
    const hierarchy = bakeFittedAnimationToThreeHierarchyClip({
        THREE,
        model: modelState.model,
        camera,
        skeleton,
        fitted,
        outputResolution: [width, height],
        name: clipName,
    });
    const clipValid = typeof hierarchy.clip.validate === 'function' && hierarchy.clip.validate() === true;
    const boneNames = new Set();
    modelState.model.traverse((node) => {
        if (node.isBone === true || node.type === 'Bone') {
            if (boneNames.has(node.name)) throw new Error(`real bundle contains duplicate bone ${node.name}`);
            boneNames.add(node.name);
        }
    });
    const trackNames = new Set();
    let allTracksBound = true;
    hierarchy.clip.tracks.forEach((track) => {
        if (trackNames.has(track.name)) throw new Error(`hierarchy clip contains duplicate track ${track.name}`);
        trackNames.add(track.name);
        const separator = track.name.lastIndexOf('.');
        if (separator < 1 || !boneNames.has(track.name.slice(0, separator))) allTracksBound = false;
        if (!Array.from(track.values).every(Number.isFinite)) {
            throw new Error(`hierarchy clip track ${track.name} contains non-finite values`);
        }
    });
    const mappingCount = prepared.provenance.browser_rgb_bridge.mappings.length;
    const minimumTargetSamples = mappingCount * minimumVisiblePoints;
    const hierarchyRayCount = fitted.frameCount * Object.values(skeleton.limbs)
        .reduce((sum, limb) => sum + limb.sourceBoneChain.length - 1, 0);
    const gateEvaluation = evaluateBrowserFitGates({
        maximumHeadReconstructionErrorWorld: modelState.maximumHeadReconstructionErrorWorld,
        restSeedAlignment,
        prepared,
        fitted,
        hierarchyQa: hierarchy.qa,
        hierarchyRayCount,
        clipValid,
        allTracksBound,
        minimumTargetSamples,
        gates: config.gates,
    });
    const bridgeReport = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        status: 'VALIDATED',
        browserOnly: true,
        blenderUsed: false,
        inputs: {
            bundleDirectory: validated.bundleDirectory,
            observationsPath: validated.observationsPath,
            ...validated.integrity,
        },
        camera: prepared.provenance.browser_rgb_bridge.camera,
        mappingMode: prepared.provenance.browser_rgb_bridge.mappingMode,
        minimumVisibleRatio,
        minimumVisiblePoints,
        sourceTrackCount: observations.tracks.length,
        selectedTrackCount: prepared.tracks.length,
        mappings: prepared.provenance.browser_rgb_bridge.mappings,
        sourceContacts: Array.isArray(observations.contacts) ? observations.contacts.length : 0,
        preparedContacts: prepared.contacts.length,
        restSeedAlignment,
    };
    const fitSummary = {
        schema: 'autorig-browser-fit-canary-summary.v1',
        status: gateEvaluation.passed ? 'PASS_BROWSER_FIT_GATES' : 'FAIL_BROWSER_FIT_GATES',
        browserOnly: true,
        blenderUsed: false,
        approvedForAnimationLibrary: false,
        approvalExclusions: [
            'gait_semantics_and_phase_order',
            'fixed_camera_visual_phase_qa',
            'target_mesh_deformation_qa',
        ],
        runtime: {
            node: process.version,
            threeRevision: String(THREE.REVISION || 'unknown'),
        },
        inputs: bridgeReport.inputs,
        realBundle: {
            armature: modelState.armatureName,
            sourceBoneCount: modelState.sourceBoneCount,
            fittedChainBones: new Set(Object.values(skeleton.limbs).flatMap((limb) => limb.sourceBoneChain)).size,
            maximumHeadReconstructionErrorWorld: modelState.maximumHeadReconstructionErrorWorld,
            positionMappingPolicy: skeleton.provenance.positionMappings,
        },
        observations: {
            frameCount,
            fps: observations.fps,
            mappingMode: bridgeReport.mappingMode,
            selectedTrackCount: prepared.tracks.length,
            contactCount: prepared.contacts.length,
            restSeedAlignment,
        },
        fit: {
            options: fitOptions,
            qa: fitted.qa,
            frameCount: fitted.frameCount,
            durationSeconds: fitted.durationSeconds,
            quaternionTracks: fitted.tracks.length,
            positionTracks: fitted.positionTracks.length,
        },
        hierarchyClip: {
            name: hierarchy.clip.name,
            durationSeconds: hierarchy.clip.duration,
            tracks: hierarchy.clip.tracks.length,
            validate: clipValid,
            allTracksBound,
            qa: hierarchy.qa,
            segmentRayCount: hierarchyRayCount,
        },
        gates: gateEvaluation,
    };

    fs.mkdirSync(outputDirectory, { recursive: true });
    const bridgeReportPath = path.join(outputDirectory, 'bridge-report.json');
    const fitSummaryPath = path.join(outputDirectory, 'fit-summary.json');
    writeJsonAtomic(bridgeReportPath, bridgeReport);
    writeJsonAtomic(fitSummaryPath, fitSummary);
    const outputs = { bridgeReportPath, fitSummaryPath };
    if (gateEvaluation.passed && config.emitFittedAnimation === true) {
        outputs.fittedAnimationPath = path.join(outputDirectory, 'fitted-animation.json');
        writeJsonAtomic(outputs.fittedAnimationPath, fitted);
    }
    if (gateEvaluation.passed && config.emitThreeClip === true) {
        outputs.threeClipPath = path.join(outputDirectory, 'three-clip.json');
        writeJsonAtomic(outputs.threeClipPath, serializeThreeClip(THREE, hierarchy.clip));
    }
    return { passed: gateEvaluation.passed, bridgeReport, fitSummary, outputs };
}

function helpText() {
    return `Usage:
  node browser_fit_canary.mjs --bundle-dir DIR --observations FILE \\
    --three-module FILE --output-dir EMPTY_DIR [options]

Required:
  --bundle-dir DIR               Immutable actionless fitting bundle
  --observations FILE            TAPNext++/SAM2 observations JSON
  --three-module FILE            Local Three.js ESM module (no network URL)
  --output-dir DIR               Must be absent or empty

Options:
  --clip-name NAME
  --minimum-visible-ratio N      Default 0.7
  --position-mappings MODE       auto, all, or disabled
  --iterations N                 Default 64
  --tolerance N                  Default 0.05
  --joint-attraction N           Default 0.15
  --smoothing-radius N           Default 1
  --loop-blend-frames N          Default 4
  --no-loop
  --require-four-limb-contacts
  --allow-legacy-three-track
  --max-final-mean-target-error-px N
  --max-target-error-px N
  --max-requested-point-error-px N
  --emit-fitted-animation        Written only when all browser-fit gates PASS
  --emit-three-clip              Written only when all browser-fit gates PASS
  --help

Exit codes: 0 browser-fit gates PASS, 2 invalid input/runtime, 3 QA gates FAIL.
This command never grants final animation approval; gait, visual, and target-mesh
QA remain separate release gates.`;
}

function optionValue(argv, index, flag) {
    if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
    return argv[index + 1];
}

export function parseCanaryArgs(argv) {
    const config = { fit: {}, gates: {} };
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        const take = () => {
            const value = optionValue(argv, index, flag);
            index += 1;
            return value;
        };
        if (flag === '--help') help = true;
        else if (flag === '--bundle-dir') config.bundleDirectory = take();
        else if (flag === '--observations') config.observationsPath = take();
        else if (flag === '--three-module') config.threeModule = take();
        else if (flag === '--output-dir') config.outputDirectory = take();
        else if (flag === '--clip-name') config.clipName = take();
        else if (flag === '--minimum-visible-ratio') config.minimumVisibleRatio = positive(take(), flag);
        else if (flag === '--position-mappings') {
            const value = take();
            if (!['auto', 'all', 'disabled'].includes(value)) throw new Error(`${flag} must be auto, all, or disabled`);
            config.positionMappings = value === 'disabled' ? false : value;
        } else if (flag === '--iterations') config.fit.iterations = integer(take(), flag, 1);
        else if (flag === '--tolerance') config.fit.tolerance = positive(take(), flag);
        else if (flag === '--joint-attraction') config.fit.jointAttraction = finite(take(), flag);
        else if (flag === '--smoothing-radius') config.fit.smoothingRadius = integer(take(), flag);
        else if (flag === '--loop-blend-frames') config.fit.loopBlendFrames = integer(take(), flag, 1);
        else if (flag === '--no-loop') config.fit.loop = false;
        else if (flag === '--require-four-limb-contacts') config.gates.requireFourLimbContacts = true;
        else if (flag === '--allow-legacy-three-track') config.gates.requireOrderedDeformHeads = false;
        else if (flag === '--max-final-mean-target-error-px') config.gates.maximumFinalMeanTargetErrorPx = positive(take(), flag);
        else if (flag === '--max-target-error-px') config.gates.maximumTargetErrorPx = positive(take(), flag);
        else if (flag === '--max-requested-point-error-px') config.gates.maximumRequestedFittedPointErrorPx = positive(take(), flag);
        else if (flag === '--emit-fitted-animation') config.emitFittedAnimation = true;
        else if (flag === '--emit-three-clip') config.emitThreeClip = true;
        else throw new Error(`unknown option ${flag}`);
    }
    if (help) return { help: true };
    ['bundleDirectory', 'observationsPath', 'threeModule', 'outputDirectory'].forEach((field) => {
        if (!config[field]) throw new Error(`missing required option ${field}`);
    });
    return config;
}

export async function runCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseCanaryArgs(argv);
        if (config.help) {
            streams.stdout.write(`${helpText()}\n`);
            return 0;
        }
        const result = await runBrowserFitCanary(config);
        streams.stdout.write(`${JSON.stringify({
            status: result.fitSummary.status,
            outputs: result.outputs,
            failedGates: result.fitSummary.gates.results.filter((gateResult) => !gateResult.passed).map((gateResult) => gateResult.name),
        })}\n`);
        return result.passed ? 0 : 3;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedAsScript = process.argv[1]
    && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;
if (invokedAsScript) process.exitCode = await runCli();

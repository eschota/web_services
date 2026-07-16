#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import {
    applyC1PeriodicClosureToTrackSet,
    BROWSER_FITTING_SCHEMAS,
    fitBrowserAnimation,
} from '../../static/js/animation-fitting-browser-core.js';
import { prepareRgbObservationsForBrowser } from '../../static/js/animation-fitting-rgb-observation-bridge.js';
import { fitBrowserAnimationWithPinnedHoofContacts } from '../../static/js/animation-fitting-hoof-contact-inference.js';
import {
    bakeFittedAnimationToThreeHierarchyClip,
    buildHorse2BrowserFittingSkeleton,
    createViewerToLtxProjection,
} from '../../static/js/animation-fitting-three-adapter.js';

const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const IMMUTABLE_MANIFEST_SCHEMA = 'autorig-fitting-immutable-copy.v1';
const FITTING_BUNDLE_SCHEMA = 'autorig-actionless-fitting-bundle.v1';
const OBSERVATION_SCHEMA = 'autorig-fitting-observations.v1';
const LOOP_VELOCITY_SEAM_SCHEMA = 'autorig-browser-loop-velocity-seam.v1';
const FLOAT32_LOOP_INVARIANT_GATE_SCHEMA = 'autorig-browser-float32-loop-velocity-invariant-gate.v1';

export const BROWSER_FIT_CANARY_DEFAULTS = Object.freeze({
    minimumVisibleRatio: 0.7,
    minimumVisibleConfidence: null,
    maximumRestSegmentScale: null,
    c1ClosureWindow: null,
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
        maximumQuaternionAngularVelocitySeamRadPerSecond: null,
        maximumPositionVelocitySeamWorldPerSecond: null,
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

function nonNegative(value, field) {
    const result = finite(value, field);
    if (result < 0) throw new Error(`${field} must not be negative`);
    return result;
}

function unitInterval(value, field) {
    const result = finite(value, field);
    if (result < 0 || result > 1) throw new Error(`${field} must be between 0 and 1`);
    return result;
}

function maximumScale(value, field) {
    const result = finite(value, field);
    if (result < 1) throw new Error(`${field} must be at least 1`);
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

function normalizeQuaternion(values, field) {
    const result = values.map((value, index) => finite(value, `${field}[${index}]`));
    const length = Math.hypot(...result);
    if (!(length > 1e-12)) throw new Error(`${field} must be a nonzero quaternion`);
    return result.map((value) => value / length);
}

function multiplyQuaternions(first, second) {
    return [
        first[3] * second[0] + first[0] * second[3] + first[1] * second[2] - first[2] * second[1],
        first[3] * second[1] - first[0] * second[2] + first[1] * second[3] + first[2] * second[0],
        first[3] * second[2] + first[0] * second[1] - first[1] * second[0] + first[2] * second[3],
        first[3] * second[3] - first[0] * second[0] - first[1] * second[1] - first[2] * second[2],
    ];
}

function quaternionAngularVelocity(firstValue, secondValue, deltaSeconds, field) {
    const first = normalizeQuaternion(firstValue, `${field}.first`);
    const second = normalizeQuaternion(secondValue, `${field}.second`);
    let delta = normalizeQuaternion(multiplyQuaternions(
        [-first[0], -first[1], -first[2], first[3]],
        second,
    ), `${field}.delta`);
    if (delta[3] < 0) delta = delta.map((value) => -value);
    const sine = Math.hypot(delta[0], delta[1], delta[2]);
    if (sine <= 1e-12) return [0, 0, 0];
    const angle = 2 * Math.atan2(sine, Math.min(1, Math.max(-1, delta[3])));
    return delta.slice(0, 3).map((value) => (value / sine) * angle / deltaSeconds);
}

function vectorVelocity(first, second, deltaSeconds, field) {
    return first.map((value, index) => (
        finite(second[index], `${field}.second[${index}]`)
        - finite(value, `${field}.first[${index}]`)
    ) / deltaSeconds);
}

function vectorDistance(first, second) {
    return Math.hypot(...first.map((value, index) => value - second[index]));
}

/** Measure one-sided first/last interval velocity continuity on a baked Three clip. */
export function measureLoopVelocitySeam(clipValue) {
    const clip = object(clipValue, 'clip');
    if (!Array.isArray(clip.tracks) || !clip.tracks.length) throw new Error('clip.tracks must not be empty');
    const metrics = {
        schema: LOOP_VELOCITY_SEAM_SCHEMA,
        method: 'local_shortest_path_rotation_vector_and_position_first_last_interval_difference',
        coordinateSpace: 'bone_local',
        positionUnits: 'model_local_units',
        quaternionAngularVelocitySeamRadPerSecond: {
            sampleCount: 0,
            maximum: 0,
            trackName: null,
            startVelocity: null,
            endVelocity: null,
        },
        positionVelocitySeamWorldPerSecond: {
            sampleCount: 0,
            maximum: 0,
            trackName: null,
            startVelocity: null,
            endVelocity: null,
        },
        quaternionPoseSeamRad: { sampleCount: 0, maximum: 0, trackName: null },
        positionPoseSeamWorld: { sampleCount: 0, maximum: 0, trackName: null },
    };
    clip.tracks.forEach((trackValue, trackIndex) => {
        const track = object(trackValue, `clip.tracks[${trackIndex}]`);
        const name = nonEmptyString(track.name, `clip.tracks[${trackIndex}].name`);
        const quaternion = name.endsWith('.quaternion');
        const position = name.endsWith('.position');
        if (!quaternion && !position) throw new Error(`unsupported loop velocity track ${name}`);
        const times = Array.from(track.times || [], (value, index) => finite(value, `${name}.times[${index}]`));
        if (times.length < 3) throw new Error(`${name}.times must contain at least 3 samples`);
        const startDeltaSeconds = times[1] - times[0];
        const endDeltaSeconds = times.at(-1) - times.at(-2);
        if (!(startDeltaSeconds > 0) || !(endDeltaSeconds > 0)) {
            throw new Error(`${name}.times must increase at both loop boundaries`);
        }
        const itemSize = quaternion ? 4 : 3;
        const values = Array.from(track.values || [], (value, index) => finite(value, `${name}.values[${index}]`));
        if (values.length !== times.length * itemSize) throw new Error(`${name}.values do not match its timeline`);
        const sample = (index) => values.slice(index * itemSize, (index + 1) * itemSize);
        const poseSeam = quaternion
            ? Math.hypot(...quaternionAngularVelocity(
                sample(0),
                sample(times.length - 1),
                1,
                `${name}.poseSeam`,
            ))
            : vectorDistance(sample(0), sample(times.length - 1));
        const poseOutput = quaternion ? metrics.quaternionPoseSeamRad : metrics.positionPoseSeamWorld;
        poseOutput.sampleCount += 1;
        if (poseOutput.trackName == null || poseSeam > poseOutput.maximum) {
            poseOutput.maximum = poseSeam;
            poseOutput.trackName = name;
        }
        const startVelocity = quaternion
            ? quaternionAngularVelocity(sample(0), sample(1), startDeltaSeconds, `${name}.startVelocity`)
            : vectorVelocity(sample(0), sample(1), startDeltaSeconds, `${name}.startVelocity`);
        const endVelocity = quaternion
            ? quaternionAngularVelocity(sample(times.length - 2), sample(times.length - 1), endDeltaSeconds, `${name}.endVelocity`)
            : vectorVelocity(sample(times.length - 2), sample(times.length - 1), endDeltaSeconds, `${name}.endVelocity`);
        const seam = vectorDistance(startVelocity, endVelocity);
        const output = quaternion
            ? metrics.quaternionAngularVelocitySeamRadPerSecond
            : metrics.positionVelocitySeamWorldPerSecond;
        output.sampleCount += 1;
        if (output.trackName == null || seam > output.maximum) {
            output.maximum = seam;
            output.trackName = name;
            output.startVelocity = startVelocity;
            output.endVelocity = endVelocity;
        }
    });
    return metrics;
}

/**
 * Derive a non-artistic Float32 continuity tolerance from the clip's stored
 * precision and boundary sampling interval. sqrt(epsilon) is the conventional
 * forward-error scale for stable nonlinear operations such as quaternion
 * normalize/log; positions additionally scale by their largest local value.
 */
export function deriveFloat32LoopVelocityInvariantGate(clipValue) {
    const clip = object(clipValue, 'clip');
    if (!Array.isArray(clip.tracks) || !clip.tracks.length) throw new Error('clip.tracks must not be empty');
    let minimumBoundaryDeltaSeconds = Number.POSITIVE_INFINITY;
    let maximumAbsoluteLocalPosition = 0;
    let quaternionTrackCount = 0;
    let positionTrackCount = 0;
    clip.tracks.forEach((trackValue, trackIndex) => {
        const track = object(trackValue, `clip.tracks[${trackIndex}]`);
        const name = nonEmptyString(track.name, `clip.tracks[${trackIndex}].name`);
        const quaternion = name.endsWith('.quaternion');
        const position = name.endsWith('.position');
        if (!quaternion && !position) throw new Error(`unsupported loop invariant track ${name}`);
        const times = Array.from(track.times || [], (value, index) => finite(value, `${name}.times[${index}]`));
        if (times.length < 3) throw new Error(`${name}.times must contain at least 3 samples`);
        const startDeltaSeconds = times[1] - times[0];
        const endDeltaSeconds = times.at(-1) - times.at(-2);
        if (!(startDeltaSeconds > 0) || !(endDeltaSeconds > 0)) {
            throw new Error(`${name}.times must increase at both loop boundaries`);
        }
        minimumBoundaryDeltaSeconds = Math.min(
            minimumBoundaryDeltaSeconds,
            startDeltaSeconds,
            endDeltaSeconds,
        );
        if (quaternion) {
            quaternionTrackCount += 1;
        } else {
            positionTrackCount += 1;
            Array.from(track.values || [], (value, index) => finite(value, `${name}.values[${index}]`))
                .forEach((value) => { maximumAbsoluteLocalPosition = Math.max(maximumAbsoluteLocalPosition, Math.abs(value)); });
        }
    });
    const machineEpsilon = 2 ** -23;
    const relativeTolerance = Math.sqrt(machineEpsilon);
    const positionMagnitudeScale = Math.max(1, maximumAbsoluteLocalPosition);
    return {
        schema: FLOAT32_LOOP_INVARIANT_GATE_SCHEMA,
        enabled: true,
        precision: 'IEEE_754_binary32_clip_storage',
        machineEpsilon,
        relativeTolerance,
        relativeToleranceFormula: 'sqrt(2^-23)',
        minimumBoundaryDeltaSeconds,
        maximumAbsoluteLocalPosition,
        positionMagnitudeScale,
        thresholdFormula: 'sqrt(binary32_machine_epsilon) * magnitude_scale / minimum_boundary_delta_seconds',
        quaternionTrackCount,
        positionTrackCount,
        maximumQuaternionAngularVelocitySeamRadPerSecond:
            relativeTolerance / minimumBoundaryDeltaSeconds,
        maximumPositionVelocitySeamWorldPerSecond:
            relativeTolerance * positionMagnitudeScale / minimumBoundaryDeltaSeconds,
    };
}

function loopVelocityGateContract(gatesValue = {}) {
    const gates = object(gatesValue, 'gates');
    const quaternion = gates.maximumQuaternionAngularVelocitySeamRadPerSecond;
    const position = gates.maximumPositionVelocitySeamWorldPerSecond;
    if ((quaternion == null) !== (position == null)) {
        throw new Error('quaternion and position loop velocity seam thresholds must be provided together');
    }
    return {
        enabled: quaternion != null,
        maximumQuaternionAngularVelocitySeamRadPerSecond:
            quaternion == null ? null : nonNegative(quaternion, 'maximumQuaternionAngularVelocitySeamRadPerSecond'),
        maximumPositionVelocitySeamWorldPerSecond:
            position == null ? null : nonNegative(position, 'maximumPositionVelocitySeamWorldPerSecond'),
    };
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
    loopVelocitySeam = null,
    gates: gateOverrides = {},
}) {
    const gates = { ...BROWSER_FIT_CANARY_DEFAULTS.gates, ...gateOverrides };
    const loopVelocityGate = loopVelocityGateContract(gates);
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
    if (loopVelocityGate.enabled) {
        const seam = object(loopVelocitySeam, 'loopVelocitySeam');
        gate(
            'quaternion_angular_velocity_seam_rad_per_second',
            seam.quaternionAngularVelocitySeamRadPerSecond?.maximum,
            loopVelocityGate.maximumQuaternionAngularVelocitySeamRadPerSecond,
            results,
        );
        gate(
            'position_velocity_seam_world_per_second',
            seam.positionVelocitySeamWorldPerSecond?.maximum,
            loopVelocityGate.maximumPositionVelocitySeamWorldPerSecond,
            results,
        );
    }
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

function writeJsonSetFromStaging(entries) {
    const staged = entries.map(({ filename, value }) => ({
        filename,
        temporary: `${filename}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`,
        contents: `${JSON.stringify(value, null, 2)}\n`,
    }));
    const published = [];
    try {
        staged.forEach((item) => fs.writeFileSync(item.temporary, item.contents, { encoding: 'utf8', flag: 'wx' }));
        staged.forEach((item) => {
            fs.renameSync(item.temporary, item.filename);
            published.push(item.filename);
        });
    } catch (error) {
        staged.forEach((item) => {
            if (fs.existsSync(item.temporary)) fs.rmSync(item.temporary, { force: true });
        });
        published.forEach((filename) => fs.rmSync(filename, { force: true }));
        throw error;
    }
}

function serializeThreeClip(THREE, clip) {
    if (typeof THREE.AnimationClip?.toJSON !== 'function') {
        throw new Error('THREE.AnimationClip.toJSON() is required to emit a hierarchy clip');
    }
    const serialized = THREE.AnimationClip.toJSON(clip);
    if (clip.userData && Object.keys(clip.userData).length) serialized.userData = clip.userData;
    return serialized;
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
    const minimumVisibleConfidence = config.minimumVisibleConfidence == null
        ? BROWSER_FIT_CANARY_DEFAULTS.minimumVisibleConfidence
        : unitInterval(config.minimumVisibleConfidence, 'minimumVisibleConfidence');
    const maximumRestSegmentScale = config.maximumRestSegmentScale == null
        ? BROWSER_FIT_CANARY_DEFAULTS.maximumRestSegmentScale
        : maximumScale(config.maximumRestSegmentScale, 'maximumRestSegmentScale');
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
        minimumVisibleConfidence,
        maximumRestSegmentScale,
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
    const c1ClosureWindow = config.c1ClosureWindow == null
        ? BROWSER_FIT_CANARY_DEFAULTS.c1ClosureWindow
        : integer(config.c1ClosureWindow, 'c1ClosureWindow', 1);
    const useFloat32LoopVelocityInvariantGates = config.float32LoopVelocityInvariantGates === true;
    const explicitLoopVelocityGate = loopVelocityGateContract({
        ...BROWSER_FIT_CANARY_DEFAULTS.gates,
        ...(config.gates || {}),
    });
    if (useFloat32LoopVelocityInvariantGates && explicitLoopVelocityGate.enabled) {
        throw new Error('Float32-derived and explicit loop velocity seam thresholds are mutually exclusive');
    }
    if (useFloat32LoopVelocityInvariantGates && c1ClosureWindow == null) {
        throw new Error('Float32 loop velocity invariant gates require C1 periodic closure');
    }
    if ((explicitLoopVelocityGate.enabled || useFloat32LoopVelocityInvariantGates) && fitOptions.loop === false) {
        throw new Error('loop velocity seam thresholds require loop fitting');
    }
    if (c1ClosureWindow != null && fitOptions.loop === false) {
        throw new Error('C1 periodic closure requires loop fitting');
    }
    let preparedForFit = prepared;
    let contactRefit = null;
    let fitted;
    if (dependencies.contactRefit != null) {
        const contract = object(dependencies.contactRefit, 'dependencies.contactRefit');
        contactRefit = fitBrowserAnimationWithPinnedHoofContacts({
            skeleton,
            observations: prepared,
            schedule: object(contract.schedule, 'dependencies.contactRefit.schedule'),
            pins: object(contract.pins, 'dependencies.contactRefit.pins'),
            fitOptions,
            gaitQaOptions: contract.gaitQaOptions || {},
        });
        preparedForFit = contactRefit.observations;
        fitted = contactRefit.fitted;
    } else {
        fitted = fitBrowserAnimation({ skeleton, observations: prepared, options: fitOptions });
    }
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
    const c1PeriodicClosure = c1ClosureWindow == null
        ? {
            schema: BROWSER_FITTING_SCHEMAS.c1PeriodicClosure,
            enabled: false,
            windowFrames: null,
        }
        : applyC1PeriodicClosureToTrackSet({
            tracks: hierarchy.clip.tracks,
            windowFrames: c1ClosureWindow,
        });
    if (c1PeriodicClosure.enabled) {
        hierarchy.clip.userData = {
            ...(hierarchy.clip.userData || {}),
            autorigC1PeriodicClosure: c1PeriodicClosure,
        };
    }
    const clipValid = typeof hierarchy.clip.validate === 'function' && hierarchy.clip.validate() === true;
    const loopVelocitySeam = measureLoopVelocitySeam(hierarchy.clip);
    const float32LoopVelocityInvariantGate = useFloat32LoopVelocityInvariantGates
        ? deriveFloat32LoopVelocityInvariantGate(hierarchy.clip)
        : {
            schema: FLOAT32_LOOP_INVARIANT_GATE_SCHEMA,
            enabled: false,
        };
    const effectiveGateOverrides = {
        ...(config.gates || {}),
        ...(useFloat32LoopVelocityInvariantGates ? {
            maximumQuaternionAngularVelocitySeamRadPerSecond:
                float32LoopVelocityInvariantGate.maximumQuaternionAngularVelocitySeamRadPerSecond,
            maximumPositionVelocitySeamWorldPerSecond:
                float32LoopVelocityInvariantGate.maximumPositionVelocitySeamWorldPerSecond,
        } : {}),
    };
    const loopVelocityGate = {
        ...loopVelocityGateContract({
            ...BROWSER_FIT_CANARY_DEFAULTS.gates,
            ...effectiveGateOverrides,
        }),
        derivation: float32LoopVelocityInvariantGate,
    };
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
        prepared: preparedForFit,
        fitted,
        hierarchyQa: hierarchy.qa,
        hierarchyRayCount,
        clipValid,
        allTracksBound,
        minimumTargetSamples,
        loopVelocitySeam,
        gates: contactRefit
            ? { ...effectiveGateOverrides, requireFourLimbContacts: true }
            : effectiveGateOverrides,
    });
    if (c1PeriodicClosure.enabled) {
        gate(
            'c1_quaternion_pose_seam_rad',
            loopVelocitySeam.quaternionPoseSeamRad.maximum,
            c1PeriodicClosure.poseEpsilon,
            gateEvaluation.results,
        );
        gate(
            'c1_position_pose_seam_world',
            loopVelocitySeam.positionPoseSeamWorld.maximum,
            c1PeriodicClosure.poseEpsilon,
            gateEvaluation.results,
        );
    }
    if (contactRefit) {
        gateEvaluation.results.push({
            name: 'pinned_contact_schedule',
            passed: contactRefit.schedule.status === 'PASS',
            actual: contactRefit.schedule.status,
            comparator: '===',
            threshold: 'PASS',
        });
        gateEvaluation.results.push({
            name: 'semantic_walk_gait',
            passed: contactRefit.gaitQa.accepted === true,
            actual: contactRefit.gaitQa.accepted,
            comparator: '===',
            threshold: true,
        });
        gateEvaluation.results.push({
            name: 'fitted_walk_contact_slide',
            passed: contactRefit.fittedWalkQa.status === 'PASS',
            actual: contactRefit.fittedWalkQa.maximumContactSlideRatio,
            comparator: '<=',
            threshold: contactRefit.fittedWalkQa.thresholdRatio,
        });
    }
    gateEvaluation.passed = gateEvaluation.results.every((result) => result.passed);
    const fittingMode = contactRefit ? 'contact_constrained_refit' : 'unconstrained_diagnostic';
    const bridgeReport = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        status: 'VALIDATED',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode,
        inputs: {
            bundleDirectory: validated.bundleDirectory,
            observationsPath: validated.observationsPath,
            ...validated.integrity,
        },
        camera: prepared.provenance.browser_rgb_bridge.camera,
        mappingMode: prepared.provenance.browser_rgb_bridge.mappingMode,
        minimumVisibleRatio,
        minimumVisiblePoints,
        minimumVisibleConfidence,
        confidenceFilter: prepared.provenance.browser_rgb_bridge.confidenceFilter,
        maximumRestSegmentScale,
        restSegmentConsistencyFilter:
            prepared.provenance.browser_rgb_bridge.restSegmentConsistencyFilter,
        sourceTrackCount: observations.tracks.length,
        selectedTrackCount: prepared.tracks.length,
        mappings: prepared.provenance.browser_rgb_bridge.mappings,
        sourceContacts: Array.isArray(observations.contacts) ? observations.contacts.length : 0,
        preparedContacts: preparedForFit.contacts.length,
        restSeedAlignment,
        loopVelocityGate,
        c1PeriodicClosure,
    };
    const fitSummary = {
        schema: 'autorig-browser-fit-canary-summary.v1',
        status: gateEvaluation.passed
            ? (contactRefit ? 'PASS_BROWSER_CONTACT_REFIT_GATES' : 'PASS_BROWSER_FIT_GATES')
            : (contactRefit ? 'FAIL_BROWSER_CONTACT_REFIT_GATES' : 'FAIL_BROWSER_FIT_GATES'),
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode,
        approvedForBrowserContactFit: Boolean(contactRefit && gateEvaluation.passed),
        approvedForAnimationLibrary: false,
        approvalExclusions: contactRefit
            ? [
                'fixed_camera_visual_phase_qa',
                'target_mesh_deformation_qa',
            ]
            : [
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
            selectedTrackCount: preparedForFit.tracks.length,
            contactCount: preparedForFit.contacts.length,
            confidenceFilter: prepared.provenance.browser_rgb_bridge.confidenceFilter,
            restSegmentConsistencyFilter:
                prepared.provenance.browser_rgb_bridge.restSegmentConsistencyFilter,
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
            qaStage: c1PeriodicClosure.enabled ? 'pre_c1_hierarchy_bake' : 'final_clip',
            segmentRayCount: hierarchyRayCount,
            loopVelocitySeam,
            loopVelocityGate,
            c1PeriodicClosure,
            postC1Validation: c1PeriodicClosure.enabled ? {
                poseSeamGateNames: [
                    'c1_quaternion_pose_seam_rad',
                    'c1_position_pose_seam_world',
                ],
                velocitySeamGateNames: loopVelocityGate.enabled ? [
                    'quaternion_angular_velocity_seam_rad_per_second',
                    'position_velocity_seam_world_per_second',
                ] : [],
                hierarchyBakeQaReevaluation: 'required_by_fixed_camera_visual_phase_qa',
            } : null,
        },
        gates: gateEvaluation,
        ...(contactRefit ? {
            contactRefit: {
                provenance: contactRefit.observations.provenance.browser_hoof_contacts,
                scheduleStatus: contactRefit.schedule.status,
                scheduleSupport: contactRefit.schedule.qa.support,
                inferredTouchdownOrder: contactRefit.schedule.inferredTouchdownOrder,
                semanticGaitQa: contactRefit.gaitQa,
                fittedWalkQa: contactRefit.fittedWalkQa,
            },
        } : {}),
    };

    fs.mkdirSync(outputDirectory, { recursive: true });
    const bridgeReportPath = path.join(outputDirectory, 'bridge-report.json');
    const fitSummaryPath = path.join(outputDirectory, 'fit-summary.json');
    writeJsonAtomic(bridgeReportPath, bridgeReport);
    writeJsonAtomic(fitSummaryPath, fitSummary);
    const outputs = { bridgeReportPath, fitSummaryPath };
    if (gateEvaluation.passed && config.emitFittedAnimation === true && config.emitThreeClip === true) {
        outputs.fittedAnimationPath = path.join(outputDirectory, 'fitted-animation.json');
        outputs.threeClipPath = path.join(outputDirectory, 'three-clip.json');
        writeJsonSetFromStaging([
            { filename: outputs.fittedAnimationPath, value: fitted },
            { filename: outputs.threeClipPath, value: serializeThreeClip(THREE, hierarchy.clip) },
        ]);
    } else if (gateEvaluation.passed && config.emitFittedAnimation === true) {
        outputs.fittedAnimationPath = path.join(outputDirectory, 'fitted-animation.json');
        writeJsonAtomic(outputs.fittedAnimationPath, fitted);
    } else if (gateEvaluation.passed && config.emitThreeClip === true) {
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
  --minimum-visible-confidence N Optional 0..1 threshold; disabled by default
  --maximum-rest-segment-scale N Optional ordered-head outlier limit; disabled by default
  --position-mappings MODE       auto, all, or disabled
  --iterations N                 Default 64
  --tolerance N                  Default 0.05
  --joint-attraction N           Default 0.15
  --smoothing-radius N           Default 1
  --loop-blend-frames N          Default 4
  --c1-closure-window N          Optional local-track C1 loop window; disabled by default
  --float32-loop-velocity-invariant-gates
                                Derive velocity gates from binary32 epsilon and clip scale
  --no-loop
  --require-four-limb-contacts
  --allow-legacy-three-track
  --max-final-mean-target-error-px N
  --max-target-error-px N
  --max-requested-point-error-px N
  --max-quaternion-angular-velocity-seam-rad-per-second N
  --max-position-velocity-seam-world-per-second N
                                Optional pair; velocity gates disabled by default
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
        else if (flag === '--minimum-visible-confidence') config.minimumVisibleConfidence = unitInterval(take(), flag);
        else if (flag === '--maximum-rest-segment-scale') config.maximumRestSegmentScale = maximumScale(take(), flag);
        else if (flag === '--position-mappings') {
            const value = take();
            if (!['auto', 'all', 'disabled'].includes(value)) throw new Error(`${flag} must be auto, all, or disabled`);
            config.positionMappings = value === 'disabled' ? false : value;
        } else if (flag === '--iterations') config.fit.iterations = integer(take(), flag, 1);
        else if (flag === '--tolerance') config.fit.tolerance = positive(take(), flag);
        else if (flag === '--joint-attraction') config.fit.jointAttraction = finite(take(), flag);
        else if (flag === '--smoothing-radius') config.fit.smoothingRadius = integer(take(), flag);
        else if (flag === '--loop-blend-frames') config.fit.loopBlendFrames = integer(take(), flag, 1);
        else if (flag === '--c1-closure-window') config.c1ClosureWindow = integer(take(), flag, 1);
        else if (flag === '--float32-loop-velocity-invariant-gates') {
            config.float32LoopVelocityInvariantGates = true;
        }
        else if (flag === '--no-loop') config.fit.loop = false;
        else if (flag === '--require-four-limb-contacts') config.gates.requireFourLimbContacts = true;
        else if (flag === '--allow-legacy-three-track') config.gates.requireOrderedDeformHeads = false;
        else if (flag === '--max-final-mean-target-error-px') config.gates.maximumFinalMeanTargetErrorPx = positive(take(), flag);
        else if (flag === '--max-target-error-px') config.gates.maximumTargetErrorPx = positive(take(), flag);
        else if (flag === '--max-requested-point-error-px') config.gates.maximumRequestedFittedPointErrorPx = positive(take(), flag);
        else if (flag === '--max-quaternion-angular-velocity-seam-rad-per-second') {
            config.gates.maximumQuaternionAngularVelocitySeamRadPerSecond = nonNegative(take(), flag);
        } else if (flag === '--max-position-velocity-seam-world-per-second') {
            config.gates.maximumPositionVelocitySeamWorldPerSecond = nonNegative(take(), flag);
        }
        else if (flag === '--emit-fitted-animation') config.emitFittedAnimation = true;
        else if (flag === '--emit-three-clip') config.emitThreeClip = true;
        else throw new Error(`unknown option ${flag}`);
    }
    if (help) return { help: true };
    ['bundleDirectory', 'observationsPath', 'threeModule', 'outputDirectory'].forEach((field) => {
        if (!config[field]) throw new Error(`missing required option ${field}`);
    });
    const explicitLoopVelocityGate = loopVelocityGateContract({
        ...BROWSER_FIT_CANARY_DEFAULTS.gates,
        ...config.gates,
    });
    if (config.float32LoopVelocityInvariantGates === true && explicitLoopVelocityGate.enabled) {
        throw new Error('Float32-derived and explicit loop velocity seam thresholds are mutually exclusive');
    }
    if (config.float32LoopVelocityInvariantGates === true && config.c1ClosureWindow == null) {
        throw new Error('Float32 loop velocity invariant gates require --c1-closure-window');
    }
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

#!/usr/bin/env node
/**
 * Browser-only Horse_2 target deformation and fixed-camera visual-phase QA.
 *
 * The command reconstructs the immutable 344-vertex Horse_2 skinned surface in
 * Three.js, evaluates every exact keyframe from a fitted Three AnimationClip,
 * renders fixed-camera PNG evidence plus an MP4, and authors a fail-closed
 * ``autorig.animation-visual-phase-qa.v1`` evidence contract.  It never invokes
 * Blender and never grants animation-library approval: a human decision must be
 * added by a separate reviewed publication step.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import zlib from 'node:zlib';

export const HORSE_VISUAL_PHASE_QA_SCHEMA = 'autorig.animation-visual-phase-qa.v1';
export const HORSE_VISUAL_PHASE_REQUIRED_PHASES = Object.freeze(['start', 'middle', 'three_quarter']);
export const HORSE_VISUAL_PHASE_THRESHOLDS = Object.freeze({
    maximumEdgeStretch: 5,
    p99EdgeStretch: 2.5,
    zeroWeightVertices: 0,
    coincidentRestSeparationM: 0.04,
});
export const HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS = Object.freeze({
    windowFrames: 3,
    maximumP99AdjacentDisplacementModelDiagonal: 0.03,
    maximumMedianAdjacentDisplacementModelDiagonal: 0.01,
    minimumCentroidDropModelHeight: 0.15,
    groundContactToleranceModelHeight: 0.05,
    maximumGroundPenetrationModelHeight: 0.10,
});

const IMMUTABLE_SCHEMA = 'autorig-fitting-immutable-copy.v1';
const BUNDLE_SCHEMA = 'autorig-actionless-fitting-bundle.v1';
const WIDTH = 768;
const HEIGHT = 448;
const SHA256_RE = /^[0-9a-f]{64}$/;

function fail(message) {
    throw new Error(message);
}

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`${field} must be an object`);
    return value;
}

function finite(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) fail(`${field} must be finite`);
    return result;
}

function integer(value, field, minimum = 0) {
    if (!Number.isInteger(value) || value < minimum) fail(`${field} must be an integer >= ${minimum}`);
    return value;
}

function string(value, field) {
    if (typeof value !== 'string' || !value.trim()) fail(`${field} must be a non-empty string`);
    return value;
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function readSnapshot(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    const before = fs.statSync(filename);
    if (!before.isFile()) fail(`${field} must be a file: ${filename}`);
    const buffer = fs.readFileSync(filename);
    const after = fs.statSync(filename);
    if (before.size !== after.size || before.mtimeMs !== after.mtimeMs || buffer.length !== after.size) {
        fail(`${field} changed while it was read`);
    }
    return { path: filename, buffer, bytes: buffer.length, sha256: sha256Buffer(buffer) };
}

function parseJson(snapshot, field) {
    try {
        return object(JSON.parse(snapshot.buffer.toString('utf8')), field);
    } catch (error) {
        fail(`${field} is invalid JSON: ${error.message}`);
    }
}

function parseGzipJson(snapshot, field) {
    let buffer;
    try {
        buffer = zlib.gunzipSync(snapshot.buffer);
    } catch (error) {
        fail(`${field} is invalid gzip: ${error.message}`);
    }
    try {
        return object(JSON.parse(buffer.toString('utf8')), field);
    } catch (error) {
        fail(`${field} contains invalid JSON: ${error.message}`);
    }
}

function safeFilename(value, field) {
    const result = string(value, field);
    if (path.basename(result) !== result || result === '.' || result === '..') {
        fail(`${field} must be a bundle-root filename`);
    }
    return result;
}

function canonicalJsonBuffer(value) {
    return Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeNew(filename, buffer) {
    const descriptor = fs.openSync(filename, 'wx');
    try {
        fs.writeFileSync(descriptor, buffer);
        fs.fsyncSync(descriptor);
    } finally {
        fs.closeSync(descriptor);
    }
}

function pinFile(filename, extra = {}) {
    const snapshot = readSnapshot(filename, filename);
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256, ...extra };
}

function matrixValues(value, field) {
    if (!Array.isArray(value) || value.length !== 16) fail(`${field} must contain 16 numbers`);
    return value.map((entry, index) => finite(entry, `${field}[${index}]`));
}

function vector3(value, field) {
    if (!Array.isArray(value) || value.length !== 3) fail(`${field} must contain 3 numbers`);
    return value.map((entry, index) => finite(entry, `${field}[${index}]`));
}

function validateThreeClip(clip, clipPin, boneNames, { loop = true } = {}) {
    const name = string(clip.name, 'threeClip.name');
    const duration = finite(clip.duration, 'threeClip.duration');
    if (duration <= 0) fail('threeClip.duration must be positive');
    if (!Array.isArray(clip.tracks) || !clip.tracks.length) fail('threeClip.tracks must not be empty');
    const names = new Set();
    let timeline = null;
    let maximumLoopEndpointError = 0;
    const durationTimelineTolerance = Math.max(Number.EPSILON, Math.abs(duration) * (2 ** -23));
    for (const [index, raw] of clip.tracks.entries()) {
        const track = object(raw, `threeClip.tracks[${index}]`);
        const trackName = string(track.name, `threeClip.tracks[${index}].name`);
        if (names.has(trackName)) fail(`threeClip repeats track ${trackName}`);
        names.add(trackName);
        const match = trackName.match(/^(.*)\.(quaternion|position)$/);
        if (!match || !boneNames.has(match[1])) fail(`threeClip track ${trackName} is not bound to the immutable skeleton`);
        if (!Array.isArray(track.times) || track.times.length !== 49) {
            fail(`${trackName}.times must contain the exact 49-frame Horse_2 fitting interval`);
        }
        const times = track.times.map((entry, timeIndex) => finite(entry, `${trackName}.times[${timeIndex}]`));
        if (times[0] !== 0 || Math.abs(times.at(-1) - duration) > durationTimelineTolerance
            || times.some((time, timeIndex) => timeIndex && time <= times[timeIndex - 1])) {
            fail(`${trackName}.times do not preserve a 0..duration chronology`);
        }
        if (timeline && JSON.stringify(timeline) !== JSON.stringify(times)) {
            fail('all Three clip tracks must share the exact fitted timeline');
        }
        timeline ||= times;
        const itemSize = match[2] === 'quaternion' ? 4 : 3;
        if (!Array.isArray(track.values) || track.values.length !== times.length * itemSize) {
            fail(`${trackName}.values do not match its timeline`);
        }
        const values = track.values.map((entry, valueIndex) => finite(entry, `${trackName}.values[${valueIndex}]`));
        const first = values.slice(0, itemSize);
        const last = values.slice(-itemSize);
        const endpointError = match[2] === 'quaternion'
            ? Math.min(
                Math.hypot(...first.map((entry, component) => entry - last[component])),
                Math.hypot(...first.map((entry, component) => entry + last[component])),
            )
            : Math.hypot(...first.map((entry, component) => entry - last[component]));
        maximumLoopEndpointError = Math.max(maximumLoopEndpointError, endpointError);
    }
    if (loop && maximumLoopEndpointError > 1e-5) {
        fail(`Three clip loop endpoint error ${maximumLoopEndpointError} exceeds 1e-5`);
    }
    return {
        name,
        duration,
        timeline,
        frameCount: timeline.length,
        fps: (timeline.length - 1) / duration,
        trackCount: clip.tracks.length,
        loop,
        temporalMode: loop ? 'loop' : 'one_shot',
        maximumLoopEndpointError,
        durationTimelineTolerance,
        pin: clipPin,
    };
}

/** Validate and pin every byte of the canonical Horse_2 bundle and Three clip. */
export function validateHorse2QaInputs({
    bundleDirectory: directoryValue,
    threeClipPath: clipValue,
    expectedImmutableManifestSha256,
    expectedFittingBundleSha256,
    expectedSourceModelSha256,
    expectedLoop = true,
}) {
    for (const [value, field] of [
        [expectedImmutableManifestSha256, 'expectedImmutableManifestSha256'],
        [expectedFittingBundleSha256, 'expectedFittingBundleSha256'],
        [expectedSourceModelSha256, 'expectedSourceModelSha256'],
    ]) if (!SHA256_RE.test(value)) fail(`${field} must be an externally supplied lowercase SHA-256`);
    const bundleDirectory = path.resolve(string(directoryValue, 'bundleDirectory'));
    if (!fs.statSync(bundleDirectory).isDirectory()) fail(`bundleDirectory must be a directory: ${bundleDirectory}`);
    const immutableSnapshot = readSnapshot(path.join(bundleDirectory, 'immutable_manifest.json'), 'immutable manifest');
    if (immutableSnapshot.sha256 !== expectedImmutableManifestSha256) {
        fail('immutable manifest does not match the externally supplied SHA-256');
    }
    const immutable = parseJson(immutableSnapshot, 'immutable manifest');
    if (immutable.schema !== IMMUTABLE_SCHEMA) fail(`immutable manifest schema must be ${IMMUTABLE_SCHEMA}`);
    if (!Array.isArray(immutable.files) || !immutable.files.length) fail('immutable manifest files must not be empty');
    const entries = new Map();
    let totalBytes = 0;
    for (const [index, raw] of immutable.files.entries()) {
        const row = object(raw, `immutable.files[${index}]`);
        const filename = safeFilename(row.filename, `immutable.files[${index}].filename`);
        if (entries.has(filename)) fail(`immutable manifest repeats ${filename}`);
        if (!SHA256_RE.test(row.sha256)) fail(`immutable manifest SHA-256 is invalid for ${filename}`);
        const snapshot = readSnapshot(path.join(bundleDirectory, filename), `bundle file ${filename}`);
        if (snapshot.bytes !== integer(row.bytes, `${filename}.bytes`) || snapshot.sha256 !== row.sha256) {
            fail(`bundle file ${filename} does not match its immutable pin`);
        }
        entries.set(filename, snapshot);
        totalBytes += snapshot.bytes;
    }
    if (integer(immutable.bundle_file_count, 'immutable.bundle_file_count', 1) !== entries.size
        || integer(immutable.bundle_total_bytes, 'immutable.bundle_total_bytes', 1) !== totalBytes) {
        fail('immutable bundle count/bytes do not match its file inventory');
    }
    const actualNames = fs.readdirSync(bundleDirectory).sort();
    const expectedNames = [...entries.keys(), 'immutable_manifest.json'].sort();
    if (JSON.stringify(actualNames) !== JSON.stringify(expectedNames)) fail('canonical bundle contains unpinned or missing files');
    const bundleName = safeFilename(immutable.bundle_manifest?.filename, 'immutable.bundle_manifest.filename');
    const bundleSnapshot = entries.get(bundleName);
    if (!bundleSnapshot || immutable.bundle_manifest.sha256 !== bundleSnapshot.sha256) {
        fail('immutable fitting-bundle manifest pin is invalid');
    }
    if (bundleSnapshot.sha256 !== expectedFittingBundleSha256) {
        fail('fitting bundle does not match the externally supplied SHA-256');
    }
    const fittingBundle = parseJson(bundleSnapshot, 'fitting bundle');
    if (fittingBundle.schema !== BUNDLE_SCHEMA || fittingBundle.source?.rig_type !== 'HORSE_2'
        || fittingBundle.source?.species !== 'horse' || fittingBundle.actionless?.actionless !== true) {
        fail('fitting bundle must be the actionless HORSE_2 horse contract');
    }
    if (immutable.source_model?.sha256 !== expectedSourceModelSha256
        || fittingBundle.source?.sha256 !== expectedSourceModelSha256
        || immutable.source_model?.sha256 !== fittingBundle.source?.sha256
        || immutable.source_model?.filename !== fittingBundle.source?.filename) {
        fail('immutable/source fitting bundle model provenance does not match the externally supplied SHA-256');
    }
    if (fittingBundle.counts?.vertices !== 344 || fittingBundle.counts?.faces !== 258
        || fittingBundle.counts?.armatures !== 1 || fittingBundle.counts?.meshes !== 1) {
        fail('fitting bundle must contain the canonical 344-vertex/258-face Horse_2 surface');
    }
    const artifact = (key, expectedFilename) => {
        const declared = object(fittingBundle.artifacts?.[key], `fittingBundle.artifacts.${key}`);
        const filename = safeFilename(declared.filename, `fittingBundle.artifacts.${key}.filename`);
        if (filename !== expectedFilename) fail(`fittingBundle.artifacts.${key} filename changed`);
        const snapshot = entries.get(filename);
        if (!snapshot || snapshot.sha256 !== declared.sha256 || snapshot.bytes !== declared.bytes) {
            fail(`fittingBundle.artifacts.${key} is not pinned by the immutable manifest`);
        }
        return snapshot;
    };
    const skeletonSnapshot = artifact('skeleton', 'skeleton.json');
    const weightsSnapshot = artifact('skin_weights', 'skin_weights.json.gz');
    const topologySnapshot = artifact('surface_topology', 'surface_topology.json.gz');
    const skeleton = parseJson(skeletonSnapshot, 'skeleton');
    const skinWeights = parseGzipJson(weightsSnapshot, 'skin weights');
    const topology = parseGzipJson(topologySnapshot, 'surface topology');
    if (!Array.isArray(skeleton.armatures) || skeleton.armatures.length !== 1) fail('skeleton must contain one armature');
    const armature = object(skeleton.armatures[0], 'skeleton.armatures[0]');
    matrixValues(armature.matrix_world, 'armature.matrix_world');
    if (!Array.isArray(armature.bones) || armature.bones.length !== 304) fail('Horse_2 skeleton must contain 304 bones');
    const boneNames = new Set();
    armature.bones.forEach((raw, index) => {
        const bone = object(raw, `skeleton.bones[${index}]`);
        const name = string(bone.name, `skeleton.bones[${index}].name`);
        if (boneNames.has(name)) fail(`skeleton repeats bone ${name}`);
        boneNames.add(name);
        vector3(bone.head_local, `${name}.head_local`);
        vector3(bone.tail_local, `${name}.tail_local`);
        matrixValues(bone.matrix_local, `${name}.matrix_local`);
        matrixValues(bone.parent_relative_matrix, `${name}.parent_relative_matrix`);
    });
    armature.bones.forEach((bone) => {
        if (bone.parent != null && !boneNames.has(bone.parent)) fail(`${bone.name} parent is missing`);
    });
    if (!Array.isArray(skinWeights.vertices) || skinWeights.vertices.length !== 344) {
        fail('skin weights must contain exactly 344 vertices');
    }
    let zeroWeightVertices = 0;
    skinWeights.vertices.forEach((raw, index) => {
        const vertex = object(raw, `skinWeights.vertices[${index}]`);
        if (vertex.vertex_index !== index || vertex.vertex_id !== index) fail('skin-weight vertices must be dense and ordered');
        vector3(vertex.local, `vertex ${index}.local`);
        vector3(vertex.world, `vertex ${index}.world`);
        if (!Array.isArray(vertex.weights)) fail(`vertex ${index}.weights must be an array`);
        const positive = vertex.weights.filter((weight) => finite(weight.weight, `vertex ${index} weight`) > 0);
        if (!positive.length) zeroWeightVertices += 1;
        positive.forEach((weight) => {
            if (!boneNames.has(weight.bone)) fail(`vertex ${index} uses missing bone ${weight.bone}`);
        });
    });
    if (!Array.isArray(topology.faces) || topology.faces.length !== 258) fail('surface topology must contain 258 faces');
    topology.faces.forEach((raw, index) => {
        const face = object(raw, `topology.faces[${index}]`);
        if (!Array.isArray(face.vertex_ids) || face.vertex_ids.length < 3) fail(`face ${index} must contain >=3 vertices`);
        face.vertex_ids.forEach((vertexId) => integer(vertexId, `face ${index} vertex`, 0) >= 344 && fail(`face ${index} vertex is out of range`));
    });
    const clipSnapshot = readSnapshot(clipValue, 'threeClipPath');
    const threeClip = parseJson(clipSnapshot, 'Three clip');
    const clipContract = validateThreeClip(threeClip, {
        path: clipSnapshot.path,
        bytes: clipSnapshot.bytes,
        sha256: clipSnapshot.sha256,
    }, boneNames, { loop: expectedLoop === true });
    const camera = object(fittingBundle.camera, 'fittingBundle.camera');
    if (JSON.stringify(camera.resolution) !== JSON.stringify([WIDTH, HEIGHT])) fail(`Horse_2 camera resolution must be ${WIDTH}x${HEIGHT}`);
    matrixValues(camera.camera_to_world, 'camera.camera_to_world');
    matrixValues(camera.world_to_camera, 'camera.world_to_camera');
    return {
        bundleDirectory,
        fittingBundle,
        immutable,
        immutablePin: { path: immutableSnapshot.path, bytes: immutableSnapshot.bytes, sha256: immutableSnapshot.sha256 },
        fittingBundlePin: { path: bundleSnapshot.path, bytes: bundleSnapshot.bytes, sha256: bundleSnapshot.sha256 },
        skeleton,
        skeletonPin: { path: skeletonSnapshot.path, bytes: skeletonSnapshot.bytes, sha256: skeletonSnapshot.sha256 },
        skinWeights,
        skinWeightsPin: { path: weightsSnapshot.path, bytes: weightsSnapshot.bytes, sha256: weightsSnapshot.sha256 },
        topology,
        topologyPin: { path: topologySnapshot.path, bytes: topologySnapshot.bytes, sha256: topologySnapshot.sha256 },
        threeClip,
        clipContract,
        zeroWeightVertices,
    };
}

function distance3(first, second) {
    return Math.hypot(first[0] - second[0], first[1] - second[1], first[2] - second[2]);
}

function percentileNearestRank(values, percentile) {
    if (!values.length) fail('percentile sample set must not be empty');
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.max(0, Math.ceil(percentile * sorted.length) - 1)];
}

function topologyEdges(topology, vertexCount) {
    const keys = new Set();
    for (const face of topology.faces) {
        for (let index = 0; index < face.vertex_ids.length; index += 1) {
            const first = integer(face.vertex_ids[index], 'topology edge vertex', 0);
            const second = integer(face.vertex_ids[(index + 1) % face.vertex_ids.length], 'topology edge vertex', 0);
            if (first >= vertexCount || second >= vertexCount || first === second) fail('topology contains an invalid edge');
            keys.add(first < second ? `${first}:${second}` : `${second}:${first}`);
        }
    }
    return [...keys].sort().map((key) => key.split(':').map(Number));
}

function coincidentRestGroups(restPositions, toleranceM = 1e-6) {
    const bins = new Map();
    restPositions.forEach((position, index) => {
        const key = position.map((value) => Math.round(value / toleranceM)).join(':');
        const bucket = bins.get(key) || [];
        bucket.push(index);
        bins.set(key, bucket);
    });
    return [...bins.values()].filter((indices) => indices.length > 1);
}

/**
 * Measure every frame of browser-deformed world vertices. Stretch is symmetric
 * (max(current/rest, rest/current)), so both tears and collapses are blocked.
 */
export function measureHorse2Deformation({
    skinWeights,
    topology,
    frames,
    thresholds = HORSE_VISUAL_PHASE_THRESHOLDS,
    requireRootMotionLocked = true,
}) {
    const vertices = skinWeights?.vertices;
    if (!Array.isArray(vertices) || !vertices.length) fail('skinWeights.vertices must not be empty');
    if (!Array.isArray(frames) || frames.length < 5) fail('deformation frames must contain every fitted frame');
    const rest = vertices.map((vertex, index) => vector3(vertex.world, `vertex ${index}.world`));
    const zeroWeightVertices = vertices.filter((vertex) => !Array.isArray(vertex.weights)
        || !vertex.weights.some((entry) => Number.isFinite(Number(entry.weight)) && Number(entry.weight) > 0)).length;
    const edges = topologyEdges(topology, rest.length);
    const edgeRest = edges.map(([first, second]) => distance3(rest[first], rest[second]));
    const nonzeroEdges = edges.map((edge, index) => ({ edge, restLength: edgeRest[index] }))
        .filter((row) => row.restLength > 1e-9);
    if (!nonzeroEdges.length) fail('surface topology has no measurable nonzero rest edges');
    const coincidentGroups = coincidentRestGroups(rest);
    const stretchSamples = [];
    const perFrame = [];
    let maximumCoincidentRestSeparationM = 0;
    frames.forEach((raw, expectedIndex) => {
        const frame = object(raw, `frames[${expectedIndex}]`);
        if (frame.frameIndex !== expectedIndex) fail(`deformation frame ${expectedIndex} lost chronology`);
        if (!Array.isArray(frame.positions) || frame.positions.length !== rest.length) {
            fail(`deformation frame ${expectedIndex} must contain ${rest.length} vertices`);
        }
        const positions = frame.positions.map((position, vertexIndex) => vector3(position, `frame ${expectedIndex} vertex ${vertexIndex}`));
        let maximumEdgeStretch = 1;
        let collapsedEdgeSampleCount = 0;
        const frameSamples = [];
        for (const { edge: [first, second], restLength } of nonzeroEdges) {
            const currentLength = distance3(positions[first], positions[second]);
            if (currentLength <= 1e-12) collapsedEdgeSampleCount += 1;
            const ratio = currentLength <= 1e-12
                ? 1e12
                : Math.max(currentLength / restLength, restLength / currentLength);
            frameSamples.push(ratio);
            stretchSamples.push(ratio);
            maximumEdgeStretch = Math.max(maximumEdgeStretch, ratio);
        }
        let maximumCoincidentSeparationM = 0;
        for (const indices of coincidentGroups) {
            for (let left = 0; left < indices.length; left += 1) {
                for (let right = left + 1; right < indices.length; right += 1) {
                    maximumCoincidentSeparationM = Math.max(
                        maximumCoincidentSeparationM,
                        distance3(positions[indices[left]], positions[indices[right]]),
                    );
                }
            }
        }
        maximumCoincidentRestSeparationM = Math.max(maximumCoincidentRestSeparationM, maximumCoincidentSeparationM);
        perFrame.push({
            frameIndex: expectedIndex,
            timeSeconds: finite(frame.timeSeconds, `frame ${expectedIndex}.timeSeconds`),
            maximumEdgeStretch,
            p99EdgeStretch: percentileNearestRank(frameSamples, 0.99),
            collapsedEdgeSampleCount,
            maximumCoincidentRestSeparationM: maximumCoincidentSeparationM,
            rootMotionLocked: frame.rootMotionLocked === true,
            cameraStatic: frame.cameraStatic === true,
        });
    });
    const maximumEdgeStretch = Math.max(...stretchSamples);
    const p99EdgeStretch = percentileNearestRank(stretchSamples, 0.99);
    const rootMotionLocked = perFrame.every((frame) => frame.rootMotionLocked);
    const cameraStatic = perFrame.every((frame) => frame.cameraStatic);
    const gates = {
        maximumEdgeStretch: maximumEdgeStretch <= finite(thresholds.maximumEdgeStretch, 'threshold maximumEdgeStretch'),
        p99EdgeStretch: p99EdgeStretch <= finite(thresholds.p99EdgeStretch, 'threshold p99EdgeStretch'),
        zeroWeightVertices: zeroWeightVertices <= integer(thresholds.zeroWeightVertices, 'threshold zeroWeightVertices'),
        coincidentRestSeparation: maximumCoincidentRestSeparationM
            <= finite(thresholds.coincidentRestSeparationM, 'threshold coincidentRestSeparationM'),
        rootMotionLocked: requireRootMotionLocked ? rootMotionLocked : true,
        cameraStatic,
    };
    return {
        schema: 'autorig.browser-horse-target-deformation-qa.v1',
        measuredEveryFrame: true,
        frameCount: frames.length,
        vertexCount: rest.length,
        edgeCount: nonzeroEdges.length,
        edgeSampleCount: stretchSamples.length,
        collapsedEdgeSampleCount: perFrame.reduce((total, frame) => total + frame.collapsedEdgeSampleCount, 0),
        coincidentRestGroupCount: coincidentGroups.length,
        coincidentRestSampleCount: frames.length,
        maximumEdgeStretch,
        p99EdgeStretch,
        zeroWeightVertices,
        maximumCoincidentRestSeparationM,
        thresholds: { ...thresholds },
        rootMotionLocked,
        rootMotionLockRequired: requireRootMotionLocked,
        cameraStatic,
        gates,
        passed: Object.values(gates).every(Boolean),
        frames: perFrame,
    };
}

/**
 * One-shot actions are not looped.  Their terminal contract instead requires a
 * materially lowered body, real ground contact without deep penetration, and a
 * settled final three-frame window.  All thresholds scale from the immutable
 * Horse_2 rest bounds so this gate is resolution- and framerate-independent.
 */
export function measureHorseOneShotFinalPose({
    skinWeights,
    frames,
    groundHeight,
    thresholds = HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS,
}) {
    const rest = skinWeights?.vertices?.map((vertex, index) => vector3(vertex.world, `vertex ${index}.world`));
    if (!Array.isArray(rest) || !rest.length) fail('skinWeights.vertices must not be empty');
    if (!Array.isArray(frames) || frames.length < 5) fail('one-shot frames must contain every fitted frame');
    const windowFrames = integer(thresholds.windowFrames, 'one-shot windowFrames', 2);
    if (windowFrames > frames.length) fail('one-shot windowFrames exceeds frame count');
    const axes = [0, 1, 2].map((axis) => rest.map((position) => position[axis]));
    const minimum = axes.map((values) => Math.min(...values));
    const maximum = axes.map((values) => Math.max(...values));
    const modelHeight = maximum[2] - minimum[2];
    const modelDiagonal = distance3(minimum, maximum);
    if (!(modelHeight > 1e-9) || !(modelDiagonal > 1e-9)) fail('immutable horse rest bounds are degenerate');
    const normalized = frames.map((raw, frameIndex) => {
        const frame = object(raw, `oneShot.frames[${frameIndex}]`);
        if (frame.frameIndex !== frameIndex || !Array.isArray(frame.positions) || frame.positions.length !== rest.length) {
            fail(`one-shot frame ${frameIndex} lost chronology or vertex inventory`);
        }
        return {
            frameIndex,
            positions: frame.positions.map((position, vertexIndex) => vector3(position, `one-shot frame ${frameIndex} vertex ${vertexIndex}`)),
            cameraStatic: frame.cameraStatic === true,
        };
    });
    const centroidZ = (positions) => positions.reduce((total, position) => total + position[2], 0) / positions.length;
    const finalWindow = normalized.slice(-windowFrames);
    const transitions = [];
    for (let index = 1; index < finalWindow.length; index += 1) {
        const previous = finalWindow[index - 1];
        const current = finalWindow[index];
        const displacements = current.positions.map((position, vertexIndex) => distance3(position, previous.positions[vertexIndex]));
        transitions.push({
            fromFrame: previous.frameIndex,
            toFrame: current.frameIndex,
            medianDisplacementM: percentileNearestRank(displacements, 0.5),
            p99DisplacementM: percentileNearestRank(displacements, 0.99),
            maximumDisplacementM: Math.max(...displacements),
        });
    }
    const finalPositions = normalized.at(-1).positions;
    const initialCentroidZ = centroidZ(normalized[0].positions);
    const finalCentroidZ = centroidZ(finalPositions);
    const centroidDropM = initialCentroidZ - finalCentroidZ;
    const finalMinimumZ = Math.min(...finalPositions.map((position) => position[2]));
    const ground = finite(groundHeight, 'groundHeight');
    const maximumP99AdjacentDisplacementM = Math.max(...transitions.map((row) => row.p99DisplacementM));
    const maximumMedianAdjacentDisplacementM = Math.max(...transitions.map((row) => row.medianDisplacementM));
    const resolved = {
        maximumP99AdjacentDisplacementM: finite(
            thresholds.maximumP99AdjacentDisplacementModelDiagonal,
            'maximumP99AdjacentDisplacementModelDiagonal',
        ) * modelDiagonal,
        maximumMedianAdjacentDisplacementM: finite(
            thresholds.maximumMedianAdjacentDisplacementModelDiagonal,
            'maximumMedianAdjacentDisplacementModelDiagonal',
        ) * modelDiagonal,
        minimumCentroidDropM: finite(thresholds.minimumCentroidDropModelHeight, 'minimumCentroidDropModelHeight') * modelHeight,
        groundContactToleranceM: finite(thresholds.groundContactToleranceModelHeight, 'groundContactToleranceModelHeight') * modelHeight,
        maximumGroundPenetrationM: finite(thresholds.maximumGroundPenetrationModelHeight, 'maximumGroundPenetrationModelHeight') * modelHeight,
    };
    const gates = {
        finalP99Motion: maximumP99AdjacentDisplacementM <= resolved.maximumP99AdjacentDisplacementM,
        finalMedianMotion: maximumMedianAdjacentDisplacementM <= resolved.maximumMedianAdjacentDisplacementM,
        centroidDrop: centroidDropM >= resolved.minimumCentroidDropM,
        groundContact: finalMinimumZ <= ground + resolved.groundContactToleranceM,
        groundPenetration: finalMinimumZ >= ground - resolved.maximumGroundPenetrationM,
        cameraStatic: normalized.every((frame) => frame.cameraStatic),
    };
    return {
        schema: 'autorig.browser-horse-one-shot-final-pose-qa.v1',
        temporalMode: 'one_shot',
        frameCount: normalized.length,
        finalWindowFrames: finalWindow.map((frame) => frame.frameIndex),
        modelHeightM: modelHeight,
        modelDiagonalM: modelDiagonal,
        groundHeightM: ground,
        initialCentroidZM: initialCentroidZ,
        finalCentroidZM: finalCentroidZ,
        centroidDropM,
        finalMinimumZM: finalMinimumZ,
        maximumP99AdjacentDisplacementM,
        maximumMedianAdjacentDisplacementM,
        transitions,
        thresholds: { ...thresholds },
        resolvedThresholds: resolved,
        gates,
        passed: Object.values(gates).every(Boolean),
    };
}

function phaseIndices(frameCount) {
    integer(frameCount, 'frameCount', 5);
    const result = [0, Math.floor((frameCount - 1) / 2), Math.floor((frameCount - 1) * 0.75)];
    if (new Set(result).size !== 3) fail('visual phase indices must be unique');
    return result;
}

/** Author the deliberately non-approving human-review evidence contract. */
export function buildHorseVisualPhaseEvidence({
    semanticId,
    validated,
    deformationReport,
    deformationReportPin,
    phaseFramePins,
    videoPin,
    renderer,
    cameraSettingsPin,
    finalPoseReport = null,
    finalPoseReportPin = null,
}) {
    const semantic = string(semanticId, 'semanticId').trim().toLowerCase();
    if (!/^[a-z0-9][a-z0-9_]{0,63}$/.test(semantic)) fail('semanticId is invalid');
    if (!cameraSettingsPin || !SHA256_RE.test(cameraSettingsPin.sha256) || cameraSettingsPin.bytes <= 0) {
        fail('cameraSettingsPin must pin the immutable fixed-camera settings');
    }
    if (!deformationReport || deformationReport.schema !== 'autorig.browser-horse-target-deformation-qa.v1') {
        fail('deformationReport schema is invalid');
    }
    if (!Array.isArray(phaseFramePins) || phaseFramePins.length !== 3) fail('exactly three phase-frame pins are required');
    const expectedIndices = phaseIndices(validated.clipContract.frameCount);
    const localFrames = phaseFramePins.map((raw, index) => {
        const row = object(raw, `phaseFramePins[${index}]`);
        if (row.phase !== HORSE_VISUAL_PHASE_REQUIRED_PHASES[index] || row.frameIndex !== expectedIndices[index]) {
            fail('phase-frame pins are incomplete, unordered, or use the wrong exact frames');
        }
        if (!SHA256_RE.test(row.sha256) || !Number.isInteger(row.bytes) || row.bytes <= 0) fail('phase-frame pin is invalid');
        return { phase: row.phase, frame_index: row.frameIndex, path: path.resolve(row.path), bytes: row.bytes, sha256: row.sha256 };
    });
    if (!videoPin || !SHA256_RE.test(videoPin.sha256) || videoPin.bytes <= 0) fail('fixed-camera video pin is invalid');
    if (!deformationReportPin || !SHA256_RE.test(deformationReportPin.sha256) || deformationReportPin.bytes <= 0) {
        fail('deformation report pin is invalid');
    }
    const oneShot = validated.clipContract.temporalMode === 'one_shot';
    if (oneShot && (finalPoseReport?.schema !== 'autorig.browser-horse-one-shot-final-pose-qa.v1'
        || !finalPoseReportPin || !SHA256_RE.test(finalPoseReportPin.sha256) || finalPoseReportPin.bytes <= 0)) {
        fail('one-shot evidence requires a pinned final-pose stability report');
    }
    if (!oneShot && (finalPoseReport != null || finalPoseReportPin != null)) {
        fail('loop evidence must not attach a one-shot final-pose report');
    }
    const machinePassed = deformationReport.passed === true && (!oneShot || finalPoseReport.passed === true);
    const visualPhaseGate = {
        schema: HORSE_VISUAL_PHASE_QA_SCHEMA,
        version: 1,
        rig_type: 'horse',
        semantic_id: semantic,
        fitted_clip_sha256: validated.clipContract.pin.sha256,
        decision: null,
        camera: {
            static: true,
            projection: 'perspective',
            view: 'canonical_fitting_bundle',
            root_motion_locked: validated.clipContract.loop,
            settings_sha256: cameraSettingsPin.sha256,
        },
        coincident_rest_vertex_separation: {
            measured: true,
            pass: deformationReport.gates.coincidentRestSeparation === true,
            threshold_m: HORSE_VISUAL_PHASE_THRESHOLDS.coincidentRestSeparationM,
            max_separation_m: deformationReport.maximumCoincidentRestSeparationM,
            sample_count: deformationReport.coincidentRestSampleCount,
            group_count: deformationReport.coincidentRestGroupCount,
            report_url: null,
            report_sha256: deformationReportPin.sha256,
        },
        required_phases: [...HORSE_VISUAL_PHASE_REQUIRED_PHASES],
        frames: localFrames.map((frame) => ({
            phase: frame.phase,
            frame_index: frame.frame_index,
            evidence_url: null,
            sha256: frame.sha256,
        })),
        reviewer: { id: null, reviewed_at: null },
    };
    return {
        schema: 'autorig.browser-horse-visual-phase-evidence-envelope.v1',
        visual_phase_gate: visualPhaseGate,
        local_evidence: {
            source_rig_type: 'HORSE_2',
            temporal_mode: validated.clipContract.temporalMode,
            browser_only: true,
            blender_used: false,
            animation_evaluation: 'Three.AnimationMixer',
            immutable_inputs: {
                source_model: {
                    filename: validated.fittingBundle.source.filename,
                    sha256: validated.fittingBundle.source.sha256,
                },
                immutable_manifest: validated.immutablePin,
                fitting_bundle: validated.fittingBundlePin,
                skeleton: validated.skeletonPin,
                skin_weights: validated.skinWeightsPin,
                surface_topology: validated.topologyPin,
                three_clip: validated.clipContract.pin,
            },
            camera_settings: cameraSettingsPin,
            browser_reconstruction_qa: {
                maximum_bone_head_error_world: renderer?.runtime?.maximumHeadReconstructionErrorWorld ?? null,
                maximum_rest_vertex_error_world: renderer?.runtime?.maximumRestVertexErrorWorld ?? null,
                animated_non_root_bones: renderer?.runtime?.animatedNonRootBoneNames ?? [],
                maximum_animated_bone_head_displacement_world:
                    renderer?.runtime?.maximumAnimatedBoneHeadDisplacementWorld ?? null,
                thresholds: {
                    maximum_bone_head_error_world: 1e-5,
                    maximum_rest_vertex_error_world: 1e-5,
                    minimum_animated_bone_head_displacement_world: 1e-6,
                },
            },
            target_mesh_deformation_qa: {
                measured_every_frame: true,
                passed: machinePassed,
                maximum_edge_stretch: deformationReport.maximumEdgeStretch,
                p99_edge_stretch: deformationReport.p99EdgeStretch,
                zero_weight_vertices: deformationReport.zeroWeightVertices,
                thresholds: {
                    maximum_edge_stretch: HORSE_VISUAL_PHASE_THRESHOLDS.maximumEdgeStretch,
                    p99_edge_stretch: HORSE_VISUAL_PHASE_THRESHOLDS.p99EdgeStretch,
                    zero_weight_vertices: HORSE_VISUAL_PHASE_THRESHOLDS.zeroWeightVertices,
                },
                report: deformationReportPin,
            },
            phase_frames: localFrames,
            one_shot_final_pose_qa: oneShot ? {
                passed: finalPoseReport.passed === true,
                final_window_frames: finalPoseReport.finalWindowFrames,
                maximum_p99_adjacent_displacement_m: finalPoseReport.maximumP99AdjacentDisplacementM,
                centroid_drop_m: finalPoseReport.centroidDropM,
                final_minimum_z_m: finalPoseReport.finalMinimumZM,
                gates: finalPoseReport.gates,
                report: finalPoseReportPin,
            } : null,
            video: {
                ...videoPin,
                fixed_camera: true,
                root_motion_locked: validated.clipContract.loop,
                root_motion_policy: validated.clipContract.loop ? 'suppress' : 'allow_one_shot',
            },
            renderer,
            human_review: { decision: null, reviewer_id: null, reviewed_at: null, required: true },
            approvals: {
                machine_qa_passed: machinePassed,
                ready_for_human_review: machinePassed,
                approved_for_animation_library: false,
                release_ready: false,
                fail_closed_reason: machinePassed
                    ? 'human_visual_phase_decision_and_public_urls_unset'
                    : (deformationReport.passed !== true
                        ? 'machine_target_deformation_qa_failed'
                        : 'machine_one_shot_final_pose_qa_failed'),
            },
        },
    };
}

function harnessHtml() {
    return `<!doctype html><html><head><meta charset="utf-8"><style>
html,body{margin:0;width:100%;height:100%;overflow:hidden;background:#717b86}canvas{display:block}
</style></head><body><script type="module">
import * as THREE from '/three.module.js';
const config = await (await fetch('/config.json', {cache:'no-store'})).json();
function matrix4(values, field) {
  if (!Array.isArray(values) || values.length !== 16 || values.some((v)=>!Number.isFinite(Number(v)))) throw new Error(field+' invalid');
  return new THREE.Matrix4().set(...values.map(Number));
}
function closeArray(a,b,epsilon=1e-8){return a.length===b.length&&a.every((v,i)=>Math.abs(Number(v)-Number(b[i]))<=epsilon)}
function buildModel(){
  const armature=config.skeleton.armatures[0];
  const model=new THREE.Group(); model.name='AutoRig_Horse2_Final_QA';
  const armatureMatrix=matrix4(armature.matrix_world,'armature.matrix_world');
  armatureMatrix.decompose(model.position,model.quaternion,model.scale);
  const bones=new Map();
  for(const source of armature.bones){
    const bone=new THREE.Bone(); bone.name=source.name;
    matrix4(source.parent ? source.parent_relative_matrix : source.matrix_local,source.name+'.matrix').decompose(bone.position,bone.quaternion,bone.scale);
    bones.set(source.name,bone);
  }
  for(const source of armature.bones){const bone=bones.get(source.name); if(source.parent) bones.get(source.parent).add(bone); else model.add(bone)}
  model.updateWorldMatrix(true,true);
  let maximumHeadReconstructionErrorWorld=0;
  for(const source of armature.bones){const expected=new THREE.Vector3(...source.head_local).applyMatrix4(armatureMatrix);const actual=bones.get(source.name).getWorldPosition(new THREE.Vector3());maximumHeadReconstructionErrorWorld=Math.max(maximumHeadReconstructionErrorWorld,actual.distanceTo(expected))}
  return {model,bones,sources:armature.bones,maximumHeadReconstructionErrorWorld};
}
function buildSkin(state){
  const vertices=config.skinWeights.vertices;
  const boneOrder=state.sources.map((source)=>state.bones.get(source.name));
  const boneIndex=new Map(boneOrder.map((bone,index)=>[bone.name,index]));
  const positions=[],skinIndices=[],skinWeights=[];
  let zeroWeightVertices=0;
  for(const vertex of vertices){
    positions.push(...vertex.local.map(Number));
    let influences=vertex.weights.filter((entry)=>Number(entry.weight)>0).sort((a,b)=>Number(b.weight)-Number(a.weight)).slice(0,4);
    if(!influences.length){zeroWeightVertices+=1;influences=[{bone:boneOrder[0].name,weight:1}]}
    const sum=influences.reduce((total,entry)=>total+Number(entry.weight),0);
    while(influences.length<4) influences.push({bone:influences[0].bone,weight:0});
    for(const influence of influences){if(!boneIndex.has(influence.bone))throw new Error('missing influence '+influence.bone);skinIndices.push(boneIndex.get(influence.bone));skinWeights.push(Number(influence.weight)/sum)}
  }
  const indices=[];
  for(const face of config.topology.faces){for(let index=1;index<face.vertex_ids.length-1;index+=1)indices.push(face.vertex_ids[0],face.vertex_ids[index],face.vertex_ids[index+1])}
  const geometry=new THREE.BufferGeometry();
  geometry.setAttribute('position',new THREE.Float32BufferAttribute(positions,3));
  geometry.setAttribute('skinIndex',new THREE.Uint16BufferAttribute(skinIndices,4));
  geometry.setAttribute('skinWeight',new THREE.Float32BufferAttribute(skinWeights,4));
  geometry.setIndex(indices);geometry.computeVertexNormals();geometry.computeBoundingSphere();
  const material=new THREE.MeshStandardMaterial({color:0xe9e4dc,roughness:0.74,metalness:0,flatShading:true,side:THREE.DoubleSide});
  const mesh=new THREE.SkinnedMesh(geometry,material);mesh.name='Horse_geo_browser_344v';mesh.castShadow=false;mesh.receiveShadow=false;
  const skeleton=new THREE.Skeleton(boneOrder);state.model.add(mesh);state.model.updateWorldMatrix(true,true);mesh.bind(skeleton,new THREE.Matrix4());
  return {mesh,skeleton,zeroWeightVertices};
}
function buildCamera(){
  const contract=config.camera,[width,height]=contract.resolution,{fx,fy,cx,cy}=contract.intrinsics,near=.01,far=1000;
  const camera=new THREE.PerspectiveCamera();camera.matrixAutoUpdate=false;camera.matrix.copy(matrix4(contract.camera_to_world,'camera_to_world'));
  camera.matrixWorld.copy(camera.matrix);camera.matrixWorldInverse.copy(matrix4(contract.world_to_camera,'world_to_camera'));
  camera.projectionMatrix.set(2*fx/width,0,1-2*cx/width,0,0,2*fy/height,2*cy/height-1,0,0,0,(far+near)/(near-far),2*far*near/(near-far),0,0,-1,0);
  camera.projectionMatrixInverse.copy(camera.projectionMatrix).invert();camera.updateProjectionMatrix=()=>{};camera.updateWorldMatrix(true,false);return camera;
}
function buildRenderer(){const renderer=new THREE.WebGLRenderer({antialias:true,alpha:false,preserveDrawingBuffer:true});renderer.setPixelRatio(1);renderer.setSize(config.camera.resolution[0],config.camera.resolution[1],false);renderer.setClearColor(0x717b86,1);renderer.outputColorSpace=THREE.SRGBColorSpace;renderer.toneMapping=THREE.ACESFilmicToneMapping;renderer.toneMappingExposure=1.1;renderer.shadowMap.enabled=false;document.body.replaceChildren(renderer.domElement);return renderer}
function buildScene(model){const scene=new THREE.Scene();scene.background=new THREE.Color(0x717b86);scene.add(model);scene.add(new THREE.HemisphereLight(0xe9f1ff,0x3f4650,2.1));const key=new THREE.DirectionalLight(0xffffff,3.5);key.position.set(4.5,-5.5,8.5);scene.add(key);scene.add(key.target);const ground=new THREE.Mesh(new THREE.PlaneGeometry(50,50),new THREE.MeshStandardMaterial({color:0xb8c3cc,roughness:.92,metalness:0}));ground.position.z=Number(config.groundHeight);scene.add(ground);return scene}
try{
  const state=buildModel(),skin=buildSkin(state),camera=buildCamera(),renderer=buildRenderer(),scene=buildScene(state.model);
  const roots=new Set(state.sources.filter((source)=>source.parent==null||/(^|[._])(root|c_pos|c_traj|traj)([._]|$)/i.test(source.name)).map((source)=>source.name));
  const parsed=THREE.AnimationClip.parse(config.threeClip),suppressedRootTracks=[];
  const tracks=parsed.tracks.filter((track)=>{const match=track.name.match(/^(.*)\.(quaternion|position)$/);if(!match||!state.bones.has(match[1]))throw new Error('unbound track '+track.name);if(config.suppressRootMotion===true&&(roots.has(match[1])||/(^|[._])(root|c_pos)([._]|$)/i.test(match[1]))){suppressedRootTracks.push(track.name);return false}return true});
  state.model.updateWorldMatrix(true,true);skin.skeleton.update();
  const animatedNonRootBoneNames=[...new Set(tracks.map((track)=>track.name.match(/^(.*)\.(quaternion|position)$/)[1]))].sort();
  const animatedRestHeads=new Map(animatedNonRootBoneNames.map((name)=>[name,state.bones.get(name).getWorldPosition(new THREE.Vector3()).clone()]));
  const restPositionAttribute=skin.mesh.geometry.getAttribute('position');let maximumRestVertexErrorWorld=0;
  for(let vertexIndex=0;vertexIndex<restPositionAttribute.count;vertexIndex+=1){const actual=new THREE.Vector3().fromBufferAttribute(restPositionAttribute,vertexIndex);skin.mesh.applyBoneTransform(vertexIndex,actual);skin.mesh.localToWorld(actual);const expected=new THREE.Vector3(...config.skinWeights.vertices[vertexIndex].world);maximumRestVertexErrorWorld=Math.max(maximumRestVertexErrorWorld,actual.distanceTo(expected))}
  const clip=new THREE.AnimationClip(parsed.name,parsed.duration,tracks,parsed.blendMode),mixer=new THREE.AnimationMixer(state.model),action=mixer.clipAction(clip);action.setLoop(THREE.LoopOnce,1);action.clampWhenFinished=true;action.play();
  const modelState={position:state.model.position.toArray(),quaternion:state.model.quaternion.toArray(),scale:state.model.scale.toArray()};
  const rootState=new Map([...roots].map((name)=>{const bone=state.bones.get(name);return[name,{position:bone.position.toArray(),quaternion:bone.quaternion.toArray(),scale:bone.scale.toArray()}]}));
  const cameraState={matrix:camera.matrix.toArray(),matrixWorld:camera.matrixWorld.toArray(),projection:camera.projectionMatrix.toArray()};
  const positionAttribute=skin.mesh.geometry.getAttribute('position');
  window.__renderHorseFrame=async(frameIndex)=>{
    if(!Number.isInteger(frameIndex)||frameIndex<0||frameIndex>=config.timeline.length)throw new Error('invalid frame index');
    mixer.setTime(Number(config.timeline[frameIndex]));state.model.updateWorldMatrix(true,true);skin.skeleton.update();
    renderer.render(scene,camera);await new Promise((resolve)=>requestAnimationFrame(resolve));renderer.render(scene,camera);
    const positions=[];
    for(let vertexIndex=0;vertexIndex<positionAttribute.count;vertexIndex+=1){const value=new THREE.Vector3().fromBufferAttribute(positionAttribute,vertexIndex);skin.mesh.applyBoneTransform(vertexIndex,value);skin.mesh.localToWorld(value);positions.push(value.toArray())}
    const rootMotionLocked=closeArray(state.model.position.toArray(),modelState.position)&&closeArray(state.model.quaternion.toArray(),modelState.quaternion)&&closeArray(state.model.scale.toArray(),modelState.scale)&&[...rootState].every(([name,rest])=>{const bone=state.bones.get(name);return closeArray(bone.position.toArray(),rest.position)&&closeArray(bone.quaternion.toArray(),rest.quaternion)&&closeArray(bone.scale.toArray(),rest.scale)});
    const cameraStatic=closeArray(camera.matrix.toArray(),cameraState.matrix)&&closeArray(camera.matrixWorld.toArray(),cameraState.matrixWorld)&&closeArray(camera.projectionMatrix.toArray(),cameraState.projection);
    const maximumAnimatedBoneHeadDisplacementWorld=animatedNonRootBoneNames.reduce((maximum,name)=>Math.max(maximum,state.bones.get(name).getWorldPosition(new THREE.Vector3()).distanceTo(animatedRestHeads.get(name))),0);
    return {frameIndex,timeSeconds:Number(config.timeline[frameIndex]),positions,rootMotionLocked,cameraStatic,maximumAnimatedBoneHeadDisplacementWorld,width:renderer.domElement.width,height:renderer.domElement.height,dataUrl:renderer.domElement.toDataURL('image/png')};
  };
  window.__AUTORIG_RESULT__={threeRevision:String(THREE.REVISION),vertexCount:positionAttribute.count,faceCount:config.topology.faces.length,boneCount:state.sources.length,skinBoneCount:skin.skeleton.bones.length,zeroWeightVertices:skin.zeroWeightVertices,maximumHeadReconstructionErrorWorld:state.maximumHeadReconstructionErrorWorld,maximumRestVertexErrorWorld,animatedNonRootBoneNames,suppressedRootTracks,rootBoneNames:[...roots],rootMotionPolicy:config.suppressRootMotion===true?'suppress':'allow_one_shot',animationEvaluation:'Three.AnimationMixer',renderer:{webgl2:renderer.capabilities.isWebGL2===true,outputColorSpace:'SRGBColorSpace',toneMapping:'ACESFilmicToneMapping',toneMappingExposure:1.1,shadowsEnabled:false}};
  window.__AUTORIG_READY__=true;
}catch(error){window.__AUTORIG_ERROR__=String(error?.stack||error);console.error(error)}
</script></body></html>`;
}

function mime(filename) {
    if (filename.endsWith('.js')) return 'text/javascript; charset=utf-8';
    return 'application/octet-stream';
}

function startHarnessServer({ config, threeModuleSnapshot }) {
    const routes = new Map([
        ['/index.html', { buffer: Buffer.from(harnessHtml(), 'utf8'), type: 'text/html; charset=utf-8' }],
        ['/config.json', { buffer: Buffer.from(JSON.stringify(config), 'utf8'), type: 'application/json; charset=utf-8' }],
        ['/three.module.js', { buffer: threeModuleSnapshot.buffer, type: mime(threeModuleSnapshot.path) }],
    ]);
    const server = http.createServer((request, response) => {
        const route = routes.get(new URL(request.url, 'http://127.0.0.1').pathname);
        response.setHeader('Cache-Control', 'no-store');
        if (!route) { response.writeHead(404); response.end('not found'); return; }
        response.writeHead(200, { 'Content-Type': route.type });
        if (route.filename) fs.createReadStream(route.filename).pipe(response); else response.end(route.buffer);
    });
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => resolve({ server, url: `http://127.0.0.1:${server.address().port}/index.html` }));
    });
}

class CdpClient {
    constructor(url) {
        this.socket = new WebSocket(url);
        this.nextId = 1;
        this.pending = new Map();
        this.socket.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (!message.id) return;
            const pending = this.pending.get(message.id);
            if (!pending) return;
            this.pending.delete(message.id);
            if (message.error) pending.reject(new Error(message.error.message)); else pending.resolve(message.result || {});
        };
    }
    async open() {
        if (this.socket.readyState === WebSocket.OPEN) return;
        await new Promise((resolve, reject) => { this.socket.onopen = resolve; this.socket.onerror = () => reject(new Error('CDP connection failed')); });
    }
    command(method, params = {}) {
        const id = this.nextId++;
        return new Promise((resolve, reject) => { this.pending.set(id, { resolve, reject }); this.socket.send(JSON.stringify({ id, method, params })); });
    }
    close() { this.socket.close(); }
}

function delay(milliseconds) { return new Promise((resolve) => setTimeout(resolve, milliseconds)); }

async function launchChrome(chromeExecutable) {
    const profileDirectory = fs.mkdtempSync(path.join(process.env.TEMP || process.cwd(), 'autorig-horse-final-qa-'));
    const child = spawn(chromeExecutable, [
        '--headless=new', '--use-angle=swiftshader', '--enable-webgl', '--ignore-gpu-blocklist',
        '--disable-background-networking', '--disable-component-update', '--disable-default-apps', '--disable-extensions',
        '--disable-sync', '--no-first-run', '--no-default-browser-check', '--remote-debugging-address=127.0.0.1',
        '--remote-debugging-port=0', `--user-data-dir=${profileDirectory}`, 'about:blank',
    ], { stdio: ['ignore', 'ignore', 'pipe'], windowsHide: true });
    let stderr = '';
    let websocketUrl = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => { stderr += chunk; websocketUrl ||= stderr.match(/DevTools listening on (ws:\/\/[^\s]+)/)?.[1] || ''; });
    const started = Date.now();
    while (!websocketUrl && Date.now() - started < 15000) {
        if (child.exitCode != null) fail(`Chrome exited during startup (${child.exitCode}): ${stderr}`);
        await delay(50);
    }
    if (!websocketUrl) fail(`Chrome did not expose CDP: ${stderr}`);
    const endpoint = new URL(websocketUrl);
    const pages = await (await fetch(`http://${endpoint.host}/json/list`)).json();
    const page = pages.find((entry) => entry.type === 'page');
    if (!page?.webSocketDebuggerUrl) fail('Chrome did not expose a page target');
    return { child, profileDirectory, pageWebSocketUrl: page.webSocketDebuggerUrl, stderr: () => stderr };
}

async function stopChrome(runtime) {
    if (!runtime) return;
    try {
        if (runtime.child.exitCode == null) runtime.child.kill();
        await Promise.race([new Promise((resolve) => runtime.child.once('exit', resolve)), delay(3000)]);
        if (runtime.child.exitCode == null) runtime.child.kill('SIGKILL');
    } finally {
        fs.rmSync(runtime.profileDirectory, { recursive: true, force: true });
    }
}

async function evaluate(client, expression) {
    const result = await client.command('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
    if (result.exceptionDetails) fail(`browser evaluation failed: ${result.exceptionDetails.text}`);
    return result.result?.value;
}

export async function renderHorse2QaFramesInBrowser({
    chromeExecutable,
    threeModule,
    threeModuleSnapshot: suppliedThreeModuleSnapshot,
    expectedThreeModuleSha256,
    expectedThreeRevision = '160',
    validated,
}) {
    const chrome = path.resolve(string(chromeExecutable, 'chromeExecutable'));
    const three = path.resolve(string(threeModule, 'threeModule'));
    if (!fs.statSync(chrome).isFile() || !fs.statSync(three).isFile()) fail('Chrome and Three module must be local files');
    const threeModuleSnapshot = suppliedThreeModuleSnapshot || readSnapshot(three, 'threeModule');
    if (path.resolve(threeModuleSnapshot.path) !== three
        || !SHA256_RE.test(threeModuleSnapshot.sha256)
        || (expectedThreeModuleSha256 && threeModuleSnapshot.sha256 !== expectedThreeModuleSha256)) {
        fail('supplied Three module snapshot does not match its external pin/path');
    }
    if (String(expectedThreeRevision) !== '160') fail('Horse_2 QA requires pinned Three revision 160');
    const config = {
        skeleton: validated.skeleton,
        skinWeights: validated.skinWeights,
        topology: validated.topology,
        threeClip: validated.threeClip,
        timeline: validated.clipContract.timeline,
        suppressRootMotion: validated.clipContract.loop,
        camera: validated.fittingBundle.camera,
        groundHeight: validated.fittingBundle.ground_plane.height,
    };
    const { server, url } = await startHarnessServer({ config, threeModuleSnapshot });
    let runtime;
    let client;
    try {
        runtime = await launchChrome(chrome);
        client = new CdpClient(runtime.pageWebSocketUrl);
        await client.open();
        await client.command('Page.enable');
        await client.command('Runtime.enable');
        await client.command('Emulation.setDeviceMetricsOverride', { width: WIDTH, height: HEIGHT, deviceScaleFactor: 1, mobile: false });
        await client.command('Page.navigate', { url });
        const started = Date.now();
        let runtimeReport;
        while (Date.now() - started < 30000) {
            const state = await evaluate(client, `({ready:window.__AUTORIG_READY__===true,error:window.__AUTORIG_ERROR__||null,result:window.__AUTORIG_RESULT__||null})`);
            if (state?.error) fail(`Horse_2 browser harness failed: ${state.error}`);
            if (state?.ready) { runtimeReport = state.result; break; }
            await delay(100);
        }
        if (!runtimeReport) fail(`Horse_2 browser harness timed out: ${runtime.stderr()}`);
        if (runtimeReport.threeRevision !== String(expectedThreeRevision)) {
            fail(`browser loaded Three revision ${runtimeReport.threeRevision}, expected ${expectedThreeRevision}`);
        }
        if (runtimeReport.vertexCount !== 344 || runtimeReport.faceCount !== 258 || runtimeReport.boneCount !== 304
            || runtimeReport.skinBoneCount !== 304 || runtimeReport.zeroWeightVertices !== validated.zeroWeightVertices) {
            fail('Horse_2 browser reconstruction inventory changed');
        }
        if (finite(runtimeReport.maximumHeadReconstructionErrorWorld, 'maximumHeadReconstructionErrorWorld') > 1e-5) {
            fail(`Horse_2 bone-head hierarchy reconstruction drifted by ${runtimeReport.maximumHeadReconstructionErrorWorld} m`);
        }
        if (finite(runtimeReport.maximumRestVertexErrorWorld, 'maximumRestVertexErrorWorld') > 1e-5) {
            fail(`Horse_2 browser rest reconstruction drifted by ${runtimeReport.maximumRestVertexErrorWorld} m`);
        }
        const frames = [];
        for (let frameIndex = 0; frameIndex < validated.clipContract.frameCount; frameIndex += 1) {
            const rendered = await evaluate(client, `window.__renderHorseFrame(${frameIndex})`);
            if (!rendered?.dataUrl?.startsWith('data:image/png;base64,') || rendered.width !== WIDTH || rendered.height !== HEIGHT) {
                fail(`browser frame ${frameIndex} is not a ${WIDTH}x${HEIGHT} PNG`);
            }
            frames.push({ ...rendered, png: Buffer.from(rendered.dataUrl.slice('data:image/png;base64,'.length), 'base64') });
            delete frames.at(-1).dataUrl;
        }
        runtimeReport.maximumAnimatedBoneHeadDisplacementWorld = Math.max(
            0,
            ...frames.map((frame) => finite(
                frame.maximumAnimatedBoneHeadDisplacementWorld,
                `frame ${frame.frameIndex} maximumAnimatedBoneHeadDisplacementWorld`,
            )),
        );
        if (!Array.isArray(runtimeReport.animatedNonRootBoneNames)
            || runtimeReport.animatedNonRootBoneNames.length === 0
            || runtimeReport.maximumAnimatedBoneHeadDisplacementWorld <= 1e-6) {
            fail('Three clip did not animate any non-root bone head above the 1e-6 m proof threshold');
        }
        return {
            frames,
            runtimeReport,
            threeModulePin: {
                path: threeModuleSnapshot.path,
                bytes: threeModuleSnapshot.bytes,
                sha256: threeModuleSnapshot.sha256,
            },
        };
    } finally {
        client?.close();
        await stopChrome(runtime);
        await new Promise((resolve) => server.close(resolve));
    }
}

function pngDimensions(buffer, field) {
    const signature = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
    if (!Buffer.isBuffer(buffer) || buffer.length < 24 || !buffer.subarray(0, 8).equals(signature)) fail(`${field} is not PNG`);
    return [buffer.readUInt32BE(16), buffer.readUInt32BE(20)];
}

function runChecked(executable, args, field, options = {}) {
    const result = spawnSync(executable, args, {
        encoding: 'utf8', windowsHide: true, maxBuffer: options.maxBuffer || 8 * 1024 * 1024,
    });
    if (result.error || result.status !== 0) {
        fail(`${field} failed: ${result.error?.message || String(result.stderr || '').trim() || `exit ${result.status}`}`);
    }
    return result;
}

function encodeAndValidateMp4({ ffmpeg, ffprobe, framesDirectory, outputPath, fps, frameCount }) {
    runChecked(ffmpeg, [
        '-hide_banner', '-loglevel', 'error', '-nostdin', '-n',
        '-framerate', String(fps), '-start_number', '0', '-i', path.join(framesDirectory, 'frame_%04d.png'),
        '-frames:v', String(frameCount), '-an', '-c:v', 'libx264', '-preset', 'medium', '-crf', '18',
        '-pix_fmt', 'yuv420p', '-movflags', '+faststart', outputPath,
    ], 'fixed-camera MP4 encode');
    const probe = JSON.parse(runChecked(ffprobe, [
        '-v', 'error', '-count_frames', '-show_entries',
        'format=format_name,duration:stream=index,codec_type,codec_name,pix_fmt,width,height,r_frame_rate,nb_read_frames',
        '-of', 'json', outputPath,
    ], 'fixed-camera MP4 ffprobe').stdout);
    const streams = Array.isArray(probe.streams) ? probe.streams : [];
    const videoStreams = streams.filter((stream) => stream.codec_type === 'video');
    const audioStreams = streams.filter((stream) => stream.codec_type === 'audio');
    const video = videoStreams[0];
    const [rateNumerator, rateDenominator] = String(video?.r_frame_rate || '').split('/').map(Number);
    const measuredFps = rateNumerator / rateDenominator;
    if (videoStreams.length !== 1 || audioStreams.length !== 0
        || !String(probe.format?.format_name || '').split(',').some((name) => name === 'mp4' || name === 'mov')
        || video.codec_name !== 'h264' || video.pix_fmt !== 'yuv420p'
        || Number(video.width) !== WIDTH || Number(video.height) !== HEIGHT
        || Number(video.nb_read_frames) !== frameCount || Math.abs(measuredFps - fps) > 1e-6) {
        fail(`fixed-camera MP4 contract changed: ${JSON.stringify(probe)}`);
    }
    return pinFile(outputPath, {
        container: 'mp4', codec: 'h264', pixel_format: 'yuv420p', width: WIDTH, height: HEIGHT,
        fps: measuredFps, frame_count: frameCount, audio_stream_count: 0, duration_seconds: Number(probe.format.duration),
    });
}

/** Run the complete browser-only QA transaction and atomically publish evidence. */
export async function runHorseVisualPhaseQa(configuration, dependencies = {}) {
    const config = object(configuration, 'configuration');
    const validated = validateHorse2QaInputs({
        bundleDirectory: config.bundleDirectory,
        threeClipPath: config.threeClipPath,
        expectedImmutableManifestSha256: config.expectedImmutableManifestSha256,
        expectedFittingBundleSha256: config.expectedFittingBundleSha256,
        expectedSourceModelSha256: config.expectedSourceModelSha256,
        expectedLoop: config.loop !== false,
    });
    if (!SHA256_RE.test(config.expectedThreeClipSha256)
        || validated.clipContract.pin.sha256 !== config.expectedThreeClipSha256) {
        fail('Three clip does not match --three-clip-sha256');
    }
    if (!SHA256_RE.test(config.expectedThreeModuleSha256)) {
        fail('expectedThreeModuleSha256 must be an externally supplied lowercase SHA-256');
    }
    if (String(config.expectedThreeRevision) !== '160') fail('expectedThreeRevision must be exactly 160');
    const threeModuleSnapshot = readSnapshot(config.threeModule, 'threeModule');
    if (threeModuleSnapshot.sha256 !== config.expectedThreeModuleSha256) {
        fail('Three module does not match --three-module-sha256');
    }
    const outputDirectory = path.resolve(string(config.outputDirectory, 'outputDirectory'));
    const parent = path.dirname(outputDirectory);
    if (!fs.existsSync(parent) || !fs.statSync(parent).isDirectory()) fail(`output parent does not exist: ${parent}`);
    const existedEmpty = fs.existsSync(outputDirectory);
    if (existedEmpty && (!fs.statSync(outputDirectory).isDirectory() || fs.readdirSync(outputDirectory).length)) {
        fail(`outputDirectory must be absent or empty: ${outputDirectory}`);
    }
    const staging = `${outputDirectory}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    fs.mkdirSync(staging);
    try {
        const browserRunner = dependencies.renderHorse2QaFramesInBrowser || renderHorse2QaFramesInBrowser;
        const rendered = await browserRunner({
            chromeExecutable: config.chromeExecutable,
            threeModule: config.threeModule,
            threeModuleSnapshot,
            expectedThreeModuleSha256: config.expectedThreeModuleSha256,
            expectedThreeRevision: String(config.expectedThreeRevision),
            validated,
        });
        if (!Array.isArray(rendered.frames) || rendered.frames.length !== validated.clipContract.frameCount) {
            fail('browser did not return every exact Three clip frame');
        }
        if (rendered.threeModulePin?.sha256 !== config.expectedThreeModuleSha256
            || path.resolve(rendered.threeModulePin?.path || '') !== threeModuleSnapshot.path
            || rendered.threeModulePin?.bytes !== threeModuleSnapshot.bytes) {
            fail('browser did not consume the externally pinned read-once Three module snapshot');
        }
        const framesDirectory = path.join(staging, 'frames');
        fs.mkdirSync(framesDirectory);
        rendered.frames.forEach((frame, frameIndex) => {
            if (frame.frameIndex !== frameIndex || !Buffer.isBuffer(frame.png)) fail(`browser frame ${frameIndex} is invalid`);
            if (JSON.stringify(pngDimensions(frame.png, `browser frame ${frameIndex}`)) !== JSON.stringify([WIDTH, HEIGHT])) {
                fail(`browser frame ${frameIndex} dimensions changed`);
            }
            writeNew(path.join(framesDirectory, `frame_${String(frameIndex).padStart(4, '0')}.png`), frame.png);
        });
        const deformation = measureHorse2Deformation({
            skinWeights: validated.skinWeights,
            topology: validated.topology,
            frames: rendered.frames.map(({ frameIndex, timeSeconds, positions, rootMotionLocked, cameraStatic }) => ({
                frameIndex, timeSeconds, positions, rootMotionLocked, cameraStatic,
            })),
            requireRootMotionLocked: validated.clipContract.loop,
        });
        if (deformation.zeroWeightVertices !== validated.zeroWeightVertices) fail('browser/numeric zero-weight inventories disagree');
        const deformationPath = path.join(staging, 'deformation-report.json');
        writeNew(deformationPath, canonicalJsonBuffer({
            ...deformation,
            inputs: {
                fittingBundleSha256: validated.fittingBundlePin.sha256,
                threeClipSha256: validated.clipContract.pin.sha256,
                skinWeightsSha256: validated.skinWeightsPin.sha256,
                topologySha256: validated.topologyPin.sha256,
            },
        }));
        const finalDeformationPath = path.join(outputDirectory, 'deformation-report.json');
        const deformationPin = { ...pinFile(deformationPath), path: finalDeformationPath };
        let finalPose = null;
        let finalPosePin = null;
        if (!validated.clipContract.loop) {
            finalPose = measureHorseOneShotFinalPose({
                skinWeights: validated.skinWeights,
                frames: rendered.frames.map(({ frameIndex, positions, cameraStatic }) => ({
                    frameIndex, positions, cameraStatic,
                })),
                groundHeight: validated.fittingBundle.ground_plane.height,
            });
            const finalPosePath = path.join(staging, 'final-pose-stability-report.json');
            writeNew(finalPosePath, canonicalJsonBuffer({
                ...finalPose,
                inputs: {
                    fittingBundleSha256: validated.fittingBundlePin.sha256,
                    threeClipSha256: validated.clipContract.pin.sha256,
                    skinWeightsSha256: validated.skinWeightsPin.sha256,
                },
            }));
            finalPosePin = {
                ...pinFile(finalPosePath),
                path: path.join(outputDirectory, 'final-pose-stability-report.json'),
            };
        }
        const videoPath = path.join(staging, 'fixed-camera-preview.mp4');
        const encoder = dependencies.encodeAndValidateMp4 || encodeAndValidateMp4;
        const stagedVideoPin = encoder({
            ffmpeg: config.ffmpeg,
            ffprobe: config.ffprobe,
            framesDirectory,
            outputPath: videoPath,
            fps: validated.clipContract.fps,
            frameCount: validated.clipContract.frameCount,
        });
        const videoPin = { ...stagedVideoPin, path: path.join(outputDirectory, 'fixed-camera-preview.mp4') };
        const indices = phaseIndices(validated.clipContract.frameCount);
        const phasePins = HORSE_VISUAL_PHASE_REQUIRED_PHASES.map((phase, index) => {
            const frameIndex = indices[index];
            const stagedFramePath = path.join(framesDirectory, `frame_${String(frameIndex).padStart(4, '0')}.png`);
            return {
                ...pinFile(stagedFramePath),
                path: path.join(outputDirectory, 'frames', path.basename(stagedFramePath)),
                phase,
                frameIndex,
            };
        });
        const cameraContract = {
            schema: 'autorig.browser-horse-fixed-camera.v1',
            camera: validated.fittingBundle.camera,
            temporalMode: validated.clipContract.temporalMode,
            rootMotionPolicy: validated.clipContract.loop
                ? 'suppress_armature_root_tracks_and_lock_model_transform'
                : 'allow_one_shot_root_tracks_keep_camera_static',
            resolution: [WIDTH, HEIGHT],
            renderer: rendered.runtimeReport?.renderer,
        };
        const cameraSettingsPath = path.join(staging, 'camera-settings.json');
        writeNew(cameraSettingsPath, canonicalJsonBuffer(cameraContract));
        const cameraSettingsPin = {
            ...pinFile(cameraSettingsPath),
            path: path.join(outputDirectory, 'camera-settings.json'),
        };
        const evidence = buildHorseVisualPhaseEvidence({
            semanticId: config.semanticId,
            validated,
            deformationReport: deformation,
            deformationReportPin: deformationPin,
            phaseFramePins: phasePins,
            videoPin,
            cameraSettingsPin,
            renderer: {
                browser: 'headless_chrome_cdp',
                three_revision: rendered.runtimeReport?.threeRevision || null,
                three_module: rendered.threeModulePin,
                runtime: rendered.runtimeReport,
            },
            finalPoseReport: finalPose,
            finalPoseReportPin: finalPosePin,
        });
        const evidencePath = path.join(staging, 'visual-phase-qa.json');
        writeNew(evidencePath, canonicalJsonBuffer(evidence));
        if (evidence.visual_phase_gate.decision !== null
            || evidence.visual_phase_gate.frames.some((frame) => frame.evidence_url !== null)
            || evidence.visual_phase_gate.coincident_rest_vertex_separation.report_url !== null
            || evidence.visual_phase_gate.reviewer.id !== null
            || evidence.local_evidence.human_review.decision !== null
            || evidence.local_evidence.approvals.approved_for_animation_library !== false
            || evidence.local_evidence.approvals.release_ready !== false) {
            fail('visual-phase evidence did not remain fail-closed');
        }
        if (existedEmpty) fs.rmdirSync(outputDirectory);
        try {
            fs.renameSync(staging, outputDirectory);
        } catch (error) {
            if (existedEmpty && !fs.existsSync(outputDirectory)) fs.mkdirSync(outputDirectory);
            throw error;
        }
        return {
            passedMachineQa: deformation.passed && (validated.clipContract.loop || finalPose?.passed === true),
            approvedForAnimationLibrary: false,
            outputDirectory,
            evidencePath: path.join(outputDirectory, 'visual-phase-qa.json'),
            deformationPath: finalDeformationPath,
            finalPosePath: finalPosePin?.path || null,
            videoPath: videoPin.path,
            evidence,
        };
    } catch (error) {
        fs.rmSync(staging, { recursive: true, force: true });
        throw error;
    }
}

export function parseHorseVisualPhaseQaArgs(argv) {
    const config = {};
    let help = false;
    const take = (index, flag) => {
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) fail(`${flag} requires a value`);
        return argv[index + 1];
    };
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag === '--help' || flag === '-h') help = true;
        else if (flag === '--bundle-dir') config.bundleDirectory = take(index++, flag);
        else if (flag === '--immutable-manifest-sha256') config.expectedImmutableManifestSha256 = take(index++, flag);
        else if (flag === '--fitting-bundle-sha256') config.expectedFittingBundleSha256 = take(index++, flag);
        else if (flag === '--source-model-sha256') config.expectedSourceModelSha256 = take(index++, flag);
        else if (flag === '--three-clip') config.threeClipPath = take(index++, flag);
        else if (flag === '--three-clip-sha256') config.expectedThreeClipSha256 = take(index++, flag);
        else if (flag === '--semantic-id') config.semanticId = take(index++, flag);
        else if (flag === '--three-module') config.threeModule = take(index++, flag);
        else if (flag === '--three-module-sha256') config.expectedThreeModuleSha256 = take(index++, flag);
        else if (flag === '--three-revision') config.expectedThreeRevision = take(index++, flag);
        else if (flag === '--chrome') config.chromeExecutable = take(index++, flag);
        else if (flag === '--ffmpeg') config.ffmpeg = take(index++, flag);
        else if (flag === '--ffprobe') config.ffprobe = take(index++, flag);
        else if (flag === '--output-dir') config.outputDirectory = take(index++, flag);
        else if (flag === '--one-shot') config.loop = false;
        else fail(`unknown option ${flag}`);
    }
    if (help) return { help: true };
    for (const field of [
        'bundleDirectory', 'expectedImmutableManifestSha256', 'expectedFittingBundleSha256',
        'expectedSourceModelSha256', 'threeClipPath', 'expectedThreeClipSha256', 'semanticId',
        'threeModule', 'expectedThreeModuleSha256', 'expectedThreeRevision',
        'chromeExecutable', 'ffmpeg', 'ffprobe', 'outputDirectory',
    ]) if (!config[field]) fail(`missing required option ${field}`);
    return config;
}

function helpText() {
    return `Usage:
  node browser_horse_visual_phase_qa.mjs --bundle-dir DIR \\
    --immutable-manifest-sha256 SHA256 --fitting-bundle-sha256 SHA256 \\
    --source-model-sha256 SHA256 --three-clip FILE --three-clip-sha256 SHA256 \\
    --semantic-id walk_forward --three-module FILE --three-module-sha256 SHA256 \\
    --three-revision 160 --chrome FILE --ffmpeg FILE --ffprobe FILE \\
    --output-dir EMPTY_DIR [--one-shot]

Reconstructs the immutable 344-vertex Horse_2 mesh in Three.js, evaluates and
measures every fitted keyframe and locks the canonical camera. Loop clips also
lock root motion; --one-shot clips allow root motion and require a settled,
grounded final-pose report. The command emits pinned phase PNGs, MP4, reports, and
autorig.animation-visual-phase-qa.v1 evidence. Bundle/model/clip/Three runtime
identities must be supplied externally; self-computed trust is rejected.
Blender is never used. Human
decision remains unset and animation-library/release approvals remain false.

Exit codes: 0 machine QA PASS (still awaiting human review), 3 machine QA FAIL,
2 invalid input/runtime.`;
}

export async function runHorseVisualPhaseQaCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseHorseVisualPhaseQaArgs(argv);
        if (config.help) { streams.stdout.write(`${helpText()}\n`); return 0; }
        const result = await runHorseVisualPhaseQa(config);
        streams.stdout.write(`${JSON.stringify({
            status: result.passedMachineQa ? 'PASS_MACHINE_QA_AWAITING_HUMAN' : 'FAIL_MACHINE_QA',
            approvedForAnimationLibrary: false,
            evidencePath: result.evidencePath,
            videoPath: result.videoPath,
        })}\n`);
        return result.passedMachineQa ? 0 : 3;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = await runHorseVisualPhaseQaCli();

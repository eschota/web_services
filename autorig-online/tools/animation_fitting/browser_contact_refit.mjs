#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import {
    runBrowserFitCanary,
    validateImmutableInputs,
} from './browser_fit_canary.mjs';
import {
    prepareBridgeObservations,
    validateBridgeAndRawPins,
} from './diagnose_browser_hoof_contacts.mjs';
import {
    HOOF_CONTACT_INFERENCE_CONTRACT,
    validatePinnedHoofContactSchedule,
} from '../../static/js/animation-fitting-hoof-contact-inference.js';

const INPUT_SCHEMA = 'autorig-browser-contact-refit-input.v1';
const DIAGNOSTIC_SCHEMA = 'autorig-browser-hoof-contact-diagnostic.v1';
const BRIDGE_REPORT_SCHEMA = 'autorig-browser-fit-canary-bridge-report.v1';
const FIT_SUMMARY_SCHEMA = 'autorig-browser-fit-canary-summary.v1';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const FITTED_ANIMATION_SCHEMA = 'autorig-browser-fitted-animation.v1';
const FINAL_GATE_NAMES = Object.freeze([
    'head_reconstruction_world',
    'rest_seed_alignment_px',
    'final_mean_target_error_px',
    'maximum_target_error_px',
    'bone_length_error_px',
    'joint_limit_violation_rad',
    'contact_slide_px',
    'loop_endpoint_error',
    'hierarchy_segment_drift_world',
    'hierarchy_reprojection_error_px',
    'requested_fitted_point_error_px',
    'unreachable_pixel_ray_ratio',
    'target_sample_coverage',
    'target_error_improved',
    'ordered_deform_heads',
    'four_limb_contacts',
    'three_clip_validate',
    'three_tracks_bound',
    'pinned_contact_schedule',
    'semantic_walk_gait',
    'fitted_walk_contact_slide',
]);

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function string(value, field) {
    if (typeof value !== 'string' || !value.trim()) throw new Error(`${field} must be a non-empty string`);
    return value.trim();
}

function sha256(value, field) {
    const result = string(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function integer(value, field, minimum = 0) {
    const result = Number(value);
    if (!Number.isInteger(result) || result < minimum) throw new Error(`${field} must be an integer >= ${minimum}`);
    return result;
}

function finite(value, field) {
    const result = Number(value);
    if (!Number.isFinite(result)) throw new Error(`${field} must be finite`);
    return result;
}

function parseJsonSnapshot(snapshot, field) {
    try {
        return object(JSON.parse(snapshot.buffer.toString('utf8')), field);
    } catch (error) {
        if (error.message.startsWith(`${field} must be`)) throw error;
        throw new Error(`${field} is not valid JSON: ${error.message}`);
    }
}

function resolveDeclaredPath(ownerPath, declared, field) {
    const value = string(declared, field);
    return path.normalize(path.isAbsolute(value) ? value : path.resolve(path.dirname(ownerPath), value));
}

function fileIntegrity(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    let before;
    let after;
    let buffer;
    try {
        before = fs.statSync(filename);
        buffer = fs.readFileSync(filename);
        after = fs.statSync(filename);
    } catch (error) {
        throw new Error(`${field} is unavailable at ${filename}: ${error.message}`);
    }
    if (!before.isFile() || !after.isFile() || buffer.length <= 0) {
        throw new Error(`${field} must be a non-empty file`);
    }
    if (before.size !== buffer.length || after.size !== buffer.length
        || before.dev !== after.dev || before.ino !== after.ino
        || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while its immutable bytes were read`);
    }
    return {
        path: filename,
        bytes: buffer.length,
        sha256: crypto.createHash('sha256').update(buffer).digest('hex'),
        buffer,
    };
}

function pinnedFile(manifestPath, rowValue, field) {
    const row = object(rowValue, field);
    const filename = resolveDeclaredPath(manifestPath, row.path, `${field}.path`);
    const integrity = fileIntegrity(filename, field);
    if (integrity.bytes !== integer(row.bytes, `${field}.bytes`, 1)) {
        throw new Error(`${field} byte count does not match its immutable pin`);
    }
    if (integrity.sha256 !== sha256(row.sha256, `${field}.sha256`)) {
        throw new Error(`${field} SHA-256 does not match its immutable pin`);
    }
    return {
        path: integrity.path,
        bytes: integrity.bytes,
        sha256: integrity.sha256,
        json: parseJsonSnapshot(integrity, field),
    };
}

function samePath(first, second) {
    const normalize = (value) => {
        const resolved = path.normalize(path.resolve(value));
        return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
    };
    return normalize(first) === normalize(second);
}

function requireEqual(actual, expected, field) {
    if (actual !== expected) throw new Error(`${field} does not match the immutable contact-refit contract`);
}

function validateUnconstrainedDiagnostic({ summary, bridgeReport, pins }) {
    if (summary.schema !== FIT_SUMMARY_SCHEMA || summary.status !== 'PASS_BROWSER_FIT_GATES') {
        throw new Error('initial fit summary must be a PASS browser-fit diagnostic');
    }
    if (summary.browserOnly !== true || summary.blenderUsed !== false || summary.mixerUsed !== false
        || summary.fittingMode !== 'unconstrained_diagnostic') {
        throw new Error('initial fit summary is not a browser-only unconstrained diagnostic');
    }
    if (summary.approvedForAnimationLibrary !== false || summary.approvedForBrowserContactFit !== false) {
        throw new Error('initial unconstrained fit must not carry approval');
    }
    if (summary.gates?.passed !== true || summary.observations?.contactCount !== 0) {
        throw new Error('initial unconstrained fit must pass structural gates with zero contacts');
    }
    const contactGate = summary.gates.results?.find((result) => result?.name === 'four_limb_contacts');
    if (!contactGate || contactGate.enforced !== false || contactGate.actual !== 0) {
        throw new Error('initial fit does not prove the unconstrained four-limb diagnostic state');
    }
    if (bridgeReport.schema !== BRIDGE_REPORT_SCHEMA || bridgeReport.status !== 'VALIDATED'
        || bridgeReport.browserOnly !== true || bridgeReport.blenderUsed !== false
        || bridgeReport.mixerUsed !== false || bridgeReport.fittingMode !== 'unconstrained_diagnostic'
        || bridgeReport.sourceContacts !== 0 || bridgeReport.preparedContacts !== 0) {
        throw new Error('bridge report is not the matching unconstrained browser diagnostic');
    }
    for (const [summaryField, pinField] of [
        ['sourceVideoSha256', 'sourceVideoSha256'],
        ['fittingBundleSha256', 'fittingBundleSha256'],
        ['immutableManifestSha256', 'immutableManifestSha256'],
        ['sourceModelSha256', 'sourceModelSha256'],
        ['skeletonSha256', 'sourceSkeletonSha256'],
        ['observationsSha256', 'observationsSha256'],
    ]) {
        requireEqual(summary.inputs?.[summaryField], pins[pinField], `initial fit inputs.${summaryField}`);
        requireEqual(bridgeReport.inputs?.[summaryField], pins[pinField], `bridge report inputs.${summaryField}`);
    }
}

function validateDiagnostic({ diagnostic, bridgeReport, semanticObservations, pins, files }) {
    if (diagnostic.schema !== DIAGNOSTIC_SCHEMA || diagnostic.status !== 'PASS') {
        throw new Error('contact diagnostic must be PASS');
    }
    requireEqual(diagnostic.inputs?.observations?.sha256, pins.observationsSha256, 'diagnostic observations SHA-256');
    requireEqual(diagnostic.inputs?.bridgeReport?.sha256, pins.bridgeReportSha256, 'diagnostic bridge-report SHA-256');
    requireEqual(diagnostic.inputs?.sourceVideo?.sha256, pins.sourceVideoSha256, 'diagnostic source-video SHA-256');
    requireEqual(diagnostic.inputs?.bundleManifest?.sha256, pins.fittingBundleSha256, 'diagnostic fitting-bundle SHA-256');
    requireEqual(diagnostic.inputs?.immutableManifest?.sha256, pins.immutableManifestSha256, 'diagnostic immutable-manifest SHA-256');
    requireEqual(diagnostic.inputs?.sourceSkeletonSha256, pins.sourceSkeletonSha256, 'diagnostic skeleton SHA-256');
    requireEqual(diagnostic.inputs?.sourceModelSha256, pins.sourceModelSha256, 'diagnostic source-model SHA-256');
    requireEqual(diagnostic.inputs?.trackerBackend, HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend, 'diagnostic tracker backend');
    requireEqual(diagnostic.inputs?.segmenterBackend, HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend, 'diagnostic segmenter backend');
    if (diagnostic.inputs?.frames !== semanticObservations.frame_count
        || diagnostic.inputs?.fps !== semanticObservations.fps
        || diagnostic.inputs?.loop !== true) {
        throw new Error('contact diagnostic timing does not match observations');
    }
    if (!Array.isArray(diagnostic.bridge?.hoofTracks) || diagnostic.bridge.hoofTracks.length !== 4) {
        throw new Error('contact diagnostic must pin all four semantic hoof mappings');
    }
    const declaredFeet = new Set();
    const bridgeMappingBySemantic = new Map();
    if (!Array.isArray(bridgeReport.mappings)) throw new Error('bridge report mappings are missing');
    bridgeReport.mappings.forEach((mapping, index) => {
        if (!mapping || typeof mapping.semanticAnchorId !== 'string'
            || bridgeMappingBySemantic.has(mapping.semanticAnchorId)) {
            throw new Error(`bridge report mapping ${index} is invalid or duplicated`);
        }
        bridgeMappingBySemantic.set(mapping.semanticAnchorId, mapping);
    });
    diagnostic.bridge.hoofTracks.forEach((row, index) => {
        const foot = row?.foot;
        if (!HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.includes(foot)
            || row.semanticId !== `${foot}.hoof`
            || typeof row.sourceTrackId !== 'string' || !row.sourceTrackId
            || typeof row.sourceAnchorId !== 'string' || !row.sourceAnchorId
            || typeof row.sourceBone !== 'string' || !row.sourceBone) {
            throw new Error(`contact diagnostic hoof mapping ${index} is invalid`);
        }
        if (declaredFeet.has(foot)) throw new Error(`contact diagnostic repeats hoof mapping ${foot}`);
        declaredFeet.add(foot);
        const bridgeMapping = bridgeMappingBySemantic.get(row.semanticId);
        if (!bridgeMapping || bridgeMapping.limb !== foot
            || bridgeMapping.sourceTrackId !== row.sourceTrackId
            || bridgeMapping.sourceAnchorId !== row.sourceAnchorId
            || bridgeMapping.sourceBone !== row.sourceBone) {
            throw new Error(`contact diagnostic hoof mapping ${foot} does not match the pinned bridge mapping`);
        }
    });
    validatePinnedHoofContactSchedule({ observations: semanticObservations, schedule: diagnostic.schedule });
    requireEqual(files.diagnostic.sha256, pins.diagnosticSha256, 'diagnostic file SHA-256');
}

function finiteArray(value, field, minimumLength = 1) {
    if (!Array.isArray(value) || value.length < minimumLength || value.some((item) => !Number.isFinite(Number(item)))) {
        throw new Error(`${field} must contain at least ${minimumLength} finite values`);
    }
    return value.map(Number);
}

function validateSerializedTrack(trackValue, field, frameCount, durationSeconds, allowedTypes) {
    const track = object(trackValue, field);
    const name = string(track.name, `${field}.name`);
    if (!allowedTypes.includes(track.type)) throw new Error(`${field}.type is not an allowed browser track type`);
    const times = finiteArray(track.times, `${field}.times`, frameCount);
    if (times.length !== frameCount || times[0] !== 0
        || Math.abs(times.at(-1) - durationSeconds) > 1e-9
        || times.some((time, index) => index > 0 && time <= times[index - 1])) {
        throw new Error(`${field}.times do not preserve the exact fitted timeline`);
    }
    const itemSize = track.type === 'quaternion' ? 4 : 3;
    const values = finiteArray(track.values, `${field}.values`, frameCount * itemSize);
    if (values.length !== frameCount * itemSize) throw new Error(`${field}.values length does not match its timeline`);
    return { name, type: track.type, times, values };
}

function validateFittedAnimation(value, summary) {
    const fitted = object(value, 'fittedAnimation');
    if (fitted.schema !== FITTED_ANIMATION_SCHEMA || fitted.loop !== true) {
        throw new Error(`fitted animation must be loop=true ${FITTED_ANIMATION_SCHEMA}`);
    }
    const frameCount = integer(fitted.frameCount, 'fittedAnimation.frameCount', 8);
    const fps = finite(fitted.fps, 'fittedAnimation.fps');
    const durationSeconds = finite(fitted.durationSeconds, 'fittedAnimation.durationSeconds');
    if (fps <= 0 || Math.abs(durationSeconds - (frameCount - 1) / fps) > 1e-9) {
        throw new Error('fitted animation timing contract is invalid');
    }
    if (summary.fit?.frameCount !== frameCount || summary.fit?.durationSeconds !== durationSeconds) {
        throw new Error('fitted animation timing does not match fit-summary.json');
    }
    const tracks = [
        ...(Array.isArray(fitted.tracks) ? fitted.tracks : []),
        ...(Array.isArray(fitted.positionTracks) ? fitted.positionTracks : []),
        ...(fitted.rootTrack ? [fitted.rootTrack] : []),
    ];
    if (!Array.isArray(fitted.tracks) || !fitted.tracks.length || tracks.length === 0) {
        throw new Error('fitted animation has no browser tracks');
    }
    const names = new Set();
    tracks.forEach((track, index) => {
        const normalized = validateSerializedTrack(
            track,
            `fittedAnimation.tracks[${index}]`,
            frameCount,
            durationSeconds,
            ['quaternion', 'vector'],
        );
        if (names.has(normalized.name)) throw new Error(`fitted animation repeats track ${normalized.name}`);
        names.add(normalized.name);
    });
    if (summary.fit?.quaternionTracks !== fitted.tracks.length
        || summary.fit?.positionTracks !== (fitted.positionTracks || []).length) {
        throw new Error('fitted animation track inventory does not match fit-summary.json');
    }
    const qa = object(fitted.qa, 'fittedAnimation.qa');
    for (const field of [
        'targetSamples', 'initialMeanTargetErrorPx', 'finalMeanTargetErrorPx', 'maximumTargetErrorPx',
        'maximumBoneLengthErrorPx', 'maximumJointLimitViolationRad', 'maximumContactSlidePx', 'loopEndpointError',
    ]) finite(qa[field], `fittedAnimation.qa.${field}`);
    if (JSON.stringify(qa) !== JSON.stringify(summary.fit?.qa)) {
        throw new Error('fitted animation QA does not match fit-summary.json');
    }
    if (!Array.isArray(fitted.frames) || fitted.frames.length !== frameCount) {
        throw new Error('fitted animation debug frames do not match frameCount');
    }
    fitted.frames.forEach((frameValue, frameIndex) => {
        const frame = object(frameValue, `fittedAnimation.frames[${frameIndex}]`);
        if (frame.frame !== frameIndex) throw new Error(`fitted animation frame ${frameIndex} lost chronology`);
        const limbs = object(frame.limbs, `fittedAnimation.frames[${frameIndex}].limbs`);
        FOOT_ORDER_FOR_VALIDATION.forEach((foot) => {
            const points = object(limbs[foot], `fittedAnimation frame ${frameIndex} ${foot}`).points;
            if (!Array.isArray(points) || points.length < 3) throw new Error(`fitted animation frame ${frameIndex} is missing ${foot}`);
            points.forEach((point, pointIndex) => finiteArray(point, `fittedAnimation frame ${frameIndex} ${foot} point ${pointIndex}`, 2));
        });
    });
    return { frameCount, fps, durationSeconds, trackNames: names };
}

const FOOT_ORDER_FOR_VALIDATION = Object.freeze([...HOOF_CONTACT_INFERENCE_CONTRACT.footOrder]);

function validateThreeClip(value, summary, fittedContract) {
    const clip = object(value, 'threeClip');
    if (string(clip.name, 'threeClip.name') !== summary.hierarchyClip?.name
        || finite(clip.duration, 'threeClip.duration') !== fittedContract.durationSeconds
        || typeof clip.uuid !== 'string' || !clip.uuid
        || !Number.isFinite(Number(clip.blendMode))) {
        throw new Error('Three clip header does not match the validated hierarchy summary');
    }
    if (!Array.isArray(clip.tracks) || !clip.tracks.length
        || clip.tracks.length !== summary.hierarchyClip?.tracks) {
        throw new Error('Three clip track inventory does not match fit-summary.json');
    }
    const names = new Set();
    const byBone = new Map();
    clip.tracks.forEach((track, index) => {
        const normalized = validateSerializedTrack(
            track,
            `threeClip.tracks[${index}]`,
            fittedContract.frameCount,
            fittedContract.durationSeconds,
            ['quaternion', 'vector'],
        );
        if (names.has(normalized.name)) throw new Error(`Three clip repeats track ${normalized.name}`);
        names.add(normalized.name);
        const suffix = normalized.type === 'quaternion' ? '.quaternion' : '.position';
        if (!normalized.name.endsWith(suffix)) throw new Error(`Three clip track ${normalized.name} has an invalid binding`);
        const bone = normalized.name.slice(0, -suffix.length);
        const types = byBone.get(bone) || new Set();
        types.add(normalized.type);
        byBone.set(bone, types);
    });
    if ([...byBone.values()].some((types) => types.size !== 2
        || !types.has('quaternion') || !types.has('vector'))) {
        throw new Error('Three clip must contain quaternion and position tracks for every bound bone');
    }
}

function validateFinalPassArtifacts({ result, artifacts, validated }) {
    const summary = artifacts.fitSummaryPath.json;
    const bridge = artifacts.bridgeReportPath.json;
    if (summary.schema !== FIT_SUMMARY_SCHEMA || summary.status !== 'PASS_BROWSER_CONTACT_REFIT_GATES'
        || summary.gates?.passed !== true || summary.approvedForBrowserContactFit !== true
        || summary.approvedForAnimationLibrary !== false || summary.browserOnly !== true
        || summary.blenderUsed !== false || summary.mixerUsed !== false
        || summary.fittingMode !== 'contact_constrained_refit') {
        throw new Error('contact refit runner returned an invalid final PASS contract');
    }
    if (result.fitSummary?.status !== summary.status || result.passed !== true) {
        throw new Error('in-memory contact refit result disagrees with staged fit-summary.json');
    }
    const results = Array.isArray(summary.gates.results) ? summary.gates.results : [];
    const byName = new Map();
    results.forEach((gate, index) => {
        if (!gate || typeof gate.name !== 'string' || byName.has(gate.name)) {
            throw new Error(`final gate ${index} is invalid or duplicated`);
        }
        byName.set(gate.name, gate);
    });
    if (byName.size !== FINAL_GATE_NAMES.length
        || FINAL_GATE_NAMES.some((name) => !byName.has(name))) {
        throw new Error('final fit summary does not contain the exact contact-refit gate inventory');
    }
    const declaredGatePasses = (gate) => {
        if (gate.comparator === '<=') {
            return Number.isFinite(Number(gate.actual))
                && Number.isFinite(Number(gate.threshold))
                && Number(gate.actual) <= Number(gate.threshold);
        }
        if (gate.comparator === '>=') {
            return Number.isFinite(Number(gate.actual))
                && Number.isFinite(Number(gate.threshold))
                && Number(gate.actual) >= Number(gate.threshold);
        }
        if (gate.comparator === '===') return gate.actual === gate.threshold;
        return false;
    };
    if ([...byName.values()].some((gate) => gate.passed !== true || !declaredGatePasses(gate))
        || byName.get('four_limb_contacts').actual !== 4
        || byName.get('four_limb_contacts').enforced !== true
        || byName.get('pinned_contact_schedule').actual !== 'PASS'
        || byName.get('semantic_walk_gait').actual !== true
        || Number(byName.get('contact_slide_px').actual) > Number(byName.get('contact_slide_px').threshold)
        || Number(byName.get('fitted_walk_contact_slide').actual) > Number(byName.get('fitted_walk_contact_slide').threshold)) {
        throw new Error('final fit summary contains a failed or forged required contact gate');
    }
    const refit = object(summary.contactRefit, 'finalFitSummary.contactRefit');
    if (refit.scheduleStatus !== 'PASS' || refit.semanticGaitQa?.accepted !== true
        || refit.semanticGaitQa?.simultaneousSwingFrameCount !== 0
        || refit.fittedWalkQa?.status !== 'PASS'
        || !Array.isArray(refit.fittedWalkQa?.failures) || refit.fittedWalkQa.failures.length
        || Number(refit.fittedWalkQa.maximumContactSlideRatio) > Number(refit.fittedWalkQa.thresholdRatio)) {
        throw new Error('final fit summary contact-refit QA is not PASS');
    }
    const approvalExclusions = Array.isArray(summary.approvalExclusions)
        ? [...summary.approvalExclusions].sort()
        : [];
    if (summary.observations?.contactCount !== 4
        || JSON.stringify(approvalExclusions) !== JSON.stringify([
            'fixed_camera_visual_phase_qa',
            'target_mesh_deformation_qa',
        ])) {
        throw new Error('final fit summary does not preserve four approved contact constraints');
    }
    if (bridge.schema !== BRIDGE_REPORT_SCHEMA || bridge.status !== 'VALIDATED'
        || bridge.browserOnly !== true || bridge.blenderUsed !== false || bridge.mixerUsed !== false
        || bridge.fittingMode !== 'contact_constrained_refit' || bridge.preparedContacts !== 4) {
        throw new Error('final bridge report is not the contact-constrained browser bridge');
    }
    const expectedInputPins = [
        ['sourceVideoSha256', 'sourceVideoSha256'],
        ['fittingBundleSha256', 'fittingBundleSha256'],
        ['immutableManifestSha256', 'immutableManifestSha256'],
        ['sourceModelSha256', 'sourceModelSha256'],
        ['skeletonSha256', 'sourceSkeletonSha256'],
        ['observationsSha256', 'observationsSha256'],
    ];
    expectedInputPins.forEach(([inputField, pinField]) => {
        requireEqual(summary.inputs?.[inputField], validated.pins[pinField], `final summary inputs.${inputField}`);
        requireEqual(bridge.inputs?.[inputField], validated.pins[pinField], `final bridge inputs.${inputField}`);
    });
    if (!samePath(summary.inputs?.bundleDirectory, validated.bundleDirectory)
        || !samePath(bridge.inputs?.bundleDirectory, validated.bundleDirectory)
        || !samePath(summary.inputs?.observationsPath, validated.observationsPath)
        || !samePath(bridge.inputs?.observationsPath, validated.observationsPath)) {
        throw new Error('final browser artifacts do not resolve to the pinned contact-refit inputs');
    }
    const contactProvenance = object(refit.provenance, 'finalFitSummary.contactRefit.provenance');
    if (contactProvenance.schema !== HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitProvenance
        || contactProvenance.source !== 'immutable_pass_diagnostic'
        || contactProvenance.browserOnly !== true || contactProvenance.blenderUsed !== false
        || contactProvenance.mixerUsed !== false) {
        throw new Error('final contact-refit provenance is not the browser-only immutable diagnostic contract');
    }
    for (const [pinField, expected] of Object.entries(validated.pins)) {
        requireEqual(contactProvenance[pinField], expected, `final contact-refit provenance.${pinField}`);
    }
    const fittedContract = validateFittedAnimation(artifacts.fittedAnimationPath.json, summary);
    validateThreeClip(artifacts.threeClipPath.json, summary, fittedContract);
    return { summary, bridge };
}

/** Validate the entire immutable chain before Three.js or the solver is loaded. */
export function validateContactRefitInputs({ inputManifestPath: manifestValue, expectedManifestSha256 }) {
    const manifestSnapshot = fileIntegrity(manifestValue, 'contact-refit input manifest');
    if (manifestSnapshot.sha256 !== sha256(expectedManifestSha256, 'expectedManifestSha256')) {
        throw new Error('contact-refit input manifest SHA-256 does not match the external pin');
    }
    const manifest = parseJsonSnapshot(manifestSnapshot, 'contactRefitInput');
    const manifestFile = {
        path: manifestSnapshot.path,
        bytes: manifestSnapshot.bytes,
        sha256: manifestSnapshot.sha256,
    };
    if (manifest.schema !== INPUT_SCHEMA || manifest.browserOnly !== true
        || manifest.blenderUsed !== false || manifest.mixerUsed !== false) {
        throw new Error(`contact-refit input must use ${INPUT_SCHEMA} and browser-only runtime flags`);
    }
    const inputRows = object(manifest.inputs, 'contactRefitInput.inputs');
    const files = {
        observations: pinnedFile(manifestFile.path, inputRows.observations, 'contactRefitInput.inputs.observations'),
        bridgeReport: pinnedFile(manifestFile.path, inputRows.bridgeReport, 'contactRefitInput.inputs.bridgeReport'),
        initialFitSummary: pinnedFile(manifestFile.path, inputRows.initialFitSummary, 'contactRefitInput.inputs.initialFitSummary'),
        diagnostic: pinnedFile(manifestFile.path, inputRows.contactDiagnostic, 'contactRefitInput.inputs.contactDiagnostic'),
    };
    const bundleDirectory = resolveDeclaredPath(
        manifestFile.path,
        inputRows.bundleDirectory,
        'contactRefitInput.inputs.bundleDirectory',
    );
    const pinsValue = object(manifest.pins, 'contactRefitInput.pins');
    const pinNames = [
        'observationsSha256', 'bridgeReportSha256', 'initialFitSummarySha256', 'diagnosticSha256',
        'sourceVideoSha256', 'fittingBundleSha256', 'immutableManifestSha256',
        'sourceModelSha256', 'sourceSkeletonSha256',
    ];
    const pins = Object.fromEntries(pinNames.map((name) => [name, sha256(pinsValue[name], `contactRefitInput.pins.${name}`)]));
    requireEqual(files.observations.sha256, pins.observationsSha256, 'observations file SHA-256');
    requireEqual(files.bridgeReport.sha256, pins.bridgeReportSha256, 'bridge-report file SHA-256');
    requireEqual(files.initialFitSummary.sha256, pins.initialFitSummarySha256, 'initial-fit file SHA-256');
    requireEqual(files.diagnostic.sha256, pins.diagnosticSha256, 'diagnostic file SHA-256');

    const raw = files.observations.json;
    const bridgeReport = files.bridgeReport.json;
    const initialFitSummary = files.initialFitSummary.json;
    const diagnostic = files.diagnostic.json;
    const immutable = validateImmutableInputs({
        bundleDirectory,
        observationsPath: files.observations.path,
    });
    const bridgeIntegrity = validateBridgeAndRawPins({
        raw,
        report: bridgeReport,
        observationPath: files.observations.path,
        bridgeReportPath: files.bridgeReport.path,
    });
    if (!samePath(immutable.bundleDirectory, bundleDirectory)
        || !samePath(bridgeReport.inputs?.bundleDirectory, bundleDirectory)
        || !samePath(bridgeReport.inputs?.observationsPath, files.observations.path)) {
        throw new Error('contact-refit paths do not resolve to the immutable browser-fit inputs');
    }
    for (const [name, actual] of [
        ['sourceVideoSha256', immutable.integrity.sourceVideoSha256],
        ['fittingBundleSha256', immutable.integrity.fittingBundleSha256],
        ['immutableManifestSha256', immutable.integrity.immutableManifestSha256],
        ['sourceModelSha256', immutable.integrity.sourceModelSha256],
        ['sourceSkeletonSha256', immutable.integrity.skeletonSha256],
        ['observationsSha256', immutable.integrity.observationsSha256],
    ]) requireEqual(actual, pins[name], `contact-refit pin ${name}`);
    requireEqual(bridgeIntegrity.bridgeReport.sha256, pins.bridgeReportSha256, 'validated bridge-report SHA-256');
    requireEqual(bridgeIntegrity.observations.sha256, pins.observationsSha256, 'validated observations SHA-256');
    validateUnconstrainedDiagnostic({ summary: initialFitSummary, bridgeReport, pins });
    const semanticObservations = prepareBridgeObservations(raw, bridgeReport);
    validateDiagnostic({ diagnostic, bridgeReport, semanticObservations, pins, files });

    return {
        manifest,
        manifestIntegrity: manifestFile,
        bundleDirectory,
        observationsPath: files.observations.path,
        files,
        pins: {
            inputManifestSha256: manifestFile.sha256,
            ...pins,
        },
        schedule: structuredClone(diagnostic.schedule),
    };
}

export function parseContactRefitArgs(argv) {
    const config = { fit: {}, gates: {} };
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        const take = () => {
            if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
            index += 1;
            return argv[index];
        };
        if (flag === '--help') help = true;
        else if (flag === '--input-manifest') config.inputManifestPath = take();
        else if (flag === '--input-manifest-sha256') config.expectedManifestSha256 = sha256(take(), flag);
        else if (flag === '--three-module') config.threeModule = take();
        else if (flag === '--output-dir') config.outputDirectory = take();
        else if (flag === '--clip-name') config.clipName = take();
        else if (flag === '--iterations') config.fit.iterations = integer(take(), flag, 1);
        else if (flag === '--tolerance') config.fit.tolerance = finite(take(), flag);
        else if (flag === '--joint-attraction') config.fit.jointAttraction = finite(take(), flag);
        else if (flag === '--smoothing-radius') config.fit.smoothingRadius = integer(take(), flag);
        else if (flag === '--loop-blend-frames') config.fit.loopBlendFrames = integer(take(), flag, 1);
        else if (flag === '--max-final-mean-target-error-px') config.gates.maximumFinalMeanTargetErrorPx = finite(take(), flag);
        else if (flag === '--max-target-error-px') config.gates.maximumTargetErrorPx = finite(take(), flag);
        else if (flag === '--max-requested-point-error-px') config.gates.maximumRequestedFittedPointErrorPx = finite(take(), flag);
        else throw new Error(`unknown option ${flag}`);
    }
    if (help) return { help: true };
    ['inputManifestPath', 'expectedManifestSha256', 'threeModule', 'outputDirectory'].forEach((field) => {
        if (!config[field]) throw new Error(`missing required option ${field}`);
    });
    return config;
}

function helpText() {
    return `Usage:
  node browser_contact_refit.mjs --input-manifest FILE \\
    --input-manifest-sha256 SHA256 --three-module FILE --output-dir EMPTY_DIR

The immutable manifest pins the actionless bundle, raw observations, initial
unconstrained bridge/fit reports and PASS hoof-contact diagnostic. The command
reruns the pure browser solver with all four semantic hoof contacts and emits
fitted-animation.json plus three-clip.json only after structural, gait,
support, contact-slide and gateFittedWalk gates all PASS.

Runtime invariants: browserOnly=true, blenderUsed=false, mixerUsed=false.
Exit codes: 0 final contact refit PASS, 2 invalid input/runtime, 3 QA FAIL.`;
}

export async function runBrowserContactRefit(configuration, dependencies = {}) {
    const config = object(configuration, 'configuration');
    const validated = validateContactRefitInputs({
        inputManifestPath: config.inputManifestPath,
        expectedManifestSha256: config.expectedManifestSha256,
    });
    const runner = dependencies.runBrowserFitCanary || runBrowserFitCanary;
    const outputDirectory = path.resolve(string(config.outputDirectory, 'configuration.outputDirectory'));
    const parentDirectory = path.dirname(outputDirectory);
    if (!fs.existsSync(parentDirectory) || !fs.statSync(parentDirectory).isDirectory()) {
        throw new Error(`contact-refit output parent does not exist: ${parentDirectory}`);
    }
    const finalExistedEmpty = fs.existsSync(outputDirectory);
    if (finalExistedEmpty) {
        if (!fs.statSync(outputDirectory).isDirectory() || fs.readdirSync(outputDirectory).length) {
            throw new Error(`contact-refit output must be absent or empty: ${outputDirectory}`);
        }
    }
    const stagingDirectory = `${outputDirectory}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    const cleanupStaging = () => fs.rmSync(stagingDirectory, { recursive: true, force: true });
    try {
        const result = await runner({
            bundleDirectory: validated.bundleDirectory,
            observationsPath: validated.observationsPath,
            threeModule: config.threeModule,
            outputDirectory: stagingDirectory,
            clipName: config.clipName || 'Horse_LTX_Browser_Contact_Refit',
            fit: { ...config.fit, loop: true },
            gates: { ...config.gates, requireFourLimbContacts: true },
            emitFittedAnimation: true,
            emitThreeClip: true,
        }, {
            contactRefit: {
                schedule: validated.schedule,
                pins: validated.pins,
            },
        });
        if (result.passed !== true) {
            cleanupStaging();
            return { ...result, outputs: {} };
        }
        if (result.fitSummary?.status !== 'PASS_BROWSER_CONTACT_REFIT_GATES'
            || result.fitSummary?.approvedForBrowserContactFit !== true
            || result.fitSummary?.browserOnly !== true
            || result.fitSummary?.blenderUsed !== false
            || result.fitSummary?.mixerUsed !== false) {
            throw new Error('contact refit runner returned an invalid final PASS contract');
        }
        const expectedNames = {
            bridgeReportPath: 'bridge-report.json',
            fitSummaryPath: 'fit-summary.json',
            fittedAnimationPath: 'fitted-animation.json',
            threeClipPath: 'three-clip.json',
        };
        const stagedNames = fs.readdirSync(stagingDirectory).sort();
        const expectedFilenames = Object.values(expectedNames).sort();
        if (JSON.stringify(stagedNames) !== JSON.stringify(expectedFilenames)) {
            throw new Error('contact refit PASS staging directory contains an unexpected artifact set');
        }
        const stagedArtifacts = {};
        Object.entries(expectedNames).forEach(([field, expectedName]) => {
            const actual = result.outputs?.[field];
            if (typeof actual !== 'string'
                || !samePath(actual, path.join(stagingDirectory, expectedName))) {
                throw new Error(`contact refit PASS is missing staged ${expectedName}`);
            }
            const snapshot = fileIntegrity(actual, `staged ${expectedName}`);
            stagedArtifacts[field] = {
                path: snapshot.path,
                bytes: snapshot.bytes,
                sha256: snapshot.sha256,
                json: parseJsonSnapshot(snapshot, `staged ${expectedName}`),
            };
        });
        const finalContract = validateFinalPassArtifacts({ result, artifacts: stagedArtifacts, validated });
        if (finalExistedEmpty) fs.rmdirSync(outputDirectory);
        try {
            fs.renameSync(stagingDirectory, outputDirectory);
        } catch (error) {
            if (finalExistedEmpty && !fs.existsSync(outputDirectory)) fs.mkdirSync(outputDirectory);
            throw error;
        }
        return {
            ...result,
            fitSummary: finalContract.summary,
            bridgeReport: finalContract.bridge,
            outputs: Object.fromEntries(Object.entries(expectedNames).map(([field, filename]) => [
                field,
                path.join(outputDirectory, filename),
            ])),
        };
    } catch (error) {
        cleanupStaging();
        throw error;
    }
}

export async function runCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseContactRefitArgs(argv);
        if (config.help) {
            streams.stdout.write(`${helpText()}\n`);
            return 0;
        }
        const result = await runBrowserContactRefit(config);
        streams.stdout.write(`${JSON.stringify({
            status: result.fitSummary.status,
            outputs: result.outputs,
            failedGates: result.fitSummary.gates.results
                .filter((gate) => !gate.passed)
                .map((gate) => gate.name),
        })}\n`);
        return result.passed ? 0 : 3;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = await runCli();

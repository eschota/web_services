#!/usr/bin/env node

import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import {
    gateFittedTrot,
    HOOF_CONTACT_INFERENCE_CONTRACT,
    validatePinnedHorseTrotDiagnostic,
} from '../../static/js/animation-fitting-hoof-contact-inference.js';
import {
    assessNonRootClipMotion,
    runBrowserFitCanary,
    validateImmutableInputs,
} from './browser_fit_canary.mjs';
import {
    prepareBridgeObservations,
    validateBridgeAndRawPins,
} from './diagnose_browser_hoof_contacts.mjs';

const INPUT_SCHEMA = 'autorig-browser-horse-trot-contact-refit-input.v1';
const SUMMARY_SCHEMA = 'autorig-browser-fit-canary-summary.v1';
const BRIDGE_SCHEMA = 'autorig-browser-fit-canary-bridge-report.v1';
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

const REQUIRED_FINAL_GATES = Object.freeze([
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
    'quaternion_angular_velocity_seam_rad_per_second',
    'position_velocity_seam_world_per_second',
    'unreachable_pixel_ray_ratio',
    'target_sample_coverage',
    'target_error_improved',
    'ordered_deform_heads',
    'four_limb_contacts',
    'three_clip_validate',
    'three_tracks_bound',
    'c1_quaternion_pose_seam_rad',
    'c1_position_pose_seam_world',
    'pinned_trot_contact_diagnostic',
    'semantic_trot_source_gait',
    'fitted_projected_trot_gait',
    'fitted_trot_contact_slide_ratio',
    'trot_non_root_dynamic_tracks',
    'trot_non_root_dynamic_bones',
]);

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${field} must be an object`);
    }
    return value;
}

function string(value, field) {
    if (typeof value !== 'string' || !value) throw new Error(`${field} must be a non-empty string`);
    return value;
}

function sha256(value, field) {
    const result = string(value, field);
    if (!SHA256_PATTERN.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function hash(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function samePath(first, second) {
    const normalize = (value) => {
        const resolved = path.normalize(path.resolve(value));
        return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
    };
    return normalize(first) === normalize(second);
}

function snapshot(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    const before = fs.lstatSync(filename);
    if (!before.isFile() || before.isSymbolicLink() || before.size <= 0) {
        throw new Error(`${field} must be a non-empty regular file, not a symlink`);
    }
    const buffer = fs.readFileSync(filename);
    const after = fs.lstatSync(filename);
    if (!after.isFile() || after.isSymbolicLink() || before.dev !== after.dev || before.ino !== after.ino
        || before.size !== buffer.length || after.size !== buffer.length || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while its immutable bytes were read`);
    }
    return { path: filename, bytes: buffer.length, sha256: hash(buffer), buffer };
}

function jsonSnapshot(filenameValue, field) {
    const result = snapshot(filenameValue, field);
    try {
        result.json = JSON.parse(result.buffer.toString('utf8'));
    } catch (error) {
        throw new Error(`${field} is not valid JSON: ${error.message}`);
    }
    return result;
}

function declaredPath(ownerPath, value, field) {
    const declared = string(value, field);
    return path.isAbsolute(declared)
        ? path.normalize(declared)
        : path.resolve(path.dirname(ownerPath), declared);
}

function pinnedJson(owner, rowValue, field) {
    const row = object(rowValue, field);
    const resolved = declaredPath(owner, row.path, `${field}.path`);
    const result = jsonSnapshot(resolved, field);
    if (result.bytes !== row.bytes || result.sha256 !== sha256(row.sha256, `${field}.sha256`)) {
        throw new Error(`${field} bytes do not match its immutable pin`);
    }
    return result;
}

function equal(actual, expected, field) {
    if (actual !== expected) throw new Error(`${field} does not match its immutable pin`);
}

function validateInitialFit(summary, pins) {
    if (summary.schema !== SUMMARY_SCHEMA || summary.status !== 'PASS_BROWSER_FIT_GATES'
        || summary.browserOnly !== true || summary.blenderUsed !== false || summary.mixerUsed !== false
        || summary.fittingMode !== 'unconstrained_diagnostic'
        || summary.approvedForBrowserContactFit !== false
        || summary.approvedForAnimationLibrary !== false
        || summary.gates?.passed !== true
        || !Array.isArray(summary.gates?.results)
        || summary.gates.results.some((gate) => gate?.passed !== true)
        || Number(summary.observations?.contactCount) !== 0) {
        throw new Error('initial TROT fit must be a PASS browser-only unconstrained diagnostic with zero contacts');
    }
    for (const [field, expected] of Object.entries({
        observationsSha256: pins.observationsSha256,
        fittingBundleSha256: pins.fittingBundleSha256,
        immutableManifestSha256: pins.immutableManifestSha256,
        sourceVideoSha256: pins.sourceVideoSha256,
        sourceModelSha256: pins.sourceModelSha256,
        skeletonSha256: pins.sourceSkeletonSha256,
    })) equal(summary.inputs?.[field], expected, `initial fit inputs.${field}`);
}

function validateDiagnosticPins(diagnostic, files, pins, integrity) {
    const inputs = object(diagnostic.inputs, 'TROT diagnostic.inputs');
    const verifyRow = (rowValue, expected, field) => {
        const row = object(rowValue, field);
        equal(row.sha256, expected.sha256, `${field}.sha256`);
        equal(row.bytes, expected.bytes, `${field}.bytes`);
        if (!samePath(row.path, expected.path)) throw new Error(`${field}.path does not match immutable input`);
    };
    verifyRow(inputs.observations, files.observations, 'TROT diagnostic observations');
    verifyRow(inputs.bridgeReport, files.bridgeReport, 'TROT diagnostic bridge');
    verifyRow(inputs.sourceVideo, integrity.sourceVideo, 'TROT diagnostic source video');
    verifyRow(inputs.bundleManifest, integrity.bundleManifest, 'TROT diagnostic bundle');
    verifyRow(inputs.immutableManifest, integrity.immutableManifest, 'TROT diagnostic immutable manifest');
    equal(inputs.sourceSkeletonSha256, pins.sourceSkeletonSha256, 'TROT diagnostic skeleton SHA-256');
    equal(inputs.sourceModelSha256, pins.sourceModelSha256, 'TROT diagnostic model SHA-256');
    equal(inputs.trackerBackend, HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend, 'TROT diagnostic tracker backend');
    equal(inputs.segmenterBackend, HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend, 'TROT diagnostic segmenter backend');
    if (diagnostic.runtime?.browserOnly !== true || diagnostic.runtime?.blenderUsed !== false) {
        throw new Error('TROT diagnostic runtime must be browser-only and Blender-free');
    }
    equal(integrity.sourceVideoSha256, pins.sourceVideoSha256, 'validated source-video SHA-256');
    equal(integrity.bundleSha256, pins.fittingBundleSha256, 'validated bundle SHA-256');
    equal(integrity.immutableManifestSha256, pins.immutableManifestSha256, 'validated immutable SHA-256');
}

/** Validate and recompute the complete immutable TROT chain before solver import. */
export function validateTrotContactRefitInputs({ inputManifestPath, expectedManifestSha256 } = {}) {
    const manifestFile = jsonSnapshot(inputManifestPath, 'TROT contact-refit input manifest');
    equal(manifestFile.sha256, sha256(expectedManifestSha256, 'expectedManifestSha256'), 'input manifest SHA-256');
    const manifest = manifestFile.json;
    if (manifest.schema !== INPUT_SCHEMA || manifest.browserOnly !== true
        || manifest.blenderUsed !== false || manifest.mixerUsed !== false
        || manifest.gait !== 'diagonal_pair_trot' || manifest.humanReviewRequired !== true) {
        throw new Error(`TROT input must use ${INPUT_SCHEMA} and browser-only/human-review flags`);
    }
    const inputs = object(manifest.inputs, 'TROT input.inputs');
    const files = {
        observations: pinnedJson(manifestFile.path, inputs.observations, 'TROT input observations'),
        bridgeReport: pinnedJson(manifestFile.path, inputs.bridgeReport, 'TROT input bridge report'),
        initialFitSummary: pinnedJson(manifestFile.path, inputs.initialFitSummary, 'TROT input initial fit'),
        diagnostic: pinnedJson(manifestFile.path, inputs.trotDiagnostic, 'TROT input diagnostic'),
    };
    const bundleDirectory = declaredPath(manifestFile.path, inputs.bundleDirectory, 'TROT input bundleDirectory');
    const pinsValue = object(manifest.pins, 'TROT input.pins');
    const pinNames = [
        'observationsSha256', 'bridgeReportSha256', 'initialFitSummarySha256', 'diagnosticSha256',
        'sourceVideoSha256', 'fittingBundleSha256', 'immutableManifestSha256',
        'sourceModelSha256', 'sourceSkeletonSha256',
    ];
    const pins = Object.fromEntries(pinNames.map((name) => [name, sha256(pinsValue[name], `pins.${name}`)]));
    equal(files.observations.sha256, pins.observationsSha256, 'observations file SHA-256');
    equal(files.bridgeReport.sha256, pins.bridgeReportSha256, 'bridge file SHA-256');
    equal(files.initialFitSummary.sha256, pins.initialFitSummarySha256, 'initial-fit file SHA-256');
    equal(files.diagnostic.sha256, pins.diagnosticSha256, 'diagnostic file SHA-256');

    const immutable = validateImmutableInputs({
        bundleDirectory,
        observationsPath: files.observations.path,
    });
    const integrity = validateBridgeAndRawPins({
        raw: files.observations.json,
        report: files.bridgeReport.json,
        observationPath: files.observations.path,
        bridgeReportPath: files.bridgeReport.path,
    });
    if (files.bridgeReport.json.status !== 'VALIDATED'
        || files.bridgeReport.json.browserOnly !== true
        || files.bridgeReport.json.blenderUsed !== false
        || files.bridgeReport.json.mixerUsed !== false
        || files.bridgeReport.json.fittingMode !== 'unconstrained_diagnostic'
        || Number(files.bridgeReport.json.sourceContacts) !== 0
        || Number(files.bridgeReport.json.preparedContacts) !== 0) {
        throw new Error('TROT bridge must be a browser-only unconstrained diagnostic with zero contacts');
    }
    const declaredBridgeBundle = declaredPath(
        files.bridgeReport.path,
        files.bridgeReport.json.inputs?.bundleDirectory,
        'bridge inputs.bundleDirectory',
    );
    const declaredBridgeObservations = declaredPath(
        files.bridgeReport.path,
        files.bridgeReport.json.inputs?.observationsPath,
        'bridge inputs.observationsPath',
    );
    if (!samePath(immutable.bundleDirectory, bundleDirectory)
        || !samePath(declaredBridgeBundle, bundleDirectory)
        || !samePath(declaredBridgeObservations, files.observations.path)) {
        throw new Error('TROT paths do not resolve to the exact immutable browser-fit inputs');
    }
    for (const [field, actual] of Object.entries({
        observationsSha256: immutable.integrity.observationsSha256,
        fittingBundleSha256: immutable.integrity.fittingBundleSha256,
        immutableManifestSha256: immutable.integrity.immutableManifestSha256,
        sourceVideoSha256: immutable.integrity.sourceVideoSha256,
        sourceModelSha256: immutable.integrity.sourceModelSha256,
        sourceSkeletonSha256: immutable.integrity.skeletonSha256,
    })) equal(actual, pins[field], `validated ${field}`);
    validateInitialFit(files.initialFitSummary.json, pins);
    validateDiagnosticPins(files.diagnostic.json, files, pins, integrity);
    const semanticObservations = prepareBridgeObservations(files.observations.json, files.bridgeReport.json);
    const recomputed = validatePinnedHorseTrotDiagnostic({
        observations: semanticObservations,
        diagnostic: files.diagnostic.json,
    });
    return {
        manifest,
        manifestFile,
        bundleDirectory,
        observationsPath: files.observations.path,
        files,
        semanticObservations,
        schedule: recomputed.schedule,
        pins: { inputManifestSha256: manifestFile.sha256, ...pins },
    };
}

function declaredGatePasses(gate) {
    if (gate.comparator === '<=') return Number(gate.actual) <= Number(gate.threshold);
    if (gate.comparator === '>=') return Number(gate.actual) >= Number(gate.threshold);
    if (gate.comparator === '===') return gate.actual === gate.threshold;
    return false;
}

function validatePassArtifacts(result, directory, validated) {
    const expected = ['bridge-report.json', 'fit-summary.json', 'fitted-animation.json', 'three-clip.json'];
    const actual = fs.readdirSync(directory).sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected.sort())) {
        throw new Error('TROT PASS staging directory contains an unexpected artifact set');
    }
    const summary = jsonSnapshot(path.join(directory, 'fit-summary.json'), 'final TROT fit summary').json;
    const bridge = jsonSnapshot(path.join(directory, 'bridge-report.json'), 'final TROT bridge report').json;
    const fitted = jsonSnapshot(path.join(directory, 'fitted-animation.json'), 'final TROT fitted animation').json;
    const clip = jsonSnapshot(path.join(directory, 'three-clip.json'), 'final TROT AnimationClip').json;
    if (result.passed !== true || summary.schema !== SUMMARY_SCHEMA
        || summary.status !== 'PASS_BROWSER_TROT_CONTACT_REFIT_GATES'
        || summary.gates?.passed !== true
        || summary.browserOnly !== true || summary.blenderUsed !== false || summary.mixerUsed !== false
        || summary.fittingMode !== 'trot_contact_constrained_refit'
        || summary.approvedForBrowserTrotContactFit !== true
        || summary.approvedForAnimationLibrary !== false
        || summary.humanReviewRequired !== true
        || result.fitSummary?.status !== summary.status
        || bridge.schema !== BRIDGE_SCHEMA || bridge.status !== 'VALIDATED'
        || bridge.browserOnly !== true || bridge.blenderUsed !== false || bridge.mixerUsed !== false
        || bridge.fittingMode !== 'trot_contact_constrained_refit') {
        throw new Error('final TROT PASS contract is invalid');
    }
    for (const [inputField, pinField] of [
        ['observationsSha256', 'observationsSha256'],
        ['fittingBundleSha256', 'fittingBundleSha256'],
        ['immutableManifestSha256', 'immutableManifestSha256'],
        ['sourceVideoSha256', 'sourceVideoSha256'],
        ['sourceModelSha256', 'sourceModelSha256'],
        ['skeletonSha256', 'sourceSkeletonSha256'],
    ]) {
        equal(summary.inputs?.[inputField], validated.pins[pinField], `final summary inputs.${inputField}`);
        equal(bridge.inputs?.[inputField], validated.pins[pinField], `final bridge inputs.${inputField}`);
    }
    if (!samePath(summary.inputs?.bundleDirectory, validated.bundleDirectory)
        || !samePath(bridge.inputs?.bundleDirectory, validated.bundleDirectory)
        || !samePath(summary.inputs?.observationsPath, validated.observationsPath)
        || !samePath(bridge.inputs?.observationsPath, validated.observationsPath)) {
        throw new Error('final TROT artifacts changed the immutable bundle/observation paths');
    }
    const gates = Array.isArray(summary.gates.results) ? summary.gates.results : [];
    const byName = new Map(gates.map((gate) => [gate?.name, gate]));
    if (byName.size !== gates.length || REQUIRED_FINAL_GATES.some((name) => !byName.has(name))
        || gates.some((gate) => gate?.passed !== true || !declaredGatePasses(gate))) {
        throw new Error('final TROT summary has missing, duplicate, failed, or forged gates');
    }
    const fittedQa = gateFittedTrot({ fitted, schedule: validated.schedule });
    if (fittedQa.status !== 'PASS') throw new Error('emitted fitted TROT projections fail recomputed QA');
    const motion = assessNonRootClipMotion(clip, summary.trotContactRefit?.nonRootClipMotion?.rootBoneName);
    if (motion.dynamicTrackCount < 4 || motion.dynamicBoneCount < 4
        || motion.dynamicTrackCount !== summary.trotContactRefit.nonRootClipMotion.dynamicTrackCount
        || motion.dynamicBoneCount !== summary.trotContactRefit.nonRootClipMotion.dynamicBoneCount) {
        throw new Error('emitted TROT AnimationClip is static, root-only, or disagrees with motion evidence');
    }
    for (const [field, expectedPin] of Object.entries(validated.pins)) {
        equal(summary.trotContactRefit?.provenance?.[field], expectedPin, `final TROT provenance.${field}`);
    }
    if (summary.trotContactRefit?.fittedTrotQa?.gaitQa?.accepted !== true
        || summary.trotContactRefit?.sourceGaitQa?.accepted !== true
        || summary.trotContactRefit?.humanReviewRequired !== true) {
        throw new Error('final TROT semantic/human-review evidence is invalid');
    }
    return { summary, bridge, fittedQa, motion };
}

export async function runBrowserTrotContactRefit(configuration, dependencies = {}) {
    const config = object(configuration, 'configuration');
    const validated = validateTrotContactRefitInputs(config);
    const outputDirectory = path.resolve(string(config.outputDirectory, 'configuration.outputDirectory'));
    const outputParent = path.dirname(outputDirectory);
    if (!fs.existsSync(outputParent) || !fs.statSync(outputParent).isDirectory()) {
        throw new Error(`TROT output parent does not exist: ${outputParent}`);
    }
    if (fs.existsSync(outputDirectory)) {
        const outputStats = fs.lstatSync(outputDirectory);
        if (!outputStats.isDirectory() || outputStats.isSymbolicLink()
            || fs.readdirSync(outputDirectory).length) {
            throw new Error(`TROT output must be absent or an empty regular directory: ${outputDirectory}`);
        }
    }
    const outputExisted = fs.existsSync(outputDirectory);
    const staging = `${outputDirectory}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    const cleanup = () => fs.rmSync(staging, { recursive: true, force: true });
    try {
        const runner = dependencies.runBrowserFitCanary || runBrowserFitCanary;
        const result = await runner({
            bundleDirectory: validated.bundleDirectory,
            observationsPath: validated.observationsPath,
            threeModule: string(config.threeModule, 'configuration.threeModule'),
            outputDirectory: staging,
            clipName: config.clipName || 'Horse_Trot_Browser_Contact_Refit',
            fit: { ...(config.fit || {}), loop: true },
            c1ClosureWindow: 4,
            float32LoopVelocityInvariantGates: true,
            gates: { ...(config.gates || {}), requireFourLimbContacts: true },
            emitFittedAnimation: true,
            emitThreeClip: true,
        }, {
            trotContactRefit: {
                diagnostic: validated.files.diagnostic.json,
                diagnosticObservations: validated.semanticObservations,
                pins: validated.pins,
            },
        });
        if (result.passed !== true) {
            cleanup();
            return { passed: false, fitSummary: result.fitSummary, outputs: {} };
        }
        const verified = validatePassArtifacts(result, staging, validated);
        if (outputExisted) fs.rmdirSync(outputDirectory);
        fs.renameSync(staging, outputDirectory);
        return {
            passed: true,
            fitSummary: verified.summary,
            outputs: {
                bridgeReportPath: path.join(outputDirectory, 'bridge-report.json'),
                fitSummaryPath: path.join(outputDirectory, 'fit-summary.json'),
                fittedAnimationPath: path.join(outputDirectory, 'fitted-animation.json'),
                threeClipPath: path.join(outputDirectory, 'three-clip.json'),
            },
            humanReviewRequired: true,
        };
    } catch (error) {
        cleanup();
        if (outputExisted && !fs.existsSync(outputDirectory)) fs.mkdirSync(outputDirectory);
        throw error;
    }
}

export function parseTrotRefitArgs(argv) {
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
  node browser_trot_contact_refit.mjs --input-manifest FILE \\
    --input-manifest-sha256 SHA256 --three-module FILE --output-dir EMPTY_DIR

Consumes only an immutable PASS ${HOOF_CONTACT_INFERENCE_CONTRACT.trotDiagnostic},
recomputes the code-owned diagonal-pair TROT profile, derives four hoof-contact
sets, runs the pure browser solver, then repeats projected TROT/contact-slide,
C0/C1, hierarchy and deformation gates. AnimationClip JSON is emitted only on
full machine PASS; fixed-camera human review remains mandatory.`;
}

export async function runCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseTrotRefitArgs(argv);
        if (config.help) {
            streams.stdout.write(`${helpText()}\n`);
            return 0;
        }
        const result = await runBrowserTrotContactRefit(config);
        streams.stdout.write(`${JSON.stringify({
            status: result.fitSummary?.status || 'FAIL',
            outputs: result.outputs,
            humanReviewRequired: result.humanReviewRequired === true,
        })}\n`);
        return result.passed ? 0 : 3;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invoked = process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url;
if (invoked) process.exitCode = await runCli();

export const HORSE_TROT_CONTACT_REFIT_INPUT_SCHEMA = INPUT_SCHEMA;

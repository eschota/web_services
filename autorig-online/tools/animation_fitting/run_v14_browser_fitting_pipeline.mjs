#!/usr/bin/env node
/**
 * Fail-closed, resumable command author for the Horse_2 V14 browser-fitting run.
 *
 * This program deliberately does not execute subprocesses.  It validates the
 * immutable input pins and every already-published stage, then writes a new
 * deterministic state document containing at most one exact next command.
 * Contact-refit input and final Three-clip hashes must be supplied by a later,
 * externally pinned spec revision; observed hashes are only pin requests and
 * are never promoted to trust by this process.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { validateContactRefitInputs } from './browser_contact_refit.mjs';
import { validateHorse2QaInputs } from './browser_horse_visual_phase_qa.mjs';

export const V14_PIPELINE_SPEC_SCHEMA = 'autorig.v14-browser-fitting-pipeline-spec.v1';
export const V14_PIPELINE_STATE_SCHEMA = 'autorig.v14-browser-fitting-pipeline-state.v1';

const SHA256_RE = /^[0-9a-f]{64}$/;
const TOOLS_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const PYTHON_WORKING_DIRECTORY = path.dirname(TOOLS_DIRECTORY);
const STAGE_PATHS = Object.freeze({
    objectGate: '01-object-region-gate',
    observations: '02-observations',
    initialFit: '03-initial-browser-fit',
    diagnostic: '04-hoof-contact-diagnostic.json',
    groundEvidence: '04-sam2-ground-evidence.json',
    contactManifest: '05-contact-refit-input.json',
    contactRefit: '06-browser-contact-refit',
    visualQa: '07-browser-visual-phase-qa',
});
const EXPECTED_ROOT_NAMES = new Set(Object.values(STAGE_PATHS));
export const V14_PIPELINE_TOOL_SOURCE_PATHS = Object.freeze({
    pipelineOrchestrator: path.join(TOOLS_DIRECTORY, 'run_v14_browser_fitting_pipeline.mjs'),
    objectRegionVideoGate: path.join(TOOLS_DIRECTORY, 'object_region_video_gate.py'),
    fittingErrors: path.join(TOOLS_DIRECTORY, 'errors.py'),
    fittingRig: path.join(TOOLS_DIRECTORY, 'rig.py'),
    contactProfile: path.join(TOOLS_DIRECTORY, 'contact_profile.py'),
    backendAnimationFittingInit: path.join(TOOLS_DIRECTORY, '__init__.py'),
    backendObservations: path.join(TOOLS_DIRECTORY, 'observations.py'),
    backendOptimizer: path.join(TOOLS_DIRECTORY, 'optimizer.py'),
    backendMath3d: path.join(TOOLS_DIRECTORY, 'math3d.py'),
    trackingInit: path.join(TOOLS_DIRECTORY, 'tracking_runtime', '__init__.py'),
    trackingMain: path.join(TOOLS_DIRECTORY, 'tracking_runtime', '__main__.py'),
    trackingCli: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'cli.py'),
    trackingCore: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'core.py'),
    trackingModels: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'models.py'),
    trackingOfficialBackends: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'official_backends.py'),
    trackingRuntimeLock: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'runtime_lock.py'),
    trackingContactIntegration: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'contact_integration.py'),
    trackingContactSolver: path.join(TOOLS_DIRECTORY, 'tracking_runtime', 'contact_solver.py'),
    browserFit: path.join(TOOLS_DIRECTORY, 'browser_fit_canary.mjs'),
    hoofDiagnostic: path.join(TOOLS_DIRECTORY, 'diagnose_browser_hoof_contacts.mjs'),
    contactManifestAuthor: path.join(TOOLS_DIRECTORY, 'author_browser_contact_refit_manifest.mjs'),
    browserContactRefit: path.join(TOOLS_DIRECTORY, 'browser_contact_refit.mjs'),
    browserVisualQa: path.join(TOOLS_DIRECTORY, 'browser_horse_visual_phase_qa.mjs'),
    browserCore: path.resolve(TOOLS_DIRECTORY, '../../static/js/animation-fitting-browser-core.js'),
    rgbObservationBridge: path.resolve(TOOLS_DIRECTORY, '../../static/js/animation-fitting-rgb-observation-bridge.js'),
    threeAdapter: path.resolve(TOOLS_DIRECTORY, '../../static/js/animation-fitting-three-adapter.js'),
    hoofContactInference: path.resolve(TOOLS_DIRECTORY, '../../static/js/animation-fitting-hoof-contact-inference.js'),
    semanticTracker: path.resolve(TOOLS_DIRECTORY, '../../static/js/animation-fitting-semantic-tracker.js'),
});
const FINAL_CONTACT_GATE_NAMES = Object.freeze([
    'head_reconstruction_world', 'rest_seed_alignment_px', 'final_mean_target_error_px',
    'maximum_target_error_px', 'bone_length_error_px', 'joint_limit_violation_rad',
    'contact_slide_px', 'loop_endpoint_error', 'hierarchy_segment_drift_world',
    'hierarchy_reprojection_error_px', 'requested_fitted_point_error_px',
    'unreachable_pixel_ray_ratio', 'target_sample_coverage', 'target_error_improved',
    'ordered_deform_heads', 'four_limb_contacts', 'three_clip_validate',
    'three_tracks_bound', 'pinned_contact_schedule', 'semantic_walk_gait',
    'fitted_walk_contact_slide',
]);
const FOOT_ORDER = Object.freeze(['fore_left', 'fore_right', 'hind_left', 'hind_right']);

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

function sha256(value, field) {
    const result = string(value, field);
    if (!SHA256_RE.test(result)) throw new Error(`${field} must be an externally supplied lowercase SHA-256`);
    return result;
}

function sha256Buffer(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function resolveFrom(base, value, field) {
    const raw = string(value, field);
    return path.resolve(base, raw);
}

function samePath(left, right) {
    return path.resolve(left).toLowerCase() === path.resolve(right).toLowerCase();
}

function isInside(parent, child) {
    const relative = path.relative(path.resolve(parent), path.resolve(child));
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function readSnapshot(filenameValue, field) {
    const filename = path.resolve(filenameValue);
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
        throw new Error(`${field} must be a non-empty file: ${filename}`);
    }
    if (before.size !== buffer.length || after.size !== buffer.length
        || before.dev !== after.dev || before.ino !== after.ino
        || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while its immutable bytes were read`);
    }
    return { path: filename, bytes: buffer.length, sha256: sha256Buffer(buffer), buffer };
}

function parseJsonSnapshot(snapshot, field) {
    try {
        return object(JSON.parse(snapshot.buffer.toString('utf8')), field);
    } catch (error) {
        if (error.message.startsWith(`${field} must be`)) throw error;
        throw new Error(`${field} is invalid JSON: ${error.message}`);
    }
}

function pinnedSnapshot({ base, descriptor, field, requireBytes = true }) {
    const row = object(descriptor, field);
    const expectedSha256 = sha256(row.sha256, `${field}.sha256`);
    const expectedBytes = requireBytes ? integer(row.bytes, `${field}.bytes`, 1) : null;
    const snapshot = readSnapshot(resolveFrom(base, row.path, `${field}.path`), field);
    if (snapshot.sha256 !== expectedSha256) throw new Error(`${field} SHA-256 mismatch`);
    if (expectedBytes != null && snapshot.bytes !== expectedBytes) throw new Error(`${field} byte count mismatch`);
    return snapshot;
}

function evidence(snapshot) {
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256 };
}

function safeBundleFilename(value, field) {
    const result = string(value, field);
    if (path.isAbsolute(result) || path.basename(result) !== result || result === '.' || result === '..') {
        throw new Error(`${field} must be a bundle-root filename`);
    }
    return result;
}

function validateGuide(specBase, guideValue) {
    const guide = object(guideValue, 'spec.guide');
    const bundleDirectory = resolveFrom(specBase, guide.bundleDirectory, 'spec.guide.bundleDirectory');
    if (!fs.existsSync(bundleDirectory) || !fs.statSync(bundleDirectory).isDirectory()) {
        throw new Error(`spec.guide.bundleDirectory must be a directory: ${bundleDirectory}`);
    }
    const manifestSnapshot = readSnapshot(path.join(bundleDirectory, 'immutable_manifest.json'), 'V14 guide manifest');
    if (manifestSnapshot.sha256 !== sha256(guide.immutableManifestSha256, 'spec.guide.immutableManifestSha256')) {
        throw new Error('V14 guide manifest SHA-256 mismatch');
    }
    const manifest = parseJsonSnapshot(manifestSnapshot, 'V14 guide manifest');
    if (manifest.schema !== 'autorig-browser-ltx-interval-guide-bundle.v1'
        || manifest.status !== 'PASS' || manifest.browserOnly !== true || manifest.blenderUsed !== false
        || manifest.rigType !== 'HORSE_2' || manifest.cycle_frame_count_int !== 49
        || manifest.browser_frame_count_int !== 49 || manifest.guide_count_int !== 1) {
        throw new Error('V14 guide manifest is not the authorized PASS browser-only 49-frame Horse_2 contract');
    }
    const rows = Array.isArray(manifest.frames_array) ? manifest.frames_array : [];
    if (rows.length !== 49) throw new Error('V14 guide manifest must pin exactly 49 frames');
    const frameEvidence = rows.map((rowValue, index) => {
        const row = object(rowValue, `V14 guide frame ${index}`);
        if (row.frame_index_int !== index) throw new Error(`V14 guide frame ${index} lost chronology`);
        const filename = safeBundleFilename(row.filename_string, `V14 guide frame ${index}.filename`);
        const snapshot = readSnapshot(path.join(bundleDirectory, filename), `V14 guide frame ${index}`);
        if (snapshot.sha256 !== sha256(row.sha256_string, `V14 guide frame ${index}.sha256`)
            || snapshot.bytes !== integer(row.bytes_int, `V14 guide frame ${index}.bytes`, 1)) {
            throw new Error(`V14 guide frame ${index} does not match its manifest pin`);
        }
        return evidence(snapshot);
    });
    const endpoint = pinnedSnapshot({ base: specBase, descriptor: guide.endpointGuide, field: 'spec.guide.endpointGuide' });
    if (!samePath(endpoint.path, frameEvidence[0].path)
        || manifest.endpoint_guide_sha256_string !== endpoint.sha256) {
        throw new Error('V14 endpoint guide does not resolve to the externally pinned guide frame 0');
    }
    const intervalRow = object(manifest.interval_guide_video_object, 'V14 interval video');
    const interval = readSnapshot(
        path.join(bundleDirectory, safeBundleFilename(intervalRow.filename, 'V14 interval video.filename')),
        'V14 interval video',
    );
    if (interval.sha256 !== sha256(intervalRow.sha256, 'V14 interval video.sha256')
        || interval.bytes !== integer(intervalRow.bytes, 'V14 interval video.bytes', 1)
        || intervalRow.frameCount !== 49 || intervalRow.audioStreamCount !== 0) {
        throw new Error('V14 interval video does not satisfy its immutable lossless pin');
    }
    const poseRow = object(manifest.poseContract, 'V14 pose contract');
    const pose = readSnapshot(
        path.join(bundleDirectory, safeBundleFilename(poseRow.filename, 'V14 pose contract.filename')),
        'V14 pose contract',
    );
    if (pose.sha256 !== sha256(poseRow.sha256, 'V14 pose contract.sha256')
        || pose.bytes !== integer(poseRow.bytes, 'V14 pose contract.bytes', 1)) {
        throw new Error('V14 pose contract does not match its manifest pin');
    }
    return { bundleDirectory, manifest, manifestSnapshot, endpoint, interval, pose, frameEvidence };
}

function validateCanonicalBundle(specBase, canonicalValue) {
    const canonical = object(canonicalValue, 'spec.canonicalBundle');
    const bundleDirectory = resolveFrom(specBase, canonical.directory, 'spec.canonicalBundle.directory');
    if (!fs.existsSync(bundleDirectory) || !fs.statSync(bundleDirectory).isDirectory()) {
        throw new Error(`spec.canonicalBundle.directory must be a directory: ${bundleDirectory}`);
    }
    const immutableSnapshot = readSnapshot(path.join(bundleDirectory, 'immutable_manifest.json'), 'canonical immutable manifest');
    if (immutableSnapshot.sha256 !== sha256(canonical.immutableManifestSha256, 'spec.canonicalBundle.immutableManifestSha256')) {
        throw new Error('canonical immutable manifest SHA-256 mismatch');
    }
    const immutable = parseJsonSnapshot(immutableSnapshot, 'canonical immutable manifest');
    if (immutable.schema !== 'autorig-fitting-immutable-copy.v1') throw new Error('canonical immutable manifest schema changed');
    const rows = Array.isArray(immutable.files) ? immutable.files : [];
    if (!rows.length || immutable.bundle_file_count !== rows.length) throw new Error('canonical immutable file inventory is invalid');
    const inventory = new Map();
    let totalBytes = 0;
    rows.forEach((rowValue, index) => {
        const row = object(rowValue, `canonical immutable file ${index}`);
        const filename = safeBundleFilename(row.filename, `canonical immutable file ${index}.filename`);
        if (inventory.has(filename)) throw new Error(`canonical immutable manifest repeats ${filename}`);
        const snapshot = readSnapshot(path.join(bundleDirectory, filename), `canonical immutable file ${filename}`);
        if (snapshot.sha256 !== sha256(row.sha256, `canonical immutable file ${filename}.sha256`)
            || snapshot.bytes !== integer(row.bytes, `canonical immutable file ${filename}.bytes`, 1)) {
            throw new Error(`canonical immutable file ${filename} does not match its pin`);
        }
        inventory.set(filename, snapshot);
        totalBytes += snapshot.bytes;
    });
    if (immutable.bundle_total_bytes !== totalBytes) throw new Error('canonical immutable total byte count changed');
    const actual = fs.readdirSync(bundleDirectory, { withFileTypes: true })
        .filter((entry) => entry.isFile() && entry.name !== 'immutable_manifest.json')
        .map((entry) => entry.name).sort();
    if (JSON.stringify(actual) !== JSON.stringify([...inventory.keys()].sort())) {
        throw new Error('canonical bundle contains unpinned or missing root files');
    }
    const bundleManifest = object(immutable.bundle_manifest, 'canonical immutable bundle_manifest');
    const fittingName = safeBundleFilename(bundleManifest.filename, 'canonical immutable bundle_manifest.filename');
    const fittingSnapshot = inventory.get(fittingName);
    const expectedFittingSha = sha256(canonical.fittingBundleSha256, 'spec.canonicalBundle.fittingBundleSha256');
    if (!fittingSnapshot || fittingSnapshot.sha256 !== expectedFittingSha
        || bundleManifest.sha256 !== expectedFittingSha) {
        throw new Error('canonical fitting bundle does not match its external pin');
    }
    const fitting = parseJsonSnapshot(fittingSnapshot, 'canonical fitting bundle');
    const expectedSourceSha = sha256(canonical.sourceModelSha256, 'spec.canonicalBundle.sourceModelSha256');
    if (immutable.source_model?.sha256 !== expectedSourceSha || fitting.source?.sha256 !== expectedSourceSha
        || fitting.source?.rig_type !== 'HORSE_2' || fitting.source?.species !== 'horse'
        || fitting.actionless?.actionless !== true) {
        throw new Error('canonical source/model/actionless Horse_2 provenance does not match its external pin');
    }
    return {
        bundleDirectory, immutableSnapshot, fittingSnapshot, sourceModelSha256: expectedSourceSha,
        immutable, fitting, inventory,
    };
}

function validateToolSources(specBase, sourcesValue) {
    const sources = object(sourcesValue, 'spec.toolSources');
    const expectedNames = Object.keys(V14_PIPELINE_TOOL_SOURCE_PATHS).sort();
    if (JSON.stringify(Object.keys(sources).sort()) !== JSON.stringify(expectedNames)) {
        throw new Error(`spec.toolSources must contain the exact source closure: ${expectedNames.join(', ')}`);
    }
    return Object.fromEntries(expectedNames.map((name) => {
        const snapshot = pinnedSnapshot({
            base: specBase,
            descriptor: sources[name],
            field: `spec.toolSources.${name}`,
        });
        if (!samePath(snapshot.path, V14_PIPELINE_TOOL_SOURCE_PATHS[name])) {
            throw new Error(`spec.toolSources.${name}.path does not resolve to the commanded source file`);
        }
        return [name, snapshot];
    }));
}

function validateRuntime(specBase, runtimeValue) {
    const runtime = object(runtimeValue, 'spec.runtime');
    const executable = (name) => pinnedSnapshot({
        base: specBase,
        descriptor: object(runtime.executables, 'spec.runtime.executables')[name],
        field: `spec.runtime.executables.${name}`,
    });
    const executables = {
        python: executable('python'), node: executable('node'), chrome: executable('chrome'),
        ffmpeg: executable('ffmpeg'), ffprobe: executable('ffprobe'),
    };
    const threeModule = pinnedSnapshot({ base: specBase, descriptor: runtime.threeModule, field: 'spec.runtime.threeModule' });
    if (String(object(runtime.threeModule, 'spec.runtime.threeModule').revision) !== '160') {
        throw new Error('spec.runtime.threeModule.revision must be exactly 160');
    }
    const trackingRuntimeRoot = resolveFrom(specBase, runtime.trackingRuntimeRoot, 'spec.runtime.trackingRuntimeRoot');
    if (!fs.existsSync(trackingRuntimeRoot) || !fs.statSync(trackingRuntimeRoot).isDirectory()) {
        throw new Error(`tracking runtime root must be a directory: ${trackingRuntimeRoot}`);
    }
    const trackingRuntimeLock = pinnedSnapshot({
        base: specBase,
        descriptor: runtime.trackingRuntimeLock,
        field: 'spec.runtime.trackingRuntimeLock',
    });
    return { executables, threeModule, trackingRuntimeRoot, trackingRuntimeLock };
}

function quotePowerShell(value) {
    return `'${String(value).replaceAll("'", "''")}'`;
}

function command(cwd, argv, pinnedInputs = []) {
    const resolvedCwd = path.resolve(cwd);
    const preconditions = pinnedInputs.map((input) => ({
        path: path.resolve(input.path),
        bytes: integer(input.bytes, `command precondition ${input.path}.bytes`, 1),
        sha256: sha256(input.sha256, `command precondition ${input.path}.sha256`),
    }));
    const checks = preconditions.flatMap((input, index) => {
        const item = `$pin${index}`;
        return [
            `${item}=Get-Item -LiteralPath ${quotePowerShell(input.path)} -ErrorAction Stop`,
            `if(${item}.Length -ne ${input.bytes}){throw ${quotePowerShell(`Pinned byte count changed: ${input.path}`)}}`,
            `if((Get-FileHash -LiteralPath ${quotePowerShell(input.path)} -Algorithm SHA256).Hash.ToLowerInvariant() -ne ${quotePowerShell(input.sha256)}){throw ${quotePowerShell(`Pinned SHA-256 changed: ${input.path}`)}}`,
        ];
    });
    return {
        cwd: resolvedCwd,
        argv,
        preconditions,
        powershell: [
            "$ErrorActionPreference='Stop'",
            ...checks,
            `Set-Location ${quotePowerShell(resolvedCwd)}`,
            `& ${argv.map(quotePowerShell).join(' ')}`,
            'if($LASTEXITCODE -ne 0){exit $LASTEXITCODE}',
        ].join('\n'),
    };
}

function withAdditionalPreconditions(baseCommand, pinnedInputs) {
    const unique = new Map();
    [...baseCommand.preconditions, ...pinnedInputs].forEach((pin) => unique.set(path.resolve(pin.path).toLowerCase(), pin));
    return command(baseCommand.cwd, baseCommand.argv, [...unique.values()]);
}

function expectedPaths(outputRoot) {
    return Object.fromEntries(Object.entries(STAGE_PATHS).map(([key, name]) => [key, path.join(outputRoot, name)]));
}

function ensureOutputRootShape(outputRoot) {
    if (!fs.existsSync(outputRoot)) return;
    if (!fs.statSync(outputRoot).isDirectory()) throw new Error(`spec.outputRoot must be a directory when present: ${outputRoot}`);
    const unexpected = fs.readdirSync(outputRoot).filter((name) => !EXPECTED_ROOT_NAMES.has(name));
    if (unexpected.length) throw new Error(`outputRoot contains unexpected entries: ${unexpected.sort().join(', ')}`);
}

function assertNoOutOfOrderStages(paths) {
    const groups = [
        ['object_region_gate', [paths.objectGate]],
        ['tapnext_sam2_observations', [paths.observations]],
        ['initial_browser_fit', [paths.initialFit]],
        ['hoof_contact_diagnostic', [paths.diagnostic, paths.groundEvidence]],
        ['contact_refit_manifest', [paths.contactManifest]],
        ['browser_contact_refit', [paths.contactRefit]],
        ['browser_visual_phase_qa', [paths.visualQa]],
    ];
    let missingEarlier = null;
    for (const [name, filenames] of groups) {
        const count = filenames.filter((filename) => fs.existsSync(filename)).length;
        if (count > 0 && missingEarlier) {
            throw new Error(`out-of-order stage ${name} exists before required stage ${missingEarlier}`);
        }
        if (count === 0 && missingEarlier == null) missingEarlier = name;
    }
}

function rejectExistingLaterStages(paths, keys, reason) {
    const present = keys.filter((key) => fs.existsSync(paths[key]));
    if (present.length) throw new Error(`${reason}; stale later-stage paths exist: ${present.join(', ')}`);
}

function exactDirectoryFiles(directory, expectedNames, field) {
    if (!fs.existsSync(directory)) return null;
    if (!fs.statSync(directory).isDirectory()) throw new Error(`${field} must be a directory`);
    const actual = fs.readdirSync(directory, { withFileTypes: true }).map((entry) => entry.name).sort();
    const expected = [...expectedNames].sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`${field} artifact inventory is partial or unexpected: ${actual.join(', ')}`);
    }
    return actual;
}

export function evaluateObjectGateReport(reportValue, expected) {
    const report = object(reportValue, 'object-region gate report');
    if (report.schema !== 'autorig.animation-fitting.object-region-video-gate.v1') {
        throw new Error('object-region gate schema changed');
    }
    if (report.inputs?.candidate?.sha256 !== expected.candidate.sha256
        || report.inputs?.candidate?.bytes !== expected.candidate.bytes
        || report.inputs?.endpoint_guide?.sha256 !== expected.endpoint.sha256
        || report.inputs?.endpoint_guide?.bytes !== expected.endpoint.bytes
        || report.inputs?.guide_bundle?.manifest_sha256 !== expected.guideManifestSha256) {
        throw new Error('object-region gate report does not bind the externally pinned candidate/guide inputs');
    }
    const passed = report.verdict === 'PASS' && report.approved_for_fitting === true;
    if (!passed && !(report.verdict === 'FAIL' && report.approved_for_fitting === false)) {
        throw new Error('object-region gate verdict/approval contract is inconsistent');
    }
    return { passed, verdict: report.verdict };
}

function inspectObjectGate(paths, validated) {
    if (!fs.existsSync(paths.objectGate)) return null;
    exactDirectoryFiles(paths.objectGate, ['object_region_video_gate.json', 'object_region_video_gate.png'], 'object-region gate output');
    const reportSnapshot = readSnapshot(path.join(paths.objectGate, 'object_region_video_gate.json'), 'object-region gate report');
    const pngSnapshot = readSnapshot(path.join(paths.objectGate, 'object_region_video_gate.png'), 'object-region gate evidence');
    const decision = evaluateObjectGateReport(parseJsonSnapshot(reportSnapshot, 'object-region gate report'), {
        candidate: validated.candidate,
        endpoint: validated.guide.endpoint,
        guideManifestSha256: validated.guide.manifestSnapshot.sha256,
    });
    return { decision, artifacts: [evidence(reportSnapshot), evidence(pngSnapshot)] };
}

function validateManifestInventory(directory, manifestSnapshot, manifest, field) {
    if (!Array.isArray(manifest.files) || !manifest.files.length) throw new Error(`${field}.files must not be empty`);
    const listed = new Set();
    const artifacts = [];
    manifest.files.forEach((rowValue, index) => {
        const row = object(rowValue, `${field}.files[${index}]`);
        const relative = string(row.path, `${field}.files[${index}].path`).replaceAll('\\', '/');
        if (relative.startsWith('/') || relative.split('/').includes('..') || listed.has(relative)) {
            throw new Error(`${field} contains an unsafe or duplicate path ${relative}`);
        }
        listed.add(relative);
        const snapshot = readSnapshot(path.join(directory, ...relative.split('/')), `${field} artifact ${relative}`);
        if (snapshot.sha256 !== sha256(row.sha256, `${field}.files[${index}].sha256`)
            || snapshot.bytes !== integer(row.bytes, `${field}.files[${index}].bytes`, 1)) {
            throw new Error(`${field} artifact ${relative} does not match its pin`);
        }
        artifacts.push(evidence(snapshot));
    });
    const actual = [];
    for (const entry of fs.readdirSync(directory, { recursive: true, withFileTypes: true })) {
        if (!entry.isFile()) continue;
        const absolute = path.join(entry.parentPath || entry.path, entry.name);
        const relative = path.relative(directory, absolute).replaceAll('\\', '/');
        if (relative !== path.basename(manifestSnapshot.path)) actual.push(relative);
    }
    if (JSON.stringify(actual.sort()) !== JSON.stringify([...listed].sort())) {
        throw new Error(`${field} does not pin its exact on-disk artifact inventory`);
    }
    return artifacts;
}

function inspectObservations(paths, validated) {
    if (!fs.existsSync(paths.observations)) return null;
    if (!fs.statSync(paths.observations).isDirectory()) throw new Error('observations output must be a directory');
    const manifestSnapshot = readSnapshot(path.join(paths.observations, 'observation_bundle_manifest.json'), 'observation bundle manifest');
    const manifest = parseJsonSnapshot(manifestSnapshot, 'observation bundle manifest');
    if (manifest.schema !== 'autorig-tracking-observation-bundle.v1') throw new Error('observation bundle manifest schema changed');
    const artifacts = validateManifestInventory(paths.observations, manifestSnapshot, manifest, 'observation bundle manifest');
    const observationSnapshot = readSnapshot(path.join(paths.observations, 'observations.json'), 'TAPNext++/SAM2 observations');
    const observations = parseJsonSnapshot(observationSnapshot, 'TAPNext++/SAM2 observations');
    const provenance = object(observations.provenance, 'observations.provenance');
    if (observations.schema !== 'autorig-fitting-observations.v1' || observations.frame_count !== 49
        || provenance.source_video_sha256 !== validated.candidate.sha256
        || provenance.immutable_manifest_sha256 !== validated.canonical.immutableSnapshot.sha256
        || provenance.bundle_sha256 !== validated.canonical.fittingSnapshot.sha256) {
        throw new Error('observation bundle does not bind the exact 49-frame candidate and canonical bundle');
    }
    const silhouettes = Array.isArray(observations.silhouettes) ? observations.silhouettes : [];
    if (silhouettes.length !== 49 || silhouettes.some((row, index) => row.frame !== index)) {
        throw new Error('observation bundle must contain 49 chronological SAM2 masks');
    }
    const guidePin = provenance.first_frame_reference?.selected?.manifest?.sha256;
    if (guidePin !== validated.guide.manifestSnapshot.sha256) {
        throw new Error('observation bundle did not use the pinned V14 browser interval guide');
    }
    return { artifacts: [evidence(manifestSnapshot), ...artifacts], observations: observationSnapshot };
}

function inspectInitialFit(paths, validated, observations) {
    if (!fs.existsSync(paths.initialFit)) return null;
    exactDirectoryFiles(paths.initialFit, ['bridge-report.json', 'fit-summary.json'], 'initial browser-fit output');
    const bridgeSnapshot = readSnapshot(path.join(paths.initialFit, 'bridge-report.json'), 'initial bridge report');
    const summarySnapshot = readSnapshot(path.join(paths.initialFit, 'fit-summary.json'), 'initial fit summary');
    const bridge = parseJsonSnapshot(bridgeSnapshot, 'initial bridge report');
    const summary = parseJsonSnapshot(summarySnapshot, 'initial fit summary');
    if (bridge.schema !== 'autorig-browser-fit-canary-bridge-report.v1' || bridge.status !== 'VALIDATED'
        || summary.schema !== 'autorig-browser-fit-canary-summary.v1' || summary.status !== 'PASS_BROWSER_FIT_GATES'
        || bridge.browserOnly !== true || bridge.blenderUsed !== false || bridge.mixerUsed !== false
        || summary.browserOnly !== true || summary.blenderUsed !== false || summary.mixerUsed !== false
        || summary.approvedForAnimationLibrary !== false || summary.gates?.passed !== true) {
        throw new Error('initial browser-fit artifacts are not a PASS browser-only/non-mixer contract');
    }
    const inputs = object(summary.inputs, 'initial fit summary.inputs');
    if (inputs.sourceVideoSha256 !== validated.candidate.sha256
        || inputs.immutableManifestSha256 !== validated.canonical.immutableSnapshot.sha256
        || inputs.fittingBundleSha256 !== validated.canonical.fittingSnapshot.sha256
        || inputs.sourceModelSha256 !== validated.canonical.sourceModelSha256
        || inputs.observationsSha256 !== observations.sha256) {
        throw new Error('initial browser-fit summary does not preserve the full external input chain');
    }
    return { artifacts: [evidence(bridgeSnapshot), evidence(summarySnapshot)], bridge: bridgeSnapshot, summary: summarySnapshot };
}

export function evaluateContactDiagnosticReport(reportValue, expected) {
    const report = object(reportValue, 'hoof-contact diagnostic');
    if (report.schema !== 'autorig-browser-hoof-contact-diagnostic.v1') throw new Error('hoof-contact diagnostic schema changed');
    if (report.inputs?.observations?.sha256 !== expected.observationsSha256
        || report.inputs?.bridgeReport?.sha256 !== expected.bridgeReportSha256
        || report.inputs?.sourceVideo?.sha256 !== expected.sourceVideoSha256) {
        throw new Error('hoof-contact diagnostic does not bind observations, bridge, and source video');
    }
    if (report.status !== 'PASS' && report.status !== 'FAIL') throw new Error('hoof-contact diagnostic status is invalid');
    return { passed: report.status === 'PASS', status: report.status, failures: report.schedule?.qa?.failures || [] };
}

function inspectDiagnostic(paths, validated, observations, initialFit) {
    const hasDiagnostic = fs.existsSync(paths.diagnostic);
    const hasGround = fs.existsSync(paths.groundEvidence);
    if (!hasDiagnostic && !hasGround) return null;
    if (!hasDiagnostic || !hasGround) throw new Error('hoof diagnostic/ground evidence is a partial output pair');
    const diagnosticSnapshot = readSnapshot(paths.diagnostic, 'hoof-contact diagnostic');
    const groundSnapshot = readSnapshot(paths.groundEvidence, 'SAM2 ground evidence');
    const decision = evaluateContactDiagnosticReport(parseJsonSnapshot(diagnosticSnapshot, 'hoof-contact diagnostic'), {
        observationsSha256: observations.sha256,
        bridgeReportSha256: initialFit.bridge.sha256,
        sourceVideoSha256: validated.candidate.sha256,
    });
    parseJsonSnapshot(groundSnapshot, 'SAM2 ground evidence');
    return { decision, artifacts: [evidence(diagnosticSnapshot), evidence(groundSnapshot)], diagnostic: diagnosticSnapshot };
}

function validateSerializedTrack(trackValue, field, frameCount, durationSeconds) {
    const track = object(trackValue, field);
    const name = string(track.name, `${field}.name`);
    if (!['quaternion', 'vector'].includes(track.type)) throw new Error(`${field}.type is invalid`);
    if (!Array.isArray(track.times) || track.times.length !== frameCount
        || track.times.some((value) => !Number.isFinite(Number(value)))
        || Number(track.times[0]) !== 0 || Math.abs(Number(track.times.at(-1)) - durationSeconds) > 1e-9
        || track.times.some((value, index) => index > 0 && Number(value) <= Number(track.times[index - 1]))) {
        throw new Error(`${field}.times do not preserve the exact fitted chronology`);
    }
    const itemSize = track.type === 'quaternion' ? 4 : 3;
    if (!Array.isArray(track.values) || track.values.length !== frameCount * itemSize
        || track.values.some((value) => !Number.isFinite(Number(value)))) {
        throw new Error(`${field}.values do not match the fitted chronology`);
    }
    return { name, type: track.type };
}

function validateFittedAnimationArtifact(fittedValue, summary) {
    const fitted = object(fittedValue, 'fitted animation');
    if (fitted.schema !== 'autorig-browser-fitted-animation.v1' || fitted.loop !== true) {
        throw new Error('fitted animation is not the looped browser contract');
    }
    const frameCount = integer(fitted.frameCount, 'fitted animation.frameCount', 8);
    const fps = finite(fitted.fps, 'fitted animation.fps');
    const durationSeconds = finite(fitted.durationSeconds, 'fitted animation.durationSeconds');
    if (fps <= 0 || Math.abs(durationSeconds - (frameCount - 1) / fps) > 1e-9
        || summary.fit?.frameCount !== frameCount || summary.fit?.durationSeconds !== durationSeconds) {
        throw new Error('fitted animation timing does not match fit-summary.json');
    }
    const tracks = [
        ...(Array.isArray(fitted.tracks) ? fitted.tracks : []),
        ...(Array.isArray(fitted.positionTracks) ? fitted.positionTracks : []),
        ...(fitted.rootTrack ? [fitted.rootTrack] : []),
    ];
    if (!Array.isArray(fitted.tracks) || !fitted.tracks.length || !tracks.length) throw new Error('fitted animation has no browser tracks');
    const names = new Set();
    tracks.forEach((track, index) => {
        const normalized = validateSerializedTrack(track, `fitted animation track ${index}`, frameCount, durationSeconds);
        if (names.has(normalized.name)) throw new Error(`fitted animation repeats ${normalized.name}`);
        names.add(normalized.name);
    });
    if (summary.fit?.quaternionTracks !== fitted.tracks.length
        || summary.fit?.positionTracks !== (fitted.positionTracks || []).length
        || JSON.stringify(fitted.qa) !== JSON.stringify(summary.fit?.qa)) {
        throw new Error('fitted animation track/QA inventory does not match fit-summary.json');
    }
    for (const field of [
        'targetSamples', 'initialMeanTargetErrorPx', 'finalMeanTargetErrorPx', 'maximumTargetErrorPx',
        'maximumBoneLengthErrorPx', 'maximumJointLimitViolationRad', 'maximumContactSlidePx', 'loopEndpointError',
    ]) finite(fitted.qa?.[field], `fitted animation.qa.${field}`);
    if (!Array.isArray(fitted.frames) || fitted.frames.length !== frameCount) throw new Error('fitted animation debug frames do not match frameCount');
    fitted.frames.forEach((frameValue, frameIndex) => {
        const frame = object(frameValue, `fitted animation frame ${frameIndex}`);
        if (frame.frame !== frameIndex) throw new Error(`fitted animation frame ${frameIndex} lost chronology`);
        const limbs = object(frame.limbs, `fitted animation frame ${frameIndex}.limbs`);
        FOOT_ORDER.forEach((foot) => {
            const points = object(limbs[foot], `fitted animation ${foot}`).points;
            if (!Array.isArray(points) || points.length < 3
                || points.some((point) => !Array.isArray(point) || point.length < 2
                    || point.some((value) => !Number.isFinite(Number(value))))) {
                throw new Error(`fitted animation frame ${frameIndex} is missing valid ${foot} points`);
            }
        });
    });
    return { frameCount, durationSeconds };
}

function validateThreeClipArtifact(clipValue, summary, fitted) {
    const clip = object(clipValue, 'Three clip');
    if (string(clip.name, 'Three clip.name') !== summary.hierarchyClip?.name
        || finite(clip.duration, 'Three clip.duration') !== fitted.durationSeconds
        || typeof clip.uuid !== 'string' || !clip.uuid || !Number.isFinite(Number(clip.blendMode))
        || !Array.isArray(clip.tracks) || !clip.tracks.length || clip.tracks.length !== summary.hierarchyClip?.tracks) {
        throw new Error('Three clip header/inventory does not match fit-summary.json');
    }
    const names = new Set();
    const typesByBone = new Map();
    clip.tracks.forEach((track, index) => {
        const normalized = validateSerializedTrack(track, `Three clip track ${index}`, fitted.frameCount, fitted.durationSeconds);
        if (names.has(normalized.name)) throw new Error(`Three clip repeats ${normalized.name}`);
        names.add(normalized.name);
        const suffix = normalized.type === 'quaternion' ? '.quaternion' : '.position';
        if (!normalized.name.endsWith(suffix)) throw new Error(`Three clip track ${normalized.name} has an invalid binding`);
        const bone = normalized.name.slice(0, -suffix.length);
        const types = typesByBone.get(bone) || new Set();
        types.add(normalized.type);
        typesByBone.set(bone, types);
    });
    if ([...typesByBone.values()].some((types) => types.size !== 2
        || !types.has('quaternion') || !types.has('vector'))) {
        throw new Error('Three clip must contain quaternion and position tracks for each bound bone');
    }
}

function declaredGatePasses(gate) {
    if (gate.comparator === '<=') return Number.isFinite(Number(gate.actual)) && Number(gate.actual) <= Number(gate.threshold);
    if (gate.comparator === '>=') return Number.isFinite(Number(gate.actual)) && Number(gate.actual) >= Number(gate.threshold);
    if (gate.comparator === '===') return gate.actual === gate.threshold;
    return false;
}

export function validateFinalContactRefitOutputs({ snapshots, validatedInput }) {
    const summary = parseJsonSnapshot(snapshots['fit-summary.json'], 'contact-refit fit summary');
    const bridge = parseJsonSnapshot(snapshots['bridge-report.json'], 'contact-refit bridge report');
    const fittedAnimation = parseJsonSnapshot(snapshots['fitted-animation.json'], 'contact-refit fitted animation');
    const threeClip = parseJsonSnapshot(snapshots['three-clip.json'], 'contact-refit Three clip');
    const validated = object(validatedInput, 'validated contact-refit input');
    const pins = object(validated.pins, 'validated contact-refit input.pins');
    if (summary.schema !== 'autorig-browser-fit-canary-summary.v1'
        || summary.status !== 'PASS_BROWSER_CONTACT_REFIT_GATES' || summary.gates?.passed !== true
        || summary.approvedForBrowserContactFit !== true || summary.approvedForAnimationLibrary !== false
        || summary.browserOnly !== true || summary.blenderUsed !== false || summary.mixerUsed !== false
        || summary.fittingMode !== 'contact_constrained_refit') {
        throw new Error('browser contact-refit summary is not the final PASS browser-only contract');
    }
    const gateRows = Array.isArray(summary.gates.results) ? summary.gates.results : [];
    const gates = new Map();
    gateRows.forEach((gate, index) => {
        if (!gate || typeof gate.name !== 'string' || gates.has(gate.name)) throw new Error(`contact-refit gate ${index} is invalid`);
        gates.set(gate.name, gate);
    });
    if (gates.size !== FINAL_CONTACT_GATE_NAMES.length || FINAL_CONTACT_GATE_NAMES.some((name) => !gates.has(name))
        || [...gates.values()].some((gate) => gate.passed !== true || !declaredGatePasses(gate))
        || gates.get('four_limb_contacts').actual !== 4 || gates.get('four_limb_contacts').enforced !== true
        || gates.get('pinned_contact_schedule').actual !== 'PASS' || gates.get('semantic_walk_gait').actual !== true) {
        throw new Error('contact-refit summary contains a failed, missing, or forged final gate');
    }
    const refit = object(summary.contactRefit, 'contact-refit summary.contactRefit');
    if (refit.scheduleStatus !== 'PASS' || refit.semanticGaitQa?.accepted !== true
        || refit.semanticGaitQa?.simultaneousSwingFrameCount !== 0 || refit.fittedWalkQa?.status !== 'PASS'
        || !Array.isArray(refit.fittedWalkQa?.failures) || refit.fittedWalkQa.failures.length
        || Number(refit.fittedWalkQa.maximumContactSlideRatio) > Number(refit.fittedWalkQa.thresholdRatio)
        || summary.observations?.contactCount !== 4
        || JSON.stringify([...(summary.approvalExclusions || [])].sort())
            !== JSON.stringify(['fixed_camera_visual_phase_qa', 'target_mesh_deformation_qa'])) {
        throw new Error('contact-refit summary does not preserve the PASS four-hoof gait contract');
    }
    if (bridge.schema !== 'autorig-browser-fit-canary-bridge-report.v1' || bridge.status !== 'VALIDATED'
        || bridge.browserOnly !== true || bridge.blenderUsed !== false || bridge.mixerUsed !== false
        || bridge.fittingMode !== 'contact_constrained_refit' || bridge.preparedContacts !== 4) {
        throw new Error('contact-refit bridge is not the validated four-contact browser bridge');
    }
    for (const [inputField, pinField] of [
        ['sourceVideoSha256', 'sourceVideoSha256'], ['fittingBundleSha256', 'fittingBundleSha256'],
        ['immutableManifestSha256', 'immutableManifestSha256'], ['sourceModelSha256', 'sourceModelSha256'],
        ['skeletonSha256', 'sourceSkeletonSha256'], ['observationsSha256', 'observationsSha256'],
    ]) {
        if (summary.inputs?.[inputField] !== pins[pinField] || bridge.inputs?.[inputField] !== pins[pinField]) {
            throw new Error(`contact-refit ${inputField} does not bind the immutable manifest chain`);
        }
    }
    if (!samePath(summary.inputs?.bundleDirectory, validated.bundleDirectory)
        || !samePath(bridge.inputs?.bundleDirectory, validated.bundleDirectory)
        || !samePath(summary.inputs?.observationsPath, validated.observationsPath)
        || !samePath(bridge.inputs?.observationsPath, validated.observationsPath)) {
        throw new Error('contact-refit output paths do not bind the validated manifest inputs');
    }
    const provenance = object(refit.provenance, 'contact-refit provenance');
    if (provenance.schema !== 'autorig-browser-contact-refit-provenance.v1'
        || provenance.source !== 'immutable_pass_diagnostic' || provenance.browserOnly !== true
        || provenance.blenderUsed !== false || provenance.mixerUsed !== false) {
        throw new Error('contact-refit provenance is not the immutable browser diagnostic contract');
    }
    for (const [name, expected] of Object.entries(pins)) {
        if (provenance[name] !== expected) throw new Error(`contact-refit provenance ${name} does not match its manifest pin`);
    }
    const fitted = validateFittedAnimationArtifact(fittedAnimation, summary);
    validateThreeClipArtifact(threeClip, summary, fitted);
    return { summary, bridge, fittedAnimation, threeClip };
}

function inspectContactManifest(paths, spec, validated, chain, deepValidator = validateContactRefitInputs) {
    if (!fs.existsSync(paths.contactManifest)) return null;
    const snapshot = readSnapshot(paths.contactManifest, 'contact-refit input manifest');
    const externalPin = spec.externalPins.contactRefitInputManifestSha256;
    if (externalPin == null) {
        return { awaitingExternalPin: true, observed: evidence(snapshot), artifacts: [evidence(snapshot)] };
    }
    if (snapshot.sha256 !== externalPin) throw new Error('contact-refit input manifest does not match its external pin');
    const deep = object(
        deepValidator({ inputManifestPath: snapshot.path, expectedManifestSha256: externalPin }),
        'deep contact-refit input validation',
    );
    const expectedPins = {
        inputManifestSha256: snapshot.sha256,
        sourceVideoSha256: validated.candidate.sha256,
        fittingBundleSha256: validated.canonical.fittingSnapshot.sha256,
        immutableManifestSha256: validated.canonical.immutableSnapshot.sha256,
        sourceModelSha256: validated.canonical.sourceModelSha256,
        observationsSha256: chain.observations.observations.sha256,
        bridgeReportSha256: chain.initialFit.bridge.sha256,
        initialFitSummarySha256: chain.initialFit.summary.sha256,
        diagnosticSha256: chain.diagnostic.diagnostic.sha256,
    };
    for (const [name, expected] of Object.entries(expectedPins)) {
        if (deep.pins?.[name] !== expected) throw new Error(`deep contact manifest pin ${name} breaks the candidate-to-refit chain`);
    }
    if (!samePath(deep.bundleDirectory, validated.canonical.bundleDirectory)
        || !samePath(deep.observationsPath, chain.observations.observations.path)) {
        throw new Error('deep contact manifest paths break the candidate-to-refit chain');
    }
    return {
        awaitingExternalPin: false, observed: evidence(snapshot), artifacts: [evidence(snapshot)],
        validation: deep,
    };
}

function inspectContactRefit(
    paths,
    spec,
    validated,
    contactManifest,
    deepOutputValidator = validateFinalContactRefitOutputs,
    clipValidator = validateHorse2QaInputs,
) {
    if (!fs.existsSync(paths.contactRefit)) return null;
    exactDirectoryFiles(paths.contactRefit, [
        'bridge-report.json', 'fit-summary.json', 'fitted-animation.json', 'three-clip.json',
    ], 'browser contact-refit output');
    const snapshots = Object.fromEntries(['bridge-report.json', 'fit-summary.json', 'fitted-animation.json', 'three-clip.json']
        .map((name) => [name, readSnapshot(path.join(paths.contactRefit, name), `contact-refit ${name}`)]));
    deepOutputValidator({ snapshots, validatedInput: contactManifest.validation });
    const clipSnapshot = snapshots['three-clip.json'];
    const externalPin = spec.externalPins.threeClipSha256;
    if (externalPin == null) {
        return {
            awaitingExternalPin: true, observed: evidence(clipSnapshot),
            artifacts: Object.values(snapshots).map(evidence), clip: clipSnapshot,
        };
    }
    if (clipSnapshot.sha256 !== externalPin) throw new Error('final Three clip does not match its external pin');
    clipValidator({
        bundleDirectory: validated.canonical.bundleDirectory,
        expectedImmutableManifestSha256: validated.canonical.immutableSnapshot.sha256,
        expectedFittingBundleSha256: validated.canonical.fittingSnapshot.sha256,
        expectedSourceModelSha256: validated.canonical.sourceModelSha256,
        threeClipPath: clipSnapshot.path,
        expectedThreeClipSha256: externalPin,
    });
    return {
        awaitingExternalPin: false, observed: evidence(clipSnapshot),
        artifacts: Object.values(snapshots).map(evidence), clip: clipSnapshot,
    };
}

function inspectVisualQa(paths, spec, validated) {
    if (!fs.existsSync(paths.visualQa)) return null;
    if (!fs.statSync(paths.visualQa).isDirectory()) throw new Error('visual QA output must be a directory');
    const evidenceSnapshot = readSnapshot(path.join(paths.visualQa, 'visual-phase-qa.json'), 'visual phase QA evidence');
    const report = parseJsonSnapshot(evidenceSnapshot, 'visual phase QA evidence');
    const clipPin = spec.externalPins.threeClipSha256;
    const local = object(report.local_evidence, 'visual QA local_evidence');
    const inputs = object(local.immutable_inputs, 'visual QA immutable_inputs');
    const gate = object(report.visual_phase_gate, 'visual QA visual_phase_gate');
    if (report.schema !== 'autorig.browser-horse-visual-phase-evidence-envelope.v1'
        || gate.schema !== 'autorig.animation-visual-phase-qa.v1'
        || gate.fitted_clip_sha256 !== clipPin || gate.decision !== null
        || gate.reviewer?.id !== null || gate.frames?.some((frame) => frame.evidence_url !== null)
        || gate.coincident_rest_vertex_separation?.report_url !== null
        || local.source_rig_type !== 'HORSE_2' || local.browser_only !== true || local.blender_used !== false
        || local.animation_evaluation !== 'Three.AnimationMixer'
        || inputs.three_clip?.sha256 !== clipPin
        || inputs.immutable_manifest?.sha256 !== validated.canonical.immutableSnapshot.sha256
        || inputs.fitting_bundle?.sha256 !== validated.canonical.fittingSnapshot.sha256
        || inputs.source_model?.sha256 !== validated.canonical.sourceModelSha256
        || typeof local.renderer?.three_module?.path !== 'string'
        || !samePath(local.renderer.three_module.path, validated.runtime.threeModule.path)
        || local.renderer?.three_module?.bytes !== validated.runtime.threeModule.bytes
        || local.renderer?.three_module?.sha256 !== validated.runtime.threeModule.sha256
        || local.approvals?.approved_for_animation_library !== false
        || local.approvals?.release_ready !== false || local.human_review?.decision !== null) {
        throw new Error('visual phase QA evidence does not preserve the pinned fail-closed contract');
    }
    const top = fs.readdirSync(paths.visualQa, { withFileTypes: true }).map((entry) => entry.name).sort();
    const expectedTop = ['camera-settings.json', 'deformation-report.json', 'fixed-camera-preview.mp4', 'frames', 'visual-phase-qa.json'];
    if (JSON.stringify(top) !== JSON.stringify(expectedTop)) throw new Error('visual QA output inventory is partial or unexpected');
    const frames = fs.readdirSync(path.join(paths.visualQa, 'frames')).sort();
    if (frames.length !== 49 || frames.some((name, index) => name !== `frame_${String(index).padStart(4, '0')}.png`)) {
        throw new Error('visual QA output must contain all 49 exact browser frames');
    }
    const pinnedArtifact = (rowValue, expectedPath, field) => {
        const row = object(rowValue, field);
        const snapshot = readSnapshot(expectedPath, field);
        if (!samePath(row.path, snapshot.path) || row.bytes !== snapshot.bytes || row.sha256 !== snapshot.sha256) {
            throw new Error(`${field} does not match its visual evidence pin`);
        }
        return snapshot;
    };
    const camera = pinnedArtifact(local.camera_settings, path.join(paths.visualQa, 'camera-settings.json'), 'visual QA camera settings');
    const deformation = pinnedArtifact(
        local.target_mesh_deformation_qa?.report,
        path.join(paths.visualQa, 'deformation-report.json'),
        'visual QA deformation report',
    );
    const video = pinnedArtifact(local.video, path.join(paths.visualQa, 'fixed-camera-preview.mp4'), 'visual QA video');
    if (gate.camera?.settings_sha256 !== camera.sha256
        || gate.coincident_rest_vertex_separation?.report_sha256 !== deformation.sha256
        || local.video?.fixed_camera !== true || local.video?.root_motion_locked !== true) {
        throw new Error('visual QA gate does not bind camera/deformation/video evidence');
    }
    const deformationJson = parseJsonSnapshot(deformation, 'visual QA deformation report');
    const machinePassed = deformationJson.passed === true;
    if (deformationJson.schema !== 'autorig.browser-horse-target-deformation-qa.v1'
        || local.target_mesh_deformation_qa?.measured_every_frame !== true
        || local.target_mesh_deformation_qa?.passed !== machinePassed
        || local.approvals?.machine_qa_passed !== machinePassed
        || local.approvals?.ready_for_human_review !== machinePassed
        || deformationJson.inputs?.fittingBundleSha256 !== validated.canonical.fittingSnapshot.sha256
        || deformationJson.inputs?.threeClipSha256 !== clipPin
        || deformationJson.inputs?.skinWeightsSha256 !== inputs.skin_weights?.sha256
        || deformationJson.inputs?.topologySha256 !== inputs.surface_topology?.sha256) {
        throw new Error('visual QA machine/deformation decision does not bind its immutable inputs');
    }
    const canonicalRows = [
        ['immutable_manifest', validated.canonical.immutableSnapshot],
        ['fitting_bundle', validated.canonical.fittingSnapshot],
    ];
    for (const [field, expected] of canonicalRows) {
        if (!samePath(inputs[field]?.path, expected.path) || inputs[field]?.bytes !== expected.bytes
            || inputs[field]?.sha256 !== expected.sha256) throw new Error(`visual QA ${field} pin changed`);
    }
    for (const [field, artifactKey] of [
        ['skeleton', 'skeleton'], ['skin_weights', 'skin_weights'], ['surface_topology', 'surface_topology'],
    ]) {
        const filename = validated.canonical.fitting.artifacts?.[artifactKey]?.filename;
        const expected = validated.canonical.inventory.get(filename);
        if (!expected || !samePath(inputs[field]?.path, expected.path) || inputs[field]?.bytes !== expected.bytes
            || inputs[field]?.sha256 !== expected.sha256) throw new Error(`visual QA ${field} pin changed`);
    }
    const localPhases = Array.isArray(local.phase_frames) ? local.phase_frames : [];
    const gatePhases = Array.isArray(gate.frames) ? gate.frames : [];
    const expectedPhaseRows = [['start', 0], ['middle', 24], ['three_quarter', 36]];
    if (localPhases.length !== 3 || gatePhases.length !== 3) throw new Error('visual QA must pin all three exact phases');
    expectedPhaseRows.forEach(([phase, frameIndex], index) => {
        const filename = path.join(paths.visualQa, 'frames', `frame_${String(frameIndex).padStart(4, '0')}.png`);
        const snapshot = pinnedArtifact(localPhases[index], filename, `visual QA phase ${phase}`);
        if (localPhases[index].phase !== phase || localPhases[index].frame_index !== frameIndex
            || gatePhases[index].phase !== phase || gatePhases[index].frame_index !== frameIndex
            || gatePhases[index].sha256 !== snapshot.sha256) {
            throw new Error(`visual QA phase ${phase} is not bound across local and public evidence`);
        }
    });
    const artifacts = [evidence(evidenceSnapshot), evidence(camera), evidence(deformation), evidence(video)];
    for (const name of frames) artifacts.push(evidence(readSnapshot(path.join(paths.visualQa, 'frames', name), `visual QA ${name}`)));
    return { decision: { passed: machinePassed, status: machinePassed ? 'PASS_MACHINE_QA_AWAITING_HUMAN' : 'FAIL_MACHINE_QA' }, artifacts };
}

function buildCommands(validated, paths, spec) {
    const python = validated.runtime.executables.python.path;
    const node = validated.runtime.executables.node.path;
    const scripts = Object.fromEntries([
        'browser_fit_canary.mjs', 'diagnose_browser_hoof_contacts.mjs',
        'author_browser_contact_refit_manifest.mjs', 'browser_contact_refit.mjs',
        'browser_horse_visual_phase_qa.mjs',
    ].map((name) => [name, path.join(TOOLS_DIRECTORY, name)]));
    Object.entries(scripts).forEach(([name, filename]) => readSnapshot(filename, `pipeline tool ${name}`));
    const toolSourcePins = Object.values(validated.toolSources);
    return {
        objectGate: command(PYTHON_WORKING_DIRECTORY, [
            python, '-m', 'animation_fitting.object_region_video_gate',
            '--candidate', validated.candidate.path, '--candidate-sha256', validated.candidate.sha256,
            '--candidate-bytes', String(validated.candidate.bytes), '--endpoint-guide', validated.guide.endpoint.path,
            '--endpoint-guide-sha256', validated.guide.endpoint.sha256,
            '--endpoint-guide-bytes', String(validated.guide.endpoint.bytes),
            '--guide-bundle', validated.guide.bundleDirectory,
            '--guide-manifest-sha256', validated.guide.manifestSnapshot.sha256,
            '--output-dir', paths.objectGate,
        ], [...toolSourcePins, validated.runtime.executables.python, validated.candidate,
            validated.guide.endpoint, validated.guide.manifestSnapshot]),
        observations: command(PYTHON_WORKING_DIRECTORY, [
            python, '-m', 'animation_fitting.tracking_runtime',
            '--runtime-root', validated.runtime.trackingRuntimeRoot,
            '--runtime-lock', validated.runtime.trackingRuntimeLock.path,
            'observe', '--video', validated.candidate.path,
            '--bundle', validated.canonical.bundleDirectory, '--output-dir', paths.observations,
            '--device', 'cuda', '--loop',
            '--browser-endpoint-guide-bundle', validated.guide.bundleDirectory,
            '--browser-endpoint-guide-manifest-sha256', validated.guide.manifestSnapshot.sha256,
            '--ffprobe', validated.runtime.executables.ffprobe.path,
        ], [...toolSourcePins, validated.runtime.executables.python, validated.runtime.executables.ffprobe,
            validated.runtime.trackingRuntimeLock, validated.candidate,
            validated.guide.manifestSnapshot, validated.canonical.immutableSnapshot,
            validated.canonical.fittingSnapshot]),
        initialFit: command(TOOLS_DIRECTORY, [
            node, scripts['browser_fit_canary.mjs'], '--bundle-dir', validated.canonical.bundleDirectory,
            '--observations', path.join(paths.observations, 'observations.json'),
            '--three-module', validated.runtime.threeModule.path, '--output-dir', paths.initialFit,
            '--clip-name', `${spec.clipName}_Initial`, '--position-mappings', 'auto',
        ], [...toolSourcePins, validated.runtime.executables.node, validated.runtime.threeModule,
            validated.canonical.immutableSnapshot, validated.canonical.fittingSnapshot]),
        diagnostic: command(TOOLS_DIRECTORY, [
            node, scripts['diagnose_browser_hoof_contacts.mjs'],
            '--observations', path.join(paths.observations, 'observations.json'),
            '--bridge-report', path.join(paths.initialFit, 'bridge-report.json'),
            '--masks-dir', path.join(paths.observations, 'masks'),
            '--output', paths.diagnostic, '--ground-output', paths.groundEvidence,
        ], [...toolSourcePins, validated.runtime.executables.node]),
        contactManifest: command(TOOLS_DIRECTORY, [
            node, scripts['author_browser_contact_refit_manifest.mjs'],
            '--bundle-dir', validated.canonical.bundleDirectory,
            '--observations', path.join(paths.observations, 'observations.json'),
            '--bridge-report', path.join(paths.initialFit, 'bridge-report.json'),
            '--initial-fit-summary', path.join(paths.initialFit, 'fit-summary.json'),
            '--contact-diagnostic', paths.diagnostic, '--output', paths.contactManifest,
        ], [...toolSourcePins, validated.runtime.executables.node]),
        contactRefit: command(TOOLS_DIRECTORY, [
            node, scripts['browser_contact_refit.mjs'], '--input-manifest', paths.contactManifest,
            '--input-manifest-sha256', spec.externalPins.contactRefitInputManifestSha256,
            '--three-module', validated.runtime.threeModule.path, '--output-dir', paths.contactRefit,
            '--clip-name', spec.clipName,
        ], [...toolSourcePins, validated.runtime.executables.node, validated.runtime.threeModule]),
        visualQa: command(TOOLS_DIRECTORY, [
            node, scripts['browser_horse_visual_phase_qa.mjs'],
            '--bundle-dir', validated.canonical.bundleDirectory,
            '--immutable-manifest-sha256', validated.canonical.immutableSnapshot.sha256,
            '--fitting-bundle-sha256', validated.canonical.fittingSnapshot.sha256,
            '--source-model-sha256', validated.canonical.sourceModelSha256,
            '--three-clip', path.join(paths.contactRefit, 'three-clip.json'),
            '--three-clip-sha256', spec.externalPins.threeClipSha256,
            '--semantic-id', spec.semanticId,
            '--three-module', validated.runtime.threeModule.path,
            '--three-module-sha256', validated.runtime.threeModule.sha256,
            '--three-revision', '160', '--chrome', validated.runtime.executables.chrome.path,
            '--ffmpeg', validated.runtime.executables.ffmpeg.path,
            '--ffprobe', validated.runtime.executables.ffprobe.path,
            '--output-dir', paths.visualQa,
        ], [...toolSourcePins, validated.runtime.executables.node, validated.runtime.executables.chrome,
            validated.runtime.executables.ffmpeg, validated.runtime.executables.ffprobe,
            validated.runtime.threeModule]),
    };
}

function validateSpecSnapshot(specPathValue, expectedSpecSha256) {
    const specPath = path.resolve(specPathValue);
    const snapshot = readSnapshot(specPath, 'pipeline spec');
    if (snapshot.sha256 !== sha256(expectedSpecSha256, 'expected spec SHA-256')) {
        throw new Error('pipeline spec does not match the externally supplied SHA-256');
    }
    const spec = parseJsonSnapshot(snapshot, 'pipeline spec');
    if (spec.schema !== V14_PIPELINE_SPEC_SCHEMA || spec.browserOnly !== true
        || spec.blenderUsed !== false || spec.orchestratorExecutesSubprocesses !== false) {
        throw new Error('pipeline spec must be the browser-only/no-subprocess V14 contract');
    }
    const semanticId = string(spec.semanticId, 'spec.semanticId');
    if (semanticId !== 'walk_forward') throw new Error('V14 Horse canary semanticId must be walk_forward');
    const clipName = string(spec.clipName, 'spec.clipName');
    const outputRoot = resolveFrom(path.dirname(specPath), spec.outputRoot, 'spec.outputRoot');
    const candidate = pinnedSnapshot({ base: path.dirname(specPath), descriptor: spec.candidate, field: 'spec.candidate' });
    if (isInside(outputRoot, candidate.path)) throw new Error('candidate must be outside outputRoot');
    const guide = validateGuide(path.dirname(specPath), spec.guide);
    const canonical = validateCanonicalBundle(path.dirname(specPath), spec.canonicalBundle);
    const runtime = validateRuntime(path.dirname(specPath), spec.runtime);
    const toolSources = validateToolSources(path.dirname(specPath), spec.toolSources);
    const externalPinsValue = spec.externalPins == null ? {} : object(spec.externalPins, 'spec.externalPins');
    const externalPins = {
        contactRefitInputManifestSha256: externalPinsValue.contactRefitInputManifestSha256 == null
            ? null : sha256(externalPinsValue.contactRefitInputManifestSha256, 'spec.externalPins.contactRefitInputManifestSha256'),
        threeClipSha256: externalPinsValue.threeClipSha256 == null
            ? null : sha256(externalPinsValue.threeClipSha256, 'spec.externalPins.threeClipSha256'),
    };
    return {
        snapshot,
        spec: { ...spec, semanticId, clipName, outputRoot, externalPins },
        validated: { candidate, guide, canonical, runtime, toolSources },
    };
}

export function inspectV14Pipeline({ specPath, expectedSpecSha256 }, dependencies = {}) {
    const loaded = validateSpecSnapshot(specPath, expectedSpecSha256);
    const { spec, validated } = loaded;
    ensureOutputRootShape(spec.outputRoot);
    const paths = expectedPaths(spec.outputRoot);
    assertNoOutOfOrderStages(paths);
    const commands = buildCommands(validated, paths, spec);
    const completedStages = [];
    const artifactEvidence = [];
    const complete = (name, artifacts) => {
        completedStages.push(name);
        artifactEvidence.push(...artifacts.map((row) => ({ stage: name, ...row })));
    };
    const baseState = {
        schema: V14_PIPELINE_STATE_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        fittingMixerUsed: false,
        qaAnimationMixerUsed: true,
        orchestratorExecutesSubprocesses: false,
        spec: evidence(loaded.snapshot),
        immutableInputs: {
            candidate: evidence(validated.candidate),
            guideManifest: evidence(validated.guide.manifestSnapshot),
            guideEndpoint: evidence(validated.guide.endpoint),
            canonicalImmutableManifest: evidence(validated.canonical.immutableSnapshot),
            fittingBundle: evidence(validated.canonical.fittingSnapshot),
            sourceModelSha256: validated.canonical.sourceModelSha256,
            threeModule: { ...evidence(validated.runtime.threeModule), revision: '160' },
            toolSources: Object.fromEntries(Object.entries(validated.toolSources).map(([name, pin]) => [name, evidence(pin)])),
        },
        outputRoot: spec.outputRoot,
    };
    const finish = (status, next = null, pinRequest = null, failures = []) => ({
        ...baseState, status, completedStages, artifacts: artifactEvidence,
        next, pinRequest, failures,
    });
    const ready = (status, stage, baseCommand) => finish(status, {
        stage,
        command: withAdditionalPreconditions(
            baseCommand,
            [evidence(loaded.snapshot), ...artifactEvidence.map(({ stage: _stage, ...pin }) => pin)],
        ),
    });

    const gate = inspectObjectGate(paths, validated);
    if (!gate) return ready('READY_OBJECT_REGION_GATE', 'object_region_gate', commands.objectGate);
    complete('object_region_gate', gate.artifacts);
    if (!gate.decision.passed) {
        rejectExistingLaterStages(paths, [
            'observations', 'initialFit', 'diagnostic', 'groundEvidence', 'contactManifest', 'contactRefit', 'visualQa',
        ], 'object-region gate failed');
        return finish('FAILED_OBJECT_REGION_GATE', null, null, ['object_region_gate']);
    }

    const observations = inspectObservations(paths, validated);
    if (!observations) return ready('READY_TRACKING', 'tapnext_sam2_observations', commands.observations);
    complete('tapnext_sam2_observations', observations.artifacts);

    const initialFit = inspectInitialFit(paths, validated, observations.observations);
    if (!initialFit) return ready('READY_INITIAL_BROWSER_FIT', 'initial_browser_fit', commands.initialFit);
    complete('initial_browser_fit', initialFit.artifacts);

    const diagnostic = inspectDiagnostic(paths, validated, observations.observations, initialFit);
    if (!diagnostic) return ready('READY_HOOF_CONTACT_DIAGNOSTIC', 'hoof_contact_diagnostic', commands.diagnostic);
    complete('hoof_contact_diagnostic', diagnostic.artifacts);
    if (!diagnostic.decision.passed) {
        rejectExistingLaterStages(
            paths,
            ['contactManifest', 'contactRefit', 'visualQa'],
            'hoof-contact diagnostic failed',
        );
        return finish('FAILED_HOOF_CONTACT_DIAGNOSTIC', null, null, diagnostic.decision.failures);
    }

    const manifest = inspectContactManifest(
        paths,
        spec,
        validated,
        { observations, initialFit, diagnostic },
        dependencies.validateContactRefitInputs || validateContactRefitInputs,
    );
    if (!manifest) return ready('READY_CONTACT_REFIT_MANIFEST', 'contact_refit_manifest', commands.contactManifest);
    complete('contact_refit_manifest', manifest.artifacts);
    if (manifest.awaitingExternalPin) {
        rejectExistingLaterStages(
            paths,
            ['contactRefit', 'visualQa'],
            'contact-refit manifest is not externally pinned',
        );
        return finish('AWAITING_EXTERNAL_CONTACT_MANIFEST_PIN', null, {
            field: 'externalPins.contactRefitInputManifestSha256',
            observedSha256NotTrusted: manifest.observed.sha256,
            observedBytes: manifest.observed.bytes,
            instruction: 'Create a new externally SHA-pinned spec revision; do not edit or overwrite this artifact.',
        });
    }

    const refit = inspectContactRefit(
        paths,
        spec,
        validated,
        manifest,
        dependencies.validateFinalContactRefitOutputs || validateFinalContactRefitOutputs,
        dependencies.validateHorse2QaInputs || validateHorse2QaInputs,
    );
    if (!refit) return ready('READY_BROWSER_CONTACT_REFIT', 'browser_contact_refit', commands.contactRefit);
    complete('browser_contact_refit', refit.artifacts);
    if (refit.awaitingExternalPin) {
        rejectExistingLaterStages(paths, ['visualQa'], 'Three clip is not externally pinned');
        return finish('AWAITING_EXTERNAL_THREE_CLIP_PIN', null, {
            field: 'externalPins.threeClipSha256',
            observedSha256NotTrusted: refit.observed.sha256,
            observedBytes: refit.observed.bytes,
            instruction: 'Create a new externally SHA-pinned spec revision; do not edit or overwrite this artifact.',
        });
    }

    const visualQa = inspectVisualQa(paths, spec, validated);
    if (!visualQa) return ready('READY_BROWSER_VISUAL_PHASE_QA', 'browser_visual_phase_qa', commands.visualQa);
    complete('browser_visual_phase_qa', visualQa.artifacts);
    if (!visualQa.decision.passed) return finish('FAILED_BROWSER_VISUAL_PHASE_QA', null, null, ['machine_target_or_visual_phase_qa']);
    return finish('PASS_MACHINE_QA_AWAITING_HUMAN_REVIEW');
}

function writeExclusive(filenameValue, state) {
    const filename = path.resolve(filenameValue);
    const parent = path.dirname(filename);
    if (!fs.existsSync(parent) || !fs.statSync(parent).isDirectory()) throw new Error(`state parent must exist: ${parent}`);
    const payload = Buffer.from(`${JSON.stringify(state, null, 2)}\n`, 'utf8');
    if (fs.existsSync(filename)) throw new Error(`state output already exists: ${filename}`);
    const staging = `${filename}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    const handle = fs.openSync(staging, 'wx');
    try {
        fs.writeFileSync(handle, payload);
        fs.fsyncSync(handle);
    } finally {
        fs.closeSync(handle);
    }
    try {
        fs.linkSync(staging, filename);
        fs.unlinkSync(staging);
    } catch (error) {
        try { if (fs.existsSync(staging)) fs.unlinkSync(staging); } catch { /* preserve original error */ }
        throw error;
    }
    return { path: filename, bytes: payload.length, sha256: sha256Buffer(payload) };
}

export function parsePipelineArgs(argv) {
    const result = {};
    let help = false;
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag === '--help' || flag === '-h') { help = true; continue; }
        if (!['--spec', '--spec-sha256', '--state'].includes(flag)) throw new Error(`unknown option ${flag}`);
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
        result[flag.slice(2)] = argv[++index];
    }
    if (help) return { help: true };
    for (const key of ['spec', 'spec-sha256', 'state']) if (!result[key]) throw new Error(`--${key} is required`);
    return { specPath: result.spec, expectedSpecSha256: result['spec-sha256'], statePath: result.state };
}

function helpText() {
    return `Usage:
  node run_v14_browser_fitting_pipeline.mjs --spec FILE \\
    --spec-sha256 SHA256 --state NEW_FILE

Validates immutable V14 candidate/guide/Horse_2/Three/runtime pins and every
already-published stage. Writes one deterministic, non-overwriting state file
with either one exact next command, a terminal QA failure, or an external pin
request. It never executes subprocesses, GPU work, Blender, or a mixer itself.
Fitting remains mixer-free; the planned final QA explicitly evaluates the
pinned clip with Three.AnimationMixer.

Spec schema: ${V14_PIPELINE_SPEC_SCHEMA}
Required top-level fields: browserOnly=true, blenderUsed=false,
orchestratorExecutesSubprocesses=false, candidate{path,bytes,sha256},
guide{bundleDirectory,immutableManifestSha256,endpointGuide{path,bytes,sha256}},
canonicalBundle{directory,immutableManifestSha256,fittingBundleSha256,
sourceModelSha256}, runtime{executables.{python,node,chrome,ffmpeg,ffprobe},
threeModule{path,bytes,sha256,revision:"160"},trackingRuntimeRoot,
trackingRuntimeLock{path,bytes,sha256}}, semanticId:"walk_forward", clipName,
outputRoot, exact toolSources descriptors for the exported source closure, and
externalPins. Executable/tool descriptors are externally pinned
path/bytes/SHA-256 objects. externalPins values may initially be null; the
state then requests a new externally pinned spec revision before continuing.`;
}

export function runPipelineCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parsePipelineArgs(argv);
        if (config.help) {
            streams.stdout.write(`${helpText()}\n`);
            return 0;
        }
        const state = inspectV14Pipeline(config);
        const pin = writeExclusive(config.statePath, state);
        streams.stdout.write(`${JSON.stringify({ status: state.status, state: pin, nextStage: state.next?.stage || null })}\n`);
        return state.status.startsWith('FAILED_') ? 3 : 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = runPipelineCli();

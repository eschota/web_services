#!/usr/bin/env node
/**
 * Immutable, fail-closed spec author for the real Horse_2 V14 browser fitting
 * pipeline.  It only snapshots and validates local files, authors JSON, and
 * asks the existing runner to inspect that JSON.  It never starts a job,
 * subprocess, GPU stage, Blender, database, or network operation.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { pathToFileURL } from 'node:url';

import {
    V14_PIPELINE_SPEC_SCHEMA as RUNNER_V14_PIPELINE_SPEC_SCHEMA,
    V14_PIPELINE_TOOL_SOURCE_PATHS,
    inspectV14Pipeline,
} from './run_v14_browser_fitting_pipeline.mjs';

export const V14_PIPELINE_SPEC_SCHEMA_V2 = 'autorig.v14-browser-fitting-pipeline-spec.v2';
export const V14_CONTROLLED_GENERATION_SCHEMA = 'autorig.v14-controlled-generation-binding.v1';
export const V14_SPEC_AUTHOR_PROVENANCE_SCHEMA = 'autorig.v14-browser-fitting-spec-author-provenance.v2';
export const V14_RUNTIME_PINS_SCHEMA = 'autorig.v14-browser-fitting-runtime-pins.v1';
export const V14_TOOL_SOURCE_PINS_SCHEMA = 'autorig.v14-browser-fitting-tool-source-pins.v1';

const SHA256_RE = /^[0-9a-f]{64}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$/;
const EXECUTABLE_NAMES = Object.freeze(['python', 'node', 'chrome', 'ffmpeg', 'ffprobe']);

export const REAL_V14_CONTRACT = Object.freeze({
    experimentId: 'horse_walk_v14_browser_interval_guide_seed_6550110377254033429_v1',
    experimentSha256: '0f172076147e94099ea7c0cf3c323a46f698ea48e55b7bce9acec789e0e77c66',
    jobId: 'c4d04cf43ae38e92a75b4bfe3f9763c00e4c8ef1d4d2915ed4ed9ff1d41e961e',
    promptId: '0472b8ba-385d-403d-886e-ff1f8d8bb46c',
    seed: 6550110377254033429n,
    workerId: 'local-4090',
    workerBaseUrl: 'http://127.0.0.1:8188',
    workflowName: 'autorig_ltx2_animal_loop_v1_api.json',
    workflowFingerprintSha256: 'e0f549b58d3933027a4f4d3fde69d6e3dfb6d360f0200e8f00a9d2bff278bc56',
    canonicalBundleName: 'horse-canonical-f1',
    canonicalImmutableManifestSha256: 'f5e55c5073d09bc01dac90f4b7244f995fd42b0bdd37e09258cd4178e5573873',
    fittingBundleSha256: 'e328fae0fd850a38249fb8b40c2e2766e8d90ab1ce4c1f241e926e9230d23744',
    sourceModelSha256: 'fa75772d83c2613ddd6df6f7a305a407e12abf4a75c9083bb53df4d2619f50a1',
    sourceReferenceSha256: '94bf47cc137c0aaee975b2a75b7cd2b28f75215e282cdb6865bdd4095630a0b1',
    skeletonSha256: '0e7fb527d4df5273c289a61a2bbb1f456d9cd10f83d2b09cbbea05daade6f8be',
    guideBundleName: 'horse-walk-v14-browser-interval-guide-f1',
    guideManifestSha256: 'a09418a8725984126071614b8921eeffaee7cd9a91ca9d4c4ae34b49d1f3a6cb',
    guideVideoSha256: '0a6f08834dd562e1200dd211842604d97fc487243a04ae3ad7838f0f948c7c05',
    guideVideoBytes: 650377,
    endpointGuideSha256: 'd0714166ac91d38a6cfe0f0d2ee18bc18f221fc2ca6782d99a8a0cbb215576b3',
    threeModuleSha256: '76dea8151bc9352aef3528b4262e249b2604f62543828328db978d060d61a495',
    frameCount: 49,
    inputFps: 24,
    outputFps: 30,
    width: 768,
    height: 448,
});

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${field} must be an object`);
    return value;
}

function nonEmptyString(value, field) {
    if (typeof value !== 'string' || !value.trim()) throw new Error(`${field} must be a non-empty string`);
    return value.trim();
}

function sha256(value, field) {
    const result = nonEmptyString(value, field);
    if (!SHA256_RE.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function integer(value, field, minimum = 0) {
    const result = Number(value);
    if (!Number.isSafeInteger(result) || result < minimum) throw new Error(`${field} must be a safe integer >= ${minimum}`);
    return result;
}

function hash(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
}

function resolvePath(base, value, field) {
    return path.resolve(base, nonEmptyString(value, field));
}

function samePath(left, right) {
    const a = path.resolve(left);
    const b = path.resolve(right);
    return process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
}

function isInside(parent, child) {
    const relative = path.relative(path.resolve(parent), path.resolve(child));
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function readSnapshot(filenameValue, field) {
    const filename = path.resolve(filenameValue);
    let before;
    let buffer;
    let after;
    try {
        before = fs.statSync(filename);
        buffer = fs.readFileSync(filename);
        after = fs.statSync(filename);
    } catch (error) {
        throw new Error(`${field} is unavailable at ${filename}: ${error.message}`);
    }
    if (!before.isFile() || !after.isFile() || buffer.length < 1) throw new Error(`${field} must be a non-empty file`);
    if (before.size !== buffer.length || after.size !== buffer.length
        || before.dev !== after.dev || before.ino !== after.ino || before.mtimeMs !== after.mtimeMs) {
        throw new Error(`${field} changed while read`);
    }
    return { path: filename, bytes: buffer.length, sha256: hash(buffer), buffer };
}

function parseJson(snapshot, field) {
    let parsed;
    try { parsed = JSON.parse(snapshot.buffer.toString('utf8')); } catch (error) {
        throw new Error(`${field} is invalid JSON: ${error.message}`);
    }
    return object(parsed, field);
}

function descriptor(snapshot) {
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256 };
}

function pinnedFile(base, value, field) {
    const pin = object(value, field);
    const snapshot = readSnapshot(resolvePath(base, pin.path, `${field}.path`), field);
    if (snapshot.bytes !== integer(pin.bytes, `${field}.bytes`, 1)) throw new Error(`${field} byte count mismatch`);
    if (snapshot.sha256 !== sha256(pin.sha256, `${field}.sha256`)) throw new Error(`${field} SHA-256 mismatch`);
    return snapshot;
}

function pinnedJson(filename, expectedSha256, field) {
    const snapshot = readSnapshot(filename, field);
    if (snapshot.sha256 !== sha256(expectedSha256, `${field} external SHA-256`)) {
        throw new Error(`${field} SHA-256 mismatch`);
    }
    return { snapshot, value: parseJson(snapshot, field) };
}

function safeFilename(value, field) {
    const filename = nonEmptyString(value, field);
    if (path.isAbsolute(filename) || path.basename(filename) !== filename || filename === '.' || filename === '..') {
        throw new Error(`${field} must be a bundle-root filename`);
    }
    return filename;
}

function exactKeys(value, expected, field) {
    const actual = Object.keys(object(value, field)).sort();
    const wanted = [...expected].sort();
    if (JSON.stringify(actual) !== JSON.stringify(wanted)) {
        throw new Error(`${field} must contain exactly ${wanted.join(', ')}`);
    }
}

function stableValue(value) {
    if (Array.isArray(value)) return value.map(stableValue);
    if (value && typeof value === 'object') {
        return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stableValue(value[key])]));
    }
    return value;
}

function canonicalJson(value) {
    return JSON.stringify(stableValue(value));
}

function expectedPromptId(jobId) {
    const raw = hash(Buffer.from(`autorig-controlled-animation-fitting:${jobId}`, 'utf8')).slice(0, 32);
    return `${raw.slice(0, 8)}-${raw.slice(8, 12)}-4${raw.slice(13, 16)}-8${raw.slice(17, 20)}-${raw.slice(20, 32)}`;
}

function rawJsonNumber(snapshot, key, field) {
    const escaped = key.replaceAll(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const matches = [...snapshot.buffer.toString('utf8').matchAll(new RegExp(`"${escaped}"\\s*:\\s*(-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)`, 'g'))];
    if (matches.length !== 1) throw new Error(`${field} must occur exactly once as a JSON number`);
    if (!Number.isFinite(Number(matches[0][1]))) throw new Error(`${field} must be finite`);
    return matches[0][1];
}

function validateCanonicalBundle(config, expected, snapshots) {
    const root = path.resolve(config.canonicalBundle);
    if (!fs.existsSync(root) || !fs.statSync(root).isDirectory()) throw new Error('canonical bundle must be a directory');
    if (path.basename(root) !== expected.canonicalBundleName) throw new Error('canonical bundle name drift');
    const loaded = pinnedJson(path.join(root, 'immutable_manifest.json'), config.canonicalImmutableSha256, 'canonical immutable manifest');
    snapshots.push(loaded.snapshot);
    if (loaded.snapshot.sha256 !== expected.canonicalImmutableManifestSha256) throw new Error('canonical immutable contract drift');
    const immutable = loaded.value;
    if (immutable.schema !== 'autorig-fitting-immutable-copy.v1') throw new Error('canonical immutable schema drift');
    const rows = Array.isArray(immutable.files) ? immutable.files : [];
    if (!rows.length || rows.length !== integer(immutable.bundle_file_count, 'canonical bundle_file_count', 1)) {
        throw new Error('canonical immutable inventory is invalid');
    }
    const inventory = new Map();
    let bytes = 0;
    for (const [index, rowValue] of rows.entries()) {
        const row = object(rowValue, `canonical file ${index}`);
        const filename = safeFilename(row.filename, `canonical file ${index}.filename`);
        if (inventory.has(filename)) throw new Error(`canonical immutable inventory repeats ${filename}`);
        const snapshot = readSnapshot(path.join(root, filename), `canonical file ${filename}`);
        if (snapshot.bytes !== integer(row.bytes, `canonical file ${filename}.bytes`, 1)
            || snapshot.sha256 !== sha256(row.sha256, `canonical file ${filename}.sha256`)) {
            throw new Error(`canonical file ${filename} drift`);
        }
        snapshots.push(snapshot);
        inventory.set(filename, snapshot);
        bytes += snapshot.bytes;
    }
    if (bytes !== integer(immutable.bundle_total_bytes, 'canonical bundle_total_bytes', 1)) {
        throw new Error('canonical total byte count drift');
    }
    const actual = fs.readdirSync(root, { withFileTypes: true })
        .filter((entry) => entry.isFile() && entry.name !== 'immutable_manifest.json').map((entry) => entry.name).sort();
    if (JSON.stringify(actual) !== JSON.stringify([...inventory.keys()].sort())) {
        throw new Error('canonical bundle has unpinned or missing files');
    }
    const fittingName = safeFilename(immutable.bundle_manifest?.filename, 'canonical fitting filename');
    const fittingSnapshot = inventory.get(fittingName);
    const fittingSha = sha256(config.fittingBundleSha256, 'fitting bundle external SHA-256');
    if (!fittingSnapshot || fittingSnapshot.sha256 !== fittingSha
        || fittingSha !== expected.fittingBundleSha256 || immutable.bundle_manifest?.sha256 !== fittingSha) {
        throw new Error('canonical fitting bundle drift');
    }
    const fitting = parseJson(fittingSnapshot, 'canonical fitting bundle');
    const sourceSha = sha256(config.sourceModelSha256, 'source model external SHA-256');
    if (fitting.schema !== 'autorig-actionless-fitting-bundle.v1'
        || sourceSha !== expected.sourceModelSha256 || immutable.source_model?.sha256 !== sourceSha
        || immutable.source_model?.copied !== false
        || immutable.source_model?.filename !== fitting.source?.filename
        || inventory.has(String(fitting.source?.filename || ''))
        || fitting.source?.sha256 !== sourceSha || fitting.source?.species !== 'horse'
        || fitting.source?.rig_type !== 'HORSE_2' || fitting.source?.orientation !== 'canonical'
        || fitting.actionless?.actionless !== true) {
        throw new Error('canonical Horse_2 source/actionless provenance drift');
    }
    const artifacts = object(fitting.artifacts, 'canonical fitting artifacts');
    for (const [name, pinValue] of Object.entries(artifacts)) {
        const pin = object(pinValue, `canonical fitting artifact ${name}`);
        const filename = safeFilename(pin.filename, `canonical fitting artifact ${name}.filename`);
        const snapshot = inventory.get(filename);
        if (!snapshot || snapshot.bytes !== integer(pin.bytes, `canonical fitting artifact ${name}.bytes`, 1)
            || snapshot.sha256 !== sha256(pin.sha256, `canonical fitting artifact ${name}.sha256`)) {
            throw new Error(`canonical fitting artifact ${name} breaks immutable inventory pins`);
        }
    }
    const referenceSha = sha256(artifacts.rgb?.sha256, 'canonical RGB reference SHA-256');
    const skeletonSha = sha256(artifacts.skeleton?.sha256, 'canonical skeleton SHA-256');
    if (referenceSha !== expected.sourceReferenceSha256 || skeletonSha !== expected.skeletonSha256) {
        throw new Error('canonical RGB/skeleton contract drift');
    }
    return { root, immutable: loaded.snapshot, fitting: fittingSnapshot, sourceSha, referenceSha };
}

function validateGuide(config, expected, canonical, snapshots) {
    const root = path.resolve(config.guideBundle);
    if (!fs.existsSync(root) || !fs.statSync(root).isDirectory()) throw new Error('V14 guide bundle must be a directory');
    if (path.basename(root) !== expected.guideBundleName) throw new Error('V14 guide bundle name drift');
    const loaded = pinnedJson(path.join(root, 'immutable_manifest.json'), config.guideManifestSha256, 'V14 guide manifest');
    snapshots.push(loaded.snapshot);
    if (loaded.snapshot.sha256 !== expected.guideManifestSha256) throw new Error('V14 guide manifest contract drift');
    const manifest = loaded.value;
    if (manifest.schema !== 'autorig-browser-ltx-interval-guide-bundle.v1' || manifest.status !== 'PASS'
        || manifest.browserOnly !== true || manifest.blenderUsed !== false || manifest.rigType !== 'HORSE_2'
        || manifest.cycle_frame_count_int !== expected.frameCount
        || manifest.browser_frame_count_int !== expected.frameCount || manifest.guide_count_int !== 1) {
        throw new Error('V14 guide is not the authorized browser-only Horse_2 interval contract');
    }
    if (manifest.source_reference_sha256_string !== canonical.referenceSha) {
        throw new Error('V14 guide does not bind the canonical RGB reference');
    }
    const rows = Array.isArray(manifest.frames_array) ? manifest.frames_array : [];
    if (rows.length !== expected.frameCount) throw new Error('V14 guide frame inventory drift');
    const frames = [];
    rows.forEach((rowValue, index) => {
        const row = object(rowValue, `V14 guide frame ${index}`);
        if (row.frame_index_int !== index) throw new Error(`V14 guide chronology drift at ${index}`);
        const snapshot = readSnapshot(path.join(root, safeFilename(row.filename_string, `V14 guide frame ${index}.filename`)), `V14 guide frame ${index}`);
        if (snapshot.bytes !== integer(row.bytes_int, `V14 guide frame ${index}.bytes`, 1)
            || snapshot.sha256 !== sha256(row.sha256_string, `V14 guide frame ${index}.sha256`)) {
            throw new Error(`V14 guide frame ${index} drift`);
        }
        snapshots.push(snapshot);
        frames.push(snapshot);
    });
    if (manifest.endpoint_guide_sha256_string !== frames[0].sha256
        || frames[0].sha256 !== expected.endpointGuideSha256) throw new Error('V14 endpoint guide drift');
    const intervalRow = object(manifest.interval_guide_video_object, 'V14 interval guide video');
    const expectedVideoPath = path.join(root, safeFilename(intervalRow.filename, 'V14 interval guide video.filename'));
    if (!samePath(expectedVideoPath, config.guideVideo)) throw new Error('external V14 guide video path does not match its manifest');
    const video = readSnapshot(expectedVideoPath, 'V14 interval guide video');
    const externalVideoSha = sha256(config.guideVideoSha256, 'V14 guide video external SHA-256');
    const externalVideoBytes = integer(config.guideVideoBytes, 'V14 guide video external bytes', 1);
    if (video.sha256 !== externalVideoSha || video.bytes !== externalVideoBytes
        || externalVideoSha !== expected.guideVideoSha256 || externalVideoBytes !== expected.guideVideoBytes
        || intervalRow.sha256 !== video.sha256 || intervalRow.bytes !== video.bytes
        || intervalRow.frameCount !== expected.frameCount || intervalRow.audioStreamCount !== 0
        || intervalRow.width !== expected.width || intervalRow.height !== expected.height
        || intervalRow.frameRate !== expected.outputFps) {
        throw new Error('V14 interval guide video drift');
    }
    snapshots.push(video);
    const poseRow = object(manifest.poseContract, 'V14 pose contract');
    const pose = readSnapshot(path.join(root, safeFilename(poseRow.filename, 'V14 pose contract.filename')), 'V14 pose contract');
    if (pose.sha256 !== sha256(poseRow.sha256, 'V14 pose contract.sha256')
        || pose.bytes !== integer(poseRow.bytes, 'V14 pose contract.bytes', 1)) throw new Error('V14 pose contract drift');
    snapshots.push(pose);
    return { root, manifest: loaded.snapshot, endpoint: frames[0], video };
}

function validateRuntime(config, expected, snapshots) {
    const loaded = pinnedJson(config.runtimePins, config.runtimePinsSha256, 'runtime pin manifest');
    snapshots.push(loaded.snapshot);
    const manifest = loaded.value;
    if (manifest.schema !== V14_RUNTIME_PINS_SCHEMA) throw new Error('runtime pin manifest schema drift');
    exactKeys(manifest.executables, EXECUTABLE_NAMES, 'runtime executables');
    const base = path.dirname(loaded.snapshot.path);
    const executables = {};
    for (const name of EXECUTABLE_NAMES) {
        const snapshot = pinnedFile(base, manifest.executables[name], `runtime executable ${name}`);
        snapshots.push(snapshot);
        executables[name] = descriptor(snapshot);
    }
    const three = pinnedFile(base, manifest.threeModule, 'runtime Three module');
    snapshots.push(three);
    if (String(manifest.threeModule.revision) !== '160' || three.sha256 !== expected.threeModuleSha256) {
        throw new Error('runtime Three module is not the pinned r160 contract');
    }
    const trackingRuntimeRoot = resolvePath(base, manifest.trackingRuntimeRoot, 'runtime trackingRuntimeRoot');
    if (!fs.existsSync(trackingRuntimeRoot) || !fs.statSync(trackingRuntimeRoot).isDirectory()) {
        throw new Error('runtime trackingRuntimeRoot must be a directory');
    }
    const lock = pinnedFile(base, manifest.trackingRuntimeLock, 'tracking runtime lock');
    snapshots.push(lock);
    return {
        pinManifest: loaded.snapshot,
        value: {
            executables,
            threeModule: { ...descriptor(three), revision: '160' },
            trackingRuntimeRoot,
            trackingRuntimeLock: descriptor(lock),
        },
    };
}

function validateToolSources(config, snapshots) {
    const loaded = pinnedJson(config.toolSourcePins, config.toolSourcePinsSha256, 'tool-source pin manifest');
    snapshots.push(loaded.snapshot);
    const manifest = loaded.value;
    if (manifest.schema !== V14_TOOL_SOURCE_PINS_SCHEMA) throw new Error('tool-source pin manifest schema drift');
    const expectedNames = Object.keys(V14_PIPELINE_TOOL_SOURCE_PATHS).sort();
    if (expectedNames.length !== 28) throw new Error('runner no longer exports the exact 28-file source closure');
    exactKeys(manifest.sources, expectedNames, 'tool-source pins');
    const base = path.dirname(loaded.snapshot.path);
    const sources = {};
    for (const name of expectedNames) {
        const snapshot = pinnedFile(base, manifest.sources[name], `tool source ${name}`);
        if (!samePath(snapshot.path, V14_PIPELINE_TOOL_SOURCE_PATHS[name])) {
            throw new Error(`tool source ${name} does not resolve to the runner-exported path`);
        }
        snapshots.push(snapshot);
        sources[name] = descriptor(snapshot);
    }
    return { pinManifest: loaded.snapshot, sources };
}

function validateControlledJob(config, expected, canonical, guide, outputRoot, snapshots) {
    const loaded = pinnedJson(config.controlledJob, config.controlledJobSha256, 'completed controlled job');
    snapshots.push(loaded.snapshot);
    const job = loaded.value;
    if (job.schema !== 'autorig.animation-fitting-controlled-job-identity.v1' || job.status_string !== 'completed') {
        throw new Error('controlled job must be an immutable completed controlled-job identity record');
    }
    const stateFilename = path.basename(loaded.snapshot.path);
    const jobDirectory = path.dirname(loaded.snapshot.path);
    const jobsDirectory = path.dirname(jobDirectory);
    if (!/^\d{6}\.json$/.test(stateFilename) || path.basename(jobsDirectory).toLowerCase() !== 'jobs') {
        throw new Error('controlled job state path must be an immutable six-digit jobs revision');
    }
    const revisions = fs.readdirSync(jobDirectory, { withFileTypes: true })
        .filter((entry) => entry.isFile() && /^\d{6}\.json$/.test(entry.name))
        .map((entry) => entry.name).sort();
    if (!revisions.length || revisions.at(-1) !== stateFilename
        || integer(job.sequence_int, 'controlled job sequence', 1) !== Number(stateFilename.slice(0, 6))
        || !Number.isFinite(Number(job.recorded_at_unix_float))) {
        throw new Error('controlled job state is not the latest exact immutable revision');
    }
    const seedToken = rawJsonNumber(loaded.snapshot, 'seed_int', 'controlled job seed');
    if (!/^[0-9]+$/.test(seedToken)) throw new Error('controlled job seed must be a non-negative integer');
    const exactSeed = BigInt(seedToken);
    const startStrengthToken = rawJsonNumber(loaded.snapshot, 'start_guide_strength_float', 'controlled job start guide strength');
    const endStrengthToken = rawJsonNumber(loaded.snapshot, 'end_guide_strength_float', 'controlled job end guide strength');
    const intervalStrengthToken = rawJsonNumber(loaded.snapshot, 'strength_float', 'controlled job interval guide strength');
    if (job.experiment_id_string !== expected.experimentId || job.experiment_sha256_string !== expected.experimentSha256
        || job.runtime_authorization_string !== `explicit_cli:${expected.experimentId}`
        || exactSeed !== expected.seed || job.frame_count_int !== expected.frameCount
        || job.input_fps_int !== expected.inputFps || job.output_fps_int !== expected.outputFps
        || job.start_guide_strength_float !== 1 || job.end_guide_strength_float !== 1
        || job.workflow_name_string !== expected.workflowName
        || job.workflow_fingerprint_string !== expected.workflowFingerprintSha256
        || job.worker_id_string !== expected.workerId || job.worker_base_url_string !== expected.workerBaseUrl
        || job.reference_sha256_string !== canonical.referenceSha
        || job.approval_state_string !== 'generated_not_approved' || job.send_to_skeletal_fitting_bool !== false) {
        throw new Error('controlled job identity drift');
    }
    const resolution = object(job.resolution_override_object, 'controlled job resolution override');
    if (resolution.latent_width_int !== expected.width || resolution.latent_height_int !== expected.height
        || resolution.resize_longer_int !== expected.width) throw new Error('controlled job resolution drift');
    const interval = object(job.browser_interval_guide_object, 'controlled job interval guide');
    if (interval.guide_manifest_sha256_string !== guide.manifest.sha256
        || interval.video_sha256_string !== guide.video.sha256 || interval.video_bytes_int !== guide.video.bytes
        || interval.frame_count_int !== expected.frameCount || interval.width_int !== expected.width
        || interval.height_int !== expected.height || interval.fps_int !== expected.outputFps
        || interval.strength_float !== 1 || interval.ltxv_add_guide_count_int !== 1) {
        throw new Error('controlled job guide cross-pins drift');
    }
    const identityKeys = [
        'schema', 'experiment_id_string', 'experiment_sha256_string', 'runtime_authorization_string',
        'reference_sha256_string', 'positive_prompt_sha256_string', 'negative_prompt_sha256_string',
        'seed_int', 'frame_count_int', 'input_fps_int', 'output_fps_int', 'start_guide_strength_float',
        'end_guide_strength_float', 'worker_id_string', 'worker_base_url_string', 'workflow_name_string',
        'workflow_fingerprint_string', 'approval_state_string', 'send_to_skeletal_fitting_bool',
        'resolution_override_object', 'browser_interval_guide_object',
    ];
    const identity = Object.fromEntries(identityKeys.map((key) => {
        if (!(key in job)) throw new Error(`controlled job identity is missing ${key}`);
        return [key, job[key]];
    }));
    sha256(job.positive_prompt_sha256_string, 'controlled job positive prompt SHA-256');
    sha256(job.negative_prompt_sha256_string, 'controlled job negative prompt SHA-256');
    const canonicalIdentity = structuredClone(identity);
    canonicalIdentity.seed_int = '__SEED_NUMBER__';
    canonicalIdentity.start_guide_strength_float = '__START_STRENGTH_NUMBER__';
    canonicalIdentity.end_guide_strength_float = '__END_STRENGTH_NUMBER__';
    canonicalIdentity.browser_interval_guide_object.strength_float = '__INTERVAL_STRENGTH_NUMBER__';
    const identityJson = canonicalJson(canonicalIdentity)
        .replace('"__SEED_NUMBER__"', seedToken)
        .replace('"__START_STRENGTH_NUMBER__"', startStrengthToken)
        .replace('"__END_STRENGTH_NUMBER__"', endStrengthToken)
        .replace('"__INTERVAL_STRENGTH_NUMBER__"', intervalStrengthToken);
    const jobId = hash(Buffer.from(identityJson, 'utf8'));
    if (path.basename(jobDirectory) !== jobId) {
        throw new Error('controlled job path is not bound to its deterministic job identity');
    }
    if (jobId !== expected.jobId) throw new Error('controlled job deterministic identity is not the authorized V14 job');
    const promptId = nonEmptyString(job.prompt_id_string, 'controlled job prompt id');
    if (!UUID_RE.test(promptId) || promptId !== expectedPromptId(jobId) || promptId !== expected.promptId) {
        throw new Error('controlled job prompt id drift');
    }
    const candidate = readSnapshot(resolvePath(path.dirname(loaded.snapshot.path), job.raw_video_path_string, 'controlled job raw video path'), 'controlled job candidate');
    if (path.extname(candidate.path).toLowerCase() !== '.mp4'
        || candidate.sha256 !== sha256(job.raw_video_sha256_string, 'controlled job raw video SHA-256')
        || candidate.bytes !== integer(job.raw_video_bytes_int, 'controlled job raw video bytes', 1)) {
        throw new Error('controlled job candidate drift');
    }
    const artifactRoot = path.dirname(jobsDirectory);
    const expectedCandidatePath = path.join(
        artifactRoot, 'raw', candidate.sha256.slice(0, 2), `${candidate.sha256}.mp4`,
    );
    if (!samePath(candidate.path, expectedCandidatePath)) {
        throw new Error('controlled job candidate is not stored at its exact content-addressed path');
    }
    if (isInside(outputRoot, candidate.path)) throw new Error('controlled job candidate must be outside outputRoot');
    snapshots.push(candidate);
    const framePaths = Array.isArray(job.frame_paths_array) ? job.frame_paths_array : [];
    const frameHashes = Array.isArray(job.frame_sha256_array) ? job.frame_sha256_array : [];
    if (framePaths.length !== expected.frameCount || frameHashes.length !== expected.frameCount) {
        throw new Error('controlled job frame inventory drift');
    }
    const frames = [];
    framePaths.forEach((filename, index) => {
        const snapshot = readSnapshot(resolvePath(path.dirname(loaded.snapshot.path), filename, `controlled job frame ${index}`), `controlled job frame ${index}`);
        if (snapshot.sha256 !== sha256(frameHashes[index], `controlled job frame ${index} SHA-256`)
            || !samePath(snapshot.path, path.join(
                artifactRoot, 'frames', candidate.sha256,
                `frame_${String(index).padStart(6, '0')}.png`,
            ))) {
            throw new Error(`controlled job frame ${index} drift`);
        }
        snapshots.push(snapshot);
        frames.push({ frameIndex: index, ...descriptor(snapshot) });
    });
    if (!job.backend_output_object || !String(job.backend_output_object.filename_string || '').toLowerCase().endsWith('.mp4')
        || !nonEmptyString(job.backend_output_object.subfolder_string, 'controlled job backend subfolder')
        || !nonEmptyString(job.backend_output_object.type_string, 'controlled job backend output type')) {
        throw new Error('controlled job backend MP4 output provenance is missing');
    }
    return { candidate, job: loaded.snapshot, jobId, promptId, frames };
}

function revalidateSnapshots(snapshots) {
    const unique = new Map(snapshots.map((snapshot) => [path.resolve(snapshot.path), snapshot]));
    for (const original of unique.values()) {
        const current = readSnapshot(original.path, `immutable input ${original.path}`);
        if (current.bytes !== original.bytes || current.sha256 !== original.sha256) {
            throw new Error(`immutable input drift before publication: ${original.path}`);
        }
    }
}

function optionalSha(value, field) {
    return value == null ? null : sha256(value, field);
}

export function buildV14PipelineSpec(configValue, dependencies = {}) {
    const config = object(configValue, 'config');
    const expected = dependencies.expectedContract || REAL_V14_CONTRACT;
    if (RUNNER_V14_PIPELINE_SPEC_SCHEMA !== V14_PIPELINE_SPEC_SCHEMA_V2) {
        throw new Error(`runner schema drift: expected ${V14_PIPELINE_SPEC_SCHEMA_V2}`);
    }
    const outputRoot = path.resolve(nonEmptyString(config.outputRoot, 'outputRoot'));
    const snapshots = [];
    const canonical = validateCanonicalBundle(config, expected, snapshots);
    const guide = validateGuide(config, expected, canonical, snapshots);
    const runtime = validateRuntime(config, expected, snapshots);
    const tools = validateToolSources(config, snapshots);
    if (config.controlledJob == null || config.controlledJobSha256 == null) {
        throw new Error('V14 v2 requires a completed externally pinned controlled job state');
    }
    if (config.candidate != null || config.candidateSha256 != null || config.candidateBytes != null) {
        throw new Error('V14 v2 rejects direct candidate mode; controlled generation provenance is mandatory');
    }
    const candidateSource = validateControlledJob(config, expected, canonical, guide, outputRoot, snapshots);
    const spec = {
        schema: V14_PIPELINE_SPEC_SCHEMA_V2,
        browserOnly: true,
        blenderUsed: false,
        orchestratorExecutesSubprocesses: false,
        semanticId: 'walk_forward',
        clipName: config.clipName == null
            ? 'Horse_Walk_LTX_V14_Browser_Contact_Refit'
            : nonEmptyString(config.clipName, 'clipName'),
        outputRoot,
        candidate: descriptor(candidateSource.candidate),
        controlledGeneration: {
            schema: V14_CONTROLLED_GENERATION_SCHEMA,
            jobId: candidateSource.jobId,
            promptId: candidateSource.promptId,
            experimentId: expected.experimentId,
            experimentSha256: expected.experimentSha256,
            workflowFingerprint: expected.workflowFingerprintSha256,
            state: descriptor(candidateSource.job),
            candidate: descriptor(candidateSource.candidate),
            frames: candidateSource.frames,
        },
        guide: {
            bundleDirectory: guide.root,
            immutableManifestSha256: guide.manifest.sha256,
            endpointGuide: descriptor(guide.endpoint),
        },
        canonicalBundle: {
            directory: canonical.root,
            immutableManifestSha256: canonical.immutable.sha256,
            fittingBundleSha256: canonical.fitting.sha256,
            sourceModelSha256: canonical.sourceSha,
        },
        runtime: runtime.value,
        toolSources: tools.sources,
        externalPins: {
            contactRefitInputManifestSha256: optionalSha(config.contactRefitInputManifestSha256, 'contact refit input manifest SHA-256'),
            threeClipSha256: optionalSha(config.threeClipSha256, 'Three clip SHA-256'),
        },
        authoringProvenance: {
            schema: V14_SPEC_AUTHOR_PROVENANCE_SCHEMA,
            inputMode: 'completed_controlled_job',
            controlledJob: descriptor(candidateSource.job),
            controlledJobId: candidateSource.jobId,
            controlledPromptId: candidateSource.promptId,
            guideIntervalVideo: descriptor(guide.video),
            runtimePinManifest: descriptor(runtime.pinManifest),
            toolSourcePinManifest: descriptor(tools.pinManifest),
            noGpuJobOrStageExecution: true,
            blenderUsed: false,
            databaseUsed: false,
            networkUsed: false,
        },
    };
    return { spec, snapshots };
}

function publishExclusive(filenameValue, payload, validateStaging) {
    const filename = path.resolve(filenameValue);
    const parent = path.dirname(filename);
    if (!fs.existsSync(parent) || !fs.statSync(parent).isDirectory()) throw new Error(`output parent must exist: ${parent}`);
    if (fs.existsSync(filename)) throw new Error(`output already exists: ${filename}`);
    const staging = `${filename}.staging-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
    const handle = fs.openSync(staging, 'wx');
    try {
        fs.writeFileSync(handle, payload);
        fs.fsyncSync(handle);
    } finally {
        fs.closeSync(handle);
    }
    try {
        validateStaging(staging);
        fs.linkSync(staging, filename);
        fs.unlinkSync(staging);
    } catch (error) {
        try { if (fs.existsSync(staging)) fs.unlinkSync(staging); } catch { /* keep original error */ }
        throw error;
    }
    return { path: filename, bytes: payload.length, sha256: hash(payload) };
}

export function authorV14PipelineSpec(config, dependencies = {}) {
    const built = buildV14PipelineSpec(config, dependencies);
    const payload = Buffer.from(`${JSON.stringify(built.spec, null, 2)}\n`, 'utf8');
    const payloadSha256 = hash(payload);
    let initialState;
    const pin = publishExclusive(config.output, payload, (staging) => {
        initialState = (dependencies.inspectPipeline || inspectV14Pipeline)({
            specPath: staging,
            expectedSpecSha256: payloadSha256,
        });
        revalidateSnapshots(built.snapshots);
    });
    return { spec: pin, initialPipelineStatus: initialState.status, nextStage: initialState.next?.stage || null };
}

export function parseAuthorArgs(argv) {
    const values = {};
    let help = false;
    const flags = new Set([
        '--controlled-job', '--controlled-job-sha256',
        '--canonical-bundle', '--canonical-immutable-sha256', '--fitting-bundle-sha256', '--source-model-sha256',
        '--guide-bundle', '--guide-manifest-sha256', '--guide-video', '--guide-video-sha256', '--guide-video-bytes',
        '--runtime-pins', '--runtime-pins-sha256', '--tool-source-pins', '--tool-source-pins-sha256',
        '--output-root', '--output', '--clip-name', '--contact-refit-input-manifest-sha256', '--three-clip-sha256',
    ]);
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag === '--help' || flag === '-h') { help = true; continue; }
        if (!flags.has(flag)) throw new Error(`unknown option ${flag}`);
        if (values[flag] != null) throw new Error(`duplicate option ${flag}`);
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
        values[flag] = argv[++index];
    }
    if (help) return { help: true };
    const required = [
        '--controlled-job', '--controlled-job-sha256',
        '--canonical-bundle', '--canonical-immutable-sha256', '--fitting-bundle-sha256', '--source-model-sha256',
        '--guide-bundle', '--guide-manifest-sha256', '--guide-video', '--guide-video-sha256', '--guide-video-bytes',
        '--runtime-pins', '--runtime-pins-sha256', '--tool-source-pins', '--tool-source-pins-sha256', '--output-root', '--output',
    ];
    required.forEach((flag) => { if (values[flag] == null) throw new Error(`${flag} is required`); });
    const key = (flag) => flag.slice(2).replaceAll(/-([a-z])/g, (_match, letter) => letter.toUpperCase());
    return Object.fromEntries(Object.entries(values).map(([flag, value]) => [key(flag), value]));
}

function helpText() {
    return `Usage:
  node author_v14_browser_fitting_spec.mjs \\
    --controlled-job FILE --controlled-job-sha256 SHA256 \\
    --canonical-bundle DIR --canonical-immutable-sha256 SHA256 \\
    --fitting-bundle-sha256 SHA256 --source-model-sha256 SHA256 \\
    --guide-bundle DIR --guide-manifest-sha256 SHA256 \\
    --guide-video FILE --guide-video-sha256 SHA256 --guide-video-bytes N \\
    --runtime-pins FILE --runtime-pins-sha256 SHA256 \\
    --tool-source-pins FILE --tool-source-pins-sha256 SHA256 \\
    --output-root DIR --output NEW_FILE

The runtime manifest (${V14_RUNTIME_PINS_SCHEMA}) pins executables python/node/
chrome/ffmpeg/ffprobe, threeModule revision 160, trackingRuntimeRoot, and
trackingRuntimeLock.  The tool manifest (${V14_TOOL_SOURCE_PINS_SCHEMA}) pins
the exact 28 names exported by run_v14_browser_fitting_pipeline.mjs.
V14 v2 deliberately rejects a bare MP4: the completed controlled-generation
state and its 49 ordered frame pins are mandatory.  This author performs local
immutable validation only and never executes a job,
pipeline stage, subprocess, GPU operation, Blender, database, or network call.`;
}

export function runAuthorCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseAuthorArgs(argv);
        if (config.help) { streams.stdout.write(`${helpText()}\n`); return 0; }
        const result = authorV14PipelineSpec(config);
        streams.stdout.write(`${JSON.stringify({ status: 'AUTHORED', ...result })}\n`);
        return 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = runAuthorCli();

#!/usr/bin/env node
/**
 * Fail-closed unattended stage driver for the immutable Horse_2 V14 browser
 * fitting pipeline.
 *
 * The runner remains the sole command author.  This driver only consumes the
 * runner's structured state.next.command, verifies its immutable
 * preconditions, and launches a small explicit CPU/browser allowlist without
 * a shell.  CUDA tracking is opt-in.  LTX generation, Blender, production,
 * database, and network orchestration are not expressible by this driver.
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
    authorV14PipelineSpec,
    buildV14PipelineSpec,
} from './author_v14_browser_fitting_spec.mjs';
import {
    V14_PIPELINE_STATE_SCHEMA,
    inspectV14Pipeline,
} from './run_v14_browser_fitting_pipeline.mjs';

export const V14_DRIVER_PIN_TRANSITION_SCHEMA = 'autorig.v14-browser-fitting-driver-pin-transition.v1';
export const V14_DRIVER_STAGE_RECEIPT_SCHEMA = 'autorig.v14-browser-fitting-driver-stage-receipt.v1';

const SHA256_RE = /^[0-9a-f]{64}$/;
const TOOLS_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const PYTHON_WORKING_DIRECTORY = path.dirname(TOOLS_DIRECTORY);
const DRIVER_PATH = fileURLToPath(import.meta.url);
const SPEC_AUTHOR_PATH = path.join(TOOLS_DIRECTORY, 'author_v14_browser_fitting_spec.mjs');
const MAX_TRANSITIONS = 16;

const STAGE_CONTRACTS = Object.freeze({
    object_region_gate: Object.freeze({
        runtime: 'python', cwd: PYTHON_WORKING_DIRECTORY,
        prefix: ['-m', 'animation_fitting.object_region_video_gate'], cuda: false,
    }),
    tapnext_sam2_observations: Object.freeze({
        runtime: 'python', cwd: PYTHON_WORKING_DIRECTORY,
        prefix: ['-m', 'animation_fitting.tracking_runtime'], cuda: true,
    }),
    initial_browser_fit: Object.freeze({
        runtime: 'node', cwd: TOOLS_DIRECTORY,
        script: 'browser_fit_canary.mjs', cuda: false,
    }),
    hoof_contact_diagnostic: Object.freeze({
        runtime: 'node', cwd: TOOLS_DIRECTORY,
        script: 'diagnose_browser_hoof_contacts.mjs', cuda: false,
    }),
    contact_refit_manifest: Object.freeze({
        runtime: 'node', cwd: TOOLS_DIRECTORY,
        script: 'author_browser_contact_refit_manifest.mjs', cuda: false,
    }),
    browser_contact_refit: Object.freeze({
        runtime: 'node', cwd: TOOLS_DIRECTORY,
        script: 'browser_contact_refit.mjs', cuda: false,
    }),
    browser_visual_phase_qa: Object.freeze({
        runtime: 'node', cwd: TOOLS_DIRECTORY,
        script: 'browser_horse_visual_phase_qa.mjs', cuda: false,
    }),
});

const PIN_BARRIERS = Object.freeze({
    'externalPins.contactRefitInputManifestSha256': Object.freeze({
        key: 'contact-refit-input-manifest',
        relativeArtifact: '05-contact-refit-input.json',
        expectedStatus: 'AWAITING_EXTERNAL_CONTACT_MANIFEST_PIN',
    }),
    'externalPins.threeClipSha256': Object.freeze({
        key: 'three-clip',
        relativeArtifact: path.join('06-browser-contact-refit', 'three-clip.json'),
        expectedStatus: 'AWAITING_EXTERNAL_THREE_CLIP_PIN',
    }),
});

function object(value, field) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${field} must be an object`);
    return value;
}

function string(value, field) {
    if (typeof value !== 'string' || !value.trim() || value.includes('\0')) {
        throw new Error(`${field} must be a non-empty NUL-free string`);
    }
    return value.trim();
}

function sha256(value, field) {
    const result = string(value, field);
    if (!SHA256_RE.test(result)) throw new Error(`${field} must be a lowercase SHA-256`);
    return result;
}

function integer(value, field, minimum = 0) {
    const result = Number(value);
    if (!Number.isSafeInteger(result) || result < minimum) throw new Error(`${field} must be a safe integer >= ${minimum}`);
    return result;
}

function digest(buffer) {
    return crypto.createHash('sha256').update(buffer).digest('hex');
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

function samePath(left, right) {
    const a = path.resolve(left);
    const b = path.resolve(right);
    return process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
}

function isInside(parent, child) {
    const relative = path.relative(path.resolve(parent), path.resolve(child));
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
}

function exactKeys(value, keys, field) {
    const actual = Object.keys(object(value, field)).sort();
    const expected = [...keys].sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`${field} must contain exactly ${expected.join(', ')}`);
    }
}

function readSnapshot(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, `${field}.path`));
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
    return { path: filename, bytes: buffer.length, sha256: digest(buffer), buffer };
}

function descriptor(snapshot) {
    return { path: snapshot.path, bytes: snapshot.bytes, sha256: snapshot.sha256 };
}

function readPinnedJson(filename, expectedSha256, field) {
    const snapshot = readSnapshot(filename, field);
    if (snapshot.sha256 !== sha256(expectedSha256, `${field} expected SHA-256`)) {
        throw new Error(`${field} SHA-256 mismatch`);
    }
    let value;
    try { value = object(JSON.parse(snapshot.buffer.toString('utf8')), field); } catch (error) {
        if (error.message.startsWith(`${field} must be`)) throw error;
        throw new Error(`${field} is invalid JSON: ${error.message}`);
    }
    return { snapshot, value };
}

function requireExistingDirectory(filenameValue, field) {
    const filename = path.resolve(string(filenameValue, field));
    if (!fs.existsSync(filename) || !fs.statSync(filename).isDirectory()) {
        throw new Error(`${field} must be an existing directory: ${filename}`);
    }
    return filename;
}

function publishContentAddressed(directory, kind, value) {
    const payload = Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
    const payloadSha256 = digest(payload);
    const filename = path.join(directory, `${kind}-${payloadSha256}.json`);
    if (fs.existsSync(filename)) {
        const existing = readSnapshot(filename, `${kind} journal entry`);
        if (existing.bytes !== payload.length || !existing.buffer.equals(payload)) {
            throw new Error(`${kind} journal content-address collision`);
        }
        return descriptor(existing);
    }
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
        try { if (fs.existsSync(staging)) fs.unlinkSync(staging); } catch { /* preserve original */ }
        if (fs.existsSync(filename)) {
            const existing = readSnapshot(filename, `${kind} journal entry`);
            if (existing.bytes === payload.length && existing.buffer.equals(payload)) return descriptor(existing);
        }
        throw error;
    }
    return { path: filename, bytes: payload.length, sha256: payloadSha256 };
}

function assertDescriptor(pinValue, snapshot, field) {
    const pin = object(pinValue, field);
    exactKeys(pin, ['path', 'bytes', 'sha256'], field);
    if (!samePath(pin.path, snapshot.path)
        || integer(pin.bytes, `${field}.bytes`, 1) !== snapshot.bytes
        || sha256(pin.sha256, `${field}.sha256`) !== snapshot.sha256) {
        throw new Error(`${field} no longer matches its immutable file`);
    }
}

function stateSignature(state) {
    return digest(Buffer.from(canonicalJson({
        status: state.status,
        nextStage: state.next?.stage || null,
        pinField: state.pinRequest?.field || null,
        pinSha256: state.pinRequest?.observedSha256NotTrusted || null,
        completedStages: state.completedStages || [],
    }), 'utf8'));
}

function validatePipelineState(stateValue) {
    const state = object(stateValue, 'pipeline state');
    if (state.schema !== V14_PIPELINE_STATE_SCHEMA || state.browserOnly !== true
        || state.blenderUsed !== false || state.orchestratorExecutesSubprocesses !== false) {
        throw new Error('runner returned a non-V14 or unsafe pipeline state');
    }
    string(state.status, 'pipeline state.status');
    if (state.next != null && state.pinRequest != null) throw new Error('pipeline state cannot have both next and pinRequest');
    return state;
}

export function validateV14StageCommand(stateValue, specValue, { allowCudaTracking = false } = {}) {
    const state = validatePipelineState(stateValue);
    const next = object(state.next, 'pipeline state.next');
    exactKeys(next, ['stage', 'command'], 'pipeline state.next');
    const stage = string(next.stage, 'pipeline state.next.stage');
    const contract = STAGE_CONTRACTS[stage];
    if (!contract) throw new Error(`stage is not in the V14 CPU/browser allowlist: ${stage}`);
    if (contract.cuda && !allowCudaTracking) {
        return { allowed: false, reason: 'CUDA_TRACKING_REQUIRES_EXPLICIT_ALLOW', stage, contract };
    }

    const spec = object(specValue, 'pipeline spec');
    const runtime = object(spec.runtime, 'pipeline spec.runtime');
    const executables = object(runtime.executables, 'pipeline spec.runtime.executables');
    const executablePin = object(executables[contract.runtime], `pipeline spec.runtime.executables.${contract.runtime}`);
    const command = object(next.command, 'pipeline state.next.command');
    exactKeys(command, ['cwd', 'argv', 'preconditions', 'powershell'], 'pipeline state.next.command');
    const argv = Array.isArray(command.argv) ? command.argv.map((value, index) => string(value, `command.argv[${index}]`)) : null;
    if (!argv || argv.length < 3) throw new Error('pipeline state.next.command.argv is too short');
    if (!samePath(command.cwd, contract.cwd)) throw new Error(`${stage} command cwd drift`);
    if (!samePath(argv[0], executablePin.path)) throw new Error(`${stage} command executable is not the pinned ${contract.runtime}`);
    if (contract.prefix) {
        contract.prefix.forEach((value, index) => {
            if (argv[index + 1] !== value) throw new Error(`${stage} command module prefix drift`);
        });
    } else {
        const expectedScript = path.join(TOOLS_DIRECTORY, contract.script);
        if (!samePath(argv[1], expectedScript)) throw new Error(`${stage} command script drift`);
    }
    if (contract.cuda) {
        const deviceIndex = argv.indexOf('--device');
        if (deviceIndex < 0 || argv[deviceIndex + 1] !== 'cuda') throw new Error('tracking command is not the exact CUDA contract');
    }
    if (typeof command.powershell !== 'string') throw new Error('pipeline state.next.command.powershell must be a string');
    const preconditions = Array.isArray(command.preconditions) ? command.preconditions : null;
    if (!preconditions?.length) throw new Error('pipeline state.next.command.preconditions must be non-empty');
    const preconditionPaths = new Set();
    preconditions.forEach((pin, index) => {
        exactKeys(pin, ['path', 'bytes', 'sha256'], `command.preconditions[${index}]`);
        const key = path.resolve(string(pin.path, `command.preconditions[${index}].path`)).toLowerCase();
        if (preconditionPaths.has(key)) throw new Error(`command preconditions repeat ${pin.path}`);
        preconditionPaths.add(key);
    });
    const executableSnapshot = readSnapshot(executablePin.path, `pinned ${contract.runtime} executable`);
    assertDescriptor(executablePin, executableSnapshot, `pipeline spec.runtime.executables.${contract.runtime}`);
    if (!preconditions.some((pin) => (
        samePath(pin.path, executableSnapshot.path)
        && pin.bytes === executableSnapshot.bytes
        && pin.sha256 === executableSnapshot.sha256
    ))) {
        throw new Error(`${stage} command does not carry the exact executable precondition`);
    }
    return { allowed: true, stage, command, argv, preconditions, contract };
}

export function executeV14StageCommand(state, spec, options = {}, dependencies = {}) {
    const validated = validateV14StageCommand(state, spec, options);
    if (!validated.allowed) return validated;
    for (const [index, pin] of validated.preconditions.entries()) {
        const snapshot = readSnapshot(pin.path, `command precondition ${index}`);
        assertDescriptor(pin, snapshot, `command precondition ${index}`);
    }
    const run = dependencies.spawnSync || spawnSync;
    const result = run(validated.argv[0], validated.argv.slice(1), {
        cwd: path.resolve(validated.command.cwd),
        shell: false,
        stdio: 'inherit',
        windowsHide: true,
    });
    if (result?.error) throw new Error(`${validated.stage} launch failed: ${result.error.message}`);
    if (result?.signal) throw new Error(`${validated.stage} terminated by signal ${result.signal}`);
    if (!Number.isInteger(result?.status) || result.status !== 0) {
        throw new Error(`${validated.stage} failed with exit code ${String(result?.status)}`);
    }
    return { allowed: true, stage: validated.stage, exitCode: 0 };
}

function descriptorFromSpec(value, field) {
    const pin = object(value, field);
    exactKeys(pin, ['path', 'bytes', 'sha256'], field);
    return {
        path: path.resolve(string(pin.path, `${field}.path`)),
        bytes: integer(pin.bytes, `${field}.bytes`, 1),
        sha256: sha256(pin.sha256, `${field}.sha256`),
    };
}

export function deriveV14RevisionConfig(specValue, pinField, observedSha256, output) {
    const spec = object(specValue, 'pipeline spec');
    const provenance = object(spec.authoringProvenance, 'pipeline spec.authoringProvenance');
    const controlled = object(spec.controlledGeneration, 'pipeline spec.controlledGeneration');
    const guide = object(spec.guide, 'pipeline spec.guide');
    const canonical = object(spec.canonicalBundle, 'pipeline spec.canonicalBundle');
    const runtime = object(spec.runtime, 'pipeline spec.runtime');
    const toolSources = object(spec.toolSources, 'pipeline spec.toolSources');
    const externalPins = object(spec.externalPins, 'pipeline spec.externalPins');
    const barrier = PIN_BARRIERS[pinField];
    if (!barrier) throw new Error(`unsupported external pin field ${pinField}`);
    const pinnedSha256 = sha256(observedSha256, 'observed barrier SHA-256');
    if (pinField === 'externalPins.contactRefitInputManifestSha256') {
        if (externalPins.contactRefitInputManifestSha256 != null) throw new Error('contact-refit input pin is already set');
        if (externalPins.threeClipSha256 != null) throw new Error('Three clip cannot be pinned before the contact-refit input');
    } else {
        sha256(externalPins.contactRefitInputManifestSha256, 'existing contact-refit input pin');
        if (externalPins.threeClipSha256 != null) throw new Error('Three clip pin is already set');
    }
    const job = descriptorFromSpec(controlled.state, 'pipeline spec.controlledGeneration.state');
    const interval = descriptorFromSpec(provenance.guideIntervalVideo, 'pipeline spec.authoringProvenance.guideIntervalVideo');
    const runtimePins = descriptorFromSpec(provenance.runtimePinManifest, 'pipeline spec.authoringProvenance.runtimePinManifest');
    const toolPins = descriptorFromSpec(provenance.toolSourcePinManifest, 'pipeline spec.authoringProvenance.toolSourcePinManifest');
    if (Object.keys(toolSources).length !== 28) throw new Error('pipeline spec tool-source closure drift');
    return {
        controlledJob: job.path,
        controlledJobSha256: job.sha256,
        canonicalBundle: path.resolve(string(canonical.directory, 'pipeline spec.canonicalBundle.directory')),
        canonicalImmutableSha256: sha256(canonical.immutableManifestSha256, 'canonical immutable manifest SHA-256'),
        fittingBundleSha256: sha256(canonical.fittingBundleSha256, 'fitting bundle SHA-256'),
        sourceModelSha256: sha256(canonical.sourceModelSha256, 'source model SHA-256'),
        guideBundle: path.resolve(string(guide.bundleDirectory, 'pipeline spec.guide.bundleDirectory')),
        guideManifestSha256: sha256(guide.immutableManifestSha256, 'guide manifest SHA-256'),
        guideVideo: interval.path,
        guideVideoSha256: interval.sha256,
        guideVideoBytes: interval.bytes,
        runtimePins: runtimePins.path,
        runtimePinsSha256: runtimePins.sha256,
        toolSourcePins: toolPins.path,
        toolSourcePinsSha256: toolPins.sha256,
        outputRoot: path.resolve(string(spec.outputRoot, 'pipeline spec.outputRoot')),
        output: path.resolve(string(output, 'revision output')),
        clipName: string(spec.clipName, 'pipeline spec.clipName'),
        contactRefitInputManifestSha256: pinField === 'externalPins.contactRefitInputManifestSha256'
            ? pinnedSha256 : sha256(externalPins.contactRefitInputManifestSha256, 'existing contact-refit input pin'),
        threeClipSha256: pinField === 'externalPins.threeClipSha256' ? pinnedSha256 : null,
    };
}

function expectedSpecPayload(config, buildPipelineSpec) {
    const built = buildPipelineSpec(config);
    return Buffer.from(`${JSON.stringify(built.spec, null, 2)}\n`, 'utf8');
}

function ensurePinnedRevision({
    currentSpec, currentSpecSnapshot, state, statePin, revisionDirectory, journalDirectory,
}, dependencies = {}) {
    const pinRequest = object(state.pinRequest, 'pipeline state.pinRequest');
    exactKeys(pinRequest, ['field', 'observedSha256NotTrusted', 'observedBytes', 'instruction'], 'pipeline state.pinRequest');
    const field = string(pinRequest.field, 'pipeline state.pinRequest.field');
    const barrier = PIN_BARRIERS[field];
    if (!barrier || state.status !== barrier.expectedStatus) throw new Error('pipeline pin request/status is not an allowed V14 barrier');
    const observedSha256 = sha256(pinRequest.observedSha256NotTrusted, 'pipeline pin request observed SHA-256');
    const observedBytes = integer(pinRequest.observedBytes, 'pipeline pin request observed bytes', 1);
    const outputRoot = path.resolve(string(currentSpec.outputRoot, 'pipeline spec.outputRoot'));
    const artifactPath = path.join(outputRoot, barrier.relativeArtifact);
    const artifact = readSnapshot(artifactPath, `${barrier.key} barrier artifact`);
    if (artifact.sha256 !== observedSha256 || artifact.bytes !== observedBytes) {
        throw new Error(`${barrier.key} barrier artifact does not match the runner pin request`);
    }
    const revisionIdentity = digest(Buffer.from(canonicalJson({
        parentSpecSha256: currentSpecSnapshot.sha256,
        field,
        observedSha256,
        observedBytes,
    }), 'utf8'));
    const output = path.join(revisionDirectory, `v14-spec-revision-${revisionIdentity}.json`);
    const config = deriveV14RevisionConfig(currentSpec, field, observedSha256, output);
    const buildPipelineSpec = dependencies.buildPipelineSpec || buildV14PipelineSpec;
    const authorPipelineSpec = dependencies.authorPipelineSpec || authorV14PipelineSpec;
    const expectedPayload = expectedSpecPayload(config, buildPipelineSpec);
    if (!fs.existsSync(output)) {
        try { authorPipelineSpec(config); } catch (error) {
            if (!fs.existsSync(output)) throw error;
        }
    }
    const nextSpecSnapshot = readSnapshot(output, 'authored V14 spec revision');
    if (nextSpecSnapshot.bytes !== expectedPayload.length || !nextSpecSnapshot.buffer.equals(expectedPayload)) {
        throw new Error('existing V14 spec revision does not match the deterministic author output');
    }
    const nextSpec = object(JSON.parse(nextSpecSnapshot.buffer.toString('utf8')), 'authored V14 spec revision');
    const before = object(currentSpec.externalPins, 'pipeline spec.externalPins');
    const after = object(nextSpec.externalPins, 'authored V14 spec revision.externalPins');
    const authorTool = readSnapshot(SPEC_AUTHOR_PATH, 'V14 spec author tool');
    const driverTool = readSnapshot(DRIVER_PATH, 'V14 stage driver tool');
    const transition = {
        schema: V14_DRIVER_PIN_TRANSITION_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        ltxGenerationExecuted: false,
        cudaTrackingExecuted: false,
        productionOrDatabaseUsed: false,
        parentSpec: descriptor(currentSpecSnapshot),
        runnerState: statePin,
        pipelineStatus: state.status,
        pinRequest: {
            field,
            observedSha256NotTrusted: observedSha256,
            observedBytes,
            instruction: pinRequest.instruction,
        },
        verifiedStageArtifact: descriptor(artifact),
        trustPromotionPolicy: 'read_once_exact_stage_path_then_full_spec_author_and_runner_revalidation',
        externalPinsBefore: {
            contactRefitInputManifestSha256: before.contactRefitInputManifestSha256 ?? null,
            threeClipSha256: before.threeClipSha256 ?? null,
        },
        externalPinsAfter: {
            contactRefitInputManifestSha256: after.contactRefitInputManifestSha256 ?? null,
            threeClipSha256: after.threeClipSha256 ?? null,
        },
        nextSpec: descriptor(nextSpecSnapshot),
        tools: { driver: descriptor(driverTool), specAuthor: descriptor(authorTool) },
    };
    const receipt = publishContentAddressed(journalDirectory, 'pin-transition', transition);
    return { nextSpec, nextSpecSnapshot, receipt };
}

function acceptedTerminalStatus(status) {
    return status === 'PASS_MACHINE_QA_AWAITING_HUMAN_REVIEW' || status.startsWith('FAILED_');
}

export function driveV14Pipeline(configValue, dependencies = {}) {
    const config = object(configValue, 'driver config');
    const revisionDirectory = requireExistingDirectory(config.revisionDirectory, 'revisionDirectory');
    const journalDirectory = requireExistingDirectory(config.journalDirectory, 'journalDirectory');
    const allowCudaTracking = config.allowCudaTracking === true;
    const inspectPipeline = dependencies.inspectPipeline || inspectV14Pipeline;
    const executeStage = dependencies.executeStage || ((state, spec) => executeV14StageCommand(
        state, spec, { allowCudaTracking }, dependencies,
    ));

    let loaded = readPinnedJson(config.specPath, config.expectedSpecSha256, 'initial pipeline spec');
    const outputRoot = path.resolve(string(loaded.value.outputRoot, 'pipeline spec.outputRoot'));
    if (isInside(outputRoot, revisionDirectory) || isInside(outputRoot, journalDirectory)) {
        throw new Error('revision/journal directories must be outside pipeline outputRoot');
    }
    let state = validatePipelineState(inspectPipeline({
        specPath: loaded.snapshot.path,
        expectedSpecSha256: loaded.snapshot.sha256,
    }));
    const executedStages = [];
    const authoredRevisions = [];

    for (let transitionCount = 0; transitionCount < MAX_TRANSITIONS; transitionCount += 1) {
        const statePin = publishContentAddressed(journalDirectory, 'runner-state', state);
        if (state.next != null) {
            const validation = validateV14StageCommand(state, loaded.value, { allowCudaTracking });
            if (!validation.allowed) {
                return {
                    status: 'PAUSED_CUDA_TRACKING_REQUIRES_EXPLICIT_ALLOW',
                    pipelineStatus: state.status,
                    nextStage: validation.stage,
                    currentSpec: descriptor(loaded.snapshot),
                    state: statePin,
                    executedStages,
                    authoredRevisions,
                };
            }
            const result = executeStage(state, loaded.value);
            if (!result?.allowed || result.stage !== validation.stage || result.exitCode !== 0) {
                throw new Error(`${validation.stage} executor returned an invalid success result`);
            }
            const postState = validatePipelineState(inspectPipeline({
                specPath: loaded.snapshot.path,
                expectedSpecSha256: loaded.snapshot.sha256,
            }));
            if (stateSignature(postState) === stateSignature(state)) {
                throw new Error(`${validation.stage} exited successfully but produced no runner-visible state transition`);
            }
            const postStatePin = publishContentAddressed(journalDirectory, 'runner-state', postState);
            const receiptValue = {
                schema: V14_DRIVER_STAGE_RECEIPT_SCHEMA,
                browserOnly: true,
                blenderUsed: false,
                ltxGenerationExecuted: false,
                productionOrDatabaseUsed: false,
                cudaTrackingExecuted: validation.contract.cuda,
                spec: descriptor(loaded.snapshot),
                stage: validation.stage,
                commandArgvSha256: digest(Buffer.from(canonicalJson(validation.argv), 'utf8')),
                preState: statePin,
                postState: postStatePin,
                exitCode: 0,
            };
            const receipt = publishContentAddressed(journalDirectory, 'stage-receipt', receiptValue);
            executedStages.push({ stage: validation.stage, receipt });
            state = postState;
            continue;
        }
        if (state.pinRequest != null) {
            const revision = ensurePinnedRevision({
                currentSpec: loaded.value,
                currentSpecSnapshot: loaded.snapshot,
                state,
                statePin,
                revisionDirectory,
                journalDirectory,
            }, dependencies);
            const nextState = validatePipelineState(inspectPipeline({
                specPath: revision.nextSpecSnapshot.path,
                expectedSpecSha256: revision.nextSpecSnapshot.sha256,
            }));
            if (stateSignature(nextState) === stateSignature(state)) {
                throw new Error('authored pin revision produced no runner-visible state transition');
            }
            authoredRevisions.push({ spec: descriptor(revision.nextSpecSnapshot), receipt: revision.receipt });
            loaded = { snapshot: revision.nextSpecSnapshot, value: revision.nextSpec };
            state = nextState;
            continue;
        }
        if (!acceptedTerminalStatus(state.status)) throw new Error(`unexpected non-actionable pipeline status ${state.status}`);
        return {
            status: state.status,
            currentSpec: descriptor(loaded.snapshot),
            state: statePin,
            executedStages,
            authoredRevisions,
        };
    }
    throw new Error(`V14 driver exceeded its hard limit of ${MAX_TRANSITIONS} transitions`);
}

export function parseDriverArgs(argv) {
    const values = {};
    let help = false;
    let allowCudaTracking = false;
    const valueFlags = new Set(['--spec', '--spec-sha256', '--revision-dir', '--journal-dir']);
    for (let index = 0; index < argv.length; index += 1) {
        const flag = argv[index];
        if (flag === '--help' || flag === '-h') { help = true; continue; }
        if (flag === '--allow-cuda-tracking') {
            if (allowCudaTracking) throw new Error('duplicate option --allow-cuda-tracking');
            allowCudaTracking = true;
            continue;
        }
        if (!valueFlags.has(flag)) throw new Error(`unknown option ${flag}`);
        if (values[flag] != null) throw new Error(`duplicate option ${flag}`);
        if (index + 1 >= argv.length || argv[index + 1].startsWith('--')) throw new Error(`${flag} requires a value`);
        values[flag] = argv[++index];
    }
    if (help) return { help: true };
    valueFlags.forEach((flag) => { if (values[flag] == null) throw new Error(`${flag} is required`); });
    return {
        specPath: values['--spec'],
        expectedSpecSha256: values['--spec-sha256'],
        revisionDirectory: values['--revision-dir'],
        journalDirectory: values['--journal-dir'],
        allowCudaTracking,
    };
}

function helpText() {
    return `Usage:
  node drive_v14_browser_fitting_pipeline.mjs \\
    --spec FILE --spec-sha256 SHA256 \\
    --revision-dir EXISTING_DIR --journal-dir EXISTING_DIR \\
    [--allow-cuda-tracking]

Runs only exact runner-authored CPU/browser stage argv without a shell.  The
TAPNext++/SAM2 CUDA observation stage is paused unless the explicit allow flag
is present.  Immutable contact-manifest and Three-clip spec revisions are
authored automatically with content-addressed provenance receipts.  This tool
cannot launch LTX generation, Blender, production, database, or network
orchestration.`;
}

export function runDriverCli(argv = process.argv.slice(2), streams = process) {
    try {
        const config = parseDriverArgs(argv);
        if (config.help) { streams.stdout.write(`${helpText()}\n`); return 0; }
        const result = driveV14Pipeline(config);
        streams.stdout.write(`${JSON.stringify(result)}\n`);
        return result.status.startsWith('FAILED_') ? 3 : 0;
    } catch (error) {
        streams.stderr.write(`${JSON.stringify({ status: 'ERROR', error: error.message })}\n`);
        return 2;
    }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;
if (invokedUrl === import.meta.url) process.exitCode = runDriverCli();

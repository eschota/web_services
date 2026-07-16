import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
    V14_DRIVER_PIN_TRANSITION_SCHEMA,
    deriveV14RevisionConfig,
    driveV14Pipeline,
    executeV14StageCommand,
    parseDriverArgs,
    validateV14StageCommand,
} from '../drive_v14_browser_fitting_pipeline.mjs';
import { V14_PIPELINE_STATE_SCHEMA } from '../run_v14_browser_fitting_pipeline.mjs';

const TOOLS_DIRECTORY = path.dirname(fileURLToPath(new URL('../placeholder', import.meta.url)));
const PYTHON_WORKING_DIRECTORY = path.dirname(TOOLS_DIRECTORY);
const digest = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex');

function write(filename, value) {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8');
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, buffer);
    return { path: path.resolve(filename), bytes: buffer.length, sha256: digest(buffer) };
}

function writeJson(filename, value) {
    return write(filename, `${JSON.stringify(value, null, 2)}\n`);
}

function runnerState(status, { next = null, pinRequest = null, completedStages = [] } = {}) {
    return {
        schema: V14_PIPELINE_STATE_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        fittingMixerUsed: false,
        qaAnimationMixerUsed: true,
        orchestratorExecutesSubprocesses: false,
        status,
        completedStages,
        artifacts: [],
        next,
        pinRequest,
        failures: [],
    };
}

function command({ executable, stage, preconditions }) {
    const nodeScripts = {
        initial_browser_fit: 'browser_fit_canary.mjs',
        hoof_contact_diagnostic: 'diagnose_browser_hoof_contacts.mjs',
        contact_refit_manifest: 'author_browser_contact_refit_manifest.mjs',
        browser_contact_refit: 'browser_contact_refit.mjs',
        browser_visual_phase_qa: 'browser_horse_visual_phase_qa.mjs',
    };
    let cwd;
    let argv;
    if (stage === 'object_region_gate') {
        cwd = PYTHON_WORKING_DIRECTORY;
        argv = [executable, '-m', 'animation_fitting.object_region_video_gate', '--synthetic-test'];
    } else if (stage === 'tapnext_sam2_observations') {
        cwd = PYTHON_WORKING_DIRECTORY;
        argv = [executable, '-m', 'animation_fitting.tracking_runtime', 'observe', '--device', 'cuda'];
    } else {
        cwd = TOOLS_DIRECTORY;
        argv = [executable, path.join(TOOLS_DIRECTORY, nodeScripts[stage]), '--synthetic-test'];
    }
    return { cwd, argv, preconditions, powershell: 'not executed by driver' };
}

function nextState(stage, executable, preconditions) {
    return runnerState(`READY_${stage.toUpperCase()}`, {
        next: { stage, command: command({ executable, stage, preconditions }) },
    });
}

test('CLI is exact and CUDA tracking requires an explicit valueless flag', () => {
    assert.deepEqual(parseDriverArgs(['--help']), { help: true });
    assert.deepEqual(parseDriverArgs([
        '--spec', 'spec.json', '--spec-sha256', 'a'.repeat(64),
        '--revision-dir', 'revisions', '--journal-dir', 'journal', '--allow-cuda-tracking',
    ]), {
        specPath: 'spec.json', expectedSpecSha256: 'a'.repeat(64),
        revisionDirectory: 'revisions', journalDirectory: 'journal', allowCudaTracking: true,
    });
    assert.throws(() => parseDriverArgs([]), /--spec is required/);
    assert.throws(() => parseDriverArgs(['--shell', 'pwsh']), /unknown option --shell/);
    assert.throws(() => parseDriverArgs([
        '--spec', 'x', '--spec-sha256', 'a'.repeat(64), '--revision-dir', 'r', '--journal-dir', 'j',
        '--allow-cuda-tracking', '--allow-cuda-tracking',
    ]), /duplicate option --allow-cuda-tracking/);
});

test('structured command validation rejects unknown/LTX/Blender routes and gates CUDA', (context) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-driver-command-'));
    context.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const python = write(path.join(root, 'python.exe'), 'python');
    const node = write(path.join(root, 'node.exe'), 'node');
    const pin = write(path.join(root, 'pin.dat'), 'pin');
    const spec = { runtime: { executables: { python, node } } };
    const preconditions = [python, pin];
    const tracking = nextState('tapnext_sam2_observations', python.path, preconditions);
    assert.deepEqual(
        validateV14StageCommand(tracking, spec),
        {
            allowed: false, reason: 'CUDA_TRACKING_REQUIRES_EXPLICIT_ALLOW',
            stage: 'tapnext_sam2_observations',
            contract: validateV14StageCommand(tracking, spec, { allowCudaTracking: true }).contract,
        },
    );
    assert.equal(validateV14StageCommand(tracking, spec, { allowCudaTracking: true }).allowed, true);

    const ltx = runnerState('READY_LTX', {
        next: { stage: 'ltx_generation', command: command({ executable: node.path, stage: 'initial_browser_fit', preconditions }) },
    });
    assert.throws(() => validateV14StageCommand(ltx, spec), /not in the V14 CPU\/browser allowlist/);
    const blender = nextState('initial_browser_fit', node.path, preconditions);
    blender.next.command.argv[1] = path.join(root, 'blender.exe');
    assert.throws(() => validateV14StageCommand(blender, spec), /command script drift/);
    const injected = nextState('object_region_gate', python.path, preconditions);
    injected.next.command.shell = true;
    assert.throws(() => validateV14StageCommand(injected, spec), /must contain exactly/);
});

test('stage execution verifies every pin and always launches argv with shell false', (context) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-driver-exec-'));
    context.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const python = write(path.join(root, 'python.exe'), 'python');
    const pin = write(path.join(root, 'pin.dat'), 'pin');
    const spec = { runtime: { executables: { python } } };
    const state = nextState('object_region_gate', python.path, [python, pin]);
    let launch;
    const result = executeV14StageCommand(state, spec, {}, {
        spawnSync(executable, argv, options) {
            launch = { executable, argv, options };
            return { status: 0, signal: null };
        },
    });
    assert.deepEqual(result, { allowed: true, stage: 'object_region_gate', exitCode: 0 });
    assert.equal(launch.executable, python.path);
    assert.equal(launch.options.shell, false);
    assert.equal(launch.options.cwd, PYTHON_WORKING_DIRECTORY);
    assert.deepEqual(launch.argv.slice(0, 2), ['-m', 'animation_fitting.object_region_video_gate']);
    fs.appendFileSync(pin.path, 'tamper');
    assert.throws(() => executeV14StageCommand(state, spec, {}, { spawnSync: () => ({ status: 0 }) }), /no longer matches/);
});

test('unattended driver runs an allowed CPU stage then pauses before CUDA by default', (context) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-driver-pause-'));
    context.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const outputRoot = path.join(root, 'pipeline-output');
    const revisions = path.join(root, 'revisions');
    const journal = path.join(root, 'journal');
    fs.mkdirSync(revisions);
    fs.mkdirSync(journal);
    const python = write(path.join(root, 'python.exe'), 'python');
    const pin = write(path.join(root, 'pin.dat'), 'pin');
    const specPin = writeJson(path.join(root, 'spec.json'), {
        outputRoot,
        runtime: { executables: { python } },
    });
    let inspection = 0;
    const executed = [];
    const result = driveV14Pipeline({
        specPath: specPin.path, expectedSpecSha256: specPin.sha256,
        revisionDirectory: revisions, journalDirectory: journal,
    }, {
        inspectPipeline() {
            inspection += 1;
            return inspection === 1
                ? nextState('object_region_gate', python.path, [python, pin])
                : nextState('tapnext_sam2_observations', python.path, [python, pin]);
        },
        executeStage(state) {
            executed.push(state.next.stage);
            return { allowed: true, stage: state.next.stage, exitCode: 0 };
        },
    });
    assert.equal(result.status, 'PAUSED_CUDA_TRACKING_REQUIRES_EXPLICIT_ALLOW');
    assert.equal(result.nextStage, 'tapnext_sam2_observations');
    assert.deepEqual(executed, ['object_region_gate']);
    assert.equal(result.executedStages.length, 1);
    assert.match(result.executedStages[0].receipt.path, /stage-receipt-[0-9a-f]{64}\.json$/);
});

test('both external barriers produce immutable deterministic revisions and provenance receipts', (context) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-driver-pins-'));
    context.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const outputRoot = path.join(root, 'pipeline-output');
    const revisions = path.join(root, 'revisions');
    const journal = path.join(root, 'journal');
    fs.mkdirSync(path.join(outputRoot, '06-browser-contact-refit'), { recursive: true });
    fs.mkdirSync(revisions);
    fs.mkdirSync(journal);
    const contact = writeJson(path.join(outputRoot, '05-contact-refit-input.json'), { contact: true });
    const clip = writeJson(path.join(outputRoot, '06-browser-contact-refit', 'three-clip.json'), { clip: true });
    const controlledJob = writeJson(path.join(root, 'job.json'), { completed: true });
    const guideVideo = write(path.join(root, 'guide.mkv'), 'guide');
    const runtimePins = writeJson(path.join(root, 'runtime-pins.json'), { pins: true });
    const toolPins = writeJson(path.join(root, 'tool-pins.json'), { pins: true });
    const externalPins = { contactRefitInputManifestSha256: null, threeClipSha256: null };
    const baseSpec = {
        outputRoot,
        clipName: 'Horse_Walk_Test',
        controlledGeneration: { state: controlledJob },
        guide: { bundleDirectory: path.join(root, 'guide'), immutableManifestSha256: '1'.repeat(64) },
        canonicalBundle: {
            directory: path.join(root, 'canonical'), immutableManifestSha256: '2'.repeat(64),
            fittingBundleSha256: '3'.repeat(64), sourceModelSha256: '4'.repeat(64),
        },
        runtime: { executables: {} },
        toolSources: Object.fromEntries(Array.from({ length: 28 }, (_, index) => [`tool${index}`, {}])),
        externalPins,
        authoringProvenance: {
            guideIntervalVideo: guideVideo,
            runtimePinManifest: runtimePins,
            toolSourcePinManifest: toolPins,
        },
    };
    const initial = writeJson(path.join(root, 'spec.json'), baseSpec);
    const specFromConfig = (config) => ({
        ...baseSpec,
        externalPins: {
            contactRefitInputManifestSha256: config.contactRefitInputManifestSha256,
            threeClipSha256: config.threeClipSha256,
        },
    });
    const buildPipelineSpec = (config) => ({ spec: specFromConfig(config), snapshots: [] });
    const authorPipelineSpec = (config) => {
        const pin = writeJson(config.output, specFromConfig(config));
        return { spec: pin, initialPipelineStatus: 'TEST' };
    };
    const inspectPipeline = ({ specPath }) => {
        const spec = JSON.parse(fs.readFileSync(specPath, 'utf8'));
        if (spec.externalPins.contactRefitInputManifestSha256 == null) {
            return runnerState('AWAITING_EXTERNAL_CONTACT_MANIFEST_PIN', {
                pinRequest: {
                    field: 'externalPins.contactRefitInputManifestSha256',
                    observedSha256NotTrusted: contact.sha256,
                    observedBytes: contact.bytes,
                    instruction: 'Create a new externally SHA-pinned spec revision; do not edit or overwrite this artifact.',
                },
            });
        }
        if (spec.externalPins.threeClipSha256 == null) {
            return runnerState('AWAITING_EXTERNAL_THREE_CLIP_PIN', {
                completedStages: ['contact_refit_manifest', 'browser_contact_refit'],
                pinRequest: {
                    field: 'externalPins.threeClipSha256',
                    observedSha256NotTrusted: clip.sha256,
                    observedBytes: clip.bytes,
                    instruction: 'Create a new externally SHA-pinned spec revision; do not edit or overwrite this artifact.',
                },
            });
        }
        return runnerState('PASS_MACHINE_QA_AWAITING_HUMAN_REVIEW', {
            completedStages: ['contact_refit_manifest', 'browser_contact_refit', 'browser_visual_phase_qa'],
        });
    };
    const result = driveV14Pipeline({
        specPath: initial.path, expectedSpecSha256: initial.sha256,
        revisionDirectory: revisions, journalDirectory: journal,
    }, { inspectPipeline, buildPipelineSpec, authorPipelineSpec });
    assert.equal(result.status, 'PASS_MACHINE_QA_AWAITING_HUMAN_REVIEW');
    assert.equal(result.authoredRevisions.length, 2);
    const finalSpec = JSON.parse(fs.readFileSync(result.currentSpec.path, 'utf8'));
    assert.equal(finalSpec.externalPins.contactRefitInputManifestSha256, contact.sha256);
    assert.equal(finalSpec.externalPins.threeClipSha256, clip.sha256);
    const receipts = fs.readdirSync(journal).filter((name) => name.startsWith('pin-transition-'));
    assert.equal(receipts.length, 2);
    for (const filename of receipts) {
        const receipt = JSON.parse(fs.readFileSync(path.join(journal, filename), 'utf8'));
        assert.equal(receipt.schema, V14_DRIVER_PIN_TRANSITION_SCHEMA);
        assert.equal(receipt.ltxGenerationExecuted, false);
        assert.equal(receipt.blenderUsed, false);
        assert.equal(receipt.productionOrDatabaseUsed, false);
        assert.match(receipt.tools.driver.sha256, /^[0-9a-f]{64}$/);
        assert.match(receipt.tools.specAuthor.sha256, /^[0-9a-f]{64}$/);
    }

    const derived = deriveV14RevisionConfig(baseSpec, 'externalPins.contactRefitInputManifestSha256', contact.sha256, path.join(revisions, 'x.json'));
    assert.equal(derived.contactRefitInputManifestSha256, contact.sha256);
    assert.equal(derived.threeClipSha256, null);
});

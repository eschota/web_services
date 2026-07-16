import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    V14_PIPELINE_SPEC_SCHEMA,
    V14_PIPELINE_TOOL_SOURCE_PATHS,
    evaluateContactDiagnosticReport,
    evaluateObjectGateReport,
    inspectV14Pipeline,
    parsePipelineArgs,
    runPipelineCli,
    validateFinalContactRefitOutputs,
} from '../run_v14_browser_fitting_pipeline.mjs';
import { buildHorseVisualPhaseEvidence } from '../browser_horse_visual_phase_qa.mjs';

const sha256 = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex');
const jsonBuffer = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');

function write(filename, bufferValue) {
    const buffer = Buffer.isBuffer(bufferValue) ? bufferValue : Buffer.from(String(bufferValue));
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, buffer);
    return { path: filename, bytes: buffer.length, sha256: sha256(buffer) };
}

function writeJson(filename, value) {
    return write(filename, jsonBuffer(value));
}

function fixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-pipeline-'));
    const candidate = write(path.join(root, 'candidate.mp4'), Buffer.from('49-frame-ltx-v14-candidate'));

    const guideRoot = path.join(root, 'guide');
    fs.mkdirSync(guideRoot);
    const guideFrames = Array.from({ length: 49 }, (_, frameIndex) => {
        const filename = `guide_${String(frameIndex).padStart(3, '0')}.png`;
        const pin = write(path.join(guideRoot, filename), Buffer.from(`guide-frame-${frameIndex}`));
        return {
            frame_index_int: frameIndex,
            filename_string: filename,
            sha256_string: pin.sha256,
            bytes_int: pin.bytes,
        };
    });
    const interval = write(path.join(guideRoot, 'interval_guide.mkv'), Buffer.from('lossless-v14-interval'));
    const pose = write(path.join(guideRoot, 'pose_contract.json'), Buffer.from('{"pose":"contract"}\n'));
    const guideManifest = writeJson(path.join(guideRoot, 'immutable_manifest.json'), {
        schema: 'autorig-browser-ltx-interval-guide-bundle.v1',
        status: 'PASS', browserOnly: true, blenderUsed: false, rigType: 'HORSE_2',
        cycle_frame_count_int: 49, browser_frame_count_int: 49, guide_count_int: 1,
        endpoint_guide_sha256_string: guideFrames[0].sha256_string,
        frames_array: guideFrames,
        interval_guide_video_object: {
            filename: path.basename(interval.path), bytes: interval.bytes, sha256: interval.sha256,
            frameCount: 49, audioStreamCount: 0,
        },
        poseContract: { filename: path.basename(pose.path), bytes: pose.bytes, sha256: pose.sha256 },
    });

    const bundleRoot = path.join(root, 'canonical');
    fs.mkdirSync(bundleRoot);
    const sourceModelSha256 = 'a'.repeat(64);
    const skeleton = writeJson(path.join(bundleRoot, 'skeleton.json'), { armatures: [] });
    const skinWeights = write(path.join(bundleRoot, 'skin_weights.json.gz'), Buffer.from('synthetic-skin-weights'));
    const topology = write(path.join(bundleRoot, 'surface_topology.json.gz'), Buffer.from('synthetic-surface-topology'));
    const fitting = writeJson(path.join(bundleRoot, 'fitting_bundle.json'), {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: { filename: 'Horse_2.blend', sha256: sourceModelSha256, species: 'horse', rig_type: 'HORSE_2' },
        actionless: { actionless: true },
        camera: { name: 'fixed', resolution: [768, 448] },
        artifacts: {
            skeleton: { filename: 'skeleton.json', bytes: skeleton.bytes, sha256: skeleton.sha256 },
            skin_weights: { filename: 'skin_weights.json.gz', bytes: skinWeights.bytes, sha256: skinWeights.sha256 },
            surface_topology: { filename: 'surface_topology.json.gz', bytes: topology.bytes, sha256: topology.sha256 },
        },
    });
    const bundleFiles = [fitting, skeleton, skinWeights, topology];
    const immutable = writeJson(path.join(bundleRoot, 'immutable_manifest.json'), {
        schema: 'autorig-fitting-immutable-copy.v1',
        bundle_file_count: bundleFiles.length,
        bundle_total_bytes: bundleFiles.reduce((total, pin) => total + pin.bytes, 0),
        source_model: { sha256: sourceModelSha256 },
        bundle_manifest: { filename: 'fitting_bundle.json', sha256: fitting.sha256 },
        files: bundleFiles.map((pin) => ({ filename: path.basename(pin.path), bytes: pin.bytes, sha256: pin.sha256 })),
    });

    const runtimeRoot = path.join(root, 'tracking-runtime');
    fs.mkdirSync(runtimeRoot);
    const runtimeLock = write(path.join(root, 'runtime-lock.json'), Buffer.from('{"runtime":"pinned"}\n'));
    const executable = (name) => write(path.join(root, 'bin', `${name}.exe`), Buffer.from(`pinned-${name}`));
    const executables = {
        python: executable('python'), node: executable('node'), chrome: executable('chrome'),
        ffmpeg: executable('ffmpeg'), ffprobe: executable('ffprobe'),
    };
    const three = write(path.join(root, 'three.module.js'), Buffer.from("export const REVISION='160';\n"));
    const outputRoot = path.join(root, 'run');
    const specPath = path.join(root, 'pipeline-spec.json');
    const descriptor = (pin) => ({ path: pin.path, bytes: pin.bytes, sha256: pin.sha256 });
    const spec = {
        schema: V14_PIPELINE_SPEC_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        orchestratorExecutesSubprocesses: false,
        semanticId: 'walk_forward',
        clipName: 'Horse_Walk_LTX_V14_Browser_Contact_Refit',
        outputRoot,
        candidate: descriptor(candidate),
        guide: {
            bundleDirectory: guideRoot,
            immutableManifestSha256: guideManifest.sha256,
            endpointGuide: descriptor({ ...guideFrames[0], path: path.join(guideRoot, guideFrames[0].filename_string), sha256: guideFrames[0].sha256_string, bytes: guideFrames[0].bytes_int }),
        },
        canonicalBundle: {
            directory: bundleRoot,
            immutableManifestSha256: immutable.sha256,
            fittingBundleSha256: fitting.sha256,
            sourceModelSha256,
        },
        runtime: {
            executables: Object.fromEntries(Object.entries(executables).map(([key, pin]) => [key, descriptor(pin)])),
            threeModule: { ...descriptor(three), revision: '160' },
            trackingRuntimeRoot: runtimeRoot,
            trackingRuntimeLock: descriptor(runtimeLock),
        },
        toolSources: Object.fromEntries(Object.entries(V14_PIPELINE_TOOL_SOURCE_PATHS).map(([name, filename]) => {
            const buffer = fs.readFileSync(filename);
            return [name, { path: filename, bytes: buffer.length, sha256: sha256(buffer) }];
        })),
        externalPins: {
            contactRefitInputManifestSha256: null,
            threeClipSha256: null,
        },
    };
    const specPin = writeJson(specPath, spec);
    return {
        root, candidate, guideManifest, fitting, immutable, skeleton, skinWeights, topology, sourceModelSha256,
        outputRoot, specPath, specSha256: specPin.sha256, spec,
    };
}

function gateReport(f, passed) {
    return {
        schema: 'autorig.animation-fitting.object-region-video-gate.v1',
        verdict: passed ? 'PASS' : 'FAIL',
        approved_for_fitting: passed,
        inputs: {
            candidate: { path: f.candidate.path, bytes: f.candidate.bytes, sha256: f.candidate.sha256 },
            endpoint_guide: {
                path: f.spec.guide.endpointGuide.path,
                bytes: f.spec.guide.endpointGuide.bytes,
                sha256: f.spec.guide.endpointGuide.sha256,
            },
            guide_bundle: { manifest_sha256: f.guideManifest.sha256 },
        },
    };
}

function publishGate(f, passed) {
    const directory = path.join(f.outputRoot, '01-object-region-gate');
    fs.mkdirSync(directory, { recursive: true });
    writeJson(path.join(directory, 'object_region_video_gate.json'), gateReport(f, passed));
    write(path.join(directory, 'object_region_video_gate.png'), Buffer.from('pinned-gate-evidence'));
}

test('CLI accepts help and requires externally pinned spec/state arguments', () => {
    assert.deepEqual(parsePipelineArgs(['--help']), { help: true });
    assert.throws(() => parsePipelineArgs(['--spec', 'x']), /--spec-sha256 is required/);
    assert.deepEqual(parsePipelineArgs([
        '--spec', 'spec.json', '--spec-sha256', '1'.repeat(64), '--state', 'state.json',
    ]), { specPath: 'spec.json', expectedSpecSha256: '1'.repeat(64), statePath: 'state.json' });
});

test('fresh pinned V14 run authors one exact object-gate command without executing anything', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const state = inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 });
    assert.equal(state.status, 'READY_OBJECT_REGION_GATE');
    assert.equal(state.next.stage, 'object_region_gate');
    assert.equal(state.orchestratorExecutesSubprocesses, false);
    assert.equal(state.blenderUsed, false);
    assert.equal(state.fittingMixerUsed, false);
    assert.equal(state.qaAnimationMixerUsed, true);
    assert.ok(state.next.command.argv.includes(f.candidate.sha256));
    assert.ok(state.next.command.argv.includes(f.guideManifest.sha256));
    assert.ok(state.next.command.argv.includes(path.join(f.outputRoot, '01-object-region-gate')));
    assert.ok(Object.values(f.spec.toolSources).every((pin) => (
        state.next.command.preconditions.some((row) => row.path === pin.path && row.sha256 === pin.sha256)
    )));
    assert.match(state.next.command.powershell, /animation_fitting\.object_region_video_gate/);
    const source = fs.readFileSync(new URL('../run_v14_browser_fitting_pipeline.mjs', import.meta.url), 'utf8');
    assert.doesNotMatch(source, /node:child_process|\bspawnSync\b|\bexecFile\b/);
});

test('PASS object gate advances only to the exact pinned TAPNext++/SAM2 command', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    publishGate(f, true);
    const state = inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 });
    assert.equal(state.status, 'READY_TRACKING');
    assert.deepEqual(state.completedStages, ['object_region_gate']);
    assert.equal(state.next.stage, 'tapnext_sam2_observations');
    assert.ok(state.next.command.argv.includes('animation_fitting.tracking_runtime'));
    assert.ok(state.next.command.argv.includes('--device'));
    assert.ok(state.next.command.argv.includes('cuda'));
    assert.ok(state.next.command.argv.includes(f.guideManifest.sha256));
    assert.ok(state.next.command.argv.includes(f.immutable.path.replace('immutable_manifest.json', '')) || state.next.command.argv.includes(f.spec.canonicalBundle.directory));
});

test('FAIL object gate is terminal and never exposes a tracking command', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    publishGate(f, false);
    const state = inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 });
    assert.equal(state.status, 'FAILED_OBJECT_REGION_GATE');
    assert.equal(state.next, null);
    assert.deepEqual(state.failures, ['object_region_gate']);
});

test('partial gate publication and candidate mutation fail closed', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.mkdirSync(path.join(f.outputRoot, '01-object-region-gate'), { recursive: true });
    writeJson(path.join(f.outputRoot, '01-object-region-gate', 'object_region_video_gate.json'), gateReport(f, true));
    assert.throws(
        () => inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 }),
        /artifact inventory is partial/,
    );
    fs.rmSync(f.outputRoot, { recursive: true });
    fs.appendFileSync(f.candidate.path, 'tamper');
    assert.throws(
        () => inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 }),
        /spec\.candidate SHA-256 mismatch|byte count mismatch/,
    );
});

test('stale or out-of-order later stage paths are rejected before any command is exposed', (context) => {
    const f = fixture();
    const failed = fixture();
    context.after(() => {
        fs.rmSync(f.root, { recursive: true, force: true });
        fs.rmSync(failed.root, { recursive: true, force: true });
    });
    fs.mkdirSync(path.join(f.outputRoot, '06-browser-contact-refit'), { recursive: true });
    assert.throws(
        () => inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 }),
        /out-of-order stage browser_contact_refit exists before required stage object_region_gate/,
    );
    publishGate(failed, false);
    fs.mkdirSync(path.join(failed.outputRoot, '02-observations'));
    assert.throws(
        () => inspectV14Pipeline({ specPath: failed.specPath, expectedSpecSha256: failed.specSha256 }),
        /object-region gate failed; stale later-stage paths exist: observations/,
    );
});

test('tool source closure is externally pinned and cannot be omitted or relabeled', (context) => {
    assert.deepEqual(Object.keys(V14_PIPELINE_TOOL_SOURCE_PATHS).sort(), [
        'backendAnimationFittingInit', 'backendMath3d', 'backendObservations', 'backendOptimizer',
        'browserContactRefit', 'browserCore', 'browserFit', 'browserVisualQa',
        'contactManifestAuthor', 'contactProfile', 'fittingErrors', 'fittingRig',
        'hoofContactInference', 'hoofDiagnostic', 'objectRegionVideoGate', 'pipelineOrchestrator',
        'rgbObservationBridge', 'semanticTracker', 'threeAdapter', 'trackingCli',
        'trackingContactIntegration', 'trackingContactSolver', 'trackingCore', 'trackingInit',
        'trackingMain', 'trackingModels', 'trackingOfficialBackends', 'trackingRuntimeLock',
    ]);
    assert.equal(Object.keys(V14_PIPELINE_TOOL_SOURCE_PATHS).length, 28);
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const changed = structuredClone(f.spec);
    delete changed.toolSources.browserFit;
    const changedPin = writeJson(path.join(f.root, 'missing-tool-source.json'), changed);
    assert.throws(
        () => inspectV14Pipeline({ specPath: changedPin.path, expectedSpecSha256: changedPin.sha256 }),
        /exact source closure/,
    );
    const relabeled = structuredClone(f.spec);
    relabeled.toolSources.browserFit = relabeled.toolSources.browserVisualQa;
    const relabeledPin = writeJson(path.join(f.root, 'relabeled-tool-source.json'), relabeled);
    assert.throws(
        () => inspectV14Pipeline({ specPath: relabeledPin.path, expectedSpecSha256: relabeledPin.sha256 }),
        /does not resolve to the commanded source file/,
    );
});

test('diagnostic FAIL is represented as a terminal decision and input pin drift is rejected', () => {
    const expected = {
        observationsSha256: '1'.repeat(64), bridgeReportSha256: '2'.repeat(64), sourceVideoSha256: '3'.repeat(64),
    };
    const report = {
        schema: 'autorig-browser-hoof-contact-diagnostic.v1',
        status: 'FAIL',
        inputs: {
            observations: { sha256: expected.observationsSha256 },
            bridgeReport: { sha256: expected.bridgeReportSha256 },
            sourceVideo: { sha256: expected.sourceVideoSha256 },
        },
        schedule: { qa: { failures: ['walk_phase_order'] } },
    };
    assert.deepEqual(evaluateContactDiagnosticReport(report, expected), {
        passed: false, status: 'FAIL', failures: ['walk_phase_order'],
    });
    report.inputs.sourceVideo.sha256 = '4'.repeat(64);
    assert.throws(() => evaluateContactDiagnosticReport(report, expected), /does not bind/);
});

test('object gate evaluator rejects forged PASS and exact input drift', () => {
    const expected = {
        candidate: { sha256: '1'.repeat(64), bytes: 10 },
        endpoint: { sha256: '2'.repeat(64), bytes: 20 },
        guideManifestSha256: '3'.repeat(64),
    };
    const report = {
        schema: 'autorig.animation-fitting.object-region-video-gate.v1',
        verdict: 'PASS', approved_for_fitting: false,
        inputs: {
            candidate: expected.candidate, endpoint_guide: expected.endpoint,
            guide_bundle: { manifest_sha256: expected.guideManifestSha256 },
        },
    };
    assert.throws(() => evaluateObjectGateReport(report, expected), /inconsistent/);
    report.approved_for_fitting = true;
    report.inputs.candidate = { sha256: '4'.repeat(64), bytes: 10 };
    assert.throws(() => evaluateObjectGateReport(report, expected), /does not bind/);
});

test('state publication is deterministic and create-exclusive', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const first = path.join(f.root, 'state-001.json');
    const second = path.join(f.root, 'state-002.json');
    const streams = () => ({ stdout: { write() {} }, stderr: { write() {} } });
    const argv = ['--spec', f.specPath, '--spec-sha256', f.specSha256, '--state'];
    assert.equal(runPipelineCli([...argv, first], streams()), 0);
    assert.equal(runPipelineCli([...argv, second], streams()), 0);
    assert.deepEqual(fs.readFileSync(first), fs.readFileSync(second));
    assert.equal(runPipelineCli([...argv, first], streams()), 2);
    assert.deepEqual(fs.readFileSync(first), fs.readFileSync(second));
});

test('final contact-refit outputs deep-bind manifest pins, all gates, provenance, fitted motion and Three clip', (context) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-final-refit-contract-'));
    context.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const bundleDirectory = path.join(root, 'bundle');
    fs.mkdirSync(bundleDirectory);
    const observationsPath = writeJson(path.join(root, 'observations.json'), { schema: 'observations' }).path;
    const pins = {
        inputManifestSha256: '0'.repeat(64), sourceVideoSha256: '1'.repeat(64),
        fittingBundleSha256: '2'.repeat(64), immutableManifestSha256: '3'.repeat(64),
        sourceModelSha256: '4'.repeat(64), sourceSkeletonSha256: '5'.repeat(64),
        observationsSha256: '6'.repeat(64), bridgeReportSha256: '7'.repeat(64),
        initialFitSummarySha256: '8'.repeat(64), diagnosticSha256: '9'.repeat(64),
    };
    const qa = {
        targetSamples: 10, initialMeanTargetErrorPx: 2, finalMeanTargetErrorPx: 1,
        maximumTargetErrorPx: 2, maximumBoneLengthErrorPx: 0,
        maximumJointLimitViolationRad: 0, maximumContactSlidePx: 0, loopEndpointError: 0,
    };
    const frameCount = 8;
    const fps = 7;
    const duration = 1;
    const times = Array.from({ length: frameCount }, (_, index) => index / fps);
    const quaternionTrack = { name: 'Bone.quaternion', type: 'quaternion', times, values: times.flatMap(() => [0, 0, 0, 1]) };
    const positionTrack = { name: 'Bone.position', type: 'vector', times, values: times.flatMap(() => [0, 0, 0]) };
    const gate = (name) => ({ name, comparator: '===', actual: true, threshold: true, passed: true });
    const gates = [
        'head_reconstruction_world', 'rest_seed_alignment_px', 'final_mean_target_error_px',
        'maximum_target_error_px', 'bone_length_error_px', 'joint_limit_violation_rad',
        'contact_slide_px', 'loop_endpoint_error', 'hierarchy_segment_drift_world',
        'hierarchy_reprojection_error_px', 'requested_fitted_point_error_px',
        'unreachable_pixel_ray_ratio', 'target_sample_coverage', 'target_error_improved',
        'ordered_deform_heads', 'four_limb_contacts', 'three_clip_validate',
        'three_tracks_bound', 'pinned_contact_schedule', 'semantic_walk_gait',
        'fitted_walk_contact_slide',
    ].map(gate);
    Object.assign(gates.find((row) => row.name === 'four_limb_contacts'), { actual: 4, threshold: 4, enforced: true });
    Object.assign(gates.find((row) => row.name === 'pinned_contact_schedule'), { actual: 'PASS', threshold: 'PASS' });
    const commonInputs = {
        sourceVideoSha256: pins.sourceVideoSha256, fittingBundleSha256: pins.fittingBundleSha256,
        immutableManifestSha256: pins.immutableManifestSha256, sourceModelSha256: pins.sourceModelSha256,
        skeletonSha256: pins.sourceSkeletonSha256, observationsSha256: pins.observationsSha256,
        bundleDirectory, observationsPath,
    };
    const summary = {
        schema: 'autorig-browser-fit-canary-summary.v1', status: 'PASS_BROWSER_CONTACT_REFIT_GATES',
        browserOnly: true, blenderUsed: false, mixerUsed: false, fittingMode: 'contact_constrained_refit',
        approvedForBrowserContactFit: true, approvedForAnimationLibrary: false,
        approvalExclusions: ['fixed_camera_visual_phase_qa', 'target_mesh_deformation_qa'],
        gates: { passed: true, results: gates }, inputs: commonInputs,
        observations: { contactCount: 4 },
        fit: { frameCount, durationSeconds: duration, quaternionTracks: 1, positionTracks: 1, qa },
        hierarchyClip: { name: 'Horse_Walk_Deep_Pin', tracks: 2 },
        contactRefit: {
            scheduleStatus: 'PASS', semanticGaitQa: { accepted: true, simultaneousSwingFrameCount: 0 },
            fittedWalkQa: { status: 'PASS', failures: [], maximumContactSlideRatio: 0, thresholdRatio: 0.1 },
            provenance: {
                schema: 'autorig-browser-contact-refit-provenance.v1', source: 'immutable_pass_diagnostic',
                browserOnly: true, blenderUsed: false, mixerUsed: false, ...pins,
            },
        },
    };
    const bridge = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1', status: 'VALIDATED',
        browserOnly: true, blenderUsed: false, mixerUsed: false, fittingMode: 'contact_constrained_refit',
        preparedContacts: 4, inputs: commonInputs,
    };
    const fitted = {
        schema: 'autorig-browser-fitted-animation.v1', loop: true, frameCount, fps, durationSeconds: duration,
        tracks: [quaternionTrack], positionTracks: [positionTrack], qa,
        frames: Array.from({ length: frameCount }, (_, frame) => ({
            frame,
            limbs: Object.fromEntries(['fore_left', 'fore_right', 'hind_left', 'hind_right']
                .map((foot) => [foot, { points: [[0, 0], [1, 1], [2, 2]] }])),
        })),
    };
    const clip = {
        name: 'Horse_Walk_Deep_Pin', duration, uuid: 'deep-pin', blendMode: 2500,
        tracks: [quaternionTrack, positionTrack],
    };
    const snapshot = (filename, value) => {
        const pin = writeJson(path.join(root, filename), value);
        return { ...pin, buffer: fs.readFileSync(pin.path) };
    };
    const snapshots = {
        'bridge-report.json': snapshot('bridge-report.json', bridge),
        'fit-summary.json': snapshot('fit-summary.json', summary),
        'fitted-animation.json': snapshot('fitted-animation.json', fitted),
        'three-clip.json': snapshot('three-clip.json', clip),
    };
    assert.equal(validateFinalContactRefitOutputs({
        snapshots, validatedInput: { bundleDirectory, observationsPath, pins },
    }).threeClip.name, 'Horse_Walk_Deep_Pin');
    const forged = structuredClone(summary);
    forged.inputs.sourceVideoSha256 = 'f'.repeat(64);
    snapshots['fit-summary.json'] = snapshot('fit-summary-forged.json', forged);
    assert.throws(() => validateFinalContactRefitOutputs({
        snapshots, validatedInput: { bundleDirectory, observationsPath, pins },
    }), /sourceVideoSha256 does not bind/);
});

test('synthetic complete chain pauses at both external pins then reaches visual machine PASS', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    publishGate(f, true);

    const observationsDirectory = path.join(f.outputRoot, '02-observations');
    fs.mkdirSync(observationsDirectory, { recursive: true });
    const observations = writeJson(path.join(observationsDirectory, 'observations.json'), {
        schema: 'autorig-fitting-observations.v1', frame_count: 49,
        silhouettes: Array.from({ length: 49 }, (_, frame) => ({ frame, path: `masks/frame_${frame}.png` })),
        provenance: {
            source_video_sha256: f.candidate.sha256,
            immutable_manifest_sha256: f.immutable.sha256,
            bundle_sha256: f.fitting.sha256,
            first_frame_reference: { selected: { manifest: { sha256: f.guideManifest.sha256 } } },
        },
    });
    writeJson(path.join(observationsDirectory, 'observation_bundle_manifest.json'), {
        schema: 'autorig-tracking-observation-bundle.v1',
        files: [{ path: 'observations.json', bytes: observations.bytes, sha256: observations.sha256 }],
        provenance: {},
    });

    const initialDirectory = path.join(f.outputRoot, '03-initial-browser-fit');
    fs.mkdirSync(initialDirectory);
    const bridge = writeJson(path.join(initialDirectory, 'bridge-report.json'), {
        schema: 'autorig-browser-fit-canary-bridge-report.v1', status: 'VALIDATED',
        browserOnly: true, blenderUsed: false, mixerUsed: false,
    });
    const initialSummary = writeJson(path.join(initialDirectory, 'fit-summary.json'), {
        schema: 'autorig-browser-fit-canary-summary.v1', status: 'PASS_BROWSER_FIT_GATES',
        browserOnly: true, blenderUsed: false, mixerUsed: false,
        approvedForAnimationLibrary: false, gates: { passed: true },
        inputs: {
            sourceVideoSha256: f.candidate.sha256,
            immutableManifestSha256: f.immutable.sha256,
            fittingBundleSha256: f.fitting.sha256,
            sourceModelSha256: f.sourceModelSha256,
            observationsSha256: observations.sha256,
        },
    });

    const diagnostic = writeJson(path.join(f.outputRoot, '04-hoof-contact-diagnostic.json'), {
        schema: 'autorig-browser-hoof-contact-diagnostic.v1', status: 'PASS',
        inputs: {
            observations: { sha256: observations.sha256 },
            bridgeReport: { sha256: bridge.sha256 },
            sourceVideo: { sha256: f.candidate.sha256 },
        },
        schedule: { qa: { failures: [] } },
    });
    writeJson(path.join(f.outputRoot, '04-sam2-ground-evidence.json'), { schema: 'synthetic-ground-evidence.v1' });
    const contactManifest = writeJson(path.join(f.outputRoot, '05-contact-refit-input.json'), {
        schema: 'autorig-browser-contact-refit-input.v1', diagnosticSha256: diagnostic.sha256,
    });

    let state = inspectV14Pipeline({ specPath: f.specPath, expectedSpecSha256: f.specSha256 });
    assert.equal(state.status, 'AWAITING_EXTERNAL_CONTACT_MANIFEST_PIN');
    assert.equal(state.next, null);
    assert.equal(state.pinRequest.observedSha256NotTrusted, contactManifest.sha256);

    const specV2 = structuredClone(f.spec);
    specV2.externalPins.contactRefitInputManifestSha256 = contactManifest.sha256;
    const specV2Pin = writeJson(path.join(f.root, 'pipeline-spec-v2.json'), specV2);
    let contactValidationCalls = 0;
    const dependencies = {
        validateContactRefitInputs(args) {
            contactValidationCalls += 1;
            assert.equal(args.expectedManifestSha256, contactManifest.sha256);
            return {
                bundleDirectory: f.spec.canonicalBundle.directory,
                observationsPath: observations.path,
                pins: {
                    inputManifestSha256: contactManifest.sha256,
                    sourceVideoSha256: f.candidate.sha256,
                    fittingBundleSha256: f.fitting.sha256,
                    immutableManifestSha256: f.immutable.sha256,
                    sourceModelSha256: f.sourceModelSha256,
                    sourceSkeletonSha256: f.skeleton.sha256,
                    observationsSha256: observations.sha256,
                    bridgeReportSha256: bridge.sha256,
                    initialFitSummarySha256: initialSummary.sha256,
                    diagnosticSha256: diagnostic.sha256,
                },
            };
        },
        validateFinalContactRefitOutputs(args) {
            assert.equal(args.validatedInput.pins.inputManifestSha256, contactManifest.sha256);
        },
        validateHorse2QaInputs(args) {
            assert.equal(args.expectedThreeClipSha256, threeClip.sha256);
        },
    };
    state = inspectV14Pipeline(
        { specPath: specV2Pin.path, expectedSpecSha256: specV2Pin.sha256 },
        dependencies,
    );
    assert.equal(state.status, 'READY_BROWSER_CONTACT_REFIT');
    assert.equal(state.next.stage, 'browser_contact_refit');
    assert.equal(contactValidationCalls, 1);
    assert.ok(state.next.command.preconditions.some((row) => row.sha256 === f.spec.runtime.threeModule.sha256));
    assert.match(state.next.command.powershell, /Get-FileHash/);

    const refitDirectory = path.join(f.outputRoot, '06-browser-contact-refit');
    fs.mkdirSync(refitDirectory);
    writeJson(path.join(refitDirectory, 'bridge-report.json'), { schema: 'autorig-browser-fit-canary-bridge-report.v1' });
    writeJson(path.join(refitDirectory, 'fit-summary.json'), {
        schema: 'autorig-browser-fit-canary-summary.v1', status: 'PASS_BROWSER_CONTACT_REFIT_GATES',
        gates: { passed: true }, approvedForBrowserContactFit: true, approvedForAnimationLibrary: false,
        browserOnly: true, blenderUsed: false, mixerUsed: false, fittingMode: 'contact_constrained_refit',
    });
    writeJson(path.join(refitDirectory, 'fitted-animation.json'), { schema: 'autorig-browser-fitted-animation.v1' });
    const threeClip = writeJson(path.join(refitDirectory, 'three-clip.json'), {
        name: 'synthetic-external-pin-barrier', duration: 1.6, tracks: [],
    });
    state = inspectV14Pipeline(
        { specPath: specV2Pin.path, expectedSpecSha256: specV2Pin.sha256 },
        dependencies,
    );
    assert.equal(state.status, 'AWAITING_EXTERNAL_THREE_CLIP_PIN');
    assert.equal(state.next, null);
    assert.equal(state.pinRequest.observedSha256NotTrusted, threeClip.sha256);

    const specV3 = structuredClone(specV2);
    specV3.externalPins.threeClipSha256 = threeClip.sha256;
    const specV3Pin = writeJson(path.join(f.root, 'pipeline-spec-v3.json'), specV3);
    state = inspectV14Pipeline(
        { specPath: specV3Pin.path, expectedSpecSha256: specV3Pin.sha256 },
        dependencies,
    );
    assert.equal(state.status, 'READY_BROWSER_VISUAL_PHASE_QA');
    assert.equal(state.next.stage, 'browser_visual_phase_qa');
    assert.ok(state.next.command.argv.includes(threeClip.sha256));
    assert.ok(state.next.command.argv.includes(f.spec.runtime.threeModule.sha256));

    const qaDirectory = path.join(f.outputRoot, '07-browser-visual-phase-qa');
    const framesDirectory = path.join(qaDirectory, 'frames');
    fs.mkdirSync(framesDirectory, { recursive: true });
    const framePins = [];
    for (let frame = 0; frame < 49; frame += 1) {
        framePins.push(write(path.join(framesDirectory, `frame_${String(frame).padStart(4, '0')}.png`), Buffer.from(`frame-${frame}`)));
    }
    const cameraPin = writeJson(path.join(qaDirectory, 'camera-settings.json'), {
        schema: 'autorig.browser-horse-fixed-camera.v1',
    });
    const deformationReport = {
        schema: 'autorig.browser-horse-target-deformation-qa.v1', passed: true,
        maximumEdgeStretch: 1.2, p99EdgeStretch: 1.1, zeroWeightVertices: 0,
        maximumCoincidentRestSeparationM: 0.001, coincidentRestSampleCount: 12,
        coincidentRestGroupCount: 3, gates: { coincidentRestSeparation: true },
        inputs: {
            fittingBundleSha256: f.fitting.sha256, threeClipSha256: threeClip.sha256,
            skinWeightsSha256: f.skinWeights.sha256, topologySha256: f.topology.sha256,
        },
    };
    const deformationPin = writeJson(path.join(qaDirectory, 'deformation-report.json'), deformationReport);
    const videoPin = write(path.join(qaDirectory, 'fixed-camera-preview.mp4'), Buffer.from('synthetic-no-audio-preview'));
    const phaseFramePins = [
        { ...framePins[0], phase: 'start', frameIndex: 0 },
        { ...framePins[24], phase: 'middle', frameIndex: 24 },
        { ...framePins[36], phase: 'three_quarter', frameIndex: 36 },
    ];
    const visualEvidence = buildHorseVisualPhaseEvidence({
        semanticId: 'walk_forward',
        validated: {
            fittingBundle: { source: { filename: 'Horse_2.blend', sha256: f.sourceModelSha256 } },
            clipContract: { frameCount: 49, pin: threeClip },
            immutablePin: f.immutable,
            fittingBundlePin: f.fitting,
            skeletonPin: f.skeleton,
            skinWeightsPin: f.skinWeights,
            topologyPin: f.topology,
        },
        deformationReport,
        deformationReportPin: deformationPin,
        phaseFramePins,
        videoPin: { ...videoPin, frame_count: 49 },
        cameraSettingsPin: cameraPin,
        renderer: {
            browser: 'synthetic-real-contract-test',
            three_module: f.spec.runtime.threeModule,
            runtime: { threeRevision: '160' },
        },
    });
    assert.equal(visualEvidence.schema, 'autorig.browser-horse-visual-phase-evidence-envelope.v1');
    writeJson(path.join(qaDirectory, 'visual-phase-qa.json'), visualEvidence);
    state = inspectV14Pipeline(
        { specPath: specV3Pin.path, expectedSpecSha256: specV3Pin.sha256 },
        dependencies,
    );
    assert.equal(state.status, 'PASS_MACHINE_QA_AWAITING_HUMAN_REVIEW');
    assert.equal(state.next, null);
    assert.deepEqual(state.completedStages, [
        'object_region_gate', 'tapnext_sam2_observations', 'initial_browser_fit',
        'hoof_contact_diagnostic', 'contact_refit_manifest', 'browser_contact_refit',
        'browser_visual_phase_qa',
    ]);
    fs.appendFileSync(cameraPin.path, 'tamper');
    assert.throws(() => inspectV14Pipeline(
        { specPath: specV3Pin.path, expectedSpecSha256: specV3Pin.sha256 },
        dependencies,
    ), /camera settings does not match its visual evidence pin/);
});

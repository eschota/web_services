import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    parseContactRefitArgs,
    runBrowserContactRefit,
    runCli,
    validateContactRefitInputs,
} from '../../../tools/animation_fitting/browser_contact_refit.mjs';

const FOOT_ORDER = ['hind_left', 'fore_left', 'hind_right', 'fore_right'];
const sha = (value) => crypto.createHash('sha256').update(value).digest('hex');
const jsonBuffer = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');

function write(filename, value) {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value);
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, buffer);
    return { path: filename, bytes: buffer.length, sha256: sha(buffer) };
}

function writeJson(filename, value) {
    return write(filename, jsonBuffer(value));
}

function contactFrames(touchdown) {
    const unique = Array.from({ length: 36 }, (_, offset) => (touchdown + offset) % 48)
        .sort((first, second) => first - second);
    return unique.includes(0) ? [...unique, 48] : unique;
}

function cyclicContactFrames(touchdown) {
    return Array.from({ length: 36 }, (_, offset) => (touchdown + offset) % 48);
}

function schedule(sourceVideoSha256) {
    const touchdowns = { hind_left: 0, fore_left: 12, hind_right: 24, fore_right: 36 };
    const contacts = FOOT_ORDER.map((foot) => ({
        anchor_id: `${foot}.hoof`,
        frames: contactFrames(touchdowns[foot]),
        weight: 1,
    }));
    const support = Array.from({ length: 48 }, (_, frame) => (
        contacts.filter((contact) => contact.frames.includes(frame)).length
    ));
    return {
        schema: 'autorig-browser-hoof-contact-schedule.v1',
        status: 'PASS',
        frameCount: 49,
        uniqueFrameCount: 48,
        fps: 30,
        loop: true,
        footOrderContract: [...FOOT_ORDER],
        inferredTouchdownOrder: [...FOOT_ORDER],
        touchdownGaps: FOOT_ORDER.map((foot, index) => ({
            from: foot,
            to: FOOT_ORDER[(index + 1) % 4],
            frames: 12,
            phase: 0.25,
        })),
        contacts,
        feet: Object.fromEntries(FOOT_ORDER.map((foot) => [foot, {
            touchdownFrame: touchdowns[foot],
            liftoffFrame: (touchdowns[foot] + 36) % 48,
            dutyFactor: 0.75,
            characteristicHeightPx: 160,
            contactFrames: cyclicContactFrames(touchdowns[foot]),
            failures: [],
        }])),
        qa: {
            failures: [],
            support: {
                minimum: Math.min(...support),
                maximum: Math.max(...support),
                fourSupportFrames: support.filter((count) => count === 4).length,
                byFrame: support,
            },
            thresholds: {
                minimumDutyFactor: 0.38,
                maximumDutyFactor: 0.80,
                minimumSupportFeet: 3,
                maximumFourSupportFrames: 3,
                maximumFittedContactSlideRatio: 0.002,
                minimumTouchdownGapPhase: 0.10,
                maximumTouchdownGapPhase: 0.40,
            },
            sourceVideoSha256,
            segmenterBackend: 'facebookresearch-sam2.1-video',
        },
    };
}

function fixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'autorig-contact-refit-'));
    const bundleDirectory = path.join(root, 'bundle');
    const skeleton = write(path.join(bundleDirectory, 'skeleton.json'), '{"armatures":[]}\n');
    const anchors = write(path.join(bundleDirectory, 'surface_anchors.json'), '{"bones":[]}\n');
    const sourceModelSha256 = 'a'.repeat(64);
    const bundle = {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: { filename: 'horse.glb', sha256: sourceModelSha256 },
        camera: {
            resolution: [768, 448],
            intrinsics: { fx: 600, fy: 600, cx: 384, cy: 224 },
            camera_to_world: Array(16).fill(0),
            world_to_camera: Array(16).fill(0),
        },
        artifacts: {
            skeleton: { filename: path.basename(skeleton.path), bytes: skeleton.bytes, sha256: skeleton.sha256 },
            surface_anchors: { filename: path.basename(anchors.path), bytes: anchors.bytes, sha256: anchors.sha256 },
        },
    };
    const bundleFile = writeJson(path.join(bundleDirectory, 'fitting_bundle.json'), bundle);
    const immutableRows = [skeleton, anchors, bundleFile].map((row) => ({
        filename: path.basename(row.path), bytes: row.bytes, sha256: row.sha256,
    }));
    const immutable = {
        schema: 'autorig-fitting-immutable-copy.v1',
        source_model: { sha256: sourceModelSha256 },
        bundle_file_count: immutableRows.length,
        bundle_total_bytes: immutableRows.reduce((sum, row) => sum + row.bytes, 0),
        bundle_manifest: { filename: 'fitting_bundle.json', sha256: bundleFile.sha256 },
        files: immutableRows,
    };
    const immutableFile = writeJson(path.join(bundleDirectory, 'immutable_manifest.json'), immutable);
    const video = write(path.join(root, 'source.mp4'), 'pinned source video');

    const semanticIds = FOOT_ORDER.flatMap((foot) => ['proximal', 'joint', 'hoof'].map((role) => `${foot}.${role}`));
    const mappings = semanticIds.map((semanticId, index) => ({
        limb: semanticId.split('.')[0],
        semanticAnchorId: semanticId,
        sourceTrackId: `tap-${index}`,
        sourceAnchorId: `bone-${index}:${index}`,
        sourceBone: `bone-${index}`,
    }));
    const raw = {
        schema: 'autorig-fitting-observations.v1',
        frame_count: 49,
        width: 768,
        height: 448,
        fps: 30,
        tracks: mappings.map((mapping, index) => ({
            id: mapping.sourceTrackId,
            anchor_id: mapping.sourceAnchorId,
            query_frame: 0,
            points: Array.from({ length: 49 }, (_, frame) => ({
                frame,
                x: 100 + index * 5,
                y: 200 + (mapping.semanticAnchorId.endsWith('.hoof') ? Math.sin(frame / 48 * Math.PI * 2) * 20 : 0),
                visible: true,
                confidence: 0.99,
            })),
        })),
        contacts: [],
        provenance: {
            source_video: video.path,
            source_video_sha256: video.sha256,
            bundle: bundleDirectory,
            bundle_sha256: bundleFile.sha256,
            immutable_manifest_sha256: immutableFile.sha256,
            tracker: { backend: 'google-deepmind-tapnextpp-online' },
            segmenter: { backend: 'facebookresearch-sam2.1-video' },
        },
    };
    const observations = writeJson(path.join(root, 'observations.json'), raw);
    const baseInputs = {
        bundleDirectory,
        observationsPath: observations.path,
        fittingBundleSha256: bundleFile.sha256,
        immutableManifestSha256: immutableFile.sha256,
        skeletonSha256: skeleton.sha256,
        surfaceAnchorsSha256: anchors.sha256,
        observationsSha256: observations.sha256,
        sourceVideoSha256: video.sha256,
        sourceModelSha256,
        bundleFileCount: immutableRows.length,
        bundleTotalBytes: immutable.bundle_total_bytes,
    };
    const bridgeValue = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        status: 'VALIDATED',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode: 'unconstrained_diagnostic',
        inputs: baseInputs,
        sourceContacts: 0,
        preparedContacts: 0,
        mappings,
    };
    const bridge = writeJson(path.join(root, 'bridge-report.json'), bridgeValue);
    const fitSummaryValue = {
        schema: 'autorig-browser-fit-canary-summary.v1',
        status: 'PASS_BROWSER_FIT_GATES',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode: 'unconstrained_diagnostic',
        approvedForBrowserContactFit: false,
        approvedForAnimationLibrary: false,
        inputs: baseInputs,
        observations: { contactCount: 0 },
        gates: {
            passed: true,
            results: [{ name: 'four_limb_contacts', passed: true, actual: 0, enforced: false }],
        },
    };
    const initialFit = writeJson(path.join(root, 'fit-summary.json'), fitSummaryValue);
    const diagnosticValue = {
        schema: 'autorig-browser-hoof-contact-diagnostic.v1',
        status: 'PASS',
        inputs: {
            observations,
            bridgeReport: bridge,
            sourceVideo: video,
            bundleManifest: bundleFile,
            immutableManifest: immutableFile,
            sourceSkeletonSha256: skeleton.sha256,
            sourceModelSha256,
            trackerBackend: 'google-deepmind-tapnextpp-online',
            segmenterBackend: 'facebookresearch-sam2.1-video',
            frames: 49,
            fps: 30,
            loop: true,
        },
        bridge: {
            semanticTracks: 12,
            hoofTracks: FOOT_ORDER.map((foot) => {
                const mapping = mappings.find((row) => row.semanticAnchorId === `${foot}.hoof`);
                return {
                    foot,
                    semanticId: mapping.semanticAnchorId,
                    sourceTrackId: mapping.sourceTrackId,
                    sourceAnchorId: mapping.sourceAnchorId,
                    sourceBone: mapping.sourceBone,
                };
            }),
        },
        schedule: schedule(video.sha256),
    };
    const diagnostic = writeJson(path.join(root, 'hoof-contact-diagnostic.json'), diagnosticValue);
    const manifestValue = {
        schema: 'autorig-browser-contact-refit-input.v1',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        inputs: {
            bundleDirectory: 'bundle',
            observations: { path: 'observations.json', bytes: observations.bytes, sha256: observations.sha256 },
            bridgeReport: { path: 'bridge-report.json', bytes: bridge.bytes, sha256: bridge.sha256 },
            initialFitSummary: { path: 'fit-summary.json', bytes: initialFit.bytes, sha256: initialFit.sha256 },
            contactDiagnostic: { path: 'hoof-contact-diagnostic.json', bytes: diagnostic.bytes, sha256: diagnostic.sha256 },
        },
        pins: {
            observationsSha256: observations.sha256,
            bridgeReportSha256: bridge.sha256,
            initialFitSummarySha256: initialFit.sha256,
            diagnosticSha256: diagnostic.sha256,
            sourceVideoSha256: video.sha256,
            fittingBundleSha256: bundleFile.sha256,
            immutableManifestSha256: immutableFile.sha256,
            sourceModelSha256,
            sourceSkeletonSha256: skeleton.sha256,
        },
    };
    const manifestPath = path.join(root, 'contact-refit-input.json');
    const manifest = writeJson(manifestPath, manifestValue);
    return {
        root, manifestPath, manifest, manifestValue, raw, bridgeValue, fitSummaryValue,
        diagnosticValue, paths: { diagnostic: diagnostic.path, initialFit: initialFit.path },
    };
}

function repinFile(value, field, filename, contents) {
    const integrity = writeJson(filename, contents);
    value.manifestValue.inputs[field].bytes = integrity.bytes;
    value.manifestValue.inputs[field].sha256 = integrity.sha256;
    const pinName = field === 'contactDiagnostic' ? 'diagnosticSha256' : 'initialFitSummarySha256';
    value.manifestValue.pins[pinName] = integrity.sha256;
    value.manifest = writeJson(value.manifestPath, value.manifestValue);
}

const FINAL_GATE_NAMES = [
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
];

function validGate(name) {
    if (name === 'target_sample_coverage') {
        return { name, passed: true, actual: 588, comparator: '>=', threshold: 588 };
    }
    if (name === 'ordered_deform_heads') {
        return { name, passed: true, actual: 'ordered_deform_heads', comparator: '===', threshold: 'ordered_deform_heads' };
    }
    if (name === 'four_limb_contacts') {
        return { name, passed: true, actual: 4, comparator: '===', threshold: 4, enforced: true };
    }
    if (name === 'three_clip_validate' || name === 'three_tracks_bound' || name === 'semantic_walk_gait') {
        return { name, passed: true, actual: true, comparator: '===', threshold: true };
    }
    if (name === 'pinned_contact_schedule') {
        return { name, passed: true, actual: 'PASS', comparator: '===', threshold: 'PASS' };
    }
    if (name === 'fitted_walk_contact_slide') {
        return { name, passed: true, actual: 0, comparator: '<=', threshold: 0.002 };
    }
    return { name, passed: true, actual: 0, comparator: '<=', threshold: 1 };
}

function writeValidFinalArtifacts(outputDirectory, { bundleDirectory, observationsPath, pins }) {
    fs.mkdirSync(outputDirectory, { recursive: true });
    const frameCount = 49;
    const fps = 30;
    const durationSeconds = (frameCount - 1) / fps;
    const times = Array.from({ length: frameCount }, (_, frame) => frame / fps);
    const bone = 'Horse_2_L_HindLeg_1';
    const quaternionTrack = {
        bone,
        name: `${bone}.quaternion`,
        type: 'quaternion',
        times,
        values: Array.from({ length: frameCount }, () => [0, 0, 0, 1]).flat(),
    };
    const positionTrack = {
        bone,
        name: `${bone}.position`,
        type: 'vector',
        times,
        values: Array.from({ length: frameCount }, () => [0, 0, 0]).flat(),
    };
    const qa = {
        targetSamples: 588,
        targetMode: 'ordered_deform_heads',
        initialMeanTargetErrorPx: 2,
        finalMeanTargetErrorPx: 1,
        maximumTargetErrorPx: 1,
        maximumBoneLengthErrorPx: 0,
        maximumJointLimitViolationRad: 0,
        maximumContactSlidePx: 0,
        loopEndpointError: 0,
    };
    const fitted = {
        schema: 'autorig-browser-fitted-animation.v1',
        frameCount,
        fps,
        durationSeconds,
        loop: true,
        tracks: [quaternionTrack],
        positionTracks: [positionTrack],
        rootTrack: null,
        qa,
        frames: Array.from({ length: frameCount }, (_, frame) => ({
            frame,
            limbs: Object.fromEntries(FOOT_ORDER.map((foot) => [foot, {
                points: [[0, 0], [1, 0], [2, 0]],
            }])),
        })),
    };
    const fitSummary = {
        schema: 'autorig-browser-fit-canary-summary.v1',
        status: 'PASS_BROWSER_CONTACT_REFIT_GATES',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode: 'contact_constrained_refit',
        approvedForBrowserContactFit: true,
        approvedForAnimationLibrary: false,
        approvalExclusions: ['fixed_camera_visual_phase_qa', 'target_mesh_deformation_qa'],
        inputs: {
            bundleDirectory,
            observationsPath,
            sourceVideoSha256: pins.sourceVideoSha256,
            fittingBundleSha256: pins.fittingBundleSha256,
            immutableManifestSha256: pins.immutableManifestSha256,
            sourceModelSha256: pins.sourceModelSha256,
            skeletonSha256: pins.sourceSkeletonSha256,
            observationsSha256: pins.observationsSha256,
        },
        observations: { contactCount: 4 },
        fit: {
            qa,
            frameCount,
            durationSeconds,
            quaternionTracks: 1,
            positionTracks: 1,
        },
        hierarchyClip: { name: 'Horse_LTX_Browser_Contact_Refit', tracks: 2 },
        gates: { passed: true, results: FINAL_GATE_NAMES.map(validGate) },
        contactRefit: {
            provenance: {
                schema: 'autorig-browser-contact-refit-provenance.v1',
                source: 'immutable_pass_diagnostic',
                browserOnly: true,
                blenderUsed: false,
                mixerUsed: false,
                ...pins,
            },
            scheduleStatus: 'PASS',
            semanticGaitQa: { accepted: true, simultaneousSwingFrameCount: 0 },
            fittedWalkQa: {
                status: 'PASS',
                failures: [],
                maximumContactSlideRatio: 0,
                thresholdRatio: 0.002,
            },
        },
    };
    const bridgeReport = {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        status: 'VALIDATED',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode: 'contact_constrained_refit',
        preparedContacts: 4,
        inputs: { ...fitSummary.inputs },
    };
    const threeClip = {
        name: fitSummary.hierarchyClip.name,
        duration: durationSeconds,
        uuid: '00000000-0000-4000-8000-000000000001',
        blendMode: 2500,
        tracks: [quaternionTrack, positionTrack].map(({ name, type, times: trackTimes, values }) => ({
            name, type, times: trackTimes, values,
        })),
    };
    const values = {
        bridgeReportPath: ['bridge-report.json', bridgeReport],
        fitSummaryPath: ['fit-summary.json', fitSummary],
        fittedAnimationPath: ['fitted-animation.json', fitted],
        threeClipPath: ['three-clip.json', threeClip],
    };
    const outputs = Object.fromEntries(Object.entries(values).map(([field, [filename, payload]]) => {
        const output = path.join(outputDirectory, filename);
        writeJson(output, payload);
        return [field, output];
    }));
    return { outputs, fitSummary, bridgeReport };
}

test('contact-refit CLI parser requires an externally pinned immutable manifest', () => {
    const parsed = parseContactRefitArgs([
        '--input-manifest', 'input.json',
        '--input-manifest-sha256', 'a'.repeat(64),
        '--three-module', 'three.module.js',
        '--output-dir', 'out',
        '--iterations', '80',
    ]);
    assert.equal(parsed.fit.iterations, 80);
    assert.throws(() => parseContactRefitArgs(['--input-manifest', 'x']), /missing required option/);
    assert.throws(() => parseContactRefitArgs([
        '--input-manifest', 'x', '--input-manifest-sha256', 'bad', '--three-module', 't', '--output-dir', 'o',
    ]), /lowercase SHA-256/);
});

test('immutable contact-refit chain cross-checks exact bundle, observation, bridge, initial-fit and diagnostic pins', (t) => {
    const value = fixture();
    t.after(() => fs.rmSync(value.root, { recursive: true, force: true }));
    const validated = validateContactRefitInputs({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
    });
    assert.equal(validated.schedule.status, 'PASS');
    assert.equal(validated.schedule.contacts.length, 4);
    assert.equal(validated.pins.inputManifestSha256, value.manifest.sha256);
    assert.equal(validated.pins.diagnosticSha256, value.manifestValue.pins.diagnosticSha256);

    fs.appendFileSync(value.paths.diagnostic, 'tamper');
    assert.throws(() => validateContactRefitInputs({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
    }), /byte count does not match|SHA-256 does not match/);
});

test('immutable JSON mutation during its single read fails closed before parsing or fitting', (t) => {
    const value = fixture();
    t.after(() => fs.rmSync(value.root, { recursive: true, force: true }));
    const diagnosticPath = path.resolve(value.paths.diagnostic);
    const originalReadFileSync = fs.readFileSync;
    let mutated = false;
    fs.readFileSync = function patchedReadFileSync(filename, ...args) {
        const result = originalReadFileSync.call(fs, filename, ...args);
        if (!mutated && typeof filename === 'string' && path.resolve(filename) === diagnosticPath) {
            mutated = true;
            fs.appendFileSync(diagnosticPath, ' ');
        }
        return result;
    };
    try {
        assert.throws(() => validateContactRefitInputs({
            inputManifestPath: value.manifestPath,
            expectedManifestSha256: value.manifest.sha256,
        }), /changed while its immutable bytes were read/);
        assert.equal(mutated, true);
    } finally {
        fs.readFileSync = originalReadFileSync;
    }
});

test('self-consistently repinned missing limb, bad support and diagnostic provenance still fail closed', (t) => {
    const missing = fixture();
    t.after(() => fs.rmSync(missing.root, { recursive: true, force: true }));
    missing.diagnosticValue.schedule.contacts.pop();
    repinFile(missing, 'contactDiagnostic', missing.paths.diagnostic, missing.diagnosticValue);
    assert.throws(() => validateContactRefitInputs({
        inputManifestPath: missing.manifestPath,
        expectedManifestSha256: missing.manifest.sha256,
    }), /exactly four limb contacts/);

    const support = fixture();
    t.after(() => fs.rmSync(support.root, { recursive: true, force: true }));
    support.diagnosticValue.schedule.qa.support.byFrame[0] = 0;
    repinFile(support, 'contactDiagnostic', support.paths.diagnostic, support.diagnosticValue);
    assert.throws(() => validateContactRefitInputs({
        inputManifestPath: support.manifestPath,
        expectedManifestSha256: support.manifest.sha256,
    }), /support timeline is inconsistent/);

    const provenance = fixture();
    t.after(() => fs.rmSync(provenance.root, { recursive: true, force: true }));
    provenance.diagnosticValue.inputs.observations.sha256 = 'f'.repeat(64);
    repinFile(provenance, 'contactDiagnostic', provenance.paths.diagnostic, provenance.diagnosticValue);
    assert.throws(() => validateContactRefitInputs({
        inputManifestPath: provenance.manifestPath,
        expectedManifestSha256: provenance.manifest.sha256,
    }), /diagnostic observations SHA-256/);
});

test('self-consistently repinned initial fit cannot become approved or contact-constrained', (t) => {
    const value = fixture();
    t.after(() => fs.rmSync(value.root, { recursive: true, force: true }));
    value.fitSummaryValue.approvedForAnimationLibrary = true;
    repinFile(value, 'initialFitSummary', value.paths.initialFit, value.fitSummaryValue);
    assert.throws(() => validateContactRefitInputs({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
    }), /must not carry approval/);
});

test('final runner forces four contacts and only accepts paired fitted-animation plus Three clip after PASS', async (t) => {
    const value = fixture();
    t.after(() => fs.rmSync(value.root, { recursive: true, force: true }));
    let invocation = null;
    const result = await runBrowserContactRefit({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
        threeModule: 'three.module.js',
        outputDirectory: path.join(value.root, 'output'),
        fit: {},
        gates: {},
    }, {
        runBrowserFitCanary: async (config, dependencies) => {
            invocation = { config, dependencies };
            const final = writeValidFinalArtifacts(config.outputDirectory, {
                bundleDirectory: config.bundleDirectory,
                observationsPath: config.observationsPath,
                pins: dependencies.contactRefit.pins,
            });
            return {
                passed: true,
                ...final,
            };
        },
    });
    assert.equal(result.passed, true);
    assert.equal(invocation.config.gates.requireFourLimbContacts, true);
    assert.equal(invocation.config.emitFittedAnimation, true);
    assert.equal(invocation.config.emitThreeClip, true);
    assert.equal(invocation.dependencies.contactRefit.schedule.contacts.length, 4);
    assert.equal(invocation.dependencies.contactRefit.pins.inputManifestSha256, value.manifest.sha256);
    assert.equal(fs.readdirSync(path.join(value.root, 'output')).length, 4);
    assert.equal(fs.readdirSync(value.root).some((name) => name.startsWith('output.staging-')), false);

    await assert.rejects(() => runBrowserContactRefit({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
        threeModule: 'three.module.js',
        outputDirectory: path.join(value.root, 'second-output'),
        fit: {},
        gates: {},
    }, {
        runBrowserFitCanary: async (config) => {
            fs.mkdirSync(config.outputDirectory, { recursive: true });
            const outputs = {};
            for (const [field, filename] of [
                ['bridgeReportPath', 'bridge-report.json'],
                ['fitSummaryPath', 'fit-summary.json'],
                ['fittedAnimationPath', 'fitted-animation.json'],
            ]) {
                outputs[field] = path.join(config.outputDirectory, filename);
                fs.writeFileSync(outputs[field], '{}\n');
            }
            return {
                passed: true,
                outputs,
                fitSummary: {
                    status: 'PASS_BROWSER_CONTACT_REFIT_GATES',
                    approvedForBrowserContactFit: true,
                    browserOnly: true,
                    blenderUsed: false,
                    mixerUsed: false,
                },
            };
        },
    }), /unexpected artifact set|missing staged three-clip\.json/);
    assert.equal(fs.existsSync(path.join(value.root, 'second-output')), false);

    await assert.rejects(() => runBrowserContactRefit({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
        threeModule: 'three.module.js',
        outputDirectory: path.join(value.root, 'gate-less-output'),
        fit: {},
        gates: {},
    }, {
        runBrowserFitCanary: async (config) => {
            fs.mkdirSync(config.outputDirectory, { recursive: true });
            const outputs = Object.fromEntries([
                ['bridgeReportPath', 'bridge-report.json'],
                ['fitSummaryPath', 'fit-summary.json'],
                ['fittedAnimationPath', 'fitted-animation.json'],
                ['threeClipPath', 'three-clip.json'],
            ].map(([field, filename]) => {
                const output = path.join(config.outputDirectory, filename);
                writeJson(output, {});
                return [field, output];
            }));
            return {
                passed: true,
                outputs,
                fitSummary: {
                    status: 'PASS_BROWSER_CONTACT_REFIT_GATES',
                    approvedForBrowserContactFit: true,
                    browserOnly: true,
                    blenderUsed: false,
                    mixerUsed: false,
                },
            };
        },
    }), /invalid final PASS contract/);
    assert.equal(fs.existsSync(path.join(value.root, 'gate-less-output')), false);

    await assert.rejects(() => runBrowserContactRefit({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
        threeModule: 'three.module.js',
        outputDirectory: path.join(value.root, 'wrong-job-output'),
        fit: {},
        gates: {},
    }, {
        runBrowserFitCanary: async (config, dependencies) => {
            const final = writeValidFinalArtifacts(config.outputDirectory, {
                bundleDirectory: config.bundleDirectory,
                observationsPath: config.observationsPath,
                pins: dependencies.contactRefit.pins,
            });
            final.fitSummary.contactRefit.provenance.diagnosticSha256 = 'f'.repeat(64);
            writeJson(final.outputs.fitSummaryPath, final.fitSummary);
            return { passed: true, ...final };
        },
    }), /final contact-refit provenance\.diagnosticSha256/);
    assert.equal(fs.existsSync(path.join(value.root, 'wrong-job-output')), false);
});

test('failed runner leaves final output absent and removes every staged diagnostic artifact', async (t) => {
    const value = fixture();
    t.after(() => fs.rmSync(value.root, { recursive: true, force: true }));
    const finalOutput = path.join(value.root, 'failed-output');
    const result = await runBrowserContactRefit({
        inputManifestPath: value.manifestPath,
        expectedManifestSha256: value.manifest.sha256,
        threeModule: 'three.module.js',
        outputDirectory: finalOutput,
        fit: {},
        gates: {},
    }, {
        runBrowserFitCanary: async (config) => {
            fs.mkdirSync(config.outputDirectory, { recursive: true });
            fs.writeFileSync(path.join(config.outputDirectory, 'bridge-report.json'), '{"status":"FAIL"}\n');
            return { passed: false, outputs: { bridgeReportPath: path.join(config.outputDirectory, 'bridge-report.json') }, fitSummary: { status: 'FAIL_BROWSER_CONTACT_REFIT_GATES' } };
        },
    });
    assert.equal(result.passed, false);
    assert.deepEqual(result.outputs, {});
    assert.equal(fs.existsSync(finalOutput), false);
    assert.equal(fs.readdirSync(value.root).some((name) => name.startsWith('failed-output.staging-')), false);
});

test('help explicitly proves browser-only, Blender=false and mixer=false runtime', async () => {
    let stdout = '';
    let stderr = '';
    const exitCode = await runCli(['--help'], {
        stdout: { write: (value) => { stdout += value; } },
        stderr: { write: (value) => { stderr += value; } },
    });
    assert.equal(exitCode, 0);
    assert.equal(stderr, '');
    assert.match(stdout, /browserOnly=true/);
    assert.match(stdout, /blenderUsed=false/);
    assert.match(stdout, /mixerUsed=false/);
    const source = fs.readFileSync(new URL('../../../tools/animation_fitting/browser_contact_refit.mjs', import.meta.url), 'utf8');
    assert.doesNotMatch(source, /new\s+THREE\.AnimationMixer|\.clipAction\s*\(/);
});

import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import zlib from 'node:zlib';

import {
    HORSE_VISUAL_PHASE_QA_SCHEMA,
    buildHorseVisualPhaseEvidence,
    measureHorse2Deformation,
    measureHorseOneShotFinalPose,
    parseHorseVisualPhaseQaArgs,
    renderHorse2QaFramesInBrowser,
    runHorseVisualPhaseQa,
    validateHorse2QaInputs,
} from '../browser_horse_visual_phase_qa.mjs';

const IDENTITY = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1];
const sha256 = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex');
const jsonBuffer = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');

function deformationFixture() {
    const rest = [
        [0, 0, 0],
        [0, 0, 0],
        [1, 0, 0],
        [0, 1, 0],
        [1, 1, 0],
    ];
    const skinWeights = {
        vertices: rest.map((world, vertex_index) => ({
            vertex_id: vertex_index,
            vertex_index,
            world,
            local: world,
            weights: [{ bone: 'Bone_001', weight: 1 }],
        })),
    };
    const topology = {
        faces: [
            { vertex_ids: [0, 2, 3] },
            { vertex_ids: [1, 3, 4] },
        ],
    };
    const frames = Array.from({ length: 5 }, (_, frameIndex) => ({
        frameIndex,
        timeSeconds: frameIndex / 30,
        positions: rest.map((position, vertexIndex) => vertexIndex === 1 ? [0.03, 0, 0] : [...position]),
        rootMotionLocked: true,
        cameraStatic: true,
    }));
    return { skinWeights, topology, frames };
}

function writeBundleFixture() {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'horse-final-qa-'));
    const bundleDirectory = path.join(root, 'bundle');
    fs.mkdirSync(bundleDirectory);
    const bones = Array.from({ length: 304 }, (_, index) => ({
        name: `Bone_${String(index).padStart(3, '0')}`,
        parent: index === 0 ? null : 'Bone_000',
        use_deform: index !== 0,
        helper: index === 0,
        head_local: [0, 0, 0],
        tail_local: [0, 0, 0.01],
        matrix_local: [...IDENTITY],
        parent_relative_matrix: [...IDENTITY],
        length: 0.01,
        rotation_mode: 'XYZ',
        joint_limits: [],
    }));
    const skeleton = { armatures: [{ name: 'Horse_2', matrix_world: [...IDENTITY], bones }] };
    const vertices = Array.from({ length: 344 }, (_, index) => {
        const world = [(index % 20) / 10, (Math.floor(index / 20) % 20) / 10, Math.floor(index / 400)];
        return {
            vertex_id: index,
            vertex_index: index,
            object: 'Horse_geo',
            local: world,
            world,
            weights: [{ bone: 'Bone_001', weight: 1 }],
        };
    });
    const faces = Array.from({ length: 258 }, (_, index) => ({
        face_id: index + 1,
        object: 'Horse_geo',
        polygon_index: index,
        vertex_ids: [index % 342, index % 342 + 1, index % 342 + 2],
    }));
    const rawFiles = new Map([
        ['skeleton.json', jsonBuffer(skeleton)],
        ['skin_weights.json.gz', zlib.gzipSync(jsonBuffer({ vertices }))],
        ['surface_topology.json.gz', zlib.gzipSync(jsonBuffer({ faces }))],
    ]);
    const artifact = (filename) => ({ filename, bytes: rawFiles.get(filename).length, sha256: sha256(rawFiles.get(filename)) });
    const fittingBundle = {
        schema: 'autorig-actionless-fitting-bundle.v1',
        revision: 'test',
        source: { filename: 'Horse_2.blend', sha256: 'a'.repeat(64), species: 'horse', rig_type: 'HORSE_2', orientation: 'canonical' },
        actionless: { detached_actions: [], muted_nla_tracks: 0, reset_pose_bones: 304, actionless: true },
        camera: {
            name: 'fixed',
            resolution: [768, 448],
            intrinsics: { fx: 1000, fy: 1000, cx: 384, cy: 224 },
            camera_to_world: [...IDENTITY],
            world_to_camera: [...IDENTITY],
        },
        ground_plane: { normal: [0, 0, 1], height: 0 },
        counts: { meshes: 1, armatures: 1, vertices: 344, faces: 258 },
        artifacts: {
            skeleton: artifact('skeleton.json'),
            skin_weights: artifact('skin_weights.json.gz'),
            surface_topology: artifact('surface_topology.json.gz'),
        },
    };
    rawFiles.set('fitting_bundle.json', jsonBuffer(fittingBundle));
    for (const [filename, buffer] of rawFiles) fs.writeFileSync(path.join(bundleDirectory, filename), buffer);
    const rows = [...rawFiles].map(([filename, buffer]) => ({ filename, bytes: buffer.length, sha256: sha256(buffer) }));
    const immutable = {
        schema: 'autorig-fitting-immutable-copy.v1',
        bundle_file_count: rows.length,
        bundle_total_bytes: rows.reduce((total, row) => total + row.bytes, 0),
        source_model: { filename: 'Horse_2.blend', sha256: 'a'.repeat(64), copied: false },
        bundle_manifest: { filename: 'fitting_bundle.json', sha256: sha256(rawFiles.get('fitting_bundle.json')) },
        files: rows,
    };
    const immutableBuffer = jsonBuffer(immutable);
    fs.writeFileSync(path.join(bundleDirectory, 'immutable_manifest.json'), immutableBuffer);
    const times = Array.from(new Float32Array(Array.from({ length: 49 }, (_, index) => index / 30)));
    const clip = {
        name: 'Horse_Walk_BrowserFit',
        duration: 48 / 30,
        uuid: 'test',
        blendMode: 2500,
        tracks: [
            { name: 'Bone_001.quaternion', type: 'quaternion', times, values: times.flatMap(() => [0, 0, 0, 1]) },
            {
                name: 'Bone_001.position',
                type: 'vector',
                times,
                values: times.flatMap((_, index) => [0.01 * Math.sin(Math.PI * index / 48) ** 2, 0, 0]),
            },
        ],
    };
    const threeClipPath = path.join(root, 'three-clip.json');
    fs.writeFileSync(threeClipPath, jsonBuffer(clip));
    return {
        root,
        bundleDirectory,
        threeClipPath,
        clipSha256: sha256(jsonBuffer(clip)),
        expectedImmutableManifestSha256: sha256(immutableBuffer),
        expectedFittingBundleSha256: sha256(rawFiles.get('fitting_bundle.json')),
        expectedSourceModelSha256: 'a'.repeat(64),
    };
}

test('CLI requires pinned local browser inputs and accepts help without values', () => {
    assert.deepEqual(parseHorseVisualPhaseQaArgs(['--help']), { help: true });
    assert.throws(() => parseHorseVisualPhaseQaArgs(['--bundle-dir', 'bundle']), /missing required option expectedImmutableManifestSha256/);
    const parsed = parseHorseVisualPhaseQaArgs([
        '--bundle-dir', 'bundle', '--immutable-manifest-sha256', '1'.repeat(64),
        '--fitting-bundle-sha256', '2'.repeat(64), '--source-model-sha256', '3'.repeat(64),
        '--three-clip', 'clip.json', '--three-clip-sha256', 'a'.repeat(64),
        '--semantic-id', 'walk_forward', '--three-module', 'three.js', '--chrome', 'chrome.exe',
        '--three-module-sha256', 'b'.repeat(64), '--three-revision', '160',
        '--ffmpeg', 'ffmpeg.exe', '--ffprobe', 'ffprobe.exe', '--output-dir', 'out',
    ]);
    assert.equal(parsed.semanticId, 'walk_forward');
    assert.equal(parsed.expectedThreeClipSha256, 'a'.repeat(64));
    assert.equal(parsed.expectedImmutableManifestSha256, '1'.repeat(64));
    assert.equal(parsed.expectedThreeModuleSha256, 'b'.repeat(64));
    assert.equal(parsed.expectedThreeRevision, '160');
    assert.equal(parseHorseVisualPhaseQaArgs([
        '--bundle-dir', 'bundle', '--immutable-manifest-sha256', '1'.repeat(64),
        '--fitting-bundle-sha256', '2'.repeat(64), '--source-model-sha256', '3'.repeat(64),
        '--three-clip', 'clip.json', '--three-clip-sha256', 'a'.repeat(64),
        '--semantic-id', 'death_fall', '--three-module', 'three.js', '--chrome', 'chrome.exe',
        '--three-module-sha256', 'b'.repeat(64), '--three-revision', '160',
        '--ffmpeg', 'ffmpeg.exe', '--ffprobe', 'ffprobe.exe', '--output-dir', 'out', '--one-shot',
    ]).loop, false);
});

test('immutable Horse_2 and fitted Three clip validation pins every byte', (context) => {
    const fixture = writeBundleFixture();
    context.after(() => fs.rmSync(fixture.root, { recursive: true, force: true }));
    const validated = validateHorse2QaInputs(fixture);
    assert.equal(validated.skinWeights.vertices.length, 344);
    assert.equal(validated.skeleton.armatures[0].bones.length, 304);
    assert.equal(validated.clipContract.frameCount, 49);
    assert.equal(validated.clipContract.durationTimelineTolerance, (48 / 30) * (2 ** -23));
    assert.equal(validated.clipContract.pin.sha256, fixture.clipSha256);
    assert.throws(() => validateHorse2QaInputs({
        ...fixture,
        expectedImmutableManifestSha256: '0'.repeat(64),
    }), /externally supplied SHA-256/);
    assert.throws(() => validateHorse2QaInputs({
        ...fixture,
        expectedSourceModelSha256: '0'.repeat(64),
    }), /model provenance/);
    fs.appendFileSync(path.join(fixture.bundleDirectory, 'skeleton.json'), 'tamper');
    assert.throws(() => validateHorse2QaInputs(fixture), /does not match its immutable pin/);
});

test('one-shot validation preserves chronology without requiring loop endpoint closure', (context) => {
    const fixture = writeBundleFixture();
    context.after(() => fs.rmSync(fixture.root, { recursive: true, force: true }));
    const clip = JSON.parse(fs.readFileSync(fixture.threeClipPath, 'utf8'));
    clip.tracks[1].values[clip.tracks[1].values.length - 3] = 0.5;
    const buffer = jsonBuffer(clip);
    fs.writeFileSync(fixture.threeClipPath, buffer);
    const changed = { ...fixture, clipSha256: sha256(buffer) };
    assert.throws(() => validateHorse2QaInputs(changed), /loop endpoint error/);
    const validated = validateHorse2QaInputs({ ...changed, expectedLoop: false });
    assert.equal(validated.clipContract.loop, false);
    assert.equal(validated.clipContract.temporalMode, 'one_shot');
    assert.ok(validated.clipContract.maximumLoopEndpointError > 0.49);
});

test('real non-identity Horse_2 hierarchy reconstructs every head and animates a non-root bone in Chrome r160', async (context) => {
    const bundleDirectory = 'R:/ComfyUI-data/autorig-fitting/horse-canonical-f1';
    if (!fs.existsSync(path.join(bundleDirectory, 'immutable_manifest.json'))) {
        context.skip('canonical Horse_2 f1 bundle is not installed');
        return;
    }
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'horse-final-real-bundle-'));
    context.after(() => fs.rmSync(root, { recursive: true, force: true }));
    const sourceSkeleton = JSON.parse(fs.readFileSync(path.join(bundleDirectory, 'skeleton.json'), 'utf8'));
    const animatedBone = sourceSkeleton.armatures[0].bones.find((bone) => bone.name === 'thigh.l');
    assert.ok(animatedBone && animatedBone.parent);
    const restPosition = [
        animatedBone.parent_relative_matrix[3],
        animatedBone.parent_relative_matrix[7],
        animatedBone.parent_relative_matrix[11],
    ];
    const times = Array.from({ length: 49 }, (_, index) => index / 30);
    const clip = {
        name: 'Horse_Walk_QA_Input_Pin',
        duration: 48 / 30,
        uuid: 'pin-only',
        blendMode: 2500,
        tracks: [
            {
                name: 'thigh.l.position',
                type: 'vector',
                times,
                values: times.flatMap((_, index) => [
                    restPosition[0] + 0.015 * Math.sin(Math.PI * index / 48) ** 2,
                    restPosition[1],
                    restPosition[2],
                ]),
            },
        ],
    };
    const threeClipPath = path.join(root, 'three-clip.json');
    fs.writeFileSync(threeClipPath, jsonBuffer(clip));
    const validated = validateHorse2QaInputs({
        bundleDirectory,
        threeClipPath,
        expectedImmutableManifestSha256: 'f5e55c5073d09bc01dac90f4b7244f995fd42b0bdd37e09258cd4178e5573873',
        expectedFittingBundleSha256: 'e328fae0fd850a38249fb8b40c2e2766e8d90ab1ce4c1f241e926e9230d23744',
        expectedSourceModelSha256: 'fa75772d83c2613ddd6df6f7a305a407e12abf4a75c9083bb53df4d2619f50a1',
    });
    assert.equal(validated.fittingBundlePin.sha256, 'e328fae0fd850a38249fb8b40c2e2766e8d90ab1ce4c1f241e926e9230d23744');
    assert.equal(validated.skeletonPin.sha256, '0e7fb527d4df5273c289a61a2bbb1f456d9cd10f83d2b09cbbea05daade6f8be');
    assert.equal(validated.skinWeightsPin.sha256, '69ad7534f8d48ce8207a7e56d1b59988d58665385f3ecf42f4cf463a3de45df6');
    assert.equal(validated.topologyPin.sha256, '2c8e68f829ba57cf5b64e4d85b4b2c717c71b07e2f1fe72fc9f827783a3adc04');
    assert.equal(validated.zeroWeightVertices, 0);
    const chrome = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
    const threeModule = 'R:/ComfyUI-data/autorig-fitting/runtimes/three-r160/three.module.js';
    if (!fs.existsSync(chrome) || !fs.existsSync(threeModule)) {
        context.skip('local pinned Chrome/Three runtime is not installed');
        return;
    }
    const rendered = await renderHorse2QaFramesInBrowser({
        chromeExecutable: chrome,
        threeModule,
        expectedThreeModuleSha256: '76dea8151bc9352aef3528b4262e249b2604f62543828328db978d060d61a495',
        expectedThreeRevision: '160',
        validated,
    });
    context.diagnostic(JSON.stringify({
        maximumHeadReconstructionErrorWorld: rendered.runtimeReport.maximumHeadReconstructionErrorWorld,
        maximumRestVertexErrorWorld: rendered.runtimeReport.maximumRestVertexErrorWorld,
        animatedNonRootBoneNames: rendered.runtimeReport.animatedNonRootBoneNames,
        maximumAnimatedBoneHeadDisplacementWorld: rendered.runtimeReport.maximumAnimatedBoneHeadDisplacementWorld,
        threeRevision: rendered.runtimeReport.threeRevision,
        threeModuleSha256: rendered.threeModulePin.sha256,
    }));
    assert.ok(rendered.runtimeReport.maximumHeadReconstructionErrorWorld <= 1e-5);
    assert.ok(rendered.runtimeReport.maximumRestVertexErrorWorld <= 1e-5);
    assert.ok(rendered.runtimeReport.animatedNonRootBoneNames.includes('thigh.l'));
    assert.ok(rendered.runtimeReport.maximumAnimatedBoneHeadDisplacementWorld > 0.01);
    const inconsistent = { ...validated, skeleton: structuredClone(validated.skeleton) };
    inconsistent.skeleton.armatures[0].bones.find((bone) => bone.name === 'thigh.l').head_local[0] += 0.01;
    await assert.rejects(() => renderHorse2QaFramesInBrowser({
        chromeExecutable: chrome,
        threeModule,
        expectedThreeModuleSha256: '76dea8151bc9352aef3528b4262e249b2604f62543828328db978d060d61a495',
        expectedThreeRevision: '160',
        validated: inconsistent,
    }), /bone-head hierarchy reconstruction drifted/);
});

test('all-frame target deformation gate measures stretch, zero weights and coincident-rest separation', () => {
    const fixture = deformationFixture();
    const report = measureHorse2Deformation(fixture);
    assert.equal(report.measuredEveryFrame, true);
    assert.equal(report.frameCount, 5);
    assert.equal(report.zeroWeightVertices, 0);
    assert.equal(report.coincidentRestGroupCount, 1);
    assert.equal(report.maximumCoincidentRestSeparationM, 0.03);
    assert.equal(report.passed, true);

    const stretched = structuredClone(fixture);
    stretched.frames[2].positions[2] = [10, 0, 0];
    const stretchReport = measureHorse2Deformation(stretched);
    assert.ok(stretchReport.maximumEdgeStretch > 5);
    assert.equal(stretchReport.gates.maximumEdgeStretch, false);
    assert.equal(stretchReport.passed, false);

    const separated = structuredClone(fixture);
    separated.frames[3].positions[1] = [0.041, 0, 0];
    const separationReport = measureHorse2Deformation(separated);
    assert.equal(separationReport.gates.coincidentRestSeparation, false);
    assert.equal(separationReport.passed, false);
});

test('one-shot final pose gate requires a settled lowered body at the ground plane', () => {
    const rest = [
        [-1, -1, 0], [1, -1, 0], [-1, 1, 0], [1, 1, 0],
        [-1, -1, 2], [1, -1, 2], [-1, 1, 2], [1, 1, 2],
    ];
    const settled = rest.map((position, index) => index < 4 ? [...position] : [position[0], position[1], 0.2]);
    const frames = Array.from({ length: 5 }, (_, frameIndex) => ({
        frameIndex,
        positions: frameIndex < 2 ? rest.map((position) => [...position]) : settled.map((position) => [...position]),
        cameraStatic: true,
    }));
    const skinWeights = { vertices: rest.map((world) => ({ world })) };
    const report = measureHorseOneShotFinalPose({ skinWeights, frames, groundHeight: 0 });
    assert.equal(report.passed, true);
    assert.deepEqual(report.finalWindowFrames, [2, 3, 4]);
    assert.ok(report.centroidDropM > report.resolvedThresholds.minimumCentroidDropM);
    const moving = structuredClone(frames);
    moving[4].positions[7][0] += 1;
    assert.equal(measureHorseOneShotFinalPose({ skinWeights, frames: moving, groundHeight: 0 }).passed, false);
    const standing = structuredClone(frames);
    standing.forEach((frame) => { frame.positions = rest.map((position) => [...position]); });
    const standingReport = measureHorseOneShotFinalPose({ skinWeights, frames: standing, groundHeight: 0 });
    assert.equal(standingReport.gates.centroidDrop, false);
    assert.equal(standingReport.passed, false);
});

test('visual phase evidence is pinned but cannot approve without a human decision', () => {
    const deformationReport = measureHorse2Deformation(deformationFixture());
    const pin = (digit, extra = {}) => ({ path: `C:/qa/${digit}`, bytes: 123, sha256: digit.repeat(64), ...extra });
    const validated = {
        fittingBundle: { source: { filename: 'Horse_2.blend', sha256: 'd'.repeat(64) } },
        clipContract: { frameCount: 49, pin: pin('1') },
        immutablePin: pin('2'),
        fittingBundlePin: pin('3'),
        skeletonPin: pin('4'),
        skinWeightsPin: pin('5'),
        topologyPin: pin('6'),
    };
    const phaseFramePins = [
        pin('7', { phase: 'start', frameIndex: 0 }),
        pin('8', { phase: 'middle', frameIndex: 24 }),
        pin('9', { phase: 'three_quarter', frameIndex: 36 }),
    ];
    const evidence = buildHorseVisualPhaseEvidence({
        semanticId: 'walk_forward',
        validated,
        deformationReport,
        deformationReportPin: pin('a'),
        phaseFramePins,
        videoPin: pin('b', { frame_count: 49 }),
        renderer: { browser: 'test' },
        cameraSettingsPin: pin('c'),
    });
    assert.equal(evidence.schema, 'autorig.browser-horse-visual-phase-evidence-envelope.v1');
    const gate = evidence.visual_phase_gate;
    assert.equal(gate.schema, HORSE_VISUAL_PHASE_QA_SCHEMA);
    assert.deepEqual(Object.keys(gate).sort(), [
        'schema', 'version', 'rig_type', 'semantic_id', 'fitted_clip_sha256', 'decision',
        'camera', 'coincident_rest_vertex_separation', 'required_phases', 'frames', 'reviewer',
    ].sort());
    assert.deepEqual(Object.keys(gate.camera).sort(), [
        'static', 'projection', 'view', 'root_motion_locked', 'settings_sha256',
    ].sort());
    assert.deepEqual(Object.keys(gate.coincident_rest_vertex_separation).sort(), [
        'measured', 'pass', 'threshold_m', 'max_separation_m', 'sample_count', 'group_count',
        'report_url', 'report_sha256',
    ].sort());
    gate.frames.forEach((frame) => assert.deepEqual(Object.keys(frame).sort(), [
        'phase', 'frame_index', 'evidence_url', 'sha256',
    ].sort()));
    assert.deepEqual(Object.keys(gate.reviewer).sort(), ['id', 'reviewed_at']);
    assert.equal(gate.decision, null);
    assert.equal(gate.coincident_rest_vertex_separation.report_url, null);
    assert.ok(gate.frames.every((frame) => frame.evidence_url === null));
    assert.equal(gate.reviewer.id, null);
    assert.equal(evidence.local_evidence.human_review.decision, null);
    assert.equal(evidence.local_evidence.approvals.machine_qa_passed, true);
    assert.equal(evidence.local_evidence.approvals.ready_for_human_review, true);
    assert.equal(evidence.local_evidence.approvals.approved_for_animation_library, false);
    assert.equal(evidence.local_evidence.approvals.release_ready, false);
    assert.equal(evidence.local_evidence.approvals.fail_closed_reason, 'human_visual_phase_decision_and_public_urls_unset');
    assert.throws(() => buildHorseVisualPhaseEvidence({
        semanticId: 'walk_forward', validated, deformationReport, deformationReportPin: pin('a'),
        phaseFramePins: [...phaseFramePins].reverse(), videoPin: pin('b'), renderer: {}, cameraSettingsPin: pin('c'),
    }), /phase-frame pins are incomplete/);
});

test('local Chrome r160 reconstructs Horse_2, renders every frame, and ffmpeg emits pinned no-audio MP4', async (context) => {
    const chrome = 'C:/Program Files/Google/Chrome/Application/chrome.exe';
    const threeModule = 'R:/ComfyUI-data/autorig-fitting/runtimes/three-r160/three.module.js';
    const ffmpeg = 'C:/API/ffmpeg/bin/ffmpeg.exe';
    const ffprobe = 'C:/API/ffmpeg/bin/ffprobe.exe';
    if (![chrome, threeModule, ffmpeg, ffprobe].every((filename) => fs.existsSync(filename))) {
        context.skip('local pinned Chrome/Three/ffmpeg runtime is not installed');
        return;
    }
    const fixture = writeBundleFixture();
    context.after(() => fs.rmSync(fixture.root, { recursive: true, force: true }));
    const outputDirectory = path.join(fixture.root, 'qa-output');
    const threeModuleSha256 = '76dea8151bc9352aef3528b4262e249b2604f62543828328db978d060d61a495';
    const baseConfig = {
        bundleDirectory: fixture.bundleDirectory,
        expectedImmutableManifestSha256: fixture.expectedImmutableManifestSha256,
        expectedFittingBundleSha256: fixture.expectedFittingBundleSha256,
        expectedSourceModelSha256: fixture.expectedSourceModelSha256,
        threeClipPath: fixture.threeClipPath,
        expectedThreeClipSha256: fixture.clipSha256,
        semanticId: 'walk_forward',
        threeModule,
        expectedThreeModuleSha256: threeModuleSha256,
        expectedThreeRevision: '160',
        chromeExecutable: chrome,
        ffmpeg,
        ffprobe,
        outputDirectory,
    };
    await assert.rejects(() => runHorseVisualPhaseQa({
        ...baseConfig,
        expectedThreeModuleSha256: '0'.repeat(64),
    }), /Three module does not match/);
    assert.equal(fs.existsSync(outputDirectory), false);
    const result = await runHorseVisualPhaseQa(baseConfig);
    assert.equal(result.passedMachineQa, true);
    assert.equal(result.approvedForAnimationLibrary, false);
    assert.deepEqual(fs.readdirSync(outputDirectory).sort(), [
        'deformation-report.json',
        'fixed-camera-preview.mp4',
        'frames',
        'camera-settings.json',
        'visual-phase-qa.json',
    ].sort());
    assert.equal(fs.readdirSync(path.join(outputDirectory, 'frames')).length, 49);
    const evidence = JSON.parse(fs.readFileSync(result.evidencePath, 'utf8'));
    assert.equal(evidence.local_evidence.renderer.three_revision, '160');
    assert.equal(evidence.local_evidence.renderer.three_module.sha256, threeModuleSha256);
    assert.equal(evidence.local_evidence.immutable_inputs.source_model.sha256, fixture.expectedSourceModelSha256);
    assert.ok(evidence.local_evidence.browser_reconstruction_qa.maximum_bone_head_error_world <= 1e-5);
    assert.ok(evidence.local_evidence.browser_reconstruction_qa.animated_non_root_bones.includes('Bone_001'));
    assert.ok(evidence.local_evidence.browser_reconstruction_qa.maximum_animated_bone_head_displacement_world > 1e-6);
    assert.equal(evidence.local_evidence.video.frame_count, 49);
    assert.equal(evidence.local_evidence.video.audio_stream_count, 0);
    assert.equal(evidence.visual_phase_gate.camera.static, true);
    assert.equal(evidence.visual_phase_gate.camera.root_motion_locked, true);
    assert.equal(evidence.visual_phase_gate.decision, null);
    assert.equal(evidence.local_evidence.approvals.approved_for_animation_library, false);
    await assert.rejects(() => runHorseVisualPhaseQa(baseConfig), /outputDirectory must be absent or empty/);
});

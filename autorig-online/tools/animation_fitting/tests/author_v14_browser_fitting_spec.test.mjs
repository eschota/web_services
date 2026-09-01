import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    V14_CONTROLLED_GENERATION_SCHEMA,
    V14_PIPELINE_SPEC_SCHEMA_V2,
    V14_RUNTIME_PINS_SCHEMA,
    V14_TOOL_SOURCE_PINS_SCHEMA,
    REAL_V15_CONTRACT,
    authorV14PipelineSpec,
    buildV14PipelineSpec,
    parseAuthorArgs,
} from '../author_v14_browser_fitting_spec.mjs';
import { V14_PIPELINE_TOOL_SOURCE_PATHS } from '../run_v14_browser_fitting_pipeline.mjs';

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

function descriptor(pin) {
    return { path: pin.path, bytes: pin.bytes, sha256: pin.sha256 };
}

function stableValue(value) {
    if (Array.isArray(value)) return value.map(stableValue);
    if (value && typeof value === 'object') {
        return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stableValue(value[key])]));
    }
    return value;
}

function promptId(jobId) {
    const raw = digest(Buffer.from(`autorig-controlled-animation-fitting:${jobId}`)).slice(0, 32);
    return `${raw.slice(0, 8)}-${raw.slice(8, 12)}-4${raw.slice(13, 16)}-8${raw.slice(17, 20)}-${raw.slice(20)}`;
}

function fixture({ hardEndpointGuides = false } = {}) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'v14-spec-author-'));
    const canonicalRoot = path.join(root, 'horse-canonical-test');
    const reference = write(path.join(canonicalRoot, 'reference_rgb.png'), 'canonical-rgb');
    const skeleton = writeJson(path.join(canonicalRoot, 'skeleton.json'), { armatures: [] });
    const weights = write(path.join(canonicalRoot, 'skin_weights.json.gz'), 'skin-weights');
    const topology = write(path.join(canonicalRoot, 'surface_topology.json.gz'), 'surface-topology');
    const sourceModelSha256 = 'a'.repeat(64);
    const fitting = writeJson(path.join(canonicalRoot, 'fitting_bundle.json'), {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: {
            filename: 'Horse_2.blend', sha256: sourceModelSha256, species: 'horse',
            rig_type: 'HORSE_2', orientation: 'canonical',
        },
        actionless: { actionless: true },
        artifacts: {
            rgb: { filename: path.basename(reference.path), bytes: reference.bytes, sha256: reference.sha256 },
            skeleton: { filename: path.basename(skeleton.path), bytes: skeleton.bytes, sha256: skeleton.sha256 },
            skin_weights: { filename: path.basename(weights.path), bytes: weights.bytes, sha256: weights.sha256 },
            surface_topology: { filename: path.basename(topology.path), bytes: topology.bytes, sha256: topology.sha256 },
        },
    });
    const canonicalFiles = [fitting, reference, skeleton, weights, topology];
    const immutable = writeJson(path.join(canonicalRoot, 'immutable_manifest.json'), {
        schema: 'autorig-fitting-immutable-copy.v1',
        bundle_file_count: canonicalFiles.length,
        bundle_total_bytes: canonicalFiles.reduce((total, row) => total + row.bytes, 0),
        source_model: { filename: 'Horse_2.blend', sha256: sourceModelSha256, copied: false },
        bundle_manifest: { filename: path.basename(fitting.path), sha256: fitting.sha256 },
        files: canonicalFiles.map((row) => ({ filename: path.basename(row.path), bytes: row.bytes, sha256: row.sha256 })),
    });

    const guideRoot = path.join(root, 'horse-v14-guide-test');
    const guideFrames = Array.from({ length: 49 }, (_, frameIndex) => {
        const filename = `guide_${String(frameIndex).padStart(3, '0')}.png`;
        const pin = write(path.join(guideRoot, filename), `guide-${frameIndex}`);
        return { frame_index_int: frameIndex, filename_string: filename, bytes_int: pin.bytes, sha256_string: pin.sha256 };
    });
    const guideVideo = write(path.join(guideRoot, 'interval_guide.mkv'), 'lossless-interval-video');
    const pose = writeJson(path.join(guideRoot, 'pose_contract.json'), { browserOnly: true });
    const guideManifest = writeJson(path.join(guideRoot, 'immutable_manifest.json'), {
        schema: 'autorig-browser-ltx-interval-guide-bundle.v1',
        status: 'PASS', browserOnly: true, blenderUsed: false, rigType: 'HORSE_2',
        resolution: [768, 448], source_reference_sha256_string: reference.sha256,
        endpoint_guide_sha256_string: guideFrames[0].sha256_string,
        cycle_frame_count_int: 49, browser_frame_count_int: 49, guide_count_int: 1,
        frames_array: guideFrames,
        interval_guide_video_object: {
            filename: path.basename(guideVideo.path), bytes: guideVideo.bytes, sha256: guideVideo.sha256,
            width: 768, height: 448, frameRate: 30, frameCount: 49, audioStreamCount: 0,
        },
        poseContract: { filename: path.basename(pose.path), bytes: pose.bytes, sha256: pose.sha256 },
    });

    const binRoot = path.join(root, 'bin');
    const executables = Object.fromEntries(['python', 'node', 'chrome', 'ffmpeg', 'ffprobe'].map((name) => [
        name, write(path.join(binRoot, `${name}.exe`), `runtime-${name}`),
    ]));
    const three = write(path.join(root, 'three.module.js'), "export const REVISION='160';\n");
    const trackingRoot = path.join(root, 'tracking-runtime');
    fs.mkdirSync(trackingRoot);
    const trackingLock = writeJson(path.join(trackingRoot, 'runtime-lock.json'), { pinned: true });
    const runtimeManifest = writeJson(path.join(root, 'runtime-pins.json'), {
        schema: V14_RUNTIME_PINS_SCHEMA,
        executables: Object.fromEntries(Object.entries(executables).map(([name, pin]) => [name, descriptor(pin)])),
        threeModule: { ...descriptor(three), revision: '160' },
        trackingRuntimeRoot: trackingRoot,
        trackingRuntimeLock: descriptor(trackingLock),
    });
    const toolManifestValue = {
        schema: V14_TOOL_SOURCE_PINS_SCHEMA,
        sources: Object.fromEntries(Object.entries(V14_PIPELINE_TOOL_SOURCE_PATHS).map(([name, filename]) => {
            const buffer = fs.readFileSync(filename);
            return [name, { path: path.resolve(filename), bytes: buffer.length, sha256: digest(buffer) }];
        })),
    };
    let toolManifest = writeJson(path.join(root, 'tool-pins.json'), toolManifestValue);

    const candidatePayload = Buffer.from(hardEndpointGuides ? 'real-v15-candidate' : 'real-v14-candidate');
    const candidateDigest = digest(candidatePayload);
    const candidate = write(path.join(root, 'raw', candidateDigest.slice(0, 2), `${candidateDigest}.mp4`), candidatePayload);
    const frames = Array.from({ length: 49 }, (_, index) => (
        write(path.join(root, 'frames', candidate.sha256, `frame_${String(index).padStart(6, '0')}.png`), `candidate-frame-${index}`)
    ));
    const seedString = '6550110377254033429';
    const identity = {
        schema: 'autorig.animation-fitting-controlled-job-identity.v1',
        experiment_id_string: hardEndpointGuides ? 'horse_walk_v15_test_v1' : 'horse_walk_v14_test_v1',
        experiment_sha256_string: 'b'.repeat(64),
        runtime_authorization_string: hardEndpointGuides
            ? 'explicit_cli:horse_walk_v15_test_v1'
            : 'explicit_cli:horse_walk_v14_test_v1',
        reference_sha256_string: reference.sha256,
        positive_prompt_sha256_string: 'c'.repeat(64),
        negative_prompt_sha256_string: 'd'.repeat(64),
        seed_int: '__EXACT_SEED__',
        frame_count_int: 49, input_fps_int: 24, output_fps_int: 30,
        start_guide_strength_float: '__START_STRENGTH__', end_guide_strength_float: '__END_STRENGTH__',
        worker_id_string: 'local-4090', worker_base_url_string: 'http://127.0.0.1:8188',
        workflow_name_string: 'autorig_ltx2_animal_loop_v1_api.json',
        workflow_fingerprint_string: 'e'.repeat(64),
        approval_state_string: 'generated_not_approved', send_to_skeletal_fitting_bool: false,
        resolution_override_object: { latent_width_int: 768, latent_height_int: 448, resize_longer_int: 768 },
        browser_interval_guide_object: {
            guide_manifest_sha256_string: guideManifest.sha256,
            video_sha256_string: guideVideo.sha256, video_bytes_int: guideVideo.bytes,
            frame_count_int: 49, width_int: 768, height_int: 448, fps_int: 30,
            strength_float: '__INTERVAL_STRENGTH__', ltxv_add_guide_count_int: 1,
        },
        ...(hardEndpointGuides ? {
            browser_guide_sequence_object: {
                guide_manifest_sha256_string: guideManifest.sha256,
                frames_array: [0, 48].map((frameIndex) => ({
                    frame_index_int: frameIndex,
                    sha256_string: guideFrames[0].sha256_string,
                    strength_float: '__ENDPOINT_STRENGTH__',
                })),
            },
        } : {}),
    };
    const replaceExactNumbers = (payload) => payload
        .replace('"__EXACT_SEED__"', seedString)
        .replace('"__START_STRENGTH__"', '1.0')
        .replace('"__END_STRENGTH__"', '1.0')
        .replace('"__INTERVAL_STRENGTH__"', '1.0')
        .replaceAll('"__ENDPOINT_STRENGTH__"', '1.0');
    const identityCanonical = replaceExactNumbers(JSON.stringify(stableValue(identity)));
    const jobId = digest(Buffer.from(identityCanonical));
    const controlledPromptId = promptId(jobId);
    const jobValue = {
        ...identity,
        sequence_int: 3, recorded_at_unix_float: 1234.5, status_string: 'completed',
        prompt_id_string: controlledPromptId, resumed_existing_prompt_bool: false,
        raw_video_path_string: candidate.path, raw_video_sha256_string: candidate.sha256,
        raw_video_bytes_int: candidate.bytes, frame_count_int: 49,
        frame_paths_array: frames.map((row) => row.path), frame_sha256_array: frames.map((row) => row.sha256),
        backend_output_object: { filename_string: 'candidate.mp4', subfolder_string: 'animation_fitting\\controlled\\v14', type_string: 'output' },
    };
    const jobPath = path.join(root, 'jobs', jobId, '000003.json');
    const jobPayload = `${replaceExactNumbers(JSON.stringify(jobValue, null, 2))}\n`;
    let job = write(jobPath, jobPayload);
    const expected = {
        experimentId: identity.experiment_id_string,
        experimentSha256: identity.experiment_sha256_string,
        jobId,
        promptId: controlledPromptId,
        seed: BigInt(seedString),
        workerId: identity.worker_id_string,
        workerBaseUrl: identity.worker_base_url_string,
        workflowName: identity.workflow_name_string,
        workflowFingerprintSha256: identity.workflow_fingerprint_string,
        canonicalBundleName: path.basename(canonicalRoot),
        canonicalImmutableManifestSha256: immutable.sha256,
        fittingBundleSha256: fitting.sha256,
        sourceModelSha256,
        sourceReferenceSha256: reference.sha256,
        skeletonSha256: skeleton.sha256,
        guideBundleName: path.basename(guideRoot),
        guideManifestSha256: guideManifest.sha256,
        guideVideoSha256: guideVideo.sha256,
        guideVideoBytes: guideVideo.bytes,
        endpointGuideSha256: guideFrames[0].sha256_string,
        hardEndpointGuides,
        threeModuleSha256: three.sha256,
        frameCount: 49, inputFps: 24, outputFps: 30, width: 768, height: 448,
    };
    const outputRoot = path.join(root, 'pipeline-output');
    const config = {
        experimentId: identity.experiment_id_string,
        controlledJob: job.path, controlledJobSha256: job.sha256,
        canonicalBundle: canonicalRoot, canonicalImmutableSha256: immutable.sha256,
        fittingBundleSha256: fitting.sha256, sourceModelSha256,
        guideBundle: guideRoot, guideManifestSha256: guideManifest.sha256,
        guideVideo: guideVideo.path, guideVideoSha256: guideVideo.sha256, guideVideoBytes: guideVideo.bytes,
        runtimePins: runtimeManifest.path, runtimePinsSha256: runtimeManifest.sha256,
        toolSourcePins: toolManifest.path, toolSourcePinsSha256: toolManifest.sha256,
        outputRoot, output: path.join(root, 'pipeline-spec.json'),
    };
    return {
        root, config, expected, candidate, frames, job, jobValue, jobPath, jobPayload,
        guideVideo, guideManifest, canonicalRoot, skeleton, runtimeManifest,
        toolManifest, toolManifestValue,
        rewriteToolManifest(value) {
            toolManifest = writeJson(toolManifest.path, value);
            this.toolManifest = toolManifest;
            this.config.toolSourcePinsSha256 = toolManifest.sha256;
        },
        rewriteJob(value) {
            const payload = `${replaceExactNumbers(JSON.stringify(value, null, 2))}\n`;
            job = write(jobPath, payload);
            this.job = job;
            this.config.controlledJobSha256 = job.sha256;
        },
    };
}

function strictV2Inspector({ specPath, expectedSpecSha256 }) {
    const buffer = fs.readFileSync(specPath);
    assert.equal(digest(buffer), expectedSpecSha256);
    const spec = JSON.parse(buffer);
    assert.equal(spec.schema, V14_PIPELINE_SPEC_SCHEMA_V2);
    assert.equal(spec.browserOnly, true);
    assert.equal(spec.blenderUsed, false);
    assert.equal(spec.orchestratorExecutesSubprocesses, false);
    assert.equal(spec.controlledGeneration.schema, V14_CONTROLLED_GENERATION_SCHEMA);
    assert.deepEqual(spec.controlledGeneration.candidate, spec.candidate);
    assert.equal(spec.controlledGeneration.frames.length, 49);
    assert.deepEqual(spec.controlledGeneration.frames.map((row) => row.frameIndex), Array.from({ length: 49 }, (_, i) => i));
    assert.equal(Object.keys(spec.toolSources).length, 28);
    return { status: 'READY_OBJECT_REGION_GATE', next: { stage: 'object_region_gate' } };
}

test('CLI requires a completed controlled job and rejects the old direct-MP4 escape', () => {
    assert.deepEqual(parseAuthorArgs(['--help']), { help: true });
    assert.throws(() => parseAuthorArgs(['--candidate', 'x.mp4']), /unknown option --candidate/);
    assert.throws(() => parseAuthorArgs([]), /--controlled-job is required/);
});

test('CLI accepts only the exact code-owned V14 or V15 experiment ids', () => {
    const required = [
        '--controlled-job', 'job.json', '--controlled-job-sha256', '1'.repeat(64),
        '--canonical-bundle', 'canonical', '--canonical-immutable-sha256', '2'.repeat(64),
        '--fitting-bundle-sha256', '3'.repeat(64), '--source-model-sha256', '4'.repeat(64),
        '--guide-bundle', 'guide', '--guide-manifest-sha256', '5'.repeat(64),
        '--guide-video', 'guide.mkv', '--guide-video-sha256', '6'.repeat(64), '--guide-video-bytes', '1',
        '--runtime-pins', 'runtime.json', '--runtime-pins-sha256', '7'.repeat(64),
        '--tool-source-pins', 'tools.json', '--tool-source-pins-sha256', '8'.repeat(64),
        '--output-root', 'out', '--output', 'spec.json',
    ];
    assert.equal(parseAuthorArgs([
        '--experiment-id', REAL_V15_CONTRACT.experimentId, ...required,
    ]).experimentId, REAL_V15_CONTRACT.experimentId);
    assert.throws(
        () => parseAuthorArgs(['--experiment-id', 'horse_walk_v16_untrusted', ...required]),
        /not an authorized V14\/V15 controlled experiment/,
    );
});

test('authors deterministic V14 v2 controlled-generation specs atomically without stage execution', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const first = authorV14PipelineSpec(f.config, { expectedContract: f.expected, inspectPipeline: strictV2Inspector });
    assert.equal(first.initialPipelineStatus, 'READY_OBJECT_REGION_GATE');
    assert.equal(first.nextStage, 'object_region_gate');
    assert.equal(fs.existsSync(f.config.outputRoot), false);
    const spec = JSON.parse(fs.readFileSync(f.config.output, 'utf8'));
    assert.equal(spec.controlledGeneration.jobId, f.expected.jobId);
    assert.equal(spec.controlledGeneration.promptId, f.expected.promptId);
    assert.deepEqual(spec.controlledGeneration.state, descriptor(f.job));
    assert.deepEqual(spec.controlledGeneration.candidate, descriptor(f.candidate));
    assert.deepEqual(spec.controlledGeneration.frames[48], { frameIndex: 48, ...descriptor(f.frames[48]) });
    assert.equal(spec.authoringProvenance.noGpuJobOrStageExecution, true);
    assert.equal(spec.authoringProvenance.blenderUsed, false);
    const secondConfig = { ...f.config, output: path.join(f.root, 'pipeline-spec-copy.json') };
    const second = authorV14PipelineSpec(secondConfig, { expectedContract: f.expected, inspectPipeline: strictV2Inspector });
    assert.equal(second.spec.sha256, first.spec.sha256);
    assert.throws(
        () => authorV14PipelineSpec(f.config, { expectedContract: f.expected, inspectPipeline: strictV2Inspector }),
        /output already exists/,
    );
});

test('authors a V15 hard-endpoint controlled binding and rejects endpoint sequence drift', (context) => {
    const f = fixture({ hardEndpointGuides: true });
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const built = buildV14PipelineSpec(f.config, { expectedContract: f.expected });
    assert.equal(built.spec.controlledGeneration.experimentId, f.expected.experimentId);
    assert.equal(built.spec.controlledGeneration.jobId, f.expected.jobId);
    assert.equal(built.spec.clipName, 'Horse_Walk_LTX_V15_Browser_Contact_Refit');
    const changed = structuredClone(f.jobValue);
    changed.browser_guide_sequence_object.frames_array[1].sha256_string = 'f'.repeat(64);
    f.rewriteJob(changed);
    assert.throws(
        () => buildV14PipelineSpec(f.config, { expectedContract: f.expected }),
        /hard endpoint guide sequence drift/,
    );
});

test('candidate byte drift inside the completed job fails closed', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.appendFileSync(f.candidate.path, 'tamper');
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /controlled job candidate drift/);
});

test('controlled-job identity or externally pinned state drift fails closed', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    f.rewriteJob({ ...f.jobValue, workflow_fingerprint_string: 'f'.repeat(64) });
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /controlled job identity drift/);
});

test('a completed state stops being authorable when a later immutable revision exists', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    write(path.join(path.dirname(f.job.path), '000004.json'), f.jobPayload);
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /not the latest exact immutable revision/);
});

test('V14 guide video drift fails closed before spec publication', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.appendFileSync(f.guideVideo.path, 'tamper');
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /V14 interval guide video drift/);
});

test('canonical bundle artifact drift fails closed', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    fs.appendFileSync(f.skeleton.path, 'tamper');
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /canonical file skeleton\.json drift/);
});

test('runner-exported 28-file tool closure rejects a relabeled or stale source pin', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const changed = structuredClone(f.toolManifestValue);
    changed.sources.browserFit.sha256 = '0'.repeat(64);
    f.rewriteToolManifest(changed);
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /tool source browserFit SHA-256 mismatch/);
});

test('runtime executable drift is rejected and no output file is left behind', (context) => {
    const f = fixture();
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const runtime = JSON.parse(fs.readFileSync(f.runtimeManifest.path, 'utf8'));
    fs.appendFileSync(runtime.executables.node.path, 'tamper');
    assert.throws(() => buildV14PipelineSpec(f.config, { expectedContract: f.expected }), /runtime executable node (byte count|SHA-256) mismatch/);
    assert.equal(fs.existsSync(f.config.output), false);
});

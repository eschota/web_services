import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import zlib from 'node:zlib';

import {
    BROWSER_CANDIDATE_AUTHORITATIVE_PATHS,
    BROWSER_CANDIDATE_PIPELINE_SPEC_SCHEMA,
    BROWSER_CANDIDATE_PIPELINE_STATE_SCHEMA,
    BROWSER_CANDIDATE_PIPELINE_TRUST_SCHEMA,
    BROWSER_CANDIDATE_ACTION_POLICY,
    BROWSER_CANDIDATE_TOOL_SOURCE_PATHS,
    RAW_SEMANTIC_EVIDENCE_SCHEMA,
    RAW_SEMANTIC_PASS_RECEIPT_SCHEMA,
    buildProtectedStageExecutionReceipt,
    buildServerBrowserCandidateTrustContext,
    browserFitArtifactSetSha256,
    browserObservationArtifactSetSha256,
    browserVisualQaArtifactSetSha256,
    candidatePipelineIdentity,
    discoverBrowserCandidateToolDependencies,
    dynamicJavaScriptImportExpressions,
    hasUnresolvedCommonJsLoader,
    inspectBrowserAnimationCandidatePipeline as inspectPipelineCore,
    javascriptStaticModuleSpecifiers,
    parseBrowserCandidatePipelineArgs,
    pythonAbsoluteImportClauses,
    pythonRelativeImportClauses,
    runBrowserCandidatePipelineCli,
} from '../run_browser_animation_candidate_pipeline.mjs';
import { ANIMATION_FITTING_ACTION_CONTRACTS } from '../../../static/js/animation-fitting-action-contract.js';
import {
    HOOF_CONTACT_INFERENCE_CONTRACT,
    deriveSam2GroundEvidence,
    diagnoseHoofContacts,
} from '../../../static/js/animation-fitting-hoof-contact-inference.js';
import {
    BROWSER_FIT_CANARY_DEFAULTS,
    deriveFloat32LoopVelocityInvariantGate,
    evaluateBrowserFitGates,
    measureLoopVelocitySeam,
} from '../browser_fit_canary.mjs';
import {
    loadMaskFrames,
    prepareBridgeObservations,
} from '../diagnose_browser_hoof_contacts.mjs';
import {
    evaluateSemanticVisualQa,
    inspectPublishedSemanticVisualQa,
    resolveSemanticTerminalPolicy,
    runSemanticVisualPhaseQa,
} from '../browser_animation_visual_phase_qa.mjs';
import {
    HORSE_ONE_SHOT_FINAL_POSE_THRESHOLDS,
    HORSE_VISUAL_PHASE_QA_SCHEMA,
    HORSE_VISUAL_PHASE_REQUIRED_PHASES,
    HORSE_VISUAL_PHASE_THRESHOLDS,
    measureHorse2Deformation,
    measureHorseOneShotFinalPose,
} from '../browser_horse_visual_phase_qa.mjs';

const sha256 = (buffer) => crypto.createHash('sha256').update(buffer).digest('hex');
const jsonBuffer = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8');
const TRUST_SECRET = 'test-only-server-secret-with-more-than-thirty-two-bytes-0001';
const STAGE_RECEIPT_SECRET = 'test-only-protected-worker-secret-more-than-thirty-two-bytes-0001';
const TEST_PROTECTED_STAGE_PIN_FIELDS = Object.freeze({
    officialTracking: 'observationsArtifactSetSha256',
    initialFit: 'initialFitArtifactSetSha256',
    contactRefit: 'contactRefitArtifactSetSha256',
    visualQa: 'visualQaArtifactSetSha256',
});
const TRUST_ARGS_BY_SPEC = new Map();

function inspectBrowserAnimationCandidatePipeline(args) {
    return inspectPipelineCore({
        ...(TRUST_ARGS_BY_SPEC.get(path.resolve(args.specPath)) || {}),
        ...args,
    });
}

function write(filename, value) {
    const buffer = Buffer.isBuffer(value) ? value : Buffer.from(String(value));
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, buffer);
    return {
        path: path.resolve(filename),
        realpath: fs.realpathSync.native(filename),
        bytes: buffer.length,
        sha256: sha256(buffer),
    };
}

function writeJson(filename, value) {
    return write(filename, jsonBuffer(value));
}

function pin(filename) {
    const buffer = fs.readFileSync(filename);
    return {
        path: path.resolve(filename),
        realpath: fs.realpathSync.native(filename),
        bytes: buffer.length,
        sha256: sha256(buffer),
    };
}

function recursivePins(rootDirectory) {
    const result = [];
    const visit = (directory) => {
        fs.readdirSync(directory, { withFileTypes: true })
            .sort((left, right) => left.name.localeCompare(right.name))
            .forEach((entry) => {
                const filename = path.join(directory, entry.name);
                if (entry.isDirectory()) visit(filename);
                else if (entry.isFile()) result.push(pin(filename));
                else throw new Error(`test visual artifact is not a regular file: ${filename}`);
            });
    };
    visit(rootDirectory);
    return result;
}

function contractPins() {
    return Object.fromEntries(Object.entries(BROWSER_CANDIDATE_AUTHORITATIVE_PATHS)
        .map(([name, filename]) => [name, pin(filename)]));
}

function toolPins() {
    return Object.fromEntries(Object.entries(BROWSER_CANDIDATE_TOOL_SOURCE_PATHS)
        .map(([name, filename]) => [name, pin(filename)]));
}

function dependencyInventory() {
    return {
        algorithm: 'relative-static-import-closure.v1',
        files: discoverBrowserCandidateToolDependencies().map((item) => ({
            path: item.path,
            realpath: item.realpath,
            bytes: item.bytes,
            sha256: item.sha256,
        })),
    };
}

function gzipJson(value) {
    return zlib.gzipSync(Buffer.from(JSON.stringify(value), 'utf8'));
}

function crc32(buffer) {
    let crc = 0xffffffff;
    for (const byte of buffer) {
        crc ^= byte;
        for (let bit = 0; bit < 8; bit += 1) {
            crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
        }
    }
    return (crc ^ 0xffffffff) >>> 0;
}

function pngChunk(type, data) {
    const name = Buffer.from(type, 'ascii');
    const result = Buffer.alloc(12 + data.length);
    result.writeUInt32BE(data.length, 0);
    name.copy(result, 4);
    data.copy(result, 8);
    result.writeUInt32BE(crc32(Buffer.concat([name, data])), 8 + data.length);
    return result;
}

function solidRgbPng(width = 768, height = 448) {
    const header = Buffer.alloc(13);
    header.writeUInt32BE(width, 0);
    header.writeUInt32BE(height, 4);
    header[8] = 8;
    header[9] = 2;
    const row = Buffer.alloc(1 + width * 3);
    const raw = Buffer.concat(Array.from({ length: height }, () => row));
    return Buffer.concat([
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
        pngChunk('IHDR', header),
        pngChunk('IDAT', zlib.deflateSync(raw)),
        pngChunk('IEND', Buffer.alloc(0)),
    ]);
}

function oversizedDecodedRgbPng(width = 768, height = 448) {
    const header = Buffer.alloc(13);
    header.writeUInt32BE(width, 0);
    header.writeUInt32BE(height, 4);
    header[8] = 8;
    header[9] = 2;
    const oversized = Buffer.alloc((1 + width * 3) * height + 1);
    return Buffer.concat([
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
        pngChunk('IHDR', header),
        pngChunk('IDAT', zlib.deflateSync(oversized)),
        pngChunk('IEND', Buffer.alloc(0)),
    ]);
}

let HORSE_MASK_PNG = null;

function horseMaskPng(width = 512, height = 320) {
    if (HORSE_MASK_PNG) return HORSE_MASK_PNG;
    const header = Buffer.alloc(13);
    header.writeUInt32BE(width, 0);
    header.writeUInt32BE(height, 4);
    header[8] = 8;
    header[9] = 0;
    const rows = Array.from({ length: height }, (_, y) => {
        const row = Buffer.alloc(1 + width);
        if (y >= 80 && y <= 280) row.fill(255, 1 + 80, 1 + 461);
        return row;
    });
    HORSE_MASK_PNG = Buffer.concat([
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
        pngChunk('IHDR', header),
        pngChunk('IDAT', zlib.deflateSync(Buffer.concat(rows))),
        pngChunk('IEND', Buffer.alloc(0)),
    ]);
    return HORSE_MASK_PNG;
}

const MP4_FIXTURES = new Map();

function h264Mp4(frameCount) {
    if (MP4_FIXTURES.has(frameCount)) return MP4_FIXTURES.get(frameCount);
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'browser-visual-mp4-'));
    const output = path.join(directory, `${frameCount}.mp4`);
    const bundled = 'C:\\API\\ffmpeg\\bin\\ffmpeg.exe';
    const executable = fs.existsSync(bundled) ? bundled : 'ffmpeg';
    const result = spawnSync(executable, [
        '-hide_banner', '-loglevel', 'error', '-y',
        '-f', 'lavfi', '-i', 'color=c=black:s=768x448:r=30',
        '-frames:v', String(frameCount), '-c:v', 'libx264', '-pix_fmt', 'yuv420p',
        '-movflags', '+faststart', '-an', output,
    ], { encoding: 'utf8', windowsHide: true });
    if (result.status !== 0 || !fs.existsSync(output)) {
        fs.rmSync(directory, { recursive: true, force: true });
        throw new Error(`test ffmpeg could not author H.264 fixture: ${result.stderr || result.error?.message}`);
    }
    const buffer = fs.readFileSync(output);
    fs.rmSync(directory, { recursive: true, force: true });
    MP4_FIXTURES.set(frameCount, buffer);
    return buffer;
}

function zeroMp4MediaPayload(buffer) {
    const output = Buffer.from(buffer);
    let offset = 0;
    let changed = false;
    while (offset + 8 <= output.length) {
        const size = output.readUInt32BE(offset);
        const type = output.toString('ascii', offset + 4, offset + 8);
        if (size < 8 || offset + size > output.length) break;
        if (type === 'mdat') {
            output.fill(0, offset + 8, offset + size);
            changed = true;
        }
        offset += size;
    }
    if (!changed) throw new Error('test MP4 has no mdat');
    return output;
}

function forgeUndecodableH264Slices(buffer) {
    const output = Buffer.from(buffer);
    let offset = 0;
    let changed = 0;
    while (offset + 8 <= output.length) {
        const size = output.readUInt32BE(offset);
        const type = output.toString('ascii', offset + 4, offset + 8);
        if (size < 8 || offset + size > output.length) break;
        if (type === 'mdat') {
            let cursor = offset + 8;
            while (cursor + 4 <= offset + size) {
                const nalLength = output.readUInt32BE(cursor);
                cursor += 4;
                if (!nalLength || cursor + nalLength > offset + size) {
                    throw new Error('test MP4 mdat is not four-byte length-prefixed H.264');
                }
                const nalType = output[cursor] & 0x1f;
                if ([1, 5].includes(nalType)) {
                    if (nalLength < 2) throw new Error('test H.264 slice is unexpectedly short');
                    output.fill(0, cursor, cursor + nalLength);
                    output[cursor] = nalType === 5 ? 0x65 : 0x41;
                    output[cursor + 1] = 0xe0;
                    changed += 1;
                }
                cursor += nalLength;
            }
        }
        offset += size;
    }
    if (!changed) throw new Error('test MP4 has no H.264 slices');
    return output;
}

function forgeMp4SampleCount(buffer, sampleCount) {
    const output = Buffer.from(buffer);
    const marker = Buffer.from('stsz', 'ascii');
    const matches = [];
    let cursor = 0;
    while ((cursor = output.indexOf(marker, cursor)) >= 0) {
        matches.push(cursor);
        cursor += marker.length;
    }
    if (matches.length !== 1 || matches[0] + 16 > output.length) {
        throw new Error('test MP4 does not contain one writable stsz');
    }
    output.writeUInt32BE(sampleCount >>> 0, matches[0] + 12);
    return output;
}

function canonicalHorseArtifacts() {
    const bones = Array.from({ length: 304 }, (_, index) => ({
        name: index === 0 ? 'Root' : `Bone_${String(index).padStart(3, '0')}`,
        parent: index === 0 ? null : (index === 1 ? 'Root' : `Bone_${String(index - 1).padStart(3, '0')}`),
    }));
    const vertices = Array.from({ length: 344 }, (_, index) => {
        const world = [
            (index % 10) * 0.1,
            (Math.floor(index / 10) % 10) * 0.1,
            1 + Math.floor(index / 100),
        ];
        return {
            vertex_index: index,
            vertex_id: index,
            local: world,
            world,
            weights: [{ bone: 'Bone_001', weight: 1 }],
        };
    });
    const faces = Array.from({ length: 258 }, (_, index) => ({
        vertex_ids: [index, index + 1, index + 2],
    }));
    return {
        skeleton: { armatures: [{ name: 'Horse_2', bones }] },
        skinWeights: { vertices },
        topology: { faces },
    };
}

function fixture(semanticId = 'idle_alert') {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'browser-candidate-pipeline-'));
    const action = ANIMATION_FITTING_ACTION_CONTRACTS[semanticId];
    if (!action) throw new Error(`fixture action ${semanticId} is missing`);
    const candidate = write(path.join(root, 'inputs', `${semanticId}.mp4`), Buffer.from(`mp4-${semanticId}`));
    const bundle = path.join(root, 'canonical-horse-2');
    const reference = write(path.join(bundle, 'reference_rgb.png'), Buffer.from('canonical-horse-rgb'));
    const horse = canonicalHorseArtifacts();
    const skeleton = writeJson(path.join(bundle, 'skeleton.json'), horse.skeleton);
    const skinWeights = write(path.join(bundle, 'skin_weights.json.gz'), gzipJson(horse.skinWeights));
    const surfaceTopology = write(path.join(bundle, 'surface_topology.json.gz'), gzipJson(horse.topology));
    const sourceModelSha256 = 'a'.repeat(64);
    const fitting = writeJson(path.join(bundle, 'fitting_bundle.json'), {
        schema: 'autorig-actionless-fitting-bundle.v1',
        source: {
            filename: 'Horse_2.blend',
            sha256: sourceModelSha256,
            species: 'horse',
            rig_type: 'HORSE_2',
        },
        actionless: { actionless: true },
        counts: { vertices: 344, faces: 258, armatures: 1, meshes: 1 },
        camera: {
            projection: 'perspective',
            position: [4, -6, 3],
            target: [0, 0, 1],
        },
        ground_plane: { height: 0 },
        artifacts: {
            rgb: { filename: path.basename(reference.path), bytes: reference.bytes, sha256: reference.sha256 },
            skeleton: { filename: path.basename(skeleton.path), bytes: skeleton.bytes, sha256: skeleton.sha256 },
            skin_weights: {
                filename: path.basename(skinWeights.path), bytes: skinWeights.bytes, sha256: skinWeights.sha256,
            },
            surface_topology: {
                filename: path.basename(surfaceTopology.path), bytes: surfaceTopology.bytes, sha256: surfaceTopology.sha256,
            },
        },
    });
    const immutableFiles = [fitting, reference, skeleton, skinWeights, surfaceTopology];
    const immutable = writeJson(path.join(bundle, 'immutable_manifest.json'), {
        schema: 'autorig-fitting-immutable-copy.v1',
        source_model: { sha256: sourceModelSha256 },
        bundle_manifest: { filename: 'fitting_bundle.json', sha256: fitting.sha256 },
        bundle_file_count: immutableFiles.length,
        bundle_total_bytes: immutableFiles.reduce((total, item) => total + item.bytes, 0),
        files: immutableFiles.map((item) => ({
            filename: path.basename(item.path),
            bytes: item.bytes,
            sha256: item.sha256,
        })),
    });
    const contracts = contractPins();
    const canonicalPin = {
        immutableManifestSha256: immutable.sha256,
        fittingBundleSha256: fitting.sha256,
        sourceModelSha256,
    };
    const reviewId = `review-${semanticId}-001`;
    const rawEvidence = writeJson(path.join(root, 'receipt', 'server-semantic-evidence.json'), {
        schema: RAW_SEMANTIC_EVIDENCE_SCHEMA,
        decision: 'PASS',
        approvedForFitting: true,
        semanticId,
        generationMode: action.generationMode,
        frameCount: action.frameCount,
        outputFps: 30,
        candidate: {
            sha256: candidate.sha256,
            bytes: candidate.bytes,
            frameCount: action.frameCount,
            fps: 30,
        },
        contracts: Object.fromEntries(Object.entries(contracts).map(([name, item]) => [name, {
            bytes: item.bytes,
            sha256: item.sha256,
        }])),
        canonical: canonicalPin,
        review: {
            authorityKind: 'server_machine_gate',
            reviewId,
            clientSuppliedDecisionUsed: false,
        },
        checks: {
            identityPreserved: true,
            fullBodyVisible: true,
            semanticActionReadable: true,
            temporalModeCorrect: true,
            frameContractCorrect: true,
            fixedCamera: true,
        },
    });
    const receipt = writeJson(path.join(root, 'receipt', 'raw-semantic-pass-receipt.json'), {
        schema: RAW_SEMANTIC_PASS_RECEIPT_SCHEMA,
        decision: 'PASS',
        semanticId,
        generationMode: action.generationMode,
        frameCount: action.frameCount,
        outputFps: 30,
        candidate: { ...candidate, fps: 30, frameCount: action.frameCount },
        contracts,
        canonical: { ...canonicalPin, reference },
        authority: {
            kind: 'server_machine_gate',
            issuer: 'autorig-server-semantic-gate',
            reviewId,
            clientAssertionAccepted: false,
        },
        evidence: rawEvidence,
    });
    const executable = (name) => write(path.join(root, 'runtime', `${name}.exe`), Buffer.from(`exact-${name}`));
    const executables = Object.fromEntries(['python', 'node', 'chrome', 'ffmpeg', 'ffprobe', 'git']
        .map((name) => [name, executable(name)]));
    const pinnedFfmpeg = 'C:\\API\\ffmpeg\\bin\\ffmpeg.exe';
    if (fs.existsSync(pinnedFfmpeg)) executables.ffmpeg = pin(pinnedFfmpeg);
    const threeModule = write(path.join(root, 'runtime', 'three.module.js'), Buffer.from("export const REVISION='160';\n"));
    const trackingRuntimeRoot = path.join(root, 'tracking-models');
    fs.mkdirSync(trackingRuntimeRoot);
    const trackingRuntimeLock = pin(BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.trackingRuntimeLockContract);
    const identityPayload = {
        schema: 'autorig.browser-animation-candidate-identity.v2',
        rigType: 'HORSE_2',
        semanticId,
        generationMode: action.generationMode,
        frameCount: action.frameCount,
        outputFps: 30,
        candidate,
        rawSemanticPassReceipt: receipt,
        contracts,
        canonical: { ...canonicalPin, reference },
    };
    const candidateId = candidatePipelineIdentity(identityPayload);
    const outputRoot = path.join(root, 'outputs', candidateId);
    fs.mkdirSync(path.dirname(outputRoot), { recursive: true });
    const statePath = path.join(root, 'states', 'next-state.json');
    fs.mkdirSync(path.dirname(statePath), { recursive: true });
    const specPath = path.join(root, 'spec.json');
    const spec = {
        schema: BROWSER_CANDIDATE_PIPELINE_SPEC_SCHEMA,
        browserOnly: true,
        blenderUsed: false,
        fittingMixerUsed: false,
        qaAnimationMixerUsed: true,
        orchestratorExecutesSubprocesses: false,
        rigType: 'HORSE_2',
        semanticId,
        clipName: `Horse_${semanticId}`,
        candidateId,
        outputRoot,
        candidate,
        rawSemanticPassReceipt: receipt,
        contracts,
        canonicalBundle: {
            directory: bundle,
            immutableManifestSha256: immutable.sha256,
            fittingBundleSha256: fitting.sha256,
            sourceModelSha256,
        },
        runtime: {
            executables,
            threeModule: { ...threeModule, revision: '160' },
            trackingRuntimeRoot: {
                path: trackingRuntimeRoot,
                realpath: fs.realpathSync.native(trackingRuntimeRoot),
            },
            trackingRuntimeLock,
        },
        dependencyInventory: dependencyInventory(),
        externalPins: {
            observationsArtifactSetSha256: null,
            initialFitArtifactSetSha256: null,
            contactRefitInputManifestSha256: null,
            contactRefitArtifactSetSha256: null,
            finalThreeClipSha256: null,
            visualQaArtifactSetSha256: null,
        },
    };
    const specPin = writeJson(specPath, spec);
    const f = {
        root,
        action,
        candidate,
        reference,
        skeleton,
        skinWeights,
        surfaceTopology,
        immutable,
        fitting,
        sourceModelSha256,
        rawEvidence,
        receipt,
        contracts,
        candidateId,
        outputRoot,
        spec,
        specPath,
        specSha256: specPin.sha256,
        statePath,
        trustPath: path.join(root, 'server-trust.json'),
        protectedStageOutputs: {
            officialTracking: null,
            initialFit: null,
            contactRefit: null,
            visualQa: null,
        },
    };
    refreshTrust(f);
    return f;
}

function refreshTrust(f, options = {}) {
    const trust = buildServerBrowserCandidateTrustContext({
        specPath: f.specPath,
        expectedSpecSha256: f.specSha256,
        statePath: f.statePath,
        secret: options.secret ?? TRUST_SECRET,
        keyId: options.keyId ?? 'test-animation-fitting-key-v1',
        nowEpochMs: options.nowEpochMs ?? Date.now(),
        ttlMs: options.ttlMs ?? 10 * 60 * 1000,
        protectedStageOutputs: f.protectedStageOutputs,
        stageReceiptSecret: STAGE_RECEIPT_SECRET,
    });
    if (fs.existsSync(f.trustPath)) fs.unlinkSync(f.trustPath);
    const trustPin = writeJson(f.trustPath, trust);
    f.trust = trust;
    f.trustSha256 = trustPin.sha256;
    f.inspectArgs = {
        specPath: f.specPath,
        expectedSpecSha256: f.specSha256,
        trustContextPath: f.trustPath,
        expectedTrustContextSha256: f.trustSha256,
        statePath: f.statePath,
        trustSecret: TRUST_SECRET,
    };
    TRUST_ARGS_BY_SPEC.set(path.resolve(f.specPath), f.inspectArgs);
}

function rewriteSpec(f, mutator, { refresh = true, protectedStage = null } = {}) {
    const updated = structuredClone(f.spec);
    mutator(updated);
    f.spec = updated;
    fs.writeFileSync(f.specPath, jsonBuffer(updated));
    f.specSha256 = pin(f.specPath).sha256;
    if (protectedStage != null) {
        const pinField = TEST_PROTECTED_STAGE_PIN_FIELDS[protectedStage];
        if (!pinField) throw new Error(`unknown protected test stage ${protectedStage}`);
        const artifactSetSha256 = f.spec.externalPins[pinField];
        f.protectedStageOutputs[protectedStage] = buildProtectedStageExecutionReceipt({
            specPath: f.specPath,
            expectedSpecSha256: f.specSha256,
            stage: protectedStage,
            artifactSetSha256,
            executorJobId: `protected-test-${protectedStage}-${artifactSetSha256.slice(0, 12)}`,
            secret: STAGE_RECEIPT_SECRET,
        });
    }
    if (refresh) refreshTrust(f);
}

function publishObservations(f, pinArtifactSet = true) {
    const directory = path.join(f.outputRoot, '01-observations');
    const uniqueFrames = f.action.generationMode === 'loop' ? f.action.frameCount - 1 : f.action.frameCount;
    const touchdownFractions = {
        hind_left: 42 / 48,
        fore_left: 6 / 48,
        hind_right: 18 / 48,
        fore_right: 30 / 48,
    };
    const xByFoot = { hind_left: 360, fore_left: 140, hind_right: 410, fore_right: 190 };
    const boneByFoot = {
        hind_left: 'Bone_001',
        fore_left: 'Bone_002',
        hind_right: 'Bone_003',
        fore_right: 'Bone_004',
    };
    const circular = (value) => ((value % uniqueFrames) + uniqueFrames) % uniqueFrames;
    const hoofPoint = (foot, frame) => {
        const uniqueFrame = f.action.generationMode === 'loop' && frame === uniqueFrames ? 0 : frame;
        const touchdown = Math.round(touchdownFractions[foot] * uniqueFrames) % uniqueFrames;
        const contactLength = Math.max(5, Math.round((36 / 48) * uniqueFrames));
        const phase = circular(uniqueFrame - touchdown);
        if (phase < contactLength) return [xByFoot[foot], 280];
        const swing = (phase - contactLength) / Math.max(1, uniqueFrames - contactLength);
        return [xByFoot[foot] + Math.sin(swing * Math.PI * 2) * 9, 280 - Math.sin(swing * Math.PI) * 36];
    };
    const rawTracks = [];
    const mappings = [];
    let sourceIndex = 1;
    HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.forEach((foot) => {
        ['proximal', 'joint', 'hoof'].forEach((part, headIndex) => {
            const semanticId = `${foot}.${part}`;
            const sourceBone = boneByFoot[foot];
            const sourceAnchorId = `${sourceBone}:${headIndex}`;
            const sourceTrackId = `tap_${sourceIndex}`;
            const points = Array.from({ length: f.action.frameCount }, (_, frame) => {
                const hoof = hoofPoint(foot, frame);
                const point = part === 'proximal' ? [hoof[0], hoof[1] - 80]
                    : (part === 'joint' ? [hoof[0], hoof[1] - 40] : hoof);
                return { frame, x: point[0], y: point[1], visible: true, confidence: 0.96 };
            });
            rawTracks.push({ id: sourceTrackId, anchor_id: sourceAnchorId, query_frame: 0, points });
            mappings.push({
                limb: foot,
                collection: 'limbs',
                semanticAnchorId: semanticId,
                sourceTrackId,
                sourceAnchorId,
                sourceBone,
                sourceAnchorPin: sourceAnchorId,
                headIndex,
                orderedHeadCount: 3,
                restPoint: [points[0].x, points[0].y],
                queryToRestOffsetPx: [0, 0],
            });
            sourceIndex += 1;
        });
    });
    const masks = Array.from({ length: f.action.frameCount }, (_, index) => ({
        frame: index,
        path: `masks/frame_${String(index).padStart(6, '0')}.png`,
    }));
    const observations = writeJson(path.join(directory, 'observations.json'), {
        schema: 'autorig-fitting-observations.v1',
        frame_count: f.action.frameCount,
        width: 512,
        height: 320,
        fps: 30,
        silhouettes: masks,
        tracks: rawTracks,
        contacts: [],
        provenance: {
            source_video: f.candidate.path,
            source_video_sha256: f.candidate.sha256,
            bundle: path.dirname(f.fitting.path),
            immutable_manifest_sha256: f.immutable.sha256,
            bundle_sha256: f.fitting.sha256,
            tracker: { backend: HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend },
            segmenter: { backend: HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend },
        },
    });
    const maskPins = masks.map((row) => write(
        path.join(directory, ...row.path.split('/')),
        horseMaskPng(),
    ));
    const manifest = writeJson(path.join(directory, 'observation_bundle_manifest.json'), {
        schema: 'autorig-tracking-observation-bundle.v1',
        files: [
            { path: 'observations.json', bytes: observations.bytes, sha256: observations.sha256 },
            ...maskPins.map((item, index) => ({
                path: masks[index].path,
                bytes: item.bytes,
                sha256: item.sha256,
            })),
        ],
        provenance: {
            source_video_sha256: f.candidate.sha256,
            immutable_manifest_sha256: f.immutable.sha256,
            bundle_sha256: f.fitting.sha256,
        },
    });
    f.observationMappings = mappings;
    f.observationMaskPins = maskPins;
    observations.artifactSetSha256 = browserObservationArtifactSetSha256(
        directory,
        [manifest, observations, ...maskPins],
    );
    if (pinArtifactSet) {
        rewriteSpec(f, (spec) => {
            spec.externalPins.observationsArtifactSetSha256 = observations.artifactSetSha256;
        }, { protectedStage: 'officialTracking' });
    }
    return observations;
}

function serializedTracks(frameCount, loop) {
    const duration = (frameCount - 1) / 30;
    const times = Array.from({ length: frameCount }, (_, index) => index / 30);
    const motion = Array.from({ length: frameCount }, (_, index) => {
        const t = index / (frameCount - 1);
        return loop ? Math.sin(t * Math.PI * 2) : Math.sin(t * Math.PI);
    });
    const tracks = [
        {
            name: 'Root.quaternion', type: 'quaternion', times,
            values: Array.from({ length: frameCount }, () => [0, 0, 0, 1]).flat(),
        },
        {
            name: 'Root.position', type: 'vector', times,
            values: Array.from({ length: frameCount }, () => [0, 0, 0]).flat(),
        },
    ];
    for (let index = 1; index <= 4; index += 1) {
        const bone = `Bone_${String(index).padStart(3, '0')}`;
        const scale = 0.1 + index * 0.01;
        tracks.push({
            name: `${bone}.quaternion`, type: 'quaternion', times,
            values: motion.map((value) => [0, 0, Math.sin(value * scale), Math.cos(value * scale)]).flat(),
        }, {
            name: `${bone}.position`, type: 'vector', times,
            values: motion.map((value) => [0, 0, value * 0.01 * index]).flat(),
        });
    }
    return { duration, tracks };
}

function publishFit(
    f,
    observations,
    directoryName = '02-browser-fit',
    contact = false,
    manifestSha = null,
    pinArtifactSet = true,
) {
    const directory = path.join(f.outputRoot, directoryName);
    const timeline = serializedTracks(f.action.frameCount, f.action.generationMode === 'loop');
    const policy = BROWSER_CANDIDATE_ACTION_POLICY[f.spec.semanticId];
    const clipName = contact ? f.spec.clipName
        : (policy === 'contact_refit' ? `${f.spec.clipName}_BrowserFit` : f.spec.clipName);
    const c1PeriodicClosure = !contact && f.action.generationMode === 'loop'
        ? {
            schema: 'autorig-browser-c1-periodic-closure.v1',
            enabled: true,
            windowFrames: 4,
            poseEpsilon: 1e-5,
        }
        : {
            schema: 'autorig-browser-c1-periodic-closure.v1',
            enabled: false,
            windowFrames: null,
        };
    const clipJson = {
        name: clipName,
        uuid: `clip-${f.spec.semanticId}-${contact ? 'contact' : 'initial'}`,
        blendMode: 2500,
        duration: timeline.duration,
        tracks: timeline.tracks,
        ...((!contact && f.action.generationMode === 'loop') ? {
            userData: { autorigC1PeriodicClosure: c1PeriodicClosure },
        } : {}),
    };
    const loopVelocitySeam = measureLoopVelocitySeam(clipJson);
    const invariant = !contact && f.action.generationMode === 'loop'
        ? deriveFloat32LoopVelocityInvariantGate(clipJson)
        : { schema: 'autorig-browser-float32-loop-velocity-invariant-gate.v1', enabled: false };
    const loopVelocityGate = !contact && f.action.generationMode === 'loop'
        ? {
            enabled: true,
            maximumQuaternionAngularVelocitySeamRadPerSecond:
                invariant.maximumQuaternionAngularVelocitySeamRadPerSecond,
            maximumPositionVelocitySeamWorldPerSecond:
                invariant.maximumPositionVelocitySeamWorldPerSecond,
            derivation: invariant,
        }
        : {
            enabled: false,
            maximumQuaternionAngularVelocitySeamRadPerSecond: null,
            maximumPositionVelocitySeamWorldPerSecond: null,
            derivation: invariant,
        };
    const immutableJson = JSON.parse(fs.readFileSync(f.immutable.path, 'utf8'));
    const rawObservations = JSON.parse(fs.readFileSync(observations.path, 'utf8'));
    const rawTracks = new Map(rawObservations.tracks.map((track) => [track.id, track]));
    let initialTargetErrorSum = 0;
    let targetSamples = 0;
    let maximumBoneLengthErrorPx = 0;
    const debugFrames = Array.from({ length: f.action.frameCount }, (_, frame) => ({
        frame,
        limbs: Object.fromEntries(HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => {
            const mappings = f.observationMappings.filter((mapping) => mapping.limb === foot)
                .sort((left, right) => left.headIndex - right.headIndex);
            const points = mappings.map((mapping) => {
                const point = rawTracks.get(mapping.sourceTrackId).points[frame];
                const target = [
                    point.x + mapping.queryToRestOffsetPx[0],
                    point.y + mapping.queryToRestOffsetPx[1],
                ];
                initialTargetErrorSum += Math.hypot(
                    target[0] - mapping.restPoint[0], target[1] - mapping.restPoint[1],
                );
                targetSamples += 1;
                return target;
            });
            for (let index = 1; index < points.length; index += 1) {
                const restLength = Math.hypot(
                    mappings[index].restPoint[0] - mappings[index - 1].restPoint[0],
                    mappings[index].restPoint[1] - mappings[index - 1].restPoint[1],
                );
                const fittedLength = Math.hypot(
                    points[index][0] - points[index - 1][0],
                    points[index][1] - points[index - 1][1],
                );
                maximumBoneLengthErrorPx = Math.max(
                    maximumBoneLengthErrorPx, Math.abs(fittedLength - restLength),
                );
            }
            return [foot, { points }];
        })),
    }));
    const inputs = {
        bundleDirectory: path.dirname(f.fitting.path),
        observationsPath: observations.path,
        sourceVideoSha256: f.candidate.sha256,
        immutableManifestSha256: f.immutable.sha256,
        fittingBundleSha256: f.fitting.sha256,
        sourceModelSha256: f.sourceModelSha256,
        skeletonSha256: pin(path.join(path.dirname(f.fitting.path), 'skeleton.json')).sha256,
        observationsSha256: observations.sha256,
        bundleFileCount: immutableJson.bundle_file_count,
        bundleTotalBytes: immutableJson.bundle_total_bytes,
    };
    const qa = {
        targetSamples,
        initialMeanTargetErrorPx: initialTargetErrorSum / targetSamples,
        finalMeanTargetErrorPx: 0,
        maximumTargetErrorPx: 0,
        maximumBoneLengthErrorPx,
        maximumJointLimitViolationRad: 0,
        maximumContactSlidePx: 0,
        loopEndpointError: 0,
        targetMode: 'ordered_deform_heads',
    };
    const hierarchyQa = {
        maximumSegmentLengthDriftWorld: 0,
        maximumHierarchyBakeReprojectionErrorPx: 0,
        maximumRequestedFittedPointErrorPx: 0,
        unreachablePixelRays: 0,
    };
    const restSeedAlignment = { maximumErrorPx: 0 };
    const contactDiagnostic = contact
        ? JSON.parse(fs.readFileSync(path.join(f.outputRoot, '03-hoof-contact-diagnostic.json'), 'utf8'))
        : null;
    const contactRefit = contact ? {
        scheduleStatus: contactDiagnostic.schedule.status,
        scheduleSupport: contactDiagnostic.schedule.qa.support,
        inferredTouchdownOrder: contactDiagnostic.schedule.inferredTouchdownOrder,
        semanticGaitQa: { accepted: true, simultaneousSwingFrameCount: 0 },
        fittedWalkQa: {
            status: 'PASS',
            failures: [],
            maximumContactSlideRatio: 0,
            thresholdRatio: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.maximumFittedContactSlideRatio,
        },
        provenance: {
            schema: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitProvenance,
            source: 'immutable_pass_diagnostic',
            browserOnly: true,
            blenderUsed: false,
            mixerUsed: false,
            inputManifestSha256: manifestSha,
            diagnosticSha256: pin(path.join(f.outputRoot, '03-hoof-contact-diagnostic.json')).sha256,
            bridgeReportSha256: pin(path.join(f.outputRoot, '02-browser-fit', 'bridge-report.json')).sha256,
            initialFitSummarySha256: pin(path.join(f.outputRoot, '02-browser-fit', 'fit-summary.json')).sha256,
            observationsSha256: observations.sha256,
            fittingBundleSha256: f.fitting.sha256,
            immutableManifestSha256: f.immutable.sha256,
            sourceVideoSha256: f.candidate.sha256,
            sourceModelSha256: f.sourceModelSha256,
            sourceSkeletonSha256: f.skeleton.sha256,
        },
    } : null;
    const gateOverrides = {
        ...(contact ? { requireFourLimbContacts: true } : {}),
        ...((!contact && f.action.generationMode === 'loop') ? {
            maximumQuaternionAngularVelocitySeamRadPerSecond:
                invariant.maximumQuaternionAngularVelocitySeamRadPerSecond,
            maximumPositionVelocitySeamWorldPerSecond:
                invariant.maximumPositionVelocitySeamWorldPerSecond,
        } : {}),
    };
    const gateEvaluation = evaluateBrowserFitGates({
        maximumHeadReconstructionErrorWorld: 0,
        restSeedAlignment,
        prepared: {
            contacts: contact ? HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => ({
                anchor_id: `${foot}.hoof`,
            })) : [],
        },
        fitted: { qa },
        hierarchyQa,
        hierarchyRayCount: 1,
        clipValid: true,
        allTracksBound: true,
        minimumTargetSamples: f.observationMappings.length,
        loopVelocitySeam,
        gates: gateOverrides,
    });
    if (!contact && f.action.generationMode === 'loop') {
        gateEvaluation.results.push({
            name: 'c1_quaternion_pose_seam_rad',
            passed: loopVelocitySeam.quaternionPoseSeamRad.maximum <= c1PeriodicClosure.poseEpsilon,
            actual: loopVelocitySeam.quaternionPoseSeamRad.maximum,
            comparator: '<=',
            threshold: c1PeriodicClosure.poseEpsilon,
        }, {
            name: 'c1_position_pose_seam_world',
            passed: loopVelocitySeam.positionPoseSeamWorld.maximum <= c1PeriodicClosure.poseEpsilon,
            actual: loopVelocitySeam.positionPoseSeamWorld.maximum,
            comparator: '<=',
            threshold: c1PeriodicClosure.poseEpsilon,
        });
    }
    if (contact) {
        gateEvaluation.results.push({
            name: 'pinned_contact_schedule', passed: true, actual: 'PASS', comparator: '===', threshold: 'PASS',
        }, {
            name: 'semantic_walk_gait', passed: true, actual: true, comparator: '===', threshold: true,
        }, {
            name: 'fitted_walk_contact_slide', passed: true, actual: 0, comparator: '<=',
            threshold: contactRefit.fittedWalkQa.thresholdRatio,
        });
    }
    gateEvaluation.passed = gateEvaluation.results.every((row) => row.passed);
    const bridge = writeJson(path.join(directory, 'bridge-report.json'), {
        schema: 'autorig-browser-fit-canary-bridge-report.v1',
        status: 'VALIDATED',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode: contact ? 'contact_constrained_refit' : 'unconstrained_diagnostic',
        sourceContacts: 0,
        preparedContacts: contact ? 4 : 0,
        inputs,
        mappings: f.observationMappings,
        minimumVisiblePoints: 1,
        c1PeriodicClosure,
        loopVelocityGate,
    });
    const summary = writeJson(path.join(directory, 'fit-summary.json'), {
        schema: 'autorig-browser-fit-canary-summary.v1',
        status: contact ? 'PASS_BROWSER_CONTACT_REFIT_GATES' : 'PASS_BROWSER_FIT_GATES',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        fittingMode: contact ? 'contact_constrained_refit' : 'unconstrained_diagnostic',
        gates: gateEvaluation,
        approvedForBrowserContactFit: contact,
        approvedForAnimationLibrary: false,
        approvalExclusions: contact
            ? ['fixed_camera_visual_phase_qa', 'target_mesh_deformation_qa']
            : ['gait_semantics_and_phase_order', 'fixed_camera_visual_phase_qa', 'target_mesh_deformation_qa'],
        runtime: { node: process.version, threeRevision: '160' },
        ...(contact ? {
            approvedForBrowserContactFit: true,
            observations: {
                frameCount: f.action.frameCount, fps: 30, contactCount: 4, restSeedAlignment,
            },
            contactRefit,
        } : {
            observations: {
                frameCount: f.action.frameCount, fps: 30, contactCount: 0, restSeedAlignment,
            },
        }),
        inputs,
        realBundle: { maximumHeadReconstructionErrorWorld: 0 },
        fit: {
            options: { ...BROWSER_FIT_CANARY_DEFAULTS.fit, loop: f.action.generationMode === 'loop' },
            frameCount: f.action.frameCount,
            durationSeconds: timeline.duration,
            quaternionTracks: timeline.tracks.filter((track) => track.type === 'quaternion').length,
            positionTracks: timeline.tracks.filter((track) => track.type === 'vector').length,
            qa,
        },
        hierarchyClip: {
            name: clipName,
            durationSeconds: timeline.duration,
            tracks: timeline.tracks.length,
            validate: true,
            allTracksBound: true,
            qa: hierarchyQa,
            segmentRayCount: 1,
            loopVelocitySeam,
            loopVelocityGate,
            c1PeriodicClosure,
        },
    });
    const fitted = writeJson(path.join(directory, 'fitted-animation.json'), {
        schema: 'autorig-browser-fitted-animation.v1',
        loop: f.action.generationMode === 'loop',
        frameCount: f.action.frameCount,
        fps: 30,
        durationSeconds: timeline.duration,
        tracks: timeline.tracks.filter((track) => track.type === 'quaternion'),
        positionTracks: timeline.tracks.filter((track) => track.type === 'vector'),
        rootTrack: null,
        qa,
        frames: debugFrames,
    });
    const clip = writeJson(path.join(directory, 'three-clip.json'), clipJson);
    const artifacts = [bridge, summary, fitted, clip];
    const artifactSetSha256 = browserFitArtifactSetSha256(artifacts);
    if (pinArtifactSet) {
        rewriteSpec(f, (spec) => {
            spec.externalPins[contact
                ? 'contactRefitArtifactSetSha256'
                : 'initialFitArtifactSetSha256'] = artifactSetSha256;
        }, { protectedStage: contact ? 'contactRefit' : 'initialFit' });
    }
    return { bridge, summary, fitted, clip, artifactSetSha256 };
}

function publishWalkDiagnostic(f, observations, fit, status = 'PASS') {
    const raw = JSON.parse(fs.readFileSync(observations.path, 'utf8'));
    const bridgeReport = JSON.parse(fs.readFileSync(fit.bridge.path, 'utf8'));
    const semanticObservations = prepareBridgeObservations(raw, bridgeReport);
    const loadedMasks = loadMaskFrames({ raw, observationPath: observations.path });
    const maskManifest = loadedMasks.manifest;
    const groundPayload = deriveSam2GroundEvidence({
        observations: semanticObservations,
        masks: loadedMasks.masks,
        options: {
            minimumSupportFeet: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet,
        },
    });
    groundPayload.provenance = {
        ...groundPayload.provenance,
        trackerBackend: HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend,
        observationsSha256: observations.sha256,
        bridgeReportSha256: fit.bridge.sha256,
        bundleSha256: f.fitting.sha256,
        immutableManifestSha256: f.immutable.sha256,
        maskManifestSha256: maskManifest.sha256,
    };
    const recomputedSchedule = diagnoseHoofContacts({
        observations: semanticObservations,
        groundEvidence: groundPayload,
        options: {
            minimumSupportFeet: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet,
        },
    });
    const schedule = structuredClone(recomputedSchedule);
    if (status !== recomputedSchedule.status) {
        schedule.status = status;
        schedule.qa.failures = status === 'PASS' ? [] : ['fixture_forced_failure'];
    }
    const ground = writeJson(path.join(f.outputRoot, '03-sam2-ground-evidence.json'), groundPayload);
    const immutableJson = JSON.parse(fs.readFileSync(f.immutable.path, 'utf8'));
    const immutableBundleFiles = immutableJson.files.map((row) => ({
        filename: row.filename,
        bytes: row.bytes,
        sha256: row.sha256,
    }));
    const diagnostic = writeJson(path.join(f.outputRoot, '03-hoof-contact-diagnostic.json'), {
        schema: 'autorig-browser-hoof-contact-diagnostic.v1',
        status,
        inputs: {
            observations: { path: observations.path, bytes: observations.bytes, sha256: observations.sha256 },
            bridgeReport: { path: fit.bridge.path, bytes: fit.bridge.bytes, sha256: fit.bridge.sha256 },
            sourceVideo: { path: f.candidate.path, bytes: f.candidate.bytes, sha256: f.candidate.sha256 },
            bundleManifest: { path: f.fitting.path, bytes: f.fitting.bytes, sha256: f.fitting.sha256 },
            immutableManifest: { path: f.immutable.path, bytes: f.immutable.bytes, sha256: f.immutable.sha256 },
            immutableBundleFiles,
            sourceSkeletonSha256: f.skeleton.sha256,
            sourceModelSha256: f.sourceModelSha256,
            maskManifest,
            trackerBackend: HOOF_CONTACT_INFERENCE_CONTRACT.trackerBackend,
            segmenterBackend: HOOF_CONTACT_INFERENCE_CONTRACT.segmenterBackend,
            frames: f.action.frameCount,
            fps: 30,
            loop: f.action.generationMode === 'loop',
            resolution: [raw.width, raw.height],
            minimumSupportFeet: HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet,
        },
        bridge: {
            semanticTracks: semanticObservations.tracks.length,
            hoofTracks: HOOF_CONTACT_INFERENCE_CONTRACT.footOrder.map((foot) => {
                const semanticId = `${foot}.hoof`;
                const mapping = bridgeReport.mappings.find((row) => row.semanticAnchorId === semanticId);
                return {
                    foot,
                    semanticId,
                    sourceTrackId: mapping.sourceTrackId,
                    sourceAnchorId: mapping.sourceAnchorId,
                    sourceBone: mapping.sourceBone,
                };
            }),
        },
        schedule,
    });
    return { diagnostic, ground };
}

function publishTrotDiagnostic(f, observations, fit, status = 'PASS') {
    return writeJson(path.join(f.outputRoot, '03-trot-gait-diagnostic.json'), {
        schema: 'autorig-browser-horse-trot-contact-diagnostic.v1',
        status,
        inputs: {
            observations: { sha256: observations.sha256 },
            bridgeReport: { sha256: fit.bridge.sha256 },
            sourceVideo: { sha256: f.candidate.sha256 },
        },
        qa: { failures: status === 'PASS' ? [] : ['not_diagonal'] },
        decision: {
            approvedForAnimationLibrary: false,
            eligibleForContactConstrainedRefit: status === 'PASS',
        },
    });
}

function publishContactManifest(f) {
    const observations = pin(path.join(f.outputRoot, '01-observations', 'observations.json'));
    const bridgeReport = pin(path.join(f.outputRoot, '02-browser-fit', 'bridge-report.json'));
    const initialFitSummary = pin(path.join(f.outputRoot, '02-browser-fit', 'fit-summary.json'));
    const contactDiagnostic = pin(path.join(f.outputRoot, '03-hoof-contact-diagnostic.json'));
    return writeJson(path.join(f.outputRoot, '04-contact-refit-input.json'), {
        schema: 'autorig-browser-contact-refit-input.v1',
        browserOnly: true,
        blenderUsed: false,
        mixerUsed: false,
        inputs: {
            bundleDirectory: path.dirname(f.fitting.path),
            observations,
            bridgeReport,
            initialFitSummary,
            contactDiagnostic,
        },
        pins: {
            observationsSha256: observations.sha256,
            bridgeReportSha256: bridgeReport.sha256,
            initialFitSummarySha256: initialFitSummary.sha256,
            diagnosticSha256: contactDiagnostic.sha256,
            sourceVideoSha256: f.candidate.sha256,
            fittingBundleSha256: f.fitting.sha256,
            immutableManifestSha256: f.immutable.sha256,
            sourceModelSha256: f.sourceModelSha256,
            sourceSkeletonSha256: pin(path.join(path.dirname(f.fitting.path), 'skeleton.json')).sha256,
        },
    });
}

async function publishVisualQa(
    f,
    clip,
    outputDirectory = path.join(f.outputRoot, '06-browser-visual-phase-qa'),
    pinArtifactSet = true,
) {
    const loop = f.action.generationMode === 'loop';
    const policy = resolveSemanticTerminalPolicy(f.spec.semanticId);
    const config = {
        outputDirectory,
        semanticId: f.spec.semanticId,
        loop,
        bundleDirectory: path.dirname(f.fitting.path),
        expectedImmutableManifestSha256: f.immutable.sha256,
        expectedFittingBundleSha256: f.fitting.sha256,
        expectedSourceModelSha256: f.sourceModelSha256,
        threeClipPath: clip.path,
        expectedThreeClipSha256: clip.sha256,
        threeModule: f.spec.runtime.threeModule.path,
        expectedThreeModuleSha256: f.spec.runtime.threeModule.sha256,
        expectedThreeRevision: '160',
    };
    const result = await runSemanticVisualPhaseQa(config, {
        runHorseVisualPhaseQa: async (baseConfig) => {
            const base = baseConfig.outputDirectory;
            fs.mkdirSync(path.join(base, 'frames'), { recursive: true });
            const png = solidRgbPng();
            const frames = Array.from({ length: f.action.frameCount }, (_, index) => write(
                path.join(base, 'frames', `frame_${String(index).padStart(4, '0')}.png`),
                png,
            ));
            const camera = writeJson(path.join(base, 'camera-settings.json'), {
                schema: 'autorig.browser-horse-fixed-camera.v1',
                camera: JSON.parse(fs.readFileSync(f.fitting.path, 'utf8')).camera,
                resolution: [768, 448],
                temporalMode: f.action.generationMode,
                rootMotionPolicy: loop
                    ? 'suppress_armature_root_tracks_and_lock_model_transform'
                    : 'allow_one_shot_root_tracks_keep_camera_static',
                renderer: {
                    webgl2: true,
                    outputColorSpace: 'SRGBColorSpace',
                    toneMapping: 'ACESFilmicToneMapping',
                    toneMappingExposure: 1.1,
                    shadowsEnabled: false,
                },
            });
            const skinWeightsJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(f.skinWeights.path)).toString('utf8'));
            const topologyJson = JSON.parse(zlib.gunzipSync(fs.readFileSync(f.surfaceTopology.path)).toString('utf8'));
            const evaluatedFrames = Array.from({ length: f.action.frameCount }, (_, frameIndex) => {
                const terminal = !loop && frameIndex >= f.action.frameCount - 3;
                const zOffset = terminal ? -1 : 0;
                return {
                    frameIndex,
                    timeSeconds: frameIndex / 30,
                    positions: skinWeightsJson.vertices.map((vertex) => [
                        vertex.world[0], vertex.world[1], vertex.world[2] + zOffset,
                    ]),
                    cameraStatic: true,
                    rootMotionLocked: loop,
                };
            });
            const deformationJson = measureHorse2Deformation({
                skinWeights: skinWeightsJson,
                topology: topologyJson,
                frames: evaluatedFrames,
                requireRootMotionLocked: loop,
            });
            deformationJson.inputs = {
                fittingBundleSha256: f.fitting.sha256,
                threeClipSha256: clip.sha256,
                skinWeightsSha256: f.skinWeights.sha256,
                topologySha256: f.surfaceTopology.sha256,
            };
            const deformation = writeJson(path.join(base, 'deformation-report.json'), deformationJson);
            let finalPose = null;
            if (!loop) {
                const finalPoseJson = measureHorseOneShotFinalPose({
                    skinWeights: skinWeightsJson,
                    frames: evaluatedFrames,
                    groundHeight: 0,
                });
                finalPoseJson.inputs = {
                    fittingBundleSha256: f.fitting.sha256,
                    threeClipSha256: clip.sha256,
                    skinWeightsSha256: f.skinWeights.sha256,
                };
                finalPose = writeJson(path.join(base, 'final-pose-stability-report.json'), finalPoseJson);
            }
            const video = write(path.join(base, 'fixed-camera-preview.mp4'), h264Mp4(f.action.frameCount));
            const phaseIndices = [
                0,
                Math.floor((f.action.frameCount - 1) / 2),
                Math.floor((f.action.frameCount - 1) * 0.75),
            ];
            const phaseRows = HORSE_VISUAL_PHASE_REQUIRED_PHASES.map((phase, index) => ({
                phase,
                frame_index: phaseIndices[index],
                ...frames[phaseIndices[index]],
            }));
            const actionPrompts = pin(BROWSER_CANDIDATE_AUTHORITATIVE_PATHS.actionPrompts);
            const finalPoseJson = finalPose ? JSON.parse(fs.readFileSync(finalPose.path, 'utf8')) : null;
            const baseMachinePassed = true && (!finalPoseJson || finalPoseJson.passed === true);
            const rendererRuntime = {
                animationEvaluation: 'Three.AnimationMixer',
                zeroWeightVertices: 0,
                maximumHeadReconstructionErrorWorld: 0,
                maximumRestVertexErrorWorld: 0,
                maximumAnimatedBoneHeadDisplacementWorld: 0.1,
                renderer: {
                    webgl2: true,
                    outputColorSpace: 'SRGBColorSpace',
                    toneMapping: 'ACESFilmicToneMapping',
                    toneMappingExposure: 1.1,
                    shadowsEnabled: false,
                },
            };
            const evidence = writeJson(path.join(base, 'visual-phase-qa.json'), {
                schema: 'autorig.browser-horse-visual-phase-evidence-envelope.v1',
                visual_phase_gate: {
                    schema: HORSE_VISUAL_PHASE_QA_SCHEMA,
                    version: 1,
                    rig_type: 'horse',
                    semantic_id: f.spec.semanticId,
                    fitted_clip_sha256: clip.sha256,
                    decision: null,
                    camera: {
                        static: true,
                        projection: 'perspective',
                        view: 'canonical_fitting_bundle',
                        root_motion_locked: loop,
                        settings_sha256: camera.sha256,
                    },
                    coincident_rest_vertex_separation: {
                        measured: true,
                        report_sha256: deformation.sha256,
                        pass: deformationJson.gates.coincidentRestSeparation,
                        threshold_m: HORSE_VISUAL_PHASE_THRESHOLDS.coincidentRestSeparationM,
                        max_separation_m: deformationJson.maximumCoincidentRestSeparationM,
                        sample_count: deformationJson.coincidentRestSampleCount,
                        group_count: deformationJson.coincidentRestGroupCount,
                        report_url: null,
                    },
                    required_phases: [...HORSE_VISUAL_PHASE_REQUIRED_PHASES],
                    frames: phaseRows.map((row) => ({
                        phase: row.phase,
                        frame_index: row.frame_index,
                        sha256: row.sha256,
                        evidence_url: null,
                    })),
                    reviewer: { id: null, reviewed_at: null },
                },
                local_evidence: {
                    source_rig_type: 'HORSE_2',
                    temporal_mode: f.action.generationMode,
                    browser_only: true,
                    blender_used: false,
                    animation_evaluation: 'Three.AnimationMixer',
                    immutable_inputs: {
                        source_model: {
                            filename: 'Horse_2.blend',
                            sha256: f.sourceModelSha256,
                        },
                        immutable_manifest: f.immutable,
                        fitting_bundle: f.fitting,
                        three_clip: clip,
                        action_prompts_contract: actionPrompts,
                        skeleton: f.skeleton,
                        skin_weights: f.skinWeights,
                        surface_topology: f.surfaceTopology,
                    },
                    camera_settings: camera,
                    browser_reconstruction_qa: {
                        maximum_bone_head_error_world: 0,
                        maximum_rest_vertex_error_world: 0,
                        animated_non_root_bones: ['Bone_001', 'Bone_002', 'Bone_003', 'Bone_004'],
                        maximum_animated_bone_head_displacement_world: 0.1,
                        thresholds: {
                            maximum_bone_head_error_world: 1e-5,
                            maximum_rest_vertex_error_world: 1e-5,
                            minimum_animated_bone_head_displacement_world: 1e-6,
                        },
                    },
                    target_mesh_deformation_qa: {
                        measured_every_frame: true,
                        passed: baseMachinePassed,
                        maximum_edge_stretch: deformationJson.maximumEdgeStretch,
                        p99_edge_stretch: deformationJson.p99EdgeStretch,
                        zero_weight_vertices: deformationJson.zeroWeightVertices,
                        thresholds: {
                            maximum_edge_stretch: HORSE_VISUAL_PHASE_THRESHOLDS.maximumEdgeStretch,
                            p99_edge_stretch: HORSE_VISUAL_PHASE_THRESHOLDS.p99EdgeStretch,
                            zero_weight_vertices: HORSE_VISUAL_PHASE_THRESHOLDS.zeroWeightVertices,
                        },
                        report: deformation,
                    },
                    phase_frames: phaseRows,
                    one_shot_final_pose_qa: finalPose ? {
                        passed: finalPoseJson.passed,
                        final_window_frames: finalPoseJson.finalWindowFrames,
                        maximum_p99_adjacent_displacement_m: finalPoseJson.maximumP99AdjacentDisplacementM,
                        centroid_drop_m: finalPoseJson.centroidDropM,
                        final_minimum_z_m: finalPoseJson.finalMinimumZM,
                        gates: finalPoseJson.gates,
                        report: finalPose,
                    } : null,
                    video: {
                        ...video,
                        container: 'mp4',
                        codec: 'h264',
                        pixel_format: 'yuv420p',
                        width: 768,
                        height: 448,
                        fps: 30,
                        frame_count: f.action.frameCount,
                        audio_stream_count: 0,
                        duration_seconds: f.action.frameCount / 30,
                        fixed_camera: true,
                        root_motion_locked: loop,
                        root_motion_policy: loop ? 'suppress' : 'allow_one_shot',
                    },
                    renderer: {
                        browser: 'headless_chrome_cdp',
                        three_revision: '160',
                        three_module: pin(f.spec.runtime.threeModule.path),
                        runtime: rendererRuntime,
                    },
                    human_review: {
                        decision: null,
                        reviewer_id: null,
                        reviewed_at: null,
                        required: true,
                    },
                    approvals: {
                        machine_qa_passed: baseMachinePassed,
                        ready_for_human_review: baseMachinePassed,
                        approved_for_animation_library: false,
                        release_ready: false,
                        fail_closed_reason: baseMachinePassed
                            ? 'human_visual_phase_decision_and_public_urls_unset'
                            : 'machine_one_shot_final_pose_qa_failed',
                    },
                },
            });
            return {
                evidencePath: evidence.path,
                deformationPath: deformation.path,
                finalPosePath: finalPose?.path || null,
                videoPath: video.path,
            };
        },
    });
    const artifacts = recursivePins(outputDirectory);
    const artifactSetSha256 = browserVisualQaArtifactSetSha256(outputDirectory, artifacts);
    if (pinArtifactSet) {
        rewriteSpec(f, (spec) => {
            spec.externalPins.visualQaArtifactSetSha256 = artifactSetSha256;
        }, { protectedStage: 'visualQa' });
    }
    return { ...result, artifactSetSha256 };
}

test('CLI parser requires a SHA-pinned spec and create-exclusive state path', () => {
    assert.deepEqual(parseBrowserCandidatePipelineArgs(['--help']), { help: true });
    assert.throws(
        () => parseBrowserCandidatePipelineArgs(['--spec', 'spec.json']),
        /--spec-sha256 is required/,
    );
    assert.deepEqual(parseBrowserCandidatePipelineArgs([
        '--spec', 'spec.json', '--spec-sha256', '1'.repeat(64),
        '--trust-context', 'trust.json', '--trust-context-sha256', '2'.repeat(64),
        '--state', 'state.json',
    ]), {
        specPath: 'spec.json',
        expectedSpecSha256: '1'.repeat(64),
        trustContextPath: 'trust.json',
        expectedTrustContextSha256: '2'.repeat(64),
        statePath: 'state.json',
    });
});

test('idle_alert authors exact 97-frame loop tracking then browser-only fit', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.schema, BROWSER_CANDIDATE_PIPELINE_STATE_SCHEMA);
    assert.equal(state.status, 'READY_TRACKING');
    assert.equal(state.frameCount, 97);
    assert.equal(state.generationMode, 'loop');
    assert.equal(state.policy, 'direct');
    assert.equal(state.next.command.shell, false);
    assert.equal(state.next.command.argv[0], '-c');
    assert.match(state.next.command.argv[1], /subprocess\.Popen = _pinned_popen/);
    assert.equal(path.resolve(state.next.command.argv[2]), path.resolve(f.spec.runtime.executables.git.path));
    assert.equal(state.next.command.argv[3], 'animation_fitting.tracking_runtime');
    assert.ok(state.next.command.argv.includes('--loop'));
    const observations = publishObservations(f);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_BROWSER_FIT');
    assert.ok(state.next.command.argv.includes('--c1-closure-window'));
    assert.ok(state.next.command.argv.includes('--emit-three-clip'));
    assert.ok(!state.next.command.argv.includes('--no-loop'));
    assert.ok(observations.bytes > 0);
});

test('caller-authored tracking output cannot advance without a protected worker receipt', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f, false);
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_OBSERVATIONS_ARTIFACT_SET_PIN');
    assert.equal(state.pinRequest.field, 'externalPins.observationsArtifactSetSha256');
    assert.equal(state.pinRequest.observedSha256NotTrusted, observations.artifactSetSha256);
    rewriteSpec(f, (spec) => {
        spec.externalPins.observationsArtifactSetSha256 = observations.artifactSetSha256;
    }, { protectedStage: 'officialTracking' });
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_BROWSER_FIT');
    assert.equal(
        f.trust.binding.protectedStageOutputs.officialTracking.artifactSetSha256,
        observations.artifactSetSha256,
    );
});

test('caller-authored fit JSON pauses at a server-authenticated artifact-set pin barrier', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations, '02-browser-fit', false, null, false);
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_INITIAL_FIT_ARTIFACT_SET_PIN');
    assert.equal(state.pinRequest.field, 'externalPins.initialFitArtifactSetSha256');
    assert.equal(state.pinRequest.observedSha256NotTrusted, fit.artifactSetSha256);
    assert.equal(state.next, null);
    rewriteSpec(f, (spec) => {
        spec.externalPins.initialFitArtifactSetSha256 = fit.artifactSetSha256;
    }, { protectedStage: 'initialFit' });
    assert.throws(() => buildServerBrowserCandidateTrustContext({
        specPath: f.specPath,
        expectedSpecSha256: f.specSha256,
        statePath: f.statePath,
        secret: TRUST_SECRET,
        keyId: 'test-animation-fitting-key-v1',
        stageReceiptSecret: STAGE_RECEIPT_SECRET,
    }), /protected (?:officialTracking|initialFit) receipt/);
    assert.equal(
        f.trust.binding.protectedStageOutputs.initialFit.artifactSetSha256,
        fit.artifactSetSha256,
    );
    const forgedStageOutputs = structuredClone(f.protectedStageOutputs);
    forgedStageOutputs.initialFit.signature.value = '0'.repeat(64);
    assert.throws(() => buildServerBrowserCandidateTrustContext({
        specPath: f.specPath,
        expectedSpecSha256: f.specSha256,
        statePath: f.statePath,
        secret: TRUST_SECRET,
        keyId: 'test-animation-fitting-key-v1',
        protectedStageOutputs: forgedStageOutputs,
        stageReceiptSecret: STAGE_RECEIPT_SECRET,
    }), /protected initialFit execution receipt HMAC authentication failed/);
    assert.throws(() => buildServerBrowserCandidateTrustContext({
        specPath: f.specPath,
        expectedSpecSha256: f.specSha256,
        statePath: f.statePath,
        secret: TRUST_SECRET,
        keyId: 'test-animation-fitting-key-v1',
        protectedStageOutputs: f.protectedStageOutputs,
        stageReceiptSecret: TRUST_SECRET,
    }), /HMAC secrets must be distinct/);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_THREE_CLIP_PIN');
});

test('contact refit outputs require their own authenticated four-artifact pin', (context) => {
    const f = fixture('walk_forward');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const initial = publishFit(f, observations);
    publishWalkDiagnostic(f, observations, initial);
    const manifest = publishContactManifest(f);
    rewriteSpec(f, (spec) => {
        spec.externalPins.contactRefitInputManifestSha256 = manifest.sha256;
    });
    const refit = publishFit(
        f,
        observations,
        '05-browser-contact-refit',
        true,
        manifest.sha256,
        false,
    );
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_CONTACT_REFIT_ARTIFACT_SET_PIN');
    assert.equal(state.pinRequest.observedSha256NotTrusted, refit.artifactSetSha256);
    rewriteSpec(f, (spec) => {
        spec.externalPins.contactRefitArtifactSetSha256 = refit.artifactSetSha256;
    }, { protectedStage: 'contactRefit' });
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_THREE_CLIP_PIN');
});

test('walk_forward uses exact 49-frame loop and mandatory contact-refit branch', (context) => {
    const f = fixture('walk_forward');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations);
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.frameCount, 49);
    assert.equal(state.policy, 'contact_refit');
    assert.equal(state.status, 'READY_HOOF_CONTACT_DIAGNOSTIC');
    assert.match(state.next.command.argv[0], /diagnose_browser_hoof_contacts\.mjs$/);
    assert.ok(state.next.command.argv.includes('--minimum-support-feet'));
    assert.ok(state.next.command.argv.includes(String(
        HOOF_CONTACT_INFERENCE_CONTRACT.contactRefitThresholds.minimumSupportFeet,
    )));
    publishWalkDiagnostic(f, observations, fit);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_CONTACT_REFIT_MANIFEST');
    const manifest = publishContactManifest(f);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_CONTACT_MANIFEST_PIN');
    assert.equal(state.pinRequest.observedSha256NotTrusted, manifest.sha256);
    rewriteSpec(f, (spec) => {
        spec.externalPins.contactRefitInputManifestSha256 = manifest.sha256;
    });
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_BROWSER_CONTACT_REFIT');
    assert.ok(state.next.command.argv.includes(manifest.sha256));
});

test('contact refit stays bound to admitted observations and exact emitted clip tracks', (context) => {
    const observationDrift = fixture('walk_forward');
    const clipDrift = fixture('walk_forward');
    context.after(() => [observationDrift, clipDrift]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    const prepare = (f) => {
        const observations = publishObservations(f);
        const initial = publishFit(f, observations);
        publishWalkDiagnostic(f, observations, initial);
        const manifest = publishContactManifest(f);
        rewriteSpec(f, (spec) => {
            spec.externalPins.contactRefitInputManifestSha256 = manifest.sha256;
        });
        const contact = publishFit(f, observations, '05-browser-contact-refit', true, manifest.sha256);
        return { observations, manifest, contact };
    };
    const first = prepare(observationDrift);
    for (const filename of [first.contact.bridge.path, first.contact.summary.path]) {
        const value = JSON.parse(fs.readFileSync(filename, 'utf8'));
        value.inputs.observationsSha256 = 'f'.repeat(64);
        fs.writeFileSync(filename, jsonBuffer(value));
    }
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: observationDrift.specPath, expectedSpecSha256: observationDrift.specSha256,
    }), /contact refit bridge\/summary inputs are not the exact immutable chain/);

    const second = prepare(clipDrift);
    const clip = JSON.parse(fs.readFileSync(second.contact.clip.path, 'utf8'));
    clip.tracks.find((track) => track.type === 'vector').values[3] = 0.125;
    fs.writeFileSync(second.contact.clip.path, jsonBuffer(clip));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: clipDrift.specPath, expectedSpecSha256: clipDrift.specSha256,
    }), /contact refit fitted-animation and Three clip tracks are not byte-value equivalent/);
});

test('death authors exact 65-frame one-shot tracking and no-loop browser fit', (context) => {
    const f = fixture('death');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.frameCount, 65);
    assert.equal(state.generationMode, 'one_shot');
    assert.ok(!state.next.command.argv.includes('--loop'));
    publishObservations(f);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_BROWSER_FIT');
    assert.ok(state.next.command.argv.includes('--no-loop'));
    assert.ok(!state.next.command.argv.includes('--c1-closure-window'));
});

test('attack_primary authors exact 49-frame one-shot contract', (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    publishObservations(f);
    const state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.frameCount, 49);
    assert.equal(state.generationMode, 'one_shot');
    assert.equal(state.policy, 'direct');
    assert.ok(state.next.command.argv.includes('--no-loop'));
});

test('fall is exact 49-frame one-shot airborne-to-death and uses settled death terminal QA', async (context) => {
    const f = fixture('fall');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.frameCount, 49);
    assert.equal(state.generationMode, 'one_shot');
    assert.ok(!state.next.command.argv.includes('--loop'));
    const policy = resolveSemanticTerminalPolicy('fall');
    assert.equal(policy.startPoseId, 'airborne');
    assert.equal(policy.endPoseId, 'death_end');
    assert.equal(policy.terminalPolicy, 'settled_grounded_death');
    const observations = publishObservations(f);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_BROWSER_FIT');
    assert.ok(state.next.command.argv.includes('--no-loop'));
    const fit = publishFit(f, observations);
    rewriteSpec(f, (spec) => { spec.externalPins.finalThreeClipSha256 = fit.clip.sha256; });
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_FIXED_CAMERA_DEFORMATION_QA');
    assert.ok(state.next.command.argv.includes('--one-shot'));
    assert.ok(state.next.command.argv.includes('--no-loop') === false);
    const result = await publishVisualQa(f, fit.clip);
    assert.equal(result.report.terminalPoseContract.policy, 'settled_grounded_death');
    assert.equal(result.report.terminalPoseContract.gates.centroidDrop, true);
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_HUMAN');
});

test('all 30 exact taxonomy semantic IDs resolve with 33/49/65/97 contracts', (context) => {
    const roots = [];
    context.after(() => roots.forEach((root) => fs.rmSync(root, { recursive: true, force: true })));
    assert.equal(Object.keys(ANIMATION_FITTING_ACTION_CONTRACTS).length, 30);
    for (const semanticId of Object.keys(ANIMATION_FITTING_ACTION_CONTRACTS)) {
        const f = fixture(semanticId);
        roots.push(f.root);
        const state = inspectBrowserAnimationCandidatePipeline({
            specPath: f.specPath, expectedSpecSha256: f.specSha256,
        });
        assert.equal(state.semanticId, semanticId);
        assert.ok([33, 49, 65, 97].includes(state.frameCount));
        assert.equal(state.outputFps, 30);
        assert.equal(state.policy, BROWSER_CANDIDATE_ACTION_POLICY[semanticId]);
    }
});

test('exact locomotion matrix permits only walk contact refit and fails trot/run/sprint closed', (context) => {
    assert.deepEqual(Object.keys(BROWSER_CANDIDATE_ACTION_POLICY), Object.keys(ANIMATION_FITTING_ACTION_CONTRACTS));
    assert.deepEqual(BROWSER_CANDIDATE_ACTION_POLICY, {
        idle_neutral: 'direct',
        idle_alert: 'direct',
        idle_relaxed: 'direct',
        idle_look_around: 'direct',
        idle_fidget: 'direct',
        walk_forward: 'contact_refit',
        walk_backward: 'contact_refit',
        trot_jog: 'unsupported_gait_contact',
        run: 'unsupported_gait_contact',
        sprint: 'unsupported_gait_contact',
        turn_left_90: 'direct',
        turn_right_90: 'direct',
        turn_around_180: 'direct',
        stop_brake: 'direct',
        jump_air: 'direct',
        fall: 'direct',
        jump_start: 'direct',
        jump_land: 'direct',
        jump_full: 'direct',
        attack_primary: 'direct',
        attack_secondary: 'direct',
        attack_heavy: 'direct',
        hit_front: 'direct',
        hit_left: 'direct',
        hit_right: 'direct',
        death: 'direct',
        get_up: 'direct',
        eat_interact: 'direct',
        sleep_rest: 'direct',
        vocalize_emote: 'direct',
    });
    assert.equal(BROWSER_CANDIDATE_ACTION_POLICY.walk_forward, 'contact_refit');
    assert.equal(BROWSER_CANDIDATE_ACTION_POLICY.walk_backward, 'contact_refit');
    assert.equal(BROWSER_CANDIDATE_ACTION_POLICY.trot_jog, 'unsupported_gait_contact');
    assert.equal(BROWSER_CANDIDATE_ACTION_POLICY.run, 'unsupported_gait_contact');
    assert.equal(BROWSER_CANDIDATE_ACTION_POLICY.sprint, 'unsupported_gait_contact');
    const fixtures = ['walk_backward', 'trot_jog', 'run', 'sprint'].map(fixture);
    context.after(() => fixtures.forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    const [walk, trot, run, sprint] = fixtures;
    const walkObservations = publishObservations(walk);
    publishFit(walk, walkObservations);
    assert.equal(inspectBrowserAnimationCandidatePipeline({
        specPath: walk.specPath, expectedSpecSha256: walk.specSha256,
    }).status, 'READY_HOOF_CONTACT_DIAGNOSTIC');
    const forgedTrot = publishTrotDiagnostic(
        trot,
        { sha256: '0'.repeat(64) },
        { bridge: { sha256: '1'.repeat(64) } },
    );
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: trot.specPath, expectedSpecSha256: trot.specSha256,
    }), /unexpected artifact 03-trot-gait-diagnostic\.json/);
    fs.unlinkSync(forgedTrot.path);
    for (const f of [trot, run, sprint]) {
        const state = inspectBrowserAnimationCandidatePipeline({
            specPath: f.specPath, expectedSpecSha256: f.specSha256,
        });
        assert.equal(state.status, 'FAILED_UNSUPPORTED_GAIT_CONTACT_POLICY');
        assert.equal(state.next, null);
        assert.notEqual(state.policy, 'direct');
    }
});

test('machine PASS visual/deformation evidence stops at AWAITING_HUMAN', async (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations);
    rewriteSpec(f, (spec) => { spec.externalPins.finalThreeClipSha256 = fit.clip.sha256; });
    let state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_FIXED_CAMERA_DEFORMATION_QA');
    assert.ok(!state.next.command.argv.includes('--one-shot'));
    const visual = await publishVisualQa(
        f,
        fit.clip,
        path.join(f.outputRoot, '06-browser-visual-phase-qa'),
        false,
    );
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_EXTERNAL_VISUAL_QA_ARTIFACT_SET_PIN');
    assert.equal(state.pinRequest.field, 'externalPins.visualQaArtifactSetSha256');
    assert.equal(state.pinRequest.observedSha256NotTrusted, visual.artifactSetSha256);
    rewriteSpec(f, (spec) => {
        spec.externalPins.visualQaArtifactSetSha256 = visual.artifactSetSha256;
    }, { protectedStage: 'visualQa' });
    state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'AWAITING_HUMAN');
    assert.equal(state.next, null);
    assert.equal(state.qaAnimationMixerUsed, true);
    assert.equal(state.fittingMixerUsed, false);
});

test('one-shot visual QA command carries --one-shot and remains human-gated', (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations);
    rewriteSpec(f, (spec) => { spec.externalPins.finalThreeClipSha256 = fit.clip.sha256; });
    const state = inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    });
    assert.equal(state.status, 'READY_FIXED_CAMERA_DEFORMATION_QA');
    assert.ok(state.next.command.argv.includes('--one-shot'));
    assert.match(state.next.command.argv[0], /browser_animation_visual_phase_qa\.mjs$/);
});

test('semantic terminal policy does not misclassify attack/get-up/jump-start as death', () => {
    assert.equal(resolveSemanticTerminalPolicy('death').terminalPolicy, 'settled_grounded_death');
    assert.equal(resolveSemanticTerminalPolicy('attack_primary').terminalPolicy, 'settled_grounded_action');
    assert.equal(resolveSemanticTerminalPolicy('get_up').terminalPolicy, 'settled_grounded_action');
    assert.equal(resolveSemanticTerminalPolicy('jump_start').terminalPolicy, 'airborne_transition');
    const baseGates = {
        finalP99Motion: true,
        finalMedianMotion: true,
        centroidDrop: false,
        groundContact: true,
        groundPenetration: true,
        cameraStatic: true,
    };
    const finalPose = {
        schema: 'autorig.browser-horse-one-shot-final-pose-qa.v1',
        gates: baseGates,
        passed: false,
    };
    const deformation = {
        schema: 'autorig.browser-horse-target-deformation-qa.v1',
        passed: true,
    };
    const attack = evaluateSemanticVisualQa({
        policy: resolveSemanticTerminalPolicy('attack_primary'),
        deformationReport: deformation,
        finalPoseReport: finalPose,
    });
    const death = evaluateSemanticVisualQa({
        policy: resolveSemanticTerminalPolicy('death'),
        deformationReport: deformation,
        finalPoseReport: finalPose,
    });
    assert.equal(attack.machinePassed, true);
    assert.equal(death.machinePassed, false);
    assert.equal(Object.hasOwn(attack.terminalGates, 'centroidDrop'), false);
    assert.equal(Object.hasOwn(death.terminalGates, 'centroidDrop'), true);
});

test('airborne transition requires camera stability/no penetration but not settled ground contact', () => {
    const result = evaluateSemanticVisualQa({
        policy: resolveSemanticTerminalPolicy('jump_start'),
        deformationReport: {
            schema: 'autorig.browser-horse-target-deformation-qa.v1',
            passed: true,
        },
        finalPoseReport: {
            schema: 'autorig.browser-horse-one-shot-final-pose-qa.v1',
            gates: {
                finalP99Motion: false,
                finalMedianMotion: false,
                centroidDrop: false,
                groundContact: false,
                groundPenetration: true,
                cameraStatic: true,
            },
        },
    });
    assert.equal(result.machinePassed, true);
    assert.deepEqual(result.terminalGates, { groundPenetration: true, cameraStatic: true });
});

test('semantic visual wrapper publishes immutable fail-closed evidence from base browser QA', async (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const result = await publishVisualQa(f, fit.clip, path.join(f.root, 'standalone-attack-visual-qa'));
    assert.equal(result.report.status, 'PASS_MACHINE_QA_AWAITING_HUMAN');
    assert.equal(result.report.terminalPoseContract.policy, 'settled_grounded_action');
    assert.equal(result.report.terminalPoseContract.gates.centroidDrop, undefined);
    assert.equal(result.report.humanReview.decision, null);
    assert.equal(result.report.approvals.approvedForAnimationLibrary, false);
    assert.equal(pin(result.reportPin.path).sha256, result.reportPin.sha256);
});

test('semantic visual wrapper accepts descendant world motion with a constant local track', async (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const clipJson = JSON.parse(fs.readFileSync(fit.clip.path, 'utf8'));
    const constantBone = 'Bone_004';
    for (const track of clipJson.tracks.filter((row) => row.name.startsWith(`${constantBone}.`))) {
        const stride = track.type === 'quaternion' ? 4 : 3;
        const first = track.values.slice(0, stride);
        track.values = Array.from({ length: track.times.length }, () => first).flat();
    }
    const clip = writeJson(fit.clip.path, clipJson);
    const result = await publishVisualQa(
        f,
        clip,
        path.join(f.root, 'standalone-descendant-world-motion-visual-qa'),
    );
    assert.equal(result.report.status, 'PASS_MACHINE_QA_AWAITING_HUMAN');
    assert.equal(result.report.machineQa.passed, true);
});

test('semantic visual wrapper accepts a rotating bone whose own head remains fixed', async (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const clipJson = JSON.parse(fs.readFileSync(fit.clip.path, 'utf8'));
    const sourceQuaternion = clipJson.tracks.find((row) => row.name === 'Bone_004.quaternion');
    const sourcePosition = clipJson.tracks.find((row) => row.name === 'Bone_004.position');
    assert.ok(sourceQuaternion);
    assert.ok(sourcePosition);
    const firstPosition = sourcePosition.values.slice(0, 3);
    clipJson.tracks.push({
        ...structuredClone(sourceQuaternion),
        name: 'Bone_005.quaternion',
    }, {
        ...structuredClone(sourcePosition),
        name: 'Bone_005.position',
        values: Array.from({ length: sourcePosition.times.length }, () => firstPosition).flat(),
    });
    const clip = writeJson(fit.clip.path, clipJson);
    const result = await publishVisualQa(
        f,
        clip,
        path.join(f.root, 'standalone-fixed-head-rotating-bone-visual-qa'),
    );
    assert.equal(result.report.status, 'PASS_MACHINE_QA_AWAITING_HUMAN');
    assert.equal(result.report.machineQa.passed, true);
});

test('semantic visual wrapper rejects root-only local motion as anti-static proof', async (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const clipJson = JSON.parse(fs.readFileSync(fit.clip.path, 'utf8'));
    clipJson.tracks = clipJson.tracks.slice(0, 2).map((track) => ({
        ...structuredClone(track),
        name: track.name.replace(/^Bone_001\./, 'Root.'),
    }));
    const clip = writeJson(fit.clip.path, clipJson);
    await assert.rejects(
        publishVisualQa(f, clip, path.join(f.root, 'standalone-root-only-visual-qa')),
        /base browser reconstruction evidence is not an exact animated PASS/,
    );
});

test('semantic visual wrapper accepts root motion with real non-root limb motion', async (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const clipJson = JSON.parse(fs.readFileSync(fit.clip.path, 'utf8'));
    const rootTracks = clipJson.tracks.slice(0, 2).map((track) => ({
        ...structuredClone(track),
        name: track.name.replace(/^Bone_001\./, 'Root.'),
    }));
    clipJson.tracks.push(...rootTracks);
    const clip = writeJson(fit.clip.path, clipJson);
    const result = await publishVisualQa(
        f,
        clip,
        path.join(f.root, 'standalone-root-plus-limb-visual-qa'),
    );
    assert.equal(result.report.status, 'PASS_MACHINE_QA_AWAITING_HUMAN');
    assert.equal(result.report.machineQa.passed, true);
});

test('nested deformation tampering cannot be hidden by rewriting outer evidence pins', async (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations);
    rewriteSpec(f, (spec) => { spec.externalPins.finalThreeClipSha256 = fit.clip.sha256; });
    await publishVisualQa(f, fit.clip);
    const qaRoot = path.join(f.outputRoot, '06-browser-visual-phase-qa');
    const baseRoot = path.join(qaRoot, 'horse-base-evidence');
    const deformationPath = path.join(baseRoot, 'deformation-report.json');
    const deformation = JSON.parse(fs.readFileSync(deformationPath, 'utf8'));
    deformation.maximumEdgeStretch = 999;
    const deformationPin = writeJson(deformationPath, deformation);
    const baseEvidencePath = path.join(baseRoot, 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.target_mesh_deformation_qa.maximum_edge_stretch = 999;
    baseEvidence.local_evidence.target_mesh_deformation_qa.report = deformationPin;
    baseEvidence.visual_phase_gate.coincident_rest_vertex_separation.report_sha256 = deformationPin.sha256;
    const baseEvidencePin = writeJson(baseEvidencePath, baseEvidence);
    const outerPath = path.join(qaRoot, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.targetDeformationReport = deformationPin;
    outer.immutableInputs.baseHorseVisualEvidence = baseEvidencePin;
    for (const row of outer.artifactInventory) {
        if (row.relativePath.endsWith('/deformation-report.json')) row.artifact = deformationPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = baseEvidencePin;
    }
    writeJson(outerPath, outer);
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    }), /deformation gates do not recompute|maximum edge stretch aggregate values disagree/);
});

test('a forged low aggregate P99 cannot hide a failing per-frame deformation P99', async (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const outputDirectory = path.join(f.root, 'per-frame-p99-qa');
    await publishVisualQa(f, fit.clip, outputDirectory);
    const baseRoot = path.join(outputDirectory, 'horse-base-evidence');
    const deformationPath = path.join(baseRoot, 'deformation-report.json');
    const deformation = JSON.parse(fs.readFileSync(deformationPath, 'utf8'));
    deformation.frames[0].maximumEdgeStretch = 3;
    deformation.frames[0].p99EdgeStretch = 3;
    deformation.maximumEdgeStretch = 3;
    deformation.gates.maximumEdgeStretch = true;
    deformation.gates.p99EdgeStretch = true;
    deformation.passed = true;
    const deformationPin = writeJson(deformationPath, deformation);
    const baseEvidencePath = path.join(baseRoot, 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.target_mesh_deformation_qa.maximum_edge_stretch = 3;
    baseEvidence.local_evidence.target_mesh_deformation_qa.report = deformationPin;
    baseEvidence.visual_phase_gate.coincident_rest_vertex_separation.report_sha256 = deformationPin.sha256;
    const basePin = writeJson(baseEvidencePath, baseEvidence);
    const outerPath = path.join(outputDirectory, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.targetDeformationReport = deformationPin;
    outer.immutableInputs.baseHorseVisualEvidence = basePin;
    for (const row of outer.artifactInventory) {
        if (row.relativePath.endsWith('/deformation-report.json')) row.artifact = deformationPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = basePin;
    }
    writeJson(outerPath, outer);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory,
        semanticId: f.spec.semanticId,
        expectedThreeClip: fit.clip,
        expectedImmutableManifest: f.immutable,
        expectedFittingBundle: f.fitting,
        expectedThreeModule: pin(f.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(f.spec.runtime.executables.ffmpeg.path),
    }), /deformation gates do not recompute/);
});

test('fake PNG and fake MP4 payloads remain rejected after every outer pin is rewritten', async (context) => {
    const fakePng = fixture('idle_alert');
    const fakeVideo = fixture('idle_alert');
    const corruptVideo = fixture('idle_alert');
    const fakeSlice = fixture('idle_alert');
    context.after(() => [fakePng, fakeVideo, corruptVideo, fakeSlice]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));

    const pngFit = publishFit(fakePng, publishObservations(fakePng));
    const pngOutput = path.join(fakePng.root, 'fake-png-qa');
    await publishVisualQa(fakePng, pngFit.clip, pngOutput);
    const pngRelative = 'horse-base-evidence/frames/frame_0001.png';
    const pngPin = write(path.join(pngOutput, pngRelative), Buffer.from('not-a-png'));
    const pngOuterPath = path.join(pngOutput, 'visual-phase-qa.json');
    const pngOuter = JSON.parse(fs.readFileSync(pngOuterPath, 'utf8'));
    pngOuter.artifactInventory.find((row) => row.relativePath === pngRelative).artifact = pngPin;
    writeJson(pngOuterPath, pngOuter);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: pngOutput,
        semanticId: fakePng.spec.semanticId,
        expectedThreeClip: pngFit.clip,
        expectedImmutableManifest: fakePng.immutable,
        expectedFittingBundle: fakePng.fitting,
        expectedThreeModule: pin(fakePng.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(fakePng.spec.runtime.executables.ffmpeg.path),
    }), /is not PNG/);

    const videoFit = publishFit(fakeVideo, publishObservations(fakeVideo));
    const videoOutput = path.join(fakeVideo.root, 'fake-video-qa');
    await publishVisualQa(fakeVideo, videoFit.clip, videoOutput);
    const videoPath = path.join(videoOutput, 'horse-base-evidence', 'fixed-camera-preview.mp4');
    const videoPin = write(videoPath, Buffer.from('not-an-mp4'));
    const baseEvidencePath = path.join(videoOutput, 'horse-base-evidence', 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.video = {
        ...baseEvidence.local_evidence.video,
        ...videoPin,
    };
    const basePin = writeJson(baseEvidencePath, baseEvidence);
    const videoOuterPath = path.join(videoOutput, 'visual-phase-qa.json');
    const videoOuter = JSON.parse(fs.readFileSync(videoOuterPath, 'utf8'));
    videoOuter.immutableInputs.fixedCameraPreview = videoPin;
    videoOuter.immutableInputs.baseHorseVisualEvidence = basePin;
    for (const row of videoOuter.artifactInventory) {
        if (row.relativePath.endsWith('/fixed-camera-preview.mp4')) row.artifact = videoPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = basePin;
    }
    writeJson(videoOuterPath, videoOuter);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: videoOutput,
        semanticId: fakeVideo.spec.semanticId,
        expectedThreeClip: videoFit.clip,
        expectedImmutableManifest: fakeVideo.immutable,
        expectedFittingBundle: fakeVideo.fitting,
        expectedThreeModule: pin(fakeVideo.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(fakeVideo.spec.runtime.executables.ffmpeg.path),
    }), /is not an ISO BMFF MP4|has no ftyp|box .* is invalid/);

    const corruptFit = publishFit(corruptVideo, publishObservations(corruptVideo));
    const corruptOutput = path.join(corruptVideo.root, 'corrupt-video-qa');
    await publishVisualQa(corruptVideo, corruptFit.clip, corruptOutput);
    const corruptVideoPath = path.join(corruptOutput, 'horse-base-evidence', 'fixed-camera-preview.mp4');
    const corruptPin = write(
        corruptVideoPath,
        zeroMp4MediaPayload(fs.readFileSync(corruptVideoPath)),
    );
    const corruptBasePath = path.join(corruptOutput, 'horse-base-evidence', 'visual-phase-qa.json');
    const corruptBase = JSON.parse(fs.readFileSync(corruptBasePath, 'utf8'));
    corruptBase.local_evidence.video = { ...corruptBase.local_evidence.video, ...corruptPin };
    const corruptBasePin = writeJson(corruptBasePath, corruptBase);
    const corruptOuterPath = path.join(corruptOutput, 'visual-phase-qa.json');
    const corruptOuter = JSON.parse(fs.readFileSync(corruptOuterPath, 'utf8'));
    corruptOuter.immutableInputs.fixedCameraPreview = corruptPin;
    corruptOuter.immutableInputs.baseHorseVisualEvidence = corruptBasePin;
    for (const row of corruptOuter.artifactInventory) {
        if (row.relativePath.endsWith('/fixed-camera-preview.mp4')) row.artifact = corruptPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = corruptBasePin;
    }
    writeJson(corruptOuterPath, corruptOuter);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: corruptOutput,
        semanticId: corruptVideo.spec.semanticId,
        expectedThreeClip: corruptFit.clip,
        expectedImmutableManifest: corruptVideo.immutable,
        expectedFittingBundle: corruptVideo.fitting,
        expectedThreeModule: pin(corruptVideo.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(corruptVideo.spec.runtime.executables.ffmpeg.path),
    }), /H\.264 NAL payload is invalid|coded sample has no H\.264 VCL/);

    const sliceFit = publishFit(fakeSlice, publishObservations(fakeSlice));
    const sliceOutput = path.join(fakeSlice.root, 'fake-slice-qa');
    await publishVisualQa(fakeSlice, sliceFit.clip, sliceOutput);
    const sliceVideoPath = path.join(sliceOutput, 'horse-base-evidence', 'fixed-camera-preview.mp4');
    const slicePin = write(
        sliceVideoPath,
        forgeUndecodableH264Slices(fs.readFileSync(sliceVideoPath)),
    );
    const sliceBasePath = path.join(sliceOutput, 'horse-base-evidence', 'visual-phase-qa.json');
    const sliceBase = JSON.parse(fs.readFileSync(sliceBasePath, 'utf8'));
    sliceBase.local_evidence.video = { ...sliceBase.local_evidence.video, ...slicePin };
    const sliceBasePin = writeJson(sliceBasePath, sliceBase);
    const sliceOuterPath = path.join(sliceOutput, 'visual-phase-qa.json');
    const sliceOuter = JSON.parse(fs.readFileSync(sliceOuterPath, 'utf8'));
    sliceOuter.immutableInputs.fixedCameraPreview = slicePin;
    sliceOuter.immutableInputs.baseHorseVisualEvidence = sliceBasePin;
    for (const row of sliceOuter.artifactInventory) {
        if (row.relativePath.endsWith('/fixed-camera-preview.mp4')) row.artifact = slicePin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = sliceBasePin;
    }
    writeJson(sliceOuterPath, sliceOuter);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: sliceOutput,
        semanticId: fakeSlice.spec.semanticId,
        expectedThreeClip: sliceFit.clip,
        expectedImmutableManifest: fakeSlice.immutable,
        expectedFittingBundle: fakeSlice.fitting,
        expectedThreeModule: pin(fakeSlice.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(fakeSlice.spec.runtime.executables.ffmpeg.path),
    }), /fails pinned ffmpeg decode/);
});

test('pre-receipt PNG inflate and MP4 table counts are bounded before allocation', async (context) => {
    const pngBomb = fixture('idle_alert');
    const mp4Bomb = fixture('idle_alert');
    context.after(() => [pngBomb, mp4Bomb]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));

    const pngFit = publishFit(pngBomb, publishObservations(pngBomb));
    const pngOutput = path.join(pngBomb.root, 'png-inflate-bound-qa');
    await publishVisualQa(pngBomb, pngFit.clip, pngOutput);
    const pngRelative = 'horse-base-evidence/frames/frame_0001.png';
    const pngPin = write(path.join(pngOutput, pngRelative), oversizedDecodedRgbPng());
    const pngOuterPath = path.join(pngOutput, 'visual-phase-qa.json');
    const pngOuter = JSON.parse(fs.readFileSync(pngOuterPath, 'utf8'));
    pngOuter.artifactInventory.find((row) => row.relativePath === pngRelative).artifact = pngPin;
    writeJson(pngOuterPath, pngOuter);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: pngOutput,
        semanticId: pngBomb.spec.semanticId,
        expectedThreeClip: pngFit.clip,
        expectedImmutableManifest: pngBomb.immutable,
        expectedFittingBundle: pngBomb.fitting,
        expectedThreeModule: pin(pngBomb.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(pngBomb.spec.runtime.executables.ffmpeg.path),
    }), /PNG IDAT cannot be decoded|decoded-size bound/);

    const mp4Fit = publishFit(mp4Bomb, publishObservations(mp4Bomb));
    const mp4Output = path.join(mp4Bomb.root, 'mp4-count-bound-qa');
    await publishVisualQa(mp4Bomb, mp4Fit.clip, mp4Output);
    const videoPath = path.join(mp4Output, 'horse-base-evidence', 'fixed-camera-preview.mp4');
    const videoPin = write(
        videoPath,
        forgeMp4SampleCount(fs.readFileSync(videoPath), 0xffffffff),
    );
    const basePath = path.join(mp4Output, 'horse-base-evidence', 'visual-phase-qa.json');
    const base = JSON.parse(fs.readFileSync(basePath, 'utf8'));
    base.local_evidence.video = { ...base.local_evidence.video, ...videoPin };
    const basePin = writeJson(basePath, base);
    const outerPath = path.join(mp4Output, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.fixedCameraPreview = videoPin;
    outer.immutableInputs.baseHorseVisualEvidence = basePin;
    for (const row of outer.artifactInventory) {
        if (row.relativePath.endsWith('/fixed-camera-preview.mp4')) row.artifact = videoPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = basePin;
    }
    writeJson(outerPath, outer);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: mp4Output,
        semanticId: mp4Bomb.spec.semanticId,
        expectedThreeClip: mp4Fit.clip,
        expectedImmutableManifest: mp4Bomb.immutable,
        expectedFittingBundle: mp4Bomb.fitting,
        expectedThreeModule: pin(mp4Bomb.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(mp4Bomb.spec.runtime.executables.ffmpeg.path),
    }), /coded sample count changed/);
});

test('nested one-shot final-pose measurements and exact inventory cannot be forged', async (context) => {
    const f = fixture('attack_primary');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const outputDirectory = path.join(f.root, 'attack-nested-qa');
    await publishVisualQa(f, fit.clip, outputDirectory);
    const baseRoot = path.join(outputDirectory, 'horse-base-evidence');
    const finalPath = path.join(baseRoot, 'final-pose-stability-report.json');
    const finalPose = JSON.parse(fs.readFileSync(finalPath, 'utf8'));
    finalPose.maximumP99AdjacentDisplacementM = 999;
    const finalPin = writeJson(finalPath, finalPose);
    const baseEvidencePath = path.join(baseRoot, 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.one_shot_final_pose_qa.report = finalPin;
    const basePin = writeJson(baseEvidencePath, baseEvidence);
    const outerPath = path.join(outputDirectory, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.oneShotFinalPoseReport = finalPin;
    outer.immutableInputs.baseHorseVisualEvidence = basePin;
    for (const row of outer.artifactInventory) {
        if (row.relativePath.endsWith('/final-pose-stability-report.json')) row.artifact = finalPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = basePin;
    }
    writeJson(outerPath, outer);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory,
        semanticId: f.spec.semanticId,
        expectedThreeClip: fit.clip,
        expectedImmutableManifest: f.immutable,
        expectedFittingBundle: f.fitting,
        expectedThreeModule: pin(f.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(f.spec.runtime.executables.ffmpeg.path),
    }), /final-pose gates do not recompute|maximum adjacent P99 displacement values disagree/);
    write(path.join(baseRoot, 'unexpected.txt'), 'extra');
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory,
        semanticId: f.spec.semanticId,
        expectedThreeClip: fit.clip,
        expectedImmutableManifest: f.immutable,
        expectedFittingBundle: f.fitting,
        expectedThreeModule: pin(f.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(f.spec.runtime.executables.ffmpeg.path),
    }), /unexpected root inventory/);
});

test('one-shot model bounds and ground remain derived from immutable Horse_2 artifacts', async (context) => {
    const f = fixture('death');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const outputDirectory = path.join(f.root, 'forged-final-bounds-qa');
    await publishVisualQa(f, fit.clip, outputDirectory);
    const baseRoot = path.join(outputDirectory, 'horse-base-evidence');
    const finalPath = path.join(baseRoot, 'final-pose-stability-report.json');
    const finalPose = JSON.parse(fs.readFileSync(finalPath, 'utf8'));
    finalPose.modelHeightM = 999;
    finalPose.modelDiagonalM = 999;
    finalPose.groundHeightM = -100;
    const finalPin = writeJson(finalPath, finalPose);
    const baseEvidencePath = path.join(baseRoot, 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.one_shot_final_pose_qa.report = finalPin;
    const basePin = writeJson(baseEvidencePath, baseEvidence);
    const outerPath = path.join(outputDirectory, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.oneShotFinalPoseReport = finalPin;
    outer.immutableInputs.baseHorseVisualEvidence = basePin;
    for (const row of outer.artifactInventory) {
        if (row.relativePath.endsWith('/final-pose-stability-report.json')) row.artifact = finalPin;
        if (row.relativePath.endsWith('/visual-phase-qa.json')) row.artifact = basePin;
    }
    writeJson(outerPath, outer);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory,
        semanticId: f.spec.semanticId,
        expectedThreeClip: fit.clip,
        expectedImmutableManifest: f.immutable,
        expectedFittingBundle: f.fitting,
        expectedThreeModule: pin(f.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(f.spec.runtime.executables.ffmpeg.path),
    }), /bounds\/ground are not derived from the immutable Horse_2 bundle/);
});

test('base visual wrapper rejects a substituted fitting artifact descriptor', async (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const fit = publishFit(f, publishObservations(f));
    const outputDirectory = path.join(f.root, 'substituted-artifact-qa');
    await publishVisualQa(f, fit.clip, outputDirectory);
    const baseEvidencePath = path.join(outputDirectory, 'horse-base-evidence', 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.immutable_inputs.skeleton = f.skinWeights;
    const basePin = writeJson(baseEvidencePath, baseEvidence);
    const outerPath = path.join(outputDirectory, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.baseHorseVisualEvidence = basePin;
    outer.artifactInventory.find((row) => row.relativePath.endsWith('/visual-phase-qa.json')).artifact = basePin;
    writeJson(outerPath, outer);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory,
        semanticId: f.spec.semanticId,
        expectedThreeClip: fit.clip,
        expectedImmutableManifest: f.immutable,
        expectedFittingBundle: f.fitting,
        expectedThreeModule: pin(f.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(f.spec.runtime.executables.ffmpeg.path),
    }), /skeleton is not the exact fitting-bundle artifact/);
});

test('nested video/runtime claims and extra approval-like fields fail closed after repinning', async (context) => {
    const nestedVideo = fixture('idle_alert');
    const extraApproval = fixture('idle_alert');
    context.after(() => [nestedVideo, extraApproval]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    const nestedFit = publishFit(nestedVideo, publishObservations(nestedVideo));
    const nestedOutput = path.join(nestedVideo.root, 'nested-video-qa');
    await publishVisualQa(nestedVideo, nestedFit.clip, nestedOutput);
    const baseEvidencePath = path.join(nestedOutput, 'horse-base-evidence', 'visual-phase-qa.json');
    const baseEvidence = JSON.parse(fs.readFileSync(baseEvidencePath, 'utf8'));
    baseEvidence.local_evidence.video.fps = 29;
    const basePin = writeJson(baseEvidencePath, baseEvidence);
    const outerPath = path.join(nestedOutput, 'visual-phase-qa.json');
    const outer = JSON.parse(fs.readFileSync(outerPath, 'utf8'));
    outer.immutableInputs.baseHorseVisualEvidence = basePin;
    const baseRow = outer.artifactInventory.find((row) => row.relativePath.endsWith('/visual-phase-qa.json'));
    baseRow.artifact = basePin;
    writeJson(outerPath, outer);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: nestedOutput,
        semanticId: nestedVideo.spec.semanticId,
        expectedThreeClip: nestedFit.clip,
        expectedImmutableManifest: nestedVideo.immutable,
        expectedFittingBundle: nestedVideo.fitting,
        expectedThreeModule: pin(nestedVideo.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(nestedVideo.spec.runtime.executables.ffmpeg.path),
    }), /base video fps|nested deformation\/camera\/runtime evidence is inconsistent/);

    const extraFit = publishFit(extraApproval, publishObservations(extraApproval));
    const extraOutput = path.join(extraApproval.root, 'extra-approval-qa');
    await publishVisualQa(extraApproval, extraFit.clip, extraOutput);
    const extraPath = path.join(extraOutput, 'visual-phase-qa.json');
    const extra = JSON.parse(fs.readFileSync(extraPath, 'utf8'));
    extra.approved = true;
    writeJson(extraPath, extra);
    assert.throws(() => inspectPublishedSemanticVisualQa({
        outputDirectory: extraOutput,
        semanticId: extraApproval.spec.semanticId,
        expectedThreeClip: extraFit.clip,
        expectedImmutableManifest: extraApproval.immutable,
        expectedFittingBundle: extraApproval.fitting,
        expectedThreeModule: pin(extraApproval.spec.runtime.threeModule.path),
        expectedFfmpeg: pin(extraApproval.spec.runtime.executables.ffmpeg.path),
    }), /must contain exactly/);
});

test('frame/mode mismatch and unrecognized semantic IDs fail closed', (context) => {
    const mismatch = fixture('idle_alert');
    const unknown = fixture('idle_alert');
    context.after(() => {
        fs.rmSync(mismatch.root, { recursive: true, force: true });
        fs.rmSync(unknown.root, { recursive: true, force: true });
    });
    const receiptJson = JSON.parse(fs.readFileSync(mismatch.receipt.path, 'utf8'));
    receiptJson.frameCount = 49;
    fs.writeFileSync(mismatch.receipt.path, jsonBuffer(receiptJson));
    rewriteSpec(mismatch, (spec) => {
        spec.rawSemanticPassReceipt.bytes = pin(mismatch.receipt.path).bytes;
        spec.rawSemanticPassReceipt.sha256 = pin(mismatch.receipt.path).sha256;
    }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: mismatch.specPath, expectedSpecSha256: mismatch.specSha256,
    }), /receipt does not match the authoritative action contract/);
    rewriteSpec(unknown, (spec) => { spec.semanticId = 'horse_dance_unknown'; }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: unknown.specPath, expectedSpecSha256: unknown.specSha256,
    }), /Unknown animation-fitting action/);
});

test('candidate/evidence/contract tampering fails before a command is authored', (context) => {
    const candidateTamper = fixture('idle_alert');
    const evidenceTamper = fixture('idle_alert');
    const contractTamper = fixture('idle_alert');
    context.after(() => [candidateTamper, evidenceTamper, contractTamper]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    fs.appendFileSync(candidateTamper.candidate.path, 'tamper');
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: candidateTamper.specPath, expectedSpecSha256: candidateTamper.specSha256,
    }), /spec\.candidate does not match its immutable pin/);
    const qa = JSON.parse(fs.readFileSync(evidenceTamper.rawEvidence.path, 'utf8'));
    qa.checks.fixedCamera = false;
    fs.writeFileSync(evidenceTamper.rawEvidence.path, jsonBuffer(qa));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: evidenceTamper.specPath, expectedSpecSha256: evidenceTamper.specSha256,
    }), /raw semantic receipt\.evidence does not match its immutable pin/);
    rewriteSpec(contractTamper, (spec) => { spec.contracts.taxonomy.sha256 = 'b'.repeat(64); }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: contractTamper.specPath, expectedSpecSha256: contractTamper.specSha256,
    }), /spec\.contracts\.taxonomy does not match its immutable pin/);
});

test('client QA assertion is rejected even when its receipt says PASS', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const receipt = JSON.parse(fs.readFileSync(f.receipt.path, 'utf8'));
    receipt.authority.kind = 'client';
    receipt.authority.clientAssertionAccepted = true;
    fs.writeFileSync(f.receipt.path, jsonBuffer(receipt));
    rewriteSpec(f, (spec) => {
        const current = pin(f.receipt.path);
        spec.rawSemanticPassReceipt.bytes = current.bytes;
        spec.rawSemanticPassReceipt.sha256 = current.sha256;
    }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    }), /client QA assertions are forbidden/);
});

test('standalone/self-declared server claims cannot author READY without authenticated trust', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    assert.equal(f.trust.schema, BROWSER_CANDIDATE_PIPELINE_TRUST_SCHEMA);
    assert.throws(() => inspectPipelineCore({
        specPath: f.specPath,
        expectedSpecSha256: f.specSha256,
        statePath: f.statePath,
    }), /authenticated server trust context/);
    assert.throws(() => inspectPipelineCore({
        ...f.inspectArgs,
        trustSecret: 'wrong-secret-that-is-still-long-enough-for-hmac-validation',
    }), /HMAC authentication failed/);
    const alternateState = path.join(path.dirname(f.statePath), 'alternate.json');
    assert.throws(() => inspectPipelineCore({
        ...f.inspectArgs,
        statePath: alternateState,
    }), /does not bind the exact spec/);
});

test('forged and expired trust envelopes fail before any command is authored', (context) => {
    const forged = fixture('idle_alert');
    const expired = fixture('idle_alert');
    const premature = fixture('idle_alert');
    context.after(() => [forged, expired, premature]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    const value = JSON.parse(fs.readFileSync(forged.trustPath, 'utf8'));
    value.issuedAtEpochMs += 1;
    fs.writeFileSync(forged.trustPath, jsonBuffer(value));
    const forgedPin = pin(forged.trustPath);
    assert.throws(() => inspectPipelineCore({
        ...forged.inspectArgs,
        expectedTrustContextSha256: forgedPin.sha256,
    }), /HMAC authentication failed/);
    refreshTrust(expired, { nowEpochMs: Date.now() - 60 * 60 * 1000, ttlMs: 60 * 1000 });
    assert.throws(() => inspectPipelineCore(expired.inspectArgs), /expired, premature/);
    refreshTrust(premature, { nowEpochMs: Date.now() + 60 * 60 * 1000, ttlMs: 60 * 1000 });
    assert.throws(() => inspectPipelineCore(premature.inspectArgs), /expired, premature/);
    assert.throws(() => inspectPipelineCore({
        ...forged.inspectArgs,
        expectedTrustContextSha256: forgedPin.sha256,
        nowEpochMs: Number.NaN,
    }), /validation time must be an integer/);
});

test('authenticated trust file cannot alias or overlap its bound state/output locations', (context) => {
    const stateOverlap = fixture('idle_alert');
    const outputOverlap = fixture('idle_alert');
    context.after(() => [stateOverlap, outputOverlap]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    assert.throws(() => inspectPipelineCore({
        ...stateOverlap.inspectArgs,
        statePath: stateOverlap.trustPath,
    }), /trust context must be immutable and outside output\/state/);
    fs.mkdirSync(outputOverlap.outputRoot, { recursive: true });
    const nestedTrust = path.join(outputOverlap.outputRoot, 'server-trust.json');
    fs.copyFileSync(outputOverlap.trustPath, nestedTrust);
    assert.throws(() => inspectPipelineCore({
        ...outputOverlap.inspectArgs,
        trustContextPath: nestedTrust,
    }), /trust context must be immutable and outside output\/state/);
});

test('runtime substitution and incomplete transitive dependency inventories fail closed', (context) => {
    const runtimeSwap = fixture('idle_alert');
    const runtimeContentSwap = fixture('idle_alert');
    const gitContentSwap = fixture('idle_alert');
    const runtimeLockCopy = fixture('idle_alert');
    const dependencyDrop = fixture('idle_alert');
    context.after(() => [runtimeSwap, runtimeContentSwap, gitContentSwap, runtimeLockCopy, dependencyDrop]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    const cmd = write(path.join(runtimeSwap.root, 'runtime', 'cmd.exe'), Buffer.from('not-node'));
    rewriteSpec(runtimeSwap, (spec) => { spec.runtime.executables.node = cmd; }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: runtimeSwap.specPath, expectedSpecSha256: runtimeSwap.specSha256,
    }), /not the expected runtime executable/);
    fs.appendFileSync(runtimeContentSwap.spec.runtime.executables.node.path, 'same-basename-content-swap');
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: runtimeContentSwap.specPath, expectedSpecSha256: runtimeContentSwap.specSha256,
    }), /spec\.runtime\.executables\.node does not match its immutable pin/);
    fs.appendFileSync(gitContentSwap.spec.runtime.executables.git.path, 'substituted-git');
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: gitContentSwap.specPath, expectedSpecSha256: gitContentSwap.specSha256,
    }), /spec\.runtime\.executables\.git does not match its immutable pin/);
    const copiedLock = write(
        path.join(runtimeLockCopy.root, 'runtime', 'runtime-lock.v1.json'),
        fs.readFileSync(BROWSER_CANDIDATE_TOOL_SOURCE_PATHS.trackingRuntimeLockContract),
    );
    rewriteSpec(runtimeLockCopy, (spec) => { spec.runtime.trackingRuntimeLock = copiedLock; }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: runtimeLockCopy.specPath, expectedSpecSha256: runtimeLockCopy.specSha256,
    }), /trackingRuntimeLock\.path is not authoritative/);
    rewriteSpec(dependencyDrop, (spec) => { spec.dependencyInventory.files.pop(); }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: dependencyDrop.specPath, expectedSpecSha256: dependencyDrop.specSha256,
    }), /exact transitive closure/);
    const discovered = discoverBrowserCandidateToolDependencies().map((row) => row.path.replaceAll('\\', '/'));
    for (const suffix of [
        '/tracking_runtime/independent_foreground_gate.py',
        '/animation_fitting/errors.py',
        '/static/js/animation-fitting-browser-core.js',
        '/browser_animation_visual_phase_qa.mjs',
        '/tracking_runtime/runtime-lock.v1.json',
    ]) assert.ok(discovered.some((filename) => filename.endsWith(suffix)), `missing transitive ${suffix}`);
    assert.equal(discovered.length, new Set(discovered).size);
});

test('dependency scanners cover multiline dynamic JS and from-dot Python imports', () => {
    assert.deepEqual(dynamicJavaScriptImportExpressions(`
        const first = import(
            './worker.mjs'
        );
        const second = import(
            resolveRuntime(
                config.runtime
            )
        );
        const third = import /* dependency comments cannot hide calls */ (
            './hidden.mjs'
        );
    `), [
        "'./worker.mjs'",
        'resolveRuntime(\n                config.runtime\n            )',
        "'./hidden.mjs'",
    ]);
    assert.deepEqual(pythonRelativeImportClauses(`
        from . import (
            alpha,
            # an inline package comment must not absorb beta
            beta as renamed,
        )
        from ..runtime import loader
        from . import gamma, \
            delta
        sentinel = 1; from animation_fitting import hidden
    `), [
        { dots: 1, named: '', members: ['alpha', 'beta'] },
        { dots: 2, named: 'runtime', members: ['loader'] },
        { dots: 1, named: '', members: ['gamma', 'delta'] },
        { dots: 0, named: 'animation_fitting', members: ['hidden'] },
    ]);
    assert.deepEqual(pythonAbsoluteImportClauses(`
        sentinel = 1; import animation_fitting.hidden, animation_fitting.other as alias
    `), ['animation_fitting.hidden', 'animation_fitting.other']);
    assert.deepEqual(pythonAbsoluteImportClauses(String.raw`sentinel = 1; \
        import animation_fitting.continued`), ['animation_fitting.continued']);
    assert.deepEqual(pythonRelativeImportClauses(String.raw`
        from animation_fitting.зло import x
        from .зло import y
        from animation_fitting . evil import z
        from animation_fitting \
            .continued import q
    `), [
        { dots: 0, named: 'animation_fitting.зло', members: ['x'] },
        { dots: 1, named: 'зло', members: ['y'] },
        { dots: 0, named: 'animation_fitting.evil', members: ['z'] },
        { dots: 0, named: 'animation_fitting.continued', members: ['q'] },
    ]);
    assert.deepEqual(pythonAbsoluteImportClauses(`
        import animation_fitting . evil
    `), ['animation_fitting.evil']);
    assert.deepEqual(javascriptStaticModuleSpecifiers(`
        import /* hidden gap */ './side-effect.js';
        export /* hidden gap */ { value } from './exported.js';
        import { plain } from './plain.js';
        import{compact}from'./compact-import.js';
        export{compact}from'./compact-export.js';
        const before = true; import './after-statement.js';
        ;import './after-empty-statement.js';
        function declaration(){}import './after-declaration.js';
        const unicodeLine = true; import './after-line-separator.js';
        const unicodeParagraph = true; export{x}from'./after-paragraph-separator.js';
    `), [
        './side-effect.js', './exported.js', './plain.js',
        './compact-import.js', './compact-export.js', './after-statement.js',
        './after-empty-statement.js', './after-declaration.js',
        './after-line-separator.js', './after-paragraph-separator.js',
    ]);
    assert.equal(hasUnresolvedCommonJsLoader("require /* hidden gap */ ('./escape.cjs')"), true);
});

test('bidirectional containment, hardlinks, and output reparse links are rejected', (context) => {
    const overlap = fixture('idle_alert');
    const hardlink = fixture('idle_alert');
    const linkedOutput = fixture('idle_alert');
    context.after(() => [overlap, hardlink, linkedOutput]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    rewriteSpec(overlap, (spec) => {
        spec.outputRoot = path.join(path.dirname(overlap.candidate.path), overlap.candidateId);
    }, { refresh: false });
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: overlap.specPath, expectedSpecSha256: overlap.specSha256,
    }), /overlap an immutable input root|outside all immutable inputs/);
    const secondLink = path.join(hardlink.root, 'inputs', 'candidate-hardlink.mp4');
    fs.linkSync(hardlink.candidate.path, secondLink);
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: hardlink.specPath, expectedSpecSha256: hardlink.specSha256,
    }), /must not be hard-linked/);
    fs.mkdirSync(linkedOutput.outputRoot, { recursive: true });
    const external = path.join(linkedOutput.root, 'external-observations');
    fs.mkdirSync(external);
    try {
        fs.symlinkSync(external, path.join(linkedOutput.outputRoot, '01-observations'),
            process.platform === 'win32' ? 'junction' : 'dir');
    } catch (error) {
        if (!['EPERM', 'EACCES', 'ENOTSUP'].includes(error.code)) throw error;
        return;
    }
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: linkedOutput.specPath, expectedSpecSha256: linkedOutput.specSha256,
    }), /symlink, junction, or reparse alias|link\/reparse/);
});

test('candidate replacement between lstat and open is rejected as TOCTOU', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const originalOpenSync = fs.openSync;
    const backup = `${f.candidate.path}.pre-swap`;
    let swapped = false;
    fs.openSync = function guardedOpen(filename, ...args) {
        if (!swapped && typeof filename === 'string'
            && path.resolve(filename) === path.resolve(f.candidate.path)) {
            swapped = true;
            fs.renameSync(f.candidate.path, backup);
            fs.writeFileSync(f.candidate.path, Buffer.from('attacker-replacement-with-different-inode'));
        }
        return originalOpenSync.call(fs, filename, ...args);
    };
    try {
        assert.throws(() => inspectBrowserAnimationCandidatePipeline({
            specPath: f.specPath, expectedSpecSha256: f.specSha256,
        }), /changed before it was opened|changed while it was read/);
        assert.equal(swapped, true);
    } finally {
        fs.openSync = originalOpenSync;
    }
});

test('fit outputs reject unbound tracks, non-finite chronology, and manifest pin drift', (context) => {
    const badBinding = fixture('idle_alert');
    const badPayload = fixture('idle_alert');
    const badChronology = fixture('idle_alert');
    const badClipName = fixture('idle_alert');
    const badManifest = fixture('idle_alert');
    context.after(() => [badBinding, badPayload, badChronology, badClipName, badManifest]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    const observations = publishObservations(badBinding);
    const fit = publishFit(badBinding, observations);
    const clipJson = JSON.parse(fs.readFileSync(fit.clip.path, 'utf8'));
    clipJson.tracks[0].name = 'Injected.quaternion';
    fs.writeFileSync(fit.clip.path, jsonBuffer(clipJson));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: badBinding.specPath, expectedSpecSha256: badBinding.specSha256,
    }), /not bound to an exact canonical skeleton/);
    const payloadObservations = publishObservations(badPayload);
    const payloadFit = publishFit(badPayload, payloadObservations);
    const payloadClip = JSON.parse(fs.readFileSync(payloadFit.clip.path, 'utf8'));
    payloadClip.tracks.find((track) => track.type === 'vector').values[3] = 0.25;
    fs.writeFileSync(payloadFit.clip.path, jsonBuffer(payloadClip));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: badPayload.specPath, expectedSpecSha256: badPayload.specSha256,
    }), /fitted-animation and Three clip tracks are not byte-value equivalent/);
    const chronologyObservations = publishObservations(badChronology);
    const chronologyFit = publishFit(badChronology, chronologyObservations);
    const chronologyClip = JSON.parse(fs.readFileSync(chronologyFit.clip.path, 'utf8'));
    chronologyClip.tracks[0].times[1] = chronologyClip.tracks[0].times[0];
    fs.writeFileSync(chronologyFit.clip.path, jsonBuffer(chronologyClip));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: badChronology.specPath, expectedSpecSha256: badChronology.specSha256,
    }), /does not preserve exact 30fps chronology/);
    const nameObservations = publishObservations(badClipName);
    const nameFit = publishFit(badClipName, nameObservations);
    const namedClip = JSON.parse(fs.readFileSync(nameFit.clip.path, 'utf8'));
    namedClip.name = 'Different_but_otherwise_valid_clip';
    fs.writeFileSync(nameFit.clip.path, jsonBuffer(namedClip));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: badClipName.specPath, expectedSpecSha256: badClipName.specSha256,
    }), /initial fit manifests do not bind the emitted fitted\/Three track inventories/);
    const observations2 = publishObservations(badManifest);
    const fit2 = publishFit(badManifest, observations2);
    const summary = JSON.parse(fs.readFileSync(fit2.summary.path, 'utf8'));
    summary.inputs.sourceVideoSha256 = 'f'.repeat(64);
    fs.writeFileSync(fit2.summary.path, jsonBuffer(summary));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: badManifest.specPath, expectedSpecSha256: badManifest.specSha256,
    }), /lost candidate\/action\/canonical\/observations pins/);
});

test('fit outputs reject root-only/epsilon motion and caller-authored QA or thresholds', (context) => {
    const rootOnly = fixture('attack_primary');
    const unrelatedMotion = fixture('attack_primary');
    const epsilonMotion = fixture('attack_primary');
    const qaForge = fixture('idle_alert');
    const thresholdForge = fixture('idle_alert');
    context.after(() => [rootOnly, unrelatedMotion, epsilonMotion, qaForge, thresholdForge]
        .forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));

    const rootFit = publishFit(rootOnly, publishObservations(rootOnly));
    const rootClip = JSON.parse(fs.readFileSync(rootFit.clip.path, 'utf8'));
    rootClip.tracks = rootClip.tracks.filter((track) => track.name.startsWith('Root.'));
    fs.writeFileSync(rootFit.clip.path, jsonBuffer(rootClip));
    const rootFitted = JSON.parse(fs.readFileSync(rootFit.fitted.path, 'utf8'));
    rootFitted.tracks = rootFitted.tracks.filter((track) => track.name === 'Root.quaternion');
    rootFitted.positionTracks = rootFitted.positionTracks.filter((track) => track.name === 'Root.position');
    fs.writeFileSync(rootFit.fitted.path, jsonBuffer(rootFitted));
    const rootSummary = JSON.parse(fs.readFileSync(rootFit.summary.path, 'utf8'));
    rootSummary.fit.quaternionTracks = 1;
    rootSummary.fit.positionTracks = 1;
    rootSummary.hierarchyClip.tracks = 2;
    fs.writeFileSync(rootFit.summary.path, jsonBuffer(rootSummary));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: rootOnly.specPath, expectedSpecSha256: rootOnly.specSha256,
    }), /has no animated canonical non-root bone|contains no actual non-root skeletal motion|does not animate every mapped non-root bone/);

    const unrelatedFit = publishFit(unrelatedMotion, publishObservations(unrelatedMotion));
    const renameMappedBones = (payload) => {
        payload.tracks.forEach((track) => {
            track.name = track.name.replace(/^Bone_00([1-4])\./, (_, digit) => `Bone_00${Number(digit) + 4}.`);
        });
        (payload.positionTracks || []).forEach((track) => {
            track.name = track.name.replace(/^Bone_00([1-4])\./, (_, digit) => `Bone_00${Number(digit) + 4}.`);
        });
    };
    const unrelatedClip = JSON.parse(fs.readFileSync(unrelatedFit.clip.path, 'utf8'));
    const unrelatedFitted = JSON.parse(fs.readFileSync(unrelatedFit.fitted.path, 'utf8'));
    renameMappedBones(unrelatedClip);
    renameMappedBones(unrelatedFitted);
    fs.writeFileSync(unrelatedFit.clip.path, jsonBuffer(unrelatedClip));
    fs.writeFileSync(unrelatedFit.fitted.path, jsonBuffer(unrelatedFitted));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: unrelatedMotion.specPath, expectedSpecSha256: unrelatedMotion.specSha256,
    }), /does not animate every mapped non-root bone: Bone_001, Bone_002, Bone_003, Bone_004/);

    const epsilonFit = publishFit(epsilonMotion, publishObservations(epsilonMotion));
    const epsilonClip = JSON.parse(fs.readFileSync(epsilonFit.clip.path, 'utf8'));
    const epsilonFitted = JSON.parse(fs.readFileSync(epsilonFit.fitted.path, 'utf8'));
    const suppressMotion = (track) => {
        const size = track.type === 'quaternion' ? 4 : 3;
        const first = track.values.slice(0, size);
        track.values = Array.from({ length: epsilonMotion.action.frameCount }, () => [...first]).flat();
        if (track.name === 'Bone_001.quaternion') {
            const frame = Math.floor(epsilonMotion.action.frameCount / 2);
            track.values[frame * 4 + 2] = 2e-6;
            track.values[frame * 4 + 3] = Math.sqrt(1 - (2e-6 ** 2));
        }
    };
    epsilonClip.tracks.filter((track) => !track.name.startsWith('Root.')).forEach(suppressMotion);
    [...epsilonFitted.tracks, ...epsilonFitted.positionTracks]
        .filter((track) => !track.name.startsWith('Root.')).forEach(suppressMotion);
    fs.writeFileSync(epsilonFit.clip.path, jsonBuffer(epsilonClip));
    fs.writeFileSync(epsilonFit.fitted.path, jsonBuffer(epsilonFitted));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: epsilonMotion.specPath, expectedSpecSha256: epsilonMotion.specSha256,
    }), /does not animate every mapped non-root bone/);

    const qaFit = publishFit(qaForge, publishObservations(qaForge));
    const qaSummary = JSON.parse(fs.readFileSync(qaFit.summary.path, 'utf8'));
    const qaFitted = JSON.parse(fs.readFileSync(qaFit.fitted.path, 'utf8'));
    for (const qa of [qaSummary.fit.qa, qaFitted.qa]) {
        qa.finalMeanTargetErrorPx = 0.5;
        qa.maximumTargetErrorPx = 0.5;
    }
    qaSummary.gates.results.find((row) => row.name === 'final_mean_target_error_px').actual = 0.5;
    qaSummary.gates.results.find((row) => row.name === 'maximum_target_error_px').actual = 0.5;
    qaSummary.gates.results.find((row) => row.name === 'target_error_improved').actual = 0.5;
    fs.writeFileSync(qaFit.summary.path, jsonBuffer(qaSummary));
    fs.writeFileSync(qaFit.fitted.path, jsonBuffer(qaFitted));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: qaForge.specPath, expectedSpecSha256: qaForge.specSha256,
    }), /does not recompute from observations\/debug frames/);

    const gateFit = publishFit(thresholdForge, publishObservations(thresholdForge));
    const gateSummary = JSON.parse(fs.readFileSync(gateFit.summary.path, 'utf8'));
    gateSummary.gates.results[0].threshold = 1e9;
    fs.writeFileSync(gateFit.summary.path, jsonBuffer(gateSummary));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: thresholdForge.specPath, expectedSpecSha256: thresholdForge.specSha256,
    }), /fit gate .*changed|fit gate inventory\/values changed|exact canonical gate inventory/);
});

test('minimal or forged hoof PASS schedules cannot authorize contact refitting', (context) => {
    const f = fixture('walk_forward');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations);
    const { diagnostic } = publishWalkDiagnostic(f, observations, fit);
    const value = JSON.parse(fs.readFileSync(diagnostic.path, 'utf8'));
    value.status = 'PASS';
    value.schedule = {
        schema: HOOF_CONTACT_INFERENCE_CONTRACT.schedule,
        status: 'PASS',
        frameCount: f.action.frameCount,
        uniqueFrameCount: f.action.frameCount - 1,
        fps: 30,
        loop: true,
        qa: { failures: [] },
    };
    fs.writeFileSync(diagnostic.path, jsonBuffer(value));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    }), /canonical recomputation failed|schedule does not recompute/);
});

test('SAM2 ground evidence is recomputed from admitted mask bytes', (context) => {
    const f = fixture('walk_forward');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const observations = publishObservations(f);
    const fit = publishFit(f, observations);
    const { ground } = publishWalkDiagnostic(f, observations, fit);
    const value = JSON.parse(fs.readFileSync(ground.path, 'utf8'));
    value.frames[0].hooves.hind_left.globalBottomGapPx += 10;
    fs.writeFileSync(ground.path, jsonBuffer(value));
    assert.throws(() => inspectBrowserAnimationCandidatePipeline({
        specPath: f.specPath, expectedSpecSha256: f.specSha256,
    }), /ground evidence does not recompute from admitted masks/);
});

test('authored commands are shell=false and contain no Blender or fitting mixer command', (context) => {
    const fixtures = ['idle_alert', 'walk_forward', 'death', 'attack_primary'].map(fixture);
    context.after(() => fixtures.forEach((f) => fs.rmSync(f.root, { recursive: true, force: true })));
    for (const f of fixtures) {
        const states = [];
        states.push(inspectBrowserAnimationCandidatePipeline({
            specPath: f.specPath, expectedSpecSha256: f.specSha256,
        }));
        assert.deepEqual(states[0].next.command.environment, {
            PATH: path.dirname(f.spec.runtime.executables.git.realpath),
            ...(process.platform === 'win32' ? {
                PATHEXT: '.EXE',
                NoDefaultCurrentDirectoryInExePath: '1',
            } : {}),
        });
        assert.ok(states[0].next.command.preconditions.some((row) => (
            row.sha256 === f.spec.runtime.executables.git.sha256
                && path.resolve(row.path) === path.resolve(f.spec.runtime.executables.git.path)
        )));
        publishObservations(f);
        states.push(inspectBrowserAnimationCandidatePipeline({
            specPath: f.specPath, expectedSpecSha256: f.specSha256,
        }));
        for (const state of states) {
            const command = state.next.command;
            assert.equal(command.shell, false);
            const serialized = JSON.stringify([command.executable, ...command.argv]).toLowerCase();
            assert.doesNotMatch(serialized, /blender/);
            assert.doesNotMatch(serialized, /animationmixer|mixer/);
        }
    }
});

test('CLI state publication is create-exclusive and never executes its command', (context) => {
    const f = fixture('idle_alert');
    context.after(() => fs.rmSync(f.root, { recursive: true, force: true }));
    const statePath = f.statePath;
    const previousSecret = process.env.AUTORIG_ANIMATION_FITTING_TRUST_HMAC_SECRET;
    process.env.AUTORIG_ANIMATION_FITTING_TRUST_HMAC_SECRET = TRUST_SECRET;
    context.after(() => {
        if (previousSecret == null) delete process.env.AUTORIG_ANIMATION_FITTING_TRUST_HMAC_SECRET;
        else process.env.AUTORIG_ANIMATION_FITTING_TRUST_HMAC_SECRET = previousSecret;
    });
    const stdout = [];
    const stderr = [];
    const streams = {
        stdout: { write: (value) => stdout.push(value) },
        stderr: { write: (value) => stderr.push(value) },
    };
    const argv = [
        '--spec', f.specPath, '--spec-sha256', f.specSha256,
        '--trust-context', f.trustPath, '--trust-context-sha256', f.trustSha256,
        '--state', statePath,
    ];
    assert.equal(runBrowserCandidatePipelineCli(argv, streams), 0, stderr.join(''));
    assert.equal(stderr.length, 0);
    const state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    assert.equal(state.status, 'READY_TRACKING');
    assert.equal(state.next.command.shell, false);
    assert.equal(fs.existsSync(path.join(f.outputRoot, '01-observations')), false);
    assert.equal(runBrowserCandidatePipelineCli(argv, streams), 2);
    assert.match(stderr.at(-1), /state output already exists/);
});
